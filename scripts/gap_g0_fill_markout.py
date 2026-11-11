"""Diagnostic: link actual official G0-R maker fills to pre-fill Binance gap.

One Tardis free day per process; first 10min calibrate/fit gap thresholds,
then 50min G0-R fills. ALL matching/queue/execution via hftbacktest 2.4.4.
Local-visible cross-venue gap is queried 25ms BEFORE exchange fill timestamp.
Important: local feed clocks and exchange fill clocks are different sources;
this is an exploratory association, never proof a cancellation could succeed.
"""
import argparse
import csv
import json
import math
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from check_okx_hft_data import SCHEMAS, extract_window
from cross_venue_event_probe import read_binance_bbo, visible_timestamps
from g0_fill_markout import read_exchange_bbo, asof_exchange_mid
from g0_glft_baseline import CTVAL, LOT, TICK, GAMMA
from g0_glft_rolling import run_mode
from gap_event_regression import event_gap_samples

NS = 1_000_000_000
SIGNAL_LEAD_MS = 25    # Before exchange fill; diagnostic only, not cancel ACK.
EXTRA_BINANCE_MS = 25  # Added to original collection timestamp.
HORIZON_MS = 100
MAX_AGE_MS = 250


def local_okx_bbo(path):
    ts, mid = [], []
    with Path(path).open(encoding="utf-8", newline="") as stream:
        for row in csv.DictReader(stream):
            bid, ask = float(row["bid_price"]), float(row["ask_price"])
            if not 0 < bid < ask:
                raise ValueError("Invalid OKX native quote")
            ts.append(int(row["local_timestamp"]) * 1000)
            mid.append((bid + ask) / 2)
    if not ts or np.any(np.diff(np.asarray(ts, dtype=np.int64)) < 0):
        raise ValueError("Invalid local OKX quote clock")
    return np.asarray(ts, dtype=np.int64), np.asarray(mid, dtype=np.float64)


def gap_at_strict_past_times(okx_time, okx_mid, bin_time, bin_mid, times):
    if not len(times):
        return np.empty(0, np.float64), np.empty(0, bool)
    # event_gap_samples maintains exactly the previous valid 60s causal
    # basis and strict as-of < decision time across both sources.
    unique_times, restore = np.unique(np.asarray(times, dtype=np.int64),
                                      return_inverse=True)
    observed = event_gap_samples(
        okx_time, okx_mid, bin_time, bin_mid, basis_seconds=60,
        tau_ms=HORIZON_MS, max_age_ms=MAX_AGE_MS,
        decision_times=unique_times)
    unique_gaps = np.full(len(unique_times), np.nan)
    idx = np.searchsorted(unique_times, observed["t_ns"], side="left")
    if np.any(idx >= len(unique_times)) or not np.array_equal(
            unique_times[idx], observed["t_ns"]):
        raise RuntimeError("Gap query accidentally matched a different timestamp")
    unique_gaps[idx] = observed["gap_usdt_per_btc"]
    got = unique_gaps[restore]
    return got, np.isfinite(got)


def strict_warmup_thresholds(ot, om, bt, bm, start_ns):
    # Calibrate the bins exclusively using the first 10min, before G0 trading.
    # First 60s reserved for basis warm-up. Thresholds never depend on fills.
    grid = np.arange(start_ns + 61*NS, start_ns + 600*NS,
                     50_000_000, dtype=np.int64)
    observed = event_gap_samples(ot, om, bt, bm, basis_seconds=60,
                                 tau_ms=HORIZON_MS, max_age_ms=MAX_AGE_MS,
                                 decision_times=grid)
    values = np.abs(observed["gap_usdt_per_btc"])
    if len(values) < 1000:
        raise RuntimeError("Insufficient pretrade history for fixed gap thresholds")
    a, b = np.quantile(values, [0.5, 0.9])
    return {"abs_gap_p50_usdt_per_btc": float(a),
            "abs_gap_p90_usdt_per_btc": float(b),
            "warmup_valid_fixed_50ms_samples": int(len(values))}


def group_stats(mask, signed_bps, continuation_bps, signed_gap):
    n = int(np.count_nonzero(mask))
    if not n:
        return {"fills": 0, "mean_total_markout_bps": None,
                "mean_continuation_bps": None, "adverse_total_fraction": None}
    cont = continuation_bps[mask]
    good = np.isfinite(cont)
    return {
        "fills": n,
        "mean_total_markout_bps": float(np.mean(signed_bps[mask])),
        "adverse_total_fraction": float(np.mean(signed_bps[mask] < 0)),
        "mean_continuation_bps": float(np.mean(cont[good])) if good.any() else None,
        "continuation_valid": int(np.count_nonzero(good)),
        "mean_signed_risk_gap_usdt_per_btc": float(np.mean(signed_gap[mask]))
    }


def analyze(fills, exch_ts, exch_mid, local_ot, local_om, bin_bt, bin_bm,
            thresholds, lead_ms=SIGNAL_LEAD_MS):
    if not fills:
        return {"n_official_fills": 0, "n_valid_aligned": 0}
    fts = np.asarray([f["exch_timestamp_ns"] for f in fills], np.int64)
    side = np.asarray([f["side_sign"] for f in fills], np.float64)
    if not np.all(np.isin(side, [-1, 1])):
        raise ValueError("Unexpected maker side sign")
    # Query original order, with a stable sorted map only for the as-of join.
    query = fts - lead_ms * 1_000_000
    order = np.argsort(query, kind="stable")
    gap_sorted, valid_sorted = gap_at_strict_past_times(
        local_ot, local_om, bin_bt, bin_bm, query[order])
    gap = np.empty(len(fills), np.float64)
    good_gap = np.empty(len(fills), bool)
    gap[order] = gap_sorted
    good_gap[order] = valid_sorted
    pre, valid_pre, _ = asof_exchange_mid(
        exch_ts, exch_mid, fts, strict=True, max_age_ms=MAX_AGE_MS)
    after, valid_after, _ = asof_exchange_mid(
        exch_ts, exch_mid, fts + HORIZON_MS*1_000_000,
        max_age_ms=MAX_AGE_MS)
    first_post_i = np.searchsorted(exch_ts, fts, side="right")
    ix = np.minimum(first_post_i, len(exch_ts)-1)
    first_post = exch_mid[ix]
    cont_ok = ((first_post_i < len(exch_ts)) &
               (exch_ts[ix] > fts) &
               (exch_ts[ix] <= fts + HORIZON_MS*1_000_000))
    valid = good_gap & valid_pre & valid_after
    total = 10_000.0 * side * (after - pre) / pre
    cont = np.where(cont_ok, 10_000.0*side*(after-first_post)/first_post,
                    np.nan)
    # Positive risk: Binance gap points AGAINST the maker's fill direction.
    # Positive x for a buy expects rise and is favourable, not harmful.
    # For a buy (side +1), negative gap -> positive risk; for sell opposite.
    risk = -side * gap
    p50 = thresholds["abs_gap_p50_usdt_per_btc"]
    p90 = thresholds["abs_gap_p90_usdt_per_btc"]
    magnitude = np.abs(gap)
    m_low = valid & (magnitude <= p50)
    m_mid = valid & (magnitude > p50) & (magnitude <= p90)
    m_high = valid & (magnitude > p90)
    if np.count_nonzero(m_low | m_mid | m_high) != np.count_nonzero(valid):
        raise RuntimeError("Not all valid fills assigned to an absolute gap bin")
    parts = {
        "all_valid": group_stats(valid, total, cont, risk),
        "abs_low_le_pretrade_p50": group_stats(m_low, total, cont, risk),
        "abs_medium_pretrade_p50_to_p90": group_stats(m_mid, total, cont, risk),
        "abs_extreme_gt_pretrade_p90": group_stats(m_high, total, cont, risk),
        "extreme_adverse_signal_risk_pos": group_stats(m_high & (risk > 0), total, cont, risk),
        "extreme_favourable_signal_risk_neg": group_stats(m_high & (risk < 0), total, cont, risk),
        "buy_fills": group_stats(valid & (side == 1), total, cont, risk),
        "sell_fills": group_stats(valid & (side == -1), total, cont, risk)
    }
    return {
        "n_official_fills": len(fills),
        "n_valid_aligned": int(np.count_nonzero(valid)),
        "n_without_causal_gap": int(np.count_nonzero(~good_gap)),
        "n_without_exchange_before_bbo": int(np.count_nonzero(~valid_pre)),
        "n_without_exchange_after_bbo": int(np.count_nonzero(~valid_after)),
        "n_high_magnitude_fills": int(np.count_nonzero(m_high)),
        "lead_before_exchange_fill_ms": lead_ms,
        "warning": "Potential cancellation remains unverified: mixed exchange-fill and "
                   "collection-local clocks; order active/cancellable state and "
                   "request latency competition not replayed for any G2 strategy.",
        "groups": parts,
    }


def run(day, report_dir="reports"):
    from hftbacktest.data.utils import tardis
    from hftbacktest.data.validation import validate_event_order
    start_us = int(datetime.strptime(day, "%Y-%m-%d").replace(
        tzinfo=timezone.utc).timestamp()) * 1_000_000
    end_us = start_us + 3600 * 1_000_000
    target = Path(report_dir) / f"gap_g0_fill_markout_{day}_50m.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    report = {"status": "started", "day": day,
              "train_minutes": 10, "trading_minutes": 50,
              "not_new_holdout": True, "not_g2_or_cancel_effect": True,
              "binance_extra_delay_ms": EXTRA_BINANCE_MS,
              "signal_at_exchange_fill_minus_ms": SIGNAL_LEAD_MS,
              "maker_fill_source": "official hftbacktest 2.4.4 order.exch_timestamp",
              "warning": "local-vs-exchange wall clock alignment unverified"}
    try:
        with tempfile.TemporaryDirectory(prefix="mm_gap_g0_fill_") as folder:
            folder = Path(folder)
            paths = {kind: folder/f"{kind}.csv" for kind in SCHEMAS}
            counts = {kind: extract_window(kind, day, end_us, paths[kind],
                                          max_rows=3_000_000) for kind in SCHEMAS}
            report["okx_feeds"] = {kind: {"rows": v["rows"]} for kind, v in counts.items()}
            ext, exmid = read_exchange_bbo(paths["book_ticker"])
            ot, om = local_okx_bbo(paths["book_ticker"])
            b_local, bm = read_binance_bbo(day, end_us)
            bt = visible_timestamps(b_local, EXTRA_BINANCE_MS)
            report["cross_venue_bbo_rows"] = {"okx": int(len(ot)),
                                              "binance": int(len(bt))}
            report["thresholds"] = strict_warmup_thresholds(
                ot, om, bt, bm, start_us*1000)
            dest = folder/"fused.npz"
            events = tardis.convert_fuse(
                str(paths["trades"]), str(paths["incremental_book_L2"]),
                str(paths["book_ticker"]), tick_size=TICK, lot_size=LOT,
                output_filename=str(dest),
                ss_buffer_size=max(1000, counts["incremental_book_L2"]["snapshot_rows"]+100),
                snapshot_mode="process")
            validate_event_order(events)
            report["fused_events"] = int(len(events))
            del events
            lower, upper = float(exmid.min()) - 1000, float(exmid.max()) + 1000
            g0 = run_mode(dest, lower, upper, 50, True,
                          gamma_code=GAMMA, capture_fills=True)
            fills = g0.pop("fill_trace")
            if len(fills) != g0["trading"]["engine_fills"]:
                raise RuntimeError("Real official maker fill trace count mismatch")
            report["official_g0"] = g0["trading"]
            report["bin_diagnostic"] = analyze(
                fills, ext, exmid, ot, om, bt, bm, report["thresholds"])
            if report["bin_diagnostic"]["n_official_fills"] != g0["trading"]["engine_fills"]:
                raise RuntimeError("Fill count mismatch after gap join")
            report["status"] = "complete"
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        target.write_text(json.dumps(report, ensure_ascii=False, indent=2)+"\n",
                          encoding="utf-8")
        print("G0 gap/fill markout", day, report["status"], target, flush=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--day", default="2026-11-01",
                   choices=("2026-06-01", "2026-07-01", "2026-08-01",
                            "2026-09-01", "2026-10-01", "2026-11-01"))
    p.add_argument("--report-dir", default="reports")
    a = p.parse_args()
    run(a.day, a.report_dir)


if __name__ == "__main__":
    main()
