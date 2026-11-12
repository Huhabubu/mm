"""Paired GLFT event replay at beta=0 vs beta=1 using Tardis causal basis.

The beta-one model uses the FULL normalized Binance-minus-OKX gap as quote
centre shift, never fitted future price labels. All official HBT trading
and order latency are the existing frozen event-driven G0-R implementation.
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
from g0_glft_baseline import CTVAL, GAMMA, LOT, TICK, TRAIN
from g0_glft_rolling import asset_for
from g0_glft_event_driven import run_event_glft, summarize
from gap_event_regression import event_gap_samples

NS = 1_000_000_000
MAX_QUOTE_AGE_NS = 250_000_000


def read_local_bbo(path):
    ts, mid = [], []
    with Path(path).open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            bid, ask = float(row["bid_price"]), float(row["ask_price"])
            if not (math.isfinite(bid) and math.isfinite(ask) and 0 < bid < ask):
                raise ValueError("Invalid native OKX BBO")
            ts.append(int(row["local_timestamp"]) * 1000)
            mid.append((bid + ask) / 2.0)
    t = np.asarray(ts, dtype=np.int64)
    m = np.asarray(mid, dtype=np.float64)
    if len(t) == 0 or np.any(np.diff(t) < 0):
        raise ValueError("Empty or unordered OKX local BBO")
    return t, m


def causal_60s_basis(okx_time, okx_mid, bin_time, bin_mid, basis_seconds=60):
    """Match gap_event_regression's strictly prior 1sec grid basis exactly.

    The value at integer grid second g refers to both venues' updates STRICTLY
    before g; it becomes available on/after g. Event strategy uses grid < now
    for conservative exact-second tie treatment.
    """
    if basis_seconds < 2:
        raise ValueError("Need at least two 1s basis samples")
    start = ((max(int(okx_time[0]), int(bin_time[0])) + NS - 1) // NS) * NS
    end = min(int(okx_time[-1]), int(bin_time[-1]))
    grid = np.arange(start, end + 1, NS, dtype=np.int64)
    if len(grid) < basis_seconds:
        raise ValueError("No full 60sec basis window")
    oi = np.searchsorted(okx_time, grid, side="left") - 1
    bi = np.searchsorted(bin_time, grid, side="left") - 1
    osafe, bsafe = np.maximum(oi, 0), np.maximum(bi, 0)
    valid = ((oi >= 0) & (bi >= 0) &
             (grid-okx_time[osafe] <= MAX_QUOTE_AGE_NS) &
             (grid-bin_time[bsafe] <= MAX_QUOTE_AGE_NS))
    spreads = np.where(valid, bin_mid[bsafe]-okx_mid[osafe], 0.0)
    sums = np.r_[0.0, np.cumsum(spreads, dtype=np.float64)]
    counts = np.r_[0, np.cumsum(valid.astype(np.int64))]
    width = basis_seconds
    n = counts[width:] - counts[:-width]
    coverage = n >= int(np.ceil(.75*width))
    basis = np.full(len(grid), np.nan, dtype=np.float64)
    basis[width-1:] = np.where(
        coverage, (sums[width:]-sums[:-width]) / np.maximum(n, 1), np.nan)
    return grid, basis


def quote_gap_coverage(okx_ts, okx_mid, bin_ts, bin_mid, basis_ts, basis,
                       first, last):
    """Audit 100ms decisions only; no future information or return labels."""
    clock = np.arange(first, last, 100_000_000, dtype=np.int64)
    oo = np.searchsorted(okx_ts, clock, side="left") - 1
    bb = np.searchsorted(bin_ts, clock, side="left") - 1
    gg = np.searchsorted(basis_ts, clock, side="left") - 1
    osafe, bsafe, gsafe = [np.maximum(v, 0) for v in (oo, bb, gg)]
    good = (oo >= 0) & (bb >= 0) & (gg >= 0)
    good &= clock - okx_ts[osafe] <= MAX_QUOTE_AGE_NS
    good &= clock - bin_ts[bsafe] <= MAX_QUOTE_AGE_NS
    good &= np.isfinite(basis[gsafe])
    gaps = bin_mid[bsafe[good]] - okx_mid[osafe[good]] - basis[gsafe[good]]
    return {
        "audit_interval_ms": 100,
        "total_clock_points": int(len(clock)),
        "usable_gap_points": int(good.sum()),
        "usable_gap_fraction": float(np.mean(good)),
        "mean_abs_gap_usdt_per_btc": float(np.mean(np.abs(gaps))) if len(gaps) else None,
        "p90_abs_gap_usdt_per_btc": float(np.quantile(np.abs(gaps), .9)) if len(gaps) else None,
        "fraction_abs_gap_at_least_one_tick": float(np.mean(np.abs(gaps) >= TICK)) if len(gaps) else None,
        "largest_abs_gap_usdt_per_btc": float(np.max(np.abs(gaps))) if len(gaps) else None,
    }


def pretrade_gap_thresholds(ot, om, bt, bm, basis_t, basis, day_start_ns):
    """Pre-fixed absolute gap P50/P90 from first 10min, on a 100ms time grid."""
    start = day_start_ns + 61*NS
    stop = day_start_ns + 600*NS - 100_000_000
    clock = np.arange(start, stop, 100_000_000, dtype=np.int64)
    oi = np.searchsorted(ot, clock, side="left") - 1
    bi = np.searchsorted(bt, clock, side="left") - 1
    gi = np.searchsorted(basis_t, clock, side="left") - 1
    osafe, bsafe, gsafe = [np.maximum(v, 0) for v in (oi, bi, gi)]
    good = (oi >= 0) & (bi >= 0) & (gi >= 0)
    good &= clock-ot[osafe] <= MAX_QUOTE_AGE_NS
    good &= clock-bt[bsafe] <= MAX_QUOTE_AGE_NS
    good &= np.isfinite(basis[gsafe])
    gaps = np.abs(bm[bsafe[good]] - om[osafe[good]] - basis[gsafe[good]])
    if len(gaps) < 300:
        raise RuntimeError("Insufficient pretrade 100ms gap observations to set filter")
    p50, p90 = np.quantile(gaps, [0.5, 0.9])
    if not (np.isfinite(p50) and np.isfinite(p90) and 0 <= p50 < p90):
        raise RuntimeError("No separable pretrade gap quantiles")
    return {
        "history_cutoff_ns_exclusive": int(stop),
        "training_clock_samples": int(len(gaps)),
        "abs_gap_p50_usdt_per_btc": float(p50),
        "abs_gap_p90_usdt_per_btc": float(p90),
        "activate_ticks": float(p90/TICK),
        "release_ticks": float(p50/TICK),
        "refresh_min_change_ticks": 10.0,
        "refresh_min_interval_ms": 250.0,
        "widen_control_extra_half_ticks": float(p90/TICK),
    }


def run_pair(npz, low, high, bt, bm, ot, om, basis_t, basis, trade_minutes,
             experiment=False, thresholds=None, wall_search_ticks=0,
              stats_out=None):
    from hftbacktest import ROIVectorMarketDepthBacktest, Recorder
    from hftbacktest.stats import (LinearAssetRecord, Ret, MaxDrawdown,
                                  NumberOfTrades, TradingValue,
                                  ReturnOverTrade, MaxPositionValue)

    specifications = [("beta_0", 0.0, 0.0, 0.0),
                      ("beta_1", 1.0, 0.0, 0.0)]
    if experiment:
        if thresholds is None:
            raise ValueError("Placebo/filter experiment requires pretrade thresholds")
        specifications.extend([
            ("beta_minus_1", -1.0, 0.0, 0.0),
            ("beta_1_filtered", 1.0, thresholds["activate_ticks"], 0.0),
            ("beta_minus_1_filtered", -1.0, thresholds["activate_ticks"], 0.0),
            # This control uses the SAME pretrade threshold to widen GLFT
            # half-spreads, without any directional Binance information.
            ("no_signal_widen_p90", 0.0, 0.0,
             thresholds["widen_control_extra_half_ticks"]),
        ])
    if wall_search_ticks > 0:
        # One common L2 post-processor applied to BOTH beta variants.
        # Zero wall_search_ticks keeps every historical default bit-identical.
        specifications.extend([
            ("beta_0_wall", 0.0, 0.0, 0.0),
            ("beta_1_wall", 1.0, 0.0, 0.0),
        ])
    variants = {}
    for name, beta, enter, extra_half in specifications:
        engine = ROIVectorMarketDepthBacktest([asset_for(npz, low, high)])
        recorder = (Recorder(engine.num_assets, trade_minutes * 600 + 100)
                    if stats_out is not None else None)
        try:
            raw = run_event_glft(
                engine, bt, bm, trade_minutes*600,
                True, GAMMA, True, beta, ot, om, basis_t, basis,
                enter, (thresholds["release_ticks"] if enter else 0.0),
                (thresholds["refresh_min_change_ticks"] if enter else 10.0),
                250_000_000, extra_half, None,
                wall_search_ticks if name.endswith("_wall") else 0,
                recorder.recorder if recorder is not None else None)
            variants[name] = summarize(
                raw, trade_minutes, True, beta=beta)
            if recorder is not None:
                data = recorder.get(0)
                result = (LinearAssetRecord(data).contract_size(CTVAL)
                          .resample("1ns")
                          .stats([Ret, MaxDrawdown, NumberOfTrades, TradingValue,
                                  ReturnOverTrade, MaxPositionValue]))
                numbers = result.summary().to_dicts()[-1]
                first = float(data[0]["balance"] + CTVAL * data[0]["position"]
                              * data[0]["price"] - data[0]["fee"])
                last = float(data[-1]["balance"] + CTVAL * data[-1]["position"]
                             * data[-1]["price"] - data[-1]["fee"])
                # Native 100ms statistics must reconcile to the engine final PnL.
                # Resampling to 1s can omit a final mid-price update.
                if abs(numbers["Return"] - variants[name]["trading"]["end_net_marked_equity_usdt"]) > 1e-6:
                    raise RuntimeError(f"Official HBT Return does not reconcile for {name}")
                stats_out[name] = {
                    "library": "hftbacktest.stats.LinearAssetRecord",
                    "resample": "1ns (preserve raw timestamp resolution)", "contract_size_btc_per_contract": CTVAL,
                    "record_samples": int(len(data)),
                    "recorded_first_equity_usdt": first,
                    "recorded_last_equity_usdt": last,
                    "engine_net_equity_usdt": variants[name]["trading"]["end_net_marked_equity_usdt"],
                    "end_equity_reconciliation_usdt": last - variants[name]["trading"]["end_net_marked_equity_usdt"],
                    "metrics": {k: (float(v) if isinstance(v, (int, float))
                                    else (None if v is None else str(v)))
                                for k, v in numbers.items()
                                if k not in ("start", "end")},
                }
        finally:
            engine.close()
    base, test = variants["beta_0"], variants["beta_1"]
    if base["time_count"]["binance_reads"] != base["time_count"]["binance"]:
        raise RuntimeError("Baseline did not consume all Binance BBO events")
    # Order responses and OKX feed can be consumed in the SAME HBT clock step.
    # Therefore counts of HBT feed wakeups vary ENDOGENOUSLY across strategies.
    # Only Binance's exogenous event stream and the 100ms calibration calendar
    # must be identical; the underlying OKX market tape is the same NPZ.
    for name, candidate in variants.items():
        for clock_field in ("binance", "binance_reads"):
            if base["time_count"][clock_field] != candidate["time_count"][clock_field]:
                raise RuntimeError(f"Changed external event clocks for {name}: {clock_field}")
        if base["samples"] != candidate["samples"] or base["decisions"] == 0:
            raise RuntimeError(f"Changed 100ms observation calendar for {name}")
        if base["calibration_trace"] != candidate["calibration_trace"]:
            raise RuntimeError(f"Changed causal GLFT calibration history for {name}")
    return variants


def run(day, minutes=50, delay_ms=25, report_dir="reports",
        experiment=False, wall_search_ticks=0, with_hbt_stats=False):
    from hftbacktest.data.utils import tardis
    from hftbacktest.data.validation import validate_event_order
    if minutes not in (5, 50):
        raise ValueError("Only validated 5 or 50 trading-minute windows")
    day_start_us = int(datetime.strptime(day, "%Y-%m-%d").replace(
        tzinfo=timezone.utc).timestamp()) * 1_000_000
    end_us = day_start_us + (600 + minutes*60) * 1_000_000 + 500_000
    label = ("glft_local_peak" if wall_search_ticks > 0 else
             "gap_filter_placebo" if experiment else "beta_one_direct_gap")
    if wall_search_ticks < 0 or wall_search_ticks > 1000:
        raise ValueError("Wall range must be between 1 and 1000 ticks, or disabled at 0")
    if experiment and wall_search_ticks:
        raise ValueError("Keep placebos and L2 wall as separate experiments")
    target = Path(report_dir)/f"{label}_{day}_{minutes}m.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "status": "started", "date_utc": day,
        "warmup_minutes": 10, "trade_minutes": minutes,
        "extra_binance_local_feed_delay_ms": delay_ms,
        "same_native_bbo_clocks_and_order_latency": True,
        "beta_1_hypothesis": "full Binance-OKX abnormal spread is OKX quote-centre shift",
        "no_beta_fitting": True, "not_new_blind_holdout": True,
        "not_daily_pnl": True,
    }
    try:
        with tempfile.TemporaryDirectory(prefix="mm_beta_one_") as tmp:
            root = Path(tmp)
            paths, counts = {}, {}
            for kind in SCHEMAS:
                paths[kind] = root/f"{kind}.csv"
                counts[kind] = extract_window(
                    kind, day, end_us, paths[kind], max_rows=2_500_000)
            npz = root/"fused.npz"
            fused = tardis.convert_fuse(
                str(paths["trades"]), str(paths["incremental_book_L2"]),
                str(paths["book_ticker"]), tick_size=TICK, lot_size=LOT,
                output_filename=str(npz),
                ss_buffer_size=max(1000, counts["incremental_book_L2"]["snapshot_rows"] + 100),
                snapshot_mode="process")
            validate_event_order(fused)
            n_events = len(fused)
            del fused
            ot, om = read_local_bbo(paths["book_ticker"])
            b_local, bm = read_binance_bbo(day, end_us)
            bt = visible_timestamps(b_local, delay_ms)
            base_t, base = causal_60s_basis(ot, om, bt, bm)
            base_valid = int(np.count_nonzero(np.isfinite(base)))
            if base_valid == 0:
                raise RuntimeError("No causal Binance/OKX historical basis")
            audit = quote_gap_coverage(
                ot, om, bt, bm, base_t, base,
                day_start_us*1000 + 600*NS, day_start_us*1000 + (600+minutes*60)*NS)
            if audit["usable_gap_points"] == 0:
                raise RuntimeError("No possible online beta-one signals")
            low, high = float(np.min(om))-1000, float(np.max(om))+1000
            thresholds = (pretrade_gap_thresholds(
                ot, om, bt, bm, base_t, base, day_start_us*1000)
                if experiment else None)
            stats_out = {} if with_hbt_stats else None
            result = run_pair(
                npz, low, high, bt, bm, ot, om, base_t, base, minutes,
                experiment=experiment, thresholds=thresholds,
                wall_search_ticks=wall_search_ticks,
                stats_out=stats_out)
            report.update({
                "status": "complete",
                "okx_fused_event_rows": int(n_events),
                "okx_native_bbo_rows": int(len(ot)),
                "binance_bbo_rows": int(len(bt)),
                "basis_valid_1sec_points": base_valid,
                "gap_visibility_audit_100ms": audit,
                "variants": result,
                "pretrade_filter_thresholds": thresholds,
                "experiment": ("liquidity_wall" if wall_search_ticks > 0 else
                               "placebo_filter" if experiment else "beta_one_direct"),
                "wall_search_ticks": wall_search_ticks,
                "wall_anchor_method": ("first_strict_local_usdt_notional_peak"
                                       if wall_search_ticks > 0 else "disabled"),
                "wall_search_width_usdt_per_btc": wall_search_ticks*TICK,
                "paired_net_difference_beta1_minus_beta0_usdt": (
                    result["beta_1"]["trading"]["end_net_marked_equity_usdt"] -
                    result["beta_0"]["trading"]["end_net_marked_equity_usdt"]),
                "glft_5sec_calibration_trace_identical": True,
            })
            if stats_out is not None:
                report["hbt_builtin_stats"] = stats_out
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        target.write_text(json.dumps(report, ensure_ascii=False, indent=2)+"\n",
                          encoding="utf-8")
        print("Direct beta-one gap event-driven GLFT", day, report["status"], target,
              flush=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--day", default="2026-11-01")
    p.add_argument("--minutes", type=int, default=5, choices=(5, 50))
    p.add_argument("--extra-binance-delay-ms", type=int, default=25)
    p.add_argument("--report-dir", default="reports")
    p.add_argument("--experiment", choices=("base", "placebo_filter", "wall_anchor"), default="base")
    p.add_argument("--hbt-stats", action="store_true",
                   help="Read-only official Recorder + LinearAssetRecord statistics")
    p.add_argument("--wall-search-ticks", type=int, default=0,
                   help="Scenario-specific bounded outward L2 tick range (1..1000); 0 disabled")
    a = p.parse_args()
    if datetime.strptime(a.day, "%Y-%m-%d").day != 1:
        p.error("Tardis free historical day must be the first of month UTC")
    if a.experiment == "wall_anchor" and a.wall_search_ticks <= 0:
        p.error("--wall-search-ticks N (1..1000) required in wall_anchor mode")
    if a.experiment != "wall_anchor" and a.wall_search_ticks:
        p.error("--wall-search-ticks can only be used with wall_anchor mode")
    run(a.day, a.minutes, a.extra_binance_delay_ms, a.report_dir,
        experiment=(a.experiment == "placebo_filter"),
        wall_search_ticks=a.wall_search_ticks,
         with_hbt_stats=a.hbt_stats)


if __name__ == "__main__":
    main()
