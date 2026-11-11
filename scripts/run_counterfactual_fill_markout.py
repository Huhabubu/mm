"""Official event-driven G0 counterfactual fill opportunity / adverse selection.

Use beta=0 order executions as the candidate opportunities, never subtract
aggregate fill counts as "protected fills". Same original OKX NPZ, Binance
event clock and official HBT 2.4.4 execution for all paired simulations.

This audit does not attribute cancellation causality: queue path, inventory,
order placement time and pending acknowledgements may differ. Ex-post OKX
exchange-time BBO is used only for 100/500ms markouts. Output aggregate only.
"""
import argparse
import json
import math
import tempfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
from hftbacktest import BUY

from check_okx_hft_data import SCHEMAS, extract_window
from cross_venue_event_probe import read_binance_bbo, visible_timestamps
from g0_fill_markout import read_exchange_bbo, evaluate_fill_markouts
from g0_glft_baseline import CTVAL, GAMMA, LOT, POSITION_CAP, ORDER_QTY, TICK, TRAIN, STEP_NS
from g0_glft_rolling import asset_for
from g0_glft_event_driven import run_event_glft, summarize
from run_beta_one_direct_gap import read_local_bbo, causal_60s_basis, pretrade_gap_thresholds

NS = 1_000_000_000
PROBE_LEAD_NS = 1_000_000
HORIZONS = (100, 500)


def extract_official_fills(raw):
    """Raw legacy slots 31:33 and nested diagnostic arrays at slot 34."""
    if len(raw) != 35 or not isinstance(raw[34], tuple):
        raise RuntimeError("Event runner missing nested official fill audit arrays")
    diagnostics = raw[34]
    ts, side, observed, price, qty = (np.asarray(raw[31], np.int64),
                                       np.asarray(raw[32], np.int64),
                                       np.asarray(diagnostics[1], np.int64),
                                       np.asarray(diagnostics[2], np.float64),
                                       np.asarray(diagnostics[3], np.float64))
    if not (len(ts) == len(side) == len(observed) == len(price) == len(qty)):
        raise RuntimeError("Official fills and local observation lengths disagree")
    if (np.any(ts <= 0) or np.any(price <= 0) or np.any(qty <= 0) or
            np.any(observed <= 0)):
        raise RuntimeError("Unusable official fill timestamp/price/quantity")
    if np.any(observed < ts):
        # Different source clocks can make this occur. Preserve, but flag.
        pass
    return [
        {"exch_timestamp_ns": int(ts[i]),
         "local_observed_ns": int(observed[i]),
         "side_sign": 1 if side[i] == BUY else -1,
         "execution_price_usdt_per_btc": float(price[i]),
         "executed_qty_contracts": float(qty[i])}
        for i in range(len(ts))
    ]


def match_same_exchange_time_side(baseline, variant):
    """One-to-one multiset pairing; same-clock-side != identical order/queue."""
    remaining = Counter((f["exch_timestamp_ns"], f["side_sign"])
                        for f in variant)
    mask = np.zeros(len(baseline), dtype=bool)
    for i, f in enumerate(baseline):
        key = (f["exch_timestamp_ns"], f["side_sign"])
        if remaining[key] > 0:
            mask[i] = True
            remaining[key] -= 1
    return mask


def prefill_probes(baseline, first_ns, end_ns):
    """Two independent possible time origins to expose cross-clock ambiguity.

    Nominal exchange-match minus 1ms treated on local HBT decision time;
    alternatively locally observed fill minus 1ms. Neither alone proves
    a cancellation was acknowledged before the exchange matched the trade.
    """
    t_exchange = np.array(
        [f["exch_timestamp_ns"]-PROBE_LEAD_NS for f in baseline], np.int64)
    t_local = np.array(
        [f["local_observed_ns"]-PROBE_LEAD_NS for f in baseline], np.int64)
    both = np.concatenate((t_exchange, t_local))
    use = (both >= first_ns) & (both < end_ns)
    unique = np.unique(both[use])
    return unique, t_exchange, t_local


def probe_state_class(f, record):
    """Descriptive local state, never proof a specific cancelled order avoided fill."""
    if record is None or not np.isfinite(record[6]):
        return "missing_snapshot"
    side = f["side_sign"]
    px = float(record[0] if side == 1 else record[1])
    count = float(record[2] if side == 1 else record[3])
    pos = float(record[6])
    if np.isfinite(px):
        baseline_px_tick = f["execution_price_usdt_per_btc"] / TICK
        # Buy bid less aggressive => lower; sell ask less aggressive => higher.
        if side * (px - baseline_px_tick) < -0.5:
            return "less_aggressive_cancellable_quote"
        return "same_or_more_aggressive_cancellable_quote"
    if count > 0:
        return "order_present_not_cancellable"
    if ((side == 1 and pos + ORDER_QTY > POSITION_CAP+1e-9) or
            (side == -1 and pos - ORDER_QTY < -POSITION_CAP-1e-9)):
        return "inventory_cap_no_quote"
    return "no_local_quote"


def snapshot_rows_at(probe_times, probe_rows, lookup):
    if len(lookup) == 0:
        return []
    idx = np.searchsorted(probe_times, lookup, side="left")
    result = []
    for i, x in enumerate(lookup):
        if idx[i] < len(probe_times) and probe_times[idx[i]] == x:
            row = probe_rows[idx[i]]
            result.append(row if np.isfinite(row[6]) else None)
        else:
            result.append(None)
    return result


def markouts(fills, exchange_times, exchange_mids):
    if not fills:
        return {"n_fills": 0, "horizons": {}}
    return evaluate_fill_markouts(
        fills, exchange_times, exchange_mids, horizons_ms=HORIZONS)


def describe_coverage(baseline):
    n = len(baseline)
    if not n:
        return {"n": 0}
    latency_ms = np.array(
        [(f["local_observed_ns"]-f["exch_timestamp_ns"])/1e6 for f in baseline])
    return {
        "n": n,
        "fraction_local_observation_not_before_exchange_timestamp": float(
            np.mean(latency_ms >= 0.0)),
        "median_local_observation_minus_exchange_ms": float(np.median(latency_ms)),
        "p05_ms": float(np.quantile(latency_ms, .05)),
        "p95_ms": float(np.quantile(latency_ms, .95)),
        "min_ms": float(np.min(latency_ms)),
        "max_ms": float(np.max(latency_ms))
    }


def compare_fill_opportunities(baseline, variant, probe_times, probe_rows,
                               ex_time, ex_mid):
    same = match_same_exchange_time_side(baseline, variant)
    baseline_only = [f for i, f in enumerate(baseline) if not same[i]]
    shared = [f for i, f in enumerate(baseline) if same[i]]
    variant_only = len(variant)-len(shared)
    if variant_only < 0 or len(baseline_only)+len(shared) != len(baseline):
        raise RuntimeError("Fill pairing accounting failed")
    exchange_query = np.array(
        [f["exch_timestamp_ns"]-PROBE_LEAD_NS for f in baseline], np.int64)
    local_query = np.array(
        [f["local_observed_ns"]-PROBE_LEAD_NS for f in baseline], np.int64)
    states_exchange = [probe_state_class(f, p)
                       for f, p in zip(baseline, snapshot_rows_at(
                           probe_times, probe_rows, exchange_query))]
    states_local = [probe_state_class(f, p)
                    for f, p in zip(baseline, snapshot_rows_at(
                        probe_times, probe_rows, local_query))]
    n_disagree = sum(
        a != b for a, b, kept in zip(states_exchange, states_local, same)
        if not kept)
    both_available = sum(
        (a != "missing_snapshot" and b != "missing_snapshot")
        for a, b, kept in zip(states_exchange, states_local, same)
        if not kept)
    groups = {
        "baseline_all": baseline,
        "baseline_only_no_same_time_side_fill": baseline_only,
        "baseline_shared_same_time_side_fill": shared,
        "variant_all": variant,
        "baseline_only_buy": [f for i, f in enumerate(baseline)
                              if not same[i] and f["side_sign"] == 1],
        "baseline_only_sell": [f for i, f in enumerate(baseline)
                               if not same[i] and f["side_sign"] == -1]
    }
    details = {}
    for label in set(states_exchange+states_local):
        if label == "missing_snapshot":
            continue
        subset = [f for i, f in enumerate(baseline)
                  if not same[i] and states_exchange[i] == label]
        if subset:
            groups["baseline_only_exchange_probe_"+label] = subset
        subset_local = [f for i, f in enumerate(baseline)
                        if not same[i] and states_local[i] == label]
        if subset_local:
            groups["baseline_only_local_probe_"+label] = subset_local
    for name, sample in groups.items():
        details[name] = markouts(sample, ex_time, ex_mid)
    summary = {
        "baseline_fill_count": len(baseline),
        "variant_fill_count": len(variant),
        "same_exact_exchange_time_and_side_matched_fill_count": len(shared),
        "baseline_only_fill_count": len(baseline_only),
        "variant_only_fill_count": variant_only,
        "note": "Matched means same exchange timestamp and side only; does not "
                "identify same order or establish causal counterfactual.",
        "probe_lag_before_candidate_fill_ms": PROBE_LEAD_NS/1e6,
        "baseline_only_exchange_probe_counts": dict(Counter(
            state for state, keep in zip(states_exchange, same) if not keep)),
        "baseline_only_local_probe_counts": dict(Counter(
            state for state, keep in zip(states_local, same) if not keep)),
        "baseline_only_probe_agreement_count": int(sum(
            a == b for a, b, keep in zip(states_exchange, states_local, same)
            if not keep)),
        "baseline_only_probe_disagreement_count": int(n_disagree),
        "baseline_only_both_probes_available_count": int(both_available),
        "caveat": "Local order state at a nominal probe time cannot verify "
                  "exchange-side cancellation effective by candidate fill time. "
                  "The two clock anchors are a sensitivity comparison.",
        "groups": details
    }
    if len(shared)+len(baseline_only) != len(baseline):
        raise RuntimeError("Baseline fill multiset reconciliation failed")
    return summary


def replay_variant(npz, low, high, bt, bm, ot, om, basis_t, basis,
                   trade_minutes, name, thresholds=None, probes=None):
    from hftbacktest import ROIVectorMarketDepthBacktest
    beta = {
        "beta_0": 0., "beta_1": 1., "beta_minus_1": -1.,
        "beta_1_filtered": 1., "no_signal_widen_p90": 0.
    }[name]
    enter = (thresholds["activate_ticks"]
             if name == "beta_1_filtered" else 0.)
    extra_half = (thresholds["widen_control_extra_half_ticks"]
                  if name == "no_signal_widen_p90" else 0.)
    engine = ROIVectorMarketDepthBacktest([asset_for(npz, low, high)])
    try:
        raw = run_event_glft(
            engine, bt, bm, trade_minutes*600, True, GAMMA, True,
            beta, ot, om, basis_t, basis,
            enter, (thresholds["release_ticks"] if enter else 0.0),
            10.0, 250_000_000, extra_half, probes)
        result = summarize(raw, trade_minutes, rolling=True, beta=beta)
    finally:
        engine.close()
    trace = extract_official_fills(raw)
    if len(trace) != result["trading"]["engine_fills"]:
        raise RuntimeError(f"{name} missing official fill trace")
    return result, trace, raw[34][4]


def run(day, minutes=50, delay_ms=25, report_dir="reports"):
    from hftbacktest.data.utils import tardis
    from hftbacktest.data.validation import validate_event_order
    if minutes not in (5, 50):
        raise ValueError("Only 5 or 50min scope verified for counterfactual audit")
    start_us = int(datetime.strptime(day, "%Y-%m-%d").replace(
        tzinfo=timezone.utc).timestamp()) * 1_000_000
    end_us = start_us + (600 + minutes*60)*1_000_000 + 1_000_000
    target = Path(report_dir)/f"counterfactual_fill_markout_{day}_{minutes}m.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "status": "started", "day_utc": day,
        "train_minutes": 10, "trade_minutes": minutes,
        "horizons_ms": list(HORIZONS),
        "scope": "exploratory_fills_50min_not_daily_or_causal_cancellation_effect",
        "same_native_okx_market_replay": True, "binance_extra_delay_ms": delay_ms,
        "order_execution_source": "official hftbacktest 2.4.4",
        "fill_time": "official Order.exch_timestamp",
        "local_fill_observation_time": "engine.current_timestamp when FILLED discovered",
        "markout_quote_time": "native OKX book_ticker.exchange timestamp",
        "no_raw_fill_trace_in_report": True
    }
    try:
        with tempfile.TemporaryDirectory(prefix="mm_counterfactual_") as tmp:
            folder = Path(tmp)
            paths = {kind: folder/f"{kind}.csv" for kind in SCHEMAS}
            count = {kind: extract_window(kind, day, end_us, paths[kind],
                                          max_rows=2_500_000)
                     for kind in SCHEMAS}
            npz = folder/"fused.npz"
            events = tardis.convert_fuse(
                str(paths["trades"]), str(paths["incremental_book_L2"]),
                str(paths["book_ticker"]), tick_size=TICK, lot_size=LOT,
                output_filename=str(npz),
                ss_buffer_size=max(1000, count["incremental_book_L2"]["snapshot_rows"]+100),
                snapshot_mode="process")
            validate_event_order(events)
            report["fused_event_rows"] = len(events)
            del events

            ex_time, ex_mid = read_exchange_bbo(paths["book_ticker"])
            ot, om = read_local_bbo(paths["book_ticker"])
            b_local, bm = read_binance_bbo(day, end_us)
            bt = visible_timestamps(b_local, delay_ms)
            basis_t, basis = causal_60s_basis(ot, om, bt, bm)
            thresholds = pretrade_gap_thresholds(
                ot, om, bt, bm, basis_t, basis, start_us*1000)
            report["pretrade_signal_thresholds"] = thresholds
            low = float(np.min(om)) - 1000
            high = float(np.max(om)) + 1000

            base_report, baseline, _ = replay_variant(
                npz, low, high, bt, bm, ot, om, basis_t, basis,
                minutes, "beta_0", thresholds)
            report["reference"] = {
                "variant": "beta_0", "trading": base_report["trading"],
                "fill_clock_alignment": describe_coverage(baseline),
                "total_baseline_fills": len(baseline),
            }
            trade_start = base_report["start_time_ns"] + TRAIN*STEP_NS
            trade_end = base_report["end_time_ns"]
            probe_times, exchange_probe, local_probe = prefill_probes(
                baseline, trade_start, trade_end)
            report["probe_counts"] = {
                "candidate_exchange_fill_minus_1ms": int(len(exchange_probe)),
                "candidate_local_observation_minus_1ms": int(len(local_probe)),
                "unique_probe_timestamps": int(len(probe_times)),
            }
            for name in ("beta_1", "beta_1_filtered", "beta_minus_1",
                         "no_signal_widen_p90"):
                variant_report, fill_trace, probe_rows = replay_variant(
                    npz, low, high, bt, bm, ot, om, basis_t, basis,
                    minutes, name, thresholds, probes=probe_times)
                if probe_rows.shape != (len(probe_times), 7):
                    raise RuntimeError("Probe order-state trace shape mismatch")
                if (variant_report["samples"] != base_report["samples"] or
                        variant_report["calibration_trace"] !=
                        base_report["calibration_trace"]):
                    raise RuntimeError(f"{name} changed original GLFT estimator")
                if (variant_report["time_count"]["binance"] !=
                        base_report["time_count"]["binance"]):
                    raise RuntimeError(f"{name} used a different Binance feed clock")
                if name == "beta_1":
                    # Pure read-only probes must not change strategy PnL/orders
                    # or the official filled orders. Check on actual replay.
                    check_report, check_fills, check_probe = replay_variant(
                        npz, low, high, bt, bm, ot, om, basis_t, basis,
                        minutes, name, thresholds, probes=None)
                    if (check_report["trading"] != variant_report["trading"] or
                            check_fills != fill_trace or
                            check_report["calibration_trace"] !=
                            variant_report["calibration_trace"]):
                        raise RuntimeError(
                            "Diagnostic probe changed beta=1 trading outcomes")
                    if check_probe.shape[0] != 0:
                        raise RuntimeError("Unprobed replay created audit rows")
                    report["probe_invariance_against_unprobed_beta1"] = True
                report.setdefault("variants", {})[name] = {
                    "trading": variant_report["trading"],
                    "fill_clock_alignment": describe_coverage(fill_trace),
                    "comparison_to_beta_0": compare_fill_opportunities(
                        baseline, fill_trace, probe_times, probe_rows,
                        ex_time, ex_mid),
                }
            report["status"] = "complete"
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        target.write_text(json.dumps(report, ensure_ascii=False, indent=2)+"\n",
                          encoding="utf-8")
        print("Counterfactual official maker fill markout", day, report["status"],
              target, flush=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--day", default="2026-11-01",
                   choices=("2026-06-01","2026-07-01","2026-08-01",
                            "2026-09-01","2026-10-01","2026-11-01"))
    p.add_argument("--minutes", type=int, default=5, choices=(5, 50))
    p.add_argument("--extra-binance-delay-ms", type=int, default=25)
    p.add_argument("--report-dir", default="reports")
    a = p.parse_args()
    run(a.day, a.minutes, a.extra_binance_delay_ms, a.report_dir)


if __name__ == "__main__":
    main()
