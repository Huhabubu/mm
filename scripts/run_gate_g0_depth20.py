"""Gate BTC-USDT perpetual G0 vs G0 depth-20 exploratory replay.

Same rolling GLFT estimator and execution as existing multivenue runner.
The 20-level rule counts nonempty price levels, not 20 tick increments.
"""
import argparse
import csv
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from check_okx_hft_data import extract_window, require_unshifted_target_clock
from g0_glft_event_driven import run_event_glft, summarize
from run_multivenue_glft import Market, build_asset, order_scenario

GATE = Market("gate-io-futures", "BTC_USDT", "BTC",
              .1, 1., .0001, .0002, .0005,
              "2026-11-15 Gate live spec; fee scenarios held at OKX 2/5bps")
KINDS = ("trades", "incremental_book_L2")


def feed_cadence(path):
    """Measure unique receive timestamps; individual L2 rows are NOT messages."""
    times = []
    previous = None
    with open(path, encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            t = int(row["local_timestamp"])
            if t != previous:
                times.append(t)
                previous = t
    d = np.diff(np.asarray(times, dtype=np.int64))
    return {
        "distinct_local_timestamps": len(times),
        "interarrival_ms_p50": float(np.percentile(d, 50) / 1000) if len(d) else None,
        "interarrival_ms_p90": float(np.percentile(d, 90) / 1000) if len(d) else None,
        "interarrival_ms_p99": float(np.percentile(d, 99) / 1000) if len(d) else None,
    }


def run(day, minutes, out):
    from hftbacktest import ROIVectorMarketDepthBacktest
    from hftbacktest.data.utils import tardis
    from hftbacktest.data.validation import validate_event_order

    start_us = int(datetime.strptime(day, "%Y-%m-%d").replace(
        tzinfo=timezone.utc).timestamp()) * 1_000_000
    end_us = start_us + (600 + 60 * minutes) * 1_000_000 + 5_000_000
    report = {"day": day, "venue": "gate-io-futures", "symbol": "BTC_USDT",
              "warmup_minutes": 10, "trade_minutes": minutes,
              "mode": "event-driven rolling GLFT (100ms observation, 5s refit)",
              "per_side_min_action_ms": 20, "order_latency_ms": 1,
              "response_latency_ms": 1, "fee_scenario": {
                  "maker": GATE.maker_fee, "taker": GATE.taker_fee},
              "gate_contract": {"tick_usdt_per_btc": GATE.tick, "lot_contracts": GATE.lot,
                                "btc_per_contract": GATE.ctval},
              "depth20_definition": "20th nonzero displayed price level on each side",
              "new_data_not_blind_test": True}
    with tempfile.TemporaryDirectory(prefix="gate_depth20_") as tmp:
        root = Path(tmp)
        paths = {}
        counts = {}
        for kind in KINDS:
            p = root / (kind + ".csv")
            paths[kind] = str(p)
            counts[kind] = extract_window(
                kind, day, end_us, p, max_rows=2_500_000,
                exchange=GATE.exchange, symbol=GATE.symbol)
        require_unshifted_target_clock(counts)
        report["source_counts"] = counts
        report["depth_cadence"] = feed_cadence(paths["incremental_book_L2"])
        events = tardis.convert(
            [paths["trades"], paths["incremental_book_L2"]],
            output_filename=str(root / "gate.npz"),
            buffer_size=counts["trades"]["rows"] + counts["incremental_book_L2"]["rows"] + 2000,
            ss_buffer_size=max(2000, counts["incremental_book_L2"]["snapshot_rows"] + 100),
            snapshot_mode="process")
        validate_event_order(events)
        report["hbt_market_events"] = len(events)
        # Prices only; do not use hidden future data in strategy parameter calibration.
        positive_px = events["px"][events["px"] > 0]
        lo, hi = float(np.min(positive_px)), float(np.max(positive_px))
        del events
        qty_mid = 0.5 * (lo + hi)
        scenario = order_scenario(GATE, qty_mid)
        report["order_scenario"] = scenario
        result = {}
        for name, cap in (("raw_glft", 0), ("depth20_glft", 20)):
            engine = ROIVectorMarketDepthBacktest([
                build_asset(root / "gate.npz", lo, hi, GATE)])
            counters = np.zeros(12, dtype=np.int64)
            try:
                raw = run_event_glft(
                    engine, np.empty(0, dtype=np.int64), np.empty(0, dtype=np.float64),
                    minutes * 600, rolling=True,
                    gamma_code=scenario["gamma_code"], read_binance=False,
                    beta=0.0, tick_size=GATE.tick, contract_value=GATE.ctval,
                    order_qty=scenario["order_qty"],
                    position_cap=scenario["position_cap"],
                    depth_cap_levels=cap, depth_cap_stats=counters if cap else None)
                if int(raw[5]) != 1:
                    result[name] = {"status": "no_glft_calibration",
                                    "fit_attempts": int(raw[2]),
                                    "fit_rejected": int(raw[3])}
                else:
                    result[name] = summarize(
                        raw, minutes, True, 0.0,
                        scenario["position_cap"], scenario["order_qty"])
                    result[name]["status"] = "ok"
                if cap:
                    total, bv, av, bo, ao, bm, am, fixed, bf, af, bfv, afv = map(int, counters)
                    result[name]["depth20_diagnostic"] = {
                        "quote_decisions": total,
                        "bid_valid_level20": bv, "ask_valid_level20": av,
                        "bid_raw_beyond_level20": bo, "ask_raw_beyond_level20": ao,
                        "bid_level20_missing": bm, "ask_level20_missing": am,
                        "bid_beyond_fraction_of_valid": bo / bv if bv else None,
                        "ask_beyond_fraction_of_valid": ao / av if av else None,
                        "fixed_100ms_decisions": fixed,
                        "bid_fixed_valid": bfv, "ask_fixed_valid": afv,
                        "bid_fixed_beyond": bf, "ask_fixed_beyond": af,
                        "bid_fixed_beyond_fraction": bf / bfv if bfv else None,
                        "ask_fixed_beyond_fraction": af / afv if afv else None}
            finally:
                engine.close()
        report["variants"] = result
    dest = Path(out)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    for name, v in result.items():
        print("GATE_G0_RESULT", name, json.dumps(v, ensure_ascii=False), flush=True)
    print("GATE_DATA_COUNTS", json.dumps(counts), flush=True)
    print("GATE_DEPTH_CADENCE", json.dumps(report["depth_cadence"]), flush=True)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", default="2026-11-01")
    parser.add_argument("--minutes", type=int, default=50)
    parser.add_argument("--output", default="reports/gate_btc_g0_depth20_2026-11-01.json")
    args = parser.parse_args()
    if args.minutes not in (5, 50):
        parser.error("Only 5 and 50 minutes supported for comparison with existing runs")
    run(args.day, args.minutes, args.output)
