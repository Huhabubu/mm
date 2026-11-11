"""Real 15min event-driven G0-R beta=0 matched-clock execution diagnostic.

All trading and matching use hftbacktest 2.4.4. First 10m warmup, next 5m
trading, one OKX tape, same Binance trigger clock in both paired modes.
No return is presented as a daily return or Binance signal improvement.
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
from g0_glft_baseline import LOT, TICK, TRAIN, STEP_NS, GAMMA
from g0_glft_rolling import asset_for, run_mode
from g0_glft_event_driven import run_event_glft, summarize


def run_pair(npz, low, high, b_ts, b_mid, minutes, rolling=True):
    from hftbacktest import ROIVectorMarketDepthBacktest
    result = []
    for read_binance in (False, True):
        engine = ROIVectorMarketDepthBacktest([asset_for(npz, low, high)])
        try:
            raw = run_event_glft(engine, b_ts, b_mid, minutes * 600,
                                 rolling, GAMMA, read_binance)
            result.append(summarize(raw, minutes, rolling))
        finally:
            engine.close()
    first, second = result
    if first["time_count"]["binance"] <= 0:
        raise RuntimeError("No Binance signals reached trading/simulation clock")
    if second["time_count"]["binance_reads"] != second["time_count"]["binance"]:
        raise RuntimeError("Binance diagnostic reads do not match event count")
    # Only purely diagnostic reads change; strategy never touches Binance mids.
    check = json.loads(json.dumps(second))
    check["time_count"]["binance_reads"] = first["time_count"]["binance_reads"]
    if first != check:
        raise RuntimeError("beta=0 paired executions diverged on identical event clocks")
    if first["calibration_attempts"] != 1 + minutes * 60 // 5:
        raise RuntimeError("5s parameter update schedule was not preserved")
    return result


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--day", default="2026-11-01")
    p.add_argument("--minutes", type=int, default=5, choices=(5, 50))
    p.add_argument("--extra-binance-delay-ms", type=int, default=25)
    p.add_argument("--report-dir", default="reports")
    args = p.parse_args()
    if datetime.strptime(args.day, "%Y-%m-%d").day != 1:
        p.error("Free historical Tardis day must be first of month")

    day_start = int(datetime.strptime(args.day, "%Y-%m-%d").replace(
        tzinfo=timezone.utc).timestamp()) * 1_000_000
    # Extra buffer ensures hftbacktest can finish at least 9000 100ms samples
    # even if the tape begins a few milliseconds after day_start.
    end_us = day_start + (600 + args.minutes*60)*1_000_000 + 500_000
    report = {"status": "started", "day": args.day, "minutes": args.minutes,
              "extra_binance_delay_ms": args.extra_binance_delay_ms,
              "beta": 0, "not_a_new_holdout": True, "not_daily_pnl": True}
    out = Path(args.report_dir)/f"g0_event_driven_{args.day}_{args.minutes}m.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    try:
        from hftbacktest.data.utils import tardis
        from hftbacktest.data.validation import validate_event_order
        with tempfile.TemporaryDirectory(prefix="mm_event_g0_") as folder:
            folder = Path(folder)
            paths = {kind: folder/f"{kind}.csv" for kind in SCHEMAS}
            counts = {kind: extract_window(kind, args.day, end_us, paths[kind],
                                          max_rows=2_500_000) for kind in SCHEMAS}
            npz = folder/"fused.npz"
            events = tardis.convert_fuse(
                str(paths["trades"]), str(paths["incremental_book_L2"]),
                str(paths["book_ticker"]), tick_size=TICK, lot_size=LOT,
                output_filename=str(npz),
                ss_buffer_size=max(1000, counts["incremental_book_L2"]["snapshot_rows"] + 100),
                snapshot_mode="process")
            validate_event_order(events)
            event_rows = int(len(events))
            del events
            low, high = math.inf, -math.inf
            with paths["book_ticker"].open(encoding="utf-8", newline="") as stream:
                for row in csv.DictReader(stream):
                    low = min(low, float(row["bid_price"]))
                    high = max(high, float(row["ask_price"]))
            b_ts_local, b_mid = read_binance_bbo(args.day, end_us)
            b_visible = visible_timestamps(b_ts_local, args.extra_binance_delay_ms)
            results = run_pair(npz, low, high, b_visible, b_mid, args.minutes)
            legacy = run_mode(npz, low, high, args.minutes, True)
            for key in ("A_per_second", "k_per_real_tick",
                        "sigma_tick_per_sqrt_second",
                        "half_spread_ticks", "skew_ticks_per_contract"):
                current_key = "k_per_tick" if key == "k_per_real_tick" else key
                got = results[0]["initial_parameters"][current_key]
                expected = legacy["initial_parameters"][key]
                if not math.isclose(got, expected, rel_tol=1e-10, abs_tol=1e-10):
                    raise RuntimeError(
                        f"100ms calibration drift: {key} event={got}, legacy={expected}")
            report["legacy_100ms_G0_R_trading"] = legacy["trading"]
            report["initial_parameters_equal_to_legacy"] = True
            report.update({"status": "complete", "okx_market_counts": counts,
                           "okx_fused_rows": event_rows,
                           "binance_bbo_rows": int(len(b_visible)),
                           "modes": {"beta_zero_no_signal_read": results[0],
                                     "beta_zero_read_only_binance": results[1]},
                           "paired_equal_except_diagnostic_reads": True})
    except Exception as e:
        report["status"] = "failed"
        report["error"] = f"{type(e).__name__}: {e}"
        raise
    finally:
        out.write_text(json.dumps(report, ensure_ascii=False, indent=2)+"\n",
                       encoding="utf-8")
        print("G0 event-driven:", report["status"], out, flush=True)


if __name__ == "__main__":
    main()
