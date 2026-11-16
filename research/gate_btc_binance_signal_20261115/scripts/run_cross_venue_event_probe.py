"""60-second real OKX/Binance event-order smoke check, no trading or PnL."""
import argparse
import csv
import json
import math
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from check_okx_hft_data import SCHEMAS, extract_window
from cross_venue_event_probe import read_binance_bbo, replay_union, visible_timestamps
from g0_glft_baseline import LOT, TICK
from g0_glft_rolling import asset_for


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", default="2026-11-01")
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument("--delay-ms", type=int, default=25)
    parser.add_argument("--report-dir", default="reports")
    args = parser.parse_args()
    if datetime.strptime(args.day, "%Y-%m-%d").day != 1:
        parser.error("Tardis free day must be first of month")
    if not 10 <= args.seconds <= 90:
        parser.error("smoke test supports 10-90s")
    start_ns = int(datetime.strptime(args.day, "%Y-%m-%d").replace(
        tzinfo=timezone.utc).timestamp() * 1e9)
    end_ns = start_ns + args.seconds * 1_000_000_000
    report = {"status": "started", "day_utc": args.day, "seconds": args.seconds,
              "extra_signal_delay_ms": args.delay_ms,
              "no_trades_submitted": True, "not_pnl_result": True}
    path = Path(args.report_dir) / ("cross_venue_event_probe_" + args.day + ".json")
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        from hftbacktest import ROIVectorMarketDepthBacktest
        from hftbacktest.data.utils import tardis
        from hftbacktest.data.validation import validate_event_order

        with tempfile.TemporaryDirectory(prefix="mm_event_probe_") as folder:
            root = Path(folder)
            paths = {kind: root / (kind + ".csv") for kind in SCHEMAS}
            counts = {kind: extract_window(kind, args.day, end_ns // 1000,
                                          paths[kind], max_rows=700_000)
                      for kind in SCHEMAS}
            npz = root / "okx_fused.npz"
            events = tardis.convert_fuse(str(paths["trades"]), str(paths["incremental_book_L2"]),
                                         str(paths["book_ticker"]), tick_size=TICK,
                                         lot_size=LOT, output_filename=str(npz),
                                         ss_buffer_size=max(1000, counts["incremental_book_L2"]["snapshot_rows"]+100),
                                         snapshot_mode="process")
            validate_event_order(events)
            event_count = int(len(events))
            del events
            low = math.inf
            high = -math.inf
            with paths["book_ticker"].open(newline="", encoding="utf-8") as stream:
                for row in csv.DictReader(stream):
                    low = min(low, float(row["bid_price"]))
                    high = max(high, float(row["ask_price"]))
            b_ts, b_mid = read_binance_bbo(args.day, end_ns // 1000)
            visible = visible_timestamps(b_ts, args.delay_ms)
            results = []
            for use_signal in (False, True):
                engine = ROIVectorMarketDepthBacktest([asset_for(npz, low, high)])
                observed = {"valid_okx_bbo": 0, "valid_gap": 0, "stale_signal": 0}
                def on_event(kind, now, mid):
                    depth = engine.depth(0)
                    if depth.best_bid_tick > 0 and depth.best_ask_tick > depth.best_bid_tick:
                        observed["valid_okx_bbo"] += 1
                        if math.isfinite(mid):
                            i = int(np.searchsorted(visible, now, side="right")) - 1
                            if i >= 0 and 0 <= now - int(visible[i]) <= 250_000_000:
                                observed["valid_gap"] += 1
                            else:
                                observed["stale_signal"] += 1
                try:
                    stats = replay_union(engine, visible if use_signal else visible[:0],
                                         b_mid if use_signal else b_mid[:0],
                                         end_ns, on_event)
                    state = engine.state_values(0)
                    results.append({"with_binance_trigger": use_signal, "event_counts": stats,
                                    "observations": observed,
                                    "engine_trades": int(state.num_trades),
                                    "engine_position": float(state.position)})
                finally:
                    engine.close()
            plain, combined = results
            if combined["event_counts"]["binance_feed"] <= 0:
                raise RuntimeError("No visible Binance events were replayed")
            if combined["event_counts"]["okx_feed"] != plain["event_counts"]["okx_feed"]:
                raise RuntimeError("External-only feed modified official OKX feed event count")
            if combined["engine_trades"] or plain["engine_trades"]:
                raise RuntimeError("Smoke test unexpectedly traded")
            report.update({"status": "complete", "okx_market_counts": counts,
                           "fused_events": event_count,
                           "binance_bbo_rows": int(len(b_ts)), "two_replays": results})
    except Exception as e:
        report["status"] = "failed"
        report["error"] = f"{type(e).__name__}: {e}"
        raise
    finally:
        path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n",
                        encoding="utf-8")
        print("cross-venue event probe:", report["status"], path, flush=True)


if __name__ == "__main__":
    main()
