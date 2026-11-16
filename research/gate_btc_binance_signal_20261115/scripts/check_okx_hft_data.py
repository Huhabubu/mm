"""Small, bounded OKX Tardis -> hftbacktest compatibility probe (no orders/PnL)."""
import argparse
import csv
import gzip
import io
import json
import math
import tempfile
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

from inspect_tardis import BASE_URL

EXCHANGE = "okex-swap"
SYMBOL = "BTC-USDT-SWAP"
SCHEMAS = {
    "trades": ("exchange", "symbol", "timestamp", "local_timestamp", "id", "side", "price", "amount"),
    "incremental_book_L2": ("exchange", "symbol", "timestamp", "local_timestamp", "is_snapshot", "side", "price", "amount"),
    "book_ticker": ("exchange", "symbol", "timestamp", "local_timestamp", "ask_amount", "ask_price", "bid_price", "bid_amount"),
}


def extract_window(kind, day, end_us, target, max_rows=500_000, opener=urllib.request.urlopen,
                   exchange=EXCHANGE, symbol=SYMBOL):
    """Stream gzip and crop at the start of the day; preserve order and amount units."""
    yyyy, mm, dd = day.split("-")
    url = f"{BASE_URL}/{exchange}/{kind}/{yyyy}/{mm}/{dd}/{symbol}.csv.gz"
    fields = SCHEMAS[kind]
    stats = {"rows": 0, "snapshot_rows": 0, "initial_snapshot_bid": 0,
             "initial_snapshot_ask": 0, "incremental_rows": 0,
             "local_backwards": 0, "invalid_rows": 0, "same_local_ts": 0,
             "first_local_us": None, "last_local_us": None,
             "first_snapshot": None, "initial_snapshot_complete": False,
             "skipped_pre_snapshot": 0, "min_feed_latency_us": None,
             "negative_feed_latency_rows": 0}
    started_snapshot = False
    left_initial_snapshot = False
    last_ts = None
    with opener(urllib.request.Request(url, headers={"User-Agent": "mm-g0-compat/1.0"}), timeout=120) as response:
        with gzip.GzipFile(fileobj=response) as zipped:
            with io.TextIOWrapper(zipped, encoding="utf-8", newline="") as stream:
                reader = csv.DictReader(stream)
                if not set(fields).issubset(set(reader.fieldnames or [])):
                    raise ValueError(f"{kind}: missing columns {set(fields) - set(reader.fieldnames or [])}")
                with target.open("w", encoding="utf-8", newline="") as out:
                    writer = csv.DictWriter(out, fieldnames=fields)
                    writer.writeheader()
                    for row in reader:
                        ts = int(row["local_timestamp"])
                        if ts >= end_us:
                            break
                        if last_ts is not None:
                            if ts < last_ts:
                                stats["local_backwards"] += 1
                            if ts == last_ts:
                                stats["same_local_ts"] += 1
                        if stats["first_local_us"] is None:
                            stats["first_local_us"] = ts
                        last_ts = ts
                        stats["last_local_us"] = ts
                        if row["exchange"] != exchange or row["symbol"] != symbol:
                            raise ValueError(f"{kind}: wrong exchange or symbol")
                        if int(row["timestamp"]) <= 0:
                            stats["invalid_rows"] += 1
                        if kind == "incremental_book_L2":
                            snapshot = row["is_snapshot"].lower() == "true"
                            # Tardis can start with pre-snapshot deltas; they have no valid book state.
                            if not started_snapshot and not snapshot:
                                stats["skipped_pre_snapshot"] += 1
                                continue
                            if row["is_snapshot"].lower() not in ("true", "false"):
                                stats["invalid_rows"] += 1
                            if snapshot:
                                stats["snapshot_rows"] += 1
                                if not started_snapshot:
                                    stats["first_snapshot"] = ts
                                    started_snapshot = True
                                if not left_initial_snapshot:
                                    if row["side"] == "bid":
                                        stats["initial_snapshot_bid"] += 1
                                    elif row["side"] == "ask":
                                        stats["initial_snapshot_ask"] += 1
                            else:
                                stats["incremental_rows"] += 1
                                if started_snapshot:
                                    left_initial_snapshot = True
                            if row["side"] not in ("bid", "ask"):
                                stats["invalid_rows"] += 1
                            if float(row["price"]) <= 0 or float(row["amount"]) < 0:
                                stats["invalid_rows"] += 1
                        elif kind == "trades":
                            if row["side"] not in ("buy", "sell") or float(row["price"]) <= 0 or float(row["amount"]) <= 0:
                                stats["invalid_rows"] += 1
                        else:
                            b, a = float(row["bid_price"]), float(row["ask_price"])
                            bq, aq = float(row["bid_amount"]), float(row["ask_amount"])
                            if not all(math.isfinite(x) for x in (b, a, bq, aq)) or not (0 < b < a and bq >= 0 and aq >= 0):
                                stats["invalid_rows"] += 1
                        writer.writerow({field: row[field] for field in fields})
                        # Only rows passed to the converter count toward clock checks.
                        latency_us = ts - int(row["timestamp"])
                        old_min = stats["min_feed_latency_us"]
                        stats["min_feed_latency_us"] = (latency_us if old_min is None
                                                        else min(old_min, latency_us))
                        if latency_us < 0:
                            stats["negative_feed_latency_rows"] += 1
                        stats["rows"] += 1
                        if stats["rows"] > max_rows:
                            raise RuntimeError(f"{kind}: bounded sample exceeded {max_rows} rows")
    stats["initial_snapshot_complete"] = (started_snapshot and left_initial_snapshot
                                           and stats["initial_snapshot_bid"] > 0
                                           and stats["initial_snapshot_ask"] > 0)
    if kind == "incremental_book_L2" and not stats["initial_snapshot_complete"]:
        raise ValueError(f"{kind}: no complete opening bid/ask snapshot followed by an update: {stats}")
    if not stats["rows"] or stats["local_backwards"] or stats["invalid_rows"]:
        raise ValueError(f"{kind}: empty, nonmonotonic or invalid sample: {stats}")
    return stats


def require_unshifted_target_clock(counts):
    """Reject source clocks that HBT would shift without shifting Binance data."""
    for kind, stats in counts.items():
        latency = stats.get("min_feed_latency_us")
        if latency is None:
            raise ValueError(f"{kind}: missing raw feed latency audit")
        if latency < 0 or stats.get("negative_feed_latency_rows", 0):
            raise ValueError(
                f"{kind}: target feed has negative local-minus-exchange latency "
                f"({latency} us); HBT would shift its local clock independently "
                "of Binance signals; abort paired replay")


def probe_converters(paths, counts, tick_size, lot_size, directory):
    import hftbacktest
    import numpy as np
    from hftbacktest.data.utils import tardis
    from hftbacktest.data.validation import validate_event_order
    from hftbacktest import BacktestAsset, ROIVectorMarketDepthBacktest

    # The official converter's defaults can allocate >6GB; bound them to this crop.
    n = counts["trades"]["rows"] + counts["incremental_book_L2"]["rows"]
    snapshot_capacity = max(1000, counts["incremental_book_L2"]["snapshot_rows"] + 100)
    out = {}
    with paths["book_ticker"].open(encoding="utf-8") as stream:
        bbo_rows = list(csv.DictReader(stream))
    lower = min(float(x["bid_price"]) for x in bbo_rows) - 100
    upper = max(float(x["ask_price"]) for x in bbo_rows) + 100
    for label in ("l2_only", "fused"):
        npz = directory / f"{label}.npz"
        if label == "l2_only":
            events = tardis.convert([str(paths["trades"]), str(paths["incremental_book_L2"])],
                                    output_filename=str(npz), buffer_size=max(10_000, 3 * n),
                                    ss_buffer_size=snapshot_capacity, snapshot_mode="process")
        else:
            events = tardis.convert_fuse(str(paths["trades"]), str(paths["incremental_book_L2"]),
                                        str(paths["book_ticker"]), tick_size=tick_size,
                                        lot_size=lot_size, output_filename=str(npz),
                                        ss_buffer_size=snapshot_capacity, snapshot_mode="process")
        validate_event_order(events)
        if len(events) == 0 or not np.all(np.isfinite(events["px"])):
            raise ValueError(f"{label}: invalid converted events")
        with np.load(npz) as saved:
            restored = saved["data"]
            if restored.dtype != events.dtype or len(restored) != len(events):
                raise ValueError(f"{label}: NPZ round trip failed")
        # Configuration only to prove that replay loads; not G0 economics.
        asset = (BacktestAsset().data([str(npz)]).linear_asset(1.0)
                 .constant_order_latency(1_000_000, 1_000_000)
                 .power_prob_queue_model(2.0).no_partial_fill_exchange()
                 .trading_value_fee_model(0.0, 0.0)
                 .tick_size(tick_size).lot_size(lot_size)
                 .roi_lb(lower).roi_ub(upper).last_trades_capacity(100))
        hbt = ROIVectorMarketDepthBacktest([asset])
        try:
            replay_status = hbt.elapse(1_000_000_000)
            depth = hbt.depth(0)
            best_bid = float(depth.best_bid)
            best_ask = float(depth.best_ask)
            if replay_status != 0 or not (0 < best_bid < best_ask):
                raise ValueError(f"{label}: invalid replay BBO/status: {replay_status}, {best_bid}, {best_ask}")
        finally:
            hbt.close()
        out[label] = {"converted_events": int(len(events)), "npz_roundtrip": True,
                      "event_order_valid": True, "engine_replay_1s": True,
                      "best_bid_after_1s": best_bid, "best_ask_after_1s": best_ask}
        del events, restored
    out["hftbacktest_version"] = getattr(hftbacktest, "__version__", "2.4.4-pinned")
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", default="2026-11-01")
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument("--tick-size", type=float, default=0.1,
                        help="provisional observed tick; verify historical tickSz")
    parser.add_argument("--lot-size", type=float, default=0.01,
                        help="provisional quantity step for fusion; verify historical lotSz")
    parser.add_argument("--report-dir", default="reports")
    args = parser.parse_args()
    date = datetime.strptime(args.day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if date.day != 1 or not 1 <= args.seconds <= 300 or args.tick_size <= 0 or args.lot_size <= 0:
        parser.error("monthly free day, 1-300 seconds, positive tick/lot required")
    end_us = int((date + timedelta(seconds=args.seconds)).timestamp() * 1_000_000)
    report = {"date": args.day, "window_seconds": args.seconds, "symbol": SYMBOL,
              "scope": "data compatibility only; no G0 trading, fills or PnL",
              "conversion_params_provisional": {
                  "tick_size": args.tick_size, "lot_size_contracts": args.lot_size,
                  "contract_multiplier_verified": False,
                  "historical_instrument_spec_verified": False},
              "feeds": {}, "conversions": {}, "status": "started"}
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    output = report_dir / f"g0_okx_data_compat_{args.day}.json"
    try:
        with tempfile.TemporaryDirectory(prefix="mm_okx_g0_") as folder:
            directory = Path(folder)
            paths = {}
            for kind in SCHEMAS:
                paths[kind] = directory / f"BTC-USDT-SWAP_{kind}.csv"
                report["feeds"][kind] = extract_window(kind, args.day, end_us, paths[kind])
            report["conversions"] = probe_converters(paths, report["feeds"],
                                                       args.tick_size, args.lot_size, directory)
            report["status"] = "complete"
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"Wrote aggregate report: {output} (status: {report['status']})", flush=True)


if __name__ == "__main__":
    main()
