"""Read-only protocol and short HftBacktest checks on a verified Tokyo archive window."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
from pathlib import Path, PurePosixPath
import tempfile
import time

import numpy as np

import data_coverage as coverage
import hourly_incremental_qa as archive
import qa_convert as convert

from tokyo_l2_protocol_qa import (
    BINANCE, GATE, BYBIT, BITGET, Book, Chain, Depth, ProtocolReport,
    depth_event, rows, symbol_key, verify_gate_sizes,
)

FOLLOWERS = (GATE, BYBIT, BITGET)
BTC = "BTCUSDT"
NS = 1_000_000_000


def download(remote: str, rel: str, path: Path) -> Path:
    archive._download_with_retry(remote, rel, path, retries=3, sleeper=time.sleep)
    return path


def selected_archives(remote, config, run_id, begin, end, tmp, warmup_windows, report):
    step = coverage.configured_partition_minutes(config) * 60
    earliest = begin - step * warmup_windows
    day = datetime.fromtimestamp(earliest, timezone.utc).date().isoformat()
    candidates, discovery, warnings, _, index_batch = archive._discover_verified_candidates(
        remote, config, since_date=day, since_start_s=earliest, run_id_filter=run_id,
        download_retries=3, sleeper=time.sleep,
    )
    if discovery != "archive_manifest_index" or index_batch != run_id:
        raise ValueError("verified archive index does not match the requested collection batch")
    candidates = [c for c in candidates if c["planned_end_s"] <= end]
    target = [c for c in candidates if c["planned_start_s"] == begin]
    expected = set(coverage.enabled_exchanges(config))
    if set(c["exchange"] for c in target) != expected:
        raise ValueError("target window does not have verified files for every enabled exchange")
    if sum(int(c["data_bytes"] or 0) for c in candidates) > 3 * 1024**3:
        raise ValueError("archive sample exceeds the compressed input budget")
    report["warmup_inputs"] = [
        {"exchange": c["exchange"], "window_id": coverage.window_descriptor(c["planned_start_s"], step // 60)["window_id"]}
        for c in candidates if c["planned_start_s"] < begin
    ]
    report["target_inputs"] = [
        {"exchange": c["exchange"], "manifest_sha256": c["manifest_sha256"], "data_sha256": c["data_sha256"]}
        for c in target
    ]
    for i, c in enumerate(candidates):
        dest = tmp / str(i)
        dest.mkdir()
        rel = PurePosixPath(c["source_manifest"])
        data = PurePosixPath(c["source_data"])
        if rel.parts[0] != "raw" or ".." in rel.parts or rel.is_absolute():
            raise ValueError("invalid path in verified index")
        if str(data) != str(rel).removesuffix(".manifest.json") + ".tsv.zst":
            raise ValueError("index data and manifest paths do not match")
        mf_path = download(remote, str(rel), dest / rel.name)
        data_path = download(remote, str(data), dest / data.name)
        manifest = json.loads(mf_path.read_bytes())
        archive._verify_three_way_index_manifest_data(
            cand=c, manifest_path=mf_path, manifest=manifest, data_path=data_path, run_id_filter=run_id,
        )
        yield c, manifest, data_path


def load_btc_specs(remote: str, start_ns: int, tmp: Path) -> dict:
    names = archive._list_meta_files(remote)
    result = {}
    for exchange in FOLLOWERS:
        # The specification already observed by the time the sample starts.
        options = sorted(
            (name for name in names if name.startswith(f"specs/{exchange}_") and name.endswith(".meta.json")),
            reverse=True,
        )
        for name in options:
            if PurePosixPath(name).parent != PurePosixPath("specs"):
                continue
            stamp = PurePosixPath(name).name[len(exchange) + 1 :].split("_", 1)[0]
            seen_s = datetime.strptime(stamp, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc).timestamp()
            if seen_s * NS > start_ns:
                continue
            meta = json.loads(download(remote, "meta/" + name, tmp / "instrument.meta.json").read_bytes())
            if meta["exchange"] != exchange or int(meta["reply_wall_ns"]) > start_ns:
                continue
            raw_name = meta["raw_file"]
            if Path(raw_name).name != raw_name or not raw_name.startswith(exchange + "_"):
                raise ValueError("invalid instrument snapshot path")
            raw = download(remote, "meta/specs/" + raw_name, tmp / "instrument.json").read_bytes()
            if hashlib.sha256(raw).hexdigest() != meta["sha256"]:
                raise ValueError("instrument snapshot digest mismatch")
            result[exchange] = btc_spec(exchange, json.loads(raw))
            break
    return result


def btc_spec(exchange, raw):
    if exchange == GATE:
        row = next(x for x in raw if x["name"] == "BTC_USDT")
        # The REST minimum is a minimum order size. For a decimal contract use a
        # documented explicit step when available, retaining the source of the field.
        step = row.get("order_size_step") or row.get("order_size_round")
        lot = step or (1 if not row.get("enable_decimal") else row["order_size_min"])
        return dict(tick=row["order_price_round"], lot=lot, multiplier=row["quanto_multiplier"],
                    order_qty=row["order_size_min"], lot_source="step" if step else "minimum_order_size")
    if exchange == BYBIT:
        row = next(x for p in raw["pages"] for x in p["result"]["list"] if x["symbol"] == BTC)
        return dict(tick=row["priceFilter"]["tickSize"], lot=row["lotSizeFilter"]["qtyStep"], multiplier=1,
                    order_qty=row["lotSizeFilter"]["minOrderQty"], lot_source="qtyStep")
    row = next(x for x in raw["data"] if (x.get("symbol") or x.get("instId")) == BTC)
    return dict(tick=row["priceMultiplier"], lot=row["quantityMultiplier"], multiplier=1,
                order_qty=row["minOrderQty"], lot_source="quantityMultiplier")


def trading_event(exchange, obj):
    if exchange == GATE and obj.get("channel") == "futures.trades" and obj.get("event") == "update":
        data = obj.get("result", [])
        return data if isinstance(data, list) else [data]
    if exchange == BYBIT and obj.get("topic", "").startswith("publicTrade."):
        return obj.get("data", [])
    arg = obj.get("arg") or {}
    if exchange == BITGET and (arg.get("topic") or arg.get("channel")) == "publicTrade":
        return obj.get("data", [])
    return []


def follower_trade(exchange, obj, wall, buffer):
    record = {"kind": "DATA", "recv_wall_ns": wall, "payload": json.dumps(obj)}
    ingest = {GATE: convert._ingest_gate_record, BYBIT: convert._ingest_bybit_record,
              BITGET: convert._ingest_bitget_record}[exchange]
    key = (exchange, BTC, buffer.date_utc)
    ingest(record, buffer.date_utc, {key: buffer})


def on_grid(value, tick):
    try:
        decimal = Decimal(str(value))
        return decimal.is_finite() and decimal > 0 and decimal % tick == 0
    except InvalidOperation:
        return False


def scan(items, config, start_ns, end_ns, sample_end, specs, summary):
    chains, book_proofs = {}, ProtocolReport()
    expected_depths = {
        (exchange, symbol_key(symbol))
        for exchange, symbol, channel in coverage.expected_streams(config)
        if coverage.classify_channel(exchange, channel) == coverage.CHANNEL_CATEGORY_DEPTH
    }
    buffers = {ex: convert.AssetDayBuffers(ex, BTC, convert.date_utc_from_wall_ns(start_ns)) for ex in FOLLOWERS}
    books = {ex: Book() for ex in FOLLOWERS}
    seeded = set()
    signal = []
    file_reports = []
    quotes_before = None
    seen_anchors = set()
    trade_ids = set()
    for candidate, mf, path in sorted(items, key=lambda item: (
        item[0]["exchange"], item[0]["planned_start_s"], item[1].get("start_wall_ns") or 0,
        item[1].get("first_rx_seq") or 0, item[1]["file_name"],
    )):
        exchange = candidate["exchange"]
        if candidate["planned_start_s"] * NS == start_ns:
            audited = archive.check_partition(path, mf, config)
            file_reports.append({"exchange": exchange, "qa_status": audited["qa_status"],
                                 "problem_count": len(audited["problems"])})
        for wall, conn, epoch, kind, obj in rows(path):
            if not wall < end_ns:
                continue
            in_target = wall >= start_ns
            if sample_end > start_ns and exchange == BINANCE and kind != "META" and wall < sample_end:
                data = obj.get("data", obj)
                if data.get("e") == "bookTicker" and data.get("s") == BTC:
                    bid, ask = float(data["b"]), float(data["a"])
                    if not (math.isfinite(bid) and math.isfinite(ask) and 0 < bid < ask):
                        if in_target:
                            book_proofs.count(exchange, BTC, "invalid_signal_quote")
                    else:
                        quote = (wall, (bid + ask) / 2)
                        if in_target:
                            signal.append(quote)
                        else:
                            quotes_before = quote
            event = depth_event(exchange, mf["boot_id"], conn, epoch, kind, obj)
            if event is not None and (exchange, event.symbol) in expected_depths:
                chain = chains.setdefault((exchange, event.symbol), Chain())
                if in_target:
                    book_proofs.frames[(exchange, event.symbol)] += 1
                if sample_end > start_ns and exchange in FOLLOWERS and event.symbol == BTC and in_target and exchange not in seeded:
                    seeded.add(exchange)
                    if chain.seq is not None and chain.connection == event.connection and event.mode != "snapshot":
                        book = books[exchange]
                        if book.bids and book.asks and not book.crossed():
                            convert._emit_depth_batch(buffers[exchange], True, start_ns - 2, start_ns - 1,
                                                      list(book.bids.items()), list(book.asks.items()))
                if event.mode == "snapshot":
                    seen_anchors.add((exchange, event.symbol, "target" if in_target else "warmup"))
                problem = chain.apply(exchange, event)
                if not problem and exchange == GATE and not verify_gate_sizes(event):
                    problem = "gate_quantity_not_decimal_string"
                if problem:
                    if in_target:
                        book_proofs.count(exchange, event.symbol, problem)
                    if sample_end > start_ns and exchange in books and event.symbol == BTC:
                        books[exchange] = Book()
                    continue
                if sample_end > start_ns and exchange in books and event.symbol == BTC:
                    book = books[exchange]
                    book.apply(event)
                    if book.crossed() and in_target:
                        book_proofs.count(exchange, BTC, "crossed_book")
                    if in_target and wall < sample_end and exchange in specs:
                        tick = Decimal(str(specs[exchange]["tick"]))
                        if tick <= 0:
                            raise ValueError("invalid instrument tick size")
                        if any(not on_grid(x[0], tick) for level in (event.bids, event.asks) for x in level or []):
                            book_proofs.count(exchange, BTC, "off_grid_depth_price")
                        ms = int(event.exchange_ms)
                        if ms <= 0:
                            book_proofs.count(exchange, BTC, "invalid_exchange_time")
                        else:
                            convert._emit_depth_batch(buffers[exchange], event.mode == "snapshot", ms * 1_000_000,
                                                      wall, convert._parse_price_qty_pairs(event.bids or []),
                                                      convert._parse_price_qty_pairs(event.asks or []))
            elif in_target and kind != "META" and exchange in FOLLOWERS:
                trades = trading_event(exchange, obj)
                for trade in trades:
                    sym = symbol_key(str(trade.get("s") or trade.get("contract") or
                                         (obj.get("arg") or {}).get("symbol") or
                                         (obj.get("arg") or {}).get("instId") or obj.get("topic", "").split(".")[-1]))
                    tid = trade.get("id") if exchange == GATE else trade.get("i") or trade.get("tradeId")
                    if tid is None:
                        book_proofs.count(exchange, sym, "missing_trade_id")
                        continue
                    k = (exchange, sym, str(tid))
                    if k in trade_ids:
                        book_proofs.count(exchange, sym, "duplicate_trade_id")
                    trade_ids.add(k)
                if wall < sample_end and trades and exchange in specs:
                    # Current per-message feeds generally share one symbol. Preserve
                    # trade identity and converter handling for the representative symbol.
                    last_sym = symbol_key(str(trades[-1].get("s") or trades[-1].get("contract") or
                                             (obj.get("arg") or {}).get("symbol") or
                                             (obj.get("arg") or {}).get("instId") or obj.get("topic", "").split(".")[-1]))
                    if last_sym == BTC:
                        follower_trade(exchange, obj, wall, buffers[exchange])
    for exchange, symbol in expected_depths:
        if not book_proofs.frames[(exchange, symbol)]:
            book_proofs.count(exchange, symbol, "no_depth_frames")
    if quotes_before:
        signal.insert(0, quotes_before)
    signal.sort(key=lambda row: row[0])
    summary["observed_snapshot_markets"] = [
        {"exchange": ex, "symbol": sym, "phase": phase} for ex, sym, phase in sorted(seen_anchors)
    ]
    summary["archive_file_checks"] = file_reports
    summary["protocol"] = book_proofs.json()
    summary["protocol_status"] = "failed" if book_proofs.issues else "passed"
    summary["replay"] = {}
    if sample_end <= start_ns:
        return
    # Only numeric counts, errors and statuses are exported. The converted inputs
    # stay in the job's temporary directory and expire with the runner.
    for exchange in FOLLOWERS:
        if exchange not in coverage.enabled_exchanges(config):
            continue
        if exchange not in specs:
            summary["replay"][exchange] = {"status": "blocked", "reason": "missing_effective_instrument_spec"}
            continue
        if any(e == exchange and s == BTC for e, s, _ in book_proofs.issues):
            summary["replay"][exchange] = {"status": "blocked", "reason": "representative_symbol_failed_protocol"}
            continue
        if len(signal) < 2 or not buffers[exchange].seen_snapshot or not buffers[exchange].hbt_rows:
            summary["replay"][exchange] = {"status": "blocked", "reason": "missing_signal_or_initial_book"}
            continue
        try:
            summary["replay"][exchange] = run_replay(exchange, buffers[exchange], signal, specs[exchange], sample_end)
        except Exception as exc:
            summary["replay"][exchange] = {
                "status": "failed", "reason": "converter_or_engine_error", "error_type": type(exc).__name__,
            }


def run_replay(exchange, buffer, signal, spec, sample_end):
    import sys

    sys.path.insert(0, str(Path(convert.__file__).resolve().parents[2] / "scripts"))
    from g0_glft_baseline import STEP_NS, TRAIN

    if sample_end - int(buffer.hbt_rows[0][2]) <= TRAIN * STEP_NS:
        return {"status": "blocked", "reason": "sample_too_short_for_strategy_warmup",
                "strategy_warmup_seconds": TRAIN * STEP_NS // NS}
    tick, lot, multiplier = (float(spec[k]) for k in ("tick", "lot", "multiplier"))
    if not all(math.isfinite(x) and x > 0 for x in (tick, lot, multiplier)):
        return {"status": "blocked", "reason": "invalid_contract_spec"}
    raw = np.array(buffer.hbt_rows, dtype=convert.event_dtype)
    ordered, diagnostics = convert._order_and_validate_hbt_events(
        raw, exchange=exchange, symbol=BTC, date_utc=buffer.date_utc, stream_kind="real_archive_smoke",
    )
    with tempfile.TemporaryDirectory(prefix="tokyo_real_hbt_") as directory:
        depth = Path(directory) / "depth.npz"
        bbo = Path(directory) / "signal.npz"
        np.savez_compressed(depth, data=ordered)
        np.savez_compressed(
            bbo, local_ts_ns=np.array([row[0] for row in signal], dtype=np.int64),
            mid_px=np.array([row[1] for row in signal], dtype=np.float64),
        )
        order_qty = max(float(spec["order_qty"]), lot)
        result = convert.run_offline_e2e_hbt_replay(
            depth, bbo, tick_size=tick, lot_size=lot, contract_value=multiplier,
            order_qty=order_qty, position_cap=5 * order_qty, trade_steps=6000, stop_at_ns=sample_end,
        )
    metrics = result["metrics"]
    ok = (
        result["status"] == "ok" and metrics["target_feed_events"] > 0 and metrics["binance_events"] > 0
        and metrics["final_equity"] is not None and math.isfinite(metrics["final_equity"])
    )
    return {"status": "passed" if ok else "failed", "symbol": BTC, "source_rows": len(raw),
            "converted_rows": len(ordered), "feed_events": metrics["target_feed_events"],
            "signal_events": metrics["binance_events"], "submits": metrics["submits"],
            "cancels": metrics["cancels"], "fills": metrics["engine_fills"],
            "raw_exchange_reversals": diagnostics["raw_exch_ts_reversals"],
            "lot_source": spec["lot_source"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--remote", required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--window", required=True)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--warmup-windows", type=int, default=1, choices=range(0, 9))
    parser.add_argument("--replay-seconds", type=int, default=660, choices=range(0, 3601))
    args = parser.parse_args()
    out = {"status": "failed", "run_id": args.run_id, "window_id": args.window,
           "warmup_windows": args.warmup_windows, "replay_sample_seconds": args.replay_seconds}
    try:
        config = json.loads(args.config.read_text())
        pm = coverage.configured_partition_minutes(config)
        begin = coverage.parse_window_start_s(args.window)
        if coverage.window_descriptor(begin, pm)["planned_start_s"] != begin:
            raise ValueError("target window must align to the configured partition length")
        end = begin + pm * 60
        if end > time.time():
            raise ValueError("target window has not ended")
        with tempfile.TemporaryDirectory(prefix="tokyo_archive_qa_") as directory:
            tmp = Path(directory)
            specs = load_btc_specs(args.remote, begin * NS, tmp) if args.replay_seconds else {}
            items = list(selected_archives(args.remote, config, args.run_id, begin, end, tmp, args.warmup_windows, out))
            scan(items, config, begin * NS, end * NS, min(end, begin + args.replay_seconds) * NS, specs, out)
        passed = out["protocol_status"] == "passed" and all(
            x["qa_status"] == "passed" for x in out["archive_file_checks"]
        ) and all(x["status"] == "passed" for x in out["replay"].values())
        out["status"] = "passed" if passed else "blocked"
    except (ValueError, KeyError, OSError, RuntimeError, StopIteration) as exc:
        # External execution errors can contain a credential or a data value. Keep logs structural.
        out["error_type"] = type(exc).__name__
        if isinstance(exc, ValueError) and not any(c.isdigit() for c in str(exc)):
            out["reason"] = str(exc)[:200]
    args.summary.parent.mkdir(parents=True, exist_ok=True)
    args.summary.write_text(json.dumps(out, indent=2) + "\n")
    print(json.dumps(out, indent=2))
    return 0 if out["status"] == "passed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
