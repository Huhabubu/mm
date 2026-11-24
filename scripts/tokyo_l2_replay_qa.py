"""Read-only protocol and short HftBacktest checks on a verified Tokyo archive window."""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
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


def verified_candidates(remote, config, run_id, earliest, state_dir=None, *, historical=False):
    day = datetime.fromtimestamp(earliest, timezone.utc).date().isoformat()
    candidates, discovery, _, _, index_batch = archive._discover_verified_candidates(
        remote, config, since_date=day, since_start_s=earliest, run_id_filter=run_id,
        download_retries=3, sleeper=time.sleep, state_dir=state_dir,
    )
    if discovery != "archive_manifest_index" or (not historical and index_batch != run_id):
        raise ValueError("verified archive index does not match the requested collection batch")
    return [item for item in candidates if item["run_id"] == run_id] if historical else candidates


def input_digests(candidates):
    return sorted(
        ({"exchange": c["exchange"], "manifest_sha256": c["manifest_sha256"], "data_sha256": c["data_sha256"]}
         for c in candidates),
        key=lambda item: (item["exchange"], item["manifest_sha256"], item["data_sha256"]),
    )


def selected_archives(remote, config, run_id, begin, end, tmp, warmup_windows, report, candidates=None):
    step = coverage.configured_partition_minutes(config) * 60
    earliest = begin - step * warmup_windows
    if candidates is None:
        candidates = verified_candidates(remote, config, run_id, earliest)
    candidates = [c for c in candidates if earliest <= c["planned_start_s"] and c["planned_end_s"] <= end]
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
    report["target_inputs"] = input_digests(target)
    def prepare(entry):
        i, c = entry
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
        return c, manifest, data_path

    # Each download has its own destination and is verified before its rows are read.
    with ThreadPoolExecutor(max_workers=4) as pool:
        yield from pool.map(prepare, enumerate(candidates))


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


def on_grid(value, step, *, allow_zero=False):
    try:
        decimal = Decimal(str(value))
        return decimal.is_finite() and (decimal >= 0 if allow_zero else decimal > 0) and decimal % step == 0
    except InvalidOperation:
        return False


def valid_non_depth_frame(exchange, symbol, channel, obj):
    """Validate fields consumed by quote and trade replay before using a frame as evidence."""
    try:
        if channel == "bookTicker":
            record = obj.get("data", obj)
            bid, ask = float(record["b"]), float(record["a"])
        elif channel == "futures.book_ticker":
            bid, ask = float(obj["result"]["b"]), float(obj["result"]["a"])
        elif channel in {"orderbook.1", "books1"}:
            record = obj["data"] if channel == "orderbook.1" else obj["data"][0]
            bid = float((record.get("b") or record["bids"])[0][0])
            ask = float((record.get("a") or record["asks"])[0][0])
        else:
            trades = [obj.get("data", obj)] if exchange == BINANCE else trading_event(exchange, obj)
            if exchange == GATE:
                trades = [trade for trade in trades if symbol_key(trade.get("contract") or "") == symbol]
            return bool(trades) and all(
                any(trade.get(key) is not None for key in ("id", "i", "tradeId", "a"))
                and math.isfinite(float(trade.get("price") or trade.get("p") or trade.get("P") or trade["px"]))
                and float(trade.get("price") or trade.get("p") or trade.get("P") or trade["px"]) > 0
                and 0 < abs(float(trade.get("size") or trade.get("q") or trade.get("v") or trade["sz"])) < math.inf
                for trade in trades
            )
        return 0 < bid < ask < math.inf
    except (AttributeError, IndexError, KeyError, TypeError, ValueError):
        return False


class StreamEvidence:
    """Track independently scoped feed evidence while walking sealed segments."""

    def __init__(self, config):
        streams = {(ex, symbol_key(sym), ch) for ex, sym, ch in coverage.expected_streams(config)}
        self.timeline = convert.MarketAvailability(streams)
        self.by_exchange = {}
        self.depth = {}
        self.last = {}
        thresholds = coverage.channel_silence_thresholds(config)
        self.limits = {}
        for key in sorted(streams):
            ex, sym, channel = key
            self.by_exchange.setdefault(ex, []).append(key)
            category = coverage.classify_channel(ex, channel)
            if category == coverage.CHANNEL_CATEGORY_DEPTH:
                self.depth[(ex, sym)] = key
            limit = "event_trade_quiet_max" if category == coverage.CHANNEL_CATEGORY_EVENT_TRADE else category
            self.limits[key] = int(thresholds[limit] * NS)

    def interrupt(self, exchange, wall, event, symbol="ALL", endpoint=None, channel=None):
        depth_only = event in {"DEPTH_UNAVAILABLE", "SNAPSHOT_BRIDGE_RETRY"}
        status = "known_gap" if event in {"DEPTH_UNAVAILABLE", "GAP_START", "SNAPSHOT_BRIDGE_RETRY"} else "unverified"
        for key in self.by_exchange.get(exchange, ()):
            _, sym, stream_channel = key
            if channel and channel != stream_channel:
                continue
            if symbol != "ALL" and symbol != sym:
                continue
            if endpoint and endpoint != coverage.stream_owning_endpoint(exchange, stream_channel):
                continue
            if depth_only and self.depth.get((exchange, sym)) != key:
                continue
            self.last.pop(key, None)
            if self.timeline.state(key) != "known_gap" or status == "known_gap":
                self.timeline.mark(key, wall, status, event.lower())

    def observe(self, key, wall, status="verified_usable", reason="observed_stream", resume_at=None):
        if key not in self.limits:
            return
        previous = self.last.get(key)
        if previous is not None and wall < previous:
            # An earlier raw timestamp cannot move the start of a later recovery.
            return
        if previous is not None and wall - previous > self.limits[key]:
            self.timeline.mark(key, previous + 1, "unverified", "stream_silence")
        self.last[key] = wall
        self.timeline.mark(key, wall if resume_at is None else resume_at, status, reason)

    def report(self, start_ns, end_ns, invalid_exchanges):
        for key, last in self.last.items():
            if end_ns - last > self.limits[key] and self.timeline.state(key) == "verified_usable":
                self.timeline.mark(key, last + 1, "unverified", "stream_silence")
        return self.timeline.report(start_ns, end_ns, invalid_exchanges)


def scan(items, config, start_ns, end_ns, sample_end, specs, summary, checkpoint=None, source_restrictions=None):
    boundary_checkpoints = {
        (row["exchange"], row["symbol"]): Chain(
            tuple(row["connection"]), row["seq"], row["awaiting_delta"], anchored=True
        )
        for row in checkpoint or ()
    }
    checkpoint_wall = {
        (row["exchange"], row["symbol"]): row.get("last_wall_ns") for row in checkpoint or ()
    }
    chains = {}
    replay_warmup_chains = {}
    checkpoint_applied = set()
    reset_seen = set()
    unanchored_delta_counts = Counter()
    unanchored_sources = {}
    summary["boundary_checkpoint_markets"] = len(boundary_checkpoints)
    book_proofs = ProtocolReport()
    evidence = StreamEvidence(config)
    expected_depths = set(evidence.depth)
    restrictions = source_restrictions or {}
    invalid_exchanges = set(restrictions.get(start_ns // NS, ()))
    previous_end = {}
    legacy_ends = {}
    recovered_depth = set()
    depth_frames_by_mode = Counter()
    buffers = {ex: convert.AssetDayBuffers(ex, BTC, convert.date_utc_from_wall_ns(start_ns)) for ex in FOLLOWERS}
    books = {ex: Book() for ex in FOLLOWERS}
    last_depth_exchange_ns = {}
    seeded = set()
    signal = []
    file_reports = []
    quotes_before = None
    seen_anchors = set()
    missing_anchor_sources = {}
    boundary_events = ProtocolReport()
    trade_ids = set()
    for candidate, mf, path in sorted(items, key=lambda item: (
        item[0]["exchange"], item[0]["planned_start_s"], item[1].get("start_wall_ns") or 0,
        item[1].get("first_rx_seq") or 0, item[1]["file_name"],
    )):
        exchange = candidate["exchange"]
        file_start = candidate["planned_start_s"] * NS
        file_end = candidate.get("planned_end_s", candidate["planned_start_s"] + (end_ns - start_ns) // NS) * NS
        discontinuous = exchange in previous_end and previous_end[exchange] < file_start
        audited = archive.check_partition(path, mf, config)
        structural_failure = (
            bool(audited.get("structural_problems"))
            or (audited.get("coverage_incomplete") is True
                and not (file_start < start_ns and audited.get("is_startup_stub") is True))
            or exchange in restrictions.get(candidate["planned_start_s"], ())
        )
        if file_start == start_ns:
            file_reports.append({"exchange": exchange, "qa_status": audited["qa_status"],
                                 "problem_count": len(audited["problems"])})
            if structural_failure:
                invalid_exchanges.add(exchange)
            if audited.get("run_id") is None and audited.get("last_wall_ns") is not None:
                legacy_ends[exchange] = max(legacy_ends.get(exchange, start_ns), audited["last_wall_ns"] + 1)
        if discontinuous or structural_failure:
            evidence.interrupt(exchange, previous_end.get(exchange, file_start), "ARCHIVE_DISCONTINUITY")
            for key in expected_depths:
                if key[0] == exchange:
                    chains.pop(key, None)
                    replay_warmup_chains.pop(key, None)
                    recovered_depth.discard(key)
                    if file_start >= start_ns or file_end == start_ns:
                        checkpoint_applied.add(key)
            if exchange in books:
                books[exchange] = Book()
                last_depth_exchange_ns.pop(exchange, None)
        previous_end[exchange] = file_end
        if structural_failure:
            continue
        for wall, conn, epoch, kind, obj in rows(path):
            if not wall < end_ns:
                continue
            in_target = wall >= start_ns
            if kind == "META" and obj.get("event") in {
                "LOCAL_LOSS", "GAP_START", "DEPTH_UNAVAILABLE", "SNAPSHOT_BRIDGE_RETRY", "SWITCH",
            }:
                event_name = obj["event"]
                symbol = symbol_key(str(obj.get("symbol") or "ALL"))
                endpoint = obj.get("endpoint")
                reported_since = obj.get("unavailable_since_wall_ns") or obj.get("start_wall_ns") or wall
                evidence.interrupt(exchange, min(wall, int(reported_since)), event_name, symbol, endpoint)
                # Binance trades and public depth use independent connections.
                if (
                    exchange == BINANCE
                    and obj.get("endpoint") == "binance_market"
                    and event_name in {"SWITCH", "GAP_START", "LOCAL_LOSS"}
                ):
                    continue
                if in_target:
                    boundary_events.count(exchange, symbol, event_name)
                for ex, sym in expected_depths:
                    if ex == exchange and (symbol == "ALL" or symbol == sym):
                        key = (ex, sym)
                        recovered_depth.discard(key)
                        if in_target:
                            checkpoint_applied.add(key)
                            reset_seen.add(key)
                            chains.pop(key, None)
                        elif key not in boundary_checkpoints:
                            reset_seen.add(key)
                            chains.pop(key, None)
                        replay_warmup_chains.pop(key, None)
                        if ex in books and sym == BTC:
                            books[ex] = Book()
                            last_depth_exchange_ns.pop(ex, None)
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
            if kind != "META":
                for ex, sym, channel in coverage._data_streams(exchange, obj):
                    stream = (ex, symbol_key(sym), channel)
                    if stream in evidence.limits and coverage.classify_channel(exchange, channel) != coverage.CHANNEL_CATEGORY_DEPTH:
                        if valid_non_depth_frame(ex, stream[1], channel, obj):
                            evidence.observe(stream, wall)
                        else:
                            evidence.interrupt(ex, wall, "INVALID_STREAM_FRAME", sym, channel=channel)
            event = depth_event(exchange, mf.get("boot_id") or mf["file_name"], conn, epoch, kind, obj)
            if event is not None and (exchange, event.symbol) in expected_depths:
                key = (exchange, event.symbol)
                stream = evidence.depth[key]
                # The checksum validates file identity, while this validates the
                # levels and native sequence before publishing an availability span.
                payload_issue = archive._validate_depth_book_sides(
                    event.bids, event.asks, required=event.mode == "snapshot",
                    require_non_empty=event.mode == "snapshot", require_string_size=exchange == GATE,
                    ctx=exchange,
                )
                if not in_target and key in boundary_checkpoints:
                    # Preserve the verified boundary checkpoint at start_ns; only warm up BTC book for replay.
                    if event.mode == "snapshot":
                        seen_anchors.add((exchange, event.symbol, "warmup", event.connection[1], event.connection[2]))
                    if sample_end > start_ns and exchange in books and event.symbol == BTC:
                        w_chain = replay_warmup_chains.setdefault(key, Chain())
                        if not payload_issue and w_chain.apply(exchange, event) is None:
                            books[exchange].apply(event)
                            last_depth_exchange_ns[exchange] = int(event.exchange_ms or 0) * 1_000_000
                        else:
                            books[exchange] = Book()
                            last_depth_exchange_ns.pop(exchange, None)
                    continue

                using_boundary = in_target and key in boundary_checkpoints and key not in checkpoint_applied
                if using_boundary:
                    chains[key] = boundary_checkpoints[key]
                    checkpoint_applied.add(key)

                chain = chains.setdefault(key, Chain(anchored=False))
                if in_target:
                    book_proofs.frames[key] += 1
                    depth_frames_by_mode[(exchange, event.mode)] += 1
                if sample_end > start_ns and exchange in FOLLOWERS and event.symbol == BTC and in_target and exchange not in seeded:
                    seeded.add(exchange)
                    if chain.anchored and chain.seq is not None and chain.connection == event.connection and event.mode != "snapshot":
                        book = books[exchange]
                        previous_exchange_ns = last_depth_exchange_ns.get(exchange)
                        target_exchange_ns = int(event.exchange_ms or 0) * 1_000_000
                        if book.bids and book.asks and not book.crossed() and previous_exchange_ns is not None:
                            if previous_exchange_ns > target_exchange_ns:
                                book_proofs.count(exchange, BTC, "depth_timestamp_reversal_at_boundary")
                            else:
                                convert._emit_depth_batch(buffers[exchange], True, previous_exchange_ns, start_ns - 1,
                                                          list(book.bids.items()), list(book.asks.items()))
                consecutive_full_books = (
                    exchange == BITGET and event.mode == "snapshot" and key in recovered_depth
                    and chain.connection == event.connection and chain.seq is not None and event.seq > chain.seq
                )
                if event.mode == "snapshot":
                    seen_anchors.add((exchange, event.symbol, "target" if in_target else "warmup",
                                      event.connection[1], event.connection[2]))
                    reset_seen.discard(key)
                    problem = chain.apply(exchange, event)
                elif chain.anchored or key in reset_seen:
                    problem = chain.apply(exchange, event)
                else:
                    # Pre-existing long-lived connection whose initial snapshot predates bounded lookback.
                    # Still verify delta-to-delta sequence continuity so any real break is proven immediately.
                    if in_target:
                        unanchored_delta_counts[key] += 1
                        unanchored_sources.setdefault(key, event.connection[1:])
                    problem = chain.apply_unanchored_delta(exchange, event)

                if payload_issue:
                    problem = "invalid_depth_levels"
                    if exchange == GATE and not verify_gate_sizes(event):
                        problem = "gate_quantity_not_decimal_string"
                if problem:
                    recovered_depth.discard(key)
                    chains.pop(key, None)
                    reset_seen.add(key)
                    if evidence.timeline.state(stream) != "known_gap":
                        failure_status = "unverified" if problem == "missing_same_connection_anchor" else "known_gap"
                        evidence.timeline.mark(stream, wall, failure_status, problem)
                    evidence.last.pop(stream, None)
                    if in_target:
                        book_proofs.count(exchange, event.symbol, problem)
                        if problem == "missing_same_connection_anchor":
                            missing_anchor_sources.setdefault(key, event.connection[1:])
                    if sample_end > start_ns and exchange in books and event.symbol == BTC:
                        books[exchange] = Book()
                        last_depth_exchange_ns.pop(exchange, None)
                    continue
                if chain.anchored and chain.seq is not None:
                    if event.mode == "snapshot":
                        recovered_depth.add(key)
                    state = evidence.timeline.state(stream)
                    # After a recorded interruption, a checked delta or a newer
                    # independent Bitget book on the same connection proves recovery.
                    proved = event.mode != "snapshot" or (
                        (not chain.awaiting_delta or exchange == BITGET) and state != "known_gap"
                    ) or consecutive_full_books
                    if proved and (state != "known_gap" or key in recovered_depth):
                        prior = checkpoint_wall.get(key)
                        fits_boundary = (
                            using_boundary and key not in reset_seen and event.mode != "snapshot"
                            and prior is not None and wall - prior <= evidence.limits[stream]
                        )
                        evidence.observe(
                            stream, wall, reason="verified_depth_sequence",
                            resume_at=start_ns if fits_boundary else None,
                        )
                    if event.mode != "snapshot" or evidence.timeline.state(stream) == "verified_usable":
                        recovered_depth.discard(key)
                if chain.anchored and sample_end > start_ns and exchange in books and event.symbol == BTC:
                    book = books[exchange]
                    book.apply(event)
                    ms = int(event.exchange_ms or 0)
                    last_depth_exchange_ns[exchange] = ms * 1_000_000
                    if book.crossed() and in_target:
                        book_proofs.count(exchange, BTC, "crossed_book")
                    if in_target and wall < sample_end and exchange in specs:
                        tick = Decimal(str(specs[exchange]["tick"]))
                        if tick <= 0:
                            raise ValueError("invalid instrument tick size")
                        if any(not on_grid(x[0], tick) for level in (event.bids, event.asks) for x in level or []):
                            book_proofs.count(exchange, BTC, "off_grid_depth_price")
                        if specs[exchange]["lot_source"] != "minimum_order_size":
                            lot = Decimal(str(specs[exchange]["lot"]))
                            if lot <= 0:
                                raise ValueError("invalid instrument lot size")
                            if any(not on_grid(x[1], lot, allow_zero=True)
                                   for level in (event.bids, event.asks) for x in level or []):
                                book_proofs.count(exchange, BTC, "off_grid_depth_quantity")
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
        {"exchange": ex, "symbol": sym, "phase": phase, "connection": conn, "epoch": epoch}
        for ex, sym, phase, conn, epoch in sorted(seen_anchors)
    ]
    summary["missing_anchor_sources"] = [
        {"exchange": ex, "symbol": sym, "connection": conn, "epoch": epoch}
        for (ex, sym), (conn, epoch) in sorted(missing_anchor_sources.items())
    ]
    summary["unanchored_lookback_markets"] = [
        {
            "exchange": ex,
            "symbol": sym,
            "connection": conn,
            "epoch": epoch,
            "delta_frames_continuity_checked": unanchored_delta_counts[(ex, sym)],
            "proven_chain_breaks": book_proofs.issues.get((ex, sym, "exchange_update_chain_break"), 0),
        }
        for (ex, sym), (conn, epoch) in sorted(unanchored_sources.items())
    ]
    summary["boundary_events"] = boundary_events.json()["issues"]
    summary["archive_file_checks"] = file_reports
    summary["depth_frames_by_mode"] = [
        {"exchange": exchange, "mode": mode, "count": count}
        for (exchange, mode), count in sorted(depth_frames_by_mode.items())
    ]
    summary["protocol"] = book_proofs.json()
    summary["proven_protocol_issues"] = summary["protocol"]["issues"]
    if book_proofs.issues:
        summary["protocol_status"] = "failed"
    elif summary["unanchored_lookback_markets"]:
        summary["protocol_status"] = "anchor_unverified"
    else:
        summary["protocol_status"] = "passed"
    # Preserve structural sequence checkpoints per-market for every cleanly anchored market
    # so one failed/unanchored market never discards continuity state for healthy markets.
    for exchange, last_ns in legacy_ends.items():
        if last_ns < end_ns:
            for stream in evidence.by_exchange[exchange]:
                if evidence.timeline.state(stream) != "known_gap":
                    evidence.timeline.mark(stream, last_ns, "unverified", "legacy_actual_end")
            watchdog_ns = int(config.get("deployment", {}).get("silence_watchdog_seconds", 3) * NS)
            if end_ns - last_ns > watchdog_ns:
                for stream in evidence.by_exchange[exchange]:
                    evidence.last.pop(stream, None)
    market_availability = evidence.report(start_ns, end_ns, invalid_exchanges)
    summary["market_availability"] = market_availability
    summary["end_sequence_checkpoint"] = [
        {"exchange": ex, "symbol": sym, "connection": list(chain.connection),
         "seq": chain.seq, "awaiting_delta": chain.awaiting_delta,
         "last_wall_ns": evidence.last[evidence.depth[(ex, sym)]]}
        for (ex, sym), chain in sorted(chains.items())
        if (
            (ex, sym) in expected_depths
            and ex not in invalid_exchanges
            and book_proofs.frames[(ex, sym)] > 0
            and evidence.depth[(ex, sym)] in evidence.last
            and chain.anchored
            and (not chain.awaiting_delta or ex == BITGET)
            and (ex, sym) not in recovered_depth
            and chain.seq is not None
            and chain.connection is not None
        )
    ]
    summary["replay"] = {}
    if sample_end <= start_ns:
        return
    # A smoke replay is one independent, gap-free episode. An order resting at
    # an outage has an unknown execution outcome. Discard that entire episode,
    # without simulating a cancellation acknowledgement or carrying its position.
    for exchange in FOLLOWERS:
        if exchange not in coverage.enabled_exchanges(config):
            continue
        if exchange not in specs:
            summary["replay"][exchange] = {"status": "blocked", "reason": "missing_effective_instrument_spec"}
            continue
        if any(e == exchange and s == BTC for e, s, _ in book_proofs.issues):
            summary["replay"][exchange] = {"status": "blocked", "reason": "representative_symbol_failed_protocol"}
            continue
        required_streams = {(BINANCE, BTC, "bookTicker")} | {
            stream for stream in evidence.by_exchange[exchange]
            if stream[1] == BTC and coverage.classify_channel(exchange, stream[2]) in {
                coverage.CHANNEL_CATEGORY_DEPTH, coverage.CHANNEL_CATEGORY_EVENT_TRADE,
            }
        }
        # The smoke replay still uses the initial warmup period. A short unknown
        # prefix can be trimmed only before the first replay input, never mid-episode.
        start_replay = max(start_ns, int(buffers[exchange].hbt_rows[0][2])) if buffers[exchange].hbt_rows else start_ns
        common = convert.common_usable_intervals(market_availability, required_streams, start_replay, sample_end)
        summary.setdefault("common_replay_intervals", {})[exchange] = common
        if (exchange, BTC) in unanchored_sources and not buffers[exchange].seen_snapshot:
            summary["replay"][exchange] = {"status": "blocked", "reason": "anchor_not_in_lookback"}
            continue
        precondition = replay_precondition_reason(
            len(signal), buffers[exchange].seen_snapshot, bool(buffers[exchange].hbt_rows)
        )
        if precondition is not None:
            summary["replay"][exchange] = {"status": "blocked", "reason": precondition}
            continue
        if common != [{"start_wall_ns": start_replay, "end_wall_ns": sample_end}]:
            summary["replay"][exchange] = {
                "status": "blocked", "reason": "required_stream_interval_unverified",
                "resting_orders_policy": "censor_episode_if_execution_unobservable",
            }
            continue
        try:
            summary["replay"][exchange] = run_replay(exchange, buffers[exchange], signal, specs[exchange], sample_end)
        except Exception as exc:
            summary["replay"][exchange] = {
                "status": "failed", "reason": "converter_or_engine_error", "error_type": type(exc).__name__,
            }


def probe_orders(depth, tick, lot, multiplier, order_qty):
    # A deterministic offline probe exercises the engine independently of
    # whether the research strategy decides to quote in this particular sample.
    from hftbacktest import (
        BacktestAsset, CANCELED, FILLED, GTX, HashMapMarketDepthBacktest, LIMIT, NEW,
    )
    from hftbacktest.order import IOC

    asset = (BacktestAsset().data([str(depth)]).linear_asset(multiplier)
             .constant_order_latency(1_000_000, 1_000_000).power_prob_queue_model(2.0)
             .no_partial_fill_exchange().trading_value_fee_model(0.0, 0.0005)
             .tick_size(tick).lot_size(lot))
    engine = HashMapMarketDepthBacktest([asset])
    try:
        if engine.elapse(NS) != 0:
            return {"status": "failed", "reason": "probe_no_feed"}
        bid, ask = engine.depth(0).best_bid, engine.depth(0).best_ask
        if not math.isfinite(bid) or not math.isfinite(ask) or bid <= 100 * tick or bid >= ask:
            return {"status": "failed", "reason": "probe_invalid_market"}
        passive_buy = (round(bid / tick) - 100) * tick
        if engine.submit_buy_order(0, 1, passive_buy, order_qty, GTX, LIMIT, True) != 0:
            return {"status": "failed", "reason": "probe_submit_failed"}
        if engine.orders(0).get(1) is None or engine.orders(0).get(1).status != NEW:
            return {"status": "failed", "reason": "probe_order_unconfirmed"}
        if engine.cancel(0, 1, True) != 0 or engine.orders(0).get(1).status != CANCELED:
            return {"status": "failed", "reason": "probe_cancel_unconfirmed"}
        ask = engine.depth(0).best_ask
        marketable_buy = (round(ask / tick) + 10) * tick
        if engine.submit_buy_order(0, 2, marketable_buy, order_qty, IOC, LIMIT, True) != 0:
            return {"status": "failed", "reason": "probe_marketable_order_failed"}
        order = engine.orders(0).get(2)
        if order is None or order.status != FILLED:
            return {"status": "failed", "reason": "probe_fill_unconfirmed"}
        state = engine.state_values(0)
        ledger_ok = (
            math.isclose(engine.position(0), order_qty, rel_tol=1e-9)
            and state.trading_value > 0
            and math.isclose(-state.balance, state.trading_value, rel_tol=1e-9)
            and math.isclose(state.fee, state.trading_value * 0.0005, rel_tol=1e-9)
        )
        return {"status": "passed" if ledger_ok else "failed", "submitted": 2, "canceled": 1,
                "taker_filled": 1, "ledger_ok": ledger_ok}
    finally:
        engine.close()


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
        probe = probe_orders(depth, tick, lot, multiplier, order_qty)
        result = convert.run_offline_e2e_hbt_replay(
            depth, bbo, tick_size=tick, lot_size=lot, contract_value=multiplier,
            order_qty=order_qty, position_cap=5 * order_qty, trade_steps=6000, stop_at_ns=sample_end,
        )
    metrics = result["metrics"]
    ok = (
        result["status"] == "ok" and metrics["target_feed_events"] > 0 and metrics["binance_events"] > 0
        and probe["status"] == "passed"
        and metrics["final_equity"] is not None and math.isfinite(metrics["final_equity"])
    )
    return {"status": "passed" if ok else "failed", "symbol": BTC, "source_rows": len(raw),
            "converted_rows": len(ordered), "feed_events": metrics["target_feed_events"],
            "signal_events": metrics["binance_events"], "submits": metrics["submits"],
            "cancels": metrics["cancels"], "fills": metrics["engine_fills"],
            "raw_exchange_reversals": diagnostics["raw_exch_ts_reversals"],
            "order_probe": probe, "lot_source": spec["lot_source"]}


REPLAY_INIT_REASONS = frozenset({"initial_book_not_in_lookback", "anchor_not_in_lookback"})


def replay_precondition_reason(signal_events, seen_snapshot, has_book_rows):
    """Keep a missing signal, missing target rows and an uninitialised book as separate reasons."""
    if signal_events < 2:
        return "missing_signal"
    if not seen_snapshot:
        return "initial_book_not_in_lookback"
    if not has_book_rows:
        return "missing_target_book_rows"
    return None


def window_status(protocol_status, files_ok, replay_vals):
    """Classify a window; replay initialisation gaps are never reported as passed."""
    replay_ok = all(x["status"] == "passed" for x in replay_vals)
    replay_only_unanchored = bool(replay_vals) and all(
        x["status"] == "passed" or x.get("reason") == "anchor_not_in_lookback"
        for x in replay_vals
    )
    replay_only_init = bool(replay_vals) and not replay_ok and all(
        x["status"] == "passed" or x.get("reason") in REPLAY_INIT_REASONS
        for x in replay_vals
    )
    if protocol_status == "passed" and files_ok and replay_ok:
        return "passed"
    if protocol_status == "anchor_unverified" and files_ok and (replay_ok or replay_only_unanchored):
        return "anchor_unverified"
    if protocol_status == "passed" and files_ok and replay_only_init:
        return "replay_init_unverified"
    return "data_failed"


def audit_window(remote, config, run_id, begin, window, replay_seconds, warmup_windows,
                 candidates=None, checkpoint=None, anchor_lookback_windows=None, source_restrictions=None):
    out = {"status": "retryable", "run_id": run_id, "window_id": window,
           "warmup_windows": warmup_windows, "replay_sample_seconds": replay_seconds}
    stage = "validate_window"
    try:
        pm = coverage.configured_partition_minutes(config)
        if coverage.window_descriptor(begin, pm)["planned_start_s"] != begin:
            raise ValueError("target window must align to the configured partition length")
        end = begin + pm * 60
        if end > time.time():
            raise ValueError("target window has not ended")
        with tempfile.TemporaryDirectory(prefix="tokyo_archive_qa_") as directory:
            tmp = Path(directory)
            stage = "read_verified_archives"
            lookback = warmup_windows if anchor_lookback_windows is None else anchor_lookback_windows
            items = list(selected_archives(remote, config, run_id, begin, end, tmp, lookback, out, candidates))
            stage = "read_effective_btc_specs"
            try:
                specs = load_btc_specs(remote, begin * NS, tmp) if replay_seconds else {}
            except (KeyError, OSError, RuntimeError, StopIteration, ValueError) as exc:
                out["spec_error_type"] = type(exc).__name__
                specs = {}
            stage = "validate_archived_events"
            scan(items, config, begin * NS, end * NS, min(end, begin + replay_seconds) * NS,
                 specs, out, checkpoint=checkpoint, source_restrictions=source_restrictions)
        files_ok = all(x["qa_status"] == "passed" for x in out["archive_file_checks"])
        out["status"] = window_status(out["protocol_status"], files_ok, list(out["replay"].values()))
        if out.get("spec_error_type") or any(x["reason"] == "converter_or_engine_error"
                                             for x in out["replay"].values() if "reason" in x):
            out["status"] = "retryable"
    except Exception as exc:
        # A dependency error may include credentials or raw data. Export only its type and stage.
        out.update(error_type=type(exc).__name__, error_stage=stage)
    return out


def audit_range(remote, config, run_id, first, last):
    """Process an explicit archive batch in time order with read-only index access."""
    pm = coverage.configured_partition_minutes(config)
    step = pm * 60
    if first % step or last % step or not first <= last < first + 96 * step:
        raise ValueError("range must contain 1–96 aligned windows")
    candidates = verified_candidates(remote, config, run_id, first - step, historical=True)
    by_window = {}
    for item in candidates:
        by_window.setdefault(item["planned_start_s"], []).append(item)
    reports = []
    checkpoint = None
    for start in range(first, last + step, step):
        window = coverage.window_descriptor(start, pm)["window_id"]
        context = by_window.get(start - step, []) if start == first else []
        report = audit_window(
            remote, config, run_id, start, window, 0, int(bool(context)),
            candidates=context + by_window.get(start, []), checkpoint=checkpoint,
        )
        reports.append(report)
        checkpoint = report.get("end_sequence_checkpoint") if report["status"] != "retryable" else None
        panels = report.get("market_availability", ())
        by_status = Counter(
            (row["exchange"], status)
            for row in panels for status in {span["status"] for span in row["intervals"]}
        )
        totals = {f"{exchange}/{status}": count for (exchange, status), count in sorted(by_status.items())}
        print(f"{window} {report['status']} {json.dumps(totals, sort_keys=True)}", flush=True)
    counts = dict(Counter(report["status"] for report in reports))
    return {
        "status": "incomplete" if counts.get("retryable") else "completed",
        "run_id": run_id, "partition_minutes": pm, "counts": counts, "windows": reports,
    }


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
    config = json.loads(args.config.read_text())
    if ".." in args.window:
        first, last = map(coverage.parse_window_start_s, args.window.split("..", 1))
        run_id = args.run_id
        if run_id == "legacy-60m":
            run_id = None
            config["deployment"]["partition_minutes"] = 60
        out = audit_range(args.remote, config, run_id, first, last)
        print(json.dumps({"status": out["status"], "counts": out["counts"]}))
    else:
        window = coverage.parse_window_start_s(args.window)
        out = audit_window(args.remote, config, args.run_id, window, args.window,
                           args.replay_seconds, args.warmup_windows)
        print(json.dumps(out, indent=2))
    args.summary.parent.mkdir(parents=True, exist_ok=True)
    args.summary.write_text(json.dumps(out, indent=2) + "\n")
    return 0 if out["status"] in {"passed", "completed"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
