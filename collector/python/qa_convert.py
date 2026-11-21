#!/usr/bin/env python3
"""Offline QA Gatekeeper, Strict-Separation HBT 2.4.4 Converter, and E2E Replay Verifier.

Enforces Sections 3, 4.4, 5, 6.1, and 6.3 of SPEC_v1.4:
- Verifies zstd decompression, SHA-256, rx_seq & recv_mono_ns monotonicity, LOCAL_LOSS == 0,
  chrony <= 1.0 ms certification, and spec-change isolation windows.
- Strictly separates:
    1) Leader signal: Binance @bookTicker -> binance_bbo_{SYM}_{YYYYMMDD}.npz
       (diagnostics @depth@100ms and unsplit @aggTrade saved separately).
    2) Follower 50-level matching book + deduplicated trades ->
       {exchange}_l2_{SYM}_{YYYYMMDD}.npz (official hftbacktest==2.4.4 event_dtype,
       passing validate_event_order, preserving atomic WS message local_ts boundaries,
       preserving is_internal / BT / RPI / isRPI flags in `ival`).
    3) Follower independent L1 BBO -> {exchange}_l1_{SYM}_{YYYYMMDD}.npz
       (NEVER merged into or overwriting the 50-level matching book).
- Never silently drops or pads uncovered gaps or LOCAL_LOSS days: records explicit
  `execution_unverified` status in `eligibility_calendar.json` and `eligibility_calendar.csv`.
- Provides `evaluate_50_level_coverage` to tag orders/quotes beyond the real 50th non-empty
  level as `QUEUE_UNKNOWN` (NEVER `queue=0`, NEVER selectively dropping losing fills).
- Provides `run_offline_e2e_hbt_replay` integrating directly with `hftbacktest==2.4.4` and
  `Huhabubu/mm/scripts/core_glft_engine.py::run_core_glft`.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import sys
import tempfile
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import zstandard as zstd
from hftbacktest import (
    BUY_EVENT,
    DEPTH_CLEAR_EVENT,
    DEPTH_EVENT,
    DEPTH_SNAPSHOT_EVENT,
    EXCH_EVENT,
    LOCAL_EVENT,
    SELL_EVENT,
    TRADE_EVENT,
    event_dtype,
)
from hftbacktest.data.validation import correct_event_order, validate_event_order

from archive_sync import (
    ArchiveBackend,
    ArchiveLedger,
    RcloneBackend,
    restore_pruned_partition,
    verify_local_partition,
)

# Trade special flag bitmask stored in HBT event_dtype `ival`
TRADE_FLAG_IS_INTERNAL = 1 << 0  # Gate futures.trades is_internal
TRADE_FLAG_BYBIT_BT = 1 << 1     # Bybit publicTrade BT (block trade)
TRADE_FLAG_RPI = 1 << 2          # Bybit RPI / Bitget isRPI


def canonical_asset(exchange: str, raw_symbol: str) -> str:
    """Normalize exchange-native symbol to canonical base asset (e.g. BTC_USDT / BTCUSDT -> BTC)."""
    s = raw_symbol.upper()
    if s.startswith("OB."):
        parts = s.split(".")
        if len(parts) >= 2:
            s = parts[1]
    if s.endswith("_USDT"):
        return s[:-5]
    if s.endswith("USDT"):
        return s[:-4]
    return s


def date_utc_from_wall_ns(wall_ns: int) -> str:
    dt = datetime.fromtimestamp(wall_ns // 1_000_000_000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def yyyymmdd_from_date(date_utc: str) -> str:
    return date_utc.replace("-", "")


def parse_tsv_zst_records(data_path: Path) -> list[dict[str, Any]]:
    """Decompress and parse TSV-Raw lines without Base64, restoring DATA_ESCAPED verbatim."""
    dctx = zstd.ZstdDecompressor()
    records: list[dict[str, Any]] = []
    with data_path.open("rb") as fh:
        with dctx.stream_reader(fh) as reader:
            text_stream = io.TextIOWrapper(reader, encoding="utf-8", errors="strict")
            for line_no, line in enumerate(text_stream, start=1):
                trimmed = line.rstrip("\n")
                if not trimmed:
                    continue
                parts = trimmed.split("\t", 6)
                if len(parts) != 7:
                    raise ValueError(f"{data_path.name}:{line_no}: expected 7 TSV columns, got {len(parts)}")
                recv_wall_ns = int(parts[0])
                recv_mono_ns = int(parts[1])
                conn_id = parts[2]
                epoch = int(parts[3])
                rx_seq = int(parts[4])
                kind = parts[5]
                raw_col = parts[6]
                if kind == "DATA_ESCAPED":
                    payload = json.loads(raw_col)
                else:
                    payload = raw_col
                records.append(
                    {
                        "recv_wall_ns": recv_wall_ns,
                        "recv_mono_ns": recv_mono_ns,
                        "conn_id": conn_id,
                        "epoch": epoch,
                        "rx_seq": rx_seq,
                        "kind": kind,
                        "payload": payload,
                    }
                )
    return records


def load_chrony_uncertified_intervals(meta_dir: Path | None) -> dict[str, list[dict[str, Any]]]:
    """Load chrony tracking CSVs written by meta_daemon.py and return per-date uncertified (>1.0ms) samples."""
    by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    if meta_dir is None:
        return by_date
    chrony_dir = meta_dir / "chrony"
    if not chrony_dir.exists():
        return by_date
    for csv_path in sorted(chrony_dir.glob("chrony_*.csv")):
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                cert_raw = row.get("certified_le_1ms")
                if cert_raw is None:
                    cert_raw = row.get("certified_under_1ms", "")
                certified = str(cert_raw).strip().lower() == "true"
                if not certified:
                    ts_ns = int(row.get("wall_ns") or row.get("ts_utc_ns") or 0)
                    raw_stem = csv_path.stem.replace("chrony_", "")
                    fallback_date = (
                        f"{raw_stem[:4]}-{raw_stem[4:6]}-{raw_stem[6:8]}"
                        if len(raw_stem) == 8 and raw_stem.isdigit()
                        else raw_stem
                    )
                    date_str = date_utc_from_wall_ns(ts_ns) if ts_ns > 0 else fallback_date
                    err_val = row.get("estimated_error_ms") or row.get("root_bound_ms")
                    by_date[date_str].append(
                        {
                            "wall_ns": ts_ns,
                            "estimated_error_ms": float(err_val) if err_val else None,
                            "leap_status": row.get("leap_status"),
                        }
                    )
    return by_date


def load_spec_change_isolations(meta_dir: Path | None) -> dict[tuple[str, str], list[dict[str, Any]]]:
    """Load spec_changes.jsonl and *.meta.json from meta/specs/ and return map[(exchange, date_utc)] -> events."""
    isolations: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    if meta_dir is None:
        return isolations
    specs_dir = meta_dir / "specs"
    if not specs_dir.exists():
        return isolations
    changes_file = specs_dir / "spec_changes.jsonl"
    if changes_file.exists():
        for line in changes_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            ev = json.loads(line)
            if ev.get("isolation_required"):
                ts_iso = ev.get("ts_utc", "")
                if len(ts_iso) >= 10:
                    date_str = ts_iso[:10]
                elif ev.get("observed_wall_ns"):
                    date_str = date_utc_from_wall_ns(int(ev["observed_wall_ns"]))
                else:
                    date_str = "unknown"
                isolations[(ev.get("exchange", ""), date_str)].append(ev)
    else:
        for mf in sorted(specs_dir.glob("*.meta.json")):
            obj = json.loads(mf.read_text(encoding="utf-8"))
            if obj.get("spec_changed_since_previous_poll"):
                date_str = date_utc_from_wall_ns(int(obj.get("reply_wall_ns") or obj.get("query_wall_ns") or 0))
                isolations[(obj.get("exchange", ""), date_str)].append(obj)
    return isolations


@dataclass
class AssetDayBuffers:
    exchange: str
    symbol: str
    date_utc: str
    # HBT follower L2 + trades rows: tuples matching event_dtype
    hbt_rows: list[tuple[int, int, int, float, float, int, int, float]] = field(default_factory=list)
    # Independent L1 BBO rows (for followers) OR Binance @bookTicker signal
    bbo_rows: list[tuple[int, int, float, float, float, float, float]] = field(default_factory=list)
    # Binance @aggTrade diagnostic rows (unsplit)
    agg_trade_rows: list[tuple[int, int, int, int, int, float, float, int]] = field(default_factory=list)
    # Live reconstructed 50-level orderbook state for QUEUE_UNKNOWN edge tracking
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)
    seen_snapshot: bool = False
    # Dedup set for trades: (symbol, trade_id)
    seen_trade_ids: set[str] = field(default_factory=set)
    dedup_trades_dropped: int = 0
    special_flag_counts: dict[str, int] = field(
        default_factory=lambda: {"is_internal": 0, "BT": 0, "RPI": 0}
    )
    exch_ahead_of_local_count: int = 0
    edge_samples: list[tuple[int, float, float, float, float, int, int]] = field(default_factory=list)
    last_book_local_ts: int = -1

    def apply_levels(self, is_snapshot: bool, bid_levels: list[tuple[float, float]], ask_levels: list[tuple[float, float]], local_ts: int) -> None:
        if is_snapshot:
            self.bids.clear()
            self.asks.clear()
            self.seen_snapshot = True
        for px, qty in bid_levels:
            if qty <= 0.0:
                self.bids.pop(px, None)
            else:
                self.bids[px] = qty
        for px, qty in ask_levels:
            if qty <= 0.0:
                self.asks.pop(px, None)
            else:
                self.asks[px] = qty
        if self.bids and self.asks:
            sorted_bids = sorted(self.bids.keys(), reverse=True)
            sorted_asks = sorted(self.asks.keys())
            n_b = len(sorted_bids)
            n_a = len(sorted_asks)
            best_bid = sorted_bids[0]
            edge_bid = sorted_bids[min(49, n_b - 1)]
            best_ask = sorted_asks[0]
            edge_ask = sorted_asks[min(49, n_a - 1)]
            self.edge_samples.append((local_ts, best_bid, edge_bid, best_ask, edge_ask, n_b, n_a))
        self.last_book_local_ts = local_ts


_EVENT_DIRECTION_MASK = np.uint64(EXCH_EVENT | LOCAL_EVENT)
_EVENT_SEMANTIC_MASK = ~_EVENT_DIRECTION_MASK


def _find_first_order_conflict(data: np.ndarray) -> dict[str, Any] | None:
    """Locate the first exchange-time or local-time ordering violation for diagnostics."""
    exch_idx = np.flatnonzero((data["ev"] & EXCH_EVENT) == EXCH_EVENT)
    if len(exch_idx) > 1:
        bad = np.flatnonzero(np.diff(data["exch_ts"][exch_idx]) < 0)
        if len(bad) > 0:
            pos = int(bad[0])
            i0, i1 = int(exch_idx[pos]), int(exch_idx[pos + 1])
            return {
                "axis": "exch_ts",
                "prev_index": i0,
                "curr_index": i1,
                "prev_ev": hex(int(data["ev"][i0])),
                "curr_ev": hex(int(data["ev"][i1])),
                "prev_exch_ts": int(data["exch_ts"][i0]),
                "curr_exch_ts": int(data["exch_ts"][i1]),
                "prev_local_ts": int(data["local_ts"][i0]),
                "curr_local_ts": int(data["local_ts"][i1]),
                "total_exch_conflicts": int(len(bad)),
            }
    local_idx = np.flatnonzero((data["ev"] & LOCAL_EVENT) == LOCAL_EVENT)
    if len(local_idx) > 1:
        bad = np.flatnonzero(np.diff(data["local_ts"][local_idx]) < 0)
        if len(bad) > 0:
            pos = int(bad[0])
            i0, i1 = int(local_idx[pos]), int(local_idx[pos + 1])
            return {
                "axis": "local_ts",
                "prev_index": i0,
                "curr_index": i1,
                "prev_ev": hex(int(data["ev"][i0])),
                "curr_ev": hex(int(data["ev"][i1])),
                "prev_exch_ts": int(data["exch_ts"][i0]),
                "curr_exch_ts": int(data["exch_ts"][i1]),
                "prev_local_ts": int(data["local_ts"][i0]),
                "curr_local_ts": int(data["local_ts"][i1]),
                "total_local_conflicts": int(len(bad)),
            }
    return None


def _find_first_raw_reversal(raw_arr: np.ndarray, field_name: str) -> dict[str, Any] | None:
    """Record the first raw timestamp reversal (observational diagnostic only; never alters data)."""
    if len(raw_arr) < 2:
        return None
    bad = np.flatnonzero(np.diff(raw_arr[field_name]) < 0)
    if len(bad) == 0:
        return None
    i0 = int(bad[0])
    i1 = i0 + 1
    return {
        "axis": field_name,
        "prev_index": i0,
        "curr_index": i1,
        "prev_ev": hex(int(raw_arr["ev"][i0])),
        "curr_ev": hex(int(raw_arr["ev"][i1])),
        "prev_exch_ts": int(raw_arr["exch_ts"][i0]),
        "curr_exch_ts": int(raw_arr["exch_ts"][i1]),
        "prev_local_ts": int(raw_arr["local_ts"][i0]),
        "curr_local_ts": int(raw_arr["local_ts"][i1]),
        "total_reversals": int(len(bad)),
    }


def _order_and_validate_hbt_events(
    raw_arr: np.ndarray,
    *,
    exchange: str,
    symbol: str,
    date_utc: str,
    stream_kind: str,
) -> tuple[np.ndarray, dict[str, Any]]:
    """Strip direction flags, run HBT correct_event_order(), and strictly run validate_event_order().

    `hftbacktest.data.validation.correct_event_order()` assigns `EXCH_EVENT` and `LOCAL_EVENT`
    via bitwise OR when splitting out-of-order rows. Any pre-set `EXCH_EVENT | LOCAL_EVENT` bits
    on input rows must be cleared beforehand (while preserving event type and buy/sell side bits)
    so split rows do not erroneously retain both direction flags.
    """
    first_raw_exch = _find_first_raw_reversal(raw_arr, "exch_ts")
    first_raw_local = _find_first_raw_reversal(raw_arr, "local_ts")

    clean = raw_arr.copy()
    clean["ev"] = clean["ev"] & _EVENT_SEMANTIC_MASK

    ordered = correct_event_order(
        clean,
        np.argsort(clean["exch_ts"], kind="mergesort"),
        np.argsort(clean["local_ts"], kind="mergesort"),
    )
    try:
        validate_event_order(ordered)
    except ValueError as exc:
        conflict = _find_first_order_conflict(ordered)
        raise ValueError(
            f"{exc} (exchange={exchange}, symbol={symbol}, date_utc={date_utc}, "
            f"stream={stream_kind}, first_conflict={json.dumps(conflict, sort_keys=True)})"
        ) from exc

    diag = {
        "input_rows": int(len(raw_arr)),
        "ordered_rows": int(len(ordered)),
        "split_extra_rows": int(len(ordered) - len(raw_arr)),
        "raw_exch_ts_reversals": int(first_raw_exch["total_reversals"]) if first_raw_exch else 0,
        "raw_local_ts_reversals": int(first_raw_local["total_reversals"]) if first_raw_local else 0,
        "first_raw_exch_reversal": first_raw_exch,
        "first_raw_local_reversal": first_raw_local,
    }
    return ordered, diag


def _emit_depth_batch(
    buf: AssetDayBuffers,
    is_snapshot: bool,
    exch_ts_ns: int,
    local_ts_ns: int,
    bid_levels: list[tuple[float, float]],
    ask_levels: list[tuple[float, float]],
) -> None:
    """Emit atomic WS depth message into HBT event rows sharing identical (exch_ts, local_ts).

    Direction flags (`EXCH_EVENT` / `LOCAL_EVENT`) are intentionally NOT pre-set here; they are
    assigned exclusively by `hftbacktest.data.validation.correct_event_order()`.
    """
    if exch_ts_ns > local_ts_ns:
        buf.exch_ahead_of_local_count += 1
    if is_snapshot:
        buf.hbt_rows.append((DEPTH_CLEAR_EVENT, exch_ts_ns, local_ts_ns, 0.0, 0.0, 0, 0, 0.0))
        ev_type = DEPTH_SNAPSHOT_EVENT
    else:
        ev_type = DEPTH_EVENT
    for px, qty in bid_levels:
        buf.hbt_rows.append((ev_type | BUY_EVENT, exch_ts_ns, local_ts_ns, px, qty, 0, 0, 0.0))
    for px, qty in ask_levels:
        buf.hbt_rows.append((ev_type | SELL_EVENT, exch_ts_ns, local_ts_ns, px, qty, 0, 0, 0.0))
    buf.apply_levels(is_snapshot, bid_levels, ask_levels, local_ts_ns)


def _parse_price_qty_pairs(raw_levels: list[Any]) -> list[tuple[float, float]]:
    out: list[tuple[float, float]] = []
    for item in raw_levels:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            out.append((float(item[0]), float(item[1])))
    return out


def _ingest_binance_record(
    rec: dict[str, Any],
    date_utc: str,
    buffers: dict[tuple[str, str, str], AssetDayBuffers],
) -> None:
    local_ts = rec["recv_wall_ns"]
    kind = rec["kind"]
    obj = json.loads(rec["payload"])

    if kind == "META":
        snapshot = {
            "REST_SNAPSHOT": obj.get("raw"),
            "REST_SNAPSHOT_ANCHOR": obj.get("snapshot"),
        }.get(obj.get("event"))
        if isinstance(snapshot, dict):
            sym = canonical_asset("binance_futures", str(obj.get("symbol", "")))
            raw = snapshot
            exch_ts = int(raw.get("E") or raw.get("T") or (local_ts // 1_000_000)) * 1_000_000
            bids = _parse_price_qty_pairs(raw.get("bids") or [])
            asks = _parse_price_qty_pairs(raw.get("asks") or [])
            buf = buffers.setdefault(
                ("binance_futures", sym, date_utc),
                AssetDayBuffers("binance_futures", sym, date_utc),
            )
            _emit_depth_batch(buf, True, exch_ts, local_ts, bids, asks)
        return

    stream = obj.get("stream", "")
    data = obj.get("data") if isinstance(obj.get("data"), dict) else obj
    ev = data.get("e", "")

    if ev == "bookTicker" or stream.endswith("@bookTicker"):
        sym = canonical_asset("binance_futures", str(data["s"]))
        exch_ms = int(data.get("T") or data.get("E") or (local_ts // 1_000_000))
        exch_ts = exch_ms * 1_000_000
        bid_px = float(data["b"])
        bid_qty = float(data["B"])
        ask_px = float(data["a"])
        ask_qty = float(data["A"])
        mid_px = 0.5 * (bid_px + ask_px)
        buf = buffers.setdefault(
            ("binance_futures", sym, date_utc),
            AssetDayBuffers("binance_futures", sym, date_utc),
        )
        if exch_ts > local_ts:
            buf.exch_ahead_of_local_count += 1
        buf.bbo_rows.append((local_ts, exch_ts, bid_px, bid_qty, ask_px, ask_qty, mid_px))
    elif ev == "depthUpdate" or "@depth" in stream:
        sym = canonical_asset("binance_futures", str(data["s"]))
        exch_ms = int(data.get("T") or data.get("E") or (local_ts // 1_000_000))
        exch_ts = exch_ms * 1_000_000
        bids = _parse_price_qty_pairs(data.get("b") or [])
        asks = _parse_price_qty_pairs(data.get("a") or [])
        buf = buffers.setdefault(
            ("binance_futures", sym, date_utc),
            AssetDayBuffers("binance_futures", sym, date_utc),
        )
        _emit_depth_batch(buf, False, exch_ts, local_ts, bids, asks)
    elif ev == "aggTrade" or stream.endswith("@aggTrade"):
        # Preserve raw aggTrade without splitting!
        sym = canonical_asset("binance_futures", str(data["s"]))
        exch_ms = int(data.get("T") or data.get("E") or (local_ts // 1_000_000))
        exch_ts = exch_ms * 1_000_000
        agg_id = int(data["a"])
        first_id = int(data.get("f", agg_id))
        last_id = int(data.get("l", agg_id))
        px = float(data["p"])
        qty = float(data["q"])
        is_buyer_maker = 1 if bool(data.get("m", False)) else 0
        buf = buffers.setdefault(
            ("binance_futures", sym, date_utc),
            AssetDayBuffers("binance_futures", sym, date_utc),
        )
        if exch_ts > local_ts:
            buf.exch_ahead_of_local_count += 1
        buf.agg_trade_rows.append((local_ts, exch_ts, agg_id, first_id, last_id, px, qty, is_buyer_maker))


def _ingest_gate_record(
    rec: dict[str, Any],
    date_utc: str,
    buffers: dict[tuple[str, str, str], AssetDayBuffers],
) -> None:
    if rec["kind"] == "META":
        return
    local_ts = rec["recv_wall_ns"]
    obj = json.loads(rec["payload"])
    channel = obj.get("channel", "")
    event = obj.get("event", "")
    if event != "update":
        return
    result = obj.get("result")
    if result is None:
        return

    if channel == "futures.obu" and isinstance(result, dict):
        sym = canonical_asset("gate_futures", str(result["s"]))
        exch_ms = int(result.get("t") or obj.get("time_ms") or (local_ts // 1_000_000))
        exch_ts = exch_ms * 1_000_000
        is_full = bool(result.get("full", False))
        # Decimal strings (X-Gate-Size-Decimal: 1) parsed as float, NEVER int!
        bids = _parse_price_qty_pairs(result.get("b") or [])
        asks = _parse_price_qty_pairs(result.get("a") or [])
        buf = buffers.setdefault(
            ("gate_futures", sym, date_utc),
            AssetDayBuffers("gate_futures", sym, date_utc),
        )
        _emit_depth_batch(buf, is_full, exch_ts, local_ts, bids, asks)

    elif channel == "futures.trades":
        trades = result if isinstance(result, list) else [result]
        for tr in trades:
            if not isinstance(tr, dict):
                continue
            contract = str(tr.get("contract") or "")
            sym = canonical_asset("gate_futures", contract)
            tid = str(tr["id"])
            buf = buffers.setdefault(
                ("gate_futures", sym, date_utc),
                AssetDayBuffers("gate_futures", sym, date_utc),
            )
            dedup_key = f"{contract}:{tid}"
            if dedup_key in buf.seen_trade_ids:
                buf.dedup_trades_dropped += 1
                continue
            buf.seen_trade_ids.add(dedup_key)

            exch_ms = int(tr.get("create_time_ms") or obj.get("time_ms") or (local_ts // 1_000_000))
            exch_ts = exch_ms * 1_000_000
            if exch_ts > local_ts:
                buf.exch_ahead_of_local_count += 1
            raw_size = float(tr["size"])
            side_flag = BUY_EVENT if raw_size >= 0.0 else SELL_EVENT
            qty = abs(raw_size)
            px = float(tr["price"])
            ival = 0
            if bool(tr.get("is_internal", False)):
                ival |= TRADE_FLAG_IS_INTERNAL
                buf.special_flag_counts["is_internal"] += 1
            buf.hbt_rows.append(
                (TRADE_EVENT | side_flag, exch_ts, local_ts, px, qty, 0, ival, 0.0)
            )

    elif channel == "futures.book_ticker" and isinstance(result, dict):
        # Strictly separate independent L1 BBO (NEVER merged into 50-level L2 book!)
        sym = canonical_asset("gate_futures", str(result["s"]))
        exch_ms = int(result.get("t") or obj.get("time_ms") or (local_ts // 1_000_000))
        exch_ts = exch_ms * 1_000_000
        bid_px = float(result["b"])
        bid_qty = float(result["B"])
        ask_px = float(result["a"])
        ask_qty = float(result["A"])
        mid_px = 0.5 * (bid_px + ask_px)
        buf = buffers.setdefault(
            ("gate_futures", sym, date_utc),
            AssetDayBuffers("gate_futures", sym, date_utc),
        )
        buf.bbo_rows.append((local_ts, exch_ts, bid_px, bid_qty, ask_px, ask_qty, mid_px))


def _ingest_bybit_record(
    rec: dict[str, Any],
    date_utc: str,
    buffers: dict[tuple[str, str, str], AssetDayBuffers],
) -> None:
    if rec["kind"] == "META":
        return
    local_ts = rec["recv_wall_ns"]
    obj = json.loads(rec["payload"])
    topic = str(obj.get("topic") or "")
    if not topic:
        return

    if topic.startswith("orderbook.50."):
        data = obj.get("data") or {}
        sym = canonical_asset("bybit_linear", str(data.get("s") or topic.split(".")[-1]))
        exch_ms = int(obj.get("cts") or obj.get("ts") or (local_ts // 1_000_000))
        exch_ts = exch_ms * 1_000_000
        is_snap = obj.get("type") == "snapshot"
        bids = _parse_price_qty_pairs(data.get("b") or [])
        asks = _parse_price_qty_pairs(data.get("a") or [])
        buf = buffers.setdefault(
            ("bybit_linear", sym, date_utc),
            AssetDayBuffers("bybit_linear", sym, date_utc),
        )
        _emit_depth_batch(buf, is_snap, exch_ts, local_ts, bids, asks)

    elif topic.startswith("orderbook.1."):
        # Strictly separate independent L1 BBO (NEVER merged into 50-level L2 book!)
        data = obj.get("data") or {}
        sym = canonical_asset("bybit_linear", str(data.get("s") or topic.split(".")[-1]))
        exch_ms = int(obj.get("cts") or obj.get("ts") or (local_ts // 1_000_000))
        exch_ts = exch_ms * 1_000_000
        bids = _parse_price_qty_pairs(data.get("b") or [])
        asks = _parse_price_qty_pairs(data.get("a") or [])
        if bids and asks:
            bid_px, bid_qty = bids[0]
            ask_px, ask_qty = asks[0]
            buf = buffers.setdefault(
                ("bybit_linear", sym, date_utc),
                AssetDayBuffers("bybit_linear", sym, date_utc),
            )
            buf.bbo_rows.append((local_ts, exch_ts, bid_px, bid_qty, ask_px, ask_qty, 0.5 * (bid_px + ask_px)))

    elif topic.startswith("publicTrade."):
        trades = obj.get("data") or []
        for tr in trades:
            if not isinstance(tr, dict):
                continue
            raw_sym = str(tr.get("s") or topic.split(".")[-1])
            sym = canonical_asset("bybit_linear", raw_sym)
            tid = str(tr["i"])
            buf = buffers.setdefault(
                ("bybit_linear", sym, date_utc),
                AssetDayBuffers("bybit_linear", sym, date_utc),
            )
            dedup_key = f"{raw_sym}:{tid}"
            if dedup_key in buf.seen_trade_ids:
                buf.dedup_trades_dropped += 1
                continue
            buf.seen_trade_ids.add(dedup_key)

            exch_ms = int(tr.get("T") or obj.get("ts") or (local_ts // 1_000_000))
            exch_ts = exch_ms * 1_000_000
            if exch_ts > local_ts:
                buf.exch_ahead_of_local_count += 1
            side_str = str(tr.get("S", "")).lower()
            side_flag = BUY_EVENT if side_str == "buy" else SELL_EVENT
            px = float(tr["p"])
            qty = float(tr["v"])
            ival = 0
            if bool(tr.get("BT", False)):
                ival |= TRADE_FLAG_BYBIT_BT
                buf.special_flag_counts["BT"] += 1
            if bool(tr.get("RPI", False)):
                ival |= TRADE_FLAG_RPI
                buf.special_flag_counts["RPI"] += 1
            buf.hbt_rows.append(
                (TRADE_EVENT | side_flag, exch_ts, local_ts, px, qty, 0, ival, 0.0)
            )


def _ingest_bitget_record(
    rec: dict[str, Any],
    date_utc: str,
    buffers: dict[tuple[str, str, str], AssetDayBuffers],
) -> None:
    if rec["kind"] == "META":
        return
    local_ts = rec["recv_wall_ns"]
    obj = json.loads(rec["payload"])
    arg = obj.get("arg")
    if not isinstance(arg, dict):
        return
    channel = str(arg.get("topic") or arg.get("channel") or "")
    inst_id = str(arg.get("symbol") or arg.get("instId") or "")
    action = str(obj.get("action") or "")
    data_list = obj.get("data")
    if not isinstance(data_list, list):
        return
    sym = canonical_asset("bitget_v3", inst_id)

    if channel == "books50":
        is_snap = action == "snapshot"
        for item in data_list:
            if not isinstance(item, dict):
                continue
            exch_ms = int(item.get("ts") or obj.get("ts") or (local_ts // 1_000_000))
            exch_ts = exch_ms * 1_000_000
            bids = _parse_price_qty_pairs(item.get("b") or item.get("bids") or [])
            asks = _parse_price_qty_pairs(item.get("a") or item.get("asks") or [])
            buf = buffers.setdefault(
                ("bitget_v3", sym, date_utc),
                AssetDayBuffers("bitget_v3", sym, date_utc),
            )
            _emit_depth_batch(buf, is_snap, exch_ts, local_ts, bids, asks)

    elif channel == "books1":
        # Strictly separate independent L1 BBO (NEVER merged into 50-level L2 book!)
        for item in data_list:
            if not isinstance(item, dict):
                continue
            exch_ms = int(item.get("ts") or obj.get("ts") or (local_ts // 1_000_000))
            exch_ts = exch_ms * 1_000_000
            bids = _parse_price_qty_pairs(item.get("b") or item.get("bids") or [])
            asks = _parse_price_qty_pairs(item.get("a") or item.get("asks") or [])
            if bids and asks:
                bid_px, bid_qty = bids[0]
                ask_px, ask_qty = asks[0]
                buf = buffers.setdefault(
                    ("bitget_v3", sym, date_utc),
                    AssetDayBuffers("bitget_v3", sym, date_utc),
                )
                buf.bbo_rows.append((local_ts, exch_ts, bid_px, bid_qty, ask_px, ask_qty, 0.5 * (bid_px + ask_px)))

    elif channel == "publicTrade":
        for tr in data_list:
            if not isinstance(tr, dict):
                continue
            tid = str(tr.get("i") or tr.get("tradeId") or "")
            if not tid:
                continue
            buf = buffers.setdefault(
                ("bitget_v3", sym, date_utc),
                AssetDayBuffers("bitget_v3", sym, date_utc),
            )
            dedup_key = f"{inst_id}:{tid}"
            if dedup_key in buf.seen_trade_ids:
                buf.dedup_trades_dropped += 1
                continue
            buf.seen_trade_ids.add(dedup_key)

            exch_ms = int(tr.get("T") or tr.get("ts") or obj.get("ts") or (local_ts // 1_000_000))
            exch_ts = exch_ms * 1_000_000
            if exch_ts > local_ts:
                buf.exch_ahead_of_local_count += 1
            side_str = str(tr.get("S") or tr.get("side") or "").lower()
            side_flag = BUY_EVENT if side_str == "buy" else SELL_EVENT
            price = tr["p"] if "p" in tr else tr["P"] if "P" in tr else tr["px"]
            px = float(price)
            qty = float(tr.get("v") if "v" in tr else tr["sz"])
            ival = 0
            is_rpi = tr.get("isRPI")
            if is_rpi is True or str(is_rpi).strip().lower() in {"yes", "true", "1"}:
                ival |= TRADE_FLAG_RPI
                buf.special_flag_counts["RPI"] += 1
            buf.hbt_rows.append(
                (TRADE_EVENT | side_flag, exch_ts, local_ts, px, qty, 0, ival, 0.0)
            )


def evaluate_50_level_coverage(
    quotes: list[dict[str, Any]],
    edge_samples: list[tuple[int, float, float, float, float, int, int]],
) -> dict[str, Any]:
    """Evaluate whether quotes/orders fall inside the real 50-level visible L2 depth.

    Any price strictly beyond the 50th real non-empty bid/ask level is explicitly
    labeled `QUEUE_UNKNOWN` (NEVER treated as queue=0, and NEVER dropping only losing fills).
    """
    if not edge_samples:
        labeled = [{**q, "depth_coverage": "QUEUE_UNKNOWN", "queue_assumption": "UNOBSERVABLE_NO_BOOK"} for q in quotes]
        return {
            "total": len(labeled),
            "visible_within_50": 0,
            "queue_unknown": len(labeled),
            "queue_unknown_ratio": 1.0 if labeled else 0.0,
            "items": labeled,
        }

    edge_ts = np.array([e[0] for e in edge_samples], dtype=np.int64)
    labeled: list[dict[str, Any]] = []
    visible_cnt = 0
    unknown_cnt = 0

    for q in quotes:
        ts = int(q["ts_ns"])
        side = str(q["side"]).lower()
        px = float(q["price"])
        idx = int(np.searchsorted(edge_ts, ts, side="right")) - 1
        if idx < 0:
            cov = "QUEUE_UNKNOWN"
            reason = "before_first_book_snapshot"
        else:
            _, best_bid, edge_bid, best_ask, edge_ask, n_b, n_a = edge_samples[idx]
            if side == "buy":
                if n_b >= 50 and px < edge_bid - 1e-12:
                    cov = "QUEUE_UNKNOWN"
                    reason = "beyond_50th_bid_level"
                elif px < edge_bid - 1e-12 and n_b < 50:
                    cov = "QUEUE_UNKNOWN"
                    reason = "beyond_visible_bid_edge"
                else:
                    cov = "VISIBLE_WITHIN_50"
                    reason = "within_visible_bid_depth"
            else:
                if n_a >= 50 and px > edge_ask + 1e-12:
                    cov = "QUEUE_UNKNOWN"
                    reason = "beyond_50th_ask_level"
                elif px > edge_ask + 1e-12 and n_a < 50:
                    cov = "QUEUE_UNKNOWN"
                    reason = "beyond_visible_ask_edge"
                else:
                    cov = "VISIBLE_WITHIN_50"
                    reason = "within_visible_ask_depth"
        if cov == "VISIBLE_WITHIN_50":
            visible_cnt += 1
        else:
            unknown_cnt += 1
        labeled.append({**q, "depth_coverage": cov, "coverage_reason": reason})

    return {
        "total": len(labeled),
        "visible_within_50": visible_cnt,
        "queue_unknown": unknown_cnt,
        "queue_unknown_ratio": (unknown_cnt / len(labeled)) if labeled else 0.0,
        "items": labeled,
    }


def _resolve_manifest_inputs(
    manifest_arg: Path,
    allow_unverified_local: bool,
    date_utc: str | None = None,
) -> tuple[list[tuple[Path, Path, dict[str, Any] | None]], Path | None, list[str]]:
    """Resolve input manifest/SQLite ledger/JSONL ledger/directory into [(data_path, manifest_path, ledger_entry), ...]."""
    errors: list[str] = []
    items: list[tuple[Path, Path, dict[str, Any] | None]] = []
    inferred_meta_dir: Path | None = None

    if manifest_arg.is_dir():
        raw_root = manifest_arg / "raw" if (manifest_arg / "raw").exists() else manifest_arg
        inferred_meta_dir = manifest_arg / "meta" if (manifest_arg / "meta").exists() else None
        for mpath in sorted(raw_root.rglob("*.manifest.json")):
            try:
                mf = json.loads(mpath.read_text(encoding="utf-8"))
                fn = mf.get("file_name")
                if isinstance(fn, str) and fn and Path(fn).name == fn and fn.endswith(".tsv.zst"):
                    dpath = mpath.with_name(fn)
                else:
                    dpath = mpath.with_name(mpath.name.replace(".manifest.json", ".tsv.zst"))
            except Exception:
                dpath = mpath.with_name(mpath.name.replace(".manifest.json", ".tsv.zst"))
            items.append((dpath, mpath, None))
    elif manifest_arg.suffix.lower() in (".sqlite", ".db"):
        base_dir = manifest_arg.parent
        inferred_meta_dir = base_dir / "meta" if (base_dir / "meta").exists() else None
        with ArchiveLedger(manifest_arg) as ledger:
            states = ("SEALED", "VERIFIED", "PRUNED") if allow_unverified_local else ("VERIFIED", "PRUNED")
            rows = ledger.list_partitions(states=states)
            for row in rows:
                if date_utc is not None and row["date_utc"] != date_utc:
                    continue
                is_verified = bool(row["remote_data_verified"] and row["remote_manifest_verified"])
                if not is_verified and not allow_unverified_local:
                    errors.append(
                        f"{manifest_arg.name}: partition not dual remote-verified ({row['rel_data_path']})"
                    )
                    continue
                entry = {
                    "remote_path": row["rel_data_path"],
                    "remote_manifest_path": row["rel_manifest_path"],
                    "bytes": int(row["local_bytes"]),
                    "sha256": row["local_sha256"],
                    "md5": row["local_md5"],
                    "manifest_bytes": int(row["manifest_bytes"]),
                    "manifest_sha256": row["manifest_sha256"],
                    "verified": is_verified,
                    "state": row["state"],
                    "boot_id": row["boot_id"],
                    "partition_id": row["partition_id"],
                }
                items.append((base_dir / row["rel_data_path"], base_dir / row["rel_manifest_path"], entry))
    elif manifest_arg.name.endswith(".jsonl"):
        # Verified archive ledger from `archive --verify-remote`
        base_dir = manifest_arg.parent
        inferred_meta_dir = base_dir / "meta" if (base_dir / "meta").exists() else None
        seen_paths: set[str] = set()
        for line_no, line in enumerate(manifest_arg.read_text(encoding="utf-8").splitlines(), start=1):
            line = line.strip()
            if not line:
                continue
            entry = json.loads(line)
            if date_utc is not None and entry.get("date_utc") not in (None, date_utc):
                continue
            if not entry.get("verified") and not allow_unverified_local:
                errors.append(f"{manifest_arg.name}:{line_no}: ledger entry not remote-verified ({entry.get('remote_path')})")
                continue
            rel_data = entry["remote_path"]
            rel_manifest = entry["remote_manifest_path"]
            if rel_data in seen_paths:
                continue
            seen_paths.add(rel_data)
            dpath = base_dir / rel_data
            mpath = base_dir / rel_manifest
            items.append((dpath, mpath, entry))
    elif manifest_arg.name.endswith(".manifest.json"):
        try:
            mf = json.loads(manifest_arg.read_text(encoding="utf-8"))
            fn = mf.get("file_name")
            if isinstance(fn, str) and fn and Path(fn).name == fn and fn.endswith(".tsv.zst"):
                dpath = manifest_arg.with_name(fn)
            else:
                dpath = manifest_arg.with_name(manifest_arg.name.replace(".manifest.json", ".tsv.zst"))
        except Exception:
            dpath = manifest_arg.with_name(manifest_arg.name.replace(".manifest.json", ".tsv.zst"))
        items.append((dpath, manifest_arg, None))
    else:
        errors.append(f"Unsupported --manifest path: {manifest_arg}")

    return items, inferred_meta_dir, errors


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1 << 20)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def run_qa_convert(
    manifest_arg: Path,
    out_dir: Path,
    meta_dir: Path | None = None,
    allow_unverified_local: bool = False,
    strict_zero_loss: bool = False,
    remote_backend: ArchiveBackend | None = None,
    restore_cache_dir: Path | None = None,
    coverage_config: dict[str, Any] | None = None,
    coverage_date_utc: str | None = None,
) -> dict[str, Any]:
    """Run full QA gatekeeper + strict-separation HBT 2.4.4 conversion (with PRUNED restore support)."""
    from data_coverage import CompleteDayCoverage

    if (coverage_config is None) != (coverage_date_utc is None):
        return {"status": "blocked", "phase": "qa_convert", "failed_items": ["coverage_config_and_date_required_together"]}
    try:
        coverage = (
            CompleteDayCoverage(coverage_config, coverage_date_utc)
            if coverage_config is not None and coverage_date_utc is not None
            else None
        )
    except (ValueError, TypeError, KeyError) as exc:
        return {"status": "blocked", "phase": "qa_convert", "failed_items": [f"invalid_coverage_config:{exc}"]}
    items_resolved, inferred_meta, resolve_errors = _resolve_manifest_inputs(
        manifest_arg, allow_unverified_local, coverage.date if coverage is not None else None
    )
    if resolve_errors:
        return {
            "status": "blocked",
            "phase": "qa_convert",
            "failed_items": resolve_errors,
            "minimal_fixes": [
                "Supply archive_ledger.sqlite or verified_archive_ledger.jsonl produced by `archive --verify-remote`"
            ],
        }
    if not items_resolved:
        return {
            "status": "blocked",
            "phase": "qa_convert",
            "failed_items": [f"No sealed partitions found from {manifest_arg}"],
            "minimal_fixes": ["Verify sealed .tsv.zst and .manifest.json files exist"],
        }

    effective_meta = meta_dir if meta_dir is not None else inferred_meta
    chrony_uncertified = load_chrony_uncertified_intervals(effective_meta)
    spec_isolations = load_spec_change_isolations(effective_meta)

    failed_items: list[str] = []
    restored_pruned_partitions: list[str] = []
    temp_restore_ctx: tempfile.TemporaryDirectory[str] | None = None
    active_cache_dir = restore_cache_dir

    # Group verified partitions by exchange and sort chronologically by date/hour/start_wall_ns/boot_id
    by_exchange: dict[str, list[tuple[dict[str, Any], Path, Path]]] = defaultdict(list)
    try:
        for dpath, mpath, ledger_entry in items_resolved:
            try:
                if coverage is not None and mpath.is_file():
                    input_manifest = json.loads(mpath.read_text(encoding="utf-8"))
                    if input_manifest["date_utc"] != coverage.date:
                        continue
                effective_dpath = dpath
                effective_mpath = mpath

                if not effective_dpath.is_file():
                    if ledger_entry is not None and (
                        ledger_entry.get("verified") or ledger_entry.get("state") in ("VERIFIED", "PRUNED")
                    ):
                        if remote_backend is None:
                            raise FileNotFoundError(
                                f"Partition {ledger_entry['remote_path']} is not on local disk "
                                f"(state={ledger_entry.get('state', 'UNKNOWN')}); supply --remote to restore from archive"
                            )
                        if active_cache_dir is None:
                            temp_restore_ctx = tempfile.TemporaryDirectory(prefix="tokyo_l2_qa_restore_")
                            active_cache_dir = Path(temp_restore_ctx.name)
                        if not effective_mpath.is_file() and ledger_entry.get("remote_manifest_path"):
                            restored_m = active_cache_dir / ledger_entry["remote_manifest_path"]
                            remote_backend.download_file(ledger_entry["remote_manifest_path"], restored_m)
                            effective_mpath = restored_m
                        effective_dpath = restore_pruned_partition(
                            rel_data_path=str(ledger_entry["remote_path"]),
                            expected_bytes=int(ledger_entry["bytes"]),
                            expected_sha256=str(ledger_entry["sha256"]),
                            backend=remote_backend,
                            cache_dir=active_cache_dir,
                        )
                        restored_pruned_partitions.append(str(ledger_entry["remote_path"]))

                checked = verify_local_partition(effective_dpath, effective_mpath)
                mf = checked["manifest"]
                if coverage is None or mf["date_utc"] == coverage.date:
                    by_exchange[mf["exchange"]].append((mf, effective_dpath, effective_mpath))
                    if coverage is not None:
                        coverage.observe_partition(mf)
            except Exception as exc:
                failed_items.append(f"integrity_failed:{dpath.name}:{type(exc).__name__}:{exc}")

        if failed_items:
            return {
                "status": "blocked",
                "phase": "qa_convert",
                "failed_items": failed_items,
                "minimal_fixes": ["Re-download or re-seal corrupted partitions before conversion"],
            }

        buffers: dict[tuple[str, str, str], AssetDayBuffers] = {}
        # Per-(exchange, date_utc) structural QA tracking
        day_gaps: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        day_local_loss: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        local_loss_partitions: list[dict[str, Any]] = []
        day_switches: dict[tuple[str, str], int] = defaultdict(int)
        day_active_gap_start: dict[tuple[str, str], int | None] = defaultdict(lambda: None)
        exchange_stats: dict[str, dict[str, Any]] = {}

        for exchange, items in sorted(by_exchange.items()):
            items.sort(
                key=lambda x: (
                    x[0]["date_utc"],
                    x[0]["hour_utc"],
                    x[0].get("start_wall_ns") or 0,
                    str(x[0].get("boot_id") or ""),
                    x[0].get("first_rx_seq") or 0,
                    x[1].name,
                )
            )
            prev_boot: str | None = None
            prev_rx_seq: int | None = None
            prev_mono_ns: int | None = None
            total_records = 0

            for mf, dpath, mpath in items:
                boot_id = str(mf.get("boot_id", ""))
                records = parse_tsv_zst_records(dpath)
                recorded_local_loss = 0
                dropped_frames = 0
                if boot_id != prev_boot:
                    prev_boot = boot_id
                    prev_rx_seq = None
                    prev_mono_ns = None

                for rec in records:
                    if coverage is not None:
                        coverage.observe_record(exchange, rec)
                    total_records += 1
                    rx_seq = rec["rx_seq"]
                    mono_ns = rec["recv_mono_ns"]
                    wall_ns = rec["recv_wall_ns"]
                    date_utc = date_utc_from_wall_ns(wall_ns)

                    if prev_rx_seq is not None and rx_seq <= prev_rx_seq:
                        failed_items.append(
                            f"rx_seq_non_monotonic:{exchange}:{dpath.name}:prev={prev_rx_seq},curr={rx_seq}"
                        )
                    if prev_mono_ns is not None and mono_ns < prev_mono_ns:
                        failed_items.append(
                            f"recv_mono_ns_regressed:{exchange}:{dpath.name}:prev={prev_mono_ns},curr={mono_ns}"
                        )
                    prev_rx_seq = rx_seq
                    prev_mono_ns = mono_ns

                    if rec["kind"] == "META":
                        meta_obj = json.loads(rec["payload"])
                        ev = meta_obj.get("event", "")
                        if ev == "GAP_START":
                            day_active_gap_start[(exchange, date_utc)] = wall_ns
                            day_gaps[(exchange, date_utc)].append(
                                {"start_wall_ns": wall_ns, "end_wall_ns": None, "reason": meta_obj.get("reason", "")}
                            )
                        elif ev == "GAP_END":
                            if day_gaps[(exchange, date_utc)] and day_gaps[(exchange, date_utc)][-1]["end_wall_ns"] is None:
                                day_gaps[(exchange, date_utc)][-1]["end_wall_ns"] = wall_ns
                            else:
                                day_gaps[(exchange, date_utc)].append(
                                    {"start_wall_ns": None, "end_wall_ns": wall_ns, "reason": "gap_end_cross_day"}
                                )
                            day_active_gap_start[(exchange, date_utc)] = None
                        elif ev == "LOCAL_LOSS":
                            dropped = int(meta_obj.get("dropped_frames", meta_obj.get("dropped", 1)))
                            recorded_local_loss += 1
                            dropped_frames += dropped
                            day_local_loss[(exchange, date_utc)].append(
                                {"wall_ns": wall_ns, "dropped": dropped, "reason": meta_obj.get("reason", "")}
                            )
                        elif ev == "SWITCH":
                            day_switches[(exchange, date_utc)] += 1

                    if exchange == "binance_futures":
                        _ingest_binance_record(rec, date_utc, buffers)
                    elif exchange == "gate_futures":
                        _ingest_gate_record(rec, date_utc, buffers)
                    elif exchange == "bybit_linear":
                        _ingest_bybit_record(rec, date_utc, buffers)
                    elif exchange == "bitget_v3":
                        _ingest_bitget_record(rec, date_utc, buffers)

                manifest_local_loss = int(mf.get("local_loss_lines") or 0)
                if recorded_local_loss or manifest_local_loss:
                    local_loss_partitions.append({
                        "exchange": exchange,
                        "date_utc": mf["date_utc"],
                        "hour_utc": mf["hour_utc"],
                        "boot_id": boot_id,
                        "manifest_file": mpath.name,
                        "data_file": dpath.name,
                        "data_sha256": mf["sha256"],
                        "manifest_local_loss_lines": manifest_local_loss,
                        "recorded_local_loss_lines": recorded_local_loss,
                        "recorded_dropped_frames": dropped_frames,
                    })

            exchange_stats[exchange] = {
                "partitions_verified": len(items),
                "records_parsed": total_records,
            }
    finally:
        if temp_restore_ctx is not None:
            temp_restore_ctx.cleanup()

    coverage_report = coverage.report(effective_meta) if coverage is not None else {"status": "not_assessed"}
    if coverage is not None and coverage_report["status"] != "complete":
        failed_items.append("incomplete_utc_day_coverage")
    if (strict_zero_loss or coverage is not None) and local_loss_partitions:
        failed_items.append("local_loss_detected_under_strict_zero_loss_gate")
    if coverage is not None and (any(day_gaps.values()) or chrony_uncertified.get(coverage.date)):
        failed_items.append("complete_day_gap_or_uncertified_clock")

    if failed_items:
        return {
            "status": "blocked",
            "phase": "qa_convert",
            "failed_items": failed_items,
            "coverage": coverage_report,
            "local_loss_partitions": local_loss_partitions,
            "minimal_fixes": ["Review missing hours, uncertified clock, sequence errors, and local loss before converting"],
        }

    out_dir.mkdir(parents=True, exist_ok=True)
    artifacts: list[dict[str, Any]] = []
    calendar_rows: list[dict[str, Any]] = []
    loss_partition_days = {(row["exchange"], row["date_utc"]) for row in local_loss_partitions}

    for (exchange, sym, date_utc), buf in sorted(buffers.items()):
        ymd = yyyymmdd_from_date(date_utc)
        reasons: list[str] = []
        gaps = day_gaps.get((exchange, date_utc), [])
        losses = day_local_loss.get((exchange, date_utc), [])
        clock_issues = chrony_uncertified.get(date_utc, [])
        spec_issues = spec_isolations.get((exchange, date_utc), [])

        if gaps:
            reasons.append("uncovered_gap")
        if losses or (exchange, date_utc) in loss_partition_days:
            reasons.append("local_loss")
        if clock_issues:
            reasons.append("clock_uncertified_gt_1ms")
        if spec_issues:
            reasons.append("spec_change_isolation")

        day_files: dict[str, str] = {}
        day_sha256: dict[str, str] = {}

        if exchange == "binance_futures":
            if buf.bbo_rows:
                bbo_arr = np.array(buf.bbo_rows, dtype=np.float64)
                # Ensure monotonic local_ts_ns
                order = np.argsort(bbo_arr[:, 0], kind="mergesort")
                bbo_arr = bbo_arr[order]
                bbo_path = out_dir / f"binance_bbo_{sym}_{ymd}.npz"
                np.savez_compressed(
                    bbo_path,
                    local_ts_ns=bbo_arr[:, 0].astype(np.int64),
                    exch_ts_ns=bbo_arr[:, 1].astype(np.int64),
                    bid_px=bbo_arr[:, 2],
                    bid_qty=bbo_arr[:, 3],
                    ask_px=bbo_arr[:, 4],
                    ask_qty=bbo_arr[:, 5],
                    mid_px=bbo_arr[:, 6],
                )
                day_files["binance_bbo"] = str(bbo_path)
                day_sha256["binance_bbo"] = _sha256_file(bbo_path)
                artifacts.append({"kind": "binance_bbo", "exchange": exchange, "symbol": sym, "date_utc": date_utc, "path": str(bbo_path), "sha256": day_sha256["binance_bbo"], "rows": int(len(bbo_arr))})

            if buf.hbt_rows:
                raw_arr = np.array(buf.hbt_rows, dtype=event_dtype)
                ordered, order_diag = _order_and_validate_hbt_events(
                    raw_arr,
                    exchange=exchange,
                    symbol=sym,
                    date_utc=date_utc,
                    stream_kind="binance_depth_diag",
                )
                dep_path = out_dir / f"binance_depth_diag_{sym}_{ymd}.npz"
                np.savez_compressed(dep_path, data=ordered)
                day_files["binance_depth_diag"] = str(dep_path)
                day_sha256["binance_depth_diag"] = _sha256_file(dep_path)
                artifacts.append(
                    {
                        "kind": "binance_depth_diag",
                        "exchange": exchange,
                        "symbol": sym,
                        "date_utc": date_utc,
                        "path": str(dep_path),
                        "sha256": day_sha256["binance_depth_diag"],
                        "rows": int(len(ordered)),
                        "order_diagnostics": order_diag,
                    }
                )

            if buf.agg_trade_rows:
                agg_path = out_dir / f"binance_aggtrade_diag_{sym}_{ymd}.npz"
                np.savez_compressed(
                    agg_path,
                    local_ts_ns=np.array([r[0] for r in buf.agg_trade_rows], dtype=np.int64),
                    exch_ts_ns=np.array([r[1] for r in buf.agg_trade_rows], dtype=np.int64),
                    agg_id=np.array([r[2] for r in buf.agg_trade_rows], dtype=np.int64),
                    first_trade_id=np.array([r[3] for r in buf.agg_trade_rows], dtype=np.int64),
                    last_trade_id=np.array([r[4] for r in buf.agg_trade_rows], dtype=np.int64),
                    price=np.array([r[5] for r in buf.agg_trade_rows], dtype=np.float64),
                    qty=np.array([r[6] for r in buf.agg_trade_rows], dtype=np.float64),
                    is_buyer_maker=np.array([r[7] for r in buf.agg_trade_rows], dtype=np.int8),
                )
                day_files["binance_aggtrade_diag"] = str(agg_path)
                day_sha256["binance_aggtrade_diag"] = _sha256_file(agg_path)
                artifacts.append({"kind": "binance_aggtrade_diag", "exchange": exchange, "symbol": sym, "date_utc": date_utc, "path": str(agg_path), "sha256": day_sha256["binance_aggtrade_diag"], "rows": len(buf.agg_trade_rows)})

        else:
            # Follower exchange: strictly separate 50-level HBT L2+trades vs independent L1 BBO
            if not buf.seen_snapshot:
                reasons.append("missing_l2_snapshot_anchor")
            if buf.hbt_rows:
                raw_arr = np.array(buf.hbt_rows, dtype=event_dtype)
                ordered, order_diag = _order_and_validate_hbt_events(
                    raw_arr,
                    exchange=exchange,
                    symbol=sym,
                    date_utc=date_utc,
                    stream_kind="follower_l2_hbt",
                )
                l2_path = out_dir / f"{exchange}_l2_{sym}_{ymd}.npz"
                np.savez_compressed(l2_path, data=ordered)
                day_files["follower_l2_hbt"] = str(l2_path)
                day_sha256["follower_l2_hbt"] = _sha256_file(l2_path)
                artifacts.append(
                    {
                        "kind": "follower_l2_hbt",
                        "exchange": exchange,
                        "symbol": sym,
                        "date_utc": date_utc,
                        "path": str(l2_path),
                        "sha256": day_sha256["follower_l2_hbt"],
                        "rows": int(len(ordered)),
                        "order_diagnostics": order_diag,
                    }
                )

            if buf.edge_samples:
                edge_path = out_dir / f"{exchange}_l2_edges_{sym}_{ymd}.npz"
                np.savez_compressed(
                    edge_path,
                    local_ts_ns=np.array([e[0] for e in buf.edge_samples], dtype=np.int64),
                    best_bid=np.array([e[1] for e in buf.edge_samples], dtype=np.float64),
                    edge_bid_50=np.array([e[2] for e in buf.edge_samples], dtype=np.float64),
                    best_ask=np.array([e[3] for e in buf.edge_samples], dtype=np.float64),
                    edge_ask_50=np.array([e[4] for e in buf.edge_samples], dtype=np.float64),
                    visible_bid_levels=np.array([e[5] for e in buf.edge_samples], dtype=np.int32),
                    visible_ask_levels=np.array([e[6] for e in buf.edge_samples], dtype=np.int32),
                )
                day_files["follower_l2_edges"] = str(edge_path)
                day_sha256["follower_l2_edges"] = _sha256_file(edge_path)

            if buf.bbo_rows:
                l1_arr = np.array(buf.bbo_rows, dtype=np.float64)
                order = np.argsort(l1_arr[:, 0], kind="mergesort")
                l1_arr = l1_arr[order]
                l1_path = out_dir / f"{exchange}_l1_{sym}_{ymd}.npz"
                np.savez_compressed(
                    l1_path,
                    local_ts_ns=l1_arr[:, 0].astype(np.int64),
                    exch_ts_ns=l1_arr[:, 1].astype(np.int64),
                    bid_px=l1_arr[:, 2],
                    bid_qty=l1_arr[:, 3],
                    ask_px=l1_arr[:, 4],
                    ask_qty=l1_arr[:, 5],
                    mid_px=l1_arr[:, 6],
                )
                day_files["follower_l1_bbo"] = str(l1_path)
                day_sha256["follower_l1_bbo"] = _sha256_file(l1_path)
                artifacts.append({"kind": "follower_l1_bbo", "exchange": exchange, "symbol": sym, "date_utc": date_utc, "path": str(l1_path), "sha256": day_sha256["follower_l1_bbo"], "rows": int(len(l1_arr))})

        status = "execution_unverified" if reasons else "eligible"
        calendar_rows.append(
            {
                "exchange": exchange,
                "symbol": sym,
                "date_utc": date_utc,
                "eligibility_status": status,
                "unverified_reasons": reasons,
                "gap_count": len(gaps),
                "gap_intervals": gaps,
                "local_loss_count": sum(x["dropped"] for x in losses),
                "switch_count": day_switches.get((exchange, date_utc), 0),
                "clock_uncertified_samples": len(clock_issues),
                "spec_change_isolations": len(spec_issues),
                "hbt_event_count": len(buf.hbt_rows),
                "bbo_row_count": len(buf.bbo_rows),
                "agg_trade_row_count": len(buf.agg_trade_rows),
                "dedup_trades_dropped": buf.dedup_trades_dropped,
                "special_flag_counts": buf.special_flag_counts,
                "exch_ahead_of_local_count": buf.exch_ahead_of_local_count,
                "files": day_files,
                "sha256": day_sha256,
            }
        )

    # Write eligibility calendar JSON + CSV (never silently drop unverified rows!)
    cal_json = out_dir / "eligibility_calendar.json"
    cal_json.write_text(json.dumps(calendar_rows, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    cal_csv = out_dir / "eligibility_calendar.csv"
    with cal_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "exchange",
                "symbol",
                "date_utc",
                "eligibility_status",
                "unverified_reasons",
                "gap_count",
                "local_loss_count",
                "switch_count",
                "clock_uncertified_samples",
                "spec_change_isolations",
                "hbt_event_count",
                "bbo_row_count",
                "dedup_trades_dropped",
            ]
        )
        for r in calendar_rows:
            w.writerow(
                [
                    r["exchange"],
                    r["symbol"],
                    r["date_utc"],
                    r["eligibility_status"],
                    ";".join(r["unverified_reasons"]),
                    r["gap_count"],
                    r["local_loss_count"],
                    r["switch_count"],
                    r["clock_uncertified_samples"],
                    r["spec_change_isolations"],
                    r["hbt_event_count"],
                    r["bbo_row_count"],
                    r["dedup_trades_dropped"],
                ]
            )

    summary = {
        "status": "ok",
        "phase": "qa_convert",
        "hftbacktest_version": "2.4.4",
        "exchanges": exchange_stats,
        "calendar_path": str(cal_json),
        "calendar_csv_path": str(cal_csv),
        "coverage": coverage_report,
        "local_loss_partitions": local_loss_partitions,
        "eligible_asset_days": sum(1 for r in calendar_rows if r["eligibility_status"] == "eligible"),
        "execution_unverified_asset_days": sum(
            1 for r in calendar_rows if r["eligibility_status"] == "execution_unverified"
        ),
        "restored_pruned_partitions": restored_pruned_partitions,
        "artifacts": artifacts,
    }
    (out_dir / "qa_report.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return summary


def run_offline_e2e_hbt_replay(
    follower_l2_npz: Path,
    binance_bbo_npz: Path,
    *,
    tick_size: float = 0.1,
    lot_size: float = 0.0001,
    contract_value: float = 0.0001,
    order_qty: float = 10.0,
    position_cap: float = 50.0,
    maker_fee: float = 0.0,
    taker_fee: float = 0.0005,
    order_latency_ns: int = 1_000_000,
    trade_steps: int = 1200,
    quote_mode: int = 2,
    beta: float = 0.5,
    extra_signal_delay_ns: int = 0,
    cross_timestamp_tie_mode: str = "microsecond",
    stop_at_ns: int = 0,
    probe_ns: np.ndarray | None = None,
) -> dict[str, Any]:
    """Run Huhabubu/mm's official `scripts/core_glft_engine.py::run_core_glft` on converted .npz files.

    Returns exact deterministic fingerprints (input SHA-256, fills, ledger, decisions) for P1
    repeatability and anti-lookahead verification.
    """
    from hftbacktest import BacktestAsset, ROIVectorMarketDepthBacktest

    repo_root = Path(__file__).resolve().parents[2]
    scripts_dir = str(repo_root / "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)

    from core_glft_engine import run_core_glft  # type: ignore

    follower_data = np.load(follower_l2_npz)["data"]
    validate_event_order(follower_data)

    bbo = np.load(binance_bbo_npz)
    binance_visible_ns = bbo["local_ts_ns"].astype(np.int64) + int(extra_signal_delay_ns)
    binance_mid = bbo["mid_px"].astype(np.float64)

    # Determine price bounds strictly from data ROI
    valid_px = follower_data["px"][(follower_data["px"] > 0.0) & np.isfinite(follower_data["px"])]
    if len(valid_px) == 0:
        raise ValueError("Follower L2 npz contains no valid positive prices")
    lb = float(np.min(valid_px)) - 500.0 * tick_size
    ub = float(np.max(valid_px)) + 500.0 * tick_size

    # Strictly causal 5-second basis grid (zero basis, finite positive sigma from past ticks only)
    t_start = int(follower_data["local_ts"][0])
    t_end = int(follower_data["local_ts"][-1])
    grid_step_ns = 5_000_000_000
    basis_grid_ns = np.arange(t_start, t_end + grid_step_ns, grid_step_ns, dtype=np.int64)
    if len(basis_grid_ns) < 2:
        basis_grid_ns = np.array([t_start, t_start + grid_step_ns], dtype=np.int64)
    basis_values = np.zeros(len(basis_grid_ns), dtype=np.float64)
    basis_sigmas = np.full(len(basis_grid_ns), max(tick_size * 2.0, 0.2), dtype=np.float64)

    asset = (
        BacktestAsset()
        .data([str(follower_l2_npz)])
        .linear_asset(contract_value)
        .constant_order_latency(order_latency_ns, order_latency_ns)
        .power_prob_queue_model(2.0)
        .no_partial_fill_exchange()
        .trading_value_fee_model(maker_fee, taker_fee)
        .tick_size(tick_size)
        .lot_size(lot_size)
        .roi_lb(max(tick_size, lb))
        .roi_ub(ub)
        .last_trades_capacity(20_000)
    )
    engine = ROIVectorMarketDepthBacktest([asset])
    diagnostics = np.zeros(12, dtype=np.float64)
    try:
        raw = run_core_glft(
            engine,
            binance_visible_ns,
            binance_mid,
            trade_steps,
            beta,
            basis_grid_ns,
            basis_values,
            basis_sigmas,
            quote_mode,
            tick_size=tick_size,
            contract_value=contract_value,
            order_qty=order_qty,
            position_cap=position_cap,
            stop_at_ns=stop_at_ns,
            probe_ns=probe_ns,
            fit_mode="physical_1tick_poisson",
            post_only_clamp_mode="unified",
            diagnostics=diagnostics,
            cross_timestamp_tie_mode=cross_timestamp_tie_mode,
        )
    finally:
        engine.close()

    (
        sample_count,
        decisions,
        attempts,
        rejects,
        successes,
        initial_ok,
        updates,
        submits,
        cancels,
        buy_fills,
        sell_fills,
        engine_fills,
        final_pos,
        max_pos,
        final_balance,
        final_fee,
        trading_value,
        final_mid,
        final_equity,
        min_equity,
        max_equity,
        first_equity,
        max_drawdown,
        pos_abs_seconds,
        pos_sq_seconds,
        at_cap_seconds,
        binance_events,
        target_feed_events,
        order_reply_events,
        count_binance_reads,
        observed_binance,
        fill_ts_ns,
        fill_sides,
        t0,
        tail_state,
        origin_stats,
    ) = raw
    now_end, fill_observed_ns, fill_exec_price, fill_exec_qty, probe_rows = tail_state

    fingerprint_payload = {
        "sample_count": int(sample_count),
        "decisions": int(decisions),
        "submits": int(submits),
        "cancels": int(cancels),
        "buy_fills": int(buy_fills),
        "sell_fills": int(sell_fills),
        "engine_fills": int(engine_fills),
        "final_pos": float(final_pos),
        "final_balance": float(final_balance),
        "final_fee": float(final_fee),
        "trading_value": float(trading_value),
        "final_mid": None if math.isnan(float(final_mid)) else float(final_mid),
        "final_equity": None if math.isnan(float(final_equity)) else float(final_equity),
        "binance_events": int(binance_events),
        "target_feed_events": int(target_feed_events),
        "fill_ts_ns": [int(x) for x in fill_ts_ns],
        "fill_sides": [int(x) for x in fill_sides],
        "fill_exec_price": [float(x) for x in fill_exec_price],
        "fill_exec_qty": [float(x) for x in fill_exec_qty],
        "origin_stats": [int(x) for x in origin_stats],
    }
    ledger_sha256 = hashlib.sha256(
        json.dumps(fingerprint_payload, sort_keys=True).encode("utf-8")
    ).hexdigest()

    return {
        "status": "ok",
        "follower_l2_sha256": _sha256_file(follower_l2_npz),
        "binance_bbo_sha256": _sha256_file(binance_bbo_npz),
        "ledger_sha256": ledger_sha256,
        "t0_ns": int(t0),
        "t_end_ns": int(now_end),
        "diagnostics": [float(x) for x in diagnostics],
        "probe_rows": probe_rows.tolist() if probe_ns is not None else [],
        "metrics": fingerprint_payload,
    }


def main() -> int:
    p = argparse.ArgumentParser(description="Offline QA Gatekeeper and HBT 2.4.4 Converter")
    p.add_argument("--manifest", type=Path, required=True, help="Archive ledger (.sqlite/.jsonl), .manifest.json, or sealed root dir")
    p.add_argument("--out", type=Path, required=True, help="Output directory for HBT .npz files and eligibility calendar")
    p.add_argument("--meta-dir", type=Path, default=None, help="Optional meta/ directory with chrony/ and specs/")
    p.add_argument("--coverage-config", type=Path, help="Collector configuration for complete-day coverage")
    p.add_argument("--date-utc", help="UTC day (YYYY-MM-DD) to certify with --coverage-config")
    p.add_argument("--allow-unverified-local", action="store_true", help="Allow reading unverified local sealed partitions (offline tests only)")
    p.add_argument("--strict-zero-loss", action="store_true", help="Fail closed with status=blocked if any LOCAL_LOSS occurred")
    p.add_argument("--remote", default=None, help="Optional rclone remote root to restore PRUNED partitions (e.g. gdrive_main:tokyo_l2_data)")
    p.add_argument("--restore-cache-dir", type=Path, default=None, help="Optional local cache directory for restored PRUNED partitions")
    args = p.parse_args()

    remote_backend = RcloneBackend(args.remote) if args.remote else None
    result = run_qa_convert(
        args.manifest,
        args.out,
        meta_dir=args.meta_dir,
        allow_unverified_local=args.allow_unverified_local,
        strict_zero_loss=args.strict_zero_loss,
        remote_backend=remote_backend,
        restore_cache_dir=args.restore_cache_dir,
        coverage_config=json.loads(args.coverage_config.read_text(encoding="utf-8")) if args.coverage_config else None,
        coverage_date_utc=args.date_utc,
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0 if result["status"] == "ok" else 2


if __name__ == "__main__":
    raise SystemExit(main())
