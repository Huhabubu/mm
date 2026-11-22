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
    last_exch_ts: int = 0
    last_local_ts: int = 0
    binance_depth_anchor: dict[str, Any] | None = None
    follower_depth_anchor: dict[str, Any] | None = None
    depth_unavailable_intervals: list[dict[str, Any]] = field(default_factory=list)
    local_book_clears: list[tuple[int, int]] = field(default_factory=list)
    bridged_delta_records: list[dict[str, Any]] = field(default_factory=list)
    unrebuildable_depth_reasons: list[str] = field(default_factory=list)
    unrebuildable_depth_intervals: list[dict[str, Any]] = field(default_factory=list)
    excluded_unrebuildable_depth_rows: int = 0
    depth_recovered_count: int = 0

    def emit_book_clear_if_active(self, local_ts: int) -> None:
        """Record a LOCAL_EVENT-only DEPTH_CLEAR_EVENT when local depth continuity breaks.

        A local disconnect/switch/rotation did NOT happen on the exchange matching engine, so
        we must never fabricate an EXCH_EVENT clear with an estimated exchange timestamp (which
        could sort after a higher-latency new-connection snapshot and wipe the matching book).
        Instead, only the local view is cleared at exact `local_ts` and edge_samples records an
        empty-book sentinel so gap-interval quotes/fills are explicitly flagged unverified.
        """
        if not self.bids and not self.asks:
            return
        self.local_book_clears.append((local_ts, len(self.hbt_rows)))
        self.edge_samples.append((local_ts, float("nan"), float("nan"), float("nan"), float("nan"), 0, 0))
        self.bids.clear()
        self.asks.clear()
        self.last_local_ts = max(self.last_local_ts, local_ts)

    def close_unavailable_interval_on_snapshot(
        self,
        *,
        boot_id: str,
        conn_id: str,
        epoch: int,
        recovered_wall_ns: int,
    ) -> None:
        """Close the latest open DEPTH_UNAVAILABLE interval ONLY upon an actual valid on-disk snapshot."""
        if not self.depth_unavailable_intervals:
            return
        iv = self.depth_unavailable_intervals[-1]
        if iv.get("end_wall_ns") is not None:
            return
        if iv.get("planned"):
            iv_boot = str(iv.get("boot_id") or "")
            iv_conn = str(iv.get("conn_id") or "")
            iv_epoch = iv.get("epoch")
            if iv_boot and boot_id and iv_boot != boot_id:
                return
            if iv_conn and conn_id and (iv_conn != conn_id or (iv_epoch is not None and int(iv_epoch) != epoch)):
                return
        iv["end_wall_ns"] = recovered_wall_ns

    def record_unrebuildable_depth(
        self,
        *,
        reason: str,
        wall_ns: int,
        conn_id: str,
        epoch: int,
    ) -> None:
        if reason not in self.unrebuildable_depth_reasons:
            self.unrebuildable_depth_reasons.append(reason)
        if (
            self.unrebuildable_depth_intervals
            and self.unrebuildable_depth_intervals[-1]["reason"] == reason
            and self.unrebuildable_depth_intervals[-1]["conn_id"] == conn_id
            and self.unrebuildable_depth_intervals[-1]["epoch"] == epoch
        ):
            self.unrebuildable_depth_intervals[-1]["end_wall_ns"] = wall_ns
            self.unrebuildable_depth_intervals[-1]["excluded_frames"] += 1
        else:
            self.unrebuildable_depth_intervals.append(
                {
                    "exchange": self.exchange,
                    "symbol": self.symbol,
                    "reason": reason,
                    "start_wall_ns": wall_ns,
                    "end_wall_ns": wall_ns,
                    "conn_id": conn_id,
                    "epoch": epoch,
                    "excluded_frames": 1,
                }
            )
        self.excluded_unrebuildable_depth_rows += 1

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
    local_book_clears: list[tuple[int, int]] | None = None,
) -> tuple[np.ndarray, dict[str, Any]]:
    """Strip direction flags, run HBT correct_event_order(), merge LOCAL_EVENT-only clears, and validate_event_order().

    `hftbacktest.data.validation.correct_event_order()` assigns `EXCH_EVENT` and `LOCAL_EVENT`
    via bitwise OR when splitting out-of-order rows. Any pre-set `EXCH_EVENT | LOCAL_EVENT` bits
    on input rows must be cleared beforehand (while preserving event type and buy/sell side bits)
    so split rows do not erroneously retain both direction flags.

    Local-only clears (recorded on local WS switch/rotation/outage) are merged strictly with
    `LOCAL_EVENT | DEPTH_CLEAR_EVENT` (never `EXCH_EVENT`) after exchange-event ordering so a
    local disconnect never synthesizes an exchange-side clear that could invert against a
    higher-latency snapshot on the new connection.
    """
    first_raw_exch = _find_first_raw_reversal(raw_arr, "exch_ts")
    first_raw_local = _find_first_raw_reversal(raw_arr, "local_ts")

    clean = raw_arr.copy()
    clean["ev"] = clean["ev"] & _EVENT_SEMANTIC_MASK

    local_order = np.argsort(clean["local_ts"], kind="mergesort")
    ordered = correct_event_order(
        clean,
        np.argsort(clean["exch_ts"], kind="mergesort"),
        local_order,
    )
    clears = sorted(local_book_clears or ())
    if clears:
        local_positions = np.flatnonzero((ordered["ev"] & LOCAL_EVENT) != 0)
        local_times = ordered["local_ts"][local_positions]
        merged = np.empty(len(ordered) + len(clears), dtype=event_dtype)
        begin = 0
        for offset, (local_ts, raw_boundary) in enumerate(clears):
            first = int(np.searchsorted(local_times, local_ts, side="left"))
            last = int(np.searchsorted(local_times, local_ts, side="right"))
            # Only break timestamp ties by archive order. Sorting by receive time can move
            # a trade across the cutover, so its original index cannot determine the time.
            local_index = first + int(np.searchsorted(local_order[first:last], raw_boundary))
            end = int(local_positions[local_index]) if local_index < len(local_positions) else len(ordered)
            merged[begin + offset:end + offset] = ordered[begin:end]
            merged[end + offset] = (
                int(LOCAL_EVENT | DEPTH_CLEAR_EVENT), 0, local_ts, 0.0, 0.0, 0, 0, 0.0
            )
            begin = end
        merged[begin + len(clears):] = ordered[begin:]
        ordered = merged
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
        "split_extra_rows": int(len(ordered) - len(raw_arr) - len(clears)),
        "local_only_book_clears": int(len(clears)),
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
    buf.last_exch_ts = max(buf.last_exch_ts, exch_ts_ns)
    buf.last_local_ts = max(buf.last_local_ts, local_ts_ns)
    buf.apply_levels(is_snapshot, bid_levels, ask_levels, local_ts_ns)


def _parse_price_qty_pairs(raw_levels: list[Any]) -> list[tuple[float, float]]:
    out: list[tuple[float, float]] = []
    for item in raw_levels:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            out.append((float(item[0]), float(item[1])))
    return out


def _parse_and_validate_depth_side(
    raw_levels: Any,
    *,
    required: bool,
    require_non_empty: bool,
    require_string_size: bool,
    is_bid: bool,
) -> tuple[list[tuple[float, float]] | None, float | None, str | None]:
    if raw_levels is None:
        if required:
            return None, None, "missing_levels_array"
        return [], None, None
    if not isinstance(raw_levels, list):
        return None, None, "levels_not_list"
    parsed: list[tuple[float, float]] = []
    positive_count = 0
    best_px: float | None = None
    for item in raw_levels:
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            return None, None, "malformed_level_entry"
        raw_px, raw_qty = item[0], item[1]
        if isinstance(raw_px, bool):
            return None, None, "invalid_price_type"
        try:
            px = float(raw_px)
        except (TypeError, ValueError):
            return None, None, "non_numeric_price"
        if not math.isfinite(px) or px <= 0.0:
            return None, None, "non_positive_or_non_finite_price"
        if require_string_size:
            if not isinstance(raw_qty, str) or not raw_qty.strip():
                return None, None, "gate_size_not_decimal_string"
            try:
                qty = float(raw_qty)
            except ValueError:
                return None, None, "non_numeric_size"
        else:
            if isinstance(raw_qty, bool):
                return None, None, "invalid_size_type"
            try:
                qty = float(raw_qty)
            except (TypeError, ValueError):
                return None, None, "non_numeric_size"
        if not math.isfinite(qty) or qty < 0.0:
            return None, None, "negative_or_non_finite_size"
        parsed.append((px, qty))
        if qty > 0.0:
            positive_count += 1
            if best_px is None:
                best_px = px
            else:
                best_px = max(best_px, px) if is_bid else min(best_px, px)
    if require_non_empty and positive_count == 0:
        return None, None, "empty_snapshot_levels"
    return parsed, best_px, None


def _parse_and_validate_depth_sides(
    raw_bids: Any,
    raw_asks: Any,
    *,
    is_snap: bool,
    require_string_size: bool,
) -> tuple[list[tuple[float, float]] | None, list[tuple[float, float]] | None, str | None]:
    bids, best_bid, err_b = _parse_and_validate_depth_side(
        raw_bids,
        required=is_snap,
        require_non_empty=is_snap,
        require_string_size=require_string_size,
        is_bid=True,
    )
    if err_b is not None:
        return None, None, f"bids:{err_b}"
    asks, best_ask, err_a = _parse_and_validate_depth_side(
        raw_asks,
        required=is_snap,
        require_non_empty=is_snap,
        require_string_size=require_string_size,
        is_bid=False,
    )
    if err_a is not None:
        return None, None, f"asks:{err_a}"
    if is_snap and best_bid is not None and best_ask is not None and best_bid >= best_ask:
        return None, None, "crossed_snapshot_book"
    return bids, asks, None


def _ingest_binance_record(
    rec: dict[str, Any],
    date_utc: str,
    buffers: dict[tuple[str, str, str], AssetDayBuffers],
) -> None:
    local_ts = rec["recv_wall_ns"]
    kind = rec["kind"]
    obj = json.loads(rec["payload"])

    if kind == "META":
        ev_name = obj.get("event")
        if ev_name in ("REST_SNAPSHOT", "REST_SNAPSHOT_ANCHOR"):
            snapshot = obj.get("raw") if ev_name == "REST_SNAPSHOT" else obj.get("snapshot")
            sym = canonical_asset("binance_futures", str(obj.get("symbol", "")))
            buf = buffers.setdefault(
                ("binance_futures", sym, date_utc),
                AssetDayBuffers("binance_futures", sym, date_utc),
            )
            ready_local_ts = int(obj.get("snapshot_ready_wall_ns") or local_ts)
            rec_boot = str(obj.get("boot_id") or rec.get("boot_id") or "")
            rec_conn = str(obj.get("conn_id") or rec.get("conn_id") or "")
            rec_epoch = int(obj["epoch"]) if obj.get("epoch") is not None else int(rec.get("epoch", 0))
            if not isinstance(snapshot, dict):
                buf.binance_depth_anchor = None
                buf.emit_book_clear_if_active(ready_local_ts)
                buf.record_unrebuildable_depth(
                    reason="invalid_depth_snapshot_payload",
                    wall_ns=ready_local_ts,
                    conn_id=rec_conn,
                    epoch=rec_epoch,
                )
                return
            raw = snapshot
            bids, asks, err = _parse_and_validate_depth_sides(
                raw.get("bids"),
                raw.get("asks"),
                is_snap=True,
                require_string_size=False,
            )
            if err is not None or bids is None or asks is None:
                buf.binance_depth_anchor = None
                buf.emit_book_clear_if_active(ready_local_ts)
                buf.record_unrebuildable_depth(
                    reason="invalid_depth_snapshot_payload",
                    wall_ns=ready_local_ts,
                    conn_id=rec_conn,
                    epoch=rec_epoch,
                )
                return
            exch_ts = int(raw.get("E") or raw.get("T") or (ready_local_ts // 1_000_000)) * 1_000_000
            last_uid = int(obj.get("lastUpdateId") or raw.get("lastUpdateId") or 0)
            buf.close_unavailable_interval_on_snapshot(
                boot_id=rec_boot,
                conn_id=rec_conn,
                epoch=rec_epoch,
                recovered_wall_ns=ready_local_ts,
            )
            buf.binance_depth_anchor = {
                "boot_id": rec_boot,
                "conn_id": rec_conn,
                "epoch": rec_epoch,
                "last_update_id": last_uid,
                "last_u": None,
                "awaiting_first_delta": True,
            }
            _emit_depth_batch(buf, True, exch_ts, ready_local_ts, bids, asks)
        elif ev_name == "DEPTH_BRIDGED_DELTA":
            sym = canonical_asset("binance_futures", str(obj.get("symbol", "")))
            buf = buffers.setdefault(
                ("binance_futures", sym, date_utc),
                AssetDayBuffers("binance_futures", sym, date_utc),
            )
            rec_boot = str(obj.get("boot_id") or rec.get("boot_id") or "")
            conn_id = str(obj.get("conn_id") or rec.get("conn_id") or "")
            epoch = int(obj["epoch"]) if obj.get("epoch") is not None else int(rec.get("epoch", 0))
            anchor = buf.binance_depth_anchor
            if (
                anchor is None
                or anchor.get("boot_id", "") != rec_boot
                or anchor["conn_id"] != conn_id
                or anchor["epoch"] != epoch
            ):
                buf.record_unrebuildable_depth(
                    reason="missing_same_connection_anchor",
                    wall_ns=local_ts,
                    conn_id=conn_id,
                    epoch=epoch,
                )
                buf.binance_depth_anchor = None
                return
            raw_ws = obj.get("raw")
            raw_obj = json.loads(raw_ws) if isinstance(raw_ws, str) else raw_ws
            if not isinstance(raw_obj, dict):
                raise ValueError(f"Binance DEPTH_BRIDGED_DELTA missing raw WS payload for {sym}")
            data = raw_obj.get("data") if isinstance(raw_obj.get("data"), dict) else raw_obj
            first_u = int(data.get("U") if data.get("U") is not None else obj["first_u"])
            last_u = int(data.get("u") if data.get("u") is not None else obj["last_u"])
            prev_u = int(data.get("pu") if data.get("pu") is not None else obj["prev_u"])
            snap_uid = int(anchor["last_update_id"])
            if anchor["awaiting_first_delta"]:
                if not (first_u <= snap_uid <= last_u):
                    raise ValueError(
                        f"Binance bridged first delta violates U<=S<=u for {sym}: "
                        f"U={first_u}, S={snap_uid}, u={last_u}"
                    )
                anchor["awaiting_first_delta"] = False
            elif anchor["last_u"] is not None and prev_u != anchor["last_u"]:
                raise ValueError(
                    f"Binance bridged delta pu break for {sym}: pu={prev_u} != prev_u={anchor['last_u']}"
                )
            anchor["last_u"] = last_u
            orig_wall_ns = int(obj.get("orig_recv_wall_ns") or 0)
            ready_wall_ns = int(obj.get("ready_wall_ns") or local_ts)
            if orig_wall_ns > 0 and ready_wall_ns < orig_wall_ns:
                raise ValueError(
                    f"Binance bridged delta lookahead timestamp for {sym}: "
                    f"ready={ready_wall_ns} < orig={orig_wall_ns}"
                )
            exch_ms = int(data.get("T") or data.get("E") or (ready_wall_ns // 1_000_000))
            exch_ts = exch_ms * 1_000_000
            bids, asks, err = _parse_and_validate_depth_sides(
                data.get("b"),
                data.get("a"),
                is_snap=False,
                require_string_size=False,
            )
            if err is not None or bids is None or asks is None:
                buf.binance_depth_anchor = None
                buf.emit_book_clear_if_active(ready_wall_ns)
                buf.record_unrebuildable_depth(
                    reason="invalid_depth_delta_payload",
                    wall_ns=ready_wall_ns,
                    conn_id=conn_id,
                    epoch=epoch,
                )
                return
            buf.bridged_delta_records.append(
                {
                    "conn_id": conn_id,
                    "epoch": epoch,
                    "bridge_index": int(obj.get("bridge_index") or 0),
                    "orig_recv_wall_ns": orig_wall_ns,
                    "orig_recv_mono_ns": int(obj.get("orig_recv_mono_ns") or 0),
                    "ready_wall_ns": ready_wall_ns,
                    "ready_mono_ns": int(obj.get("ready_mono_ns") or rec.get("recv_mono_ns", 0)),
                    "first_u": first_u,
                    "last_u": last_u,
                    "prev_u": prev_u,
                }
            )
            _emit_depth_batch(buf, False, exch_ts, ready_wall_ns, bids, asks)
        elif ev_name in ("DEPTH_UNAVAILABLE", "SNAPSHOT_BRIDGE_RETRY"):
            raw_sym = str(obj.get("symbol") or "")
            if raw_sym:
                sym = canonical_asset("binance_futures", raw_sym)
                buf = buffers.setdefault(
                    ("binance_futures", sym, date_utc),
                    AssetDayBuffers("binance_futures", sym, date_utc),
                )
                buf.binance_depth_anchor = None
                buf.emit_book_clear_if_active(local_ts)
                since_ns = int(obj.get("unavailable_since_wall_ns") or local_ts)
                if not buf.depth_unavailable_intervals or buf.depth_unavailable_intervals[-1].get("end_wall_ns") is not None:
                    buf.depth_unavailable_intervals.append(
                        {
                            "start_wall_ns": since_ns,
                            "end_wall_ns": None,
                            "reason": str(obj.get("reason") or ev_name),
                            "planned": bool(obj.get("planned")),
                            "boot_id": str(obj.get("boot_id") or rec.get("boot_id") or ""),
                            "conn_id": str(obj.get("conn_id") or rec.get("conn_id") or ""),
                            "epoch": int(obj["epoch"]) if obj.get("epoch") is not None else int(rec.get("epoch", 0)),
                        }
                    )
                else:
                    iv = buf.depth_unavailable_intervals[-1]
                    iv["planned"] = bool(iv.get("planned")) and bool(obj.get("planned"))
                    iv["boot_id"] = str(obj.get("boot_id") or rec.get("boot_id") or iv.get("boot_id") or "")
                    iv["conn_id"] = str(obj.get("conn_id") or rec.get("conn_id") or iv.get("conn_id") or "")
                    iv["epoch"] = int(obj["epoch"]) if obj.get("epoch") is not None else int(rec.get("epoch", iv.get("epoch", 0)))
        elif ev_name in ("SWITCH", "GAP_START", "LOCAL_LOSS"):
            to_conn = str(obj.get("to") or "") if ev_name == "SWITCH" else ""
            for (ex, _sym, d_utc), b in buffers.items():
                if ex == "binance_futures" and d_utc == date_utc:
                    if ev_name != "SWITCH" or b.binance_depth_anchor is None or not to_conn or b.binance_depth_anchor.get("conn_id") != to_conn:
                        b.binance_depth_anchor = None
                        b.emit_book_clear_if_active(local_ts)
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
        buf = buffers.setdefault(
            ("binance_futures", sym, date_utc),
            AssetDayBuffers("binance_futures", sym, date_utc),
        )
        anchor = buf.binance_depth_anchor
        rec_boot = str(rec.get("boot_id") or "")
        rec_conn = str(rec.get("conn_id") or "")
        rec_epoch = int(rec.get("epoch", 0))
        bids, asks, level_err = _parse_and_validate_depth_sides(
            data.get("b"),
            data.get("a"),
            is_snap=False,
            require_string_size=False,
        )
        if level_err is not None or bids is None or asks is None:
            buf.binance_depth_anchor = None
            buf.emit_book_clear_if_active(local_ts)
            buf.record_unrebuildable_depth(
                reason="invalid_depth_delta_payload",
                wall_ns=local_ts,
                conn_id=rec_conn,
                epoch=rec_epoch,
            )
            return
        if (
            anchor is None
            or anchor.get("boot_id", "") != rec_boot
            or anchor["conn_id"] != rec_conn
            or anchor["epoch"] != rec_epoch
        ):
            buf.record_unrebuildable_depth(
                reason="missing_same_connection_anchor",
                wall_ns=local_ts,
                conn_id=rec_conn,
                epoch=rec_epoch,
            )
            buf.binance_depth_anchor = None
            buf.emit_book_clear_if_active(local_ts)
            return
        if "U" in data and "u" in data:
            first_u = int(data["U"])
            last_u = int(data["u"])
            prev_u = int(data["pu"]) if "pu" in data else None
            snap_uid = int(anchor["last_update_id"])
            if anchor["awaiting_first_delta"]:
                if last_u < snap_uid:
                    return
                if not (first_u <= snap_uid <= last_u):
                    buf.record_unrebuildable_depth(
                        reason="exchange_update_chain_break",
                        wall_ns=local_ts,
                        conn_id=rec_conn,
                        epoch=rec_epoch,
                    )
                    buf.binance_depth_anchor = None
                    return
                anchor["awaiting_first_delta"] = False
            elif anchor["last_u"] is not None and prev_u is not None and prev_u != anchor["last_u"]:
                buf.record_unrebuildable_depth(
                    reason="exchange_update_chain_break",
                    wall_ns=local_ts,
                    conn_id=rec_conn,
                    epoch=rec_epoch,
                )
                buf.binance_depth_anchor = None
                return
            anchor["last_u"] = last_u
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


def reconstruct_binance_depth_book(
    records: list[dict[str, Any]],
    symbol: str,
) -> dict[str, Any]:
    """Strictly reconstruct one Binance symbol's L2 book from canonical TSV-Raw archive records.

    Enforces:
    - Same-connection `(conn_id, epoch)` between `REST_SNAPSHOT_ANCHOR`, `DEPTH_BRIDGED_DELTA`,
      and live `depth@100ms` DATA records (zero cross-connection splicing).
    - Official USDS-M Futures bridging invariant (`U <= S <= u` then `pu == prev_u`).
    - Distinction between `orig_recv_wall_ns` and strategy-visible `ready_wall_ns` (`ready >= orig`).
    """
    target = symbol.upper()
    target_canon = canonical_asset("binance_futures", target)
    bids: dict[float, float] = {}
    asks: dict[float, float] = {}
    anchor: dict[str, Any] | None = None
    snapshot_count = 0
    bridged_deltas: list[dict[str, Any]] = []
    live_delta_count = 0
    unavailable_intervals: list[dict[str, Any]] = []

    def _matches(raw_sym: str) -> bool:
        up = raw_sym.upper()
        return up == target or canonical_asset("binance_futures", up) == target_canon

    def _apply(is_snapshot: bool, b_levels: list[tuple[float, float]], a_levels: list[tuple[float, float]]) -> None:
        if is_snapshot:
            bids.clear()
            asks.clear()
        for px, qty in b_levels:
            if qty <= 0.0:
                bids.pop(px, None)
            else:
                bids[px] = qty
        for px, qty in a_levels:
            if qty <= 0.0:
                asks.pop(px, None)
            else:
                asks[px] = qty

    for rec in records:
        kind = rec["kind"]
        wall_ns = int(rec["recv_wall_ns"])
        obj = json.loads(rec["payload"])
        if kind == "META":
            ev_name = obj.get("event")
            if ev_name in ("REST_SNAPSHOT", "REST_SNAPSHOT_ANCHOR"):
                raw_sym = str(obj.get("symbol") or "")
                if not _matches(raw_sym):
                    continue
                snap = obj.get("snapshot") if ev_name == "REST_SNAPSHOT_ANCHOR" else obj.get("raw")
                if not isinstance(snap, dict):
                    raise ValueError(f"Malformed {ev_name} for {raw_sym}")
                rec_boot = str(obj.get("boot_id") or rec.get("boot_id") or "")
                conn_id = str(obj.get("conn_id") or rec.get("conn_id") or "")
                epoch = int(obj["epoch"]) if obj.get("epoch") is not None else int(rec.get("epoch", 0))
                last_uid = int(obj.get("lastUpdateId") or snap.get("lastUpdateId") or 0)
                ready_ns = int(obj.get("snapshot_ready_wall_ns") or wall_ns)
                if unavailable_intervals and unavailable_intervals[-1]["end_wall_ns"] is None:
                    unavailable_intervals[-1]["end_wall_ns"] = ready_ns
                anchor = {
                    "boot_id": rec_boot,
                    "conn_id": conn_id,
                    "epoch": epoch,
                    "last_update_id": last_uid,
                    "last_u": None,
                    "awaiting_first_delta": True,
                    "ready_wall_ns": ready_ns,
                }
                _apply(
                    True,
                    _parse_price_qty_pairs(snap.get("bids") or []),
                    _parse_price_qty_pairs(snap.get("asks") or []),
                )
                snapshot_count += 1
            elif ev_name == "DEPTH_BRIDGED_DELTA":
                raw_sym = str(obj.get("symbol") or "")
                if not _matches(raw_sym):
                    continue
                rec_boot = str(obj.get("boot_id") or rec.get("boot_id") or "")
                conn_id = str(obj.get("conn_id") or rec.get("conn_id") or "")
                epoch = int(obj["epoch"]) if obj.get("epoch") is not None else int(rec.get("epoch", 0))
                if (
                    anchor is None
                    or anchor.get("boot_id", "") != rec_boot
                    or anchor["conn_id"] != conn_id
                    or anchor["epoch"] != epoch
                ):
                    raise ValueError(
                        f"DEPTH_BRIDGED_DELTA without matching same-connection anchor for {raw_sym}: "
                        f"delta=({rec_boot},{conn_id},{epoch}) anchor={anchor}"
                    )
                raw_ws = obj.get("raw")
                raw_obj = json.loads(raw_ws) if isinstance(raw_ws, str) else raw_ws
                data = raw_obj.get("data") if isinstance(raw_obj.get("data"), dict) else raw_obj
                first_u = int(data["U"])
                last_u = int(data["u"])
                prev_u = int(data["pu"])
                snap_uid = int(anchor["last_update_id"])
                if anchor["awaiting_first_delta"]:
                    if not (first_u <= snap_uid <= last_u):
                        raise ValueError(
                            f"Bridged first delta violates U<=S<=u for {raw_sym}: "
                            f"U={first_u}, S={snap_uid}, u={last_u}"
                        )
                    anchor["awaiting_first_delta"] = False
                elif prev_u != anchor["last_u"]:
                    raise ValueError(
                        f"Bridged delta pu break for {raw_sym}: pu={prev_u} != prev_u={anchor['last_u']}"
                    )
                anchor["last_u"] = last_u
                orig_wall_ns = int(obj["orig_recv_wall_ns"])
                ready_wall_ns = int(obj["ready_wall_ns"])
                if ready_wall_ns < orig_wall_ns:
                    raise ValueError(
                        f"Bridged delta ready_wall_ns < orig_recv_wall_ns: {ready_wall_ns} < {orig_wall_ns}"
                    )
                _apply(
                    False,
                    _parse_price_qty_pairs(data.get("b") or []),
                    _parse_price_qty_pairs(data.get("a") or []),
                )
                bridged_deltas.append(
                    {
                        "conn_id": conn_id,
                        "epoch": epoch,
                        "bridge_index": int(obj.get("bridge_index") or 0),
                        "orig_recv_wall_ns": orig_wall_ns,
                        "orig_recv_mono_ns": int(obj.get("orig_recv_mono_ns") or 0),
                        "ready_wall_ns": ready_wall_ns,
                        "ready_mono_ns": int(obj.get("ready_mono_ns") or 0),
                        "first_u": first_u,
                        "last_u": last_u,
                        "prev_u": prev_u,
                    }
                )
            elif ev_name in ("DEPTH_UNAVAILABLE", "SNAPSHOT_BRIDGE_RETRY"):
                raw_sym = str(obj.get("symbol") or "")
                if raw_sym and _matches(raw_sym):
                    anchor = None
                    since_ns = int(obj.get("unavailable_since_wall_ns") or wall_ns)
                    if not unavailable_intervals or unavailable_intervals[-1]["end_wall_ns"] is not None:
                        unavailable_intervals.append(
                            {
                                "start_wall_ns": since_ns,
                                "end_wall_ns": None,
                                "reason": str(obj.get("reason") or ev_name),
                                "conn_id": str(obj.get("conn_id") or rec.get("conn_id") or ""),
                                "epoch": int(obj["epoch"]) if obj.get("epoch") is not None else int(rec.get("epoch", 0)),
                            }
                        )
            elif ev_name == "SWITCH":
                to_conn = str(obj.get("to") or "")
                if anchor is not None and (not to_conn or anchor["conn_id"] != to_conn):
                    anchor = None
            continue

        stream = obj.get("stream", "")
        data = obj.get("data") if isinstance(obj.get("data"), dict) else obj
        if data.get("e") == "depthUpdate" or "@depth" in stream:
            raw_sym = str(data.get("s") or "")
            if not _matches(raw_sym):
                continue
            rec_boot = str(rec.get("boot_id") or "")
            rec_conn = str(rec.get("conn_id") or "")
            rec_epoch = int(rec.get("epoch", 0))
            if anchor is None:
                raise ValueError(f"Unanchored live Binance depthUpdate for {raw_sym} at wall_ns={wall_ns}")
            if (
                anchor.get("boot_id", "") != rec_boot
                or anchor["conn_id"] != rec_conn
                or anchor["epoch"] != rec_epoch
            ):
                raise ValueError(
                    f"Cross-connection Binance depthUpdate for {raw_sym}: "
                    f"delta=({rec_boot},{rec_conn},{rec_epoch}) anchor=({anchor.get('boot_id', '')},{anchor['conn_id']},{anchor['epoch']})"
                )
            first_u = int(data["U"])
            last_u = int(data["u"])
            prev_u = int(data["pu"])
            snap_uid = int(anchor["last_update_id"])
            if anchor["awaiting_first_delta"]:
                if last_u < snap_uid:
                    continue
                if not (first_u <= snap_uid <= last_u):
                    raise ValueError(
                        f"First live delta violates U<=S<=u for {raw_sym}: U={first_u}, S={snap_uid}, u={last_u}"
                    )
                anchor["awaiting_first_delta"] = False
            elif prev_u != anchor["last_u"]:
                raise ValueError(
                    f"Live delta pu break for {raw_sym}: pu={prev_u} != prev_u={anchor['last_u']}"
                )
            anchor["last_u"] = last_u
            _apply(
                False,
                _parse_price_qty_pairs(data.get("b") or []),
                _parse_price_qty_pairs(data.get("a") or []),
            )
            live_delta_count += 1

    return {
        "symbol": target,
        "active_conn_id": anchor["conn_id"] if anchor else None,
        "active_epoch": anchor["epoch"] if anchor else None,
        "last_update_id": anchor["last_update_id"] if anchor else None,
        "last_u": anchor["last_u"] if anchor else None,
        "snapshot_count": snapshot_count,
        "bridged_delta_count": len(bridged_deltas),
        "live_delta_count": live_delta_count,
        "bridged_deltas": bridged_deltas,
        "unavailable_intervals": unavailable_intervals,
        "bids": dict(bids),
        "asks": dict(asks),
        "sorted_bids": sorted(bids.items(), key=lambda kv: kv[0], reverse=True),
        "sorted_asks": sorted(asks.items(), key=lambda kv: kv[0]),
    }


def _ingest_follower_meta(
    exchange: str,
    rec: dict[str, Any],
    date_utc: str,
    buffers: dict[tuple[str, str, str], AssetDayBuffers],
) -> None:
    local_ts = rec["recv_wall_ns"]
    obj = json.loads(rec["payload"])
    if not isinstance(obj, dict):
        return
    ev_name = str(obj.get("event") or "")
    raw_sym = str(obj.get("symbol") or "ALL").strip()
    is_all = not raw_sym or raw_sym.upper() == "ALL"

    def _target_buffers() -> list[AssetDayBuffers]:
        if is_all:
            return [
                b
                for (ex, _sym, d_utc), b in buffers.items()
                if ex == exchange and d_utc == date_utc
            ]
        sym = canonical_asset(exchange, raw_sym)
        return [
            buffers.setdefault(
                (exchange, sym, date_utc),
                AssetDayBuffers(exchange, sym, date_utc),
            )
        ]

    if ev_name in ("SWITCH", "GAP_START", "LOCAL_LOSS"):
        for buf in _target_buffers():
            buf.follower_depth_anchor = None
            buf.emit_book_clear_if_active(local_ts)
    elif ev_name == "DEPTH_UNAVAILABLE":
        since_ns = int(obj.get("unavailable_since_wall_ns") or local_ts)
        for buf in _target_buffers():
            buf.follower_depth_anchor = None
            buf.emit_book_clear_if_active(local_ts)
            if not buf.depth_unavailable_intervals or buf.depth_unavailable_intervals[-1].get("end_wall_ns") is not None:
                buf.depth_unavailable_intervals.append(
                    {
                        "start_wall_ns": since_ns,
                        "end_wall_ns": None,
                        "reason": str(obj.get("reason") or ev_name),
                        "planned": bool(obj.get("planned")),
                        "boot_id": str(obj.get("boot_id") or rec.get("boot_id") or ""),
                        "conn_id": str(obj.get("conn_id") or rec.get("conn_id") or ""),
                        "epoch": int(obj["epoch"]) if obj.get("epoch") is not None else int(rec.get("epoch", 0)),
                    }
                )
            else:
                iv = buf.depth_unavailable_intervals[-1]
                iv["planned"] = bool(iv.get("planned")) and bool(obj.get("planned"))
                iv["boot_id"] = str(obj.get("boot_id") or rec.get("boot_id") or iv.get("boot_id") or "")
                iv["conn_id"] = str(obj.get("conn_id") or rec.get("conn_id") or iv.get("conn_id") or "")
                iv["epoch"] = int(obj["epoch"]) if obj.get("epoch") is not None else int(rec.get("epoch", iv.get("epoch", 0)))
    elif ev_name == "DEPTH_RECOVERED" and not is_all:
        sym = canonical_asset(exchange, raw_sym)
        buf = buffers.setdefault(
            (exchange, sym, date_utc),
            AssetDayBuffers(exchange, sym, date_utc),
        )
        # Never close an open depth_unavailable_interval from META alone; only an actual
        # valid on-disk snapshot closes end_wall_ns.
        buf.depth_recovered_count += 1


def _validate_and_apply_follower_depth(
    *,
    buf: AssetDayBuffers,
    exchange: str,
    rec: dict[str, Any],
    is_snap: bool,
    seq: int | None,
    parent: int | None,
    exch_ts: int,
    local_ts: int,
    raw_bids: Any,
    raw_asks: Any,
) -> None:
    rec_boot = str(rec.get("boot_id") or "")
    rec_conn = str(rec.get("conn_id") or "")
    rec_epoch = int(rec.get("epoch", 0))
    bids, asks, level_err = _parse_and_validate_depth_sides(
        raw_bids,
        raw_asks,
        is_snap=is_snap,
        require_string_size=(exchange == "gate_futures"),
    )
    if level_err is not None or bids is None or asks is None:
        buf.follower_depth_anchor = None
        buf.emit_book_clear_if_active(local_ts)
        buf.record_unrebuildable_depth(
            reason="invalid_depth_snapshot_payload" if is_snap else "invalid_depth_delta_payload",
            wall_ns=local_ts,
            conn_id=rec_conn,
            epoch=rec_epoch,
        )
        return
    if is_snap:
        buf.close_unavailable_interval_on_snapshot(
            boot_id=rec_boot,
            conn_id=rec_conn,
            epoch=rec_epoch,
            recovered_wall_ns=local_ts,
        )
        buf.follower_depth_anchor = {
            "boot_id": rec_boot,
            "conn_id": rec_conn,
            "epoch": rec_epoch,
            "seq": seq,
            "awaiting_delta": exchange == "bitget_v3",
        }
        _emit_depth_batch(buf, True, exch_ts, local_ts, bids, asks)
        return

    anchor = buf.follower_depth_anchor
    if (
        anchor is None
        or anchor.get("boot_id", "") != rec_boot
        or anchor["conn_id"] != rec_conn
        or anchor["epoch"] != rec_epoch
    ):
        buf.record_unrebuildable_depth(
            reason="missing_same_connection_anchor",
            wall_ns=local_ts,
            conn_id=rec_conn,
            epoch=rec_epoch,
        )
        buf.follower_depth_anchor = None
        buf.emit_book_clear_if_active(local_ts)
        return

    prev_seq = anchor.get("seq")
    if prev_seq is not None and seq is not None:
        if exchange == "gate_futures":
            valid = parent is not None and parent == prev_seq + 1 and seq >= parent
        elif exchange == "bitget_v3":
            if parent is None or parent >= seq:
                valid = False
            elif anchor.get("awaiting_delta"):
                valid = parent <= prev_seq <= seq
            else:
                valid = parent == prev_seq
        else:
            valid = seq >= prev_seq
        if not valid:
            buf.record_unrebuildable_depth(
                reason="exchange_update_chain_break",
                wall_ns=local_ts,
                conn_id=rec_conn,
                epoch=rec_epoch,
            )
            buf.follower_depth_anchor = None
            buf.emit_book_clear_if_active(local_ts)
            return

    anchor["seq"] = seq
    anchor["awaiting_delta"] = False
    _emit_depth_batch(buf, False, exch_ts, local_ts, bids, asks)


def _ingest_gate_record(
    rec: dict[str, Any],
    date_utc: str,
    buffers: dict[tuple[str, str, str], AssetDayBuffers],
) -> None:
    if rec["kind"] == "META":
        _ingest_follower_meta("gate_futures", rec, date_utc, buffers)
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
        seq = int(result["u"]) if result.get("u") is not None else None
        parent = int(result["U"]) if result.get("U") is not None else None
        # Decimal strings (X-Gate-Size-Decimal: 1) validated & parsed as float, NEVER int!
        buf = buffers.setdefault(
            ("gate_futures", sym, date_utc),
            AssetDayBuffers("gate_futures", sym, date_utc),
        )
        _validate_and_apply_follower_depth(
            buf=buf,
            exchange="gate_futures",
            rec=rec,
            is_snap=is_full,
            seq=seq,
            parent=parent,
            exch_ts=exch_ts,
            local_ts=local_ts,
            raw_bids=result.get("b"),
            raw_asks=result.get("a"),
        )

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
            buf.last_exch_ts = max(buf.last_exch_ts, exch_ts)
            buf.last_local_ts = max(buf.last_local_ts, local_ts)

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
        _ingest_follower_meta("bybit_linear", rec, date_utc, buffers)
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
        seq = int(data["u"]) if data.get("u") is not None else None
        is_snap = obj.get("type") == "snapshot" or seq == 1
        buf = buffers.setdefault(
            ("bybit_linear", sym, date_utc),
            AssetDayBuffers("bybit_linear", sym, date_utc),
        )
        _validate_and_apply_follower_depth(
            buf=buf,
            exchange="bybit_linear",
            rec=rec,
            is_snap=is_snap,
            seq=seq,
            parent=None,
            exch_ts=exch_ts,
            local_ts=local_ts,
            raw_bids=data.get("b"),
            raw_asks=data.get("a"),
        )

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
            buf.last_exch_ts = max(buf.last_exch_ts, exch_ts)
            buf.last_local_ts = max(buf.last_local_ts, local_ts)


def _ingest_bitget_record(
    rec: dict[str, Any],
    date_utc: str,
    buffers: dict[tuple[str, str, str], AssetDayBuffers],
) -> None:
    if rec["kind"] == "META":
        _ingest_follower_meta("bitget_v3", rec, date_utc, buffers)
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
            seq = int(item["seq"]) if item.get("seq") is not None else None
            parent = int(item["pseq"]) if item.get("pseq") is not None else None
            raw_bids = item["b"] if "b" in item else item.get("bids")
            raw_asks = item["a"] if "a" in item else item.get("asks")
            buf = buffers.setdefault(
                ("bitget_v3", sym, date_utc),
                AssetDayBuffers("bitget_v3", sym, date_utc),
            )
            _validate_and_apply_follower_depth(
                buf=buf,
                exchange="bitget_v3",
                rec=rec,
                is_snap=is_snap,
                seq=seq,
                parent=parent,
                exch_ts=exch_ts,
                local_ts=local_ts,
                raw_bids=raw_bids,
                raw_asks=raw_asks,
            )

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
            buf.last_exch_ts = max(buf.last_exch_ts, exch_ts)
            buf.last_local_ts = max(buf.last_local_ts, local_ts)


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
    depth_unavail_cnt = 0

    for q in quotes:
        ts = int(q["ts_ns"])
        side = str(q["side"]).lower()
        px = float(q["price"])
        idx = int(np.searchsorted(edge_ts, ts, side="right")) - 1
        extra: dict[str, Any] = {}
        if idx < 0:
            cov = "QUEUE_UNKNOWN"
            reason = "before_first_book_snapshot"
        else:
            _, best_bid, edge_bid, best_ask, edge_ask, n_b, n_a = edge_samples[idx]
            if (
                int(n_b) <= 0
                or int(n_a) <= 0
                or not math.isfinite(float(best_bid))
                or not math.isfinite(float(best_ask))
            ):
                cov = "DEPTH_UNAVAILABLE"
                reason = "depth_unavailable_gap"
                extra["queue_assumption"] = "UNOBSERVABLE_DEPTH_GAP"
            elif side == "buy":
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
            if cov == "DEPTH_UNAVAILABLE":
                depth_unavail_cnt += 1
        labeled.append({**q, "depth_coverage": cov, "coverage_reason": reason, **extra})

    return {
        "total": len(labeled),
        "visible_within_50": visible_cnt,
        "queue_unknown": unknown_cnt,
        "depth_unavailable": depth_unavail_cnt,
        "queue_unknown_ratio": (unknown_cnt / len(labeled)) if labeled else 0.0,
        "items": labeled,
    }


def audit_fills_against_depth_unavailable_intervals(
    fills: list[dict[str, Any]],
    depth_unavailable_intervals: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    """Explicitly account for simulated fills (including pre-switch resting orders triggered by
    public trades) occurring inside `depth_unavailable_intervals`.

    Policy (`retain_raw_fill_and_flag_unverified`, matching SPEC_v1.4):
    - Never silently drop or selectively filter out fills from raw engine PnL / position accounting.
    - Any fill whose execution or local-observation timestamp falls within an unavailable depth
      interval `[start_wall_ns, end_wall_ns)` (or `>= start_wall_ns` if unrecovered) is explicitly
      tagged `execution_status="depth_unavailable_unverified"` and `queue_assumption="UNOBSERVABLE_DEPTH_GAP"`.
    - A gap fill or missing interval metadata makes the overall audit `"execution_unverified"`.
    """
    labeled: list[dict[str, Any]] = []
    verified_cnt = 0
    unverified_cnt = 0
    unverified_qty = 0.0
    hit_intervals: list[dict[str, Any]] = []
    seen_iv_keys: set[tuple[int, int | None]] = set()

    for fill in fills:
        cand_ts: list[int] = []
        for k in ("ts_ns", "exch_ts_ns", "local_ts_ns", "observed_ts_ns"):
            val = fill.get(k)
            if val is not None and int(val) > 0:
                cand_ts.append(int(val))
        matched_iv: dict[str, Any] | None = None
        for iv in depth_unavailable_intervals or ():
            start_ns = int(iv.get("start_wall_ns") or 0)
            end_raw = iv.get("end_wall_ns")
            end_ns = int(end_raw) if end_raw is not None else None
            for t in cand_ts:
                if t >= start_ns and (end_ns is None or t < end_ns):
                    matched_iv = iv
                    break
            if matched_iv is not None:
                break

        if matched_iv is not None:
            unverified_cnt += 1
            unverified_qty += abs(float(fill.get("qty") or fill.get("exec_qty") or 0.0))
            iv_key = (
                int(matched_iv.get("start_wall_ns") or 0),
                int(matched_iv["end_wall_ns"]) if matched_iv.get("end_wall_ns") is not None else None,
            )
            if iv_key not in seen_iv_keys:
                seen_iv_keys.add(iv_key)
                hit_intervals.append(matched_iv)
            labeled.append(
                {
                    **fill,
                    "execution_status": "depth_unavailable_unverified",
                    "queue_assumption": "UNOBSERVABLE_DEPTH_GAP",
                    "matched_interval": matched_iv,
                }
            )
        elif depth_unavailable_intervals is None:
            labeled.append({
                **fill,
                "execution_status": "depth_availability_unknown",
                "queue_assumption": "UNKNOWN_DEPTH_AVAILABILITY",
            })
        else:
            verified_cnt += 1
            labeled.append(
                {
                    **fill,
                    "execution_status": "verified",
                    "queue_assumption": "OBSERVABLE_BOOK",
                }
            )

    intervals_available = depth_unavailable_intervals is not None
    return {
        "status": "execution_unverified" if unverified_cnt or not intervals_available else "verified",
        "policy": "retain_raw_fill_and_flag_unverified",
        "depth_intervals_available": intervals_available,
        "total_fills": len(labeled),
        "verified_fills": verified_cnt,
        "unassessed_fills": len(labeled) - verified_cnt - unverified_cnt,
        "unverified_gap_fills": unverified_cnt,
        "unverified_gap_fill_qty": unverified_qty,
        "unverified_intervals_hit": hit_intervals,
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

        from data_coverage import normalize_manifest_window, record_monotonic_producer_key

        for exchange, items in sorted(by_exchange.items()):
            items.sort(
                key=lambda x: (
                    normalize_manifest_window(x[0], fallback_file_name=x[1].name)["planned_start_wall_ns"],
                    x[0].get("start_wall_ns") or 0,
                    str(x[0].get("boot_id") or ""),
                    x[0].get("first_rx_seq") or 0,
                    x[1].name,
                )
            )
            prev_boot: str | None = None
            prev_file_last_rx_seq: int | None = None
            prev_file_had_late_frames = False
            boot_seen_rx_seqs: set[int] = set()
            producer_prev_mono: dict[tuple[str, str, int], int] = {}
            total_records = 0

            for mf, dpath, mpath in items:
                norm_mf = normalize_manifest_window(mf, fallback_file_name=dpath.name)
                boot_id = norm_mf["boot_id"]
                records = parse_tsv_zst_records(dpath)
                recorded_local_loss = 0
                dropped_frames = 0
                if boot_id != prev_boot:
                    prev_boot = boot_id
                    prev_file_last_rx_seq = None
                    prev_file_had_late_frames = False
                    boot_seen_rx_seqs.clear()
                    producer_prev_mono.clear()

                file_prev_rx_seq: int | None = None
                file_first_rx_seq: int | None = None
                for rec in records:
                    rec["boot_id"] = boot_id
                    if coverage is not None:
                        coverage.observe_record(exchange, rec)
                    total_records += 1
                    rx_seq = rec["rx_seq"]
                    mono_ns = rec["recv_mono_ns"]
                    wall_ns = rec["recv_wall_ns"]
                    date_utc = date_utc_from_wall_ns(wall_ns)

                    if file_first_rx_seq is None:
                        file_first_rx_seq = rx_seq
                        if (
                            prev_file_last_rx_seq is not None
                            and not prev_file_had_late_frames
                            and rx_seq <= prev_file_last_rx_seq
                        ):
                            failed_items.append(
                                f"rx_seq_non_monotonic:{exchange}:{dpath.name}:prev={prev_file_last_rx_seq},curr={rx_seq}"
                            )

                    if file_prev_rx_seq is not None and rx_seq <= file_prev_rx_seq:
                        failed_items.append(
                            f"rx_seq_non_monotonic:{exchange}:{dpath.name}:prev={file_prev_rx_seq},curr={rx_seq}"
                        )
                    if rx_seq in boot_seen_rx_seqs:
                        failed_items.append(
                            f"rx_seq_duplicate_in_boot:{exchange}:{dpath.name}:boot={boot_id},rx_seq={rx_seq}"
                        )
                    boot_seen_rx_seqs.add(rx_seq)
                    file_prev_rx_seq = rx_seq

                    payload_obj: Any = None
                    try:
                        payload_obj = json.loads(rec["payload"])
                    except Exception:
                        payload_obj = None
                    prod_key = record_monotonic_producer_key(
                        exchange, rec["kind"], str(rec["conn_id"]), int(rec["epoch"]), payload_obj
                    )
                    prev_prod_mono = producer_prev_mono.get(prod_key)
                    if prev_prod_mono is not None and mono_ns < prev_prod_mono:
                        failed_items.append(
                            f"recv_mono_ns_regressed:{exchange}:{dpath.name}:producer={prod_key[0]}:{prod_key[1]}:{prod_key[2]}:prev={prev_prod_mono},curr={mono_ns}"
                        )
                    producer_prev_mono[prod_key] = mono_ns

                    if rec["kind"] == "META":
                        meta_obj = payload_obj if isinstance(payload_obj, dict) else json.loads(rec["payload"])
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
                        elif ev == "LATE_FRAME_REJECTED":
                            dropped = int(meta_obj.get("dropped_frames", 1))
                            target_wid = str(meta_obj.get("target_window_id") or meta_obj.get("loss_attributed_window_id") or "")
                            target_date = target_wid[:10] if len(target_wid) >= 10 else date_utc
                            recorded_local_loss += 1
                            dropped_frames += dropped
                            day_local_loss[(exchange, target_date)].append(
                                {
                                    "wall_ns": int(meta_obj.get("orig_recv_wall_ns") or wall_ns),
                                    "dropped": dropped,
                                    "reason": f"late_frame_rejected_for_{target_wid or 'prior_window'}",
                                }
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

                if file_prev_rx_seq is not None:
                    prev_file_last_rx_seq = (
                        max(prev_file_last_rx_seq, file_prev_rx_seq)
                        if prev_file_last_rx_seq is not None
                        else file_prev_rx_seq
                    )
                prev_file_had_late_frames = norm_mf["late_frames_accepted"] > 0

                manifest_local_loss = int(mf.get("local_loss_lines") or 0) + int(mf.get("late_frames_rejected") or 0)
                if recorded_local_loss or manifest_local_loss:
                    local_loss_partitions.append({
                        "exchange": exchange,
                        "date_utc": mf["date_utc"],
                        "hour_utc": mf["hour_utc"],
                        "window_id": norm_mf["window_id"],
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

            unrecovered_unavail = [
                iv for iv in buf.depth_unavailable_intervals if iv.get("end_wall_ns") is None
            ]
            for iv in unrecovered_unavail:
                buf.record_unrebuildable_depth(
                    reason="depth_unavailable_unrecovered",
                    wall_ns=int(iv.get("start_wall_ns") or 0),
                    conn_id=str(iv.get("conn_id") or ""),
                    epoch=int(iv.get("epoch") or 0),
                )
            for r_code in buf.unrebuildable_depth_reasons:
                if r_code not in reasons:
                    reasons.append(r_code)

            if buf.hbt_rows and not buf.unrebuildable_depth_reasons:
                raw_arr = np.array(buf.hbt_rows, dtype=event_dtype)
                ordered, order_diag = _order_and_validate_hbt_events(
                    raw_arr,
                    exchange=exchange,
                    symbol=sym,
                    date_utc=date_utc,
                    stream_kind="binance_depth_diag",
                    local_book_clears=buf.local_book_clears,
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
            unrecovered_unavail = [
                iv for iv in buf.depth_unavailable_intervals if iv.get("end_wall_ns") is None
            ]
            for iv in unrecovered_unavail:
                buf.record_unrebuildable_depth(
                    reason="depth_unavailable_unrecovered",
                    wall_ns=int(iv.get("start_wall_ns") or 0),
                    conn_id=str(iv.get("conn_id") or ""),
                    epoch=int(iv.get("epoch") or 0),
                )
            for r_code in buf.unrebuildable_depth_reasons:
                if r_code not in reasons:
                    reasons.append(r_code)

            depth_rebuildable = buf.seen_snapshot and not buf.unrebuildable_depth_reasons
            if buf.hbt_rows and depth_rebuildable:
                raw_arr = np.array(buf.hbt_rows, dtype=event_dtype)
                ordered, order_diag = _order_and_validate_hbt_events(
                    raw_arr,
                    exchange=exchange,
                    symbol=sym,
                    date_utc=date_utc,
                    stream_kind="follower_l2_hbt",
                    local_book_clears=buf.local_book_clears,
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

            if buf.edge_samples and depth_rebuildable:
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
        depth_rebuild_status = (
            "unrebuildable_excluded"
            if buf.unrebuildable_depth_reasons
            else ("ok" if (exchange == "binance_futures" or buf.seen_snapshot) else "missing_anchor")
        )
        calendar_rows.append(
            {
                "exchange": exchange,
                "symbol": sym,
                "date_utc": date_utc,
                "eligibility_status": status,
                "depth_rebuild_status": depth_rebuild_status,
                "unverified_reasons": reasons,
                "unrebuildable_depth_reasons": buf.unrebuildable_depth_reasons,
                "unrebuildable_depth_intervals": buf.unrebuildable_depth_intervals,
                "excluded_unrebuildable_depth_rows": buf.excluded_unrebuildable_depth_rows,
                "depth_unavailable_intervals": buf.depth_unavailable_intervals,
                "gap_execution_policy": "local_clear_only_retain_gap_fills_as_unverified",
                "gap_count": len(gaps),
                "gap_intervals": gaps,
                "local_loss_count": sum(x["dropped"] for x in losses),
                "switch_count": day_switches.get((exchange, date_utc), 0),
                "clock_uncertified_samples": len(clock_issues),
                "spec_change_isolations": len(spec_issues),
                "hbt_event_count": len(buf.hbt_rows) if not buf.unrebuildable_depth_reasons else 0,
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

    unrebuildable_depth_markets = [
        {
            "exchange": r["exchange"],
            "symbol": r["symbol"],
            "date_utc": r["date_utc"],
            "reasons": r["unrebuildable_depth_reasons"],
            "intervals": r["unrebuildable_depth_intervals"],
            "excluded_rows": r["excluded_unrebuildable_depth_rows"],
        }
        for r in calendar_rows
        if r["depth_rebuild_status"] == "unrebuildable_excluded"
    ]
    summary = {
        "status": "ok",
        "phase": "qa_convert",
        "hftbacktest_version": "2.4.4",
        "exchanges": exchange_stats,
        "calendar_path": str(cal_json),
        "calendar_csv_path": str(cal_csv),
        "coverage": coverage_report,
        "local_loss_partitions": local_loss_partitions,
        "unrebuildable_depth_markets": unrebuildable_depth_markets,
        "eligible_asset_days": sum(1 for r in calendar_rows if r["eligibility_status"] == "eligible"),
        "execution_unverified_asset_days": sum(
            1 for r in calendar_rows if r["eligibility_status"] == "execution_unverified"
        ),
        "restored_pruned_partitions": restored_pruned_partitions,
        "artifacts": artifacts,
    }
    (out_dir / "qa_report.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return summary


def _load_replay_depth_intervals(
    follower_path: Path, follower_sha256: str,
) -> list[dict[str, Any]] | None:
    """Read interval metadata from the calendar for this exact conversion artifact."""
    calendar = follower_path.with_name("eligibility_calendar.json")
    try:
        rows = json.loads(calendar.read_text(encoding="utf-8"))
    except (FileNotFoundError, ValueError):
        return None
    if not isinstance(rows, list):
        return None
    for row in rows:
        if not isinstance(row, dict):
            continue
        files, hashes = row.get("files"), row.get("sha256")
        if not isinstance(files, dict) or not isinstance(hashes, dict):
            continue
        recorded_path = files.get("follower_l2_hbt")
        if (not isinstance(recorded_path, str) or Path(recorded_path).name != follower_path.name
                or hashes.get("follower_l2_hbt") != follower_sha256):
            continue
        intervals = row.get("depth_unavailable_intervals")
        if not isinstance(intervals, list):
            return None
        for interval in intervals:
            if not isinstance(interval, dict) or "end_wall_ns" not in interval:
                return None
            start, end = interval.get("start_wall_ns"), interval["end_wall_ns"]
            if type(start) is not int or start <= 0:
                return None
            if end is not None and (type(end) is not int or end < start):
                return None
        return intervals
    return None


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
    depth_unavailable_intervals: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Run Huhabubu/mm's official `scripts/core_glft_engine.py::run_core_glft` on converted .npz files.

    Returns exact deterministic fingerprints (input SHA-256, fills, ledger, decisions) for P1
    repeatability and anti-lookahead verification. Unless explicitly supplied, depth intervals
    are read from the eligibility calendar beside the follower L2 artifact. Missing or unmatched
    metadata leaves execution unverified while preserving raw engine accounting.
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

    fill_records = [
        {
            "exch_ts_ns": int(fill_ts_ns[i]),
            "observed_ts_ns": int(fill_observed_ns[i]),
            "side": "buy" if int(fill_sides[i]) == 1 else "sell",
            "price": float(fill_exec_price[i]),
            "qty": float(fill_exec_qty[i]),
        }
        for i in range(len(fill_ts_ns))
    ]
    follower_sha256 = _sha256_file(follower_l2_npz)
    if depth_unavailable_intervals is None:
        depth_unavailable_intervals = _load_replay_depth_intervals(follower_l2_npz, follower_sha256)
    gap_execution_audit = audit_fills_against_depth_unavailable_intervals(
        fill_records,
        depth_unavailable_intervals,
    )

    return {
        "status": "ok",
        "execution_status": gap_execution_audit["status"],
        "follower_l2_sha256": follower_sha256,
        "binance_bbo_sha256": _sha256_file(binance_bbo_npz),
        "ledger_sha256": ledger_sha256,
        "t0_ns": int(t0),
        "t_end_ns": int(now_end),
        "diagnostics": [float(x) for x in diagnostics],
        "probe_rows": probe_rows.tolist() if probe_ns is not None else [],
        "gap_execution_audit": gap_execution_audit,
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
