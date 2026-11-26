#!/usr/bin/env python3
"""Streaming Tokyo L2 -> hftbacktest 2.4.4 converter for ONE Gate symbol and its Binance leader.

Independent of the old converter (`tokyo_l2_bt_convert.py`, kept unchanged as the reference).

Reused unchanged from the frozen collector code (collector/python @ COLLECTOR_SHA):
- archive_sync.verify_local_partition: zstd integrity, timestamp scan, bytes + SHA-256 vs manifest;
- data_coverage.normalize_manifest_window / record_monotonic_producer_key / CompleteDayCoverage;
- qa_convert.AssetDayBuffers + _validate_and_apply_follower_depth: Gate obu snapshot / U=prev_u+1
  chain / same (boot, conn, epoch) anchor rules, local-only clears, unrebuildable intervals;
- qa_convert._order_and_validate_hbt_events: HBT correct_event_order + validate_event_order;
- qa_convert.load_chrony_uncertified_intervals / load_spec_change_isolations.

Deliberate differences from qa_convert.run_qa_convert:
- streaming: every TSV line is read once; a payload is JSON-decoded at most once; other symbols
  and Binance non-bookTicker streams are dropped by substring before decoding; rows go to NumPy
  chunks (no whole-day Python row lists);
- timestamps stay int64 end to end (qa_convert sends BBO/L1 timestamps through float64, which
  rounds 1.8e18 ns to multiples of 256 ns);
- book, sequence and connection state is one object carried across partitions and across runs
  (pickled checkpoint); a partition boundary never re-anchors or clears the book;
- Gate trade duplicates are classified with the PR #30 rule instead of being dropped
  unconditionally: an identical copy on a new connection within 5 s of that connection's first
  trade frame and of a connection event is handover overlap (first copy kept); same-connection
  repeats and conflicting copies block the conversion;
- depth deltas before the first real snapshot of the processed range are an explicit
  initialization exclusion (not emitted, reported); no snapshot is ever synthesized;
- final HBT arrays are ordered once over the whole range, so partitioning cannot change order.
"""
from __future__ import annotations

import argparse
from array import array
import bisect
from collections import Counter, deque
from datetime import datetime, timezone
import hashlib
import io
import json
import math
from pathlib import Path
import pickle
import shutil
import sys
import time
from typing import Any

import numpy as np
import zstandard as zstd

from archive_sync import verify_local_partition
from data_coverage import CompleteDayCoverage, normalize_manifest_window, record_monotonic_producer_key
from hftbacktest import (BUY_EVENT, DEPTH_CLEAR_EVENT, DEPTH_EVENT, DEPTH_SNAPSHOT_EVENT, EXCH_EVENT,
                         LOCAL_EVENT, SELL_EVENT, TRADE_EVENT, event_dtype)
from qa_convert import (TRADE_FLAG_IS_INTERNAL, AssetDayBuffers, _order_and_validate_hbt_events,
                        _validate_and_apply_follower_depth, load_chrony_uncertified_intervals,
                        load_spec_change_isolations)
from tokyo_l2_bt_convert import base_asset, narrowed_binance_config, narrowed_config, native_symbols

LEADER = "binance_futures"
FOLLOWER = "gate_futures"
NS = 1_000_000_000
DAY_NS = 86_400 * NS
TRADE_OVERLAP_NS = 5 * NS
CONNECTION_EVENTS = frozenset({"SWITCH", "STANDBY_DISCONNECT", "GAP_START", "GAP_END"})
BINANCE_DEPTH_META = frozenset({"REST_SNAPSHOT", "REST_SNAPSHOT_ANCHOR", "DEPTH_BRIDGED_DELTA",
                                "DEPTH_UNAVAILABLE", "SNAPSHOT_BRIDGE_RETRY"})
TRADE_COMPARE_FIELDS = ("contract", "id", "price", "size", "create_time_ms", "create_time", "is_internal")
CHUNK_ROWS = 1 << 16
STATE_VERSION = 2
STATE_FILE = "state.pkl"
PROGRESS_FILE = "progress.json"
BBO_DTYPE = np.dtype([("local_ts_ns", "<i8"), ("exch_ts_ns", "<i8"), ("bid_px", "<f8"), ("bid_qty", "<f8"),
                      ("ask_px", "<f8"), ("ask_qty", "<f8"), ("mid_px", "<f8")])
EDGE_DTYPE = np.dtype([("local_ts_ns", "<i8"), ("best_bid", "<f8"), ("edge_bid_50", "<f8"), ("best_ask", "<f8"),
                       ("edge_ask_50", "<f8"), ("visible_bid_levels", "<i4"), ("visible_ask_levels", "<i4")])
EVENT_TYPES = {DEPTH_EVENT: "depth", TRADE_EVENT: "trade", DEPTH_CLEAR_EVENT: "clear",
               DEPTH_SNAPSHOT_EVENT: "snapshot"}


def peak_rss_mb() -> float | None:
    try:
        import resource
        return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)
    except Exception:
        return None


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with Path(path).open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def dir_bytes(path: Path) -> int:
    p = Path(path)
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file()) if p.exists() else 0


def event_histogram(data: np.ndarray) -> dict[str, int]:
    """Count HBT rows by event type, side and direction flags (official event_dtype `ev`)."""
    ev = data["ev"].astype(np.uint64)
    kind = ev & np.uint64(0xFF)
    side = np.where((ev & np.uint64(BUY_EVENT)) != 0, "buy", np.where((ev & np.uint64(SELL_EVENT)) != 0, "sell", "-"))
    exch = (ev & np.uint64(EXCH_EVENT)) != 0
    local = (ev & np.uint64(LOCAL_EVENT)) != 0
    direction = np.where(exch & local, "both", np.where(exch, "exch", np.where(local, "local", "none")))
    out: Counter = Counter()
    for k, s, d in zip(kind.tolist(), side.tolist(), direction.tolist()):
        out[f"{EVENT_TYPES.get(int(k), str(int(k)))}|{s}|{d}"] += 1
    return dict(sorted(out.items()))


class RowSink:
    """Append-only row buffer flushed to NumPy chunks; len() counts every row ever appended."""

    def __init__(self, dtype: np.dtype, chunk_rows: int = CHUNK_ROWS):
        self.dtype = np.dtype(dtype)
        self.chunk_rows = chunk_rows
        self._pending: list[tuple] = []
        self._chunks: list[np.ndarray] = []
        self._flushed = 0

    def append(self, row: tuple) -> None:
        self._pending.append(row)
        if len(self._pending) >= self.chunk_rows:
            self._flush()

    def _flush(self) -> None:
        if self._pending:
            arr = np.array(self._pending, dtype=self.dtype)
            self._chunks.append(arr)
            self._flushed += len(arr)
            self._pending = []

    def __len__(self) -> int:
        return self._flushed + len(self._pending)

    def drain(self) -> np.ndarray:
        """Rows appended since the previous drain; the global count keeps running."""
        self._flush()
        out = np.concatenate(self._chunks) if self._chunks else np.empty(0, dtype=self.dtype)
        self._chunks = []
        return out

    def is_drained(self) -> bool:
        return not self._pending and not self._chunks


class ExchangeState:
    """Write-order and receive-clock checks of qa_convert.run_qa_convert, carried across partitions."""

    def __init__(self, exchange: str):
        self.exchange = exchange
        self.prev_boot: str | None = None
        self.prev_file_last_rx_seq: int | None = None
        self.prev_file_had_late_frames = False
        self.boot_rx_blocks: list[tuple] = []
        self.producer_prev_mono: dict[tuple[str, str, int], int] = {}
        self.last_sort_key: tuple | None = None
        self.counts: Counter = Counter()


class TradeDedup:
    """First-receive Gate trade dedup with PR #30 handover classification (deferred by 5 s)."""

    def __init__(
        self,
        compare_fields: tuple[str, ...] = TRADE_COMPARE_FIELDS,
        strict_handover_only: bool = True,
    ) -> None:
        self.compare_fields = tuple(compare_fields)
        self.strict_handover_only = bool(strict_handover_only)
        self.seen: dict[str, tuple[int, int]] = {}      # key -> (fields digest, connection index)
        self.copy_conns: dict[str, list[int]] = {}      # key -> connection index of every copy
        self.conns: list[tuple[str, str, int]] = []
        self.conn_index: dict[tuple[str, str, int], int] = {}
        self.conn_first_trade: dict[tuple[str, str, int], int] = {}
        self.conn_events: list[int] = []
        self.pending: deque = deque()
        self.counts: Counter = Counter()
        self.samples: list[dict[str, Any]] = []

    def conn(self, key: tuple[str, str, int]) -> int:
        idx = self.conn_index.get(key)
        if idx is None:
            idx = self.conn_index[key] = len(self.conns)
            self.conns.append(key)
        return idx

    def add_event(self, wall: int) -> None:
        bisect.insort(self.conn_events, wall)

    def digest(self, trade: dict[str, Any]) -> int:
        fields = getattr(self, "compare_fields", TRADE_COMPARE_FIELDS)
        text = json.dumps([trade.get(k) for k in fields], separators=(",", ":"), default=str)
        return int.from_bytes(hashlib.blake2b(text.encode(), digest_size=8).digest(), "little")

    def observe(self, key: str, trade: dict[str, Any], conn_key: tuple[str, str, int], wall: int) -> bool:
        """True for the first copy (emit it). Later copies are queued for classification."""
        cidx = self.conn(conn_key)
        dig = self.digest(trade)
        first = self.seen.get(key)
        self.counts["trade_copies"] += 1
        if first is None:
            self.seen[key] = (dig, cidx)
            self.counts["trades_emitted"] += 1
            return True
        conns = self.copy_conns.setdefault(key, [first[1]])
        cross = cidx not in conns
        conns.append(cidx)
        self.pending.append((key, cidx, wall, dig == first[0], cross))
        return False

    def resolve(self, now_wall: int | None) -> None:
        while self.pending and (now_wall is None or now_wall > self.pending[0][2] + TRADE_OVERLAP_NS):
            key, cidx, wall, identical, cross = self.pending.popleft()
            conn_key = self.conns[cidx]
            since_start = wall - self.conn_first_trade.get(conn_key, wall)
            i = bisect.bisect_left(self.conn_events, wall - TRADE_OVERLAP_NS)
            near_event = i < len(self.conn_events) and self.conn_events[i] <= wall + TRADE_OVERLAP_NS
            if identical and cross and since_start <= TRADE_OVERLAP_NS and near_event:
                cls = "connection_overlap"
            elif identical:
                cls = "duplicate_trade_id"
            else:
                cls = "conflicting_duplicate_trade"
            self.counts[cls] += 1
            if cls != "connection_overlap" and len(self.samples) < 50:
                self.samples.append({"key": key, "class": cls, "wall_ns": wall, "conn": list(conn_key),
                                     "cross_connection": cross, "since_conn_first_trade_ns": since_start,
                                     "near_connection_event": near_event})

    def issues(self) -> int:
        if getattr(self, "strict_handover_only", True):
            return self.counts["duplicate_trade_id"] + self.counts["conflicting_duplicate_trade"]
        return self.counts["conflicting_duplicate_trade"]


def _rx_dups_against(blocks: list[tuple], uniq: np.ndarray) -> int:
    lo, hi = int(uniq[0]), int(uniq[-1])
    n = 0
    for block in blocks:
        if block[0] == "range":
            blo, bhi = block[1], block[2]
            if bhi < lo or blo > hi:
                continue
            n += int(np.searchsorted(uniq, bhi, "right") - np.searchsorted(uniq, blo, "left"))
        else:
            arr = block[1]
            if int(arr[-1]) < lo or int(arr[0]) > hi:
                continue
            n += int(len(np.intersect1d(arr, uniq, assume_unique=True)))
    return n


def _decode(kind: str, raw: str) -> Any:
    if kind == "DATA_ESCAPED":
        inner = json.loads(raw)
        return json.loads(inner) if isinstance(inner, str) else inner
    return json.loads(raw)


def frozen_sort_key(mf: dict[str, Any], data_path: Path) -> tuple:
    """Partition order of qa_convert.run_qa_convert (per exchange)."""
    norm = normalize_manifest_window(mf, fallback_file_name=Path(data_path).name)
    return (int(norm["planned_start_wall_ns"]), mf.get("start_wall_ns") or 0, str(mf.get("boot_id") or ""),
            mf.get("first_rx_seq") or 0, Path(data_path).name)


def manifest_data_path(manifest_path: Path) -> Path:
    """Same data-file resolution as qa_convert._resolve_manifest_inputs."""
    try:
        fn = json.loads(Path(manifest_path).read_text(encoding="utf-8")).get("file_name")
        if isinstance(fn, str) and fn and Path(fn).name == fn and fn.endswith(".tsv.zst"):
            return Path(manifest_path).with_name(fn)
    except Exception:
        pass
    return Path(manifest_path).with_name(Path(manifest_path).name.replace(".manifest.json", ".tsv.zst"))


def discover_partitions(fetched: Path, making_exchange: str = FOLLOWER) -> list[tuple[str, Path, Path, tuple]]:
    """(exchange, data, manifest, sort_key) for Binance + making_exchange under <fetched>/raw, in frozen order."""
    raw_root = Path(fetched) / "raw" if (Path(fetched) / "raw").exists() else Path(fetched)
    out = []
    for ex in (LEADER, making_exchange):
        for mpath in sorted((raw_root / ex).rglob("*.manifest.json")):
            mf = json.loads(mpath.read_text(encoding="utf-8"))
            dpath = manifest_data_path(mpath)
            out.append((ex, dpath, mpath, frozen_sort_key(mf, dpath)))
    out.sort(key=lambda x: (x[3][0], x[0] != LEADER, x[3]))
    return out


class StreamConverter:
    def __init__(
        self,
        gate_symbol: str,
        config: dict[str, Any] | None = None,
        coverage_date: str | None = None,
        making_exchange: str = FOLLOWER,
        include_leader: bool = True,
    ):
        self.making_exchange = str(making_exchange)
        self.include_leader = bool(include_leader)
        if self.making_exchange == FOLLOWER:
            base, binance_sym, follower_sym = native_symbols(gate_symbol)
            cmp_fields = TRADE_COMPARE_FIELDS
        else:
            from tokyo_l2_exchange_adapters import to_base_symbol, to_native_symbol, trade_compare_fields
            base = to_base_symbol(gate_symbol)
            binance_sym = to_native_symbol(LEADER, base)
            follower_sym = to_native_symbol(self.making_exchange, base)
            cmp_fields = trade_compare_fields(self.making_exchange)
        self.version = STATE_VERSION
        self.gate_symbol = follower_sym if self.making_exchange != FOLLOWER else gate_symbol
        self.follower_symbol = follower_sym
        self.base, self.binance_sym = base, binance_sym
        self.coverage_date = coverage_date
        if coverage_date is not None and config is None:
            raise ValueError("complete-day coverage needs the collector config")
        self.coverage = (
            CompleteDayCoverage(
                narrowed_config(config, self.gate_symbol, making_exchange=self.making_exchange, include_leader=self.include_leader),
                coverage_date,
            )
            if coverage_date else None
        )
        self.ex = (
            {LEADER: ExchangeState(LEADER), self.making_exchange: ExchangeState(self.making_exchange)}
            if self.include_leader else {self.making_exchange: ExchangeState(self.making_exchange)}
        )
        self.bbo = RowSink(BBO_DTYPE)
        self.bbo_exch_ahead_of_local = 0
        buf = AssetDayBuffers(self.making_exchange, base, "stream")
        buf.hbt_rows = RowSink(event_dtype)
        buf.edge_samples = RowSink(EDGE_DTYPE)
        buf.bbo_rows = RowSink(BBO_DTYPE)
        self.gate = buf
        self.trades = TradeDedup(
            compare_fields=cmp_fields,
            strict_handover_only=(self.making_exchange == FOLLOWER),
        )
        self.init_exclusion = {"frames": 0, "first_local_ts": None, "last_local_ts": None}
        self.initial_gate_window = None
        self.current_gate_window = None
        self.initial_exemption_closed = False
        self.execution_unavailable_intervals = []
        self.pending_confirmation = None
        self.failed: Counter = Counter()
        self.failed_samples: list[str] = []
        self.day_gaps: dict[str, list[dict[str, Any]]] = {}
        self.day_local_loss: dict[str, list[dict[str, Any]]] = {}
        self.day_switches: Counter = Counter()
        self.local_loss_partitions: list[dict[str, Any]] = []
        self.binance_depth_meta: list[dict[str, Any]] = []
        self.other_symbol_meta: Counter = Counter()
        self.dates_seen: set[tuple[str, str]] = set()
        self.parts: list[dict[str, Any]] = []
        self._day_lo = self._day_hi = None
        self._day = ""

    # ----------------------------------------------------------------- helpers
    def _fail(self, code: str, detail: str) -> None:
        self.failed[code] += 1
        if len(self.failed_samples) < 50:
            self.failed_samples.append(f"{code}:{detail}")

    def _date(self, wall: int) -> str:
        if self._day_lo is None or not (self._day_lo <= wall < self._day_hi):
            day = datetime.fromtimestamp(wall // NS, tz=timezone.utc).strftime("%Y-%m-%d")
            start = int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()) * NS
            self._day_lo, self._day_hi, self._day = start, start + DAY_NS, day
        return self._day

    def _book_state(self) -> dict[str, Any]:
        buf = self.gate
        anchor = buf.follower_depth_anchor
        return {"seen_snapshot": buf.seen_snapshot, "bid_levels": len(buf.bids), "ask_levels": len(buf.asks),
                "best_bid": max(buf.bids) if buf.bids else None, "best_ask": min(buf.asks) if buf.asks else None,
                "anchor": None if anchor is None else {k: anchor.get(k) for k in ("boot_id", "conn_id", "epoch", "seq")},
                "hbt_rows": len(buf.hbt_rows), "local_book_clears": len(buf.local_book_clears),
                "unrebuildable_reasons": list(buf.unrebuildable_depth_reasons)}

    def _block_execution(self, wall, reason):
        if self.pending_confirmation is None:
            iv = {"start_wall_ns": int(wall), "end_wall_ns": None, "reason": reason,
                  "snapshot_wall_ns": None, "confirmation_lag_ns": None}
            self.execution_unavailable_intervals.append(iv)
            self.pending_confirmation = iv
        return self.pending_confirmation

    # ------------------------------------------------------------------- META
    def _gate_meta(self, obj: dict[str, Any], ev: str, is_all: bool, wall: int, boot: str, conn: str, epoch: int) -> None:
        """qa_convert._ingest_follower_meta for the single target buffer (payload already decoded)."""
        buf = self.gate
        if ev in ("SWITCH", "GAP_START", "LOCAL_LOSS", "DEPTH_UNAVAILABLE"):
            self.initial_exemption_closed = True
            iv = self._block_execution(int(obj.get("unavailable_since_wall_ns") or wall), ev)
            iv["snapshot_wall_ns"] = None
        if ev in ("SWITCH", "GAP_START", "LOCAL_LOSS"):
            buf.follower_depth_anchor = None
            buf.emit_book_clear_if_active(wall)
        elif ev == "DEPTH_UNAVAILABLE":
            since_ns = int(obj.get("unavailable_since_wall_ns") or wall)
            buf.follower_depth_anchor = None
            buf.emit_book_clear_if_active(wall)
            ivs = buf.depth_unavailable_intervals
            if not ivs or ivs[-1].get("end_wall_ns") is not None:
                ivs.append({"start_wall_ns": since_ns, "end_wall_ns": None, "reason": str(obj.get("reason") or ev),
                            "planned": bool(obj.get("planned")), "boot_id": str(obj.get("boot_id") or boot or ""),
                            "conn_id": str(obj.get("conn_id") or conn or ""),
                            "epoch": int(obj["epoch"]) if obj.get("epoch") is not None else int(epoch)})
            else:
                iv = ivs[-1]
                iv["planned"] = bool(iv.get("planned")) and bool(obj.get("planned"))
                iv["boot_id"] = str(obj.get("boot_id") or boot or iv.get("boot_id") or "")
                iv["conn_id"] = str(obj.get("conn_id") or conn or iv.get("conn_id") or "")
                iv["epoch"] = int(obj["epoch"]) if obj.get("epoch") is not None else int(epoch)
        elif ev == "DEPTH_RECOVERED" and not is_all:
            buf.depth_recovered_count += 1

    # ------------------------------------------------------------- Gate DATA
    def _gate_depth(self, obj, result, wall, boot, conn, epoch, c) -> None:
        buf = self.gate
        exch_ms = int(result.get("t") or obj.get("time_ms") or (wall // 1_000_000))
        is_full = bool(result.get("full", False))
        if not buf.seen_snapshot and not is_full:
            if self.current_gate_window != self.initial_gate_window or self.initial_exemption_closed:
                self._fail("initialization_exemption_outside_sample_start", str(wall))
                self.initial_exemption_closed = True
                return
            ie = self.init_exclusion
            ie["frames"] += 1
            ie["first_local_ts"] = wall if ie["first_local_ts"] is None else ie["first_local_ts"]
            ie["last_local_ts"] = wall
            c["obu_pre_first_snapshot"] += 1
            return
        c["obu_snapshot" if is_full else "obu_delta"] += 1
        before_errors = len(buf.unrebuildable_depth_intervals)
        _validate_and_apply_follower_depth(
            buf=buf, exchange=FOLLOWER, rec={"boot_id": boot, "conn_id": conn, "epoch": epoch},
            is_snap=is_full, seq=int(result["u"]) if result.get("u") is not None else None,
            parent=int(result["U"]) if result.get("U") is not None else None,
            exch_ts=exch_ms * 1_000_000, local_ts=wall, raw_bids=result.get("b"), raw_asks=result.get("a"))
        anchor = buf.follower_depth_anchor
        valid = anchor is not None and len(buf.unrebuildable_depth_intervals) == before_errors
        if not valid:
            self.initial_exemption_closed = True
            self._block_execution(wall, "depth_validation_failed")["snapshot_wall_ns"] = None
        elif is_full:
            self.initial_exemption_closed = True
            iv = self._block_execution(wall, "snapshot_awaiting_contiguous_delta")
            iv.update(snapshot_wall_ns=wall, boot_id=boot, conn_id=conn, epoch=epoch,
                      snapshot_seq=anchor["seq"])
        elif self.pending_confirmation is not None:
            iv = self.pending_confirmation
            if iv["snapshot_wall_ns"] is not None and (boot, conn, epoch) == (
                    iv["boot_id"], iv["conn_id"], iv["epoch"]):
                iv.update(end_wall_ns=wall, confirmation_lag_ns=wall - iv["snapshot_wall_ns"],
                          confirming_U=int(result["U"]), confirming_u=int(result["u"]))
                self.pending_confirmation = None

    def _gate_trades(self, obj, trades, wall, boot, conn, epoch, c) -> None:
        buf = self.gate
        for tr in trades:
            key = f"{tr.get('contract') or ''}:{tr['id']}"
            if not self.trades.observe(key, tr, (boot, conn, epoch), wall):
                c["trade_duplicate_copies"] += 1
                continue
            exch_ts = int(tr.get("create_time_ms") or obj.get("time_ms") or (wall // 1_000_000)) * 1_000_000
            if exch_ts > wall:
                buf.exch_ahead_of_local_count += 1
            raw_size = float(tr["size"])
            ival = 0
            if bool(tr.get("is_internal", False)):
                ival |= TRADE_FLAG_IS_INTERNAL
                buf.special_flag_counts["is_internal"] += 1
            buf.hbt_rows.append((TRADE_EVENT | (BUY_EVENT if raw_size >= 0.0 else SELL_EVENT), exch_ts, wall,
                                 float(tr["price"]), abs(raw_size), 0, ival, 0.0))
            buf.last_exch_ts = max(buf.last_exch_ts, exch_ts)
            buf.last_local_ts = max(buf.last_local_ts, wall)
            c["trades_emitted"] += 1
        self.trades.resolve(wall)

    def _gate_l1(self, obj, result, wall) -> None:
        exch_ts = int(result.get("t") or obj.get("time_ms") or (wall // 1_000_000)) * 1_000_000
        bid_px, ask_px = float(result["b"]), float(result["a"])
        self.gate.bbo_rows.append((wall, exch_ts, bid_px, float(result["B"]), ask_px, float(result["A"]),
                                   0.5 * (bid_px + ask_px)))

    def _apply_generic_depth(self, item: dict[str, Any], wall: int, boot: str, conn: str, epoch: int, c: Counter) -> None:
        buf = self.gate
        is_full = bool(item["is_snapshot"])
        if not buf.seen_snapshot and not is_full:
            if self.current_gate_window != self.initial_gate_window or self.initial_exemption_closed:
                self._fail("initialization_exemption_outside_sample_start", str(wall))
                self.initial_exemption_closed = True
                return
            ie = self.init_exclusion
            ie["frames"] += 1
            ie["first_local_ts"] = wall if ie["first_local_ts"] is None else ie["first_local_ts"]
            ie["last_local_ts"] = wall
            c["obu_pre_first_snapshot"] += 1
            return
        c["obu_snapshot" if is_full else "obu_delta"] += 1
        before_errors = len(buf.unrebuildable_depth_intervals)
        _validate_and_apply_follower_depth(
            buf=buf, exchange=self.making_exchange, rec={"boot_id": boot, "conn_id": conn, "epoch": epoch},
            is_snap=is_full, seq=item["seq"], parent=item["parent"],
            exch_ts=int(item["exch_ts_ns"]), local_ts=wall,
            raw_bids=item["raw_bids"], raw_asks=item["raw_asks"],
        )
        anchor = buf.follower_depth_anchor
        valid = anchor is not None and len(buf.unrebuildable_depth_intervals) == before_errors
        if not valid:
            self.initial_exemption_closed = True
            self._block_execution(wall, "depth_validation_failed")["snapshot_wall_ns"] = None
        elif is_full:
            self.initial_exemption_closed = True
            iv = self._block_execution(wall, "snapshot_awaiting_contiguous_delta")
            iv.update(snapshot_wall_ns=wall, boot_id=boot, conn_id=conn, epoch=epoch,
                      snapshot_seq=anchor["seq"])
        elif self.pending_confirmation is not None:
            iv = self.pending_confirmation
            if iv["snapshot_wall_ns"] is not None and (boot, conn, epoch) == (
                    iv["boot_id"], iv["conn_id"], iv["epoch"]):
                iv.update(end_wall_ns=wall, confirmation_lag_ns=wall - iv["snapshot_wall_ns"],
                          confirming_U=item["parent"], confirming_u=item["seq"])
                self.pending_confirmation = None

    def _apply_generic_trades(self, parsed_trades: list[dict[str, Any]], wall: int, boot: str, conn: str, epoch: int, c: Counter) -> None:
        buf = self.gate
        for tr in parsed_trades:
            if not self.trades.observe(tr["dedup_key"], tr["raw_trade"], (boot, conn, epoch), wall):
                c["trade_duplicate_copies"] += 1
                continue
            exch_ts = int(tr["exch_ts_ns"])
            if exch_ts > wall:
                buf.exch_ahead_of_local_count += 1
            for flag_name in tr["special_flags"]:
                buf.special_flag_counts[flag_name] += 1
            side_flag = BUY_EVENT if tr["is_buy"] else SELL_EVENT
            buf.hbt_rows.append((TRADE_EVENT | side_flag, exch_ts, wall,
                                 float(tr["price"]), float(tr["qty"]), 0, int(tr["flags"]), 0.0))
            buf.last_exch_ts = max(buf.last_exch_ts, exch_ts)
            buf.last_local_ts = max(buf.last_local_ts, wall)
            c["trades_emitted"] += 1
        self.trades.resolve(wall)

    # ------------------------------------------------------------- partition
    def process_partition(self, data_path: Path, manifest_path: Path, parts_dir: Path,
                          progress_every: int = 2_000_000) -> dict[str, Any]:
        data_path, manifest_path, parts_dir = Path(data_path), Path(manifest_path), Path(parts_dir)
        t0 = time.time()
        checked = verify_local_partition(data_path, manifest_path)
        t_verify = time.time() - t0
        mf = checked["manifest"]
        exchange = str(mf["exchange"])
        if exchange not in self.ex:
            raise ValueError(f"unexpected exchange {exchange} in {manifest_path.name}")
        norm = normalize_manifest_window(mf, fallback_file_name=data_path.name)
        st = self.ex[exchange]
        key = frozen_sort_key(mf, data_path)
        if st.last_sort_key is not None and key <= st.last_sort_key:
            raise ValueError(f"{exchange}: partition {data_path.name} is not after the previous one")
        in_cov = self.coverage is not None and norm["date_utc"] == self.coverage.date
        if in_cov:
            self.coverage.observe_partition(mf)
        boot = norm["boot_id"]
        making_ex = getattr(self, "making_exchange", FOLLOWER)
        if exchange == making_ex:
            self.current_gate_window = norm["window_id"]
            if self.initial_gate_window is None:
                self.initial_gate_window = self.current_gate_window
                self._block_execution(int(norm["planned_start_wall_ns"]), "sample_initialization")
        if boot != st.prev_boot:
            st.prev_boot, st.prev_file_last_rx_seq, st.prev_file_had_late_frames = boot, None, False
            st.boot_rx_blocks, st.producer_prev_mono = [], {}
        before = {"hbt_rows": len(self.gate.hbt_rows), "state": self._book_state()}
        c: Counter = Counter()
        rxs = array("q")
        recorded_local_loss = dropped_frames = 0
        is_leader = exchange == LEADER
        is_gate = making_ex == FOLLOWER
        needle = self.binance_sym if is_leader else getattr(self, "follower_symbol", self.gate_symbol)
        base = self.base
        prev_mono = st.producer_prev_mono
        coverage = self.coverage if in_cov else None
        if not is_leader and not is_gate:
            from tokyo_l2_exchange_adapters import is_follower_trade_raw, parse_follower_payload
        dctx = zstd.ZstdDecompressor()
        with data_path.open("rb") as fh, dctx.stream_reader(fh) as reader:
            text = io.TextIOWrapper(reader, encoding="utf-8", errors="strict")
            for line_no, line in enumerate(text, start=1):
                trimmed = line.rstrip("\n")
                if not trimmed:
                    continue
                parts = trimmed.split("\t", 6)
                if len(parts) != 7:
                    raise ValueError(f"{data_path.name}:{line_no}: expected 7 TSV columns, got {len(parts)}")
                wall, mono, conn, epoch, rx = int(parts[0]), int(parts[1]), parts[2], int(parts[3]), int(parts[4])
                kind, raw = parts[5], parts[6]
                rxs.append(rx)
                c["lines"] += 1
                if progress_every and c["lines"] % progress_every == 0:
                    print(f"PROGRESS {exchange} {norm['window_id']} lines={c['lines']} "
                          f"elapsed={time.time() - t0:.1f}s peak_rss={peak_rss_mb()}MB", flush=True)
                obj = None
                if kind == "META":
                    obj = json.loads(raw)
                    c["decoded"] += 1
                    pkey = record_monotonic_producer_key(exchange, kind, conn, epoch, obj)
                elif is_leader:
                    pkey = ("binance_market" if "aggTrade" in raw else "binance_public", conn, epoch)
                else:
                    pkey = (f"{exchange}_ws", conn, epoch)
                pm = prev_mono.get(pkey)
                if pm is not None and mono < pm:
                    self._fail("recv_mono_ns_regressed", f"{exchange}:{data_path.name}:{pkey}:prev={pm},curr={mono}")
                prev_mono[pkey] = mono

                if kind == "META":
                    c["meta"] += 1
                    if not isinstance(obj, dict):
                        self._fail("meta_not_object", f"{exchange}:{data_path.name}:{line_no}")
                        continue
                    ev = str(obj.get("event") or "")
                    sym = str(obj.get("symbol") or "ALL").strip()
                    is_all = not sym or sym.upper() == "ALL"
                    if not is_leader and ev in CONNECTION_EVENTS:
                        self.trades.add_event(wall)
                    if is_leader and ev in BINANCE_DEPTH_META:
                        c["binance_depth_meta"] += 1
                        self.binance_depth_meta.append({"event": ev, "symbol": sym, "wall_ns": wall,
                                                        "window_id": norm["window_id"],
                                                        "reason": obj.get("reason")})
                        continue
                    if not (is_all or base_asset(sym) == base):
                        self.other_symbol_meta[f"{exchange}:{ev}"] += 1
                        continue
                    date = self._date(wall)
                    gk = f"{exchange}|{date}"
                    if ev == "GAP_START":
                        self.day_gaps.setdefault(gk, []).append(
                            {"start_wall_ns": wall, "end_wall_ns": None, "reason": obj.get("reason", "")})
                    elif ev == "GAP_END":
                        gaps = self.day_gaps.setdefault(gk, [])
                        if gaps and gaps[-1]["end_wall_ns"] is None:
                            gaps[-1]["end_wall_ns"] = wall
                        else:
                            gaps.append({"start_wall_ns": None, "end_wall_ns": wall, "reason": "gap_end_cross_day"})
                    elif ev == "LOCAL_LOSS":
                        dropped = int(obj.get("dropped_frames", obj.get("dropped", 1)))
                        recorded_local_loss += 1
                        dropped_frames += dropped
                        self.day_local_loss.setdefault(gk, []).append(
                            {"wall_ns": wall, "dropped": dropped, "reason": obj.get("reason", "")})
                    elif ev == "LATE_FRAME_REJECTED":
                        dropped = int(obj.get("dropped_frames", 1))
                        wid = str(obj.get("target_window_id") or obj.get("loss_attributed_window_id") or "")
                        recorded_local_loss += 1
                        dropped_frames += dropped
                        self.day_local_loss.setdefault(f"{exchange}|{wid[:10] if len(wid) >= 10 else date}", []).append(
                            {"wall_ns": int(obj.get("orig_recv_wall_ns") or wall), "dropped": dropped,
                             "reason": f"late_frame_rejected_for_{wid or 'prior_window'}"})
                    elif ev == "SWITCH":
                        self.day_switches[gk] += 1
                    if not is_leader:
                        self._gate_meta(obj, ev, is_all, wall, boot, conn, epoch)
                    continue

                c["data"] += 1
                if is_leader:
                    if needle not in raw or "bookTicker" not in raw:
                        c["dropped_prefilter"] += 1
                        continue
                    obj = _decode(kind, raw)
                    c["decoded"] += 1
                    data = obj.get("data") if isinstance(obj.get("data"), dict) else obj
                    is_bbo = data.get("e") == "bookTicker" or str(obj.get("stream", "")).endswith("@bookTicker")
                    if not (is_bbo and base_asset(data.get("s")) == base):
                        c["dropped_after_decode"] += 1
                        continue
                    if coverage is not None:
                        coverage.observe_record(exchange, {"kind": kind, "recv_wall_ns": wall, "payload": obj})
                    exch_ts = int(data.get("T") or data.get("E") or (wall // 1_000_000)) * 1_000_000
                    if exch_ts > wall:
                        self.bbo_exch_ahead_of_local += 1
                    bid_px, ask_px = float(data["b"]), float(data["a"])
                    self.bbo.append((wall, exch_ts, bid_px, float(data["B"]), ask_px, float(data["A"]),
                                     0.5 * (bid_px + ask_px)))
                    c["bbo"] += 1
                    self.dates_seen.add((exchange, self._date(wall)))
                    continue

                if is_gate:
                    if "futures.trades" in raw and "update" in raw:
                        self.trades.conn_first_trade.setdefault((boot, conn, epoch), wall)
                    if needle not in raw:
                        c["dropped_prefilter"] += 1
                        continue
                    obj = _decode(kind, raw)
                    c["decoded"] += 1
                    if not isinstance(obj, dict):
                        c["dropped_after_decode"] += 1
                        continue
                    channel, result = obj.get("channel", ""), obj.get("result")
                    if channel == "futures.trades":
                        trades = result if isinstance(result, list) else [result]
                        mine = [t for t in trades if isinstance(t, dict) and base_asset(t.get("contract")) == base]
                        if not mine:
                            c["dropped_after_decode"] += 1
                            continue
                        if len(mine) != len(trades):
                            c["gate_mixed_symbol_trade_messages"] += 1
                            obj = {**obj, "result": mine}
                        if coverage is not None:
                            coverage.observe_record(exchange, {"kind": kind, "recv_wall_ns": wall, "payload": obj})
                        if obj.get("event", "") == "update":
                            c["trade_frames"] += 1
                            self._gate_trades(obj, mine, wall, boot, conn, epoch, c)
                        continue
                    if not (isinstance(result, dict) and base_asset(result.get("s")) == base):
                        c["dropped_after_decode"] += 1
                        continue
                    if coverage is not None:
                        coverage.observe_record(exchange, {"kind": kind, "recv_wall_ns": wall, "payload": obj})
                    if obj.get("event", "") != "update":
                        c["non_update_frames"] += 1
                        continue
                    self.dates_seen.add((exchange, self._date(wall)))
                    if channel == "futures.obu":
                        self._gate_depth(obj, result, wall, boot, conn, epoch, c)
                    elif channel == "futures.book_ticker":
                        self._gate_l1(obj, result, wall)
                        c["l1"] += 1
                    else:
                        c["dropped_after_decode"] += 1
                    continue

                # Non-Gate making exchange (bybit_linear, bitget_v3)
                if is_follower_trade_raw(making_ex, raw):
                    self.trades.conn_first_trade.setdefault((boot, conn, epoch), wall)
                if needle not in raw:
                    c["dropped_prefilter"] += 1
                    continue
                obj = _decode(kind, raw)
                c["decoded"] += 1
                parsed = parse_follower_payload(making_ex, obj, wall, base) if isinstance(obj, dict) else None
                if parsed is None:
                    c["dropped_after_decode"] += 1
                    continue
                if coverage is not None:
                    coverage.observe_record(exchange, {"kind": kind, "recv_wall_ns": wall, "payload": parsed["coverage_obj"]})
                pkind = parsed["kind"]
                if pkind == "ignored_non_update":
                    c["non_update_frames"] += 1
                    continue
                self.dates_seen.add((exchange, self._date(wall)))
                if pkind == "trades":
                    if parsed["trades"]:
                        c["trade_frames"] += 1
                        self._apply_generic_trades(parsed["trades"], wall, boot, conn, epoch, c)
                elif pkind == "depth":
                    self._apply_generic_depth(parsed, wall, boot, conn, epoch, c)
                elif pkind == "depth_batch":
                    for item in parsed["items"]:
                        self._apply_generic_depth(item, wall, boot, conn, epoch, c)
                elif pkind == "l1":
                    bp, ap = parsed["bid_px"], parsed["ask_px"]
                    self.gate.bbo_rows.append((wall, parsed["exch_ts_ns"], bp, parsed["bid_qty"],
                                               ap, parsed["ask_qty"], 0.5 * (bp + ap)))
                    c["l1"] += 1
                elif pkind == "l1_batch":
                    for item in parsed["items"]:
                        bp, ap = item["bid_px"], item["ask_px"]
                        self.gate.bbo_rows.append((wall, item["exch_ts_ns"], bp, item["bid_qty"],
                                                   ap, item["ask_qty"], 0.5 * (bp + ap)))
                        c["l1"] += 1

        self._finish_rx_seq(st, exchange, data_path, rxs, norm)
        manifest_local_loss = int(mf.get("local_loss_lines") or 0) + int(mf.get("late_frames_rejected") or 0)
        if recorded_local_loss or manifest_local_loss:
            self.local_loss_partitions.append({
                "exchange": exchange, "date_utc": mf["date_utc"], "window_id": norm["window_id"], "boot_id": boot,
                "data_file": data_path.name, "manifest_local_loss_lines": manifest_local_loss,
                "recorded_local_loss_lines": recorded_local_loss, "recorded_dropped_frames": dropped_frames})
        st.last_sort_key = key
        st.counts.update(c)
        summary = self._write_part(exchange, norm, mf, data_path, parts_dir, before, c)
        summary.update(verify_s=round(t_verify, 2), elapsed_s=round(time.time() - t0, 2), peak_rss_mb=peak_rss_mb())
        self.parts.append(summary)
        return summary

    def _finish_rx_seq(self, st: ExchangeState, exchange: str, data_path: Path, rxs: array, norm: dict) -> None:
        arr = np.frombuffer(rxs, dtype=np.int64).copy() if len(rxs) else np.empty(0, dtype=np.int64)
        if len(arr):
            first = int(arr[0])
            if st.prev_file_last_rx_seq is not None and not st.prev_file_had_late_frames \
                    and first <= st.prev_file_last_rx_seq:
                self._fail("rx_seq_non_monotonic", f"{exchange}:{data_path.name}:prev={st.prev_file_last_rx_seq},curr={first}")
            bad = np.flatnonzero(np.diff(arr) <= 0)
            for i in bad[:5].tolist():
                self._fail("rx_seq_non_monotonic", f"{exchange}:{data_path.name}:prev={int(arr[i])},curr={int(arr[i + 1])}")
            if len(bad) > 5:
                self.failed["rx_seq_non_monotonic"] += len(bad) - 5
            uniq = np.unique(arr)
            dups = len(arr) - len(uniq) + _rx_dups_against(st.boot_rx_blocks, uniq)
            if dups:
                self.failed["rx_seq_duplicate_in_boot"] += dups
                self.failed_samples.append(f"rx_seq_duplicate_in_boot:{exchange}:{data_path.name}:count={dups}")
            contiguous = int(uniq[-1]) - int(uniq[0]) + 1 == len(uniq)
            st.boot_rx_blocks.append(("range", int(uniq[0]), int(uniq[-1])) if contiguous else ("array", uniq))
            last = int(arr[-1])
            st.prev_file_last_rx_seq = last if st.prev_file_last_rx_seq is None else max(st.prev_file_last_rx_seq, last)
        st.prev_file_had_late_frames = int(norm["late_frames_accepted"]) > 0

    def _write_part(self, exchange, norm, mf, data_path, parts_dir, before, c) -> dict[str, Any]:
        parts_dir.mkdir(parents=True, exist_ok=True)
        name = f"{exchange}_{norm['window_id'].replace(':', '')}_{len(self.parts):04d}"
        tmp = parts_dir / f"{name}.tmp.npz"
        if exchange == LEADER:
            bbo = self.bbo.drain()
            np.savez_compressed(tmp, bbo=bbo)
            rows = {"bbo": int(len(bbo))}
        else:
            hbt = self.gate.hbt_rows.drain()
            l1 = self.gate.bbo_rows.drain()
            edges = self.gate.edge_samples.drain()
            if before["hbt_rows"] + len(hbt) != len(self.gate.hbt_rows):
                raise AssertionError("HBT row offsets are not contiguous")
            np.savez_compressed(tmp, hbt_raw=hbt, l1=l1, edges=edges,
                                hbt_offset=np.array([before["hbt_rows"]], dtype=np.int64))
            rows = {"hbt_raw": int(len(hbt)), "l1": int(len(l1)), "edges": int(len(edges)),
                    "hbt_offset": before["hbt_rows"],
                    "hbt_types": event_histogram(hbt) if len(hbt) else {}}
        path = parts_dir / f"{name}.npz"
        tmp.replace(path)
        out = {"exchange": exchange, "window_id": norm["window_id"], "date_utc": norm["date_utc"],
               "boot_id": norm["boot_id"], "data_file": data_path.name, "data_sha256": mf["sha256"],
               "data_bytes": int(mf["bytes"]), "part_file": path.name, "part_sha256": sha256_file(path),
               "rows": rows, "counts": dict(c)}
        if exchange == getattr(self, "making_exchange", FOLLOWER):
            out["book_before"] = before["state"]
            out["book_after"] = self._book_state()
            out["trade_dedup"] = dict(self.trades.counts)
            out["initialization_exclusion"] = dict(self.init_exclusion)
        return out

    # ------------------------------------------------------------- checkpoint
    def checkpoint(self, state_dir: Path) -> str:
        state_dir = Path(state_dir)
        state_dir.mkdir(parents=True, exist_ok=True)
        for sink in (self.bbo, self.gate.hbt_rows, self.gate.bbo_rows, self.gate.edge_samples):
            if not sink.is_drained():
                raise AssertionError("checkpoint with undrained rows")
        tmp = state_dir / (STATE_FILE + ".tmp")
        with tmp.open("wb") as fh:
            pickle.dump(self, fh, protocol=pickle.HIGHEST_PROTOCOL)
        digest = sha256_file(tmp)
        tmp.replace(state_dir / STATE_FILE)
        prog = {"version": STATE_VERSION, "making_exchange": getattr(self, "making_exchange", FOLLOWER),
                "gate_symbol": self.gate_symbol, "coverage_date": self.coverage_date,
                "state_sha256": digest, "saved_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "parts": [{k: p[k] for k in ("exchange", "window_id", "data_file", "data_sha256", "part_file",
                                              "part_sha256")} for p in self.parts]}
        ptmp = state_dir / (PROGRESS_FILE + ".tmp")
        ptmp.write_text(json.dumps(prog, indent=2) + "\n", encoding="utf-8")
        ptmp.replace(state_dir / PROGRESS_FILE)
        return digest

    @staticmethod
    def load(state_dir: Path, parts_dir: Path) -> "StreamConverter":
        state_dir = Path(state_dir)
        prog = json.loads((state_dir / PROGRESS_FILE).read_text(encoding="utf-8"))
        if sha256_file(state_dir / STATE_FILE) != prog["state_sha256"]:
            raise ValueError("checkpoint state SHA-256 mismatch")
        with (state_dir / STATE_FILE).open("rb") as fh:
            conv = pickle.load(fh)
        if not isinstance(conv, StreamConverter) or conv.version != STATE_VERSION:
            raise ValueError("incompatible checkpoint")
        for p in conv.parts:
            f = Path(parts_dir) / p["part_file"]
            if not f.is_file() or sha256_file(f) != p["part_sha256"]:
                raise ValueError(f"part output missing or changed: {p['part_file']}")
        return conv

    def done_data(self) -> set[str]:
        return {p["data_sha256"] for p in self.parts}

    # ---------------------------------------------------------------- finalize
    def gate_report(self, meta_dir: Path | None) -> tuple[list[str], dict[str, Any]]:
        """Blocking failures and per-exchange eligibility, mirroring run_qa_convert's gates."""
        self.trades.resolve(None)
        making_ex = getattr(self, "making_exchange", FOLLOWER)
        prefix = "gate" if making_ex == FOLLOWER else making_ex
        buf = self.gate
        for iv in [iv for iv in buf.depth_unavailable_intervals if iv.get("end_wall_ns") is None]:
            buf.record_unrebuildable_depth(reason="depth_unavailable_unrecovered", wall_ns=int(iv.get("start_wall_ns") or 0),
                                           conn_id=str(iv.get("conn_id") or ""), epoch=int(iv.get("epoch") or 0))
            iv["end_wall_ns"] = -1
        failed = [f"{k}:{n}" for k, n in sorted(self.failed.items())]
        coverage_report = self.coverage.report(meta_dir) if self.coverage is not None else {"status": "not_assessed"}
        chrony = load_chrony_uncertified_intervals(meta_dir)
        specs = load_spec_change_isolations(meta_dir)
        if self.coverage is not None and coverage_report["status"] != "complete":
            failed.append("incomplete_utc_day_coverage")
        if self.local_loss_partitions:
            failed.append("local_loss_detected_under_strict_zero_loss_gate")
        if self.coverage is not None and (any(self.day_gaps.values()) or chrony.get(self.coverage.date)):
            failed.append("complete_day_gap_or_uncertified_clock")
        if self.trades.issues():
            failed.append(f"{prefix}_unexplained_duplicate_trades:{self.trades.issues()}")
        if not buf.seen_snapshot:
            failed.append("missing_l2_snapshot_anchor")
        if buf.unrebuildable_depth_reasons:
            failed.append(f"{prefix}_depth_unrebuildable:" + ",".join(buf.unrebuildable_depth_reasons))
        eligibility = {}
        for ex in (LEADER, making_ex):
            for date in sorted({d for e, d in self.dates_seen if e == ex}):
                reasons = []
                if self.day_gaps.get(f"{ex}|{date}"):
                    reasons.append("uncovered_gap")
                if self.day_local_loss.get(f"{ex}|{date}") or any(
                        p["exchange"] == ex and p["date_utc"] == date for p in self.local_loss_partitions):
                    reasons.append("local_loss")
                if chrony.get(date):
                    reasons.append("clock_uncertified_gt_1ms")
                if specs.get((ex, date)):
                    reasons.append("spec_change_isolation")
                if ex == making_ex:
                    reasons += [r for r in buf.unrebuildable_depth_reasons if r not in reasons]
                    if not buf.seen_snapshot:
                        reasons.append("missing_l2_snapshot_anchor")
                eligibility[f"{ex}|{date}"] = {"status": "execution_unverified" if reasons else "eligible",
                                               "reasons": reasons}
        diag = {"coverage": coverage_report, "eligibility": eligibility,
                "chrony_uncertified_samples": {d: len(v) for d, v in chrony.items()},
                "gaps": self.day_gaps, "local_loss": self.day_local_loss,
                "local_loss_partitions": self.local_loss_partitions, "switches": dict(self.day_switches),
                "failed_samples": self.failed_samples,
                "trade_dedup": {**dict(self.trades.counts), "issue_samples": self.trades.samples,
                                "connection_events": len(self.trades.conn_events),
                                "connections_with_trades": len(self.trades.conn_first_trade)},
                "initialization_exclusion": dict(self.init_exclusion),
                "execution_unavailable_intervals": self.execution_unavailable_intervals,
                "depth_unavailable_intervals": buf.depth_unavailable_intervals,
                "unrebuildable_depth_intervals": buf.unrebuildable_depth_intervals,
                "binance_depth_meta": self.binance_depth_meta, "other_symbol_meta": dict(self.other_symbol_meta),
                "exch_ahead_of_local": {"binance_bbo": self.bbo_exch_ahead_of_local,
                                        prefix: buf.exch_ahead_of_local_count},
                "special_flag_counts": dict(buf.special_flag_counts),
                "records": {ex: dict(s.counts) for ex, s in self.ex.items()}}
        return failed, diag

    def finalize(self, parts_dir: Path, out_dir: Path, meta_dir: Path | None, label_date: str,
                 *, diagnostic_allow_known_gaps: bool = False) -> dict[str, Any]:
        t0 = time.time()
        out_dir = Path(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        failed, diag = self.gate_report(meta_dir)
        report: dict[str, Any] = {"converter": "scripts/tokyo_l2_bt_stream_convert.py",
                                  "making_exchange": getattr(self, "making_exchange", FOLLOWER),
                                  "gate_symbol": self.gate_symbol,
                                  "label_date": label_date, "mode": "complete_day" if self.coverage else "sample_no_coverage",
                                  "failed_items": failed, "parts": self.parts, **diag, "artifacts": []}
        if failed and not (diagnostic_allow_known_gaps and only_known_gap_failures(failed)):
            report["status"] = "blocked"
        else:
            self._write_outputs(Path(parts_dir), out_dir, label_date, report)
            report["status"] = "ok_with_known_gaps" if failed else "ok"
        report.update(finalize_s=round(time.time() - t0, 2), peak_rss_mb=peak_rss_mb(), out_bytes=dir_bytes(out_dir))
        (out_dir / "stream_report.json").write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
        return report

    def _load_parts(self, parts_dir: Path, exchange: str, key: str, dtype: np.dtype) -> np.ndarray:
        arrays = []
        for p in self.parts:
            if p["exchange"] != exchange:
                continue
            f = parts_dir / p["part_file"]
            if sha256_file(f) != p["part_sha256"]:
                raise ValueError(f"part output changed: {f.name}")
            with np.load(f) as z:
                arr = z[key]
            if arr.dtype.fields != dtype.fields or arr.dtype.itemsize != dtype.itemsize:
                raise ValueError(f"part {f.name}:{key} dtype {arr.dtype} != {dtype}")
            arrays.append(arr.view(dtype))
        return np.concatenate(arrays) if arrays else np.empty(0, dtype=dtype)

    def _write_outputs(self, parts_dir: Path, out_dir: Path, label_date: str, report: dict[str, Any]) -> None:
        ymd = label_date.replace("-", "")
        base = self.base
        making_ex = getattr(self, "making_exchange", FOLLOWER)

        def save(kind: str, path: Path, rows: int, **arrays) -> None:
            np.savez_compressed(path, **arrays)
            report["artifacts"].append({"kind": kind, "path": path.name, "rows": rows, "sha256": sha256_file(path)})

        if getattr(self, "include_leader", True):
            bbo = self._load_parts(parts_dir, LEADER, "bbo", BBO_DTYPE)
            bbo = bbo[np.argsort(bbo["local_ts_ns"], kind="mergesort")]
            save("binance_bbo", out_dir / f"binance_bbo_{base}_{ymd}.npz", len(bbo),
                 **{name: bbo[name] for name in BBO_DTYPE.names})
            del bbo
        raw = self._load_parts(parts_dir, making_ex, "hbt_raw", event_dtype)
        if len(raw) != len(self.gate.hbt_rows):
            raise AssertionError(f"HBT rows {len(raw)} != emitted {len(self.gate.hbt_rows)}")
        ordered, order_diag = _order_and_validate_hbt_events(
            raw, exchange=making_ex, symbol=base, date_utc=label_date, stream_kind="follower_l2_hbt",
            local_book_clears=self.gate.local_book_clears)
        del raw
        save("follower_l2_hbt", out_dir / f"{making_ex}_l2_{base}_{ymd}.npz", len(ordered), data=ordered)
        report["order_diagnostics"] = order_diag
        report["hbt_event_types"] = event_histogram(ordered)
        report["hbt_time_range"] = {"first_local_ts": int(ordered["local_ts"].min()) if len(ordered) else None,
                                    "last_local_ts": int(ordered["local_ts"].max()) if len(ordered) else None}
        del ordered
        edges = self._load_parts(parts_dir, making_ex, "edges", EDGE_DTYPE)
        save("follower_l2_edges", out_dir / f"{making_ex}_l2_edges_{base}_{ymd}.npz", len(edges),
             **{name: edges[name] for name in EDGE_DTYPE.names})
        del edges
        l1 = self._load_parts(parts_dir, making_ex, "l1", BBO_DTYPE)
        l1 = l1[np.argsort(l1["local_ts_ns"], kind="mergesort")]
        save("follower_l1_bbo", out_dir / f"{making_ex}_l1_{base}_{ymd}.npz", len(l1),
             **{name: l1[name] for name in BBO_DTYPE.names})


class MultiSymbolBinanceBBOExtractor:
    """Single-pass streaming Binance @bookTicker extractor for multiple base symbols.

    Decompresses and validates each Binance partition ONCE (SHA-256, rx_seq,
    recv_mono, META gap/loss/switch, CompleteDayCoverage) and simultaneously emits
    `binance_bbo_{BASE}_{YMD}.npz` for every requested base symbol so multiple
    making-exchange experiments share one Binance download and scan pass.
    """

    def __init__(
        self,
        base_symbols: list[str] | tuple[str, ...],
        config: dict[str, Any] | None = None,
        coverage_date: str | None = None,
    ) -> None:
        self.bases = sorted({base_asset(s) for s in base_symbols})
        if not self.bases:
            raise ValueError("base_symbols must not be empty")
        self.native_to_base = {f"{b}USDT": b for b in self.bases}
        self.native_syms = tuple(sorted(self.native_to_base.keys()))
        self.coverage_date = coverage_date
        if coverage_date is not None and config is None:
            raise ValueError("complete-day coverage needs the collector config")
        self.coverage = (
            CompleteDayCoverage(narrowed_binance_config(config, self.bases), coverage_date)
            if coverage_date else None
        )
        self.st = ExchangeState(LEADER)
        self.sinks = {b: RowSink(BBO_DTYPE) for b in self.bases}
        self.exch_ahead_of_local: Counter = Counter()
        self.failed: Counter = Counter()
        self.failed_samples: list[str] = []
        self.day_gaps: dict[str, list[dict[str, Any]]] = {}
        self.day_local_loss: dict[str, list[dict[str, Any]]] = {}
        self.day_switches: Counter = Counter()
        self.local_loss_partitions: list[dict[str, Any]] = []
        self.parts: list[dict[str, Any]] = []
        self._day_lo = self._day_hi = None
        self._day = ""

    def _fail(self, code: str, detail: str) -> None:
        self.failed[code] += 1
        if len(self.failed_samples) < 50:
            self.failed_samples.append(f"{code}:{detail}")

    def _date(self, wall: int) -> str:
        if self._day_lo is None or not (self._day_lo <= wall < self._day_hi):
            day = datetime.fromtimestamp(wall // NS, tz=timezone.utc).strftime("%Y-%m-%d")
            start = int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()) * NS
            self._day_lo, self._day_hi, self._day = start, start + DAY_NS, day
        return self._day

    def process_partition(self, data_path: Path, manifest_path: Path, parts_dir: Path) -> dict[str, Any]:
        data_path, manifest_path, parts_dir = Path(data_path), Path(manifest_path), Path(parts_dir)
        t0 = time.time()
        checked = verify_local_partition(data_path, manifest_path)
        mf = checked["manifest"]
        if str(mf["exchange"]) != LEADER:
            raise ValueError(f"expected {LEADER} partition, got {mf['exchange']}")
        norm = normalize_manifest_window(mf, fallback_file_name=data_path.name)
        st = self.st
        key = frozen_sort_key(mf, data_path)
        if st.last_sort_key is not None and key <= st.last_sort_key:
            raise ValueError(f"{LEADER}: partition {data_path.name} is not after previous")
        in_cov = self.coverage is not None and norm["date_utc"] == self.coverage.date
        if in_cov:
            self.coverage.observe_partition(mf)
        boot = norm["boot_id"]
        if boot != st.prev_boot:
            st.prev_boot, st.prev_file_last_rx_seq, st.prev_file_had_late_frames = boot, None, False
            st.boot_rx_blocks, st.producer_prev_mono = [], {}

        c: Counter = Counter()
        rxs = array("q")
        recorded_local_loss = dropped_frames = 0
        prev_mono = st.producer_prev_mono
        coverage = self.coverage if in_cov else None
        bases_set = set(self.bases)
        native_syms = self.native_syms
        sinks = self.sinks

        dctx = zstd.ZstdDecompressor()
        with data_path.open("rb") as fh, dctx.stream_reader(fh) as reader:
            text = io.TextIOWrapper(reader, encoding="utf-8", errors="strict")
            for line_no, line in enumerate(text, start=1):
                trimmed = line.rstrip("\n")
                if not trimmed:
                    continue
                parts = trimmed.split("\t", 6)
                if len(parts) != 7:
                    raise ValueError(f"{data_path.name}:{line_no}: expected 7 TSV columns, got {len(parts)}")
                wall, mono, conn, epoch, rx = int(parts[0]), int(parts[1]), parts[2], int(parts[3]), int(parts[4])
                kind, raw = parts[5], parts[6]
                rxs.append(rx)
                c["lines"] += 1
                obj = None
                if kind == "META":
                    obj = json.loads(raw)
                    c["decoded"] += 1
                    pkey = record_monotonic_producer_key(LEADER, kind, conn, epoch, obj)
                else:
                    pkey = ("binance_market" if "aggTrade" in raw else "binance_public", conn, epoch)
                pm = prev_mono.get(pkey)
                if pm is not None and mono < pm:
                    self._fail("recv_mono_ns_regressed", f"{LEADER}:{data_path.name}:{pkey}:prev={pm},curr={mono}")
                prev_mono[pkey] = mono

                if kind == "META":
                    c["meta"] += 1
                    if not isinstance(obj, dict):
                        self._fail("meta_not_object", f"{LEADER}:{data_path.name}:{line_no}")
                        continue
                    ev = str(obj.get("event") or "")
                    sym = str(obj.get("symbol") or "ALL").strip()
                    is_all = not sym or sym.upper() == "ALL"
                    if ev in BINANCE_DEPTH_META:
                        c["binance_depth_meta"] += 1
                        continue
                    if not (is_all or base_asset(sym) in bases_set):
                        continue
                    date = self._date(wall)
                    gk = f"{LEADER}|{date}"
                    if ev == "GAP_START":
                        self.day_gaps.setdefault(gk, []).append(
                            {"start_wall_ns": wall, "end_wall_ns": None, "reason": obj.get("reason", "")})
                    elif ev == "GAP_END":
                        gaps = self.day_gaps.setdefault(gk, [])
                        if gaps and gaps[-1]["end_wall_ns"] is None:
                            gaps[-1]["end_wall_ns"] = wall
                        else:
                            gaps.append({"start_wall_ns": None, "end_wall_ns": wall, "reason": "gap_end_cross_day"})
                    elif ev == "LOCAL_LOSS":
                        dropped = int(obj.get("dropped_frames", obj.get("dropped", 1)))
                        recorded_local_loss += 1
                        dropped_frames += dropped
                        self.day_local_loss.setdefault(gk, []).append(
                            {"wall_ns": wall, "dropped": dropped, "reason": obj.get("reason", "")})
                    elif ev == "LATE_FRAME_REJECTED":
                        dropped = int(obj.get("dropped_frames", 1))
                        wid = str(obj.get("target_window_id") or obj.get("loss_attributed_window_id") or "")
                        recorded_local_loss += 1
                        dropped_frames += dropped
                        self.day_local_loss.setdefault(f"{LEADER}|{wid[:10] if len(wid) >= 10 else date}", []).append(
                            {"wall_ns": int(obj.get("orig_recv_wall_ns") or wall), "dropped": dropped,
                             "reason": f"late_frame_rejected_for_{wid or 'prior_window'}"})
                    elif ev == "SWITCH":
                        self.day_switches[gk] += 1
                    continue

                c["data"] += 1
                if "bookTicker" not in raw or not any(nsym in raw for nsym in native_syms):
                    c["dropped_prefilter"] += 1
                    continue
                obj = _decode(kind, raw)
                c["decoded"] += 1
                data = obj.get("data") if isinstance(obj.get("data"), dict) else obj
                is_bbo = data.get("e") == "bookTicker" or str(obj.get("stream", "")).endswith("@bookTicker")
                b = base_asset(data.get("s"))
                if not (is_bbo and b in bases_set):
                    c["dropped_after_decode"] += 1
                    continue
                if coverage is not None:
                    coverage.observe_record(LEADER, {"kind": kind, "recv_wall_ns": wall, "payload": obj})
                exch_ts = int(data.get("T") or data.get("E") or (wall // 1_000_000)) * 1_000_000
                if exch_ts > wall:
                    self.exch_ahead_of_local[b] += 1
                bid_px, ask_px = float(data["b"]), float(data["a"])
                sinks[b].append((wall, exch_ts, bid_px, float(data["B"]), ask_px, float(data["A"]),
                                 0.5 * (bid_px + ask_px)))
                c[f"bbo_{b}"] += 1

        # rx_seq monotonic check (same as StreamConverter._finish_rx_seq)
        arr = np.frombuffer(rxs, dtype=np.int64).copy() if len(rxs) else np.empty(0, dtype=np.int64)
        if len(arr):
            first = int(arr[0])
            if st.prev_file_last_rx_seq is not None and not st.prev_file_had_late_frames and first <= st.prev_file_last_rx_seq:
                self._fail("rx_seq_non_monotonic", f"{LEADER}:{data_path.name}:prev={st.prev_file_last_rx_seq},curr={first}")
            bad = np.flatnonzero(np.diff(arr) <= 0)
            for i in bad[:5].tolist():
                self._fail("rx_seq_non_monotonic", f"{LEADER}:{data_path.name}:prev={int(arr[i])},curr={int(arr[i + 1])}")
            uniq, counts = np.unique(arr, return_counts=True)
            in_dups = int(np.sum(counts[counts > 1] - 1))
            if in_dups:
                self._fail("rx_seq_duplicate_in_boot", f"{LEADER}:{data_path.name}:in_file={in_dups}")
            x_dups = _rx_dups_against(st.boot_rx_blocks, uniq)
            if x_dups:
                self._fail("rx_seq_duplicate_in_boot", f"{LEADER}:{data_path.name}:cross_file={x_dups}")
            lo, hi = int(uniq[0]), int(uniq[-1])
            st.boot_rx_blocks.append(("range", lo, hi) if len(uniq) == hi - lo + 1 else ("array", uniq))
            last = int(arr[-1])
            st.prev_file_last_rx_seq = last if st.prev_file_last_rx_seq is None else max(st.prev_file_last_rx_seq, last)
        st.prev_file_had_late_frames = int(norm["late_frames_accepted"]) > 0

        manifest_local_loss = int(mf.get("local_loss_lines") or 0) + int(mf.get("late_frames_rejected") or 0)
        if recorded_local_loss or manifest_local_loss:
            self.local_loss_partitions.append({
                "exchange": LEADER, "date_utc": mf["date_utc"], "window_id": norm["window_id"],
                "boot_id": boot, "data_file": data_path.name,
            })
        st.last_sort_key = key
        st.counts.update(c)

        parts_dir.mkdir(parents=True, exist_ok=True)
        name = f"{LEADER}_multi_{norm['window_id'].replace(':', '')}_{len(self.parts):04d}"
        tmp = parts_dir / f"{name}.tmp.npz"
        payload = {f"bbo_{b}": sinks[b].drain() for b in self.bases}
        np.savez_compressed(tmp, **payload)
        path = parts_dir / f"{name}.npz"
        tmp.replace(path)
        summary = {
            "window_id": norm["window_id"], "data_file": data_path.name, "data_sha256": mf["sha256"],
            "part_file": path.name, "part_sha256": sha256_file(path),
            "rows": {b: int(len(payload[f"bbo_{b}"])) for b in self.bases},
            "elapsed_s": round(time.time() - t0, 2),
        }
        self.parts.append(summary)
        return summary

    def finalize(
        self,
        parts_dir: Path,
        out_dir: Path,
        meta_dir: Path | None,
        label_date: str,
        *,
        diagnostic_allow_known_gaps: bool = False,
    ) -> dict[str, Any]:
        t0 = time.time()
        out_dir = Path(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        failed = [f"{k}:{n}" for k, n in sorted(self.failed.items())]
        coverage_report = self.coverage.report(meta_dir) if self.coverage is not None else {"status": "not_assessed"}
        chrony = load_chrony_uncertified_intervals(meta_dir)
        if self.coverage is not None and coverage_report["status"] != "complete":
            failed.append("incomplete_utc_day_coverage")
        if self.local_loss_partitions:
            failed.append("local_loss_detected_under_strict_zero_loss_gate")
        if self.coverage is not None and (any(self.day_gaps.values()) or chrony.get(self.coverage.date)):
            failed.append("complete_day_gap_or_uncertified_clock")

        ymd = label_date.replace("-", "")
        artifacts: dict[str, dict[str, Any]] = {}
        report: dict[str, Any] = {
            "extractor": "MultiSymbolBinanceBBOExtractor",
            "base_symbols": self.bases,
            "label_date": label_date,
            "failed_items": failed,
            "coverage": coverage_report,
            "gaps": self.day_gaps,
            "switches": dict(self.day_switches),
            "exch_ahead_of_local": dict(self.exch_ahead_of_local),
            "parts_count": len(self.parts),
            "artifacts": artifacts,
        }
        if failed and not (diagnostic_allow_known_gaps and only_known_gap_failures(failed)):
            report["status"] = "blocked"
        else:
            for b in self.bases:
                chunks = []
                for p in self.parts:
                    fpath = Path(parts_dir) / p["part_file"]
                    if sha256_file(fpath) != p["part_sha256"]:
                        raise ValueError(f"part output changed: {fpath.name}")
                    with np.load(fpath) as z:
                        chunks.append(z[f"bbo_{b}"].view(BBO_DTYPE))
                bbo = np.concatenate(chunks) if chunks else np.empty(0, dtype=BBO_DTYPE)
                bbo = bbo[np.argsort(bbo["local_ts_ns"], kind="mergesort")]
                apath = out_dir / f"binance_bbo_{b}_{ymd}.npz"
                np.savez_compressed(apath, **{name: bbo[name] for name in BBO_DTYPE.names})
                artifacts[b] = {"path": apath.name, "rows": int(len(bbo)), "sha256": sha256_file(apath)}
                del bbo, chunks
            report["status"] = "ok_with_known_gaps" if failed else "ok"
        report["finalize_s"] = round(time.time() - t0, 2)
        (out_dir / "binance_bbo_report.json").write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
        return report


def only_known_gap_failures(failed: list[str]) -> bool:
    """True iff failed_items contains only known/bounded interruption flags (never corruption/conflicts)."""
    if not failed:
        return False
    for item in failed:
        if item == "complete_day_gap_or_uncertified_clock":
            continue
        if "_depth_unrebuildable:" in item:
            continue
        return False
    return True


def run(args: argparse.Namespace) -> int:
    state_dir, parts_dir = Path(args.state), Path(args.state) / "parts"
    making_ex = getattr(args, "making_exchange", FOLLOWER) or FOLLOWER
    if (state_dir / PROGRESS_FILE).is_file():
        conv = StreamConverter.load(state_dir, parts_dir)
        if (conv.gate_symbol != args.gate_symbol or conv.coverage_date != (args.coverage_date or None)
                or getattr(conv, "making_exchange", FOLLOWER) != making_ex):
            raise ValueError("checkpoint was made for another exchange, symbol, or coverage date")
        print(f"RESUME parts_done={len(conv.parts)}", flush=True)
    else:
        config = json.loads(Path(args.config).read_text(encoding="utf-8")) if args.config else None
        conv = StreamConverter(args.gate_symbol, config, args.coverage_date or None, making_exchange=making_ex)
    done = conv.done_data()
    n = 0
    for exchange, dpath, mpath, _key in discover_partitions(Path(args.fetched), making_exchange=making_ex) if args.fetched else []:
        mf = json.loads(mpath.read_text(encoding="utf-8"))
        if mf.get("sha256") in done:
            continue
        summary = conv.process_partition(dpath, mpath, parts_dir)
        conv.checkpoint(state_dir)
        n += 1
        print("PART", json.dumps({k: summary[k] for k in ("exchange", "window_id", "rows", "elapsed_s", "peak_rss_mb")}),
              f"state_bytes={dir_bytes(state_dir)}", flush=True)
        if args.delete_inputs:
            dpath.unlink()
    print(f"CONVERTED new_parts={n} total_parts={len(conv.parts)}", flush=True)
    if args.finalize:
        label = args.coverage_date or args.label_date
        if not label:
            raise ValueError("--finalize needs --coverage-date or --label-date")
        meta = Path(args.meta) if args.meta else (Path(args.fetched) / "meta" if args.fetched else None)
        allow_gaps = bool(getattr(args, "diagnostic_allow_known_gaps", False))
        report = conv.finalize(parts_dir, Path(args.out), meta if meta and meta.exists() else None, label,
                               diagnostic_allow_known_gaps=allow_gaps)
        print("FINAL", report["status"], json.dumps(report["failed_items"]), flush=True)
        return 0 if report["status"] in ("ok", "ok_with_known_gaps") else 2
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Streaming Tokyo L2 converter (one making exchange symbol + Binance bookTicker)")
    p.add_argument("--fetched", type=Path, help="dir with raw/<exchange>/<date>/*.{manifest.json,tsv.zst}")
    p.add_argument("--config", type=Path, help="collector config (required with --coverage-date)")
    p.add_argument("--gate-symbol", required=True)
    p.add_argument("--making-exchange", default=FOLLOWER, help="making exchange (gate_futures, bybit_linear, bitget_v3)")
    p.add_argument("--state", type=Path, required=True, help="checkpoint dir (state.pkl, progress.json, parts/)")
    p.add_argument("--coverage-date", default="", help="enforce the complete-UTC-day gate for this date")
    p.add_argument("--label-date", default="", help="output file date when no coverage date is given")
    p.add_argument("--meta", type=Path)
    p.add_argument("--out", type=Path)
    p.add_argument("--finalize", action="store_true")
    p.add_argument("--delete-inputs", action="store_true", help="remove each .tsv.zst after its part is saved")
    p.add_argument("--diagnostic-allow-known-gaps", action="store_true",
                   help="emit diagnostic .npz artifacts when only known gap/depth-gap warnings exist")
    args = p.parse_args(argv)
    if args.finalize and not args.out:
        p.error("--finalize needs --out")
    return run(args)


if __name__ == "__main__":
    # Re-import under the module name so checkpoints pickle as tokyo_l2_bt_stream_convert.*.
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from tokyo_l2_bt_stream_convert import main as _main
    raise SystemExit(_main())
