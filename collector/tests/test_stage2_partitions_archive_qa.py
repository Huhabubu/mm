"""Stage 2 offline joint-acceptance tests:
- Configurable 5/15/30/60m partitions & mixed legacy hourly + v2 segmented format reading
- Idempotent SQLite v1 -> v2 ledger migration preserving existing records
- Google Drive 15m archive: bounded async seal wait, multi-segment merge & overlap detection,
  partial-window failure + next-window success + prior-window tracking & catch-up retry
- GitHub Incremental QA: verified archive index discovery, planned window & batch filtering,
  per-(producer, conn_id, epoch) monotonic check, 5-tuple memoization & invalidation,
  file/window/day 3-level reports, startup stub separation, 5-state model (zero-file overdue
  fail-closed), transient download retry, and injected outage / loss / cross-window seq break alerts.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from unittest.mock import patch

import pytest
import zstandard as zstd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python"))

import archive_sync as arch  # noqa: E402
import data_coverage as dc  # noqa: E402
import hourly_incremental_qa as qa  # noqa: E402


EXCHANGES_4 = ("binance_futures", "bybit_linear", "bitget_v3", "gate_futures")


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _md5_bytes(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def _make_config(
    *,
    partition_minutes: int = 15,
    exchanges: tuple[str, ...] = EXCHANGES_4,
    symbols: list[str] | None = None,
    stale_seconds: int = 900,
) -> dict:
    syms = symbols or ["BTCUSDT"]
    gate_syms = [s.replace("USDT", "_USDT") for s in syms]
    return {
        "schema_version": 1,
        "spec_version": "v1.4",
        "mode": "trial_15m_test",
        "deployment": {
            "partition_minutes": partition_minutes,
            "late_frame_grace_ms": 2000,
        },
        "archive": {
            "rclone_remote": "gdrive_main:tokyo_l2_data",
            "verify_remote_hash": True,
            "seal_wait_seconds": 10,
            "seal_poll_interval_seconds": 2,
        },
        "meta_daemon": {
            "collector_stale_seconds": stale_seconds,
            "continuous_quote_stale_seconds": stale_seconds,
            "depth_stale_seconds": stale_seconds,
            "event_trade_stale_seconds": stale_seconds,
            "event_trade_quiet_max_seconds": max(stale_seconds, 1800),
            "require_stream_activity": True,
        },
        "exchanges": {
            "binance_futures": {
                "enabled": "binance_futures" in exchanges,
                "symbols": syms,
                "public_channels": ["bookTicker", "depth@100ms"],
                "market_channels": ["aggTrade"],
            },
            "bybit_linear": {
                "enabled": "bybit_linear" in exchanges,
                "symbols": syms,
                "channels": ["orderbook.50", "orderbook.1", "publicTrade"],
            },
            "bitget_v3": {
                "enabled": "bitget_v3" in exchanges,
                "symbols": syms,
                "channels": ["books50", "books1", "publicTrade"],
            },
            "gate_futures": {
                "enabled": "gate_futures" in exchanges,
                "symbols": gate_syms,
                "channels": ["futures.obu.50", "futures.book_ticker", "futures.trades"],
            },
        },
    }


def _sample_payloads(exchange: str, symbol: str = "BTCUSDT", *, include_depth: bool = True, include_trade: bool = True) -> list[dict]:
    if exchange == "binance_futures":
        out = [{"stream": f"{symbol.lower()}@bookTicker", "data": {"e": "bookTicker", "s": symbol}}]
        if include_depth:
            out.append({"stream": f"{symbol.lower()}@depth@100ms", "data": {"e": "depthUpdate", "s": symbol}})
        if include_trade:
            out.append({"stream": f"{symbol.lower()}@aggTrade", "data": {"e": "aggTrade", "s": symbol}})
        return out
    if exchange == "bybit_linear":
        out = [{"topic": f"orderbook.1.{symbol}", "type": "snapshot", "data": {"s": symbol}}]
        if include_depth:
            out.append({"topic": f"orderbook.50.{symbol}", "type": "snapshot", "data": {"s": symbol}})
        if include_trade:
            out.append({"topic": f"publicTrade.{symbol}", "data": [{"s": symbol}]})
        return out
    if exchange == "bitget_v3":
        out = [{"arg": {"channel": "books1", "instId": symbol}, "data": [{"asks": [], "bids": []}]}]
        if include_depth:
            out.append({"arg": {"channel": "books50", "instId": symbol}, "data": [{"asks": [], "bids": []}]})
        if include_trade:
            out.append({"arg": {"channel": "publicTrade", "instId": symbol}, "data": [{"px": "1"}]})
        return out
    if exchange == "gate_futures":
        gs = symbol.replace("USDT", "_USDT")
        out = [{"channel": "futures.book_ticker", "event": "update", "result": {"s": gs}}]
        if include_depth:
            out.append({"channel": "futures.obu", "event": "update", "result": {"s": f"ob.{gs}.50", "full": True}})
        if include_trade:
            out.append({"channel": "futures.trades", "event": "update", "result": [{"contract": gs}]})
        return out
    raise ValueError(exchange)


def _write_sealed_partition(
    root: Path,
    *,
    exchange: str,
    window_start_s: int,
    partition_minutes: int = 15,
    boot_id: str = "boot_01",
    run_id: str | None = "trial_run_1",
    legacy_v1: bool = False,
    first_rx_seq: int = 1,
    offset_start_s: int = 1,
    offset_end_s: int | None = None,
    is_startup_stub: bool = False,
    include_depth: bool = True,
    include_trade: bool = True,
    extra_meta: list[dict] | None = None,
    custom_rows: list[tuple[int, int, str, int, int, str, dict]] | None = None,
    segment_suffix: str | None = None,
    dense: bool = False,
) -> tuple[Path, Path, dict]:
    wd = dc.window_descriptor(window_start_s, partition_minutes)
    date_utc = wd["date_utc"]
    ymd = date_utc.replace("-", "")
    hh = wd["hour_utc"]
    mm = wd["minute_utc"]
    start_ns = wd["planned_start_wall_ns"]
    end_ns = wd["planned_end_wall_ns"]
    if legacy_v1:
        stem = f"{exchange}_{ymd}_{hh}"
    else:
        tag = segment_suffix or f"{run_id or 'seg'}_{hashlib.sha256(boot_id.encode()).hexdigest()[:8]}"
        stem = f"{exchange}_{ymd}_{hh}{mm}_p{partition_minutes}m_{tag}" if partition_minutes < 60 else f"{exchange}_{ymd}_{hh}_{tag}"

    day_dir = root / "raw" / exchange / date_utc
    day_dir.mkdir(parents=True, exist_ok=True)
    data_path = day_dir / f"{stem}.tsv.zst"
    mf_path = day_dir / f"{stem}.manifest.json"

    rows_lines: list[str] = []
    data_lines = meta_lines = loss_lines = gap_lines = 0
    first_wall = last_wall = None
    last_seq = first_rx_seq - 1

    if custom_rows is not None:
        for wall_ns, mono_ns, conn, ep, seq, kind, payload in custom_rows:
            rows_lines.append(
                "\t".join([str(wall_ns), str(mono_ns), conn, str(ep), str(seq), kind, json.dumps(payload, separators=(",", ":"))])
            )
            first_wall = wall_ns if first_wall is None else min(first_wall, wall_ns)
            last_wall = wall_ns if last_wall is None else max(last_wall, wall_ns)
            last_seq = seq
            if kind == "META":
                meta_lines += 1
                if payload.get("event") == "LOCAL_LOSS":
                    loss_lines += 1
                if payload.get("event") == "GAP_START":
                    gap_lines += 1
            else:
                data_lines += 1
        actual_first_seq = custom_rows[0][4]
    else:
        payloads = _sample_payloads(exchange, include_depth=include_depth, include_trade=include_trade)
        seq = first_rx_seq
        mono = 1000
        tail_offset = offset_end_s if offset_end_s is not None else max(offset_start_s + 2, partition_minutes * 60 - 2)
        offsets = list(range(offset_start_s, tail_offset, 120)) if dense else [offset_start_s]
        offsets.append(tail_offset)
        times = [start_ns + offset * 1_000_000_000 for offset in offsets]
        for t_idx, base_t in enumerate(times):
            for p_idx, payload in enumerate(payloads):
                w = base_t + p_idx * 1_000_000
                rows_lines.append(
                    "\t".join([str(w), str(mono), "A", "1", str(seq), "DATA", json.dumps(payload, separators=(",", ":"))])
                )
                first_wall = w if first_wall is None else min(first_wall, w)
                last_wall = w if last_wall is None else max(last_wall, w)
                last_seq = seq
                seq += 1
                mono += 100
                data_lines += 1
        for meta_ev in extra_meta or []:
            w = (last_wall or start_ns) + 1_000_000
            rows_lines.append(
                "\t".join([str(w), str(mono), "A", "1", str(seq), "META", json.dumps(meta_ev, separators=(",", ":"))])
            )
            last_wall = max(last_wall or w, w)
            last_seq = seq
            seq += 1
            mono += 100
            meta_lines += 1
            if meta_ev.get("event") == "LOCAL_LOSS":
                loss_lines += 1
            if meta_ev.get("event") == "GAP_START":
                gap_lines += 1
        actual_first_seq = first_rx_seq

    raw_bytes = ("\n".join(rows_lines) + "\n").encode("utf-8")
    compressed = zstd.ZstdCompressor().compress(raw_bytes)
    data_path.write_bytes(compressed)

    if legacy_v1:
        manifest = {
            "schema_version": 1,
            "exchange": exchange,
            "date_utc": date_utc,
            "hour_utc": hh,
            "boot_id": boot_id,
            "file_name": data_path.name,
            "bytes": len(compressed),
            "sha256": _sha256_bytes(compressed),
            "zstd_verified": True,
            "start_wall_ns": first_wall,
            "end_wall_ns": last_wall,
            "first_rx_seq": actual_first_seq,
            "last_rx_seq": last_seq,
            "data_lines": data_lines,
            "data_escaped_lines": 0,
            "meta_lines": meta_lines,
            "local_loss_lines": loss_lines,
            "gap_start_lines": gap_lines,
            "switch_lines": 0,
        }
    else:
        manifest = {
            "schema_version": 2,
            "exchange": exchange,
            "date_utc": date_utc,
            "hour_utc": hh,
            "minute_utc": mm,
            "partition_minutes": partition_minutes,
            "window_id": wd["window_id"],
            "planned_start_wall_ns": start_ns,
            "planned_end_wall_ns": end_ns,
            "run_id": run_id,
            "segment_id": stem,
            "boot_id": boot_id,
            "file_name": data_path.name,
            "bytes": len(compressed),
            "sha256": _sha256_bytes(compressed),
            "zstd_verified": True,
            "start_wall_ns": first_wall,
            "end_wall_ns": last_wall,
            "first_rx_seq": actual_first_seq,
            "last_rx_seq": last_seq,
            "data_lines": data_lines,
            "data_escaped_lines": 0,
            "meta_lines": meta_lines,
            "local_loss_lines": loss_lines,
            "gap_start_lines": gap_lines,
            "switch_lines": 0,
            "is_startup_stub": is_startup_stub,
            "settled_by_timer": False,
            "coverage_incomplete": is_startup_stub,
            "late_frames_accepted": 0,
            "late_frames_rejected": 0,
        }
    mf_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return data_path, mf_path, manifest


class _MemoryArchiveBackend(arch.ArchiveBackend):
    """In-memory Google Drive backend supporting MD5/SHA256 verification and fault injection."""

    def __init__(self, fail_copy_paths: set[str] | None = None) -> None:
        self.objects: dict[str, bytes] = {}
        self.copy_calls: list[str] = []
        self.fail_copy_paths: set[str] = set(fail_copy_paths or set())

    def stat_remote(self, rel_path: str) -> arch.RemoteObjectInfo:
        blob = self.objects.get(rel_path)
        if blob is None:
            return arch.RemoteObjectInfo(exists=False)
        return arch.RemoteObjectInfo(
            exists=True,
            size=len(blob),
            md5=_md5_bytes(blob),
            sha256=_sha256_bytes(blob),
            remote_id=f"gid_{hashlib.sha1(rel_path.encode()).hexdigest()[:12]}",
        )

    def copy_file(self, local_path: Path, rel_path: str, immutable: bool = True) -> None:
        self.copy_calls.append(rel_path)
        if rel_path in self.fail_copy_paths:
            raise RuntimeError(f"injected transient upload error for {rel_path}")
        self.objects[rel_path] = local_path.read_bytes()

    def readback_sha256(self, rel_path: str) -> str:
        return _sha256_bytes(self.objects[rel_path])

    def download_file(self, rel_path: str, local_dest: Path) -> None:
        local_dest.parent.mkdir(parents=True, exist_ok=True)
        local_dest.write_bytes(self.objects[rel_path])


# ---------------------------------------------------------------------------
# 1. Mixed legacy hourly + new 15m reading & idempotent SQLite v1 -> v2 migration
# ---------------------------------------------------------------------------


def test_mixed_legacy_hourly_and_new_15m_ledger_migration_and_index():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        w_legacy = int(datetime(2026, 11, 21, 0, 0, tzinfo=timezone.utc).timestamp())
        w_new15 = int(datetime(2026, 11, 21, 1, 0, tzinfo=timezone.utc).timestamp())

        # Create a legacy hourly v1 file + legacy v1 SQLite schema (without Stage 2 columns)
        d_leg, m_leg, _ = _write_sealed_partition(
            root, exchange="binance_futures", window_start_s=w_legacy, partition_minutes=60, legacy_v1=True, boot_id="old_boot"
        )
        rel_leg_data = d_leg.relative_to(root).as_posix()
        rel_leg_mf = m_leg.relative_to(root).as_posix()
        db_path = root / arch.LEDGER_DB_FILENAME
        db_path.parent.mkdir(parents=True, exist_ok=True)

        # Simulate exact production legacy v1 SQLite database before Stage 2 upgrade
        conn = sqlite3.connect(str(db_path))
        conn.executescript(
            """
            CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO schema_meta(key, value) VALUES ('schema_version', '1');
            CREATE TABLE partitions (
                exchange TEXT NOT NULL,
                date_utc TEXT NOT NULL,
                hour_utc TEXT NOT NULL,
                boot_id TEXT NOT NULL,
                partition_id TEXT NOT NULL,
                rel_data_path TEXT NOT NULL UNIQUE,
                rel_manifest_path TEXT NOT NULL UNIQUE,
                state TEXT NOT NULL CHECK (state IN ('SEALED', 'VERIFIED', 'PRUNED')),
                local_bytes INTEGER NOT NULL,
                local_sha256 TEXT NOT NULL,
                local_md5 TEXT NOT NULL,
                manifest_bytes INTEGER NOT NULL,
                manifest_sha256 TEXT NOT NULL,
                manifest_md5 TEXT NOT NULL,
                local_mtime_epoch_s REAL,
                sealed_at_utc TEXT NOT NULL,
                remote_data_id TEXT,
                remote_data_bytes INTEGER,
                remote_data_md5 TEXT,
                remote_data_sha256 TEXT,
                remote_data_verified INTEGER NOT NULL DEFAULT 0,
                remote_data_verification_method TEXT,
                remote_manifest_id TEXT,
                remote_manifest_bytes INTEGER,
                remote_manifest_md5 TEXT,
                remote_manifest_sha256 TEXT,
                remote_manifest_verified INTEGER NOT NULL DEFAULT 0,
                remote_manifest_verification_method TEXT,
                verified_at_utc TEXT,
                pruned_at_utc TEXT,
                last_error_code TEXT,
                last_error_message TEXT,
                last_error_at_utc TEXT,
                has_conflict INTEGER NOT NULL DEFAULT 0,
                conflict_detail_json TEXT,
                source TEXT NOT NULL DEFAULT 'live_scan',
                updated_at_utc TEXT NOT NULL,
                PRIMARY KEY (exchange, date_utc, hour_utc, boot_id, partition_id)
            );
            CREATE TABLE archive_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_utc TEXT NOT NULL,
                run_id TEXT NOT NULL,
                exchange TEXT,
                date_utc TEXT,
                hour_utc TEXT,
                boot_id TEXT,
                partition_id TEXT,
                rel_data_path TEXT,
                event_type TEXT NOT NULL,
                from_state TEXT,
                to_state TEXT,
                detail_json TEXT
            );
            CREATE TABLE meta_sync_state (
                rel_path TEXT PRIMARY KEY,
                size_bytes INTEGER NOT NULL,
                sha256 TEXT NOT NULL,
                synced_at_utc TEXT NOT NULL
            );
            """
        )
        leg_b = d_leg.read_bytes()
        mf_b = m_leg.read_bytes()
        conn.execute(
            """
            INSERT INTO partitions (
                rel_data_path, rel_manifest_path, exchange, date_utc, hour_utc, boot_id, partition_id,
                state, local_bytes, local_sha256, local_md5, manifest_bytes, manifest_sha256, manifest_md5,
                sealed_at_utc, remote_data_id, remote_data_bytes, remote_data_md5, remote_data_sha256, remote_data_verified,
                remote_manifest_id, remote_manifest_bytes, remote_manifest_md5, remote_manifest_sha256, remote_manifest_verified,
                verified_at_utc, updated_at_utc
            ) VALUES (?, ?, 'binance_futures', '2026-11-21', '00', 'old_boot', 'binance_futures_20261121_00',
                'VERIFIED', ?, ?, ?, ?, ?, ?, '2026-11-21T01:05:00Z',
                'gid_old_d', ?, ?, ?, 1, 'gid_old_m', ?, ?, ?, 1,
                '2026-11-21T01:05:00Z', '2026-11-21T01:05:00Z')
            """,
            (
                rel_leg_data, rel_leg_mf,
                len(leg_b), _sha256_bytes(leg_b), _md5_bytes(leg_b),
                len(mf_b), _sha256_bytes(mf_b), _md5_bytes(mf_b),
                len(leg_b), _md5_bytes(leg_b), _sha256_bytes(leg_b),
                len(mf_b), _md5_bytes(mf_b), _sha256_bytes(mf_b),
            ),
        )
        conn.commit()
        conn.close()

        # Now add a brand new 15m v2 segmented partition
        _write_sealed_partition(
            root, exchange="binance_futures", window_start_s=w_new15, partition_minutes=15, legacy_v1=False, boot_id="new_boot_15m"
        )

        backend = _MemoryArchiveBackend()
        res = arch.sync_sealed_directory(
            root,
            backend,
            verify_remote=True,
            now_epoch_s=w_new15 + 16 * 60,
            upload_ledger_snapshot=False,
        )
        assert res["status"] == "ok"
        assert res["skipped_verified_count"] == 1
        assert res["newly_verified_count"] == 1

        # Verify SQLite migrated cleanly to schema v2 without losing the legacy verified row
        with arch.ArchiveLedger(db_path) as ledger:
            assert ledger.get_meta("schema_version") == str(arch.LEDGER_SCHEMA_VERSION)
            old_row = ledger.get_partition_by_rel_data(rel_leg_data)
            assert old_row is not None and old_row["state"] == arch.STATE_VERIFIED
            assert old_row["partition_minutes"] == 60
            assert old_row["window_id"] == "2026-11-21T00:00Z"

        # Verify cloud-readable archive_manifest_index.json contains BOTH legacy hourly & new 15m entries!
        assert arch.ARCHIVE_MANIFEST_INDEX_RELPATH in backend.objects
        idx_obj = json.loads(backend.objects[arch.ARCHIVE_MANIFEST_INDEX_RELPATH].decode("utf-8"))
        assert len(idx_obj["entries"]) == 2
        pms = sorted(e["partition_minutes"] for e in idx_obj["entries"])
        assert pms == [15, 60]

        # Repeat sync with zero changes must remain strictly no-op (zero remote uploads)
        backend.copy_calls.clear()
        res2 = arch.sync_sealed_directory(
            root, backend, verify_remote=True, now_epoch_s=w_new15 + 17 * 60, upload_ledger_snapshot=False
        )
        assert res2["status"] == "ok"
        assert backend.copy_calls == []


# ---------------------------------------------------------------------------
# 2. Archive 15m async seal wait, multi-segment merge & overlap detection
# ---------------------------------------------------------------------------


def test_archive_15m_bounded_seal_wait_and_multi_segment_reconciliation():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        w0 = int(datetime(2026, 11, 21, 0, 0, tzinfo=timezone.utc).timestamp())
        now_s = w0 + 16 * 60  # 00:16Z -> expected window is 00:00Z

        # Create sealed segments for 3 exchanges; binance_futures still sealing as `.partial`
        for ex in ("bitget_v3", "gate_futures"):
            _write_sealed_partition(root, exchange=ex, window_start_s=w0, partition_minutes=15, boot_id="b1", dense=True)

        # Mid-window restart produced 2 strictly non-overlapping segments for bybit_linear
        _write_sealed_partition(
            root,
            exchange="bybit_linear",
            window_start_s=w0,
            partition_minutes=15,
            boot_id="b1",
            first_rx_seq=1,
            offset_start_s=1,
            offset_end_s=440,
            dense=True,
        )
        _write_sealed_partition(
            root,
            exchange="bybit_linear",
            window_start_s=w0,
            partition_minutes=15,
            boot_id="b2_restart",
            first_rx_seq=1,
            offset_start_s=450,
            offset_end_s=895,
            dense=True,
        )

        # Pending partial for binance_futures in 00:00Z
        b_dir = root / "raw" / "binance_futures" / "2026-11-21"
        b_dir.mkdir(parents=True, exist_ok=True)
        partial = b_dir / "binance_futures_20261121_0000_p15m_trial_run_1_11111111.tsv.zst.partial"
        partial.write_bytes(b"sealing-in-progress")

        sleeps: list[float] = []

        def fake_sleep(sec: float) -> None:
            sleeps.append(sec)
            if len(sleeps) == 2:
                partial.unlink()
                _write_sealed_partition(
                    root, exchange="binance_futures", window_start_s=w0, partition_minutes=15, boot_id="b1", dense=True
                )

        backend = _MemoryArchiveBackend()
        res = arch.sync_sealed_directory(
            root,
            backend,
            verify_remote=True,
            now_epoch_s=now_s,
            upload_ledger_snapshot=False,
            expected_hourly_exchanges=set(EXCHANGES_4),
            partition_minutes=15,
            seal_wait_seconds=10.0,
            seal_poll_interval_seconds=2.0,
            sleeper=fake_sleep,
        )
        assert res["status"] == "ok"
        rec = res["expected_window_reconciliation"]
        assert rec["expected_window_id"] == "2026-11-21T00:00Z"
        assert rec["seal_wait_attempts"] == 2
        assert rec["per_exchange_segments"]["bybit_linear"]["segment_count"] == 2
        assert rec["overlaps"] == []
        assert rec["missing_exchanges"] == []

        # Now inject a same-boot rx_seq overlap on gate_futures and verify explicit overlap error!
        _write_sealed_partition(
            root,
            exchange="gate_futures",
            window_start_s=w0,
            partition_minutes=15,
            boot_id="b1",
            first_rx_seq=2,
            segment_suffix="dup_overlap",
            dense=True,
        )
        res_ov = arch.sync_sealed_directory(
            root,
            backend,
            verify_remote=True,
            now_epoch_s=now_s + 30,
            upload_ledger_snapshot=False,
            expected_hourly_exchanges=set(EXCHANGES_4),
            partition_minutes=15,
        )
        assert res_ov["status"] == "blocked"
        ov_list = res_ov["expected_window_reconciliation"]["overlaps"]
        assert len(ov_list) == 1
        assert ov_list[0]["exchange"] == "gate_futures"
        assert ov_list[0]["same_boot_seq_overlap"] is True
        assert any("segment_seq_overlap_in_expected_window:gate_futures" in e for e in res_ov["failed_items"])


# ---------------------------------------------------------------------------
# 3. Archive partial success on W0 -> W1 succeeds -> W0 still tracked & retried
# ---------------------------------------------------------------------------


def test_archive_window_n_partial_failure_still_tracked_and_retried_after_window_n1_succeeds():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        w0 = int(datetime(2026, 11, 21, 0, 0, tzinfo=timezone.utc).timestamp())
        w1 = w0 + 15 * 60

        w0_paths: dict[str, Path] = {}
        for ex in EXCHANGES_4:
            d, _, _ = _write_sealed_partition(root, exchange=ex, window_start_s=w0, partition_minutes=15, boot_id="b1", dense=True)
            w0_paths[ex] = d
        rel_fail = w0_paths["binance_futures"].relative_to(root).as_posix()

        backend = _MemoryArchiveBackend(fail_copy_paths={rel_fail})

        # Run 1 (at 00:16Z, expected W0): 3 exchanges verify, binance_futures fails -> blocked!
        r1 = arch.sync_sealed_directory(
            root,
            backend,
            verify_remote=True,
            now_epoch_s=w0 + 16 * 60,
            upload_ledger_snapshot=False,
            expected_hourly_exchanges=set(EXCHANGES_4),
            partition_minutes=15,
        )
        assert r1["status"] == "blocked"
        assert r1["verified_count"] == 3  # 3 valid sealed objects WERE archived immediately
        assert r1["expected_window_reconciliation"]["missing_exchanges"] == ["binance_futures"]

        # Window W1 arrives for all 4 exchanges; binance W0 STILL failing on Run 2 (at 00:31Z)
        for ex in EXCHANGES_4:
            _write_sealed_partition(root, exchange=ex, window_start_s=w1, partition_minutes=15, boot_id="b1", first_rx_seq=10, dense=True)

        r2 = arch.sync_sealed_directory(
            root,
            backend,
            verify_remote=True,
            now_epoch_s=w1 + 16 * 60,
            upload_ledger_snapshot=False,
            expected_hourly_exchanges=set(EXCHANGES_4),
            partition_minutes=15,
        )
        # W1 itself verified all 4 exchanges, BUT prior window W0 is STILL tracked & blocks whole-job OK!
        assert r2["status"] == "blocked"
        rec2 = r2["expected_window_reconciliation"]
        assert rec2["expected_window_id"] == "2026-11-21T00:15Z"
        assert rec2["missing_exchanges"] == []
        assert "2026-11-21T00:00Z" in rec2["pending_prior_windows"]
        assert any("pending_prior_window_unverified:2026-11-21T00:00Z" in e for e in r2["failed_items"])

        # Run 3: transient error clears -> binance W0 is retried & verified -> all green!
        backend.fail_copy_paths.clear()
        r3 = arch.sync_sealed_directory(
            root,
            backend,
            verify_remote=True,
            now_epoch_s=w1 + 17 * 60,
            upload_ledger_snapshot=False,
            expected_hourly_exchanges=set(EXCHANGES_4),
            partition_minutes=15,
        )
        assert r3["status"] == "ok"
        assert r3["newly_verified_count"] == 1
        assert r3["expected_window_reconciliation"]["pending_prior_windows"] == []
        assert r3["state_counts"][arch.STATE_VERIFIED] == 8


# ---------------------------------------------------------------------------
# 4. GitHub Incremental QA: per-producer mono check, index discovery, 5-tuple
#    memoization, 3-level reports, 5-state zero-file overdue, retries & alerts
# ---------------------------------------------------------------------------


def test_qa_per_producer_monotonic_clock_allows_binance_multi_endpoint_and_catches_real_regression():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        w0 = int(datetime(2026, 11, 21, 0, 0, tzinfo=timezone.utc).timestamp())
        start_ns = w0 * 1_000_000_000
        cfg = _make_config(partition_minutes=15, exchanges=("binance_futures",), stale_seconds=900)

        # Case A: binance_public (mono 200, 300) interleaved with binance_market aggTrade (mono 150, 250)
        # This reproduces the real run 44887069060 cross-endpoint interleaving and MUST PASS!
        interleaved_rows = [
            (start_ns + 1_000_000_000, 200, "A", 1, 1, "DATA", {"stream": "btcusdt@bookTicker", "data": {"e": "bookTicker", "s": "BTCUSDT"}}),
            (start_ns + 1_001_000_000, 150, "A", 1, 2, "DATA", {"stream": "btcusdt@aggTrade", "data": {"e": "aggTrade", "s": "BTCUSDT"}}),
            (start_ns + 1_002_000_000, 300, "A", 1, 3, "DATA", {"stream": "btcusdt@depth@100ms", "data": {"e": "depthUpdate", "s": "BTCUSDT"}}),
            (start_ns + 898_000_000_000, 400, "A", 1, 4, "DATA", {"stream": "btcusdt@bookTicker", "data": {"e": "bookTicker", "s": "BTCUSDT"}}),
            (start_ns + 898_001_000_000, 250, "A", 1, 5, "DATA", {"stream": "btcusdt@aggTrade", "data": {"e": "aggTrade", "s": "BTCUSDT"}}),
            (start_ns + 898_002_000_000, 500, "A", 1, 6, "DATA", {"stream": "btcusdt@depth@100ms", "data": {"e": "depthUpdate", "s": "BTCUSDT"}}),
        ]
        d_ok, _, mf_ok = _write_sealed_partition(
            root, exchange="binance_futures", window_start_s=w0, partition_minutes=15, custom_rows=interleaved_rows, boot_id="mono_ok"
        )
        res_ok = qa.check_partition(d_ok, mf_ok, cfg)
        assert res_ok["qa_status"] == qa.QA_STATUS_PASSED, res_ok["problems"]

        # Case B: true same-producer regression (binance_public bookTicker mono=300 -> depth@100ms mono=250) -> MUST FAIL!
        regressed_rows = list(interleaved_rows)
        regressed_rows[2] = (
            start_ns + 1_002_000_000, 180, "A", 1, 3, "DATA", {"stream": "btcusdt@depth@100ms", "data": {"e": "depthUpdate", "s": "BTCUSDT"}}
        )
        d_bad, _, mf_bad = _write_sealed_partition(
            root, exchange="binance_futures", window_start_s=w0, partition_minutes=15, custom_rows=regressed_rows, boot_id="mono_bad"
        )
        res_bad = qa.check_partition(d_bad, mf_bad, cfg)
        assert res_bad["qa_status"] == qa.QA_STATUS_FAILED
        assert any("receive monotonic clock regressed (binance_public:A:1)" in p for p in res_bad["problems"])


def _wire_mock_drive(archive_root: Path, backend: _MemoryArchiveBackend):
    """Build rclone `_run` and `_download` fakes backed by `_MemoryArchiveBackend`."""
    def fake_run(*args: str) -> str:
        # Handle `rclone lsf <remote>/meta -R --files-only` or `<remote>/raw ...`
        target = args[2]
        if target.endswith("/meta"):
            prefix = "meta/"
            return "\n".join(sorted(k.removeprefix(prefix) for k in backend.objects if k.startswith(prefix))) + "\n"
        if target.endswith("/raw"):
            prefix = "raw/"
            return "\n".join(
                sorted(k.removeprefix(prefix) for k in backend.objects if k.startswith(prefix) and k.endswith(".manifest.json"))
            ) + "\n"
        return ""

    def fake_download(_remote: str, rel_path: str, target: Path) -> None:
        if rel_path not in backend.objects:
            raise subprocess.CalledProcessError(3, ["rclone", "copyto", rel_path], stderr="not found")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(backend.objects[rel_path])

    return fake_run, fake_download


def test_qa_index_discovery_memoization_invalidation_three_levels_and_five_states():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        archive_root = root / "local_data"
        state_dir = root / "qa_state"
        w0 = int(datetime(2026, 11, 21, 0, 0, tzinfo=timezone.utc).timestamp())
        w1 = w0 + 15 * 60
        cfg = _make_config(partition_minutes=15, exchanges=EXCHANGES_4, stale_seconds=900)
        cfg_path = root / "config.json"
        cfg_path.write_text(json.dumps(cfg, indent=2), encoding="utf-8")

        # Produce a complete 4-exchange 15m window W0 (with 1 startup stub on binance) and archive to memory Drive
        for ex in EXCHANGES_4:
            _write_sealed_partition(
                archive_root,
                exchange=ex,
                window_start_s=w0,
                partition_minutes=15,
                boot_id="boot_qa_1",
                run_id="batch_A",
                is_startup_stub=(ex == "binance_futures"),
            )
        # Also add a different batch_B partition to test `--run-id` filtering
        _write_sealed_partition(
            archive_root,
            exchange="gate_futures",
            window_start_s=w0,
            partition_minutes=15,
            boot_id="boot_other_batch",
            run_id="batch_B",
        )

        backend = _MemoryArchiveBackend()
        sync_res = arch.sync_sealed_directory(
            archive_root, backend, verify_remote=True, now_epoch_s=w0 + 16 * 60, upload_ledger_snapshot=False
        )
        assert sync_res["status"] == "ok"
        fake_run, fake_download = _wire_mock_drive(archive_root, backend)

        # 1. First QA run filtering by run_id="batch_A" & since_window="2026-11-21T00:00Z"
        with patch.object(qa, "_run", side_effect=fake_run), \
             patch.object(qa, "_download", side_effect=fake_download) as dl_mock:
            r1 = qa.run(
                "qa:",
                cfg_path,
                state_dir,
                limit=16,
                since_date="2026-11-21",
                since_window="2026-11-21T00:00Z",
                run_id="batch_A",
                checker_commit="abcdef1234567890",
                now_epoch_s=w0 + 16 * 60,
                sla_grace_minutes=10.0,
            )
        assert r1["discovery_mode"] == "archive_manifest_index"
        assert r1["eligible_manifest_count"] == 4  # batch_B excluded!
        assert r1["attempted"] == 4
        assert r1["ok"] == 4
        assert r1["qa_status"] == qa.QA_STATUS_PASSED
        assert r1["status"] == "ok"
        assert r1["checker_commit_sha"] == "abcdef1234567890"
        assert r1["window_reports_written"] == 1
        assert r1["day_reports_written"] == 1
        # 1 index download + 4 * (manifest + data) = 9 downloads
        assert dl_mock.call_count == 9

        # Verify all 3 report levels exist on disk and startup stub is listed separately!
        win_report_path = state_dir / "qa_reports" / "windows" / "2026-11-21" / "2026-11-21T0000Z.window.qa.json"
        day_report_path = state_dir / "qa_reports" / "days" / "2026-11-21.day.qa.json"
        assert win_report_path.is_file() and day_report_path.is_file()
        win_rep = json.loads(win_report_path.read_text(encoding="utf-8"))
        day_rep = json.loads(day_report_path.read_text(encoding="utf-8"))
        assert win_rep["checker_commit_sha"] == "abcdef1234567890"
        assert win_rep["qa_status"] == qa.QA_STATUS_PASSED
        assert len(win_rep["startup_stub_segments"]) == 1
        assert win_rep["startup_stub_segments"][0]["exchange"] == "binance_futures"
        assert day_rep["checker_commit_sha"] == "abcdef1234567890"
        assert day_rep["expected_windows_count"] == 96  # 24h / 15m

        # 2. Second QA run with identical inputs -> 100% skipped, 0 re-downloads of partition data!
        with patch.object(qa, "_run", side_effect=fake_run), \
             patch.object(qa, "_download", side_effect=fake_download) as dl_mock2:
            r2 = qa.run(
                "qa:",
                cfg_path,
                state_dir,
                limit=16,
                since_date="2026-11-21",
                since_window="2026-11-21T00:00Z",
                run_id="batch_A",
                checker_commit="abcdef1234567890",
                now_epoch_s=w0 + 17 * 60,
                sla_grace_minutes=10.0,
            )
        assert r2["attempted"] == 0
        assert r2["already_checked"] == 4
        assert r2["qa_status"] == qa.QA_STATUS_PASSED
        assert r2["status"] == "ok"
        assert dl_mock2.call_count == 1  # only archive_manifest_index.json!

        # 3. Rule version bump (`QA_VERSION`) forces re-check!
        with patch.object(qa, "QA_VERSION", qa.QA_VERSION + 1), \
             patch.object(qa, "_run", side_effect=fake_run), \
             patch.object(qa, "_download", side_effect=fake_download):
            r3 = qa.run(
                "qa:",
                cfg_path,
                state_dir,
                limit=16,
                since_date="2026-11-21",
                since_window="2026-11-21T00:00Z",
                run_id="batch_A",
                now_epoch_s=w0 + 18 * 60,
                sla_grace_minutes=10.0,
            )
        assert r3["attempted"] == 4 and r3["already_checked"] == 0

        # 4. Collector config change (`effective_config_sha256`) forces re-check!
        cfg_changed = json.loads(cfg_path.read_text(encoding="utf-8"))
        cfg_changed["meta_daemon"]["collector_stale_seconds"] = 600
        cfg_path.write_text(json.dumps(cfg_changed, indent=2), encoding="utf-8")
        with patch.object(qa, "_run", side_effect=fake_run), \
             patch.object(qa, "_download", side_effect=fake_download):
            r4 = qa.run(
                "qa:",
                cfg_path,
                state_dir,
                limit=16,
                since_date="2026-11-21",
                since_window="2026-11-21T00:00Z",
                run_id="batch_A",
                now_epoch_s=w0 + 19 * 60,
                sla_grace_minutes=10.0,
            )
        assert r4["attempted"] == 4 and r4["already_checked"] == 0

        # 5. Content hash change in archive index forces re-check of ONLY the changed partition!
        #    And because the index hash ("f"*64) differs from the actual remote manifest file,
        #    3-way verification (Index <-> Manifest <-> Data) must fail closed (`blocked == 1`)!
        orig_index_bytes = backend.objects[arch.ARCHIVE_MANIFEST_INDEX_RELPATH]
        idx_mut = json.loads(orig_index_bytes.decode("utf-8"))
        for ent in idx_mut["entries"]:
            if ent.get("collection_run_id") == "batch_A" and ent["exchange"] == "gate_futures":
                ent["manifest_sha256"] = "f" * 64
        backend.objects[arch.ARCHIVE_MANIFEST_INDEX_RELPATH] = json.dumps(idx_mut).encode("utf-8")
        with patch.object(qa, "_run", side_effect=fake_run), \
             patch.object(qa, "_download", side_effect=fake_download):
            r5 = qa.run(
                "qa:",
                cfg_path,
                state_dir,
                limit=16,
                since_date="2026-11-21",
                since_window="2026-11-21T00:00Z",
                run_id="batch_A",
                now_epoch_s=w0 + 20 * 60,
                sla_grace_minutes=10.0,
            )
        assert r5["attempted"] == 1 and r5["already_checked"] == 3
        assert r5["blocked"] == 1
        assert r5["qa_status"] == qa.QA_STATUS_FAILED

        # Restore genuine index -> re-check of that 1 partition passes 3-way verification again!
        backend.objects[arch.ARCHIVE_MANIFEST_INDEX_RELPATH] = orig_index_bytes
        with patch.object(qa, "_run", side_effect=fake_run), \
             patch.object(qa, "_download", side_effect=fake_download):
            r5_restored = qa.run(
                "qa:",
                cfg_path,
                state_dir,
                limit=16,
                since_date="2026-11-21",
                since_window="2026-11-21T00:00Z",
                run_id="batch_A",
                now_epoch_s=w0 + 20 * 60,
                sla_grace_minutes=10.0,
            )
        assert r5_restored["attempted"] == 1 and r5_restored["already_checked"] == 3
        assert r5_restored["ok"] == 1 and r5_restored["qa_status"] == qa.QA_STATUS_PASSED

        # 6. Zero-file / missing-window SLA tests:
        #    At w1 + 17m (00:32Z, 2m after W1 ended, sla_grace=10m) -> W1 is PENDING_ARCHIVE (not overdue!)
        #    At w1 + 30m (00:45Z, 15m after W1 ended, sla_grace=10m) -> W1 is OVERDUE (status="blocked"!)
        with patch.object(qa, "_run", side_effect=fake_run), \
             patch.object(qa, "_download", side_effect=fake_download):
            r_pending = qa.run(
                "qa:",
                cfg_path,
                state_dir,
                limit=16,
                since_date="2026-11-21",
                since_window="2026-11-21T00:00Z",
                run_id="batch_A",
                now_epoch_s=w1 + 17 * 60,
                sla_grace_minutes=10.0,
            )
            r_overdue = qa.run(
                "qa:",
                cfg_path,
                state_dir,
                limit=16,
                since_date="2026-11-21",
                since_window="2026-11-21T00:00Z",
                run_id="batch_A",
                now_epoch_s=w1 + 30 * 60,
                sla_grace_minutes=10.0,
            )
        assert r_pending["qa_status"] == qa.QA_STATUS_PENDING_ARCHIVE
        assert r_pending["status"] == "blocked"  # pending_archive must NOT report whole-job status="ok"!
        assert r_pending["current_window_completed"] is False
        assert r_pending["current_window_verification_status"] == qa.QA_STATUS_PENDING_ARCHIVE
        assert "2026-11-21T00:15Z" in r_pending["pending_archive_windows"]
        assert r_overdue["qa_status"] == qa.QA_STATUS_OVERDUE
        assert r_overdue["status"] == "blocked"
        assert "2026-11-21T00:15Z" in r_overdue["overdue_windows"]


def test_qa_zero_files_overdue_fails_closed_never_false_green():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        state_dir = root / "state"
        cfg_path = root / "config.json"
        cfg_path.write_text(json.dumps(_make_config(partition_minutes=15)), encoding="utf-8")
        w0 = int(datetime(2026, 11, 21, 8, 0, tzinfo=timezone.utc).timestamp())

        # Empty verified index on Drive (reproduces run 44887217848 zero-file bug!)
        backend = _MemoryArchiveBackend()
        backend.objects[arch.ARCHIVE_MANIFEST_INDEX_RELPATH] = json.dumps(
            {"schema_version": 2, "entries": []}
        ).encode("utf-8")
        fake_run, fake_download = _wire_mock_drive(root, backend)
        with patch.object(qa, "_run", side_effect=fake_run), \
             patch.object(qa, "_download", side_effect=fake_download):
            res = qa.run(
                "qa:",
                cfg_path,
                state_dir,
                limit=16,
                since_date="2026-11-21",
                since_window="2026-11-21T08:00Z",
                now_epoch_s=w0 + 30 * 60,  # 08:30Z -> 08:00Z window is 15m past end (> 10m SLA!)
                sla_grace_minutes=10.0,
            )
        assert res["eligible_manifest_count"] == 0
        assert res["attempted"] == 0
        assert res["qa_status"] == qa.QA_STATUS_OVERDUE
        assert res["status"] == "blocked"
        assert "2026-11-21T08:00Z" in res["overdue_windows"]


def test_qa_injected_single_stream_outage_local_loss_cross_window_seq_break_and_download_retry():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        archive_root = root / "data"
        state_dir = root / "state"
        w0 = int(datetime(2026, 11, 21, 0, 0, tzinfo=timezone.utc).timestamp())
        w1 = w0 + 15 * 60
        cfg = _make_config(partition_minutes=15, exchanges=("binance_futures",), stale_seconds=900)
        cfg_path = root / "config.json"
        cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

        # W0: healthy (seq 1..6), W1: cross-window seq break (starts at 20 instead of 7) + missing depth + LOCAL_LOSS!
        _write_sealed_partition(
            archive_root, exchange="binance_futures", window_start_s=w0, partition_minutes=15, boot_id="b_same", first_rx_seq=1
        )
        _write_sealed_partition(
            archive_root,
            exchange="binance_futures",
            window_start_s=w1,
            partition_minutes=15,
            boot_id="b_same",
            first_rx_seq=20,  # break! expected 7
            include_depth=False,  # single-stream depth@100ms outage!
            extra_meta=[{"event": "LOCAL_LOSS", "dropped_frames": 4}],
        )

        backend = _MemoryArchiveBackend()
        arch.sync_sealed_directory(archive_root, backend, verify_remote=True, now_epoch_s=w1 + 16 * 60, upload_ledger_snapshot=False)
        fake_run, base_download = _wire_mock_drive(archive_root, backend)

        # Inject 2 transient timeouts on first data download, succeeding on attempt #3!
        flaky_attempts: dict[str, int] = {}
        sleeps: list[float] = []

        def flaky_download(remote: str, rel_path: str, target: Path) -> None:
            if rel_path.endswith(".tsv.zst"):
                flaky_attempts[rel_path] = flaky_attempts.get(rel_path, 0) + 1
                if flaky_attempts[rel_path] <= 2:
                    raise subprocess.TimeoutExpired(cmd=["rclone", "copyto"], timeout=1800)
            base_download(remote, rel_path, target)

        with patch.object(qa, "_run", side_effect=fake_run), \
             patch.object(qa, "_download", side_effect=flaky_download):
            res = qa.run(
                "qa:",
                cfg_path,
                state_dir,
                limit=16,
                since_date="2026-11-21",
                since_window="2026-11-21T00:00Z",
                now_epoch_s=w1 + 16 * 60,
                download_retries=3,
                sleeper=sleeps.append,
            )

        # Retries succeeded without permanent download error
        assert res["retry_needed"] == 0
        assert len(sleeps) >= 2
        # Injected faults caught!
        assert res["qa_status"] == qa.QA_STATUS_FAILED
        assert res["status"] == "blocked"
        assert res["ok"] == 1 and res["blocked"] == 1
        assert "2026-11-21T00:15Z" in res["failed_windows"]

        # Check W1 window report & Day report for exact injected alerts
        w1_rep = json.loads(
            (state_dir / "qa_reports" / "windows" / "2026-11-21" / "2026-11-21T0015Z.window.qa.json").read_text()
        )
        assert w1_rep["qa_status"] == qa.QA_STATUS_FAILED
        assert "binance_futures/BTCUSDT/depth@100ms" in w1_rep["missing_hard_channels"]
        assert w1_rep["local_loss_events"] == 1
        assert w1_rep["dropped_frames"] == 4

        day_rep = json.loads((state_dir / "qa_reports" / "days" / "2026-11-21.day.qa.json").read_text())
        assert day_rep["qa_status"] == qa.QA_STATUS_FAILED
        assert any("expected_first_rx_seq=7,got=20" in s for s in day_rep["cross_window_sequence_issues"])


def test_qa_needs_review_on_quiet_event_trade_and_mixed_format_coverage():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        w0 = int(datetime(2026, 11, 21, 0, 0, tzinfo=timezone.utc).timestamp())
        # Two symbols so total expected streams = 6 (> 3) -> missing event_trade yields NEEDS_REVIEW, not FAILED
        cfg = _make_config(
            partition_minutes=15,
            exchanges=("binance_futures",),
            symbols=["BTCUSDT", "ETHUSDT"],
            stale_seconds=900,
        )
        start_ns = w0 * 1_000_000_000
        rows = []
        seq = 1
        mono = 1000
        # BTCUSDT has all 3 channels; ETHUSDT has bookTicker + depth@100ms, ZERO aggTrade!
        for t_off in (1, 898):
            t_ns = start_ns + t_off * 1_000_000_000
            for payload in (
                {"stream": "btcusdt@bookTicker", "data": {"e": "bookTicker", "s": "BTCUSDT"}},
                {"stream": "btcusdt@depth@100ms", "data": {"e": "depthUpdate", "s": "BTCUSDT"}},
                {"stream": "btcusdt@aggTrade", "data": {"e": "aggTrade", "s": "BTCUSDT"}},
                {"stream": "ethusdt@bookTicker", "data": {"e": "bookTicker", "s": "ETHUSDT"}},
                {"stream": "ethusdt@depth@100ms", "data": {"e": "depthUpdate", "s": "ETHUSDT"}},
            ):
                rows.append((t_ns + seq * 1000, mono, "A", 1, seq, "DATA", payload))
                seq += 1
                mono += 50
        d_path, _, mf = _write_sealed_partition(
            root, exchange="binance_futures", window_start_s=w0, partition_minutes=15, custom_rows=rows, boot_id="quiet_tr"
        )
        res = qa.check_partition(d_path, mf, cfg)
        assert res["qa_status"] == qa.QA_STATUS_NEEDS_REVIEW
        assert res["problems"] == []
        assert any("ethusdt" in item.lower() or "aggTrade" in item for item in res["review_items"])

        # Mixed legacy hourly + 15m filename parser verification
        p_legacy = dc.parse_partition_window_from_rel_path(
            "raw/binance_futures/2026-11-21/binance_futures_20261121_05.manifest.json"
        )
        p_15m = dc.parse_partition_window_from_rel_path(
            "raw/binance_futures/2026-11-21/binance_futures_20261121_0515_p15m_trial_15m_a1b2c3d4_s2.manifest.json"
        )
        assert p_legacy is not None and p_legacy["partition_minutes"] == 60 and p_legacy["window_id"] == "2026-11-21T05:00Z"
        assert p_15m is not None and p_15m["partition_minutes"] == 15 and p_15m["window_id"] == "2026-11-21T05:15Z"


# ---------------------------------------------------------------------------
# 5. User Review Reproductions (4 P1 + 1 P2)
# ---------------------------------------------------------------------------


def test_review_p1_1_window_qa_and_archive_isolate_batch_and_duration():
    """Reproduces P1 #1:
    (a) 60-minute archive starting at 2026-11-21T00:00Z must NOT satisfy a 15-minute window [00:00Z, 00:15Z).
    (b) Two partial batches (`batch_1` with 2 exchanges, `batch_2` with the other 2 exchanges) must NOT
        combine to pass a 15-minute window in either archive reconciliation or incremental QA.
    """
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        w0 = int(datetime(2026, 11, 21, 0, 0, tzinfo=timezone.utc).timestamp())
        cfg_15m = _make_config(
            partition_minutes=15,
            exchanges=EXCHANGES_4,
            symbols=["BTCUSDT", "ETHUSDT", "XRPUSDT", "SOLUSDT", "DOGEUSDT", "SUIUSDT"],
            stale_seconds=900,
        )
        cfg_path = root / "config_15m.json"
        cfg_path.write_text(json.dumps(cfg_15m), encoding="utf-8")

        # Sample A: 60-minute archive starting at 00:00Z evaluated under 15-minute config
        dir_60m = root / "case_60m_in_15m"
        for ex in EXCHANGES_4:
            d60, _, mf60 = _write_sealed_partition(
                dir_60m,
                exchange=ex,
                window_start_s=w0,
                partition_minutes=60,
                legacy_v1=True,
                boot_id="legacy_60m",
            )
            # Single-file check_partition under 15m config must fail duration mismatch
            single_res = qa.check_partition(d60, mf60, cfg_15m)
            assert single_res["qa_status"] == qa.QA_STATUS_FAILED
            assert single_res["status"] == "blocked"
            assert any("partition duration mismatch" in p for p in single_res["problems"])

        backend_60m = _MemoryArchiveBackend()
        # Archive under 15m config must NOT allow 60m partitions to satisfy the 15m [00:00Z, 00:15Z) window
        arch_60m = arch.sync_sealed_directory(
            dir_60m,
            backend_60m,
            verify_remote=True,
            now_epoch_s=w0 + 16 * 60,
            upload_ledger_snapshot=False,
            expected_hourly_exchanges=set(EXCHANGES_4),
            partition_minutes=15,
        )
        assert arch_60m["status"] == "blocked"
        assert sorted(arch_60m["expected_window_reconciliation"]["missing_exchanges"]) == sorted(EXCHANGES_4)

        fake_run_60, fake_dl_60 = _wire_mock_drive(dir_60m, backend_60m)
        with patch.object(qa, "_run", side_effect=fake_run_60), \
             patch.object(qa, "_download", side_effect=fake_dl_60):
            qa_60m = qa.run(
                "qa:",
                cfg_path,
                root / "state_60m",
                limit=16,
                since_date="2026-11-21",
                since_window="2026-11-21T00:00Z",
                now_epoch_s=w0 + 30 * 60,
                sla_grace_minutes=10.0,
            )
        assert qa_60m["eligible_manifest_count"] == 0
        assert qa_60m["qa_status"] != qa.QA_STATUS_PASSED
        assert qa_60m["status"] == "blocked"

        # Sample B: Two partial batches (`batch_1` has binance+bybit; `batch_2` has bitget+gate)
        dir_mixed = root / "case_mixed_batches"
        for ex in ("binance_futures", "bybit_linear"):
            _write_sealed_partition(
                dir_mixed, exchange=ex, window_start_s=w0, partition_minutes=15, boot_id="b1", run_id="batch_1"
            )
        for ex in ("bitget_v3", "gate_futures"):
            _write_sealed_partition(
                dir_mixed, exchange=ex, window_start_s=w0, partition_minutes=15, boot_id="b2", run_id="batch_2"
            )

        backend_mixed = _MemoryArchiveBackend()
        arch_mixed = arch.sync_sealed_directory(
            dir_mixed,
            backend_mixed,
            verify_remote=True,
            now_epoch_s=w0 + 16 * 60,
            upload_ledger_snapshot=False,
            expected_hourly_exchanges=set(EXCHANGES_4),
            partition_minutes=15,
        )
        assert arch_mixed["status"] == "blocked"
        assert any("mixed_batches_in_expected_window:" in f for f in arch_mixed["failed_items"])

        fake_run_mix, fake_dl_mix = _wire_mock_drive(dir_mixed, backend_mixed)
        with patch.object(qa, "_run", side_effect=fake_run_mix), \
             patch.object(qa, "_download", side_effect=fake_dl_mix):
            # Even if run without --run-id, mixed batches in the same window MUST fail and never union to 4 exchanges!
            qa_mixed_unfiltered = qa.run(
                "qa:",
                cfg_path,
                root / "state_mixed_unfiltered",
                limit=16,
                since_date="2026-11-21",
                since_window="2026-11-21T00:00Z",
                now_epoch_s=w0 + 16 * 60,
                sla_grace_minutes=10.0,
            )
            # And when bound to --run-id batch_1, it reports missing bitget_v3 and gate_futures!
            qa_mixed_bound = qa.run(
                "qa:",
                cfg_path,
                root / "state_mixed_bound",
                limit=16,
                since_date="2026-11-21",
                since_window="2026-11-21T00:00Z",
                run_id="batch_1",
                now_epoch_s=w0 + 16 * 60,
                sla_grace_minutes=10.0,
            )
        assert qa_mixed_unfiltered["qa_status"] == qa.QA_STATUS_FAILED
        assert qa_mixed_unfiltered["status"] == "blocked"
        assert qa_mixed_bound["qa_status"] == qa.QA_STATUS_FAILED
        assert qa_mixed_bound["status"] == "blocked"
        bound_win = json.loads(
            (root / "state_mixed_bound" / "qa_reports" / "windows" / "2026-11-21" / "2026-11-21T0000Z.window.qa.json").read_text()
        )
        assert bound_win["missing_exchanges"] == ["bitget_v3", "gate_futures"]


def test_review_p1_2_late_frame_rejected_counts_as_loss_and_blocks_target_window():
    """Reproduces P1 #2:
    A compressed sample containing 1 LATE_FRAME_REJECTED record must NOT return passed
    with local_loss_events=0; it must count as data loss, block the partition, and attribute
    the loss back to the target window `target_window_id` where the late frame originally belonged.
    """
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        archive_root = root / "data"
        state_dir = root / "state"
        w0 = int(datetime(2026, 11, 21, 0, 0, tzinfo=timezone.utc).timestamp())
        w1 = w0 + 15 * 60
        cfg = _make_config(partition_minutes=15, exchanges=("binance_futures",), stale_seconds=900)
        cfg_path = root / "config.json"
        cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

        # Window W0 (00:00Z): initially clean on its own
        _write_sealed_partition(
            archive_root,
            exchange="binance_futures",
            window_start_s=w0,
            partition_minutes=15,
            boot_id="b_late",
            run_id="batch_late",
            first_rx_seq=1,
        )
        # Window W1 (00:15Z): contains 1 LATE_FRAME_REJECTED whose original timestamp belonged to W0 (00:00Z)!
        d_w1, mf_w1_path, mf_w1 = _write_sealed_partition(
            archive_root,
            exchange="binance_futures",
            window_start_s=w1,
            partition_minutes=15,
            boot_id="b_late",
            run_id="batch_late",
            first_rx_seq=7,
            extra_meta=[
                {
                    "event": "LATE_FRAME_REJECTED",
                    "dropped_frames": 1,
                    "orig_recv_wall_ns": w1 * 1_000_000_000 - 10_000_000,
                    "orig_recv_mono_ns": 550,
                    "orig_rx_seq": 6,
                    "orig_kind": "DATA",
                    "target_window_id": "2026-11-21T00:00Z",
                    "loss_attributed_window_id": "2026-11-21T00:00Z",
                    "target_planned_start_wall_ns": w0 * 1_000_000_000,
                    "target_planned_end_wall_ns": w1 * 1_000_000_000,
                    "target_partition_minutes": 15,
                }
            ],
        )
        mf_w1["late_frames_rejected"] = 1
        mf_w1_path.write_text(json.dumps(mf_w1, indent=2) + "\n", encoding="utf-8")

        # 1. Single-file check_partition on W1 MUST fail with local_loss_events=1, dropped_frames=1, late_frames_rejected=1
        single_w1 = qa.check_partition(d_w1, mf_w1, cfg)
        assert single_w1["qa_status"] == qa.QA_STATUS_FAILED
        assert single_w1["status"] == "blocked"
        assert single_w1["late_frames_rejected"] == 1
        assert single_w1["local_loss_events"] == 1
        assert single_w1["dropped_frames"] == 1
        assert single_w1["late_loss_by_target_window"] == {"2026-11-21T00:00Z": 1}

        # 2. Full QA run across W0 and W1 attributes the rejected late frame back to W0 (00:00Z),
        #    blocking BOTH W0 (where the data was lost) and W1 (where the rejection occurred)!
        backend = _MemoryArchiveBackend()
        arch.sync_sealed_directory(archive_root, backend, verify_remote=True, now_epoch_s=w1 + 16 * 60, upload_ledger_snapshot=False)
        fake_run, fake_dl = _wire_mock_drive(archive_root, backend)
        with patch.object(qa, "_run", side_effect=fake_run), \
             patch.object(qa, "_download", side_effect=fake_dl):
            res = qa.run(
                "qa:",
                cfg_path,
                state_dir,
                limit=16,
                since_date="2026-11-21",
                since_window="2026-11-21T00:00Z",
                run_id="batch_late",
                now_epoch_s=w1 + 16 * 60,
            )
        assert res["qa_status"] == qa.QA_STATUS_FAILED
        assert res["status"] == "blocked"
        assert "2026-11-21T00:00Z" in res["failed_windows"]
        assert "2026-11-21T00:15Z" in res["failed_windows"]

        w0_win_rep = json.loads(
            (state_dir / "qa_reports" / "windows" / "2026-11-21" / "2026-11-21T0000Z.window.qa.json").read_text()
        )
        assert w0_win_rep["qa_status"] == qa.QA_STATUS_FAILED
        assert w0_win_rep["attributed_late_frames_rejected"] == 1
        assert w0_win_rep["local_loss_events"] == 1
        assert w0_win_rep["dropped_frames"] == 1


def test_review_p1_3_archive_separates_file_verification_from_window_coverage_and_tracks_zero_file_backlog():
    """Reproduces P1 #3:
    (a) Having 1 verified file with >0 lines per exchange must NOT pass archive window reconciliation
        when the exchange has a head, tail, or internal middle gap.
    (b) An expected window with 0 files on disk on Run 1 must still be persisted in SQLite `expected_windows`
        and tracked in `pending_prior_windows` on Run 2 even after the next window succeeds.
    """
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        w0 = int(datetime(2026, 11, 21, 0, 0, tzinfo=timezone.utc).timestamp())
        w1 = w0 + 15 * 60
        backend = _MemoryArchiveBackend()

        # Run 1 at 00:16Z (expected W0 = 00:00Z): ZERO files on disk for W0!
        r_empty = arch.sync_sealed_directory(
            root,
            backend,
            verify_remote=True,
            now_epoch_s=w0 + 16 * 60,
            upload_ledger_snapshot=False,
            expected_hourly_exchanges=set(EXCHANGES_4),
            partition_minutes=15,
        )
        assert r_empty["status"] == "blocked"
        assert r_empty["expected_window_reconciliation"]["window_coverage_status"] == "missing"

        # Run 2 at 00:31Z (expected W1 = 00:15Z): W1 has all 4 exchanges with full coverage,
        # while W0 STILL has 0 files on disk -> W0 MUST remain in pending_prior_windows and block status="ok"!
        for ex in EXCHANGES_4:
            _write_sealed_partition(root, exchange=ex, window_start_s=w1, partition_minutes=15, boot_id="b1", dense=True)

        r_w1 = arch.sync_sealed_directory(
            root,
            backend,
            verify_remote=True,
            now_epoch_s=w1 + 16 * 60,
            upload_ledger_snapshot=False,
            expected_hourly_exchanges=set(EXCHANGES_4),
            partition_minutes=15,
        )
        assert r_w1["status"] == "blocked"
        rec_w1 = r_w1["expected_window_reconciliation"]
        assert rec_w1["expected_window_id"] == "2026-11-21T00:15Z"
        assert rec_w1["file_verification_status"] == "verified"
        assert rec_w1["window_coverage_status"] == "complete"
        assert "2026-11-21T00:00Z" in rec_w1["pending_prior_windows"]
        assert any("pending_prior_window_unverified:2026-11-21T00:00Z" in f for f in r_w1["failed_items"])

        # Run 3: Backfill W0 for all 4 exchanges, BUT gate_futures only covers seconds [1..60] of the 900s window
        # (leaving an 840s tail gap > max_coverage_gap_seconds)!
        for ex in ("binance_futures", "bybit_linear", "bitget_v3"):
            _write_sealed_partition(root, exchange=ex, window_start_s=w0, partition_minutes=15, boot_id="b0", dense=True)
        _write_sealed_partition(
            root,
            exchange="gate_futures",
            window_start_s=w0,
            partition_minutes=15,
            boot_id="b0",
            offset_start_s=1,
            offset_end_s=60,  # 840s tail gap!, dense=True
        )

        r_gap = arch.sync_sealed_directory(
            root,
            backend,
            verify_remote=True,
            now_epoch_s=w0 + 16 * 60,
            upload_ledger_snapshot=False,
            expected_hourly_exchanges=set(EXCHANGES_4),
            partition_minutes=15,
        )
        assert r_gap["status"] == "blocked"
        rec_gap = r_gap["expected_window_reconciliation"]
        # Files themselves ARE verified, but whole-window coverage is INCOMPLETE!
        assert rec_gap["file_verification_status"] == "verified"
        assert rec_gap["window_coverage_status"] == "incomplete"
        assert rec_gap["incomplete_coverage_exchanges"] == ["gate_futures"]
        assert any(
            "expected_window_coverage_incomplete:gate_futures:2026-11-21T00:00Z:tail_gap_" in f
            for f in r_gap["failed_items"]
        )


def test_review_p1_4_qa_never_bypasses_verified_index_and_enforces_three_way_verification():
    """Reproduces P1 #4:
    (a) When `meta/archive_manifest_index.json` is unavailable/corrupted, formal QA must NOT
        fall back to scanning unverified `raw/`; it must report `verified_index_unavailable` and block.
    (b) After download, QA must verify 3-way hash consistency across Index <-> Manifest <-> Data File.
    """
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        archive_root = root / "data"
        w0 = int(datetime(2026, 11, 21, 0, 0, tzinfo=timezone.utc).timestamp())
        cfg = _make_config(partition_minutes=15, exchanges=("binance_futures",), stale_seconds=900)
        cfg_path = root / "config.json"
        cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

        _write_sealed_partition(
            archive_root, exchange="binance_futures", window_start_s=w0, partition_minutes=15, boot_id="b_idx", run_id="batch_idx"
        )
        backend = _MemoryArchiveBackend()
        arch.sync_sealed_directory(archive_root, backend, verify_remote=True, now_epoch_s=w0 + 16 * 60, upload_ledger_snapshot=False)

        # Case A: Remove verified index while keeping unverified files under raw/
        saved_index = backend.objects.pop(arch.ARCHIVE_MANIFEST_INDEX_RELPATH)
        backend.objects.pop("meta/verified_archive_ledger.compat.jsonl", None)
        fake_run, fake_dl = _wire_mock_drive(archive_root, backend)
        with patch.object(qa, "_run", side_effect=fake_run), \
             patch.object(qa, "_download", side_effect=fake_dl):
            res_no_idx = qa.run(
                "qa:",
                cfg_path,
                root / "state_no_idx",
                limit=16,
                since_date="2026-11-21",
                since_window="2026-11-21T00:00Z",
                run_id="batch_idx",
                now_epoch_s=w0 + 16 * 60,
            )
        assert res_no_idx["discovery_mode"] == "verified_index_unavailable"
        assert res_no_idx["attempted"] == 0
        assert res_no_idx["job_execution_status"] == "failed"
        assert res_no_idx["status"] == "blocked"

        # Case B: Restore index, but tamper with index `sha256` (data_sha256) so Manifest == Data != Index
        idx_tampered = json.loads(saved_index.decode("utf-8"))
        idx_tampered["entries"][0]["sha256"] = "e" * 64
        backend.objects[arch.ARCHIVE_MANIFEST_INDEX_RELPATH] = json.dumps(idx_tampered).encode("utf-8")
        with patch.object(qa, "_run", side_effect=fake_run), \
             patch.object(qa, "_download", side_effect=fake_dl):
            res_3way = qa.run(
                "qa:",
                cfg_path,
                root / "state_3way",
                limit=16,
                since_date="2026-11-21",
                since_window="2026-11-21T00:00Z",
                run_id="batch_idx",
                now_epoch_s=w0 + 16 * 60,
            )
        assert res_3way["attempted"] == 1
        assert res_3way["blocked"] == 1
        assert res_3way["qa_status"] == qa.QA_STATUS_FAILED
        assert res_3way["status"] == "blocked"


def test_review_p2_5_qa_heartbeat_separates_job_current_window_and_historical_damage():
    """Reproduces P2 #5:
    (a) Window W0 (00:00Z) fails with LOCAL_LOSS on Run 1 -> status="blocked", historical damage recorded.
    (b) Window W1 (00:15Z) is in `pending_archive` on Run 2 (0 new files checked) ->
        `current_window_completed` is False, `current_window_verification_status == "pending_archive"`,
        and `status == "blocked"` (never false-positive success when current window is unverified!).
    (c) Window W1 (00:15Z) finishes archiving all healthy exchanges on Run 3 ->
        `job_execution_status == "ok"`, `current_window_completed is True`,
        `current_window_verification_status == "passed"`, and `status == "ok"` (so the current-window
        heartbeat recovers), while `historical_data_quality_status == "has_historical_damage"` and
        `historical_damaged_windows == ["2026-11-21T00:00Z"]` preserves W0's damage record!
    """
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        archive_root = root / "data"
        state_dir = root / "state"
        w0 = int(datetime(2026, 11, 21, 0, 0, tzinfo=timezone.utc).timestamp())
        w1 = w0 + 15 * 60
        cfg = _make_config(partition_minutes=15, exchanges=("binance_futures",), stale_seconds=900)
        cfg_path = root / "config.json"
        cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

        # W0 has a LOCAL_LOSS marker
        _write_sealed_partition(
            archive_root,
            exchange="binance_futures",
            window_start_s=w0,
            partition_minutes=15,
            boot_id="b_p2",
            run_id="batch_p2",
            first_rx_seq=1,
            extra_meta=[{"event": "LOCAL_LOSS", "dropped_frames": 3}],
        )
        backend = _MemoryArchiveBackend()
        arch.sync_sealed_directory(archive_root, backend, verify_remote=True, now_epoch_s=w0 + 16 * 60, upload_ledger_snapshot=False)
        fake_run, fake_dl = _wire_mock_drive(archive_root, backend)

        # Run 1 at 00:16Z (current expected window = W0 00:00Z): W0 fails!
        with patch.object(qa, "_run", side_effect=fake_run), \
             patch.object(qa, "_download", side_effect=fake_dl):
            r1 = qa.run(
                "qa:",
                cfg_path,
                state_dir,
                limit=16,
                since_date="2026-11-21",
                since_window="2026-11-21T00:00Z",
                run_id="batch_p2",
                now_epoch_s=w0 + 16 * 60,
                sla_grace_minutes=10.0,
            )
        assert r1["job_execution_status"] == "ok"
        assert r1["current_window_id"] == "2026-11-21T00:00Z"
        assert r1["current_window_completed"] is True
        assert r1["current_window_verification_status"] == qa.QA_STATUS_FAILED
        assert r1["status"] == "blocked"

        # Run 2 at 00:32Z (current expected window = W1 00:15Z, still within 10m archive grace):
        # 0 files for W1 yet -> pending_archive -> MUST NOT report status="ok"!
        with patch.object(qa, "_run", side_effect=fake_run), \
             patch.object(qa, "_download", side_effect=fake_dl):
            r2 = qa.run(
                "qa:",
                cfg_path,
                state_dir,
                limit=16,
                since_date="2026-11-21",
                since_window="2026-11-21T00:00Z",
                run_id="batch_p2",
                now_epoch_s=w1 + 17 * 60,
                sla_grace_minutes=10.0,
            )
        assert r2["job_execution_status"] == "ok"
        assert r2["current_window_id"] == "2026-11-21T00:15Z"
        assert r2["current_window_completed"] is False
        assert r2["current_window_verification_status"] == qa.QA_STATUS_PENDING_ARCHIVE
        assert r2["historical_data_quality_status"] == "has_historical_damage"
        assert r2["historical_damaged_windows"] == ["2026-11-21T00:00Z"]
        assert r2["status"] == "blocked"

        # Run 3 at 00:34Z: W1 (00:15Z) is archived and passes QA!
        _write_sealed_partition(
            archive_root,
            exchange="binance_futures",
            window_start_s=w1,
            partition_minutes=15,
            boot_id="b_p2",
            run_id="batch_p2",
            first_rx_seq=8,
        )
        arch.sync_sealed_directory(archive_root, backend, verify_remote=True, now_epoch_s=w1 + 19 * 60, upload_ledger_snapshot=False)
        with patch.object(qa, "_run", side_effect=fake_run), \
             patch.object(qa, "_download", side_effect=fake_dl):
            r3 = qa.run(
                "qa:",
                cfg_path,
                state_dir,
                limit=16,
                since_date="2026-11-21",
                since_window="2026-11-21T00:00Z",
                run_id="batch_p2",
                now_epoch_s=w1 + 19 * 60,
                sla_grace_minutes=10.0,
            )
        assert r3["job_execution_status"] == "ok"
        assert r3["current_window_id"] == "2026-11-21T00:15Z"
        assert r3["current_window_completed"] is True
        assert r3["current_window_verification_status"] == qa.QA_STATUS_PASSED
        assert r3["status"] == "ok"
        assert r3["historical_data_quality_status"] == "has_historical_damage"
        assert r3["historical_damaged_windows"] == ["2026-11-21T00:00Z"]
        assert len(r3["historical_damaged_partitions"]) == 1


# ---------------------------------------------------------------------------
# 6. User Review Round 2 Reproductions (3 P1 + 2 Report Consistency Fixes)
# ---------------------------------------------------------------------------


def test_review_round2_p1_1_unified_run_id_across_collector_archive_and_qa():
    """Reproduces Review Round 2 P1 #1:
    Verify that collector, archive service, and GitHub incremental QA all resolve
    `effective_run_id` from the same unified sources (`--run-id`, `TOKYO_L2_RUN_ID`,
    and `deployment.run_id` in `config.json`), fail closed on any conflict, and verify
    the actual `run_id` across archive manifests and `meta/archive_manifest_index.json`.
    """
    repo_root = Path(__file__).resolve().parents[1]
    collector_unit = (repo_root / "deploy" / "systemd" / "tokyo-l2-collector.service").read_text(encoding="utf-8")
    archive_unit = (repo_root / "deploy" / "systemd" / "tokyo-l2-archive.service").read_text(encoding="utf-8")
    assert "/etc/tokyo-l2/live.env" in collector_unit
    assert "/etc/tokyo-l2/live.env" in archive_unit
    assert "--config /etc/tokyo-l2/config.json" in collector_unit
    assert "--config /etc/tokyo-l2/config.json" in archive_unit
    assert "--run-id ${TOKYO_L2_RUN_ID}" in collector_unit
    assert "--run-id ${TOKYO_L2_RUN_ID}" in archive_unit

    cfg = _make_config(partition_minutes=15, exchanges=("binance_futures",), stale_seconds=900)
    cfg["deployment"]["run_id"] = "trial_15m_unified_01"
    cfg["deployment"]["cutover_window_utc"] = "2026-11-21T00:00Z"

    # 1. Unexpanded systemd literal `${TOKYO_L2_RUN_ID}` falls back to config `deployment.run_id`
    assert (
        dc.resolve_effective_run_id(cfg, "${TOKYO_L2_RUN_ID}", env={"TOKYO_L2_RUN_ID": ""})
        == "trial_15m_unified_01"
    )
    # 2. Matching env + config + CLI resolves cleanly
    assert (
        dc.resolve_effective_run_id(
            cfg,
            "trial_15m_unified_01",
            env={"TOKYO_L2_RUN_ID": "trial_15m_unified_01"},
        )
        == "trial_15m_unified_01"
    )
    # 3. Conflict between `live.env` TOKYO_L2_RUN_ID and `config.json` deployment.run_id MUST fail closed!
    with pytest.raises(ValueError, match="conflicting run_id"):
        dc.resolve_effective_run_id(
            cfg,
            None,
            env={"TOKYO_L2_RUN_ID": "old_60m_boot_id"},
        )

    # 4. End-to-end verification across manifest -> archive index -> QA run using config's deployment.run_id
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        archive_root = root / "data"
        state_dir = root / "state"
        cfg_path = root / "config.json"
        cfg_path.write_text(json.dumps(cfg), encoding="utf-8")
        w0 = int(datetime(2026, 11, 21, 0, 0, tzinfo=timezone.utc).timestamp())

        _write_sealed_partition(
            archive_root,
            exchange="binance_futures",
            window_start_s=w0,
            partition_minutes=15,
            boot_id="b_unified",
            run_id="trial_15m_unified_01",
            dense=True,
        )
        backend = _MemoryArchiveBackend()
        sync_res = arch.sync_sealed_directory(
            archive_root,
            backend,
            verify_remote=True,
            now_epoch_s=w0 + 16 * 60,
            upload_ledger_snapshot=False,
            expected_hourly_exchanges={"binance_futures"},
            partition_minutes=15,
            expected_run_id=dc.resolve_effective_run_id(cfg, None, env={}),
            cutover_window_utc=dc.resolve_cutover_window_utc(cfg, None, env={}),
        )
        assert sync_res["status"] == "ok"
        assert sync_res["effective_run_id"] == "trial_15m_unified_01"
        assert sync_res["expected_window_reconciliation"]["expected_run_id"] == "trial_15m_unified_01"

        idx_obj = json.loads(backend.objects[arch.ARCHIVE_MANIFEST_INDEX_RELPATH].decode("utf-8"))
        assert idx_obj["effective_run_id"] == "trial_15m_unified_01"
        assert idx_obj["entries"][0]["collection_run_id"] == "trial_15m_unified_01"

        # QA run with run_id=None and since_window=None automatically resolves both from config.json!
        fake_run, fake_dl = _wire_mock_drive(archive_root, backend)
        with patch.object(qa, "_run", side_effect=fake_run), \
             patch.object(qa, "_download", side_effect=fake_dl):
            qa_res = qa.run(
                "qa:",
                cfg_path,
                state_dir,
                limit=16,
                since_date="2026-11-21",
                now_epoch_s=w0 + 16 * 60,
            )
        assert qa_res["status"] == "ok"
        assert qa_res["effective_run_id"] == "trial_15m_unified_01"
        assert qa_res["index_effective_run_id"] == "trial_15m_unified_01"
        assert qa_res["effective_since_window"] == "2026-11-21T00:00Z"


def test_review_round2_p1_2_archive_market_data_only_coverage_cross_process_overlap_and_startup_stub_progression():
    """Reproduces all 3 user cases in Review Round 2 P1 #2:
    (a) 1 real market-data record (`DATA`) stretched across the window by head and tail `META` control
        records must NOT return COMPLETE; it must return `window_coverage_status == "incomplete"`,
        `window_status == "PENDING"`, and outer `status == "blocked"`.
    (b) Overlapping segments from different processes (`boot_id_1 != boot_id_2`, `wall_overlap=True`)
        must populate `errors` with `segment_wall_overlap_in_expected_window:...` and force outer
        `sync_sealed_directory["status"] == "blocked"`.
    (c) An initial mid-window startup stub in `W0` must be single-listed in `startup_stub_windows`
        (`window_status == "STARTUP_STUB"`), and full-window acceptance must start from `first_full_window_id`
        (`W1`) so subsequent complete windows return `status == "ok"` instead of being permanently blocked.
    """
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        w0 = int(datetime(2026, 11, 21, 0, 0, tzinfo=timezone.utc).timestamp())
        w1 = w0 + 15 * 60
        start_ns = w0 * 1_000_000_000

        # Case (a): 1 real DATA record at +450s, flanked by head META (+1s) and tail META (+899s)
        dir_meta_stretch = root / "case_meta_stretch"
        rows_meta_stretch = [
            (start_ns + 1_000_000_000, 100, "A", 1, 1, "META", {"event": "HEAD_CONTROL"}),
            (
                start_ns + 450_000_000_000,
                200,
                "A",
                1,
                2,
                "DATA",
                {"stream": "btcusdt@bookTicker", "data": {"e": "bookTicker", "s": "BTCUSDT"}},
            ),
            (start_ns + 899_000_000_000, 300, "A", 1, 3, "META", {"event": "TAIL_CONTROL"}),
        ]
        _, mf_path_a, mf_a = _write_sealed_partition(
            dir_meta_stretch,
            exchange="binance_futures",
            window_start_s=w0,
            partition_minutes=15,
            custom_rows=rows_meta_stretch,
            boot_id="b_meta",
        )
        # Even if manifest start_wall_ns/end_wall_ns claimed [w0+1s, w0+899s] and data_lines=3,
        # archive_sync scans real DATA/DATA_ESCAPED rows from the compressed file and catches it!
        mf_a["start_wall_ns"] = start_ns + 1_000_000_000
        mf_a["end_wall_ns"] = start_ns + 899_000_000_000
        mf_a["data_lines"] = 1
        mf_a["meta_lines"] = 2
        mf_path_a.write_text(json.dumps(mf_a, indent=2) + "\n", encoding="utf-8")

        res_a = arch.sync_sealed_directory(
            dir_meta_stretch,
            _MemoryArchiveBackend(),
            verify_remote=True,
            now_epoch_s=w0 + 16 * 60,
            upload_ledger_snapshot=False,
            expected_hourly_exchanges={"binance_futures"},
            partition_minutes=15,
        )
        assert res_a["status"] == "blocked"
        rec_a = res_a["expected_window_reconciliation"]
        assert rec_a["window_status"] == "PENDING"
        assert rec_a["window_coverage_status"] == "incomplete"
        assert "insufficient_market_data_records" in rec_a["coverage_gaps"]["binance_futures"]["reasons"]

        # Case (b): Two segments from DIFFERENT processes (`boot_p1` != `boot_p2`) with overlapping wall timestamps
        dir_overlap = root / "case_cross_process_overlap"
        _write_sealed_partition(
            dir_overlap,
            exchange="binance_futures",
            window_start_s=w0,
            partition_minutes=15,
            boot_id="boot_p1",
            first_rx_seq=1,
            offset_start_s=1,
            offset_end_s=600,
        )
        _write_sealed_partition(
            dir_overlap,
            exchange="binance_futures",
            window_start_s=w0,
            partition_minutes=15,
            boot_id="boot_p2",
            first_rx_seq=1,
            offset_start_s=400,  # overlaps [400s, 600s] with boot_p1!
            offset_end_s=898,
        )
        res_b = arch.sync_sealed_directory(
            dir_overlap,
            _MemoryArchiveBackend(),
            verify_remote=True,
            now_epoch_s=w0 + 16 * 60,
            upload_ledger_snapshot=False,
            expected_hourly_exchanges={"binance_futures"},
            partition_minutes=15,
        )
        assert res_b["status"] == "blocked"
        rec_b = res_b["expected_window_reconciliation"]
        assert rec_b["window_status"] == "PENDING"
        assert any("segment_wall_overlap_in_expected_window:binance_futures:2026-11-21T00:00Z" in e for e in rec_b["errors"])
        assert any("segment_wall_overlap_in_expected_window:binance_futures:2026-11-21T00:00Z" in e for e in res_b["failed_items"])

        # Case (c): Initial startup stub in W0 (started 600s into W0), followed by complete full window W1
        dir_stub = root / "case_startup_stub"
        backend_stub = _MemoryArchiveBackend()
        for ex in EXCHANGES_4:
            _write_sealed_partition(
                dir_stub,
                exchange=ex,
                window_start_s=w0,
                partition_minutes=15,
                boot_id="boot_init",
                run_id="batch_init",
                offset_start_s=600,
                offset_end_s=898,
                is_startup_stub=True,
            )
        # At 00:16Z (only W0 startup stub exists so far): W0 is single-listed as STARTUP_STUB, outer status is blocked
        # because no full window has completed yet
        res_c0 = arch.sync_sealed_directory(
            dir_stub,
            backend_stub,
            verify_remote=True,
            now_epoch_s=w0 + 16 * 60,
            upload_ledger_snapshot=False,
            expected_hourly_exchanges=set(EXCHANGES_4),
            partition_minutes=15,
            expected_run_id="batch_init",
        )
        assert res_c0["status"] == "blocked"
        assert res_c0["expected_window_reconciliation"]["window_status"] == "STARTUP_STUB"
        assert res_c0["expected_window_reconciliation"]["startup_stub_windows"] == ["2026-11-21T00:00Z"]

        # Now full window W1 (00:15Z) completes for all 4 exchanges -> at 00:31Z, W0 remains single-listed
        # in startup_stub_windows, first_full_window_id is 2026-11-21T00:15Z, pending_prior_windows is empty,
        # and outer sync_sealed_directory returns status == "ok"!
        for ex in EXCHANGES_4:
            _write_sealed_partition(
                dir_stub,
                exchange=ex,
                window_start_s=w1,
                partition_minutes=15,
                boot_id="boot_init",
                run_id="batch_init",
                first_rx_seq=10,
                dense=True,
            )
        res_c1 = arch.sync_sealed_directory(
            dir_stub,
            backend_stub,
            verify_remote=True,
            now_epoch_s=w1 + 16 * 60,
            upload_ledger_snapshot=False,
            expected_hourly_exchanges=set(EXCHANGES_4),
            partition_minutes=15,
            expected_run_id="batch_init",
        )
        assert res_c1["status"] == "ok", res_c1["failed_items"]
        rec_c1 = res_c1["expected_window_reconciliation"]
        assert rec_c1["expected_window_id"] == "2026-11-21T00:15Z"
        assert rec_c1["window_status"] == "COMPLETE"
        assert rec_c1["first_full_window_id"] == "2026-11-21T00:15Z"
        assert rec_c1["startup_stub_windows"] == ["2026-11-21T00:00Z"]
        assert rec_c1["pending_prior_windows"] == []


def test_review_round2_p1_3_historical_missing_overdue_window_does_not_fail_latest_passed_window_heartbeat():
    """Reproduces Review Round 2 P1 #3:
    When previous window W0 (00:00Z) has 0 files and is overdue, while latest window W1 (00:15Z)
    has all expected exchanges and passes verification:
    - `job_execution_status == "ok"`
    - `current_window_id == "2026-11-21T00:15Z"`
    - `current_window_completed is True`
    - `current_window_verification_status == "passed"`
    - `heartbeat_status == "ok"` and overall `status == "ok"` (so the workflow does NOT send `/fail`)
    - `historical_data_quality_status == "has_historical_damage"`
    - `historical_overdue_windows == ["2026-11-21T00:00Z"]` and `historical_damaged_windows == ["2026-11-21T00:00Z"]`
    """
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        archive_root = root / "data"
        state_dir = root / "state"
        w0 = int(datetime(2026, 11, 21, 0, 0, tzinfo=timezone.utc).timestamp())
        w1 = w0 + 15 * 60
        cfg = _make_config(partition_minutes=15, exchanges=EXCHANGES_4, stale_seconds=900)
        cfg_path = root / "config.json"
        cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

        # W0 (00:00Z) has ZERO files! W1 (00:15Z) has all 4 exchanges and is healthy.
        for ex in EXCHANGES_4:
            _write_sealed_partition(
                archive_root,
                exchange=ex,
                window_start_s=w1,
                partition_minutes=15,
                boot_id="b_p1_3",
                run_id="batch_p1_3",
            )
        backend = _MemoryArchiveBackend()
        arch.sync_sealed_directory(
            archive_root,
            backend,
            verify_remote=True,
            now_epoch_s=w1 + 19 * 60,
            upload_ledger_snapshot=False,
        )
        fake_run, fake_dl = _wire_mock_drive(archive_root, backend)

        # At w1 + 19m (00:34Z): W0 ended 19m ago (>10m SLA -> overdue), W1 is the latest complete window [00:15Z, 00:30Z)
        with patch.object(qa, "_run", side_effect=fake_run), \
             patch.object(qa, "_download", side_effect=fake_dl):
            res = qa.run(
                "qa:",
                cfg_path,
                state_dir,
                limit=16,
                since_date="2026-11-21",
                since_window="2026-11-21T00:00Z",
                run_id="batch_p1_3",
                now_epoch_s=w1 + 19 * 60,
                sla_grace_minutes=10.0,
            )
        assert res["job_execution_status"] == "ok"
        assert res["current_window_id"] == "2026-11-21T00:15Z"
        assert res["current_window_completed"] is True
        assert res["current_window_verification_status"] == qa.QA_STATUS_PASSED
        assert res["heartbeat_status"] == "ok"
        assert res["qa_status"] == qa.QA_STATUS_PASSED
        assert res["status"] == "ok"
        assert res["historical_data_quality_status"] == "has_historical_damage"
        assert res["historical_overdue_windows"] == ["2026-11-21T00:00Z"]
        assert res["historical_damaged_windows"] == ["2026-11-21T00:00Z"]


def test_review_round2_report_fixes_multi_segment_late_frame_no_double_count_and_four_historical_loss_manifests_preserved():
    """Reproduces Review Round 2 Report Consistency Fixes #1 and #2:
    (a) When target window W0 has 2 segments (`seg1`, `seg2`) for `binance_futures`, a single
        `LATE_FRAME_REJECTED` (`dropped_frames=1`) in W1 targeting W0 must be counted ONCE
        (`local_loss_events == 1`, `dropped_frames == 1`, `attributed_late_frames_rejected == 1`),
        never double-counted as 2.
    (b) The 4 historical 60-minute loss-marked manifests must be preserved in a standalone queryable
        record (`qa_reports/historical_loss_manifests.json`) and automatically included in
        `res["historical_loss_manifests"]` and `res["historical_damaged_partitions"]` even after
        switching QA to a 15-minute config and a new `--run-id` / `--since-window`.
    """
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        archive_root = root / "data"
        state_dir = root / "state"
        w_hist_base = int(datetime(2026, 11, 20, 0, 0, tzinfo=timezone.utc).timestamp())
        w0 = int(datetime(2026, 11, 21, 0, 0, tzinfo=timezone.utc).timestamp())
        w1 = w0 + 15 * 60
        w2 = w1 + 15 * 60

        cfg = _make_config(partition_minutes=15, exchanges=("binance_futures",), stale_seconds=900)
        cfg_path = root / "config.json"
        cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

        # 1. Seed 4 historical 60-minute loss-marked manifests from before the 15m cutover
        for idx, ex in enumerate(EXCHANGES_4):
            _, mf_h_path, mf_h = _write_sealed_partition(
                archive_root,
                exchange=ex,
                window_start_s=w_hist_base + idx * 3600,
                partition_minutes=60,
                legacy_v1=True,
                boot_id=f"legacy_loss_boot_{idx}",
                extra_meta=[{"event": "LOCAL_LOSS", "dropped_frames": idx + 2}],
            )
            mf_h["local_loss_lines"] = 1
            mf_h_path.write_text(json.dumps(mf_h, indent=2) + "\n", encoding="utf-8")

        # 2. Target window W0 (00:00Z) has TWO segments for binance_futures in batch_multi_seg
        _write_sealed_partition(
            archive_root,
            exchange="binance_futures",
            window_start_s=w0,
            partition_minutes=15,
            boot_id="boot_seg1",
            run_id="batch_multi_seg",
            first_rx_seq=1,
            offset_start_s=1,
            offset_end_s=440,
        )
        _write_sealed_partition(
            archive_root,
            exchange="binance_futures",
            window_start_s=w0,
            partition_minutes=15,
            boot_id="boot_seg2",
            run_id="batch_multi_seg",
            first_rx_seq=1,
            offset_start_s=445,
            offset_end_s=898,
        )

        # 3. Window W1 (00:15Z) has ONE rejected late frame targeting W0 (00:00Z)
        _, mf_w1_path, mf_w1 = _write_sealed_partition(
            archive_root,
            exchange="binance_futures",
            window_start_s=w1,
            partition_minutes=15,
            boot_id="boot_seg2",
            run_id="batch_multi_seg",
            first_rx_seq=7,
            extra_meta=[
                {
                    "event": "LATE_FRAME_REJECTED",
                    "dropped_frames": 1,
                    "orig_recv_wall_ns": w1 * 1_000_000_000 - 5_000_000,
                    "orig_recv_mono_ns": 500,
                    "orig_rx_seq": 6,
                    "orig_kind": "DATA",
                    "target_window_id": "2026-11-21T00:00Z",
                    "loss_attributed_window_id": "2026-11-21T00:00Z",
                    "target_planned_start_wall_ns": w0 * 1_000_000_000,
                    "target_planned_end_wall_ns": w1 * 1_000_000_000,
                    "target_partition_minutes": 15,
                }
            ],
        )
        mf_w1["late_frames_rejected"] = 1
        mf_w1_path.write_text(json.dumps(mf_w1, indent=2) + "\n", encoding="utf-8")

        # 4. Window W2 (00:30Z) is a clean new cutover batch `batch_cutover_clean`
        _write_sealed_partition(
            archive_root,
            exchange="binance_futures",
            window_start_s=w2,
            partition_minutes=15,
            boot_id="boot_cutover",
            run_id="batch_cutover_clean",
            first_rx_seq=1,
        )

        backend = _MemoryArchiveBackend()
        arch.sync_sealed_directory(
            archive_root,
            backend,
            verify_remote=True,
            now_epoch_s=w2 + 16 * 60,
            upload_ledger_snapshot=False,
        )
        fake_run, fake_dl = _wire_mock_drive(archive_root, backend)

        # Verify Fix #1: Run QA for `batch_multi_seg` across W0 and W1 -> W0 must count the late frame ONCE (1, not 2!)
        with patch.object(qa, "_run", side_effect=fake_run), \
             patch.object(qa, "_download", side_effect=fake_dl):
            res_multi = qa.run(
                "qa:",
                cfg_path,
                state_dir,
                limit=16,
                since_date="2026-11-21",
                since_window="2026-11-21T00:00Z",
                run_id="batch_multi_seg",
                now_epoch_s=w1 + 16 * 60,
            )
        w0_rep = json.loads(
            (state_dir / "qa_reports" / "windows" / "2026-11-21" / "2026-11-21T0000Z.window.qa.json").read_text()
        )
        assert w0_rep["qa_status"] == qa.QA_STATUS_FAILED
        assert w0_rep["attributed_late_frames_rejected"] == 1
        assert w0_rep["local_loss_events"] == 1
        assert w0_rep["dropped_frames"] == 1
        assert len(w0_rep["damaged_segments"]) == 1

        # Verify Fix #2: Switch QA to new batch `batch_cutover_clean` starting at W2 (2026-11-21T00:30Z)
        # All 4 prior 60m loss-marked manifests (plus the prior batch's loss-marked manifest) MUST remain preserved
        # in `historical_loss_manifests`, `historical_damaged_partitions`, and `qa_reports/historical_loss_manifests.json`!
        with patch.object(qa, "_run", side_effect=fake_run), \
             patch.object(qa, "_download", side_effect=fake_dl):
            res_cutover = qa.run(
                "qa:",
                cfg_path,
                state_dir,
                limit=16,
                since_date="2026-11-21",
                since_window="2026-11-21T00:30Z",
                run_id="batch_cutover_clean",
                now_epoch_s=w2 + 16 * 60,
            )
        assert res_cutover["status"] == "ok"
        assert res_cutover["heartbeat_status"] == "ok"
        assert res_cutover["current_window_id"] == "2026-11-21T00:30Z"
        assert res_cutover["current_window_verification_status"] == qa.QA_STATUS_PASSED
        assert res_cutover["historical_data_quality_status"] == "has_historical_damage"
        hist_60m = [
            m for m in res_cutover["historical_loss_manifests"] if int(m.get("partition_minutes") or 0) == 60
        ]
        assert len(hist_60m) == 4
        damaged_60m = [
            p for p in res_cutover["historical_damaged_partitions"] if int(p.get("partition_minutes") or 0) == 60
        ]
        assert len(damaged_60m) == 4
        persisted_hist_file = state_dir / "qa_reports" / "historical_loss_manifests.json"
        assert persisted_hist_file.is_file()
        persisted_hist_obj = json.loads(persisted_hist_file.read_text(encoding="utf-8"))
        assert persisted_hist_obj["count"] >= 4




def test_15m_archive_blocks_intra_file_silence_and_reuses_verified_scan(tmp_path):
    w0 = dc.parse_window_start_s("2026-11-21T00:00Z")
    _write_sealed_partition(tmp_path, exchange="binance_futures", window_start_s=w0)
    backend = _MemoryArchiveBackend()
    opts = dict(
        verify_remote=True,
        now_epoch_s=w0 + 16 * 60,
        upload_ledger_snapshot=False,
        expected_hourly_exchanges={"binance_futures"},
        partition_minutes=15,
    )
    result = arch.sync_sealed_directory(tmp_path, backend, **opts)
    assert result["status"] == "blocked"
    coverage = result["expected_window_reconciliation"]["coverage_gaps"]["binance_futures"]
    assert coverage["max_internal_gap_seconds"] > 890
    with patch.object(arch, "_scan_market_data_timestamps", wraps=arch._scan_market_data_timestamps) as scan:
        repeated = arch.sync_sealed_directory(tmp_path, backend, **opts)
    scan.assert_not_called()
    assert repeated["expected_window_reconciliation"]["coverage_gaps"]["binance_futures"] == coverage


def test_cutover_rejects_mid_window_start():
    cfg = _make_config(partition_minutes=15)
    with pytest.raises(ValueError, match="start of a complete 15-minute UTC window"):
        dc.resolve_cutover_window_utc(cfg, "2026-11-21T00:07Z", env={})
    assert dc.resolve_cutover_window_utc(cfg, "2026-11-21T00:15Z", env={}) == "2026-11-21T00:15Z"


def test_qa_blocks_mismatched_index_batch_even_when_matching_files_exist(tmp_path):
    w0 = dc.parse_window_start_s("2026-11-21T00:00Z")
    cfg = _make_config(partition_minutes=15, exchanges=("binance_futures",))
    cfg_path = tmp_path / "config.json"
    cfg_path.write_text(json.dumps(cfg), encoding="utf-8")
    _write_sealed_partition(tmp_path, exchange="binance_futures", window_start_s=w0, run_id="batch_new")
    backend = _MemoryArchiveBackend()
    arch.sync_sealed_directory(
        tmp_path, backend, verify_remote=True, now_epoch_s=w0 + 16 * 60, upload_ledger_snapshot=False
    )
    idx = json.loads(backend.objects[arch.ARCHIVE_MANIFEST_INDEX_RELPATH])
    idx["effective_run_id"] = "batch_old"
    backend.objects[arch.ARCHIVE_MANIFEST_INDEX_RELPATH] = json.dumps(idx).encode()
    fake_run, fake_dl = _wire_mock_drive(tmp_path, backend)
    with patch.object(qa, "_run", side_effect=fake_run), patch.object(qa, "_download", side_effect=fake_dl):
        result = qa.run(
            "qa:", cfg_path, tmp_path / "state", limit=16, since_date="2026-11-21",
            since_window="2026-11-21T00:00Z", run_id="batch_new", now_epoch_s=w0 + 16 * 60,
        )
    assert result["status"] == "blocked"
    assert result["heartbeat_status"] == "failed"
    assert result["attempted"] == 0
    assert any("verified_archive_index_batch_mismatch" in detail for detail in result["details"])
