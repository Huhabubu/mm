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
        # Emit at window head and window tail so stream gaps stay small
        tail_offset = offset_end_s if offset_end_s is not None else max(offset_start_s + 2, partition_minutes * 60 - 2)
        times = [start_ns + offset_start_s * 1_000_000_000, start_ns + tail_offset * 1_000_000_000]
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
            _write_sealed_partition(root, exchange=ex, window_start_s=w0, partition_minutes=15, boot_id="b1")

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
                    root, exchange="binance_futures", window_start_s=w0, partition_minutes=15, boot_id="b1"
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
            d, _, _ = _write_sealed_partition(root, exchange=ex, window_start_s=w0, partition_minutes=15, boot_id="b1")
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
            _write_sealed_partition(root, exchange=ex, window_start_s=w1, partition_minutes=15, boot_id="b1", first_rx_seq=10)

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
        idx_mut = json.loads(backend.objects[arch.ARCHIVE_MANIFEST_INDEX_RELPATH].decode("utf-8"))
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

        # Empty Drive (reproduces run 44887217848 zero-file bug!)
        with patch.object(qa, "_run", return_value=""), \
             patch.object(qa, "_download"):
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

