"""Comprehensive Phase P1 test suite for SQLite incremental archive ledger & safe rolling pruning.

Covers all Section 1.5 acceptance scenarios deterministically offline:
1. First upload succeeds (SEALED -> VERIFIED with dual data+manifest remote verification).
2. Repeat execution performs zero redundant uploads, zero remote stats, and zero zstd re-reads.
3. Upload interruption preserves local data, stays SEALED with error event, and resumes cleanly.
4. Crash after upload before ledger commit recovers idempotently with zero redundant data upload.
5. Crash after VERIFIED commit before pruning recovers and prunes cleanly on next aged run.
6. Crash after local unlink before PRUNED commit recovers safely via PRUNE_INTENT + remote proof.
7. Unexpected local data disappearance (both unverified and VERIFIED without PRUNE_INTENT) blocks.
8. Remote same-name different-hash conflicts (both data & manifest) block without overwriting/pruning.
9. SQLite WAL restart recovery + consistent sqlite3.Connection.backup snapshot upload to meta/.
10. Multiple immutable partitions within the same UTC hour (distinct boot_id / partition_id).
11. Real cross-process concurrent archive mutual exclusion via POSIX flock.
12. Legacy verified_archive_ledger.jsonl migration + PRUNED partition QA download/restore verification.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest
import zstandard as zstd

REPO_ROOT = Path(__file__).resolve().parents[2]
COLLECTOR_DIR = REPO_ROOT / "collector"
PYTHON_DIR = COLLECTOR_DIR / "python"
SPECS_DIR = COLLECTOR_DIR / "specs"

if str(PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(PYTHON_DIR))

import archive_sync as arch_mod  # noqa: E402
from archive_sync import (  # noqa: E402
    ARCHIVE_LOCK_FILENAME,
    LEDGER_DB_FILENAME,
    LEGACY_LEDGER_JSONL_FILENAME,
    PROJECTION_LEDGER_JSONL_FILENAME,
    STATE_PRUNED,
    STATE_SEALED,
    STATE_VERIFIED,
    ArchiveLedger,
    RemoteObjectInfo,
    file_hashes,
    sync_sealed_directory,
)
from qa_convert import run_qa_convert  # noqa: E402


class InstrumentedMockBackend:
    """In-memory ArchiveBackend recording every stat, copy, download, and readback call."""

    def __init__(
        self,
        *,
        fail_on_copy: bool = False,
        fail_on_stat: bool = False,
        expose_hashes: bool = True,
    ) -> None:
        self.store: dict[str, bytes] = {}
        self.id_overrides: dict[str, str] = {}
        self.fail_on_copy = fail_on_copy
        self.fail_on_stat = fail_on_stat
        self.expose_hashes = expose_hashes
        self.stat_calls: list[str] = []
        self.copy_calls: list[tuple[str, bool]] = []
        self.download_calls: list[str] = []
        self.readback_calls: list[str] = []

    def reset_counts(self) -> None:
        self.stat_calls.clear()
        self.copy_calls.clear()
        self.download_calls.clear()
        self.readback_calls.clear()

    def stat_remote(self, remote_rel_path: str) -> RemoteObjectInfo:
        self.stat_calls.append(remote_rel_path)
        if self.fail_on_stat:
            raise RuntimeError(f"simulated stat failure for {remote_rel_path}")
        blob = self.store.get(remote_rel_path)
        if blob is None:
            return RemoteObjectInfo(exists=False)
        remote_id = self.id_overrides.get(
            remote_rel_path,
            f"gd:{hashlib.md5(remote_rel_path.encode()).hexdigest()[:12]}",
        )
        return RemoteObjectInfo(
            exists=True,
            remote_id=remote_id,
            size=len(blob),
            md5=hashlib.md5(blob).hexdigest() if self.expose_hashes else None,
            sha256=hashlib.sha256(blob).hexdigest() if self.expose_hashes else None,
        )

    def copy_file(self, local_path: Path, remote_rel_path: str, *, immutable: bool = True) -> None:
        self.copy_calls.append((remote_rel_path, immutable))
        if self.fail_on_copy:
            raise ConnectionResetError(f"simulated copy failure for {remote_rel_path}")
        self.store[remote_rel_path] = local_path.read_bytes()

    def download_file(self, remote_rel_path: str, local_dest: Path) -> None:
        self.download_calls.append(remote_rel_path)
        if remote_rel_path not in self.store:
            raise FileNotFoundError(f"remote object missing: {remote_rel_path}")
        local_dest.parent.mkdir(parents=True, exist_ok=True)
        local_dest.write_bytes(self.store[remote_rel_path])

    def readback_sha256(self, remote_rel_path: str) -> str:
        self.readback_calls.append(remote_rel_path)
        return hashlib.sha256(self.store[remote_rel_path]).hexdigest()


def _create_sealed_partition(
    sealed_root: Path,
    *,
    exchange: str = "gate_futures",
    date_utc: str = "2026-12-03",
    hour_utc: str = "00",
    boot_id: str = "boot_1",
    stem: str | None = None,
    partition_id: str | None = None,
    start_wall_ns: int = 1_796_256_001_000_000_000,
    records: list[tuple[int, int, str, int, int, str, str]] | None = None,
    mtime_epoch_s: float | None = None,
) -> tuple[Path, Path, dict[str, Any]]:
    """Create a valid compressed .tsv.zst + matching .manifest.json partition."""
    yyyymmdd = date_utc.replace("-", "")
    eff_stem = stem or f"{exchange}_{yyyymmdd}_{hour_utc}"
    part_dir = sealed_root / "raw" / exchange / date_utc
    part_dir.mkdir(parents=True, exist_ok=True)
    data_path = part_dir / f"{eff_stem}.tsv.zst"
    manifest_path = part_dir / f"{eff_stem}.manifest.json"

    if records is None:
        base_ms = start_wall_ns // 1_000_000
        payload = json.dumps(
            {
                "time": base_ms // 1000,
                "time_ms": base_ms,
                "channel": "futures.obu",
                "event": "update",
                "result": {
                    "t": base_ms,
                    "full": True,
                    "s": "ob.BTC_USDT.50",
                    "u": 10,
                    "b": [["95000.0", "1.5"]],
                    "a": [["95000.1", "1.2"]],
                },
            }
        )
        records = [(start_wall_ns, 1_000_000, "A", 1, 1, "DATA", payload)]

    raw_text = "".join(f"{r[0]}\t{r[1]}\t{r[2]}\t{r[3]}\t{r[4]}\t{r[5]}\t{r[6]}\n" for r in records)
    cctx = zstd.ZstdCompressor(level=3)
    data_path.write_bytes(cctx.compress(raw_text.encode("utf-8")))
    hashes = file_hashes(data_path)

    manifest: dict[str, Any] = {
        "schema_version": 1,
        "exchange": exchange,
        "date_utc": date_utc,
        "hour_utc": hour_utc,
        "boot_id": boot_id,
        "file_name": data_path.name,
        "bytes": hashes["bytes"],
        "sha256": hashes["sha256"],
        "zstd_verified": True,
        "start_wall_ns": records[0][0],
        "end_wall_ns": records[-1][0],
        "first_rx_seq": records[0][4],
        "last_rx_seq": records[-1][4],
        "data_lines": sum(1 for r in records if r[5] == "DATA"),
        "data_escaped_lines": sum(1 for r in records if r[5] == "DATA_ESCAPED"),
        "meta_lines": sum(1 for r in records if r[5] == "META"),
        "local_loss_lines": 0,
        "gap_start_lines": 0,
        "switch_lines": 0,
    }
    if partition_id is not None:
        manifest["partition_id"] = partition_id

    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    if mtime_epoch_s is not None:
        os.utime(data_path, (mtime_epoch_s, mtime_epoch_s))
    return data_path, manifest_path, manifest


def test_p1_first_upload_and_incremental_skip_on_repeat(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """1 & 2: First archive verifies & uploads data+manifest+snapshot; repeat does zero reads/uploads/stats."""
    sealed = tmp_path / "sealed"
    dpath, mpath, _ = _create_sealed_partition(sealed)
    rel_data = dpath.relative_to(sealed).as_posix()
    rel_manifest = mpath.relative_to(sealed).as_posix()

    zstd_calls: list[Path] = []
    orig_verify_zstd = arch_mod.verify_zstd_integrity

    def spy_verify_zstd(path: Path) -> None:
        zstd_calls.append(path)
        orig_verify_zstd(path)

    monkeypatch.setattr(arch_mod, "verify_zstd_integrity", spy_verify_zstd)

    backend = InstrumentedMockBackend()
    t0 = dpath.stat().st_mtime + 60.0
    r1 = sync_sealed_directory(sealed, backend=backend, verify_remote=True, prune_older_than_hours=24.0, now_epoch_s=t0)
    assert r1["status"] == "ok"
    assert r1["newly_verified_count"] == 1
    assert r1["state_counts"] == {STATE_SEALED: 0, STATE_VERIFIED: 1, STATE_PRUNED: 0}
    assert rel_data in backend.store
    assert rel_manifest in backend.store
    assert r1["snapshot_remote_path"] is not None
    assert r1["snapshot_remote_path"] in backend.store
    assert len(zstd_calls) == 1

    with ArchiveLedger(sealed / LEDGER_DB_FILENAME) as ledger:
        row = ledger.get_partition_by_rel_data(rel_data)
        assert row is not None
        assert row["state"] == STATE_VERIFIED
        assert row["remote_data_verified"] == 1
        assert row["remote_manifest_verified"] == 1

    # Second identical run: ZERO copy_file, ZERO stat_remote, ZERO zstd decompressions!
    backend.reset_counts()
    zstd_calls.clear()
    r2 = sync_sealed_directory(
        sealed, backend=backend, verify_remote=True, prune_older_than_hours=24.0, now_epoch_s=t0 + 60.0
    )
    assert r2["status"] == "ok"
    assert r2["newly_verified_count"] == 0
    assert r2["skipped_verified_count"] == 1
    assert backend.copy_calls == []
    assert backend.stat_calls == []
    assert zstd_calls == []
    assert dpath.is_file()


def test_p1_upload_interruption_and_resume(tmp_path: Path) -> None:
    """3: Upload interruption keeps partition SEALED with error event, never prunes, and resumes cleanly."""
    sealed = tmp_path / "sealed"
    dpath, _, _ = _create_sealed_partition(sealed)
    rel_data = dpath.relative_to(sealed).as_posix()

    backend = InstrumentedMockBackend(fail_on_copy=True)
    r_fail = sync_sealed_directory(sealed, backend=backend, verify_remote=True, prune_older_than_hours=0.0)
    assert r_fail["status"] == "blocked"
    assert dpath.is_file(), "Local file must NEVER be pruned after upload interruption"

    with ArchiveLedger(sealed / LEDGER_DB_FILENAME) as ledger:
        row = ledger.get_partition_by_rel_data(rel_data)
        assert row is not None
        assert row["state"] == STATE_SEALED
        assert row["last_error_code"] == "ConnectionResetError"
        assert row["remote_data_verified"] == 0
        events = [e["event_type"] for e in ledger.list_events(rel_data)]
        assert "ERROR" in events

    # Resume with healthy backend
    backend.fail_on_copy = False
    r_ok = sync_sealed_directory(sealed, backend=backend, verify_remote=True, prune_older_than_hours=24.0)
    assert r_ok["status"] == "ok"
    with ArchiveLedger(sealed / LEDGER_DB_FILENAME) as ledger:
        row = ledger.get_partition_by_rel_data(rel_data)
        assert row is not None
        assert row["state"] == STATE_VERIFIED
        assert row["last_error_code"] is None


def test_p1_crash_after_upload_before_ledger_commit(tmp_path: Path) -> None:
    """4: Crash after upload succeeded but before VERIFIED commit resumes idempotently without re-uploading."""
    sealed = tmp_path / "sealed"
    dpath, mpath, _ = _create_sealed_partition(sealed)
    rel_data = dpath.relative_to(sealed).as_posix()
    rel_manifest = mpath.relative_to(sealed).as_posix()

    backend = InstrumentedMockBackend()

    def crash_before_commit(_rel: str) -> None:
        raise RuntimeError("simulated_crash_before_verify_commit")

    r_crash = sync_sealed_directory(
        sealed,
        backend=backend,
        verify_remote=True,
        prune_older_than_hours=0.0,
        fault_hooks={"before_verify_commit": crash_before_commit},
    )
    assert r_crash["status"] == "blocked"
    assert rel_data in backend.store and rel_manifest in backend.store
    assert dpath.is_file()

    with ArchiveLedger(sealed / LEDGER_DB_FILENAME) as ledger:
        row = ledger.get_partition_by_rel_data(rel_data)
        assert row is not None
        assert row["state"] == STATE_SEALED

    # Next run: sees identical remote data & manifest -> ZERO raw copy_file calls -> commits VERIFIED!
    backend.reset_counts()
    r_recover = sync_sealed_directory(sealed, backend=backend, verify_remote=True, prune_older_than_hours=24.0)
    assert r_recover["status"] == "ok"
    raw_copies = [p for p, _ in backend.copy_calls if p.startswith("raw/")]
    assert raw_copies == [], f"Expected zero raw re-uploads on crash recovery, got {raw_copies}"

    with ArchiveLedger(sealed / LEDGER_DB_FILENAME) as ledger:
        row = ledger.get_partition_by_rel_data(rel_data)
        assert row is not None
        assert row["state"] == STATE_VERIFIED


def test_p1_crash_after_verified_before_prune(tmp_path: Path) -> None:
    """5: Crash after VERIFIED committed before unlink keeps local file, prunes cleanly on next run, then skips."""
    sealed = tmp_path / "sealed"
    base_mtime = 1_700_000_000.0
    dpath, mpath, _ = _create_sealed_partition(sealed, mtime_epoch_s=base_mtime)
    rel_data = dpath.relative_to(sealed).as_posix()

    backend = InstrumentedMockBackend()

    def crash_before_prune(_rel: str) -> None:
        raise RuntimeError("simulated_crash_after_verified_before_prune")

    r_crash = sync_sealed_directory(
        sealed,
        backend=backend,
        verify_remote=True,
        prune_older_than_hours=24.0,
        now_epoch_s=base_mtime + 25 * 3600.0,
        fault_hooks={"after_verify_before_prune": crash_before_prune},
    )
    assert r_crash["status"] == "blocked"
    assert dpath.is_file()
    with ArchiveLedger(sealed / LEDGER_DB_FILENAME) as ledger:
        assert ledger.get_partition_by_rel_data(rel_data)["state"] == STATE_VERIFIED

    # Retry aged run: automatically re-verifies remote data+manifest (clearing transient error),
    # performs ZERO raw uploads, prunes local .tsv.zst, keeps manifest, and commits PRUNED!
    backend.reset_counts()
    r_prune = sync_sealed_directory(
        sealed,
        backend=backend,
        verify_remote=True,
        prune_older_than_hours=24.0,
        now_epoch_s=base_mtime + 25 * 3600.0,
    )
    assert r_prune["status"] == "ok"
    assert rel_data in r_prune["pruned_local_files"]
    assert not dpath.exists()
    assert mpath.is_file(), "Local manifest MUST remain preserved after pruning"
    raw_copies = [p for p, _ in backend.copy_calls if p.startswith("raw/")]
    assert raw_copies == []

    # Subsequent hourly scan after PRUNED: ZERO errors, ZERO stat_remote, ZERO copy_file!
    backend.reset_counts()
    r_after = sync_sealed_directory(
        sealed,
        backend=backend,
        verify_remote=True,
        prune_older_than_hours=24.0,
        now_epoch_s=base_mtime + 26 * 3600.0,
    )
    assert r_after["status"] == "ok"
    assert r_after["skipped_pruned_count"] == 1
    assert backend.stat_calls == []
    assert backend.copy_calls == []


def test_p1_crash_after_unlink_before_pruned_commit(tmp_path: Path) -> None:
    """6: Crash after dpath.unlink() before PRUNED commit recovers safely via PRUNE_INTENT + remote proof."""
    sealed = tmp_path / "sealed"
    base_mtime = 1_700_000_000.0
    dpath, mpath, _ = _create_sealed_partition(sealed, mtime_epoch_s=base_mtime)
    rel_data = dpath.relative_to(sealed).as_posix()

    backend = InstrumentedMockBackend()
    # First run: verify cleanly
    r1 = sync_sealed_directory(
        sealed, backend=backend, verify_remote=True, prune_older_than_hours=24.0, now_epoch_s=base_mtime + 3600.0
    )
    assert r1["status"] == "ok"
    assert dpath.is_file()

    # Second run (aged): crash right AFTER dpath.unlink() and BEFORE mark_pruned() commits!
    def crash_after_unlink(_rel: str) -> None:
        raise RuntimeError("simulated_power_loss_after_unlink")

    r_crash = sync_sealed_directory(
        sealed,
        backend=backend,
        verify_remote=True,
        prune_older_than_hours=24.0,
        now_epoch_s=base_mtime + 25 * 3600.0,
        fault_hooks={"after_prune_unlink_before_commit": crash_after_unlink},
    )
    assert r_crash["status"] == "blocked"
    assert not dpath.exists(), "Unlink occurred before simulated crash"
    assert mpath.is_file()
    with ArchiveLedger(sealed / LEDGER_DB_FILENAME) as ledger:
        row = ledger.get_partition_by_rel_data(rel_data)
        assert row["state"] == STATE_VERIFIED
        assert ledger.has_pending_prune_intent(rel_data) is True

    # Recovery run: verifies remote data + remote manifest and cleanly transitions to PRUNED!
    backend.reset_counts()
    r_rec = sync_sealed_directory(
        sealed,
        backend=backend,
        verify_remote=True,
        prune_older_than_hours=24.0,
        now_epoch_s=base_mtime + 26 * 3600.0,
    )
    assert r_rec["status"] == "ok"
    assert rel_data in r_rec["recovered_pruned_files"]
    with ArchiveLedger(sealed / LEDGER_DB_FILENAME) as ledger:
        row = ledger.get_partition_by_rel_data(rel_data)
        assert row["state"] == STATE_PRUNED
        assert row["last_error_code"] is None
        assert ledger.has_pending_prune_intent(rel_data) is False
        events = [e["event_type"] for e in ledger.list_events(rel_data)]
        assert "PRUNE_RECOVERY_OK" in events

    # Next run: cleanly skipped with zero API calls
    backend.reset_counts()
    r_next = sync_sealed_directory(
        sealed, backend=backend, verify_remote=True, prune_older_than_hours=24.0, now_epoch_s=base_mtime + 27 * 3600.0
    )
    assert r_next["status"] == "ok"
    assert r_next["skipped_pruned_count"] == 1
    assert backend.stat_calls == []
    assert backend.copy_calls == []


def test_p1_unexpected_missing_local_data_blocked(tmp_path: Path) -> None:
    """7: Missing local .tsv.zst without proof OR while VERIFIED without PRUNE_INTENT fails closed."""
    # Case A: Brand new manifest whose .tsv.zst disappeared BEFORE verification -> CRITICAL ERROR
    sealed_a = tmp_path / "sealed_a"
    dpath_a, _, _ = _create_sealed_partition(sealed_a)
    rel_a = dpath_a.relative_to(sealed_a).as_posix()
    dpath_a.unlink()

    backend_a = InstrumentedMockBackend()
    res_a = sync_sealed_directory(sealed_a, backend=backend_a, verify_remote=True)
    assert res_a["status"] == "blocked"
    assert any("MISSING_LOCAL_DATA" in x or "NO valid VERIFIED/PRUNED proof" in x for x in res_a["failed_items"])
    with ArchiveLedger(sealed_a / LEDGER_DB_FILENAME) as ledger:
        events = [e["event_type"] for e in ledger.list_events(rel_a)]
        assert "MISSING_LOCAL_DATA_CRITICAL" in events

    # Case B: Partition is VERIFIED (within retention, NO PRUNE_INTENT), then local file unexpectedly deleted
    sealed_b = tmp_path / "sealed_b"
    dpath_b, _, _ = _create_sealed_partition(sealed_b)
    rel_b = dpath_b.relative_to(sealed_b).as_posix()
    backend_b = InstrumentedMockBackend()
    assert sync_sealed_directory(sealed_b, backend=backend_b, verify_remote=True)["status"] == "ok"
    dpath_b.unlink()  # Accidental external deletion (no PRUNE_INTENT!)

    res_b = sync_sealed_directory(sealed_b, backend=backend_b, verify_remote=True, prune_older_than_hours=24.0)
    assert res_b["status"] == "blocked"
    assert any("without PRUNE_INTENT" in x for x in res_b["failed_items"])
    with ArchiveLedger(sealed_b / LEDGER_DB_FILENAME) as ledger:
        row = ledger.get_partition_by_rel_data(rel_b)
        assert row["state"] == STATE_VERIFIED, "MUST NOT silently promote accidental local deletion to PRUNED!"
        assert row["last_error_code"] == "LOCAL_DATA_MISSING_UNEXPECTED"


def test_p1_remote_hash_conflict_data_and_manifest(tmp_path: Path) -> None:
    """8: Remote same-name different-hash conflicts on data OR manifest block without overwriting or pruning."""
    # Data conflict
    sealed1 = tmp_path / "sealed_data_conflict"
    dpath1, mpath1, _ = _create_sealed_partition(sealed1)
    rel_data1 = dpath1.relative_to(sealed1).as_posix()

    backend1 = InstrumentedMockBackend()
    backend1.store[rel_data1] = b"DIFFERENT_REMOTE_DATA_BYTES"
    res1 = sync_sealed_directory(sealed1, backend=backend1, verify_remote=True, prune_older_than_hours=0.0)
    assert res1["status"] == "blocked"
    assert len(res1["conflicts"]) == 1
    assert backend1.store[rel_data1] == b"DIFFERENT_REMOTE_DATA_BYTES"
    assert dpath1.is_file()
    with ArchiveLedger(sealed1 / LEDGER_DB_FILENAME) as ledger:
        row = ledger.get_partition_by_rel_data(rel_data1)
        assert row["state"] == STATE_SEALED
        assert row["has_conflict"] == 1

    # Manifest conflict (data matches, remote manifest differs!)
    sealed2 = tmp_path / "sealed_manifest_conflict"
    dpath2, mpath2, _ = _create_sealed_partition(sealed2)
    rel_data2 = dpath2.relative_to(sealed2).as_posix()
    rel_mf2 = mpath2.relative_to(sealed2).as_posix()

    backend2 = InstrumentedMockBackend()
    backend2.store[rel_data2] = dpath2.read_bytes()
    backend2.store[rel_mf2] = b'{"tampered":"manifest"}\n'
    res2 = sync_sealed_directory(sealed2, backend=backend2, verify_remote=True, prune_older_than_hours=0.0)
    assert res2["status"] == "blocked"
    assert len(res2["conflicts"]) == 1
    assert res2["conflicts"][0]["action"] == "ISOLATED_MANIFEST_CONFLICT_NO_OVERWRITE"
    assert backend2.store[rel_mf2] == b'{"tampered":"manifest"}\n'
    assert dpath2.is_file(), "Local file must NEVER be pruned on manifest conflict"
    with ArchiveLedger(sealed2 / LEDGER_DB_FILENAME) as ledger:
        row = ledger.get_partition_by_rel_data(rel_data2)
        assert row["state"] == STATE_SEALED
        assert row["has_conflict"] == 1


def test_p1_sqlite_restart_recovery_and_wal_snapshot_backup(tmp_path: Path) -> None:
    """9: Verify SQLite WAL + synchronous=FULL, restart persistence, consistent backup snapshot, and WAL exclusion."""
    sealed = tmp_path / "sealed"
    dpath, _, _ = _create_sealed_partition(sealed)
    rel_data = dpath.relative_to(sealed).as_posix()

    # Create decoy WAL/SHM/partial/lock files under meta/ that MUST NEVER be raw-copied
    meta_dir = sealed / "meta" / "chrony"
    meta_dir.mkdir(parents=True)
    (meta_dir / "chrony_20261203.csv").write_text("ts,ok\n1,true\n", encoding="utf-8")
    (sealed / "meta" / "live.sqlite-wal").write_bytes(b"RAW_WAL_MUST_NOT_UPLOAD")
    (sealed / "meta" / "live.sqlite-shm").write_bytes(b"RAW_SHM_MUST_NOT_UPLOAD")
    (sealed / "meta" / "temp.partial").write_bytes(b"PARTIAL_MUST_NOT_UPLOAD")

    backend = InstrumentedMockBackend()
    res = sync_sealed_directory(sealed, backend=backend, verify_remote=True)
    assert res["status"] == "ok"
    snap_rel = res["snapshot_remote_path"]
    assert snap_rel is not None and snap_rel.startswith("meta/archive_ledger/archive_ledger_")
    assert f"{snap_rel}.sha256" in backend.store
    assert "meta/live.sqlite-wal" not in backend.store
    assert "meta/live.sqlite-shm" not in backend.store
    assert "meta/temp.partial" not in backend.store
    assert "meta/chrony/chrony_20261203.csv" in backend.store

    # Verify WAL & synchronous=FULL on reopened DB (simulating process restart)
    with ArchiveLedger(sealed / LEDGER_DB_FILENAME) as ledger:
        jm = ledger.conn.execute("PRAGMA journal_mode;").fetchone()[0]
        sync_mode = ledger.conn.execute("PRAGMA synchronous;").fetchone()[0]
        assert str(jm).lower() == "wal"
        assert int(sync_mode) == 2  # FULL
        row = ledger.get_partition_by_rel_data(rel_data)
        assert row is not None and row["state"] == STATE_VERIFIED

    # Verify remote snapshot passes sha256 + SQLite PRAGMA integrity_check and contains committed row
    snap_bytes = backend.store[snap_rel]
    sha_line = backend.store[f"{snap_rel}.sha256"].decode("utf-8").strip().split()[0]
    assert hashlib.sha256(snap_bytes).hexdigest() == sha_line
    snap_copy = tmp_path / "downloaded_snapshot.sqlite"
    snap_copy.write_bytes(snap_bytes)
    conn = sqlite3.connect(str(snap_copy))
    try:
        assert conn.execute("PRAGMA integrity_check;").fetchone()[0] == "ok"
        rows = conn.execute("SELECT rel_data_path, state FROM partitions").fetchall()
        assert rows == [(rel_data, STATE_VERIFIED)]
    finally:
        conn.close()


def test_p1_multiple_partitions_same_utc_hour(tmp_path: Path) -> None:
    """10: Multiple immutable partitions in the SAME UTC hour (different boot_id/partition_id) tracked independently."""
    sealed = tmp_path / "sealed"
    t_old = 1_700_000_000.0
    t_new = t_old + 25 * 3600.0  # 25h newer

    base_wall = 1_796_256_000_000_000_000
    base_ms = base_wall // 1_000_000

    p1_payload = json.dumps(
        {
            "time": base_ms // 1000,
            "time_ms": base_ms + 1,
            "channel": "futures.obu",
            "event": "update",
            "result": {"t": base_ms + 1, "full": True, "s": "ob.BTC_USDT.50", "u": 1, "b": [["95000.0", "1.0"]], "a": [["95000.1", "1.0"]]},
        }
    )
    p2_payload = json.dumps(
        {
            "time": base_ms // 1000,
            "time_ms": base_ms + 5000,
            "channel": "futures.obu",
            "event": "update",
            "result": {"t": base_ms + 5000, "full": True, "s": "ob.BTC_USDT.50", "u": 100, "b": [["95001.0", "2.0"]], "a": [["95001.1", "2.0"]]},
        }
    )

    d1, m1, _ = _create_sealed_partition(
        sealed,
        exchange="gate_futures",
        date_utc="2026-12-03",
        hour_utc="00",
        boot_id="boot_A",
        stem="gate_futures_20261203_00__boot_A__p01",
        partition_id="p01",
        start_wall_ns=base_wall + 1_000_000,
        records=[(base_wall + 1_000_000, 1_000_000, "A", 1, 1, "DATA", p1_payload)],
        mtime_epoch_s=t_old,
    )
    d2, m2, _ = _create_sealed_partition(
        sealed,
        exchange="gate_futures",
        date_utc="2026-12-03",
        hour_utc="00",
        boot_id="boot_B",
        stem="gate_futures_20261203_00__boot_B__p02",
        partition_id="p02",
        start_wall_ns=base_wall + 5_000_000_000,
        records=[(base_wall + 5_000_000_000, 1_000_000, "A", 1, 1, "DATA", p2_payload)],
        mtime_epoch_s=t_new,
    )

    rel1 = d1.relative_to(sealed).as_posix()
    rel2 = d2.relative_to(sealed).as_posix()

    backend = InstrumentedMockBackend()
    res = sync_sealed_directory(
        sealed,
        backend=backend,
        verify_remote=True,
        prune_older_than_hours=24.0,
        now_epoch_s=t_new + 60.0,
    )
    assert res["status"] == "ok"
    assert res["pruned_local_files"] == [rel1], "Only aged part 1 should be pruned; young part 2 must remain"
    assert not d1.exists() and m1.is_file()
    assert d2.is_file() and m2.is_file()

    with ArchiveLedger(sealed / LEDGER_DB_FILENAME) as ledger:
        rows = ledger.list_partitions()
        assert len(rows) == 2
        by_rel = {r["rel_data_path"]: r for r in rows}
        assert by_rel[rel1]["state"] == STATE_PRUNED
        assert by_rel[rel1]["boot_id"] == "boot_A"
        assert by_rel[rel1]["partition_id"] == "p01"
        assert by_rel[rel2]["state"] == STATE_VERIFIED
        assert by_rel[rel2]["boot_id"] == "boot_B"
        assert by_rel[rel2]["partition_id"] == "p02"

    # Verify QA restores PRUNED part 1 + reads local part 2 in chronological order!
    out_qa = tmp_path / "qa_multi_part"
    qa = run_qa_convert(sealed / LEDGER_DB_FILENAME, out_qa, remote_backend=backend, strict_zero_loss=True)
    assert qa["status"] == "ok"
    assert qa["exchanges"]["gate_futures"]["partitions_verified"] == 2
    assert qa["exchanges"]["gate_futures"]["records_parsed"] == 2
    assert qa["restored_pruned_partitions"] == [rel1]


def test_p1_concurrent_archive_mutual_exclusion(tmp_path: Path) -> None:
    """11: Overlapping archive invocation fails closed via POSIX flock without corrupting ledger or uploading."""
    sealed = tmp_path / "sealed"
    _create_sealed_partition(sealed)
    lock_path = sealed / ARCHIVE_LOCK_FILENAME

    # Spawn a separate OS process holding the exclusive flock on .archive_sync.lock
    holder_code = f"""
import fcntl, os, sys, time
fd = os.open({str(lock_path)!r}, os.O_CREAT | os.O_RDWR, 0o644)
fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
print("LOCKED", flush=True)
sys.stdin.readline()
fcntl.flock(fd, fcntl.LOCK_UN)
os.close(fd)
"""
    proc = subprocess.Popen(
        [sys.executable, "-c", holder_code],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert proc.stdout is not None
        ready = proc.stdout.readline().strip()
        assert ready == "LOCKED"

        backend = InstrumentedMockBackend()
        res_busy = sync_sealed_directory(sealed, backend=backend, verify_remote=True)
        assert res_busy["status"] == "blocked"
        assert res_busy.get("lock_busy") is True
        assert backend.copy_calls == []
        assert backend.stat_calls == []
    finally:
        if proc.stdin is not None:
            proc.stdin.write("\n")
            proc.stdin.flush()
        proc.wait(timeout=5)

    # After lock is released, archive succeeds cleanly!
    backend2 = InstrumentedMockBackend()
    res_ok = sync_sealed_directory(sealed, backend=backend2, verify_remote=True)
    assert res_ok["status"] == "ok"


def test_p1_legacy_jsonl_migration_idempotent_no_reupload(tmp_path: Path) -> None:
    """12a: Existing verified_archive_ledger.jsonl is preserved verbatim while migrating into SQLite + compat projection."""
    sealed = tmp_path / "sealed"
    dpath, mpath, _ = _create_sealed_partition(sealed)
    rel_data = dpath.relative_to(sealed).as_posix()
    rel_manifest = mpath.relative_to(sealed).as_posix()
    dh = file_hashes(dpath)

    # Seed legacy JSONL ledger with:
    # - 2 historical verified lines for the SAME rel_data (duplicate verification history!)
    # - 1 unverified line
    # - 1 malformed/invalid historical line
    legacy_path = sealed / LEGACY_LEDGER_JSONL_FILENAME
    verified_line_old = {
        "verified_utc": "2026-12-03T01:05:00+00:00",
        "remote_path": rel_data,
        "remote_manifest_path": rel_manifest,
        "remote_id": "gd:legacy_old",
        "bytes": dh["bytes"],
        "sha256": dh["sha256"],
        "md5": dh["md5"],
        "verification_method": "remote_native_hash",
        "verified": True,
    }
    verified_line_latest = {
        **verified_line_old,
        "verified_utc": "2026-12-03T02:05:00+00:00",
        "remote_id": "gd:legacy_latest",
    }
    unverified_line = {
        "verified_utc": "2026-12-03T01:05:00+00:00",
        "remote_path": "raw/gate_futures/2026-12-03/unverified_fake.tsv.zst",
        "remote_manifest_path": "raw/gate_futures/2026-12-03/unverified_fake.manifest.json",
        "bytes": 100,
        "sha256": "0" * 64,
        "verified": False,
    }
    original_content = (
        json.dumps(verified_line_old)
        + "\n"
        + json.dumps(verified_line_latest)
        + "\n"
        + json.dumps(unverified_line)
        + "\n"
        + "{corrupted_historical_line\n"
    )
    legacy_path.write_text(original_content, encoding="utf-8")
    original_bytes = legacy_path.read_bytes()
    original_sha = hashlib.sha256(original_bytes).hexdigest()

    backend = InstrumentedMockBackend()
    backend.store[rel_data] = dpath.read_bytes()
    backend.store[rel_manifest] = mpath.read_bytes()

    r1 = sync_sealed_directory(sealed, backend=backend, verify_remote=True, prune_older_than_hours=24.0)
    assert r1["status"] == "ok"
    assert r1["migration"]["migrated"] == 1
    assert r1["migration"]["total_lines"] == 4
    assert r1["migration"]["verified_lines"] == 2
    assert r1["migration"]["duplicate_verified_lines"] == 1
    assert r1["migration"]["skipped_unverified"] == 1
    assert r1["migration"]["invalid_lines"] == 1
    assert r1["migration"]["original_sha256"] == original_sha
    assert r1["migration"]["original_bytes"] == len(original_bytes)

    # Original verified_archive_ledger.jsonl MUST remain 100% byte-for-byte untouched!
    assert legacy_path.read_bytes() == original_bytes
    assert r1["legacy_ledger_path"] == str(legacy_path)

    # Preserved immutable copy + .sha256 exist locally and were synced to remote meta/legacy_ledger/!
    preserved_rel = r1["migration"]["preserved_backup_path"]
    assert preserved_rel is not None and preserved_rel.startswith("meta/legacy_ledger/")
    assert (sealed / preserved_rel).read_bytes() == original_bytes
    assert backend.store[preserved_rel] == original_bytes
    assert f"{preserved_rel}.sha256" in backend.store

    # Compatibility projection written to a separate file (verified_archive_ledger.compat.jsonl)
    compat_path = sealed / PROJECTION_LEDGER_JSONL_FILENAME
    assert r1["ledger_path"] == str(compat_path)
    assert r1["projection_ledger_path"] == str(compat_path)
    assert compat_path.is_file()
    compat_rows = [json.loads(l) for l in compat_path.read_text(encoding="utf-8").splitlines() if l.strip()]
    assert len(compat_rows) == 1
    assert compat_rows[0]["remote_path"] == rel_data

    # Data and manifest were already remote -> ZERO raw copy_file uploads!
    raw_copies = [p for p, _ in backend.copy_calls if p.startswith("raw/")]
    assert raw_copies == []

    with ArchiveLedger(sealed / LEDGER_DB_FILENAME) as ledger:
        row = ledger.get_partition_by_rel_data(rel_data)
        assert row is not None
        assert row["state"] == STATE_VERIFIED
        assert row["remote_data_verified"] == 1
        assert row["remote_manifest_verified"] == 1
        assert ledger.get_partition_by_rel_data("raw/gate_futures/2026-12-03/unverified_fake.tsv.zst") is None
        assert ledger.get_meta("legacy_jsonl_original_sha256") == original_sha
        assert ledger.get_meta("legacy_jsonl_total_lines") == "4"
        assert ledger.get_meta("legacy_jsonl_duplicate_verified_lines") == "1"
        ev_types = [e["event_type"] for e in ledger.list_events()]
        assert "MIGRATION_HISTORICAL_VERIFIED_LINE" in ev_types
        assert "MIGRATION_SKIPPED_UNVERIFIED" in ev_types
        assert "MIGRATION_SKIPPED_INVALID" in ev_types
        assert "MIGRATION_LEGACY_JSONL_SUMMARY" in ev_types

    # Repeat run: migration is idempotent (already_migrated), original file still untouched, zero remote calls!
    backend.reset_counts()
    r2 = sync_sealed_directory(sealed, backend=backend, verify_remote=True, prune_older_than_hours=24.0)
    assert r2["status"] == "ok"
    assert r2["migration"]["already_migrated"] is True
    assert r2["migration"]["original_sha256"] == original_sha
    assert legacy_path.read_bytes() == original_bytes
    assert backend.copy_calls == []
    assert backend.stat_calls == []


def test_p1_pruned_partition_qa_restore_and_bit_identical_hbt_output(tmp_path: Path) -> None:
    """12b: PRUNED partitions restore from remote with SHA-256/zstd check & reproduce bit-identical HBT output."""
    sealed = tmp_path / "sealed"
    base_mtime = 1_700_000_000.0
    base_wall = 1_796_256_000_000_000_000
    base_ms = base_wall // 1_000_000

    # Create valid Binance + Gate partitions
    bin_payload = json.dumps(
        {
            "stream": "btcusdt@bookTicker",
            "data": {"e": "bookTicker", "u": 1, "s": "BTCUSDT", "b": "95000.0", "B": "3.0", "a": "95000.1", "A": "2.5", "T": base_ms, "E": base_ms},
        }
    )
    gate_payload = json.dumps(
        {
            "time": base_ms // 1000,
            "time_ms": base_ms + 2,
            "channel": "futures.obu",
            "event": "update",
            "result": {"t": base_ms + 2, "full": True, "s": "ob.BTC_USDT.50", "u": 50, "b": [["95000.0", "1.5"]], "a": [["95000.1", "1.2"]]},
        }
    )

    d_bin, m_bin, _ = _create_sealed_partition(
        sealed,
        exchange="binance_futures",
        date_utc="2026-12-03",
        hour_utc="00",
        boot_id="boot_qa",
        records=[(base_wall + 1_000_000, 1_000_000, "A", 1, 1, "DATA", bin_payload)],
        mtime_epoch_s=base_mtime,
    )
    d_gate, m_gate, _ = _create_sealed_partition(
        sealed,
        exchange="gate_futures",
        date_utc="2026-12-03",
        hour_utc="00",
        boot_id="boot_qa",
        records=[(base_wall + 2_000_000, 2_000_000, "A", 1, 1, "DATA", gate_payload)],
        mtime_epoch_s=base_mtime,
    )

    backend = InstrumentedMockBackend(expose_hashes=False)  # Also exercises readback_sha256 fallback!
    r_up = sync_sealed_directory(
        sealed, backend=backend, verify_remote=True, prune_older_than_hours=24.0, now_epoch_s=base_mtime + 3600.0
    )
    assert r_up["status"] == "ok"
    assert len(backend.readback_calls) >= 4  # Both data & manifest verified via readback fallback!

    # Baseline QA BEFORE pruning
    qa_before = run_qa_convert(sealed / LEDGER_DB_FILENAME, tmp_path / "qa_before", strict_zero_loss=True)
    assert qa_before["status"] == "ok"
    before_hashes = {a["kind"]: a["sha256"] for a in qa_before["artifacts"]}

    # Now prune both partitions after 25h
    r_prune = sync_sealed_directory(
        sealed, backend=backend, verify_remote=True, prune_older_than_hours=24.0, now_epoch_s=base_mtime + 25 * 3600.0
    )
    assert r_prune["status"] == "ok"
    assert len(r_prune["pruned_local_files"]) == 2
    assert not d_bin.exists() and not d_gate.exists()
    assert m_bin.is_file() and m_gate.is_file()

    # Without remote_backend, QA on PRUNED ledger must fail closed with actionable error!
    qa_no_remote = run_qa_convert(sealed / LEDGER_DB_FILENAME, tmp_path / "qa_fail", strict_zero_loss=True)
    assert qa_no_remote["status"] == "blocked"
    assert any("--remote" in x for x in qa_no_remote["failed_items"])

    # With remote_backend on SQLite ledger -> restores PRUNED files & produces bit-identical artifacts!
    cache_dir = tmp_path / "restore_cache"
    qa_from_sqlite = run_qa_convert(
        sealed / LEDGER_DB_FILENAME,
        tmp_path / "qa_after_sqlite",
        strict_zero_loss=True,
        remote_backend=backend,
        restore_cache_dir=cache_dir,
    )
    assert qa_from_sqlite["status"] == "ok"
    assert len(qa_from_sqlite["restored_pruned_partitions"]) == 2
    after_sqlite_hashes = {a["kind"]: a["sha256"] for a in qa_from_sqlite["artifacts"]}
    assert after_sqlite_hashes == before_hashes

    # Also works via separate compatibility projection verified_archive_ledger.compat.jsonl (reusing verified cache)!
    backend.reset_counts()
    qa_from_jsonl = run_qa_convert(
        sealed / PROJECTION_LEDGER_JSONL_FILENAME,
        tmp_path / "qa_after_jsonl",
        strict_zero_loss=True,
        remote_backend=backend,
        restore_cache_dir=cache_dir,
    )
    assert qa_from_jsonl["status"] == "ok"
    assert backend.download_calls == [], "Verified cache should avoid duplicate remote downloads"
    assert {a["kind"]: a["sha256"] for a in qa_from_jsonl["artifacts"]} == before_hashes

    # Tamper remote object -> restore MUST fail closed and NEVER leave corrupt file in fresh cache!
    rel_gate = d_gate.relative_to(sealed).as_posix()
    backend.store[rel_gate] = b"CORRUPTED_REMOTE_ZSTD_BYTES"
    bad_cache = tmp_path / "bad_cache"
    qa_tampered = run_qa_convert(
        sealed / LEDGER_DB_FILENAME,
        tmp_path / "qa_tampered",
        strict_zero_loss=True,
        remote_backend=backend,
        restore_cache_dir=bad_cache,
    )
    assert qa_tampered["status"] == "blocked"
    assert not (bad_cache / rel_gate).exists()


def test_p1_rclone_download_file_atomic_partial_rename(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify RcloneBackend.download_file uses copyto -> .partial -> atomic replace and cleans up on error."""
    from archive_sync import RcloneBackend

    backend = RcloneBackend("gdrive_main:tokyo_l2_data")
    cmds: list[list[str]] = []

    def fake_run_ok(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        cmds.append(cmd)
        Path(cmd[-1]).write_bytes(b"VERIFIED_BYTES")
        return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

    monkeypatch.setattr(arch_mod.subprocess, "run", fake_run_ok)
    dst = tmp_path / "cache" / "raw/gate_futures/2026-12-03/file.tsv.zst"
    backend.download_file("raw/gate_futures/2026-12-03/file.tsv.zst", dst)
    assert dst.read_bytes() == b"VERIFIED_BYTES"
    assert cmds[0][0:3] == ["rclone", "copyto", "gdrive_main:tokyo_l2_data/raw/gate_futures/2026-12-03/file.tsv.zst"]
    assert cmds[0][3].endswith(".partial")
    assert not Path(cmds[0][3]).exists()

    # On failure, temp .partial file is cleaned up and destination is not created
    def fake_run_fail(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        Path(cmd[-1]).write_bytes(b"HALF_WRITTEN")
        raise subprocess.CalledProcessError(returncode=1, cmd=cmd, stderr="network reset")

    monkeypatch.setattr(arch_mod.subprocess, "run", fake_run_fail)
    dst_fail = tmp_path / "cache" / "raw/gate_futures/2026-12-03/failed.tsv.zst"
    with pytest.raises(subprocess.CalledProcessError):
        backend.download_file("raw/gate_futures/2026-12-03/failed.tsv.zst", dst_fail)
    assert not dst_fail.exists()
    assert list(dst_fail.parent.glob("*.partial")) == []


def test_p1_prune_rechecks_local_sha256_blocks_same_size_tampering_and_skips_hash_within_retention(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Issue 1 regression: skip local hashing within retention, but re-verify SHA-256 right before unlink."""
    sealed = tmp_path / "sealed"
    base_mtime = 1_700_000_000.0
    dpath, _, _ = _create_sealed_partition(sealed, mtime_epoch_s=base_mtime)
    rel_data = dpath.relative_to(sealed).as_posix()

    backend = InstrumentedMockBackend()
    r1 = sync_sealed_directory(
        sealed, backend=backend, verify_remote=True, prune_older_than_hours=24.0, now_epoch_s=base_mtime + 3600.0
    )
    assert r1["status"] == "ok"

    # Spy on file_hashes: within 24h retention, repeat run MUST NOT re-hash the verified data file!
    hashed_paths: list[Path] = []
    orig_file_hashes = arch_mod.file_hashes

    def spy_file_hashes(path: Path) -> dict[str, Any]:
        hashed_paths.append(path)
        return orig_file_hashes(path)

    monkeypatch.setattr(arch_mod, "file_hashes", spy_file_hashes)
    r_within = sync_sealed_directory(
        sealed, backend=backend, verify_remote=True, prune_older_than_hours=24.0, now_epoch_s=base_mtime + 12 * 3600.0
    )
    assert r_within["status"] == "ok"
    assert dpath not in hashed_paths, "Within-retention verified files must NOT be re-hashed"

    # Tamper 1 byte in-place keeping EXACT same byte length and aged mtime!
    orig_bytes = dpath.read_bytes()
    tampered = bytearray(orig_bytes)
    tampered[-1] ^= 0xFF
    assert len(tampered) == len(orig_bytes)
    dpath.write_bytes(bytes(tampered))
    os.utime(dpath, (base_mtime, base_mtime))

    hashed_paths.clear()
    r_aged = sync_sealed_directory(
        sealed, backend=backend, verify_remote=True, prune_older_than_hours=24.0, now_epoch_s=base_mtime + 25 * 3600.0
    )
    assert r_aged["status"] == "blocked"
    assert dpath in hashed_paths, "Must re-hash local file right before pruning"
    assert dpath.is_file(), "Same-size tampered local file MUST NOT be deleted!"
    assert r_aged["pruned_local_files"] == []
    assert len(r_aged["conflicts"]) == 1
    assert r_aged["conflicts"][0]["action"] == "ISOLATED_LOCAL_SHA256_MISMATCH_BEFORE_PRUNE"
    assert r_aged["conflicts"][0]["expected_bytes"] == r_aged["conflicts"][0]["actual_bytes"]

    with ArchiveLedger(sealed / LEDGER_DB_FILENAME) as ledger:
        row = ledger.get_partition_by_rel_data(rel_data)
        assert row is not None
        assert row["state"] == STATE_VERIFIED
        assert row["has_conflict"] == 1


def test_p1_partial_failure_still_uploads_consistent_sqlite_snapshot(tmp_path: Path) -> None:
    """Issue 2 regression: when 3/4 exchanges succeed (and prune) while 1/4 fails, snapshot still uploads."""
    sealed = tmp_path / "sealed"
    base_mtime = 1_700_000_000.0
    exchanges = ["binance_futures", "bybit_linear", "bitget_v3", "gate_futures"]
    paths: dict[str, Path] = {}
    for ex in exchanges:
        dp, _, _ = _create_sealed_partition(sealed, exchange=ex, mtime_epoch_s=base_mtime)
        paths[ex] = dp

    # Inject remote hash conflict on gate_futures ONLY; the other 3 exchanges succeed & prune!
    backend = InstrumentedMockBackend()
    rel_gate = paths["gate_futures"].relative_to(sealed).as_posix()
    backend.store[rel_gate] = b"CONFLICTING_REMOTE_GATE_OBJECT"

    res = sync_sealed_directory(
        sealed,
        backend=backend,
        verify_remote=True,
        prune_older_than_hours=24.0,
        now_epoch_s=base_mtime + 25 * 3600.0,
    )
    assert res["status"] == "blocked", "Overall status must still be blocked due to gate_futures conflict"
    assert len(res["pruned_local_files"]) == 3
    assert paths["gate_futures"].is_file(), "Conflicted gate_futures local file must be preserved"
    for ok_ex in ("binance_futures", "bybit_linear", "bitget_v3"):
        assert not paths[ok_ex].exists()

    # CRITICAL: Consistent SQLite snapshot MUST still have been uploaded despite gate_futures conflict!
    snap_rel = res["snapshot_remote_path"]
    assert snap_rel is not None and snap_rel in backend.store
    assert f"{snap_rel}.sha256" in backend.store

    snap_copy = tmp_path / "partial_run_snapshot.sqlite"
    snap_copy.write_bytes(backend.store[snap_rel])
    conn = sqlite3.connect(str(snap_copy))
    try:
        assert conn.execute("PRAGMA integrity_check;").fetchone()[0] == "ok"
        states = dict(conn.execute("SELECT exchange, state FROM partitions").fetchall())
        assert states == {
            "binance_futures": STATE_PRUNED,
            "bybit_linear": STATE_PRUNED,
            "bitget_v3": STATE_PRUNED,
            "gate_futures": STATE_SEALED,
        }
    finally:
        conn.close()


def test_p1_prune_rechecks_remote_deleted_or_replaced_and_zero_calls_within_retention(
    tmp_path: Path,
) -> None:
    """Pre-prune remote check: zero remote calls within retention; remote deletion/replacement blocks pruning."""
    base_mtime = 1_700_000_000.0

    def _setup_verified(name: str) -> tuple[Path, Path, Path, str, str, InstrumentedMockBackend]:
        sealed = tmp_path / name
        dpath, mpath, _ = _create_sealed_partition(sealed, mtime_epoch_s=base_mtime)
        rel_data = dpath.relative_to(sealed).as_posix()
        rel_mf = mpath.relative_to(sealed).as_posix()
        backend = InstrumentedMockBackend()
        r_init = sync_sealed_directory(
            sealed,
            backend=backend,
            verify_remote=True,
            prune_older_than_hours=24.0,
            now_epoch_s=base_mtime + 3600.0,
        )
        assert r_init["status"] == "ok"
        assert dpath.is_file()
        backend.reset_counts()
        return sealed, dpath, mpath, rel_data, rel_mf, backend

    # Case A: Remote data deleted after VERIFIED -> within 24h makes ZERO remote calls; at 25h blocks & preserves!
    sealed_a, dpath_a, _, rel_data_a, _, backend_a = _setup_verified("remote_data_deleted")
    del backend_a.store[rel_data_a]

    r_within = sync_sealed_directory(
        sealed_a,
        backend=backend_a,
        verify_remote=True,
        prune_older_than_hours=24.0,
        now_epoch_s=base_mtime + 12 * 3600.0,
    )
    assert r_within["status"] == "ok"
    assert backend_a.stat_calls == [], "Within-retention verified partitions must make ZERO remote requests"
    assert backend_a.copy_calls == []
    assert dpath_a.is_file()

    r_aged_del = sync_sealed_directory(
        sealed_a,
        backend=backend_a,
        verify_remote=True,
        prune_older_than_hours=24.0,
        now_epoch_s=base_mtime + 25 * 3600.0,
    )
    assert r_aged_del["status"] == "blocked"
    assert dpath_a.is_file(), "Local data file MUST be preserved when remote data was deleted!"
    assert r_aged_del["pruned_local_files"] == []
    assert backend_a.stat_calls == [rel_data_a]
    assert len(r_aged_del["conflicts"]) == 1
    assert r_aged_del["conflicts"][0]["action"] == "ISOLATED_REMOTE_MISSING_BEFORE_PRUNE"
    assert r_aged_del["conflicts"][0]["target"] == "data"
    with ArchiveLedger(sealed_a / LEDGER_DB_FILENAME) as ledger:
        row_a = ledger.get_partition_by_rel_data(rel_data_a)
        assert row_a is not None
        assert row_a["state"] == STATE_VERIFIED
        assert row_a["has_conflict"] == 1

    # Subsequent run must continue surfacing blocked conflict without pruning or hammering remote
    backend_a.reset_counts()
    r_repeat_conflict = sync_sealed_directory(
        sealed_a,
        backend=backend_a,
        verify_remote=True,
        prune_older_than_hours=24.0,
        now_epoch_s=base_mtime + 26 * 3600.0,
    )
    assert r_repeat_conflict["status"] == "blocked"
    assert dpath_a.is_file()
    assert backend_a.stat_calls == []

    # Case B: Remote data replaced in-place with same-size corrupted bytes (hash mismatch)
    sealed_b, dpath_b, _, rel_data_b, _, backend_b = _setup_verified("remote_data_hash_drift")
    tampered_data = bytearray(backend_b.store[rel_data_b])
    tampered_data[-1] ^= 0xFF
    backend_b.store[rel_data_b] = bytes(tampered_data)

    r_aged_hash = sync_sealed_directory(
        sealed_b,
        backend=backend_b,
        verify_remote=True,
        prune_older_than_hours=24.0,
        now_epoch_s=base_mtime + 25 * 3600.0,
    )
    assert r_aged_hash["status"] == "blocked"
    assert dpath_b.is_file(), "Local data file MUST be preserved on remote hash drift!"
    assert r_aged_hash["conflicts"][0]["action"] == "ISOLATED_REMOTE_HASH_MISMATCH_BEFORE_PRUNE"
    assert r_aged_hash["conflicts"][0]["target"] == "data"

    # Case C: Remote data replaced with different size
    sealed_c, dpath_c, _, rel_data_c, _, backend_c = _setup_verified("remote_data_size_drift")
    backend_c.store[rel_data_c] = b"TRUNCATED"

    r_aged_size = sync_sealed_directory(
        sealed_c,
        backend=backend_c,
        verify_remote=True,
        prune_older_than_hours=24.0,
        now_epoch_s=base_mtime + 25 * 3600.0,
    )
    assert r_aged_size["status"] == "blocked"
    assert dpath_c.is_file(), "Local data file MUST be preserved on remote size drift!"
    assert r_aged_size["conflicts"][0]["action"] == "ISOLATED_REMOTE_SIZE_MISMATCH_BEFORE_PRUNE"
    assert r_aged_size["conflicts"][0]["target"] == "data"

    # Case D: Remote data replaced with IDENTICAL bytes but DIFFERENT object ID (remote_id mismatch!)
    sealed_d, dpath_d, _, rel_data_d, _, backend_d = _setup_verified("remote_data_id_replaced")
    backend_d.id_overrides[rel_data_d] = "gd:replaced_object_identity"

    r_aged_id = sync_sealed_directory(
        sealed_d,
        backend=backend_d,
        verify_remote=True,
        prune_older_than_hours=24.0,
        now_epoch_s=base_mtime + 25 * 3600.0,
    )
    assert r_aged_id["status"] == "blocked"
    assert dpath_d.is_file(), "Local data file MUST be preserved when remote object ID changed!"
    assert r_aged_id["conflicts"][0]["action"] == "ISOLATED_REMOTE_ID_MISMATCH_BEFORE_PRUNE"
    assert r_aged_id["conflicts"][0]["target"] == "data"
    assert r_aged_id["conflicts"][0]["actual_remote_id"] == "gd:replaced_object_identity"

    # Case E: Remote manifest deleted while remote data stays intact
    sealed_e, dpath_e, _, rel_data_e, rel_mf_e, backend_e = _setup_verified("remote_manifest_deleted")
    del backend_e.store[rel_mf_e]

    r_mf_del = sync_sealed_directory(
        sealed_e,
        backend=backend_e,
        verify_remote=True,
        prune_older_than_hours=24.0,
        now_epoch_s=base_mtime + 25 * 3600.0,
    )
    assert r_mf_del["status"] == "blocked"
    assert dpath_e.is_file(), "Local data file MUST be preserved when remote manifest is missing!"
    assert r_mf_del["conflicts"][0]["action"] == "ISOLATED_REMOTE_MISSING_BEFORE_PRUNE"
    assert r_mf_del["conflicts"][0]["target"] == "manifest"
    assert r_mf_del["conflicts"][0]["remote_path"] == rel_mf_e

    # Case F: Remote manifest replaced with different content/ID while remote data stays intact
    sealed_f, dpath_f, _, _, rel_mf_f, backend_f = _setup_verified("remote_manifest_replaced")
    backend_f.store[rel_mf_f] = b'{"replaced":true}\n'

    r_mf_rep = sync_sealed_directory(
        sealed_f,
        backend=backend_f,
        verify_remote=True,
        prune_older_than_hours=24.0,
        now_epoch_s=base_mtime + 25 * 3600.0,
    )
    assert r_mf_rep["status"] == "blocked"
    assert dpath_f.is_file(), "Local data file MUST be preserved when remote manifest was replaced!"
    assert r_mf_rep["conflicts"][0]["target"] == "manifest"
    assert r_mf_rep["conflicts"][0]["action"] in (
        "ISOLATED_REMOTE_SIZE_MISMATCH_BEFORE_PRUNE",
        "ISOLATED_REMOTE_HASH_MISMATCH_BEFORE_PRUNE",
    )


def test_p1_prune_preserves_local_on_remote_api_outage_and_recovers_safely(tmp_path: Path) -> None:
    """Network/API outage at prune time preserves local file; recovery prunes if intact or blocks if ID changed."""
    base_mtime = 1_700_000_000.0

    # Sub-case 1: Outage at 25h -> preserves local file -> recovers & prunes cleanly at 26h when remote intact
    sealed1 = tmp_path / "outage_recover_ok"
    dpath1, mpath1, _ = _create_sealed_partition(sealed1, mtime_epoch_s=base_mtime)
    rel_data1 = dpath1.relative_to(sealed1).as_posix()
    rel_mf1 = mpath1.relative_to(sealed1).as_posix()

    backend1 = InstrumentedMockBackend()
    assert (
        sync_sealed_directory(
            sealed1,
            backend=backend1,
            verify_remote=True,
            prune_older_than_hours=24.0,
            now_epoch_s=base_mtime + 3600.0,
        )["status"]
        == "ok"
    )

    backend1.reset_counts()
    backend1.fail_on_stat = True
    r_outage = sync_sealed_directory(
        sealed1,
        backend=backend1,
        verify_remote=True,
        prune_older_than_hours=24.0,
        now_epoch_s=base_mtime + 25 * 3600.0,
    )
    assert r_outage["status"] == "blocked"
    assert dpath1.is_file(), "Local data file MUST be preserved when remote API is unavailable!"
    assert r_outage["pruned_local_files"] == []
    with ArchiveLedger(sealed1 / LEDGER_DB_FILENAME) as ledger:
        row1 = ledger.get_partition_by_rel_data(rel_data1)
        assert row1 is not None
        assert row1["state"] == STATE_VERIFIED
        assert row1["has_conflict"] == 0
        assert row1["last_error_code"] == "PRE_PRUNE_REMOTE_CHECK_FAILED"

    # Network recovers -> re-verifies remote data + manifest once (no double stat) and prunes cleanly
    backend1.reset_counts()
    backend1.fail_on_stat = False
    r_recover = sync_sealed_directory(
        sealed1,
        backend=backend1,
        verify_remote=True,
        prune_older_than_hours=24.0,
        now_epoch_s=base_mtime + 26 * 3600.0,
    )
    assert r_recover["status"] == "ok"
    assert r_recover["pruned_local_files"] == [rel_data1]
    assert not dpath1.exists()
    assert mpath1.is_file()
    assert backend1.stat_calls == [rel_data1, rel_mf1]
    with ArchiveLedger(sealed1 / LEDGER_DB_FILENAME) as ledger:
        row1_after = ledger.get_partition_by_rel_data(rel_data1)
        assert row1_after is not None
        assert row1_after["state"] == STATE_PRUNED
        pruned_events = [e for e in ledger.list_events(rel_data1) if e["event_type"] == "PRUNED_OK"]
        assert len(pruned_events) == 1
        detail = json.loads(pruned_events[0]["detail_json"])
        assert detail["pre_prune_remote_verified"] is True
        assert detail["remote_data_id"] == row1_after["remote_data_id"]
        assert detail["remote_manifest_id"] == row1_after["remote_manifest_id"]

    # Sub-case 2: Outage at 25h, AND remote object ID changed before network recovery -> MUST block, NOT overwrite ID!
    sealed2 = tmp_path / "outage_then_id_swapped"
    dpath2, _, _ = _create_sealed_partition(sealed2, mtime_epoch_s=base_mtime)
    rel_data2 = dpath2.relative_to(sealed2).as_posix()

    backend2 = InstrumentedMockBackend()
    assert (
        sync_sealed_directory(
            sealed2,
            backend=backend2,
            verify_remote=True,
            prune_older_than_hours=24.0,
            now_epoch_s=base_mtime + 3600.0,
        )["status"]
        == "ok"
    )
    with ArchiveLedger(sealed2 / LEDGER_DB_FILENAME) as ledger:
        orig_remote_id = ledger.get_partition_by_rel_data(rel_data2)["remote_data_id"]

    backend2.fail_on_stat = True
    r_outage2 = sync_sealed_directory(
        sealed2,
        backend=backend2,
        verify_remote=True,
        prune_older_than_hours=24.0,
        now_epoch_s=base_mtime + 25 * 3600.0,
    )
    assert r_outage2["status"] == "blocked"
    assert dpath2.is_file()

    backend2.fail_on_stat = False
    backend2.id_overrides[rel_data2] = "gd:swapped_during_outage"
    r_after_swap = sync_sealed_directory(
        sealed2,
        backend=backend2,
        verify_remote=True,
        prune_older_than_hours=24.0,
        now_epoch_s=base_mtime + 26 * 3600.0,
    )
    assert r_after_swap["status"] == "blocked"
    assert dpath2.is_file(), "Local file MUST NOT be pruned when remote object ID was replaced during outage!"
    assert r_after_swap["conflicts"][0]["action"] == "ISOLATED_REMOTE_ID_MISMATCH_BEFORE_PRUNE"
    with ArchiveLedger(sealed2 / LEDGER_DB_FILENAME) as ledger:
        row2 = ledger.get_partition_by_rel_data(rel_data2)
        assert row2 is not None
        assert row2["state"] == STATE_VERIFIED
        assert row2["has_conflict"] == 1
        assert row2["remote_data_id"] == orig_remote_id, "Original verified remote_data_id must NOT be overwritten!"
