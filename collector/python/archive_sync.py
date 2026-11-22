#!/usr/bin/env python3
"""Incremental SQLite-backed archive ledger, Google Drive Pro (rclone) verifier, and safe rolling pruner.

Implements Phase P1 reliability upgrade over SPEC_v1.4 Section 4.2:
1. Persistent SQLite archive ledger (`archive_ledger.sqlite`, WAL mode, `synchronous=FULL`) with
   composite partition primary key `(exchange, date_utc, hour_utc, boot_id, partition_id)` so
   multiple immutable partitions within the same UTC hour are tracked independently.
2. Strict state machine: `SEALED -> VERIFIED -> PRUNED` with append-only `archive_events` audit
   trail. Failures and conflicts are recorded as auxiliary error/conflict fields and events and
   NEVER masquerade as `VERIFIED` or `PRUNED`.
3. True incremental execution:
   - Already-`PRUNED` partitions (missing local `.tsv.zst`) are skipped with zero disk reads and
     zero remote API calls.
   - Already-`VERIFIED` partitions skip redundant decompression, hashing, and remote stat calls
     until eligible for retention pruning.
   - Only newly sealed, previously failed/unverified, or pruning-eligible partitions are acted on.
4. Dual remote verification (both data `.tsv.zst` AND `.manifest.json` must be verified in remote
   object ID, byte size, and content hash/readback) BEFORE transitioning to `VERIFIED` or pruning.
5. Crash-safe two-phase pruning (`PRUNE_INTENT` -> unlink `.tsv.zst` -> commit `PRUNED`) with
   remote-proof recovery if interrupted after unlink, and fail-closed critical error reporting on
   unexpected local data disappearance without proof.
6. Process mutual exclusion via non-blocking `fcntl.flock` (`.archive_sync.lock`) preventing
   concurrent overlapping archive runs.
7. Consistent online SQLite backup (`sqlite3.Connection.backup`) uploaded as a timestamped snapshot
   to remote `meta/archive_ledger/` (never raw-copying live WAL/SHM database files).
8. Idempotent migration from legacy `verified_archive_ledger.jsonl` and backward-compatible atomic
   JSONL projection export plus verified remote restore helper for `PRUNED` partitions in QA.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import sqlite3
import subprocess
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Protocol

import zstandard as zstd

# Official rclone exit codes indicating genuine absence (vs API/network/auth failure):
#   3 = Directory not found
#   4 = File not found
RCLONE_NOT_FOUND_EXIT_CODES = {3, 4}

LEDGER_DB_FILENAME = "archive_ledger.sqlite"
LEGACY_LEDGER_JSONL_FILENAME = "verified_archive_ledger.jsonl"
PROJECTION_LEDGER_JSONL_FILENAME = "verified_archive_ledger.compat.jsonl"
ARCHIVE_MANIFEST_INDEX_RELPATH = "meta/archive_manifest_index.json"
ARCHIVE_LOCK_FILENAME = ".archive_sync.lock"
LEDGER_SCHEMA_VERSION = "2"

STATE_SEALED = "SEALED"
STATE_VERIFIED = "VERIFIED"
STATE_PRUNED = "PRUNED"
VALID_STATES = {STATE_SEALED, STATE_VERIFIED, STATE_PRUNED}

# Files under meta/ that must NEVER be raw-copied to remote storage
_META_SYNC_EXCLUDED_SUFFIXES = (".sqlite-wal", ".sqlite-shm", ".sqlite-journal", ".lock", ".partial")


@dataclass
class RemoteObjectInfo:
    exists: bool
    remote_id: str | None = None
    size: int | None = None
    md5: str | None = None
    sha256: str | None = None


class ArchiveBackend(Protocol):
    def stat_remote(self, remote_rel_path: str) -> RemoteObjectInfo: ...
    def copy_file(self, local_path: Path, remote_rel_path: str, *, immutable: bool = True) -> None: ...
    def readback_sha256(self, remote_rel_path: str) -> str: ...
    def download_file(self, remote_rel_path: str, local_dest: Path) -> None: ...


class RcloneBackend:
    """Real rclone backend targeting single Google Drive Pro (`gdrive_main:tokyo_l2_data`)."""

    def __init__(
        self,
        remote_root: str,
        *,
        proxy_url: str | None = None,
        proxy_mode: str | None = None,
    ):
        self.remote_root = remote_root.rstrip("/")
        self.proxy_url = proxy_url
        self.proxy_mode = proxy_mode
        self._proxy_resolution: dict[str, Any] | None = None
        self._child_env: dict[str, str] | None = None

    def resolve_proxy(self, *, force: bool = False) -> dict[str, Any]:
        """Resolve runtime proxy config and enforce `required` vs `fallback_direct` fail-closed semantics."""
        if self._proxy_resolution is not None and not force:
            return self._proxy_resolution
        from lightsail_proxy import build_rclone_subprocess_env, resolve_rclone_proxy_runtime

        res = resolve_rclone_proxy_runtime(
            proxy_url=self.proxy_url,
            proxy_mode=self.proxy_mode,
        )
        self._proxy_resolution = res.to_dict()
        self._child_env = build_rclone_subprocess_env(
            res.active_proxy_url,
            clear_proxy=res.fallback_used,
        )
        return self._proxy_resolution

    def _subprocess_kwargs(self) -> dict[str, Any]:
        self.resolve_proxy()
        if self._child_env is None:
            return {}
        return {"env": self._child_env}

    def _full(self, rel: str) -> str:
        return f"{self.remote_root}/{rel.lstrip('/')}"

    def stat_remote(self, remote_rel_path: str) -> RemoteObjectInfo:
        target = self._full(remote_rel_path)
        proc = subprocess.run(
            ["rclone", "lsjson", "--hash", "--files-only", target],
            capture_output=True,
            text=True,
            timeout=60,
            **self._subprocess_kwargs(),
        )
        if proc.returncode != 0:
            if proc.returncode in RCLONE_NOT_FOUND_EXIT_CODES:
                return RemoteObjectInfo(exists=False)
            # Fail closed on ANY transient API/network/rate-limit/auth error!
            # Never misclassify an unreachable remote as "does not exist".
            stderr_msg = (proc.stderr or "").strip()
            raise RuntimeError(
                f"rclone stat_remote failed (rc={proc.returncode}) for {target}: {stderr_msg or 'unknown error'}"
            )
        items = json.loads(proc.stdout or "[]")
        if not items:
            return RemoteObjectInfo(exists=False)
        item = items[0]
        hashes = item.get("Hashes") or {}
        return RemoteObjectInfo(
            exists=True,
            remote_id=item.get("ID"),
            size=int(item["Size"]) if "Size" in item else None,
            md5=hashes.get("md5") or hashes.get("MD5"),
            sha256=hashes.get("sha256") or hashes.get("SHA-256"),
        )

    def copy_file(self, local_path: Path, remote_rel_path: str, *, immutable: bool = True) -> None:
        parent = str(Path(remote_rel_path).parent).replace("\\", "/")
        dest_dir = self._full(parent)
        cmd = ["rclone", "copy", "--no-traverse"]
        if immutable:
            # Defense-in-depth: refuse at rclone layer to overwrite any differing remote object
            cmd.append("--immutable")
        cmd.extend([str(local_path), dest_dir])
        subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            timeout=600,
            **self._subprocess_kwargs(),
        )

    def download_file(self, remote_rel_path: str, local_dest: Path) -> None:
        target = self._full(remote_rel_path)
        local_dest.parent.mkdir(parents=True, exist_ok=True)
        tmp_dest = local_dest.with_name(f"{local_dest.name}.download.{os.getpid()}.partial")
        try:
            subprocess.run(
                ["rclone", "copyto", target, str(tmp_dest)],
                check=True,
                capture_output=True,
                text=True,
                timeout=600,
                **self._subprocess_kwargs(),
            )
            os.replace(tmp_dest, local_dest)
        finally:
            if tmp_dest.exists():
                try:
                    tmp_dest.unlink()
                except OSError:
                    pass

    def readback_sha256(self, remote_rel_path: str) -> str:
        with tempfile.TemporaryDirectory(prefix="tokyo_l2_readback_") as tmp:
            dst = Path(tmp) / Path(remote_rel_path).name
            self.download_file(remote_rel_path, dst)
            return file_hashes(dst)["sha256"]


class ArchiveLockBusyError(RuntimeError):
    """Raised when another archive_sync process holds the exclusive directory lock."""


class ArchiveDirectoryLock:
    """Non-blocking POSIX flock mutual exclusion guard for a sealed_root directory."""

    def __init__(self, lock_path: Path):
        self.lock_path = lock_path
        self._fd: int | None = None

    def acquire(self) -> None:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(self.lock_path), os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(fd)
            raise ArchiveLockBusyError(f"Another archive process holds lock {self.lock_path}") from exc
        self._fd = fd
        os.ftruncate(fd, 0)
        os.write(fd, f"pid={os.getpid()} acquired_utc={_iso_utc(time.time())}\n".encode("utf-8"))

    def release(self) -> None:
        if self._fd is not None:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
            finally:
                os.close(self._fd)
                self._fd = None

    def __enter__(self) -> ArchiveDirectoryLock:
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.release()


def _iso_utc(epoch_s: float) -> str:
    return datetime.fromtimestamp(epoch_s, tz=timezone.utc).isoformat()


def file_hashes(path: Path) -> dict[str, Any]:
    h_sha = hashlib.sha256()
    h_md5 = hashlib.md5()
    size = 0
    with path.open("rb") as f:
        while True:
            chunk = f.read(1 << 20)
            if not chunk:
                break
            size += len(chunk)
            h_sha.update(chunk)
            h_md5.update(chunk)
    return {"bytes": size, "sha256": h_sha.hexdigest(), "md5": h_md5.hexdigest()}


def verify_zstd_integrity(path: Path) -> None:
    dctx = zstd.ZstdDecompressor()
    with path.open("rb") as f:
        with dctx.stream_reader(f) as reader:
            while True:
                buf = reader.read(1 << 18)
                if not buf:
                    break


def _scan_market_data_timestamps(path: Path) -> dict[str, Any]:
    """Decompress a `.tsv.zst` partition and extract timestamp bounds and internal gaps
    strictly from real market data rows (`DATA` / `DATA_ESCAPED`), ignoring `META` control records.
    """
    import io

    dctx = zstd.ZstdDecompressor()
    parsed_tsv = False
    data_count = 0
    meta_count = 0
    data_start_ns: int | None = None
    data_end_ns: int | None = None
    prev_data_ns: int | None = None
    max_internal_gap_ns = 0

    with path.open("rb") as f:
        with dctx.stream_reader(f) as reader:
            text_stream = io.TextIOWrapper(reader, encoding="utf-8", errors="replace")
            for raw_line in text_stream:
                line = raw_line.rstrip("\r\n")
                if not line:
                    continue
                parts = line.split("\t", 6)
                if len(parts) != 7:
                    continue
                try:
                    wall_ns = int(parts[0])
                except ValueError:
                    continue
                kind = parts[5]
                if kind in ("DATA", "DATA_ESCAPED"):
                    parsed_tsv = True
                    data_count += 1
                    data_start_ns = wall_ns if data_start_ns is None else min(data_start_ns, wall_ns)
                    data_end_ns = wall_ns if data_end_ns is None else max(data_end_ns, wall_ns)
                    if prev_data_ns is not None and wall_ns > prev_data_ns:
                        max_internal_gap_ns = max(max_internal_gap_ns, wall_ns - prev_data_ns)
                    prev_data_ns = wall_ns
                elif kind == "META":
                    parsed_tsv = True
                    meta_count += 1

    return {
        "parsed_tsv": parsed_tsv,
        "data_lines": data_count,
        "meta_lines": meta_count,
        "start_wall_ns": data_start_ns,
        "end_wall_ns": data_end_ns,
        "max_internal_gap_ns": max_internal_gap_ns,
    }


def verify_local_partition(data_path: Path, manifest_path: Path) -> dict[str, Any]:
    if not data_path.is_file():
        raise FileNotFoundError(f"Missing sealed data file: {data_path}")
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Missing manifest file: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    verify_zstd_integrity(data_path)
    market_data = _scan_market_data_timestamps(data_path)
    hashes = file_hashes(data_path)
    if hashes["bytes"] != int(manifest["bytes"]):
        raise ValueError(f"Byte mismatch for {data_path.name}: local={hashes['bytes']} manifest={manifest['bytes']}")
    if hashes["sha256"] != manifest["sha256"]:
        raise ValueError(f"SHA-256 mismatch for {data_path.name}: local={hashes['sha256']} manifest={manifest['sha256']}")
    return {"manifest": manifest, "hashes": hashes, "market_data": market_data}


def _hashes_match(local_hashes: dict[str, Any], remote: RemoteObjectInfo) -> bool | None:
    """Return True if remote hash matches, False if mismatch, None if remote exposes no hash."""
    if remote.sha256:
        return remote.sha256.lower() == local_hashes["sha256"].lower()
    if remote.md5:
        return remote.md5.lower() == local_hashes["md5"].lower()
    return None


def _derive_partition_identity(
    sealed_root: Path,
    manifest_path: Path,
    manifest: dict[str, Any],
) -> dict[str, Any]:
    """Extract canonical partition primary key, planned window metadata, and relative paths."""
    from data_coverage import normalize_manifest_window

    file_name = manifest.get("file_name")
    if isinstance(file_name, str) and file_name and Path(file_name).name == file_name and file_name.endswith(".tsv.zst"):
        data_path = manifest_path.with_name(file_name)
    else:
        data_name = manifest_path.name.replace(".manifest.json", ".tsv.zst")
        data_path = manifest_path.with_name(data_name)

    stem = manifest_path.name
    if stem.endswith(".manifest.json"):
        stem = stem[: -len(".manifest.json")]
    norm = normalize_manifest_window(manifest, fallback_file_name=data_path.name)
    partition_id = str(manifest.get("partition_id") or norm["segment_id"] or stem)
    data_lines = int(manifest.get("data_lines") or 0) + int(manifest.get("data_escaped_lines") or 0)
    local_loss_lines = int(manifest.get("local_loss_lines") or 0)
    gap_start_lines = int(manifest.get("gap_start_lines") or 0)
    late_frames_rejected = int(manifest.get("late_frames_rejected") or 0)

    return {
        "exchange": norm["exchange"],
        "date_utc": norm["date_utc"],
        "hour_utc": norm["hour_utc"],
        "minute_utc": norm["minute_utc"],
        "partition_minutes": norm["partition_minutes"],
        "window_id": norm["window_id"],
        "planned_start_wall_ns": norm["planned_start_wall_ns"],
        "planned_end_wall_ns": norm["planned_end_wall_ns"],
        "collection_run_id": norm["run_id"],
        "segment_id": norm["segment_id"],
        "boot_id": norm["boot_id"],
        "partition_id": partition_id,
        "is_startup_stub": norm["is_startup_stub"],
        "settled_by_timer": norm["settled_by_timer"],
        "coverage_incomplete": norm["coverage_incomplete"],
        "first_rx_seq": manifest.get("first_rx_seq"),
        "last_rx_seq": manifest.get("last_rx_seq"),
        "start_wall_ns": manifest.get("start_wall_ns"),
        "end_wall_ns": manifest.get("end_wall_ns"),
        "data_lines": data_lines,
        "local_loss_lines": local_loss_lines,
        "gap_start_lines": gap_start_lines,
        "late_frames_rejected": late_frames_rejected,
        "data_path": data_path,
        "manifest_path": manifest_path,
        "rel_data_path": data_path.relative_to(sealed_root).as_posix(),
        "rel_manifest_path": manifest_path.relative_to(sealed_root).as_posix(),
    }


_LEGACY_PATH_RE = re.compile(
    r"^raw/(?P<exchange>[^/]+)/(?P<date_utc>\d{4}-\d{2}-\d{2})/(?P<stem>.+)\.tsv\.zst$"
)


def _infer_identity_from_rel_path(rel_data_path: str) -> dict[str, str]:
    m = _LEGACY_PATH_RE.match(rel_data_path)
    if m:
        stem = m.group("stem")
        # Extract hour from ..._YYYYMMDD_HH or ..._YYYYMMDD_HHMM_p... or ..._YYYYMMDD_HH_<suffix>
        hm = re.search(r"_(\d{8})_(\d{2})(?:$|_|\d{2}_p)", stem)
        hour_utc = hm.group(2) if hm else "00"
        return {
            "exchange": m.group("exchange"),
            "date_utc": m.group("date_utc"),
            "hour_utc": hour_utc,
            "boot_id": "legacy_migrated",
            "partition_id": stem,
        }
    stem = Path(rel_data_path).name.replace(".tsv.zst", "")
    return {
        "exchange": "unknown",
        "date_utc": "1970-01-01",
        "hour_utc": "00",
        "boot_id": "legacy_migrated",
        "partition_id": stem,
    }


_PARTITION_EXTRA_COLUMNS: tuple[tuple[str, str], ...] = (
    ("minute_utc", "TEXT NOT NULL DEFAULT '00'"),
    ("partition_minutes", "INTEGER NOT NULL DEFAULT 60"),
    ("window_id", "TEXT"),
    ("planned_start_wall_ns", "INTEGER"),
    ("planned_end_wall_ns", "INTEGER"),
    ("collection_run_id", "TEXT"),
    ("segment_id", "TEXT"),
    ("is_startup_stub", "INTEGER NOT NULL DEFAULT 0"),
    ("settled_by_timer", "INTEGER NOT NULL DEFAULT 0"),
    ("coverage_incomplete", "INTEGER NOT NULL DEFAULT 0"),
    ("first_rx_seq", "INTEGER"),
    ("last_rx_seq", "INTEGER"),
    ("start_wall_ns", "INTEGER"),
    ("end_wall_ns", "INTEGER"),
    ("data_lines", "INTEGER NOT NULL DEFAULT 0"),
    ("local_loss_lines", "INTEGER NOT NULL DEFAULT 0"),
    ("gap_start_lines", "INTEGER NOT NULL DEFAULT 0"),
    ("late_frames_rejected", "INTEGER NOT NULL DEFAULT 0"),
)


class ArchiveLedger:
    """Durable SQLite WAL incremental archive state store & event journal."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path), timeout=10.0)
        self.conn.row_factory = sqlite3.Row
        self._configure_connection()
        self._init_schema()

    def _configure_connection(self) -> None:
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=FULL;")
        self.conn.execute("PRAGMA foreign_keys=ON;")
        self.conn.execute("PRAGMA busy_timeout=5000;")

    def _migrate_window_columns(self) -> None:
        """Idempotently add Stage 2 window/segment columns to existing SQLite ledgers without data loss."""
        existing = {
            str(row["name"])
            for row in self.conn.execute("PRAGMA table_info(partitions)").fetchall()
        }
        for col_name, col_ddl in _PARTITION_EXTRA_COLUMNS:
            if col_name not in existing:
                self.conn.execute(f"ALTER TABLE partitions ADD COLUMN {col_name} {col_ddl}")
        self.conn.execute(
            "UPDATE partitions SET window_id = date_utc || 'T' || hour_utc || ':00Z' WHERE window_id IS NULL"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_partitions_window ON partitions(window_id, exchange)"
        )

    def _init_schema(self) -> None:
        with self.conn:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS schema_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS partitions (
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

                CREATE INDEX IF NOT EXISTS idx_partitions_state
                    ON partitions(state);

                CREATE TABLE IF NOT EXISTS archive_events (
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

                CREATE INDEX IF NOT EXISTS idx_archive_events_rel_path
                    ON archive_events(rel_data_path, event_type);

                CREATE TABLE IF NOT EXISTS meta_sync_state (
                    rel_path TEXT PRIMARY KEY,
                    size_bytes INTEGER NOT NULL,
                    sha256 TEXT NOT NULL,
                    synced_at_utc TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS expected_windows (
                    window_key TEXT PRIMARY KEY,
                    window_id TEXT NOT NULL,
                    window_span_id TEXT NOT NULL,
                    date_utc TEXT NOT NULL,
                    hour_utc TEXT NOT NULL,
                    minute_utc TEXT NOT NULL,
                    partition_minutes INTEGER NOT NULL,
                    planned_start_wall_ns INTEGER NOT NULL,
                    planned_end_wall_ns INTEGER NOT NULL,
                    collection_run_id TEXT,
                    expected_exchanges_json TEXT NOT NULL,
                    file_verified_exchanges_json TEXT NOT NULL,
                    covered_exchanges_json TEXT NOT NULL,
                    missing_exchanges_json TEXT NOT NULL,
                    incomplete_coverage_exchanges_json TEXT NOT NULL,
                    file_verification_status TEXT NOT NULL,
                    window_coverage_status TEXT NOT NULL,
                    window_status TEXT NOT NULL,
                    detail_json TEXT,
                    first_checked_utc TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_expected_windows_status
                    ON expected_windows(window_status, partition_minutes);

                CREATE TABLE IF NOT EXISTS archive_market_scan_stats (
                    data_sha256 TEXT PRIMARY KEY,
                    stats_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS qa_dispatch_outbox (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    target_window_utc TEXT NOT NULL,
                    archive_revision TEXT NOT NULL,
                    partition_minutes INTEGER NOT NULL,
                    window_span_id TEXT,
                    index_sha256 TEXT NOT NULL,
                    index_synced_at_utc TEXT NOT NULL,
                    manifest_count INTEGER NOT NULL,
                    manifests_json TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (state IN ('PENDING', 'ACCEPTED', 'QA_COMPLETED', 'FAILED_PERMANENT')),
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at_epoch_s REAL NOT NULL DEFAULT 0,
                    first_enqueued_at_utc TEXT NOT NULL,
                    last_attempt_at_utc TEXT,
                    api_accepted_at_utc TEXT,
                    workflow_run_id INTEGER,
                    workflow_run_url TEXT,
                    workflow_run_started_at_utc TEXT,
                    qa_completed_at_utc TEXT,
                    qa_result_status TEXT,
                    last_http_status INTEGER,
                    last_error_code TEXT,
                    last_error_message TEXT,
                    discovery_source TEXT NOT NULL DEFAULT 'archive_sync',
                    updated_at_utc TEXT NOT NULL,
                    UNIQUE (run_id, target_window_utc, archive_revision)
                );

                CREATE INDEX IF NOT EXISTS idx_qa_dispatch_outbox_state
                    ON qa_dispatch_outbox(state, next_attempt_at_epoch_s);
                """
            )
            self._migrate_window_columns()
            self.conn.execute(
                "INSERT INTO schema_meta(key, value) VALUES('schema_version', ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (LEDGER_SCHEMA_VERSION,),
            )

    def upsert_expected_window(
        self,
        *,
        window_key: str,
        win_desc: dict[str, Any],
        collection_run_id: str | None,
        expected_exchanges: list[str],
        file_verified_exchanges: list[str],
        covered_exchanges: list[str],
        missing_exchanges: list[str],
        incomplete_coverage_exchanges: list[str],
        file_verification_status: str,
        window_coverage_status: str,
        window_status: str,
        detail: dict[str, Any],
        now_iso: str,
    ) -> None:
        span_id = str(
            win_desc.get("window_span_id")
            or f"{win_desc['window_id']}__{win_desc['partition_minutes']}m"
        )
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO expected_windows (
                    window_key, window_id, window_span_id, date_utc, hour_utc, minute_utc,
                    partition_minutes, planned_start_wall_ns, planned_end_wall_ns,
                    collection_run_id, expected_exchanges_json, file_verified_exchanges_json,
                    covered_exchanges_json, missing_exchanges_json, incomplete_coverage_exchanges_json,
                    file_verification_status, window_coverage_status, window_status,
                    detail_json, first_checked_utc, updated_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(window_key) DO UPDATE SET
                    file_verified_exchanges_json = excluded.file_verified_exchanges_json,
                    covered_exchanges_json = excluded.covered_exchanges_json,
                    missing_exchanges_json = excluded.missing_exchanges_json,
                    incomplete_coverage_exchanges_json = excluded.incomplete_coverage_exchanges_json,
                    file_verification_status = excluded.file_verification_status,
                    window_coverage_status = excluded.window_coverage_status,
                    window_status = excluded.window_status,
                    detail_json = excluded.detail_json,
                    updated_at_utc = excluded.updated_at_utc
                """,
                (
                    window_key,
                    str(win_desc["window_id"]),
                    span_id,
                    str(win_desc["date_utc"]),
                    str(win_desc["hour_utc"]),
                    str(win_desc["minute_utc"]),
                    int(win_desc["partition_minutes"]),
                    int(win_desc["planned_start_wall_ns"]),
                    int(win_desc["planned_end_wall_ns"]),
                    collection_run_id,
                    json.dumps(expected_exchanges),
                    json.dumps(file_verified_exchanges),
                    json.dumps(covered_exchanges),
                    json.dumps(missing_exchanges),
                    json.dumps(incomplete_coverage_exchanges),
                    file_verification_status,
                    window_coverage_status,
                    window_status,
                    json.dumps(detail, sort_keys=True),
                    now_iso,
                    now_iso,
                ),
            )

    def list_expected_windows(
        self,
        *,
        partition_minutes: int | None = None,
        collection_run_id: str | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if partition_minutes is not None:
            clauses.append("partition_minutes = ?")
            params.append(int(partition_minutes))
        if collection_run_id is not None:
            clauses.append("collection_run_id = ?")
            params.append(str(collection_run_id))
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self.conn.execute(
            f"SELECT * FROM expected_windows {where_sql} ORDER BY planned_start_wall_ns ASC, window_key ASC",
            tuple(params),
        ).fetchall()
        return [dict(r) for r in rows]

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None  # type: ignore[assignment]

    def __enter__(self) -> ArchiveLedger:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()

    def get_meta(self, key: str) -> str | None:
        row = self.conn.execute("SELECT value FROM schema_meta WHERE key = ?", (key,)).fetchone()
        return str(row["value"]) if row else None

    def set_meta(self, key: str, value: str) -> None:
        with self.conn:
            self.conn.execute(
                "INSERT INTO schema_meta(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )

    def get_partition_by_pk(
        self,
        exchange: str,
        date_utc: str,
        hour_utc: str,
        boot_id: str,
        partition_id: str,
    ) -> dict[str, Any] | None:
        row = self.conn.execute(
            """
            SELECT * FROM partitions
            WHERE exchange = ? AND date_utc = ? AND hour_utc = ? AND boot_id = ? AND partition_id = ?
            """,
            (exchange, date_utc, hour_utc, boot_id, partition_id),
        ).fetchone()
        return dict(row) if row else None

    def get_partition_by_rel_data(self, rel_data_path: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT * FROM partitions WHERE rel_data_path = ?",
            (rel_data_path,),
        ).fetchone()
        return dict(row) if row else None

    def list_partitions(self, states: tuple[str, ...] | None = None) -> list[dict[str, Any]]:
        if states:
            placeholders = ",".join("?" for _ in states)
            rows = self.conn.execute(
                f"SELECT * FROM partitions WHERE state IN ({placeholders}) "
                "ORDER BY date_utc, hour_utc, exchange, boot_id, partition_id",
                states,
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM partitions ORDER BY date_utc, hour_utc, exchange, boot_id, partition_id"
            ).fetchall()
        return [dict(r) for r in rows]

    def record_event(
        self,
        *,
        ts_utc: str,
        run_id: str,
        event_type: str,
        exchange: str | None = None,
        date_utc: str | None = None,
        hour_utc: str | None = None,
        boot_id: str | None = None,
        partition_id: str | None = None,
        rel_data_path: str | None = None,
        from_state: str | None = None,
        to_state: str | None = None,
        detail: dict[str, Any] | None = None,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO archive_events (
                    ts_utc, run_id, exchange, date_utc, hour_utc, boot_id, partition_id,
                    rel_data_path, event_type, from_state, to_state, detail_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts_utc,
                    run_id,
                    exchange,
                    date_utc,
                    hour_utc,
                    boot_id,
                    partition_id,
                    rel_data_path,
                    event_type,
                    from_state,
                    to_state,
                    json.dumps(detail, sort_keys=True) if detail is not None else None,
                ),
            )

    def has_pending_prune_intent(self, rel_data_path: str) -> bool:
        """Return True if the latest prune-lifecycle event for this partition is PRUNE_INTENT."""
        row = self.conn.execute(
            """
            SELECT event_type FROM archive_events
            WHERE rel_data_path = ?
              AND event_type IN ('PRUNE_INTENT', 'PRUNED_OK', 'PRUNE_RECOVERY_OK', 'PRUNE_ABORTED')
            ORDER BY id DESC LIMIT 1
            """,
            (rel_data_path,),
        ).fetchone()
        return bool(row and row["event_type"] == "PRUNE_INTENT")

    def list_events(self, rel_data_path: str | None = None) -> list[dict[str, Any]]:
        if rel_data_path is None:
            rows = self.conn.execute("SELECT * FROM archive_events ORDER BY id ASC").fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM archive_events WHERE rel_data_path = ? ORDER BY id ASC",
                (rel_data_path,),
            ).fetchall()
        return [dict(r) for r in rows]

    def upsert_sealed(
        self,
        *,
        ident: dict[str, Any],
        local_hashes: dict[str, Any],
        manifest_hashes: dict[str, Any],
        local_mtime_epoch_s: float | None,
        now_iso: str,
        run_id: str,
        source: str = "live_scan",
    ) -> dict[str, Any]:
        existing = self.get_partition_by_rel_data(ident["rel_data_path"])
        minute_utc = str(ident.get("minute_utc") or "00").zfill(2)
        partition_minutes = int(ident.get("partition_minutes") or 60)
        window_id = ident.get("window_id") or f"{ident['date_utc']}T{ident['hour_utc']}:{minute_utc}Z"
        planned_start = ident.get("planned_start_wall_ns")
        planned_end = ident.get("planned_end_wall_ns")
        collection_run_id = ident.get("collection_run_id")
        segment_id = str(ident.get("segment_id") or ident["partition_id"])
        is_startup_stub = 1 if ident.get("is_startup_stub") else 0
        settled_by_timer = 1 if ident.get("settled_by_timer") else 0
        coverage_incomplete = 1 if ident.get("coverage_incomplete") else 0
        first_rx_seq = ident.get("first_rx_seq")
        last_rx_seq = ident.get("last_rx_seq")
        start_wall_ns = ident.get("start_wall_ns")
        end_wall_ns = ident.get("end_wall_ns")
        data_lines = int(ident.get("data_lines") or 0)
        local_loss_lines = int(ident.get("local_loss_lines") or 0)
        gap_start_lines = int(ident.get("gap_start_lines") or 0)
        late_frames_rejected = int(ident.get("late_frames_rejected") or 0)
        with self.conn:
            if existing is None:
                self.conn.execute(
                    """
                    INSERT INTO partitions (
                        exchange, date_utc, hour_utc, boot_id, partition_id,
                        rel_data_path, rel_manifest_path, state,
                        local_bytes, local_sha256, local_md5,
                        manifest_bytes, manifest_sha256, manifest_md5,
                        local_mtime_epoch_s, sealed_at_utc, source, updated_at_utc,
                        minute_utc, partition_minutes, window_id,
                        planned_start_wall_ns, planned_end_wall_ns,
                        collection_run_id, segment_id,
                        is_startup_stub, settled_by_timer, coverage_incomplete,
                        first_rx_seq, last_rx_seq, start_wall_ns, end_wall_ns, data_lines,
                        local_loss_lines, gap_start_lines, late_frames_rejected
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'SEALED', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ident["exchange"],
                        ident["date_utc"],
                        ident["hour_utc"],
                        ident["boot_id"],
                        ident["partition_id"],
                        ident["rel_data_path"],
                        ident["rel_manifest_path"],
                        int(local_hashes["bytes"]),
                        str(local_hashes["sha256"]),
                        str(local_hashes["md5"]),
                        int(manifest_hashes["bytes"]),
                        str(manifest_hashes["sha256"]),
                        str(manifest_hashes["md5"]),
                        local_mtime_epoch_s,
                        now_iso,
                        source,
                        now_iso,
                        minute_utc,
                        partition_minutes,
                        window_id,
                        planned_start,
                        planned_end,
                        collection_run_id,
                        segment_id,
                        is_startup_stub,
                        settled_by_timer,
                        coverage_incomplete,
                        first_rx_seq,
                        last_rx_seq,
                        start_wall_ns,
                        end_wall_ns,
                        data_lines,
                        local_loss_lines,
                        gap_start_lines,
                        late_frames_rejected,
                    ),
                )
                self.conn.execute(
                    """
                    INSERT INTO archive_events (
                        ts_utc, run_id, exchange, date_utc, hour_utc, boot_id, partition_id,
                        rel_data_path, event_type, from_state, to_state, detail_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'SEALED_RECORDED', NULL, 'SEALED', ?)
                    """,
                    (
                        now_iso,
                        run_id,
                        ident["exchange"],
                        ident["date_utc"],
                        ident["hour_utc"],
                        ident["boot_id"],
                        ident["partition_id"],
                        ident["rel_data_path"],
                        json.dumps({"bytes": local_hashes["bytes"], "sha256": local_hashes["sha256"], "window_id": window_id}),
                    ),
                )
            elif existing["state"] == STATE_SEALED:
                self.conn.execute(
                    """
                    UPDATE partitions
                    SET local_bytes = ?, local_sha256 = ?, local_md5 = ?,
                        manifest_bytes = ?, manifest_sha256 = ?, manifest_md5 = ?,
                        local_mtime_epoch_s = COALESCE(?, local_mtime_epoch_s),
                        minute_utc = ?, partition_minutes = ?, window_id = ?,
                        planned_start_wall_ns = COALESCE(?, planned_start_wall_ns),
                        planned_end_wall_ns = COALESCE(?, planned_end_wall_ns),
                        collection_run_id = COALESCE(?, collection_run_id),
                        segment_id = COALESCE(?, segment_id),
                        is_startup_stub = ?, settled_by_timer = ?, coverage_incomplete = ?,
                        first_rx_seq = COALESCE(?, first_rx_seq),
                        last_rx_seq = COALESCE(?, last_rx_seq),
                        start_wall_ns = ?,
                        end_wall_ns = ?,
                        data_lines = ?,
                        local_loss_lines = ?,
                        gap_start_lines = ?,
                        late_frames_rejected = ?,
                        updated_at_utc = ?
                    WHERE rel_data_path = ?
                    """,
                    (
                        int(local_hashes["bytes"]),
                        str(local_hashes["sha256"]),
                        str(local_hashes["md5"]),
                        int(manifest_hashes["bytes"]),
                        str(manifest_hashes["sha256"]),
                        str(manifest_hashes["md5"]),
                        local_mtime_epoch_s,
                        minute_utc,
                        partition_minutes,
                        window_id,
                        planned_start,
                        planned_end,
                        collection_run_id,
                        segment_id,
                        is_startup_stub,
                        settled_by_timer,
                        coverage_incomplete,
                        first_rx_seq,
                        last_rx_seq,
                        start_wall_ns,
                        end_wall_ns,
                        data_lines,
                        local_loss_lines,
                        gap_start_lines,
                        late_frames_rejected,
                        now_iso,
                        ident["rel_data_path"],
                    ),
                )
        row = self.get_partition_by_rel_data(ident["rel_data_path"])
        assert row is not None
        return row

    def mark_verified(
        self,
        *,
        rel_data_path: str,
        remote_data: RemoteObjectInfo,
        data_method: str,
        remote_manifest: RemoteObjectInfo,
        manifest_method: str,
        now_iso: str,
        run_id: str,
    ) -> dict[str, Any]:
        row = self.get_partition_by_rel_data(rel_data_path)
        assert row is not None
        from_state = row["state"]
        with self.conn:
            self.conn.execute(
                """
                UPDATE partitions
                SET state = 'VERIFIED',
                    remote_data_id = ?,
                    remote_data_bytes = ?,
                    remote_data_md5 = ?,
                    remote_data_sha256 = ?,
                    remote_data_verified = 1,
                    remote_data_verification_method = ?,
                    remote_manifest_id = ?,
                    remote_manifest_bytes = ?,
                    remote_manifest_md5 = ?,
                    remote_manifest_sha256 = ?,
                    remote_manifest_verified = 1,
                    remote_manifest_verification_method = ?,
                    verified_at_utc = COALESCE(verified_at_utc, ?),
                    last_error_code = NULL,
                    last_error_message = NULL,
                    last_error_at_utc = NULL,
                    has_conflict = 0,
                    conflict_detail_json = NULL,
                    updated_at_utc = ?
                WHERE rel_data_path = ?
                """,
                (
                    remote_data.remote_id,
                    remote_data.size,
                    remote_data.md5,
                    remote_data.sha256,
                    data_method,
                    remote_manifest.remote_id,
                    remote_manifest.size,
                    remote_manifest.md5,
                    remote_manifest.sha256,
                    manifest_method,
                    now_iso,
                    now_iso,
                    rel_data_path,
                ),
            )
            self.conn.execute(
                """
                INSERT INTO archive_events (
                    ts_utc, run_id, exchange, date_utc, hour_utc, boot_id, partition_id,
                    rel_data_path, event_type, from_state, to_state, detail_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'VERIFIED_OK', ?, 'VERIFIED', ?)
                """,
                (
                    now_iso,
                    run_id,
                    row["exchange"],
                    row["date_utc"],
                    row["hour_utc"],
                    row["boot_id"],
                    row["partition_id"],
                    rel_data_path,
                    from_state,
                    json.dumps(
                        {
                            "remote_data_id": remote_data.remote_id,
                            "data_method": data_method,
                            "remote_manifest_id": remote_manifest.remote_id,
                            "manifest_method": manifest_method,
                        },
                        sort_keys=True,
                    ),
                ),
            )
        updated = self.get_partition_by_rel_data(rel_data_path)
        assert updated is not None
        return updated

    def mark_pruned(
        self,
        *,
        rel_data_path: str,
        now_iso: str,
        run_id: str,
        event_type: str = "PRUNED_OK",
        detail: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        row = self.get_partition_by_rel_data(rel_data_path)
        assert row is not None
        if row["state"] != STATE_VERIFIED:
            raise ValueError(f"Refusing to transition {rel_data_path} to PRUNED from state={row['state']}")
        if not (row["remote_data_verified"] and row["remote_manifest_verified"]):
            raise ValueError(f"Refusing to transition {rel_data_path} to PRUNED without dual remote verification")
        if row["has_conflict"]:
            raise ValueError(f"Refusing to transition {rel_data_path} to PRUNED with unresolved conflict")

        with self.conn:
            self.conn.execute(
                """
                UPDATE partitions
                SET state = 'PRUNED',
                    pruned_at_utc = ?,
                    last_error_code = NULL,
                    last_error_message = NULL,
                    last_error_at_utc = NULL,
                    updated_at_utc = ?
                WHERE rel_data_path = ?
                """,
                (now_iso, now_iso, rel_data_path),
            )
            self.conn.execute(
                """
                INSERT INTO archive_events (
                    ts_utc, run_id, exchange, date_utc, hour_utc, boot_id, partition_id,
                    rel_data_path, event_type, from_state, to_state, detail_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'VERIFIED', 'PRUNED', ?)
                """,
                (
                    now_iso,
                    run_id,
                    row["exchange"],
                    row["date_utc"],
                    row["hour_utc"],
                    row["boot_id"],
                    row["partition_id"],
                    rel_data_path,
                    event_type,
                    json.dumps(detail or {}, sort_keys=True),
                ),
            )
        updated = self.get_partition_by_rel_data(rel_data_path)
        assert updated is not None
        return updated

    def mark_conflict(
        self,
        *,
        rel_data_path: str,
        conflict_detail: dict[str, Any],
        now_iso: str,
        run_id: str,
    ) -> None:
        row = self.get_partition_by_rel_data(rel_data_path)
        with self.conn:
            if row is not None:
                self.conn.execute(
                    """
                    UPDATE partitions
                    SET has_conflict = 1,
                        conflict_detail_json = ?,
                        last_error_code = 'REMOTE_HASH_CONFLICT',
                        last_error_message = ?,
                        last_error_at_utc = ?,
                        updated_at_utc = ?
                    WHERE rel_data_path = ?
                    """,
                    (
                        json.dumps(conflict_detail, sort_keys=True),
                        conflict_detail.get("action", "REMOTE_HASH_CONFLICT"),
                        now_iso,
                        now_iso,
                        rel_data_path,
                    ),
                )
            self.conn.execute(
                """
                INSERT INTO archive_events (
                    ts_utc, run_id, exchange, date_utc, hour_utc, boot_id, partition_id,
                    rel_data_path, event_type, from_state, to_state, detail_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'CONFLICT_ISOLATED', ?, ?, ?)
                """,
                (
                    now_iso,
                    run_id,
                    row["exchange"] if row else None,
                    row["date_utc"] if row else None,
                    row["hour_utc"] if row else None,
                    row["boot_id"] if row else None,
                    row["partition_id"] if row else None,
                    rel_data_path,
                    row["state"] if row else None,
                    row["state"] if row else None,
                    json.dumps(conflict_detail, sort_keys=True),
                ),
            )

    def mark_error(
        self,
        *,
        rel_data_path: str,
        error_code: str,
        error_message: str,
        now_iso: str,
        run_id: str,
        event_type: str = "ERROR",
        detail: dict[str, Any] | None = None,
    ) -> None:
        row = self.get_partition_by_rel_data(rel_data_path)
        with self.conn:
            if row is not None:
                self.conn.execute(
                    """
                    UPDATE partitions
                    SET last_error_code = ?,
                        last_error_message = ?,
                        last_error_at_utc = ?,
                        updated_at_utc = ?
                    WHERE rel_data_path = ?
                    """,
                    (error_code, error_message, now_iso, now_iso, rel_data_path),
                )
            self.conn.execute(
                """
                INSERT INTO archive_events (
                    ts_utc, run_id, exchange, date_utc, hour_utc, boot_id, partition_id,
                    rel_data_path, event_type, from_state, to_state, detail_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now_iso,
                    run_id,
                    row["exchange"] if row else None,
                    row["date_utc"] if row else None,
                    row["hour_utc"] if row else None,
                    row["boot_id"] if row else None,
                    row["partition_id"] if row else None,
                    rel_data_path,
                    event_type,
                    row["state"] if row else None,
                    row["state"] if row else None,
                    json.dumps(
                        {"error_code": error_code, "error_message": error_message, **(detail or {})},
                        sort_keys=True,
                    ),
                ),
            )

    def get_meta_sync(self, rel_path: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT * FROM meta_sync_state WHERE rel_path = ?",
            (rel_path,),
        ).fetchone()
        return dict(row) if row else None

    def upsert_meta_sync(self, rel_path: str, size_bytes: int, sha256: str, now_iso: str) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO meta_sync_state(rel_path, size_bytes, sha256, synced_at_utc)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(rel_path) DO UPDATE SET
                    size_bytes = excluded.size_bytes,
                    sha256 = excluded.sha256,
                    synced_at_utc = excluded.synced_at_utc
                """,
                (rel_path, size_bytes, sha256, now_iso),
            )

    def get_qa_dispatch(
        self,
        *,
        run_id: str,
        target_window_utc: str,
        archive_revision: str,
    ) -> dict[str, Any] | None:
        row = self.conn.execute(
            """
            SELECT * FROM qa_dispatch_outbox
            WHERE run_id = ? AND target_window_utc = ? AND archive_revision = ?
            """,
            (str(run_id), str(target_window_utc), str(archive_revision)),
        ).fetchone()
        return dict(row) if row else None

    def enqueue_qa_dispatch(
        self,
        *,
        run_id: str,
        target_window_utc: str,
        archive_revision: str,
        partition_minutes: int,
        window_span_id: str | None,
        index_sha256: str,
        index_synced_at_utc: str,
        manifests: list[dict[str, Any]],
        now_iso: str,
        discovery_source: str = "archive_sync",
    ) -> tuple[bool, dict[str, Any]]:
        """Insert a durable PENDING QA dispatch record deduplicated by `(run_id, target_window_utc, archive_revision)`."""
        with self.conn:
            cur = self.conn.execute(
                """
                INSERT OR IGNORE INTO qa_dispatch_outbox (
                    run_id, target_window_utc, archive_revision, partition_minutes,
                    window_span_id, index_sha256, index_synced_at_utc, manifest_count,
                    manifests_json, state, attempt_count, next_attempt_at_epoch_s,
                    first_enqueued_at_utc, discovery_source, updated_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', 0, 0, ?, ?, ?)
                """,
                (
                    str(run_id),
                    str(target_window_utc),
                    str(archive_revision),
                    int(partition_minutes),
                    window_span_id,
                    str(index_sha256),
                    str(index_synced_at_utc),
                    len(manifests),
                    json.dumps(manifests, sort_keys=True, separators=(",", ":")),
                    now_iso,
                    str(discovery_source),
                    now_iso,
                ),
            )
            inserted = cur.rowcount == 1
        row = self.get_qa_dispatch(
            run_id=str(run_id),
            target_window_utc=str(target_window_utc),
            archive_revision=str(archive_revision),
        )
        assert row is not None
        return inserted, row

    def list_qa_dispatches(
        self,
        *,
        states: tuple[str, ...] | None = None,
        run_id: str | None = None,
        due_before_epoch_s: float | None = None,
        newest_first: bool = True,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if states:
            placeholders = ",".join("?" for _ in states)
            clauses.append(f"state IN ({placeholders})")
            params.extend(states)
        if run_id is not None:
            clauses.append("run_id = ?")
            params.append(str(run_id))
        if due_before_epoch_s is not None:
            clauses.append("next_attempt_at_epoch_s <= ?")
            params.append(float(due_before_epoch_s))
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        order_sql = (
            "ORDER BY target_window_utc DESC, id DESC"
            if newest_first
            else "ORDER BY target_window_utc ASC, id ASC"
        )
        rows = self.conn.execute(
            f"SELECT * FROM qa_dispatch_outbox {where_sql} {order_sql}",
            tuple(params),
        ).fetchall()
        return [dict(r) for r in rows]

    def mark_qa_dispatch_accepted(
        self,
        dispatch_id: int,
        *,
        accepted_at_utc: str,
        http_status: int = 204,
        now_iso: str,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE qa_dispatch_outbox
                SET state = 'ACCEPTED',
                    attempt_count = attempt_count + 1,
                    last_attempt_at_utc = ?,
                    api_accepted_at_utc = COALESCE(api_accepted_at_utc, ?),
                    last_http_status = ?,
                    last_error_code = NULL,
                    last_error_message = NULL,
                    updated_at_utc = ?
                WHERE id = ?
                """,
                (now_iso, accepted_at_utc, int(http_status), now_iso, int(dispatch_id)),
            )

    def mark_qa_dispatch_retry(
        self,
        dispatch_id: int,
        *,
        next_attempt_at_epoch_s: float,
        http_status: int | None,
        error_code: str,
        error_message: str,
        now_iso: str,
        permanent: bool = False,
    ) -> None:
        new_state = "FAILED_PERMANENT" if permanent else "PENDING"
        with self.conn:
            self.conn.execute(
                """
                UPDATE qa_dispatch_outbox
                SET state = ?,
                    attempt_count = attempt_count + 1,
                    next_attempt_at_epoch_s = ?,
                    last_attempt_at_utc = ?,
                    last_http_status = ?,
                    last_error_code = ?,
                    last_error_message = ?,
                    updated_at_utc = ?
                WHERE id = ?
                """,
                (
                    new_state,
                    float(next_attempt_at_epoch_s),
                    now_iso,
                    int(http_status) if http_status is not None else None,
                    str(error_code),
                    str(error_message)[:500],
                    now_iso,
                    int(dispatch_id),
                ),
            )

    def mark_qa_dispatch_completed(
        self,
        *,
        run_id: str,
        target_window_utc: str,
        archive_revision: str,
        qa_result_status: str,
        qa_completed_at_utc: str,
        workflow_run_id: int | None = None,
        workflow_run_url: str | None = None,
        workflow_run_started_at_utc: str | None = None,
        now_iso: str,
    ) -> bool:
        with self.conn:
            cur = self.conn.execute(
                """
                UPDATE qa_dispatch_outbox
                SET state = 'QA_COMPLETED',
                    qa_result_status = ?,
                    qa_completed_at_utc = COALESCE(qa_completed_at_utc, ?),
                    workflow_run_id = COALESCE(?, workflow_run_id),
                    workflow_run_url = COALESCE(?, workflow_run_url),
                    workflow_run_started_at_utc = COALESCE(?, workflow_run_started_at_utc),
                    updated_at_utc = ?
                WHERE run_id = ? AND target_window_utc = ? AND archive_revision = ?
                """,
                (
                    str(qa_result_status),
                    str(qa_completed_at_utc),
                    int(workflow_run_id) if workflow_run_id is not None else None,
                    workflow_run_url,
                    workflow_run_started_at_utc,
                    now_iso,
                    str(run_id),
                    str(target_window_utc),
                    str(archive_revision),
                ),
            )
            return cur.rowcount > 0

    def create_consistent_snapshot(self, dest_path: Path) -> dict[str, Any]:
        """Create a crash-consistent standalone SQLite snapshot using sqlite3.Connection.backup()."""
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = dest_path.with_name(f"{dest_path.name}.tmp.{os.getpid()}")
        if tmp_path.exists():
            tmp_path.unlink()
        dst_conn = sqlite3.connect(str(tmp_path))
        try:
            self.conn.backup(dst_conn)
            dst_conn.commit()
            check_row = dst_conn.execute("PRAGMA integrity_check;").fetchone()
            if not check_row or str(check_row[0]).lower() != "ok":
                raise RuntimeError(f"SQLite backup integrity_check failed: {check_row}")
        finally:
            dst_conn.close()
        os.replace(tmp_path, dest_path)
        hashes = file_hashes(dest_path)
        return {"path": str(dest_path), "bytes": hashes["bytes"], "sha256": hashes["sha256"], "md5": hashes["md5"]}


def migrate_legacy_jsonl_ledger(
    sealed_root: Path,
    ledger: ArchiveLedger,
    *,
    now_s: float | None = None,
    run_id: str = "migration",
) -> dict[str, Any]:
    """Idempotently import historical proofs from `verified_archive_ledger.jsonl` into SQLite.

    Preserves the pre-migration `verified_archive_ledger.jsonl` byte-for-byte untouched in place AND
    stores an immutable backup copy (+ .sha256) under `meta/legacy_ledger/` so historical duplicate
    verifications and non-verified lines are never lost. Records verifiable original SHA-256 and
    complete line counts in `schema_meta` and `archive_events`.

    Because the legacy JSONL format did not record independent manifest remote verification,
    imported rows start in `SEALED` with `remote_data_verified=1` and `remote_manifest_verified=0`
    until the archive loop verifies the remote manifest (requiring zero re-upload of data files).
    """
    legacy_path = sealed_root / LEGACY_LEDGER_JSONL_FILENAME
    if not legacy_path.is_file():
        return {
            "migrated": 0,
            "skipped_existing": 0,
            "skipped_unverified": 0,
            "duplicate_verified_lines": 0,
            "invalid_lines": 0,
            "total_lines": 0,
            "verified_lines": 0,
            "legacy_present": False,
        }

    raw_bytes = legacy_path.read_bytes()
    current_sha = hashlib.sha256(raw_bytes).hexdigest()
    prev_sha = ledger.get_meta("legacy_jsonl_original_sha256")
    if prev_sha == current_sha:
        return {
            "migrated": 0,
            "skipped_existing": 0,
            "skipped_unverified": int(ledger.get_meta("legacy_jsonl_unverified_lines") or 0),
            "duplicate_verified_lines": int(ledger.get_meta("legacy_jsonl_duplicate_verified_lines") or 0),
            "invalid_lines": int(ledger.get_meta("legacy_jsonl_invalid_lines") or 0),
            "total_lines": int(ledger.get_meta("legacy_jsonl_total_lines") or 0),
            "verified_lines": int(ledger.get_meta("legacy_jsonl_verified_lines") or 0),
            "original_sha256": current_sha,
            "original_bytes": len(raw_bytes),
            "preserved_backup_path": ledger.get_meta("legacy_jsonl_preserved_rel_path"),
            "legacy_present": True,
            "already_migrated": True,
        }

    eff_now_s = time.time() if now_s is None else now_s
    now_iso = _iso_utc(eff_now_s)

    # Preserve an immutable byte-for-byte backup of the original pre-migration JSONL under
    # meta/legacy_ledger/ so it is also synced to Google Drive meta/ (original file stays untouched).
    ts_tag = datetime.fromtimestamp(eff_now_s, tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_dir = sealed_root / "meta" / "legacy_ledger"
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_name = f"verified_archive_ledger.pre_migration_{ts_tag}_{current_sha[:8]}.jsonl"
    backup_path = backup_dir / backup_name
    backup_sha_path = backup_dir / f"{backup_name}.sha256"
    if not backup_path.exists():
        fd = os.open(str(backup_path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        try:
            with os.fdopen(fd, "wb") as bf:
                bf.write(raw_bytes)
                bf.flush()
                os.fsync(bf.fileno())
        except Exception:
            if backup_path.exists():
                backup_path.unlink()
            raise
        backup_sha_path.write_text(f"{current_sha}  {backup_name}\n", encoding="utf-8")
    preserved_rel_path = backup_path.relative_to(sealed_root).as_posix()

    latest_by_rel: dict[str, tuple[int, dict[str, Any]]] = {}
    total_lines = 0
    verified_lines = 0
    duplicate_verified_lines = 0
    skipped_unverified = 0
    invalid_lines = 0

    for line_no, raw_line in enumerate(raw_bytes.decode("utf-8", errors="replace").splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        total_lines += 1
        try:
            entry = json.loads(line)
        except Exception as exc:
            invalid_lines += 1
            ledger.record_event(
                ts_utc=now_iso,
                run_id=run_id,
                event_type="MIGRATION_SKIPPED_INVALID",
                detail={"line_no": line_no, "error": str(exc), "raw_line": line[:512]},
            )
            continue
        if not isinstance(entry, dict) or not entry.get("remote_path"):
            invalid_lines += 1
            ledger.record_event(
                ts_utc=now_iso,
                run_id=run_id,
                event_type="MIGRATION_SKIPPED_INVALID",
                detail={"line_no": line_no, "error": "missing_remote_path", "entry": entry},
            )
            continue
        rel_data = str(entry["remote_path"])
        if not entry.get("verified"):
            skipped_unverified += 1
            ledger.record_event(
                ts_utc=now_iso,
                run_id=run_id,
                rel_data_path=rel_data,
                event_type="MIGRATION_SKIPPED_UNVERIFIED",
                detail={"line_no": line_no, "entry": entry},
            )
            continue
        verified_lines += 1
        if rel_data in latest_by_rel:
            prev_line_no, prev_entry = latest_by_rel[rel_data]
            duplicate_verified_lines += 1
            ledger.record_event(
                ts_utc=now_iso,
                run_id=run_id,
                rel_data_path=rel_data,
                event_type="MIGRATION_HISTORICAL_VERIFIED_LINE",
                detail={"line_no": prev_line_no, "superseded_by_line_no": line_no, "entry": prev_entry},
            )
        latest_by_rel[rel_data] = (line_no, entry)

    migrated = 0
    skipped_existing = 0
    for rel_data, (line_no, entry) in sorted(latest_by_rel.items()):
        if ledger.get_partition_by_rel_data(rel_data) is not None:
            skipped_existing += 1
            continue

        rel_manifest = str(entry.get("remote_manifest_path") or rel_data.replace(".tsv.zst", ".manifest.json"))
        mpath = sealed_root / rel_manifest
        dpath = sealed_root / rel_data

        if mpath.is_file():
            mf = json.loads(mpath.read_text(encoding="utf-8"))
            ident = _derive_partition_identity(sealed_root, mpath, mf)
            mf_hashes = file_hashes(mpath)
        else:
            inferred = _infer_identity_from_rel_path(rel_data)
            ident = {
                **inferred,
                "rel_data_path": rel_data,
                "rel_manifest_path": rel_manifest,
            }
            mf_hashes = {"bytes": 0, "sha256": "", "md5": ""}

        # Honor explicit v2+ exported fields if present, otherwise stay conservative (SEALED until
        # remote manifest is verified).
        has_manifest_proof = bool(entry.get("remote_manifest_verified")) and bool(mf_hashes["sha256"])
        exported_state = str(entry.get("state") or "")
        if has_manifest_proof and exported_state in (STATE_VERIFIED, STATE_PRUNED):
            initial_state = exported_state
        else:
            initial_state = STATE_SEALED

        local_mtime = dpath.stat().st_mtime if dpath.is_file() else None
        with ledger.conn:
            ledger.conn.execute(
                """
                INSERT INTO partitions (
                    exchange, date_utc, hour_utc, boot_id, partition_id,
                    rel_data_path, rel_manifest_path, state,
                    local_bytes, local_sha256, local_md5,
                    manifest_bytes, manifest_sha256, manifest_md5,
                    local_mtime_epoch_s, sealed_at_utc,
                    remote_data_id, remote_data_bytes, remote_data_md5, remote_data_sha256,
                    remote_data_verified, remote_data_verification_method,
                    remote_manifest_id, remote_manifest_bytes, remote_manifest_md5, remote_manifest_sha256,
                    remote_manifest_verified, remote_manifest_verification_method,
                    verified_at_utc, pruned_at_utc, source, updated_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'legacy_jsonl_migration', ?)
                """,
                (
                    ident["exchange"],
                    ident["date_utc"],
                    ident["hour_utc"],
                    ident["boot_id"],
                    ident["partition_id"],
                    rel_data,
                    rel_manifest,
                    initial_state,
                    int(entry["bytes"]),
                    str(entry["sha256"]),
                    str(entry.get("md5") or ""),
                    int(mf_hashes["bytes"]),
                    str(mf_hashes["sha256"]),
                    str(mf_hashes["md5"]),
                    local_mtime,
                    str(entry.get("verified_utc") or now_iso),
                    entry.get("remote_id"),
                    int(entry["bytes"]),
                    str(entry.get("md5") or "") or None,
                    str(entry["sha256"]),
                    str(entry.get("verification_method") or "legacy_jsonl"),
                    entry.get("remote_manifest_id"),
                    int(mf_hashes["bytes"]) if has_manifest_proof else None,
                    str(mf_hashes["md5"]) if has_manifest_proof else None,
                    str(mf_hashes["sha256"]) if has_manifest_proof else None,
                    1 if has_manifest_proof else 0,
                    entry.get("manifest_verification_method") if has_manifest_proof else None,
                    str(entry.get("verified_utc") or now_iso),
                    entry.get("pruned_at_utc") if initial_state == STATE_PRUNED else None,
                    now_iso,
                ),
            )
            ledger.conn.execute(
                """
                INSERT INTO archive_events (
                    ts_utc, run_id, exchange, date_utc, hour_utc, boot_id, partition_id,
                    rel_data_path, event_type, from_state, to_state, detail_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'MIGRATED_LEGACY_JSONL', NULL, ?, ?)
                """,
                (
                    now_iso,
                    run_id,
                    ident["exchange"],
                    ident["date_utc"],
                    ident["hour_utc"],
                    ident["boot_id"],
                    ident["partition_id"],
                    rel_data,
                    initial_state,
                    json.dumps(
                        {
                            "line_no": line_no,
                            "legacy_verified_utc": entry.get("verified_utc"),
                            "remote_id": entry.get("remote_id"),
                            "sha256": entry.get("sha256"),
                            "has_manifest_proof": has_manifest_proof,
                        },
                        sort_keys=True,
                    ),
                ),
            )
        migrated += 1

    ledger.set_meta("legacy_jsonl_migrated_at", now_iso)
    ledger.set_meta("legacy_jsonl_original_sha256", current_sha)
    ledger.set_meta("legacy_jsonl_original_bytes", str(len(raw_bytes)))
    ledger.set_meta("legacy_jsonl_preserved_rel_path", preserved_rel_path)
    ledger.set_meta("legacy_jsonl_total_lines", str(total_lines))
    ledger.set_meta("legacy_jsonl_verified_lines", str(verified_lines))
    ledger.set_meta("legacy_jsonl_duplicate_verified_lines", str(duplicate_verified_lines))
    ledger.set_meta("legacy_jsonl_unverified_lines", str(skipped_unverified))
    ledger.set_meta("legacy_jsonl_invalid_lines", str(invalid_lines))
    ledger.set_meta("legacy_jsonl_migrated_partitions", str(migrated))
    ledger.record_event(
        ts_utc=now_iso,
        run_id=run_id,
        event_type="MIGRATION_LEGACY_JSONL_SUMMARY",
        detail={
            "original_sha256": current_sha,
            "original_bytes": len(raw_bytes),
            "preserved_backup_path": preserved_rel_path,
            "total_lines": total_lines,
            "verified_lines": verified_lines,
            "duplicate_verified_lines": duplicate_verified_lines,
            "skipped_unverified": skipped_unverified,
            "invalid_lines": invalid_lines,
            "migrated_partitions": migrated,
            "skipped_existing": skipped_existing,
        },
    )
    return {
        "migrated": migrated,
        "skipped_existing": skipped_existing,
        "skipped_unverified": skipped_unverified,
        "duplicate_verified_lines": duplicate_verified_lines,
        "invalid_lines": invalid_lines,
        "total_lines": total_lines,
        "verified_lines": verified_lines,
        "original_sha256": current_sha,
        "original_bytes": len(raw_bytes),
        "preserved_backup_path": preserved_rel_path,
        "legacy_present": True,
        "already_migrated": False,
    }


def _row_to_jsonl_entry(row: dict[str, Any], sealed_root: Path | None = None) -> dict[str, Any]:
    minute_utc = str(row.get("minute_utc") or "00").zfill(2)
    partition_minutes = int(row.get("partition_minutes") or 60)
    window_id = row.get("window_id") or f"{row['date_utc']}T{row['hour_utc']}:{minute_utc}Z"
    local_loss_lines = int(row.get("local_loss_lines") or 0)
    gap_start_lines = int(row.get("gap_start_lines") or 0)
    late_frames_rejected = int(row.get("late_frames_rejected") or 0)

    # Enrich from local sidecar manifest when a legacy v1 migrated SQLite row had 0 in new columns
    if sealed_root is not None and local_loss_lines == 0 and gap_start_lines == 0 and late_frames_rejected == 0:
        rel_mf = row.get("rel_manifest_path")
        if rel_mf:
            mf_path = sealed_root / str(rel_mf)
            if mf_path.is_file():
                try:
                    mf_obj = json.loads(mf_path.read_text(encoding="utf-8"))
                    local_loss_lines = int(mf_obj.get("local_loss_lines") or 0)
                    gap_start_lines = int(mf_obj.get("gap_start_lines") or 0)
                    late_frames_rejected = int(mf_obj.get("late_frames_rejected") or 0)
                except Exception:
                    pass

    return {
        "verified_utc": row["verified_at_utc"] or row["updated_at_utc"],
        "remote_path": row["rel_data_path"],
        "remote_manifest_path": row["rel_manifest_path"],
        "remote_id": row["remote_data_id"],
        "remote_manifest_id": row["remote_manifest_id"],
        "bytes": int(row["local_bytes"]),
        "sha256": row["local_sha256"],
        "md5": row["local_md5"],
        "manifest_bytes": int(row["manifest_bytes"]),
        "manifest_sha256": row["manifest_sha256"],
        "verification_method": row["remote_data_verification_method"] or "remote_native_hash",
        "manifest_verification_method": row["remote_manifest_verification_method"],
        "remote_manifest_verified": bool(row["remote_manifest_verified"]),
        "verified": bool(row["remote_data_verified"] and row["remote_manifest_verified"]),
        "state": row["state"],
        "pruned_at_utc": row["pruned_at_utc"],
        "exchange": row["exchange"],
        "date_utc": row["date_utc"],
        "hour_utc": row["hour_utc"],
        "minute_utc": minute_utc,
        "partition_minutes": partition_minutes,
        "window_id": window_id,
        "planned_start_wall_ns": row.get("planned_start_wall_ns"),
        "planned_end_wall_ns": row.get("planned_end_wall_ns"),
        "collection_run_id": row.get("collection_run_id"),
        "segment_id": row.get("segment_id") or row["partition_id"],
        "boot_id": row["boot_id"],
        "partition_id": row["partition_id"],
        "is_startup_stub": bool(row.get("is_startup_stub")),
        "settled_by_timer": bool(row.get("settled_by_timer")),
        "coverage_incomplete": bool(row.get("coverage_incomplete")),
        "first_rx_seq": row.get("first_rx_seq"),
        "last_rx_seq": row.get("last_rx_seq"),
        "start_wall_ns": row.get("start_wall_ns"),
        "end_wall_ns": row.get("end_wall_ns"),
        "data_lines": int(row.get("data_lines") or 0),
        "local_loss_lines": local_loss_lines,
        "gap_start_lines": gap_start_lines,
        "late_frames_rejected": late_frames_rejected,
    }


def export_verified_jsonl_projection(sealed_root: Path, ledger: ArchiveLedger) -> Path:
    """Atomically write SQLite compatibility projection to `verified_archive_ledger.compat.jsonl`.

    Never overwrites or modifies any pre-existing legacy `verified_archive_ledger.jsonl`.
    """
    out_path = sealed_root / PROJECTION_LEDGER_JSONL_FILENAME
    rows = ledger.list_partitions(states=(STATE_VERIFIED, STATE_PRUNED))
    lines = [json.dumps(_row_to_jsonl_entry(r, sealed_root), sort_keys=False) for r in rows]
    payload = ("\n".join(lines) + "\n") if lines else ""

    tmp_path = out_path.with_name(f"{out_path.name}.tmp.{os.getpid()}")
    with tmp_path.open("w", encoding="utf-8") as fh:
        fh.write(payload)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp_path, out_path)
    ledger.set_meta("projection_jsonl_sha256", hashlib.sha256(payload.encode("utf-8")).hexdigest())
    ledger.set_meta("projection_jsonl_row_count", str(len(rows)))
    return out_path


def export_archive_manifest_index(
    sealed_root: Path,
    ledger: ArchiveLedger,
    *,
    effective_run_id: str | None = None,
) -> Path | None:
    """Atomically write content-deterministic cloud-readable index of remotely verified partitions.

    Only contains dual-remote-verified (`VERIFIED` or `PRUNED`) records and deterministic expected-window
    reconciliation records with zero volatile timestamps so repeat runs with no state changes keep an
    identical SHA-256 and issue zero remote uploads.
    """
    rows = [
        r
        for r in ledger.list_partitions(states=(STATE_VERIFIED, STATE_PRUNED))
        if r["remote_data_verified"] and r["remote_manifest_verified"] and not r["has_conflict"]
    ]
    win_rows = ledger.list_expected_windows()
    if not rows and not win_rows:
        return None
    out_path = sealed_root / ARCHIVE_MANIFEST_INDEX_RELPATH
    out_path.parent.mkdir(parents=True, exist_ok=True)
    entries = sorted(
        (_row_to_jsonl_entry(r, sealed_root) for r in rows),
        key=lambda e: (e["date_utc"], e["window_id"], e["exchange"], e["remote_path"]),
    )
    historical_loss_manifests = [
        {
            "remote_manifest_path": e["remote_manifest_path"],
            "remote_path": e["remote_path"],
            "exchange": e["exchange"],
            "date_utc": e["date_utc"],
            "hour_utc": e["hour_utc"],
            "minute_utc": e["minute_utc"],
            "partition_minutes": e["partition_minutes"],
            "window_id": e["window_id"],
            "collection_run_id": e.get("collection_run_id"),
            "boot_id": e["boot_id"],
            "sha256": e["sha256"],
            "manifest_sha256": e["manifest_sha256"],
            "local_loss_lines": int(e.get("local_loss_lines") or 0),
            "late_frames_rejected": int(e.get("late_frames_rejected") or 0),
            "gap_start_lines": int(e.get("gap_start_lines") or 0),
        }
        for e in entries
        if int(e.get("local_loss_lines") or 0) > 0 or int(e.get("late_frames_rejected") or 0) > 0
    ]
    from data_coverage import canonical_window_manifest_entries, compute_window_archive_revision

    entries_by_window: dict[tuple[str, str, int], list[dict[str, Any]]] = {}
    for e in entries:
        e_key = (
            str(e.get("collection_run_id") or ""),
            str(e["window_id"]),
            int(e.get("partition_minutes") or 60),
        )
        entries_by_window.setdefault(e_key, []).append(e)

    window_ledger = []
    for wr in win_rows:
        w_run_id = str(wr["collection_run_id"] or "")
        w_id = str(wr["window_id"])
        w_pm = int(wr["partition_minutes"])
        w_entries = entries_by_window.get((w_run_id, w_id, w_pm), [])
        w_rev: str | None = None
        if w_entries:
            try:
                w_rev = compute_window_archive_revision(
                    w_entries,
                    run_id=w_run_id or None,
                    window_id=w_id,
                    partition_minutes=w_pm,
                )
            except ValueError:
                w_rev = None
        w_item: dict[str, Any] = {
            "window_key": wr["window_key"],
            "window_id": w_id,
            "window_span_id": wr["window_span_id"],
            "date_utc": wr["date_utc"],
            "hour_utc": wr["hour_utc"],
            "minute_utc": wr["minute_utc"],
            "partition_minutes": w_pm,
            "planned_start_wall_ns": int(wr["planned_start_wall_ns"]),
            "planned_end_wall_ns": int(wr["planned_end_wall_ns"]),
            "collection_run_id": wr["collection_run_id"],
            "expected_exchanges": json.loads(wr["expected_exchanges_json"]),
            "file_verified_exchanges": json.loads(wr["file_verified_exchanges_json"]),
            "covered_exchanges": json.loads(wr["covered_exchanges_json"]),
            "missing_exchanges": json.loads(wr["missing_exchanges_json"]),
            "incomplete_coverage_exchanges": json.loads(wr["incomplete_coverage_exchanges_json"]),
            "file_verification_status": wr["file_verification_status"],
            "window_coverage_status": wr["window_coverage_status"],
            "window_status": wr["window_status"],
            "manifest_count": len(w_entries),
        }
        if w_rev is not None:
            w_item["archive_revision"] = w_rev
        window_ledger.append(w_item)

    doc: dict[str, Any] = {
        "schema_version": 2,
        "effective_run_id": effective_run_id,
        "verified_count": len(entries),
        "entries": entries,
        "historical_loss_manifests": historical_loss_manifests,
    }
    if window_ledger:
        doc["window_ledger"] = window_ledger
    payload = json.dumps(doc, indent=2, sort_keys=False) + "\n"
    if out_path.is_file():
        try:
            if out_path.read_text(encoding="utf-8") == payload:
                return out_path
        except OSError:
            pass
    tmp_path = out_path.with_name(f"{out_path.name}.tmp.{os.getpid()}")
    with tmp_path.open("w", encoding="utf-8") as fh:
        fh.write(payload)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp_path, out_path)
    return out_path


def discover_and_enqueue_qa_dispatches(
    sealed_root: Path,
    ledger: ArchiveLedger,
    *,
    now_epoch_s: float | None = None,
    run_id_filter: str | None = None,
    partition_minutes_filter: int | None = None,
    discovery_source: str = "archive_sync",
    max_age_hours: float | None = None,
) -> dict[str, Any]:
    """Discover COMPLETE windows whose verified manifests are already synced in Google Drive's
    `meta/archive_manifest_index.json` and enqueue deduplicated durable QA dispatch records.

    Preconditions enforced strictly per window (independent of overall archive exit code):
      1. `meta/archive_manifest_index.json` exists locally AND its current SHA-256/size matches
         `meta_sync_state` (proving GitHub-readable index sync to Google Drive succeeded).
      2. Window status in the synced index is `COMPLETE` (`file_verification_status == "verified"`
         and `window_coverage_status == "complete"`). Startup stubs and incomplete/pending windows
         are NEVER enqueued.
      3. All expected exchanges for the window have remotely verified entries in the synced index.
      4. Deduplicated by `(run_id, target_window_utc, archive_revision)`.

    Safe to call both at the end of `sync_sealed_directory()` and on startup of the independent
    dispatcher process to recover from a crash between Drive index sync and outbox persistence.
    """
    from data_coverage import canonical_window_manifest_entries, compute_window_archive_revision

    now_s = time.time() if now_epoch_s is None else float(now_epoch_s)
    now_iso = _iso_utc(now_s)
    index_path = sealed_root / ARCHIVE_MANIFEST_INDEX_RELPATH
    if not index_path.is_file():
        return {
            "index_synced": False,
            "reason": "archive_manifest_index_missing",
            "enqueued": [],
            "already_queued": [],
            "skipped_not_complete": [],
        }

    idx_hashes = file_hashes(index_path)
    sync_row = ledger.get_meta_sync(ARCHIVE_MANIFEST_INDEX_RELPATH)
    if (
        sync_row is None
        or str(sync_row.get("sha256") or "").lower() != str(idx_hashes["sha256"]).lower()
        or int(sync_row.get("size_bytes") or -1) != int(idx_hashes["bytes"])
    ):
        return {
            "index_synced": False,
            "reason": "archive_manifest_index_not_synced",
            "local_index_sha256": idx_hashes["sha256"],
            "synced_index_sha256": sync_row.get("sha256") if sync_row else None,
            "enqueued": [],
            "already_queued": [],
            "skipped_not_complete": [],
        }

    try:
        doc = json.loads(index_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return {
            "index_synced": False,
            "reason": f"archive_manifest_index_parse_error:{type(exc).__name__}",
            "enqueued": [],
            "already_queued": [],
            "skipped_not_complete": [],
        }

    raw_entries = doc.get("entries") if isinstance(doc, dict) else None
    window_ledger = doc.get("window_ledger") if isinstance(doc, dict) else None
    if not isinstance(raw_entries, list) or not isinstance(window_ledger, list):
        return {
            "index_synced": True,
            "index_sha256": idx_hashes["sha256"],
            "index_synced_at_utc": str(sync_row["synced_at_utc"]),
            "reason": "no_window_ledger_in_index",
            "enqueued": [],
            "already_queued": [],
            "skipped_not_complete": [],
        }

    verified_by_window: dict[tuple[str, str, int], list[dict[str, Any]]] = {}
    for entry in raw_entries:
        if not isinstance(entry, dict):
            continue
        if not (entry.get("verified") is True and entry.get("remote_manifest_verified", True) is True):
            continue
        e_run_id = str(entry.get("collection_run_id") or "")
        e_wid = str(entry.get("window_id") or "")
        e_pm = int(entry.get("partition_minutes") or 60)
        if not e_wid:
            continue
        verified_by_window.setdefault((e_run_id, e_wid, e_pm), []).append(entry)

    enqueued: list[dict[str, Any]] = []
    already_queued: list[dict[str, Any]] = []
    skipped_not_complete: list[dict[str, Any]] = []

    for wrow in window_ledger:
        if not isinstance(wrow, dict):
            continue
        wid = str(wrow.get("window_id") or "")
        w_pm = int(wrow.get("partition_minutes") or 60)
        w_run_id = str(wrow.get("collection_run_id") or doc.get("effective_run_id") or "")
        w_status = str(wrow.get("window_status") or "")
        file_status = str(wrow.get("file_verification_status") or "")
        cov_status = str(wrow.get("window_coverage_status") or "")

        if run_id_filter is not None and w_run_id != str(run_id_filter):
            continue
        if partition_minutes_filter is not None and w_pm != int(partition_minutes_filter):
            continue
        if max_age_hours is not None and wrow.get("planned_end_wall_ns") is not None:
            end_s = int(wrow["planned_end_wall_ns"]) / 1_000_000_000.0
            if (now_s - end_s) > float(max_age_hours) * 3600.0:
                continue

        if w_status != "COMPLETE" or file_status != "verified" or cov_status != "complete":
            skipped_not_complete.append(
                {
                    "run_id": w_run_id,
                    "target_window_utc": wid,
                    "window_status": w_status,
                    "file_verification_status": file_status,
                    "window_coverage_status": cov_status,
                }
            )
            continue

        expected_exchanges = {str(x) for x in (wrow.get("expected_exchanges") or []) if x}
        win_entries = verified_by_window.get((w_run_id, wid, w_pm), [])
        present_exchanges = {str(e.get("exchange") or "") for e in win_entries if e.get("exchange")}
        if not expected_exchanges or not expected_exchanges.issubset(present_exchanges):
            skipped_not_complete.append(
                {
                    "run_id": w_run_id,
                    "target_window_utc": wid,
                    "window_status": w_status,
                    "reason": "missing_verified_entries_in_synced_index",
                    "missing_exchanges": sorted(expected_exchanges - present_exchanges),
                }
            )
            continue

        canonical_manifests = canonical_window_manifest_entries(win_entries)
        archive_revision = compute_window_archive_revision(
            canonical_manifests,
            run_id=w_run_id or None,
            window_id=wid,
            partition_minutes=w_pm,
        )
        inserted, record = ledger.enqueue_qa_dispatch(
            run_id=w_run_id,
            target_window_utc=wid,
            archive_revision=archive_revision,
            partition_minutes=w_pm,
            window_span_id=wrow.get("window_span_id"),
            index_sha256=str(idx_hashes["sha256"]),
            index_synced_at_utc=str(sync_row["synced_at_utc"]),
            manifests=canonical_manifests,
            now_iso=now_iso,
            discovery_source=discovery_source,
        )
        item_summary = {
            "id": record["id"],
            "run_id": record["run_id"],
            "target_window_utc": record["target_window_utc"],
            "archive_revision": record["archive_revision"],
            "partition_minutes": record["partition_minutes"],
            "manifest_count": record["manifest_count"],
            "state": record["state"],
            "discovery_source": record["discovery_source"],
        }
        if inserted:
            enqueued.append(item_summary)
            ledger.record_event(
                ts_utc=now_iso,
                run_id=w_run_id or "archive_sync",
                event_type="QA_DISPATCH_ENQUEUED",
                detail={
                    "target_window_utc": wid,
                    "archive_revision": archive_revision,
                    "partition_minutes": w_pm,
                    "manifest_count": len(canonical_manifests),
                    "index_sha256": idx_hashes["sha256"],
                    "discovery_source": discovery_source,
                },
            )
        else:
            already_queued.append(item_summary)

    return {
        "index_synced": True,
        "index_sha256": str(idx_hashes["sha256"]),
        "index_synced_at_utc": str(sync_row["synced_at_utc"]),
        "enqueued": enqueued,
        "already_queued": already_queued,
        "skipped_not_complete": skipped_not_complete,
    }


def _verify_remote_object_matches(
    *,
    backend: ArchiveBackend,
    rel_path: str,
    expected_hashes: dict[str, Any],
    verify_remote: bool,
) -> tuple[RemoteObjectInfo, str]:
    """Verify remote object exists with exact size & hash (or readback SHA-256 fallback)."""
    remote = backend.stat_remote(rel_path)
    if not verify_remote:
        return remote, "skipped"
    if not remote.exists:
        raise RuntimeError(f"Remote object missing after upload: {rel_path}")
    if remote.size != int(expected_hashes["bytes"]):
        raise RuntimeError(
            f"Remote byte count mismatch for {rel_path}: {remote.size} != {expected_hashes['bytes']}"
        )
    hm = _hashes_match(expected_hashes, remote)
    if hm is True:
        return remote, "remote_native_hash"
    if hm is False:
        raise RuntimeError(f"Remote hash mismatch after upload for {rel_path}")
    rb = backend.readback_sha256(rel_path)
    if rb.lower() != str(expected_hashes["sha256"]).lower():
        raise RuntimeError(f"Remote readback SHA-256 mismatch for {rel_path}")
    return remote, "remote_readback_sha256"


def _can_safely_prune(
    row: dict[str, Any],
    *,
    dpath: Path,
    verify_remote: bool,
    prune_older_than_hours: float | None,
    now_s: float,
) -> tuple[bool, str]:
    """Check pruning eligibility guards BEFORE computing local SHA-256 (zero disk read if ineligible)."""
    if not verify_remote or prune_older_than_hours is None:
        return False, "pruning_disabled"
    if row["state"] != STATE_VERIFIED:
        return False, f"state_not_verified:{row['state']}"
    if not row["remote_data_verified"]:
        return False, "remote_data_not_verified"
    if not row["remote_manifest_verified"]:
        return False, "remote_manifest_not_verified"
    if row["has_conflict"]:
        return False, "unresolved_conflict"
    if row["last_error_code"]:
        return False, f"unresolved_error:{row['last_error_code']}"
    if not dpath.is_file():
        return False, "local_file_missing"
    st = dpath.stat()
    age_hours = (now_s - st.st_mtime) / 3600.0
    if age_hours < prune_older_than_hours:
        return False, f"within_retention:{age_hours:.3f}h<{prune_older_than_hours}h"
    return True, "eligible"


def _verify_local_integrity_before_prune(row: dict[str, Any], dpath: Path) -> dict[str, Any] | None:
    """Recompute local byte size + SHA-256 right before pruning; return conflict dict on mismatch, else None."""
    actual = file_hashes(dpath)
    expected_bytes = int(row["local_bytes"])
    expected_sha256 = str(row["local_sha256"]).lower()
    if actual["bytes"] != expected_bytes or actual["sha256"].lower() != expected_sha256:
        return {
            "remote_path": row["rel_data_path"],
            "expected_bytes": expected_bytes,
            "actual_bytes": actual["bytes"],
            "expected_sha256": expected_sha256,
            "actual_sha256": actual["sha256"],
            "action": "ISOLATED_LOCAL_SHA256_MISMATCH_BEFORE_PRUNE",
        }
    return None


def _verify_remote_single_before_prune(
    *,
    backend: ArchiveBackend,
    target: str,
    rel_path: str,
    rel_data_path: str,
    expected_remote_id: str | None,
    expected_hashes: dict[str, Any],
) -> tuple[dict[str, Any] | None, RemoteObjectInfo | None, str | None]:
    """Re-verify one remote object (data or manifest) right before pruning.

    Checks existence, remote object identity (`remote_id` when previously recorded), exact
    byte size, and exact content hash (native sha256/md5, or readback SHA-256 fallback).
    Network/API failures propagate as exceptions so callers treat them as transient errors
    (preserving local files and retrying later) rather than permanent conflicts.
    """
    remote = backend.stat_remote(rel_path)
    expected_bytes = int(expected_hashes["bytes"])
    expected_sha256 = str(expected_hashes["sha256"]).lower()
    expected_md5 = str(expected_hashes.get("md5") or "").lower()

    if not remote.exists:
        return (
            {
                "target": target,
                "remote_path": rel_path,
                "data_path": rel_data_path,
                "expected_remote_id": expected_remote_id,
                "expected_bytes": expected_bytes,
                "expected_sha256": expected_sha256,
                "action": "ISOLATED_REMOTE_MISSING_BEFORE_PRUNE",
            },
            None,
            None,
        )

    if expected_remote_id and remote.remote_id != expected_remote_id:
        return (
            {
                "target": target,
                "remote_path": rel_path,
                "data_path": rel_data_path,
                "expected_remote_id": expected_remote_id,
                "actual_remote_id": remote.remote_id,
                "expected_bytes": expected_bytes,
                "actual_bytes": remote.size,
                "expected_sha256": expected_sha256,
                "actual_sha256": remote.sha256,
                "actual_md5": remote.md5,
                "action": "ISOLATED_REMOTE_ID_MISMATCH_BEFORE_PRUNE",
            },
            None,
            None,
        )

    if remote.size != expected_bytes:
        return (
            {
                "target": target,
                "remote_path": rel_path,
                "data_path": rel_data_path,
                "expected_remote_id": expected_remote_id,
                "actual_remote_id": remote.remote_id,
                "expected_bytes": expected_bytes,
                "actual_bytes": remote.size,
                "expected_sha256": expected_sha256,
                "actual_sha256": remote.sha256,
                "actual_md5": remote.md5,
                "action": "ISOLATED_REMOTE_SIZE_MISMATCH_BEFORE_PRUNE",
            },
            None,
            None,
        )

    hm = _hashes_match(expected_hashes, remote)
    if hm is True:
        return None, remote, "remote_native_hash"
    if hm is False:
        return (
            {
                "target": target,
                "remote_path": rel_path,
                "data_path": rel_data_path,
                "expected_remote_id": expected_remote_id,
                "actual_remote_id": remote.remote_id,
                "expected_bytes": expected_bytes,
                "actual_bytes": remote.size,
                "expected_sha256": expected_sha256,
                "expected_md5": expected_md5,
                "actual_sha256": remote.sha256,
                "actual_md5": remote.md5,
                "action": "ISOLATED_REMOTE_HASH_MISMATCH_BEFORE_PRUNE",
            },
            None,
            None,
        )

    rb = backend.readback_sha256(rel_path)
    if rb.lower() != expected_sha256:
        return (
            {
                "target": target,
                "remote_path": rel_path,
                "data_path": rel_data_path,
                "expected_remote_id": expected_remote_id,
                "actual_remote_id": remote.remote_id,
                "expected_bytes": expected_bytes,
                "actual_bytes": remote.size,
                "expected_sha256": expected_sha256,
                "actual_readback_sha256": rb,
                "action": "ISOLATED_REMOTE_HASH_MISMATCH_BEFORE_PRUNE",
            },
            None,
            None,
        )
    return None, remote, "remote_readback_sha256"


def _verify_remote_integrity_before_prune(
    row: dict[str, Any],
    backend: ArchiveBackend,
) -> tuple[dict[str, Any] | None, tuple[RemoteObjectInfo, str] | None, tuple[RemoteObjectInfo, str] | None]:
    """Re-verify BOTH remote data and remote manifest (object ID, size, hash) right before pruning."""
    rel_data = str(row["rel_data_path"])
    rel_manifest = str(row["rel_manifest_path"])
    expected_data = {
        "bytes": int(row["local_bytes"]),
        "sha256": str(row["local_sha256"]),
        "md5": str(row["local_md5"]),
    }
    expected_mf = {
        "bytes": int(row["manifest_bytes"]),
        "sha256": str(row["manifest_sha256"]),
        "md5": str(row["manifest_md5"]),
    }

    data_conflict, remote_data, data_method = _verify_remote_single_before_prune(
        backend=backend,
        target="data",
        rel_path=rel_data,
        rel_data_path=rel_data,
        expected_remote_id=row.get("remote_data_id"),
        expected_hashes=expected_data,
    )
    if data_conflict is not None:
        return data_conflict, None, None

    mf_conflict, remote_mf, mf_method = _verify_remote_single_before_prune(
        backend=backend,
        target="manifest",
        rel_path=rel_manifest,
        rel_data_path=rel_data,
        expected_remote_id=row.get("remote_manifest_id"),
        expected_hashes=expected_mf,
    )
    if mf_conflict is not None:
        return mf_conflict, None, None

    assert remote_data is not None and data_method is not None
    assert remote_mf is not None and mf_method is not None
    return None, (remote_data, data_method), (remote_mf, mf_method)


def restore_pruned_partition(
    *,
    rel_data_path: str,
    expected_bytes: int,
    expected_sha256: str,
    backend: ArchiveBackend,
    cache_dir: Path,
) -> Path:
    """Download a PRUNED/VERIFIED remote `.tsv.zst` into cache_dir and verify zstd + SHA-256.

    Fails closed on any size, SHA-256, or zstd corruption mismatch and never leaves corrupt
    files at the final cache path.
    """
    dest = cache_dir / rel_data_path
    if dest.is_file():
        existing_hashes = file_hashes(dest)
        if existing_hashes["bytes"] == expected_bytes and existing_hashes["sha256"].lower() == expected_sha256.lower():
            verify_zstd_integrity(dest)
            return dest

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dest.with_name(f"{dest.name}.restore.{os.getpid()}.partial")
    try:
        backend.download_file(rel_data_path, tmp_path)
        verify_zstd_integrity(tmp_path)
        actual = file_hashes(tmp_path)
        if actual["bytes"] != expected_bytes:
            raise ValueError(
                f"Restored byte mismatch for {rel_data_path}: actual={actual['bytes']} expected={expected_bytes}"
            )
        if actual["sha256"].lower() != expected_sha256.lower():
            raise ValueError(
                f"Restored SHA-256 mismatch for {rel_data_path}: actual={actual['sha256']} expected={expected_sha256}"
            )
        os.replace(tmp_path, dest)
        return dest
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass


def _find_window_files(
    raw_root: Path,
    sealed_root: Path,
    exchanges: set[str],
    win_desc: dict[str, Any],
) -> tuple[list[str], list[str]]:
    """Locate leftover `.tsv.zst.partial` files and `.tsv.zst` files missing `.manifest.json`
    matching the expected planned UTC window (both legacy hourly and sub-hour/segmented names).
    """
    from data_coverage import parse_partition_window_from_rel_path

    expected_date = win_desc["date_utc"]
    expected_window_id = win_desc["window_id"]
    expected_pm = int(win_desc["partition_minutes"])
    expected_hour = win_desc["hour_utc"]
    unsealed_partials: list[str] = []
    sealed_without_manifest: list[str] = []

    for exchange in sorted(exchanges):
        day_dir = raw_root / exchange / expected_date
        if not day_dir.is_dir():
            continue
        for entry in sorted(day_dir.iterdir()):
            name = entry.name
            if not (name.endswith(".tsv.zst.partial") or name.endswith(".tsv.zst")):
                continue
            rel = entry.relative_to(sealed_root).as_posix()
            parsed = parse_partition_window_from_rel_path(rel)
            matches_window = False
            if parsed is not None:
                if parsed["window_id"] == expected_window_id and int(parsed["partition_minutes"]) == expected_pm:
                    matches_window = True
                elif expected_pm == 60 and parsed["date_utc"] == expected_date and parsed["hour_utc"] == expected_hour:
                    matches_window = True
            if not matches_window:
                continue
            if name.endswith(".tsv.zst.partial"):
                unsealed_partials.append(rel)
            elif name.endswith(".tsv.zst"):
                mf_path = entry.with_name(name.removesuffix(".tsv.zst") + ".manifest.json")
                if not mf_path.is_file():
                    sealed_without_manifest.append(rel)

    return unsealed_partials, sealed_without_manifest


def _row_planned_start_s(row: dict[str, Any]) -> int:
    raw_start = row.get("planned_start_wall_ns")
    if raw_start is not None:
        return int(raw_start) // 1_000_000_000
    day_s = int(datetime.fromisoformat(str(row["date_utc"])).replace(tzinfo=timezone.utc).timestamp())
    hh = int(row.get("hour_utc") or 0)
    mm = int(row.get("minute_utc") or 0)
    return day_s + hh * 3600 + mm * 60


def _row_matches_window_and_batch(
    row: dict[str, Any],
    win_desc: dict[str, Any],
    expected_run_id: str | None,
) -> tuple[bool, str | None]:
    """Check whether a ledger partition row belongs to the exact planned UTC window, duration, and batch."""
    expected_pm = int(win_desc["partition_minutes"])
    row_pm = int(row.get("partition_minutes") or 60)
    if row_pm != expected_pm:
        return False, None

    p_start_ns = int(win_desc["planned_start_wall_ns"])
    p_end_ns = int(win_desc["planned_end_wall_ns"])
    if row.get("planned_start_wall_ns") is not None and row.get("planned_end_wall_ns") is not None:
        if int(row["planned_start_wall_ns"]) != p_start_ns or int(row["planned_end_wall_ns"]) != p_end_ns:
            return False, None
    else:
        row_min = str(row.get("minute_utc") or "00").zfill(2)
        row_wid = row.get("window_id") or f"{row['date_utc']}T{row['hour_utc']}:{row_min}Z"
        matches_legacy = (row_wid == win_desc["window_id"]) or (
            expected_pm == 60
            and row["date_utc"] == win_desc["date_utc"]
            and str(row["hour_utc"]).zfill(2) == win_desc["hour_utc"]
        )
        if not matches_legacy:
            return False, None

    raw_run_id = row.get("collection_run_id")
    eff_run_id = str(raw_run_id) if raw_run_id not in (None, "") else None
    if expected_run_id is not None:
        boot_str = str(row.get("boot_id") or "")
        if eff_run_id != expected_run_id and not (
            eff_run_id is None and (boot_str == expected_run_id or boot_str.startswith(f"{expected_run_id}_"))
        ):
            return False, eff_run_id
    return True, eff_run_id


def _reconcile_single_window(
    *,
    win_desc: dict[str, Any],
    expected_exchanges: set[str],
    expected_run_id: str | None,
    all_verified_rows: list[dict[str, Any]],
    pending_unverified_rows: list[dict[str, Any]],
    manifests_with_data: set[str],
    raw_root: Path,
    sealed_root: Path,
    max_coverage_gap_seconds: float,
    seal_wait_attempts: int = 0,
    seal_waited_seconds: float = 0.0,
    verify_remote: bool = True,
    market_scan_cache: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Evaluate BOTH file verification and whole-window data coverage for one expected UTC window."""
    expected_date = win_desc["date_utc"]
    expected_hour = win_desc["hour_utc"]
    expected_window_id = win_desc["window_id"]
    expected_pm = int(win_desc["partition_minutes"])
    p_start_ns = int(win_desc["planned_start_wall_ns"])
    p_end_ns = int(win_desc["planned_end_wall_ns"])
    window_label = f"{expected_date}T{expected_hour}Z" if expected_pm == 60 else expected_window_id
    max_gap_ns = int(max_coverage_gap_seconds * 1_000_000_000)
    scan_cache = market_scan_cache if market_scan_cache is not None else {}

    win_errors: list[str] = []
    if not verify_remote:
        win_errors.append("verify_remote_required_when_config_enforced")

    matching_verified: list[tuple[dict[str, Any], str]] = []
    for row in all_verified_rows:
        ex = str(row["exchange"])
        if ex not in expected_exchanges:
            continue
        if not (row["remote_data_verified"] and row["remote_manifest_verified"]):
            continue
        if row["has_conflict"] or row["last_error_code"]:
            continue
        matches_win, eff_batch = _row_matches_window_and_batch(row, win_desc, expected_run_id)
        if not matches_win:
            continue

        r_eff = dict(row)
        rel_data = str(r_eff.get("rel_data_path") or "")
        data_sha = r_eff.get("local_sha256")
        scan_key = str(data_sha or rel_data)
        cached = scan_cache.get(scan_key)
        if cached is None and rel_data:
            dpath = sealed_root / rel_data
            if dpath.is_file():
                try:
                    if data_sha and file_hashes(dpath)["sha256"] != data_sha:
                        win_errors.append(f"market_scan_local_hash_mismatch:{rel_data}")
                        continue
                    cached = _scan_market_data_timestamps(dpath)
                    scan_cache[scan_key] = cached
                except (OSError, zstd.ZstdError) as exc:
                    win_errors.append(f"market_scan_failed:{rel_data}:{type(exc).__name__}")
                    continue
        if cached and cached.get("parsed_tsv"):
            r_eff["data_lines"] = int(cached["data_lines"])
            r_eff["start_wall_ns"] = cached["start_wall_ns"]
            r_eff["end_wall_ns"] = cached["end_wall_ns"]
            r_eff["_max_internal_gap_ns"] = int(cached["max_internal_gap_ns"])

        has_data = int(r_eff.get("data_lines") or 0) > 0 or (
            not (rel_data and (sealed_root / rel_data).is_file())
            and r_eff.get("rel_manifest_path") in manifests_with_data
        )
        if not has_data:
            continue
        matching_verified.append((r_eff, eff_batch or "__default__"))

    # Enforce strict batch isolation: never merge segments across different collection_run_id batches
    distinct_batches = sorted({b for _, b in matching_verified})
    mixed_batches: list[str] = []
    selected_batch: str | None = None
    if len(distinct_batches) > 1:
        mixed_batches = [b for b in distinct_batches]
        win_errors.append(
            f"mixed_batches_in_expected_window:{window_label}:{','.join(mixed_batches)}"
        )
        # Pick the single batch with the most distinct exchanges (never union across batches!)
        by_batch_ex: dict[str, set[str]] = {b: set() for b in distinct_batches}
        for row, b in matching_verified:
            by_batch_ex[b].add(str(row["exchange"]))
        selected_batch = max(distinct_batches, key=lambda b: (len(by_batch_ex[b]), b))
    elif len(distinct_batches) == 1:
        selected_batch = distinct_batches[0]

    per_exchange_segments: dict[str, list[dict[str, Any]]] = {
        ex: [] for ex in sorted(expected_exchanges)
    }
    for row, b in matching_verified:
        if selected_batch is None or b == selected_batch:
            per_exchange_segments[str(row["exchange"])].append(row)

    unsealed_partials, sealed_without_manifest = _find_window_files(
        raw_root, sealed_root, expected_exchanges, win_desc
    )
    for rel_p in sealed_without_manifest:
        win_errors.append(f"sealed_file_without_manifest:{rel_p}")
    for rel_part in unsealed_partials:
        win_errors.append(f"unsealed_partial_in_expected_hour:{rel_part}")

    exchanges_with_unverified_local: set[str] = set()
    for rel_p in unsealed_partials + sealed_without_manifest:
        parts = rel_p.split("/")
        if len(parts) >= 2:
            exchanges_with_unverified_local.add(parts[1])
    for prow in pending_unverified_rows:
        m_ok, _ = _row_matches_window_and_batch(prow, win_desc, expected_run_id)
        if m_ok:
            exchanges_with_unverified_local.add(str(prow["exchange"]))

    file_verified: set[str] = set()
    covered_exchanges: set[str] = set()
    incomplete_coverage_exchanges: list[str] = []
    coverage_gaps: dict[str, dict[str, Any]] = {}
    segment_summaries: dict[str, dict[str, Any]] = {}
    overlaps: list[dict[str, Any]] = []
    startup_stub_segments: list[dict[str, Any]] = []

    for ex in sorted(expected_exchanges):
        segs = sorted(
            per_exchange_segments[ex],
            key=lambda r: (
                r.get("start_wall_ns") if r.get("start_wall_ns") is not None else 0,
                r.get("first_rx_seq") if r.get("first_rx_seq") is not None else 0,
                r["rel_data_path"],
            ),
        )
        for r in segs:
            if bool(r.get("is_startup_stub")):
                startup_stub_segments.append(
                    {
                        "exchange": ex,
                        "window_id": expected_window_id,
                        "rel_data_path": r["rel_data_path"],
                        "rel_manifest_path": r.get("rel_manifest_path"),
                        "boot_id": r["boot_id"],
                        "collection_run_id": r.get("collection_run_id"),
                        "start_wall_ns": r.get("start_wall_ns"),
                        "end_wall_ns": r.get("end_wall_ns"),
                        "data_lines": int(r.get("data_lines") or 0),
                    }
                )

        ex_has_overlap = False
        for prev_r, cur_r in zip(segs, segs[1:]):
            same_boot_seq_overlap = (
                prev_r["boot_id"] == cur_r["boot_id"]
                and prev_r.get("last_rx_seq") is not None
                and cur_r.get("first_rx_seq") is not None
                and int(cur_r["first_rx_seq"]) <= int(prev_r["last_rx_seq"])
            )
            wall_overlap = (
                prev_r.get("end_wall_ns") is not None
                and cur_r.get("start_wall_ns") is not None
                and int(cur_r["start_wall_ns"]) < int(prev_r["end_wall_ns"])
            )
            if same_boot_seq_overlap or wall_overlap:
                ov = {
                    "exchange": ex,
                    "window_id": expected_window_id,
                    "first_segment": prev_r["rel_data_path"],
                    "second_segment": cur_r["rel_data_path"],
                    "same_boot_seq_overlap": same_boot_seq_overlap,
                    "wall_overlap": wall_overlap,
                }
                overlaps.append(ov)
                ex_has_overlap = True
                if same_boot_seq_overlap:
                    win_errors.append(
                        f"segment_seq_overlap_in_expected_window:{ex}:{expected_window_id}"
                    )
                if wall_overlap:
                    win_errors.append(
                        f"segment_wall_overlap_in_expected_window:{ex}:{expected_window_id}"
                    )

        if segs and ex not in exchanges_with_unverified_local and not ex_has_overlap:
            file_verified.add(ex)

        total_ex_data_lines = sum(int(r.get("data_lines") or 0) for r in segs)
        starts = [int(r["start_wall_ns"]) for r in segs if r.get("start_wall_ns") is not None]
        ends = [int(r["end_wall_ns"]) for r in segs if r.get("end_wall_ns") is not None]
        head_gap_ns = max(0, min(starts) - p_start_ns) if starts else (p_end_ns - p_start_ns)
        tail_gap_ns = max(0, p_end_ns - max(ends)) if ends else (p_end_ns - p_start_ns)
        inter_gaps_ns = [
            max(0, int(cur["start_wall_ns"]) - int(prev["end_wall_ns"]))
            for prev, cur in zip(segs, segs[1:])
            if prev.get("end_wall_ns") is not None and cur.get("start_wall_ns") is not None
        ]
        intra_gaps_ns = [int(seg.get("_max_internal_gap_ns") or 0) for seg in segs]
        max_internal_gap_ns = max(inter_gaps_ns + intra_gaps_ns, default=0)

        gap_reasons: list[str] = []
        if segs:
            if not starts or not ends:
                gap_reasons.append("missing_segment_timestamps")
            else:
                if total_ex_data_lines < 2 or max(ends) <= min(starts):
                    gap_reasons.append("insufficient_market_data_records")
                if head_gap_ns > max_gap_ns or bool(segs[0].get("is_startup_stub")):
                    gap_reasons.append(f"head_gap_{head_gap_ns / 1e9:.1f}s")
                if tail_gap_ns > max_gap_ns or bool(segs[-1].get("settled_by_timer")):
                    gap_reasons.append(f"tail_gap_{tail_gap_ns / 1e9:.1f}s")
                if max_internal_gap_ns > max_gap_ns:
                    gap_reasons.append(f"internal_gap_{max_internal_gap_ns / 1e9:.1f}s")
                if len(segs) == 1 and bool(segs[0].get("coverage_incomplete")) and not gap_reasons:
                    gap_reasons.append("manifest_marked_coverage_incomplete")

        if ex in file_verified and not gap_reasons and not mixed_batches:
            covered_exchanges.add(ex)
        elif segs:
            incomplete_coverage_exchanges.append(ex)
            coverage_gaps[ex] = {
                "head_gap_seconds": round(head_gap_ns / 1e9, 3),
                "tail_gap_seconds": round(tail_gap_ns / 1e9, 3),
                "max_internal_gap_seconds": round(max_internal_gap_ns / 1e9, 3),
                "reasons": gap_reasons or ["unverified_or_mixed_batch"],
            }
            if gap_reasons:
                win_errors.append(
                    f"expected_window_coverage_incomplete:{ex}:{window_label}:{','.join(gap_reasons)}"
                )

        segment_summaries[ex] = {
            "segment_count": len(segs),
            "segments": [r["rel_data_path"] for r in segs],
            "boot_ids": sorted({str(r["boot_id"]) for r in segs}),
            "collection_run_ids": sorted({str(r.get("collection_run_id") or "") for r in segs if r.get("collection_run_id")}),
            "total_data_lines": total_ex_data_lines,
            "merged_start_wall_ns": min(starts) if starts else None,
            "merged_end_wall_ns": max(ends) if ends else None,
            "head_gap_seconds": round(head_gap_ns / 1e9, 3) if starts else None,
            "tail_gap_seconds": round(tail_gap_ns / 1e9, 3) if ends else None,
            "max_internal_gap_seconds": round(max_internal_gap_ns / 1e9, 3) if starts else 0.0,
            "has_startup_stub": any(bool(r.get("is_startup_stub")) for r in segs),
            "settled_by_timer": any(bool(r.get("settled_by_timer")) for r in segs),
            "file_verified": ex in file_verified,
            "coverage_complete": ex in covered_exchanges,
        }

    missing_exchanges = sorted(expected_exchanges - file_verified)
    win_errors.extend(
        f"expected_hour_not_verified:{exchange}:{window_label}"
        for exchange in missing_exchanges
    )

    file_verification_status = (
        "verified"
        if (
            not missing_exchanges
            and not unsealed_partials
            and not sealed_without_manifest
            and not overlaps
            and not mixed_batches
            and verify_remote
        )
        else "pending"
    )
    if file_verification_status == "verified" and len(covered_exchanges) == len(expected_exchanges) and not incomplete_coverage_exchanges:
        window_coverage_status = "complete"
    elif len(file_verified) == 0:
        window_coverage_status = "missing"
    else:
        window_coverage_status = "incomplete"

    window_status = (
        "COMPLETE"
        if (file_verification_status == "verified" and window_coverage_status == "complete")
        else "PENDING"
    )
    if window_status != "COMPLETE" and not win_errors:
        win_errors.append(
            f"expected_window_not_complete:{window_label}:file={file_verification_status},coverage={window_coverage_status}"
        )

    # Check if this window is a verified initial startup-stub window (all local files verified,
    # contains startup stub segment(s), no unsealed partials/overlaps/mixed batches, and no
    # non-stub exchange suffered a tail/internal outage).
    is_startup_stub_candidate = bool(
        len(file_verified) > 0
        and len(startup_stub_segments) > 0
        and not unsealed_partials
        and not sealed_without_manifest
        and not overlaps
        and not mixed_batches
        and not exchanges_with_unverified_local
        and verify_remote
        and all(
            (ex in covered_exchanges)
            or (
                segment_summaries[ex]["has_startup_stub"]
                and not segment_summaries[ex]["settled_by_timer"]
                and all(
                    not r.startswith("internal_gap_")
                    and (not r.startswith("tail_gap_") or segment_summaries[ex]["total_data_lines"] < 2)
                    for r in coverage_gaps.get(ex, {}).get("reasons", [])
                )
            )
            for ex in file_verified
        )
    )

    return {
        "expected_hour_utc": window_label,
        "expected_window_id": expected_window_id,
        "window_span_id": win_desc.get("window_span_id"),
        "planned_start_wall_ns": p_start_ns,
        "planned_end_wall_ns": p_end_ns,
        "partition_minutes": expected_pm,
        "expected_run_id": expected_run_id,
        "observed_batches": distinct_batches,
        "mixed_batches": mixed_batches,
        "expected_exchanges": sorted(expected_exchanges),
        "verified_exchanges": sorted(file_verified),
        "file_verified_exchanges": sorted(file_verified),
        "covered_exchanges": sorted(covered_exchanges),
        "missing_exchanges": missing_exchanges,
        "incomplete_coverage_exchanges": sorted(incomplete_coverage_exchanges),
        "coverage_gaps": coverage_gaps,
        "file_verification_status": file_verification_status,
        "window_coverage_status": window_coverage_status,
        "window_status": window_status,
        "is_startup_stub_candidate": is_startup_stub_candidate,
        "startup_stub_segments": startup_stub_segments,
        "per_exchange_segments": segment_summaries,
        "overlaps": overlaps,
        "unsealed_partials": unsealed_partials,
        "sealed_without_manifest": sealed_without_manifest,
        "seal_wait_attempts": seal_wait_attempts,
        "seal_waited_seconds": round(seal_waited_seconds, 3),
        "verify_remote_enforced": bool(verify_remote),
        "errors": win_errors,
    }


def sync_sealed_directory(
    sealed_root: Path,
    backend: ArchiveBackend,
    verify_remote: bool = True,
    prune_older_than_hours: float | None = None,
    now_epoch_s: float | None = None,
    *,
    upload_ledger_snapshot: bool = True,
    expected_hourly_exchanges: set[str] | None = None,
    partition_minutes: int = 60,
    expected_run_id: str | None = None,
    cutover_window_utc: str | None = None,
    max_coverage_gap_seconds: float | None = None,
    seal_wait_seconds: float = 0.0,
    seal_poll_interval_seconds: float = 1.0,
    sleeper: Callable[[float], None] = time.sleep,
    fault_hooks: dict[str, Callable[[str], None]] | None = None,
) -> dict[str, Any]:
    """Incrementally verify, upload, remote-verify, snapshot-backup, and safely prune partitions.

    Supports configurable UTC windows (`partition_minutes` in `{5, 15, 30, 60}`), bounded wait for
    asynchronous sealing, multi-segment per-exchange reconciliation, separate file-verification vs
    whole-window coverage checks, and persistent catch-up of prior unfinished/missing windows.

    `fault_hooks` (strictly for deterministic fault-injection testing) supports optional callbacks:
      - `before_verify_commit(rel_data_path)`
      - `after_verify_before_prune(rel_data_path)`
      - `after_prune_unlink_before_commit(rel_data_path)`
    """
    from data_coverage import previous_complete_window

    now_s = time.time() if now_epoch_s is None else now_epoch_s
    now_iso = _iso_utc(now_s)
    run_id = f"arch_{int(now_s)}_{uuid.uuid4().hex[:8]}"
    hooks = fault_hooks or {}

    ledger_db_path = sealed_root / LEDGER_DB_FILENAME
    ledger_jsonl_path = sealed_root / PROJECTION_LEDGER_JSONL_FILENAME
    legacy_jsonl_path = sealed_root / LEGACY_LEDGER_JSONL_FILENAME
    lock_path = sealed_root / ARCHIVE_LOCK_FILENAME
    raw_root = sealed_root / "raw"

    lock = ArchiveDirectoryLock(lock_path)
    try:
        lock.acquire()
    except ArchiveLockBusyError as exc:
        return {
            "status": "blocked",
            "phase": "archive",
            "lock_busy": True,
            "failed_items": [f"archive_lock_busy:{exc}"],
            "minimal_fixes": ["Wait for the active archive run to finish; never run concurrent archive jobs."],
            "ledger_db_path": str(ledger_db_path),
            "ledger_path": str(ledger_jsonl_path),
            "projection_ledger_path": str(ledger_jsonl_path),
            "legacy_ledger_path": str(legacy_jsonl_path) if legacy_jsonl_path.is_file() else None,
        }

    try:
        proxy_info: dict[str, Any] | None = None
        if hasattr(backend, "resolve_proxy"):
            proxy_info = backend.resolve_proxy()

        expected_win_desc = (
            previous_complete_window(now_s, int(partition_minutes))
            if expected_hourly_exchanges is not None
            else None
        )
        seal_wait_attempts = 0
        seal_waited_seconds = 0.0
        if (
            expected_hourly_exchanges is not None
            and expected_win_desc is not None
            and seal_wait_seconds > 0
            and raw_root.exists()
        ):
            poll_step = max(0.05, float(seal_poll_interval_seconds))
            while seal_waited_seconds < float(seal_wait_seconds):
                pending_partials, pending_no_mf = _find_window_files(
                    raw_root, sealed_root, expected_hourly_exchanges, expected_win_desc
                )
                if not pending_partials and not pending_no_mf:
                    break
                step = min(poll_step, float(seal_wait_seconds) - seal_waited_seconds)
                sleeper(step)
                seal_wait_attempts += 1
                seal_waited_seconds += step

        manifests = sorted(raw_root.rglob("*.manifest.json")) if raw_root.exists() else []
        if not manifests and expected_hourly_exchanges is None:
            return {
                "status": "blocked",
                "phase": "archive",
                "proxy": proxy_info,
                "seal_wait_attempts": seal_wait_attempts,
                "seal_waited_seconds": round(seal_waited_seconds, 3),
                "failed_items": [f"No sealed *.manifest.json found under {raw_root}"],
                "minimal_fixes": ["Seal at least one partition before invoking archive"],
                "ledger_db_path": str(ledger_db_path),
                "ledger_path": str(ledger_jsonl_path),
                "projection_ledger_path": str(ledger_jsonl_path),
                "legacy_ledger_path": str(legacy_jsonl_path) if legacy_jsonl_path.is_file() else None,
            }

        with ArchiveLedger(ledger_db_path) as ledger:
            migration_summary = migrate_legacy_jsonl_ledger(
                sealed_root, ledger, now_s=now_s, run_id=run_id
            )
            if proxy_info and proxy_info.get("fallback_used"):
                ledger.record_event(
                    ts_utc=now_iso,
                    run_id=run_id,
                    event_type="PROXY_FALLBACK_DIRECT",
                    detail=proxy_info,
                )

            newly_verified_entries: list[dict[str, Any]] = []
            conflicts: list[dict[str, Any]] = []
            errors: list[str] = []
            pruned: list[str] = []
            recovered_pruned: list[str] = []
            skipped_pruned = 0
            skipped_verified = 0
            manifests_with_data: set[str] = set()

            for mpath in manifests:
                try:
                    manifest = json.loads(mpath.read_text(encoding="utf-8"))
                    ident = _derive_partition_identity(sealed_root, mpath, manifest)
                    if expected_hourly_exchanges is not None and (
                        int(manifest.get("data_lines") or 0) + int(manifest.get("data_escaped_lines") or 0) > 0
                    ):
                        manifests_with_data.add(ident["rel_manifest_path"])
                except Exception as exc:
                    errors.append(f"{mpath}: manifest_parse_error: {type(exc).__name__}: {exc}")
                    continue

                dpath: Path = ident["data_path"]
                rel_data: str = ident["rel_data_path"]
                rel_manifest: str = ident["rel_manifest_path"]
                row = ledger.get_partition_by_rel_data(rel_data)

                # --- Fast-path 1: Already PRUNED ---
                if row is not None and row["state"] == STATE_PRUNED:
                    if not dpath.is_file():
                        if row["remote_data_verified"] and row["remote_manifest_verified"] and not row["has_conflict"]:
                            skipped_pruned += 1
                            continue
                        msg = f"{rel_data}: PRUNED row lacks valid dual remote verification proof"
                        ledger.mark_error(
                            rel_data_path=rel_data,
                            error_code="INVALID_PRUNED_PROOF",
                            error_message=msg,
                            now_iso=now_iso,
                            run_id=run_id,
                            event_type="CRITICAL_INTEGRITY_ERROR",
                        )
                        errors.append(msg)
                        continue
                    # Unexpected: local data file reappeared at an already-PRUNED path
                    reappeared = file_hashes(dpath)
                    if reappeared["sha256"] != row["local_sha256"] or reappeared["bytes"] != int(row["local_bytes"]):
                        conflict = {
                            "remote_path": rel_data,
                            "local_sha256": reappeared["sha256"],
                            "ledger_sha256": row["local_sha256"],
                            "action": "ISOLATED_REAPPEARED_PRUNED_MISMATCH",
                        }
                        ledger.mark_conflict(
                            rel_data_path=rel_data,
                            conflict_detail=conflict,
                            now_iso=now_iso,
                            run_id=run_id,
                        )
                        conflicts.append(conflict)
                        continue
                    skipped_pruned += 1
                    continue

                # --- Fast-path 2: Already VERIFIED (skip re-reading/re-uploading; handle prune or crash recovery) ---
                if row is not None and row["state"] == STATE_VERIFIED:
                    if dpath.is_file():
                        if row["has_conflict"]:
                            # Unresolved conflict on a VERIFIED partition (e.g. pre-prune local/remote drift):
                            # never prune and continue surfacing blocked status without redundant remote calls.
                            try:
                                saved_conflict = json.loads(row["conflict_detail_json"] or "{}")
                                if not isinstance(saved_conflict, dict) or not saved_conflict.get("remote_path"):
                                    saved_conflict = {
                                        "remote_path": rel_data,
                                        "action": row["last_error_message"] or "UNRESOLVED_CONFLICT",
                                    }
                            except Exception:
                                saved_conflict = {
                                    "remote_path": rel_data,
                                    "action": row["last_error_message"] or "UNRESOLVED_CONFLICT",
                                }
                            conflicts.append(saved_conflict)
                            continue

                        remote_reverified_proof: tuple[RemoteObjectInfo, str, RemoteObjectInfo, str] | None = None
                        if row["last_error_code"] and not row["has_conflict"] and verify_remote:
                            try:
                                remote_conflict, rd_pair, rm_pair = _verify_remote_integrity_before_prune(row, backend)
                                if remote_conflict is not None:
                                    ledger.mark_conflict(
                                        rel_data_path=rel_data,
                                        conflict_detail=remote_conflict,
                                        now_iso=now_iso,
                                        run_id=run_id,
                                    )
                                    conflicts.append(remote_conflict)
                                    continue
                                assert rd_pair is not None and rm_pair is not None
                                rd_recheck, d_m = rd_pair
                                rm_recheck, m_m = rm_pair
                                row = ledger.mark_verified(
                                    rel_data_path=rel_data,
                                    remote_data=rd_recheck,
                                    data_method=d_m,
                                    remote_manifest=rm_recheck,
                                    manifest_method=m_m,
                                    now_iso=now_iso,
                                    run_id=run_id,
                                )
                                remote_reverified_proof = (rd_recheck, d_m, rm_recheck, m_m)
                            except Exception as exc:
                                ledger.mark_error(
                                    rel_data_path=rel_data,
                                    error_code="REVERIFY_BEFORE_PRUNE_FAILED",
                                    error_message=f"{type(exc).__name__}: {exc}",
                                    now_iso=now_iso,
                                    run_id=run_id,
                                )
                                errors.append(f"{rel_data}: {type(exc).__name__}: {exc}")
                                continue
                        can_prune, _reason = _can_safely_prune(
                            row,
                            dpath=dpath,
                            verify_remote=verify_remote,
                            prune_older_than_hours=prune_older_than_hours,
                            now_s=now_s,
                        )
                        if not can_prune:
                            skipped_verified += 1
                            continue
                        try:
                            # 1. Re-verify exact local content SHA-256 right before deletion (never on normal
                            # within-retention runs). Same-size tampering/corruption blocks pruning!
                            integrity_conflict = _verify_local_integrity_before_prune(row, dpath)
                            if integrity_conflict is not None:
                                ledger.mark_conflict(
                                    rel_data_path=rel_data,
                                    conflict_detail=integrity_conflict,
                                    now_iso=now_iso,
                                    run_id=run_id,
                                )
                                conflicts.append(integrity_conflict)
                                continue

                            # 2. Re-verify BOTH remote data and remote manifest (object identity, byte size,
                            # and hash/readback) right before deletion. Within retention, zero remote requests
                            # are issued; on remote deletion, replacement, corruption, or API/network outage,
                            # the local file is preserved.
                            if remote_reverified_proof is None:
                                try:
                                    remote_conflict, rd_pair, rm_pair = _verify_remote_integrity_before_prune(
                                        row, backend
                                    )
                                except Exception as exc:
                                    ledger.mark_error(
                                        rel_data_path=rel_data,
                                        error_code="PRE_PRUNE_REMOTE_CHECK_FAILED",
                                        error_message=f"{type(exc).__name__}: {exc}",
                                        now_iso=now_iso,
                                        run_id=run_id,
                                    )
                                    errors.append(f"{rel_data}: {type(exc).__name__}: {exc}")
                                    continue
                                if remote_conflict is not None:
                                    ledger.mark_conflict(
                                        rel_data_path=rel_data,
                                        conflict_detail=remote_conflict,
                                        now_iso=now_iso,
                                        run_id=run_id,
                                    )
                                    conflicts.append(remote_conflict)
                                    continue
                                assert rd_pair is not None and rm_pair is not None
                                remote_reverified_proof = (rd_pair[0], rd_pair[1], rm_pair[0], rm_pair[1])

                            rd_ok, d_method_ok, rm_ok, m_method_ok = remote_reverified_proof
                            prune_detail = {
                                "prune_older_than_hours": prune_older_than_hours,
                                "pre_prune_remote_verified": True,
                                "remote_data_id": rd_ok.remote_id,
                                "data_method": d_method_ok,
                                "remote_manifest_id": rm_ok.remote_id,
                                "manifest_method": m_method_ok,
                            }
                            ledger.record_event(
                                ts_utc=now_iso,
                                run_id=run_id,
                                exchange=row["exchange"],
                                date_utc=row["date_utc"],
                                hour_utc=row["hour_utc"],
                                boot_id=row["boot_id"],
                                partition_id=row["partition_id"],
                                rel_data_path=rel_data,
                                event_type="PRUNE_INTENT",
                                from_state=STATE_VERIFIED,
                                to_state=STATE_PRUNED,
                                detail=prune_detail,
                            )
                            dpath.unlink()
                            if "after_prune_unlink_before_commit" in hooks:
                                hooks["after_prune_unlink_before_commit"](rel_data)
                            ledger.mark_pruned(
                                rel_data_path=rel_data,
                                now_iso=now_iso,
                                run_id=run_id,
                                event_type="PRUNED_OK",
                                detail=prune_detail,
                            )
                            pruned.append(rel_data)
                        except Exception as exc:
                            ledger.mark_error(
                                rel_data_path=rel_data,
                                error_code="PRUNE_FAILED",
                                error_message=f"{type(exc).__name__}: {exc}",
                                now_iso=now_iso,
                                run_id=run_id,
                            )
                            errors.append(f"{rel_data}: {type(exc).__name__}: {exc}")
                        continue

                    # Local .tsv.zst is MISSING while row is VERIFIED!
                    # Distinguish interrupted prune (PRUNE_INTENT present) vs unexpected local data loss.
                    try:
                        if ledger.has_pending_prune_intent(rel_data):
                            if not verify_remote:
                                raise RuntimeError(
                                    f"Cannot recover interrupted prune for {rel_data} with verify_remote=False"
                                )
                            remote_conflict, rd_pair, rm_pair = _verify_remote_integrity_before_prune(row, backend)
                            if remote_conflict is not None:
                                ledger.mark_conflict(
                                    rel_data_path=rel_data,
                                    conflict_detail=remote_conflict,
                                    now_iso=now_iso,
                                    run_id=run_id,
                                )
                                conflicts.append(remote_conflict)
                                continue
                            assert rd_pair is not None and rm_pair is not None
                            remote_d, d_method = rd_pair
                            remote_m, m_method = rm_pair
                            ledger.mark_pruned(
                                rel_data_path=rel_data,
                                now_iso=now_iso,
                                run_id=run_id,
                                event_type="PRUNE_RECOVERY_OK",
                                detail={
                                    "remote_data_id": remote_d.remote_id,
                                    "data_method": d_method,
                                    "remote_manifest_id": remote_m.remote_id,
                                    "manifest_method": m_method,
                                },
                            )
                            recovered_pruned.append(rel_data)
                            pruned.append(rel_data)
                        else:
                            # Unexpected local disappearance without PRUNE_INTENT -> report critical error!
                            remote_intact = False
                            try:
                                remote_conflict, _rd_pair, _rm_pair = _verify_remote_integrity_before_prune(
                                    row, backend
                                )
                                remote_intact = remote_conflict is None
                            except Exception:
                                remote_intact = False
                            msg = (
                                f"{rel_data}: local data file unexpectedly missing while state=VERIFIED "
                                f"without PRUNE_INTENT (remote_copy_intact={remote_intact})"
                            )
                            ledger.mark_error(
                                rel_data_path=rel_data,
                                error_code="LOCAL_DATA_MISSING_UNEXPECTED",
                                error_message=msg,
                                now_iso=now_iso,
                                run_id=run_id,
                                event_type="MISSING_LOCAL_DATA_CRITICAL",
                                detail={"remote_copy_intact": remote_intact},
                            )
                            errors.append(msg)
                    except Exception as exc:
                        ledger.mark_error(
                            rel_data_path=rel_data,
                            error_code="PRUNE_RECOVERY_FAILED",
                            error_message=f"{type(exc).__name__}: {exc}",
                            now_iso=now_iso,
                            run_id=run_id,
                        )
                        errors.append(f"{rel_data}: {type(exc).__name__}: {exc}")
                    continue

                # --- Slow-path 3: New partition OR SEALED (pending/retry/legacy-migrated) ---
                if not dpath.is_file():
                    msg = (
                        f"{rel_data}: critical missing local sealed data file with NO valid VERIFIED/PRUNED proof "
                        f"(ledger_state={row['state'] if row else 'NONE'})"
                    )
                    ledger.mark_error(
                        rel_data_path=rel_data,
                        error_code="MISSING_LOCAL_DATA_NO_PROOF",
                        error_message=msg,
                        now_iso=now_iso,
                        run_id=run_id,
                        event_type="MISSING_LOCAL_DATA_CRITICAL",
                    )
                    errors.append(msg)
                    continue

                try:
                    checked = verify_local_partition(dpath, mpath)
                    local_hashes = checked["hashes"]
                    manifest_hashes = file_hashes(mpath)
                    local_mtime = dpath.stat().st_mtime
                    mdata = checked.get("market_data") or {}
                    if mdata.get("parsed_tsv"):
                        ident["data_lines"] = int(mdata["data_lines"])
                        ident["start_wall_ns"] = mdata["start_wall_ns"]
                        ident["end_wall_ns"] = mdata["end_wall_ns"]

                    row = ledger.upsert_sealed(
                        ident=ident,
                        local_hashes=local_hashes,
                        manifest_hashes=manifest_hashes,
                        local_mtime_epoch_s=local_mtime,
                        now_iso=now_iso,
                        run_id=run_id,
                    )

                    # 1. Pre-upload data conflict check: NEVER overwrite differing remote object!
                    existing_data = backend.stat_remote(rel_data)
                    if existing_data.exists:
                        hm = _hashes_match(local_hashes, existing_data)
                        size_ok = existing_data.size == local_hashes["bytes"]
                        if hm is False or not size_ok:
                            conflict = {
                                "remote_path": rel_data,
                                "local_sha256": local_hashes["sha256"],
                                "local_md5": local_hashes["md5"],
                                "local_bytes": local_hashes["bytes"],
                                "remote_id": existing_data.remote_id,
                                "remote_bytes": existing_data.size,
                                "remote_md5": existing_data.md5,
                                "remote_sha256": existing_data.sha256,
                                "action": "ISOLATED_LOCAL_PRESERVED_NO_OVERWRITE",
                            }
                            ledger.mark_conflict(
                                rel_data_path=rel_data,
                                conflict_detail=conflict,
                                now_iso=now_iso,
                                run_id=run_id,
                            )
                            conflicts.append(conflict)
                            continue
                        if hm is None and verify_remote:
                            rb_sha = backend.readback_sha256(rel_data)
                            if rb_sha.lower() != local_hashes["sha256"].lower():
                                conflict = {
                                    "remote_path": rel_data,
                                    "local_sha256": local_hashes["sha256"],
                                    "remote_readback_sha256": rb_sha,
                                    "action": "ISOLATED_LOCAL_PRESERVED_NO_OVERWRITE",
                                }
                                ledger.mark_conflict(
                                    rel_data_path=rel_data,
                                    conflict_detail=conflict,
                                    now_iso=now_iso,
                                    run_id=run_id,
                                )
                                conflicts.append(conflict)
                                continue
                    else:
                        backend.copy_file(dpath, rel_data, immutable=True)
                        ledger.record_event(
                            ts_utc=now_iso,
                            run_id=run_id,
                            exchange=ident["exchange"],
                            date_utc=ident["date_utc"],
                            hour_utc=ident["hour_utc"],
                            boot_id=ident["boot_id"],
                            partition_id=ident["partition_id"],
                            rel_data_path=rel_data,
                            event_type="UPLOAD_DATA_OK",
                            from_state=STATE_SEALED,
                            to_state=STATE_SEALED,
                            detail={"bytes": local_hashes["bytes"], "sha256": local_hashes["sha256"]},
                        )

                    # 2. Pre-upload manifest conflict check & idempotent upload
                    existing_m = backend.stat_remote(rel_manifest)
                    if existing_m.exists:
                        hm_m = _hashes_match(manifest_hashes, existing_m)
                        if hm_m is False or existing_m.size != manifest_hashes["bytes"]:
                            conflict = {
                                "remote_path": rel_manifest,
                                "data_path": rel_data,
                                "local_sha256": manifest_hashes["sha256"],
                                "remote_bytes": existing_m.size,
                                "remote_sha256": existing_m.sha256,
                                "remote_md5": existing_m.md5,
                                "action": "ISOLATED_MANIFEST_CONFLICT_NO_OVERWRITE",
                            }
                            ledger.mark_conflict(
                                rel_data_path=rel_data,
                                conflict_detail=conflict,
                                now_iso=now_iso,
                                run_id=run_id,
                            )
                            conflicts.append(conflict)
                            continue
                        if hm_m is None and verify_remote:
                            rb_m = backend.readback_sha256(rel_manifest)
                            if rb_m.lower() != manifest_hashes["sha256"].lower():
                                conflict = {
                                    "remote_path": rel_manifest,
                                    "data_path": rel_data,
                                    "local_sha256": manifest_hashes["sha256"],
                                    "remote_readback_sha256": rb_m,
                                    "action": "ISOLATED_MANIFEST_CONFLICT_NO_OVERWRITE",
                                }
                                ledger.mark_conflict(
                                    rel_data_path=rel_data,
                                    conflict_detail=conflict,
                                    now_iso=now_iso,
                                    run_id=run_id,
                                )
                                conflicts.append(conflict)
                                continue
                    else:
                        backend.copy_file(mpath, rel_manifest, immutable=True)
                        ledger.record_event(
                            ts_utc=now_iso,
                            run_id=run_id,
                            exchange=ident["exchange"],
                            date_utc=ident["date_utc"],
                            hour_utc=ident["hour_utc"],
                            boot_id=ident["boot_id"],
                            partition_id=ident["partition_id"],
                            rel_data_path=rel_data,
                            event_type="UPLOAD_MANIFEST_OK",
                            from_state=STATE_SEALED,
                            to_state=STATE_SEALED,
                            detail={"bytes": manifest_hashes["bytes"], "sha256": manifest_hashes["sha256"]},
                        )

                    # 3. Post-upload dual remote verification (BOTH data AND manifest)
                    remote_data_after, data_method = _verify_remote_object_matches(
                        backend=backend,
                        rel_path=rel_data,
                        expected_hashes=local_hashes,
                        verify_remote=verify_remote,
                    )
                    remote_manifest_after, manifest_method = _verify_remote_object_matches(
                        backend=backend,
                        rel_path=rel_manifest,
                        expected_hashes=manifest_hashes,
                        verify_remote=verify_remote,
                    )

                    if "before_verify_commit" in hooks:
                        hooks["before_verify_commit"](rel_data)

                    if verify_remote:
                        row = ledger.mark_verified(
                            rel_data_path=rel_data,
                            remote_data=remote_data_after,
                            data_method=data_method,
                            remote_manifest=remote_manifest_after,
                            manifest_method=manifest_method,
                            now_iso=now_iso,
                            run_id=run_id,
                        )
                        newly_verified_entries.append(_row_to_jsonl_entry(row))

                        if "after_verify_before_prune" in hooks:
                            hooks["after_verify_before_prune"](rel_data)

                        # 4. Safe rolling prune if already past retention threshold
                        can_prune, _reason = _can_safely_prune(
                            row,
                            dpath=dpath,
                            verify_remote=verify_remote,
                            prune_older_than_hours=prune_older_than_hours,
                            now_s=now_s,
                        )
                        if can_prune:
                            integrity_conflict = _verify_local_integrity_before_prune(row, dpath)
                            if integrity_conflict is not None:
                                ledger.mark_conflict(
                                    rel_data_path=rel_data,
                                    conflict_detail=integrity_conflict,
                                    now_iso=now_iso,
                                    run_id=run_id,
                                )
                                conflicts.append(integrity_conflict)
                                continue
                            ledger.record_event(
                                ts_utc=now_iso,
                                run_id=run_id,
                                exchange=row["exchange"],
                                date_utc=row["date_utc"],
                                hour_utc=row["hour_utc"],
                                boot_id=row["boot_id"],
                                partition_id=row["partition_id"],
                                rel_data_path=rel_data,
                                event_type="PRUNE_INTENT",
                                from_state=STATE_VERIFIED,
                                to_state=STATE_PRUNED,
                                detail={"prune_older_than_hours": prune_older_than_hours},
                            )
                            dpath.unlink()
                            if "after_prune_unlink_before_commit" in hooks:
                                hooks["after_prune_unlink_before_commit"](rel_data)
                            ledger.mark_pruned(
                                rel_data_path=rel_data,
                                now_iso=now_iso,
                                run_id=run_id,
                                event_type="PRUNED_OK",
                                detail={"prune_older_than_hours": prune_older_than_hours},
                            )
                            pruned.append(rel_data)
                except Exception as exc:
                    # Never delete local source on any failure!
                    ledger.mark_error(
                        rel_data_path=rel_data,
                        error_code=type(exc).__name__,
                        error_message=str(exc),
                        now_iso=now_iso,
                        run_id=run_id,
                    )
                    errors.append(f"{rel_data}: {type(exc).__name__}: {exc}")

            # Atomically export compatibility projection to verified_archive_ledger.compat.jsonl
            # (never overwriting any pre-existing legacy verified_archive_ledger.jsonl).
            export_verified_jsonl_projection(sealed_root, ledger)

            all_verified_rows = ledger.list_partitions(states=(STATE_VERIFIED, STATE_PRUNED))
            pending_unverified_rows = ledger.list_partitions(states=(STATE_SEALED,))
            if not manifests and not all_verified_rows and not pending_unverified_rows:
                errors.append(f"No sealed *.manifest.json found under {raw_root}")

            expected_hour_reconciliation: dict[str, Any] | None = None
            if expected_hourly_exchanges is not None and expected_win_desc is not None:
                from data_coverage import format_window_id, parse_window_start_s, window_descriptor

                expected_pm = int(expected_win_desc["partition_minutes"])
                step_s = expected_pm * 60
                eff_max_gap_s = (
                    float(max_coverage_gap_seconds)
                    if max_coverage_gap_seconds is not None
                    else min(300.0, float(step_s) / 3.0)
                )
                cutover_start_s: int | None = None
                if cutover_window_utc:
                    cutover_start_s = (parse_window_start_s(cutover_window_utc) // step_s) * step_s

                # Discover earliest window start to track contiguously (from existing expected_windows,
                # matching partitions in ledger, cutover_window_utc, or the current expected window).
                candidate_starts: list[int] = [int(expected_win_desc["planned_start_s"])]
                if cutover_start_s is not None and cutover_start_s <= int(expected_win_desc["planned_start_s"]):
                    candidate_starts.append(cutover_start_s)
                for ew in ledger.list_expected_windows(
                    partition_minutes=expected_pm, collection_run_id=expected_run_id
                ):
                    ew_s = int(ew["planned_start_wall_ns"]) // 1_000_000_000
                    if ew_s <= int(expected_win_desc["planned_start_s"]):
                        candidate_starts.append(ew_s)
                for row in all_verified_rows + pending_unverified_rows:
                    if int(row.get("partition_minutes") or 60) != expected_pm:
                        continue
                    if expected_run_id is not None:
                        r_run = row.get("collection_run_id")
                        b_str = str(row.get("boot_id") or "")
                        if r_run != expected_run_id and not (
                            r_run in (None, "") and (b_str == expected_run_id or b_str.startswith(f"{expected_run_id}_"))
                        ):
                            continue
                    r_start_s = (_row_planned_start_s(row) // step_s) * step_s
                    if r_start_s <= int(expected_win_desc["planned_start_s"]):
                        candidate_starts.append(r_start_s)

                earliest_start_s = min(candidate_starts)
                cur_s = earliest_start_s
                market_scan_cache = {
                    row["data_sha256"]: json.loads(row["stats_json"])
                    for row in ledger.conn.execute("SELECT data_sha256, stats_json FROM archive_market_scan_stats")
                }
                previously_scanned = set(market_scan_cache)
                reconciled_windows: list[tuple[dict[str, Any], dict[str, Any], bool]] = []

                while cur_s <= int(expected_win_desc["planned_start_s"]):
                    w_desc = window_descriptor(cur_s, expected_pm)
                    is_current_target = cur_s == int(expected_win_desc["planned_start_s"])
                    rec = _reconcile_single_window(
                        win_desc=w_desc,
                        expected_exchanges=expected_hourly_exchanges,
                        expected_run_id=expected_run_id,
                        all_verified_rows=all_verified_rows,
                        pending_unverified_rows=pending_unverified_rows,
                        manifests_with_data=manifests_with_data,
                        raw_root=raw_root,
                        sealed_root=sealed_root,
                        max_coverage_gap_seconds=eff_max_gap_s,
                        seal_wait_attempts=seal_wait_attempts if is_current_target else 0,
                        seal_waited_seconds=seal_waited_seconds if is_current_target else 0.0,
                        verify_remote=verify_remote,
                        market_scan_cache=market_scan_cache,
                    )
                    reconciled_windows.append((w_desc, rec, is_current_target))
                    cur_s += step_s

                with ledger.conn:
                    ledger.conn.executemany(
                        "INSERT OR IGNORE INTO archive_market_scan_stats(data_sha256, stats_json) VALUES (?, ?)",
                        (
                            (sha, json.dumps(stats))
                            for sha, stats in market_scan_cache.items()
                            if len(sha) == 64 and sha not in previously_scanned and stats.get("parsed_tsv")
                        ),
                    )

                pending_prior_windows: list[str] = []
                pending_prior_window_details: list[dict[str, Any]] = []
                startup_stub_windows: list[str] = []
                startup_stub_window_details: list[dict[str, Any]] = []
                current_rec: dict[str, Any] | None = None
                still_in_leading_startup = True
                first_full_window_start_s: int | None = cutover_start_s

                for w_desc, rec, is_current_target in reconciled_windows:
                    w_start_s = int(w_desc["planned_start_s"])
                    is_pre_cutover = cutover_start_s is not None and w_start_s < cutover_start_s
                    is_leading_prior_stub = (
                        still_in_leading_startup
                        and not is_current_target
                        and (
                            (is_pre_cutover and not rec["unsealed_partials"] and not rec["sealed_without_manifest"] and not rec["overlaps"] and not rec["mixed_batches"])
                            or (cutover_start_s is None and rec["is_startup_stub_candidate"])
                        )
                    )
                    if is_leading_prior_stub:
                        if rec["file_verified_exchanges"] or rec["startup_stub_segments"]:
                            rec["window_coverage_status"] = "startup_stub"
                            rec["window_status"] = "STARTUP_STUB"
                            rec["errors"] = []
                            startup_stub_windows.append(w_desc["window_id"])
                            startup_stub_window_details.append(
                                {
                                    "window_id": w_desc["window_id"],
                                    "window_span_id": w_desc.get("window_span_id"),
                                    "file_verified_exchanges": rec["file_verified_exchanges"],
                                    "startup_stub_segments": rec["startup_stub_segments"],
                                }
                            )
                        else:
                            # Pre-cutover empty window before cutover_window_utc
                            rec["window_coverage_status"] = "pre_cutover_ignored"
                            rec["window_status"] = "STARTUP_STUB"
                            rec["errors"] = []
                    else:
                        if is_current_target and still_in_leading_startup and rec["is_startup_stub_candidate"]:
                            # Collector started in this current expected window; stub files are verified,
                            # single-list as startup stub, and await the first full window before reporting COMPLETE.
                            rec["window_coverage_status"] = "startup_stub"
                            rec["window_status"] = "STARTUP_STUB"
                            rec["errors"] = [
                                f"expected_window_is_startup_stub_awaiting_first_full_window:{w_desc['window_id']}"
                            ]
                            startup_stub_windows.append(w_desc["window_id"])
                            startup_stub_window_details.append(
                                {
                                    "window_id": w_desc["window_id"],
                                    "window_span_id": w_desc.get("window_span_id"),
                                    "file_verified_exchanges": rec["file_verified_exchanges"],
                                    "startup_stub_segments": rec["startup_stub_segments"],
                                }
                            )
                            if first_full_window_start_s is None:
                                first_full_window_start_s = w_start_s + step_s
                        else:
                            if first_full_window_start_s is None:
                                first_full_window_start_s = w_start_s
                        still_in_leading_startup = False

                    w_key = (
                        f"{expected_run_id or ''}|{w_desc['planned_start_wall_ns']}|"
                        f"{w_desc['planned_end_wall_ns']}|{expected_pm}"
                    )
                    ledger.upsert_expected_window(
                        window_key=w_key,
                        win_desc=w_desc,
                        collection_run_id=expected_run_id,
                        expected_exchanges=rec["expected_exchanges"],
                        file_verified_exchanges=rec["file_verified_exchanges"],
                        covered_exchanges=rec["covered_exchanges"],
                        missing_exchanges=rec["missing_exchanges"],
                        incomplete_coverage_exchanges=rec["incomplete_coverage_exchanges"],
                        file_verification_status=rec["file_verification_status"],
                        window_coverage_status=rec["window_coverage_status"],
                        window_status=rec["window_status"],
                        detail={
                            "coverage_gaps": rec["coverage_gaps"],
                            "mixed_batches": rec["mixed_batches"],
                            "overlaps": rec["overlaps"],
                            "unsealed_partials": rec["unsealed_partials"],
                            "sealed_without_manifest": rec["sealed_without_manifest"],
                            "startup_stub_segments": rec["startup_stub_segments"],
                        },
                        now_iso=now_iso,
                    )
                    if is_current_target:
                        current_rec = rec
                    elif rec["window_status"] not in ("COMPLETE", "STARTUP_STUB"):
                        pending_prior_windows.append(w_desc["window_id"])
                        pending_prior_window_details.append(
                            {
                                "window_id": w_desc["window_id"],
                                "window_span_id": w_desc.get("window_span_id"),
                                "file_verification_status": rec["file_verification_status"],
                                "window_coverage_status": rec["window_coverage_status"],
                                "missing_exchanges": rec["missing_exchanges"],
                                "incomplete_coverage_exchanges": rec["incomplete_coverage_exchanges"],
                                "coverage_gaps": rec["coverage_gaps"],
                            }
                        )

                # Also include any other unverified rows in STATE_SEALED not in the contiguous range
                # (matching the active batch/duration if specified)
                for prow in pending_unverified_rows:
                    if int(prow.get("partition_minutes") or 60) != expected_pm:
                        continue
                    if expected_run_id is not None and prow.get("collection_run_id") not in (None, "", expected_run_id):
                        continue
                    pwid = str(prow.get("window_id") or f"{prow['date_utc']}T{prow['hour_utc']}:00Z")
                    if pwid != expected_win_desc["window_id"] and pwid not in pending_prior_windows:
                        pending_prior_windows.append(pwid)
                pending_prior_windows.sort()

                assert current_rec is not None
                errors.extend(current_rec["errors"])
                # Directly constrain outer archive result by current expected window status!
                if current_rec["window_status"] != "COMPLETE":
                    fallback_err = (
                        f"expected_window_not_complete:{current_rec['expected_window_id']}:"
                        f"status={current_rec['window_status']},file={current_rec['file_verification_status']},"
                        f"coverage={current_rec['window_coverage_status']}"
                    )
                    if not current_rec["errors"] and fallback_err not in errors:
                        errors.append(fallback_err)
                if verify_remote:
                    for pwid in pending_prior_windows:
                        errors.append(f"pending_prior_window_unverified:{pwid}")

                eff_first_full_s = (
                    first_full_window_start_s
                    if first_full_window_start_s is not None
                    else int(expected_win_desc["planned_start_s"])
                )
                expected_hour_reconciliation = dict(current_rec)
                expected_hour_reconciliation["first_full_window_id"] = format_window_id(eff_first_full_s)
                expected_hour_reconciliation["startup_stub_windows"] = startup_stub_windows
                expected_hour_reconciliation["startup_stub_window_details"] = startup_stub_window_details
                expected_hour_reconciliation["pending_prior_windows"] = pending_prior_windows
                expected_hour_reconciliation["pending_prior_window_details"] = pending_prior_window_details

            # Atomically export deterministic cloud-readable verified archive index BEFORE meta/ sync
            # so GitHub Incremental QA discovers only remotely verified partitions and window states.
            archive_index_path = export_archive_manifest_index(
                sealed_root, ledger, effective_run_id=expected_run_id
            )

            # Upload changed meta/ files incrementally (excluding WAL/SHM/lock/partial files)
            meta_uploaded: list[str] = []
            meta_skipped = 0
            meta_root = sealed_root / "meta"
            if meta_root.exists():
                for fpath in sorted(meta_root.rglob("*")):
                    if not fpath.is_file():
                        continue
                    if fpath.name == "stream_activity.json" or any(
                        fpath.name.endswith(suf) for suf in _META_SYNC_EXCLUDED_SUFFIXES
                    ):
                        continue
                    rel = fpath.relative_to(sealed_root).as_posix()
                    try:
                        mh = file_hashes(fpath)
                        prev_meta = ledger.get_meta_sync(rel)
                        if (
                            prev_meta is not None
                            and prev_meta["sha256"] == mh["sha256"]
                            and int(prev_meta["size_bytes"]) == int(mh["bytes"])
                        ):
                            meta_skipped += 1
                            continue
                        backend.copy_file(fpath, rel, immutable=False)
                        ledger.upsert_meta_sync(rel, int(mh["bytes"]), str(mh["sha256"]), now_iso)
                        meta_uploaded.append(rel)
                    except Exception as exc:
                        errors.append(f"{rel}: {type(exc).__name__}: {exc}")

            # Enqueue durable deduplicated GitHub QA dispatch records strictly per COMPLETE window
            # ONLY after `meta/archive_manifest_index.json` is verified synced to Google Drive.
            # Evaluated independently of outer archive errors (such as historical missing windows).
            qa_dispatch_outbox: dict[str, Any] = {
                "index_synced": False,
                "reason": "verify_remote_disabled",
                "enqueued": [],
                "already_queued": [],
                "skipped_not_complete": [],
            }
            if verify_remote and archive_index_path is not None:
                qa_dispatch_outbox = discover_and_enqueue_qa_dispatches(
                    sealed_root,
                    ledger,
                    now_epoch_s=now_s,
                    run_id_filter=expected_run_id,
                    partition_minutes_filter=expected_pm if expected_hourly_exchanges else None,
                    discovery_source="archive_sync",
                )

            # Compare committed recovery state to the last successfully copied snapshot, even
            # after a crash or a retry with zero new uploads. Exclude the snapshot's own metadata.
            snapshot_remote_path: str | None = None
            snapshot_sha256: str | None = None
            recovery_state = sorted(
                (row["rel_data_path"], row["state"], row["verified_at_utc"], row["pruned_at_utc"],
                 row["local_sha256"], row["manifest_sha256"])
                for row in all_verified_rows
            )
            meta_state = [
                tuple(row) for row in ledger.conn.execute(
                    "SELECT rel_path, sha256 FROM meta_sync_state ORDER BY rel_path"
                )
            ]
            generation = hashlib.sha256(
                json.dumps([recovery_state, meta_state], separators=(",", ":")).encode("utf-8")
            ).hexdigest()
            snapshot_needed = bool(recovery_state or meta_state) and (
                generation != ledger.get_meta("last_snapshot_generation")
            )
            if upload_ledger_snapshot and snapshot_needed:
                ts_tag = datetime.fromtimestamp(now_s, tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                snap_name = f"archive_ledger_{ts_tag}_{run_id[-6:]}.sqlite"
                snap_rel = f"meta/archive_ledger/{snap_name}"
                snap_sha_rel = f"{snap_rel}.sha256"
                try:
                    with tempfile.TemporaryDirectory(prefix="tokyo_l2_ledger_snap_") as tmpdir:
                        snap_local = Path(tmpdir) / snap_name
                        snap_info = ledger.create_consistent_snapshot(snap_local)
                        snap_sha_local = Path(tmpdir) / f"{snap_name}.sha256"
                        snap_sha_local.write_text(
                            f"{snap_info['sha256']}  {snap_name}\n",
                            encoding="utf-8",
                        )
                        backend.copy_file(snap_local, snap_rel, immutable=True)
                        backend.copy_file(snap_sha_local, snap_sha_rel, immutable=True)
                        snapshot_remote_path = snap_rel
                        snapshot_sha256 = snap_info["sha256"]
                        ledger.set_meta("last_snapshot_remote_path", snap_rel)
                        ledger.set_meta("last_snapshot_sha256", snap_info["sha256"])
                        ledger.set_meta("last_snapshot_utc", now_iso)
                        ledger.set_meta("last_snapshot_generation", generation)
                        ledger.record_event(
                            ts_utc=now_iso,
                            run_id=run_id,
                            event_type="SNAPSHOT_UPLOADED",
                            detail={
                                "remote_path": snap_rel,
                                "bytes": snap_info["bytes"],
                                "sha256": snap_info["sha256"],
                                "partial_run_had_errors": bool(errors or conflicts),
                            },
                        )
                except Exception as exc:
                    errors.append(f"{snap_rel}: {type(exc).__name__}: {exc}")

            all_entries = [_row_to_jsonl_entry(r) for r in all_verified_rows]
            state_counts = {
                STATE_SEALED: len(pending_unverified_rows),
                STATE_VERIFIED: len(ledger.list_partitions(states=(STATE_VERIFIED,))),
                STATE_PRUNED: len(ledger.list_partitions(states=(STATE_PRUNED,))),
            }
            legacy_out = str(legacy_jsonl_path) if legacy_jsonl_path.is_file() else None

            if conflicts or errors:
                return {
                    "status": "blocked",
                    "phase": "archive",
                    "run_id": run_id,
                    "effective_run_id": expected_run_id,
                    "proxy": proxy_info,
                    "verified_count": len(newly_verified_entries),
                    "total_verified_or_pruned_count": len(all_verified_rows),
                    "skipped_verified_count": skipped_verified,
                    "skipped_pruned_count": skipped_pruned,
                    "recovered_pruned_files": recovered_pruned,
                    "state_counts": state_counts,
                    "migration": migration_summary,
                    "expected_hour_reconciliation": expected_hour_reconciliation,
                    "expected_window_reconciliation": expected_hour_reconciliation,
                    "archive_manifest_index_path": str(archive_index_path) if archive_index_path else None,
                    "qa_dispatch_outbox": qa_dispatch_outbox,
                    "conflicts": conflicts,
                    "failed_items": errors + [f"remote_hash_conflict:{c['remote_path']}" for c in conflicts],
                    "minimal_fixes": [
                        "Inspect isolated remote hash conflicts or errors without overwriting remote objects; local source files were preserved."
                    ],
                    "pruned_local_files": pruned,
                    "ledger_db_path": str(ledger_db_path),
                    "ledger_path": str(ledger_jsonl_path),
                    "projection_ledger_path": str(ledger_jsonl_path),
                    "legacy_ledger_path": legacy_out,
                    "snapshot_remote_path": snapshot_remote_path,
                    "snapshot_sha256": snapshot_sha256,
                }

            return {
                "status": "ok",
                "phase": "archive",
                "run_id": run_id,
                "effective_run_id": expected_run_id,
                "proxy": proxy_info,
                "verified_count": len(all_verified_rows),
                "newly_verified_count": len(newly_verified_entries),
                "skipped_verified_count": skipped_verified,
                "skipped_pruned_count": skipped_pruned,
                "recovered_pruned_files": recovered_pruned,
                "state_counts": state_counts,
                "migration": migration_summary,
                "expected_hour_reconciliation": expected_hour_reconciliation,
                "expected_window_reconciliation": expected_hour_reconciliation,
                "archive_manifest_index_path": str(archive_index_path) if archive_index_path else None,
                "qa_dispatch_outbox": qa_dispatch_outbox,
                "meta_uploaded_count": len(meta_uploaded),
                "meta_skipped_count": meta_skipped,
                "pruned_local_files": pruned,
                "ledger_db_path": str(ledger_db_path),
                "ledger_path": str(ledger_jsonl_path),
                "projection_ledger_path": str(ledger_jsonl_path),
                "legacy_ledger_path": legacy_out,
                "snapshot_remote_path": snapshot_remote_path,
                "snapshot_sha256": snapshot_sha256,
                "entries": all_entries,
            }
    finally:
        lock.release()


def audit_effective_archive_service(
    systemd_cat_output: str,
    *,
    expected_config_path: str = "/etc/tokyo-l2/config.json",
    forbid_auto_prune: bool = True,
) -> dict[str, Any]:
    """Audit `systemctl cat tokyo-l2-archive.service` output for effective ExecStart & drop-in overrides.

    Follows systemd ExecStart override semantics (an empty `ExecStart=` in a drop-in clears
    earlier `ExecStart=` entries from the base unit file).
    """
    import shlex

    current_source = "<base>"
    in_service_section = False
    base_exec_starts: list[str] = []
    effective_exec_starts: list[str] = []
    drop_in_files: list[str] = []
    drop_in_replaced_exec_start = False

    for raw_line in systemd_cat_output.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("# /"):
            current_source = line[2:].strip()
            in_service_section = False
            if ".service.d/" in current_source and current_source not in drop_in_files:
                drop_in_files.append(current_source)
            continue
        if line.startswith("#") or line.startswith(";"):
            continue
        if line.startswith("[") and line.endswith("]"):
            in_service_section = line == "[Service]"
            continue
        if not in_service_section:
            continue
        if line.startswith("ExecStart="):
            val = line.split("=", 1)[1].strip()
            is_drop_in = ".service.d/" in current_source
            if val == "":
                effective_exec_starts.clear()
                if is_drop_in:
                    drop_in_replaced_exec_start = True
            else:
                if not is_drop_in:
                    base_exec_starts.append(val)
                else:
                    drop_in_replaced_exec_start = True
                effective_exec_starts.append(val)

    issues: list[str] = []
    has_expected_config = False
    has_verify_remote = False
    has_prune_hours = False
    has_run_id_flag = False
    parsed_tokens: list[list[str]] = []

    if not effective_exec_starts:
        issues.append("missing_effective_exec_start")
    for cmd in effective_exec_starts:
        try:
            tokens = shlex.split(cmd)
        except ValueError:
            tokens = cmd.split()
        parsed_tokens.append(tokens)
        if "--verify-remote" in tokens:
            has_verify_remote = True
        for idx, tok in enumerate(tokens):
            if tok == "--config" and idx + 1 < len(tokens) and tokens[idx + 1] == expected_config_path:
                has_expected_config = True
            elif tok == f"--config={expected_config_path}":
                has_expected_config = True
            if tok == "--prune-hours" or tok.startswith("--prune-hours="):
                has_prune_hours = True
            if tok == "--run-id" or tok.startswith("--run-id="):
                has_run_id_flag = True

    if effective_exec_starts and not has_expected_config:
        issues.append(f"missing_expected_config:{expected_config_path}")
    if effective_exec_starts and not has_verify_remote:
        issues.append("missing_verify_remote_flag")
    if forbid_auto_prune and has_prune_hours:
        issues.append("auto_prune_must_remain_off")

    return {
        "ok": len(issues) == 0,
        "base_exec_start": base_exec_starts,
        "effective_exec_start": effective_exec_starts,
        "drop_in_files": drop_in_files,
        "drop_in_replaced_exec_start": drop_in_replaced_exec_start,
        "has_expected_config": has_expected_config,
        "has_verify_remote": has_verify_remote,
        "has_run_id_flag": has_run_id_flag,
        "auto_prune_disabled": not has_prune_hours,
        "issues": issues,
    }


def main() -> int:
    from data_coverage import (
        configured_partition_minutes,
        enabled_exchanges,
        resolve_cutover_window_utc,
        resolve_effective_run_id,
    )

    p = argparse.ArgumentParser(description="Incremental SQLite archive ledger and Google Drive verifier")
    p.add_argument("--dir", type=Path, required=True, help="Root directory containing raw/ and meta/")
    p.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Require verified partitions for all configured exchanges in the previous UTC window",
    )
    p.add_argument("--remote", default="gdrive_main:tokyo_l2_data", help="rclone remote root")
    p.add_argument("--verify-remote", action="store_true", help="Verify remote ID, size, and hash after upload")
    p.add_argument("--prune-hours", type=float, default=None, help="Optional safe local retention hours after remote verify")
    p.add_argument("--no-snapshot", action="store_true", help="Skip uploading consistent SQLite ledger snapshot to meta/")
    p.add_argument("--run-id", default=None, help="Optional collection batch run_id filter for window reconciliation")
    p.add_argument("--cutover-window", default=None, help="Optional first full acceptance UTC window (YYYY-MM-DDTHH:MMZ)")
    p.add_argument("--seal-wait-seconds", type=float, default=None, help="Override bounded wait for async partition sealing")
    p.add_argument("--proxy-url", default=None, help="Optional SOCKS5 URL scoped to rclone only")
    p.add_argument(
        "--proxy-mode",
        choices=("off", "required", "fallback_direct"),
        default=None,
        help="Optional archive proxy mode overriding TOKYO_L2_ARCHIVE_PROXY_MODE",
    )
    args = p.parse_args()

    if args.proxy_url is not None or args.proxy_mode is not None:
        backend = RcloneBackend(args.remote, proxy_url=args.proxy_url, proxy_mode=args.proxy_mode)
    else:
        backend = RcloneBackend(args.remote)
    cfg_obj: dict[str, Any] | None = None
    expected = None
    partition_minutes = 60
    seal_wait_seconds = 0.0
    seal_poll_interval = 1.0
    if args.config is not None:
        cfg_obj = json.loads(args.config.read_text(encoding="utf-8"))
        expected = set(enabled_exchanges(cfg_obj))
        partition_minutes = configured_partition_minutes(cfg_obj)
        arch_cfg = cfg_obj.get("archive", {})
        seal_wait_seconds = float(arch_cfg.get("seal_wait_seconds", 0.0))
        seal_poll_interval = float(arch_cfg.get("seal_poll_interval_seconds", 1.0))
    if args.seal_wait_seconds is not None:
        seal_wait_seconds = float(args.seal_wait_seconds)
    effective_run_id = resolve_effective_run_id(cfg_obj, args.run_id)
    cutover_window_utc = resolve_cutover_window_utc(cfg_obj, args.cutover_window)
    result = sync_sealed_directory(
        args.dir,
        backend=backend,
        verify_remote=args.verify_remote,
        prune_older_than_hours=args.prune_hours,
        upload_ledger_snapshot=not args.no_snapshot,
        expected_hourly_exchanges=expected,
        partition_minutes=partition_minutes,
        expected_run_id=effective_run_id,
        cutover_window_utc=cutover_window_utc,
        seal_wait_seconds=seal_wait_seconds,
        seal_poll_interval_seconds=seal_poll_interval,
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0 if result["status"] == "ok" else 2


if __name__ == "__main__":
    raise SystemExit(main())
