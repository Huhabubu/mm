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
ARCHIVE_LOCK_FILENAME = ".archive_sync.lock"
LEDGER_SCHEMA_VERSION = "1"

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


def verify_local_partition(data_path: Path, manifest_path: Path) -> dict[str, Any]:
    if not data_path.is_file():
        raise FileNotFoundError(f"Missing sealed data file: {data_path}")
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Missing manifest file: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    verify_zstd_integrity(data_path)
    hashes = file_hashes(data_path)
    if hashes["bytes"] != int(manifest["bytes"]):
        raise ValueError(f"Byte mismatch for {data_path.name}: local={hashes['bytes']} manifest={manifest['bytes']}")
    if hashes["sha256"] != manifest["sha256"]:
        raise ValueError(f"SHA-256 mismatch for {data_path.name}: local={hashes['sha256']} manifest={manifest['sha256']}")
    return {"manifest": manifest, "hashes": hashes}


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
    """Extract canonical partition primary key and local/relative paths from a manifest."""
    file_name = manifest.get("file_name")
    if isinstance(file_name, str) and file_name and Path(file_name).name == file_name and file_name.endswith(".tsv.zst"):
        data_path = manifest_path.with_name(file_name)
    else:
        data_name = manifest_path.name.replace(".manifest.json", ".tsv.zst")
        data_path = manifest_path.with_name(data_name)

    stem = manifest_path.name
    if stem.endswith(".manifest.json"):
        stem = stem[: -len(".manifest.json")]
    partition_id = str(manifest.get("partition_id") or stem)
    exchange = str(manifest["exchange"])
    date_utc = str(manifest["date_utc"])
    hour_utc = str(manifest["hour_utc"]).zfill(2)
    boot_id = str(manifest.get("boot_id") or "unknown_boot")

    return {
        "exchange": exchange,
        "date_utc": date_utc,
        "hour_utc": hour_utc,
        "boot_id": boot_id,
        "partition_id": partition_id,
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
        # Extract hour from ..._YYYYMMDD_HH or ..._YYYYMMDD_HH_<suffix>
        hm = re.search(r"_(\d{8})_(\d{2})(?:$|_)", stem)
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
                """
            )
            self.conn.execute(
                "INSERT OR IGNORE INTO schema_meta(key, value) VALUES('schema_version', ?)",
                (LEDGER_SCHEMA_VERSION,),
            )

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
        with self.conn:
            if existing is None:
                self.conn.execute(
                    """
                    INSERT INTO partitions (
                        exchange, date_utc, hour_utc, boot_id, partition_id,
                        rel_data_path, rel_manifest_path, state,
                        local_bytes, local_sha256, local_md5,
                        manifest_bytes, manifest_sha256, manifest_md5,
                        local_mtime_epoch_s, sealed_at_utc, source, updated_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'SEALED', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        json.dumps({"bytes": local_hashes["bytes"], "sha256": local_hashes["sha256"]}),
                    ),
                )
            elif existing["state"] == STATE_SEALED:
                self.conn.execute(
                    """
                    UPDATE partitions
                    SET local_bytes = ?, local_sha256 = ?, local_md5 = ?,
                        manifest_bytes = ?, manifest_sha256 = ?, manifest_md5 = ?,
                        local_mtime_epoch_s = COALESCE(?, local_mtime_epoch_s),
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


def _row_to_jsonl_entry(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "verified_utc": row["verified_at_utc"] or row["updated_at_utc"],
        "remote_path": row["rel_data_path"],
        "remote_manifest_path": row["rel_manifest_path"],
        "remote_id": row["remote_data_id"],
        "remote_manifest_id": row["remote_manifest_id"],
        "bytes": int(row["local_bytes"]),
        "sha256": row["local_sha256"],
        "md5": row["local_md5"],
        "verification_method": row["remote_data_verification_method"] or "remote_native_hash",
        "manifest_verification_method": row["remote_manifest_verification_method"],
        "remote_manifest_verified": bool(row["remote_manifest_verified"]),
        "verified": bool(row["remote_data_verified"] and row["remote_manifest_verified"]),
        "state": row["state"],
        "pruned_at_utc": row["pruned_at_utc"],
        "exchange": row["exchange"],
        "date_utc": row["date_utc"],
        "hour_utc": row["hour_utc"],
        "boot_id": row["boot_id"],
        "partition_id": row["partition_id"],
    }


def export_verified_jsonl_projection(sealed_root: Path, ledger: ArchiveLedger) -> Path:
    """Atomically write SQLite compatibility projection to `verified_archive_ledger.compat.jsonl`.

    Never overwrites or modifies any pre-existing legacy `verified_archive_ledger.jsonl`.
    """
    out_path = sealed_root / PROJECTION_LEDGER_JSONL_FILENAME
    rows = ledger.list_partitions(states=(STATE_VERIFIED, STATE_PRUNED))
    lines = [json.dumps(_row_to_jsonl_entry(r), sort_keys=False) for r in rows]
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


def sync_sealed_directory(
    sealed_root: Path,
    backend: ArchiveBackend,
    verify_remote: bool = True,
    prune_older_than_hours: float | None = None,
    now_epoch_s: float | None = None,
    *,
    upload_ledger_snapshot: bool = True,
    expected_hourly_exchanges: set[str] | None = None,
    fault_hooks: dict[str, Callable[[str], None]] | None = None,
) -> dict[str, Any]:
    """Incrementally verify, upload, remote-verify, snapshot-backup, and safely prune partitions.

    `fault_hooks` (strictly for deterministic fault-injection testing) supports optional callbacks:
      - `before_verify_commit(rel_data_path)`
      - `after_verify_before_prune(rel_data_path)`
      - `after_prune_unlink_before_commit(rel_data_path)`
    """
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

        manifests = sorted(raw_root.rglob("*.manifest.json")) if raw_root.exists() else []
        if not manifests:
            return {
                "status": "blocked",
                "phase": "archive",
                "proxy": proxy_info,
                "failed_items": [f"No sealed *.manifest.json found under {raw_root}"],
                "minimal_fixes": ["Seal at least one hourly partition before invoking archive"],
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

            for mpath in manifests:
                try:
                    manifest = json.loads(mpath.read_text(encoding="utf-8"))
                    ident = _derive_partition_identity(sealed_root, mpath, manifest)
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

            # Upload changed meta/ files incrementally (excluding WAL/SHM/lock/partial files)
            meta_uploaded: list[str] = []
            meta_skipped = 0
            meta_root = sealed_root / "meta"
            if meta_root.exists():
                for fpath in sorted(meta_root.rglob("*")):
                    if not fpath.is_file():
                        continue
                    if any(fpath.name.endswith(suf) for suf in _META_SYNC_EXCLUDED_SUFFIXES):
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

            # Atomically export compatibility projection to verified_archive_ledger.compat.jsonl
            # (never overwriting any pre-existing legacy verified_archive_ledger.jsonl).
            export_verified_jsonl_projection(sealed_root, ledger)

            all_verified_rows = ledger.list_partitions(states=(STATE_VERIFIED, STATE_PRUNED))
            if expected_hourly_exchanges is not None:
                previous_hour = datetime.fromtimestamp(now_s, timezone.utc) - timedelta(hours=1)
                expected_date = previous_hour.strftime("%Y-%m-%d")
                expected_hour = previous_hour.strftime("%H")
                verified = {
                    row["exchange"]
                    for row in all_verified_rows
                    if row["date_utc"] == expected_date and row["hour_utc"] == expected_hour
                    and row["remote_data_verified"] and row["remote_manifest_verified"]
                    and not row["has_conflict"] and not row["last_error_code"]
                }
                errors.extend(
                    f"expected_hour_not_verified:{exchange}:{expected_date}T{expected_hour}Z"
                    for exchange in sorted(expected_hourly_exchanges - verified)
                )
                for exchange in sorted(expected_hourly_exchanges):
                    for path in (raw_root / exchange / expected_date).glob(
                        f"{exchange}_{previous_hour:%Y%m%d}_{expected_hour}*.tsv.zst"
                    ):
                        if not path.with_name(path.name.removesuffix(".tsv.zst") + ".manifest.json").is_file():
                            errors.append(f"sealed_file_without_manifest:{path.relative_to(sealed_root).as_posix()}")

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
                STATE_SEALED: len(ledger.list_partitions(states=(STATE_SEALED,))),
                STATE_VERIFIED: len(ledger.list_partitions(states=(STATE_VERIFIED,))),
                STATE_PRUNED: len(ledger.list_partitions(states=(STATE_PRUNED,))),
            }
            legacy_out = str(legacy_jsonl_path) if legacy_jsonl_path.is_file() else None

            if conflicts or errors:
                return {
                    "status": "blocked",
                    "phase": "archive",
                    "run_id": run_id,
                    "proxy": proxy_info,
                    "verified_count": len(newly_verified_entries),
                    "total_verified_or_pruned_count": len(all_verified_rows),
                    "skipped_verified_count": skipped_verified,
                    "skipped_pruned_count": skipped_pruned,
                    "recovered_pruned_files": recovered_pruned,
                    "state_counts": state_counts,
                    "migration": migration_summary,
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
                "proxy": proxy_info,
                "verified_count": len(all_verified_rows),
                "newly_verified_count": len(newly_verified_entries),
                "skipped_verified_count": skipped_verified,
                "skipped_pruned_count": skipped_pruned,
                "recovered_pruned_files": recovered_pruned,
                "state_counts": state_counts,
                "migration": migration_summary,
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


def main() -> int:
    p = argparse.ArgumentParser(description="Incremental SQLite archive ledger and Google Drive verifier")
    p.add_argument("--dir", type=Path, required=True, help="Root directory containing raw/ and meta/")
    p.add_argument("--remote", default="gdrive_main:tokyo_l2_data", help="rclone remote root")
    p.add_argument("--verify-remote", action="store_true", help="Verify remote ID, size, and hash after upload")
    p.add_argument("--prune-hours", type=float, default=None, help="Optional safe local retention hours after remote verify")
    p.add_argument("--no-snapshot", action="store_true", help="Skip uploading consistent SQLite ledger snapshot to meta/")
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
    result = sync_sealed_directory(
        args.dir,
        backend=backend,
        verify_remote=args.verify_remote,
        prune_older_than_hours=args.prune_hours,
        upload_ledger_snapshot=not args.no_snapshot,
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0 if result["status"] == "ok" else 2


if __name__ == "__main__":
    raise SystemExit(main())
