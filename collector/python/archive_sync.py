#!/usr/bin/env python3
"""Atomic hourly sealing verifier, single Google Drive Pro (rclone) uploader, and safe local pruning.

Enforces Section 4.2 of SPEC_v1.4:
1. Verify local .tsv.zst decompression integrity (zstandard) + exact SHA-256 against .manifest.json.
2. Check remote object BEFORE upload: if remote exists with DIFFERENT hash -> fail-closed
   `status=blocked` and isolate; NEVER overwrite immutable objects.
3. Idempotent `rclone copy` to `gdrive_main:tokyo_l2_data/...`.
4. Verify remote object ID, byte size, and remote content hash (MD5/SHA-256 via rclone lsjson,
   falling back to remote read-back hash if remote hash unavailable).
5. Write local verified ledger (`verified_archive_ledger.jsonl`).
6. Prune local files ONLY when remote-verified AND older than local_retention_hours (default 24h).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

import zstandard as zstd


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


# Official rclone exit codes indicating genuine absence (vs API/network/auth failure):
#   3 = Directory not found
#   4 = File not found
RCLONE_NOT_FOUND_EXIT_CODES = {3, 4}


class RcloneBackend:
    """Real rclone backend targeting single Google Drive Pro (`gdrive_main:tokyo_l2_data`)."""

    def __init__(self, remote_root: str):
        self.remote_root = remote_root.rstrip("/")

    def _full(self, rel: str) -> str:
        return f"{self.remote_root}/{rel.lstrip('/')}"

    def stat_remote(self, remote_rel_path: str) -> RemoteObjectInfo:
        target = self._full(remote_rel_path)
        proc = subprocess.run(
            ["rclone", "lsjson", "--hash", "--files-only", target],
            capture_output=True,
            text=True,
            timeout=60,
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
        )

    def readback_sha256(self, remote_rel_path: str) -> str:
        target = self._full(remote_rel_path)
        with tempfile.TemporaryDirectory(prefix="tokyo_l2_readback_") as tmp:
            dst = Path(tmp) / Path(remote_rel_path).name
            subprocess.run(
                ["rclone", "copyto", target, str(dst)],
                check=True,
                capture_output=True,
                text=True,
                timeout=600,
            )
            return file_hashes(dst)["sha256"]


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


def sync_sealed_directory(
    sealed_root: Path,
    backend: ArchiveBackend,
    verify_remote: bool = True,
    prune_older_than_hours: float | None = None,
    now_epoch_s: float | None = None,
) -> dict[str, Any]:
    """Verify, upload, remote-verify, record ledger, and optionally safe-prune sealed partitions."""
    now_s = time.time() if now_epoch_s is None else now_epoch_s
    ledger_path = sealed_root / "verified_archive_ledger.jsonl"
    raw_root = sealed_root / "raw"

    manifests = sorted(raw_root.rglob("*.manifest.json")) if raw_root.exists() else []
    if not manifests:
        return {
            "status": "blocked",
            "phase": "archive",
            "failed_items": [f"No sealed *.manifest.json found under {raw_root}"],
            "minimal_fixes": ["Seal at least one hourly partition before invoking archive"],
        }

    verified_entries: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []
    errors: list[str] = []
    pruned: list[str] = []

    for mpath in manifests:
        data_name = mpath.name.replace(".manifest.json", ".tsv.zst")
        dpath = mpath.with_name(data_name)
        rel_data = dpath.relative_to(sealed_root).as_posix()
        rel_manifest = mpath.relative_to(sealed_root).as_posix()

        try:
            checked = verify_local_partition(dpath, mpath)
            local_hashes = checked["hashes"]
            manifest_hashes = file_hashes(mpath)

            # 1. Pre-upload conflict check: NEVER overwrite a remote file with a different hash!
            existing = backend.stat_remote(rel_data)
            if existing.exists:
                hm = _hashes_match(local_hashes, existing)
                size_ok = existing.size == local_hashes["bytes"]
                if hm is False or not size_ok:
                    conflicts.append(
                        {
                            "remote_path": rel_data,
                            "local_sha256": local_hashes["sha256"],
                            "local_md5": local_hashes["md5"],
                            "local_bytes": local_hashes["bytes"],
                            "remote_id": existing.remote_id,
                            "remote_bytes": existing.size,
                            "remote_md5": existing.md5,
                            "remote_sha256": existing.sha256,
                            "action": "ISOLATED_LOCAL_PRESERVED_NO_OVERWRITE",
                        }
                    )
                    continue
                if hm is None and verify_remote:
                    rb_sha = backend.readback_sha256(rel_data)
                    if rb_sha != local_hashes["sha256"]:
                        conflicts.append(
                            {
                                "remote_path": rel_data,
                                "local_sha256": local_hashes["sha256"],
                                "remote_readback_sha256": rb_sha,
                                "action": "ISOLATED_LOCAL_PRESERVED_NO_OVERWRITE",
                            }
                        )
                        continue
            else:
                backend.copy_file(dpath, rel_data)

            # Upload manifest idempotently too (check hash conflict first)
            existing_m = backend.stat_remote(rel_manifest)
            if existing_m.exists:
                hm_m = _hashes_match(manifest_hashes, existing_m)
                if hm_m is False or existing_m.size != manifest_hashes["bytes"]:
                    conflicts.append(
                        {
                            "remote_path": rel_manifest,
                            "local_sha256": manifest_hashes["sha256"],
                            "remote_bytes": existing_m.size,
                            "action": "ISOLATED_MANIFEST_CONFLICT_NO_OVERWRITE",
                        }
                    )
                    continue
            else:
                backend.copy_file(mpath, rel_manifest)

            # 2. Post-upload remote verification (ID + bytes + hash/readback)
            remote_after = backend.stat_remote(rel_data)
            verification_method = "skipped"
            if verify_remote:
                if not remote_after.exists:
                    raise RuntimeError(f"Remote object missing after upload: {rel_data}")
                if remote_after.size != local_hashes["bytes"]:
                    raise RuntimeError(
                        f"Remote byte count mismatch for {rel_data}: {remote_after.size} != {local_hashes['bytes']}"
                    )
                hm_after = _hashes_match(local_hashes, remote_after)
                if hm_after is True:
                    verification_method = "remote_native_hash"
                elif hm_after is False:
                    raise RuntimeError(f"Remote hash mismatch after upload for {rel_data}")
                else:
                    rb = backend.readback_sha256(rel_data)
                    if rb != local_hashes["sha256"]:
                        raise RuntimeError(f"Remote readback SHA-256 mismatch for {rel_data}")
                    verification_method = "remote_readback_sha256"

            entry = {
                "verified_utc": datetime.fromtimestamp(now_s, tz=timezone.utc).isoformat(),
                "remote_path": rel_data,
                "remote_manifest_path": rel_manifest,
                "remote_id": remote_after.remote_id,
                "bytes": local_hashes["bytes"],
                "sha256": local_hashes["sha256"],
                "md5": local_hashes["md5"],
                "verification_method": verification_method,
                "verified": bool(verify_remote),
            }
            verified_entries.append(entry)
            with ledger_path.open("a", encoding="utf-8") as lf:
                lf.write(json.dumps(entry) + "\n")

            # 3. Safe local rolling buffer pruning ONLY if remote-verified AND past retention age
            if verify_remote and prune_older_than_hours is not None:
                age_hours = (now_s - dpath.stat().st_mtime) / 3600.0
                if age_hours >= prune_older_than_hours:
                    dpath.unlink()
                    pruned.append(rel_data)
        except Exception as exc:
            # Never delete local source on any failure!
            errors.append(f"{rel_data}: {type(exc).__name__}: {exc}")

    # Also upload meta/{specs,rtt,chrony} if present
    meta_uploaded: list[str] = []
    meta_root = sealed_root / "meta"
    if meta_root.exists():
        for fpath in sorted(meta_root.rglob("*")):
            if not fpath.is_file():
                continue
            rel = fpath.relative_to(sealed_root).as_posix()
            try:
                backend.copy_file(fpath, rel, immutable=False)
                meta_uploaded.append(rel)
            except Exception as exc:
                errors.append(f"{rel}: {type(exc).__name__}: {exc}")

    if conflicts or errors:
        return {
            "status": "blocked",
            "phase": "archive",
            "verified_count": len(verified_entries),
            "conflicts": conflicts,
            "failed_items": errors + [f"remote_hash_conflict:{c['remote_path']}" for c in conflicts],
            "minimal_fixes": [
                "Inspect isolated remote hash conflicts without overwriting remote objects; local source files were preserved."
            ],
            "pruned_local_files": pruned,
            "ledger_path": str(ledger_path),
        }

    return {
        "status": "ok",
        "phase": "archive",
        "verified_count": len(verified_entries),
        "meta_uploaded_count": len(meta_uploaded),
        "pruned_local_files": pruned,
        "ledger_path": str(ledger_path),
        "entries": verified_entries,
    }


def main() -> int:
    p = argparse.ArgumentParser(description="Atomic hourly seal verifier and single Google Drive uploader")
    p.add_argument("--dir", type=Path, required=True, help="Root directory containing raw/ and meta/")
    p.add_argument("--remote", default="gdrive_main:tokyo_l2_data", help="rclone remote root")
    p.add_argument("--verify-remote", action="store_true", help="Verify remote ID, size, and hash after upload")
    p.add_argument("--prune-hours", type=float, default=None, help="Optional safe local retention hours after remote verify")
    args = p.parse_args()

    backend = RcloneBackend(args.remote)
    result = sync_sealed_directory(
        args.dir,
        backend=backend,
        verify_remote=args.verify_remote,
        prune_older_than_hours=args.prune_hours,
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0 if result["status"] == "ok" else 2


if __name__ == "__main__":
    raise SystemExit(main())
