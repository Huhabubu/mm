"""Verified conversion cache helpers for Tokyo L2 backtesting.

Allows converted `.npz` artifacts (signal BBO, maker L2 HBT, maker L1 BBO, edges,
stream report) to be generated once per `(date, symbol, exchange)` and reused across
all strategy models, research stages, and optional private cache directories.

Verification is strictly fail-closed: cache reuse requires exact match of:
  - signal_exchange, making_exchange, base_symbol, date_utc
  - converter version & converter script SHA-256 hashes
  - contract_spec_sha256
  - ordered list of source partition manifest/data SHA-256 hashes
  - SHA-256 hashes of every converted `.npz` artifact in the cache directory
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

CACHE_MANIFEST_FILE = "cache_manifest.json"
REQUIRED_ARTIFACT_KINDS = ("binance_bbo", "follower_l2_hbt", "follower_l1_bbo")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with Path(path).open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def converter_code_sha256(script_paths: list[Path]) -> str:
    """Deterministic digest over the active converter & adapter source files."""
    h = hashlib.sha256()
    for p in sorted((Path(x) for x in script_paths), key=lambda x: x.name):
        h.update(p.name.encode("utf-8"))
        h.update(b"\0")
        h.update(sha256_file(p).encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def source_partitions_digest(partitions: list[dict[str, Any]]) -> str:
    """Deterministic SHA-256 over the verified source partition hashes."""
    canonical = sorted(
        (
            str(p["exchange"]),
            str(p["window_id"]),
            str(p["manifest_sha256"]),
            str(p["data_sha256"]),
        )
        for p in partitions
    )
    return hashlib.sha256(json.dumps(canonical, separators=(",", ":")).encode("utf-8")).hexdigest()


def compute_cache_key(
    *,
    signal_exchange: str,
    making_exchange: str,
    base_symbol: str,
    date_utc: str,
    source_sha256: str,
    converter_sha256: str,
    converter_version: str,
    contract_spec_sha256: str,
) -> str:
    payload = {
        "signal_exchange": signal_exchange,
        "making_exchange": making_exchange,
        "base_symbol": base_symbol,
        "date_utc": date_utc,
        "source_sha256": source_sha256,
        "converter_sha256": converter_sha256,
        "converter_version": converter_version,
        "contract_spec_sha256": contract_spec_sha256,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()[:24]


def write_cache_manifest(
    converted_dir: Path,
    *,
    signal_exchange: str,
    making_exchange: str,
    base_symbol: str,
    date_utc: str,
    source_sha256: str,
    converter_sha256: str,
    converter_version: str,
    contract_spec_sha256: str,
    stream_report: dict[str, Any],
) -> dict[str, Any]:
    """Write a self-verifying cache_manifest.json alongside converted `.npz` artifacts."""
    converted_dir = Path(converted_dir)
    artifacts = {}
    for item in stream_report.get("artifacts", []):
        kind = str(item["kind"])
        fname = str(item["path"])
        fpath = converted_dir / fname
        if not fpath.is_file():
            raise FileNotFoundError(f"missing converted artifact for cache manifest: {fpath}")
        actual = sha256_file(fpath)
        if actual != item.get("sha256"):
            raise ValueError(f"artifact SHA-256 mismatch while writing cache manifest: {fname}")
        artifacts[kind] = {"file": fname, "rows": int(item["rows"]), "sha256": actual}
    for req in REQUIRED_ARTIFACT_KINDS:
        if req not in artifacts:
            raise ValueError(f"stream_report missing required artifact kind {req!r}")
    cache_key = compute_cache_key(
        signal_exchange=signal_exchange,
        making_exchange=making_exchange,
        base_symbol=base_symbol,
        date_utc=date_utc,
        source_sha256=source_sha256,
        converter_sha256=converter_sha256,
        converter_version=converter_version,
        contract_spec_sha256=contract_spec_sha256,
    )
    manifest = {
        "schema": "tokyo_l2_converted_cache_v1",
        "cache_key": cache_key,
        "signal_exchange": signal_exchange,
        "making_exchange": making_exchange,
        "base_symbol": base_symbol,
        "date_utc": date_utc,
        "source_sha256": source_sha256,
        "converter_sha256": converter_sha256,
        "converter_version": converter_version,
        "contract_spec_sha256": contract_spec_sha256,
        "stream_status": stream_report.get("status"),
        "artifacts": artifacts,
    }
    (converted_dir / CACHE_MANIFEST_FILE).write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return manifest


def verify_cache_dir(
    cache_dir: Path,
    *,
    signal_exchange: str,
    making_exchange: str,
    base_symbol: str,
    date_utc: str,
    source_sha256: str,
    converter_sha256: str,
    converter_version: str,
    contract_spec_sha256: str,
) -> tuple[bool, str, dict[str, Any] | None]:
    """Fail-closed verification of a converted artifact cache directory."""
    cache_dir = Path(cache_dir)
    mpath = cache_dir / CACHE_MANIFEST_FILE
    if not mpath.is_file():
        return False, "missing_cache_manifest", None
    try:
        manifest = json.loads(mpath.read_text(encoding="utf-8"))
    except Exception as exc:
        return False, f"unreadable_cache_manifest:{exc}", None

    expected_fields = {
        "schema": "tokyo_l2_converted_cache_v1",
        "signal_exchange": signal_exchange,
        "making_exchange": making_exchange,
        "base_symbol": base_symbol,
        "date_utc": date_utc,
        "source_sha256": source_sha256,
        "converter_sha256": converter_sha256,
        "converter_version": converter_version,
        "contract_spec_sha256": contract_spec_sha256,
    }
    for k, exp in expected_fields.items():
        if manifest.get(k) != exp:
            return False, f"cache_mismatch:{k}", None

    expected_key = compute_cache_key(
        signal_exchange=signal_exchange,
        making_exchange=making_exchange,
        base_symbol=base_symbol,
        date_utc=date_utc,
        source_sha256=source_sha256,
        converter_sha256=converter_sha256,
        converter_version=converter_version,
        contract_spec_sha256=contract_spec_sha256,
    )
    if manifest.get("cache_key") != expected_key:
        return False, "cache_mismatch:cache_key", None

    artifacts = manifest.get("artifacts") or {}
    for req in REQUIRED_ARTIFACT_KINDS:
        if req not in artifacts:
            return False, f"missing_artifact_entry:{req}", None
    for kind, info in artifacts.items():
        fname = str(info.get("file") or "")
        if Path(fname).name != fname:
            return False, f"unsafe_artifact_name:{kind}", None
        fpath = cache_dir / fname
        if not fpath.is_file():
            return False, f"missing_artifact_file:{fname}", None
        if sha256_file(fpath) != info.get("sha256"):
            return False, f"corrupted_artifact_sha256:{fname}", None

    sr_path = cache_dir / "stream_report.json"
    if not sr_path.is_file():
        return False, "missing_stream_report", None
    return True, "cache_hit_verified", manifest
