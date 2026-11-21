"""Incremental partitioned QA for Tokyo L2 archived partitions (5/15/30/60m + legacy hourly).

Reads ONLY from read-only Google Drive (`scope=drive.readonly`), inspects one compressed
partition at a time in bounded scratch space, and persists immutable summary reports
(single-file, whole-window, and complete UTC day) to the separate `tokyo-l2-qa-state` branch.
Never touches the collector, never writes to Drive, and never stores raw data or credentials.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import csv
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
from typing import Any, Callable

import zstandard as zstd

from data_coverage import (
    CHANNEL_CATEGORY_EVENT_TRADE,
    _data_streams,
    channel_silence_thresholds,
    classify_channel,
    configured_partition_minutes,
    effective_config_hash,
    enabled_exchanges,
    expected_streams,
    iter_expected_windows,
    normalize_manifest_window,
    parse_partition_window_from_rel_path,
    parse_window_start_s,
    record_monotonic_producer_key,
    window_descriptor,
)

QA_VERSION = 2
MAX_PARTITION_BYTES = 8 * 1024**3

QA_STATUS_PASSED = "passed"
QA_STATUS_FAILED = "failed"
QA_STATUS_PENDING_ARCHIVE = "pending_archive"
QA_STATUS_NEEDS_REVIEW = "needs_review"
QA_STATUS_OVERDUE = "overdue"

ARCHIVE_INDEX_RELPATH = "meta/archive_manifest_index.json"


def _run(*args: str) -> str:
    proc = subprocess.run(args, capture_output=True, text=True, check=False)
    if proc.returncode:
        raise RuntimeError(f"{args[0]} failed (exit {proc.returncode}): {proc.stderr[-600:]}")
    return proc.stdout


def _download(remote: str, rel_path: str, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["rclone", "copyto", f"{remote.rstrip('/')}/{rel_path}", str(target)],
        check=True,
        capture_output=True,
        text=True,
        timeout=1800,
    )


def _download_with_retry(
    remote: str,
    rel_path: str,
    target: Path,
    *,
    retries: int = 3,
    backoff_seconds: float = 1.0,
    sleeper: Callable[[float], None] = time.sleep,
) -> None:
    attempts = max(1, int(retries))
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            _download(remote, rel_path, target)
            return
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as exc:
            last_exc = exc
            # rclone exit codes 3/4 indicate genuine not-found; do not burn retries on missing optional files
            if isinstance(exc, subprocess.CalledProcessError) and exc.returncode in (3, 4):
                raise
            if attempt < attempts and backoff_seconds > 0:
                sleeper(backoff_seconds * attempt)
    assert last_exc is not None
    raise last_exc


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _atomic_write_json(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(obj, ensure_ascii=False, indent=2) + "\n"
    tmp = path.with_name(f"{path.name}.tmp.{os.getpid()}")
    try:
        with tmp.open("w", encoding="utf-8") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def _resolve_checker_commit(explicit_sha: str | None = None) -> str:
    if explicit_sha and explicit_sha.strip():
        return explicit_sha.strip()
    env_sha = os.environ.get("TOKYO_L2_QA_CHECKER_COMMIT") or os.environ.get("GITHUB_SHA")
    if env_sha and env_sha.strip():
        return env_sha.strip()
    try:
        out = subprocess.run(
            ["git", "-C", str(Path(__file__).resolve().parent), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
            check=True,
        )
        return out.stdout.strip()
    except Exception:
        return "unknown"


def _safe_window_filename(window_id: str) -> str:
    return window_id.replace(":", "").replace("/", "_").replace(" ", "_")


def check_partition(data_path: Path, manifest: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    """Inspect one sealed partition (legacy hourly or 5/15/30/60m segment) in streaming O(1) memory."""
    norm = normalize_manifest_window(manifest, fallback_file_name=data_path.name)
    exchange = norm["exchange"]
    date_utc = norm["date_utc"]
    hour_utc = norm["hour_utc"]
    minute_utc = norm["minute_utc"]
    partition_minutes = int(norm["partition_minutes"])
    window_id = norm["window_id"]
    planned_end_id = norm["planned_end_id"]
    window_span_id = norm["window_span_id"]
    start_ns = int(norm["planned_start_wall_ns"])
    end_ns = int(norm["planned_end_wall_ns"])
    config_pm = configured_partition_minutes(config)
    expected_span_ns = config_pm * 60 * 1_000_000_000

    hour_int = int(hour_utc)
    if not (0 <= hour_int < 24):
        raise ValueError("invalid UTC hour")
    if data_path.name != manifest["file_name"]:
        raise ValueError("data filename does not match manifest")
    if data_path.stat().st_size != int(manifest["bytes"]):
        raise ValueError("compressed file size differs from manifest")
    actual_sha = _sha256(data_path)
    if actual_sha != manifest["sha256"]:
        raise ValueError("compressed SHA-256 differs from manifest")

    expected = {key for key in expected_streams(config) if key[0] == exchange}
    if not expected:
        raise ValueError(f"unknown/disabled exchange: {exchange}")
    thresholds = channel_silence_thresholds(config)
    stale_ns = int(thresholds["other"] * 1_000_000_000)

    counts: dict[str, int] = defaultdict(int)
    streams: dict[tuple[str, str, str], dict[str, Any]] = {}
    problems: list[str] = []
    review_items: list[str] = []
    if partition_minutes != config_pm or (end_ns - start_ns) != expected_span_ns:
        problems.append(
            f"partition duration mismatch: manifest={partition_minutes}m ({window_span_id}) != config={config_pm}m"
        )

    first_seq = last_seq = prev_seq = None
    producer_prev_mono: dict[tuple[str, str, int], int] = {}
    first_wall = last_wall = None
    loss_events = gap_events = depth_unavailable_events = late_rejected_events = 0
    dropped_frames = late_rejected_dropped = 0
    late_loss_by_target_window: dict[str, int] = defaultdict(int)

    with data_path.open("rb") as fh:
        with zstd.ZstdDecompressor().stream_reader(fh) as reader:
            with io.TextIOWrapper(reader, encoding="utf-8", errors="strict") as lines:
                for n, line in enumerate(lines, 1):
                    parts = line.rstrip("\n").split("\t", 6)
                    if len(parts) != 7:
                        raise ValueError(f"line {n}: expected 7 TSV fields")
                    wall_ns_s, mono_ns_s, conn, epoch_s, rx_seq_s, kind, raw = parts
                    wall_ns, mono_ns, epoch_i, rx_seq = (
                        int(wall_ns_s),
                        int(mono_ns_s),
                        int(epoch_s),
                        int(rx_seq_s),
                    )
                    if not (start_ns <= wall_ns < end_ns):
                        problems.append(f"line {n}: timestamp outside UTC partition [{window_id}]")
                        if len(problems) >= 20:
                            break
                    if prev_seq is not None and rx_seq <= prev_seq:
                        problems.append(f"line {n}: rx_seq non-monotonic")
                    if kind not in {"DATA", "DATA_ESCAPED", "META"}:
                        raise ValueError(f"line {n}: unknown record kind {kind}")
                    payload = json.loads(raw)
                    if kind == "DATA_ESCAPED":
                        payload = json.loads(payload) if isinstance(payload, str) else payload

                    prod_key = record_monotonic_producer_key(exchange, kind, conn, epoch_i, payload)
                    prev_mono = producer_prev_mono.get(prod_key)
                    if prev_mono is not None and mono_ns < prev_mono:
                        problems.append(
                            f"line {n}: receive monotonic clock regressed ({prod_key[0]}:{conn}:{epoch_i})"
                        )
                    producer_prev_mono[prod_key] = mono_ns
                    if len(problems) >= 20:
                        break
                    prev_seq = rx_seq

                    if first_seq is None:
                        first_seq, first_wall = rx_seq, wall_ns
                    else:
                        first_wall = min(first_wall, wall_ns) if first_wall is not None else wall_ns
                    last_seq = rx_seq
                    last_wall = max(last_wall, wall_ns) if last_wall is not None else wall_ns
                    counts[kind] += 1

                    if kind == "META":
                        event = payload.get("event", "") if isinstance(payload, dict) else ""
                        if event == "LOCAL_LOSS":
                            loss_events += 1
                            dropped_frames += int(payload.get("dropped_frames", payload.get("dropped", 1)))
                        elif event == "GAP_START":
                            gap_events += 1
                        elif event in ("DEPTH_UNAVAILABLE", "SNAPSHOT_BRIDGE_RETRY"):
                            depth_unavailable_events += 1
                        elif event == "LATE_FRAME_REJECTED":
                            late_rejected_events += 1
                            dropped = int(payload.get("dropped_frames", 1)) if isinstance(payload, dict) else 1
                            late_rejected_dropped += dropped
                            t_wid = ""
                            if isinstance(payload, dict):
                                t_wid = str(
                                    payload.get("target_window_id")
                                    or payload.get("loss_attributed_window_id")
                                    or ""
                                ).strip()
                                if not t_wid and payload.get("orig_recv_wall_ns") is not None:
                                    orig_s = int(payload["orig_recv_wall_ns"]) // 1_000_000_000
                                    t_wid = window_descriptor(orig_s, partition_minutes)["window_id"]
                            if not t_wid:
                                prev_s = (start_ns // 1_000_000_000) - partition_minutes * 60
                                t_wid = window_descriptor(prev_s, partition_minutes)["window_id"]
                            late_loss_by_target_window[t_wid] += dropped
                    else:
                        for key in _data_streams(exchange, payload) & expected:
                            item = streams.setdefault(
                                key,
                                {"count": 0, "first_ns": wall_ns, "last_ns": wall_ns, "max_internal_gap_ns": 0},
                            )
                            item["first_ns"] = min(item["first_ns"], wall_ns)
                            item["max_internal_gap_ns"] = max(
                                item["max_internal_gap_ns"], max(0, wall_ns - item["last_ns"])
                            )
                            item["last_ns"] = max(item["last_ns"], wall_ns)
                            item["count"] += 1

    if first_seq is not None and manifest.get("first_rx_seq") is not None and first_seq != int(manifest["first_rx_seq"]):
        problems.append("first_rx_seq differs from manifest")
    if last_seq is not None and manifest.get("last_rx_seq") is not None and last_seq != int(manifest["last_rx_seq"]):
        problems.append("last_rx_seq differs from manifest")
    for kind, field in (("DATA", "data_lines"), ("DATA_ESCAPED", "data_escaped_lines"), ("META", "meta_lines")):
        if counts[kind] != int(manifest.get(field) or 0):
            problems.append(f"{field} count differs from manifest")
    if loss_events != int(manifest.get("local_loss_lines") or 0):
        problems.append("LOCAL_LOSS count differs from manifest")
    if gap_events != int(manifest.get("gap_start_lines") or 0):
        problems.append("GAP_START count differs from manifest")

    manifest_late_rej = int(manifest.get("late_frames_rejected") or 0)
    if "late_frames_rejected" in manifest and late_rejected_events != manifest_late_rej:
        problems.append("LATE_FRAME_REJECTED count differs from manifest")
    eff_late_rejected = max(late_rejected_events, manifest_late_rej)
    eff_late_dropped = max(late_rejected_dropped, eff_late_rejected)
    if eff_late_rejected > 0 and not late_loss_by_target_window:
        prev_s = (start_ns // 1_000_000_000) - partition_minutes * 60
        late_loss_by_target_window[window_descriptor(prev_s, partition_minutes)["window_id"]] = eff_late_dropped

    total_loss_events = loss_events + eff_late_rejected
    total_dropped_frames = dropped_frames + eff_late_dropped

    if loss_events or gap_events:
        problems.append(f"recorded loss/gap events: loss={loss_events}, gap_start={gap_events}")
    if eff_late_rejected > 0:
        targets_desc = ", ".join(f"{k}:{v}" for k, v in sorted(late_loss_by_target_window.items()))
        problems.append(
            f"rejected late market data frames (data loss attributed to target window): "
            f"rejected={eff_late_rejected}, dropped={eff_late_dropped}, target_windows=[{targets_desc}]"
        )
    if depth_unavailable_events:
        problems.append(f"depth unavailable events recorded: {depth_unavailable_events}")

    absent_keys = sorted(expected - set(streams))
    absent = ["/".join(s) for s in absent_keys]
    absent_hard = [s for s in absent_keys if classify_channel(s[0], s[2]) != CHANNEL_CATEGORY_EVENT_TRADE]
    absent_trade = [s for s in absent_keys if classify_channel(s[0], s[2]) == CHANNEL_CATEGORY_EVENT_TRADE]
    if absent_hard:
        problems.append(f"{len(absent_hard)} expected quote/depth channels have zero DATA")
    if absent_trade:
        # Event-driven trade channel with zero fills cannot be assumed healthy solely from sibling channels.
        review_items.append(
            f"{len(absent_trade)} event_trade channels have zero DATA (needs review): "
            + ", ".join("/".join(s) for s in absent_trade[:6])
        )
        if absent_hard or len(expected) <= 3:
            # Keep backward-compatible problem reporting when a trade channel is absent alongside other failures
            # or in compact single-symbol test fixtures.
            problems.append(f"{len(absent_trade)} expected event_trade channels have zero DATA")

    stream_rows: list[dict[str, Any]] = []
    max_gap_by_category: dict[str, float] = {}
    for key in sorted(streams):
        item = streams[key]
        category = classify_channel(key[0], key[2])
        cat_limit_s = (
            thresholds["event_trade_quiet_max"]
            if category == CHANNEL_CATEGORY_EVENT_TRADE
            else thresholds.get(category, stale_ns / 1e9)
        )
        cat_limit_ns = int(cat_limit_s * 1_000_000_000)
        head_ns = max(0, item["first_ns"] - start_ns)
        tail_ns = max(0, end_ns - item["last_ns"])
        gap_ns = max(head_ns, tail_ns, item["max_internal_gap_ns"])
        gap_s = round(gap_ns / 1e9, 6)
        max_gap_by_category[category] = max(max_gap_by_category.get(category, 0.0), gap_s)
        stream_rows.append(
            {
                "exchange": key[0],
                "symbol": key[1],
                "channel": key[2],
                "category": category,
                "count": item["count"],
                "first_wall_ns": item["first_ns"],
                "last_wall_ns": item["last_ns"],
                "head_gap_seconds": round(head_ns / 1e9, 6),
                "tail_gap_seconds": round(tail_ns / 1e9, 6),
                "max_internal_gap_seconds": round(item["max_internal_gap_ns"] / 1e9, 6),
                "max_gap_seconds": gap_s,
            }
        )
        if gap_ns > cat_limit_ns:
            msg = f"stream gap > {cat_limit_s:g}s ({category}): {'/'.join(key)} ({gap_ns / 1e9:.1f}s)"
            if category == CHANNEL_CATEGORY_EVENT_TRADE:
                review_items.append(msg)
            else:
                problems.append(msg)

    head_gap_s = round(max(0, first_wall - start_ns) / 1e9, 6) if first_wall is not None else None
    tail_gap_s = round(max(0, end_ns - last_wall) / 1e9, 6) if last_wall is not None else None

    if problems:
        qa_status = QA_STATUS_FAILED
    elif review_items:
        qa_status = QA_STATUS_NEEDS_REVIEW
    else:
        qa_status = QA_STATUS_PASSED

    return {
        "status": "ok" if qa_status == QA_STATUS_PASSED else "blocked",
        "qa_status": qa_status,
        "report_level": "file",
        "exchange": exchange,
        "date_utc": date_utc,
        "hour_utc": hour_utc,
        "minute_utc": minute_utc,
        "partition_minutes": partition_minutes,
        "window_id": window_id,
        "planned_end_id": planned_end_id,
        "window_span_id": window_span_id,
        "planned_start_wall_ns": start_ns,
        "planned_end_wall_ns": end_ns,
        "run_id": norm["run_id"],
        "segment_id": norm["segment_id"],
        "boot_id": norm["boot_id"],
        "is_startup_stub": norm["is_startup_stub"],
        "settled_by_timer": norm["settled_by_timer"],
        "coverage_incomplete": norm["coverage_incomplete"],
        "late_frames_accepted": norm["late_frames_accepted"],
        "late_frames_rejected": eff_late_rejected,
        "late_loss_by_target_window": dict(sorted(late_loss_by_target_window.items())),
        "file_name": data_path.name,
        "sha256": actual_sha,
        "bytes": data_path.stat().st_size,
        "data_records": counts["DATA"] + counts["DATA_ESCAPED"],
        "meta_records": counts["META"],
        "first_rx_seq": first_seq,
        "last_rx_seq": last_seq,
        "first_wall_ns": first_wall,
        "last_wall_ns": last_wall,
        "head_gap_seconds": head_gap_s,
        "tail_gap_seconds": tail_gap_s,
        "local_loss_events": total_loss_events,
        "intrinsic_local_loss_events": total_loss_events,
        "in_window_local_loss_events": loss_events,
        "dropped_frames": total_dropped_frames,
        "intrinsic_dropped_frames": total_dropped_frames,
        "gap_start_events": gap_events,
        "depth_unavailable_events": depth_unavailable_events,
        "max_gap_by_category": max_gap_by_category,
        "streams": stream_rows,
        "missing_streams": absent,
        "review_items": review_items,
        "problems": problems,
    }


def _list_meta_files(remote: str) -> set[str]:
    """List files under `<remote>/meta` once per run to avoid blind speculative downloads."""
    try:
        out = _run(
            "rclone",
            "lsf",
            f"{remote.rstrip('/')}/meta",
            "-R",
            "--files-only",
        )
    except Exception:
        return set()
    return {line.strip().lstrip("/") for line in out.splitlines() if line.strip()}


def _discover_verified_candidates(
    remote: str,
    config: dict[str, Any],
    *,
    since_date: str,
    since_start_s: int | None,
    run_id_filter: str | None,
    download_retries: int,
    sleeper: Callable[[float], None],
    meta_files: set[str] | None = None,
    state_dir: Path | None = None,
) -> tuple[list[dict[str, Any]], str, list[str], list[dict[str, Any]], str | None]:
    """Discover candidate manifests strictly from a trusted remotely-verified archive index.

    Formal QA NEVER bypasses the verified index by scanning unverified `raw/` directories.
    If the primary `meta/archive_manifest_index.json` (and fallback verified JSONL ledger under `meta/`)
    cannot be downloaded or parsed, returns `discovery_mode="verified_index_unavailable"`.

    Also collects all historical loss-marked manifests across ALL batches, dates, and partition
    durations (including the 4 historical loss-marked manifests from pre-cutover 60m collection)
    so batch/config cutovers never drop historical loss evidence.
    """
    enabled = set(enabled_exchanges(config))
    config_pm = configured_partition_minutes(config)
    expected_span_s = config_pm * 60
    candidates: list[dict[str, Any]] = []
    warnings: list[str] = []

    raw_entries: list[dict[str, Any]] | None = None
    index_historical_losses: list[dict[str, Any]] = []
    index_effective_run_id: str | None = None
    discovery_mode = "verified_index_unavailable"

    hist_cache_path = (state_dir / "qa_reports" / "historical_loss_manifests.json") if state_dir is not None else None
    cached_checked_shas: set[str] = set()
    historical_loss_by_path: dict[str, dict[str, Any]] = {}
    if hist_cache_path is not None and hist_cache_path.is_file():
        try:
            cached_obj = json.loads(hist_cache_path.read_text(encoding="utf-8"))
            if isinstance(cached_obj, dict):
                for s in cached_obj.get("checked_manifest_sha256s") or ():
                    if isinstance(s, str) and s:
                        cached_checked_shas.add(s)
                for item in cached_obj.get("manifests") or ():
                    if isinstance(item, dict) and item.get("source_manifest"):
                        historical_loss_by_path[str(item["source_manifest"])] = item
        except Exception:
            pass

    # Also seed historical_loss_by_path from any existing per-file .qa.json reports under state_dir
    if state_dir is not None and (state_dir / "qa_reports" / "raw").is_dir():
        for qpath in sorted((state_dir / "qa_reports" / "raw").rglob("*.qa.json")):
            try:
                qobj = json.loads(qpath.read_text(encoding="utf-8"))
                rel_m = str(qobj.get("source_manifest") or "")
                if not rel_m:
                    rel_m = qpath.relative_to(state_dir / "qa_reports").as_posix().removesuffix(".qa.json")
                loss_ev = int(qobj.get("local_loss_events") or 0)
                late_ev = int(qobj.get("late_frames_rejected") or 0)
                if loss_ev > 0 or late_ev > 0:
                    historical_loss_by_path.setdefault(
                        rel_m,
                        {
                            "source_manifest": rel_m,
                            "file_name": qobj.get("file_name"),
                            "exchange": qobj.get("exchange"),
                            "date_utc": qobj.get("date_utc"),
                            "window_id": qobj.get("window_id"),
                            "partition_minutes": int(qobj.get("partition_minutes") or 60),
                            "run_id": qobj.get("run_id"),
                            "boot_id": qobj.get("boot_id"),
                            "sha256": qobj.get("sha256"),
                            "manifest_sha256": qobj.get("manifest_sha256"),
                            "local_loss_events": loss_ev,
                            "dropped_frames": int(qobj.get("dropped_frames") or 0),
                            "late_frames_rejected": late_ev,
                        },
                    )
                if qobj.get("manifest_sha256"):
                    cached_checked_shas.add(str(qobj["manifest_sha256"]))
            except Exception:
                pass

    with tempfile.TemporaryDirectory(prefix="tokyo_qa_idx_") as tmpdir:
        idx_path = Path(tmpdir) / "archive_manifest_index.json"
        try:
            _download_with_retry(
                remote,
                ARCHIVE_INDEX_RELPATH,
                idx_path,
                retries=download_retries,
                sleeper=sleeper,
            )
            parsed = json.loads(idx_path.read_text(encoding="utf-8"))
            if isinstance(parsed, dict) and isinstance(parsed.get("entries"), list):
                raw_entries = parsed["entries"]
                if isinstance(parsed.get("historical_loss_manifests"), list):
                    index_historical_losses = parsed["historical_loss_manifests"]
                if parsed.get("effective_run_id") not in (None, ""):
                    index_effective_run_id = str(parsed["effective_run_id"])
                discovery_mode = "archive_manifest_index"
        except Exception as exc:
            warnings.append(f"archive_manifest_index_download_or_parse_failed:{type(exc).__name__}")

        # Secondary trusted verified index: verified_archive_ledger.compat.jsonl if present under meta/
        if raw_entries is None and meta_files:
            for alt_rel in ("meta/verified_archive_ledger.compat.jsonl", "verified_archive_ledger.compat.jsonl"):
                alt_base = alt_rel.removeprefix("meta/")
                if alt_base in meta_files or alt_rel in meta_files:
                    alt_path = Path(tmpdir) / "verified_archive_ledger.compat.jsonl"
                    try:
                        _download_with_retry(
                            remote,
                            f"meta/{alt_base}",
                            alt_path,
                            retries=download_retries,
                            sleeper=sleeper,
                        )
                        lines = [
                            json.loads(line)
                            for line in alt_path.read_text(encoding="utf-8").splitlines()
                            if line.strip()
                        ]
                        raw_entries = lines
                        discovery_mode = "verified_archive_ledger_compat"
                        break
                    except Exception as exc:
                        warnings.append(f"trusted_ledger_fallback_failed:{type(exc).__name__}")

        if raw_entries is None:
            warnings.append("verified_archive_index_unavailable")
            return [], "verified_index_unavailable", warnings, sorted(historical_loss_by_path.values(), key=lambda x: str(x.get("source_manifest") or "")), None

        for h_item in index_historical_losses:
            if not isinstance(h_item, dict):
                continue
            rel_m = str(h_item.get("remote_manifest_path") or h_item.get("source_manifest") or "")
            if not rel_m:
                continue
            loss_l = int(h_item.get("local_loss_lines") or h_item.get("local_loss_events") or 0)
            late_l = int(h_item.get("late_frames_rejected") or 0)
            if loss_l > 0 or late_l > 0:
                historical_loss_by_path[rel_m] = {
                    "source_manifest": rel_m,
                    "file_name": Path(str(h_item.get("remote_path") or rel_m.removesuffix(".manifest.json") + ".tsv.zst")).name,
                    "exchange": h_item.get("exchange"),
                    "date_utc": h_item.get("date_utc"),
                    "window_id": h_item.get("window_id"),
                    "partition_minutes": int(h_item.get("partition_minutes") or 60),
                    "run_id": h_item.get("collection_run_id") or h_item.get("run_id"),
                    "boot_id": h_item.get("boot_id"),
                    "sha256": h_item.get("sha256"),
                    "manifest_sha256": h_item.get("manifest_sha256"),
                    "local_loss_events": loss_l + late_l,
                    "dropped_frames": int(h_item.get("dropped_frames") or (loss_l + late_l)),
                    "late_frames_rejected": late_l,
                }

        for entry in raw_entries:
            if not isinstance(entry, dict):
                continue
            if not (entry.get("verified") is True and entry.get("remote_manifest_verified", True) is True):
                continue
            ex = str(entry.get("exchange") or "")
            day = str(entry.get("date_utc") or "")
            rel_mf = str(entry.get("remote_manifest_path") or entry.get("rel_manifest_path") or "")
            if not rel_mf:
                continue

            entry_run_id = entry.get("collection_run_id")
            if entry_run_id in (None, ""):
                entry_run_id = entry.get("run_id")
            eff_run_id = str(entry_run_id) if entry_run_id not in (None, "") else None
            pm = int(entry.get("partition_minutes") or 60)
            mf_sha = str(entry.get("manifest_sha256") or "").strip()
            data_sha = str(entry.get("sha256") or entry.get("data_sha256") or "").strip()

            # Check if this index entry (from ANY batch, date, or partition duration) is a loss-marked manifest
            if "local_loss_lines" in entry or "late_frames_rejected" in entry:
                loss_l = int(entry.get("local_loss_lines") or 0)
                late_l = int(entry.get("late_frames_rejected") or 0)
                if loss_l > 0 or late_l > 0:
                    historical_loss_by_path[rel_mf] = {
                        "source_manifest": rel_mf,
                        "file_name": Path(str(entry.get("remote_path") or entry.get("rel_data_path") or rel_mf.removesuffix(".manifest.json") + ".tsv.zst")).name,
                        "exchange": ex,
                        "date_utc": day,
                        "window_id": entry.get("window_id") or f"{day}T{str(entry.get('hour_utc') or '00').zfill(2)}:{str(entry.get('minute_utc') or '00').zfill(2)}Z",
                        "partition_minutes": pm,
                        "run_id": eff_run_id,
                        "boot_id": entry.get("boot_id"),
                        "sha256": data_sha or None,
                        "manifest_sha256": mf_sha or None,
                        "local_loss_events": loss_l + late_l,
                        "dropped_frames": int(entry.get("dropped_frames") or (loss_l + late_l)),
                        "late_frames_rejected": late_l,
                    }
                if mf_sha:
                    cached_checked_shas.add(mf_sha)
            elif (
                (run_id_filter is not None and eff_run_id != run_id_filter)
                or pm != config_pm
                or day < since_date
            ) and mf_sha and mf_sha not in cached_checked_shas and rel_mf not in historical_loss_by_path:
                # Legacy index entry without local_loss_lines field from a prior batch/duration:
                # inspect its lightweight sidecar manifest once so historical loss manifests are never missed.
                probe_mf_path = Path(tmpdir) / f"hist_{hashlib.sha1(rel_mf.encode()).hexdigest()[:12]}.manifest.json"
                try:
                    _download_with_retry(remote, rel_mf, probe_mf_path, retries=1, sleeper=sleeper)
                    mf_obj = json.loads(probe_mf_path.read_text(encoding="utf-8"))
                    loss_l = int(mf_obj.get("local_loss_lines") or 0)
                    late_l = int(mf_obj.get("late_frames_rejected") or 0)
                    cached_checked_shas.add(mf_sha)
                    if loss_l > 0 or late_l > 0:
                        historical_loss_by_path[rel_mf] = {
                            "source_manifest": rel_mf,
                            "file_name": mf_obj.get("file_name") or Path(rel_mf.removesuffix(".manifest.json") + ".tsv.zst").name,
                            "exchange": ex,
                            "date_utc": day,
                            "window_id": mf_obj.get("window_id") or entry.get("window_id") or f"{day}T{str(entry.get('hour_utc') or '00').zfill(2)}:00Z",
                            "partition_minutes": pm,
                            "run_id": eff_run_id,
                            "boot_id": mf_obj.get("boot_id") or entry.get("boot_id"),
                            "sha256": data_sha or mf_obj.get("sha256"),
                            "manifest_sha256": mf_sha,
                            "local_loss_events": loss_l + late_l,
                            "dropped_frames": loss_l + late_l,
                            "late_frames_rejected": late_l,
                        }
                except Exception:
                    pass

            if ex not in enabled or day < since_date:
                continue
            if run_id_filter is not None and eff_run_id != run_id_filter:
                continue
            if pm != config_pm:
                warnings.append(f"skipped_mismatched_partition_minutes:{rel_mf}:{pm}m!=config_{config_pm}m")
                continue

            planned_start_ns = entry.get("planned_start_wall_ns")
            planned_end_ns = entry.get("planned_end_wall_ns")
            if planned_start_ns is not None and planned_end_ns is not None:
                span_s = (int(planned_end_ns) - int(planned_start_ns)) // 1_000_000_000
                if span_s != expected_span_s:
                    warnings.append(f"skipped_mismatched_window_span:{rel_mf}:{span_s}s!=expected_{expected_span_s}s")
                    continue
                planned_start_s = int(planned_start_ns) // 1_000_000_000
                wd = window_descriptor(planned_start_s, pm)
            elif planned_start_ns is not None:
                planned_start_s = int(planned_start_ns) // 1_000_000_000
                wd = window_descriptor(planned_start_s, pm)
            else:
                hh = str(entry.get("hour_utc") or "00").zfill(2)
                mm = str(entry.get("minute_utc") or "00").zfill(2)
                planned_start_s = parse_window_start_s(f"{day}T{hh}:{mm}Z")
                wd = window_descriptor(planned_start_s, pm)

            if since_start_s is not None and wd["planned_start_s"] < since_start_s:
                continue

            data_bytes = entry.get("bytes") if entry.get("bytes") is not None else entry.get("data_bytes")
            mf_bytes = entry.get("manifest_bytes")

            candidates.append(
                {
                    "source_manifest": rel_mf,
                    "source_data": str(
                        entry.get("remote_path")
                        or entry.get("rel_data_path")
                        or rel_mf.removesuffix(".manifest.json") + ".tsv.zst"
                    ),
                    "exchange": ex,
                    "date_utc": wd["date_utc"],
                    "window_id": wd["window_id"],
                    "planned_end_id": wd["planned_end_id"],
                    "window_span_id": wd["window_span_id"],
                    "partition_minutes": pm,
                    "planned_start_s": wd["planned_start_s"],
                    "planned_end_s": wd["planned_end_s"],
                    "run_id": eff_run_id,
                    "boot_id": entry.get("boot_id"),
                    "segment_id": entry.get("segment_id") or entry.get("partition_id"),
                    "data_sha256": data_sha or None,
                    "manifest_sha256": mf_sha or None,
                    "data_bytes": int(data_bytes) if data_bytes is not None else None,
                    "manifest_bytes": int(mf_bytes) if mf_bytes is not None else None,
                    "is_startup_stub": bool(entry.get("is_startup_stub", False)),
                    "discovery_source": discovery_mode,
                }
            )

    historical_loss_list = sorted(
        historical_loss_by_path.values(),
        key=lambda x: (str(x.get("date_utc") or ""), str(x.get("window_id") or ""), str(x.get("source_manifest") or "")),
    )
    if hist_cache_path is not None:
        try:
            _atomic_write_json(
                hist_cache_path,
                {
                    "schema_version": 1,
                    "count": len(historical_loss_list),
                    "manifests": historical_loss_list,
                    "checked_manifest_sha256s": sorted(cached_checked_shas),
                },
            )
        except OSError:
            pass

    candidates.sort(key=lambda c: (c["planned_start_s"], c["date_utc"], c["exchange"], c["source_manifest"]))
    return candidates, discovery_mode, warnings, historical_loss_list, index_effective_run_id


def _verify_three_way_index_manifest_data(
    *,
    cand: dict[str, Any],
    manifest_path: Path,
    manifest: dict[str, Any],
    data_path: Path,
    run_id_filter: str | None = None,
) -> None:
    """Strictly verify 3-way cryptographic and metadata consistency across:
    1) Trusted verified archive index entry (`cand`)
    2) Downloaded `.manifest.json` (`manifest_path` & parsed `manifest`)
    3) Downloaded `.tsv.zst` (`data_path`)
    """
    idx_mf_sha = cand.get("manifest_sha256")
    idx_data_sha = cand.get("data_sha256")
    if not idx_mf_sha or not idx_data_sha:
        raise ValueError(
            "verified archive index entry is missing required data_sha256 or manifest_sha256 checksums"
        )

    actual_mf_sha = _sha256(manifest_path)
    if actual_mf_sha.lower() != str(idx_mf_sha).lower():
        raise ValueError(
            f"three-way verification failed: manifest SHA-256 ({actual_mf_sha}) differs from verified archive index ({idx_mf_sha})"
        )
    if cand.get("manifest_bytes") is not None and manifest_path.stat().st_size != int(cand["manifest_bytes"]):
        raise ValueError(
            f"three-way verification failed: manifest byte size ({manifest_path.stat().st_size}) differs from verified archive index ({cand['manifest_bytes']})"
        )

    mf_data_sha = str(manifest.get("sha256") or "")
    if mf_data_sha.lower() != str(idx_data_sha).lower():
        raise ValueError(
            f"three-way verification failed: manifest data sha256 ({mf_data_sha}) differs from verified archive index ({idx_data_sha})"
        )
    if cand.get("data_bytes") is not None and int(manifest.get("bytes") or 0) != int(cand["data_bytes"]):
        raise ValueError(
            f"three-way verification failed: manifest data bytes ({manifest.get('bytes')}) differs from verified archive index ({cand['data_bytes']})"
        )

    actual_data_sha = _sha256(data_path)
    if actual_data_sha.lower() != str(idx_data_sha).lower():
        raise ValueError(
            f"three-way verification failed: downloaded data SHA-256 ({actual_data_sha}) differs from verified archive index ({idx_data_sha})"
        )
    if cand.get("data_bytes") is not None and data_path.stat().st_size != int(cand["data_bytes"]):
        raise ValueError(
            f"three-way verification failed: downloaded data byte size ({data_path.stat().st_size}) differs from verified archive index ({cand['data_bytes']})"
        )

    norm = normalize_manifest_window(manifest, fallback_file_name=data_path.name)
    if norm["exchange"] != cand["exchange"]:
        raise ValueError(
            f"three-way verification failed: manifest exchange ({norm['exchange']}) != index ({cand['exchange']})"
        )
    if int(norm["partition_minutes"]) != int(cand["partition_minutes"]):
        raise ValueError(
            f"three-way verification failed: manifest partition_minutes ({norm['partition_minutes']}) != index ({cand['partition_minutes']})"
        )
    if int(norm["planned_start_s"]) != int(cand["planned_start_s"]) or int(norm["planned_end_s"]) != int(cand["planned_end_s"]):
        raise ValueError(
            f"three-way verification failed: manifest window bounds ({norm['window_span_id']}) != index ({cand.get('window_span_id')})"
        )
    if cand.get("run_id") is not None and norm["run_id"] not in (None, cand["run_id"]):
        raise ValueError(
            f"three-way verification failed: manifest run_id ({norm['run_id']}) != index ({cand['run_id']})"
        )
    if run_id_filter is not None and (
        cand.get("run_id") != run_id_filter or norm["run_id"] != run_id_filter
    ):
        raise ValueError(
            f"three-way verification failed: effective run_id ({run_id_filter}) != index ({cand.get('run_id')}) / manifest ({norm['run_id']})"
        )


def _is_memoized_up_to_date(
    previous: dict[str, Any],
    cand: dict[str, Any],
    *,
    cfg_hash: str,
    config_pm: int,
) -> bool:
    if previous.get("qa_version") != QA_VERSION:
        return False
    if previous.get("source_manifest") != cand["source_manifest"]:
        return False
    if previous.get("effective_config_sha256") != cfg_hash:
        return False
    if int(previous.get("partition_minutes") or 0) != int(config_pm):
        return False
    if previous.get("qa_status") not in {QA_STATUS_PASSED, QA_STATUS_FAILED, QA_STATUS_NEEDS_REVIEW}:
        return False
    if not cand.get("data_sha256") or not cand.get("manifest_sha256"):
        return False
    if previous.get("sha256") != cand["data_sha256"]:
        return False
    if previous.get("manifest_sha256") != cand["manifest_sha256"]:
        return False
    return True


def _build_window_reports(
    *,
    state_dir: Path,
    config: dict[str, Any],
    cfg_hash: str,
    checker_commit: str,
    file_reports: dict[str, dict[str, Any]],
    expected_windows: list[dict[str, Any]],
    now_s: float,
    sla_grace_seconds: float,
    run_id_filter: str | None = None,
    unchecked_windows: set[str] | None = None,
) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    """Aggregate single-file QA reports into whole-window reports isolated by batch (run_id) and [start, end) duration."""
    expected_ex = sorted(enabled_exchanges(config).keys())
    all_expected_streams = expected_streams(config)
    thresholds = channel_silence_thresholds(config)
    config_pm = configured_partition_minutes(config)
    expected_span_ns = config_pm * 60 * 1_000_000_000
    unchecked = unchecked_windows or set()

    for rep in file_reports.values():
        if "intrinsic_local_loss_events" not in rep:
            rep["intrinsic_local_loss_events"] = int(rep.get("local_loss_events") or 0)
        if "intrinsic_dropped_frames" not in rep:
            rep["intrinsic_dropped_frames"] = int(rep.get("dropped_frames") or 0)

    # 1. Attribute late-frame rejections from subsequent files back to their target windows
    #    Keyed by (batch_key, target_window_id) -> list of attribution dicts
    attributed_late_losses: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for rel, rep in list(file_reports.items()):
        rep_pm = int(rep.get("partition_minutes") or 0)
        rep_start_ns = int(rep.get("planned_start_wall_ns") or 0)
        rep_end_ns = int(rep.get("planned_end_wall_ns") or 0)
        if rep_pm != config_pm or (rep_end_ns - rep_start_ns) != expected_span_ns:
            continue
        rep_run_id = rep.get("run_id")
        if run_id_filter is not None and rep_run_id != run_id_filter:
            continue
        b_key = str(rep_run_id or (run_id_filter or "__default__"))
        late_map = rep.get("late_loss_by_target_window") or {}
        if isinstance(late_map, dict):
            for target_wid, dropped_cnt in late_map.items():
                cnt = int(dropped_cnt or 0)
                if cnt <= 0:
                    continue
                attributed_late_losses[(b_key, str(target_wid))].append(
                    {
                        "exchange": rep.get("exchange"),
                        "source_file": rep.get("file_name"),
                        "source_manifest": rel,
                        "source_window_id": rep.get("window_id"),
                        "run_id": rep_run_id,
                        "dropped_frames": cnt,
                    }
                )
                # Update at most ONE target segment on disk (the latest segment of that exchange in the target window)
                # so a multi-segment target window never double-counts a single rejected late frame!
                matching_target_segs = [
                    (t_rel, t_rep)
                    for t_rel, t_rep in file_reports.items()
                    if str(t_rep.get("run_id") or (run_id_filter or "__default__")) == b_key
                    and t_rep.get("window_id") == str(target_wid)
                    and t_rep.get("exchange") == rep.get("exchange")
                ]
                if matching_target_segs:
                    t_rel, t_rep = max(
                        matching_target_segs,
                        key=lambda item: (
                            int(item[1].get("last_wall_ns") or 0),
                            int(item[1].get("first_wall_ns") or 0),
                            str(item[1].get("file_name") or ""),
                        ),
                    )
                    msg = (
                        f"attributed late-frame data loss recorded in subsequent partition "
                        f"{rep.get('file_name')} ({rep.get('window_id')}): dropped={cnt}"
                    )
                    probs = list(t_rep.get("problems") or [])
                    if msg not in probs:
                        probs.append(msg)
                        t_rep["problems"] = probs
                        t_rep["attributed_late_dropped_frames"] = int(
                            t_rep.get("attributed_late_dropped_frames") or 0
                        ) + cnt
                        t_rep["local_loss_events"] = int(t_rep.get("local_loss_events") or 0) + 1
                        t_rep["dropped_frames"] = int(t_rep.get("dropped_frames") or 0) + cnt
                        t_rep["qa_status"] = QA_STATUS_FAILED
                        t_rep["status"] = "blocked"
                        t_path = state_dir / "qa_reports" / (t_rel + ".qa.json")
                        if t_path.parent.exists():
                            _atomic_write_json(t_path, t_rep)

    # 2. Group eligible file reports by (batch_key, planned_start_wall_ns, planned_end_wall_ns, partition_minutes)
    SpanKey = tuple[int, int, int]
    by_span_and_batch: dict[tuple[str, SpanKey], list[dict[str, Any]]] = defaultdict(list)
    batches_per_span: dict[SpanKey, set[str]] = defaultdict(set)
    span_meta: dict[SpanKey, dict[str, Any]] = {}

    for wd in expected_windows:
        if int(wd["partition_minutes"]) != config_pm:
            continue
        s_key: SpanKey = (
            int(wd["planned_start_wall_ns"]),
            int(wd["planned_end_wall_ns"]),
            int(wd["partition_minutes"]),
        )
        span_meta[s_key] = wd

    for rep in file_reports.values():
        rep_pm = int(rep.get("partition_minutes") or 0)
        rep_start_ns = int(rep.get("planned_start_wall_ns") or 0)
        rep_end_ns = int(rep.get("planned_end_wall_ns") or 0)
        # Strict duration & start/end span isolation: never allow a 60m report into a 15m window (or vice versa)
        if rep_pm != config_pm or (rep_end_ns - rep_start_ns) != expected_span_ns or rep_start_ns <= 0:
            continue
        rep_run_id = rep.get("run_id")
        if run_id_filter is not None and rep_run_id != run_id_filter:
            continue
        b_key = str(rep_run_id or (run_id_filter or "__default__"))
        s_key = (rep_start_ns, rep_end_ns, config_pm)
        if s_key not in span_meta:
            span_meta[s_key] = window_descriptor(rep_start_ns // 1_000_000_000, config_pm)
        by_span_and_batch[(b_key, s_key)].append(rep)
        batches_per_span[s_key].add(b_key)

    # Ensure any target window with attributed late loss is also in span_meta
    for (b_key, target_wid) in list(attributed_late_losses.keys()):
        try:
            t_start_s = parse_window_start_s(target_wid)
            wd = window_descriptor(t_start_s, config_pm)
            s_key = (int(wd["planned_start_wall_ns"]), int(wd["planned_end_wall_ns"]), config_pm)
            span_meta.setdefault(s_key, wd)
            batches_per_span[s_key].add(b_key)
        except ValueError:
            pass

    window_reports: list[dict[str, Any]] = []
    overdue_windows: list[str] = []
    pending_windows: list[str] = []

    default_batch_key = str(run_id_filter or "__default__")

    for s_key in sorted(span_meta.keys()):
        wd = span_meta[s_key]
        wid = wd["window_id"]
        w_span_id = wd["window_span_id"]
        date_utc = wd["date_utc"]
        start_ns = int(wd["planned_start_wall_ns"])
        end_ns = int(wd["planned_end_wall_ns"])
        end_s = int(wd["planned_end_s"])
        is_overdue_time = now_s >= (end_s + sla_grace_seconds)

        span_batches = sorted(batches_per_span.get(s_key) or {default_batch_key})
        has_mixed_batches = len(span_batches) > 1

        for b_key in span_batches:
            reps = sorted(
                by_span_and_batch.get((b_key, s_key), []),
                key=lambda r: (str(r.get("exchange", "")), r.get("first_wall_ns") or 0, str(r.get("file_name", ""))),
            )
            startup_stubs = [
                {
                    "exchange": r.get("exchange"),
                    "file_name": r.get("file_name"),
                    "source_manifest": r.get("source_manifest"),
                    "boot_id": r.get("boot_id"),
                    "run_id": r.get("run_id"),
                    "head_gap_seconds": r.get("head_gap_seconds"),
                }
                for r in reps
                if r.get("is_startup_stub")
            ]

            present_exchanges = sorted({str(r.get("exchange")) for r in reps if r.get("exchange")})
            missing_exchanges = [ex for ex in expected_ex if ex not in present_exchanges]

            # Merge stream coverage across segments for the SAME exchange & SAME batch in this window
            merged_streams: dict[tuple[str, str, str], dict[str, Any]] = {}
            total_loss = 0
            total_dropped = 0
            total_gaps = 0
            total_depth_unavail = 0
            seq_breaks: list[str] = []
            damaged_segments: list[dict[str, Any]] = []
            review_reasons: list[str] = []
            hard_problems: list[str] = []

            if has_mixed_batches:
                hard_problems.append(
                    f"multiple collection batches mixed in window {w_span_id}: {span_batches}"
                )

            for r in reps:
                intrinsic_loss = int(
                    r.get("intrinsic_local_loss_events")
                    if r.get("intrinsic_local_loss_events") is not None
                    else r.get("local_loss_events") or 0
                )
                intrinsic_dropped = int(
                    r.get("intrinsic_dropped_frames")
                    if r.get("intrinsic_dropped_frames") is not None
                    else r.get("dropped_frames") or 0
                )
                total_loss += intrinsic_loss
                total_dropped += intrinsic_dropped
                total_gaps += int(r.get("gap_start_events") or 0)
                total_depth_unavail += int(r.get("depth_unavailable_events") or 0)
                for p in r.get("problems") or ():
                    if "rx_seq" in p or "monotonic" in p:
                        seq_breaks.append(f"{r.get('file_name')}: {p}")
                if r.get("qa_status") == QA_STATUS_FAILED:
                    damaged_segments.append(
                        {
                            "exchange": r.get("exchange"),
                            "file_name": r.get("file_name"),
                            "source_manifest": r.get("source_manifest"),
                            "run_id": r.get("run_id"),
                            "qa_status": r.get("qa_status"),
                            "local_loss_events": r.get("local_loss_events", 0),
                            "gap_start_events": r.get("gap_start_events", 0),
                            "problems": r.get("problems", []),
                        }
                    )
                elif r.get("qa_status") == QA_STATUS_NEEDS_REVIEW:
                    review_reasons.extend(
                        f"{r.get('file_name')}: {item}" for item in (r.get("review_items") or ())
                    )

                for s in r.get("streams") or ():
                    key = (str(s["exchange"]), str(s["symbol"]), str(s["channel"]))
                    cur = merged_streams.get(key)
                    f_ns = int(s["first_wall_ns"])
                    l_ns = int(s["last_wall_ns"])
                    int_gap_ns = int(float(s.get("max_internal_gap_seconds") or 0.0) * 1e9)
                    if cur is None:
                        merged_streams[key] = {
                            "count": int(s["count"]),
                            "first_ns": f_ns,
                            "last_ns": l_ns,
                            "max_internal_gap_ns": int_gap_ns,
                        }
                    else:
                        inter_gap_ns = max(0, f_ns - cur["last_ns"]) if f_ns > cur["last_ns"] else 0
                        cur["count"] += int(s["count"])
                        cur["first_ns"] = min(cur["first_ns"], f_ns)
                        cur["last_ns"] = max(cur["last_ns"], l_ns)
                        cur["max_internal_gap_ns"] = max(cur["max_internal_gap_ns"], int_gap_ns, inter_gap_ns)

            # Apply attributed late-frame data loss belonging to this target window ONCE per event
            attr_losses = attributed_late_losses.get((b_key, wid), [])
            for al in attr_losses:
                hard_problems.append(
                    f"attributed late-frame data loss from {al['source_file']} ({al['exchange']}): dropped={al['dropped_frames']}"
                )
                total_loss += 1
                total_dropped += int(al["dropped_frames"])
                if not any(r.get("exchange") == al["exchange"] and r.get("attributed_late_dropped_frames") for r in reps):
                    damaged_segments.append(
                        {
                            "exchange": al["exchange"],
                            "file_name": al["source_file"],
                            "source_manifest": al["source_manifest"],
                            "run_id": al["run_id"],
                            "qa_status": QA_STATUS_FAILED,
                            "local_loss_events": 1,
                            "gap_start_events": 0,
                            "problems": [f"late frame dropped for target window {wid}: dropped={al['dropped_frames']}"],
                        }
                    )

            missing_channels = sorted(all_expected_streams - set(merged_streams.keys()))
            missing_hard_channels = [
                "/".join(k) for k in missing_channels if classify_channel(k[0], k[2]) != CHANNEL_CATEGORY_EVENT_TRADE
            ]
            missing_trade_channels = [
                "/".join(k) for k in missing_channels if classify_channel(k[0], k[2]) == CHANNEL_CATEGORY_EVENT_TRADE
            ]

            max_gap_by_category: dict[str, float] = {}
            worst_head_s = 0.0
            worst_tail_s = 0.0
            for key, st in merged_streams.items():
                cat = classify_channel(key[0], key[2])
                h_s = max(0, st["first_ns"] - start_ns) / 1e9
                t_s = max(0, end_ns - st["last_ns"]) / 1e9
                g_s = max(h_s, t_s, st["max_internal_gap_ns"] / 1e9)
                worst_head_s = max(worst_head_s, h_s)
                worst_tail_s = max(worst_tail_s, t_s)
                max_gap_by_category[cat] = max(max_gap_by_category.get(cat, 0.0), round(g_s, 6))
                limit_s = (
                    thresholds["event_trade_quiet_max"]
                    if cat == CHANNEL_CATEGORY_EVENT_TRADE
                    else thresholds.get(cat, thresholds["other"])
                )
                if g_s > limit_s:
                    msg = f"merged stream gap > {limit_s:g}s ({cat}): {'/'.join(key)} ({g_s:.1f}s)"
                    if cat == CHANNEL_CATEGORY_EVENT_TRADE:
                        review_reasons.append(msg)
                    else:
                        hard_problems.append(msg)

            if has_mixed_batches or attr_losses:
                win_status = QA_STATUS_FAILED
            elif missing_exchanges or wid in unchecked:
                if is_overdue_time:
                    win_status = QA_STATUS_OVERDUE
                    if wid not in overdue_windows:
                        overdue_windows.append(wid)
                else:
                    win_status = QA_STATUS_PENDING_ARCHIVE
                    if wid not in pending_windows:
                        pending_windows.append(wid)
            else:
                non_stub_damaged = [
                    d for d in damaged_segments
                    if not any(
                        r.get("source_manifest") == d["source_manifest"]
                        and r.get("is_startup_stub")
                        and not (
                            r.get("local_loss_events")
                            or r.get("gap_start_events")
                            or r.get("depth_unavailable_events")
                        )
                        for r in reps
                    )
                ]
                if total_loss or total_gaps or total_depth_unavail or seq_breaks or missing_hard_channels or hard_problems:
                    win_status = QA_STATUS_FAILED
                elif non_stub_damaged:
                    unresolved_hard = False
                    for d in non_stub_damaged:
                        for p in d.get("problems") or ():
                            if not p.startswith("stream gap >"):
                                unresolved_hard = True
                    win_status = QA_STATUS_FAILED if unresolved_hard else (
                        QA_STATUS_NEEDS_REVIEW if (review_reasons or missing_trade_channels) else QA_STATUS_PASSED
                    )
                elif review_reasons or missing_trade_channels:
                    win_status = QA_STATUS_NEEDS_REVIEW
                elif startup_stubs and len(startup_stubs) == len(reps):
                    win_status = QA_STATUS_NEEDS_REVIEW
                    review_reasons.append("window_contains_only_startup_stub_segments")
                else:
                    win_status = QA_STATUS_PASSED

            eff_batch_out = None if b_key == "__default__" else b_key
            win_obj = {
                "report_level": "window",
                "qa_version": QA_VERSION,
                "checker_commit_sha": checker_commit,
                "effective_config_sha256": cfg_hash,
                "date_utc": date_utc,
                "window_id": wid,
                "planned_end_id": wd["planned_end_id"],
                "window_span_id": w_span_id,
                "partition_minutes": int(wd["partition_minutes"]),
                "planned_start_wall_ns": start_ns,
                "planned_end_wall_ns": end_ns,
                "run_id": eff_batch_out,
                "observed_batches": [None if b == "__default__" else b for b in span_batches],
                "qa_status": win_status,
                "status": "ok" if win_status == QA_STATUS_PASSED else "blocked",
                "expected_exchanges": expected_ex,
                "present_exchanges": present_exchanges,
                "missing_exchanges": missing_exchanges,
                "expected_channel_count": len(all_expected_streams),
                "observed_channel_count": len(merged_streams),
                "missing_hard_channels": missing_hard_channels,
                "missing_trade_channels": missing_trade_channels,
                "worst_head_gap_seconds": round(worst_head_s, 6) if merged_streams else None,
                "worst_tail_gap_seconds": round(worst_tail_s, 6) if merged_streams else None,
                "max_gap_by_category": max_gap_by_category,
                "local_loss_events": total_loss,
                "dropped_frames": total_dropped,
                "attributed_late_frames_rejected": sum(int(al["dropped_frames"]) for al in attr_losses),
                "attributed_late_losses": attr_losses,
                "gap_start_events": total_gaps,
                "depth_unavailable_events": total_depth_unavail,
                "sequence_breaks": seq_breaks,
                "segment_count": len(reps),
                "startup_stub_segments": startup_stubs,
                "damaged_segments": damaged_segments,
                "review_reasons": review_reasons,
                "hard_problems": hard_problems,
                "files": [r.get("source_manifest") for r in reps],
            }
            # Write isolated report by batch + start/end span, plus canonical window report path
            batch_folder = _safe_window_filename(eff_batch_out or "default")
            isolated_win_path = (
                state_dir
                / "qa_reports"
                / "windows"
                / batch_folder
                / date_utc
                / f"{_safe_window_filename(w_span_id)}.window.qa.json"
            )
            _atomic_write_json(isolated_win_path, win_obj)
            if not has_mixed_batches or b_key == span_batches[0]:
                canonical_win_path = (
                    state_dir / "qa_reports" / "windows" / date_utc / f"{_safe_window_filename(wid)}.window.qa.json"
                )
                _atomic_write_json(canonical_win_path, win_obj)
            window_reports.append(win_obj)

    return window_reports, overdue_windows, pending_windows


def _count_chrony_certified_minutes(
    remote: str,
    date_utc: str,
    *,
    download_retries: int,
    sleeper: Callable[[float], None],
    meta_files: set[str] | None = None,
) -> int | None:
    """Download read-only `meta/chrony/chrony_YYYYMMDD.csv` if present and count certified <=1ms minutes."""
    ymd = date_utc.replace("-", "")
    day_start_s = int(datetime.fromisoformat(date_utc).replace(tzinfo=timezone.utc).timestamp())
    sub_rel = f"chrony/chrony_{ymd}.csv"
    rel = f"meta/{sub_rel}"
    if meta_files is not None and sub_rel not in meta_files and rel not in meta_files:
        return None
    with tempfile.TemporaryDirectory(prefix="tokyo_qa_chrony_") as tmpdir:
        dst = Path(tmpdir) / f"chrony_{ymd}.csv"
        try:
            _download_with_retry(remote, rel, dst, retries=min(2, download_retries), sleeper=sleeper)
        except Exception:
            return None
        certified: set[int] = set()
        with dst.open(encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh):
                state = row.get("certified_le_1ms", row.get("certified_under_1ms", ""))
                wall = row.get("wall_ns") or row.get("ts_utc_ns")
                if wall and str(state).strip().lower() == "true":
                    minute = (int(wall) // 1_000_000_000 - day_start_s) // 60
                    if 0 <= minute < 1440:
                        certified.add(minute)
        return len(certified)


def _build_day_reports(
    *,
    remote: str,
    state_dir: Path,
    config: dict[str, Any],
    cfg_hash: str,
    checker_commit: str,
    file_reports: dict[str, dict[str, Any]],
    window_reports: list[dict[str, Any]],
    now_s: float,
    sla_grace_seconds: float,
    download_retries: int,
    sleeper: Callable[[float], None],
    run_id_filter: str | None = None,
    meta_files: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Aggregate window and file reports into complete UTC day qualification reports isolated by duration and batch."""
    pm = configured_partition_minutes(config)
    expected_span_ns = pm * 60 * 1_000_000_000
    windows_per_day = (24 * 60) // pm
    expected_ex = sorted(enabled_exchanges(config).keys())

    eligible_files = [
        r
        for r in file_reports.values()
        if int(r.get("partition_minutes") or 0) == pm
        and (int(r.get("planned_end_wall_ns") or 0) - int(r.get("planned_start_wall_ns") or 0)) == expected_span_ns
        and (run_id_filter is None or r.get("run_id") == run_id_filter)
    ]

    dates = sorted(
        {w["date_utc"] for w in window_reports}
        | {str(r.get("date_utc")) for r in eligible_files if r.get("date_utc")}
    )
    day_reports: list[dict[str, Any]] = []

    for day in dates:
        day_start_s = int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp())
        day_end_s = day_start_s + 86_400
        day_complete_time = now_s >= (day_end_s + sla_grace_seconds)

        expected_wids = [window_descriptor(day_start_s + i * pm * 60, pm)["window_id"] for i in range(windows_per_day)]
        day_win_list = [w for w in window_reports if w["date_utc"] == day]
        win_by_id: dict[str, dict[str, Any]] = {}
        for w in day_win_list:
            wid = w["window_id"]
            if wid not in win_by_id or w["qa_status"] == QA_STATUS_FAILED:
                win_by_id[wid] = w
        day_files = [r for r in eligible_files if r.get("date_utc") == day]
        observed_batches = sorted({str(r.get("run_id") or "") for r in day_files if r.get("run_id")})

        missing_windows = [wid for wid in expected_wids if wid not in win_by_id]
        failed_windows = [wid for wid, w in sorted(win_by_id.items()) if w["qa_status"] == QA_STATUS_FAILED]
        overdue_w = [wid for wid, w in sorted(win_by_id.items()) if w["qa_status"] == QA_STATUS_OVERDUE]
        review_w = [wid for wid, w in sorted(win_by_id.items()) if w["qa_status"] == QA_STATUS_NEEDS_REVIEW]
        pending_w = [wid for wid, w in sorted(win_by_id.items()) if w["qa_status"] == QA_STATUS_PENDING_ARCHIVE]

        # Cross-window sequence continuity per (exchange, boot_id) & restart tracking
        cross_window_seq_issues: list[str] = []
        restarts_by_exchange: dict[str, list[str]] = {}
        for ex in expected_ex:
            ex_files = sorted(
                (r for r in day_files if r.get("exchange") == ex),
                key=lambda r: (
                    int(r.get("planned_start_wall_ns") or 0),
                    int(r.get("first_wall_ns") or 0),
                    int(r.get("first_rx_seq") or 0),
                    str(r.get("file_name") or ""),
                ),
            )
            boots_ordered: list[str] = []
            prev_boot: str | None = None
            prev_last_seq: int | None = None
            prev_late_accepted = 0
            for rf in ex_files:
                b = str(rf.get("boot_id") or "")
                if not boots_ordered or boots_ordered[-1] != b:
                    boots_ordered.append(b)
                f_seq = rf.get("first_rx_seq")
                l_seq = rf.get("last_rx_seq")
                if b != prev_boot:
                    prev_boot = b
                    prev_last_seq = int(l_seq) if l_seq is not None else None
                    prev_late_accepted = int(rf.get("late_frames_accepted") or 0)
                    continue
                if prev_last_seq is not None and f_seq is not None:
                    if prev_late_accepted == 0 and int(f_seq) != prev_last_seq + 1:
                        cross_window_seq_issues.append(
                            f"{ex}:{rf.get('window_id')}:boot={b}:expected_first_rx_seq={prev_last_seq + 1},got={f_seq}"
                        )
                    elif prev_late_accepted > 0 and int(f_seq) > prev_last_seq + 1:
                        cross_window_seq_issues.append(
                            f"{ex}:{rf.get('window_id')}:boot={b}:gap_after_late_frames:prev_last={prev_last_seq},first={f_seq}"
                        )
                if l_seq is not None:
                    prev_last_seq = max(prev_last_seq, int(l_seq)) if prev_last_seq is not None else int(l_seq)
                prev_late_accepted = int(rf.get("late_frames_accepted") or 0)
            restarts_by_exchange[ex] = boots_ordered

        chrony_certified = _count_chrony_certified_minutes(
            remote,
            day,
            download_retries=download_retries,
            sleeper=sleeper,
            meta_files=meta_files,
        )
        clock_ok = chrony_certified == 1440

        damaged_partitions = [
            {
                "exchange": r.get("exchange"),
                "window_id": r.get("window_id"),
                "window_span_id": r.get("window_span_id"),
                "run_id": r.get("run_id"),
                "file_name": r.get("file_name"),
                "source_manifest": r.get("source_manifest"),
                "qa_status": r.get("qa_status"),
                "local_loss_events": r.get("local_loss_events", 0),
                "gap_start_events": r.get("gap_start_events", 0),
                "problems": r.get("problems", []),
            }
            for r in sorted(day_files, key=lambda x: str(x.get("source_manifest", "")))
            if r.get("qa_status") == QA_STATUS_FAILED
        ]
        startup_stubs = [
            {
                "exchange": r.get("exchange"),
                "window_id": r.get("window_id"),
                "run_id": r.get("run_id"),
                "file_name": r.get("file_name"),
                "boot_id": r.get("boot_id"),
            }
            for r in sorted(day_files, key=lambda x: str(x.get("source_manifest", "")))
            if r.get("is_startup_stub")
        ]

        if failed_windows or cross_window_seq_issues or len(observed_batches) > 1 or (day_complete_time and chrony_certified is not None and not clock_ok):
            day_status = QA_STATUS_FAILED
        elif overdue_w or (day_complete_time and missing_windows):
            day_status = QA_STATUS_OVERDUE
        elif missing_windows or pending_w or not day_complete_time:
            day_status = QA_STATUS_PENDING_ARCHIVE
        elif review_w or chrony_certified is None:
            day_status = QA_STATUS_NEEDS_REVIEW
        else:
            day_status = QA_STATUS_PASSED

        day_obj = {
            "report_level": "day",
            "qa_version": QA_VERSION,
            "checker_commit_sha": checker_commit,
            "effective_config_sha256": cfg_hash,
            "date_utc": day,
            "partition_minutes": pm,
            "run_id": run_id_filter,
            "observed_batches": observed_batches,
            "expected_windows_count": windows_per_day,
            "observed_windows_count": len(win_by_id),
            "missing_windows": missing_windows,
            "failed_windows": failed_windows,
            "overdue_windows": overdue_w,
            "pending_windows": pending_w,
            "review_windows": review_w,
            "cross_window_sequence_issues": cross_window_seq_issues,
            "restarts_by_exchange": restarts_by_exchange,
            "clock_certified_minutes": chrony_certified,
            "clock_certification_passed": clock_ok,
            "startup_stub_segments": startup_stubs,
            "damaged_partitions": damaged_partitions,
            "qa_status": day_status,
            "status": "ok" if day_status == QA_STATUS_PASSED else "blocked",
        }
        batch_folder = _safe_window_filename(run_id_filter or "default")
        isolated_day_path = state_dir / "qa_reports" / "days" / batch_folder / f"{day}_p{pm}m.day.qa.json"
        _atomic_write_json(isolated_day_path, day_obj)
        canonical_day_path = state_dir / "qa_reports" / "days" / f"{day}.day.qa.json"
        _atomic_write_json(canonical_day_path, day_obj)
        day_reports.append(day_obj)

    return day_reports


def run(
    remote: str,
    config_path: Path,
    state_dir: Path,
    limit: int,
    since_date: str,
    force: bool = False,
    since_hour: str | None = None,
    *,
    since_window: str | None = None,
    run_id: str | None = None,
    checker_commit: str | None = None,
    now_epoch_s: float | None = None,
    sla_grace_minutes: float | None = None,
    download_retries: int = 3,
    sleeper: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Run incremental QA across verified archive candidates and emit file/window/day reports.

    Separates:
      1) `job_execution_status`: whether the QA runner itself executed cleanly (`"ok"` vs `"failed"`)
      2) `current_window_completed`, `current_window_verification_status`, & `heartbeat_status`:
         whether the latest expected/due window completed verification and passed/failed/is pending
      3) `historical_data_quality_status`, `historical_damaged_windows`, `historical_overdue_windows`,
         `historical_damaged_partitions`, & `historical_loss_manifests`:
         persistent historical damaged/missing window and loss-marked manifest evidence across batch cutovers.
    """
    from data_coverage import (
        previous_complete_window,
        resolve_cutover_window_utc,
        resolve_effective_run_id,
    )

    config = json.loads(config_path.read_text(encoding="utf-8"))
    effective_run_id = resolve_effective_run_id(config, run_id)
    effective_since_spec = resolve_cutover_window_utc(config, since_window or since_hour)
    since_start_s = parse_window_start_s(effective_since_spec) if effective_since_spec is not None else None
    pm = configured_partition_minutes(config)
    cfg_hash = effective_config_hash(config)
    commit_sha = _resolve_checker_commit(checker_commit)
    now_s = time.time() if now_epoch_s is None else float(now_epoch_s)
    eff_sla_min = (
        float(sla_grace_minutes)
        if sla_grace_minutes is not None
        else (10.0 if pm < 60 else 20.0)
    )
    sla_grace_seconds = max(0.0, eff_sla_min * 60.0)
    meta_files = _list_meta_files(remote)

    (
        candidates,
        discovery_mode,
        discovery_warnings,
        historical_loss_manifests,
        index_effective_run_id,
    ) = _discover_verified_candidates(
        remote,
        config,
        since_date=since_date,
        since_start_s=since_start_s,
        run_id_filter=effective_run_id,
        download_retries=download_retries,
        sleeper=sleeper,
        meta_files=meta_files,
        state_dir=state_dir,
    )
    if (
        effective_run_id is not None
        and index_effective_run_id is not None
        and index_effective_run_id != effective_run_id
    ):
        discovery_warnings.append(
            f"verified index effective_run_id ({index_effective_run_id}) differs from requested effective_run_id ({effective_run_id})"
        )

    index_available = discovery_mode != "verified_index_unavailable"
    attempted = ok = blocked = review_cnt = skipped = transient = 0
    remaining_backlog = 0
    unchecked_windows: set[str] = set()
    newly_failed_windows: set[str] = set()
    summary: list[str] = []
    file_reports: dict[str, dict[str, Any]] = {}

    if not index_available:
        summary.append(
            "ERROR verified_archive_index_unavailable: trusted meta/archive_manifest_index.json could not be downloaded or parsed"
        )

    for cand in candidates:
        rel = cand["source_manifest"]
        result_path = state_dir / "qa_reports" / (rel + ".qa.json")
        if not force and result_path.is_file():
            try:
                previous = json.loads(result_path.read_text(encoding="utf-8"))
                if _is_memoized_up_to_date(previous, cand, cfg_hash=cfg_hash, config_pm=pm):
                    skipped += 1
                    file_reports[rel] = previous
                    continue
            except (ValueError, OSError):
                pass
        if attempted >= limit:
            remaining_backlog += 1
            unchecked_windows.add(cand["window_id"])
            continue
        attempted += 1
        with tempfile.TemporaryDirectory(prefix="tokyo_qa_") as temp:
            directory = Path(temp)
            manifest_path = directory / "manifest.json"
            try:
                _download_with_retry(
                    remote, rel, manifest_path, retries=download_retries, sleeper=sleeper
                )
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as exc:
                transient += 1
                unchecked_windows.add(cand["window_id"])
                summary.append(f"RETRY {rel}: manifest download {type(exc).__name__}")
                continue
            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                expected_name = Path(rel).name.removesuffix(".manifest.json") + ".tsv.zst"
                if manifest.get("file_name") != expected_name:
                    raise ValueError("manifest file_name mismatches archive path")
                if not 0 < int(manifest.get("bytes") or 0) <= MAX_PARTITION_BYTES:
                    raise ValueError("invalid file size or partition exceeds 8 GiB runner disk cap")
                data_path = directory / expected_name
                try:
                    _download_with_retry(
                        remote,
                        str(Path(rel).parent / expected_name),
                        data_path,
                        retries=download_retries,
                        sleeper=sleeper,
                    )
                except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as exc:
                    transient += 1
                    unchecked_windows.add(cand["window_id"])
                    summary.append(f"RETRY {rel}: data download {type(exc).__name__}")
                    continue
                _verify_three_way_index_manifest_data(
                    cand=cand,
                    manifest_path=manifest_path,
                    manifest=manifest,
                    data_path=data_path,
                    run_id_filter=effective_run_id,
                )
                result = check_partition(data_path, manifest, config)
            except (ValueError, TypeError, KeyError, UnicodeError, zstd.ZstdError, RuntimeError) as exc:
                result = {
                    "status": "blocked",
                    "qa_status": QA_STATUS_FAILED,
                    "report_level": "file",
                    "exchange": cand["exchange"],
                    "date_utc": cand["date_utc"],
                    "window_id": cand["window_id"],
                    "planned_end_id": cand.get("planned_end_id"),
                    "window_span_id": cand.get("window_span_id"),
                    "partition_minutes": cand["partition_minutes"],
                    "planned_start_wall_ns": cand["planned_start_s"] * 1_000_000_000,
                    "planned_end_wall_ns": cand["planned_end_s"] * 1_000_000_000,
                    "run_id": cand.get("run_id"),
                    "file_name": Path(cand["source_data"]).name,
                    "problems": [f"{type(exc).__name__}: {exc}"],
                }
            result.update(
                {
                    "qa_version": QA_VERSION,
                    "checker_commit_sha": commit_sha,
                    "effective_config_sha256": cfg_hash,
                    "source_manifest": rel,
                    "manifest_sha256": _sha256(manifest_path),
                    "index_data_sha256": cand.get("data_sha256"),
                    "index_manifest_sha256": cand.get("manifest_sha256"),
                    "three_way_verified": result.get("qa_status") != QA_STATUS_FAILED
                    or not any("three-way verification failed" in p for p in (result.get("problems") or ())),
                    "discovery_source": discovery_mode,
                }
            )
            _atomic_write_json(result_path, result)
            file_reports[rel] = result
            qst = result.get("qa_status", QA_STATUS_FAILED)
            if qst == QA_STATUS_PASSED:
                ok += 1
            elif qst == QA_STATUS_NEEDS_REVIEW:
                review_cnt += 1
            else:
                blocked += 1
                newly_failed_windows.add(str(result.get("window_id") or cand["window_id"]))
            summary.append(f"{qst.upper()} {rel}: {len(result.get('problems', []))} issues")

    # Load previously checked reports for eligible candidates so historical damaged partitions remain queryable.
    unresolved_failed = 0
    unresolved_review = 0
    for cand in candidates:
        rel = cand["source_manifest"]
        report = state_dir / "qa_reports" / (rel + ".qa.json")
        if report.is_file():
            try:
                obj = json.loads(report.read_text(encoding="utf-8"))
                if (
                    obj.get("qa_version") == QA_VERSION
                    and obj.get("effective_config_sha256") == cfg_hash
                    and int(obj.get("partition_minutes") or 0) == pm
                ):
                    file_reports.setdefault(rel, obj)
                    qst = obj.get("qa_status")
                    if qst == QA_STATUS_FAILED or (qst is None and obj.get("status") == "blocked"):
                        unresolved_failed += 1
                    elif qst == QA_STATUS_NEEDS_REVIEW:
                        unresolved_review += 1
            except (ValueError, OSError):
                pass

    # Enumerate expected complete windows when a start window (`since_window` / `since_hour`) was given,
    # OR when zero candidates exist so overdue zero-file runs NEVER report false-green.
    expected_windows: list[dict[str, Any]] = []
    if effective_since_spec is not None:
        expected_windows = iter_expected_windows(effective_since_spec, now_s, pm)
    elif not candidates and index_available:
        expected_windows = iter_expected_windows(f"{since_date}T00:00Z", now_s, pm)

    window_reports, overdue_windows, pending_windows = _build_window_reports(
        state_dir=state_dir,
        config=config,
        cfg_hash=cfg_hash,
        checker_commit=commit_sha,
        file_reports=file_reports,
        expected_windows=expected_windows,
        now_s=now_s,
        sla_grace_seconds=sla_grace_seconds,
        run_id_filter=effective_run_id,
        unchecked_windows=unchecked_windows,
    )
    day_reports = _build_day_reports(
        remote=remote,
        state_dir=state_dir,
        config=config,
        cfg_hash=cfg_hash,
        checker_commit=commit_sha,
        file_reports=file_reports,
        window_reports=window_reports,
        now_s=now_s,
        sla_grace_seconds=sla_grace_seconds,
        download_retries=download_retries,
        sleeper=sleeper,
        run_id_filter=effective_run_id,
        meta_files=meta_files,
    )

    # Consolidate queryable historical loss-marked manifests across ALL batches and durations
    # (from archive index, previously cached historical_loss_manifests.json, any prior .qa.json on state_dir,
    # and any partition checked in this run with local_loss_events / late_frames_rejected > 0).
    historical_loss_by_manifest: dict[str, dict[str, Any]] = {
        str(m["source_manifest"]): dict(m)
        for m in historical_loss_manifests
        if isinstance(m, dict) and m.get("source_manifest")
    }
    raw_reports_root = state_dir / "qa_reports" / "raw"
    if raw_reports_root.is_dir():
        for r_path in sorted(raw_reports_root.rglob("*.manifest.json.qa.json")):
            try:
                r_obj = json.loads(r_path.read_text(encoding="utf-8"))
                src_mf = str(
                    r_obj.get("source_manifest")
                    or r_path.relative_to(state_dir / "qa_reports").as_posix().removesuffix(".qa.json")
                )
                loss_ev = int(r_obj.get("local_loss_events") or 0)
                dropped_fr = int(r_obj.get("dropped_frames") or 0)
                late_rej = int(r_obj.get("late_frames_rejected") or 0)
                if loss_ev > 0 or dropped_fr > 0 or late_rej > 0:
                    existing = historical_loss_by_manifest.get(src_mf, {})
                    historical_loss_by_manifest[src_mf] = {
                        "source_manifest": src_mf,
                        "source_data": existing.get("source_data")
                        or src_mf.removesuffix(".manifest.json") + ".tsv.zst",
                        "file_name": r_obj.get("file_name") or existing.get("file_name") or Path(src_mf).name,
                        "exchange": r_obj.get("exchange") or existing.get("exchange"),
                        "date_utc": r_obj.get("date_utc") or existing.get("date_utc"),
                        "window_id": r_obj.get("window_id") or existing.get("window_id"),
                        "window_span_id": r_obj.get("window_span_id") or existing.get("window_span_id"),
                        "partition_minutes": int(
                            r_obj.get("partition_minutes") or existing.get("partition_minutes") or 60
                        ),
                        "run_id": r_obj.get("run_id") if r_obj.get("run_id") is not None else existing.get("run_id"),
                        "boot_id": r_obj.get("boot_id") or existing.get("boot_id"),
                        "local_loss_events": max(loss_ev, int(existing.get("local_loss_events") or 0)),
                        "local_loss_lines": max(
                            int(existing.get("local_loss_lines") or 0),
                            int(r_obj.get("intrinsic_local_loss_events") or loss_ev),
                        ),
                        "dropped_frames": max(dropped_fr, int(existing.get("dropped_frames") or 0)),
                        "late_frames_rejected": max(late_rej, int(existing.get("late_frames_rejected") or 0)),
                        "gap_start_events": max(
                            int(r_obj.get("gap_start_events") or 0),
                            int(existing.get("gap_start_events") or 0),
                        ),
                        "sha256": r_obj.get("sha256") or existing.get("sha256"),
                        "manifest_sha256": r_obj.get("manifest_sha256") or existing.get("manifest_sha256"),
                        "problems": r_obj.get("problems") or existing.get("problems") or [],
                        "qa_status": QA_STATUS_FAILED,
                        "status": "blocked",
                    }
            except (ValueError, OSError):
                pass
    historical_loss_manifests_out = sorted(
        historical_loss_by_manifest.values(),
        key=lambda x: (
            str(x.get("date_utc") or ""),
            str(x.get("window_id") or ""),
            str(x.get("source_manifest") or ""),
        ),
    )
    try:
        _atomic_write_json(
            state_dir / "qa_reports" / "historical_loss_manifests.json",
            {
                "schema_version": 1,
                "count": len(historical_loss_manifests_out),
                "manifests": historical_loss_manifests_out,
            },
        )
    except OSError:
        pass

    # Separate:
    # 1) Job execution status
    # 2) Current due/expected window completion, status, and independent heartbeat
    # 3) Historical damaged/overdue windows & partitions (including prior 4 loss-marked manifests across batches)
    job_execution_status = "ok" if (index_available and transient == 0) else "failed"

    latest_complete_wd = previous_complete_window(now_s, pm)
    sorted_win_reports = sorted(
        window_reports,
        key=lambda w: (int(w.get("planned_start_wall_ns") or 0), str(w.get("window_id") or "")),
    )
    current_win_rep = sorted_win_reports[-1] if sorted_win_reports else None
    current_window_id = (
        str(current_win_rep["window_id"])
        if current_win_rep is not None
        else str(latest_complete_wd["window_id"])
    )

    if not index_available:
        current_window_verification_status = QA_STATUS_FAILED
        current_window_completed = False
    elif current_win_rep is not None:
        current_window_verification_status = str(current_win_rep["qa_status"])
        current_window_completed = current_window_verification_status in {
            QA_STATUS_PASSED,
            QA_STATUS_FAILED,
            QA_STATUS_NEEDS_REVIEW,
        }
    elif effective_since_spec is not None and not expected_windows and not file_reports:
        current_window_verification_status = QA_STATUS_PENDING_ARCHIVE
        current_window_completed = False
    else:
        is_latest_overdue = now_s >= (int(latest_complete_wd["planned_end_s"]) + sla_grace_seconds)
        current_window_verification_status = (
            QA_STATUS_OVERDUE if is_latest_overdue else QA_STATUS_PENDING_ARCHIVE
        )
        current_window_completed = False

    failed_win_ids = [w["window_id"] for w in sorted_win_reports if w["qa_status"] == QA_STATUS_FAILED]
    review_win_ids = [w["window_id"] for w in sorted_win_reports if w["qa_status"] == QA_STATUS_NEEDS_REVIEW]

    current_overdue_windows = [wid for wid in overdue_windows if wid == current_window_id]
    historical_overdue_windows = [wid for wid in overdue_windows if wid != current_window_id]
    current_pending_windows = [wid for wid in pending_windows if wid == current_window_id]

    current_damaged_windows = [
        wid for wid in failed_win_ids if wid == current_window_id
    ]
    historical_damaged_windows = sorted(
        set(wid for wid in failed_win_ids if wid != current_window_id)
        | set(historical_overdue_windows)
    )

    all_damaged_partitions = [
        {
            "source_manifest": r.get("source_manifest"),
            "file_name": r.get("file_name"),
            "exchange": r.get("exchange"),
            "date_utc": r.get("date_utc"),
            "window_id": r.get("window_id"),
            "window_span_id": r.get("window_span_id"),
            "partition_minutes": r.get("partition_minutes"),
            "run_id": r.get("run_id"),
            "local_loss_events": r.get("local_loss_events", 0),
            "dropped_frames": r.get("dropped_frames", 0),
            "gap_start_events": r.get("gap_start_events", 0),
            "problems": r.get("problems", []),
        }
        for r in sorted(file_reports.values(), key=lambda x: str(x.get("source_manifest") or ""))
        if r.get("qa_status") == QA_STATUS_FAILED
    ]
    current_damaged_partitions = [
        p
        for p in all_damaged_partitions
        if p["window_id"] == current_window_id
    ]
    historical_damaged_by_manifest: dict[str, dict[str, Any]] = {
        str(p["source_manifest"]): p
        for p in all_damaged_partitions
        if p not in current_damaged_partitions and p.get("source_manifest")
    }
    current_damaged_manifest_set = {
        str(p["source_manifest"]) for p in current_damaged_partitions if p.get("source_manifest")
    }
    for hm in historical_loss_manifests_out:
        src_mf = str(hm.get("source_manifest") or "")
        if src_mf and src_mf not in current_damaged_manifest_set and src_mf not in historical_damaged_by_manifest:
            historical_damaged_by_manifest[src_mf] = {
                "source_manifest": src_mf,
                "file_name": hm.get("file_name"),
                "exchange": hm.get("exchange"),
                "date_utc": hm.get("date_utc"),
                "window_id": hm.get("window_id"),
                "window_span_id": hm.get("window_span_id"),
                "partition_minutes": hm.get("partition_minutes"),
                "run_id": hm.get("run_id"),
                "local_loss_events": hm.get("local_loss_events", hm.get("local_loss_lines", 0)),
                "dropped_frames": hm.get("dropped_frames", 0),
                "gap_start_events": hm.get("gap_start_events", 0),
                "problems": hm.get("problems", []),
            }
    historical_damaged_partitions = sorted(
        historical_damaged_by_manifest.values(),
        key=lambda x: str(x.get("source_manifest") or ""),
    )
    historical_data_quality_status = (
        "has_historical_damage"
        if (
            historical_damaged_windows
            or historical_overdue_windows
            or historical_damaged_partitions
            or historical_loss_manifests_out
        )
        else "clean"
    )

    current_window_failed_files = [
        rel
        for rel, rep in file_reports.items()
        if rep.get("window_id") == current_window_id and rep.get("qa_status") == QA_STATUS_FAILED
    ]
    current_window_review_files = [
        rel
        for rel, rep in file_reports.items()
        if rep.get("window_id") == current_window_id and rep.get("qa_status") == QA_STATUS_NEEDS_REVIEW
    ]

    # Latest window QA status & heartbeat determination:
    # Determined independently by job execution status and the current due window's verification status,
    # while historical damaged or historical missing/overdue windows are preserved and reported separately!
    if (
        not index_available
        or transient
        or current_damaged_windows
        or current_window_failed_files
        or current_window_verification_status == QA_STATUS_FAILED
    ):
        overall_qa_status = QA_STATUS_FAILED
    elif (
        current_overdue_windows
        or current_window_verification_status == QA_STATUS_OVERDUE
        or (not current_window_completed and overdue_windows)
    ):
        overall_qa_status = QA_STATUS_OVERDUE
    elif (
        current_pending_windows
        or current_window_verification_status == QA_STATUS_PENDING_ARCHIVE
        or not current_window_completed
    ):
        overall_qa_status = QA_STATUS_PENDING_ARCHIVE
    elif current_window_review_files or current_window_verification_status == QA_STATUS_NEEDS_REVIEW:
        overall_qa_status = QA_STATUS_NEEDS_REVIEW
    else:
        overall_qa_status = QA_STATUS_PASSED

    heartbeat_status = (
        "ok"
        if (
            job_execution_status == "ok"
            and current_window_completed
            and current_window_verification_status == QA_STATUS_PASSED
            and overall_qa_status == QA_STATUS_PASSED
        )
        else "failed"
    )
    overall_status = "ok" if heartbeat_status == "ok" else "blocked"

    out_summary = {
        "status": overall_status,
        "qa_status": overall_qa_status,
        "heartbeat_status": heartbeat_status,
        "job_execution_status": job_execution_status,
        "current_window_id": current_window_id,
        "current_window_completed": current_window_completed,
        "current_window_verification_status": current_window_verification_status,
        "historical_data_quality_status": historical_data_quality_status,
        "historical_damaged_windows": historical_damaged_windows,
        "historical_overdue_windows": historical_overdue_windows,
        "historical_damaged_partitions": historical_damaged_partitions,
        "historical_loss_manifests": historical_loss_manifests_out,
        "current_damaged_windows": current_damaged_windows,
        "current_damaged_partitions": current_damaged_partitions,
        "qa_version": QA_VERSION,
        "checker_commit_sha": commit_sha,
        "effective_config_sha256": cfg_hash,
        "partition_minutes": pm,
        "run_id": effective_run_id,
        "effective_run_id": effective_run_id,
        "index_effective_run_id": index_effective_run_id,
        "effective_since_window": effective_since_spec,
        "discovery_mode": discovery_mode,
        "discovery_warnings": discovery_warnings,
        "attempted": attempted,
        "ok": ok,
        "blocked": blocked,
        "needs_review": review_cnt,
        "retry_needed": transient,
        "remaining_backlog": remaining_backlog,
        "unresolved_blocked": unresolved_failed,
        "unresolved_review": unresolved_review,
        "already_checked": skipped,
        "eligible_manifest_count": len(candidates),
        "expected_window_count": len(expected_windows),
        "window_reports_written": len(window_reports),
        "day_reports_written": len(day_reports),
        "overdue_windows": overdue_windows,
        "pending_archive_windows": pending_windows,
        "failed_windows": failed_win_ids,
        "review_windows": review_win_ids,
        "details": summary,
    }
    try:
        _atomic_write_json(state_dir / "qa_reports" / "latest_run_summary.json", out_summary)
    except OSError:
        pass
    return out_summary


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Incremental read-only Tokyo L2 archive QA (file, window, and UTC day reports)"
    )
    parser.add_argument("--remote", required=True, help="Read-only rclone remote root")
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--state-dir", required=True, type=Path)
    parser.add_argument("--since-date", default="2026-11-21")
    parser.add_argument("--since-hour", default=None, help="Backward-compatible inclusive UTC hour (YYYY-MM-DDTHH)")
    parser.add_argument("--since-window", default=None, help="Inclusive planned UTC window start (YYYY-MM-DDTHH:MMZ)")
    parser.add_argument("--run-id", default=None, help="Collection batch run_id filter")
    parser.add_argument("--checker-commit", default=None, help="Git commit SHA of checker recorded in every report")
    parser.add_argument("--sla-grace-minutes", type=float, default=None, help="Minutes after window end before missing archive becomes overdue")
    parser.add_argument("--download-retries", type=int, default=3, help="Retry count for transient cloud download errors")
    parser.add_argument("--limit", type=int, default=16)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    if args.limit < 1:
        parser.error("--limit must be positive")
    result = run(
        args.remote,
        args.config,
        args.state_dir,
        args.limit,
        args.since_date,
        args.force,
        args.since_hour,
        since_window=args.since_window,
        run_id=args.run_id,
        checker_commit=args.checker_commit,
        sla_grace_minutes=args.sla_grace_minutes,
        download_retries=args.download_retries,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["status"] == "ok" else 2


if __name__ == "__main__":
    raise SystemExit(main())

