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
    return window_id.replace(":", "").replace("/", "_")


def check_partition(data_path: Path, manifest: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    """Inspect one sealed partition (legacy hourly or 5/15/30/60m segment) in streaming O(1) memory."""
    norm = normalize_manifest_window(manifest, fallback_file_name=data_path.name)
    exchange = norm["exchange"]
    date_utc = norm["date_utc"]
    hour_utc = norm["hour_utc"]
    minute_utc = norm["minute_utc"]
    partition_minutes = int(norm["partition_minutes"])
    window_id = norm["window_id"]
    start_ns = int(norm["planned_start_wall_ns"])
    end_ns = int(norm["planned_end_wall_ns"])

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
    first_seq = last_seq = prev_seq = None
    producer_prev_mono: dict[tuple[str, str, int], int] = {}
    first_wall = last_wall = None
    loss_events = gap_events = depth_unavailable_events = late_rejected_events = 0
    dropped_frames = 0

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
    if loss_events or gap_events:
        problems.append(f"recorded loss/gap events: loss={loss_events}, gap_start={gap_events}")
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
        "planned_start_wall_ns": start_ns,
        "planned_end_wall_ns": end_ns,
        "run_id": norm["run_id"],
        "segment_id": norm["segment_id"],
        "boot_id": norm["boot_id"],
        "is_startup_stub": norm["is_startup_stub"],
        "settled_by_timer": norm["settled_by_timer"],
        "coverage_incomplete": norm["coverage_incomplete"],
        "late_frames_accepted": norm["late_frames_accepted"],
        "late_frames_rejected": norm["late_frames_rejected"] or late_rejected_events,
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
        "local_loss_events": loss_events,
        "dropped_frames": dropped_frames,
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
) -> tuple[list[dict[str, Any]], str, list[str]]:
    """Discover candidate manifests from remotely verified archive index, with legacy fallback."""
    enabled = set(enabled_exchanges(config))
    candidates: list[dict[str, Any]] = []
    warnings: list[str] = []

    # 1. Preferred authoritative discovery: remotely verified `meta/archive_manifest_index.json`
    index_obj: dict[str, Any] | None = None
    index_basename = ARCHIVE_INDEX_RELPATH.removeprefix("meta/")
    should_fetch_index = meta_files is None or (
        index_basename in meta_files or ARCHIVE_INDEX_RELPATH in meta_files
    )
    if should_fetch_index:
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
                    index_obj = parsed
            except Exception:
                index_obj = None

    if index_obj is not None:
        for entry in index_obj["entries"]:
            if not isinstance(entry, dict):
                continue
            if not (entry.get("verified") and entry.get("remote_manifest_verified", True)):
                continue
            ex = str(entry.get("exchange") or "")
            day = str(entry.get("date_utc") or "")
            rel_mf = str(entry.get("remote_manifest_path") or "")
            if ex not in enabled or not rel_mf or day < since_date:
                continue
            if run_id_filter is not None and entry.get("collection_run_id") != run_id_filter:
                continue
            pm = int(entry.get("partition_minutes") or 60)
            planned_start_ns = entry.get("planned_start_wall_ns")
            if planned_start_ns is not None:
                planned_start_s = int(planned_start_ns) // 1_000_000_000
                wd = window_descriptor(planned_start_s, pm)
            else:
                hh = str(entry.get("hour_utc") or "00").zfill(2)
                mm = str(entry.get("minute_utc") or "00").zfill(2)
                planned_start_s = parse_window_start_s(f"{day}T{hh}:{mm}Z")
                wd = window_descriptor(planned_start_s, pm)
            if since_start_s is not None and wd["planned_start_s"] < since_start_s:
                continue
            candidates.append(
                {
                    "source_manifest": rel_mf,
                    "source_data": str(entry.get("remote_path") or rel_mf.removesuffix(".manifest.json") + ".tsv.zst"),
                    "exchange": ex,
                    "date_utc": wd["date_utc"],
                    "window_id": wd["window_id"],
                    "partition_minutes": pm,
                    "planned_start_s": wd["planned_start_s"],
                    "planned_end_s": wd["planned_end_s"],
                    "run_id": entry.get("collection_run_id"),
                    "boot_id": entry.get("boot_id"),
                    "segment_id": entry.get("segment_id") or entry.get("partition_id"),
                    "data_sha256": entry.get("sha256"),
                    "manifest_sha256": entry.get("manifest_sha256"),
                    "is_startup_stub": bool(entry.get("is_startup_stub", False)),
                    "discovery_source": "archive_manifest_index",
                }
            )
        candidates.sort(key=lambda c: (c["planned_start_s"], c["date_utc"], c["exchange"], c["source_manifest"]))
        return candidates, "archive_manifest_index", warnings

    # 2. Legacy fallback: rclone lsf raw/ (compatible with legacy hourly and new minute/segment names)
    warnings.append("archive_manifest_index_unavailable_using_raw_lsf_fallback")
    relative = _run(
        "rclone",
        "lsf",
        f"{remote.rstrip('/')}/raw",
        "-R",
        "--files-only",
        "--include",
        "*.manifest.json",
    )
    for path in relative.splitlines():
        path = path.strip()
        parts = path.split("/")
        if len(parts) != 3 or not parts[2].endswith(".manifest.json"):
            continue
        ex, day, _filename = parts
        if day < since_date or ex not in enabled:
            continue
        rel_mf = f"raw/{path}"
        parsed = parse_partition_window_from_rel_path(rel_mf)
        if parsed is None:
            continue
        if since_start_s is not None and parsed["planned_start_s"] < since_start_s:
            continue
        candidates.append(
            {
                "source_manifest": rel_mf,
                "source_data": rel_mf.removesuffix(".manifest.json") + ".tsv.zst",
                "exchange": ex,
                "date_utc": parsed["date_utc"],
                "window_id": parsed["window_id"],
                "partition_minutes": int(parsed["partition_minutes"]),
                "planned_start_s": int(parsed["planned_start_s"]),
                "planned_end_s": int(parsed["planned_end_s"]),
                "run_id": None,
                "boot_id": None,
                "segment_id": parsed["segment_id"],
                "data_sha256": None,
                "manifest_sha256": None,
                "is_startup_stub": False,
                "discovery_source": "raw_lsf_fallback",
            }
        )
    candidates.sort(key=lambda c: (c["planned_start_s"], c["date_utc"], c["exchange"], c["source_manifest"]))
    return candidates, "raw_lsf_fallback", warnings


def _is_memoized_up_to_date(
    previous: dict[str, Any],
    cand: dict[str, Any],
    *,
    cfg_hash: str,
) -> bool:
    if previous.get("qa_version") != QA_VERSION:
        return False
    if previous.get("source_manifest") != cand["source_manifest"]:
        return False
    if previous.get("effective_config_sha256") != cfg_hash:
        return False
    if previous.get("qa_status") not in {QA_STATUS_PASSED, QA_STATUS_FAILED, QA_STATUS_NEEDS_REVIEW}:
        return False
    if cand.get("data_sha256") and previous.get("sha256") != cand["data_sha256"]:
        return False
    if cand.get("manifest_sha256") and previous.get("manifest_sha256") != cand["manifest_sha256"]:
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
    unchecked_windows: set[str] | None = None,
) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    """Aggregate single-file QA reports into whole-window reports (4 exchanges, 72 channels)."""
    expected_ex = sorted(enabled_exchanges(config).keys())
    all_expected_streams = expected_streams(config)
    thresholds = channel_silence_thresholds(config)
    config_pm = configured_partition_minutes(config)
    unchecked = unchecked_windows or set()

    by_window: dict[str, list[dict[str, Any]]] = defaultdict(list)
    window_meta: dict[str, dict[str, Any]] = {}

    for wd in expected_windows:
        wid = wd["window_id"]
        window_meta[wid] = wd

    for rep in file_reports.values():
        wid = rep.get("window_id")
        if not wid:
            continue
        by_window[wid].append(rep)
        if wid not in window_meta:
            pm = int(rep.get("partition_minutes") or config_pm)
            start_s = int(rep.get("planned_start_wall_ns") or 0) // 1_000_000_000
            if start_s > 0:
                window_meta[wid] = window_descriptor(start_s, pm)

    window_reports: list[dict[str, Any]] = []
    overdue_windows: list[str] = []
    pending_windows: list[str] = []

    for wid in sorted(window_meta.keys()):
        wd = window_meta[wid]
        date_utc = wd["date_utc"]
        start_ns = int(wd["planned_start_wall_ns"])
        end_ns = int(wd["planned_end_wall_ns"])
        end_s = int(wd["planned_end_s"])
        is_overdue_time = now_s >= (end_s + sla_grace_seconds)

        reps = sorted(
            by_window.get(wid, []),
            key=lambda r: (str(r.get("exchange", "")), r.get("first_wall_ns") or 0, str(r.get("file_name", ""))),
        )
        startup_stubs = [
            {
                "exchange": r.get("exchange"),
                "file_name": r.get("file_name"),
                "source_manifest": r.get("source_manifest"),
                "boot_id": r.get("boot_id"),
                "head_gap_seconds": r.get("head_gap_seconds"),
            }
            for r in reps
            if r.get("is_startup_stub")
        ]

        present_exchanges = sorted({str(r.get("exchange")) for r in reps if r.get("exchange")})
        missing_exchanges = [ex for ex in expected_ex if ex not in present_exchanges]

        # Merge stream coverage across segments for the same exchange in this window
        merged_streams: dict[tuple[str, str, str], dict[str, Any]] = {}
        total_loss = 0
        total_dropped = 0
        total_gaps = 0
        total_depth_unavail = 0
        seq_breaks: list[str] = []
        damaged_segments: list[dict[str, Any]] = []
        review_reasons: list[str] = []
        hard_problems: list[str] = []

        for r in reps:
            total_loss += int(r.get("local_loss_events") or 0)
            total_dropped += int(r.get("dropped_frames") or 0)
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
                    # Inter-segment gap when restarted mid-window
                    inter_gap_ns = max(0, f_ns - cur["last_ns"]) if f_ns > cur["last_ns"] else 0
                    cur["count"] += int(s["count"])
                    cur["first_ns"] = min(cur["first_ns"], f_ns)
                    cur["last_ns"] = max(cur["last_ns"], l_ns)
                    cur["max_internal_gap_ns"] = max(cur["max_internal_gap_ns"], int_gap_ns, inter_gap_ns)

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

        if missing_exchanges or wid in unchecked:
            if is_overdue_time:
                win_status = QA_STATUS_OVERDUE
                overdue_windows.append(wid)
            else:
                win_status = QA_STATUS_PENDING_ARCHIVE
                pending_windows.append(wid)
        else:
            # Check whether all segments of an exchange collectively covered the window cleanly
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
            # If an exchange had multiple segments in one window, recompute whether merged coverage solved
            # individual segment head/tail gaps (excluding loss/gap/clock/seq errors!)
            if total_loss or total_gaps or total_depth_unavail or seq_breaks or missing_hard_channels or hard_problems:
                win_status = QA_STATUS_FAILED
            elif non_stub_damaged:
                # Check if damaged segments ONLY had per-segment head/tail gaps resolved by merging
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

        win_obj = {
            "report_level": "window",
            "qa_version": QA_VERSION,
            "checker_commit_sha": checker_commit,
            "effective_config_sha256": cfg_hash,
            "date_utc": date_utc,
            "window_id": wid,
            "partition_minutes": int(wd["partition_minutes"]),
            "planned_start_wall_ns": start_ns,
            "planned_end_wall_ns": end_ns,
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
        win_path = state_dir / "qa_reports" / "windows" / date_utc / f"{_safe_window_filename(wid)}.window.qa.json"
        _atomic_write_json(win_path, win_obj)
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
    meta_files: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Aggregate window and file reports into complete UTC day qualification reports."""
    pm = configured_partition_minutes(config)
    windows_per_day = (24 * 60) // pm
    expected_ex = sorted(enabled_exchanges(config).keys())

    dates = sorted({w["date_utc"] for w in window_reports} | {str(r.get("date_utc")) for r in file_reports.values() if r.get("date_utc")})
    day_reports: list[dict[str, Any]] = []

    for day in dates:
        day_start_s = int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp())
        day_end_s = day_start_s + 86_400
        day_complete_time = now_s >= (day_end_s + sla_grace_seconds)

        expected_wids = [window_descriptor(day_start_s + i * pm * 60, pm)["window_id"] for i in range(windows_per_day)]
        win_by_id = {w["window_id"]: w for w in window_reports if w["date_utc"] == day}
        day_files = [r for r in file_reports.values() if r.get("date_utc") == day]

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
                "file_name": r.get("file_name"),
                "boot_id": r.get("boot_id"),
            }
            for r in sorted(day_files, key=lambda x: str(x.get("source_manifest", "")))
            if r.get("is_startup_stub")
        ]

        if failed_windows or cross_window_seq_issues or (day_complete_time and chrony_certified is not None and not clock_ok):
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
        day_path = state_dir / "qa_reports" / "days" / f"{day}.day.qa.json"
        _atomic_write_json(day_path, day_obj)
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
    """Run incremental QA across verified archive candidates and emit file/window/day reports."""
    effective_since_spec = since_window or since_hour
    since_start_s = parse_window_start_s(effective_since_spec) if effective_since_spec is not None else None
    config = json.loads(config_path.read_text(encoding="utf-8"))
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

    candidates, discovery_mode, discovery_warnings = _discover_verified_candidates(
        remote,
        config,
        since_date=since_date,
        since_start_s=since_start_s,
        run_id_filter=run_id,
        download_retries=download_retries,
        sleeper=sleeper,
        meta_files=meta_files,
    )

    attempted = ok = blocked = review_cnt = skipped = transient = 0
    remaining_backlog = 0
    unchecked_windows: set[str] = set()
    summary: list[str] = []
    file_reports: dict[str, dict[str, Any]] = {}

    for cand in candidates:
        rel = cand["source_manifest"]
        result_path = state_dir / "qa_reports" / (rel + ".qa.json")
        if not force and result_path.is_file():
            try:
                previous = json.loads(result_path.read_text(encoding="utf-8"))
                if _is_memoized_up_to_date(previous, cand, cfg_hash=cfg_hash):
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
                result = check_partition(data_path, manifest, config)
            except (ValueError, TypeError, KeyError, UnicodeError, zstd.ZstdError, RuntimeError) as exc:
                result = {
                    "status": "blocked",
                    "qa_status": QA_STATUS_FAILED,
                    "report_level": "file",
                    "exchange": cand["exchange"],
                    "date_utc": cand["date_utc"],
                    "window_id": cand["window_id"],
                    "partition_minutes": cand["partition_minutes"],
                    "planned_start_wall_ns": cand["planned_start_s"] * 1_000_000_000,
                    "planned_end_wall_ns": cand["planned_end_s"] * 1_000_000_000,
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
            summary.append(f"{qst.upper()} {rel}: {len(result.get('problems', []))} issues")

    # Previously failed/review immutable partitions remain tracked and queryable.
    unresolved_failed = 0
    unresolved_review = 0
    for cand in candidates:
        rel = cand["source_manifest"]
        report = state_dir / "qa_reports" / (rel + ".qa.json")
        if report.is_file():
            try:
                obj = json.loads(report.read_text(encoding="utf-8"))
                if obj.get("qa_version") == QA_VERSION and obj.get("effective_config_sha256") == cfg_hash:
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
    elif not candidates:
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
        meta_files=meta_files,
    )

    # Determine overall 5-state QA conclusion (NEVER report zero-file overdue as passed!)
    failed_win_ids = [w["window_id"] for w in window_reports if w["qa_status"] == QA_STATUS_FAILED]
    review_win_ids = [w["window_id"] for w in window_reports if w["qa_status"] == QA_STATUS_NEEDS_REVIEW]

    if blocked or unresolved_failed or failed_win_ids or transient:
        overall_qa_status = QA_STATUS_FAILED
    elif overdue_windows:
        overall_qa_status = QA_STATUS_OVERDUE
    elif review_cnt or unresolved_review or review_win_ids:
        overall_qa_status = QA_STATUS_NEEDS_REVIEW
    elif pending_windows:
        overall_qa_status = QA_STATUS_PENDING_ARCHIVE
    elif effective_since_spec is not None and not expected_windows and not file_reports:
        overall_qa_status = QA_STATUS_PENDING_ARCHIVE
    else:
        overall_qa_status = QA_STATUS_PASSED

    overall_status = "ok" if overall_qa_status in {QA_STATUS_PASSED, QA_STATUS_PENDING_ARCHIVE} else "blocked"

    return {
        "status": overall_status,
        "qa_status": overall_qa_status,
        "qa_version": QA_VERSION,
        "checker_commit_sha": commit_sha,
        "effective_config_sha256": cfg_hash,
        "partition_minutes": pm,
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
    parser.add_argument("--run-id", default=None, help="Optional collection batch run_id filter")
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
