"""Incremental hourly QA for Tokyo L2 sealed partitions.

One compressed partition at a time; persist immutable, versioned result files in
a separate GitHub state branch. Never touches the collector or writes to Drive.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tempfile

import zstandard as zstd

from data_coverage import _data_streams, expected_streams

QA_VERSION = 1
MAX_PARTITION_BYTES = 8 * 1024**3


def _run(*args: str) -> str:
    proc = subprocess.run(args, capture_output=True, text=True, check=False)
    if proc.returncode:
        raise RuntimeError(f"{args[0]} failed (exit {proc.returncode}): {proc.stderr[-600:]}")
    return proc.stdout


def _download(remote: str, rel_path: str, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["rclone", "copyto", f"{remote.rstrip('/')}/{rel_path}", str(target)],
        check=True, capture_output=True, text=True, timeout=1800,
    )


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def check_partition(data_path: Path, manifest: dict, config: dict) -> dict:
    """Inspect one sealed exchange-hour without loading all rows into memory."""
    exchange = manifest["exchange"]
    date = manifest["date_utc"]
    hour = int(manifest["hour_utc"])
    if not (0 <= hour < 24):
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
    start_ns = int(datetime.fromisoformat(date).replace(tzinfo=timezone.utc).timestamp() * 1e9)
    start_ns += hour * 3600 * 1_000_000_000
    end_ns = start_ns + 3600 * 1_000_000_000
    stale_ns = int(config.get("meta_daemon", {}).get("collector_stale_seconds", 300)) * 1_000_000_000
    if stale_ns <= 0:
        raise ValueError("stale threshold must be positive")

    counts = defaultdict(int)
    streams = {}
    problems = []
    first_seq = last_seq = prev_seq = prev_mono = None
    first_wall = last_wall = None
    loss_events = gap_events = 0
    dropped_frames = 0

    with data_path.open("rb") as fh:
        with zstd.ZstdDecompressor().stream_reader(fh) as reader:
            with io.TextIOWrapper(reader, encoding="utf-8", errors="strict") as lines:
                for n, line in enumerate(lines, 1):
                    parts = line.rstrip("\n").split("\t", 6)
                    if len(parts) != 7:
                        raise ValueError(f"line {n}: expected 7 TSV fields")
                    wall_ns, mono_ns, conn, epoch, rx_seq, kind, raw = parts
                    wall_ns, mono_ns, rx_seq = int(wall_ns), int(mono_ns), int(rx_seq)
                    if not (start_ns <= wall_ns < end_ns):
                        problems.append(f"line {n}: timestamp outside UTC partition")
                        if len(problems) >= 20:
                            break
                    if prev_seq is not None and rx_seq <= prev_seq:
                        problems.append(f"line {n}: rx_seq non-monotonic")
                    if prev_mono is not None and mono_ns < prev_mono:
                        problems.append(f"line {n}: receive monotonic clock regressed")
                    if len(problems) >= 20:
                        break
                    prev_seq, prev_mono = rx_seq, mono_ns
                    if first_seq is None:
                        first_seq, first_wall = rx_seq, wall_ns
                    last_seq, last_wall = rx_seq, wall_ns
                    if kind not in {"DATA", "DATA_ESCAPED", "META"}:
                        raise ValueError(f"line {n}: unknown record kind {kind}")
                    counts[kind] += 1
                    payload = json.loads(raw)
                    if kind == "META":
                        event = payload.get("event", "") if isinstance(payload, dict) else ""
                        if event == "LOCAL_LOSS":
                            loss_events += 1
                            dropped_frames += int(payload.get("dropped_frames", payload.get("dropped", 1)))
                        elif event == "GAP_START":
                            gap_events += 1
                    else:
                        if kind == "DATA_ESCAPED":
                            payload = json.loads(payload) if isinstance(payload, str) else payload
                        for key in _data_streams(exchange, payload) & expected:
                            item = streams.setdefault(key, {"count": 0, "first_ns": wall_ns, "last_ns": wall_ns, "max_internal_gap_ns": 0})
                            item["max_internal_gap_ns"] = max(item["max_internal_gap_ns"], wall_ns - item["last_ns"])
                            item["last_ns"] = wall_ns
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

    absent = ["/".join(s) for s in sorted(expected - set(streams))]
    if absent:
        problems.append(f"{len(absent)} expected channels have zero DATA")
    stream_rows = []
    for key in sorted(streams):
        item = streams[key]
        gap_ns = max(item["first_ns"] - start_ns, end_ns - item["last_ns"], item["max_internal_gap_ns"])
        stream_rows.append({
            "exchange": key[0], "symbol": key[1], "channel": key[2],
            "count": item["count"], "first_wall_ns": item["first_ns"],
            "last_wall_ns": item["last_ns"],
            "max_gap_seconds": round(gap_ns / 1e9, 6),
        })
        if gap_ns > stale_ns:
            problems.append(f"stream gap > {stale_ns / 1e9:g}s: {'/'.join(key)} ({gap_ns / 1e9:.1f}s)")

    return {
        "status": "ok" if not problems else "blocked",
        "exchange": exchange, "date_utc": date, "hour_utc": f"{hour:02d}",
        "boot_id": manifest.get("boot_id"), "file_name": data_path.name,
        "sha256": actual_sha, "bytes": data_path.stat().st_size,
        "data_records": counts["DATA"] + counts["DATA_ESCAPED"], "meta_records": counts["META"],
        "first_rx_seq": first_seq, "last_rx_seq": last_seq,
        "first_wall_ns": first_wall, "last_wall_ns": last_wall,
        "local_loss_events": loss_events, "dropped_frames": dropped_frames, "gap_start_events": gap_events,
        "streams": stream_rows, "missing_streams": absent, "problems": problems,
    }


def run(remote: str, config_path: Path, state_dir: Path, limit: int, since_date: str,
        force: bool = False) -> dict:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    relative = _run("rclone", "lsf", f"{remote.rstrip('/')}/raw", "-R", "--files-only",
                    "--include", "*.manifest.json")
    candidates = []
    for path in relative.splitlines():
        parts = path.split("/")
        if len(parts) != 3 or not parts[2].endswith(".manifest.json"):
            continue
        ex, day, filename = parts
        if day >= since_date and ex in config["exchanges"] and config["exchanges"][ex].get("enabled"):
            candidates.append(f"raw/{path}")
    candidates.sort(key=lambda p: (p.split("/")[2], p.split("/")[3], p.split("/")[1]))
    attempted = ok = blocked = skipped = transient = 0
    summary = []
    for rel in candidates:
        result_path = state_dir / "qa_reports" / (rel + ".qa.json")
        if not force and result_path.is_file():
            try:
                previous = json.loads(result_path.read_text(encoding="utf-8"))
                if previous.get("qa_version") == QA_VERSION and previous.get("source_manifest") == rel:
                    skipped += 1
                    continue
            except (ValueError, OSError):
                pass
        if attempted >= limit:
            break
        attempted += 1
        with tempfile.TemporaryDirectory(prefix="tokyo_qa_") as temp:
            directory = Path(temp)
            manifest_path = directory / "manifest.json"
            try:
                _download(remote, rel, manifest_path)
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                expected_name = Path(rel).name.removesuffix(".manifest.json") + ".tsv.zst"
                if manifest.get("file_name") != expected_name:
                    raise ValueError("manifest file_name mismatches archive path")
                if int(manifest.get("bytes") or 0) > MAX_PARTITION_BYTES:
                    raise RuntimeError("partition exceeds 8 GiB runner disk safety cap")
                data_path = directory / expected_name
                _download(remote, str(Path(rel).parent / expected_name), data_path)
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as e:
                transient += 1
                summary.append(f"RETRY {rel}: {type(e).__name__}")
                continue
            try:
                result = check_partition(data_path, manifest, config)
            except (ValueError, TypeError, KeyError, UnicodeError, zstd.ZstdError) as e:
                result = {"status": "blocked", "problems": [f"{type(e).__name__}: {e}"]}
            result.update({
                "qa_version": QA_VERSION, "source_manifest": rel,
                "manifest_sha256": _sha256(manifest_path),
            })
            result_path.parent.mkdir(parents=True, exist_ok=True)
            result_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            if result["status"] == "ok":
                ok += 1
            else:
                blocked += 1
            summary.append(f"{result['status'].upper()} {rel}: {len(result.get('problems', []))} issues")
    return {"status": "ok" if not (blocked or transient) else "blocked",
            "attempted": attempted, "ok": ok, "blocked": blocked, "retry_needed": transient,
            "already_checked": skipped, "eligible_manifest_count": len(candidates),
            "details": summary}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--remote", required=True, help="Read-only rclone remote root")
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--state-dir", required=True, type=Path)
    parser.add_argument("--since-date", default="2026-11-21")
    parser.add_argument("--limit", type=int, default=16)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    if args.limit < 1:
        parser.error("--limit must be positive")
    result = run(args.remote, args.config, args.state_dir, args.limit, args.since_date, args.force)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["status"] == "ok" else 2


if __name__ == "__main__":
    raise SystemExit(main())
