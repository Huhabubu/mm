#!/usr/bin/env python3
"""Profile real Tokyo L2 archive QA phases (download, check_partition, decompress, JSON, book/protocol, lookback, HBT replay)."""
from __future__ import annotations

import argparse
from collections import defaultdict
import json
import os
from pathlib import Path
import resource
import time
from unittest.mock import patch

import zstandard as zstd

import data_coverage as coverage
import hourly_incremental_qa as archive
import tokyo_l2_protocol_qa as protocol
import tokyo_l2_replay_qa as replay


def _peak_rss_mb() -> dict[str, float]:
    # On Linux ru_maxrss is KiB.
    self_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0
    child_mb = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss / 1024.0
    return {
        "self_peak_rss_mb": round(self_mb, 2),
        "children_peak_rss_mb": round(child_mb, 2),
    }


def profile_window(
    remote: str,
    config: dict,
    run_id: str,
    window_id: str,
    *,
    replay_seconds: int = 660,
    warmup_windows: int = 0,
    lookback_windows: int = 0,
) -> dict:
    begin = coverage.parse_window_start_s(window_id)
    pm = coverage.configured_partition_minutes(config)
    step = pm * 60
    earliest = begin - max(warmup_windows, lookback_windows, 1) * step

    timings: dict[str, float] = defaultdict(float)
    counts: dict[str, int] = defaultdict(int)
    per_exchange_rows: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    current_exchange = [""]

    t_cand0 = time.perf_counter()
    candidates = replay.verified_candidates(remote, config, run_id, earliest, historical=True)
    timings["index_discovery_s"] = time.perf_counter() - t_cand0

    orig_selected = replay.selected_archives
    orig_lookback = replay.reconstruct_historical_checkpoint
    orig_check_partition = archive.check_partition
    orig_run_replay = replay.run_replay
    orig_load_specs = replay.load_btc_specs

    def timed_selected(*args, **kwargs):
        t0 = time.perf_counter()
        items = list(orig_selected(*args, **kwargs))
        timings["download_and_verify_s"] += time.perf_counter() - t0
        counts["target_and_warmup_files"] += len(items)
        for cand, _mf, _path in items:
            counts["compressed_bytes"] += int(cand.get("data_bytes") or 0)
        return iter(items)

    def timed_lookback(*args, **kwargs):
        t0 = time.perf_counter()
        res = orig_lookback(*args, **kwargs)
        timings["historical_lookback_total_s"] += time.perf_counter() - t0
        return res

    def timed_check_partition(data_path, mf, cfg):
        ex = str(mf.get("exchange") or "unknown")
        current_exchange[0] = ex
        t0 = time.perf_counter()
        res = orig_check_partition(data_path, mf, cfg)
        dt = time.perf_counter() - t0
        timings["check_partition_s"] += dt
        per_exchange_rows[ex]["check_partition_s"] += dt
        counts["check_partition_calls"] += 1
        return res

    def timed_rows(path: Path):
        ex = current_exchange[0] or path.name.split("_", 1)[0]
        row_count = 0
        with path.open("rb") as raw, zstd.open(raw, "rt", encoding="utf-8") as reader:
            it = iter(reader)
            while True:
                t0 = time.perf_counter()
                try:
                    line = next(it)
                except StopIteration:
                    dt = time.perf_counter() - t0
                    timings["decompress_s"] += dt
                    per_exchange_rows[ex]["decompress_s"] += dt
                    break
                t1 = time.perf_counter()
                dt_dec = t1 - t0
                timings["decompress_s"] += dt_dec
                per_exchange_rows[ex]["decompress_s"] += dt_dec

                wall, _mono, conn, epoch, _seq, kind, payload = line.rstrip("\n").split("\t", 6)
                obj = json.loads(payload)
                if kind == "DATA_ESCAPED" and isinstance(obj, str):
                    obj = json.loads(obj)
                if not isinstance(obj, dict):
                    raise ValueError("archive payload is not a JSON object")
                t2 = time.perf_counter()
                dt_json = t2 - t1
                timings["json_parse_s"] += dt_json
                per_exchange_rows[ex]["json_parse_s"] += dt_json

                row_count += 1
                yield int(wall), conn, int(epoch), kind, obj
                t3 = time.perf_counter()
                dt_proto = t3 - t2
                timings["protocol_and_book_s"] += dt_proto
                per_exchange_rows[ex]["protocol_and_book_s"] += dt_proto
        counts["rows_parsed"] += row_count
        per_exchange_rows[ex]["rows"] += row_count

    def timed_run_replay(*args, **kwargs):
        t0 = time.perf_counter()
        res = orig_run_replay(*args, **kwargs)
        timings["hbt_replay_s"] += time.perf_counter() - t0
        return res

    def timed_load_specs(*args, **kwargs):
        t0 = time.perf_counter()
        res = orig_load_specs(*args, **kwargs)
        timings["load_btc_specs_s"] += time.perf_counter() - t0
        return res

    t_total0 = time.perf_counter()
    export_state: dict = {}
    with patch.object(replay, "selected_archives", side_effect=timed_selected), \
         patch.object(replay, "reconstruct_historical_checkpoint", side_effect=timed_lookback), \
         patch.object(archive, "check_partition", side_effect=timed_check_partition), \
         patch.object(replay, "rows", side_effect=timed_rows), \
         patch.object(protocol, "rows", side_effect=timed_rows), \
         patch.object(replay, "run_replay", side_effect=timed_run_replay), \
         patch.object(replay, "load_btc_specs", side_effect=timed_load_specs):
        report = replay.audit_window(
            remote,
            config,
            run_id,
            begin,
            window_id,
            replay_seconds,
            warmup_windows,
            candidates=candidates,
            anchor_lookback_windows=lookback_windows if lookback_windows > 0 else warmup_windows,
            export_checkpoint_state=export_state,
        )
    timings["audit_window_wall_s"] = time.perf_counter() - t_total0

    return {
        "cpu_count": os.cpu_count(),
        "window_id": window_id,
        "replay_seconds": replay_seconds,
        "warmup_windows": warmup_windows,
        "lookback_windows": lookback_windows,
        "status": report.get("status"),
        "protocol_status": report.get("protocol_status"),
        "end_checkpoint_markets": len(report.get("end_sequence_checkpoint") or []),
        "proven_issues_count": len(report.get("proven_protocol_issues") or []),
        "unanchored_count": len(report.get("unanchored_lookback_markets") or []),
        "timings_s": {k: round(v, 3) for k, v in sorted(timings.items())},
        "per_exchange_s": {
            ex: {k: round(v, 3) for k, v in sorted(vals.items())}
            for ex, vals in sorted(per_exchange_rows.items())
        },
        "counts": dict(counts),
        "memory": _peak_rss_mb(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--remote", required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--windows", required=True, help="Comma-separated UTC window IDs")
    parser.add_argument("--warmup-windows", type=int, default=0)
    parser.add_argument("--lookback-windows", type=int, default=0)
    parser.add_argument("--replay-seconds", type=int, default=660)
    parser.add_argument("--compare-parallel", action="store_true")
    args = parser.parse_args()

    config = json.loads(args.config.read_text(encoding="utf-8"))
    results = []
    for wid in [w.strip() for w in args.windows.split(",") if w.strip()]:
        rs = args.replay_seconds if wid.endswith(":00Z") else 0
        res = profile_window(
            args.remote,
            config,
            args.run_id,
            wid,
            replay_seconds=rs,
            warmup_windows=args.warmup_windows,
            lookback_windows=args.lookback_windows,
        )
        results.append(res)
    print(json.dumps({"profiles": results}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
