"""Run bounded incremental protocol validation after the archive QA gate."""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path

import data_coverage as coverage
import hourly_incremental_qa as archive
from tokyo_l2_replay_qa import audit_window, input_digests, verified_candidates


def read_report(path):
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, ValueError):
        return {}


def plan_windows(ready, recorded, current):
    # Prioritize the notified target / latest due window, then one bounded unit of catch-up.
    backlog = sorted(set(ready) - set(recorded) - {current})
    return ([current] if current in ready and current not in recorded else []) + backlog[:1]


def _window_archive_revision(items, run_id, window_id, pm):
    if not items:
        return None
    if hasattr(coverage, "compute_window_archive_revision"):
        return coverage.compute_window_archive_revision(
            items, run_id=run_id, window_id=window_id, partition_minutes=pm
        )
    return None


def run(remote, config, run_id, cutover, current, state, checker, *, archive_revision=None, trigger_mode=None):
    started_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    pm = coverage.configured_partition_minutes(config)
    step = pm * 60
    candidates = verified_candidates(remote, config, run_id, cutover - step, state)
    groups = defaultdict(list)
    for item in candidates:
        groups[item["planned_start_s"]].append(item)

    report_dir = state / "qa_reports" / "protocol" / sha256(run_id.encode()).hexdigest()[:24] / f"{pm}m"
    report_dir.mkdir(parents=True, exist_ok=True)
    wid = lambda start: coverage.window_descriptor(start, pm)["window_id"]

    observed_current_rev = _window_archive_revision(groups.get(current, []), run_id, wid(current), pm)
    expected_rev = str(archive_revision or "").strip() or None
    if expected_rev is not None and observed_current_rev is not None and observed_current_rev != expected_rev:
        completed_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        return {
            "status": "revision_mismatch",
            "run_id": run_id,
            "trigger_mode": trigger_mode or "unknown",
            "current_window": wid(current),
            "expected_archive_revision": expected_rev,
            "archive_revision": observed_current_rev,
            "reused_current_result": False,
            "protocol_status": "revision_mismatch",
            "hourly_replay_window": wid(current - current % 3600),
            "hourly_btc_replay": {},
            "historical_failed_windows": [],
            "pending_ready_windows": 0,
            "started_at_utc": started_at_utc,
            "completed_at_utc": completed_at_utc,
        }

    def report_path(start):
        return report_dir / f"{archive._safe_window_filename(wid(start))}.json"

    def archived_window_passed(start):
        window = wid(start)
        path = (state / "qa_reports" / "windows" / window[:10] /
                f"{archive._safe_window_filename(window)}.window.qa.json")
        summary = read_report(path)
        return (summary.get("qa_status") == "passed" and summary.get("run_id") == run_id
                and summary.get("partition_minutes") == pm
                and summary.get("checker_commit_sha") == checker.split("/", 1)[0]
                and set(summary.get("files") or []) == {item["source_manifest"] for item in groups[start]})

    ready = [s for s in groups if cutover <= s and archived_window_passed(s)]
    current_report = {}
    reports = {}
    revisions = {}
    for start in ready:
        inputs = input_digests(groups[start])
        earlier = input_digests(groups[start - step])
        win_rev = _window_archive_revision(groups[start], run_id, wid(start), pm)
        revisions[start] = win_rev
        proof = read_report(report_path(start))
        rev_ok = (proof.get("archive_revision") in (None, win_rev)) if win_rev is not None else True
        if (proof.get("checker") == checker and proof.get("target_inputs") == inputs
                and proof.get("previous_inputs") == earlier and rev_ok
                and proof.get("status") in {"passed", "data_failed"}):
            reports[start] = proof

    reused_current_result = current in reports
    for start in plan_windows(ready, reports, current):
        replay_seconds = 660 if start % 3600 == 0 else 0
        previous = reports.get(start - step, {})
        checkpoint = previous.get("end_sequence_checkpoint") if previous.get("protocol_status") == "passed" else None
        warmup_windows = 1 if replay_seconds or not checkpoint else 0
        report = audit_window(remote, config, run_id, start, wid(start), replay_seconds, warmup_windows,
                              candidates=candidates, checkpoint=checkpoint if not warmup_windows else None)
        report.update(
            checker=checker,
            archive_revision=revisions.get(start),
            previous_inputs=input_digests(groups[start - step]),
        )
        archive._atomic_write_json(report_path(start), report)
        reports[start] = report

    current_report = reports.get(current, {})
    # Use the most recent due hourly sample for visibility even on intervening windows.
    latest_hour = current - current % 3600
    hourly = reports.get(latest_hour, {})
    historical = [wid(start) for start, report in sorted(reports.items())
                  if start != current and report["status"] == "data_failed"]
    completed_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "status": current_report.get("status", "pending"),
        "run_id": run_id,
        "trigger_mode": trigger_mode or "unknown",
        "current_window": wid(current),
        "expected_archive_revision": expected_rev,
        "archive_revision": observed_current_rev,
        "reused_current_result": reused_current_result,
        "protocol_status": current_report.get("protocol_status", "pending"),
        "hourly_replay_window": wid(latest_hour),
        "hourly_btc_replay": {ex: v["status"] for ex, v in hourly.get("replay", {}).items()},
        "historical_failed_windows": historical,
        "pending_ready_windows": len(set(ready) - reports.keys()),
        "started_at_utc": started_at_utc,
        "completed_at_utc": completed_at_utc,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--remote", required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--cutover", required=True)
    parser.add_argument("--current", required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--checker", required=True)
    parser.add_argument("--archive-revision", default=None)
    parser.add_argument("--trigger-mode", default=None)
    args = parser.parse_args()
    data = run(args.remote, json.loads(args.config.read_text()), args.run_id,
               coverage.parse_window_start_s(args.cutover), coverage.parse_window_start_s(args.current),
               args.state_dir, args.checker,
               archive_revision=args.archive_revision, trigger_mode=args.trigger_mode)
    body = json.dumps(data, indent=2)
    print(body)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as dest:
            dest.write("\n## Archive protocol QA and hourly BTC replay\n\n" + body + "\n")
    if os.environ.get("GITHUB_ENV"):
        with open(os.environ["GITHUB_ENV"], "a") as dest:
            dest.write(f"QA_PROTOCOL_STATUS={data['protocol_status']}\n")
            dest.write(f"QA_PROTOCOL_REUSED={'true' if data.get('reused_current_result') else 'false'}\n")
            dest.write(f"QA_PROTOCOL_COMPLETED_AT_UTC={data.get('completed_at_utc', '')}\n")
            replay = ",".join(f"{e}={s}" for e, s in data["hourly_btc_replay"].items()) or "pending"
            dest.write(f"QA_HOURLY_BTC_REPLAY={replay}\n")
    return 0 if data["status"] == "passed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
