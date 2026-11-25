"""Run bounded protocol validation on verified archive inputs."""
from __future__ import annotations

import argparse
import base64
from collections import defaultdict
import inspect
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import zlib

import data_coverage as coverage
import hourly_incremental_qa as archive
from tokyo_l2_replay_qa import audit_window, input_digests, verified_candidates


def read_report(path):
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, ValueError):
        return {}


def plan_windows(ready, recorded, current, *, max_catchup=1, deferred=None, catchup_first=False):
    # Prioritize the notified target / latest due window, or oldest-first chronological catch-up when enabled.
    # Windows in `deferred` (e.g. anchor_unverified awaiting a new predecessor checkpoint) are
    # skipped so they never starve subsequent ready windows.
    skip = set(recorded) | set(deferred or ())
    backlog = sorted(set(ready) - skip - {current})
    budget = max(0, int(max_catchup))
    cur = [current] if current in ready and current not in skip else []
    if catchup_first:
        return (backlog + cur)[: budget + len(cur)]
    return cur + backlog[:budget]


PROTOCOL_QA_VERSION = 2
CHECKPOINT_FORMAT_VERSION = 1


def _checkpoint_digest(checkpoint):
    norm = sorted(
        (
            str(r.get("exchange") or ""),
            str(r.get("symbol") or ""),
            list(r.get("connection") or []),
            r.get("seq"),
            bool(r.get("awaiting_delta")),
            r.get("last_wall_ns"),
            bool(r.get("book_anchored")),
            str(r.get("book_sha256") or ""),
        )
        for r in (checkpoint or ())
    )
    return sha256(json.dumps(norm, separators=(",", ":")).encode("utf-8")).hexdigest()


def _checkpoint_sidecar_path(state, run_id, pm, window_id):
    return (
        state
        / "qa_reports"
        / "checkpoints"
        / sha256(run_id.encode()).hexdigest()[:24]
        / f"{pm}m"
        / f"{archive._safe_window_filename(window_id)}.checkpoint.json"
    )


def write_checkpoint_sidecar(path, run_id, window_id, checkpoint_rows, export_state):
    books = (export_state or {}).get("books") or {}
    end_quote = (export_state or {}).get("end_signal_quote")
    streams = (export_state or {}).get("streams") or {}
    anchored_keys = {
        (row["exchange"], row["symbol"]): row
        for row in checkpoint_rows or ()
        if row.get("book_anchored")
    }
    if not anchored_keys and end_quote is None and not streams:
        if path.exists():
            path.unlink()
        return False
    encoded_books = {}
    for (ex, sym), row in sorted(anchored_keys.items()):
        entry = books.get((ex, sym))
        if not entry or entry.get("book_sha256") != row.get("book_sha256"):
            continue
        levels_blob = json.dumps(
            {"bids": entry["bids"], "asks": entry["asks"]}, separators=(",", ":"),
        ).encode("utf-8")
        encoded_books[f"{ex}|{sym}"] = {
            "book_sha256": entry["book_sha256"],
            "last_exchange_ns": entry["last_exchange_ns"],
            "levels_zlib_b64": base64.b64encode(zlib.compress(levels_blob, level=6)).decode("ascii"),
        }
    encoded_streams = {
        f"{ex}|{sym}|{ch}": int(last_ns)
        for (ex, sym, ch), last_ns in sorted(streams.items())
    }
    payload = {
        "format_version": CHECKPOINT_FORMAT_VERSION,
        "run_id": run_id,
        "window_id": window_id,
        "end_signal_quote": end_quote,
        "streams": encoded_streams,
        "books": encoded_books,
    }
    archive._atomic_write_json(path, payload)
    return True


def read_checkpoint_sidecar(path, expected_checkpoint):
    """Load and verify compressed orderbook levels; return None if any anchored market is corrupt."""
    from tokyo_l2_protocol_qa import Book

    expected_anchored = [row for row in expected_checkpoint or () if row.get("book_anchored")]
    data = read_report(path)
    if not data:
        return None if expected_anchored else ({}, None, {})
    if data.get("format_version") != CHECKPOINT_FORMAT_VERSION:
        return None
    raw_books = data.get("books")
    if not isinstance(raw_books, dict):
        return None
    decoded = {}
    for row in expected_anchored:
        key = (row["exchange"], row["symbol"])
        entry = raw_books.get(f"{key[0]}|{key[1]}")
        if not isinstance(entry, dict) or entry.get("book_sha256") != row.get("book_sha256"):
            return None
        try:
            raw_json = zlib.decompress(base64.b64decode(entry["levels_zlib_b64"].encode("ascii")))
            levels = json.loads(raw_json.decode("utf-8"))
            book = Book.from_levels(levels)
            if book.digest() != row["book_sha256"]:
                return None
        except Exception:
            return None
        decoded[key] = {
            "book_sha256": entry["book_sha256"],
            "last_exchange_ns": entry.get("last_exchange_ns") or row.get("last_exchange_ns"),
            "bids": levels["bids"],
            "asks": levels["asks"],
        }
    quote = data.get("end_signal_quote")
    decoded_streams = {}
    for skey, s_ns in (data.get("streams") or {}).items():
        parts = str(skey).split("|", 2)
        if len(parts) == 3:
            decoded_streams[(parts[0], parts[1], parts[2])] = int(s_ns)
    return decoded, quote, decoded_streams


def _reports_with_current_checkpoints(proofs, step):
    validated = {}
    for start, proof in sorted(proofs.items()):
        previous = validated.get(start - step, {})
        if proof.get("input_checkpoint_digest") == _checkpoint_digest(previous.get("end_sequence_checkpoint")):
            validated[start] = proof
    return validated


def _window_archive_revision(items, run_id, window_id, pm):
    if not items:
        return None
    if hasattr(coverage, "compute_window_archive_revision"):
        return coverage.compute_window_archive_revision(
            items, run_id=run_id, window_id=window_id, partition_minutes=pm
        )
    return None


def _summarize_protocol_diagnostics(report):
    """Extract structural per-market failure reasons (zero raw prices/sizes/payloads)."""
    if not isinstance(report, dict):
        return {
            "protocol_issues": [],
            "proven_protocol_issues": [],
            "failed_markets": [],
            "missing_anchor_sources": [],
            "unanchored_lookback_markets": [],
            "boundary_events": [],
            "archive_file_checks": [],
        }
    issues = list(
        report.get("proven_protocol_issues")
        if report.get("proven_protocol_issues") is not None
        else ((report.get("protocol") or {}).get("issues") or [])
    )
    failed_markets = sorted(
        {
            f"{item.get('exchange')}:{item.get('symbol')}"
            for item in issues
            if isinstance(item, dict) and item.get("exchange") and item.get("symbol")
        }
    )
    out = {
        "protocol_issues": issues,
        "proven_protocol_issues": issues,
        "failed_markets": failed_markets,
        "missing_anchor_sources": list(report.get("missing_anchor_sources") or []),
        "unanchored_lookback_markets": list(report.get("unanchored_lookback_markets") or []),
        "boundary_events": list(report.get("boundary_events") or []),
        "archive_file_checks": list(report.get("archive_file_checks") or []),
    }
    if report.get("error_type"):
        out["error_type"] = report.get("error_type")
    if report.get("error_stage"):
        out["error_stage"] = report.get("error_stage")
    if report.get("spec_error_type"):
        out["spec_error_type"] = report.get("spec_error_type")
    return out


MAX_ANCHOR_LOOKBACK_WINDOWS = 24


def _lookback_inputs(groups, start, step, windows):
    return input_digests(
        item for back in range(1, windows + 1) for item in groups.get(start - back * step, ())
    )


def _expected_depth_markets(config):
    from tokyo_l2_protocol_qa import symbol_key

    if not isinstance(config, dict) or not config.get("exchanges"):
        return set()
    return {
        (exchange, symbol_key(symbol))
        for exchange, symbol, channel in coverage.expected_streams(config)
        if coverage.classify_channel(exchange, channel) == coverage.CHANNEL_CATEGORY_DEPTH
    }


def run(remote, config, run_id, cutover, current, state, checker, *, archive_revision=None, trigger_mode=None, max_catchup=None, catchup_first=False):
    started_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    pm = coverage.configured_partition_minutes(config)
    step = pm * 60
    candidates = verified_candidates(
        remote, config, run_id, cutover - MAX_ANCHOR_LOOKBACK_WINDOWS * step, state,
    )
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
            "job_execution_status": "failed",
            "run_id": run_id,
            "trigger_mode": trigger_mode or "unknown",
            "current_window": wid(current),
            "current_window_ready": False,
            "expected_archive_revision": expected_rev,
            "archive_revision": observed_current_rev,
            "reused_current_result": False,
            "protocol_status": "revision_mismatch",
            "protocol_issues": [],
            "proven_protocol_issues": [],
            "failed_markets": [],
            "missing_anchor_sources": [],
            "unanchored_lookback_markets": [],
            "boundary_events": [],
            "archive_file_checks": [],
            "hourly_replay_window": wid(current - current % 3600),
            "hourly_btc_replay": {},
            "hourly_btc_replay_reasons": {},
            "checked_windows": [],
            "historical_checked_windows": [],
            "historical_passed_windows": [],
            "historical_anchor_unverified_windows": [],
            "deferred_anchor_unverified_windows": [],
            "historical_replay_init_unverified_windows": [],
            "historical_failed_windows": [],
            "pending_ready_windows": 0,
            "started_at_utc": started_at_utc,
            "completed_at_utc": completed_at_utc,
        }

    def report_path(start):
        return report_dir / f"{archive._safe_window_filename(wid(start))}.json"

    checked_windows_by_start = {}

    def archived_window_checked(start):
        wd = coverage.window_descriptor(start, pm)
        window = wd["window_id"]
        span_id = wd["window_span_id"]
        batch_folder = archive._safe_window_filename(run_id or "default")
        candidate_paths = (
            state / "qa_reports" / "windows" / batch_folder / window[:10]
            / f"{archive._safe_window_filename(span_id)}.window.qa.json",
            state / "qa_reports" / "windows" / window[:10]
            / f"{archive._safe_window_filename(window)}.window.qa.json",
        )
        summary = {}
        for path in candidate_paths:
            summary = read_report(path)
            if summary:
                break
        status = summary.get("qa_status")
        complete = status == "passed" or (
            status in {"failed", "needs_review"} and summary.get("archive_complete_in_index") is True
        )
        if not (complete and summary.get("run_id") == run_id
                and summary.get("partition_minutes") == pm
                and summary.get("checker_commit_sha") == checker.split("/", 1)[0]
                and set(summary.get("files") or []) == {item["source_manifest"] for item in groups[start]}):
            return False
        checked_windows_by_start[start] = summary
        return True

    ready = sorted(s for s in groups if cutover <= s and archived_window_checked(s))
    expected_depth_markets = _expected_depth_markets(config)

    def restrictions(start, lookback):
        result = {}
        for at in range(start - lookback * step, start + step, step):
            status = checked_windows_by_start.get(at, {})
            bad = {loss["exchange"] for loss in status.get("attributed_late_losses", ()) if loss.get("exchange")}
            if bad:
                result[str(at)] = sorted(bad)
        return result

    proofs = {}
    saved_reports = {}
    revisions = {}
    def sidecar_path(start):
        return _checkpoint_sidecar_path(state, run_id, pm, wid(start))

    sidecar_cache = {}
    new_exports = {}

    def load_sidecar(start, expected_ckpt):
        if not expected_ckpt:
            return {}, None, {}
        if not any(r.get("book_anchored") for r in expected_ckpt):
            return None, None, {}
        cached = sidecar_cache.get(start)
        if cached is not None:
            return cached
        loaded = read_checkpoint_sidecar(sidecar_path(start), expected_ckpt)
        if loaded is not None:
            sidecar_cache[start] = loaded
        return loaded

    for start in ready:
        inputs = input_digests(groups[start])
        earlier = input_digests(groups[start - step])
        win_rev = _window_archive_revision(groups[start], run_id, wid(start), pm)
        revisions[start] = win_rev
        proof = read_report(report_path(start))
        if proof.get("protocol_qa_version") != PROTOCOL_QA_VERSION:
            continue
        saved_reports[start] = proof
        lookback = proof.get("anchor_lookback_windows")
        ckpt_rows = proof.get("end_sequence_checkpoint") or []
        sidecar_ok = (not any(r.get("book_anchored") for r in ckpt_rows)) or (
            load_sidecar(start, ckpt_rows) is not None
        )
        if (
            sidecar_ok
            and proof.get("target_inputs") == inputs
            and proof.get("previous_inputs") == earlier
            and (lookback is None or proof.get("anchor_lookback_inputs") == _lookback_inputs(groups, start, step, lookback))
            and proof.get("source_restrictions", {}) == restrictions(start, lookback or 0)
            and (win_rev is None or proof.get("archive_revision") == win_rev)
        ):
            proofs[start] = proof

    validated = _reports_with_current_checkpoints(proofs, step)
    # Validated passed/data_failed reports survive benign checker commits without redundant re-runs;
    # unverified reports are re-evaluated once when checker changes, then wait on evidence/checkpoint updates.
    recorded = {
        s for s, proof in validated.items()
        if proof.get("status") in {"passed", "data_failed"}
        or (proof.get("status") == "replay_init_unverified" and proof.get("checker") == checker)
    }
    deferred = {
        s for s, proof in validated.items()
        if proof.get("status") == "anchor_unverified" and proof.get("checker") == checker
    }
    eff_max_catchup = (
        int(max_catchup)
        if max_catchup is not None
        else (4 if trigger_mode == "schedule_catchup" else 1)
    )
    checked = set()
    checked_windows = []
    target_audit_fn = (
        audit_window.side_effect
        if callable(getattr(audit_window, "side_effect", None))
        else audit_window
    )
    audit_params = inspect.signature(target_audit_fn).parameters
    supports_extended_audit = (
        "checkpoint_books" in audit_params
        or any(p.kind == inspect.Parameter.VAR_KEYWORD for p in audit_params.values())
    )
    for start in plan_windows(ready, recorded, current, max_catchup=eff_max_catchup, deferred=deferred, catchup_first=catchup_first):
        replay_seconds = 660 if start % 3600 == 0 else 0
        checkpoint = validated.get(start - step, {}).get("end_sequence_checkpoint") or []
        prev_books, prev_quote, prev_streams = (None, None, {})
        if checkpoint:
            loaded_prev = load_sidecar(start - step, checkpoint)
            if loaded_prev is not None:
                prev_books, prev_quote, prev_streams = loaded_prev
        covered_markets = {
            (row["exchange"], row["symbol"])
            for row in checkpoint
            if (prev_books is None or (row.get("book_anchored") and (row["exchange"], row["symbol"]) in prev_books))
        }
        all_markets_covered = bool(expected_depth_markets) and expected_depth_markets.issubset(covered_markets)
        need_warmup_quote = bool(replay_seconds and prev_books is not None and prev_quote is None)
        need_warmup_streams = bool(replay_seconds and prev_books is not None and not prev_streams)
        warmup_windows = 1 if (
            not all_markets_covered
            or (replay_seconds and prev_books is None)
            or need_warmup_quote
            or need_warmup_streams
        ) else 0
        lookback = warmup_windows
        if not all_markets_covered:
            lookback = max((i for i in range(1, MAX_ANCHOR_LOOKBACK_WINDOWS + 1)
                            if groups.get(start - i * step)), default=warmup_windows)
        blocked_sources = restrictions(start, lookback)
        source_options = (
            {"source_restrictions": {int(at): values for at, values in blocked_sources.items()}}
            if blocked_sources else {}
        )
        extra_audit_kwargs = {}
        export_state = {}
        if supports_extended_audit:
            ancestors = {}
            if lookback > warmup_windows:
                for back in range(2, lookback + 2):
                    anc_s = start - back * step
                    anc_ckpt = validated.get(anc_s, {}).get("end_sequence_checkpoint") or []
                    if any(r.get("book_anchored") for r in anc_ckpt):
                        loaded_anc = load_sidecar(anc_s, anc_ckpt)
                        if loaded_anc is not None:
                            ancestors[anc_s] = (anc_ckpt, loaded_anc[0], loaded_anc[1], loaded_anc[2])
            extra_audit_kwargs = {
                "checkpoint_books": prev_books,
                "checkpoint_quote": prev_quote,
                "checkpoint_streams": prev_streams or None,
                "ancestor_checkpoints": ancestors or None,
                "export_checkpoint_state": export_state,
            }
        report = audit_window(
            remote, config, run_id, start, wid(start), replay_seconds, warmup_windows,
            candidates=candidates, checkpoint=checkpoint or None, anchor_lookback_windows=lookback,
            **source_options, **extra_audit_kwargs,
        )
        report.update(
            protocol_qa_version=PROTOCOL_QA_VERSION,
            checker=checker,
            archive_revision=revisions.get(start),
            previous_inputs=input_digests(groups[start - step]),
            anchor_lookback_windows=lookback,
            anchor_lookback_inputs=_lookback_inputs(groups, start, step, lookback),
            source_restrictions=blocked_sources,
            input_checkpoint_digest=_checkpoint_digest(checkpoint),
            completed_at_utc=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
        proofs[start] = report
        new_exports[start] = export_state
        if export_state.get("books") or export_state.get("end_signal_quote") is not None or export_state.get("streams"):
            sidecar_cache[start] = (
                export_state.get("books") or {},
                export_state.get("end_signal_quote"),
                export_state.get("streams") or {},
            )
        # The target runs first. A subsequent catch-up can change the checkpoint the target used.
        validated = _reports_with_current_checkpoints(proofs, step)
        checked.add(start)
        checked_windows.append(wid(start))

    for start in sorted(saved_reports.keys() | checked):
        if start in validated:
            if start in checked:
                archive._atomic_write_json(report_path(start), validated[start])
                write_checkpoint_sidecar(
                    sidecar_path(start), run_id, wid(start),
                    validated[start].get("end_sequence_checkpoint") or [],
                    new_exports.get(start),
                )
        elif start in checked or saved_reports[start].get("status") != "pending":
            # Reconciliation reads these reports directly, so revoke stale terminal results on disk.
            archive._atomic_write_json(report_path(start), {
                "protocol_qa_version": PROTOCOL_QA_VERSION,
                "status": "pending",
                "protocol_status": "pending",
                "reason": "protocol_inputs_changed",
                "run_id": run_id,
                "window_id": wid(start),
                "archive_revision": revisions[start],
                "checker": checker,
            })
            if sidecar_path(start).exists():
                sidecar_path(start).unlink()

    all_known = {
        start: proof for start, proof in validated.items()
        if start in checked or proof.get("status") in {
            "passed", "data_failed", "anchor_unverified", "replay_init_unverified",
        }
    }
    current_report = all_known.get(current, {})
    reused_current_result = current in all_known and current not in checked
    diag = _summarize_protocol_diagnostics(current_report)
    # Use the most recent due hourly sample for visibility even on intervening windows.
    latest_hour = current - current % 3600
    hourly = all_known.get(latest_hour, {})
    historical_passed = [wid(start) for start, report in sorted(all_known.items())
                         if start != current and report.get("status") == "passed"]
    historical_unverified = [wid(start) for start, report in sorted(all_known.items())
                             if start != current and report.get("status") == "anchor_unverified"]
    historical = [wid(start) for start, report in sorted(all_known.items())
                  if start != current and report.get("status") == "data_failed"]
    historical_replay_init = [wid(start) for start, report in sorted(all_known.items())
                              if start != current and report.get("status") == "replay_init_unverified"]
    deferred_windows = [wid(start) for start, report in sorted(all_known.items())
                        if report.get("status") == "anchor_unverified"]
    completed_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    cur_status = current_report.get("status", "pending")
    terminal_quality = {"passed", "data_failed", "anchor_unverified", "replay_init_unverified"}
    any_checked_retryable = any(proofs[s].get("status") == "retryable" for s in checked if s in proofs)
    clean_exec = (
        not any_checked_retryable
        and (
            cur_status in terminal_quality
            or (cur_status == "pending" and current not in ready)
        )
    )
    out = {
        "status": cur_status,
        "job_execution_status": "ok" if clean_exec else "failed",
        "protocol_qa_version": PROTOCOL_QA_VERSION,
        "run_id": run_id,
        "trigger_mode": trigger_mode or "unknown",
        "current_window": wid(current),
        "current_window_ready": current in ready,
        "expected_archive_revision": expected_rev,
        "archive_revision": observed_current_rev,
        "reused_current_result": reused_current_result,
        "protocol_status": current_report.get("protocol_status", "pending"),
        **diag,
        "end_sequence_checkpoint_markets": len(current_report.get("end_sequence_checkpoint") or []),
        "hourly_replay_window": wid(latest_hour),
        "hourly_btc_replay": {ex: v["status"] for ex, v in hourly.get("replay", {}).items()},
        "hourly_btc_replay_reasons": {
            ex: v["reason"] for ex, v in hourly.get("replay", {}).items() if v.get("reason")
        },
        "checked_windows": checked_windows,
        "historical_checked_windows": [w for w in checked_windows if w != wid(current)],
        "historical_passed_windows": historical_passed,
        "historical_anchor_unverified_windows": historical_unverified,
        "deferred_anchor_unverified_windows": deferred_windows,
        "historical_replay_init_unverified_windows": historical_replay_init,
        "historical_failed_windows": historical,
        "pending_ready_windows": len(set(ready) - set(all_known)),
        "started_at_utc": started_at_utc,
        "completed_at_utc": completed_at_utc,
    }
    return out


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
    parser.add_argument("--max-catchup", type=int, default=None)
    parser.add_argument(
        "--catchup-first",
        action="store_true",
        help="Process backlog windows chronologically before the current window.",
    )
    parser.add_argument(
        "--exit-zero-on-clean-execution",
        action="store_true",
        help="Return exit code 0 whenever job_execution_status == 'ok', even if data quality is failed/unverified.",
    )
    args = parser.parse_args()
    data = run(args.remote, json.loads(args.config.read_text()), args.run_id,
               coverage.parse_window_start_s(args.cutover), coverage.parse_window_start_s(args.current),
               args.state_dir, args.checker,
               archive_revision=args.archive_revision, trigger_mode=args.trigger_mode,
               max_catchup=args.max_catchup, catchup_first=args.catchup_first)
    body = json.dumps(data, indent=2)
    print(body)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as dest:
            dest.write("\n## Archive protocol QA and hourly BTC replay\n\n")
            if data.get("proven_protocol_issues"):
                issue_str = "; ".join(
                    f"{x.get('exchange')}:{x.get('symbol')}={x.get('issue')}({x.get('count')})"
                    for x in data["proven_protocol_issues"]
                )
                dest.write(f"- **Proven failed markets**: `{data.get('failed_markets')}`\n")
                dest.write(f"- **Proven protocol issues**: `{issue_str}`\n")
            if data.get("unanchored_lookback_markets"):
                un_str = "; ".join(
                    f"{x.get('exchange')}:{x.get('symbol')}(conn={x.get('connection')}/e{x.get('epoch')}, deltas_checked={x.get('delta_frames_continuity_checked')}, breaks={x.get('proven_chain_breaks')})"
                    for x in data["unanchored_lookback_markets"]
                )
                dest.write(f"- **Unanchored in lookback (no snapshot in warmup)**: `{un_str}`\n")
            if data.get("missing_anchor_sources"):
                dest.write(f"- **Proven missing anchor sources (post-reset / reconnection)**: `{data['missing_anchor_sources']}`\n")
            if data.get("historical_checked_windows"):
                dest.write(f"- **Historical catch-up windows checked**: `{data['historical_checked_windows']}`\n")
            if data.get("error_type"):
                dest.write(f"- **Execution error**: `{data.get('error_type')}` at `{data.get('error_stage')}`\n")
            dest.write("\n```json\n" + body + "\n```\n")
    if os.environ.get("GITHUB_ENV"):
        with open(os.environ["GITHUB_ENV"], "a") as dest:
            dest.write(f"QA_PROTOCOL_STATUS={data['protocol_status']}\n")
            dest.write(f"QA_PROTOCOL_WINDOW_STATUS={data.get('status', 'pending')}\n")
            dest.write(f"QA_PROTOCOL_JOB_EXECUTION_STATUS={data.get('job_execution_status', 'failed')}\n")
            dest.write(f"QA_PROTOCOL_REUSED={'true' if data.get('reused_current_result') else 'false'}\n")
            dest.write(f"QA_PROTOCOL_COMPLETED_AT_UTC={data.get('completed_at_utc', '')}\n")
            replay = ",".join(f"{e}={s}" for e, s in data["hourly_btc_replay"].items()) or "pending"
            dest.write(f"QA_HOURLY_BTC_REPLAY={replay}\n")
    if args.exit_zero_on_clean_execution:
        return 0 if data.get("job_execution_status") == "ok" else 2
    return 0 if data["status"] == "passed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
