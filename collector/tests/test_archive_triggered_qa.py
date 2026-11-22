"""Comprehensive tests for Issue #20: archive-verified event-triggered GitHub QA.

Covers:
  1. Target-window priority verification ahead of 132 historical unchecked manifests.
  2. EC2 outbox preconditions: Drive index sync failure, startup stubs, incomplete windows,
     historical missing windows (independent per-window trigger), 3-tuple deduplication,
     archive_revision changes, and crash recovery from synced archive_manifest_index.json.
  3. Independent lightweight GitHub workflow_dispatch notifier: connect/total timeouts,
     429/5xx/network exponential backoff, 401/403 visible token failure without token leakage,
     non-blocking archive/collector isolation, and separate API-accepted vs QA-completed tracking.
  4. Target-window result sharing by (run_id, target_window_utc, archive_revision, checker_commit),
     duplicate delivery reuse, revision mismatch fail-closed, checker upgrade re-verification,
     and transient failure retry.
  5. Low-frequency catch-up draining missed/concurrency-replaced windows and disjoint 4-way
     reporting (not_archived, archived_pending_qa, verified_failed, verified_passed).
  6. Least-privilege systemd unit separation for the GitHub dispatch token env file.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import socket
import sys
import tempfile
import threading
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python"))

import archive_sync as arch  # noqa: E402
import data_coverage as cov  # noqa: E402
import hourly_incremental_qa as qa  # noqa: E402
import qa_dispatch as qd  # noqa: E402
from test_stage2_partitions_archive_qa import (  # noqa: E402
    EXCHANGES_4,
    _MemoryArchiveBackend,
    _make_config,
    _wire_mock_drive,
    _write_sealed_partition,
)


def _create_index_only_window_fixtures(
    root: Path,
    *,
    window_starts: list[int],
    run_id: str,
    partition_minutes: int = 15,
    exchanges: tuple[str, ...] = EXCHANGES_4,
    loss_windows: set[int] | None = None,
) -> tuple[Path, dict[str, Path], list[dict]]:
    """Build lightweight real zstd partition + manifest fixtures and a verified index quickly."""
    loss_set = loss_windows or set()
    archive_root = root / "archive_src"
    files_by_rel: dict[str, Path] = {}
    entries: list[dict] = []

    for idx, w_start in enumerate(window_starts):
        wd = cov.window_descriptor(w_start, partition_minutes)
        for ex in exchanges:
            inject_loss = (w_start in loss_set) and (ex == exchanges[0])
            dpath, mpath, mf_obj = _write_sealed_partition(
                archive_root,
                exchange=ex,
                window_start_s=w_start,
                partition_minutes=partition_minutes,
                boot_id="boot_t20",
                run_id=run_id,
                first_rx_seq=1 + idx * 100,
                extra_meta=[{"event": "LOCAL_LOSS", "dropped_frames": 3}] if inject_loss else None,
                dense=True,
            )
            rel_data = dpath.relative_to(archive_root).as_posix()
            rel_mf = mpath.relative_to(archive_root).as_posix()
            files_by_rel[rel_data] = dpath
            files_by_rel[rel_mf] = mpath
            d_bytes = dpath.stat().st_size
            m_bytes = mpath.stat().st_size
            d_sha = hashlib.sha256(dpath.read_bytes()).hexdigest()
            m_sha = hashlib.sha256(mpath.read_bytes()).hexdigest()
            entries.append(
                {
                    "exchange": ex,
                    "date_utc": wd["date_utc"],
                    "hour_utc": wd["hour_utc"],
                    "minute_utc": wd["minute_utc"],
                    "partition_minutes": partition_minutes,
                    "window_id": wd["window_id"],
                    "planned_start_wall_ns": wd["planned_start_wall_ns"],
                    "planned_end_wall_ns": wd["planned_end_wall_ns"],
                    "collection_run_id": run_id,
                    "boot_id": mf_obj["boot_id"],
                    "segment_id": mf_obj.get("segment_id"),
                    "remote_path": rel_data,
                    "remote_manifest_path": rel_mf,
                    "bytes": d_bytes,
                    "sha256": d_sha,
                    "manifest_bytes": m_bytes,
                    "manifest_sha256": m_sha,
                    "local_loss_lines": int(mf_obj.get("local_loss_lines") or 0),
                    "late_frames_rejected": int(mf_obj.get("late_frames_rejected") or 0),
                    "verified": True,
                    "remote_manifest_verified": True,
                }
            )

    index_path = root / "archive_manifest_index.json"
    index_path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "effective_run_id": run_id,
                "verified_count": len(entries),
                "entries": entries,
                "historical_loss_manifests": [],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    files_by_rel[qa.ARCHIVE_INDEX_RELPATH] = index_path
    return index_path, files_by_rel, entries


def test_target_window_verified_first_ahead_of_132_historical_backlog_files():
    """Acceptance #1: Given 132 historical unchecked files (33 windows x 4 exchanges),
    triggering a newly uploaded complete window verifies the notified target window FIRST
    and traces run_id, target_window_utc, and archive_revision in logs/summary.
    """
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        state_dir = root / "state"
        run_id = "trial15_20261121T1400Z"
        w0 = int(datetime(2026, 11, 21, 16, 15, tzinfo=timezone.utc).timestamp())
        # 33 historical windows (132 manifests) + 1 newly uploaded target window (4 manifests) = 136 total
        hist_starts = [w0 + i * 15 * 60 for i in range(33)]
        target_start = w0 + 33 * 15 * 60  # 2026-11-22T00:30Z
        all_starts = hist_starts + [target_start]
        target_wd = cov.window_descriptor(target_start, 15)
        target_wid = target_wd["window_id"]

        cfg = _make_config(partition_minutes=15, exchanges=EXCHANGES_4, stale_seconds=900)
        cfg_path = root / "config.json"
        cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

        _, files_by_rel, entries = _create_index_only_window_fixtures(
            root, window_starts=all_starts, run_id=run_id, partition_minutes=15
        )
        target_entries = [e for e in entries if e["window_id"] == target_wid]
        expected_rev = cov.compute_window_archive_revision(
            target_entries, run_id=run_id, window_id=target_wid, partition_minutes=15
        )

        download_order: list[str] = []

        def fake_dl(_remote: str, rel: str, dst: Path) -> None:
            download_order.append(rel)
            shutil.copyfile(files_by_rel[rel], dst)

        with patch.object(qa, "_run", return_value="archive_manifest_index.json\n"), \
             patch.object(qa, "_download", side_effect=fake_dl):
            res = qa.run(
                "qa:",
                cfg_path,
                state_dir,
                limit=16,
                since_date="2026-11-21",
                since_window="2026-11-21T16:15Z",
                run_id=run_id,
                target_window_utc=target_wid,
                archive_revision=expected_rev,
                trigger_mode="archive_dispatch",
                checker_commit="deadbeef1234",
                now_epoch_s=target_start + 17 * 60,
                sla_grace_minutes=10.0,
            )

        assert res["status"] == "ok"
        assert res["heartbeat_status"] == "ok"
        assert res["trigger_mode"] == "archive_dispatch"
        assert res["target_window_utc"] == target_wid
        assert res["expected_archive_revision"] == expected_rev
        assert res["observed_archive_revision"] == expected_rev
        assert res["target_revision_matched"] is True
        assert res["target_validation_error"] is None
        assert res["target_window_reused_result"] is False
        assert res["current_window_id"] == target_wid
        assert res["current_window_completed"] is True
        assert res["current_window_verification_status"] == qa.QA_STATUS_PASSED
        assert res["eligible_manifest_count"] == 136
        assert res["attempted"] == 16  # 4 target + 12 historical catch-up
        assert res["ok"] == 16
        assert res["remaining_backlog"] == 120  # 30 remaining historical windows x 4

        # Verify target window manifests were downloaded and checked FIRST before any historical backlog!
        non_index_downloads = [p for p in download_order if p != qa.ARCHIVE_INDEX_RELPATH]
        first_4_manifests = [p for p in non_index_downloads[:8] if p.endswith(".manifest.json")]
        assert len(first_4_manifests) == 4
        assert all("20261122_0030_" in p for p in first_4_manifests)

        # Traceability lines in details include batch, window, and archive_revision
        assert any(
            f"run_id={run_id}" in line
            and f"target_window_utc={target_wid}" in line
            and f"observed_archive_revision={expected_rev}" in line
            for line in res["details"]
        )
        assert [line for line in res["details"] if line.startswith("TARGET PASSED ")]
        assert len([line for line in res["details"] if line.startswith("TARGET PASSED ")]) == 4

        # Remaining 30 historical archived windows are reported as archived_pending_qa, NOT unarchived/overdue!
        assert target_wid in res["passed_windows"]
        assert len(res["passed_windows"]) == 4  # target + 3 oldest historical windows
        assert len(res["archived_pending_qa_windows"]) == 30
        assert res["not_archived_windows"] == []
        assert res["overdue_windows"] == []


def test_archive_outbox_gating_index_sync_failure_stubs_historical_gaps_dedup_and_crash_recovery():
    """Acceptance #2a & #2b:
    - Drive index sync failure prevents enqueuing.
    - Startup stub & incomplete windows never enqueue.
    - Historical missing window does NOT block a newly COMPLETE window from enqueuing.
    - (run_id, target_window_utc, archive_revision) deduplicates; revision change allows new enqueue.
    - Crash between Drive index sync and outbox write is recovered from synced archive_manifest_index.json.
    """
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        archive_root = root / "data"
        run_id = "trial15_20261121T1400Z"
        w0 = int(datetime(2026, 11, 21, 14, 0, tzinfo=timezone.utc).timestamp())
        w1 = w0 + 15 * 60  # missing historical window
        w2 = w0 + 30 * 60  # COMPLETE window
        w3 = w0 + 45 * 60  # incomplete window (only 2 exchanges)
        exchanges = set(EXCHANGES_4)

        class _FlakyIndexBackend(_MemoryArchiveBackend):
            def __init__(self) -> None:
                super().__init__()
                self.fail_index_upload = False

            def copy_file(self, local_path: Path, remote_rel_path: str, *, immutable: bool = True) -> None:
                if self.fail_index_upload and remote_rel_path == arch.ARCHIVE_MANIFEST_INDEX_RELPATH:
                    raise RuntimeError("simulated Google Drive index upload failure")
                super().copy_file(local_path, remote_rel_path, immutable=immutable)

        backend = _FlakyIndexBackend()

        # 1. W0 is a STARTUP_STUB window (starts 6m late) -> must NOT enqueue!
        for ex in EXCHANGES_4:
            _write_sealed_partition(
                archive_root,
                exchange=ex,
                window_start_s=w0,
                partition_minutes=15,
                boot_id="boot_a",
                run_id=run_id,
                is_startup_stub=True,
                offset_start_s=360,
            )
        res_stub = arch.sync_sealed_directory(
            archive_root,
            backend,
            verify_remote=True,
            now_epoch_s=w0 + 16 * 60,
            upload_ledger_snapshot=False,
            expected_hourly_exchanges=exchanges,
            partition_minutes=15,
            expected_run_id=run_id,
            cutover_window_utc="2026-11-21T14:00Z",
        )
        assert res_stub["qa_dispatch_outbox"]["enqueued"] == []

        # 2. W1 is intentionally MISSING (historical gap!), while W2 is COMPLETE.
        # First simulate Google Drive index upload failure -> MUST NOT enqueue W2!
        for ex in EXCHANGES_4:
            _write_sealed_partition(
                archive_root,
                exchange=ex,
                window_start_s=w2,
                partition_minutes=15,
                boot_id="boot_a",
                run_id=run_id,
                first_rx_seq=100,
                dense=True,
            )
        backend.fail_index_upload = True
        res_idx_fail = arch.sync_sealed_directory(
            archive_root,
            backend,
            verify_remote=True,
            now_epoch_s=w2 + 16 * 60,
            upload_ledger_snapshot=False,
            expected_hourly_exchanges=exchanges,
            partition_minutes=15,
            expected_run_id=run_id,
            cutover_window_utc="2026-11-21T14:00Z",
        )
        assert res_idx_fail["status"] == "blocked"
        assert res_idx_fail["qa_dispatch_outbox"]["index_synced"] is False
        assert res_idx_fail["qa_dispatch_outbox"]["enqueued"] == []

        # 3. Now Drive index upload succeeds! Outer archive status is STILL "blocked" because of
        # historical missing window W1, yet W2 IS COMPLETE and index synced -> MUST enqueue W2!
        backend.fail_index_upload = False
        res_w2 = arch.sync_sealed_directory(
            archive_root,
            backend,
            verify_remote=True,
            now_epoch_s=w2 + 17 * 60,
            upload_ledger_snapshot=False,
            expected_hourly_exchanges=exchanges,
            partition_minutes=15,
            expected_run_id=run_id,
            cutover_window_utc="2026-11-21T14:00Z",
        )
        assert res_w2["status"] == "blocked"  # due to missing W1!
        assert any("pending_prior_window_unverified:2026-11-21T14:15Z" in e for e in res_w2["failed_items"])
        assert res_w2["qa_dispatch_outbox"]["index_synced"] is True
        assert len(res_w2["qa_dispatch_outbox"]["enqueued"]) == 1
        enq = res_w2["qa_dispatch_outbox"]["enqueued"][0]
        assert enq["run_id"] == run_id
        assert enq["target_window_utc"] == "2026-11-21T14:30Z"
        assert enq["archive_revision"].startswith("sha256:")
        first_rev = enq["archive_revision"]

        # 4. Repeat archive sync with identical state -> deduplicated (0 new enqueues)
        res_w2_dup = arch.sync_sealed_directory(
            archive_root,
            backend,
            verify_remote=True,
            now_epoch_s=w2 + 18 * 60,
            upload_ledger_snapshot=False,
            expected_hourly_exchanges=exchanges,
            partition_minutes=15,
            expected_run_id=run_id,
            cutover_window_utc="2026-11-21T14:00Z",
        )
        assert res_w2_dup["qa_dispatch_outbox"]["enqueued"] == []
        assert len(res_w2_dup["qa_dispatch_outbox"]["already_queued"]) == 1

        # 5. Incomplete window W3 (only 2 of 4 exchanges) must NOT enqueue!
        for ex in EXCHANGES_4[:2]:
            _write_sealed_partition(
                archive_root,
                exchange=ex,
                window_start_s=w3,
                partition_minutes=15,
                boot_id="boot_a",
                run_id=run_id,
                first_rx_seq=20,
            )
        res_w3 = arch.sync_sealed_directory(
            archive_root,
            backend,
            verify_remote=True,
            now_epoch_s=w3 + 16 * 60,
            upload_ledger_snapshot=False,
            expected_hourly_exchanges=exchanges,
            partition_minutes=15,
            expected_run_id=run_id,
            cutover_window_utc="2026-11-21T14:00Z",
        )
        assert res_w3["qa_dispatch_outbox"]["enqueued"] == []
        assert any(
            s["target_window_utc"] == "2026-11-21T14:45Z"
            for s in res_w3["qa_dispatch_outbox"]["skipped_not_complete"]
        )

        # 6. Simulate crash AFTER index upload & meta_sync_state commit, BEFORE outbox row write:
        # Delete the outbox row, then run independent dispatcher -> recovers from synced index!
        with arch.ArchiveLedger(archive_root / arch.LEDGER_DB_FILENAME) as ledger:
            with ledger.conn:
                ledger.conn.execute("DELETE FROM qa_dispatch_outbox")
            assert ledger.list_qa_dispatches() == []

        sent_calls: list[dict] = []

        def ok_sender(url: str, headers: dict[str, str], body: bytes, ct: float, tt: float) -> qd.HttpDispatchResponse:
            sent_calls.append({"url": url, "headers": headers, "payload": json.loads(body.decode("utf-8"))})
            return qd.HttpDispatchResponse(status_code=204, headers={}, body="")

        disp = qd.dispatch_pending_qa(
            archive_root,
            token="ghp_test_token_only",
            now_epoch_s=w2 + 20 * 60,
            http_sender=ok_sender,
        )
        assert disp["status"] == "ok"
        assert len(disp["recovery"]["enqueued"]) == 1
        assert disp["recovery"]["enqueued"][0]["discovery_source"] == "index_recovery"
        assert disp["accepted_count"] == 1
        assert len(sent_calls) == 1
        assert sent_calls[0]["payload"]["ref"] == "main"
        assert sent_calls[0]["payload"]["inputs"] == {
            "run_id": run_id,
            "target_window_utc": "2026-11-21T14:30Z",
            "archive_revision": first_rev,
        }

        # 7. If W2's verified manifest set changes (e.g. second non-overlapping segment added -> new revision),
        # a NEW outbox record with the new archive_revision is allowed!
        with arch.ArchiveLedger(archive_root / arch.LEDGER_DB_FILENAME) as ledger:
            idx_path = archive_root / arch.ARCHIVE_MANIFEST_INDEX_RELPATH
            doc = json.loads(idx_path.read_text(encoding="utf-8"))
            for e in doc["entries"]:
                if e["window_id"] == "2026-11-21T14:30Z" and e["exchange"] == "binance_futures":
                    e["sha256"] = "a" * 64
            idx_path.write_text(json.dumps(doc, indent=2), encoding="utf-8")
            h = arch.file_hashes(idx_path)
            ledger.upsert_meta_sync(
                arch.ARCHIVE_MANIFEST_INDEX_RELPATH,
                int(h["bytes"]),
                str(h["sha256"]),
                arch._iso_utc(w2 + 21 * 60),
            )
            redisc = arch.discover_and_enqueue_qa_dispatches(
                archive_root,
                ledger,
                now_epoch_s=w2 + 21 * 60,
                run_id_filter=run_id,
                discovery_source="archive_sync",
            )
            assert len(redisc["enqueued"]) == 1
            new_rev = redisc["enqueued"][0]["archive_revision"]
            assert new_rev != first_rev
            rows = ledger.list_qa_dispatches(run_id=run_id)
            assert len(rows) == 2


def test_dispatcher_retries_timeout_429_5xx_auth_errors_and_tracks_acceptance_vs_completion():
    """Acceptance #2c:
    - Timeouts, network errors, 429 rate limits, and 5xx keep record PENDING with backoff.
    - 401/403 token failures are visibly surfaced without leaking token value.
    - Subsequent success records api_accepted_at_utc separately from qa_completed_at_utc.
    - Real TCP blackhole socket enforces connect/total timeout in default_http_sender.
    """
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        archive_root = root / "data"
        run_id = "trial15_20261121T1400Z"
        wid = "2026-11-21T15:00Z"
        rev = "sha256:" + "1" * 64
        t0 = 1_795_200_000.0
        secret_token = "github_pat_SUPER_SECRET_DO_NOT_LEAK_12345"

        with arch.ArchiveLedger(archive_root / arch.LEDGER_DB_FILENAME) as ledger:
            inserted, _ = ledger.enqueue_qa_dispatch(
                run_id=run_id,
                target_window_utc=wid,
                archive_revision=rev,
                partition_minutes=15,
                window_span_id="2026-11-21T15:00Z__2026-11-21T15:15Z_15m",
                index_sha256="2" * 64,
                index_synced_at_utc="2026-11-21T15:16:00Z",
                manifests=[],
                now_iso="2026-11-21T15:16:01Z",
            )
            assert inserted

        # 1. Timeout -> stays PENDING with backoff
        def timeout_sender(*_args):
            raise TimeoutError("timed out talking to api.github.com")

        r_to = qd.dispatch_pending_qa(
            archive_root, token=secret_token, now_epoch_s=t0,
            http_sender=timeout_sender, recover_from_index=False,
        )
        assert r_to["status"] == "retry_scheduled"
        assert r_to["retried"][0]["error_code"] == "github_timeout"
        assert r_to["outbox_counts"]["PENDING"] == 1

        # Calling immediately before backoff expires does NOT hammer GitHub
        r_wait = qd.dispatch_pending_qa(
            archive_root, token=secret_token, now_epoch_s=t0 + 5,
            http_sender=timeout_sender, recover_from_index=False,
        )
        assert r_wait["attempted_count"] == 0

        # 2. HTTP 429 rate limit with Retry-After=120
        def rate_limit_sender(*_args):
            return qd.HttpDispatchResponse(429, {"retry-after": "120"}, '{"message":"rate limit exceeded"}')

        r_429 = qd.dispatch_pending_qa(
            archive_root, token=secret_token, now_epoch_s=t0 + 40,
            http_sender=rate_limit_sender, recover_from_index=False,
        )
        assert r_429["status"] == "retry_scheduled"
        assert r_429["retried"][0]["error_code"] == "github_rate_limited"
        assert r_429["retried"][0]["http_status"] == 429

        # 3. HTTP 503 service unavailable
        def err_503_sender(*_args):
            return qd.HttpDispatchResponse(503, {}, '{"message":"Service Unavailable"}')

        r_503 = qd.dispatch_pending_qa(
            archive_root, token=secret_token, now_epoch_s=t0 + 200,
            http_sender=err_503_sender, recover_from_index=False,
        )
        assert r_503["status"] == "retry_scheduled"
        assert r_503["retried"][0]["error_code"] == "github_http_503"

        # 4. HTTP 401 bad credentials -> visible auth_error, token redacted!
        def err_401_sender(*_args):
            return qd.HttpDispatchResponse(401, {}, f'{{"message":"Bad credentials for {secret_token}"}}')

        r_401 = qd.dispatch_pending_qa(
            archive_root, token=secret_token, now_epoch_s=t0 + 500,
            http_sender=err_401_sender, recover_from_index=False,
        )
        assert r_401["status"] == "auth_error"
        assert r_401["auth_error_count"] == 1
        assert r_401["auth_errors"][0]["error_code"] == "github_token_invalid"
        assert secret_token not in json.dumps(r_401)

        # 5. Token fixed -> HTTP 204 accepted! api_accepted_at_utc set, qa_completed_at_utc still None.
        def ok_sender(*_args):
            return qd.HttpDispatchResponse(204, {}, "")

        r_ok = qd.dispatch_pending_qa(
            archive_root, token="new_valid_token", now_epoch_s=t0 + 1000,
            http_sender=ok_sender, recover_from_index=False,
        )
        assert r_ok["status"] == "ok"
        assert r_ok["accepted_count"] == 1
        assert r_ok["accepted"][0]["api_accepted_at_utc"] is not None
        assert r_ok["accepted"][0]["qa_completed_at_utc"] is None
        assert r_ok["outbox_counts"] == {"PENDING": 0, "ACCEPTED": 1, "QA_COMPLETED": 0}

        # 6. Record QA completion separately
        comp = qd.record_qa_completion(
            archive_root,
            run_id=run_id,
            target_window_utc=wid,
            archive_revision=rev,
            qa_result_status="passed",
            qa_completed_at_utc="2026-11-21T15:19:45Z",
            workflow_run_id=987654321,
            workflow_run_url="https://github.com/Huhabubu/mm/actions/runs/987654321",
            workflow_run_started_at_utc="2026-11-21T15:17:10Z",
        )
        assert comp["updated"] is True
        assert comp["record"]["state"] == "QA_COMPLETED"
        assert comp["record"]["api_accepted_at_utc"] is not None
        assert comp["record"]["qa_completed_at_utc"] == "2026-11-21T15:19:45Z"
        assert comp["record"]["workflow_run_id"] == 987654321

        # 7. Real socket timeout test on default_http_sender
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.bind(("127.0.0.1", 0))
        srv.listen(1)
        port = srv.getsockname()[1]
        accepted_socks: list[socket.socket] = []

        def hold_connection():
            try:
                conn, _ = srv.accept()
                accepted_socks.append(conn)
                conn.recv(4096)
                # Intentionally never reply to trigger total_timeout_s
            except OSError:
                pass

        t = threading.Thread(target=hold_connection, daemon=True)
        t.start()
        try:
            with pytest.raises((TimeoutError, socket.timeout)):
                qd.default_http_sender(
                    f"http://127.0.0.1:{port}/dispatches",
                    {"Content-Type": "application/json"},
                    b"{}",
                    connect_timeout_s=0.3,
                    total_timeout_s=0.4,
                )
        finally:
            for s in accepted_socks:
                s.close()
            srv.close()


def test_duplicate_dispatch_reuses_result_revision_mismatch_blocks_and_transient_retries():
    """Acceptance #2d:
    - Duplicate dispatch for same (run_id, target_window_utc, archive_revision, checker_commit)
      reuses persisted result with 0 data/manifest downloads.
    - Mismatched archive_revision fails closed (never false pass).
    - New checker_commit re-verifies target window.
    - Transient download failure does not persist terminal pass and retries cleanly.
    """
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        state_dir = root / "state"
        run_id = "trial15_20261121T1400Z"
        w0 = int(datetime(2026, 11, 21, 16, 0, tzinfo=timezone.utc).timestamp())
        wid = "2026-11-21T16:00Z"
        cfg = _make_config(partition_minutes=15, exchanges=EXCHANGES_4, stale_seconds=900)
        cfg_path = root / "config.json"
        cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

        _, files_by_rel, entries = _create_index_only_window_fixtures(
            root, window_starts=[w0], run_id=run_id, partition_minutes=15
        )
        rev = cov.compute_window_archive_revision(entries, run_id=run_id, window_id=wid, partition_minutes=15)

        # 1. Transient failure on first data download -> blocked, retry_needed=1, not terminal passed!
        fail_once = {"done": False}

        def flaky_dl(_remote: str, rel: str, dst: Path) -> None:
            if rel.endswith(".tsv.zst") and not fail_once["done"]:
                fail_once["done"] = True
                raise OSError("simulated transient network glitch")
            shutil.copyfile(files_by_rel[rel], dst)

        with patch.object(qa, "_run", return_value="archive_manifest_index.json\n"), \
             patch.object(qa, "_download", side_effect=flaky_dl):
            r_transient = qa.run(
                "qa:", cfg_path, state_dir, limit=16, since_date="2026-11-21",
                since_window=wid, run_id=run_id, target_window_utc=wid,
                archive_revision=rev, checker_commit="commit_v1",
                now_epoch_s=w0 + 16 * 60, download_retries=1, sleeper=lambda _: None,
            )
        assert r_transient["status"] == "blocked"
        assert r_transient["retry_needed"] == 1
        assert r_transient["target_window_reused_result"] is False

        # 2. Retry succeeds and writes terminal passed report
        dl_calls: list[str] = []

        def ok_dl(_remote: str, rel: str, dst: Path) -> None:
            dl_calls.append(rel)
            shutil.copyfile(files_by_rel[rel], dst)

        with patch.object(qa, "_run", return_value="archive_manifest_index.json\n"), \
             patch.object(qa, "_download", side_effect=ok_dl):
            r_pass = qa.run(
                "qa:", cfg_path, state_dir, limit=16, since_date="2026-11-21",
                since_window=wid, run_id=run_id, target_window_utc=wid,
                archive_revision=rev, checker_commit="commit_v1",
                now_epoch_s=w0 + 17 * 60,
            )
        assert r_pass["status"] == "ok"
        assert r_pass["target_window_reused_result"] is False

        # 3. Duplicate notification with same (run_id, wid, rev, commit_v1) -> reuses result, 0 partition downloads!
        dl_calls.clear()
        with patch.object(qa, "_run", return_value="archive_manifest_index.json\n"), \
             patch.object(qa, "_download", side_effect=ok_dl):
            r_dup = qa.run(
                "qa:", cfg_path, state_dir, limit=16, since_date="2026-11-21",
                since_window=wid, run_id=run_id, target_window_utc=wid,
                archive_revision=rev, checker_commit="commit_v1",
                now_epoch_s=w0 + 18 * 60,
            )
        assert r_dup["status"] == "ok"
        assert r_dup["target_window_reused_result"] is True
        assert r_dup["attempted"] == 0
        assert dl_calls == [qa.ARCHIVE_INDEX_RELPATH]

        # 4. Wrong archive_revision fails closed (never false pass)
        with patch.object(qa, "_run", return_value="archive_manifest_index.json\n"), \
             patch.object(qa, "_download", side_effect=ok_dl):
            r_bad_rev = qa.run(
                "qa:", cfg_path, state_dir, limit=16, since_date="2026-11-21",
                since_window=wid, run_id=run_id, target_window_utc=wid,
                archive_revision="sha256:" + "0" * 64, checker_commit="commit_v1",
                now_epoch_s=w0 + 19 * 60,
            )
        assert r_bad_rev["status"] == "blocked"
        assert r_bad_rev["target_revision_matched"] is False
        assert r_bad_rev["target_validation_error"].startswith("target_archive_revision_mismatch:")

        # 5. New QA checker commit (commit_v2) invalidates target reuse and re-verifies
        dl_calls.clear()
        with patch.object(qa, "_run", return_value="archive_manifest_index.json\n"), \
             patch.object(qa, "_download", side_effect=ok_dl):
            r_v2 = qa.run(
                "qa:", cfg_path, state_dir, limit=16, since_date="2026-11-21",
                since_window=wid, run_id=run_id, target_window_utc=wid,
                archive_revision=rev, checker_commit="commit_v2",
                now_epoch_s=w0 + 20 * 60,
            )
        assert r_v2["status"] == "ok"
        assert r_v2["target_window_reused_result"] is False
        assert r_v2["attempted"] == 4


def test_low_frequency_catchup_drains_missed_windows_and_reports_four_way_classification():
    """Acceptance #3:
    Low-frequency catch-up recovers missed/concurrency-replaced windows and separately reports:
      1. not_archived (未归档)
      2. archived_pending_qa (已归档待验)
      3. verified_failed (已验失败)
      4. verified_passed (已验通过)
    """
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        state_dir = root / "state"
        run_id = "trial15_20261121T1400Z"
        w0 = int(datetime(2026, 11, 21, 14, 0, tzinfo=timezone.utc).timestamp())
        w1 = w0 + 15 * 60
        w2 = w0 + 30 * 60  # NOT archived at all!
        w3 = w0 + 45 * 60  # Archived, missed/replaced dispatch
        w4 = w0 + 60 * 60  # Archived, missed/replaced dispatch

        cfg = _make_config(partition_minutes=15, exchanges=EXCHANGES_4, stale_seconds=900)
        cfg_path = root / "config.json"
        cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

        # Archive W0 (healthy), W1 (loss -> verified_failed), W3 (healthy), W4 (healthy). W2 is NOT archived!
        _, files_by_rel, _ = _create_index_only_window_fixtures(
            root,
            window_starts=[w0, w1, w3, w4],
            run_id=run_id,
            partition_minutes=15,
            loss_windows={w1},
        )

        def fake_dl(_remote: str, rel: str, dst: Path) -> None:
            shutil.copyfile(files_by_rel[rel], dst)

        now_s = w4 + 26 * 60  # 15:26Z: W0..W4 are complete & past 10m SLA, 15:15Z is still in progress

        # Pass 1: limit=12 checks W0 (passed), W1 (failed), W3 (passed) -> leaves W4 in archived_pending_qa!
        with patch.object(qa, "_run", return_value="archive_manifest_index.json\n"), \
             patch.object(qa, "_download", side_effect=fake_dl):
            r1 = qa.run(
                "qa:", cfg_path, state_dir, limit=12, since_date="2026-11-21",
                since_window="2026-11-21T14:00Z", run_id=run_id,
                trigger_mode="schedule_catchup", checker_commit="catchup_sha",
                now_epoch_s=now_s, sla_grace_minutes=10.0,
            )

        wc1 = r1["window_classification"]
        assert wc1["not_archived"] == ["2026-11-21T14:30Z"]
        assert wc1["archived_pending_qa"] == ["2026-11-21T15:00Z"]
        assert wc1["verified_failed"] == ["2026-11-21T14:15Z"]
        assert wc1["verified_passed"] == ["2026-11-21T14:00Z", "2026-11-21T14:45Z"]
        assert r1["latest_verified_window_id"] == "2026-11-21T14:45Z"
        assert r1["latest_verified_window_status"] == qa.QA_STATUS_PASSED
        # W4 is still archived_pending_qa -> current due window not yet completed -> must not false-green
        assert r1["current_window_id"] == "2026-11-21T15:00Z"
        assert r1["current_window_verification_status"] == qa.QA_STATUS_ARCHIVED_PENDING_QA
        assert r1["status"] == "blocked"

        # Pass 2: low-frequency catch-up drains remaining missed window W4!
        with patch.object(qa, "_run", return_value="archive_manifest_index.json\n"), \
             patch.object(qa, "_download", side_effect=fake_dl):
            r2 = qa.run(
                "qa:", cfg_path, state_dir, limit=12, since_date="2026-11-21",
                since_window="2026-11-21T14:00Z", run_id=run_id,
                trigger_mode="schedule_catchup", checker_commit="catchup_sha",
                now_epoch_s=now_s, sla_grace_minutes=10.0,
            )

        wc2 = r2["window_classification"]
        assert wc2["not_archived"] == ["2026-11-21T14:30Z"]
        assert wc2["archived_pending_qa"] == []
        assert wc2["verified_failed"] == ["2026-11-21T14:15Z"]
        assert wc2["verified_passed"] == ["2026-11-21T14:00Z", "2026-11-21T14:45Z", "2026-11-21T15:00Z"]
        assert r2["current_window_id"] == "2026-11-21T15:00Z"
        assert r2["current_window_completed"] is True
        assert r2["current_window_verification_status"] == qa.QA_STATUS_PASSED
        assert r2["heartbeat_status"] == "ok"
        assert r2["status"] == "ok"
        assert r2["historical_data_quality_status"] == "has_historical_damage"
        assert "2026-11-21T14:30Z" in r2["historical_overdue_windows"]


def test_systemd_units_isolate_github_dispatch_token_to_notifier_only():
    """Verify least-privilege separation: ONLY tokyo-l2-qa-dispatch.service reads github_qa_dispatch.env."""
    systemd_dir = Path(__file__).resolve().parents[1] / "deploy" / "systemd"
    dispatch_unit = (systemd_dir / "tokyo-l2-qa-dispatch.service").read_text(encoding="utf-8")
    collector_unit = (systemd_dir / "tokyo-l2-collector.service").read_text(encoding="utf-8")
    archive_unit = (systemd_dir / "tokyo-l2-archive.service").read_text(encoding="utf-8")
    meta_unit = (systemd_dir / "tokyo-l2-meta.service").read_text(encoding="utf-8")

    assert "github_qa_dispatch.env" in dispatch_unit
    assert "github_qa_dispatch.env" not in collector_unit
    assert "github_qa_dispatch.env" not in archive_unit
    assert "github_qa_dispatch.env" not in meta_unit


def test_index_swap_toctou_cannot_enqueue_unsynced_window():
    """Review P1:
    discover_and_enqueue_qa_dispatches must read meta/archive_manifest_index.json ONCE,
    verify meta_sync_state against that exact byte snapshot, and parse JSON from the same
    bytes so a concurrent index replacement cannot enqueue an unsynced new window.
    """
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        archive_root = root / "sealed"
        run_id = "trial15_20261121T1400Z"
        w0 = int(datetime(2026, 11, 21, 14, 0, tzinfo=timezone.utc).timestamp())
        w1 = w0 + 15 * 60
        backend = _MemoryArchiveBackend()

        # Sync W0 cleanly so meta/archive_manifest_index.json and meta_sync_state match W0.
        for ex in EXCHANGES_4:
            _write_sealed_partition(
                archive_root,
                exchange=ex,
                window_start_s=w0,
                partition_minutes=15,
                boot_id="boot_toctou",
                run_id=run_id,
                dense=True,
            )
        r0 = arch.sync_sealed_directory(
            archive_root,
            backend=backend,
            verify_remote=True,
            expected_hourly_exchanges=set(EXCHANGES_4),
            partition_minutes=15,
            expected_run_id=run_id,
            now_epoch_s=w0 + 16 * 60,
        )
        assert r0["status"] == "ok"

        index_path = archive_root / "meta" / "archive_manifest_index.json"
        synced_obj = json.loads(index_path.read_text(encoding="utf-8"))
        # Craft a newer unsynced index containing W1 (NOT uploaded to Drive, meta_sync_state still on W0!).
        w1_wd = cov.window_descriptor(w1, 15)
        unsynced_obj = json.loads(json.dumps(synced_obj))
        unsynced_obj["window_ledger"].append(
            {
                "window_id": w1_wd["window_id"],
                "window_span_id": w1_wd["window_span_id"],
                "partition_minutes": 15,
                "collection_run_id": run_id,
                "status": "COMPLETE",
                "file_verification_status": "verified",
                "window_coverage_status": "complete",
                "archive_revision": "sha256:" + "f" * 64,
                "present_exchanges": list(EXCHANGES_4),
                "missing_exchanges": [],
                "manifests": ["raw/binance/unsynced_w1.manifest.json"],
            }
        )
        unsynced_bytes = json.dumps(unsynced_obj, indent=2, sort_keys=True).encode("utf-8")

        with arch.ArchiveLedger(archive_root / arch.LEDGER_DB_FILENAME) as ledger:
            # Clear existing outbox so W0 would be re-enqueued from the synced snapshot.
            with ledger.conn:
                ledger.conn.execute("DELETE FROM qa_dispatch_outbox")

            orig_get_meta_sync = ledger.get_meta_sync

            def racing_get_meta_sync(rel_meta_path: str):
                # Reproduce exact TOCTOU: archive replaces index file on disk right after
                # hash calculation / during meta_sync_state check!
                rec = orig_get_meta_sync(rel_meta_path)
                index_path.write_bytes(unsynced_bytes)
                return rec

            with patch.object(ledger, "get_meta_sync", side_effect=racing_get_meta_sync):
                res = arch.discover_and_enqueue_qa_dispatches(
                    archive_root,
                    ledger,
                    now_epoch_s=w1 + 16 * 60,
                    run_id_filter=run_id,
                    discovery_source="index_recovery",
                )

            assert res["index_synced"] is True
            enqueued_windows = [x["target_window_utc"] for x in res["enqueued"]]
            assert enqueued_windows == ["2026-11-21T14:00Z"]
            rows = ledger.list_qa_dispatches(run_id=run_id)
            assert [r["target_window_utc"] for r in rows] == ["2026-11-21T14:00Z"]

            # Next call sees the unsynced index on disk whose hash does NOT match meta_sync_state -> refused!
            res2 = arch.discover_and_enqueue_qa_dispatches(
                archive_root,
                ledger,
                now_epoch_s=w1 + 16 * 60 + 5,
                run_id_filter=run_id,
                discovery_source="index_recovery",
            )
            assert res2["index_synced"] is False
            assert res2["enqueued"] == []
            assert [r["target_window_utc"] for r in ledger.list_qa_dispatches(run_id=run_id)] == [
                "2026-11-21T14:00Z"
            ]


def test_outbox_write_exception_does_not_abort_archive_sync():
    """Review P1:
    An exception while writing qa_dispatch_outbox must be reported separately under
    qa_dispatch_outbox['error'] without aborting archive finalization or ledger snapshot upload.
    """
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        archive_root = root / "sealed"
        run_id = "trial15_20261121T1400Z"
        w0 = int(datetime(2026, 11, 21, 14, 0, tzinfo=timezone.utc).timestamp())
        backend = _MemoryArchiveBackend()

        for ex in EXCHANGES_4:
            _write_sealed_partition(
                archive_root,
                exchange=ex,
                window_start_s=w0,
                partition_minutes=15,
                boot_id="boot_outbox_err",
                run_id=run_id,
                dense=True,
            )

        with patch.object(
            arch,
            "discover_and_enqueue_qa_dispatches",
            side_effect=RuntimeError("simulated sqlite busy on outbox"),
        ):
            res = arch.sync_sealed_directory(
                archive_root,
                backend=backend,
                verify_remote=True,
                upload_ledger_snapshot=True,
                expected_hourly_exchanges=set(EXCHANGES_4),
                partition_minutes=15,
                expected_run_id=run_id,
                now_epoch_s=w0 + 16 * 60,
            )

        assert res["status"] == "ok"
        assert res["verified_count"] == 4
        assert res["snapshot_remote_path"] is not None
        assert res["snapshot_sha256"] is not None
        assert res["qa_dispatch_outbox"]["reason"] == "outbox_enqueue_error:RuntimeError"
        assert "simulated sqlite busy on outbox" in res["qa_dispatch_outbox"]["error"]


def test_historical_download_failure_does_not_block_target_window_protocol_qa():
    """Review P1:
    When the target window passes all 4 exchanges and a subsequent historical catch-up file
    fails to download transiently, target_window_execution_status must stay 'ok' and
    active_window_ready_for_protocol_qa must stay True so protocol/replay QA still runs,
    while historical_catchup_status reports 'retry_needed'. Conversely, a transient failure on
    the target window itself must keep active_window_ready_for_protocol_qa False.
    """
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        state_dir = root / "state"
        run_id = "trial15_20261121T1400Z"
        w_hist = int(datetime(2026, 11, 21, 14, 0, tzinfo=timezone.utc).timestamp())
        w_target = int(datetime(2026, 11, 21, 14, 15, tzinfo=timezone.utc).timestamp())
        target_wid = "2026-11-21T14:15Z"
        cfg = _make_config(partition_minutes=15, exchanges=EXCHANGES_4, stale_seconds=900)
        cfg_path = root / "config.json"
        cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

        _, files_by_rel, entries = _create_index_only_window_fixtures(
            root, window_starts=[w_hist, w_target], run_id=run_id, partition_minutes=15
        )
        target_entries = [e for e in entries if e["window_id"] == target_wid]
        target_rev = cov.compute_window_archive_revision(
            target_entries, run_id=run_id, window_id=target_wid, partition_minutes=15
        )

        # Case A: target window downloads cleanly, historical window 14:00Z fails transiently.
        def hist_failing_dl(_remote: str, rel: str, dst: Path) -> None:
            if "_1400_" in rel and rel.endswith(".tsv.zst"):
                raise OSError("simulated rclone transient timeout on historical file")
            shutil.copyfile(files_by_rel[rel], dst)

        with patch.object(qa, "_run", return_value="archive_manifest_index.json\n"), \
             patch.object(qa, "_download", side_effect=hist_failing_dl):
            res_ok = qa.run(
                "qa:",
                cfg_path,
                state_dir,
                limit=16,
                since_date="2026-11-21",
                since_window="2026-11-21T14:00Z",
                run_id=run_id,
                target_window_utc=target_wid,
                archive_revision=target_rev,
                trigger_mode="event_dispatch",
                checker_commit="commit_p1_sep",
                now_epoch_s=w_target + 16 * 60,
                download_retries=1,
                sleeper=lambda _: None,
            )

        assert res_ok["status"] == "ok"
        assert res_ok["heartbeat_status"] == "ok"
        assert res_ok["target_window_execution_status"] == "ok"
        assert res_ok["active_window_ready_for_protocol_qa"] is True
        assert res_ok["target_retry_needed"] == 0
        assert res_ok["historical_retry_needed"] >= 1
        assert res_ok["historical_catchup_status"] == "retry_needed"
        assert res_ok["window_classification"]["verified_passed"] == [target_wid]
        assert "2026-11-21T14:00Z" in res_ok["window_classification"]["archived_pending_qa"]
        assert res_ok["window_classification"]["verified_failed"] == []

        # Case B: target window itself hits a transient download failure -> MUST block protocol QA!
        state_dir_b = root / "state_b"

        def target_failing_dl(_remote: str, rel: str, dst: Path) -> None:
            if "_1415_" in rel and rel.endswith(".tsv.zst"):
                raise OSError("simulated transient failure on target window")
            shutil.copyfile(files_by_rel[rel], dst)

        with patch.object(qa, "_run", return_value="archive_manifest_index.json\n"), \
             patch.object(qa, "_download", side_effect=target_failing_dl):
            res_blk = qa.run(
                "qa:",
                cfg_path,
                state_dir_b,
                limit=16,
                since_date="2026-11-21",
                since_window="2026-11-21T14:00Z",
                run_id=run_id,
                target_window_utc=target_wid,
                archive_revision=target_rev,
                trigger_mode="event_dispatch",
                checker_commit="commit_p1_sep",
                now_epoch_s=w_target + 16 * 60,
                download_retries=1,
                sleeper=lambda _: None,
            )

        assert res_blk["status"] == "blocked"
        assert res_blk["target_window_execution_status"] == "retry_needed"
        assert res_blk["active_window_ready_for_protocol_qa"] is False
        assert res_blk["target_retry_needed"] >= 1


def test_accepted_reconciliation_stale_requeue_and_low_freq_catchup_completion():
    """Review P2:
    - Live dispatch_pending_qa reconciles ACCEPTED records by (run_id, target_window_utc, archive_revision)
      against persisted window + protocol QA reports.
    - Mismatched archive_revision reports are rejected (never falsely complete).
    - Stale ACCEPTED records missing final reports (e.g. replaced in GitHub's concurrency queue)
      are requeued to PENDING with error_code='accepted_result_missing_or_replaced'.
    - When low-frequency catch-up subsequently completes full QA (archive + protocol) for the
      missed window, the next notifier tick writes back QA_COMPLETED without redundant dispatch.
    - GitHub Contents API fetcher automatically falls back from HTTP 403 (Actions-only PAT)
      to unauthenticated GET on public repos.
    """
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        archive_root = root / "sealed"
        state_dir = root / "qa_state"
        run_id = "trial15_20261121T1400Z"
        w0 = int(datetime(2026, 11, 21, 14, 0, tzinfo=timezone.utc).timestamp())
        w1 = w0 + 15 * 60
        wid0, wid1 = "2026-11-21T14:00Z", "2026-11-21T14:15Z"
        t0 = float(w1 + 16 * 60)

        cfg = _make_config(partition_minutes=15, exchanges=EXCHANGES_4, stale_seconds=900)
        cfg_path = root / "config.json"
        cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

        # Archive both W0 and W1 locally + in mock Drive backend so both enter outbox as PENDING.
        backend = _MemoryArchiveBackend()
        for w_start in (w0, w1):
            for ex in EXCHANGES_4:
                _write_sealed_partition(
                    archive_root,
                    exchange=ex,
                    window_start_s=w_start,
                    partition_minutes=15,
                    boot_id="boot_reconcile",
                    run_id=run_id,
                    dense=True,
                )
        sync_res = arch.sync_sealed_directory(
            archive_root,
            backend=backend,
            verify_remote=True,
            expected_hourly_exchanges=set(EXCHANGES_4),
            partition_minutes=15,
            expected_run_id=run_id,
            now_epoch_s=t0,
        )
        assert sync_res["status"] == "ok"
        assert len(sync_res["qa_dispatch_outbox"]["enqueued"]) == 2

        # Dispatch both W0 and W1 -> both transition PENDING -> ACCEPTED.
        sent_windows: list[str] = []

        def ok_sender(_url, _hdrs, body_bytes, _ct, _tt):
            payload = json.loads(body_bytes.decode("utf-8"))
            sent_windows.append(payload["inputs"]["target_window_utc"])
            return qd.HttpDispatchResponse(204, {}, "")

        r_disp = qd.dispatch_pending_qa(
            archive_root,
            token="pat_test",
            max_dispatches=2,
            now_epoch_s=t0,
            http_sender=ok_sender,
            qa_state_dir=state_dir,
            require_protocol_report=True,
            recover_from_index=False,
        )
        assert r_disp["status"] == "ok"
        assert r_disp["outbox_counts"] == {"PENDING": 0, "ACCEPTED": 2, "QA_COMPLETED": 0}
        assert set(sent_windows) == {wid0, wid1}

        with arch.ArchiveLedger(archive_root / arch.LEDGER_DB_FILENAME) as ledger:
            by_wid = {r["target_window_utc"]: r for r in ledger.list_qa_dispatches(run_id=run_id)}
        rev0 = by_wid[wid0]["archive_revision"]
        rev1 = by_wid[wid1]["archive_revision"]

        # Simulate GitHub concurrency replacement:
        # W1 runs & finishes archive QA + protocol QA; W0 was replaced in queue (only wrong-rev report exists).
        run_fn, dl_fn = _wire_mock_drive(archive_root, backend)
        with patch.object(qa, "_run", side_effect=run_fn), \
             patch.object(qa, "_download", side_effect=dl_fn):
            r_w1_qa = qa.run(
                "qa:",
                cfg_path,
                state_dir,
                limit=4,  # Only enough budget for target window W1!
                since_date="2026-11-21",
                since_window=wid0,
                run_id=run_id,
                target_window_utc=wid1,
                archive_revision=rev1,
                trigger_mode="event_dispatch",
                checker_commit="commit_p2",
                now_epoch_s=t0 + 60,
            )
        assert r_w1_qa["status"] == "ok"
        assert r_w1_qa["active_window_ready_for_protocol_qa"] is True

        # Before protocol report is written, require_protocol_report=True keeps W1 in_flight!
        r_mid = qd.dispatch_pending_qa(
            archive_root,
            token="pat_test",
            max_dispatches=0,
            now_epoch_s=t0 + 120,
            http_sender=ok_sender,
            qa_state_dir=state_dir,
            accepted_stale_timeout_s=1800,
            require_protocol_report=True,
            recover_from_index=False,
        )
        assert r_mid["reconciliation"]["completed_count"] == 0
        assert r_mid["reconciliation"]["in_flight_count"] == 2

        # Write matching terminal protocol report for W1, and a WRONG-revision window report for W0.
        proto_w1_path = state_dir / qd._protocol_report_rel_path(by_wid[wid1])
        proto_w1_path.parent.mkdir(parents=True, exist_ok=True)
        proto_w1_path.write_text(
            json.dumps(
                {
                    "status": "passed",
                    "protocol_status": "passed",
                    "run_id": run_id,
                    "window_id": wid1,
                    "archive_revision": rev1,
                    "checker": "commit_p2/rules_v1",
                    "completed_at_utc": "2026-11-21T14:33:30Z",
                }
            ),
            encoding="utf-8",
        )
        wrong_w0_path = state_dir / qd._window_report_rel_paths(by_wid[wid0])[0]
        wrong_w0_path.parent.mkdir(parents=True, exist_ok=True)
        wrong_w0_path.write_text(
            json.dumps(
                {
                    "run_id": run_id,
                    "window_id": wid0,
                    "archive_revision": "sha256:" + "0" * 64,
                    "checker_commit_sha": "commit_p2",
                    "qa_status": "passed",
                }
            ),
            encoding="utf-8",
        )

        # Past accepted_stale_timeout_s (2000s > 1800s):
        # W1 -> QA_COMPLETED; W0 (wrong revision ignored & stale) -> requeued to PENDING!
        r_rec = qd.dispatch_pending_qa(
            archive_root,
            token="pat_test",
            max_dispatches=0,
            now_epoch_s=t0 + 2000,
            http_sender=ok_sender,
            qa_state_dir=state_dir,
            accepted_stale_timeout_s=1800,
            require_protocol_report=True,
            expected_protocol_checker="commit_p2/rules_v1",
            recover_from_index=False,
        )
        assert r_rec["reconciliation"]["completed_count"] == 1
        assert r_rec["reconciliation"]["completed"][0]["target_window_utc"] == wid1
        assert r_rec["reconciliation"]["requeued_stale_count"] == 1
        assert r_rec["reconciliation"]["requeued_stale"][0]["target_window_utc"] == wid0
        assert r_rec["reconciliation"]["requeued_stale"][0]["error_code"] == "accepted_result_missing_or_replaced"
        assert r_rec["outbox_counts"] == {"PENDING": 1, "ACCEPTED": 0, "QA_COMPLETED": 1}

        with arch.ArchiveLedger(archive_root / arch.LEDGER_DB_FILENAME) as ledger:
            rec_w1 = ledger.get_qa_dispatch(run_id=run_id, target_window_utc=wid1, archive_revision=rev1)
            rec_w0 = ledger.get_qa_dispatch(run_id=run_id, target_window_utc=wid0, archive_revision=rev0)
        assert rec_w1["state"] == "QA_COMPLETED"
        assert rec_w1["qa_result_status"] == "passed"
        assert rec_w1["qa_completed_at_utc"] == "2026-11-21T14:33:30Z"
        assert rec_w1["api_accepted_at_utc"] != rec_w1["qa_completed_at_utc"]
        assert rec_w0["state"] == "PENDING"
        assert rec_w0["last_error_code"] == "accepted_result_missing_or_replaced"

        # Now simulate the low-frequency catch-up run picking up missed W0 and completing full QA
        # (archive QA + protocol report persisted to state_dir).
        with patch.object(qa, "_run", side_effect=run_fn), \
             patch.object(qa, "_download", side_effect=dl_fn):
            r_catchup = qa.run(
                "qa:",
                cfg_path,
                state_dir,
                limit=16,
                since_date="2026-11-21",
                since_window=wid0,
                run_id=run_id,
                target_window_utc=None,
                trigger_mode="schedule_catchup",
                checker_commit="commit_p2",
                now_epoch_s=t0 + 120,
            )
        assert r_catchup["status"] == "ok"
        assert sorted(r_catchup["window_classification"]["verified_passed"]) == [wid0, wid1]
        proto_w0_path = state_dir / qd._protocol_report_rel_path(by_wid[wid0])
        proto_w0_path.parent.mkdir(parents=True, exist_ok=True)
        proto_w0_path.write_text(
            json.dumps(
                {
                    "status": "passed",
                    "protocol_status": "passed",
                    "run_id": run_id,
                    "window_id": wid0,
                    "archive_revision": rev0,
                    "checker": "commit_p2/rules_v1",
                    "completed_at_utc": "2026-11-21T15:14:10Z",
                }
            ),
            encoding="utf-8",
        )

        # Next notifier tick reconciles the requeued PENDING W0 directly to QA_COMPLETED with 0 redundant dispatches!
        sent_windows.clear()
        r_final = qd.dispatch_pending_qa(
            archive_root,
            token="pat_test",
            max_dispatches=2,
            now_epoch_s=t0 + 2600,
            http_sender=ok_sender,
            qa_state_dir=state_dir,
            accepted_stale_timeout_s=1800,
            require_protocol_report=True,
            expected_protocol_checker="commit_p2/rules_v1",
            recover_from_index=False,
        )
        assert r_final["reconciliation"]["completed_count"] == 1
        assert r_final["reconciliation"]["completed"][0]["target_window_utc"] == wid0
        assert r_final["reconciliation"]["completed"][0]["reconciled_from_state"] == "PENDING"
        assert r_final["attempted_count"] == 0
        assert sent_windows == []
        assert r_final["outbox_counts"] == {"PENDING": 0, "ACCEPTED": 0, "QA_COMPLETED": 2}

        # Verify GitHub Contents API fetcher:
        # - Automatically resolves live (checker_commit_sha, rules_sha) from source_ref + workflow_ref
        # - Handles Actions-only PAT HTTP 403 -> unauthenticated fallback -> 200
        # - Rejects two mutually consistent reports from an OLDER checker/rules version!
        script_bodies = {
            "scripts/tokyo_l2_protocol_qa.py": b"# protocol rules v2\n",
            "scripts/tokyo_l2_replay_qa.py": b"# replay rules v2\n",
            "scripts/tokyo_l2_recurring_qa.py": b"# recurring rules v2\n",
        }
        live_checker_sha = "c" * 40
        expected_rules_sha = qd.compute_protocol_rules_sha(script_bodies)
        get_calls: list[tuple[str, bool]] = []
        serve_stale_pair = {"flag": True}

        def fake_github_get(url: str, hdrs: dict[str, str], _ct: float, _tt: float):
            has_auth = "Authorization" in hdrs
            get_calls.append((url, has_auth))
            if has_auth:
                return qd.HttpDispatchResponse(403, {}, '{"message":"Resource not accessible by personal access token"}')
            if "/commits/" in url:
                assert hdrs["Accept"] == "application/vnd.github.sha"
                return qd.HttpDispatchResponse(200, {}, live_checker_sha)
            for rel, raw in script_bodies.items():
                if f"/contents/{rel}?ref=main" in url:
                    return qd.HttpDispatchResponse(200, {}, raw.decode("utf-8"))
            if url.endswith(".window.qa.json?ref=tokyo-l2-qa-state"):
                chk = "commit_old" if serve_stale_pair["flag"] else live_checker_sha
                return qd.HttpDispatchResponse(
                    200,
                    {},
                    json.dumps(
                        {
                            "run_id": run_id,
                            "window_id": wid1,
                            "archive_revision": rev1,
                            "qa_status": "passed",
                            "checker_commit_sha": chk,
                        }
                    ),
                )
            if "/qa_reports/protocol/" in url:
                chk = "commit_old/old_rules" if serve_stale_pair["flag"] else f"{live_checker_sha}/{expected_rules_sha}"
                return qd.HttpDispatchResponse(
                    200,
                    {},
                    json.dumps(
                        {
                            "status": "passed" if serve_stale_pair["flag"] else "data_failed",
                            "protocol_status": "passed" if serve_stale_pair["flag"] else "failed",
                            "run_id": run_id,
                            "window_id": wid1,
                            "archive_revision": rev1,
                            "checker": chk,
                            "completed_at_utc": "2026-11-21T14:35:00Z",
                        }
                    ),
                )
            return qd.HttpDispatchResponse(404, {}, "{}")

        gh_fetcher = qd.make_github_result_fetcher(
            repo="Huhabubu/mm",
            state_branch="tokyo-l2-qa-state",
            token="actions_only_pat",
            http_get=fake_github_get,
            require_protocol_report=True,
        )
        # Even though both old window + old protocol reports agree on commit_old, auto-resolved
        # live checker binding MUST reject them!
        assert gh_fetcher(by_wid[wid1]) is None

        # Once reports match the live resolved commit and rules digest, reconciliation accepts!
        serve_stale_pair["flag"] = False
        verdict = gh_fetcher(by_wid[wid1])
        assert verdict is not None
        assert verdict["qa_result_status"] == "protocol_failed"
        assert verdict["protocol_checker"] == f"{live_checker_sha}/{expected_rules_sha}"
        assert verdict["qa_completed_at_utc"] == "2026-11-21T14:35:00Z"
        assert any(auth for _, auth in get_calls)
        assert any(not auth for _, auth in get_calls)


def test_checker_resolution_rejects_a_truncated_full_commit_response():
    # Simulate the former 256 KiB read limit on a commit with a large diff.
    full_commit = json.dumps({"sha": "a" * 40, "files": [{"patch": "x" * 300_000}]})
    truncated = full_commit[:262_144]

    def fake_get(url, headers, _connect_timeout, _total_timeout):
        assert url.endswith("/commits/feat%2Ftokyo-l2-reliability-fixes")
        assert headers["Accept"] == "application/vnd.github.sha"
        return qd.HttpDispatchResponse(200, {}, truncated)

    with pytest.raises(RuntimeError, match="Invalid checker commit SHA"):
        qd.resolve_expected_checker_from_github(
            repo="Huhabubu/mm", http_get=fake_get, include_protocol_rules=False
        )


def test_reconciliation_rejects_missing_revision_and_stale_checker_protocol_reports():
    """Review Round 3 #1 & Round 4 P2:
    Reconciliation MUST fail closed and MUST NOT prematurely mark a record completed when:
      0. Expected checker commit / protocol rules version is left unbound (even if both reports match each other!),
      1. Protocol report omits archive_revision (None or ''),
      2. Protocol report omits checker or comes from an older checker commit than window_report,
      3. Window/protocol reports come from an older checker commit or rules_sha than expected,
      4. Protocol report is non-terminal (e.g. 'anchor_unverified' or 'retryable').
    """
    rec = {
        "run_id": "trial15_20261121T1400Z",
        "target_window_utc": "2026-11-22T03:30Z",
        "archive_revision": "sha256:" + "a" * 64,
    }
    good_win = {
        "run_id": rec["run_id"],
        "window_id": rec["target_window_utc"],
        "archive_revision": rec["archive_revision"],
        "checker_commit_sha": "commit_new",
        "qa_status": "passed",
    }
    old_win = dict(good_win, checker_commit_sha="commit_old")
    proto_old_checker = {
        "status": "passed",
        "protocol_status": "passed",
        "run_id": rec["run_id"],
        "window_id": rec["target_window_utc"],
        "archive_revision": rec["archive_revision"],
        "checker": "commit_old/rules_v1",
    }

    # 0. Review Round 4 P2: two mutually consistent OLD reports with default empty expected checker -> MUST return None!
    assert qd.evaluate_qa_reports_for_record(
        rec, window_report=old_win, protocol_report=proto_old_checker, require_protocol_report=True
    ) is None
    assert qd.evaluate_qa_reports_for_record(
        rec, window_report=old_win, protocol_report=proto_old_checker, require_protocol_report=False
    ) is None
    # With require_protocol_report=True, binding ONLY commit without rules_sha -> MUST also return None!
    assert qd.evaluate_qa_reports_for_record(
        rec,
        window_report=good_win,
        protocol_report={
            "status": "passed",
            "protocol_status": "passed",
            "run_id": rec["run_id"],
            "window_id": rec["target_window_utc"],
            "archive_revision": rec["archive_revision"],
            "checker": "commit_new/rules_v1",
        },
        require_protocol_report=True,
        expected_checker_commit="commit_new",
    ) is None

    # 1. Missing archive_revision in protocol report -> MUST return None even with bound expected_protocol_checker!
    proto_no_rev = {
        "status": "passed",
        "protocol_status": "passed",
        "run_id": rec["run_id"],
        "window_id": rec["target_window_utc"],
        "checker": "commit_new/rules_v2",
    }
    assert qd.evaluate_qa_reports_for_record(
        rec,
        window_report=good_win,
        protocol_report=proto_no_rev,
        require_protocol_report=True,
        expected_protocol_checker="commit_new/rules_v2",
    ) is None

    # 2. Older checker commit in protocol report than window_report -> MUST return None!
    assert qd.evaluate_qa_reports_for_record(
        rec,
        window_report=good_win,
        protocol_report=proto_old_checker,
        require_protocol_report=True,
        expected_protocol_checker="commit_new/rules_v2",
    ) is None

    # 3. Missing checker in protocol report -> MUST return None!
    proto_no_checker = {
        "status": "passed",
        "protocol_status": "passed",
        "run_id": rec["run_id"],
        "window_id": rec["target_window_utc"],
        "archive_revision": rec["archive_revision"],
    }
    assert qd.evaluate_qa_reports_for_record(
        rec,
        window_report=good_win,
        protocol_report=proto_no_checker,
        require_protocol_report=True,
        expected_protocol_checker="commit_new/rules_v2",
    ) is None

    # 4. Both reports on old checker/rules when expected is newer -> MUST return None!
    assert qd.evaluate_qa_reports_for_record(
        rec,
        window_report=old_win,
        protocol_report=proto_old_checker,
        require_protocol_report=True,
        expected_checker_commit="commit_new",
        expected_protocol_checker="commit_new/rules_v2",
    ) is None
    proto_old_rules = {
        "status": "passed",
        "protocol_status": "passed",
        "run_id": rec["run_id"],
        "window_id": rec["target_window_utc"],
        "archive_revision": rec["archive_revision"],
        "checker": "commit_new/rules_v1",
    }
    assert qd.evaluate_qa_reports_for_record(
        rec,
        window_report=good_win,
        protocol_report=proto_old_rules,
        require_protocol_report=True,
        expected_protocol_checker="commit_new/rules_v2",
    ) is None

    # 5. Window report missing checker_commit_sha -> MUST return None!
    win_no_checker = dict(good_win)
    win_no_checker.pop("checker_commit_sha")
    assert qd.evaluate_qa_reports_for_record(
        rec,
        window_report=win_no_checker,
        protocol_report=proto_old_rules,
        require_protocol_report=False,
        expected_checker_commit="commit_new",
    ) is None

    # 6. Non-terminal anchor_unverified protocol report -> MUST return None!
    proto_unverified = {
        "status": "anchor_unverified",
        "protocol_status": "anchor_unverified",
        "run_id": rec["run_id"],
        "window_id": rec["target_window_utc"],
        "archive_revision": rec["archive_revision"],
        "checker": "commit_new/rules_v2",
    }
    assert qd.evaluate_qa_reports_for_record(
        rec,
        window_report=good_win,
        protocol_report=proto_unverified,
        require_protocol_report=True,
        expected_protocol_checker="commit_new/rules_v2",
    ) is None

    # 7. All four dimensions (run_id, window_id, archive_revision, bound checker+rules) match -> completes!
    proto_ok = {
        "status": "passed",
        "protocol_status": "passed",
        "run_id": rec["run_id"],
        "window_id": rec["target_window_utc"],
        "archive_revision": rec["archive_revision"],
        "checker": "commit_new/rules_v2",
        "completed_at_utc": "2026-11-22T03:49:00Z",
    }
    verdict = qd.evaluate_qa_reports_for_record(
        rec,
        window_report=good_win,
        protocol_report=proto_ok,
        require_protocol_report=True,
        expected_checker_commit="commit_new",
        expected_protocol_checker="commit_new/rules_v2",
    )
    assert verdict is not None
    assert verdict["qa_result_status"] == "passed"
    assert verdict["checker_commit_sha"] == "commit_new"
    assert verdict["protocol_checker"] == "commit_new/rules_v2"


