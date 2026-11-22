# Archive-Triggered GitHub QA (Issue #20)

## Overview

Replaces the 15-minute GitHub Actions `schedule` primary trigger with an archive-verified,
event-driven dispatch pipeline while keeping a low-frequency 3-hour staggered catch-up schedule:

1. **EC2 Archive Gate (`collector/python/archive_sync.py`)**
   - After a partition window reaches `COMPLETE` (`file_verification_status=verified`,
     `window_coverage_status=complete`, all expected exchanges remotely verified) **and**
     `meta/archive_manifest_index.json` is confirmed synced to Google Drive (`meta_sync_state`
     matches local SHA-256 and byte size), a durable `PENDING` record is enqueued into
     `qa_dispatch_outbox` inside `archive_ledger.sqlite`.
   - Enqueueing is evaluated strictly per window + index sync status, independent of overall
     archive exit codes (historical missing windows do not suppress a newly `COMPLETE` window).
   - Startup stubs (`STARTUP_STUB`) and incomplete/pending windows are never enqueued.
   - Records are deduplicated by `(run_id, target_window_utc, archive_revision)`, where
     `archive_revision` is a deterministic `sha256:...` digest computed from the stably sorted
     verified manifest set of that window (`data_coverage.compute_window_archive_revision`).
   - If the archive process exits unexpectedly between Drive index sync and outbox write,
     `discover_and_enqueue_qa_dispatches()` automatically recovers missing `COMPLETE` windows
     from the synced `meta/archive_manifest_index.json`.
   - `discover_and_enqueue_qa_dispatches()` reads `meta/archive_manifest_index.json` once into
     memory, hashes those exact bytes to verify against `meta_sync_state`, and parses JSON from
     the same byte snapshot so a concurrent index swap can never enqueue an unsynced window.
   - Outbox write failures inside `sync_sealed_directory()` are caught and reported under
     `qa_dispatch_outbox.error` so they never abort archive finalization or ledger snapshot upload.

2. **Independent Lightweight Notifier (`collector/python/qa_dispatch.py`, `collector/bin/qa-dispatch`)**
   - Runs as a separate systemd unit/timer (`tokyo-l2-qa-dispatch.service` /
     `tokyo-l2-qa-dispatch.timer`) completely decoupled from `tokyo-l2-collector.service` and
     `tokyo-l2-archive.service`.
   - Reads a fine-grained GitHub PAT (`Actions: Read and write` scoped strictly to
     `Huhabubu/mm`) exclusively from `/etc/tokyo-l2/github_qa_dispatch.env` (`0600`).
   - Sends `POST /repos/Huhabubu/mm/actions/workflows/tokyo-l2-incremental-qa.yml/dispatches`
     targeting `ref=main` with `{run_id, target_window_utc, archive_revision}`.
   - Enforces separate connect (`5s`) and total (`15s`) timeouts, keeps network/timeout/429/5xx
     failures `PENDING` with bounded exponential backoff, surfaces 401/403 token errors cleanly
     without leaking secrets, and tracks `api_accepted_at_utc` separately from
     `qa_completed_at_utc`.
   - Reconciles `ACCEPTED` (and retried `PENDING`) outbox records against persisted QA reports on
     `tokyo-l2-qa-state` (or `--qa-state-dir`) keyed by `(run_id, target_window_utc,
     archive_revision)`: matching terminal reports transition the record to `QA_COMPLETED`, while
     stale `ACCEPTED` records missing final reports past `TOKYO_L2_GITHUB_QA_ACCEPTED_STALE_TIMEOUT_S`
     (e.g. replaced in GitHub's concurrency queue) are requeued to `PENDING` with explicit
     `accepted_result_missing_or_replaced` visibility.

3. **GitHub Incremental QA (`collector/python/hourly_incremental_qa.py` & `.github/workflows/tokyo-l2-incremental-qa.yml`)**
   - Validates `(run_id, target_window_utc, archive_revision)` against Drive's
     `meta/archive_manifest_index.json` before passing.
   - Prioritizes `target_window_utc` FIRST ahead of any historical backlog, then spends remaining
     `limit` budget on oldest-first historical catch-up.
   - Evaluates `target_window_execution_status` and `historical_catchup_status` independently and
     exports `active_window_ready_for_protocol_qa`, so a transient download failure on a historical
     catch-up file never blocks deep protocol/replay QA for a target window whose archive files
     already passed.
   - Shares target-window results by `(run_id, target_window_utc, archive_revision,
     checker_commit_sha, qa_version, effective_config_sha256)` so duplicate dispatches reuse
     verified results while transient failures remain retryable.
   - Separately reports four disjoint window classes: `not_archived`, `archived_pending_qa`,
     `verified_failed`, and `verified_passed`, keeping historical backlog isolated from active
     window heartbeats.

## Paired Release Order (Required)

Production `.github/workflows/tokyo-l2-incremental-qa.yml` checks out collector Python code from
`feat/tokyo-l2-reliability-fixes` (via `source_ref`). To avoid invoking `--target-window` against
an older `hourly_incremental_qa.py`:

1. **First**, merge `feat/tokyo-l2-archive-triggered-qa` into `feat/tokyo-l2-reliability-fixes`.
2. **Second**, merge `feat/tokyo-l2-qa-workflow-event-trigger` into `main` (the workflow also
   includes a pre-flight CLI guard verifying `--target-window` support before running).
3. **Third**, roll out the EC2 notifier unit/timer below and verify end-to-end auto-dispatch before
   relying on the 3-hour catch-up schedule alone.

## EC2 Rollout (Scheduled Separately)

1. Create a fine-grained GitHub PAT scoped ONLY to `Huhabubu/mm` with `Actions: Read and write`.
2. Install the env file with `0600` permissions:
   ```bash
   sudo install -m 0600 -o ubuntu -g ubuntu \
     collector/deploy/env/github_qa_dispatch.env.example \
     /etc/tokyo-l2/github_qa_dispatch.env
   ```
3. Install and enable the independent systemd unit & timer:
   ```bash
   sudo cp collector/deploy/systemd/tokyo-l2-qa-dispatch.{service,timer} /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable --now tokyo-l2-qa-dispatch.timer
   ```
4. Inspect outbox status at any time without touching credentials:
   ```bash
   /opt/mm/collector/bin/qa-dispatch --dir /data/tokyo_l2 --config /etc/tokyo_l2/config.json --status-only
   ```
