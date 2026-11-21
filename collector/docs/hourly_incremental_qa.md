# Tokyo L2 incremental Google Drive QA (configurable 60m / 15m)

This is a separate, **read-only** analysis pipeline. It never restarts the EC2 collector or alters archived market data.

## Architecture / activation

- EC2 seals and uploads per-exchange UTC-aligned partitions (`5`, `15`, `30`, or `60` minutes; legacy hourly `*.tsv.zst` and restart-safe segmented `*_p15m_<tag>.tsv.zst` files are both supported) along with `*.manifest.json` and a remotely verified index `meta/archive_manifest_index.json`.
- `.github/workflows/tokyo-l2-incremental-qa.yml` lives on the repository's **default `main` branch** (delivered via a separate PR targeting `main`). Scheduled GitHub Actions only run from `main`.
-In 15-minute trial mode, the workflow triggers approximately every 15 minutes (`3,18,33,48 * * * *` UTC) and requires explicit binding to the active cutover parameters (`TOKYO_L2_QA_CONFIG_PATH`, `TOKYO_L2_QA_SINCE_WINDOW`, `TOKYO_L2_QA_RUN_ID` repository variables or `workflow_dispatch` inputs) so scheduled runs never fall back to mismatched historical defaults or empty batch IDs.
- The workflow checks out the Python checker from `feat/tokyo-l2-reliability-fixes` and records `checker_commit_sha` (`git -C source rev-parse HEAD`) on every single report and run summary.
- **Trusted verified index & 3-way verification**:
  - Formal QA discovers verified partitions strictly from `meta/archive_manifest_index.json` (or trusted `meta/verified_archive_ledger.compat.jsonl`). It **never** falls back to scanning unverified `raw/` files; if the verified index is unavailable or corrupt, QA reports `discovery_mode="verified_index_unavailable"`, `job_execution_status="failed"`, and `status="blocked"`.
  - After downloading a partition and its sidecar manifest, QA enforces **3-way verification** (`Index ↔ Manifest ↔ Data File`): data SHA-256/bytes and manifest SHA-256/bytes must match across all three sources, and `[planned_start_wall_ns, planned_end_wall_ns)`, `partition_minutes`, and `collection_run_id` must match the bound configuration and batch.
- **Strict batch & duration isolation**:
  - Archive reconciliation and QA reports isolate windows by `(run_id, planned_start_wall_ns, planned_end_wall_ns, partition_minutes)`. A 60-minute archive starting at the same timestamp cannot fill a 15-minute window, and partial files from two different `run_id` batches cannot combine to satisfy a window.
- QA summary reports (zero raw market data, zero credentials) are committed to the dedicated `tokyo-l2-qa-state` branch at three levels:
  1. **Single-file report**: `qa_reports/raw/<exchange>/<date>/<manifest>.qa.json`
  2. **Whole-window report**: `qa_reports/windows/<date>/<window_id>.window.qa.json` (merges multi-segment restarts per exchange within the same batch and exact window span, checks all 4 exchanges & 72 expected channels, separates startup stubs, lists damaged segments)
  3. **Complete UTC day report**: `qa_reports/days/<date>.day.qa.json` (checks all expected windows in the UTC day, cross-window `rx_seq` continuity per `boot_id`, cross-restart tracking, `chrony` 1,440 certified minutes, and protocol continuity)
- **Memoization & re-check key**: `(source_manifest, data_sha256, manifest_sha256, QA_VERSION, effective_config_sha256)`. Identical verified objects skip without re-downloading; content, manifest, rule-version, or collector-config changes automatically trigger re-check.
- **Separated status & heartbeat model**:
  - `job_execution_status`: `"ok"` or `"failed"` (whether the QA job, trusted index load, and downloads succeeded).
  - `current_window_id`, `current_window_completed`, `current_window_verification_status`: whether the latest expected/due window (`passed`, `needs_review`, `failed`, `overdue`, `pending_archive`) has completed verification. When a window is still `pending_archive` (`current_window_completed=false`), the job never sends a false-positive success heartbeat.
  - `historical_data_quality_status`, `historical_damaged_windows`, `historical_damaged_partitions`: tracks all historical damaged windows and partitions (including the 4 historical loss-marked manifests) separately so old historical damage remains visible in every report without permanently masking whether current windows are verifying cleanly.

## Required GitHub repository Secrets & Variables

Add Secrets via **Settings → Secrets and variables → Actions → Secrets**:

1. `TOKYO_L2_QA_GDRIVE_SA_JSON`: complete JSON key of a dedicated Google service account with **Viewer-only** (`drive.readonly`) access to the target Drive folder.
2. `TOKYO_L2_QA_GDRIVE_FOLDER_ID`: folder ID of `tokyo_l2_data` in Google Drive.
3. `TOKYO_L2_QA_HEALTHCHECKS_URL` *(currently NOT configured in Huhabubu/mm — recommended before enabling scheduled 15m QA)*: dedicated QA Healthchecks ping URL, strictly separate from EC2 collector and archive heartbeat URLs.

Add Variables via **Settings → Secrets and variables → Actions → Variables** before enabling scheduled runs:

1. `TOKYO_L2_QA_CONFIG_PATH`: effective collector config path for the active batch (e.g. `collector/specs/config.example.json` for 60m or `collector/specs/config.trial_15m.json` for 15m trial).
2. `TOKYO_L2_QA_SINCE_WINDOW`: exact UTC cutover window ID (`YYYY-MM-DDTHH:MMZ`) where the active batch started.
3. `TOKYO_L2_QA_RUN_ID`: exact `deployment.run_id` / `collection_run_id` of the active collection batch.

## Ordering, late-frame & validation rules

- **Rejected late frames (`LATE_FRAME_REJECTED`)**: Expired late frames are NOT counted as market-data writes in live stream activity (`written_wall_ns` is not updated; `rejected_late_frames` is incremented). In single-file QA, `LATE_FRAME_REJECTED` increments `local_loss_events` and `dropped_frames`, blocks the partition (`qa_status="failed"`), and attributes the loss back to `target_window_id` (`loss_attributed_window_id`), blocking the target window where the frame originally belonged.
- **File write order**: `rx_seq` MUST be strictly monotonic inside every file; across windows of the same `boot_id`, bounded cross-window late frames are accounted for without allowing duplicate `rx_seq` or unrecorded gaps.
- **Receive monotonic clock**: `recv_mono_ns` is checked per `(producer, conn_id, epoch)` within each `boot_id` so multi-endpoint / multi-connection Tokio producers (`binance_public` A/B, `binance_market` A/B, REST snapshots) do not trigger false clock regressions while true per-producer regressions fail closed.
- **Channel taxonomy**: Shared with live `meta_daemon.py` (`continuous_quote`, `depth`, `event_trade`). Silent event-driven trade channels are routed to `needs_review` rather than falsely assumed healthy from active sibling channels.
