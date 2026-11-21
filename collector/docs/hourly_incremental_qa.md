# Tokyo L2 incremental Google Drive QA (configurable 60m / 15m)

This is a separate, **read-only** analysis pipeline. It never restarts the EC2 collector or alters archived market data.

## Architecture / activation

- EC2 seals and uploads per-exchange UTC-aligned partitions (`5`, `15`, `30`, or `60` minutes; legacy hourly `*.tsv.zst` and restart-safe segmented `*_p15m_<tag>.tsv.zst` files are both supported) along with `*.manifest.json` and a remotely verified index `meta/archive_manifest_index.json`.
- `.github/workflows/tokyo-l2-incremental-qa.yml` lives on the repository's **default `main` branch** (delivered via a separate PR targeting `main`). Scheduled GitHub Actions only run from `main`.
- In 15-minute trial mode, the workflow triggers approximately every 15 minutes (`3,18,33,48 * * * *` UTC) and preserves manual `workflow_dispatch` inputs (`config_path`, `since_window`, `run_id`, `limit`, `force`, `sla_grace_minutes`). If GitHub queues, delays, or drops a scheduled run, subsequent runs catch up automatically from verified archive records.
- The workflow checks out the Python checker from `feat/tokyo-l2-reliability-fixes` and records `checker_commit_sha` (`git -C source rev-parse HEAD`) on every single report and run summary.
- QA summary reports (zero raw market data, zero credentials) are committed to the dedicated `tokyo-l2-qa-state` branch at three levels:
  1. **Single-file report**: `qa_reports/raw/<exchange>/<date>/<manifest>.qa.json`
  2. **Whole-window report**: `qa_reports/windows/<date>/<window_id>.window.qa.json` (merges multi-segment restarts per exchange, checks all 4 exchanges & 72 expected channels, separates startup stubs, lists damaged segments)
  3. **Complete UTC day report**: `qa_reports/days/<date>.day.qa.json` (checks all expected windows in the UTC day, cross-window `rx_seq` continuity per `boot_id`, cross-restart tracking, `chrony` 1,440 certified minutes, and protocol continuity)
- **Memoization & re-check key**: `(source_manifest, data_sha256, manifest_sha256, QA_VERSION, effective_config_sha256)`. Identical verified objects skip without re-downloading; content, manifest, rule-version, or collector-config changes automatically trigger re-check.
- **Five explicit states**: `passed`, `failed`, `pending_archive`, `needs_review`, `overdue`.
  - When an expected window is past delivery SLA (`--sla-grace-minutes`) with missing files, missing QA results, or persistence failure, the job exits non-zero (`overdue` / `failed`) and **never** reports zero-file success.
  - When all due windows already have valid up-to-date results and no new files arrived, the run skips cleanly (`passed`, `attempted=0`, `already_checked>0`).
  - Transient download/timeout failures retry with bounded backoff and do not corrupt stored state.

## Required GitHub repository Secrets

Add these via **Settings → Secrets and variables → Actions → New repository secret**:

1. `TOKYO_L2_QA_GDRIVE_SA_JSON`: complete JSON key of a dedicated Google service account with **Viewer-only** (`drive.readonly`) access to the target Drive folder.
2. `TOKYO_L2_QA_GDRIVE_FOLDER_ID`: folder ID of `tokyo_l2_data` in Google Drive.
3. `TOKYO_L2_QA_HEALTHCHECKS_URL` *(currently NOT configured in Huhabubu/mm — recommended before enabling scheduled 15m QA)*: dedicated QA Healthchecks ping URL, strictly separate from EC2 collector and archive heartbeat URLs. When absent, the workflow prints a warning naming this missing secret and completes without pinging Healthchecks.

## Ordering & validation rules

- **File write order**: `rx_seq` MUST be strictly monotonic inside every file; across windows of the same `boot_id`, bounded cross-window late frames are accounted for without allowing duplicate `rx_seq` or unrecorded gaps.
- **Receive monotonic clock**: `recv_mono_ns` is checked per `(producer, conn_id, epoch)` within each `boot_id` so multi-endpoint / multi-connection Tokio producers (`binance_public` A/B, `binance_market` A/B, REST snapshots) do not trigger false clock regressions while true per-producer regressions fail closed.
- **Channel taxonomy**: Shared with live `meta_daemon.py` (`continuous_quote`, `depth`, `event_trade`). Silent event-driven trade channels are routed to `needs_review` rather than falsely assumed healthy from active sibling channels.
