# Tokyo L2 incremental Google Drive QA (every 3 hours)

This is a separate, **read-only** analysis pipeline. It never restarts the EC2 collector or alters archived market data.

## Architecture / activation

- EC2 seals and uploads a per-exchange UTC-hour `*.tsv.zst` and `*.manifest.json`.
- `.github/workflows/tokyo-l2-incremental-qa.yml` lives on the repository's **default `main` branch**. Scheduled GitHub Actions only run from that branch.
- The workflow checks out the Python checker from `feat/tokyo-l2-reliability-fixes` (merge the checker PR into that branch **before** enabling the schedule).
- QA results are committed to the dedicated `tokyo-l2-qa-state` branch in `qa_reports/raw/<exchange>/<date>/<manifest>.qa.json`.
- Both `ok` and `blocked` outcomes skip future reprocessing for the same immutable manifest path and `QA_VERSION`. Cloud download/auth failures are **not** recorded as checked, so they can retry. To re-run a stored result, use manual `force` or increment `QA_VERSION` after material logic changes.
- The schedule is `17 */3 * * *` (UTC); 16 partitions maximum per invocation, sequentially, one compressed partition in local scratch space at a time. It scans for new sealed manifests, not currently open `.partial` data. New files continue to accumulate while the workflow runs.
- Initial scan starts at UTC date 2026-11-21. Adjust `since_date` for manual backfill.

## Required GitHub repository Secrets

Add these via **Settings → Secrets and variables → Actions → New repository secret**.

1. `TOKYO_L2_QA_GDRIVE_SA_JSON`: complete JSON key of a dedicated Google service account with **Viewer-only** access to the target folder.
2. `TOKYO_L2_QA_GDRIVE_FOLDER_ID`: folder ID of `tokyo_l2_data` in Google Drive (not the account's entire Drive). Share this folder with the service account's `client_email` as Viewer.

Optional: `TOKYO_L2_QA_HEALTHCHECKS_URL` for dedicated QA heartbeat; don't reuse the EC2 collector or archive heartbeat URL. Never commit credentials, secrets, or production OAuth tokens.

The action builds a temporary `rclone` config with `scope = drive.readonly`, uses it to list manifests and fetch the selected raw partitions, and deletes temporary credentials when the GitHub runner terminates. It writes only to its GitHub QA-state branch. `GITHUB_TOKEN` must have contents write permission for that branch; if branch protection prevents updates, the persistence step fails.

## First verification

1. Merge the checker PR into `feat/tokyo-l2-reliability-fixes`.
2. Configure service-account Viewer sharing and the two secrets.
3. Merge the workflow PR into `main`.
4. Open **Actions → Tokyo L2 incremental hourly QA (3h) → Run workflow**. Use `limit=1` for the first run.
5. Verify the action reads exactly one raw exchange-hour and writes a JSON report under the state branch. Run again: `attempted=0`, `already_checked>=1`.
6. Increase the limit to 16 and inspect the first real quality results.

## QA scope and limitations

This check does full Zstd decompression, compressed SHA-256 + size verification, record parsing, timestamp range, within-partition sequence/clock monotonicity, manifest counts, recorded local loss/gap, expected symbol/channel presence, and channel inactivity windows. A low-activity trade feed can be flagged for manual review by the generic 300-second threshold.

**Each hourly report is a per-partition preliminary qualification**, not a certificate that the entire UTC day is continuous. Full-day certification still needs cross-hour sequence/continuity, all 96 expected exchange-hour partitions, 1,440 clock-certified minutes, source-specific exchange sequence integrity, and the existing HBT event-order validation. An hour with zero unexpected-error flags is not proof that the venue published every expected market event.

The workflow does not upload HBT NPZ files or raw market data to GitHub. A runner is capped at 8 GiB *compressed* per individual file as a disk-safety measure; adjust only after measuring available disk. Results contain summary metadata only.
