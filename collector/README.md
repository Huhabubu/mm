# AWS Tokyo Co-City L2 Autonomous Collector & Offline QA Pipeline (`SPEC_v1.4`)

Single-binary Rust dual-active causal collector + atomic hourly TSV-Raw Zstd storage + single Google Drive Pro (`rclone`) verifier + offline QA gatekeeper & `hftbacktest==2.4.4` converter, governed by [`SPEC_v1.4.md`](SPEC_v1.4.md).

## Architecture Overview

| Layer | Component | Responsibility |
| --- | --- | --- |
| **P0 Specs** | `collector/specs/{protocol_matrix,instrument_map,research_bindings}.json`, `config*.json`, `freeze.template.json` | 4-exchange protocol contracts, 24-market mapping (`BTC/ETH/XRP/SOL/DOGE/SUI`), research bindings (`frozen_research_sha: null` until R1/R2/R3 freeze). |
| **Hot Path (Rust)** | `collector/src/` (`tokyo-l2-collector`) | Dual-active A/B WS connections per endpoint, pre-parse `recv_wall_ns`/`recv_mono_ns` stamping, independent per-connection protocol validators, strictly causal single-primary selector (zero cross-connection delta splicing), atomic hourly `TSV-Raw` UTF-8 + `zstd -3` + `SHA-256` manifest sealing (no Base64). |
| **Metadata Daemon** | `collector/python/meta_daemon.py` | Strictly off hot-path: REST contract specs (with `X-Gate-Size-Decimal: 1` & Bybit pagination + inter-poll change isolation), 5-min RTT CSV, 1-min `chronyc tracking` CSV (`<= 1.0 ms` certification), disk `> 70%` & webhook alerts. |
| **Archive Sync** | `collector/python/archive_sync.py` | Local zstd + SHA-256 verification, pre-upload remote conflict check (never overwrites differing hash), idempotent `rclone copy` to `gdrive_main:tokyo_l2_data/`, remote ID/bytes/hash/readback verification, `verified_archive_ledger.jsonl`, safe `>24h` local pruning. |
| **QA & HBT 2.4.4 Converter** | `collector/python/qa_convert.py` | Verifies integrity/monotonicity/`LOCAL_LOSS`/`chrony`/spec-isolation, converts strictly separated streams (`binance_bbo`, follower 50-level HBT `event_dtype` `.npz` passing `validate_event_order`, independent follower L1 BBO `.npz`), outputs `eligibility_calendar.{json,csv}`, evaluates `QUEUE_UNKNOWN` beyond 50 levels, and integrates with `scripts/core_glft_engine.py::run_core_glft`. |
| **Deployment** | `collector/deploy/` | `chrony.conf` (`169.254.169.123`), `systemd/` units (`collector`, `meta`, `archive.timer`), and `aws_tokyo_setup.sh` (verifies `apne1-az4`). |

## Unified CLI Commands (§6.3)

All commands emit structured JSON and fail closed (`"status": "blocked"`, non-zero exit code) on any failed gate.

```bash
# 1. Offline preflight (P0/P1 single-command clean-environment verification; zero network calls)
./collector/bin/preflight --config collector/specs/config.btc_gate_binance_mvp.json --offline
./collector/bin/preflight --config collector/specs/config.example.json --offline

# 2. Live dual-active collection (BLOCKED with exit code 3 unless explicitly authorized in P2/P3)
./collector/bin/collect --config /etc/tokyo-l2/config.json --run-id p2_trial_YYYYMMDD --allow-live-run

# 3. Atomic archive verification & upload to single Google Drive Pro (rclone)
./collector/bin/archive --dir /data/tokyo_l2 --remote gdrive_main:tokyo_l2_data --verify-remote --prune-hours 24

# 4. Offline QA gatekeeper & hftbacktest==2.4.4 conversion
./collector/bin/qa-convert --manifest /data/tokyo_l2/verified_archive_ledger.jsonl --out /data/tokyo_l2/hbt_npz
```

### Exit Codes

| Exit Code | Meaning |
| --- | --- |
| `0` | `status="ok"` — all gates passed |
| `2` | `status="blocked"` — config, protocol, integrity, archive conflict, or QA gate failed |
| `3` | `status="blocked"` — live collection refused because `TOKYO_L2_LIVE_AUTHORIZED=1` / `--allow-live-run` was not explicitly granted |
| `4` | `status="blocked"` — runtime collector error |

## Single-Command Offline Reproduction (P1)

```bash
# Run all Rust protocol, causal selector, storage, and anti-lookahead tests
cargo test --manifest-path collector/Cargo.toml

# Run all Python E2E HBT 2.4.4 + core_glft_engine.py, anti-lookahead, 4-exchange, and archive fault tests
.venv/bin/pytest -v collector/tests/test_pipeline_offline.py

# Run full offline preflight smoke verification
./collector/bin/preflight --config collector/specs/config.example.json --offline
```

## Phase Status & P2/P3 Handover Checklist

- [x] **P0 (Protocol & Research Interface Freeze)**: `protocol_matrix.json`, `instrument_map.json`, `research_bindings.json` verified against 2026 official docs (`frozen_research_sha` explicitly kept `null` until client R1/R2/R3 freeze).
- [x] **P1 (Offline Build, Deterministic Tests & CI)**: Rust collector + Python meta/archive/QA/HBT 2.4.4 pipeline + offline pytest & GitHub Actions CI green.
- [ ] **P2 (AWS Tokyo `apne1-az4` 24h Technical Trial)** — requires client cooperation:
  1. Provision AWS Tokyo `ap-northeast-1` EC2 instance in physical AZ **`apne1-az4`** (`c7i.large` / `c6i.large`, 100 GB `gp3`, direct Public IPv4, no NAT Gateway).
  2. Run `collector/deploy/aws_tokyo_setup.sh` on the EC2 host to verify `apne1-az4`, deploy AWS Time Sync `chrony.conf`, and measure 10-min 4-exchange RTT (`0.5-5.0 ms` median target).
  3. Configure single Google Drive Pro (5 TB) `rclone` remote `gdrive_main:tokyo_l2_data` with personal OAuth Client ID and optional alert webhook (`TOKYO_L2_ALERT_WEBHOOK_URL`).
  4. Authorize P2 live 4-exchange first-frame check & 24h cross-UTC-midnight trial.
- [ ] **P3 (Formal Blind Collection, 21–30 complete UTC days)** — after P2 passes and Bitget `2026-12-01` live `books50` delta cutover is verified on live frames, seal `freeze.json` with the final immutable `Huhabubu/mm` R1/R2/R3 commit SHA before starting blind collection.
- [ ] **P4 (Unblinding & Frozen Evaluation)** — strictly one-pass execution against sealed `freeze.json`.
