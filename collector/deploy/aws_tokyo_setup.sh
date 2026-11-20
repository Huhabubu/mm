#!/usr/bin/env bash
# AWS Tokyo (ap-northeast-1 / apne1-az4) EC2 Host Bootstrap & Pre-Flight Verification Script
# Run ONLY on the client-provisioned EC2 instance during Phase P2!
set -euo pipefail

EXPECTED_AZ_ID="apne1-az4"
DATA_ROOT="${DATA_ROOT:-/data/tokyo_l2}"
RTT_DURATION_SECS="${RTT_DURATION_SECS:-600}"

echo "=== [1/6] Checking AWS EC2 IMDSv2 physical Availability Zone ID ==="
TOKEN=$(curl -sS --max-time 3 -X PUT "http://169.254.169.254/latest/api/token" \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" || true)
if [[ -z "${TOKEN}" ]]; then
  echo "BLOCKED: Unable to reach EC2 IMDSv2. Run this script on an AWS Tokyo EC2 instance." >&2
  exit 2
fi
AZ_ID=$(curl -sS -H "X-aws-ec2-metadata-token: ${TOKEN}" \
  http://169.254.169.254/latest/meta-data/placement/availability-zone-id)
REGION=$(curl -sS -H "X-aws-ec2-metadata-token: ${TOKEN}" \
  http://169.254.169.254/latest/meta-data/placement/region)
INSTANCE_TYPE=$(curl -sS -H "X-aws-ec2-metadata-token: ${TOKEN}" \
  http://169.254.169.254/latest/meta-data/instance-type)
echo "Region=${REGION}, Physical AZ ID=${AZ_ID}, InstanceType=${INSTANCE_TYPE}"
if [[ "${REGION}" != "ap-northeast-1" || "${AZ_ID}" != "${EXPECTED_AZ_ID}" ]]; then
  echo "BLOCKED: Expected ap-northeast-1 / ${EXPECTED_AZ_ID}, got ${REGION} / ${AZ_ID}." >&2
  exit 2
fi

echo "=== [2/6] Installing OS packages (chrony, zstd, rclone, jq, curl, python3-venv) ==="
sudo apt-get update -y
sudo apt-get install -y chrony zstd rclone jq curl python3-venv

echo "=== [3/6] Configuring AWS Time Sync Service (169.254.169.123) ==="
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
sudo cp "${SCRIPT_DIR}/chrony.conf" /etc/chrony/chrony.conf
sudo systemctl enable --now chrony
sudo systemctl restart chrony
sleep 5
chronyc tracking

echo "=== [4/6] Preparing data directory ${DATA_ROOT} & verifying EBS capacity ==="
sudo mkdir -p "${DATA_ROOT}/raw" "${DATA_ROOT}/meta/specs" "${DATA_ROOT}/meta/rtt" "${DATA_ROOT}/meta/chrony"
sudo chown -R "$(id -u):$(id -g)" "${DATA_ROOT}"
df -h "${DATA_ROOT}"

echo "=== [5/6] Installing CI-verified prebuilt Rust binary (no EC2 Rust build) & Python .venv ==="
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
bash "${SCRIPT_DIR}/install_prebuilt_collector.sh" "${TOKYO_L2_RELEASE_TAG:-}"

if [[ ! -d "${REPO_ROOT}/.venv" ]]; then
  python3 -m venv "${REPO_ROOT}/.venv"
  "${REPO_ROOT}/.venv/bin/pip" install --upgrade pip
  "${REPO_ROOT}/.venv/bin/pip" install hftbacktest==2.4.4 numpy numba zstandard pytest
fi

echo "=== [6/6] Running 4-exchange Tokyo RTT & Chrony precheck (${RTT_DURATION_SECS}s) ==="
"${REPO_ROOT}/.venv/bin/python" - <<PY
import time, statistics, json, sys
from pathlib import Path
sys.path.insert(0, "${REPO_ROOT}/collector/python")
from meta_daemon import RTT_TARGETS, measure_tcp_rtt_ms, sample_chrony_once

samples = {ex: [] for ex in RTT_TARGETS}
deadline = time.time() + int("${RTT_DURATION_SECS}")
while True:
    for ex, (host, port) in RTT_TARGETS.items():
        rtt = measure_tcp_rtt_ms(host, port)
        if rtt is not None:
            samples[ex].append(rtt)
    if time.time() >= deadline:
        break
    time.sleep(10)

chrony = sample_chrony_once(Path("${DATA_ROOT}"))
summary = {
    "chrony": chrony,
    "rtt_median_ms": {ex: (round(statistics.median(v), 3) if v else None) for ex, v in samples.items()},
    "rtt_samples": {ex: len(v) for ex, v in samples.items()},
}
print(json.dumps(summary, indent=2))
if not chrony.get("certified_le_1ms"):
    print("WARNING: chrony bound > 1.0 ms; wait for sync before starting P2/P3!", file=sys.stderr)
PY

echo ""
echo "Next manual steps before P2 start:"
echo "  1. Configure rclone Google Drive Pro remote named 'gdrive_main' with your own OAuth Client ID:"
echo "       rclone config"
echo "       rclone mkdir gdrive_main:tokyo_l2_data"
echo "       rclone about gdrive_main:"
echo "  2. Copy collector/specs/config.example.json to /etc/tokyo-l2/config.json"
echo "  3. Create /etc/tokyo-l2/live.env with:"
echo "       TOKYO_L2_LIVE_AUTHORIZED=1"
echo "       TOKYO_L2_RUN_ID=p2_trial_YYYYMMDD"
echo "  4. Install systemd units from collector/deploy/systemd/ and start services."
