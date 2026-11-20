#!/usr/bin/env bash
# Download, SHA-256 verify, and install the CI-built Ubuntu 24.04 x86_64
# `tokyo-l2-collector` binary from Huhabubu/mm GitHub Prerelease (zero Rust compilation on EC2).
#
# Usage:
#   ./collector/deploy/install_prebuilt_collector.sh [RELEASE_TAG]
#
# If RELEASE_TAG is omitted, resolves the latest `collector-pre-*` prerelease from GitHub.
set -euo pipefail

REPO="${TOKYO_L2_GITHUB_REPO:-Huhabubu/mm}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DEST_DIR="${TOKYO_L2_INSTALL_DIR:-${REPO_ROOT}/collector/target/release}"
DEST_BIN="${DEST_DIR}/tokyo-l2-collector"
TAG="${1:-${TOKYO_L2_RELEASE_TAG:-}}"

if [[ -z "${TAG}" ]]; then
  echo "Resolving latest collector-pre-* GitHub Prerelease from ${REPO}..."
  TAG=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases?per_page=20" | \
    python3 -c '
import json, sys
rels = json.load(sys.stdin)
for r in rels:
    if not r.get("draft") and r.get("tag_name", "").startswith("collector-pre-"):
        print(r["tag_name"])
        sys.exit(0)
sys.exit(1)
') || {
    echo "ERROR: Could not find any published collector-pre-* prerelease in ${REPO}." >&2
    exit 1
  }
fi

BASE_URL="https://github.com/${REPO}/releases/download/${TAG}"
TARBALL="tokyo-l2-collector-ubuntu24.04-x86_64.tar.gz"
TARBALL_SHA="${TARBALL}.sha256"

TMP_DIR="$(mktemp -d -t tokyo-l2-prebuilt-XXXXXX)"
trap 'rm -rf "${TMP_DIR}"' EXIT

echo "Downloading ${TAG} from ${BASE_URL}..."
curl -fsSL -o "${TMP_DIR}/${TARBALL}" "${BASE_URL}/${TARBALL}"
curl -fsSL -o "${TMP_DIR}/${TARBALL_SHA}" "${BASE_URL}/${TARBALL_SHA}"

echo "Verifying tarball SHA-256..."
(cd "${TMP_DIR}" && sha256sum -c "${TARBALL_SHA}")

echo "Extracting and verifying executable SHA-256..."
tar -C "${TMP_DIR}" -xzf "${TMP_DIR}/${TARBALL}"
(cd "${TMP_DIR}" && sha256sum -c SHA256SUMS)

mkdir -p "${DEST_DIR}"
install -m 755 "${TMP_DIR}/tokyo-l2-collector" "${DEST_BIN}.new"
mv -f "${DEST_BIN}.new" "${DEST_BIN}"

echo "Installed verified prebuilt binary -> ${DEST_BIN}"
cat "${TMP_DIR}/BUILD_INFO.txt"
cat "${TMP_DIR}/SHA256SUMS"

if [[ -f "${REPO_ROOT}/collector/specs/config.example.json" ]]; then
  echo "Running offline preflight self-check on installed binary..."
  "${DEST_BIN}" preflight --config "${REPO_ROOT}/collector/specs/config.example.json" --offline
fi
