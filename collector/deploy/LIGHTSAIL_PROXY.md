# Phase P5: Tokyo Lightsail Archive Egress Proxy Deployment & Cutover Guide

> Scope: Routes **ONLY `rclone` archive/restore Google Drive traffic** through the Tokyo Lightsail SSH SOCKS5 tunnel (`127.0.0.1:1080`) to reduce long-term AWS EC2 internet egress costs.  
> **Hard invariants**:
> - `tokyo-l2-collector.service` (Rust 4-exchange WebSockets) stays **100% direct on EC2 (`apne1-az4`)** — NEVER touched or restarted.
> - Global `HTTP_PROXY` / `HTTPS_PROXY` / `ALL_PROXY` environment variables MUST NOT exist in `/etc/environment`, `tokyo-l2-collector.service`, or `tokyo-l2-meta.service`.
> - Auto-pruning (`--prune-hours 24`) remains **OFF** until both P1 real prune/restore acceptance and P5 billing/proxy verification are signed off.

---

## 1. Architecture & Isolation Guarantee

- `tokyo-l2-lightsail-proxy.service` maintains an unprivileged (`User=ubuntu`), auto-restarting (`Restart=always`, `ExitOnForwardFailure=yes`, `BatchMode=yes`, pinned `StrictHostKeyChecking=yes`) SSH SOCKS5 listener on `127.0.0.1:1080`.
- `tokyo-l2-archive.service.d/lightsail-proxy.conf` loads `/etc/tokyo-l2/proxy.env` (`TOKYO_L2_ARCHIVE_PROXY_URL=socks5://127.0.0.1:1080`, `TOKYO_L2_ARCHIVE_PROXY_MODE=required`) **only into `tokyo-l2-archive.service`**.
- Inside `RcloneBackend`, proxy env vars (`HTTP_PROXY`/`HTTPS_PROXY`/`ALL_PROXY`) are injected **strictly into the child `rclone` subprocess `env`**. Python's `notify_archive_healthcheck` (`hc-ping.com`), Telegram alerts, and the Rust live collector stay direct so tunnel failures still fire `/fail` alerts immediately.
- **`required` mode (default when enabled)**: performs RFC-1928 SOCKS5 greeting handshake before launching `rclone`. If the tunnel is down, archiving aborts **fail-closed** (`ProxyRequiredError`, exit code `2`) with zero silent direct bypass and zero local file deletion.
- **`fallback_direct` mode (explicit opt-in only)**: falls back to EC2 direct only if explicitly configured, recording `PROXY_FALLBACK_DIRECT` in SQLite events and `proxy=direct_fallback` in Healthchecks.

---

## 2. EC2 Pre-Deployment Preparation & Host Fingerprint Pinning

1. Tighten Lightsail TCP `22` firewall rule to allow only this EC2 public IP (`43.206.220.182` or Elastic IP) and authorized admin IPs.
2. Install the dedicated EC2 private key (`~/.ssh/tokyo_l2_lightsail`) with strict `0600` permissions under `/etc/tokyo-l2`:
   ```bash
   sudo install -d -m 0750 -o ubuntu -g ubuntu /etc/tokyo-l2
   sudo install -m 0600 -o ubuntu -g ubuntu /home/ubuntu/.ssh/tokyo_l2_lightsail /etc/tokyo-l2/tokyo_l2_lightsail
   ```
3. **Verify host fingerprint BEFORE pinning `/etc/tokyo-l2/lightsail_known_hosts` (NEVER blindly trust `ssh-keyscan`):**
   ```bash
   EXPECTED_FP="<VERIFIED_LIGHTSAIL_ED25519_SHA256_FINGERPRINT>"  # e.g. SHA256:... previously verified on Lightsail
   TMP_PUB="$(mktemp)"
   ssh-keyscan -p 22 -t ed25519 <LIGHTSAIL_PUBLIC_IP> > "${TMP_PUB}"
   ACTUAL_FP="$(ssh-keygen -lf "${TMP_PUB}" | awk '{print $2}')"
   echo "Scanned FP=${ACTUAL_FP} | Expected FP=${EXPECTED_FP}"
   test "${ACTUAL_FP}" = "${EXPECTED_FP}" || { echo "HOST KEY MISMATCH! ABORTING." >&2; rm -f "${TMP_PUB}"; exit 1; }
   ssh-keygen -H -f "${TMP_PUB}" >/dev/null 2>&1 && rm -f "${TMP_PUB}.old"
   sudo install -m 0644 -o ubuntu -g ubuntu "${TMP_PUB}" /etc/tokyo-l2/lightsail_known_hosts
   rm -f "${TMP_PUB}"
   ```
   *(Alternatively, if the host key was already verified and recorded in `/home/ubuntu/.ssh/known_hosts`, copy and verify only that matching line rather than re-scanning.)*
4. Populate `/etc/tokyo-l2/proxy.env`:
   ```bash
   sudo install -m 0600 -o ubuntu -g ubuntu /opt/mm/collector/deploy/env/proxy.env.example /etc/tokyo-l2/proxy.env
   sudo nano /etc/tokyo-l2/proxy.env
   # Confirm TOKYO_L2_LIGHTSAIL_HOST=<LIGHTSAIL_PUBLIC_IP> and TOKYO_L2_LIGHTSAIL_KEY=/etc/tokyo-l2/tokyo_l2_lightsail
   ```
5. Pre-test non-interactive SSH authentication + pinned host key **without** touching port `1080`:
   ```bash
   sudo -u ubuntu ssh -T \
     -o BatchMode=yes \
     -o StrictHostKeyChecking=yes \
     -o UserKnownHostsFile=/etc/tokyo-l2/lightsail_known_hosts \
     -o IdentitiesOnly=yes \
     -i /etc/tokyo-l2/tokyo_l2_lightsail \
     -p 22 \
     ubuntu@<LIGHTSAIL_PUBLIC_IP> 'echo LIGHTSAIL_SSH_AUTH_AND_HOSTKEY_OK'
   ```

---

## 3. Controlled Takeover of Port `127.0.0.1:1080` & Tunnel Verification

> Because an earlier manual test SSH tunnel may already occupy `127.0.0.1:1080`, `tokyo-l2-lightsail-proxy.service` (`ExitOnForwardFailure=yes`) must **not** be started until port `1080` is cleanly released outside the hourly archive window (`*:05 UTC`).

1. Verify archive service is idle and inspect any process holding `127.0.0.1:1080`:
   ```bash
   /opt/mm/collector/bin/proxy-verify check-takeover --port 1080
   ss -ltnp 'sport = :1080'
   pgrep -af 'ssh.*-D.*1080' || true
   ```
   Confirm current UTC time is outside `*:04–*:10 UTC` and `archive_active=false` (and `tokyo-l2-archive.service.d/lightsail-proxy.conf` is **not** installed yet so any manual delay affects nothing).
2. Cleanly stop **only** the verified temporary manual `ssh -D ... 1080` process (`kill <MANUAL_TUNNEL_PID>` — never `pkill ssh`), then confirm readiness:
   ```bash
   kill <MANUAL_TUNNEL_PID>
   /opt/mm/collector/bin/proxy-verify check-takeover --port 1080
   # Must report: "ready_for_systemd_takeover": true
   ```
3. Install and start `tokyo-l2-lightsail-proxy.service` to take ownership of `127.0.0.1:1080` (does **not** touch `tokyo-l2-archive.service` or `tokyo-l2-collector.service` yet):
   ```bash
   sudo install -m 0644 /opt/mm/collector/deploy/systemd/tokyo-l2-lightsail-proxy.service /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable --now tokyo-l2-lightsail-proxy.service
   sudo systemctl status tokyo-l2-lightsail-proxy.service
   /opt/mm/collector/bin/proxy-verify check-takeover --port 1080
   # Must report: "already_managed_by_systemd": true
   ```
4. Run full isolation, SOCKS5 handshake, egress IP, fail-closed (`127.0.0.1:1099`), remote roundtrip & billing/quota report:
   ```bash
   /opt/mm/collector/bin/proxy-verify verify \
     --check-systemd \
     --expected-proxy-ip <LIGHTSAIL_PUBLIC_IP> \
     --remote gdrive_main:tokyo_l2_data \
     --verify-remote

   /opt/mm/collector/bin/proxy-verify compare-report \
     --check-systemd \
     --expected-proxy-ip <LIGHTSAIL_PUBLIC_IP> \
     --remote gdrive_main:tokyo_l2_data \
     --run-live-benchmark \
     --monthly-upload-gb 500 \
     --out-md /tmp/p5_proxy_compare_report.md \
     --out-json /tmp/p5_proxy_compare_report.json
   ```
5. Confirm Rust collector isolation explicitly:
   ```bash
   systemctl show tokyo-l2-collector.service --property=Environment --property=EnvironmentFiles
   ```
   Verify **zero** `PROXY` variables appear.

---

## 4. Cutover `tokyo-l2-archive.service`, Billing Gate & Rollback

1. Only after step 3 passes and AWS Billing / Cost Explorer confirms EC2 → Lightsail transfer pricing (remember Lightsail counts **both** inbound + outbound against monthly quota, i.e., `500 GB` upload consumes `~1000 GB` quota):
   ```bash
   sudo mkdir -p /etc/systemd/system/tokyo-l2-archive.service.d
   sudo install -m 0644 \
     /opt/mm/collector/deploy/systemd/tokyo-l2-archive.service.d/lightsail-proxy.conf.example \
     /etc/systemd/system/tokyo-l2-archive.service.d/lightsail-proxy.conf
   sudo systemctl daemon-reload
   ```
2. Run a manual single archive sweep and confirm Healthchecks ping reports `proxy=lightsail_proxy`:
   ```bash
   sudo systemctl start tokyo-l2-archive.service
   sudo journalctl -u tokyo-l2-archive.service -n 50 --no-pager
   ```
3. Tunnel fault test before permanent reliance: temporarily stop `tokyo-l2-lightsail-proxy.service`, run `tokyo-l2-archive.service`, verify it exits blocked (`ProxyRequiredError`), local files stay intact, Healthchecks receives `/fail`, then restart `tokyo-l2-lightsail-proxy.service` and re-run `tokyo-l2-archive.service` to verify clean recovery.
4. **Instant Rollback to Direct EC2 Archive (zero impact on collector)**:
   ```bash
   sudo rm -f /etc/systemd/system/tokyo-l2-archive.service.d/lightsail-proxy.conf
   sudo systemctl daemon-reload
   ```
