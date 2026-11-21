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

## 2. EC2 Pre-Deployment Preparation (Run on EC2)

1. Tighten Lightsail TCP `22` firewall rule to allow only this EC2 public IP (`43.206.220.182` or Elastic IP) and authorized admin IPs.
2. Pin the Lightsail SSH host key & verify the dedicated private key permissions:
   ```bash
   sudo install -d -m 0750 -o ubuntu -g ubuntu /etc/tokyo-l2
   # Move/copy dedicated private key (never stored in git)
   sudo install -m 0600 -o ubuntu -g ubuntu ~/.ssh/lightsail_tokyo_ed25519 /etc/tokyo-l2/lightsail_ed25519
   ssh-keyscan -p 22 -H <LIGHTSAIL_PUBLIC_IP> | sudo tee /etc/tokyo-l2/lightsail_known_hosts >/dev/null
   sudo chown ubuntu:ubuntu /etc/tokyo-l2/lightsail_known_hosts
   sudo chmod 0644 /etc/tokyo-l2/lightsail_known_hosts
   ```
3. Populate `/etc/tokyo-l2/proxy.env`:
   ```bash
   sudo install -m 0600 -o ubuntu -g ubuntu /opt/mm/collector/deploy/env/proxy.env.example /etc/tokyo-l2/proxy.env
   sudo nano /etc/tokyo-l2/proxy.env
   ```

---

## 3. Install & Verify Tunnel Before Cutting Over Archive Service

1. Install and start the tunnel unit (does **not** touch `tokyo-l2-archive.service` or `tokyo-l2-collector.service` yet):
   ```bash
   sudo install -m 0644 /opt/mm/collector/deploy/systemd/tokyo-l2-lightsail-proxy.service /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable --now tokyo-l2-lightsail-proxy.service
   sudo systemctl status tokyo-l2-lightsail-proxy.service
   ```
2. Run isolation, connectivity, egress IP, fail-closed (`127.0.0.1:1099`), remote roundtrip & billing/quota report:
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
3. Confirm Rust collector isolation explicitly:
   ```bash
   systemctl show tokyo-l2-collector.service --property=Environment --property=EnvironmentFiles
   ```
   Verify **zero** `PROXY` variables appear.

---

## 4. Cutover `tokyo-l2-archive.service` & Verify Cost Explorer Billing

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
