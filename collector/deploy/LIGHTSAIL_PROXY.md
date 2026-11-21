# Phase P5: Tokyo Lightsail Private VPC Peering Archive Proxy Guide

> Scope: Routes **ONLY `rclone` archive/restore Google Drive traffic** through a dedicated SSH SOCKS5 tunnel (`127.0.0.1:1080`) over **same-AZ (`ap-northeast-1a`) VPC Peering** to Lightsail private IP `172.26.12.211:22`, while keeping Lightsail public IP `13.192.29.63` as the verified `HostKeyAlias` identity and backup SSH target.
>
> **Hard invariants**:
> - `tokyo-l2-collector.service` (Rust 4-exchange WebSockets) stays **100% direct on EC2** (`Environment` contains zero `PROXY` variables) — NEVER touched or restarted.
> - Global `HTTP_PROXY` / `HTTPS_PROXY` / `ALL_PROXY` environment variables MUST NOT exist in `/etc/environment`, `tokyo-l2-collector.service`, or `tokyo-l2-meta.service`.
> - Archive proxy mode MUST stay `TOKYO_L2_ARCHIVE_PROXY_MODE=required` (**do NOT enable `fallback_direct`**).
> - Auto-pruning (`--prune-hours 24`) remains **OFF** until separately authorized.

---

## 1. Verified Topology, Isolation & Billing Model

- **Confirmed AZ & VPC Peering**: Both EC2 and Lightsail are in AWS Tokyo region AZ `ap-northeast-1a` (`1a`) with VPC Peering enabled; private SSH `172.26.12.211:22` (`PRIVATE_SSH_OK`) and public IP `13.192.29.63` have been verified on EC2.
- **SSH Identity Pinning**: `tokyo-l2-lightsail-proxy.service` connects to `172.26.12.211:22` with `-o HostKeyAlias=13.192.29.63`, `-o StrictHostKeyChecking=yes`, `-o BatchMode=yes`, `-o ExitOnForwardFailure=yes`, `-o IdentitiesOnly=yes`, and `/etc/tokyo-l2/tokyo_l2_lightsail`. Unknown host keys are NEVER accepted automatically.
- **Process Isolation**: `/etc/tokyo-l2/proxy.env` is loaded only by `tokyo-l2-lightsail-proxy.service` and `tokyo-l2-archive.service.d/lightsail-proxy.conf`. Inside `RcloneBackend`, `HTTP_PROXY`/`HTTPS_PROXY`/`ALL_PROXY=socks5://127.0.0.1:1080` are injected **strictly into child `rclone` processes**. Healthchecks (`hc-ping.com`), Telegram, and the Rust live collector stay direct.
- **Cost & Quota Basis**:
  - **Primary Private Path (VPC Peering same physical AZ `ap-northeast-1a` -> `172.26.12.211`)**: `$0.00/GB` EC2 → Lightsail transfer under AWS VPC Peering same-AZ rules.
  - **Public Fallback Comparison (`13.192.29.63`)**: `$0.01/GB` EC2 → Lightsail transfer reference.
  - **Lightsail Plan**: Fixed `$5.00/month` with `1 TB` (`1024 GB`) monthly allowance counting **both inbound (from EC2) + outbound (to Google Drive)** (`~2×` uploaded bytes); overage `$0.14/GB`.
  - **Billing Note**: Hypothetical scenario numbers (e.g. `500 GB/month` modeling input) are illustrative only and NOT measured live collection volume; subsequent AWS Cost Explorer bills are for post-cutover reconciliation only.

---

## 2. EC2 Preparation, Host Key Verification & Private SSH Pre-Check

1. Install the verified EC2 private key (`~/.ssh/tokyo_l2_lightsail`) with strict `0600` permissions:
   ```bash
   sudo install -d -m 0750 -o ubuntu -g ubuntu /etc/tokyo-l2
   sudo install -m 0600 -o ubuntu -g ubuntu /home/ubuntu/.ssh/tokyo_l2_lightsail /etc/tokyo-l2/tokyo_l2_lightsail
   ```
2. **Pin the ALREADY-VERIFIED `13.192.29.63` Ed25519 host key into `/etc/tokyo-l2/lightsail_known_hosts`** (never accept unknown keys and never blindly pipe unverified scan output):
   ```bash
   # Option A (preferred): reuse the verified 13.192.29.63 host key line from /home/ubuntu/.ssh/known_hosts
   TMP_PUB="$(mktemp)"
   ssh-keygen -F 13.192.29.63 -f /home/ubuntu/.ssh/known_hosts | grep -v '^#' > "${TMP_PUB}"
   ssh-keygen -lf "${TMP_PUB}"   # visually confirm exact verified Ed25519 SHA256 fingerprint
   sudo install -m 0644 -o ubuntu -g ubuntu "${TMP_PUB}" /etc/tokyo-l2/lightsail_known_hosts
   rm -f "${TMP_PUB}"
   ```
   *(If scanning `13.192.29.63` fresh to a temporary file, check `test "$(ssh-keygen -lf "${TMP_PUB}" | awk '{print $2}')" = "${EXPECTED_FP}"` BEFORE running `ssh-keygen -H` and `install` into `/etc/tokyo-l2/lightsail_known_hosts`.)*
3. Populate `/etc/tokyo-l2/proxy.env` (`HOST=172.26.12.211`, `PUBLIC_FALLBACK_HOST=13.192.29.63`, `HOSTKEY_ALIAS=13.192.29.63`, `KEY=/etc/tokyo-l2/tokyo_l2_lightsail`, `TOKYO_L2_ARCHIVE_PROXY_MODE=required`):
   ```bash
   sudo install -m 0600 -o ubuntu -g ubuntu /opt/mm/collector/deploy/env/proxy.env.example /etc/tokyo-l2/proxy.env
   ```
4. Test non-interactive private SSH authentication using `HostKeyAlias=13.192.29.63` **without** touching port `1080`:
   ```bash
   sudo -u ubuntu ssh -T \
     -o BatchMode=yes \
     -o StrictHostKeyChecking=yes \
     -o UserKnownHostsFile=/etc/tokyo-l2/lightsail_known_hosts \
     -o HostKeyAlias=13.192.29.63 \
     -o IdentitiesOnly=yes \
     -i /etc/tokyo-l2/tokyo_l2_lightsail \
     -p 22 \
     ubuntu@172.26.12.211 'echo LIGHTSAIL_PRIVATE_VPC_SSH_OK'
   ```

---

## 3. Pre-Cutover Validation on Temporary Port `127.0.0.1:1081` (Zero Disruption to `:1080`)

Before touching any existing manual SSH tunnel on `:1080`, validate the entire VPC Peering SOCKS5 + `rclone` path on temporary loopback port `1081`:

1. Start a temporary background SSH SOCKS5 tunnel on `127.0.0.1:1081` via private IP `172.26.12.211` and record its PID:
   ```bash
   sudo -u ubuntu ssh -N -T \
     -o BatchMode=yes \
     -o ExitOnForwardFailure=yes \
     -o StrictHostKeyChecking=yes \
     -o UserKnownHostsFile=/etc/tokyo-l2/lightsail_known_hosts \
     -o HostKeyAlias=13.192.29.63 \
     -o IdentitiesOnly=yes \
     -i /etc/tokyo-l2/tokyo_l2_lightsail \
     -p 22 \
     -D 127.0.0.1:1081 \
     ubuntu@172.26.12.211 &
   TMP_TUNNEL_PID=$!
   ```
2. Verify isolation, SOCKS5 handshake, Lightsail public internet egress (`13.192.29.63`), fail-closed dead-proxy guard, and `rclone` readback over `127.0.0.1:1081`:
   ```bash
   /opt/mm/collector/bin/proxy-verify verify \
     --proxy-url socks5://127.0.0.1:1081 \
     --check-systemd \
     --expected-proxy-ip 13.192.29.63 \
     --remote gdrive_main:tokyo_l2_data \
     --verify-remote

   /opt/mm/collector/bin/proxy-verify compare-report \
     --proxy-url socks5://127.0.0.1:1081 \
     --check-systemd \
     --expected-proxy-ip 13.192.29.63 \
     --remote gdrive_main:tokyo_l2_data \
     --transfer-mode vpc_peering_same_az \
     --ec2-az ap-northeast-1a \
     --lightsail-az ap-northeast-1a \
     --run-live-benchmark \
     --monthly-upload-gb 500 \
     --out-md /tmp/p5_proxy_compare_report.md \
     --out-json /tmp/p5_proxy_compare_report.json
   ```
3. Cleanly stop ONLY the temporary `:1081` SSH process before transitioning `:1080`:
   ```bash
   kill "${TMP_TUNNEL_PID}"
   ```

---

## 4. Fail-Closed Controlled Takeover of Formal Port `127.0.0.1:1080`

> `check-takeover` **MUST** receive `--data-root /data/tokyo_l2` so `.archive_sync.lock` is never skipped. Any missing argument, unreadable lock file, unavailable/unclear `systemd` status, active archive run, or unverified `:1080` socket ownership fails closed with `status="blocked"` (exit code `2`).

1. Inspect takeover gate outside the hourly archive window (`*:04–*:10 UTC`):
   ```bash
   /opt/mm/collector/bin/proxy-verify check-takeover --port 1080 --data-root /data/tokyo_l2
   ss -ltnp 'sport = :1080'
   pgrep -af 'ssh.*-D.*1080' || true
   ```
   (Verify `tokyo-l2-archive.service.d/lightsail-proxy.conf` is NOT installed yet so stopping a manual tunnel cannot affect direct archiving.)
2. If `:1080` is occupied by the earlier manual tunnel (`conflict=port_occupied_by_non_systemd_process`), stop ONLY that verified manual `:1080` PID (`kill <MANUAL_TUNNEL_PID>` — never `pkill ssh`) and re-run the readiness gate:
   ```bash
   kill <MANUAL_TUNNEL_PID>
   /opt/mm/collector/bin/proxy-verify check-takeover --port 1080 --data-root /data/tokyo_l2
   # MUST exit 0 and report: "status": "ok", "ready_for_systemd_takeover": true
   ```
3. Install and start `tokyo-l2-lightsail-proxy.service`, then confirm both `systemd` active state AND actual `:1080` socket PID ownership:
   ```bash
   sudo install -m 0644 /opt/mm/collector/deploy/systemd/tokyo-l2-lightsail-proxy.service /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable --now tokyo-l2-lightsail-proxy.service
   sudo systemctl status tokyo-l2-lightsail-proxy.service
   /opt/mm/collector/bin/proxy-verify check-takeover --port 1080 --data-root /data/tokyo_l2
   # MUST exit 0 and report: "already_managed_by_systemd": true AND "port_owned_by_proxy_unit": true
   ```
4. Re-verify `tokyo-l2-collector.service` has zero proxy env exposure and verify formal `:1080` path:
   ```bash
   systemctl show tokyo-l2-collector.service --property=Environment --property=EnvironmentFiles
   /opt/mm/collector/bin/proxy-verify verify \
     --proxy-url socks5://127.0.0.1:1080 \
     --check-systemd \
     --expected-proxy-ip 13.192.29.63 \
     --remote gdrive_main:tokyo_l2_data \
     --verify-remote
   ```

---

## 5. Cutover `tokyo-l2-archive.service`, Backup Public Switch & Instant Rollback

1. Install the archive drop-in (keeps `required` fail-closed mode, NO `fallback_direct`, NO `--prune-hours 24`):
   ```bash
   sudo mkdir -p /etc/systemd/system/tokyo-l2-archive.service.d
   sudo install -m 0644 \
     /opt/mm/collector/deploy/systemd/tokyo-l2-archive.service.d/lightsail-proxy.conf.example \
     /etc/systemd/system/tokyo-l2-archive.service.d/lightsail-proxy.conf
   sudo systemctl daemon-reload
   ```
2. Trigger one manual archive run and verify `proxy=lightsail_proxy`:
   ```bash
   sudo systemctl start tokyo-l2-archive.service
   sudo journalctl -u tokyo-l2-archive.service -n 50 --no-pager
   ```
3. **Failure-injection check before relying on proxy**: temporarily stop `tokyo-l2-lightsail-proxy.service`, run `tokyo-l2-archive.service`, verify `ProxyRequiredError` aborts fail-closed without direct fallback, local files remain intact, and Healthchecks receives `/fail`; then restart `tokyo-l2-lightsail-proxy.service` and confirm clean recovery.
4. **Emergency failover to Lightsail public IP `13.192.29.63`** (same verified `HostKeyAlias=13.192.29.63` and `lightsail_known_hosts`):
   Edit `TOKYO_L2_LIGHTSAIL_HOST=13.192.29.63` in `/etc/tokyo-l2/proxy.env` and `sudo systemctl restart tokyo-l2-lightsail-proxy.service`.
5. **Instant rollback to EC2 Direct archive (zero impact on Rust collector)**:
   ```bash
   sudo rm -f /etc/systemd/system/tokyo-l2-archive.service.d/lightsail-proxy.conf
   sudo systemctl daemon-reload
   ```
