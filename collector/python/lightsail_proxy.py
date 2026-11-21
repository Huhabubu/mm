#!/usr/bin/env python3
"""Phase P5: Lightsail SSH SOCKS5 proxy isolation, fail-closed validation, and benchmark reporting.

Strict architectural boundaries:
1. Proxy applies ONLY to `rclone` child subprocesses spawned by archive/restore (`RcloneBackend`).
2. Rust market-data collector (`tokyo-l2-collector.service`) and Telegram/Healthchecks requests MUST
   remain direct on EC2 (`apne1-az4`); global HTTP_PROXY / HTTPS_PROXY env vars are forbidden.
3. Under default P5 `required` mode, an unreachable SOCKS5 tunnel aborts archiving fail-closed with
   zero silent direct fallback and preserves all local `.tsv.zst`, `.manifest.json`, and SQLite state.
4. Optional `fallback_direct` mode requires explicit configuration, verifies failure via RFC-1928 probe,
   strips proxy vars cleanly for `rclone`, and records explicit fallback warnings in reports/Healthchecks.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import socket
import subprocess
import sys
import tempfile
import time
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

PYTHON_DIR = Path(__file__).resolve().parent
COLLECTOR_DIR = PYTHON_DIR.parent
DEPLOY_DIR = COLLECTOR_DIR / "deploy"

DEFAULT_PROXY_URL = "socks5://127.0.0.1:1080"
VALID_PROXY_MODES = ("off", "required", "fallback_direct")
VALID_PROXY_SCHEMES = ("socks5", "socks5h")
PROXY_ENV_KEYS = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
)
COLLECTOR_FORBIDDEN_ENV_KEYS = PROXY_ENV_KEYS + (
    "NO_PROXY",
    "no_proxy",
    "TOKYO_L2_ARCHIVE_PROXY_URL",
    "TOKYO_L2_ARCHIVE_PROXY_MODE",
)

SYSTEMD_COLLECTOR_UNIT = "tokyo-l2-collector.service"
SYSTEMD_ARCHIVE_UNIT = "tokyo-l2-archive.service"
SYSTEMD_PROXY_UNIT = "tokyo-l2-lightsail-proxy.service"

_RE_ENV_ASSIGN = re.compile(r"^\s*(?:Environment\s*=\s*\"?)?([A-Za-z_][A-Za-z0-9_]*)\s*=", re.MULTILINE)


class ProxyRequiredError(RuntimeError):
    """Raised when `required` Lightsail proxy mode fails validation or connectivity probe."""


@dataclass(frozen=True)
class ProxyResolution:
    mode: str
    configured_url: str | None
    active_proxy_url: str | None
    used_route: str
    fallback_used: bool
    probe: dict[str, Any] | None
    warning: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "configured_url": self.configured_url,
            "active_proxy_url": self.active_proxy_url,
            "used_route": self.used_route,
            "fallback_used": self.fallback_used,
            "probe": self.probe,
            "warning": self.warning,
        }


def parse_proxy_url(proxy_url: str) -> tuple[str, str, int]:
    """Validate and parse a SOCKS5 proxy URL into `(scheme, host, port)`."""
    raw = str(proxy_url or "").strip()
    if not raw:
        raise ValueError("Empty proxy URL")
    parsed = urllib.parse.urlparse(raw)
    scheme = (parsed.scheme or "").lower()
    if scheme not in VALID_PROXY_SCHEMES:
        raise ValueError(
            f"Unsupported proxy scheme {scheme!r}; expected one of {VALID_PROXY_SCHEMES}"
        )
    host = parsed.hostname
    if not host:
        raise ValueError(f"Missing hostname in proxy URL: {proxy_url!r}")
    port = parsed.port
    if port is None or not (1 <= int(port) <= 65535):
        raise ValueError(f"Invalid or missing port in proxy URL: {proxy_url!r}")
    return scheme, host, int(port)


def probe_socks5_proxy(
    host: str,
    port: int,
    *,
    timeout_s: float = 3.0,
) -> dict[str, Any]:
    """Perform a real TCP + RFC-1928 SOCKS5 greeting (`NO AUTH`) handshake against `host:port`."""
    t0 = time.monotonic()
    sock: socket.socket | None = None
    try:
        sock = socket.create_connection((host, int(port)), timeout=timeout_s)
        sock.settimeout(timeout_s)
        # RFC-1928 greeting: VER=0x05, NMETHODS=0x01, METHOD=0x00 (No authentication)
        sock.sendall(b"\x05\x01\x00")
        resp = sock.recv(2)
        elapsed_ms = round((time.monotonic() - t0) * 1000.0, 3)
        if len(resp) < 2:
            return {
                "reachable": False,
                "host": host,
                "port": int(port),
                "auth_method": None,
                "latency_ms": elapsed_ms,
                "error": f"truncated SOCKS5 greeting reply ({len(resp)} bytes)",
            }
        if resp[0] != 0x05:
            return {
                "reachable": False,
                "host": host,
                "port": int(port),
                "auth_method": resp[1],
                "latency_ms": elapsed_ms,
                "error": f"unexpected SOCKS version byte {resp[0]} (expected 5)",
            }
        if resp[1] == 0xFF:
            return {
                "reachable": False,
                "host": host,
                "port": int(port),
                "auth_method": resp[1],
                "latency_ms": elapsed_ms,
                "error": "SOCKS5 server rejected no-auth greeting (0xFF)",
            }
        return {
            "reachable": True,
            "host": host,
            "port": int(port),
            "auth_method": resp[1],
            "latency_ms": elapsed_ms,
            "error": None,
        }
    except Exception as exc:
        elapsed_ms = round((time.monotonic() - t0) * 1000.0, 3)
        return {
            "reachable": False,
            "host": host,
            "port": int(port),
            "auth_method": None,
            "latency_ms": elapsed_ms,
            "error": f"{type(exc).__name__}: {exc}",
        }
    finally:
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass


def resolve_rclone_proxy_config(
    *,
    proxy_url: str | None = None,
    proxy_mode: str | None = None,
    environ: Mapping[str, str] | None = None,
) -> tuple[str, str | None]:
    """Resolve effective `(mode, url)` from explicit overrides or `TOKYO_L2_ARCHIVE_PROXY_*` env."""
    env = os.environ if environ is None else environ
    mode_raw = (
        proxy_mode
        if proxy_mode is not None
        else env.get("TOKYO_L2_ARCHIVE_PROXY_MODE", "off")
    )
    mode = str(mode_raw or "off").strip().lower()
    if mode not in VALID_PROXY_MODES:
        raise ValueError(
            f"Invalid archive proxy mode {mode!r}; expected one of {VALID_PROXY_MODES}"
        )

    url_raw = (
        proxy_url
        if proxy_url is not None
        else env.get("TOKYO_L2_ARCHIVE_PROXY_URL", "")
    )
    url_str = str(url_raw or "").strip()
    if mode == "off":
        return "off", (url_str or None)

    eff_url = url_str or DEFAULT_PROXY_URL
    parse_proxy_url(eff_url)
    return mode, eff_url


def build_rclone_subprocess_env(
    active_proxy_url: str | None,
    *,
    clear_proxy: bool = False,
    base_env: Mapping[str, str] | None = None,
) -> dict[str, str] | None:
    """Build isolated child environment ONLY for `rclone` subprocesses.

    - Returns `None` when no proxy overrides are active and no stale proxy cleanup is needed,
      preserving normal unmodified subprocess inheritance.
    - When `active_proxy_url` is set, injects SOCKS5 URL into proxy env vars and removes NO_PROXY
      so `rclone` cannot silently bypass the proxy.
    - When `clear_proxy=True`, explicitly strips proxy env vars from child env during fallback.
    """
    if active_proxy_url is None and not clear_proxy:
        return None
    src = os.environ if base_env is None else base_env
    child = dict(src)
    for k in PROXY_ENV_KEYS + ("NO_PROXY", "no_proxy"):
        child.pop(k, None)
    if active_proxy_url is not None:
        parse_proxy_url(active_proxy_url)
        for k in PROXY_ENV_KEYS:
            child[k] = active_proxy_url
    return child


def resolve_rclone_proxy_runtime(
    *,
    proxy_url: str | None = None,
    proxy_mode: str | None = None,
    environ: Mapping[str, str] | None = None,
    probe_fn: Callable[[str, int], dict[str, Any]] = probe_socks5_proxy,
) -> ProxyResolution:
    """Resolve and verify runtime proxy routing BEFORE running `rclone` transfers."""
    mode, configured_url = resolve_rclone_proxy_config(
        proxy_url=proxy_url,
        proxy_mode=proxy_mode,
        environ=environ,
    )
    if mode == "off":
        return ProxyResolution(
            mode="off",
            configured_url=configured_url,
            active_proxy_url=None,
            used_route="direct",
            fallback_used=False,
            probe=None,
            warning=None,
        )

    assert configured_url is not None
    _, host, port = parse_proxy_url(configured_url)
    probe = probe_fn(host, port)
    if probe.get("reachable"):
        return ProxyResolution(
            mode=mode,
            configured_url=configured_url,
            active_proxy_url=configured_url,
            used_route="lightsail_proxy",
            fallback_used=False,
            probe=probe,
            warning=None,
        )

    err_detail = probe.get("error") or "unreachable"
    if mode == "required":
        raise ProxyRequiredError(
            f"Lightsail SOCKS5 proxy {configured_url} unreachable ({err_detail}); "
            "aborting archive fail-closed without silent direct fallback (local source files preserved)."
        )

    # Explicit `fallback_direct` authorization only
    warn = (
        f"[ARCHIVE_PROXY_FALLBACK_DIRECT] Lightsail SOCKS5 proxy {configured_url} unreachable "
        f"({err_detail}); explicitly falling back to direct EC2 archive transfer."
    )
    print(warn, file=sys.stderr, flush=True)
    return ProxyResolution(
        mode=mode,
        configured_url=configured_url,
        active_proxy_url=None,
        used_route="direct_fallback",
        fallback_used=True,
        probe=probe,
        warning=warn,
    )


def _extract_env_assignments(text: str) -> set[str]:
    found: set[str] = set()
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        m = _RE_ENV_ASSIGN.match(s)
        if m:
            found.add(m.group(1))
        if s.startswith("Environment="):
            rest = s.split("=", 1)[1].strip().strip('"').strip("'")
            for token in rest.split():
                if "=" in token:
                    found.add(token.split("=", 1)[0])
    return found


def audit_proxy_isolation(
    deploy_dir: Path = DEPLOY_DIR,
    *,
    collector_systemd_env_text: str | None = None,
) -> dict[str, Any]:
    """Audit deployment templates (and optional live systemd env) to ensure Rust collector stays direct."""
    violations: list[str] = []
    checked_files: list[str] = []

    collector_unit = deploy_dir / "systemd" / SYSTEMD_COLLECTOR_UNIT
    notify_env = deploy_dir / "env" / "notify.env.example"
    archive_unit = deploy_dir / "systemd" / SYSTEMD_ARCHIVE_UNIT
    proxy_unit = deploy_dir / "systemd" / SYSTEMD_PROXY_UNIT
    proxy_env = deploy_dir / "env" / "proxy.env.example"
    archive_dropin = deploy_dir / "systemd" / f"{SYSTEMD_ARCHIVE_UNIT}.d" / "lightsail-proxy.conf.example"

    for must_exist in (collector_unit, archive_unit, proxy_unit, proxy_env, archive_dropin):
        if not must_exist.is_file():
            violations.append(f"Missing required deploy template: {must_exist}")

    if collector_unit.is_file():
        checked_files.append(str(collector_unit))
        col_txt = collector_unit.read_text(encoding="utf-8")
        col_keys = _extract_env_assignments(col_txt)
        bad_col = sorted(k for k in col_keys if k in COLLECTOR_FORBIDDEN_ENV_KEYS)
        if bad_col:
            violations.append(f"{collector_unit.name} contains forbidden proxy keys: {bad_col}")
        if "proxy.env" in col_txt:
            violations.append(f"{collector_unit.name} must NEVER load proxy.env")

    if notify_env.is_file():
        checked_files.append(str(notify_env))
        n_keys = _extract_env_assignments(notify_env.read_text(encoding="utf-8"))
        bad_n = sorted(k for k in n_keys if k in COLLECTOR_FORBIDDEN_ENV_KEYS)
        if bad_n:
            violations.append(f"{notify_env.name} must not set proxy vars: {bad_n}")

    if archive_unit.is_file():
        checked_files.append(str(archive_unit))
        a_txt = archive_unit.read_text(encoding="utf-8")
        a_keys = _extract_env_assignments(a_txt)
        bad_a = sorted(k for k in a_keys if k in PROXY_ENV_KEYS)
        if bad_a:
            violations.append(
                f"{archive_unit.name} sets global {bad_a}; must only scope via TOKYO_L2_ARCHIVE_PROXY_* so Healthchecks stays direct"
            )

    if archive_dropin.is_file():
        checked_files.append(str(archive_dropin))
        d_txt = archive_dropin.read_text(encoding="utf-8")
        d_keys = _extract_env_assignments(d_txt)
        bad_d = sorted(k for k in d_keys if k in PROXY_ENV_KEYS)
        if bad_d:
            violations.append(
                f"{archive_dropin.name} sets global {bad_d}; must only load TOKYO_L2_ARCHIVE_PROXY_* env"
            )

    if proxy_env.is_file():
        checked_files.append(str(proxy_env))
        p_keys = _extract_env_assignments(proxy_env.read_text(encoding="utf-8"))
        bad_p = sorted(k for k in p_keys if k in PROXY_ENV_KEYS)
        if bad_p:
            violations.append(f"{proxy_env.name} must not export global {bad_p}")

    if proxy_unit.is_file():
        checked_files.append(str(proxy_unit))
        pu_txt = proxy_unit.read_text(encoding="utf-8")
        for req in ("ExitOnForwardFailure=yes", "StrictHostKeyChecking=yes", "BatchMode=yes", "Restart=always"):
            if req not in pu_txt:
                violations.append(f"{proxy_unit.name} missing hardened option {req!r}")

    if collector_systemd_env_text is not None:
        live_keys = _extract_env_assignments(collector_systemd_env_text)
        bad_live = sorted(k for k in live_keys if k in COLLECTOR_FORBIDDEN_ENV_KEYS)
        if bad_live:
            violations.append(f"Live {SYSTEMD_COLLECTOR_UNIT} environment exposes forbidden proxy keys: {bad_live}")

    return {
        "isolated": len(violations) == 0,
        "violations": violations,
        "checked_files": checked_files,
    }


def default_egress_ip_fetcher(proxy_url: str | None, *, check_url: str = "https://checkip.amazonaws.com") -> str:
    """Query public egress IP via curl directly or through SOCKS5 (`socks5h://` remote DNS)."""
    cmd = ["curl", "-fsS", "--max-time", "10"]
    if proxy_url:
        # Use socks5h semantics with curl for DNS-over-tunnel verification
        scheme, host, port = parse_proxy_url(proxy_url)
        curl_proxy = f"socks5h://{host}:{port}" if scheme in ("socks5", "socks5h") else proxy_url
        cmd.extend(["--proxy", curl_proxy])
    cmd.append(check_url)
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
    if proc.returncode != 0:
        raise RuntimeError(f"egress IP check failed (rc={proc.returncode}): {proc.stderr.strip()}")
    return proc.stdout.strip()


def estimate_lightsail_monthly_cost(
    monthly_upload_gb: float,
    *,
    plan_usd: float = 5.0,
    plan_quota_gb: float = 1024.0,
    ec2_direct_per_gb: float = 0.114,
    ec2_free_gb: float = 100.0,
    ec2_to_lightsail_per_gb: float | None = 0.01,
    lightsail_overage_per_gb: float = 0.14,
) -> dict[str, Any]:
    """Estimate EC2 direct vs Lightsail relay egress costs (accounting for Lightsail 2x IN+OUT quota).

    Note: `ec2_to_lightsail_transfer_rate_verified` stays False until verified against real AWS Cost Explorer.
    """
    up_gb = max(0.0, float(monthly_upload_gb))
    direct_billable_gb = max(0.0, up_gb - max(0.0, float(ec2_free_gb)))
    direct_cost_usd = round(direct_billable_gb * float(ec2_direct_per_gb), 2)

    # Lightsail counts BOTH inbound (from EC2) and outbound (to Google Drive) against monthly transfer quota
    lightsail_quota_consumed_gb = round(up_gb * 2.0, 2)
    lightsail_overage_gb = max(0.0, round(lightsail_quota_consumed_gb - float(plan_quota_gb), 2))
    lightsail_overage_usd = round(lightsail_overage_gb * float(lightsail_overage_per_gb), 2)
    ec2_relay_transfer_usd = (
        round(up_gb * float(ec2_to_lightsail_per_gb), 2)
        if ec2_to_lightsail_per_gb is not None
        else None
    )
    total_lightsail_est = (
        round(float(plan_usd) + lightsail_overage_usd + (ec2_relay_transfer_usd or 0.0), 2)
        if ec2_relay_transfer_usd is not None
        else None
    )

    return {
        "monthly_upload_gb": up_gb,
        "ec2_direct": {
            "account_free_tier_gb": float(ec2_free_gb),
            "rate_per_gb_usd": float(ec2_direct_per_gb),
            "billable_gb": direct_billable_gb,
            "estimated_cost_usd": direct_cost_usd,
        },
        "lightsail_relay": {
            "plan_usd": float(plan_usd),
            "plan_quota_gb": float(plan_quota_gb),
            "quota_note": "Lightsail counts BOTH inbound + outbound against allowance (~2x uploaded volume)",
            "quota_consumed_gb": lightsail_quota_consumed_gb,
            "overage_gb": lightsail_overage_gb,
            "overage_usd": lightsail_overage_usd,
            "assumed_ec2_to_lightsail_per_gb_usd": ec2_to_lightsail_per_gb,
            "estimated_ec2_to_lightsail_usd": ec2_relay_transfer_usd,
            "estimated_total_usd": total_lightsail_est,
            "billing_gate_verified": False,
        },
    }


def run_proxy_verification(
    *,
    proxy_url: str = DEFAULT_PROXY_URL,
    dead_proxy_url: str = "socks5://127.0.0.1:1099",
    expected_proxy_ip: str | None = None,
    remote: str | None = None,
    verify_remote: bool = False,
    deploy_dir: Path = DEPLOY_DIR,
    probe_fn: Callable[[str, int], dict[str, Any]] = probe_socks5_proxy,
    ip_fetcher: Callable[[str | None], str] = default_egress_ip_fetcher,
    remote_roundtrip_fn: Callable[[str, str], dict[str, Any]] | None = None,
    collector_systemd_env_text: str | None = None,
    check_live_egress: bool = True,
) -> dict[str, Any]:
    """Execute end-to-end verification of Lightsail proxy isolation, SOCKS5 handshake, failover, and preservation."""
    from archive_sync import RcloneBackend, file_hashes

    failed: list[str] = []
    isolation = audit_proxy_isolation(
        deploy_dir, collector_systemd_env_text=collector_systemd_env_text
    )
    if not isolation["isolated"]:
        failed.extend(isolation["violations"])

    _, host, port = parse_proxy_url(proxy_url)
    socks_probe = probe_fn(host, port)
    if not socks_probe.get("reachable"):
        failed.append(f"SOCKS5 handshake probe failed for {proxy_url}: {socks_probe.get('error')}")

    direct_ip: str | None = None
    proxy_ip: str | None = None
    ip_distinct: bool | None = None
    if check_live_egress:
        try:
            direct_ip = ip_fetcher(None)
        except Exception as exc:
            failed.append(f"Direct egress IP probe failed: {exc}")
        try:
            proxy_ip = ip_fetcher(proxy_url)
        except Exception as exc:
            failed.append(f"Proxied egress IP probe failed: {exc}")
        if direct_ip and proxy_ip:
            ip_distinct = direct_ip != proxy_ip
            if not ip_distinct:
                failed.append(
                    f"Proxied egress IP ({proxy_ip}) matches EC2 direct IP ({direct_ip}); tunnel not routing egress!"
                )
            if expected_proxy_ip and proxy_ip != expected_proxy_ip:
                failed.append(
                    f"Proxied egress IP mismatch: got {proxy_ip}, expected {expected_proxy_ip}"
                )

    # Negative test 1: unreachable proxy under `required` mode MUST abort fail-closed and preserve local data
    dead_required_blocked = False
    dead_required_error: str | None = None
    local_preserved_on_fail = False
    with tempfile.TemporaryDirectory(prefix="p5_neg_probe_") as tmpdir:
        sentinel = Path(tmpdir) / "local_source_preserved.tsv.zst"
        sentinel.write_bytes(b"PRESERVE_ON_PROXY_FAILURE")
        try:
            resolve_rclone_proxy_runtime(
                proxy_url=dead_proxy_url,
                proxy_mode="required",
                probe_fn=probe_fn,
            )
            failed.append(
                f"Fail-closed negative test failed: dead proxy {dead_proxy_url} did NOT raise ProxyRequiredError"
            )
        except ProxyRequiredError as exc:
            dead_required_blocked = True
            dead_required_error = str(exc)
        local_preserved_on_fail = sentinel.is_file() and sentinel.read_bytes() == b"PRESERVE_ON_PROXY_FAILURE"
        if not local_preserved_on_fail:
            failed.append("Local source sentinel file was altered during failed proxy probe")

    # Negative test 2: unreachable proxy under explicit `fallback_direct` records fallback cleanly
    fallback_res = resolve_rclone_proxy_runtime(
        proxy_url=dead_proxy_url,
        proxy_mode="fallback_direct",
        probe_fn=probe_fn,
    )
    fallback_ok = (
        fallback_res.used_route == "direct_fallback"
        and fallback_res.fallback_used is True
        and fallback_res.active_proxy_url is None
        and bool(fallback_res.warning)
    )
    if not fallback_ok:
        failed.append("Explicit fallback_direct test did not produce direct_fallback route with warning")

    # Optional real rclone roundtrip probe over proxy (never alters collector data paths; cleans up or uses _network_test/)
    remote_probe_res: dict[str, Any] | None = None
    if verify_remote and remote:
        if remote_roundtrip_fn is not None:
            remote_probe_res = remote_roundtrip_fn(remote, proxy_url)
        else:
            with tempfile.TemporaryDirectory(prefix="p5_rclone_probe_") as tmpdir:
                probe_file = Path(tmpdir) / f"p5_proxy_probe_{int(time.time())}.bin"
                probe_file.write_bytes(os.urandom(65536))
                local_sha = file_hashes(probe_file)["sha256"]
                rel = f"_network_test/{probe_file.name}"
                backend = RcloneBackend(remote, proxy_url=proxy_url, proxy_mode="required")
                t0 = time.monotonic()
                backend.copy_file(probe_file, rel, immutable=True)
                info = backend.stat_remote(rel)
                readback_sha = backend.readback_sha256(rel)
                elapsed_s = round(time.monotonic() - t0, 3)
                matched = (readback_sha == local_sha) and probe_file.is_file()
                if not matched:
                    failed.append(
                        f"Remote proxied readback mismatch: local={local_sha}, readback={readback_sha}"
                    )
                remote_probe_res = {
                    "remote_rel_path": rel,
                    "bytes": len(probe_file.read_bytes()),
                    "local_sha256": local_sha,
                    "remote_id": info.remote_id,
                    "readback_sha256": readback_sha,
                    "sha256_match": matched,
                    "elapsed_s": elapsed_s,
                    "local_source_preserved": probe_file.is_file(),
                }

    return {
        "status": "ok" if not failed else "blocked",
        "phase": "p5_lightsail_proxy_verify",
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "proxy_url": proxy_url,
        "isolation_audit": isolation,
        "socks5_probe": socks_probe,
        "egress": {
            "direct_ip": direct_ip,
            "proxy_ip": proxy_ip,
            "expected_proxy_ip": expected_proxy_ip,
            "ip_distinct": ip_distinct,
        },
        "fail_closed_probe": {
            "dead_proxy_url": dead_proxy_url,
            "blocked": dead_required_blocked,
            "error": dead_required_error,
            "local_source_preserved": local_preserved_on_fail,
        },
        "fallback_probe": fallback_res.to_dict(),
        "remote_roundtrip_probe": remote_probe_res,
        "failed_items": failed,
    }


def format_compare_report(
    *,
    verification: dict[str, Any],
    direct_transfer: dict[str, Any] | None = None,
    proxy_transfer: dict[str, Any] | None = None,
    cost_estimate: dict[str, Any],
) -> str:
    """Render Markdown comparison & cost/quota signoff report for direct vs Lightsail archive egress."""
    eg = verification.get("egress") or {}
    iso = verification.get("isolation_audit") or {}
    fc = verification.get("fail_closed_probe") or {}
    fb = verification.get("fallback_probe") or {}
    ls_est = cost_estimate.get("lightsail_relay") or {}
    ec2_est = cost_estimate.get("ec2_direct") or {}

    lines = [
        "# Tokyo L2 Archive Egress: EC2 Direct vs Lightsail Proxy Verification Report",
        "",
        f"- **Generated (UTC)**: `{verification.get('checked_at_utc')}`",
        f"- **Overall Gate**: `{verification.get('status')}`",
        f"- **Proxy URL**: `{verification.get('proxy_url')}`",
        "",
        "## 1. Security & Process Isolation",
        f"- **Rust Collector (`tokyo-l2-collector.service`) Direct / Zero Global Proxy**: `{iso.get('isolated')}`",
        f"- **Checked Templates**: `{len(iso.get('checked_files') or [])}` files",
    ]
    if iso.get("violations"):
        for v in iso["violations"]:
            lines.append(f"  - VIOLATION: {v}")

    lines.extend(
        [
            "",
            "## 2. Connectivity & Egress Identity",
            f"- **SOCKS5 RFC-1928 Handshake**: `reachable={verification.get('socks5_probe', {}).get('reachable')}` "
            f"(`latency_ms={verification.get('socks5_probe', {}).get('latency_ms')}`)",
            f"- **Direct Egress IP**: `{eg.get('direct_ip')}`",
            f"- **Proxied Egress IP**: `{eg.get('proxy_ip')}` (distinct=`{eg.get('ip_distinct')}`)",
            "",
            "## 3. Failure Protection & Data Preservation",
            f"- **Required Mode Dead Proxy (`{fc.get('dead_proxy_url')}`) Fail-Closed**: `blocked={fc.get('blocked')}`, "
            f"`local_source_preserved={fc.get('local_source_preserved')}`",
            f"- **Explicit `fallback_direct` Probe**: `used_route={fb.get('used_route')}`, "
            f"`fallback_used={fb.get('fallback_used')}`",
            "",
            "## 4. Transfer Benchmark (Single-Sample Reference; Not a Decision Criterion)",
            "| Path | Bytes | Elapsed (s) | SHA-256 Verified | Local Preserved |",
            "|---|---:|---:|---|---|",
        ]
    )

    for label, row in (("EC2 Direct", direct_transfer), ("Lightsail SOCKS5", proxy_transfer)):
        if row:
            lines.append(
                f"| {label} | {row.get('bytes', 'N/A')} | {row.get('elapsed_s', 'N/A')} | "
                f"`{row.get('sha256_match', 'N/A')}` | `{row.get('local_source_preserved', 'N/A')}` |"
            )
        else:
            lines.append(f"| {label} | N/A | N/A | `pending` | `true` |")

    lines.extend(
        [
            "",
            "## 5. Monthly Egress Cost & Lightsail Quota Estimate (Billing Verification Pending)",
            f"- **Assumed Monthly Google Drive Upload**: `{cost_estimate.get('monthly_upload_gb')} GB`",
            f"- **EC2 Direct Estimate**: `{ec2_est.get('billable_gb')} GB` billable × `${ec2_est.get('rate_per_gb_usd')}/GB` = **`${ec2_est.get('estimated_cost_usd')}`**",
            f"- **Lightsail Quota Consumed (Ingress + Egress = 2× upload)**: `{ls_est.get('quota_consumed_gb')} GB` / `{ls_est.get('plan_quota_gb')} GB` "
            f"(overage=`{ls_est.get('overage_gb')} GB`, overage_cost=`${ls_est.get('overage_usd')}`)",
            f"- **Lightsail Total Estimate**: `${ls_est.get('estimated_total_usd')}` "
            f"(includes plan `${ls_est.get('plan_usd')}` + assumed EC2→Lightsail `${ls_est.get('estimated_ec2_to_lightsail_usd')}`)",
            f"- **AWS Cost Explorer Billing Verified**: `{ls_est.get('billing_gate_verified')}` "
            "(MUST verify actual EC2→Lightsail transfer billing item on real AWS bill before permanent cutover; keep `--prune-hours 24` disabled during verification).",
            "",
        ]
    )
    return "\n".join(lines)


def _measure_rclone_benchmark(
    *,
    remote: str,
    proxy_url: str | None,
    proxy_mode: str,
    payload_bytes: int,
    label: str,
) -> dict[str, Any]:
    from archive_sync import RcloneBackend, file_hashes

    with tempfile.TemporaryDirectory(prefix=f"p5_bench_{label}_") as tmpdir:
        src = Path(tmpdir) / f"p5_bench_{label}_{int(time.time())}.bin"
        src.write_bytes(os.urandom(payload_bytes))
        local_sha = file_hashes(src)["sha256"]
        rel = f"_network_test/{src.name}"
        backend = RcloneBackend(remote, proxy_url=proxy_url, proxy_mode=proxy_mode)
        t0 = time.monotonic()
        backend.copy_file(src, rel, immutable=True)
        readback = backend.readback_sha256(rel)
        elapsed_s = round(time.monotonic() - t0, 3)
        return {
            "label": label,
            "remote_rel_path": rel,
            "bytes": payload_bytes,
            "elapsed_s": elapsed_s,
            "sha256_match": (readback == local_sha),
            "local_source_preserved": src.is_file(),
        }


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="proxy-verify",
        description="P5 Lightsail SOCKS5 proxy isolation audit, connectivity check, and comparison report CLI",
    )
    sub = p.add_subparsers(dest="command", required=True)

    p_ver = sub.add_parser("verify", help="Verify isolation, SOCKS5 handshake, egress IP, fail-closed & optional remote roundtrip")
    p_ver.add_argument("--proxy-url", default=os.environ.get("TOKYO_L2_ARCHIVE_PROXY_URL", DEFAULT_PROXY_URL))
    p_ver.add_argument("--dead-proxy-url", default="socks5://127.0.0.1:1099")
    p_ver.add_argument("--expected-proxy-ip", default=None)
    p_ver.add_argument("--remote", default=None)
    p_ver.add_argument("--verify-remote", action="store_true")
    p_ver.add_argument("--deploy-dir", type=Path, default=DEPLOY_DIR)
    p_ver.add_argument("--skip-live-egress", action="store_true", help="Offline mode: audit & local SOCKS5 probe only without internet curl")
    p_ver.add_argument("--check-systemd", action="store_true", help="Also query live systemctl show environment on EC2")

    p_rep = sub.add_parser("compare-report", help="Run verification and generate Direct vs Lightsail Markdown + JSON comparison report")
    p_rep.add_argument("--proxy-url", default=os.environ.get("TOKYO_L2_ARCHIVE_PROXY_URL", DEFAULT_PROXY_URL))
    p_rep.add_argument("--expected-proxy-ip", default=None)
    p_rep.add_argument("--remote", default=None)
    p_rep.add_argument("--benchmark-bytes", type=int, default=1048576, help="Payload bytes if --run-live-benchmark is set")
    p_rep.add_argument("--run-live-benchmark", action="store_true", help="Upload single test object direct and via proxy to compare")
    p_rep.add_argument("--monthly-upload-gb", type=float, default=500.0)
    p_rep.add_argument("--plan-usd", type=float, default=5.0)
    p_rep.add_argument("--plan-quota-gb", type=float, default=1024.0)
    p_rep.add_argument("--deploy-dir", type=Path, default=DEPLOY_DIR)
    p_rep.add_argument("--skip-live-egress", action="store_true")
    p_rep.add_argument("--out-md", type=Path, default=None)
    p_rep.add_argument("--out-json", type=Path, default=None)

    return p


def _read_live_collector_systemd_env() -> str:
    proc = subprocess.run(
        ["systemctl", "show", SYSTEMD_COLLECTOR_UNIT, "--property=Environment", "--property=EnvironmentFiles"],
        capture_output=True,
        text=True,
        timeout=5,
    )
    return proc.stdout if proc.returncode == 0 else ""


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    sys_env_txt: str | None = None
    if getattr(args, "check_systemd", False):
        sys_env_txt = _read_live_collector_systemd_env()

    if args.command == "verify":
        res = run_proxy_verification(
            proxy_url=args.proxy_url,
            dead_proxy_url=args.dead_proxy_url,
            expected_proxy_ip=args.expected_proxy_ip,
            remote=args.remote,
            verify_remote=args.verify_remote,
            deploy_dir=args.deploy_dir,
            collector_systemd_env_text=sys_env_txt,
            check_live_egress=not args.skip_live_egress,
        )
        print(json.dumps(res, indent=2, ensure_ascii=False))
        return 0 if res["status"] == "ok" else 2

    if args.command == "compare-report":
        ver = run_proxy_verification(
            proxy_url=args.proxy_url,
            expected_proxy_ip=args.expected_proxy_ip,
            remote=args.remote,
            verify_remote=False,
            deploy_dir=args.deploy_dir,
            collector_systemd_env_text=sys_env_txt,
            check_live_egress=not args.skip_live_egress,
        )
        direct_row = None
        proxy_row = None
        if args.run_live_benchmark and args.remote and ver["status"] == "ok":
            direct_row = _measure_rclone_benchmark(
                remote=args.remote,
                proxy_url=None,
                proxy_mode="off",
                payload_bytes=args.benchmark_bytes,
                label="direct",
            )
            proxy_row = _measure_rclone_benchmark(
                remote=args.remote,
                proxy_url=args.proxy_url,
                proxy_mode="required",
                payload_bytes=args.benchmark_bytes,
                label="proxy",
            )
        cost = estimate_lightsail_monthly_cost(
            args.monthly_upload_gb,
            plan_usd=args.plan_usd,
            plan_quota_gb=args.plan_quota_gb,
        )
        md_text = format_compare_report(
            verification=ver,
            direct_transfer=direct_row,
            proxy_transfer=proxy_row,
            cost_estimate=cost,
        )
        out_obj = {
            "status": ver["status"],
            "verification": ver,
            "direct_transfer": direct_row,
            "proxy_transfer": proxy_row,
            "cost_estimate": cost,
            "markdown_report": md_text,
        }
        if args.out_md is not None:
            args.out_md.parent.mkdir(parents=True, exist_ok=True)
            args.out_md.write_text(md_text, encoding="utf-8")
        if args.out_json is not None:
            args.out_json.parent.mkdir(parents=True, exist_ok=True)
            args.out_json.write_text(json.dumps(out_obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(md_text if args.out_json is None else json.dumps(out_obj, indent=2, ensure_ascii=False))
        return 0 if ver["status"] == "ok" else 2

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
