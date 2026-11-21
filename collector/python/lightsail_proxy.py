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
import errno
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
        for line in a_txt.splitlines():
            s = line.strip()
            if s and not s.startswith("#") and "--prune-hours" in s:
                violations.append(f"{archive_unit.name} must keep --prune-hours disabled by default")

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
        pe_txt = proxy_env.read_text(encoding="utf-8")
        p_keys = _extract_env_assignments(pe_txt)
        bad_p = sorted(k for k in p_keys if k in PROXY_ENV_KEYS)
        if bad_p:
            violations.append(f"{proxy_env.name} must not export global {bad_p}")
        if "lightsail_ed25519" in pe_txt:
            violations.append(f"{proxy_env.name} references wrong key name lightsail_ed25519; expected /etc/tokyo-l2/tokyo_l2_lightsail")
        if "TOKYO_L2_LIGHTSAIL_KEY=" in pe_txt and "/etc/tokyo-l2/tokyo_l2_lightsail" not in pe_txt:
            violations.append(f"{proxy_env.name} must configure TOKYO_L2_LIGHTSAIL_KEY=/etc/tokyo-l2/tokyo_l2_lightsail")
        if "REPLACE_WITH_LIGHTSAIL_PUBLIC_IP" in pe_txt:
            violations.append(f"{proxy_env.name} still contains unconfigured placeholder REPLACE_WITH_LIGHTSAIL_PUBLIC_IP")
        if "TOKYO_L2_LIGHTSAIL_HOST=" in pe_txt and not re.search(r"(?m)^\s*TOKYO_L2_LIGHTSAIL_HOST=172\.26\.12\.211\s*$", pe_txt):
            violations.append(f"{proxy_env.name} must set primary TOKYO_L2_LIGHTSAIL_HOST=172.26.12.211")
        if "TOKYO_L2_LIGHTSAIL_HOSTKEY_ALIAS=13.192.29.63" not in pe_txt:
            violations.append(f"{proxy_env.name} must pin TOKYO_L2_LIGHTSAIL_HOSTKEY_ALIAS=13.192.29.63")
        if "TOKYO_L2_LIGHTSAIL_PUBLIC_FALLBACK_HOST=13.192.29.63" not in pe_txt:
            violations.append(f"{proxy_env.name} must retain TOKYO_L2_LIGHTSAIL_PUBLIC_FALLBACK_HOST=13.192.29.63")
        if re.search(r"(?m)^\s*TOKYO_L2_ARCHIVE_PROXY_MODE\s*=\s*fallback_direct\b", pe_txt):
            violations.append(f"{proxy_env.name} must NOT enable fallback_direct")
        if "TOKYO_L2_ARCHIVE_PROXY_MODE=" in pe_txt and not re.search(r"(?m)^\s*TOKYO_L2_ARCHIVE_PROXY_MODE=required\s*$", pe_txt):
            violations.append(f"{proxy_env.name} must enforce TOKYO_L2_ARCHIVE_PROXY_MODE=required")

    if proxy_unit.is_file():
        checked_files.append(str(proxy_unit))
        pu_txt = proxy_unit.read_text(encoding="utf-8")
        for req in (
            "ExitOnForwardFailure=yes",
            "StrictHostKeyChecking=yes",
            "HostKeyAlias=${TOKYO_L2_LIGHTSAIL_HOSTKEY_ALIAS}",
            "BatchMode=yes",
            "Restart=always",
        ):
            if req not in pu_txt:
                violations.append(f"{proxy_unit.name} missing hardened option {req!r}")
        if re.search(r"StrictHostKeyChecking\s*=\s*(?:no|accept-new)", pu_txt):
            violations.append(f"{proxy_unit.name} must never disable or auto-accept SSH host keys")

    guide_doc = deploy_dir / "LIGHTSAIL_PROXY.md"
    if guide_doc.is_file():
        checked_files.append(str(guide_doc))
        gd_txt = guide_doc.read_text(encoding="utf-8")
        if "tokyo_l2_lightsail" not in gd_txt or "lightsail_ed25519" in gd_txt:
            violations.append(f"{guide_doc.name} must reference /etc/tokyo-l2/tokyo_l2_lightsail (not lightsail_ed25519)")
        if "172.26.12.211" not in gd_txt or "13.192.29.63" not in gd_txt or "HostKeyAlias" not in gd_txt:
            violations.append(f"{guide_doc.name} must document private IP 172.26.12.211, public fallback 13.192.29.63, and HostKeyAlias")
        if "1081" not in gd_txt:
            violations.append(f"{guide_doc.name} must document temporary port 1081 pre-cutover verification")
        if "--data-root /data/tokyo_l2" not in gd_txt:
            violations.append(f"{guide_doc.name} must require check-takeover --data-root /data/tokyo_l2")
        if "ssh-keygen -lf" not in gd_txt:
            violations.append(f"{guide_doc.name} must verify host key SHA-256 fingerprint via ssh-keygen -lf before pinning")
        if re.search(r"ssh-keyscan[^\n]*\|[^\n]*tee[^\n]*lightsail_known_hosts", gd_txt):
            violations.append(f"{guide_doc.name} must not pipe ssh-keyscan blindly into lightsail_known_hosts")
        if re.search(r"StrictHostKeyChecking\s*=\s*(?:no|accept-new)", gd_txt):
            violations.append(f"{guide_doc.name} must never instruct auto-accepting unknown SSH host keys")

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


def _default_port_in_use(host: str, port: int) -> dict[str, Any]:
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    bind_failed = False
    bind_error: str | None = None
    bind_errno: int | None = None
    probe_error: str | None = None
    sock = socket.socket(family, socket.SOCK_STREAM)
    try:
        sock.bind((host, int(port)))
    except OSError as exc:
        bind_errno = exc.errno
        bind_error = str(exc)
        if exc.errno in (errno.EADDRINUSE, getattr(errno, "WSAEADDRINUSE", errno.EADDRINUSE)):
            bind_failed = True
        else:
            probe_error = f"bind probe error (errno={exc.errno}): {exc}"
    finally:
        try:
            sock.close()
        except Exception:
            pass

    connect_ok = False
    connect_host = "127.0.0.1" if host in ("", "0.0.0.0") else host
    try:
        with socket.create_connection((connect_host, int(port)), timeout=0.5):
            connect_ok = True
    except OSError:
        connect_ok = False

    if probe_error and connect_ok:
        # Another user's non-privileged port (<1024) or bind restriction still reachable -> port is in use
        probe_error = None
        bind_failed = True

    return {
        "host": host,
        "port": int(port),
        "in_use": bool(bind_failed or connect_ok),
        "bind_failed": bind_failed,
        "bind_errno": bind_errno,
        "bind_error": bind_error,
        "connect_reachable": connect_ok,
        "probe_error": probe_error,
    }


def _addr_conflicts_with_host(local_addr: str, host: str, port: int) -> bool:
    s = local_addr.strip()
    if ":" not in s:
        return False
    addr_part, port_part = s.rsplit(":", 1)
    if port_part != str(int(port)):
        return False
    addr_clean = addr_part.strip("[]").split("%", 1)[0].lower()
    target = host.strip("[]").lower()
    if addr_clean in ("*", "0.0.0.0", "::", ""):
        return True
    if target in ("", "0.0.0.0", "::", "*"):
        return True
    return addr_clean == target


def _default_port_listener_owners(host: str, port: int) -> dict[str, Any]:
    port_i = int(port)
    try:
        proc = subprocess.run(
            ["ss", "-H", "-ltnp", f"sport = :{port_i}"],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (FileNotFoundError, OSError, subprocess.TimeoutExpired) as exc:
        return {
            "available": False,
            "pids": [],
            "entries": [],
            "pid_visibility_denied": False,
            "error": str(exc),
        }
    if proc.returncode != 0:
        return {
            "available": False,
            "pids": [],
            "entries": [],
            "pid_visibility_denied": False,
            "error": (proc.stderr or proc.stdout or f"rc={proc.returncode}").strip(),
        }

    pids: list[int] = []
    entries: list[dict[str, Any]] = []
    pid_visibility_denied = False

    for raw_line in proc.stdout.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) < 4:
            continue
        local_addr = parts[3]
        if not _addr_conflicts_with_host(local_addr, host, port_i):
            continue
        proc_matches = re.findall(r'\("([^"]+)",pid=(\d+)(?:,fd=\d+)?\)', line)
        line_pids = [int(pid_s) for _, pid_s in proc_matches]
        line_procs = [{"comm": comm, "pid": int(pid_s)} for comm, pid_s in proc_matches]
        if not line_pids:
            pid_visibility_denied = True
        for pid in line_pids:
            if pid not in pids:
                pids.append(pid)
        entries.append(
            {
                "local_address": local_addr,
                "pids": line_pids,
                "processes": line_procs,
                "raw": line,
            }
        )

    return {
        "available": True,
        "pids": pids,
        "entries": entries,
        "pid_visibility_denied": pid_visibility_denied,
        "error": None,
    }


def _pid_in_control_group(pid: int, control_group: str | None) -> bool:
    if not control_group:
        return False
    cg_path = Path(f"/proc/{int(pid)}/cgroup")
    try:
        content = cg_path.read_text(encoding="utf-8")
    except OSError:
        return False
    for line in content.splitlines():
        if ":" in line:
            path_part = line.rsplit(":", 1)[-1].strip()
            if path_part == control_group or path_part.startswith(f"{control_group}/"):
                return True
    return False


def _default_archive_lock_status(data_root: Path) -> dict[str, Any]:
    try:
        root = Path(data_root)
        if not root.exists():
            return {
                "checked": False,
                "data_root": str(root),
                "lock_path": str(root / ".archive_sync.lock"),
                "lock_file_exists": False,
                "lock_held": False,
                "error": f"data_root does not exist: {root}",
            }
        if not root.is_dir():
            return {
                "checked": False,
                "data_root": str(root),
                "lock_path": str(root / ".archive_sync.lock"),
                "lock_file_exists": False,
                "lock_held": False,
                "error": f"data_root is not a directory: {root}",
            }
        if not os.access(root, os.R_OK | os.X_OK):
            return {
                "checked": False,
                "data_root": str(root),
                "lock_path": str(root / ".archive_sync.lock"),
                "lock_file_exists": False,
                "lock_held": False,
                "error": f"data_root is not readable/traversable: {root}",
            }
    except OSError as exc:
        return {
            "checked": False,
            "data_root": str(data_root),
            "lock_path": None,
            "lock_file_exists": False,
            "lock_held": False,
            "error": f"data_root stat failed: {exc}",
        }

    lock_file = root / ".archive_sync.lock"
    try:
        os.stat(lock_file)
    except FileNotFoundError:
        return {
            "checked": True,
            "data_root": str(root),
            "lock_path": str(lock_file),
            "lock_file_exists": False,
            "lock_held": False,
            "error": None,
        }
    except OSError as exc:
        return {
            "checked": False,
            "data_root": str(root),
            "lock_path": str(lock_file),
            "lock_file_exists": False,
            "lock_held": False,
            "error": f"cannot stat lock file {lock_file}: {exc}",
        }

    try:
        import fcntl
    except ImportError as exc:
        return {
            "checked": False,
            "data_root": str(root),
            "lock_path": str(lock_file),
            "lock_file_exists": True,
            "lock_held": False,
            "error": f"fcntl unavailable: {exc}",
        }

    fd: int | None = None
    acquired = False
    try:
        fd = os.open(str(lock_file), os.O_RDONLY)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        acquired = True
        return {
            "checked": True,
            "data_root": str(root),
            "lock_path": str(lock_file),
            "lock_file_exists": True,
            "lock_held": False,
            "error": None,
        }
    except BlockingIOError:
        return {
            "checked": True,
            "data_root": str(root),
            "lock_path": str(lock_file),
            "lock_file_exists": True,
            "lock_held": True,
            "error": None,
        }
    except OSError as exc:
        if exc.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
            return {
                "checked": True,
                "data_root": str(root),
                "lock_path": str(lock_file),
                "lock_file_exists": True,
                "lock_held": True,
                "error": None,
            }
        return {
            "checked": False,
            "data_root": str(root),
            "lock_path": str(lock_file),
            "lock_file_exists": True,
            "lock_held": False,
            "error": f"cannot verify archive lock {lock_file}: {exc}",
        }
    finally:
        if fd is not None:
            if acquired:
                try:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                except OSError:
                    pass
            try:
                os.close(fd)
            except OSError:
                pass


def _default_systemd_unit_state(unit: str) -> dict[str, Any]:
    try:
        proc = subprocess.run(
            [
                "systemctl",
                "show",
                unit,
                "--property=LoadState",
                "--property=ActiveState",
                "--property=SubState",
                "--property=MainPID",
                "--property=ControlGroup",
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (FileNotFoundError, OSError, subprocess.TimeoutExpired) as exc:
        return {
            "unit": unit,
            "available": False,
            "load_state": "unknown",
            "active_state": "unknown",
            "sub_state": "unknown",
            "main_pid": None,
            "control_group": None,
            "error": str(exc),
        }
    if proc.returncode != 0:
        return {
            "unit": unit,
            "available": False,
            "load_state": "unknown",
            "active_state": "unknown",
            "sub_state": "unknown",
            "main_pid": None,
            "control_group": None,
            "error": (proc.stderr or proc.stdout or f"rc={proc.returncode}").strip(),
        }
    vals: dict[str, str] = {}
    for line in proc.stdout.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            vals[k.strip()] = v.strip()
    load_state = vals.get("LoadState", "unknown") or "unknown"
    active_state = vals.get("ActiveState", "unknown") or "unknown"
    sub_state = vals.get("SubState", "unknown") or "unknown"
    raw_pid = vals.get("MainPID", "").strip()
    main_pid: int | None = int(raw_pid) if raw_pid.isdigit() and int(raw_pid) > 0 else None
    control_group = vals.get("ControlGroup", "").strip() or None
    if active_state == "unknown":
        return {
            "unit": unit,
            "available": False,
            "load_state": load_state,
            "active_state": "unknown",
            "sub_state": sub_state,
            "main_pid": main_pid,
            "control_group": control_group,
            "error": "systemctl show returned unknown/empty ActiveState",
        }
    return {
        "unit": unit,
        "available": True,
        "load_state": load_state,
        "active_state": active_state,
        "sub_state": sub_state,
        "main_pid": main_pid,
        "control_group": control_group,
    }


def check_takeover_readiness(
    *,
    host: str = "127.0.0.1",
    port: int = 1080,
    proxy_unit: str = SYSTEMD_PROXY_UNIT,
    archive_unit: str = SYSTEMD_ARCHIVE_UNIT,
    data_root: Path | None = None,
    port_probe_fn: Callable[[str, int], dict[str, Any]] | None = None,
    unit_state_fn: Callable[[str], dict[str, Any]] | None = None,
    lock_check_fn: Callable[[Path], dict[str, Any]] | None = None,
    listener_owner_fn: Callable[[str, int], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    probe_fn = port_probe_fn or _default_port_in_use
    state_fn = unit_state_fn or _default_systemd_unit_state
    owner_fn = listener_owner_fn or _default_port_listener_owners

    blocking_reasons: list[str] = []

    if data_root is None:
        lock_status: dict[str, Any] = {
            "checked": False,
            "data_root": None,
            "lock_path": None,
            "lock_file_exists": False,
            "lock_held": False,
            "error": "data_root is required (--data-root /data/tokyo_l2)",
        }
        blocking_reasons.append("missing_data_root")
    else:
        check_lock = lock_check_fn or _default_archive_lock_status
        lock_status = dict(check_lock(Path(data_root)))
        if not lock_status.get("checked"):
            blocking_reasons.append("archive_lock_check_failed")
        elif lock_status.get("lock_held"):
            blocking_reasons.append("archive_active")

    port_probe = dict(probe_fn(host, int(port)))
    listener_probe = dict(owner_fn(host, int(port)))
    proxy_state = dict(state_fn(proxy_unit))
    archive_state = dict(state_fn(archive_unit))

    if port_probe.get("probe_error") or port_probe.get("error"):
        blocking_reasons.append("port_probe_failed")

    if "in_use" not in port_probe and "reachable" not in port_probe:
        blocking_reasons.append("port_probe_failed")
        raw_port_in_use = True
    else:
        raw_port_in_use = bool(port_probe.get("in_use", port_probe.get("reachable", False)))

    port_in_use = bool(raw_port_in_use or bool(listener_probe.get("entries")))

    proxy_avail = bool(proxy_state.get("available"))
    archive_avail = bool(archive_state.get("available"))
    proxy_load = str(proxy_state.get("load_state", "loaded") or "unknown")
    archive_load = str(archive_state.get("load_state", "loaded") or "unknown")
    proxy_active = str(proxy_state.get("active_state") or "unknown")
    archive_active = str(archive_state.get("active_state") or "unknown")

    if not proxy_avail or not archive_avail:
        blocking_reasons.append("systemd_unavailable")
    else:
        if archive_active == "active":
            if "archive_active" not in blocking_reasons:
                blocking_reasons.append("archive_active")
        elif archive_active != "inactive" or archive_load in ("error", "bad-setting", "not-found", "masked", "unknown"):
            blocking_reasons.append("archive_state_unclear")

        if proxy_active not in ("active", "inactive") or proxy_load in ("error", "bad-setting", "masked", "unknown"):
            blocking_reasons.append("proxy_state_unclear")

    archive_lock_held = bool(lock_status.get("lock_held"))
    archive_busy = (archive_active == "active") or archive_lock_held
    proxy_systemd_active = proxy_avail and (proxy_active == "active")

    listener_pids = [int(p) for p in listener_probe.get("pids") or []]
    proxy_main_pid = proxy_state.get("main_pid")
    proxy_cgroup = proxy_state.get("control_group")
    matched_listener_pids: list[int] = []
    foreign_listener_pids: list[int] = []
    port_owned_by_proxy_unit = False

    if port_in_use:
        if proxy_systemd_active:
            if (
                not listener_probe.get("available")
                or listener_probe.get("pid_visibility_denied")
                or proxy_main_pid is None
                or not listener_pids
            ):
                blocking_reasons.append("port_owner_unverified")
            else:
                for pid in listener_pids:
                    if (proxy_main_pid is not None and pid == int(proxy_main_pid)) or _pid_in_control_group(pid, proxy_cgroup):
                        matched_listener_pids.append(pid)
                    else:
                        foreign_listener_pids.append(pid)
                if matched_listener_pids and not foreign_listener_pids:
                    port_owned_by_proxy_unit = True
                else:
                    blocking_reasons.append("port_owned_by_different_process")
        elif "systemd_unavailable" not in blocking_reasons:
            blocking_reasons.append("port_occupied_by_non_systemd_process")
        else:
            blocking_reasons.append("port_occupied")
    elif proxy_systemd_active:
        blocking_reasons.append("proxy_unit_active_but_port_not_listening")

    already_managed_by_systemd = bool(port_in_use and proxy_systemd_active and port_owned_by_proxy_unit)
    ready_for_systemd_takeover = bool((not blocking_reasons) and (not port_in_use) and (not archive_busy))
    conflict: str | None = blocking_reasons[0] if blocking_reasons else None
    status = (
        "ok"
        if (not blocking_reasons and (ready_for_systemd_takeover or already_managed_by_systemd))
        else "blocked"
    )

    return {
        "status": status,
        "host": host,
        "port": int(port),
        "port_in_use": port_in_use,
        "port_probe": port_probe,
        "port_listeners": listener_probe,
        "matched_listener_pids": matched_listener_pids,
        "foreign_listener_pids": foreign_listener_pids,
        "port_owned_by_proxy_unit": port_owned_by_proxy_unit,
        "proxy_unit": proxy_unit,
        "proxy_unit_state": proxy_state,
        "archive_unit": archive_unit,
        "archive_unit_state": archive_state,
        "data_root": str(data_root) if data_root is not None else None,
        "archive_lock_status": lock_status,
        "archive_lock_path": lock_status.get("lock_path"),
        "archive_lock_held": archive_lock_held,
        "archive_busy": archive_busy,
        "already_managed_by_systemd": already_managed_by_systemd,
        "ready_for_systemd_takeover": ready_for_systemd_takeover,
        "conflict": conflict,
        "blocking_reasons": blocking_reasons,
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
    transfer_mode: str = "vpc_peering_same_az",
    ec2_to_lightsail_per_gb: float | None = None,
    ec2_to_lightsail_public_per_gb: float = 0.01,
    ec2_to_lightsail_cross_az_per_gb: float = 0.01,
    ec2_az: str = "ap-northeast-1a",
    lightsail_az: str = "ap-northeast-1a",
    same_physical_az: bool = True,
    lightsail_overage_per_gb: float = 0.14,
) -> dict[str, Any]:
    """Estimate EC2 direct vs Lightsail relay egress costs (accounting for Lightsail 2x IN+OUT quota).

    Defaults to same-physical-AZ (`ap-northeast-1a`) VPC Peering ($0.00/GB EC2 -> Lightsail) while
    retaining the $0.01/GB public IPv4 comparison path. Monthly volume is treated strictly as a
    hypothetical modeling scenario (not measured live traffic), and subsequent AWS bills serve only
    for post-cutover reconciliation.
    """
    mode_norm = str(transfer_mode or "vpc_peering_same_az").strip().lower()
    if mode_norm not in ("vpc_peering_same_az", "public_ipv4"):
        raise ValueError(
            f"Unsupported transfer_mode {transfer_mode!r}; expected 'vpc_peering_same_az' or 'public_ipv4'"
        )

    up_gb = max(0.0, float(monthly_upload_gb))
    direct_billable_gb = max(0.0, up_gb - max(0.0, float(ec2_free_gb)))
    direct_cost_usd = round(direct_billable_gb * float(ec2_direct_per_gb), 2)

    # Lightsail counts BOTH inbound (from EC2) and outbound (to Google Drive) against monthly transfer quota
    lightsail_quota_consumed_gb = round(up_gb * 2.0, 2)
    lightsail_overage_gb = max(0.0, round(lightsail_quota_consumed_gb - float(plan_quota_gb), 2))
    lightsail_overage_usd = round(lightsail_overage_gb * float(lightsail_overage_per_gb), 2)

    same_az_peering_rate = 0.0 if same_physical_az else float(ec2_to_lightsail_cross_az_per_gb)
    same_az_peering_transfer_usd = round(up_gb * same_az_peering_rate, 2)
    same_az_peering_total_usd = round(float(plan_usd) + lightsail_overage_usd + same_az_peering_transfer_usd, 2)

    public_rate = float(ec2_to_lightsail_public_per_gb)
    public_transfer_usd = round(up_gb * public_rate, 2)
    public_total_usd = round(float(plan_usd) + lightsail_overage_usd + public_transfer_usd, 2)

    if ec2_to_lightsail_per_gb is not None:
        selected_rate = float(ec2_to_lightsail_per_gb)
        billing_condition = f"Caller explicit EC2->Lightsail override (${selected_rate}/GB)."
    elif mode_norm == "vpc_peering_same_az":
        selected_rate = same_az_peering_rate
        if same_physical_az:
            billing_condition = (
                f"Same-physical-AZ VPC Peering ({ec2_az} -> {lightsail_az} via 172.26.12.211): "
                "$0/GB under AWS VPC Peering same-AZ rule (cross-physical-AZ or public fallback is billed)."
            )
        else:
            billing_condition = (
                f"Cross-physical-AZ VPC Peering ({ec2_az} -> {lightsail_az}, same_physical_az=False): "
                f"NOT eligible for $0/GB same-AZ transfer; estimated at ${selected_rate}/GB."
            )
    else:
        selected_rate = public_rate
        billing_condition = (
            f"Public IPv4 fallback comparison (13.192.29.63, EC2={ec2_az}, Lightsail={lightsail_az}): "
            f"estimated at ${selected_rate}/GB."
        )

    ec2_relay_transfer_usd = round(up_gb * selected_rate, 2)
    total_lightsail_est = round(float(plan_usd) + lightsail_overage_usd + ec2_relay_transfer_usd, 2)

    return {
        "monthly_upload_gb": up_gb,
        "monthly_upload_is_hypothetical_sample": True,
        "volume_disclaimer": (
            f"Hypothetical modeling volume ({up_gb} GB/month scenario only; NOT measured live production collection rate)."
        ),
        "ec2_direct": {
            "account_free_tier_gb": float(ec2_free_gb),
            "rate_per_gb_usd": float(ec2_direct_per_gb),
            "billable_gb": direct_billable_gb,
            "estimated_cost_usd": direct_cost_usd,
        },
        "lightsail_relay": {
            "transfer_mode": mode_norm,
            "plan_usd": float(plan_usd),
            "plan_quota_gb": float(plan_quota_gb),
            "quota_note": "Lightsail counts BOTH inbound (from EC2) + outbound (to Google Drive) against monthly 1 TB allowance (~2x uploaded volume)",
            "quota_consumed_gb": lightsail_quota_consumed_gb,
            "overage_gb": lightsail_overage_gb,
            "overage_usd": lightsail_overage_usd,
            "ec2_az": ec2_az,
            "lightsail_az": lightsail_az,
            "same_physical_az": bool(same_physical_az),
            "billing_condition": billing_condition,
            "reconciliation_note": "Subsequent AWS Cost Explorer bills are used only for post-cutover reconciliation.",
            "assumed_ec2_to_lightsail_per_gb_usd": selected_rate,
            "estimated_ec2_to_lightsail_usd": ec2_relay_transfer_usd,
            "estimated_total_usd": total_lightsail_est,
            "billing_gate_verified": False,
        },
        "scenarios": {
            "vpc_peering_same_az": {
                "ssh_target": "172.26.12.211",
                "ec2_az": ec2_az,
                "lightsail_az": lightsail_az,
                "same_physical_az": bool(same_physical_az),
                "ec2_to_lightsail_per_gb_usd": same_az_peering_rate,
                "estimated_ec2_to_lightsail_usd": same_az_peering_transfer_usd,
                "quota_consumed_gb": lightsail_quota_consumed_gb,
                "overage_gb": lightsail_overage_gb,
                "overage_usd": lightsail_overage_usd,
                "estimated_total_usd": same_az_peering_total_usd,
                "condition": (
                    "AWS VPC Peering same-physical-AZ intra-region transfer ($0/GB)"
                    if same_physical_az
                    else "Cross-physical-AZ VPC Peering does NOT qualify for $0/GB"
                ),
            },
            "public_ipv4": {
                "ssh_target": "13.192.29.63",
                "ec2_to_lightsail_per_gb_usd": public_rate,
                "estimated_ec2_to_lightsail_usd": public_transfer_usd,
                "quota_consumed_gb": lightsail_quota_consumed_gb,
                "overage_gb": lightsail_overage_gb,
                "overage_usd": lightsail_overage_usd,
                "estimated_total_usd": public_total_usd,
                "condition": "Same-region public IPv4 comparison ($0.01/GB)",
            },
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
    scenarios = cost_estimate.get("scenarios") or {}
    priv_sc = scenarios.get("vpc_peering_same_az") or {}
    pub_sc = scenarios.get("public_ipv4") or {}

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
            "## 5. Monthly Egress Cost & Lightsail Quota Model (Hypothetical Scenario; Post-Cutover Bill Reconciliation Only)",
            f"- **Hypothetical Modeling Volume (NOT Measured Collection Traffic)**: `{cost_estimate.get('monthly_upload_gb')} GB` / month (`{cost_estimate.get('volume_disclaimer')}`)",
            f"- **Physical AZ Topology & Transfer Condition**: EC2 AZ=`{ls_est.get('ec2_az')}`, Lightsail AZ=`{ls_est.get('lightsail_az')}`, "
            f"`same_physical_az={ls_est.get('same_physical_az')}`, selected mode=`{ls_est.get('transfer_mode')}` — {ls_est.get('billing_condition')}",
            f"- **EC2 Direct Baseline**: `{ec2_est.get('billable_gb')} GB` billable × `${ec2_est.get('rate_per_gb_usd')}/GB` = **`${ec2_est.get('estimated_cost_usd')}`**",
            f"- **Lightsail Fixed Plan & 2× Quota (`Ingress + Egress`)**: `${ls_est.get('plan_usd')}/month` for `{ls_est.get('plan_quota_gb')} GB` (`1 TB`); "
            f"quota consumed=`{ls_est.get('quota_consumed_gb')} GB`, overage=`{ls_est.get('overage_gb')} GB`, overage_cost=`${ls_est.get('overage_usd')}`",
            f"- **Scenario A — Primary Private VPC Peering (`172.26.12.211`, same physical AZ `1a`)**: "
            f"EC2→Lightsail `${priv_sc.get('ec2_to_lightsail_per_gb_usd')}/GB` (`${priv_sc.get('estimated_ec2_to_lightsail_usd')}`) + plan/overage = **`${priv_sc.get('estimated_total_usd')}`**",
            f"- **Scenario B — Public IPv4 Fallback Comparison (`13.192.29.63`)**: "
            f"EC2→Lightsail `${pub_sc.get('ec2_to_lightsail_per_gb_usd')}/GB` (`${pub_sc.get('estimated_ec2_to_lightsail_usd')}`) + plan/overage = **`${pub_sc.get('estimated_total_usd')}`**",
            f"- **Selected Mode Total Estimate (`{ls_est.get('transfer_mode')}`)**: **`${ls_est.get('estimated_total_usd')}`**",
            f"- **Reconciliation Note**: {ls_est.get('reconciliation_note')} (`billing_gate_verified={ls_est.get('billing_gate_verified')}`; keep `--prune-hours 24` disabled independently).",
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
    p_rep.add_argument(
        "--transfer-mode",
        choices=("vpc_peering_same_az", "public_ipv4"),
        default="vpc_peering_same_az",
        help="Primary EC2 -> Lightsail path for estimate (default: same-AZ VPC Peering $0/GB)",
    )
    p_rep.add_argument("--ec2-az", default="ap-northeast-1a")
    p_rep.add_argument("--lightsail-az", default="ap-northeast-1a")
    p_rep.add_argument(
        "--cross-physical-az",
        action="store_true",
        help="Set when EC2 and Lightsail are NOT in the same physical AZ (disables $0/GB VPC Peering rate)",
    )
    p_rep.add_argument("--deploy-dir", type=Path, default=DEPLOY_DIR)
    p_rep.add_argument("--skip-live-egress", action="store_true")
    p_rep.add_argument("--out-md", type=Path, default=None)
    p_rep.add_argument("--out-json", type=Path, default=None)

    p_takeover = sub.add_parser(
        "check-takeover",
        help="Fail-closed readiness check for controlled systemd takeover of 127.0.0.1:1080",
    )
    p_takeover.add_argument("--host", default="127.0.0.1")
    p_takeover.add_argument("--port", type=int, default=1080)
    p_takeover.add_argument("--proxy-unit", default=SYSTEMD_PROXY_UNIT)
    p_takeover.add_argument("--archive-unit", default=SYSTEMD_ARCHIVE_UNIT)
    p_takeover.add_argument(
        "--data-root",
        type=Path,
        required=True,
        help="Required archive data root directory containing .archive_sync.lock (e.g. /data/tokyo_l2)",
    )

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

    if args.command == "check-takeover":
        res = check_takeover_readiness(
            host=args.host,
            port=args.port,
            proxy_unit=args.proxy_unit,
            archive_unit=args.archive_unit,
            data_root=args.data_root,
        )
        print(json.dumps(res, indent=2, ensure_ascii=False))
        return 0 if res["status"] == "ok" else 2

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
            transfer_mode=args.transfer_mode,
            ec2_az=args.ec2_az,
            lightsail_az=args.lightsail_az,
            same_physical_az=not args.cross_physical_az,
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
