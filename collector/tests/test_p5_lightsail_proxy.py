"""Deterministic offline tests for Phase P5 Lightsail SOCKS5 archive proxy isolation & failover."""
from __future__ import annotations

import json
import os
import socket
import sqlite3
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any

import pytest
import zstandard as zstd

REPO_ROOT = Path(__file__).resolve().parents[2]
COLLECTOR_DIR = REPO_ROOT / "collector"
PYTHON_DIR = COLLECTOR_DIR / "python"
DEPLOY_DIR = COLLECTOR_DIR / "deploy"

if str(PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(PYTHON_DIR))

import archive_sync as arch_mod  # noqa: E402
import cli as cli_mod  # noqa: E402
import lightsail_proxy as lp_mod  # noqa: E402
import meta_daemon as md_mod  # noqa: E402
from archive_sync import (  # noqa: E402
    LEDGER_DB_FILENAME,
    RcloneBackend,
    file_hashes,
    sync_sealed_directory,
)


def _unused_local_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = int(s.getsockname()[1])
    s.close()
    return port


class _LoopbackSocks5Server:
    """Minimal pure-stdlib RFC-1928 SOCKS5 server handling greeting negotiation cleanly."""

    def __init__(self, *, reject_auth: bool = False) -> None:
        self.reject_auth = reject_auth
        self.handshakes = 0
        self._stop = threading.Event()
        self._srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._srv.bind(("127.0.0.1", 0))
        self._srv.listen(16)
        self._srv.settimeout(0.1)
        self.port = int(self._srv.getsockname()[1])
        self._thr = threading.Thread(target=self._loop, daemon=True)
        self._thr.start()

    @property
    def url(self) -> str:
        return f"socks5://127.0.0.1:{self.port}"

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                conn, _ = self._srv.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            try:
                conn.settimeout(1.0)
                hdr = conn.recv(2)
                if len(hdr) == 2 and hdr[0] == 5 and hdr[1] >= 1:
                    _methods = conn.recv(hdr[1])
                    self.handshakes += 1
                    conn.sendall(b"\x05\xff" if self.reject_auth else b"\x05\x00")
            except OSError:
                pass
            finally:
                try:
                    conn.close()
                except OSError:
                    pass

    def stop(self) -> None:
        self._stop.set()
        try:
            self._srv.close()
        except OSError:
            pass
        self._thr.join(timeout=2.0)


def _seed_sealed_partition(
    data_root: Path,
    *,
    exchange: str = "binance_futures",
    date_utc: str = "2026-12-03",
    hour_utc: str = "09",
    age_s: float = 100.0,
) -> tuple[Path, Path]:
    ex_dir = data_root / "raw" / exchange / date_utc
    ex_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{exchange}_{date_utc.replace('-', '')}_{hour_utc}"
    dpath = ex_dir / f"{stem}.tsv.zst"
    mpath = ex_dir / f"{stem}.manifest.json"
    cctx = zstd.ZstdCompressor(level=3)
    dpath.write_bytes(
        cctx.compress(b'1796292000000000000\t1000\tA\t1\t1\tDATA\t{"ok":true}\n')
    )
    dh = file_hashes(dpath)
    manifest = {
        "schema_version": 1,
        "exchange": exchange,
        "date_utc": date_utc,
        "hour_utc": hour_utc,
        "boot_id": "boot_p5",
        "file_name": dpath.name,
        "bytes": dh["bytes"],
        "sha256": dh["sha256"],
        "data_lines": 1,
        "data_escaped_lines": 0,
        "meta_lines": 0,
        "local_loss_lines": 0,
        "gap_start_lines": 0,
        "switch_lines": 0,
    }
    mpath.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    ts = 1_796_292_720.0 - age_s
    os.utime(dpath, (ts, ts))
    os.utime(mpath, (ts, ts))
    return dpath, mpath


def test_p5_proxy_url_validation_and_subprocess_env_isolation(monkeypatch: pytest.MonkeyPatch) -> None:
    """Proxy config rejects malformed schemes/modes and isolates env strictly to rclone child dict."""
    for bad in ("", "http://127.0.0.1:1080", "socks5://127.0.0.1", "socks5://:1080", "socks5://127.0.0.1:99999"):
        with pytest.raises(ValueError):
            lp_mod.parse_proxy_url(bad)

    assert lp_mod.parse_proxy_url("socks5://127.0.0.1:1080") == ("socks5", "127.0.0.1", 1080)
    assert lp_mod.parse_proxy_url("socks5h://127.0.0.1:1081") == ("socks5h", "127.0.0.1", 1081)

    with pytest.raises(ValueError):
        lp_mod.resolve_rclone_proxy_config(proxy_mode="invalid_mode")

    # Off mode returns None child env override
    assert lp_mod.build_rclone_subprocess_env(None) is None

    # Active SOCKS5 injects proxy env ONLY in returned dict and strips NO_PROXY without mutating caller env
    base = {"PATH": "/usr/bin", "NO_PROXY": "*", "no_proxy": "*", "TOKYO_L2_KEEP": "yes"}
    child = lp_mod.build_rclone_subprocess_env("socks5://127.0.0.1:1080", base_env=base)
    assert child is not None
    assert child["HTTP_PROXY"] == "socks5://127.0.0.1:1080"
    assert child["HTTPS_PROXY"] == "socks5://127.0.0.1:1080"
    assert child["ALL_PROXY"] == "socks5://127.0.0.1:1080"
    assert "NO_PROXY" not in child and "no_proxy" not in child
    assert child["TOKYO_L2_KEEP"] == "yes"
    assert "HTTPS_PROXY" not in base

    # Direct fallback clears inherited proxy vars cleanly
    leaked = {"PATH": "/usr/bin", "HTTPS_PROXY": "socks5://127.0.0.1:9999", "http_proxy": "socks5://127.0.0.1:9999"}
    cleaned = lp_mod.build_rclone_subprocess_env(None, clear_proxy=True, base_env=leaked)
    assert cleaned is not None
    for k in lp_mod.PROXY_ENV_KEYS:
        assert k not in cleaned


def test_p5_required_mode_fail_closed_preserves_local_files_and_sends_direct_hc_fail(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Dead SOCKS5 under `required` aborts before rclone, preserves aged local data, and pings HC /fail directly."""
    data_root = tmp_path / "tokyo_l2"
    dpath, mpath = _seed_sealed_partition(data_root, age_s=48 * 3600.0)
    orig_bytes = dpath.read_bytes()

    dead_port = _unused_local_port()
    for k in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("TOKYO_L2_ARCHIVE_PROXY_MODE", "required")
    monkeypatch.setenv("TOKYO_L2_ARCHIVE_PROXY_URL", f"socks5://127.0.0.1:{dead_port}")
    monkeypatch.setenv(
        "TOKYO_L2_HEALTHCHECKS_ARCHIVE_URL",
        "https://hc-ping.com/11111111-2222-3333-4444-555555555555",
    )

    rclone_calls: list[list[str]] = []

    def _forbid_rclone(*args: Any, **kwargs: Any) -> Any:
        rclone_calls.append(list(args[0]))
        raise AssertionError("rclone subprocess MUST NOT be invoked when required SOCKS5 handshake fails")

    monkeypatch.setattr(arch_mod.subprocess, "run", _forbid_rclone)

    hc_events: list[dict[str, Any]] = []

    def _capture_hc(url: str, *, ok: bool, reason: str = "", **kwargs: Any) -> bool:
        # Critical isolation check: HC sender must run with NO SOCKS5 proxy injected in process env!
        hc_events.append(
            {
                "url": url,
                "ok": ok,
                "reason": reason,
                "process_https_proxy": os.environ.get("HTTPS_PROXY"),
                "process_all_proxy": os.environ.get("ALL_PROXY"),
            }
        )
        return True

    monkeypatch.setattr(md_mod, "ping_healthcheck", _capture_hc)

    # Invoke CLI archive handler even with --prune-hours 24 -> MUST fail closed and preserve local files!
    ns = cli_mod.build_parser().parse_args(
        ["archive", "--dir", str(data_root), "--verify-remote", "--prune-hours", "24"]
    )
    rc = cli_mod.cmd_archive(ns)
    assert rc == 2
    assert rclone_calls == []
    assert dpath.is_file() and dpath.read_bytes() == orig_bytes
    assert mpath.is_file()
    assert len(hc_events) == 1
    assert hc_events[0]["ok"] is False
    assert "ProxyRequiredError" in hc_events[0]["reason"]
    assert hc_events[0]["process_https_proxy"] is None
    assert hc_events[0]["process_all_proxy"] is None


def test_p5_required_mode_live_socks5_routes_only_rclone_subprocesses(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When SOCKS5 is reachable under `required`, ONLY rclone child env gets proxy URL while HC stays direct."""
    socks = _LoopbackSocks5Server()
    try:
        data_root = tmp_path / "tokyo_l2"
        dpath, mpath = _seed_sealed_partition(data_root)
        remote_store: dict[str, bytes] = {}
        observed_subprocs: list[dict[str, Any]] = []

        for k in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
            monkeypatch.delenv(k, raising=False)
        monkeypatch.setenv("TOKYO_L2_ARCHIVE_PROXY_MODE", "required")
        monkeypatch.setenv("TOKYO_L2_ARCHIVE_PROXY_URL", socks.url)
        monkeypatch.setenv(
            "TOKYO_L2_HEALTHCHECKS_ARCHIVE_URL",
            "https://hc-ping.com/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        )

        def _fake_rclone(cmd: list[str], *args: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
            env = kwargs.get("env")
            observed_subprocs.append({"cmd": cmd, "env": env})
            assert env is not None
            assert env.get("HTTPS_PROXY") == socks.url
            assert env.get("HTTP_PROXY") == socks.url
            assert env.get("ALL_PROXY") == socks.url
            assert "NO_PROXY" not in env

            op = cmd[1]
            if op == "lsjson":
                target = cmd[-1]
                rel = target.split("gdrive_main:tokyo_l2_data/", 1)[1]
                blob = remote_store.get(rel)
                if blob is None:
                    return subprocess.CompletedProcess(cmd, 4, stdout="", stderr="object not found")
                import hashlib

                item = [
                    {
                        "Name": Path(rel).name,
                        "Size": len(blob),
                        "ID": f"drive_{Path(rel).name}",
                        "Hashes": {
                            "sha256": hashlib.sha256(blob).hexdigest(),
                            "md5": hashlib.md5(blob).hexdigest(),
                        },
                    }
                ]
                return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps(item), stderr="")
            if op == "copy":
                local_file = Path(cmd[-2])
                dest_dir = cmd[-1].split("gdrive_main:tokyo_l2_data/", 1)[1].rstrip("/")
                rel = f"{dest_dir}/{local_file.name}" if dest_dir else local_file.name
                remote_store[rel] = local_file.read_bytes()
                return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
            raise AssertionError(f"Unexpected rclone op: {cmd}")

        monkeypatch.setattr(arch_mod.subprocess, "run", _fake_rclone)
        hc_calls: list[dict[str, Any]] = []

        def _capture_hc(url: str, *, ok: bool, reason: str = "", **kwargs: Any) -> bool:
            hc_calls.append({"ok": ok, "reason": reason, "parent_https": os.environ.get("HTTPS_PROXY")})
            return True

        monkeypatch.setattr(md_mod, "ping_healthcheck", _capture_hc)

        ns = cli_mod.build_parser().parse_args(
            ["archive", "--dir", str(data_root), "--verify-remote"]
        )
        rc = cli_mod.cmd_archive(ns)
        assert rc == 0
        assert socks.handshakes >= 1
        assert len(observed_subprocs) >= 4
        # Parent Python process never leaked HTTPS_PROXY!
        assert os.environ.get("HTTPS_PROXY") is None
        assert len(hc_calls) == 1
        assert hc_calls[0]["ok"] is True
        assert "proxy=lightsail_proxy" in hc_calls[0]["reason"]
        assert hc_calls[0]["parent_https"] is None
        assert dpath.is_file() and mpath.is_file()
    finally:
        socks.stop()


def test_p5_fallback_direct_mode_audits_event_and_strips_proxy_env(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Explicit `fallback_direct` on dead proxy strips proxy env for rclone, logs SQLite event, and pings HC cleanly."""
    data_root = tmp_path / "tokyo_l2"
    _seed_sealed_partition(data_root)
    dead_port = _unused_local_port()
    remote_store: dict[str, bytes] = {}
    observed_envs: list[dict[str, str] | None] = []

    # Simulate stale ambient proxy env to prove fallback explicitly clears it from rclone subprocess
    monkeypatch.setenv("HTTPS_PROXY", f"socks5://127.0.0.1:{dead_port}")
    monkeypatch.setenv("TOKYO_L2_ARCHIVE_PROXY_MODE", "fallback_direct")
    monkeypatch.setenv("TOKYO_L2_ARCHIVE_PROXY_URL", f"socks5://127.0.0.1:{dead_port}")

    def _fake_rclone(cmd: list[str], *args: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        env = kwargs.get("env")
        observed_envs.append(env)
        assert env is not None
        for k in lp_mod.PROXY_ENV_KEYS:
            assert k not in env

        op = cmd[1]
        if op == "lsjson":
            rel = cmd[-1].split("gdrive_main:tokyo_l2_data/", 1)[1]
            blob = remote_store.get(rel)
            if blob is None:
                return subprocess.CompletedProcess(cmd, 4, stdout="", stderr="not found")
            import hashlib

            return subprocess.CompletedProcess(
                cmd,
                0,
                stdout=json.dumps(
                    [
                        {
                            "Size": len(blob),
                            "ID": f"id_{Path(rel).name}",
                            "Hashes": {"sha256": hashlib.sha256(blob).hexdigest()},
                        }
                    ]
                ),
                stderr="",
            )
        if op == "copy":
            local_file = Path(cmd[-2])
            dest_dir = cmd[-1].split("gdrive_main:tokyo_l2_data/", 1)[1].rstrip("/")
            rel = f"{dest_dir}/{local_file.name}" if dest_dir else local_file.name
            remote_store[rel] = local_file.read_bytes()
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        raise AssertionError(cmd)

    monkeypatch.setattr(arch_mod.subprocess, "run", _fake_rclone)

    backend = RcloneBackend("gdrive_main:tokyo_l2_data")
    res = sync_sealed_directory(data_root, backend=backend, verify_remote=True)
    assert res["status"] == "ok"
    assert res["proxy"]["used_route"] == "direct_fallback"
    assert res["proxy"]["fallback_used"] is True
    assert observed_envs

    with sqlite3.connect(data_root / LEDGER_DB_FILENAME) as conn:
        events = [
            row[0]
            for row in conn.execute("SELECT event_type FROM archive_events ORDER BY id ASC").fetchall()
        ]
    assert "PROXY_FALLBACK_DIRECT" in events

    hc_reasons: list[str] = []
    monkeypatch.setattr(md_mod, "ping_healthcheck", lambda u, ok, reason="", **kw: hc_reasons.append(reason) or True)
    md_mod.notify_archive_healthcheck(res, base_url="https://hc-ping.com/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    assert len(hc_reasons) == 1
    assert "proxy=direct_fallback" in hc_reasons[0]


def test_p5_deploy_templates_strict_isolation_and_proxy_verify_cli(tmp_path: Path) -> None:
    """Audit real deploy systemd/env templates, run verification suite & CLI compare-report + billing math."""
    iso_ok = lp_mod.audit_proxy_isolation(DEPLOY_DIR)
    assert iso_ok["isolated"] is True, iso_ok["violations"]

    # Negative isolation guard: global proxy variable leaked onto collector unit must FAIL audit
    iso_bad = lp_mod.audit_proxy_isolation(
        DEPLOY_DIR,
        collector_systemd_env_text='Environment=RUST_LOG=info HTTPS_PROXY=socks5://127.0.0.1:1080\n',
    )
    assert iso_bad["isolated"] is False
    assert any("HTTPS_PROXY" in v for v in iso_bad["violations"])

    socks = _LoopbackSocks5Server()
    socks_rejected = _LoopbackSocks5Server(reject_auth=True)
    dead_port = _unused_local_port()
    try:
        # Rejected auth method (0xFF) MUST report reachable=False
        rej_probe = lp_mod.probe_socks5_proxy("127.0.0.1", socks_rejected.port)
        assert rej_probe["reachable"] is False

        # Full verify with injected egress IPs & remote roundtrip simulation
        ver = lp_mod.run_proxy_verification(
            proxy_url=socks.url,
            dead_proxy_url=f"socks5://127.0.0.1:{dead_port}",
            expected_proxy_ip="13.192.29.63",
            remote="gdrive_main:tokyo_l2_data",
            verify_remote=True,
            deploy_dir=DEPLOY_DIR,
            ip_fetcher=lambda p: "13.192.29.63" if p else "43.206.220.182",
            remote_roundtrip_fn=lambda r, p: {
                "remote": r,
                "proxy": p,
                "bytes": 65536,
                "sha256_match": True,
                "elapsed_s": 0.12,
                "local_source_preserved": True,
            },
            collector_systemd_env_text="Environment=RUST_LOG=info\n",
            check_live_egress=True,
        )
        assert ver["status"] == "ok", ver["failed_items"]
        assert ver["socks5_probe"]["reachable"] is True
        assert ver["egress"]["ip_distinct"] is True
        assert ver["fail_closed_probe"]["blocked"] is True
        assert ver["fail_closed_probe"]["local_source_preserved"] is True
        assert ver["fallback_probe"]["used_route"] == "direct_fallback"

        # Cost math check: default same-AZ (1a) VPC Peering is $0/GB EC2->Lightsail; public fallback ($0.01/GB) preserved as comparison
        cost = lp_mod.estimate_lightsail_monthly_cost(500.0, plan_usd=5.0, plan_quota_gb=1024.0)
        assert cost["monthly_upload_is_hypothetical_sample"] is True
        assert cost["ec2_direct"]["billable_gb"] == 400.0
        assert cost["ec2_direct"]["estimated_cost_usd"] == 45.6
        assert cost["lightsail_relay"]["transfer_mode"] == "vpc_peering_same_az"
        assert cost["lightsail_relay"]["same_physical_az"] is True
        assert cost["lightsail_relay"]["quota_consumed_gb"] == 1000.0
        assert cost["lightsail_relay"]["overage_gb"] == 0.0
        assert cost["lightsail_relay"]["assumed_ec2_to_lightsail_per_gb_usd"] == 0.0
        assert cost["lightsail_relay"]["estimated_ec2_to_lightsail_usd"] == 0.0
        assert cost["lightsail_relay"]["estimated_total_usd"] == 5.0
        assert cost["lightsail_relay"]["billing_gate_verified"] is False
        assert cost["scenarios"]["vpc_peering_same_az"]["estimated_total_usd"] == 5.0
        assert cost["scenarios"]["public_ipv4"]["estimated_ec2_to_lightsail_usd"] == 5.0
        assert cost["scenarios"]["public_ipv4"]["estimated_total_usd"] == 10.0

        # Public mode & cross-AZ edge checks + overage beyond 1 TB (2x IN+OUT quota)
        cost_pub = lp_mod.estimate_lightsail_monthly_cost(
            500.0, transfer_mode="public_ipv4", plan_usd=5.0, plan_quota_gb=1024.0
        )
        assert cost_pub["lightsail_relay"]["estimated_ec2_to_lightsail_usd"] == 5.0
        assert cost_pub["lightsail_relay"]["estimated_total_usd"] == 10.0

        cost_cross = lp_mod.estimate_lightsail_monthly_cost(
            500.0, transfer_mode="vpc_peering_same_az", same_physical_az=False
        )
        assert cost_cross["lightsail_relay"]["assumed_ec2_to_lightsail_per_gb_usd"] == 0.01
        assert cost_cross["lightsail_relay"]["estimated_total_usd"] == 10.0

        cost_over = lp_mod.estimate_lightsail_monthly_cost(600.0, plan_usd=5.0, plan_quota_gb=1024.0)
        assert cost_over["lightsail_relay"]["quota_consumed_gb"] == 1200.0
        assert cost_over["lightsail_relay"]["overage_gb"] == 176.0
        assert cost_over["lightsail_relay"]["overage_usd"] == 24.64
        assert cost_over["lightsail_relay"]["estimated_total_usd"] == 29.64
        assert cost_over["scenarios"]["public_ipv4"]["estimated_total_usd"] == 35.64

        # Real CLI wrapper test (`collector/bin/proxy-verify compare-report --skip-live-egress`)
        out_md = tmp_path / "compare_report.md"
        out_json = tmp_path / "compare_report.json"
        proc = subprocess.run(
            [
                str(COLLECTOR_DIR / "bin" / "proxy-verify"),
                "compare-report",
                "--proxy-url",
                socks.url,
                "--skip-live-egress",
                "--transfer-mode",
                "vpc_peering_same_az",
                "--ec2-az",
                "ap-northeast-1a",
                "--lightsail-az",
                "ap-northeast-1a",
                "--monthly-upload-gb",
                "500",
                "--out-md",
                str(out_md),
                "--out-json",
                str(out_json),
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        rep = json.loads(proc.stdout)
        assert rep["status"] == "ok"
        assert rep["cost_estimate"]["lightsail_relay"]["quota_consumed_gb"] == 1000.0
        assert rep["cost_estimate"]["lightsail_relay"]["estimated_total_usd"] == 5.0
        assert rep["cost_estimate"]["scenarios"]["public_ipv4"]["estimated_total_usd"] == 10.0
        md_txt = out_md.read_text(encoding="utf-8")
        assert "Lightsail" in md_txt
        assert "172.26.12.211" in md_txt
        assert "13.192.29.63" in md_txt
        assert "NOT Measured Collection Traffic" in md_txt
        assert out_json.is_file()
    finally:
        socks.stop()
        socks_rejected.stop()


def test_p5_meta_daemon_tunnel_monitoring_opt_in_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """meta_daemon alerts on Lightsail tunnel down ONLY when proxy mode is enabled, leaving collector HC green."""
    tests_dir = str(Path(__file__).resolve().parent)
    if tests_dir not in sys.path:
        sys.path.insert(0, tests_dir)
    from test_p3_monitoring import FakeTransport, _build_monitor, _seed_exchange_files  # noqa: E402

    data_root = tmp_path / "tokyo_l2"
    cfg = json.loads((COLLECTOR_DIR / "specs" / "config.example.json").read_text(encoding="utf-8"))
    cfg["meta_daemon"]["require_stream_activity"] = False  # proxy-status fixture
    cfg["deployment"]["data_root"] = str(data_root)
    cfg_path = tmp_path / "p5_meta_cfg.json"
    cfg_path.write_text(json.dumps(cfg, indent=2), encoding="utf-8")

    t0 = 1_796_292_720.0
    partials: dict[str, Path] = {}
    for ex in ("binance_futures", "bybit_linear", "bitget_v3", "gate_futures"):
        pp, _, _ = _seed_exchange_files(data_root, ex, now_s=t0, partial_age_s=2.0)
        partials[ex] = pp

    # 1. Proxy mode OFF (default) -> proxy probe is never even used, zero alerts
    monkeypatch.setenv("TOKYO_L2_ARCHIVE_PROXY_MODE", "off")
    transport = FakeTransport()
    tunnel_up = [False]
    mon_off = _build_monitor(cfg_path, transport)
    mon_off.proxy_probe = lambda h, p: {"reachable": tunnel_up[0], "error": "down" if not tunnel_up[0] else None}
    assert mon_off.run_sweep(now_epoch_s=t0, force_collector_hc=True)["active_alerts"] == []

    # 2. Proxy mode REQUIRED with tunnel down -> fires lightsail_proxy_unavailable WITHOUT breaking collector HC!
    monkeypatch.setenv("TOKYO_L2_ARCHIVE_PROXY_MODE", "required")
    monkeypatch.setenv("TOKYO_L2_ARCHIVE_PROXY_URL", "socks5://127.0.0.1:1080")
    for pp in partials.values():
        os.utime(pp, (t0 + 20.0, t0 + 20.0))
    r_down = mon_off.run_sweep(now_epoch_s=t0 + 25.0, force_collector_hc=True)
    assert "lightsail_proxy_unavailable" in r_down["active_alerts"]
    assert r_down["collector_healthcheck"]["healthy"] is True

    # 3. Tunnel recovers -> alert clears and sends recovery message
    tunnel_up[0] = True
    for pp in partials.values():
        os.utime(pp, (t0 + 40.0, t0 + 40.0))
    r_up = mon_off.run_sweep(now_epoch_s=t0 + 45.0, force_collector_hc=True)
    assert r_up["active_alerts"] == []
    assert r_up["alert_dispatch"]["sent_recoveries"] == ["lightsail_proxy_unavailable"]


def test_p5_check_takeover_fail_closed_ownership_and_private_peering_templates(tmp_path: Path) -> None:
    """Verify fail-closed check-takeover gates (--data-root, lock, systemd, PID ownership) & VPC Peering templates."""
    import fcntl
    import shutil

    data_root = tmp_path / "tokyo_l2"
    data_root.mkdir(parents=True, exist_ok=True)
    idle_units = lambda u: {  # noqa: E731
        "unit": u,
        "available": True,
        "load_state": "loaded",
        "active_state": "inactive",
        "sub_state": "dead",
        "main_pid": None,
        "control_group": None,
    }
    no_listeners = lambda h, p: {  # noqa: E731
        "available": True,
        "pids": [],
        "entries": [],
        "pid_visibility_denied": False,
        "error": None,
    }

    # 1. Missing data_root -> blocked fail-closed (never skip .archive_sync.lock!)
    missing_root_res = lp_mod.check_takeover_readiness(
        host="127.0.0.1",
        port=1080,
        data_root=None,
        port_probe_fn=lambda h, p: {"host": h, "port": p, "in_use": False},
        unit_state_fn=idle_units,
        listener_owner_fn=no_listeners,
    )
    assert missing_root_res["status"] == "blocked"
    assert missing_root_res["conflict"] == "missing_data_root"
    assert missing_root_res["ready_for_systemd_takeover"] is False

    # 2. Non-existent data_root or lock check failure -> blocked fail-closed
    nonexistent_res = lp_mod.check_takeover_readiness(
        host="127.0.0.1",
        port=1080,
        data_root=tmp_path / "does_not_exist",
        port_probe_fn=lambda h, p: {"host": h, "port": p, "in_use": False},
        unit_state_fn=idle_units,
        listener_owner_fn=no_listeners,
    )
    assert nonexistent_res["status"] == "blocked"
    assert nonexistent_res["conflict"] == "archive_lock_check_failed"
    assert nonexistent_res["ready_for_systemd_takeover"] is False

    lock_err_res = lp_mod.check_takeover_readiness(
        host="127.0.0.1",
        port=1080,
        data_root=data_root,
        port_probe_fn=lambda h, p: {"host": h, "port": p, "in_use": False},
        unit_state_fn=idle_units,
        lock_check_fn=lambda _p: {
            "checked": False,
            "lock_path": str(data_root / ".archive_sync.lock"),
            "lock_held": False,
            "error": "EACCES permission denied",
        },
        listener_owner_fn=no_listeners,
    )
    assert lock_err_res["status"] == "blocked"
    assert lock_err_res["conflict"] == "archive_lock_check_failed"

    # 3. Systemd unavailable or unclear/transitioning unit state -> blocked fail-closed
    sys_unavail = lp_mod.check_takeover_readiness(
        host="127.0.0.1",
        port=1080,
        data_root=data_root,
        port_probe_fn=lambda h, p: {"host": h, "port": p, "in_use": False},
        unit_state_fn=lambda u: {
            "unit": u,
            "available": False,
            "load_state": "unknown",
            "active_state": "unknown",
            "sub_state": "unknown",
            "main_pid": None,
            "control_group": None,
        },
        listener_owner_fn=no_listeners,
    )
    assert sys_unavail["status"] == "blocked"
    assert sys_unavail["conflict"] == "systemd_unavailable"
    assert sys_unavail["ready_for_systemd_takeover"] is False

    unclear_res = lp_mod.check_takeover_readiness(
        host="127.0.0.1",
        port=1080,
        data_root=data_root,
        port_probe_fn=lambda h, p: {"host": h, "port": p, "in_use": False},
        unit_state_fn=lambda u: {
            "unit": u,
            "available": True,
            "load_state": "loaded",
            "active_state": "activating" if u == lp_mod.SYSTEMD_PROXY_UNIT else "inactive",
            "sub_state": "auto-restart",
            "main_pid": None,
            "control_group": None,
        },
        listener_owner_fn=no_listeners,
    )
    assert unclear_res["status"] == "blocked"
    assert unclear_res["conflict"] == "proxy_state_unclear"

    # 4. Free port + idle loaded archive + free lock + systemd available -> status='ok', ready=True
    free_res = lp_mod.check_takeover_readiness(
        host="127.0.0.1",
        port=1080,
        data_root=data_root,
        port_probe_fn=lambda h, p: {"host": h, "port": p, "in_use": False},
        unit_state_fn=idle_units,
        listener_owner_fn=no_listeners,
    )
    assert free_res["status"] == "ok"
    assert free_res["ready_for_systemd_takeover"] is True
    assert free_res["conflict"] is None

    # 5. Occupied port + systemd proxy inactive (manual SSH tunnel) -> blocked
    occ_res = lp_mod.check_takeover_readiness(
        host="127.0.0.1",
        port=1080,
        data_root=data_root,
        port_probe_fn=lambda h, p: {"host": h, "port": p, "in_use": True},
        unit_state_fn=idle_units,
        listener_owner_fn=lambda h, p: {
            "available": True,
            "pids": [9999],
            "entries": [{"local_address": "127.0.0.1:1080", "pids": [9999]}],
            "pid_visibility_denied": False,
        },
    )
    assert occ_res["status"] == "blocked"
    assert occ_res["ready_for_systemd_takeover"] is False
    assert occ_res["conflict"] == "port_occupied_by_non_systemd_process"

    # 6. Active archive unit OR held .archive_sync.lock -> blocked with archive_active
    arch_res = lp_mod.check_takeover_readiness(
        host="127.0.0.1",
        port=1080,
        data_root=data_root,
        port_probe_fn=lambda h, p: {"host": h, "port": p, "in_use": False},
        unit_state_fn=lambda u: {
            "unit": u,
            "available": True,
            "load_state": "loaded",
            "active_state": "active" if u == lp_mod.SYSTEMD_ARCHIVE_UNIT else "inactive",
            "sub_state": "running" if u == lp_mod.SYSTEMD_ARCHIVE_UNIT else "dead",
            "main_pid": 1234 if u == lp_mod.SYSTEMD_ARCHIVE_UNIT else None,
        },
        listener_owner_fn=no_listeners,
    )
    assert arch_res["status"] == "blocked"
    assert arch_res["conflict"] == "archive_active"

    lock_file = data_root / ".archive_sync.lock"
    lock_file.write_text("1234\n", encoding="utf-8")
    with lock_file.open("r+", encoding="utf-8") as lf:
        fcntl.flock(lf.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            lock_res = lp_mod.check_takeover_readiness(
                host="127.0.0.1",
                port=1080,
                data_root=data_root,
                port_probe_fn=lambda h, p: {"host": h, "port": p, "in_use": False},
                unit_state_fn=idle_units,
                listener_owner_fn=no_listeners,
            )
            assert lock_res["status"] == "blocked"
            assert lock_res["archive_lock_held"] is True
            assert lock_res["conflict"] == "archive_active"
        finally:
            fcntl.flock(lf.fileno(), fcntl.LOCK_UN)

    # 7. Active systemd proxy MUST still prove actual listener PID ownership!
    active_proxy_units = lambda u: {  # noqa: E731
        "unit": u,
        "available": True,
        "load_state": "loaded",
        "active_state": "active" if u == lp_mod.SYSTEMD_PROXY_UNIT else "inactive",
        "sub_state": "running" if u == lp_mod.SYSTEMD_PROXY_UNIT else "dead",
        "main_pid": 4321 if u == lp_mod.SYSTEMD_PROXY_UNIT else None,
        "control_group": "/system.slice/tokyo-l2-lightsail-proxy.service" if u == lp_mod.SYSTEMD_PROXY_UNIT else None,
    }
    # 7a. Active proxy MainPID=4321, but port 1080 owned by different PID 9999 -> BLOCKED!
    wrong_owner = lp_mod.check_takeover_readiness(
        host="127.0.0.1",
        port=1080,
        data_root=data_root,
        port_probe_fn=lambda h, p: {"host": h, "port": p, "in_use": True},
        unit_state_fn=active_proxy_units,
        listener_owner_fn=lambda h, p: {
            "available": True,
            "pids": [9999],
            "entries": [{"local_address": "127.0.0.1:1080", "pids": [9999]}],
            "pid_visibility_denied": False,
        },
    )
    assert wrong_owner["status"] == "blocked"
    assert wrong_owner["already_managed_by_systemd"] is False
    assert wrong_owner["port_owned_by_proxy_unit"] is False
    assert wrong_owner["conflict"] == "port_owned_by_different_process"

    # 7b. Active proxy, but listener PID cannot be determined (ss unavailable / pid_visibility_denied) -> BLOCKED!
    unverified_owner = lp_mod.check_takeover_readiness(
        host="127.0.0.1",
        port=1080,
        data_root=data_root,
        port_probe_fn=lambda h, p: {"host": h, "port": p, "in_use": True},
        unit_state_fn=active_proxy_units,
        listener_owner_fn=lambda h, p: {
            "available": True,
            "pids": [],
            "entries": [{"local_address": "127.0.0.1:1080", "pids": []}],
            "pid_visibility_denied": True,
        },
    )
    assert unverified_owner["status"] == "blocked"
    assert unverified_owner["already_managed_by_systemd"] is False
    assert unverified_owner["conflict"] == "port_owner_unverified"

    # 7c. Active proxy MainPID=4321 AND listener PID=[4321] -> status='ok', already_managed_by_systemd=True
    managed_res = lp_mod.check_takeover_readiness(
        host="127.0.0.1",
        port=1080,
        data_root=data_root,
        port_probe_fn=lambda h, p: {"host": h, "port": p, "in_use": True},
        unit_state_fn=active_proxy_units,
        listener_owner_fn=lambda h, p: {
            "available": True,
            "pids": [4321],
            "entries": [{"local_address": "127.0.0.1:1080", "pids": [4321]}],
            "pid_visibility_denied": False,
        },
    )
    assert managed_res["status"] == "ok"
    assert managed_res["port_owned_by_proxy_unit"] is True
    assert managed_res["already_managed_by_systemd"] is True
    assert managed_res["conflict"] is None

    # 8. Real CLI subprocess execution:
    #    - missing `--data-root` fails immediately (rc=2)
    #    - running without systemd in container fails closed (`systemd_unavailable`, rc=2)
    #    - with simulated systemd + REAL `ss -H -ltnp` against live loopback socket owned by os.getpid():
    #        * MainPID=os.getpid() passes ownership gate (`rc=0`, `port_owned_by_proxy_unit=True`)
    #        * MainPID=999999 rejects mismatched listener (`rc=2`, `port_owned_by_different_process`)
    cli_bin = str(COLLECTOR_DIR / "bin" / "proxy-verify")
    proc_no_root = subprocess.run(
        [cli_bin, "check-takeover", "--port", "1080"],
        capture_output=True,
        text=True,
    )
    assert proc_no_root.returncode == 2
    assert "--data-root" in proc_no_root.stderr

    free_port = _unused_local_port()
    # Unmocked CLI on test host (either no-systemd container -> systemd_unavailable,
    # or real systemd without archive unit installed -> archive_state_unclear) MUST fail closed!
    proc_host_blocked = subprocess.run(
        [cli_bin, "check-takeover", "--host", "127.0.0.1", "--port", str(free_port), "--data-root", str(data_root)],
        capture_output=True,
        text=True,
    )
    assert proc_host_blocked.returncode == 2
    host_payload = json.loads(proc_host_blocked.stdout)
    assert host_payload["status"] == "blocked"
    assert any(r in host_payload["blocking_reasons"] for r in ("systemd_unavailable", "archive_state_unclear"))

    fake_bin = tmp_path / "fake_bin"
    fake_bin.mkdir()
    fake_systemctl = fake_bin / "systemctl"
    fake_systemctl.write_text(
        "#!/usr/bin/env bash\n"
        "if [ \"${FAKE_SYSCTL_FAIL:-0}\" = \"1\" ]; then\n"
        "  echo \"System has not been booted with systemd as init system\" >&2\n"
        "  exit 1\n"
        "fi\n"
        "UNIT=\"$2\"\n"
        "if [ \"$UNIT\" = \"tokyo-l2-lightsail-proxy.service\" ]; then\n"
        "  echo \"LoadState=loaded\"\n"
        "  echo \"ActiveState=${FAKE_PROXY_ACTIVE:-inactive}\"\n"
        "  echo \"SubState=${FAKE_PROXY_SUB:-dead}\"\n"
        "  echo \"MainPID=${FAKE_PROXY_PID:-0}\"\n"
        "  echo \"ControlGroup=/system.slice/tokyo-l2-lightsail-proxy.service\"\n"
        "else\n"
        "  echo \"LoadState=loaded\"\n"
        "  echo \"ActiveState=inactive\"\n"
        "  echo \"SubState=dead\"\n"
        "  echo \"MainPID=0\"\n"
        "  echo \"ControlGroup=/system.slice/tokyo-l2-archive.service\"\n"
        "fi\n",
        encoding="utf-8",
    )
    fake_systemctl.chmod(0o755)
    env_fake = dict(os.environ)
    env_fake["PATH"] = f"{fake_bin}:{os.environ.get('PATH', '')}"

    # Deterministic CLI check for systemd unavailability on ANY host/CI runner
    env_no_sys = dict(env_fake, FAKE_SYSCTL_FAIL="1")
    proc_no_systemd = subprocess.run(
        [cli_bin, "check-takeover", "--host", "127.0.0.1", "--port", str(free_port), "--data-root", str(data_root)],
        env=env_no_sys,
        capture_output=True,
        text=True,
    )
    assert proc_no_systemd.returncode == 2
    no_sys_payload = json.loads(proc_no_systemd.stdout)
    assert no_sys_payload["status"] == "blocked"
    assert "systemd_unavailable" in no_sys_payload["blocking_reasons"]

    # 8a. Free port + fake systemctl idle -> rc=0, ready=True
    proc_ready = subprocess.run(
        [cli_bin, "check-takeover", "--host", "127.0.0.1", "--port", str(free_port), "--data-root", str(data_root)],
        env=env_fake,
        capture_output=True,
        text=True,
    )
    assert proc_ready.returncode == 0
    assert json.loads(proc_ready.stdout)["ready_for_systemd_takeover"] is True

    # 8b. Live socket bound by os.getpid() tested through REAL `ss` + fake `systemctl`
    socks = _LoopbackSocks5Server()
    try:
        env_wrong = dict(env_fake, FAKE_PROXY_ACTIVE="active", FAKE_PROXY_SUB="running", FAKE_PROXY_PID="999999")
        proc_mismatch = subprocess.run(
            [cli_bin, "check-takeover", "--host", "127.0.0.1", "--port", str(socks.port), "--data-root", str(data_root)],
            env=env_wrong,
            capture_output=True,
            text=True,
        )
        assert proc_mismatch.returncode == 2
        mismatch_payload = json.loads(proc_mismatch.stdout)
        assert mismatch_payload["conflict"] == "port_owned_by_different_process"
        assert mismatch_payload["already_managed_by_systemd"] is False

        env_owned = dict(env_fake, FAKE_PROXY_ACTIVE="active", FAKE_PROXY_SUB="running", FAKE_PROXY_PID=str(os.getpid()))
        proc_owned = subprocess.run(
            [cli_bin, "check-takeover", "--host", "127.0.0.1", "--port", str(socks.port), "--data-root", str(data_root)],
            env=env_owned,
            capture_output=True,
            text=True,
        )
        assert proc_owned.returncode == 0
        owned_payload = json.loads(proc_owned.stdout)
        assert owned_payload["port_owned_by_proxy_unit"] is True
        assert owned_payload["already_managed_by_systemd"] is True
        assert os.getpid() in owned_payload["matched_listener_pids"]
    finally:
        socks.stop()

    # 9. Audit real deploy templates (VPC Peering private IP 172.26.12.211, HostKeyAlias 13.192.29.63,
    #    temp 1081 flow, --data-root /data/tokyo_l2, no fallback_direct, no --prune-hours 24, zero Rust collector proxy)
    real_iso = lp_mod.audit_proxy_isolation(DEPLOY_DIR)
    assert real_iso["isolated"] is True, real_iso["violations"]
    proxy_env_txt = (DEPLOY_DIR / "env" / "proxy.env.example").read_text(encoding="utf-8")
    proxy_unit_txt = (DEPLOY_DIR / "systemd" / "tokyo-l2-lightsail-proxy.service").read_text(encoding="utf-8")
    archive_unit_txt = (DEPLOY_DIR / "systemd" / "tokyo-l2-archive.service").read_text(encoding="utf-8")
    guide_txt = (DEPLOY_DIR / "LIGHTSAIL_PROXY.md").read_text(encoding="utf-8")
    assert "TOKYO_L2_LIGHTSAIL_HOST=172.26.12.211" in proxy_env_txt
    assert "TOKYO_L2_LIGHTSAIL_PUBLIC_FALLBACK_HOST=13.192.29.63" in proxy_env_txt
    assert "TOKYO_L2_LIGHTSAIL_HOSTKEY_ALIAS=13.192.29.63" in proxy_env_txt
    assert "TOKYO_L2_ARCHIVE_PROXY_MODE=required" in proxy_env_txt
    assert "/etc/tokyo-l2/tokyo_l2_lightsail" in proxy_env_txt
    assert "-o HostKeyAlias=${TOKYO_L2_LIGHTSAIL_HOSTKEY_ALIAS}" in proxy_unit_txt
    assert "--prune-hours" not in [ln for ln in archive_unit_txt.splitlines() if ln.strip() and not ln.strip().startswith("#")]
    assert "1081" in guide_txt
    assert "--data-root /data/tokyo_l2" in guide_txt
    assert "ssh-keygen -lf" in guide_txt

    tmp_deploy = tmp_path / "deploy_copy"
    shutil.copytree(DEPLOY_DIR, tmp_deploy)
    (tmp_deploy / "LIGHTSAIL_PROXY.md").write_text(
        "# Broken Guide\nssh -o StrictHostKeyChecking=accept-new ubuntu@172.26.12.211\n"
        "ssh-keyscan -H 13.192.29.63 | sudo tee /etc/tokyo-l2/lightsail_known_hosts\n",
        encoding="utf-8",
    )
    (tmp_deploy / "env" / "proxy.env.example").write_text(
        "TOKYO_L2_LIGHTSAIL_HOST=13.192.29.63\n"
        "TOKYO_L2_LIGHTSAIL_KEY=/etc/tokyo-l2/lightsail_ed25519\n"
        "TOKYO_L2_ARCHIVE_PROXY_MODE=fallback_direct\n",
        encoding="utf-8",
    )
    bad_iso = lp_mod.audit_proxy_isolation(tmp_deploy)
    assert bad_iso["isolated"] is False
    assert any("lightsail_ed25519" in v for v in bad_iso["violations"])
    assert any("172.26.12.211" in v for v in bad_iso["violations"])
    assert any("fallback_direct" in v for v in bad_iso["violations"])
    assert any("StrictHostKeyChecking" in v or "auto-accepting" in v for v in bad_iso["violations"])
    assert any("lightsail_known_hosts" in v for v in bad_iso["violations"])
