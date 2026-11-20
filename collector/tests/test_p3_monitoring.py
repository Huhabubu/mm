"""Deterministic offline test suite for Phase P3 monitoring, Telegram alerts, and Healthchecks."""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import urllib.error
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import zstandard as zstd

REPO_ROOT = Path(__file__).resolve().parents[2]
COLLECTOR_DIR = REPO_ROOT / "collector"
PYTHON_DIR = COLLECTOR_DIR / "python"
SPECS_DIR = COLLECTOR_DIR / "specs"

if str(PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(PYTHON_DIR))

import cli as cli_mod  # noqa: E402
import meta_daemon as md_mod  # noqa: E402
from archive_sync import (  # noqa: E402
    LEDGER_DB_FILENAME,
    ArchiveLedger,
    RemoteObjectInfo,
    file_hashes,
    sync_sealed_directory,
)
from meta_daemon import (  # noqa: E402
    CollectorMonitor,
    NotificationClient,
    redact_secrets,
)

GOOD_CHRONY_OUTPUT = """Reference ID    : A9FEA97B (169.254.169.123)
Stratum         : 3
Ref time (UTC)  : Thu Dec 03 10:00:00 2026
System time     : 0.000012000 seconds fast of NTP time
Last offset     : +0.000020000 seconds
RMS offset      : 0.000025000 seconds
Frequency       : 1.000 ppm slow
Residual freq   : +0.001 ppm
Skew            : 0.010 ppm
Root delay      : 0.000180000 seconds
Root dispersion : 0.000100000 seconds
Update interval : 16.0 seconds
Leap status     : Normal
"""

BAD_CHRONY_OUTPUT = GOOD_CHRONY_OUTPUT.replace(
    "Root dispersion : 0.000100000 seconds",
    "Root dispersion : 0.002500000 seconds",
)


def _write_test_config(tmp_path: Path, data_root: Path) -> Path:
    cfg = json.loads((SPECS_DIR / "config.example.json").read_text(encoding="utf-8"))
    cfg["deployment"]["data_root"] = str(data_root)
    cfg["meta_daemon"]["collector_stale_seconds"] = 300
    cfg["meta_daemon"]["collector_hc_interval_seconds"] = 300
    cfg["meta_daemon"]["alert_cooldown_seconds"] = 1800
    cfg["meta_daemon"]["restart_recovery_seconds"] = 600
    cfg["meta_daemon"]["outage_recovery_seconds"] = 60
    cfg["meta_daemon"]["loss_recovery_seconds"] = 300
    cfg["meta_daemon"]["hourly_report_minute_utc"] = 10
    cfg_path = tmp_path / "p3_config.json"
    cfg_path.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
    return cfg_path


def _seed_exchange_files(
    data_root: Path,
    exchange: str,
    *,
    now_s: float,
    date_utc: str = "2026-12-03",
    hour_utc: str = "09",
    partial_age_s: float = 5.0,
    data_lines: int = 1000,
    local_loss_lines: int = 0,
    gap_start_lines: int = 0,
    switch_lines: int = 0,
    boot_id: str = "boot_1",
) -> tuple[Path, Path, Path]:
    """Create .partial + sealed .manifest.json + dummy non-zstd .tsv.zst (never decompressed by P3)."""
    ex_dir = data_root / "raw" / exchange / date_utc
    ex_dir.mkdir(parents=True, exist_ok=True)
    yyyymmdd = date_utc.replace("-", "")

    # Write non-zstd sentinel bytes so any accidental decompression attempt immediately explodes.
    partial_path = ex_dir / f"{exchange}_{yyyymmdd}_10.tsv.zst.partial"
    partial_path.write_bytes(b"NOT_ZSTD_PARTIAL_SENTINEL_BYTES")
    os.utime(partial_path, (now_s - partial_age_s, now_s - partial_age_s))

    data_path = ex_dir / f"{exchange}_{yyyymmdd}_{hour_utc}.tsv.zst"
    data_path.write_bytes(b"NOT_ZSTD_SEALED_SENTINEL_BYTES")
    dh = file_hashes(data_path)

    manifest_path = ex_dir / f"{exchange}_{yyyymmdd}_{hour_utc}.manifest.json"
    manifest = {
        "schema_version": 1,
        "exchange": exchange,
        "date_utc": date_utc,
        "hour_utc": hour_utc,
        "boot_id": boot_id,
        "file_name": data_path.name,
        "bytes": dh["bytes"],
        "sha256": dh["sha256"],
        "zstd_verified": True,
        "start_wall_ns": int((now_s - 3600) * 1e9),
        "end_wall_ns": int((now_s - 60) * 1e9),
        "first_rx_seq": 1,
        "last_rx_seq": data_lines,
        "data_lines": data_lines,
        "data_escaped_lines": 0,
        "meta_lines": switch_lines + gap_start_lines + local_loss_lines,
        "local_loss_lines": local_loss_lines,
        "gap_start_lines": gap_start_lines,
        "switch_lines": switch_lines,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    os.utime(manifest_path, (now_s - partial_age_s - 10, now_s - partial_age_s - 10))
    return partial_path, data_path, manifest_path


def _seed_ledger_row(
    data_root: Path,
    dpath: Path,
    mpath: Path,
    *,
    exchange: str,
    hour_utc: str = "09",
    state: str = "VERIFIED",
    last_error_code: str | None = None,
    has_conflict: int = 0,
    sealed_at_utc: str = "2026-12-03T10:00:00+00:00",
) -> None:
    dh = file_hashes(dpath)
    mh = file_hashes(mpath)
    rel_d = dpath.relative_to(data_root).as_posix()
    rel_m = mpath.relative_to(data_root).as_posix()
    verified_flag = 1 if state in ("VERIFIED", "PRUNED") else 0
    with ArchiveLedger(data_root / LEDGER_DB_FILENAME) as ledger:
        with ledger.conn:
            ledger.conn.execute(
                """
                INSERT OR REPLACE INTO partitions (
                    exchange, date_utc, hour_utc, boot_id, partition_id,
                    rel_data_path, rel_manifest_path, state,
                    local_bytes, local_sha256, local_md5,
                    manifest_bytes, manifest_sha256, manifest_md5,
                    sealed_at_utc, remote_data_verified, remote_manifest_verified,
                    last_error_code, last_error_message, has_conflict, updated_at_utc
                ) VALUES (?, '2026-12-03', ?, 'boot_1', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    exchange,
                    hour_utc,
                    dpath.stem.replace(".tsv", ""),
                    rel_d,
                    rel_m,
                    state,
                    dh["bytes"],
                    dh["sha256"],
                    dh["md5"],
                    mh["bytes"],
                    mh["sha256"],
                    mh["md5"],
                    sealed_at_utc,
                    verified_flag,
                    verified_flag,
                    last_error_code,
                    last_error_code,
                    has_conflict,
                    sealed_at_utc,
                ),
            )


class FakeTransport:
    """Records all Telegram Bot API & Healthchecks HTTP POST calls with configurable failures."""

    def __init__(self) -> None:
        self.tg_messages: list[dict[str, Any]] = []
        self.hc_calls: list[dict[str, Any]] = []
        self.fail_tg: bool = False
        self.fail_hc: bool = False
        self.tg_status_code: int = 500
        self.raise_network_error: bool = False

    def send(self, url: str, body: bytes, headers: dict[str, str], timeout: float) -> tuple[int, bytes]:
        if "/sendMessage" in url:
            if self.fail_tg:
                if self.raise_network_error:
                    raise TimeoutError("simulated telegram timeout")
                return self.tg_status_code, b'{"ok":false}'
            payload = json.loads(body.decode("utf-8"))
            self.tg_messages.append({"url": url, "timeout": timeout, **payload})
            return 200, b'{"ok":true}'

        if self.fail_hc:
            if self.raise_network_error:
                raise ConnectionResetError("simulated hc connection reset")
            return 503, b"unavailable"
        self.hc_calls.append(
            {
                "url": url,
                "is_fail": url.endswith("/fail"),
                "body": body.decode("utf-8", errors="replace"),
                "timeout": timeout,
            }
        )
        return 200, b"OK"


def _healthy_service(unit: str) -> dict[str, Any]:
    return {
        "available": True,
        "unit": unit,
        "load_state": "loaded",
        "active_state": "active",
        "sub_state": "running" if "collector" in unit else "dead",
        "main_pid": 4242 if "collector" in unit else 0,
        "n_restarts": 0,
        "exec_main_status": 0,
        "result": "success",
        "uptime_seconds": 1800.0,
        "error": None,
    }


def _build_monitor(
    cfg_path: Path,
    transport: FakeTransport,
    *,
    service_probe: Any = None,
    journal_reader: Any = None,
    chrony_runner: Any = None,
    disk_checker: Any = None,
) -> CollectorMonitor:
    notifier = NotificationClient(
        telegram_bot_token="123456789:AAFakeSecretTokenForUnitTests",
        telegram_chat_id="-1009988776655",
        http_sender=transport.send,
        sleeper=lambda _s: None,
    )
    return CollectorMonitor(
        cfg_path,
        notifier=notifier,
        hc_collector_url="https://hc-ping.com/11111111-2222-3333-4444-555555555555",
        hc_archive_url="https://hc-ping.com/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        service_probe=service_probe or _healthy_service,
        journal_reader=journal_reader or (lambda _u, cur: ([], cur)),
        spec_fetcher=lambda _u, _h: b'{"ok":true}',
        rtt_prober=lambda _h, _p: 1.5,
        chrony_runner=chrony_runner or (lambda: GOOD_CHRONY_OUTPUT),
        disk_checker=disk_checker or (lambda _p: 35.0),
        hc_sender=transport.send,
        sleeper=lambda _s: None,
    )


def test_p3_healthy_run_hourly_battle_report_and_zero_zstd_decompression(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Healthy run: 0 alerts, HC ping ok every 5 min, 1 battle report/hour, ZERO .tsv.zst decompression."""
    def forbid_zstd(*_a: Any, **_kw: Any) -> Any:
        raise AssertionError("P3 monitor MUST NEVER open or decompress .tsv.zst files on EC2!")

    monkeypatch.setattr(zstd, "ZstdDecompressor", forbid_zstd)

    data_root = tmp_path / "tokyo_l2"
    cfg_path = _write_test_config(tmp_path, data_root)
    # 2026-12-03T10:12:00 UTC -> 1796292720
    t0 = 1_796_292_720.0
    for ex, lines in (
        ("binance_futures", 12000),
        ("bybit_linear", 4500),
        ("bitget_v3", 5200),
        ("gate_futures", 3528),
    ):
        _, dp, mp = _seed_exchange_files(data_root, ex, now_s=t0, data_lines=lines)
        _seed_ledger_row(data_root, dp, mp, exchange=ex, state="VERIFIED")

    transport = FakeTransport()
    mon = _build_monitor(cfg_path, transport)

    r1 = mon.run_sweep(now_epoch_s=t0)
    assert r1["active_alerts"] == []
    assert r1["collector_healthcheck"]["healthy"] is True
    assert r1["collector_healthcheck"]["sent"] is True
    assert len(transport.hc_calls) == 1
    assert not transport.hc_calls[0]["is_fail"]

    # Exactly 1 hourly battle report sent (zero spurious alerts)
    assert r1["hourly_report"]["sent"] is True
    assert len(transport.tg_messages) == 1
    report = transport.tg_messages[0]["text"]
    assert "东京 L2 小时战报" in report
    assert "归档状态: VERIFIED" in report
    assert "深度数据质量: PENDING（待后续 GitHub 深度 QA 验证）" in report
    for ex in ("binance_futures", "bybit_linear", "bitget_v3", "gate_futures"):
        assert ex in report
    assert "当前异常: 无" in report

    # Second sweep 60s later in SAME hour: ZERO duplicate battle report, ZERO HC ping (<300s)
    r2 = mon.run_sweep(now_epoch_s=t0 + 60.0)
    assert r2["hourly_report"]["due"] is False
    assert r2["collector_healthcheck"]["due"] is False
    assert len(transport.tg_messages) == 1
    assert len(transport.hc_calls) == 1

    # Sweep +300s later (collector keeps writing .partial actively): 5-min Collector HC pings ok again!
    for ex in ("binance_futures", "bybit_linear", "bitget_v3", "gate_futures"):
        p_path = next((data_root / "raw" / ex / "2026-12-03").glob("*.partial"))
        os.utime(p_path, (t0 + 302.0, t0 + 302.0))
    r3 = mon.run_sweep(now_epoch_s=t0 + 305.0)
    assert r3["collector_healthcheck"]["sent"] is True
    assert len(transport.hc_calls) == 2
    assert len(transport.tg_messages) == 1

    # Next UTC hour (+3600s, minute=12): sends next hourly battle report!
    for ex in ("binance_futures", "bybit_linear", "bitget_v3", "gate_futures"):
        p_path = next((data_root / "raw" / ex / "2026-12-03").glob("*.partial"))
        os.utime(p_path, (t0 + 3600.0, t0 + 3600.0))
    r4 = mon.run_sweep(now_epoch_s=t0 + 3605.0)
    assert r4["hourly_report"]["sent"] is True
    assert len(transport.tg_messages) == 2


def test_p3_collector_stopped_abnormal_restart_and_data_stale_alerts_and_recovery(
    tmp_path: Path,
) -> None:
    """Process stop, abnormal NRestarts/shutdown, and .partial staleness alert, fail HC, and recover."""
    data_root = tmp_path / "tokyo_l2"
    cfg_path = _write_test_config(tmp_path, data_root)
    t0 = 1_796_292_720.0
    partials: dict[str, Path] = {}
    for ex in ("binance_futures", "bybit_linear", "bitget_v3", "gate_futures"):
        pp, dp, mp = _seed_exchange_files(data_root, ex, now_s=t0, partial_age_s=3.0)
        partials[ex] = pp
        _seed_ledger_row(data_root, dp, mp, exchange=ex, state="VERIFIED")

    svc_state: dict[str, Any] = {
        "collector_active": "active",
        "collector_sub": "running",
        "n_restarts": 0,
        "result": "success",
    }

    def probe(unit: str) -> dict[str, Any]:
        if "archive" in unit:
            return _healthy_service(unit)
        return {
            "available": True,
            "unit": unit,
            "load_state": "loaded",
            "active_state": svc_state["collector_active"],
            "sub_state": svc_state["collector_sub"],
            "main_pid": 4242 if svc_state["collector_active"] == "active" else 0,
            "n_restarts": svc_state["n_restarts"],
            "exec_main_status": 0 if svc_state["result"] == "success" else 1,
            "result": svc_state["result"],
            "uptime_seconds": 1200.0,
            "error": None,
        }

    transport = FakeTransport()
    mon = _build_monitor(cfg_path, transport, service_probe=probe)

    # Initial healthy sweep
    r0 = mon.run_sweep(now_epoch_s=t0)
    assert r0["active_alerts"] == []
    transport.tg_messages.clear()
    transport.hc_calls.clear()

    # 1. Collector stops -> immediate alert + HC /fail
    svc_state["collector_active"] = "failed"
    svc_state["collector_sub"] = "failed"
    svc_state["result"] = "exit-code"
    r_down = mon.run_sweep(now_epoch_s=t0 + 301.0)
    assert "collector_stopped" in r_down["active_alerts"]
    assert "collector_abnormal_restart" in r_down["active_alerts"]
    assert any("collector_stopped" in m["text"] for m in transport.tg_messages)
    assert transport.hc_calls[-1]["is_fail"] is True
    assert "collector_stopped" in transport.hc_calls[-1]["body"]

    # 2. Collector auto-restarts (active=active, NRestarts=1, result=success) -> collector_stopped recovers!
    svc_state["collector_active"] = "active"
    svc_state["collector_sub"] = "running"
    svc_state["n_restarts"] = 1
    svc_state["result"] = "success"
    for pp in partials.values():
        os.utime(pp, (t0 + 605.0, t0 + 605.0))
    r_up = mon.run_sweep(now_epoch_s=t0 + 610.0)
    assert "collector_stopped" not in r_up["active_alerts"]
    assert "collector_stopped" in r_up["alert_dispatch"]["sent_recoveries"]
    assert any("[已恢复] collector_stopped" in m["text"] for m in transport.tg_messages)
    # abnormal_restart stays firing during 600s stabilization window, then recovers!
    assert "collector_abnormal_restart" in r_up["active_alerts"]
    for pp in partials.values():
        os.utime(pp, (t0 + 1300.0, t0 + 1300.0))
    r_stable = mon.run_sweep(now_epoch_s=t0 + 1305.0)
    assert r_stable["active_alerts"] == []
    assert "collector_abnormal_restart" in r_stable["alert_dispatch"]["sent_recoveries"]
    assert transport.hc_calls[-1]["is_fail"] is False

    # 3. Single exchange data stall (.partial and .manifest aged > 300s on bybit_linear)
    transport.tg_messages.clear()
    transport.hc_calls.clear()
    stale_t = t0 + 1700.0
    for ex, pp in partials.items():
        if ex == "bybit_linear":
            # Keep old mtime (t0 + 1300s -> 400s old > 300s)
            mf_path = next((data_root / "raw" / ex / "2026-12-03").glob("*.manifest.json"))
            os.utime(pp, (t0 + 1300.0, t0 + 1300.0))
            os.utime(mf_path, (t0 + 1200.0, t0 + 1200.0))
        else:
            os.utime(pp, (stale_t - 2.0, stale_t - 2.0))
    r_stale = mon.run_sweep(now_epoch_s=stale_t)
    assert "collector_data_stale" in r_stale["active_alerts"]
    assert any("bybit_linear" in m["text"] for m in transport.tg_messages)
    assert transport.hc_calls[-1]["is_fail"] is True
    assert "collector_data_stale" in transport.hc_calls[-1]["body"]

    # Bybit resumes writing -> recovers + HC back to ok
    os.utime(partials["bybit_linear"], (stale_t + 305.0, stale_t + 305.0))
    for ex, pp in partials.items():
        os.utime(pp, (stale_t + 305.0, stale_t + 305.0))
    r_resumed = mon.run_sweep(now_epoch_s=stale_t + 310.0)
    assert r_resumed["active_alerts"] == []
    assert "collector_data_stale" in r_resumed["alert_dispatch"]["sent_recoveries"]
    assert transport.hc_calls[-1]["is_fail"] is False


def test_p3_dual_outage_local_loss_disk_and_clock_alerts_with_recovery(tmp_path: Path) -> None:
    """Dual A/B > 5s, LOCAL_LOSS (log & manifest), disk > 70%, and Chrony > 1ms alerts & recoveries."""
    data_root = tmp_path / "tokyo_l2"
    cfg_path = _write_test_config(tmp_path, data_root)
    t0 = 1_796_292_720.0
    for ex in ("binance_futures", "bybit_linear", "bitget_v3", "gate_futures"):
        _, dp, mp = _seed_exchange_files(data_root, ex, now_s=t0, partial_age_s=2.0)
        _seed_ledger_row(data_root, dp, mp, exchange=ex, state="VERIFIED")

    journal_queue: list[str] = []
    disk_box = [40.0]
    chrony_box = [GOOD_CHRONY_OUTPUT]

    def j_reader(_u: str, cursor: str | None) -> tuple[list[str], str | None]:
        out = list(journal_queue)
        journal_queue.clear()
        next_c = f"cur_{len(out)}_{cursor or '0'}"
        return out, next_c

    transport = FakeTransport()
    mon = _build_monitor(
        cfg_path,
        transport,
        journal_reader=j_reader,
        chrony_runner=lambda: chrony_box[0],
        disk_checker=lambda _p: disk_box[0],
    )

    assert mon.run_sweep(now_epoch_s=t0)["active_alerts"] == []
    transport.tg_messages.clear()

    # Inject all 4 anomalies simultaneously
    journal_queue.extend(
        [
            "ERROR tokyo_l2_collector::live: [ALERT CRITICAL] binance_public dual A/B outage exceeded 5s",
            "ERROR tokyo_l2_collector::live: [ALERT CRITICAL] LOCAL_LOSS on gate_futures: dropped=3 reason=bounded_channel_full",
        ]
    )
    disk_box[0] = 74.5
    chrony_box[0] = BAD_CHRONY_OUTPUT

    r_bad = mon.run_sweep(now_epoch_s=t0 + 65.0)
    assert set(r_bad["active_alerts"]) == {
        "dual_ab_outage_gt_5s",
        "local_loss_detected",
        "disk_usage_high",
        "clock_unsynced",
    }
    sent_keys = set(r_bad["alert_dispatch"]["sent_alerts"])
    assert sent_keys == {
        "dual_ab_outage_gt_5s",
        "local_loss_detected",
        "disk_usage_high",
        "clock_unsynced",
    }

    # Verify disk & clock recover immediately when normalized; outage recovers after 60s, loss after 300s
    disk_box[0] = 52.0
    chrony_box[0] = GOOD_CHRONY_OUTPUT
    for ex in ("binance_futures", "bybit_linear", "bitget_v3", "gate_futures"):
        p_path = next((data_root / "raw" / ex / "2026-12-03").glob("*.partial"))
        os.utime(p_path, (t0 + 400.0, t0 + 400.0))
    r_rec = mon.run_sweep(now_epoch_s=t0 + 405.0, run_chrony=True)
    assert r_rec["active_alerts"] == []
    assert set(r_rec["alert_dispatch"]["sent_recoveries"]) == {
        "dual_ab_outage_gt_5s",
        "local_loss_detected",
        "disk_usage_high",
        "clock_unsynced",
    }

    # Also test LOCAL_LOSS detected from a newly sealed .manifest.json (without needing journal)
    transport.tg_messages.clear()
    _seed_exchange_files(
        data_root,
        "gate_futures",
        now_s=t0 + 450.0,
        hour_utc="10",
        local_loss_lines=2,
    )
    r_mf_loss = mon.run_sweep(now_epoch_s=t0 + 455.0)
    assert "local_loss_detected" in r_mf_loss["active_alerts"]
    assert "local_loss_detected" in r_mf_loss["alert_dispatch"]["sent_alerts"]


def test_p3_archive_errors_conflicts_and_cli_archive_healthchecks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Archive error/conflict triggers TG alerts & battle report FAILED/CONFLICT; cli archive pings HC."""
    data_root = tmp_path / "tokyo_l2"
    cfg_path = _write_test_config(tmp_path, data_root)
    t0 = 1_796_292_720.0

    refs: dict[str, tuple[Path, Path]] = {}
    for ex in ("binance_futures", "bybit_linear", "bitget_v3", "gate_futures"):
        _, dp, mp = _seed_exchange_files(data_root, ex, now_s=t0)
        refs[ex] = (dp, mp)

    # Inject 1 upload error on bybit and 1 conflict on gate_futures
    _seed_ledger_row(data_root, *refs["binance_futures"], exchange="binance_futures", state="VERIFIED")
    _seed_ledger_row(
        data_root, *refs["bybit_linear"], exchange="bybit_linear", state="SEALED", last_error_code="ConnectionResetError"
    )
    _seed_ledger_row(data_root, *refs["bitget_v3"], exchange="bitget_v3", state="VERIFIED")
    _seed_ledger_row(
        data_root,
        *refs["gate_futures"],
        exchange="gate_futures",
        state="SEALED",
        last_error_code="REMOTE_HASH_CONFLICT",
        has_conflict=1,
    )

    transport = FakeTransport()
    mon = _build_monitor(cfg_path, transport)
    r_bad = mon.run_sweep(now_epoch_s=t0)
    assert "archive_upload_or_verify_failed" in r_bad["active_alerts"]
    assert "archive_ledger_conflict" in r_bad["active_alerts"]
    assert r_bad["hourly_report"]["text"] is not None
    assert "归档状态: CONFLICT" in r_bad["hourly_report"]["text"]
    assert "深度数据质量: PENDING（待后续 GitHub 深度 QA 验证）" in r_bad["hourly_report"]["text"]

    # Resolve both in SQLite -> monitor fires recovery alerts cleanly
    _seed_ledger_row(data_root, *refs["bybit_linear"], exchange="bybit_linear", state="VERIFIED", last_error_code=None)
    _seed_ledger_row(
        data_root, *refs["gate_futures"], exchange="gate_futures", state="VERIFIED", last_error_code=None, has_conflict=0
    )
    r_ok = mon.run_sweep(now_epoch_s=t0 + 60.0)
    assert r_ok["active_alerts"] == []
    assert set(r_ok["alert_dispatch"]["sent_recoveries"]) == {
        "archive_upload_or_verify_failed",
        "archive_ledger_conflict",
    }

    # --- Verify cli.py cmd_archive Healthchecks integration ---
    hc_requests: list[dict[str, Any]] = []
    hc_fail_network = [False]

    def fake_http_post(url: str, body: bytes, headers: dict[str, str], timeout: float) -> tuple[int, bytes]:
        if hc_fail_network[0]:
            raise ConnectionError("hc ping unreachable")
        hc_requests.append({"url": url, "is_fail": url.endswith("/fail"), "body": body.decode("utf-8")})
        return 200, b"OK"

    monkeypatch.setattr(md_mod, "default_http_post", fake_http_post)
    monkeypatch.setenv("TOKYO_L2_HEALTHCHECKS_ARCHIVE_URL", "https://hc-ping.com/99999999-8888-7777-6666-555555555555")

    sealed_cli = tmp_path / "sealed_cli"
    (sealed_cli / "raw" / "gate_futures" / "2026-12-03").mkdir(parents=True)
    # Missing .tsv.zst with valid manifest -> blocked! Should send /fail with short reason
    import zstandard as _z

    dp2 = sealed_cli / "raw" / "gate_futures" / "2026-12-03" / "gate_futures_20261203_00.tsv.zst"
    mp2 = sealed_cli / "raw" / "gate_futures" / "2026-12-03" / "gate_futures_20261203_00.manifest.json"
    dp2.write_bytes(_z.ZstdCompressor().compress(b"1\t1\tA\t1\t1\tMETA\t{}\n"))
    dh2 = file_hashes(dp2)
    mp2.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "exchange": "gate_futures",
                "date_utc": "2026-12-03",
                "hour_utc": "00",
                "boot_id": "boot_c",
                "file_name": dp2.name,
                "bytes": dh2["bytes"],
                "sha256": dh2["sha256"],
                "zstd_verified": True,
            }
        ),
        encoding="utf-8",
    )

    class _MockCliBackend:
        def __init__(self, _remote: str, *, fail_copy: bool = False) -> None:
            self.store: dict[str, bytes] = {}
            self.fail_copy = fail_copy

        def stat_remote(self, p: str) -> RemoteObjectInfo:
            b = self.store.get(p)
            if b is None:
                return RemoteObjectInfo(exists=False)
            return RemoteObjectInfo(exists=True, remote_id="r1", size=len(b), sha256=md_mod.sha256_bytes(b))

        def copy_file(self, lp: Path, rp: str, *, immutable: bool = True) -> None:
            if self.fail_copy:
                raise RuntimeError("drive upload failed 503")
            self.store[rp] = lp.read_bytes()

        def readback_sha256(self, rp: str) -> str:
            return md_mod.sha256_bytes(self.store[rp])

    # 1. Failing archive run -> exit code 2 AND Healthchecks receives /fail with reason
    monkeypatch.setattr(cli_mod, "RcloneBackend", lambda r: _MockCliBackend(r, fail_copy=True))
    args = SimpleNamespace(
        dir=sealed_cli, verify_remote=True, remote="gdrive_main:tokyo_l2_data", prune_hours=None, no_snapshot=True, no_healthchecks=False
    )
    rc_fail = cli_mod.cmd_archive(args)
    assert rc_fail == 2
    assert len(hc_requests) == 1
    assert hc_requests[0]["is_fail"] is True
    assert "archive_blocked" in hc_requests[0]["body"]

    # 2. Succeeding archive run -> exit code 0 AND Healthchecks receives success (not /fail)
    hc_requests.clear()
    monkeypatch.setattr(cli_mod, "RcloneBackend", lambda r: _MockCliBackend(r, fail_copy=False))
    rc_ok = cli_mod.cmd_archive(args)
    assert rc_ok == 0
    assert len(hc_requests) == 1
    assert hc_requests[0]["is_fail"] is False
    assert hc_requests[0]["body"].startswith("ok ")

    # 3. Even if Healthchecks endpoint completely unreachable, archive STILL returns 0!
    hc_fail_network[0] = True
    monkeypatch.setattr(md_mod.time, "sleep", lambda _s: None)
    rc_hc_down = cli_mod.cmd_archive(args)
    assert rc_hc_down == 0


def test_p3_alert_dedup_cooldown_network_failure_and_secret_redaction(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Dedup prevents spam; Telegram/HC outages use finite retries without crashing & NEVER leak secrets."""
    data_root = tmp_path / "tokyo_l2"
    cfg_path = _write_test_config(tmp_path, data_root)
    t0 = 1_796_292_720.0
    for ex in ("binance_futures", "bybit_linear", "bitget_v3", "gate_futures"):
        pp, dp, mp = _seed_exchange_files(data_root, ex, now_s=t0)
        os.utime(pp, (t0 + 2000.0, t0 + 2000.0))
        _seed_ledger_row(data_root, dp, mp, exchange=ex, state="VERIFIED")

    transport = FakeTransport()
    disk_box = [75.0]
    mon = _build_monitor(cfg_path, transport, disk_checker=lambda _p: disk_box[0])

    # 1. Start with BOTH Telegram AND Healthchecks network failing!
    transport.fail_tg = True
    transport.fail_hc = True
    transport.raise_network_error = True

    r_outage = mon.run_sweep(now_epoch_s=t0)
    assert "disk_usage_high" in r_outage["active_alerts"]
    assert "disk_usage_high" in r_outage["alert_dispatch"]["notify_failures"]
    assert r_outage["collector_healthcheck"]["sent"] is False
    # Monitor continues normally & saves state without raising!
    assert (data_root / "meta" / "monitor_state.json").is_file()

    # Verify secrets are STRICTLY redacted from stderr and saved monitor_state.json!
    captured = capsys.readouterr()
    state_text = (data_root / "meta" / "monitor_state.json").read_text(encoding="utf-8")
    secrets = [
        "123456789:AAFakeSecretTokenForUnitTests",
        "-1009988776655",
        "11111111-2222-3333-4444-555555555555",
        "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
    ]
    for sec in secrets:
        assert sec not in captured.err, f"Secret leaked to stderr: {sec}"
        assert sec not in captured.out, f"Secret leaked to stdout: {sec}"
        assert sec not in state_text, f"Secret leaked to monitor_state.json: {sec}"

    # 2. Network recovers after 70s -> because initial alert delivery failed, retry delivers it once!
    transport.fail_tg = False
    transport.fail_hc = False
    r_retry = mon.run_sweep(now_epoch_s=t0 + 70.0)
    assert "disk_usage_high" in r_retry["alert_dispatch"]["sent_alerts"]
    alert_msgs = [
        m
        for m in transport.tg_messages
        if ("[告警" in m["text"] or "[持续告警" in m["text"]) and "disk_usage_high" in m["text"]
    ]
    assert len(alert_msgs) == 1

    # 3. Repeated sweeps within 1800s cooldown -> SUPPRESSED (no spam!)
    for offset in (130.0, 200.0, 600.0, 1200.0):
        r_rep = mon.run_sweep(now_epoch_s=t0 + offset)
        assert "disk_usage_high" in r_rep["alert_dispatch"]["suppressed"]
        assert r_rep["alert_dispatch"]["sent_alerts"] == []
    assert (
        len(
            [
                m
                for m in transport.tg_messages
                if ("[告警" in m["text"] or "[持续告警" in m["text"]) and "disk_usage_high" in m["text"]
            ]
        )
        == 1
    )

    # 4. After 1800s cooldown elapses while still broken -> sends 1 reminder mentioning suppressed count
    r_rem = mon.run_sweep(now_epoch_s=t0 + 1900.0)
    assert "disk_usage_high" in r_rem["alert_dispatch"]["sent_alerts"]
    reminders = [m for m in transport.tg_messages if "持续告警" in m["text"]]
    assert len(reminders) == 1
    assert "去重抑制: 4 次" in reminders[0]["text"]

    # Check redact_secrets helper directly against generic HC / Telegram strings
    sample_err = (
        "POST https://api.telegram.org/bot987654321:XYZ_RealLookingToken_123/sendMessage failed; "
        "HC https://hc-ping.com/12345678-1234-1234-1234-1234567890ab/fail unreachable"
    )
    scrubbed = redact_secrets(sample_err, extra_secrets=["XYZ_RealLookingToken_123"])
    assert "987654321:XYZ_RealLookingToken_123" not in scrubbed
    assert "12345678-1234-1234-1234-1234567890ab" not in scrubbed
    assert "***" in scrubbed


def test_p3_legacy_minimal_config_defaults_and_oneshot_archive_idle(tmp_path: Path) -> None:
    """Legacy EC2 config.json without P3 keys works cleanly; oneshot archive idle != failed."""
    data_root = tmp_path / "tokyo_l2"
    cfg = json.loads((SPECS_DIR / "config.btc_gate_binance_mvp.json").read_text(encoding="utf-8"))
    cfg["deployment"]["data_root"] = str(data_root)
    # Explicitly verify only legacy 5 meta_daemon keys exist
    assert set(cfg["meta_daemon"].keys()) == {
        "rtt_interval_seconds",
        "chrony_interval_seconds",
        "chrony_max_error_ms",
        "dual_outage_alert_seconds",
        "alert_webhook_env",
    }
    cfg_path = tmp_path / "legacy_config.json"
    cfg_path.write_text(json.dumps(cfg, indent=2), encoding="utf-8")

    t0 = 1_796_292_720.0
    for ex in ("binance_futures", "gate_futures"):
        _, dp, mp = _seed_exchange_files(data_root, ex, now_s=t0)
        _seed_ledger_row(data_root, dp, mp, exchange=ex, state="VERIFIED")

    arch_state = ["inactive"]

    def probe(unit: str) -> dict[str, Any]:
        if "archive" in unit:
            return {
                "available": True,
                "unit": unit,
                "load_state": "loaded",
                "active_state": arch_state[0],
                "sub_state": "dead" if arch_state[0] == "inactive" else "failed",
                "main_pid": 0,
                "n_restarts": 0,
                "exec_main_status": 0 if arch_state[0] == "inactive" else 2,
                "result": "success" if arch_state[0] == "inactive" else "exit-code",
                "uptime_seconds": None,
                "error": None,
            }
        return _healthy_service(unit)

    transport = FakeTransport()
    mon = _build_monitor(cfg_path, transport, service_probe=probe)
    assert mon.collector_hc_interval_seconds == 300
    assert mon.alert_cooldown_seconds == 1800

    # Type=oneshot archive service sitting inactive/dead between hourly runs MUST NOT false-alarm!
    r_idle = mon.run_sweep(now_epoch_s=t0)
    assert r_idle["active_alerts"] == []

    # When archive systemd unit actually enters failed state -> MUST alert!
    arch_state[0] = "failed"
    r_fail = mon.run_sweep(now_epoch_s=t0 + 60.0)
    assert "archive_upload_or_verify_failed" in r_fail["active_alerts"]

