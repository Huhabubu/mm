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
    cfg["meta_daemon"]["require_stream_activity"] = False  # legacy monitor fixtures
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
    assert "东京 L2｜小时采集报告" in report
    assert "云端归档：VERIFIED" in report
    assert "深度数据质量：PENDING" in report
    for ex in ("Binance", "Bybit", "Bitget", "Gate"):
        assert ex in report
    assert "当前没有监控告警" in report

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

    # Next UTC hour (+3600s, minute=12): a fresh complete-hour manifest
    # exists, so DATA volume stays healthy and only one report is sent.
    for ex in ("binance_futures", "bybit_linear", "bitget_v3", "gate_futures"):
        _seed_exchange_files(
            data_root, ex, now_s=t0 + 3600.0,
            hour_utc="10", data_lines=12000,
        )
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
    assert any("采集程序停止" in m["text"] for m in transport.tg_messages)
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
    assert any("[已恢复]" in m["text"] and "采集程序停止" in m["text"] for m in transport.tg_messages)
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

    # Without endpoint recovery evidence, binance_public dual outage MUST NOT recover solely from elapsed time
    # or binance_futures .partial updates (e.g. from sibling binance_market)! Only disk/clock/loss recover here.
    disk_box[0] = 52.0
    chrony_box[0] = GOOD_CHRONY_OUTPUT
    for ex in ("binance_futures", "bybit_linear", "bitget_v3", "gate_futures"):
        p_path = next((data_root / "raw" / ex / "2026-12-03").glob("*.partial"))
        os.utime(p_path, (t0 + 400.0, t0 + 400.0))
    r_mid = mon.run_sweep(now_epoch_s=t0 + 405.0, run_chrony=True)
    assert r_mid["active_alerts"] == ["dual_ab_outage_gt_5s"]
    assert set(r_mid["alert_dispatch"]["sent_recoveries"]) == {
        "local_loss_detected",
        "disk_usage_high",
        "clock_unsynced",
    }

    # Even if binance_public logs Connected and binance_futures .partial continues updating (e.g. from sibling stream),
    # conservative default MUST NOT auto-recover dual_ab_outage_gt_5s without explicit operator acknowledgement.
    journal_queue.append("INFO tokyo_l2_collector::live: Connected binance_public conn=A epoch=2")
    r_conn = mon.run_sweep(now_epoch_s=t0 + 410.0)
    assert "dual_ab_outage_gt_5s" in r_conn["active_alerts"]

    for ex in ("binance_futures", "bybit_linear", "bitget_v3", "gate_futures"):
        p_path = next((data_root / "raw" / ex / "2026-12-03").glob("*.partial"))
        os.utime(p_path, (t0 + 475.0, t0 + 475.0))
    r_still = mon.run_sweep(now_epoch_s=t0 + 480.0, run_chrony=True)
    assert r_still["active_alerts"] == ["dual_ab_outage_gt_5s"]
    assert r_still["alert_dispatch"]["sent_recoveries"] == []

    # Operator explicitly acknowledges binance_public outage once endpoint health is verified -> recovers!
    ack = mon.acknowledge_dual_outage("binance_public", now_epoch_s=t0 + 482.0)
    assert ack["cleared_endpoints"] == ["binance_public"]
    r_rec = mon.run_sweep(now_epoch_s=t0 + 485.0, run_chrony=True)
    assert r_rec["active_alerts"] == []
    assert r_rec["alert_dispatch"]["sent_recoveries"] == ["dual_ab_outage_gt_5s"]

    # Also test LOCAL_LOSS detected from a newly sealed .manifest.json (without needing journal)
    transport.tg_messages.clear()
    _seed_exchange_files(
        data_root,
        "gate_futures",
        now_s=t0 + 490.0,
        hour_utc="10",
        local_loss_lines=2,
    )
    r_mf_loss = mon.run_sweep(now_epoch_s=t0 + 495.0)
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
    assert "云端归档：CONFLICT" in r_bad["hourly_report"]["text"]
    assert "深度数据质量：PENDING" in r_bad["hourly_report"]["text"]

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
        if ("[告警" in m["text"] or "[持续告警" in m["text"]) and "磁盘使用率过高" in m["text"]
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
                if ("[告警" in m["text"] or "[持续告警" in m["text"]) and "磁盘使用率过高" in m["text"]
            ]
        )
        == 1
    )

    # 4. After 1800s cooldown elapses while still broken -> sends 1 reminder mentioning suppressed count
    r_rem = mon.run_sweep(now_epoch_s=t0 + 1900.0)
    assert "disk_usage_high" in r_rem["alert_dispatch"]["sent_alerts"]
    reminders = [m for m in transport.tg_messages if "持续告警" in m["text"]]
    assert len(reminders) == 1
    assert "去重抑制 4 次" in reminders[0]["text"]

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


def test_p3_probe_failures_and_unverified_startup_block_healthy_heartbeat(tmp_path: Path) -> None:
    """Review Issue 1: systemctl/chrony/disk probe failures & unverified freshness block HC success ping."""
    data_root = tmp_path / "tokyo_l2"
    cfg_path = _write_test_config(tmp_path, data_root)
    t0 = 1_796_292_720.0
    for ex in ("binance_futures", "bybit_linear", "bitget_v3", "gate_futures"):
        pp, dp, mp = _seed_exchange_files(data_root, ex, now_s=t0, partial_age_s=2.0)
        _seed_ledger_row(data_root, dp, mp, exchange=ex, state="VERIFIED")

    svc_mode = ["unavailable"]
    chrony_ok = [True]
    disk_fail = [False]

    def probe(unit: str) -> dict[str, Any]:
        if svc_mode[0] == "unavailable":
            return {"available": False, "unit": unit, "error": "systemctl dbus timeout"}
        if svc_mode[0] == "raise":
            raise RuntimeError("systemctl subprocess crashed")
        if svc_mode[0] == "startup_grace":
            res = _healthy_service(unit)
            if "collector" in unit:
                res["uptime_seconds"] = 10.0
            return res
        return _healthy_service(unit)

    def chrony() -> str:
        if not chrony_ok[0]:
            raise RuntimeError("chronyc socket missing")
        return GOOD_CHRONY_OUTPUT

    def disk_chk(_p: Path) -> float:
        if disk_fail[0]:
            raise OSError("statvfs EIO")
        return 32.0

    transport = FakeTransport()
    mon = _build_monitor(
        cfg_path,
        transport,
        service_probe=probe,
        chrony_runner=chrony,
        disk_checker=disk_chk,
    )

    # 1. systemctl unavailable -> NO false healthy ping! Sends /fail and triggers collector_probe_failed!
    r1 = mon.run_sweep(now_epoch_s=t0, force_collector_hc=True)
    assert "collector_probe_failed" in r1["active_alerts"]
    assert r1["collector_healthcheck"]["healthy"] is False
    assert transport.hc_calls[-1]["is_fail"] is True
    assert "collector_probe_failed" in transport.hc_calls[-1]["body"]

    # 2. service_probe raises exception -> ALSO fail-closed!
    svc_mode[0] = "raise"
    r2 = mon.run_sweep(now_epoch_s=t0 + 305.0, force_collector_hc=True)
    assert "collector_probe_failed" in r2["active_alerts"]
    assert r2["collector_healthcheck"]["healthy"] is False
    assert transport.hc_calls[-1]["is_fail"] is True

    # 3. systemctl recovers, BUT chronyc fails -> clock uncertified -> blocks healthy ping!
    svc_mode[0] = "healthy"
    chrony_ok[0] = False
    r3 = mon.run_sweep(now_epoch_s=t0 + 610.0, run_chrony=True, force_collector_hc=True)
    assert "collector_probe_failed" not in r3["active_alerts"]
    assert "clock_unsynced" in r3["active_alerts"]
    assert r3["collector_healthcheck"]["healthy"] is False
    assert transport.hc_calls[-1]["is_fail"] is True

    # 4. Chrony recovers, BUT disk probe raises -> disk_probe_failed -> blocks healthy ping!
    chrony_ok[0] = True
    disk_fail[0] = True
    for ex in ("binance_futures", "bybit_linear", "bitget_v3", "gate_futures"):
        pp = next((data_root / "raw" / ex / "2026-12-03").glob("*.partial"))
        os.utime(pp, (t0 + 670.0, t0 + 670.0))
    r4 = mon.run_sweep(now_epoch_s=t0 + 675.0, run_chrony=True, force_collector_hc=True)
    assert "disk_probe_failed" in r4["active_alerts"]
    assert r4["collector_healthcheck"]["healthy"] is False
    assert transport.hc_calls[-1]["is_fail"] is True

    # 5. All probes & freshness genuinely healthy -> ONLY NOW sends success heartbeat!
    disk_fail[0] = False
    r5 = mon.run_sweep(now_epoch_s=t0 + 740.0, run_chrony=True, force_collector_hc=True)
    assert r5["active_alerts"] == []
    assert r5["collector_healthcheck"]["healthy"] is True
    assert transport.hc_calls[-1]["is_fail"] is False

    # 6. Fresh startup grace without any data files yet -> no stale alert, yet HC blocks ok=True!
    data_empty = tmp_path / "empty_startup"
    cfg2_obj = json.loads(cfg_path.read_text(encoding="utf-8"))
    cfg2_obj["deployment"]["data_root"] = str(data_empty)
    cfg2_path = tmp_path / "p3_empty_startup.json"
    cfg2_path.write_text(json.dumps(cfg2_obj), encoding="utf-8")
    svc_mode[0] = "startup_grace"
    transport2 = FakeTransport()
    mon2 = _build_monitor(cfg2_path, transport2, service_probe=probe)
    r_boot = mon2.run_sweep(now_epoch_s=t0, force_collector_hc=True)
    assert "collector_data_stale" not in r_boot["active_alerts"]
    assert r_boot["collector_healthcheck"]["healthy"] is False
    assert transport2.hc_calls[-1]["is_fail"] is True
    assert "data_freshness_unverified" in transport2.hc_calls[-1]["body"]


def test_p3_dual_outage_no_premature_recovery_without_endpoint_evidence(tmp_path: Path) -> None:
    """Connected handshake + sibling stream .partial writes MUST NOT clear dual A/B outage in conservative mode."""
    import subprocess

    data_root = tmp_path / "tokyo_l2"
    cfg_path = _write_test_config(tmp_path, data_root)
    t0 = 1_796_292_720.0
    partials: dict[str, Path] = {}
    for ex in ("binance_futures", "bybit_linear", "bitget_v3", "gate_futures"):
        pp, dp, mp = _seed_exchange_files(data_root, ex, now_s=t0, partial_age_s=2.0)
        partials[ex] = pp
        _seed_ledger_row(data_root, dp, mp, exchange=ex, state="VERIFIED")

    journal_queue: list[str] = []

    def j_reader(_u: str, cur: str | None) -> tuple[list[str], str | None]:
        out = list(journal_queue)
        journal_queue.clear()
        return out, f"cur_{len(out)}_{cur or '0'}"

    transport = FakeTransport()
    mon = _build_monitor(cfg_path, transport, journal_reader=j_reader)
    assert mon.dual_outage_recovery_mode == "conservative"
    assert mon.run_sweep(now_epoch_s=t0)["active_alerts"] == []
    transport.tg_messages.clear()
    transport.hc_calls.clear()

    # Two endpoints go into dual outage simultaneously: binance_public + gate_public
    journal_queue.extend(
        [
            "ERROR live: [ALERT CRITICAL] binance_public dual A/B outage exceeded 5s",
            "ERROR live: [ALERT CRITICAL] gate_public dual A/B outage exceeded 5s",
        ]
    )
    r1 = mon.run_sweep(now_epoch_s=t0 + 10.0, force_collector_hc=True)
    assert "dual_ab_outage_gt_5s" in r1["active_alerts"]
    assert r1["collector_healthcheck"]["healthy"] is False
    assert transport.hc_calls[-1]["is_fail"] is True

    # Scenario: WebSocket connected cleanly (`Connected binance_public`), subscription never succeeded / zero
    # binance_public market frames arrive, while sibling stream (`binance_market`) continuously writes the shared
    # binance_futures `.partial` file for 30+ minutes -> MUST stay FIRING and HC MUST remain `/fail`!
    journal_queue.append("INFO tokyo_l2_collector::live: Connected binance_public conn=A epoch=2")
    for step in (75.0, 600.0, 1200.0, 1800.0, 2100.0):
        for pp in partials.values():
            os.utime(pp, (t0 + step - 1.0, t0 + step - 1.0))
        r_still = mon.run_sweep(now_epoch_s=t0 + step, force_collector_hc=(step == 1800.0))
        assert "dual_ab_outage_gt_5s" in r_still["active_alerts"]
        assert r_still["alert_dispatch"]["sent_recoveries"] == []
        assert r_still["collector_healthcheck"]["healthy"] in (None, False)
        if step == 1800.0:
            assert r_still["collector_healthcheck"]["healthy"] is False
            assert transport.hc_calls[-1]["is_fail"] is True

    # External CLI partial ack (`--ack-outage gate_public`) MUST ONLY queue a request file without modifying monitor_state.json!
    state_before = mon.state_path.read_text(encoding="utf-8")
    res_part = subprocess.run(
        [sys.executable, str(PYTHON_DIR / "meta_daemon.py"), "--config", str(cfg_path), "--ack-outage", "gate_public"],
        capture_output=True,
        text=True,
        check=True,
    )
    ack_part_payload = json.loads(res_part.stdout)
    assert ack_part_payload["status"] == "queued"
    assert ack_part_payload["target"] == "gate_public"
    assert "does NOT verify" in ack_part_payload["warning"]
    assert mon.state_path.read_text(encoding="utf-8") == state_before

    for pp in partials.values():
        os.utime(pp, (t0 + 2150.0, t0 + 2150.0))
    r_partial = mon.run_sweep(now_epoch_s=t0 + 2155.0, force_collector_hc=True)
    assert "dual_ab_outage_gt_5s" in r_partial["active_alerts"]
    assert r_partial["outage_acks_applied"][0]["cleared_endpoints"] == ["gate_public"]
    assert mon.state["dual_outage_endpoints"].keys() == {"binance_public"}
    assert list(mon.ack_requests_dir.glob("req_*.json")) == []
    assert r_partial["alert_dispatch"]["sent_recoveries"] == []
    assert r_partial["collector_healthcheck"]["healthy"] is False

    # External CLI full ack (`--ack-outage`) queues request -> running monitor consumes it cleanly, dispatches recovery & restores HC green!
    res_all = subprocess.run(
        [sys.executable, str(PYTHON_DIR / "meta_daemon.py"), "--config", str(cfg_path), "--ack-outage"],
        capture_output=True,
        text=True,
        check=True,
    )
    ack_all_payload = json.loads(res_all.stdout)
    assert ack_all_payload["status"] == "queued"
    assert ack_all_payload["target"] == "ALL"

    for pp in partials.values():
        os.utime(pp, (t0 + 2200.0, t0 + 2200.0))
    r_rec = mon.run_sweep(now_epoch_s=t0 + 2205.0, force_collector_hc=True)
    assert r_rec["active_alerts"] == []
    assert r_rec["outage_acks_applied"][0]["cleared_endpoints"] == ["binance_public"]
    assert list(mon.ack_requests_dir.glob("req_*.json")) == []
    assert r_rec["alert_dispatch"]["sent_recoveries"] == ["dual_ab_outage_gt_5s"]
    assert r_rec["collector_healthcheck"]["healthy"] is True
    assert transport.hc_calls[-1]["is_fail"] is False


def test_p3_dual_outage_explicit_gap_end_mode_ignores_connected_and_sibling_partial(tmp_path: Path) -> None:
    """Even under explicit_gap_end mode, Connected (even w/o subscribe errors) + sibling writes MUST NOT auto-clear."""
    data_root = tmp_path / "tokyo_l2"
    cfg_path = _write_test_config(tmp_path, data_root)
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    cfg["meta_daemon"]["dual_outage_recovery_mode"] = "explicit_gap_end"
    cfg_path.write_text(json.dumps(cfg, indent=2), encoding="utf-8")

    t0 = 1_796_292_720.0
    partials: dict[str, Path] = {}
    for ex in ("binance_futures", "bybit_linear", "bitget_v3", "gate_futures"):
        pp, dp, mp = _seed_exchange_files(data_root, ex, now_s=t0, partial_age_s=2.0)
        partials[ex] = pp
        _seed_ledger_row(data_root, dp, mp, exchange=ex, state="VERIFIED")

    journal_queue: list[str] = []

    def j_reader(_u: str, cur: str | None) -> tuple[list[str], str | None]:
        out = list(journal_queue)
        journal_queue.clear()
        return out, f"cur_{len(out)}_{cur or '0'}"

    transport = FakeTransport()
    mon = _build_monitor(cfg_path, transport, journal_reader=j_reader)
    assert mon.dual_outage_recovery_mode == "explicit_gap_end"
    assert mon.run_sweep(now_epoch_s=t0)["active_alerts"] == []
    transport.hc_calls.clear()

    # Include ANSI color escapes (matching tracing_subscriber real output)
    journal_queue.extend(
        [
            "\x1b[31mERROR\x1b[0m tokyo_l2_collector::live: [ALERT CRITICAL] binance_public dual A/B outage exceeded 5s",
            "\x1b[31mERROR\x1b[0m tokyo_l2_collector::live: [ALERT CRITICAL] gate_public dual A/B outage exceeded 5s",
        ]
    )
    r1 = mon.run_sweep(now_epoch_s=t0 + 10.0)
    assert "dual_ab_outage_gt_5s" in r1["active_alerts"]

    # Clean ANSI Connected log without market frames + sibling .partial continuously written for 30+ min -> stays FIRING!
    journal_queue.append("\x1b[32mINFO\x1b[0m tokyo_l2_collector::live: Connected binance_public conn=A epoch=2")
    for step in (75.0, 900.0, 1900.0):
        for pp in partials.values():
            os.utime(pp, (t0 + step - 1.0, t0 + step - 1.0))
        r_still = mon.run_sweep(now_epoch_s=t0 + step, force_collector_hc=(step == 1900.0))
        assert "dual_ab_outage_gt_5s" in r_still["active_alerts"]
        assert r_still["alert_dispatch"]["sent_recoveries"] == []
        if step == 1900.0:
            assert r_still["collector_healthcheck"]["healthy"] is False
            assert transport.hc_calls[-1]["is_fail"] is True

    # gate_public recovers cleanly via explicit GAP_END, while binance_public stays broken -> stays FIRING!
    journal_queue.append('INFO live: {"event":"GAP_END","endpoint":"gate_public","recovered_conn":"A"}')
    for pp in partials.values():
        os.utime(pp, (t0 + 1965.0, t0 + 1965.0))
    r_partial_rec = mon.run_sweep(now_epoch_s=t0 + 1970.0)
    assert "dual_ab_outage_gt_5s" in r_partial_rec["active_alerts"]
    assert mon.state["dual_outage_endpoints"].keys() == {"binance_public"}

    # Finally binance_public explicitly emits GAP_END recovery + fresh binance_futures data -> RECOVERS!
    journal_queue.append("INFO live: GAP_END endpoint=binance_public")
    for pp in partials.values():
        os.utime(pp, (t0 + 1985.0, t0 + 1985.0))
    r_full_rec = mon.run_sweep(now_epoch_s=t0 + 1990.0, force_collector_hc=True)
    assert r_full_rec["active_alerts"] == []
    assert r_full_rec["alert_dispatch"]["sent_recoveries"] == ["dual_ab_outage_gt_5s"]
    assert r_full_rec["collector_healthcheck"]["healthy"] is True


def test_p3_hourly_report_prev_hour_vs_cumulative_metrics(tmp_path: Path) -> None:
    """Review Issue 3: Hourly battle report separates previous complete UTC hour vs cumulative history."""
    data_root = tmp_path / "tokyo_l2"
    cfg_path = _write_test_config(tmp_path, data_root)
    # 2026-12-03T10:12:00 UTC -> previous complete UTC hour MUST be 2026-12-03T09!
    t0 = 1_796_292_720.0

    for ex in ("binance_futures", "bybit_linear", "bitget_v3", "gate_futures"):
        # Old hour 07 manifest with historical LOSS=7 & DATA=50000
        _, dp_old, mp_old = _seed_exchange_files(
            data_root,
            ex,
            now_s=t0,
            hour_utc="07",
            data_lines=50000,
            local_loss_lines=7 if ex == "gate_futures" else 0,
            gap_start_lines=3 if ex == "gate_futures" else 0,
            switch_lines=2 if ex == "gate_futures" else 0,
            boot_id="boot_0",
        )
        _seed_ledger_row(data_root, dp_old, mp_old, exchange=ex, hour_utc="07", state="PRUNED")

        # Previous complete hour 09 manifest with clean LOSS=0 & DATA=12000
        _, dp_prev, mp_prev = _seed_exchange_files(
            data_root,
            ex,
            now_s=t0,
            hour_utc="09",
            data_lines=12000 if ex == "gate_futures" else 8000,
            local_loss_lines=0,
            gap_start_lines=1 if ex == "gate_futures" else 0,
            switch_lines=1 if ex == "gate_futures" else 0,
            boot_id="boot_1",
        )
        _seed_ledger_row(data_root, dp_prev, mp_prev, exchange=ex, hour_utc="09", state="VERIFIED")

    transport = FakeTransport()
    mon = _build_monitor(cfg_path, transport)
    r = mon.run_sweep(now_epoch_s=t0, force_hourly_report=True)
    report = r["hourly_report"]["text"]
    assert report is not None

    # Previous-hour market data is visible; detailed historical totals stay in the state.
    assert "统计：2026-12-03T09 UTC" in report
    assert "Gate: 12,000 行 · GAP 1 · LOSS 0" in report
    assert "云端归档：VERIFIED" in report
    assert "V=4 / P=4" in report
    assert r["data_tree"]["per_exchange"]["gate_futures"]["cumulative_data_lines"] == 62000
    assert r["data_tree"]["per_exchange"]["gate_futures"]["cumulative_local_loss_lines"] == 7

    # Verify UTC midnight boundary: at 2026-12-04T00:12:00 UTC -> prev_hour_key = 2026-12-03T23
    t_midnight = 1_796_343_120.0
    _, dp_23, mp_23 = _seed_exchange_files(
        data_root,
        "binance_futures",
        now_s=t_midnight,
        date_utc="2026-12-03",
        hour_utc="23",
        data_lines=9999,
        local_loss_lines=0,
    )
    _seed_ledger_row(data_root, dp_23, mp_23, exchange="binance_futures", hour_utc="23", state="VERIFIED")
    r_mid = mon.run_sweep(now_epoch_s=t_midnight, force_hourly_report=True)
    mid_report = r_mid["hourly_report"]["text"]
    assert mid_report is not None
    assert "统计：2026-12-03T23 UTC" in mid_report
    assert "Binance: 9,999 行" in mid_report


def _ws_encode_frame(opcode: int, payload: bytes) -> bytes:
    header = bytearray([0x80 | (opcode & 0x0F)])
    n = len(payload)
    if n < 126:
        header.append(n)
    elif n < 65536:
        header.append(126)
        header.extend(
            __import__("struct").pack("!H", n)
        )
    else:
        header.append(127)
        header.extend(
            __import__("struct").pack("!Q", n)
        )
    return bytes(header) + payload


def _ws_read_pending(conn: Any, rx_buf: bytearray) -> bool:
    """Drain masked client WS frames, answer Ping with Pong, return False on EOF/Close."""
    import select
    import struct

    try:
        readable, _, _ = select.select([conn], [], [], 0.0)
        if not readable:
            return True
        chunk = conn.recv(4096)
    except (TimeoutError, BlockingIOError):
        return True
    except (OSError, ValueError):
        return False
    if not chunk:
        return False
    rx_buf.extend(chunk)
    while len(rx_buf) >= 2:
        b0, b1 = rx_buf[0], rx_buf[1]
        opcode = b0 & 0x0F
        masked = bool(b1 & 0x80)
        plen = b1 & 0x7F
        idx = 2
        if plen == 126:
            if len(rx_buf) < 4:
                break
            plen = struct.unpack("!H", rx_buf[2:4])[0]
            idx = 4
        elif plen == 127:
            if len(rx_buf) < 10:
                break
            plen = struct.unpack("!Q", rx_buf[2:10])[0]
            idx = 10
        mask_len = 4 if masked else 0
        total = idx + mask_len + plen
        if len(rx_buf) < total:
            break
        mask = rx_buf[idx : idx + 4] if masked else b""
        raw_payload = bytes(rx_buf[idx + mask_len : total])
        del rx_buf[:total]
        payload = (
            bytes(raw_payload[i] ^ mask[i % 4] for i in range(len(raw_payload)))
            if masked
            else raw_payload
        )
        if opcode == 0x8:  # CLOSE
            try:
                conn.sendall(_ws_encode_frame(0x8, b""))
            except OSError:
                pass
            return False
        if opcode == 0x9:  # PING -> PONG
            try:
                conn.sendall(_ws_encode_frame(0xA, payload))
            except OSError:
                return False
    return True


class _LoopbackBinanceWsServer:
    """Tiny pure-stdlib RFC-6455 loopback WS endpoint supporting streaming / reject / silent_connected modes."""

    def __init__(self, kind: str) -> None:
        import socket
        import threading

        self.kind = kind
        self.mode = "streaming"
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._conns: set[socket.socket] = set()
        self._srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._srv.bind(("127.0.0.1", 0))
        self._srv.listen(32)
        self._srv.settimeout(0.1)
        self.port = int(self._srv.getsockname()[1])
        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._accept_thread.start()

    def set_mode(self, mode: str, *, close_existing: bool = True) -> None:
        import socket

        with self._lock:
            self.mode = mode
            conns = list(self._conns) if close_existing else []
        for c in conns:
            try:
                c.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                c.close()
            except OSError:
                pass

    def stop(self) -> None:
        self._stop.set()
        self.set_mode("reject", close_existing=True)
        try:
            self._srv.close()
        except OSError:
            pass
        self._accept_thread.join(timeout=2.0)

    def _accept_loop(self) -> None:
        import threading

        while not self._stop.is_set():
            try:
                conn, _ = self._srv.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            t = threading.Thread(target=self._handle_client, args=(conn,), daemon=True)
            t.start()

    def _handle_client(self, conn: Any) -> None:
        import base64
        import hashlib
        import time

        try:
            conn.settimeout(1.0)
            req = bytearray()
            while b"\r\n\r\n" not in req:
                chunk = conn.recv(4096)
                if not chunk:
                    return
                req.extend(chunk)
                if len(req) > 65536:
                    return

            with self._lock:
                cur_mode = self.mode
            if cur_mode == "reject":
                conn.sendall(
                    b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
                return

            ws_key = ""
            for line in bytes(req).decode("latin1", errors="ignore").split("\r\n"):
                if line.lower().startswith("sec-websocket-key:"):
                    ws_key = line.split(":", 1)[1].strip()
                    break
            if not ws_key:
                return
            accept = base64.b64encode(
                hashlib.sha1(
                    (ws_key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")
                ).digest()
            ).decode("ascii")
            conn.sendall(
                (
                    "HTTP/1.1 101 Switching Protocols\r\n"
                    "Upgrade: websocket\r\n"
                    "Connection: Upgrade\r\n"
                    f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
                ).encode("ascii")
            )
            with self._lock:
                if self._stop.is_set() or self.mode == "reject":
                    return
                self._conns.add(conn)

            conn.settimeout(1.0)
            rx_buf = bytearray()
            seq = 1000
            prev_u = 999
            agg_id = 50_000
            last_send = 0.0

            while not self._stop.is_set():
                if not _ws_read_pending(conn, rx_buf):
                    break
                with self._lock:
                    cur_mode = self.mode
                if cur_mode == "reject":
                    break
                if cur_mode == "silent_connected":
                    time.sleep(0.05)
                    continue

                now_mono = time.monotonic()
                interval_s = 0.10 if self.kind == "public" else 0.02
                if now_mono - last_send < interval_s:
                    time.sleep(0.01)
                    continue
                last_send = now_mono
                now_ms = int(time.time() * 1000)

                if self.kind == "public":
                    fu = seq
                    lu = seq + 5
                    depth_payload = json.dumps(
                        {
                            "stream": "btcusdt@depth@100ms",
                            "data": {
                                "e": "depthUpdate",
                                "E": now_ms,
                                "T": now_ms,
                                "s": "BTCUSDT",
                                "U": fu,
                                "u": lu,
                                "pu": prev_u,
                                "b": [["95000.0", "3.5"], ["94999.9", "2.1"]],
                                "a": [["95000.1", "2.9"], ["95000.2", "4.4"]],
                            },
                        }
                    ).encode("utf-8")
                    conn.sendall(_ws_encode_frame(0x1, depth_payload))
                    prev_u = lu
                    seq = lu + 1

                    bt_payload = json.dumps(
                        {
                            "stream": "btcusdt@bookTicker",
                            "data": {
                                "e": "bookTicker",
                                "u": lu,
                                "s": "BTCUSDT",
                                "b": "95000.0",
                                "B": "3.5",
                                "a": "95000.1",
                                "A": "2.9",
                                "T": now_ms,
                                "E": now_ms,
                            },
                        }
                    ).encode("utf-8")
                    conn.sendall(_ws_encode_frame(0x1, bt_payload))
                else:
                    frames = bytearray()
                    for _ in range(30):
                        agg_id += 1
                        px = 95000.0 + ((agg_id * 37) % 997) * 0.1
                        qty = 0.01 + ((agg_id * 19) % 89) * 0.01
                        trade_payload = json.dumps(
                            {
                                "stream": "btcusdt@aggTrade",
                                "data": {
                                    "e": "aggTrade",
                                    "E": now_ms + (agg_id % 17),
                                    "a": agg_id,
                                    "s": "BTCUSDT",
                                    "p": f"{px:.1f}",
                                    "q": f"{qty:.2f}",
                                    "f": agg_id * 2,
                                    "l": agg_id * 2 + 1,
                                    "T": now_ms + (agg_id % 17),
                                    "m": bool(agg_id % 2),
                                },
                            }
                        ).encode("utf-8")
                        frames.extend(_ws_encode_frame(0x1, trade_payload))
                    conn.sendall(bytes(frames))
        except OSError:
            pass
        finally:
            with self._lock:
                self._conns.discard(conn)
            try:
                conn.close()
            except OSError:
                pass


class _LoopbackBinanceCluster:
    """Localhost REST depth + split /public & /market WS cluster for real Rust collector testing."""

    def __init__(self) -> None:
        import socket
        import threading

        self._stop = threading.Event()
        self.rest_hits = 0
        self._rest_lock = threading.Lock()
        self._rest_srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._rest_srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._rest_srv.bind(("127.0.0.1", 0))
        self._rest_srv.listen(16)
        self._rest_srv.settimeout(0.1)
        self.rest_port = int(self._rest_srv.getsockname()[1])
        self._rest_thread = threading.Thread(target=self._serve_rest, daemon=True)
        self._rest_thread.start()

        self.pub_ws = _LoopbackBinanceWsServer("public")
        self.mkt_ws = _LoopbackBinanceWsServer("market")

    def _serve_rest(self) -> None:
        body = '{"lastUpdateId":1005,"bids":[["95000.0","5.0"]],"asks":[["95000.1","4.0"]]}'
        resp = (
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\n"
            "Connection: close\r\n\r\n"
            + body
        ).encode("utf-8")
        while not self._stop.is_set():
            try:
                conn, _ = self._rest_srv.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            try:
                conn.settimeout(0.5)
                _ = conn.recv(1024)
                conn.sendall(resp)
                with self._rest_lock:
                    self.rest_hits += 1
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
            self._rest_srv.close()
        except OSError:
            pass
        self.pub_ws.stop()
        self.mkt_ws.stop()
        self._rest_thread.join(timeout=2.0)


def _wait_until(pred: Any, *, timeout_s: float = 15.0, desc: str = "condition", on_timeout: Any = None) -> None:
    import time

    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if pred():
            return
        time.sleep(0.05)
    extra = f" | diag={on_timeout()}" if on_timeout is not None else ""
    raise AssertionError(f"Timed out after {timeout_s}s waiting for: {desc}{extra}")


def test_p3_real_rust_collector_dual_outage_logs_and_conservative_lock(tmp_path: Path) -> None:
    """Real Rust collector stderr linkage: no GAP_END in stderr, silent Connected + sibling .partial never clears."""
    import signal
    import subprocess
    import threading
    import time

    rust_bin = cli_mod._find_or_build_rust_binary()
    cluster = _LoopbackBinanceCluster()
    data_root = tmp_path / "tokyo_l2"
    cfg_path = _write_test_config(tmp_path, data_root)
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    cfg["deployment"]["silence_watchdog_seconds"] = 5
    cfg["meta_daemon"]["dual_outage_alert_seconds"] = 1
    cfg["meta_daemon"]["dual_outage_recovery_mode"] = "conservative"
    cfg["exchanges"]["binance_futures"]["enabled"] = True
    cfg["exchanges"]["binance_futures"]["symbols"] = ["BTCUSDT"]
    cfg["exchanges"]["binance_futures"]["public_ws_url"] = f"ws://127.0.0.1:{cluster.pub_ws.port}/public/stream"
    cfg["exchanges"]["binance_futures"]["market_ws_url"] = f"ws://127.0.0.1:{cluster.mkt_ws.port}/market/stream"
    cfg["exchanges"]["binance_futures"]["rest_depth_url"] = f"http://127.0.0.1:{cluster.rest_port}/fapi/v1/depth"
    cfg["exchanges"]["bybit_linear"]["enabled"] = False
    cfg["exchanges"]["bitget_v3"]["enabled"] = False
    cfg["exchanges"]["gate_futures"]["enabled"] = False
    cfg_path.write_text(json.dumps(cfg, indent=2), encoding="utf-8")

    stderr_lines: list[str] = []
    stderr_lock = threading.Lock()
    child_env = os.environ.copy()
    for k in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        child_env.pop(k, None)
    child_env["NO_PROXY"] = "127.0.0.1,localhost"
    child_env["no_proxy"] = "127.0.0.1,localhost"
    child_env["TOKYO_L2_LIVE_AUTHORIZED"] = "1"
    child_env["TOKYO_L2_ALLOW_LOCALHOST_TEST_ENDPOINTS"] = "1"
    child_env["RUST_LOG"] = "info"

    proc = subprocess.Popen(
        [
            str(rust_bin),
            "collect",
            "--config",
            str(cfg_path),
            "--run-id",
            "p3_real_link",
            "--allow-live-run",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        env=child_env,
    )

    def _drain_stderr() -> None:
        assert proc.stderr is not None
        for line in proc.stderr:
            with stderr_lock:
                stderr_lines.append(line.rstrip("\r\n"))

    stderr_thread = threading.Thread(target=_drain_stderr, daemon=True)
    stderr_thread.start()

    try:
        # Incremental journal_reader wired directly to REAL `tokyo-l2-collector` stderr output!
        def real_journal_reader(_unit: str, cur: str | None) -> tuple[list[str], str | None]:
            idx = int(cur or "0")
            with stderr_lock:
                new_lines = list(stderr_lines[idx:])
            return new_lines, str(idx + len(new_lines))

        def snapshot_stderr(start: int = 0) -> list[str]:
            with stderr_lock:
                return list(stderr_lines[start:])

        def _diag() -> dict[str, Any]:
            parts = [
                (p.name, p.stat().st_size)
                for p in (data_root / "raw" / "binance_futures").rglob("*.partial")
            ] if (data_root / "raw" / "binance_futures").exists() else []
            return {
                "rc": proc.poll(),
                "rest_hits": cluster.rest_hits,
                "partials": parts,
                "stderr_tail": snapshot_stderr()[-15:],
            }

        transport = FakeTransport()
        mon = _build_monitor(cfg_path, transport, journal_reader=real_journal_reader)
        assert mon.enabled_exchanges == ["binance_futures"]
        assert mon.dual_outage_recovery_mode == "conservative"

        # Stage 1: Healthy streaming + initial REST depth snapshots + active binance_futures .partial
        _wait_until(
            lambda: (
                any("Connected binance_public" in ln for ln in snapshot_stderr())
                and any("Connected binance_market" in ln for ln in snapshot_stderr())
                and cluster.rest_hits >= 2
                and len(list((data_root / "raw" / "binance_futures").rglob("*.tsv.zst.partial"))) == 1
                and next((data_root / "raw" / "binance_futures").rglob("*.tsv.zst.partial")).stat().st_size > 0
            ),
            timeout_s=15.0,
            desc="initial Rust WS connects, REST snapshots, and non-empty .partial",
            on_timeout=_diag,
        )
        partial_path = next((data_root / "raw" / "binance_futures").rglob("*.tsv.zst.partial"))

        r_init = mon.run_sweep(now_epoch_s=time.time(), force_collector_hc=True)
        assert r_init["active_alerts"] == []
        assert r_init["collector_healthcheck"]["healthy"] is True
        transport.tg_messages.clear()
        transport.hc_calls.clear()

        # Stage 2: Drop binance_public A & B completely (`reject`), while sibling binance_market keeps writing .partial
        cluster.pub_ws.set_mode("reject", close_existing=True)
        _wait_until(
            lambda: any(
                "binance_public dual A/B outage exceeded" in md_mod._RE_ANSI.sub("", ln)
                for ln in snapshot_stderr()
            ),
            timeout_s=15.0,
            desc="real Rust collector dual A/B outage alert in stderr",
            on_timeout=_diag,
        )
        size_outage_1 = partial_path.stat().st_size
        _wait_until(
            lambda: partial_path.stat().st_size > size_outage_1,
            timeout_s=8.0,
            desc="sibling binance_market keeps growing shared .partial during outage",
            on_timeout=_diag,
        )

        r_outage = mon.run_sweep(now_epoch_s=time.time(), force_collector_hc=True)
        assert "dual_ab_outage_gt_5s" in r_outage["active_alerts"]
        assert mon.state["dual_outage_endpoints"].keys() == {"binance_public"}
        assert r_outage["collector_healthcheck"]["healthy"] is False
        assert transport.hc_calls[-1]["is_fail"] is True
        assert any("行情双连接中断" in m["text"] for m in transport.tg_messages)
        transport.tg_messages.clear()

        # Stage 3: binance_public WS reconnects cleanly (`silent_connected` = Connected logged, ZERO frames),
        # while sibling binance_market CONTINUES growing the shared binance_futures .partial!
        mark_stage3 = len(snapshot_stderr())
        rest_before_s3 = cluster.rest_hits
        size_stage3_start = partial_path.stat().st_size
        cluster.pub_ws.set_mode("silent_connected", close_existing=True)
        _wait_until(
            lambda: (
                any("Connected binance_public conn=A" in md_mod._RE_ANSI.sub("", ln) for ln in snapshot_stderr(mark_stage3))
                and any("Connected binance_public conn=B" in md_mod._RE_ANSI.sub("", ln) for ln in snapshot_stderr(mark_stage3))
                and cluster.rest_hits > rest_before_s3
                and partial_path.stat().st_size > size_stage3_start
            ),
            timeout_s=15.0,
            desc="Connected binance_public A+B + Stage 3 REST attempt while sibling .partial keeps growing",
            on_timeout=_diag,
        )

        # Advance monitor clock +90s (> legacy 60s): conservative lock MUST remain FIRING!
        stage3_clean_logs = [md_mod._RE_ANSI.sub("", ln) for ln in snapshot_stderr(mark_stage3)]
        assert any("Connected binance_public conn=A" in ln for ln in stage3_clean_logs)
        assert any("Connected binance_public conn=B" in ln for ln in stage3_clean_logs)

        t_stage3 = time.time() + 90.0
        r_silent = mon.run_sweep(now_epoch_s=t_stage3, force_collector_hc=True)
        assert "dual_ab_outage_gt_5s" in r_silent["active_alerts"]
        assert mon.state["dual_outage_endpoints"].keys() == {"binance_public"}
        assert r_silent["alert_dispatch"]["sent_recoveries"] == []
        assert not any("[已恢复]" in m["text"] for m in transport.tg_messages)
        assert r_silent["collector_healthcheck"]["healthy"] is False
        assert transport.hc_calls[-1]["is_fail"] is True
        diag_s3 = mon.state["dual_outage_endpoints"]["binance_public"]["diagnostics"]
        assert int(diag_s3.get("ws_connects_since_outage") or 0) >= 2, (
            f"Expected ingested Connected telemetry in ep_state={mon.state['dual_outage_endpoints']}; "
            f"stderr={snapshot_stderr()}"
        )

        # Stage 4a: Reconnecting live stream (`streaming`) still leaves unmodified Rust collector unable to recover
        # logs cleanly (Connected + sibling .partial keeps growing, ZERO `GAP_END` in stderr).
        # Conservative monitor MUST stay FIRING until explicit CLI `--ack-outage`.
        mark_stage4 = len(snapshot_stderr())
        cluster.pub_ws.set_mode("streaming", close_existing=True)
        _wait_until(
            lambda: any("Connected binance_public" in md_mod._RE_ANSI.sub("", ln) for ln in snapshot_stderr(mark_stage4)),
            timeout_s=15.0,
            desc="binance_public reconnect under streaming mode",
            on_timeout=_diag,
        )
        size_resumed = partial_path.stat().st_size
        _wait_until(
            lambda: partial_path.stat().st_size > size_resumed,
            timeout_s=8.0,
            desc="post-outage .partial continues growing from sibling stream",
            on_timeout=_diag,
        )

        r_still_locked = mon.run_sweep(now_epoch_s=t_stage3 + 10.0, force_collector_hc=True)
        assert "dual_ab_outage_gt_5s" in r_still_locked["active_alerts"]
        assert r_still_locked["alert_dispatch"]["sent_recoveries"] == []
        assert not any("GAP_END" in md_mod._RE_ANSI.sub("", ln) for ln in snapshot_stderr())

        # Operator runs `--ack-outage binance_public` after verifying live recovery -> clears lock & HC turns green!
        ack_res = subprocess.run(
            [
                sys.executable,
                str(PYTHON_DIR / "meta_daemon.py"),
                "--config",
                str(cfg_path),
                "--ack-outage",
                "binance_public",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        ack_json = json.loads(ack_res.stdout)
        assert ack_json["status"] == "queued"
        assert ack_json["target"] == "binance_public"

        r_acked = mon.run_sweep(now_epoch_s=t_stage3 + 15.0, force_collector_hc=True)
        assert r_acked["active_alerts"] == []
        assert r_acked["outage_acks_applied"][0]["cleared_endpoints"] == ["binance_public"]
        assert r_acked["alert_dispatch"]["sent_recoveries"] == ["dual_ab_outage_gt_5s"]
        assert any("[已恢复]" in m["text"] and "行情双连接中断" in m["text"] for m in transport.tg_messages)
        assert r_acked["collector_healthcheck"]["healthy"] is True
        assert transport.hc_calls[-1]["is_fail"] is False

        # Stage 4b: SIGINT seals binance_futures; confirm real Rust wrote GAP_START/META only inside .tsv.zst,
        # never to stderr (where GAP_END never appears either).
        proc.send_signal(signal.SIGINT)
        rc = proc.wait(timeout=15)
        assert rc == 0, f"tokyo-l2-collector exited {rc}; stderr tail: {snapshot_stderr()[-20:]}"
        stderr_thread.join(timeout=3.0)

        live_stderr_all = snapshot_stderr()
        assert not any("GAP_END" in md_mod._RE_ANSI.sub("", ln) for ln in live_stderr_all)
        assert not any("GAP_START" in md_mod._RE_ANSI.sub("", ln) for ln in live_stderr_all)
        sealed_files = list((data_root / "raw" / "binance_futures").rglob("*.tsv.zst"))
        assert len(sealed_files) == 1
        dctx = zstd.ZstdDecompressor()
        with sealed_files[0].open("rb") as fh:
            raw_tsv = dctx.stream_reader(fh).read().decode("utf-8", errors="replace")
        meta_lines = [ln for ln in raw_tsv.splitlines() if "\tMETA\t" in ln]
        assert '"event":"GAP_START"' in raw_tsv, f"Missing GAP_START. meta_lines={meta_lines}"
        assert '"endpoint":"binance_public"' in raw_tsv, f"Missing binance_public META. meta_lines={meta_lines}"

        # Stage 5: Confirm via real `tokyo-l2-collector replay-fixture` that even when CausalSelector emits GAP_END,
        # the unmodified Rust binary writes GAP_END exclusively to compressed `.tsv.zst` (NEVER stdout/stderr!).
        replay_cfg = json.loads((SPECS_DIR / "config.example.json").read_text(encoding="utf-8"))
        replay_out = tmp_path / "replay_out"
        replay_cfg["deployment"]["data_root"] = str(replay_out)
        replay_cfg["exchanges"]["binance_futures"]["symbols"] = ["BTCUSDT"]
        replay_cfg["exchanges"]["bybit_linear"]["enabled"] = False
        replay_cfg["exchanges"]["bitget_v3"]["enabled"] = False
        replay_cfg["exchanges"]["gate_futures"]["enabled"] = False
        replay_cfg_path = tmp_path / "replay_cfg.json"
        replay_cfg_path.write_text(json.dumps(replay_cfg), encoding="utf-8")

        depth_frame = json.dumps(
            {
                "stream": "btcusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1796292720000,
                    "T": 1796292720000,
                    "s": "BTCUSDT",
                    "U": 1000,
                    "u": 1005,
                    "pu": 999,
                    "b": [["95000.0", "1.0"]],
                    "a": [["95000.1", "1.0"]],
                },
            }
        )
        rest_snap = '{"lastUpdateId":1005,"bids":[],"asks":[]}'
        fixture_path = tmp_path / "gap_recovery.jsonl"
        fixture_events = [
            {"op": "reconnect", "recv_mono_ns": 10, "endpoint": "binance_public", "conn_id": "A", "epoch": 1},
            {"op": "frame", "recv_wall_ns": 1796292720000000000, "recv_mono_ns": 20, "endpoint": "binance_public", "conn_id": "A", "epoch": 1, "payload": depth_frame},
            {"op": "rest_snapshot", "recv_wall_ns": 1796292720100000000, "recv_mono_ns": 30, "endpoint": "binance_public", "conn_id": "A", "symbol": "BTCUSDT", "last_update_id": 1005, "raw_json": rest_snap},
            {"op": "disconnect", "recv_wall_ns": 1796292721000000000, "recv_mono_ns": 100, "endpoint": "binance_public", "conn_id": "A", "reason": "simulated_dual_loss"},
            {"op": "reconnect", "recv_mono_ns": 200, "endpoint": "binance_public", "conn_id": "A", "epoch": 2},
            {"op": "frame", "recv_wall_ns": 1796292725000000000, "recv_mono_ns": 210, "endpoint": "binance_public", "conn_id": "A", "epoch": 2, "payload": depth_frame},
            {"op": "rest_snapshot", "recv_wall_ns": 1796292725100000000, "recv_mono_ns": 220, "endpoint": "binance_public", "conn_id": "A", "symbol": "BTCUSDT", "last_update_id": 1005, "raw_json": rest_snap},
        ]
        fixture_path.write_text("\n".join(json.dumps(ev) for ev in fixture_events) + "\n", encoding="utf-8")

        replay_proc = subprocess.run(
            [
                str(rust_bin),
                "replay-fixture",
                "--config",
                str(replay_cfg_path),
                "--fixture",
                str(fixture_path),
                "--out",
                str(replay_out),
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        assert "GAP_END" not in replay_proc.stdout
        assert "GAP_END" not in replay_proc.stderr
        assert md_mod.inspect_collector_logs(replay_proc.stderr.splitlines())["endpoint_timeline"] == {}
        replay_tsv_files = list(replay_out.rglob("*.tsv.zst"))
        assert len(replay_tsv_files) == 1
        with replay_tsv_files[0].open("rb") as rfh:
            replay_tsv = zstd.ZstdDecompressor().stream_reader(rfh).read().decode("utf-8", errors="replace")
        assert '"event":"GAP_START"' in replay_tsv
        assert '"event":"GAP_END"' in replay_tsv
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
        stderr_thread.join(timeout=2.0)
        cluster.stop()


def test_p3_ack_outage_concurrent_with_daemon_sweep_never_overwrites_state(tmp_path: Path) -> None:
    """Single-writer state: concurrent CLI --ack-outage never clobbers concurrent monitor_state.json updates."""
    import concurrent.futures
    import subprocess
    import threading

    data_root = tmp_path / "tokyo_l2"
    cfg_path = _write_test_config(tmp_path, data_root)
    t0 = 1_796_292_720.0
    partials: dict[str, Path] = {}
    for ex in ("binance_futures", "bybit_linear", "bitget_v3", "gate_futures"):
        pp, dp, mp = _seed_exchange_files(data_root, ex, now_s=t0, partial_age_s=2.0)
        partials[ex] = pp
        _seed_ledger_row(data_root, dp, mp, exchange=ex, state="VERIFIED")

    journal_queue: list[str] = []
    in_hc_barrier = threading.Event()
    release_hc_barrier = threading.Event()
    armed = [False]

    def j_reader(_u: str, cur: str | None) -> tuple[list[str], str | None]:
        out = list(journal_queue)
        journal_queue.clear()
        return out, f"cur_{len(out)}_{cur or '0'}"

    class _BarrierTransport(FakeTransport):
        def send(self, url: str, body: bytes, headers: dict[str, str], timeout: float) -> tuple[int, bytes]:
            res = super().send(url, body, headers, timeout)
            if armed[0] and "/fail" in url:
                armed[0] = False
                in_hc_barrier.set()
                assert release_hc_barrier.wait(timeout=5.0), "timed out waiting to release sweep barrier"
            return res

    transport = _BarrierTransport()
    mon = _build_monitor(cfg_path, transport, journal_reader=j_reader)
    assert mon.run_sweep(now_epoch_s=t0)["active_alerts"] == []

    # 1. Existing outage on binance_public saved on disk
    journal_queue.append("ERROR live: [ALERT CRITICAL] binance_public dual A/B outage exceeded 5s")
    for pp in partials.values():
        os.utime(pp, (t0 + 10.0, t0 + 10.0))
    r1 = mon.run_sweep(now_epoch_s=t0 + 12.0, force_collector_hc=True)
    assert "dual_ab_outage_gt_5s" in r1["active_alerts"]
    assert mon.state["dual_outage_endpoints"].keys() == {"binance_public"}
    disk_before_mid = mon.state_path.read_text(encoding="utf-8")

    # 2. Daemon starts a sweep discovering NEW gate_public outage, pausing mid-sweep BEFORE _save_state().
    journal_queue.append("ERROR live: [ALERT CRITICAL] gate_public dual A/B outage exceeded 5s")
    for pp in partials.values():
        os.utime(pp, (t0 + 25.0, t0 + 25.0))
    armed[0] = True
    sweep_box: list[dict[str, Any]] = []
    sweep_err: list[BaseException] = []

    def _run_paused_sweep() -> None:
        try:
            sweep_box.append(mon.run_sweep(now_epoch_s=t0 + 30.0, force_collector_hc=True))
        except BaseException as exc:  # pragma: no cover
            sweep_err.append(exc)

    daemon_thr = threading.Thread(target=_run_paused_sweep, daemon=True)
    daemon_thr.start()
    assert in_hc_barrier.wait(timeout=5.0), "daemon sweep did not hit HC barrier"

    try:
        # Meanwhile operator runs TWO concurrent CLI acks (`binance_public` + `bybit_public`) while sweep is in flight!
        def _cli_ack(ep: str) -> dict[str, Any]:
            proc = subprocess.run(
                [sys.executable, str(PYTHON_DIR / "meta_daemon.py"), "--config", str(cfg_path), "--ack-outage", ep],
                capture_output=True,
                text=True,
                check=True,
            )
            return json.loads(proc.stdout)

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            f_bin = pool.submit(_cli_ack, "binance_public")
            f_byb = pool.submit(_cli_ack, "bybit_public")
            res_bin = f_bin.result(timeout=5.0)
            res_byb = f_byb.result(timeout=5.0)

        assert res_bin["status"] == "queued"
        assert res_byb["status"] == "queued"
        assert res_bin["request_id"] != res_byb["request_id"]
        assert len(list(mon.ack_requests_dir.glob("req_*.json"))) == 2
        # Critical concurrency guarantee: CLI NEVER mutated or overwrote monitor_state.json!
        assert mon.state_path.read_text(encoding="utf-8") == disk_before_mid
    finally:
        release_hc_barrier.set()
        daemon_thr.join(timeout=5.0)

    assert not sweep_err
    assert len(sweep_box) == 1
    # Newly discovered gate_public fault MUST be persisted cleanly alongside binance_public on disk!
    disk_after_mid = json.loads(mon.state_path.read_text(encoding="utf-8"))
    assert set(disk_after_mid["dual_outage_endpoints"].keys()) == {"binance_public", "gate_public"}
    assert len(list(mon.ack_requests_dir.glob("req_*.json"))) == 2

    # 3. Next sweep consumes both queued CLI acks atomically: clears binance_public, preserves gate_public!
    for pp in partials.values():
        os.utime(pp, (t0 + 40.0, t0 + 40.0))
    r_drained = mon.run_sweep(now_epoch_s=t0 + 45.0, force_collector_hc=True)
    assert "dual_ab_outage_gt_5s" in r_drained["active_alerts"]
    assert len(r_drained["outage_acks_applied"]) == 2
    assert mon.state["dual_outage_endpoints"].keys() == {"gate_public"}
    assert list(mon.ack_requests_dir.glob("req_*.json")) == []
    assert r_drained["alert_dispatch"]["sent_recoveries"] == []
    assert r_drained["collector_healthcheck"]["healthy"] is False

    # 4. `--ack-outage ALL` concurrent with an inbound `bitget_public` outage in the same sweep MUST clear gate_public
    #    while preserving the new bitget_public outage (drain happens BEFORE new journal outage ingestion)!
    subprocess.run(
        [sys.executable, str(PYTHON_DIR / "meta_daemon.py"), "--config", str(cfg_path), "--ack-outage"],
        capture_output=True,
        text=True,
        check=True,
    )
    journal_queue.append("ERROR live: [ALERT CRITICAL] bitget_public dual A/B outage exceeded 5s")
    for pp in partials.values():
        os.utime(pp, (t0 + 55.0, t0 + 55.0))
    r_overlap = mon.run_sweep(now_epoch_s=t0 + 60.0, force_collector_hc=True)
    assert "dual_ab_outage_gt_5s" in r_overlap["active_alerts"]
    assert mon.state["dual_outage_endpoints"].keys() == {"bitget_public"}
    assert list(mon.ack_requests_dir.glob("req_*.json")) == []

    # 5. Final ack clears bitget_public -> full recovery & HC green
    subprocess.run(
        [sys.executable, str(PYTHON_DIR / "meta_daemon.py"), "--config", str(cfg_path), "--ack-outage", "bitget_public"],
        capture_output=True,
        text=True,
        check=True,
    )
    for pp in partials.values():
        os.utime(pp, (t0 + 70.0, t0 + 70.0))
    r_clean = mon.run_sweep(now_epoch_s=t0 + 75.0, force_collector_hc=True)
    assert r_clean["active_alerts"] == []
    assert r_clean["alert_dispatch"]["sent_recoveries"] == ["dual_ab_outage_gt_5s"]
    assert r_clean["collector_healthcheck"]["healthy"] is True
    assert list(mon.ack_requests_dir.glob("req_*.json")) == []


def test_p3_ack_outage_idempotent_on_crash_after_state_save_before_unlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Crash after _save_state() before req_*.unlink() must NOT re-apply old `--ack-outage ALL` on restart."""
    import subprocess

    data_root = tmp_path / "tokyo_l2"
    cfg_path = _write_test_config(tmp_path, data_root)
    t0 = 1_796_292_720.0
    partials: dict[str, Path] = {}
    for ex in ("binance_futures", "bybit_linear", "bitget_v3", "gate_futures"):
        pp, dp, mp = _seed_exchange_files(data_root, ex, now_s=t0, partial_age_s=2.0)
        partials[ex] = pp
        _seed_ledger_row(data_root, dp, mp, exchange=ex, state="VERIFIED")

    journal_queue: list[str] = []

    def j_reader(_u: str, cur: str | None) -> tuple[list[str], str | None]:
        out = list(journal_queue)
        journal_queue.clear()
        return out, f"cur_{len(out)}_{cur or '0'}"

    transport = FakeTransport()
    mon1 = _build_monitor(cfg_path, transport, journal_reader=j_reader)
    assert mon1.run_sweep(now_epoch_s=t0)["active_alerts"] == []

    # 1. Existing outage A (binance_public) committed to monitor_state.json
    journal_queue.append("ERROR live: [ALERT CRITICAL] binance_public dual A/B outage exceeded 5s")
    for pp in partials.values():
        os.utime(pp, (t0 + 10.0, t0 + 10.0))
    r1 = mon1.run_sweep(now_epoch_s=t0 + 12.0, force_collector_hc=True)
    assert "dual_ab_outage_gt_5s" in r1["active_alerts"]
    assert mon1.state["dual_outage_endpoints"].keys() == {"binance_public"}

    # 2. Operator submits `--ack-outage ALL` intending to clear old outage A;
    #    meanwhile new outage B (gate_public) enters the journal batch.
    ack_proc = subprocess.run(
        [sys.executable, str(PYTHON_DIR / "meta_daemon.py"), "--config", str(cfg_path), "--ack-outage"],
        capture_output=True,
        text=True,
        check=True,
    )
    req_id = json.loads(ack_proc.stdout)["request_id"]
    req_files = list(mon1.ack_requests_dir.glob("req_*.json"))
    assert len(req_files) == 1

    journal_queue.append("ERROR live: [ALERT CRITICAL] gate_public dual A/B outage exceeded 5s")
    for pp in partials.values():
        os.utime(pp, (t0 + 25.0, t0 + 25.0))

    # 3. Inject simulated crash strictly AFTER _save_state() succeeds and BEFORE req_path.unlink() finishes!
    orig_unlink = Path.unlink

    def _crash_on_req_unlink(self_path: Path, *args: Any, **kwargs: Any) -> None:
        if self_path.parent == mon1.ack_requests_dir and self_path.name.startswith("req_"):
            raise RuntimeError("simulated SIGKILL after _save_state before req unlink")
        orig_unlink(self_path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", _crash_on_req_unlink)
    with pytest.raises(RuntimeError, match="simulated SIGKILL"):
        mon1.run_sweep(now_epoch_s=t0 + 30.0, force_collector_hc=True)
    monkeypatch.undo()

    # Verify post-crash disk state: monitor_state.json has ONLY new outage B + persisted processed req_id,
    # while stale req_<id>.json still lingers in outage_ack_requests/.
    persisted = json.loads(mon1.state_path.read_text(encoding="utf-8"))
    assert list(persisted["dual_outage_endpoints"].keys()) == ["gate_public"]
    assert req_id in persisted["processed_outage_ack_ids"]
    assert len(list(mon1.ack_requests_dir.glob("req_*.json"))) == 1

    # 4. Restart a fresh CollectorMonitor from disk and run sweep:
    #    Leftover `--ack-outage ALL` request MUST be ignored idempotently (NOT wipe gate_public!) & deleted!
    transport2 = FakeTransport()
    mon2 = _build_monitor(cfg_path, transport2, journal_reader=j_reader)
    for pp in partials.values():
        os.utime(pp, (t0 + 40.0, t0 + 40.0))
    r_restart = mon2.run_sweep(now_epoch_s=t0 + 45.0, force_collector_hc=True)

    assert "dual_ab_outage_gt_5s" in r_restart["active_alerts"]
    assert mon2.state["dual_outage_endpoints"].keys() == {"gate_public"}
    assert r_restart["outage_acks_applied"] == []
    assert list(mon2.ack_requests_dir.glob("req_*.json")) == []
    assert r_restart["alert_dispatch"]["sent_recoveries"] == []
    assert r_restart["collector_healthcheck"]["healthy"] is False
    assert transport2.hc_calls[-1]["is_fail"] is True


def test_p3_no_late_hourly_backfill_after_restart_and_next_window(tmp_path: Path) -> None:
    """13:50 startup does not send a stale report; the next UTC :10 window sends once."""
    from datetime import datetime, timezone

    root = tmp_path / "data"
    cfg = _write_test_config(tmp_path, root)
    t_late = datetime(2026, 12, 3, 10, 50, tzinfo=timezone.utc).timestamp()
    for ex in ("binance_futures", "bybit_linear", "bitget_v3", "gate_futures"):
        _seed_exchange_files(root, ex, now_s=t_late, data_lines=5000)
    t = FakeTransport()
    mon = _build_monitor(cfg, t)
    first = mon.run_sweep(now_epoch_s=t_late)
    assert first["hourly_report"]["due"] is False
    assert not any("小时采集报告" in m["text"] for m in t.tg_messages)

    # State-backed restart still cannot backfill after the fixed delivery window.
    restarted = _build_monitor(cfg, t)
    next_try = restarted.run_sweep(now_epoch_s=t_late + 30.0)
    assert next_try["hourly_report"]["due"] is False

    # At UTC 11:10 the last complete hour is 10.
    for ex in ("binance_futures", "bybit_linear", "bitget_v3", "gate_futures"):
        _seed_exchange_files(root, ex, now_s=t_late + 1200, hour_utc="10", data_lines=6000)
    in_window = restarted.run_sweep(now_epoch_s=t_late + 1200.0)
    assert in_window["hourly_report"]["sent"] is True
    assert sum("小时采集报告" in m["text"] for m in t.tg_messages) == 1
    again = restarted.run_sweep(now_epoch_s=t_late + 1230.0)
    assert again["hourly_report"]["due"] is False


def test_p3_meta_still_writing_but_zero_data_blocks_healthy_hc(tmp_path: Path) -> None:
    """Recent .partial activity cannot establish health when Bybit's sealed DATA is zero."""
    root = tmp_path / "data"
    cfg = _write_test_config(tmp_path, root)
    t0 = 1_796_292_720.0  # UTC 10:12, previous complete hour 09
    for ex in ("binance_futures", "bybit_linear", "bitget_v3", "gate_futures"):
        _seed_exchange_files(
            root, ex, now_s=t0,
            data_lines=0 if ex == "bybit_linear" else 10000,
        )
    t = FakeTransport()
    mon = _build_monitor(cfg, t)
    result = mon.run_sweep(now_epoch_s=t0)
    assert result["collector_healthcheck"]["healthy"] is False
    assert t.hc_calls[-1]["is_fail"] is True
    assert "collector_data_volume_abnormal" in result["active_alerts"]
    assert result["data_tree"]["per_exchange"]["bybit_linear"]["last_activity_age_s"] < 300
    msg = next(m["text"] for m in t.tg_messages if "有效行情量异常" in m["text"])
    assert "Bybit" in msg and "0 行" in msg
    assert "{" not in msg and "current_sha256" not in msg
    report = result["hourly_report"]["text"]
    assert "🔴 Bybit: 0 行" in report

    # Later valid complete hour removes the operational alarm; deep QA stays pending.
    for ex in ("binance_futures", "bybit_linear", "bitget_v3", "gate_futures"):
        _seed_exchange_files(root, ex, now_s=t0 + 3600, hour_utc="10", data_lines=11000)
    recovered = mon.run_sweep(now_epoch_s=t0 + 3600.0, force_collector_hc=True)
    assert "collector_data_volume_abnormal" not in recovered["active_alerts"]
    assert "collector_data_volume_abnormal" in recovered["alert_dispatch"]["sent_recoveries"]


def test_p3_very_low_data_ratio_vs_prior_hours_blocks_healthy(tmp_path: Path) -> None:
    """223 rows against three earlier healthy 10,000-row hours must fail initial screening."""
    root = tmp_path / "data"
    cfg = _write_test_config(tmp_path, root)
    t0 = 1_796_292_720.0
    for ex in ("binance_futures", "bybit_linear", "bitget_v3", "gate_futures"):
        for hour in ("06", "07", "08"):
            _seed_exchange_files(root, ex, now_s=t0, hour_utc=hour, data_lines=10000)
        _seed_exchange_files(
            root, ex, now_s=t0, hour_utc="09",
            data_lines=223 if ex == "binance_futures" else 10000,
        )
    t = FakeTransport()
    mon = _build_monitor(cfg, t)
    result = mon.run_sweep(now_epoch_s=t0)
    issue = result["data_tree"]["hourly_volume_assessment"]["affected"]
    assert issue["binance_futures"]["data_lines"] == 223
    assert issue["binance_futures"]["reference_data_lines"] == 10000
    assert "bybit_linear" not in issue
    assert result["collector_healthcheck"]["healthy"] is False


def test_p3_raw_spec_body_changes_are_audited_without_unverified_tg_alarm(tmp_path: Path) -> None:
    """Raw REST body change does not prove tick size / multiplier changed."""
    root = tmp_path / "data"
    cfg = _write_test_config(tmp_path, root)
    t0 = 1_796_292_720.0
    for ex in ("binance_futures", "bybit_linear", "bitget_v3", "gate_futures"):
        _seed_exchange_files(root, ex, now_s=t0, data_lines=10000)
    t = FakeTransport()
    mon = _build_monitor(cfg, t)
    version = [1]
    mon.spec_fetcher = lambda _url, _headers: json.dumps({"dynamic": version[0]}).encode()
    mon.run_sweep(now_epoch_s=t0, run_specs=True)
    version[0] = 2
    result = mon.run_sweep(now_epoch_s=t0 + 30, run_specs=True)
    assert "raw_spec_response_changed_unverified" in result["spec_change_alerts"]
    assert not any("合约规格变更" in m["text"] for m in t.tg_messages)
    assert (root / "meta" / "specs" / "spec_changes.jsonl").is_file()


def test_p3_local_loss_expiry_message_preserves_historical_gap() -> None:
    msg = md_mod.format_recovery_message(
        alert_key="local_loss_detected",
        summary="旧告警已清除",
        started_at_s=1000.0,
        now_s=1600.0,
        last_loss_detected_at_s=1300.0,
    )
    assert "[无新增]" in msg
    assert "过去 300 秒未检测到新增本地丢数或写盘异常" in msg
    assert "历史丢数记录继续保留" in msg
    assert "[已恢复]" not in msg


def test_stream_activity_does_not_count_an_active_sibling(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    meta = data_root / "meta"
    meta.mkdir(parents=True)
    now_s = 1_800_000_000
    expected = {
        ("binance_futures", "BTCUSDT", "bookTicker"),
        ("binance_futures", "BTCUSDT", "aggTrade"),
    }
    sample = {
        "sampled_wall_ns": now_s * 10**9,
        "max_sampled_queue_depth": 11,
        "streams": [{
            "exchange": "binance_futures", "symbol": "BTCUSDT", "channel": "bookTicker",
            "last_written_wall_ns": now_s * 10**9, "written_frames": 9,
        }],
    }
    path = meta / "stream_activity.json"
    path.write_text(json.dumps(sample), encoding="utf-8")
    stats = md_mod.inspect_stream_activity(data_root, expected, now_s=now_s, stale_seconds=300)
    assert stats["missing_or_stale"] == ["binance_futures/BTCUSDT/aggTrade"]
    assert not stats["healthy"]
    assert stats["max_sampled_queue_depth"] == 11
    sample["streams"].append({**sample["streams"][0], "channel": "aggTrade"})
    path.write_text(json.dumps(sample), encoding="utf-8")
    assert md_mod.inspect_stream_activity(data_root, expected, now_s=now_s, stale_seconds=300)["healthy"]
    assert not md_mod.inspect_stream_activity(data_root, expected, now_s=now_s + 61, stale_seconds=300)["healthy"]


def test_stream_activity_drives_collector_healthcheck(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    cfg = _write_test_config(tmp_path, data_root)
    config = json.loads(cfg.read_text(encoding="utf-8"))
    config["meta_daemon"]["require_stream_activity"] = True
    for exchange, spec in config["exchanges"].items():
        spec["enabled"] = exchange == "binance_futures"
    config["exchanges"]["binance_futures"]["symbols"] = ["BTCUSDT"]
    cfg.write_text(json.dumps(config), encoding="utf-8")

    now_s = 1_800_000_000
    _seed_exchange_files(data_root, "binance_futures", now_s=now_s)
    monitor = CollectorMonitor(
        cfg,
        service_probe=lambda unit: {"available": True, "active_state": "active", "sub_state": "running"},
        journal_reader=lambda unit, cursor: ([], cursor),
        chrony_runner=lambda: GOOD_CHRONY_OUTPUT,
        disk_checker=lambda path: 10.0,
        allow_notify=False,
    )
    def sweep():
        return monitor.run_sweep(
            now_epoch_s=now_s, run_chrony=True, run_specs=False, run_rtt=False, force_collector_hc=True,
        )

    first = sweep()
    assert "collector_stream_stale" in first["active_alerts"]
    assert first["collector_healthcheck"]["healthy"] is False

    sample = {"sampled_wall_ns": now_s * 10**9, "streams": [
        {"exchange": "binance_futures", "symbol": "BTCUSDT", "channel": channel,
         "written_frames": 1, "last_written_wall_ns": now_s * 10**9}
        for channel in ("bookTicker", "depth@100ms", "aggTrade")
    ]}
    (data_root / "meta" / "stream_activity.json").write_text(json.dumps(sample), encoding="utf-8")
    assert sweep()["collector_healthcheck"]["healthy"] is True


def test_p3_loss_report_keeps_manifest_evidence_after_alert_expires(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    cfg = _write_test_config(tmp_path, data_root)
    t0 = 1_796_292_720.0
    partials = []
    for ex in ("binance_futures", "bybit_linear", "bitget_v3", "gate_futures"):
        partial, _, _ = _seed_exchange_files(
            data_root, ex, now_s=t0, data_lines=10000, local_loss_lines=2 if ex == "gate_futures" else 0,
        )
        partials.append(partial)

    journal: list[str] = []

    def read_journal(_unit: str, cursor: str | None) -> tuple[list[str], str | None]:
        messages = journal[:]
        journal.clear()
        return messages, cursor

    transport = FakeTransport()
    monitor = _build_monitor(cfg, transport, journal_reader=read_journal)
    assert "local_loss_detected" in monitor.run_sweep(now_epoch_s=t0)["active_alerts"]

    journal.append("ERROR [ALERT CRITICAL] LOCAL_LOSS on gate_futures: dropped=1")
    monitor.run_sweep(now_epoch_s=t0 + 120.0)
    for path in partials:
        os.utime(path, (t0 + 419.0, t0 + 419.0))
    before = monitor.run_sweep(now_epoch_s=t0 + 419.0)
    assert "local_loss_detected" in before["active_alerts"]
    assert not any("[无新增]" in message["text"] for message in transport.tg_messages)

    after = monitor.run_sweep(now_epoch_s=t0 + 420.0, force_hourly_report=True)
    assert "local_loss_detected" not in after["active_alerts"]
    assert "local_loss_detected" in after["alert_dispatch"]["sent_recoveries"]
    quiet_message = next(message["text"] for message in transport.tg_messages if "[无新增]" in message["text"])
    assert "过去 300 秒" in quiet_message and "历史丢数记录继续保留" in quiet_message
    assert "[已恢复]" not in quiet_message
    assert len(after["data_tree"]["manifests_with_local_loss"]) == 1
    assert "历史丢数证据（累计）：1 个分区" in after["hourly_report"]["text"]
