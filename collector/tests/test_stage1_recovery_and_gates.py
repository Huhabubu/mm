"""Stage 1 offline acceptance tests: 4-state per-channel monitoring, shared taxonomy,
60-minute archive gate & systemd ExecStart audit, historical loss preservation,
and Binance bridged-delta orderbook reconstruction.
"""
from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import zstandard as zstd

PYTHON_DIR = Path(__file__).resolve().parents[1] / "python"
if str(PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(PYTHON_DIR))

import archive_sync as arch_mod  # noqa: E402
import data_coverage as cov_mod  # noqa: E402
import hourly_incremental_qa as hqa_mod  # noqa: E402
import meta_daemon as md_mod  # noqa: E402
import qa_convert as qa_mod  # noqa: E402


class _MemoryBackend:
    def __init__(self) -> None:
        self.store: dict[str, bytes] = {}
        self.fail_copy_for: set[str] = set()

    def stat_remote(self, remote_rel_path: str) -> arch_mod.RemoteObjectInfo:
        blob = self.store.get(remote_rel_path)
        if blob is None:
            return arch_mod.RemoteObjectInfo(exists=False)
        return arch_mod.RemoteObjectInfo(
            exists=True,
            remote_id=f"mem:{hashlib.md5(remote_rel_path.encode()).hexdigest()[:12]}",
            size=len(blob),
            md5=hashlib.md5(blob).hexdigest(),
            sha256=hashlib.sha256(blob).hexdigest(),
        )

    def copy_file(self, local_path: Path, remote_rel_path: str, *, immutable: bool = True) -> None:
        if remote_rel_path in self.fail_copy_for:
            raise RuntimeError(f"injected upload failure for {remote_rel_path}")
        self.store[remote_rel_path] = local_path.read_bytes()

    def readback_sha256(self, remote_rel_path: str) -> str:
        return hashlib.sha256(self.store[remote_rel_path]).hexdigest()

    def download_file(self, remote_rel_path: str, local_dest: Path) -> None:
        local_dest.parent.mkdir(parents=True, exist_ok=True)
        local_dest.write_bytes(self.store[remote_rel_path])


def _write_sealed_partition(
    sealed_root: Path,
    *,
    exchange: str,
    dt_hour: datetime,
    rows: list[str],
    boot_id: str = "boot_stage1",
    local_loss_lines: int = 0,
) -> tuple[Path, Path, dict]:
    date_utc = dt_hour.strftime("%Y-%m-%d")
    ymd = dt_hour.strftime("%Y%m%d")
    hour_utc = dt_hour.strftime("%H")
    part_dir = sealed_root / "raw" / exchange / date_utc
    part_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{exchange}_{ymd}_{hour_utc}"
    data_path = part_dir / f"{stem}.tsv.zst"
    manifest_path = part_dir / f"{stem}.manifest.json"

    raw_bytes = ("\n".join(rows) + "\n").encode("utf-8")
    compressed = zstd.ZstdCompressor(level=3).compress(raw_bytes)
    data_path.write_bytes(compressed)

    data_lines = sum(1 for r in rows if r.split("\t", 6)[5] == "DATA")
    data_escaped = sum(1 for r in rows if r.split("\t", 6)[5] == "DATA_ESCAPED")
    meta_lines = sum(1 for r in rows if r.split("\t", 6)[5] == "META")
    seqs = [int(r.split("\t", 6)[4]) for r in rows]
    walls = [int(r.split("\t", 6)[0]) for r in rows]

    manifest = {
        "schema_version": 1,
        "exchange": exchange,
        "date_utc": date_utc,
        "hour_utc": hour_utc,
        "file_name": data_path.name,
        "bytes": len(compressed),
        "sha256": hashlib.sha256(compressed).hexdigest(),
        "zstd_verified": True,
        "start_wall_ns": min(walls),
        "end_wall_ns": max(walls),
        "first_rx_seq": min(seqs),
        "last_rx_seq": max(seqs),
        "data_lines": data_lines,
        "data_escaped_lines": data_escaped,
        "meta_lines": meta_lines,
        "local_loss_lines": local_loss_lines,
        "gap_start_lines": 0,
        "switch_lines": 0,
        "boot_id": boot_id,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return data_path, manifest_path, manifest


def test_stage1_four_state_monitoring_and_shared_taxonomy(tmp_path: Path) -> None:
    """Verify 4-state per-channel monitoring, shared category taxonomy, event-trade evidence rules,
    sibling non-inference, and collector Healthchecks fail-closed blocking.
    """
    data_root = tmp_path / "data"
    meta_dir = data_root / "meta"
    meta_dir.mkdir(parents=True)
    now_s = 1_800_000_000.0
    now_ns = int(now_s * 1e9)

    cfg = {
        "deployment": {"data_root": str(data_root)},
        "archive": {"disk_alert_pct": 70.0},
        "meta_daemon": {
            "require_stream_activity": True,
            "collector_stale_seconds": 300,
            "continuous_quote_stale_seconds": 120,
            "depth_stale_seconds": 180,
            "event_trade_stale_seconds": 300,
            "event_trade_quiet_max_seconds": 1800,
        },
        "exchanges": {
            "binance_futures": {
                "enabled": True,
                "symbols": ["BTCUSDT"],
                "public_channels": ["bookTicker", "depth@100ms"],
                "market_channels": ["aggTrade"],
            },
            "bybit_linear": {
                "enabled": True,
                "symbols": ["SUIUSDT"],
                "channels": ["orderbook.50", "orderbook.1", "publicTrade"],
            },
        },
    }
    cfg_path = tmp_path / "config.json"
    cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

    # Seed fresh .partial files matching synthetic now_s so exchange-level freshness stays green
    import os

    for ex in ("binance_futures", "bybit_linear"):
        pdir = data_root / "raw" / ex / "2027-01-15"
        pdir.mkdir(parents=True, exist_ok=True)
        part = pdir / f"{ex}_20270115_08.tsv.zst.partial"
        part.write_bytes(b"active")
        os.utime(part, (now_s, now_s))

    # Scenario 1:
    # - binance bookTicker: all 4 states true -> ok
    # - binance depth@100ms: ws_connected=True, subscription_evidenced=True, received_valid=True
    #   (buffered in WaitAnchor), written=False -> confirmed_outage!
    # - binance aggTrade: ws_connected=True, subscription_evidenced=False (combined_url_requested),
    #   written=False -> needs_review (sibling bookTicker active MUST NOT prove it healthy!)
    # - bybit orderbook.50 & orderbook.1: fresh -> ok
    # - bybit publicTrade: quiet for 600s (>300s, <=1800s) WITH explicit subscribe ACK + ws_connected + protocol_healthy
    #   -> quiet_evidenced (NOT outage!)
    sa_payload = {
        "schema_version": 2,
        "boot_id": "boot_test",
        "sampled_wall_ns": now_ns,
        "max_sampled_queue_depth": 4,
        "endpoints": {
            "binance_public": {"ws_connected": True, "protocol_healthy": True},
            "binance_market": {"ws_connected": True, "protocol_healthy": True},
            "bybit_public": {"ws_connected": True, "protocol_healthy": True},
        },
        "streams": [
            {
                "exchange": "binance_futures", "symbol": "BTCUSDT", "channel": "bookTicker",
                "ws_connected": True, "protocol_healthy": True,
                "subscription_evidenced": True, "subscription_evidence": "frame_observed",
                "received_valid_market_data": True, "received_frames": 100, "last_received_wall_ns": now_ns,
                "written": True, "written_frames": 100, "last_written_wall_ns": now_ns,
            },
            {
                "exchange": "binance_futures", "symbol": "BTCUSDT", "channel": "depth@100ms",
                "ws_connected": True, "protocol_healthy": True,
                "subscription_evidenced": True, "subscription_evidence": "frame_observed",
                "received_valid_market_data": True, "received_frames": 50, "last_received_wall_ns": now_ns,
                "buffered_wait_anchor_frames": 50,
                "written": False, "written_frames": 0, "last_written_wall_ns": 0,
                "depth_state": "WaitAnchor",
            },
            {
                "exchange": "binance_futures", "symbol": "BTCUSDT", "channel": "aggTrade",
                "ws_connected": True, "protocol_healthy": True,
                "subscription_evidenced": False, "subscription_evidence": "combined_url_requested",
                "received_valid_market_data": False, "received_frames": 0, "last_received_wall_ns": None,
                "written": False, "written_frames": 0, "last_written_wall_ns": 0,
            },
            {
                "exchange": "bybit_linear", "symbol": "SUIUSDT", "channel": "orderbook.50",
                "ws_connected": True, "protocol_healthy": True,
                "subscription_evidenced": True, "subscription_evidence": "explicit_ack",
                "received_valid_market_data": True, "received_frames": 80, "last_received_wall_ns": now_ns,
                "written": True, "written_frames": 80, "last_written_wall_ns": now_ns,
            },
            {
                "exchange": "bybit_linear", "symbol": "SUIUSDT", "channel": "orderbook.1",
                "ws_connected": True, "protocol_healthy": True,
                "subscription_evidenced": True, "subscription_evidence": "explicit_ack",
                "received_valid_market_data": True, "received_frames": 80, "last_received_wall_ns": now_ns,
                "written": True, "written_frames": 80, "last_written_wall_ns": now_ns,
            },
            {
                "exchange": "bybit_linear", "symbol": "SUIUSDT", "channel": "publicTrade",
                "ws_connected": True, "protocol_healthy": True,
                "subscription_evidenced": True, "subscription_evidence": "explicit_ack",
                "received_valid_market_data": True, "received_frames": 3,
                "last_received_wall_ns": now_ns - 600 * 10**9,
                "written": True, "written_frames": 3,
                "last_written_wall_ns": now_ns - 600 * 10**9,
            },
        ],
    }
    (meta_dir / "stream_activity.json").write_text(json.dumps(sa_payload), encoding="utf-8")

    expected = cov_mod.expected_streams(cfg)
    stats = md_mod.inspect_stream_activity(data_root, expected, now_s=now_s, stale_seconds=300, config=cfg)
    assert stats["healthy"] is False
    assert stats["confirmed_outage"] == ["binance_futures/BTCUSDT/depth@100ms"]
    assert stats["needs_review"] == ["binance_futures/BTCUSDT/aggTrade"]
    assert stats["quiet_evidenced"] == ["bybit_linear/SUIUSDT/publicTrade"]

    by_key = {row["stream_key"]: row for row in stats["stream_states"]}
    depth_row = by_key["binance_futures/BTCUSDT/depth@100ms"]
    assert depth_row["category"] == cov_mod.CHANNEL_CATEGORY_DEPTH
    assert depth_row["ws_connected"] is True
    assert depth_row["subscription_evidenced"] is True
    assert depth_row["received_valid_market_data"] is True
    assert depth_row["written"] is False
    assert depth_row["reason"] == "depth_wait_anchor_not_written"

    agg_row = by_key["binance_futures/BTCUSDT/aggTrade"]
    assert agg_row["category"] == cov_mod.CHANNEL_CATEGORY_EVENT_TRADE
    assert agg_row["status"] == "needs_review"

    # CollectorMonitor must fire collector_stream_stale AND refuse healthy collector Healthchecks!
    hc_pings: list[tuple[str, bytes]] = []

    def fake_hc(url: str, body: bytes, _headers: dict, _timeout: float) -> tuple[int, bytes]:
        hc_pings.append((url, body))
        return 200, b"OK"

    good_chrony = (
        "Reference ID    : A9FEA97B (169.254.169.123)\n"
        "System time     : 0.000012 seconds fast of NTP time\n"
        "Last offset     : +0.000008 seconds\n"
        "Root delay      : 0.000200 seconds\n"
        "Root dispersion : 0.000100 seconds\n"
        "Leap status     : Normal\n"
    )
    monitor = md_mod.CollectorMonitor(
        cfg_path,
        hc_collector_url="https://hc-ping.com/11111111-2222-3333-4444-555555555555",
        service_probe=lambda _u: {"available": True, "active_state": "active", "sub_state": "running"},
        journal_reader=lambda _u, c: ([], c),
        chrony_runner=lambda: good_chrony,
        disk_checker=lambda _p: 20.0,
        hc_sender=fake_hc,
        allow_notify=True,
    )
    sweep1 = monitor.run_sweep(
        now_epoch_s=now_s, run_chrony=True, run_specs=False, run_rtt=False, force_collector_hc=True
    )
    assert "collector_stream_stale" in sweep1["active_alerts"]
    assert sweep1["collector_healthcheck"]["healthy"] is False
    assert hc_pings[-1][0].endswith("/fail")

    # Once Binance depth & aggTrade recover (while Bybit publicTrade stays quiet_evidenced with ACK),
    # healthcheck flips to healthy!
    for s in sa_payload["streams"]:
        if s["exchange"] == "binance_futures":
            s["subscription_evidenced"] = True
            s["subscription_evidence"] = "frame_observed"
            s["received_valid_market_data"] = True
            s["received_frames"] = max(10, int(s["received_frames"]))
            s["last_received_wall_ns"] = now_ns
            s["written"] = True
            s["written_frames"] = max(10, int(s["written_frames"]))
            s["last_written_wall_ns"] = now_ns
            s["buffered_wait_anchor_frames"] = 0
    (meta_dir / "stream_activity.json").write_text(json.dumps(sa_payload), encoding="utf-8")
    sweep2 = monitor.run_sweep(
        now_epoch_s=now_s + 10, run_chrony=True, run_specs=False, run_rtt=False, force_collector_hc=True
    )
    assert "collector_stream_stale" not in sweep2["active_alerts"]
    assert sweep2["collector_healthcheck"]["healthy"] is True
    assert not hc_pings[-1][0].endswith("/fail")

    # Verify shared taxonomy in hourly_incremental_qa.py: event_trade uses event_trade_quiet_max,
    # while continuous_quote uses strict continuous_quote_stale_seconds!
    dt_h = datetime(2026, 11, 21, 15, 0, tzinfo=timezone.utc)
    start_ns = int(dt_h.timestamp() * 1e9)
    # Place bookTicker & depth every 100s across the hour, but only 2 aggTrades 1000s apart (<1800s quiet max, >300s)
    rows: list[str] = []
    seq = 0
    for sec in range(0, 3600, 100):
        seq += 1
        rows.append("\t".join([
            str(start_ns + sec * 10**9), str(seq), "A", "1", str(seq), "DATA",
            json.dumps({"stream": "btcusdt@bookTicker", "data": {"e": "bookTicker", "s": "BTCUSDT"}}),
        ]))
        seq += 1
        rows.append("\t".join([
            str(start_ns + sec * 10**9 + 1), str(seq), "A", "1", str(seq), "DATA",
            json.dumps({"stream": "btcusdt@depth@100ms", "data": {"e": "depthUpdate", "s": "BTCUSDT"}}),
        ]))
    for sec in (500, 1500, 2500):
        seq += 1
        rows.append("\t".join([
            str(start_ns + sec * 10**9 + 2), str(seq), "A", "1", str(seq), "DATA",
            json.dumps({"stream": "btcusdt@aggTrade", "data": {"e": "aggTrade", "s": "BTCUSDT"}}),
        ]))
    # Sort rows by wall_ns and reassign monotonic seq & mono
    parsed_rows = [r.split("\t", 6) for r in rows]
    parsed_rows.sort(key=lambda p: int(p[0]))
    ordered_rows = [
        "\t".join([p[0], str(i), p[2], p[3], str(i), p[5], p[6]])
        for i, p in enumerate(parsed_rows, start=1)
    ]
    dpath, _, mf = _write_sealed_partition(
        tmp_path / "qa_tax", exchange="binance_futures", dt_hour=dt_h, rows=ordered_rows
    )
    qa_cfg = {
        "meta_daemon": {
            "collector_stale_seconds": 300,
            "continuous_quote_stale_seconds": 120,
            "depth_stale_seconds": 180,
            "event_trade_stale_seconds": 300,
            "event_trade_quiet_max_seconds": 1800,
        },
        "exchanges": {"binance_futures": cfg["exchanges"]["binance_futures"]},
    }
    qa_res = hqa_mod.check_partition(dpath, mf, qa_cfg)
    assert qa_res["status"] == "ok", f"expected quiet aggTrade <=1800s to pass, got {qa_res['problems']}"
    cats = {s["channel"]: s["category"] for s in qa_res["streams"]}
    assert cats == {
        "bookTicker": cov_mod.CHANNEL_CATEGORY_CONTINUOUS_QUOTE,
        "depth@100ms": cov_mod.CHANNEL_CATEGORY_DEPTH,
        "aggTrade": cov_mod.CHANNEL_CATEGORY_EVENT_TRADE,
    }


def test_stage1_archive_service_audit_and_60m_gate(tmp_path: Path) -> None:
    """Verify systemd ExecStart drop-in audit and 60-minute archive reconciliation gate + HC blocking."""
    repo_unit = (
        Path(__file__).resolve().parents[1] / "deploy" / "systemd" / "tokyo-l2-archive.service"
    ).read_text(encoding="utf-8")
    good_cat = "# /etc/systemd/system/tokyo-l2-archive.service\n" + repo_unit
    audit_ok = arch_mod.audit_effective_archive_service(good_cat)
    assert audit_ok["ok"] is True
    assert audit_ok["has_expected_config"] is True
    assert audit_ok["has_verify_remote"] is True
    assert audit_ok["auto_prune_disabled"] is True
    assert audit_ok["drop_in_replaced_exec_start"] is False

    # Drop-in replaces ExecStart and drops --config -> MUST FAIL and flag override!
    override_missing_cfg = (
        good_cat
        + "\n# /etc/systemd/system/tokyo-l2-archive.service.d/override.conf\n"
        + "[Service]\n"
        + "ExecStart=\n"
        + "ExecStart=/opt/mm/collector/bin/archive --dir /data/tokyo_l2 --remote gdrive_main:tokyo_l2_data --verify-remote\n"
    )
    audit_bad_cfg = arch_mod.audit_effective_archive_service(override_missing_cfg)
    assert audit_bad_cfg["ok"] is False
    assert audit_bad_cfg["drop_in_replaced_exec_start"] is True
    assert "missing_expected_config:/etc/tokyo-l2/config.json" in audit_bad_cfg["issues"]

    # Drop-in enabling --prune-hours must also fail while auto-prune is forbidden
    override_prune = (
        good_cat
        + "\n# /etc/systemd/system/tokyo-l2-archive.service.d/prune.conf\n"
        + "[Service]\n"
        + "ExecStart=\n"
        + "ExecStart=/opt/mm/collector/bin/archive --dir /data/tokyo_l2 --config /etc/tokyo-l2/config.json --remote gdrive_main:tokyo_l2_data --verify-remote --prune-hours 24\n"
    )
    audit_prune = arch_mod.audit_effective_archive_service(override_prune)
    assert audit_prune["ok"] is False
    assert "auto_prune_must_remain_off" in audit_prune["issues"]

    # 60m reconciliation gate: now = 2026-12-03T10:05:00Z -> expects previous UTC hour 2026-12-03T09Z
    now_dt = datetime(2026, 12, 3, 10, 5, tzinfo=timezone.utc)
    prev_dt = datetime(2026, 12, 3, 9, 0, tzinfo=timezone.utc)
    expected_ex = {"binance_futures", "bybit_linear", "bitget_v3", "gate_futures"}
    sealed_root = tmp_path / "sealed"
    start_ns = int(prev_dt.timestamp()) * 1_000_000_000
    offsets = [*range(1, 3598, 120), 3598]
    full_hour_rows = [
        f'{start_ns + offset * 1_000_000_000}\t{seq * 100}\tA\t1\t{seq}\tDATA\t{{"ok":true}}'
        for seq, offset in enumerate(offsets, start=1)
    ]

    # Seed only 3 of 4 exchanges (gate_futures missing)
    for ex in ("binance_futures", "bybit_linear", "bitget_v3"):
        _write_sealed_partition(sealed_root, exchange=ex, dt_hour=prev_dt, rows=full_hour_rows)

    backend = _MemoryBackend()
    hc_calls: list[tuple[str, bytes]] = []

    def hc_sender(url: str, body: bytes, _h: dict, _t: float) -> tuple[int, bytes]:
        hc_calls.append((url, body))
        return 200, b"OK"

    res_missing = arch_mod.sync_sealed_directory(
        sealed_root,
        backend=backend,
        verify_remote=True,
        now_epoch_s=now_dt.timestamp(),
        expected_hourly_exchanges=expected_ex,
    )
    assert res_missing["status"] == "blocked"
    assert res_missing["expected_hour_reconciliation"]["missing_exchanges"] == ["gate_futures"]
    md_mod.notify_archive_healthcheck(
        res_missing,
        base_url="https://hc-ping.com/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        http_sender=hc_sender,
    )
    assert hc_calls[-1][0].endswith("/fail")

    # Add 4th exchange BUT also leave an unsealed .partial in expected hour -> still blocked!
    _write_sealed_partition(sealed_root, exchange="gate_futures", dt_hour=prev_dt, rows=full_hour_rows)
    leftover_partial = sealed_root / "raw/gate_futures/2026-12-03/gate_futures_20261203_09_seg2.tsv.zst.partial"
    leftover_partial.write_bytes(b"unsealed")

    res_partial = arch_mod.sync_sealed_directory(
        sealed_root,
        backend=backend,
        verify_remote=True,
        now_epoch_s=now_dt.timestamp(),
        expected_hourly_exchanges=expected_ex,
    )
    assert res_partial["status"] == "blocked"
    assert any("unsealed_partial_in_expected_hour:" in x for x in res_partial["failed_items"])

    # Remove leftover partial; verify_remote=False with expected_hourly_exchanges MUST fail closed!
    leftover_partial.unlink()
    res_no_verify = arch_mod.sync_sealed_directory(
        sealed_root,
        backend=backend,
        verify_remote=False,
        now_epoch_s=now_dt.timestamp(),
        expected_hourly_exchanges=expected_ex,
    )
    assert res_no_verify["status"] == "blocked"
    assert "verify_remote_required_when_config_enforced" in res_no_verify["failed_items"]

    # Remote verify enabled and all 4 present -> status ok & Healthchecks sends OK (not /fail)
    res_ok = arch_mod.sync_sealed_directory(
        sealed_root,
        backend=backend,
        verify_remote=True,
        now_epoch_s=now_dt.timestamp(),
        expected_hourly_exchanges=expected_ex,
    )
    assert res_ok["status"] == "ok"
    assert res_ok["expected_hour_reconciliation"]["missing_exchanges"] == []
    md_mod.notify_archive_healthcheck(
        res_ok,
        base_url="https://hc-ping.com/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        http_sender=hc_sender,
    )
    assert not hc_calls[-1][0].endswith("/fail")


def test_stage1_historical_loss_report_and_strict_qa_full_damaged_list(tmp_path: Path) -> None:
    """Verify hourly report lists all historical damaged partitions even when previous hour has no new loss,
    and strict_zero_loss QA failure returns the complete list of all 4 damaged partitions.
    """
    sealed_root = tmp_path / "sealed"
    exchanges = ("binance_futures", "bybit_linear", "bitget_v3", "gate_futures")
    old_dt = datetime(2026, 12, 3, 7, 0, tzinfo=timezone.utc)
    prev_dt = datetime(2026, 12, 3, 9, 0, tzinfo=timezone.utc)
    now_dt = datetime(2026, 12, 3, 10, 10, tzinfo=timezone.utc)

    # Seed 4 historical damaged partitions (hour 07) + 4 clean previous-hour partitions (hour 09)
    for idx, ex in enumerate(exchanges, start=1):
        old_ns = int(old_dt.timestamp() * 1e9) + 1_000_000
        loss_meta = json.dumps({"event": "LOCAL_LOSS", "dropped_frames": idx * 3, "reason": "queue_full"})
        _write_sealed_partition(
            sealed_root,
            exchange=ex,
            dt_hour=old_dt,
            rows=[
                f"{old_ns}\t100\tA\t1\t1\tMETA\t{loss_meta}",
            ],
            boot_id="boot_old",
            local_loss_lines=1,
        )
        prev_ns = int(prev_dt.timestamp() * 1e9) + 1_000_000
        _write_sealed_partition(
            sealed_root,
            exchange=ex,
            dt_hour=prev_dt,
            rows=[f"{prev_ns}\t200\tA\t1\t2\tDATA\t{{\"ok\":true}}"],
            boot_id="boot_new",
            local_loss_lines=0,
        )

    # 1. Inspect data tree & hourly report: previous hour has 0 loss, historical has 4 damaged manifests
    tree = md_mod.inspect_data_tree(
        sealed_root, list(exchanges), now_s=now_dt.timestamp(), stale_seconds=300
    )
    tree["hourly_volume_assessment"] = md_mod.assess_hourly_data_volume(
        tree, now_s=now_dt.timestamp(), report_minute=10
    )
    report = md_mod.format_hourly_report(
        now_s=now_dt.timestamp(),
        enabled_exchanges=list(exchanges),
        collector_status={"available": True, "active_state": "active", "sub_state": "running"},
        archive_status={"available": True, "active_state": "inactive"},
        data_tree=tree,
        ledger_info={"status": "VERIFIED", "state_counts": {"VERIFIED": 8, "PRUNED": 0}},
        disk_pct=25.0,
        disk_thresh=70.0,
        chrony_row={"certified_le_1ms": True, "estimated_error_ms": 0.123},
        active_alerts={},
    )
    assert "历史丢数证据（累计）：4 个分区" in report
    assert "历史受损分区清单：" in report
    for ex in exchanges:
        assert f"{ex}_20261203_07.manifest.json · LOSS 1" in report

    # 2. Strict QA failure MUST still output the complete list of all 4 damaged partitions!
    backend = _MemoryBackend()
    arch = arch_mod.sync_sealed_directory(sealed_root, backend=backend, verify_remote=True)
    assert arch["status"] == "ok"
    qa_res = qa_mod.run_qa_convert(
        Path(arch["ledger_db_path"]),
        tmp_path / "qa_out",
        strict_zero_loss=True,
    )
    assert qa_res["status"] == "blocked"
    assert "local_loss_detected_under_strict_zero_loss_gate" in qa_res["failed_items"]
    damaged_files = sorted(item["manifest_file"] for item in qa_res["local_loss_partitions"])
    assert damaged_files == sorted(f"{ex}_20261203_07.manifest.json" for ex in exchanges)


def test_stage1_python_binance_bridged_delta_book_reconstruction_and_cross_conn_rejection(
    tmp_path: Path,
) -> None:
    """Verify qa_convert & reconstruct_binance_depth_book replay REST_SNAPSHOT_ANCHOR + ordered
    DEPTH_BRIDGED_DELTA + live depthUpdate level-by-level, and reject cross-connection splicing.
    """
    dt_h = datetime(2026, 12, 3, 11, 0, tzinfo=timezone.utc)
    t0 = int(dt_h.timestamp() * 1e9)

    snap_payload = {
        "event": "REST_SNAPSHOT_ANCHOR",
        "endpoint": "binance_public",
        "conn_id": "A",
        "epoch": 1,
        "symbol": "BTCUSDT",
        "lastUpdateId": 200,
        "snapshot_ready_wall_ns": t0 + 500_000_000,
        "snapshot_ready_mono_ns": 500_000_000,
        "bridged_buffered_deltas": 2,
        "snapshot": {
            "lastUpdateId": 200,
            "E": (t0 // 1_000_000) + 450,
            "T": (t0 // 1_000_000) + 449,
            "bids": [["95000.0", "2.0"], ["94999.0", "5.0"]],
            "asks": [["95001.0", "3.0"], ["95002.0", "4.0"]],
        },
    }
    raw_d1 = json.dumps({
        "stream": "btcusdt@depth@100ms",
        "data": {
            "e": "depthUpdate", "E": (t0 // 1_000_000) + 200, "T": (t0 // 1_000_000) + 199,
            "s": "BTCUSDT", "U": 199, "u": 202, "pu": 198,
            "b": [["95000.0", "4.5"]], "a": [["95001.0", "0.0"]],
        },
    })
    raw_d2 = json.dumps({
        "stream": "btcusdt@depth@100ms",
        "data": {
            "e": "depthUpdate", "E": (t0 // 1_000_000) + 300, "T": (t0 // 1_000_000) + 299,
            "s": "BTCUSDT", "U": 203, "u": 205, "pu": 202,
            "b": [["94999.5", "6.0"]], "a": [["95002.0", "1.25"]],
        },
    })
    bridged1 = {
        "event": "DEPTH_BRIDGED_DELTA",
        "endpoint": "binance_public", "conn_id": "A", "epoch": 1, "symbol": "BTCUSDT",
        "snapshot_last_update_id": 200, "bridge_index": 0,
        "orig_recv_wall_ns": t0 + 200_000_000, "orig_recv_mono_ns": 200_000_000,
        "ready_wall_ns": t0 + 500_000_000, "ready_mono_ns": 500_000_000,
        "first_u": 199, "last_u": 202, "prev_u": 198, "raw": raw_d1,
    }
    bridged2 = {
        "event": "DEPTH_BRIDGED_DELTA",
        "endpoint": "binance_public", "conn_id": "A", "epoch": 1, "symbol": "BTCUSDT",
        "snapshot_last_update_id": 200, "bridge_index": 1,
        "orig_recv_wall_ns": t0 + 300_000_000, "orig_recv_mono_ns": 300_000_000,
        "ready_wall_ns": t0 + 500_000_000, "ready_mono_ns": 500_000_000,
        "first_u": 203, "last_u": 205, "prev_u": 202, "raw": raw_d2,
    }
    live_d3 = {
        "stream": "btcusdt@depth@100ms",
        "data": {
            "e": "depthUpdate", "E": (t0 // 1_000_000) + 600, "T": (t0 // 1_000_000) + 599,
            "s": "BTCUSDT", "U": 206, "u": 208, "pu": 205,
            "b": [["94999.0", "0.0"]], "a": [["95003.0", "8.0"]],
        },
    }
    bbo = {
        "stream": "btcusdt@bookTicker",
        "data": {
            "e": "bookTicker", "u": 300, "s": "BTCUSDT",
            "b": "95000.0", "B": "4.5", "a": "95002.0", "A": "1.25",
            "T": (t0 // 1_000_000) + 601, "E": (t0 // 1_000_000) + 601,
        },
    }
    rows = [
        f"{t0 + 500_000_000}\t500000000\tA\t1\t1\tMETA\t{json.dumps(snap_payload)}",
        f"{t0 + 500_000_000}\t500000000\tA\t1\t2\tMETA\t{json.dumps(bridged1)}",
        f"{t0 + 500_000_000}\t500000000\tA\t1\t3\tMETA\t{json.dumps(bridged2)}",
        f"{t0 + 600_000_000}\t600000000\tA\t1\t4\tDATA\t{json.dumps(live_d3)}",
        f"{t0 + 601_000_000}\t601000000\tA\t1\t5\tDATA\t{json.dumps(bbo)}",
    ]

    sealed_root = tmp_path / "sealed"
    dpath, mpath, _ = _write_sealed_partition(
        sealed_root, exchange="binance_futures", dt_hour=dt_h, rows=rows
    )
    records = qa_mod.parse_tsv_zst_records(dpath)
    rebuilt = qa_mod.reconstruct_binance_depth_book(records, "BTCUSDT")
    assert rebuilt["snapshot_count"] == 1
    assert rebuilt["bridged_delta_count"] == 2
    assert rebuilt["live_delta_count"] == 1
    assert rebuilt["bids"] == {95000.0: 4.5, 94999.5: 6.0}
    assert rebuilt["asks"] == {95002.0: 1.25, 95003.0: 8.0}
    assert all(d["orig_recv_wall_ns"] < d["ready_wall_ns"] for d in rebuilt["bridged_deltas"])

    # Full qa_convert pipeline validates HBT event order and produces binance_depth_diag + binance_bbo
    qa_out = tmp_path / "converted"
    res = qa_mod.run_qa_convert(mpath, qa_out, allow_unverified_local=True, strict_zero_loss=True)
    assert res["status"] == "ok", res
    kinds = {a["kind"] for a in res["artifacts"]}
    assert "binance_bbo" in kinds and "binance_depth_diag" in kinds

    # Cross-connection delta splice (conn B delta applied onto conn A anchor) MUST raise ValueError!
    spliced_records = list(records)
    bad_rec = dict(records[3])
    bad_rec["conn_id"] = "B"
    spliced_records[3] = bad_rec
    with pytest.raises(ValueError, match="Cross-connection"):
        qa_mod.reconstruct_binance_depth_book(spliced_records, "BTCUSDT")
