"""Focused tests for streaming per-hour QA and persistent skip logic."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import sys
import tempfile
from unittest.mock import patch

import pytest
import zstandard as zstd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python"))
import hourly_incremental_qa as qa  # noqa: E402


def _fixture(root: Path):
    date = "2026-11-21"
    start = int(datetime(2026, 11, 21, 15, tzinfo=timezone.utc).timestamp() * 1e9)
    exchange = "binance_futures"
    name = "binance_futures_20261121_15.tsv.zst"
    rel_manifest = f"raw/{exchange}/{date}/{name.removesuffix('.tsv.zst')}.manifest.json"
    config = {
        "meta_daemon": {"collector_stale_seconds": 3600},
        "exchanges": {
            exchange: {
                "enabled": True, "symbols": ["BTCUSDT"],
                "public_channels": ["bookTicker", "depth@100ms"],
                "market_channels": ["aggTrade"],
            }
        },
    }
    payloads = [
        {"stream": "btcusdt@bookTicker", "data": {"e": "bookTicker", "s": "BTCUSDT"}},
        {"stream": "btcusdt@depth@100ms", "data": {"e": "depthUpdate", "s": "BTCUSDT"}},
        {"stream": "btcusdt@aggTrade", "data": {"e": "aggTrade", "s": "BTCUSDT"}},
    ]
    rows = [
        "\t".join([str(start + i * 1_000_000), str(i + 1), "A", "1", str(i + 1), "DATA",
                   json.dumps(payload, separators=(",", ":"))])
        for i, payload in enumerate(payloads)
    ]
    raw = ("\n".join(rows) + "\n").encode("utf-8")
    data = root / name
    data.write_bytes(zstd.ZstdCompressor().compress(raw))
    manifest = {
        "exchange": exchange, "date_utc": date, "hour_utc": "15",
        "file_name": name, "bytes": data.stat().st_size,
        "sha256": hashlib.sha256(data.read_bytes()).hexdigest(),
        "data_lines": 3, "data_escaped_lines": 0, "meta_lines": 0,
        "local_loss_lines": 0, "gap_start_lines": 0,
        "first_rx_seq": 1, "last_rx_seq": 3, "boot_id": "test",
    }
    manifest_path = root / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    cfg_path = root / "config.json"
    cfg_path.write_text(json.dumps(config), encoding="utf-8")
    return data, manifest_path, cfg_path, rel_manifest, config, manifest


def test_partition_streaming_valid_and_missing_channel():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        data, _, _, _, config, manifest = _fixture(root)
        result = qa.check_partition(data, manifest, config)
        assert result["status"] == "ok"
        assert result["data_records"] == 3
        assert len(result["streams"]) == 3
        config["exchanges"]["binance_futures"]["symbols"].append("ETHUSDT")
        result = qa.check_partition(data, manifest, config)
        assert result["status"] == "blocked"
        assert len(result["missing_streams"]) == 3


def test_partition_sha_mismatch_fails():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        data, _, _, _, config, manifest = _fixture(root)
        manifest["sha256"] = "0" * 64
        with pytest.raises(ValueError, match="SHA-256"):
            qa.check_partition(data, manifest, config)


def test_incremental_skips_completed_partition_even_when_blocked():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        data, manifest_path, cfg_path, rel, _, mf_obj = _fixture(root)
        state = root / "state"
        remote = "qa:"
        rel_data = rel.removesuffix(".manifest.json") + ".tsv.zst"
        index_payload = {
            "schema_version": 2,
            "entries": [
                {
                    "exchange": "binance_futures",
                    "date_utc": "2026-11-21",
                    "hour_utc": "15",
                    "minute_utc": "00",
                    "partition_minutes": 60,
                    "window_id": "2026-11-21T15:00Z",
                    "rel_data_path": rel_data,
                    "rel_manifest_path": rel,
                    "bytes": data.stat().st_size,
                    "sha256": mf_obj["sha256"],
                    "manifest_bytes": manifest_path.stat().st_size,
                    "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
                    "verified": True,
                    "remote_manifest_verified": True,
                }
            ],
        }
        index_path = root / "archive_manifest_index.json"
        index_path.write_text(json.dumps(index_payload), encoding="utf-8")

        def fake_download(_remote, path, dst):
            if path == qa.ARCHIVE_INDEX_RELPATH:
                shutil.copyfile(index_path, dst)
            elif path == rel:
                shutil.copyfile(manifest_path, dst)
            else:
                shutil.copyfile(data, dst)

        with patch.object(qa, "_download", side_effect=fake_download) as download:
            first = qa.run(remote, cfg_path, state, 16, "2026-11-21")
            second = qa.run(remote, cfg_path, state, 16, "2026-11-21")
        assert first["ok"] == 1 and first["attempted"] == 1
        assert second["already_checked"] == 1 and second["attempted"] == 0
        # 1 index download + 2 file downloads on run 1, plus 1 index download on run 2 = 4 calls
        assert download.call_count == 4


def test_loss_marker_blocks_qualification():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        data, _, _, _, config, manifest = _fixture(root)
        raw = zstd.ZstdDecompressor().decompress(data.read_bytes()).decode()
        start = int(datetime(2026, 11, 21, 15, tzinfo=timezone.utc).timestamp() * 1e9)
        loss = "\t".join([str(start + 9_000_000), "9", "A", "1", "4", "META",
                           json.dumps({"event": "LOCAL_LOSS", "dropped_frames": 7})])
        data.write_bytes(zstd.ZstdCompressor().compress((raw + loss + "\n").encode()))
        manifest.update(bytes=data.stat().st_size,
                        sha256=hashlib.sha256(data.read_bytes()).hexdigest(),
                        meta_lines=1, last_rx_seq=4, local_loss_lines=1)
        result = qa.check_partition(data, manifest, config)
        assert result["status"] == "blocked"
        assert result["local_loss_events"] == 1
        assert result["dropped_frames"] == 7


def _bitget_partition(root: Path, records):
    date = "2026-11-21"
    start = int(datetime(2026, 11, 21, 15, tzinfo=timezone.utc).timestamp() * 1e9)
    exchange = "bitget_v3"
    name = "bitget_v3_20261121_15.tsv.zst"
    config = {
        "meta_daemon": {"collector_stale_seconds": 3600},
        "exchanges": {
            exchange: {"enabled": True, "symbols": ["BTCUSDT"], "channels": ["books50"]},
        },
    }
    rows = []
    meta_lines = 0
    for i, (conn, epoch, kind, payload) in enumerate(records):
        if kind == "META":
            meta_lines += 1
        rows.append("\t".join([str(start + (i + 1) * 1_000_000), str(i + 1), conn, str(epoch), str(i + 1), kind,
                               json.dumps(payload, separators=(",", ":"))]))
    data = root / name
    data.write_bytes(zstd.ZstdCompressor().compress(("\n".join(rows) + "\n").encode("utf-8")))
    manifest = {
        "exchange": exchange, "date_utc": date, "hour_utc": "15",
        "file_name": name, "bytes": data.stat().st_size,
        "sha256": hashlib.sha256(data.read_bytes()).hexdigest(),
        "data_lines": len(records) - meta_lines, "data_escaped_lines": 0, "meta_lines": meta_lines,
        "local_loss_lines": 0, "gap_start_lines": 0,
        "first_rx_seq": 1, "last_rx_seq": len(records), "boot_id": "test",
    }
    return data, manifest, config, start


def _bitget_books50_snapshot(seq: int):
    return {
        "action": "snapshot",
        "arg": {"instType": "USDT-FUTURES", "topic": "books50", "symbol": "BTCUSDT"},
        "data": [{"a": [["100.1", "2"]], "b": [["100.0", "3"]], "seq": seq, "ts": "1"}],
    }


def _bitget_planned_wait(start: int):
    return {
        "event": "DEPTH_UNAVAILABLE", "symbol": "BTCUSDT", "planned": True,
        "reason": "planned_rotation", "conn_id": "B", "epoch": 2,
        "unavailable_since_wall_ns": start,
    }


def test_bitget_books50_full_snapshot_chain_closes_planned_wait():
    with tempfile.TemporaryDirectory() as tmp:
        start = int(datetime(2026, 11, 21, 15, tzinfo=timezone.utc).timestamp() * 1e9)
        data, manifest, config, _ = _bitget_partition(Path(tmp), [
            ("B", 2, "META", _bitget_planned_wait(start)),
            ("B", 2, "DATA", _bitget_books50_snapshot(10)),
            ("B", 2, "DATA", _bitget_books50_snapshot(11)),
        ])
        result = qa.check_partition(data, manifest, config)
        assert result["planned_depth_refresh_events"] == 1
        assert result["depth_unavailable_events"] == 0
        intervals = result["planned_depth_unavailable_intervals"]
        assert len(intervals) == 1 and intervals[0]["recovered"] is True
        assert not any("planned_rotation" in p or "depth unavailable" in p for p in result["problems"])


def test_bitget_books50_single_snapshot_keeps_planned_wait_open():
    with tempfile.TemporaryDirectory() as tmp:
        start = int(datetime(2026, 11, 21, 15, tzinfo=timezone.utc).timestamp() * 1e9)
        data, manifest, config, _ = _bitget_partition(Path(tmp), [
            ("B", 2, "META", _bitget_planned_wait(start)),
            ("B", 2, "DATA", _bitget_books50_snapshot(10)),
        ])
        result = qa.check_partition(data, manifest, config)
        assert result["status"] == "blocked"
        assert result["depth_unavailable_events"] == 1
        assert result["planned_depth_unavailable_intervals"][0]["recovered"] is False
        assert any("missing continuous delta after snapshot" in p for p in result["problems"])


def test_bitget_books50_snapshot_on_other_connection_does_not_close_wait():
    with tempfile.TemporaryDirectory() as tmp:
        start = int(datetime(2026, 11, 21, 15, tzinfo=timezone.utc).timestamp() * 1e9)
        data, manifest, config, _ = _bitget_partition(Path(tmp), [
            ("B", 2, "META", _bitget_planned_wait(start)),
            ("B", 2, "DATA", _bitget_books50_snapshot(10)),
            ("A", 1, "DATA", _bitget_books50_snapshot(11)),
        ])
        result = qa.check_partition(data, manifest, config)
        assert result["status"] == "blocked"
        assert result["depth_unavailable_events"] == 1
        assert result["planned_depth_unavailable_intervals"][0]["recovered"] is False


def test_bitget_books50_non_increasing_snapshot_seq_does_not_close_wait():
    with tempfile.TemporaryDirectory() as tmp:
        start = int(datetime(2026, 11, 21, 15, tzinfo=timezone.utc).timestamp() * 1e9)
        data, manifest, config, _ = _bitget_partition(Path(tmp), [
            ("B", 2, "META", _bitget_planned_wait(start)),
            ("B", 2, "DATA", _bitget_books50_snapshot(10)),
            ("B", 2, "DATA", _bitget_books50_snapshot(10)),
        ])
        result = qa.check_partition(data, manifest, config)
        assert result["status"] == "blocked"
        assert result["depth_unavailable_events"] == 1
