"""Comprehensive offline P1 verification suite for AWS Tokyo L2 Collector (SPEC_v1.4).

Covers all Section 6.1 acceptance gates strictly offline (zero live exchange WS connections):
1. Minimal closed loop (Binance + Gate BTC): Rust replay-fixture -> archive_sync -> qa_convert
   -> official hftbacktest==2.4.4 + Huhabubu/mm scripts/core_glft_engine.py::run_core_glft,
   asserting 100% bit-identical reproducibility across independent runs.
2. Full 4-exchange protocol edge cases & strict channel separation (Binance, Gate, Bybit, Bitget).
3. Full-pipeline anti-lookahead test (truncation + future mutation -> 100% bit-identical decisions/fills <= t).
4. 50-level visible depth boundary -> QUEUE_UNKNOWN labeling (never queue=0, symmetric retention).
5. Uncovered gaps, LOCAL_LOSS, chrony > 1ms, and spec-change isolation -> execution_unverified
   calendar retention + fail-closed status=blocked under strict gates.
6. Archive fault injection: upload interruption + remote same-name different-hash conflict ->
   status=blocked, zero overwrite, local files preserved.
7. meta_daemon.py chrony parser, spec diff isolation, and disk check.
8. Unified CLI fail-closed guards (collect blocked without authorization; preflight offline passes).
"""
from __future__ import annotations

import csv
import hashlib
import json
import math
import subprocess
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pytest
from hftbacktest import TRADE_EVENT
from hftbacktest.data.validation import validate_event_order

REPO_ROOT = Path(__file__).resolve().parents[2]
COLLECTOR_DIR = REPO_ROOT / "collector"
PYTHON_DIR = COLLECTOR_DIR / "python"
BIN_DIR = COLLECTOR_DIR / "bin"
SPECS_DIR = COLLECTOR_DIR / "specs"

if str(PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(PYTHON_DIR))

from archive_sync import RcloneBackend, RemoteObjectInfo, sync_sealed_directory  # noqa: E402
from meta_daemon import (  # noqa: E402
    check_disk_usage_pct,
    fetch_specs_once,
    parse_chronyc_tracking,
    sample_chrony_once,
    sample_rtt_once,
)
from qa_convert import (  # noqa: E402
    TRADE_FLAG_BYBIT_BT,
    TRADE_FLAG_IS_INTERNAL,
    TRADE_FLAG_RPI,
    evaluate_50_level_coverage,
    run_offline_e2e_hbt_replay,
    run_qa_convert,
)


def _rust_binary() -> Path:
    for rel in ("target/release/tokyo-l2-collector", "target/debug/tokyo-l2-collector"):
        cand = COLLECTOR_DIR / rel
        if cand.is_file():
            return cand
    subprocess.run(
        ["cargo", "build", "--manifest-path", str(COLLECTOR_DIR / "Cargo.toml")],
        check=True,
        capture_output=True,
        text=True,
    )
    return COLLECTOR_DIR / "target/debug/tokyo-l2-collector"


class MockRemoteBackend:
    """Controllable in-memory remote storage backend for offline archive & fault tests."""

    def __init__(self, fail_on_copy: bool = False, fail_on_stat: bool = False) -> None:
        self.store: dict[str, bytes] = {}
        self.fail_on_copy = fail_on_copy
        self.fail_on_stat = fail_on_stat
        self.copy_calls = 0

    def stat_remote(self, remote_rel_path: str) -> RemoteObjectInfo:
        if self.fail_on_stat:
            raise RuntimeError(f"rclone stat_remote failed (rc=5) for {remote_rel_path}: 429 rateLimitExceeded")
        blob = self.store.get(remote_rel_path)
        if blob is None:
            return RemoteObjectInfo(exists=False)
        return RemoteObjectInfo(
            exists=True,
            remote_id=f"mock:{hashlib.md5(remote_rel_path.encode()).hexdigest()[:10]}",
            size=len(blob),
            md5=hashlib.md5(blob).hexdigest(),
            sha256=hashlib.sha256(blob).hexdigest(),
        )

    def copy_file(self, local_path: Path, remote_rel_path: str, *, immutable: bool = True) -> None:
        self.copy_calls += 1
        if self.fail_on_copy:
            raise ConnectionResetError("simulated_upload_interruption")
        self.store[remote_rel_path] = local_path.read_bytes()

    def readback_sha256(self, remote_rel_path: str) -> str:
        return hashlib.sha256(self.store[remote_rel_path]).hexdigest()


def _run_rust_replay(config_path: Path, events: list[dict[str, Any]], out_dir: Path, boot_id: str = "test_boot") -> dict[str, Any]:
    fixture = out_dir.parent / f"{boot_id}_fixture.jsonl"
    fixture.write_text("\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8")
    proc = subprocess.run(
        [
            str(_rust_binary()),
            "replay-fixture",
            "--config",
            str(config_path),
            "--fixture",
            str(fixture),
            "--out",
            str(out_dir),
            "--boot-id",
            boot_id,
        ],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, f"replay-fixture failed: {proc.stdout}\n{proc.stderr}"
    return json.loads(proc.stdout)


def _generate_binance_gate_btc_e2e_events(
    duration_seconds: int = 720,
    mutate_after_ns: int | None = None,
    truncate_after_ns: int | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """Generate a deterministic 12-minute Binance + Gate BTC dual-venue market fixture.

    Includes:
    - Initial 50-level Gate futures.obu full snapshot + strict sequential deltas with decimal sizes.
    - Regular trades on both sides (so HBT physical_1tick_poisson estimator calibrates and fills occur),
      plus 1 duplicate Gate trade id and 1 is_internal=true trade.
    - Separate Gate futures.book_ticker (L1) frames.
    - Binance @bookTicker with periodic leading price impulses (triggering Binance-origin filter),
      plus @depth@100ms + REST snapshot bridge + unsplit @aggTrade.
    """
    base_wall = 1_796_256_000_000_000_000  # 2026-12-03T00:00:00Z
    events: list[dict[str, Any]] = []
    mono = 1_000_000

    def emit(wall_ns: int, op_dict: dict[str, Any]) -> None:
        nonlocal mono
        if truncate_after_ns is not None and wall_ns > truncate_after_ns:
            return
        mono += 100_000
        op_dict["recv_wall_ns"] = wall_ns
        op_dict["recv_mono_ns"] = mono
        events.append(op_dict)

    # Initial 50-level Gate snapshot at t0
    base_mid = 95000.0
    snap_bids = [[f"{base_mid - 0.1 * (i + 1):.1f}", f"{1.2 + 0.1 * (i % 5):.1f}"] for i in range(50)]
    snap_asks = [[f"{base_mid + 0.1 * (i + 1):.1f}", f"{1.1 + 0.1 * (i % 5):.1f}"] for i in range(50)]
    t0_ms = base_wall // 1_000_000
    emit(
        base_wall + 1_000_000,
        {
            "op": "frame",
            "endpoint": "gate_public",
            "conn_id": "A",
            "epoch": 1,
            "payload": json.dumps(
                {
                    "time": t0_ms // 1000,
                    "time_ms": t0_ms,
                    "channel": "futures.obu",
                    "event": "update",
                    "result": {
                        "t": t0_ms,
                        "full": True,
                        "s": "ob.BTC_USDT.50",
                        "u": 1000,
                        "b": snap_bids,
                        "a": snap_asks,
                    },
                }
            ),
        },
    )
    # Initial Binance REST snapshot + bookTicker + aggTrade
    emit(
        base_wall + 1_500_000,
        {
            "op": "frame",
            "endpoint": "binance_public",
            "conn_id": "A",
            "epoch": 1,
            "payload": json.dumps(
                {
                    "stream": "btcusdt@bookTicker",
                    "data": {
                        "e": "bookTicker",
                        "u": 10,
                        "s": "BTCUSDT",
                        "b": f"{base_mid - 0.1:.1f}",
                        "B": "5.0",
                        "a": f"{base_mid + 0.1:.1f}",
                        "A": "5.0",
                        "T": t0_ms,
                        "E": t0_ms,
                    },
                }
            ),
        },
    )
    emit(
        base_wall + 1_800_000,
        {
            "op": "frame",
            "endpoint": "binance_market",
            "conn_id": "A",
            "epoch": 1,
            "payload": json.dumps(
                {
                    "stream": "btcusdt@aggTrade",
                    "data": {
                        "e": "aggTrade",
                        "E": t0_ms,
                        "a": 50001,
                        "s": "BTCUSDT",
                        "p": f"{base_mid + 0.1:.1f}",
                        "q": "0.75",
                        "f": 100,
                        "l": 105,
                        "T": t0_ms,
                        "m": False,
                    },
                }
            ),
        },
    )

    gate_u = 1000
    trade_id = 900_000
    prev_bid = base_mid - 0.1
    prev_ask = base_mid + 0.1

    # Step every 500ms across 720 seconds (1440 steps) -> fast & rich calibration + trading
    steps = duration_seconds * 2
    for step in range(1, steps + 1):
        step_wall = base_wall + step * 500_000_000
        is_mutated = mutate_after_ns is not None and step_wall > mutate_after_ns

        # Deterministic gentle random-walk-like oscillation in ticks
        wave = round(6.0 * math.sin(step / 17.0) + 3.0 * math.cos(step / 7.0)) * 0.1
        if is_mutated:
            wave += 5000.0  # Extreme future shock for anti-lookahead test

        # Binance moves 20ms ahead of Gate with occasional stronger impulse
        impulse = 0.8 if (step % 25 == 0) else (-0.8 if (step % 25 == 12) else 0.0)
        if is_mutated:
            impulse += 250.0
        b_mid = round((base_mid + wave + impulse) * 10.0) / 10.0
        b_wall = step_wall - 20_000_000
        b_ms = b_wall // 1_000_000
        emit(
            b_wall,
            {
                "op": "frame",
                "endpoint": "binance_public",
                "conn_id": "A",
                "epoch": 1,
                "payload": json.dumps(
                    {
                        "stream": "btcusdt@bookTicker",
                        "data": {
                            "e": "bookTicker",
                            "u": 10 + step,
                            "s": "BTCUSDT",
                            "b": f"{b_mid - 0.1:.1f}",
                            "B": "4.0",
                            "a": f"{b_mid + 0.1:.1f}",
                            "A": "4.0",
                            "T": b_ms - 1,
                            "E": b_ms - 1,
                        },
                    }
                ),
            },
        )

        # Gate follows at step_wall: clear old top levels if shifted and set fresh narrow book
        g_mid = round((base_mid + wave) * 10.0) / 10.0
        new_bid = round((g_mid - 0.1) * 10.0) / 10.0
        new_ask = round((g_mid + 0.1) * 10.0) / 10.0
        gate_u += 1
        g_ms = step_wall // 1_000_000

        # Emit full snapshot every 60s (or on mutated shock) to keep 50-level book crisp,
        # and strict sequential delta (U == prev_u + 1) otherwise.
        if step % 120 == 0 or is_mutated:
            bids = [[f"{new_bid - 0.1 * i:.1f}", f"{1.0 + 0.1 * ((step + i) % 4):.1f}"] for i in range(50)]
            asks = [[f"{new_ask + 0.1 * i:.1f}", f"{1.0 + 0.1 * ((step + i) % 4):.1f}"] for i in range(50)]
            emit(
                step_wall,
                {
                    "op": "frame",
                    "endpoint": "gate_public",
                    "conn_id": "A",
                    "epoch": 1,
                    "payload": json.dumps(
                        {
                            "time": g_ms // 1000,
                            "time_ms": g_ms,
                            "channel": "futures.obu",
                            "event": "update",
                            "result": {
                                "t": g_ms - 1,
                                "full": True,
                                "s": "ob.BTC_USDT.50",
                                "u": gate_u,
                                "b": bids,
                                "a": asks,
                            },
                        }
                    ),
                },
            )
        else:
            b_updates = []
            a_updates = []
            # Remove stale overlapping levels if mid crossed
            if new_bid < prev_bid:
                p = prev_bid
                while p > new_bid + 1e-9:
                    b_updates.append([f"{p:.1f}", "0"])
                    p = round((p - 0.1) * 10.0) / 10.0
            if new_ask > prev_ask:
                p = prev_ask
                while p < new_ask - 1e-9:
                    a_updates.append([f"{p:.1f}", "0"])
                    p = round((p + 0.1) * 10.0) / 10.0
            b_updates.append([f"{new_bid:.1f}", f"{0.5 + 0.2 * (step % 5):.1f}"])
            b_updates.append([f"{new_bid - 0.1:.1f}", "1.7"])
            a_updates.append([f"{new_ask:.1f}", f"{0.6 + 0.2 * (step % 5):.1f}"])
            a_updates.append([f"{new_ask + 0.1:.1f}", "1.9"])
            emit(
                step_wall,
                {
                    "op": "frame",
                    "endpoint": "gate_public",
                    "conn_id": "A",
                    "epoch": 1,
                    "payload": json.dumps(
                        {
                            "time": g_ms // 1000,
                            "time_ms": g_ms,
                            "channel": "futures.obu",
                            "event": "update",
                            "result": {
                                "t": g_ms - 1,
                                "full": False,
                                "s": "ob.BTC_USDT.50",
                                "U": gate_u,
                                "u": gate_u,
                                "b": b_updates,
                                "a": a_updates,
                            },
                        }
                    ),
                },
            )
        prev_bid, prev_ask = new_bid, new_ask

        # Emit Gate independent L1 book_ticker
        if step % 4 == 0:
            emit(
                step_wall + 500_000,
                {
                    "op": "frame",
                    "endpoint": "gate_public",
                    "conn_id": "A",
                    "epoch": 1,
                    "payload": json.dumps(
                        {
                            "time": g_ms // 1000,
                            "time_ms": g_ms,
                            "channel": "futures.book_ticker",
                            "event": "update",
                            "result": {
                                "t": g_ms - 1,
                                "u": gate_u,
                                "s": "BTC_USDT",
                                "b": f"{new_bid:.1f}",
                                "B": "1.0",
                                "a": f"{new_ask:.1f}",
                                "A": "1.0",
                            },
                        }
                    ),
                },
            )

        # Emit Gate trades alternating buy/sell sweeping across top ticks so GLFT gets calibrated & filled
        trade_id += 1
        is_buy_taker = (step % 2 == 0)
        tr_px = new_ask + (0.2 if (step % 6 == 0) else 0.0) if is_buy_taker else new_bid - (0.2 if (step % 6 == 3) else 0.0)
        tr_size = "15.5" if is_buy_taker else "-15.5"
        tr_wall = step_wall + 5_000_000
        tr_ms = tr_wall // 1_000_000
        emit(
            tr_wall,
            {
                "op": "frame",
                "endpoint": "gate_public",
                "conn_id": "A",
                "epoch": 1,
                "payload": json.dumps(
                    {
                        "time": tr_ms // 1000,
                        "time_ms": tr_ms,
                        "channel": "futures.trades",
                        "event": "update",
                        "result": [
                            {
                                "id": trade_id,
                                "contract": "BTC_USDT",
                                "size": tr_size,
                                "price": f"{tr_px:.1f}",
                                "create_time_ms": tr_ms - 1,
                                "is_internal": (step == 10),
                            }
                        ],
                    }
                ),
            },
        )
        # Inject one duplicate trade at step == 20 to verify deduplication in E2E pipeline
        if step == 20:
            emit(
                tr_wall + 1_000_000,
                {
                    "op": "frame",
                    "endpoint": "gate_public",
                    "conn_id": "A",
                    "epoch": 1,
                    "payload": json.dumps(
                        {
                            "time": tr_ms // 1000,
                            "time_ms": tr_ms,
                            "channel": "futures.trades",
                            "event": "update",
                            "result": [
                                {
                                    "id": trade_id,
                                    "contract": "BTC_USDT",
                                    "size": tr_size,
                                    "price": f"{tr_px:.1f}",
                                    "create_time_ms": tr_ms - 1,
                                    "is_internal": False,
                                }
                            ],
                        }
                    ),
                },
            )

    return events, base_wall


def test_minimal_closed_loop_binance_gate_btc_e2e_hbt_reproducibility(tmp_path: Path) -> None:
    """P1 Gate: Binance+Gate BTC closed loop through official HBT 2.4.4 + core_glft_engine.py, run twice -> 100% bit-identical."""
    mvp_cfg = SPECS_DIR / "config.btc_gate_binance_mvp.json"
    events, _ = _generate_binance_gate_btc_e2e_events(duration_seconds=720)

    run_results = []
    for run_idx in (1, 2):
        run_dir = tmp_path / f"run_{run_idx}"
        sealed_dir = run_dir / "sealed"
        out_dir = run_dir / "converted"
        sealed_dir.mkdir(parents=True)

        _run_rust_replay(mvp_cfg, events, sealed_dir, boot_id=f"e2e_boot_{run_idx}")
        backend = MockRemoteBackend()
        arch = sync_sealed_directory(sealed_dir, backend=backend, verify_remote=True)
        assert arch["status"] == "ok"
        assert arch["verified_count"] == 2  # binance_futures + gate_futures

        qa = run_qa_convert(Path(arch["ledger_path"]), out_dir, strict_zero_loss=True)
        assert qa["status"] == "ok"
        assert qa["eligible_asset_days"] == 2
        assert qa["execution_unverified_asset_days"] == 0

        # Verify Gate trade dedup + internal flag + unsplit Binance aggTrade + separate L1
        cal = json.loads((out_dir / "eligibility_calendar.json").read_text(encoding="utf-8"))
        gate_row = [r for r in cal if r["exchange"] == "gate_futures"][0]
        assert gate_row["dedup_trades_dropped"] == 1
        assert gate_row["special_flag_counts"]["is_internal"] == 1
        assert "follower_l1_bbo" in gate_row["files"]
        assert "follower_l2_hbt" in gate_row["files"]

        bin_agg = np.load(out_dir / "binance_aggtrade_diag_BTC_20261203.npz")
        assert len(bin_agg["agg_id"]) == 1
        assert int(bin_agg["first_trade_id"][0]) == 100
        assert int(bin_agg["last_trade_id"][0]) == 105  # Unsplit!

        e2e = run_offline_e2e_hbt_replay(
            out_dir / "gate_futures_l2_BTC_20261203.npz",
            out_dir / "binance_bbo_BTC_20261203.npz",
            trade_steps=1100,
            quote_mode=2,
            beta=0.5,
            cross_timestamp_tie_mode="microsecond",
        )
        assert e2e["status"] == "ok"
        assert e2e["metrics"]["decisions"] > 0
        assert e2e["metrics"]["submits"] > 0
        assert e2e["metrics"]["binance_events"] > 0
        run_results.append(e2e)

    # 100% bit-identical reproducibility across independent runs!
    assert run_results[0]["follower_l2_sha256"] == run_results[1]["follower_l2_sha256"]
    assert run_results[0]["binance_bbo_sha256"] == run_results[1]["binance_bbo_sha256"]
    assert run_results[0]["ledger_sha256"] == run_results[1]["ledger_sha256"]
    assert run_results[0]["metrics"] == run_results[1]["metrics"]


def test_full_pipeline_anti_lookahead_truncation_and_future_mutation(tmp_path: Path) -> None:
    """P1 Gate: Truncating OR mutating future frames strictly after t_eval produces 100% bit-identical arrays, decisions, probes, and fills at <= t_eval."""
    mvp_cfg = SPECS_DIR / "config.btc_gate_binance_mvp.json"
    base_wall = 1_796_256_000_000_000_000
    t_eval = base_wall + 659_000_000_000  # 600s warmup + 59s live trading
    probes = np.array(
        [
            base_wall + 610_000_000_000,
            base_wall + 630_000_000_000,
            base_wall + 655_000_000_000,
        ],
        dtype=np.int64,
    )

    # - original: full 720s stream
    # - truncated: drops the remaining ~60.5s of future data right after the boundary tick past t_eval
    #              (keeping 1 boundary frame past t_eval so HBT reaches stop_at_ns=t_eval without EOF at t_eval-20ms)
    # - future_mutated: mutates EVERY frame strictly > t_eval with +$5000 price shocks
    variants = {
        "original": _generate_binance_gate_btc_e2e_events(duration_seconds=720)[0],
        "truncated": _generate_binance_gate_btc_e2e_events(duration_seconds=720, truncate_after_ns=t_eval + 600_000_000)[0],
        "future_mutated": _generate_binance_gate_btc_e2e_events(duration_seconds=720, mutate_after_ns=t_eval)[0],
    }

    replays: dict[str, dict[str, Any]] = {}
    prefix_l2_hashes: dict[str, str] = {}
    prefix_bbo_hashes: dict[str, str] = {}
    for name, evs in variants.items():
        vdir = tmp_path / name
        sealed = vdir / "sealed"
        converted = vdir / "converted"
        sealed.mkdir(parents=True)
        _run_rust_replay(mvp_cfg, evs, sealed, boot_id=f"al_{name}")
        arch = sync_sealed_directory(sealed, backend=MockRemoteBackend(), verify_remote=True)
        assert arch["status"] == "ok"
        qa = run_qa_convert(Path(arch["ledger_path"]), converted, strict_zero_loss=True)
        assert qa["status"] == "ok"

        # 1. Verify converted L2 events & Binance BBO prefix (<= t_eval) are 100% bit-identical!
        l2_arr = np.load(converted / "gate_futures_l2_BTC_20261203.npz")["data"]
        l2_prefix = l2_arr[l2_arr["local_ts"] <= t_eval]
        prefix_l2_hashes[name] = hashlib.sha256(l2_prefix.tobytes()).hexdigest()

        bbo = np.load(converted / "binance_bbo_BTC_20261203.npz")
        mask = bbo["local_ts_ns"] <= t_eval
        prefix_bbo_hashes[name] = hashlib.sha256(
            bbo["local_ts_ns"][mask].tobytes() + bbo["mid_px"][mask].tobytes()
        ).hexdigest()

        # 2. Verify full HBT 2.4.4 + core_glft_engine.py replay up to t_eval is 100% bit-identical!
        replays[name] = run_offline_e2e_hbt_replay(
            converted / "gate_futures_l2_BTC_20261203.npz",
            converted / "binance_bbo_BTC_20261203.npz",
            trade_steps=1100,
            quote_mode=2,
            beta=0.5,
            stop_at_ns=t_eval,
            probe_ns=probes,
            cross_timestamp_tie_mode="microsecond",
        )

    assert prefix_l2_hashes["original"] == prefix_l2_hashes["truncated"]
    assert prefix_l2_hashes["original"] == prefix_l2_hashes["future_mutated"]
    assert prefix_bbo_hashes["original"] == prefix_bbo_hashes["truncated"]
    assert prefix_bbo_hashes["original"] == prefix_bbo_hashes["future_mutated"]

    assert replays["original"]["metrics"]["decisions"] > 0
    assert replays["original"]["ledger_sha256"] == replays["truncated"]["ledger_sha256"]
    assert replays["original"]["ledger_sha256"] == replays["future_mutated"]["ledger_sha256"]
    assert replays["original"]["metrics"] == replays["truncated"]["metrics"]
    assert replays["original"]["metrics"] == replays["future_mutated"]["metrics"]
    np.testing.assert_allclose(
        np.array(replays["original"]["probe_rows"], dtype=np.float64),
        np.array(replays["truncated"]["probe_rows"], dtype=np.float64),
        equal_nan=True,
        rtol=0.0,
        atol=0.0,
    )
    np.testing.assert_allclose(
        np.array(replays["original"]["probe_rows"], dtype=np.float64),
        np.array(replays["future_mutated"]["probe_rows"], dtype=np.float64),
        equal_nan=True,
        rtol=0.0,
        atol=0.0,
    )


def test_four_exchange_protocol_edge_cases_and_channel_separation(tmp_path: Path) -> None:
    """P1 Gate: Verify all 4 exchanges' protocol rules, trade dedup, special flags, and L1/L2 separation."""
    full_cfg = SPECS_DIR / "config.example.json"
    base_wall = 1_796_256_000_000_000_000
    base_ms = base_wall // 1_000_000

    events = [
        # --- Binance: @bookTicker + @depth@100ms + REST snapshot + unsplit @aggTrade ---
        {
            "op": "frame",
            "recv_wall_ns": base_wall + 1_000_000,
            "recv_mono_ns": 1_000_000,
            "endpoint": "binance_public",
            "conn_id": "A",
            "epoch": 1,
            "payload": json.dumps(
                {
                    "stream": "btcusdt@bookTicker",
                    "data": {"e": "bookTicker", "u": 1, "s": "BTCUSDT", "b": "95000.0", "B": "2.0", "a": "95000.1", "A": "2.0", "T": base_ms, "E": base_ms},
                }
            ),
        },
        {
            "op": "frame",
            "recv_wall_ns": base_wall + 2_000_000,
            "recv_mono_ns": 2_000_000,
            "endpoint": "binance_public",
            "conn_id": "A",
            "epoch": 1,
            "payload": json.dumps(
                {
                    "stream": "btcusdt@depth@100ms",
                    "data": {"e": "depthUpdate", "E": base_ms + 1, "T": base_ms + 1, "s": "BTCUSDT", "U": 100, "u": 105, "pu": 99, "b": [["95000.0", "3.0"]], "a": [["95000.1", "2.5"]]},
                }
            ),
        },
        {
            "op": "rest_snapshot",
            "recv_wall_ns": base_wall + 2_500_000,
            "recv_mono_ns": 2_500_000,
            "endpoint": "binance_public",
            "conn_id": "A",
            "symbol": "BTCUSDT",
            "last_update_id": 102,
            "raw_json": json.dumps({"lastUpdateId": 102, "E": base_ms + 1, "bids": [["95000.0", "2.0"]], "asks": [["95000.1", "2.0"]]}),
        },
        {
            "op": "frame",
            "recv_wall_ns": base_wall + 3_000_000,
            "recv_mono_ns": 3_000_000,
            "endpoint": "binance_market",
            "conn_id": "A",
            "epoch": 1,
            "payload": json.dumps(
                {
                    "stream": "btcusdt@aggTrade",
                    "data": {"e": "aggTrade", "E": base_ms + 2, "a": 7001, "s": "BTCUSDT", "p": "95000.1", "q": "1.2", "f": 10, "l": 14, "T": base_ms + 2, "m": False},
                }
            ),
        },
        # --- Gate: decimal strings + L1 distinct price (MUST NOT enter L2!) + trade dedup & is_internal ---
        {
            "op": "frame",
            "recv_wall_ns": base_wall + 4_000_000,
            "recv_mono_ns": 4_000_000,
            "endpoint": "gate_public",
            "conn_id": "A",
            "epoch": 1,
            "payload": json.dumps(
                {
                    "time": base_ms // 1000,
                    "time_ms": base_ms + 3,
                    "channel": "futures.obu",
                    "event": "update",
                    "result": {"t": base_ms + 3, "full": True, "s": "ob.BTC_USDT.50", "u": 10, "b": [["95000.0", "0.1"]], "a": [["95000.1", "1.7"]]},
                }
            ),
        },
        {
            "op": "frame",
            "recv_wall_ns": base_wall + 4_500_000,
            "recv_mono_ns": 4_500_000,
            "endpoint": "gate_public",
            "conn_id": "A",
            "epoch": 1,
            "payload": json.dumps(
                {
                    "time": base_ms // 1000,
                    "time_ms": base_ms + 4,
                    "channel": "futures.book_ticker",
                    "event": "update",
                    # Deliberately distinct sentinel L1 price 88888.8 to prove L1 NEVER overwrites L2!
                    "result": {"t": base_ms + 4, "u": 11, "s": "BTC_USDT", "b": "88888.8", "B": "9.9", "a": "88888.9", "A": "9.9"},
                }
            ),
        },
        {
            "op": "frame",
            "recv_wall_ns": base_wall + 5_000_000,
            "recv_mono_ns": 5_000_000,
            "endpoint": "gate_public",
            "conn_id": "A",
            "epoch": 1,
            "payload": json.dumps(
                {
                    "time": base_ms // 1000,
                    "time_ms": base_ms + 5,
                    "channel": "futures.trades",
                    "event": "update",
                    "result": [
                        {"id": 501, "contract": "BTC_USDT", "size": "0.1", "price": "95000.1", "create_time_ms": base_ms + 5, "is_internal": True},
                        {"id": 501, "contract": "BTC_USDT", "size": "0.1", "price": "95000.1", "create_time_ms": base_ms + 5, "is_internal": True},
                    ],
                }
            ),
        },
        # --- Bybit: non-strict u jump + u==1 restart + same-seq two distinct trades + dedup + BT/RPI + separate L1 ---
        {
            "op": "frame",
            "recv_wall_ns": base_wall + 6_000_000,
            "recv_mono_ns": 6_000_000,
            "endpoint": "bybit_public",
            "conn_id": "A",
            "epoch": 1,
            "payload": json.dumps(
                {"topic": "orderbook.50.BTCUSDT", "type": "snapshot", "ts": base_ms + 6, "cts": base_ms + 6, "data": {"s": "BTCUSDT", "b": [["95000.0", "1.0"]], "a": [["95000.1", "1.0"]], "u": 100, "seq": 10}}
            ),
        },
        {
            "op": "frame",
            "recv_wall_ns": base_wall + 7_000_000,
            "recv_mono_ns": 7_000_000,
            "endpoint": "bybit_public",
            "conn_id": "A",
            "epoch": 1,
            "payload": json.dumps(
                {"topic": "orderbook.50.BTCUSDT", "type": "delta", "ts": base_ms + 7, "cts": base_ms + 7, "data": {"s": "BTCUSDT", "b": [["95000.0", "1.5"]], "a": [], "u": 105, "seq": 15}}
            ),
        },
        {
            "op": "frame",
            "recv_wall_ns": base_wall + 8_000_000,
            "recv_mono_ns": 8_000_000,
            "endpoint": "bybit_public",
            "conn_id": "A",
            "epoch": 1,
            "payload": json.dumps(
                {"topic": "orderbook.50.BTCUSDT", "type": "snapshot", "ts": base_ms + 8, "cts": base_ms + 8, "data": {"s": "BTCUSDT", "b": [["95000.0", "2.0"]], "a": [["95000.1", "2.0"]], "u": 1, "seq": 16}}
            ),
        },
        {
            "op": "frame",
            "recv_wall_ns": base_wall + 8_500_000,
            "recv_mono_ns": 8_500_000,
            "endpoint": "bybit_public",
            "conn_id": "A",
            "epoch": 1,
            "payload": json.dumps(
                {"topic": "orderbook.1.BTCUSDT", "type": "snapshot", "ts": base_ms + 8, "data": {"s": "BTCUSDT", "b": [["77777.7", "5.0"]], "a": [["77777.8", "5.0"]], "u": 1, "seq": 16}}
            ),
        },
        {
            "op": "frame",
            "recv_wall_ns": base_wall + 9_000_000,
            "recv_mono_ns": 9_000_000,
            "endpoint": "bybit_public",
            "conn_id": "A",
            "epoch": 1,
            "payload": json.dumps(
                {
                    "topic": "publicTrade.BTCUSDT",
                    "type": "snapshot",
                    "ts": base_ms + 9,
                    "data": [
                        {"T": base_ms + 9, "s": "BTCUSDT", "S": "Buy", "v": "0.2", "p": "95000.1", "i": "t-same-seq-1", "BT": True, "RPI": False, "seq": 500},
                        {"T": base_ms + 9, "s": "BTCUSDT", "S": "Sell", "v": "0.3", "p": "95000.0", "i": "t-same-seq-2", "BT": False, "RPI": True, "seq": 500},
                        {"T": base_ms + 9, "s": "BTCUSDT", "S": "Buy", "v": "0.2", "p": "95000.1", "i": "t-same-seq-1", "BT": True, "RPI": False, "seq": 500},
                    ],
                }
            ),
        },
        # --- Bitget UTA v3: snapshot (pseq=0) + post-cutover update (pseq=prev_seq) + books1 separate + publicTrade dedup & isRPI ---
        {
            "op": "frame",
            "recv_wall_ns": base_wall + 10_000_000,
            "recv_mono_ns": 10_000_000,
            "endpoint": "bitget_public",
            "conn_id": "A",
            "epoch": 1,
            "payload": json.dumps(
                {
                    "action": "snapshot",
                    "arg": {"instType": "usdt-futures", "topic": "books50", "symbol": "BTCUSDT"},
                    "data": [{"a": [["95000.1", "2.0"]], "b": [["95000.0", "2.0"]], "ts": str(base_ms + 10), "seq": 800, "pseq": 0}],
                    "ts": base_ms + 10,
                }
            ),
        },
        {
            "op": "frame",
            "recv_wall_ns": base_wall + 11_000_000,
            "recv_mono_ns": 11_000_000,
            "endpoint": "bitget_public",
            "conn_id": "A",
            "epoch": 1,
            "payload": json.dumps(
                {
                    "action": "update",
                    "arg": {"instType": "usdt-futures", "topic": "books50", "symbol": "BTCUSDT"},
                    "data": [{"a": [["95000.1", "2.5"]], "b": [], "ts": str(base_ms + 11), "seq": 801, "pseq": 800}],
                    "ts": base_ms + 11,
                }
            ),
        },
        {
            "op": "frame",
            "recv_wall_ns": base_wall + 11_500_000,
            "recv_mono_ns": 11_500_000,
            "endpoint": "bitget_public",
            "conn_id": "A",
            "epoch": 1,
            "payload": json.dumps(
                {
                    "action": "snapshot",
                    "arg": {"instType": "usdt-futures", "topic": "books1", "symbol": "BTCUSDT"},
                    "data": [{"a": [["66666.7", "1.0"]], "b": [["66666.6", "1.0"]], "ts": str(base_ms + 11), "seq": 1, "pseq": 0}],
                    "ts": base_ms + 11,
                }
            ),
        },
        {
            "op": "frame",
            "recv_wall_ns": base_wall + 12_000_000,
            "recv_mono_ns": 12_000_000,
            "endpoint": "bitget_public",
            "conn_id": "A",
            "epoch": 1,
            "payload": json.dumps(
                {
                    "action": "update",
                    "arg": {"instType": "usdt-futures", "topic": "publicTrade", "symbol": "BTCUSDT"},
                    "data": [
                        {"T": str(base_ms + 12), "p": "95000.1", "v": "0.4", "S": "buy", "i": "bg-1", "isRPI": "yes"},
                        {"T": str(base_ms + 12), "p": "95000.1", "v": "0.4", "S": "buy", "i": "bg-1", "isRPI": "yes"},
                    ],
                    "ts": base_ms + 12,
                }
            ),
        },
    ]

    sealed = tmp_path / "sealed"
    converted = tmp_path / "converted"
    sealed.mkdir()
    _run_rust_replay(full_cfg, events, sealed, boot_id="four_ex")
    arch = sync_sealed_directory(sealed, backend=MockRemoteBackend(), verify_remote=True)
    assert arch["status"] == "ok"
    qa = run_qa_convert(Path(arch["ledger_path"]), converted, strict_zero_loss=True)
    assert qa["status"] == "ok"
    assert qa["eligible_asset_days"] == 4

    # Gate checks: decimal qty 0.1 & 1.7 preserved, L1 sentinel 88888.8 NOT in L2, trade dedup & is_internal bit
    gate_l2 = np.load(converted / "gate_futures_l2_BTC_20261203.npz")["data"]
    validate_event_order(gate_l2)
    assert not np.any(np.isclose(gate_l2["px"], 88888.8)), "L1 price leaked into Gate L2 book!"
    gate_trades = gate_l2[(gate_l2["ev"] & 0xFF) == TRADE_EVENT]
    assert len(gate_trades) == 1
    assert np.isclose(gate_trades["qty"][0], 0.1)
    assert int(gate_trades["ival"][0]) & TRADE_FLAG_IS_INTERNAL
    gate_l1 = np.load(converted / "gate_futures_l1_BTC_20261203.npz")
    assert np.isclose(gate_l1["bid_px"][0], 88888.8)

    # Bybit checks: L1 sentinel 77777.7 NOT in L2, same-seq 2 distinct trades kept + 1 dup dropped, BT & RPI preserved
    bybit_l2 = np.load(converted / "bybit_linear_l2_BTC_20261203.npz")["data"]
    validate_event_order(bybit_l2)
    assert not np.any(np.isclose(bybit_l2["px"], 77777.7)), "L1 price leaked into Bybit L2 book!"
    bybit_trades = bybit_l2[(bybit_l2["ev"] & 0xFF) == TRADE_EVENT]
    assert len(bybit_trades) == 2
    ivals = set(int(x) for x in bybit_trades["ival"])
    assert TRADE_FLAG_BYBIT_BT in ivals
    assert TRADE_FLAG_RPI in ivals

    # Bitget checks: L1 sentinel 66666.6 NOT in L2, trade dedup & isRPI preserved
    bitget_l2 = np.load(converted / "bitget_v3_l2_BTC_20261203.npz")["data"]
    validate_event_order(bitget_l2)
    assert not np.any(np.isclose(bitget_l2["px"], 66666.6)), "L1 price leaked into Bitget L2 book!"
    bitget_trades = bitget_l2[(bitget_l2["ev"] & 0xFF) == TRADE_EVENT]
    assert len(bitget_trades) == 1
    assert int(bitget_trades["ival"][0]) & TRADE_FLAG_RPI


def test_queue_unknown_beyond_50_levels_and_symmetric_retention() -> None:
    """P1 Gate: Quotes outside the real 50th non-empty level are tagged QUEUE_UNKNOWN (never queue=0, symmetric)."""
    # Edge sample at t=1000: 50 bid levels [999.9 .. 995.0], 50 ask levels [1000.1 .. 1005.0]
    edges = [(1000, 999.9, 995.0, 1000.1, 1005.0, 50, 50)]
    quotes = [
        {"ts_ns": 1500, "side": "buy", "price": 995.0, "pnl_tag": "win"},      # At 50th bid -> VISIBLE
        {"ts_ns": 1500, "side": "buy", "price": 994.9, "pnl_tag": "loss"},     # Beyond 50th bid -> QUEUE_UNKNOWN
        {"ts_ns": 1500, "side": "sell", "price": 1005.0, "pnl_tag": "loss"},   # At 50th ask -> VISIBLE
        {"ts_ns": 1500, "side": "sell", "price": 1005.1, "pnl_tag": "win"},    # Beyond 50th ask -> QUEUE_UNKNOWN
    ]
    res = evaluate_50_level_coverage(quotes, edges)
    assert res["total"] == 4  # Never drops any fill (winning or losing)!
    assert res["visible_within_50"] == 2
    assert res["queue_unknown"] == 2
    assert res["items"][0]["depth_coverage"] == "VISIBLE_WITHIN_50"
    assert res["items"][1]["depth_coverage"] == "QUEUE_UNKNOWN"
    assert res["items"][2]["depth_coverage"] == "VISIBLE_WITHIN_50"
    assert res["items"][3]["depth_coverage"] == "QUEUE_UNKNOWN"


def test_gap_local_loss_chrony_and_spec_change_mark_execution_unverified(tmp_path: Path) -> None:
    """P1 Gate: Uncovered gap, LOCAL_LOSS, chrony > 1ms, and spec change mark day execution_unverified & retain row."""
    mvp_cfg = SPECS_DIR / "config.btc_gate_binance_mvp.json"
    base_wall = 1_796_256_000_000_000_000
    base_ms = base_wall // 1_000_000

    events = [
        # Gate starts LIVE on A, then both A and B disconnect -> GAP_START, then A recovers -> GAP_END, plus LOCAL_LOSS
        {
            "op": "frame",
            "recv_wall_ns": base_wall + 1_000_000,
            "recv_mono_ns": 1_000_000,
            "endpoint": "gate_public",
            "conn_id": "A",
            "epoch": 1,
            "payload": json.dumps(
                {"time": base_ms // 1000, "time_ms": base_ms, "channel": "futures.obu", "event": "update", "result": {"t": base_ms, "full": True, "s": "ob.BTC_USDT.50", "u": 1, "b": [["95000.0", "1.0"]], "a": [["95000.1", "1.0"]]}}
            ),
        },
        {"op": "disconnect", "recv_wall_ns": base_wall + 2_000_000, "recv_mono_ns": 2_000_000, "endpoint": "gate_public", "conn_id": "A", "reason": "fault_a"},
        {"op": "reconnect", "recv_mono_ns": 3_000_000, "endpoint": "gate_public", "conn_id": "A", "epoch": 2},
        {
            "op": "frame",
            "recv_wall_ns": base_wall + 4_000_000,
            "recv_mono_ns": 4_000_000,
            "endpoint": "gate_public",
            "conn_id": "A",
            "epoch": 2,
            "payload": json.dumps(
                {"time": base_ms // 1000, "time_ms": base_ms + 4, "channel": "futures.obu", "event": "update", "result": {"t": base_ms + 4, "full": True, "s": "ob.BTC_USDT.50", "u": 2, "b": [["95000.0", "1.0"]], "a": [["95000.1", "1.0"]]}}
            ),
        },
        {"op": "local_loss", "recv_wall_ns": base_wall + 5_000_000, "recv_mono_ns": 5_000_000, "endpoint": "gate_public", "dropped": 1, "reason": "test_backpressure"},
    ]

    sealed = tmp_path / "sealed"
    meta_dir = sealed / "meta"
    (meta_dir / "chrony").mkdir(parents=True)
    (meta_dir / "specs").mkdir(parents=True)
    # Write uncertified chrony sample (> 1ms)
    with (meta_dir / "chrony" / "chrony_20261203.csv").open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ts_utc_ns", "ref_id", "stratum", "last_offset_ms", "root_delay_ms", "root_dispersion_ms", "root_bound_ms", "leap_status", "certified_under_1ms"])
        w.writerow([base_wall + 3_000_000, "169.254.169.123", 3, 0.2, 0.4, 1.5, 1.9, "Normal", False])
    # Write spec change isolation event
    (meta_dir / "specs" / "spec_changes.jsonl").write_text(
        json.dumps({"ts_utc": "2026-12-03T00:00:02Z", "exchange": "gate_futures", "isolation_required": True, "changes": [{"symbol": "BTC_USDT", "field": "order_price_round"}]}) + "\n",
        encoding="utf-8",
    )

    _run_rust_replay(mvp_cfg, events, sealed, boot_id="gap_test")
    arch = sync_sealed_directory(sealed, backend=MockRemoteBackend(), verify_remote=True)
    assert arch["status"] == "ok"

    # Under strict_zero_loss -> must fail closed with status=blocked!
    qa_strict = run_qa_convert(Path(arch["ledger_path"]), tmp_path / "out_strict", meta_dir=meta_dir, strict_zero_loss=True)
    assert qa_strict["status"] == "blocked"

    # Normal calendar mode -> must NOT drop the day; must retain with execution_unverified + all 4 reasons!
    out_dir = tmp_path / "out_cal"
    qa = run_qa_convert(Path(arch["ledger_path"]), out_dir, meta_dir=meta_dir, strict_zero_loss=False)
    assert qa["status"] == "ok"
    assert qa["execution_unverified_asset_days"] == 1
    cal = json.loads((out_dir / "eligibility_calendar.json").read_text(encoding="utf-8"))
    assert len(cal) == 1
    row = cal[0]
    assert row["eligibility_status"] == "execution_unverified"
    assert set(row["unverified_reasons"]) == {
        "uncovered_gap",
        "local_loss",
        "clock_uncertified_gt_1ms",
        "spec_change_isolation",
    }
    # Verify CSV also retains the unverified row
    csv_rows = list(csv.DictReader((out_dir / "eligibility_calendar.csv").open(encoding="utf-8")))
    assert len(csv_rows) == 1
    assert csv_rows[0]["eligibility_status"] == "execution_unverified"


def test_archive_fault_injection_interruption_and_remote_hash_conflict_blocked(tmp_path: Path) -> None:
    """P1 Gate: Upload interruption and remote same-name different-hash conflict return status=blocked & preserve local files."""
    mvp_cfg = SPECS_DIR / "config.btc_gate_binance_mvp.json"
    base_wall = 1_796_256_000_000_000_000
    base_ms = base_wall // 1_000_000
    events = [
        {
            "op": "frame",
            "recv_wall_ns": base_wall + 1_000_000,
            "recv_mono_ns": 1_000_000,
            "endpoint": "gate_public",
            "conn_id": "A",
            "epoch": 1,
            "payload": json.dumps(
                {"time": base_ms // 1000, "time_ms": base_ms, "channel": "futures.obu", "event": "update", "result": {"t": base_ms, "full": True, "s": "ob.BTC_USDT.50", "u": 1, "b": [["95000.0", "1.0"]], "a": [["95000.1", "1.0"]]}}
            ),
        }
    ]
    sealed = tmp_path / "sealed"
    sealed.mkdir()
    _run_rust_replay(mvp_cfg, events, sealed, boot_id="arch_fault")
    zst_files = list((sealed / "raw").rglob("*.tsv.zst"))
    assert len(zst_files) == 1
    local_file = zst_files[0]
    rel_path = local_file.relative_to(sealed).as_posix()

    # Fault 1: Simulated upload interruption -> status=blocked, local file preserved even with prune_older_than_hours=0
    failing_backend = MockRemoteBackend(fail_on_copy=True)
    res_fail = sync_sealed_directory(sealed, backend=failing_backend, verify_remote=True, prune_older_than_hours=0.0)
    assert res_fail["status"] == "blocked"
    assert local_file.exists(), "Local file MUST NEVER be deleted on upload interruption!"

    # Fault 2: Remote already has same filename with DIFFERENT content -> status=blocked, NO overwrite, local preserved!
    conflict_backend = MockRemoteBackend(fail_on_copy=False)
    preexisting_bytes = b"REMOTE_IMMUTABLE_DIFFERENT_OBJECT"
    conflict_backend.store[rel_path] = preexisting_bytes
    res_conflict = sync_sealed_directory(sealed, backend=conflict_backend, verify_remote=True, prune_older_than_hours=0.0)
    assert res_conflict["status"] == "blocked"
    assert len(res_conflict["conflicts"]) == 1
    assert res_conflict["conflicts"][0]["action"] == "ISOLATED_LOCAL_PRESERVED_NO_OVERWRITE"
    assert conflict_backend.store[rel_path] == preexisting_bytes, "Remote immutable object MUST NEVER be overwritten!"
    assert local_file.exists(), "Local file MUST NEVER be deleted on remote conflict!"

    # Clean path: upload + verify + safe pruning only once older than 24h
    clean_backend = MockRemoteBackend(fail_on_copy=False)
    res_ok_young = sync_sealed_directory(
        sealed, backend=clean_backend, verify_remote=True, prune_older_than_hours=24.0
    )
    assert res_ok_young["status"] == "ok"
    assert local_file.exists(), "Recent file (<24h) should not be pruned yet"

    res_ok_aged = sync_sealed_directory(
        sealed,
        backend=clean_backend,
        verify_remote=True,
        prune_older_than_hours=24.0,
        now_epoch_s=local_file.stat().st_mtime + 25 * 3600.0,
    )
    assert res_ok_aged["status"] == "ok"
    assert rel_path in res_ok_aged["pruned_local_files"]
    assert not local_file.exists(), "Verified file older than 24h should be safely pruned"


def test_meta_daemon_chrony_parser_spec_diff_and_disk_alert(tmp_path: Path) -> None:
    """Verify meta_daemon chrony 1ms certification formula, spec change isolation detection, RTT CSV, and disk usage."""
    good_output = """Reference ID    : A9FEA97B (169.254.169.123)
Stratum         : 3
Ref time (UTC)  : Thu Dec 03 00:00:00 2026
System time     : 0.000012345 seconds fast of NTP time
Last offset     : +0.000045000 seconds
RMS offset      : 0.000050000 seconds
Frequency       : 1.234 ppm slow
Residual freq   : +0.001 ppm
Skew            : 0.020 ppm
Root delay      : 0.000200000 seconds
Root dispersion : 0.000150000 seconds
Update interval : 16.0 seconds
Leap status     : Normal
"""
    parsed_good = parse_chronyc_tracking(good_output)
    assert parsed_good["certified_le_1ms"] is True
    assert parsed_good["estimated_error_ms"] < 0.35

    bad_output = good_output.replace("Root dispersion : 0.000150000 seconds", "Root dispersion : 0.001500000 seconds")
    parsed_bad = parse_chronyc_tracking(bad_output)
    assert parsed_bad["certified_le_1ms"] is False
    assert parsed_bad["estimated_error_ms"] > 1.0

    # Test chrony & RTT CSV writers with injected offline fakes (no network / no chronyc binary)
    crow = sample_chrony_once(tmp_path, runner=lambda: good_output)
    assert crow["certified_le_1ms"] is True
    rrows = sample_rtt_once(tmp_path, exchanges=["binance_futures", "gate_futures"], prober=lambda h, p: 1.25)
    assert len(rrows) == 2
    assert rrows[0]["status"] == "ok"

    # Test fetch_specs_once: Gate decimal header verification + inter-poll change -> isolation flag + spec_changes.jsonl
    seen_headers: list[dict[str, str]] = []
    version_box = [1]

    def fake_fetch(url: str, headers: dict[str, str]) -> bytes:
        seen_headers.append(headers)
        return json.dumps([{"name": "BTC_USDT", "order_price_round": "0.1" if version_box[0] == 1 else "0.5"}]).encode("utf-8")

    prev_hashes: dict[str, str] = {}
    s1 = fetch_specs_once(tmp_path, "startup", exchanges=["gate_futures"], fetcher=fake_fetch, previous_hashes=prev_hashes)
    assert s1["results"]["gate_futures"]["status"] == "ok"
    assert seen_headers[0].get("X-Gate-Size-Decimal") == "1"
    assert s1["spec_change_isolation_flags"] == []

    version_box[0] = 2
    s2 = fetch_specs_once(tmp_path, "utc_midnight", exchanges=["gate_futures"], fetcher=fake_fetch, previous_hashes=prev_hashes)
    assert len(s2["spec_change_isolation_flags"]) == 1
    assert s2["spec_change_isolation_flags"][0]["isolation_required"] is True
    assert (tmp_path / "meta" / "specs" / "spec_changes.jsonl").is_file()

    disk_pct = check_disk_usage_pct(tmp_path)
    assert 0.0 <= disk_pct <= 100.0


def test_collect_guard_and_unified_cli_fail_closed() -> None:
    """P1 Gate: Unified CLI commands follow Section 6.3 and fail closed when unauthorized or missing --offline."""
    # 1. collect without authorization MUST block (exit code 3, status=blocked)
    p_col = subprocess.run(
        [str(BIN_DIR / "collect"), "--config", str(SPECS_DIR / "config.example.json"), "--run-id", "unauth_test"],
        capture_output=True,
        text=True,
    )
    assert p_col.returncode == 3
    assert json.loads(p_col.stdout)["status"] == "blocked"

    # 2. preflight WITHOUT --offline MUST block (exit code 2, status=blocked)
    p_online = subprocess.run(
        [str(BIN_DIR / "preflight"), "--config", str(SPECS_DIR / "config.example.json")],
        capture_output=True,
        text=True,
    )
    assert p_online.returncode == 2
    assert json.loads(p_online.stdout)["status"] == "blocked"

    # 3. preflight --offline MUST succeed cleanly for both MVP and full 24-market configs
    for cfg in (SPECS_DIR / "config.btc_gate_binance_mvp.json", SPECS_DIR / "config.example.json"):
        p_ok = subprocess.run(
            [str(BIN_DIR / "preflight"), "--config", str(cfg), "--offline"],
            capture_output=True,
            text=True,
        )
        assert p_ok.returncode == 0, f"preflight failed for {cfg}: {p_ok.stdout}\n{p_ok.stderr}"
        assert json.loads(p_ok.stdout)["status"] == "ok"


def test_rclone_stat_remote_fail_closed_on_api_errors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Regression: RcloneBackend.stat_remote MUST only return exists=False on rc 3/4 or empty rc 0; API errors raise & block upload."""
    import archive_sync as arch_mod

    backend = RcloneBackend("gdrive_main:tokyo_l2_data")
    captured_cmds: list[list[str]] = []

    def make_fake_run(returncode: int, stdout: str = "", stderr: str = ""):
        def _fake(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
            captured_cmds.append(cmd)
            return subprocess.CompletedProcess(args=cmd, returncode=returncode, stdout=stdout, stderr=stderr)
        return _fake

    # 1. Explicit not-found exit codes (3=dir not found, 4=file not found) -> exists=False
    for rc in (3, 4):
        monkeypatch.setattr(arch_mod.subprocess, "run", make_fake_run(rc, stderr="directory/file not found"))
        info = backend.stat_remote("raw/gate_futures/2026-12-03/gate_futures_20261203_00.tsv.zst")
        assert info.exists is False

    # 2. rc=0 with empty JSON list -> exists=False
    monkeypatch.setattr(arch_mod.subprocess, "run", make_fake_run(0, stdout="[]"))
    assert backend.stat_remote("raw/gate_futures/2026-12-03/empty.tsv.zst").exists is False

    # 3. rc=0 with object metadata -> exists=True with parsed ID/size/hashes
    sample_json = json.dumps([{"ID": "drive-id-1", "Size": 1234, "Hashes": {"md5": "abc", "sha256": "def"}}])
    monkeypatch.setattr(arch_mod.subprocess, "run", make_fake_run(0, stdout=sample_json))
    found = backend.stat_remote("raw/gate_futures/2026-12-03/ok.tsv.zst")
    assert found.exists is True
    assert found.remote_id == "drive-id-1"
    assert found.size == 1234
    assert found.md5 == "abc"
    assert found.sha256 == "def"

    # 4. Transient API / auth / rate-limit / fatal errors (rc=1, 2, 5, 7) MUST raise RuntimeError!
    for bad_rc in (1, 2, 5, 7):
        monkeypatch.setattr(
            arch_mod.subprocess,
            "run",
            make_fake_run(bad_rc, stderr="429 userRateLimitExceeded / transient API error"),
        )
        with pytest.raises(RuntimeError, match=f"rc={bad_rc}"):
            backend.stat_remote("raw/gate_futures/2026-12-03/existing.tsv.zst")

    # 5. Verify copy_file passes --immutable for raw partitions and omits it for mutable daily meta logs
    captured_cmds.clear()
    monkeypatch.setattr(arch_mod.subprocess, "run", make_fake_run(0))
    dummy_local = tmp_path / "dummy.tsv.zst"
    dummy_local.write_bytes(b"abc")
    backend.copy_file(dummy_local, "raw/gate_futures/2026-12-03/dummy.tsv.zst", immutable=True)
    backend.copy_file(dummy_local, "meta/rtt/rtt_20261203.csv", immutable=False)
    assert "--immutable" in captured_cmds[0]
    assert "--immutable" not in captured_cmds[1]

    # Restore real subprocess.run before invoking the Rust replay binary
    monkeypatch.undo()

    # 6. End-to-end: when stat_remote fails transiently, sync_sealed_directory returns status=blocked,
    #    NEVER calls copy_file (zero overwrite risk), and preserves local .tsv.zst!
    mvp_cfg = SPECS_DIR / "config.btc_gate_binance_mvp.json"
    base_wall = 1_796_256_000_000_000_000
    base_ms = base_wall // 1_000_000
    events = [
        {
            "op": "frame",
            "recv_wall_ns": base_wall + 1_000_000,
            "recv_mono_ns": 1_000_000,
            "endpoint": "gate_public",
            "conn_id": "A",
            "epoch": 1,
            "payload": json.dumps(
                {"time": base_ms // 1000, "time_ms": base_ms, "channel": "futures.obu", "event": "update", "result": {"t": base_ms, "full": True, "s": "ob.BTC_USDT.50", "u": 1, "b": [["95000.0", "1.0"]], "a": [["95000.1", "1.0"]]}}
            ),
        }
    ]
    sealed = tmp_path / "sealed"
    sealed.mkdir()
    _run_rust_replay(mvp_cfg, events, sealed, boot_id="stat_err_test")
    local_zst = list((sealed / "raw").rglob("*.tsv.zst"))[0]

    stat_fail_backend = MockRemoteBackend(fail_on_stat=True)
    res = sync_sealed_directory(sealed, backend=stat_fail_backend, verify_remote=True, prune_older_than_hours=0.0)
    assert res["status"] == "blocked"
    assert stat_fail_backend.copy_calls == 0, "MUST NOT attempt upload when stat_remote fails!"
    assert local_zst.exists(), "MUST NOT delete local file when stat_remote fails!"

