"""Comprehensive unit, parity, anti-leakage, cache, and multi-exchange tests for the unified T2 framework."""
from __future__ import annotations

import csv
import hashlib
import io
import json
import math
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest

import numpy as np
import zstandard as zstd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "kaggle"))
sys.path.insert(0, str(ROOT / "kaggle" / "tasks"))

import full_day_glft as fd
import launcher
from core_glft_rules import quoted_ticks
from test_kaggle_full_day_glft import DAY0, NS, build_synthetic_hbt_and_quotes
from tokyo_l2_bt_cache import (
    compute_cache_key,
    converter_code_sha256,
    source_partitions_digest,
    verify_cache_dir,
    write_cache_manifest,
)
from tokyo_l2_bt_stream_convert import StreamConverter, discover_partitions
from tokyo_l2_exchange_adapters import (
    compute_strategy_scenario,
    contract_spec_digest,
    load_live_contract_from_meta,
    parse_live_contract,
    to_base_symbol,
    to_native_symbol,
)
from tokyo_l2_experiment_config import (
    load_experiment_config,
    tardis_legacy_config,
    validate_config,
)
from tokyo_l2_report_writer import write_unified_reports
from tokyo_l2_research_stages import (
    lock_stage5c_selection,
    run_models_on_arrays,
    train_and_lock_stage4_controls,
    verify_stage4_lock,
    verify_stage5c_lock,
)


def _write_zst_partition(
    raw_root: Path,
    exchange: str,
    date_utc: str,
    window_id: str,
    lines: list[str],
    boot_id: str = "boot-test-1",
) -> tuple[Path, Path]:
    out_dir = raw_root / exchange / date_utc
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{exchange}_{window_id.replace(':', '')}__{boot_id}"
    dpath = out_dir / f"{stem}.tsv.zst"
    mpath = out_dir / f"{stem}.manifest.json"
    blob = ("\n".join(lines) + "\n").encode("utf-8")
    comp = zstd.ZstdCompressor().compress(blob)
    dpath.write_bytes(comp)
    sha = hashlib.sha256(comp).hexdigest()
    first_wall = int(lines[0].split("\t", 1)[0])
    last_wall = int(lines[-1].split("\t", 1)[0])
    manifest = {
        "exchange": exchange,
        "date_utc": date_utc,
        "window_id": window_id,
        "partition_minutes": 15,
        "planned_start_wall_ns": DAY0,
        "planned_end_wall_ns": DAY0 + 900 * NS,
        "start_wall_ns": first_wall,
        "end_wall_ns": last_wall,
        "boot_id": boot_id,
        "file_name": dpath.name,
        "bytes": len(comp),
        "sha256": sha,
        "lines": len(lines),
        "first_rx_seq": 1,
        "last_rx_seq": len(lines),
        "local_loss_lines": 0,
        "late_frames_accepted": 0,
        "late_frames_rejected": 0,
    }
    mpath.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return dpath, mpath


def _write_meta_snapshot(meta_dir: Path, exchange: str, payload: object, reply_wall_ns: int) -> None:
    specs_dir = meta_dir / "specs"
    specs_dir.mkdir(parents=True, exist_ok=True)
    raw_name = f"{exchange}_20261122T235500Z_spec.json"
    raw_bytes = json.dumps(payload).encode("utf-8")
    (specs_dir / raw_name).write_bytes(raw_bytes)
    meta = {
        "exchange": exchange,
        "reply_wall_ns": int(reply_wall_ns),
        "raw_file": raw_name,
        "sha256": hashlib.sha256(raw_bytes).hexdigest(),
    }
    (specs_dir / f"{exchange}_20261122T235500Z_spec.meta.json").write_text(json.dumps(meta), encoding="utf-8")


def _build_synthetic_raw_exchange_feed(
    tmp: Path,
    exchange: str,
    base_symbol: str,
    base_px: float,
    tick: float,
    minutes_total: int = 12,
) -> tuple[Path, Path]:
    """Generate realistic raw TSV.ZST leader + maker feeds for Gate, Bybit, or Bitget."""
    raw_root = tmp / "fetched" / "raw"
    meta_dir = tmp / "fetched" / "meta"
    date_utc = "2026-11-23"
    window_id = "2026-11-23T00:00Z"
    bin_sym = to_native_symbol("binance_futures", base_symbol)
    mak_sym = to_native_symbol(exchange, base_symbol)

    bin_lines: list[str] = []
    mak_lines: list[str] = []
    rx_b = rx_m = 0
    step_ns = 100_000_000  # 100ms
    n_steps = minutes_total * 600
    seq = 100

    def add_b(wall: int, obj: dict) -> None:
        nonlocal rx_b
        rx_b += 1
        bin_lines.append(f"{wall}\t{wall - DAY0 + 1000}\tconn_b\t1\t{rx_b}\tDATA\t{json.dumps(obj, separators=(',', ':'))}")

    def add_m(wall: int, obj: dict) -> None:
        nonlocal rx_m
        rx_m += 1
        mak_lines.append(f"{wall}\t{wall - DAY0 + 1000}\tconn_m\t1\t{rx_m}\tDATA\t{json.dumps(obj, separators=(',', ':'))}")

    def fmt_px(v: float) -> str:
        return f"{v:.8f}".rstrip("0").rstrip(".")

    for i in range(n_steps + 5):
        t = DAY0 + (i + 1) * step_ns
        ex_ms = (t - 2_000_000) // 1_000_000
        osc = round(4.0 * math.sin(i / 15.0)) * tick
        mid = base_px + osc
        bid = mid - 5 * tick
        ask = mid + 5 * tick

        lead = (15 * tick) if (i > 6050 and (i % 40) < 5) else 0.0
        add_b(t - 5_000_000, {
            "stream": f"{bin_sym.lower()}@bookTicker",
            "data": {
                "e": "bookTicker", "s": bin_sym, "T": ex_ms - 5,
                "b": fmt_px(bid + lead), "B": "10", "a": fmt_px(ask + lead), "A": "10",
            },
        })

        bids_lvl = [[fmt_px(bid - lvl * tick), "25"] for lvl in range(5)]
        asks_lvl = [[fmt_px(ask + lvl * tick), "25"] for lvl in range(5)]
        is_snap = i == 0

        if exchange == "gate_futures":
            seq += 1
            if is_snap:
                add_m(t, {
                    "channel": "futures.obu", "event": "update", "time_ms": ex_ms,
                    "result": {"s": f"ob.{mak_sym}.50", "t": ex_ms, "u": seq, "full": True, "b": bids_lvl, "a": asks_lvl},
                })
            else:
                add_m(t, {
                    "channel": "futures.obu", "event": "update", "time_ms": ex_ms,
                    "result": {"s": f"ob.{mak_sym}.50", "t": ex_ms, "U": seq, "u": seq, "b": bids_lvl[:2], "a": asks_lvl[:2]},
                })
            add_m(t + 100_000, {
                "channel": "futures.book_ticker", "event": "update", "time_ms": ex_ms,
                "result": {"s": mak_sym, "t": ex_ms, "b": fmt_px(bid), "B": "25", "a": fmt_px(ask), "A": "25"},
            })
            if i % 2 == 0:
                is_buy = (i // 2) % 2 == 0
                depth_off = ((i % 6) + 1) * 5 * tick
                tpx = (ask + depth_off) if is_buy else (bid - depth_off)
                add_m(t + 500_000, {
                    "channel": "futures.trades", "event": "update", "time_ms": ex_ms,
                    "result": [{"contract": mak_sym, "id": i + 1, "price": fmt_px(tpx),
                                "size": 5 if is_buy else -5, "create_time_ms": ex_ms}],
                })
        elif exchange == "bybit_linear":
            seq += 1
            add_m(t, {
                "topic": f"orderbook.50.{mak_sym}",
                "type": "snapshot" if is_snap else "delta",
                "ts": ex_ms, "cts": ex_ms,
                "data": {"s": mak_sym, "u": 1 if is_snap else seq, "b": bids_lvl if is_snap else bids_lvl[:2],
                         "a": asks_lvl if is_snap else asks_lvl[:2]},
            })
            add_m(t + 100_000, {
                "topic": f"orderbook.1.{mak_sym}", "type": "snapshot", "ts": ex_ms, "cts": ex_ms,
                "data": {"s": mak_sym, "u": seq, "b": [[fmt_px(bid), "25"]], "a": [[fmt_px(ask), "25"]]},
            })
            if i % 2 == 0:
                is_buy = (i // 2) % 2 == 0
                depth_off = ((i % 6) + 1) * 5 * tick
                tpx = (ask + depth_off) if is_buy else (bid - depth_off)
                add_m(t + 500_000, {
                    "topic": f"publicTrade.{mak_sym}", "ts": ex_ms,
                    "data": [{"s": mak_sym, "i": f"byb-{i+1}", "T": ex_ms,
                              "S": "Buy" if is_buy else "Sell", "p": fmt_px(tpx), "v": "0.05"}],
                })
        elif exchange == "bitget_v3":
            prev_seq = seq
            seq += 1
            add_m(t, {
                "action": "snapshot" if is_snap else "update",
                "arg": {"topic": "books50", "symbol": mak_sym},
                "ts": ex_ms,
                "data": [{"ts": str(ex_ms), "seq": seq, "pseq": prev_seq,
                          "b": bids_lvl if is_snap else bids_lvl[:2],
                          "a": asks_lvl if is_snap else asks_lvl[:2]}],
            })
            add_m(t + 100_000, {
                "action": "snapshot",
                "arg": {"topic": "books1", "symbol": mak_sym},
                "ts": ex_ms,
                "data": [{"ts": str(ex_ms), "b": [[fmt_px(bid), "25"]], "a": [[fmt_px(ask), "25"]]}],
            })
            if i % 2 == 0:
                is_buy = (i // 2) % 2 == 0
                depth_off = ((i % 6) + 1) * 5 * tick
                tpx = (ask + depth_off) if is_buy else (bid - depth_off)
                add_m(t + 500_000, {
                    "action": "update",
                    "arg": {"topic": "publicTrade", "symbol": mak_sym},
                    "ts": ex_ms,
                    "data": [{"i": f"bg-{i+1}", "T": str(ex_ms),
                              "S": "buy" if is_buy else "sell", "p": fmt_px(tpx), "v": "50"}],
                })

    _write_zst_partition(raw_root, "binance_futures", date_utc, window_id, bin_lines)
    _write_zst_partition(raw_root, exchange, date_utc, window_id, mak_lines)
    return tmp / "fetched", meta_dir


class UnifiedFrameworkTests(unittest.TestCase):
    def test_config_validation_and_leakage_guards(self):
        cfg = load_experiment_config(ROOT / "configs" / "tokyo_l2_20261123_gate_btc.json")
        self.assertEqual(cfg["base_symbol"], "BTC")
        self.assertEqual(cfg["signal_native_symbol"], "BTCUSDT")
        self.assertEqual(cfg["making_native_symbol"], "BTC_USDT")
        self.assertEqual(cfg["models"], ["glft", "both_1p0", "defensive_1p0", "risk_side_pause"])
        self.assertEqual(cfg["latency"]["binance_extra_visible_delay_ns"], 0)
        self.assertEqual(cfg["order_controls"]["min_requote_ns"], 0)
        self.assertFalse(cfg["stage4"]["authorized"])
        self.assertFalse(cfg["stage5c"]["authorized"])

        # Stage 4 train/eval overlap must fail closed
        with self.assertRaises(ValueError):
            validate_config({"dates": {"eval_dates": ["2026-11-23"], "train_dates": ["2026-11-23"]}})
        # Stage 5C holdout/eval overlap must fail closed
        with self.assertRaises(ValueError):
            validate_config({"dates": {"eval_dates": ["2026-11-23"], "holdout_dates": ["2026-11-23"]}})
        # Native order modify must remain prohibited
        with self.assertRaises(ValueError):
            validate_config({"order_controls": {"native_order_modify_enabled": True}})
        # Unknown making exchange rejected
        with self.assertRaises(ValueError):
            validate_config({"making_exchange": "unknown_ex"})

    def test_frozen_defensive_1p0_and_tardis_legacy_parity(self):
        # Verify defensive_1p0 (quote_mode=2) shifts ONLY threatened side
        bb, ba, wb_pos, wa_pos = quoted_ticks(1000.0, 0.0, 0.1, 2.0, 999, 1001, 1.0, 3.0, 2, "unified")
        self.assertEqual(wb_pos, bb)
        self.assertGreater(wa_pos, ba)
        bb2, ba2, wb_neg, wa_neg = quoted_ticks(1000.0, 0.0, 0.1, 2.0, 999, 1001, 1.0, -3.0, 2, "unified")
        self.assertLess(wb_neg, bb2)
        self.assertEqual(wa_neg, ba2)

        # Verify Tardis legacy preset (2.5ms delay, 20ms cooldown, legacy clamp) runs cleanly
        tcfg = tardis_legacy_config(eval_dates=["2026-06-01"], models=["glft", "defensive_1p0"], stages=["stage1", "stage2", "stage3"])
        self.assertEqual(tcfg["latency"]["binance_extra_visible_delay_ns"], 2_500_000)
        self.assertEqual(tcfg["order_controls"]["min_requote_ns"], 20_000_000)
        self.assertEqual(tcfg["strategy"]["post_only_clamp_mode"], "legacy")

        with tempfile.TemporaryDirectory() as tmp:
            hbt_path, b_ts, b_mid, g_loc, g_ex, g_mid = build_synthetic_hbt_and_quotes(Path(tmp), 12)
            tcfg["strategy"]["trade_minutes"] = 2
            spec = {
                "exchange": "gate_futures", "native_symbol": "BTC_USDT", "base_symbol": "BTC",
                "tick_size": 0.1, "lot_size": 1.0, "min_order_size": 1.0, "contract_value": 0.0001,
            }
            res = run_models_on_arrays(
                config=tcfg, date_utc="2026-06-01", hbt_path=hbt_path,
                bbo_local_ns=b_ts, bbo_mid=b_mid,
                maker_l1_local_ns=g_loc, maker_l1_exch_ns=g_ex, maker_l1_mid=g_mid,
                contract_spec=spec, day_start_ns=DAY0, day_end_ns=DAY0 + 12 * 60 * NS,
                roi_low=59_990.0, roi_high=60_010.0, witness_stop_ns=DAY0 + 12 * 60 * NS,
            )
            self.assertEqual(res["effective_parameters"]["binance_extra_visible_delay_ns"], 2_500_000)
            self.assertEqual(res["effective_parameters"]["min_requote_ns"], 20_000_000)
            self.assertTrue(res["stage3"]["parity_verified"])
            self.assertEqual(res["stage3"]["parameters"]["min_cancel_ns"], 20_000_000)

    def test_unified_matches_full_day_glft_bit_for_bit(self):
        """New unified runner must match legacy full_day_glft.run_three_models_on_arrays identically."""
        with tempfile.TemporaryDirectory() as tmp:
            hbt_path, b_ts, b_mid, g_loc, g_ex, g_mid = build_synthetic_hbt_and_quotes(Path(tmp), 12)
            day_end = DAY0 + 12 * 60 * NS
            legacy_contract = {
                "name": "BTC_USDT", "order_price_round": "0.1", "order_size_min": 1,
                "quanto_multiplier": "0.0001", "maker_fee_rate": "0.0002", "taker_fee_rate": "0.0005",
            }
            episodes = [(DAY0 + 630 * NS, DAY0 + 631 * NS)]
            legacy = fd.run_three_models_on_arrays(
                hbt_path, b_ts, b_mid, g_loc, g_ex, g_mid,
                legacy_contract, DAY0, day_end, 59_990.0, 60_010.0, episodes,
                trade_minutes=2, witness_stop_ns=day_end,
            )

            cfg = validate_config({
                "symbol": "BTC",
                "making_exchange": "gate_futures",
                "dates": {"eval_dates": ["2026-11-23"]},
                "strategy": {"trade_minutes": 2},
                "models": ["glft", "both_1p0", "defensive_1p0", "risk_side_pause"],
                "stages": ["stage1", "stage2", "stage3", "stage4", "stage5a", "stage5b", "stage5c"],
            })
            unified_spec = parse_live_contract("gate_futures", [legacy_contract], "BTC_USDT")
            unified = run_models_on_arrays(
                config=cfg, date_utc="2026-11-23", hbt_path=hbt_path,
                bbo_local_ns=b_ts, bbo_mid=b_mid,
                maker_l1_local_ns=g_loc, maker_l1_exch_ns=g_ex, maker_l1_mid=g_mid,
                contract_spec=unified_spec, day_start_ns=DAY0, day_end_ns=day_end,
                roi_low=59_990.0, roi_high=60_010.0, episodes=episodes,
                witness_stop_ns=day_end,
            )

            self.assertEqual(
                unified["effective_parameters"]["shared_first_eligible_entry_ns"],
                legacy["effective_parameters"]["shared_first_eligible_entry_ns"],
            )
            for m in ("glft", "both_1p0", "risk_side_pause"):
                lt = legacy["models"][m]["trading"]
                ut = unified["models"][m]["trading"]
                for k in ("engine_fills", "buy_fills", "sell_fills", "submits", "cancels",
                          "max_order_ops_per_s", "origin_activations", "tie_excluded_checks"):
                    self.assertEqual(ut[k], lt[k], f"{m}.{k} mismatch")
                for k in ("gross_marked_pnl_usdt", "net_marked_pnl_usdt_2bps", "fee_usdt_2bps",
                          "trading_value_usdt", "position_std_contracts", "max_abs_position_contracts"):
                    self.assertAlmostEqual(ut[k], lt[k], places=9, msg=f"{m}.{k} mismatch")

            # Verify defensive_1p0 + Stages 1, 2, 3, 5A, 5B + honest Stage 4/5C pending + report writer
            self.assertIn("defensive_1p0", unified["models"])
            self.assertTrue(all(v["accounting_closed"] for v in unified["stage1"]["models"].values()))
            self.assertTrue(unified["stage3"]["parity_verified"])
            self.assertEqual(unified["stage3"]["parameters"]["min_cancel_ns"], 0)
            self.assertTrue(unified["stage5a"]["all_four_core_models_present"])
            self.assertEqual(unified["stage5b"]["status"], "complete")
            self.assertEqual(unified["stage4"]["status"], "pending_independent_training_date")
            self.assertEqual(unified["stage5c"]["status"], "pending_authorized_holdout_date")

            rep_dir = Path(tmp) / "reports"
            written = write_unified_reports({
                "status": "complete",
                "started_utc": "2026-11-23T00:00:00Z",
                "finished_utc": "2026-11-23T00:01:00Z",
                "config": cfg,
                "days": {"2026-11-23": {"research": unified}},
            }, rep_dir)
            self.assertTrue((rep_dir / "unified_report.json").is_file())
            self.assertTrue((rep_dir / "models_summary.csv").is_file())
            self.assertTrue((rep_dir / "research_report.md").is_file())
            with (rep_dir / "models_summary.csv").open(encoding="utf-8") as fh:
                rows = list(csv.DictReader(fh))
            self.assertEqual([r["model"] for r in rows], ["glft", "both_1p0", "defensive_1p0", "risk_side_pause"])
            self.assertIn("research_report.md", written.values())

    def test_stage4_independent_train_lock_and_stage5c_preregistered_lock(self):
        with tempfile.TemporaryDirectory() as tmp:
            hbt_path, b_ts, b_mid, g_loc, g_ex, g_mid = build_synthetic_hbt_and_quotes(Path(tmp), 12)
            day_end = DAY0 + 12 * 60 * NS
            spec = {
                "exchange": "gate_futures", "native_symbol": "BTC_USDT", "base_symbol": "BTC",
                "tick_size": 0.1, "lot_size": 1.0, "min_order_size": 1.0, "contract_value": 0.0001,
            }
            cfg = validate_config({
                "dates": {"train_dates": ["2026-11-20"], "eval_dates": ["2026-11-23"], "holdout_dates": ["2026-11-25"]},
                "strategy": {"trade_minutes": 2},
                "models": ["glft", "both_1p0", "defensive_1p0", "risk_side_pause", "static_widen", "local_vol_pause"],
                "stages": ["stage1", "stage2", "stage3", "stage4", "stage5a", "stage5b", "stage5c"],
            })
            train_bundle = {
                "date_utc": "2026-11-20", "hbt_path": hbt_path,
                "bbo_local_ns": b_ts, "bbo_mid": b_mid,
                "maker_l1_local_ns": g_loc, "maker_l1_exch_ns": g_ex, "maker_l1_mid": g_mid,
                "contract_spec": spec, "day_start_ns": DAY0, "day_end_ns": day_end,
                "roi_low": 59_990.0, "roi_high": 60_010.0, "witness_stop_ns": day_end,
            }
            lock4_path = Path(tmp) / "stage4_lock.json"
            lock4 = train_and_lock_stage4_controls(train_day_bundles=[train_bundle], config=cfg, lock_path=lock4_path)
            self.assertEqual(lock4["training_status"], "locked")

            # Tampering or eval overlap must fail closed
            tampered = dict(lock4)
            tampered["baseline_train_glft_fills"] = -1
            with self.assertRaises(ValueError):
                verify_stage4_lock(tampered, ["2026-11-23"])
            with self.assertRaises(ValueError):
                verify_stage4_lock(lock4, ["2026-11-20"])

            # Locked eval runs ALL 6 models!
            eval_res = run_models_on_arrays(
                config=cfg, date_utc="2026-11-23", hbt_path=hbt_path,
                bbo_local_ns=b_ts, bbo_mid=b_mid,
                maker_l1_local_ns=g_loc, maker_l1_exch_ns=g_ex, maker_l1_mid=g_mid,
                contract_spec=spec, day_start_ns=DAY0, day_end_ns=day_end,
                roi_low=59_990.0, roi_high=60_010.0, witness_stop_ns=day_end,
                stage4_lock=lock4,
            )
            self.assertEqual(
                set(eval_res["models"].keys()),
                {"glft", "both_1p0", "defensive_1p0", "risk_side_pause", "static_widen", "local_vol_pause"},
            )
            self.assertEqual(eval_res["stage4"]["status"], "evaluated_with_independent_lock")

            # Stage 5C pre-registered lock before touching holdout date 2026-11-25
            lock5c_path = Path(tmp) / "stage5c_lock.json"
            lock5c = lock_stage5c_selection(
                stage5a_reports_by_date={"2026-11-23": eval_res["stage5a"]},
                config=cfg, holdout_dates=["2026-11-25"], lock_path=lock5c_path,
            )
            self.assertIn(lock5c["selected_mode"], ("both_1p0", "defensive_1p0", "risk_side_pause"))
            # Explored date as holdout must fail closed
            with self.assertRaises(ValueError):
                verify_stage5c_lock(lock5c, ["2026-11-23"])

            holdout_cfg = validate_config({
                "dates": {"eval_dates": ["2026-11-25"]},
                "strategy": {"trade_minutes": 2},
                "models": ["glft", lock5c["selected_mode"]],
                "stages": ["stage1", "stage2", "stage5a", "stage5c"],
            })
            holdout_res = run_models_on_arrays(
                config=holdout_cfg, date_utc="2026-11-25", hbt_path=hbt_path,
                bbo_local_ns=b_ts, bbo_mid=b_mid,
                maker_l1_local_ns=g_loc, maker_l1_exch_ns=g_ex, maker_l1_mid=g_mid,
                contract_spec=spec, day_start_ns=DAY0, day_end_ns=day_end,
                roi_low=59_990.0, roi_high=60_010.0, witness_stop_ns=day_end,
                stage5c_lock=lock5c,
            )
            self.assertEqual(holdout_res["stage5c"]["status"], "evaluated_with_preregistered_lock")
            self.assertEqual(holdout_res["stage5c"]["selected_mode"], lock5c["selected_mode"])

    def test_conversion_cache_reuse_and_invalidation(self):
        with tempfile.TemporaryDirectory() as tmp:
            cdir = Path(tmp) / "converted"
            cdir.mkdir()
            for fname in ("binance_bbo_BTC_20261123.npz", "gate_futures_l2_BTC_20261123.npz", "gate_futures_l1_BTC_20261123.npz"):
                np.savez_compressed(cdir / fname, x=np.array([1, 2, 3]))
            stream_report = {
                "status": "ok",
                "artifacts": [
                    {"kind": "binance_bbo", "path": "binance_bbo_BTC_20261123.npz", "rows": 3,
                     "sha256": hashlib.sha256((cdir / "binance_bbo_BTC_20261123.npz").read_bytes()).hexdigest()},
                    {"kind": "follower_l2_hbt", "path": "gate_futures_l2_BTC_20261123.npz", "rows": 3,
                     "sha256": hashlib.sha256((cdir / "gate_futures_l2_BTC_20261123.npz").read_bytes()).hexdigest()},
                    {"kind": "follower_l1_bbo", "path": "gate_futures_l1_BTC_20261123.npz", "rows": 3,
                     "sha256": hashlib.sha256((cdir / "gate_futures_l1_BTC_20261123.npz").read_bytes()).hexdigest()},
                ],
            }
            (cdir / "stream_report.json").write_text(json.dumps(stream_report), encoding="utf-8")
            src_sha = source_partitions_digest([
                {"exchange": "binance_futures", "window_id": "w1", "manifest_sha256": "m1", "data_sha256": "d1"},
                {"exchange": "gate_futures", "window_id": "w1", "manifest_sha256": "m2", "data_sha256": "d2"},
            ])
            conv_sha = converter_code_sha256([ROOT / "scripts" / "tokyo_l2_bt_stream_convert.py"])
            spec_sha = "spec-sha-abc"
            common = dict(
                signal_exchange="binance_futures", making_exchange="gate_futures", base_symbol="BTC",
                date_utc="2026-11-23", source_sha256=src_sha, converter_sha256=conv_sha,
                converter_version="stream_convert_v3", contract_spec_sha256=spec_sha,
            )
            write_cache_manifest(cdir, stream_report=stream_report, **common)
            ok, reason, man = verify_cache_dir(cdir, **common)
            self.assertTrue(ok, reason)
            self.assertEqual(man["cache_key"], compute_cache_key(**common))

            # Changing source hash, converter hash, or spec hash invalidates cache fail-closed
            ok_src, reason_src, _ = verify_cache_dir(cdir, **{**common, "source_sha256": "different"})
            self.assertFalse(ok_src)
            self.assertIn("source_sha256", reason_src)

            ok_spec, reason_spec, _ = verify_cache_dir(cdir, **{**common, "contract_spec_sha256": "different"})
            self.assertFalse(ok_spec)
            self.assertIn("contract_spec_sha256", reason_spec)

            # Corrupting an artifact invalidates cache fail-closed
            (cdir / "binance_bbo_BTC_20261123.npz").write_bytes(b"corrupted")
            ok_corrupt, reason_corrupt, _ = verify_cache_dir(cdir, **common)
            self.assertFalse(ok_corrupt)
            self.assertIn("corrupted_artifact_sha256", reason_corrupt)

    def test_multi_exchange_and_multi_symbol_end_to_end(self):
        """Config-only switch runs raw TSV -> StreamConverter -> meta spec -> 4 models + Stages 1-5B
        across Gate ETH, Bybit BTC, and Bitget DOGE (dual-grid quote_tick_mult=10)."""
        cases = [
            (
                "gate_futures", "ETH", 3000.0, 0.05, 1, None,
                [{"name": "ETH_USDT", "order_price_round": "0.05", "order_size_min": 1,
                  "quanto_multiplier": "0.01", "maker_fee_rate": "0.0002", "taker_fee_rate": "0.0005"}],
            ),
            (
                "bybit_linear", "BTC", 60000.0, 0.1, 1, None,
                {"pages": [{"result": {"list": [{
                    "symbol": "BTCUSDT",
                    "priceFilter": {"tickSize": "0.1"},
                    "lotSizeFilter": {"qtyStep": "0.001", "minOrderQty": "0.001"},
                }]}}]},
            ),
            (
                "bitget_v3", "DOGE", 0.20, 0.00001, 10, 0.000001,
                {"data": [{
                    "symbol": "DOGEUSDT", "priceMultiplier": "0.00001",
                    "quantityMultiplier": "1", "minOrderQty": "10",
                }]},
            ),
        ]
        for exchange, sym, base_px, legal_tick, qmult, hbt_tick_override, meta_payload in cases:
            with self.subTest(exchange=exchange, symbol=sym), tempfile.TemporaryDirectory() as tmp:
                tdir = Path(tmp)
                fetched_dir, meta_dir = _build_synthetic_raw_exchange_feed(
                    tdir, exchange, sym, base_px, legal_tick, minutes_total=12
                )
                _write_meta_snapshot(meta_dir, exchange, meta_payload, reply_wall_ns=DAY0 - 60 * NS)

                native_sym = to_native_symbol(exchange, sym)
                conv = StreamConverter(native_sym, config=None, coverage_date=None, making_exchange=exchange)
                parts_dir = tdir / "state" / "parts"
                for _ex, dpath, mpath, _sk in discover_partitions(fetched_dir, making_exchange=exchange):
                    conv.process_partition(dpath, mpath, parts_dir, progress_every=0)
                out_dir = tdir / "converted"
                srep = conv.finalize(parts_dir, out_dir, meta_dir=None, label_date="2026-11-23")
                self.assertEqual(srep["status"], "ok", f"{exchange} {sym} failed: {srep['failed_items']}")

                live = load_live_contract_from_meta(
                    None, meta_dir, exchange, native_sym, DAY0,
                    quote_tick_mult=qmult, hbt_tick_size_override=hbt_tick_override,
                )
                spec = live["contract"]
                self.assertEqual(spec["exchange"], exchange)
                self.assertEqual(spec["native_symbol"], native_sym)
                self.assertEqual(spec["quote_tick_mult"], qmult)

                arts = {a["kind"]: out_dir / a["path"] for a in srep["artifacts"]}
                with np.load(arts["binance_bbo"]) as zb:
                    b_ts, b_mid = zb["local_ts_ns"].copy(), zb["mid_px"].copy()
                with np.load(arts["follower_l1_bbo"]) as zl:
                    m_loc, m_ex, m_mid = zl["local_ts_ns"].copy(), zl["exch_ts_ns"].copy(), zl["mid_px"].copy()

                cfg = validate_config({
                    "making_exchange": exchange,
                    "symbol": sym,
                    "dates": {"eval_dates": ["2026-11-23"]},
                    "strategy": {"trade_minutes": 2},
                    "models": ["glft", "both_1p0", "defensive_1p0", "risk_side_pause"],
                    "stages": ["stage1", "stage2", "stage3", "stage5a", "stage5b"],
                })
                day_end = DAY0 + 12 * 60 * NS
                res = run_models_on_arrays(
                    config=cfg, date_utc="2026-11-23", hbt_path=arts["follower_l2_hbt"],
                    bbo_local_ns=b_ts, bbo_mid=b_mid,
                    maker_l1_local_ns=m_loc, maker_l1_exch_ns=m_ex, maker_l1_mid=m_mid,
                    contract_spec=spec, day_start_ns=DAY0, day_end_ns=day_end,
                    roi_low=base_px * 0.98, roi_high=base_px * 1.02, witness_stop_ns=day_end,
                )
                self.assertTrue(res["stage5a"]["all_four_core_models_present"])
                self.assertTrue(res["stage3"]["parity_verified"])
                self.assertEqual(res["effective_parameters"]["making_exchange"], exchange)
                self.assertEqual(res["effective_parameters"]["making_native_symbol"], native_sym)
                self.assertEqual(res["effective_parameters"]["quote_tick_mult"], qmult)
                self.assertTrue(all(v["accounting_closed"] for v in res["stage1"]["models"].values()))

    def test_kaggle_job_package_closure_unified(self):
        import os
        import qa_convert

        job_path = ROOT / "kaggle" / "job.json"
        job = json.loads(job_path.read_text(encoding="utf-8"))
        self.assertEqual(job["task"], "kaggle/tasks/unified_backtest.py")
        collector = Path(qa_convert.__file__).resolve().parents[2]
        blob, _ = launcher.build_bundle(ROOT, collector, list(job["files"]) + [job["task"]])
        with tempfile.TemporaryDirectory() as tmp:
            unpack = Path(tmp) / "unpacked"
            with tarfile.open(fileobj=io.BytesIO(blob), mode="r:gz") as tf:
                tf.extractall(unpack, filter="data")
            self.assertTrue((unpack / "repo" / "configs" / "tokyo_l2_20261123_gate_btc.json").is_file())
            self.assertTrue((unpack / "repo" / "kaggle" / "tasks" / "unified_backtest.py").is_file())
            env = dict(os.environ, PYTHONPATH=os.pathsep.join([
                str(unpack / "repo" / "kaggle" / "tasks"),
                str(unpack / "repo" / "scripts"),
                str(unpack / "collector" / "python"),
            ]))
            code = (
                "import unified_backtest, tokyo_l2_exchange_adapters, tokyo_l2_experiment_config, "
                "tokyo_l2_bt_cache, tokyo_l2_research_stages, tokyo_l2_report_writer, "
                "audit_gate_stage5b_same_fill, analyze_gate_1645_fill_quality; "
                "cfg = tokyo_l2_experiment_config.load_experiment_config('repo/configs/tokyo_l2_20261123_gate_btc.json'); "
                "assert cfg['models'] == ['glft', 'both_1p0', 'defensive_1p0', 'risk_side_pause']; "
                "print('BUNDLE_IMPORT_OK')"
            )
            r = subprocess.run([sys.executable, "-c", code], cwd=unpack, env=env, capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
            self.assertIn("BUNDLE_IMPORT_OK", r.stdout)


if __name__ == "__main__":
    unittest.main()
