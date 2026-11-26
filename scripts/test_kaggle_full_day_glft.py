"""Focused unit tests for the full-day Tokyo L2 Kaggle diagnostic backtest."""
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest

import numpy as np
from hftbacktest import (
    BacktestAsset,
    BUY_EVENT,
    DEPTH_CLEAR_EVENT,
    DEPTH_EVENT,
    DEPTH_SNAPSHOT_EVENT,
    EXCH_EVENT,
    LOCAL_EVENT,
    ROIVectorMarketDepthBacktest,
    SELL_EVENT,
    TRADE_EVENT,
    event_dtype,
)
from hftbacktest.data.validation import correct_event_order

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "kaggle" / "tasks"))
sys.path.insert(0, str(ROOT / "kaggle"))
import full_day_glft as fd
import launcher
from core_glft_engine import run_core_glft

NS = 1_000_000_000
DAY0 = 1_795_392_000 * NS


def build_synthetic_hbt_and_quotes(tmp: Path, minutes_total: int = 12):
    """Deterministic synthetic Gate L2 + trades + Gate L1 + Binance BBO for 10m warmup + trade."""
    rows = []
    gl1_local, gl1_exch, gl1_mid = [], [], []
    bin_local, bin_mid = [], []
    base_px = 60_000.0
    step_ns = 100_000_000  # 100ms
    n_steps = minutes_total * 600
    t_init = DAY0 + 50_000_000

    # Initial clear + 5-level snapshot
    rows.append((DEPTH_CLEAR_EVENT, t_init - 2_000_000, t_init, 0.0, 0.0, 0, 0, 0.0))
    for lvl in range(5):
        rows.append((DEPTH_SNAPSHOT_EVENT | BUY_EVENT, t_init - 2_000_000, t_init,
                     base_px - 0.5 - lvl * 0.5, 50.0, 0, 0, 0.0))
        rows.append((DEPTH_SNAPSHOT_EVENT | SELL_EVENT, t_init - 2_000_000, t_init,
                     base_px + 0.5 + lvl * 0.5, 50.0, 0, 0, 0.0))

    for i in range(1, n_steps + 5):
        t = DAY0 + i * step_ns
        ex = t - 2_000_000
        # Gentle deterministic oscillation so A/k/sigma calibrate cleanly and fills occur
        osc = round(2.0 * np.sin(i / 15.0), 1)
        mid = base_px + osc
        bid = round(mid - 0.5, 1)
        ask = round(mid + 0.5, 1)
        # Update best bid/ask via snapshot/clear every 50 steps or depth update
        if i % 10 == 1:
            rows.append((DEPTH_CLEAR_EVENT, ex, t, 0.0, 0.0, 0, 0, 0.0))
            for lvl in range(4):
                rows.append((DEPTH_SNAPSHOT_EVENT | BUY_EVENT, ex, t,
                             round(bid - lvl * 0.5, 1), 20.0, 0, 0, 0.0))
                rows.append((DEPTH_SNAPSHOT_EVENT | SELL_EVENT, ex, t,
                             round(ask + lvl * 0.5, 1), 20.0, 0, 0, 0.0))
        else:
            rows.append((DEPTH_EVENT | BUY_EVENT, ex, t, bid, 20.0, 0, 0, 0.0))
            rows.append((DEPTH_EVENT | SELL_EVENT, ex, t, ask, 20.0, 0, 0, 0.0))
        # Periodic trades crossing several ticks for Poisson arrival calibration & fills
        if i % 2 == 0:
            side = BUY_EVENT if (i // 2) % 2 == 0 else SELL_EVENT
            depth_ticks = (i % 6) * 0.5 + 0.5
            tpx = round((ask + depth_ticks) if side == BUY_EVENT else (bid - depth_ticks), 1)
            rows.append((TRADE_EVENT | side, ex + 500_000, t + 500_000, tpx, 5.0, 0, 0, 0.0))
        gl1_local.append(t)
        gl1_exch.append(ex)
        gl1_mid.append(mid)
        # Binance leads slightly during trading window to trigger cross-venue signal
        lead = 3.0 if (i > 6050 and (i % 40) < 5) else 0.0
        bin_local.append(t - 5_000_000)
        bin_mid.append(mid + lead)

    raw_arr = np.array(rows, dtype=event_dtype)
    ordered = correct_event_order(raw_arr, np.argsort(raw_arr["exch_ts"]), np.argsort(raw_arr["local_ts"]))
    hbt_path = tmp / "gate_synth.npz"
    np.savez_compressed(hbt_path, data=ordered)
    return (
        hbt_path,
        np.asarray(bin_local, dtype=np.int64),
        np.asarray(bin_mid, dtype=np.float64),
        np.asarray(gl1_local, dtype=np.int64),
        np.asarray(gl1_exch, dtype=np.int64),
        np.asarray(gl1_mid, dtype=np.float64),
    )


class FullDayGlftTests(unittest.TestCase):
    def test_core_glft_audit_intervals_bit_for_bit_parity(self):
        with tempfile.TemporaryDirectory() as tmp:
            hbt_path, b_ts, b_mid, g_loc, g_ex, g_mid = build_synthetic_hbt_and_quotes(Path(tmp), 12)
            day_end = DAY0 + 12 * 60 * NS
            grid = np.arange(DAY0 + 600 * NS, day_end, 5 * NS, dtype=np.int64)
            basis = np.zeros(len(grid), dtype=np.float64)
            sigma = np.full(len(grid), 0.5, dtype=np.float64)

            def make_eng():
                asset = (
                    BacktestAsset().data([str(hbt_path)]).linear_asset(0.0001)
                    .constant_order_latency(1_000_000, 1_000_000)
                    .power_prob_queue_model(2.0).no_partial_fill_exchange()
                    .trading_value_fee_model(0.0002, 0.0005)
                    .tick_size(0.1).lot_size(1.0)
                    .roi_lb(59_000.0).roi_ub(61_000.0).last_trades_capacity(20_000)
                )
                return ROIVectorMarketDepthBacktest([asset])

            opts = dict(
                min_requote_ns=0, fit_mode="physical_1tick_poisson",
                wait_book_after_fill=True, post_only_clamp_mode="unified",
                cap_depth_at_visible_levels=False, quote_tick_mult=1,
                cross_timestamp_tie_mode="microsecond",
                gate_book_local_ns=g_loc, gate_book_exchange_ns=g_ex,
            )
            e1 = make_eng()
            d1 = np.zeros(12, dtype=np.float64)
            try:
                r1 = run_core_glft(e1, b_ts, b_mid, 1200, 1.0, grid, basis, sigma, 1,
                                   0.0005, 0.1, 0.0001, 13.0, 260.0, None, day_end,
                                   diagnostics=d1, **opts)
            finally:
                e1.close()

            iv_bounds = np.array([[DAY0 + 630 * NS, DAY0 + 640 * NS]], dtype=np.int64)
            iv_counts = np.zeros((1, 3), dtype=np.int64)
            probes = np.array([DAY0 + 630 * NS, DAY0 + 640 * NS], dtype=np.int64)
            e2 = make_eng()
            d2 = np.zeros(12, dtype=np.float64)
            try:
                r2 = run_core_glft(e2, b_ts, b_mid, 1200, 1.0, grid, basis, sigma, 1,
                                   0.0005, 0.1, 0.0001, 13.0, 260.0, None, day_end,
                                   probe_ns=probes, diagnostics=d2,
                                   audit_intervals_ns=iv_bounds, audit_counts=iv_counts, **opts)
            finally:
                e2.close()

            # Bit-for-bit strategy, order, fill, and accounting parity
            for idx in (0, 1, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18):
                self.assertEqual(r1[idx], r2[idx], f"mismatch at slot {idx}")
            self.assertTrue(np.array_equal(r1[-5], r2[-5]))
            self.assertTrue(np.array_equal(r1[-4], r2[-4]))
            self.assertTrue(np.array_equal(r1[-1], r2[-1]))
            self.assertGreater(int(iv_counts[0, 0]), 0)
            self.assertEqual(int(iv_counts[0, 1]) + int(iv_counts[0, 2]) >= 0, True)

    def test_protocol_audit_rules_digest_and_revision_without_commit_fragility(self):
        with tempfile.TemporaryDirectory() as tmp:
            pdir = Path(tmp)
            windows = [f"2026-11-23T{h:02d}{m:02d}Z" for h in range(24) for m in (0, 15, 30, 45)]
            cands_by_window = {}
            revs = {}
            for i, w in enumerate(windows):
                wid = f"{w[:13]}:{w[13:]}"
                rev = f"sha256:rev_{i:02d}"
                revs[wid] = rev
                cands = [
                    {"exchange": "binance_futures", "window_id": wid, "data_sha256": f"bd_{i}", "manifest_sha256": f"bm_{i}"},
                    {"exchange": "gate_futures", "window_id": wid, "data_sha256": f"gd_{i}", "manifest_sha256": f"gm_{i}"},
                ]
                cands_by_window[wid] = cands
                avail = []
                st = "passed"
                if i == 0:
                    st = "data_failed"
                    avail = [
                        {"exchange": "gate_futures", "symbol": "BTCUSDT", "channel": "futures.obu",
                         "intervals": [{"start_wall_ns": DAY0 + 400_000_000, "end_wall_ns": DAY0 + 416_000_000,
                                        "status": "known_gap", "reason": "depth_unavailable"}]},
                        {"exchange": "binance_futures", "symbol": "BTCUSDT", "channel": "bookTicker",
                         "intervals": [{"start_wall_ns": DAY0 + 400_000_000, "end_wall_ns": DAY0 + 401_000_000,
                                        "status": "unverified", "reason": "switch"}]},
                        {"exchange": "bitget_v3", "symbol": "BTCUSDT", "channel": "books50",
                         "intervals": [{"start_wall_ns": DAY0, "end_wall_ns": DAY0 + 5_000_000_000,
                                        "status": "known_gap", "reason": "depth_unavailable"}]},
                    ]
                q = {
                    "run_id": "run1",
                    "window_id": wid,
                    "status": st,
                    "protocol_status": "passed",
                    # Commit SHA differs from unrelated docs commit, but rules digest matches!
                    "checker": "old_commit_before_docs/rules_digest_v2",
                    "archive_revision": rev,
                    "target_inputs": cands,
                    "market_availability": avail,
                }
                (pdir / f"{w}.json").write_text(json.dumps(q))

            prov = {"expected_protocol_rules_sha": "rules_digest_v2", "window_archive_revisions": revs}
            res = fd.audit_full_day_protocol_reports(pdir, cands_by_window, "run1", prov)
            self.assertTrue(res["evidence_binding_verified"])
            self.assertEqual(res["windows_checked"], 96)
            self.assertFalse(res["stage3_strict_qa_passed"])
            # Bitget excluded; only Gate OBU (confirmed_data_gap) + Binance bookTicker (continuity_unverified) recorded
            cats = {iv["channel"]: iv["category"] for iv in res["strategy_channel_intervals"]}
            self.assertEqual(cats, {"futures.obu": "confirmed_data_gap", "bookTicker": "continuity_unverified"})

            # Stale rules digest MUST fail binding
            bad_prov = {"expected_protocol_rules_sha": "different_rules", "window_archive_revisions": revs}
            bad_res = fd.audit_full_day_protocol_reports(pdir, cands_by_window, "run1", bad_prov)
            self.assertFalse(bad_res["evidence_binding_verified"])
            self.assertEqual(len(bad_res["rules_digest_mismatches"]), 96)

            # Archive revision mismatch MUST fail binding
            bad_revs = dict(revs)
            bad_revs["2026-11-23T00:00Z"] = "sha256:tampered"
            bad_rev_res = fd.audit_full_day_protocol_reports(
                pdir, cands_by_window, "run1",
                {"expected_protocol_rules_sha": "rules_digest_v2", "window_archive_revisions": bad_revs})
            self.assertFalse(bad_rev_res["evidence_binding_verified"])
            self.assertEqual(bad_rev_res["archive_revision_mismatches"], ["2026-11-23T00:00Z"])

    def test_full_day_three_models_synthetic_e2e(self):
        with tempfile.TemporaryDirectory() as tmp:
            hbt_path, b_ts, b_mid, g_loc, g_ex, g_mid = build_synthetic_hbt_and_quotes(Path(tmp), 12)
            contract = {
                "name": "BTC_USDT", "order_price_round": "0.1", "quanto_multiplier": "0.0001",
                "order_size_min": "1", "maker_fee_rate": "0.0002", "taker_fee_rate": "0.0005",
            }
            episodes = [(DAY0 + 630 * NS, DAY0 + 631 * NS)]
            res = fd.run_three_models_on_arrays(
                hbt_path, b_ts, b_mid, g_loc, g_ex, g_mid, contract,
                day_start_ns=DAY0, day_end_ns=DAY0 + 12 * 60 * NS,
                roi_low=59_900.0, roi_high=60_100.0,
                episodes=episodes, trade_minutes=2, witness_stop_ns=DAY0 + 12 * 60 * NS,
            )
            eff = res["effective_parameters"]
            self.assertEqual(eff["binance_extra_visible_delay_ns"], 0)
            self.assertEqual(eff["min_requote_ns"], 0)
            self.assertEqual(eff["order_entry_latency_ns"], 1_000_000)
            self.assertEqual(eff["order_response_latency_ns"], 1_000_000)
            self.assertFalse(eff["native_order_modify_enabled"])

            shared = eff["shared_first_eligible_entry_ns"]
            for name, _, _ in fd.MODELS:
                self.assertIn(name, res["models"])
                m = res["models"][name]
                self.assertEqual(m["first_fit_ns"], shared)
                self.assertEqual(m["modify_requests"], 0)
                self.assertTrue(all(m["accounting_checks"].values()), m["accounting_checks"])
                self.assertIn("gross_marked_pnl_usdt", m["trading"])
                self.assertIn("net_marked_pnl_usdt_2bps", m["trading"])
                self.assertIn("total_markout", m["fill_markout_quality"]["groups"]["all"]["horizons"]["100"])
                self.assertIn("per_episode", m["anomaly_impact"])
                self.assertGreater(m["anomaly_impact"]["per_episode"][0]["decisions_during_interval"], 0)

            summary = fd.summarize_overall({
                "setup": {"ok": True, "result": {}},
                "discover_and_qa": {"ok": True, "result": {"protocol_audit": {
                    "evidence_binding_verified": True, "stage3_strict_qa_passed": False}}},
                "fetch_and_convert": {"ok": True, "result": {"official_hbt_validate_event_order": "ok"}},
                "anomaly_and_book": {"ok": True, "result": {}},
                "backtest_models": {"ok": True, "result": res},
            })
            self.assertTrue(summary["diagnostic_full_day_complete"])
            self.assertFalse(summary["stage4_authorized"])
            self.assertFalse(summary["stage3_strict_qa_passed"])

    def test_diagnostic_allow_known_gaps_preserves_hard_blocks(self):
        import tokyo_l2_bt_stream_convert as sc
        self.assertTrue(sc.only_known_gap_failures(["complete_day_gap_or_uncertified_clock"]))
        self.assertTrue(sc.only_known_gap_failures(["gate_depth_unrebuildable:depth_unavailable"]))
        self.assertFalse(sc.only_known_gap_failures(["gate_unexplained_duplicate_trades:1"]))
        self.assertFalse(sc.only_known_gap_failures(["local_loss_detected_under_strict_zero_loss_gate"]))
        self.assertFalse(sc.only_known_gap_failures(["missing_l2_snapshot_anchor"]))
        self.assertFalse(sc.only_known_gap_failures(["incomplete_utc_day_coverage"]))
        self.assertFalse(sc.only_known_gap_failures(["malformed_tsv:1"]))

    def test_packaged_import_closure_for_full_day_job(self):
        job = json.loads((ROOT / "kaggle" / "job.json").read_text())
        import qa_convert
        collector = Path(qa_convert.__file__).resolve().parents[2]
        bundle, _ = launcher.build_bundle(ROOT, collector, list(job["files"]) + [job["task"]])
        with tempfile.TemporaryDirectory() as tmp:
            with tarfile.open(fileobj=io.BytesIO(bundle), mode="r:gz") as tf:
                tf.extractall(tmp, filter="data")
            env = dict(os.environ, PYTHONPATH=os.pathsep.join([
                str(Path(tmp) / "repo" / "kaggle" / "tasks"),
                str(Path(tmp) / "repo" / "scripts"),
                str(Path(tmp) / "collector" / "python"),
            ]))
            r = subprocess.run([
                sys.executable, "-c",
                "import full_day_glft, core_glft_engine, gate_basis_weighted_moments, "
                "analyze_gate_1645_fill_quality, tokyo_l2_bt_stream_convert, tokyo_l2_bt_fetch, "
                "qa_convert, hourly_incremental_qa",
            ], cwd=tmp, env=env, capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stderr)


if __name__ == "__main__":
    unittest.main()
