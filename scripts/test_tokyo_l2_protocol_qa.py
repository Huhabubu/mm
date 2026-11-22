from pathlib import Path
import tempfile
import unittest

import numpy as np

from tokyo_l2_replay_qa import probe_orders
from tokyo_l2_recurring_qa import plan_windows

from tokyo_l2_protocol_qa import BITGET, BYBIT, GATE, Book, Chain, Depth, depth_event, verify_gate_sizes


def depth(mode, seq, parent=None, epoch=1):
    return Depth("BTCUSDT", ("boot", "A", epoch), mode, seq, parent)


class ProtocolTest(unittest.TestCase):
    def test_recurring_checks_latest_and_one_oldest_pending_partition(self):
        self.assertEqual([400, 200], plan_windows([100, 200, 300, 400], {100: {}}, 400))
        self.assertEqual([200], plan_windows([100, 200, 300, 400], {100: {}, 400: {}}, 400))
        # Explicit target window in the middle of backlog is prioritized ahead of oldest pending window.
        self.assertEqual([300, 200], plan_windows([100, 200, 300, 400], {100: {}}, 300))
        # Schedule catch-up expands bounded backlog budget to drain multiple missed windows.
        self.assertEqual(
            [500, 100, 200, 300, 400],
            plan_windows([100, 200, 300, 400, 500], {}, 500, max_catchup=4),
        )

    def test_recurring_surfaces_per_market_protocol_failure_diagnostics_and_catchup_completion(self):
        import json
        from unittest.mock import patch
        import data_coverage as coverage
        import hourly_incremental_qa as archive
        import tokyo_l2_recurring_qa as recurring

        w0 = 1_795_276_800  # 2026-11-21T16:00Z (missed/concurrency-replaced historical window)
        w1 = w0 + 900       # 2026-11-21T16:15Z (latest window)
        run_id = "trial15_run"
        checker_sha = "abc123def456"
        checker = f"{checker_sha}/rules123"
        cfg = {"deployment": {"partition_minutes": 15}}

        def make_cand(start):
            wid = coverage.window_descriptor(start, 15)["window_id"]
            mf = f"raw/gate_futures/{wid[:10]}/gate_{start}.manifest.json"
            return {
                "exchange": "gate_futures",
                "source_manifest": mf,
                "source_data": mf.replace(".manifest.json", ".tsv.zst"),
                "manifest_sha256": f"{start:064x}",
                "data_sha256": f"{start + 1:064x}",
                "manifest_bytes": 100,
                "data_bytes": 200,
                "planned_start_s": start,
            }

        cands = [make_cand(w0), make_cand(w1)]
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp)
            for c in cands:
                wid = coverage.window_descriptor(c["planned_start_s"], 15)["window_id"]
                wpath = (
                    state / "qa_reports" / "windows" / wid[:10]
                    / f"{archive._safe_window_filename(wid)}.window.qa.json"
                )
                wpath.parent.mkdir(parents=True, exist_ok=True)
                wpath.write_text(
                    json.dumps(
                        {
                            "qa_status": "passed",
                            "run_id": run_id,
                            "partition_minutes": 15,
                            "checker_commit_sha": checker_sha,
                            "files": [c["source_manifest"]],
                        }
                    ),
                    encoding="utf-8",
                )

            audited_starts = []

            def fake_audit(_remote, _cfg, _rid, start, window, _replay_s, _warmup, **_kw):
                audited_starts.append(start)
                if start == w1:
                    return {
                        "status": "data_failed",
                        "run_id": run_id,
                        "window_id": window,
                        "protocol_status": "failed",
                        "target_inputs": recurring.input_digests([cands[1]]),
                        "protocol": {
                            "frames": [{"exchange": "gate_futures", "symbol": "ETHUSDT", "count": 42}],
                            "issues": [
                                {
                                    "exchange": "gate_futures",
                                    "symbol": "ETHUSDT",
                                    "issue": "missing_same_connection_anchor",
                                    "count": 12,
                                }
                            ],
                        },
                        "missing_anchor_sources": [
                            {"exchange": "gate_futures", "symbol": "ETHUSDT", "connection": "A", "epoch": 1}
                        ],
                        "boundary_events": [],
                        "archive_file_checks": [
                            {"exchange": "gate_futures", "qa_status": "passed", "problem_count": 0}
                        ],
                        "end_sequence_checkpoint": [],
                        "replay": {},
                    }
                return {
                    "status": "passed",
                    "run_id": run_id,
                    "window_id": window,
                    "protocol_status": "passed",
                    "target_inputs": recurring.input_digests([cands[0]]),
                    "protocol": {"frames": [], "issues": []},
                    "missing_anchor_sources": [],
                    "boundary_events": [],
                    "archive_file_checks": [
                        {"exchange": "gate_futures", "qa_status": "passed", "problem_count": 0}
                    ],
                    "end_sequence_checkpoint": [],
                    "replay": {},
                }

            with patch.object(recurring, "verified_candidates", return_value=cands), \
                 patch.object(recurring, "audit_window", side_effect=fake_audit):
                res = recurring.run(
                    "qa:", cfg, run_id, w0, w1, state, checker,
                    trigger_mode="schedule_catchup",
                )

            # Low-frequency catch-up verified BOTH latest W1 and missed historical W0!
            self.assertEqual([w1, w0], audited_starts)
            self.assertEqual("data_failed", res["status"])
            self.assertEqual("failed", res["protocol_status"])
            self.assertEqual(["gate_futures:ETHUSDT"], res["failed_markets"])
            self.assertEqual("missing_same_connection_anchor", res["protocol_issues"][0]["issue"])
            self.assertEqual(1, len(res["missing_anchor_sources"]))
            self.assertEqual(0, res["pending_ready_windows"])
            # Persisted protocol report for missed W0 includes completed_at_utc & archive_revision for outbox reconciliation!
            pdir = state / "qa_reports" / "protocol"
            saved_reports = list(pdir.rglob("*.json"))
            self.assertEqual(2, len(saved_reports))
            for sp in saved_reports:
                doc = json.loads(sp.read_text(encoding="utf-8"))
                self.assertIn("completed_at_utc", doc)
                self.assertTrue(str(doc.get("archive_revision") or "").startswith("sha256:"))

    def test_recurring_rejects_mismatched_archive_revision(self):
        from unittest.mock import patch
        import tokyo_l2_recurring_qa as recurring

        start = 1_795_276_800  # 2026-11-21T16:00Z
        cand = {
            "exchange": "binance_futures",
            "source_manifest": "raw/binance_futures/2026-11-21/f.manifest.json",
            "source_data": "raw/binance_futures/2026-11-21/f.tsv.zst",
            "manifest_sha256": "a" * 64,
            "data_sha256": "b" * 64,
            "manifest_bytes": 100,
            "data_bytes": 200,
            "planned_start_s": start,
        }
        cfg = {"deployment": {"partition_minutes": 15}}
        with tempfile.TemporaryDirectory() as temp, \
             patch.object(recurring, "verified_candidates", return_value=[cand]):
            res = recurring.run(
                "qa:", cfg, "trial15_run", start, start, Path(temp), "chk/rules",
                archive_revision="sha256:" + "0" * 64, trigger_mode="archive_dispatch",
            )
        self.assertEqual("revision_mismatch", res["status"])
        self.assertEqual("revision_mismatch", res["protocol_status"])

    def test_pinned_hft_orders_and_ledger(self):
        from hftbacktest import BUY_EVENT, DEPTH_EVENT, EXCH_EVENT, LOCAL_EVENT, SELL_EVENT, event_dtype

        bid = DEPTH_EVENT | BUY_EVENT | EXCH_EVENT | LOCAL_EVENT
        ask = DEPTH_EVENT | SELL_EVENT | EXCH_EVENT | LOCAL_EVENT
        start = 1_700_000_000_000_000_000
        rows = np.array([
            (bid, start, start, 300.0, 5.0, 0, 0, 0.0),
            (ask, start, start, 301.0, 5.0, 0, 0, 0.0),
            (bid, start + 5_000_000_000, start + 5_000_000_000, 300.0, 5.0, 0, 0, 0.0),
        ], dtype=event_dtype)
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "orders.npz"
            np.savez(path, data=rows)
            result = probe_orders(path, tick=1.0, lot=1.0, multiplier=1.0, order_qty=1.0)
        self.assertEqual("passed", result["status"], result)
        self.assertTrue(result["ledger_ok"])

    def test_gate_detects_lost_update_despite_increasing_sequence(self):
        chain = Chain()
        self.assertIsNone(chain.apply(GATE, depth("snapshot", 100)))
        self.assertIsNone(chain.apply(GATE, depth("delta", 103, 101)))
        self.assertEqual("exchange_update_chain_break", chain.apply(GATE, depth("delta", 107, 107)))

    def test_connection_change_needs_fresh_anchor(self):
        chain = Chain()
        chain.apply(GATE, depth("snapshot", 100))
        self.assertEqual("missing_same_connection_anchor", chain.apply(GATE, depth("delta", 101, 101, 2)))

    def test_bitget_full_snapshots_and_incremental_bridge(self):
        chain = Chain()
        chain.apply(BITGET, depth("snapshot", 55))
        chain.apply(BITGET, depth("snapshot", 60))
        self.assertIsNone(chain.apply(BITGET, depth("update", 65, 59)))
        self.assertIsNone(chain.apply(BITGET, depth("update", 66, 65)))
        self.assertEqual("exchange_update_chain_break", chain.apply(BITGET, depth("update", 68, 0)))

    def test_bybit_restart_resets_and_valid_jumps_do_not_count_as_gap(self):
        event = depth_event(BYBIT, "boot", "A", 1, "DATA",
                            {"topic": "orderbook.50.BTCUSDT", "type": "delta", "data": {"u": 1}})
        self.assertEqual(event.mode, "snapshot")
        chain = Chain()
        chain.apply(BYBIT, event)
        self.assertIsNone(chain.apply(BYBIT, depth("delta", 30)))

    def test_cross_is_checked_after_whole_message_and_gate_sizes_keep_decimals(self):
        event = depth("snapshot", 5)
        event.bids, event.asks = [["12", "1.5"]], [["12", "0.1"]]
        book = Book()
        book.apply(event)
        self.assertTrue(book.crossed())
        self.assertTrue(verify_gate_sizes(event))
        event = depth("delta", 6, 6)
        event.asks = [["12", "0"], ["13", "1"]]
        book.apply(event)
        self.assertFalse(book.crossed())

    def test_recurring_runs_historical_catchup_when_current_window_missing_or_failed_and_propagates_partial_checkpoint(self):
        import json
        from unittest.mock import patch
        import data_coverage as coverage
        import hourly_incremental_qa as archive
        import tokyo_l2_recurring_qa as recurring

        w0 = 1_795_276_800  # 2026-11-21T16:00Z (historical ready)
        w1 = w0 + 900       # 2026-11-21T16:15Z (historical ready, builds on w0 per-market checkpoint)
        w2 = w1 + 900       # 2026-11-21T16:30Z (latest current window: NOT archived / failed file QA!)
        run_id = "trial15_run"
        checker_sha = "abc123def456"
        checker = f"{checker_sha}/rules123"
        cfg = {
            "deployment": {"partition_minutes": 15},
            "exchanges": {
                "gate_futures": {
                    "enabled": True,
                    "symbols": ["BTC_USDT", "ETH_USDT"],
                    "channels": ["futures.obu"],
                }
            },
        }

        def make_cand(start):
            wid = coverage.window_descriptor(start, 15)["window_id"]
            mf = f"raw/gate_futures/{wid[:10]}/gate_{start}.manifest.json"
            return {
                "exchange": "gate_futures",
                "source_manifest": mf,
                "source_data": mf.replace(".manifest.json", ".tsv.zst"),
                "manifest_sha256": f"{start:064x}",
                "data_sha256": f"{start + 1:064x}",
                "manifest_bytes": 100,
                "data_bytes": 200,
                "planned_start_s": start,
            }

        cands = [make_cand(w0), make_cand(w1)]
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp)
            for c in cands:
                wid = coverage.window_descriptor(c["planned_start_s"], 15)["window_id"]
                wpath = (
                    state / "qa_reports" / "windows" / wid[:10]
                    / f"{archive._safe_window_filename(wid)}.window.qa.json"
                )
                wpath.parent.mkdir(parents=True, exist_ok=True)
                wpath.write_text(
                    json.dumps(
                        {
                            "qa_status": "passed",
                            "run_id": run_id,
                            "partition_minutes": 15,
                            "checker_commit_sha": checker_sha,
                            "files": [c["source_manifest"]],
                        }
                    ),
                    encoding="utf-8",
                )

            calls = []

            def fake_audit(_remote, _cfg, _rid, start, window, _replay_s, warmup, **kw):
                calls.append((start, warmup, kw.get("checkpoint")))
                if start == w0:
                    # One market (ETHUSDT) failed, but BTCUSDT checkpoint MUST survive for w1!
                    return {
                        "status": "data_failed",
                        "run_id": run_id,
                        "window_id": window,
                        "protocol_status": "failed",
                        "target_inputs": recurring.input_digests([cands[0]]),
                        "protocol": {
                            "frames": [],
                            "issues": [{"exchange": "gate_futures", "symbol": "ETHUSDT", "issue": "exchange_update_chain_break", "count": 1}],
                        },
                        "end_sequence_checkpoint": [
                            {"exchange": "gate_futures", "symbol": "BTCUSDT", "connection": ["boot", "B", 1], "seq": 102, "awaiting_delta": False}
                        ],
                        "replay": {},
                    }
                return {
                    "status": "passed",
                    "run_id": run_id,
                    "window_id": window,
                    "protocol_status": "passed",
                    "target_inputs": recurring.input_digests([cands[1]]),
                    "protocol": {"frames": [], "issues": []},
                    "end_sequence_checkpoint": [
                        {"exchange": "gate_futures", "symbol": "BTCUSDT", "connection": ["boot", "B", 1], "seq": 150, "awaiting_delta": False},
                        {"exchange": "gate_futures", "symbol": "ETHUSDT", "connection": ["boot", "B", 1], "seq": 320, "awaiting_delta": False},
                    ],
                    "replay": {},
                }

            with patch.object(recurring, "verified_candidates", return_value=cands), \
                 patch.object(recurring, "audit_window", side_effect=fake_audit):
                res = recurring.run(
                    "qa:", cfg, run_id, w0, w2, state, checker,
                    trigger_mode="schedule_catchup",
                )

            # Even though current window w2 is missing/unready, BOTH ready historical windows w0 & w1 were audited!
            self.assertFalse(res["current_window_ready"])
            self.assertEqual("pending", res["status"])
            self.assertEqual([w0, w1], [c[0] for c in calls])
            self.assertEqual(2, len(res["historical_checked_windows"]))
            # w1 received w0's partial per-market BTCUSDT checkpoint EVEN THOUGH w0 failed on ETHUSDT,
            # and also requested warmup_windows=1 so uncovered ETHUSDT could warm up!
            self.assertEqual(1, calls[1][1])
            self.assertEqual(1, len(calls[1][2]))
            self.assertEqual("BTCUSDT", calls[1][2][0]["symbol"])

    def test_scan_preserves_per_market_checkpoints_and_distinguishes_unanchored_lookback_vs_proven_break(self):
        import json
        import zstandard as zstd
        from unittest.mock import patch
        import hourly_incremental_qa as archive
        from tokyo_l2_replay_qa import NS, scan

        cfg = {
            "deployment": {"partition_minutes": 15},
            "exchanges": {
                "gate_futures": {
                    "enabled": True,
                    "symbols": ["BTC_USDT", "ETH_USDT"],
                    "channels": ["futures.obu"],
                }
            },
        }
        start_s = 1_795_276_800
        start_ns = start_s * NS
        end_ns = (start_s + 900) * NS

        def write_gate_zst(path: Path, lines: list[tuple[int, str, int, str, dict]]):
            with path.open("wb") as raw, zstd.open(raw, "wt", encoding="utf-8") as w:
                for idx, (wall, conn, epoch, kind, obj) in enumerate(lines, start=1):
                    w.write(f"{wall}\t{idx}\t{conn}\t{epoch}\t{idx}\t{kind}\t{json.dumps(obj)}\n")

        def gate_msg(sym: str, u: int, U: int | None = None, full: bool = False):
            res = {"s": f"ob.{sym}.50", "t": 1795276801000, "u": u, "b": [["100.0", "1.0"]], "a": [["101.0", "1.0"]]}
            if full:
                res["full"] = True
            else:
                res["U"] = U
            return {"channel": "futures.obu", "event": "update", "result": res}

        with tempfile.TemporaryDirectory() as temp, \
             patch.object(archive, "check_partition", return_value={"qa_status": "passed", "problems": []}):
            tmp = Path(temp)
            mf_base = {"boot_id": "boot1", "start_wall_ns": start_ns, "first_rx_seq": 1, "file_name": "gate.tsv.zst"}

            # 1) Window 1: BTCUSDT anchored & clean (100->101->102); ETHUSDT has proven sequence break (200->205, U=203).
            p1 = tmp / "w1.tsv.zst"
            write_gate_zst(p1, [
                (start_ns + 1, "B", 1, "DATA", gate_msg("BTC_USDT", 100, full=True)),
                (start_ns + 2, "B", 1, "DATA", gate_msg("BTC_USDT", 101, U=101)),
                (start_ns + 3, "B", 1, "DATA", gate_msg("BTC_USDT", 102, U=102)),
                (start_ns + 4, "B", 1, "DATA", gate_msg("ETH_USDT", 200, full=True)),
                (start_ns + 5, "B", 1, "DATA", gate_msg("ETH_USDT", 205, U=203)),
            ])
            cand1 = {"exchange": GATE, "planned_start_s": start_s}
            s1 = {}
            scan([(cand1, mf_base, p1)], cfg, start_ns, end_ns, start_ns, {}, s1)
            self.assertEqual("failed", s1["protocol_status"])
            self.assertEqual(1, len(s1["proven_protocol_issues"]))
            self.assertEqual("ETHUSDT", s1["proven_protocol_issues"][0]["symbol"])
            self.assertEqual("exchange_update_chain_break", s1["proven_protocol_issues"][0]["issue"])
            # BTCUSDT checkpoint is PRESERVED despite ETHUSDT failing!
            self.assertEqual(1, len(s1["end_sequence_checkpoint"]))
            self.assertEqual("BTCUSDT", s1["end_sequence_checkpoint"][0]["symbol"])
            self.assertEqual(102, s1["end_sequence_checkpoint"][0]["seq"])

            # 2) Window 2: mixed boundary checkpoint (BTCUSDT deltas only, no snapshot!) + warmup (w1 included) + ETHUSDT fresh snapshot!
            w2_start_s = start_s + 900
            w2_start_ns = w2_start_s * NS
            w2_end_ns = (w2_start_s + 900) * NS
            p2 = tmp / "w2.tsv.zst"
            write_gate_zst(p2, [
                (w2_start_ns + 1, "B", 1, "DATA", gate_msg("BTC_USDT", 103, U=103)),
                (w2_start_ns + 2, "B", 1, "DATA", gate_msg("BTC_USDT", 104, U=104)),
                (w2_start_ns + 3, "B", 1, "DATA", gate_msg("ETH_USDT", 300, full=True)),
                (w2_start_ns + 4, "B", 1, "DATA", gate_msg("ETH_USDT", 301, U=301)),
            ])
            cand2 = {"exchange": GATE, "planned_start_s": w2_start_s}
            mf2 = dict(mf_base, start_wall_ns=w2_start_ns, file_name="gate2.tsv.zst")
            s2 = {}
            scan(
                [(cand1, mf_base, p1), (cand2, mf2, p2)],
                cfg, w2_start_ns, w2_end_ns, w2_start_ns, {}, s2,
                checkpoint=s1["end_sequence_checkpoint"],
            )
            self.assertEqual("passed", s2["protocol_status"])
            self.assertEqual([], s2["proven_protocol_issues"])
            self.assertEqual([], s2["unanchored_lookback_markets"])
            self.assertEqual(2, len(s2["end_sequence_checkpoint"]))

            # 3) Pre-existing long-lived connection without snapshot in lookback:
            #    continuous deltas -> anchor_unverified (NOT proven chain break!)
            p3 = tmp / "w3_unanchored.tsv.zst"
            write_gate_zst(p3, [
                (start_ns + 1, "B", 1, "DATA", gate_msg("BTC_USDT", 500, U=500)),
                (start_ns + 2, "B", 1, "DATA", gate_msg("BTC_USDT", 501, U=501)),
                (start_ns + 3, "B", 1, "DATA", gate_msg("BTC_USDT", 502, U=502)),
                (start_ns + 4, "B", 1, "DATA", gate_msg("ETH_USDT", 600, full=True)),
                (start_ns + 5, "B", 1, "DATA", gate_msg("ETH_USDT", 601, U=601)),
            ])
            s3 = {}
            scan([(cand1, mf_base, p3)], cfg, start_ns, end_ns, start_ns, {}, s3)
            self.assertEqual("anchor_unverified", s3["protocol_status"])
            self.assertEqual([], s3["proven_protocol_issues"])
            self.assertEqual(1, len(s3["unanchored_lookback_markets"]))
            self.assertEqual("BTCUSDT", s3["unanchored_lookback_markets"][0]["symbol"])
            self.assertEqual(3, s3["unanchored_lookback_markets"][0]["delta_frames_continuity_checked"])
            self.assertEqual(0, s3["unanchored_lookback_markets"][0]["proven_chain_breaks"])
            # ETHUSDT was anchored and clean -> still saved in end_sequence_checkpoint; unanchored BTCUSDT is excluded!
            self.assertEqual(["ETHUSDT"], [c["symbol"] for c in s3["end_sequence_checkpoint"]])

            # 4) Unanchored pre-existing stream WITH a real delta sequence break -> PROVEN exchange_update_chain_break!
            p4 = tmp / "w4_unanchored_break.tsv.zst"
            write_gate_zst(p4, [
                (start_ns + 1, "B", 1, "DATA", gate_msg("BTC_USDT", 500, U=500)),
                (start_ns + 2, "B", 1, "DATA", gate_msg("BTC_USDT", 505, U=503)),  # gap: expected U=501!
                (start_ns + 3, "B", 1, "DATA", gate_msg("ETH_USDT", 600, full=True)),
            ])
            s4 = {}
            scan([(cand1, mf_base, p4)], cfg, start_ns, end_ns, start_ns, {}, s4)
            self.assertEqual("failed", s4["protocol_status"])
            self.assertEqual("exchange_update_chain_break", s4["proven_protocol_issues"][0]["issue"])
            self.assertEqual("BTCUSDT", s4["proven_protocol_issues"][0]["symbol"])

            # 5) Mid-stream reconnection (epoch 1 -> epoch 2) without fresh snapshot -> PROVEN missing_same_connection_anchor!
            p5 = tmp / "w5_reconn.tsv.zst"
            write_gate_zst(p5, [
                (start_ns + 1, "B", 1, "DATA", gate_msg("BTC_USDT", 500, U=500)),
                (start_ns + 2, "B", 2, "DATA", gate_msg("BTC_USDT", 501, U=501)),  # epoch changed without snapshot!
                (start_ns + 3, "B", 1, "DATA", gate_msg("ETH_USDT", 600, full=True)),
            ])
            s5 = {}
            scan([(cand1, mf_base, p5)], cfg, start_ns, end_ns, start_ns, {}, s5)
            self.assertEqual("failed", s5["protocol_status"])
            self.assertEqual("missing_same_connection_anchor", s5["proven_protocol_issues"][0]["issue"])
            self.assertEqual("BTCUSDT", s5["proven_protocol_issues"][0]["symbol"])

    def test_market_intervals_keep_other_streams_and_wait_for_verified_recovery(self):
        import json
        import zstandard as zstd
        from unittest.mock import patch
        import hourly_incremental_qa as archive
        import qa_convert as convert
        from tokyo_l2_replay_qa import NS, scan

        exchange = GATE
        start = 1_795_276_800
        base = start * NS
        config = {
            "deployment": {"partition_minutes": 15},
            "exchanges": {exchange: {
                "enabled": True, "symbols": ["BTC_USDT", "ETH_USDT", "SUI_USDT"],
                "channels": ["futures.obu", "futures.trades"],
            }},
        }

        def book(symbol, seq, snapshot=False):
            return {"channel": "futures.obu", "event": "update", "result": {
                "s": f"ob.{symbol}.50", "u": seq, "U": seq, "full": snapshot,
                "t": 1795276800000, "b": [["100", "1"]], "a": [["101", "1"]],
            }}

        manifest = {"boot_id": "boot", "start_wall_ns": base, "first_rx_seq": 1, "file_name": "gate.tsv.zst"}
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(archive, "check_partition", return_value={"qa_status": "failed", "problems": ["depth gap"]}):
            items = []
            for offset, records in enumerate([
                [(1, "A", "DATA", book("BTC_USDT", 100, True)), (2, "A", "DATA", book("ETH_USDT", 200, True))],
                [(1, "A", "DATA", book("BTC_USDT", 101)), (2, "A", "DATA", book("ETH_USDT", 201))],
                [
                    (11, "A", "DATA", book("BTC_USDT", 102)),
                    (12, "A", "DATA", book("ETH_USDT", 202)),
                    (13, "A", "DATA", book("SUI_USDT", 500)),
                    (14, "A", "DATA", {"channel": "futures.trades", "event": "update", "result": [{"contract": "BTC_USDT", "id": 1, "price": "100", "size": "1"}]}),
                    (20, "B", "META", {"event": "DEPTH_UNAVAILABLE", "symbol": "BTC_USDT",
                                      "unavailable_since_wall_ns": base + 1800 * NS + 19}),
                    (23, "B", "META", {"event": "DEPTH_RECOVERED", "symbol": "BTC_USDT"}),
                    (25, "B", "DATA", book("BTC_USDT", 300, True)),
                    (39, "B", "DATA", book("BTC_USDT", 301)),
                ],
            ]):
                path = Path(directory) / f"{offset}.tsv.zst"
                window = start + offset * 900
                with path.open("wb") as raw, zstd.open(raw, "wt", encoding="utf-8") as writer:
                    for i, (ns, conn, kind, obj) in enumerate(records, 1):
                        writer.write(f"{window * NS + ns}\t{i}\t{conn}\t1\t{i}\t{kind}\t{json.dumps(obj)}\n")
                items.append(({"exchange": exchange, "planned_start_s": window, "planned_end_s": window + 900},
                              manifest, path))
            target = base + 1800 * NS
            report = {}
            scan(items, config, target, target + 60, target, {}, report)
        panel = {
            (row["symbol"], row["channel"]): row["intervals"] for row in report["market_availability"]
        }
        self.assertEqual(
            [("unverified", 0, 11), ("verified_usable", 11, 19),
             ("known_gap", 19, 39), ("verified_usable", 39, 60)],
            [(i["status"], i["start_wall_ns"] - target, i["end_wall_ns"] - target)
             for i in panel[("BTCUSDT", "futures.obu")]],
        )
        self.assertEqual(["unverified", "verified_usable"], [i["status"] for i in panel[("ETHUSDT", "futures.obu")]])
        self.assertEqual(["unverified"], [i["status"] for i in panel[("SUIUSDT", "futures.obu")]])
        self.assertEqual(["unverified", "verified_usable"], [i["status"] for i in panel[("BTCUSDT", "futures.trades")]])
        self.assertEqual(
            [(14, 19), (39, 60)],
            [(i["start_wall_ns"] - target, i["end_wall_ns"] - target) for i in convert.common_usable_intervals(
                report["market_availability"], {(exchange, "BTCUSDT", "futures.obu"),
                                                (exchange, "BTCUSDT", "futures.trades")}, target, target + 60
            )],
        )
        self.assertIn("BTCUSDT", {row["symbol"] for row in report["end_sequence_checkpoint"]})

    def test_binance_market_switch_does_not_invalidate_public_depth(self):
        """Market trades failover is independent; public depth failover still needs an anchor."""
        import json
        import zstandard as zstd
        from unittest.mock import patch
        import hourly_incremental_qa as archive
        from tokyo_l2_replay_qa import NS, scan

        cfg = {
            "deployment": {"partition_minutes": 15},
            "exchanges": {"binance_futures": {
                "enabled": True, "symbols": ["BTCUSDT"],
                "public_channels": ["depth@100ms"], "market_channels": ["aggTrade"],
            }},
        }
        begin = 1_795_276_800 * NS
        snap = {
            "event": "REST_SNAPSHOT_ANCHOR", "symbol": "BTCUSDT",
            "snapshot": {"lastUpdateId": 100, "bids": [["100", "2"]], "asks": [["101", "3"]]},
        }
        delta = {
            "e": "depthUpdate", "s": "BTCUSDT", "U": 100, "u": 101, "pu": 100,
            "b": [["100", "4"]], "a": [], "T": 1795277700001,
        }

        def write(path, recs):
            with path.open("wb") as raw, zstd.open(raw, "wt", encoding="utf-8") as out:
                for idx, (wall, conn, epoch, kind, obj) in enumerate(recs, 1):
                    out.write(f"{wall}\t{idx}\t{conn}\t{epoch}\t{idx}\t{kind}\t{json.dumps(obj)}\n")

        with tempfile.TemporaryDirectory() as tmp, patch.object(
            archive, "check_partition", return_value={"qa_status": "passed", "problems": []},
        ):
            p0, p1 = Path(tmp) / "warmup.tsv.zst", Path(tmp) / "target.tsv.zst"
            write(p1, [(begin + 1, "A", 1, "DATA", delta)])
            m0 = {"boot_id": "boot", "start_wall_ns": begin - 900 * NS,
                  "first_rx_seq": 1, "file_name": "warmup.tsv.zst"}
            m1 = dict(m0, start_wall_ns=begin, file_name="target.tsv.zst")
            cases = (
                ("binance_market", "SWITCH", "passed"),
                ("binance_market", "GAP_START", "passed"),
                ("binance_market", "LOCAL_LOSS", "passed"),
                ("binance_public", "SWITCH", "failed"),
                ("binance_public", "DEPTH_UNAVAILABLE", "failed"),
                ("binance_market", "DEPTH_UNAVAILABLE", "failed"),
                (None, "SWITCH", "failed"),
            )
            for endpoint, marker, expected in cases:
                with self.subTest(endpoint=endpoint, event=marker):
                    write(p0, [
                        (begin - 3, "A", 1, "META", snap),
                        (begin - 2, "B", 1, "META", {"event": marker, "endpoint": endpoint, "symbol": "ALL"}),
                    ])
                    report = {}
                    scan([
                        ({"exchange": "binance_futures", "planned_start_s": begin // NS - 900}, m0, p0),
                        ({"exchange": "binance_futures", "planned_start_s": begin // NS}, m1, p1),
                    ], cfg, begin, begin + 900 * NS, begin, {}, report)
                    self.assertEqual(expected, report["protocol_status"])
                    if expected == "passed":
                        proof, = report["end_sequence_checkpoint"]
                        self.assertEqual(["boot", "A", 1], proof["connection"])
                        self.assertEqual(101, proof["seq"])
                    else:
                        self.assertEqual("missing_same_connection_anchor",
                                         report["proven_protocol_issues"][0]["issue"])

    def test_anchor_unverified_window_does_not_starve_subsequent_windows_and_wakes_on_predecessor_checkpoint(self):
        """Review Round 4 P1:
        Reproduce 3+ consecutive catch-up runs when the earliest ready window W0 returns
        anchor_unverified: W0 must be deferred while its predecessor checkpoint is unchanged so
        subsequent ready windows W1, W2 advance every run instead of starving; and as soon as a
        predecessor produces a new checkpoint, the deferred window wakes up and promotes to passed.
        """
        import json
        from unittest.mock import patch
        import data_coverage as coverage
        import hourly_incremental_qa as archive
        import tokyo_l2_recurring_qa as recurring

        w0 = 1_795_276_800  # 16:00Z (anchor_unverified, no predecessor)
        w1 = w0 + 900       # 16:15Z (passes & writes full checkpoint)
        w2 = w1 + 900       # 16:30Z (passes using w1 checkpoint)
        w3 = w2 + 900       # 16:45Z (current: initially anchor_unverified, then wakes up once w2 passes!)
        run_id = "trial15_run"
        checker_sha = "abc123def456"
        checker = f"{checker_sha}/rules123"
        cfg = {
            "deployment": {"partition_minutes": 15},
            "exchanges": {
                "gate_futures": {
                    "enabled": True,
                    "symbols": ["BTC_USDT"],
                    "channels": ["futures.obu"],
                }
            },
        }

        def make_cand(start):
            wid = coverage.window_descriptor(start, 15)["window_id"]
            mf = f"raw/gate_futures/{wid[:10]}/gate_{start}.manifest.json"
            return {
                "exchange": "gate_futures",
                "source_manifest": mf,
                "source_data": mf.replace(".manifest.json", ".tsv.zst"),
                "manifest_sha256": f"{start:064x}",
                "data_sha256": f"{start + 1:064x}",
                "manifest_bytes": 100,
                "data_bytes": 200,
                "planned_start_s": start,
            }

        cands = [make_cand(w0), make_cand(w1), make_cand(w2), make_cand(w3)]
        cands_by_start = {c["planned_start_s"]: [c] for c in cands}

        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp)
            for c in cands:
                wid = coverage.window_descriptor(c["planned_start_s"], 15)["window_id"]
                wpath = (
                    state / "qa_reports" / "windows" / wid[:10]
                    / f"{archive._safe_window_filename(wid)}.window.qa.json"
                )
                wpath.parent.mkdir(parents=True, exist_ok=True)
                wpath.write_text(
                    json.dumps(
                        {
                            "qa_status": "passed",
                            "run_id": run_id,
                            "partition_minutes": 15,
                            "checker_commit_sha": checker_sha,
                            "files": [c["source_manifest"]],
                        }
                    ),
                    encoding="utf-8",
                )

            round_calls: list[list[int]] = []

            def fake_audit(_remote, _cfg, _rid, start, window, _replay_s, _warmup, **kw):
                round_calls[-1].append(start)
                ckpt = kw.get("checkpoint") or []
                if start == w0:
                    return {
                        "status": "anchor_unverified",
                        "run_id": run_id,
                        "window_id": window,
                        "protocol_status": "anchor_unverified",
                        "target_inputs": recurring.input_digests(cands_by_start[start]),
                        "protocol": {"frames": [], "issues": []},
                        "unanchored_lookback_markets": [{"exchange": "gate_futures", "symbol": "BTCUSDT"}],
                        "end_sequence_checkpoint": [],
                        "replay": {},
                    }
                if start == w3 and not ckpt:
                    return {
                        "status": "anchor_unverified",
                        "run_id": run_id,
                        "window_id": window,
                        "protocol_status": "anchor_unverified",
                        "target_inputs": recurring.input_digests(cands_by_start[start]),
                        "protocol": {"frames": [], "issues": []},
                        "unanchored_lookback_markets": [{"exchange": "gate_futures", "symbol": "BTCUSDT"}],
                        "end_sequence_checkpoint": [],
                        "replay": {},
                    }
                return {
                    "status": "passed",
                    "run_id": run_id,
                    "window_id": window,
                    "protocol_status": "passed",
                    "target_inputs": recurring.input_digests(cands_by_start[start]),
                    "protocol": {"frames": [], "issues": []},
                    "unanchored_lookback_markets": [],
                    "end_sequence_checkpoint": [
                        {"exchange": "gate_futures", "symbol": "BTCUSDT", "connection": ["boot", "B", 1], "seq": start, "awaiting_delta": False}
                    ],
                    "replay": {},
                }

            with patch.object(recurring, "verified_candidates", return_value=cands), \
                 patch.object(recurring, "audit_window", side_effect=fake_audit):
                # Round 1 (max_catchup=1): checks current W3 (anchor_unverified) and earliest W0 (anchor_unverified).
                round_calls.append([])
                r1 = recurring.run("qa:", cfg, run_id, w0, w3, state, checker, max_catchup=1)
                self.assertEqual([w3, w0], round_calls[-1])
                self.assertEqual("anchor_unverified", r1["status"])

                # Round 2: W0 & W3 are deferred -> MUST advance to W1 instead of re-picking W0!
                round_calls.append([])
                r2 = recurring.run("qa:", cfg, run_id, w0, w3, state, checker, max_catchup=1)
                self.assertEqual([w1], round_calls[-1])
                self.assertIn(coverage.window_descriptor(w1, 15)["window_id"], r2["historical_passed_windows"])
                self.assertEqual("anchor_unverified", r2["status"])

                # Round 3: W0 still deferred, W1 passed -> MUST advance to W2!
                round_calls.append([])
                r3 = recurring.run("qa:", cfg, run_id, w0, w3, state, checker, max_catchup=1)
                self.assertEqual([w2], round_calls[-1])
                self.assertIn(coverage.window_descriptor(w2, 15)["window_id"], r3["historical_passed_windows"])

                # Round 4: W2 now produced a new checkpoint -> W3 AUTOMATICALLY wakes up and promotes to passed!
                round_calls.append([])
                r4 = recurring.run("qa:", cfg, run_id, w0, w3, state, checker, max_catchup=1)
                self.assertEqual([w3], round_calls[-1])
                self.assertEqual("passed", r4["status"])
                self.assertEqual("passed", r4["protocol_status"])
                self.assertEqual(0, r4["pending_ready_windows"])
                self.assertEqual([coverage.window_descriptor(w0, 15)["window_id"]], r4["deferred_anchor_unverified_windows"])


    def test_recurring_revokes_downstream_reports_when_predecessor_checkpoint_changes(self):
        import json
        from unittest.mock import patch
        import data_coverage as coverage
        import hourly_incremental_qa as archive
        import tokyo_l2_recurring_qa as recurring

        w0 = 1_795_276_800
        w1 = w0 + 900
        w2 = w1 + 900
        run_id = "checkpoint_revision_test"
        checker_sha = "abc123def456"
        checker = f"{checker_sha}/rules"
        config = {"deployment": {"partition_minutes": 15}}
        candidates = {}
        for start in (w0, w1, w2):
            manifest = f"raw/gate_futures/gate_{start}.manifest.json"
            candidates[start] = {
                "exchange": "gate_futures",
                "source_manifest": manifest,
                "source_data": manifest.replace(".manifest.json", ".tsv.zst"),
                "manifest_sha256": f"{start:064x}",
                "data_sha256": f"{start + 1:064x}",
                "manifest_bytes": 100,
                "data_bytes": 200,
                "planned_start_s": start,
            }

        calls = []
        revised = False

        def audit(_remote, _config, batch, start, window, _seconds, _warmup, *, candidates, checkpoint, anchor_lookback_windows):
            calls.append((start, checkpoint))
            if revised and start == w0:
                status, protocol_status = "data_failed", "failed"
            elif not checkpoint and (start == w2 or (revised and start == w1)):
                status = protocol_status = "anchor_unverified"
            else:
                status = protocol_status = "passed"
            return {
                "status": status,
                "protocol_status": protocol_status,
                "run_id": batch,
                "window_id": window,
                "target_inputs": recurring.input_digests(
                    [item for item in candidates if item["planned_start_s"] == start]
                ),
                "end_sequence_checkpoint": [
                    {"exchange": "gate_futures", "symbol": "BTCUSDT",
                     "connection": ["boot", "A", 1], "seq": start, "awaiting_delta": False}
                ] if status == "passed" else [],
            }

        def window_id(start):
            return coverage.window_descriptor(start, 15)["window_id"]

        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp)
            for start, item in candidates.items():
                window = window_id(start)
                path = (
                    state / "qa_reports" / "windows" / window[:10]
                    / f"{archive._safe_window_filename(window)}.window.qa.json"
                )
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps({
                    "qa_status": "passed",
                    "run_id": run_id,
                    "partition_minutes": 15,
                    "checker_commit_sha": checker_sha,
                    "files": [item["source_manifest"]],
                }), encoding="utf-8")

            def run(current, max_catchup=0):
                return recurring.run(
                    "qa:", config, run_id, w0, current, state, checker, max_catchup=max_catchup
                )

            def saved(start):
                name = f"{archive._safe_window_filename(window_id(start))}.json"
                path = next((state / "qa_reports" / "protocol").rglob(name))
                return json.loads(path.read_text(encoding="utf-8"))

            with patch.object(recurring, "verified_candidates", return_value=list(candidates.values())), \
                 patch.object(recurring, "audit_window", side_effect=audit):
                # W1 can independently anchor, so W2 can pass before W0 has been audited.
                self.assertEqual("passed", run(w1)["status"])
                self.assertEqual("passed", run(w2)["status"])
                self.assertTrue(run(w2)["reused_current_result"])

                # When catch-up adds W0's checkpoint, invalidate W1 and W2 in the same run.
                calls.clear()
                caught_up = run(w2, max_catchup=1)
                self.assertEqual([(w0, None)], calls)
                self.assertEqual("pending", caught_up["status"])
                self.assertFalse(caught_up["reused_current_result"])
                for start in (w1, w2):
                    proof = saved(start)
                    self.assertEqual("pending", proof["status"])
                    self.assertNotIn("end_sequence_checkpoint", proof)

                # Recheck against the new chain and verify unchanged results can still be reused.
                self.assertEqual("passed", run(w1)["status"])
                self.assertEqual("passed", run(w2)["status"])
                self.assertTrue(run(w2)["reused_current_result"])

                # Revision affects W0 alone. W2's inputs still match, but its transitive checkpoint does not.
                revised = True
                candidates[w0]["manifest_sha256"] = "f" * 64
                calls.clear()
                result = run(w2, max_catchup=2)
                self.assertEqual([(w2, None), (w0, None), (w1, None)], calls)
                self.assertFalse(result["reused_current_result"])
                self.assertEqual("anchor_unverified", result["status"])
                self.assertEqual([window_id(w0)], result["historical_failed_windows"])
                self.assertEqual([window_id(w1)], result["historical_anchor_unverified_windows"])
                self.assertEqual("anchor_unverified", saved(w2)["status"])


if __name__ == "__main__":
    unittest.main()
