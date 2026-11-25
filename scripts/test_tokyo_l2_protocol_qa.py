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
                            "qa_status": "failed" if c is cands[0] else "passed",
                            "archive_complete_in_index": True,
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

    def test_bitget_full_book_stream_recovers_on_newer_same_connection_snapshot(self):
        import json
        import zstandard as zstd
        from unittest.mock import patch
        import hourly_incremental_qa as archive
        from tokyo_l2_replay_qa import NS, scan

        start = 1_795_276_800
        wall = start * NS
        cfg = {
            "deployment": {"partition_minutes": 15},
            "exchanges": {BITGET: {"enabled": True, "symbols": ["BTCUSDT"], "channels": ["books50"]}},
        }

        def full_book(seq):
            return {"arg": {"topic": "books50", "symbol": "BTCUSDT"}, "action": "snapshot",
                    "data": [{"seq": seq, "bids": [["100", "1"]], "asks": [["101", "1"]]}]}

        records = [
            (1, "A", "DATA", full_book(10)),
            (2, "B", "META", {"event": "DEPTH_UNAVAILABLE", "symbol": "BTCUSDT"}),
            (3, "B", "DATA", full_book(20)),
            (4, "B", "DATA", full_book(20)),
            (5, "B", "DATA", full_book(21)),
        ]
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(archive, "check_partition", return_value={"qa_status": "failed", "problems": ["depth gap"]}):
            path = Path(directory) / "bitget.tsv.zst"
            with path.open("wb") as raw, zstd.open(raw, "wt", encoding="utf-8") as writer:
                for stamp, conn, kind, obj in records:
                    writer.write(f"{wall + stamp}\t{stamp}\t{conn}\t1\t{stamp}\t{kind}\t{json.dumps(obj)}\n")
            report = {}
            scan(
                [({"exchange": BITGET, "planned_start_s": start},
                  {"boot_id": "boot", "file_name": path.name}, path)],
                cfg, wall, wall + 10, wall, {}, report,
            )
        spans = report["market_availability"][0]["intervals"]
        self.assertEqual(
            [("unverified", 0, 1), ("verified_usable", 1, 2),
             ("known_gap", 2, 5), ("verified_usable", 5, 10)],
            [(iv["status"], iv["start_wall_ns"] - wall, iv["end_wall_ns"] - wall) for iv in spans],
        )
        self.assertEqual(21, report["end_sequence_checkpoint"][0]["seq"])
        self.assertTrue(report["end_sequence_checkpoint"][0]["awaiting_delta"])

    def _bitget_trade_scan(self, records):
        import json
        import zstandard as zstd
        from unittest.mock import patch
        import hourly_incremental_qa as archive
        from tokyo_l2_replay_qa import NS, scan

        start = 1_795_276_800
        wall = start * NS
        cfg = {
            "deployment": {"partition_minutes": 15},
            "exchanges": {BITGET: {"enabled": True, "symbols": ["BTCUSDT"], "channels": ["books50", "publicTrade"]}},
        }
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(archive, "check_partition", return_value={"qa_status": "passed", "problems": []}):
            path = Path(directory) / "bitget.tsv.zst"
            with path.open("wb") as raw, zstd.open(raw, "wt", encoding="utf-8") as writer:
                for seq, (offset_ns, conn, kind, obj) in enumerate(records, 1):
                    writer.write(f"{wall + offset_ns}\t{seq}\t{conn}\t1\t{seq}\t{kind}\t{json.dumps(obj)}\n")
            report = {}
            scan(
                [({"exchange": BITGET, "planned_start_s": start},
                  {"boot_id": "boot", "file_name": path.name}, path)],
                cfg, wall, wall + 20 * NS, wall, {}, report,
            )
        issues = {(i["symbol"], i["issue"]): i["count"] for i in report["protocol"]["issues"]}
        overlap = {(o["exchange"], o["symbol"]): o["count"] for o in report["connection_overlap_trades"]}
        return issues, overlap

    @staticmethod
    def _bitget_trade(tid, price="100"):
        return {"arg": {"topic": "publicTrade", "symbol": "BTCUSDT"}, "action": "update",
                "data": [{"i": tid, "p": price, "v": "1", "S": "buy", "T": 1795276800000}]}

    def test_cross_connection_handover_duplicate_is_reported_as_overlap_not_issue(self):
        from tokyo_l2_replay_qa import NS

        issues, overlap = self._bitget_trade_scan([
            (1, "B", "DATA", self._bitget_trade("7")),
            (NS, "B", "DATA", self._bitget_trade("8")),
            (NS + 2, "A", "META", {"event": "SWITCH", "endpoint": "bitget_public"}),
            (NS + 9, "A", "DATA", self._bitget_trade("8")),
            (NS + 20, "A", "DATA", self._bitget_trade("9")),
        ])
        self.assertNotIn(("BTCUSDT", "duplicate_trade_id"), issues)
        self.assertNotIn(("BTCUSDT", "conflicting_duplicate_trade"), issues)
        self.assertEqual({(BITGET, "BTCUSDT"): 1}, overlap)

    def test_same_connection_late_and_conflicting_duplicates_stay_proven_issues(self):
        from tokyo_l2_replay_qa import NS

        issues, overlap = self._bitget_trade_scan([
            (1, "B", "DATA", self._bitget_trade("1")),
            (2, "B", "DATA", self._bitget_trade("1")),
            (3, "B", "DATA", self._bitget_trade("2")),
            (4, "A", "META", {"event": "SWITCH", "endpoint": "bitget_public"}),
            (5, "A", "DATA", self._bitget_trade("2", price="101")),
            (6, "A", "DATA", self._bitget_trade("3")),
            (12 * NS, "A", "DATA", self._bitget_trade("4")),
            (13 * NS, "C", "DATA", self._bitget_trade("5")),
            (13 * NS + 1, "C", "DATA", self._bitget_trade("4")),
        ])
        self.assertEqual(2, issues[("BTCUSDT", "duplicate_trade_id")])
        self.assertEqual(1, issues[("BTCUSDT", "conflicting_duplicate_trade")])
        self.assertEqual({}, overlap)

    def test_bitget_px_sz_conflict_and_refailover_segment_overlap(self):
        from tokyo_l2_replay_qa import NS

        px_trade = lambda tid, px: {
            "arg": {"topic": "publicTrade", "symbol": "BTCUSDT"}, "action": "update",
            "data": [{"tradeId": tid, "px": px, "sz": "1", "side": "buy", "ts": 1795276800000}],
        }
        issues, overlap = self._bitget_trade_scan([
            (1, "B", "DATA", px_trade("10", "100")),
            (2, "A", "META", {"event": "SWITCH", "endpoint": "bitget_public"}),
            (3, "A", "DATA", px_trade("10", "101")),  # px differs -> conflicting_duplicate_trade!
            (4, "A", "DATA", px_trade("11", "100")),
            (10 * NS, "B", "META", {"event": "SWITCH", "endpoint": "bitget_public"}),
            (10 * NS + 5, "B", "DATA", px_trade("11", "100")),  # A->B second switch within 5s of B segment start
        ])
        self.assertEqual(1, issues[("BTCUSDT", "conflicting_duplicate_trade")])
        self.assertNotIn(("BTCUSDT", "duplicate_trade_id"), issues)
        self.assertEqual({(BITGET, "BTCUSDT"): 1}, overlap)

    def test_replay_converter_receives_btc_trades_once_per_trade_id(self):
        import json
        import zstandard as zstd
        from unittest.mock import patch
        import hourly_incremental_qa as archive
        import qa_convert as convert
        from tokyo_l2_replay_qa import BTC, NS, follower_trade, scan

        buffer = convert.AssetDayBuffers(BITGET, convert.canonical_asset(BITGET, BTC), "2026-11-24")
        follower_trade(BITGET, self._bitget_trade("42"), 10, buffer)
        follower_trade(BITGET, self._bitget_trade("42"), 20, buffer)
        follower_trade(BITGET, self._bitget_trade("43"), 30, buffer)
        trades = [row for row in buffer.hbt_rows if int(row[0]) & 0xFF == convert.TRADE_EVENT]
        self.assertEqual([10, 30], [int(row[2]) for row in trades])
        self.assertEqual(1, buffer.dedup_trades_dropped)

        # Mixed-symbol Gate batch [BTC_USDT, ETH_USDT] must NOT drop BTC_USDT trades when last item is ETH_USDT,
        # and cross-connection duplicate trade across SWITCH must feed converter only the earliest received copy.
        start = 1_795_276_800
        wall = start * NS
        cfg = {
            "deployment": {"partition_minutes": 15},
            "exchanges": {GATE: {"enabled": True, "symbols": ["BTC_USDT", "ETH_USDT"],
                                 "channels": ["futures.obu.50", "futures.trades"]}},
        }
        mixed = {"channel": "futures.trades", "event": "update", "result": [
            {"id": 100, "contract": "BTC_USDT", "price": "90000", "size": "1", "create_time_ms": start * 1000},
            {"id": 200, "contract": "ETH_USDT", "price": "3000", "size": "1", "create_time_ms": start * 1000},
        ]}
        dup_btc = {"channel": "futures.trades", "event": "update", "result": [
            {"id": 100, "contract": "BTC_USDT", "price": "90000", "size": "1", "create_time_ms": start * 1000},
        ]}
        with tempfile.TemporaryDirectory() as directory,              patch.object(archive, "check_partition", return_value={"qa_status": "passed", "problems": []}):
            path = Path(directory) / "gate.tsv.zst"
            with path.open("wb") as raw, zstd.open(raw, "wt", encoding="utf-8") as writer:
                writer.write(f"{wall + 10}	1	A	1	1	DATA	{json.dumps(mixed)}\n")
                writer.write(f"{wall + 20}	2	B	1	2	META	{json.dumps({'event': 'SWITCH'})}\n")
                writer.write(f"{wall + 30}	3	B	1	3	DATA	{json.dumps(dup_btc)}\n")
            report = {}
            scan(
                [({"exchange": GATE, "planned_start_s": start}, {"boot_id": "boot", "file_name": path.name}, path)],
                cfg, wall, wall + 20 * NS, wall + 10 * NS, {GATE: {"tick": "0.1", "lot": "1", "multiplier": 1, "order_qty": "1", "lot_source": "step"}}, report,
            )
        gate_rows = next(r for r in report["replay_trade_rows"] if r["exchange"] == GATE)
        self.assertEqual(1, gate_rows["trade_rows"])
        self.assertEqual(1, gate_rows["dedup_trades_dropped"])

    def test_stream_evidence_interrupt_normalizes_gate_underscore_symbol(self):
        from tokyo_l2_replay_qa import StreamEvidence

        cfg = {"exchanges": {GATE: {"enabled": True, "symbols": ["BTC_USDT", "ETH_USDT"],
                                    "channels": ["futures.obu.50", "futures.trades"]}}}
        ev = StreamEvidence(cfg)
        ev.observe((GATE, "ETHUSDT", "futures.trades"), 10)
        ev.interrupt(GATE, 20, "INVALID_STREAM_FRAME", "ETH_USDT", channel="futures.trades")
        self.assertEqual("unverified", ev.timeline.state((GATE, "ETHUSDT", "futures.trades")))

    def test_range_starts_with_verified_predecessor_then_carries_checkpoint(self):
        from unittest.mock import patch
        import tokyo_l2_replay_qa as replay

        first = 1_795_276_800
        start_records = [{"planned_start_s": first - 900}, {"planned_start_s": first}, {"planned_start_s": first + 900}]
        seq = [{"exchange": GATE, "symbol": "BTCUSDT", "seq": 123}]
        report = {"status": "passed", "window_id": "test", "end_sequence_checkpoint": seq}
        cfg = {"deployment": {"partition_minutes": 15}}
        with patch.object(replay, "verified_candidates", return_value=start_records) as discover, \
             patch.object(replay, "audit_window", return_value=report) as audit:
            replay.audit_range("qa:", cfg, "batch", first, first + 900)
        discover.assert_called_once_with("qa:", cfg, "batch", first - 900, historical=True)
        self.assertEqual(1, audit.call_args_list[0].args[6])
        self.assertEqual(start_records[:2], audit.call_args_list[0].kwargs["candidates"])
        self.assertEqual(start_records[2:], audit.call_args_list[1].kwargs["candidates"])
        self.assertEqual(seq, audit.call_args_list[1].kwargs["checkpoint"])

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

    def test_replay_precondition_separates_missing_signal_from_uninitialised_book(self):
        from tokyo_l2_replay_qa import replay_precondition_reason

        self.assertEqual("missing_signal", replay_precondition_reason(1, True, True))
        self.assertEqual("missing_signal", replay_precondition_reason(0, False, False))
        self.assertEqual("initial_book_not_in_lookback", replay_precondition_reason(5, False, True))
        self.assertEqual("initial_book_not_in_lookback", replay_precondition_reason(5, False, False))
        self.assertEqual("missing_target_book_rows", replay_precondition_reason(5, True, False))
        self.assertIsNone(replay_precondition_reason(5, True, True))

    def test_window_status_reports_replay_init_failure_without_passing(self):
        from tokyo_l2_replay_qa import window_status

        ok = {"status": "passed"}
        init = {"status": "blocked", "reason": "initial_book_not_in_lookback"}
        unanchored = {"status": "blocked", "reason": "anchor_not_in_lookback"}
        no_signal = {"status": "blocked", "reason": "missing_signal"}
        engine = {"status": "failed"}
        self.assertEqual("passed", window_status("passed", True, [ok, ok]))
        self.assertEqual("replay_init_unverified", window_status("passed", True, [ok, init]))
        self.assertEqual("replay_init_unverified", window_status("passed", True, [init, unanchored]))
        self.assertEqual("data_failed", window_status("passed", True, [init, no_signal]))
        self.assertEqual("data_failed", window_status("passed", True, [init, engine]))
        self.assertEqual("data_failed", window_status("passed", False, [ok, init]))
        self.assertEqual("data_failed", window_status("failed", True, [ok, init]))
        self.assertEqual("anchor_unverified", window_status("anchor_unverified", True, [ok, unanchored]))
        self.assertEqual("data_failed", window_status("anchor_unverified", True, [ok, init]))


    def test_cross_partition_pure_delta_book_inheritance_and_hourly_replay(self):
        import io
        import json
        from unittest.mock import patch
        import zstandard as zstd
        import data_coverage as coverage
        import tokyo_l2_protocol_qa as protocol
        import tokyo_l2_replay_qa as replay

        w0 = 1_795_276_800 - 1800  # 15:30Z (initial snapshot)
        w1 = w0 + 900               # 15:45Z (pure deltas only)
        w2 = w1 + 900               # 16:00Z (hourly replay, pure deltas only!)
        step_ns = 900 * 1_000_000_000

        def write_zst(path, rows_list):
            buf = io.StringIO()
            for r in rows_list:
                buf.write("\t".join(map(str, r)) + "\n")
            cctx = zstd.ZstdCompressor()
            path.write_bytes(cctx.compress(buf.getvalue().encode("utf-8")))

        def gate_depth(wall_ns, ms, u, prev_u, bids, asks, full=False):
            payload = {
                "channel": "futures.obu",
                "event": "update",
                "result": {
                    "s": "ob.BTC_USDT.50",
                    "u": u,
                    "U": prev_u,
                    "t": ms,
                    "b": bids,
                    "a": asks,
                },
            }
            if full:
                payload["result"]["full"] = True
                del payload["result"]["U"]
            return (wall_ns, wall_ns, "c1", 1, u, "DATA", json.dumps(payload))

        def gate_trade(wall_ns, ms, tid, px="50000.5", sz="1"):
            payload = {
                "channel": "futures.trades",
                "event": "update",
                "result": [
                    {
                        "id": tid,
                        "contract": "BTC_USDT",
                        "price": px,
                        "size": sz,
                        "create_time_ms": ms,
                        "ts": ms,
                    }
                ],
            }
            return (wall_ns, wall_ns, "c1", 1, tid, "DATA", json.dumps(payload))

        def binance_bbo(wall_ns, ms, bid="50000.0", ask="50001.0"):
            payload = {"e": "bookTicker", "s": "BTCUSDT", "b": bid, "a": ask, "T": ms}
            return (wall_ns, wall_ns, "b1", 1, ms, "DATA", json.dumps(payload))

        def binance_depth(wall_ns, ms, u, pu, is_snap=False):
            if is_snap:
                payload = {
                    "event": "REST_SNAPSHOT",
                    "symbol": "BTCUSDT",
                    "conn_id": "b1",
                    "epoch": 1,
                    "snapshot": {"lastUpdateId": u, "T": ms, "bids": [["50000.0", "1"]], "asks": [["50001.0", "1"]]},
                }
                return (wall_ns, wall_ns, "b1", 1, u, "META", json.dumps(payload))
            payload = {
                "e": "depthUpdate", "s": "BTCUSDT", "U": pu, "u": u, "pu": pu, "T": ms,
                "b": [["50000.0", "1.5"]], "a": [["50001.0", "1.5"]],
            }
            return (wall_ns, wall_ns, "b1", 1, u, "DATA", json.dumps(payload))

        cfg = {
            "deployment": {
                "partition_minutes": 15,
                "channel_silence_max_seconds": {
                    "depth": 1000,
                    "signal": 1000,
                    "bbo": 1000,
                    "event_trade": 1000,
                    "event_trade_quiet_max": 1000,
                    "other": 1000,
                },
            },
            "exchanges": {
                "binance_futures": {
                    "enabled": True,
                    "symbols": ["BTCUSDT"],
                    "public_channels": ["depth@100ms"],
                    "market_channels": ["bookTicker"],
                },
                "gate_futures": {
                    "enabled": True,
                    "symbols": ["BTC_USDT"],
                    "channels": ["futures.obu", "futures.trades"],
                },
            },
        }
        specs = {
            "gate_futures": {"tick": "0.1", "lot": "1", "multiplier": "0.0001", "order_qty": "1", "lot_source": "step"},
        }

        with tempfile.TemporaryDirectory() as temp:
            tmp = Path(temp)
            files = {}
            for idx, (w, u_start, is_first) in enumerate(((w0, 100, True), (w1, 103, False), (w2, 106, False))):
                w_ns = w * 1_000_000_000
                g_rows = []
                b_rows = []
                if is_first:
                    g_rows.append(gate_depth(w_ns + 1_000_000_000, (w + 1) * 1000, u_start, None,
                                             [["50000.0", "2"], ["49999.0", "2"]], [["50001.0", "2"], ["50002.0", "2"]], full=True))
                    b_rows.append(binance_depth(w_ns + 1_000_000_000, (w + 1) * 1000, u_start, None, is_snap=True))
                else:
                    g_rows.append(gate_depth(w_ns + 1_000_000_000, (w + 1) * 1000, u_start, u_start,
                                             [["50000.0", "3"]], [["50001.0", "3"]]))
                    b_rows.append(binance_depth(w_ns + 1_000_000_000, (w + 1) * 1000, u_start, u_start - 1))
                # Second & third updates covering target + replay window (>600s for w2).
                t_mid = w + (650 if w == w2 else 450)
                t_end = w + 850
                for step_idx, t_s in enumerate((t_mid, t_end), start=1):
                    u_val = u_start + step_idx
                    g_rows.append(gate_depth(t_s * 1_000_000_000, t_s * 1000, u_val, u_val,
                                             [["50000.0", str(2 + step_idx)]], [["50001.0", str(2 + step_idx)]]))
                    b_rows.append(binance_depth(t_s * 1_000_000_000, t_s * 1000, u_val, u_val - 1))
                g_rows.append(gate_trade((w + 10) * 1_000_000_000, (w + 10) * 1000, idx * 10 + 1))
                g_rows.append(gate_trade((w + 700) * 1_000_000_000, (w + 700) * 1000, idx * 10 + 2))
                b_rows.append(binance_bbo((w + 2) * 1_000_000_000, (w + 2) * 1000))
                b_rows.append(binance_bbo((w + 610) * 1_000_000_000, (w + 610) * 1000))
                b_rows.append(binance_bbo((w + 700) * 1_000_000_000, (w + 700) * 1000))
                g_rows.sort(key=lambda r: r[0])
                b_rows.sort(key=lambda r: r[0])
                g_path = tmp / f"gate_{w}.tsv.zst"
                b_path = tmp / f"binance_{w}.tsv.zst"
                write_zst(g_path, g_rows)
                write_zst(b_path, b_rows)
                files[w] = [
                    ({"exchange": "binance_futures", "planned_start_s": w, "planned_end_s": w + 900},
                     {"file_name": b_path.name, "boot_id": "boot1"}, b_path),
                    ({"exchange": "gate_futures", "planned_start_s": w, "planned_end_s": w + 900},
                     {"file_name": g_path.name, "boot_id": "boot1"}, g_path),
                ]

            fake_check = {"qa_status": "passed", "problems": [], "structural_problems": [], "coverage_incomplete": False}
            with patch.object(replay.archive, "check_partition", return_value=fake_check), \
                 patch.object(replay, "FOLLOWERS", ("gate_futures",)):
                # W0 (15:30Z) establishes anchored books.
                exp0 = {}
                s0 = {}
                replay.scan(files[w0], cfg, w0 * 1_000_000_000, w1 * 1_000_000_000, 0, {}, s0, export_checkpoint_state=exp0)
                self.assertEqual("passed", s0["protocol_status"])
                self.assertEqual(2, len(s0["end_sequence_checkpoint"]))
                self.assertTrue(all(r.get("book_anchored") for r in s0["end_sequence_checkpoint"]))

                # W1 (15:45Z) has pure deltas only and zero warmup files passed — inherits & extends book state!
                exp1 = {}
                s1 = {}
                replay.scan(
                    files[w1], cfg, w1 * 1_000_000_000, w2 * 1_000_000_000, 0, {}, s1,
                    checkpoint=s0["end_sequence_checkpoint"],
                    checkpoint_books=exp0["books"],
                    checkpoint_quote=exp0["end_signal_quote"],
                    checkpoint_streams=exp0["streams"],
                    export_checkpoint_state=exp1,
                )
                self.assertEqual("passed", s1["protocol_status"])
                self.assertEqual(2, s1["boundary_book_anchored_markets"])
                self.assertEqual(2, len(s1["end_sequence_checkpoint"]))
                self.assertEqual([[50000.0, 4.0], [49999.0, 2.0]], exp1["books"][("gate_futures", "BTCUSDT")]["bids"])

                # W2 (16:00Z) runs real hourly HftBacktest smoke replay + probe_orders on pure deltas with warmup=0!
                s2 = {}
                replay.scan(
                    files[w2], cfg, w2 * 1_000_000_000, (w2 + 900) * 1_000_000_000, (w2 + 660) * 1_000_000_000,
                    specs, s2,
                    checkpoint=s1["end_sequence_checkpoint"],
                    checkpoint_books=exp1["books"],
                    checkpoint_quote=exp1["end_signal_quote"],
                    checkpoint_streams=exp1["streams"],
                )
                self.assertEqual("passed", s2["protocol_status"])
                self.assertEqual("passed", s2["replay"]["gate_futures"]["status"], s2["replay"]["gate_futures"])
                self.assertEqual("passed", replay.window_status(s2["protocol_status"], True, list(s2["replay"].values())))

                # Tampering book levels fails digest validation closed (never silently accepts bad state).
                tampered_books = {
                    k: ({**v, "bids": [[99999.0, 1.0]]} if k == ("gate_futures", "BTCUSDT") else v)
                    for k, v in exp1["books"].items()
                }
                s_tampered = {}
                replay.scan(
                    files[w2], cfg, w2 * 1_000_000_000, (w2 + 900) * 1_000_000_000, (w2 + 660) * 1_000_000_000,
                    specs, s_tampered,
                    checkpoint=s1["end_sequence_checkpoint"],
                    checkpoint_books=tampered_books,
                    checkpoint_quote=exp1["end_signal_quote"],
                    checkpoint_streams=exp1["streams"],
                )
                self.assertEqual("anchor_unverified", s_tampered["protocol_status"])
                self.assertEqual("blocked", s_tampered["replay"]["gate_futures"]["status"])
                self.assertIn("gate_futures", {m["exchange"] for m in s_tampered["rejected_checkpoint_markets"]})

    def test_reconstruct_historical_checkpoint_walks_past_four_windows_and_stops_on_gap(self):
        import io
        import json
        from unittest.mock import patch
        import zstandard as zstd
        import data_coverage as coverage
        import tokyo_l2_replay_qa as replay

        w0 = 1_795_276_800
        step = 900
        windows = [w0 + i * step for i in range(7)]  # w0..w6 (6-window span > legacy lookback=4)
        cfg = {
            "deployment": {
                "partition_minutes": 15,
                "channel_silence_max_seconds": {
                    "depth": 1000, "signal": 1000, "bbo": 1000,
                    "event_trade": 1000, "event_trade_quiet_max": 1000, "other": 1000,
                },
            },
            "exchanges": {"gate_futures": {"enabled": True, "symbols": ["BTC_USDT"], "channels": ["futures.obu"]}},
        }

        def make_zst(path, msgs):
            buf = io.StringIO()
            for r in msgs:
                buf.write("\t".join(map(str, r)) + "\n")
            path.write_bytes(zstd.ZstdCompressor().compress(buf.getvalue().encode("utf-8")))

        with tempfile.TemporaryDirectory() as temp:
            tmp = Path(temp)
            files_by_w = {}
            cands = []
            seq = 10
            for idx, w in enumerate(windows):
                w_ns = w * 1_000_000_000
                msgs = []
                if idx == 0:
                    payload = {"channel": "futures.obu", "event": "update", "result": {
                        "s": "ob.BTC_USDT.50", "full": True, "u": seq, "t": w * 1000 + 1000,
                        "b": [["50000.0", "2"]], "a": [["50001.0", "2"]],
                    }}
                    msgs.append((w_ns + 1_000_000_000, w_ns + 1_000_000_000, "c1", 1, seq, "DATA", json.dumps(payload)))
                else:
                    # Break sequence on window 3 in a copy below; here keep strictly continuous.
                    seq += 1
                    payload = {"channel": "futures.obu", "event": "update", "result": {
                        "s": "ob.BTC_USDT.50", "u": seq, "U": seq, "t": w * 1000 + 1000,
                        "b": [["50000.0", str(2 + idx)]], "a": [["50001.0", "2"]],
                    }}
                    msgs.append((w_ns + 1_000_000_000, w_ns + 1_000_000_000, "c1", 1, seq, "DATA", json.dumps(payload)))
                seq += 1
                msgs.append(((w + 600) * 1_000_000_000, (w + 600) * 1_000_000_000, "c1", 1, seq, "DATA", json.dumps({
                    "channel": "futures.obu", "event": "update", "result": {
                        "s": "ob.BTC_USDT.50", "u": seq, "U": seq, "t": (w + 600) * 1000,
                        "b": [["50000.0", str(3 + idx)]], "a": [["50001.0", "2"]],
                    },
                })))
                fpath = tmp / f"gate_{w}.tsv.zst"
                make_zst(fpath, msgs)
                cand = {
                    "exchange": "gate_futures",
                    "planned_start_s": w,
                    "planned_end_s": w + step,
                    "source_manifest": f"raw/gate_futures/{w}.manifest.json",
                    "source_data": f"raw/gate_futures/{w}.tsv.zst",
                    "manifest_sha256": "a" * 64,
                    "data_sha256": "b" * 64,
                    "data_bytes": 100,
                }
                cands.append(cand)
                files_by_w[w] = (cand, {"file_name": fpath.name, "boot_id": "b1"}, fpath)

            def fake_prepare(_remote, cand, _run_id, _dest_dir):
                return files_by_w[cand["planned_start_s"]]

            fake_check = {"qa_status": "passed", "problems": [], "structural_problems": [], "coverage_incomplete": False}
            with patch.object(replay, "_prepare_single_candidate", side_effect=fake_prepare), \
                 patch.object(replay.archive, "check_partition", return_value=fake_check):
                ckpt, books, _q, diag = replay.reconstruct_historical_checkpoint(
                    "qa:", cfg, "r1", windows[-1], step, 12, tmp, candidates=cands,
                )
                self.assertEqual(6, diag["reconstructed_lookback_windows"])
                self.assertEqual(1, len(ckpt))
                self.assertTrue(ckpt[0]["book_anchored"])
                self.assertEqual([[50000.0, 8.0]], books[("gate_futures", "BTCUSDT")]["bids"])

                # Inject a sequence break in window 3 -> reconstruction MUST refuse to anchor window 6.
                w3 = windows[3]
                broken_path = tmp / "gate_w3_broken.tsv.zst"
                make_zst(broken_path, [
                    (w3 * 1_000_000_000 + 1_000_000_000, w3 * 1_000_000_000 + 1_000_000_000, "c1", 1, 999, "DATA", json.dumps({
                        "channel": "futures.obu", "event": "update", "result": {
                            "s": "ob.BTC_USDT.50", "u": 999, "U": 998, "t": w3 * 1000 + 1000,
                            "b": [["50000.0", "1"]], "a": [["50001.0", "2"]],
                        },
                    })),
                ])
                files_by_w[w3] = (files_by_w[w3][0], files_by_w[w3][1], broken_path)
                ckpt_bad, books_bad, _qb, _db = replay.reconstruct_historical_checkpoint(
                    "qa:", cfg, "r1", windows[-1], step, 12, tmp, candidates=cands,
                )
                self.assertEqual([], ckpt_bad)
                self.assertEqual({}, books_bad)

    def test_recurring_v2_checker_survival_and_clean_execution_exit_code(self):
        import json
        import sys
        from unittest.mock import patch
        import data_coverage as coverage
        import hourly_incremental_qa as archive
        import tokyo_l2_recurring_qa as recurring

        w0 = 1_795_276_800
        w1 = w0 + 900
        run_id = "v2_cache_run"
        sha1 = "aaa111"
        sha2 = "bbb222"
        cfg = {
            "deployment": {"partition_minutes": 15},
            "exchanges": {"gate_futures": {"enabled": True, "symbols": ["BTC_USDT"], "channels": ["futures.obu"]}},
        }
        cands = []
        for s in (w0, w1):
            wid = coverage.window_descriptor(s, 15)["window_id"]
            mf = f"raw/gate_futures/{wid[:10]}/gate_{s}.manifest.json"
            cands.append({
                "exchange": "gate_futures", "source_manifest": mf,
                "source_data": mf.replace(".manifest.json", ".tsv.zst"),
                "manifest_sha256": f"{s:064x}", "data_sha256": f"{s+1:064x}",
                "manifest_bytes": 100, "data_bytes": 200, "planned_start_s": s,
            })

        def set_window_reports(sha):
            for c in cands:
                wid = coverage.window_descriptor(c["planned_start_s"], 15)["window_id"]
                p = state / "qa_reports" / "windows" / wid[:10] / f"{archive._safe_window_filename(wid)}.window.qa.json"
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text(json.dumps({
                    "qa_status": "passed", "run_id": run_id, "partition_minutes": 15,
                    "checker_commit_sha": sha, "files": [c["source_manifest"]],
                }), encoding="utf-8")

        calls = []

        def fake_audit(_r, _cfg, _rid, start, window, _rs, _wu, **kw):
            calls.append(start)
            exp = kw.get("export_checkpoint_state")
            if start == w0:
                from tokyo_l2_protocol_qa import Book
                b = Book.from_levels({"bids": [[50000.0, 1.0]], "asks": [[50001.0, 1.0]]})
                bhash = b.digest()
                if exp is not None:
                    exp["books"] = {("gate_futures", "BTCUSDT"): {
                        "book_sha256": bhash, "last_exchange_ns": start * 1_000_000_000,
                        **b.canonical_levels(),
                    }}
                return {
                    "status": "passed", "protocol_status": "passed", "run_id": run_id, "window_id": window,
                    "target_inputs": recurring.input_digests([cands[0]]),
                    "end_sequence_checkpoint": [{
                        "exchange": "gate_futures", "symbol": "BTCUSDT", "connection": ["b", "c", 1],
                        "seq": 10, "awaiting_delta": False, "last_wall_ns": start * 1_000_000_000,
                        "book_anchored": True, "book_sha256": bhash, "last_exchange_ns": start * 1_000_000_000,
                    }],
                }
            return {
                "status": "anchor_unverified", "protocol_status": "anchor_unverified",
                "run_id": run_id, "window_id": window,
                "target_inputs": recurring.input_digests([cands[1]]),
                "end_sequence_checkpoint": [],
            }

        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp)
            set_window_reports(sha1)
            with patch.object(recurring, "verified_candidates", return_value=cands), \
                 patch.object(recurring, "audit_window", side_effect=fake_audit):
                r_pre = recurring.run("qa:", cfg, run_id, w0, w1, state, f"{sha1}/r1", max_catchup=2)
                self.assertEqual([w1, w0], calls)
                # w0's new checkpoint revokes w1's unanchored result to pending until re-evaluated.
                self.assertEqual("pending", r_pre["status"])

                calls.clear()
                r1 = recurring.run("qa:", cfg, run_id, w0, w1, state, f"{sha1}/r1", max_catchup=2)
                self.assertEqual([w1], calls)
                self.assertEqual("anchor_unverified", r1["status"])
                self.assertEqual("ok", r1["job_execution_status"])

                # Same checker: neither w0 nor w1 re-runs (w1 deferred).
                calls.clear()
                r2 = recurring.run("qa:", cfg, run_id, w0, w1, state, f"{sha1}/r1", max_catchup=2)
                self.assertEqual([], calls)
                self.assertTrue(r2["reused_current_result"])

                # New checker SHA: w0 (passed, v2) stays cached with intact sidecar; w1 (anchor_unverified) re-evaluates once!
                set_window_reports(sha2)
                calls.clear()
                r3 = recurring.run("qa:", cfg, run_id, w0, w1, state, f"{sha2}/r2", max_catchup=2)
                self.assertEqual([w1], calls)
                self.assertEqual("ok", r3["job_execution_status"])

                # CLI --exit-zero-on-clean-execution exits 0 on anchor_unverified and 2 on revision_mismatch.
                cfg_path = state / "cfg.json"
                cfg_path.write_text(json.dumps(cfg), encoding="utf-8")
                argv = [
                    "tokyo_l2_recurring_qa.py", "--remote", "qa:", "--config", str(cfg_path),
                    "--run-id", run_id, "--cutover", coverage.window_descriptor(w0, 15)["window_id"],
                    "--current", coverage.window_descriptor(w1, 15)["window_id"],
                    "--state-dir", str(state), "--checker", f"{sha2}/r2",
                    "--exit-zero-on-clean-execution",
                ]
                with patch.object(sys, "argv", argv), patch.object(recurring, "run", return_value=r3):
                    self.assertEqual(0, recurring.main())
                with patch.object(sys, "argv", argv), patch.object(
                    recurring, "run", return_value={**r3, "status": "revision_mismatch", "job_execution_status": "failed"}
                ):
                    self.assertEqual(2, recurring.main())


if __name__ == "__main__":
    unittest.main()
