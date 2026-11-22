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


if __name__ == "__main__":
    unittest.main()
