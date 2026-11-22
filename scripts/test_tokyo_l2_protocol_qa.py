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
