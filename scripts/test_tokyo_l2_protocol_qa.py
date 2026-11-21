import unittest

from tokyo_l2_recurring_qa import plan_windows

from tokyo_l2_protocol_qa import BITGET, BYBIT, GATE, Book, Chain, Depth, depth_event, verify_gate_sizes


def depth(mode, seq, parent=None, epoch=1):
    return Depth("BTCUSDT", ("boot", "A", epoch), mode, seq, parent)


class ProtocolTest(unittest.TestCase):
    def test_recurring_checks_latest_and_one_oldest_pending_partition(self):
        self.assertEqual([400, 200], plan_windows([100, 200, 300, 400], {100: {}}, 400))
        self.assertEqual([200], plan_windows([100, 200, 300, 400], {100: {}, 400: {}}, 400))

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
