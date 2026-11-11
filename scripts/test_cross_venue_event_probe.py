"""Deterministic event ordering and real HBT cancel-versus-fill tests.

All times/quotes in this file are synthetic fixtures, never empirical results.
"""
import tempfile
import unittest
from pathlib import Path

import numpy as np

from cross_venue_event_probe import replay_union, visible_timestamps


class FakeEngine:
    """Only checks the scheduler protocol; official HBT tested separately."""
    def __init__(self):
        self.current_timestamp = 0
        self.events = [(2, "okx_feed"), (2, "order_reply"), (8, "okx_feed")]
        self.index = 0

    def elapse(self, duration):
        assert duration == 0
        return 0

    def wait_next_feed(self, include_order_resp, timeout):
        assert include_order_resp and timeout >= 0
        target = self.current_timestamp + timeout
        if self.index < len(self.events) and self.events[self.index][0] <= target:
            t, kind = self.events[self.index]
            self.index += 1
            self.current_timestamp = t
            return 2 if kind == "okx_feed" else 3
        self.current_timestamp = target
        return 0


class SchedulerTests(unittest.TestCase):
    def test_interleaving_and_relative_timeout(self):
        engine = FakeEngine()
        seen = []
        stats = replay_union(engine, np.array([3, 3, 7]), np.array([101., 102., 103.]),
                             10, lambda kind, ts, price: seen.append((kind, ts, price)))
        self.assertEqual([(k, t) for k, t, _ in seen],
                         [("okx_feed", 2), ("order_reply", 2),
                          ("binance_feed", 3), ("binance_feed", 3),
                          ("binance_feed", 7), ("okx_feed", 8)])
        self.assertEqual(seen[2][2], 101.)
        self.assertEqual(seen[3][2], 102.)
        self.assertEqual(stats["binance_feed"], 3)
        self.assertEqual(engine.current_timestamp, 10)

    def test_no_future_signal_and_delay(self):
        self.assertTrue(np.array_equal(
            visible_timestamps(np.array([1000, 1000, 3000], dtype=np.int64), 1),
            np.array([1_001_001, 1_001_001, 1_003_001])))
        with self.assertRaises(ValueError):
            visible_timestamps([3, 2], 0)
        with self.assertRaises(ValueError):
            visible_timestamps([3], -1)

    def test_ordering_rejects_unsorted_source(self):
        with self.assertRaises(ValueError):
            replay_union(FakeEngine(), [5, 3], [100., 99.], 10, lambda *_: None)


class OfficialCancelCompetition(unittest.TestCase):
    def execute(self, cancel_ns):
        from hftbacktest import BacktestAsset, ROIVectorMarketDepthBacktest
        from hftbacktest import GTX, LIMIT, FILLED
        from hftbacktest.data.utils import tardis

        t0_us = 1793491200000000
        t0_ns = t0_us * 1000
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            books, trades, npz = (root / n for n in ("book.csv", "trade.csv", "okx.npz"))
            books.write_text(
                "exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount\\n"
                f"okex-swap,BTC-USDT-SWAP,{t0_us},{t0_us},true,bid,100.0,1\\n"
                f"okex-swap,BTC-USDT-SWAP,{t0_us},{t0_us},true,ask,101.0,1\\n"
                f"okex-swap,BTC-USDT-SWAP,{t0_us+5100},{t0_us+5100},false,bid,100.0,0\\n"
                f"okex-swap,BTC-USDT-SWAP,{t0_us+5101},{t0_us+5101},false,bid,99.9,2\\n"
                f"okex-swap,BTC-USDT-SWAP,{t0_us+8000},{t0_us+8000},false,ask,101.0,2\\n")
            trades.write_text(
                "exchange,symbol,timestamp,local_timestamp,id,side,price,amount\\n"
                f"okex-swap,BTC-USDT-SWAP,{t0_us+5000},{t0_us+5000},s1,sell,99.9,100\\n")
            tardis.convert([str(trades), str(books)], output_filename=str(npz),
                           buffer_size=10000, ss_buffer_size=1000, snapshot_mode="process")
            asset = (BacktestAsset().data([str(npz)]).linear_asset(0.01)
                     .constant_order_latency(1_000_000, 1_000_000)
                     .power_prob_queue_model(2.0).no_partial_fill_exchange()
                     .trading_value_fee_model(0.0002, 0.0005)
                     .tick_size(0.1).lot_size(0.01)
                     .roi_lb(90).roi_ub(110).last_trades_capacity(50))
            engine = ROIVectorMarketDepthBacktest([asset])
            try:
                events = []
                def handle(kind, now, _mid):
                    if kind != "binance_feed":
                        return
                    events.append(now)
                    if len(events) == 1:
                        engine.submit_buy_order(0, 2000, 100., 0.1, GTX, LIMIT, False)
                    else:
                        order = engine.orders(0).get(2000)
                        if order.cancellable:
                            engine.cancel(0, 2000, False)
                        else:
                            raise AssertionError("Resting order not cancellable before second signal")
                bt = np.array([t0_ns + 1_000_001, t0_ns + cancel_ns], dtype=np.int64)
                stats = replay_union(engine, bt, np.array([100., 99.]),
                                     t0_ns + 9_000_000, handle)
                order = engine.orders(0).get(2000)
                filled = order.status == FILLED
                return filled, int(engine.state_values(0).num_trades), stats, events
            finally:
                engine.close()

    def test_cancel_before_trade_vs_after_trade(self):
        early = self.execute(3_000_001)  # cancel reaches venue near 4ms, trade at 5ms
        late = self.execute(4_500_001)   # cancel reaches venue near 5.5ms, trade at 5ms
        self.assertFalse(early[0])
        self.assertEqual(early[1], 0)
        self.assertTrue(late[0])
        self.assertEqual(late[1], 1)
        self.assertEqual(len(early[3]), 2)
        self.assertEqual(len(late[3]), 2)


if __name__ == "__main__":
    unittest.main()
