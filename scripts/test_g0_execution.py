"""Controlled execution-accounting regression on synthetic OKX-like data.

The MATCHING ENGINE is official hftbacktest. Synthetic tape is not an empirical result.
"""
import tempfile
import unittest
from pathlib import Path


class OfficialEngineAccountingTests(unittest.TestCase):
    def test_post_only_fill_inventory_cash_fee(self):
        from hftbacktest import (BacktestAsset, ROIVectorMarketDepthBacktest,
                                 GTX, LIMIT)
        from hftbacktest.data.utils import tardis

        t0 = 1793491200000000
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            books = root / "books.csv"
            trades = root / "trades.csv"
            npz = root / "events.npz"
            books.write_text(
                "exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount\n"
                f"okex-swap,BTC-USDT-SWAP,{t0},{t0},true,bid,100.0,1\n"
                f"okex-swap,BTC-USDT-SWAP,{t0},{t0},true,ask,101.0,1\n"
                f"okex-swap,BTC-USDT-SWAP,{t0+2100000},{t0+2100000},false,bid,100.0,0\n"
                f"okex-swap,BTC-USDT-SWAP,{t0+2100001},{t0+2100001},false,bid,99.9,2\n"
                f"okex-swap,BTC-USDT-SWAP,{t0+5000000},{t0+5000000},false,ask,101.0,2\n"
            )
            trades.write_text(
                "exchange,symbol,timestamp,local_timestamp,id,side,price,amount\n"
                f"okex-swap,BTC-USDT-SWAP,{t0+2000000},{t0+2000000},s1,sell,99.9,100\n"
            )
            events = tardis.convert([str(trades), str(books)], output_filename=str(npz),
                                    buffer_size=10_000, ss_buffer_size=1000,
                                    snapshot_mode="process")
            self.assertGreater(len(events), 0)
            asset = (BacktestAsset().data([str(npz)]).linear_asset(0.01)
                     .constant_order_latency(1_000_000, 1_000_000)
                     .power_prob_queue_model(2.0).no_partial_fill_exchange()
                     .trading_value_fee_model(0.0002, 0.0005)
                     .tick_size(0.1).lot_size(0.01)
                     .roi_lb(90).roi_ub(110).last_trades_capacity(50))
            engine = ROIVectorMarketDepthBacktest([asset])
            try:
                self.assertEqual(engine.elapse(1_000_000_000), 0)
                engine.submit_buy_order(0, 2000, 100.0, 0.1, GTX, LIMIT, True)
                engine.elapse(2_000_000_000)
                state = engine.state_values(0)
                self.assertGreater(state.num_trades, 0,
                                   "Expected official engine to fill maker buy after trade-through")
                self.assertAlmostEqual(state.position, 0.1)
                self.assertAlmostEqual(state.balance, -0.1 * 0.01 * 100.0)
                self.assertAlmostEqual(state.fee, 0.1 * 0.01 * 100.0 * 0.0002)
                mark = 100.5
                expected_equity = state.balance + 0.1 * 0.01 * mark - state.fee
                self.assertAlmostEqual(expected_equity,
                                       0.1*0.01*(mark-100.0)-state.fee)
            finally:
                engine.close()


if __name__ == "__main__":
    unittest.main()
