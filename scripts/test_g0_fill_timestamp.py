"""Verify that official hftbacktest filled order exposes actual exchange fill time.

This is a synthetic controlled tape; results are not empirical crypto observations.
No matching engine implementation is replaced or added.
"""
import tempfile
import unittest
from pathlib import Path


class FilledOrderTimestampTests(unittest.TestCase):
    def test_filled_order_exchange_timestamp_tracks_fill_event(self):
        from hftbacktest import (BacktestAsset, ROIVectorMarketDepthBacktest,
                                 FILLED, GTX, LIMIT)
        from hftbacktest.data.utils import tardis

        t0=1793491200000000  # us
        buy_order_id=2000
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            books=root/"books.csv"
            trades=root/"trades.csv"
            event_file=root/"events.npz"
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
            events=tardis.convert([str(trades),str(books)],
                                  output_filename=str(event_file),
                                  buffer_size=10000,ss_buffer_size=1000,
                                  snapshot_mode="process")
            self.assertGreater(len(events),0)
            asset=(BacktestAsset().data([str(event_file)]).linear_asset(0.01)
                   .constant_order_latency(1_000_000,1_000_000)
                   .power_prob_queue_model(2.0).no_partial_fill_exchange()
                   .trading_value_fee_model(0.0002,0.0005)
                   .tick_size(0.1).lot_size(0.01)
                   .roi_lb(90).roi_ub(110).last_trades_capacity(50))
            engine=ROIVectorMarketDepthBacktest([asset])
            try:
                self.assertEqual(engine.elapse(1_000_000_000),0)
                engine.submit_buy_order(0,buy_order_id,100.0,0.1,GTX,LIMIT,True)
                before=engine.orders(0).get(buy_order_id)
                before_exch=int(before.exch_timestamp)
                before_local=int(before.local_timestamp)
                self.assertEqual(engine.elapse(2_000_000_000),0)
                after=engine.orders(0).get(buy_order_id)
                self.assertEqual(after.status,FILLED)
                after_exch=int(after.exch_timestamp)
                after_local=int(after.local_timestamp)
                exchange_trade_ns=(t0+2000000)*1000
                print("PROBE order_id=",buy_order_id,
                      "before_exch_ns=",before_exch,
                      "before_local_ns=",before_local,
                      "filled_exch_ns=",after_exch,
                      "filled_local_ns=",after_local,
                      "trade_exch_ns=",exchange_trade_ns)
                self.assertGreater(after_exch,before_exch,
                    "Order.exchange_timestamp did not advance at fill")
                self.assertEqual(after_exch,exchange_trade_ns,
                    "Cannot use order.exchange_timestamp as exact trade time until documented")
                self.assertEqual(after.exec_qty,0.1)
                self.assertAlmostEqual(after.exec_price,100.0)
            finally:
                engine.close()


if __name__=="__main__":
    unittest.main()
