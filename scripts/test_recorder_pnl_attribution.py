"""Tests for the exact PnL bridge and local observed-time post-fill markout.

All synthetic examples are numerical unit tests, not actual market results.
"""
import unittest
import numpy as np

from analyze_multivenue_recorder_markout import compute_attribution


class PnlAttributionTests(unittest.TestCase):
    def test_buy_then_sell_conserves_ledger_and_marks_toxic_short_horizon(self):
        base=1_793_491_200_000_000_000
        samples=30_000
        dtype=np.dtype([
            ("timestamp","<i8"),("price","<f8"),("position","<f8"),
            ("balance","<f8"),("fee","<f8"),
            ("num_trades","<i8"),("trading_volume","<f8"),
            ("trading_value","<f8")])
        rec=np.zeros(samples,dtype=dtype)
        rec["timestamp"]=base+(np.arange(1,samples+1,dtype=np.int64)*100_000_000)
        quote_times=base+np.arange(7,dtype=np.int64)*100_000_000
        quote_mid=np.array([100.,100.,99.,98.,98.,101.,101.])
        rec["price"] = quote_mid[
            np.minimum(np.searchsorted(quote_times,rec["timestamp"],side="right")-1,
                       len(quote_mid)-1)]
        buy_at=base+150_000_000
        sell_at=base+450_000_000
        bought=rec["timestamp"]>=buy_at
        sold=rec["timestamp"]>=sell_at
        rec["position"][bought]=1.
        rec["position"][sold]=0.
        rec["balance"][bought]=-99.5
        rec["balance"][sold]=-0.5
        rec["fee"][sold]=0.05
        rec["num_trades"][bought]=1
        rec["num_trades"][sold]=2
        fills={
            "local_observed_ns":np.array([buy_at,sell_at],dtype=np.int64),
            "exchange_timestamp_ns":np.array([buy_at-1_000_000,sell_at-1_000_000]),
            "side":np.array([1,-1],dtype=np.int64),
            "exec_qty":np.array([1.,1.]),
            "exec_price":np.array([99.5,99.])
        }
        trader={"engine_fills":2,"balance_usdt":-0.5,
                "end_position_contracts":0.,
                "end_mid_usdt_per_btc":101.,
                "fee_usdt":0.05,"end_net_marked_equity_usdt":-0.55}
        result,annot=compute_attribution(
            rec,fills,trader,1.,quote_times,quote_mid)
        self.assertAlmostEqual(result["gross_usdt"],-.5)
        self.assertAlmostEqual(result["execution_edge_at_observation_usdt"],1.5)
        self.assertAlmostEqual(result["inventory_native_bbo_mark_usdt"],-2.)
        self.assertAlmostEqual(result["terminal_hbt_bbo_mid_adjust_usdt"],0.)
        self.assertAlmostEqual(result["reconciliation_residual_usdt"],0.,places=9)
        self.assertEqual(result["markout"]["100ms"]["fresh_n"],2)
        self.assertLess(result["markout"]["100ms"]["fresh_weighted_bps"],0.)
        self.assertAlmostEqual(result["local_observation_minus_exchange_time_p50_ms"],1.)
        np.testing.assert_array_equal(annot["side"],[1,-1])

    def test_rejects_bad_fee_or_balance_ledger(self):
        base=1_793_491_200_000_000_000
        dtype=np.dtype([
            ("timestamp","<i8"),("price","<f8"),("position","<f8"),
            ("balance","<f8"),("fee","<f8")])
        rec=np.zeros(30_000,dtype=dtype)
        rec["timestamp"]=base+np.arange(1,30_001)*100_000_000
        rec["price"]=100.
        fills={"local_observed_ns":np.array([],dtype=np.int64),
               "exchange_timestamp_ns":np.array([],dtype=np.int64),
               "side":np.array([],dtype=np.int64),
               "exec_qty":np.array([],dtype=np.float64),
               "exec_price":np.array([],dtype=np.float64)}
        trader={"engine_fills":0,"balance_usdt":0.,"end_position_contracts":0.,
                "end_mid_usdt_per_btc":100.,"fee_usdt":0.,
                "end_net_marked_equity_usdt":-1.}
        with self.assertRaisesRegex(ValueError,"gross PnL identity"):
            compute_attribution(rec,fills,trader,1.,
                                np.array([base],dtype=np.int64),np.array([100.]))

if __name__=="__main__":
    unittest.main()
