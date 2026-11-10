"""No-lookahead maker fill markout using true exchange timestamps."""
import unittest
import numpy as np
from g0_fill_markout import asof_exchange_mid, evaluate_fill_markouts


class FillMarkoutTests(unittest.TestCase):
    def test_asof_strict_at_fill_and_no_future_leak(self):
        t0=1_000_000_000_000
        qts=np.array([t0-10_000_000,t0,t0+100_000_000,
                      t0+500_000_000],dtype=np.int64)
        px=np.array([100.,105.,99.,98.])
        at,valid,age=asof_exchange_mid(qts,px,np.array([t0]),strict=True)
        self.assertTrue(valid[0])
        self.assertEqual(at[0],100)
        future,ok,_=asof_exchange_mid(qts,px,np.array([t0+100_000_000]))
        self.assertTrue(ok[0])
        self.assertEqual(future[0],99)

    def test_reference_missing_when_quote_too_stale(self):
        a=np.array([1_000_000_000],dtype=np.int64)
        b=np.array([100.])
        result,valid,_=asof_exchange_mid(
            a,b,np.array([1_000_000_000+1_000_000_000],dtype=np.int64),
            strict=True,max_age_ms=250)
        self.assertFalse(valid[0])
        self.assertTrue(np.isnan(result[0]))

    def test_buy_markout_decomposes_capture_fee_adverse_move(self):
        t0=1_000_000_000_000
        qts=np.array([t0-10_000_000,t0+100_000_000,
                      t0+500_000_000],dtype=np.int64)
        price=np.array([100.,99.,98.])
        fill=[{"exch_timestamp_ns":t0,"side_sign":1,
               "execution_price_usdt_per_btc":99.5,
               "executed_qty_contracts":0.1,"order_id":1}]
        x=evaluate_fill_markouts(fill,qts,price,(100,))
        m=x["horizons"]["100"]
        self.assertEqual(x["n_valid_reference_mid"],1)
        self.assertEqual(m["n_valid"],1)
        self.assertAlmostEqual(m["mean_spread_capture_usdt"],0.0005)
        self.assertAlmostEqual(m["mean_signed_markout_usdt"],-0.001)
        self.assertAlmostEqual(m["mean_maker_fee_usdt"],0.0000199)
        self.assertAlmostEqual(m["mean_horizon_net_proxy_usdt"],
                               .0005-.001-.0000199)
        self.assertAlmostEqual(m["adverse_pre_fill_to_horizon_fraction"],1)
        self.assertEqual(m["n_valid_impact_continuation_split"],1)
        self.assertAlmostEqual(m["mean_immediate_signed_move_usdt"],-0.001)
        self.assertAlmostEqual(m["mean_continuation_signed_move_usdt"],0)
        self.assertAlmostEqual(m["continuation_adverse_fraction"],0)

    def test_sell_has_opposite_markout_sign(self):
        t0=1_000_000_000_000
        qts=np.array([t0-10_000_000,t0+100_000_000],dtype=np.int64)
        price=np.array([100.,101.])
        fill=[{"exch_timestamp_ns":t0,"side_sign":-1,
               "execution_price_usdt_per_btc":100.5,
               "executed_qty_contracts":0.1,"order_id":2}]
        m=evaluate_fill_markouts(fill,qts,price,(100,))["horizons"]["100"]
        self.assertLess(m["mean_signed_markout_usdt"],0)

    def test_late_fill_is_censored_not_assumed_zero(self):
        t0=1_000_000_000_000
        qts=np.array([t0-10_000_000],dtype=np.int64)
        mid=np.array([100.])
        fill=[{"exch_timestamp_ns":t0,"side_sign":1,
               "execution_price_usdt_per_btc":99.9,
               "executed_qty_contracts":0.1,"order_id":2}]
        result=evaluate_fill_markouts(fill,qts,mid,(100,500))
        self.assertEqual(result["horizons"]["500"]["n_valid"],0)
        self.assertEqual(result["horizons"]["500"]["n_censored"],1)


if __name__=="__main__":
    unittest.main()
