"""Deterministic SYNTHETIC tests. These are not real empirical markouts."""
import unittest
import numpy as np

from full_day_fill_quality import multiset_match, evaluate_full_day_fill_quality
from g0_glft_event_driven import audit_rolling_request_2s

NS = 1_000_000_000


def f(time_ns, side, price):
    return {
        "exch_timestamp_ns": time_ns,
        "side_sign": side,
        "execution_price_usdt_per_btc": price,
        "executed_qty_contracts": .1,
    }


class FullDayFillQualityTests(unittest.TestCase):
    def test_multiset_matches_once_and_price_sensitive_partition(self):
        t = 10*NS
        b = [f(t,1,99.9),f(t,1,99.8),f(t,-1,100.1)]
        a = [f(t,1,99.8),f(t,-1,100.2)]
        x,y = multiset_match(b,a)
        self.assertEqual(x.tolist(), [True,False,True])
        self.assertEqual(y.tolist(), [True,True])
        px,py = multiset_match(b,a,include_execution_price=True)
        self.assertEqual(px.tolist(), [False,True,False])
        self.assertEqual(py.tolist(), [True,False])

    def test_impact_and_continuation_signs_are_distinct(self):
        t = 10*NS
        # buy from strictly prefill mid 100.0, first postfill 99.0;
        # 100ms mid 99.5: overall adverse, postfill continuation favorable.
        xs = np.array([t-10_000_000,t+1_000_000,
                       t+100_000_000,t+500_000_000],np.int64)
        mids = np.array([100.,99.,99.5,99.0])
        one = f(t,1,99.9)
        r = evaluate_full_day_fill_quality({
            "beta_0":[one],
            "beta_1":[],
            "beta_0_wall":[one],
            "beta_1_wall":[],
        },xs,mids)
        x = r["all_actual_fills"]["beta_0"]["horizons"]["100"]
        self.assertEqual(x["n_valid"],1)
        self.assertLess(x["mean_signed_markout_bps"],0)
        self.assertGreater(x["mean_continuation_signed_move_bps"],0)
        self.assertEqual(x["continuation_adverse_fraction"],0.0)
        self.assertEqual(r["paired_baseline_versus_signal"][
            "beta_0_vs_beta_1"]["baseline_only_time_side"],1)

    def test_nothing_is_called_prevented_from_overlap(self):
        t = 10*NS
        arr = [f(t,1,99.9),f(t,1,99.8),f(t+NS,-1,100.1)]
        alternative = [f(t,1,99.8),f(t+2*NS,-1,100.1)]
        time = np.arange(t-100_000_000,t+3*NS,10_000_000,dtype=np.int64)
        mids = np.linspace(100.,99.,len(time))
        audit = evaluate_full_day_fill_quality({
            "beta_0":arr,"beta_1":alternative,
            "beta_0_wall":arr,"beta_1_wall":alternative,
        },time,mids)
        d = audit["paired_baseline_versus_signal"]["beta_0_vs_beta_1"]
        self.assertEqual(d["baseline_fills"],3)
        self.assertEqual(d["signal_fills"],2)
        self.assertEqual(d["same_exchange_ns_side_overlap"],1)
        self.assertEqual(d["same_exchange_ns_side_execution_price_tick_overlap"],1)
        self.assertEqual(d["baseline_only_time_side"],2)
        self.assertEqual(d["signal_only_time_side"],1)
        self.assertEqual(d["baseline_only_exact_quote"],2)
        self.assertEqual(d["signal_only_exact_quote"],1)
        self.assertEqual(d["groups"][
            "baseline_without_simultaneous_same_side_signal_fill"]["n_fills"],2)

    def test_request_rate_rolling_2second_boundary_and_limit(self):
        # Synthetic local send clock. 2-second-old requests leave the window.
        clock=np.empty(32,np.int64)
        head=tail=peak=over=0
        for tt in (0,100_000_000,1_900_000_000,2_000_000_000,2_100_000_000):
            head,tail,peak,over=audit_rolling_request_2s(
                clock,head,tail,tt,peak,over,2)
        self.assertEqual(peak,3)
        self.assertEqual(over,3)
        self.assertEqual(tail-head,3)

    def test_invalid_fill_or_unsorted_bbo_fails(self):
        t=10*NS
        inputs={
            "beta_0":[f(t,0,100.)],"beta_1":[],
            "beta_0_wall":[],"beta_1_wall":[]
        }
        with self.assertRaises(ValueError):
            evaluate_full_day_fill_quality(
                inputs,np.array([t-1,t+1],np.int64),np.array([100.,100.]))
        inputs["beta_0"]=[f(t,1,100.)]
        with self.assertRaises(ValueError):
            evaluate_full_day_fill_quality(
                inputs,np.array([t+1,t-1],np.int64),np.array([100.,100.]))


if __name__=="__main__":
    unittest.main()
