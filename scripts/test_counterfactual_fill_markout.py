"""Deterministic counterfactual audit tests; synthetic quotes and fills only."""
import unittest

import numpy as np

from g0_glft_baseline import TICK
from run_counterfactual_fill_markout import (
    match_same_exchange_time_side, prefill_probes, probe_state_class,
    snapshot_rows_at, compare_fill_opportunities, describe_coverage, NS
)


def fill(t, side, price=100., local_lag_ms=3):
    return {
        "exch_timestamp_ns": int(t),
        "local_observed_ns": int(t+local_lag_ms*1_000_000),
        "side_sign": side,
        "execution_price_usdt_per_btc": price,
        "executed_qty_contracts": .1,
    }


class CounterfactualAuditTests(unittest.TestCase):
    def test_matching_respects_same_time_side_and_duplicates(self):
        t=10*NS
        base=[fill(t,1),fill(t,1),fill(t,-1),
              fill(t+10_000_000,-1)]
        alt=[fill(t,1),fill(t,-1),fill(t+5_000_000,1)]
        found=match_same_exchange_time_side(base,alt)
        self.assertEqual(found.tolist(),[True,False,True,False])

    def test_nominal_probe_states_are_conservative_descriptions(self):
        t=10*NS
        buy=fill(t,1,100.)
        sell=fill(t,-1,100.)
        # 0=best bid tick,1=best ask tick,2/3 total local order counts,
        # 4/5 cancellable order counts,6=inventory
        self.assertEqual(probe_state_class(buy, np.array(
            [999., np.nan,1.,0.,1.,0.,0.])),
            "less_aggressive_cancellable_quote")
        self.assertEqual(probe_state_class(sell, np.array(
            [np.nan,1001.,0.,1.,0.,1.,0.])),
            "less_aggressive_cancellable_quote")
        self.assertEqual(probe_state_class(buy, np.array(
            [1000.,np.nan,1.,0.,1.,0.,0.])),
            "same_or_more_aggressive_cancellable_quote")
        self.assertEqual(probe_state_class(buy, np.array(
            [np.nan,np.nan,1.,0.,0.,0.,0.])),
            "order_present_not_cancellable")
        self.assertEqual(probe_state_class(buy, np.array(
            [np.nan,np.nan,0.,0.,0.,0.,2.])),
            "inventory_cap_no_quote")
        self.assertEqual(probe_state_class(sell, np.array(
            [np.nan,np.nan,0.,0.,0.,0.,0.])),
            "no_local_quote")
        self.assertEqual(probe_state_class(buy,None),"missing_snapshot")

    def test_probe_schedule_uses_strict_prior_observation_times(self):
        b=[fill(10*NS,1),fill(10*NS,1),fill(11*NS,-1)]
        times,ex,local=prefill_probes(b,9*NS,12*NS)
        self.assertEqual(len(times),4)
        self.assertTrue(np.all(times < 12*NS))
        self.assertEqual(ex[0],10*NS-1_000_000)
        self.assertEqual(local[0],10*NS+2_000_000)
        obs=snapshot_rows_at(times,np.full((len(times),7),np.nan),ex)
        self.assertTrue(all(x is None for x in obs))

    def test_baseline_only_adverse_and_favourable_are_both_counted(self):
        t=10*NS
        base=[fill(t,1,99.9), fill(t+NS,-1,100.1)]
        alternative=[fill(t+NS,-1,100.1)]
        probetimes, exch,local=prefill_probes(base,t-NS,t+2*NS)
        out=np.full((len(probetimes),7),np.nan)
        for j,tt in enumerate(probetimes):
            out[j,6]=0.
            if tt == exch[0] or tt == local[0]:
                out[j,0]=990.  # variant buy at 99.0 < baseline buy 99.9
                out[j,2]=1.
                out[j,4]=1.
        # Sparse artificial exchange quotes, refreshed before t+100 and +500.
        xs=np.array([t-10_000_000,t+90_000_000,t+490_000_000,
                     t+NS-10_000_000,t+NS+90_000_000,t+NS+490_000_000],np.int64)
        mids=np.array([100.,99.,98.,100.,99.,99.])
        got=compare_fill_opportunities(base,alternative,probetimes,out,xs,mids)
        self.assertEqual(got["baseline_only_fill_count"],1)
        self.assertEqual(got["same_exact_exchange_time_and_side_matched_fill_count"],1)
        m=got["groups"]["baseline_only_no_same_time_side_fill"]["horizons"]
        self.assertEqual(m["100"]["n_valid"],1)
        self.assertLess(m["100"]["mean_signed_markout_bps"],0.)
        self.assertEqual(got["baseline_only_exchange_probe_counts"]
                         ["less_aggressive_cancellable_quote"],1)
        self.assertEqual(got["baseline_only_probe_disagreement_count"],0)

    def test_local_observed_delay_reported_with_clock_sign(self):
        x=describe_coverage([fill(10*NS,1,local_lag_ms=3),
                             fill(20*NS,-1,local_lag_ms=-1)])
        self.assertEqual(x["n"],2)
        self.assertAlmostEqual(x["median_local_observation_minus_exchange_ms"],1.)
        self.assertEqual(x["fraction_local_observation_not_before_exchange_timestamp"],.5)


if __name__=="__main__":
    unittest.main()
