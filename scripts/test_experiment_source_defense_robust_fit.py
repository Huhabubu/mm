"""Synthetic causal prior regression checks; no fabricated HBT results."""
import unittest

import numpy as np

from experiment_source_defense_robust_fit import (
    sufficient_stats, research_fit, chronological_reliability,
    fit_rolling_research_3level
)


def three_level_stats():
    a=np.full(6000,np.nan)
    a[:10]=2.
    a[10:30]=1.
    v=np.sin(np.linspace(0,6,6000))
    z=sufficient_stats(a,v)
    assert z["positive_distance_levels"]==3
    return z


class ResearchRobustFitTests(unittest.TestCase):
    def test_sparse_requires_earlier_prior(self):
        z=three_level_stats()
        a=research_fit(z)
        self.assertFalse(a["valid"])
        self.assertEqual(a["reason"],"sparse_levels_without_causal_prior")
        b=research_fit(z,prior_k=.5)
        self.assertTrue(b["valid"])
        self.assertEqual(b["method"],"sparse_causal_prior_shrinkage")
        self.assertTrue(0<b["k_per_tick"]<5)

    def test_live_numba_20min_three_levels_requires_prior(self):
        arrivals=np.full(12000,np.nan)
        arrivals[:10]=2.0
        arrivals[10:30]=1.0
        change=np.sin(np.linspace(0,50,12000))
        missing=fit_rolling_research_3level(arrivals,change,np.nan)
        prior=fit_rolling_research_3level(arrivals,change,.5)
        self.assertEqual(missing[0],0)
        self.assertEqual(prior[0],1)
        self.assertEqual(prior[4],3)
        self.assertGreater(prior[1],0.)
        self.assertGreater(prior[2],0.)
        self.assertGreater(prior[3],0.)

    def test_no_future_prior_can_rescue_earliest_day(self):
        z=three_level_stats()
        late=sufficient_stats(np.r_[np.ones(10)*5, np.ones(20)*3,
                                      np.ones(5970)*1],np.ones(6000)*.01)
        # The earliest sparse day fails independently of how well
        # the next day identifies a slope.
        result=chronological_reliability([
            ("2026-07-01",late),("2026-06-01",z)])
        self.assertEqual(result[0]["day"],"2026-06-01")
        self.assertFalse(result[0]["valid"])
        self.assertIsNone(result[0]["prior_source_day"])

    def test_window_seconds_from_observed_rows(self):
        z=sufficient_stats(np.ones(12000)*3.,np.ones(12000)*.4)
        self.assertAlmostEqual(z["window_seconds"],1200.)
        self.assertEqual(z["samples"],12000)

    def test_invalid_bound_on_k_is_fail_closed(self):
        z=three_level_stats()
        v=research_fit(z,prior_k=1e20)
        self.assertFalse(v["valid"])

    def test_cumulative_counts_do_not_fabricate_fills(self):
        a=np.full(6000,np.nan)
        a[:12]=1.6
        z=sufficient_stats(a,np.sin(np.arange(6000)*.001))
        self.assertEqual(z["crossing_100ms_intervals"],12)
        self.assertEqual(z["positive_distance_levels"],3)
        self.assertEqual(z["counts_by_distance"][0],12)
        self.assertEqual(z["counts_by_distance"][2],12)


if __name__=="__main__":
    unittest.main()
