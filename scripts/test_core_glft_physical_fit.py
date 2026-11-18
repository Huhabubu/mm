"""Offline deterministic unit tests for physical GLFT two-sided Poisson arrivals."""
import math
import unittest
import numpy as np

from g0_glft_baseline import TRAIN
from g0_glft_rolling import (
    fit_physical_1tick_poisson, physical_poisson_rates,
    PHYSICAL_FIT_BINS, PHYSICAL_MIN_FIT_BINS,
)


class PhysicalFitTest(unittest.TestCase):
    def test_two_sides_do_not_overwrite_same_interval(self):
        bid=np.full(TRAIN,np.nan)
        ask=np.full(TRAIN,np.nan)
        bid[0]=.5
        ask[0]=.5
        rates,crossed,extreme=physical_poisson_rates(bid,ask)
        self.assertEqual(crossed,2)
        self.assertEqual(extreme,0)
        self.assertAlmostEqual(rates[0],-math.log1p(-2/(2*TRAIN))/.1)
        self.assertEqual(rates[1],0)

    def test_physical_1tick_thresholds_include_touch(self):
        bid=np.full(TRAIN,np.nan)
        ask=np.full(TRAIN,np.nan)
        bid[:20]=.5
        ask[:20]=1.5
        rates,crossed,_=physical_poisson_rates(bid,ask)
        self.assertEqual(crossed,40)
        self.assertGreater(rates[0],rates[1])
        self.assertEqual(rates[2],0)
        self.assertEqual(len(rates),35)

    def test_slope_positive_with_real_variation(self):
        bid=np.full(TRAIN,np.nan)
        ask=np.full(TRAIN,np.nan)
        bid[:100]=.5
        ask[:100]=.5
        bid[:70]=1.5
        ask[:60]=1.5
        bid[:30]=2.5
        ask[:25]=2.5
        bid[:12]=3.5
        ask[:10]=3.5
        changes=np.sin(np.arange(TRAIN,dtype=float)/11.0)
        valid,a,k,sigma,bins,crossed,_=fit_physical_1tick_poisson(
            bid,ask,changes)
        self.assertEqual(valid,1)
        self.assertGreaterEqual(bins,PHYSICAL_MIN_FIT_BINS)
        self.assertGreater(k,0)
        self.assertGreater(a,0)
        self.assertGreater(sigma,0)
        self.assertGreater(crossed,10)

    def test_empty_or_few_bins_fail_closed(self):
        bid=np.full(TRAIN,np.nan)
        ask=np.full(TRAIN,np.nan)
        bid[:50]=.5
        ask[:50]=.5
        changes=np.sin(np.arange(TRAIN,dtype=float)/11.0)
        valid,*_=fit_physical_1tick_poisson(bid,ask,changes)
        self.assertEqual(valid,0)

    def test_hit_prob_clamps(self):
        bid=np.full(TRAIN,35.5)
        ask=np.full(TRAIN,35.5)
        rates,crossed,extreme=physical_poisson_rates(bid,ask)
        self.assertEqual(crossed,12000)
        self.assertEqual(extreme,12000)
        self.assertTrue(np.isfinite(rates).all())
        self.assertTrue(np.all(rates>0))
        self.assertAlmostEqual(rates[0],-math.log(.001)/.1,places=8)


if __name__=="__main__":
    unittest.main()
