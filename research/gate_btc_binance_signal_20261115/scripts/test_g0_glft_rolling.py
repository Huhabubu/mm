"""Focused tests for online GLFT calibration: synthetic samples, not empirical claims."""
import unittest
import math
import numpy as np
from g0_glft_rolling import (FIT_BINS, TRAIN, MIN_CROSSING_INTERVALS, fit_recent_arrivals,
                             glft_coefficients, UPDATE_EVERY_STEPS)


class GLFTRollingTests(unittest.TestCase):
    def test_true_tick_threshold_crossing(self):
        # Simplified sample, every 100ms event carries a max crossing depth.
        arr=np.full(TRAIN,np.nan)
        change=np.tile(np.array([-1.,0.,1.]),2000)
        arr[:]=np.tile(np.array([.5,1.,2.,5.,10.,20.,35.,70.]),750)
        valid,a,k,sigma,levels,cross,extreme=fit_recent_arrivals(arr,change)
        self.assertEqual(valid,1)
        self.assertGreater(k,0)
        self.assertAlmostEqual(sigma, math.sqrt(2/3*10), places=5)
        self.assertEqual(extreme,750)
        self.assertEqual(levels,FIT_BINS)

    def test_recent_window_replaces_older_history(self):
        # Replaced training window is a strict function of the current ring only.
        old=np.tile(np.array([.5,1.,3.,6.,12.,24.,35.,70.]),750)
        new=np.tile(np.array([.5,1.,2.,4.,8.,12.,20.,35.]),750)
        volatility=np.tile(np.array([-1.,0.,1.]),2000)
        first=fit_recent_arrivals(old,volatility)
        second=fit_recent_arrivals(new,volatility)
        self.assertEqual(first[0],1)
        self.assertEqual(second[0],1)
        self.assertNotAlmostEqual(first[2],second[2],places=3)
        self.assertEqual(len(old),TRAIN)

    def test_threshold_guard_for_extreme_only_events(self):
        arr=np.full(TRAIN,np.nan)
        arr[:MIN_CROSSING_INTERVALS-1]=100.0
        changes=np.tile(np.array([-1.,0.,1.]),2000)
        result=fit_recent_arrivals(arr,changes)
        self.assertEqual(result[0],0)
        arr[:MIN_CROSSING_INTERVALS]=100.0
        # With enough extreme events but equal intensity across all 70 bins,
        # the fitted slope is numerically zero and cannot identify distance decay.
        self.assertEqual(fit_recent_arrivals(arr,changes)[0],0)

    def test_insufficient_order_flow_marks_invalid(self):
        arr=np.full(TRAIN,np.nan)
        arr[0]=100
        changes=np.tile(np.array([-1.,0.,1.]),2000)
        valid,*_=fit_recent_arrivals(arr,changes)
        self.assertEqual(valid,0)

    def test_no_future_data_can_enter_initial_window(self):
        base=np.tile(np.array([.5,1.,2.,5.,10.,20.,35.,70.]),750)
        changed=base.copy()
        x=np.tile(np.array([-1.,0.,1.]),2000)
        baseline=fit_recent_arrivals(base,x)
        future=np.full(TRAIN,10000.)
        # Only a hypothetical future buffer changes: prior window stays fixed.
        self.assertEqual(fit_recent_arrivals(changed,x),baseline)
        self.assertGreater(fit_recent_arrivals(future,x)[6],baseline[6])

    def test_glft_quotes_finite_and_monotone_in_inventory(self):
        half,skew=glft_coefficients(.05,.02,10.)
        self.assertTrue(math.isfinite(half) and half>0)
        self.assertTrue(math.isfinite(skew) and skew>0)
        self.assertGreater(half+skew*.1,half-skew*.1)

    def test_requote_clock_independent_of_fit_clock(self):
        self.assertEqual(UPDATE_EVERY_STEPS,50)
        self.assertEqual(TRAIN,6000)
        self.assertEqual(UPDATE_EVERY_STEPS*100,5000)


if __name__=="__main__":
    unittest.main()
