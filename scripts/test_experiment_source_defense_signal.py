"""Strict-causality tests for sparse source defense research signal."""
import unittest
import numpy as np

from experiment_source_defense_signal import (
    past_grid_time_ema, defend_side, source_gate
)


class SparseSignalTests(unittest.TestCase):
    def test_gap_sign_and_attributed_source(self):
        self.assertEqual(defend_side(5,2,0,1),1)
        self.assertEqual(defend_side(-5,-2,0,2),-1)
        self.assertEqual(defend_side(5,0,-2,3),1)
        self.assertEqual(defend_side(5,0,2,3),0)
        self.assertEqual(defend_side(5,-2,0,1),0)
        self.assertEqual(defend_side(0,2,-2,1),0)

    def test_magnitude_gate(self):
        self.assertFalse(source_gate(.5,2,1,1))
        self.assertFalse(source_gate(1,.5,1,1))
        self.assertTrue(source_gate(1,1,1,1))

    def test_event_density_invariance_for_bbo_sampling(self):
        ts=np.arange(1,501,dtype=np.int64)*10_000_000
        bmid=np.where(ts>=2_000_000_000,101.,100.)
        ot=ts.copy()
        om=np.ones(len(ot))*100
        x,y=past_grid_time_ema(ot,om,ts,bmid,.1)
        # Inserting *duplicate* same-price timestamps cannot change the fixed grid.
        dup=np.repeat(np.arange(len(ts)),2)
        d_t,d_b=ts[dup],bmid[dup]
        xx,yy=past_grid_time_ema(ot,om,d_t,d_b,.1)
        np.testing.assert_array_equal(x,xx)
        np.testing.assert_allclose(y,yy,equal_nan=True,atol=1e-11)

    def test_future_mutation_does_not_change_past_basis(self):
        ts=np.arange(1,501,dtype=np.int64)*10_000_000
        bin_mid=np.where(ts>=2_000_000_000,101.,100.)
        om=np.ones(len(ts))*100
        a,z=past_grid_time_ema(ts,om,ts+1,bin_mid,.1)
        future=bin_mid.copy()
        future[ts>3_000_000_000]+=9999
        b,y=past_grid_time_ema(ts,om,ts+1,future,.1)
        np.testing.assert_array_equal(a,b)
        np.testing.assert_allclose(z[a<=3_000_000_000],
                                   y[b<=3_000_000_000],equal_nan=True)

    def test_ema_eventually_fades_shock(self):
        ts=np.arange(1,801,dtype=np.int64)*10_000_000
        gap=np.where((ts>=2_000_000_000)&(ts<3_000_000_000),10.,0.)
        g,b=past_grid_time_ema(ts,np.zeros(len(ts)),ts,gap,.2)
        peak=b[np.searchsorted(g,3_000_000_000)-1]
        later=b[np.searchsorted(g,5_000_000_000)]
        self.assertTrue(np.isfinite(peak))
        self.assertGreater(peak,5.)
        self.assertLess(abs(later),.1)

    def test_ema_requires_historical_warmup(self):
        ts=np.arange(1,300,dtype=np.int64)*10_000_000
        g,b=past_grid_time_ema(ts,np.ones(len(ts)),ts,np.ones(len(ts))*2,.5)
        self.assertFalse(np.any(np.isfinite(b)))


if __name__=="__main__":
    unittest.main()
