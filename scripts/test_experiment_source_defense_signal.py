"""Strict-causality tests for sparse source defense research signal."""
import unittest
import numpy as np

from experiment_source_defense_signal import (
    past_grid_time_ema, defend_side_bps, source_gate_bps,
    widen_ticks_from_bps, next_defense_state, adaptive_bps_gates,
    queue_shield_allows
)


class SparseSignalTests(unittest.TestCase):
    def test_gap_sign_and_attributed_source(self):
        self.assertEqual(defend_side_bps(5.,2.,0.,1,.5,.2),1)
        self.assertEqual(defend_side_bps(-5.,-2.,0.,2,.5,.2),-1)
        self.assertEqual(defend_side_bps(5.,0.,-2.,3,.5,.2),1)
        self.assertEqual(defend_side_bps(5.,0.,2.,3,.5,.2),0)
        self.assertEqual(defend_side_bps(5.,-2.,0.,1,.5,.2),0)
        self.assertEqual(defend_side_bps(0.,2.,-2.,1,.5,.2),0)

    def test_magnitude_gate(self):
        self.assertFalse(source_gate_bps(.5,2.,1.,1.))
        self.assertFalse(source_gate_bps(1.,.5,1.,1.))
        self.assertTrue(source_gate_bps(1.,1.,1.,1.))

    def test_bps_width_changes_with_mid_price_and_tick(self):
        self.assertEqual(widen_ticks_from_bps(80000.,.1,.5),40)
        self.assertEqual(widen_ticks_from_bps(100.,.01,.5),1)
        self.assertEqual(widen_ticks_from_bps(3000.,.1,1.),3)

    def test_both_low_releases_before_hold_expiry(self):
        side,start,end=next_defense_state(100_000_000,0,-1,-1_000_000_000,
                                          1,.7,.8,.2,100_000_000,75_000_000)
        self.assertEqual(side,1)
        nextside,_,last=next_defense_state(110_000_000,side,start,end,
                                          0,.1,.1,.2,100_000_000,75_000_000)
        self.assertEqual(nextside,0)
        self.assertEqual(last,110_000_000)

    def test_hold_does_not_refresh_indefinitely(self):
        side,start,end=next_defense_state(100_000_000,0,-1,-1_000_000_000,
                                          1,.7,.8,.2,75_000_000,75_000_000)
        self.assertEqual(side,1)
        for now in (120_000_000,140_000_000,160_000_000):
            side,started,end=next_defense_state(now,side,start,end,
                                               0,.4,.1,.2,75_000_000,75_000_000)
            self.assertEqual(side,1)
            self.assertEqual(started,start)
        side,_,end=next_defense_state(180_000_000,side,start,end,
                                         0,.4,.1,.2,75_000_000,75_000_000)
        self.assertEqual(side,0)
        self.assertEqual(end,180_000_000)

    def test_reactivation_respects_cooldown(self):
        side,start,end=next_defense_state(200_000_000,0,-1,180_000_000,
                                          1,.8,.8,.2,75_000_000,75_000_000)
        self.assertEqual(side,0)
        side,start,end=next_defense_state(260_000_000,0,-1,180_000_000,
                                          1,.8,.8,.2,75_000_000,75_000_000)
        self.assertEqual(side,1)

    def test_pretrade_volatility_respects_tick_and_point_three_floor(self):
        # BTC 0.1 USDT tick / 80k mid -> ~0.0125bps; floor is 0.3bps.
        self.assertEqual(adaptive_bps_gates(.1,80000.,.1,2.,1.5),
                         (.3,.3,.3))
        # SOL one price tick ~1bps and higher 100ms SD.
        a,b,f=adaptive_bps_gates(1.,100.,.01,2.,1.5)
        self.assertAlmostEqual(a,2.)
        self.assertAlmostEqual(b,1.5)
        self.assertAlmostEqual(f,1.)

    def test_queue_shield_targets_dangerous_side(self):
        self.assertFalse(queue_shield_allows(1,1.,1.,100.,200.,40.,50.))
        self.assertTrue(queue_shield_allows(1,1.,1.,100.,20.,40.,50.))
        self.assertTrue(queue_shield_allows(-1,-1.,1.,20.,200.,40.,50.))
        self.assertTrue(queue_shield_allows(1,2.5,1.,100.,200.,40.,50.))
        self.assertFalse(queue_shield_allows(-1,-1.,1.,100.,20.,40.,50.))

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
