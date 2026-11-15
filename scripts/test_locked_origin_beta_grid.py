"""Deterministic tests for the new locked-origin beta-gradient signal.

Toy price tapes below are only test fixtures, not observed trading results.
"""
import unittest
import numpy as np

from locked_origin_basis import (
    NS, REFIT_NS, WINDOW_NS, collapse_same_timestamp, event_tape,
    rolling_time_weighted_basis, candidate_gate, locked_origin_event_series,
    prepare_signal,
)
from g0_glft_event_driven import locked_origin_quote_shift


class LockedOriginTests(unittest.TestCase):
    def test_time_weighted_exact_mean_and_population_std(self):
        ts=np.array([0,300*NS,600*NS,700*NS],dtype=np.int64)
        gaps=np.array([2.,4.,999.,999.])
        grid,mean,sigma,coverage=rolling_time_weighted_basis(
            ts,gaps,600*NS,605*NS)
        self.assertEqual(len(grid),1)
        self.assertAlmostEqual(mean[0],3.)
        self.assertAlmostEqual(sigma[0],1.)
        self.assertAlmostEqual(coverage[0],1.)
        # A future quote at t=600s cannot modify a 600s strictly-past window.
        ts2=ts.copy()
        gap2=gaps.copy(); gap2[2:]=-5000.
        _,mean2,sigma2,_=rolling_time_weighted_basis(
            ts2,gap2,600*NS,605*NS)
        np.testing.assert_array_equal(mean,mean2)
        np.testing.assert_array_equal(sigma,sigma2)

    def test_same_timestamp_last_state_and_cross_venue_visibility(self):
        bt=np.array([100,100,200],dtype=np.int64)
        bm=np.array([100.,101.,102.])
        ot=np.array([100,150],dtype=np.int64)
        om=np.array([98.,99.])
        t,b,m=event_tape(bt,bm,ot,om)
        self.assertTrue(np.all(np.diff(t)>0))
        self.assertEqual(t.tolist(),[101,151,200])
        self.assertEqual(b.tolist(),[101.,101.,102.])
        self.assertEqual(m.tolist(),[98.,99.,99.])

    def test_anchor_freeze_trigger_and_reentry(self):
        start=600*NS
        ts=start+np.arange(1,8,dtype=np.int64)*1000
        b=np.array([100.,103.,105.,105.,101.,105.,106.])
        m=np.array([100.,100.,100.,102.5,100.5,104.,100.])
        grid=np.array([start],dtype=np.int64)
        mean=np.array([0.])
        sigma=np.array([2.])
        out=locked_origin_event_series(ts,b,m,grid,mean,sigma,start)
        _,ab,am,ready,passed,over=out
        self.assertTrue(np.all(ready))
        self.assertTrue(np.all(ab[:4]==100.))
        self.assertTrue(np.all(am[:4]==100.))
        self.assertFalse(passed[0])
        self.assertFalse(passed[1])  # 1.5sigma, anchor already locked
        self.assertTrue(passed[2])   # 2.5sigma, Binance did all of the move
        self.assertFalse(passed[3])  # 1.25sigma, still locked
        self.assertAlmostEqual(ab[4],101.)
        self.assertAlmostEqual(am[4],100.5)
        self.assertAlmostEqual(ab[5],105.)
        self.assertAlmostEqual(am[5],104.)
        self.assertFalse(passed[6])  # Binance move 1 < target move 4
        self.assertTrue(over[6])

    def test_no_initial_anchor_outside_one_sigma(self):
        t=np.array([600*NS+1,600*NS+2,600*NS+3],dtype=np.int64)
        b=np.array([106.,100.,105.])
        m=np.array([100.,100.,100.])
        out=locked_origin_event_series(t,b,m,np.array([600*NS]),np.array([0.]),
                                      np.array([2.]),600*NS)
        self.assertFalse(out[3][0])
        self.assertFalse(out[4][0])
        self.assertTrue(out[3][1])
        self.assertTrue(out[4][2])

    def test_direction_and_strict_boundaries(self):
        self.assertTrue(candidate_gate(5.,2.,+3.,+1.))
        self.assertTrue(candidate_gate(-5.,2.,-3.,-1.))
        self.assertFalse(candidate_gate(5.,2.,-3.,-1.))
        self.assertFalse(candidate_gate(-5.,2.,+3.,+1.))
        self.assertFalse(candidate_gate(5.,2.,+1.,+1.))  # tie excluded
        self.assertFalse(candidate_gate(1.96*2.,2.,+5.,0.))  # strict >
        self.assertFalse(candidate_gate(5.,0.,+5.,0.))

    def test_quote_shift_uses_live_hbt_target_mid_and_beta(self):
        grid=np.array([1000],dtype=np.int64)
        mu=np.array([2.])
        std=np.array([1.])
        ts=np.array([1001],dtype=np.int64)
        ab=np.array([100.])
        am=np.array([99.])
        flag=np.array([True])
        for beta in (0.,.25,.5,.75,1.):
            # live spread abnormal = 106-100-2=4 >1.96sigma;
            # Binance +6 > target +1 => qualifies
            result=locked_origin_quote_shift(1002,beta,106.,100.,grid,mu,std,
                                             ts,ab,am,flag,.1)
            self.assertAlmostEqual(result,beta*40.)
        self.assertEqual(locked_origin_quote_shift(1002,1.,106.,105.,grid,mu,std,
                                                   ts,ab,am,flag,.1),0.)
        self.assertEqual(locked_origin_quote_shift(1002,1.,106.,100.,grid,mu,std,
                                                   ts,ab,am,np.array([False]),.1),0.)
        self.assertEqual(locked_origin_quote_shift(1000,1.,106.,100.,grid,mu,std,
                                                   ts,ab,am,flag,.1),0.)

    def test_past_full_tape_not_future(self):
        # Test that preparing signal ignores future within the first basis
        # estimate. First trade after 600s and two stable native markets.
        t=np.arange(0,610*NS,100_000_000,dtype=np.int64)
        bt=t+2_500_001
        ot=t.copy()
        bb=np.full(len(t),100.)
        mm=np.full(len(t),98.)
        mm[t>=300*NS]=97.
        sig=prepare_signal(bt,bb,ot,mm,600*NS,605*NS)
        self.assertEqual(len(sig["basis_mean"]),1)
        self.assertEqual(len(sig["basis_sigma"]),1)
        self.assertAlmostEqual(sig["basis_mean"][0],2.5,places=3)
        self.assertAlmostEqual(sig["basis_sigma"][0],.5,places=3)
        changed=bb.copy()
        changed[t>=600*NS]=1000.
        sig2=prepare_signal(bt,changed,ot,mm,600*NS,605*NS)
        np.testing.assert_array_equal(sig["basis_mean"],sig2["basis_mean"])
        np.testing.assert_array_equal(sig["basis_sigma"],sig2["basis_sigma"])


if __name__=="__main__":
    unittest.main()
