"""Tests only the diagnostic interpretation, never changes the production fit."""
import unittest
import numpy as np
from g0_glft_baseline import TRAIN
from diagnose_gate_altcoin_initial_fit import diagnose

SPEC = {"gamma_code":0.00005,"qty":100.0}


class InitialFitDiagnosticTests(unittest.TestCase):
    def test_no_trade_through(self):
        arrivals=np.full(TRAIN,-np.inf)
        changes=np.resize(np.array([0.0,1.0,2.0]),TRAIN)
        d=diagnose(arrivals,changes,SPEC)
        self.assertEqual(d["reason"],"LESS_THAN_5_POSITIVE_DEPTH_BINS")
        self.assertEqual(d["crossed_distance_levels"],0)

    def test_exact_crossing_intervals_gate(self):
        arrivals=np.full(TRAIN,-np.inf)
        arrivals[:9]=8.0
        changes=np.resize(np.array([0.0,1.0,2.0]),TRAIN)
        d=diagnose(arrivals,changes,SPEC)
        self.assertEqual(d["qualifying_intervals"],9)
        self.assertEqual(d["reason"],"LESS_THAN_10_TRADE_THROUGH_INTERVALS")

    def test_flat_midpoint_sigma_zero(self):
        arrivals=np.full(TRAIN,-np.inf)
        arrivals[:180]=np.resize(np.array([2.,4.,8.,12.,20.,30.]),180)
        changes=np.zeros(TRAIN)
        d=diagnose(arrivals,changes,SPEC)
        self.assertEqual(d["reason"],"NON_POSITIVE_OR_NON_FINITE_SIGMA")

    def test_valid_typed_coefficients(self):
        arrivals=np.full(TRAIN,-np.inf)
        arrivals[:180]=np.resize(np.array([2.,4.,8.,12.,20.,30.]),180)
        changes=np.resize(np.array([0.0,1.0,2.0]),TRAIN)
        d=diagnose(arrivals,changes,SPEC)
        self.assertEqual(d["reason"],"ACCEPTED")
        self.assertGreater(d["estimated_k_per_tick"],0)


if __name__=="__main__":
    unittest.main()
