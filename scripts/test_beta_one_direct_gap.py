"""Deterministic full abnormal-gap beta=1 quote shift / no-lookahead checks.

Synthetic quotes in these tests are NOT evidence of strategy profitability.
"""
import unittest

import numpy as np

from g0_glft_baseline import TICK
from g0_glft_event_driven import causal_price_shift_ticks
from run_beta_one_direct_gap import causal_60s_basis, quote_gap_coverage, NS


class DirectGapTests(unittest.TestCase):
    def test_beta_one_is_abnormal_gap_in_real_ticks(self):
        now = 70*NS+100_000_000
        ot = np.array([70*NS, 70*NS+100_000_000, 70*NS+200_000_000])
        om = np.array([100., 101., 102.])
        gt = np.array([69*NS, 70*NS, 71*NS])
        bv = np.array([1., 2., 999.])
        # At exact NOW the OKX new quote 101 cannot enter strict-left.
        out = causal_price_shift_ticks(
            now, now-25_000_000, 110., 1., ot, om, gt, bv)
        self.assertAlmostEqual(out, (110.-100.-2.)/TICK)
        self.assertAlmostEqual(
            causal_price_shift_ticks(
                now, now-25_000_000, 110., 0., ot, om, gt, bv), 0.)
        self.assertAlmostEqual(
            causal_price_shift_ticks(
                now, now-25_000_000, 110., -1., ot, om, gt, bv), -out)

    def test_future_input_and_stale_bbo_cannot_shift(self):
        now = 70*NS+100_000_000
        ot = np.array([70*NS, 70*NS+200_000_000])
        om = np.array([100., 999.])
        gt = np.array([70*NS, 71*NS])
        bv = np.array([2., 999.])
        normal = causal_price_shift_ticks(
            now, now-25_000_000, 110., 1., ot, om, gt, bv)
        self.assertAlmostEqual(normal, 80.)
        self.assertEqual(causal_price_shift_ticks(
            now, now-251_000_000, 110., 1., ot, om, gt, bv), 0.)
        self.assertEqual(causal_price_shift_ticks(
            now, now+1, 110., 1., ot, om, gt, bv), 0.)
        self.assertEqual(causal_price_shift_ticks(
            now+500_000_000, now+490_000_000, 110., 1., ot, om, gt, bv), 0.)
        self.assertEqual(causal_price_shift_ticks(
            now, now-25_000_000, 110., 1., ot, om, gt,
            np.array([np.nan, 999.])), 0.)
        # A future local OKX BBO or a future basis value cannot change shift.
        om[1] = 150000.
        bv[1] = -100000.
        self.assertAlmostEqual(causal_price_shift_ticks(
            now, now-25_000_000, 110., 1., ot, om, gt, bv), normal)

    def test_causal_60s_basis_matches_simple_constant_spread(self):
        times = np.arange(7100, dtype=np.int64)*100_000_000 + 10_000_000
        ot, om = times, np.full(len(times), 100.)
        bt, bm = times + 10_000_000, np.full(len(times), 103.)
        grid, basis = causal_60s_basis(ot, om, bt, bm)
        self.assertGreater(np.isfinite(basis).sum(), 600)
        self.assertAlmostEqual(basis[np.flatnonzero(np.isfinite(basis))[0]], 3.)
        audit = quote_gap_coverage(ot, om, bt, bm, grid, basis,
                                    600*NS+500_000_000, 605*NS)
        self.assertGreater(audit["usable_gap_points"], 0)
        self.assertLess(audit["mean_abs_gap_usdt_per_btc"], 1e-10)

    def test_insufficient_basis_history_is_rejected(self):
        ot = np.arange(10, dtype=np.int64)*NS+10_000_000
        with self.assertRaises(ValueError):
            causal_60s_basis(ot, np.ones(10), ot+1, np.ones(10))


if __name__ == "__main__":
    unittest.main()
