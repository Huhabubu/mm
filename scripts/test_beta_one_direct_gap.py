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
        # The HBT book has already consumed the target quote at exact NOW.
        out = causal_price_shift_ticks(
            now, now-25_000_000, 110., 1., ot, 101., gt, bv)
        self.assertAlmostEqual(out, (110.-101.-2.)/TICK)
        self.assertAlmostEqual(
            causal_price_shift_ticks(
                now, now-25_000_000, 110., 0., ot, 101., gt, bv), 0.)
        self.assertAlmostEqual(
            causal_price_shift_ticks(
                now, now-25_000_000, 110., -1., ot, 101., gt, bv), -out)

    def test_future_input_and_stale_bbo_cannot_shift(self):
        now = 70*NS+100_000_000
        ot = np.array([70*NS, 70*NS+200_000_000])
        om = np.array([100., 999.])
        gt = np.array([70*NS, 71*NS])
        bv = np.array([2., 999.])
        normal = causal_price_shift_ticks(
            now, now-25_000_000, 110., 1., ot, 100., gt, bv)
        self.assertAlmostEqual(normal, 80.)
        self.assertEqual(causal_price_shift_ticks(
            now, now-251_000_000, 110., 1., ot, 100., gt, bv), 0.)
        self.assertEqual(causal_price_shift_ticks(
            now, now+1, 110., 1., ot, 100., gt, bv), 0.)
        self.assertEqual(causal_price_shift_ticks(
            now+500_000_000, now+490_000_000, 110., 1., ot, 100., gt, bv), 0.)
        self.assertEqual(causal_price_shift_ticks(
            now, now-25_000_000, 110., 1., ot, 100., gt,
            np.array([np.nan, 999.])), 0.)
        # A future basis value cannot change the signal.
        bv[1] = -100000.
        self.assertAlmostEqual(causal_price_shift_ticks(
            now, now-25_000_000, 110., 1., ot, 100., gt, bv), normal)

    def test_target_catches_up_at_exact_event_timestamp(self):
        # Synthetic regression: a new HBT target quote is already visible.
        now = 70*NS + 100_000_000
        ot = np.array([now-100_000_000, now, now+100_000_000],
                      dtype=np.int64)
        grid = np.array([now-NS], dtype=np.int64)
        basis = np.array([0.])
        # Earlier native target was 100, but the current HBT midpoint is 110.
        shifted = causal_price_shift_ticks(
            now, now-10_000_000, 110., 1., ot, 110., grid, basis)
        self.assertAlmostEqual(shifted, 0.)
        self.assertAlmostEqual(110./TICK + shifted, 110./TICK)
        # Raw BBO can still be older than the fused HBT book; the shift must
        # continue using the latter, with native timestamps only as a gate.
        old_native_only = np.array([now-100_000_000], dtype=np.int64)
        self.assertAlmostEqual(causal_price_shift_ticks(
            now, now-10_000_000, 110., 1.,
            old_native_only, 110., grid, basis), 0.)
        # A genuinely lagging HBT target book still produces a valid shift.
        self.assertAlmostEqual(causal_price_shift_ticks(
            now, now-10_000_000, 110., 1.,
            old_native_only, 100., grid, basis), 10./TICK)

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
