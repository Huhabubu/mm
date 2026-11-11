"""Synthetic tests for causal P90/P50 gap gating and placebo symmetry.

All prices are manufactured for code verification, not empirical evidence.
"""
import unittest

import numpy as np

from g0_glft_event_driven import filtered_gap_shift, causal_price_shift_ticks
from run_beta_one_direct_gap import pretrade_gap_thresholds, causal_60s_basis, NS


class SignalHysteresisTests(unittest.TestCase):
    def test_extreme_entry_refresh_deadzone_release_and_direction_flip(self):
        t0 = 1_000_000_000
        t = t0
        held = 0.
        last = -1
        def transition(at, proposed, previous, prev_update):
            return filtered_gap_shift(at, proposed, previous, prev_update,
                                      40., 10., 10., 250_000_000)

        held, last = transition(t, 35., held, last)
        self.assertEqual((held, last), (0., -1))  # below P90 cannot trigger
        held, last = transition(t+10_000_000, 42., held, last)
        self.assertEqual((held, last), (42., t+10_000_000))
        held, last = transition(t+20_000_000, 50., held, last)
        self.assertEqual(held, 42.)  # less than 250ms, cannot re-quote for 8 ticks
        held, last = transition(t+200_000_000, 80., held, last)
        self.assertEqual(held, 42.)  # even >10tick move within hold is ignored
        held, last = transition(t+300_000_000, 80., held, last)
        self.assertEqual(held, 80.)
        held, last = transition(t+310_000_000, 60., held, last)
        self.assertEqual(held, 80.)
        held, last = transition(t+320_000_000, -20., held, last)
        self.assertEqual(held, 0.)  # reversed but below extreme activation
        held, last = transition(t+330_000_000, -42., held, last)
        self.assertEqual(held, -42.)
        held, last = transition(t+340_000_000, 0., held, last)
        self.assertEqual(held, 0.)  # stale/missing gap releases promptly

    def test_strong_opposite_sign_and_plausible_quote_moves(self):
        t = 10*NS
        a, last = filtered_gap_shift(t, 70., 0., -1, 40., 10., 10., 250_000_000)
        self.assertEqual(a, 70.)
        b, last = filtered_gap_shift(t+50_000_000, -60., a, last,
                                     40., 10., 10., 250_000_000)
        self.assertEqual(b, -60.)  # dangerous reverse gap cannot remain old sign
        c, last = filtered_gap_shift(t+55_000_000, -9., b, last,
                                     40., 10., 10., 250_000_000)
        self.assertEqual(c, 0.)

    def test_pretrade_quantiles_cannot_see_after_trading_start(self):
        # All 1-second quote-grid values are valid because 100ms source data.
        t = np.arange(7000, dtype=np.int64)*100_000_000 + 10_000_000
        ot = t.copy()
        bt = t + 5_000_000
        om = np.full(len(t), 100., dtype=np.float64)
        bm = 101. + 0.2*np.sin(np.arange(len(t))/15.) + 0.1*np.cos(np.arange(len(t))/41.)
        baset, basis = causal_60s_basis(ot, om, bt, bm)
        original = pretrade_gap_thresholds(ot, om, bt, bm, baset, basis, 0)
        self.assertGreater(original["training_clock_samples"], 300)
        self.assertLess(original["release_ticks"], original["activate_ticks"])
        self.assertEqual(original["refresh_min_interval_ms"], 250.)
        self.assertGreater(original["abs_gap_p90_usdt_per_btc"], 0.)
        bm2 = bm.copy()
        bm2[t >= 600*NS] += 100000.
        baset2, basis2 = causal_60s_basis(ot, om, bt, bm2)
        later = pretrade_gap_thresholds(ot, om, bt, bm2, baset2, basis2, 0)
        self.assertEqual(original, later)

    def test_placebo_shift_opposite_sign_with_equal_magnitude(self):
        now = 110*NS
        ot = np.array([now-100_000_000], dtype=np.int64)
        om = np.array([100.], dtype=np.float64)
        basis_t = np.array([now-NS], dtype=np.int64)
        basis = np.array([1.], dtype=np.float64)
        positive = causal_price_shift_ticks(now, now-25_000_000,
                                            105., 1., ot, om, basis_t, basis)
        negative = causal_price_shift_ticks(now, now-25_000_000,
                                            105., -1., ot, om, basis_t, basis)
        self.assertEqual(positive, -negative)
        self.assertGreater(positive, 0.)


if __name__ == "__main__":
    unittest.main()
