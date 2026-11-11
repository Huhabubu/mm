"""Synthetic regression and timing tests: numbers are NOT real-market findings."""
import unittest

import numpy as np

from gap_event_regression import event_gap_samples, fit_beta, score, subset


NS = 1_000_000_000


def tape():
    ot = (np.arange(15, dtype=np.int64) * NS) + 100_000_000
    bt = (np.arange(15, dtype=np.int64) * NS) + 200_000_001
    om = np.full(15, 100., dtype=np.float64)
    bm = np.full(15, 102., dtype=np.float64)
    return ot, om, bt, bm


class EventGapTests(unittest.TestCase):
    def test_constant_cross_venue_basis_does_not_create_gap(self):
        ot, om, bt, bm = tape()
        s = event_gap_samples(ot, om, bt, bm, basis_seconds=3,
                              tau_ms=100, max_age_ms=2000)
        self.assertGreater(len(s["t_ns"]), 10)
        self.assertTrue(np.allclose(s["gap_usdt_per_btc"], 0.))
        self.assertTrue(np.allclose(s["response_usdt_per_btc"], 0.))
        self.assertEqual(set(s["source"]), {0, 1})

    def test_one_price_jump_survives_strict_past_basis(self):
        ot, om, bt, bm = tape()
        bm[8] = 112.0
        s = event_gap_samples(ot, om, bt, bm, basis_seconds=3,
                              tau_ms=100, max_age_ms=2000)
        t = int(bt[8])
        row = np.flatnonzero((s["t_ns"] == t) & (s["source"] == 1))
        self.assertEqual(len(row), 1)
        self.assertAlmostEqual(s["gap_usdt_per_btc"][row[0]], 10.)
        self.assertAlmostEqual(s["basis_usdt_per_btc"][row[0]], 2.)

    def test_duplicate_source_timestamp_does_not_peek_later_row(self):
        ot, om, bt, bm = tape()
        bt = np.insert(bt, 9, bt[8])
        bm = np.insert(bm, 9, 112.)
        bm[8] = 104.
        s = event_gap_samples(ot, om, bt, bm, basis_seconds=3,
                              tau_ms=100, max_age_ms=2000)
        rows = np.flatnonzero((s["t_ns"] == bt[8]) & (s["source"] == 1))
        self.assertEqual(len(rows), 2)
        self.assertAlmostEqual(s["gap_usdt_per_btc"][rows[0]], 2.)
        self.assertAlmostEqual(s["gap_usdt_per_btc"][rows[1]], 10.)

    def test_future_tape_change_does_not_modify_prior_gap(self):
        ot, om, bt, bm = tape()
        base = event_gap_samples(ot, om, bt, bm, basis_seconds=3,
                                 tau_ms=100, max_age_ms=2000)
        om2 = om.copy()
        bm2 = bm.copy()
        om2[13:] += 20.
        bm2[13:] -= 5.
        altered = event_gap_samples(ot, om2, bt, bm2, basis_seconds=3,
                                    tau_ms=100, max_age_ms=2000)
        a = base["t_ns"] < 11 * NS
        b = altered["t_ns"] < 11 * NS
        self.assertTrue(np.array_equal(base["t_ns"][a], altered["t_ns"][b]))
        self.assertTrue(np.allclose(base["gap_usdt_per_btc"][a],
                                    altered["gap_usdt_per_btc"][b]))
        self.assertTrue(np.allclose(base["response_usdt_per_btc"][a],
                                    altered["response_usdt_per_btc"][b]))

    def test_fitted_beta_is_dimensionless_and_intercept_checked(self):
        x = np.linspace(-10., 10., 500)
        y = 0.25 + 0.5 * x
        sample = {"t_ns": np.arange(500, dtype=np.int64) * 100_000_000,
                  "gap_usdt_per_btc": x, "response_usdt_per_btc": y,
                  "source": np.zeros(500, np.int8)}
        f = fit_beta(sample)
        self.assertAlmostEqual(f["beta"], .5, places=10)
        self.assertAlmostEqual(f["alpha_usdt_per_btc"], .25, places=10)
        self.assertGreater(score(sample, f)["r2_vs_zero"], .99)
        sliced = subset(sample, sample["source"] == 0)
        self.assertEqual(len(sliced["t_ns"]), 500)

    def test_stale_market_disqualifies_samples(self):
        ot, om, bt, bm = tape()
        # Quotes one second apart, 1ms freshness threshold cannot support
        # complete 1-second history on the exogenous grid.
        with self.assertRaises(ValueError):
            event_gap_samples(ot, om, bt, bm, basis_seconds=3,
                              tau_ms=100, max_age_ms=1)


if __name__ == "__main__":
    unittest.main()
