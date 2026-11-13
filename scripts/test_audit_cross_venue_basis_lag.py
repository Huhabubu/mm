"""Focused causal/paired-evaluation tests for the read-only basis-lag audit."""
import unittest

import numpy as np

from audit_cross_venue_basis_lag import (
    aligned_series, diagnostic_metrics, future_okx_bps, past_indices,
    prediction_table,
)
from run_beta_one_direct_gap import causal_60s_basis

NS = 1_000_000_000


class BasisLagTests(unittest.TestCase):
    def synthetic(self):
        indices = np.arange(480, dtype=np.int64)
        ts = indices * NS + 900_000_000
        okx = np.full(len(ts), 100.0)
        shock = np.where((indices >= 350) & (indices < 380), 20.0, 0.0)
        binance = okx + shock
        return ts, okx, ts.copy(), binance

    def test_past_strict_vs_equal(self):
        stamps = np.array([10, 20, 20, 30], dtype=np.int64)
        self.assertEqual(int(past_indices(stamps, np.array([20]))[0]), 0)
        self.assertEqual(int(past_indices(stamps, np.array([20]), True)[0]), 2)

    def test_60s_lags_a_completed_shock_more_than_300s(self):
        ot, om, bt, bm = self.synthetic()
        t60, b60 = causal_60s_basis(ot, om, bt, bm, 60)
        t300, b300 = causal_60s_basis(ot, om, bt, bm, 300)
        clock = np.array([381 * NS], dtype=np.int64)
        x = aligned_series(clock, ot, om, bt, bm, t60, b60, t300, b300)
        self.assertTrue(x["valid"][0])
        self.assertAlmostEqual(x["d"][0], 0.0)
        self.assertLess(x["g60"][0], -8.0)
        self.assertLess(x["g300"][0], 0.0)
        self.assertGreater(abs(x["g60"][0]), 4 * abs(x["g300"][0]))

    def test_future_edits_cannot_change_past_signal(self):
        ot, om, bt, bm = self.synthetic()
        clock = np.array([381 * NS], dtype=np.int64)

        def get_signal(data):
            t60, b60 = causal_60s_basis(ot, om, bt, data, 60)
            t300, b300 = causal_60s_basis(ot, om, bt, data, 300)
            return aligned_series(clock, ot, om, bt, data, t60, b60, t300, b300)

        a = get_signal(bm)
        altered = bm.copy()
        altered[bt > clock[0]] += 999.0
        b = get_signal(altered)
        for name in ("d", "g60", "g300", "b60", "b300"):
            np.testing.assert_array_equal(a[name], b[name])

    def test_future_asof_endpoint_uses_no_later_quote(self):
        clock = np.array([100_000_000], dtype=np.int64)
        ot = np.array([0, 105_000_000, 112_000_000], dtype=np.int64)
        om = np.array([100., 101., 102.])
        valid5, y5 = future_okx_bps(clock, 5, ot, om, np.array([100.]))
        valid10, y10 = future_okx_bps(clock, 10, ot, om, np.array([100.]))
        self.assertTrue(valid5[0] and valid10[0])
        self.assertAlmostEqual(y5[0], 100.0)
        self.assertAlmostEqual(y10[0], 100.0)

    def test_paired_horizons_do_not_change_with_signal(self):
        ot, om, bt, bm = self.synthetic()
        t60, b60 = causal_60s_basis(ot, om, bt, bm, 60)
        t300, b300 = causal_60s_basis(ot, om, bt, bm, 300)
        clock = np.arange(381, 384, dtype=np.int64) * NS
        x = aligned_series(clock, ot, om, bt, bm, t60, b60, t300, b300)
        a = prediction_table(clock, ot, om, x, .1)
        for h in ("5", "10", "25", "50"):
            n = a[h]["joint_valid_points"]
            self.assertEqual(n, 3)
            self.assertEqual(a[h]["variants"]["g60"]["nonzero_label_and_signal_points"], 0)
            self.assertIsNone(a[h]["variants"]["g60"]["direction_hit_fraction_given_nonzero"])

    def test_proxy_is_labelled_not_truth(self):
        x = {"valid": np.array([True, True]),
             "g60": np.array([-3., 2.]),
             "g300": np.array([0., -1.]),
             "b60": np.array([3., -2.]),
             "b300": np.array([0., 1.])}
        d = diagnostic_metrics(x, 1.0)
        self.assertEqual(d["joint_valid_100ms_points"], 2)
        self.assertEqual(d["near_300s_but_strong_60s_points"], 2)
        self.assertEqual(d["opposite_60s_vs_300s_residual_points"], 1)


if __name__ == "__main__":
    unittest.main()
