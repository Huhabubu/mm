"""Regression tests for bounded intensity diagnostics; no downloads or simulation."""
import unittest
import numpy as np
from diagnose_g0_intensity import empirical_curve, fit_curve, fitted_quote, diagnose


class IntensityTests(unittest.TestCase):
    def test_same_price_touch_not_guaranteed_cross(self):
        # synthetic maximum aggressor distances: 0.5, 1, 1.5, 3 ticks
        rows = empirical_curve(np.array([0.5, 1.0, 1.5, 3.0, np.nan]),
                               (0.5, 1, 2, 3), seconds=1)
        self.assertEqual([(x["touch_intervals"],x["cross_intervals"]) for x in rows],
                         [(4,3),(3,2),(1,1),(1,0)])

    def test_depth_unit_means_one_real_tick(self):
        # Synthetic exponential counts at actual, full-tick distances.
        x = np.arange(0.5, 20.5, 0.5)
        a, k = 7., 0.12
        rows = [{"distance_ticks":float(v), "cross_intervals": 100,
                 "cross_per_second":float(a*np.exp(-k*v))} for v in x]
        fitted = fit_curve(rows, 20)
        self.assertEqual(fitted["status"],"ok")
        self.assertAlmostEqual(fitted["k_per_tick"],k, places=10)
        self.assertAlmostEqual(fitted["A_intervals_per_second"],a, places=10)

    def test_large_depth_kept_as_observation_not_clipped(self):
        rows = empirical_curve(np.array([0.5,2.0,100.5]), (1, 35, 100), seconds=600)
        self.assertEqual([x["cross_intervals"] for x in rows], [2,1,1])
        self.assertEqual([x["touch_intervals"] for x in rows], [2,1,1])

    def test_no_stable_fit_marks_missing(self):
        rows = empirical_curve(np.array([0.5,1.0]), np.arange(.5,5.5,.5))
        self.assertEqual(fit_curve(rows,5)["status"],"insufficient_positive_levels")

    def test_fitted_quote_flags_extrapolation(self):
        fit={"status":"ok","A_intervals_per_second":2.0,"k_per_tick":.01,
             "max_fitted_distance_ticks":5}
        r=fitted_quote(fit,10)
        self.assertGreater(r["half_spread_ticks"],5)
        self.assertTrue(r["outside_fitted_distance_range"])

    def test_regression_diagnosis_is_deterministic(self):
        train=np.tile(np.array([1.,2.,5.,10.,20.,50.,100.,np.nan]), 10)
        d=diagnose(train,train,2)
        self.assertEqual(d["train_intervals_with_trades"],70)
        self.assertEqual(d["train_intervals_max_depth_above_35_ticks"],20)
        self.assertEqual(d["selected_depths"]["train"][0]["distance_ticks"],0.5)


if __name__ == "__main__":
    unittest.main()
