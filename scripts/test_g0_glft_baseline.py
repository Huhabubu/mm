"""G0 parameter, units and formula tests (no network or simulated fills)."""
import unittest
import numpy as np
from g0_glft_baseline import (TICK, LOT, CTVAL, calibrate,
                              quote_coefficients, validate_current_spec)


class G0Tests(unittest.TestCase):
    def test_contract_conversion(self):
        self.assertAlmostEqual(LOT*CTVAL, 0.0001)
        self.assertAlmostEqual(0.1*CTVAL*77500, 77.5)
        self.assertAlmostEqual(TICK, 0.1)

    def test_live_observation_cannot_replace_historical_snapshot(self):
        spec = {"status":"ok","fields":{"ctVal":"0.01","ctMult":"1",
                "ctValCcy":"BTC","ctType":"linear","tickSz":"0.1",
                "lotSz":"0.01","minSz":"0.01"}}
        self.assertTrue(validate_current_spec(spec))
        self.assertFalse(validate_current_spec({"status":"unavailable"}))
        spec["fields"]["ctVal"] = "0.001"
        with self.assertRaisesRegex(ValueError, "ctVal"):
            validate_current_spec(spec)

    def test_glft_coefficients_have_inventory_risk_slope(self):
        half, skew = quote_coefficients(2.0, 0.05, 10)
        self.assertGreater(half, 0)
        self.assertGreater(skew, 0)
        self.assertGreater(half + skew, half - skew)

    def test_intensity_fit_on_synthetic_declining_curve(self):
        depths = np.repeat(np.array([5., 10., 20., 30., 45., 70.]),1000)
        changes = np.tile(np.array([-1., 0., 1.]), 2000)
        c = calibrate(depths, changes)
        self.assertGreater(c["k_per_tick"], 0)
        self.assertGreater(c["A_per_second"], 0)
        self.assertGreater(c["sigma_tick_per_sqrt_second"], 0)


if __name__ == "__main__":
    unittest.main()
