"""Gamma policy sensitivity tests; no network and no simulated claims."""
import math
import unittest
from g0_glft_baseline import GAMMA
from g0_glft_rolling import glft_coefficients
from g0_gamma_sweep import GAMMA_USDT_GRID,FREE_DATES,gamma_usdt_to_code


class GammaSensitivityTests(unittest.TestCase):
    def test_fixed_sweep_contains_original_legacy(self):
        self.assertEqual(GAMMA_USDT_GRID,(.001,.01,.05,.5,5.,50.))
        self.assertEqual(len(FREE_DATES),6)
        self.assertAlmostEqual(gamma_usdt_to_code(50),GAMMA)

    def test_invalid_cash_risk_rejected(self):
        self.assertAlmostEqual(gamma_usdt_to_code(.05),.00005)
        for x in (0,-.01,float("nan"),float("inf")):
            with self.assertRaises(ValueError):
                gamma_usdt_to_code(x)
        with self.assertRaises(ValueError):
            gamma_usdt_to_code(.05,contract_value=0)
        with self.assertRaises(ValueError):
            gamma_usdt_to_code(.05,tick_size=0)

    def test_same_cash_risk_scaled_by_contract(self):
        self.assertAlmostEqual(gamma_usdt_to_code(.05,.01,.1),.00005)
        self.assertAlmostEqual(gamma_usdt_to_code(.05,.001,.1),.000005)

    def test_legacy_default_matches_explicit_sweep_anchor(self):
        a,k,sigma=.05056095,.01046594,13.08882
        self.assertEqual(glft_coefficients(a,k,sigma),
                         glft_coefficients(a,k,sigma,gamma_usdt_to_code(50)))

    def test_predeclared_values_change_inventory_skew(self):
        a,k,sigma=.05056095,.01046594,13.08882
        x=[glft_coefficients(a,k,sigma,gamma_usdt_to_code(g))
           for g in GAMMA_USDT_GRID]
        self.assertTrue(all(all(math.isfinite(v) and v>0 for v in item) for item in x))
        self.assertLess(x[0][1],x[-1][1])
        self.assertLess(x[0][0],x[-1][0])


if __name__=="__main__":
    unittest.main()
