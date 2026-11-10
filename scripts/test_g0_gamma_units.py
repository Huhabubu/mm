"""Audit real GLFT code unit conversion, no market data or trading.

This test uses *actual* g0_glft_rolling.glft_coefficients, and checks its
dimensional invariance when contract/tick units change to BTC/USDT.
Synthetic/archived calibration inputs here do NOT imply strategy profitability.
"""
import math
import unittest

from g0_glft_baseline import (GAMMA, ORDER_QTY, CTVAL, TICK,
                              MAKER_FEE, POSITION_CAP)
from g0_glft_rolling import glft_coefficients


# The recorded real OKX Nov 1 2026 first/last rolling estimates.
EXAMPLE_ESTIMATES = (
    (0.05056095, 0.01046594, 13.08882, 104.12498),
    (0.11223479, 0.02848537, 16.79930, 45.10902),
)


def glft_formula(gamma, order_units, a, k, sigma):
    """Independent direct algebra for dimensional invariance; gamma per cash."""
    u = gamma * order_units / k
    c1 = math.log1p(u) / (gamma * order_units)
    c2 = math.sqrt(gamma / (2 * a * order_units * k) *
                   math.exp((k / (gamma * order_units) + 1)*math.log1p(u)))
    return c1 + order_units/2 * c2 * sigma, c2 * sigma


class GammaUnitAuditTests(unittest.TestCase):
    def test_current_hardcoded_scenario_identified(self):
        self.assertEqual(GAMMA, .05)
        self.assertEqual(ORDER_QTY, .1)
        self.assertEqual(CTVAL, .01)
        self.assertEqual(TICK, .1)
        self.assertEqual(POSITION_CAP, 2.0)

    def test_cash_gamma_conversion(self):
        # Per contract per tick cash PnL sensitivity: BTC/contract * USDT/BTC/tick
        cash_per_contract_tick = CTVAL * TICK
        self.assertAlmostEqual(cash_per_contract_tick, .001)
        # If exponential utility acts on USDT wealth, G_cash=G_code/cash_unit.
        self.assertAlmostEqual(GAMMA / cash_per_contract_tick, 50.0)
        self.assertAlmostEqual(1 / (GAMMA / cash_per_contract_tick), .02)

    def test_quote_formula_invariant_in_usdt_btc_units(self):
        for a, k_tick, sigma_tick, reference_half_tick in EXAMPLE_ESTIMATES:
            # Existing production code uses ticks, contracts, 0.05.
            half_tick, skew_tick_per_contract = glft_coefficients(a,k_tick,sigma_tick)
            self.assertAlmostEqual(half_tick, reference_half_tick, places=3)

            # Convert *the same wealth utility* to USDT, BTC and USDT/BTC.
            gamma_cash = GAMMA / (CTVAL*TICK)     # inverse USDT
            qty_btc = ORDER_QTY * CTVAL           # BTC
            k_price = k_tick / TICK               # (USDT/BTC)^-1
            sigma_price = sigma_tick * TICK       # (USDT/BTC)/sqrt(s)
            h_price, skew_price_per_btc = glft_formula(
                gamma_cash,qty_btc,a,k_price,sigma_price)
            self.assertAlmostEqual(h_price, half_tick*TICK, places=10)
            self.assertAlmostEqual(skew_price_per_btc,
                                   skew_tick_per_contract*TICK/CTVAL, places=10)

    def test_different_cash_gamma_is_policy_change_not_unit_conversion(self):
        a,k,sigma,_=EXAMPLE_ESTIMATES[0]
        h_current, skew_current = glft_coefficients(a,k,sigma)
        cash_gamma_choice=.05        # 1/USDT, a proposed sensitivity point only
        gamma_code_equiv=cash_gamma_choice*CTVAL*TICK
        h_proposal,_=glft_formula(gamma_code_equiv,ORDER_QTY,a,k,sigma)
        self.assertAlmostEqual(gamma_code_equiv,.00005)
        self.assertAlmostEqual(h_current,104.12495,places=3)
        self.assertAlmostEqual(h_proposal,96.26694,places=3)
        self.assertNotAlmostEqual(h_current,h_proposal,places=1)
        self.assertGreater(skew_current,0)

    def test_maker_fee_one_side_price_tick_floor(self):
        price_usdt_per_btc=77500.0 # illustrative, not a historical timestamp price
        fee_usdt_per_fill=(ORDER_QTY*CTVAL)*price_usdt_per_btc*MAKER_FEE
        fee_equivalent_price_ticks=price_usdt_per_btc*MAKER_FEE/TICK
        self.assertAlmostEqual(fee_usdt_per_fill,.0155)
        self.assertAlmostEqual(fee_equivalent_price_ticks,155)
        self.assertTrue(all(x[3] < fee_equivalent_price_ticks for x in EXAMPLE_ESTIMATES))

    def test_gamma_changes_inventory_skew_substantially(self):
        a,k,sigma,_=EXAMPLE_ESTIMATES[0]
        _,skew_code=glft_coefficients(a,k,sigma)
        _,skew_cash_point=glft_formula(.05*CTVAL*TICK,ORDER_QTY,a,k,sigma)
        self.assertAlmostEqual(skew_code,520.44296,places=2)
        self.assertAlmostEqual(skew_cash_point,14.8345,places=2)
        # q=0.1 contracts: 52 real tick displacement at current code gamma.
        self.assertAlmostEqual(skew_code*ORDER_QTY,52.044296,places=3)


if __name__=="__main__":
    unittest.main()
