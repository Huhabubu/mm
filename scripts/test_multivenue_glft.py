"""Deterministic cross-market contract-scale and risk accounting checks.

These are unit scenarios, not market outcomes or historical specifications.
"""
import math
import unittest

import numpy as np

from cross_venue_event_probe import visible_timestamps
from run_multivenue_glft import MARKETS, order_scenario, MAX_WALL_TICKS


class CrossMarketScaleTests(unittest.TestCase):
    def test_market_matrix_is_two_venues_times_two_coins(self):
        self.assertEqual(set(MARKETS),{
            ("okx","BTC"),("okx","SOL"),("bitget","BTC"),("bitget","SOL")})
        self.assertEqual(MAX_WALL_TICKS,50)

    def test_legacy_btc_okx_gamma_and_notional_are_preserved(self):
        spec=MARKETS[("okx","BTC")]
        x=order_scenario(spec,77500)
        self.assertAlmostEqual(x["gamma_code"],.05,places=12)
        self.assertAlmostEqual(x["order_qty"],.1,places=12)
        self.assertAlmostEqual(x["position_cap"],2.,places=12)
        self.assertAlmostEqual(x["initial_order_notional_usdt"],77.5,places=7)

    def test_bitget_and_sol_order_quantities_use_distinct_lots(self):
        observed_mid={"BTC":77500.0,"SOL":95.4}
        for (venue,coin),spec in MARKETS.items():
            q=order_scenario(spec,observed_mid[coin])
            self.assertAlmostEqual(q["order_qty"]/spec.lot,
                                   round(q["order_qty"]/spec.lot),places=7)
            self.assertGreaterEqual(q["initial_order_notional_usdt"],5)
            self.assertLessEqual(q["initial_order_notional_usdt"],120)
            self.assertAlmostEqual(q["gamma_code"],
                                   50*spec.ctval*spec.tick,places=10)
            self.assertAlmostEqual(q["position_cap"],20*q["order_qty"],places=10)
        self.assertAlmostEqual(order_scenario(MARKETS[("bitget","BTC")],77500)
                               ["order_qty"],.001,places=7)

    def test_fractional_2_5ms_binance_delay_is_exact(self):
        clock = np.array([1_000_000_000, 1_000_000_000], dtype=np.int64)
        actual = visible_timestamps(clock, extra_delay_ms=2.5)
        np.testing.assert_array_equal(actual - clock, [2_500_001, 2_500_001])

    def test_error_when_price_invalid(self):
        with self.assertRaises(ValueError):
            order_scenario(MARKETS[("okx","BTC")],float("nan"))
        with self.assertRaises(ValueError):
            order_scenario(MARKETS[("okx","BTC")],0.0)


if __name__=="__main__":
    unittest.main()
