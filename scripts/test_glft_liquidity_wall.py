"""Deterministic synthetic checks for optional GLFT quote anchoring.

All depths are manufactured. This does not test economic profitability.
"""
import unittest
import numpy as np
from g0_glft_event_driven import wall_front_quote_tick


class LiquidityWallUnitTests(unittest.TestCase):
    def setUp(self):
        self.lb = 990
        self.depth = np.zeros(40, np.float64)

    def test_bid_selects_maximum_usdt_notional_beyond_glft_quote(self):
        base = 1000
        self.depth[999-self.lb] = 10.0
        self.depth[997-self.lb] = 200.0
        self.depth[994-self.lb] = 100.0
        self.depth[1001-self.lb] = 10000.0  # inside quote, must ignore
        self.assertEqual(wall_front_quote_tick(
            self.depth,self.lb,base,True,8),998)
        self.assertLessEqual(wall_front_quote_tick(
            self.depth,self.lb,base,True,8),base)

    def test_ask_selects_maximum_usdt_notional_and_fronts_one_tick(self):
        base = 1010
        self.depth[1011-self.lb] = 10.0
        self.depth[1013-self.lb] = 70.0
        self.depth[1016-self.lb] = 20.0
        self.depth[1009-self.lb] = 10000.0  # behind market, not outward
        self.assertEqual(wall_front_quote_tick(
            self.depth,self.lb,base,False,7),1012)
        self.assertGreaterEqual(wall_front_quote_tick(
            self.depth,self.lb,base,False,7),base)

    def test_notional_uses_price_times_quantity(self):
        # 1001*9.99 > 1002*9.98 despite a lower contract quantity.
        base = 1000
        self.depth[1001-self.lb] = 9.99
        self.depth[1002-self.lb] = 9.98
        self.assertEqual(wall_front_quote_tick(
            self.depth,self.lb,base,False,2),1000)
        self.depth[1001-self.lb] = 9.98
        self.depth[1002-self.lb] = 9.99
        self.assertEqual(wall_front_quote_tick(
            self.depth,self.lb,base,False,2),1001)

    def test_fallback_for_empty_zero_nan_or_outside_roi(self):
        self.assertEqual(wall_front_quote_tick(
            self.depth,self.lb,1000,True,20),1000)
        self.depth[999-self.lb] = np.nan
        self.assertEqual(wall_front_quote_tick(
            self.depth,self.lb,1000,True,20),1000)
        self.depth[999-self.lb] = -100.
        self.assertEqual(wall_front_quote_tick(
            self.depth,self.lb,1000,True,20),1000)
        self.depth[990-self.lb] = 25.
        self.assertEqual(wall_front_quote_tick(
            self.depth,self.lb,991,True,50),991)  # wall at 990 => quote=991
        self.assertEqual(wall_front_quote_tick(
            self.depth,self.lb,1050,False,50),1050)

    def test_nearest_wins_exact_notional_tie_and_zero_off(self):
        base = 1000
        self.depth[999-self.lb] = 1.0
        self.depth[998-self.lb] = 999.0/998.0
        self.assertEqual(wall_front_quote_tick(
            self.depth,self.lb,base,True,3),1000)
        self.assertEqual(wall_front_quote_tick(
            self.depth,self.lb,base,True,0),1000)


if __name__ == "__main__":
    unittest.main()
