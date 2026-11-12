"""Deterministic tests of nearest local notional peak (NOT a global max).

Synthetic L2 book. No tests here are empirical evidence or PnL results.
"""
import unittest
import numpy as np

from g0_glft_event_driven import wall_front_quote_tick


class LiquidityWallUnitTests(unittest.TestCase):
    def setUp(self):
        self.lb = 990
        self.depth = np.zeros(40, np.float64)

    def test_bid_finds_first_local_peak_not_farther_global_max(self):
        base = 1000
        # From the GLFT bid downwards: 999->998->997 is 3, 20, 4.
        # 998 is the first strict local maximum, while 994 is larger.
        self.depth[999-self.lb] = 3.
        self.depth[998-self.lb] = 20.
        self.depth[997-self.lb] = 4.
        self.depth[994-self.lb] = 500.
        self.depth[1001-self.lb] = 10000.  # closer to mid: excluded
        self.assertEqual(
            wall_front_quote_tick(self.depth, self.lb, base, True, 8), 999)

    def test_ask_finds_first_local_peak_and_steps_one_tick_inward(self):
        base = 1010
        self.depth[1011-self.lb] = 3.
        self.depth[1012-self.lb] = 20.
        self.depth[1013-self.lb] = 4.
        self.depth[1016-self.lb] = 500.
        self.depth[1009-self.lb] = 10000.  # closer to mid: excluded
        self.assertEqual(
            wall_front_quote_tick(self.depth, self.lb, base, False, 7), 1011)

    def test_strict_local_peak_compares_usdt_notional_not_contracts(self):
        base = 1000
        # 1001*9.99 > 1002*9.98 despite the latter being nearer in size.
        self.depth[1001-self.lb] = 9.99
        self.depth[1002-self.lb] = 9.98
        self.assertEqual(wall_front_quote_tick(
            self.depth, self.lb, base, False, 3), 1000)
        self.depth[1001-self.lb] = 9.98
        self.depth[1002-self.lb] = 9.99
        self.assertEqual(wall_front_quote_tick(
            self.depth, self.lb, base, False, 3), 1001)

    def test_fallback_when_empty_invalid_or_missing_neighbor(self):
        self.assertEqual(wall_front_quote_tick(
            self.depth, self.lb, 1000, True, 20), 1000)
        self.depth[999-self.lb] = np.nan
        self.assertEqual(wall_front_quote_tick(
            self.depth, self.lb, 1000, True, 20), 1000)
        self.depth[999-self.lb] = -100.
        self.assertEqual(wall_front_quote_tick(
            self.depth, self.lb, 1000, True, 20), 1000)
        # 990 is last ROI price tick; missing 989 makes 990 unclassifiable.
        self.depth[990-self.lb] = 25.
        self.assertEqual(wall_front_quote_tick(
            self.depth, self.lb, 991, True, 50), 991)
        self.assertEqual(wall_front_quote_tick(
            self.depth, self.lb, 1050, False, 50), 1050)

    def test_flat_plateau_is_not_strict_peak_and_disable_keeps_original(self):
        base = 1000
        self.depth[999-self.lb] = 10.0
        self.depth[998-self.lb] = 999.0/998.0 * 10
        # Same price*qty to within float precision -> no strict peak.
        self.assertEqual(wall_front_quote_tick(
            self.depth, self.lb, base, True, 3), base)
        self.assertEqual(wall_front_quote_tick(
            self.depth, self.lb, base, True, 0), base)

    def test_search_limit_keeps_candidates_inside_range_but_checks_neighbor(self):
        base = 1000
        self.depth[999-self.lb] = 2.
        self.depth[998-self.lb] = 3.
        self.depth[997-self.lb] = 10.
        self.depth[996-self.lb] = 1.
        # At 2-tick range 997 is outside; neither 999 nor 998 is a peak.
        self.assertEqual(wall_front_quote_tick(
            self.depth, self.lb, base, True, 2), base)
        # At 3-tick range 997 is first strict peak.
        self.assertEqual(wall_front_quote_tick(
            self.depth, self.lb, base, True, 3), 998)

    def test_only_one_tick_away_peak_may_leave_glft_quote_unchanged(self):
        self.depth[999-self.lb] = 30.0
        self.assertEqual(wall_front_quote_tick(
            self.depth, self.lb, 1000, True, 5), 1000)


if __name__ == "__main__":
    unittest.main()
