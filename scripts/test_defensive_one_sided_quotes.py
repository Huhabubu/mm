"""Synthetic boundary tests for alpha=0 one-sided protective GLFT quotes.

These are deliberately toy ticks, NOT thesis empirical price estimates.
"""
import unittest
from g0_glft_event_driven import defensive_one_sided_quotes


class OneSidedDefensiveQuotes(unittest.TestCase):
    def test_positive_gap_only_lifts_ask(self):
        original=(980,1020)
        self.assertEqual(defensive_one_sided_quotes(1000.,20.,0.,5.,999,1001),
                         (original[0],1025))
        self.assertEqual(defensive_one_sided_quotes(1000.,20.,0.,0.,999,1001),
                         original)

    def test_negative_gap_only_lowers_bid(self):
        self.assertEqual(defensive_one_sided_quotes(1000.,20.,0.,-5.,999,1001),
                         (975,1020))

    def test_tick_capping_and_half_spread_unchanged(self):
        # Simulated GLFT mid/half plus nearest-spread passive limits.
        self.assertEqual(defensive_one_sided_quotes(1000.,0.,0.,5.,999,1001),
                         (999,1005))
        self.assertEqual(defensive_one_sided_quotes(1000.,0.,0.,-5.,999,1001),
                         (995,1001))
        self.assertEqual(defensive_one_sided_quotes(1000.,20.,3.,0.,999,1001),
                         (977,1023))

    def test_sub_tick_signal_respects_target_tick_not_source_tick(self):
        # 0.007 USDT/SOL at OKX tick 0.01 = +0.7 tick; ask may round +1.
        self.assertEqual(defensive_one_sided_quotes(1000.,20.,0.,.007/.01,999,1001),
                         (980,1021))
        # For target BTC tick 0.1, same price shift is only 0.07 tick.
        self.assertEqual(defensive_one_sided_quotes(1000.,20.,0.,.007/.1,999,1001),
                         (980,1020))


if __name__=="__main__":
    unittest.main()
