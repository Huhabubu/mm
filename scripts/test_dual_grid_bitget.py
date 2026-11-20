"""Simplified synthetic examples, NOT empirical 2026 Bitget orderbook results.

Spec: raw HBT tick=0.000001, legal Bitget order tick=0.00001.
Check legacy Gate functions remain EXACTLY identical at multiplier=1.
"""
import unittest
import numpy as np
from core_glft_rules import quoted_ticks,dual_grid_quoted_ticks
from g0_glft_rolling import fit_physical_1tick_poisson
from g0_glft_baseline import TRAIN


class DualGridRules(unittest.TestCase):
    def test_two_raw_ticks_stay_distinct_and_quotes_are_legal(self):
        # Adjacent 6-decimal prices are distinct and never rounded/merged in L2.
        bad_old_price=0.069049
        legal_price=0.069050
        raw_tick=0.000001
        self.assertEqual(round(bad_old_price/raw_tick),69049)
        self.assertEqual(round(legal_price/raw_tick),69050)
        self.assertNotEqual(round(bad_old_price/raw_tick),round(legal_price/raw_tick))
        self.assertEqual(round(bad_old_price/0.00001),round(legal_price/0.00001))
        # Opposite ask is itself an off-grid legacy order.
        bid,ask=69049,69053
        self.assertLess(bid,ask)
        mid_economic=(bid+ask)/(2*10)
        for mode,beta,signal in ((0,0.,0.),(1,1.,15.),(1,1.,-15.),(3,1.,15.)):
            bb,ba,want_bid,want_ask=dual_grid_quoted_ticks(
                mid_economic,0.,.3,2.,bid,ask,beta,signal,mode,10)
            self.assertEqual(want_bid%10,0)
            self.assertEqual(want_ask%10,0)
            self.assertLess(want_bid,ask)
            self.assertGreater(want_ask,bid)
            self.assertLess(want_bid,want_ask)

    def test_legal_round_and_post_only_limit_whether_raw_book_on_grid(self):
        for raw_bid,raw_ask in ((69040,69050),(69049,69053),
                                (69048,69060),(69050,69051)):
            q=dual_grid_quoted_ticks(
                (raw_bid+raw_ask)/20.,0,0,0.01,raw_bid,raw_ask,1,2.,1,10)
            self.assertEqual(q[2]%10,0)
            self.assertEqual(q[3]%10,0)
            self.assertLess(q[2],raw_ask)
            self.assertGreater(q[3],raw_bid)

    def test_one_tick_physical_fit_uses_legal_economic_tick(self):
        # Same hypothetical 100ms price changes and trade-throughs in legal
        # ticks, represented by HBT raw prices 10 times finer in DOGE.
        bid=np.full(TRAIN,np.nan)
        ask=np.full(TRAIN,np.nan)
        change=np.full(TRAIN,np.nan)
        for i in range(TRAIN):
            change[i] = (i%3-1)*.4
            if i%7==0: bid[i]=9.3
            if i%11==0: ask[i]=5.2
        first=fit_physical_1tick_poisson(bid,ask,change)
        bid_from_raw=(bid*10)/10
        ask_from_raw=(ask*10)/10
        change_from_raw=(change*10)/10
        second=fit_physical_1tick_poisson(bid_from_raw,ask_from_raw,change_from_raw)
        self.assertEqual(first[0],1)
        for i in range(1,4):
            self.assertAlmostEqual(first[i],second[i],places=11)
        # If the 10x conversion is accidentally omitted, fitted k and sigma
        # reflect raw tick distance and change; detect regression.
        wrong=fit_physical_1tick_poisson(bid*10,ask*10,change*10)
        self.assertNotAlmostEqual(first[2],wrong[2],places=5)

    def test_dual_grid_collision_preserves_inventory_reduction_side(self):
        # Simplified synthetic raw=1e-6 and legal=1e-5 grid, not real DOGE.
        def quote(pos):
            return dual_grid_quoted_ticks(
                6905.,pos,0.,.01,69000,69100,0.,0.,0,10)
        self.assertEqual(quote(1.)[2:],(69040,69050))
        self.assertEqual(quote(-1.)[2:],(69050,69060))
        self.assertEqual(quote(0.)[2:],(69040,69060))
        for pos in (-1.,0.,1.):
            q=quote(pos)
            self.assertEqual(q[2]%10,0)
            self.assertEqual(q[3]%10,0)
            self.assertLess(q[2],q[3])

    def test_exact_legacy_gate_quoted_ticks_unchanged(self):
        observed=quoted_ticks(102.,0.,0.,.01,100,104,0.,0.,0,"unified")
        self.assertEqual(observed,(100,104,101,103))
        self.assertEqual(quoted_ticks(101.,-1.,2.,1.,100,102,0.,0.,0,
                                       "legacy")[2],100)

if __name__=="__main__":
    unittest.main()
