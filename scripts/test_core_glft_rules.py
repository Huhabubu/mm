"""Offline execution-rule checks (simplified synthetic examples, not paper model)."""
import unittest
import numpy as np
from core_glft_rules import (
    quoted_ticks, last_visible_bid_tick, last_visible_ask_tick,
    cap_quotes_to_visible_depth, book_update_unlocks, note_order_operation
)


class CoreRuleTests(unittest.TestCase):
    def test_legacy_and_unified_inventory_clamp(self):
        # One synthetic inside-spread quote: legacy pre-clamp suppresses skew.
        legacy=quoted_ticks(101, -1, 2, 1, 100,102,0,0,0,"legacy")
        unified=quoted_ticks(101, -1, 2, 1, 100,102,0,0,0,"unified")
        self.assertEqual(legacy[2],100)
        self.assertEqual(unified[2],101)
        self.assertLess(unified[2],102)
        self.assertGreater(unified[3],100)

    def test_unified_rounded_same_tick_is_separated_without_crossing(self):
        # Simplified synthetic case, not thesis empirical evidence.
        # A coarse-tick asset can produce two identical rounded targets.
        old=quoted_ticks(102.,0.,0.,0.01,100,104,0.,0.,0,"legacy")
        self.assertEqual(old[2:],(100,104))  # frozen legacy BBO clipping
        q=quoted_ticks(102.,0.,0.,0.01,100,104,0.,0.,0,"unified")
        self.assertEqual(q[2:],(101,102))
        self.assertLess(q[2],q[3])
        self.assertLess(q[2],104)
        self.assertGreater(q[3],100)

    def test_unified_applies_binance_before_post_only(self):
        legacy=quoted_ticks(100.5,0,0,1,100,102,1,2,1,"legacy")
        unified=quoted_ticks(100.5,0,0,1,100,102,1,2,1,"unified")
        self.assertEqual(legacy[2],101)
        self.assertEqual(unified[2],101)
        self.assertEqual(unified[3],104)
        self.assertEqual(legacy[3],104)

    def test_only_threatened_side_moves_for_defensive(self):
        no=quoted_ticks(100.5,0,0,2,100,102,1,5,2,"unified")
        self.assertEqual(no[2],98)
        self.assertEqual(no[3],108)
        negative=quoted_ticks(100.5,0,0,2,100,102,1,-5,2,"unified")
        self.assertEqual(negative[2],94)
        self.assertEqual(negative[3],102)

    def test_depth_count_actual_levels_not_twenty_ticks(self):
        lb=50;ub=130
        bid=np.zeros(ub-lb+1)
        ask=np.zeros(ub-lb+1)
        bid_levels=list(range(100,60,-2))  # 20 levels => 100 down to 62
        ask_levels=list(range(102,142,2))  # clipped to ROI? adjust
        ask_levels=[102+1*i for i in range(20)]
        for px in bid_levels:bid[px-lb]=1
        for px in ask_levels:ask[px-lb]=2
        self.assertEqual(last_visible_bid_tick(bid,lb,100),62)
        self.assertEqual(last_visible_ask_tick(ask,lb,ub,102),121)
        self.assertEqual(cap_quotes_to_visible_depth(50,125,62,121,100,102),
                         (62,121))

    def test_missing_depth_fails_closed(self):
        with self.assertRaisesRegex(ValueError,"Missing 20"):
            cap_quotes_to_visible_depth(90,120,-1,130,100,102)

    def test_private_fill_does_not_unlock_without_public_newer_bbo(self):
        self.assertFalse(book_update_unlocks(1000,1200,900,1300))
        self.assertFalse(book_update_unlocks(1000,1200,1001,1199))
        self.assertTrue(book_update_unlocks(1000,1200,1001,1300))

    def test_exact_one_second_sliding_operation_counter(self):
        ring=np.empty(4,np.int64)
        head=tail=maximum=0
        for t in (0,1,2,1_000_000_001,1_000_000_002):
            head,tail,maximum=note_order_operation(ring,head,tail,t,maximum)
        self.assertEqual(maximum,3)
        self.assertEqual(tail-head,2)


if __name__=="__main__":
    unittest.main()
