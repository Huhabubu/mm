"""R1 synthetic defect regression fixtures; no empirical results in this file."""
import unittest
import numpy as np

from core_glft_rules import (
    inventory_sample_is_live, same_microsecond_target_feed, quoted_ticks)
from core_glft_engine import gate_origin_filter
from cross_venue_event_probe import visible_timestamps


class R1DefectTests(unittest.TestCase):
    def test_c1_microsecond_equal_but_nanoseconds_unequal(self):
        gate_ns=1_790_000_000_000_123_000
        external=visible_timestamps(
            np.array([gate_ns],dtype=np.int64),0)[0]
        self.assertEqual(int(external),gate_ns+1)
        self.assertNotEqual(int(external),gate_ns)
        self.assertTrue(same_microsecond_target_feed(gate_ns,int(external)))
        self.assertFalse(same_microsecond_target_feed(gate_ns-1000,int(external)))
        self.assertFalse(same_microsecond_target_feed(-1,int(external)))

    def test_c1_tie_actually_blocks_a_dominant_origin_activation(self):
        # 2bp above a 1bp sigma threshold, Binance changed by 4 vs
        # Gate changed by 1; only the same-microsecond flag differs.
        tie=gate_origin_filter(
            2.,1.,104.,101.,100.,100.,True,False,True,True)
        no_tie=gate_origin_filter(
            2.,1.,104.,101.,100.,100.,True,False,True,False)
        self.assertEqual(tie[8],1)  # blocked, no activation
        self.assertEqual(tie[7],0)
        self.assertFalse(tie[4])
        self.assertEqual(no_tie[7],1)
        self.assertEqual(no_tie[8],0)
        self.assertTrue(no_tie[4])

    def test_c2_symmetric_flat_quote_and_inventory_side(self):
        long=quoted_ticks(102.,1.,0.,0.01,100,104,0,0,0,"unified")
        short=quoted_ticks(102.,-1.,0.,0.01,100,104,0,0,0,"unified")
        flat=quoted_ticks(102.,0.,0.,0.01,100,104,0,0,0,"unified")
        self.assertEqual(long[2:],(101,102))
        self.assertEqual(short[2:],(102,103))
        self.assertEqual(flat[2:],(101,103))
        self.assertEqual(quoted_ticks(102.,0.,0.,0.01,100,104,0,0,0,"legacy")[2:],(100,104))

    def test_c3_warmup_and_forced_start_excluded_from_recording(self):
        start=1_790_000_000_000_000_000
        self.assertFalse(inventory_sample_is_live(6000,6000,start+50,0))
        self.assertTrue(inventory_sample_is_live(6001,6000,start+50,0))
        self.assertFalse(inventory_sample_is_live(6001,6000,start+50,start+100))
        self.assertTrue(inventory_sample_is_live(6002,6000,start+100,start+100))


if __name__=="__main__":
    unittest.main()
