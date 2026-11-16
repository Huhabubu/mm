"""Stage3 shadow classification: no matching and no causal fill proof."""
import unittest
import numpy as np
from audit_gate_1645_stage3_shadow import (
    probe_deadlines, signal_categories, pair_fill_ids,
)

class Stage3Test(unittest.TestCase):
    def setUp(self):
        self.exch=np.array([10_000_000_000,20_000_000_000,30_000_000_000],dtype=np.int64)
        self.fills={
            "exchange_timestamp_ns":self.exch,
            "local_observed_ns":self.exch+1_000_000,
            "side":np.array([-1,1,-1],dtype=np.int64),
            "exec_price":np.array([100.,100.,100.]),
            "exec_qty":np.array([1.,1.,1.]),
        }

    def test_order_request_deadline_is_strictly_early(self):
        probe,inverse=probe_deadlines(self.fills)
        np.testing.assert_array_equal(
            probe,self.exch-1_001_000)
        np.testing.assert_array_equal(inverse,np.arange(3))

    def test_sign_and_cancellation_require_all_gates(self):
        probe,inverse=probe_deadlines(self.fills)
        rows=np.full((3,25),np.nan)
        rows[:,7]=[1.,-2.,0.]         # SELL, BUY, no effect
        rows[:,8]=[3.,-3.,0.]
        rows[:,9]=[1.,1.,1.]
        rows[:,10]=[1,1,0]
        rows[:,11]=probe-1_000_000
        rows[:,12]=probe-10_000_000
        rows[:,0]=100               # bid order
        rows[:,1]=101               # ask order
        rows[:,4]=1
        rows[:,5]=1
        rows[:,16]=99               # signal buy price differs
        rows[:,17]=102              # signal sell price differs
        rows[:,18]=probe-21_000_000
        rows[:,19]=probe-21_000_000
        labels=signal_categories(self.fills,rows,inverse,probe)
        self.assertEqual(labels["category"].tolist(),[
            "threatened","threatened","inactive_or_no_effect"])
        self.assertEqual(labels["theoretical_pre_fill_cancel_request"].tolist(),[
            True,True,False])
        rows[1,18]=probe[1]-19_000_000 # side rate gate fails
        rows[0,17]=101                 # no quote difference
        labels=signal_categories(self.fills,rows,inverse,probe)
        self.assertEqual(labels["theoretical_pre_fill_cancel_request"].tolist(),[
            False,False,False])
        self.assertEqual(labels["category"].tolist(),[
            "threatened","threatened","inactive_or_no_effect"])

    def test_opposite_side_uses_direction_not_trade_price(self):
        probe,inverse=probe_deadlines(self.fills)
        rows=np.full((3,25),np.nan)
        rows[:,7]=[-1.,1.,0.]
        rows[:,10]=[1,1,0]
        labels=signal_categories(self.fills,rows,inverse,probe)
        self.assertEqual(labels["category"].tolist(),[
            "opposite","opposite","inactive_or_no_effect"])
        self.assertFalse(np.any(labels["theoretical_pre_fill_cancel_request"]))

    def test_multiset_same_execution_side_never_counts_new_fill_as_saved(self):
        original=self.fills
        comparison={
            "exchange_timestamp_ns":np.array([10_000_000_000,10_000_000_000],np.int64),
            "side":np.array([-1,1],np.int64),
        }
        found=pair_fill_ids(original,comparison)
        self.assertEqual(found.tolist(),[True,False,False])

    def test_no_quote_or_no_cancellable_order_disables_flag(self):
        probe,inverse=probe_deadlines(self.fills)
        rows=np.full((3,25),np.nan)
        rows[:,7]=[1.,1.,1.]
        rows[:,10]=1
        rows[:,5]=[0,0,1]
        rows[:,1]=[101.,101.,101.]
        rows[:,17]=[102.,102.,np.nan]
        rows[:,19]=probe-30_000_000
        labels=signal_categories(self.fills,rows,inverse,probe)
        self.assertFalse(np.any(labels["theoretical_pre_fill_cancel_request"]))

if __name__=="__main__":
    unittest.main()
