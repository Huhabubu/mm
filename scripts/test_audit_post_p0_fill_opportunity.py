"""Focused synthetic fill matching and adverse-selection accounting tests."""
import unittest
import numpy as np
from audit_post_p0_fill_opportunity import pair_exact_clock_side,group_stats


def fixture():
    t=np.array([10,10,10,20,30],np.int64)
    s=np.array([1,1,-1,-1,1],np.int64)
    return {"exchange_timestamp_ns":t,"side":s,
            "local_observed_ns":t+1,
            "signed_markout_bps_100ms":np.array([-2.,-1.,-.5,0.,1.]),
            "signed_markout_bps_500ms":np.array([-3.,-1.5,-.8,.1,2.]),
            "fresh_100ms":np.array([1,1,1,1,0],bool),
            "fresh_500ms":np.array([1,1,1,1,0],bool),
            "exec_qty":np.ones(5),"native_mid_at_observation":np.ones(5)*100}


class OfficialFillAttributionTests(unittest.TestCase):
    def test_exact_time_and_side_is_multiset_not_cartesian(self):
        b=fixture()
        v={k:(x[[0,2,4]] if len(x)==5 else x) for k,x in b.items()}
        same_b,same_v=pair_exact_clock_side(b,v)
        self.assertEqual(int(same_b.sum()),3)
        self.assertEqual(int(same_v.sum()),3)
        self.assertEqual(int((~same_b).sum()),2)

    def test_only_shared_and_new_accounting(self):
        b=fixture()
        v={k:(x[[1,2,3]] if len(x)==5 else x) for k,x in b.items()}
        same_b,same_v=pair_exact_clock_side(b,v)
        self.assertEqual(len(b["side"]),int(same_b.sum())+int((~same_b).sum()))
        self.assertEqual(len(v["side"]),int(same_v.sum())+int((~same_v).sum()))

    def test_markout_reports_only_fresh_with_negative_sign(self):
        b=fixture()
        mask=np.array([True,True,False,False,True])
        x=group_stats(b,mask)
        self.assertEqual(x["fill_count"],3)
        self.assertEqual(x["markout"]["100ms"]["fresh_fills"],2)
        self.assertAlmostEqual(x["markout"]["100ms"]["mean_signed_markout_bps"],-1.5)
        self.assertAlmostEqual(x["markout"]["100ms"]["negative_fraction"],1.)

    def test_timestamp_shift_invalidates_same_fill_pairing(self):
        b=fixture()
        v={k:x.copy() for k,x in b.items()}
        v["exchange_timestamp_ns"]+=1
        a,c=pair_exact_clock_side(b,v)
        self.assertFalse(a.any() or c.any())


if __name__=="__main__":
    unittest.main()
