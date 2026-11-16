"""Focused no-lookahead and weighted-tail tests for Gate stage 2."""
import unittest
import numpy as np
from analyze_gate_1645_fill_quality import (
    label_fills, model_day, stats, weighted_p10_and_es10, asof_quotes
)
NS = 1_000_000

def synthetic_fills():
    return {
        "exchange_timestamp_ns": np.array([149, 149], dtype=np.int64)*NS,
        "local_observed_ns": np.array([150, 150], dtype=np.int64)*NS,
        "side": np.array([1,-1], dtype=np.int64),
        "exec_price": np.array([99., 101.]),
        "exec_qty": np.array([1.,2.])
    }


class GateMarkoutTest(unittest.TestCase):
    def setUp(self):
        self.t = np.array([0,100,150,200,300],dtype=np.int64)*NS
        self.mid = np.array([100.,100.,105.,99.,101.])
        self.f = synthetic_fills()

    def test_strict_before_entry_and_horizon_asof(self):
        annotated, flags = label_fills(self.f, self.t, self.mid, 1_000*NS, 0.001)
        self.assertEqual(annotated["gate_quote_mid_at_observation"][0],100.0)
        self.assertAlmostEqual(annotated["markout_100ms_bps"][0],-100.)
        self.assertAlmostEqual(annotated["markout_100ms_bps"][1],+100.)
        self.assertAlmostEqual(annotated["capture_halfspread_bps"][0],+100.)
        self.assertAlmostEqual(annotated["capture_halfspread_bps"][1],-100.)
        self.assertTrue(flags[100][1].all())

    def test_no_future_entry_quote(self):
        anno1,_ = label_fills(self.f,self.t,self.mid,1_000*NS,0.001)
        changed = self.mid.copy()
        changed[2] = 10_000.  # same microsecond as fill; must not leak into entry
        anno2,_ = label_fills(self.f,self.t,changed,1_000*NS,0.001)
        np.testing.assert_array_equal(
            anno1["gate_quote_mid_at_observation"],
            anno2["gate_quote_mid_at_observation"])
        np.testing.assert_array_equal(
            anno1["capture_halfspread_bps"],
            anno2["capture_halfspread_bps"])

    def test_stale_start_or_end_excluded(self):
        self.t=np.array([0,100,300],dtype=np.int64)*NS
        self.mid=np.array([100.,100.,99.])
        f={k:v[:1].copy() for k,v in self.f.items()}
        _,flags=label_fills(f,self.t,self.mid,1_000*NS,0.001)
        self.assertTrue(flags[100][1][0])
        self.assertFalse(flags[500][1][0])

    def test_future_quote_not_used_before_horizon(self):
        f={k:v[:1].copy() for k,v in self.f.items()}
        t=np.array([0,100,300],dtype=np.int64)*NS
        mids=np.array([100.,100.,99.])
        val,_=label_fills(f,t,mids,2_000*NS,0.001)
        self.assertEqual(val["markout_100ms_bps"][0],0)
        self.assertLess(val["markout_500ms_bps"][0],0)

    def test_day_boundary_excludes_1sec_markout(self):
        f={k:v[:1].copy() for k,v in self.f.items()}
        _, flags=label_fills(f,self.t,self.mid,500*NS,0.001)
        self.assertFalse(flags[1000][0][0])

    def test_tail_exact_count_and_weighted_tail(self):
        x=np.array([-10.,-5.,2.,3.,8.,12.])
        w=np.array([1.,1.,1.,1.,1.,5.])
        d=stats(x,w)
        self.assertEqual(d["count"],6)
        self.assertEqual(d["worst10_mean_bps"],-10.)
        self.assertEqual(d["notional_weighted_worst10_mean_bps"],-10.)
        self.assertAlmostEqual(d["negative_fraction"],2/6)
        self.assertAlmostEqual(d["notional_weighted_negative_fraction"],2/10)

    def test_model_hbt_counts_required(self):
        with self.assertRaisesRegex(ValueError,"HBT count mismatch"):
            model_day("glft",self.f,{"trading":{"engine_fills":3}},
                      self.t,self.mid,1_000*NS,0.001)

    def test_negative_and_zero_not_confused(self):
        x=np.array([-1.,0.,1.])
        w=np.ones(3)
        self.assertAlmostEqual(stats(x,w)["negative_fraction"],1/3)


if __name__=="__main__":
    unittest.main()
