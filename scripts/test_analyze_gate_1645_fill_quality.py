"""Focused no-lookahead and weighted-tail tests for Gate stage 2."""
import unittest
import tempfile
from pathlib import Path
import numpy as np
from analyze_gate_1645_fill_quality import (
    label_fills, model_day, stats, weighted_p10_and_es10, asof_quotes,
    quote_age_bucket_mask, depth_visibility_diagnostic
)
from audit_gate_stage5b_same_fill import summarize_horizon
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
        self.assertAlmostEqual(annotated["capture_halfspread_bps"][1],+100.)
        self.assertTrue(flags[100][1].all())

    def test_execution_price_total_markout_and_legacy_preserved(self):
        anno, _ = label_fills(self.f, self.t, self.mid, 1_000*NS, 0.001)
        self.assertAlmostEqual(anno["total_markout_100ms_bps"][0], 0.0)
        self.assertAlmostEqual(anno["total_markout_100ms_bps"][1], (101-99)/101*10000)
        self.assertAlmostEqual(anno["markout_100ms_bps"][0], -100.0)
        self.assertTrue(anno["total_fresh_100ms"].all())
        self.assertTrue(anno["future_quote_updated_100ms"].all())
        row = summarize_horizon(anno, np.ones(2,bool), "synthetic", "both", 100)
        self.assertAlmostEqual(row["total_markout_bps"],
                               anno["total_markout_100ms_bps"].mean())
        self.assertAlmostEqual(row["total_markout_minus_maker2bps_bps"],
                               row["total_markout_bps"]-2.0)
        self.assertEqual(row["fresh_fills"],2)
        self.assertEqual(row["total_fresh_fills"],2)

    def test_future_quote_fresh_while_entry_quote_stale(self):
        f = {k:v[:1].copy() for k,v in self.f.items()}
        f["exchange_timestamp_ns"] = np.array([799])*NS
        f["local_observed_ns"] = np.array([800])*NS
        t = np.array([0,100,1250],dtype=np.int64)*NS
        mid = np.array([100.,100.,99.])
        anno,_ = label_fills(f,t,mid,2_000*NS,0.001)
        self.assertFalse(anno["fresh_500ms"][0])
        self.assertTrue(anno["total_fresh_500ms"][0])
        self.assertTrue(anno["future_quote_updated_500ms"][0])
        self.assertAlmostEqual(anno["total_markout_500ms_bps"][0],0)

    def test_quote_age_buckets_disjoint(self):
        ages=np.array([-1.,0.,99.999,100.,250.,250.001])
        a=quote_age_bucket_mask(ages,"lt_100ms")
        b=quote_age_bucket_mask(ages,"100_to_250ms")
        np.testing.assert_array_equal(a,[False,True,True,False,False,False])
        np.testing.assert_array_equal(b,[False,False,False,True,True,False])
        self.assertFalse(np.any(a & b))

    def test_depth_missing_fails_closed(self):
        r=depth_visibility_diagnostic("2026-06-01","glft",None,self.f)
        self.assertEqual(r["status"],"unavailable_missing_order_placement_l2_witness")
        self.assertIsNone(r["orders_outside_visible_fraction"])

    def test_depth_witness_counts_and_future_l2_guard(self):
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/"2026-06-01_glft_depth_witness.npz"
            w=dict(
                order_side=np.array([1,-1]),
                order_price_tick=np.array([90.,105.]),
                order_last_visible_tick=np.array([95.,105.]),
                order_submit_local_ns=np.array([100,100])*NS,
                order_book_local_ns=np.array([99,99])*NS,
                fill_order_side=np.array([1,-1]),
                fill_order_price_tick=np.array([90.,105.]),
                fill_order_last_visible_tick=np.array([95.,105.]),
                fill_order_submit_local_ns=np.array([100,100])*NS,
                fill_order_book_local_ns=np.array([99,99])*NS,
                fill_exchange_timestamp_ns=self.f["exchange_timestamp_ns"])
            np.savez(path,**w)
            r=depth_visibility_diagnostic("2026-06-01","glft",Path(d),self.f)
            self.assertEqual(r["orders_outside_visible_count"],1)
            self.assertEqual(r["fills_outside_visible_count"],1)
            w["order_book_local_ns"][0]=101*NS
            np.savez(path,**w)
            with self.assertRaisesRegex(ValueError,"future-information"):
                depth_visibility_diagnostic("2026-06-01","glft",Path(d),self.f)

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
        self.t=np.array([0,100],dtype=np.int64)*NS
        self.mid=np.array([100.,100.])
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
