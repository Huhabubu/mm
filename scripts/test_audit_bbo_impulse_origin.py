"""Deterministic causal event-origin checks; synthetic prices are not results."""
import unittest

import numpy as np

from audit_bbo_impulse_origin import (
    first_per_20ms_bucket, keep_last_of_equal_time, label_price_move,
    paired_origin_report, source_events,
)
NS = 1_000_000_000
MS = 1_000_000


class EventOriginTests(unittest.TestCase):
    def fixture(self):
        bt = np.array([NS, NS+20*MS, NS+40*MS], dtype=np.int64)
        bm = np.array([100., 101., 101.])
        ot = np.array([NS, NS+30*MS, NS+50*MS], dtype=np.int64)
        om = np.array([100., 100., 99.])
        grid = np.array([0], dtype=np.int64)
        basis = np.array([0.], dtype=float)
        return bt, bm, ot, om, grid, basis

    def test_source_split_gives_two_different_reasons_for_positive_gap(self):
        bt,bm,ot,om,g,b=self.fixture()
        x=source_events(bt,bm,ot,om,g,b,"binance",.1,NS,NS+100*MS)
        y=source_events(ot,om,bt,bm,g,b,"okx",.1,NS,NS+100*MS)
        self.assertEqual(x["eligible_after_causal_gates"],1)
        self.assertEqual(y["eligible_after_causal_gates"],1)
        self.assertEqual(x["time"].tolist(),[NS+20*MS])
        self.assertEqual(y["time"].tolist(),[NS+50*MS])
        self.assertAlmostEqual(x["gap"][0],1.0)
        self.assertAlmostEqual(y["gap"][0],2.0)
        self.assertGreater(x["source_price_change"][0],0)
        self.assertLess(y["source_price_change"][0],0)

    def test_same_timestamp_price_updates_are_excluded(self):
        bt,bm,ot,om,g,b=self.fixture()
        ot=np.array([NS, NS+20*MS, NS+30*MS, NS+50*MS],np.int64)
        om=np.array([100., 99., 99., 98.])
        x=source_events(bt,bm,ot,om,g,b,"binance",.1,NS,NS+100*MS)
        y=source_events(ot,om,bt,bm,g,b,"okx",.1,NS,NS+100*MS)
        self.assertEqual(x["eligible_after_causal_gates"],0)
        self.assertNotIn(NS+20*MS,y["time"].tolist())

    def test_changes_after_decision_cannot_modify_predecision_signal(self):
        bt,bm,ot,om,g,b=self.fixture()
        x=source_events(bt,bm,ot,om,g,b,"binance",.1,NS,NS+21*MS)
        updated=om.copy()
        updated[ot > NS+20*MS]+=500
        y=source_events(bt,bm,ot,updated,g,b,"binance",.1,NS,NS+21*MS)
        self.assertEqual(x["time"].tolist(), y["time"].tolist())
        np.testing.assert_array_equal(x["gap"], y["gap"])

    def test_20ms_dedup_limits_high_frequency_repeated_events(self):
        events={"time":np.array([20,21,39,40,50,60],np.int64)*MS}
        self.assertEqual(first_per_20ms_bucket(events).tolist(),[0,3,5])

    def test_future_label_asof_never_reads_later_quote(self):
        ot=np.array([NS, NS+20*MS,NS+120*MS],np.int64)
        om=np.array([100.,101.,200.])
        good,y=label_price_move(np.array([NS+30*MS],np.int64),
                                np.array([101.]),ot,om,50,NS+200*MS)
        self.assertTrue(good[0])
        self.assertAlmostEqual(y[0],0.)
        self.assertEqual(len(y),1)

    def test_last_state_of_same_timestamp_is_preserved(self):
        ts=np.array([10,20,20,20,30],np.int64)
        mid=np.array([1.,2.,3.,4.,5.])
        t,m=keep_last_of_equal_time(ts,mid)
        self.assertEqual(t.tolist(),[10,20,30])
        self.assertEqual(m.tolist(),[1.,4.,5.])

    def test_result_includes_zero_future_movement(self):
        bt,bm,ot,om,g,b=self.fixture()
        x=source_events(bt,bm,ot,om,g,b,"binance",.1,NS,NS+100*MS)
        y=source_events(ot,om,bt,bm,g,b,"okx",.1,NS,NS+100*MS)
        report=paired_origin_report(x,y,ot,om,NS+200*MS)
        b5=report["binance_price_change_source"]["future_okx_toward_binance"]["5"]
        self.assertEqual(b5["valid_events"],1)
        self.assertEqual(b5["nonzero_future_move_events"],0)
        self.assertEqual(b5["mean_toward_binance_bps_including_zeros"],0.0)


if __name__=="__main__":
    unittest.main()
