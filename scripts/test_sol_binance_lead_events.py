"""Synthetic tests for per-event Binance lead audit; no fabricated market claims."""
import unittest

import numpy as np

from audit_sol_binance_lead_events import (
    asof, count_summary, stats, EXTRA_DELAY_NS, TARGET,
)


class LeadAuditTests(unittest.TestCase):
    def test_strict_current_tie_and_visible_latency(self):
        timestamps=np.array([1_000,2_000,2_000,3_000],dtype=np.int64)
        q=np.array([2_000,2_500],dtype=np.int64)
        np.testing.assert_array_equal(asof(timestamps,q),[0,2])
        np.testing.assert_array_equal(asof(timestamps,q,True),[2,2])
        self.assertEqual(EXTRA_DELAY_NS,2_500_001)
        self.assertEqual(TARGET.tick,.01)
        self.assertEqual(TARGET.ctval,1.)
        self.assertEqual(TARGET.lot,.01)

    def test_deduplicate_first_event_per_price_stable_okx_interval(self):
        result=count_summary(
            candidate=np.array([True,True,True]),
            segment_id=np.array([1,1,2]),
            raw_ts=np.array([10_000_000,12_000_000,20_000_000]),
            visible_okx_changed=np.array([True,False,False]),
            visible_good=np.array([True,True,True]),
            survival_ns=None,
            event_ns=np.array([10_000_000,12_000_000,20_000_000]),
            interval_ns=np.array([1_000_000,6_000_000,12_000_000],dtype=float),
            raw_g_bps=np.array([12.,11.,10.]),
            visible_g_bps=np.array([2.,11.,10.]),
            raw_spread_bps=np.array([13.,12.,11.]),
        )
        self.assertEqual(result["event_count"],3)
        self.assertEqual(result["unique_okx_unchanged_segments"],2)
        self.assertEqual(result["visible_still_same_okx_price_count"],2)
        self.assertEqual(result["visible_gap_still_at_least_10bps_count"],2)
        self.assertEqual(result["follow_survival"]["2.5ms"]["n"],1)

    def test_distribution_ignores_missing_and_inf(self):
        a=stats([1.,2.,np.inf,np.nan,3.])
        self.assertEqual(a["n"],3)
        self.assertEqual(a["max"],3.)
        self.assertEqual(stats([]),{"n":0})


if __name__=="__main__":
    unittest.main()
