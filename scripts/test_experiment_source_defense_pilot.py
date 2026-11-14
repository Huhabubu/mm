"""Synthetic pilot acceptance/fill accounting unit tests, not market results."""
import unittest
import numpy as np
from hftbacktest import BUY, SELL

from experiment_source_defense_pilot import (
    annotate_fills, classify_opportunities, fill_stats,
    attribution_of_paired_fills
)


class SparsePilotTests(unittest.TestCase):
    def sample_fills(self):
        return {"exchange_timestamp_ns":np.array([10,20,30],np.int64),
                "local_observed_ns":np.array([11,21,31],np.int64),
                "side_sign":np.array([1,-1,1],np.int8),
                "exec_qty":np.ones(3),
                "native_mid_at_fill_observation":np.ones(3)*100.,
                "spread_capture_bps":np.array([1.,1.,2.]),
                "fresh_100ms":np.array([1,1,1],bool),
                "fresh_500ms":np.array([1,1,1],bool),
                "fresh_1000ms":np.array([1,1,1],bool),
                "fresh_5000ms":np.array([1,1,1],bool),
                "markout_100ms_bps":np.array([-5.,-1.,.5]),
                "markout_500ms_bps":np.array([-5.,-1.,.5]),
                "markout_1000ms_bps":np.array([-5.,-1.,.5]),
                "markout_5000ms_bps":np.array([-5.,-1.,.5]),
                "gross_edge_100ms_bps":np.array([-4.,0.,2.5]),
                "gross_edge_500ms_bps":np.array([-4.,0.,2.5]),
                "gross_edge_1000ms_bps":np.array([-4.,0.,2.5]),
                "gross_edge_5000ms_bps":np.array([-4.,0.,2.5])}

    def test_exact_exchange_matching_one_to_one(self):
        a=self.sample_fills()
        b={k:(v[[0,2]] if len(v)==3 else v) for k,v in a.items()}
        dropped,kept=classify_opportunities(a,b)
        self.assertEqual(dropped.tolist(),[False,True,False])
        self.assertEqual(kept.sum(),2)

    def test_selectivity_lift_direction_and_toxic_recall(self):
        a=self.sample_fills()
        b={k:(v[[1,2]] if len(v)==3 else v) for k,v in a.items()}
        y=attribution_of_paired_fills(a,b)
        self.assertEqual(y["base_only_count"],1)
        self.assertAlmostEqual(y["by_horizon"]["100ms"][
            "selectivity_lift_kept_minus_dropped_bps"],4.75)
        self.assertEqual(y["toxic_recall_descriptive"],1.)
        self.assertEqual(y["safe_sacrifice_descriptive"],0.)

    def test_correct_spread_and_future_markout_sign(self):
        raw=[None]*35
        raw[31]=np.array([200_000_000],np.int64)
        raw[32]=np.array([BUY],np.int64)
        raw[34]=(None,np.array([201_000_000],np.int64),
                 np.array([99.]),np.array([1.]),None)
        ts=np.array([100_000_000,200_000_000,201_000_000,
                     301_000_000,701_000_000,1_201_000_000,5_201_000_000],np.int64)
        prices=np.array([100.,100.,100.,99.,99.,99.,99.])
        out=annotate_fills(raw,ts,prices,.1,1.)
        self.assertAlmostEqual(out["spread_capture_bps"][0],100.)
        self.assertAlmostEqual(out["markout_100ms_bps"][0],-100.)
        self.assertAlmostEqual(out["adverse_selection_cost_100ms_bps"][0],100.)
        self.assertAlmostEqual(out["gross_edge_100ms_bps"][0],0.)
        self.assertTrue(out["fresh_100ms"][0])

    def test_missing_horizon_stays_unverified(self):
        a=self.sample_fills()
        a["fresh_5000ms"][:]=False
        stats=fill_stats(a,np.ones(3,bool),5000)
        self.assertEqual(stats["valid_markout_fills"],0)
        self.assertIsNone(stats["p10_markout_bps"])


if __name__=="__main__":
    unittest.main()
