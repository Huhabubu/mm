"""Strict visibility and polarity tests for Tardis aggressor trade flow."""
import unittest
import numpy as np

from experiment_source_defense_trade_flow import causal_taker_window


class TakerFlowTests(unittest.TestCase):
    def test_later_trades_cannot_leak_into_now(self):
        visible=np.array([1_000_000,2_000_000,3_000_000],np.int64)
        cumulative_signed=np.array([0.,100.,200.,-800.])
        cumulative_notional=np.array([0.,100.,200.,1200.])
        a=causal_taker_window(2_500_000,visible,cumulative_signed,
                               cumulative_notional,2_500_000,1,.35,100.)
        self.assertTrue(a[0])
        self.assertAlmostEqual(a[1],1.)
        # Even a huge future sell must not change the earlier decision.
        signed=cumulative_signed.copy()
        signed[-1]=-99_800.
        b=causal_taker_window(2_500_000,visible,signed,
                               cumulative_notional,2_500_000,1,.35,100.)
        self.assertEqual(a,b)

    def test_same_visible_time_never_available_as_past(self):
        ts=np.array([1_000_000],np.int64)
        net=np.array([0.,1000.])
        tot=np.array([0.,1000.])
        self.assertFalse(causal_taker_window(1_000_000,ts,net,tot,100_000_000,
                                             1,.35,100.)[0])
        self.assertTrue(causal_taker_window(1_000_001,ts,net,tot,100_000_000,
                                            1,.35,100.)[0])

    def test_negative_net_cannot_confirm_buy(self):
        ts=np.array([1_000_000,1_200_000],np.int64)
        net=np.array([0.,-600.,-900.])
        tot=np.array([0.,600.,1000.])
        a=causal_taker_window(2_000_000,ts,net,tot,2_000_000,1,.35,100.)
        b=causal_taker_window(2_000_000,ts,net,tot,2_000_000,-1,.35,100.)
        self.assertFalse(a[0])
        self.assertTrue(b[0])


if __name__=="__main__":
    unittest.main()
