"""Synthetic unit tests: as-of Binance vs strictly prior OKX/basis at 100ms.

These values are toy inputs to check clock semantics, not market estimates.
"""
import unittest

import numpy as np

from analyze_binance_okx_gt_distribution import (
    MAX_QUOTE_AGE_NS, compute_gt_grid, describe, summarize,
)


class SignalDistributionTests(unittest.TestCase):
    def test_alignment_250ms_freshness_and_invalid_zero_fallback(self):
        b=1_000_000_000
        clock=np.array([b+100_000_000,b+200_000_000,b+300_000_000,
                        b+500_000_000],dtype=np.int64)
        okx=np.array([b-200_000_000,b+50_000_000,b+220_000_000],dtype=np.int64)
        okx_mid=np.array([99.,100.,101.])
        binance=np.array([b+100_000_000,b+160_000_000],dtype=np.int64)
        binance_mid=np.array([110.,112.])
        basis=np.array([0,b],dtype=np.int64)
        basis_val=np.array([1.,2.])
        x=compute_gt_grid(clock,okx,okx_mid,binance,binance_mid,basis,basis_val)
        # At exact current Binance timestamp, the Binance event has been consumed.
        # OKX and basis always strictly prior; no future reference.
        self.assertEqual(x["G_usdt_per_btc"][0],9.)
        self.assertEqual(x["G_usdt_per_btc"][1],10.)
        self.assertEqual(x["G_usdt_per_btc"][2],9.)
        self.assertFalse(x["valid"][3])
        self.assertTrue(np.isnan(x["G_usdt_per_btc"][3]))
        self.assertEqual(x["effective_shift_usdt_per_btc"][3],0.)
        self.assertEqual(MAX_QUOTE_AGE_NS,250_000_000)

    def test_quote_is_not_allowed_at_current_timestamp_for_okx(self):
        clock=np.array([1_000_000_000],dtype=np.int64)
        ot=np.array([999_000_000,1_000_000_000],dtype=np.int64)
        om=np.array([99.,500.])
        bt=np.array([1_000_000_000],dtype=np.int64)
        bm=np.array([110.])
        baseline=np.array([800_000_000,1_000_000_000],dtype=np.int64)
        base_mid=np.array([3.,100.])
        x=compute_gt_grid(clock,ot,om,bt,bm,baseline,base_mid)
        self.assertAlmostEqual(x["G_usdt_per_btc"][0],8.)

    def test_distribution_count_and_signed_or_absolute_quantiles(self):
        x=describe([-2.,-1.,0.,1.,2.])
        self.assertEqual(x["n"],5)
        self.assertEqual(x["p50"],0.)
        self.assertAlmostEqual(x["std"],np.sqrt(2.5))
        self.assertEqual(describe([np.nan])["n"],0.)


if __name__=="__main__":
    unittest.main()
