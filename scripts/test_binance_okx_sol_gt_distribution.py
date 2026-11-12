"""Unit tests: SOL signal's currency and target tick must not borrow BTC conventions.

Synthetic quotes only; not a financial result.
"""
import unittest

import numpy as np

from analyze_binance_okx_sol_gt_distribution import (
    OKX_PRICE_TICK, TARGET, compute_gt_grid, summary
)


class SolSignalSpecTests(unittest.TestCase):
    def test_target_is_okx_sol_linear_contract(self):
        self.assertEqual(TARGET.exchange, "okex-swap")
        self.assertEqual(TARGET.symbol, "SOL-USDT-SWAP")
        self.assertEqual(TARGET.coin, "SOL")
        self.assertEqual(TARGET.tick, .01)
        self.assertEqual(TARGET.ctval, 1.0)
        self.assertEqual(TARGET.lot, .01)
        self.assertEqual(OKX_PRICE_TICK, .01)

    def test_sol_gap_and_target_ticks_in_same_price_unit(self):
        # Simplified synthetic data, all prices USDT/SOL.
        t=1_000_000_000
        obs=np.array([t+100_000_000],dtype=np.int64)
        ot=np.array([t+1_000_000],dtype=np.int64)
        om=np.array([95.00],dtype=np.float64)
        bt=np.array([t+50_000_000],dtype=np.int64)
        bm=np.array([95.075],dtype=np.float64)
        baseline_t=np.array([t-100_000_000],dtype=np.int64)
        baseline=np.array([.025],dtype=np.float64)
        z=compute_gt_grid(obs,ot,om,bt,bm,baseline_t,baseline,
                          tick_size=OKX_PRICE_TICK)
        self.assertTrue(z["valid"][0])
        self.assertAlmostEqual(z["G_usdt_per_btc"][0],.05,places=10)
        self.assertAlmostEqual(z["G_ticks"][0],5.,places=10)
        # 0.05 USDT/SOL at SOL 95 is 5.26 bps, NOT 0.005 bps.
        self.assertAlmostEqual(1e4*z["G_usdt_per_btc"][0]/om[0],
                               5.263157894736842,places=9)

    def test_dataset_output_labels_do_not_contain_btc(self):
        from inspect import getsource
        from analyze_binance_okx_sol_gt_distribution import sample_day
        source=getsource(sample_day)
        self.assertIn('"G_usdt_per_sol"',source)
        self.assertIn('"G_okx_sol_ticks"',source)
        self.assertIn('tick_size=OKX_PRICE_TICK',source)


if __name__=="__main__":
    unittest.main()
