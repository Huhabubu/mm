"""Pure deterministic tests; synthetic numbers never represent market evidence."""
import unittest
import numpy as np

from full_day_gap_rolling import (
    ScoreAccumulator, GapBinAccumulator, minute_block_bootstrap,
)
from gap_g0_fill_markout import (
    analyze, gap_at_strict_past_times, strict_warmup_thresholds, NS,
)


class MinuteBootstrapTests(unittest.TestCase):
    def test_deterministic_interval_and_exact_pooled_point(self):
        scores = []
        for i in range(60):
            s = ScoreAccumulator()
            x = np.arange(10., 20., dtype=np.float64)
            y = 0.5 * x + np.sin(i/10)
            s.add(x, y, 0.5, 0.45, 0.0)
            scores.append(s)
        a = minute_block_bootstrap(scores, block_minutes=5, repetitions=300, seed=123)
        b = minute_block_bootstrap(scores, block_minutes=5, repetitions=300, seed=123)
        self.assertEqual(a, b)
        self.assertEqual(a["clusters"], 12)
        all_rows = ScoreAccumulator()
        for i in range(60):
            x = np.arange(10., 20., dtype=np.float64)
            y = .5*x + np.sin(i/10)
            all_rows.add(x, y, .5, .45, 0.)
        self.assertAlmostEqual(
            a["scores"]["rolling_beta_alpha0"]["point_r2_vs_zero"],
            all_rows.report()["r2_vs_zero_rolling_beta_alpha0"], places=13)
        lo, hi = a["scores"]["rolling_beta_alpha0"]["ci95_percentile"]
        self.assertLess(lo, hi)
        self.assertGreater(lo, 0.)

    def test_missing_samples_fails(self):
        with self.assertRaises(ValueError):
            minute_block_bootstrap([ScoreAccumulator() for _ in range(20)],
                                   repetitions=100)


class FillGapTests(unittest.TestCase):
    def test_signal_at_fill_minus_25ms_uses_prior_only_and_duplicate_times(self):
        # 90s of 100ms synthetic BBOs, with Binance jump at 70 seconds.
        ot = np.arange(900, dtype=np.int64)*100_000_000
        bt = ot + 5_000_001
        om = np.full(900, 100., np.float64)
        bm = np.full(900, 101., np.float64)
        bm[700:] = 111.
        # Every asof quote must be earlier than query. Fill times are synthetic.
        t = np.array([70*NS, 70*NS, 70*NS+30_000_000], np.int64)
        gap, valid = gap_at_strict_past_times(ot, om, bt, bm, t)
        self.assertTrue(valid.all())
        self.assertAlmostEqual(gap[0], gap[1])
        self.assertGreater(gap[-1], gap[0])

    def test_pretrade_bins_and_sign_aligned_risk(self):
        # Same synthetic market, with last-minute gap regime after 10min.
        ot = np.arange(7000, dtype=np.int64)*100_000_000
        bt = ot + 5_000_001
        om = np.full(7000, 100., np.float64)
        bm = np.full(7000, 101., np.float64)
        bm[6500:] = 111.
        thresholds = strict_warmup_thresholds(ot, om, bt, bm, start_ns=0)
        self.assertLess(thresholds["abs_gap_p90_usdt_per_btc"], 1.)
        fills = [{"exch_timestamp_ns": 660*NS+30_000_000,
                  "side_sign": 1, "execution_price_usdt_per_btc": 100,
                  "executed_qty_contracts": 0.1}]
        # Exchange BBO reference: from 100 to 99 after fill is adverse for a buy.
        ext = np.arange(6500, 7000, dtype=np.int64)*100_000_000
        exmid = np.full(len(ext), 100.)
        exmid[ext > 660*NS+30_000_000] = 99.
        a = analyze(fills, ext, exmid, ot, om, bt, bm, thresholds)
        self.assertEqual(a["n_official_fills"], 1)
        self.assertEqual(a["n_valid_aligned"], 1)
        self.assertEqual(a["groups"]["abs_extreme_gt_pretrade_p90"]["fills"], 1)
        # Buy sees Binance ABOVE OKX -> signal favourable, not adverse.
        self.assertEqual(a["groups"]["extreme_favourable_signal_risk_neg"]["fills"], 1)
        self.assertEqual(a["groups"]["extreme_adverse_signal_risk_pos"]["fills"], 0)
        self.assertLess(a["groups"]["all_valid"]["mean_total_markout_bps"], 0)


if __name__ == "__main__":
    unittest.main()
