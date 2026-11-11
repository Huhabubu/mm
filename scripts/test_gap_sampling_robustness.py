"""Exploratory sampling-clock diagnostics: deterministic non-market fixtures."""
import io
import gzip
import csv
import unittest
from unittest.mock import patch

import numpy as np

from full_day_gap_rolling import (
    NS, BBOStream, collapse_same_timestamp_last, run_one_day,
)
from gap_event_regression import event_gap_samples


class SamplingClockTests(unittest.TestCase):
    def test_last_row_per_microsecond_preserves_latest_price(self):
        ts = np.array([1_000, 1_000, 1_000, 2_000, 2_000, 3_000], np.int64)
        prices = np.array([100., 101., 102., 103., 104., 105.])
        a, b = collapse_same_timestamp_last(ts, prices)
        np.testing.assert_array_equal(a, [1_000, 2_000, 3_000])
        np.testing.assert_array_equal(b, [102., 104., 105.])
        c, d = collapse_same_timestamp_last(a, b)
        np.testing.assert_array_equal(a, c)
        np.testing.assert_array_equal(b, d)
        with self.assertRaises(ValueError):
            collapse_same_timestamp_last([2, 1], [1., 2.])

    def test_fixed_grid_strictly_past_at_exact_update_timestamp(self):
        ot = np.arange(15, dtype=np.int64)*NS + 100_000_000
        bt = np.arange(15, dtype=np.int64)*NS + 200_000_001
        om = np.full(15, 100.0)
        bm = np.full(15, 102.0)
        bm[8] = 112.0
        # With left-asof, exactly at the Binance update the old quote is used.
        t = np.array([bt[8], bt[8]+1], np.int64)
        s = event_gap_samples(ot, om, bt, bm, basis_seconds=3,
                              tau_ms=100, max_age_ms=2000, decision_times=t)
        np.testing.assert_array_equal(s["source"], [2, 2])
        np.testing.assert_array_equal(s["t_ns"], t)
        self.assertAlmostEqual(s["gap_usdt_per_btc"][0], 0.0)
        self.assertAlmostEqual(s["gap_usdt_per_btc"][1], 10.0)

    def test_nonmonotone_fixed_clock_fails(self):
        ot = np.arange(15, dtype=np.int64)*NS+100_000_000
        bt = np.arange(15, dtype=np.int64)*NS+200_000_000
        with self.assertRaises(ValueError):
            event_gap_samples(ot, np.ones(15)*100, bt, np.ones(15)*102,
                              decision_times=np.array([5*NS, 5*NS]))

    def test_full_window_chunk_invariance_for_grid_and_dedup(self):
        # Two venues at 10Hz, plus same-local-microsecond Binance revisions.
        # All values are synthetic. The split must never duplicate core rows.
        origin_us = 1793491200000000
        tape = {}
        for venue in ("okex-swap", "binance-futures"):
            fp = io.StringIO()
            w = csv.writer(fp)
            w.writerow(["local_timestamp", "bid_price", "ask_price"])
            for i in range(10200):
                stamp = origin_us + i*100_000 + (5000 if venue == "binance-futures" else 0)
                mid = 100. + i*.001 + (.1 if venue == "binance-futures" else 0.)
                w.writerow([stamp, mid-.05, mid+.05])
                if venue == "binance-futures" and i % 7 == 0:
                    # Same microsecond has a genuinely different final quote.
                    w.writerow([stamp, mid-.03, mid+.07])
            tape[venue] = gzip.compress(fp.getvalue().encode())

        orig = BBOStream
        def loader(day, venue, symbol):
            return orig(day, venue, symbol,
                        opener=lambda req, timeout: io.BytesIO(tape[venue]))
        with patch("full_day_gap_rolling.BBOStream", side_effect=loader):
            for mode in ("binance_last_per_microsecond", "fixed_50ms"):
                split = run_one_day("2026-11-01", hours=.25, chunk_minutes=5,
                                    sample_mode=mode)
                whole = run_one_day("2026-11-01", hours=.25, chunk_minutes=15,
                                    sample_mode=mode)
                self.assertEqual(split["sample_mode"], mode)
                self.assertEqual(split["sampling"]["eligible_event_rows"],
                                 whole["sampling"]["eligible_event_rows"])
                a = split["matched_out_of_sample_scores"]["combined"]
                b = whole["matched_out_of_sample_scores"]["combined"]
                self.assertEqual(a["events"], b["events"])
                for key in ("r2_vs_zero_fixed_alpha_beta",
                            "r2_vs_zero_rolling_beta_alpha0"):
                    self.assertAlmostEqual(a[key], b[key], places=10)
                self.assertGreater(a["events"], 0)


if __name__ == "__main__":
    unittest.main()
