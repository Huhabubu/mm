"""Tests for bounded Tardis BBO streaming and strictly matured online beta.

All quotes in this file are synthetic; they are not empirical market results.
"""
import csv
import gzip
import io
import unittest
from unittest.mock import patch

import numpy as np

from full_day_gap_rolling import BBOStream, fit_from_stats, aggregate_rows, run_one_day, NS


def fake_feed_for_kind(kind, count=14000):
    """20min at 10Hz: valid basis grid and enough online 10min training."""
    fp = io.StringIO()
    writer = csv.writer(fp)
    writer.writerow(["local_timestamp", "bid_price", "ask_price"])
    base_us = 1793491200000000
    if kind == "binance":
        base_us += 5000
    for i in range(count):
        center = 100.0 + 0.003 * i + (0.1 if kind == "binance" else 0.0)
        writer.writerow([base_us + i*100000, center-0.05, center+0.05])
    return gzip.compress(fp.getvalue().encode())


class BBOTests(unittest.TestCase):
    def test_stream_chunk_boundaries_and_context_no_row_lost(self):
        rows = ("local_timestamp,bid_price,ask_price\n"
                "1000000,100,101\n"
                "1000000,101,102\n"
                "1500000,102,103\n"
                "2100000,103,104\n"
                "3200000,104,105\n")
        blob = gzip.compress(rows.encode())
        def open_stub(_request, timeout):
            return io.BytesIO(blob)
        with BBOStream("2026-11-01", "binance-futures", "BTCUSDT",
                       opener=open_stub) as stream:
            ts, mid = stream.until(1_600_000_000)
            self.assertEqual(len(ts), 3)
            self.assertEqual(stream.n_total, 3)
            self.assertEqual(stream.n_equal, 1)
            stream.retain_from(1_500_000_000)
            ts, mid = stream.until(4_000_000_000)
            self.assertTrue(np.array_equal(
                ts, np.array([1_000_000_000, 1_500_000_000, 2_100_000_000, 3_200_000_000])))
            self.assertEqual(stream.n_total, 5)

    def test_rolling_sufficient_statistics(self):
        x = np.arange(2.0, 102.0)
        y = 0.4 + 0.2*x
        sums = np.array([len(x), x.sum(), y.sum(), np.dot(x,x), np.dot(x,y)])
        got = fit_from_stats(sums, min_events=20)
        self.assertAlmostEqual(got["beta_free_intercept"], 0.2)
        self.assertAlmostEqual(got["alpha_usdt_per_btc"], 0.4)
        self.assertNotAlmostEqual(got["beta_zero_intercept"], 0.2)
        self.assertIsNone(fit_from_stats(sums, min_events=300))

    def test_labels_only_mature_after_future_time(self):
        st = np.array([3*NS + 999_999_999, 4*NS, 4*NS + 200_000_000])
        m = aggregate_rows(None, None, st, 0, tau_ms=100)
        self.assertTrue(np.array_equal(m, np.array([5, 5, 5])))
        # Nothing from this group may enter a fit at start t=4s.
        self.assertTrue(np.all(m > 4))

    def test_one_tape_streamed_in_several_chunks_matches_no_duplicate_core(self):
        cache = {"okex-swap": fake_feed_for_kind("okx"),
                 "binance-futures": fake_feed_for_kind("binance")}
        def opener(req, timeout):
            venue = "okex-swap" if "okex-swap" in req.full_url else "binance-futures"
            return io.BytesIO(cache[venue])
        orig = BBOStream
        def build(day, venue, symbol):
            return orig(day, venue, symbol, opener=opener)
        with patch("full_day_gap_rolling.BBOStream", side_effect=build):
            # 20min synthetic timeline, split into 3.5 chunks; compare with
            # one continuous core. 10min model training + >5min evaluation.
            short = run_one_day("2026-11-01", hours=1/3, chunk_minutes=5)
            long = run_one_day("2026-11-01", hours=1/3, chunk_minutes=20)
        self.assertEqual(short["sampling"]["eligible_event_rows"],
                         long["sampling"]["eligible_event_rows"])
        for key in ("combined","okx_trigger","binance_trigger"):
            a = short["matched_out_of_sample_scores"][key]
            b = long["matched_out_of_sample_scores"][key]
            self.assertEqual(a["events"], b["events"])
            for metric in ("r2_vs_zero_rolling_beta_alpha0",
                           "r2_vs_zero_fixed_alpha_beta"):
                self.assertAlmostEqual(a[metric], b[metric], places=11)
        self.assertGreater(short["rolling_beta_updates"]["n"], 0)
        self.assertEqual(short["sampling"]["missing_valid_rolling_model_events"], 0)


if __name__ == "__main__":
    unittest.main()
