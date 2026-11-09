"""Tests for symmetric event response rules and short event life."""
import csv
import gzip
import tempfile
import unittest
from pathlib import Path

import numpy as np

from binance_price_events import select_binance_events
from predict_leadlag import read_quotes
from symmetric_price_event_response import evaluate_direction

FIELDS = ("exchange", "symbol", "timestamp", "local_timestamp",
          "bid_price", "ask_price", "bid_amount", "ask_amount")


def write_quotes(path, entries):
    with gzip.open(path, "wt", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for time_us, mid, size in entries:
            w.writerow({
                "exchange": "test", "symbol": "TESTUSDT",
                "timestamp": time_us, "local_timestamp": time_us,
                "bid_price": mid - 1, "ask_price": mid + 1,
                "bid_amount": size, "ask_amount": 2,
            })


class TestReversePriceEvents(unittest.TestCase):
    def test_forward_has_response_but_reverse_already_absorbed(self):
        with tempfile.TemporaryDirectory() as root:
            bp = Path(root) / "binance.csv.gz"
            op = Path(root) / "okx.csv.gz"
            # Simplified synthetic: Binance mid first rises at 1.020s,
            # OKX mid rises at 1.060s. Not a real empirical claim.
            write_quotes(bp, [(0, 100, 2), (1_020_000, 101, 2),
                              (1_160_000, 101, 3)])
            write_quotes(op, [(0, 100, 2), (1_060_000, 101, 2),
                              (1_200_000, 101, 3)])
            be, bc = select_binance_events(bp)
            oe, oc = select_binance_events(op)
            self.assertEqual(bc["selected_up"], 1)
            self.assertEqual(oc["selected_up"], 1)
            forward = evaluate_direction(be, read_quotes(op), 0)
            reverse = evaluate_direction(oe, read_quotes(bp), 0)
            self.assertEqual(forward["events_eligible"], 1)
            self.assertEqual(reverse["events_eligible"], 1)
            self.assertAlmostEqual(forward["responses"]["100"]["mean_signed_response_bps"], 100)
            self.assertAlmostEqual(reverse["responses"]["100"]["mean_signed_response_bps"], 0)
            self.assertAlmostEqual(forward["target_first_move_latency_median_ms_conditional_moved"], 40)
            self.assertAlmostEqual(forward["target_first_move_agrees_pct_conditional_moved"], 100)

    def test_response_starts_from_delayed_decision_time(self):
        with tempfile.TemporaryDirectory() as root:
            bp = Path(root) / "b.csv.gz"
            op = Path(root) / "o.csv.gz"
            write_quotes(bp, [(0, 100, 2), (1_020_000, 101, 2)])
            write_quotes(op, [(0, 100, 2), (1_060_000, 101, 2)])
            events, _ = select_binance_events(bp)
            target = read_quotes(op)
            early = evaluate_direction(events, target, 0, delay_ms=0)
            delayed = evaluate_direction(events, target, 0, delay_ms=50)
            self.assertGreater(early["responses"]["100"]["mean_signed_response_bps"], 0)
            self.assertAlmostEqual(delayed["responses"]["100"]["mean_signed_response_bps"], 0)

    def test_simultaneous_target_tick_excluded(self):
        with tempfile.TemporaryDirectory() as root:
            bp = Path(root) / "b.csv.gz"
            op = Path(root) / "o.csv.gz"
            write_quotes(bp, [(0, 100, 2), (1_020_000, 101, 2)])
            write_quotes(op, [(0, 100, 2), (1_020_000, 101, 2)])
            events, _ = select_binance_events(bp)
            with self.assertRaisesRegex(ValueError, "No eligible"):
                evaluate_direction(events, read_quotes(op), 0)


if __name__ == "__main__":
    unittest.main()
