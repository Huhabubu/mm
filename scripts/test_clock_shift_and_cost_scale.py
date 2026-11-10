"""Synthetic falsification and quote-scale checks (not empirical evidence)."""
import csv
import gzip
import tempfile
import unittest
from pathlib import Path

import numpy as np

from binance_price_events import select_binance_events
from clock_shift_and_cost_scale import quote_economics, shifted_events
from predict_leadlag import read_quotes
from symmetric_price_event_response import evaluate_direction

FIELDS = ["exchange", "symbol", "timestamp", "local_timestamp",
          "bid_price", "ask_price", "bid_amount", "ask_amount"]


def write_bbo(path, rows):
    with gzip.open(path, "wt", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        for t, bid, ask in rows:
            writer.writerow({
                "exchange": "unit_test", "symbol": "TESTUSDT",
                "timestamp": t, "local_timestamp": t,
                "bid_price": bid, "ask_price": ask,
                "bid_amount": 3, "ask_amount": 2
            })


class TestClockShiftAndCostScale(unittest.TestCase):
    def test_forward_shift_erases_already_absorbed_move(self):
        with tempfile.TemporaryDirectory() as tmp:
            binance = Path(tmp) / "b.gz"
            okx = Path(tmp) / "o.gz"
            write_bbo(binance, [(0, 99, 101), (2_020_000, 100, 102)])
            write_bbo(okx, [(0, 99, 101), (2_060_000, 100, 102)])
            source, _ = select_binance_events(binance)
            target = read_quotes(okx)
            p0 = evaluate_direction(shifted_events(source, 0), target, 0)
            p25 = evaluate_direction(shifted_events(source, 25), target, 0)
            p50 = evaluate_direction(shifted_events(source, 50), target, 0)
            p60s = evaluate_direction(shifted_events(source, 60000), target, 0)
            self.assertEqual(p0["events_eligible"], 1)
            self.assertGreater(p0["responses"]["100"]["mean_signed_response_bps"], 0)
            self.assertGreater(p25["responses"]["100"]["mean_signed_response_bps"], 0)
            self.assertEqual(p50["responses"]["100"]["mean_signed_response_bps"], 0)
            self.assertEqual(p60s["responses"]["100"]["mean_signed_response_bps"], 0)
            # Only event timestamps change; direction/magnitude are frozen.
            self.assertEqual(source["up"]["sign"].tolist(), shifted_events(source, 25)["up"]["sign"].tolist())

    def test_negative_shift_is_diagnostic_future_binance_information(self):
        with tempfile.TemporaryDirectory() as tmp:
            binance = Path(tmp) / "b.gz"
            okx = Path(tmp) / "o.gz"
            write_bbo(binance, [(0, 99, 101), (2_020_000, 100, 102)])
            write_bbo(okx, [(0, 99, 101), (2_060_000, 100, 102)])
            source, _ = select_binance_events(binance)
            target = read_quotes(okx)
            advanced = evaluate_direction(shifted_events(source, -25), target, 0)
            self.assertEqual(advanced["events_eligible"], 1)
            self.assertGreater(advanced["responses"]["100"]["mean_signed_response_bps"], 0)

    def test_quote_grid_and_empirical_step(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "o.gz"
            # Simplified synthetic quotes, not true BTC data.
            write_bbo(p, [(0, 100.0, 100.2),
                          (1_500_000, 100.1, 100.3),
                          (3_500_000, 100.2, 100.4)])
            q = quote_economics(p, 0)
            self.assertEqual(q["invalid_rows"], 0)
            self.assertAlmostEqual(q["minimum_observed_positive_quote_price_step_usdt"], 0.1)
            self.assertAlmostEqual(q["median_mid_usdt"], 100.3)
            self.assertGreater(q["half_spread_bps"]["median"], 0)
            self.assertEqual(q["fee_assumption"]["maker_maker_round_trip_bps"], 4.0)
            self.assertEqual(q["fee_assumption"]["maker_taker_round_trip_bps"], 7.0)


if __name__ == "__main__":
    unittest.main()
