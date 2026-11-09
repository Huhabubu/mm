"""Synthetic event-order, direction, delay and matched-control tests."""
import csv
import gzip
import tempfile
import unittest
from pathlib import Path

import numpy as np

from binance_price_events import (
    aggregate, match_size_control, response_events, select_binance_events,
)


FIELDS = ["local_timestamp", "bid_price", "ask_price", "bid_amount", "ask_amount"]


def native(path, rows):
    with gzip.open(path, "wt", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        for t, bid, ask, qb, qa in rows:
            writer.writerow(dict(local_timestamp=t, bid_price=bid, ask_price=ask,
                                 bid_amount=qb, ask_amount=qa))


def okx_quotes(t, m):
    return {"t": np.asarray(t, dtype=np.int64),
            "mid": np.asarray(m, dtype=np.float64)}


class TestBinancePriceEvents(unittest.TestCase):
    def setUp(self):
        self.start = 1_793_491_200_000_000  # Arbitrary UTC microsecond day boundary
        self.rows = [
            (self.start + 100_000, 99, 101, 2, 2),
            (self.start + 1_015_000, 100, 102, 2, 2),  # First price rise
            (self.start + 1_015_000, 101, 103, 2, 2),  # Same-us second price rise
            (self.start + 1_016_000, 101, 103, 3, 2),  # Size-only (recent sign +)
            (self.start + 1_019_000, 99, 101, 3, 2),   # Same bucket, ignored price event
            (self.start + 1_040_000, 98, 100, 3, 2),   # Next bucket, price down
        ]

    def test_only_first_price_event_in_bucket_and_file_order(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "bbo.csv.gz"
            native(path, self.rows)
            events, counters = select_binance_events(path)
            self.assertEqual(counters["raw_price_changes"], 4)
            self.assertEqual(counters["selected_up"], 1)
            self.assertEqual(counters["selected_down"], 1)
            self.assertEqual(events["up"]["t"].tolist(), [self.start + 1_015_000])
            self.assertAlmostEqual(float(events["up"]["impact_bps"][0]), 100, places=4)
            self.assertEqual(events["size_only"]["sign"].tolist(), [1])

    def test_okx_future_response_excludes_future_before_event(self):
        okx = okx_quotes(
            [self.start + 500_000, self.start + 1_000_000,
             self.start + 1_030_000, self.start + 1_080_000],
            [100, 100, 100.2, 100.2],
        )
        events = {"t": np.asarray([self.start + 1_015_000], dtype=np.int64),
                  "sign": np.asarray([1], dtype=np.int8),
                  "impact_bps": np.asarray([100.0])}
        immediate = response_events(okx, events, self.start, delay_ms=0)
        delayed = response_events(okx, events, self.start, delay_ms=25)
        self.assertEqual(immediate["n"], 1)
        self.assertAlmostEqual(float(immediate["forward_bps"][0, 0]), 20.0, places=3)
        self.assertAlmostEqual(float(delayed["forward_bps"][0, 0]), 0.0, places=3)
        self.assertEqual(immediate["state"].tolist(), ["flat"])

    def test_signed_down_event_and_separate_state(self):
        okx = okx_quotes([self.start + 500_000, self.start + 1_000_000,
                          self.start + 1_050_000], [100, 100, 99.8])
        evt = {"t": np.asarray([self.start + 1_040_000], dtype=np.int64),
               "sign": np.asarray([-1], dtype=np.int8),
               "impact_bps": np.asarray([-100.0])}
        actual = response_events(okx, evt, self.start)
        self.assertAlmostEqual(float(actual["forward_bps"][0, 0]), 20.0, places=2)
        self.assertGreater(aggregate(actual)["response"]["20"]["mean_signed_bps"], 0)

    def test_stratum_matching_preserves_price_event_weights(self):
        price = {
            "t": np.asarray([self.start + 1_000_000, self.start + 2_000_000]),
            "sign": np.asarray([1, 1]),
            "state": np.asarray(["flat", "flat"]),
            "age_us": np.asarray([10_000, 10_000]),
            "forward_bps": np.asarray([[2., 2., 2., 2., 2., 2.],
                                        [4., 4., 4., 4., 4., 4.]]),
            "n": 2, "day_start_us": self.start,
        }
        ctrl = dict(price)
        ctrl["forward_bps"] = np.asarray([[1., 1., 1., 1., 1., 1.]])
        ctrl["t"] = ctrl["t"][:1]
        ctrl["sign"] = ctrl["sign"][:1]
        ctrl["state"] = ctrl["state"][:1]
        ctrl["age_us"] = ctrl["age_us"][:1]
        ctrl["n"] = 1
        matched = match_size_control(price, ctrl)
        self.assertEqual(matched["matched_price_events"], 2)
        self.assertAlmostEqual(matched["by_horizon"]["20"]["price_minus_size_only_bps"], 2.0)


if __name__ == "__main__":
    unittest.main()
