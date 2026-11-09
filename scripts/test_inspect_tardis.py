"""Small deterministic tests of timestamp integrity and past-only quote alignment."""
import csv
import gzip
import tempfile
import unittest
from pathlib import Path

from inspect_tardis import align_quality, inspect_csv


HEADER = ["exchange", "symbol", "timestamp", "local_timestamp",
          "bid_price", "ask_price", "bid_amount", "ask_amount"]


def write_quotes(path, events):
    with gzip.open(path, "wt", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=HEADER)
        writer.writeheader()
        for ts, local, bid, ask in events:
            writer.writerow({"exchange": "test", "symbol": "BTC",
                             "timestamp": ts, "local_timestamp": local,
                             "bid_price": bid, "ask_price": ask,
                             "bid_amount": 1, "ask_amount": 2})


class TestDataQA(unittest.TestCase):
    def test_asof_does_not_use_future_event(self):
        with tempfile.TemporaryDirectory() as td:
            b = Path(td) / "binance.csv.gz"
            o = Path(td) / "okx.csv.gz"
            write_quotes(b, [(150_000, 150_000, 100, 101)])
            write_quotes(o, [(10_000, 10_000, 100, 101)])
            r = align_quality(b, o, 0, 400_000, step_us=100_000)
            self.assertEqual(r["counts"]["grid_points"], 4)
            self.assertEqual(r["counts"]["missing_initial_side"], 2)
            self.assertEqual(r["counts"]["both_fresh"], 2)

    def test_inspection_catches_crossed_and_backwards(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "quotes.csv.gz"
            write_quotes(p, [
                (10_000, 10_000, 100, 101),
                (11_000, 20_000, 101, 101),
                (9_000, 15_000, 102, 103),
            ])
            r = inspect_csv(p, "quotes", 0, 100_000)
            self.assertEqual(r["counts"]["rows"], 3)
            self.assertEqual(r["counts"]["crossed_or_locked_quote"], 1)
            self.assertEqual(r["counts"]["local_timestamp_backwards"], 1)
            self.assertEqual(r["counts"]["exchange_timestamp_backwards"], 1)


if __name__ == "__main__":
    unittest.main()
