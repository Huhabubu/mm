"""Synthetic tests for native BBO vs reconstructed-L2 QA."""
import csv
import gzip
import tempfile
import unittest
from pathlib import Path
from compare_bbo_feeds import compare_on_grid, scan


COLS = ["exchange", "symbol", "timestamp", "local_timestamp",
        "ask_amount", "ask_price", "bid_price", "bid_amount"]


def write_file(path, rows):
    with gzip.open(path, "wt", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=COLS)
        writer.writeheader()
        for t, bid, ask in rows:
            writer.writerow({"exchange": "test", "symbol": "BTC", "timestamp": t,
                             "local_timestamp": t, "bid_price": bid,
                             "ask_price": ask, "bid_amount": 3, "ask_amount": 2})


class TestNativeBBO(unittest.TestCase):
    def test_no_future_source_leak(self):
        with tempfile.TemporaryDirectory() as tmp:
            a = Path(tmp) / "a.csv.gz"
            b = Path(tmp) / "b.csv.gz"
            # Native price changes at t=40ms, L2 at t=80ms.
            write_file(a, [(0, 100, 101), (40_000, 101, 102)])
            write_file(b, [(0, 100, 101), (80_000, 101, 102)])
            r = compare_on_grid(a, b, 0, 100_000)
            self.assertEqual(r["counts"]["grid"], 10)
            self.assertEqual(r["counts"]["both_present"], 10)
            self.assertEqual(r["counts"]["different_mid"], 4)

    def test_data_quality_scan_flags_bad_and_backwards_exchange_time(self):
        with tempfile.TemporaryDirectory() as tmp:
            a = Path(tmp) / "quotes.csv.gz"
            write_file(a, [(1000, 100, 101), (2000, 100, 100), (3000, 100, 102)])
            r = scan(a, 0, 10_000)
            self.assertEqual(r["rows"], 3)
            self.assertEqual(r["valid_rows"], 2)
            self.assertEqual(r["invalid_rows"], 1)
            self.assertEqual(r["mid_changes"], 1)


if __name__ == "__main__":
    unittest.main()
