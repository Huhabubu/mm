"""Focused offline data-crop regressions; no network or hftbacktest dependency."""
import gzip
import io
import tempfile
import unittest
from pathlib import Path

from check_okx_hft_data import extract_window

DAY = "2026-11-01"
BASE = 1793491200000000


def sample(rows):
    return gzip.compress(("\n".join(rows) + "\n").encode())


class ProbeTests(unittest.TestCase):
    def test_depth_snapshot_preserved_through_first_update(self):
        data = sample([
            "exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount",
            f"okex-swap,BTC-USDT-SWAP,{BASE},{BASE},true,bid,100,20",
            f"okex-swap,BTC-USDT-SWAP,{BASE},{BASE},true,ask,101,30",
            f"okex-swap,BTC-USDT-SWAP,{BASE+1},{BASE+1},false,bid,100,19",
            f"okex-swap,BTC-USDT-SWAP,{BASE+5_000_000},{BASE+5_000_000},false,ask,102,20",
        ])
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "depth.csv"
            stats = extract_window("incremental_book_L2", DAY, BASE + 2_000_000,
                                   path, opener=lambda *a, **k: io.BytesIO(data))
            self.assertEqual(stats["rows"], 3)
            self.assertTrue(stats["initial_snapshot_complete"])
            self.assertEqual(stats["same_local_ts"], 1)
            self.assertIn("true,ask", path.read_text())

    def test_missing_opening_snapshot_rejected(self):
        data = sample([
            "exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount",
            f"okex-swap,BTC-USDT-SWAP,{BASE},{BASE},false,bid,100,10",
        ])
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaisesRegex(ValueError, "no complete opening"):
                extract_window("incremental_book_L2", DAY, BASE+2_000_000,
                               Path(folder)/"bad.csv", opener=lambda *a, **k: io.BytesIO(data))

    def test_pre_snapshot_deltas_are_dropped(self):
        data = sample([
            "exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount",
            f"okex-swap,BTC-USDT-SWAP,{BASE},{BASE},false,bid,99,1",
            f"okex-swap,BTC-USDT-SWAP,{BASE+1},{BASE+1},true,bid,100,20",
            f"okex-swap,BTC-USDT-SWAP,{BASE+1},{BASE+1},true,ask,101,30",
            f"okex-swap,BTC-USDT-SWAP,{BASE+2},{BASE+2},false,bid,100,19",
        ])
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/"depth.csv"
            stats = extract_window("incremental_book_L2", DAY, BASE+2_000_000, path,
                                   opener=lambda *a, **k: io.BytesIO(data))
            self.assertEqual(stats["skipped_pre_snapshot"], 1)
            self.assertEqual(stats["rows"], 3)
            self.assertTrue(stats["initial_snapshot_complete"])
            self.assertNotIn("99,1", path.read_text())

    def test_local_timestamp_backwards_rejected(self):
        data = sample([
            "exchange,symbol,timestamp,local_timestamp,id,side,price,amount",
            f"okex-swap,BTC-USDT-SWAP,{BASE+3},{BASE+3},x,buy,100,2",
            f"okex-swap,BTC-USDT-SWAP,{BASE+2},{BASE+2},y,sell,101,3",
        ])
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaisesRegex(ValueError, "nonmonotonic"):
                extract_window("trades", DAY, BASE+2_000_000,
                               Path(folder)/"bad.csv", opener=lambda *a, **k: io.BytesIO(data))

    def test_contract_amount_not_normalized(self):
        data = sample([
            "exchange,symbol,timestamp,local_timestamp,id,side,price,amount",
            f"okex-swap,BTC-USDT-SWAP,{BASE},{BASE},x,buy,100,2",
        ])
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/"trades.csv"
            stats = extract_window("trades", DAY, BASE+2_000_000, path,
                                   opener=lambda *a, **k: io.BytesIO(data))
            self.assertEqual(stats["rows"], 1)
            self.assertTrue(path.read_text().strip().endswith(",2"))


if __name__ == "__main__":
    unittest.main()
