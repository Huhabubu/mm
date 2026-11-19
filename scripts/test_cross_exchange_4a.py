"""Offline, simplified synthetic feed tests; NOT historical empirical evidence."""
import csv
import gzip
import io
import unittest
from datetime import datetime,timezone
from unittest.mock import patch

from audit_cross_exchange_4a import audit_stream, LagStats, on_grid, percentile, url_for

DAY="2026-09-01"
START=int(datetime.strptime(DAY,"%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()*1e6)
STOP=START+10_000_000

def mock_gzip_rows(kind, bad=False):
    if kind=="incremental_book_L2":
        header=["exchange","symbol","timestamp","local_timestamp","side","price","amount","is_snapshot"]
        entries=[
            ["bitget-futures","BTCUSDT",START+90_000,START+100_000,"bid","65000.0","1","true"],
            ["bitget-futures","BTCUSDT",START+90_000,START+100_000,"ask","65000.1","1","true"],
            ["bitget-futures","BTCUSDT",START+190_000,START+200_000,"bid","64999.9","2","false"],
            ["bitget-futures","BTCUSDT",START+8_990_000,START+9_000_000,"ask","65000.2","1","false"]
        ]
        if bad:
            entries[0][4]="ask"
    elif kind=="trades":
        header=["exchange","symbol","timestamp","local_timestamp","side","price","amount"]
        entries=[
            ["bitget-futures","BTCUSDT",START+90_000,START+100_000,"buy","65000.1",".01"],
            ["bitget-futures","BTCUSDT",START+8_990_000,START+9_000_000,"sell","65000.0",".02"]
        ]
    else:
        header=["exchange","symbol","timestamp","local_timestamp","bid_price","ask_price","bid_amount","ask_amount"]
        entries=[
            ["bitget-futures","BTCUSDT",START+90_000,START+100_000,"65000.0","65000.1","1","1"],
            ["bitget-futures","BTCUSDT",START+8_990_000,START+9_000_000,"65000.0","65000.2","1","1"]
        ]
    data=io.StringIO()
    writer=csv.writer(data)
    writer.writerow(header)
    writer.writerows(entries)
    return gzip.compress(data.getvalue().encode())

class RealAuditRulesTests(unittest.TestCase):
    def test_lag_quantiles_and_negative_clock(self):
        a=LagStats()
        for lag in [5_000, 5_100, 20_000, 35_000]:
            a.add(lag,0)
        a.add(-1000,0)
        out=a.result()
        self.assertEqual(out["n_nonnegative"],4)
        self.assertEqual(out["n_negative"],1)
        self.assertGreater(out["p95_ms"],20)
        self.assertLess(out["p50_ms"],6)

    def test_native_delta_and_initial_snapshot(self):
        with patch("urllib.request.urlopen",return_value=io.BytesIO(
                mock_gzip_rows("incremental_book_L2"))):
            d=audit_stream("bitget-futures","BTCUSDT","incremental_book_L2",
                          DAY,START,STOP,.1,.0001)
        self.assertEqual(d["normalized_gate_status"],"passed")
        self.assertEqual(d["snapshot_groups"],1)
        self.assertEqual(d["incremental_rows"],2)
        self.assertEqual(d["initial_snapshot_bid_levels"],1)
        self.assertEqual(d["initial_snapshot_ask_levels"],1)
        self.assertFalse(d["native_channel_proven_by_normalized_csv"])
        self.assertFalse(d["raw_sequence_number_available"])

    def test_incomplete_snapshot_blocks(self):
        with patch("urllib.request.urlopen",return_value=io.BytesIO(
                mock_gzip_rows("incremental_book_L2",bad=True))):
            d=audit_stream("bitget-futures","BTCUSDT","incremental_book_L2",
                          DAY,START,STOP,.1,.0001)
        self.assertEqual(d["normalized_gate_status"],"blocked")
        self.assertIn("initial_snapshot_missing_side",d["blocking_checks"])

    def test_same_local_timestamp_batch_not_counted_as_crossed_book(self):
        # Simplified synthetic example, not the paper's empirical order book.
        head=["exchange","symbol","timestamp","local_timestamp","side","price","amount","is_snapshot"]
        rows=[
            ["bitget-futures","BTCUSDT",START+90_000,START+100_000,"bid","65000.0","1","true"],
            ["bitget-futures","BTCUSDT",START+90_000,START+100_000,"ask","65000.1","1","true"],
            ["bitget-futures","BTCUSDT",START+190_000,START+200_000,"bid","64999.9","1","false"],
        ]
        ts=START+35_000_000
        rows.extend([
            ["bitget-futures","BTCUSDT",ts-100,ts,"bid","65000.2","1","false"],
            ["bitget-futures","BTCUSDT",ts-100,ts,"ask","65000.1","0","false"],
            ["bitget-futures","BTCUSDT",ts-100,ts,"ask","65000.3","1","false"],
            ["bitget-futures","BTCUSDT",START+69_000_000-100,START+69_000_000,
             "bid","65000.2","2","false"]
        ])
        output=io.StringIO()
        w=csv.writer(output);w.writerow(head);w.writerows(rows)
        with patch("urllib.request.urlopen",return_value=io.BytesIO(
                gzip.compress(output.getvalue().encode()))):
            day=audit_stream("bitget-futures","BTCUSDT","incremental_book_L2",
                              DAY,START,START+70_000_000,.1,.0001)
        self.assertGreater(day["sampled_reconstructed_book_checks"],0)
        self.assertEqual(day["sampled_reconstructed_book_crosses"],0)
        # This sparse synthetic day deliberately has >10s feed gaps;
        # only the 'crossed' field is under test.

    def test_bbo_silence_is_diagnostic_not_network_gap_failure(self):
        head=["exchange","symbol","timestamp","local_timestamp","bid_price","ask_price","bid_amount","ask_amount"]
        rows=[
            ["bitget-futures","BTCUSDT",START+90_000,START+100_000,"65000.0","65000.1","1","1"],
            ["bitget-futures","BTCUSDT",START+50_000_000-100,START+50_000_000,
             "65000.0","65000.2","1","1"]
        ]
        out=io.StringIO();w=csv.writer(out);w.writerow(head);w.writerows(rows)
        with patch("urllib.request.urlopen",return_value=io.BytesIO(
                gzip.compress(out.getvalue().encode()))):
            day=audit_stream("bitget-futures","BTCUSDT","quotes",DAY,
                              START,START+60_000_000,.1,.0001)
        self.assertEqual(day["normalized_gate_status"],"passed")
        self.assertEqual(day["bbo_unchanged_10s_count"],1)

    def test_quotes_trades_and_fail_closed_grid(self):
        for kind in ("trades","quotes"):
            with patch("urllib.request.urlopen",return_value=io.BytesIO(mock_gzip_rows(kind))):
                d=audit_stream("bitget-futures","BTCUSDT",kind,
                               DAY,START,STOP,.1,.0001)
            self.assertEqual(d["normalized_gate_status"],"passed")
            self.assertEqual(d["rows"],2)
        self.assertTrue(on_grid(65000.1,.1))
        self.assertFalse(on_grid(65000.13,.1))
        self.assertEqual(url_for("okex-swap","quotes",DAY,"BTC-USDT-SWAP"),
            "https://datasets.tardis.dev/v1/okex-swap/quotes/2026/09/01/BTC-USDT-SWAP.csv.gz")

if __name__=="__main__":
    unittest.main()
