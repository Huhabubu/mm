"""R1 synthetic Tardis capture-lag fixtures; NOT actual exchange measurements."""
import csv
from datetime import datetime, timezone
import gzip
import io
import unittest
from unittest.mock import patch

from audit_gate_capture_lag_r1 import audit_lag, DAYS, SYMBOLS


def gz_payload(exchange,symbol,day,lags):
    start=int(datetime.strptime(day,"%Y-%m-%d").replace(
        tzinfo=timezone.utc).timestamp()*1_000_000)
    buf=io.StringIO()
    w=csv.writer(buf)
    w.writerow(("exchange","symbol","timestamp","local_timestamp","bid_price","ask_price","bid_amount","ask_amount"))
    for i,lag in enumerate(lags):
        local=start+1000+i*100_000
        w.writerow((exchange,symbol,local-lag,local,1,2,1,1))
    return gzip.compress(buf.getvalue().encode())


class GateLagR1Tests(unittest.TestCase):
    def test_full_day_dates_and_4a_lag_histogram(self):
        self.assertEqual(len(SYMBOLS)*len(DAYS),24)
        day="2026-06-01"
        payload=gz_payload("gate-io-futures","ETH_USDT",day,[1000,1500,2000,2500])
        fake=lambda _req,timeout=120: io.BytesIO(payload)
        out=audit_lag("gate-io-futures","ETH_USDT","quotes",day,opener=fake)
        self.assertEqual(out["rows"],4)
        self.assertEqual(out["lag"]["n_nonnegative"],4)
        self.assertAlmostEqual(out["lag"]["p50_ms"],1.55)
        self.assertAlmostEqual(out["lag"]["p95_ms"],2.55)
        self.assertAlmostEqual(out["lag"]["p99_ms"],2.55)

    def test_clock_negative_is_recorded_separately(self):
        day="2026-11-01"
        payload=gz_payload("binance-futures","SOLUSDT",day,[-10,400,1200])
        out=audit_lag("binance-futures","SOLUSDT","book_ticker",day,
                      opener=lambda _req,timeout=120: io.BytesIO(payload))
        self.assertEqual(out["lag"]["n_negative"],1)
        self.assertEqual(out["lag"]["n_nonnegative"],2)


if __name__=="__main__":
    unittest.main()
