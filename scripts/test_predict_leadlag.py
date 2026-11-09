"""Offline tests for sample causality, time delay, and regression consistency."""
import unittest
from datetime import datetime, timezone

import numpy as np

from predict_leadlag import (
    FixedRidge, TRAIN_DAYS, VALID_DAYS, TEST_DAYS, make_samples
)


def make_quotes(times, mids):
    times = np.asarray(times, dtype=np.int64)
    mids = np.asarray(mids, dtype=np.float64)
    return {
        "t": times,
        "mid": mids,
        "obi": np.full(len(times), 0.2, dtype=np.float64),
        "spread": np.full(len(times), 0.5, dtype=np.float64),
    }


class TestLeadLag(unittest.TestCase):
    def setUp(self):
        self.start = int(datetime(2026, 11, 1, tzinfo=timezone.utc).timestamp() * 1e6)
        s = self.start
        # All times are collection timestamps; a Binance jump at +1.115s.
        self.okx = make_quotes(
            [s + v for v in (500_000, 1_000_000, 1_100_000, 1_120_000, 1_140_000, 1_600_000)],
            [100, 100, 100, 100.2, 100.3, 100.5],
        )
        self.binance = make_quotes(
            [s + v for v in (500_000, 900_000, 1_115_000, 1_500_000)],
            [100, 100, 102, 102],
        )

    def test_future_binance_not_used_and_delay_applied(self):
        now = make_samples(self.okx, self.binance, self.start, delay_ms=0)
        delayed = make_samples(self.okx, self.binance, self.start, delay_ms=25)
        self.assertEqual(now["rows"], delayed["rows"])
        self.assertGreaterEqual(now["rows"], 3)
        # At OKX 1.100s, the 1.115s Binance jump is in the future.
        self.assertAlmostEqual(float(now["joint"][1, 4]), 0.0, places=4)
        # At OKX 1.120s, the jump has arrived only with zero added delay.
        self.assertGreater(float(now["joint"][2, 4]), 100.0)
        self.assertAlmostEqual(float(delayed["joint"][2, 4]), 0.0, places=4)
        # OKX-only baseline is unchanged by Binance delay.
        np.testing.assert_array_equal(now["local"], delayed["local"])

    def test_future_target_at_exact_horizon(self):
        s = make_samples(self.okx, self.binance, self.start, delay_ms=0)
        # At time 1.100s, future OKX mid at 1.120s is 100.2, +20 bps.
        self.assertAlmostEqual(float(s["y"][1, 0]), 20.0, places=2)

    def test_date_split_has_no_shared_day(self):
        self.assertEqual(len(set(TRAIN_DAYS + VALID_DAYS + TEST_DAYS)), 6)
        self.assertLess(max(TRAIN_DAYS), min(VALID_DAYS))
        self.assertLess(max(VALID_DAYS), min(TEST_DAYS))

    def test_ridge_regression_reproduces_simple_relation(self):
        x = np.arange(1, 101, dtype=float).reshape(-1, 1)
        y = np.column_stack([x[:, 0] / 10, -2 * x[:, 0]])
        p = FixedRidge().fit(x, y).predict(x)
        self.assertEqual(p.shape, y.shape)
        self.assertLess(float(np.mean((p - y) ** 2)), 0.1)


if __name__ == "__main__":
    unittest.main()
