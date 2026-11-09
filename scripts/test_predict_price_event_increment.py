"""Focused synthetic tests for matched P1/P2 Binance-price-event prediction."""
import unittest

import numpy as np

from predict_price_event_increment import (
    DELAYS_MS, TRAIN, VALID, TEST, HORIZONS_MS,
    event_samples, fit_compare, metrics
)


def make_native_samples(with_simultaneous=False):
    # Artificial one-day subsegment, all in UTC microseconds.
    start = 1_799_500_800_000_000
    # OKX quotes every 10ms. This is deliberately simple fabricated data.
    t = start + np.arange(100_000, 3_000_001, 10_000, dtype=np.int64)
    mid = 100 + np.arange(len(t), dtype=np.float64) * 0.0003
    okx = {
        "t": t, "mid": mid,
        "obi": np.full(len(t), 0.1, dtype=np.float64),
        "spread": np.full(len(t), 0.2, dtype=np.float64),
    }
    price_up = start + np.arange(1_005_000, 1_305_000, 30_000, dtype=np.int64)
    price_down = start + np.arange(1_027_000, 1_327_000, 30_000, dtype=np.int64)
    if with_simultaneous:
        price_up = np.concatenate((price_up, np.array([start + 1_400_000])))
    events = {
        "up": {
            "t": price_up,
            "sign": np.ones(len(price_up), dtype=np.int8),
            "impact_bps": np.full(len(price_up), 0.12),
        },
        "down": {
            "t": price_down,
            "sign": -np.ones(len(price_down), dtype=np.int8),
            "impact_bps": np.full(len(price_down), -0.1),
        },
    }
    return okx, events, start


class TestPriceEventIncrement(unittest.TestCase):
    def test_equal_model_rows_and_label_horizons(self):
        okx, ev, start = make_native_samples()
        s = event_samples(okx, ev, start)
        self.assertEqual(s["n"], len(s["x1"]))
        self.assertEqual(s["n"], len(s["x2"]))
        self.assertEqual(s["n"], len(s["y"]))
        self.assertEqual(s["x1"].shape[1], 5)
        self.assertEqual(s["x2"].shape[1], 7)
        self.assertEqual(s["y"].shape[1], len(HORIZONS_MS))
        np.testing.assert_array_equal(s["x1"], s["x2"][:, :5])
        self.assertTrue(np.all(s["x2"][:, -2] == s["sign"]))
        self.assertTrue(np.all(s["x2"][:, -1] * s["sign"] > 0))

    def test_future_target_is_after_decision_only(self):
        okx, ev, start = make_native_samples()
        s0 = event_samples(okx, ev, start, delay_ms=0)
        s25 = event_samples(okx, ev, start, delay_ms=25)
        # All prices increase monotonically; later reference price means
        # lower remaining next-horizon change near end (though trend is linear).
        self.assertTrue(np.all(s0["y"][:, 0] >= 0))
        self.assertTrue(np.all(s25["y"][:, 0] >= 0))
        # Different delay must actually alter reference-time local features.
        self.assertFalse(np.array_equal(s0["x1"], s25["x1"]))

    def test_cross_venue_ties_are_excluded(self):
        okx, ev, start = make_native_samples(with_simultaneous=True)
        s = event_samples(okx, ev, start)
        self.assertGreaterEqual(s["excluded_simultaneous"], 1)
        self.assertEqual(s["n_source_events"], 21)
        self.assertEqual(s["n"], 20)

    def test_fixed_splits_and_horizons(self):
        self.assertEqual(set(TRAIN) & set(VALID + TEST), set())
        self.assertEqual(set(VALID) & set(TEST), set())
        self.assertEqual(DELAYS_MS, (0, 25, 50, 100))
        self.assertEqual(HORIZONS_MS, (20, 50, 100, 200, 500, 1000))

    def test_paired_fit_pipeline_smoke(self):
        okx, ev, start = make_native_samples()
        sample = event_samples(okx, ev, start)
        # This test checks shape/split pipelines, not statistical performance.
        p = {d: sample for d in TRAIN + VALID + TEST}
        r = fit_compare(p)
        self.assertEqual(r["training_events"], sample["n"] * 3)
        self.assertEqual(len(r["test_daily_equal_weight"]), len(HORIZONS_MS))
        for d in TEST:
            self.assertEqual(r["exploratory_test"][d]["n_events"], sample["n"])


if __name__ == "__main__":
    unittest.main()
