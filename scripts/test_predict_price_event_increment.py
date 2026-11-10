"""Focused synthetic tests for matched P1/P2 Binance-price-event prediction."""
import unittest
from unittest.mock import patch

import numpy as np

import predict_price_event_increment as event_module
from predict_price_event_increment import (
    DELAYS_MS, MICRO_DELAYS_MS, TRAIN, VALID, TEST, HORIZONS_MS,
    event_samples, fit_compare, metrics, validation_shrink_scale
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

    def test_validation_shrink_scale_handles_overforecast_and_zero_signal(self):
        pred = np.array([[2.0, -2.0, 0.0, 1.0],
                         [4.0, -4.0, 0.0, -1.0]])
        truth = np.array([[1.0, 2.0, 1.0, 3.0],
                          [2.0, 4.0, 1.0, -3.0]])
        scale = validation_shrink_scale(pred, truth)
        np.testing.assert_allclose(scale, [0.5, 0.0, 0.0, 1.0])

    def test_calibration_is_fit_on_validation_only(self):
        okx, ev, start = make_native_samples()
        sample = event_samples(okx, ev, start)
        dates = {d: sample for d in TRAIN + VALID + TEST}
        first = fit_compare(dates)["validation_amplitude_calibration"]["scales_from_validation"]
        # Alter only the Oct/Nov labels: fitted shrink factors must be unchanged.
        changed = dict(dates)
        for d in TEST:
            changed[d] = dict(sample)
            changed[d]["y"] = sample["y"] * -4
        second = fit_compare(changed)["validation_amplitude_calibration"]["scales_from_validation"]
        self.assertEqual(first, second)

    def test_short_horizons_report_sparse_quote_arrivals(self):
        okx, events, start = make_native_samples()
        with patch.object(event_module, "HORIZONS_MS", (5, 10, 20, 50, 100)):
            s = event_module.event_samples(okx, events, start)
            self.assertEqual(s["y"].shape[1], 5)
            self.assertEqual(s["future_quote_arrivals"].shape, s["y"].shape)
            # If no OKX quote arrives, the as-of target mid cannot move.
            self.assertTrue(np.all(s["y"][~s["future_quote_arrivals"]] == 0))
            arrival = s["future_quote_arrivals"].astype(int)
            self.assertTrue(np.all(np.diff(arrival, axis=1) >= 0))
            outcome = event_module.metrics("synthetic", s, np.zeros_like(s["y"]),
                                           np.zeros_like(s["y"]))
            for h in (5, 10, 20, 50, 100):
                row = outcome["horizons"][str(h)]
                self.assertGreaterEqual(row["future_quote_arrival_pct"], row["nonzero_label_pct"])
                self.assertGreaterEqual(row["nonzero_label_pct"], row["nontrivial_label_pct"])

    def test_micro_delay_horizons_preserve_matching_and_recalculate_features(self):
        okx, events, start = make_native_samples()
        self.assertEqual(MICRO_DELAYS_MS, (0, 1, 2, 5, 10))
        with patch.object(event_module, "HORIZONS_MS", (5, 10, 20, 50, 100)):
            samples = {d: event_module.event_samples(okx, events, start, delay_ms=d)
                       for d in MICRO_DELAYS_MS}
        for d, s in samples.items():
            self.assertEqual(s["n"], len(s["y"]))
            self.assertEqual(s["y"].shape[1], 5)
            self.assertTrue(np.isfinite(s["y"]).all())
            np.testing.assert_array_equal(s["x1"], s["x2"][:, :5])
            self.assertTrue(np.all(s["y"][~s["future_quote_arrivals"]] == 0))
        # Even for the same originating Binance event, the local OKX
        # reference quote and/or age updates as the decision is delayed.
        self.assertFalse(np.array_equal(samples[0]["x1"], samples[10]["x1"]))

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
