"""Focused unit tests for event G0 summary; synthetic outputs are not research results."""
import unittest

import numpy as np
from hftbacktest import BUY, SELL

from g0_glft_baseline import POSITION_CAP, TRAIN
from g0_glft_event_driven import MIN_REQUOTE_NS, summarize


def example_result():
    trace = np.ones((2, 9), dtype=np.float64)
    trace[0, 1:6] = [0.1, 0.02, 2.0, 25.0, 0.3]
    trace[1, 1:6] = [0.2, 0.03, 3.0, 24.0, 0.4]
    raw = (
        TRAIN + 3000, 20, 61, 0, 61, 1, trace,
        10, 5, 1, 1, 2, 0.0, 0.1, 0.2, 0.03, 310.0, 78000.0,
        0.17, -0.1, 0.25, 0.0, 0.2,
        15.0, 1.0, 0.0,
        100, 200, 50, 100, 8500000.0,
        np.array([100_000_000, 200_000_000], dtype=np.int64),
        np.array([BUY, SELL], dtype=np.int64),
        1_000, 901_000_000_000
    )
    return raw


class EventG0SummaryTests(unittest.TestCase):
    def test_valid_report_and_correct_units(self):
        res = summarize(example_result(), minutes=5, rolling=True)
        self.assertEqual(MIN_REQUOTE_NS, 20_000_000)
        self.assertEqual(res["beta"], 0)
        self.assertEqual(res["time_count"]["binance"], 100)
        self.assertAlmostEqual(res["initial_parameters"]["k_per_tick"], 0.02)
        self.assertEqual(res["trading"]["engine_fills"], 2)
        self.assertEqual(res["fill_buy_sell_sign_sum"], 0)

    def test_reject_bad_fill_accounting(self):
        raw = list(example_result())
        raw[11] = 3  # Engine fills disagree with observed two orders.
        with self.assertRaises(RuntimeError):
            summarize(tuple(raw), minutes=5, rolling=True)

    def test_reject_unidentified_initial_calibration(self):
        raw = list(example_result())
        raw[5] = 0
        with self.assertRaises(RuntimeError):
            summarize(tuple(raw), minutes=5, rolling=True)

    def test_reject_position_above_cap(self):
        raw = list(example_result())
        raw[13] = POSITION_CAP + 0.2
        with self.assertRaises(RuntimeError):
            summarize(tuple(raw), minutes=5, rolling=True)

    def test_reject_truncated_replay(self):
        raw = list(example_result())
        raw[0] = TRAIN + 100
        with self.assertRaises(RuntimeError):
            summarize(tuple(raw), minutes=5, rolling=True)


if __name__ == "__main__":
    unittest.main()
