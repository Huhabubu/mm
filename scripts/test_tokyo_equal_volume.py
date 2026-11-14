"""Deterministic leakage and strict per-date fee/fill admission tests."""
import unittest
from copy import deepcopy
from check_tokyo_equal_volume import (
    CALIBRATION,DATES,MARKETS,validate_calibration,evaluate
)
from check_tokyo_0815_access import DAY,URL


def preset():
    return {
        "calibration_date":CALIBRATION,
        "controller_kind":"source_blind_pause",
        "uses_binance_signals":False,
        "tuning_dates":[CALIBRATION],
        "frozen_parameters_by_market":{
            m:{"fixed_before_testing":True,"probability":.02} for m in MARKETS}
    }


def rows(ratio=1.0,fee_ratio=1.0):
    return [
        {"market":m,"date":d,
         "z1_cancel_qs":{"fills":1000,"fee_usdt":10.},
         "source_blind_control":{"fills":round(1000*ratio),"fee_usdt":10*fee_ratio}}
        for m in MARKETS for d in DATES
    ]


class EqualVolumeTests(unittest.TestCase):
    def test_only_aug15_is_permitted_for_independent_calibration(self):
        self.assertEqual(DAY,CALIBRATION)
        self.assertIn("/2026/08/15/",URL)
        self.assertNotIn("2026/08/14",URL)
        bad=preset();bad["tuning_dates"]=list(DATES)
        with self.assertRaises(ValueError):
            validate_calibration(bad)

    def test_three_percent_requires_fills_and_fees_on_each_day(self):
        self.assertTrue(evaluate(preset(),rows(1.02,1.02))["all_markets_and_days_pass"])
        self.assertFalse(evaluate(preset(),rows(1.04,1.0))["all_markets_and_days_pass"])
        self.assertFalse(evaluate(preset(),rows(1.0,1.04))["all_markets_and_days_pass"])

    def test_same_total_with_bad_single_day_is_not_equal_volume(self):
        x=rows()
        x[0]["source_blind_control"]["fills"]=950
        x[1]["source_blind_control"]["fills"]=1050
        self.assertFalse(evaluate(preset(),x)["all_markets_and_days_pass"])

    def test_reject_binance_reading_and_missing_one_market(self):
        x=preset();x["uses_binance_signals"]=True
        with self.assertRaises(ValueError):
            validate_calibration(x)
        with self.assertRaises(ValueError):
            evaluate(preset(),rows()[:-1])


if __name__=="__main__":
    unittest.main()
