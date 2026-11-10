"""Time-ordering and zero-leak regression tests; synthetic arrays, no data download."""
import unittest
import numpy as np
from g0_queue_temporal import (DAYS, BANDS, validate_schedule, relative_error,
                               sum_training, distance_band_errors, assessment,
                               calculate_stability)
from g0_queue_intensity import HALF_TICK_BINS


class TemporalTests(unittest.TestCase):
    def test_calendar_order_must_be_unique_and_monthly(self):
        self.assertEqual(validate_schedule(DAYS),DAYS)
        for days in (("2026-10-01","2026-09-01"),
                     ("2026-09-01","2026-09-01"),
                     ("2026-09-01","2026-09-02")):
            with self.assertRaises(ValueError):
                validate_schedule(days)

    def test_combine_prior_training_only(self):
        a=np.zeros(HALF_TICK_BINS)
        b=np.zeros(HALF_TICK_BINS)
        a[1],b[1]=10,30
        e,n=sum_training([a,b],[a,b])
        self.assertEqual(e[1],40)
        self.assertEqual(n[1],40)
        with self.assertRaises(ValueError):
            sum_training([a],[a,b])

    def test_expected_count_bias_sign_and_empty(self):
        self.assertAlmostEqual(relative_error(10,13),.3)
        self.assertAlmostEqual(relative_error(10,7),-.3)
        self.assertIsNone(relative_error(0,3))

    def test_distance_band_bounds_and_sum(self):
        e=np.zeros(HALF_TICK_BINS)
        n=np.zeros(HALF_TICK_BINS)
        e[1]=100; e[4]=50; e[5]=50; e[200]=100
        n[1]=1; n[4]=1; n[5]=2; n[200]=1
        fit={"status":"ok","fit_min_distance_ticks":.5,"fit_max_distance_ticks":100,
             "A_fills_per_exposed_order_second":.02,"k_per_real_tick":.01}
        bins=distance_band_errors(fit,e,n)
        self.assertEqual(len(bins),len(BANDS))
        self.assertEqual(sum(x["actual_matched_fills"] for x in bins),5)
        self.assertEqual(sum(x["exposure_seconds"] for x in bins),300)
        self.assertGreater(sum(x["predicted_fills"] for x in bins),0)

    def test_cross_month_evaluation_never_sees_future_training(self):
        # High exposure, smoothly decreasing fills make every synthetic fit stable.
        days=("2026-09-01","2026-10-01","2026-11-01")
        arrays={}
        for index,day in enumerate(days):
            e=np.zeros(HALF_TICK_BINS)
            n=np.zeros(HALF_TICK_BINS)
            for i in range(1,201):
                e[i]=1000
                n[i]=round(500*np.exp(-.025*(i/2)))*(index+1)
            arrays[day]=(e,n,e,n)
        out=calculate_stability(arrays,days)
        self.assertEqual(len(out["same_day"]),3)
        self.assertEqual(len(out["previous_month"]),2)
        self.assertEqual(len(out["expanding_previous_months"]),2)
        for r in out["previous_month"] + out["expanding_previous_months"]:
            self.assertTrue(all(x<r["evaluation_date"] for x in r["training_dates"]))
        self.assertEqual(out["expanding_previous_months"][-1]["training_dates"],
                         list(days[:2]))
        self.assertEqual(out["expanding_previous_months"][-1]["evaluation_date"],
                         "2026-11-01")

    def test_unusable_fit_is_not_overclaimed(self):
        e=np.zeros(HALF_TICK_BINS)
        n=np.zeros(HALF_TICK_BINS)
        result=assessment({"status":"insufficient_exposure_or_fills"},
                          e,n,("2026-09-01",),"2026-10-01","test")
        self.assertNotIn("holdout_relative_count_error",result)
        self.assertEqual(result["holdout_validation"]["status"],"not_evaluable")


if __name__=="__main__":
    unittest.main()
