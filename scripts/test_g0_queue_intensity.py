"""Focus on identifiable exposure hazard and safe sample split; no exchange network."""
import unittest
import numpy as np
from g0_queue_intensity import (HALF_TICK_BINS, estimate_poisson_curve,
                                 check_holdout, pool_rows, observation_schedule)


class QueueIntensityTests(unittest.TestCase):
    def test_10ms_observation_preserves_100ms_decision_clock(self):
        coarse_ns, coarse_per_quote, coarse_samples = observation_schedule(100)
        fine_ns, fine_per_quote, fine_samples = observation_schedule(10)
        self.assertEqual((coarse_ns, coarse_per_quote, coarse_samples), (100_000_000,1,6000))
        self.assertEqual((fine_ns, fine_per_quote, fine_samples), (10_000_000,10,60000))
        self.assertEqual(coarse_samples//coarse_per_quote, fine_samples//fine_per_quote)
        self.assertEqual([(i+1)*coarse_ns for i in range(100)],
                         [(i+1)*fine_per_quote*fine_ns for i in range(100)])
        with self.assertRaises(ValueError):
            observation_schedule(25)

    def test_exposure_seconds_are_sampling_invariant(self):
        # 50 completed 10ms intervals and 5 completed 100ms intervals are both .5s.
        for ms in (10, 100):
            step_ns, _, _ = observation_schedule(ms)
            self.assertAlmostEqual((500_000_000//step_ns)*(step_ns/1e9),0.5)

    def test_exposure_rate_not_nominal_time(self):
        e = np.zeros(HALF_TICK_BINS)
        n = np.zeros(HALF_TICK_BINS)
        for i in range(2,40,2):
            e[i] = 100.0 + i
            n[i] = round(e[i]*3.0*np.exp(-0.055*(i/2)))
        fit=estimate_poisson_curve(e,n,max_tick=20)
        self.assertEqual(fit["status"],"ok")
        self.assertGreater(fit["A_fills_per_exposed_order_second"],2.0)
        self.assertTrue(0.02 < fit["k_per_real_tick"] < 0.10)

    def test_zero_fills_include_exposure_in_fit(self):
        e=np.zeros(HALF_TICK_BINS)
        n=np.zeros(HALF_TICK_BINS)
        for i in range(1,201):
            e[i]=100.0
            n[i]=round(100*.5*np.exp(-.05*(i/2)))
        fit=estimate_poisson_curve(e,n)
        self.assertEqual(fit["status"],"ok")
        self.assertGreater(fit["exposed_bins"],190)
        self.assertEqual(fit["total_exposure_seconds"],20_000)

    def test_insufficient_events_not_extrapolated(self):
        e=np.zeros(HALF_TICK_BINS)
        n=np.zeros(HALF_TICK_BINS)
        e[1:10]=300
        self.assertEqual(estimate_poisson_curve(e,n)["status"],
                         "insufficient_exposure_or_fills")
        self.assertEqual(check_holdout({"status":"insufficient_exposure_or_fills"},e,n)["status"],"not_evaluable")

    def test_holdout_is_evaluated_with_train_parameters(self):
        e=np.zeros(HALF_TICK_BINS)
        n=np.zeros(HALF_TICK_BINS)
        for i in range(1,201):
            e[i]=500
            n[i]=round(500*1.2*np.exp(-.03*i/2))
        fit=estimate_poisson_curve(e,n)
        result=check_holdout(fit,e,n)
        self.assertEqual(result["status"],"ok")
        self.assertAlmostEqual(result["predicted_fills"],result["observed_fills"],delta=10)
        self.assertGreaterEqual(result["exponential_poisson_deviance"],0)

    def test_pooled_counts_are_exact(self):
        e=np.zeros(HALF_TICK_BINS)
        n=np.zeros(HALF_TICK_BINS,dtype=int)
        e[1]=.1
        n[1]=1
        items=[{"exposure_by_half_tick_seconds":e.tolist(),
                "matched_fills_by_half_tick":n.tolist(),
                "exposure_matched_fills":1}]*2
        summed_e,summed_n=pool_rows(items)
        self.assertAlmostEqual(summed_e[1],.2)
        self.assertEqual(int(summed_n[1]),2)


if __name__=="__main__":
    unittest.main()
