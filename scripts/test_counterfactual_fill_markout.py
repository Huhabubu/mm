"""Synthetic deterministic mechanism-audit tests: NOT empirical claims."""
import unittest
import numpy as np

from counterfactual_fill_markout import (
    matched_fill_indices, canceled_same_quote_before_fill,
    evaluate_mechanisms, evaluate_one_trace,
)

NS = 1_000_000_000


def fixture(ts_seconds, sides, prices_tick, cancels=()):
    return {
        "ts_ns": np.asarray([int(round(t*NS)) for t in ts_seconds], dtype=np.int64),
        "side": np.asarray(sides, dtype=np.int64),
        "price_tick": np.asarray(prices_tick, dtype=np.int64),
        "confirmed_canceled": np.asarray(
            [[int(round(t*NS)), side, px] for t, side, px in cancels],
            dtype=np.int64).reshape(-1,3)
    }


class MechanismTests(unittest.TestCase):
    def setUp(self):
        self.base = fixture([10.05, 10.15, 10.25], [1,1,1],
                            [1000,1000,1000])
        self.alt = fixture([10.05, 10.35], [1,1], [1000,999],
                           [(10.12,1,1000), (10.22,-1,1000)])
        self.ex_t = np.asarray(np.arange(9.5,11.5,0.05)*NS,dtype=np.int64)
        self.ex_mid = 100 - np.arange(len(self.ex_t))*.1

    def test_exact_price_matching_and_multiset_duplicate(self):
        a, b = matched_fill_indices(self.base, self.alt)
        np.testing.assert_array_equal(a, [True, False, False])
        np.testing.assert_array_equal(b, [True, False])
        dup_base = fixture([10.05,10.05], [1,1], [1000,1000])
        c,d = matched_fill_indices(dup_base, self.alt)
        self.assertEqual(int(c.sum()),1)
        self.assertEqual(int(d.sum()),1)

    def test_confirmed_exchange_cancel_requires_same_side_quote_and_past(self):
        matched, age = canceled_same_quote_before_fill(self.base, self.alt)
        np.testing.assert_array_equal(matched, [False, True, True])
        self.assertAlmostEqual(age[1], 30.)
        future_cancel = fixture([], [], [], [(10.16,1,1000)])
        m,_ = canceled_same_quote_before_fill(self.base, future_cancel)
        np.testing.assert_array_equal(m, [False,False,True])
        old = fixture([], [], [], [(9.,1,1000)])
        m,_ = canceled_same_quote_before_fill(self.base, old)
        self.assertFalse(m.any())
        wrong_quote = fixture([], [], [], [(10.1,1,999)])
        m,_ = canceled_same_quote_before_fill(self.base, wrong_quote)
        self.assertFalse(m.any())

    def test_full_100_500ms_exposure_and_partition_invariant(self):
        out = evaluate_mechanisms(
            {"beta_0":self.base,"beta_1":self.alt},self.ex_t,self.ex_mid)
        b = out["baseline"]["beta_0_all_actual_fills"]
        self.assertEqual(b["fills"],3)
        self.assertEqual(b["horizons"]["100"]["valid"],3)
        self.assertEqual(b["horizons"]["500"]["valid"],3)
        self.assertGreater(b["horizons"]["100"]["adverse_fraction"],.9)
        v = out["comparisons"]["beta_1"]
        self.assertEqual(v["exact_exchange_time_side_price_overlap"],1)
        self.assertEqual(v["baseline_only_exact_quote"],2)
        self.assertEqual(v["confirmed_same_quote_cancel_within_500ms_baseline_only"],2)
        self.assertEqual(v["groups"]["alternative_only_new_quote_fills"]["fills"],1)
        self.assertEqual(v["groups"]["baseline_quote_shared_exact_fill"]["fills"],1)
        self.assertEqual(v["groups"]["baseline_quote_not_filled_in_alternative"]["fills"],2)

    def test_markout_censoring_when_bbo_horizon_unavailable(self):
        ext = np.array([9*NS+850_000_000, 9*NS+860_000_000],dtype=np.int64)
        mids = np.array([100.,100.])
        t = fixture([10.05],[1],[1000])
        out = evaluate_one_trace(t,ext,mids)
        self.assertFalse(out[100][0][0])
        self.assertFalse(out[500][0][0])
        result = evaluate_mechanisms({"beta_0":t,"beta_1":fixture([],[],[])},ext,mids)
        self.assertEqual(result["baseline"]["beta_0_all_actual_fills"]["horizons"]["100"]["valid"],0)


if __name__ == "__main__":
    unittest.main()
