"""Pure V6 invariants: clipped +60s schedule and fill groups."""
import unittest
import numpy as np
from experiment_source_defense_v6_mechanism import (
    clamp_shift, asof_direct, asof_urgent_tail, groups)
from experiment_source_defense_v6_clock_microprice import (
    distribution, microprice, next_same_direction_follow_us,
    signed_nearest_same_direction_ms)
from experiment_source_defense_v6_matched_exposure import schedule_pair


class MechanismInvariants(unittest.TestCase):
    def test_shift_never_wraps_future_into_first_60seconds(self):
        starts=np.array([10,100,850],np.int64)*1_000_000_000
        ends=np.array([11,101,855],np.int64)*1_000_000_000
        sides=np.array([1,-1,1],np.int64)
        out=clamp_shift(starts,ends,sides,900_000_000_000,60_000_000_000)
        self.assertEqual(out[0].tolist(),[70_000_000_000,160_000_000_000])
        self.assertEqual(out[3]["original_episodes"],3)
        self.assertEqual(out[3]["shifted_episodes_in_window"],2)
        self.assertFalse(out[3]["exact_50min_full_exposure_matched"])
        self.assertEqual(out[3]["missing_from_last_60sec_active_ns"],5_000_000_000)

    def test_shift_episodes_all_preserved_if_finish_before_cutoff(self):
        x=clamp_shift(np.array([10,100],np.int64),
                      np.array([25,125],np.int64),
                      np.array([1,-1],np.int64),200,60)
        self.assertTrue(x[3]["exact_50min_full_exposure_matched"])
        self.assertEqual(x[3]["original_active_ns"],x[3]["shifted_active_ns"])

    def test_protection_must_match_dangerous_side(self):
        t=np.array([110,130,160],np.int64)
        signs=np.array([-1,1,-1],np.int64)
        hit=asof_direct(t,signs,np.array([100]),np.array([150]),np.array([1]))
        self.assertEqual(hit.tolist(),[True,False,False])

    def test_urgent_tail_is_200ms_not_permanent(self):
        s=100_000_000
        t=np.array([s,s+199_999_999,s+200_000_000],np.int64)
        signs=np.array([-1,-1,-1],np.int64)
        hit=asof_urgent_tail(t,signs,np.array([s]),np.array([1]))
        self.assertEqual(hit.tolist(),[True,True,False])

    def test_microprice_equality_and_pressure_direction(self):
        p=microprice(np.array([100.,100.]),
                     np.array([102.,102.]),
                     np.array([2.,1.]),
                     np.array([1.,2.]))
        np.testing.assert_allclose(p,[101+1./3,100+2./3])

    def test_strict_retained_schedule_exposure_count_and_duration(self):
        t=1_000_000_000
        src,sh,m=schedule_pair(
            np.array([10,250],np.int64)*t,
            np.array([15,255],np.int64)*t,
            np.array([1,-1]),300*t)
        self.assertEqual(m["retained_episode_count"],1)
        self.assertEqual(m["retained_source_active_ns"],m["shifted_active_ns"])
        self.assertTrue(m["same_episode_count"])
        self.assertTrue(m["same_total_scheduled_duration"])
        self.assertEqual(sh[0][0]-src[0][0],60*t)

    def test_signed_clock_nearest_includes_target_before_binance(self):
        result=signed_nearest_same_direction_ms(
            (np.array([1000,3000],np.int64),np.array([1,1],np.int8)),
            (np.array([900,3150],np.int64),np.array([1,1],np.int8)))
        self.assertEqual(result["n"],2)
        self.assertEqual(result["binance_before_target_fraction_of_matched"],.5)
        self.assertEqual(result["binance_after_target_fraction_of_matched"],.5)
        self.assertAlmostEqual(result["median_ms"],.025)

    def test_clock_delta_and_unmatched(self):
        c=distribution(np.array([-1.,0.,2.]))
        self.assertEqual(c["negative_fraction"],1/3)
        x=next_same_direction_follow_us(
            (np.array([1000,2000]),np.array([1,-1])),
            (np.array([1500,2500]),np.array([1,-1])),max_ms=1)
        self.assertEqual(x["n"],2)
        self.assertEqual(x["median_ms"],.5)


if __name__=="__main__":
    unittest.main()
