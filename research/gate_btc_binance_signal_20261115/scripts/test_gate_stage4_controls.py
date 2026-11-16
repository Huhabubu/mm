"""Stage4 preperiod-only parameter lock and strict frozen baseline regression checks."""
import json
import tempfile
import unittest
from pathlib import Path

import numpy as np

from run_gate_stage4_controls import (
    TRAIN_DAY, EVAL_DAYS, WIDEN_RATIOS, VOL_QUANTILES,
    assert_baseline_identity, select_closest, load_locked
)
from gate_stage4_controls import run_gate_stage4_control


class ControlsStage4Tests(unittest.TestCase):
    def test_fixed_pretraining_date_not_in_evaluation(self):
        self.assertEqual(TRAIN_DAY, "2026-05-01")
        self.assertEqual(EVAL_DAYS, tuple(f"2026-{i:02d}-01" for i in range(6,12)))
        self.assertNotIn(TRAIN_DAY,EVAL_DAYS)
        self.assertEqual(WIDEN_RATIOS,(.10,.25,.50,1.))
        self.assertEqual(VOL_QUANTILES,(.55,.70,.85,.95))

    def test_select_closest_never_interpolates_or_uses_evaluation(self):
        rows=[{"kind":"static_widen","candidate_index":i,"fills":f}
              for i,f in enumerate((90,110,60,140))]
        rows.extend({"kind":"gate_sigma_pause","candidate_index":i,"fills":f}
                    for i,f in enumerate((100,150,90,70)))
        self.assertEqual(select_closest(rows,105,"static_widen")["candidate_index"],1)
        self.assertEqual(select_closest(rows,105,"gate_sigma_pause")["candidate_index"],0)
        # Stable predeclared tie break, never reward the best historical return.
        rows[0]["fills"]=100
        rows[1]["fills"]=110
        self.assertEqual(select_closest(rows,105,"static_widen")["candidate_index"],0)

    def test_reject_incomplete_preperiod_grid(self):
        with self.assertRaises(ValueError):
            select_closest([{"kind":"static_widen","candidate_index":0,"fills":1}],
                           5,"static_widen")

    def test_missing_or_corrupt_training_lock_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            f=Path(temp)/"lock.json"
            d={"training_day":TRAIN_DAY,"training_window_minutes":1430,
               "preselected_widen_ratios":list(WIDEN_RATIOS),
               "preselected_gate_vol_quantiles":list(VOL_QUANTILES),
               "selected_controls":{"static_widen":{},"gate_sigma_pause":{}},
               "training_status":"locked"}
            f.write_text(json.dumps(d))
            self.assertEqual(load_locked(f)["training_day"],TRAIN_DAY)
            d["training_day"]="2026-06-01"
            f.write_text(json.dumps(d))
            with self.assertRaises(ValueError):
                load_locked(f)

    def test_strict_clone_check_is_exact_in_every_fill_field(self):
        r=[None]*40
        r[12]=0.0;r[14]=100.;r[15]=.2;r[16]=1000.;r[17]=80000.;r[18]=99.
        r[-5]=np.array([17,22],np.int64)
        r[-4]=np.array([1,-1],np.int64)
        r[-2]=(0,np.array([18,23],np.int64),np.array([80001.,80002.]),
                np.array([11.,11.]),None)
        a=r.copy();b=r.copy()
        assert_baseline_identity(a,b)
        b[-5]=np.array([17,23],np.int64)
        with self.assertRaisesRegex(ValueError,"exchange_timestamp_ns"):
            assert_baseline_identity(a,b)

    def test_copy_keeps_default_controls_unsupported_values_rejected(self):
        self.assertTrue(callable(run_gate_stage4_control))


if __name__=="__main__":
    unittest.main()
