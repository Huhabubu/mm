"""Stage5C no-April-data parameter choice and deterministic risk budget tests."""
import json
import tempfile
import unittest
from pathlib import Path
from run_gate_stage5c_holdout import choose_mode,lock,HOLDOUT


class HoldoutLockTests(unittest.TestCase):
    def fixture(self,results):
        return {
            "status":"complete",
            "evaluation_days":[f"2026-{m:02d}-01" for m in range(6,12)],
            "no_gamma_sigma_beta_changes":True,
            "day_equal_mean":results
        }

    def test_highest_fee_net_pnl_wins_when_inventory_rms_within_limit(self):
        r={"both_1p0":{"fee_net_pnl_usdt":-100.,"rms_inventory":5.},
           "defensive_1p0":{"fee_net_pnl_usdt":-90.,"rms_inventory":5.1},
           "risk_side_pause":{"fee_net_pnl_usdt":-80.,"rms_inventory":6.}}
        chosen,_=choose_mode(self.fixture(r))
        self.assertEqual(chosen,"risk_side_pause")

    def test_exclude_inventory_rms_above_125pct_bilateral(self):
        r={"both_1p0":{"fee_net_pnl_usdt":-100.,"rms_inventory":5.},
           "defensive_1p0":{"fee_net_pnl_usdt":-90.,"rms_inventory":5.1},
           "risk_side_pause":{"fee_net_pnl_usdt":-80.,"rms_inventory":6.5}}
        chosen,_=choose_mode(self.fixture(r))
        self.assertEqual(chosen,"defensive_1p0")

    def test_tie_break_favors_frozen_bilateral(self):
        r={name:{"fee_net_pnl_usdt":-99.,"rms_inventory":5.} for name in
           ("both_1p0","defensive_1p0","risk_side_pause")}
        chosen,_=choose_mode(self.fixture(r))
        self.assertEqual(chosen,"both_1p0")

    def test_no_lookahead_or_data_in_select_and_april_fixed(self):
        self.assertEqual(HOLDOUT,"2026-04-01")
        r={name:{"fee_net_pnl_usdt":-99.,"rms_inventory":5.} for name in
           ("both_1p0","defensive_1p0","risk_side_pause")}
        with tempfile.TemporaryDirectory() as tmp:
            source=Path(tmp)/"exploration.json"
            dest=Path(tmp)/"locked.json"
            source.write_text(json.dumps(self.fixture(r)))
            lock(source,dest)
            locked=json.loads(dest.read_text())
            self.assertTrue(locked["locked_before_holdout_data"])
            self.assertEqual(locked["holdout_date"],HOLDOUT)
            self.assertEqual(len(locked["stage5a_sha256"]),64)
            self.assertFalse(any("april" in k.lower() for k in self.fixture(r)))

    def test_incomplete_or_unaudited_six_day_rejected(self):
        with self.assertRaises(ValueError):
            choose_mode({"status":"complete","evaluation_days":[]})


if __name__=="__main__":
    unittest.main()
