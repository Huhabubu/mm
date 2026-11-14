"""Admission precision tests: synthetic; not exchange tick history."""
import unittest
from unittest.mock import patch
from experiment_source_defense_admission import live_tick, inspect, scan_bbo

class AdmissionTests(unittest.TestCase):
    def test_live_okx_tick(self):
        self.assertAlmostEqual(live_tick({"tickSz":"0.001"},"okex-swap"),.001)

    def test_live_bitget_step(self):
        self.assertAlmostEqual(live_tick({"priceEndStep":"5","pricePlace":"4"},
                                         "bitget-futures"),.0005)

    def test_no_live_spec_remains_unverified(self):
        self.assertIsNone(live_tick({"error":"timeout"},"okex-swap"))

    @patch("experiment_source_defense_admission.scan_bbo")
    @patch("experiment_source_defense_admission.current_spec")
    def test_tick_acceptance_and_history_disclaimer(self,spec,bbo):
        spec.return_value={"tickSz":".0001"}
        bbo.return_value={"mid_price":2., "rows":250,"bad_rows":0}
        result=inspect("2026-06-01","DOGE","okex-swap")
        self.assertEqual(result["verdict"],"reject_tick_range")
        spec.return_value={"tickSz":".0002"}
        result=inspect("2026-06-01","DOGE","okex-swap")
        self.assertEqual(result["verdict"],"eligible_tick_only")
        self.assertIn("6/6 GLFT fit",result["not_proven"])

    def test_pepe_needs_normalization(self):
        with patch("experiment_source_defense_admission.current_spec",return_value={"tickSz":"0.00000001"}):
            with patch("experiment_source_defense_admission.scan_bbo",return_value={"mid_price":.00001,"rows":20,"bad_rows":0}):
                r=inspect("2026-06-01","1000PEPE","okex-swap")
                self.assertTrue(r["binance_symbol_price_normalization_required"])


if __name__=="__main__":
    unittest.main()
