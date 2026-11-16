"""Pure, deterministic stage5A risk-side inversion and frozen-accounting gates."""
import unittest
from gate_stage5a_pause_strategy import stage5_risk_pause_sides
from run_gate_stage5a_pause import same_trading


class Stage5ATests(unittest.TestCase):
    def test_binance_led_up_blocks_only_gate_ask(self):
        self.assertEqual(stage5_risk_pause_sides(3, 5.1), (False, True))

    def test_binance_led_down_blocks_only_gate_bid(self):
        self.assertEqual(stage5_risk_pause_sides(3, -5.1), (True, False))

    def test_inactive_signal_both_sides_resume(self):
        self.assertEqual(stage5_risk_pause_sides(3, 0.0), (False, False))

    def test_frozen_existing_models_never_pause(self):
        for mode in (0, 1, 2):
            for gap in (-100., 0., 100.):
                self.assertEqual(stage5_risk_pause_sides(mode, gap),(False,False))

    def test_frozen_model_ledger_must_match(self):
        base={"engine_fills":50,"end_position_contracts":-2.,
              "balance_usdt":3.,"fee_usdt":1.,"end_net_marked_equity_usdt":-4.,
              "trading_value_usdt":5000.}
        same_trading(base,dict(base),"2026-06-01")
        for key in base:
            mod=dict(base)
            mod[key]+=2 if key=="engine_fills" else 0.0001
            with self.assertRaisesRegex(ValueError,"Frozen defensive_1p0"):
                same_trading(base,mod,"2026-06-01")


if __name__=="__main__":
    unittest.main()
