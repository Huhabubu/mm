"""Synthetic unit arithmetic: Gate's 80-USDT scenario -> exact Bitget base sizing.

These test inputs are illustrative, NOT 2026-08-01 observed market mids.
"""
import math
import unittest
from qa_gate_btc_futures import fetch_contract
from run_bitget_core_glft import config_for, exact_gate_equivalent


class MatchedGateBaseQtyTests(unittest.TestCase):
    def test_contract_and_base_conversions_have_identical_exposure(self):
        for sym,mid in (("BTCUSDT",80000.),("SOLUSDT",125.),
                        ("DOGEUSDT",0.08)):
            _,sp=config_for(sym,"configs/bitget_core_4b_smoke.json")
            gate=fetch_contract(sp["gate_symbol"])
            m=exact_gate_equivalent(mid,gate,sp)
            self.assertTrue(m["strict_base_amount_equal"])
            self.assertAlmostEqual(m["gate_order_qty_base"],
                                   m["bitget_order_qty_base"],places=10)
            self.assertAlmostEqual(m["gate_max_pos_base"],
                                   m["bitget_max_pos_base"],places=10)
            self.assertEqual(m["quantity_abs_difference_base"]<1e-9,True)
            self.assertEqual(m["bitget_order_qty_base"]>0,True)

    def test_eth_alignment_can_fail_without_silent_round_up(self):
        _,sp=config_for("ETHUSDT","configs/bitget_core_4b_smoke.json")
        gate=fetch_contract("ETH_USDT")
        # Example spot mid yields 0.027 ETH in Gate contracts, which is NOT
        # legal if the Bitget order quantity step is really 0.01 ETH.
        with self.assertRaisesRegex(ValueError,"Cannot exactly align"):
            exact_gate_equivalent(3000.,gate,sp)

    def test_doge_requires_hbt_raw_tick_one_tenth_of_quote_tick(self):
        _,spec=config_for("DOGEUSDT","configs/bitget_core_4b_smoke.json")
        self.assertEqual(spec["quote_tick_mult"],10)
        self.assertAlmostEqual(spec["hbt_tick_size"]*10,spec["legal_quote_tick"])
        self.assertEqual(spec["bitget_lot_size_base"],1)
        _,btc=config_for("BTCUSDT","configs/bitget_core_4b_smoke.json")
        self.assertEqual(btc["quote_tick_mult"],1)

if __name__=="__main__":
    unittest.main()
