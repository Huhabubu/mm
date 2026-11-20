"""Synthetic unit arithmetic: Gate's 80-USDT scenario -> exact Bitget base sizing.

These test inputs are illustrative, NOT 2026-08-01 observed market mids.
"""
import math
import csv
import tempfile
from pathlib import Path
import unittest
from qa_gate_btc_futures import fetch_contract
from run_bitget_core_glft import config_for, exact_gate_equivalent, source_l2_grid_check


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

    def test_eth_nearest_step_is_disclosed_and_inventory_multiple_matched(self):
        _,sp=config_for("ETHUSDT","configs/bitget_core_4b_smoke.json")
        gate=fetch_contract("ETH_USDT")
        # Simplified synthetic 3000 USDT reference; not a historic day.
        m=exact_gate_equivalent(3000.,gate,sp)
        self.assertAlmostEqual(m["gate_order_qty_base"],.027)
        self.assertAlmostEqual(m["bitget_order_qty_base"],.03)
        self.assertAlmostEqual(m["quantity_signed_difference_base"],.003)
        self.assertAlmostEqual(m["nominal_difference_at_gate_reference_usdt"],9.)
        self.assertFalse(m["strict_base_amount_equal"])
        self.assertAlmostEqual(m["position_cap_over_order_qty_gate"],20.)
        self.assertAlmostEqual(m["position_cap_over_order_qty_bitget"],20.)
        self.assertAlmostEqual(m["bitget_max_pos_base"],.6)

    def test_nearest_positive_minimum_for_tiny_target(self):
        _,sp=config_for("ETHUSDT","configs/bitget_core_4b_smoke.json")
        gate=fetch_contract("ETH_USDT")
        m=exact_gate_equivalent(50000.,gate,sp)
        self.assertGreaterEqual(m["bitget_order_qty_base"],sp["min_order_qty_base"])
        self.assertAlmostEqual(m["bitget_order_qty_base"]/sp["bitget_lot_size_base"],
                               round(m["bitget_order_qty_base"]/sp["bitget_lot_size_base"]))

    def test_grouped_raw_doge_book_keeps_6_digits_without_false_mid_message_cross(self):
        # Simplified synthetic example, not the empirical paper dataset.
        from datetime import datetime,timezone
        first=int(datetime(2026,8,1,tzinfo=timezone.utc).timestamp()*1e6)
        headers=("exchange","symbol","timestamp","local_timestamp",
                 "is_snapshot","side","price","amount")
        def event(micro,snapshot,side,price,qty):
            ts=str(first+micro)
            return ("bitget-futures","DOGEUSDT",ts,ts,snapshot,side,price,qty)
        # First complete snapshot. At a shared later timestamp, new bid
        # temporarily crosses old ask until same-message ask rows update.
        rows=[
            event(100000,"true","bid","0.069049","10"),
            event(100000,"true","ask","0.069060","11"),
            event(200000,"false","bid","0.069055","12"),
            event(200000,"false","ask","0.069060","0"),
            event(200000,"false","ask","0.069070","11"),
            event(300000,"false","bid","0.069055","13"),
        ]
        with tempfile.TemporaryDirectory() as d:
            source=Path(d)/"l2.csv"
            with source.open("w",newline="") as f:
                w=csv.writer(f);w.writerow(headers);w.writerows(rows)
            audit=source_l2_grid_check(source,.000001,.00001)
            self.assertEqual(audit["rows"],6)
            self.assertEqual(audit["off_raw_hbt_grid_rows"],0)
            self.assertGreaterEqual(audit["off_legal_quote_grid_rows"],1)
            self.assertTrue(audit["initial_snapshot_complete"])
            self.assertGreater(audit["l2_post_message_book_checks"],0)
            self.assertEqual(audit["l2_crossed_post_message_book_checks"],0)

    def test_doge_requires_hbt_raw_tick_one_tenth_of_quote_tick(self):
        _,spec=config_for("DOGEUSDT","configs/bitget_core_4b_smoke.json")
        self.assertEqual(spec["quote_tick_mult"],10)
        self.assertAlmostEqual(spec["hbt_tick_size"]*10,spec["legal_quote_tick"])
        self.assertEqual(spec["bitget_lot_size_base"],1)
        _,btc=config_for("BTCUSDT","configs/bitget_core_4b_smoke.json")
        self.assertEqual(btc["quote_tick_mult"],1)

if __name__=="__main__":
    unittest.main()
