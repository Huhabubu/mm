"""Deterministic synthetic R1-to-4B full-day gates; NOT empirical market results."""
import csv
import gzip
import io
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np

from bitget_4b_posthoc import source_risk_events, merged_windows, included_mask
from aggregate_bitget_4b_full import assert_report, group_means, DAYS, MODELS
from run_gate_btc_origin_glft import safe_gate_csv


def toy_report():
    sym="DOGEUSDT";day="2026-08-01";start=1_785_543_000_157_000_000
    quality={"hbt_fills":1}
    model={
        "initial_fit_ns":start,
        "trading":{"gross_marked_pnl_usdt":-1., "fee_usdt":.1,
                   "end_net_marked_equity_usdt":-1.1,
                   "trading_value_usdt":500.,
                   "active_inventory_samples":100,
                   "max_abs_position_base":3.,"engine_fills":1},
        "microsecond_collisions":1,
        "binance_origin_filter":{"same_timestamp_ties_blocked":0},
        "quote_quality":quality,
        "posthoc_slices":{"l2_reconnect_gap_pm5s":{},"oct_01_02utc":{}}
    }
    return {"status":"complete","venue":"bitget-futures",
       "symbol":sym,"day":day,"minutes_traded":1430,"no_full_day":False,
       "core_options":{"min_requote_ns":0,"fit_mode":"physical_1tick_poisson",
           "wait_book_after_fill":True,"post_only_clamp_mode":"unified",
           "cap_depth_at_visible_levels":False,
           "cross_timestamp_tie_mode":"microsecond"},
       "lossless_raw_L2_check":{"off_raw_hbt_grid_rows":0,
           "l2_crossed_post_message_book_checks":0,
           "initial_snapshot_complete":True},
       "gate_base_quantity_reference":{"position_cap_over_order_qty_gate":20.,
           "position_cap_over_order_qty_bitget":20.,
           "quantity_signed_difference_base":.3,
           "gate_target_qty_base":2.7,"bitget_actual_qty_base":3.0},
       "scenario":{"maker_fee_rate":.0002,"position_cap":60.,"qty":3.},
       "shared_first_eligible_entry_ns":start,
       "models":{k:dict(model) for k in MODELS}}


class FullDaySafetyTests(unittest.TestCase):
    def test_bitget_partial_trade_print_under_new_order_step(self):
        # Market trades are partial executions, not necessarily new orders.
        from check_okx_hft_data import SCHEMAS
        day="2026-08-01";begin=1785542400000000
        buffer=io.StringIO()
        writer=csv.DictWriter(buffer,fieldnames=SCHEMAS["trades"])
        writer.writeheader()
        writer.writerow({"exchange":"bitget-futures","symbol":"BTCUSDT",
                         "timestamp":begin+100,"local_timestamp":begin+200,
                         "id":"1","side":"buy","price":"100.0","amount":"0.00003"})
        data=gzip.compress(buffer.getvalue().encode("utf-8"))
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/"print.csv"
            with patch("run_gate_btc_origin_glft.urllib.request.urlopen",
                       lambda _req,timeout=240:io.BytesIO(data)):
                audit=safe_gate_csv("trades",day,begin+1000,path,10,
                    symbol="BTCUSDT",historical_tick=.1,
                    historical_lot=None,source_exchange="bitget-futures")
            self.assertEqual(audit["rows"],1)
            self.assertEqual(audit["invalid_rows"],0)
            self.assertIn("0.00003",path.read_text())

    def test_report_accepts_realistic_compatible_schema(self):
        self.assertTrue(assert_report(toy_report(),"DOGEUSDT","2026-08-01"))
        d=toy_report()
        d["models"]["both_1p0"]=dict(d["models"]["both_1p0"],
                                     initial_fit_ns=d["shared_first_eligible_entry_ns"]+1)
        with self.assertRaisesRegex(ValueError,"initial fit"):
            assert_report(d,"DOGEUSDT","2026-08-01")

    def test_l2_gap_pm5s_merges_and_excludes_full_gap(self):
        day=1_790_000_000_000_000
        with tempfile.TemporaryDirectory() as t:
            path=Path(t)/"l2.csv"
            with path.open("w",newline="") as f:
                w=csv.DictWriter(f,fieldnames=["local_timestamp","is_snapshot"])
                w.writeheader()
                for us,snap in ((day,True),(day+100000,False),
                                (day+2_200_000,False),(day+2_300_000,True),
                                (day+2_400_000,False)):
                    w.writerow({"local_timestamp":us,
                                "is_snapshot":"true" if snap else "false"})
            events=source_risk_events(path,"initial_snapshot_plus_native_deltas")
            self.assertEqual(events["l2_gap_count"],1)
            self.assertEqual(events["native_extra_snapshot_count"],1)
            windows=merged_windows(events,day*1000,(day+10_000_000)*1000)
            self.assertEqual(len(windows),1)
            times=np.array([(day+200000)*1000,(day+2_250_000)*1000,
                            (day+9_000_000)*1000],dtype=np.int64)
            mask=included_mask(times,windows)
            self.assertEqual(mask.tolist(),[False,False,True])

    def test_full_day_scope_and_inventory_threshold(self):
        self.assertEqual(len(DAYS),6)
        d=toy_report()
        d["models"]["glft"]["trading"]["active_inventory_samples"]=0
        with self.assertRaisesRegex(ValueError,"post-entry"):
            assert_report(d,"DOGEUSDT","2026-08-01")


if __name__=="__main__":
    unittest.main()
