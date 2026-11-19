"""Synthetic-only unit fixtures for report shape; not chapter5 evidence."""
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from report_bitget_multiasset_4a import (
    DAYS,SYMBOLS,EXPECTED,SOURCE_RUNS,evidence,load_cases,row_for,report,write
)

def fixture(symbol,day):
    feeds={}
    for name in ("trades","incremental_book_L2","quotes","binance_book_ticker"):
        x={"rows":10,"normalized_gate_status":"passed","blocking_checks":[],
           "max_local_gap_ms":100,
           "lag":{"p50_ms":5,"p95_ms":10,"p99_ms":20},
           "non_grid_prices":0}
        if name=="incremental_book_L2":
            x.update({"snapshot_groups":2,"reconnect_resnapshots":1,
                      "initial_snapshot_bid_levels":1000,
                      "initial_snapshot_ask_levels":1000,
                      "incremental_rows":1000,
                      "l2_unknown_before_initial_snapshot":0})
        if name=="trades":
            x.update({"observed_qty_min_positive":0.01,
                      "amount_not_on_proposed_order_step":0})
        feeds[name]=x
    return {"venue":"bitget-futures","symbol":symbol,"day":day,
            "status":"passed_normalized_gate_with_unresolved_metadata",
            "normalized_feeds_passed":True,"feeds":feeds,
            "historical_contract_date_effective_verified":False,
            "contract_candidate":{"tick_size_usdt":0.01,"min_order_qty_base":0.01,
                                  "qty_step_candidate_base":0.01,"quantity_unit":"ETH",
                                  "contract_base_multiplier":1}}

def write_fixtures(root, missing=None):
    for sym in SYMBOLS:
        for day in DAYS:
            if (sym,day)==missing:
                continue
            folder=(f"cross-4a-day-bitget-futures-{day}" if sym=="BTCUSDT"
                    else f"bitget-4a-{sym}-{day}")
            filename=(f"4a_bitget-futures_{day}.json" if sym=="BTCUSDT"
                      else f"4a_bitget-futures_{sym}_{day}.json")
            dest=root/folder/filename
            dest.parent.mkdir(parents=True,exist_ok=True)
            dest.write_text(json.dumps(fixture(sym,day)))
    tail=root/"bitget-4a-btc-2026-10-01-lag-tail"/"bitget_btc_oct_lag_tail.json"
    tail.parent.mkdir(parents=True,exist_ok=True)
    hourly=[{"hour_utc":h,"n_nonnegative":100,"p50_ms":5,
             "p95_ms":10,"p99_ms":20,"over_50ms_fraction":0} for h in range(24)]
    five=[{"start_utc":f"{i//12:02d}:{i%12*5:02d}","n":100} for i in range(288)]
    src={"hours_with_data":24,"observations":2400,
         "hours_with_p95_gt_50ms":0,"hours_with_more_than_5pct_lag_gt_50ms":0,
         "fraction_over_50ms":.01,"top3_hours_share_of_all_gt50ms":.05,
         "hourly":hourly,"five_minute":five}
    tail.write_text(json.dumps({"date":"2026-10-01","no_hbt":True,
                  "sources":{"bitget_quotes":src,"binance_book_ticker":src},
                  "diagnosis":{"descriptive_tail_classification":"mixed_or_transitioning"}}))

class BitgetMultiAsset4ATests(unittest.TestCase):
    def test_exact_24_dates_and_provenance(self):
        self.assertEqual(len(EXPECTED),24)
        with TemporaryDirectory() as tmp:
            root=Path(tmp);write_fixtures(root)
            found=load_cases(root)
            self.assertEqual(len(found),24)
            for sym in SYMBOLS:
                self.assertEqual(sum(k[0]==sym for k in found),6)
            self.assertEqual(row_for("BTCUSDT",DAYS[0],found)["source_run_id"],44459481767)
            self.assertEqual(row_for("DOGEUSDT",DAYS[0],found)["source_run_id"],44552849015)
            self.assertEqual(row_for("ETHUSDT",DAYS[0],found)["all_four_feeds_passed"],True)
            self.assertEqual(row_for("ETHUSDT",DAYS[0],found)["historical_contract_date_effective_verified"],False)

    def test_missing_day_stays_missing_and_not_zero(self):
        with TemporaryDirectory() as tmp:
            root=Path(tmp);write_fixtures(root,("SOLUSDT",DAYS[1]))
            found=load_cases(root)
            self.assertEqual(len(found),23)
            row=row_for("SOLUSDT",DAYS[1],found)
            self.assertEqual(row["status"],"missing")
            self.assertIsNone(row["incremental_book_L2_rows"])
            self.assertFalse(row["all_four_feeds_passed"])

    def test_all_24_synthetic_dates_generate_complete_report_shapes(self):
        # Explicitly synthetic fixtures, not paper observations.
        with TemporaryDirectory() as tmp:
            root=Path(tmp)
            write_fixtures(root)
            agg,tail=report(root,Path("configs/bitget_multiasset_contract_specs_4a.json"))
            self.assertEqual(agg["actual_asset_day_reports"],24)
            self.assertEqual(len(agg["rows"]),24)
            for sym in SYMBOLS:
                self.assertEqual(agg["counts_by_symbol"][sym]["actual_daily_reports"],6)
            self.assertTrue(agg["no_strategy_or_hbt_executed"])
            out=root/"out"
            write(out,agg,tail)
            for filename in ("summary.json","report.md","per_day_gate.csv",
                             "btc_oct_hourly_lag.csv","btc_oct_5minute_lag.csv",
                             "btc_oct_lag_tail.json"):
                self.assertTrue((out/filename).is_file(),filename)
            self.assertIn("BTC 2026-10-01", (out/"report.md").read_text())

    def test_reject_wrong_source_directory(self):
        with TemporaryDirectory() as tmp:
            root=Path(tmp);write_fixtures(root)
            path=root/"bitget-4a-ETHUSDT-2026-06-01"/"4a_bitget-futures_ETHUSDT_2026-06-01.json"
            with self.assertRaisesRegex(ValueError,"Untrusted source"):
                evidence(path,"SOLUSDT","2026-06-01")

if __name__=="__main__":
    unittest.main()
