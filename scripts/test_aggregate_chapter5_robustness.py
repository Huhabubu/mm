"""Synthetic report-shape tests only; no synthetic prices/results enter experiments."""
import tempfile
import json
import unittest
from datetime import datetime, timezone
from pathlib import Path

from aggregate_chapter5_robustness import (
    DAYS, MODELS, SYMBOLS, expect_cases, validate_case, collect,
    make_rows, build_summary, mean,
)

def fixture(symbol="BTC_USDT", day="2026-06-01", cap=False):
    start=int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp()*1e9)+600_000_000_000
    settings={"min_requote_ns":0,"fit_mode":"physical_1tick_poisson",
              "wait_book_after_fill":True,"post_only_clamp_mode":"unified",
              "cap_depth_at_visible_levels":cap}
    def model():
        quality={"hbt_fills":2,"groups":{"all":{"horizons":{
            str(h):{"total_fresh_count":2,"future_quote_updated_count":1,
                    "total_markout":{"mean_bps":-0.2,"count":2},
                    "total_markout_after_quote_update":{"mean_bps":-0.3,"count":1}}
            for h in (100,500,1000)}}}}
        return {"initial_fit_ns":start,"calibration_wait_ns":0,
                "start_time_ns":start-600_000_000_000,
                "max_ops_per_sec":10,"resting_time_ratio":{
                    "bid":.4,"ask":.5,"verified_exchange_resting":False},
                "quote_quality":quality,"depth_cap_diagnostic":{
                    "enabled":cap,"verified_source_20level":cap,
                    "quote_decision_checks":100,"price_adjustment_checks":10,
                    "price_adjustment_fraction":.1,
                    "insufficient_depth_protection_checks":2,
                    "insufficient_depth_fraction":.02},
                "trading":{
                    "engine_fills":2,"buy_fills":1,"sell_fills":1,
                    "gross_marked_pnl_usdt":-1.0,
                    "end_net_marked_equity_usdt":-1.2,
                    "fee_usdt":.2,"trading_value_usdt":1000.,
                    "gross_marked_pnl_per_turnover_bps":-10.,
                    "net_marked_pnl_per_turnover_bps":-12.,
                    "position_std_contracts":2.,
                    "max_abs_position_contracts":6.}}
    return {"gate_symbol":symbol,"day":day,"status":"complete",
            "core_options":settings,"core_extension":True,
            "minutes_traded":1430,"binance_extra_visible_delay_ms":2.5,
            "shared_first_eligible_entry_ns":start,
            "scenario":{"maker_fee_rate":0.0002},
            "variants":{k:model() for k in (MODELS[:2] if cap else MODELS)}}

class Chapter5AggregationTests(unittest.TestCase):
    def test_expected_grid(self):
        self.assertEqual(len(expect_cases()),29)
        self.assertEqual(len(DAYS),6)
        self.assertEqual(len(SYMBOLS),4)

    def test_complete_identity(self):
        key,row=validate_case(fixture(),Path("fake.json"))
        self.assertEqual(key,("BTC_USDT","2026-06-01","base"))
        self.assertEqual(row["wait_s"],0)
        key,row=validate_case(fixture(cap=True),Path("fake.json"))
        self.assertEqual(key,("BTC_USDT","2026-06-01","cap"))

    def test_utc_wait_includes_initial_feed_delay_and_engine_wait_does_not(self):
        v=fixture()
        # First actual feed 0.356 seconds after 00:00:00.
        late=356_000_000
        v["shared_first_eligible_entry_ns"]+=late
        for row in v["variants"].values():
            row["initial_fit_ns"]+=late
            row["start_time_ns"]+=late
            # Core relative clock waits zero: fit is at first t0+600s.
            self.assertEqual(row["calibration_wait_ns"],0)
        _,result=validate_case(v,Path("fake.json"))
        self.assertAlmostEqual(result["wait_s"],0.356,places=6)

    def test_force_same_calibration_or_bad_fee_rejected(self):
        v=fixture()
        v["variants"]["both_1p0"]["initial_fit_ns"]+=1
        with self.assertRaisesRegex(ValueError,"identical"):
            validate_case(v,Path("fake.json"))
        v=fixture();v["scenario"]["maker_fee_rate"]=-0.0001
        with self.assertRaisesRegex(ValueError,"2bps"):
            validate_case(v,Path("fake.json"))
        v=fixture();v["variants"]["glft"]["quote_quality"]["hbt_fills"]=0
        with self.assertRaisesRegex(ValueError,"Markout"):
            validate_case(v,Path("fake.json"))

    def test_missing_day_remains_missing_not_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            (root/"a_1430m.json").write_text(json.dumps(fixture()))
            found=collect(root)
            self.assertEqual(len(found),29)
            self.assertEqual(sum(x["status"]=="complete" for x in found.values()),1)
            self.assertEqual(found[("SOL_USDT","2026-06-01","base")]["status"],"missing")
            rows=make_rows(found)
            summary=build_summary(found,rows)
            self.assertEqual(summary["status"],"incomplete")
            self.assertEqual(summary["calibration"]["SOL_USDT"]["valid_days"],0)
            self.assertIsNone(summary["groups"]["SOL_six_days"]["model_stats"]["glft"]["net_pnl_usdt"])
            self.assertEqual(summary["groups"]["BTC_six_days"]["model_stats"]["glft"]["valid_days"],1)

    def test_empty_mean_is_not_zero(self):
        self.assertIsNone(mean([]))

if __name__=="__main__":
    unittest.main()
