"""Offline six-day smoke tests; no market-data download or HBT replay."""
import csv
import io
import json
import tempfile
import unittest
from pathlib import Path
from zipfile import ZipFile

import numpy as np

from analyze_gate_1645_fill_quality import (
    DAYS, MODELS, aggregate, label_fills, model_day, depth_visibility_diagnostic
)
from audit_gate_stage5b_same_fill import calculate

NS = 1_000_000


def fake_fill():
    return dict(
        exchange_timestamp_ns=np.array([149,149,149], dtype=np.int64)*NS,
        local_observed_ns=np.array([150,150,150], dtype=np.int64)*NS,
        side=np.array([1,-1,1], dtype=np.int64),
        exec_price=np.array([99.,101.,99.5]),
        exec_qty=np.array([1.,1.,1.]),
    )


def fake_quotes():
    return (np.array([0,100,150,200,300,600,1100,1250], dtype=np.int64)*NS,
            np.array([100.,100.,105.,99.,101.,100.,99.5,100.]))


class SixDayQualityTest(unittest.TestCase):
    def test_aggregate_preserves_main_legacy_and_missing_depth(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            fill = fake_fill()
            ts, mid = fake_quotes()
            for day in DAYS:
                models = {}
                for model in MODELS:
                    _, record = model_day(
                        model, fill, {"trading": {"engine_fills":3}},
                        ts, mid, 2_000*NS, 0.001)
                    record["depth_visibility"] = depth_visibility_diagnostic(
                        day, model, None, fill)
                    models[model] = record
                (root/f"{day}_quality.json").write_text(json.dumps({
                    "day": day, "status": "complete", "models": models
                }))
            out = root/"out"
            aggregate(root, out)
            summary = json.loads((out/"summary.json").read_text())
            z = summary["results"]["glft"]["all"]["500"]
            self.assertEqual(z["days"], 6)
            self.assertEqual(z["valid_days"], 6)
            self.assertEqual(z["total_future_fresh_fills"], 18)
            self.assertIsNotNone(z["equal_day_mean_total_markout_bps"])
            missing = summary["depth_visibility_comparison"]["glft"]
            self.assertEqual(len(missing["five_20_level_days"]["missing_days"]),5)
            self.assertEqual(len(missing["november_400_initial_plus_deltas"]["missing_days"]),1)
            self.assertIn("Total Markout", (out/"report.md").read_text())
            with (out/"per_day_quality.csv").open(newline="") as f:
                rows=list(csv.DictReader(f))
            self.assertEqual(len(rows),108)
            self.assertIn("total_markout_bps",rows[0])
            self.assertIn("mean_markout_bps",rows[0])

    def test_stage5b_samefill_uses_reprocessed_stage2(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td)
            stage2=root/"stage2"
            stage2.mkdir()
            z3=root/"stage3_delivery.zip"
            fill=fake_fill()
            ts,mid=fake_quotes()
            annotation,_=label_fills(fill,ts,mid,2_000*NS,0.001)
            category=np.array(["threatened","inactive_or_no_effect","opposite"])
            with ZipFile(z3,"w") as destination:
                for day in DAYS:
                    buf=io.BytesIO()
                    np.savez_compressed(buf,category=category,
                        exchange_timestamp_ns=fill["exchange_timestamp_ns"],
                        side=fill["side"])
                    destination.writestr(
                        f"daily/{day}/{day}_stage3_classified.npz",buf.getvalue())
                    buf=io.BytesIO()
                    np.savez_compressed(buf,**annotation)
                    with ZipFile(stage2/f"gate_stage2_verified_{day[5:7]}.zip","w") as z:
                        z.writestr(f"{day}_glft_annotated.npz",buf.getvalue())
            summary={
                "daily_results": {
                    day: {"glft":{"fills":3,"zero_fee_pnl_usdt":0.1,"notional_usdt":1000},
                          "both_1p0":{"fills":3,"zero_fee_pnl_usdt":0.2,"notional_usdt":1000}}
                    for day in DAYS
                },
                "equal_day_summary": {
                    model: {"day_equal_mean": {
                        "zero_fee_pnl_usdt":0.1,"notional_usdt":1000
                    }} for model in MODELS
                }
            }
            stage4=root/"stage4.json"
            stage4.write_text(json.dumps(summary))
            out=root/"stage5b"
            calculate(z3,stage2,stage4,out)
            report=json.loads((out/"summary.json").read_text())
            self.assertIsNotNone(report["groups"]["threatened"]["500"]["total_markout_bps"])
            self.assertIsNotNone(report["groups"]["opposite"]["1000"]["total_markout_bps"])
            with (out/"stage5b_same_fill_by_day.csv").open(newline="") as f:
                rows=list(csv.DictReader(f))
            self.assertEqual(len(rows),54)
            self.assertIn("total_markout_minus_maker2bps_bps",rows[0])
            self.assertIn("capture_plus_markout_bps",rows[0])


if __name__=="__main__":
    unittest.main()
