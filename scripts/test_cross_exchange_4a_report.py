"""Offline report-schema tests. Synthetic fixtures are not real-day evidence."""
import json
import tempfile
import unittest
from pathlib import Path
from summarize_cross_exchange_4a import summary_of, write_report

class ReportContract(unittest.TestCase):
    def test_expected_nine_cases_and_missing_not_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            f=root/"case"/"4a_bitget-futures_2026-06-01.json"
            f.parent.mkdir(parents=True)
            f.write_text(json.dumps({
                "venue":"bitget-futures","day":"2026-06-01",
                "status":"blocked","normalized_feeds_passed":False,
                "feeds":{"trades":{"rows":100,"lag":{"p50_ms":3.0}}}
            }))
            s=summary_of(root)
            self.assertEqual(s["requested_days"],9)
            self.assertEqual(s["days_with_json"],1)
            self.assertEqual(s["status"],"incomplete")
            self.assertEqual(s["venues"]["bitget-futures"]["normalized_gate_passed_days"],0)
            self.assertTrue(any(x["status"]=="missing" for x in s["cases"]))
            out=root/"report"
            write_report(out,s)
            self.assertTrue((out/"summary.json").is_file())
            self.assertTrue((out/"per_day_gate.csv").is_file())
            self.assertTrue((out/"report.md").is_file())
            self.assertIn("未逐日认证", (out/"report.md").read_text()) if s[
                "contract_candidates_from_official_docs_not_history_proof"] else None

    def test_complete_nine_reports_still_not_hbt_permission(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            for venue,months in (("bitget-futures",range(6,12)),
                                  ("okex-swap",range(9,12))):
                for m in months:
                    date=f"2026-{m:02d}-01"
                    (root/f"4a_{venue}_{date}.json").write_text(json.dumps({
                        "venue":venue,"day":date,
                        "status":"passed_normalized_gate_with_unresolved_metadata",
                        "normalized_feeds_passed":True,
                        "feeds":{k:{"rows":1} for k in
                                 ("trades","incremental_book_L2","quotes","binance_book_ticker")},
                        "historical_contract_date_effective_verified":False
                    }))
            s=summary_of(root)
            self.assertEqual(s["days_with_json"],9)
            self.assertEqual(s["venues"]["bitget-futures"]["normalized_gate_passed_days"],6)
            self.assertEqual(s["venues"]["okex-swap"]["normalized_gate_passed_days"],3)
            self.assertTrue(s["no_backtest_performed"])
            self.assertFalse(s["venues"]["okex-swap"]["eligible_for_automatic_4b"])

if __name__=="__main__":
    unittest.main()
