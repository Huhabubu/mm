"""R1 independent synthetic USDT accounting test. No empirical output."""
import unittest
from aggregate_cross_exchange_r1 import (
    aggregate_reports, decompose, independent_day_means, model_row)

def fake_model(gross,turnover,std,fee_rate=.0002):
    fee=turnover*fee_rate
    t={"end_net_marked_equity_usdt":gross-fee,
       "fee_usdt":fee,"trading_value_usdt":turnover,
       "position_std_base":std}
    horizons={str(h):{"total_markout":{"mean_bps":-1.2+h/10000.},
                      "total_fresh_count":8} for h in (500,1000)}
    return {"trading":t,"quote_quality":{
        "groups":{"all":{"horizons":horizons}}}}

def report():
    return {"status":"complete","day":"2026-08-01",
        "symbol":"ETHUSDT","venue":"bitget-futures",
        "scenario":{"qty":.03,"ctval":1.,
                    "position_cap":.6,"maker_fee_rate":.0002},
        "gate_base_quantity_reference":{
            "gate_target_qty_base":.027,"bitget_actual_qty_base":.03,
            "quantity_signed_difference_base":.003,
            "nominal_difference_at_gate_reference_usdt":9.,
            "position_cap_over_order_qty_bitget":20.},
        "models":{"glft":fake_model(-10.,1000.,.15),
                  "both_1p0":fake_model(-4.2,600.,.12)}}


class R1AccountingTests(unittest.TestCase):
    def test_exact_three_term_identity_including_fee_scenarios(self):
        r=report()
        rows=aggregate_reports([(r,"real_report.json")])
        g=next(z for z in rows if z["model"]=="glft")
        m=next(z for z in rows if z["model"]=="both_1p0")
        self.assertAlmostEqual(g["gross_bps"],-100.)
        self.assertAlmostEqual(m["gross_bps"],-70.)
        self.assertAlmostEqual(m["pos_std_over_order_qty"],4.)
        self.assertAlmostEqual(m["nominal_difference_at_gate_mid_usdt"],9.)
        self.assertAlmostEqual(m["fee_saved_usdt"],.08)
        self.assertAlmostEqual(m["vol_shrink_gross_usdt"],4.)
        self.assertAlmostEqual(m["quality_gross_usdt"],1.8)
        self.assertAlmostEqual(m["net_delta_vs_glft_usdt"],5.88)
        self.assertAlmostEqual(m["decomposition_residual_usdt"],0.,places=9)
        self.assertAlmostEqual(m["net_fee_0bps_usdt"],-4.2)
        self.assertAlmostEqual(m["net_fee_minus_0p5bps_usdt"],-4.17)
        self.assertAlmostEqual(m["net_fee_plus_2bps_usdt"],-4.32)
        self.assertAlmostEqual(m["total_markout_500ms_bps"],-1.15)
        self.assertAlmostEqual(m["total_markout_1000ms_bps"],-1.10)
        self.assertEqual(independent_day_means(rows)[0]["valid_days"],1)

    def test_ledger_invalid_fee_raises(self):
        r=report()
        r["models"]["glft"]["trading"]["fee_usdt"]+=.01
        with self.assertRaisesRegex(ValueError,"Fee ledger"):
            aggregate_reports([(r,"failed.json")])

    def test_no_duplicate_day(self):
        r=report()
        with self.assertRaisesRegex(ValueError,"Duplicate"):
            aggregate_reports([(r,"a.json"),(r,"b.json")])


if __name__=="__main__":
    unittest.main()
