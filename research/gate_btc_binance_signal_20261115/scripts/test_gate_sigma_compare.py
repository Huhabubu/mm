"""Focused regression for fee rebasing and six-day paired sigma comparison."""
import csv
import json
import sys
import tempfile
from pathlib import Path
from compare_gate_sigma_thresholds import DAYS, MODELS, main


def run():
    with tempfile.TemporaryDirectory() as tmp:
        root=Path(tmp)
        old=root/"old.csv"
        source=root/"new"
        source.mkdir()
        fields=("date","model","net_after_fees_usdt","signed_fee_usdt",
                "fills","cancel_requests","binance_origin_activations")
        with old.open("w", newline="", encoding="utf-8") as f:
            writer=csv.DictWriter(f,fieldnames=fields)
            writer.writeheader()
            for day in DAYS:
                for model in MODELS:
                    writer.writerow({
                        "date":day,"model":model,
                        "net_after_fees_usdt":-1.0,
                        "signed_fee_usdt":-.1,
                        "fills":10,"cancel_requests":20,
                        "binance_origin_activations":0 if model=="glft" else 2,
                    })
        for day in DAYS:
            variants={}
            for model in MODELS:
                extra=0.0 if model=="glft" else .1
                variants[model]={
                    "trading":{
                        "end_net_marked_equity_usdt":-1.3+extra,
                        "fee_usdt":.2,
                        "engine_fills":10,
                        "cancel_requests":20
                    },
                    "binance_origin_filter":{
                        "binance_origin_activations":0 if model=="glft" else 3
                    }
                }
            (source/f"{day}_1430m.json").write_text(json.dumps({
                "day":day,"status":"complete","minutes_traded":1430,
                "experimental_trigger_sigma_multiple":1.645,
                "scenario":{"maker_fee_rate":.0002},
                "fee_policy":"VIP0_assumed_maker_plus_0.020pct_no_rebate",
                "variants":variants
            }),encoding="utf-8")
        sys.argv=["compare_gate_sigma_thresholds.py",
                  "--old",str(old),"--new",str(source),
                  "--out",str(root/"out")]
        main()
        result=json.loads((root/"out/summary.json").read_text(encoding="utf-8"))
        assert result["baseline_reproduced_6_days"]
        assert result["results"]["glft"]["mean_delta_vip0"]==0
        assert abs(result["results"]["both_1p0"]["mean_delta_zero_fee"]-.1)<1e-8
        assert abs(result["results"]["both_1p0"]["mean_delta_vip0"]-.1)<1e-8
        assert (root/"out/paired_54_models.csv").exists()
        # Fail closed if an entire historical day is absent.
        (source/f"{DAYS[-1]}_1430m.json").unlink()
        try:
            main()
        except ValueError as exc:
            assert DAYS[-1] in str(exc)
        else:
            raise AssertionError("Missing day silently accepted")
    print("GATE_SIGMA_COMPARATOR_TESTS_OK")


if __name__=="__main__":
    run()
