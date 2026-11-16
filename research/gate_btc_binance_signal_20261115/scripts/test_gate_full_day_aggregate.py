"""Synthetic complete/missing date checks for six-day HBT summary writer."""
import json
import tempfile
from pathlib import Path
from aggregate_gate_origin_full_day import DAYS,KEYS,aggregate


def run():
    with tempfile.TemporaryDirectory() as d:
        root=Path(d)/"inputs"
        root.mkdir()
        for i,day in enumerate(DAYS):
            variants={}
            for j,name in enumerate(KEYS):
                variants[name]={
                    "beta":0. if name=="glft" else float(name.split("_")[1].replace("p",".")),
                    "trading":{
                        "end_net_marked_equity_usdt":float(i+j*.1),
                        "fee_usdt":-0.05,"engine_fills":10,"submissions":13,
                        "cancel_requests":3,
                        "max_abs_position_contracts":2.0,"max_drawdown_usdt":.3,
                    },
                    "binance_origin_filter":{
                        "binance_origin_activations":j,
                        "active_decisions":j*10,
                        "above_1p96sigma_checks":j*20,
                    },
                }
            (root/f"{day}_1430m.json").write_text(json.dumps({
                "day":day,"minutes_traded":1430,"status":"complete",
                "daily_pnl":True,"variants":variants
            }))
        dst=Path(d)/"out"
        summary=aggregate(root,dst)
        assert set(summary)==set(KEYS)
        assert abs(summary["glft"]["equal_day_mean_net_usdt"]-2.5)<1e-12
        assert abs(summary["both_0p25"]["equal_day_mean_delta_vs_glft_usdt"]-.1)<1e-12
        assert len((dst/"per_day_model.csv").read_text().splitlines())==55
        (root/f"{DAYS[0]}_1430m.json").unlink()
        try:
            aggregate(root,dst)
        except ValueError as e:
            assert "2026-06-01" in str(e)
        else:
            raise AssertionError("Missing day was silently accepted")
        print("GATE_FULL_DAY_AGGREGATION_TESTS_OK")


if __name__=="__main__":
    run()
