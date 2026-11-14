"""Strict no-Binance equal-volume controller admission.

The separate calibrator MUST have only 2026-08-15 source-free training.
Never fit pause probability or half-spread on 09/10/11 target outcomes.
This checker only evaluates frozen holdout case summaries; cannot create
a credible controller by guessing a parameter when 08-15 is unavailable.
"""
import argparse
import json
from pathlib import Path

DATES=("2026-09-01","2026-10-01","2026-11-01")
CALIBRATION="2026-08-15"
MARKETS=("SOL_okex-swap","DOGE_okex-swap","1000PEPE_okex-swap",
         "1000PEPE_bitget-futures")
ALLOWED=("source_blind_pause","source_blind_static_halfspread")
MAX_RELATIVE_ERROR=.03


def validate_calibration(x):
    if x.get("calibration_date")!=CALIBRATION:
        raise ValueError("Missing only-admitted post-migration calibration 2026-08-15")
    if x.get("uses_binance_signals") is not False:
        raise ValueError("Equal-volume controls must not read Binance")
    if x.get("controller_kind") not in ALLOWED:
        raise ValueError("Invalid source-blind control")
    if x.get("tuning_dates")!=[CALIBRATION]:
        raise ValueError("Parameter leakage: tuner accessed out-of-sample month-first dates")
    if set(x.get("frozen_parameters_by_market",{}))!=set(MARKETS):
        raise ValueError("All 4 venues need calibration from the same exact date")
    for market,p in x["frozen_parameters_by_market"].items():
        if not isinstance(p,dict) or not p.get("fixed_before_testing",False):
            raise ValueError("Controller not frozen "+market)
    return x


def evaluate(training,case_data):
    validate_calibration(training)
    if len(case_data)!=12:
        raise ValueError("Require 3 days x 4 markets, fully measured")
    seen=set()
    output={}
    for row in case_data:
        k=row["market"];d=row["date"]
        if k not in MARKETS or d not in DATES or (k,d) in seen:
            raise ValueError("Duplicated or non-admitted date/market")
        seen.add((k,d))
        source=row["z1_cancel_qs"]
        blind=row["source_blind_control"]
        ref=int(source["fills"])
        if ref<=0 or int(blind["fills"])<0:raise ValueError("Invalid fill denominator")
        sf=float(source["fee_usdt"]);bf=float(blind["fee_usdt"])
        if sf<=0:raise ValueError("Invalid original Maker fee reference")
        fr=abs(int(blind["fills"])-ref)/ref
        er=abs(bf-sf)/sf
        output[(k,d)]={"fill_error":fr,"fee_error":er,
                       "passes_per_date":fr<=MAX_RELATIVE_ERROR and er<=MAX_RELATIVE_ERROR}
    if seen!={(m,d) for m in MARKETS for d in DATES}:
        raise ValueError("Missing control or source case")
    res={}
    for m in MARKETS:
        days={d:output[(m,d)] for d in DATES}
        res[m]={"days":days,"all_three_days_pass":all(v["passes_per_date"] for v in days.values())}
    return {"calibration_date":CALIBRATION,"maximum_relative_error":MAX_RELATIVE_ERROR,
            "source_blind_frozen_controller":training["controller_kind"],
            "markets":res,"all_markets_and_days_pass":all(z["all_three_days_pass"] for z in res.values()),
            "does_not_prove_selection_alpha":True}


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--calibration",required=True)
    p.add_argument("--cases",required=True)
    p.add_argument("--out",required=True)
    a=p.parse_args()
    training=json.loads(Path(a.calibration).read_text())
    rows=json.loads(Path(a.cases).read_text())
    out=evaluate(training,rows)
    dest=Path(a.out);dest.mkdir(parents=True,exist_ok=True)
    (dest/"equal_volume_admission.json").write_text(json.dumps(out,indent=2,ensure_ascii=False))
    (dest/"equal_volume_admission.md").write_text(
        "# 东京期无 Binance 等量对照准入\n\n"
        f"独立校准：{CALIBRATION}；控制：{out['source_blind_frozen_controller']}。"
        f"三日逐日成交/手续费误差均 ≤3%：{out['all_markets_and_days_pass']}。\n\n"
        "只检查按日期冻结的成交及费用匹配；选择性因果仍须额外验证。\n")
    if not out["all_markets_and_days_pass"]:
        raise RuntimeError("Frozen controller fails <=3% every date; do not claim equal volume")


if __name__=="__main__":
    main()
