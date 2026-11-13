"""Recompute signed fill markout exclusively from the repaired-code 2.5ms HBT run.

The original accounting logic is reused without editing its formulas.
"""
import argparse
import csv
import json
from pathlib import Path

from analyze_multivenue_recorder_markout import (
    EXPECTED, aggregate, analyse_case, render_md,
)


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--source",default="gathered")
    p.add_argument("--out",default="reports/repaired_2p5ms_markout")
    p.add_argument("--source-run-id",required=True)
    a=p.parse_args()
    root=Path(a.source)
    out=Path(a.out)
    out.mkdir(parents=True,exist_ok=True)
    folders=sorted(root.glob("repaired-2p5-*"))
    if len(folders)!=24:
        raise RuntimeError(f"Expected 24 repaired 2.5ms cases, found {len(folders)}")
    cases=[analyse_case(folder,out,EXPECTED) for folder in folders]
    z=aggregate(cases)
    if z["count_complete"]!=20 or z["count_cases"]!=24:
        raise RuntimeError("Unexpected 2.5ms repaired calibration coverage")
    z["source_run_id"]=a.source_run_id
    z["code_status"]="P0 quotation reference fixed before official HBT replay"
    (out/"summary.json").write_text(json.dumps(z,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    md=render_md(z).replace(
        "原生Recorder NPZ和官方模拟成交NPZ保留在原始GitHub Action #43198245510",
        "原生Recorder NPZ和官方模拟成交NPZ来自本次修复后GitHub Action #"+a.source_run_id)
    md=(f"**审计版本：已修复GLFT目标中间价重复计算，源运行 #"
        f"{a.source_run_id}。所有旧版损益与markout数据均失效，此结果独立重算。**\n\n"+md)
    (out/"report.md").write_text(md,encoding="utf-8")
    with (out/"by_market_model.csv").open("w",encoding="utf-8",newline="") as f:
        w=csv.DictWriter(f,fieldnames=list(z["summary_rows"][0]))
        w.writeheader()
        w.writerows(z["summary_rows"])
    print("REPAIRED_MARKOUT_AUDIT_COMPLETE",z["count_complete"],"OF",z["count_cases"],flush=True)
    for x in z["summary_rows"]:
        print("REPAIRED_MARKOUT",x["market"],x["model"],"DAYS",x["valid_days"],
              "NET",x["net_mean_usdt"],
              "EDGE",x["edge_mean_usdt"],
              "HOLDING",x["hold_mean_usdt"],
              "M100",x["markout100_fresh_weighted_bps"],
              "M500",x["markout500_fresh_weighted_bps"],flush=True)


if __name__=="__main__":
    main()
