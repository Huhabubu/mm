"""Aggregate Gate BTC Binance-origin HBT full-day fee-inclusive results.

Each date is one independent observation. Never treat 50minute experiments
as day observations; never substitute zero for an invalid/missing day.
"""
import argparse
import csv
import json
import math
from pathlib import Path

DAYS=("2026-06-01","2026-07-01","2026-08-01",
      "2026-09-01","2026-10-01","2026-11-01")
BETAS=(0.25,0.50,0.75,1.0)
KEYS=("glft",)+tuple(f"{mode}_{str(beta).replace('.','p')}"
                      for mode in ("both","defensive") for beta in BETAS)


def read_day(path,day):
    d=json.loads(path.read_text(encoding="utf-8"))
    if (d.get("day")!=day or d.get("minutes_traded")!=1430 or
            d.get("status")!="complete" or not d.get("daily_pnl")
            or set(d["variants"])!=set(KEYS)):
        raise ValueError(f"Day incomplete or wrong model matrix: {day} {path}")
    rows=[]
    base=d["variants"]["glft"]["trading"]["end_net_marked_equity_usdt"]
    for key in KEYS:
        m=d["variants"][key]; t=m["trading"]; c=m["binance_origin_filter"]
        net=float(t["end_net_marked_equity_usdt"])
        fee=float(t["fee_usdt"])
        fills=int(t["engine_fills"])
        if not all(map(math.isfinite,(net,fee))) or fills<0:
            raise ValueError(f"Invalid net/fee/fills {day}: {key}")
        rows.append({
            "date":day,"model":key,"beta":float(m["beta"]),
            "mode":"baseline" if key=="glft" else key.split("_")[0],
            "net_after_fees_usdt":net,
            "delta_net_vs_glft_usdt":net-base,
            "signed_fee_usdt":fee,
            "fills":fills,
            "submissions":int(t["submissions"]),
            "cancel_requests":int(t["cancel_requests"]),
            "max_abs_inventory_contracts":float(t["max_abs_position_contracts"]),
            "max_drawdown_usdt":float(t["max_drawdown_usdt"]),
            "binance_origin_activations":int(c["binance_origin_activations"]),
            "active_decisions":int(c["active_decisions"]),
            "above_1p96sigma_checks":int(c["above_1p96sigma_checks"]),
        })
    return rows,d


def aggregate(root,dst):
    all_rows=[]
    days_by_date={}
    for day in DAYS:
        matches=list(root.rglob(day+"_1430m.json"))
        if len(matches)!=1:
            raise ValueError(f"Expected exactly one completed result for {day}; found {matches}")
        rows,report=read_day(matches[0],day)
        all_rows.extend(rows)
        days_by_date[day]=report
    dst.mkdir(parents=True,exist_ok=True)
    fields=tuple(all_rows[0].keys())
    with (dst/"per_day_model.csv").open("w",encoding="utf-8",newline="") as f:
        writer=csv.DictWriter(f,fieldnames=fields)
        writer.writeheader();writer.writerows(all_rows)
    summary={}
    for model in KEYS:
        selected=[row for row in all_rows if row["model"]==model]
        if len(selected)!=len(DAYS):
            raise ValueError(f"Missing dates for {model}")
        summary[model]={
            "valid_dates":len(selected),
            "daily_net_usdt":[{"date":r["date"],"net_usdt":r["net_after_fees_usdt"],
                               "delta_vs_glft_usdt":r["delta_net_vs_glft_usdt"]}
                              for r in selected],
            "equal_day_mean_net_usdt":sum(r["net_after_fees_usdt"] for r in selected)/len(selected),
            "equal_day_mean_delta_vs_glft_usdt":
                sum(r["delta_net_vs_glft_usdt"] for r in selected)/len(selected),
            "equal_day_mean_signed_fee_usdt":
                sum(r["signed_fee_usdt"] for r in selected)/len(selected),
            "equal_day_mean_fills":sum(r["fills"] for r in selected)/len(selected),
            "equal_day_mean_cancels":sum(r["cancel_requests"] for r in selected)/len(selected),
        }
    (dst/"summary.json").write_text(json.dumps({
        "status":"complete","dates":DAYS,"minutes_traded_each_day":1430,
        "window_utc":"00:10:00–24:00:00",
        "net_fee_convention":"net=official_HBT_balance+marked_position_value-signed_fee",
        "negative_fee_means_maker_rebate":True,
        "fee_rate_scenario_is_historical_unverified":True,
        "results":summary,
    },ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    lines=["# Binance-origin Gate GLFT: six UTC-day fee-inclusive exploratory comparison",
           "", "All dates are previously explored historical samples, not blind tests.",
           "Net means official HBT end-of-day marked equity **after** fees/rebates;",
           "negative signed fees represent maker rebates in the Gate public-fee scenario.",
           "This does not include funding payments or final forced liquidation cost.",
           "Each date trades 00:10–24:00 UTC; first 10min are calibration only.",
           "Each model shares queue, delays, inventory/risk parameter and feed.",
           "","| Model | Equal-day net mean (USDT/day) | Mean net delta vs GLFT | Daily mean fills | Daily mean cancels |",
           "|---|---:|---:|---:|---:|"]
    for name in KEYS:
        q=summary[name]
        lines.append(f"| {name} | {q['equal_day_mean_net_usdt']:+.4f} | {q['equal_day_mean_delta_vs_glft_usdt']:+.4f} | {q['equal_day_mean_fills']:.1f} | {q['equal_day_mean_cancels']:.1f} |")
    lines +=["","## Daily fee-inclusive net equity (USDT, 1430min/day)",
            "","| Date | "+ " | ".join(KEYS) +" |",
            "|---|"+ "|".join(["---:"]*len(KEYS))+"|"]
    for day in DAYS:
        values=[]
        for name in KEYS:
            x=next(z for z in all_rows if z["date"]==day and z["model"]==name)
            values.append(f"{x['net_after_fees_usdt']:+.4f}")
        lines.append(f"| {day} | "+" | ".join(values)+" |")
    lines += ["","## Caveats",
              "- All beta and both/defensive modes were fixed before this six-day comparison.",
              "- Fee/rebate scenario uses Gate API at run time; historical account-specific fees unknown.",
              "- Tardis collector timestamps and assumed Binance extra 2.5ms delay do not prove production latency.",
              "- Signals use 600s time-weighted historical spread & sigma updated every 5 seconds.",
              "- The sign/magnitude and activation filters do not establish Binance causal leadership.",
              "- Compare fills and fee changes before attributing higher net PnL to lower adverse selection.",
              "- Additional 100/500ms fill markout audits and untouched test dates remain necessary.",
              ""]
    (dst/"report.md").write_text("\n".join(lines),encoding="utf-8")
    print("GATE_FULL_DAY_AGGREGATE_OK",
          json.dumps({k:{"net":v["equal_day_mean_net_usdt"],
                         "delta":v["equal_day_mean_delta_vs_glft_usdt"]}
                      for k,v in summary.items()}),flush=True)
    return summary


if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--input",default="full_day_downloads")
    p.add_argument("--out",default="reports/gate_origin_glft_full_day_aggregate")
    args=p.parse_args()
    aggregate(Path(args.input),Path(args.out))
