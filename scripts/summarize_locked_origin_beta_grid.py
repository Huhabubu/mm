"""Aggregate exactly matched 50min daily HBT beta-gradient runs.

Only fee-inclusive net marked equity is presented as a return metric.
"""
import argparse
import json
from pathlib import Path

BETAS=(0.,.25,.5,.75,1.)


def aggregate(reports):
    cases=[]
    for r in reports:
        if r["status"] == "no_glft_calibration":
            cases.append({"date":r["day"],"status":r["status"],"reason":r["calibration_diagnostic"]})
            continue
        if r["status"]!="complete":
            raise ValueError("Incomplete day: "+r["day"])
        v=r["variants"]
        if set(v)!={f"beta_{b:g}" for b in BETAS}:
            raise ValueError("Missing beta variant in "+r["day"])
        ref=v["beta_0"]
        one={"day":r["day"],"status":"complete",
             "gate":r["signal_diagnostic"],"variants":{}}
        for b in BETAS:
            key=f"beta_{b:g}"
            item=v[key]
            if (item["calibration_trace"]!=ref["calibration_trace"] or
                item["time_count"]["binance"]!=ref["time_count"]["binance"] or
                item["samples"]!=ref["samples"]):
                raise ValueError("Non-identical replay comparability: "+r["day"]+key)
            t=item["trading"]
            one["variants"][key]={
                "net_usdt":t["end_net_marked_equity_usdt"],
                "fee_usdt":t["fee_usdt"],
                "net_change_vs_beta0_usdt":
                    t["end_net_marked_equity_usdt"]-
                    ref["trading"]["end_net_marked_equity_usdt"],
                "fills":t["engine_fills"],"cancel_requests":t["cancel_requests"],
                "submissions":t["submissions"],
                "max_inventory_contracts":t["max_abs_position_contracts"],
                "max_drawdown_usdt":t["max_drawdown_usdt"],
            }
        cases.append(one)
    good=[x for x in cases if x["status"]=="complete"]
    if not good:
        raise ValueError("No valid day for independent-day aggregation")
    summary={}
    for beta in BETAS:
        key=f"beta_{beta:g}"
        rows=[d["variants"][key] for d in good]
        summary[key]={
            k:sum(row[k] for row in rows)/len(rows)
            for k in ("net_usdt","fee_usdt","net_change_vs_beta0_usdt",
                      "fills","cancel_requests","submissions",
                      "max_inventory_contracts","max_drawdown_usdt")}
        summary[key]["n_days"]=len(rows)
    return {"valid_days":len(good),"cases":cases,"daily_equal_means":summary,
            "scope":"10min warmup + 50min official HBT per day, pre-explored monthly days",
            "not_full_day":True,"not_new_holdout":True,
            "net_equity_includes_maker_fees":True,
            "not_modelled":"funding and end-of-day taker liquidation"}


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--input",default="gathered")
    p.add_argument("--output",default="reports/locked_origin_beta_grid_aggregate")
    p.add_argument("--min-completed-days",type=int,default=3)
    args=p.parse_args()
    files=sorted(Path(args.input).rglob("locked_origin_beta_grid_okx_BTC_*_50m.json"))
    if not files:
        raise ValueError("No beta run output found")
    reports=[json.loads(p.read_text(encoding="utf-8")) for p in files]
    if len({r["day"] for r in reports})!=len(reports):
        raise ValueError("Duplicate case days")
    result=aggregate(reports)
    if result["valid_days"]<args.min_completed_days:
        raise ValueError("Insufficient complete paired dates")
    out=Path(args.output);out.mkdir(parents=True,exist_ok=True)
    (out/"summary.json").write_text(json.dumps(result,indent=2,ensure_ascii=False)+"\n",encoding="utf-8")
    lines=["# Locked-origin Binance gate: beta gradient official HBT",
           "",
           "Tardis real BBO (Binance, OKX BTC/USDT-SWAP), native OKX L2/trades",
           "converted with official hftbacktest 2.4.4. Historical normal gap",
           "mean and population std use exact continuous time-weighting over",
           "the prior 600 seconds, recomputed every 5 seconds, no quote-age cap.",
           "Inside 1sigma refresh Binance/OKX origin prices; outside lock until",
           "reentry. Require >1.96sigma abnormal spread, Binance same direction",
           "and abs Binance move > abs OKX move, then shift GLFT centre by beta*gap.",
           "",
           "Same market tape, same calibration, same fees, inventory and 1ms/1ms",
           "order latency for all five beta values. No legacy P90/P50 gate.",
           "Signal measured using 2.5ms hypothetical Binance added delay.",
           "All net results are fee-inclusive, BTC target market only;",
           "not a 24-hour result or fresh holdout. Funding and final forced",
           "unwind/slippage are absent.",
           "",
           "| Beta | Mean net USDT/50m | Mean change vs beta0 USDT | Mean fee USDT | Mean fills | Mean cancels |",
           "|---:|---:|---:|---:|---:|---:|"]
    for beta in BETAS:
        k=f"beta_{beta:g}";z=result["daily_equal_means"][k]
        lines.append(f"| {beta:g} | {z['net_usdt']:+.4f} | {z['net_change_vs_beta0_usdt']:+.4f} | {z['fee_usdt']:.4f} | {z['fills']:.1f} | {z['cancel_requests']:.1f} |")
    lines.extend(["","## Individually paired days",""])
    for d in result["cases"]:
        if d["status"]!="complete":
            lines.append(f"- {d['date']}: no GLFT calibration, excluded")
            continue
        lines.append(f"- {d['day']}: source events {d['gate']['native_bbo_union_events']:,}, "
                     f">1.96sigma with anchor {d['gate']['events_above_1p96_sigma_with_anchor']:,}, "
                     f"passes source dominance {d['gate']['events_passing_binance_origin_gate']:,}, "
                     f"gate onsets {d['gate']['distinct_gate_onsets']:,}")
        for beta in BETAS:
            k=f"beta_{beta:g}";v=d["variants"][k]
            lines.append(f"  - beta {beta:g}: net {v['net_usdt']:+.4f} USDT, "
                         f"fee {v['fee_usdt']:.4f}, fills {v['fills']}, "
                         f"cancels {v['cancel_requests']}")
    lines.extend(["","### Interpretation limit",
        "This gate is a **directional excursion classification**, not proof of",
        "causal Binance price leadership. Past monthly free dates were already",
        "used for exploratory decisions; use fresh market dates/symbols for",
        "out-of-sample significance. The native target BBO source gate is",
        "rechecked against the HBT fused current target midpoint at execution."])
    (out/"summary.md").write_text("\n".join(lines)+"\n",encoding="utf-8")
    print("BETA_DAILY_AGGREGATE",json.dumps(result["daily_equal_means"]),flush=True)


if __name__=="__main__":
    main()
