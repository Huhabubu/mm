"""Step 2 aggregate: 11 predeclared official-HBT bps variants, Added, P10 and CI.

Equal-volume comparison requires *prior-date* calibration and is reported
separately; this post-hoc table does not tune a G0 comparator on June fills.
"""
import argparse
import json
from pathlib import Path

from experiment_source_defense_aggregate import block_ci, load_npz
from experiment_source_defense_step1_diagnostic import fill_group_summary
from experiment_source_defense_pilot import classify_opportunities

MODES=("glft","beta1") + tuple(
    f"c{n}_{action}" for n in (1,2,3)
    for action in ("widen","cancel","okx_placebo")
)
HORIZONS=(100,500,1000,5000)


def stats_for_case(folder):
    src=json.loads((folder/"summary.json").read_text(encoding="utf8"))
    if set(src["variants"])!=set(MODES):
        raise ValueError("Incomplete predeclared Step2 modes")
    coin=src["coin"]
    ctval=.01 if coin=="BTC" else 1.
    baseline=src["variants"]["glft"]["trading"]
    g0=load_npz(folder/"glft_fill_diagnostics.npz")
    g0_gross=baseline["end_net_marked_equity_usdt"]+baseline["fee_usdt"]
    turnover=baseline["trading_value_usdt"]
    if turnover<=0:raise ValueError("Zero reference trade notional")
    result={"coin":coin,"day":src["day"],"reported_models":{},
            "params":src["sparse_params"],
            "limits":["one explored 50min only","30sec blocks are within-day not day-to-day CI",
                      "Matched exchange timestamp+side is not identical order",
                      "Residual turnover-scale gross is not identifiable selection alpha"]}
    for name in MODES:
        tr=src["variants"][name]["trading"]
        gross=tr["end_net_marked_equity_usdt"]+tr["fee_usdt"]
        r=tr["trading_value_usdt"]/turnover
        vol=g0_gross*(r-1)
        residual=gross-r*g0_gross
        feesaved=baseline["fee_usdt"]-tr["fee_usdt"]
        delta=tr["end_net_marked_equity_usdt"]-baseline["end_net_marked_equity_usdt"]
        if abs(feesaved+vol+residual-delta)>1.e-7:
            raise ValueError("Official HBT net fee/volume bridge fails")
        item={
            "cancels":tr["cancel_requests"],"fills":tr["engine_fills"],
            "fill_retention_ratio":tr["engine_fills"]/baseline["engine_fills"],
            "net_pnl_usdt_fee_included":tr["end_net_marked_equity_usdt"],
            "net_pnl_delta_vs_glft_usdt":delta,
            "terminal_inventory_contracts":tr["end_position_contracts"],
            "mean_abs_inventory_contracts":tr["mean_abs_inventory_contracts"],
            "rms_inventory_contracts":tr["rms_inventory_contracts"],
            "fees_usdt":tr["fee_usdt"],"fee_scenarios":src["variants"][name]["fee_scenarios_net_marked_usdt"],
            "net_delta_bridge":{
                "fee_saved_usdt":feesaved,"volume_scaled_gross_delta_usdt":vol,
                "gross_after_turnover_scaling_residual_usdt":residual,
                "residual_contains_inventory_execution_and_selection":True}}
        if name!="glft":
            f=load_npz(folder/(name+"_fill_diagnostics.npz"))
            drop,kept=classify_opportunities(g0,f)
            added,_=classify_opportunities(f,g0)
            item["Added"]=fill_group_summary(f,added,ctval)
            item["Dropped"]=fill_group_summary(g0,drop,ctval)
            item["Kept"]=fill_group_summary(g0,kept,ctval)
            item["selectivity"]={}
            for h in HORIZONS:
                a=item["Dropped"]["by_horizon"][f"{h}ms"]
                b=item["Kept"]["by_horizon"][f"{h}ms"]
                lift=(b["mean_markout_bps"]-a["mean_markout_bps"] if
                      a["mean_markout_bps"] is not None and
                      b["mean_markout_bps"] is not None else None)
                p10=(b["p10_markout_bps"]-a["p10_markout_bps"] if
                      a["p10_markout_bps"] is not None and
                      b["p10_markout_bps"] is not None else None)
                item["selectivity"][f"{h}ms"]={
                    "lift_mean_bps":lift,"p10_kept_minus_dropped_bps":p10,
                    "block_bootstrap_30s_ci":block_ci(g0,f,h),
                }
            item["redlines_descriptive"]={
                "fills_gte_80pct":item["fill_retention_ratio"]>=.8,
                "fills_gte_75pct":item["fill_retention_ratio"]>=.75,
                "cancels_no_more_than_1p5x_baseline":tr["cancel_requests"]<=1.5*baseline["cancel_requests"],
                "selectivity_mean_positive_100_500ms":all(
                    item["selectivity"][f"{h}ms"]["lift_mean_bps"] is not None and
                    item["selectivity"][f"{h}ms"]["lift_mean_bps"]>0
                    for h in (100,500)),
                "selection_lower_CI_positive_100_500ms":all(
                    item["selectivity"][f"{h}ms"]["block_bootstrap_30s_ci"]["lower"] is not None and
                    item["selectivity"][f"{h}ms"]["block_bootstrap_30s_ci"]["lower"]>0
                    for h in (100,500)),
                "terminal_flat":abs(tr["end_position_contracts"])<1e-9}
        result["reported_models"][name]=item
    return result


def render(cases):
    lines=["# Step 2: 2026-06-01 2.5ms BTC/SOL bps 双门控单日审计",
           "", "所有模型的实际净收益包括2bps maker费；100/500ms Lift及尾部P10为逐笔事后价格变化。",
           "仅三个离散固定候选：c1=(1sEMA, gap0.5bps, impulse0.2bps, widen0.5bps, hold50ms)，"
           "c2=(1sEMA, 0.5bps,0.5bps,1.0bps,75ms)，"
           "c3=(2sEMA,1.0bps,0.5bps,1.5bps,100ms)。",
           "每个配置均含widen、cancel和OKX本地placebo；不对已探索单日网格调参。",
           ""]
    for coin in ("BTC","SOL"):
        c=cases[coin]
        lines+=["## OKX "+coin,"",
                "|方案|成交|成交保留率|撤单|含费净PnL USDT|100ms Lift|500ms Lift|P10 500ms 改善|Added 500ms 均值|区块CI(500ms)|",
                "|---|---:|---:|---:|---:|---:|---:|---:|---:|---|"]
        for name,x in c["reported_models"].items():
            if name=="glft":
                lines.append(f"|{name}|{x['fills']}|100%|{x['cancels']}|{x['net_pnl_usdt_fee_included']:+.3f}|N/A|N/A|N/A|N/A|N/A|")
                continue
            s=x["selectivity"]
            fmt=lambda z:f"{z:+.4f}" if z is not None else "N/A"
            ci=s["500ms"]["block_bootstrap_30s_ci"]
            ci_label=(f"[{ci['lower']:+.4f},{ci['upper']:+.4f}]"
                      if ci["lower"] is not None else "N/A")
            lines.append(
                f"|{name}|{x['fills']}|{x['fill_retention_ratio']:.1%}|{x['cancels']}|"
                f"{x['net_pnl_usdt_fee_included']:+.3f}|"
                f"{fmt(s['100ms']['lift_mean_bps'])}|{fmt(s['500ms']['lift_mean_bps'])}|"
                f"{fmt(s['500ms']['p10_kept_minus_dropped_bps'])}|"
                f"{fmt(x['Added']['by_horizon']['500ms']['mean_markout_bps'])}|{ci_label}|")
        lines+=[""]
    lines+=["## 规则和限制","",
            "- 撤单不暴增定义为≤原GLFT的1.5倍；成交保留率目标≥80%，最低75%。",
            "- 对每个方案单独打印100ms/500ms均值与P10差和30s时间块Bootstrap分位数，"
            "仅单日内部区间，不能代替独立日显著性。",
            "- Added成交单笔及名义额、库存终值/平均与四期限分解，以及金额严格闭合桥接，见JSON。",
            "- 净收益金额恒等式包含按实际成交额缩放的毛PnL残差，"
            "其中库存与队列效应无法分辨，**不能命名为选择性Alpha**。",
            "- 所有数据已探索，无样本外有效性结论；Equal-Volume G0必须在更早独立日期校准，"
            "不得以2026-06-01的填单数量做回测期回填。",
            "- 风险：真实Post-Only拒单/撤单应答、交互延迟、资金费率与历史合约规格。"]
    return "\n".join(lines)+"\n"


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--source",required=True)
    parser.add_argument("--out",required=True)
    args=parser.parse_args()
    cases={}
    for p in Path(args.source).rglob("summary.json"):
        data=json.loads(p.read_text(encoding="utf8"))
        if "variants" not in data or "coin" not in data:continue
        cases[data["coin"]]=stats_for_case(p.parent)
    if set(cases)!={"BTC","SOL"}:
        raise RuntimeError("Need both BTC and SOL Step2 scenario results")
    root=Path(args.out);root.mkdir(parents=True,exist_ok=True)
    (root/"step2_bps_summary.json").write_text(
        json.dumps(cases,indent=2,ensure_ascii=False),encoding="utf8")
    md=render(cases)
    (root/"step2_bps_summary.md").write_text(md,encoding="utf8")
    print(md)


if __name__=="__main__":
    main()
