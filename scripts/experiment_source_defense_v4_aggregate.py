"""Compact V4 official HBT three-coin ablation and redline report.

Input is V4 official HBT summary plus previously paired G0-only, shared,
and variant-only fill analysis. Does not rerun a trade or tune a threshold.
"""
import argparse
import json
import math
from pathlib import Path

COINS=("BTC","SOL","1000PEPE")
PRESETS=("z1","z2")
ACTIONS=("cancel","widen")
FILTERS=("none","shield","trades","both")


def number(x):
    if x is None:return None
    return float(x) if math.isfinite(x) else None


def entry(row,base):
    tr=row["trading"]
    base_tr=base["trading"]
    x={
        "fills":tr["engine_fills"],
        "fill_retention":tr["engine_fills"]/base_tr["engine_fills"],
        "cancels":tr["cancel_requests"],
        "net_pnl_after_maker2bps_usdt":tr["end_net_marked_equity_usdt"],
        "net_pnl_delta_vs_G0_usdt":tr["end_net_marked_equity_usdt"]-
                                 base_tr["end_net_marked_equity_usdt"],
        "fees_usdt":tr["fee_usdt"],
        "turnover_usdt":tr["trading_value_usdt"],
        "end_inventory_contracts":tr["end_position_contracts"],
        "mean_abs_inventory_contracts":tr["mean_abs_inventory_contracts"],
        "rms_inventory_contracts":tr["rms_inventory_contracts"],
        "causal_pretrade_thresholds":row["sampled_thresholds"],
        "action_counts":row["counters"],
    }
    if "matched_fill_analysis" in row:
        v=row["matched_fill_analysis"]
        x.update({
            "Added":v["Added"],
            "Dropped":v["Dropped"],
            "Kept":v["Kept"],
            "selectivity":v["selectivity"],
            "redlines":row["acceptance"]})
        q100=v["selectivity"]["100ms"]
        q500=v["selectivity"]["500ms"]
        x["redline_all_mechanical"]=bool(
            x["fill_retention"]>=.75 and
            x["cancels"]<=1.5*base_tr["cancel_requests"] and
            q100["lift_mean_bps"] is not None and q100["lift_mean_bps"]>0 and
            q500["lift_mean_bps"] is not None and q500["lift_mean_bps"]>0 and
            q100["p10_kept_minus_dropped_bps"] is not None and
            q100["p10_kept_minus_dropped_bps"]>0 and
            q500["p10_kept_minus_dropped_bps"] is not None and
            q500["p10_kept_minus_dropped_bps"]>0)
        x["redline_statistically_convincing"]=bool(
            x["redline_all_mechanical"] and
            all(v["selectivity"][f"{h}ms"]["within_one_day_block_CI"]["lower"] is not None
                and v["selectivity"][f"{h}ms"]["within_one_day_block_CI"]["lower"]>0
                for h in (100,500)))
    r=tr["trading_value_usdt"]/base_tr["trading_value_usdt"]
    g0_gross=base_tr["end_net_marked_equity_usdt"]+base_tr["fee_usdt"]
    gross=tr["end_net_marked_equity_usdt"]+tr["fee_usdt"]
    fee_saved=base_tr["fee_usdt"]-tr["fee_usdt"]
    scale=g0_gross*(r-1)
    residual=gross-r*g0_gross
    delta=tr["end_net_marked_equity_usdt"]-base_tr["end_net_marked_equity_usdt"]
    if abs(fee_saved+scale+residual-delta)>1e-6:
        raise ValueError("Net profit accounting bridge not closed")
    x["net_bridge_no_causal_alpha_claim"]={
        "fee_saved_usdt":fee_saved,
        "scaled_G0_gross_delta_usdt":scale,
        "residual_inventory_queue_execution_and_selection_usdt":residual}
    return x


def aggregate(source):
    files=list(Path(source).rglob("summary.json"))
    cases={}
    for f in files:
        r=json.loads(f.read_text(encoding="utf8"))
        if "coin" not in r or "scenarios" not in r:continue
        coin=r["coin"]
        if coin in cases:raise ValueError("Duplicate coin "+coin)
        names={"g0"}|{f"{z}_{a}_{ff}" for z in PRESETS
                       for a in ACTIONS for ff in FILTERS}
        if names!=set(r["scenarios"]):
            raise ValueError("Missing required ablation: "+str(names^set(r["scenarios"])))
        base=r["scenarios"]["g0"]
        if not r["all_model_pretrade_GLFT_identical"]:
            raise ValueError("G0 and variants differ in rolling GLFT fit")
        models={key:entry(value,base) for key,value in r["scenarios"].items()}
        effects={}
        for z in PRESETS:
            for action in ACTIONS:
                stem=f"{z}_{action}"
                root=models[f"{stem}_none"]
                for flt in ("shield","trades","both"):
                    v=models[f"{stem}_{flt}"]
                    e={}
                    for k in ("fill_retention","cancels"):
                        e["delta_"+k]=v[k]-root[k]
                    for h in (100,500):
                        n=f"{h}ms"
                        x=v["selectivity"][n]["lift_mean_bps"]
                        y=root["selectivity"][n]["lift_mean_bps"]
                        e[f"delta_selectivity_mean_{n}_bps"]=(
                            x-y if x is not None and y is not None else None)
                    effects[f"{stem}_{flt}_minus_none"]=e
        cases[coin]={
            "day":r["day"],
            "target_spec":{"exchange":"okex-swap","symbol":r["instrument"],
                           "tick":r["tick_size_usdt"],"lot":r["lot"],"ctval":r["ctval"],
                           "historical_spec_verified":r["historical_contract_spec_verified"]},
            "calibration_window_minutes":r["pretrade_minutes"],
            "trade_window_utc":r["test_window_utc"],
            "extra_binance_delay_ms":r["binance_extra_delay_ms"],
            "maker_fee_bps":r["maker_fee_bps"],
            "trade_loader_quality":r["binance_trade_loader_qa"],
            "source_notional_units":"USDT",
            "models":models,"filter_effects_within_same_z_and_action":effects,
            "predeclared_presets":r["fixed_adaptive_presets"]}
    if set(cases)!=set(COINS):
        raise ValueError("Missing actual HBT run for one of BTC SOL 1000PEPE")
    return {"cases":cases,
            "limitations":[
                "Only previously explored June-1, no out-of-sample days",
                "Binance taker confirmation is causal by local_timestamp+2.5ms",
                "SOL 20min/3level cannot be compared directly with archived 10min/5level GLFT",
                "PEPE historical contract metadata not yet officially certified",
                "Repricing queue differences and Added fills confound causal Selection Alpha",
                "Within-day block bootstrap is not across independent days",
                "Equal-volume counterfactual required only if a strictly passing candidate exists"]}


def markdown(d):
    lines=[
        "# V4: 2026-06-01 BTC / SOL / 1000PEPE 自适应 bps＋单侧紧急撤单消融",
        "",
        "G0与所有新模型使用官方hftbacktest、Maker 2bps费用；Binance附加2.5ms可见延迟。",
        "SOL采用当日20分钟历史的研究版3+档拟合，G0及全部策略交易窗共同后移至UTC00:20–01:10；"
        "BTC及PEPE沿用10分钟和原始5+档估计，窗为00:10–01:00。",
        "BBO触发为同向价格位移；trades增强以Tardis主动吃单方确认。",
        "固定2组自适应z倍数 × 两种动作 × 四类门控状态，不在六月反复寻优。",
        "",
    ]
    for coin in COINS:
        case=d["cases"][coin]
        lines+=[f"## OKX {coin}","",
                f"预热{case['calibration_window_minutes']:.0f}min，交易窗{case['trade_window_utc']}；"
                f"Binance真实主动成交记录 {case['trade_loader_quality']['trade_rows']:,} 条。",
                "",
                "|模型|成交/保留率|撤单|含2bps费用净PnL USDT|100ms Lift/P10|500ms Lift/P10|95%单日块CI(500ms)|Added 500ms均值|",
                "|---|---:|---:|---:|---:|---:|---|---:|"]
        for key,v in case["models"].items():
            def fmt(x):
                return f"{x:+.3f}" if x is not None else "N/A"
            if key=="g0":
                lines.append(f"|G0|{v['fills']} / 100%|{v['cancels']}|"
                             f"{v['net_pnl_after_maker2bps_usdt']:+.3f}|—|—|—|—|")
                continue
            t=v["selectivity"]
            a=t["100ms"];b=t["500ms"];ci=b["within_one_day_block_CI"]
            interval=(f"[{ci['lower']:+.3f},{ci['upper']:+.3f}]"
                      if ci["lower"] is not None else "N/A")
            added=v["Added"]["by_horizon"]["500ms"]["mean_markout_bps"]
            lines.append(f"|{key}|{v['fills']} / {v['fill_retention']:.1%}|"
                         f"{v['cancels']}|{v['net_pnl_after_maker2bps_usdt']:+.3f}|"
                         f"{fmt(a['lift_mean_bps'])}/{fmt(a['p10_kept_minus_dropped_bps'])}|"
                         f"{fmt(b['lift_mean_bps'])}/{fmt(b['p10_kept_minus_dropped_bps'])}|"
                         f"{interval}|{fmt(added)}|")
        passes=[name for name,v in case["models"].items()
                if v.get("redline_all_mechanical")]
        strong=[name for name,v in case["models"].items()
                if v.get("redline_statistically_convincing")]
        lines+=["",f"单日机械红线同时通过的预设候选：{', '.join(passes) if passes else '无'}。",
                f"单日内区块CI亦全部>0的候选：{', '.join(strong) if strong else '无'}。",
                "各门控的净贡献（相同z和动作下 vs none）见JSON的filter_effects_within_same_z_and_action。",
                ""]
    lines+=["## 审计边界","",
            "1. 单日成交配对只按交易所成交时间及方向一对一识别，不能替代原订单ID因果反事实。",
            "2. 含手续费净PnL由官方HBT账本取得；净PnL差拆解仅是精确会计桥接，"
            "残差包含库存、执行与队列路径，不是纯Selection Alpha。",
            "3. P10、小样本分桶及30秒区块重抽样仅对这一天具有描述性；"
            "实际跨日期显著性尚未建立。",
            "4. 新策略需要严格正100ms/500ms Lift与P10、保留成交≥75%、撤单量接近G0"
            "才可考虑下一阶段。未经审查不得扩跑6日防御。"]
    return "\n".join(lines)+"\n"


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--source",required=True)
    p.add_argument("--out",required=True)
    args=p.parse_args()
    data=aggregate(args.source)
    path=Path(args.out);path.mkdir(parents=True,exist_ok=True)
    (path/"v4_ablation_summary.json").write_text(
        json.dumps(data,ensure_ascii=False,indent=2,allow_nan=False),encoding="utf8")
    md=markdown(data)
    (path/"v4_ablation_summary.md").write_text(md,encoding="utf8")
    print(md)


if __name__=="__main__":
    main()
