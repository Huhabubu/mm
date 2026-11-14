"""Six independent-date evidence, day-cluster+within-day 30s bootstrap.

Input: 24 official HBT artifacts, including temporary NPZ fill arrays.
Commit ONLY compact aggregated JSON and MD; never commit NPZ tapes/fills.
Fixed 6 monthly first days, four pre-admitted venue/coin markets.
Volume equality is descriptive (May-calibrated random 5 seeds), not causal
selection-alpha identification; inventory/queue remain unobserved confounds.
"""
import argparse
import json
import math
from pathlib import Path

import numpy as np

from experiment_source_defense_step2_aggregate import load_npz
from experiment_source_defense_pilot import classify_opportunities
from experiment_source_defense_step1_diagnostic import fill_group_summary
from experiment_source_defense_v5_multiday import (
    MONTH_DAYS,MARKET_KEYS,PAUSE_SEEDS,
)
HORIZONS=(100,500,1000,5000)
CANDIDATES=("z1_cancel_qs","z1_cancel_qs_tf_p80","okx_local_placebo")
RNG_SEED=20261114
BOOT_REPEATS=2000
BLOCK_NS=30_000_000_000


def finite(a):
    a=np.asarray(a,dtype=float)
    return a[np.isfinite(a)]


def pct(a,q):
    a=finite(a)
    return float(np.quantile(a,q)) if len(a) else None


def mean(a):
    a=finite(a)
    return float(np.mean(a)) if len(a) else None


def sd(a):
    a=finite(a)
    return float(np.std(a,ddof=1)) if len(a)>1 else None


def ledger(model):
    return model["trading"]


def income(model):
    return float(ledger(model)["end_net_marked_equity_usdt"])


def fee(model):
    return float(ledger(model)["fee_usdt"])


def turnover(model):
    return float(ledger(model)["trading_value_usdt"])


def fee_scenarios(xs):
    return {
        "maker_2bps_total_net_usdt":sum(income(v) for v in xs),
        "maker_2bps_mean_daily_net_usdt":mean([income(v) for v in xs]),
        "maker_0bps_total_net_usdt_diagnostic":sum(income(v)+fee(v) for v in xs),
        "maker_minus0p2bps_total_net_usdt":
            sum(income(v)+fee(v)+0.00002*turnover(v) for v in xs),
        "total_turnover_usdt":sum(turnover(v) for v in xs),
        "total_fees_usdt":sum(fee(v) for v in xs),
        "total_fills":sum(ledger(v)["engine_fills"] for v in xs),
        "total_cancel_requests":sum(ledger(v)["cancel_requests"] for v in xs),
        "terminal_inventory_by_day_contracts":[ledger(v)["end_position_contracts"] for v in xs]}


def pnl_bridge(bases,variants):
    g0_gross=sum(income(v)+fee(v) for v in bases)
    v_gross=sum(income(v)+fee(v) for v in variants)
    bturn=sum(turnover(v) for v in bases)
    turn=sum(turnover(v) for v in variants)
    saved=sum(fee(v) for v in bases)-sum(fee(v) for v in variants)
    ratio=turn/bturn if bturn>0 else None
    volume=g0_gross*(ratio-1) if ratio is not None else None
    residual=v_gross-ratio*g0_gross if ratio is not None else None
    diff=sum(income(v) for v in variants)-sum(income(v) for v in bases)
    if ratio is not None and abs(diff-(saved+volume+residual))>1.e-6:
        raise RuntimeError("Official HBT fee/volume/residual accounting identity fails")
    return {
        "net_improvement_total_usdt":diff,
        "fee_saved_usdt":saved,
        "volume_scaled_g0_gross_delta_usdt":volume,
        "residual_gross_usdt":residual,
        "residual_is_not_causal_selection_alpha":True,
        "residual_includes_inventory_execution_queue_and_selection":True}


def block_sums(g0,dm,kept,h):
    good=g0[f"fresh_{h}ms"].astype(bool)
    mark=np.asarray(g0[f"markout_{h}ms_bps"],float)
    t=np.asarray(g0["local_observed_ns"],np.int64)
    # Each day starts a fresh absolute timestamp day block; no cross-date overlap.
    origin=int(t.min()) if len(t) else 0
    blocks=(t-origin)//BLOCK_NS if len(t) else np.empty(0,np.int64)
    out=[]
    for bi in np.unique(blocks):
        sel=(blocks==bi)&good&np.isfinite(mark)
        d=sel&dm
        k=sel&kept
        out.append([float(mark[d].sum()),int(d.sum()),
                    float(mark[k].sum()),int(k.sum())])
    return np.asarray(out,dtype=float).reshape(-1,4)


def day_cluster_block_ci(daily_blocks,seed,horizon):
    rng=np.random.default_rng(seed+horizon)
    nd=len(daily_blocks)
    boot=[]
    # Nested fixed independent daily resampling and 30s block resampling.
    # Also report n=6 day uncertainty rather than treating all fills IID.
    for _ in range(BOOT_REPEATS):
        totals=np.zeros(4,dtype=float)
        for k in rng.integers(0,nd,size=nd):
            b=daily_blocks[k]
            if len(b)==0:continue
            ids=rng.integers(0,len(b),size=len(b))
            totals+=b[ids].sum(axis=0)
        if totals[1]>0 and totals[3]>0:
            boot.append(totals[2]/totals[3]-totals[0]/totals[1])
    return {
        "lower":pct(boot,.025),
        "upper":pct(boot,.975),
        "bootstrap_success":len(boot),
        "bootstrap_replicates_target":BOOT_REPEATS,
        "resampling":"sample 6 UTC days with replacement, then 30-second within-day markout blocks",
        "small_day_count_warning":"Only six independent dates; paired block bootstrap still low-power"}


def collect_selectivity(cases,model_name,ctval):
    merged={}
    for h in HORIZONS:
        drops=[];keeps=[];adds=[];blocks=[]
        for report,path in cases:
            g0=load_npz(path/"g0_fills.npz")
            new=load_npz(path/(model_name+"_fills.npz"))
            dm,km=classify_opportunities(g0,new)
            am,_=classify_opportunities(new,g0)
            def usable(v,mask):
                ok=np.asarray(v[f"fresh_{h}ms"],bool)&mask
                data=np.asarray(v[f"markout_{h}ms_bps"],float)
                return data[ok&np.isfinite(data)]
            drops.append(usable(g0,dm))
            keeps.append(usable(g0,km))
            adds.append(usable(new,am))
            blocks.append(block_sums(g0,dm,km,h))
        drop=np.concatenate(drops)
        kept=np.concatenate(keeps)
        added=np.concatenate(adds)
        lift=(float(np.mean(kept)-np.mean(drop))
              if len(kept) and len(drop) else None)
        merged[f"{h}ms"]={
            "Dropped_valid_markout_count":len(drop),
            "Kept_valid_markout_count":len(kept),
            "Added_valid_markout_count":len(added),
            "dropped_mean_markout_bps":mean(drop),
            "kept_mean_markout_bps":mean(kept),
            "added_mean_markout_bps":mean(added),
            "dropped_p10_bps":pct(drop,.1),
            "kept_p10_bps":pct(kept,.1),
            "added_p10_bps":pct(added,.1),
            "selectivity_lift_kept_minus_dropped_bps":lift,
            "p10_kept_minus_dropped_bps":(
                pct(kept,.1)-pct(drop,.1) if len(kept) and len(drop) else None),
            "two_stage_bootstrap_95pct":day_cluster_block_ci(
                blocks,RNG_SEED+len(model_name)*7919,h)}
    return merged


def overall_markout(cases,model_name):
    out={}
    for h in HORIZONS:
        ms=[];captures=[];grosses=[]
        for r,path in cases:
            d=load_npz(path/(model_name+"_fills.npz"))
            if len(d["side_sign"])==0:continue
            valid=np.asarray(d[f"fresh_{h}ms"],bool)
            ms.extend(d[f"markout_{h}ms_bps"][valid].tolist())
            captures.extend(d["spread_capture_bps"][valid].tolist())
            grosses.extend(d[f"gross_edge_{h}ms_bps"][valid].tolist())
        out[f"{h}ms"]={
            "official_fill_gross_edge_mean_bps":mean(grosses),
            "spread_capture_mean_bps":mean(captures),
            "adverse_selection_cost_mean_bps":(
                -mean(ms) if mean(ms) is not None else None),
            "markout_p10_bps":pct(ms,.1),
            "n_fresh":len(ms)}
    return out


def aggregate(root):
    root=Path(root)
    found={}
    for p in root.rglob("case.json"):
        item=json.loads(p.read_text(encoding="utf8"))
        if item.get("purpose")!="day":continue
        key=(item["coin"],item["venue"],item["day"])
        if key in found:raise ValueError("Duplicated backtest market/day "+str(key))
        if not item["all_models_glft_fit_identical"]:
            raise RuntimeError("Mismatched GLFT calibration within a case")
        found[key]=(item,p.parent)
    outcome={
        "market_summaries":{},
        "calendar_days":MONTH_DAYS,
        "bootstrap_repeats":BOOT_REPEATS,
        "bootstrap_seed":RNG_SEED,
        "scope":"2026-06-01 through 2026-11-01 monthly day-1 50min independent accounting",
        "limitations":[
            "Mixed out-of-sample: all month-first dates were used in earlier parameter-admission screening",
            "Six days are independent, but same market dates can be subject to shared crypto shocks",
            "Within-day 30s blocks plus day cluster CI cannot prove causality",
            "Timestamp+side matched fills do not identify exact G0 order and cancel ACK",
            "Random pause probabilities and durations fixed from May, never adjusted with 6 test dates",
            "Random-Pause equal-volume is only a comparison of same aggregate fill/fee burden, not pure Selection Alpha",
            "Historical instrument specifications not independently certified",
            "Post-only GTX order rejection, latency and taker fees all follow official HBT assumptions",
            "All market/strategy profits include maker 2bps primary accounting"]}
    for coin,venue in MARKET_KEYS:
        mk=coin+"_"+venue
        cases=[found[(coin,venue,d)] for d in MONTH_DAYS
               if (coin,venue,d) in found]
        missing=[d for d in MONTH_DAYS if (coin,venue,d) not in found]
        if missing:
            outcome["market_summaries"][mk]={
                "status":"incomplete","received_date_count":len(cases),
                "missing_dates":missing,
                "conclusion":"No six-day pooled result or confidence interval may be reported"}
            continue
        models=[d["variants"] for d,p in cases]
        keys=set(models[0])
        if any(set(d)!=keys for d in models):
            outcome["market_summaries"][mk]={
                "status":"incomplete","reason":"Model set missing across dates",
                "model_sets_by_day":[list(m) for m in models]}
            continue
        g0=[d["g0"] for d in models]
        base=fee_scenarios(g0)
        count0=base["total_fills"]
        ctval=cases[0][0]["contract_value_in_target_base_coin"]
        result={
            "status":"six_days_complete","days":MONTH_DAYS,
            "pretrade_minutes":cases[0][0]["pretrade_minutes"],
            "trade_window":cases[0][0]["trade_window_utc"],
            "g0":base,"models":{},
            "May_preset":cases[0][0].get("may_random_pause_preset"),
            "total_original_g0_fills":count0,
            "daily_fills_g0":[ledger(v)["engine_fills"] for v in g0]}
        for key in keys:
            if key=="g0":continue
            xs=[m[key] for m in models]
            ledger_stats=fee_scenarios(xs)
            ledger_stats["pooled_fill_retention"]=ledger_stats["total_fills"]/count0
            ledger_stats["pooled_cancel_ratio"]=(
                ledger_stats["total_cancel_requests"]/base["total_cancel_requests"])
            ledger_stats["net_bridge"]=pnl_bridge(g0,xs)
            ledger_stats["daily_fills"]=[ledger(x)["engine_fills"] for x in xs]
            ledger_stats["daily_fee_included_net_usdt"]=[income(x) for x in xs]
            ledger_stats["daily_cancels"]=[ledger(x)["cancel_requests"] for x in xs]
            ledger_stats["mean_daily_fills"]=mean(ledger_stats["daily_fills"])
            ledger_stats["selectivity"]=collect_selectivity(cases,key,ctval)
            ledger_stats["economic_fill_markouts"]=overall_markout(cases,key)
            ledger_stats["daily_active_duty_fraction"]=[
                x["defense_duty"]["active_fraction_50min"] for x in xs]
            ledger_stats["binance_trade_P80_thresholds_by_day"]=[
                x["thresholds"]["binance_buy_net_P80_usdt"] for x in xs]
            result["models"][key]=ledger_stats
        if all("pause_seed_"+str(s) in keys for s in PAUSE_SEEDS):
            randoms=[result["models"]["pause_seed_"+str(s)] for s in PAUSE_SEEDS]
            source=result["models"]["z1_cancel_qs"]
            mismatch=[]
            for a in randoms:
                mismatch.append({
                    "seed_fills":a["total_fills"],
                    "seed_net_after_fee":a["maker_2bps_total_net_usdt"],
                    "fills_error":abs(a["total_fills"]/source["total_fills"]-1),
                    "fee_error":abs(a["total_fees_usdt"]/source["total_fees_usdt"]-1)})
            e_fill=np.array([a["fills_error"] for a in mismatch])
            e_fee=np.array([a["fee_error"] for a in mismatch])
            mean_fills=mean([a["total_fills"] for a in randoms])
            mean_fees=mean([a["total_fees_usdt"] for a in randoms])
            overall_fill_error=abs(mean_fills/source["total_fills"]-1)
            overall_fee_error=abs(mean_fees/source["total_fees_usdt"]-1)
            result["equal_volume_random_5seed"]={
                "source_model":"z1_cancel_qs",
                "seed_results":mismatch,
                "mean_seed_fills":mean_fills,
                "sd_seed_fills":sd([a["total_fills"] for a in randoms]),
                "mean_seed_total_fees_usdt":mean_fees,
                "sd_seed_total_fees_usdt":sd([a["total_fees_usdt"] for a in randoms]),
                "overall_seed_mean_fill_relative_error":overall_fill_error,
                "overall_seed_mean_fee_relative_error":overall_fee_error,
                "all_seeds_match_both_3pct":all(
                    a["fills_error"]<=.03 and a["fee_error"]<=.03
                    for a in mismatch),
                "mean_matches_both_3pct":overall_fill_error<=.03 and overall_fee_error<=.03,
                "mean_matches_both_2pct":overall_fill_error<=.02 and overall_fee_error<=.02,
                "random_net_after_fee_mean_usdt":mean([
                    a["maker_2bps_total_net_usdt"] for a in randoms]),
                "random_net_after_fee_sd_across_seeds_usdt":sd([
                    a["maker_2bps_total_net_usdt"] for a in randoms]),
                "not_identifiable_as_pure_causal_alpha":True,
                "no_June_plus_parameter_calibration":True}
        else:
            result["equal_volume_random_5seed"]={
                "status":"missing_May_calibration_or_pilot",
                "strict_equal_volume_pass":False}
        outcome["market_summaries"][mk]=result
    return outcome


def markdown(data):
    lines=[
        "# 六日期来源防御：G0 / Z1 Queue Shield / Binance P80 / 本地安慰剂 / 5种随机停报",
        "",
        "固定每月1日（6月至11月），每个独立日交易50分钟；净收益全部包括Maker2bps手续费。",
        "随机停报时长、频率与危险侧占比只来自5月预训练；6月以后不得重调。",
        "原G0与Random-Pause不消费Binance价格、方向或消息时间；额外报告历史Binance时钟G0。",
        "统计为每日6个独立簇、日内30秒时间块的两阶段重抽样，2000次固定随机种子。",
        "净收益改善拆成Fee Saved、Volume-Scale Gross及Gross Residual，会计恒等成立；"
        "Residual仍混合仓位路径、执行与队列，不可称纯Selection Alpha。",
        ""]
    for name,r in data["market_summaries"].items():
        lines.extend([f"## {name}",""])
        if r["status"]!="six_days_complete":
            lines.extend([f"审查失败：{r['status']}，{r.get('missing_dates',r.get('reason'))}。",
                          "该市场不报告6日选择性结论。",""])
            continue
        b=r["g0"]
        lines.append(f"预热{r['pretrade_minutes']:.0f}分钟，交易时间{r['trade_window']}；"
                     f"G0合计成交{b['total_fills']}笔、撤单{b['total_cancel_requests']}次、"
                     f"含费净收益{b['maker_2bps_total_net_usdt']:.4f}USDT。")
        lines.extend(["","|模型|成交/相对G0|撤单|含费净收益USDT|100ms Lift及95%CI|500ms Lift及95%CI|500ms P10改善|",
                      "|---|---:|---:|---:|---|---|---:|"])
        for k in ("g0_source_clock",)+CANDIDATES+tuple("pause_seed_"+str(v) for v in PAUSE_SEEDS):
            if k not in r["models"]:continue
            v=r["models"][k]
            def lift(h):
                p=v["selectivity"][h]
                val=p["selectivity_lift_kept_minus_dropped_bps"]
                ci=p["two_stage_bootstrap_95pct"]
                if val is None:return "N/A"
                low=ci["lower"];up=ci["upper"]
                return (f"{val:+.3f} [{low:+.3f},{up:+.3f}]"
                        if low is not None and up is not None else f"{val:+.3f} [NA]")
            p10=v["selectivity"]["500ms"]["p10_kept_minus_dropped_bps"]
            lines.append(f"|{k}|{v['total_fills']} / {v['pooled_fill_retention']:.1%}|"
                        f"{v['total_cancel_requests']}|"
                        f"{v['maker_2bps_total_net_usdt']:+.3f}|"
                        f"{lift('100ms')}|{lift('500ms')}|"
                        f"{p10:+.3f}|" if p10 is not None else
                         f"|{k}|{v['total_fills']} / {v['pooled_fill_retention']:.1%}|"
                         f"{v['total_cancel_requests']}|"
                         f"{v['maker_2bps_total_net_usdt']:+.3f}|"
                         f"{lift('100ms')}|{lift('500ms')}|N/A|")
        pa=r["equal_volume_random_5seed"]
        lines.extend(["",f"随机暂停五种子等量审查：{pa.get('mean_matches_both_3pct',False)}；"
                      f"均值成交误差{pa.get('overall_seed_mean_fill_relative_error',float('nan')):.2%}，"
                      f"均值手续费误差{pa.get('overall_seed_mean_fee_relative_error',float('nan')):.2%}。",
                      "每笔Gross Edge、AS Cost、四期限完整分组及fee0/-0.2bps敏感性见JSON。",
                      ""])
    lines.extend(["## 审计限制","",
                 "六日期样本有限；不能仅因合并后Dropped>200就假定日期独立且结论显著。",
                 "HBT执行、价格跳变与资金费率、历史期货合约规则及队列成交假设仍需核查。",
                 "Random-Pause等量成功也只能控制总成交与手续费，仓位/库存、订单队列和价格路径依旧可能不同。",
                 "所有历史天已用于先前模型诊断；未建立真正样本外跨所因果效果。"])
    return "\n".join(lines)+"\n"


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--source",required=True)
    p.add_argument("--out",required=True)
    args=p.parse_args()
    result=aggregate(args.source)
    out=Path(args.out);out.mkdir(parents=True,exist_ok=True)
    (out/"v5_sixdate_summary.json").write_text(
        json.dumps(result,ensure_ascii=False,indent=2,allow_nan=False),encoding="utf8")
    m=markdown(result)
    (out/"v5_sixdate_summary.md").write_text(m,encoding="utf8")
    print(m)


if __name__=="__main__":
    main()
