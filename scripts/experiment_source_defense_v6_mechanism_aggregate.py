"""V6 mechanism rollup: exact pooled G0 Dropped A/B, kept, new,
and source vs +60s time-shift official HBT fills.

Read CI-only per-fill NPZ from artifact; output only compact JSON/MD to repo.
No per-fill raw records in Git. Six independent UTC dates, no full V5 matrix.
"""
import argparse
import json
from pathlib import Path

import numpy as np
from experiment_source_defense_v5_multiday import MONTH_DAYS
from experiment_source_defense_pilot import classify_opportunities
from experiment_source_defense_v6_mechanism import (
    HORIZONS,asof_direct,asof_urgent_tail,summarize_mask)
BOOT_SEED=20261114


def read_mode(data,name):
    out={}
    keys=("exchange_timestamp_ns","local_observed_ns","side_sign",
          "spread_capture_bps")
    for k in keys:
        out[k]=data[name+"__"+k]
    for h in HORIZONS:
        for k in (f"markout_{h}ms_bps",f"fresh_{h}ms",f"gross_edge_{h}ms_bps"):
            out[k]=data[name+"__"+k]
    return out


def masks_from_duty(data,base,variant,name):
    dropped,kept=classify_opportunities(base,variant)
    added,_=classify_opportunities(variant,base)
    st=data[name+"__episode_start"]
    en=data[name+"__episode_end"]
    sd=data[name+"__episode_side"]
    at=data[name+"__urgent_time"]
    si=data[name+"__urgent_side"]
    a=asof_direct(base["exchange_timestamp_ns"],base["side_sign"],st,en,sd)
    tail=asof_urgent_tail(base["exchange_timestamp_ns"],base["side_sign"],at,si)
    direct=dropped&(a|tail)
    return {
        "Group_A_direct_window_or_200ms_cancel_tail":(base,direct),
        "Group_A_strict_active_only":(base,dropped&a),
        "Group_A_cancel_tail_only":(base,dropped&~a&tail),
        "Group_B_off_window":(base,dropped&~direct),
        "Kept":(base,kept),
        "Added":(variant,added),
        "Dropped_all":(base,dropped)}


def pooled_group(grouped):
    ans={"fills":sum(int(mask.sum()) for d,mask in grouped),
         "fill_pct_among_baseline_all":None}
    for h in HORIZONS:
        all_marks=[];all_gross=[]
        for d,mask in grouped:
            valid=mask & np.asarray(d[f"fresh_{h}ms"],bool)
            mark=np.asarray(d[f"markout_{h}ms_bps"],float)
            gross=np.asarray(d[f"gross_edge_{h}ms_bps"],float)
            flag=valid&np.isfinite(mark)
            all_marks.extend(mark[flag].tolist())
            all_gross.extend(gross[flag].tolist())
        m=np.asarray(all_marks,float)
        edge=np.asarray(all_gross,float)
        ans[f"{h}ms"]={
            "valid_markout_n":len(m),
            "mean_markout_bps":float(np.mean(m)) if len(m) else None,
            "p10_markout_bps":float(np.quantile(m,.1)) if len(m) else None,
            "mean_adverse_selection_cost_bps":float(-np.mean(m)) if len(m) else None,
            "mean_gross_edge_bps":float(np.mean(edge)) if len(edge) else None}
    return ans


def bootstrap_paired_day_avg(delta_by_day,seed=BOOT_SEED,repeats=5000):
    """Six-day paired clusters. Daily means equal-weight, no IID fill claim."""
    a=np.asarray(delta_by_day,float)
    a=a[np.isfinite(a)]
    if len(a)<3:
        return {"valid_days":len(a),"lower":None,"upper":None}
    rng=np.random.default_rng(seed)
    n=len(a)
    means=np.mean(a[rng.integers(0,n,size=(repeats,n))],axis=1)
    return {"valid_days":n,"lower":float(np.quantile(means,.025)),
            "upper":float(np.quantile(means,.975)),
            "mean_equal_day_delta":float(np.mean(a)),
            "resampling":"six-day paired day-cluster only, 5000 draws"}


def aggregate(src):
    files=list(Path(src).rglob("case.json"))
    cases={}
    for f in files:
        x=json.loads(f.read_text(encoding="utf8"))
        key=(x["day"],x["coin"])
        if key in cases:raise ValueError(f"Duplicate {key}")
        files_npz=f.parent/"fill_groups_CI_only.npz"
        if not files_npz.exists():raise FileNotFoundError(str(files_npz))
        cases[key]=(x,files_npz)
    required={("2026-06-01",c) for c in ("BTC","SOL","DOGE","1000PEPE")}
    required.update((day,"SOL") for day in MONTH_DAYS)
    missing=required-set(cases)
    if missing:raise ValueError("Incomplete minimum V6 audit: "+str(missing))
    sols=[cases[(d,"SOL")] for d in MONTH_DAYS]
    groups_by_name={"source":{}, "shift60":{}}
    model_trades={"source":[],"shift60":[],"g0":[]}
    durations=[]
    day_mark_diff={h:[] for h in HORIZONS}
    for day,(report,npz_file) in zip(MONTH_DAYS,sols):
        with np.load(npz_file) as data:
            base=read_mode(data,"g0")
            model_trades["g0"].append(report["g0"])
            ex=report["time_shift_60s"]["window_exposure_equality"]
            durations.append(ex)
            for mode in ("source","shift60"):
                d=read_mode(data,mode)
                m=masks_from_duty(data,base,d,mode)
                for name,v in m.items():
                    groups_by_name[mode].setdefault(name,[]).append(v)
                z=(report["source"] if mode=="source" else
                   report["time_shift_60s"]["strategy"])
                model_trades[mode].append(z)
            src=read_mode(data,"source")
            dst=read_mode(data,"shift60")
            for h in HORIZONS:
                def avg(v):
                    mask=np.asarray(v[f"fresh_{h}ms"],bool)
                    x=np.asarray(v[f"markout_{h}ms_bps"],float)[mask]
                    return float(x.mean()) if len(x) else float("nan")
                day_mark_diff[h].append(avg(src)-avg(dst))
    pools={}
    for mode,gs in groups_by_name.items():
        t={k:pooled_group(v) for k,v in gs.items()}
        orig=sum(int(x["fills"]) for x in model_trades["g0"])
        drops=t["Dropped_all"]["fills"]
        for k in t:
            t[k]["fill_pct_among_baseline_all"]=t[k]["fills"]/orig
            if k.startswith("Group_A") or k.startswith("Group_B"):
                t[k]["pct_among_all_Dropped"]=t[k]["fills"]/max(1,drops)
        pools[mode]=t
    totals={}
    for name,models in model_trades.items():
        totals[name]={
            "six_day_fills":sum(x["fills"] for x in models),
            "total_cancel_requests":sum(x["cancels"] for x in models),
            "total_fee_included_net_usdt":sum(x["fee_included_net_usdt"] for x in models),
            "mean_daily_fee_included_net_usdt":float(np.mean(
                [x["fee_included_net_usdt"] for x in models])),
            "total_fee_usdt":sum(x["fee_usdt"] for x in models),
            "mean_gross_edge_per_fill_bps":{
                str(h)+"ms":None for h in HORIZONS},
            "daily_fills":[x["fills"] for x in models],
            "daily_marked_inventory":[x["terminal_inventory"] for x in models]}
    outcome={
        "SOL_okex_swap":{
            "days":MONTH_DAYS,
            "time_shift_exposure_per_day":durations,
            "whole_50min_identical_episodes_and_durations_across_6_days":
                all(x["exact_50min_full_exposure_matched"] for x in durations),
            "source_vs_shift60_total_active_duration_ratio":
                sum(x["shifted_active_ns"] for x in durations)/max(
                    1,sum(x["original_active_ns"] for x in durations)),
            "strategy_economics":totals,
            "fill_groups":pools,
            "source_minus_shift60_daily_equal_weight_markout_mean_bps":{
                f"{h}ms":bootstrap_paired_day_avg(day_mark_diff[h],BOOT_SEED+h)
                for h in HORIZONS},
            "G0_vs_source_and_shift_group_caveat":"only exchange time+side pairing, distinct order/queue paths and unverified effective exchange cancel ACK",
            "equal_episode_count_caveat":"last 60s original defense cannot shift into original 50min trade window without future leakage; clipped intervals disclosed",
            "five_day_no_reoptimizing":True
        },
        "June_diagnostics":{
            coin:cases[("2026-06-01",coin)][0]
            for coin in ("BTC","DOGE","1000PEPE")},
        "limitations":[
            "Time membership is timestamp alignment, not proven canceled G0 order fate",
            "Local defensive active intervals and exchange fills use different clocks",
            "Cancel request is not exchange ACK",
            "Time-shift trading is past-source-only, but schedule generated from first pass of same-day event path",
            "Clipped last-minute intervals destroy exact full 50min active duty equality",
            "Results not independently out-of-sample",
            "Net PnL under official hftbacktest 2bps maker fees; no positive profit claim"]}
    return outcome


def markdown(result):
    s=result["SOL_okex_swap"]
    t=s["strategy_economics"]
    lines=["# V6 六日期SOL +60秒Time-Shift：严格防御窗A/B归因与官方HBT反事实",
           "",
           "仅本轮6个SOL交易日×G0/Z1+QS/+60s Shift，不重复四市场V5矩阵。",
           "所有收益已扣Maker2bps模拟费用。匹配按交易所成交时间＋方向一对一。",
           "防御窗来自真实研究引擎本地时间，包含按相同危险侧的紧急撤单请求后200ms。",
           "",
           "|模型|总成交|总撤单|六日净PnL USDT|平均每日净PnL USDT|",
           "|---|---:|---:|---:|---:|"]
    for name in ("g0","source","shift60"):
        z=t[name]
        lines.append(f"|{name}|{z['six_day_fills']}|{z['total_cancel_requests']}|"
                     f"{z['total_fee_included_net_usdt']:+.4f}|"
                     f"{z['mean_daily_fee_included_net_usdt']:+.4f}|")
    lines+=["","|比较模型|G0成交分组|成交笔数|占原G0成交|占Dropped|100ms均值/P10 bps|500ms均值/P10 bps|1s均值/P10 bps|",
            "|---|---|---:|---:|---:|---|---|---|"]
    def fmt(g,h):
        v=g[f"{h}ms"]
        a=v["mean_markout_bps"];b=v["p10_markout_bps"]
        return f"{a:+.3f}/{b:+.3f}" if a is not None and b is not None else "N/A"
    for mode in ("source","shift60"):
        for name,v in s["fill_groups"][mode].items():
            lines.append(f"|{mode}|{name}|{v['fills']}|"
                         f"{v['fill_pct_among_baseline_all']:.1%}|"
                         f"{v.get('pct_among_all_Dropped',0):.1%}|"
                         f"{fmt(v,100)}|{fmt(v,500)}|{fmt(v,1000)}|")
    lines+=["","## +60s时间平移的防御曝光截断审计","",
            f"原完整50min防御时长中被平移保留下来的比例："
            f"{s['source_vs_shift60_total_active_duration_ratio']:.3%}；"
            f"六日均完整等曝光：{s['whole_50min_identical_episodes_and_durations_across_6_days']}。",
            "为了避免把测试最后一分钟的未来防御窗口提前放入开头，这里不做模50min循环平移。",
            "如果暴露未全部保留，本安慰剂只能作为负对照而非严格等时长对照。",
            "",
            "## Source减Shift自身实际成交的按日等权Markout差（非同一成交配对）","",
            "|期限|六日期均差bps|按交易日重抽样95%区间|",
            "|---|---:|---|"]
    for h in HORIZONS:
        row=s["source_minus_shift60_daily_equal_weight_markout_mean_bps"][f"{h}ms"]
        if row["lower"] is None:val="N/A";ci="N/A"
        else:
            val=f"{row['mean_equal_day_delta']:+.4f}"
            ci=f"[{row['lower']:+.4f},{row['upper']:+.4f}]"
        lines.append(f"|{h}ms|{val}|{ci}|")
    lines+=["","## 限制与后续","",
            "Group A是时间窗/危险侧对应，若挂单早已成交或撤单请求仍在途中，"
            "不能仅凭时间成员资格断定该成交本可直接被取消。"
            "Group B只是窗外Dropped，路径/队列/库存原因需订单ID及ACK级别的反事实审计。",
            "两种策略分别形成不同的订单与库存路径；净PnL差和成交Gross Edge差也不是已识别的纯Selection Alpha。",
            "下一轮需做独立样本外以及官方HBT订单ACK原子级观测后才能做因果归因。"]
    return "\n".join(lines)+"\n"


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--source",required=True)
    p.add_argument("--out",required=True)
    a=p.parse_args()
    result=aggregate(a.source)
    out=Path(a.out);out.mkdir(parents=True,exist_ok=True)
    (out/"mechanism_shift60_summary.json").write_text(
        json.dumps(result,indent=2,ensure_ascii=False,allow_nan=False),encoding="utf8")
    md=markdown(result)
    (out/"mechanism_shift60_summary.md").write_text(md,encoding="utf8")
    print(md)


if __name__=="__main__":
    main()
