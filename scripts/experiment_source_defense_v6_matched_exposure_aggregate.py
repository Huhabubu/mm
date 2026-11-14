"""Six-day exact *retained* scheduled pulse +60s placebo aggregate, no NPZ commit."""
import argparse
import json
from pathlib import Path

import numpy as np
from experiment_source_defense_v6_matched_exposure import DAYS
from experiment_source_defense_v6_mechanism_aggregate import bootstrap_paired_day_avg

H=(100,500,1000)


def pooled(mode,daily_npz):
    vals={}
    for horizon in H:
        mark=[];gross=[];means=[]
        for _,p in daily_npz:
            with np.load(p) as f:
                v=np.asarray(f[f"{mode}__markout_{horizon}ms_bps"],float)
                g=np.asarray(f[f"{mode}__gross_edge_{horizon}ms_bps"],float)
                flag=np.asarray(f[f"{mode}__fresh_{horizon}ms"],bool)
                v=v[flag];g=g[flag]
                mark.extend(v.tolist())
                gross.extend(g.tolist())
                means.append(float(np.mean(v)) if len(v) else np.nan)
        arr=np.asarray(mark,float);edge=np.asarray(gross,float)
        vals[f"{horizon}ms"]={
            "count_fresh":len(arr),
            "pooled_mean_markout_bps":float(arr.mean()) if len(arr) else None,
            "pooled_p10_markout_bps":float(np.quantile(arr,.1)) if len(arr) else None,
            "pooled_mean_AS_cost_bps":float(-arr.mean()) if len(arr) else None,
            "pooled_mean_gross_edge_bps":float(edge.mean()) if len(edge) else None,
            "daily_mean_markouts":means}
    return vals


def aggregate(root):
    cases={}
    for p in Path(root).rglob("case.json"):
        r=json.loads(p.read_text(encoding="utf8"))
        if r["day"] in cases:raise ValueError("Duplicate matched day")
        file=p.parent/"fills_CI_only.npz"
        if not file.exists():raise ValueError("Matched HBT fills missing")
        cases[r["day"]]=(r,file)
    if set(cases)!=set(DAYS):raise ValueError("Need six fixed SOL days")
    rows=[cases[d] for d in DAYS]
    act=[r["actual_HBT_episode_count_and_duration_equal"] for r,f in rows]
    a=[r["source_schedule_no_current_Binance_input"] for r,f in rows]
    b=[r["shift_60s_schedule_no_current_Binance_input"] for r,f in rows]
    s=pooled("source_time",rows)
    t=pooled("shift60_time",rows)
    delta={}
    for h in H:
        k=f"{h}ms"
        x=np.asarray(s[k]["daily_mean_markouts"])
        y=np.asarray(t[k]["daily_mean_markouts"])
        delta[k]=bootstrap_paired_day_avg(x-y,seed=20261114+h)
        delta[k]["pooled_mean_difference_bps"]=(
            s[k]["pooled_mean_markout_bps"]-t[k]["pooled_mean_markout_bps"])
        delta[k]["pooled_p10_difference_bps"]=(
            s[k]["pooled_p10_markout_bps"]-t[k]["pooled_p10_markout_bps"])
    result={
        "scope":"SOL OKX 6 days, research 20min warmup UTC 00:20–01:10",
        "days":DAYS,"test_assumptions":"official HBT 2.4.4, 1ms send +1ms ACK and Maker 2bps",
        "actual_held_exposure_all_six_equal":all(act),
        "actual_equal_by_day":act,
        "scheduled_pulse_count_and_total_duration_all_six_equal":all(
            r["schedule_control"]["same_total_scheduled_duration"] and
            r["schedule_control"]["same_episode_count"] for r,f in rows),
        "retained_fraction_each_day":[r["schedule_control"]["retained_fraction_of_original"]
                                       for r,f in rows],
        "daily_source_fills":[v["fills"] for v in a],
        "daily_shift_fills":[v["fills"] for v in b],
        "daily_source_net_after_2bps":[v["fee_included_net_usdt"] for v in a],
        "daily_shift_net_after_2bps":[v["fee_included_net_usdt"] for v in b],
        "source_time":{
            "total_fills":sum(v["fills"] for v in a),
            "total_cancel_requests":sum(v["cancels"] for v in a),
            "total_net_after_2bps_USDT":sum(v["fee_included_net_usdt"] for v in a),
            "mean_daily_net_after_2bps_USDT":float(np.mean(
                [v["fee_included_net_usdt"] for v in a])),
            "pooled_fill_marks":s},
        "shift_60s":{
            "total_fills":sum(v["fills"] for v in b),
            "total_cancel_requests":sum(v["cancels"] for v in b),
            "total_net_after_2bps_USDT":sum(v["fee_included_net_usdt"] for v in b),
            "mean_daily_net_after_2bps_USDT":float(np.mean(
                [v["fee_included_net_usdt"] for v in b])),
            "pooled_fill_marks":t},
        "paired_daily_clocks_adjusted_markout_difference":delta,
        "methodology":"Both arms are source-blind HBT replays of exact same retained May-unseen source episode schedule, shifted +60s for placebo. Trailing source episode(s) clipped symmetrically to avoid future wrapping.",
        "disclaimer":"Replayed current-day original source episode schedule is used as precomputed calendar, so it is a retrospective timing placebo; it does not constitute an independently runnable source-blind production signal.",
        "causal_selection_alpha_proven":False,
        "fee_or_fill_matching_requirement":"Same scheduled active durations do not imply same fills or fees. Report mismatch explicitly."
    }
    return result


def markdown(x):
    a=x["source_time"];b=x["shift_60s"]
    lines=[
        "# SOL六日期：严格相等的保留防御脉冲时间轴与 +60秒安慰剂",
        "",
        "先将原真实Z1+QS防御时轴最后60秒的不可后移部分对称裁剪；"
        "然后把同一脉冲簇、方向、次数与保留时长分别放在原时刻和+60秒。",
        "两个新研究回放都不读Binance当前价格、方向或行情消息时钟，"
        "仅利用预计算的时间表驱动官方HBT取消/恢复。所有经济结果扣Maker2bps。",
        "",
        f"六日实际防御次数+时长完全一致：{x['actual_held_exposure_all_six_equal']}。",
        f"六日日历预定次数+时长完全一致：{x['scheduled_pulse_count_and_total_duration_all_six_equal']}。",
        "",
        "|项目|原时刻Source簇|+60秒平移簇|",
        "|---|---:|---:|",
        f"|6日总成交|{a['total_fills']}|{b['total_fills']}|",
        f"|6日撤单请求|{a['total_cancel_requests']}|{b['total_cancel_requests']}|",
        f"|6日含2bps费净PnL USDT|{a['total_net_after_2bps_USDT']:+.4f}|{b['total_net_after_2bps_USDT']:+.4f}|",
        f"|每日平均含费净PnL USDT|{a['mean_daily_net_after_2bps_USDT']:+.4f}|{b['mean_daily_net_after_2bps_USDT']:+.4f}|",
        "",
        "|期限|Source平均Markout bps|Shift平均Markout bps|Source−Shift按日等权差bps|95%按日Bootstrap区间|Source−Shift P10 bps|",
        "|---|---:|---:|---:|---|---:|"]
    for h in H:
        k=f"{h}ms"
        sm=a["pooled_fill_marks"][k]
        pm=b["pooled_fill_marks"][k]
        d=x["paired_daily_clocks_adjusted_markout_difference"][k]
        if d["lower"] is not None: ci=f"[{d['lower']:+.4f},{d['upper']:+.4f}]"
        else:ci="N/A"
        lines.append(f"|{k}|{sm['pooled_mean_markout_bps']:+.4f}|"
                     f"{pm['pooled_mean_markout_bps']:+.4f}|"
                     f"{d['mean_equal_day_delta']:+.4f}|"
                     f"{ci}|{d['pooled_p10_difference_bps']:+.4f}|")
    lines+=["","## 必须保留的局限","",
            "1. 本对照等同的是危险侧防御时间簇，不保证相同成交笔数、手续费、未平仓库存或队列位置。",
            "2. 删除最后60秒不能平移的防御事件，两个实验组都删除相同片段，"
            "因此相对于原完整Z1+QS策略是受限子样本；报告另保留每一天丢失比例。",
            "3. 原时刻的防御时间表来自先前使用当前日Binance及目标L2观测的模型，"
            "本实验是回顾性时间安慰剂，不能独立部署或构成严格因果随机试验。",
            "4. 六个日期曾用于参数诊断，没有新样本外；若Markout差为正仍须结合历史行情时钟偏差审查。"]
    return "\n".join(lines)+"\n"


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--source",required=True)
    p.add_argument("--out",required=True)
    a=p.parse_args()
    result=aggregate(a.source)
    out=Path(a.out);out.mkdir(parents=True,exist_ok=True)
    (out/"matched_exposure_shift60.json").write_text(
        json.dumps(result,indent=2,ensure_ascii=False,allow_nan=False))
    md=markdown(result)
    (out/"matched_exposure_shift60.md").write_text(md)
    print(md)


if __name__=="__main__":
    main()
