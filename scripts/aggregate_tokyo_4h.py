"""Strict 3-day by-market aggregation of locked pure-Tokyo 4h HBT cases.

No tuning with test dates. Stage core = 4 markets x 3 dates;
stage delay = 3 OKX markets x 3 dates. Nothing past 2026-11-01.
"""
import argparse
import json
from pathlib import Path
import numpy as np
from experiment_tokyo_4h import DAYS,MARKETS

HORIZONS=("100ms","500ms")
GROUPS=("Group_A_active_or_200ms_post_cancel","Kept",
        "Group_B_no_local_defense_or_cancel_tail","Dropped_all")
DELAY_MARKETS=tuple(m for m in MARKETS if m[1]=="okex-swap")


def load(root,stage,markets):
    cases={}
    for p in Path(root).rglob("case.json"):
        d=json.loads(p.read_text())
        if d.get("stage")!=stage:raise ValueError(f"Unexpected {d.get('stage')}: {p}")
        k=(d["date"],d["coin"],d["venue"])
        if k in cases:raise ValueError("Duplicate "+str(k))
        cases[k]=d
    expected={(d,*m) for d in DAYS for m in markets}
    if set(cases)!=expected:
        raise ValueError(f"Missing {sorted(expected-set(cases))}, additional {sorted(set(cases)-expected)}")
    return cases


def aggregate_rows(rows,model):
    z=[a["variants"][model] for a in rows]
    g=[a["variants"]["g0"] for a in rows]
    ret=[x["fills"]/b["fills"] if b["fills"] else None for x,b in zip(z,g)]
    n=sum(x["fills"] for x in g)
    out={
        "daily_fills":[x["fills"] for x in z],
        "total_fills":sum(x["fills"] for x in z),
        "G0_total_fills":n,
        "pooled_retention":sum(x["fills"] for x in z)/n if n else None,
        "daily_retention":ret,
        "daily_duty_fraction":[x["defense_duty_fraction"] for x in z],
        "mean_daily_duty_fraction":float(np.mean([x["defense_duty_fraction"] for x in z])),
        "daily_cancel_delta_vs_G0":[x["cancel_requests"]-b["cancel_requests"] for x,b in zip(z,g)],
        "total_cancel_delta_vs_G0":sum(x["cancel_requests"]-b["cancel_requests"] for x,b in zip(z,g)),
        "daily_net_after_fee_usdt":[x["net_after_2bps_usdt"] for x in z],
        "mean_daily_net_after_fee_usdt":float(np.mean([x["net_after_2bps_usdt"] for x in z])),
        "mean_daily_gross_marked_usdt":float(np.mean([x["gross_marked_usdt"] for x in z])),
        "total_official_maker_fees_usdt":sum(x["fee_usdt"] for x in z),
        "strict_ACK_confirmed_avoided_total":0,
        "strict_ACK_confirmed_avoided_by_day":[],
        "source_variant":model
    }
    if model=="g0":
        return out
    for x in z:
        attr=x["order_level"]
        out["strict_ACK_confirmed_avoided_by_day"].append(
            attr["strict_same_submit_order_id_cancel_ACK_before_G0_fill"]["count"])
    out["strict_ACK_confirmed_avoided_total"]=sum(out["strict_ACK_confirmed_avoided_by_day"])
    for k in GROUPS:
        out[k]={}
        for horizon in HORIZONS:
            t=[x["group_window_observational"][k][horizon] for x in z]
            fresh=[(a["fresh_n"],a["markout_mean_bps"]) for a in t if a["markout_mean_bps"] is not None]
            n=sum(v[0] for v in fresh)
            out[k][horizon]={
                "valid_fills":n,
                "pooled_markout_bps":sum(cnt*v for cnt,v in fresh)/n if n else None,
                "daily_markout_bps":[a["markout_mean_bps"] for a in t]}
    out["selectivity_lift_daily_equal"]={}
    for horizon in HORIZONS:
        kept=out["Kept"][horizon]["daily_markout_bps"]
        drop=out["Dropped_all"][horizon]["daily_markout_bps"]
        vals=[(a-b if a is not None and b is not None else None) for a,b in zip(kept,drop)]
        observed=[v for v in vals if v is not None]
        out["selectivity_lift_daily_equal"][horizon]={
            "daily":vals,"valid_days":len(observed),
            "mean_bps":float(np.mean(observed)) if observed else None}
    return out


def aggregate_core(root):
    cases=load(root,"core",MARKETS)
    out={}
    for coin,venue in MARKETS:
        rows=[cases[(d,coin,venue)] for d in DAYS]
        expected={"g0","z1","z1_p80","g0_gamma75","z1_gamma75"}
        for r in rows:
            if set(r["variants"])!=expected or r["trade_steps"]!=144000:
                raise ValueError("Incomplete 4h variant calibration")
        info={model:aggregate_rows(rows,model) for model in expected}
        total=sum(r["variants"]["g0"]["fills"] for r in rows)
        average_duty=info["z1"]["mean_daily_duty_fraction"]
        retention=info["z1"]["pooled_retention"]
        info["decision_flags"]={
            "sol_doge_z075_eligible":bool(
                coin in ("SOL","DOGE") and average_duty<.0025 and retention>.95),
            "pepe_P80_precision_check_recommended":bool(
                coin=="1000PEPE" and average_duty>.025),
            "z075_status":"not_executed_in_core",
            "model_selection_requires_precommitted_freeze":True
        }
        info["gamma_sensitivity"]={
            "gamma_50": {
                "g0_gross_mean_usdt":info["g0"]["mean_daily_gross_marked_usdt"],
                "source_gross_mean_usdt":info["z1"]["mean_daily_gross_marked_usdt"],
                "source_minus_g0_gross_mean_usdt":info["z1"]["mean_daily_gross_marked_usdt"]-
                    info["g0"]["mean_daily_gross_marked_usdt"]},
            "gamma_75": {
                "g0_gross_mean_usdt":info["g0_gamma75"]["mean_daily_gross_marked_usdt"],
                "source_gross_mean_usdt":info["z1_gamma75"]["mean_daily_gross_marked_usdt"],
                "source_minus_g0_gross_mean_usdt":info["z1_gamma75"]["mean_daily_gross_marked_usdt"]-
                    info["g0_gamma75"]["mean_daily_gross_marked_usdt"]}}
        out[f"{coin}_{venue}"]=info
    return {"date_scope":DAYS,"trading_window":"00:20–04:20 UTC",
            "warmup_window":"00:00–00:20 UTC",
            "official_HBT":True,"fee_maker_bps":2.,
            "markets":out,"calibration_day_0815_status":"requires paid Tardis access; no training on test days",
            "no_blind_days_opened":True,
            "limitations":[
                "All three days have already been explored; results cannot be described as OOS",
                "Only identical HBT synthetic order id + submit timestamp and qty + local cancel ACK before G0 fill gives conservative count",
                "Order ID is price-derived and reused; failed lineage matching stays unattributed",
                "Order ACK is simulated under 1ms request and response latency, not measured live ACK",
                "Group B and Added include queue/inventory/path effects and cannot be called causal queue-loss",
                "No order model here establishes real exchange routing or RTT"]}


def aggregate_delay(root):
    cases=load(root,"delay",DELAY_MARKETS)
    res={}
    for coin,venue in DELAY_MARKETS:
        rows=[cases[(d,coin,venue)] for d in DAYS]
        part={}
        for delay in (0.,1.,2.5):
            part[f"{delay:g}ms"]=aggregate_rows(rows,f"z1_delay_{delay:g}ms")
        res[f"{coin}_{venue}"]=part
    return {"date_scope":DAYS,"model":"fixed Z1 cancel QS",
            "extra_binance_delay_ms":[0,1,2.5],"markets":res,
            "absolute_end_to_end_cancel_latency_not_measured":True,
            "limit":"Conservative HBT simulated ACK plus original submission matching only; not empirical latency"}


def md_core(x):
    s=["# 纯东京 4 小时：G0/Z1/P80 与 γ 稳健性",
       "", "三个已探索日期，每日20min预热+240min交易，统一HBT Maker 2bps。毛收益仅机制诊断，最终经济收益以净权益为准。", "",
       "|市场|模型|3日G0成交|3日模型成交|保留率|激活时间%|3日撤单差|日均净USDT|日均毛USDT|ACK严格匹配笔数|",
       "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for market,d in x["markets"].items():
        for model in ("g0","z1","z1_p80","g0_gamma75","z1_gamma75"):
            v=d[model];rt=v["pooled_retention"]
            s.append(f"|{market}|{model}|{v['G0_total_fills']}|{v['total_fills']}|{rt:.1%}|{100*v['mean_daily_duty_fraction']:.3f}%|{v['total_cancel_delta_vs_G0']:+d}|{v['mean_daily_net_after_fee_usdt']:+.4f}|{v['mean_daily_gross_marked_usdt']:+.4f}|{v['strict_ACK_confirmed_avoided_total']}|")
    s.extend(["","## Group A / Kept / Group B 500ms Markout、整体按日等权选择性",
              "单位bps，按日等权用于核心结论；ACK严格计数只是一组可证明匹配的保守下界。",
              "|市场|模型|Group A pooled|Kept pooled|Group B pooled|100ms Lift 日等权|500ms Lift 日等权|",
              "|---|---|---:|---:|---:|---:|---:|"])
    for market,d in x["markets"].items():
        for model in ("z1","z1_p80","z1_gamma75"):
            v=d[model]
            def fmt(g):
                z=v[g]["500ms"]["pooled_markout_bps"]
                return f"{z:+.3f}" if z is not None else "NA"
            def lift(h):
                z=v["selectivity_lift_daily_equal"][h]["mean_bps"]
                return f"{z:+.3f}" if z is not None else "NA"
            s.append(f"|{market}|{model}|{fmt('Group_A_active_or_200ms_post_cancel')}|{fmt('Kept')}|{fmt('Group_B_no_local_defense_or_cancel_tail')}|{lift('100ms')}|{lift('500ms')}|")
    s.extend(["","## 预设 Z0.75 启用门槛",
              "仅当SOL/DOGE原Z1在三个4h日期的平均激活占比低于0.25%且总成交保留率大于95%，才启动Z0.75。PEPE的P80为原始预定对照，不基于本轮重新优化。",""])
    for m,d in x["markets"].items():
        s.append(f"- {m}: Z0.75触发={d['decision_flags']['sol_doge_z075_eligible']}；PEPE P80检查={d['decision_flags']['pepe_P80_precision_check_recommended']}")
    s.extend(["","γ=50与75的日均毛结果差已保存在JSON；两侧统一风险厌恶系数。观察仍需独立日期复验。",
             "8月15日缺少付费Tardis授权，等成交量无Binance对照尚未准入。"])
    return "\n".join(s)+"\n"


def md_delay(x):
    s=["# 纯东京 4 小时订单级防御与额外延迟",
       "", "0/1/2.5ms为Binance额外可见延迟，HBT 1ms发送及1ms响应固定。订单级严格匹配必须具备同一模拟订单提交谱系和更早Cancel ACK；未匹配保持未知。", "",
       "|市场|BN额外延迟|总成交|保留率|Z1激活时间%|防御窗内500ms Markout|严格ACK笔数|3日净USDT|",
       "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for market,d in x["markets"].items():
        for delay,v in d.items():
            a=v["Group_A_active_or_200ms_post_cancel"]["500ms"]["pooled_markout_bps"]
            s.append(f"|{market}|{delay}|{v['total_fills']}|{v['pooled_retention']:.1%}|{100*v['mean_daily_duty_fraction']:.3f}%|{f'{a:+.3f}' if a is not None else 'NA'}|{v['strict_ACK_confirmed_avoided_total']}|{3*v['mean_daily_net_after_fee_usdt']:+.4f}|")
    s.extend(["","Group B 与 Added 依然存在路径依赖，不把窗口外漏单表述为已识别的队列损失。"])
    return "\n".join(s)+"\n"


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--stage",choices=("core","delay"),required=True)
    p.add_argument("--source",required=True)
    p.add_argument("--out",required=True)
    a=p.parse_args()
    dest=Path(a.out);dest.mkdir(parents=True,exist_ok=True)
    z=aggregate_core(a.source) if a.stage=="core" else aggregate_delay(a.source)
    name="tokyo_four_hour_"+a.stage
    (dest/(name+".json")).write_text(json.dumps(z,indent=2,ensure_ascii=False,allow_nan=False))
    report=md_core(z) if a.stage=="core" else md_delay(z)
    (dest/(name+".md")).write_text(report)
    print(report)


if __name__=="__main__":
    main()
