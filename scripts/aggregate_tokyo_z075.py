"""Aggregate only the predeclared Z0.75 SOL condition from three Tokyo 4h days."""
import argparse
import json
from pathlib import Path
from experiment_tokyo_4h import DAYS


def aggregate(root,core_json=None):
    days={}
    for p in Path(root).rglob("case.json"):
        x=json.loads(p.read_text())
        if x.get("stage")!="z075" or x.get("coin")!="SOL" or x.get("venue")!="okex-swap":
            raise ValueError("Only SOL OKX predeclared Z0.75 cases")
        day=x["date"]
        if day in days or day not in DAYS:raise ValueError("Duplicate/disallowed Z0.75 day")
        if set(x["variants"])!={"g0","z075"}:raise ValueError("Unexpected tuned variants")
        days[day]=x
    if set(days)!=set(DAYS):raise ValueError("Incomplete predeclared 3-day Z0.75")
    ordered=[days[d] for d in DAYS]
    base=[a["variants"]["g0"] for a in ordered]
    test=[a["variants"]["z075"] for a in ordered]
    for g,z in zip(base,test):
        if g["initial_GLFT"]!=z["initial_GLFT"]:
            raise ValueError("Different A/k/sigma baseline")
    numerator=sum(a["fills"] for a in test);denominator=sum(a["fills"] for a in base)
    group={}
    for name in ("Group_A_active_or_200ms_post_cancel","Kept",
                 "Group_B_no_local_defense_or_cancel_tail","Dropped_all"):
        group[name]={}
        for h in ("100ms","500ms"):
            y=[a["group_window_observational"][name][h] for a in test]
            ok=[(b["fresh_n"],b["markout_mean_bps"]) for b in y if b["markout_mean_bps"] is not None]
            n=sum(v[0] for v in ok)
            group[name][h]={"n":n,"pooled_mean_bps":sum(k*v for k,v in ok)/n if n else None,
                            "daily_mean_bps":[a["markout_mean_bps"] for a in y]}
    day_lift={}
    for h in ("100ms","500ms"):
        k=group["Kept"][h]["daily_mean_bps"]
        d=group["Dropped_all"][h]["daily_mean_bps"]
        v=[a-b for a,b in zip(k,d) if a is not None and b is not None]
        day_lift[h]={"valid_days":len(v),"equal_day_mean_bps":sum(v)/len(v) if v else None}
    compared_to_Z1=None
    if core_json is not None:
        allcore=json.loads(Path(core_json).read_text())["markets"]["SOL_okex-swap"]
        ref=allcore["z1"]
        same_by_day=[]
        for i,(g,z) in enumerate(zip(base,test)):
            same=(g["fills"]==allcore["g0"]["daily_fills"][i] and
                  z["fills"]==ref["daily_fills"][i] and
                  abs(z["defense_duty_fraction"]-ref["daily_duty_fraction"][i])<1e-10 and
                  abs(z["net_after_2bps_usdt"]-ref["daily_net_after_fee_usdt"][i])<1e-6 and
                  z["cancel_requests"]-g["cancel_requests"]==
                     ref["daily_cancel_delta_vs_G0"][i] and
                  z["order_level"]["strict_same_submit_order_id_cancel_ACK_before_G0_fill"]["count"]==
                     ref["strict_ACK_confirmed_avoided_by_day"][i])
            same_by_day.append(bool(same))
        compared_to_Z1={
            "same_daily_fills_duty_net_cancels_and_ACK":same_by_day,
            "all_three_dates_execution_metrics_identical":all(same_by_day),
            "reduced_z_might_bind_to_floor_or_leave_discrete_action_unchanged":True,
            "cannot_assert_different_effective_threshold_without_raw_gate_values":True}
    return {
        "Z075_vs_Z1":compared_to_Z1,
        "scope":"Only SOL OKX, three previously explored pure Tokyo 4h days",
        "dates":list(DAYS),
        "precommitted_gate":"Z1 duty <0.25% AND pooled retention >95% on the same 3 4h explored days",
        "chosen_z_multiplier":.75,
        "fixed_adaptive_z_gap":1.125,"fixed_adaptive_z_imp":.75,
        "G0_fills":denominator,"z075_fills":numerator,
        "retention":numerator/denominator if denominator else None,
        "duty_equal_day_mean":sum(a["defense_duty_fraction"] for a in test)/3,
        "ACK_strict_total":sum(a["order_level"]["strict_same_submit_order_id_cancel_ACK_before_G0_fill"]["count"] for a in test),
        "cancel_request_delta":sum(a["cancel_requests"]-b["cancel_requests"] for a,b in zip(test,base)),
        "mean_daily_gross_usdt_G0":sum(a["gross_marked_usdt"] for a in base)/3,
        "mean_daily_gross_usdt_Z075":sum(a["gross_marked_usdt"] for a in test)/3,
        "mean_daily_net_2bps_usdt_G0":sum(a["net_after_2bps_usdt"] for a in base)/3,
        "mean_daily_net_2bps_usdt_Z075":sum(a["net_after_2bps_usdt"] for a in test)/3,
        "groups":group,"selectivity_lift":day_lift,
        "no_further_threshold_search":True,
        "not_untouched_sample_out":True
    }


def markdown(x):
    def f(y):return f"{y:+.4f}" if y is not None else "NA"
    return ("# 纯东京 SOL Z0.75 最终预设门槛诊断\n\n"
            f"仅已探索09/10/11每月1日，单日20min预热+4h HBT；三天G0成交{x['G0_fills']}、Z0.75成交{x['z075_fills']}、"
            f"成交保留率{x['retention']:.2%}、防御激活{x['duty_equal_day_mean']:.3%}。\n\n"
            f"严格订单提交谱系+模拟Cancel ACK对应避免数：{x['ACK_strict_total']}；"
            f"撤单请求相对G0变化{x['cancel_request_delta']:+d}。\n\n"
            f"日均G0/Z0.75含费净收益：{f(x['mean_daily_net_2bps_usdt_G0'])}/"
            f"{f(x['mean_daily_net_2bps_usdt_Z075'])} USDT；"
            f"费前毛收益：{f(x['mean_daily_gross_usdt_G0'])}/"
            f"{f(x['mean_daily_gross_usdt_Z075'])} USDT。\n\n"
            f"按日等权100/500ms Selectivity Lift："
            f"{f(x['selectivity_lift']['100ms']['equal_day_mean_bps'])}/"
            f"{f(x['selectivity_lift']['500ms']['equal_day_mean_bps'])} bps。\n\n"
            f"与Z1同日执行指标完全相同：{x['Z075_vs_Z1']['all_three_dates_execution_metrics_identical'] if x['Z075_vs_Z1'] else '未复核'}；" +
            "若完全相同，可能与阈值下限或离散订单动作有关，尚需门槛值审计。\n\n"
            "仅这一个额外预设档位，不以效果好坏调第三次参数。"
            "所有日期均为历史探索，不能宣称新的样本外证据。\n")


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--source",required=True)
    p.add_argument("--core-json",default=None)
    p.add_argument("--out",required=True)
    a=p.parse_args()
    out=Path(a.out);out.mkdir(parents=True,exist_ok=True)
    x=aggregate(a.source,a.core_json)
    (out/"tokyo_four_hour_z075.json").write_text(json.dumps(x,ensure_ascii=False,indent=2,allow_nan=False))
    (out/"tokyo_four_hour_z075.md").write_text(markdown(x))
    print(markdown(x))


if __name__=="__main__":
    main()
