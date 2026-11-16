"""Gate stage4: 6 independent days, 4-way frozen baseline / beta1 / 2 no-signal controls."""
import argparse
import csv
import json
import math
from pathlib import Path
from statistics import mean
from run_gate_stage4_controls import (
    TRAIN_DAY,EVAL_DAYS,TARGET_NAMES,CONTROL_NAMES,load_locked
)

NAMES=("glft","both_1p0","static_widen","gate_sigma_pause")
HORIZONS=(100,500,1000)


def read_stage2(path):
    with Path(path).open(newline="",encoding="utf-8") as fp:
        rows=list(csv.DictReader(fp))
    return {(r["date"],r["model"],r["side"],int(r["horizon_ms"])):r
            for r in rows}


def make_report(root,locked,stage2_csv,out):
    lock=load_locked(locked)
    m=read_stage2(stage2_csv)
    results={}
    for day in EVAL_DAYS:
        paths=list(Path(root).rglob(f"{day}_stage4.json"))
        if len(paths)!=1:raise ValueError(f"Need exactly one complete control day: {day}")
        r=json.loads(paths[0].read_text(encoding="utf-8"))
        if (r["day"]!=day or r["status"]!="complete" or
            not r["same_source_gate_qa"] or
            r["training_lock_day"]!=TRAIN_DAY or
            not r["binance_direction_disabled_in_controls"] or
            r["selected_controls"]!=lock["selected_controls"]):
            raise ValueError(f"Protocol mismatch for {day}")
        if set(r["controls"])!=set(CONTROL_NAMES):
            raise ValueError("Missing one of two controls")
        result={}
        for model in NAMES:
            if model in TARGET_NAMES:
                tr=r["immutable_frozen_reference"][model]["trading"]
            else:
                tr=r["controls"][model]["trading"]
            row={"date":day,"model":model,
                 "fills":tr["engine_fills"],
                 "notional_usdt":tr["trading_value_usdt"],
                 "maker_fee_usdt":tr["fee_usdt"],
                 "fee_net_pnl_usdt":tr["end_net_marked_equity_usdt"],
                 "zero_fee_pnl_usdt":tr["end_net_marked_equity_usdt"]+tr["fee_usdt"],
                 "mean_abs_inventory_contracts":tr["mean_abs_inventory_contracts"],
                 "rms_inventory_contracts":tr["rms_inventory_contracts"],
                 "max_abs_inventory_contracts":tr["max_abs_position_contracts"],
                 "cancel_requests":tr["cancel_requests"],
                 "order_submissions":tr["submissions"]}
            for h in HORIZONS:
                if model in TARGET_NAMES:
                    q=m.get((day,model,"all",h))
                    if q is None:raise ValueError("Missing official stage2 markout day")
                    vals={"capture_bps":q["capture_bps"],
                          "markout_mean_bps":q["mean_markout_bps"],
                          "markout_p10_bps":q["p10_bps"],
                          "markout_worst10_bps":q["worst10_mean_bps"],
                          "negative_fraction":q["negative_fraction"],
                          "quote_coverage":q["coverage"],
                          "fresh_fills":q["fresh_fills"]}
                else:
                    q=r["controls"][model]["stage4_quality"]["groups"]["all"]["horizons"][str(h)]
                    s=q["fresh"]
                    vals={"capture_bps":r["controls"][model]["stage4_quality"]["groups"]["all"]["capture"]["mean_bps"] if False else None,
                          "markout_mean_bps":s["mean_bps"],
                          "markout_p10_bps":s["p10_bps"],
                          "markout_worst10_bps":s["worst10_mean_bps"],
                          "negative_fraction":s["negative_fraction"],
                          "quote_coverage":q["fresh_fraction"],
                          "fresh_fills":q["fresh_count"]}
                    # The captured spread on same horizon/fresh sample is retained by stage2 per-horizon stats.
                    vals["capture_bps"]=q["capture_on_same_fresh_fills"]["mean_bps"] if q.get("capture_on_same_fresh_fills") else None
                for key,value in vals.items():
                    row[f"{h}ms_{key}"]=float(value) if value not in (None,"") else None
            if (row["maker_fee_usdt"]<0 or row["fills"]<0 or
                row["notional_usdt"]<0):
                raise ValueError(f"Bad maker fee or fills: {day} {model}")
            result[model]=row
        results[day]=result
    # 6-day equal-weighted. Not aggregated by fills; May training is NOT part of evaluation.
    per_day=[r for day in EVAL_DAYS for r in results[day].values()]
    out=Path(out)
    out.mkdir(parents=True,exist_ok=True)
    with (out/"per_day_four_models.csv").open("w",newline="",encoding="utf-8") as fp:
        w=csv.DictWriter(fp,fieldnames=list(per_day[0]))
        w.writeheader();w.writerows(per_day)
    columns=list(per_day[0])
    aggregates={}
    for name in NAMES:
        r=[results[day][name] for day in EVAL_DAYS]
        aggregates[name]={
            "valid_days":len(r),
            "day_equal_mean":{
                key:mean(float(x[key]) for x in r)
                for key in columns if key not in ("date","model")
                and all(x[key] is not None for x in r)
            },
            "sum_fills_six_days":sum(x["fills"] for x in r),
        }
        if aggregates[name]["valid_days"]!=6:
            raise ValueError(f"Unbalanced panel: {name}")
    target=aggregates["both_1p0"]["day_equal_mean"]["fills"]
    for name in CONTROL_NAMES:
        a=aggregates[name]["day_equal_mean"]
        a["difference_vs_beta1_mean_fills"]=a["fills"]-target
        a["relative_fill_gap_vs_beta1"]=(a["fills"]-target)/target
        a["relative_notional_gap_vs_beta1"]=(a["notional_usdt"]-
            aggregates["both_1p0"]["day_equal_mean"]["notional_usdt"]) / aggregates["both_1p0"]["day_equal_mean"]["notional_usdt"]
    paired={}
    for model in CONTROL_NAMES:
        paired[model]={
            "six_day_paired_beta1_minus_control_fee_net_usdt":mean(
                results[day]["both_1p0"]["fee_net_pnl_usdt"]-results[day][model]["fee_net_pnl_usdt"]
                for day in EVAL_DAYS),
            "six_day_paired_beta1_minus_control_zero_fee_usdt":mean(
                results[day]["both_1p0"]["zero_fee_pnl_usdt"]-results[day][model]["zero_fee_pnl_usdt"]
                for day in EVAL_DAYS),
            "daily_delta_fee_net_usdt":{
                day:results[day]["both_1p0"]["fee_net_pnl_usdt"]-results[day][model]["fee_net_pnl_usdt"]
                for day in EVAL_DAYS},
            "six_day_count_beta1_better_fee_net":sum(
                results[day]["both_1p0"]["fee_net_pnl_usdt"]>results[day][model]["fee_net_pnl_usdt"]
                for day in EVAL_DAYS)
        }
    payload={"status":"complete","training_day":TRAIN_DAY,"training_lock":lock,
             "evaluation_days":list(EVAL_DAYS),"num_evaluation_days":6,
             "baseline_immutable":True,"new_venue_direction_used":False,
             "matched_on":"May-01 trading fill count only, not June-Nov realized fills",
             "daily_results":results,"equal_day_summary":aggregates,
             "paired_effects":paired,
             "cautions":["Old six evaluation days are exploratory, not true out-of-sample",
                         "Rate match calibrated only on May; unequal later fills must be reported without retuning",
                         "Control A static widen and control B Gate-only sigma pause change queue/exposure and may differ in cancel intensity",
                         "Missing funding, forced final unwinding, real exchange latencies and market capture synchronization"]}
    (out/"summary.json").write_text(
        json.dumps(payload,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    lines=["# Gate 1.645σ：阶段④无Binance方向的成交强度对照","",
           "**阶段④固定两类对照**：A仅使用预先 May 01 训练日选择的双边GLFT固定扩宽；B仅使用Gate本地过去波动率的高波动暂停挂单。两者β=0，在正式HBT原始撮合和Binance事件时钟上运行；全程无需Binance价格方向做报价。5月为训练日，六个6—11月评价日没有用于参数选择。","",
           f"May完整日Binance β1目标成交：{lock['beta1_train_target_fills']}笔；原GLFT：{lock['beta0_train_fills']}笔。May选择A外移{lock['selected_controls']['static_widen']['extra_half_ticks']:.6f}目标tick；B Gate自身sigma阈值{lock['selected_controls']['gate_sigma_pause']['local_sigma_ceiling']:.6f} tick/sqrt(s)。",
           "",
           "| 六日等权、每完整日 | GLFT β0 | Binance β1 | A 固定扩宽 | B Gate波动过滤 |",
           "|---|---:|---:|---:|---:|"]
    pretty=[
        ("fills","实际成交（笔）",1),
        ("notional_usdt","名义成交额（USDT）",2),
        ("zero_fee_pnl_usdt","零手续费权益（USDT）",3),
        ("fee_net_pnl_usdt","Maker+0.02%净权益（USDT）",3),
        ("maker_fee_usdt","实际手续费（USDT）",3),
        ("mean_abs_inventory_contracts","平均绝对库存（张）",3),
        ("rms_inventory_contracts","库存均方根（张）",3),
        ("max_abs_inventory_contracts","最大绝对库存（张；逐日最大值再等权）",2),
        ("cancel_requests","撤单请求（次）",1),
        ("order_submissions","订单提交（次）",1),
        ("100ms_markout_mean_bps","100ms方向Markout（bps）",4),
        ("100ms_markout_worst10_bps","100ms最差10%均值（bps）",4),
        ("500ms_markout_mean_bps","500ms方向Markout（bps）",4),
        ("1000ms_markout_mean_bps","1s方向Markout（bps）",4),
    ]
    for metric,label,ndec in pretty:
        values=[aggregates[name]["day_equal_mean"].get(metric) for name in NAMES]
        lines.append(f"| {label} | "+ " | ".join(
            f"{v:,.{ndec}f}" if v is not None else "NA" for v in values)+" |")
    lines.extend(["","## 重要核验",""])
    for name in CONTROL_NAMES:
        x=aggregates[name]["day_equal_mean"]
        p=paired[name]
        lines.append(f"- {name}：评价日相对β1每日成交量差{x['difference_vs_beta1_mean_fills']:+.2f}笔（相对{x['relative_fill_gap_vs_beta1']:+.2%}），名义额差{x['relative_notional_gap_vs_beta1']:+.2%}；β1比它每天少亏/多赚手续费后 {p['six_day_paired_beta1_minus_control_fee_net_usdt']:+.3f} USDT，零手续费 {p['six_day_paired_beta1_minus_control_zero_fee_usdt']:+.3f} USDT，净权益6日中β1更优{p['six_day_count_beta1_better_fee_net']}/6日。")
    lines.extend(["","### 解释限制",
        "这是May预训练后冻结参数的、六个此前已探索日期上的样本检验；若A/B在六日无法与β1在成交量/名义额上接近，只能报告“匹配失败”，不能当作等成交量因果识别，也不能回头用评价日再次选参数。",
        "同成交笔数不等同同成交机会、成交时间、仓位风险或撤单生效；单独表明谁更多亏损不能精确分配55.58%的减亏。核心Markout取两端报价年龄≤500ms的本地可见成交后变化（不是PnL组成项）；原有VIP0 Maker+0.02%计入，零手续费是用于拆机制的诊断。",
        "资金费率、主动期末清仓成本、真实延迟误差和独立新日期样本外检验仍为限制。",""])
    (out/"report.md").write_text("\n".join(lines),encoding="utf-8")
    print("STAGE4_FINAL_SIXDAY",json.dumps({
        x:{k:aggregates[x]["day_equal_mean"][k] for k in
           ("fills","fee_net_pnl_usdt","zero_fee_pnl_usdt")}
        for x in NAMES},ensure_ascii=False),flush=True)


if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--daily",required=True)
    p.add_argument("--lock",required=True)
    p.add_argument("--stage2",required=True)
    p.add_argument("--out",required=True)
    a=p.parse_args()
    make_report(a.daily,a.lock,a.stage2,a.out)
