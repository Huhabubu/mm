"""Read-only initial SOL GLFT fit failure audit from official corrected HBT JSON.

Does not label fallback parameters as successfully fitted GLFT. Candidate
historical priors are proposed ONLY from prior UTC dates; same-day future
data, future months and test-period fill targets must never tune this branch.
"""
import argparse
import json
from pathlib import Path


def analyze_day(obj):
    day=obj["day"]
    if obj["market"]["coin"]!="SOL" or obj["market"]["exchange"]!="okex-swap":
        raise ValueError("Not an OKX SOL case")
    result={"day":day,"status":obj["status"],"fit_source":"10min history only"}
    if obj["status"]=="complete":
        init=obj["variants"]["glft"]["initial_parameters"]
        result.update({"reason":"calibrated","initial_A_per_sec":init["A_per_second"],
                       "initial_k_per_tick":init["k_per_tick"],
                       "initial_sigma_tick_sqrt_s":init["sigma_tick_per_sqrt_second"]})
        return result
    d=obj.get("calibration_diagnostic") or {}
    if obj["status"]!="no_glft_calibration":
        return {**result,"reason":"other_failure","details":obj.get("error","unknown")}
    n=int(d.get("positive_levels",-1))
    crosses=int(d.get("crossing_100ms_intervals",-1))
    volatility=int(d.get("valid_volatility_samples",-1))
    reasons=[]
    if n<5:reasons.append("fewer_than_5_observed_positive_distance_levels")
    if crosses<10:reasons.append("fewer_than_10_crossing_intervals")
    if volatility<3000:reasons.append("fewer_than_half_valid_volatility_samples")
    if not reasons:reasons.append("estimator_numeric_or_nonpositive_A_k_sigma_requires_detail")
    result.update({"reason":reasons,"n_positive_levels":n,
                   "crossing_intervals":crosses,"valid_volatility_samples":volatility,
                   "extreme_intervals":d.get("extreme_crossing_intervals"),
                   "max_crossing_ticks":d.get("max_crossing_ticks"),
                   "min_required_levels":5,"min_required_crossing_intervals":10})
    return result


def summarize(items):
    if len(items)!=6:
        raise ValueError(f"Expected six dated OKX SOL reports, found {len(items)}")
    rows=[analyze_day(r) for r in sorted(items,key=lambda x:x["day"])]
    dates=[x["day"] for x in rows]
    if len(set(dates))!=6:raise ValueError("Duplicate SOL dates")
    usable={}
    for row in rows:
        day=row["day"]
        earlier=sorted(x for x in usable if x<day)
        row["preexisting_successful_prior_day"]=earlier[-1] if earlier else None
        row["prior_is_only_research_candidate"]=bool(earlier)
        if row["status"]=="complete":
            usable[day]={"A":row["initial_A_per_sec"],
                         "k":row["initial_k_per_tick"]}
    return rows


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--source",required=True)
    p.add_argument("--out",required=True)
    args=p.parse_args()
    reports=[]
    for file in Path(args.source).rglob("okx_SOL_*_50m.json"):
        reports.append(json.loads(file.read_text(encoding="utf-8")))
    rows=summarize(reports)
    out=Path(args.out);out.mkdir(parents=True,exist_ok=True)
    (out/"diagnosis.json").write_text(json.dumps(rows,ensure_ascii=False,indent=2),encoding="utf-8")
    lines=["# OKX SOL GLFT 10分钟预热拟合失败：只读根因复核","",
           "|日期|官方状态|有效报价距离档|成交跨越时间窗|主要拒绝原因|只允许使用的已有成功先验日|",
           "|---|---|---:|---:|---|---|"]
    for row in rows:
        reasons=row["reason"] if isinstance(row["reason"],str) else ", ".join(row["reason"])
        lines.append(f"|{row['day']}|{row['status']}|{row.get('n_positive_levels','—')}|"
                     f"{row.get('crossing_intervals','—')}|{reasons}|"
                     f"{row.get('preexisting_successful_prior_day') or '无'}|")
    lines.extend(["","**初步修复方案（未来独立实验，尚未执行）**：",
                  "1. 保持正式G0原始≥5档门槛不变；将此次不通过标记为参数不可识别。",
                  "2. 研究版可以尝试历史分层收缩：k向**严格早于当日的已通过样本**收缩，"
                  "当日A仅从预热10分钟的交易穿越计数估计；sigma仍使用当日预热波动。",
                  "3. 先报告信息覆盖与事前参数可识别性，只有物理合理且经后续日验证后"
                  "才能称为可交易的‘稳健改进GLFT’；历史先验不可拿未来10月数据去补7月。",
                  "4. 如若无充分同币种历史先验，拒绝生成报价。达不到6/6也须保留事实。",
                  "5. 调整半tick分档、将累计跨距当独立观察点会造成重复计数假象；"
                  "不能用来机械把4档变成8档。",
                  "该工作只读取旧官方日志及预热诊断，不产生任何新模拟收益。"])
    report="\n".join(lines)+"\n"
    (out/"diagnosis.md").write_text(report,encoding="utf-8")
    print(report)


if __name__=="__main__":
    main()
