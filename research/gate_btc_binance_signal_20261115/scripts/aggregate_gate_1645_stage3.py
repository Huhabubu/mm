"""Aggregate six successful exact-parity shadow signal stage3 cases."""
import argparse
import csv
import json
from pathlib import Path
import numpy as np

DAYS=tuple(f"2026-{x:02d}-01" for x in range(6,12))
GROUPS=("inactive_or_no_effect","threatened","opposite")
HORIZONS=("100","500","1000")


def summarize(root,out):
    cases=[]
    for day in DAYS:
        hits=list(root.rglob(f"{day}_stage3.json"))
        if len(hits)!=1:
            raise ValueError(f"Missing or duplicated Gate stage3 case: {day}: {len(hits)}")
        x=json.loads(hits[0].read_text(encoding="utf-8"))
        if (x["day"]!=day or x["exact_parity"]["all_fills"]!=x["total_fills"]
                or not all(x["input_gates"].values())):
            raise ValueError(f"Stage3 frozen source or HBT parity failed: {day}")
        if sum(x["categories"]["100"][g]["all_fills"] for g in GROUPS)!=x["total_fills"]:
            raise ValueError(f"Non-exclusive categories: {day}")
        cases.append(x)
    result={"status":"complete","days":list(DAYS),"audit":"read-only GLFT shadow using exact HBT matched fills",
            "category_definitions":{
                "threatened":"GLFT sell when Binance gap positive, or GLFT buy when gap negative; active+effective at last decision before cancel deadline",
                "opposite":"other fill side during effective shadow signal",
                "inactive_or_no_effect":"no valid active effective signal before cancellation deadline",
                "theoretical_cancel":"threatened, locally cancellable actual GLFT order, shadow quote differs, 20ms side gate ready; hypothetical 1ms request arrives before fill timestamp with 1us guard",
            },"cases":cases,"groups":{}}
    rows=[]
    for h in HORIZONS:
        result["groups"][h]={}
        for g in GROUPS:
            data=[x["categories"][h][g] for x in cases]
            d={}
            d["fills_total"]=sum(x["all_fills"] for x in data)
            d["baseline_only_total"]=sum(x["baseline_only"] for x in data)
            d["shared_total"]=sum(x["shared"] for x in data)
            for name in ("fresh","baseline_only_fresh","shared_fresh",
                         "theoretical_cancellable_fresh"):
                v=[x[name] for x in data if name in x and x[name]["n"]]
                d[name]={
                    "days_valid":len(v),
                    "fresh_fills_total":sum(z["n"] for z in v),
                    "daily_equal_mean_markout_bps":float(np.mean([z["mean_markout_bps"] for z in v])) if v else None,
                    "daily_equal_negative_fraction":float(np.mean([z["negative_fraction"] for z in v])) if v else None,
                    "daily_equal_p10_bps":float(np.mean([z["p10_bps"] for z in v])) if v else None,
                    "daily_equal_worst10_mean_bps":float(np.mean([z["worst10_mean_bps"] for z in v])) if v else None,
                }
            if g=="threatened":
                d["theoretical_cancellable_total"]=sum(x["theoretical_cancellable_count"] for x in data)
                d["theoretical_cancellable_baseline_only_total"]=sum(x["theoretical_cancellable_baseline_only"] for x in data)
            result["groups"][h][g]=d
            rows.append({"horizon_ms":int(h),"category":g,
                         "fills_total":d["fills_total"],
                         "baseline_only_total":d["baseline_only_total"],
                         "shared_total":d["shared_total"],
                         "fresh_fills":d["fresh"]["fresh_fills_total"],
                         "fresh_valid_days":d["fresh"]["days_valid"],
                         "daily_mean_markout_bps":d["fresh"]["daily_equal_mean_markout_bps"],
                         "daily_negative_fraction":d["fresh"]["daily_equal_negative_fraction"],
                         "baseline_only_daily_markout_bps":d["baseline_only_fresh"]["daily_equal_mean_markout_bps"],
                         "shared_daily_markout_bps":d["shared_fresh"]["daily_equal_mean_markout_bps"],
                         "theoretical_cancellable_total":d.get("theoretical_cancellable_total"),
                         "theoretical_cancellable_baseline_only_total":d.get("theoretical_cancellable_baseline_only_total")})
    out.mkdir(parents=True,exist_ok=True)
    (out/"summary.json").write_text(json.dumps(result,indent=2,ensure_ascii=False)+"\n",encoding="utf-8")
    with (out/"group_summary.csv").open("w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0]))
        w.writeheader();w.writerows(rows)
    summary=result["groups"]["100"]
    f=lambda v: "NA" if v is None else f"{v:+.4f}"
    lines=[
        "# Gate BTC 1.645σ：阶段③六日 GLFT 信号暴露与理论可撤审计","",
        "六天独立完整1430分钟正式GLFT、同源 Tardis→HBT 全量重新回放；先逐笔精确比较原始 GLFT 官方 HBT fills(时间/方向/价/量)和手续费权益，相等才接纳该日。",
        "",
        "**信号定义**：原GLFT自身订单/库存路径下，使用完全相同1.645σ/600s时间加权基差、5s发布、1σ锚定带、Binance价格主动变动、方向/幅度优势、2.5ms可见时间及HBT融合Gate中间价，影子评估激活状态。影子计算不参与实际撤挂单。",
        "",
        "**时间**：对于每笔成交，最迟模拟撤单请求时刻=交易所成交时间−1ms请求延迟−1μs保守边界；检索该时刻前最近一次有效策略决策和当时的本地HBT订单状态。理论可请求撤单还须现有订单可撤、影子目标价不同、距本侧上一次挂撤动作≥20ms。**没有真实发送撤单的ACK，因此不能把此标记写成“成功阻止成交”。**",
        "",
        "| 100ms分组 | 原GLFT成交总笔数 | 原GLFT独有成交 | 有效Markout笔数 | 日等权平均Markout(bps) | 负向占比 |",
        "|---|---:|---:|---:|---:|---:|"]
    for g in GROUPS:
        x=summary[g]
        y=x["fresh"]
        lines.append(f'| {g} | {x["fills_total"]} | {x["baseline_only_total"]} | {y["fresh_fills_total"]} | {f(y["daily_equal_mean_markout_bps"])} | {f(y["daily_equal_negative_fraction"])} |')
    x=summary["threatened"]
    lines.extend(["",
        f'受威胁侧中满足“本地有可撤订单+新报价不同+20ms已过”的理论最迟请求条件：**{x["theoretical_cancellable_total"]}笔**；其中在同交易所时间同方向上仅原GLFT成交的 **{x["theoretical_cancellable_baseline_only_total"]}笔**。这些仍不表示β1真实成功取消了对应订单。',
        "",
        "分类仅对**原始GLFT已成交样本**，与阶段②实际β1已成交样本不同；共同/独有只用同交易所撮合时间和方向的一对一匹配，订单ID、队列和真实撤单ACK未配对。",
        "",
        "六日已用于探索、无独立样本外。手续费净收益仍均为负；等成交量无方向对照、每笔订单真实撤单生效时刻仍需进一步重放验证。资金费率/强制期末平仓成本未加入。",
        ""])
    for h in HORIZONS:
        lines.append(f"## {h}ms 分组逐日等权指标")
        lines.append("")
        for g in GROUPS:
            y=result["groups"][h][g]["fresh"]
            lines.append(f'- {g}：有效笔数{y["fresh_fills_total"]}，{y["days_valid"]}/6日有样本，平均Markout {f(y["daily_equal_mean_markout_bps"])}bps，最差10%均值{f(y["daily_equal_worst10_mean_bps"])}bps')
        lines.append("")
    (out/"report.md").write_text("\n".join(lines),encoding="utf-8")
    print("STAGE3_SIXDAY_EXACT_PARITY_AGGREGATE_OK",len(cases),flush=True)


if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--input",required=True)
    p.add_argument("--out",required=True)
    a=p.parse_args()
    summarize(Path(a.input),Path(a.out))
