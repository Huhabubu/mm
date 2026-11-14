"""Read-only paired fill opportunity audit for corrected P0 official HBT records.

Strict same exchange-timestamp and side matching only. A baseline-only fill
means no same-time same-side fill in the variant. It is NOT proof that a
specific cancellation reached the exchange before the baseline fill.

Uses 100/500ms post-local-observation native BBO markouts produced by the
existing checked Recorder attribution; does not modify the HBT ledger.
"""
import argparse
import json
import statistics
from collections import defaultdict, deque
from pathlib import Path

import numpy as np

COMPARISONS = (
    ("glft", "glft_binance"),
    ("glft", "glft_local_peak"),
    ("glft_local_peak", "glft_binance_local_peak"),
)
HORIZONS = (100, 500)


def load_fill(path):
    with np.load(path) as z:
        required = {"exchange_timestamp_ns", "side", "local_observed_ns",
                    "signed_markout_bps_100ms", "signed_markout_bps_500ms",
                    "fresh_100ms", "fresh_500ms", "exec_qty",
                    "native_mid_at_observation"}
        if not required.issubset(z.files):
            raise ValueError(f"Annotation columns missing {required-set(z.files)}")
        return {k:z[k].copy() for k in z.files}


def pair_exact_clock_side(baseline, variant):
    """One-to-one multiset pairing; no false net-dropped-fill accounting."""
    n, m = len(baseline["side"]), len(variant["side"])
    remaining=defaultdict(deque)
    for j in range(m):
        remaining[(int(variant["exchange_timestamp_ns"][j]),
                   int(variant["side"][j]))].append(j)
    same_b=np.zeros(n,dtype=bool)
    same_v=np.zeros(m,dtype=bool)
    for i in range(n):
        key=(int(baseline["exchange_timestamp_ns"][i]),
             int(baseline["side"][i]))
        if remaining[key]:
            j=remaining[key].popleft()
            same_b[i]=same_v[j]=True
    if same_b.sum()!=same_v.sum():
        raise RuntimeError("Fill one-to-one pairing mismatch")
    return same_b,same_v


def group_stats(annotations, selected):
    n=int(np.count_nonzero(selected))
    groups={}
    for h in HORIZONS:
        v=selected & annotations[f"fresh_{h}ms"].astype(bool)
        x=annotations[f"signed_markout_bps_{h}ms"][v].astype(float)
        q=annotations["exec_qty"][v].astype(float)
        m=annotations["native_mid_at_observation"][v].astype(float)
        ok=np.isfinite(x)&np.isfinite(q)&np.isfinite(m)&(q>0)&(m>0)
        x,q,m=x[ok],q[ok],m[ok]
        notional=q*m # constant contract value cancels within the same market
        groups[f"{h}ms"]={
            "fresh_fills":len(x),
            "mean_signed_markout_bps":float(np.mean(x)) if len(x) else None,
            "notional_weighted_signed_markout_bps":(
                float(np.average(x,weights=notional)) if len(x) else None),
            "negative_fraction":float(np.mean(x<0)) if len(x) else None,
            "p05_signed_markout_bps":float(np.quantile(x,0.05)) if len(x) else None,
            "p10_signed_markout_bps":float(np.quantile(x,0.10)) if len(x) else None,
        }
    return {"fill_count":n,"markout":groups}


def audit_case(case_dir):
    parts=case_dir.name.split("_")
    if len(parts)!=3:
        raise ValueError(f"Unexpected market-day case {case_dir.name}")
    venue,coin,day=parts
    output={"venue":venue,"coin":coin,"day":day,"comparisons":{}}
    for baseline_name,test_name in COMPARISONS:
        b=load_fill(case_dir / (baseline_name+".npz"))
        t=load_fill(case_dir / (test_name+".npz"))
        shared_b,shared_t=pair_exact_clock_side(b,t)
        if shared_b.sum()+len(b["side"])-shared_b.sum()!=len(b["side"]):
            raise RuntimeError("Inconsistent fill partition")
        name=f"{test_name}_vs_{baseline_name}"
        output["comparisons"][name]={
            "baseline":baseline_name,"variant":test_name,
            "baseline_total":len(b["side"]),
            "variant_total":len(t["side"]),
            "shared_same_time_same_side":int(shared_b.sum()),
            "baseline_only":group_stats(b,~shared_b),
            "baseline_shared":group_stats(b,shared_b),
            "variant_only":group_stats(t,~shared_t),
            "variant_shared":group_stats(t,shared_t),
            "note":"Same exact exchange timestamp and side, not same order ID/queue position/cancel ACK.",
        }
    return output


def aggregate(results):
    out={"cases":results, "by_market":{}}
    lines=["# 修复后P0的同刻同侧成交反事实审计（官方HBT）","",
           "标记baseline-only：原始GLFT在该时刻同方向成交，策略对照未在完全相同交易所时刻同方向成交。",
           "标记shared：同交易所时间戳且同方向均成交；不等于同一订单或者相同队列。",
           "比较成交后100/500ms的目标原生BBO中间价变化，以本地回报时刻作为起点。",
           "**这份统计无法证明被取消的具体订单已经在交易所生效，也不代表等成交数对照。**",""]
    for market in sorted({(r["venue"],r["coin"]) for r in results}):
        v,c=market
        reports=[x for x in results if (x["venue"],x["coin"])==market]
        lines.extend([f"## {v} {c}: {len(reports)} 个可用样本日（每日期50分钟）","",
                      "| 对照 | 共有成交 | GLFT独有成交 | 对照独有成交 | GLFT独有100ms均值bps | GLFT共有100ms均值bps | 有效日 |",
                      "|---|---:|---:|---:|---:|---:|---:|"])
        out["by_market"][f"{v}_{c}"]={}
        for baseline_name,test_name in COMPARISONS:
            key=f"{test_name}_vs_{baseline_name}"
            valid=[r["comparisons"][key] for r in reports]
            all_shared=sum(x["shared_same_time_same_side"] for x in valid)
            only=sum(x["baseline_only"]["fill_count"] for x in valid)
            new=sum(x["variant_only"]["fill_count"] for x in valid)
            paired=[(x["baseline_only"]["markout"]["100ms"]["mean_signed_markout_bps"],
                     x["baseline_shared"]["markout"]["100ms"]["mean_signed_markout_bps"])
                    for x in valid]
            paired=[x for x in paired if x[0] is not None and x[1] is not None]
            mean_only=statistics.mean(x[0] for x in paired) if paired else None
            mean_shared=statistics.mean(x[1] for x in paired) if paired else None
            out["by_market"][f"{v}_{c}"][key]={
                "shared_total":all_shared,"baseline_only_total":only,
                "variant_only_total":new, "paired_days":len(paired),
                "baseline_only_day_equal_mean_100ms_bps":mean_only,
                "baseline_shared_day_equal_mean_100ms_bps":mean_shared,
            }
            def fmt(x):
                return f"{x:+.4f}" if x is not None else "N/A"
            lines.append(f"| {test_name} vs {baseline_name} | {all_shared} | {only} "
                         f"| {new} | {fmt(mean_only)} | {fmt(mean_shared)} "
                         f"| {len(paired)}/{len(valid)} |")
        lines.append("")
    lines.extend(["## 解释范围","",
                  "日期为此前已多轮探索的2026年每月1日，不能称样本外；历史行情只用于官方HBT及只读成交后诊断。",
                  "保留独有与共有成交对应的样本数量/尾部P5、P10及后续500ms指标于JSON。",
                  "不同策略改变了库存、队列、报价和未来成交路径；本报告仅描述同刻成交机会。",
                  "逆向选择比较必须结合独立交易日净PnL、费率、订单有效撤单ACK和等成交量无方向对照。"])
    return out,"\n".join(lines)+"\n"


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--annotated",required=True)
    p.add_argument("--out",required=True)
    a=p.parse_args()
    root=Path(a.annotated)
    cases=sorted(d for d in root.iterdir() if d.is_dir())
    if not cases:
        raise RuntimeError("Missing annotated cases")
    reports=[audit_case(x) for x in cases]
    result,report=aggregate(reports)
    out=Path(a.out)
    out.mkdir(parents=True,exist_ok=True)
    (out/"summary.json").write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding="utf-8")
    (out/"report.md").write_text(report,encoding="utf-8")
    print(report)


if __name__=="__main__":
    main()
