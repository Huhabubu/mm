"""Compare 1.645σ vs frozen 1.96σ HBT results, with 0-fee and VIP0 maker fee.

The previous 1.96σ official run had Maker=-0.0001 (rebate); first remove
that fee effect. New 1.645σ official HBT runs charge Maker=+0.0002.
Both models are Post-Only; the maker-fee scenario has no Taker fills.
"""
import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path

DAYS = tuple(f"2026-{m:02d}-01" for m in range(6, 12))
MODELS = ("glft",) + tuple(
    f"{mode}_{str(beta).replace('.', 'p')}"
    for mode in ("both", "defensive") for beta in (0.25, 0.5, 0.75, 1.0)
)


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--old", required=True, help="Frozen 1.96sigma per_day_model.csv")
    p.add_argument("--new", required=True, help="Directory with new 1430m JSONs")
    p.add_argument("--out", required=True)
    opts=p.parse_args()
    with open(opts.old, newline="",encoding="utf-8") as f:
        old={(r["date"],r["model"]):r for r in csv.DictReader(f)}
    if len(old)!=54:
        raise ValueError(f"Expected 54 old model/day pairs, got {len(old)}")
    fresh={}
    for day in DAYS:
        matches=list(Path(opts.new).rglob(day+"_1430m.json"))
        if len(matches)!=1:
            raise ValueError(f"Must have one full-day report for {day}, found {len(matches)}")
        r=json.loads(matches[0].read_text(encoding="utf-8"))
        if r.get("day")!=day or r.get("status")!="complete" or r.get("minutes_traded")!=1430:
            raise ValueError(f"Incomplete new report for {day}")
        if r.get("experimental_trigger_sigma_multiple")!=1.645:
            raise ValueError(f"Threshold unverified on {day}")
        if abs(r["scenario"]["maker_fee_rate"]-0.0002)>1e-12:
            raise ValueError(f"Fee rate is not VIP0 charge on {day}")
        if r.get("fee_policy")!="VIP0_assumed_maker_plus_0.020pct_no_rebate":
            raise ValueError(f"Fee scenario metadata missing on {day}")
        if set(r["variants"])!=set(MODELS):
            raise ValueError(f"Expected nine models for {day}")
        fresh[day]=r
    records=[]
    for day in DAYS:
        for name in MODELS:
            prior=old.get((day,name))
            if prior is None:
                raise ValueError(f"Missing old pair {day} {name}")
            b=fresh[day]["variants"][name]
            curr=b["trading"]
            old_net=float(prior["net_after_fees_usdt"])
            old_signed_fee=float(prior["signed_fee_usdt"])
            if old_signed_fee>=0.0:
                raise ValueError(f"Old Maker rebate input changed {day} {name}")
            new_net=float(curr["end_net_marked_equity_usdt"])
            new_fee=float(curr["fee_usdt"])
            if new_fee<0:
                raise ValueError(f"New simulation contains rebate {day} {name}")
            old_zero=old_net+old_signed_fee
            old_vip=old_net+3*old_signed_fee  # -0.01% rebate -> +0.02% charge
            new_zero=new_net+new_fee
            if name=="glft":
                if abs(old_zero-new_zero)>0.00001 or int(prior["fills"])!=int(curr["engine_fills"]):
                    raise ValueError(f"β0 replay failed to reproduce baseline {day}: "
                                     f"zero-fee old={old_zero} new={new_zero}")
                if abs(old_vip-new_net)>0.00001:
                    raise ValueError(f"β0 positive Maker fee failed reproduction {day}")
            records.append({
                "date":day,"model":name,
                "old_196_zero_fee_usdt":old_zero,
                "new_1645_zero_fee_usdt":new_zero,
                "delta_zero_fee_usdt":new_zero-old_zero,
                "old_196_vip0_net_usdt":old_vip,
                "new_1645_vip0_net_usdt":new_net,
                "delta_vip0_usdt":new_net-old_vip,
                "old_fills":int(prior["fills"]),
                "new_fills":int(curr["engine_fills"]),
                "old_cancels":int(prior["cancel_requests"]),
                "new_cancels":int(curr["cancel_requests"]),
                "old_activations":int(prior["binance_origin_activations"]),
                "new_activations":int(b["binance_origin_filter"]["binance_origin_activations"]),
                "new_maker_fees_usdt":new_fee
            })
    dest=Path(opts.out)
    dest.mkdir(parents=True,exist_ok=True)
    fieldnames=list(records[0])
    with (dest/"paired_54_models.csv").open("w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=fieldnames)
        w.writeheader()
        w.writerows(records)
    stats={}
    for name in MODELS:
        batch=[r for r in records if r["model"]==name]
        mean=lambda key:sum(r[key] for r in batch)/len(batch)
        stats[name]={
            "days":len(batch),
            "old_mean_zero_fee":mean("old_196_zero_fee_usdt"),
            "new_mean_zero_fee":mean("new_1645_zero_fee_usdt"),
            "mean_delta_zero_fee":mean("delta_zero_fee_usdt"),
            "old_mean_vip0":mean("old_196_vip0_net_usdt"),
            "new_mean_vip0":mean("new_1645_vip0_net_usdt"),
            "mean_delta_vip0":mean("delta_vip0_usdt"),
            "days_improved_zero_fee":sum(r["delta_zero_fee_usdt"]>1e-6 for r in batch),
            "days_improved_vip0":sum(r["delta_vip0_usdt"]>1e-6 for r in batch),
            "mean_old_fills":mean("old_fills"),
            "mean_new_fills":mean("new_fills"),
            "mean_old_cancels":mean("old_cancels"),
            "mean_new_cancels":mean("new_cancels"),
            "mean_old_activations":mean("old_activations"),
            "mean_new_activations":mean("new_activations")
        }
    (dest/"summary.json").write_text(json.dumps({
        "status":"complete","days":DAYS,
        "thresholds":[1.96,1.645],
        "note":"Old 1.96sigma Maker rebate removed; new 1.645sigma HBT charged 0.02% Maker. Funding and forced close excluded.",
        "baseline_reproduced_6_days":True,
        "results":stats
    },indent=2,ensure_ascii=False)+"\n",encoding="utf-8")
    lines=[
        "# 1.645σ versus 1.96σ：Gate Binance 跨所指导做市六日敏感性对比",
        "",
        "每个历史日 UTC 00:10–24:00 独立计算；`β=0` 的零费净权益、正手续费净权益及成交次数在新旧回放间全部复现。",
        "这是已反复观察历史日的探索性敏感性分析，不能被表述为样本外结果。",
        "",
        "| 模型 | 原 1.96σ 零费 | 新 1.645σ 零费 | 零费平均增量 | 原 1.96σ VIP0 | 新 1.645σ VIP0 | VIP0 平均增量 |",
        "|---|---:|---:|---:|---:|---:|---:|"
    ]
    for name in MODELS:
        x=stats[name]
        lines.append(
            f"| {name} | {x['old_mean_zero_fee']:+.3f} | {x['new_mean_zero_fee']:+.3f} | "
            f"{x['mean_delta_zero_fee']:+.3f} | {x['old_mean_vip0']:+.3f} | "
            f"{x['new_mean_vip0']:+.3f} | {x['mean_delta_vip0']:+.3f} |"
        )
    lines += ["",
        "费用：0 手续费为完全不计撮合成交费；VIP0 情景为 Maker +0.02% 收费、无返佣（Post-Only）。",
        "收益是按 Gate 中间价盯市、还未纳入资金费率及期末主动平仓成本的权益变化。",
        "",
        "## 每日 1.645σ 对比 1.96σ 的净收益差（USDT）",
        "",
        "| 日期 | 双边 β=1：零费增量 | 双边 β=1：VIP0 增量 | 单边 β=1：零费增量 | 单边 β=1：VIP0 增量 |",
        "|---|---:|---:|---:|---:|"
    ]
    for day in DAYS:
        b=next(r for r in records if r["date"]==day and r["model"]=="both_1p0")
        s=next(r for r in records if r["date"]==day and r["model"]=="defensive_1p0")
        lines.append(f"| {day} | {b['delta_zero_fee_usdt']:+.3f} | {b['delta_vip0_usdt']:+.3f} | {s['delta_zero_fee_usdt']:+.3f} | {s['delta_vip0_usdt']:+.3f} |")
    lines += ["",
        "信号多寡、成交减少并不能直接证明避开逆向选择；具体成交后的 100/500ms markout 仍须单独检验。"]
    (dest/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")
    print("PAIRED_THRESHOLD_COMPARISON_OK")
    print(json.dumps(stats,ensure_ascii=False))
if __name__=="__main__":
    main()
