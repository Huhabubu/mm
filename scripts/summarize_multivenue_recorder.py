"""Aggregate official Recorder-backed multi-market GLFT paired 50min reports.

Reads only summary JSON plus checks its recorder NPZ files exist. Never
treats failed/uncalibrated strategies as zero profit. Reports within-market
daily-equal weighted means and a balanced common-date panel.
"""
import argparse
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path

EXPECTED = ("glft","glft_binance","glft_local_peak","glft_binance_local_peak")
VENUES = ("okx","bitget")
COINS = ("BTC","SOL")


def analyse(root):
    items = {}
    statuses = defaultdict(list)
    for path in root.rglob("*_50m.json"):
        try:
            r=json.loads(path.read_text(encoding="utf-8"))
        except (ValueError,OSError):
            continue
        m=r.get("market")
        if not isinstance(m,dict) or "exchange" not in m:
            continue
        venue="okx" if m["exchange"]=="okex-swap" else "bitget" if m["exchange"]=="bitget-futures" else None
        coin=m.get("coin")
        if venue is None or coin not in COINS:
            continue
        day=r.get("day")
        key=(venue,coin,day)
        if key in items:
            raise ValueError(f"Duplicate venue coin day summary {key}")
        items[key]=r
        statuses[(venue,coin)].append({"day":day,"status":r.get("status"),
                                       "diagnostic":r.get("calibration_diagnostic")})
        if r.get("status")=="complete":
            if set(r["variants"])!=set(EXPECTED):
                raise ValueError(f"Missing variant in {key}")
            for name,v in r["variants"].items():
                rec=v.get("recorder")
                if not rec or int(rec["samples"])<29000:
                    raise ValueError(f"Missing 100ms official Recorder in {key} {name}")
                for filename in (rec["file"],rec["fill_file"]):
                    # Workflow uploads strip the "reports/" path prefix.
                    relative=Path(filename)
                    if relative.parts and relative.parts[0]=="reports":
                        relative=Path(*relative.parts[1:])
                    if not (path.parent / relative).exists():
                        raise ValueError(f"Recorder or fill NPZ missing: {path.parent / relative}")
    models=[]
    per_day=[]
    coverage={}
    for venue in VENUES:
        for coin in COINS:
            group=(venue,coin)
            rs=[r for (v,c,d),r in items.items() if (v,c)==group and r.get("status")=="complete"]
            coverage[f"{venue}_{coin}"]={
                "date_count":len([k for k in items if k[:2]==group]),
                "complete_count":len(rs),
                "statuses":sorted(statuses[group],key=lambda x:x["day"])}
            for r in sorted(rs,key=lambda x:x["day"]):
                day=r["day"]
                models_pnl={}
                for name in EXPECTED:
                    t=r["variants"][name]["trading"]
                    net=t["end_net_marked_equity_usdt"]
                    fee=t["fee_usdt"]
                    per_day.append({"venue":venue,"coin":coin,"day":day,
                                    "model":name,"gross_usdt":net+fee,
                                    "fees_usdt":fee,"net_usdt":net,
                                    "fills":t["engine_fills"],"submits":t["submissions"],
                                    "cancels":t["cancel_requests"],
                                    "max_position":t["max_abs_position_contracts"]})
                    models_pnl[name]=net
                assert all(k in models_pnl for k in EXPECTED)
            for name in EXPECTED:
                rows=[r for r in per_day if r["venue"]==venue and
                      r["coin"]==coin and r["model"]==name]
                if not rows:
                    continue
                models.append({"venue":venue,"coin":coin,"model":name,
                               "valid_dates":len(rows),
                               "gross_usdt_mean":statistics.mean(x["gross_usdt"] for x in rows),
                               "fees_usdt_mean":statistics.mean(x["fees_usdt"] for x in rows),
                               "net_usdt_mean":statistics.mean(x["net_usdt"] for x in rows),
                               "fills_mean":statistics.mean(x["fills"] for x in rows),
                               "submits_mean":statistics.mean(x["submits"] for x in rows),
                               "cancels_mean":statistics.mean(x["cancels"] for x in rows)})
    complete_by_venue_coin={
        g:{d for (v,c,d),r in items.items() if (v,c)==g and r.get("status")=="complete"}
        for g in ((v,c) for v in VENUES for c in COINS)
    }
    common=set.intersection(*complete_by_venue_coin.values()) if complete_by_venue_coin else set()
    matched=[]
    for venue in VENUES:
        for coin in COINS:
            for day in sorted(common):
                r=items[(venue,coin,day)]
                t=r["variants"]
                net=lambda model:t[model]["trading"]["end_net_marked_equity_usdt"]
                matched.append({"venue":venue,"coin":coin,"day":day,
                    "binance_net_delta":net("glft_binance")-net("glft"),
                    "local_peak_net_delta":net("glft_local_peak")-net("glft"),
                    "binance_with_peak_net_delta":net("glft_binance_local_peak")-net("glft_local_peak")})
    return {"expected_market_cases":len(VENUES)*len(COINS),
            "found_case_dates":len(items),
            "coverage":coverage,
            "common_complete_dates":sorted(common),
            "model_means":models,"per_day":per_day,
            "balanced_daily_differences":matched,
            "interpretation":"Exploratory 50min after 10min warmup; previously explored first-of-month days; "
            "HBT official execution, recorder, scenario fees; no funding or final liquidation."}


def render_markdown(data):
    lines=["# 四市场 × 四模型 GLFT：六个月首日前50分钟官方回测汇总","",
           "**单位：USDT/50分钟。** 每日期前10分钟仅校准，之后50分钟交易；"
           "每个可估计日期等权平均。毛收益为净盯市权益加回模拟手续费。","",
           "## 样本覆盖及可估计性","",
           "| 市场 | 完成 / 总日期 | 未成功日期 |","|---|---:|---|"]
    for venue in VENUES:
        for coin in COINS:
            c=data["coverage"][f"{venue}_{coin}"]
            errs=[x["day"]+": "+x["status"] for x in c["statuses"]
                  if x["status"]!="complete"]
            lines.append(f"| {venue} {coin} | {c['complete_count']} / {c['date_count']} | "
                         f"{'; '.join(errs) or '—'} |")
    lines+=["","## 各市场四模型的可估计日期平均值","",
            "| 市场 | 模型 | 有效日期 | 毛收益 | 手续费 | 净收益 | 成交 | 撤单 |",
            "|---|---|---:|---:|---:|---:|---:|---:|"]
    for x in data["model_means"]:
        lines.append(f"| {x['venue']} {x['coin']} | {x['model']} | {x['valid_dates']} "
                     f"| {x['gross_usdt_mean']:+.4f} | {x['fees_usdt_mean']:.4f} "
                     f"| {x['net_usdt_mean']:+.4f} | {x['fills_mean']:.1f} "
                     f"| {x['cancels_mean']:.1f} |")
    lines+=["","## 同日期内β1−β0净收益改善","",
            "| 市场 | 日期 | β1−GLFT | β1+峰−GLFT+峰 |",
            "|---|---|---:|---:|"]
    per=defaultdict(dict)
    for x in data["per_day"]:
        per[(x["venue"],x["coin"],x["day"])][x["model"]]=x["net_usdt"]
    for key,mods in sorted(per.items()):
        v,c,d=key
        lines.append(f"| {v} {c} | {d} | "
                     f"{mods['glft_binance']-mods['glft']:+.4f} | "
                     f"{mods['glft_binance_local_peak']-mods['glft_local_peak']:+.4f} |")
    lines+=["","## 四市场共同具有完整四模型记录的日期","",
            ", ".join(data["common_complete_dates"]) or "无", "",
            "## 方法限制","",
            "- 全部为此前探索过的2026年各月首日，50分钟片段不等于完整交易日，也不是独立样本外。",
            "- GLFT 无法在前10分钟估计时，结果标记为 unavailable，不能按0收益并入均值。",
            "- 合约面值、价格精度、数量单位已按各所当前公开规格转换；历史变更尚未完全核验。",
            "- 跨市场订单采用约80 USDT名义金额、相同 USDT 财富风险厌恶与20倍订单量库存上限；"
            "每日数量依据预热中间价确定，因此与旧BTC-OKX固定0.1张版本可能不同。",
            "- 局部极大值搜索使用50个各自交易所原生价格档；各币种对应的bps距离不同，"
            "不能据此直接断言某所流动性大单更加有效。",
            "- 真实撤单确认、限频、交易费等级、资金费率及收盘实际平仓成本仍需核查。",
            "- Recorder按100ms记录官方状态（时间、价格、仓位、余额、费用、成交量），另存官方成交时刻及执行价格/数量；"
            "均由GitHub Actions作为90天保留的NPZ附件存储。"]
    return "\n".join(lines)+"\n"


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--source",default="gathered")
    p.add_argument("--out",default="reports/multivenue_sixdate_summary")
    a=p.parse_args()
    data=analyse(Path(a.source))
    out=Path(a.out)
    out.mkdir(parents=True,exist_ok=True)
    (out/"summary.json").write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    (out/"summary.md").write_text(render_markdown(data),encoding="utf-8")
    if data["per_day"]:
        with (out/"per_day.csv").open("w",newline="",encoding="utf-8") as f:
            writer=csv.DictWriter(f,fieldnames=list(data["per_day"][0]))
            writer.writeheader()
            writer.writerows(data["per_day"])
    print("SUMMARY_COVERAGE",data["coverage"])
    print("COMMON_COMPLETE_DATES",data["common_complete_dates"])
    for row in data["model_means"]:
        print("MEAN",row["venue"],row["coin"],row["model"],
              row["valid_dates"],"NET",row["net_usdt_mean"],
              "GROSS",row["gross_usdt_mean"],"FEES",row["fees_usdt_mean"])
    print("RESULT_FILE",out/"summary.md")


if __name__=="__main__":
    main()
