"""4A aggregation of real normalized free Tardis CSV day audits; no fills."""
import argparse
import csv
import json
from pathlib import Path

DAYS_BITGET=tuple(f"2026-{i:02d}-01" for i in range(6,12))
DAYS_OKX=tuple(f"2026-{i:02d}-01" for i in range(9,12))
EXPECTED={(v,d) for v,ds in (("bitget-futures",DAYS_BITGET),("okex-swap",DAYS_OKX)) for d in ds}
FEEDS=("incremental_book_L2","quotes","trades","binance_book_ticker")

def summary_of(root):
    data={}
    for path in sorted(Path(root).rglob("4a_*.json")):
        doc=json.loads(path.read_text(encoding="utf-8"))
        key=(doc.get("venue"),doc.get("day"))
        if key not in EXPECTED:
            raise ValueError(f"Unexpected venue/date: {key} {path}")
        if key in data:
            raise ValueError(f"Duplicate audit {key}")
        data[key]=doc
    if not data:
        raise ValueError("No actual Tardis daily reports")
    cases=[]
    for key in sorted(EXPECTED):
        doc=data.get(key) or {
            "venue":key[0],"day":key[1],"status":"missing","feeds":{},
            "fatal_error":"No actual CSV audit artifact for expected date"}
        feeds=doc.get("feeds",{})
        row={"venue":key[0],"day":key[1],"status":doc["status"],
             "all_feed_rows_present":all(feeds.get(x,{}).get("rows",0)>0 for x in FEEDS),
             "fatal_error":doc.get("fatal_error",""),
             "normalized_feeds_passed":doc.get("normalized_feeds_passed",False),
             "ready_for_4b":False,
             "contract_history_verified":doc.get("historical_contract_date_effective_verified",False)}
        for feed in FEEDS:
            f=feeds.get(feed,{})
            lag=f.get("lag",{})
            fields={
                "rows":f.get("rows"),"status":f.get("normalized_gate_status"),
                "max_gap_ms":f.get("max_local_gap_ms"),
                "gaps_over_3s":f.get("gaps_over_3s"),
                "gaps_over_10s":f.get("gaps_over_10s"),
                "capture_p50_ms":lag.get("p50_ms"),
                "capture_p95_ms":lag.get("p95_ms"),
                "capture_p99_ms":lag.get("p99_ms"),
                "negative_lag_rows":lag.get("n_negative"),
                "source_url":f.get("source_url"),
                "blockers":"; ".join(f.get("blocking_checks",[])),
            }
            for col,val in fields.items():
                row[f"{feed}_{col}"]=val
            if feed=="incremental_book_L2":
                for col in ("snapshot_groups","reconnect_resnapshots",
                            "initial_snapshot_bid_levels","initial_snapshot_ask_levels",
                            "incremental_rows","snapshot_rows","snapshots_missing_side",
                            "sampled_reconstructed_book_crosses"):
                    row[f"l2_{col}"]=f.get(col)
        compare=doc.get("capture_lag_comparison",{})
        for col in ("p50_delta_ms","p95_delta_ms","p99_delta_ms",
                    "risk_flag_p50_difference_over_20ms",
                    "risk_flag_p99_difference_over_100ms"):
            row[f"target_minus_binance_{col}"]=compare.get(col)
        cases.append(row)
    groups={}
    for venue,dates in (("bitget-futures",DAYS_BITGET),("okex-swap",DAYS_OKX)):
        chosen=[r for r in cases if r["venue"]==venue]
        groups[venue]={
            "target_days":len(dates),
            "complete_feed_days":sum(x["all_feed_rows_present"] for x in chosen),
            "normalized_gate_passed_days":sum(x["normalized_feeds_passed"] for x in chosen),
            "blocked_or_missing_days":[x["day"] for x in chosen if not x["normalized_feeds_passed"]],
            "historical_contract_verified_days":sum(x["contract_history_verified"] for x in chosen),
            "eligible_for_automatic_4b":False,
        }
    return {
        "status":"all_days_audited" if len(data)==len(EXPECTED) else "incomplete",
        "stage":"4A only","requested_days":9,"days_with_json":len(data),
        "no_backtest_performed":True,"venues":groups,
        "source_delay_definition":"Tardis local_timestamp minus exchange timestamp in milliseconds, NOT pure one-way network delay",
        "limitations":[
            "Normalized incremental_book_L2 cannot prove original WebSocket channel or raw sequence continuity",
            "Reconnect resets count snapshot segments, not exact disconnects; raw disconnect markers absent in CSV",
            "Historical contract min/lot/tick/fee exact effective values need separately archived venue metadata",
            "Colocation based on stated Tardis collector/server regions cannot prove actual matching-engine AZ",
        ],
        "cases":cases,
    }

def write_report(out,summary):
    out.mkdir(parents=True,exist_ok=True)
    cases=summary["cases"]
    with (out/"per_day_gate.csv").open("w",newline="",encoding="utf-8") as file:
        writer=csv.DictWriter(file,fieldnames=cases[0].keys())
        writer.writeheader()
        writer.writerows(cases)
    (out/"summary.json").write_text(
        json.dumps(summary,ensure_ascii=False,indent=2,allow_nan=False)+"\n",
        encoding="utf-8")
    lines=[
        "# 第5.3节阶段四4A：跨交易所真实Tardis前置数据门禁",
        "",
        f"已产生审计文件 {summary['days_with_json']}/9。仅审计真实CSV，未执行HBT、成交或策略收益回测。",
        "",
        "Tardis采集延迟 = local_timestamp − timestamp（毫秒），其包含网络、交易所发布节拍与时钟同步误差；不可直接解释为纯网络单程延迟。",
        "",
        "| 交易所 | UTC日期 | 四份CSV | 归一化盘口/时钟门禁 | L2快照组/额外重建组 | L2最大间隔ms | 本所 quotes P50/P95/P99 ms | Binance book_ticker P50/P95/P99 ms | 本所−Binance P50/P95/P99 ms |",
        "|---|---|---|---|---|---:|---|---|---|",
    ]
    def triple(c,feed):
        return "/".join(str(c.get(f"{feed}_capture_{p}_ms") if c.get(
            f"{feed}_capture_{p}_ms") is not None else "NA") for p in ("p50","p95","p99"))
    for c in cases:
        diff="/".join(str(c.get(f"target_minus_binance_{p}_delta_ms") if c.get(
            f"target_minus_binance_{p}_delta_ms") is not None else "NA")
            for p in ("p50","p95","p99"))
        lines.append(
            f'| {c["venue"]} | {c["day"]} | {"完整" if c["all_feed_rows_present"] else "缺失"}'
            f' | {"通过" if c["normalized_feeds_passed"] else "阻断"}'
            f' | {c.get("l2_snapshot_groups")}/{c.get("l2_reconnect_resnapshots")}'
            f' | {c.get("incremental_book_L2_max_gap_ms")}'
            f' | {triple(c,"quotes")} | {triple(c,"binance_book_ticker")} | {diff} |')
    lines.extend(["","## 重建与断线风险","",
        "额外快照组是重建/重订阅的代理，精确WebSocket断线次数不能从免费归一化CSV观察。",
        "",
        "| 交易所 | 日期 | 初始Bid/Ask档数 | 原生归一化增量行 | 额外快照组 | L2超过3秒间隔数 | L2超过10秒间隔数 | 阻断理由 |",
        "|---|---|---|---:|---:|---:|---:|---|"])
    for c in cases:
        why="; ".join(str(c.get(f"{feed}_blockers") or "") for feed in FEEDS)
        why=(why+"; "+str(c.get("fatal_error") or "")).replace("|","/").strip(" ;")[:330]
        lines.append(
            f'| {c["venue"]} | {c["day"]} '
            f'| {c.get("l2_initial_snapshot_bid_levels")}/{c.get("l2_initial_snapshot_ask_levels")}'
            f' | {c.get("l2_incremental_rows")} | {c.get("l2_reconnect_resnapshots")}'
            f' | {c.get("incremental_book_L2_gaps_over_3s")}'
            f' | {c.get("incremental_book_L2_gaps_over_10s")}'
            f' | {why or "无记录的归一化CSV硬异常"} |')
    lines.extend(["","## 归一化数据通过率（与4B许可不同）","",
        "| 交易所 | 预定日 | 四类CSV齐全 | 归一化门禁通过 | 能否自动进入4B |",
        "|---|---:|---:|---:|---|"])
    for venue,g in summary["venues"].items():
        lines.append(f'| {venue} | {g["target_days"]}'
                     f' | {g["complete_feed_days"]}'
                     f' | {g["normalized_gate_passed_days"]}'
                     f' | 暂不许可：历史规格、原生频道和策略HBT适配未全部独立认证 |')
    lines.extend(["",
        "## 研究约束",
        "",
        "- 本报告中的L2格式一致性仅代表归一化CSV的初始快照+增量可重建表现；并非“绝无网络断层”。",
        "- 默认Maker统一2bps；Bitget官方标准Maker 2bps/Taker 6bps，OKX官方例示Maker 2bps/Taker 5bps。",
        "- Bitget BTCUSDT数量通常按BTC数量计量；OKX BTC-USDT-SWAP为合约张数，约0.01 BTC/张。历史规格需要交易所时点证据。",
        "- 如果本所与Binance采集延迟分布显著不同，需分析采集频率/时间戳定义/交易所时钟，不可直接归因跨海网络。",
        "- 4B必须单独实现并验证Bitget与OKX的HBT合约单位、L2适配及成交假设，用户要求4A通过前禁止自动启动。",
        ""])
    (out/"report.md").write_text("\n".join(lines),encoding="utf-8")

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--input",type=Path,required=True)
    parser.add_argument("--out",type=Path,required=True)
    a=parser.parse_args()
    summary=summary_of(a.input)
    write_report(a.out,summary)
    print("CROSS_EXCHANGE_4A_DAYS",summary["days_with_json"],"/9",
          summary["venues"],flush=True)
    return 0 if summary["days_with_json"]==9 else 2

if __name__=="__main__":
    raise SystemExit(main())
