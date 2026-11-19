"""Stage 4A Bitget-only multiasset data gate: source-linked 4x6-day report.

Read real full-day per-symbol Tardis CSV audit JSONs, not simulated prices.
BTC source: pinned historical run 44459481767 (six dates, unchanged).
ETH/SOL/DOGE source: pinned dedicated run 44552849015 (18 dates).
BTC October timing source: the same second run, independent raw quote reread.
Never implicitly infer missing files, never give permission to launch HBT.
"""
import argparse
from collections import Counter
import csv
import hashlib
import json
from pathlib import Path

SYMBOLS=("BTCUSDT","ETHUSDT","SOLUSDT","DOGEUSDT")
DAYS=tuple(f"2026-{i:02d}-01" for i in range(6,12))
EXPECTED={(sym,day) for sym in SYMBOLS for day in DAYS}
SOURCE_RUNS={"BTCUSDT":44459481767,"ETHUSDT":44552849015,
             "SOLUSDT":44552849015,"DOGEUSDT":44552849015}
FEEDS=("trades","incremental_book_L2","quotes","binance_book_ticker")

def evidence(path, symbol, day):
    marker=(f"cross-4a-day-bitget-futures-{day}" if symbol=="BTCUSDT"
            else f"bitget-4a-{symbol}-{day}")
    name=(f"4a_bitget-futures_{day}.json" if symbol=="BTCUSDT"
          else f"4a_bitget-futures_{symbol}_{day}.json")
    if marker not in path.parts or path.name!=name:
        raise ValueError(f"Untrusted source folder/name for {symbol} {day}: {path}")
    return {"run_id":SOURCE_RUNS[symbol],
            "run_url":f"https://github.com/Huhabubu/mm/actions/runs/{SOURCE_RUNS[symbol]}",
            "artifact_name":marker,"filename":name,
            "sha256":hashlib.sha256(path.read_bytes()).hexdigest()}

def load_cases(root):
    found={}
    for p in sorted(Path(root).rglob("4a_bitget-futures*.json")):
        doc=json.loads(p.read_text(encoding="utf-8"))
        key=(doc.get("symbol"),doc.get("day"))
        if key not in EXPECTED:
            raise ValueError(f"Unexpected audit artifact {key}: {p}")
        if doc.get("venue")!="bitget-futures":
            raise ValueError(f"Not a Bitget audit: {p}")
        if key in found:
            raise ValueError(f"Duplicate actual data {key}")
        proof=evidence(p,*key)
        if not isinstance(doc.get("feeds"),dict):
            raise ValueError(f"Missing per-stream audit evidence: {p}")
        found[key]=(doc,proof)
    if not found:
        raise ValueError(f"No real Bitget CSV audit JSONs found in {root}")
    return found

def read_metric(data, kind, attr):
    return data.get("feeds",{}).get(kind,{}).get(attr)

def lag_stat(data, kind, item):
    return data.get("feeds",{}).get(kind,{}).get("lag",{}).get(item)

def row_for(symbol,day,found):
    base={
        "symbol":symbol,"utc_day":day,
        "source_run_id":None,"source_run_url":None,"source_artifact":None,
        "source_json_sha256":None,"status":"missing",
        "all_four_feeds_present":False,"all_four_feeds_passed":False,
        "historical_contract_date_effective_verified":False,
        "candidate_tick_usdt":None,"candidate_min_order_qty_base":None,
        "candidate_order_qty_step_base":None,"contract_multiplier_base_per_unit":None,
        "candidate_qty_unit":None,
    }
    for kind in FEEDS:
        for x in ("rows","max_local_gap_ms","gaps_over_1s","gaps_over_3s",
                  "gaps_over_10s","non_grid_prices","blocking_checks"):
            base[f"{kind}_{x}"]=None
        for quant in ("p50_ms","p95_ms","p99_ms"):
            base[f"{kind}_lag_{quant}"]=None
    for x in ("snapshot_groups","reconnect_resnapshots",
              "initial_snapshot_bid_levels","initial_snapshot_ask_levels",
              "incremental_rows","snapshot_rows",
              "sampled_reconstructed_book_crosses",
              "l2_unknown_before_initial_snapshot"):
        base[f"l2_{x}"]=None
    for x in ("observed_qty_min_positive","amount_not_on_proposed_order_step"):
        base[f"trades_{x}"]=None
    for q in ("p50","p95","p99"):
        base[f"bitget_minus_binance_{q}_ms"]=None
    base["lag_tail_review"]=None
    if (symbol,day) not in found:
        return base
    doc,proof=found[(symbol,day)]
    base.update({"status":doc.get("status","unknown"),
                 "source_run_id":proof["run_id"],
                 "source_run_url":proof["run_url"],
                 "source_artifact":proof["artifact_name"],
                 "source_json_sha256":proof["sha256"]})
    feeds=doc["feeds"]
    base["all_four_feeds_present"]=all(feeds.get(k,{}).get("rows",0)>0 for k in FEEDS)
    base["all_four_feeds_passed"]=all(feeds.get(k,{}).get("normalized_gate_status")=="passed" for k in FEEDS)
    if "normalized_feeds_passed" in doc and bool(doc["normalized_feeds_passed"])!=base["all_four_feeds_passed"]:
        raise ValueError(f"Internal per-feed status disagrees with aggregate gate: {symbol} {day}")
    base["historical_contract_date_effective_verified"]=bool(
        doc.get("historical_contract_date_effective_verified",False))
    spec=doc.get("contract_candidate",{})
    for key,old,new in (
        ("candidate_tick_usdt","expected_price_tick_usdt","tick_size_usdt"),
        ("candidate_min_order_qty_base","expected_min_size","min_order_qty_base"),
        ("candidate_order_qty_step_base","expected_order_step","qty_step_candidate_base"),
        ("contract_multiplier_base_per_unit","contract_multiplier_base_btc_per_unit","contract_base_multiplier"),
        ("candidate_qty_unit","min_size_unit","quantity_unit"),
    ):
        base[key]=spec.get(old,spec.get(new))
    for kind in FEEDS:
        v=feeds.get(kind,{})
        for x in ("rows","max_local_gap_ms","gaps_over_1s","gaps_over_3s",
                  "gaps_over_10s","non_grid_prices"):
            base[f"{kind}_{x}"]=v.get(x)
        base[f"{kind}_blocking_checks"]="; ".join(v.get("blocking_checks",[]))
        for quant in ("p50_ms","p95_ms","p99_ms"):
            base[f"{kind}_lag_{quant}"]=v.get("lag",{}).get(quant)
    l2=feeds.get("incremental_book_L2",{})
    for x in ("snapshot_groups","reconnect_resnapshots",
              "initial_snapshot_bid_levels","initial_snapshot_ask_levels",
              "incremental_rows","snapshot_rows",
              "sampled_reconstructed_book_crosses",
              "l2_unknown_before_initial_snapshot"):
        base[f"l2_{x}"]=l2.get(x)
    trades=feeds.get("trades",{})
    for x in ("observed_qty_min_positive","amount_not_on_proposed_order_step"):
        base[f"trades_{x}"]=trades.get(x)
    for q in ("p50","p95","p99"):
        a=base[f"quotes_lag_{q}_ms"]
        b=base[f"binance_book_ticker_lag_{q}_ms"]
        base[f"bitget_minus_binance_{q}_ms"]=(
            round(float(a)-float(b),4)
            if isinstance(a,(int,float)) and isinstance(b,(int,float)) else None)
    p95=base["bitget_minus_binance_p95_ms"]
    p99=base["bitget_minus_binance_p99_ms"]
    base["lag_tail_review"]=bool(
        (p95 is not None and abs(p95)>30) or
        (p99 is not None and abs(p99)>75))
    return base

def load_tail(root):
    found=list(Path(root).rglob("bitget_btc_oct_lag_tail.json"))
    if len(found)!=1:
        raise ValueError(f"Require exactly one real full-day BTC Oct forensic JSON, got {len(found)}")
    p=found[0]
    if "bitget-4a-btc-2026-10-01-lag-tail" not in p.parts:
        raise ValueError(f"BTC tail artifact source not pinned to expected 4A run: {p}")
    j=json.loads(p.read_text(encoding="utf-8"))
    if j.get("date")!="2026-10-01" or not j.get("no_hbt"):
        raise ValueError("BTC Oct timing file is not the approved source")
    if set(j.get("sources",{}))!={"bitget_quotes","binance_book_ticker"}:
        raise ValueError("Missing one of BTC Oct source distributions")
    for source in j["sources"].values():
        if source["hours_with_data"]!=24 or source["observations"]<1:
            raise ValueError("Incomplete BTC Oct hourly source")
        if len(source["hourly"])!=24 or len(source["five_minute"])!=288:
            raise ValueError("Missing BTC Oct hourly or 5-min timeline")
    return j,{"run_id":44552849015,"sha256":hashlib.sha256(p.read_bytes()).hexdigest()}

def report(root,settings):
    found=load_cases(root)
    rows=[row_for(symbol,day,found) for symbol in SYMBOLS for day in DAYS]
    if len(found)>24:
        raise ValueError("Unexpected number of official daily scans")
    spec=json.loads(Path(settings).read_text(encoding="utf-8"))
    if set(spec["contracts"])!=set(SYMBOLS)-{"BTCUSDT"}:
        raise ValueError("Wrong or incomplete Bitget additional-symbol candidate specs")
    tail,tail_source=load_tail(root)
    wins={}
    for symbol in SYMBOLS:
        part=[r for r in rows if r["symbol"]==symbol]
        wins[symbol]={
            "requested_days":6,
            "actual_daily_reports":sum(r["status"]!="missing" for r in part),
            "all_feeds_present_days":sum(r["all_four_feeds_present"] for r in part),
            "normalized_data_passed_days":sum(r["all_four_feeds_passed"] for r in part),
            "blocked_or_missing_days":[r["utc_day"] for r in part if not r["all_four_feeds_passed"]],
            "lag_tail_review_days":[r["utc_day"] for r in part if r["lag_tail_review"]],
            "historical_contract_spec_verified_days":sum(r["historical_contract_date_effective_verified"] for r in part),
        }
    aggregate={
        "status":"all_24_real_days_collected" if len(found)==24 else "incomplete",
        "chapter":"5.3 stage 4A Bitget-only data gate; OKX frozen",
        "no_strategy_or_hbt_executed":True,
        "requested_asset_days":24,
        "actual_asset_day_reports":len(found),
        "normalized_gate_passed_asset_days":sum(r["all_four_feeds_passed"] for r in rows),
        "counts_by_symbol":wins,
        "source_run_ids":{"frozen_btc":44459481767,"new_three_symbols_and_oct_forensic":44552849015},
        "sources_by_asset_day":[
            {k:r[k] for k in ("symbol","utc_day","source_run_id","source_artifact","source_json_sha256","status")}
            for r in rows],
        "october_btc_delay_diagnostic":{
            "forensic_summary":tail["diagnosis"],
            "venue_summary":{name:{k:v for k,v in source.items() if k not in ("hourly","five_minute")}
                             for name,source in tail["sources"].items()},
            "source":tail_source,
        },
        "contract_candidates":{
            "BTCUSDT":"reports/chapter5_cross_exchange_4a_20261119/contract_specs_candidates.json (pinned previous 4A)",
            **spec["contracts"]},
        "fees":{"bitget_standard_maker_bps":2,"bitget_standard_taker_bps":6,"research_maker_bps":2,
                "historical_vip_fee_verified":False},
        "limitations":[
            "Tardis normalized L2 snapshot+updates do not contain raw feed sequence or exact disconnect events",
            "Reconnected snapshot groups are proxies for resubscription, not exact disconnect counts",
            "Trade amount may be a partial fill below minimum submitted order; off-step executions are diagnostics, not automatic invalid-order proof",
            "Date-effective exchange min order/lot/tick changes are not independently certified by historical Pro metadata",
            "BBO capture lag is local_timestamp minus venue-specific exchange timestamp, not a pure network transmission latency",
        ],
        "rows":rows,
    }
    return aggregate,tail

def write_csv(path,rows):
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open("w",newline="",encoding="utf-8") as f:
        writer=csv.DictWriter(f,fieldnames=list(rows[0]))
        writer.writeheader();writer.writerows(rows)

def f(v,digits=2):
    return "NA" if v is None else f"{v:.{digits}f}"

def write(out,agg,tail):
    out.mkdir(parents=True,exist_ok=True)
    rows=agg["rows"]
    write_csv(out/"per_day_gate.csv",rows)
    hourly=[]
    five=[]
    for source_key,source in tail["sources"].items():
        for r in source["hourly"]:
            hourly.append({"source":source_key,**r})
        for r in source["five_minute"]:
            five.append({"source":source_key,**r})
    write_csv(out/"btc_oct_hourly_lag.csv",hourly)
    write_csv(out/"btc_oct_5minute_lag.csv",five)
    (out/"summary.json").write_text(json.dumps(agg,ensure_ascii=False,indent=2,allow_nan=False)+"\n",
                                    encoding="utf-8")
    (out/"btc_oct_lag_tail.json").write_text(json.dumps(tail,ensure_ascii=False,indent=2,allow_nan=False)+"\n",
                                           encoding="utf-8")
    lines=[
        "# 论文5.3前置数据门禁：Bitget四币种×六日",
        "",
        f"实际审计资产日：**{agg['actual_asset_day_reports']}/24**；"
        f"归一化CSV通过：**{agg['normalized_gate_passed_asset_days']}/24**。"
        "此前BTC六日归档保持冻结，本轮补充ETH/SOL/DOGE共18日。"
        "**OKX冻结；没有运行HBT/交易策略。**",
        "",
        "## 逐资产日订单簿与采集延迟",
        "",
        "P50/P95/P99单位毫秒，按当日Bitget归一化quotes对同日同币种Binance原生book_ticker分别计算。",
        "最大L2消息间隔按Tardis本地采集时钟；额外快照为可能的重新订阅代理。",
        "",
        "| 币种 | UTC日 | 四源门禁 | L2行数 | 初始Bid/Ask档 | 额外快照 | L2最大间隔ms | Bitget P50/P95/P99 ms | Binance P50/P95/P99 ms | P95相对差ms |",
        "|---|---|---|---:|---|---:|---:|---|---|---:|"
    ]
    for r in rows:
        def parts(source):
            return "/".join(f(r[f"{source}_lag_{q}_ms"]) for q in ("p50","p95","p99"))
        lines.append(f'| {r["symbol"]} | {r["utc_day"]} | {"通过" if r["all_four_feeds_passed"] else "阻断"}'
                     f' | {r["incremental_book_L2_rows"]}'
                     f' | {r["l2_initial_snapshot_bid_levels"]}/{r["l2_initial_snapshot_ask_levels"]}'
                     f' | {r["l2_reconnect_resnapshots"]}'
                     f' | {f(r["incremental_book_L2_max_local_gap_ms"],3)}'
                     f' | {parts("quotes")} | {parts("binance_book_ticker")}'
                     f' | {f(r["bitget_minus_binance_p95_ms"])} |')
    lines.extend(["","## 合约候选与成交量单位交叉检查","",
        "四币种均为USDT线性永续，成交/委托数量按对应基础币种（BTC、ETH、SOL、DOGE）表达；"
        "合约单位面值定义为每1个计量单位对应1个基础币。"
        "这些是HBT适配候选规格，未由Tardis Pro历史合约元数据逐日认证。",
        "",
        "| 币种 | Tick(USDT) | 最小委托(币) | 候选委托数量步长(币) | 实测最小成交量(币)：六日范围 | 价格不合档行数(六日) | 成交量不合候选步长行数(六日) |",
        "|---|---:|---:|---:|---|---:|---:|"])
    for symbol in SYMBOLS:
        z=[r for r in rows if r["symbol"]==symbol]
        v0=z[0]
        mins=[float(r["trades_observed_qty_min_positive"]) for r in z
              if r["trades_observed_qty_min_positive"] is not None]
        nt=sum(r["trades_non_grid_prices"] or 0 for r in z)
        nq=sum(r["trades_amount_not_on_proposed_order_step"] or 0 for r in z)
        lines.append(f'| {symbol} | {v0["candidate_tick_usdt"]}'
                     f' | {v0["candidate_min_order_qty_base"]}'
                     f' | {v0["candidate_order_qty_step_base"]}'
                     f' | {min(mins) if mins else "NA"}—{max(mins) if mins else "NA"}'
                     f' | {nt} | {nq} |')
    lines.extend(["","其中，成交量属于逐笔撮合成交，可能是订单被部分成交的数量；"
                  "即使成交量小于最小下单量，也不能独立推断历史委托违反交易所最小委托规则。",
                  "",
                  "## BTC 2026-10-01 延迟尾部：逐小时与五分钟重检",
                  "",
                  f'**分类（预先编码的规则）：{tail["diagnosis"]["descriptive_tail_classification"]}**。'
                  "按UTC小时比较两种采集源各自的P50/P95/P99以及延迟超过50ms的比例；"
                  "见btc_oct_hourly_lag.csv与btc_oct_5minute_lag.csv。",
                  "",
                  "| 采集源 | 记录数 | 延迟>50ms比例 | P95>50ms的小时数/24 | 每小时>5%消息延迟50ms的小时数/24 | 全日超50ms事件集中在最严重三小时的比例 |",
                  "|---|---:|---:|---:|---:|---:|"])
    for k,v in tail["sources"].items():
        lines.append(f'| {k} | {v["observations"]}'
                     f' | {v["fraction_over_50ms"]*100:.2f}%'
                     f' | {v["hours_with_p95_gt_50ms"]}/24'
                     f' | {v["hours_with_more_than_5pct_lag_gt_50ms"]}/24'
                     f' | {v["top3_hours_share_of_all_gt50ms"]*100:.2f}% |')
    lines.extend(["","### Bitget BTC逐小时尾部明细","",
        "| UTC小时 | Bitget P50/P95/P99 ms | Bitget>50ms占比 | Binance P50/P95/P99 ms |",
        "|---|---|---:|---|"])
    A=tail["sources"]["bitget_quotes"]["hourly"]
    B=tail["sources"]["binance_book_ticker"]["hourly"]
    for a,b in zip(A,B):
        def fmt3(v):
            return "/".join(f(v[q]) for q in ("p50_ms","p95_ms","p99_ms"))
        lines.append(f'| {a["hour_utc"]:02d}:00 | {fmt3(a)}'
                     f' | {100*a["over_50ms_fraction"]:.2f}% | {fmt3(b)} |')
    lines.extend(["","## 数据质量结论与模型约束","",
        "归一化数据通过仅证明在本次免费CSV标准化、采集时间与预设间隔/快照校验下通过，"
        "不能证明原始WS无丢包或撮合交易所内部队列连续可见。",
        "比对的两种BBO源具有不同发布机制/时钟定义，尾部分布差异不能直接解释为地理跨海延迟。",
        "Bitget和Binance数据按同一天、同币种独立计数，任何完整日缺失保留NA而不当作零。",
        "HBT前尚需独立验证可交易历史合约规格、各币种数量单位及其订单成交概率模型。"
    ])
    (out/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--root",type=Path,required=True)
    p.add_argument("--contracts",type=Path,default=Path("configs/bitget_multiasset_contract_specs_4a.json"))
    p.add_argument("--out",type=Path,required=True)
    args=p.parse_args()
    agg,tail=report(args.root,args.contracts)
    write(args.out,agg,tail)
    print("BITGET_MULTI_ASSET_4A",agg["actual_asset_day_reports"],"/24",
          "pass",agg["normalized_gate_passed_asset_days"],flush=True)
    print("BTC_OCT_TAIL",agg["october_btc_delay_diagnostic"]["forensic_summary"],flush=True)
    return 0 if agg["actual_asset_day_reports"]==24 else 3

if __name__=="__main__":
    raise SystemExit(main())
