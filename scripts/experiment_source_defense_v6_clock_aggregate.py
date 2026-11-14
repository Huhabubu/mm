"""Compact V6 clock/microprice aggregate from eight read-only Tardis cases."""
import argparse
import json
from pathlib import Path
from experiment_source_defense_v6_clock_microprice import DATES,COINS


def aggregate(root):
    root=Path(root)
    items=[]
    for path in root.rglob("clock_microprice_case.json"):
        obj=json.loads(path.read_text(encoding="utf8"))
        for row in obj["targets"]:
            items.append(row)
    expected=sum(2 if coin=="1000PEPE" else 1 for coin in COINS)*len(DATES)
    if len(items)!=expected:
        raise ValueError(f"Need {expected} clock market/target observations, got {len(items)}")
    return {
        "cases":sorted(items,key=lambda x:(x["date"],x["coin"],x["source_venue"])),
        "days":DATES,"coins":COINS,
        "data_channel":"raw Tardis native book_ticker only",
        "causality":"no network propagation can be inferred from venue timestamp vs collector timestamp without clock sync, routing, publication latencies",
        "event_alignment":"next same-direction native target BBO mid jump within 100ms; nonunique, frequency biased",
        "prejump_scope":"June BTC/SOL only; feature before selected Binance midpoint jump; no trading execution",
        "no_strategy_parameter_changes":True}


def markdown(result):
    lines=[
        "# V6：BTC/SOL/DOGE/PEPE 双所Tardis采集时钟偏差与预跳Microprice",
        "",
        "单位：毫秒。Tardis local_timestamp−timestamp 的P10、P50、均值和P90；"
        "两个交易所的timestamp来源不同，不能将差值解释为真实网络单程延迟。",
        "",
        "|日期|币种|目标所|BN P10/P50/均值/P90|目标所 P10/P50/均值/P90|BN-目标中位差|Local同向下一跳中位ms|Exchange同向下一跳中位ms|",
        "|---|---|---|---|---|---:|---:|---:|"]
    def g(d):
        return "/".join(f"{d[k]:+.3f}" for k in ("p10_ms","median_ms","mean_ms","p90_ms"))
    def fmt(x):
        return f"{x:+.3f}" if x is not None else "N/A"
    for r in result["cases"]:
        a=r["event_link_by_clock"]
        lines.append(f"|{r['date']}|{r['coin']}|{r['source_venue']}|"
                     f"{g(r['binance_local_minus_exchange'])}|"
                     f"{g(r['target_local_minus_exchange'])}|"
                     f"{r['binance_vs_target_median_ms_difference']:+.3f}|"
                     f"{fmt(a['local']['median_ms'])}|"
                     f"{fmt(a['exchange']['median_ms'])}|")
    lines+=["","## 双时钟最近同方向跳变的有符号相对时差","",
            "定义Δ=t_target−t_Binance，正值表示按所选时钟Binance行先出现，"
            "负值表示目标所行先出现；同方向最近事件可被重复配对，"
            "交易所时间戳未校时且collector时差存在，均非真实传播因果。",
            "",
            "|日期|币种|目标所|Local Δ P10/P50/均值/P90 ms|Local BN先出现比例|Exchange Δ P10/P50/均值/P90 ms|Exchange BN先出现比例|",
            "|---|---|---|---|---:|---|---:|"]
    for r in result["cases"]:
        q=r["signed_nearest_event_lag_by_clock"]
        lo=q["local"];ex=q["exchange"]
        lines.append(f"|{r['date']}|{r['coin']}|{r['source_venue']}|"
                     f"{g(lo)}|{lo['binance_before_target_fraction_of_matched']:.1%}|"
                     f"{g(ex)}|{ex['binance_before_target_fraction_of_matched']:.1%}|")
    lines+=["","## June BTC / SOL：Binance mid跳变前5–50ms的Microprice代理","",
            "Microprice用Binance一档买卖挂单量对bid/ask加权，"
            "预跳偏离符号与随后Binance BBO跳变符号对比；仅条件化于未来确实发生跳变的事件，"
            "同时报告随后100ms目标所按跳变方向的均值响应。不能据此宣称事前可盈利预测。","",
            "|币种|目标所|提前ms|事件数|非零Microprice方向命中比|同向100ms目标响应bps|反向100ms目标响应bps|",
            "|---|---|---:|---:|---:|---:|---:|"]
    for r in result["cases"]:
        if "binance_prejump_microprice" not in r:continue
        for lag,q in r["binance_prejump_microprice"].items():
            h=q["aligned_fraction_excluding_zeros"]
            prefix=(f"|{r['coin']}|{r['source_venue']}|{lag}|"
                    f"{q['sample_count']}|{h:.2%}|" if h is not None else
                    f"|{r['coin']}|{r['source_venue']}|{lag}|{q['sample_count']}|N/A|")
            lines.append(prefix+
                f"{fmt(q['100ms_target_signed_markout_mean_when_aligned_bps'])}|"
                f"{fmt(q['100ms_target_signed_markout_mean_when_opposed_bps'])}|")
    lines+=["","## 解释边界","",
            "采集本地时间差同时叠加交易所内部行情发布、交易所时间戳偏差、"
            "网络转发与Tardis本地采集延迟；它只支持记录时钟偏差审计。"
            "本报告的相同方向跳变按下一次事件非唯一配对，可能重复使用目标BBO事件。"
            "Microprice在跳变前已可见，但事件样本由未来Binance跳变筛选，"
            "需在所有时间点与无跳变时间点复核、以及增加真正样本外日期才能检验报警经济价值。"]
    return "\n".join(lines)+"\n"


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--source",required=True);p.add_argument("--out",required=True)
    a=p.parse_args()
    d=aggregate(a.source)
    target=Path(a.out);target.mkdir(parents=True,exist_ok=True)
    (target/"clock_microprice_summary.json").write_text(
        json.dumps(d,ensure_ascii=False,indent=2,allow_nan=False),encoding="utf8")
    m=markdown(d)
    (target/"clock_microprice_summary.md").write_text(m,encoding="utf8")
    print(m)


if __name__=="__main__":
    main()
