"""Day-equal aggregation of conditional BBO price impulse origin results."""
import argparse
import json
import statistics
from pathlib import Path

CLASSES = ("binance_price_change_source", "okx_price_change_source")
HORIZONS = ("5", "10", "25", "50", "100")
COINS = ("BTC", "SOL")


def summarize(reports):
    r=sorted(reports,key=lambda x:(x["coin"],x["day"]))
    seen=set()
    for item in r:
        key=(item["coin"],item["day"])
        if key in seen:
            raise ValueError("Duplicate origin report")
        seen.add(key)
        if item["delay_ms"] != 2.5:
            raise ValueError("Only homogeneous 2.5ms results allowed")
    out={"source_cases":len(r),"daily_records":r,"by_coin":{}}
    lines=["# Binance BBO变动 vs OKX BBO变动：同向gap的条件价格响应",
           "", "**额外Binance可见延迟：2.5ms。** 每日UTC00:10–01:00，使用原生BBO；"
           "从有符号异常价差方向定义未来目标OKX中间价移动（bps）。",
           "两类是不同来源事件，不等于严格因果识别或同等流动性/成交条件。",
           "每个来源每个20ms桶只保留首个有效事件，尽量降低密集行情的重复计数影响。",
           "**全部日期此前已用于探索，绝非新增样本外；本实验没有交易、手续费或PnL。**",""]
    for coin in COINS:
        items=[v for v in r if v["coin"]==coin]
        lines += [f"## {coin}: {len(items)}/6日期", "",
                  "| 日期 | Binance事件筛选后 | OKX事件筛选后 |",
                  "|---|---:|---:|"]
        for item in items:
            a=item["classes"]
            lines.append(f"| {item['day']} | {a[CLASSES[0]]['eligible_events_after_20ms_dedup']:,} "
                         f"| {a[CLASSES[1]]['eligible_events_after_20ms_dedup']:,} |")
        lines += ["","| 未来OKX期限 | Binance触发：朝gap方向bps | OKX触发：朝gap方向bps | 日等权差异 | 双侧有效天数 | 非零未来变化样本(B/O) |",
                  "|---|---:|---:|---:|---:|---:|"]
        stats={}
        for horizon in HORIZONS:
            pairs=[]
            n0=n1=0
            for day in items:
                a=day["classes"]
                x=a[CLASSES[0]]["future_okx_toward_binance"][horizon]
                y=a[CLASSES[1]]["future_okx_toward_binance"][horizon]
                if (x["mean_toward_binance_bps_including_zeros"] is None or
                        y["mean_toward_binance_bps_including_zeros"] is None):
                    continue
                pairs.append((x["mean_toward_binance_bps_including_zeros"],
                              y["mean_toward_binance_bps_including_zeros"]))
                n0+=x["nonzero_future_move_events"]
                n1+=y["nonzero_future_move_events"]
            if pairs:
                b=statistics.mean(x[0] for x in pairs)
                o=statistics.mean(x[1] for x in pairs)
                delta=statistics.mean(x[0]-x[1] for x in pairs)
                lines.append(f"| {horizon}ms | {b:+.6f} | {o:+.6f} | {delta:+.6f} "
                             f"| {len(pairs)}/{len(items)} | {n0:,}/{n1:,} |")
            else:
                b=o=delta=None
                lines.append(f"| {horizon}ms | N/A | N/A | N/A | 0/{len(items)} | 0/0 |")
            stats[horizon]={"binance_mean_bps":b,"okx_mean_bps":o,"day_mean_delta_bps":delta,
                            "paired_days":len(pairs),"nonzero_move_events_binance":n0,
                            "nonzero_move_events_okx":n1}
        out["by_coin"][coin]=stats
        lines+=["", "注意：两组条件事件选择机制不同，均值差异不可直接解释为Binance因果领先。"]
    lines+=["","## 当前研究边界","",
             "- 原生BBO的中间价不等于正式GLFT hftbacktest融合L2/trades后的盘口价格。",
             "- 只识别最后触发价格变化的交易所消息源；局部市场冲击也可能先于Binance数据接收。",
             "- 标准化事件间隔20ms为预先固定的去重规则，未用收益或正确率调参。",
             "- 未来5–100ms有许多零变化时点；需要披露非零事件数并按天检验稳健性。",
             "- 单次数据来源、时间戳和2.5ms额外延迟不能替代实盘可见性验证。",
             "- 此次纯预测诊断不能说明实际maker成交是否被保护；正式回测独立开展。",""]
    return out,"\n".join(lines)


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--source",required=True)
    parser.add_argument("--out",required=True)
    args=parser.parse_args()
    files=sorted(Path(args.source).rglob("*_2p5ms.json"))
    if not files:
        raise RuntimeError("No event origin reports")
    reports=[json.loads(p.read_text(encoding="utf-8")) for p in files]
    out,md=summarize(reports)
    if out["source_cases"] != 12:
        raise ValueError("Missing any of the 12 preregistered date-market reports")
    path=Path(args.out)
    path.mkdir(parents=True,exist_ok=True)
    (path/"summary.json").write_text(json.dumps(out,ensure_ascii=False,indent=2),encoding="utf-8")
    (path/"report.md").write_text(md,encoding="utf-8")
    print(md)


if __name__=="__main__":
    main()
