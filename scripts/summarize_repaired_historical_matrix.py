"""Pair the repaired 2.5ms and 25ms six-date multivenue HBT summaries."""
import argparse
import json
from pathlib import Path

VENUES=("okx","bitget")
COINS=("BTC","SOL")
MODELS=("glft","glft_binance","glft_local_peak","glft_binance_local_peak")


def load(path, delay):
    d=json.loads(Path(path).read_text(encoding="utf-8"))
    assert d["signal_delay_ms"]==delay,(path,d["signal_delay_ms"])
    assert d["found_case_dates"]==24,(path,d["found_case_dates"])
    assert len(d["model_means"])==16
    assert len(d["per_day"])==80  # 6 BTC days per venue, 6 Bitget SOL, 2 OKX SOL.
    assert d["common_complete_dates"]==["2026-06-01","2026-10-01"]
    for venue in VENUES:
        for coin in COINS:
            c=d["coverage"][f"{venue}_{coin}"]
            assert c["date_count"]==6
            assert c["complete_count"]==(2 if venue=="okx" and coin=="SOL" else 6)
    return d


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--two-five",required=True)
    p.add_argument("--twenty-five",required=True)
    p.add_argument("--out",required=True)
    a=p.parse_args()
    x=load(a.two_five,2.5)
    y=load(a.twenty_five,25.)
    index=lambda d:{(m["venue"],m["coin"],m["model"]):m for m in d["model_means"]}
    left,right=index(x),index(y)
    assert set(left)==set(right)
    rows=[]
    for venue in VENUES:
        for coin in COINS:
            for model in MODELS:
                l=left[(venue,coin,model)]; r=right[(venue,coin,model)]
                assert l["valid_dates"]==r["valid_dates"]
                rows.append({
                    "venue":venue,"coin":coin,"model":model,"valid_days":l["valid_dates"],
                    "net_2p5ms":l["net_usdt_mean"],
                    "net_25ms":r["net_usdt_mean"],
                    "fees_2p5ms":l["fees_usdt_mean"],
                    "fees_25ms":r["fees_usdt_mean"],
                    "fills_2p5ms":l["fills_mean"],
                    "fills_25ms":r["fills_mean"],
                    "cancels_2p5ms":l["cancels_mean"],
                    "cancels_25ms":r["cancels_mean"],
                })
    effects=[]
    for venue in VENUES:
        for coin in COINS:
            for profile,table in (("2.5ms",left),("25ms",right)):
                for label,b,g in (("basic","glft","glft_binance"),
                                  ("peak","glft_local_peak","glft_binance_local_peak")):
                    v=table[(venue,coin,g)]["net_usdt_mean"]-table[(venue,coin,b)]["net_usdt_mean"]
                    bv=table[(venue,coin,b)]
                    gv=table[(venue,coin,g)]
                    effects.append({
                        "venue":venue,"coin":coin,"profile":profile,
                        "model_pair":label,"valid_days":gv["valid_dates"],
                        "beta1_minus_beta0_net":v,
                        "baseline_net":bv["net_usdt_mean"],
                        "binance_net":gv["net_usdt_mean"],
                        "baseline_fills":bv["fills_mean"],
                        "binance_fills":gv["fills_mean"],
                        "baseline_fees":bv["fees_usdt_mean"],
                        "binance_fees":gv["fees_usdt_mean"],
                    })
    z={"2p5ms":x,"25ms":y,"paired_model_rows":rows,"binance_effect_rows":effects}
    out=Path(a.out);out.mkdir(parents=True,exist_ok=True)
    (out/"summary.json").write_text(json.dumps(z,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    md=["# 修复后的四市场历史对照：2.5ms vs 25ms",
        "",
        "均基于修复后的同一代码，官方HBT 2.4.4、每日期10min训练+50min交易、"
        "GLFT及4个变种、模拟Maker/Taker费用与1ms请求+1ms回报，"
        "同侧20ms限制保持不变。2026年6月至11月每月1日每日等权平均。",
        "",
        "## 主要结果：Binance指导相对同市场GLFT的含费净收益增量（USDT/50min）",
        "",
        "| 交易所 | 品种 | 有效天数 | 2.5ms: β1−β0 | 25ms: β1−β0 |",
        "|---|---|---:|---:|---:|"]
    for venue in VENUES:
        for coin in COINS:
            row2=next(v for v in effects if v["venue"]==venue and v["coin"]==coin
                      and v["profile"]=="2.5ms" and v["model_pair"]=="basic")
            row25=next(v for v in effects if v["venue"]==venue and v["coin"]==coin
                       and v["profile"]=="25ms" and v["model_pair"]=="basic")
            md.append(f"| {venue} | {coin} | {row2['valid_days']} "
                      f"| {row2['beta1_minus_beta0_net']:+.6f} "
                      f"| {row25['beta1_minus_beta0_net']:+.6f} |")
    md+=["","## 各四模型净收益（每日等权平均，USDT/50min，含费用）","",
         "| 交易所 | 品种 | 模型 | 2.5ms | 25ms |","|---|---|---|---:|---:|"]
    for r in rows:
        md.append(f"| {r['venue']} | {r['coin']} | {r['model']} "
                  f"| {r['net_2p5ms']:+.6f} | {r['net_25ms']:+.6f} |")
    md+=["","## 说明","",
         "- 这份基线只引用此次修复后原始报告与官方Recorder，不引用修复前旧利润或旧逆向选择归因。",
         "- OKX SOL校准只有6月与10月两天成功，其他4天保留无效，净收益不填零。",
         "- 25ms和2.5ms均为**人为额外Binance可见延迟**；订单请求和回报保持各1ms。",
         "- 两类延迟情景不能直接证明因果预测能力：β0/β1都受到Binance事件唤醒，"
         "信号可能通过减少成交和费用改善净收益；需要重跑成交后markout及等成交量对照。",
         "- 用户此前要求的全零情景（Binance 0ns、订单请求0ns、回报0ns）"
         "已经在另一份修复后历史运行 #43348209165 中完成，不能与修复前数据混用。",
         "- 模拟手续费已计入净收益；尚不包含资金费率、期末真实平仓成本与实际交易所限频。",
         ""]
    (out/"summary.md").write_text("\n".join(md),encoding="utf-8")
    for e in effects:
        print("EFFECT",e["venue"],e["coin"],e["profile"],e["model_pair"],
              "DAYS",e["valid_days"],
              "NET_DELTA",e["beta1_minus_beta0_net"],
              "BASE",e["baseline_net"],"GUIDED",e["binance_net"],flush=True)
    print("VALIDATED_48_CASES",flush=True)


if __name__=="__main__":
    main()
