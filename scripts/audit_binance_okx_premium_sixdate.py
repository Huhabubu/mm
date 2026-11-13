"""Report whether Binance mid is more expensive than OKX, separately from G.

Uses the existing verified six-date 100ms Tardis archives. Both mids
denominated USDT per base coin. Count ONLY 250ms-fresh plus valid-basis
paired observations; do not give high-frequency Binance events more weight.
"""
import argparse
import json
from pathlib import Path

import numpy as np

DAYS=tuple(f"2026-{m:02d}-01" for m in range(6,12))
COINS=("BTC","SOL")


def audit_day(path,coin):
    with np.load(path) as z:
        valid=z["valid"]
        assert len(valid)==30_000
        if coin=="BTC":
            a=z["binance_mid_usdt_per_btc"][valid]
            b=z["okx_mid_usdt_per_btc"][valid]
            g=z["G_usdt_per_btc"][valid]
            basis=z["basis_usdt_per_btc"][valid]
        else:
            a=z["binance_mid_usdt_per_sol"][valid]
            b=z["okx_mid_usdt_per_sol"][valid]
            g=z["G_usdt_per_sol"][valid]
            basis=z["baseline_usdt_per_sol"][valid]
        if len(a)==0:raise ValueError("No fresh jointly usable market observations")
        np.testing.assert_allclose(a-b-basis,g,atol=1e-11,rtol=0)
        delta=a-b
        return {
            "valid":int(valid.sum()),"all_points":int(len(valid)),
            "premium_positive":int(np.count_nonzero(delta>0)),
            "premium_negative":int(np.count_nonzero(delta<0)),
            "premium_zero":int(np.count_nonzero(delta==0)),
            "g_positive":int(np.count_nonzero(g>0)),
            "g_negative":int(np.count_nonzero(g<0)),
            "g_zero":int(np.count_nonzero(g==0)),
            "raw_spread_mean_usdt_per_coin":float(delta.mean()),
            "raw_spread_median_usdt_per_coin":float(np.median(delta)),
            "raw_spread_mean_bps":float(np.mean(10000*delta/b)),
            "raw_spread_median_bps":float(np.median(10000*delta/b)),
            "basis_median_usdt_per_coin":float(np.median(basis)),
        }


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--btc-source",default="btc")
    p.add_argument("--sol-source",default="sol")
    p.add_argument("--out",default="reports/binance_okx_premium")
    a=p.parse_args()
    folder=Path(a.out);folder.mkdir(parents=True,exist_ok=True)
    result={}
    for coin in COINS:
        base=Path(a.btc_source if coin=="BTC" else a.sol_source)
        out=[]
        for day in DAYS:
            suffix="_100ms_G.npz" if coin=="BTC" else "_100ms_G_SOL.npz"
            item=audit_day(base/(day+suffix),coin)
            item["day"]=day
            out.append(item)
        agg={k:sum(row[k] for row in out) for k in
             ("valid","all_points","premium_positive","premium_negative",
              "premium_zero","g_positive","g_negative","g_zero")}
        agg["raw_premium_positive_fraction"]=agg["premium_positive"]/agg["valid"]
        agg["G_positive_fraction"]=agg["g_positive"]/agg["valid"]
        result[coin]={"by_day":out,"pooled":agg}
        print("PREMIUM",coin,json.dumps(agg,ensure_ascii=False),flush=True)
    (folder/"summary.json").write_text(json.dumps(result,indent=2,ensure_ascii=False)+"\n",encoding="utf-8")
    s=["# Binance 相对 OKX 中间价是否长期偏高（原始价差与G严格分开）","",
       "样本：2026年6—11月每月1日，UTC00:10—01:00；每100ms观测一次。"
       "两所行情250ms内新鲜、60秒历史基差有效的时点才纳入。"
       "Binance额外可见延迟2.5ms；Tardis BBO中间价均为USDT/基础币。"
       "**这里的统计是原始Binance中间价与OKX中间价的同时刻大小，"
       "不是Binance领先预测力或G为正的概率。**","",
       "| 币种 | 有效点 | Binance>OKX占比 | Binance<OKX占比 | 相等占比 | G>0占比 |",
       "|---|---:|---:|---:|---:|---:|"]
    for coin,r in result.items():
        q=r["pooled"]
        fmt=lambda k: f"{100*q[k]/q['valid']:.2f}%"
        s.append(f"| {coin} | {q['valid']:,} | {fmt('premium_positive')} "
                 f"| {fmt('premium_negative')} | {fmt('premium_zero')} "
                 f"| {fmt('g_positive')} |")
    for coin,r in result.items():
        s+=["",f"## {coin} 每日比例","",
            "| 日期 | 有效点 | 原始 Binance>OKX | G>0 | 价差均值（USDT/币） | 原始价差均值（bps） |",
            "|---|---:|---:|---:|---:|---:|"]
        for d in r["by_day"]:
            s.append(f"| {d['day']} | {d['valid']:,} "
                     f"| {100*d['premium_positive']/d['valid']:.2f}% "
                     f"| {100*d['g_positive']/d['valid']:.2f}% "
                     f"| {d['raw_spread_mean_usdt_per_coin']:+.6f} "
                     f"| {d['raw_spread_mean_bps']:+.4f} |")
    s+=["","统计精确含义：固定时间等权，不把Binance更频繁的消息重复计数。"
         "汇总按所有有效100ms时间点合并，6日的有效率有差异；"
         "每一天比例也独立披露。报价修复后策略G使用官方HBT实时目标盘口，"
         "此处使用原生OKX BBO用于检验原始价格高低方向，不完全等于策略事件时刻的G。"]
    (folder/"report.md").write_text("\n".join(s)+"\n",encoding="utf-8")


if __name__=="__main__":
    main()
