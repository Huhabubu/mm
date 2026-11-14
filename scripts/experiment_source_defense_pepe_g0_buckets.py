"""1000PEPE June-1 read-only original G0 fill conditional Binance gap buckets.

Input: already saved OFFICIAL hftbacktest G0 fills from the V4 June pilot.
No custom matching or new orders. Normalize Binance 1000PEPEUSDT price to
target PEPE-USDT-SWAP USDT/PEPE before causal EMA gap.
"""
import argparse
import json
from datetime import datetime,timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import numpy as np

from check_okx_hft_data import extract_window
from run_beta_one_direct_gap import read_local_bbo
from cross_venue_event_probe import read_binance_bbo,visible_timestamps
from experiment_source_defense_signal import past_grid_time_ema
from experiment_source_defense_step1_diagnostic import (
    load_fills, prefill_binance_gap_buckets,
)



def visibility_reasons(g0,ot,om,bt,bm,grid,ema,stale_ns):
    """Exhaustive exclusive reason labels, never forward-looking fills."""
    t=np.asarray(g0["exchange_timestamp_ns"],np.int64)-1_000_000
    window=t-100_000_000
    b=np.searchsorted(bt,t,side="left")-1
    o=np.searchsorted(ot,t,side="left")-1
    g=np.searchsorted(grid,t,side="left")-1
    w=np.searchsorted(bt,window,side="right")-1
    labels=[]
    ages=[]
    for i in range(len(t)):
        age={"bn_now_ms":(t[i]-bt[b[i]])/1e6 if b[i]>=0 else None,
             "okx_now_ms":(t[i]-ot[o[i]])/1e6 if o[i]>=0 else None,
             "bn_window_ms":(window[i]-bt[w[i]])/1e6 if w[i]>=0 else None}
        ages.append(age)
        if b[i]<0: reason="no_prior_binance_asof_now"
        elif o[i]<0: reason="no_prior_okx_asof_now"
        elif g[i]<0: reason="no_prior_causal_ema_grid"
        elif w[i]<0: reason="no_prior_binance_asof_100ms"
        elif not np.isfinite(ema[g[i]]):reason="ema_not_finite"
        elif not np.isfinite(om[o[i]]) or om[o[i]]<=0:reason="target_mid_invalid"
        elif age["bn_now_ms"]<0 or age["bn_now_ms"]*1e6>stale_ns:
            reason="binance_latest_bbo_stale"
        elif age["okx_now_ms"]<0 or age["okx_now_ms"]*1e6>stale_ns:
            reason="okx_latest_bbo_stale"
        elif age["bn_window_ms"]<0 or age["bn_window_ms"]*1e6>stale_ns:
            reason="binance_100ms_window_prior_bbo_stale"
        else:reason="fully_evaluable"
        labels.append(reason)
    counts={}
    for x in labels:counts[x]=counts.get(x,0)+1
    missing=[i for i,x in enumerate(labels) if x!="fully_evaluable"]
    return {"max_age_ms":stale_ns/1e6,
            "reason_counts_exclusive":counts,
            "missing_total":len(missing),
            "missing_age_ms_samples":[{"exchange_fill_timestamp_ns":int(g0["exchange_timestamp_ns"][i]),
               "reason":labels[i],**ages[i]} for i in missing[:12]],
            "max_bbo_age_ms":{k:max((v[k] for v in ages if v[k] is not None),default=None)
                              for k in ("bn_now_ms","okx_now_ms","bn_window_ms")},
            "exact_original_freshness_sensitivity":True}


def audit(folder,day="2026-06-01"):
    root=Path(folder)
    if not (root/"summary.json").exists():
        raise FileNotFoundError("Official V4 June 1000PEPE baseline summary missing")
    summary=json.loads((root/"summary.json").read_text(encoding="utf8"))
    if summary["coin"]!="1000PEPE":
        raise ValueError("Must be June 1000PEPE official HBT original G0")
    if summary["scenarios"]["g0"]["status"]!="complete":
        raise ValueError("G0 baseline not recorded")
    if summary["pretrade_minutes"]!=10:
        raise ValueError("PEPE G0 must use common original 10min warmup")
    day_us=int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp()*1_000_000)
    end=day_us+3605*1_000_000
    with TemporaryDirectory(prefix="pepe_g0_bbo_buckets_") as scratch:
        path=Path(scratch)/"okx_pepe.csv"
        extract_window("book_ticker",day,end,path,
                       exchange="okex-swap",symbol="PEPE-USDT-SWAP",
                       max_rows=5_000_000)
        ot,om=read_local_bbo(path)
        bt,bmraw=read_binance_bbo(day,end,symbol="1000PEPEUSDT")
        bt=visible_timestamps(bt,2.5)
        bm=bmraw/1000.
        grid,basis=past_grid_time_ema(ot,om,bt,bm,1.)
        g0=load_fills(root,"g0")
        qa250=visibility_reasons(g0,ot,om,bt,bm,grid,basis,250_000_000)
        qa1000=visibility_reasons(g0,ot,om,bt,bm,grid,basis,1_000_000_000)
        qa2000=visibility_reasons(g0,ot,om,bt,bm,grid,basis,2_000_000_000)
        original=prefill_binance_gap_buckets(
            g0,ot,om,bt,bm,grid,basis,summary["tick_size_usdt"])
        # A strictly backward as-of quote may remain unchanged for >250ms.
        # 2s is an *exploratory label-coverage sensitivity* only: the trading
        # engine's 250ms signal freshness gate is deliberately unchanged.
        table=prefill_binance_gap_buckets(
            g0,ot,om,bt,bm,grid,basis,summary["tick_size_usdt"],
            freshness_ns=2_000_000_000)
    return {
        "market":"OKX PEPE-USDT-SWAP","binance_symbol":"1000PEPEUSDT",
        "Binance_price_divisor":1000.,
        "date":day,"G0_original_fill_count":len(g0["side_sign"]),
        "baseline_uses_official_hftbacktest":True,
        "basis_kind":"causal 1s EMA on previsible normalized price gap",
        "asof":"1ms before exchange execution; not the actual quote decision",
        "trade_confirmation":"none in this Step1 table, BBO impulse only",
        "conditional_table":table,
        "original_250ms_table":original,
        "coverage_reasons_250ms":qa250,
        "coverage_reasons_1000ms":qa1000,
        "coverage_reasons_2000ms":qa2000,
        "coverage_change_only_posttrade_diagnostic":True,
        "caveats":["No causal source of price movement proven by BBO-only table",
                   "Small buckets are descriptive, do not optimize thresholds",
                   "Historical PEPE instrument specification unverified"]}


def render(d):
    rows=["# 1000PEPE / OKX 单日 G0 原始成交条件 Markout 分桶",
          "", "Binance 1000PEPEUSDT 价格先除1000；目标所原生 PEPE-USDT-SWAP。",
          "全部成交来自此前V4正式HBT的原G0回放，不重新运行策略。",
          "Binance触发定义为成交前1ms已可见中间价过去100ms同方向移动≥1个目标价格档。",
          "该触发是BBO价格位移代理，非逐笔主动吃单确认。",
          "诊断：原250ms vs 扩至2s的严格过去 as-of 价格敏感性；策略防御仍使用原250ms时效门。",
          "",
          "|信号状态与绝对价差bps|成交|占比|100ms均值/P10 bps|500ms均值/P10 bps|1s均值/P10 bps|",
          "|---|---:|---:|---|---|---|"]
    for z in d["conditional_table"]["buckets"]:
        def fmt(h):
            x=z["by_horizon"][f"{h}ms"]
            a=x["mean_markout_bps"];b=x["p10_markout_bps"]
            return f"{a:+.4f}/{b:+.4f}" if a is not None and b is not None else "N/A"
        rows.append(f"|{z['bin']}|{z['fills']}|{z['fraction_all_g0_fills']:.1%}|"
                    f"{fmt(100)}|{fmt(500)}|{fmt(1000)}|")
    rows+=["", "## 缺失标签根因（互斥归类）", "",
            "|原因|原250ms人数|2s严格过去As-Of人数|",
            "|---|---:|---:|"]
    all_reasons=sorted(set(d["coverage_reasons_250ms"]["reason_counts_exclusive"])|
                       set(d["coverage_reasons_2000ms"]["reason_counts_exclusive"]))
    for k in all_reasons:
        rows.append(f"|{k}|{d['coverage_reasons_250ms']['reason_counts_exclusive'].get(k,0)}|"
                    f"{d['coverage_reasons_2000ms']['reason_counts_exclusive'].get(k,0)}|")
    rows+=["", "验证限制：仅一个已探索日期，不能凭单个桶判定显著毒单；"
            "后续与前瞻因果阈值以及Queue Shield/taker trades消融共同判断。"]
    return "\n".join(rows)+"\n"


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--source",required=True)
    p.add_argument("--out",default="reports/source_defense/v4_pepe_g0_buckets")
    args=p.parse_args()
    parents=list(Path(args.source).rglob("summary.json"))
    folders=[x.parent for x in parents if x.parent.name.endswith("_okx_1000PEPE")]
    if len(folders)!=1:
        raise ValueError(f"Expected one V4 PEPE case, found {len(folders)}")
    item=audit(folders[0])
    out=Path(args.out);out.mkdir(parents=True,exist_ok=True)
    (out/"pepe_g0_buckets.json").write_text(
        json.dumps(item,ensure_ascii=False,indent=2,allow_nan=False),encoding="utf8")
    md=render(item)
    (out/"pepe_g0_buckets.md").write_text(md,encoding="utf8")
    print(md)


if __name__=="__main__":
    main()
