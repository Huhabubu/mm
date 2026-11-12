"""Descriptive distribution of the *actual β=1 abnormal cross-venue gap*.

Uses exactly the existing BTC-OKX causal 60s basis, BTC Binance BBO,
the 2.5ms assumed Binance extra visibility, and 250ms quote-age bounds.

Primary sampling: UTC 00:10:00..00:59:59.9 every 100ms, to avoid weighting
the distribution by Binance's higher event frequency. This is NOT an
event-weighted histogram of every quote decision or of actual changed orders.
"""
import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np

from check_okx_hft_data import extract_window
from cross_venue_event_probe import read_binance_bbo, visible_timestamps
from run_beta_one_direct_gap import causal_60s_basis, read_local_bbo

NS = 1_000_000_000
CLOCK_STEP_NS = 100_000_000
MAX_QUOTE_AGE_NS = 250_000_000
TICK_USDT_PER_BTC = .1
DAYS = tuple(f"2026-{month:02d}-01" for month in range(6, 12))
PERCENTILES = (1,5,10,25,50,75,90,95,99)
THRESHOLDS_USDT = (.1, .5, 1., 2.5, 5., 10., 25., 50.)
DAY_SECONDS = 3600
WARMUP_SECONDS = 600


def compute_gt_grid(clock, okx_ns, okx_mid, binance_visible_ns,
                    binance_mid, basis_ns, basis_value,
                    tick_size=TICK_USDT_PER_BTC):
    """Mimic causal_price_shift_ticks at equal clock samples, as β=1.

    Live event code has already consumed Binance rows at current time t, so
    the latest Binance timestamp is <=t. OKX native BBO and completed-second
    baseline use strictly <t; all three have independent sequence clocks.
    """
    oi = np.searchsorted(okx_ns, clock, side="left") - 1
    bi = np.searchsorted(binance_visible_ns, clock, side="right") - 1
    gi = np.searchsorted(basis_ns, clock, side="left") - 1
    valid = (oi >= 0) & (bi >= 0) & (gi >= 0)
    oo, bb, gg = np.maximum(oi,0),np.maximum(bi,0),np.maximum(gi,0)
    age_okx = clock - okx_ns[oo]
    age_binance = clock - binance_visible_ns[bb]
    valid &= (age_okx >= 0) & (age_okx <= MAX_QUOTE_AGE_NS)
    valid &= (age_binance >= 0) & (age_binance <= MAX_QUOTE_AGE_NS)
    valid &= np.isfinite(basis_value[gg])
    g = np.full(len(clock), np.nan, dtype=np.float64)
    g[valid] = (binance_mid[bb[valid]]-okx_mid[oo[valid]]
                -basis_value[gg[valid]])
    # The actual β=1 quote-centre signal is zero if unavailable.
    effective = np.where(valid,g,0.0)
    return {
        "timestamp_ns":clock,
        "valid":valid,
        "G_usdt_per_btc":g,
        "effective_shift_usdt_per_btc":effective,
        "G_ticks":g/tick_size,
        "basis_usdt_per_btc":np.where(valid,basis_value[gg],np.nan),
        "okx_mid_usdt_per_btc":np.where(valid,okx_mid[oo],np.nan),
        "binance_mid_usdt_per_btc":np.where(valid,binance_mid[bb],np.nan),
        "okx_quote_age_ms":np.where(valid,age_okx/1e6,np.nan),
        "binance_quote_age_ms":np.where(valid,age_binance/1e6,np.nan)
    }


def describe(x):
    a=np.asarray(x,dtype=np.float64)
    a=a[np.isfinite(a)]
    if not len(a):
        return {"n":0}
    r={
        "n":int(len(a)), "mean":float(np.mean(a)),
        "std":float(np.std(a,ddof=1)) if len(a)>1 else None,
        "min":float(np.min(a)),"max":float(np.max(a)),
        "rms":float(np.sqrt(np.mean(a*a))),
    }
    for p in PERCENTILES:
        r[f"p{p:02d}"] = float(np.percentile(a,p))
    return r


def summarize(day, data):
    valid=data["valid"]
    g=data["G_usdt_per_btc"][valid]
    absolute=np.abs(g)
    mid=data["okx_mid_usdt_per_btc"][valid]
    a=data["effective_shift_usdt_per_btc"]
    return {
        "day":day, "clock_points":int(len(valid)),
        "valid_points":int(np.sum(valid)),
        "valid_fraction":float(np.mean(valid)),
        "invalid_zero_fallback_points":int(np.sum(~valid)),
        "G_signed_USDT":describe(g),
        "G_absolute_USDT":describe(absolute),
        "G_signed_ticks":describe(g/TICK_USDT_PER_BTC),
        "G_absolute_ticks":describe(absolute/TICK_USDT_PER_BTC),
        "G_signed_bps":describe(1e4*g/mid),
        "G_absolute_bps":describe(1e4*absolute/mid),
        "effective_including_invalid_zeros_USDT":describe(a),
        "positive_fraction":float(np.mean(g>0)) if len(g) else None,
        "negative_fraction":float(np.mean(g<0)) if len(g) else None,
        "zero_fraction":float(np.mean(g==0)) if len(g) else None,
        "absolute_exceeds_USDT":{
            f"{t:g}":float(np.mean(absolute>=t)) if len(absolute) else None
            for t in THRESHOLDS_USDT
        },
        "okx_quote_age_ms":describe(data["okx_quote_age_ms"][valid]),
        "binance_quote_age_ms":describe(data["binance_quote_age_ms"][valid]),
    }


def read_day(day,delay_ms):
    start_us = int(datetime.fromisoformat(day).replace(
        tzinfo=timezone.utc).timestamp()*1e6)
    end_us=start_us+DAY_SECONDS*1_000_000+5_000_000
    with TemporaryDirectory(prefix=f"gt_{day}_") as tmp:
        target=Path(tmp)/"okx_book_ticker.csv"
        count=extract_window("book_ticker",day,end_us,target,
            max_rows=3_000_000,exchange="okex-swap",symbol="BTC-USDT-SWAP")
        ot,om=read_local_bbo(target)
        bl,bm=read_binance_bbo(day,end_us,symbol="BTCUSDT")
    bt=visible_timestamps(bl,delay_ms)
    basis_t,basis=causal_60s_basis(ot,om,bt,bm)
    day0=start_us*1000
    clock=np.arange(day0+WARMUP_SECONDS*NS,day0+DAY_SECONDS*NS,
                    CLOCK_STEP_NS,dtype=np.int64)
    x=compute_gt_grid(clock,ot,om,bt,bm,basis_t,basis)
    return x,{"okx_book_ticker_rows":len(ot),
              "binance_book_ticker_rows":len(bt),
              "okx_extract_metadata":count,
              "basis_valid_second_points":int(np.count_nonzero(np.isfinite(basis)))}


def summary_text(report):
    rows=report["days"]
    grand=report["pooled_valid"]
    z=[
        "# 2.5ms Binance→OKX BTC 跨所异常价差 G_t 描述性统计",
        "",
        "**真实数据源：** Tardis 月首免费历史 Binance Futures BTCUSDT 与 OKX BTC-USDT-SWAP "
        "的原生 book_ticker（最优买卖报价）。2026-06至11月每月1日、"
        "UTC00:10—01:00交易时间，每100ms等间距观察。"
        "β=1、额外Binance延迟2.5ms、各所报价最大年龄250ms、"
        "60秒历史跨所基差的严格因果计算，保持现有回测同口径。",
        "",
        "## 方法与变量",
        "",
        "G_t = Binance中间价 − OKX中间价 − 60秒历史平均跨所价差，单位 USDT/BTC。"
        "β=1时理论报价中心平移G_t。"
        "有效时，分别采用截至当前已可见的Binance BBO、严格早于当前时刻的OKX BBO、"
        "严格早于当前时刻的已完成秒基差。数据缺失或过旧则信号修正为0，"
        "主分布只对有效信号统计，另外披露有效率与含无效归零的分布。",
        "",
        f"共计 {report['total_clock_points']} 个固定100ms观察点；"
        f"有效 {report['total_valid_points']} 个 "
        f"({100*report['overall_valid_fraction']:.3f}%)。",
        "",
        "## 逐日分布：G_t的符号和绝对值",
        "",
        "| 日期 | 有效点 | 有效率 | G均值(USDT) | G标准差 | G中位数 | G P05 | G P95 | |G|中位数 | |G| P90 | |G| P99 | |G|最大值 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
    ]
    for x in rows:
        s=x["G_signed_USDT"];a=x["G_absolute_USDT"]
        z.append(f"| {x['day']} | {x['valid_points']} | {100*x['valid_fraction']:.2f}% "
                 f"| {s['mean']:+.4f} | {s['std']:.4f} | {s['p50']:+.4f} "
                 f"| {s['p05']:+.4f} | {s['p95']:+.4f} "
                 f"| {a['p50']:.4f} | {a['p90']:.4f} | {a['p99']:.4f} "
                 f"| {a['max']:.4f} |")
    s=grand["G_signed_USDT"];a=grand["G_absolute_USDT"]
    z.extend([
        f"| **六日合并** | **{report['total_valid_points']}** | "
        f"**{100*report['overall_valid_fraction']:.2f}%** "
        f"| **{s['mean']:+.4f}** | **{s['std']:.4f}** | **{s['p50']:+.4f}** "
        f"| **{s['p05']:+.4f}** | **{s['p95']:+.4f}** "
        f"| **{a['p50']:.4f}** | **{a['p90']:.4f}** | **{a['p99']:.4f}** "
        f"| **{a['max']:.4f}** |",
        "",
        "## 合并分布：价格档和基点",
        "",
        f"OKX BTC原生tick = 0.1 USDT/BTC；因此 G_t = 1 USDT/BTC "
        f"对应理论报价中心平移10 tick。"
        f" |G|中位数 {grand['G_absolute_ticks']['p50']:.2f} tick；"
        f" P90 {grand['G_absolute_ticks']['p90']:.2f} tick；"
        f" P99 {grand['G_absolute_ticks']['p99']:.2f} tick。",
        "",
        f"按同期OKX中间价换算，|G|中位数 "
        f"{grand['G_absolute_bps']['p50']:.4f}bps，"
        f"P90 {grand['G_absolute_bps']['p90']:.4f}bps，"
        f"P99 {grand['G_absolute_bps']['p99']:.4f}bps。",
        "",
        f"有效G>0占比 {100*grand['positive_fraction']:.2f}%，"
        f"G<0占比 {100*grand['negative_fraction']:.2f}%，"
        f"G=0占比 {100*grand['zero_fraction']:.2f}%。",
        "",
        "### 绝对信号强度阈值的经验覆盖率",
        "",
        "| |G|阈值(USDT/BTC) | 理论档位 | 有效观察点中达到阈值的比例 |",
        "|---:|---:|---:|"
    ])
    for t in THRESHOLDS_USDT:
        v=grand["absolute_exceeds_USDT"][f"{t:g}"]
        z.append(f"| {t:g} | {t/TICK_USDT_PER_BTC:g} tick | {100*v:.2f}% |")
    z.extend([
        "",
        "## 注意",
        "",
        "- 这是**固定100ms时间加权的G_t分布**，每个交易窗口每秒取10次，"
        "不将高频Binance BBO每条事件重复当作独立观测，也不等于实际每次报价决策时的分布。",
        "- 实际β=1模式没有启用最小G阈值和滞回过滤。上表阈值仅是"
        "**事后描述性分组**，没有用于下单或回测交易。",
        "- 信号值与最后实际挂单价格变化不一定相等：报价要经过GLFT库存偏移、"
        "价格档取整、最优买卖价限制以及每侧20ms订单动作间隔。",
        "- 六日期均为已探索的月首日50分钟，并非六个完整交易日或独立样本外。",
        "- 包含原生Tardis local_timestamp与人为2.5ms可见延迟，"
        "尚未实测真实跨所服务器同步与行情传输时延。",
        "- NPZ保留逐时点有效性、原始G、无效信号归零后实际理论平移、"
        "OKX/Binance中间价、历史基差、各所BBO年龄，便于未来筛选阈值和重复分析。",
        "",
    ])
    return "\n".join(z)


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--day",choices=DAYS,help="Optional individual sample date")
    p.add_argument("--extra-binance-delay-ms",type=float,default=2.5)
    p.add_argument("--out",default="reports/binance_okx_btc_gt_2p5ms")
    p.add_argument("--reuse-saved",action="store_true",
                   help="Aggregate previously recorded NPZ from per-day matrix jobs")
    args=p.parse_args()
    if args.extra_binance_delay_ms!=2.5:
        raise ValueError("This fixed-protocol experiment is for 2.5ms only")
    out=Path(args.out)
    out.mkdir(parents=True,exist_ok=True)
    days=[args.day] if args.day else list(DAYS)
    stats=[]
    vectors=[]
    for day in days:
        if args.reuse_saved:
            file=out/f"{day}_100ms_G.npz"
            with np.load(file) as z:
                data={k:z[k].copy() for k in z.files}
            metadata={"source":"previous_matrix_download"}
        else:
            data,metadata=read_day(day,args.extra_binance_delay_ms)
        s=summarize(day,data)
        s["source_metadata"]={k:v for k,v in metadata.items() if k!="okx_extract_metadata"}
        stats.append(s)
        vectors.append(data)
        if not args.reuse_saved:
            np.savez_compressed(out/f"{day}_100ms_G.npz",**data)
        print("DATE",day,"VALID",s["valid_points"],"/",s["clock_points"],
              "MEAN",s["G_signed_USDT"].get("mean"),
              "MEDIAN_ABS",s["G_absolute_USDT"].get("p50"),
              "P90_ABS",s["G_absolute_USDT"].get("p90"),
              "MAX_ABS",s["G_absolute_USDT"].get("max"),flush=True)
    # Pooled distribution only over valid G; effective shift zeros kept separate.
    combined={key:np.concatenate([v[key] for v in vectors]) for key in vectors[0]}
    pooled=summarize("all_dates",combined)
    report={"experiment":"binance_okx_btc_Gt_fixed_100ms_beta1",
            "venue":"okex-swap", "target_symbol":"BTC-USDT-SWAP",
            "source_exchange":"binance-futures","source_symbol":"BTCUSDT",
            "beta":1.0,"assumed_extra_binance_delay_ms":2.5,
            "clock_step_ms":100,"warmup_minutes":10,"trade_minutes":50,
            "tick_size_usdt_per_btc":TICK_USDT_PER_BTC,
            "freshness_limit_ms":250,"basis_window_seconds":60,
            "basis_min_coverage_seconds":45,
            "days":stats,"pooled_valid":pooled,
            "total_clock_points":sum(s["clock_points"] for s in stats),
            "total_valid_points":sum(s["valid_points"] for s in stats)}
    report["overall_valid_fraction"]=report["total_valid_points"]/report["total_clock_points"]
    (out/"summary.json").write_text(json.dumps(report,indent=2,ensure_ascii=False)+"\n",encoding="utf-8")
    (out/"report.md").write_text(summary_text(report),encoding="utf-8")
    with (out/"by_day.csv").open("w",encoding="utf-8",newline="") as f:
        w=csv.writer(f)
        w.writerow(["date","valid","fraction","mean_G_USDT","std_G_USDT",
                    "p05_G_USDT","p50_G_USDT","p95_G_USDT",
                    "mean_abs_G_USDT","p50_abs_G_USDT","p90_abs_G_USDT",
                    "p99_abs_G_USDT","max_abs_G_USDT"])
        for s in stats:
            g=s["G_signed_USDT"];a=s["G_absolute_USDT"]
            w.writerow([s["day"],s["valid_points"],s["valid_fraction"],
                        g.get("mean"),g.get("std"),g.get("p05"),g.get("p50"),g.get("p95"),
                        a.get("mean"),a.get("p50"),a.get("p90"),a.get("p99"),a.get("max")])
    print("POOLED",json.dumps({
        "mean_G":pooled["G_signed_USDT"].get("mean"),
        "std_G":pooled["G_signed_USDT"].get("std"),
        "p50_abs_G":pooled["G_absolute_USDT"].get("p50"),
        "p90_abs_G":pooled["G_absolute_USDT"].get("p90"),
        "p99_abs_G":pooled["G_absolute_USDT"].get("p99"),
        "max_abs_G":pooled["G_absolute_USDT"].get("max"),
        "median_abs_bps":pooled["G_absolute_bps"].get("p50"),
        "valid_fraction":report["overall_valid_fraction"]}),flush=True)
    print("REPORT",out/"report.md",flush=True)


if __name__=="__main__":
    main()
