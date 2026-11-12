"""Binance SOLUSDT -> OKX SOL-USDT-SWAP: strictly causal 2.5ms G_t statistics.

Prices: BOTH venues' native book_ticker prices are USDT per SOL.
OKX tick_size = 0.01 USDT/SOL, contract value = 1 SOL/contract,
lot step = 0.01 contracts. Binance SOL futures quantity is SOL, not
OKX contracts, and neither quantity is used to calculate a price gap.

Reuse BTC study's exact as-of alignment and 60-second causal basis while
passing the SOL-specific OKX tick explicitly. No matching engine is created.
"""
import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np

from analyze_binance_okx_gt_distribution import (
    DAYS, CLOCK_STEP_NS, DAY_SECONDS, NS, PERCENTILES,
    WARMUP_SECONDS, compute_gt_grid, describe,
)
from check_okx_hft_data import extract_window
from cross_venue_event_probe import read_binance_bbo, visible_timestamps
from run_beta_one_direct_gap import causal_60s_basis, read_local_bbo
from run_multivenue_glft import MARKETS

TARGET = MARKETS[("okx", "SOL")]
BINANCE_SYMBOL = "SOLUSDT"
OKX_PRICE_TICK = 0.01  # USDT/SOL, NOT Binance tick (0.001) or OKX BTC tick (0.1)
THRESHOLDS_USDT = (0.01, 0.05, 0.1, 0.2, 0.5, 1., 2., 5.)
AGE_MAX_MS = 250

assert TARGET.symbol == "SOL-USDT-SWAP" and TARGET.exchange == "okex-swap"
assert TARGET.tick == OKX_PRICE_TICK and TARGET.ctval == 1.0
assert TARGET.lot == .01 and TARGET.coin == "SOL"


def validate_okx_bbo_prices(path):
    """Verify historical OKX prices, not order quantities, on the SOL tick."""
    n = 0
    lo, hi = np.inf, -np.inf
    with Path(path).open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        for row in reader:
            if row["exchange"] != TARGET.exchange or row["symbol"] != TARGET.symbol:
                raise ValueError("Wrong historical OKX exchange/symbol")
            bid, ask = float(row["bid_price"]), float(row["ask_price"])
            if not (np.isfinite(bid) and np.isfinite(ask) and 0 < bid < ask):
                raise ValueError("Invalid historical OKX SOL quote")
            for price in (bid, ask):
                tick_count = price / OKX_PRICE_TICK
                if abs(tick_count - round(tick_count)) > 1e-6:
                    raise ValueError(f"Historical OKX SOL price off 0.01 grid: {price}")
            n += 1
            lo = min(lo, bid)
            hi = max(hi, ask)
    if n < 200:
        raise ValueError(f"Unexpectedly sparse OKX SOL quote tape: {n}")
    return {"okx_bbo_rows":n,"okx_quote_min":float(lo),"okx_quote_max":float(hi),
            "okx_price_tick_usdt_per_sol":OKX_PRICE_TICK,
            "okx_contract_value_sol_per_contract":TARGET.ctval,
            "okx_lot_step_contracts":TARGET.lot}


def sample_day(day, delay_ms):
    epoch_us = int(datetime.strptime(day, "%Y-%m-%d").replace(
        tzinfo=timezone.utc).timestamp() * 1e6)
    end_us = epoch_us + DAY_SECONDS*1_000_000 + 5_000_000
    with TemporaryDirectory(prefix=f"okx_sol_gt_{day}_") as temp:
        path = Path(temp)/"okx_sol_native_bbo.csv"
        extracted = extract_window("book_ticker",day,end_us,path,
                                   max_rows=3_000_000,
                                   exchange=TARGET.exchange,symbol=TARGET.symbol)
        audit = validate_okx_bbo_prices(path)
        ot, om = read_local_bbo(path)
        bt_raw, bm = read_binance_bbo(day,end_us,symbol=BINANCE_SYMBOL)
    if len(ot)!=audit["okx_bbo_rows"]:
        raise ValueError("Native BBO row loss in SOL price reader")
    # Both native prices are USDT/SOL. Contract value and lot step MUST NOT enter.
    ratio = float(np.median(bm)/np.median(om))
    if not .9 < ratio < 1.1:
        raise ValueError(f"Different price quote units suspected: Binance / OKX median={ratio}")
    bt = visible_timestamps(bt_raw,delay_ms)
    baseline_t,baseline = causal_60s_basis(ot,om,bt,bm)
    clock = np.arange(epoch_us*1000+WARMUP_SECONDS*NS,
                      epoch_us*1000+DAY_SECONDS*NS,
                      CLOCK_STEP_NS,dtype=np.int64)
    raw = compute_gt_grid(clock,ot,om,bt,bm,baseline_t,baseline,
                          tick_size=OKX_PRICE_TICK)
    # Explicitly replace legacy BTC field labels so stored SOL data cannot
    # silently be interpreted as USDT/BTC.
    ret = {
        "timestamp_ns":raw["timestamp_ns"],
        "valid":raw["valid"],
        "G_usdt_per_sol":raw["G_usdt_per_btc"],
        "G_okx_sol_ticks":raw["G_ticks"],
        "effective_shift_usdt_per_sol":raw["effective_shift_usdt_per_btc"],
        "okx_mid_usdt_per_sol":raw["okx_mid_usdt_per_btc"],
        "binance_mid_usdt_per_sol":raw["binance_mid_usdt_per_btc"],
        "baseline_usdt_per_sol":raw["basis_usdt_per_btc"],
        "okx_quote_age_ms":raw["okx_quote_age_ms"],
        "binance_quote_age_ms":raw["binance_quote_age_ms"]
    }
    finite = ret["valid"]
    if not np.any(finite):
        raise ValueError(f"No valid signal in {day}")
    np.testing.assert_allclose(
        ret["G_okx_sol_ticks"][finite]*OKX_PRICE_TICK,
        ret["G_usdt_per_sol"][finite],atol=1e-11,rtol=1e-12)
    audit.update({"binance_bbo_rows":int(len(bm)),
                  "okx_bbo_rows_in_analysis":int(len(om)),
                  "binance_to_okx_median_price_ratio":ratio,
                  "okx_bbo_file_rows":extracted["rows"]})
    return ret, audit


def summary(day, series):
    valid = series["valid"]
    g = series["G_usdt_per_sol"][valid]
    ab = np.abs(g)
    mid = series["okx_mid_usdt_per_sol"][valid]
    return {
        "day":day, "clock_points":int(len(valid)),
        "valid_points":int(np.count_nonzero(valid)),
        "valid_fraction":float(np.mean(valid)),
        "invalid_zero_fallback_points":int(np.count_nonzero(~valid)),
        "G_signed_usdt_per_sol":describe(g),
        "G_absolute_usdt_per_sol":describe(ab),
        "G_absolute_okx_ticks":describe(ab/OKX_PRICE_TICK),
        "G_signed_bps":describe(10000*g/mid),
        "G_absolute_bps":describe(10000*ab/mid),
        "effective_including_missing_zeros":describe(series["effective_shift_usdt_per_sol"]),
        "G_positive_fraction":float(np.mean(g>0)),
        "G_negative_fraction":float(np.mean(g<0)),
        "G_zero_fraction":float(np.mean(g==0)),
        "abs_G_at_least_usdt_per_sol":{
            f"{t:g}":float(np.mean(ab>=t)) for t in THRESHOLDS_USDT
        },
        "okx_quote_age_ms":describe(series["okx_quote_age_ms"][valid]),
        "binance_quote_age_ms":describe(series["binance_quote_age_ms"][valid])
    }


def markdown(report):
    pool=report["pooled_valid"]
    signed=pool["G_signed_usdt_per_sol"]
    absolute=pool["G_absolute_usdt_per_sol"]
    ticks=pool["G_absolute_okx_ticks"]
    bps=pool["G_absolute_bps"]
    z=[
        "# Binance SOLUSDT → OKX SOL-USDT-SWAP：异常价差 G_t（2.5ms）",
        "",
        "**均使用USDT/SOL价格。OKX SOL tick=0.01 USDT，合约面值=1 SOL/张，数量步长=0.01张；"
        "Binance SOLUSDT合约报价为USDT/SOL，不乘OKX合约面值，也不使用Binance价格档转换目标报价。**",
        "",
        "样本：2026年6—11月每月1日UTC00:10—01:00，每100ms采样，"
        "相同2.5ms Binance额外行情延迟、250ms两所BBO新鲜度、"
        "60秒严格历史基差，β=1。仅对有效G进行描述，失效修正归零的比例单独报告。",
        "",
        "## 合并样本统计",
        "",
        f"总观察点 {report['total_clock_points']:,}，有效 {report['total_valid_points']:,}，"
        f"占 {100*report['overall_valid_fraction']:.2f}%。",
        "",
        "| 项目 | 结果 |",
        "|---|---:|",
        f"| G均值 (USDT/SOL) | {signed['mean']:+.6f} |",
        f"| G标准差 (USDT/SOL) | {signed['std']:.6f} |",
        f"| G中位数 (USDT/SOL) | {signed['p50']:+.6f} |",
        f"| G P05 / P95 (USDT/SOL) | {signed['p05']:+.6f} / {signed['p95']:+.6f} |",
        f"| 绝对G均值 (USDT/SOL) | {absolute['mean']:.6f} |",
        f"| **绝对G中位数 (USDT/SOL)** | **{absolute['p50']:.6f}** |",
        f"| 绝对G P90 (USDT/SOL) | {absolute['p90']:.6f} |",
        f"| 绝对G P99 (USDT/SOL) | {absolute['p99']:.6f} |",
        f"| 绝对G最大值 (USDT/SOL) | {absolute['max']:.6f} |",
        f"| **绝对G中位数 (OKX SOL ticks)** | **{ticks['p50']:.3f}** |",
        f"| 绝对G P90 (OKX SOL ticks) | {ticks['p90']:.3f} |",
        f"| **绝对G中位数 (bps)** | **{bps['p50']:.6f}** |",
        f"| 绝对G P90 (bps) | {bps['p90']:.6f} |",
        f"| 绝对G P99 (bps) | {bps['p99']:.6f} |",
        f"| G>0 / G<0 / G=0 | "
        f"{100*pool['G_positive_fraction']:.2f}% / "
        f"{100*pool['G_negative_fraction']:.2f}% / "
        f"{100*pool['G_zero_fraction']:.2f}% |",
        "",
        "## 逐日统计",
        "",
        "| 日期 | 有效点 | 有效率 | G均值 (USDT/SOL) | 绝对G中位数 | 绝对G P90 | 绝对G中位数 (bps) |",
        "|---|---:|---:|---:|---:|---:|---:|"
    ]
    for day in report["days"]:
        g=day["G_signed_usdt_per_sol"]
        a=day["G_absolute_usdt_per_sol"]
        z.append(f"| {day['day']} | {day['valid_points']:,} | {100*day['valid_fraction']:.2f}% "
                 f"| {g['mean']:+.6f} | {a['p50']:.6f} | {a['p90']:.6f} "
                 f"| {day['G_absolute_bps']['p50']:.6f} |")
    z.extend(["", "## 绝对信号幅度阈值覆盖（仅描述，现有策略未启用）", "",
              "| 绝对G至少达到 (USDT/SOL) | 按OKX SOL 0.01USDT换算的价格档 | 有效点占比 |",
              "|---:|---:|---:|"])
    for t in THRESHOLDS_USDT:
        z.append(f"| {t:g} | {t/OKX_PRICE_TICK:g} | "
                 f"{100*pool['abs_G_at_least_usdt_per_sol'][f'{t:g}']:.2f}% |")
    z.extend(["","## 单位与识别边界","",
              "- OKX SOL报价精度0.01 USDT/SOL、合约面值1 SOL/张，规格取自2026-11-12官方合约信息复核。"
              "行情原生最优买卖报价的价格单位为USDT/SOL；逐日核查OKX两侧报价均对齐0.01价格档。"
              "下单数量的张数或SOL币数不参与G_t计算。",
              "- Binance SOLUSDT最小价格档历史公告为0.001 USDT/SOL，"
              "本研究只读取Binance原生BBO报价（USDT/SOL），不据此修改OKX价格档或合约面值。",
              "- 使用固定100ms定时取样统计信号分布；这与策略实际由两所消息、订单回报驱动的"
              "报价检查事件分布不同，不能直接将这个统计解释为成功发单时的G_t分布。",
              "- 2026年7/8/9/11月OKX-SOL GLFT由于历史10分钟参数拟合失败无法有效回测做市，"
              "但本统计只依赖有效的两所原生BBO，不依赖GLFT A/k估计，因此可报告全部六日期的G_t统计。",
              "- 报价新鲜度限制为250ms，失效时β1平移回退0；统计只计算有效G分布。"
              "Binance额外2.5ms是假设而非实测传输时延。",
              "- 逐时点NPZ按USDT/SOL命名键，包含G、基差、目标与源交易所中间价、价格档、"
              "信号有效标记及双方报价年龄；所有原始数据与分析产出可在GitHub Actions重跑。"
    ])
    return "\n".join(z)+"\n"


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--day",choices=DAYS)
    p.add_argument("--extra-binance-delay-ms",type=float,default=2.5)
    p.add_argument("--reuse-saved",action="store_true")
    p.add_argument("--out",default="reports/binance_okx_sol_gt_2p5ms")
    a=p.parse_args()
    if a.extra_binance_delay_ms != 2.5:
        p.error("Fixed protocol: only 2.5ms extra Binance delay")
    out=Path(a.out)
    out.mkdir(parents=True,exist_ok=True)
    days=[a.day] if a.day else DAYS
    records=[]; summaries=[]; audits={}
    for day in days:
        if a.reuse_saved:
            with np.load(out/f"{day}_100ms_G_SOL.npz") as z:
                item={k:z[k].copy() for k in z.files}
            audits[day]={"source":"saved_npz"}
        else:
            item,audit=sample_day(day,a.extra_binance_delay_ms)
            audits[day]=audit
            np.savez_compressed(out/f"{day}_100ms_G_SOL.npz",**item)
        stat=summary(day,item)
        summaries.append(stat)
        records.append(item)
        print("DAY",day,"VALID",stat["valid_points"],"OF",stat["clock_points"],
              "MED_ABS_USDT_SOL",stat["G_absolute_usdt_per_sol"]["p50"],
              "MED_ABS_BPS",stat["G_absolute_bps"]["p50"],
              "P90_ABS_USDT_SOL",stat["G_absolute_usdt_per_sol"]["p90"],
              flush=True)
    joined={key:np.concatenate([r[key] for r in records]) for key in records[0]}
    pooled=summary("pooled",joined)
    result={"experiment":"binance_sol_to_okx_sol_Gt_2p5ms",
            "venue":"okex-swap","target_symbol":"SOL-USDT-SWAP",
            "source_exchange":"binance-futures","source_symbol":"SOLUSDT",
            "price_unit":"USDT per SOL", "beta":1.0,
            "target_tick_usdt_per_sol":OKX_PRICE_TICK,
            "target_contract_value_sol_per_contract":TARGET.ctval,
            "target_order_step_contracts":TARGET.lot,
            "assumed_extra_binance_delay_ms":2.5,
            "clock_step_ms":100,
            "warmup_minutes":10,"trade_minutes":50,
            "basis_window_seconds":60,
            "basis_min_coverage_seconds":45,
            "quote_age_max_ms":AGE_MAX_MS,
            "total_clock_points":int(len(joined["timestamp_ns"])),
            "total_valid_points":int(np.sum(joined["valid"])),
            "overall_valid_fraction":float(np.mean(joined["valid"])),
            "days":summaries,"pooled_valid":pooled,"audits":audits}
    if len(days)==len(DAYS):
        assert result["total_clock_points"]==180_000
    (out/"summary.json").write_text(json.dumps(result,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    (out/"report.md").write_text(markdown(result),encoding="utf-8")
    with (out/"by_day.csv").open("w",encoding="utf-8",newline="") as f:
        w=csv.writer(f)
        w.writerow(("date","valid_points","valid_fraction","G_mean_usdt_per_sol",
                    "G_std_usdt_per_sol","G_p05_usdt_per_sol","G_p50_usdt_per_sol",
                    "G_p95_usdt_per_sol","abs_G_mean_usdt_per_sol","abs_G_median_usdt_per_sol",
                    "abs_G_p90_usdt_per_sol","abs_G_p99_usdt_per_sol","abs_G_median_okx_ticks",
                    "abs_G_median_bps","abs_G_p90_bps"))
        for d in summaries:
            g=d["G_signed_usdt_per_sol"];x=d["G_absolute_usdt_per_sol"]
            w.writerow((d["day"],d["valid_points"],d["valid_fraction"],
                        g["mean"],g["std"],g["p05"],g["p50"],g["p95"],
                        x["mean"],x["p50"],x["p90"],x["p99"],
                        d["G_absolute_okx_ticks"]["p50"],
                        d["G_absolute_bps"]["p50"],d["G_absolute_bps"]["p90"]))
    print("POOLED",json.dumps({
        "valid":result["total_valid_points"],
        "valid_fraction":result["overall_valid_fraction"],
        "signed_mean":pooled["G_signed_usdt_per_sol"]["mean"],
        "signed_std":pooled["G_signed_usdt_per_sol"]["std"],
        "absolute_p50":pooled["G_absolute_usdt_per_sol"]["p50"],
        "absolute_p90":pooled["G_absolute_usdt_per_sol"]["p90"],
        "absolute_p99":pooled["G_absolute_usdt_per_sol"]["p99"],
        "absolute_tick_p50":pooled["G_absolute_okx_ticks"]["p50"],
        "absolute_bps_p50":pooled["G_absolute_bps"]["p50"],
        "absolute_bps_p90":pooled["G_absolute_bps"]["p90"]
    },ensure_ascii=False),flush=True)


if __name__=="__main__":
    main()
