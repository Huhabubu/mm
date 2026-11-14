"""Migration-period V6 extension: 6 days, raw BBO dual clocks and conditional response.

This is a read-only audit, never a trading PnL or a physical latency measurement.
No future target quote is used for the pre-event state; future quotes are labels.
"""
import argparse
import json
from pathlib import Path

import numpy as np
from experiment_source_defense_v6_clock_microprice import (
    MARKETS, read_native, clock_report, change_events,
    signed_nearest_same_direction_ms
)

DAYS = ("2026-06-01", "2026-07-01", "2026-08-01",
        "2026-09-01", "2026-10-01", "2026-11-01")
COINS = ("BTC", "SOL", "DOGE", "1000PEPE")
HORIZONS_MS = (50, 100)
MAX_AGE_US = 250_000


def last_timestamp_rows(x):
    """One known final state per equal-local-microsecond delivery batch."""
    t = x["local_us"]
    pick = np.r_[np.diff(t) != 0, True]
    return {k: (v[pick] if isinstance(v, np.ndarray) and len(v) == len(t) else v)
            for k, v in x.items()}


def price_move_indices(x):
    mid = (x["bid"] + x["ask"]) / 2
    indices = np.flatnonzero(np.diff(mid) != 0) + 1
    if not len(indices):
        return indices, np.empty(0, dtype=np.int8)
    # One price change per 20ms window. A deterministic density control.
    bucket = x["local_us"][indices] // 20_000
    valid = np.r_[True, np.diff(bucket) != 0]
    indices = indices[valid]
    return indices, np.sign(mid[indices] - mid[indices - 1]).astype(np.int8)


def signed_follow_response(trigger, target, self_trigger=False, delay_ms=0.):
    """Signed target mid change after trigger; target self-trigger excludes own jump.

    The target price at a Binance trigger is strictly before the decision time,
    whereas for a target-native trigger the just-observed triggering move is
    included in the baseline. Both use local collector time, one microsecond
    batch at a time. Unaligned exchange clocks are never combined here.
    """
    ix, signs = price_move_indices(trigger)
    tt = target["local_us"]
    tm = (target["bid"] + target["ask"]) / 2
    when = trigger["local_us"][ix] + int(round(delay_ms * 1000))
    base_ix = (ix if self_trigger else np.searchsorted(tt, when, side="left") - 1)
    safe = np.maximum(base_ix, 0)
    base_ok = (base_ix >= 0) & (when >= tt[safe]) & (when - tt[safe] <= MAX_AGE_US)
    result = {"trigger_events_before_age_filter": int(len(ix))}
    for horizon in HORIZONS_MS:
        end = when + horizon * 1000
        tail_ix = np.searchsorted(tt, end, side="right") - 1
        tail = np.maximum(tail_ix, 0)
        ok = (base_ok & (tail_ix >= 0) & (end >= tt[tail])
              & (end - tt[tail] <= MAX_AGE_US) & (tail >= safe)
              & (end <= tt[-1]))
        v = signs[ok] * (tm[tail[ok]] - tm[safe[ok]]) / tm[safe[ok]] * 10000.
        result[f"{horizon}ms"] = {
            "n": int(len(v)), "mean_signed_bps": float(v.mean()) if len(v) else None,
            "p10_signed_bps": float(np.quantile(v, .1)) if len(v) else None,
            "median_signed_bps": float(np.median(v)) if len(v) else None,
            "positive_fraction": float(np.mean(v > 0)) if len(v) else None,
            "nonzero_fraction": float(np.mean(v != 0)) if len(v) else None
        }
    return result


def case(day, coin):
    if day not in DAYS or coin not in COINS:
        raise ValueError("Out of fixed six-day/four-coin audit scope")
    sources = MARKETS[coin]
    bn_raw = read_native(day, *sources[0])
    bn = last_timestamp_rows(bn_raw)
    recs = []
    for venue, symbol, div in sources[1:]:
        target_raw = read_native(day, venue, symbol, div)
        target = last_timestamp_rows(target_raw)
        recs.append({
            "date": day, "coin": coin, "venue": venue, "symbol": symbol,
            "binance_book_ticker": sources[0][1],
            "binance_collector_lag": clock_report(bn_raw),
            "target_collector_lag": clock_report(target_raw),
            "clock_sample_population": "raw_all_native_rows",
            "response_event_population": "last_state_of_each_local_microsecond_batch",
            "signed_nearest_100ms": {
                clock: signed_nearest_same_direction_ms(
                    change_events(bn_raw, clock), change_events(target_raw, clock))
                for clock in ("exchange", "local")
            },
            "response": {
                "binance_trigger_0ms": signed_follow_response(bn, target),
                "binance_trigger_2p5ms": signed_follow_response(bn, target, delay_ms=2.5),
                "target_native_price_trigger": signed_follow_response(
                    target, target, self_trigger=True)
            },
            "clock_difference_not_physical_latency": True,
            "matching_can_reuse_target_events": True,
            "sample_window_utc": "00:00–01:10:05",
            "same_microsecond_final_state_for_trigger": True
        })
    return {"day": day, "coin": coin, "targets": recs}


def aggregate(root):
    cases = []
    for path in Path(root).rglob("case.json"):
        for row in json.loads(path.read_text())["targets"]:
            cases.append(row)
    cases.sort(key=lambda r: (r["coin"], r["venue"], r["date"]))
    if any(r.get("clock_sample_population") != "raw_all_native_rows" for r in cases):
        raise ValueError("Clock source must retain ALL native rows for identical original V6 lag quantiles; redo older de-duplicated cases")
    unique = {(r["date"], r["coin"], r["venue"]) for r in cases}
    if len(cases) != 30 or len(unique) != 30:
        raise ValueError(f"Expected 30 unique coin/venue/day cases; got {len(unique)}")
    periods = {}
    for coin, venue in sorted({(r["coin"], r["venue"]) for r in cases}):
        periods[f"{coin}_{venue}"] = {}
        for name, dates in (("pre", DAYS[:3]), ("post", DAYS[3:])):
            rows = [r for r in cases if (r["coin"], r["venue"]) == (coin, venue)
                    and r["date"] in dates]
            def per_day_mean(selector):
                return float(np.mean([selector(r) for r in rows]))
            periods[f"{coin}_{venue}"][name] = {
                "days": list(dates),
                "binance_local_minus_exchange_p50_ms_equal_day_mean": per_day_mean(
                    lambda r: r["binance_collector_lag"]["median_ms"]),
                "target_local_minus_exchange_p50_ms_equal_day_mean": per_day_mean(
                    lambda r: r["target_collector_lag"]["median_ms"]),
                "signed_nearest_lag_p50_ms_equal_day_mean": {
                    clk: per_day_mean(lambda r: r["signed_nearest_100ms"][clk]["median_ms"])
                    for clk in ("exchange", "local")
                },
                "trigger_signed_response_bps_equal_day_mean": {
                    trig: {f"{h}ms": per_day_mean(
                        lambda r: r["response"][trig][f"{h}ms"]["mean_signed_bps"])
                        for h in HORIZONS_MS}
                    for trig in ("binance_trigger_0ms", "binance_trigger_2p5ms",
                                 "target_native_price_trigger")
                }
            }
    return {"cases": cases, "periods": periods,
            "date_cut": {"pre": list(DAYS[:3]), "post": list(DAYS[3:])},
            "method": "Native book_ticker, deterministic equal-local-time batch collapse, 20ms event density control, 250ms quote age; 50/100ms labels.",
            "limitations": [
                "Signed nearest same-direction match may reuse target events and is affected by event frequency",
                "Exchange timestamp clocks lack independently verified synchronization",
                "Collector local timestamp is not a measured executable network round trip",
                "A binance-trigger baseline excludes simultaneous target quotes; a target-native trigger baseline includes its own move",
                "Six monthly first days, not an identified 2026-08-04 regression discontinuity"
            ]}


def markdown(result):
    lines = ["# 2026-08-04 前后六日期时钟与条件价格响应审计", "",
             "BBO 为最优买卖报价；local−exchange 与所有有符号相对时差单位为毫秒。",
             "正向相对时差表示匹配的 Binance 更新先出现。该事件配对非唯一，不能解释为物理传播或因果领先。", "",
             "## 每日分位数和最近同向价格变动", "",
             "|日期|币种|目标所|BN lag P10/P50/P90|目标lag P10/P50/P90|exchange Δ P10/P50/P90|local Δ P10/P50/P90|",
             "|---|---|---|---|---|---|---|"]
    def q(x):
        return "/".join(f"{x[k]:.3f}" for k in ("p10_ms", "median_ms", "p90_ms"))
    for r in result["cases"]:
        lines.append(f"|{r['date']}|{r['coin']}|{r['venue']}|{q(r['binance_collector_lag'])}|{q(r['target_collector_lag'])}|{q(r['signed_nearest_100ms']['exchange'])}|{q(r['signed_nearest_100ms']['local'])}|")
    lines.extend(["", "## 迁移前后按日等权平均及50/100ms条件响应", "",
                  "响应为触发方向乘后续目标中间价收益，单位bps。BN触发比较0ms与额外2.5ms可见时刻；目标所本地触发基准已包含触发时的本所价格变动。", "",
                  "|市场|时期|BN P50|目标P50|exchange Δ P50|local Δ P50|BN0 50/100ms|BN2.5 50/100ms|目标本地50/100ms|",
                  "|---|---|---:|---:|---:|---:|---|---|---|"])
    for label, periods in result["periods"].items():
        for name, d in periods.items():
            ans = d["trigger_signed_response_bps_equal_day_mean"]
            def ab(k):
                return "/".join(f"{ans[k][f'{h}ms']:+.4f}" for h in HORIZONS_MS)
            signed = d["signed_nearest_lag_p50_ms_equal_day_mean"]
            lines.append(f"|{label}|{name}|{d['binance_local_minus_exchange_p50_ms_equal_day_mean']:.3f}|{d['target_local_minus_exchange_p50_ms_equal_day_mean']:.3f}|{signed['exchange']:+.3f}|{signed['local']:+.3f}|{ab('binance_trigger_0ms')}|{ab('binance_trigger_2p5ms')}|{ab('target_native_price_trigger')}|")
    lines.extend(["", "### 限制", "",
                  "前后各三天属于描述性比较，迁移恰在2026-08-04而月首日未直接覆盖迁移当日。需单独控制日期、行情发布节奏、交易量、合约规格、市场波动以及时钟校准。"])
    return "\n".join(lines) + "\n"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", required=True, choices=("case", "aggregate"))
    ap.add_argument("--day")
    ap.add_argument("--coin")
    ap.add_argument("--source")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    if a.mode == "case":
        if not a.day or not a.coin:
            ap.error("case requires --day --coin")
        (out / "case.json").write_text(
            json.dumps(case(a.day, a.coin), indent=2, ensure_ascii=False, allow_nan=False))
    else:
        if not a.source:
            ap.error("aggregate requires --source")
        obj = aggregate(a.source)
        (out / "migration_clock.json").write_text(
            json.dumps(obj, indent=2, ensure_ascii=False, allow_nan=False))
        (out / "migration_clock.md").write_text(markdown(obj))
        print(markdown(obj))


if __name__ == "__main__":
    main()
