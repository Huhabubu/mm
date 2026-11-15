"""Summarize rolling Binance-Gate historical basis changes in Gate quote ticks.

Input: existing causal 600s lookback, 5s update, no-age-filter CSV results.
Only evaluates historical B_t movement; not real quote amendments.
"""
import argparse
import csv
import json
import math
from datetime import datetime, timedelta
from pathlib import Path


def percentile(values, p):
    if not values or not 0 <= p <= 1:
        raise ValueError("Invalid quantile")
    ordered = sorted(values)
    index = (len(ordered)-1)*p
    lower = int(math.floor(index))
    upper = min(lower+1, len(ordered)-1)
    return ordered[lower] + (ordered[upper]-ordered[lower])*(index-lower)


def one_day(path, tick):
    if not math.isfinite(tick) or tick <= 0:
        raise ValueError("Price tick must be positive")
    with path.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if len(rows) != 600:
        raise ValueError(f"Expected 600 estimates for 50min/5s: {path}, got {len(rows)}")
    t = [datetime.fromisoformat(r["time_utc"]) for r in rows]
    b = [float(r["time_weighted"]) for r in rows]
    if not all(math.isfinite(v) for v in b):
        raise ValueError(f"Non-finite estimate: {path}")
    if any(t[i] - t[i-1] != timedelta(seconds=5) for i in range(1,len(t))):
        raise ValueError(f"Unexpected rolling update clock: {path}")
    if any(abs(float(r["valid_time_fraction"])-1) > 1e-9 for r in rows):
        raise ValueError(f"Quotes missing from no-age-filter case: {path}")
    # Rounding the signed baseline level to exchange tick prices makes the
    # whole-tick transition count transparent and distinct from raw frac-ticks.
    rounded = [math.floor(v/tick+0.5) for v in b]
    raw = [(b[i]-b[i-1])/tick for i in range(1,len(b))]
    discrete = [rounded[i]-rounded[i-1] for i in range(1,len(b))]
    a = [abs(v) for v in raw]
    ad = [abs(v) for v in discrete]
    return {
        "day":t[0].date().isoformat(),"tick_usdt":tick,
        "first_utc":t[0].isoformat(),"last_utc":t[-1].isoformat(),
        "rolling_estimates":len(b),"consecutive_changes":len(raw),
        "raw_abs_tick_equivalents":{
            "median":percentile(a,.5),"mean":sum(a)/len(a),
            "p90":percentile(a,.9),"p95":percentile(a,.95),
            "p99":percentile(a,.99),"maximum":max(a),
            "cumulative_abs":sum(a),
            "updating_up":sum(x>1e-10 for x in raw),
            "updating_down":sum(x< -1e-10 for x in raw),
            "at_least_half_tick":sum(x>=.5-1e-9 for x in a),
            "at_least_one_tick":sum(x>=1-1e-9 for x in a),
            "at_least_two_ticks":sum(x>=2-1e-9 for x in a),
        },
        "rounded_baseline_tick_levels":{
            "updates_with_price_tick_change":sum(x>0 for x in ad),
            "updates_without_change":sum(x==0 for x in ad),
            "cumulative_abs_integer_ticks":sum(ad),
            "one_tick_transitions":sum(x==1 for x in ad),
            "two_tick_transitions":sum(x==2 for x in ad),
            "three_or_more_tick_transitions":sum(x>=3 for x in ad),
            "maximum_single_change_ticks":max(ad),
            "fraction_updates_with_change":sum(x>0 for x in ad)/len(ad),
        },
    }


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--input",default="reports/visualizations/binance_gate_basis_600s_5s_no_age")
    p.add_argument("--output",default="reports/visualizations/gate_basis_tick_movement")
    p.add_argument("--tick",type=float,default=.1)
    args=p.parse_args()
    root=Path(args.input)
    dst=Path(args.output);dst.mkdir(parents=True,exist_ok=True)
    days=("2026-09-01","2026-10-01","2026-11-01")
    result=[one_day(root/f"rolling_basis_{day}_50min.csv",args.tick) for day in days]
    (dst/"summary.json").write_text(
        json.dumps(result,indent=2,ensure_ascii=False)+"\n",encoding="utf-8")
    lines=[
        "# Gate BTC_USDT rolling historical cross-venue basis: tick movement",
        "",
        "Selected estimator: `time_weighted`, Binance minus Gate midprice,",
        "strictly previous 600s, recomputed every 5s, without quote-age exclusion.",
        "Binance collector timestamps have an extra assumed +2.5ms visibility delay",
        "in the upstream CSV. Gate BBO is Tardis L2-reconstructed quotes.",
        "Assumed Gate order price tick = **0.1 USDT per BTC**",
        "(Gate API contract property `order_price_round`).",
        "Gate documentation: https://www.gate.com/docs/apiv4/index.html",
        "The historical 2026-09 to 11 instrument tick is assumed unchanged; ",
        "validate historical contract configuration before final paper use.",
        "",
        "10min prehistory, then UTC 00:10:00..00:59:55, 50min at 5s cadence:",
        "600 computed basis estimates and **599 adjacent transitions per day**.",
        "Continuous absolute change is `abs(B_t - B_(t-5s)) / 0.1` and may",
        "be fractional. Integer quote tick level is `floor(B_t / 0.1 + 0.5)`.",
        "Integer transition is the absolute difference of adjacent levels.",
        "**These are basis changes, not actual maker-order amendments or cancels.**",
        "",
        "| Day | Median abs change (tick equiv.) | P95 abs change | Max abs change | Changes >=1 tick | Changes >=0.5 tick |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for x in result:
        r=x["raw_abs_tick_equivalents"]
        lines.append(f"| {x['day']} | {r['median']:.3f} | {r['p95']:.3f} | {r['maximum']:.3f} | {r['at_least_one_tick']}/599 | {r['at_least_half_tick']}/599 |")
    lines.extend(["","| Day | Rounded-level changed updates | 1-tick transitions | 2-tick transitions | >=3-tick transitions | Cumulative abs integer ticks |",
                  "|---|---:|---:|---:|---:|---:|"])
    for x in result:
        r=x["rounded_baseline_tick_levels"]
        lines.append(f"| {x['day']} | {r['updates_with_price_tick_change']}/599 | {r['one_tick_transitions']} | {r['two_tick_transitions']} | {r['three_or_more_tick_transitions']} | {r['cumulative_abs_integer_ticks']} |")
    lines.extend(["","Data sources:",
        " - [September through November 5-second rolling baseline CSVs](../binance_gate_basis_600s_5s_no_age/)",
        " - Computed on existing true Tardis data, without downloading a new source.",
        " - Source CSV has `valid_time_fraction=1` for all evaluated windows.",
        " - A 5-second estimate update can cross a tick boundary even when",
        "   the raw mean changes by less than one full tick.",
        " - A common Gate quote level is a separate decision from GLFT inventory,",
        "   model spread, and actual amend/cancel execution."])
    (dst/"README.md").write_text("\n".join(lines)+"\n",encoding="utf-8")
    for x in result:
        print("GATE_TICK_STATS",json.dumps(x,ensure_ascii=False),flush=True)


if __name__=="__main__":
    main()
