"""Causal comparison of four 600s Binance/Gate basis estimators.

Same 5s update clock, strict-past window, event union and quote-age rule.
Uses native Binance book_ticker and Gate L2-reconstructed quotes.
This is an estimation diagnostic; not an HBT trading backtest.
"""
import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from statistics import NormalDist

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from plot_binance_gate_event_mids import read_mid, timestamp_us

WINDOW_US = 600_000_000
UPDATE_US = 5_000_000
AGE_US = 250_000
GAUSSIAN_Z = NormalDist().inv_cdf(0.985)  # nominal central 97%, two-sided
KEYS = ("time_weighted", "one_second", "event_trim_gauss97", "event_equal")
COLORS = ("#202124", "#2277c9", "#d9544d", "#299b65")
LABELS = ("Time weighted", "1-second equally weighted",
          "Event equally weighted + normal 97% trim", "Event equally weighted")


class Tape:
    def __init__(self, bt, bm, gt, gm):
        self.bt, self.bm = np.asarray(bt, np.int64), np.asarray(bm, float)
        self.gt, self.gm = np.asarray(gt, np.int64), np.asarray(gm, float)
        if (not len(bt) or not len(gt) or
                np.any(np.diff(self.bt) <= 0) or np.any(np.diff(self.gt) <= 0)):
            raise ValueError("Both streams must have strictly increasing unique timestamps")
        # For any timestamp on EITHER exchange, use latest quote with timestamp <= event.
        self.t = np.union1d(self.bt, self.gt)
        ib = np.searchsorted(self.bt, self.t, "right") - 1
        ig = np.searchsorted(self.gt, self.t, "right") - 1
        both = (ib >= 0) & (ig >= 0)
        self.t = self.t[both]
        ib, ig = ib[both], ig[both]
        self.d = self.bm[ib] - self.gm[ig]
        self.fresh = ((self.t-self.bt[ib] <= AGE_US) &
                      (self.t-self.gt[ig] <= AGE_US))
        # A quote becomes invalid 250ms after its latest update, including
        # even if neither feed sends any further messages during that interval.
        next_event = np.r_[self.t[1:], np.iinfo(np.int64).max]
        self.valid_end = np.minimum.reduce(
            (next_event, self.bt[ib] + AGE_US, self.gt[ig] + AGE_US))

    def time_integral(self, lo, hi):
        """Piecewise-constant integral, expires both quote states at 250ms."""
        if hi <= lo:
            raise ValueError("Invalid time interval")
        i = max(int(np.searchsorted(self.t, lo, "right")) - 1, 0)
        j = int(np.searchsorted(self.t, hi, "left"))
        if i >= j:
            return float("nan"), 0.
        lower = np.maximum(self.t[i:j], lo)
        upper = np.minimum(self.valid_end[i:j], hi)
        dt = np.maximum(upper - lower, 0).astype(float)
        eligible_us = float(np.sum(dt))
        return (float(np.dot(dt, self.d[i:j]) / eligible_us)
                if eligible_us else float("nan")), eligible_us / (hi-lo)

    def sampled_1s(self, lo, hi):
        grid = np.arange(lo, hi, 1_000_000, dtype=np.int64)
        if len(grid) != 600:
            raise ValueError("Expected exactly 600 one-second samples")
        ib = np.searchsorted(self.bt, grid, "left") - 1  # strictly past
        ig = np.searchsorted(self.gt, grid, "left") - 1
        b = np.maximum(ib, 0)
        g = np.maximum(ig, 0)
        good = ((ib >= 0) & (ig >= 0) &
                (grid-self.bt[b] <= AGE_US) & (grid-self.gt[g] <= AGE_US))
        delta = self.bm[b] - self.gm[g]
        return (float(np.mean(delta[good])) if np.any(good)
                else float("nan")), int(np.count_nonzero(good))

    def estimate_at(self, now):
        lo = int(now-WINDOW_US)
        # Every event t in [lo, now); no future event can enter a mean.
        left = int(np.searchsorted(self.t, lo, "left"))
        right = int(np.searchsorted(self.t, now, "left"))
        values = self.d[left:right][self.fresh[left:right]]
        n = len(values)
        e_mean = float(np.mean(values)) if n else float("nan")
        if n:
            e_std = float(np.std(values))
            keep = (np.abs(values-e_mean) <= GAUSSIAN_Z*e_std
                    if e_std else np.ones(n, bool))
            e_trim = float(np.mean(values[keep])) if np.any(keep) else float("nan")
            trimmed = int(n-np.count_nonzero(keep))
        else:
            e_trim, trimmed = float("nan"), 0
        tw, frac = self.time_integral(lo, now)
        sec, sec_n = self.sampled_1s(lo, now)
        return {
            "time_weighted": tw,
            "one_second": sec,
            "event_trim_gauss97": e_trim,
            "event_equal": e_mean,
            "valid_time_fraction": frac,
            "valid_seconds": sec_n,
            "valid_event_count": n,
            "trimmed_event_count": trimmed,
            "trimmed_fraction": trimmed/n if n else float("nan"),
            "coverage_75pct": bool(frac >= .75 and sec_n >= 450),
        }


def stats(rows, key, require_coverage=False):
    ok = (lambda r: r["coverage_75pct"]) if require_coverage else (lambda r: True)
    pairs = [(i, r[key]) for i,r in enumerate(rows) if ok(r) and np.isfinite(r[key])]
    vals = np.array([v for _,v in pairs], dtype=float)
    diffs = np.array([pairs[j][1]-pairs[j-1][1]
                      for j in range(1,len(pairs))
                      if pairs[j][0] == pairs[j-1][0]+1], dtype=float)
    return {
        "observations":len(vals), "eligible_adjacent_5s_pairs":len(diffs),
        "level_std_usdt":float(np.std(vals)) if len(vals) else None,
        "median_abs_5s_change_usdt":float(np.median(np.abs(diffs))) if len(diffs) else None,
        "p95_abs_5s_change_usdt":float(np.quantile(np.abs(diffs),.95)) if len(diffs) else None,
        "max_abs_5s_change_usdt":float(np.max(np.abs(diffs))) if len(diffs) else None,
    }


def plot(day, rows, path):
    x = np.array([(r["utc_us"]-rows[0]["utc_us"])/60_000_000. for r in rows])
    fig, (top, bottom) = plt.subplots(
        2,1,figsize=(15,7.5), sharex=True,
        gridspec_kw={"height_ratios":[3.4,1.0]}, constrained_layout=True)
    for key, label, color in zip(KEYS,LABELS,COLORS):
        top.plot(x,[r[key] for r in rows],color=color,label=label,lw=1.15)
    top.set_ylabel("Historical mean Binance - Gate (USDT/BTC)")
    top.grid(alpha=.25)
    top.legend(ncol=2,fontsize=9)
    bottom.plot(x,[r["valid_time_fraction"] for r in rows],
                color="#ad7900",lw=1.1,label="Both quotes fresh: valid time")
    bottom.axhline(.75,color="#a33b3b",ls="--",lw=1,label="75% coverage threshold")
    bottom.set_ylim(0,1.02)
    bottom.set_ylabel("Valid time fraction")
    bottom.set_xlabel("Minutes after 00:10 UTC (each baseline updated every 5 seconds)")
    bottom.grid(alpha=.25)
    bottom.legend(fontsize=8)
    fig.suptitle(
        f"Binance / Gate BTC: four 600-second rolling basis estimators | {day} UTC\n"
        "10-minute warmup, 50-minute observation, 5-second refit",
        fontsize=12)
    fig.savefig(path,dpi=165,facecolor="white")
    plt.close(fig)


def run_day(day, output, delay_ms, observation_minutes):
    day0 = timestamp_us(day,"00:00:00")
    first = day0+WINDOW_US
    end = first+observation_minutes*60_000_000
    print("BASIS_START",day,flush=True)
    bt,bm,bin_meta = read_mid("binance-futures","book_ticker","BTCUSDT",day,end)
    gt,gm,gate_meta = read_mid("gate-io-futures","quotes","BTC_USDT",day,end)
    # In the live strategy, Binance becomes visible only after its
    # extra assumed transport delay. Gate uses its measured local clock.
    bt = bt + int(round(delay_ms*1000))+1
    tape = Tape(bt,bm,gt,gm)
    rows=[]
    for now in range(first,end,UPDATE_US):
        r=tape.estimate_at(now)
        r["utc_us"]=now
        # Future window used only for diagnostics, never as a strategy input.
        future, fut_coverage = tape.time_integral(now,now+UPDATE_US)
        r["next_5s_timeweighted_spread"]=future
        r["next_5s_coverage"]=fut_coverage
        rows.append(r)
    csvname=f"rolling_basis_{day}_{observation_minutes}min.csv"
    png=f"rolling_basis_{day}_{observation_minutes}min.png"
    with (output/csvname).open("w",newline="",encoding="utf-8") as f:
        writer=csv.DictWriter(f,fieldnames=["time_utc",*KEYS,"valid_time_fraction",
            "valid_seconds","valid_event_count","trimmed_event_count",
            "trimmed_fraction","coverage_75pct","next_5s_timeweighted_spread",
            "next_5s_coverage"])
        writer.writeheader()
        for row in rows:
            d={"time_utc":datetime.fromtimestamp(row["utc_us"]/1e6,timezone.utc).isoformat()}
            d.update({k:row[k] for k in writer.fieldnames if k in row})
            writer.writerow(d)
    plot(day,rows,output/png)
    # Short-term forecast is diagnostic only; do not call it normal-spread
    # ground truth or conclude anything about net making profit.
    predictive={}
    for key in KEYS:
        pairs=[(r[key],r["next_5s_timeweighted_spread"]) for r in rows
               if r["coverage_75pct"] and r["next_5s_coverage"]>=.75
               and np.isfinite(r[key]) and np.isfinite(r["next_5s_timeweighted_spread"])]
        predictive[key]={"n":len(pairs),
          "next_5s_spread_mae_usdt":float(np.mean([abs(a-b) for a,b in pairs])) if pairs else None}
    result={
        "date":day,"window_start_utc":"00:10:00",
        "observation_minutes":observation_minutes,
        "lookback_seconds":600,"refit_seconds":5,
        "binance_extra_visible_delay_ms":delay_ms,
        "max_quote_age_ms":250,"normal_97pct_two_sided_sigma":GAUSSIAN_Z,
        "n_estimates":len(rows),
        "fraction_windows_meeting_75pct_time_and_second_coverage":
            float(np.mean([r["coverage_75pct"] for r in rows])),
        "coverage_time_median":float(np.median([r["valid_time_fraction"] for r in rows])),
        "coverage_time_min":float(np.min([r["valid_time_fraction"] for r in rows])),
        "coverage_seconds_median":float(np.median([r["valid_seconds"] for r in rows])),
        "trim_fraction_median":float(np.median([r["trimmed_fraction"] for r in rows])),
        "trim_fraction_max":float(np.max([r["trimmed_fraction"] for r in rows])),
        "metrics_all_observations":{k:stats(rows,k) for k in KEYS},
        "metrics_only_high_coverage":{k:stats(rows,k,True) for k in KEYS},
        "next_5s_descriptive_mae_only_high_coverage":predictive,
        "binance_source":bin_meta,"gate_source":gate_meta,
        "png":png,"csv":csvname,
    }
    print("BASIS_RESULT",json.dumps({k:result[k] for k in result
          if k not in ("binance_source","gate_source")}),flush=True)
    return result


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--days",nargs="+",default=["2026-09-01","2026-10-01","2026-11-01"])
    p.add_argument("--out",default="reports/visualizations/binance_gate_basis_600s_5s")
    p.add_argument("--binance-delay-ms",type=float,default=2.5)
    p.add_argument("--minutes",type=int,default=50)
    args=p.parse_args()
    if args.minutes<=0 or args.minutes>1430 or args.binance_delay_ms<0:
        p.error("invalid minutes or delay")
    root=Path(args.out);root.mkdir(parents=True,exist_ok=True)
    result=[run_day(d,root,args.binance_delay_ms,args.minutes) for d in args.days]
    (root/"summary.json").write_text(json.dumps(result,indent=2,ensure_ascii=False)+"\n",encoding="utf-8")
    readme=(
        "# Binance–Gate historical basis: four methods, matched scales\n\n"
        f"BTC USDT perpetual, {args.minutes}min measurement following first 10min warmup. "
        "All four methods use a strictly-past 600s window and recompute each 5s. "
        f"Binance BBO has an assumed +{args.binance_delay_ms}ms visibility delay; "
        "Gate uses L2-reconstructed quotes without added delay. "
        "Event timeline is the union of both feeds, with latest historically visible "
        "value at each event. Rows at the same microsecond are reduced to the final state.\n\n"
        "A: piecewise-constant time-weighted mean, with both BBOs expiring at 250ms. "
        "B: 600 fixed 1-second points, strict-past as-of, equal weight. "
        "C: equally weighted event samples, compute event mean and population sigma, "
        "discard points outside mean ±2.17009 sigma (nominal central Gaussian 97%), "
        "then recompute mean on retained points; actual rejection proportion is recorded. "
        "D: equally weighted event samples without trimming.\n\n"
        "All methods exclude samples where either quote is older than 250ms. "
        "CSV records validity and a common 75% eligibility diagnostic, rather than "
        "silently extrapolating stale quotes. Plots show even low-coverage estimates "
        "with valid-time fraction in the lower panel. Only eligible windows enter "
        "the high-coverage stability comparison. Future 5s spread MAE is a descriptive "
        "diagnostic only; future samples are never used in estimator construction. "
        "No trading simulation or fee-inclusive PnL is performed.\n\n"
        + "\n".join(f"- {r['date']}: [{r['png']}]({r['png']}), "
                      f"[5s series]({r['csv']})" for r in result) + "\n"
    )
    (root/"README.md").write_text(readme,encoding="utf-8")


if __name__=="__main__":
    main()
