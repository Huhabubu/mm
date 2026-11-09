"""Symmetric venue-price event study: Binance→OKX versus OKX→Binance.

This is a diagnostic comparison only; it cannot by itself resolve whether
Tardis collectors' different receive paths create measured lead–lag.
Only aggregate results are uploaded from ephemeral Actions runners.
"""
import argparse
import gc
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from binance_price_events import select_binance_events
from inspect_tardis import download
from predict_leadlag import read_quotes

DATES = tuple(f"2026-{month:02d}-01" for month in range(6, 12))
HORIZONS_MS = (20, 50, 100, 200, 500, 1000, 2000)
DELAYS_MS = (0, 25, 50)
AGE_LIMIT_US = 2_000_000


def merged_price_events(events):
    """Use the same 20ms first-price-change rule for BOTH venues."""
    tt = np.concatenate((events["up"]["t"], events["down"]["t"]))
    signs = np.concatenate((events["up"]["sign"], events["down"]["sign"]))
    impacts = np.concatenate((events["up"]["impact_bps"], events["down"]["impact_bps"]))
    seq = np.argsort(tt, kind="stable")
    return tt[seq], signs[seq], impacts[seq]


def evaluate_direction(source_events, target, start_us, delay_ms=0):
    """Event responses from delayed decision time, preserving strict target history.

    Source events are already frozen at their original event time.
    Target messages with the same microsecond as the decision are omitted
    because cross-collector ordering inside that microsecond is ambiguous.
    """
    tt, sign, impact = merged_price_events(source_events)
    times, mid = target["t"], target["mid"]
    decision_t = tt + int(delay_ms * 1000)
    before = np.searchsorted(times, decision_t, side="left") - 1
    after_at_t = np.searchsorted(times, decision_t, side="right")
    exact_ties = after_at_t > (before + 1)
    mask = (
        (decision_t >= start_us + 1_000_000)
        & (decision_t + max(HORIZONS_MS) * 1_000 < start_us + 86_400_000_000)
        & (before >= 0) & (~exact_ties)
    )
    age = decision_t - times[np.maximum(before, 0)]
    mask &= (age >= 0) & (age <= AGE_LIMIT_US)
    decision_t, sign, impact, before, age = (
        a[mask] for a in (decision_t, sign, impact, before, age)
    )
    if len(decision_t) == 0:
        raise ValueError("No eligible price events after target availability checks")
    signed_change = {}
    for h in HORIZONS_MS:
        future = np.searchsorted(times, decision_t + h * 1_000, side="right") - 1
        r = 10_000 * (mid[future] / mid[before] - 1) * sign
        active = np.abs(r) >= 0.1
        signed_change[str(h)] = {
            "mean_signed_response_bps": float(np.mean(r)),
            "median_signed_response_bps": float(np.median(r)),
            "mean_absolute_response_bps": float(np.mean(np.abs(r))),
            "nontrivial_response_pct": 100 * float(np.mean(active)),
            "same_direction_pct_all": 100 * float(np.mean(r > 0)),
            "same_direction_pct_nontrivial": (
                100 * float(np.mean(r[active] > 0)) if np.any(active) else None
            ),
        }

    # First distinct target midprice change after the source event,
    # not the next quantity-only BBO update. Never use this as a feature.
    changed = np.flatnonzero(mid[1:] != mid[:-1]) + 1
    next_ix = np.searchsorted(times[changed], decision_t, side="right")
    found = next_ix < len(changed)
    first_ix = changed[np.minimum(next_ix, len(changed) - 1)]
    first_lag = times[first_ix] - decision_t
    within_100 = found & (first_lag > 0) & (first_lag <= 100_000)
    first_sign = np.sign(mid[first_ix] - mid[first_ix - 1])
    return {
        "events_selected_source": int(len(tt)),
        "events_eligible": int(len(decision_t)),
        "events_excluded_simultaneous": int(np.count_nonzero(exact_ties)),
        "source_event_absolute_change_mean_bps": float(np.mean(np.abs(impact))),
        "source_event_absolute_change_median_bps": float(np.median(np.abs(impact))),
        "target_quote_age_median_ms": float(np.median(age)) / 1_000,
        "target_first_mid_move_within_100ms_pct": 100 * float(np.mean(within_100)),
        "target_first_move_agrees_with_source_pct_all": 100 * float(np.mean(within_100 & (first_sign == sign))),
        "target_first_move_agrees_pct_conditional_moved": (
            100 * float(np.mean(first_sign[within_100] == sign[within_100]))
            if np.any(within_100) else None
        ),
        "target_first_move_latency_median_ms_conditional_moved": (
            float(np.median(first_lag[within_100])) / 1000 if np.any(within_100) else None
        ),
        "responses": signed_change,
    }


def analyze_day(day, data_dir):
    t0 = int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1e6)
    paths = {}
    for name, exch, symbol in (
        ("binance", "binance-futures", "BTCUSDT"),
        ("okx", "okex-swap", "BTC-USDT-SWAP"),
    ):
        p = Path(data_dir) / day / f"{exch}_{symbol}_book_ticker.csv.gz"
        if not p.exists():
            print(f"Downloading {day} {name} native book_ticker", flush=True)
            download(exch, symbol, "book_ticker", day, p)
        paths[name] = p

    print(f"Selecting price events on both venues: {day}", flush=True)
    selected = {}
    counts = {}
    for name in ("binance", "okx"):
        selected[name], counts[name] = select_binance_events(paths[name])
        print(f"  {name} raw changes {counts[name]['raw_price_changes']:,}, "
              f"selected {counts[name]['selected_up'] + counts[name]['selected_down']:,}",
              flush=True)

    # Read one target venue at a time to cap peak memory, especially for
    # Binance with tens of millions of native BBO rows.
    result = {"event_counts": counts, "comparisons": {}}
    for source_name, target_name, label in (
        ("binance", "okx", "binance_to_okx"),
        ("okx", "binance", "okx_to_binance"),
    ):
        print(f"Reading {target_name} target and evaluating {label}: {day}", flush=True)
        target = read_quotes(paths[target_name])
        result["comparisons"][label] = {
            str(d): evaluate_direction(selected[source_name], target, t0, delay_ms=d)
            for d in DELAYS_MS
        }
        del target
        gc.collect()
    return result


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--days", default=",".join(DATES))
    p.add_argument("--data-dir", default="data/raw")
    p.add_argument("--report-dir", default="reports")
    args = p.parse_args()
    days = tuple(x.strip() for x in args.days.split(",") if x.strip())
    if not days or any(datetime.strptime(x, "%Y-%m-%d").day != 1 for x in days):
        p.error("Use comma-separated free Tardis YYYY-MM-01 dates")
    report = {
        "status": "started",
        "description": "Symmetric first price-change in 20ms bin; native BBO, directionally signed target response",
        "days": days, "horizons_ms": HORIZONS_MS, "delays_ms": DELAYS_MS,
        "max_target_quote_age_ms": AGE_LIMIT_US / 1000,
        "data": {},
        "limitations": [
            "Reverse and forward directions have different source-event count and jump-size distributions.",
            "Positive response in both directions does not establish information leadership.",
            "Both venues use separate Tardis receiver paths despite same Tokyo cloud region.",
            "25/50ms is hypothetical additional delay, not measured real execution latency.",
            "Overlapping events are strongly dependent; six dates, not millions of independent samples.",
            "No fills, no fees, no GLFT execution simulation.",
        ],
    }
    outfile = Path(args.report_dir) / "btc_symmetric_price_event_response_2026_06_to_11.json"
    outfile.parent.mkdir(parents=True, exist_ok=True)
    try:
        for d in days:
            report["data"][d] = analyze_day(d, args.data_dir)
            for direction in ("binance_to_okx", "okx_to_binance"):
                v = report["data"][d]["comparisons"][direction]["0"]
                print(f"  {d} {direction}: eligible {v['events_eligible']:,}, "
                      f"100ms signed response {v['responses']['100']['mean_signed_response_bps']:+.5f}bps, "
                      f"500ms {v['responses']['500']['mean_signed_response_bps']:+.5f}bps",
                      flush=True)
            # Allow parsing and scanning to be repeated with bounded memory.
            gc.collect()
        report["status"] = "complete"
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        outfile.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Saved {outfile}, status={report['status']}", flush=True)


if __name__ == "__main__":
    main()
