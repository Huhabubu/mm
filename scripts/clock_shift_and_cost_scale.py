"""BTC native BBO clock-shift falsification and OKX quote economics.

The underlying Binance price events are fixed in file order. Changing all
event receive times by a constant models an unknown relative collector clock
offset (small shifts) or a deliberately broken relationship (60-second placebo).
Negative offsets use future Binance information and are NON-CAUSAL diagnostics.
All source files are temporary; only aggregated metrics are published.
"""
import argparse
import csv
import gzip
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from binance_price_events import select_binance_events
from inspect_tardis import download
from predict_leadlag import read_quotes
from symmetric_price_event_response import evaluate_direction

SHIFTS_MS = (-60000, -50, -25, 0, 25, 50, 100, 1000, 60000)
HORIZONS_TO_SHOW_MS = (20, 100, 500, 1000, 2000)
OKX_MAKER_BPS_SCENARIO = 2.0
OKX_TAKER_BPS_SCENARIO = 5.0


def shifted_events(events, shift_ms):
    """Shift every selected Binance event, preserving direction and magnitude."""
    return {
        name: {
            "t": group["t"] + np.int64(shift_ms * 1000),
            "sign": group["sign"],
            "impact_bps": group["impact_bps"],
        }
        for name, group in events.items()
    }


def _describe(a):
    a = np.asarray(a, dtype=np.float64)
    if not len(a):
        return {"count": 0, "p10": None, "median": None, "p90": None, "mean": None}
    return {"count": int(len(a)), "p10": float(np.percentile(a, 10)),
            "median": float(np.median(a)), "p90": float(np.percentile(a, 90)),
            "mean": float(np.mean(a))}


def quote_economics(path, start_us):
    """One-second fixed UTC grid with past-only OKX BBO, plus observed price step.

    The observed minimum positive BBO price change is an EMPIRICAL estimate
    and is never called the exchange's historical official tickSz.
    """
    end_us = start_us + 86_400_000_000
    checkpoints = range(start_us + 1_000_000, end_us, 1_000_000)
    checkpoints_iter = iter(checkpoints)
    next_check = next(checkpoints_iter, None)
    prior = None
    prior_bid = prior_ask = None
    minimum_positive_step = None
    total_rows = 0
    halfspread_bps, mids, full_spread_bps, positive_gaps = [], [], [], 0
    invalid = 0
    with gzip.open(path, "rt", newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        required = {"local_timestamp", "bid_price", "ask_price"}
        if not required.issubset(set(reader.fieldnames or [])):
            raise ValueError(f"Missing OKX native BBO columns: {required - set(reader.fieldnames or [])}")
        for row in reader:
            total_rows += 1
            t = int(row["local_timestamp"])
            bid, ask = float(row["bid_price"]), float(row["ask_price"])
            if not (start_us <= t < end_us and 0 < bid < ask):
                invalid += 1
                continue
            while next_check is not None and next_check < t:
                if prior is not None:
                    pbid, pask = prior
                    mid = (pbid + pask) / 2
                    mids.append(mid)
                    half = 1e4 * (pask - pbid) / (pbid + pask)
                    halfspread_bps.append(half)
                    full_spread_bps.append(2 * half)
                next_check = next(checkpoints_iter, None)
            for last, now in ((prior_bid, bid), (prior_ask, ask)):
                if last is None:
                    continue
                # Round to eight decimal places to avoid float subtraction noise.
                delta = round(abs(now - last), 8)
                if delta > 0:
                    positive_gaps += 1
                    if minimum_positive_step is None or delta < minimum_positive_step:
                        minimum_positive_step = delta
            prior = (bid, ask)
            prior_bid, prior_ask = bid, ask
    while next_check is not None:
        if prior is not None:
            pbid, pask = prior
            mid = (pbid + pask) / 2
            mids.append(mid)
            half = 1e4 * (pask - pbid) / (pbid + pask)
            halfspread_bps.append(half)
            full_spread_bps.append(2 * half)
        next_check = next(checkpoints_iter, None)
    if not mids or minimum_positive_step is None:
        raise ValueError("No usable OKX midpoint snapshots or observed increments")
    median_mid = float(np.median(mids))
    tick_bps = 1e4 * minimum_positive_step / median_mid
    half = _describe(halfspread_bps)
    return {
        "rows": total_rows,
        "invalid_rows": invalid,
        "snapshot_definition": "UTC 1-second grid, previous or equal native OKX BBO",
        "snapshots": len(mids),
        "median_mid_usdt": median_mid,
        "minimum_observed_positive_quote_price_step_usdt": minimum_positive_step,
        "positive_bid_or_ask_price_updates": positive_gaps,
        "empirical_step_in_bps_at_median_mid": tick_bps,
        "half_spread_bps": half,
        "full_spread_bps": _describe(full_spread_bps),
        "median_halfspread_in_empirical_steps": (
            half["median"] / tick_bps if tick_bps > 0 else None
        ),
        "fee_assumption": {
            "source": "OKX published Lv1 contract fee example, not verified account-specific 2026 history",
            "maker_one_leg_bps": OKX_MAKER_BPS_SCENARIO,
            "taker_one_leg_bps": OKX_TAKER_BPS_SCENARIO,
            "maker_maker_round_trip_bps": 2 * OKX_MAKER_BPS_SCENARIO,
            "maker_taker_round_trip_bps": OKX_MAKER_BPS_SCENARIO + OKX_TAKER_BPS_SCENARIO,
        },
        "interpretation": (
            "Quotes and event-conditional midprice changes are not realized fills, "
            "spread captured, funding, net PnL or markouts conditioned on actual fills."
        ),
    }


def analyze_day(day, base_dir):
    t0 = int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1e6)
    paths = {}
    for venue, exchange, symbol in (
        ("binance", "binance-futures", "BTCUSDT"),
        ("okx", "okex-swap", "BTC-USDT-SWAP"),
    ):
        path = Path(base_dir) / day / f"{exchange}_{symbol}_book_ticker.csv.gz"
        if not path.exists():
            print(f"Download {day} {venue} BBO", flush=True)
            download(exchange, symbol, "book_ticker", day, path)
        paths[venue] = path
    print(f"Select Binance events for {day}", flush=True)
    events, counts = select_binance_events(paths["binance"])
    print(f"Read OKX BBO for {day}", flush=True)
    okx = read_quotes(paths["okx"])
    results = {}
    for shift in SHIFTS_MS:
        v = evaluate_direction(shifted_events(events, shift), okx, t0)
        results[str(shift)] = {
            "events_eligible": v["events_eligible"],
            "source_abs_change_mean_bps": v["source_event_absolute_change_mean_bps"],
            "target_quote_age_median_ms": v["target_quote_age_median_ms"],
            "mean_signed_response_bps": {
                str(h): v["responses"][str(h)]["mean_signed_response_bps"]
                for h in HORIZONS_TO_SHOW_MS
            },
            "first_mid_move_100ms_pct": v["target_first_mid_move_within_100ms_pct"],
        }
        print(f"  Shift {shift:+7d}ms: n={v['events_eligible']:,}, "
              f"future100ms={v['responses']['100']['mean_signed_response_bps']:+.5f}bps", flush=True)
    del okx
    print(f"Profile OKX 1-second snapshots for {day}", flush=True)
    cost = quote_economics(paths["okx"], t0)
    print(f"  OKX empirical min price step={cost['minimum_observed_positive_quote_price_step_usdt']} "
          f"USDT, median halfspread={cost['half_spread_bps']['median']:.4f}bps", flush=True)
    return {"binance_event_counts": counts, "shifts": results, "okx_quote_cost_scale": cost}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", default="2026-06-01,2026-07-01,2026-08-01,2026-09-01,2026-10-01,2026-11-01")
    ap.add_argument("--data-dir", default="data/raw")
    ap.add_argument("--report-dir", default="reports")
    args = ap.parse_args()
    days = [s.strip() for s in args.days.split(",") if s.strip()]
    if not days or any(datetime.strptime(day, "%Y-%m-%d").day != 1 for day in days):
        ap.error("Expected free monthly UTC YYYY-MM-01 dates")
    result = {
        "status": "started", "days": days, "asset": "BTC",
        "method": "Shift all selected Binance event receive timestamps, keep original signs, match OKX native BBO past-only",
        "clock_sensitivity_ms": [-50, -25, 0, 25, 50, 100],
        "placebo_shifts_ms": [-60000, 1000, 60000],
        "horizons_ms": HORIZONS_TO_SHOW_MS,
        "limitations": [
            "Shifts <0 use a future Binance signal relative to the assigned time; diagnostic only, never executable.",
            "60s forward/backward shifts intentionally break local temporal correspondence; near-zero response is a falsification check, not proof of causal leadership.",
            "Shifting selected events keeps original event identities and signs; not a new raw-feed resampling experiment.",
            "Receiver clock shift does not model order entry, cancel acknowledgement, matched fills, or venue latency paths.",
            "1-second quote grid estimates spread scale only. Empirical min price increment is not historical official tickSz.",
            "OKX Lv1 maker/taker fee is a published illustrative tier; actual historical fees/account discounts must be verified.",
            "Event-conditioned mid responses are not executed position returns or maker adverse-selection markouts.",
        ],
        "data": {},
    }
    out = Path(args.report_dir) / "btc_clock_shift_and_spread_scale_2026.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    try:
        for day in days:
            result["data"][day] = analyze_day(day, args.data_dir)
        result["status"] = "complete"
    except Exception as e:
        result["status"] = "failed"
        result["error"] = f"{type(e).__name__}: {e}"
        raise
    finally:
        out.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"Saved {out}; status={result['status']}", flush=True)


if __name__ == "__main__":
    main()
