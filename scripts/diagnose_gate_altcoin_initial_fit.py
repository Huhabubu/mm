"""Read-only Gate SOL/DOGE first-10-minute GLFT initialization diagnostic.

No strategy quotes, matching modifications, threshold changes, or fee-free PnL.
Reuse the exact original Tardis feeds, HBT event clock and fit_recent_arrivals.
Output all atomic acceptance conditions, including denominator and coefficients.
"""
import argparse
import json
import math
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
from numba import njit
from hftbacktest import BUY_EVENT, ROIVectorMarketDepthBacktest
from hftbacktest.data.utils import tardis
from hftbacktest.data.validation import validate_event_order

from check_okx_hft_data import SCHEMAS, require_unshifted_target_clock
from cross_venue_event_probe import read_binance_bbo, visible_timestamps
from gate_basis_weighted_moments import load_gate_mid
from g0_glft_baseline import TRAIN, STEP_NS
from g0_glft_rolling import (
    FIT_BINS, FIT_STEP_TICKS, MIN_CROSSING_INTERVALS,
    fit_recent_arrivals, glft_coefficients,
)
from qa_gate_btc_futures import fetch_contract
from run_gate_btc_origin_glft import (
    BINANCE_DELAY_MS, build_asset, safe_gate_csv, scenario,
)

SYMBOLS = ("SOL_USDT", "DOGE_USDT")


@njit
def replay_initial_observations(engine, binance_visible_ns, tick_size):
    # Identical original event clock and first-6000-sample observation rules.
    # Original strategy submits no orders until after the warmup, so this
    # diagnostic needs only an unchanged HBT replay with NO order submissions.
    if engine.elapse(0) != 0:
        raise ValueError("Cannot initialize official HBT replay")
    t0 = engine.current_timestamp
    next_sample = t0 + STEP_NS
    now = t0
    sample_count = 0
    idx = 0
    while idx < len(binance_visible_ns) and binance_visible_ns[idx] < t0:
        idx += 1

    arrivals = np.full(TRAIN, np.nan, np.float64)
    changes = np.full(TRAIN, np.nan, np.float64)
    prev_mid = np.nan
    gate_event_count = 0
    binance_event_count = 0

    while sample_count < TRAIN:
        next_binance = next_sample
        if idx < len(binance_visible_ns):
            next_binance = binance_visible_ns[idx]
        due = min(next_sample, next_binance)
        if due > now:
            status = engine.wait_next_feed(True, due - now)
            if status == 1:
                break
            now = engine.current_timestamp
            if now > due:
                raise ValueError("HBT clock overshot warmup sampling deadline")
            if status == 2:
                gate_event_count += 1
            elif status not in (0, 3):
                raise ValueError("Unexpected HBT event status")

        while idx < len(binance_visible_ns) and binance_visible_ns[idx] <= now:
            binance_event_count += 1
            idx += 1

        if now >= next_sample:
            if now != next_sample:
                raise ValueError("100ms sampling clock mismatch")
            slot = sample_count % TRAIN
            furthest = -np.inf
            if np.isfinite(prev_mid):
                for trade in engine.last_trades(0):
                    price_tick = trade.px / tick_size
                    if trade.ev & BUY_EVENT == BUY_EVENT:
                        furthest = max(furthest, price_tick - prev_mid)
                    else:
                        furthest = max(furthest, prev_mid - price_tick)
            engine.clear_last_trades(0)
            arrivals[slot] = furthest
            book = engine.depth(0)
            best_bid, best_ask = book.best_bid_tick, book.best_ask_tick
            if best_bid > 0 and best_ask > best_bid and best_ask < 1_000_000_000:
                mid_tick = (best_bid + best_ask) / 2.0
                changes[slot] = mid_tick - prev_mid if np.isfinite(prev_mid) else np.nan
                prev_mid = mid_tick
            else:
                changes[slot] = np.nan
                prev_mid = np.nan
            sample_count += 1
            next_sample += STEP_NS
    return (arrivals, changes, sample_count, t0, now,
            gate_event_count, binance_event_count)


def diagnose(arrivals, changes, spec):
    count = np.zeros(FIT_BINS, dtype=np.float64)
    qualifying = 0
    extreme = 0
    for depth in arrivals:
        if not np.isfinite(depth):
            continue
        if depth > FIT_BINS * FIT_STEP_TICKS + 1e-7:
            extreme += 1
        if depth <= FIT_STEP_TICKS + 1e-7:
            continue
        qualifying += 1
        for i in range(FIT_BINS):
            distance = (i + 1) * FIT_STEP_TICKS
            if depth > distance + 1e-7:
                count[i] += 1
            else:
                break
    positive = count > 0
    n = int(positive.sum())
    valid_change = changes[np.isfinite(changes)]
    change_count = len(valid_change)
    x = (np.arange(FIT_BINS, dtype=np.float64) + 1) * FIT_STEP_TICKS
    sx, sy, sxx, sxy, denominator = (None,)*5
    if n >= 1:
        xx = x[positive]
        yy = np.log(count[positive] / (TRAIN * STEP_NS * 1e-9))
        sx = float(xx.sum())
        sy = float(yy.sum())
        sxx = float((xx*xx).sum())
        sxy = float((xx*yy).sum())
        denominator = float(n*sxx - sx*sx)

    raw_ok, a, k, sigma, orig_levels, orig_cross, orig_extreme = fit_recent_arrivals(
        arrivals, changes)
    if (n != orig_levels or qualifying != orig_cross or extreme != orig_extreme):
        raise RuntimeError("Independent count audit differs from original estimator")
    checks = {
        "fit_levels_at_least_5": n >= 5,
        "trade_through_intervals_at_least_10": qualifying >= MIN_CROSSING_INTERVALS,
        "valid_mid_changes_at_least_3000": change_count >= TRAIN//2,
        "regression_denominator_positive": denominator is not None and denominator > 0,
        "A_positive_finite": bool(np.isfinite(a) and a > 0),
        "k_positive_gt_1e_minus_8_per_tick": bool(np.isfinite(k) and k > 1e-8),
        "sigma_positive_finite": bool(np.isfinite(sigma) and sigma > 0),
    }
    half = skew = None
    if raw_ok:
        h, s = glft_coefficients(a, k, sigma, spec["gamma_code"], spec["qty"])
        half, skew = float(h), float(s)
        checks["GLFT_half_and_skew_positive_finite"] = bool(
            np.isfinite(half) and np.isfinite(skew) and half > 0 and skew > 0)
    else:
        checks["GLFT_half_and_skew_positive_finite"] = False

    # Diagnose independent conditions even when original estimator short-circuits:
    # A/k/sigma may be NaN because the count/sample threshold gate fired first.
    fallback = {}
    if (n >= 2 and denominator is not None and denominator > 0):
        slope = (n*sxy - sx*sy)/denominator
        intercept = (sy - slope*sx)/n
        fallback["slope_per_tick"] = slope
        fallback["k_per_tick"] = -slope
        fallback["A_per_second"] = math.exp(intercept)
        fallback["first_5_levels_cross_counts"] = count[:5].tolist()
        fallback["last_5_levels_cross_counts"] = count[-5:].tolist()
    if change_count > 0:
        v = float(np.var(valid_change))
        fallback["sigma_tick_per_sqrt_second"] = math.sqrt(
            max(0.0, v)/(STEP_NS*1e-9))

    # Exact condition chain mirrors fit_recent_arrivals() in g0_glft_rolling.py.
    if n < 5:
        reason = "LESS_THAN_5_POSITIVE_DEPTH_BINS"
    elif qualifying < MIN_CROSSING_INTERVALS:
        reason = "LESS_THAN_10_TRADE_THROUGH_INTERVALS"
    elif change_count < TRAIN//2:
        reason = "LESS_THAN_3000_VALID_MIDPOINT_CHANGES"
    elif denominator is None or denominator <= 0:
        reason = "DEGENERATE_DEPTH_REGRESSION_DENOMINATOR"
    elif not (np.isfinite(a) and a > 0):
        reason = "NON_POSITIVE_OR_NON_FINITE_A"
    elif not (np.isfinite(k) and k > 1e-8):
        reason = "NON_POSITIVE_OR_NON_FINITE_K"
    elif not (np.isfinite(sigma) and sigma > 0):
        reason = "NON_POSITIVE_OR_NON_FINITE_SIGMA"
    elif not checks["GLFT_half_and_skew_positive_finite"]:
        reason = "NON_POSITIVE_OR_NON_FINITE_GLFT_COEFFICIENTS"
    else:
        reason = "ACCEPTED"

    if (reason == "ACCEPTED") != bool(raw_ok and checks["GLFT_half_and_skew_positive_finite"]):
        raise RuntimeError("Diagnostic interpretation disagrees with production estimator")
    return {
        "reason": reason, "accepted": reason == "ACCEPTED",
        "conditions": checks,
        "qualifying_intervals": qualifying,
        "crossed_distance_levels": n,
        "extreme_intervals_over_35_ticks": extreme,
        "valid_midpoint_changes": change_count,
        "max_trade_through_ticks": float(np.max(arrivals[np.isfinite(arrivals)]))
            if np.isfinite(arrivals).any() else None,
        "filled_depth_thresholds_count_min": float(count[positive].min()) if n else 0,
        "filled_depth_thresholds_count_max": float(count[positive].max()) if n else 0,
        "estimated_A_per_second": float(a) if np.isfinite(a) else None,
        "estimated_k_per_tick": float(k) if np.isfinite(k) else None,
        "estimated_sigma_tick_per_sqrt_second": float(sigma) if np.isfinite(sigma) else None,
        "estimated_half_spread_ticks": half,
        "estimated_inventory_skew_ticks_per_contract": skew,
        "diagnostic_unconditional_regression": fallback,
        "first_10_depth_counts": count[:10].tolist(),
        "last_10_depth_counts": count[-10:].tolist(),
    }


def run(symbol, day, out):
    if symbol not in SYMBOLS:
        raise ValueError("Unsupported diagnostic symbol")
    date = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if date.day != 1:
        raise ValueError("Only specified first-of-month dates supported")
    start_us = int(date.timestamp() * 1_000_000)
    # Keep a margin after the 10-minute warmup. No next-day data.
    end_us = start_us + 660_000_000
    with tempfile.TemporaryDirectory(prefix="gate_fit_diag_") as td:
        root = Path(td)
        spec = fetch_contract(symbol)
        paths, counts = {}, {}
        for kind in SCHEMAS:
            path = root / (kind + ".csv")
            counts[kind] = safe_gate_csv(
                kind, day, end_us, path, 6_000_000, symbol=symbol,
                historical_tick=float(spec["order_price_round"]),
                historical_lot=float(spec["order_size_min"]),
            )
            paths[kind] = path
        require_unshifted_target_clock(counts)
        local, gate_mid = load_gate_mid(paths["book_ticker"])
        b_local, b_mid = read_binance_bbo(day, end_us, symbol=symbol.replace("_",""))
        visible = visible_timestamps(b_local, BINANCE_DELAY_MS)
        cfg = scenario(float(np.median(gate_mid[:200])), spec)
        fused = root / "hbt_fused.npz"
        events = tardis.convert_fuse(
            str(paths["trades"]), str(paths["incremental_book_L2"]),
            str(paths["book_ticker"]), tick_size=cfg["tick"], lot_size=cfg["lot"],
            output_filename=str(fused),
            ss_buffer_size=max(1000,counts["incremental_book_L2"]["snapshot_rows"]+100),
            snapshot_mode="process",
        )
        validate_event_order(events)
        del events
        engine = ROIVectorMarketDepthBacktest([
            build_asset([fused], float(np.min(gate_mid)), float(np.max(gate_mid)), cfg)
        ])
        try:
            arrivals, changes, samples, t0, stop, gate_events, binance_events = (
                replay_initial_observations(engine, visible, cfg["tick"]))
        finally:
            engine.close()

    if samples != TRAIN:
        raise ValueError(f"Warmup truncated {samples} out of {TRAIN} intervals")
    diag = diagnose(arrivals, changes, cfg)
    diag.update({
        "gate_symbol":symbol, "binance_symbol":symbol.replace("_",""),
        "day":day,"source":"Tardis Gate trades/L2/quotes & Binance book_ticker",
        "official_hftbacktest":"2.4.4",
        "start_timestamp_ns":int(t0),"last_timestamp_ns":int(stop),
        "warmup_samples":int(samples),
        "gate_events_replayed":int(gate_events),
        "binance_events_replayed":int(binance_events),
        "scenario":{"tick":cfg["tick"],"lot":cfg["lot"],"ctval":cfg["ctval"],
                    "qty":cfg["qty"],"gamma_code":cfg["gamma_code"]},
        "feed_stats":{k:{"rows":v["rows"],"first_local_us":v["first_local_us"],
                         "last_local_us":v["last_local_us"]} for k,v in counts.items()}
    })
    out.mkdir(parents=True,exist_ok=True)
    (out / f"{symbol}_{day}_fit.json").write_text(
        json.dumps(diag,indent=2,ensure_ascii=False)+"\n",encoding="utf-8")
    print("GATE_ALTCOIN_INITIAL_FIT_DIAG",json.dumps({
        key:diag[key] for key in (
            "gate_symbol","day","reason","qualifying_intervals",
            "crossed_distance_levels","extreme_intervals_over_35_ticks",
            "valid_midpoint_changes","estimated_A_per_second",
            "estimated_k_per_tick","estimated_sigma_tick_per_sqrt_second",
            "max_trade_through_ticks")
    },ensure_ascii=False),flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--symbol",choices=SYMBOLS,required=True)
    p.add_argument("--day",required=True)
    p.add_argument("--out",type=Path,required=True)
    args = p.parse_args()
    run(args.symbol,args.day,args.out)
