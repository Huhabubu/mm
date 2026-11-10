"""Diagnose G0 intensity calibration against real OKX events and official-engine fills.

Training: first 10 minutes of 2026-11-01. Holdout: next 10 minutes.
This is a calibration/fill diagnostic, not a new GLFT return backtest.
"""
import argparse
import csv
import json
import math
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from check_okx_hft_data import SCHEMAS, extract_window
from g0_glft_baseline import (TICK, LOT, CTVAL, GAMMA, ORDER_QTY, POSITION_CAP,
                              MAKER_FEE, TAKER_FEE, STEP_NS, TRAIN,
                              calibrate, calibrate_replay, quote_coefficients)

# Distances measured in ACTUAL tick-size units (0.1 USDT per tick).
CHECK_DISTANCES = (0.5, 1, 2, 5, 10, 20, 35, 50, 100)
FIT_RANGES = (("near_5_ticks", 5), ("near_20_ticks", 20),
              ("legacy_35_ticks", 35), ("wide_100_ticks", 100))
SECONDS = TRAIN * STEP_NS / 1_000_000_000


def empirical_curve(depths, distances, seconds=SECONDS):
    """Frequency per 100ms observation interval, counting max directional trade depth.

    Equal-price touch is only a POSSIBLE execution under queue assumptions.
    Strict crossing is a trade-through proxy assuming a resting order.
    """
    finite = np.asarray(depths, dtype=float)
    finite = finite[np.isfinite(finite)]
    out = []
    for distance in distances:
        touch = int(np.count_nonzero(finite >= distance - 1e-7))
        cross = int(np.count_nonzero(finite > distance + 1e-7))
        out.append({"distance_ticks": float(distance), "touch_intervals": touch,
                    "cross_intervals": cross, "touch_per_second": touch / seconds,
                    "cross_per_second": cross / seconds})
    return out


def fit_curve(rows, max_depth):
    """OLS of log strict-cross interval rates; actual x is distance in full ticks."""
    eligible = [r for r in rows if r["distance_ticks"] <= max_depth and r["cross_intervals"] > 0]
    if len(eligible) < 5:
        return {"status": "insufficient_positive_levels", "used_levels": len(eligible)}
    x = np.array([r["distance_ticks"] for r in eligible], dtype=float)
    y = np.log([r["cross_per_second"] for r in eligible])
    slope, intercept = np.polyfit(x, y, 1)
    residuals = y - (slope * x + intercept)
    total = float(np.sum((y - np.mean(y)) ** 2))
    k = float(-slope)
    a = float(math.exp(intercept))
    if k <= 0 or not math.isfinite(k) or not math.isfinite(a):
        return {"status": "nondecreasing_or_invalid_fit", "used_levels": len(eligible),
                "k_per_tick": k, "A_intervals_per_second": a}
    rmse_log = float(np.sqrt(np.mean(residuals ** 2)))
    return {"status": "ok", "used_levels": len(eligible),
            "min_fitted_distance_ticks": float(min(x)),
            "max_fitted_distance_ticks": float(max(x)),
            "A_intervals_per_second": a, "k_per_tick": k,
            "fit_r2_log_rate": float(1 - np.sum(residuals ** 2) / total) if total > 0 else None,
            "rmse_log_rate": rmse_log}


def compare_fit_on_holdout(fit, holdout):
    if fit["status"] != "ok":
        return {"status": fit["status"]}
    evaluated = [r for r in holdout
                 if (r["distance_ticks"] >= fit["min_fitted_distance_ticks"]
                     and r["distance_ticks"] <= fit["max_fitted_distance_ticks"]
                     and r["cross_intervals"] > 0)]
    if not evaluated:
        return {"status": "no_positive_holdout_levels"}
    errs = [math.log(r["cross_per_second"]) -
            (math.log(fit["A_intervals_per_second"]) - fit["k_per_tick"] * r["distance_ticks"])
            for r in evaluated]
    return {"status": "ok", "evaluated_levels": len(errs),
            "holdout_rmse_log_rate": float(np.sqrt(np.mean(np.square(errs))))}


def fitted_quote(fit, sigma):
    if fit["status"] != "ok":
        return None
    try:
        half, skew = quote_coefficients(fit["A_intervals_per_second"],
                                         fit["k_per_tick"], sigma)
    except (ValueError, OverflowError, ZeroDivisionError):
        return {"status": "invalid_glft_quote_from_fit"}
    return {"half_spread_ticks": float(half),
            "half_spread_usdt_per_btc": float(half * TICK),
            "skew_ticks_per_contract": float(skew),
            "outside_fitted_distance_range": half > fit["max_fitted_distance_ticks"]}


def instrument_asset(npz, low, high):
    from hftbacktest import BacktestAsset
    return (BacktestAsset().data([str(npz)]).linear_asset(CTVAL)
            .constant_order_latency(1_000_000, 1_000_000)
            .power_prob_queue_model(2.0).no_partial_fill_exchange()
            .trading_value_fee_model(MAKER_FEE, TAKER_FEE)
            .tick_size(TICK).lot_size(LOT)
            .roi_lb(low - 1000).roi_ub(high + 1000)
            .last_trades_capacity(5000))


def replay_quoted_distances(npz, low, high, n_steps=TRAIN):
    """Each scenario replays identical holdout events, submits GTX orders with official engine.

    Quotes are re-priced every 100ms, one in-flight order per side; queue and
    1ms order/response latencies are applied by hftbacktest itself.
    """
    from hftbacktest import ROIVectorMarketDepthBacktest, BUY, SELL, GTX, LIMIT, FILLED
    from numba import njit

    @njit
    def run(engine, distance_ticks, steps):
        n = bids = asks = cancels = fill_buy = fill_sell = 0
        bid_dist_sum = ask_dist_sum = 0.0
        min_bid_dist = min_ask_dist = np.inf
        max_pos = 0.0
        for _ in range(steps):
            if engine.elapse(STEP_NS) != 0:
                break
            orders = engine.orders(0)
            it = orders.values()
            while it.has_next():
                o = it.get()
                if o.status == FILLED:
                    if o.side == BUY:
                        fill_buy += 1
                    else:
                        fill_sell += 1
            engine.clear_inactive_orders(0)
            engine.clear_last_trades(0)
            d = engine.depth(0)
            bid, ask = d.best_bid_tick, d.best_ask_tick
            if bid <= 0 or ask <= bid or ask >= 1_000_000_000:
                continue
            mid = (bid + ask) / 2.0
            price_bid = min(round(mid - distance_ticks), bid)
            price_ask = max(round(mid + distance_ticks), ask)
            bid_dist = mid - price_bid
            ask_dist = price_ask - mid
            bid_dist_sum += bid_dist
            ask_dist_sum += ask_dist
            min_bid_dist = min(min_bid_dist, bid_dist)
            min_ask_dist = min(min_ask_dist, ask_dist)
            position = engine.position(0)
            max_pos = max(max_pos, abs(position))
            has_buy = False
            has_sell = False
            it = orders.values()
            while it.has_next():
                o = it.get()
                if o.side == BUY:
                    has_buy = True
                    if o.cancellable and o.price_tick != price_bid:
                        engine.cancel(0, o.order_id, False)
                        cancels += 1
                elif o.side == SELL:
                    has_sell = True
                    if o.cancellable and o.price_tick != price_ask:
                        engine.cancel(0, o.order_id, False)
                        cancels += 1
            if not has_buy and position + ORDER_QTY <= POSITION_CAP + 1e-9:
                oid = int(price_bid) * 2
                engine.submit_buy_order(0, oid, price_bid*TICK, ORDER_QTY, GTX, LIMIT, False)
                bids += 1
            if not has_sell and position - ORDER_QTY >= -POSITION_CAP - 1e-9:
                oid = int(price_ask)*2 + 1
                engine.submit_sell_order(0, oid, price_ask*TICK, ORDER_QTY, GTX, LIMIT, False)
                asks += 1
            n += 1
        st = engine.state_values(0)
        return (n, bids, asks, cancels, fill_buy, fill_sell, st.num_trades,
                st.position, max_pos, st.fee, bid_dist_sum, ask_dist_sum,
                min_bid_dist, min_ask_dist)

    out = {}
    for level in CHECK_DISTANCES:
        engine = ROIVectorMarketDepthBacktest([instrument_asset(npz, low, high)])
        try:
            if engine.elapse(int(SECONDS * 1_000_000_000)) != 0:
                raise ValueError(f"Could not advance to held-out start for {level} ticks")
            result = run(engine, float(level), n_steps)
        finally:
            engine.close()
        (n, bids, asks, cancels, fb, fs, trades, pos, maximum, fee,
         bd, ad, mb, ma) = result
        if n < n_steps - 5:
            raise ValueError(f"Early finish at {level} ticks: {n}")
        if trades != fb + fs:
            raise ValueError(f"Fill counting mismatch at {level} ticks: {trades} vs {fb + fs}")
        if abs(pos) > POSITION_CAP + ORDER_QTY + 1e-9:
            raise ValueError(f"Position cap broken at {level} ticks")
        out[str(level)] = {
            "target_depth_ticks": level, "decision_steps": int(n),
            "submitted_bid_orders": int(bids), "submitted_ask_orders": int(asks),
            "cancel_requests": int(cancels),
            "maker_buy_fills": int(fb), "maker_sell_fills": int(fs),
            "engine_fills": int(trades), "end_position_contracts": float(pos),
            "max_abs_position_contracts": float(maximum), "maker_fee_usdt": float(fee),
            "mean_actual_bid_distance_ticks": bd / n,
            "mean_actual_ask_distance_ticks": ad / n,
            "min_bid_distance_ticks": float(mb), "min_ask_distance_ticks": float(ma)}
    return out


def diagnose(train, holdout, sigma):
    fine = np.arange(1, 201, dtype=float) * .5
    curves = {"train": empirical_curve(train, fine),
              "holdout": empirical_curve(holdout, fine)}
    lookup = set(CHECK_DISTANCES)
    selected = {"train": [x for x in curves["train"] if x["distance_ticks"] in lookup],
                "holdout": [x for x in curves["holdout"] if x["distance_ticks"] in lookup]}
    profiles = {}
    for name, maxdist in FIT_RANGES:
        fit = fit_curve(curves["train"], maxdist)
        profiles[name] = {"fit": fit, "holdout": compare_fit_on_holdout(fit, curves["holdout"]),
                          "GLFT_quote_diagnostic": fitted_quote(fit, sigma)}
    finite = np.asarray(train, dtype=float)
    finite = finite[np.isfinite(finite)]
    near = sum(1 for x in curves["train"] if x["distance_ticks"] == 0.5)
    assert near == 1
    return {"interval_seconds": 0.1, "calibration_seconds": SECONDS,
            "holdout_seconds": SECONDS,
            "train_intervals_with_trades": int(len(finite)),
            "train_intervals_max_depth_above_35_ticks": int(np.sum(finite > 35)),
            "train_intervals_max_depth_above_100_ticks": int(np.sum(finite > 100)),
            "depth_quantiles_ticks": ({str(p): float(np.percentile(finite,p))
                                     for p in (50,90,95,99,99.9)} if len(finite) else {}),
            "selected_depths": selected, "profiles": profiles,
            "definition": "per-100ms interval maximum directional trade depth vs prior visible OKX mid; touch is upper-bound opportunity, cross is strict trade-through; neither guarantees fills"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", default="2026-11-01")
    parser.add_argument("--report-dir", default="reports")
    args = parser.parse_args()
    day = datetime.strptime(args.day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if day.day != 1:
        parser.error("Tardis monthly free data require day 1")
    report = {"status": "started", "day": args.day,
              "scope": "G0 calibration diagnosis; no strategy profitability inference",
              "samples": "first 10min training / next 10min holdout",
              "hftbacktest_queue": "power_prob_queue_model(2.0)",
              "order_latency_scenario_ms": [1,1],
              "maker_order_qty_contracts": ORDER_QTY,
              "price_tick_usdt": TICK, "btc_per_contract": CTVAL}
    target = Path(args.report_dir)/f"g0_intensity_diagnostic_{args.day}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        from hftbacktest import ROIVectorMarketDepthBacktest
        from hftbacktest.data.utils import tardis
        from hftbacktest.data.validation import validate_event_order
        with tempfile.TemporaryDirectory(prefix="mm_g0_intensity_") as tmp:
            directory = Path(tmp)
            paths = {}
            stats = {}
            until = int(day.timestamp()*1_000_000) + 1200*1_000_000
            for kind in SCHEMAS:
                paths[kind] = directory/f"{kind}.csv"
                stats[kind] = extract_window(kind, args.day, until, paths[kind],
                                             max_rows=1_200_000)
            report["input_counts"] = stats
            npz = directory/"replay.npz"
            events = tardis.convert_fuse(
                str(paths["trades"]), str(paths["incremental_book_L2"]),
                str(paths["book_ticker"]), tick_size=TICK, lot_size=LOT,
                output_filename=str(npz),
                ss_buffer_size=max(1000,stats["incremental_book_L2"]["snapshot_rows"]+100),
                snapshot_mode="process")
            validate_event_order(events)
            report["converted_events"] = len(events)
            del events
            lb, ub = math.inf, -math.inf
            spreads = []
            split = int(day.timestamp()*1_000_000) + 600*1_000_000
            with paths["book_ticker"].open(encoding="utf-8", newline="") as f:
                for row in csv.DictReader(f):
                    bid, ask = float(row["bid_price"]), float(row["ask_price"])
                    lb = min(lb,bid)
                    ub = max(ub,ask)
                    if int(row["local_timestamp"]) < split:
                        spreads.append((ask-bid)/TICK)
            report["train_median_native_bbo_spread_ticks"] = float(np.median(spreads))
            engine = ROIVectorMarketDepthBacktest([instrument_asset(npz, lb, ub)])
            try:
                train_arr, changes, n1 = calibrate_replay(engine)
                holdout_arr, holdout_changes, n2 = calibrate_replay(engine)
            finally:
                engine.close()
            if n1 != TRAIN or n2 != TRAIN:
                raise ValueError(f"Missing 100ms calibration/holdout steps {n1} / {n2}")
            prior = calibrate(train_arr, changes)
            report["legacy_estimate_original_unchanged"] = prior
            report["legacy_quote_original_unchanged"] = dict(zip(
                ("half_spread_ticks", "skew_ticks_per_contract"),
                quote_coefficients(prior["A_per_second"], prior["k_per_tick"],
                                   prior["sigma_tick_per_sqrt_second"])))
            report["diagnostic"] = diagnose(train_arr, holdout_arr,
                                            prior["sigma_tick_per_sqrt_second"])
            report["hftbacktest_holdout_quote_fills"] = replay_quoted_distances(npz, lb, ub)
            report["status"] = "complete"
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        target.write_text(json.dumps(report,ensure_ascii=False,indent=2)+"\n", encoding="utf-8")
        print(f"G0 intensity diagnosis: {report['status']} -> {target}", flush=True)


if __name__ == "__main__":
    main()
