"""G0 static vs rolling GLFT: identical causal estimator, official hftbacktest matching.

Training first 10min. Trade next 5/50min of OKX BTC-USDT-SWAP on 2026-11-01.
Rolling case recalibrates every 5s using the last 10min of 100ms observations.
"""
import argparse
import csv
import json
import math
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
from numba import njit

from check_okx_hft_data import SCHEMAS, extract_window
from g0_glft_baseline import (CTVAL, GAMMA, LOT, MAKER_FEE, ORDER_QTY,
                              POSITION_CAP, STEP_NS, TAKER_FEE, TICK, TRAIN,
                              unit_grid_audit)

UPDATE_EVERY_STEPS = 50                 # 5sec / 100ms
FIT_BINS = 70                           # 0.5..35 actual price ticks
FIT_STEP_TICKS = 0.5
WINDOW_SECONDS = TRAIN * STEP_NS / 1e9
TRADE_CHOICES_MINUTES = (5, 50)


@njit
def fit_recent_arrivals(arrivals, changes):
    """Fit full-price-tick threshold counts, with 10min interval denominator.

    Trade-through count: max directional trade depth > quote distance.
    An extreme trade crosses EVERY closer threshold. No artificial clipping.
    """
    counts = np.zeros(FIT_BINS, np.float64)
    qualifying_intervals = 0
    extreme_intervals = 0
    for depth in arrivals:
        if not np.isfinite(depth):
            continue
        if depth > FIT_BINS * FIT_STEP_TICKS + 1e-7:
            extreme_intervals += 1
        if depth <= FIT_STEP_TICKS + 1e-7:
            continue
        qualifying_intervals += 1
        for i in range(FIT_BINS):
            distance = (i + 1) * FIT_STEP_TICKS
            if depth > distance + 1e-7:
                counts[i] += 1
            else:
                break

    n = 0
    sx = sy = sxx = sxy = 0.0
    for i in range(FIT_BINS):
        if counts[i] <= 0:
            continue
        x = (i + 1) * FIT_STEP_TICKS
        y = math.log(counts[i] / WINDOW_SECONDS)
        n += 1
        sx += x
        sy += y
        sxx += x*x
        sxy += x*y

    var_n = 0
    total = squared = 0.0
    for value in changes:
        if np.isfinite(value):
            var_n += 1
            total += value
            squared += value*value
    if n < 5 or var_n < TRAIN // 2:
        return 0, np.nan, np.nan, np.nan, n, qualifying_intervals, extreme_intervals

    denominator = n*sxx - sx*sx
    if denominator <= 0:
        return 0, np.nan, np.nan, np.nan, n, qualifying_intervals, extreme_intervals
    slope = (n*sxy - sx*sy)/denominator
    intercept = (sy - slope*sx)/n
    a = math.exp(intercept)
    k = -slope
    variance = max(0.0, squared/var_n - (total/var_n)**2)
    sigma = math.sqrt(variance / (STEP_NS / 1e9))
    if not (np.isfinite(a) and np.isfinite(k) and np.isfinite(sigma) and
            a > 0 and k > 0 and sigma > 0):
        return 0, a, k, sigma, n, qualifying_intervals, extreme_intervals
    return 1, a, k, sigma, n, qualifying_intervals, extreme_intervals


@njit
def glft_coefficients(a, k, sigma):
    """Retain original G0 scenario gamma and contract-based order quantity."""
    u = GAMMA * ORDER_QTY / k
    c1 = math.log1p(u) / (GAMMA * ORDER_QTY)
    c2 = math.sqrt(GAMMA / (2 * a * ORDER_QTY * k)
                   * math.exp((k/(GAMMA * ORDER_QTY) + 1) * math.log1p(u)))
    half = c1 + ORDER_QTY/2 * c2 * sigma
    skew = c2 * sigma
    return half, skew


@njit
def run_strategy(engine, rolling, trade_steps):
    """One official simulator run: warmup and trading in one causal event loop."""
    from hftbacktest import BUY, BUY_EVENT, SELL, GTX, LIMIT, FILLED
    # Reusable ring: every sample is collected *after* replaying past 100ms.
    arrivals = np.full(TRAIN, np.nan, np.float64)
    changes = np.full(TRAIN, np.nan, np.float64)
    updates = np.full((1 + trade_steps // UPDATE_EVERY_STEPS + 2, 9), np.nan)
    last_mid = np.nan
    first_good = 0
    n_updates = 0
    attempted = rejected = 0
    initial_valid = 0
    a = k = sigma = half = skew = np.nan
    submissions = cancels = buys = sells = decision_steps = 0
    min_equity, max_equity = np.inf, -np.inf
    max_position = 0.0
    first_equity = np.nan
    pre_fills = engine.state_values(0).num_trades
    actual_steps = 0
    # First 6000 intervals for warmup only, then trade decision every 100ms.
    for sample in range(TRAIN + trade_steps):
        if engine.elapse(STEP_NS) != 0:
            break
        actual_steps += 1
        depth_through = -np.inf
        if np.isfinite(last_mid):
            for trade in engine.last_trades(0):
                trade_tick = trade.px / TICK
                if trade.ev & BUY_EVENT == BUY_EVENT:
                    depth_through = max(depth_through, trade_tick-last_mid)
                else:
                    depth_through = max(depth_through, last_mid-trade_tick)
        engine.clear_last_trades(0)
        slot = sample % TRAIN
        arrivals[slot] = depth_through
        book = engine.depth(0)
        bid, ask = book.best_bid_tick, book.best_ask_tick
        if bid > 0 and ask > bid and ask < 1_000_000_000:
            mid = (bid+ask)/2.0
            changes[slot] = mid-last_mid if np.isfinite(last_mid) else np.nan
            last_mid = mid
        else:
            changes[slot] = np.nan
            last_mid = np.nan

        # Full past 10min become available at sample=5999.
        # Subsequent updates at 5sec intervals are based only on observed past.
        should_update = (sample == TRAIN-1 or
                         (rolling and sample >= TRAIN and
                          (sample+1-TRAIN) % UPDATE_EVERY_STEPS == 0))
        if should_update:
            attempted += 1
            valid, new_a, new_k, new_sigma, fit_levels, trade_intervals, extremes = (
                fit_recent_arrivals(arrivals, changes))
            if valid:
                new_half, new_skew = glft_coefficients(new_a, new_k, new_sigma)
                if (np.isfinite(new_half) and np.isfinite(new_skew)
                        and new_half > 0 and new_skew > 0):
                    a, k, sigma, half, skew = (
                        new_a, new_k, new_sigma, new_half, new_skew)
                    if sample == TRAIN-1:
                        initial_valid = 1
                    first_good += 1
                    updates[n_updates,0] = (sample+1-TRAIN) * STEP_NS/1e9
                    updates[n_updates,1] = a
                    updates[n_updates,2] = k
                    updates[n_updates,3] = sigma
                    updates[n_updates,4] = half
                    updates[n_updates,5] = skew
                    updates[n_updates,6] = fit_levels
                    updates[n_updates,7] = trade_intervals
                    updates[n_updates,8] = extremes
                    n_updates += 1
                else:
                    rejected += 1
            else:
                rejected += 1

        if sample < TRAIN:
            engine.clear_inactive_orders(0)
            continue
        if not np.isfinite(half):
            continue

        orders = engine.orders(0)
        it = orders.values()
        while it.has_next():
            order = it.get()
            if order.status == FILLED:
                if order.side == BUY:
                    buys += 1
                else:
                    sells += 1
        engine.clear_inactive_orders(0)
        book = engine.depth(0)
        bid, ask = book.best_bid_tick, book.best_ask_tick
        if bid <= 0 or ask <= bid or ask > 1_000_000_000:
            continue
        mid = (bid+ask)/2.0
        pos = engine.position(0)
        st = engine.state_values(0)
        equity = st.balance + CTVAL*pos*mid*TICK - st.fee
        if not np.isfinite(first_equity):
            first_equity = equity
        min_equity = min(min_equity,equity)
        max_equity = max(max_equity,equity)
        max_position = max(max_position,abs(pos))
        desired_reservation = mid-skew*pos
        want_bid = min(round(desired_reservation-half),bid)
        want_ask = max(round(desired_reservation+half),ask)

        # At most one outstanding order per side (including pending cancels).
        # This slightly conservatively waits for old side to clear.
        has_bid = has_ask = False
        it = orders.values()
        while it.has_next():
            order = it.get()
            if order.side == BUY:
                has_bid = True
                if order.cancellable and order.price_tick != want_bid:
                    engine.cancel(0, order.order_id, False)
                    cancels += 1
            elif order.side == SELL:
                has_ask = True
                if order.cancellable and order.price_tick != want_ask:
                    engine.cancel(0, order.order_id, False)
                    cancels += 1
        if not has_bid and pos+ORDER_QTY <= POSITION_CAP+1e-9:
            bid_id = int(want_bid)*2
            engine.submit_buy_order(0,bid_id,want_bid*TICK,ORDER_QTY,GTX,LIMIT,False)
            submissions += 1
        if not has_ask and pos-ORDER_QTY >= -POSITION_CAP-1e-9:
            ask_id = int(want_ask)*2+1
            engine.submit_sell_order(0,ask_id,want_ask*TICK,ORDER_QTY,GTX,LIMIT,False)
            submissions += 1
        decision_steps += 1

    final = engine.state_values(0)
    b = engine.depth(0)
    final_mid = (b.best_bid + b.best_ask)/2.0
    final_equity = final.balance+CTVAL*final.position*final_mid-final.fee
    max_position = max(max_position,abs(final.position))
    return (actual_steps, decision_steps, attempted, rejected, first_good,
            initial_valid, updates[:n_updates], submissions, cancels, buys, sells,
            final.num_trades-pre_fills, final.position, max_position,
            final.balance,final.fee,final.trading_value,final_mid,
            final_equity,min_equity,max_equity,first_equity)


def asset_for(npz, lb, ub):
    from hftbacktest import BacktestAsset
    return (BacktestAsset().data([str(npz)]).linear_asset(CTVAL)
            .constant_order_latency(1_000_000, 1_000_000)
            .power_prob_queue_model(2.0).no_partial_fill_exchange()
            .trading_value_fee_model(MAKER_FEE, TAKER_FEE)
            .tick_size(TICK).lot_size(LOT).roi_lb(lb-1000).roi_ub(ub+1000)
            .last_trades_capacity(20_000))


def run_mode(npz, lb, ub, minutes, rolling):
    from hftbacktest import ROIVectorMarketDepthBacktest
    engine = ROIVectorMarketDepthBacktest([asset_for(npz, lb, ub)])
    try:
        raw = run_strategy(engine, rolling, minutes*600)
    finally:
        engine.close()
    (actual, steps, attempted, rejected, successful, initial_ok, updates,
     submissions, cancels, buys, sells, fills, position, max_pos, balance, fee,
     turnover, final_mid, final_equity, lo_equity, hi_equity, first_equity) = raw
    if initial_ok != 1 or successful < 1:
        raise RuntimeError("Initial 10min GLFT calibration failed; no trading result")
    if actual < TRAIN+minutes*600-5:
        raise RuntimeError(f"Unexpected early market end: {actual} steps")
    if fills != buys+sells:
        raise RuntimeError(f"Maker-fill accounting differs from official engine: {fills} vs {buys+sells}")
    if max_pos > POSITION_CAP + ORDER_QTY + 1e-9:
        raise RuntimeError("Inventory cap violated")
    if not math.isfinite(final_equity):
        raise RuntimeError("Non-finite net marked equity")
    update_rows = [
        {"elapsed_seconds_after_training":float(r[0]),"A_per_second":float(r[1]),
         "k_per_real_tick":float(r[2]),"sigma_tick_per_sqrt_second":float(r[3]),
         "half_spread_ticks":float(r[4]),"skew_ticks_per_contract":float(r[5]),
         "positive_fitted_levels":int(r[6]),"crossing_intervals":int(r[7]),
         "out_of_range_crossing_intervals":int(r[8])}
        for r in updates
    ]
    return {
        "mode": "rolling" if rolling else "static",
        "observation_step_ms": 100, "calibration_window_seconds": 600,
        "parameter_update_interval_seconds": 5 if rolling else None,
        "actual_replay_steps":int(actual),"trading_decision_steps":int(steps),
        "calibration_attempts":int(attempted),"calibration_rejections":int(rejected),
        "successful_calibrations":int(successful),
        "initial_parameters":update_rows[0],
        "last_parameters":update_rows[-1],
        "calibration_trace":update_rows if rolling else update_rows[:1],
        "trading":{
            "submitted_orders":int(submissions),"cancel_requests":int(cancels),
            "filled_buy_orders":int(buys),"filled_sell_orders":int(sells),
            "engine_fills":int(fills),
            "end_position_contracts":float(position),
            "max_abs_position_contracts":float(max_pos),
            "balance_usdt":float(balance),"fees_usdt":float(fee),
            "turnover_usdt":float(turnover),"end_mid_usdt_per_btc":float(final_mid),
            "end_net_marked_equity_usdt":float(final_equity),
            "first_marked_equity_usdt":float(first_equity),
            "min_marked_equity_usdt":float(lo_equity),
            "max_marked_equity_usdt":float(hi_equity)}}


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--day",default="2026-11-01")
    p.add_argument("--trade-minutes",type=int,default=50,choices=TRADE_CHOICES_MINUTES)
    p.add_argument("--report-dir",default="reports")
    args=p.parse_args()
    day=datetime.strptime(args.day,"%Y-%m-%d").replace(tzinfo=timezone.utc)
    if day.day!=1:
        p.error("Tardis free data require day 1")
    out=Path(args.report_dir)/f"g0_rolling_vs_static_{args.day}_{args.trade_minutes}m.json"
    out.parent.mkdir(parents=True,exist_ok=True)
    report={"status":"started","day_utc":args.day,
            "evaluation":"first 10min parameter warmup then next "+str(args.trade_minutes)+"min strategy",
            "scenario":{"hftbacktest_version":"2.4.4",
                        "midprice_observation_ms":100,
                        "strategy_requote_ms":100,
                        "rolling_fit_window_sec":600,
                        "rolling_update_sec":5,
                        "tick_usdt_per_btc":TICK,"lot_contracts":LOT,
                        "ctval_btc_per_contract":CTVAL,
                        "order_qty_contracts":ORDER_QTY,"max_abs_position_contracts":POSITION_CAP,
                        "gamma":GAMMA,"maker_fee":MAKER_FEE,"taker_fee":TAKER_FEE,
                        "order_entry_latency_ms":1,"order_response_latency_ms":1,
                        "queue":"power_prob(2.0)","exchange":"no_partial_fill",
                        "estimator":"strict prior-mid trade-through, x=0.5..35 REAL price ticks, A per second",
                        "parameter_units_caveat":"original G0 arbitrary gamma uses inventory contracts; CTVAL and risk units need subsequent formal dimensional audit"},
            "NOT_daily_return":True}
    try:
        from hftbacktest.data.utils import tardis
        from hftbacktest.data.validation import validate_event_order
        with tempfile.TemporaryDirectory(prefix="mm_g0_roll_") as td:
            root=Path(td)
            end_us=int(day.timestamp()*1_000_000)+(600+args.trade_minutes*60)*1_000_000
            paths={};counts={}
            for kind in SCHEMAS:
                paths[kind]=root/(kind+".csv")
                counts[kind]=extract_window(kind,args.day,end_us,paths[kind],max_rows=2_500_000)
            report["market_counts"]=counts
            report["unit_grid_audit"]=unit_grid_audit(paths)
            npz=root/"fused.npz"
            events=tardis.convert_fuse(str(paths["trades"]),str(paths["incremental_book_L2"]),
                    str(paths["book_ticker"]),tick_size=TICK,lot_size=LOT,
                    output_filename=str(npz),
                    ss_buffer_size=max(1000,counts["incremental_book_L2"]["snapshot_rows"]+100),
                    snapshot_mode="process")
            validate_event_order(events)
            report["converted_events"]=int(len(events))
            del events
            lb,ub=math.inf,-math.inf
            with paths["book_ticker"].open(encoding="utf-8",newline="") as f:
                for row in csv.DictReader(f):
                    lb=min(lb,float(row["bid_price"]))
                    ub=max(ub,float(row["ask_price"]))
            static=run_mode(npz,lb,ub,args.trade_minutes,False)
            rolling=run_mode(npz,lb,ub,args.trade_minutes,True)
            for key in ("A_per_second","k_per_real_tick","sigma_tick_per_sqrt_second",
                        "half_spread_ticks","skew_ticks_per_contract"):
                if not math.isclose(static["initial_parameters"][key],
                                    rolling["initial_parameters"][key],rel_tol=1e-12):
                    raise RuntimeError(f"Paired initial {key} differ")
            report["static"]=static
            report["rolling"]=rolling
            report["paired_end_equity_difference_usdt"]=(
                rolling["trading"]["end_net_marked_equity_usdt"]-
                static["trading"]["end_net_marked_equity_usdt"])
        report["status"]="complete"
    except Exception as ex:
        report["status"]="failed"
        report["error"]=f"{type(ex).__name__}: {ex}"
        raise
    finally:
        out.write_text(json.dumps(report,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
        print(f"G0 rolling/static {report['status']}: {out}",flush=True)


if __name__=="__main__":
    main()
