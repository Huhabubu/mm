"""Event-triggered G0-R GLFT using hftbacktest 2.4.4 execution only.

Estimator: exactly the existing G0-R fixed 100ms ring and 5s refit.
Quoting: on OKX feed, order reply, Binance *clock*, or maintenance.
Default beta is zero; optional direct-gap/placebo/filter experimental modes.
"""
import math

import numpy as np
from numba import njit
from hftbacktest import BUY, BUY_EVENT, SELL, GTX, LIMIT, FILLED, CANCELED

from g0_glft_baseline import CTVAL, GAMMA, ORDER_QTY, POSITION_CAP, STEP_NS, TICK, TRAIN
from g0_glft_rolling import UPDATE_EVERY_STEPS, fit_recent_arrivals, glft_coefficients

MIN_REQUOTE_NS = 20_000_000  # Per side, at most one order action per 20ms.


@njit
def causal_price_shift_ticks(now, binance_visible_ns, binance_mid, beta,
                             okx_native_ns, okx_native_mid, basis_grid_ns,
                             basis_values):
    """No forward BBO / basis access. Strict past OKX and basis, fresh Binance."""
    if beta == 0.0 or not np.isfinite(binance_mid):
        return 0.0
    if binance_visible_ns < 0 or now < binance_visible_ns or now-binance_visible_ns > 250_000_000:
        return 0.0
    oi = np.searchsorted(okx_native_ns, now, side="left") - 1
    gi = np.searchsorted(basis_grid_ns, now, side="left") - 1
    if oi < 0 or gi < 0 or now < okx_native_ns[oi] or now-okx_native_ns[oi] > 250_000_000:
        return 0.0
    if not np.isfinite(basis_values[gi]):
        return 0.0
    gap_usdt_per_btc = binance_mid - okx_native_mid[oi] - basis_values[gi]
    return beta * gap_usdt_per_btc / TICK


@njit
def filtered_gap_shift(now, raw_shift_ticks, held_shift_ticks, last_update_ns,
                       enter_ticks, exit_ticks, refresh_change_ticks,
                       refresh_interval_ns):
    """Pretrade P90/P50 hysteresis plus bounded signal-centre refresh.

    This filters Binance-driven quote centre changes, not OKX/GLFT quote
    changes. A stale signal resets to zero immediately.
    """
    if not np.isfinite(raw_shift_ticks):
        raw_shift_ticks = 0.0
    if held_shift_ticks == 0.0:
        if abs(raw_shift_ticks) >= enter_ticks:
            return raw_shift_ticks, now
        return 0.0, last_update_ns
    if abs(raw_shift_ticks) <= exit_ticks:
        return 0.0, now
    if raw_shift_ticks * held_shift_ticks < 0.0:
        if abs(raw_shift_ticks) >= enter_ticks:
            return raw_shift_ticks, now
        return 0.0, now
    if (now - last_update_ns >= refresh_interval_ns and
            abs(raw_shift_ticks - held_shift_ticks) >= refresh_change_ticks):
        return raw_shift_ticks, now
    return held_shift_ticks, last_update_ns


@njit
def run_event_glft(engine, binance_visible_ns, binance_mid, trade_steps,
                   rolling=True, gamma_code=GAMMA, read_binance=False,
                   beta=0.0, okx_native_ns=None, okx_native_mid=None,
                   basis_grid_ns=None, basis_values=None,
                   filter_enter_ticks=0.0, filter_exit_ticks=0.0,
                   filter_refresh_change_ticks=10.0,
                   filter_refresh_ns=250_000_000,
                   extra_half_ticks=0.0, capture_mechanism=False):
    """Replay full warmup + trading with strictly causal external event deadlines.

    All actual sends, cancels, queue updates and fills use the official engine.
    Signal-read flag only adds telemetry; beta and filter parameters select quote shifts.
    """
    if (filter_enter_ticks > 0.0 and
            (beta == 0.0 or filter_exit_ticks < 0.0 or
             filter_exit_ticks >= filter_enter_ticks or
             filter_refresh_change_ticks <= 0.0 or filter_refresh_ns < 0)):
        raise ValueError("Invalid gap hysteresis configuration")
    if extra_half_ticks < 0.0 or not np.isfinite(extra_half_ticks):
        raise ValueError("Invalid GLFT no-signal widening")
    if beta != 0.0:
        if (okx_native_ns is None or okx_native_mid is None or
                basis_grid_ns is None or basis_values is None):
            raise ValueError("Nonzero beta needs causal OKX BBO and baseline arrays")
        if (len(okx_native_ns) != len(okx_native_mid) or
                len(basis_grid_ns) != len(basis_values)):
            raise ValueError("BBO and basis shapes differ")
        if not np.isfinite(beta):
            raise ValueError("Nonfinite beta")
    if engine.elapse(0) != 0:
        raise ValueError("Cannot initialize OKX market replay")
    t0 = engine.current_timestamp
    t_stop = t0 + (TRAIN + trade_steps) * STEP_NS
    next_sample = t0 + STEP_NS
    sample_count = 0
    idx = 0
    while idx < len(binance_visible_ns) and binance_visible_ns[idx] < t0:
        idx += 1

    arrivals = np.full(TRAIN, np.nan, np.float64)
    changes = np.full(TRAIN, np.nan, np.float64)
    updates = np.full((1 + trade_steps // UPDATE_EVERY_STEPS + 2, 9), np.nan)
    n_updates = 0
    prev_mid = np.nan
    a = k = sigma = half = skew = np.nan
    attempts = rejects = successes = initial_ok = 0
    bid_action = ask_action = t0 - MIN_REQUOTE_NS
    submits = cancels = buy_fills = sell_fills = decisions = 0
    okx_feed_events = order_reply_events = binance_events = 0
    observed_binance = 0.0
    count_binance_reads = 0
    latest_binance = np.nan
    latest_binance_ns = -1
    held_shift_ticks = 0.0
    last_shift_update_ns = t0 - filter_refresh_ns
    initial_trade_count = engine.state_values(0).num_trades
    max_position = 0.0
    min_equity = np.inf
    max_equity = -np.inf
    first_equity = np.nan
    max_drawdown = 0.0
    peak_equity = 0.0
    pos_abs_seconds = pos_sq_seconds = at_cap_seconds = 0.0
    fill_limit = max(2 * (trade_steps * STEP_NS // MIN_REQUOTE_NS) + 10, 100)
    fill_ts_ns = np.empty(fill_limit, np.int64)
    fill_sides = np.empty(fill_limit, np.int64)
    # Optional per-fill and per-order observation for exchange-time mechanism audit.
    # Disabled in every previously verified GLFT caller.
    fill_prices = np.empty(fill_limit if capture_mechanism else 1, np.int64)
    confirmed_cancel = np.empty((fill_limit if capture_mechanism else 1, 3), np.int64)
    cancel_count = 0
    n_fills = 0

    now = t0
    while now < t_stop:
        next_binance = t_stop
        if idx < len(binance_visible_ns):
            next_binance = binance_visible_ns[idx]
        due = min(t_stop, next_sample, next_binance)
        feed_status = 0
        if due > now:
            # Relative timeout in nanoseconds: do NOT pass an epoch timestamp.
            feed_status = engine.wait_next_feed(True, due - now)
            if feed_status == 1:
                break
            now = engine.current_timestamp
            if now > due:
                raise ValueError("HBT overshot event deadline")
            if feed_status == 2:
                okx_feed_events += 1
            elif feed_status == 3:
                order_reply_events += 1
            elif feed_status != 0:
                raise ValueError("Unexpected HBT event status")

        # HBT has already consumed all its events at this timestamp.
        # External rows at same timestamp are consumed IN ORIGINAL ROW ORDER.
        signal_event = False
        while idx < len(binance_visible_ns) and binance_visible_ns[idx] <= now:
            latest_binance = binance_mid[idx]
            latest_binance_ns = binance_visible_ns[idx]
            if read_binance:
                observed_binance += binance_mid[idx]  # Diagnostic telemetry only.
                count_binance_reads += 1
            binance_events += 1
            idx += 1
            signal_event = True

        sample_event = False
        if now >= next_sample:
            if now != next_sample:
                raise ValueError("Sample clock skipped an exact 100ms boundary")
            slot = sample_count % TRAIN
            furthest = -np.inf
            if np.isfinite(prev_mid):
                for tr in engine.last_trades(0):
                    price_tick = tr.px / TICK
                    if tr.ev & BUY_EVENT == BUY_EVENT:
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
            sample_event = True
            next_sample += STEP_NS
            recalibrate = (sample_count == TRAIN or
                           (rolling and sample_count > TRAIN and
                            (sample_count - TRAIN) % UPDATE_EVERY_STEPS == 0))
            if recalibrate:
                attempts += 1
                valid, new_a, new_k, new_sigma, fit_levels, crossed, extreme = (
                    fit_recent_arrivals(arrivals, changes))
                if valid:
                    new_half, new_skew = glft_coefficients(new_a, new_k, new_sigma, gamma_code)
                    if np.isfinite(new_half) and np.isfinite(new_skew) and new_half > 0 and new_skew > 0:
                        a, k, sigma, half, skew = (
                            new_a, new_k, new_sigma, new_half, new_skew)
                        successes += 1
                        if sample_count == TRAIN:
                            initial_ok = 1
                        updates[n_updates, 0] = (sample_count - TRAIN) * STEP_NS / 1e9
                        updates[n_updates, 1] = a
                        updates[n_updates, 2] = k
                        updates[n_updates, 3] = sigma
                        updates[n_updates, 4] = half
                        updates[n_updates, 5] = skew
                        updates[n_updates, 6] = fit_levels
                        updates[n_updates, 7] = crossed
                        updates[n_updates, 8] = extreme
                        n_updates += 1
                    else:
                        rejects += 1
                else:
                    rejects += 1

            # For fair reporting, inventory/equity are still sampled every 100ms.
            if sample_count > TRAIN:
                book = engine.depth(0)
                if book.best_bid_tick > 0 and book.best_ask_tick > book.best_bid_tick:
                    mid_price = (book.best_bid_tick + book.best_ask_tick) * TICK / 2
                    pos = engine.position(0)
                    st = engine.state_values(0)
                    equity = st.balance + CTVAL * pos * mid_price - st.fee
                    if not np.isfinite(first_equity):
                        first_equity = equity
                    min_equity = min(min_equity, equity)
                    max_equity = max(max_equity, equity)
                    max_position = max(max_position, abs(pos))
                    pos_abs_seconds += abs(pos) * STEP_NS / 1e9
                    pos_sq_seconds += pos * pos * STEP_NS / 1e9
                    if abs(pos) >= POSITION_CAP - 1e-9:
                        at_cap_seconds += STEP_NS / 1e9
                    peak_equity = max(peak_equity, equity)
                    max_drawdown = max(max_drawdown, peak_equity - equity)

        if sample_count > TRAIN and (feed_status == 2 or feed_status == 3 or
                                       signal_event or sample_event):
            # Observe live inventory peaks at every event, not only 100ms samples.
            max_position = max(max_position, abs(engine.position(0)))
            # Always collect fills BEFORE discarding inactive orders.
            orders = engine.orders(0)
            it = orders.values()
            while it.has_next():
                order = it.get()
                if order.status == FILLED:
                    if n_fills >= fill_limit:
                        raise ValueError("Exceeded fill trace allocation")
                    fill_ts_ns[n_fills] = order.exch_timestamp
                    fill_sides[n_fills] = order.side
                    if capture_mechanism:
                        fill_prices[n_fills] = order.price_tick
                    n_fills += 1
                    if order.side == BUY:
                        buy_fills += 1
                    else:
                        sell_fills += 1
                if capture_mechanism and order.status == CANCELED:
                    if cancel_count >= len(confirmed_cancel):
                        raise ValueError("Exceeded cancellation mechanism allocation")
                    confirmed_cancel[cancel_count, 0] = order.exch_timestamp
                    confirmed_cancel[cancel_count, 1] = order.side
                    confirmed_cancel[cancel_count, 2] = order.price_tick
                    cancel_count += 1
            engine.clear_inactive_orders(0)
            if now >= t_stop or not np.isfinite(half):
                continue

            book = engine.depth(0)
            bid, ask = book.best_bid_tick, book.best_ask_tick
            if bid <= 0 or ask <= bid or ask > 1_000_000_000:
                continue
            mid = (bid + ask) / 2.0
            pos = engine.position(0)
            # Use only last locally visible Binance BBO, strictly older OKX BBO,
            # and strictly earlier completed-second 60s causal basis.
            # Missing or stale quotes -> beta shift 0; no future labels enter.
            shift_ticks = 0.0
            if (beta != 0.0 and okx_native_ns is not None and
                    okx_native_mid is not None and basis_grid_ns is not None and
                    basis_values is not None):
                shift_ticks = causal_price_shift_ticks(
                    now, latest_binance_ns, latest_binance, beta,
                    okx_native_ns, okx_native_mid, basis_grid_ns, basis_values)
            if filter_enter_ticks > 0.0:
                held_shift_ticks, last_shift_update_ns = filtered_gap_shift(
                    now, shift_ticks, held_shift_ticks, last_shift_update_ns,
                    filter_enter_ticks, filter_exit_ticks,
                    filter_refresh_change_ticks, filter_refresh_ns)
                shift_ticks = held_shift_ticks
            reservation = mid - skew * pos + shift_ticks
            want_bid = min(round(reservation - half - extra_half_ticks), bid)
            want_ask = max(round(reservation + half + extra_half_ticks), ask)

            # One side may be pending cancellation; no replacement until cleared.
            has_bid = has_ask = False
            orders = engine.orders(0)
            it = orders.values()
            while it.has_next():
                order = it.get()
                if order.side == BUY:
                    has_bid = True
                    if (order.cancellable and order.price_tick != want_bid and
                            now - bid_action >= MIN_REQUOTE_NS):
                        engine.cancel(0, order.order_id, False)
                        bid_action = now
                        cancels += 1
                elif order.side == SELL:
                    has_ask = True
                    if (order.cancellable and order.price_tick != want_ask and
                            now - ask_action >= MIN_REQUOTE_NS):
                        engine.cancel(0, order.order_id, False)
                        ask_action = now
                        cancels += 1
            if (not has_bid and pos + ORDER_QTY <= POSITION_CAP + 1e-9 and
                    now - bid_action >= MIN_REQUOTE_NS):
                oid = int(want_bid) * 2
                engine.submit_buy_order(0, oid, want_bid*TICK, ORDER_QTY, GTX, LIMIT, False)
                bid_action = now
                submits += 1
            if (not has_ask and pos - ORDER_QTY >= -POSITION_CAP - 1e-9 and
                    now - ask_action >= MIN_REQUOTE_NS):
                oid = int(want_ask) * 2 + 1
                engine.submit_sell_order(0, oid, want_ask*TICK, ORDER_QTY, GTX, LIMIT, False)
                ask_action = now
                submits += 1
            decisions += 1

    # Last event can trigger a fill immediately before the data stream ends.
    orders = engine.orders(0)
    it = orders.values()
    while it.has_next():
        order = it.get()
        if order.status == FILLED:
            if n_fills >= fill_limit:
                raise ValueError("Exceeded last fill allocation")
            fill_ts_ns[n_fills] = order.exch_timestamp
            fill_sides[n_fills] = order.side
            if capture_mechanism:
                fill_prices[n_fills] = order.price_tick
            n_fills += 1
            if order.side == BUY:
                buy_fills += 1
            else:
                sell_fills += 1
        if capture_mechanism and order.status == CANCELED:
            if cancel_count >= len(confirmed_cancel):
                raise ValueError("Exceeded final cancellation mechanism allocation")
            confirmed_cancel[cancel_count, 0] = order.exch_timestamp
            confirmed_cancel[cancel_count, 1] = order.side
            confirmed_cancel[cancel_count, 2] = order.price_tick
            cancel_count += 1
    final = engine.state_values(0)
    book = engine.depth(0)
    final_mid = (book.best_bid + book.best_ask) / 2.0
    final_equity = final.balance + CTVAL * final.position * final_mid - final.fee
    max_position = max(max_position, abs(final.position))
    peak_equity = max(peak_equity, final_equity)
    max_drawdown = max(max_drawdown, peak_equity - final_equity)
    return (sample_count, decisions, attempts, rejects, successes, initial_ok,
            updates[:n_updates], submits, cancels, buy_fills, sell_fills,
            final.num_trades - initial_trade_count, final.position, max_position,
            final.balance, final.fee, final.trading_value, final_mid, final_equity,
            min_equity, max_equity, first_equity, max_drawdown,
            pos_abs_seconds, pos_sq_seconds, at_cap_seconds,
            binance_events, okx_feed_events, order_reply_events, count_binance_reads,
            observed_binance, fill_ts_ns[:n_fills], fill_sides[:n_fills],
            t0, now,
            fill_prices[:n_fills] if capture_mechanism else fill_prices[:0],
            confirmed_cancel[:cancel_count])


def summarize(raw, minutes, rolling, beta=0.0):
    """Aggregate only; no trades or raw market data leave the runner."""
    (samples, decisions, attempts, rejects, successes, initial_ok, updates,
     submits, cancels, buys, sells, fills, pos, max_pos, balance, fee,
     turnover, mid, equity, min_eq, max_eq, first_eq, drawdown,
     abs_seconds, sq_seconds, at_cap_seconds, bin_events, okx_events,
     order_events, reads, observed, fill_ts, fill_sides, start, end) = raw[:33]
    if initial_ok != 1 or successes < 1:
        raise RuntimeError("G0-R first 10 minutes cannot identify GLFT parameters")
    if samples < TRAIN + minutes * 600 - 5:
        raise RuntimeError(f"Truncated market replay, samples={samples}")
    if fills != buys+sells or len(fill_ts) != fills:
        raise RuntimeError(f"Official fills and strategy accounting disagree: {fills} vs {buys+sells}")
    if max_pos > POSITION_CAP + ORDER_QTY + 1e-9:
        raise RuntimeError("Inventory hard cap exceeded")
    if not math.isfinite(equity):
        raise RuntimeError("Nonfinite marked equity")
    if np.any(fill_ts <= 0):
        raise RuntimeError("Invalid exchange fill timestamps")
    return {
        "mode": "rolling" if rolling else "static",
        "beta": float(beta), "parameter_observation_ms": 100,
        "rolling_fit_interval_s": 5 if rolling else None,
        "min_per_side_order_action_ms": MIN_REQUOTE_NS/1e6,
        "start_time_ns": int(start), "end_time_ns": int(end),
        "samples": int(samples), "decisions": int(decisions),
        "calibration_attempts": int(attempts),
        "calibration_rejections": int(rejects),
        "successful_calibrations": int(successes),
        "initial_parameters": {
            "A_per_second": float(updates[0,1]),
            "k_per_tick": float(updates[0,2]),
            "sigma_tick_per_sqrt_second": float(updates[0,3]),
            "half_spread_ticks": float(updates[0,4]),
            "skew_ticks_per_contract": float(updates[0,5])
        },
        "last_parameters": {
            "A_per_second": float(updates[-1,1]),
            "k_per_tick": float(updates[-1,2]),
            "sigma_tick_per_sqrt_second": float(updates[-1,3]),
            "half_spread_ticks": float(updates[-1,4]),
            "skew_ticks_per_contract": float(updates[-1,5])
        },
        "calibration_trace": [
            {
                "elapsed_seconds_after_training": float(row[0]),
                "A_per_second": float(row[1]),
                "k_per_real_tick": float(row[2]),
                "sigma_tick_per_sqrt_second": float(row[3]),
                "half_spread_ticks": float(row[4]),
                "skew_ticks_per_contract": float(row[5])
            } for row in updates
        ],
        "time_count": {"binance": int(bin_events), "okx_feed": int(okx_events),
                       "order_reply": int(order_events), "binance_reads": int(reads)},
        "trading": {
            "submissions": int(submits), "cancel_requests": int(cancels),
            "buy_fills": int(buys), "sell_fills": int(sells), "engine_fills": int(fills),
            "end_position_contracts": float(pos),
            "max_abs_position_contracts": float(max_pos),
            "end_net_marked_equity_usdt": float(equity),
            "end_mid_usdt_per_btc": float(mid),
            "balance_usdt": float(balance), "fee_usdt": float(fee),
            "trading_value_usdt": float(turnover),
            "min_marked_equity_usdt": float(min_eq),
            "max_marked_equity_usdt": float(max_eq),
            "first_marked_equity_usdt": float(first_eq),
            "max_drawdown_usdt": float(drawdown),
            "mean_abs_inventory_contracts": float(abs_seconds/(max(samples-TRAIN, 1)*STEP_NS/1e9)),
            "rms_inventory_contracts": float(math.sqrt(sq_seconds/(max(samples-TRAIN, 1)*STEP_NS/1e9))),
            "seconds_at_inventory_cap": float(at_cap_seconds)
        },
        "fill_exchange_timestamp_min_ns": int(fill_ts.min()) if fills else None,
        "fill_exchange_timestamp_max_ns": int(fill_ts.max()) if fills else None,
        "fill_buy_sell_sign_sum": int(np.sum(fill_sides == BUY) - np.sum(fill_sides == SELL))
    }
