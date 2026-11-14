"""ISOLATED RESEARCH COPY of event-triggered G0-R GLFT using official HBT only.

The archived baseline scripts remain unchanged. This copy tests independent
source-attributed asymmetric quote defenses without custom order matching.

Original template: scripts/g0_glft_event_driven.py


Estimator: exactly the existing G0-R fixed 100ms ring and 5s refit.
Quoting: on OKX feed, order reply, Binance *clock*, or maintenance.
Default beta is zero; optional direct-gap/placebo/filter experimental modes.
"""
import math

import numpy as np
from numba import njit
from hftbacktest import BUY, BUY_EVENT, SELL, GTX, LIMIT, FILLED

from g0_glft_baseline import CTVAL, GAMMA, ORDER_QTY, POSITION_CAP, STEP_NS, TICK, TRAIN
from g0_glft_rolling import UPDATE_EVERY_STEPS, fit_recent_arrivals, glft_coefficients
from experiment_source_defense_signal import (
    defend_side_bps, widen_ticks_from_bps, next_defense_state,
)

MIN_REQUOTE_NS = 20_000_000  # Per side, at most one order action per 20ms.


@njit
def cancel_if_needed(engine, order, now, last_side_action_ns, want_price_tick,
                     dangerous_side, suppress):
    """Submit emergency evacuation immediately once the order is cancellable.

    Return 2 emergency, 1 normal requote, 0 no action. Only OUTWARD
    evacuation or full cancel overrides the usual 20ms per-side throttle.
    Pending/not-cancellable orders are never re-cancelled.
    """
    if not order.cancellable:
        return 0
    if dangerous_side:
        outward = (order.price_tick > want_price_tick if order.side == BUY
                   else order.price_tick < want_price_tick)
        if suppress or outward:
            engine.cancel(0, order.order_id, False)
            return 2
        # During active defense, never pull a protected order inward.
        return 0
    if (order.price_tick != want_price_tick and
            now - last_side_action_ns >= MIN_REQUOTE_NS):
        engine.cancel(0, order.order_id, False)
        return 1
    return 0



@njit
def wall_front_quote_tick(side_depth, roi_lb_tick, base_tick,
                          is_bid, search_ticks, tick_size=TICK, contract_value=CTVAL):
    """Anchor to the FIRST strict local notional peak outward from GLFT.

    Compare immediate neighboring ticks on the SAME side of visible OKX L2.
    A candidate must exceed BOTH neighbors; a flat plateau is not a peak.
    Both neighbors must exist in ROI (including beyond search-window edge).
    No peak -> original GLFT quote. No global-maximum selection.
    """
    if search_ticks <= 0:
        return base_tick
    for distance in range(1, search_ticks + 1):
        price_tick = base_tick - distance if is_bid else base_tick + distance
        idx = price_tick - roi_lb_tick
        if price_tick <= 0 or idx <= 0 or idx + 1 >= len(side_depth):
            break
        qty = side_depth[idx]
        if not np.isfinite(qty) or qty <= 0.0:
            continue
        candidate_notional = price_tick * tick_size * qty * contract_value
        prev_tick = price_tick + 1 if is_bid else price_tick - 1
        next_tick = price_tick - 1 if is_bid else price_tick + 1
        prev_qty = side_depth[prev_tick - roi_lb_tick]
        next_qty = side_depth[next_tick - roi_lb_tick]
        prev_notional = (prev_tick * tick_size * prev_qty * contract_value
                         if np.isfinite(prev_qty) and prev_qty > 0.0 else 0.0)
        next_notional = (next_tick * tick_size * next_qty * contract_value
                         if np.isfinite(next_qty) and next_qty > 0.0 else 0.0)
        if (candidate_notional > prev_notional + 1e-9 and
                candidate_notional > next_notional + 1e-9):
            return price_tick + 1 if is_bid else price_tick - 1
    return base_tick

@njit
def causal_price_shift_ticks(now, binance_visible_ns, binance_mid, beta,
                             okx_native_ns, current_target_mid_price, basis_grid_ns,
                             basis_values, tick_size=TICK):
    """Use the live HBT target mid; raw past BBO only gates signal freshness."""
    if (beta == 0.0 or not np.isfinite(binance_mid) or
            not np.isfinite(current_target_mid_price) or current_target_mid_price <= 0.0):
        return 0.0
    if binance_visible_ns < 0 or now < binance_visible_ns or now-binance_visible_ns > 250_000_000:
        return 0.0
    oi = np.searchsorted(okx_native_ns, now, side="left") - 1
    gi = np.searchsorted(basis_grid_ns, now, side="left") - 1
    if oi < 0 or gi < 0 or now < okx_native_ns[oi] or now-okx_native_ns[oi] > 250_000_000:
        return 0.0
    if not np.isfinite(basis_values[gi]):
        return 0.0
    gap_usdt = binance_mid - current_target_mid_price - basis_values[gi]
    return beta * gap_usdt / tick_size


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
def run_sparse_glft(engine, binance_visible_ns, binance_mid, trade_steps,
                   rolling=True, gamma_code=GAMMA, read_binance=False,
                   beta=0.0, okx_native_ns=None, okx_native_mid=None,
                   basis_grid_ns=None, basis_values=None,
                   filter_enter_ticks=0.0, filter_exit_ticks=0.0,
                   filter_refresh_change_ticks=10.0,
                   filter_refresh_ns=250_000_000,
                   extra_half_ticks=0.0, probe_ns=None,
                   wall_search_ticks=0, tick_size=TICK, contract_value=CTVAL,
                   order_qty=ORDER_QTY, position_cap=POSITION_CAP,
                   recorder=None, sparse_mode=0,
                   ema_grid_ns=None, ema_values=None,
                   impulse_window_ns=100_000_000,
                   gap_gate_bps=0.5, impulse_gate_bps=0.2,
                   hysteresis_exit_th_bps=0.1, widen_bps=0.5,
                   defense_hold_ns=75_000_000,
                   defense_cooldown_ns=75_000_000):
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
    if wall_search_ticks < 0 or wall_search_ticks > 1000:
        raise ValueError("Wall search must be 0 to 1000 ticks from GLFT price")
    if beta != 0.0:
        if (okx_native_ns is None or okx_native_mid is None or
                basis_grid_ns is None or basis_values is None):
            raise ValueError("Nonzero beta needs causal OKX BBO and baseline arrays")
        if (len(okx_native_ns) != len(okx_native_mid) or
                len(basis_grid_ns) != len(basis_values)):
            raise ValueError("BBO and basis shapes differ")
        if not np.isfinite(beta):
            raise ValueError("Nonfinite beta")
    if sparse_mode not in (0,1,2,3):
        raise ValueError("Sparse mode must be 0(standard),1(widen),2(cancel),3(local-placebo)")
    if sparse_mode:
        if (okx_native_ns is None or okx_native_mid is None or
                ema_grid_ns is None or ema_values is None):
            raise ValueError("Sparse mode needs native OKX BBO and historical EMA")
        if (len(okx_native_ns) != len(okx_native_mid) or
                len(ema_grid_ns) != len(ema_values)):
            raise ValueError("Sparse array lengths disagree")
        if (impulse_window_ns<=0 or defense_cooldown_ns<0 or
                not np.isfinite(gap_gate_bps) or gap_gate_bps<=0 or
                not np.isfinite(impulse_gate_bps) or impulse_gate_bps<=0 or
                not np.isfinite(hysteresis_exit_th_bps) or
                hysteresis_exit_th_bps<0 or
                hysteresis_exit_th_bps>=min(gap_gate_bps,impulse_gate_bps) or
                not np.isfinite(widen_bps) or widen_bps<=0 or
                defense_hold_ns<0 or defense_hold_ns>100_000_000):
            raise ValueError("Invalid sparse protection parameters")
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
    defense_side = 0
    defense_started_ns = -1
    last_defense_end_ns = t0 - defense_cooldown_ns
    sparse_gate_checks = sparse_gate_hits = sparse_activation_count = 0
    sparse_binance_only_safe_side_cancel = 0
    sparse_bid_cancel = sparse_ask_cancel = 0
    sparse_bid_submit = sparse_ask_submit = 0
    urgent_bid_cancels = urgent_ask_cancels = 0
    ordinary_bid_cancels = ordinary_ask_cancels = 0
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
    # Retain short-window allocation; cap long-day traces to avoid O(day_length)
    # preallocation (a full-day upper bound would consume ~170MB per model).
    # Existing FILLED observations still fail loudly if this cap is reached.
    fill_limit = min(max(2 * (trade_steps * STEP_NS // MIN_REQUOTE_NS) + 10, 100),
                     100_000)
    fill_ts_ns = np.empty(fill_limit, np.int64)
    fill_sides = np.empty(fill_limit, np.int64)
    fill_observed_ns = np.empty(fill_limit, np.int64)
    fill_exec_price = np.empty(fill_limit, np.float64)
    fill_exec_qty = np.empty(fill_limit, np.float64)
    n_fills = 0
    # Optional read-only audit deadlines, never a trading signal/trigger.
    n_probes = 0
    if probe_ns is not None:
        n_probes = len(probe_ns)
        for jj in range(1, n_probes):
            if probe_ns[jj] <= probe_ns[jj-1]:
                raise ValueError("Diagnostic probe timestamps must increase")
    probe_rows = np.full((n_probes, 7), np.nan)
    probe_idx = 0

    now = t0
    while now < t_stop:
        next_binance = t_stop
        if idx < len(binance_visible_ns):
            next_binance = binance_visible_ns[idx]
        due = min(t_stop, next_sample, next_binance)
        if probe_ns is not None and probe_idx < n_probes:
            if probe_ns[probe_idx] >= now:
                due = min(due, probe_ns[probe_idx])
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
                    price_tick = tr.px / tick_size
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
                    new_half, new_skew = glft_coefficients(new_a, new_k, new_sigma, gamma_code, order_qty)
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
                    mid_price = (book.best_bid_tick + book.best_ask_tick) * tick_size / 2
                    pos = engine.position(0)
                    st = engine.state_values(0)
                    equity = st.balance + contract_value * pos * mid_price - st.fee
                    if not np.isfinite(first_equity):
                        first_equity = equity
                    min_equity = min(min_equity, equity)
                    max_equity = max(max_equity, equity)
                    max_position = max(max_position, abs(pos))
                    pos_abs_seconds += abs(pos) * STEP_NS / 1e9
                    pos_sq_seconds += pos * pos * STEP_NS / 1e9
                    if abs(pos) >= position_cap - 1e-9:
                        at_cap_seconds += STEP_NS / 1e9
                    if recorder is not None:
                        recorder.record(engine)
                    peak_equity = max(peak_equity, equity)
                    max_drawdown = max(max_drawdown, peak_equity - equity)

        # Local order snapshot at preselected counterfactual observation times.
        # Pure audit: does not submit, cancel or trigger the quoting decision.
        if probe_ns is not None:
            while probe_idx < n_probes and probe_ns[probe_idx] <= now:
                best_buy = -1.0
                best_sell = 1e12
                count_buy = count_sell = 0
                cancellable_buy = cancellable_sell = 0
                values = engine.orders(0).values()
                while values.has_next():
                    oo = values.get()
                    if oo.status == FILLED:
                        continue
                    if oo.side == BUY:
                        count_buy += 1
                        if oo.cancellable:
                            cancellable_buy += 1
                            best_buy = max(best_buy, float(oo.price_tick))
                    elif oo.side == SELL:
                        count_sell += 1
                        if oo.cancellable:
                            cancellable_sell += 1
                            best_sell = min(best_sell, float(oo.price_tick))
                probe_rows[probe_idx, 0] = best_buy if best_buy >= 0 else np.nan
                probe_rows[probe_idx, 1] = best_sell if best_sell < 1e11 else np.nan
                probe_rows[probe_idx, 2] = float(count_buy)
                probe_rows[probe_idx, 3] = float(count_sell)
                probe_rows[probe_idx, 4] = float(cancellable_buy)
                probe_rows[probe_idx, 5] = float(cancellable_sell)
                probe_rows[probe_idx, 6] = engine.position(0)
                probe_idx += 1

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
                    fill_observed_ns[n_fills] = now
                    fill_exec_price[n_fills] = order.exec_price
                    fill_exec_qty[n_fills] = order.exec_qty
                    n_fills += 1
                    if order.side == BUY:
                        buy_fills += 1
                    else:
                        sell_fills += 1
            engine.clear_inactive_orders(0)
            if now >= t_stop or not np.isfinite(half):
                continue

            book = engine.depth(0)
            bid, ask = book.best_bid_tick, book.best_ask_tick
            if bid <= 0 or ask <= bid or ask > 1_000_000_000:
                continue
            mid = (bid + ask) / 2.0
            pos = engine.position(0)
            # Use current HBT target mid for both reservation and cross-venue gap.
            # Strict-past native BBO is only a freshness gate; basis stays causal.
            # Missing or stale quotes -> beta shift 0; no future labels enter.
            shift_ticks = 0.0
            if (beta != 0.0 and okx_native_ns is not None and
                    okx_native_mid is not None and basis_grid_ns is not None and
                    basis_values is not None):
                shift_ticks = causal_price_shift_ticks(
                    now, latest_binance_ns, latest_binance, beta,
                    okx_native_ns, mid * tick_size, basis_grid_ns, basis_values,
                    tick_size)
            if filter_enter_ticks > 0.0:
                held_shift_ticks, last_shift_update_ns = filtered_gap_shift(
                    now, shift_ticks, held_shift_ticks, last_shift_update_ns,
                    filter_enter_ticks, filter_exit_ticks,
                    filter_refresh_change_ticks, filter_refresh_ns)
                shift_ticks = held_shift_ticks
            # Step2: strictly causal target-mid-relative bps gates.
            # BBO midpoint movement is a price impulse, not signed trade flow.
            previous_defense_side = defense_side
            candidate_defense_side = 0
            gap_bps = np.nan
            impulse_bps = np.nan
            if sparse_mode:
                sparse_gate_checks += 1
                oi = np.searchsorted(okx_native_ns, now, side="left") - 1
                gi = np.searchsorted(ema_grid_ns, now, side="left") - 1
                valid = (
                    oi >= 0 and gi >= 0 and
                    latest_binance_ns >= 0 and
                    now >= latest_binance_ns and
                    now - latest_binance_ns <= 250_000_000 and
                    now >= okx_native_ns[oi] and
                    now - okx_native_ns[oi] <= 250_000_000 and
                    np.isfinite(ema_values[gi]))
                if valid:
                    mid_price = mid * tick_size
                    gap_bps = 1.e4 * (
                        latest_binance - mid_price - ema_values[gi]
                    ) / mid_price
                    window_start = now - impulse_window_ns
                    if sparse_mode in (1, 2):
                        past_idx = np.searchsorted(
                            binance_visible_ns, window_start, side="right") - 1
                        if (past_idx >= 0 and
                                window_start >= binance_visible_ns[past_idx] and
                                window_start-binance_visible_ns[past_idx] <= 250_000_000):
                            impulse_bps = 1.e4 * (
                                latest_binance-binance_mid[past_idx]) / mid_price
                    else:
                        past_idx = np.searchsorted(
                            okx_native_ns, window_start, side="right") - 1
                        if (past_idx >= 0 and
                                window_start >= okx_native_ns[past_idx] and
                                window_start-okx_native_ns[past_idx] <= 250_000_000):
                            impulse_bps = 1.e4 * (
                                okx_native_mid[oi]-okx_native_mid[past_idx]) / mid_price
                    if np.isfinite(impulse_bps):
                        if sparse_mode == 3:
                            candidate_defense_side = defend_side_bps(
                                gap_bps, 0., impulse_bps, sparse_mode,
                                gap_gate_bps, impulse_gate_bps)
                        else:
                            candidate_defense_side = defend_side_bps(
                                gap_bps, impulse_bps, 0., sparse_mode,
                                gap_gate_bps, impulse_gate_bps)
                        if candidate_defense_side != 0:
                            sparse_gate_hits += 1
            # A non-renewable <=100ms hold, independent of message frequency.
            # A *both-low* signal removes protection immediately.
            # Local placebo's impulse sign points opposite to gap direction.
            effective_impulse = -impulse_bps if sparse_mode == 3 else impulse_bps
            defense_side, defense_started_ns, last_defense_end_ns = (
                next_defense_state(
                    now, defense_side, defense_started_ns, last_defense_end_ns,
                    candidate_defense_side, gap_bps, effective_impulse,
                    hysteresis_exit_th_bps, defense_hold_ns, defense_cooldown_ns))
            if defense_side != 0 and previous_defense_side != defense_side:
                sparse_activation_count += 1
            reservation = mid - skew * pos + shift_ticks
            want_bid = min(round(reservation - half - extra_half_ticks), bid)
            want_ask = max(round(reservation + half + extra_half_ticks), ask)
            if wall_search_ticks > 0:
                # Post-process ONLY: keep GLFT A/k/sigma/risk/skew unchanged.
                # Bound search to OUTWARD prices, then step one tick in front.
                want_bid = wall_front_quote_tick(
                    book.bid_depth, book.roi_lb_tick, int(want_bid),
                    True, wall_search_ticks, tick_size, contract_value)
                want_ask = wall_front_quote_tick(
                    book.ask_depth, book.roi_lb_tick, int(want_ask),
                    False, wall_search_ticks, tick_size, contract_value)
                # Never cross the market or improve beyond GLFT target.
                want_bid = min(want_bid, bid)
                want_ask = max(want_ask, ask)

            # Sparse price action is one-sided. A Binance-only timestamp may
            # alter the dangerous side, but never price/replace the safe side.
            if defense_side != 0 and sparse_mode != 2:
                width = widen_ticks_from_bps(mid*tick_size, tick_size, widen_bps)
                if defense_side == 1:
                    want_ask += width
                else:
                    want_bid -= width
            # Reapply post-only clamp after every experimental quote branch.
            want_bid = min(want_bid, bid)
            want_ask = max(want_ask, ask)
            if want_bid >= ask or want_ask <= bid:
                raise ValueError("Experimental quote crossed target best opposite")
            binance_only = (sparse_mode != 0 and signal_event and
                            feed_status == 0 and not sample_event)
            # When one side is defended, NEVER recompute/cancel/replace the
            # opposite-side resting quote, even on local OKX market events.
            allow_bid = ((not binance_only or
                          defense_side == -1 or previous_defense_side == -1)
                         and defense_side != 1)
            allow_ask = ((not binance_only or
                          defense_side == 1 or previous_defense_side == 1)
                         and defense_side != -1)
            suppress_bid = (sparse_mode == 2 and defense_side == -1)
            suppress_ask = (sparse_mode == 2 and defense_side == 1)
            # One side may be pending cancellation; no replacement until cleared.
            has_bid = has_ask = False
            orders = engine.orders(0)
            it = orders.values()
            while it.has_next():
                order = it.get()
                if order.side == BUY:
                    has_bid = True
                    if allow_bid:
                        disposition = cancel_if_needed(
                            engine, order, now, bid_action, want_bid,
                            defense_side == -1, suppress_bid)
                        if disposition != 0:
                            bid_action = now
                            cancels += 1
                            sparse_bid_cancel += 1
                            if disposition == 2:
                                urgent_bid_cancels += 1
                            else:
                                ordinary_bid_cancels += 1
                elif order.side == SELL:
                    has_ask = True
                    if allow_ask:
                        disposition = cancel_if_needed(
                            engine, order, now, ask_action, want_ask,
                            defense_side == 1, suppress_ask)
                        if disposition != 0:
                            ask_action = now
                            cancels += 1
                            sparse_ask_cancel += 1
                            if disposition == 2:
                                urgent_ask_cancels += 1
                            else:
                                ordinary_ask_cancels += 1
            if (allow_bid and not suppress_bid and
                    not has_bid and pos + order_qty <= position_cap + 1e-9 and
                    now - bid_action >= MIN_REQUOTE_NS):
                oid = int(want_bid) * 2
                engine.submit_buy_order(0, oid, want_bid*tick_size, order_qty, GTX, LIMIT, False)
                bid_action = now
                submits += 1
                sparse_bid_submit += 1
            if (allow_ask and not suppress_ask and
                    not has_ask and pos - order_qty >= -position_cap - 1e-9 and
                    now - ask_action >= MIN_REQUOTE_NS):
                oid = int(want_ask) * 2 + 1
                engine.submit_sell_order(0, oid, want_ask*tick_size, order_qty, GTX, LIMIT, False)
                ask_action = now
                submits += 1
                sparse_ask_submit += 1
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
            fill_observed_ns[n_fills] = now
            fill_exec_price[n_fills] = order.exec_price
            fill_exec_qty[n_fills] = order.exec_qty
            n_fills += 1
            if order.side == BUY:
                buy_fills += 1
            else:
                sell_fills += 1
    final = engine.state_values(0)
    book = engine.depth(0)
    final_mid = (book.best_bid + book.best_ask) / 2.0
    final_equity = final.balance + contract_value * final.position * final_mid - final.fee
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
            t0, (now, fill_observed_ns[:n_fills],
                 fill_exec_price[:n_fills], fill_exec_qty[:n_fills],
                 probe_rows),
            np.array([sparse_gate_checks, sparse_gate_hits,
                      sparse_activation_count,
                      sparse_binance_only_safe_side_cancel,
                      sparse_bid_cancel, sparse_ask_cancel,
                      sparse_bid_submit, sparse_ask_submit,
                      urgent_bid_cancels, urgent_ask_cancels,
                      ordinary_bid_cancels, ordinary_ask_cancels], dtype=np.int64))


def summarize(raw, minutes, rolling, beta=0.0, position_cap=POSITION_CAP,
              order_qty=ORDER_QTY):
    """Aggregate only; no trades or raw market data leave the runner."""
    (samples, decisions, attempts, rejects, successes, initial_ok, updates,
     submits, cancels, buys, sells, fills, pos, max_pos, balance, fee,
     turnover, mid, equity, min_eq, max_eq, first_eq, drawdown,
     abs_seconds, sq_seconds, at_cap_seconds, bin_events, okx_events,
     order_events, reads, observed, fill_ts, fill_sides, start, end) = raw[:35]
    # Diagnostic variants return additional trace arrays with the end timestamp.
    if isinstance(end, tuple):
        end = end[0]
    if initial_ok != 1 or successes < 1:
        raise RuntimeError("G0-R first 10 minutes cannot identify GLFT parameters")
    if samples < TRAIN + minutes * 600 - 5:
        raise RuntimeError(f"Truncated market replay, samples={samples}")
    if fills != buys+sells or len(fill_ts) != fills:
        raise RuntimeError(f"Official fills and strategy accounting disagree: {fills} vs {buys+sells}")
    if max_pos > position_cap + order_qty + 1e-9:
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
