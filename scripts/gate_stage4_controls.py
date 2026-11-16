"""Isolated phase-4 control copy; HBT matching, old GLFT and Binance beta1 untouched.\n\nFresh Gate GLFT research branch: sigma gate, Binance-origin filter and two quote modes.

Copied the event/reply/queue/fee/accounting backbone from g0_glft_event_driven
without modifying that module. Gate-specific signal/price transformation is
the only change to core strategy decisions; no prior P90 filter or L2 wall.
"""
import math
import numpy as np
from numba import njit
from hftbacktest import BUY, BUY_EVENT, SELL, GTX, LIMIT, FILLED
from g0_glft_baseline import CTVAL, GAMMA, ORDER_QTY, POSITION_CAP, STEP_NS, TICK, TRAIN
from g0_glft_rolling import UPDATE_EVERY_STEPS, fit_recent_arrivals, glft_coefficients
from g0_glft_event_driven import MIN_REQUOTE_NS, summarize
@njit
def gate_origin_filter(gap, sigma, binance_price, gate_price, base_b, base_m,
                       has_base, active, binance_price_changed, gate_feed_same_time):
    """Single causal state transition: normal anchors, trigger, dominance, origin."""
    above = dominant = activated = tie_block = 0
    eff = 0.0
    if (not np.isfinite(gap) or not np.isfinite(sigma) or sigma <= 0
            or not np.isfinite(binance_price) or not np.isfinite(gate_price)):
        return eff, base_b, base_m, has_base, False, above, dominant, activated, tie_block
    if abs(gap) <= sigma:
        return 0.0, binance_price, gate_price, True, False, above, dominant, activated, tie_block
    if not has_base or abs(gap) <= 1.645 * sigma:
        return 0.0, base_b, base_m, has_base, False, above, dominant, activated, tie_block
    above = 1
    delta_b = binance_price - base_b
    delta_m = gate_price - base_m
    dominance = ((gap > 0 and delta_b > 0) or (gap < 0 and delta_b < 0))
    if not dominance or abs(delta_b) <= abs(delta_m):
        return 0.0, base_b, base_m, has_base, False, above, dominant, activated, tie_block
    dominant = 1
    if not active:
        if binance_price_changed and not gate_feed_same_time:
            active = True
            activated = 1
        elif binance_price_changed:
            tie_block = 1
    if active:
        eff = gap
    return eff, base_b, base_m, has_base, active, above, dominant, activated, tie_block


@njit
def run_gate_stage4_control(engine, binance_visible_ns, binance_mid, trade_steps,
                         beta, basis_grid_ns, basis_values, basis_sigmas,
                         quote_mode, gamma_code=GAMMA,
                         tick_size=TICK, contract_value=CTVAL,
                         order_qty=ORDER_QTY, position_cap=POSITION_CAP,
                         recorder=None, stop_at_ns=0, control_kind=0,\n                         extra_half_ticks=0.0, local_sigma_ceiling=np.inf):
    """Two sigma gates + Binance-origin anchor; HBT retains all matching/queues."""
    if not np.isfinite(beta) or beta < 0 or beta > 1:
        raise ValueError("Invalid beta")
    if quote_mode not in (0,1,2):
        raise ValueError("0=baseline, 1=both, 2=defensive")
    if control_kind not in (0,1,2):\n        raise ValueError("Stage4: 0=frozen reference, 1=static widen, 2=Gate-local sigma pause")\n    if not np.isfinite(extra_half_ticks) or extra_half_ticks < 0.0:\n        raise ValueError("Stage4: invalid widening offset")\n    if local_sigma_ceiling <= 0.0 or np.isnan(local_sigma_ceiling):\n        raise ValueError("Stage4: invalid Gate sigma threshold")\n    if (len(basis_grid_ns) != len(basis_values) or
            len(basis_values) != len(basis_sigmas)):
        raise ValueError("Basis and sigma lengths mismatch")
    rolling = True
    read_binance = True
    probe_ns = None
    if engine.elapse(0) != 0:
        raise ValueError("Cannot initialize OKX market replay")
    t0 = engine.current_timestamp
    t_stop = t0 + (TRAIN + trade_steps) * STEP_NS
    if stop_at_ns > 0:
        if stop_at_ns <= t0 + TRAIN * STEP_NS:
            raise ValueError("UTC stopping deadline falls before trading")
        t_stop = min(t_stop, stop_at_ns)
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
    # Price anchors are updated ONLY while residual is inside the 1-sigma band.
    base_b = np.nan
    base_m = np.nan
    has_base = False
    active_origin = False
    price_updates = trigger_checks = above_trigger = direction_ok = 0
    activated = active_decisions = 0
    tie_excluded = 0
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
        prior_binance = latest_binance
        while idx < len(binance_visible_ns) and binance_visible_ns[idx] <= now:
            latest_binance = binance_mid[idx]
            latest_binance_ns = binance_visible_ns[idx]
            if read_binance:
                observed_binance += binance_mid[idx]  # Diagnostic telemetry only.
                count_binance_reads += 1
            binance_events += 1
            idx += 1
            signal_event = True
        binance_price_change = (signal_event and np.isfinite(prior_binance)
                                and latest_binance != prior_binance)
        if binance_price_change:
            price_updates += 1

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
            # Price difference is between *now-visible* Binance and HBT Gate mid.
            # The 600s time-weighted normal spread/sigma were published at 5s
            # deadlines and read strictly before this decision timestamp.
            shift_ticks = 0.0
            if beta > 0.0 and np.isfinite(latest_binance):
                gi = np.searchsorted(basis_grid_ns, now, side="left") - 1
                if gi >= 0 and np.isfinite(basis_values[gi]) and (
                        np.isfinite(basis_sigmas[gi]) and basis_sigmas[gi] > 0.0):
                    observed_gap = latest_binance - mid * tick_size - basis_values[gi]
                    historical_sigma = basis_sigmas[gi]
                    trigger_checks += 1
                    (effective_gap, base_b, base_m, has_base, active_origin,
                     one_above, one_dominant, one_activated, one_tie) = gate_origin_filter(
                        observed_gap, historical_sigma, latest_binance,
                        mid * tick_size, base_b, base_m, has_base,
                        active_origin, binance_price_change, feed_status == 2)
                    above_trigger += one_above
                    direction_ok += one_dominant
                    activated += one_activated
                    tie_excluded += one_tie
                    if effective_gap != 0.0:
                        shift_ticks = effective_gap / tick_size
                        active_decisions += 1
                else:
                    active_origin = False
            reservation = mid - skew * pos
            # Control A widens GLFT symmetrically. B pauses when Gate-local sigma high.
            widened_half = half + extra_half_ticks if control_kind == 1 else half
            base_bid = min(round(reservation - widened_half), bid)
            base_ask = max(round(reservation + widened_half), ask)
            vol_pause = control_kind == 2 and sigma > local_sigma_ceiling
            want_bid, want_ask = base_bid, base_ask
            if shift_ticks != 0.0:
                offset = beta * shift_ticks
                if quote_mode == 1:
                    # Improve inside spread only while strictly post-only.
                    want_bid = min(round(base_bid + offset), ask - 1)
                    want_ask = max(round(base_ask + offset), bid + 1)
                elif quote_mode == 2:
                    if shift_ticks > 0:
                        want_ask = max(round(base_ask + offset), bid + 1)
                    else:
                        want_bid = min(round(base_bid + offset), ask - 1)
            if want_bid >= want_ask:
                raise ValueError("Proposed self-crossed quote prices")
            # One side may be pending cancellation; no replacement until cleared.
            has_bid = has_ask = False
            orders = engine.orders(0)
            it = orders.values()
            while it.has_next():
                order = it.get()
                if order.side == BUY:
                    has_bid = True
                    if (order.cancellable and (vol_pause or order.price_tick != want_bid) and
                            now - bid_action >= MIN_REQUOTE_NS):
                        engine.cancel(0, order.order_id, False)
                        bid_action = now
                        cancels += 1
                elif order.side == SELL:
                    has_ask = True
                    if (order.cancellable and (vol_pause or order.price_tick != want_ask) and
                            now - ask_action >= MIN_REQUOTE_NS):
                        engine.cancel(0, order.order_id, False)
                        ask_action = now
                        cancels += 1
            if (not vol_pause and not has_bid and pos + order_qty <= position_cap + 1e-9 and
                    now - bid_action >= MIN_REQUOTE_NS):
                oid = int(want_bid) * 2
                engine.submit_buy_order(0, oid, want_bid*tick_size, order_qty, GTX, LIMIT, False)
                bid_action = now
                submits += 1
            if (not vol_pause and not has_ask and pos - order_qty >= -position_cap - 1e-9 and
                    now - ask_action >= MIN_REQUOTE_NS):
                oid = int(want_ask) * 2 + 1
                engine.submit_sell_order(0, oid, want_ask*tick_size, order_qty, GTX, LIMIT, False)
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
            np.array([price_updates,trigger_checks,above_trigger,direction_ok,
                      activated,active_decisions,tie_excluded],dtype=np.int64))

