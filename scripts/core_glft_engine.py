"""Unified Gate GLFT HBT loop: legacy baseline, Stage3 diagnostics, Stage4 controls, Stage5A pause.\n\nFresh Gate GLFT research branch: sigma gate, Binance-origin filter and two quote modes.

Copied the event/reply/queue/fee/accounting backbone from g0_glft_event_driven
without modifying that module. Gate-specific signal/price transformation is
the only change to core strategy decisions; no prior P90 filter or L2 wall.
"""
import math
import numpy as np
from numba import njit
from hftbacktest import BUY, BUY_EVENT, SELL, GTX, LIMIT, FILLED
from g0_glft_baseline import CTVAL, GAMMA, ORDER_QTY, POSITION_CAP, STEP_NS, TICK, TRAIN
from g0_glft_rolling import (UPDATE_EVERY_STEPS, fit_recent_arrivals,
                             fit_physical_1tick_poisson, glft_coefficients)
from core_glft_rules import (quoted_ticks, last_visible_bid_tick,
    last_visible_ask_tick, cap_quotes_to_visible_depth,
    book_update_unlocks, note_order_operation)
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
def run_core_glft(engine, binance_visible_ns, binance_mid, trade_steps,
                         beta, basis_grid_ns, basis_values, basis_sigmas,
                         quote_mode, gamma_code=GAMMA,
                         tick_size=TICK, contract_value=CTVAL,
                         order_qty=ORDER_QTY, position_cap=POSITION_CAP,
                         recorder=None, stop_at_ns=0, probe_ns=None, shadow_gate=False,
                         control_kind=0, extra_half_ticks=0.0,
                         local_sigma_ceiling=np.inf,
                         min_requote_ns=MIN_REQUOTE_NS,
                         fit_mode="legacy_half_tick",
                         wait_book_after_fill=False,
                         post_only_clamp_mode="legacy",
                         cap_depth_at_visible_levels=False,
                         forced_start_ns=0,
                         gate_book_local_ns=None, gate_book_exchange_ns=None,
                         depth_is_20_level_snapshot=False,
                         diagnostics=None):
    """Two sigma gates + Binance-origin anchor; HBT retains all matching/queues."""
    if not np.isfinite(beta) or beta < 0 or beta > 1:
        raise ValueError("Invalid beta")
    if quote_mode not in (0,1,2,3):
        raise ValueError("0=baseline, 1=both, 2=defensive, 3=risk-side pause")
    if control_kind not in (0,1,2):
        raise ValueError("0=reference, 1=static-widen, 2=Gate-vol pause")
    if not np.isfinite(extra_half_ticks) or extra_half_ticks < 0:
        raise ValueError("Bad Stage4 widening offset")
    if local_sigma_ceiling <= 0 or np.isnan(local_sigma_ceiling):
        raise ValueError("Bad Gate sigma pause threshold")
    if min_requote_ns not in (0,20_000_000):
        raise ValueError("min_requote_ns must be 0 or 20_000_000")
    if fit_mode not in ("legacy_half_tick","physical_1tick_poisson"):
        raise ValueError("Unknown GLFT fit_mode")
    if post_only_clamp_mode not in ("legacy","unified"):
        raise ValueError("post_only_clamp_mode must be legacy or unified")
    if forced_start_ns < 0:
        raise ValueError("forced_start_ns cannot be negative")
    if diagnostics is not None and len(diagnostics)<7:
        raise ValueError("Seven diagnostic slots required")
    if fit_mode == "physical_1tick_poisson" and diagnostics is None:
        raise ValueError("Physical estimator requires diagnostic start/coverage output")
    # Always use concrete arrays: Numba must compile the disabled lock path too.
    book_locals = np.empty(0,np.int64)
    book_exchanges = np.empty(0,np.int64)
    if gate_book_local_ns is not None:
        book_locals = gate_book_local_ns
    if gate_book_exchange_ns is not None:
        book_exchanges = gate_book_exchange_ns
    if wait_book_after_fill:
        if len(book_locals) != len(book_exchanges) or len(book_locals)==0:
            raise ValueError("Market sync requires complete causal Gate BBO update clocks")
        for jj in range(len(book_locals)):
            if (book_exchanges[jj]>book_locals[jj]
                    or (jj>0 and book_locals[jj]<book_locals[jj-1])):
                raise ValueError("Invalid Gate BBO update clock order")
    if cap_depth_at_visible_levels and not depth_is_20_level_snapshot:
        raise ValueError("20-level boundary cap requires verified 20-level snapshot source")
    if (len(basis_grid_ns) != len(basis_values) or
            len(basis_values) != len(basis_sigmas)):
        raise ValueError("Basis and sigma lengths mismatch")
    rolling = True
    read_binance = True
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
    bid_arrivals = np.full(TRAIN, np.nan, np.float64)
    ask_arrivals = np.full(TRAIN, np.nan, np.float64)
    changes = np.full(TRAIN, np.nan, np.float64)
    updates = np.full((1 + trade_steps // UPDATE_EVERY_STEPS + 2, 9), np.nan)
    n_updates = 0
    prev_mid = np.nan
    a = k = sigma = half = skew = np.nan
    attempts = rejects = successes = initial_ok = 0
    bid_action = ask_action = t0 - min_requote_ns
    first_fit_ns = -1
    awaiting_market_sync = False
    last_fill_exchange_ns = -1
    last_fill_observed_ns = -1
    last_observed_position = engine.position(0)
    book_idx = 0
    if wait_book_after_fill:
        while book_idx < len(book_locals) and book_locals[book_idx] <= t0:
            book_idx += 1
    if forced_start_ns and forced_start_ns < t0 + TRAIN*STEP_NS:
        raise ValueError("forced_start_ns before 10min training")
    op_times = np.empty(100_000 if diagnostics is not None else 1,np.int64)
    op_head = op_tail = max_ops = 0
    resting_bid_ns = resting_ask_ns = 0
    last_resting_check_ns = t0
    prior_bid_resting = prior_ask_resting = False
    # Local confirmation lower bounds for HBT resting exposure.
    # Pending submit/cancel packet exchange-side state is not fully observable here.
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
    last_shadow_effective = 0.0
    last_shadow_gap = np.nan
    last_shadow_sigma = np.nan
    last_shadow_decision = -1
    last_shadow_activation = -1
    last_shadow_bid_target = np.nan
    last_shadow_ask_target = np.nan
    last_shadow_gate_mid = np.nan
    last_shadow_above = 0
    last_shadow_dominant = 0
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
    fill_limit = min(max(2 * (trade_steps * STEP_NS // max(1_000_000, min_requote_ns)) + 10, 100),
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
    probe_rows = np.full((n_probes, 25), np.nan)
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
        if wait_book_after_fill and awaiting_market_sync and book_idx < len(book_locals):
            due = min(due, max(now, book_locals[book_idx]))
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

        if diagnostics is not None and now > last_resting_check_ns:
            elapsed = now-last_resting_check_ns
            if prior_bid_resting:
                resting_bid_ns += elapsed
            if prior_ask_resting:
                resting_ask_ns += elapsed
            last_resting_check_ns = now
            current_bid_resting = current_ask_resting = False
            view = engine.orders(0).values()
            while view.has_next():
                current_order = view.get()
                # Locally cancellable = confirmed open HBT order, conservative proxy.
                if current_order.cancellable:
                    if current_order.side == BUY:
                        current_bid_resting = True
                    elif current_order.side == SELL:
                        current_ask_resting = True
            prior_bid_resting = current_bid_resting
            prior_ask_resting = current_ask_resting
        # Consume separately timestamped real Gate BBO updates. A trade-only
        # feed_status==2 is insufficient to release the private-fill lock.
        if wait_book_after_fill:
            while book_idx < len(book_locals) and book_locals[book_idx] <= now:
                if awaiting_market_sync and book_update_unlocks(
                        last_fill_exchange_ns,last_fill_observed_ns,
                        book_exchanges[book_idx],book_locals[book_idx]):
                    awaiting_market_sync = False
                book_idx += 1
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
            bid_furthest = ask_furthest = -np.inf
            if np.isfinite(prev_mid):
                for tr in engine.last_trades(0):
                    price_tick = tr.px / tick_size
                    if tr.ev & BUY_EVENT == BUY_EVENT:
                        furthest = max(furthest, price_tick - prev_mid)
                        ask_furthest = max(ask_furthest, price_tick - prev_mid)
                    else:
                        furthest = max(furthest, prev_mid - price_tick)
                        bid_furthest = max(bid_furthest, prev_mid - price_tick)
            engine.clear_last_trades(0)
            arrivals[slot] = furthest
            bid_arrivals[slot] = bid_furthest
            ask_arrivals[slot] = ask_furthest
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
                if fit_mode == "legacy_half_tick":
                    valid, new_a, new_k, new_sigma, fit_levels, crossed, extreme = (
                        fit_recent_arrivals(arrivals, changes))
                else:
                    valid, new_a, new_k, new_sigma, fit_levels, crossed, extreme = (
                        fit_physical_1tick_poisson(bid_arrivals, ask_arrivals, changes))
                if valid:
                    new_half, new_skew = glft_coefficients(new_a, new_k, new_sigma, gamma_code, order_qty)
                    if np.isfinite(new_half) and np.isfinite(new_skew) and new_half > 0 and new_skew > 0:
                        a, k, sigma, half, skew = (
                            new_a, new_k, new_sigma, new_half, new_skew)
                        successes += 1
                        if sample_count == TRAIN or fit_mode == "physical_1tick_poisson":
                            initial_ok = 1
                        if first_fit_ns < 0:
                            first_fit_ns = now
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
                probe_rows[probe_idx, 7] = last_shadow_effective
                probe_rows[probe_idx, 8] = last_shadow_gap
                probe_rows[probe_idx, 9] = last_shadow_sigma
                probe_rows[probe_idx, 10] = 1.0 if active_origin else 0.0
                probe_rows[probe_idx, 11] = float(last_shadow_decision)
                probe_rows[probe_idx, 12] = float(last_shadow_activation)
                probe_rows[probe_idx, 13] = float(latest_binance_ns)
                probe_rows[probe_idx, 14] = base_b
                probe_rows[probe_idx, 15] = base_m
                probe_rows[probe_idx, 16] = last_shadow_bid_target
                probe_rows[probe_idx, 17] = last_shadow_ask_target
                probe_rows[probe_idx, 18] = float(bid_action)
                probe_rows[probe_idx, 19] = float(ask_action)
                probe_rows[probe_idx, 20] = float(engine.depth(0).best_bid_tick)
                probe_rows[probe_idx, 21] = float(engine.depth(0).best_ask_tick)
                probe_rows[probe_idx, 22] = float(last_shadow_above)
                probe_rows[probe_idx, 23] = float(last_shadow_dominant)
                probe_rows[probe_idx, 24] = float(last_shadow_gate_mid)
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
            if wait_book_after_fill and feed_status == 3:
                latest_position = engine.position(0)
                if latest_position != last_observed_position:
                    awaiting_market_sync = True
                    last_fill_observed_ns = now
                    if n_fills == 0:
                        raise ValueError("Private fill changed position without HBT FILLED trace")
                    latest_exchange = -1
                    for kk in range(n_fills-1,-1,-1):
                        if fill_observed_ns[kk] != now:
                            break
                        latest_exchange=max(latest_exchange,fill_ts_ns[kk])
                    if latest_exchange <= 0:
                        raise ValueError("Missing local-reply official fill exchange timestamp")
                    last_fill_exchange_ns = latest_exchange
                last_observed_position = latest_position
            engine.clear_inactive_orders(0)
            if now >= t_stop or not np.isfinite(half):
                continue
            if forced_start_ns > 0 and now < forced_start_ns:
                continue
            if (forced_start_ns > 0 and now >= forced_start_ns and
                    not np.isfinite(half)):
                raise ValueError("Forced shared start has no valid GLFT calibration")

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
            binance_is_fresh = (fit_mode == "legacy_half_tick" or
                                (latest_binance_ns >= 0 and
                                 0 <= now-latest_binance_ns <= 250_000_000))
            if not binance_is_fresh:
                active_origin = False
            if (beta > 0.0 or shadow_gate) and np.isfinite(latest_binance) and binance_is_fresh:
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
                    last_shadow_gap = observed_gap
                    last_shadow_sigma = historical_sigma
                    last_shadow_decision = now
                    last_shadow_effective = effective_gap
                    last_shadow_gate_mid = mid * tick_size
                    last_shadow_above = one_above
                    last_shadow_dominant = one_dominant
                    if one_activated:
                        last_shadow_activation = now
                    if effective_gap != 0.0:
                        shift_ticks = effective_gap / tick_size
                        active_decisions += 1
                else:
                    active_origin = False
                    last_shadow_effective = 0.0
                    last_shadow_above = 0
                    last_shadow_dominant = 0
            widened_half = half + extra_half_ticks if control_kind == 1 else half
            base_bid, base_ask, want_bid, want_ask = quoted_ticks(
                mid, pos, skew, widened_half, bid, ask, beta, shift_ticks,
                quote_mode, post_only_clamp_mode)
            vol_pause = control_kind == 2 and sigma > local_sigma_ceiling
            # Read-only counterfactual quote for the actual baseline inventory.
            # Its value is never used in submit or cancel below.
            last_shadow_bid_target = base_bid
            last_shadow_ask_target = base_ask
            if shadow_gate and last_shadow_effective != 0.0:
                so = last_shadow_effective / tick_size
                last_shadow_bid_target = min(round(base_bid + so), ask - 1)
                last_shadow_ask_target = max(round(base_ask + so), bid + 1)
            pause_buy = quote_mode == 3 and shift_ticks < 0.0
            pause_sell = quote_mode == 3 and shift_ticks > 0.0
            pause_buy = pause_buy or vol_pause
            pause_sell = pause_sell or vol_pause
            if cap_depth_at_visible_levels:
                edge_bid = last_visible_bid_tick(
                    book.bid_depth,book.roi_lb_tick,bid,20)
                edge_ask = last_visible_ask_tick(
                    book.ask_depth,book.roi_lb_tick,book.roi_ub_tick,ask,20)
                want_bid,want_ask = cap_quotes_to_visible_depth(
                    want_bid,want_ask,edge_bid,edge_ask,bid,ask)
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
                    if (order.cancellable and (pause_buy or order.price_tick != want_bid) and
                            now - bid_action >= min_requote_ns):
                        engine.cancel(0, order.order_id, False)
                        if diagnostics is not None:
                            op_head,op_tail,max_ops = note_order_operation(
                                op_times,op_head,op_tail,now,max_ops)
                        bid_action = now
                        cancels += 1
                elif order.side == SELL:
                    has_ask = True
                    if (order.cancellable and (pause_sell or order.price_tick != want_ask) and
                            now - ask_action >= min_requote_ns):
                        engine.cancel(0, order.order_id, False)
                        if diagnostics is not None:
                            op_head,op_tail,max_ops = note_order_operation(
                                op_times,op_head,op_tail,now,max_ops)
                        ask_action = now
                        cancels += 1
            if (not pause_buy and not awaiting_market_sync and not has_bid and
                    pos + order_qty <= position_cap + 1e-9 and
                    now - bid_action >= min_requote_ns):
                oid = int(want_bid) * 2
                engine.submit_buy_order(0, oid, want_bid*tick_size, order_qty, GTX, LIMIT, False)
                if diagnostics is not None:
                    op_head,op_tail,max_ops = note_order_operation(
                        op_times,op_head,op_tail,now,max_ops)
                bid_action = now
                submits += 1
            if (not pause_sell and not awaiting_market_sync and not has_ask and
                    pos - order_qty >= -position_cap - 1e-9 and
                    now - ask_action >= min_requote_ns):
                oid = int(want_ask) * 2 + 1
                engine.submit_sell_order(0, oid, want_ask*tick_size, order_qty, GTX, LIMIT, False)
                if diagnostics is not None:
                    op_head,op_tail,max_ops = note_order_operation(
                        op_times,op_head,op_tail,now,max_ops)
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
    if diagnostics is not None:
        # Denominator is the observed post-warmup trading session, not pretrade.
        begin = t0+TRAIN*STEP_NS
        if forced_start_ns > 0:
            begin = max(begin,forced_start_ns)
        duration=max(1,now-begin)
        diagnostics[0] = max_ops
        diagnostics[1] = resting_bid_ns/duration
        diagnostics[2] = resting_ask_ns/duration
        diagnostics[3] = first_fit_ns
        diagnostics[4] = max(0,first_fit_ns-(t0+TRAIN*STEP_NS))
        diagnostics[5] = 1 if first_fit_ns >= 0 else 0
        diagnostics[6] = 1 if awaiting_market_sync else 0
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




def legacy_public_result(raw):
    """Keep the historical 7-column empty-probe ABI for Stage1/4/5A runners.

    Stage3 deliberately retains its 25-column counterfactual probe array.
    The underlying event loop is unchanged by this result shape adapter.
    """
    state = raw[-2]
    if state[4].shape[1] == 7:
        return raw
    return raw[:-2] + ((state[0],state[1],state[2],state[3],
                        state[4][:,:7]),raw[-1])
