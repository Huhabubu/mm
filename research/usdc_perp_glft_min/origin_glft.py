"""Event-driven GLFT on Binance USDC-M ETHUSDC with the Gate-study Binance-origin spread filter.

gate_origin_filter below is copied verbatim from Huhabubu/mm@6a6345b4acf72ae61b815c457703c7d9283e1977 (branch exp/tokyo-l2-glft),
research/gate_btc_binance_signal_20261115/scripts/gate_origin_glft_strategy.py
(normal band 1 sigma, trigger 1.645 sigma, dominance, Binance-price-event activation, tie exclusion).
The event loop follows run_gate_origin_glft (wait_next_feed on target feed / order reply,
signal rows and a 100ms sample clock) with these requested differences:
  * no MIN_REQUOTE_NS throttle on either side: re-quote whenever the target price tick changes;
  * signal visible at its Tardis local_timestamp (the caller adds +1ns for same-timestamp ordering only);
  * GLFT fit and coefficients are this directory's glft_core (unchanged from the previous run).
Quote modes: 0 = baseline GLFT, 1 = both sides shifted by beta*G_eff, 2 = only the threatened side moved away.
"""
import math

import numpy as np
from numba import njit
from hftbacktest import BUY, BUY_EVENT, FILLED, GTX, LIMIT, SELL

from glft_core import fit_intensity_from_counts, glft_coefficients, glft_quotes, push_change, push_depth

GATE_SOURCE_SHA = '6a6345b4acf72ae61b815c457703c7d9283e1977'
TICK = 0.01
LOT = 0.001
ORDER_QTY = 0.03
MAX_UNITS = 5.0
STEP_NS = 100_000_000
STEP_S = 0.1
WARMUP = 6000
WINDOW = 6000
MIN_FIT_STEPS = 600
REFIT_EVERY = 50
GRID = 10
MAX_BINS = 1000
MIN_COUNT = 10
DIAG_NAMES = ('signal_price_updates', 'trigger_checks', 'above_1645_sigma', 'dominant',
              'activations', 'active_decisions', 'tie_excluded', 'decisions')

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
def run_origin_glft(hbt, max_steps, sig_ts, sig_mid, grid_ns, basis, bsigma, beta, quote_mode, gamma,
                    out_mid, out_eq, out_pos, fills, fits, diag):
    if not math.isfinite(beta) or beta < 0.0 or beta > 1.0:
        raise ValueError('invalid beta')
    if quote_mode < 0 or quote_mode > 2:
        raise ValueError('0=baseline, 1=both, 2=defensive')
    if grid_ns.shape[0] != basis.shape[0] or basis.shape[0] != bsigma.shape[0]:
        raise ValueError('basis grid length mismatch')
    if hbt.elapse(0) != 0:
        raise ValueError('cannot initialise replay')
    t0 = hbt.current_timestamp
    t_stop = t0 + max_steps * STEP_NS
    next_sample = t0 + STEP_NS
    n_sample = 0
    n_sig = sig_ts.shape[0]
    sp = 0
    while sp < n_sig and sig_ts[sp] < t0:
        sp += 1
    latest = np.nan
    depth_ring = np.zeros(WINDOW, np.int64)
    counts = np.zeros(MAX_BINS + 1, np.int64)
    chg_ring = np.zeros(WINDOW, np.float64)
    d_idx = 0
    d_fill = 0
    c_idx = 0
    c_fill = 0
    s1 = 0.0
    s2 = 0.0
    prev_mid = np.nan
    a_hat = 0.0
    k_hat = 0.0
    sigma = 0.0
    half = 0.0
    skew = 0.0
    fitted = False
    n_fit = 0
    base_b = np.nan
    base_m = np.nan
    has_base = False
    active = False
    oid = 1
    n_fill = 0
    submits = 0
    cancels = 0
    max_pos = 0.0
    n_sig_events = 0
    n_feed = 0
    n_resp = 0
    now = t0
    while now < t_stop:
        due = next_sample
        if sp < n_sig and sig_ts[sp] < due:
            due = sig_ts[sp]
        if t_stop < due:
            due = t_stop
        status = 0
        if due > now:
            status = hbt.wait_next_feed(True, due - now)
            if status == 1:
                break
            now = hbt.current_timestamp
            if now > due:
                raise ValueError('HBT overshot event deadline')
            if status == 2:
                n_feed += 1
            elif status == 3:
                n_resp += 1
            elif status != 0:
                raise ValueError('unexpected HBT wait status')

        prior = latest
        sig_event = False
        while sp < n_sig and sig_ts[sp] <= now:
            latest = sig_mid[sp]
            sp += 1
            n_sig_events += 1
            sig_event = True
        price_change = sig_event and math.isfinite(prior) and latest != prior
        if price_change:
            diag[0] += 1

        sample_event = False
        if now >= next_sample:
            if now != next_sample:
                raise ValueError('sample clock skipped a 100ms boundary')
            book = hbt.depth(0)
            bb = book.best_bid_tick
            ba = book.best_ask_tick
            valid = bb > 0 and ba > bb and ba < 1_000_000_000
            mid = prev_mid
            if valid:
                mid = (bb + ba) / 2.0
            if valid and not math.isnan(prev_mid):
                furthest = 0.0
                for tr in hbt.last_trades(0):
                    p = tr.px / TICK
                    if (tr.ev & BUY_EVENT) == BUY_EVENT:
                        d = p - prev_mid
                    else:
                        d = prev_mid - p
                    if d > furthest:
                        furthest = d
                dj = int(furthest * GRID)
                if dj > MAX_BINS:
                    dj = MAX_BINS
                d_idx, d_fill = push_depth(depth_ring, d_idx, d_fill, counts, dj)
                c_idx, c_fill, s1, s2 = push_change(chg_ring, c_idx, c_fill, s1, s2, mid - prev_mid)
            hbt.clear_last_trades(0)
            prev_mid = mid
            if n_sample % REFIT_EVERY == 0 and d_fill >= MIN_FIT_STEPS:
                a, k, ok = fit_intensity_from_counts(counts, d_fill * STEP_S, MIN_COUNT)
                mean = s1 / c_fill
                var = s2 / c_fill - mean * mean
                sigma = math.sqrt(var / STEP_S) if var > 0.0 else 0.0
                if ok:
                    a_hat = a
                    k_hat = k * GRID
                    fitted = True
                if fitted and sigma > 0.0:
                    half, skew = glft_coefficients(a_hat, k_hat, sigma, gamma, 1.0)
                if fitted and n_fit < fits.shape[0]:
                    fits[n_fit, 0] = a_hat
                    fits[n_fit, 1] = k_hat
                    fits[n_fit, 2] = sigma
                    fits[n_fit, 3] = half
                    fits[n_fit, 4] = skew
                    n_fit += 1
            st = hbt.state_values(0)
            out_mid[n_sample] = mid * TICK
            out_eq[n_sample] = st.balance + st.position * mid * TICK - st.fee
            out_pos[n_sample] = st.position
            n_sample += 1
            next_sample += STEP_NS
            sample_event = True

        if not (status == 2 or status == 3 or sig_event or sample_event):
            continue
        pos = hbt.position(0)
        if abs(pos) > max_pos:
            max_pos = abs(pos)
        values = hbt.orders(0).values()
        while values.has_next():
            o = values.get()
            if o.status == FILLED:
                if n_fill >= fills.shape[0]:
                    raise ValueError('fill trace capacity exceeded')
                fills[n_fill, 0] = o.exch_timestamp
                fills[n_fill, 1] = 1.0 if o.side == BUY else -1.0
                fills[n_fill, 2] = o.exec_price
                fills[n_fill, 3] = o.exec_qty
                n_fill += 1
        hbt.clear_inactive_orders(0)
        if now >= t_stop or n_sample <= WARMUP or not fitted or not sigma > 0.0:
            continue
        if not (math.isfinite(half) and math.isfinite(skew) and half < 1e5 and skew < 1e5):
            continue
        book = hbt.depth(0)
        bb = book.best_bid_tick
        ba = book.best_ask_tick
        if bb <= 0 or ba <= bb or ba >= 1_000_000_000:
            continue
        mid = (bb + ba) / 2.0
        q = pos / ORDER_QTY
        base_bid, base_ask = glft_quotes(mid, q, half, skew, bb, ba)
        shift = 0.0
        if beta > 0.0 and math.isfinite(latest):
            gi = np.searchsorted(grid_ns, now, side='left') - 1
            if gi >= 0 and math.isfinite(basis[gi]) and math.isfinite(bsigma[gi]) and bsigma[gi] > 0.0:
                gap = latest - mid * TICK - basis[gi]
                diag[1] += 1
                (eff, base_b, base_m, has_base, active, above, dominant, activated,
                 tie) = gate_origin_filter(gap, bsigma[gi], latest, mid * TICK, base_b, base_m,
                                           has_base, active, price_change, status == 2)
                diag[2] += above
                diag[3] += dominant
                diag[4] += activated
                diag[6] += tie
                if eff != 0.0:
                    shift = eff / TICK
                    diag[5] += 1
            else:
                active = False
        want_bid = base_bid
        want_ask = base_ask
        if shift != 0.0:
            off = beta * shift
            if quote_mode == 1:
                want_bid = min(int(round(base_bid + off)), ba - 1)
                want_ask = max(int(round(base_ask + off)), bb + 1)
            elif quote_mode == 2:
                if shift > 0.0:
                    want_ask = max(int(round(base_ask + off)), bb + 1)
                else:
                    want_bid = min(int(round(base_bid + off)), ba - 1)
        if want_bid >= want_ask:
            raise ValueError('self-crossed quotes')
        allow_bid = q < MAX_UNITS - 1e-9 and want_bid > 0
        allow_ask = q > -MAX_UNITS + 1e-9
        has_bid = False
        has_ask = False
        values = hbt.orders(0).values()
        while values.has_next():
            o = values.get()
            if o.side == BUY:
                has_bid = True
                if o.cancellable and (not allow_bid or o.price_tick != want_bid):
                    hbt.cancel(0, o.order_id, False)
                    cancels += 1
            elif o.side == SELL:
                has_ask = True
                if o.cancellable and (not allow_ask or o.price_tick != want_ask):
                    hbt.cancel(0, o.order_id, False)
                    cancels += 1
        if allow_bid and not has_bid:
            hbt.submit_buy_order(0, oid, want_bid * TICK, ORDER_QTY, GTX, LIMIT, False)
            oid += 1
            submits += 1
        if allow_ask and not has_ask:
            hbt.submit_sell_order(0, oid, want_ask * TICK, ORDER_QTY, GTX, LIMIT, False)
            oid += 1
            submits += 1
        diag[7] += 1

    values = hbt.orders(0).values()
    while values.has_next():
        o = values.get()
        if o.status == FILLED:
            if n_fill >= fills.shape[0]:
                raise ValueError('fill trace capacity exceeded')
            fills[n_fill, 0] = o.exch_timestamp
            fills[n_fill, 1] = 1.0 if o.side == BUY else -1.0
            fills[n_fill, 2] = o.exec_price
            fills[n_fill, 3] = o.exec_qty
            n_fill += 1
    st = hbt.state_values(0)
    return (n_sample, n_fill, n_fit, submits, cancels, max_pos, n_sig_events, n_feed, n_resp,
            st.trading_value, st.fee, st.num_trades, t0)