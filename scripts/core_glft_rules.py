"""Small Numba-compatible Gate GLFT execution rules, fed by one HBT engine."""
import math
import numpy as np
from numba import njit


@njit
def same_microsecond_target_feed(target_feed_ns, external_visible_ns):
    """Microsecond collision while retaining the external +1ns tie ordering."""
    return (target_feed_ns >= 0 and external_visible_ns >= 0
            and target_feed_ns // 1000 == external_visible_ns // 1000)


@njit
def inventory_sample_is_live(sample_count, warmup_steps, now, forced_start_ns):
    """Exclude pre-entry zero inventory samples in the new shared-entry mode."""
    return (sample_count > warmup_steps
            and (forced_start_ns == 0 or now >= forced_start_ns))


@njit
def quoted_ticks(mid, pos, skew, half, bid, ask, beta, shift_ticks,
                 quote_mode, clamp_mode):
    """Return base bid/ask and final quote ticks; legacy matches frozen GLFT."""
    reservation = mid - skew*pos
    base_bid = min(round(reservation-half), bid)
    base_ask = max(round(reservation+half), ask)
    want_bid, want_ask = base_bid,base_ask
    if clamp_mode == "legacy":
        if shift_ticks != 0.0:
            offset = beta*shift_ticks
            if quote_mode == 1:
                want_bid = min(round(base_bid+offset),ask-1)
                want_ask = max(round(base_ask+offset),bid+1)
            elif quote_mode == 2:
                if shift_ticks > 0:
                    want_ask = max(round(base_ask+offset),bid+1)
                else:
                    want_bid = min(round(base_bid+offset),ask-1)
    elif clamp_mode == "unified":
        # Preserve unilateral mode semantics; only shift the threatened side.
        bid_shift = ask_shift = 0.0
        if quote_mode == 1:
            bid_shift = ask_shift = beta*shift_ticks
        elif quote_mode == 2:
            if shift_ticks > 0:
                ask_shift = beta*shift_ticks
            elif shift_ticks < 0:
                bid_shift = beta*shift_ticks
        raw_bid = round(reservation-half+bid_shift)
        raw_ask = round(reservation+half+ask_shift)
        want_bid = min(raw_bid,ask-1)
        want_ask = max(raw_ask,bid+1)
        # Protect the inventory-reducing side when coarse ticks collide.
        # With no inventory, bracket the unrounded theoretical quote center.
        if want_bid >= want_ask:
            if pos > 0:
                want_bid = want_ask - 1
            elif pos < 0:
                want_ask = want_bid + 1
            else:
                center = reservation + (bid_shift + ask_shift) / 2.0
                if abs(center - round(center)) < 1e-10:
                    want_bid = min(round(center) - 1, ask - 1)
                    want_ask = max(round(center) + 1, bid + 1)
                else:
                    lo = math.floor(center)
                    want_bid = min(lo, ask - 1)
                    want_ask = max(lo + 1, bid + 1)
                if want_bid >= want_ask:
                    want_bid = want_ask - 1
    else:
        raise ValueError("post_only_clamp_mode must be legacy or unified")
    if want_bid >= want_ask:
        raise ValueError("Proposed self-crossed quote prices")
    return base_bid,base_ask,want_bid,want_ask



@njit
def dual_grid_quoted_ticks(mid_quote_ticks, pos, skew_quote_ticks, half_quote_ticks,
                           book_bid_raw_tick, book_ask_raw_tick, beta,
                           shift_quote_ticks, quote_mode, quote_tick_mult):
    """One economic quoting tick spans N *lossless* HBT/orderbook ticks.

    Mid, GLFT sigma, fitted arrival distances and origin shift are already
    expressed in *legal quote ticks*, while HBT book/price/order IDs remain
    in raw ticks. Never coalesce raw orderbook levels. Only final order
    targets are multiples of quote_tick_mult. Opposing best prices may
    themselves be old 6-decimal legacy price levels.
    """
    if quote_tick_mult <= 1:
        raise ValueError("Dual-grid helper requires multiplier > 1")
    if quote_mode not in (0, 1, 2, 3):
        raise ValueError("Invalid quote mode")
    reservation = mid_quote_ticks - skew_quote_ticks * pos
    # Baseline quote positions retain own-top-of-book restraint.
    own_bid_legal = (book_bid_raw_tick // quote_tick_mult) * quote_tick_mult
    own_ask_legal = ((book_ask_raw_tick + quote_tick_mult - 1)
                     // quote_tick_mult) * quote_tick_mult
    base_bid = min(round(reservation - half_quote_ticks) * quote_tick_mult,
                   own_bid_legal)
    base_ask = max(round(reservation + half_quote_ticks) * quote_tick_mult,
                   own_ask_legal)
    # STRICT GTX post-only: bid must be BELOW raw best ask, which can
    # itself be off the legal quoting grid (and conversely for ask).
    bid_postonly_limit = ((book_ask_raw_tick - 1) // quote_tick_mult) * quote_tick_mult
    ask_postonly_limit = ((book_bid_raw_tick + quote_tick_mult)
                          // quote_tick_mult) * quote_tick_mult
    bid_shift = 0.0
    ask_shift = 0.0
    if quote_mode == 1:
        bid_shift = ask_shift = beta * shift_quote_ticks
    elif quote_mode == 2:
        if shift_quote_ticks > 0:
            ask_shift = beta * shift_quote_ticks
        elif shift_quote_ticks < 0:
            bid_shift = beta * shift_quote_ticks
    want_bid = min(round(reservation-half_quote_ticks+bid_shift)*quote_tick_mult,
                   bid_postonly_limit)
    want_ask = max(round(reservation+half_quote_ticks+ask_shift)*quote_tick_mult,
                   ask_postonly_limit)
    if want_bid >= want_ask:
        if pos > 0:
            want_bid = want_ask - quote_tick_mult
        elif pos < 0:
            want_ask = want_bid + quote_tick_mult
        else:
            center = reservation + (bid_shift + ask_shift) / 2.0
            if abs(center - round(center)) < 1e-10:
                lo = round(center) - 1
                hi = round(center) + 1
            else:
                lo = math.floor(center)
                hi = lo + 1
            want_bid = min(lo * quote_tick_mult, bid_postonly_limit)
            want_ask = max(hi * quote_tick_mult, ask_postonly_limit)
            if want_bid >= want_ask:
                want_bid = want_ask - quote_tick_mult
    if (want_bid <= 0 or want_bid >= book_ask_raw_tick
            or want_ask <= book_bid_raw_tick or want_bid >= want_ask
            or want_bid % quote_tick_mult or want_ask % quote_tick_mult):
        raise ValueError("Dual-grid quote crossed an orderbook or is off-grid")
    return base_bid,base_ask,want_bid,want_ask

@njit
def last_visible_bid_tick(book_bid_depth, roi_lower_tick, best_bid_tick,
                          target_levels=20):
    """The 20th actual nonzero same-side bid level, not 20 arithmetic ticks."""
    found=0
    for px in range(best_bid_tick,roi_lower_tick-1,-1):
        if book_bid_depth[px-roi_lower_tick]>0:
            found+=1
            if found==target_levels:
                return px
    return -1


@njit
def last_visible_ask_tick(book_ask_depth, roi_lower_tick, roi_upper_tick,
                          best_ask_tick, target_levels=20):
    found=0
    for px in range(best_ask_tick,roi_upper_tick+1):
        if book_ask_depth[px-roi_lower_tick]>0:
            found+=1
            if found==target_levels:
                return px
    return -1


@njit
def cap_quotes_to_visible_depth(want_bid, want_ask, bid_edge, ask_edge,
                                best_bid_tick, best_ask_tick):
    if bid_edge < 0 or ask_edge < 0:
        raise ValueError("Missing 20 actual visible levels on one or both sides")
    want_bid=max(want_bid,bid_edge)
    want_ask=min(want_ask,ask_edge)
    want_bid=min(want_bid,best_ask_tick-1)
    want_ask=max(want_ask,best_bid_tick+1)
    if want_bid>=want_ask:
        raise ValueError("Depth cap/post-only quote crosses opposite side")
    return want_bid,want_ask


@njit
def book_update_unlocks(fill_exch_ns, fill_observed_local_ns,
                        book_exch_ns, book_local_ns):
    """Exchange-timestamp newer AND public update received after fill reply."""
    return (fill_exch_ns>0 and book_exch_ns>fill_exch_ns
            and book_local_ns>fill_observed_local_ns)


@njit
def note_order_operation(op_times, head, tail, now, maximum):
    """Exact trailing 1s sliding count; monotonic local nanosecond requests."""
    cap=len(op_times)
    if cap<1:
        raise ValueError("Order-operation ring cannot be empty")
    while head<tail and now-op_times[head%cap]>=1_000_000_000:
        head+=1
    if tail-head>=cap:
        raise ValueError("Order-operation ring overflow (API rate exceeding bound)")
    op_times[tail%cap]=now
    tail+=1
    return head,tail,max(maximum,tail-head)
