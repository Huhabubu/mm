"""Small Numba-compatible Gate GLFT execution rules, fed by one HBT engine."""
import numpy as np
from numba import njit


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
    else:
        raise ValueError("post_only_clamp_mode must be legacy or unified")
    if want_bid >= want_ask:
        raise ValueError("Proposed self-crossed quote prices")
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
