"""600s time-weighted Binance-target spread basis and locked-origin signal gate.

No freshness expiry. Quotes are held to their next native BBO update.
Timestamps are collector-visible times, not exchange-time order guarantees.
"""
import numpy as np

NS = 1_000_000_000
WINDOW_NS = 600 * NS
REFIT_NS = 5 * NS
ENTER_SIGMA = 1.0
TRIGGER_SIGMA = 1.96


def collapse_same_timestamp(times, prices):
    times = np.asarray(times, dtype=np.int64)
    prices = np.asarray(prices, dtype=np.float64)
    if not len(times) or len(times) != len(prices) or np.any(np.diff(times) < 0):
        raise ValueError("Malformed or unordered native BBO tape")
    if np.any(~np.isfinite(prices)) or np.any(prices <= 0):
        raise ValueError("Invalid native BBO mid")
    # For equal collector microseconds there is no defendable inter-venue
    # sequence; expose the last source state only after t+1ns.
    ends = np.r_[times[1:] != times[:-1], True]
    return times[ends], prices[ends]


def event_tape(bin_visible_ns, bin_mid, target_local_ns, target_mid):
    bt, bm = collapse_same_timestamp(bin_visible_ns, bin_mid)
    ot, om = collapse_same_timestamp(target_local_ns, target_mid)
    # Binance already carries a +1ns tie guard from visible_timestamps().
    # Target native BBO first becomes visible at its collector-time +1ns.
    ot = ot + 1
    events = np.union1d(bt, ot)
    bi = np.searchsorted(bt, events, side="right") - 1
    oi = np.searchsorted(ot, events, side="right") - 1
    valid = (bi >= 0) & (oi >= 0)
    events = events[valid]
    bi, oi = bi[valid], oi[valid]
    b, m = bm[bi], om[oi]
    return events, b, m


def rolling_time_weighted_basis(events, gap, first_trade_ns, last_trade_ns):
    """Return 5s full-history integral mean and time-weighted population sigma.

    A quote gap is a step function. Integrate D and D**2 exactly (nanosecond
    intervals); denominator is observed window duration if the UTC-midnight
    initial BBO state arrives slightly after the window begins.
    """
    events = np.asarray(events, np.int64)
    gap = np.asarray(gap, np.float64)
    if len(events) < 2 or len(events) != len(gap) or np.any(np.diff(events) <= 0):
        raise ValueError("Malformed event tape for basis")
    if np.any(~np.isfinite(gap)):
        raise ValueError("Nonfinite historical spread")
    grid = np.arange(first_trade_ns, last_trade_ns, REFIT_NS, dtype=np.int64)
    dt = np.diff(events).astype(np.float64)
    ints = np.r_[0., np.cumsum(gap[:-1] * dt)]
    sqs = np.r_[0., np.cumsum((gap[:-1]**2) * dt)]

    def integral(x, cumulative, state):
        x = np.maximum(x, events[0])
        i = np.searchsorted(events, x, side="left") - 1
        i = np.maximum(i, 0)
        return cumulative[i] + (x - events[i]).astype(np.float64) * state[i]

    lo = grid - WINDOW_NS
    clipped_lo = np.maximum(lo, events[0])
    duration = (grid - clipped_lo).astype(np.float64)
    if np.any(duration < WINDOW_NS * .99):
        raise ValueError("Insufficient time coverage in initial 600s window")
    a1 = integral(grid, ints, gap) - integral(clipped_lo, ints, gap)
    a2 = integral(grid, sqs, gap * gap) - integral(clipped_lo, sqs, gap * gap)
    mean = a1 / duration
    var = np.maximum(a2 / duration - mean * mean, 0.)
    sigma = np.sqrt(var)
    if np.any(~np.isfinite(mean)) or np.any(~np.isfinite(sigma)):
        raise ValueError("Nonfinite rolling historical spread mean/std")
    return grid, mean, sigma, duration / WINDOW_NS


def candidate_gate(gap_abnormal, sigma, delta_b, delta_m):
    """1.96-sigma excursion and same-sign Binance-dominant absolute move."""
    if not np.isfinite(gap_abnormal) or not np.isfinite(sigma) or sigma <= 0:
        return False
    if not np.isfinite(delta_b) or not np.isfinite(delta_m):
        return False
    return (abs(gap_abnormal) > TRIGGER_SIGMA * sigma and
            gap_abnormal * delta_b > 0 and abs(delta_b) > abs(delta_m))


def locked_origin_event_series(events, b, m, grid, mean, sigma, first_trade_ns):
    """Causal source origin snapshot for every native BBO event after warmup.

    Inside 1 sigma: refresh both anchor prices. Outside: freeze existing
    anchors until reentry. If first observed event is already outside 1 sigma
    without an anchor, fail closed until a normal-band observation arrives.
    """
    if not (len(events) == len(b) == len(m)) or len(grid) != len(mean) or len(grid) != len(sigma):
        raise ValueError("Shape mismatch")
    start_idx = int(np.searchsorted(events, first_trade_ns, side="left"))
    ts = events[start_idx:].copy()
    prices_b = b[start_idx:]
    prices_m = m[start_idx:]
    gi = np.searchsorted(grid, ts, side="right") - 1
    anchors_b = np.full(len(ts), np.nan, dtype=np.float64)
    anchors_m = np.full(len(ts), np.nan, dtype=np.float64)
    active = np.zeros(len(ts), dtype=np.bool_)
    passed = np.zeros(len(ts), dtype=np.bool_)
    over_196 = np.zeros(len(ts), dtype=np.bool_)
    had_anchor = False
    base_b = base_m = np.nan
    for i in range(len(ts)):
        j = int(gi[i])
        if j < 0:
            continue
        g = prices_b[i] - prices_m[i] - mean[j]
        s = sigma[j]
        if not np.isfinite(s) or s <= 0:
            continue
        if abs(g) <= ENTER_SIGMA*s:
            base_b = prices_b[i]
            base_m = prices_m[i]
            had_anchor = True
        if had_anchor:
            anchors_b[i] = base_b
            anchors_m[i] = base_m
            active[i] = True
            over_196[i] = abs(g) > TRIGGER_SIGMA*s
            passed[i] = candidate_gate(
                g, s, prices_b[i]-base_b, prices_m[i]-base_m)
    return ts, anchors_b, anchors_m, active, passed, over_196


def prepare_signal(bin_visible_ns, bin_mid, target_local_ns, target_mid,
                   first_trade_ns, last_trade_ns):
    events, b, m = event_tape(bin_visible_ns,bin_mid,target_local_ns,target_mid)
    grid, means, sigmas, coverage = rolling_time_weighted_basis(
        events,b-m,first_trade_ns,last_trade_ns)
    ts, anchors_b, anchors_m, active, passed, over = locked_origin_event_series(
        events,b,m,grid,means,sigmas,first_trade_ns)
    return {
        "basis_ns":grid, "basis_mean":means, "basis_sigma":sigmas,
        "basis_time_coverage":coverage,
        "source_event_ns":ts, "base_b":anchors_b,"base_m":anchors_m,
        "anchor_ready":active, "source_pass":passed,
        "source_over_196":over,
    }
