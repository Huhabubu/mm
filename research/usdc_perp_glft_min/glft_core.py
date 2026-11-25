"""Minimal GLFT helpers (numba only, no hftbacktest import) for USDC-M perp quoting.

Units: prices in ticks, time in seconds, inventory in order-size units (delta = 1).
"""
import math

from numba import njit


@njit(cache=False)
def glft_coefficients(a, k, sigma, gamma, delta=1.0):
    """Gueant (2017) closed-form approximation, same form as the hftbacktest GLFT tutorial.

    a: arrivals per second at depth 0; k: decay per tick; sigma: ticks per sqrt(second).
    Returns (half_spread_ticks, skew_ticks_per_inventory_unit).
    """
    xi = gamma
    c1 = math.log(1.0 + xi * delta / k) / (xi * delta)
    c2 = math.sqrt(gamma / (2.0 * a * delta * k) * (1.0 + xi * delta / k) ** (k / (xi * delta) + 1.0))
    return c1 + 0.5 * delta * sigma * c2, sigma * c2


@njit(cache=False)
def fit_intensity_from_counts(counts, window_seconds, min_count):
    """counts[j]: steps in the window whose max trade penetration reached >= j grid units.

    Least-squares fit of log(counts[j] / T) = log(a) - k * j for j = 1.. while counts >= min_count.
    Returns (a_per_second, k_per_grid_unit, ok).
    """
    sx = 0.0
    sy = 0.0
    sxx = 0.0
    sxy = 0.0
    m = 0
    for j in range(1, counts.shape[0]):
        c = counts[j]
        if c < min_count:
            break
        x = float(j)
        y = math.log(c / window_seconds)
        sx += x
        sy += y
        sxx += x * x
        sxy += x * y
        m += 1
    if m < 3:
        return 0.0, 0.0, False
    den = m * sxx - sx * sx
    if den <= 0.0:
        return 0.0, 0.0, False
    slope = (m * sxy - sx * sy) / den
    k = -slope
    if not k > 0.0:
        return 0.0, 0.0, False
    return math.exp((sy - slope * sx) / m), k, True


@njit(cache=False)
def push_depth(ring, idx, filled, counts, depth):
    """Ring buffer of per-step depths with incremental cumulative counts (counts[j] = #depth >= j)."""
    n = ring.shape[0]
    if filled == n:
        old = ring[idx]
        for j in range(1, old + 1):
            counts[j] -= 1
    else:
        filled += 1
    ring[idx] = depth
    for j in range(1, depth + 1):
        counts[j] += 1
    return (idx + 1) % n, filled


@njit(cache=False)
def push_change(ring, idx, filled, s1, s2, x):
    """Ring buffer of per-step mid changes with running sum and sum of squares."""
    n = ring.shape[0]
    if filled == n:
        old = ring[idx]
        s1 -= old
        s2 -= old * old
    else:
        filled += 1
    ring[idx] = x
    s1 += x
    s2 += x * x
    return (idx + 1) % n, filled, s1, s2


@njit(cache=False)
def glft_quotes(fair, q, half_spread, skew, best_bid_tick, best_ask_tick):
    """Post-only GLFT quotes in ticks, kept on or behind the touch."""
    bid = math.floor(fair - half_spread - skew * q)
    ask = math.ceil(fair + half_spread - skew * q)
    if bid > best_bid_tick:
        bid = best_bid_tick
    if ask < best_ask_tick:
        ask = best_ask_tick
    return int(bid), int(ask)
