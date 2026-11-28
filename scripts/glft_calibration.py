"""Shared GLFT estimation and pricing. Unified caller supplies actual contract sizing."""
import math
import numpy as np
from numba import njit

TICK = 0.1               # USDT per BTC
CTVAL = 0.01             # BTC per contract; additional contract multiplier 1
ORDER_QTY = 0.1          # contracts, 0.001 BTC
POSITION_CAP = 2.0       # contracts, 0.02 BTC
GAMMA = 0.05             # scenario risk parameter
STEP_NS = 100_000_000    # 100ms
TRAIN = 6000             # 10 minutes
UPDATE_EVERY_STEPS = 50                 # 5sec / 100ms
FIT_BINS = 70                           # 0.5..35 actual price ticks
FIT_STEP_TICKS = 0.5
MIN_CROSSING_INTERVALS = 10               # minimum distinct 100ms trade-through events
WINDOW_SECONDS = TRAIN * STEP_NS / 1e9

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
    if n < 5 or qualifying_intervals < MIN_CROSSING_INTERVALS or var_n < TRAIN // 2:
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
            a > 0 and k > 1e-8 and sigma > 0):
        return 0, a, k, sigma, n, qualifying_intervals, extreme_intervals
    return 1, a, k, sigma, n, qualifying_intervals, extreme_intervals


PHYSICAL_FIT_BINS = 35             # 0.5, 1.5, ... 34.5 actual one-tick grid
PHYSICAL_MIN_FIT_BINS = 3
POISSON_MAX_HIT_PROB = 0.999


@njit
def physical_poisson_rates(bid_arrivals, ask_arrivals):
    """Pool two independently observed 100ms side exposures into symmetric rates.

    bid_arrivals: max active sell trade depth (against our potential bid).
    ask_arrivals: max active buy trade depth (against our potential ask).
    A side-specific hit probability is total hits / 2*TRAIN, not / TRAIN.
    """
    if len(bid_arrivals) != TRAIN or len(ask_arrivals) != TRAIN:
        raise ValueError("Both physical-side arrival rings require TRAIN samples")
    counts = np.zeros(PHYSICAL_FIT_BINS, np.float64)
    crossed = 0
    extreme = 0
    for side in (bid_arrivals, ask_arrivals):
        for depth in side:
            if not np.isfinite(depth):
                continue
            if depth >= PHYSICAL_FIT_BINS - .5:
                extreme += 1
            if depth < .5 - 1e-7:
                continue
            crossed += 1
            for i in range(PHYSICAL_FIT_BINS):
                distance = i + .5
                if depth >= distance - 1e-7:
                    counts[i] += 1
                else:
                    break
    rates = np.zeros(PHYSICAL_FIT_BINS, np.float64)
    for i in range(PHYSICAL_FIT_BINS):
        p = min(counts[i] / (2 * TRAIN), POISSON_MAX_HIT_PROB)
        if p > 0:
            rates[i] = -math.log1p(-p) / (STEP_NS / 1e9)
    return rates, crossed, extreme


@njit
def fit_physical_1tick_poisson(bid_arrivals, ask_arrivals, changes):
    """Fit one symmetric (A,k) with independent side rings and Poisson correction.

    sigma is unchanged GLFT 100ms realized mid variation in ticks/sqrt(second).
    This is an explicitly different estimator, never used by legacy mode.
    """
    rates, crossed, extreme = physical_poisson_rates(bid_arrivals, ask_arrivals)
    n = 0
    sx = sy = sxx = sxy = 0.0
    for i in range(PHYSICAL_FIT_BINS):
        if rates[i] <= 0:
            continue
        x = i + .5
        y = math.log(rates[i])
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
    if (n < PHYSICAL_MIN_FIT_BINS or crossed < MIN_CROSSING_INTERVALS
            or var_n < TRAIN // 2):
        return 0, np.nan, np.nan, np.nan, n, crossed, extreme
    denom = n*sxx - sx*sx
    if denom <= 0:
        return 0, np.nan, np.nan, np.nan, n, crossed, extreme
    slope = (n*sxy-sx*sy)/denom
    intercept = (sy-slope*sx)/n
    a = math.exp(intercept)
    k = -slope
    var = max(0.0, squared/var_n - (total/var_n)**2)
    sigma = math.sqrt(var/(STEP_NS/1e9))
    if not (np.isfinite(a) and np.isfinite(k) and np.isfinite(sigma)
            and a > 0 and k > 1e-8 and sigma > 0):
        return 0, a, k, sigma, n, crossed, extreme
    return 1, a, k, sigma, n, crossed, extreme


@njit
def glft_coefficients(a, k, sigma, gamma_code=GAMMA, order_qty=ORDER_QTY):
    """Risk aversion gamma_code is in inverse (contract * price tick) units."""
    u = gamma_code * order_qty / k
    c1 = math.log1p(u) / (gamma_code * order_qty)
    c2 = math.sqrt(gamma_code / (2 * a * order_qty * k)
                   * math.exp((k/(gamma_code * order_qty) + 1) * math.log1p(u)))
    half = c1 + order_qty/2 * c2 * sigma
    skew = c2 * sigma
    return half, skew
