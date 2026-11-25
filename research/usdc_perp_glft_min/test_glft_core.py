"""Unit tests for glft_core (plain runner: numpy + numba only)."""
import math

import numpy as np

from glft_core import (fit_intensity_from_counts, glft_coefficients, glft_quotes,
                       push_change, push_depth)


def test_coefficients_match_closed_form():
    a, k, sigma, gamma = 2.0, 0.5, 3.0, 0.05
    half, skew = glft_coefficients(a, k, sigma, gamma, 1.0)
    c1 = math.log(1 + gamma / k) / gamma
    c2 = math.sqrt(gamma / (2 * a * k) * (1 + gamma / k) ** (k / gamma + 1))
    assert abs(half - (c1 + 0.5 * sigma * c2)) < 1e-12
    assert abs(skew - sigma * c2) < 1e-12


def test_fit_recovers_exponential():
    a, k, window = 5.0, 0.3, 1000.0
    counts = np.zeros(41, np.int64)
    for j in range(1, 41):
        counts[j] = int(round(a * math.exp(-k * j) * window))
    a_hat, k_hat, ok = fit_intensity_from_counts(counts, window, 5)
    assert ok
    assert abs(k_hat - k) < 0.02, k_hat
    assert abs(a_hat / a - 1) < 0.1, a_hat


def test_fit_rejects_too_few_levels():
    counts = np.zeros(10, np.int64)
    counts[1] = 100
    counts[2] = 3
    assert not fit_intensity_from_counts(counts, 10.0, 5)[2]


def test_incremental_depth_counts():
    rng = np.random.default_rng(0)
    width, grid = 50, 10
    ring = np.zeros(width, np.int64)
    counts = np.zeros(grid + 1, np.int64)
    idx, filled, hist = 0, 0, []
    for _ in range(500):
        depth = int(rng.integers(0, grid + 1))
        hist.append(depth)
        idx, filled = push_depth(ring, idx, filled, counts, depth)
        window = hist[-width:]
        for j in range(1, grid + 1):
            assert counts[j] == sum(1 for x in window if x >= j)


def test_incremental_variance():
    rng = np.random.default_rng(1)
    width = 64
    ring = np.zeros(width)
    idx, filled, s1, s2, hist = 0, 0, 0.0, 0.0, []
    for _ in range(1000):
        x = float(rng.normal(0, 3))
        hist.append(x)
        idx, filled, s1, s2 = push_change(ring, idx, filled, s1, s2, x)
    window = np.array(hist[-width:])
    assert abs(s2 / filled - (s1 / filled) ** 2 - window.var()) < 1e-9


def test_quotes_skew_and_touch_clamp():
    assert glft_quotes(100.0, 0.0, 2.0, 0.5, 99, 101) == (98, 102)
    assert glft_quotes(100.0, 2.0, 2.0, 0.5, 99, 101) == (97, 101)
    assert glft_quotes(110.0, 0.0, 2.0, 0.5, 99, 101) == (99, 112)


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print("PASS", name)
    print("ALL_PASS")
