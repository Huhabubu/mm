"""Unit tests: copied Gate origin filter transitions and causal 600s time-weighted basis moments."""
import numpy as np

from basis_moments import NS, exact_moments_5s
from origin_glft import gate_origin_filter


def filt(gap, sigma=1.0, b=100.0, m=99.2, base_b=99.0, base_m=99.0, has_base=True,
         active=False, changed=True, tie=False):
    return gate_origin_filter(gap, sigma, b, m, base_b, base_m, has_base, active, changed, tie)


def test_normal_band_refreshes_anchor_and_resets():
    r = filt(0.5, b=101.0, m=100.0, base_b=np.nan, base_m=np.nan, has_base=False, active=True)
    assert r[0] == 0.0 and r[1] == 101.0 and r[2] == 100.0 and r[3] and not r[4]


def test_activation_needs_binance_price_event():
    r = filt(2.0)
    assert r[0] == 2.0 and r[4] and r[5] == 1 and r[6] == 1 and r[7] == 1
    r = filt(2.0, changed=False)
    assert r[0] == 0.0 and not r[4] and r[5] == 1 and r[6] == 1 and r[7] == 0


def test_same_time_target_feed_is_excluded():
    r = filt(2.0, tie=True)
    assert r[0] == 0.0 and not r[4] and r[8] == 1


def test_active_signal_persists_without_new_event():
    r = filt(2.0, active=True, changed=False)
    assert r[0] == 2.0 and r[4]


def test_between_bands_keeps_anchor_and_clears_active():
    r = filt(1.5, active=True)
    assert r[0] == 0.0 and r[1] == 99.0 and r[2] == 99.0 and not r[4] and r[5] == 0


def test_dominance_and_direction():
    assert filt(2.0, m=100.5)[0] == 0.0
    assert filt(-2.0)[0] == 0.0
    r = filt(-2.0, b=98.0, m=98.9)
    assert r[0] == -2.0


def test_no_anchor_or_invalid_sigma():
    assert filt(2.0, has_base=False)[0] == 0.0
    r = filt(2.0, sigma=0.0, active=True)
    assert r[0] == 0.0 and not r[4]


def test_constant_spread_moments():
    ts = np.arange(0, 700, dtype=np.int64) * NS
    grid, basis, sig, cov = exact_moments_5s(ts, np.full(700, 101.0), ts, np.full(700, 100.0),
                                             600 * NS, 690 * NS)
    assert np.allclose(basis, 1.0) and np.allclose(sig, 0.0, atol=1e-6) and cov.min() >= 0.99


def test_alternating_spread_moments():
    ts = np.arange(0, 700, dtype=np.int64) * NS
    spread = np.where(np.arange(700) % 2 == 0, 0.0, 2.0)
    grid, basis, sig, _ = exact_moments_5s(ts, 100.0 + spread, ts, np.full(700, 100.0),
                                           600 * NS, 605 * NS)
    assert abs(basis[0] - 1.0) < 1e-9 and abs(sig[0] - 1.0) < 1e-6


def test_moments_do_not_look_ahead():
    ts = np.arange(0, 700, dtype=np.int64) * NS
    spread = np.where(np.arange(700) < 600, 1.0, 100.0)
    grid, basis, _, _ = exact_moments_5s(ts, 100.0 + spread, ts, np.full(700, 100.0),
                                         600 * NS, 605 * NS)
    assert abs(basis[0] - 1.0) < 1e-9


if __name__ == '__main__':
    names = [n for n in sorted(globals()) if n.startswith('test_')]
    for n in names:
        globals()[n]()
        print('PASS', n)
    print('ALL', len(names), 'PASSED')