"""Unit tests for the USDT/USDC event study (pure numpy)."""
import numpy as np

from event_study import MS, asof, clocks_from_arrays, fill_conditioning, last_per_timestamp, response

TICK = 0.01


def series(points):
    return np.array([p[0] for p in points], np.int64), np.array([p[1] for p in points], np.float64)


def test_last_per_timestamp_keeps_last_row():
    ts, mid = last_per_timestamp(np.array([1, 1, 2, 3, 3], np.int64), np.array([1., 2., 3., 4., 5.]))
    assert ts.tolist() == [1, 2, 3] and mid.tolist() == [2., 3., 5.]


def test_asof_is_at_or_before():
    s = series([(10, 1.0), (20, 2.0)])
    assert np.isnan(asof(s, np.array([9]))[0])
    assert asof(s, np.array([10, 19, 25])).tolist() == [1., 1., 2.]


def test_exchange_clock_is_sorted():
    c = clocks_from_arrays(np.array([1, 2, 3], np.int64), np.array([5, 4, 6], np.int64), np.array([1., 2., 3.]))
    assert c['exch'][0].tolist() == [4, 5, 6] and c['exch'][1].tolist() == [2., 1., 3.]
    assert c['exch_out_of_order_rows'] == 1


def test_response_lead_and_lag():
    src = series([(0, 100.0), (10 * MS, 100.01)])
    dst = series([(0, 200.0), (12 * MS, 200.01)])
    r = response(src, dst, TICK)
    assert r['events'] == 1 and r['same_ts']['mean_ticks'] == 0.0
    assert r['post']['1ms']['mean_ticks'] == 0.0 and abs(r['post']['5ms']['mean_ticks'] - 1.0) < 1e-6
    assert r['pre']['5ms']['mean_ticks'] == 0.0
    back = response(dst, src, TICK)
    assert abs(back['pre']['5ms']['mean_ticks'] - 1.0) < 1e-6 and back['post']['5ms']['mean_ticks'] == 0.0


def test_fill_conditioning_clocks():
    sig = {'exch': series([(0, 100.0), (18 * MS, 99.99)]), 'local': series([(0, 100.0), (19_500_000, 99.99)])}
    tgt = {'exch': series([(0, 200.0)]), 'local': series([(0, 200.0)])}
    fills = np.array([[20 * MS, 1.0, 200.0, 0.03]])

    def mk(sub):
        assert len(sub) == 1
        return {'100ms': {'mean_bp': -1.0}}

    r = fill_conditioning(fills, sig, tgt, TICK, MS, mk)
    assert r['signal_exch']['5ms']['adverse']['n'] == 1
    assert r['signal_exch']['5ms']['adverse']['markout_mean_bp']['100ms'] == -1.0
    assert r['signal_exch']['1ms']['none']['n'] == 1
    assert r['signal_actionable']['5ms']['none']['n'] == 1
    assert r['target_exch']['5ms']['none']['n'] == 1
    assert r['signal_actionable_when_target_flat']['5ms']['none']['n'] == 1


if __name__ == '__main__':
    names = [n for n in sorted(globals()) if n.startswith('test_')]
    for n in names:
        globals()[n]()
        print('PASS', n)
    print('ALL', len(names), 'PASSED')
