import numpy as np

from wall_study import FILL_COLS, book_series, breach_flags, virtual_orders

LAT = 1000
N = 32
BOOK = [(0, 0, 1, 10, 1.0, 1), (0, 0, 1, 8, 10.0, 1), (0, 0, -1, 14, 1.0, 1)]


def ev(rows):
    a = np.array(rows, dtype=np.float64)
    return (a[:, 0].astype(np.int64), a[:, 1].astype(np.int8), a[:, 2].astype(np.int8),
            a[:, 3].astype(np.int64), a[:, 4].copy(), a[:, 5].astype(np.bool_))


def run(rows, mode=1, dist=0.0, thr=5.0, frac=-1.0):
    fills = np.zeros((16, len(FILL_COLS)))
    cancels = np.zeros((16, 4))
    nf, nc, cnt = virtual_orders(*ev(rows), N, mode, dist, thr, frac, LAT, 50.0, 60.0, fills, cancels)
    return nf, nc, cnt, fills, cancels


def test_book_series():
    mid_ts, mid_v, bbo_ts, bbv, bav, samples, over = book_series(*ev(BOOK + [(5, 0, 1, 10, 0.0, 0)]), N,
                                                                 1_000_000_000, 50.0)
    assert list(mid_v) == [12.0, 11.0] and list(mid_ts) == [0, 5]
    assert list(bbv) == [10, 8] and list(bav) == [14, 14]
    assert sorted(samples) == [1.0, 1.0, 10.0] and over == 0


def test_wall_front_fill():
    nf, nc, cnt, f, _ = run(BOOK + [(2 * LAT, 1, -1, 9, 0.5, 0)])
    assert nf == 1 and cnt[0] == 1
    assert f[0, 1] == 1.0 and f[0, 2] == 9 and f[0, 4] == 3.0 and f[0, 6] == 10.0
    assert f[0, 7] == 4.0 and f[0, 8] == 0.0 and f[0, 10] == LAT


def test_latency_blocks_early_fill():
    nf, _, cnt, _, _ = run(BOOK + [(LAT - 1, 1, -1, 9, 0.5, 0)])
    assert nf == 0 and cnt[0] == 0


def test_queue_consumption():
    rows = BOOK + [(0, 0, 1, 9, 2.0, 1), (2 * LAT, 1, -1, 9, 1.0, 0)]
    assert run(rows)[0] == 0
    nf, _, _, f, _ = run(rows + [(3 * LAT, 1, -1, 9, 1.5, 0)])
    assert nf == 1 and f[0, 8] == 2.0


def test_cancel_on_shrink_with_latency():
    rows = BOOK + [(2 * LAT, 0, 1, 8, 4.0, 0), (3 * LAT + 1, 1, -1, 9, 0.5, 0)]
    nf, nc, cnt, _, c = run(rows, frac=0.5)
    assert nf == 0 and nc == 1 and cnt[2] == 1 and c[0, 2] == 8 and c[0, 3] == 1
    nf, nc, _, f, _ = run(rows, frac=-1.0)
    assert nf == 1 and nc == 0 and abs(f[0, 9] - 0.4) < 1e-12


def test_fixed_offset_queue():
    rows = BOOK + [(2 * LAT, 1, -1, 10, 0.5, 0), (3 * LAT, 1, -1, 10, 0.6, 0)]
    nf, _, _, f, _ = run(rows, mode=0, dist=2.0)
    assert nf == 1 and f[0, 2] == 10 and f[0, 8] == 1.0


def test_breach_flags():
    bts = np.array([0, 10, 20], np.int64)
    bbv = np.array([10, 9, 7], np.int64)
    bav = np.array([14, 14, 14], np.int64)
    t = np.array([0], np.int64)
    sg = np.array([1.0])
    wall = np.array([8], np.int64)
    assert not breach_flags(t, sg, wall, bts, bbv, bav, 15)[0]
    assert breach_flags(t, sg, wall, bts, bbv, bav, 25)[0]


if __name__ == '__main__':
    for name, fn in sorted(globals().items()):
        if name.startswith('test_') and callable(fn):
            fn()
            print('PASS', name)
