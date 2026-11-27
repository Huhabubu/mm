import os

import numpy as np

from fill_sweep_check import blocked_fraction, bucket, classify, fill_mids, fill_sweep_check, merge_events, sweep_event_arrays

MS = 1_000_000


def test_classify():
    ev = {'t_star': (np.array([50.0, 60.0, 99.5]) * MS).astype(np.int64), 'sg': np.array([-1.0, 1.0, -1.0])}
    T = np.array([100 * MS, 100 * MS], dtype=np.int64)
    s = np.array([1.0, -1.0])
    adv, fav, late = classify(T, s, ev, MS)
    assert adv[0] == 50 * MS and fav[0] == 40 * MS and late[0]
    assert adv[1] == 40 * MS and fav[1] == 50 * MS and not late[1]


def test_blocked_and_bucket():
    t = np.array([0, 50, 500], dtype=np.int64) * MS
    assert abs(blocked_fraction(t, 100 * MS, 0, 1000 * MS) - 0.25) < 1e-12
    f = np.array([[0.0, 1.0, 100.0, 1.0], [0.0, -1.0, 100.0, 2.0]])
    mids = {'1s': np.array([101.0, 101.0])}
    b = bucket(np.array([True, True]), f, mids)
    assert b['n'] == 2 and abs(b['1s']['sum_usdc'] - (1.0 - 2.0)) < 1e-12
    assert abs(b['1s']['mean_bp'] - 0.0) < 1e-9


def test_fill_mids_index():
    step = 100 * MS
    mid = np.arange(1000, dtype=np.float64)
    f = np.array([[250.0 * MS, 1.0, 1.0, 1.0]])
    m = fill_mids(f, mid, 1000, 0, step)
    assert m['100ms'][0] == 3.0 and m['1s'][0] == 12.0 and np.isnan(fill_mids(f, mid, 5, 0, step)['1s'][0])


def test_end_to_end():
    d = os.path.join(os.path.dirname(os.path.abspath(__file__)), '_fill_sweep_e2e')
    os.makedirs(d, exist_ok=True)
    p = os.path.join(d, 'trades.csv')
    rows = ['exchange,symbol,timestamp,local_timestamp,id,side,price,amount']
    for i, px in enumerate((100.00, 99.99, 99.97)):
        rows.append(f'x,ETHUSDC,10000,{12000 + i},{1 + i},sell,{px:.2f},1')
    rows.append('x,ETHUSDC,20000,22000,4,buy,100.00,1')
    open(p, 'w', newline='\n').write('\n'.join(rows) + '\n')
    ev = sweep_event_arrays(p, 0.01, (1, 4))
    assert len(ev[1]['t_star']) == 1 and ev[1]['t_star'][0] == 12001 * 1000 and ev[1]['sg'][0] == -1.0
    assert len(ev[4]['t_star']) == 0
    both = merge_events(ev[1], ev[4])
    f = np.array([[12001 * 1000 + 50 * MS, 1.0, 99.0, 1.0], [12001 * 1000 + 50 * MS, -1.0, 101.0, 1.0]])
    mid = np.full(10, 100.0)
    rep = fill_sweep_check(f, {'s_j1': both}, MS, mid, 10, 0, 100 * MS)
    b = rep['by_source']['s_j1']['100ms']
    assert b['adverse_actionable']['n'] == 1 and b['favorable_only']['n'] == 1 and b['none']['n'] == 0
    assert rep['by_source']['s_j1']['1000ms']['adverse_actionable']['n'] == 1


if __name__ == '__main__':
    for name, fn in sorted(globals().items()):
        if name.startswith('test_') and callable(fn):
            fn()
            print('PASS', name)
