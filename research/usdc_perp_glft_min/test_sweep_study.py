import os

import numpy as np

from sweep_study import MS, event_block, first_crossings, run_sweep_study, signed_return, sweep_groups

NL = chr(10)


def test_sweep_groups():
    exch = np.array([10, 10, 10, 10, 20, 20], np.int64)
    tid = np.array([1, 2, 3, 5, 6, 7], np.int64)
    sg = np.array([1, 1, 1, 1, -1, -1], np.int64)
    pt = np.array([100, 101, 103, 103, 103, 104], np.int64)
    gid, starts, exc = sweep_groups(exch, tid, sg, pt)
    assert list(gid) == [0, 0, 0, 1, 2, 3] and list(starts) == [0, 3, 4, 5]
    assert list(exc) == [0, 1, 3, 0, 0, 0]
    assert list(first_crossings(gid, exc, 1)) == [1] and list(first_crossings(gid, exc, 2)) == [2]
    assert len(first_crossings(gid, exc, 4)) == 0


def test_sell_sweep_excursion():
    gid, starts, exc = sweep_groups(np.zeros(3, np.int64), np.arange(3), -np.ones(3, np.int64),
                                    np.array([50, 48, 48], np.int64))
    assert list(gid) == [0, 0, 0] and list(exc) == [0, 2, 2]


def tgt():
    ts = np.array([0, 6 * MS, 7 * MS], np.int64)
    mid = np.array([100.0, 101.0, 103.0])
    return {'ts': ts, 'mid': mid, 'bid': mid - 0.5, 'ask': mid + 0.5}


def test_signed_return_and_delay():
    t = tgt()
    r, bp = signed_return(t['ts'], t['mid'], np.array([6 * MS]), MS, np.array([1.0]), 1.0)
    assert r[0] == 2.0 and abs(bp[0] - 2.0 / 101.0 * 1e4) < 1e-9
    r, _ = signed_return(t['ts'], t['mid'], np.array([6 * MS]), 10 * MS, np.array([1.0]), 1.0)
    assert np.isnan(r[0])
    r, _ = signed_return(t['ts'], t['mid'], np.array([6 * MS]), MS, np.array([-1.0]), 1.0)
    assert r[0] == -2.0


def test_event_block_race():
    ev = {'t_star': np.array([6 * MS], np.int64), 't_first': np.array([6 * MS], np.int64),
          'sg': np.array([1.0]), 'px': np.array([103.0]), 'notional': np.array([500.0])}
    out = event_block(ev, tgt(), 1.0, True)
    assert out['delay']['d0ms']['1ms']['mean_ticks'] == 2.0 and out['delay']['d0ms']['10ms'] is None
    assert out['delay']['d1ms']['1ms'] is None
    assert out['race_touch_reflected']['0ms'] == 0.0 and out['race_touch_reflected']['1ms'] == 1.0
    assert out['by_notional'][0]['1ms']['n'] == 1 and out['by_notional'][1]['1ms'] is None
    assert out['by_target_pre_5ms']['with']['n'] == 1 and out['by_target_pre_5ms']['flat']['n'] == 0


def write(path, header, rows):
    with open(path, 'w') as fh:
        fh.write(header + NL + NL.join(rows) + NL)


def test_run_end_to_end():
    d = os.path.join(os.path.dirname(os.path.abspath(__file__)), '_sweep_e2e'); os.makedirs(d, exist_ok=True)
    th = 'exchange,symbol,timestamp,local_timestamp,id,side,price,amount'
    write(os.path.join(d, 'c.csv'), th, ['x,ETHUSDC,1000,1500,1,buy,100.00,1', 'x,ETHUSDC,1000,1600,2,buy,100.01,1',
                                         'x,ETHUSDC,1000,1700,3,buy,100.03,1', 'x,ETHUSDC,5500,5400,9,sell,100.02,1'])
    write(os.path.join(d, 't.csv'), th, ['x,ETHUSDT,1000,1200,7,sell,100.50,2'])
    bh = 'exchange,symbol,timestamp,local_timestamp,ask_amount,ask_price,bid_price,bid_amount'
    write(os.path.join(d, 'cb.csv'), bh, ['x,ETHUSDC,0,100,1,100.01,99.99,1', 'x,ETHUSDC,1650,1800,1,100.04,100.02,1'])
    write(os.path.join(d, 'tb.csv'), bh, ['x,ETHUSDT,0,300,1,100.51,100.49,1'])
    rep = run_sweep_study(*(os.path.join(d, f) for f in ('c.csv', 't.csv', 'cb.csv', 'tb.csv')), 0.01)
    ev = rep['events']['ETHUSDC']
    assert ev['j1']['n'] == 1 and ev['j2']['n'] == 1 and ev['j4']['n'] == 0
    assert abs(ev['j1']['notional_to_trigger']['p50'] - 200.01) < 1e-9
    assert rep['events']['ETHUSDT']['j1']['n'] == 0
    lat = rep['latency_ms']['ETHUSDC_trades']
    assert lat['rows'] == 4 and abs(lat['p50'] - 0.55) < 1e-9
    assert lat['exch_ms_aligned_frac'] == 0.75 and lat['frac_negative'] == 0.25
    g = rep['groups']['ETHUSDC']
    assert g['groups'] == 2 and g['multi_row_groups'] == 1 and g['rows_in_multi_row_groups'] == 3
    assert rep['checks']['ETHUSDC_trades']['min_price_step_ticks'] == 1


if __name__ == '__main__':
    for name, fn in sorted(globals().items()):
        if name.startswith('test_') and callable(fn):
            fn()
            print('PASS', name)
