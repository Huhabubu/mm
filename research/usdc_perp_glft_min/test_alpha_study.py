import os

import numpy as np

from alpha_study import MS, SEC, bucket_ids, burst_events, decay, run_alpha_study
from sweep_study import sweep_groups

NL = chr(10)
TICK = 0.01


def test_decay_causal():
    grid = np.array([0, 10, 20, 30], np.int64) * MS
    t = np.array([10 * MS, 25 * MS], np.int64)
    w = np.array([1.0, -2.0])
    out = decay(grid, t, w, 10 * MS)
    assert out[0] == 0.0 and abs(out[1] - 1.0) < 1e-12
    assert abs(out[2] - np.exp(-1.0)) < 1e-12
    assert abs(out[3] - (np.exp(-2.0) - 2.0 * np.exp(-0.5))) < 1e-12


def test_burst_events_and_buckets():
    exch = np.array([5, 5, 5, 9, 9], np.int64)
    tid = np.array([1, 2, 3, 4, 5], np.int64)
    sg = np.array([1, 1, 1, -1, -1], np.int64)
    pt = np.array([100, 101, 103, 103, 103], np.int64)
    loc = np.array([7, 8, 8, 12, 11], np.int64)
    px = pt * TICK
    qty = np.array([1e3, 1e3, 1e3, 2e3, 2e3]) / px
    gid, starts, exc = sweep_groups(exch, tid, sg, pt)
    tr = {'loc': loc, 'sg': sg, 'starts': starts, 'exc': exc, 'cum_notional': np.r_[0.0, np.cumsum(px * qty)]}
    ev = burst_events(tr)
    assert list(ev['t']) == [8, 12]
    assert list(ev['sg']) == [1.0, -1.0] and list(ev['exc']) == [3, 0] and list(ev['rows']) == [3, 2]
    assert np.allclose(ev['notional'], [3e3, 4e3])
    nb, eb = bucket_ids({'notional': np.array([5e3, 1e4, 2e6]), 'exc': np.array([0, 1, 12])})
    assert list(nb) == [0, 1, 3] and list(eb) == [0, 1, 4]


def write_csv(path, header, rows):
    with open(path, 'w', encoding='utf-8', newline='') as fh:
        fh.write(header + NL)
        fh.write(NL.join(rows) + NL)


def synthetic(d, seconds=300, seed=7):
    rng = np.random.default_rng(seed)
    step_us = 5000
    n = seconds * 1_000_000 // step_us
    t_us = np.arange(n, dtype=np.int64) * step_us + 1_000_000
    jumps = np.zeros(n, np.int64)
    burst_rows, tid = [], 1
    for k in np.flatnonzero(rng.random(n) < 0.012):
        s = 1 if rng.random() < 0.5 else -1
        jumps[k] += 3 * s
        base = 300000 + int(jumps[:k].sum())
        for r in range(3):
            side = 'buy' if s > 0 else 'sell'
            burst_rows.append(f'x,ETHUSDT,{t_us[k]},{t_us[k] + 1000},{tid},{side},{(base + s * r) * TICK:.2f},2')
            tid += 1
    noise = rng.random(n) < 0.004
    jumps[noise] += rng.choice([-1, 1], noise.sum())
    b_u = 300000 + np.cumsum(jumps)
    lag = 60
    b_c = np.r_[np.full(lag, b_u[0]), b_u[:-lag]]
    hdr = 'exchange,symbol,timestamp,local_timestamp,ask_amount,ask_price,bid_price,bid_amount'
    qb = rng.uniform(1, 5, n)
    qa = rng.uniform(1, 5, n)
    for name, b, path in (('ETHUSDT', b_u, 'ub.csv'), ('ETHUSDC', b_c, 'cb.csv')):
        write_csv(os.path.join(d, path), hdr,
                  [f'x,{name},{t},{t + 1000},{a:.3f},{(bb + 1) * TICK:.2f},{bb * TICK:.2f},{q:.3f}'
                   for t, bb, a, q in zip(t_us, b, qa, qb)])
    thdr = 'exchange,symbol,timestamp,local_timestamp,id,side,price,amount'
    write_csv(os.path.join(d, 'ut.csv'), thdr, burst_rows)
    crow = []
    for i, k in enumerate(np.flatnonzero(rng.random(n) < 0.01)):
        side = 'buy' if rng.random() < 0.5 else 'sell'
        crow.append(f'x,ETHUSDC,{t_us[k]},{t_us[k] + 1000},{i + 1},{side},{b_c[k] * TICK:.2f},0.5')
    write_csv(os.path.join(d, 'ct.csv'), thdr, crow)
    return len(burst_rows) // 3


def test_end_to_end():
    d = '_alpha_e2e'
    os.makedirs(d, exist_ok=True)
    n_bursts = synthetic(d)
    p = lambda f: os.path.join(d, f)
    po = {}
    rep = run_alpha_study(p('ct.csv'), p('ut.csv'), p('cb.csv'), p('ub.csv'), TICK, gbm=True, pred_out=po)
    n = rep['grid']['rows']
    for key in ('grid_ns', 'B_100ms', 'B_1s', 'B_5s'):
        assert po[key].shape == (n,), key
    assert np.all(np.isfinite(po['B_1s'])) and int(po['grid_ns'][0]) == rep['grid']['start_ns']
    assert po['test_start_ns'] == int(po['grid_ns'][rep['grid']['test_rows'][0]])
    u = rep['events']['ETHUSDT']
    assert u['bursts'] == n_bursts and u['sweeps_exc_ge1'] == n_bursts
    assert sum(b['n'] for b in u['buckets']) == n_bursts
    assert u['all_bursts']['1s']['mean_ticks'] > 2.0
    r = rep['ridge']
    assert r['B']['1s']['test']['r2_pct'] > 10.0 and r['C']['1s']['test']['r2_pct'] > 10.0
    assert r['A']['1s']['test']['r2_pct'] < r['B']['1s']['test']['r2_pct']
    for s in ('A', 'B', 'C'):
        for h in ('100ms', '1s', '5s'):
            assert r[s][h]['placebo']['r2_pct'] < 1.0, (s, h, r[s][h]['placebo'])
    assert r['C']['1s']['test']['top5']['mean_signed_move_ticks'] > 1.0
    gerr = rep['gbm_C'].get('error', '')
    if os.name == 'nt' and gerr.startswith('PermissionError'):
        print('SKIP gbm assert (local sandbox blocks multiprocessing pipes):', gerr)
    else:
        assert not gerr and rep['gbm_C']['1s']['test']['r2_pct'] > 5.0, rep['gbm_C']
    assert len(rep['features']['names']) == len(rep['features']['groups']) == 34


if __name__ == '__main__':
    for name, fn in list(globals().items()):
        if name.startswith('test_') and callable(fn):
            fn()
            print('PASS', name)
