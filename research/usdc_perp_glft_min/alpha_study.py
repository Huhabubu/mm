"""Mid-price alpha study for GLFT market making on Binance ETHUSDC perp; no strategy change.

The only purpose is a GLFT alpha term: can the ETHUSDC mid over the next 100ms / 1s / 5s be predicted well
enough to shift the reservation price? Signals come from ETHUSDT and ETHUSDC (Tardis binance-futures).

Step 1, bucket table. A same-ms aggressive burst is a run of trades with the same exchange ms, consecutive ids,
the same taker side and monotone price (sweep_study.sweep_groups). Each burst is an event at the local arrival of
its last row (every field used is known then). Bursts are bucketed by notional and price excursion in ticks; the
target is the ETHUSDC bookTicker mid change signed by the taker side, from local t + 1ms to t + 1ms + h.

Step 2, mid prediction (method of the Kaggle notebook micro-price-prediction-with-lob-indicators). 10ms grid on
the local clock; features use only rows with local time <= t; target is the ETHUSDC mid change over the next h.
Ridge in closed form, train-half standardisation, clip +-10, three nested feature sets: A = ETHUSDC book and
trade features, B = A + ETHUSDT features and basis, C = B + burst features. First half fit, second half test,
embargo between them; placebo = target shifted +60s and refit. GBM (depth 3, 100 trees) on set C.
pred_out (optional dict) receives grid_ns, test_start_ns and the set-B ridge prediction B_<h> (ticks) of every
horizon on the whole grid, fitted on the first half only; the GLFT alpha backtest (quote mode 3) uses them.
"""
import time

import numpy as np
import pandas as pd
from numba import njit

from sweep_study import asof, read_trades

MS = 1_000_000
SEC = 1_000_000_000
STEP_NS = 10 * MS
NOTIONAL_EDGES = (1e4, 1e5, 1e6)
NOTIONAL_LABELS = ('<10k', '10k-100k', '100k-1m', '>=1m')
EXC_EDGES = (1, 2, 4, 10)
EXC_LABELS = ('0', '1', '2-3', '4-9', '10+')
EVENT_DELAY_NS = 1 * MS
EVENT_HORIZONS = (('100ms', 100 * MS), ('1s', SEC), ('5s', 5 * SEC))
GRID_HORIZONS = (('100ms', 10), ('1s', 100), ('5s', 500))
WINDOWS = (('100ms', 100 * MS), ('1s', SEC), ('5s', 5 * SEC))
PLACEBO_STEPS = 6000
EMBARGO_STEPS = PLACEBO_STEPS + 500
WARMUP_STEPS = 60000
MIN_BUCKET_N = 30
BIG_NOTIONAL = 1e5
EXC_CAP = 20
LAM = 1.0
CLIP = 10.0
TOP_Q = 0.95
CHUNK = 500_000
GBM_TRAIN_EVERY = 10
GBM_PARAMS = {'max_depth': 3, 'max_iter': 100, 'learning_rate': 0.1, 'early_stopping': False, 'random_state': 42}


@njit(cache=False)
def _decay(grid, t_ev, w_ev, tau):
    out = np.empty(grid.size)
    s = 0.0
    j = 0
    prev = grid[0] if grid.size else 0
    for k in range(grid.size):
        t = grid[k]
        s *= np.exp(-(t - prev) / tau)
        while j < t_ev.size and t_ev[j] <= t:
            s += w_ev[j] * np.exp(-(t - t_ev[j]) / tau)
            j += 1
        out[k] = s
        prev = t
    return out


@njit(cache=False)
def _ema(x, alpha):
    out = np.empty(x.size)
    e = x[0] if x.size else 0.0
    for k in range(x.size):
        e += alpha * (x[k] - e)
        out[k] = e
    return out


def decay(grid, t, w, tau_ns):
    return _decay(np.ascontiguousarray(grid, np.int64), np.ascontiguousarray(t, np.int64),
                  np.ascontiguousarray(w, np.float64), float(tau_ns))


def burst_events(tr):
    n = len(tr['loc'])
    if not n:
        return {k: np.zeros(0) for k in ('t', 'sg', 'notional', 'exc', 'rows')}
    st = tr['starts']
    en = np.r_[st[1:] - 1, n - 1]
    ev = {'t': np.maximum.reduceat(tr['loc'], st), 'sg': tr['sg'][st].astype(np.float64),
          'notional': tr['cum_notional'][en + 1] - tr['cum_notional'][st], 'exc': tr['exc'][en].astype(np.int64),
          'rows': (en - st + 1).astype(np.int64)}
    keep = ev['sg'] != 0
    order = np.argsort(ev['t'][keep], kind='stable')
    return {k: v[keep][order] for k, v in ev.items()}


def bucket_ids(ev):
    return (np.searchsorted(NOTIONAL_EDGES, ev['notional'], side='right'),
            np.searchsorted(EXC_EDGES, ev['exc'], side='right'))


def move_stats(x):
    x = x[np.isfinite(x)]
    if x.size < 2:
        return {'n': int(x.size)}
    m = float(x.mean())
    se = float(x.std(ddof=1) / np.sqrt(x.size))
    return {'n': int(x.size), 'mean_ticks': m, 'se_ticks': se, 't': m / se if se > 0 else None,
            'median_ticks': float(np.median(x)), 'p_cont': float(np.mean(x > 1e-9)),
            'p_rev': float(np.mean(x < -1e-9))}


def read_book(path):
    df = pd.read_csv(path, usecols=['local_timestamp', 'bid_price', 'bid_amount', 'ask_price', 'ask_amount'])
    ts = df['local_timestamp'].to_numpy(np.int64) * 1000
    bid = df['bid_price'].to_numpy(np.float64)
    ask = df['ask_price'].to_numpy(np.float64)
    bq = df['bid_amount'].to_numpy(np.float64)
    aq = df['ask_amount'].to_numpy(np.float64)
    del df
    ok = (np.isfinite(bid) & np.isfinite(ask) & np.isfinite(bq) & np.isfinite(aq) & (bid > 0) & (ask > bid)
          & (bq >= 0) & (aq >= 0) & (bq + aq > 0))
    ts, bid, ask, bq, aq = ts[ok], bid[ok], ask[ok], bq[ok], aq[ok]
    unsorted = int(np.count_nonzero(np.diff(ts) < 0))
    if unsorted:
        o = np.argsort(ts, kind='stable')
        ts, bid, ask, bq, aq = ts[o], bid[o], ask[o], bq[o], aq[o]
    e = np.zeros(len(ts))
    if len(ts) > 1:
        e[1:] = (np.where(bid[1:] >= bid[:-1], bq[1:], 0.0) - np.where(bid[1:] <= bid[:-1], bq[:-1], 0.0)
                 - np.where(ask[1:] <= ask[:-1], aq[1:], 0.0) + np.where(ask[1:] >= ask[:-1], aq[:-1], 0.0))
    return {'ts': ts, 'bid': bid, 'ask': ask, 'bq': bq, 'aq': aq, 'mid': 0.5 * (bid + ask), 'cum_ofi': np.cumsum(e),
            'rows': int(len(ts)), 'dropped': int((~ok).sum()), 'local_not_sorted_rows': unsorted}


def event_moves(ev, book, tick):
    t0 = ev['t'] + EVENT_DELAY_NS
    m0 = asof(book['ts'], book['mid'], t0)
    out = {}
    for lab, h in EVENT_HORIZONS:
        m1 = asof(book['ts'], book['mid'], t0 + h)
        if len(book['ts']):
            m1[t0 + h > book['ts'][-1]] = np.nan
        out[lab] = ev['sg'] * (m1 - m0) / tick
    return out


def bucket_table(ev, moves, t_split, hours):
    nb, eb = bucket_ids(ev)
    first = ev['t'] < t_split
    means = np.zeros((len(NOTIONAL_LABELS), len(EXC_LABELS)))
    rows = []
    for i, nl in enumerate(NOTIONAL_LABELS):
        for j, el in enumerate(EXC_LABELS):
            m = (nb == i) & (eb == j)
            row = {'notional': nl, 'excursion_ticks': el, 'n': int(m.sum()),
                   'per_hour': float(m.sum() / hours) if hours > 0 else None,
                   'buy_share': float(np.mean(ev['sg'][m] > 0)) if m.any() else None}
            for lab, _ in EVENT_HORIZONS:
                x = moves[lab]
                row[lab] = {'all': move_stats(x[m]), 'first_half': move_stats(x[m & first]),
                            'second_half': move_stats(x[m & ~first])}
            fit = moves['1s'][m & first]
            fit = fit[np.isfinite(fit)]
            if fit.size >= MIN_BUCKET_N:
                means[i, j] = float(fit.mean())
            rows.append(row)
    return rows, means


def book_features(book, grid, tick, pre, put, group):
    i = np.searchsorted(book['ts'], grid, side='right') - 1
    bid, ask, bq, aq = book['bid'][i], book['ask'][i], book['bq'][i], book['aq'][i]
    mid = 0.5 * (bid + ask)
    put(f'{pre}_qi1', (bq - aq) / (bq + aq), group)
    put(f'{pre}_micro_dev', ((ask * bq + bid * aq) / (bq + aq) - mid) / tick, group)
    put(f'{pre}_spread', (ask - bid) / tick, group)
    cum = book['cum_ofi']
    for lab, w in WINDOWS[:2]:
        j = np.searchsorted(book['ts'], grid - w, side='right') - 1
        put(f'{pre}_ofi_{lab}', cum[i] - np.where(j >= 0, cum[np.maximum(j, 0)], 0.0), group)
    for lab, w in WINDOWS:
        k = int(w // STEP_NS)
        r = np.zeros(len(grid))
        if k < len(grid):
            r[k:] = (mid[k:] - mid[:-k]) / tick
        put(f'{pre}_ret_{lab}', r, group)
    stale = float(np.mean(grid - book['ts'][i] > SEC))
    return mid, stale


def trade_features(tr, grid, pre, put, group):
    order = np.argsort(tr['loc'], kind='stable')
    ts = tr['loc'][order]
    notional = np.diff(tr['cum_notional'])[order]
    sg = tr['sg'][order]
    cb = np.r_[0.0, np.cumsum(notional * (sg > 0))]
    cs = np.r_[0.0, np.cumsum(notional * (sg < 0))]
    i = np.searchsorted(ts, grid, side='right')
    for lab, w in WINDOWS:
        j = np.searchsorted(ts, grid - w, side='right')
        b = cb[i] - cb[j]
        s = cs[i] - cs[j]
        put(f'{pre}_timb_{lab}', np.where(b + s > 0, (b - s) / np.maximum(b + s, 1e-12), 0.0), group)


def burst_features(ev, grid, pre, means, put, group):
    t, sg, exc = ev['t'], ev['sg'], ev['exc']
    sw = exc >= 1
    big = sw & (ev['notional'] >= BIG_NOTIONAL)
    put(f'{pre}_sw_100ms', decay(grid, t[sw], sg[sw], 100 * MS), group)
    put(f'{pre}_sw_1s', decay(grid, t[sw], sg[sw], SEC), group)
    put(f'{pre}_sw_big_1s', decay(grid, t[big], sg[big], SEC), group)
    put(f'{pre}_sw_exc_1s', decay(grid, t[sw], sg[sw] * np.minimum(exc[sw], EXC_CAP), SEC), group)
    nb, eb = bucket_ids(ev)
    w = sg * means[nb, eb]
    nz = w != 0
    put(f'{pre}_bucket_1s', decay(grid, t[nz], w[nz], SEC), group)


def gram(F, rows, ys):
    p = F.shape[1]
    G = np.zeros((p, p))
    s = np.zeros(p)
    c = np.zeros((len(ys), p))
    sy = np.zeros(len(ys))
    for a in range(rows.start, rows.stop, CHUNK):
        b = min(a + CHUNK, rows.stop)
        X = F[a:b].astype(np.float64)
        Y = np.stack([y[a:b] for y in ys])
        G += X.T @ X
        s += X.sum(0)
        c += Y @ X
        sy += Y.sum(1)
    return G, s, c, sy, rows.stop - rows.start


def ridge_solve(G, s, c_k, sy_k, n, cols):
    cols = np.asarray(cols)
    m = s[cols] / n
    ybar = sy_k / n
    A = G[np.ix_(cols, cols)] / n - np.outer(m, m) + (LAM / n) * np.eye(len(cols))
    beta = np.linalg.solve(A, c_k[cols] / n - m * ybar)
    return beta, float(ybar - m @ beta)


def predict(F, rows, cols, beta, a0):
    out = np.empty(rows.stop - rows.start)
    for a in range(rows.start, rows.stop, CHUNK):
        b = min(a + CHUNK, rows.stop)
        out[a - rows.start:b - rows.start] = F[a:b][:, cols].astype(np.float64) @ beta + a0
    return out


def metrics(pred, y, thr, bp_per_tick):
    ok = np.isfinite(y) & np.isfinite(pred)
    p, y = pred[ok], y[ok]
    sst = float(np.sum(y * y))
    out = {'n': int(y.size), 'r2_pct': 100.0 * (1.0 - float(np.sum((y - p) ** 2)) / sst) if sst > 0 else None,
           'moved_frac': float(np.mean(y != 0)), 'target_std_ticks': float(y.std())}
    mv = y != 0
    out['dir_acc_moved'] = float(np.mean(np.sign(p[mv]) == np.sign(y[mv]))) if mv.any() else None
    top = np.abs(p) >= thr
    if top.any():
        sy = np.sign(p[top]) * y[top]
        tm = sy != 0
        out['top5'] = {'thr_ticks': float(thr), 'frac': float(top.mean()),
                       'mean_abs_pred_ticks': float(np.abs(p[top]).mean()),
                       'mean_signed_move_ticks': float(sy.mean()),
                       'mean_signed_move_bp': float(sy.mean() * bp_per_tick),
                       'median_signed_move_ticks': float(np.median(sy)),
                       'hit_rate_moved': float(np.mean(sy[tm] > 0)) if tm.any() else None,
                       'mean_abs_move_ticks': float(np.abs(y[top]).mean())}
    return out


def fwd(mid, k, tick):
    y = np.full(len(mid), np.nan)
    if k < len(mid):
        y[:-k] = (mid[k:] - mid[:-k]) / tick
    return y


def shifted(y, k):
    out = np.full(len(y), np.nan)
    if k < len(y):
        out[:-k] = y[k:]
    return out


def run_alpha_study(target_trades, signal_trades, target_bbo, signal_bbo, tick, names=('ETHUSDC', 'ETHUSDT'),
                    gbm=True, pred_out=None):
    t_start = time.time()
    tgt, sig = names
    book = {tgt: read_book(target_bbo), sig: read_book(signal_bbo)}
    trades = {tgt: read_trades(target_trades, tick), sig: read_trades(signal_trades, tick)}
    ev = {nm: burst_events(trades[nm]) for nm in names}
    start = max(int(book[tgt]['ts'][0]), int(book[sig]['ts'][0]))
    start = -(-start // STEP_NS) * STEP_NS
    end = min(int(book[tgt]['ts'][-1]), int(book[sig]['ts'][-1]))
    n = (end - start) // STEP_NS + 1
    grid = start + np.arange(n, dtype=np.int64) * STEP_NS
    warm = min(WARMUP_STEPS, n // 10)
    split = n // 2
    tr = slice(warm, split - EMBARGO_STEPS)
    te = slice(split, n - EMBARGO_STEPS)
    if tr.stop - tr.start < 1000 or te.stop - te.start < 1000:
        raise ValueError(f'grid too short: {n} rows')
    hours = (grid[-1] - grid[0]) / 3.6e12
    bp_per_tick = float(tick / np.median(book[tgt]['mid']) * 1e4)
    rep = {'config': {'purpose': 'GLFT alpha: predict the ETHUSDC mid', 'tick': tick, 'bp_per_tick': bp_per_tick,
                      'step_ms': STEP_NS // MS, 'grid_horizons': [lab for lab, _ in GRID_HORIZONS],
                      'event_delay_ns': EVENT_DELAY_NS, 'notional_labels': list(NOTIONAL_LABELS),
                      'excursion_labels': list(EXC_LABELS), 'placebo_steps': PLACEBO_STEPS,
                      'embargo_steps': EMBARGO_STEPS, 'warmup_steps': warm, 'lam': LAM, 'clip': CLIP, 'top_q': TOP_Q,
                      'gbm': GBM_PARAMS, 'gbm_train_every': GBM_TRAIN_EVERY,
                      'event': 'burst = same exchange ms, consecutive ids, same side, monotone price; '
                               'event at local arrival of its last row',
                      'r2': '1 - SSE / sum(y^2) on the test half'},
           'grid': {'rows': int(n), 'start_ns': int(start), 'hours': float(hours),
                    'train_rows': [tr.start, tr.stop], 'test_rows': [te.start, te.stop]},
           'checks': {nm: {'book_rows': book[nm]['rows'], 'book_dropped': book[nm]['dropped'],
                           'book_local_not_sorted_rows': book[nm]['local_not_sorted_rows'],
                           'trades': trades[nm]['checks']} for nm in names},
           'events': {}}
    t_split = int(grid[split])
    means = {}
    for nm in names:
        moves = event_moves(ev[nm], book[tgt], tick)
        rows, means[nm] = bucket_table(ev[nm], moves, t_split, hours)
        rep['events'][nm] = {'bursts': int(len(ev[nm]['t'])), 'sweeps_exc_ge1': int(np.sum(ev[nm]['exc'] >= 1)),
                             'all_bursts': {lab: move_stats(moves[lab]) for lab, _ in EVENT_HORIZONS},
                             'buckets': rows, 'bucket_mean_1s_first_half': means[nm].tolist()}
    cols, fnames, groups = [], [], []

    def put(name, arr, group):
        cols.append(np.nan_to_num(np.asarray(arr, np.float64), nan=0.0, posinf=0.0, neginf=0.0).astype(np.float32))
        fnames.append(name)
        groups.append(group)

    mid_c, stale_c = book_features(book[tgt], grid, tick, 'c', put, 'A')
    trade_features(trades[tgt], grid, 'c', put, 'A')
    mid_u, stale_u = book_features(book[sig], grid, tick, 'u', put, 'B')
    trade_features(trades[sig], grid, 'u', put, 'B')
    basis = (mid_u - mid_c) / tick
    for lab, tau in (('10s', 10 * SEC), ('60s', 60 * SEC)):
        put(f'basis_dev_{lab}', basis - _ema(basis, 1.0 - np.exp(-STEP_NS / tau)), 'B')
    burst_features(ev[sig], grid, 'u', means[sig], put, 'C')
    burst_features(ev[tgt], grid, 'c', means[tgt], put, 'C')
    rep['checks']['stale_book_gt_1s_frac'] = {tgt: stale_c, sig: stale_u}
    del book, trades, ev
    F = np.empty((n, len(cols)), np.float32)
    for j in range(len(cols)):
        F[:, j] = cols[j]
        cols[j] = None
    del cols
    for j in range(F.shape[1]):
        v = F[tr, j].astype(np.float64)
        mu, sd = v.mean(), v.std()
        F[:, j] = np.clip((F[:, j] - mu) / (sd if sd > 0 else 1.0), -CLIP, CLIP)
    sets = {'A': [j for j, g in enumerate(groups) if g == 'A'],
            'B': [j for j, g in enumerate(groups) if g in ('A', 'B')],
            'C': list(range(len(groups)))}
    rep['features'] = {'names': fnames, 'groups': groups}
    ys = [fwd(mid_c, k, tick) for _, k in GRID_HORIZONS]
    ys += [shifted(y, PLACEBO_STEPS) for y in ys]
    nh = len(GRID_HORIZONS)
    for k in range(len(ys)):
        if not np.all(np.isfinite(ys[k][tr])):
            raise ValueError('non-finite target in the train rows')
    G, s, c, sy, ntr = gram(F, tr, ys)
    rep['ridge'] = {}
    rep['coef_C'] = {}
    for sname, sc in sets.items():
        rep['ridge'][sname] = {}
        for hi, (lab, _) in enumerate(GRID_HORIZONS):
            blk = {}
            for kind, k in (('test', hi), ('placebo', hi + nh)):
                beta, a0 = ridge_solve(G, s, c[k], sy[k], ntr, sc)
                thr = float(np.quantile(np.abs(predict(F, tr, sc, beta, a0)), TOP_Q))
                blk[kind] = metrics(predict(F, te, sc, beta, a0), ys[k][te], thr, bp_per_tick)
                if sname == 'C' and kind == 'test':
                    rep['coef_C'][lab] = {fnames[j]: float(b) for j, b in zip(sc, beta)}
            rep['ridge'][sname][lab] = blk
    if pred_out is not None:
        pred_out['grid_ns'] = grid
        pred_out['test_start_ns'] = int(grid[te.start])
        for hi, (lab, _) in enumerate(GRID_HORIZONS):
            beta, a0 = ridge_solve(G, s, c[hi], sy[hi], ntr, sets['B'])
            pred_out[f'B_{lab}'] = predict(F, slice(0, n), sets['B'], beta, a0)
    rep['univariate_r2_pct'] = {}
    for j, nm in enumerate(fnames):
        rep['univariate_r2_pct'][nm] = {}
        for hi, (lab, _) in enumerate(GRID_HORIZONS):
            beta, a0 = ridge_solve(G, s, c[hi], sy[hi], ntr, [j])
            y = ys[hi][te]
            p = F[te, j].astype(np.float64) * beta[0] + a0
            ok = np.isfinite(y)
            rep['univariate_r2_pct'][nm][lab] = 100.0 * (1.0 - float(np.sum((y[ok] - p[ok]) ** 2)) /
                                                         float(np.sum(y[ok] ** 2)))
    if gbm:
        try:
            from sklearn.ensemble import HistGradientBoostingRegressor
            idx = np.arange(tr.start, tr.stop, GBM_TRAIN_EVERY)
            sc = sets['C']
            Xs = F[idx][:, sc]
            rep['gbm_C'] = {}
            for hi, (lab, _) in enumerate(GRID_HORIZONS):
                blk = {}
                for kind, k in (('test', hi), ('placebo', hi + nh)):
                    model = HistGradientBoostingRegressor(**GBM_PARAMS).fit(Xs, ys[k][idx])
                    thr = float(np.quantile(np.abs(model.predict(Xs)), TOP_Q))
                    pte = np.empty(te.stop - te.start)
                    for a in range(te.start, te.stop, CHUNK):
                        b = min(a + CHUNK, te.stop)
                        pte[a - te.start:b - te.start] = model.predict(F[a:b][:, sc])
                    blk[kind] = metrics(pte, ys[k][te], thr, bp_per_tick)
                rep['gbm_C'][lab] = blk
        except Exception as exc:
            rep['gbm_C'] = {'error': f'{type(exc).__name__}: {exc}'}
    rep['runtime_s'] = round(time.time() - t_start, 1)
    return rep
