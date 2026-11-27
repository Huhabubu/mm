'''Sweep event study on Binance ETHUSDC (USDC-M) and ETHUSDT (USDT-M) perps; strategy unchanged.

Trades are Tardis binance-futures trades, mapped from the @trade stream (one row per fill; timestamp is the
exchange trade time T, local_timestamp the capture arrival time). A sweep group is a run of rows in file
(arrival) order with consecutive trade ids, the same exchange timestamp and taker side, and prices monotone
in the taker direction. Grouping uses exchange fields only; everything after that uses the local clock.
A group triggers causally at the arrival of the first row whose excursion from the first price of the group
reaches jmin ticks (the end of the sweep is unknown then). Returns are signed by the taker side and start
from the bookTicker mid visible locally at t* + d.
'''
import time

import numpy as np
import pandas as pd

US = 1000
MS = 1_000_000
EPS = 1e-9
JMINS = (1, 2, 4, 8)
DELAYS_MS = (0, 1, 5, 10)
RACE_MS = (0, 1, 5, 10)
HORIZONS = (('1ms', MS), ('10ms', 10 * MS), ('100ms', 100 * MS), ('1s', 1000 * MS), ('5s', 5000 * MS),
            ('60s', 60000 * MS))
PRE_NS = 10 * MS
TGT_PRE_NS = 5 * MS
PLACEBO_NS = 60000 * MS
NOTIONAL_EDGES = (0.0, 1e3, 5e3, 2e4, 1e5, 1e12)
J_EDGES = (0, 1, 2, 4, 8, 16, 10 ** 9)
LAT_Q = (0.001, 0.01, 0.5, 0.99, 0.999)


def quantiles(x, qs=(0.05, 0.25, 0.5, 0.75, 0.95)):
    x = np.asarray(x, np.float64)
    x = x[np.isfinite(x)]
    if not x.size:
        return None
    return {f'p{q * 100:g}': float(np.quantile(x, q)) for q in qs}


def asof(ts, v, t):
    out = np.full(len(t), np.nan)
    if not len(ts):
        return out
    i = np.searchsorted(ts, t, side='right') - 1
    ok = i >= 0
    out[ok] = v[i[ok]]
    return out


def ret_stats(ticks, bp=None):
    ok = np.isfinite(ticks)
    t = ticks[ok]
    if not t.size:
        return None
    out = {'n': int(t.size), 'mean_ticks': float(t.mean()), 'median_ticks': float(np.median(t)),
           'se_ticks': float(t.std(ddof=1) / np.sqrt(t.size)) if t.size > 1 else None,
           'p_cont': float(np.mean(t > EPS)), 'p_rev': float(np.mean(t < -EPS))}
    if bp is not None:
        out['mean_bp'] = float(bp[ok].mean())
    return out


def signed_return(ts, mid, t0, h, sg, tick):
    m0 = asof(ts, mid, t0)
    m1 = asof(ts, mid, t0 + h)
    if len(ts):
        m1[(t0 + h) > ts[-1]] = np.nan
    d = sg * (m1 - m0)
    return d / tick, d / m0 * 1e4


def latency_stats(loc, exch):
    lat = (loc - exch) / MS
    out = {'rows': int(len(lat))}
    if not len(lat):
        return out
    out.update({f'p{q * 100:g}': float(np.quantile(lat, q)) for q in LAT_Q})
    out.update({'min': float(lat.min()), 'max': float(lat.max()), 'mean': float(lat.mean()),
                'frac_negative': float(np.mean(lat < 0)), 'exch_ms_aligned_frac': float(np.mean(exch % MS == 0))})
    return out


def sweep_groups(exch, tid, sg, pt):
    n = len(pt)
    new = np.ones(n, np.bool_)
    if n > 1:
        cont = ((exch[1:] == exch[:-1]) & (sg[1:] == sg[:-1]) & (sg[1:] != 0)
                & (tid[1:] == tid[:-1] + 1) & (sg[1:] * (pt[1:] - pt[:-1]) >= 0))
        new[1:] = ~cont
    starts = np.flatnonzero(new)
    gid = np.cumsum(new) - 1
    exc = sg * (pt - pt[starts][gid]) if n else np.zeros(0, np.int64)
    return gid, starts, exc


def first_crossings(gid, exc, jmin):
    idx = np.flatnonzero(exc >= jmin)
    if not idx.size:
        return idx
    g = gid[idx]
    return idx[np.r_[True, g[1:] != g[:-1]]]


def read_trades(path, tick):
    df = pd.read_csv(path, usecols=['timestamp', 'local_timestamp', 'id', 'side', 'price', 'amount'])
    loc = df['local_timestamp'].to_numpy(np.int64) * US
    exch = df['timestamp'].to_numpy(np.int64) * US
    tid = pd.to_numeric(df['id'], errors='coerce').fillna(-1).to_numpy(np.int64)
    side = df['side'].to_numpy()
    sg = np.where(side == 'buy', 1, np.where(side == 'sell', -1, 0)).astype(np.int64)
    px = df['price'].to_numpy(np.float64)
    qty = df['amount'].to_numpy(np.float64)
    del df
    pt = np.rint(px / tick).astype(np.int64)
    gid, starts, exc = sweep_groups(exch, tid, sg, pt)
    up = np.unique(pt)
    return {'loc': loc, 'exch': exch, 'sg': sg, 'px': px, 'pt': pt, 'gid': gid, 'starts': starts, 'exc': exc,
            'cum_notional': np.r_[0.0, np.cumsum(px * qty)],
            'checks': {'rows': int(len(px)), 'local_not_sorted_rows': int(np.count_nonzero(np.diff(loc) < 0)),
                       'id_not_increasing_rows': int(np.count_nonzero(np.diff(tid) <= 0)),
                       'unknown_side_rows': int(np.count_nonzero(sg == 0)),
                       'min_price_step_ticks': int(np.diff(up).min()) if up.size > 1 else None,
                       'off_grid_rows': int(np.count_nonzero(np.abs(px / tick - pt) > 1e-6))}}


def read_bbo(path):
    df = pd.read_csv(path, usecols=['timestamp', 'local_timestamp', 'bid_price', 'ask_price'])
    bid = df['bid_price'].to_numpy(np.float64)
    ask = df['ask_price'].to_numpy(np.float64)
    ok = np.isfinite(bid) & np.isfinite(ask) & (bid > 0) & (ask > bid)
    loc = df['local_timestamp'].to_numpy(np.int64)[ok] * US
    exch = df['timestamp'].to_numpy(np.int64)[ok] * US
    bid = bid[ok]
    ask = ask[ok]
    del df
    unsorted = int(np.count_nonzero(np.diff(loc) < 0))
    if unsorted:
        o = np.argsort(loc, kind='stable')
        loc, exch, bid, ask = loc[o], exch[o], bid[o], ask[o]
    lat = latency_stats(loc, exch)
    keep = np.r_[loc[1:] != loc[:-1], True] if len(loc) else np.zeros(0, np.bool_)
    return {'ts': loc[keep], 'mid': 0.5 * (bid[keep] + ask[keep]), 'bid': bid[keep], 'ask': ask[keep],
            'latency_ms': lat, 'local_not_sorted_rows': unsorted}


def group_summary(tr, hours):
    starts = tr['starts']
    n = len(tr['pt'])
    if not n:
        return {'trades': 0}
    ends = np.r_[starts[1:], n] - 1
    size = ends - starts + 1
    jf = tr['exc'][ends]
    multi = size >= 2
    return {'trades': int(n), 'groups': int(len(starts)), 'multi_row_groups': int(multi.sum()),
            'rows_in_multi_row_groups': int(size[multi].sum()),
            'groups_by_final_excursion_ticks': [
                {'range': [lo, hi], 'groups': int(((jf >= lo) & (jf < hi)).sum()),
                 'per_hour': float(((jf >= lo) & (jf < hi)).sum() / hours) if hours > 0 else None}
                for lo, hi in zip(J_EDGES[:-1], J_EDGES[1:])],
            'rows_per_group_multi': quantiles(size[multi]),
            'arrival_spread_ms_multi': quantiles((tr['loc'][ends] - tr['loc'][starts])[multi] / MS)}


def sweep_events(tr, jmin):
    idx = first_crossings(tr['gid'], tr['exc'], jmin)
    st = tr['starts'][tr['gid'][idx]]
    return {'t_star': tr['loc'][idx], 't_first': tr['loc'][st], 'sg': tr['sg'][idx].astype(np.float64),
            'px': tr['px'][idx], 'notional': tr['cum_notional'][idx + 1] - tr['cum_notional'][st]}


def _horizon_table(ts, mid, t0, sg, tick, mask=None):
    out = {}
    for lab, h in HORIZONS:
        r_t, r_bp = signed_return(ts, mid, t0, h, sg, tick)
        if mask is not None:
            r_t, r_bp = r_t[mask], r_bp[mask]
        out[lab] = ret_stats(r_t, r_bp)
    return out


def event_block(ev, tgt, tick, race):
    ts, mid = tgt['ts'], tgt['mid']
    t_star, sg = ev['t_star'], ev['sg']
    out = {}
    m_now = asof(ts, mid, t_star)
    out['pre_move_ticks'] = ret_stats(sg * (m_now - asof(ts, mid, ev['t_first'] - PRE_NS)) / tick)
    out['delay'] = {f'd{d}ms': _horizon_table(ts, mid, t_star + d * MS, sg, tick) for d in DELAYS_MS}
    out['placebo_minus_60s'] = _horizon_table(ts, mid, t_star - PLACEBO_NS, sg, tick)
    out['by_notional'] = [{'range': [lo, hi], **_horizon_table(ts, mid, t_star, sg, tick,
                                                              (ev['notional'] >= lo) & (ev['notional'] < hi))}
                          for lo, hi in zip(NOTIONAL_EDGES[:-1], NOTIONAL_EDGES[1:])]
    tp = sg * (m_now - asof(ts, mid, t_star - TGT_PRE_NS)) / tick
    out['by_target_pre_5ms'] = {name: {'n': int(m.sum()), **_horizon_table(ts, mid, t_star, sg, tick, m)}
                                for name, m in (('against', tp < -EPS), ('flat', np.abs(tp) <= EPS),
                                                ('with', tp > EPS))}
    if race:
        out['race_touch_reflected'] = {}
        for off in RACE_MS:
            tt = t_star + off * MS
            touch = np.where(sg > 0, asof(ts, tgt['ask'], tt), asof(ts, tgt['bid'], tt))
            ok = np.isfinite(touch)
            refl = sg[ok] * (touch[ok] - ev['px'][ok]) / tick >= -0.5
            out['race_touch_reflected'][f'{off}ms'] = float(refl.mean()) if refl.size else None
    return out


def run_sweep_study(target_trades, signal_trades, target_bbo, signal_bbo, tick, names=('ETHUSDC', 'ETHUSDT')):
    t_start = time.time()
    trades = {names[0]: read_trades(target_trades, tick), names[1]: read_trades(signal_trades, tick)}
    bbo = {names[0]: read_bbo(target_bbo), names[1]: read_bbo(signal_bbo)}
    loc0 = trades[names[0]]['loc']
    hours = (int(loc0.max()) - int(loc0.min())) / 3.6e12 if len(loc0) else 0.0
    rep = {'config': {'tick': tick, 'jmin_ticks': list(JMINS), 'delays_ms': list(DELAYS_MS),
                      'horizons': [lab for lab, _ in HORIZONS], 'pre_move_ns': PRE_NS,
                      'target_pre_ns': TGT_PRE_NS, 'placebo_shift_ns': -PLACEBO_NS,
                      'notional_edges': list(NOTIONAL_EDGES), 'race_ms': list(RACE_MS),
                      'group': 'consecutive id, same exchange ts, same taker side, monotone price; arrival order',
                      'trigger': 'local arrival of the first row with excursion >= jmin ticks',
                      'mid': 'bookTicker mid, last row per local timestamp'},
           'hours': hours, 'checks': {}, 'latency_ms': {}, 'groups': {}, 'events': {}}
    for nm in names:
        rep['checks'][f'{nm}_trades'] = trades[nm]['checks']
        rep['checks'][f'{nm}_book_ticker_local_not_sorted_rows'] = bbo[nm]['local_not_sorted_rows']
        rep['latency_ms'][f'{nm}_trades'] = latency_stats(trades[nm]['loc'], trades[nm]['exch'])
        rep['latency_ms'][f'{nm}_book_ticker'] = bbo[nm]['latency_ms']
        rep['groups'][nm] = group_summary(trades[nm], hours)
    for src in names:
        rep['events'][src] = {}
        for j in JMINS:
            ev = sweep_events(trades[src], j)
            n = int(len(ev['t_star']))
            rep['events'][src][f'j{j}'] = {
                'n': n, 'per_hour': n / hours if hours > 0 else None,
                'notional_to_trigger': quantiles(ev['notional']),
                'to': {tgt: event_block(ev, bbo[tgt], tick, tgt == src) for tgt in names}}
    rep['runtime_s'] = round(time.time() - t_start, 1)
    return rep
