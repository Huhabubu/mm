"""Event study: Binance ETHUSDT (USDT-M) vs ETHUSDC (USDC-M) bookTicker mids; strategy unchanged.

lead_lag_report: for each mid change of one symbol, the other symbol's signed mid move before it,
at the same timestamp and after it, on the exchange clock and on the capture (local) clock.
fill_conditioning: fills bucketed by the prior signed mid move of the signal (exchange clock, and the
actionable clock: visible locally one order latency before the fill) and of the target itself.
Times are ns, mids are prices, moves are in ticks; adverse means against the filled side.
"""
import numpy as np

MS = 1_000_000
HORIZONS_MS = (1, 5, 10, 50, 100, 1000)
PRE_MS = (1, 5, 10, 50)
MARKOUT_KEYS = ('100ms', '1s', '5s', '60s')
EPS = 1e-9


def last_per_timestamp(ts, mid):
    if len(ts) != len(mid) or np.any(np.diff(ts) < 0):
        raise ValueError('timestamps must be sorted and aligned with mids')
    if not len(ts):
        return ts, mid
    keep = np.r_[ts[1:] != ts[:-1], True]
    return ts[keep], mid[keep]


def clocks_from_arrays(local, exch, mid):
    order = np.argsort(exch, kind='stable')
    return {'local': last_per_timestamp(local, mid),
            'exch': last_per_timestamp(exch[order], mid[order]),
            'exch_out_of_order_rows': int(np.count_nonzero(np.diff(exch) < 0))}


def clocks_from_csv(path):
    import pandas as pd
    df = pd.read_csv(path, usecols=['timestamp', 'local_timestamp', 'bid_price', 'ask_price'])
    bid = df['bid_price'].to_numpy(np.float64)
    ask = df['ask_price'].to_numpy(np.float64)
    ok = np.isfinite(bid) & np.isfinite(ask) & (bid > 0) & (ask > bid)
    return clocks_from_arrays(df['local_timestamp'].to_numpy(np.int64)[ok] * 1000,
                              df['timestamp'].to_numpy(np.int64)[ok] * 1000, 0.5 * (bid[ok] + ask[ok]))


def asof(series, t):
    ts, mid = series
    idx = np.searchsorted(ts, t, side='right') - 1
    return np.where(idx >= 0, mid[np.maximum(idx, 0)], np.nan)


def change_events(series):
    ts, mid = series
    d = np.diff(mid)
    nz = d != 0
    return ts[1:][nz], np.sign(d[nz])


def signed_stats(x, tick):
    x = x[np.isfinite(x)] / tick
    if not x.size:
        return None
    return {'n': int(x.size), 'mean_ticks': float(x.mean()), 'p_same': float(np.mean(x > EPS)),
            'p_opp': float(np.mean(x < -EPS))}


def response(src, dst, tick):
    t, s = change_events(src)
    before = asof(dst, t - 1)
    out = {'events': int(t.size), 'same_ts': signed_stats(s * (asof(dst, t) - before), tick), 'pre': {}, 'post': {}}
    for h in HORIZONS_MS:
        out['pre'][f'{h}ms'] = signed_stats(s * (before - asof(dst, t - 1 - h * MS)), tick)
        out['post'][f'{h}ms'] = signed_stats(s * (asof(dst, t + h * MS) - asof(dst, t)), tick)
    return out


def lead_lag_report(sig, tgt, tick):
    rep = {'exch_out_of_order_rows': {'signal': sig['exch_out_of_order_rows'],
                                      'target': tgt['exch_out_of_order_rows']}}
    for clock in ('exch', 'local'):
        rep[clock] = {'signal_to_target': response(sig[clock], tgt[clock], tick),
                      'target_to_signal': response(tgt[clock], sig[clock], tick)}
    return rep


def compact(table):
    return {k: (table[k]['mean_bp'] if table.get(k) else None) for k in MARKOUT_KEYS}


def buckets(pre, fills, markout):
    groups = {'adverse': pre < -EPS, 'none': np.abs(pre) <= EPS, 'favorable': pre > EPS}
    return {g: {'n': int(m.sum()), 'markout_mean_bp': compact(markout(fills[m])) if m.any() else None}
            for g, m in groups.items()}


def fill_conditioning(fills, sig, tgt, tick, latency_ns, markout):
    """fills rows: (exch_ts_ns, side +1/-1, price, qty); markout(sub) returns a markout table."""
    out = {'fills': int(len(fills)), 'latency_ns': int(latency_ns)}
    if not len(fills):
        return out
    ts = fills[:, 0].astype(np.int64)
    side = fills[:, 1]
    views = {'signal_exch': (sig['exch'], ts - 1), 'signal_actionable': (sig['local'], ts - latency_ns - 1),
             'target_exch': (tgt['exch'], ts - 1)}
    for h in PRE_MS:
        pre = {}
        for name, (series, end) in views.items():
            pre[name] = side * (asof(series, end) - asof(series, end - h * MS)) / tick
            out.setdefault(name, {})[f'{h}ms'] = buckets(pre[name], fills, markout)
        joint = np.where(np.abs(pre['target_exch']) <= EPS, pre['signal_actionable'], np.nan)
        out.setdefault('signal_actionable_when_target_flat', {})[f'{h}ms'] = buckets(joint, fills, markout)
    return out
