'''Worth-it check for sweep defence on the baseline GLFT fills (no strategy change).

Sweep events come from sweep_study (same exchange ms, consecutive trade ids, same taker side, monotone price;
trigger at the local arrival of the first row whose excursion reaches J ticks). A baseline fill at exchange time
T on side s (+1: our bid was filled) is adverse-actionable at window W when a sweep with taker side -s has
t* + latency <= T and T - t* <= W: a cancel sent at t* would reach the exchange before the fill under the
backtest's constant order latency (same clock convention as the replay). 'late_only' fills have an adverse
sweep only with T - latency < t* <= T + LATE_NS, so reacting to it cannot avoid the fill. Buckets are exclusive:
adverse_actionable, late_only, favorable_only, none. Markouts use the 100ms mid samples like markout_table.
'''
import numpy as np

from sweep_study import read_trades, sweep_events

MS = 1_000_000
JMINS = (1, 4)
WINDOWS_MS = (100, 1000)
LATE_NS = 10 * MS
HORIZONS = (('100ms', 1), ('1s', 10), ('5s', 50), ('60s', 600))


def sweep_event_arrays(path, tick, jmins=JMINS):
    tr = read_trades(path, tick)
    out = {}
    for j in jmins:
        ev = sweep_events(tr, j)
        order = np.argsort(ev['t_star'], kind='stable')
        out[j] = {'t_star': ev['t_star'][order].astype(np.int64), 'sg': ev['sg'][order].astype(np.float64)}
    return out


def merge_events(a, b):
    t = np.concatenate([a['t_star'], b['t_star']])
    sg = np.concatenate([a['sg'], b['sg']])
    order = np.argsort(t, kind='stable')
    return {'t_star': t[order], 'sg': sg[order]}


def fill_mids(f, mid, steps, t0, step_ns):
    first = np.maximum(np.ceil((f[:, 0] - t0) / step_ns).astype(np.int64) - 1, 0)
    out = {}
    for lab, h in HORIZONS:
        idx = first + h
        ok = idx < steps
        m = np.full(len(f), np.nan)
        m[ok] = mid[idx[ok]]
        out[lab] = m
    return out


def bucket(mask, f, mids):
    res = {'n': int(mask.sum()), 'share': float(mask.mean()) if len(mask) else None}
    if not mask.any():
        return res
    px, sd, q = f[mask, 2], f[mask, 1], f[mask, 3]
    for lab, m in mids.items():
        d = sd * (m[mask] - px)
        ok = np.isfinite(d)
        res[lab] = {'mean_bp': float(np.mean(d[ok] / px[ok] * 1e4)) if ok.any() else None,
                    'sum_usdc': float(np.sum(d[ok] * q[ok]))}
    return res


def blocked_fraction(t, w_ns, lo, hi):
    t = t[(t >= lo) & (t <= hi)]
    span = hi - lo
    if not len(t) or span <= 0:
        return 0.0
    return float((np.minimum(np.diff(t), w_ns).sum() + min(w_ns, hi - t[-1])) / span)


def ages(t_ev, T, lag_ns):
    i = np.searchsorted(t_ev, T - lag_ns, side='right') - 1
    out = np.full(len(T), np.inf)
    ok = i >= 0
    out[ok] = (T[ok] - t_ev[i[ok]]).astype(np.float64)
    return out


def any_in(t_ev, lo, hi):
    return np.searchsorted(t_ev, hi, side='right') > np.searchsorted(t_ev, lo, side='right')


def classify(T, s, ev, latency_ns):
    t, sg = ev['t_star'], ev['sg']
    tb, tsl = t[sg > 0], t[sg < 0]
    buy = s > 0
    adv_age = np.where(buy, ages(tsl, T, latency_ns), ages(tb, T, latency_ns))
    fav_age = np.where(buy, ages(tb, T, latency_ns), ages(tsl, T, latency_ns))
    late = np.where(buy, any_in(tsl, T - latency_ns, T + LATE_NS), any_in(tb, T - latency_ns, T + LATE_NS))
    return adv_age, fav_age, late


def fill_sweep_check(f, events, latency_ns, mid, steps, t0, step_ns):
    out = {'config': {'windows_ms': list(WINDOWS_MS), 'late_ns': LATE_NS, 'latency_ns': int(latency_ns),
                      'horizons': [lab for lab, _ in HORIZONS],
                      'rule': 'adverse: taker side opposite our fill side, t* + latency <= T, T - t* <= W'},
           'fills': int(len(f))}
    if not len(f):
        return out
    T = f[:, 0].astype(np.int64)
    s = f[:, 1]
    lo, hi = int(T.min()), int(T.max())
    mids = fill_mids(f, mid, steps, t0, step_ns)
    out['fill_time_ns'] = [lo, hi]
    out['all'] = bucket(np.ones(len(f), dtype=bool), f, mids)
    out['by_source'] = {}
    for key, ev in events.items():
        adv_age, fav_age, late = classify(T, s, ev, latency_ns)
        t, sg = ev['t_star'], ev['sg']
        blk = {'events_in_fill_span': int(np.count_nonzero((t >= lo) & (t <= hi)))}
        for w in WINDOWS_MS:
            wn = w * MS
            adv = adv_age <= wn
            late_only = late & ~adv
            fav = (fav_age <= wn) & ~adv & ~late_only
            none = ~adv & ~late_only & ~fav
            blk[f'{w}ms'] = {'adverse_actionable': bucket(adv, f, mids), 'late_only': bucket(late_only, f, mids),
                             'favorable_only': bucket(fav, f, mids), 'none': bucket(none, f, mids),
                             'blocked_fraction': {'after_buy_sweeps': blocked_fraction(t[sg > 0], wn, lo, hi),
                                                  'after_sell_sweeps': blocked_fraction(t[sg < 0], wn, lo, hi)}}
        out['by_source'][key] = blk
    return out
