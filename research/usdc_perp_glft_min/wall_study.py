'''Wall study on Binance USDC-M ETHUSDC: virtual orders one tick in front of large resting levels,
with fixed-offset quotes as the control. Measurement only; the GLFT models are unchanged.

Data: Tardis incremental_book_L2 and trades in local_timestamp order, trades before book rows at the
same timestamp. Virtual orders are 1 lot, have no market impact and carry no inventory limit.
Queue: an order joins behind the visible level size; trades at its price consume the queue and a
smaller level size caps it (cancels are assumed to be ahead). A trade through the price fills it.
Latency: every place, cancel or replace takes effect latency_ns after the decision; the old order
stays fillable until then. Decisions use the book after the last row of a timestamp.
Wall: the nearest level within DMAX ticks of mid on that side with size >= threshold; thresholds are
quantiles of level sizes within DMAX ticks of mid, sampled once per second.
Cancel rules (frac): -1 hold until fill or STALE; 0 cancel when the wall level is empty;
0.3 / 0.5 cancel when the wall shrinks to that fraction of its size at placement.
'''
import math
import time

import numpy as np
import pandas as pd
from numba import njit

NS = 1_000_000_000
DMAX = 50.0
STALE = 60.0
SAMPLE_NS = NS
HORIZONS = (('100ms', NS // 10), ('1s', NS), ('5s', 5 * NS), ('60s', 60 * NS))
BREACH_NS = 5 * NS
QUANTILES = (0.90, 0.95, 0.99)
FRACS = (-1.0, 0.0, 0.3, 0.5)
FIXED_D = (1, 2, 4, 8, 12, 16, 24, 32)
FILL_COLS = ('t_rel', 'side', 'px', 'mid', 'dist', 'imb_side', 'w0', 'wall_dist0', 'queue0', 'wall_left', 'age_ns')
COUNT_NAMES = ('placed', 'post_only_rejected', 'cancel_shrink', 'cancel_stale', 'replaces', 'fills')
DIST_EDGES = (-1e9, 1, 2, 4, 8, 16, 32, 64, 1e9)
IMB_EDGES = (-1.01, -0.5, 0.0, 0.5, 1.01)
SIZE_EDGES = (1, 2, 4, 8, 1e9)
WALL_DIST_EDGES = (0, 2, 5, 10, 20, 51)
QUEUE_EDGES = (0, 1e-12, 1e9)
CAP_FILLS = 3_000_000
CAP_CANCELS = 3_000_000
CAP_SERIES = 8_000_000
CAP_SAMPLES = 15_000_000


@njit(cache=False)
def _apply_l2(bidq, askq, bb, ba, s, p, q, n):
    if s > 0:
        bidq[p] = q
        if q > 0.0:
            if p > bb:
                bb = p
        elif p == bb:
            j = p - 1
            while j >= 0 and bidq[j] <= 0.0:
                j -= 1
            bb = j
    else:
        askq[p] = q
        if q > 0.0:
            if p < ba:
                ba = p
        elif p == ba:
            j = p + 1
            while j < n and askq[j] <= 0.0:
                j += 1
            ba = j
    return bb, ba


@njit(cache=False)
def book_series(ts, kind, side, px, qty, snap, n, sample_ns, dmax):
    bidq = np.zeros(n)
    askq = np.zeros(n)
    bb = -1
    ba = n
    prev_snap = False
    mid_ts = np.empty(CAP_SERIES, np.int64)
    mid_v = np.empty(CAP_SERIES)
    bbo_ts = np.empty(CAP_SERIES, np.int64)
    bbv = np.empty(CAP_SERIES, np.int64)
    bav = np.empty(CAP_SERIES, np.int64)
    samples = np.empty(CAP_SAMPLES)
    nm = 0
    nb = 0
    ns = 0
    over = 0
    last_mid = -1.0
    last_bb = -2
    last_ba = -2
    next_sample = 0
    m = len(ts)
    for i in range(m):
        if kind[i] == 0:
            if snap[i] and not prev_snap:
                bidq[:] = 0.0
                askq[:] = 0.0
                bb = -1
                ba = n
            prev_snap = snap[i]
            p = px[i]
            if p >= 0 and p < n:
                bb, ba = _apply_l2(bidq, askq, bb, ba, side[i], p, qty[i], n)
        if i == m - 1 or ts[i + 1] != ts[i]:
            if bb >= 0 and ba < n and bb < ba:
                mid = 0.5 * (bb + ba)
                if mid != last_mid:
                    if nm < CAP_SERIES:
                        mid_ts[nm] = ts[i]
                        mid_v[nm] = mid
                        nm += 1
                    else:
                        over |= 1
                    last_mid = mid
                if bb != last_bb or ba != last_ba:
                    if nb < CAP_SERIES:
                        bbo_ts[nb] = ts[i]
                        bbv[nb] = bb
                        bav[nb] = ba
                        nb += 1
                    else:
                        over |= 2
                    last_bb = bb
                    last_ba = ba
                if ts[i] >= next_sample:
                    lim = int(math.ceil(mid - dmax))
                    j = bb
                    while j >= lim and j >= 0:
                        if bidq[j] > 0.0:
                            if ns < CAP_SAMPLES:
                                samples[ns] = bidq[j]
                                ns += 1
                            else:
                                over |= 4
                        j -= 1
                    lim = int(math.floor(mid + dmax))
                    j = ba
                    while j <= lim and j < n:
                        if askq[j] > 0.0:
                            if ns < CAP_SAMPLES:
                                samples[ns] = askq[j]
                                ns += 1
                            else:
                                over |= 4
                        j += 1
                    next_sample = (ts[i] // sample_ns + 1) * sample_ns
    return mid_ts[:nm], mid_v[:nm], bbo_ts[:nb], bbv[:nb], bav[:nb], samples[:ns], over


@njit(cache=False)
def virtual_orders(ts, kind, side, px, qty, snap, n, mode, dist, thr, frac, latency, dmax, stale, fills, cancels):
    '''mode 0: fixed offset dist from mid, replace when the target tick changes; mode 1: wall front.'''
    bidq = np.zeros(n)
    askq = np.zeros(n)
    bb = -1
    ba = n
    prev_snap = False
    live = np.zeros(2, np.bool_)
    opx = np.zeros(2, np.int64)
    qa = np.zeros(2)
    placed = np.zeros(2, np.int64)
    wpx = np.full(2, -1, np.int64)
    w0 = np.zeros(2)
    wd0 = np.zeros(2)
    q0 = np.zeros(2)
    pend = np.zeros(2, np.int64)
    pts = np.zeros(2, np.int64)
    ppx = np.zeros(2, np.int64)
    pwpx = np.full(2, -1, np.int64)
    pw0 = np.zeros(2)
    pwd0 = np.zeros(2)
    cnt = np.zeros(6, np.int64)
    last_mid = math.nan
    last_imb = 0.0
    nf = 0
    nc = 0
    m = len(ts)
    t_first = ts[0]
    for i in range(m):
        t = ts[i]
        for s in range(2):
            if pend[s] != 0 and t >= pts[s]:
                act = pend[s]
                pend[s] = 0
                if act >= 2:
                    live[s] = False
                if act == 1 or act == 3:
                    p = ppx[s]
                    if (s == 0 and p < ba) or (s == 1 and p > bb):
                        live[s] = True
                        opx[s] = p
                        if s == 0:
                            qa[s] = bidq[p]
                        else:
                            qa[s] = askq[p]
                        q0[s] = qa[s]
                        placed[s] = pts[s]
                        wpx[s] = pwpx[s]
                        w0[s] = pw0[s]
                        wd0[s] = pwd0[s]
                        cnt[0] += 1
                    else:
                        cnt[1] += 1
        p = px[i]
        if kind[i] == 0:
            if snap[i] and not prev_snap:
                bidq[:] = 0.0
                askq[:] = 0.0
                bb = -1
                ba = n
            prev_snap = snap[i]
            if p >= 0 and p < n:
                q = qty[i]
                bb, ba = _apply_l2(bidq, askq, bb, ba, side[i], p, q, n)
                s = 0 if side[i] > 0 else 1
                if live[s] and opx[s] == p and q < qa[s]:
                    qa[s] = q
        elif side[i] != 0:
            s = 1 if side[i] > 0 else 0
            if live[s]:
                hit = False
                if (s == 0 and p < opx[s]) or (s == 1 and p > opx[s]):
                    hit = True
                elif p == opx[s]:
                    qa[s] -= qty[i]
                    if qa[s] < 0.0:
                        hit = True
                if hit:
                    sg = 1.0 if s == 0 else -1.0
                    if nf < fills.shape[0]:
                        fills[nf, 0] = t - t_first
                        fills[nf, 1] = sg
                        fills[nf, 2] = opx[s]
                        fills[nf, 3] = last_mid
                        fills[nf, 4] = sg * (last_mid - opx[s])
                        fills[nf, 5] = sg * last_imb
                        fills[nf, 6] = w0[s]
                        fills[nf, 7] = wd0[s]
                        fills[nf, 8] = q0[s]
                        wl = math.nan
                        if wpx[s] >= 0 and w0[s] > 0.0:
                            if s == 0:
                                wl = bidq[wpx[s]] / w0[s]
                            else:
                                wl = askq[wpx[s]] / w0[s]
                        fills[nf, 9] = wl
                        fills[nf, 10] = t - placed[s]
                    nf += 1
                    cnt[5] += 1
                    live[s] = False
                    if pend[s] == 3:
                        pend[s] = 1
                    elif pend[s] == 2:
                        pend[s] = 0
        if i == m - 1 or ts[i + 1] != t:
            if bb >= 0 and ba < n and bb < ba:
                mid = 0.5 * (bb + ba)
                last_mid = mid
                tot = bidq[bb] + askq[ba]
                last_imb = (bidq[bb] - askq[ba]) / tot if tot > 0.0 else 0.0
                for s in range(2):
                    if pend[s] != 0:
                        continue
                    if mode == 0:
                        if s == 0:
                            tgt = int(math.floor(mid - dist))
                            if tgt > ba - 1:
                                tgt = ba - 1
                        else:
                            tgt = int(math.ceil(mid + dist))
                            if tgt < bb + 1:
                                tgt = bb + 1
                        if tgt < 0 or tgt >= n:
                            continue
                        if not live[s]:
                            pend[s] = 1
                        elif opx[s] != tgt:
                            pend[s] = 3
                            cnt[4] += 1
                        else:
                            continue
                        pts[s] = t + latency
                        ppx[s] = tgt
                        pwpx[s] = -1
                        pw0[s] = 0.0
                        pwd0[s] = 0.0
                    elif live[s]:
                        wp = wpx[s]
                        wq = bidq[wp] if s == 0 else askq[wp]
                        d_now = mid - opx[s] if s == 0 else opx[s] - mid
                        reason = 0
                        if frac >= 0.0 and wq <= frac * w0[s]:
                            reason = 1
                        elif d_now > stale:
                            reason = 2
                        if reason > 0:
                            pend[s] = 2
                            pts[s] = t + latency
                            cnt[1 + reason] += 1
                            if nc < cancels.shape[0]:
                                cancels[nc, 0] = t - t_first
                                cancels[nc, 1] = 1.0 if s == 0 else -1.0
                                cancels[nc, 2] = wp
                                cancels[nc, 3] = reason
                            nc += 1
                    elif s == 0:
                        lim = int(math.ceil(mid - dmax))
                        j = bb
                        while j >= lim and j >= 0:
                            if bidq[j] >= thr and j + 1 < ba:
                                pend[s] = 1
                                pts[s] = t + latency
                                ppx[s] = j + 1
                                pwpx[s] = j
                                pw0[s] = bidq[j]
                                pwd0[s] = mid - j
                                break
                            j -= 1
                    else:
                        lim = int(math.floor(mid + dmax))
                        j = ba
                        while j <= lim and j < n:
                            if askq[j] >= thr and j - 1 > bb:
                                pend[s] = 1
                                pts[s] = t + latency
                                ppx[s] = j - 1
                                pwpx[s] = j
                                pw0[s] = askq[j]
                                pwd0[s] = j - mid
                                break
                            j += 1
    return nf, nc, cnt


@njit(cache=False)
def breach_flags(t, sg, wall, bbo_ts, bbv, bav, h):
    '''True when the best bid (buy) / ask (sell) moves beyond the wall level within (t - last change, t + h].'''
    out = np.zeros(len(t), np.bool_)
    for k in range(len(t)):
        i0 = np.searchsorted(bbo_ts, t[k], side='right') - 1
        if i0 < 0:
            i0 = 0
        i1 = np.searchsorted(bbo_ts, t[k] + h, side='right')
        for j in range(i0, i1):
            if (sg[k] > 0 and bbv[j] < wall[k]) or (sg[k] < 0 and bav[j] > wall[k]):
                out[k] = True
                break
    return out


def load_events(l2_path, trades_path, tick, lo, hi):
    base = int(math.floor(lo * 0.97 / tick))
    n = int(math.ceil(hi * 1.03 / tick)) - base + 1
    tr = pd.read_csv(trades_path, usecols=['local_timestamp', 'side', 'price', 'amount'])
    tside = tr['side'].to_numpy()
    tr_side = np.where(tside == 'buy', 1, np.where(tside == 'sell', -1, 0)).astype(np.int8)
    l2 = pd.read_csv(l2_path, usecols=['local_timestamp', 'is_snapshot', 'side', 'price', 'amount'])
    snap_col = l2['is_snapshot']
    if snap_col.dtype == bool:
        l2_snap = snap_col.to_numpy(np.bool_)
    else:
        l2_snap = snap_col.astype(str).str.lower().eq('true').to_numpy()
    l2_side = np.where(l2['side'].to_numpy() == 'bid', 1, -1).astype(np.int8)
    ts = np.concatenate([tr['local_timestamp'].to_numpy(np.int64), l2['local_timestamp'].to_numpy(np.int64)]) * 1000
    kind = np.concatenate([np.ones(len(tr), np.int8), np.zeros(len(l2), np.int8)])
    sides = np.concatenate([tr_side, l2_side])
    prices = np.concatenate([tr['price'].to_numpy(np.float64), l2['price'].to_numpy(np.float64)])
    px = np.rint(prices / tick).astype(np.int64) - base
    qty = np.concatenate([tr['amount'].to_numpy(np.float64), l2['amount'].to_numpy(np.float64)])
    snap = np.concatenate([np.zeros(len(tr), np.bool_), l2_snap])
    n_tr, n_l2 = len(tr), len(l2)
    del tr, l2, prices
    order = np.argsort(ts, kind='stable')
    ev = (ts[order], kind[order], sides[order], px[order], qty[order], snap[order])
    return ev, base, n, n_tr, n_l2


def _markout(f, ts0, end_ts, base, tick, qty_eth, mid_ts, mid_v):
    out = {'n': int(len(f))}
    if len(f) == 0 or len(mid_ts) == 0:
        return out
    t = f[:, 0].astype(np.int64) + ts0
    pxa = f[:, 2] + base
    for label, h in HORIZONS:
        idx = np.searchsorted(mid_ts, t + h, side='right') - 1
        ok = (t + h <= end_ts) & (idx >= 0)
        if not ok.any():
            out[label] = None
            continue
        v = f[ok, 1] * (mid_v[idx[ok]] + base - pxa[ok]) / pxa[ok] * 1e4
        out[label] = {'n': int(ok.sum()), 'mean_bp': float(v.mean()), 'median_bp': float(np.median(v)),
                      'pnl_usdc': float(np.sum(v * 1e-4 * qty_eth * pxa[ok] * tick))}
    return out


def _buckets(x, edges, f, mk):
    return [{'range': [lo_, hi_], **mk(f[(x >= lo_) & (x < hi_)])} for lo_, hi_ in zip(edges[:-1], edges[1:])]


def _wall_summary(f, c, cnt, nf, nc, thr, hours, mk, ts0, bbo):
    bbo_ts, bbv, bav = bbo
    t = f[:, 0].astype(np.int64) + ts0
    wall = (f[:, 2] - f[:, 1]).astype(np.int64)
    br = breach_flags(t, f[:, 1].copy(), wall, bbo_ts, bbv, bav, BREACH_NS)
    out = {'threshold_eth': thr, 'counts': {k: int(v) for k, v in zip(COUNT_NAMES, cnt)},
           'fill_overflow': bool(nf > len(f)), 'cancel_overflow': bool(nc > len(c)),
           'fills_per_hour': nf / hours if hours > 0 else None,
           'markout': mk(f),
           'breach_5s_rate': float(br.mean()) if len(br) else None,
           'markout_hold': mk(f[~br]), 'markout_breach': mk(f[br]),
           'by_dist': _buckets(f[:, 4], DIST_EDGES, f, mk),
           'by_imbalance': _buckets(f[:, 5], IMB_EDGES, f, mk),
           'by_wall_size': _buckets(f[:, 6] / thr, SIZE_EDGES, f, mk),
           'by_wall_dist': _buckets(f[:, 7], WALL_DIST_EDGES, f, mk),
           'by_queue0': _buckets(f[:, 8], QUEUE_EDGES, f, mk),
           'cancels': {}}
    for r, name in ((1, 'shrink'), (2, 'stale')):
        sel = c[:, 3] == r
        if sel.any():
            tc = c[sel, 0].astype(np.int64) + ts0
            bc = breach_flags(tc, c[sel, 1].copy(), c[sel, 2].astype(np.int64), bbo_ts, bbv, bav, BREACH_NS)
            out['cancels'][name] = {'n': int(sel.sum()), 'breach_5s_after_rate': float(bc.mean())}
        else:
            out['cancels'][name] = {'n': 0}
    return out


def run_wall_study(l2_path, trades_path, tick, lo, hi, latency_ns, order_qty):
    t_start = time.time()
    ev, base, n, n_tr, n_l2 = load_events(l2_path, trades_path, tick, lo, hi)
    ts = ev[0]
    ts0 = int(ts[0])
    end_ts = int(ts[-1])
    hours = (end_ts - ts0) / NS / 3600.0
    mid_ts, mid_v, bbo_ts, bbv, bav, samples, over = book_series(*ev, n, SAMPLE_NS, DMAX)
    thresholds = {f'q{q:g}': float(np.quantile(samples, q)) for q in QUANTILES}
    rep = {'config': {'dmax_ticks': DMAX, 'stale_ticks': STALE, 'latency_ns': int(latency_ns),
                      'quantiles': list(QUANTILES), 'fracs': list(FRACS), 'fixed_d_ticks': list(FIXED_D),
                      'breach_ns': BREACH_NS, 'order_qty_eth': order_qty,
                      'queue': 'join behind visible size; trades at price consume; size drop caps the queue',
                      'clock': 'local_timestamp; trades before book rows at equal timestamps'},
           'rows': {'trades': int(n_tr), 'incremental_book_L2': int(n_l2)}, 'levels': int(n), 'base_tick': base,
           'hours': hours, 'mid_changes': int(len(mid_ts)), 'bbo_changes': int(len(bbo_ts)),
           'series_overflow': int(over), 'level_samples': int(len(samples)),
           'level_size_eth': {f'p{q * 100:g}': float(np.quantile(samples, q)) for q in (0.5, 0.9, 0.95, 0.99, 0.999)},
           'thresholds_eth': thresholds, 'fixed': {}, 'wall': {}}

    def mk(f):
        return _markout(f, ts0, end_ts, base, tick, order_qty, mid_ts, mid_v)

    fills = np.zeros((CAP_FILLS, len(FILL_COLS)))
    cancels = np.zeros((CAP_CANCELS, 4))
    for d in FIXED_D:
        nf, nc, cnt = virtual_orders(*ev, n, 0, float(d), 0.0, -1.0, int(latency_ns), DMAX, STALE, fills, cancels)
        f = fills[:min(nf, CAP_FILLS)]
        rep['fixed'][f'd{d}'] = {'counts': {k: int(v) for k, v in zip(COUNT_NAMES, cnt)},
                                 'fill_overflow': bool(nf > CAP_FILLS),
                                 'fills_per_hour': nf / hours if hours > 0 else None,
                                 'markout': mk(f), 'by_dist': _buckets(f[:, 4], DIST_EDGES, f, mk),
                                 'by_imbalance': _buckets(f[:, 5], IMB_EDGES, f, mk)}
    for qn, thr in thresholds.items():
        for fr in FRACS:
            nf, nc, cnt = virtual_orders(*ev, n, 1, 0.0, thr, fr, int(latency_ns), DMAX, STALE, fills, cancels)
            f = fills[:min(nf, CAP_FILLS)]
            c = cancels[:min(nc, CAP_CANCELS)]
            rep['wall'][f'{qn}_f{fr:g}'] = _wall_summary(f, c, cnt, nf, nc, thr, hours, mk, ts0, (bbo_ts, bbv, bav))
    rep['runtime_s'] = round(time.time() - t_start, 1)
    return rep
