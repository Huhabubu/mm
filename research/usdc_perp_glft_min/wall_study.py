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
GLFT peak (modes 2/3): a 100ms sample clock on the reconstructed book refits the GLFT half spread h with
glft_core and the origin_glft constants (warmup 6000, window 6000, refit every 50, same gamma); the
virtual order carries no inventory (q = 0). G_bid = floor(mid - h) kept on or behind the best bid, G_ask
symmetric. Mode 3 quotes G (control). Mode 2 walks from G away from mid for PEAK_DEPTH ticks and takes
the first level whose notional (price * size) exceeds both neighbouring levels and whose size >= vmin;
it quotes one tick in front of that level but never better than G, and quotes G when none is found.
Both re-quote whenever the target tick changes (event driven, no throttle).
'''
import math
import time

import numpy as np
import pandas as pd
from numba import njit

from glft_core import fit_intensity_from_counts, glft_coefficients, push_change, push_depth

NS = 1_000_000_000
DMAX = 50.0
STALE = 60.0
SAMPLE_NS = NS
HORIZONS = (('100ms', NS // 10), ('1s', NS), ('5s', 5 * NS), ('60s', 60 * NS))
BREACH_NS = 5 * NS
QUANTILES = (0.90, 0.95, 0.99)
FRACS = (-1.0, 0.0, 0.3, 0.5)
FIXED_D = (1, 2, 4, 8, 12, 16, 24, 32)
FILL_COLS = ('t_rel', 'side', 'px', 'mid', 'dist', 'imb_side', 'w0', 'wall_dist0', 'queue0', 'wall_left', 'age_ns',
             'glft_gap0', 'wall_px')
COUNT_NAMES = ('placed', 'post_only_rejected', 'cancel_shrink', 'cancel_stale', 'replaces', 'fills',
               'peak_decisions', 'no_peak_decisions')
DIST_EDGES = (-1e9, 1, 2, 4, 8, 16, 32, 64, 1e9)
IMB_EDGES = (-1.01, -0.5, 0.0, 0.5, 1.01)
SIZE_EDGES = (1, 2, 4, 8, 1e9)
WALL_DIST_EDGES = (0, 2, 5, 10, 20, 51)
QUEUE_EDGES = (0, 1e-12, 1e9)
CAP_FILLS = 3_000_000
CAP_CANCELS = 3_000_000
CAP_SERIES = 8_000_000
CAP_SAMPLES = 15_000_000
GLFT_STEP_NS = 100_000_000
GLFT_STEP_S = 0.1
GLFT_WARMUP = 6000
GLFT_WINDOW = 6000
GLFT_MIN_FIT_STEPS = 600
GLFT_REFIT_EVERY = 50
GLFT_GRID = 10
GLFT_MAX_BINS = 1000
GLFT_MIN_COUNT = 10
PEAK_DEPTH = 20
PEAK_VMIN_FIXED = 5.0
GAP_EDGES = (0, 1, 2, 4, 8, 16, 21)
ETH_EDGES = (0, 1e-12, 2, 5, 15, 50, 1e9)


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
def virtual_orders(ts, kind, side, px, qty, snap, n, mode, dist, thr, frac, latency, dmax, stale, fills, cancels,
                   hs_ts, hs_half, base, depth):
    '''mode 0: fixed offset dist from mid, replace when the target tick changes; mode 1: wall front;
    mode 2: GLFT peak (thr = vmin, cancels rows record decisions); mode 3: GLFT quote.'''
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
    pgap = np.zeros(2)
    gap0 = np.zeros(2)
    hp = -1
    cnt = np.zeros(8, np.int64)
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
                        gap0[s] = pgap[s]
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
                        fills[nf, 11] = gap0[s]
                        fills[nf, 12] = wpx[s]
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
                    if mode >= 2:
                        while hp + 1 < hs_ts.shape[0] and hs_ts[hp + 1] <= t:
                            hp += 1
                        if hp < 0 or not math.isfinite(hs_half[hp]):
                            continue
                        hf = hs_half[hp]
                        wj = -1
                        if s == 0:
                            g = int(math.floor(mid - hf))
                            if g > bb:
                                g = bb
                            tgt = g
                            if mode == 2:
                                j = g
                                while j >= g - depth and j >= 1:
                                    vq = bidq[j]
                                    if vq > 0.0 and vq >= thr:
                                        vj = vq * (j + base)
                                        if vj > bidq[j + 1] * (j + 1 + base) and vj > bidq[j - 1] * (j - 1 + base):
                                            wj = j
                                            if j + 1 < g:
                                                tgt = j + 1
                                            break
                                    j -= 1
                        else:
                            g = int(math.ceil(mid + hf))
                            if g < ba:
                                g = ba
                            tgt = g
                            if mode == 2:
                                j = g
                                while j <= g + depth and j < n - 1:
                                    vq = askq[j]
                                    if vq > 0.0 and vq >= thr:
                                        vj = vq * (j + base)
                                        if vj > askq[j + 1] * (j + 1 + base) and vj > askq[j - 1] * (j - 1 + base):
                                            wj = j
                                            if j - 1 > g:
                                                tgt = j - 1
                                            break
                                    j += 1
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
                        pwpx[s] = wj
                        if wj >= 0:
                            pw0[s] = bidq[wj] if s == 0 else askq[wj]
                            pwd0[s] = mid - wj if s == 0 else wj - mid
                            cnt[6] += 1
                        else:
                            pw0[s] = 0.0
                            pwd0[s] = -1.0
                            cnt[7] += 1
                        pgap[s] = float(g - tgt) if s == 0 else float(tgt - g)
                        if nc < cancels.shape[0]:
                            cancels[nc, 0] = t - t_first
                            cancels[nc, 1] = 1.0 if s == 0 else -1.0
                            cancels[nc, 2] = mid - tgt if s == 0 else tgt - mid
                            cancels[nc, 3] = pgap[s]
                        nc += 1
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


@njit(cache=False)
def glft_half_series(ts, kind, side, px, qty, snap, n, gamma):
    '''GLFT half spread in ticks on a 100ms clock from the reconstructed book; NaN until quotable.'''
    m = len(ts)
    t0 = ts[0]
    cap = int((ts[m - 1] - t0) // GLFT_STEP_NS) + 1
    out_ts = np.empty(cap, np.int64)
    out_h = np.full(cap, np.nan)
    bidq = np.zeros(n)
    askq = np.zeros(n)
    bb = -1
    ba = n
    prev_snap = False
    depth_ring = np.zeros(GLFT_WINDOW, np.int64)
    counts = np.zeros(GLFT_MAX_BINS + 1, np.int64)
    chg_ring = np.zeros(GLFT_WINDOW)
    d_idx = 0
    d_fill = 0
    c_idx = 0
    c_fill = 0
    s1 = 0.0
    s2 = 0.0
    prev_mid = np.nan
    a_hat = 0.0
    k_hat = 0.0
    sigma = 0.0
    half = 0.0
    fitted = False
    furthest = 0.0
    next_sample = t0 + GLFT_STEP_NS
    ns = 0
    for i in range(m):
        while ts[i] > next_sample and ns < cap:
            mid = prev_mid
            valid = bb >= 0 and ba < n and bb < ba
            if valid:
                mid = 0.5 * (bb + ba)
            if valid and not math.isnan(prev_mid):
                dj = int(furthest * GLFT_GRID)
                if dj > GLFT_MAX_BINS:
                    dj = GLFT_MAX_BINS
                d_idx, d_fill = push_depth(depth_ring, d_idx, d_fill, counts, dj)
                c_idx, c_fill, s1, s2 = push_change(chg_ring, c_idx, c_fill, s1, s2, mid - prev_mid)
            furthest = 0.0
            prev_mid = mid
            if ns % GLFT_REFIT_EVERY == 0 and d_fill >= GLFT_MIN_FIT_STEPS:
                a, k, ok = fit_intensity_from_counts(counts, d_fill * GLFT_STEP_S, GLFT_MIN_COUNT)
                mean = s1 / c_fill
                var = s2 / c_fill - mean * mean
                sigma = math.sqrt(var / GLFT_STEP_S) if var > 0.0 else 0.0
                if ok:
                    a_hat = a
                    k_hat = k * GLFT_GRID
                    fitted = True
                if fitted and sigma > 0.0:
                    half, _skew = glft_coefficients(a_hat, k_hat, sigma, gamma, 1.0)
            out_ts[ns] = next_sample
            if ns >= GLFT_WARMUP and fitted and sigma > 0.0 and math.isfinite(half) and half < 1e5:
                out_h[ns] = half
            ns += 1
            next_sample += GLFT_STEP_NS
        p = px[i]
        if kind[i] == 0:
            if snap[i] and not prev_snap:
                bidq[:] = 0.0
                askq[:] = 0.0
                bb = -1
                ba = n
            prev_snap = snap[i]
            if p >= 0 and p < n:
                bb, ba = _apply_l2(bidq, askq, bb, ba, side[i], p, qty[i], n)
        elif side[i] != 0 and not math.isnan(prev_mid):
            fp = float(p)
            d = fp - prev_mid if side[i] > 0 else prev_mid - fp
            if d > furthest:
                furthest = d
    return out_ts[:ns], out_h[:ns]


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


def _qs(x):
    if len(x) == 0:
        return None
    return {f'p{p:g}': float(np.quantile(x, p / 100.0)) for p in (5, 25, 50, 75, 95)}


def _glft_summary(f, c, cnt, nf, nc, hours, mk, ts0, bbo):
    bbo_ts, bbv, bav = bbo
    pk = f[:, 12] >= 0
    out = {'counts': {k: int(v) for k, v in zip(COUNT_NAMES, cnt)},
           'fill_overflow': bool(nf > len(f)), 'decision_overflow': bool(nc > len(c)),
           'fills_per_hour': nf / hours if hours > 0 else None,
           'markout': mk(f), 'markout_peak': mk(f[pk]), 'markout_no_peak': mk(f[~pk]),
           'decision_dist_mid_ticks': _qs(c[:, 2]), 'decision_glft_gap_ticks': _qs(c[:, 3]),
           'decision_by_dist': [{'range': [lo_, hi_], 'n': int(((c[:, 2] >= lo_) & (c[:, 2] < hi_)).sum())}
                                for lo_, hi_ in zip(DIST_EDGES[:-1], DIST_EDGES[1:])],
           'by_dist': _buckets(f[:, 4], DIST_EDGES, f, mk),
           'by_glft_gap': _buckets(f[:, 11], GAP_EDGES, f, mk),
           'by_wall_size_eth': _buckets(f[:, 6], ETH_EDGES, f, mk),
           'by_wall_dist': _buckets(f[:, 7], WALL_DIST_EDGES, f, mk),
           'by_imbalance': _buckets(f[:, 5], IMB_EDGES, f, mk),
           'by_queue0': _buckets(f[:, 8], QUEUE_EDGES, f, mk)}
    fp = f[pk]
    if len(fp):
        t = fp[:, 0].astype(np.int64) + ts0
        br = breach_flags(t, fp[:, 1].copy(), fp[:, 12].astype(np.int64), bbo_ts, bbv, bav, BREACH_NS)
        out['breach_5s_rate_peak'] = float(br.mean())
        out['markout_peak_hold'] = mk(fp[~br])
        out['markout_peak_breach'] = mk(fp[br])
    return out


def run_wall_study(l2_path, trades_path, tick, lo, hi, latency_ns, order_qty, gamma=0.05):
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
    e_ts = np.zeros(0, np.int64)
    e_h = np.zeros(0)
    for d in FIXED_D:
        nf, nc, cnt = virtual_orders(*ev, n, 0, float(d), 0.0, -1.0, int(latency_ns), DMAX, STALE, fills, cancels,
                                     e_ts, e_h, base, 0)
        f = fills[:min(nf, CAP_FILLS)]
        rep['fixed'][f'd{d}'] = {'counts': {k: int(v) for k, v in zip(COUNT_NAMES, cnt)},
                                 'fill_overflow': bool(nf > CAP_FILLS),
                                 'fills_per_hour': nf / hours if hours > 0 else None,
                                 'markout': mk(f), 'by_dist': _buckets(f[:, 4], DIST_EDGES, f, mk),
                                 'by_imbalance': _buckets(f[:, 5], IMB_EDGES, f, mk)}
    for qn, thr in thresholds.items():
        for fr in FRACS:
            nf, nc, cnt = virtual_orders(*ev, n, 1, 0.0, thr, fr, int(latency_ns), DMAX, STALE, fills, cancels,
                                         e_ts, e_h, base, 0)
            f = fills[:min(nf, CAP_FILLS)]
            c = cancels[:min(nc, CAP_CANCELS)]
            rep['wall'][f'{qn}_f{fr:g}'] = _wall_summary(f, c, cnt, nf, nc, thr, hours, mk, ts0, (bbo_ts, bbv, bav))
    t_glft = time.time()
    hs_ts, hs_half = glft_half_series(*ev, n, float(gamma))
    okh = np.isfinite(hs_half)
    variants = (('glft_quote', 3, 0.0), ('peak_v0', 2, 0.0), ('peak_v5', 2, PEAK_VMIN_FIXED),
                ('peak_q0.9', 2, thresholds['q0.9']))
    rep['glft'] = {'gamma': float(gamma), 'samples': int(len(hs_ts)), 'quotable_samples': int(okh.sum()),
                   'half_spread_ticks': _qs(hs_half[okh]), 'peak_depth_ticks': PEAK_DEPTH,
                   'variants': {k: {'mode': md, 'vmin_eth': float(v)} for k, md, v in variants},
                   'fit_runtime_s': round(time.time() - t_glft, 1)}
    rep['glft_peak'] = {}
    for name, md, vmin in variants:
        nf, nc, cnt = virtual_orders(*ev, n, md, 0.0, float(vmin), -1.0, int(latency_ns), DMAX, STALE, fills,
                                     cancels, hs_ts, hs_half, base, PEAK_DEPTH)
        f = fills[:min(nf, CAP_FILLS)]
        c = cancels[:min(nc, CAP_CANCELS)]
        rep['glft_peak'][name] = _glft_summary(f, c, cnt, nf, nc, hours, mk, ts0, (bbo_ts, bbv, bav))
    rep['runtime_s'] = round(time.time() - t_start, 1)
    return rep
