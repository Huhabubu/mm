'''Kaggle task: Tardis free day -> official HBT 2.4.4 -> simple GLFT on Binance USDC-M ETHUSDC.

Fair price optionally led by the ETHUSDT perp bookTicker. Raw CSV and NPZ stay in /tmp;
only report.json is written to the working directory.
'''
import argparse
import json
import math
import shutil
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from numba import njit
from hftbacktest import BUY, BUY_EVENT, GTX, LIMIT, SELL

from check_okx_hft_data import SCHEMAS, extract_window
from glft_core import (fit_intensity_from_counts, glft_coefficients, glft_quotes,
                       push_change, push_depth)
from tardis_segmented_hbt import convert_fuse_parts

EXCHANGE = 'binance-futures'
TARGET = 'ETHUSDC'
SIGNAL = 'ETHUSDT'
TICK = 0.01
LOT = 0.001
ORDER_QTY = 0.03
MAX_UNITS = 5.0
STEP_NS = 100_000_000
STEP_S = 0.1
STEPS_PER_HOUR = 36_000
WARMUP = 6000
WINDOW = 6000
MIN_FIT_STEPS = 600
REFIT_EVERY = 50
GRID = 10
MAX_BINS = 1000
MIN_COUNT = 10
BASIS_LAMBDA = 1.0 - 0.5 ** (STEP_S / 60.0)
SIGNAL_MAX_AGE_NS = 1_000_000_000
TAKER_FEE = 0.0005
FEE_BPS = (0.5, 1.0, 2.0)
MARKOUT_STEPS = (('1s', 10), ('5s', 50), ('30s', 300), ('60s', 600))
PARAMS = ((0.0, 0.05), (0.0, 0.01), (1.0, 0.05), (1.0, 0.01))
MAX_ROWS = 75_000_000
WORK = Path('/tmp/usdc_perp_glft_min')
FIT_NAMES = ('a_per_s', 'k_per_tick', 'sigma_ticks_per_sqrt_s', 'half_spread_ticks', 'skew_ticks_per_unit')


@njit
def run_glft(hbt, max_steps, sig_ts, sig_mid, alpha, gamma, out_mid, out_eq, fills, fits):
    depth_ring = np.zeros(WINDOW, np.int64)
    counts = np.zeros(MAX_BINS + 1, np.int64)
    chg_ring = np.zeros(WINDOW, np.float64)
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
    skew = 0.0
    fitted = False
    basis = 0.0
    basis_init = False
    sp = 0
    n_sig = sig_ts.shape[0]
    sig_ok = 0
    oid = 1
    st = hbt.state_values(0)
    prev_pos = st.position
    prev_bal = st.balance
    prev_nt = st.num_trades
    n_fill = 0
    n_fit = 0
    multi = 0
    submits = 0
    cancels = 0
    max_pos = 0.0
    step = 0
    while step < max_steps:
        if hbt.elapse(STEP_NS) != 0:
            break
        now = hbt.current_timestamp
        hbt.clear_inactive_orders(0)
        book = hbt.depth(0)
        bb = book.best_bid_tick
        ba = book.best_ask_tick
        valid = bb > 0 and ba > bb and ba < 1_000_000_000
        mid = prev_mid
        if valid:
            mid = (bb + ba) / 2.0
        if valid and not math.isnan(prev_mid):
            furthest = 0.0
            for tr in hbt.last_trades(0):
                p = tr.px / TICK
                if (tr.ev & BUY_EVENT) == BUY_EVENT:
                    d = p - prev_mid
                else:
                    d = prev_mid - p
                if d > furthest:
                    furthest = d
            dj = int(furthest * GRID)
            if dj > MAX_BINS:
                dj = MAX_BINS
            d_idx, d_fill = push_depth(depth_ring, d_idx, d_fill, counts, dj)
            c_idx, c_fill, s1, s2 = push_change(chg_ring, c_idx, c_fill, s1, s2, mid - prev_mid)
        hbt.clear_last_trades(0)
        prev_mid = mid

        mt = 0.0
        while sp + 1 < n_sig and sig_ts[sp + 1] <= now:
            sp += 1
        sig_valid = n_sig > 0 and sig_ts[sp] <= now and now - sig_ts[sp] <= SIGNAL_MAX_AGE_NS
        if sig_valid:
            sig_ok += 1
            mt = sig_mid[sp] / TICK
            if valid:
                if basis_init:
                    basis += BASIS_LAMBDA * (mid - mt - basis)
                else:
                    basis = mid - mt
                    basis_init = True

        if step % REFIT_EVERY == 0 and d_fill >= MIN_FIT_STEPS:
            a, k, ok = fit_intensity_from_counts(counts, d_fill * STEP_S, MIN_COUNT)
            mean = s1 / c_fill
            var = s2 / c_fill - mean * mean
            sigma = math.sqrt(var / STEP_S) if var > 0.0 else 0.0
            if ok:
                a_hat = a
                k_hat = k * GRID
                fitted = True
            if fitted and sigma > 0.0:
                half, skew = glft_coefficients(a_hat, k_hat, sigma, gamma, 1.0)
            if fitted and n_fit < fits.shape[0]:
                fits[n_fit, 0] = a_hat
                fits[n_fit, 1] = k_hat
                fits[n_fit, 2] = sigma
                fits[n_fit, 3] = half
                fits[n_fit, 4] = skew
                n_fit += 1

        st = hbt.state_values(0)
        pos = st.position
        dn = st.num_trades - prev_nt
        if dn > 0:
            if dn > 1:
                multi += 1
            dpos = pos - prev_pos
            if abs(dpos) > 1e-12 and n_fill < fills.shape[0]:
                fills[n_fill, 0] = step
                fills[n_fill, 1] = 1.0 if dpos > 0 else -1.0
                fills[n_fill, 2] = -(st.balance - prev_bal) / dpos
                fills[n_fill, 3] = abs(dpos)
                fills[n_fill, 4] = dn
                n_fill += 1
        prev_pos = pos
        prev_bal = st.balance
        prev_nt = st.num_trades
        if abs(pos) > max_pos:
            max_pos = abs(pos)
        out_mid[step] = mid * TICK
        out_eq[step] = st.balance + pos * mid * TICK - st.fee

        quote = (step >= WARMUP and valid and fitted and sigma > 0.0
                 and math.isfinite(half) and math.isfinite(skew) and half < 1e5 and skew < 1e5)
        if quote:
            fair = mid
            if alpha > 0.0 and sig_valid and basis_init:
                fair = mid + alpha * (mt + basis - mid)
            q = pos / ORDER_QTY
            bid, ask = glft_quotes(fair, q, half, skew, bb, ba)
            want_bid = q < MAX_UNITS - 1e-9 and bid > 0
            want_ask = q > -MAX_UNITS + 1e-9
            has_bid = False
            has_ask = False
            values = hbt.orders(0).values()
            while values.has_next():
                o = values.get()
                if o.side == BUY:
                    if want_bid and not has_bid and o.price_tick == bid:
                        has_bid = True
                    elif o.cancellable:
                        hbt.cancel(0, o.order_id, False)
                        cancels += 1
                elif o.side == SELL:
                    if want_ask and not has_ask and o.price_tick == ask:
                        has_ask = True
                    elif o.cancellable:
                        hbt.cancel(0, o.order_id, False)
                        cancels += 1
            if want_bid and not has_bid:
                hbt.submit_buy_order(0, oid, bid * TICK, ORDER_QTY, GTX, LIMIT, False)
                oid += 1
                submits += 1
            if want_ask and not has_ask:
                hbt.submit_sell_order(0, oid, ask * TICK, ORDER_QTY, GTX, LIMIT, False)
                oid += 1
                submits += 1
        step += 1
    st = hbt.state_values(0)
    return (step, n_fill, n_fit, multi, submits, cancels, max_pos, sig_ok,
            st.trading_value, st.fee, st.num_trades)


def read_bbo(path):
    df = pd.read_csv(path, usecols=['local_timestamp', 'bid_price', 'ask_price'])
    ts = df['local_timestamp'].to_numpy(np.int64) * 1000
    mid = 0.5 * (df['bid_price'].to_numpy(np.float64) + df['ask_price'].to_numpy(np.float64))
    return ts, mid, float(df['bid_price'].min()), float(df['ask_price'].max())


def pct(x):
    x = np.asarray(x, np.float64)
    x = x[np.isfinite(x)]
    if not x.size:
        return None
    return dict(zip(('p05', 'p50', 'p95'), (float(v) for v in np.percentile(x, [5, 50, 95]))))


def summarize(res, alpha, gamma, mid, eq, fills, fits):
    steps, n_fill, n_fit, multi, submits, cancels, max_pos, sig_ok, turnover, fee, n_trades = res
    mid = mid[:steps]
    eq = eq[:steps]
    f = fills[:n_fill]
    pnl = float(eq[-1]) if steps else float('nan')
    out = {'alpha': alpha, 'gamma': gamma, 'steps': int(steps), 'pnl_usdc': pnl,
           'pnl_before_taker_fee_usdc': pnl + float(fee), 'taker_fee_usdc': float(fee),
           'fills': int(n_trades), 'fill_steps': int(n_fill), 'multi_fill_steps': int(multi),
           'buy_fill_steps': int((f[:, 1] > 0).sum()), 'sell_fill_steps': int((f[:, 1] < 0).sum()),
           'turnover_usdc': float(turnover), 'submits': int(submits), 'cancels': int(cancels),
           'max_abs_position_eth': float(max_pos), 'max_abs_position_units': float(max_pos) / ORDER_QTY,
           'signal_available_ratio': float(sig_ok) / steps if steps else 0.0}
    out['pnl_by_maker_fee_bp'] = {str(bp): pnl - bp * 1e-4 * float(turnover) for bp in FEE_BPS}
    mk = {}
    for name, h in MARKOUT_STEPS:
        idx = f[:, 0].astype(np.int64) + h
        ok = idx < steps
        if not ok.any():
            mk[name] = None
            continue
        px = f[ok, 2]
        v = f[ok, 1] * (mid[idx[ok]] - px) / px * 1e4
        w = f[ok, 3]
        mk[name] = {'n': int(ok.sum()), 'mean_bp': float(np.nanmean(v)),
                    'median_bp': float(np.nanmedian(v)),
                    'qty_weighted_bp': float(np.nansum(v * w) / np.sum(w))}
    out['markout'] = mk
    hourly = []
    if steps:
        ends = list(range(STEPS_PER_HOUR - 1, steps, STEPS_PER_HOUR))
        if not ends or ends[-1] != steps - 1:
            ends.append(steps - 1)
        prev = 0.0
        for e in ends:
            cur = float(np.nan_to_num(eq[e]))
            hourly.append(cur - prev)
            prev = cur
        e2 = np.nan_to_num(eq, nan=0.0)
        out['max_drawdown_usdc'] = float(np.max(np.maximum.accumulate(e2) - e2))
    out['hourly_pnl_usdc'] = hourly
    fz = fits[:n_fit]
    out['fit'] = {name: pct(fz[:, i]) for i, name in enumerate(FIT_NAMES)}
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--day', default='2026-11-01')
    ap.add_argument('--hours', type=float, default=24.0)
    ap.add_argument('--chunk-minutes', type=int, default=60)
    args = ap.parse_args()
    t0 = time.time()
    report = {'status': 'started', 'day': args.day, 'hours': args.hours, 'exchange': EXCHANGE,
              'target': TARGET, 'signal': SIGNAL,
              'config': {'tick': TICK, 'lot': LOT, 'order_qty_eth': ORDER_QTY, 'max_units': MAX_UNITS,
                         'step_ms': STEP_NS // 1_000_000, 'warmup_steps': WARMUP, 'window_steps': WINDOW,
                         'refit_every_steps': REFIT_EVERY, 'grid_per_tick': GRID, 'min_count': MIN_COUNT,
                         'basis_half_life_s': 60.0, 'signal_max_age_ms': SIGNAL_MAX_AGE_NS // 1_000_000,
                         'latency_ms': [1, 1], 'queue_model': 'power_prob(2.0)', 'fill_model': 'no_partial',
                         'maker_fee': 0.0, 'taker_fee': TAKER_FEE, 'params_alpha_gamma': PARAMS,
                         'historical_spec_verified': False, 'maker_zero_fee_verified': False}}
    out_path = Path('report.json')

    def save():
        out_path.write_text(json.dumps(report, indent=2, default=str), encoding='utf-8')

    try:
        day0 = datetime.strptime(args.day, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        if day0.day != 1:
            raise ValueError('Tardis free day must be the first of a month')
        start_us = int(day0.timestamp()) * 1_000_000
        end_us = start_us + int(args.hours * 3600) * 1_000_000
        shutil.rmtree(WORK, ignore_errors=True)
        WORK.mkdir(parents=True)
        paths = {}
        counts = {}
        for kind in SCHEMAS:
            paths[kind] = WORK / f'{TARGET}_{kind}.csv'
            counts[kind] = extract_window(kind, args.day, end_us, paths[kind], max_rows=MAX_ROWS,
                                          exchange=EXCHANGE, symbol=TARGET)
            print('DOWNLOADED', TARGET, kind, json.dumps(counts[kind]), flush=True)
        sig_path = WORK / f'{SIGNAL}_book_ticker.csv'
        sig_counts = extract_window('book_ticker', args.day, end_us, sig_path, max_rows=MAX_ROWS,
                                    exchange=EXCHANGE, symbol=SIGNAL)
        print('DOWNLOADED', SIGNAL, 'book_ticker', json.dumps(sig_counts), flush=True)
        report['feed_counts'] = {f'{TARGET}_{k}': v for k, v in counts.items()}
        report['feed_counts'][f'{SIGNAL}_book_ticker'] = sig_counts
        report['download_s'] = round(time.time() - t0, 1)
        save()
        sig_ts, sig_mid, _, _ = read_bbo(sig_path)
        _, _, lo, hi = read_bbo(paths['book_ticker'])
        sig_path.unlink()
        conv_start = min([start_us] + [int(c['first_local_us']) for c in counts.values()
                                       if c.get('first_local_us') is not None])
        parts, details = convert_fuse_parts(paths, counts, WORK, conv_start, end_us,
                                            args.chunk_minutes, TICK, LOT)
        for p in paths.values():
            p.unlink()
        report['conversion'] = {'parts': len(parts), 'start_us': conv_start, 'end_us': end_us,
                                'roi_price': [lo, hi], 'details': details}
        save()

        from hftbacktest import BacktestAsset, ROIVectorMarketDepthBacktest
        max_steps = int(args.hours * STEPS_PER_HOUR)
        report['variants'] = {}
        for alpha, gamma in PARAMS:
            asset = (BacktestAsset().data([str(p) for p in parts]).linear_asset(1.0)
                     .constant_order_latency(1_000_000, 1_000_000)
                     .power_prob_queue_model(2.0).no_partial_fill_exchange()
                     .trading_value_fee_model(0.0, TAKER_FEE)
                     .tick_size(TICK).lot_size(LOT)
                     .roi_lb(lo * 0.97).roi_ub(hi * 1.03).last_trades_capacity(20_000))
            hbt = ROIVectorMarketDepthBacktest([asset])
            mid = np.full(max_steps, np.nan)
            eq = np.full(max_steps, np.nan)
            fills = np.zeros((max_steps, 5))
            fits = np.full((max_steps // REFIT_EVERY + 1, 5), np.nan)
            t1 = time.time()
            try:
                res = run_glft(hbt, max_steps, sig_ts, sig_mid, alpha, gamma, mid, eq, fills, fits)
            finally:
                hbt.close()
            name = f'alpha{alpha:g}_gamma{gamma:g}'
            v = summarize(res, alpha, gamma, mid, eq, fills, fits)
            v['runtime_s'] = round(time.time() - t1, 1)
            report['variants'][name] = v
            print('VARIANT', name, json.dumps({k: v[k] for k in ('pnl_usdc', 'fills', 'turnover_usdc',
                                                                 'max_abs_position_units',
                                                                 'signal_available_ratio')}), flush=True)
            save()
        report['status'] = 'complete'
    except Exception as exc:
        report['status'] = 'error'
        report['error'] = f'{type(exc).__name__}: {exc}'
        report['traceback'] = traceback.format_exc()[-4000:]
        print(report['traceback'], flush=True)
    finally:
        report['elapsed_s'] = round(time.time() - t0, 1)
        save()
        shutil.rmtree(WORK, ignore_errors=True)
    return 0 if report['status'] == 'complete' else 1


if __name__ == '__main__':
    sys.exit(main())