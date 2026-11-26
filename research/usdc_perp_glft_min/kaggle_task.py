'''Kaggle task: Tardis free day -> official HBT 2.4.4 -> event-driven GLFT on Binance USDC-M ETHUSDC
with the Gate-study origin-filtered ETHUSDT-perp spread signal (9 models).

Signal usage follows research/gate_btc_binance_signal_20261115 (see origin_glft.py for the source SHA):
600s time-weighted basis/sigma on a 5s clock, 1 sigma normal band, 1.645 sigma trigger, dominance and
signal-price-event activation. Requested differences: no re-quote throttle, signal visible at its
Tardis local_timestamp + 1ns (ordering only), order latency 1ms each way, maker fee 0.
Raw CSV and NPZ stay in /tmp; only report.json is written to the working directory.
'''
import argparse
import json
import shutil
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from basis_moments import NS, exact_moments_5s
from check_okx_hft_data import SCHEMAS, extract_window
from origin_glft import (DIAG_NAMES, GATE_SOURCE_SHA, LOT, MAX_UNITS, ORDER_QTY, REFIT_EVERY, STEP_NS,
                         TICK, WARMUP, run_origin_glft)
from tardis_segmented_hbt import convert_fuse_parts

EXCHANGE = 'binance-futures'
TARGET = 'ETHUSDC'
SIGNAL = 'ETHUSDT'
GAMMA = 0.05
LATENCY_NS = 1_000_000
SIGNAL_EXTRA_NS = 1
TAKER_FEE = 0.0005
FEE_BPS = (0.5, 1.0, 2.0)
MARKOUT_STEPS = (('100ms', 1), ('1s', 10), ('5s', 50), ('30s', 300), ('60s', 600))
STEPS_PER_HOUR = 36_000
BETAS = (0.25, 0.5, 0.75, 1.0)
MODELS = ((('baseline', 0.0, 0),) + tuple((f'both_b{b:g}', b, 1) for b in BETAS)
          + tuple((f'defensive_b{b:g}', b, 2) for b in BETAS))
MAX_ROWS = 75_000_000
WORK = Path('/tmp/usdc_perp_glft_min')
FIT_NAMES = ('a_per_s', 'k_per_tick', 'sigma_ticks_per_sqrt_s', 'half_spread_ticks', 'skew_ticks_per_unit')


def read_bbo(path):
    df = pd.read_csv(path, usecols=['local_timestamp', 'bid_price', 'ask_price'])
    ts = df['local_timestamp'].to_numpy(np.int64) * 1000
    bid = df['bid_price'].to_numpy(np.float64)
    ask = df['ask_price'].to_numpy(np.float64)
    ok = np.isfinite(bid) & np.isfinite(ask) & (bid > 0) & (ask > bid)
    if np.any(np.diff(ts) < 0):
        raise ValueError(f'unsorted bookTicker {path.name}')
    return ts[ok], 0.5 * (bid[ok] + ask[ok]), float(bid[ok].min()), float(ask[ok].max()), int((~ok).sum())


def basis_grid(sig_ts, sig_mid, tgt_ts, tgt_mid):
    first = max(int(sig_ts[0]), int(tgt_ts[0]))
    last = min(int(sig_ts[-1]), int(tgt_ts[-1]))
    step = 5 * NS
    start = -(-(first + 600 * NS) // step) * step
    if start > last:
        raise ValueError('not enough paired history for a 600s basis')
    return exact_moments_5s(sig_ts, sig_mid, tgt_ts, tgt_mid, start, last + 1)


def pct(x):
    x = np.asarray(x, np.float64)
    x = x[np.isfinite(x)]
    if not x.size:
        return None
    return dict(zip(('p05', 'p50', 'p95'), (float(v) for v in np.percentile(x, [5, 50, 95]))))


def summarize(res, name, beta, mode, mid, eq, pos, fills, fits, diag):
    (steps, n_fill, n_fit, submits, cancels, max_pos, n_sig, n_feed, n_resp,
     turnover, fee, n_trades, t0) = res
    mid = mid[:steps]
    eq = eq[:steps]
    pos = pos[:steps]
    f = fills[:n_fill]
    pnl = float(eq[-1]) if steps else float('nan')
    out = {'model': name, 'beta': beta, 'quote_mode': int(mode), 'gamma': GAMMA, 'samples': int(steps),
           'pnl_usdc': pnl, 'pnl_before_taker_fee_usdc': pnl + float(fee), 'taker_fee_usdc': float(fee),
           'fills': int(n_trades), 'fill_records': int(n_fill),
           'buy_fills': int((f[:, 1] > 0).sum()), 'sell_fills': int((f[:, 1] < 0).sum()),
           'turnover_usdc': float(turnover), 'submits': int(submits), 'cancels': int(cancels),
           'max_abs_position_eth': float(max_pos), 'max_abs_position_units': float(max_pos) / ORDER_QTY,
           'mean_abs_position_units': float(np.nanmean(np.abs(pos[WARMUP:]))) / ORDER_QTY if steps > WARMUP else None,
           'signal_rows': int(n_sig), 'target_feed_wakeups': int(n_feed), 'order_reply_wakeups': int(n_resp),
           'origin_diag': dict(zip(DIAG_NAMES, (int(v) for v in diag)))}
    out['pnl_by_maker_fee_bp'] = {str(bp): pnl - bp * 1e-4 * float(turnover) for bp in FEE_BPS}
    mk = {}
    first = np.maximum(np.ceil((f[:, 0] - t0) / STEP_NS).astype(np.int64) - 1, 0)
    for label, h in MARKOUT_STEPS:
        idx = first + h
        ok = idx < steps
        if not ok.any():
            mk[label] = None
            continue
        px = f[ok, 2]
        v = f[ok, 1] * (mid[idx[ok]] - px) / px * 1e4
        w = f[ok, 3]
        mk[label] = {'n': int(ok.sum()), 'mean_bp': float(np.nanmean(v)), 'median_bp': float(np.nanmedian(v)),
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
    out['fit'] = {k: pct(fz[:, i]) for i, k in enumerate(FIT_NAMES)}
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
              'config': {'gate_source_sha': GATE_SOURCE_SHA, 'tick': TICK, 'lot': LOT, 'order_qty_eth': ORDER_QTY,
                         'max_units': MAX_UNITS, 'gamma': GAMMA, 'sample_ms': STEP_NS // 1_000_000,
                         'warmup_samples': WARMUP, 'refit_every_samples': REFIT_EVERY,
                         'decision': 'event-driven: target feed, order reply, signal row, 100ms sample',
                         'requote_throttle_ms': None, 'latency_ms': [1, 1],
                         'signal_extra_latency_ns': SIGNAL_EXTRA_NS,
                         'basis': '600s time-weighted mean/sigma of ETHUSDT mid - ETHUSDC mid, 5s clock',
                         'normal_band_sigma': 1.0, 'trigger_sigma': 1.645,
                         'models': [list(m) for m in MODELS],
                         'queue_model': 'power_prob(2.0)', 'fill_model': 'no_partial',
                         'maker_fee': 0.0, 'taker_fee': TAKER_FEE,
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
        sig_ts, sig_mid, _, _, sig_bad = read_bbo(sig_path)
        tgt_ts, tgt_mid, lo, hi, tgt_bad = read_bbo(paths['book_ticker'])
        sig_path.unlink()
        sig_ts = sig_ts + SIGNAL_EXTRA_NS
        grid, basis, bsigma, coverage = basis_grid(sig_ts, sig_mid, tgt_ts, tgt_mid)
        report['basis'] = {'grid_points': int(len(grid)), 'first_grid_ns': int(grid[0]),
                           'basis_usdt': pct(basis), 'sigma_ticks': pct(bsigma / TICK),
                           'min_coverage': float(coverage.min()),
                           'invalid_bbo_rows': {SIGNAL: sig_bad, TARGET: tgt_bad}}
        del tgt_ts, tgt_mid
        save()
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
        for name, beta, mode in MODELS:
            asset = (BacktestAsset().data([str(p) for p in parts]).linear_asset(1.0)
                     .constant_order_latency(LATENCY_NS, LATENCY_NS)
                     .power_prob_queue_model(2.0).no_partial_fill_exchange()
                     .trading_value_fee_model(0.0, TAKER_FEE)
                     .tick_size(TICK).lot_size(LOT)
                     .roi_lb(lo * 0.97).roi_ub(hi * 1.03).last_trades_capacity(20_000))
            hbt = ROIVectorMarketDepthBacktest([asset])
            mid = np.full(max_steps, np.nan)
            eq = np.full(max_steps, np.nan)
            pos = np.full(max_steps, np.nan)
            fills = np.zeros((max_steps, 4))
            fits = np.full((max_steps // REFIT_EVERY + 2, 5), np.nan)
            diag = np.zeros(len(DIAG_NAMES), np.int64)
            t1 = time.time()
            try:
                res = run_origin_glft(hbt, max_steps, sig_ts, sig_mid, grid, basis, bsigma, beta, mode, GAMMA,
                                      mid, eq, pos, fills, fits, diag)
            finally:
                hbt.close()
            v = summarize(res, name, beta, mode, mid, eq, pos, fills, fits, diag)
            v['runtime_s'] = round(time.time() - t1, 1)
            report['variants'][name] = v
            print('VARIANT', name, json.dumps({k: v[k] for k in ('pnl_usdc', 'fills', 'turnover_usdc', 'submits',
                                                                 'cancels', 'max_abs_position_units',
                                                                 'origin_diag', 'runtime_s')}), flush=True)
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