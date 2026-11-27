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
from event_study import clocks_from_csv, fill_conditioning, lead_lag_report
from wall_study import run_wall_study
from sweep_study import run_sweep_study
from fill_sweep_check import fill_sweep_check, merge_events, sweep_event_arrays
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
INTERVAL_CAP = 500_000
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


def markout_table(f, mid, steps, t0):
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
    return mk


def mid_change_stats(ts, mid, lo=None, hi=None):
    # Mid changes per row and per distinct timestamp; the replay loop sees the last row of each timestamp.
    if lo is not None:
        keep = (ts >= lo) & (ts < hi)
        ts, mid = ts[keep], mid[keep]
    if len(ts) < 2:
        return {'rows': int(len(ts))}
    last = np.r_[ts[1:] != ts[:-1], True]
    jumps = np.abs(np.diff(mid[last])) / TICK
    jumps = jumps[jumps > 0]
    span_s = (int(ts[-1]) - int(ts[0])) / NS
    return {'rows': int(len(ts)), 'distinct_ts': int(last.sum()),
            'row_mid_changes': int(np.count_nonzero(np.diff(mid))),
            'ts_mid_changes': int(jumps.size),
            'ts_mid_changes_per_s': float(jumps.size / span_s) if span_s > 0 else None,
            'jump_ticks': pct(jumps)}


def window_report(f, mid, steps, intervals, n_int, t0):
    # Group fills by origin-filter active intervals (shadow intervals for the baseline).
    iv = intervals[:min(n_int, len(intervals))]
    ts = f[:, 0]
    inside = np.zeros(len(f), dtype=bool)
    pre = np.zeros(len(f), dtype=bool)
    if len(iv) and len(f):
        starts = iv[:, 0].astype(np.float64)
        ends = iv[:, 1].astype(np.float64)
        k = np.searchsorted(starts, ts, side='right') - 1
        inside = (k >= 0) & (ts < ends[np.maximum(k, 0)])
        nxt = np.minimum(k + 1, len(starts) - 1)
        to_next = np.where(k + 1 < len(starts), starts[nxt] - ts, np.inf)
        pre = ~inside & (to_next <= NS)
    dur = (iv[:, 1] - iv[:, 0]).astype(np.float64) / NS
    quoting_s = max(steps - WARMUP, 0) * STEP_NS / NS
    groups = {'in_window': inside, 'pre_1s': pre, 'outside': ~inside & ~pre}
    return {'intervals': int(n_int), 'capacity_hit': bool(n_int > len(intervals)),
            'active_s': float(dur.sum()),
            'active_fraction_of_quoting': float(dur.sum() / quoting_s) if quoting_s > 0 else None,
            'interval_s': pct(dur),
            'fills': {g: {'n': int(m.sum()), 'markout': markout_table(f[m], mid, steps, t0) if m.any() else None}
                      for g, m in groups.items()}}


def summarize(res, name, beta, mode, mid, eq, pos, fills, fits, diag, intervals):
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
    out['markout'] = markout_table(f, mid, steps, t0)
    out['signal_window'] = window_report(f, mid, steps, intervals, int(diag[DIAG_NAMES.index('intervals')]), t0)
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
                         'historical_spec_verified': False, 'maker_zero_fee_verified': False,
                         'diagnostics': ('origin filter also runs in shadow for the baseline (no quote effect); '
                                         'fill windows compare fill exch_timestamp with local activation time')}}
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
        sig_trades_path = WORK / f'{SIGNAL}_trades.csv'
        sig_trade_counts = extract_window('trades', args.day, end_us, sig_trades_path, max_rows=MAX_ROWS,
                                          exchange=EXCHANGE, symbol=SIGNAL)
        print('DOWNLOADED', SIGNAL, 'trades', json.dumps(sig_trade_counts), flush=True)
        report['feed_counts'] = {f'{TARGET}_{k}': v for k, v in counts.items()}
        report['feed_counts'][f'{SIGNAL}_book_ticker'] = sig_counts
        report['feed_counts'][f'{SIGNAL}_trades'] = sig_trade_counts
        report['download_s'] = round(time.time() - t0, 1)
        save()
        sig_ts, sig_mid, _, _, sig_bad = read_bbo(sig_path)
        tgt_ts, tgt_mid, lo, hi, tgt_bad = read_bbo(paths['book_ticker'])
        report['raw_mid_changes'] = {SIGNAL: mid_change_stats(sig_ts, sig_mid),
                                     TARGET: mid_change_stats(tgt_ts, tgt_mid)}
        sig_clk = clocks_from_csv(sig_path)
        tgt_clk = clocks_from_csv(paths['book_ticker'])
        report['lead_lag'] = lead_lag_report(sig_clk, tgt_clk, TICK)
        try:
            report['sweep_study'] = run_sweep_study(paths['trades'], sig_trades_path, paths['book_ticker'],
                                                    sig_path, TICK, names=(TARGET, SIGNAL))
        except Exception as exc:
            report['sweep_study'] = {'error': f'{type(exc).__name__}: {exc}',
                                     'traceback': traceback.format_exc()[-3000:]}
        print('SWEEP_STUDY', json.dumps({k: report['sweep_study'].get(k) for k in ('error', 'runtime_s')}),
              flush=True)
        sweep_ev = None
        try:
            ev_u = sweep_event_arrays(sig_trades_path, TICK)
            ev_c = sweep_event_arrays(paths['trades'], TICK)
            sweep_ev = {}
            for j in sorted(ev_u):
                sweep_ev[f'usdt_j{j}'] = ev_u[j]
                sweep_ev[f'usdc_j{j}'] = ev_c[j]
                sweep_ev[f'both_j{j}'] = merge_events(ev_u[j], ev_c[j])
        except Exception as exc:
            report['fill_sweep_check'] = {'error': f'{type(exc).__name__}: {exc}',
                                          'traceback': traceback.format_exc()[-3000:]}
        sig_trades_path.unlink()
        save()
        sig_path.unlink()
        sig_ts = sig_ts + SIGNAL_EXTRA_NS
        grid, basis, bsigma, coverage = basis_grid(sig_ts, sig_mid, tgt_ts, tgt_mid)
        report['basis'] = {'grid_points': int(len(grid)), 'first_grid_ns': int(grid[0]),
                           'basis_usdt': pct(basis), 'sigma_ticks': pct(bsigma / TICK),
                           'min_coverage': float(coverage.min()),
                           'invalid_bbo_rows': {SIGNAL: sig_bad, TARGET: tgt_bad}}
        del tgt_ts, tgt_mid
        save()
        try:
            report['wall_study'] = run_wall_study(paths['incremental_book_L2'], paths['trades'], TICK,
                                                  lo, hi, LATENCY_NS, ORDER_QTY, GAMMA)
        except Exception as exc:
            report['wall_study'] = {'error': f'{type(exc).__name__}: {exc}',
                                    'traceback': traceback.format_exc()[-3000:]}
        print('WALL_STUDY', json.dumps({k: report['wall_study'].get(k) for k in
                                        ('error', 'runtime_s', 'thresholds_eth', 'series_overflow')}),
              flush=True)
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
            intervals = np.zeros((INTERVAL_CAP, 2), np.int64)
            t1 = time.time()
            try:
                res = run_origin_glft(hbt, max_steps, sig_ts, sig_mid, grid, basis, bsigma, beta, mode, GAMMA,
                                      mid, eq, pos, fills, fits, diag, intervals)
            finally:
                hbt.close()
            v = summarize(res, name, beta, mode, mid, eq, pos, fills, fits, diag, intervals)
            if mode == 0:
                lo_ns = int(res[-1])
                hi_ns = lo_ns + int(res[0]) * STEP_NS
                report['signal_check'] = {'window_ns': [lo_ns, hi_ns],
                                          'raw': mid_change_stats(sig_ts, sig_mid, lo_ns, hi_ns),
                                          'loop_signal_price_updates': v['origin_diag']['signal_price_updates'],
                                          'loop_signal_rows': v['signal_rows']}
                report['fill_event_study'] = fill_conditioning(
                    fills[:int(res[1])], sig_clk, tgt_clk, TICK, LATENCY_NS,
                    lambda sub: markout_table(sub, mid, int(res[0]), int(res[-1])))
            if name == 'baseline' and sweep_ev is not None:
                try:
                    report['fill_sweep_check'] = fill_sweep_check(fills[:int(res[1])], sweep_ev, LATENCY_NS, mid,
                                                                  int(res[0]), int(res[-1]), STEP_NS)
                except Exception as exc:
                    report['fill_sweep_check'] = {'error': f'{type(exc).__name__}: {exc}',
                                                  'traceback': traceback.format_exc()[-3000:]}
                print('FILL_SWEEP_CHECK', json.dumps({k: report['fill_sweep_check'].get(k)
                                                      for k in ('error', 'fills')}), flush=True)
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