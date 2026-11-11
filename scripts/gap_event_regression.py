"""Exploratory causal Binance-OKX gap regression at the joint event clock.

BTC/USDT linear swap on both venues. Inputs are Tardis native BBO collection
timestamps, not exchange timestamps. Only past quotes enter basis/features.
Existing six dates have been explored previously: NOT an untouched holdout.
"""
import argparse
import csv
import gzip
import io
import json
import urllib.request
from array import array
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from cross_venue_event_probe import read_binance_bbo, visible_timestamps
from inspect_tardis import BASE_URL

SECOND_NS = 1_000_000_000
TRAIN_DAYS = ("2026-06-01", "2026-07-01", "2026-08-01")
VALID_DAYS = ("2026-09-01",)
DIAG_DAYS = ("2026-10-01", "2026-11-01")


def read_okx_bbo(day, end_us, opener=urllib.request.urlopen):
    yyyy, mm, dd = day.split("-")
    url = f"{BASE_URL}/okex-swap/book_ticker/{yyyy}/{mm}/{dd}/BTC-USDT-SWAP.csv.gz"
    request = urllib.request.Request(url, headers={"User-Agent": "mm-event-gap-diagnostic/1.0"})
    ts, mid = array("q"), array("d")
    prior = -1
    with opener(request, timeout=120) as response:
        with gzip.GzipFile(fileobj=response) as zipped:
            with io.TextIOWrapper(zipped, encoding="utf-8", newline="") as stream:
                for row in csv.DictReader(stream):
                    t = int(row["local_timestamp"])
                    if t >= end_us:
                        break
                    if t < prior:
                        raise ValueError("Decreasing OKX local_timestamp")
                    prior = t
                    bid, ask = float(row["bid_price"]), float(row["ask_price"])
                    if not 0 < bid < ask:
                        raise ValueError("Invalid OKX quote")
                    ts.append(t * 1000)
                    mid.append((bid + ask) / 2)
    if not ts:
        raise ValueError("No OKX native quotes")
    return np.asarray(ts, dtype=np.int64), np.asarray(mid, dtype=np.float64)


def event_gap_samples(okx_time, okx_mid, bin_time, bin_mid,
                      basis_seconds=60, tau_ms=100, max_age_ms=250):
    """Produce samples at EVERY received native-BBO event of both venues.

    Market-source rows at equal timestamps retain their own exact row index;
    other-source as-of search is strictly earlier (left) at tied timestamps.
    The basis is a strictly trailing time-weighted 1-second grid average.
    A full-length basis window with >=75% fresh 1-second points is mandatory; no backfilling.
    bin_time must already include assumed availability delay and +1ns tie policy.
    """
    ot = np.asarray(okx_time, dtype=np.int64)
    om = np.asarray(okx_mid, dtype=np.float64)
    bt = np.asarray(bin_time, dtype=np.int64)
    bm = np.asarray(bin_mid, dtype=np.float64)
    if not (len(ot) and len(bt) and len(ot) == len(om) and len(bt) == len(bm)):
        raise ValueError("Empty or incompatible BBO arrays")
    if (np.any(np.diff(ot) < 0) or np.any(np.diff(bt) < 0) or
            not np.all(np.isfinite(om)) or not np.all(np.isfinite(bm))):
        raise ValueError("BBO must be ordered and finite")
    if basis_seconds < 2 or tau_ms <= 0 or max_age_ms <= 0:
        raise ValueError("Invalid basis/horizon/freshness configuration")
    age_limit = max_age_ms * 1_000_000
    # Exactly one decision per original source row; never as-of the source's
    # *later* records with the same local timestamp.
    t = np.concatenate((ot, bt))
    source = np.concatenate((np.zeros(len(ot), np.int8),
                             np.ones(len(bt), np.int8)))
    oi = np.concatenate((np.arange(len(ot)), np.searchsorted(ot, bt, side="left") - 1))
    bi = np.concatenate((np.searchsorted(bt, ot, side="left") - 1, np.arange(len(bt))))
    order = np.argsort(t, kind="stable")
    t, source, oi, bi = (a[order] for a in (t, source, oi, bi))

    # Historical basis on an exogenous 1-second clock, strictly BEFORE grid t.
    start = ((max(int(ot[0]), int(bt[0])) + SECOND_NS - 1) // SECOND_NS) * SECOND_NS
    end = min(int(ot[-1]), int(bt[-1]))
    grid = np.arange(start, end + 1, SECOND_NS, dtype=np.int64)
    if len(grid) < basis_seconds:
        raise ValueError("Insufficient history for full basis window")
    go = np.searchsorted(ot, grid, side="left") - 1
    gb = np.searchsorted(bt, grid, side="left") - 1
    good_grid = (go >= 0) & (gb >= 0)
    goc = np.maximum(go, 0)
    gbc = np.maximum(gb, 0)
    good_grid &= (grid - ot[goc] <= age_limit) & (grid - bt[gbc] <= age_limit)
    spread = np.where(good_grid, bm[gbc] - om[goc], 0.0)
    sums = np.r_[0.0, np.cumsum(spread, dtype=np.float64)]
    counts = np.r_[0, np.cumsum(good_grid.astype(np.int64))]
    basis = np.full(len(grid), np.nan, dtype=np.float64)
    w = basis_seconds
    coverage = counts[w:] - counts[:-w]
    valid_window = coverage >= int(np.ceil(0.75 * w))
    basis[w - 1:] = np.where(valid_window,
                             (sums[w:] - sums[:-w]) / np.maximum(coverage, 1), np.nan)

    basis_idx = np.searchsorted(grid, t, side="left") - 1
    current = (oi >= 0) & (bi >= 0)
    oc, bc = np.maximum(oi, 0), np.maximum(bi, 0)
    current &= (t - ot[oc] >= 0) & (t - ot[oc] <= age_limit)
    current &= (t - bt[bc] >= 0) & (t - bt[bc] <= age_limit)
    prior_idx = np.maximum(basis_idx, 0)
    current &= (basis_idx >= 0) & np.isfinite(basis[prior_idx])
    # No right-censoring: the future OKX BBO must be observable within tape.
    target = t + int(tau_ms * 1_000_000)
    future = np.searchsorted(ot, target, side="right") - 1
    current &= target <= ot[-1]
    if not np.any(current):
        raise ValueError(f"No causal samples: basis_valid={int(np.isfinite(basis).sum())}, fresh_events={int(np.count_nonzero((oi >= 0) & (bi >= 0)))}, total_events={len(t)}")
    keep = np.flatnonzero(current)
    oidx, bidx, fidx = oi[keep], bi[keep], future[keep]
    return {
        "t_ns": t[keep],
        "source": source[keep],
        "gap_usdt_per_btc": bm[bidx] - om[oidx] - basis[basis_idx[keep]],
        "response_usdt_per_btc": om[fidx] - om[oidx],
        "basis_usdt_per_btc": basis[basis_idx[keep]],
        "okx_age_ms": (t[keep] - ot[oidx]) / 1e6,
        "binance_age_ms": (t[keep] - bt[bidx]) / 1e6,
        "all_event_rows": int(len(t)),
        "basis_grid_seconds": int(len(grid)),
        "basis_valid_seconds": int(np.isfinite(basis).sum()),
        "basis_min_fresh_coverage_fraction": 0.75,
    }


def fit_beta(samples):
    """OLS with intercept and exploratory 5-second block-cluster t statistic."""
    x = np.asarray(samples["gap_usdt_per_btc"], dtype=np.float64)
    y = np.asarray(samples["response_usdt_per_btc"], dtype=np.float64)
    ts = np.asarray(samples["t_ns"], dtype=np.int64)
    if len(x) < 30 or np.std(x) < 1e-12:
        raise ValueError("Insufficient or constant gap signal")
    X = np.column_stack((np.ones(len(x)), x))
    coefs = np.linalg.lstsq(X, y, rcond=None)[0]
    residual = y - X @ coefs
    groups = (ts - ts.min()) // (5 * SECOND_NS)
    _, codes = np.unique(groups, return_inverse=True)
    S = np.zeros((int(codes.max()) + 1, 2))
    np.add.at(S, codes, X * residual[:, None])
    inv = np.linalg.inv(X.T @ X)
    cov = inv @ (S.T @ S) @ inv
    if len(S) > 1:
        cov *= len(S) / (len(S) - 1)
    se = float(np.sqrt(max(cov[1, 1], 0)))
    return {"alpha_usdt_per_btc": float(coefs[0]), "beta": float(coefs[1]),
            "beta_t_5s_blocks_exploratory": float(coefs[1] / se) if se > 0 else None,
            "n_events": int(len(x)), "n_5s_blocks": int(len(S)),
            "note": "5s block cluster t is diagnostic; overlapping horizons and cross-day dependence require further inference"}


def score(samples, fitted, bins=10):
    x = np.asarray(samples["gap_usdt_per_btc"])
    y = np.asarray(samples["response_usdt_per_btc"])
    pred = fitted["alpha_usdt_per_btc"] + fitted["beta"] * x
    mse = float(np.mean((y - pred) ** 2))
    base = float(np.mean(y * y))
    cuts = np.quantile(x, np.linspace(0, 1, bins + 1))
    groups = np.searchsorted(cuts[1:-1], x, side="right")
    bucket = []
    for i in range(bins):
        mask = groups == i
        if mask.any():
            bucket.append({"bin": i, "n": int(mask.sum()),
                           "mean_gap_usdt_per_btc": float(np.mean(x[mask])),
                           "mean_future_change_usdt_per_btc": float(np.mean(y[mask]))})
    return {"n": int(len(x)), "mse_model": mse, "mse_zero": base,
            "r2_vs_zero": float(1 - mse / base) if base > 0 else None,
            "mean_realized_change_usdt_per_btc": float(np.mean(y)),
            "bin_summary": bucket}


def subset(samples, mask):
    return {k: v[mask] for k, v in samples.items() if isinstance(v, np.ndarray)
            and len(v) == len(samples["t_ns"])}


def join_samples(parts):
    return {k: np.concatenate([s[k] for s in parts]) for k in
            ("t_ns", "source", "gap_usdt_per_btc", "response_usdt_per_btc")}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--minutes", type=int, default=12)
    p.add_argument("--basis-seconds", type=int, default=60)
    p.add_argument("--tau-ms", type=int, default=100)
    p.add_argument("--delay-ms", type=int, default=25)
    p.add_argument("--report-dir", default="reports")
    args = p.parse_args()
    if not (3 <= args.minutes <= 30):
        p.error("Event diagnostic duration must be 3-30 minutes")
    days = TRAIN_DAYS + VALID_DAYS + DIAG_DAYS
    report = {"status": "started", "dates": days, "train": TRAIN_DAYS,
              "validation": VALID_DAYS, "exploratory_diagnostics": DIAG_DAYS,
              "new_blind_test": False, "tau_ms": args.tau_ms,
              "signal_delay_ms": args.delay_ms, "basis_seconds": args.basis_seconds,
              "max_age_ms": 250, "basis_min_fresh_coverage_fraction": 0.75,
              "duration_minutes": args.minutes}
    out = Path(args.report_dir) / "event_gap_regression_exploratory.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    try:
        all_days = {}
        summaries = {}
        for day in days:
            start_us = int(datetime.strptime(day, "%Y-%m-%d").replace(
                tzinfo=timezone.utc).timestamp()) * 1_000_000
            end_us = start_us + args.minutes * 60 * 1_000_000
            ot, om = read_okx_bbo(day, end_us)
            bt_local, bm = read_binance_bbo(day, end_us)
            bt = visible_timestamps(bt_local, args.delay_ms)
            samples = event_gap_samples(ot, om, bt, bm,
                                        basis_seconds=args.basis_seconds,
                                        tau_ms=args.tau_ms)
            all_days[day] = samples
            summaries[day] = {"n_okx_quotes": int(len(ot)), "n_binance_quotes": int(len(bt)),
                              "n_causal_decisions": int(len(samples["t_ns"])),
                              "n_okx_trigger": int(np.count_nonzero(samples["source"] == 0)),
                              "n_binance_trigger": int(np.count_nonzero(samples["source"] == 1))}
        train = join_samples([all_days[d] for d in TRAIN_DAYS])
        fitted = fit_beta(train)
        report["fit"] = fitted
        report["day_counts"] = summaries
        report["scores"] = {}
        report["within_day_fit_DESCRIPTIVE_ONLY"] = {}
        for day in days:
            parts = all_days[day]
            report["scores"][day] = {"combined": score(parts, fitted)}
            # Never select or update fitted beta using the evaluation day's labels.
            report["within_day_fit_DESCRIPTIVE_ONLY"][day] = {
                "combined": fit_beta(parts)
            }
            for src, tag in ((0, "okx_trigger"), (1, "binance_trigger")):
                mask = parts["source"] == src
                if np.any(mask):
                    one = subset(parts, mask)
                    report["scores"][day][tag] = score(one, fitted)
                    report["within_day_fit_DESCRIPTIVE_ONLY"][day][tag] = fit_beta(one)
        report["status"] = "complete"
    except Exception as e:
        report["status"] = "failed"
        report["error"] = f"{type(e).__name__}: {e}"
        raise
    finally:
        out.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n",
                       encoding="utf-8")
        print("event gap regression:", report["status"], out, flush=True)


if __name__ == "__main__":
    main()
