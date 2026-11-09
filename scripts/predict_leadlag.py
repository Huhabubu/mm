"""Exploratory BTC cross-venue alpha test on free first-of-month Tardis quotes.

Uses OKX QUOTE EVENTS as decision clocks (first event in each 20ms bucket),
never future Binance events. Test months are never used to fit parameters.
Data are temporary on Actions; only aggregated error metrics are published.
"""
import argparse
import csv
import gzip
import json
import sys
from array import array
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
from inspect_tardis import download

TRAIN_DAYS = ("2026-06-01", "2026-07-01", "2026-08-01")
VALID_DAYS = ("2026-09-01",)
TEST_DAYS = ("2026-10-01", "2026-11-01")
HORIZONS_MS = (20, 50, 100, 200, 500, 1000, 2000, 5000)
LOOKBACK_US = (100_000, 500_000)
DECISION_BUCKET_US = 20_000
MAX_REMOTE_AGE_US = 2_000_000
RIDGE_REG = 0.01  # Fixed in advance. No tuning on test days.


def read_quotes(path):
    """Read a sorted compressed Tardis quote file; retain only numeric fields."""
    ts, mids, imbalance, spreads = array("q"), array("d"), array("d"), array("d")
    prior = -1
    with gzip.open(path, "rt", newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        for row in reader:
            t = int(row["local_timestamp"])
            if t < prior:
                raise ValueError(f"non-monotonic local_timestamp in {path}")
            prior = t
            bid, ask = float(row["bid_price"]), float(row["ask_price"])
            bqty, aqty = float(row["bid_amount"]), float(row["ask_amount"])
            if not (0 < bid < ask and bqty >= 0 and aqty >= 0):
                raise ValueError(f"invalid quote in {path} at {t}")
            mid = (bid + ask) / 2
            ts.append(t)
            mids.append(mid)
            imbalance.append((bqty - aqty) / (bqty + aqty) if bqty + aqty else 0.0)
            spreads.append(10_000 * (ask - bid) / mid)
    if not ts:
        raise ValueError(f"empty quote file {path}")
    return {
        "t": np.asarray(ts, dtype=np.int64),
        "mid": np.asarray(mids, dtype=np.float64),
        "obi": np.asarray(imbalance, dtype=np.float64),
        "spread": np.asarray(spreads, dtype=np.float64),
    }


def asof(times, requested):
    """Index of latest event <= requested timestamp; -1 if none."""
    return np.searchsorted(times, requested, side="right") - 1


def make_samples(okx, binance, day_start_us, delay_ms=0, clock="okx"):
    """Paired P1/P2 on real quote events, preserving time-of-availability."""
    if clock not in ("okx", "binance", "grid"):
        raise ValueError("clock must be okx, binance or grid")
    ot, bt = okx["t"], binance["t"]
    if clock == "grid":
        # Fixed, venue-independent 100ms decision times with predeclared 37ms phase.
        # Unlike Binance event sampling, this does not condition the test clock on a Binance tick.
        event_t = np.arange(
            day_start_us + 1_037_000, day_start_us + 86_400_000_000,
            100_000, dtype=np.int64
        )
        o_now = asof(ot, event_t)
    else:
        trigger = ot if clock == "okx" else bt
        chosen = np.flatnonzero(np.r_[True, trigger[1:] // DECISION_BUCKET_US != trigger[:-1] // DECISION_BUCKET_US])
        # Extra delay shifts WHEN an incoming Binance event triggers a decision.
        event_t = trigger[chosen] + (int(delay_ms * 1000) if clock == "binance" else 0)
        o_now = chosen if clock == "okx" else asof(ot, event_t)
    b_cutoff = event_t - int(delay_ms * 1000)
    b_now = asof(bt, b_cutoff)
    o_100 = asof(ot, event_t - LOOKBACK_US[0])
    o_500 = asof(ot, event_t - LOOKBACK_US[1])
    b_100 = asof(bt, b_cutoff - LOOKBACK_US[0])
    b_500 = asof(bt, b_cutoff - LOOKBACK_US[1])
    day_end_us = day_start_us + 86_400_000_000
    mask = (
        (event_t >= day_start_us + 1_000_000)
        & (event_t + max(HORIZONS_MS) * 1000 < day_end_us)
        & (o_now >= 0) & (b_now >= 0) & (o_100 >= 0) & (o_500 >= 0)
        & (b_100 >= 0) & (b_500 >= 0)
    )
    # Index arrays must be clipped before fancy indexing; exclude invalid rows via mask.
    age_b = event_t - bt[np.maximum(b_now, 0)]
    age_o = event_t - ot[np.maximum(o_now, 0)]
    mask &= (age_b >= 0) & (age_b <= MAX_REMOTE_AGE_US)
    mask &= (age_o >= 0) & (age_o <= MAX_REMOTE_AGE_US)
    event_t, o_now, b_now, o_100, o_500, b_100, b_500 = (
        arr[mask] for arr in (event_t, o_now, b_now, o_100, o_500, b_100, b_500)
    )
    om, bm = okx["mid"], binance["mid"]
    x_local = np.column_stack([
        10_000 * (om[o_now] / om[o_100] - 1),
        10_000 * (om[o_now] / om[o_500] - 1),
        okx["obi"][o_now],
        okx["spread"][o_now],
    ])
    x_joint = np.column_stack([
        x_local,
        10_000 * (bm[b_now] / bm[b_100] - 1),
        10_000 * (bm[b_now] / bm[b_500] - 1),
        binance["obi"][b_now],
        binance["spread"][b_now],
    ])
    # AS-OF targets: future return at exactly t+h, carrying forward last OKX quote.
    # Targets can use future OKX information; FEATURES cannot.
    y = np.column_stack([
        10_000 * (om[asof(ot, event_t + h * 1000)] / om[o_now] - 1)
        for h in HORIZONS_MS
    ])
    return {
        "local": x_local.astype(np.float32),
        "joint": x_joint.astype(np.float32),
        "y": y.astype(np.float32),
        "rows": int(len(event_t)),
    }


class FixedRidge:
    """Z-score on training rows; solve multi-output ridge including free intercept."""
    def fit(self, x, y):
        x, y = x.astype(np.float64), y.astype(np.float64)
        self.x_mean = x.mean(axis=0)
        self.x_std = x.std(axis=0)
        self.x_std[self.x_std < 1e-12] = 1.0
        self.y_mean = y.mean(axis=0)
        z = (x - self.x_mean) / self.x_std
        n, p = z.shape
        gram = z.T @ z + (n * RIDGE_REG) * np.eye(p)
        self.coef = np.linalg.solve(gram, z.T @ (y - self.y_mean))
        return self

    def predict(self, x):
        return (x.astype(np.float64) - self.x_mean) / self.x_std @ self.coef + self.y_mean


def daily_scores(y, p1, p2):
    """Assess paired errors by day, not misleading per-tick significance tests."""
    out = {}
    for col, horizon in enumerate(HORIZONS_MS):
        truth, a, b = y[:, col], p1[:, col], p2[:, col]
        mse_1 = float(np.mean((truth - a) ** 2))
        mse_2 = float(np.mean((truth - b) ** 2))
        directional = np.abs(truth) >= 0.1  # Direction conditional on >=0.1 bp move.
        out[str(horizon)] = {
            "count": len(truth),
            "mse_okx_bps2": mse_1,
            "mse_joint_bps2": mse_2,
            "mse_zero_bps2": float(np.mean(truth ** 2)),
            "relative_improvement_pct": 100 * (mse_1 - mse_2) / mse_1 if mse_1 > 0 else None,
            "nontrivial_direction_count": int(directional.sum()),
            "direction_okx": float(np.mean(np.sign(a[directional]) == np.sign(truth[directional]))) if directional.any() else None,
            "direction_joint": float(np.mean(np.sign(b[directional]) == np.sign(truth[directional]))) if directional.any() else None,
        }
    return out


def run_fit(samples):
    """Fit on 3 early days, report 1 validation and 2 untouched test days."""
    tr = [samples[d] for d in TRAIN_DAYS]
    x1 = np.concatenate([r["local"] for r in tr])
    x2 = np.concatenate([r["joint"] for r in tr])
    y_train = np.concatenate([r["y"] for r in tr])
    model_1 = FixedRidge().fit(x1, y_train)
    model_2 = FixedRidge().fit(x2, y_train)
    summary = {"training_rows": int(len(y_train)), "validation": {}, "test": {}}
    for category, days in (("validation", VALID_DAYS), ("test", TEST_DAYS)):
        for day in days:
            row = samples[day]
            p1 = model_1.predict(row["local"])
            p2 = model_2.predict(row["joint"])
            summary[category][day] = daily_scores(row["y"], p1, p2)
    summary["test_equal_day_mean"] = {}
    for h in HORIZONS_MS:
        records = [summary["test"][d][str(h)] for d in TEST_DAYS]
        summary["test_equal_day_mean"][str(h)] = {
            "mse_okx_bps2": float(np.mean([r["mse_okx_bps2"] for r in records])),
            "mse_joint_bps2": float(np.mean([r["mse_joint_bps2"] for r in records])),
            "daily_relative_improvement_pct": float(np.mean([r["relative_improvement_pct"] for r in records])),
            "days_better_joint": sum(r["mse_joint_bps2"] < r["mse_okx_bps2"] for r in records),
        }
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--delays-ms", default="0,25,50,100")
    parser.add_argument("--clock", choices=("okx", "binance", "grid"), default="binance")
    parser.add_argument("--data-dir", default="data/raw")
    parser.add_argument("--report-dir", default="reports")
    args = parser.parse_args()
    delays = tuple(int(v) for v in args.delays_ms.split(","))
    if not delays or any(d < 0 for d in delays):
        parser.error("delays must be nonnegative millisecond integers")
    day_list = TRAIN_DAYS + VALID_DAYS + TEST_DAYS
    raw = {}
    downloads = {}
    for day in day_list:
        print(f"Downloading & preparing {day}", flush=True)
        yyyy, mm, dd = day.split("-")
        downloads[day] = {}
        venues = {}
        for name, exchange, symbol in (
            ("okx", "okex-swap", "BTC-USDT-SWAP"),
            ("binance", "binance-futures", "BTCUSDT"),
        ):
            dest = Path(args.data_dir) / day / f"{exchange}_{symbol}_quotes.csv.gz"
            info = download(exchange, symbol, "quotes", day, dest)
            downloads[day][name] = {
                "compressed_bytes": info["compressed_bytes"],
                "sha256_gzip": info["sha256_gzip"],
            }
            venues[name] = read_quotes(dest)
            print(f"  {name}: {len(venues[name]['t']):,} quote events", flush=True)
        raw[day] = venues
    report = {
        "status": "complete",
        "experiment": "pilot_predict_quote_features",
        "decision_clock": args.clock,
        "days": {"train": TRAIN_DAYS, "validation": VALID_DAYS, "test": TEST_DAYS},
        "target": "OKX as-of future mid return, bps",
        "features_p1": ["OKX 100ms return", "OKX 500ms return", "OKX L1 imbalance", "OKX spread bps"],
        "features_p2_extra": ["Binance 100ms return", "Binance 500ms return", "Binance L1 imbalance", "Binance spread bps"],
        "horizons_ms": HORIZONS_MS,
        "decision": ("venue-independent 100ms grid with 37ms phase" if args.clock == "grid" else f"first {args.clock} quote event in each 20ms bucket; Binance trigger shifted by extra feed delay"),
        "time_basis": "Tardis local_timestamp per venue, may reflect non-co-located collectors",
        "binance_extra_delay_ms": delays,
        "max_remote_quote_age_ms": MAX_REMOTE_AGE_US / 1000,
        "ridge_penalty_fixed": RIDGE_REG,
        "download_metadata": downloads,
        "sensitivity": {},
        "limitations": [
            "Only two OOS dates; do not assert statistically significant alpha.",
            "P1 evaluated at common Binance-trigger times when clock=binance, so event timing is conditioned on Binance activity.",
            "Grid-clock tests use a fixed 100ms grid with a 37ms phase; different fixed phases can change microstructure results.",
            "No simulation of order execution, fees, live colocated network clocks, or trading profitability.",
            "Individual high-frequency events overlap; no per-tick p-values.",
            "Quotes only; trade flow and L2 not included in this pilot.",
            "Binance timestamps delayed in sensitivity, but collector-site time offset remains unidentified.",
        ],
    }
    for delay in delays:
        print(f"Fitting quote alpha; clock={args.clock}; Binance delayed {delay}ms", flush=True)
        samples = {}
        for day in day_list:
            start = int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1e6)
            venues = raw[day]
            samples[day] = make_samples(venues["okx"], venues["binance"], start, delay, clock=args.clock)
            if samples[day]["rows"] < 100:
                raise RuntimeError(f"Too few samples: {day} delay={delay}ms")
            print(f"  {day}: {samples[day]['rows']:,} paired samples", flush=True)
        result = run_fit(samples)
        result["rows_by_day"] = {d: samples[d]["rows"] for d in day_list}
        report["sensitivity"][str(delay)] = result
        for h in (20, 100, 500, 1000, 5000):
            s = result["test_equal_day_mean"][str(h)]
            print(
                f"  {h:4d}ms: equal-day mse P1 {s['mse_okx_bps2']:.6f} "
                f"P2 {s['mse_joint_bps2']:.6f}; "
                f"daily avg change {s['daily_relative_improvement_pct']:+.3f}% "
                f"({s['days_better_joint']}/2 test days improved)",
                flush=True,
            )
    dest = Path(args.report_dir) / f"prediction_pilot_btc_clock_{args.clock}_2026-06_to_11.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Saved aggregate summary: {dest}", flush=True)


if __name__ == "__main__":
    main()
