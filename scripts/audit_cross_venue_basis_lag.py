"""Causal cross-venue basis lag audit: read-only, native BBO, no simulated orders.

Compare the deployed 60s basis with a 300s diagnostic and raw spread, at
identical decisions (UTC 00:10--01:00), visibility and future-label clocks.
A 300s rolling basis is only an alternative proxy, not ground-truth equilibrium.
"""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np

from analyze_binance_okx_gt_distribution import CLOCK_STEP_NS, MAX_QUOTE_AGE_NS, NS
from check_okx_hft_data import extract_window
from cross_venue_event_probe import read_binance_bbo, visible_timestamps
from run_beta_one_direct_gap import causal_60s_basis, read_local_bbo

MARKETS = {
    "BTC": ("BTCUSDT", "BTC-USDT-SWAP", 0.1),
    "SOL": ("SOLUSDT", "SOL-USDT-SWAP", 0.01),
}
HORIZONS_MS = (5, 10, 25, 50)
WARMUP_SECONDS = 600
END_SECONDS = 3600


def past_indices(times, clock, include_equal=False):
    """as-of, with an explicit equality convention."""
    return np.searchsorted(times, clock, side="right" if include_equal else "left") - 1


def aligned_series(clock, ot, om, bt, bm, grid60, basis60, grid300, basis300):
    """Never use information beyond the observation clock in any signal."""
    if not np.array_equal(grid60, grid300):
        raise ValueError("Basis grids are inconsistent")
    oi = past_indices(ot, clock)
    bi = past_indices(bt, clock, include_equal=True)
    gi = past_indices(grid60, clock)
    safe_o, safe_b, safe_g = [np.maximum(x, 0) for x in (oi, bi, gi)]
    age_o = clock - ot[safe_o]
    age_b = clock - bt[safe_b]
    good = ((oi >= 0) & (bi >= 0) & (gi >= 0) &
            (age_o >= 0) & (age_o <= MAX_QUOTE_AGE_NS) &
            (age_b >= 0) & (age_b <= MAX_QUOTE_AGE_NS) &
            np.isfinite(basis60[safe_g]) & np.isfinite(basis300[safe_g]))
    spread = bm[safe_b] - om[safe_o]
    return {
        "valid": good,
        "d": spread,
        "g60": spread - basis60[safe_g],
        "g300": spread - basis300[safe_g],
        "raw": spread.copy(),
        "b60": basis60[safe_g],
        "b300": basis300[safe_g],
        "okx_mid": om[safe_o],
        "okx_age_ms": age_o / 1e6,
        "binance_age_ms": age_b / 1e6,
    }


def future_okx_bps(clock, horizon_ms, ot, om, current_mid):
    """Target may use later quotes; feature extraction must never use this output."""
    target = clock + int(horizon_ms * 1_000_000)
    i = past_indices(ot, target, include_equal=True)
    safe = np.maximum(i, 0)
    age = target - ot[safe]
    valid = (i >= 0) & (age >= 0) & (age <= MAX_QUOTE_AGE_NS)
    label = (om[safe] - current_mid) / current_mid * 10000.0
    return valid, label


def diagnostic_metrics(series, tick):
    """Proxy for 'near long-window basis but 60s residual still large'.

    The 300s basis is also trailing and can be shock-contaminated; this
    count never proves the 60s signal was economically false.
    """
    valid = series["valid"]
    near = np.abs(series["g300"]) <= tick + 1e-10
    strong = np.abs(series["g60"]) >= 2 * tick - 1e-10
    candidate = valid & near & strong
    conflict = (valid & (np.abs(series["g300"]) >= 0.5 * tick) &
                (series["g60"] * series["g300"] < 0))
    n = int(np.count_nonzero(valid))
    return {
        "joint_valid_100ms_points": n,
        "joint_valid_fraction": float(np.mean(valid)),
        "near_300s_but_strong_60s_points": int(np.count_nonzero(candidate)),
        "near_300s_but_strong_60s_fraction_given_valid": (
            float(np.count_nonzero(candidate) / n) if n else None),
        "opposite_60s_vs_300s_residual_points": int(np.count_nonzero(conflict)),
        "opposite_60s_vs_300s_fraction_given_valid": (
            float(np.count_nonzero(conflict) / n) if n else None),
        "basis60_minus_basis300_abs_median_ticks": (
            float(np.median(np.abs(series["b60"][valid]-series["b300"][valid])/tick))
            if n else None),
    }


def prediction_table(clock, ot, om, series, tick):
    """Paired fixed decisions, fixed future labels, no model/threshold fitting."""
    out = {}
    for horizon in HORIZONS_MS:
        endpoint, label = future_okx_bps(
            clock, horizon, ot, om, series["okx_mid"])
        valid = series["valid"] & endpoint & np.isfinite(label)
        moving = valid & (np.abs(label) > 1e-10)
        row = {"joint_valid_points": int(valid.sum()),
               "nonzero_future_price_moves": int(moving.sum()),
               "variants": {}}
        for name in ("g60", "g300", "raw"):
            signal = series[name]
            # Include zero future returns to avoid conditioning away no-move periods.
            score = np.sign(signal[valid]) * label[valid]
            considered = moving & (np.abs(signal) >= 0.5 * tick)
            row["variants"][name] = {
                "mean_signed_future_bps_all_valid": (
                    float(np.mean(score)) if len(score) else None),
                "median_signed_future_bps_all_valid": (
                    float(np.median(score)) if len(score) else None),
                "nonzero_label_and_signal_points": int(considered.sum()),
                "direction_hit_fraction_given_nonzero": (
                    float(np.mean(np.sign(signal[considered]) ==
                                  np.sign(label[considered])))
                    if np.any(considered) else None),
                "fraction_abs_signal_ge_half_tick_given_valid": (
                    float(np.mean(np.abs(signal[valid]) >= 0.5 * tick))
                    if np.any(valid) else None),
            }
        out[str(horizon)] = row
    return out


def run_one(day, coin, delay_ms):
    if coin not in MARKETS:
        raise ValueError("Unknown market")
    src, dst, tick = MARKETS[coin]
    first_us = int(datetime.fromisoformat(day).replace(
        tzinfo=timezone.utc).timestamp() * 1_000_000)
    end_us = first_us + END_SECONDS * 1_000_000 + 5_000_000
    with TemporaryDirectory(prefix="basis_lag_") as directory:
        csvpath = Path(directory) / "okx_bbo.csv"
        extract_window("book_ticker", day, end_us, csvpath, max_rows=3_000_000,
                       exchange="okex-swap", symbol=dst)
        ot, om = read_local_bbo(csvpath)
        bt_raw, bm = read_binance_bbo(day, end_us, symbol=src)
    bt = visible_timestamps(bt_raw, delay_ms)
    grid60, b60 = causal_60s_basis(ot, om, bt, bm, basis_seconds=60)
    grid300, b300 = causal_60s_basis(ot, om, bt, bm, basis_seconds=300)
    start_ns = first_us * 1000
    clock = np.arange(start_ns + WARMUP_SECONDS * NS,
                      start_ns + END_SECONDS * NS, CLOCK_STEP_NS, dtype=np.int64)
    series = aligned_series(clock, ot, om, bt, bm, grid60, b60, grid300, b300)
    if not np.any(series["valid"]):
        raise ValueError("No paired valid observations; do not report empty results")
    return {
        "day": day, "coin": coin, "exchange": "okx",
        "binance_extra_delay_ms": delay_ms,
        "target_tick_usdt_per_coin": tick,
        "source_bbo_count": {"binance": len(bt), "okx": len(ot)},
        "clock_points": len(clock), "clock_interval_ms": 100,
        "basis_60s_coverage": ">=45 valid of 60 completed-second samples",
        "basis_300s_coverage": ">=225 valid of 300 completed-second samples",
        "quote_freshness_ms": 250,
        "diagnostics": diagnostic_metrics(series, tick),
        "future_okx": prediction_table(clock, ot, om, series, tick),
        "interpretation_limits": [
            "300s trailing basis is an alternative benchmark, not an unbiased truth",
            "same six exploratory first-of-month dates, not independent test dates",
            "native OKX BBO proxy differs from target HBT merged-L2 quote mid",
            "Tardis local timestamps do not prove exchange-to-exchange visibility",
            "no HBT orders, fees, adverse-selection fill attribution, or PnL in this audit",
        ],
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--day", required=True)
    p.add_argument("--coin", choices=sorted(MARKETS), required=True)
    p.add_argument("--extra-binance-delay-ms", type=float, default=2.5)
    p.add_argument("--out", default="reports/basis_lag_audit")
    args = p.parse_args()
    if not (0 <= args.extra_binance_delay_ms <= 1000):
        p.error("Invalid Binance extra visibility delay")
    result = run_one(args.day, args.coin, args.extra_binance_delay_ms)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    key = f"{args.day}_{args.coin}_{str(args.extra_binance_delay_ms).replace('.', 'p')}ms"
    target = out / (key + ".json")
    target.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps({"result": str(target), "diagnostics": result["diagnostics"],
                      "future_10ms": result["future_okx"]["10"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
