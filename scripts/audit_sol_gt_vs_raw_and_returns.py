"""Independent post-hoc audit of recorded Binance->OKX SOL G signal.

Read six archived 100ms snapshots; NO change to market-making logic.
Compare unadjusted spread, baseline, residual G, and realized single-venue
moves. Future price movements are used for descriptive audit ONLY, never
passed into strategy decisions.
"""
import argparse
import json
from pathlib import Path

import numpy as np

DAYS = tuple(f"2026-{m:02d}-01" for m in range(6, 12))
FIELDS = ("timestamp_ns", "valid", "G_usdt_per_sol",
          "G_okx_sol_ticks", "okx_mid_usdt_per_sol",
          "binance_mid_usdt_per_sol", "baseline_usdt_per_sol",
          "okx_quote_age_ms", "binance_quote_age_ms")
HORIZONS = {"100ms": 1, "500ms": 5, "1s": 10, "10s": 100}
TEN_BPS = 10.0


def arr_stats(x):
    x = np.asarray(x, dtype=np.float64)
    x = x[np.isfinite(x)]
    if not len(x):
        return {"n": 0}
    return {"n": int(len(x)), "mean": float(np.mean(x)),
            "p50": float(np.percentile(x, 50)),
            "p90": float(np.percentile(x, 90)),
            "p99": float(np.percentile(x, 99)),
            "max": float(np.max(x)),
            "fraction_ge_10bps": float(np.mean(x >= TEN_BPS))}


def get_day(path):
    with np.load(path) as archive:
        if set(FIELDS) - set(archive.files):
            raise ValueError(f"Missing SOL original fields in {path}")
        a = {k: archive[k].copy() for k in FIELDS}
    if len(a["valid"]) != 30000:
        raise ValueError(f"Unexpected fixed 100ms sample count: {path}")
    if np.any(np.diff(a["timestamp_ns"]) != 100_000_000):
        raise ValueError("Not fixed 100ms timeline")
    valid = a["valid"]
    okx = a["okx_mid_usdt_per_sol"]
    binance = a["binance_mid_usdt_per_sol"]
    basis = a["baseline_usdt_per_sol"]
    gap = a["G_usdt_per_sol"]
    if np.any(~np.isfinite(okx[valid])) or np.any(~np.isfinite(binance[valid])):
        raise ValueError("Invalid original quote prices")
    if np.any((okx[valid] < 20) | (okx[valid] > 500)):
        raise ValueError("Possible SOL price currency mismatch")
    np.testing.assert_allclose(binance[valid] - okx[valid] - basis[valid],
                               gap[valid], atol=1e-12, rtol=0)
    np.testing.assert_allclose(gap[valid] / .01, a["G_okx_sol_ticks"][valid],
                               atol=1e-12, rtol=0)
    spread = binance - okx
    metrics = {
        "n_clock": int(len(valid)), "n_valid": int(valid.sum()),
        "valid_fraction": float(np.mean(valid)),
        "sol_okx_mid_range": [float(np.min(okx[valid])), float(np.max(okx[valid]))],
        "sol_binance_mid_range": [float(np.min(binance[valid])),
                                 float(np.max(binance[valid]))],
        "sol_okx_mid_high_low_bps": float(
            10000*(np.max(okx[valid])-np.min(okx[valid]))/
            np.median(okx[valid])),
        "raw_spread_bps": arr_stats(10000*np.abs(spread[valid])/okx[valid]),
        "baseline_bps": arr_stats(10000*np.abs(basis[valid])/okx[valid]),
        "residual_G_bps": arr_stats(10000*np.abs(gap[valid])/okx[valid]),
        "raw_spread_usdt_per_sol": arr_stats(np.abs(spread[valid])),
        "baseline_usdt_per_sol": arr_stats(np.abs(basis[valid])),
        "residual_G_usdt_per_sol": arr_stats(np.abs(gap[valid])),
        "okx_quote_age_ms": arr_stats(a["okx_quote_age_ms"][valid]),
        "binance_quote_age_ms": arr_stats(a["binance_quote_age_ms"][valid]),
        "realized_mid_moves_bps": {},
    }
    for label, steps in HORIZONS.items():
        eligible = valid[:-steps] & valid[steps:]
        if not np.any(eligible):
            metrics["realized_mid_moves_bps"][label] = {"okx":{"n":0}, "binance":{"n":0}}
            continue
        a_okx = 10000*np.abs(okx[steps:][eligible]-okx[:-steps][eligible])/okx[:-steps][eligible]
        a_bin = 10000*np.abs(binance[steps:][eligible]-binance[:-steps][eligible])/binance[:-steps][eligible]
        metrics["realized_mid_moves_bps"][label] = {
            "okx": arr_stats(a_okx),
            "binance": arr_stats(a_bin),
        }
    return metrics, a


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--source", default="archived")
    p.add_argument("--out", default="reports/okx_sol_signal_definition_audit.json")
    args = p.parse_args()
    days = []
    arrays = []
    for day in DAYS:
        info, data = get_day(Path(args.source)/f"{day}_100ms_G_SOL.npz")
        info["day"] = day
        days.append(info)
        arrays.append(data)
    if sum(d["n_valid"] for d in days) != 80127:
        raise ValueError("Six-day valid count does not match recorded study")
    # Pool all valid snapshots equally in time, as the original study.
    allraw, allbasis, allg = [], [], []
    futures = {label: {"okx": [], "binance": []} for label in HORIZONS}
    for x in arrays:
        v=x["valid"];o=x["okx_mid_usdt_per_sol"];b=x["binance_mid_usdt_per_sol"]
        allraw.append(10000*np.abs((b-o)[v])/o[v])
        allbasis.append(10000*np.abs(x["baseline_usdt_per_sol"][v])/o[v])
        allg.append(10000*np.abs(x["G_usdt_per_sol"][v])/o[v])
        for label,steps in HORIZONS.items():
            elig=v[:-steps] & v[steps:]
            futures[label]["okx"].append(
                10000*np.abs(o[steps:][elig]-o[:-steps][elig])/o[:-steps][elig])
            futures[label]["binance"].append(
                10000*np.abs(b[steps:][elig]-b[:-steps][elig])/b[:-steps][elig])
    pooled={
        "raw_spread_bps":arr_stats(np.concatenate(allraw)),
        "baseline_bps":arr_stats(np.concatenate(allbasis)),
        "residual_G_bps":arr_stats(np.concatenate(allg)),
        "realized_mid_moves_bps":{
            label:{
                market:arr_stats(np.concatenate(vectors))
                for market,vectors in vals.items()
            } for label,vals in futures.items()
        },
    }
    report={"scope":"SOL Binance futures -> OKX SOL USDT swap, 2.5ms, 100ms grid",
            "units":"Prices USDT/SOL, price gap USDT/SOL, bps=10000*price difference/OKX or own mid",
            "threshold_of_interest_bps":10.0,
            "day_reports":days,"pooled":pooled}
    path=Path(args.out);path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(report,indent=2,ensure_ascii=False)+"\n",
                    encoding="utf-8")
    print("UNIT_AND_G_RECONCILIATION_PASS",len(days),"DAYS",flush=True)
    print("POOLED",json.dumps(pooled,ensure_ascii=False),flush=True)
    for d in days:
        print("DAY",d["day"],"SOL_OKX_RANGE",d["sol_okx_mid_range"],
              "HIGH_LOW_BPS",d["sol_okx_mid_high_low_bps"],
              "RAW_SPREAD_P90_BPS",d["raw_spread_bps"]["p90"],
              "G_MAX_BPS",d["residual_G_bps"]["max"],
              "OWN_100MS_P99_BPS",
              d["realized_mid_moves_bps"]["100ms"]["okx"]["p99"],flush=True)


if __name__=="__main__":
    main()
