"""Paired P1/P2 prediction only at actual Binance native-BBO price-change events.

P1 uses only OKX information available before decision time; P2 adds sign
and signed size of the *current Binance price-change event*. Target is the
future OKX native midpoint return. Date-level summaries, not event p-values.
"""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from binance_price_events import select_binance_events
from inspect_tardis import download
from predict_leadlag import FixedRidge, read_quotes

TRAIN = ("2026-06-01", "2026-07-01", "2026-08-01")
VALID = ("2026-09-01",)
TEST = ("2026-10-01", "2026-11-01")
HORIZONS_MS = (20, 50, 100, 200, 500, 1000)
DELAYS_MS = (0, 25, 50, 100)
MAX_OKX_QUOTE_AGE_US = 2_000_000
LOOKBACK_1_US = 100_000
LOOKBACK_2_US = 500_000


def event_samples(okx, events, start_us, delay_ms=0):
    """Returns EXACTLY the same decision rows and labels for P1 and P2.

    Strictly exclude simultaneous OKX quotes with decision-time timestamp
    because their cross-venue order cannot be established by microseconds.
    """
    assert delay_ms >= 0
    bt = np.concatenate((events["up"]["t"], events["down"]["t"]))
    sign = np.concatenate((events["up"]["sign"], events["down"]["sign"]))
    signed_size = np.concatenate((events["up"]["impact_bps"], events["down"]["impact_bps"]))
    order = np.argsort(bt, kind="stable")
    bt, sign, signed_size = bt[order], sign[order], signed_size[order]
    decision_t = bt + delay_ms * 1000
    ot, om = okx["t"], okx["mid"]
    now = np.searchsorted(ot, decision_t, side="left") - 1
    # A simultaneous OKX quote may or may not have been received before the
    # Binance event: drop the ambiguous event rather than assigning its move.
    simultaneous = np.searchsorted(ot, decision_t, side="right") != np.searchsorted(ot, decision_t, side="left")
    prior_100 = np.searchsorted(ot, decision_t - LOOKBACK_1_US, side="right") - 1
    prior_500 = np.searchsorted(ot, decision_t - LOOKBACK_2_US, side="right") - 1
    good = (
        (decision_t >= start_us + 1_000_000)
        & (decision_t + max(HORIZONS_MS) * 1000 < start_us + 86_400_000_000)
        & (now >= 0) & (prior_100 >= 0) & (prior_500 >= 0)
        & (~simultaneous)
    )
    quote_age = decision_t - ot[np.maximum(now, 0)]
    good &= (quote_age >= 0) & (quote_age <= MAX_OKX_QUOTE_AGE_US)
    bt, decision_t, sign, signed_size, now, prior_100, prior_500, quote_age = (
        arr[good] for arr in (bt, decision_t, sign, signed_size, now, prior_100, prior_500, quote_age)
    )
    if len(bt) < 10:
        raise RuntimeError(f"Only {len(bt)} events after availability filters")
    local = np.column_stack((
        10_000 * (om[now] / om[prior_100] - 1),
        10_000 * (om[now] / om[prior_500] - 1),
        okx["obi"][now],
        okx["spread"][now],
        quote_age / 1_000,  # Local quote age, milliseconds.
    ))
    combined = np.column_stack((local, sign, signed_size))
    # Target may use later data, features may not.
    y = np.column_stack([
        10_000 * (om[np.searchsorted(ot, decision_t + h * 1_000, side="right") - 1] / om[now] - 1)
        for h in HORIZONS_MS
    ])
    return {
        "x1": local.astype(np.float32),
        "x2": combined.astype(np.float32),
        "y": y.astype(np.float32),
        "sign": sign.astype(np.int8),
        "signed_size": signed_size.astype(np.float32),
        "n": len(bt),
        "excluded_simultaneous": int(np.count_nonzero(simultaneous)),
        "n_source_events": len(order),
    }


def metrics(day, samples, pred1, pred2):
    y = samples["y"].astype(np.float64)
    result = {"n_events": int(samples["n"]), "excluded_simultaneous": samples["excluded_simultaneous"],
              "source_price_events": samples["n_source_events"], "horizons": {}}
    sign = samples["sign"]
    for k, h in enumerate(HORIZONS_MS):
        truth, a, b = y[:, k], pred1[:, k], pred2[:, k]
        err_a, err_b = float(np.mean((truth - a) ** 2)), float(np.mean((truth - b) ** 2))
        nonzero = np.abs(truth) >= 0.1
        result["horizons"][str(h)] = {
            "mse_p1_bps2": err_a,
            "mse_p2_bps2": err_b,
            "mse_zero_bps2": float(np.mean(truth**2)),
            "improvement_pct": 100 * (err_a - err_b) / err_a if err_a > 0 else None,
            "nontrivial_label_pct": 100 * float(np.mean(nonzero)),
            "direction_p1_conditional_nontrivial_pct": 100 * float(np.mean(np.sign(a[nonzero]) == np.sign(truth[nonzero]))) if nonzero.any() else None,
            "direction_p2_conditional_nontrivial_pct": 100 * float(np.mean(np.sign(b[nonzero]) == np.sign(truth[nonzero]))) if nonzero.any() else None,
            "mean_signed_okx_response_bps": float(np.mean(truth * sign)),
            "mean_p1_predicted_bps": float(np.mean(a)),
            "mean_p2_predicted_bps": float(np.mean(b)),
        }
    return result


def fit_compare(prepared):
    # All model parameters are fit on training days only.
    tr = [prepared[d] for d in TRAIN]
    model1 = FixedRidge().fit(np.concatenate([x["x1"] for x in tr]), np.concatenate([x["y"] for x in tr]))
    model2 = FixedRidge().fit(np.concatenate([x["x2"] for x in tr]), np.concatenate([x["y"] for x in tr]))
    report = {"training_events": sum(x["n"] for x in tr), "validation": {}, "exploratory_test": {}, "test_daily_equal_weight": {}}
    for title, days in (("validation", VALID), ("exploratory_test", TEST)):
        for day in days:
            s = prepared[day]
            report[title][day] = metrics(day, s, model1.predict(s["x1"]), model2.predict(s["x2"]))
    for h in HORIZONS_MS:
        vals = [report["exploratory_test"][d]["horizons"][str(h)] for d in TEST]
        report["test_daily_equal_weight"][str(h)] = {
            "mse_p1_bps2": float(np.mean([x["mse_p1_bps2"] for x in vals])),
            "mse_p2_bps2": float(np.mean([x["mse_p2_bps2"] for x in vals])),
            "mean_daily_improvement_pct": float(np.mean([x["improvement_pct"] for x in vals])),
            "days_p2_better": sum(x["mse_p2_bps2"] < x["mse_p1_bps2"] for x in vals),
        }
    return report


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="data/raw")
    ap.add_argument("--report-dir", default="reports")
    args = ap.parse_args()
    report = {
        "status": "started",
        "purpose": "Binance event-restricted incremental out-of-sample OKX midprice prediction",
        "source": "Tardis native book_ticker; Binance event stream and OKX target both BBO",
        "selection": "first Binance native midpoint price CHANGE in each 20ms bucket, file order",
        "training_days": TRAIN, "validation_days": VALID, "exploratory_test_days": TEST,
        "horizons_ms": HORIZONS_MS, "delays_ms": DELAYS_MS,
        "features_p1": ["OKX mid return 100ms bps", "OKX mid return 500ms bps",
                        "OKX best-level queue imbalance", "OKX spread bps", "OKX last quote age ms"],
        "features_p2_extra": ["Binance price-change direction (+1/-1)", "Binance signed price-change size bps"],
        "estimator": "Ridge regression, fixed regularization 0.01, train-only standardization",
        "clock": "Tardis local_timestamp; simultaneous OKX updates at event decision timestamp excluded",
        "sensitivity": {},
        "limitations": [
            "June-November dates have previously been viewed; results are exploratory, not a fresh blind holdout.",
            "P1 and P2 share Binance-price-event sample selection; P1 cannot independently trigger on such events.",
            "Tardis collector cross-venue reception paths still differ; extra delay is a hypothetical sensitivity.",
            "Overlapping high-frequency outcomes are dependent; date count is six, exploratory test only two.",
            "An improvement in MSE is not evidence of executable or profitable GLFT quotes.",
            "Price-event labels and feature magnitudes can be affected by tick size and differing BBO frequencies.",
        ],
    }
    outpath = Path(args.report_dir) / "price_event_p1_p2_native_bbo_2026-06_to_11.json"
    outpath.parent.mkdir(parents=True, exist_ok=True)
    try:
        raw = {}
        days = TRAIN + VALID + TEST
        for d in days:
            folder = Path(args.data_dir) / d
            prepared = {}
            for venue, exchange, symbol in (("okx", "okex-swap", "BTC-USDT-SWAP"),
                                            ("binance", "binance-futures", "BTCUSDT")):
                path = folder / f"{exchange}_{symbol}_book_ticker.csv.gz"
                if not path.is_file():
                    print(f"Downloading {d}: {venue}", flush=True)
                    download(exchange, symbol, "book_ticker", d, path)
                prepared[venue] = path
            start_us = int(datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1e6)
            print(f"Loading OKX: {d}", flush=True)
            okx = read_quotes(prepared["okx"])
            print(f"Scanning Binance price events: {d}", flush=True)
            events, counts = select_binance_events(prepared["binance"])
            raw[d] = {"okx": okx, "events": events, "counts": counts, "start_us": start_us}
            print(f"  {d}: price events {counts['selected_up']+counts['selected_down']:,}", flush=True)
        for delay in DELAYS_MS:
            print(f"Training paired models at Binance signal delay {delay}ms", flush=True)
            prepared = {
                d: event_samples(raw[d]["okx"], raw[d]["events"], raw[d]["start_us"], delay_ms=delay)
                for d in days
            }
            result = fit_compare(prepared)
            result["event_counts"] = {d: {
                "up": int(raw[d]["counts"]["selected_up"]),
                "down": int(raw[d]["counts"]["selected_down"]),
                "used": int(prepared[d]["n"]),
                "ambiguous_simultaneous_excluded": prepared[d]["excluded_simultaneous"],
            } for d in days}
            report["sensitivity"][str(delay)] = result
            for h in (20, 100, 500, 1000):
                score = result["test_daily_equal_weight"][str(h)]
                print(f"  {h:4d}ms: P1 mse={score['mse_p1_bps2']:.6f}, "
                      f"P2 mse={score['mse_p2_bps2']:.6f}; "
                      f"daily Δ={score['mean_daily_improvement_pct']:+.4f}%, "
                      f"days better={score['days_p2_better']}/2", flush=True)
        report["status"] = "complete"
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        outpath.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"Report: {outpath}, status: {report['status']}", flush=True)


if __name__ == "__main__":
    main()
