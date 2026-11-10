"""Fixed six-month G0 queue-intensity time-stability diagnostic.

Each Tardis free 1st-of-month day: 00:00-00:10 UTC train, 00:10-00:20 holdout.
Official hftbacktest queue/latency/fill model, 10ms observation, 100ms quoting.
No changes to original G0 strategy, no PnL claims, no cross-exchange signals.
"""
import argparse
import csv
import json
import math
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from check_okx_hft_data import SCHEMAS, extract_window
from g0_glft_baseline import LOT, TICK
from g0_queue_intensity import (HALF_TICK_BINS, TARGET_TICKS,
                                check_holdout, estimate_poisson_curve,
                                pool_rows, simulate_window)

DAYS = (
    "2026-06-01", "2026-07-01", "2026-08-01",
    "2026-09-01", "2026-10-01", "2026-11-01",
)
BANDS = ((0.5, 2.0), (2.5, 10.0), (10.5, 35.0), (35.5, 100.0))


def validate_schedule(days):
    """Keep later-day tests chronologically after all fitting days."""
    if len(days) < 2 or tuple(sorted(set(days))) != tuple(days):
        raise ValueError("Need >=2 unique sorted historical days")
    for day in days:
        d = datetime.strptime(day, "%Y-%m-%d")
        if d.day != 1:
            raise ValueError("Expected 1st-of-month Tardis free samples")
    return tuple(days)


def relative_error(observed, predicted):
    return float((predicted - observed) / observed) if observed else None


def sum_training(exposures, fills):
    """Combine disjoint training dates; never pool holdout labels into the fit."""
    if not exposures or len(exposures) != len(fills):
        raise ValueError("Expected matching nonempty training days")
    e = np.sum(np.stack(exposures), axis=0)
    n = np.sum(np.stack(fills), axis=0)
    return e, n


def distance_band_errors(fit, exposure, fills):
    """Break down *later* period predictions on predeclared price-distance bands."""
    if fit.get("status") != "ok":
        return []
    e = np.asarray(exposure, dtype=float)
    n = np.asarray(fills, dtype=float)
    x = np.arange(len(e)) / 2.0
    rows = []
    for lower, upper in BANDS:
        active = ((x >= lower) & (x <= upper) & (e > 0) &
                  (x >= fit["fit_min_distance_ticks"]) &
                  (x <= fit["fit_max_distance_ticks"]))
        observed = int(n[active].sum())
        exposed = float(e[active].sum())
        predicted = float(np.sum(e[active] *
                         fit["A_fills_per_exposed_order_second"] *
                         np.exp(-fit["k_per_real_tick"] * x[active])))
        rows.append({"distance_range_ticks": [lower, upper],
                     "exposure_seconds": exposed,
                     "actual_matched_fills": observed,
                     "predicted_fills": predicted,
                     "relative_count_error": relative_error(observed, predicted)})
    return rows


def assessment(fit, exposure, fills, training_dates, evaluation_date, label):
    validation = check_holdout(fit, exposure, fills)
    summary = {"comparison": label, "training_dates": list(training_dates),
               "evaluation_date": evaluation_date, "fit": fit,
               "holdout_validation": validation}
    if validation["status"] == "ok":
        obs = validation["observed_fills"]
        pred = validation["predicted_fills"]
        summary["holdout_relative_count_error"] = relative_error(obs, pred)
        summary["holdout_constant_relative_count_error"] = relative_error(
            obs, validation["constant_baseline_predicted_fills"])
        summary["distance_bands"] = distance_band_errors(fit, exposure, fills)
    return summary


def day_replay(day):
    """Download only first 20m; all actual maker fills are simulated by official HBT."""
    from hftbacktest.data.utils import tardis
    from hftbacktest.data.validation import validate_event_order
    start = int(datetime.strptime(day, "%Y-%m-%d")
                .replace(tzinfo=timezone.utc).timestamp() * 1e6)
    with tempfile.TemporaryDirectory(prefix="mm_g0_temporal_") as tmp:
        root = Path(tmp)
        paths, stats = {}, {}
        for kind in SCHEMAS:
            paths[kind] = root / (kind + ".csv")
            stats[kind] = extract_window(kind, day, start + 1200*1_000_000,
                                         paths[kind], max_rows=1_200_000)
        npz = root / "events.npz"
        events = tardis.convert_fuse(
            str(paths["trades"]), str(paths["incremental_book_L2"]),
            str(paths["book_ticker"]), tick_size=TICK, lot_size=LOT,
            output_filename=str(npz),
            ss_buffer_size=max(1000, stats["incremental_book_L2"]["snapshot_rows"]+100),
            snapshot_mode="process")
        validate_event_order(events)
        n_events = int(len(events))
        del events
        low, high = math.inf, -math.inf
        with paths["book_ticker"].open(encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                low = min(low, float(row["bid_price"]))
                high = max(high, float(row["ask_price"]))
        if not (math.isfinite(low) and math.isfinite(high) and low < high):
            raise ValueError(f"{day} invalid market window")
        first, second = [], []
        for depth in TARGET_TICKS:
            first.append(simulate_window(npz, low, high, depth, "train", 10))
            second.append(simulate_window(npz, low, high, depth, "holdout", 10))
    train_e, train_n = pool_rows(first)
    hold_e, hold_n = pool_rows(second)
    for rec in first + second:
        rec.pop("exposure_by_half_tick_seconds")
        rec.pop("matched_fills_by_half_tick")
    summary = {"day": day,
               "event_count": n_events,
               "input_counts": stats,
               "train_total_engine_fills": sum(r["total_engine_fills"] for r in first),
               "train_matched_fills": int(train_n.sum()),
               "train_unmatched_fills": sum(r["unmatched_fills"] for r in first),
               "train_exposure_order_seconds": float(train_e.sum()),
               "holdout_total_engine_fills": sum(r["total_engine_fills"] for r in second),
               "holdout_matched_fills": int(hold_n.sum()),
               "holdout_unmatched_fills": sum(r["unmatched_fills"] for r in second),
               "holdout_exposure_order_seconds": float(hold_e.sum()),
               "train_target_scenarios": first,
               "holdout_target_scenarios": second}
    return summary, train_e, train_n, hold_e, hold_n


def calculate_stability(day_arrays, days):
    validate_schedule(days)
    if tuple(day_arrays.keys()) != tuple(days):
        raise ValueError("Schedule and replay days disagree")
    fits = {d: estimate_poisson_curve(day_arrays[d][0], day_arrays[d][1]) for d in days}
    within, prior, cumulative = [], [], []
    for i, day in enumerate(days):
        e_hold, n_hold = day_arrays[day][2], day_arrays[day][3]
        within.append(assessment(fits[day],e_hold,n_hold,(day,),day,"same_day_10min_train_to_next_10min"))
        if i > 0:
            prev = days[i-1]
            prior.append(assessment(fits[prev],e_hold,n_hold,(prev,),day,"previous_month_train_to_next_month_holdout"))
            all_prior = days[:i]
            pooled_e, pooled_n = sum_training(
                [day_arrays[d][0] for d in all_prior],
                [day_arrays[d][1] for d in all_prior])
            pooled_fit = estimate_poisson_curve(pooled_e, pooled_n)
            cumulative.append(assessment(pooled_fit,e_hold,n_hold,all_prior,day,"expanding_prior_months_train_to_next_holdout"))
    return {"same_day": within, "previous_month": prior,
            "expanding_previous_months": cumulative}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--report-dir", default="reports")
    args = parser.parse_args()
    days = validate_schedule(DAYS)
    output = Path(args.report_dir) / "g0_queue_temporal_2026-06_to_11.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    report = {"status": "started", "days": list(days),
              "train_period_utc": "00:00-00:10",
              "holdout_period_utc": "00:10-00:20",
              "fixed_decision_ms": 100, "fixed_observation_ms": 10,
              "targets_ticks": list(TARGET_TICKS),
              "fixed_distance_bands_ticks": [list(b) for b in BANDS],
              "method": "official hftbacktest 2.4.4, 1ms/1ms order latency, power_prob(2.0), full maker fills; out-of-sample 10min and chronological cross-month",
              "limitations": "Six first-of-month 20min samples; fixed 100ms quote schedule and 10ms approximate accepted-order exposure; repeated target distance replays of same tape are correlated; not daily PnL or statistically independent trades",
              "daily_inputs": []}
    arrays = {}
    try:
        for day in days:
            summary, e1, n1, e2, n2 = day_replay(day)
            report["daily_inputs"].append(summary)
            arrays[day] = (e1,n1,e2,n2)
            print(f"{day} converted={summary['event_count']} train_fills={summary['train_total_engine_fills']} holdout_fills={summary['holdout_total_engine_fills']}",flush=True)
        report["evaluations"] = calculate_stability(arrays, days)
        report["status"] = "complete"
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        output.write_text(json.dumps(report,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
        print(f"G0 temporal test {report['status']}: {output}",flush=True)


if __name__=="__main__":
    main()
