"""Queue-consistent G0 arrival-intensity calibration using official hftbacktest fills.

Train: OKX 2026-11-01 00:00-00:10 UTC; holdout 00:10-00:20 UTC.
One independent simulation per target distance, no external signals or PnL claim.
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
from diagnose_g0_intensity import instrument_asset
from g0_glft_baseline import (TICK, LOT, STEP_NS, ORDER_QTY, POSITION_CAP,
                              TRAIN, quote_coefficients)

TARGET_TICKS = (0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 35.0, 50.0, 100.0)
HALF_TICK_BINS = 401             # actual 0.5-tick distance, up to 200 ticks
FIT_MAX_TICK = 100.0
INTERVAL_SECONDS = STEP_NS / 1e9


def observation_schedule(observation_ms):
    """Keep the 100ms strategy clock invariant while refining observation clock."""
    if observation_ms not in (10, 100):
        raise ValueError("Observation resolution must be 10ms or 100ms")
    step_ns = observation_ms * 1_000_000
    return step_ns, STEP_NS // step_ns, TRAIN * (STEP_NS // step_ns)


def estimate_poisson_curve(exposure, fills, max_tick=FIT_MAX_TICK):
    """Poisson MLE with per-bin exposure offset: E[N]=exposure*A*exp(-k*d).

    Correlated replays of the same tape are diagnostic, NOT independent observations.
    """
    exposure = np.asarray(exposure, dtype=float)
    fills = np.asarray(fills, dtype=float)
    if exposure.shape != fills.shape or exposure.ndim != 1:
        raise ValueError("exposure and fills require matching 1D arrays")
    if np.any(exposure < 0) or np.any(fills < 0):
        raise ValueError("negative exposure/fills")
    x_all = np.arange(exposure.size) / 2.0
    selected = ((x_all >= 0.5) & (x_all <= max_tick) & (exposure > 0))
    x, e, n = x_all[selected], exposure[selected], fills[selected]
    if len(x) < 5 or n.sum() < 5:
        return {"status": "insufficient_exposure_or_fills",
                "exposed_bins": int(len(x)), "total_fills": int(n.sum())}
    # Center x to keep Newton steps stable; the slope is per REAL price tick.
    center = float(np.average(x, weights=e))
    z = x - center
    design = np.column_stack((np.ones(len(x)), z))
    initial_rate = n.sum() / e.sum()
    beta = np.array([math.log(initial_rate), -0.01], dtype=float)

    def loss(candidate):
        eta = np.clip(design @ candidate, -35.0, 35.0)
        mu = e * np.exp(eta)
        return float(np.sum(mu - n * eta))

    for _ in range(100):
        eta = np.clip(design @ beta, -35.0, 35.0)
        mu = e * np.exp(eta)
        score = design.T @ (mu - n)
        hessian = design.T @ (mu[:, None] * design)
        try:
            step = np.linalg.solve(hessian, score)
        except np.linalg.LinAlgError:
            return {"status": "singular_information_matrix"}
        # Newton's step becomes numerically stationary before objective changes
        # are distinguishable in floating point, especially with many exposed bins.
        if np.max(np.abs(step)) < 1e-8:
            break
        old_loss = loss(beta)
        scale = 1.0
        while scale >= 1 / 65536 and loss(beta - scale*step) >= old_loss:
            scale *= 0.5
        if scale < 1 / 65536:
            if np.max(np.abs(step)) < 1e-6:
                break
            return {"status": "line_search_failure"}
        beta -= scale * step
        if np.max(np.abs(scale*step)) < 1e-8:
            break
    k = float(-beta[1])
    a = float(math.exp(beta[0] - beta[1]*center))
    if not (np.isfinite(a) and np.isfinite(k) and a > 0):
        return {"status": "invalid_estimates"}
    return {"status": "ok" if k > 0 else "nondecreasing_hazard",
            "A_fills_per_exposed_order_second": a,
            "k_per_real_tick": k,
            "fit_min_distance_ticks": float(min(x)),
            "fit_max_distance_ticks": float(max(x)),
            "exposed_bins": int(len(x)), "total_exposure_seconds": float(e.sum()),
            "total_matched_fills": int(n.sum()),
            "constant_hazard_per_second": float(initial_rate)}


def check_holdout(fit, exposure, fills):
    """Out-of-sample Poisson deviance, relative to training constant-hazard model."""
    if fit.get("status") != "ok":
        return {"status": "not_evaluable", "fit_status": fit.get("status")}
    e = np.asarray(exposure, dtype=float)
    n = np.asarray(fills, dtype=float)
    x = np.arange(e.size) / 2.0
    mask = ((x >= fit["fit_min_distance_ticks"]) &
            (x <= fit["fit_max_distance_ticks"]) & (e > 0))
    x, e, n = x[mask], e[mask], n[mask]
    if len(x) < 1:
        return {"status": "no_holdout_exposure"}
    predicted = e * fit["A_fills_per_exposed_order_second"] * np.exp(-fit["k_per_real_tick"]*x)
    baseline = e * fit["constant_hazard_per_second"]

    def deviance(y, mu):
        tiny = np.finfo(float).tiny
        return float(2 * np.sum(np.where(y > 0,
                              y * np.log(np.maximum(y, tiny)/np.maximum(mu,tiny)) - (y-mu), mu)))

    return {"status": "ok", "holdout_exposed_bins": len(x),
            "holdout_exposure_seconds": float(e.sum()),
            "observed_fills": int(n.sum()),
            "predicted_fills": float(predicted.sum()),
            "constant_baseline_predicted_fills": float(baseline.sum()),
            "exponential_poisson_deviance": deviance(n, predicted),
            "training_constant_poisson_deviance": deviance(n, baseline)}


def simulate_window(npz, low, high, target, phase, observation_ms=100):
    """Replay official engine; sample accepted-order exposure at observation_ms.

    An order filled after being NONE at interval start is reported unmatched
    and excluded from the numerator: its exchange-active time was not observed.
    """
    from hftbacktest import (ROIVectorMarketDepthBacktest, BUY, SELL, NEW,
                             FILLED, GTX, LIMIT)
    from numba import njit

    step_ns, steps_per_quote, planned_steps = observation_schedule(observation_ms)
    step_seconds = step_ns / 1e9

    @njit
    def replay(hbt, target_depth, steps, sample_ns, sample_seconds, quote_every):
        exposure = np.zeros(HALF_TICK_BINS, np.float64)
        buy_exposure = np.zeros(HALF_TICK_BINS, np.float64)
        sell_exposure = np.zeros(HALF_TICK_BINS, np.float64)
        fills = np.zeros(HALF_TICK_BINS, np.int64)
        buy_fills = np.zeros(HALF_TICK_BINS, np.int64)
        sell_fills = np.zeros(HALF_TICK_BINS, np.int64)
        processed = quote_decisions = submitted = canceled = unmatched = unmatched_new = overflow = pending = 0
        total_fills = 0
        max_pos = 0.0
        min_actual = np.inf
        for _ in range(steps):
            # Observe the ACTUAL accepted resting orders at the beginning of a step.
            order_map = hbt.orders(0)
            current = hbt.depth(0)
            mid_ok = (current.best_bid_tick > 0 and
                      current.best_ask_tick > current.best_bid_tick and
                      current.best_ask_tick < 1_000_000_000)
            mid = (current.best_bid_tick + current.best_ask_tick)/2.0 if mid_ok else 0.0
            bid_id = -1
            ask_id = -1
            bid_bin = -1
            ask_bin = -1
            it = order_map.values()
            while it.has_next():
                o = it.get()
                if o.status == NEW:
                    if mid_ok:
                        distance = (mid - o.price_tick) if o.side == BUY else (o.price_tick - mid)
                        idx = int(round(distance * 2.0))
                        if 1 <= idx < HALF_TICK_BINS:
                            min_actual = min(min_actual, distance)
                            if o.side == BUY:
                                bid_id = int(o.order_id)
                                bid_bin = idx
                            elif o.side == SELL:
                                ask_id = int(o.order_id)
                                ask_bin = idx
                        else:
                            overflow += 1
                else:
                    if o.status != FILLED:
                        pending += 1
            if hbt.elapse(sample_ns) != 0:
                break
            # Credit one observation interval to orders accepted at its start.
            # Transition timing uncertainty shrinks from 100ms to 10ms.
            if bid_bin > 0:
                exposure[bid_bin] += sample_seconds
                buy_exposure[bid_bin] += sample_seconds
            if ask_bin > 0:
                exposure[ask_bin] += sample_seconds
                sell_exposure[ask_bin] += sample_seconds
            processed += 1

            order_map = hbt.orders(0)
            it = order_map.values()
            while it.has_next():
                o = it.get()
                if o.status == FILLED:
                    total_fills += 1
                    oid = int(o.order_id)
                    if o.side == BUY and oid == bid_id and bid_bin > 0:
                        fills[bid_bin] += 1
                        buy_fills[bid_bin] += 1
                    elif o.side == SELL and oid == ask_id and ask_bin > 0:
                        fills[ask_bin] += 1
                        sell_fills[ask_bin] += 1
                    else:
                        unmatched += 1
                        if (oid != bid_id and oid != ask_id):
                            unmatched_new += 1
            hbt.clear_inactive_orders(0)
            hbt.clear_last_trades(0)
            # Strategy is deliberately still updated only every 100ms.
            if processed % quote_every != 0:
                continue
            quote_decisions += 1
            d = hbt.depth(0)
            bid, ask = d.best_bid_tick, d.best_ask_tick
            if bid <= 0 or ask <= bid or ask >= 1_000_000_000:
                continue
            midpoint = (bid + ask)/2.0
            want_bid = min(round(midpoint - target_depth), bid)
            want_ask = max(round(midpoint + target_depth), ask)
            pos = hbt.position(0)
            max_pos = max(max_pos, abs(pos))
            buy_exists = False
            sell_exists = False
            it = order_map.values()
            while it.has_next():
                o = it.get()
                if o.side == BUY:
                    buy_exists = True
                    if o.cancellable and o.price_tick != want_bid:
                        hbt.cancel(0, o.order_id, False)
                        canceled += 1
                elif o.side == SELL:
                    sell_exists = True
                    if o.cancellable and o.price_tick != want_ask:
                        hbt.cancel(0, o.order_id, False)
                        canceled += 1
            if not buy_exists and pos + ORDER_QTY <= POSITION_CAP + 1e-9:
                oid = int(want_bid) * 2
                hbt.submit_buy_order(0, oid, want_bid*TICK, ORDER_QTY, GTX, LIMIT, False)
                submitted += 1
            if not sell_exists and pos - ORDER_QTY >= -POSITION_CAP - 1e-9:
                oid = int(want_ask)*2+1
                hbt.submit_sell_order(0, oid, want_ask*TICK, ORDER_QTY, GTX, LIMIT, False)
                submitted += 1
        st = hbt.state_values(0)
        return (processed, quote_decisions, exposure, buy_exposure, sell_exposure, fills, buy_fills,
                sell_fills, total_fills, unmatched, unmatched_new, overflow, pending,
                submitted, canceled, st.num_trades, st.position, max_pos, min_actual)

    engine = ROIVectorMarketDepthBacktest([instrument_asset(npz,low,high)])
    try:
        if phase == "holdout" and engine.elapse(TRAIN*STEP_NS) != 0:
            raise RuntimeError("unable to reach holdout start")
        result = replay(engine, float(target), planned_steps, step_ns, step_seconds, steps_per_quote)
    finally:
        engine.close()
    (steps, quote_decisions, exposure, be, se, fills, bf, sf, total, unmatched, unmatched_new,
     overflow, pending, submitted, canceled, engine_trades, pos, max_pos, minimum) = result
    # The last local event can precede the 10min boundary by <500ms.
    tolerance = max(5, 500_000_000 // step_ns)
    if steps < planned_steps - tolerance:
        raise ValueError(f"{phase} {target}: only {steps}/{planned_steps} observed intervals")
    if quote_decisions != steps // steps_per_quote:
        raise ValueError(f"{phase} {target}: 100ms quote schedule drift")
    if int(total) != int(engine_trades):
        raise ValueError(f"{phase} {target}: fill state mismatch {total} / {engine_trades}")
    if not np.allclose(exposure, be+se):
        raise ValueError(f"{phase} {target}: exposure by side fails reconciliation")
    if not np.array_equal(fills, bf+sf):
        raise ValueError(f"{phase} {target}: fill by side fails reconciliation")
    if int(total) != int(np.sum(fills)) + int(unmatched):
        raise ValueError(f"{phase} {target}: missing fill attribution")
    if abs(pos) > POSITION_CAP + ORDER_QTY + 1e-9:
        raise ValueError(f"{phase} {target}: inventory cap exceeded")
    nonzero = np.flatnonzero(exposure[1:] > 0) + 1
    return {"phase": phase, "target_ticks": target,
            "observation_ms": observation_ms,
            "observed_intervals": int(steps), "observed_seconds": steps*step_seconds,
            "decision_steps": int(quote_decisions),
            "submitted": int(submitted), "cancellation_requests": int(canceled),
            "total_engine_fills": int(total), "exposure_matched_fills": int(np.sum(fills)),
            "unmatched_fills": int(unmatched), "unmatched_new_order_fills": int(unmatched_new),
            "unmeasured_accepted_steps": int(overflow), "pending_order_steps": int(pending),
            "pending_order_seconds": float(pending*step_seconds),
            "unmeasured_accepted_seconds": float(overflow*step_seconds),
            "active_exposure_seconds": float(np.sum(exposure)),
            "active_bid_exposure_seconds": float(np.sum(be)),
            "active_ask_exposure_seconds": float(np.sum(se)),
            "matched_buy_fills": int(np.sum(bf)), "matched_sell_fills": int(np.sum(sf)),
            "active_distance_min_ticks": float(minimum) if np.isfinite(minimum) else None,
            "min_observed_active_bin_ticks": float(nonzero[0]/2) if len(nonzero) else None,
            "max_observed_active_bin_ticks": float(nonzero[-1]/2) if len(nonzero) else None,
            "end_position_contracts": float(pos), "max_abs_position_contracts": float(max_pos),
            "exposure_by_half_tick_seconds": exposure.tolist(),
            "matched_fills_by_half_tick": fills.tolist()}


def pool_rows(records):
    exp = np.zeros(HALF_TICK_BINS, float)
    fills = np.zeros(HALF_TICK_BINS, np.int64)
    for item in records:
        exp += np.asarray(item["exposure_by_half_tick_seconds"], dtype=float)
        fills += np.asarray(item["matched_fills_by_half_tick"], dtype=np.int64)
    if int(fills.sum()) != sum(x["exposure_matched_fills"] for x in records):
        raise ValueError("pooled fills inconsistent")
    return exp, fills


def run_diagnostic(npz, low, high, observation_ms=100):
    train = []
    holdout = []
    for target in TARGET_TICKS:
        train.append(simulate_window(npz,low,high,target,"train", observation_ms))
        holdout.append(simulate_window(npz,low,high,target,"holdout", observation_ms))
    e1,n1 = pool_rows(train)
    e2,n2 = pool_rows(holdout)
    fitted = estimate_poisson_curve(e1,n1)
    checked = check_holdout(fitted,e2,n2)
    rows = []
    for i in range(1,HALF_TICK_BINS):
        if e1[i] > 0 or e2[i] > 0:
            rows.append({"actual_distance_ticks": i/2,
                         "train_exposure_seconds":float(e1[i]),
                         "train_fills":int(n1[i]),
                         "train_hazard_fills_per_order_second":float(n1[i]/e1[i]) if e1[i]>0 else None,
                         "holdout_exposure_seconds":float(e2[i]),"holdout_fills":int(n2[i]),
                         "holdout_hazard_fills_per_order_second":float(n2[i]/e2[i]) if e2[i]>0 else None})
    for record in train+holdout:
        record.pop("exposure_by_half_tick_seconds")
        record.pop("matched_fills_by_half_tick")
    return {"observation_ms": observation_ms,
            "quote_decision_ms": 100,
            "training_scenarios":train, "holdout_scenarios":holdout,
            "exposure_binned_by_actual_price_distance":rows,
            "train_poisson_fit":fitted, "out_of_sample_validation":checked,
            "training_total_exposure_seconds":float(e1.sum()),
            "training_total_engine_fills":sum(r["total_engine_fills"] for r in train),
            "training_matched_fills":int(n1.sum()),
            "holdout_total_exposure_seconds":float(e2.sum()),
            "holdout_total_engine_fills":sum(r["total_engine_fills"] for r in holdout),
            "holdout_matched_fills":int(n2.sum())}


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--day",default="2026-11-01")
    parser.add_argument("--report-dir",default="reports")
    parser.add_argument("--compare-10ms",action="store_true",
                        help="Run both 100ms and 10ms observation; quote decisions remain 100ms")
    args=parser.parse_args()
    day=datetime.strptime(args.day,"%Y-%m-%d").replace(tzinfo=timezone.utc)
    if day.day!=1:
        parser.error("Tardis free data needs day=1")
    report={"status":"started","day":args.day,
            "scope":"queue-consistent simulated fill intensity; original G0 unchanged",
            "train_utc":"00:00-00:10","holdout_utc":"00:10-00:20",
            "simulator":"hftbacktest 2.4.4",
            "quote_target_ticks":list(TARGET_TICKS),
            "order_size_contracts":ORDER_QTY,
            "quote_interval_ms":100,
            "entry_response_latency_ms":[1,1],
            "queue_model":"power_prob_queue_model(2.0)",
            "fill_model":"no_partial_fill_exchange",
            "exposure_approximation":"accepted-order-start observations; fills without observed active exposure excluded and counted",
            "observation_ms": [100,10] if args.compare_10ms else [100]}
    filename=(f"g0_queue_intensity_compare_{args.day}.json" if args.compare_10ms
              else f"g0_queue_intensity_{args.day}.json")
    output=Path(args.report_dir)/filename
    output.parent.mkdir(parents=True,exist_ok=True)
    try:
        from hftbacktest.data.utils import tardis
        from hftbacktest.data.validation import validate_event_order
        with tempfile.TemporaryDirectory(prefix="mm_g0_queue_") as folder:
            root=Path(folder)
            until=int(day.timestamp()*1_000_000) + 1200*1_000_000
            paths={}
            counts={}
            for kind in SCHEMAS:
                paths[kind]=root/(kind+".csv")
                counts[kind]=extract_window(kind,args.day,until,paths[kind],max_rows=1_200_000)
            report["source_counts"]=counts
            npz=root/"events.npz"
            events=tardis.convert_fuse(str(paths["trades"]),str(paths["incremental_book_L2"]),
                                      str(paths["book_ticker"]),tick_size=TICK,lot_size=LOT,
                                      output_filename=str(npz),
                                      ss_buffer_size=max(1000,counts["incremental_book_L2"]["snapshot_rows"]+100),
                                      snapshot_mode="process")
            validate_event_order(events)
            report["converted_events"]=int(len(events))
            del events
            low,high=math.inf,-math.inf
            with paths["book_ticker"].open(encoding="utf-8",newline="") as fh:
                for row in csv.DictReader(fh):
                    low=min(low,float(row["bid_price"]))
                    high=max(high,float(row["ask_price"]))
            if args.compare_10ms:
                old = run_diagnostic(npz,low,high,100)
                fine = run_diagnostic(npz,low,high,10)
                comparable = []
                for side in ("training", "holdout"):
                    for index, target in enumerate(TARGET_TICKS):
                        before = old[f"{side}_scenarios"][index]
                        after = fine[f"{side}_scenarios"][index]
                        comparable.append({
                            "phase": side, "target_ticks": target,
                            "engine_fills_100ms": before["total_engine_fills"],
                            "engine_fills_10ms": after["total_engine_fills"],
                            "order_submissions_100ms": before["submitted"],
                            "order_submissions_10ms": after["submitted"],
                            "order_cancellations_100ms": before["cancellation_requests"],
                            "order_cancellations_10ms": after["cancellation_requests"],
                            "quote_decisions_100ms": before["decision_steps"],
                            "quote_decisions_10ms": after["decision_steps"]})
                report["comparison"]={"baseline_100ms":old, "fine_10ms":fine,
                                       "per_target_execution_consistency":comparable,
                                       "note":"If engine fills or quote counts differ, schedule/measurement coupling must be investigated"}
            else:
                report["calibration"]=run_diagnostic(npz,low,high,100)
        report["status"]="complete"
    except Exception as exc:
        report["status"]="failed"
        report["error"]=f"{type(exc).__name__}: {exc}"
        raise
    finally:
        output.write_text(json.dumps(report,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
        print(f"Queue intensity diagnostic {report['status']}: {output}",flush=True)


if __name__=="__main__":
    main()
