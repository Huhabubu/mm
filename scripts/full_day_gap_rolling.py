"""Stream one Tardis free day of Binance/OKX BBO to audit causal rolling beta.

Does NOT send simulated orders; this is the next signal reliability check
before adding Binance-derived GLFT quotes. No full-day tape is stored.
Both feeds are streamed once, in bounded, overlapping 15min buffers.
"""
import argparse
import csv
import gzip
import io
import json
import math
import urllib.request
from array import array
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from cross_venue_event_probe import visible_timestamps
from gap_event_regression import event_gap_samples
from inspect_tardis import BASE_URL

NS = 1_000_000_000
FIXED_BETA = 0.25959783345
FIXED_ALPHA_USDT = 0.0905761738


class BBOStream:
    """Bounded, forward-only file reader, retaining a 62s context tail."""
    def __init__(self, day, venue, symbol, opener=urllib.request.urlopen):
        yyyy, mm, dd = day.split("-")
        self.url = f"{BASE_URL}/{venue}/book_ticker/{yyyy}/{mm}/{dd}/{symbol}.csv.gz"
        self.opener = opener
        self.stack = ExitStack()
        self.rows = None
        self.pending = None
        self.ts = np.empty(0, np.int64)
        self.mid = np.empty(0, np.float64)
        self.last_us = -1
        self.n_total = 0
        self.n_equal = 0
        self.first_us = None
        self.last_seen_us = None

    def __enter__(self):
        try:
            req = urllib.request.Request(
                self.url, headers={"User-Agent": "mm-full-day-gap-research/1.0"})
            resp = self.stack.enter_context(self.opener(req, timeout=180))
            gz = self.stack.enter_context(gzip.GzipFile(fileobj=resp))
            txt = self.stack.enter_context(io.TextIOWrapper(gz, encoding="utf-8", newline=""))
            self.rows = iter(csv.DictReader(txt))
            self._advance()
            return self
        except BaseException:
            self.stack.close()
            raise

    def __exit__(self, *exc):
        return self.stack.__exit__(*exc)

    def _advance(self):
        row = next(self.rows, None)
        if row is None:
            self.pending = None
            return
        time_us = int(row["local_timestamp"])
        if time_us < self.last_us:
            raise ValueError(f"Out-of-order Tardis local timestamp: {self.url}")
        if time_us == self.last_us:
            self.n_equal += 1
        self.last_us = time_us
        if self.first_us is None:
            self.first_us = time_us
        self.last_seen_us = time_us
        bid, ask = float(row["bid_price"]), float(row["ask_price"])
        if not (math.isfinite(bid) and math.isfinite(ask) and 0 < bid < ask):
            raise ValueError(f"Invalid Tardis native BBO: {self.url}")
        self.pending = (time_us * 1000, (bid + ask) / 2.0)

    def until(self, stop_ns):
        """Include only events strictly before stop_ns, retaining duplicate order."""
        new_ts, new_mid = array("q"), array("d")
        while self.pending is not None and self.pending[0] < stop_ns:
            ts, mid = self.pending
            new_ts.append(ts)
            new_mid.append(mid)
            self.n_total += 1
            self._advance()
        if new_ts:
            self.ts = np.concatenate((self.ts, np.asarray(new_ts, np.int64)))
            self.mid = np.concatenate((self.mid, np.asarray(new_mid, np.float64)))
        return self.ts, self.mid

    def retain_from(self, start_ns):
        """Keep one prior as-of row plus last >=start_ns 62s history."""
        k = int(np.searchsorted(self.ts, start_ns, side="left"))
        k = max(k - 1, 0)
        self.ts = self.ts[k:]
        self.mid = self.mid[k:]


def collapse_same_timestamp_last(ts, mid):
    """Keep the final original row at each reported microsecond.

    This is an experimental weighting choice, not a claim that tied BBO
    updates contain identical prices. The original tape is preserved.
    """
    ts = np.asarray(ts, np.int64)
    mid = np.asarray(mid, np.float64)
    if ts.ndim != 1 or ts.shape != mid.shape:
        raise ValueError("Timestamp/quote shape mismatch")
    if len(ts) == 0:
        return ts, mid
    if np.any(np.diff(ts) < 0):
        raise ValueError("Out-of-order timestamps")
    keep = np.r_[ts[:-1] != ts[1:], True]
    return ts[keep], mid[keep]


def fit_from_stats(a, min_events=1000):
    """Closed-form intercept and zero-intercept OLS using matured-label sums.

    [n, sum(x), sum(y), sum(x^2), sum(xy)] over a strictly past window.
    """
    n, sx, sy, xx, xy = (float(x) for x in a[:5])
    if n < min_events or xx <= 1e-10:
        return None
    beta0 = xy / xx
    denom = xx - sx * sx / n
    if denom <= 1e-10:
        return None
    beta = (xy - sx * sy / n) / denom
    alpha = (sy - beta * sx) / n
    if not all(map(math.isfinite, (beta0, beta, alpha))):
        return None
    return {"beta_zero_intercept": beta0,
            "beta_free_intercept": beta,
            "alpha_usdt_per_btc": alpha, "training_events": int(n)}


def aggregate_rows(x, y, time_ns, start_ns, tau_ms):
    """Count each outcome only after t + tau has elapsed (rounded UP to 1s)."""
    mature_sec = ((time_ns + tau_ms * 1_000_000 + NS - 1) // NS -
                  start_ns // NS).astype(np.int64)
    return mature_sec


class ScoreAccumulator:
    def __init__(self):
        self.n = 0
        self.y2 = 0.0
        self.fixed = 0.0
        self.fixed_beta0 = 0.0
        self.rolling0 = 0.0
        self.rolling_free = 0.0

    def add(self, x, y, beta0, beta_free, alpha):
        if not len(x):
            return
        self.n += len(x)
        self.y2 += float(np.dot(y, y))
        self.fixed += float(np.dot(y - (FIXED_ALPHA_USDT + FIXED_BETA*x),
                                   y - (FIXED_ALPHA_USDT + FIXED_BETA*x)))
        self.fixed_beta0 += float(np.dot(y - FIXED_BETA*x, y - FIXED_BETA*x))
        self.rolling0 += float(np.dot(y - beta0*x, y - beta0*x))
        self.rolling_free += float(np.dot(y - (alpha + beta_free*x),
                                          y - (alpha + beta_free*x)))

    def report(self):
        r = {"events": self.n}
        for name, err in (("fixed_alpha_beta", self.fixed),
                          ("fixed_beta_alpha0", self.fixed_beta0),
                          ("rolling_beta_alpha0", self.rolling0),
                          ("rolling_alpha_beta", self.rolling_free)):
            r[f"r2_vs_zero_{name}"] = 1 - err / self.y2 if self.y2 else None
        r["root_mean_square_zero_error_usdt_per_btc"] = (
            math.sqrt(self.y2 / self.n) if self.n else None)
        return r


def minute_block_bootstrap(minute_scores, block_minutes=5, repetitions=1000,
                           seed=20261111):
    """Fixed-width adjacent-minute cluster resampling of already scored predictions.

    Values are SSE sums, not β re-estimates. Covers estimator's realized
    prequential predictions, preserving within-block event dependence.
    No claim of independent, unseen trading days or economic significance.
    """
    if block_minutes < 1 or repetitions < 100 or len(minute_scores) < block_minutes:
        raise ValueError("Insufficient minute groups for specified bootstrap")
    n_groups = (len(minute_scores) + block_minutes - 1) // block_minutes
    vals = np.asarray([
        [v.n, v.y2, v.fixed, v.fixed_beta0, v.rolling0, v.rolling_free]
        for v in minute_scores], dtype=np.float64)
    groups = np.asarray([
        vals[start:start + block_minutes].sum(axis=0)
        for start in range(0, len(vals), block_minutes)], dtype=np.float64)
    overall = groups.sum(axis=0)
    if overall[0] <= 0 or overall[1] <= 0:
        raise ValueError("No bootstrap evaluation observations")
    rng = np.random.default_rng(seed)
    draw = rng.integers(0, n_groups, size=(repetitions, n_groups))
    sampled = groups[draw].sum(axis=1)
    valid = sampled[:, 1] > 0
    if not valid.all():
        raise ValueError("Bootstrap drew zero-variance samples")
    result = {}
    for name, col in (("fixed_alpha_beta", 2), ("fixed_beta_alpha0", 3),
                      ("rolling_beta_alpha0", 4), ("rolling_alpha_beta", 5)):
        boot = 1.0 - sampled[:, col] / sampled[:, 1]
        point = 1.0 - overall[col] / overall[1]
        result[name] = {
            "point_r2_vs_zero": float(point),
            "ci95_percentile": np.quantile(boot, [0.025, 0.975]).tolist(),
            "bootstrap_fraction_positive": float(np.mean(boot > 0)),
        }
    return {
        "method": "nonoverlapping contiguous minute blocks resampled with replacement; "
                  "point statistics from actual strictly chronological predictions",
        "cluster_minutes": int(block_minutes), "clusters": int(n_groups),
        "bootstrap_repetitions": int(repetitions), "seed": int(seed),
        "caveat": "only one historical day; minutes across regimes may be dependent",
        "scores": result
    }


def run_one_day(day, hours=24, chunk_minutes=15, basis_seconds=60,
                delay_ms=25, tau_ms=100, rolling_minutes=10,
                update_seconds=5, max_age_ms=250, sample_mode="joint_events",
                with_minute_bootstrap=False):
    """Full UTC day with exactly-once core processing and no future-label training."""
    if not 0 < hours <= 24 or 60 % update_seconds:
        raise ValueError("Unsupported hour horizon or update step")
    if rolling_minutes < 2 or chunk_minutes < 3 or not 0 <= delay_ms <= 1000:
        raise ValueError("Invalid analysis configuration")
    if sample_mode not in ("joint_events", "binance_last_per_microsecond",
                           "fixed_10ms", "fixed_50ms"):
        raise ValueError("Unsupported sampling clock")
    start_ns = int(datetime.strptime(day, "%Y-%m-%d").replace(
        tzinfo=timezone.utc).timestamp()) * NS
    end_ns = start_ns + int(hours * 3600 * NS)
    chunk_ns = chunk_minutes * 60 * NS
    update_ns = update_seconds * NS
    hist_s = rolling_minutes * 60
    total_s = (end_ns - start_ns) // NS
    # Plus tau and 2s for final event-time labels.
    n_cells = int(total_s) + 10
    stats = np.zeros((n_cells, 5), np.float64)
    by_source = {"combined": ScoreAccumulator(),
                 "okx_trigger": ScoreAccumulator(),
                 "binance_trigger": ScoreAccumulator()}
    by_hour = {str(h): ScoreAccumulator() for h in range(math.ceil(hours))}
    minute_scores = ([ScoreAccumulator() for _ in range(math.ceil(hours * 60))]
                     if with_minute_bootstrap else None)
    hourly_expost = np.zeros((math.ceil(hours), 3, 5), np.float64)
    # Hour index, [combined, OKX, Binance], [n, sx, sy, xx, xy].
    roller = []
    total_events = total_okx = total_binance = 0
    total_fixed = 0
    chunks_done = 0
    excluded_early = 0
    missing_rolling_fit = 0
    first_usable = None
    last_usable = None
    no_sample_chunks = []
    largest_window_events = 0
    overlap_ns = (basis_seconds + 3) * NS
    future_ns = max(2 * NS, tau_ms * 1_000_000 + NS)
    with BBOStream(day, "okex-swap", "BTC-USDT-SWAP") as okx, BBOStream(
         day, "binance-futures", "BTCUSDT") as binance:
        for core_start in range(start_ns, end_ns, chunk_ns):
            core_end = min(core_start + chunk_ns, end_ns)
            ot, om = okx.until(core_end + future_ns)
            bt_local, bm = binance.until(core_end + future_ns)
            bt = visible_timestamps(bt_local, delay_ms)
            if not len(ot) or not len(bt):
                raise ValueError(f"Missing BBO at chunk {core_start}")
            if sample_mode == "binance_last_per_microsecond":
                bt, bm = collapse_same_timestamp_last(bt, bm)
                # Count eliminated core rows only, never history/future overlap.
                # Original tied-row totals are recorded by BBOStream.n_equal.
            decision_times = None
            if sample_mode.startswith("fixed_"):
                grid_ms = int(sample_mode.split("_")[1].replace("ms", ""))
                decision_times = np.arange(core_start, core_end, grid_ms * 1_000_000,
                                           dtype=np.int64)
            samples = event_gap_samples(ot, om, bt, bm,
                                        basis_seconds=basis_seconds,
                                        tau_ms=tau_ms, max_age_ms=max_age_ms,
                                        decision_times=decision_times)
            core = ((samples["t_ns"] >= core_start) &
                    (samples["t_ns"] < core_end))
            x = samples["gap_usdt_per_btc"][core]
            y = samples["response_usdt_per_btc"][core]
            times = samples["t_ns"][core]
            source = samples["source"][core]
            largest_window_events = max(largest_window_events, len(samples["t_ns"]))
            total_events += int(len(x))
            total_okx += int(np.count_nonzero(source == 0))
            total_binance += int(np.count_nonzero(source == 1))
            total_fixed += int(np.count_nonzero(source == 2))
            if len(x):
                if first_usable is None:
                    first_usable = int(times[0])
                last_usable = int(times[-1])
            else:
                no_sample_chunks.append((core_start - start_ns) // NS)

            # Observations and predictions are in 5s *chronological* groups.
            # NEVER add current-group labels to rolling statistics until AFTER
            # predicting this group. Even then maturation time controls inclusion.
            a = int((core_start - start_ns) // update_ns)
            z = int(math.ceil((core_end - start_ns) / update_ns))
            for j in range(a, z):
                begin = start_ns + j * update_ns
                finish = min(begin + update_ns, core_end)
                l = int(np.searchsorted(times, begin, side="left"))
                r = int(np.searchsorted(times, finish, side="left"))
                qx, qy, qt, qs = x[l:r], y[l:r], times[l:r], source[l:r]
                cutoff_sec = (begin - start_ns) // NS
                rolling_sum = stats[max(0, cutoff_sec - hist_s):cutoff_sec].sum(axis=0)
                model = fit_from_stats(rolling_sum)
                if cutoff_sec < hist_s:
                    excluded_early += len(qx)
                elif model is None:
                    missing_rolling_fit += len(qx)
                else:
                    beta0 = model["beta_zero_intercept"]
                    beta = model["beta_free_intercept"]
                    alpha = model["alpha_usdt_per_btc"]
                    roller.append((float(cutoff_sec), beta0, beta, alpha,
                                   model["training_events"]))
                    if len(qx):
                        by_source["combined"].add(qx, qy, beta0, beta, alpha)
                        for typ, label in ((0, "okx_trigger"), (1, "binance_trigger")):
                            mask = qs == typ
                            by_source[label].add(qx[mask], qy[mask],
                                                 beta0, beta, alpha)
                        hour = int(cutoff_sec // 3600)
                        by_hour[str(hour)].add(qx, qy, beta0, beta, alpha)
                        if minute_scores is not None:
                            minute_index = int(cutoff_sec // 60)
                            minute_scores[minute_index].add(qx, qy, beta0, beta, alpha)

                # Ex-post within-hour slope is DESCRIPTIVE, uses its own future
                # labels and must never be used by the live online predictor.
                if len(qx):
                    hours_vec = ((qt - start_ns) // (3600 * NS)).astype(np.int64)
                    for hh in np.unique(hours_vec):
                        which_hour = hours_vec == hh
                        for si, select in ((0, np.ones(len(qx), np.bool_)),
                                           (1, qs == 0), (2, qs == 1)):
                            mask = which_hour & select
                            if mask.any():
                                vx, vy = qx[mask], qy[mask]
                                hourly_expost[hh, si] += (
                                    len(vx), vx.sum(), vy.sum(),
                                    np.dot(vx, vx), np.dot(vx, vy))

                if len(qx):
                    mature = aggregate_rows(qx, qy, qt, start_ns, tau_ms)
                    if np.any(mature < cutoff_sec):
                        raise RuntimeError("Mature time older than prediction period")
                    # Event labels only eligible from a future fit, when
                    # their precise t+tau is genuinely in the past.
                    for offset in np.unique(mature):
                        if offset >= n_cells:
                            continue  # day-end censored, outside evaluation range
                        mask = mature == offset
                        vx, vy = qx[mask], qy[mask]
                        stats[offset, :] += (len(vx), vx.sum(), vy.sum(),
                                             np.dot(vx, vx), np.dot(vx, vy))
            chunks_done += 1
            okx.retain_from(core_end - overlap_ns)
            binance.retain_from(core_end - overlap_ns)
            print("full_day_gap", day, f"chunk={chunks_done}",
                  f"end_utc_hours={(core_end-start_ns)/NS/3600:.2f}",
                  f"core_events={len(x)}", f"total_events={total_events}",
                  flush=True)
        count_info = {
            "okx_native_bbo_rows": okx.n_total,
            "binance_native_bbo_rows": binance.n_total,
            "okx_duplicate_local_timestamp_rows": okx.n_equal,
            "binance_duplicate_local_timestamp_rows": binance.n_equal,
            "okx_first_local_us": okx.first_us,
            "binance_first_local_us": binance.first_us,
            "okx_last_local_us": okx.last_seen_us,
            "binance_last_local_us": binance.last_seen_us,
        }

    rolling_array = np.asarray(roller, np.float64).reshape(-1, 5)
    daily = {src: score.report() for src, score in by_source.items()}
    hours_report = []
    for h, score in by_hour.items():
        ex_post = {}
        for kind, idx in (("combined", 0), ("okx_trigger", 1),
                          ("binance_trigger", 2)):
            f = fit_from_stats(hourly_expost[int(h), idx], min_events=30)
            ex_post[kind] = f
        hours_report.append({"hour_utc": int(h), "online_score": score.report(),
                             "expost_only": ex_post})
    if not total_events or not rolling_array.size:
        raise ValueError("No valid full-day gap / online beta samples")
    if chunks_done != math.ceil((end_ns - start_ns) / chunk_ns):
        raise RuntimeError("Not all UTC chunks completed")
    bootstrap = (minute_block_bootstrap(minute_scores)
                 if minute_scores is not None else None)
    return {
        "status": "complete", "day": day, "hours_requested": hours,
        "minute_block_bootstrap": bootstrap,
        "sample_mode": sample_mode,
        "chunks_processed": chunks_done, "minutes_per_chunk": chunk_minutes,
        "no_valid_samples_chunk_seconds": no_sample_chunks,
        "not_new_blind_sample": True,
        "signal_only_no_order_submissions": True,
        "method": "event-clock causal BBO, 60s 1s-grid past basis, 75% fresh grid; "
                  "100ms future OKX target, label becomes fit-eligible only AFTER t+100ms; "
                  "rolling 10min of matured labels, refit every 5s; no strategy PnL",
        "configuration": {
            "basis_seconds": basis_seconds, "tau_ms": tau_ms,
            "extra_binance_delay_ms": delay_ms,
            "max_quote_age_ms": max_age_ms,
            "rolling_minutes": rolling_minutes, "refit_seconds": update_seconds,
            "fixed_6_to_8_month_beta": FIXED_BETA,
            "fixed_6_to_8_month_alpha_usdt_per_btc": FIXED_ALPHA_USDT},
        "data": count_info,
        "sampling": {
            "eligible_event_rows": total_events,
            "eligible_okx_events": total_okx,
            "eligible_binance_events": total_binance,
            "eligible_fixed_clock_events": total_fixed,
            "first_eligible_ns": first_usable, "last_eligible_ns": last_usable,
            "excluded_initial_rolling_training_events": excluded_early,
            "missing_valid_rolling_model_events": missing_rolling_fit,
            "largest_context_window_event_rows": largest_window_events},
        "matched_out_of_sample_scores": daily,
        "rolling_beta_updates": {
            "n": int(len(rolling_array)),
            "alpha0_beta_min": float(np.min(rolling_array[:, 1])),
            "alpha0_beta_median": float(np.median(rolling_array[:, 1])),
            "alpha0_beta_max": float(np.max(rolling_array[:, 1])),
            "free_intercept_beta_median": float(np.median(rolling_array[:, 2])),
            "free_intercept_alpha_median_usdt_per_btc": float(np.median(rolling_array[:, 3])),
            "first_5": rolling_array[:5].tolist(),
            "last_5": rolling_array[-5:].tolist()
        },
        "by_hour": hours_report
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--day", default="2026-11-01")
    p.add_argument("--hours", type=float, default=24.)
    p.add_argument("--chunk-minutes", type=int, default=15)
    p.add_argument("--minute-block-bootstrap", action="store_true",
                   help="compute 5-minute clustered 95% confidence intervals")
    p.add_argument("--sample-mode", default="joint_events",
                   choices=("joint_events", "binance_last_per_microsecond",
                            "fixed_10ms", "fixed_50ms"))
    p.add_argument("--report-dir", default="reports")
    args = p.parse_args()
    if datetime.strptime(args.day, "%Y-%m-%d").day != 1:
        p.error("Free Tardis day expected: monthly 1st UTC")
    suffix = "" if args.sample_mode == "joint_events" else "_" + args.sample_mode
    outfile = Path(args.report_dir)/f"full_day_gap_rolling_{args.day}_{args.hours:g}h{suffix}.json"
    outfile.parent.mkdir(parents=True, exist_ok=True)
    report = {"status": "started", "day": args.day, "hours_requested": args.hours}
    try:
        report = run_one_day(args.day, hours=args.hours, chunk_minutes=args.chunk_minutes,
                             sample_mode=args.sample_mode,
                             with_minute_bootstrap=args.minute_block_bootstrap)
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        outfile.write_text(json.dumps(report, ensure_ascii=False, indent=2)+"\n",
                           encoding="utf-8")
        print("full-day rolling beta report:", report["status"], outfile, flush=True)


if __name__ == "__main__":
    main()
