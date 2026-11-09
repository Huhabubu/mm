"""Exploratory Binance price-event -> OKX future native-BBO response.

Events use Binance native book_ticker records in file order. Only aggregate
statistics leave the runner. No predictive models or trading PnL are fitted.
"""
import argparse
import csv
import gzip
import json
import math
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

from inspect_tardis import download
from predict_leadlag import asof, read_quotes

DAYS = ("2026-09-01", "2026-10-01", "2026-11-01")
HORIZONS_MS = (20, 50, 100, 200, 500, 1000)
DELAYS_MS = (0, 25, 50, 100)
BUCKET_US = 20_000
MAX_OKX_AGE_US = 2_000_000
LOCAL_HISTORY_US = 100_000
STATE_THRESHOLD_BPS = 0.1
LARGE_BINANCE_CHANGE_BPS = 0.1
CONTROL_MAX_LAST_MOVE_US = 500_000


def select_binance_events(path):
    """First price-change and first size-only event per 20ms bucket.

    A size-only control gets the sign of the *previous* Binance price change,
    restricted to the last 500ms. The 20ms bucket is used to avoid excessive
    correlated rows; selection never observes subsequent rows in that bucket.
    """
    events = {k: {"t": [], "sign": [], "impact_bps": []}
              for k in ("up", "down", "size_only")}
    prev = None
    prev_local = None
    recent_sign, recent_move_t = 0, None
    last_bucket = {"price": -1, "size": -1}
    counters = {"rows": 0, "raw_price_changes": 0,
                "raw_size_only": 0, "mid_unchanged_but_price_changed": 0,
                "same_bbo": 0, "selected_up": 0, "selected_down": 0,
                "selected_size_only": 0}
    with gzip.open(path, "rt", newline="", encoding="utf-8") as stream:
        rdr = csv.DictReader(stream)
        required = {"local_timestamp", "bid_price", "ask_price", "bid_amount", "ask_amount"}
        if not required.issubset(set(rdr.fieldnames or [])):
            raise ValueError(f"Missing source columns: {required - set(rdr.fieldnames or [])}")
        for r in rdr:
            counters["rows"] += 1
            t = int(r["local_timestamp"])
            bid, ask = float(r["bid_price"]), float(r["ask_price"])
            qb, qa = float(r["bid_amount"]), float(r["ask_amount"])
            if not (all(math.isfinite(v) for v in (bid, ask, qb, qa))
                    and bid > 0 and ask > bid and qb >= 0 and qa >= 0):
                raise ValueError(f"Invalid native BBO row at {t}")
            if prev_local is not None and t < prev_local:
                raise ValueError(f"Local timestamp backwards at {t}")
            bbo = (bid, ask, qb, qa)
            if prev is not None:
                mid, oldmid = (bid + ask) / 2, (prev[0] + prev[1]) / 2
                bucket = t // BUCKET_US
                if mid != oldmid:
                    counters["raw_price_changes"] += 1
                    recent_sign = 1 if mid > oldmid else -1
                    recent_move_t = t
                    if bucket != last_bucket["price"]:
                        kind = "up" if recent_sign > 0 else "down"
                        impact = 10_000 * (mid / oldmid - 1)
                        events[kind]["t"].append(t)
                        events[kind]["sign"].append(recent_sign)
                        events[kind]["impact_bps"].append(impact)
                        counters[f"selected_{kind}"] += 1
                        last_bucket["price"] = bucket
                elif bid == prev[0] and ask == prev[1] and (qb != prev[2] or qa != prev[3]):
                    counters["raw_size_only"] += 1
                    if (recent_move_t is not None
                            and t - recent_move_t <= CONTROL_MAX_LAST_MOVE_US
                            and bucket != last_bucket["size"]):
                        events["size_only"]["t"].append(t)
                        events["size_only"]["sign"].append(recent_sign)
                        events["size_only"]["impact_bps"].append(0.0)
                        counters["selected_size_only"] += 1
                        last_bucket["size"] = bucket
                elif bid != prev[0] or ask != prev[1]:
                    counters["mid_unchanged_but_price_changed"] += 1
                else:
                    counters["same_bbo"] += 1
            prev, prev_local = bbo, t
    converted = {
        kind: {"t": np.asarray(v["t"], dtype=np.int64),
               "sign": np.asarray(v["sign"], dtype=np.int8),
               "impact_bps": np.asarray(v["impact_bps"], dtype=np.float64)}
        for kind, v in events.items()
    }
    return converted, counters


def response_events(okx, events, day_start_us, delay_ms=0):
    """No future OKX quote in features; future outcome is as-of at t+h."""
    ot, om = okx["t"], okx["mid"]
    t = events["t"]
    if not len(t):
        return None
    decision_t = t + delay_ms * 1000
    start = day_start_us + 1_000_000
    end = day_start_us + 86_400_000_000
    now = asof(ot, decision_t)
    before = asof(ot, decision_t - LOCAL_HISTORY_US)
    good = ((decision_t >= start) & (decision_t + max(HORIZONS_MS) * 1000 < end)
            & (now >= 0) & (before >= 0))
    age = decision_t - ot[np.maximum(now, 0)]
    good &= (age >= 0) & (age <= MAX_OKX_AGE_US)
    t, decision_t, now, before, sign, impact = [
        v[good] for v in (t, decision_t, now, before,
                          events["sign"], events["impact_bps"])
    ]
    pre_signed = 10_000 * (om[now] / om[before] - 1) * sign
    state = np.where(pre_signed >= STATE_THRESHOLD_BPS, "same",
                     np.where(pre_signed <= -STATE_THRESHOLD_BPS, "opposite", "flat"))
    # Keep both sample-level signed responses and masks for auditable aggregation.
    responses = []
    for h in HORIZONS_MS:
        future = asof(ot, decision_t + h * 1000)
        responses.append(10_000 * (om[future] / om[now] - 1) * sign)
    future_signed = np.column_stack(responses)
    return {
        "n": len(t), "t": t, "sign": sign, "impact_bps": impact,
        "pre_signed": pre_signed, "state": state, "age_us": age[good],
        "forward_bps": future_signed,
        "day_start_us": day_start_us,
    }


def aggregate(res, filt=None):
    if res is None:
        return {"count": 0}
    if filt is None:
        filt = np.ones(res["n"], dtype=bool)
    n = int(np.count_nonzero(filt))
    if n == 0:
        return {"count": 0}
    values = res["forward_bps"][filt]
    data = {
        "count": n,
        "okx_pre_signed_100ms_mean_bps": float(np.mean(res["pre_signed"][filt])),
        "okx_quote_age_median_ms": float(np.median(res["age_us"][filt])) / 1e3,
        "mean_binance_event_change_bps": float(np.mean(res["impact_bps"][filt])),
        "response": {},
    }
    for i, h in enumerate(HORIZONS_MS):
        z = values[:, i]
        active = np.abs(z) >= STATE_THRESHOLD_BPS
        data["response"][str(h)] = {
            "mean_signed_bps": float(np.mean(z)),
            "direction_positive_pct": float(100 * np.mean(z > 0)),
            "direction_positive_conditional_nontrivial_pct":
                float(100 * np.mean(z[active] > 0)) if active.any() else None,
            "nontrivial_pct": float(100 * np.mean(active)),
        }
    return data


def _strata(res):
    """Observable local-state matching: UTC hour, sign, OKX momentum & quote age."""
    hour = ((res["t"] - res["day_start_us"]) // 3_600_000_000).astype(np.int8)
    age_bin = np.searchsorted([20_000, 100_000, 500_000], res["age_us"], side="right")
    return list(zip(hour, res["sign"], res["state"], age_bin))


def match_size_control(price, ctrl):
    """Stratum-weighted control mean, by price-event distribution, on the same day.

    Retains only matched strata; no causal claim (freshness/recent sign differs).
    """
    if price is None or ctrl is None:
        return {"matched_price_events": 0}
    stats = defaultdict(lambda: [0, np.zeros(len(HORIZONS_MS), dtype=np.float64)])
    for key, value in zip(_strata(ctrl), ctrl["forward_bps"]):
        stats[key][0] += 1
        stats[key][1] += value
    n, sum_price, sum_ctrl = 0, np.zeros(len(HORIZONS_MS)), np.zeros(len(HORIZONS_MS))
    for key, value in zip(_strata(price), price["forward_bps"]):
        if key not in stats:
            continue
        c, summed = stats[key]
        n += 1
        sum_price += value
        sum_ctrl += summed / c
    return {
        "matched_price_events": n,
        "price_events_all": price["n"],
        "matching_coverage_pct": 100 * n / price["n"] if price["n"] else 0.0,
        "by_horizon": {
            str(h): {
                "matched_price_signed_response_bps": float(sum_price[i] / n) if n else None,
                "matched_size_only_signed_response_bps": float(sum_ctrl[i] / n) if n else None,
                "price_minus_size_only_bps": float((sum_price[i] - sum_ctrl[i]) / n) if n else None,
            }
            for i, h in enumerate(HORIZONS_MS)
        },
        "matching_description": ("Same UTC hour, last Binance mid move sign, "
                                 "OKX signed preceding 100ms momentum band, "
                                 "and OKX last quote age band"),
    }


def analyze_day(day, data_dir):
    start = int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1e6)
    paths = {}
    for name, exchange, symbol in (
        ("okx", "okex-swap", "BTC-USDT-SWAP"),
        ("binance", "binance-futures", "BTCUSDT"),
    ):
        dest = Path(data_dir) / day / f"{exchange}_{symbol}_book_ticker.csv.gz"
        if not dest.is_file():
            print(f"Downloading {day} {name} native BBO", flush=True)
            download(exchange, symbol, "book_ticker", day, dest)
        paths[name] = dest
    print(f"Parsing OKX {day}", flush=True)
    okx = read_quotes(paths["okx"])
    print(f"Selecting Binance price/size events {day}", flush=True)
    events, counts = select_binance_events(paths["binance"])
    print(f"Binance {day}: {counts['raw_price_changes']:,} raw price changes, "
          f"{counts['selected_up'] + counts['selected_down']:,} selected price events, "
          f"{counts['selected_size_only']:,} control events", flush=True)
    out = {"raw_event_counts": counts, "delay_sensitivity": {}, "control_matching": None}
    for delay in DELAYS_MS:
        groups = {k: response_events(okx, v, start, delay)
                  for k, v in events.items()}
        combined = None
        if groups["up"] is not None and groups["down"] is not None:
            combined = {
                k: np.concatenate([groups["up"][k], groups["down"][k]])
                for k in ("t", "sign", "impact_bps", "pre_signed", "state", "age_us", "forward_bps")
            }
            combined["n"] = len(combined["t"])
            combined["day_start_us"] = start
        details = {
            "up": aggregate(groups["up"]),
            "down": aggregate(groups["down"]),
            "price_all": aggregate(combined),
            "price_btc_large": aggregate(
                combined, np.abs(combined["impact_bps"]) >= LARGE_BINANCE_CHANGE_BPS
            ) if combined else {"count": 0},
            "price_okx_pre_flat": aggregate(
                combined, combined["state"] == "flat"
            ) if combined else {"count": 0},
            "price_okx_pre_same": aggregate(
                combined, combined["state"] == "same"
            ) if combined else {"count": 0},
        }
        if delay == 0:
            details["size_only_previous_binance_direction"] = aggregate(groups["size_only"])
            out["control_matching"] = match_size_control(combined, groups["size_only"])
        out["delay_sensitivity"][str(delay)] = details
        signed = details["price_all"]["response"].get("100", {})
        print(f"  delay {delay:3}ms: price events {details['price_all']['count']:,}, "
              f"100ms signed OKX mean {signed.get('mean_signed_bps', float('nan')):+.5f}bps",
              flush=True)
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", default=",".join(DAYS))
    parser.add_argument("--data-dir", default="data/raw")
    parser.add_argument("--report-dir", default="reports")
    args = parser.parse_args()
    days = tuple(x.strip() for x in args.days.split(",") if x.strip())
    if not days or any(datetime.strptime(x, "%Y-%m-%d").day != 1 for x in days):
        parser.error("Expected comma-separated YYYY-MM-01 free Tardis days")
    report = {
        "status": "started",
        "study": "binance_native_price_update_and_okx_signed_future_mid_response",
        "days": list(days),
        "horizons_ms": HORIZONS_MS,
        "delay_ms": DELAYS_MS,
        "binance_event_bucket_ms": BUCKET_US / 1000,
        "okx_pre_momentum_window_ms": LOCAL_HISTORY_US / 1000,
        "momentum_state_threshold_bps": STATE_THRESHOLD_BPS,
        "large_binance_event_threshold_bps": LARGE_BINANCE_CHANGE_BPS,
        "max_okx_quote_age_ms": MAX_OKX_AGE_US / 1000,
        "size_only_control": "last nonzero Binance mid-move direction at most 500ms before",
        "data": {},
        "limitations": [
            "Exploratory event study, not out-of-sample fitted model or GLFT PnL.",
            "The same trading days have previously been inspected; no new independent holdout.",
            "Binance and OKX Tardis collection receive times can differ; clock offset unknown.",
            "Overlapping short-horizon outcomes are serially dependent; no event-level p-values.",
            "Size-only control uses prior price-move direction; matching does not identify causality.",
            "Price-only event effects may be microstructure noise; distinguish magnitude and local state.",
        ],
    }
    dest = Path(args.report_dir) / "binance_price_event_study_btc.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        for day in days:
            report["data"][day] = analyze_day(day, args.data_dir)
        report["status"] = "complete"
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        dest.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Saved {dest}; status={report['status']}", flush=True)


if __name__ == "__main__":
    main()
