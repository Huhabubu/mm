"""Read-only price-origin diagnostic for Binance→OKX signals.

Classify each strictly causal native BBO midpoint-change event by its source.
Every event uses the current 60s basis from the existing GLFT, Binance
visibility delay 2.5ms, and independent OKX future BBO response labels.

These groups are conditional event descriptions, NOT proof of leader causation.
No trading decisions, fill matching, fee model or matching engine are altered.
"""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np

from audit_cross_venue_basis_lag import past_indices, MARKETS
from check_okx_hft_data import extract_window
from cross_venue_event_probe import read_binance_bbo, visible_timestamps
from run_beta_one_direct_gap import causal_60s_basis, read_local_bbo

NS = 1_000_000_000
MAX_AGE_NS = 250_000_000
HORIZONS_MS = (5, 10, 25, 50, 100)
FIRST_TRADE_SECONDS = 600
END_SECONDS = 3600
DEDUP_BUCKET_NS = 20_000_000


def keep_last_of_equal_time(ts, mid):
    """The event engine consumes all same-ns updates in original row order."""
    if len(ts) != len(mid) or not len(ts) or np.any(np.diff(ts) < 0):
        raise ValueError("Need ordered nonempty quote arrays")
    keep = np.r_[ts[1:] != ts[:-1], True]
    return ts[keep], mid[keep]


def source_events(source_t, source_mid, other_t, other_mid,
                  basis_t, basis, source, tick, start_ns, end_ns):
    """Make price-change candidate events with strictly prior other-market quote.

    B event: current B and strictly older O. O event: current O and
    strictly older B. Exact timestamp ties are excluded from both.
    'Toward Binance' is sign(G) times future OKX movement. The event
    source is not necessarily the first market to discover information.
    """
    if source not in ("binance", "okx"):
        raise ValueError("Invalid source market")
    if tick <= 0:
        raise ValueError("Invalid OKX price tick")
    delta = np.empty(len(source_mid), dtype=np.float64)
    delta[0] = np.nan
    delta[1:] = source_mid[1:] - source_mid[:-1]
    candidate = ((source_t >= start_ns) & (source_t < end_ns) &
                 np.isfinite(delta) & (delta != 0))
    ix = np.flatnonzero(candidate)
    times = source_t[ix]
    oi = past_indices(other_t, times)
    gi = past_indices(basis_t, times)
    osafe, gsafe = np.maximum(oi, 0), np.maximum(gi, 0)
    # Both sources must have current quotes <=250ms old.
    other_age = times - other_t[osafe]
    good = (oi >= 0) & (gi >= 0) & (other_age <= MAX_AGE_NS) & (other_age >= 0)
    good &= np.isfinite(basis[gsafe])
    # Source price and other price must be on a comparable, valid market.
    if source == "binance":
        bmid, omid = source_mid[ix], other_mid[osafe]
    else:
        bmid, omid = other_mid[osafe], source_mid[ix]
    good &= (bmid > 0) & (omid > 0)
    gap = bmid - omid - basis[gsafe]
    # Need at least half an OKX price tick; do not optimize this threshold
    # on previously explored dates.
    good &= np.abs(gap) >= 0.5 * tick - 1e-10
    good &= (np.sign(gap) == (np.sign(delta[ix])
                              if source == "binance"
                              else -np.sign(delta[ix])))
    # With the other source strictly earlier, simultaneous price updates
    # never leak into the conditioned classification.
    return {
        "time": times[good],
        "okx_mid": omid[good],
        "gap": gap[good],
        "source_price_change": delta[ix][good],
        "source_name": source,
        "source_events_with_price_change": int(candidate.sum()),
        "eligible_after_causal_gates": int(good.sum()),
    }


def first_per_20ms_bucket(events):
    """Sample first valid impulse in each 20ms bucket per source class."""
    times = events["time"]
    if not len(times):
        return np.empty(0, dtype=np.int64)
    buckets = times // DEDUP_BUCKET_NS
    return np.flatnonzero(np.r_[True, buckets[1:] != buckets[:-1]])


def label_price_move(event_t, starting_mid, okx_t, okx_mid, horizon_ms, end_ns):
    """Future endpoint is label-only: latest observed OKX BBO at or before t+h."""
    target = event_t + horizon_ms * 1_000_000
    j = past_indices(okx_t, target, include_equal=True)
    jsafe = np.maximum(j, 0)
    age = target - okx_t[jsafe]
    good = ((j >= 0) & (target < end_ns) &
            (age >= 0) & (age <= MAX_AGE_NS) &
            np.isfinite(starting_mid) & (starting_mid > 0))
    future = okx_mid[jsafe]
    return good, 1e4 * (future - starting_mid) / starting_mid


def summarize_event_class(events, okx_t, okx_mid, end_ns):
    select = first_per_20ms_bucket(events)
    t = events["time"][select]
    gap = events["gap"][select]
    mid = events["okx_mid"][select]
    table = {}
    for horizon in HORIZONS_MS:
        good, y = label_price_move(t, mid, okx_t, okx_mid, horizon, end_ns)
        n = int(good.sum())
        signed = np.sign(gap[good]) * y[good]
        nonzero = np.abs(y[good]) > 1e-12
        table[str(horizon)] = {
            "valid_events": n,
            "nonzero_future_move_events": int(np.count_nonzero(nonzero)),
            "mean_toward_binance_bps_including_zeros":
                (float(np.mean(signed)) if n else None),
            "adverse_fraction_including_zeros":
                (float(np.mean(signed < 0)) if n else None),
            "same_sign_fraction_nonzero_future":
                (float(np.mean(signed[nonzero] > 0)) if np.any(nonzero) else None),
        }
    return {
        "source": events["source_name"],
        "all_price_change_events": events["source_events_with_price_change"],
        "eligible_events_before_20ms_dedup": events["eligible_after_causal_gates"],
        "eligible_events_after_20ms_dedup": len(t),
        "bucket_width_ms": DEDUP_BUCKET_NS // 1_000_000,
        "mean_abs_gap_target_ticks": float(np.mean(np.abs(gap))) if len(gap) else None,
        "future_okx_toward_binance": table,
    }


def paired_origin_report(binance, okx, ot, om, end_ns):
    """Only compare descriptive event classes; no randomized intervention."""
    b = summarize_event_class(binance, ot, om, end_ns)
    o = summarize_event_class(okx, ot, om, end_ns)
    return {
        "binance_price_change_source": b,
        "okx_price_change_source": o,
        "limitations": [
            "Source refers to visible BBO price-change origin of event, not causality",
            "Same-timestamp opposite-venue updates are excluded using strict-past quotes",
            "Event-clock classes have different selection and cannot identify causal leader",
            "First valid event per 20ms source bucket reduces repeat-event domination",
            "All outcomes use native OKX BBO; HBT fused target price can differ",
            "No order book execution, cross-source timestamp proof or independent test date",
        ],
    }


def run(day, coin, delay_ms):
    if coin not in MARKETS:
        raise ValueError("Invalid coin")
    src, target, tick = MARKETS[coin]
    day_us = int(datetime.fromisoformat(day).replace(
        tzinfo=timezone.utc).timestamp() * 1_000_000)
    end_us = day_us + END_SECONDS*1_000_000 + 5_000_000
    with TemporaryDirectory(prefix="bbo_origin_") as tmp:
        file = Path(tmp) / "target_native_bbo.csv"
        extract_window("book_ticker", day, end_us, file, max_rows=3_000_000,
                       exchange="okex-swap", symbol=target)
        ot, om = read_local_bbo(file)
        bt_raw, bm = read_binance_bbo(day, end_us, symbol=src)
    bt = visible_timestamps(bt_raw, delay_ms)
    ot, om = keep_last_of_equal_time(ot, om)
    bt, bm = keep_last_of_equal_time(bt, bm)
    grid, basis = causal_60s_basis(ot, om, bt, bm)
    start_ns = day_us*1000 + FIRST_TRADE_SECONDS * NS
    end_ns = day_us*1000 + END_SECONDS * NS
    b = source_events(bt, bm, ot, om, grid, basis,
                      "binance", tick, start_ns, end_ns)
    o = source_events(ot, om, bt, bm, grid, basis,
                      "okx", tick, start_ns, end_ns)
    report = paired_origin_report(b, o, ot, om, end_ns)
    return {
        "day": day, "coin": coin, "delay_ms": delay_ms,
        "target_tick_usdt_per_coin": tick,
        "basis_source": "causal_60s_basis >=45/60 past 1s BBO points",
        "quote_max_age_ms": 250,
        "origin_threshold": "signed 60s residual >= 0.5 OKX tick in impulse direction",
        "period_utc": "00:10-01:00",
        "source_counts": {"binance": len(bt), "okx": len(ot)},
        "classes": report,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--coin", required=True, choices=sorted(MARKETS))
    parser.add_argument("--day", required=True)
    parser.add_argument("--extra-binance-delay-ms", type=float, default=2.5)
    parser.add_argument("--out", default="reports/bbo_impulse_origin")
    args = parser.parse_args()
    if args.extra_binance_delay_ms < 0 or not np.isfinite(args.extra_binance_delay_ms):
        parser.error("Invalid Binance delay")
    data = run(args.day, args.coin, args.extra_binance_delay_ms)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    filename = f"{args.coin}_{args.day}_2p5ms.json"
    (out / filename).write_text(json.dumps(data, ensure_ascii=False, indent=2),
                                encoding="utf-8")
    print("EVENT_ORIGIN", args.day, args.coin, json.dumps(
        data["classes"], ensure_ascii=False))


if __name__ == "__main__":
    main()
