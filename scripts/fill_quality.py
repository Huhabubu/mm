"""Execution-price total markout and same-fill economics."""
import math
import numpy as np

HORIZONS = (100, 500, 1000)
FRESH_NS = 500_000_000
PRIMARY_HORIZONS = (500, 1000)
QUOTE_AGE_BUCKETS = ("lt_100ms", "100_to_250ms")
GROUPS = ("all", "buy", "sell")

def weighted_p10_and_es10(x, weights):
    """A notional-weighted lower-tail quantile and exactly 10%-mass expected shortfall."""
    order = np.argsort(x, kind="stable")
    xs = x[order]
    w = weights[order]
    total = float(np.sum(w))
    if not total > 0:
        raise ValueError("Missing positive notional weights")
    cutoff = total * 0.10
    cum = np.cumsum(w)
    k = int(np.searchsorted(cum, cutoff, side="left"))
    q10 = float(xs[min(k, len(xs)-1)])
    previous = float(cum[k-1]) if k else 0.0
    tail_weighted_sum = float(np.dot(xs[:k], w[:k]) + xs[k] * (cutoff-previous))
    return q10, tail_weighted_sum/cutoff


def stats(x, w):
    if not len(x):
        return None
    if not (np.isfinite(x).all() and np.isfinite(w).all()
            and np.all(w > 0)):
        raise ValueError("Nonfinite markout/notional")
    n = len(x)
    k = max(1, math.ceil(0.10*n))
    lower = np.sort(x, kind="stable")[:k]
    wq, wes = weighted_p10_and_es10(x, w)
    return {
        "mean_bps": float(np.mean(x)),
        "notional_weighted_mean_bps": float(np.average(x, weights=w)),
        "p10_bps": float(np.quantile(x, 0.1)),
        "worst10_mean_bps": float(np.mean(lower)),
        "notional_weighted_p10_bps": wq,
        "notional_weighted_worst10_mean_bps": wes,
        "negative_fraction": float(np.mean(x < 0)),
        "notional_weighted_negative_fraction": float(np.sum(w[x < 0])/np.sum(w)),
        "count": n,
        "notional_usdt": float(np.sum(w)),
    }


def asof_quotes(times, prices, queries, inclusive, last_valid_ns):
    idx = np.searchsorted(times, queries, side="right" if inclusive else "left") - 1
    valid = (idx >= 0) & (queries < last_valid_ns)
    safe = np.maximum(idx, 0)
    mid = np.where(valid, prices[safe], np.nan)
    age = np.where(valid, queries - times[safe], -1)
    return mid, age, valid


def quote_age_bucket_mask(age_ms, bucket):
    """Age of visible entry quote at the instant of local fill observation."""
    if bucket == "lt_100ms":
        return (age_ms >= 0) & (age_ms < 100)
    if bucket == "100_to_250ms":
        return (age_ms >= 100) & (age_ms <= 250)
    raise ValueError(f"Unknown quote-age bucket: {bucket}")


def label_fills(fill, ts, mid, day_end_ns, ctval):
    required = {"exchange_timestamp_ns","local_observed_ns","side","exec_price","exec_qty"}
    if set(fill) != required:
        raise ValueError(f"Official fill fields mismatch: {set(fill)}")
    t = np.asarray(fill["local_observed_ns"], dtype=np.int64)
    ex = np.asarray(fill["exchange_timestamp_ns"], dtype=np.int64)
    side = np.asarray(fill["side"], dtype=np.int64)
    price = np.asarray(fill["exec_price"], dtype=float)
    qty = np.asarray(fill["exec_qty"], dtype=float)
    if (np.any(ex > t) or np.any(t[1:] < t[:-1])
            or np.any((side != 1) & (side != -1))
            or np.any(qty <= 0) or np.any(price <= 0)
            or not (np.isfinite(qty).all() and np.isfinite(price).all())):
        raise ValueError("Invalid or unsorted official fills")
    px0, age0, valid0 = asof_quotes(ts, mid, t, False, day_end_ns)
    notional = price * qty * ctval
    capture = side * (px0-price)/px0 * 10000
    result = dict(
        exchange_timestamp_ns=ex, local_observed_ns=t,
        side=side, exec_price=price, exec_qty=qty,
        fill_notional_usdt=notional,
        gate_quote_mid_at_observation=px0,
        quote_age_at_observation_ms=age0/1e6,
        capture_halfspread_bps=capture,
    )
    flags = {}
    for h in HORIZONS:
        future = t + h * 1_000_000
        mh, ageh, validh = asof_quotes(ts, mid, future, True, day_end_ns)
        asof = valid0 & validh
        fresh = asof & (age0 <= FRESH_NS) & (ageh <= FRESH_NS)
        markout = side * (mh-px0)/px0 * 10000  # legacy entry-mid drift
        total_markout = side * (mh-price)/price * 10000
        total_fresh = validh & (ageh >= 0) & (ageh <= FRESH_NS)
        future_quote_updated = validh & ((future - ageh) > t)
        result[f"markout_{h}ms_bps"] = markout
        result[f"total_markout_{h}ms_bps"] = total_markout
        result[f"total_fresh_{h}ms"] = total_fresh
        result[f"future_quote_updated_{h}ms"] = future_quote_updated
        result[f"quote_age_{h}ms_ms"] = ageh/1e6
        result[f"asof_valid_{h}ms"] = asof
        result[f"fresh_{h}ms"] = fresh
        flags[h] = (asof, fresh)
    return result, flags


def model_day(model, fill, trace, ts, mid, day_end_ns, ctval):
    annotated, flags = label_fills(fill, ts, mid, day_end_ns, ctval)
    n = len(annotated["side"])
    if n != int(trace["trading"]["engine_fills"]):
        raise ValueError(f"Official HBT count mismatch: {model}")
    if n and not (annotated["local_observed_ns"][0] >= day_end_ns-86_000_000_000_000
                  and annotated["local_observed_ns"][-1] < day_end_ns):
        raise ValueError(f"Unexpected fill window: {model}")
    groups = {}
    for direction in GROUPS:
        mask = (np.ones(n, bool) if direction == "all" else
                annotated["side"] == (1 if direction == "buy" else -1))
        count = int(mask.sum())
        w = annotated["fill_notional_usdt"]
        capture_ok = mask & np.isfinite(annotated["capture_halfspread_bps"])
        capture = stats(annotated["capture_halfspread_bps"][capture_ok], w[capture_ok])
        horizons = {}
        for h in HORIZONS:
            asof, fresh = flags[h]
            valid = mask & fresh & np.isfinite(annotated[f"markout_{h}ms_bps"])
            x = annotated[f"markout_{h}ms_bps"][valid]
            weights = w[valid]
            total_eligible = (mask & annotated[f"total_fresh_{h}ms"]
                              & np.isfinite(annotated[f"total_markout_{h}ms_bps"]))
            updated = total_eligible & annotated[f"future_quote_updated_{h}ms"]
            age = annotated["quote_age_at_observation_ms"]
            strata = {}
            for bucket in QUOTE_AGE_BUCKETS:
                subset = total_eligible & quote_age_bucket_mask(age, bucket)
                strata[bucket] = {
                    "fills": int(subset.sum()),
                    "total_markout": stats(
                        annotated[f"total_markout_{h}ms_bps"][subset], w[subset]),
                }
            horizons[str(h)] = {
                "total_fresh_count": int(total_eligible.sum()),
                "total_fresh_fraction": float(total_eligible.sum()/count) if count else None,
                "total_markout": stats(
                    annotated[f"total_markout_{h}ms_bps"][total_eligible], w[total_eligible]),
                "future_quote_updated_count": int(updated.sum()),
                "total_markout_after_quote_update": stats(
                    annotated[f"total_markout_{h}ms_bps"][updated], w[updated]),
                "quote_age_strata": strata,
                "all_fills": count,
                "asof_count": int(np.count_nonzero(mask & asof)),
                "fresh_count": int(np.count_nonzero(valid)),
                "fresh_fraction": float(np.count_nonzero(valid)/count) if count else None,
                "fresh": stats(x, weights),
                "capture_on_same_fresh_fills": stats(
                    annotated["capture_halfspread_bps"][valid], weights),
            }
        groups[direction] = {
            "fills": count, "capture": capture,
            "horizons": horizons
        }
    return annotated, {
        "model": model,
        "hbt_fills": n,
        "buy_fills": int(np.sum(annotated["side"] == 1)),
        "sell_fills": int(np.sum(annotated["side"] == -1)),
        "total_fill_notional_usdt": float(np.sum(annotated["fill_notional_usdt"])),
        "observed_minus_exchange_ms": {
            "min": float(np.min((annotated["local_observed_ns"]-annotated["exchange_timestamp_ns"])/1e6)) if n else None,
            "max": float(np.max((annotated["local_observed_ns"]-annotated["exchange_timestamp_ns"])/1e6)) if n else None,
        },
        "groups": groups,
    }

def check_equal_stage2_stage3(c, f, day):
    """Require exact same HBT original GLFT fill order, exchange time and side."""
    for key in ("exchange_timestamp_ns","side"):
        if not np.array_equal(c[key],f[key]):
            raise ValueError(f"Stage2/3 same-fill mismatch on {day}: {key}")
    return c["category"]


def summarize_horizon(f, mask, day, cat, h):
    """Compare new execution-price total markout with frozen legacy decomposition.

    The main total calculation is independent of a stale entry midpoint.
    Legacy capture+drift is retained but uses its original stricter eligibility.
    """
    needed = (f"total_fresh_{h}ms", f"total_markout_{h}ms_bps",
              f"future_quote_updated_{h}ms")
    if any(key not in f for key in needed):
        raise ValueError(
            f"Stage2 annotations must be regenerated with total markout for {day}")
    total_ok = (mask & f[f"total_fresh_{h}ms"]
                & np.isfinite(f[f"total_markout_{h}ms_bps"]))
    old_ok = (mask & f[f"fresh_{h}ms"]
              & np.isfinite(f[f"markout_{h}ms_bps"])
              & np.isfinite(f["capture_halfspread_bps"]))
    if not total_ok.any() or not old_ok.any():
        raise ValueError(f"No valid Gate quote valuation for {day}, {cat}, {h}")
    total = f[f"total_markout_{h}ms_bps"][total_ok]
    capture = f["capture_halfspread_bps"][old_ok]
    mark = f[f"markout_{h}ms_bps"][old_ok]
    updated_ok = total_ok & f[f"future_quote_updated_{h}ms"]
    age = f["quote_age_at_observation_ms"]
    young = total_ok & (age >= 0) & (age < 100)
    middle = total_ok & (age >= 100) & (age <= 250)
    def mean_or_none(x):
        return float(np.mean(x)) if len(x) else None
    return {
        "day": day, "class": cat, "horizon_ms": h,
        "total_fills": int(mask.sum()),
        "total_fresh_fills": int(total_ok.sum()),
        "total_fresh_fraction": float(total_ok.sum()/mask.sum()) if mask.any() else None,
        "total_markout_bps": float(total.mean()),
        "total_markout_minus_maker2bps_bps": float(total.mean()-2.0),
        "updated_quote_fills": int(updated_ok.sum()),
        "updated_quote_total_markout_bps": mean_or_none(
            f[f"total_markout_{h}ms_bps"][updated_ok]),
        "entry_quote_lt100_fills": int(young.sum()),
        "entry_quote_lt100_total_markout_bps": mean_or_none(
            f[f"total_markout_{h}ms_bps"][young]),
        "entry_quote_100to250_fills": int(middle.sum()),
        "entry_quote_100to250_total_markout_bps": mean_or_none(
            f[f"total_markout_{h}ms_bps"][middle]),
        "fresh_fills": int(old_ok.sum()),
        "capture_bps": float(capture.mean()),
        "markout_bps": float(mark.mean()),  # legacy entry-mid drift
        "capture_plus_markout_bps": float((capture+mark).mean()),
        "short_horizon_maker_2bps_midmark": float((capture+mark).mean()-2.0),
    }
