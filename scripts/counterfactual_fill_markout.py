"""Exchange-time fill overlap and quote-cancellation mechanism diagnostic.

Matching here describes two independently simulated official HBT executions.
A baseline-only fill is NOT automatically a successfully prevented fill:
inventory, order price, queue position and timing differ by strategy.
A same-side/same-price prior confirmed cancellation is a conservative
association proxy, not a causal proof of avoiding the baseline fill.
All row-level arrays remain ephemeral; only aggregates are serialized.
"""
from collections import defaultdict, deque

import numpy as np

from g0_fill_markout import asof_exchange_mid
from g0_glft_baseline import MAKER_FEE, TICK


HORIZONS_MS = (100, 500)
MAX_PRECEDING_CANCEL_MS = 500


def matched_fill_indices(baseline, alternative, include_price=True):
    """Multiset exact matching; never match one alternative fill twice."""
    for source in (baseline, alternative):
        n = len(source["ts_ns"])
        if not (n == len(source["side"]) == len(source["price_tick"])):
            raise ValueError("Unbalanced official maker fill trace")
        if np.any(source["ts_ns"] <= 0) or np.any(~np.isin(source["side"], (-1, 1))):
            raise ValueError("Invalid exchange fill timestamp/direction")
    buckets = defaultdict(deque)
    for i in range(len(alternative["ts_ns"])):
        k = (int(alternative["ts_ns"][i]), int(alternative["side"][i]))
        if include_price:
            k += (int(alternative["price_tick"][i]),)
        buckets[k].append(i)
    is_shared_base = np.zeros(len(baseline["ts_ns"]), dtype=bool)
    is_shared_alt = np.zeros(len(alternative["ts_ns"]), dtype=bool)
    for i in range(len(baseline["ts_ns"])):
        k = (int(baseline["ts_ns"][i]), int(baseline["side"][i]))
        if include_price:
            k += (int(baseline["price_tick"][i]),)
        indices = buckets.get(k)
        if indices:
            j = indices.popleft()
            is_shared_base[i] = True
            is_shared_alt[j] = True
    return is_shared_base, is_shared_alt


def canceled_same_quote_before_fill(baseline, alternative,
                                    recent_cancel_ms=MAX_PRECEDING_CANCEL_MS):
    """Evidence of an HBT-confirmed earlier cancel at *exact baseline quote*.

    A later resubmission can still exist, and the canceled order can belong to
    a different inventory path. Thus this is a PROXY, not proof of prevention.
    """
    c = np.asarray(alternative["confirmed_canceled"], dtype=np.int64)
    if c.ndim != 2 or c.shape[1] != 3:
        raise ValueError("Expected official cancellation records (exch_ns,side,px_tick)")
    grouped = defaultdict(list)
    for stamp, side, px_tick in c:
        if stamp > 0 and side in (-1, 1):
            grouped[(int(side), int(px_tick))].append(int(stamp))
    grouped = {k: np.sort(v) for k, v in grouped.items()}
    matched = np.zeros(len(baseline["ts_ns"]), dtype=bool)
    lag_ms = np.full(len(matched), np.nan, dtype=np.float64)
    for i in range(len(matched)):
        t = int(baseline["ts_ns"][i])
        key = (int(baseline["side"][i]), int(baseline["price_tick"][i]))
        v = grouped.get(key)
        if v is None:
            continue
        j = np.searchsorted(v, t, side="left") - 1
        if j >= 0 and t-v[j] <= recent_cancel_ms*1_000_000:
            matched[i] = True
            lag_ms[i] = (t-v[j])/1_000_000.0
    return matched, lag_ms


def evaluate_one_trace(trace, exchange_t, exchange_mid):
    """Side-adjusted BBO markout using exchange time (no local clock mixing)."""
    t = np.asarray(trace["ts_ns"], dtype=np.int64)
    side = np.asarray(trace["side"], dtype=np.int64)
    price_tick = np.asarray(trace["price_tick"], dtype=np.int64)
    if len(t) != len(side) or len(t) != len(price_tick):
        raise ValueError("Malformed maker fill trace")
    if np.any(price_tick <= 0):
        raise ValueError("Invalid official maker quote price")
    # Previous quote strictly before the exchange fill timestamp.
    pre, ok_pre, _ = asof_exchange_mid(exchange_t, exchange_mid, t, strict=True)
    horizons = {}
    for ms in HORIZONS_MS:
        future, ok_future, _ = asof_exchange_mid(
            exchange_t, exchange_mid, t+ms*1_000_000)
        good = ok_pre & ok_future
        price = price_tick * TICK
        signed_mid_move = side * (future-pre)/pre*10_000.0
        # Difference between execution price and future mid, after maker fee;
        # quote-to-mid change has a different meaning than signed mid move.
        net_exec_markout = side * (future-price)/price*10_000.0 - MAKER_FEE*10_000.0
        horizons[ms] = (good, signed_mid_move, net_exec_markout)
    return horizons


def group_report(mask, markouts):
    out = {"fills": int(mask.sum()), "horizons": {}}
    for horizon, (good, signed_move, net_exec) in markouts.items():
        ok = mask & good
        n = int(ok.sum())
        out["horizons"][str(horizon)] = {
            "valid": n, "censored": int(mask.sum())-n,
            "adverse_fraction": float(np.mean(signed_move[ok] < 0)) if n else None,
            "mean_mid_move_bps": float(np.mean(signed_move[ok])) if n else None,
            "mean_net_execution_markout_bps": float(np.mean(net_exec[ok])) if n else None,
            "mean_adverse_only_bps": float(np.mean(signed_move[ok][signed_move[ok] < 0]))
                                     if n and np.any(signed_move[ok] < 0) else None,
        }
    return out


def evaluate_mechanisms(traces, exchange_t, exchange_mid):
    if "beta_0" not in traces:
        raise ValueError("Pure GLFT baseline fill trace missing")
    baseline = traces["beta_0"]
    n = len(baseline["ts_ns"])
    base_m = evaluate_one_trace(baseline, exchange_t, exchange_mid)
    groups = {"beta_0_all_actual_fills": group_report(np.ones(n, dtype=bool), base_m)}
    comparisons = {}
    for name, alt in traces.items():
        if name == "beta_0":
            continue
        strict_base, strict_alt = matched_fill_indices(baseline, alt, True)
        side_base, side_alt = matched_fill_indices(baseline, alt, False)
        if np.any(strict_base & ~side_base):
            # Independent greedy matches may differ with duplicates at same
            # time/side; count-level consistency is tested below.
            pass
        if strict_base.sum() > side_base.sum():
            raise RuntimeError("Exact-quote overlap exceeds time/side overlap")
        prior_cancel, lag = canceled_same_quote_before_fill(baseline, alt)
        # Confirmed cancel proxy only applies to baseline-only quote fills.
        canceled_candidate = (~strict_base) & prior_cancel
        alt_m = evaluate_one_trace(alt, exchange_t, exchange_mid)
        name_groups = {
            "baseline_quote_shared_exact_fill": group_report(strict_base, base_m),
            "baseline_quote_not_filled_in_alternative": group_report(~strict_base, base_m),
            "baseline_no_same_exchange_event_side_fill": group_report(~side_base, base_m),
            "baseline_quote_confirmed_canceled_before_fill_proxy":
                group_report(canceled_candidate, base_m),
            "alternative_actual_fills": group_report(
                np.ones(len(alt["ts_ns"]), dtype=bool), alt_m),
            "alternative_only_new_quote_fills": group_report(~strict_alt, alt_m),
        }
        if (name_groups["baseline_quote_shared_exact_fill"]["fills"] +
            name_groups["baseline_quote_not_filled_in_alternative"]["fills"] != n):
            raise RuntimeError("Baseline mechanism classes do not partition fills")
        comparisons[name] = {
            "baseline_fills": n,
            "alternative_fills": len(alt["ts_ns"]),
            "exact_exchange_time_side_price_overlap": int(strict_base.sum()),
            "same_exchange_time_side_overlap": int(side_base.sum()),
            "baseline_only_exact_quote": int((~strict_base).sum()),
            "baseline_only_no_time_side_fill": int((~side_base).sum()),
            "confirmed_same_quote_cancel_within_500ms_baseline_only":
                int(canceled_candidate.sum()),
            "canceled_proxy_median_lag_ms":
                float(np.nanmedian(lag[canceled_candidate]))
                if canceled_candidate.any() else None,
            "all_confirmed_canceled_orders_in_alternative":
                len(alt["confirmed_canceled"]),
            "groups": name_groups,
        }
    return {
        "matching_rule": "exact exchange_timestamp_ns+side+quote_price_tick multiset",
        "secondary_matching_rule": "exchange_timestamp_ns+side only",
        "cancel_proxy": "HBT order.status=CANCELED confirmation at same side/"
                        "quote price before baseline fill, within 500ms; "
                        "could be re-quoted or other inventory path",
        "markout_source": "OKX native BBO exchange timestamp; strict pre-fill "
                          "mid and as-of +100/+500ms mid, both max 250ms stale",
        "interpretation_limit": "Unmatched baseline quote is not proof of a "
            "successful signal-driven cancel. Execution paths are endogenous, "
            "including inventory, order queue, price and fill sequence.",
        "baseline_fills": n,
        "baseline": groups,
        "comparisons": comparisons
    }
