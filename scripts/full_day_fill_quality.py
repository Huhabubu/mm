"""Post-hoc, exchange-time, official HBT maker-fill mechanism audit.

No re-simulation or independent matching engine: input fills are taken from
the already completed official hftbacktest strategies. Market BBO is used
EXCLUSIVELY as a future-label source, never as a trading decision input.

A baseline-only simulated fill is NOT a proven prevented fill: price,
queue, inventory and order-state trajectories differ between strategies.
"""
from collections import defaultdict, deque

import numpy as np

from g0_fill_markout import evaluate_fill_markouts
from g0_glft_baseline import TICK

HORIZONS_MS = (100, 500)


def multiset_match(left, right, include_execution_price=False):
    """One-to-one match at identical EXCHANGE timestamp and side, optional price.

    Both masks are returned, preventing repeated exchange events from being
    paired to the same execution twice.
    """
    buckets = defaultdict(deque)
    for j, f in enumerate(right):
        key = (int(f["exch_timestamp_ns"]), int(f["side_sign"]))
        if include_execution_price:
            key += (int(round(f["execution_price_usdt_per_btc"] / TICK)),)
        buckets[key].append(j)
    left_matched = np.zeros(len(left), dtype=bool)
    right_matched = np.zeros(len(right), dtype=bool)
    for i, f in enumerate(left):
        key = (int(f["exch_timestamp_ns"]), int(f["side_sign"]))
        if include_execution_price:
            key += (int(round(f["execution_price_usdt_per_btc"] / TICK)),)
        if buckets[key]:
            j = buckets[key].popleft()
            left_matched[i] = True
            right_matched[j] = True
    if int(left_matched.sum()) != int(right_matched.sum()):
        raise AssertionError("Execution pairing was not one-to-one")
    return left_matched, right_matched


def selected(fills, mask):
    if len(fills) != len(mask):
        raise ValueError("Fill/mask sizes disagree")
    return [fills[i] for i in np.flatnonzero(mask)]


def evaluate_full_day_fill_quality(fills_by_strategy, exchange_ns, exchange_mid):
    """Describe all actual fills and both Binance pair comparisons.

    Fill groups are classified by same exchange timestamp + side. A second
    diagnostic requires an identical execution price tick; its population
    is *not* silently substituted for the time/side-only cohort.
    """
    required = ("beta_0", "beta_1", "beta_0_wall", "beta_1_wall")
    if tuple(sorted(fills_by_strategy)) != tuple(sorted(required)):
        raise ValueError("Expected exactly the four frozen GLFT variants")
    for name in required:
        fills = fills_by_strategy[name]
        for f in fills:
            if (int(f["exch_timestamp_ns"]) <= 0 or
                    int(f["side_sign"]) not in (-1, 1) or
                    float(f["execution_price_usdt_per_btc"]) <= 0 or
                    float(f["executed_qty_contracts"]) <= 0):
                raise ValueError(f"Invalid official fill trace: {name}")
    if len(exchange_ns) == 0 or np.any(np.diff(exchange_ns) < 0):
        raise ValueError("Exchange BBO timestamps must be nondecreasing")

    per_strategy = {}
    for name in required:
        per_strategy[name] = evaluate_fill_markouts(
            fills_by_strategy[name], exchange_ns, exchange_mid,
            horizons_ms=HORIZONS_MS)

    comparisons = {}
    for base_name, signal_name in (("beta_0", "beta_1"),
                                   ("beta_0_wall", "beta_1_wall")):
        base, signal = fills_by_strategy[base_name], fills_by_strategy[signal_name]
        base_shared, signal_shared = multiset_match(base, signal)
        strict_base, strict_signal = multiset_match(
            base, signal, include_execution_price=True)
        shared_n, strict_n = int(base_shared.sum()), int(strict_base.sum())
        if strict_n > shared_n:
            raise AssertionError("Price-exact overlap exceeds time/side overlap")
        base_unmatched = ~base_shared
        signal_unmatched = ~signal_shared
        if (shared_n + int(base_unmatched.sum()) != len(base) or
                shared_n + int(signal_unmatched.sum()) != len(signal)):
            raise AssertionError("Counterfactual fill accounting does not add up")

        group_fills = {
            "baseline_without_simultaneous_same_side_signal_fill":
                selected(base, base_unmatched),
            "baseline_with_simultaneous_same_side_signal_fill":
                selected(base, base_shared),
            "signal_only_at_different_time_or_side":
                selected(signal, signal_unmatched),
            "baseline_exact_quote_unmatched":
                selected(base, ~strict_base),
            "signal_exact_quote_unmatched":
                selected(signal, ~strict_signal),
        }
        comparisons[f"{base_name}_vs_{signal_name}"] = {
            "baseline_fills": len(base),
            "signal_fills": len(signal),
            "same_exchange_ns_side_overlap": shared_n,
            "same_exchange_ns_side_execution_price_tick_overlap": strict_n,
            "baseline_only_time_side": int(base_unmatched.sum()),
            "signal_only_time_side": int(signal_unmatched.sum()),
            "baseline_only_exact_quote": int((~strict_base).sum()),
            "signal_only_exact_quote": int((~strict_signal).sum()),
            "classification": "one-to-one exchange timestamp ns and side",
            "strict_sensitivity": "exchange timestamp ns, side, and actual execution price tick",
            "groups": {
                k: evaluate_fill_markouts(v, exchange_ns, exchange_mid,
                                          horizons_ms=HORIZONS_MS)
                for k, v in group_fills.items()
            },
        }
    return {
        "source": "official HBT actual maker fills; OKX native BBO exchange timestamps",
        "markout_conventions": {
            "pre_to_horizon": "pre-fill strictly earlier BBO to <=target+100/500ms",
            "immediate": "pre-fill BBO to first strictly AFTER fill BBO within 100ms",
            "continuation": "first strictly after fill BBO to asof +100/+500ms",
            "pre_and_future_max_quote_age_ms": 250,
            "horizons_ms": list(HORIZONS_MS),
            "net_execution_proxy": "actual maker execution price to future mid less maker fee",
        },
        "interpretation_limits": [
            "Matching executions does not identify the same real order or queue.",
            "Baseline-only fills are not proved signal-caused, successfully canceled orders.",
            "Postfill continuation is ex-post evaluation, not information known at quote time.",
            "Fill-conditional markouts differ from portfolio equity and do not identify causality.",
        ],
        "all_actual_fills": per_strategy,
        "paired_baseline_versus_signal": comparisons,
    }
