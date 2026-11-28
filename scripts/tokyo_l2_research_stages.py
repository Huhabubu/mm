"""Unified five-stage (Stage 1 through Stage 5C) research engine for Tokyo L2 & Tardis.

Reuses:
  - `scripts/core_glft_engine.py::run_core_glft` (sole core strategy implementation)
  - `scripts/execution_metrics.py::summarize`
  - `scripts/cross_exchange_signal.py::exact_moments_5s`
  - `scripts/fill_quality.py::model_day`, `HORIZONS`, `stats`
  - `scripts/fill_quality.py::check_equal_stage2_stage3`, `summarize_horizon`
  - `scripts/tokyo_l2_exchange_adapters.py`
  - `scripts/tokyo_l2_experiment_config.py`

Design invariants:
  - Identical data, shared calibration witness start, and identical session across models.
  - Zero-fee gross PnL is primary; +2 bps / -0.5 bps fee scenarios are supplementary.
  - Stage 3 shadow replay asserts bit-for-bit parity against baseline `glft`.
  - Stage 4 enforces disjoint train/eval dates + SHA-256 lock; refuses unauthorized runs.
  - Stage 5C enforces pre-registered mode lock + disjoint holdout date; refuses unlocked runs.
"""
from __future__ import annotations

from collections import Counter, defaultdict, deque
import hashlib
import json
import math
from pathlib import Path
import time
from typing import Any

import numpy as np

from tokyo_l2_exchange_adapters import compute_strategy_scenario, normalize_contract_spec
from tokyo_l2_experiment_config import (
    ALL_MODELS,
    CORE_MODELS,
    MODEL_REGISTRY,
    canonical_model_name,
    validate_config,
)

NS = 1_000_000_000
STAGE3_GROUPS = ("inactive_or_no_effect", "threatened", "opposite")
HORIZONS = (100, 500, 1000)


def dedup_last_by_ts(ts: np.ndarray, vals: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Keep the last value at each identical timestamp (strictly ascending timeline)."""
    ts = np.asarray(ts, dtype=np.int64)
    vals = np.asarray(vals, dtype=np.float64)
    if len(ts) <= 1:
        return ts, vals
    keep = np.empty(len(ts), dtype=bool)
    keep[:-1] = ts[:-1] != ts[1:]
    keep[-1] = True
    return np.ascontiguousarray(ts[keep]), np.ascontiguousarray(vals[keep])


def _fee_scenarios(gross: float, turnover: float, maker_rates_bps: list[float]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {
        "zero_fee_0bps": {
            "maker_fee_bps": 0.0,
            "fee_usdt": 0.0,
            "net_pnl_usdt": float(gross),
            "net_pnl_bps": (10_000.0 * gross / turnover) if turnover > 0 else None,
        }
    }
    for bps in maker_rates_bps:
        rate = float(bps) / 10_000.0
        fee = rate * turnover
        net = gross - fee
        label = f"maker_{bps:+.1f}bps".replace("+", "plus").replace("-", "minus").replace(".", "p")
        out[label] = {
            "maker_fee_bps": float(bps),
            "fee_usdt": float(fee),
            "net_pnl_usdt": float(net),
            "net_pnl_bps": (10_000.0 * net / turnover) if turnover > 0 else None,
        }
    return out


def descriptive_anomaly_impact(
    annotated: dict[str, Any],
    episodes: list[tuple[int, int]],
    audit_counts: np.ndarray,
    probe_ns: np.ndarray | None,
    probe_rows: np.ndarray | None,
    active_begin_ns: int,
) -> dict[str, Any]:
    """Descriptive interruption-impact diagnostics (official HBT fills are never altered/deleted)."""
    from fill_quality import HORIZONS as FQ_HORIZONS, stats

    t_obs = np.asarray(annotated["local_observed_ns"], dtype=np.int64)
    t_exch = np.asarray(annotated["exchange_timestamp_ns"], dtype=np.int64)
    w = np.asarray(annotated["fill_notional_usdt"], dtype=np.float64)
    n_total = int(len(t_obs))
    total_notional = float(np.sum(w)) if n_total else 0.0

    per_episode = []
    in_any_iv = np.zeros(n_total, dtype=bool)
    near_pm1s = np.zeros(n_total, dtype=bool)
    near_pm5s = np.zeros(n_total, dtype=bool)

    for idx, (s, e) in enumerate(episodes):
        s, e = int(s), int(e)
        mask_iv = ((t_obs >= s) & (t_obs <= e)) | ((t_exch >= s) & (t_exch <= e))
        in_any_iv |= mask_iv
        near_pm1s |= (t_obs >= s - NS) & (t_obs <= e + NS)
        near_pm5s |= (t_obs >= s - 5 * NS) & (t_obs <= e + 5 * NS)

        start_open = end_open = spanning_est = 0
        if probe_ns is not None and probe_rows is not None and len(probe_rows) >= 2 * (idx + 1):
            r_start = probe_rows[2 * idx]
            r_end = probe_rows[2 * idx + 1]
            start_open = int((r_start[2] if np.isfinite(r_start[2]) else 0)
                             + (r_start[3] if np.isfinite(r_start[3]) else 0))
            end_open = int((r_end[2] if np.isfinite(r_end[2]) else 0)
                           + (r_end[3] if np.isfinite(r_end[3]) else 0))
            spanning_est = max(start_open, end_open)

        dec_c = int(audit_counts[idx, 0]) if len(audit_counts) > idx else 0
        sub_c = int(audit_counts[idx, 1]) if len(audit_counts) > idx else 0
        can_c = int(audit_counts[idx, 2]) if len(audit_counts) > idx else 0
        per_episode.append({
            "episode_index": idx,
            "start_ns": s,
            "end_ns": e,
            "duration_ms": round((e - s) / 1e6, 3),
            "in_active_trading_session": bool(e >= active_begin_ns),
            "decisions_during_interval": dec_c,
            "submits_during_interval": sub_c,
            "cancels_during_interval": can_c,
            "fills_during_interval": int(mask_iv.sum()),
            "open_orders_at_start": start_open,
            "open_orders_at_end": end_open,
            "spanning_open_orders_count": spanning_est,
        })

    def slice_markout(mask: np.ndarray) -> dict[str, Any]:
        cnt = int(mask.sum())
        notional = float(np.sum(w[mask])) if cnt else 0.0
        horizons = {}
        for h in FQ_HORIZONS:
            ok = mask & annotated[f"total_fresh_{h}ms"].astype(bool)
            horizons[f"{h}ms"] = stats(annotated[f"total_markout_{h}ms_bps"][ok], w[ok])
        return {
            "fills": cnt,
            "fill_fraction": round(cnt / n_total, 6) if n_total else 0.0,
            "notional_usdt": round(notional, 4),
            "notional_fraction": round(notional / total_notional, 6) if total_notional > 0 else 0.0,
            "total_markout": horizons,
        }

    return {
        "diagnostic_disclaimer": (
            "Descriptive diagnostic only; official HBT matching is preserved across interruptions "
            "and fills are never deleted to fabricate alternative net PnL."
        ),
        "per_episode": per_episode,
        "totals_across_all_episodes": {
            "decisions_during_intervals": int(sum(e["decisions_during_interval"] for e in per_episode)),
            "submits_during_intervals": int(sum(e["submits_during_interval"] for e in per_episode)),
            "cancels_during_intervals": int(sum(e["cancels_during_interval"] for e in per_episode)),
            "fills_during_intervals": int(in_any_iv.sum()),
            "max_spanning_open_orders": max((e["spanning_open_orders_count"] for e in per_episode), default=0),
            "sum_spanning_open_orders_active_episodes": int(sum(
                e["spanning_open_orders_count"] for e in per_episode if e["in_active_trading_session"]
            )),
        },
        "recovery_proximity_descriptive_slices": {
            "inside_interruption": slice_markout(in_any_iv),
            "recovery_pm1s": slice_markout(near_pm1s),
            "recovery_pm5s": slice_markout(near_pm5s),
            "outside_recovery_pm5s": slice_markout(~near_pm5s),
        },
    }


# ------------------------------------------------------------------ Stage 1
def run_stage1_accounting(
    records: np.ndarray,
    fills: dict[str, np.ndarray],
    trading: dict[str, Any],
    end_time_ns: int,
    ctval: float,
    date_utc: str,
    model: str,
    tolerance: float = 1e-4,
) -> dict[str, Any]:
    """Exact Stage 1 PnL decomposition & accounting closure (Conventions A & B).

    Mirrors `scripts/analyze_gate_1645_stage1.py::analyze_case` on in-memory arrays.
    """
    stamp = np.asarray(records["timestamp"], dtype=np.int64)
    mid = np.asarray(records["price"], dtype=np.float64)
    pos = np.asarray(records["position"], dtype=np.float64)
    bal = np.asarray(records["balance"], dtype=np.float64)
    if len(stamp) < 2 or np.any(np.diff(stamp) <= 0):
        raise ValueError(f"Stage 1 invalid Recorder timeline: {date_utc}, {model}")
    if not (np.isfinite(pos).all() and np.isfinite(bal).all()):
        raise ValueError(f"Stage 1 non-finite Recorder position/balance: {date_utc}, {model}")

    raw_t = np.asarray(fills["local_observed_ns"], dtype=np.int64)
    raw_exch_t = np.asarray(fills["exchange_timestamp_ns"], dtype=np.int64)
    raw_side = np.asarray(fills["side"], dtype=np.int64)
    raw_px = np.asarray(fills["exec_price"], dtype=np.float64)
    raw_qty = np.asarray(fills["exec_qty"], dtype=np.float64)
    n = int(len(raw_t))
    if not (len(raw_exch_t) == len(raw_side) == len(raw_px) == len(raw_qty) == n):
        raise ValueError(f"Stage 1 ragged fill arrays: {date_utc}, {model}")
    perm = np.argsort(raw_t, kind="stable") if n else np.empty(0, dtype=np.int64)
    t = raw_t[perm]
    exch_t = raw_exch_t[perm]
    side = raw_side[perm]
    px = raw_px[perm]
    qty = raw_qty[perm]
    if n and not (np.all((side == 1) | (side == -1)) and np.all(qty > 0)):
        raise ValueError(f"Stage 1 invalid fill side/size: {date_utc}, {model}")
    if n and (not (np.isfinite(px).all() and np.isfinite(qty).all() and (px > 0).all()) or np.any(exch_t > t)):
        raise ValueError(f"Stage 1 invalid fill price/timestamp: {date_utc}, {model}")
    if n and (t[0] < stamp[0] or t[-1] > int(end_time_ns)):
        raise ValueError(f"Stage 1 fill outside Recorder interval: {date_utc}, {model}")
    if n != int(trading["engine_fills"]):
        raise ValueError(f"Stage 1 official fill-count mismatch: {date_utc}, {model}")

    signed = side.astype(np.float64) * qty
    cumulative_qty = np.r_[0.0, np.cumsum(signed)]
    cumulative_cost = np.r_[0.0, np.cumsum(signed * px * ctval)]
    end_idx = np.searchsorted(t, stamp, side="right")
    pos_error = float(np.max(np.abs(pos - pos[0] - cumulative_qty[end_idx])))
    cash_error = float(np.max(np.abs(bal - bal[0] + cumulative_cost[end_idx])))
    if pos_error > 1e-7 or cash_error > tolerance:
        raise ValueError(f"Stage 1 Recorder/cash/fill mismatch ({date_utc}, {model}): pos={pos_error}, cash={cash_error}")

    # Exclude book-cleared instants (price=NaN during reconnect/snapshot clear) without fabricating mids.
    valid_mid = np.isfinite(mid)
    non_finite_mid_count = int((~valid_mid).sum())
    if int(valid_mid.sum()) < 2:
        raise ValueError(f"Stage 1 insufficient finite-midpoint Recorder samples: {date_utc}, {model}")
    stamp_v = stamp[valid_mid]
    mid_v = mid[valid_mid]
    pos_v = pos[valid_mid]
    bal_v = bal[valid_mid]

    last_mid = float(trading["end_mid_usdt_per_btc"])
    last_pos = float(trading["end_position_contracts"])
    last_bal = float(trading["balance_usdt"])
    fee = float(trading["fee_usdt"])
    if abs(last_pos - pos[0] - cumulative_qty[-1]) > 1e-7:
        raise ValueError(f"Stage 1 final inventory mismatch: {date_utc}, {model}")
    if abs(last_bal - bal[0] + cumulative_cost[-1]) > tolerance:
        raise ValueError(f"Stage 1 final cash mismatch: {date_utc}, {model}")
    if abs(pos_v[0] - pos[0]) > 1e-7 or abs(bal_v[0] - bal[0]) > tolerance:
        raise ValueError(f"Stage 1 fill occurred before first finite midpoint: {date_utc}, {model}")

    zero = float(last_bal + ctval * last_pos * last_mid - bal_v[0] - ctval * pos_v[0] * mid_v[0])
    net = float(trading["end_net_marked_equity_usdt"]) - float(bal_v[0] + ctval * pos_v[0] * mid_v[0])
    if abs(zero - fee - net) > tolerance:
        raise ValueError(f"Stage 1 official fee/net mismatch: {date_utc}, {model}")

    # Convention A: strictly preceding finite 100ms midpoint, ending inventory.
    if n:
        prior = np.searchsorted(stamp_v, t, side="left") - 1
        if np.any(prior < 0):
            raise ValueError(f"Stage 1 fill lacking preceding midpoint: {date_utc}, {model}")
        edges = signed * ctval * (mid_v[prior] - px)
        buy_edge = float(np.sum(edges[side == 1]))
        sell_edge = float(np.sum(edges[side == -1]))
    else:
        buy_edge = sell_edge = 0.0
    capture = buy_edge + sell_edge
    inventory = float(ctval * np.dot(pos_v[1:], np.diff(mid_v)) + ctval * last_pos * (last_mid - mid_v[-1]))
    discrepancy_a = capture + inventory - zero
    if abs(discrepancy_a) > tolerance:
        raise ValueError(f"Stage 1 accounting identity A failed ({date_utc}, {model}): residual={discrepancy_a}")

    # Convention B: following finite 100ms midpoint, starting inventory (ex-post sensitivity only).
    if n:
        following_idx = np.searchsorted(stamp_v, t, side="left")
        following = np.where(following_idx < len(mid_v), mid_v[np.minimum(following_idx, len(mid_v) - 1)], last_mid)
        next_capture = float(np.sum(signed * ctval * (following - px)))
    else:
        next_capture = 0.0
    next_inventory = float(ctval * np.dot(pos_v[:-1], np.diff(mid_v)) + ctval * pos_v[-1] * (last_mid - mid_v[-1]))
    discrepancy_b = next_capture + next_inventory - zero
    if abs(discrepancy_b) > tolerance:
        raise ValueError(f"Stage 1 accounting identity B failed ({date_utc}, {model}): residual={discrepancy_b}")

    dt = np.diff(stamp).astype(np.float64) * 1e-9
    mean_abs_pos = float(np.dot(np.abs(pos[1:]), dt) / np.sum(dt))
    rms_pos = math.sqrt(float(np.dot(pos[1:] ** 2, dt) / np.sum(dt)))
    turnover = float(trading["trading_value_usdt"])
    return {
        "date": date_utc,
        "model": model,
        "fills": n,
        "buy_fills": int(np.sum(side == 1)),
        "sell_fills": int(np.sum(side == -1)),
        "turnover_usdt": turnover,
        "zero_fee_pnl_usdt": zero,
        "zero_fee_pnl_bps": (10_000.0 * zero / turnover) if turnover > 0 else None,
        "buy_capture_usdt": buy_edge,
        "sell_capture_usdt": sell_edge,
        "spread_capture_usdt": capture,
        "inventory_price_pnl_usdt": inventory,
        "accounting_residual_a_usdt": discrepancy_a,
        "next_sample_capture_usdt": next_capture,
        "next_sample_inventory_pnl_usdt": next_inventory,
        "accounting_residual_b_usdt": discrepancy_b,
        "fee_usdt_2bps": fee,
        "net_pnl_usdt_2bps": net,
        "net_pnl_bps_2bps": (10_000.0 * net / turnover) if turnover > 0 else None,
        "recorder_max_pos_error": pos_error,
        "recorder_max_cash_error": cash_error,
        "non_finite_mid_recorder_samples": non_finite_mid_count,
        "mean_abs_position_contracts": mean_abs_pos,
        "rms_position_contracts": rms_pos,
        "accounting_closed": True,
    }


# ------------------------------------------------------------------ Stage 3
def pair_fill_ids(baseline: dict[str, np.ndarray], comparison: dict[str, np.ndarray]) -> np.ndarray:
    """Strict multiset pairing on (exchange_timestamp_ns, side)."""
    remain: dict[tuple[int, int], deque[int]] = defaultdict(deque)
    for j in range(len(comparison["side"])):
        remain[(int(comparison["exchange_timestamp_ns"][j]), int(comparison["side"][j]))].append(j)
    shared = np.zeros(len(baseline["side"]), dtype=bool)
    for i in range(len(shared)):
        key = (int(baseline["exchange_timestamp_ns"][i]), int(baseline["side"][i]))
        if remain[key]:
            remain[key].popleft()
            shared[i] = True
    return shared


def _group_stat(vals: np.ndarray, weights: np.ndarray | None = None) -> dict[str, Any]:
    vals = np.asarray(vals, dtype=np.float64)
    ok = np.isfinite(vals)
    v = vals[ok]
    if not len(v):
        return {
            "n": 0, "mean_bps": None, "notional_weighted_bps": None,
            "negative_fraction": None, "p10_bps": None, "worst10_mean_bps": None,
        }
    count = max(1, int(math.ceil(0.1 * len(v))))
    nw = None
    if weights is not None:
        w = np.asarray(weights, dtype=np.float64)[ok]
        wsum = float(np.sum(w))
        nw = float(np.sum(v * w) / wsum) if wsum > 0 else None
    return {
        "n": int(len(v)),
        "mean_bps": float(np.mean(v)),
        "notional_weighted_bps": nw,
        "negative_fraction": float(np.mean(v < 0.0)),
        "p10_bps": float(np.quantile(v, 0.1)),
        "worst10_mean_bps": float(np.mean(np.sort(v)[:count])),
    }


def classify_stage3_shadow(
    fills: dict[str, np.ndarray],
    rows: np.ndarray,
    inverse: np.ndarray,
    probes: np.ndarray,
    *,
    order_entry_latency_ns: int,
    guard_ns: int,
    min_cancel_ns: int,
    tick_size: float,
) -> dict[str, Any]:
    """Classify baseline GLFT fills from read-only shadow probe rows (parameterized latency/cooldown)."""
    n = int(len(fills["side"]))
    if n == 0:
        empty_i = np.empty(0, dtype=np.int64)
        empty_f = np.empty(0, dtype=np.float64)
        empty_b = np.empty(0, dtype=bool)
        return {
            "exchange_timestamp_ns": empty_i,
            "side": empty_i,
            "category": np.empty(0, dtype="<U24"),
            "threatened": empty_b,
            "opposite": empty_b,
            "effective_gap_usdt": empty_f,
            "observed_gap_usdt": empty_f,
            "sigma_usdt": empty_f,
            "shadow_signal_active": empty_b,
            "order_cancellable_in_local_engine": empty_b,
            "open_order_present_in_local_engine": empty_b,
            "shadow_quote_would_change": empty_b,
            "side_action_rate_ready": empty_b,
            "theoretical_pre_fill_cancel_request": empty_b,
            "order_price_matches_fill": empty_b,
            "snapshot_order_tick": empty_f,
            "shadow_target_tick": empty_f,
            "order_request_deadline_ns": empty_i,
        }

    if rows.shape != (len(probes), 25) or len(inverse) != n:
        raise ValueError(f"Stage 3 shadow probe buffer malformed: rows={rows.shape}, probes={len(probes)}, fills={n}")
    r = rows[inverse]
    side = np.asarray(fills["side"], dtype=np.int64)
    exch_ts = np.asarray(fills["exchange_timestamp_ns"], dtype=np.int64)
    exec_price = np.asarray(fills["exec_price"], dtype=np.float64)

    effective = r[:, 7]
    active = np.isfinite(effective) & (effective != 0.0) & (r[:, 10] > 0.5)
    threatened = active & (((effective > 0.0) & (side == -1)) | ((effective < 0.0) & (side == 1)))
    opposite = active & ~threatened
    category = np.where(threatened, "threatened", np.where(opposite, "opposite", "inactive_or_no_effect"))

    order_tick = np.where(side == 1, r[:, 0], r[:, 1])
    n_open = np.where(side == 1, r[:, 2], r[:, 3])
    n_cancellable = np.where(side == 1, r[:, 4], r[:, 5])
    action_time = np.where(side == 1, r[:, 18], r[:, 19])
    shadow_tick = np.where(side == 1, r[:, 16], r[:, 17])
    deadline = exch_ts - int(order_entry_latency_ns) - int(guard_ns)

    has_open = (n_open > 0) & np.isfinite(order_tick)
    has_order = (n_cancellable > 0) & np.isfinite(order_tick)
    quote_would_change = has_order & np.isfinite(shadow_tick) & (order_tick != shadow_tick)
    allowed_by_rate = (deadline - action_time) >= int(min_cancel_ns)
    can_attempt = threatened & has_order & quote_would_change & allowed_by_rate

    snapshot_px = order_tick * float(tick_size)
    price_matches = has_open & np.isclose(snapshot_px, exec_price, rtol=0.0, atol=max(1e-9, 0.05 * tick_size))

    activation_age_ms = np.where((r[:, 12] >= 0) & (r[:, 12] < deadline), (deadline - r[:, 12]) / 1e6, np.nan)
    last_decision_age_ms = np.where((r[:, 11] >= 0) & (r[:, 11] <= deadline), (deadline - r[:, 11]) / 1e6, np.nan)

    return {
        "exchange_timestamp_ns": exch_ts,
        "side": side,
        "category": category,
        "threatened": threatened,
        "opposite": opposite,
        "effective_gap_usdt": effective,
        "observed_gap_usdt": r[:, 8],
        "sigma_usdt": r[:, 9],
        "shadow_signal_active": active,
        "shadow_last_decision_age_ms": last_decision_age_ms,
        "shadow_activation_age_ms": activation_age_ms,
        "open_order_present_in_local_engine": has_open,
        "order_cancellable_in_local_engine": has_order,
        "shadow_quote_would_change": quote_would_change,
        "side_action_rate_ready": allowed_by_rate,
        "theoretical_pre_fill_cancel_request": can_attempt,
        "order_price_matches_fill": price_matches,
        "snapshot_order_tick": order_tick,
        "shadow_target_tick": shadow_tick,
        "order_request_deadline_ns": deadline,
    }


def verify_fill_parity(
    shadow_fills: dict[str, np.ndarray],
    base_fills: dict[str, np.ndarray],
) -> dict[str, Any]:
    """Verify exact bit-for-bit parity of all 5 fill arrays.

    In `hftbacktest==2.4.4`, `engine.orders(0).values()` iterates a Rust HashMap whose
    bucket order is randomized per engine instance. When two orders (e.g. BUY and SELL)
    fill at the exact same `(local_observed_ns, exchange_timestamp_ns)`, they can be
    emitted in either order across runs while representing the identical fill multiset.
    This verifier asserts 100% bit-for-bit equality on the canonical sorted tuple
    `(local_observed_ns, exchange_timestamp_ns, side, exec_price, exec_qty)` AND verifies
    that any row-order permutation occurs exclusively within identical
    `(local_observed_ns, exchange_timestamp_ns)` timestamps.
    """
    keys = ("exchange_timestamp_ns", "side", "local_observed_ns", "exec_price", "exec_qty")
    for k in keys:
        if shadow_fills[k].shape != base_fills[k].shape:
            raise ValueError(
                f"Stage 3 shadow replay broke baseline GLFT fill count on {k}: "
                f"{shadow_fills[k].shape} != {base_fills[k].shape}"
            )
    if all(np.array_equal(shadow_fills[k], base_fills[k]) for k in keys):
        return {"exact_row_order": True, "simultaneous_hashmap_tie_reorders": 0}

    def _canon_perm(f: dict[str, np.ndarray]) -> np.ndarray:
        return np.lexsort((
            f["exec_qty"],
            f["exec_price"],
            f["side"],
            f["exchange_timestamp_ns"],
            f["local_observed_ns"],
        ))

    ps = _canon_perm(shadow_fills)
    pb = _canon_perm(base_fills)
    for k in keys:
        if not np.array_equal(shadow_fills[k][ps], base_fills[k][pb]):
            diff_idx = np.flatnonzero(shadow_fills[k][ps] != base_fills[k][pb])[:5].tolist()
            raise ValueError(
                f"Stage 3 shadow replay broke baseline GLFT fill parity on {k} "
                f"(canonical diff indices={diff_idx})"
            )

    # Confirm every raw index mismatch occurred at identical (local_observed_ns, exchange_timestamp_ns)
    diff_mask = np.zeros(len(base_fills["side"]), dtype=bool)
    for k in keys:
        diff_mask |= (shadow_fills[k] != base_fills[k])
    if not (np.array_equal(shadow_fills["local_observed_ns"][diff_mask], base_fills["local_observed_ns"][diff_mask])
            and np.array_equal(shadow_fills["exchange_timestamp_ns"][diff_mask], base_fills["exchange_timestamp_ns"][diff_mask])):
        raise ValueError("Stage 3 shadow replay fill reordering crossed distinct timestamps")
    return {
        "exact_row_order": False,
        "simultaneous_hashmap_tie_reorders": int(diff_mask.sum()),
    }


def run_stage3_shadow_audit(
    *,
    date_utc: str,
    make_asset,
    binance_visible_ns: np.ndarray,
    binance_mid: np.ndarray,
    grid: np.ndarray,
    basis: np.ndarray,
    sigma: np.ndarray,
    scenario: dict[str, Any],
    core_options: dict[str, Any],
    trade_steps: int,
    trade_minutes: int,
    trade_stop_ns: int,
    baseline_result: dict[str, Any],
    comparison_fills: dict[str, dict[str, np.ndarray]],
    order_entry_latency_ns: int,
    guard_ns: int,
    min_cancel_ns: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Run read-only shadow replay of baseline `glft`, verify bit-for-bit parity, and summarize Stage 3."""
    from hftbacktest import Recorder, ROIVectorMarketDepthBacktest
    from core_glft_engine import run_core_glft
    from execution_metrics import summarize

    base_fills = baseline_result["fills"]
    base_trading = baseline_result["trading"]
    annotated = baseline_result["annotated"]
    n_fills = int(len(base_fills["side"]))

    if n_fills == 0:
        classified = classify_stage3_shadow(
            base_fills, np.empty((0, 25), dtype=np.float64), np.empty(0, dtype=np.int64),
            np.empty(0, dtype=np.int64),
            order_entry_latency_ns=order_entry_latency_ns, guard_ns=guard_ns,
            min_cancel_ns=min_cancel_ns, tick_size=scenario["tick"],
        )
        return classified, {
            "date": date_utc,
            "status": "complete_zero_fills",
            "parity_verified": True,
            "parity_details": {"exact_row_order": True, "simultaneous_hashmap_tie_reorders": 0},
            "total_fills": 0,
            "parameters": {
                "order_entry_latency_ns": int(order_entry_latency_ns),
                "guard_ns": int(guard_ns),
                "min_cancel_ns": int(min_cancel_ns),
            },
            "categories": {},
            "vs_signal_models": {},
        }

    exch_ts = np.asarray(base_fills["exchange_timestamp_ns"], dtype=np.int64)
    deadlines = exch_ts - int(order_entry_latency_ns) - int(guard_ns)
    unique_probes, inverse = np.unique(deadlines, return_inverse=True)

    t0 = time.time()
    engine = ROIVectorMarketDepthBacktest([make_asset()])
    recorder = Recorder(engine.num_assets, trade_steps + 100)
    diag = np.zeros(12, dtype=np.float64)
    try:
        raw = run_core_glft(
            engine, binance_visible_ns, binance_mid, trade_steps,
            0.0, grid, basis, sigma, 0, scenario["gamma_code"],
            scenario["tick"], scenario["ctval"], scenario["qty"], scenario["position_cap"],
            recorder.recorder, trade_stop_ns, unique_probes, True,
            diagnostics=diag,
            **core_options,
        )
        shadow_fills = {
            "exchange_timestamp_ns": np.asarray(raw[-5], dtype=np.int64),
            "side": np.asarray(raw[-4], dtype=np.int64),
            "local_observed_ns": np.asarray(raw[-2][1], dtype=np.int64),
            "exec_price": np.asarray(raw[-2][2], dtype=np.float64),
            "exec_qty": np.asarray(raw[-2][3], dtype=np.float64),
        }
        parity_details = verify_fill_parity(shadow_fills, base_fills)
        summary = summarize(raw, trade_minutes, True, 0.0, scenario["position_cap"], scenario["qty"],
                            tail_tolerance_samples=25)
        st = summary["trading"]
        for field in ("end_net_marked_equity_usdt", "fee_usdt", "balance_usdt", "end_position_contracts", "trading_value_usdt"):
            if not math.isclose(float(st[field]), float(base_trading[field]), rel_tol=0.0, abs_tol=1e-7):
                raise ValueError(f"Stage 3 shadow replay ledger parity failed on {field}: {st[field]} != {base_trading[field]}")
        probe_rows = np.asarray(raw[-2][4], dtype=np.float64)
    finally:
        engine.close()

    classified = classify_stage3_shadow(
        base_fills, probe_rows, inverse, unique_probes,
        order_entry_latency_ns=order_entry_latency_ns,
        guard_ns=guard_ns,
        min_cancel_ns=min_cancel_ns,
        tick_size=scenario["tick"],
    )

    for key in ("exchange_timestamp_ns", "side", "local_observed_ns", "exec_price", "exec_qty"):
        if not np.array_equal(annotated[key], base_fills[key]):
            raise ValueError(f"Stage 2 annotated fills misaligned with baseline GLFT on {key}")

    w = np.asarray(annotated["fill_notional_usdt"], dtype=np.float64)
    cats = classified["category"]
    cat_counts = Counter(cats.tolist())

    # Compare baseline GLFT fills against every active signal model present
    shared_by_model: dict[str, np.ndarray] = {}
    vs_signal_models: dict[str, Any] = {}
    for mname, mfills in comparison_fills.items():
        if mname == "glft":
            continue
        shared = pair_fill_ids(base_fills, mfills)
        shared_by_model[mname] = shared
        by_cat = {}
        for grp in STAGE3_GROUPS:
            sel = cats == grp
            n_grp = int(sel.sum())
            n_sh = int(np.sum(sel & shared))
            n_bo = int(np.sum(sel & ~shared))
            entry = {
                "all_fills": n_grp,
                "shared": n_sh,
                "baseline_only": n_bo,
                "baseline_only_fraction": (n_bo / n_grp) if n_grp else None,
            }
            if grp == "threatened":
                c = classified["theoretical_pre_fill_cancel_request"] & sel
                entry["theoretical_cancellable"] = int(c.sum())
                entry["theoretical_cancellable_baseline_only"] = int(np.sum(c & ~shared))
                entry["theoretical_cancellable_avoided_rate"] = (
                    float(np.sum(c & ~shared) / c.sum()) if c.any() else None
                )
            by_cat[grp] = entry
        vs_signal_models[mname] = {
            "shared_same_clock_side": int(shared.sum()),
            "baseline_only": int((~shared).sum()),
            "baseline_only_fraction": float((~shared).sum() / n_fills) if n_fills else 0.0,
            "by_category": by_cat,
        }

    # Primary comparison model for legacy Stage 3 per-category shared/baseline-only view
    primary_cmp = next((m for m in ("both_1p0", "defensive_1p0", "risk_side_pause") if m in shared_by_model), None)
    primary_shared = shared_by_model[primary_cmp] if primary_cmp else np.zeros(n_fills, dtype=bool)

    categories_out: dict[str, Any] = {}
    for h in HORIZONS:
        tot_mo = np.asarray(annotated[f"total_markout_{h}ms_bps"], dtype=np.float64)
        tot_fresh = np.asarray(annotated[f"total_fresh_{h}ms"], dtype=bool)
        leg_mo = np.asarray(annotated[f"markout_{h}ms_bps"], dtype=np.float64)
        leg_fresh = np.asarray(annotated[f"fresh_{h}ms"], dtype=bool)
        one_h: dict[str, Any] = {}
        for grp in STAGE3_GROUPS:
            sel = cats == grp
            ok_tot = sel & tot_fresh
            ok_leg = sel & leg_fresh
            entry = {
                "all_fills": int(sel.sum()),
                "fill_fraction": float(sel.sum() / n_fills) if n_fills else 0.0,
                "total_markout_fresh": _group_stat(tot_mo[ok_tot], w[ok_tot]),
                "legacy_drift_fresh": _group_stat(leg_mo[ok_leg], w[ok_leg]),
            }
            if primary_cmp is not None:
                entry["primary_comparison_model"] = primary_cmp
                entry["shared"] = int(np.sum(sel & primary_shared))
                entry["baseline_only"] = int(np.sum(sel & ~primary_shared))
                entry["total_markout_baseline_only_fresh"] = _group_stat(
                    tot_mo[sel & ~primary_shared & tot_fresh], w[sel & ~primary_shared & tot_fresh]
                )
                entry["total_markout_shared_fresh"] = _group_stat(
                    tot_mo[sel & primary_shared & tot_fresh], w[sel & primary_shared & tot_fresh]
                )
            if grp == "threatened":
                c = classified["theoretical_pre_fill_cancel_request"] & sel
                entry["theoretical_cancellable_count"] = int(c.sum())
                entry["theoretical_cancellable_fraction_of_threatened"] = (
                    float(c.sum() / sel.sum()) if sel.any() else None
                )
                entry["theoretical_cancellable_total_markout_fresh"] = _group_stat(
                    tot_mo[c & tot_fresh], w[c & tot_fresh]
                )
                if primary_cmp is not None:
                    entry["theoretical_cancellable_baseline_only"] = int(np.sum(c & ~primary_shared))
            one_h[grp] = entry
        categories_out[str(h)] = one_h

    has_open = classified["open_order_present_in_local_engine"]
    has_cancel = classified["order_cancellable_in_local_engine"]
    px_match = classified["order_price_matches_fill"]
    threat = classified["threatened"]
    lifecycle = {
        "total_baseline_fills": n_fills,
        "open_order_present_at_deadline": int(has_open.sum()),
        "open_order_present_fraction": float(has_open.sum() / n_fills),
        "cancellable_order_at_deadline": int(has_cancel.sum()),
        "cancellable_order_fraction": float(has_cancel.sum() / n_fills),
        "price_exact_match_among_open": int(px_match.sum()),
        "price_exact_match_rate_among_open": float(px_match.sum() / has_open.sum()) if has_open.any() else None,
        "side_action_rate_ready_count": int(classified["side_action_rate_ready"].sum()),
        "side_action_rate_ready_fraction": float(classified["side_action_rate_ready"].mean()),
        "threatened_count": int(threat.sum()),
        "threatened_has_cancellable_order": int(np.sum(threat & has_cancel)),
        "threatened_quote_would_change": int(np.sum(threat & classified["shadow_quote_would_change"])),
        "threatened_rate_ready": int(np.sum(threat & classified["side_action_rate_ready"])),
        "threatened_theoretical_pre_fill_cancellable": int(classified["theoretical_pre_fill_cancel_request"].sum()),
        "threatened_theoretical_cancellable_fraction": (
            float(classified["theoretical_pre_fill_cancel_request"].sum() / threat.sum()) if threat.any() else None
        ),
    }

    stage3_report = {
        "date": date_utc,
        "status": "complete",
        "elapsed_s": round(time.time() - t0, 2),
        "parity_verified": True,
        "parity_details": parity_details,
        "total_fills": n_fills,
        "category_counts": {grp: int(cat_counts.get(grp, 0)) for grp in STAGE3_GROUPS},
        "shadow_total_effective_at_deadline": int(np.sum(classified["shadow_signal_active"])),
        "shadow_total_pre_fill_theoretical_cancellable": int(np.sum(classified["theoretical_pre_fill_cancel_request"])),
        "parameters": {
            "order_entry_latency_ns": int(order_entry_latency_ns),
            "guard_ns": int(guard_ns),
            "min_cancel_ns": int(min_cancel_ns),
        },
        "order_lifecycle_and_price_consistency": lifecycle,
        "categories": categories_out,
        "vs_signal_models": vs_signal_models,
    }
    return classified, stage3_report


# ------------------------------------------------------------------ Stage 5A
def run_stage5a_ablation(
    *,
    date_utc: str,
    models_out: dict[str, dict[str, Any]],
    stage1_out: dict[str, dict[str, Any]] | None = None,
    stage2_out: dict[str, dict[str, Any]] | None = None,
    maker_rates_bps: list[float] | None = None,
) -> dict[str, Any]:
    """Structured Stage 5A model ablation comparing core models on shared data/calibration."""
    rates = maker_rates_bps if maker_rates_bps is not None else [2.0, -0.5]
    present_core = [m for m in CORE_MODELS if m in models_out]
    missing_core = [m for m in CORE_MODELS if m not in models_out]
    glft_t = models_out["glft"]["trading"] if "glft" in models_out else None

    rows = []
    for name in models_out:
        t = models_out[name]["trading"]
        gross = float(t["gross_marked_pnl_usdt"])
        turnover = float(t["trading_value_usdt"])
        q = (stage2_out or {}).get(name) or models_out[name].get("fill_quality") or {}
        q_all = (q.get("groups") or {}).get("all") or {}
        q_horizons = q_all.get("horizons") or {}
        tot_mo = {}
        for h in HORIZONS:
            hm = q_horizons.get(str(h)) or q_horizons.get(f"{h}ms") or {}
            tm = hm.get("total_markout") or hm.get("total_markout_all") or {}
            tot_mo[f"{h}ms"] = tm.get("mean_bps")
        fees = _fee_scenarios(gross, turnover, rates)
        row = {
            "date": date_utc,
            "model": name,
            "fills": int(t["engine_fills"]),
            "buy_fills": int(t["buy_fills"]),
            "sell_fills": int(t["sell_fills"]),
            "turnover_usdt": turnover,
            "gross_marked_pnl_usdt": gross,
            "gross_pnl_bps": (10_000.0 * gross / turnover) if turnover > 0 else None,
            "breakeven_maker_bps": (10_000.0 * gross / turnover) if turnover > 0 else None,
            "fee_usdt_2bps": float(t["fee_usdt_2bps"]),
            "net_marked_pnl_usdt_2bps": float(t["net_marked_pnl_usdt_2bps"]),
            "net_pnl_bps_2bps": t.get("net_pnl_per_turnover_bps_2bps"),
            "fee_scenarios": fees,
            "total_markout_100ms_bps": tot_mo["100ms"],
            "total_markout_500ms_bps": tot_mo["500ms"],
            "total_markout_1000ms_bps": tot_mo["1000ms"],
            "position_std_contracts": float(t["position_std_contracts"]),
            "rms_inventory_contracts": float(t["rms_inventory_contracts"]),
            "mean_abs_inventory_contracts": float(t["mean_abs_inventory_contracts"]),
            "max_abs_position_contracts": float(t["max_abs_position_contracts"]),
            "end_position_contracts": float(t["end_position_contracts"]),
            "submits": int(t["submissions"]),
            "cancels": int(t["cancel_requests"]),
            "modify_requests": int(t.get("modify_requests", 0)),
            "max_order_ops_per_s": int(models_out[name].get("max_ops_per_sec", 0)),
            "signal_activations": int((models_out[name].get("binance_origin_filter") or {}).get("binance_origin_activations", 0)),
            "ties_excluded": int((models_out[name].get("binance_origin_filter") or {}).get("same_timestamp_ties_blocked", 0)),
        }
        if stage1_out and name in stage1_out:
            row["stage1_spread_capture_usdt"] = stage1_out[name]["spread_capture_usdt"]
            row["stage1_inventory_price_pnl_usdt"] = stage1_out[name]["inventory_price_pnl_usdt"]
            row["stage1_accounting_closed"] = stage1_out[name]["accounting_closed"]
        if glft_t is not None:
            row["delta_vs_glft_gross_usdt"] = gross - float(glft_t["gross_marked_pnl_usdt"])
            row["delta_vs_glft_net_2bps_usdt"] = (
                float(t["net_marked_pnl_usdt_2bps"]) - float(glft_t["net_marked_pnl_usdt_2bps"])
            )
            row["delta_vs_glft_fills"] = int(t["engine_fills"]) - int(glft_t["engine_fills"])
        rows.append(row)

    return {
        "date": date_utc,
        "status": "complete" if not missing_core else "partial_core_models",
        "core_models_present": present_core,
        "core_models_missing": missing_core,
        "all_four_core_models_present": len(missing_core) == 0,
        "models": {r["model"]: r for r in rows},
        "table": rows,
    }


# ------------------------------------------------------------------ Stage 5B
def run_stage5b_same_fill_audit(
    *,
    date_utc: str,
    stage2_annotated_glft: dict[str, np.ndarray],
    stage3_classified: dict[str, Any],
    stage5a_report: dict[str, Any],
    comparison_fills: dict[str, dict[str, np.ndarray]] | None = None,
) -> dict[str, Any]:
    """Same-fill Stage 5B decomposition aligning Stage 2 annotations and Stage 3 shadow classes."""
    from fill_quality import check_equal_stage2_stage3, summarize_horizon

    groups = check_equal_stage2_stage3(stage3_classified, stage2_annotated_glft, date_utc)
    n_fills = int(len(groups))
    glft_fills_expected = int(stage5a_report["models"]["glft"]["fills"])
    if n_fills != glft_fills_expected:
        raise ValueError(f"Stage 5B fill count mismatch on {date_utc}: {n_fills} != {glft_fills_expected}")

    def safe_horizon(mask: np.ndarray, label: str, h: int) -> dict[str, Any]:
        tot_ok = (mask & stage2_annotated_glft[f"total_fresh_{h}ms"].astype(bool)
                  & np.isfinite(stage2_annotated_glft[f"total_markout_{h}ms_bps"]))
        old_ok = (mask & stage2_annotated_glft[f"fresh_{h}ms"].astype(bool)
                  & np.isfinite(stage2_annotated_glft[f"markout_{h}ms_bps"])
                  & np.isfinite(stage2_annotated_glft["capture_halfspread_bps"]))
        if tot_ok.any() and old_ok.any():
            rec = summarize_horizon(stage2_annotated_glft, mask, date_utc, label, h)
            rec["total_markout_plus_rebate0p5bps_bps"] = float(rec["total_markout_bps"] + 0.5)
            return rec
        return {
            "day": date_utc, "class": label, "horizon_ms": h,
            "total_fills": int(mask.sum()), "total_fresh_fills": int(tot_ok.sum()),
            "total_fresh_fraction": float(tot_ok.sum() / mask.sum()) if mask.any() else None,
            "total_markout_bps": float(np.mean(stage2_annotated_glft[f"total_markout_{h}ms_bps"][tot_ok])) if tot_ok.any() else None,
            "total_markout_minus_maker2bps_bps": (
                float(np.mean(stage2_annotated_glft[f"total_markout_{h}ms_bps"][tot_ok]) - 2.0) if tot_ok.any() else None
            ),
            "total_markout_plus_rebate0p5bps_bps": (
                float(np.mean(stage2_annotated_glft[f"total_markout_{h}ms_bps"][tot_ok]) + 0.5) if tot_ok.any() else None
            ),
            "updated_quote_fills": 0, "updated_quote_total_markout_bps": None,
            "entry_quote_lt100_fills": 0, "entry_quote_lt100_total_markout_bps": None,
            "entry_quote_100to250_fills": 0, "entry_quote_100to250_total_markout_bps": None,
            "fresh_fills": int(old_ok.sum()),
            "capture_bps": None, "markout_bps": None, "capture_plus_markout_bps": None,
            "short_horizon_maker_2bps_midmark": None,
        }

    slices: list[tuple[str, np.ndarray]] = [
        ("all_fills", np.ones(n_fills, dtype=bool)),
        ("threatened", groups == "threatened"),
        ("inactive_or_no_effect", groups == "inactive_or_no_effect"),
        ("opposite", groups == "opposite"),
        ("threatened_cancellable", (groups == "threatened") & stage3_classified["theoretical_pre_fill_cancel_request"]),
    ]
    if comparison_fills:
        for mname in ("both_1p0", "defensive_1p0", "risk_side_pause"):
            if mname in comparison_fills:
                sh = pair_fill_ids(stage2_annotated_glft, comparison_fills[mname])
                slices.append((f"threatened_baseline_only_vs_{mname}", (groups == "threatened") & ~sh))

    rows = []
    by_class: dict[str, dict[str, Any]] = {}
    for label, mask in slices:
        by_class[label] = {}
        for h in HORIZONS:
            r = safe_horizon(mask, label, h)
            rows.append(r)
            by_class[label][str(h)] = r

    breakeven = {
        name: {
            "zero_fee_pnl_usdt": m["gross_marked_pnl_usdt"],
            "turnover_usdt": m["turnover_usdt"],
            "breakeven_maker_bps": m["breakeven_maker_bps"],
            "net_pnl_2bps_usdt": m["net_marked_pnl_usdt_2bps"],
        }
        for name, m in stage5a_report["models"].items()
    }
    return {
        "date": date_utc,
        "status": "complete",
        "same_fill_aligned": True,
        "total_glft_fills": n_fills,
        "by_class": by_class,
        "rows": rows,
        "breakeven_by_model": breakeven,
    }


# ------------------------------------------------------------------ Stage 4
def train_and_lock_stage4_controls(
    *,
    train_day_bundles: list[dict[str, Any]],
    config: dict[str, Any],
    lock_path: Path | None = None,
) -> dict[str, Any]:
    """Train Stage 4 controls (static_widen, local_vol_pause) strictly on independent training date(s).

    Enforces zero overlap with eval_dates, evaluates the prespecified 4-point grids
    for Control A (`widen_ratios`) and Control B (`vol_quantiles`), selects the
    closest-fill candidate to `both_1p0` deterministically, and emits a SHA-256 lock.
    """
    cfg = validate_config(config)
    train_dates = [b["date_utc"] for b in train_day_bundles]
    if not train_dates:
        raise ValueError("Stage 4 training requires at least one independent training date")
    if set(train_dates) & set(cfg["dates"]["eval_dates"]):
        raise ValueError("Stage 4 leakage violation: training date overlaps eval_dates")

    widen_ratios = [float(x) for x in cfg["stage4"]["widen_ratios"]]
    vol_quantiles = [float(x) for x in cfg["stage4"]["vol_quantiles"]]

    # Aggregate fills across training day(s) for glft, both_1p0, and candidate grids
    target_fills = 0
    glft_fills = 0
    baseline_sigmas: list[np.ndarray] = []
    half_tick_samples: list[float] = []

    train_cfg = validate_config({
        **cfg,
        "models": ["glft", "both_1p0"],
        "stages": ["stage1", "stage2"],
    })
    for b in train_day_bundles:
        res = run_models_on_arrays(
            config=train_cfg,
            date_utc=b["date_utc"],
            hbt_path=b["hbt_path"],
            bbo_local_ns=b["bbo_local_ns"],
            bbo_mid=b["bbo_mid"],
            maker_l1_local_ns=b["maker_l1_local_ns"],
            maker_l1_exch_ns=b["maker_l1_exch_ns"],
            maker_l1_mid=b["maker_l1_mid"],
            contract_spec=b["contract_spec"],
            day_start_ns=b["day_start_ns"],
            day_end_ns=b["day_end_ns"],
            roi_low=b["roi_low"],
            roi_high=b["roi_high"],
            episodes=b.get("episodes", []),
            witness_stop_ns=b.get("witness_stop_ns"),
            keep_calibration_trace=True,
        )
        glft_fills += int(res["models"]["glft"]["trading"]["engine_fills"])
        target_fills += int(res["models"]["both_1p0"]["trading"]["engine_fills"])
        trace = res["models"]["glft"].get("calibration_trace") or []
        sigs = np.array(
            [row["sigma_tick_per_sqrt_second"] for row in trace
             if math.isfinite(row.get("sigma_tick_per_sqrt_second", math.nan)) and row["sigma_tick_per_sqrt_second"] > 0],
            dtype=np.float64,
        )
        halfs = [
            row["half_spread_ticks"] for row in trace
            if math.isfinite(row.get("half_spread_ticks", math.nan)) and row["half_spread_ticks"] > 0
        ]
        if len(sigs):
            baseline_sigmas.append(sigs)
        half_tick_samples.extend(halfs)

    if not baseline_sigmas or not half_tick_samples:
        raise ValueError("Stage 4 training produced no valid baseline GLFT sigma/half-spread samples")
    all_sigmas = np.concatenate(baseline_sigmas)
    base_half_ticks = float(np.median(half_tick_samples))

    widen_rows = []
    for idx, ratio in enumerate(widen_ratios):
        extra = float(base_half_ticks * ratio)
        fills_sum = 0
        c_cfg = validate_config({**cfg, "models": ["static_widen"], "stages": ["stage1"]})
        for b in train_day_bundles:
            res = run_models_on_arrays(
                config=c_cfg, date_utc=b["date_utc"], hbt_path=b["hbt_path"],
                bbo_local_ns=b["bbo_local_ns"], bbo_mid=b["bbo_mid"],
                maker_l1_local_ns=b["maker_l1_local_ns"], maker_l1_exch_ns=b["maker_l1_exch_ns"],
                maker_l1_mid=b["maker_l1_mid"], contract_spec=b["contract_spec"],
                day_start_ns=b["day_start_ns"], day_end_ns=b["day_end_ns"],
                roi_low=b["roi_low"], roi_high=b["roi_high"], episodes=b.get("episodes", []),
                witness_stop_ns=b.get("witness_stop_ns"),
                control_params={"static_widen": {"extra_half_ticks": extra}},
            )
            fills_sum += int(res["models"]["static_widen"]["trading"]["engine_fills"])
        widen_rows.append({"kind": "static_widen", "candidate_index": idx, "ratio": ratio,
                           "extra_half_ticks": extra, "fills": fills_sum})

    vol_rows = []
    for idx, q in enumerate(vol_quantiles):
        ceiling = float(np.quantile(all_sigmas, q))
        fills_sum = 0
        c_cfg = validate_config({**cfg, "models": ["local_vol_pause"], "stages": ["stage1"]})
        for b in train_day_bundles:
            res = run_models_on_arrays(
                config=c_cfg, date_utc=b["date_utc"], hbt_path=b["hbt_path"],
                bbo_local_ns=b["bbo_local_ns"], bbo_mid=b["bbo_mid"],
                maker_l1_local_ns=b["maker_l1_local_ns"], maker_l1_exch_ns=b["maker_l1_exch_ns"],
                maker_l1_mid=b["maker_l1_mid"], contract_spec=b["contract_spec"],
                day_start_ns=b["day_start_ns"], day_end_ns=b["day_end_ns"],
                roi_low=b["roi_low"], roi_high=b["roi_high"], episodes=b.get("episodes", []),
                witness_stop_ns=b.get("witness_stop_ns"),
                control_params={"local_vol_pause": {"local_sigma_ceiling": ceiling}},
            )
            fills_sum += int(res["models"]["local_vol_pause"]["trading"]["engine_fills"])
        vol_rows.append({"kind": "local_vol_pause", "candidate_index": idx, "quantile": q,
                         "local_sigma_ceiling": ceiling, "fills": fills_sum})

    sel_widen = min(widen_rows, key=lambda x: (abs(x["fills"] - target_fills), x["candidate_index"]))
    sel_vol = min(vol_rows, key=lambda x: (abs(x["fills"] - target_fills), x["candidate_index"]))

    lock_body = {
        "schema": "tokyo_l2_stage4_lock_v1",
        "training_status": "locked",
        "training_dates": train_dates,
        "signal_exchange": cfg["signal_exchange"],
        "making_exchange": cfg["making_exchange"],
        "base_symbol": cfg["base_symbol"],
        "preselected_widen_ratios": widen_ratios,
        "preselected_vol_quantiles": vol_quantiles,
        "baseline_train_glft_fills": glft_fills,
        "target_train_both_1p0_fills": target_fills,
        "median_baseline_half_spread_ticks": base_half_ticks,
        "selected_controls": {
            "static_widen": sel_widen,
            "local_vol_pause": sel_vol,
        },
        "candidates": {"static_widen": widen_rows, "local_vol_pause": vol_rows},
    }
    lock_body["lock_sha256"] = hashlib.sha256(
        json.dumps(lock_body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    if lock_path is not None:
        Path(lock_path).parent.mkdir(parents=True, exist_ok=True)
        Path(lock_path).write_text(json.dumps(lock_body, indent=2) + "\n", encoding="utf-8")
    return lock_body


def verify_stage4_lock(lock_data: dict[str, Any], eval_dates: list[str]) -> dict[str, Any]:
    """Verify SHA-256 integrity and strict train/eval date disjointness of a Stage 4 lock."""
    if lock_data.get("schema") != "tokyo_l2_stage4_lock_v1" or lock_data.get("training_status") != "locked":
        raise ValueError("invalid Stage 4 lock schema or status")
    saved_sha = lock_data.get("lock_sha256")
    body = {k: v for k, v in lock_data.items() if k != "lock_sha256"}
    expected_sha = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    if saved_sha != expected_sha:
        raise ValueError("Stage 4 lock SHA-256 mismatch (tampered lock)")
    train_dates = set(lock_data.get("training_dates") or [])
    if not train_dates:
        raise ValueError("Stage 4 lock has empty training_dates")
    if train_dates & set(eval_dates):
        raise ValueError(f"Stage 4 leakage guard: lock training_dates {sorted(train_dates)} overlaps eval_dates {eval_dates}")
    sel = lock_data.get("selected_controls") or {}
    if "static_widen" not in sel or "local_vol_pause" not in sel:
        raise ValueError("Stage 4 lock missing selected_controls")
    return {
        "static_widen": {"extra_half_ticks": float(sel["static_widen"]["extra_half_ticks"])},
        "local_vol_pause": {"local_sigma_ceiling": float(sel["local_vol_pause"]["local_sigma_ceiling"])},
    }


# ------------------------------------------------------------------ Stage 5C
def lock_stage5c_selection(
    *,
    stage5a_reports_by_date: dict[str, dict[str, Any]],
    config: dict[str, Any],
    holdout_dates: list[str],
    lock_path: Path | None = None,
) -> dict[str, Any]:
    """Pre-register ONE Stage 5C mode strictly from explored Stage 5A dates BEFORE reading holdout."""
    cfg = validate_config(config)
    explored = sorted(stage5a_reports_by_date.keys())
    if not explored:
        raise ValueError("Stage 5C lock requires at least one completed Stage 5A exploration date")
    if not holdout_dates:
        raise ValueError("Stage 5C lock requires target holdout_dates")
    if set(holdout_dates) & set(explored):
        raise ValueError(f"Stage 5C isolation violation: holdout_dates {holdout_dates} overlaps explored {explored}")

    rank_order = tuple(cfg["stage5c"]["rank_order"])
    metric_key = str(cfg["stage5c"]["selection_metric"])
    rms_mult = float(cfg["stage5c"]["rms_multiplier_cap"])

    day_means: dict[str, dict[str, float]] = {}
    for name in rank_order:
        metrics, rmss = [], []
        for d in explored:
            m = stage5a_reports_by_date[d]["models"].get(name)
            if m is None:
                raise ValueError(f"Stage 5C candidate {name!r} missing from Stage 5A on {d}")
            val = float(m[metric_key])
            rms = float(m["rms_inventory_contracts"])
            if not (math.isfinite(val) and math.isfinite(rms)):
                raise ValueError(f"Non-finite Stage 5A metric/RMS for {name} on {d}")
            metrics.append(val)
            rmss.append(rms)
        day_means[name] = {"metric_mean": float(np.mean(metrics)), "rms_mean": float(np.mean(rmss))}

    base_rms = day_means["both_1p0"]["rms_mean"]
    rms_cap = rms_mult * base_rms
    eligible = []
    checks = []
    for priority, name in enumerate(rank_order):
        val = day_means[name]["metric_mean"]
        rms = day_means[name]["rms_mean"]
        ok = rms <= rms_cap + 1e-12
        checks.append({
            "mode": name,
            "selection_metric": metric_key,
            "metric_day_equal_mean": val,
            "rms_inventory_day_equal_mean": rms,
            "rms_cap": rms_cap,
            "eligible": ok,
        })
        if ok:
            eligible.append((val, -priority, name))
    if not eligible:
        raise ValueError("Stage 5C: no candidate satisfies the inventory RMS budget")
    selected = max(eligible)[2]

    lock_body = {
        "schema": "tokyo_l2_stage5c_lock_v1",
        "status": "locked",
        "locked_before_holdout_data": True,
        "signal_exchange": cfg["signal_exchange"],
        "making_exchange": cfg["making_exchange"],
        "base_symbol": cfg["base_symbol"],
        "explored_stage5a_dates": explored,
        "holdout_dates": list(holdout_dates),
        "holdout_chronology": cfg["dates"]["holdout_chronology"],
        "selection_metric": metric_key,
        "rank_order": list(rank_order),
        "rms_multiplier_cap": rms_mult,
        "selected_mode": selected,
        "candidates": checks,
    }
    lock_body["lock_sha256"] = hashlib.sha256(
        json.dumps(lock_body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    if lock_path is not None:
        Path(lock_path).parent.mkdir(parents=True, exist_ok=True)
        Path(lock_path).write_text(json.dumps(lock_body, indent=2) + "\n", encoding="utf-8")
    return lock_body


def verify_stage5c_lock(lock_data: dict[str, Any], holdout_dates: list[str]) -> str:
    """Verify SHA-256 integrity, pre-lock flag, and isolation of a Stage 5C holdout lock."""
    if (lock_data.get("schema") != "tokyo_l2_stage5c_lock_v1"
            or lock_data.get("status") != "locked"
            or lock_data.get("locked_before_holdout_data") is not True):
        raise ValueError("invalid Stage 5C lock schema/status")
    saved_sha = lock_data.get("lock_sha256")
    body = {k: v for k, v in lock_data.items() if k != "lock_sha256"}
    expected_sha = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    if saved_sha != expected_sha:
        raise ValueError("Stage 5C lock SHA-256 mismatch (tampered lock)")
    explored = set(lock_data.get("explored_stage5a_dates") or [])
    if set(holdout_dates) & explored:
        raise ValueError(f"Stage 5C isolation guard: holdout_dates {holdout_dates} overlaps explored {sorted(explored)}")
    if sorted(lock_data.get("holdout_dates") or []) != sorted(holdout_dates):
        raise ValueError(f"Stage 5C holdout_dates mismatch: lock={lock_data.get('holdout_dates')} vs {holdout_dates}")
    selected = canonical_model_name(str(lock_data["selected_mode"]))
    return selected


# --------------------------------------------------------- Core Unified Runner
def run_models_on_arrays(
    *,
    config: dict[str, Any],
    date_utc: str,
    hbt_path: Path,
    bbo_local_ns: np.ndarray,
    bbo_mid: np.ndarray,
    maker_l1_local_ns: np.ndarray,
    maker_l1_exch_ns: np.ndarray,
    maker_l1_mid: np.ndarray,
    contract_spec: dict[str, Any],
    day_start_ns: int,
    day_end_ns: int,
    roi_low: float,
    roi_high: float,
    episodes: list[tuple[int, int]] | None = None,
    witness_stop_ns: int | None = None,
    control_params: dict[str, dict[str, float]] | None = None,
    stage4_lock: dict[str, Any] | None = None,
    stage5c_lock: dict[str, Any] | None = None,
    keep_calibration_trace: bool = False,
) -> dict[str, Any]:
    """Run all configured models & research stages (1, 2, 3, 4, 5A, 5B, 5C) on one converted day.

    Preserves 100% bit-for-bit parity with `kaggle/tasks/full_day_glft.py::run_three_models_on_arrays`
    when invoked with identical Gate BTC parameters.
    """
    from hftbacktest import BacktestAsset, Recorder, ROIVectorMarketDepthBacktest
    from core_glft_engine import run_core_glft
    from cross_exchange_signal import exact_moments_5s
    from execution_metrics import summarize
    from fill_quality import model_day

    cfg = validate_config(config)
    strat = cfg["strategy"]
    lat = cfg["latency"]
    oc = cfg["order_controls"]
    fees_cfg = cfg["fees"]
    spec = normalize_contract_spec(contract_spec)
    episodes = list(episodes or [])
    stages_set = set(cfg["stages"])

    extra_delay_ns = int(lat["binance_extra_visible_delay_ns"])
    order_entry_ns = int(lat["order_entry_latency_ns"])
    order_resp_ns = int(lat["order_response_latency_ns"])
    min_requote_ns = int(oc["min_requote_ns"])
    guard_ns = int(oc["stage3_guard_ns"])
    train_minutes = int(strat["train_minutes"])
    trade_minutes = int(strat["trade_minutes"])
    trade_steps = int(trade_minutes * 600)

    # 1. Signal visible clock (0ms extra delay for Tokyo L2; 2.5ms for Tardis legacy)
    bbo_loc = np.asarray(bbo_local_ns, dtype=np.int64)
    binance_visible_ns = np.ascontiguousarray(bbo_loc + extra_delay_ns, dtype=np.int64)
    binance_mid = np.ascontiguousarray(bbo_mid, dtype=np.float64)
    if extra_delay_ns == 0 and not np.array_equal(binance_visible_ns, bbo_loc):
        raise AssertionError("Binance visible clock must equal raw local_ts_ns when extra delay is 0ms")

    # 2. Maker L1 unlock clock (strictly conservative filter for any rare exch_ts > local_ts)
    m_loc = np.asarray(maker_l1_local_ns, dtype=np.int64)
    m_exc = np.asarray(maker_l1_exch_ns, dtype=np.int64)
    m_mid = np.asarray(maker_l1_mid, dtype=np.float64)
    exch_ahead_mask = m_exc > m_loc
    exch_ahead_count = int(exch_ahead_mask.sum())
    exch_ahead_max_ns = int((m_exc[exch_ahead_mask] - m_loc[exch_ahead_mask]).max()) if exch_ahead_count else 0
    if exch_ahead_count:
        keep_clk = ~exch_ahead_mask
        book_locals = np.ascontiguousarray(m_loc[keep_clk], dtype=np.int64)
        book_exchanges = np.ascontiguousarray(m_exc[keep_clk], dtype=np.int64)
    else:
        book_locals = np.ascontiguousarray(m_loc, dtype=np.int64)
        book_exchanges = np.ascontiguousarray(m_exc, dtype=np.int64)

    # 3. Strictly causal 600s time-weighted basis & sigma on 5s clock
    bin_ts_dedup, bin_mid_dedup = dedup_last_by_ts(binance_visible_ns, binance_mid)
    maker_ts_dedup, maker_mid_dedup = dedup_last_by_ts(m_loc, m_mid)
    trade_start_ns = int(day_start_ns) + train_minutes * 60 * NS
    trade_stop_ns = min(int(day_end_ns), trade_start_ns + trade_minutes * 60 * NS)
    grid_stop_ns = min(trade_stop_ns, int(min(bin_ts_dedup[-1], maker_ts_dedup[-1])) + 1)
    grid, basis, sigma, cov = exact_moments_5s(
        bin_ts_dedup, bin_mid_dedup, maker_ts_dedup, maker_mid_dedup, trade_start_ns, grid_stop_ns
    )

    pretrade_mid_arr = maker_mid_dedup[maker_ts_dedup < trade_start_ns]
    if not len(pretrade_mid_arr):
        raise ValueError(f"missing {spec['exchange']} pretrade L1 midpoints before {trade_start_ns}")
    scenario = compute_strategy_scenario(
        float(np.median(pretrade_mid_arr)),
        spec,
        target_notional_usdt=float(strat["target_notional_usdt"]),
        risk_aversion_per_usdt=float(strat["risk_aversion_per_usdt"]),
        inventory_multiple=int(strat["inventory_multiple"]),
        maker_fee_rate=float(fees_cfg["engine_maker_fee_rate"]),
        taker_fee_rate=float(fees_cfg["engine_taker_fee_rate"]),
    )

    def make_asset():
        margin = max(1000.0 * scenario["tick"], 0.02 * (roi_low + roi_high) / 2.0)
        return (
            BacktestAsset()
            .data([str(hbt_path)])
            .linear_asset(scenario["ctval"])
            .constant_order_latency(order_entry_ns, order_resp_ns)
            .power_prob_queue_model(2.0)
            .no_partial_fill_exchange()
            .trading_value_fee_model(scenario["maker_fee_rate"], scenario["taker_fee_rate"])
            .tick_size(scenario["tick"])
            .lot_size(scenario["lot"])
            .roi_lb(max(scenario["tick"], roi_low - margin))
            .roi_ub(roi_high + margin)
            .last_trades_capacity(20_000)
        )

    core_options = {
        "min_requote_ns": min_requote_ns,
        "fit_mode": str(strat["fit_mode"]),
        "wait_book_after_fill": bool(strat["wait_book_after_fill"]),
        "post_only_clamp_mode": str(strat["post_only_clamp_mode"]),
        "cap_depth_at_visible_levels": bool(strat["cap_depth_at_visible_levels"]),
        "quote_tick_mult": int(scenario["quote_tick_mult"]),
        "cross_timestamp_tie_mode": str(strat["cross_timestamp_tie_mode"]),
        "gate_book_local_ns": book_locals,
        "gate_book_exchange_ns": book_exchanges,
        "depth_is_20_level_snapshot": False,
        "forced_start_ns": 0,
    }

    # 4. Shared calibration witness (cap=0, feed-only)
    t_wit = time.time()
    w_stop = witness_stop_ns if witness_stop_ns is not None else min(trade_stop_ns, int(day_start_ns) + 1800 * NS)
    shared_start = None
    for stop_try in (w_stop, trade_stop_ns):
        witness = ROIVectorMarketDepthBacktest([make_asset()])
        try:
            probe_diag = np.zeros(12, dtype=np.float64)
            raw_probe = run_core_glft(
                witness, binance_visible_ns, binance_mid, trade_steps,
                0.0, grid, basis, sigma, 0, scenario["gamma_code"],
                scenario["tick"], scenario["ctval"], scenario["qty"], 0.0, None,
                stop_try, diagnostics=probe_diag, **core_options,
            )
            if probe_diag[5] == 1 and probe_diag[3] >= 0:
                shared_start = int(raw_probe[-3]) + int(probe_diag[3])
                break
        finally:
            witness.close()
    if shared_start is None:
        raise RuntimeError("no valid first physical GLFT calibration in witness")
    witness_s = round(time.time() - t_wit, 2)
    core_options["forced_start_ns"] = shared_start
    print("WITNESS_DONE", f"date={date_utc}", f"shared_start_ns={shared_start}", f"elapsed={witness_s}s", flush=True)

    # Resolve Stage 4 locked control parameters if authorized
    resolved_controls: dict[str, dict[str, float]] = dict(control_params or {})
    stage4_status: dict[str, Any]
    if stage4_lock is not None:
        locked_params = verify_stage4_lock(stage4_lock, [date_utc])
        resolved_controls.update(locked_params)
        stage4_status = {
            "status": "evaluated_with_independent_lock",
            "executed": True,
            "lock_sha256": stage4_lock["lock_sha256"],
            "training_dates": stage4_lock["training_dates"],
            "selected_controls": stage4_lock["selected_controls"],
        }
    elif "stage4" in stages_set:
        stage4_status = {
            "status": "pending_independent_training_date",
            "executed": False,
            "authorized": bool(cfg["stage4"].get("authorized", False)),
            "reason": (
                "No independent non-overlapping training date lock provided; refusing to train Stage 4 controls "
                "on evaluation date or reuse historical Tardis-trained parameters."
            ),
        }
    else:
        stage4_status = {"status": "skipped_by_config", "executed": False}

    # Prepare interruption audit intervals & probes
    audit_iv_arr = (
        np.asarray(episodes, dtype=np.int64).reshape(-1, 2)
        if episodes else np.empty((0, 2), dtype=np.int64)
    )
    probe_list: list[int] = []
    for s, e in episodes:
        probe_list.append(int(s))
        probe_list.append(int(e) if int(e) > int(s) else int(s) + 1)
    probe_arr = np.asarray(probe_list, dtype=np.int64) if probe_list else None

    effective_parameters = {
        "data_source": cfg["data_source"],
        "signal_exchange": cfg["signal_exchange"],
        "making_exchange": cfg["making_exchange"],
        "base_symbol": cfg["base_symbol"],
        "signal_native_symbol": cfg["signal_native_symbol"],
        "making_native_symbol": cfg["making_native_symbol"],
        "signal_visible_clock": (
            "raw_local_ts_ns (exact local receipt timestamp)"
            if extra_delay_ns == 0 else f"raw_local_ts_ns + {extra_delay_ns / 1e6:.3f}ms"
        ),
        "binance_extra_visible_delay_ns": extra_delay_ns,
        "binance_extra_visible_delay_ms": extra_delay_ns / 1e6,
        "maker_market_data_clock": "raw_local_ts_ns (exact local receipt timestamp)",
        "order_entry_latency_ns": order_entry_ns,
        "order_response_latency_ns": order_resp_ns,
        "order_roundtrip_latency_ms": (order_entry_ns + order_resp_ns) / 1e6,
        "order_latency_assumption_note": lat["order_latency_assumption_note"],
        "min_requote_ns": min_requote_ns,
        "min_requote_ms": min_requote_ns / 1e6,
        "native_order_modify_enabled": False,
        "ack_gating_and_cancellable_preserved": True,
        "post_only_gtx_preserved": True,
        "fit_mode": core_options["fit_mode"],
        "wait_book_after_fill": core_options["wait_book_after_fill"],
        "post_only_clamp_mode": core_options["post_only_clamp_mode"],
        "cap_depth_at_visible_levels": core_options["cap_depth_at_visible_levels"],
        "quote_tick_mult": core_options["quote_tick_mult"],
        "cross_timestamp_tie_mode": core_options["cross_timestamp_tie_mode"],
        "train_seconds": train_minutes * 60,
        "recalibration_interval_seconds": int(strat["recalibration_interval_seconds"]),
        "trade_minutes": trade_minutes,
        "trade_steps": trade_steps,
        "shared_first_eligible_entry_ns": int(shared_start),
        "scenario": scenario,
        "queue_model": "power_prob_queue_model(2.0)",
        "exchange_model": "no_partial_fill_exchange",
        "primary_pnl_basis": "zero_fee_gross_marked_pnl_usdt",
        "supplementary_maker_rates_bps": list(fees_cfg["supplementary_maker_rates_bps"]),
        "maker_l1_exch_ahead_filtered_count": exch_ahead_count,
        "maker_l1_exch_ahead_max_ns": exch_ahead_max_ns,
    }

    # Build final model run list (skip control models if not parameterized/locked)
    models_to_run = []
    for mname in cfg["models"]:
        reg = MODEL_REGISTRY[mname]
        if reg["control_kind"] == 1 and "static_widen" not in resolved_controls:
            continue
        if reg["control_kind"] == 2 and "local_vol_pause" not in resolved_controls:
            continue
        models_to_run.append(mname)
    if stage4_lock is not None:
        for cm in ("static_widen", "local_vol_pause"):
            if cm not in models_to_run:
                models_to_run.append(cm)

    models_out: dict[str, Any] = {}
    internal_artifacts: dict[str, Any] = {}
    stage1_out: dict[str, Any] = {}
    stage2_out: dict[str, Any] = {}

    for name in models_to_run:
        reg = MODEL_REGISTRY[name]
        beta = float(strat["beta"]) * float(reg["beta_scale"])
        quote_mode = int(reg["quote_mode"])
        control_kind = int(reg["control_kind"])
        extra_half_ticks = float(resolved_controls.get("static_widen", {}).get("extra_half_ticks", 0.0)) if control_kind == 1 else 0.0
        local_sigma_ceiling = float(resolved_controls.get("local_vol_pause", {}).get("local_sigma_ceiling", np.inf)) if control_kind == 2 else float(np.inf)

        print("MODEL_START", date_utc, name, flush=True)
        t_m = time.time()
        engine = ROIVectorMarketDepthBacktest([make_asset()])
        recorder = Recorder(engine.num_assets, trade_steps + 100)
        diag = np.zeros(12, dtype=np.float64)
        audit_counts = np.zeros((len(audit_iv_arr), 3), dtype=np.int64)
        try:
            raw = run_core_glft(
                engine, binance_visible_ns, binance_mid, trade_steps,
                beta, grid, basis, sigma, quote_mode, scenario["gamma_code"],
                scenario["tick"], scenario["ctval"], scenario["qty"], scenario["position_cap"],
                recorder.recorder, trade_stop_ns, probe_arr, False,
                control_kind, extra_half_ticks, local_sigma_ceiling,
                diagnostics=diag,
                audit_intervals_ns=(audit_iv_arr if len(audit_iv_arr) else None),
                audit_counts=(audit_counts if len(audit_iv_arr) else None),
                **core_options,
            )
            if diag[5] != 1:
                raise RuntimeError(f"{name}: shared GLFT calibration invalid")
            first_fit = int(raw[-3]) + int(diag[3])
            if first_fit != shared_start:
                raise RuntimeError(f"{name}: first_fit_ns {first_fit} != shared {shared_start}")

            summary = summarize(raw, trade_minutes, True, beta, scenario["position_cap"], scenario["qty"],
                                tail_tolerance_samples=25)
            end_time_ns = int(summary["end_time_ns"])
            if end_time_ns < trade_stop_ns - NS:
                raise RuntimeError(f"{name}: replay ended >1s early ({end_time_ns} < {trade_stop_ns})")

            records = recorder.get(0).copy()
            if len(records) < 1 or np.any(np.diff(records["timestamp"]) <= 0):
                raise RuntimeError(f"{name}: invalid Recorder timeline")

            trading = dict(summary["trading"])
            active_duration_s = max(len(records), 1) * 0.1
            pos_std_c = float(np.std(records["position"]))
            pos_mean_c = float(np.mean(records["position"]))
            trading["position_std_contracts"] = pos_std_c
            trading["position_std_base"] = pos_std_c * scenario["ctval"]
            trading["position_mean_contracts"] = pos_mean_c
            trading["pos_std_over_order_qty"] = pos_std_c / scenario["qty"]
            trading["mean_abs_inventory_contracts"] = float(raw[23]) / active_duration_s
            trading["rms_inventory_contracts"] = math.sqrt(max(0.0, float(raw[24])) / active_duration_s)
            trading["max_abs_position_base"] = float(trading["max_abs_position_contracts"]) * scenario["ctval"]
            trading["end_position_base"] = float(trading["end_position_contracts"]) * scenario["ctval"]
            trading["active_inventory_samples"] = int(len(records))
            trading["effective_trading_seconds"] = round(active_duration_s, 1)

            turnover = float(trading["trading_value_usdt"])
            net_2bps = float(trading["end_net_marked_equity_usdt"])
            fee_2bps = float(trading["fee_usdt"])
            balance = float(trading["balance_usdt"])
            end_pos = float(trading["end_position_contracts"])
            end_mid = float(trading["end_mid_usdt_per_btc"])
            gross = net_2bps + fee_2bps
            gross_from_ledger = balance + scenario["ctval"] * end_pos * end_mid

            accounting_checks = {
                "fills_count_exact": (
                    int(trading["buy_fills"]) + int(trading["sell_fills"])
                    == int(trading["engine_fills"]) == int(len(raw[-5]))
                ),
                "fee_turnover_identity": abs(fee_2bps - scenario["maker_fee_rate"] * turnover) <= 1e-6,
                "gross_ledger_identity": abs(gross - gross_from_ledger) <= 1e-6,
                "net_gross_fee_identity": abs(net_2bps - (gross - fee_2bps)) <= 1e-6,
                "position_within_cap": float(trading["max_abs_position_contracts"]) <= scenario["position_cap"] + scenario["qty"] + 1e-9,
                "shared_start_exact": first_fit == shared_start,
            }
            if not all(accounting_checks.values()):
                raise RuntimeError(f"{name}: accounting verification failed: {accounting_checks}")

            trading["gross_marked_pnl_usdt"] = gross
            trading["gross_pnl_per_turnover_bps"] = (10_000.0 * gross / turnover) if turnover > 0 else None
            trading["net_marked_pnl_usdt_2bps"] = net_2bps
            trading["net_pnl_per_turnover_bps_2bps"] = (10_000.0 * net_2bps / turnover) if turnover > 0 else None
            trading["fee_usdt_2bps"] = fee_2bps
            trading["modify_requests"] = 0
            trading["fee_scenarios"] = _fee_scenarios(gross, turnover, list(fees_cfg["supplementary_maker_rates_bps"]))

            fills = {
                "exchange_timestamp_ns": np.asarray(raw[-5], dtype=np.int64),
                "side": np.asarray(raw[-4], dtype=np.int64),
                "local_observed_ns": np.asarray(raw[-2][1], dtype=np.int64),
                "exec_price": np.asarray(raw[-2][2], dtype=np.float64),
                "exec_qty": np.asarray(raw[-2][3], dtype=np.float64),
            }

            # Stage 1 accounting closure
            if "stage1" in stages_set or "stage5a" in stages_set:
                stage1_out[name] = run_stage1_accounting(
                    records, fills, trading, end_time_ns, scenario["ctval"], date_utc, name
                )

            # Stage 2 execution-price Total Markout & fill quality
            annotated, quality = model_day(
                name, fills, {"trading": trading}, maker_ts_dedup, maker_mid_dedup, int(day_end_ns), scenario["ctval"]
            )
            stage2_out[name] = quality

            effective_begin = max(int(raw[-3]) + train_minutes * 60 * NS, int(shared_start))
            anomaly_impact = descriptive_anomaly_impact(
                annotated, episodes, audit_counts, probe_arr, raw[-2][4], effective_begin
            )

            origin_filter = {
                k: int(v) for k, v in zip(
                    ("binance_price_update_groups", "eligible_gap_checks",
                     "above_threshold_checks", "directional_dominance_checks",
                     "binance_origin_activations", "active_decisions",
                     "same_timestamp_ties_blocked"),
                    raw[-1],
                )
            }
            origin_filter["microsecond_collisions"] = int(diag[11])

            model_entry: dict[str, Any] = {
                "model": name,
                "family": reg["family"],
                "beta": beta,
                "quote_mode": quote_mode,
                "control_kind": control_kind,
                "elapsed_s": round(time.time() - t_m, 2),
                "first_fit_ns": first_fit,
                "effective_trading_start_ns": effective_begin,
                "end_time_ns": end_time_ns,
                "decisions": int(raw[1]),
                "submissions": int(trading["submissions"]),
                "cancel_requests": int(trading["cancel_requests"]),
                "modify_requests": 0,
                "max_ops_per_sec": int(diag[0]),
                "resting_time_ratio": {
                    "bid": float(diag[1]),
                    "ask": float(diag[2]),
                    "verified_exchange_resting": False,
                },
                "binance_origin_filter": origin_filter,
                "trading": trading,
                "accounting_checks": accounting_checks,
                "fill_quality": quality,
                "anomaly_impact": anomaly_impact,
                "initial_parameters": summary.get("initial_parameters"),
                "diagnostics": {
                    "max_ops_per_sec": int(diag[0]),
                    "first_valid_fit_offset_ns": int(diag[3]),
                    "shared_calibration_valid": bool(diag[5] == 1),
                    "microsecond_collisions": int(diag[11]),
                },
            }
            if control_kind == 1:
                model_entry["extra_half_ticks"] = extra_half_ticks
            if control_kind == 2:
                model_entry["local_sigma_ceiling"] = local_sigma_ceiling
            if name == "glft":
                model_entry["calibration_trace"] = summary.get("calibration_trace", [])

            models_out[name] = model_entry
            internal_artifacts[name] = {
                "fills": fills,
                "annotated": annotated,
                "trading": trading,
            }
            print(
                "MODEL_DONE", date_utc, name,
                f"fills={trading['engine_fills']}",
                f"gross={gross:.4f}",
                f"net2bps={net_2bps:.4f}",
                f"elapsed={model_entry['elapsed_s']}s",
                flush=True,
            )
        finally:
            engine.close()

    # Stage 3 shadow audit on baseline `glft`
    stage3_report: dict[str, Any]
    stage3_classified: dict[str, Any] | None = None
    if "stage3" in stages_set and "glft" in internal_artifacts:
        print("STAGE3_SHADOW_START", date_utc, flush=True)
        stage3_classified, stage3_report = run_stage3_shadow_audit(
            date_utc=date_utc,
            make_asset=make_asset,
            binance_visible_ns=binance_visible_ns,
            binance_mid=binance_mid,
            grid=grid,
            basis=basis,
            sigma=sigma,
            scenario=scenario,
            core_options=core_options,
            trade_steps=trade_steps,
            trade_minutes=trade_minutes,
            trade_stop_ns=trade_stop_ns,
            baseline_result=internal_artifacts["glft"],
            comparison_fills={k: v["fills"] for k, v in internal_artifacts.items() if k != "glft"},
            order_entry_latency_ns=order_entry_ns,
            guard_ns=guard_ns,
            min_cancel_ns=min_requote_ns,
        )
        print(
            "STAGE3_SHADOW_DONE", date_utc,
            f"parity={stage3_report['parity_verified']}",
            f"counts={stage3_report.get('category_counts')}",
            flush=True,
        )
    else:
        stage3_report = {"status": "skipped", "executed": False}

    # Stage 5A ablation
    stage5a_report = (
        run_stage5a_ablation(
            date_utc=date_utc,
            models_out=models_out,
            stage1_out=stage1_out,
            stage2_out=stage2_out,
            maker_rates_bps=list(fees_cfg["supplementary_maker_rates_bps"]),
        )
        if ("stage5a" in stages_set or "stage5b" in stages_set)
        else {"status": "skipped", "executed": False}
    )

    # Stage 5B same-fill alignment & decomposition
    if "stage5b" in stages_set and stage3_classified is not None and "glft" in internal_artifacts:
        stage5b_report = run_stage5b_same_fill_audit(
            date_utc=date_utc,
            stage2_annotated_glft=internal_artifacts["glft"]["annotated"],
            stage3_classified=stage3_classified,
            stage5a_report=stage5a_report,
            comparison_fills={k: v["fills"] for k, v in internal_artifacts.items() if k != "glft"},
        )
    else:
        stage5b_report = {"status": "skipped", "executed": False}

    # Stage 5C status (holdout evaluation only when explicitly authorized + pre-locked)
    if stage5c_lock is not None:
        selected = verify_stage5c_lock(stage5c_lock, [date_utc])
        stage5c_report = {
            "status": "evaluated_with_preregistered_lock",
            "executed": True,
            "lock_sha256": stage5c_lock["lock_sha256"],
            "selected_mode": selected,
            "holdout_chronology": stage5c_lock["holdout_chronology"],
            "explored_stage5a_dates": stage5c_lock["explored_stage5a_dates"],
            "selected_model_result": stage5a_report.get("models", {}).get(selected),
            "baseline_glft_result": stage5a_report.get("models", {}).get("glft"),
        }
    elif "stage5c" in stages_set:
        stage5c_report = {
            "status": "pending_authorized_holdout_date",
            "executed": False,
            "authorized": bool(cfg["stage5c"].get("authorized", False)),
            "holdout_chronology": cfg["dates"]["holdout_chronology"],
            "reason": (
                "No authorized untouched holdout date & pre-registered mode lock provided; "
                "refusing to run Stage 5C on explored evaluation date."
            ),
        }
    else:
        stage5c_report = {"status": "skipped_by_config", "executed": False}

    if not keep_calibration_trace:
        for m in models_out.values():
            m.pop("calibration_trace", None)

    return {
        "date_utc": date_utc,
        "effective_parameters": effective_parameters,
        "witness_elapsed_s": witness_s,
        "basis_grid_points": int(len(grid)),
        "basis_finite_points": int(np.isfinite(basis).sum()),
        "basis_median_coverage": float(np.nanmedian(cov)) if len(cov) else 0.0,
        "models": models_out,
        "stage1": {"status": "complete" if stage1_out else "skipped", "models": stage1_out},
        "stage2": {"status": "complete" if stage2_out else "skipped", "models": stage2_out},
        "stage3": stage3_report,
        "stage4": stage4_status,
        "stage5a": stage5a_report,
        "stage5b": stage5b_report,
        "stage5c": stage5c_report,
    }
