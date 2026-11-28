"""Unified experiment configuration loader and validator for self-collected Tokyo L2 research."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any

from tokyo_l2_exchange_adapters import (
    MAKING_EXCHANGES,
    SIGNAL_EXCHANGES,
    normalize_contract_spec,
    to_base_symbol,
    to_native_symbol,
)

DATA_SOURCES = ("google_drive_tokyo_l2",)
ALL_STAGES = ("stage1", "stage2", "stage3", "stage4", "stage5a", "stage5b", "stage5c")
CORE_MODELS = ("glft", "both_1p0", "defensive_1p0", "risk_side_pause")
CONTROL_MODELS = ("static_widen", "local_vol_pause")
ALL_MODELS = CORE_MODELS + CONTROL_MODELS

# Canonical mapping into scripts/core_glft_engine.py::run_core_glft
MODEL_REGISTRY: dict[str, dict[str, Any]] = {
    "glft": {"beta_scale": 0.0, "quote_mode": 0, "control_kind": 0, "family": "baseline"},
    "both_1p0": {"beta_scale": 1.0, "quote_mode": 1, "control_kind": 0, "family": "cross_exchange_signal"},
    "defensive_1p0": {"beta_scale": 1.0, "quote_mode": 2, "control_kind": 0, "family": "cross_exchange_signal"},
    "risk_side_pause": {"beta_scale": 1.0, "quote_mode": 3, "control_kind": 0, "family": "cross_exchange_signal"},
    "static_widen": {"beta_scale": 0.0, "quote_mode": 0, "control_kind": 1, "family": "stage4_control"},
    "local_vol_pause": {"beta_scale": 0.0, "quote_mode": 0, "control_kind": 2, "family": "stage4_control"},
}

MODEL_ALIASES = {"gate_sigma_pause": "local_vol_pause"}

DEFAULT_CONFIG: dict[str, Any] = {
    "experiment_name": "tokyo_l2_unified",
    "data_source": "google_drive_tokyo_l2",
    "signal_exchange": "binance_futures",
    "making_exchange": "gate_futures",
    "symbol": "BTC",
    "dates": {
        "eval_dates": ["2026-11-23"],
        "train_dates": [],
        "holdout_dates": [],
        "holdout_chronology": "forward_oos",
    },
    "contract_spec": {
        "mode": "auto_live",
        "quote_tick_mult": 1,
        "hbt_tick_size_override": None,
        "explicit": None,
    },
    "strategy": {
        "target_notional_usdt": 80.0,
        "risk_aversion_per_usdt": 50.0,
        "inventory_multiple": 20,
        "train_minutes": 10,
        "trade_minutes": 1430,
        "recalibration_interval_seconds": 5,
        "fit_mode": "physical_1tick_poisson",
        "sigma_threshold": 1.645,
        "beta": 1.0,
        "wait_book_after_fill": True,
        "post_only_clamp_mode": "unified",
        "cap_depth_at_visible_levels": False,
        "cross_timestamp_tie_mode": "microsecond",
    },
    "latency": {
        "binance_extra_visible_delay_ns": 0,
        "order_entry_latency_ns": 1_000_000,
        "order_response_latency_ns": 1_000_000,
        "order_latency_assumption_note": (
            "Uniform 1ms one-way simulated assumption; not measured live exchange order latency"
        ),
    },
    "order_controls": {
        "min_requote_ns": 0,
        "native_order_modify_enabled": False,
        "stage3_guard_ns": 1_000,
    },
    "models": list(CORE_MODELS),
    "stages": list(ALL_STAGES),
    "stage4": {
        "authorized": False,
        "lock_file": None,
        "widen_ratios": [0.10, 0.25, 0.50, 1.00],
        "vol_quantiles": [0.55, 0.70, 0.85, 0.95],
    },
    "stage5c": {
        "authorized": False,
        "lock_file": None,
        "selection_metric": "gross_marked_pnl_usdt",
        "rank_order": ["both_1p0", "defensive_1p0", "risk_side_pause"],
        "rms_multiplier_cap": 1.25,
    },
    "fees": {
        "primary_zero_fee": True,
        "engine_maker_fee_rate": 0.0002,
        "engine_taker_fee_rate": 0.0005,
        "supplementary_maker_rates_bps": [2.0, -0.5],
    },
    "cache": {
        "reuse_converted": True,
        "cache_input_dir": None,
        "converter_version": "stream_convert_v3",
    },
    "execution": {
        "batch_windows": 8,
        "download_workers": 8,
        "diagnostic_allow_known_gaps": True,
    },
}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out = deepcopy(base)
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = deepcopy(v)
    return out


def _validate_date(date_str: str) -> str:
    s = str(date_str).strip()
    dt = datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if dt.strftime("%Y-%m-%d") != s:
        raise ValueError(f"invalid UTC date format: {date_str!r}")
    return s


def canonical_model_name(name: str) -> str:
    n = str(name).strip()
    n = MODEL_ALIASES.get(n, n)
    if n not in MODEL_REGISTRY:
        raise ValueError(f"unsupported model {name!r}; allowed: {ALL_MODELS}")
    return n


def validate_config(raw: dict[str, Any]) -> dict[str, Any]:
    """Merge with defaults, resolve native symbols, and enforce anti-leakage / parameter invariants."""
    cfg = _deep_merge(DEFAULT_CONFIG, raw)

    if cfg["data_source"] not in DATA_SOURCES:
        raise ValueError(f"unsupported data_source {cfg['data_source']!r}; allowed: {DATA_SOURCES}")
    if cfg["signal_exchange"] not in SIGNAL_EXCHANGES:
        raise ValueError(f"unsupported signal_exchange {cfg['signal_exchange']!r}; allowed: {SIGNAL_EXCHANGES}")
    if cfg["making_exchange"] not in MAKING_EXCHANGES:
        raise ValueError(f"unsupported making_exchange {cfg['making_exchange']!r}; allowed: {MAKING_EXCHANGES}")

    base = to_base_symbol(cfg["symbol"])
    cfg["base_symbol"] = base
    cfg["signal_native_symbol"] = to_native_symbol(cfg["signal_exchange"], base)
    cfg["making_native_symbol"] = to_native_symbol(cfg["making_exchange"], base)

    dates = cfg["dates"]
    eval_dates = [_validate_date(d) for d in dates.get("eval_dates") or []]
    train_dates = [_validate_date(d) for d in dates.get("train_dates") or []]
    holdout_dates = [_validate_date(d) for d in dates.get("holdout_dates") or []]
    if not eval_dates and not train_dates and not holdout_dates:
        raise ValueError("at least one date must be configured in dates.{eval_dates,train_dates,holdout_dates}")
    if len(set(eval_dates)) != len(eval_dates):
        raise ValueError(f"duplicate dates in eval_dates: {eval_dates}")
    if set(train_dates) & set(eval_dates):
        raise ValueError(f"Stage 4 leakage guard: train_dates {train_dates} overlaps eval_dates {eval_dates}")
    if set(holdout_dates) & (set(eval_dates) | set(train_dates)):
        raise ValueError(
            f"Stage 5C isolation guard: holdout_dates {holdout_dates} overlaps explored eval/train dates"
        )
    chronology = str(dates.get("holdout_chronology") or "forward_oos")
    if chronology not in ("forward_oos", "historical_robustness"):
        raise ValueError(f"invalid holdout_chronology {chronology!r}")
    dates["eval_dates"] = eval_dates
    dates["train_dates"] = train_dates
    dates["holdout_dates"] = holdout_dates
    dates["holdout_chronology"] = chronology

    cspec = cfg["contract_spec"]
    if cspec["mode"] not in ("auto_live", "explicit"):
        raise ValueError(f"contract_spec.mode must be 'auto_live' or 'explicit', got {cspec['mode']!r}")
    if cspec["mode"] == "explicit":
        if not isinstance(cspec.get("explicit"), dict):
            raise ValueError("contract_spec.mode='explicit' requires contract_spec.explicit dict")
        exp = dict(cspec["explicit"])
        exp.setdefault("exchange", cfg["making_exchange"])
        exp.setdefault("base_symbol", base)
        exp.setdefault("native_symbol", cfg["making_native_symbol"])
        exp.setdefault("quote_tick_mult", cspec.get("quote_tick_mult", 1))
        cspec["explicit"] = normalize_contract_spec(exp)

    lat = cfg["latency"]
    for k in ("binance_extra_visible_delay_ns", "order_entry_latency_ns", "order_response_latency_ns"):
        lat[k] = int(lat[k])
        if lat[k] < 0:
            raise ValueError(f"latency.{k} must be >= 0, got {lat[k]}")
    oc = cfg["order_controls"]
    oc["min_requote_ns"] = int(oc["min_requote_ns"])
    oc["stage3_guard_ns"] = int(oc.get("stage3_guard_ns", 1_000))
    if oc["min_requote_ns"] < 0 or oc["stage3_guard_ns"] < 0:
        raise ValueError("min_requote_ns and stage3_guard_ns must be >= 0")
    if oc.get("native_order_modify_enabled"):
        raise ValueError("native_order_modify_enabled=True is prohibited; must remain False")

    strat = cfg["strategy"]
    if strat["post_only_clamp_mode"] not in ("unified", "legacy"):
        raise ValueError("strategy.post_only_clamp_mode must be 'unified' or 'legacy'")
    if strat["cross_timestamp_tie_mode"] not in ("microsecond", "legacy_ns"):
        raise ValueError("strategy.cross_timestamp_tie_mode must be 'microsecond' or 'legacy_ns'")
    if float(strat["sigma_threshold"]) != 1.645:
        raise ValueError("Frozen core_glft_engine threshold is 1.645 sigma; do not alter strategy threshold")
    if float(strat["beta"]) < 0 or float(strat["target_notional_usdt"]) <= 0:
        raise ValueError("invalid strategy beta or target_notional_usdt")

    models = []
    for m in cfg["models"]:
        cm = canonical_model_name(m)
        if cm not in models:
            models.append(cm)
    if not models:
        raise ValueError("models list must not be empty")
    cfg["models"] = models

    stages = []
    for s in cfg["stages"]:
        sn = str(s).strip().lower()
        if sn not in ALL_STAGES:
            raise ValueError(f"unsupported stage {s!r}; allowed: {ALL_STAGES}")
        if sn not in stages:
            stages.append(sn)
    cfg["stages"] = stages

    cfg["config_sha256"] = config_digest(cfg)
    return cfg


def config_digest(cfg: dict[str, Any]) -> str:
    clean = {k: v for k, v in cfg.items() if k != "config_sha256"}
    return hashlib.sha256(json.dumps(clean, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def load_experiment_config(path: str | Path) -> dict[str, Any]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    return validate_config(raw)
