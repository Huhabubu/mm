"""Read-only Gate 1.645-sigma GLFT stage-1 accounting audit.

Input: six ZIPs downloaded from GitHub Actions #43786928308 and renamed
gate-YYYY-MM-01.zip. The strategy, HBT queue and matching are unchanged.

Each 100ms interval admits two exact accounting conventions. They give
the same total PnL but a different split of execution edge versus inventory.
These are diagnostic allocations, not exact execution-time half-spreads.
"""
import argparse
import csv
import io
import json
import math
import zipfile
from pathlib import Path

import numpy as np

DAYS = tuple(f"2026-{m:02d}-01" for m in range(6, 12))
MODELS = ("glft", "both_1p0")


def load_npz(archive, name):
    with np.load(io.BytesIO(archive.read(name)), allow_pickle=False) as obj:
        return {k: obj[k] for k in obj.files}


def analyze_case(archive, day, model, report, tolerance=0.00015,
                 min_recorder_points=850000):
    if report.get("day") != day or report.get("status") != "complete":
        raise ValueError(f"Incomplete/wrong day: {day}")
    if report.get("minutes_traded") != 1430:
        raise ValueError(f"Expected 1430 trading minutes: {day}")
    if report.get("experimental_trigger_sigma_multiple") != 1.645:
        raise ValueError(f"Wrong sigma threshold: {day}")
    if report.get("binance_extra_visible_delay_ms") != 2.5:
        raise ValueError(f"Wrong Binance visibility delay: {day}")
    cfg = report["scenario"]
    if abs(float(cfg["maker_fee_rate"]) - 0.0002) > 1e-12:
        raise ValueError(f"Expected positive 2bps maker fee: {day}")
    value = float(cfg["ctval"])  # base-asset units per Gate contract
    path = f"recorder/{day}/{model}/"
    loaded = load_npz(archive, path + "recorder.npz")
    if set(loaded) != {"0"}:
        raise ValueError(f"Unexpected Recorder assets: {day}, {model}")
    tape = loaded["0"]
    fill = load_npz(archive, path + "fills.npz")
    stamp = np.asarray(tape["timestamp"], dtype=np.int64)
    mid = np.asarray(tape["price"], dtype=np.float64)
    pos = np.asarray(tape["position"], dtype=np.float64)
    bal = np.asarray(tape["balance"], dtype=np.float64)
    if len(stamp) < min_recorder_points or np.any(np.diff(stamp) <= 0):
        raise ValueError(f"Missing/unsorted Recorder states: {day}, {model}")
    if not (np.isfinite(mid).all() and np.isfinite(pos).all()
            and np.isfinite(bal).all()):
        raise ValueError(f"Invalid Recorder field: {day}, {model}")
    required = ("exchange_timestamp_ns", "local_observed_ns",
                "side", "exec_price", "exec_qty")
    if set(fill) != set(required):
        raise ValueError(f"Wrong fill columns: {day}, {model}")
    n = len(fill["side"])
    if any(len(fill[k]) != n for k in required):
        raise ValueError(f"Unbalanced fill arrays: {day}, {model}")
    perm = np.argsort(fill["local_observed_ns"], kind="stable")
    t = np.asarray(fill["local_observed_ns"][perm], dtype=np.int64)
    exch_t = np.asarray(fill["exchange_timestamp_ns"][perm], dtype=np.int64)
    side = np.asarray(fill["side"][perm], dtype=np.int64)
    px = np.asarray(fill["exec_price"][perm], dtype=np.float64)
    qty = np.asarray(fill["exec_qty"][perm], dtype=np.float64)
    if np.any((side != 1) & (side != -1)) or np.any(qty <= 0):
        raise ValueError(f"Invalid fill side/size: {day}, {model}")
    if not (np.isfinite(px).all() and np.isfinite(qty).all()
            and (px > 0).all()) or np.any(exch_t > t):
        raise ValueError(f"Invalid fill price/timestamp: {day}, {model}")
    if n and (t[0] < stamp[0]
              or t[-1] > report["variants"][model]["end_time_ns"]):
        raise ValueError(f"Fill outside Recorder interval: {day}, {model}")
    trading = report["variants"][model]["trading"]
    if n != int(trading["engine_fills"]):
        raise ValueError(f"Official fill-count mismatch: {day}, {model}")
    signed = side * qty
    cumulative_qty = np.r_[0.0, np.cumsum(signed)]
    cumulative_cost = np.r_[0.0, np.cumsum(signed * px * value)]
    end_idx = np.searchsorted(t, stamp, side="right")
    pos_error = float(np.max(np.abs(pos - pos[0] - cumulative_qty[end_idx])))
    cash_error = float(np.max(np.abs(bal - bal[0] + cumulative_cost[end_idx])))
    if pos_error > 1e-7 or cash_error > tolerance:
        raise ValueError(f"Recorder/cash/fill mismatch: {day}, {model}, "
                         f"pos={pos_error}, balance={cash_error}")
    last_mid = float(trading["end_mid_usdt_per_base"] if "end_mid_usdt_per_base" in trading else trading["end_mid_usdt_per_btc"])
    last_pos = float(trading["end_position_contracts"])
    last_bal = float(trading["balance_usdt"])
    fee = float(trading["fee_usdt"])
    if abs(last_pos - pos[0] - cumulative_qty[-1]) > 1e-7:
        raise ValueError(f"Final inventory mismatch: {day}, {model}")
    if abs(last_bal - bal[0] + cumulative_cost[-1]) > tolerance:
        raise ValueError(f"Final cash mismatch: {day}, {model}")
    zero = (last_bal + value * last_pos * last_mid
            - bal[0] - value * pos[0] * mid[0])
    net = float(trading["end_net_marked_equity_usdt"]) - (
        bal[0] + value * pos[0] * mid[0])
    if abs(zero - fee - net) > tolerance:
        raise ValueError(f"Official fee/net mismatch: {day}, {model}")
    # Convention A: strictly preceding 100ms midpoint, ending inventory.
    prior = np.searchsorted(stamp, t, side="left") - 1
    if np.any(prior < 0):
        raise ValueError(f"Fill lacking preceding midpoint: {day}, {model}")
    edges = signed * value * (mid[prior] - px)
    buy_edge = float(np.sum(edges[side == 1]))
    sell_edge = float(np.sum(edges[side == -1]))
    capture = buy_edge + sell_edge
    inventory = float(value * np.dot(pos[1:], np.diff(mid))
                      + value * last_pos * (last_mid - mid[-1]))
    discrepancy = capture + inventory - zero
    if abs(discrepancy) > tolerance:
        raise ValueError(f"Accounting identity A failed: {day}, {model}")
    # Convention B: following 100ms midpoint, starting inventory.
    # This is ex-post sensitivity analysis; no future data enter decisions.
    following_idx = np.searchsorted(stamp, t, side="left")
    following = np.where(following_idx < len(mid),
                         mid[np.minimum(following_idx, len(mid)-1)], last_mid)
    next_capture = float(np.sum(signed * value * (following - px)))
    next_inventory = float(value * np.dot(pos[:-1], np.diff(mid))
                           + value * pos[-1] * (last_mid - mid[-1]))
    next_residual = next_capture + next_inventory - zero
    if abs(next_residual) > tolerance:
        raise ValueError(f"Accounting identity B failed: {day}, {model}")
    dt = np.diff(stamp).astype(np.float64) * 1e-9
    mean_abs_pos = float(np.dot(np.abs(pos[1:]), dt) / np.sum(dt))
    rms_pos = math.sqrt(float(np.dot(pos[1:] ** 2, dt) / np.sum(dt)))
    return {
        "date": day, "model": model, "fills": n,
        "buy_fills": int(np.sum(side == 1)),
        "sell_fills": int(np.sum(side == -1)),
        "buy_capture_usdt": buy_edge,
        "sell_capture_usdt": sell_edge,
        "spread_capture_usdt": capture,
        "inventory_price_pnl_usdt": inventory,
        "next_sample_capture_usdt": next_capture,
        "next_sample_inventory_price_pnl_usdt": next_inventory,
        "official_zero_fee_usdt": zero,
        "maker_fee_usdt": fee, "official_fee_net_usdt": net,
        "fill_notional_usdt": float(value * np.sum(qty * px)),
        "mean_abs_position_contracts": mean_abs_pos,
        "rms_position_contracts": rms_pos,
        "max_abs_position_contracts": float(np.max(np.abs(pos))),
        "end_position_contracts": last_pos,
        "recorder_fill_position_max_error": pos_error,
        "recorder_fill_cash_max_error_usdt": cash_error,
        "closure_residual_usdt": discrepancy,
        "next_sample_closure_residual_usdt": next_residual,
        "recorder_points": len(tape),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    root = Path(args.input)
    dest = Path(args.out)
    observations = []
    for day in DAYS:
        with zipfile.ZipFile(root / f"gate-{day}.zip") as archive:
            report = json.loads(archive.read(f"{day}_1430m.json"))
            for model in MODELS:
                result = analyze_case(archive, day, model, report)
                observations.append(result)
                print("ACCOUNTING_OK", day, model,
                      round(result["official_zero_fee_usdt"], 6), flush=True)
    dest.mkdir(parents=True, exist_ok=True)
    with (dest / "sixday_attribution.csv").open(
            "w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=list(observations[0]))
        writer.writeheader()
        writer.writerows(observations)
    keys = ("buy_capture_usdt", "sell_capture_usdt",
            "spread_capture_usdt", "inventory_price_pnl_usdt",
            "next_sample_capture_usdt", "next_sample_inventory_price_pnl_usdt",
            "official_zero_fee_usdt", "maker_fee_usdt",
            "official_fee_net_usdt", "fills", "fill_notional_usdt",
            "mean_abs_position_contracts", "rms_position_contracts")
    groups = {
        model: {"equal_day_mean_" + key:
                sum(x[key] for x in observations if x["model"] == model) / 6
                for key in keys}
        for model in MODELS
    }
    delta = {key: groups["both_1p0"][key] - groups["glft"][key]
             for key in groups["glft"]}
    outcome = {
        "status": "complete",
        "sample": "six previously explored UTC month-first days",
        "source": "official HBT Recorder and actual fills; 100ms sampling",
        "warning": "both accounting splits are sampling-convention-dependent",
        "models": groups, "beta1_minus_glft": delta,
    }
    (dest / "summary.json").write_text(
        json.dumps(outcome, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8")
    print("DONE", json.dumps(delta, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
