"""Official HBT replay aggregation."""
import math
import numpy as np
from hftbacktest import BUY, SELL
from glft_calibration import POSITION_CAP, ORDER_QTY, STEP_NS, TRAIN

MIN_REQUOTE_NS = 20_000_000  # Per side, at most one order action per 20ms.

def summarize(raw, minutes, rolling, beta=0.0, position_cap=POSITION_CAP,
              order_qty=ORDER_QTY, tail_tolerance_samples=5):
    """Aggregate only; no trades or raw market data leave the runner."""
    (samples, decisions, attempts, rejects, successes, initial_ok, updates,
     submits, cancels, buys, sells, fills, pos, max_pos, balance, fee,
     turnover, mid, equity, min_eq, max_eq, first_eq, drawdown,
     abs_seconds, sq_seconds, at_cap_seconds, bin_events, okx_events,
     order_events, reads, observed, fill_ts, fill_sides, start, end) = raw[:35]
    # Diagnostic variants return additional trace arrays with the end timestamp.
    if isinstance(end, tuple):
        end = end[0]
    if initial_ok != 1 or successes < 1:
        raise RuntimeError("G0-R first 10 minutes cannot identify GLFT parameters")
    if samples < TRAIN + minutes * 600 - tail_tolerance_samples:
        raise RuntimeError(f"Truncated market replay, samples={samples}")
    if fills != buys+sells or len(fill_ts) != fills:
        raise RuntimeError(f"Official fills and strategy accounting disagree: {fills} vs {buys+sells}")
    if max_pos > position_cap + order_qty + 1e-9:
        raise RuntimeError("Inventory hard cap exceeded")
    if not math.isfinite(equity):
        raise RuntimeError("Nonfinite marked equity")
    if np.any(fill_ts <= 0):
        raise RuntimeError("Invalid exchange fill timestamps")
    return {
        "mode": "rolling" if rolling else "static",
        "beta": float(beta), "parameter_observation_ms": 100,
        "rolling_fit_interval_s": 5 if rolling else None,
        "min_per_side_order_action_ms": MIN_REQUOTE_NS/1e6,
        "start_time_ns": int(start), "end_time_ns": int(end),
        "samples": int(samples), "decisions": int(decisions),
        "calibration_attempts": int(attempts),
        "calibration_rejections": int(rejects),
        "successful_calibrations": int(successes),
        "initial_parameters": {
            "A_per_second": float(updates[0,1]),
            "k_per_tick": float(updates[0,2]),
            "sigma_tick_per_sqrt_second": float(updates[0,3]),
            "half_spread_ticks": float(updates[0,4]),
            "skew_ticks_per_contract": float(updates[0,5])
        },
        "last_parameters": {
            "A_per_second": float(updates[-1,1]),
            "k_per_tick": float(updates[-1,2]),
            "sigma_tick_per_sqrt_second": float(updates[-1,3]),
            "half_spread_ticks": float(updates[-1,4]),
            "skew_ticks_per_contract": float(updates[-1,5])
        },
        "calibration_trace": [
            {
                "elapsed_seconds_after_training": float(row[0]),
                "A_per_second": float(row[1]),
                "k_per_real_tick": float(row[2]),
                "sigma_tick_per_sqrt_second": float(row[3]),
                "half_spread_ticks": float(row[4]),
                "skew_ticks_per_contract": float(row[5])
            } for row in updates
        ],
        "time_count": {"binance": int(bin_events), "okx_feed": int(okx_events),
                       "order_reply": int(order_events), "binance_reads": int(reads)},
        "trading": {
            "submissions": int(submits), "cancel_requests": int(cancels),
            "buy_fills": int(buys), "sell_fills": int(sells), "engine_fills": int(fills),
            "end_position_contracts": float(pos),
            "max_abs_position_contracts": float(max_pos),
            "end_net_marked_equity_usdt": float(equity),
            "end_mid_usdt_per_btc": float(mid),
            "balance_usdt": float(balance), "fee_usdt": float(fee),
            "trading_value_usdt": float(turnover),
            "min_marked_equity_usdt": float(min_eq),
            "max_marked_equity_usdt": float(max_eq),
            "first_marked_equity_usdt": float(first_eq),
            "max_drawdown_usdt": float(drawdown),
            "mean_abs_inventory_contracts": float(abs_seconds/(max(samples-TRAIN, 1)*STEP_NS/1e9)),
            "rms_inventory_contracts": float(math.sqrt(sq_seconds/(max(samples-TRAIN, 1)*STEP_NS/1e9))),
            "seconds_at_inventory_cap": float(at_cap_seconds)
        },
        "fill_exchange_timestamp_min_ns": int(fill_ts.min()) if fills else None,
        "fill_exchange_timestamp_max_ns": int(fill_ts.max()) if fills else None,
        "fill_buy_sell_sign_sum": int(np.sum(fill_sides == BUY) - np.sum(fill_sides == SELL))
    }
