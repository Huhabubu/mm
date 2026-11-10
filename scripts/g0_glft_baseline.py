"""G0 OKX GLFT baseline: 10min calibration, 5min trade evaluation, no cross-venue inputs."""
import argparse
import csv
import json
import math
import tempfile
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import numpy as np
from check_okx_hft_data import SCHEMAS, SYMBOL, extract_window

# Current-spec scenario, 2026-11-01 historical metadata NOT yet proven.
TICK = 0.1               # USDT per BTC
LOT = 0.01               # contracts, also the scenario minimum order size
CTVAL = 0.01             # BTC per contract; additional contract multiplier 1
MAKER_FEE = 0.0002       # OKX Lv1 scenario, 2bp
TAKER_FEE = 0.0005       # OKX Lv1 scenario, 5bp
ORDER_QTY = 0.1          # contracts, 0.001 BTC
POSITION_CAP = 2.0       # contracts, 0.02 BTC
GAMMA = 0.05             # scenario risk parameter
STEP_NS = 100_000_000    # 100ms
TRAIN = 6000             # 10 minutes
TRADE = 3000             # 5 minutes


def read_live_spec(opener=urllib.request.urlopen):
    """Only reports a CURRENT instrument spec; cannot verify past contract rules."""
    url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP&instId=BTC-USDT-SWAP"
    try:
        with opener(urllib.request.Request(url, headers={"User-Agent": "mm-g0/1.0"}), timeout=12) as fd:
            data = json.load(fd)
        if data.get("code") != "0":
            raise ValueError("nonzero OKX API code")
        candidates = [x for x in data["data"] if x["instId"] == SYMBOL]
        if len(candidates) != 1:
            raise ValueError("missing OKX instrument")
        fields = {k: candidates[0].get(k) for k in ("instId", "ctVal", "ctValCcy",
                  "ctMult", "ctType", "tickSz", "lotSz", "minSz", "state")}
        return {"status": "ok", "observed_utc": datetime.now(timezone.utc).isoformat(), "fields": fields}
    except (OSError, ValueError, KeyError, TimeoutError) as exc:
        return {"status": "unavailable", "reason": f"{type(exc).__name__}: {exc}"}


def validate_current_spec(result):
    if result["status"] != "ok":
        return False
    x = result["fields"]
    for key, value in (("ctVal", CTVAL), ("ctMult", 1.0), ("tickSz", TICK),
                       ("lotSz", LOT), ("minSz", LOT)):
        if x.get(key) is None or not math.isclose(float(x[key]), value, abs_tol=1e-10, rel_tol=0):
            raise ValueError(f"Current OKX {key} does not match scenario: {x.get(key)} vs {value}")
    if x.get("ctValCcy") != "BTC" or x.get("ctType") != "linear":
        raise ValueError("Current OKX contract denomination/type differs")
    return True


def unit_grid_audit(paths, max_rows=50_000):
    """All quantities stay as Tardis-provided contracts. No BTC-size reinterpretation."""
    out = {}
    for kind in SCHEMAS:
        checked = bad_price = bad_qty = 0
        minimum = None
        with paths[kind].open(encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                if checked >= max_rows:
                    break
                checked += 1
                p = (row["bid_price"], row["ask_price"]) if kind == "book_ticker" else (row["price"],)
                q = (row["bid_amount"], row["ask_amount"]) if kind == "book_ticker" else (row["amount"],)
                bad_price += sum(Decimal(v) % Decimal("0.1") != 0 for v in p)
                for value in q:
                    v = Decimal(value)
                    if v > 0:
                        minimum = v if minimum is None else min(minimum, v)
                    bad_qty += v % Decimal("0.01") != 0
        out[kind] = {"rows": checked, "off_tick": bad_price, "off_lot": bad_qty,
                     "min_positive_amount_contracts": str(minimum)}
        if bad_price or bad_qty:
            raise ValueError(f"{kind}: contract price/amount grid incompatibility: {out[kind]}")
    return out


def calibrate(arrivals, changes):
    """Same arrival-depth count and log-linear fit as the official GLFT tutorial."""
    counts = np.zeros(70, np.float64)
    for depth in arrivals:
        if np.isfinite(depth):
            last = int(round(depth / 0.5)) - 1
            if last > 0:
                counts[:min(last, 70)] += 1
    valid = counts > 0
    if int(np.sum(valid)) < 5:
        raise ValueError("Not enough positive arrival-depth levels")
    x = np.arange(70)[valid] + 0.5
    slope, intercept = np.polyfit(x, np.log(counts[valid] / 600), 1)
    a = float(np.exp(intercept))
    k = float(-slope)
    sigma = float(np.nanstd(changes) * np.sqrt(10))
    if not all(math.isfinite(v) and v > 0 for v in (a, k, sigma)):
        raise ValueError(f"Invalid GLFT estimates A={a}, k={k}, sigma={sigma}")
    return {"A_per_second": a, "k_per_tick": k,
            "sigma_tick_per_sqrt_second": sigma,
            "positive_depth_levels": int(sum(valid)),
            "arrival_intervals": int(np.isfinite(arrivals).sum())}


def quote_coefficients(a, k, sigma):
    """GLFT (4.6)-(4.7), with delta = order quantity in contracts."""
    u = GAMMA * ORDER_QTY / k
    c1 = math.log1p(u) / (GAMMA * ORDER_QTY)
    c2 = math.sqrt(GAMMA / (2 * a * ORDER_QTY * k)
                   * math.exp((k / (GAMMA * ORDER_QTY) + 1) * math.log1p(u)))
    half = c1 + ORDER_QTY / 2 * c2 * sigma
    skew = c2 * sigma
    if not all(math.isfinite(v) and v > 0 for v in (half, skew)):
        raise ValueError("Nonfinite GLFT coefficients")
    return half, skew


def calibrate_replay(hbt):
    from numba import njit
    from hftbacktest import BUY_EVENT

    @njit
    def warm(engine):
        arrivals = np.full(TRAIN, np.nan, np.float64)
        changes = np.full(TRAIN, np.nan, np.float64)
        old_mid = np.nan
        n = 0
        for i in range(TRAIN):
            if engine.elapse(STEP_NS) != 0:
                break
            furthest = -np.inf
            if np.isfinite(old_mid):
                for tr in engine.last_trades(0):
                    trade_tick = tr.px / TICK
                    if tr.ev & BUY_EVENT == BUY_EVENT:
                        furthest = max(furthest, trade_tick - old_mid)
                    else:
                        furthest = max(furthest, old_mid - trade_tick)
                if np.isfinite(furthest):
                    arrivals[i] = furthest
            engine.clear_last_trades(0)
            d = engine.depth(0)
            if d.best_bid_tick > 0 and d.best_ask_tick > d.best_bid_tick and d.best_ask_tick < 1_000_000_000:
                mid = (d.best_bid_tick + d.best_ask_tick) / 2.0
                if np.isfinite(old_mid):
                    changes[i] = mid - old_mid
                old_mid = mid
            n += 1
        return arrivals[:n], changes[:n], n
    return warm(hbt)


def trade_replay(hbt, half_tick, skew_tick):
    from numba import njit
    from hftbacktest import BUY, SELL, GTX, LIMIT, FILLED

    @njit
    def strategy(engine, half, skew):
        n = submissions = cancels = buys = sells = 0
        max_pos = 0.0
        min_equity, max_equity = np.inf, -np.inf
        pre_fills = engine.state_values(0).num_trades
        for _ in range(TRADE):
            if engine.elapse(STEP_NS) != 0:
                break
            orders = engine.orders(0)
            it = orders.values()
            while it.has_next():
                order = it.get()
                if order.status == FILLED:
                    if order.side == BUY:
                        buys += 1
                    else:
                        sells += 1
            engine.clear_inactive_orders(0)
            depth = engine.depth(0)
            bid, ask = depth.best_bid_tick, depth.best_ask_tick
            if bid <= 0 or ask <= bid or ask > 1_000_000_000:
                continue
            mid = (bid + ask) / 2.0
            position = engine.position(0)
            max_pos = max(max_pos, abs(position))
            state = engine.state_values(0)
            equity = state.balance + CTVAL * position * (mid*TICK) - state.fee
            min_equity = min(min_equity, equity)
            max_equity = max(max_equity, equity)
            reservation = mid - skew * position
            want_bid = min(round(reservation - half), bid)
            want_ask = max(round(reservation + half), ask)
            bid_id = int(want_bid)*2
            ask_id = int(want_ask)*2+1
            it = orders.values()
            while it.has_next():
                order = it.get()
                if order.cancellable:
                    if ((order.side == BUY and order.price_tick != want_bid)
                         or (order.side == SELL and order.price_tick != want_ask)):
                        engine.cancel(0, order.order_id, False)
                        cancels += 1
            if position + ORDER_QTY <= POSITION_CAP + 1e-9 and bid_id not in orders:
                engine.submit_buy_order(0, bid_id, want_bid*TICK, ORDER_QTY, GTX, LIMIT, False)
                submissions += 1
            if position - ORDER_QTY >= -POSITION_CAP - 1e-9 and ask_id not in orders:
                engine.submit_sell_order(0, ask_id, want_ask*TICK, ORDER_QTY, GTX, LIMIT, False)
                submissions += 1
            n += 1
        st = engine.state_values(0)
        d = engine.depth(0)
        final_mid = (d.best_bid + d.best_ask) / 2
        final_equity = st.balance + CTVAL*st.position*final_mid - st.fee
        return (n, submissions, cancels, buys, sells, st.num_trades-pre_fills,
                st.position, max_pos, st.balance, st.fee, st.trading_value,
                final_mid, final_equity, min_equity, max_equity)
    return strategy(hbt, half_tick, skew_tick)


def run_engine(paths, counts, folder):
    from hftbacktest import BacktestAsset, ROIVectorMarketDepthBacktest
    from hftbacktest.data.utils import tardis
    from hftbacktest.data.validation import validate_event_order

    output = folder / "okx_15min_fused.npz"
    event = tardis.convert_fuse(str(paths["trades"]), str(paths["incremental_book_L2"]),
                                str(paths["book_ticker"]), tick_size=TICK, lot_size=LOT,
                                output_filename=str(output),
                                ss_buffer_size=max(1000, counts["incremental_book_L2"]["snapshot_rows"]+100),
                                snapshot_mode="process")
    validate_event_order(event)
    event_count = len(event)
    del event
    lb, ub = math.inf, -math.inf
    with paths["book_ticker"].open(encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            lb = min(lb, float(r["bid_price"]))
            ub = max(ub, float(r["ask_price"]))
    asset = (BacktestAsset().data([str(output)]).linear_asset(CTVAL)
             .constant_order_latency(1_000_000, 1_000_000)
             .power_prob_queue_model(2.0).no_partial_fill_exchange()
             .trading_value_fee_model(MAKER_FEE, TAKER_FEE)
             .tick_size(TICK).lot_size(LOT)
             .roi_lb(lb-1000).roi_ub(ub+1000).last_trades_capacity(20_000))
    engine = ROIVectorMarketDepthBacktest([asset])
    try:
        arrivals, changes, steps = calibrate_replay(engine)
        if steps != TRAIN:
            raise ValueError(f"Too few calibration steps {steps}")
        params = calibrate(arrivals, changes)
        half, skew = quote_coefficients(params["A_per_second"], params["k_per_tick"],
                                         params["sigma_tick_per_sqrt_second"])
        results = trade_replay(engine, half, skew)
    finally:
        engine.close()
    keys = ("decision_steps", "submitted_orders", "cancel_requests",
            "filled_buy_orders", "filled_sell_orders", "engine_fills",
            "final_position_contracts", "max_abs_position_contracts",
            "balance_usdt", "fees_usdt", "turnover_usdt",
            "final_mid_usdt_per_btc", "net_marked_equity_usdt",
            "min_equity_usdt", "max_equity_usdt")
    vals = {k: (int(v) if i<6 else float(v)) for i,(k,v) in enumerate(zip(keys,results))}
    if vals["decision_steps"] < 1000 or not math.isfinite(vals["net_marked_equity_usdt"]):
        raise ValueError(f"Invalid engine evaluation: {vals}")
    if abs(vals["final_position_contracts"]) > POSITION_CAP+ORDER_QTY+1e-9:
        raise ValueError(f"Inventory cap violation: {vals}")
    if vals["engine_fills"] != vals["filled_buy_orders"]+vals["filled_sell_orders"]:
        raise ValueError(f"Fill reconciliation failure: {vals}")
    return {"converted_events": int(event_count), "estimated": params,
            "GLFT_quotes": {"half_spread_ticks": half, "skew_ticks_per_contract": skew,
                            "gamma": GAMMA, "order_qty_contracts": ORDER_QTY}, "trading": vals}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--day", default="2026-11-01")
    p.add_argument("--report-dir", default="reports")
    args = p.parse_args()
    day = datetime.strptime(args.day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if day.day != 1:
        p.error("Tardis free days must be 1st of month")
    end_us = int(day.timestamp()*1_000_000)+900_000_000
    report = {"status": "started", "day_utc": args.day,
              "window": "10min training + 5min out-of-training evaluation",
              "scenario": {"tick_size_usdt": TICK, "lot_contracts": LOT,
                           "ctval_btc_per_contract": CTVAL, "ctmult": 1,
                           "maker_fee": MAKER_FEE, "taker_fee": TAKER_FEE,
                           "entry_latency_ms": 1, "response_latency_ms": 1,
                           "queue_model": "power_prob(2.0)", "fill_model": "no_partial",
                           "gamma": GAMMA, "order_qty_contracts": ORDER_QTY,
                           "max_position_contracts": POSITION_CAP,
                           "historical_spec_verified": False, "actual_account_fee_verified": False,
                           "live_latency_measured": False},
              "NOT_daily_net_return": True}
    out = Path(args.report_dir)/f"g0_glft_btc_{args.day}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    try:
        report["live_instrument"] = read_live_spec()
        report["matches_live_spec"] = validate_current_spec(report["live_instrument"])
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp)
            paths = {}
            counts = {}
            for kind in SCHEMAS:
                paths[kind] = work/(kind+".csv")
                counts[kind] = extract_window(kind, args.day, end_us, paths[kind], max_rows=1_000_000)
            report["feed_counts"] = counts
            report["units"] = unit_grid_audit(paths)
            report["g0"] = run_engine(paths, counts, work)
        report["status"] = "complete"
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        out.write_text(json.dumps(report, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
        print(f"G0 report {out}: {report['status']}", flush=True)


if __name__ == "__main__":
    main()
