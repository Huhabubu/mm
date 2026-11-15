"""Gate BTCUSDT Tardis pre-backtest data continuity and contract-unit QA.

Download and stream every free first-of-month CSV.GZ through the requested
UTC interval. Reject unresolved price/quantity units or timestamp problems.
No market-making fills or results are generated here.
"""
import argparse
import csv
import gzip
import io
import json
import math
import os
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = "https://datasets.tardis.dev/v1"
GATE_API = "https://api.gateio.ws/api/v4/futures/usdt/contracts/BTC_USDT"
FEEDS = (
    ("gate-io-futures", "trades", "BTC_USDT"),
    ("gate-io-futures", "incremental_book_L2", "BTC_USDT"),
    ("gate-io-futures", "quotes", "BTC_USDT"),
    ("binance-futures", "book_ticker", "BTCUSDT"),
)
REQUIRED = {
    "trades": {"exchange","symbol","timestamp","local_timestamp","side","price","amount"},
    "incremental_book_L2": {"exchange","symbol","timestamp","local_timestamp",
                            "side","price","amount","is_snapshot"},
    "quotes": {"exchange","symbol","timestamp","local_timestamp",
               "bid_price","ask_price","bid_amount","ask_amount"},
    "book_ticker": {"exchange","symbol","timestamp","local_timestamp",
                    "bid_price","ask_price","bid_amount","ask_amount"},
}
UA = {"User-Agent": "mm-binance-gate-paper-qa/1.0"}


def fetch_contract():
    req = urllib.request.Request(GATE_API, headers=UA)
    with urllib.request.urlopen(req, timeout=40) as stream:
        contract = json.load(stream)
    if contract.get("name") != "BTC_USDT":
        raise ValueError("Gate API returned wrong contract")
    keys = ("name","type","quanto_multiplier","order_price_round","order_size_min",
            "order_size_max","maker_fee_rate","taker_fee_rate","config_change_time")
    spec = {k:contract.get(k) for k in keys}
    tick = float(spec["order_price_round"])
    ctval = float(spec["quanto_multiplier"])
    min_qty = int(spec["order_size_min"])
    maker = float(spec["maker_fee_rate"])
    taker = float(spec["taker_fee_rate"])
    if not (spec["type"] == "direct" and tick > 0 and ctval > 0
            and min_qty >= 1 and math.isfinite(maker) and math.isfinite(taker)):
        raise ValueError(f"Unsupported Gate contract configuration: {spec}")
    return spec


def on_grid(x, step):
    v = x/step
    return abs(v-round(v)) <= 1e-5


def audit_feed(exchange, kind, symbol, day, stop_us, tick, start_us):
    year,month,daynum = day.split("-")
    url = f"{ROOT}/{exchange}/{kind}/{year}/{month}/{daynum}/{symbol}.csv.gz"
    req = urllib.request.Request(url, headers=UA)
    state = {
        "source":url,"rows":0,"first_local_us":None,"last_local_us":None,
        "invalid_rows":0,"backward_timestamp_rows":0,"negative_latency_rows":0,
        "equal_local_timestamp_rows":0,"largest_local_gap_ms":0.0,
        "gaps_over_10s":0,"gaps_over_30s":0,"gaps_over_60s":0,
        "first_snapshot_us":None,"snapshot_bid_rows":0,"snapshot_ask_rows":0,
        "snapshot_finished":False,"max_trade_amount_fractional_error":0.,
        "trades_nonintegral_amount_rows":0,
        "non_grid_prices":0,"header":None,
    }
    prev = None
    snapshot_started = False
    n_skipped_l2_before_snapshot = 0
    with urllib.request.urlopen(req, timeout=240) as response:
        with gzip.GzipFile(fileobj=response) as gz:
            with io.TextIOWrapper(gz, encoding="utf-8", newline="") as stream:
                reader = csv.DictReader(stream)
                fields = set(reader.fieldnames or ())
                if not REQUIRED[kind].issubset(fields):
                    raise ValueError(f"Missing required {kind} CSV fields: {REQUIRED[kind]-fields}")
                state["header"] = reader.fieldnames
                for row in reader:
                    t = int(row["local_timestamp"])
                    if t >= stop_us:
                        break
                    if t < start_us:
                        continue
                    if state["first_local_us"] is None:
                        state["first_local_us"] = t
                    if prev is not None:
                        diff = t-prev
                        if diff < 0:
                            state["backward_timestamp_rows"] += 1
                        elif diff == 0:
                            state["equal_local_timestamp_rows"] += 1
                        else:
                            state["largest_local_gap_ms"] = max(
                                state["largest_local_gap_ms"], diff/1000)
                            if diff > 10_000_000:
                                state["gaps_over_10s"] += 1
                            if diff > 30_000_000:
                                state["gaps_over_30s"] += 1
                            if diff > 60_000_000:
                                state["gaps_over_60s"] += 1
                    prev = t
                    state["last_local_us"] = t
                    state["rows"] += 1
                    if state["rows"] % 2_000_000 == 0:
                        print("QA_PROGRESS", day, kind, state["rows"], flush=True)
                    try:
                        if row["exchange"] != exchange or row["symbol"] != symbol:
                            state["invalid_rows"] += 1
                        source_time = int(row["timestamp"])
                        if source_time <= 0:
                            state["invalid_rows"] += 1
                        elif t < source_time:
                            state["negative_latency_rows"] += 1
                        if kind == "incremental_book_L2":
                            snap = row["is_snapshot"].lower()
                            if snap not in ("true","false"):
                                state["invalid_rows"] += 1
                                continue
                            if not snapshot_started and snap == "false":
                                n_skipped_l2_before_snapshot += 1
                                continue
                            if snap == "true":
                                if not snapshot_started:
                                    state["first_snapshot_us"] = t
                                snapshot_started = True
                                if not state["snapshot_finished"]:
                                    if row["side"] == "bid":
                                        state["snapshot_bid_rows"] += 1
                                    elif row["side"] == "ask":
                                        state["snapshot_ask_rows"] += 1
                            else:
                                state["snapshot_finished"] = True
                            if row["side"] not in ("bid","ask"):
                                state["invalid_rows"] += 1
                            px,qty = float(row["price"]),float(row["amount"])
                            if not (math.isfinite(px) and px>0 and math.isfinite(qty) and qty>=0):
                                state["invalid_rows"] += 1
                            elif not on_grid(px,tick):
                                state["non_grid_prices"] += 1
                        elif kind == "trades":
                            px,qty = float(row["price"]),float(row["amount"])
                            if row["side"] not in ("buy","sell") or not (
                                math.isfinite(px) and px>0 and math.isfinite(qty) and qty>0):
                                state["invalid_rows"] += 1
                            else:
                                if not on_grid(px,tick):
                                    state["non_grid_prices"] += 1
                                err = abs(qty-round(qty))
                                state["max_trade_amount_fractional_error"] = max(
                                    state["max_trade_amount_fractional_error"],err)
                                if err > 1e-6:
                                    state["trades_nonintegral_amount_rows"] += 1
                        else:
                            bid,ask = float(row["bid_price"]),float(row["ask_price"])
                            bqty,aqty=float(row["bid_amount"]),float(row["ask_amount"])
                            if not all(math.isfinite(v) for v in (bid,ask,bqty,aqty)) or not (
                                0<bid<ask and bqty>=0 and aqty>=0):
                                state["invalid_rows"] += 1
                            elif kind == "quotes" and (not on_grid(bid,tick) or not on_grid(ask,tick)):
                                state["non_grid_prices"] += 1
                    except (KeyError,TypeError,ValueError,OverflowError):
                        state["invalid_rows"] += 1
    state["skipped_l2_pre_snapshot"] = n_skipped_l2_before_snapshot
    if kind == "incremental_book_L2":
        state["initial_snapshot_complete"] = bool(
            state["first_snapshot_us"] and state["snapshot_bid_rows"]>0
            and state["snapshot_ask_rows"]>0 and state["snapshot_finished"])
    last = state["last_local_us"]
    if (state["rows"]==0 or state["first_local_us"] is None or
            state["first_local_us"]>start_us+5_000_000 or last<stop_us-30_000_000):
        raise ValueError(f"{kind} coverage incomplete: {state}")
    if (state["backward_timestamp_rows"] or state["invalid_rows"] or
            state["non_grid_prices"] or state["gaps_over_30s"] or
            state["negative_latency_rows"]):
        raise ValueError(f"{kind} integrity/clock/large-gap QA failed: {state}")
    if kind == "incremental_book_L2" and not state["initial_snapshot_complete"]:
        raise ValueError(f"No valid initial Gate L2 snapshot: {state}")
    if kind == "trades" and state["trades_nonintegral_amount_rows"]:
        raise ValueError(
            "Gate trades amounts not verified as integral contract counts. "
            "Do not map order quantities without auditing exchange normalization: "
            + json.dumps(state))
    return state


def main():
    a=argparse.ArgumentParser()
    a.add_argument("--day",required=True)
    a.add_argument("--minutes",type=int,default=1440)
    a.add_argument("--out",default="reports/gate_btc_sixday_data_qa")
    args=a.parse_args()
    moment=datetime.strptime(args.day,"%Y-%m-%d").replace(tzinfo=timezone.utc)
    if moment.day!=1 or not 60<=args.minutes<=1440:
        a.error("UTC first-of-month day and 60..1440 minutes required")
    start_us=int(moment.timestamp()*1_000_000)
    stop_us=start_us+args.minutes*60_000_000
    output=Path(args.out);output.mkdir(parents=True,exist_ok=True)
    dest=output/f"qa_{args.day}_{args.minutes}min.json"
    report={"day":args.day,"window_minutes":args.minutes,
            "gate_contract_api_observed_utc":datetime.now(timezone.utc).isoformat(),
            "status":"started","feeds":{}}
    try:
        spec=fetch_contract()
        report["gate_contract_spec"]=spec
        tick=float(spec["order_price_round"])
        for exchange,kind,symbol in FEEDS:
            print("QA_BEGIN",args.day,exchange,kind,flush=True)
            report["feeds"][kind]=audit_feed(
                exchange,kind,symbol,args.day,stop_us,tick,start_us)
            print("QA_OK",args.day,exchange,kind,
                  report["feeds"][kind]["rows"],flush=True)
        report["status"]="passed"
    except Exception as e:
        report["status"]="failed"
        report["error"]=f"{type(e).__name__}: {e}"
        raise
    finally:
        dest.write_text(json.dumps(report,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
        print("QA_REPORT",dest,report["status"],flush=True)


if __name__=="__main__":
    main()
