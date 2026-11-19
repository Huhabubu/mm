"""Stage 4A: bounded-memory, real free Tardis first-of-month BTC feed audit.

This is a READ-ONLY historical data gate. It never creates orders or simulated
fills. It cannot prove raw WebSocket channel identity or packet sequence
continuity from normalized Tardis CSV; the report says so explicitly.

Clock values in Tardis normalized CSV are microseconds. "local - exchange"
includes exchange publication cadence, time synchronization error, and transit.
Treat it as OBSERVED CAPTURE LAG, not a pure one-way network latency.
"""
import argparse
from array import array
import csv
from datetime import datetime, timedelta, timezone
import gzip
import io
import json
import math
from pathlib import Path
import time
import urllib.error
import urllib.request

ROOT = "https://datasets.tardis.dev/v1"
USER_AGENT = "mm-paper-cross-exchange-4a/20261119"
REQUIRED = {
    "trades": {"exchange", "symbol", "timestamp", "local_timestamp", "side", "price", "amount"},
    "incremental_book_L2": {"exchange", "symbol", "timestamp", "local_timestamp", "side", "price", "amount", "is_snapshot"},
    "quotes": {"exchange", "symbol", "timestamp", "local_timestamp", "bid_price", "ask_price", "bid_amount", "ask_amount"},
    "book_ticker": {"exchange", "symbol", "timestamp", "local_timestamp", "bid_price", "ask_price", "bid_amount", "ask_amount"},
}
# A uniform 100-us histogram over 0..10s gives <=0.1ms precision for
# eligible quantiles with constant memory; larger values go to overflow bin.
BIN_US = 100
MAX_LAG_US = 10_000_000
HIST_LENGTH = MAX_LAG_US // BIN_US + 2
MAX_STALE_MS = 10_000  # predeclared diagnostic gate; high-frequency BTC L2/BBO
START_GRACE_MS = 10_000
END_GRACE_MS = 30_000
FEEDS = ("trades", "incremental_book_L2", "quotes", "binance_book_ticker")


def url_for(exchange, kind, day, symbol):
    year, month, date = day.split("-")
    return f"{ROOT}/{exchange}/{kind}/{year}/{month}/{date}/{symbol}.csv.gz"


def percentile(hist, n, q):
    if not n:
        return None
    threshold = math.ceil(q * n)
    tally = 0
    for i, qty in enumerate(hist):
        tally += qty
        if tally >= threshold:
            if i == HIST_LENGTH-1:
                return f">{MAX_LAG_US/1000:.0f}ms"
            return round((i + .5)*BIN_US/1000, 4)
    raise AssertionError("Histogram count and expected valid lag count differ")


class LagStats:
    def __init__(self):
        self.hist = array("Q", [0])*HIST_LENGTH
        self.n = self.negative = self.over_10s = 0
        self.max_lag_us = 0
        self.hour_count = [0]*24
        self.hour_sum_us = [0]*24

    def add(self, lag_us, hour):
        if lag_us < 0:
            self.negative += 1
            return
        self.n += 1
        self.max_lag_us = max(self.max_lag_us, lag_us)
        if lag_us > MAX_LAG_US:
            self.over_10s += 1
        bucket = min(lag_us//BIN_US, HIST_LENGTH-1)
        self.hist[bucket] += 1
        self.hour_count[hour] += 1
        self.hour_sum_us[hour] += lag_us

    def result(self):
        return {
            "unit": "ms",
            "n_nonnegative": self.n,
            "n_negative": self.negative,
            "p50_ms": percentile(self.hist,self.n,.50),
            "p95_ms": percentile(self.hist,self.n,.95),
            "p99_ms": percentile(self.hist,self.n,.99),
            "max_ms": round(self.max_lag_us/1000,3),
            "over_10s": self.over_10s,
            "hourly_mean_ms": [
                round(self.hour_sum_us[h]/self.hour_count[h]/1000,4)
                if self.hour_count[h] else None for h in range(24)
            ]
        }


def on_grid(value, step):
    if step <= 0:
        raise ValueError("Nonpositive frozen candidate tick")
    return abs(value/step-round(value/step))<=2e-5


def audit_stream(exchange, symbol, kind, day, start_us, stop_us,
                 expected_tick, expected_size_step, retries=2):
    url=url_for(exchange,kind,day,symbol)
    stats={
        "source_url":url,"exchange":exchange,"symbol":symbol,
        "kind":kind,"rows":0,"first_local_us":None,"last_local_us":None,
        "invalid_rows":0,"local_time_backwards":0,"exchange_time_nonpositive":0,
        "nonpositive_price":0,"non_grid_prices":0,"negative_amounts":0,
        "amount_not_on_proposed_order_step":0,
        "max_local_gap_ms":0.0,"gaps_over_1s":0,"gaps_over_3s":0,
        "gaps_over_5s":0,"gaps_over_10s":0,
        "snapshot_groups":0,"reconnect_resnapshots":0,
        "initial_snapshot_bid_levels":0,"initial_snapshot_ask_levels":0,
        "snapshots_missing_side":0,"incremental_rows":0,
        "snapshot_rows":0,"l2_unknown_before_initial_snapshot":0,
        "sampled_reconstructed_book_crosses":0,
        "sampled_reconstructed_book_checks":0,
        "bbo_crossed_or_zero":0,
        "gap_over_3s_first5":[],
        "observed_qty_min_positive":None,
        "observed_max_bid_depth":0,"observed_max_ask_depth":0
    }
    lag=LagStats()
    start=datetime.strptime(day,"%Y-%m-%d").replace(tzinfo=timezone.utc)
    previous=None
    book_bid={}
    book_ask={}
    snap_in_progress=False
    snap_bid=snap_ask=0
    initialized=False
    last_l2_check_us=start_us
    initial_snap=False
    amount_min=math.inf

    for attempt in range(retries+1):
        try:
            req=urllib.request.Request(url,headers={"User-Agent":USER_AGENT})
            response=urllib.request.urlopen(req, timeout=120)
            break
        except (urllib.error.HTTPError,urllib.error.URLError,TimeoutError) as exc:
            if attempt == retries:
                raise RuntimeError(f"Tardis CSV unavailable {url}: {exc}") from exc
            time.sleep(2+attempt)
    with response:
        with gzip.GzipFile(fileobj=response) as decompressed:
            with io.TextIOWrapper(decompressed,encoding="utf-8",newline="") as stream:
                reader=csv.DictReader(stream)
                header=set(reader.fieldnames or [])
                key="book_ticker" if kind=="book_ticker" else kind
                if not REQUIRED[key].issubset(header):
                    raise ValueError(f"Missing Tardis columns {REQUIRED[key]-header} at {url}")
                stats["csv_columns"]=reader.fieldnames
                for row in reader:
                    local=int(row["local_timestamp"])
                    if local>=stop_us:
                        break
                    if local<start_us:
                        continue
                    if row["exchange"]!=exchange or row["symbol"]!=symbol:
                        stats["invalid_rows"]+=1
                    ex=int(row["timestamp"])
                    if ex<=0:
                        stats["exchange_time_nonpositive"]+=1
                    elif local>0:
                        hr=min(23,max(0,int((local-start_us)//3_600_000_000)))
                        lag.add(local-ex,hr)
                    if stats["first_local_us"] is None:
                        stats["first_local_us"]=local
                    if previous is not None:
                        gap=local-previous
                        if gap<0:
                            stats["local_time_backwards"]+=1
                        elif gap>0:
                            gap_ms=gap/1000
                            stats["max_local_gap_ms"]=max(stats["max_local_gap_ms"],gap_ms)
                            for threshold,keyname in ((1000,"gaps_over_1s"),(3000,"gaps_over_3s"),
                                                      (5000,"gaps_over_5s"),(10000,"gaps_over_10s")):
                                if gap_ms>threshold:
                                    stats[keyname]+=1
                            if gap_ms>3000 and len(stats["gap_over_3s_first5"])<5:
                                stats["gap_over_3s_first5"].append({
                                    "previous_local_us":previous,
                                    "current_local_us":local,"gap_ms":round(gap_ms,3)})
                    previous=local
                    stats["last_local_us"]=local
                    stats["rows"]+=1
                    if stats["rows"] % 4_000_000 == 0:
                        print("DATA_QA_PROGRESS",exchange,day,kind,stats["rows"],flush=True)

                    if kind=="incremental_book_L2":
                        is_snapshot=row["is_snapshot"].lower()
                        side=row["side"]
                        if is_snapshot not in ("true","false") or side not in ("bid","ask"):
                            stats["invalid_rows"]+=1
                            continue
                        if is_snapshot=="true" and not snap_in_progress:
                            # A snapshot can be a new day initial book or a
                            # reconnect reset, not necessarily WS packet loss.
                            if initialized:
                                stats["reconnect_resnapshots"]+=1
                            snap_in_progress=True
                            snap_bid=snap_ask=0
                            book_bid.clear()
                            book_ask.clear()
                            stats["snapshot_groups"]+=1
                        elif is_snapshot=="false" and snap_in_progress:
                            if snap_bid==0 or snap_ask==0:
                                stats["snapshots_missing_side"]+=1
                            if not initialized:
                                stats["initial_snapshot_bid_levels"]=snap_bid
                                stats["initial_snapshot_ask_levels"]=snap_ask
                            initialized=True
                            snap_in_progress=False
                        if is_snapshot=="true":
                            stats["snapshot_rows"]+=1
                            if side=="bid":
                                snap_bid+=1
                            else:
                                snap_ask+=1
                        else:
                            stats["incremental_rows"]+=1
                        price=float(row["price"])
                        qty=float(row["amount"])
                        if not math.isfinite(price) or price<=0:
                            stats["nonpositive_price"]+=1
                            continue
                        if not math.isfinite(qty) or qty<0:
                            stats["negative_amounts"]+=1
                            continue
                        if not on_grid(price,expected_tick):
                            stats["non_grid_prices"]+=1
                        if not initialized and not snap_in_progress:
                            stats["l2_unknown_before_initial_snapshot"]+=1
                            continue
                        book=book_bid if side=="bid" else book_ask
                        if qty==0:
                            book.pop(price,None)
                        else:
                            book[price]=qty
                        if initialized and not snap_in_progress and local-last_l2_check_us>=30_000_000:
                            last_l2_check_us=local
                            if book_bid and book_ask:
                                stats["sampled_reconstructed_book_checks"]+=1
                                if max(book_bid)>=min(book_ask):
                                    stats["sampled_reconstructed_book_crosses"]+=1
                            stats["observed_max_bid_depth"]=max(stats["observed_max_bid_depth"],len(book_bid))
                            stats["observed_max_ask_depth"]=max(stats["observed_max_ask_depth"],len(book_ask))

                    elif kind=="trades":
                        side=row["side"]
                        price=float(row["price"])
                        qty=float(row["amount"])
                        if side not in ("buy","sell") or not math.isfinite(price) or price<=0 or not math.isfinite(qty) or qty<=0:
                            stats["invalid_rows"]+=1
                            continue
                        if not on_grid(price,expected_tick):
                            stats["non_grid_prices"]+=1
                        amount_min=min(amount_min,qty)
                        if expected_size_step>0 and not on_grid(qty,expected_size_step):
                            stats["amount_not_on_proposed_order_step"]+=1
                    else:
                        bid=float(row["bid_price"])
                        ask=float(row["ask_price"])
                        bq=float(row["bid_amount"])
                        aq=float(row["ask_amount"])
                        if not all(math.isfinite(v) for v in (bid,ask,bq,aq)) or bid<=0 or ask<=bid or bq<0 or aq<0:
                            stats["bbo_crossed_or_zero"]+=1
                        elif exchange!="binance-futures":
                            if not on_grid(bid,expected_tick) or not on_grid(ask,expected_tick):
                                stats["non_grid_prices"]+=1
    if snap_in_progress:
        if snap_bid==0 or snap_ask==0:
            stats["snapshots_missing_side"]+=1
        if not initialized:
            stats["initial_snapshot_bid_levels"]=snap_bid
            stats["initial_snapshot_ask_levels"]=snap_ask
        initialized=True
    stats["lag"]=lag.result()
    stats["observed_qty_min_positive"]=None if math.isinf(amount_min) else amount_min
    stats["coverage_start_offset_ms"]=(None if stats["first_local_us"] is None
        else round((stats["first_local_us"]-start_us)/1000,3))
    stats["coverage_end_shortfall_ms"]=(None if stats["last_local_us"] is None
        else round((stop_us-stats["last_local_us"])/1000,3))
    stats["native_channel_proven_by_normalized_csv"]=False
    stats["raw_sequence_number_available"]=False
    stats["disconnect_marker_available_in_csv"]=False

    blockers=[]
    if stats["rows"]<1:
        blockers.append("no_rows")
    if stats["first_local_us"] is None or stats["first_local_us"]-start_us>START_GRACE_MS*1000:
        blockers.append("late_first_event_gt_10s")
    if stats["last_local_us"] is None or stop_us-stats["last_local_us"]>END_GRACE_MS*1000:
        blockers.append("early_last_event_gt_30s")
    for key in ("invalid_rows","local_time_backwards","exchange_time_nonpositive",
                "nonpositive_price","negative_amounts","non_grid_prices"):
        if stats[key]:
            blockers.append(f"{key}={stats[key]}")
    if kind in ("incremental_book_L2","book_ticker","quotes"):
        if stats["gaps_over_10s"]:
            blockers.append(f"market_data_gap_gt_10s={stats['gaps_over_10s']}")
    if kind=="incremental_book_L2":
        if stats["snapshot_groups"]<1 or stats["incremental_rows"]<1:
            blockers.append("no_snapshot_plus_deltas")
        if not stats["initial_snapshot_bid_levels"] or not stats["initial_snapshot_ask_levels"]:
            blockers.append("initial_snapshot_missing_side")
        if stats["snapshots_missing_side"] or stats["l2_unknown_before_initial_snapshot"]:
            blockers.append("l2_snapshot_integrity_invalid")
        if stats["sampled_reconstructed_book_crosses"]>0:
            blockers.append("crossed_book_in_sample")
    if kind in ("quotes","book_ticker") and stats["bbo_crossed_or_zero"]>0:
        blockers.append("crossed_or_nonpositive_quotes")
    lag_negative=stats["lag"]["n_negative"]
    total_lag=lag_negative+stats["lag"]["n_nonnegative"]
    if total_lag and lag_negative/total_lag>.01:
        blockers.append("negative_capture_lag_over_1pct")
    stats["blocking_checks"]=blockers
    stats["normalized_gate_status"]="blocked" if blockers else "passed"
    return stats


def audit_day(venue, day, out_path, config_path):
    cfg=json.loads(Path(config_path).read_text(encoding="utf-8"))
    if not cfg.get("contracts"):
        raise ValueError("Missing frozen 4A candidate contract specs")
    if venue not in ("bitget-futures","okex-swap"):
        raise ValueError(f"Unsupported audit target venue: {venue}")
    symbol="BTCUSDT" if venue=="bitget-futures" else "BTC-USDT-SWAP"
    contract=cfg["contracts"][f"{venue}:{symbol}"]
    if contract["historical_date_effective_verified"]:
        raise ValueError("Spec falsely claims verified history without independent archival evidence")
    valid_dates=(["2026-%02d-01"%month for month in range(6,12)] if venue=="bitget-futures"
                 else ["2026-%02d-01"%month for month in range(9,12)])
    if day not in valid_dates:
        raise ValueError(f"Date {day} invalid for strict double-Tokyo candidate {venue}")
    t=datetime.strptime(day,"%Y-%m-%d").replace(tzinfo=timezone.utc)
    start_us=int(t.timestamp()*1_000_000)
    stop_us=start_us+86_400_000_000
    result={"day":day,"venue":venue,"symbol":symbol,"status":"started",
            "utc_interval":"[00:00:00,24:00:00)","feeds":{},
            "contract_candidate":contract,"normalized_l2_not_native_proof":True,
            "comment":"No hftbacktest orders or strategy PnL produced",
            "csv_capture_lag_warning":"local_timestamp - timestamp is not pure network propagation latency"}
    out_path.parent.mkdir(parents=True,exist_ok=True)
    try:
        for exchange,kind,sym in ((venue,"trades",symbol),
                (venue,"incremental_book_L2",symbol),
                (venue,"quotes",symbol),
                ("binance-futures","book_ticker","BTCUSDT")):
            name="binance_book_ticker" if exchange=="binance-futures" else kind
            print("AUDIT_BEGIN",day,exchange,kind,flush=True)
            try:
                feed=audit_stream(
                    exchange,sym,kind,day,start_us,stop_us,
                    float(contract["expected_price_tick_usdt"]) if exchange!=
                    "binance-futures" else 0.1,
                    float(contract["expected_order_step"]) if exchange!=
                    "binance-futures" else 0.0)
            except Exception as exc:
                feed={"normalized_gate_status":"blocked",
                      "blocking_checks":[f"{type(exc).__name__}: {exc}"],
                      "source_url":url_for(exchange,kind,day,sym),
                      "rows":0}
            result["feeds"][name]=feed
            print("AUDIT_FEED",day,exchange,kind,
                  feed["normalized_gate_status"],feed.get("rows"),
                  feed.get("blocking_checks"),flush=True)
        a=result["feeds"].get("quotes",{}).get("lag",{})
        b=result["feeds"].get("binance_book_ticker",{}).get("lag",{})
        def d(key):
            av,bv=a.get(key),b.get(key)
            if isinstance(av,(float,int)) and isinstance(bv,(float,int)):
                return round(av-bv,4)
            return None
        result["capture_lag_comparison"]={
            "target_feed":"quotes","reference_feed":"binance book_ticker",
            "p50_delta_ms":d("p50_ms"),"p95_delta_ms":d("p95_ms"),
            "p99_delta_ms":d("p99_ms"),
            "not_causal_latency_comparison":True,
            "risk_flag_p50_difference_over_20ms":(
                abs(d("p50_ms"))>20 if d("p50_ms") is not None else None),
            "risk_flag_p99_difference_over_100ms":(
                abs(d("p99_ms"))>100 if d("p99_ms") is not None else None)
        }
        result["normalized_feeds_passed"]=all(
            f.get("normalized_gate_status")=="passed" for f in result["feeds"].values())
        result["native_incremental_verified"]=False  # needs raw channel or external historical attestation
        result["historical_contract_date_effective_verified"]=False
        result["eligible_to_launch_4b_automatically"]=False
        result["status"]="passed_normalized_gate_with_unresolved_metadata" if result[
            "normalized_feeds_passed"] else "blocked"
    except Exception as exc:
        result["status"]="failed"
        result["fatal_error"]=f"{type(exc).__name__}: {exc}"
        raise
    finally:
        out_path.write_text(json.dumps(result,ensure_ascii=False,indent=2,allow_nan=False)+"\n",
                            encoding="utf-8")
        print("AUDIT_RESULT",out_path,result["status"],flush=True)
    return result


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--venue",required=True,choices=["bitget-futures","okex-swap"])
    parser.add_argument("--day",required=True)
    parser.add_argument("--out",type=Path,required=True)
    parser.add_argument("--config",type=Path,default=
        Path(__file__).resolve().parent.parent/"configs"/"cross_exchange_contract_specs_4a.json")
    args=parser.parse_args()
    result=audit_day(args.venue,args.day,args.out,args.config)
    return 0 if result["status"]!="failed" else 2


if __name__=="__main__":
    raise SystemExit(main())
