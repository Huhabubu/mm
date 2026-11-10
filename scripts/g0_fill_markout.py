"""Actual-timestamp maker-fill markouts for the unmodified OKX G0-R policy.

No self-built matching. Uses filled orders from official hftbacktest 2.4.4.
The exchange-time quote series is for EX POST markout only, never decisions.
"""
import argparse
import csv
import json
import math
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from check_okx_hft_data import SCHEMAS, extract_window
from g0_glft_baseline import (CTVAL, LOT, MAKER_FEE, TAKER_FEE, TICK,
                              unit_grid_audit)
from g0_glft_rolling import run_mode, GAMMA

HORIZONS_MS = (100, 500, 1000, 2000)
MAX_EXCHANGE_QUOTE_AGE_MS = 250
TRADE_MINUTES = 50
FREE_DATES = ("2026-06-01","2026-07-01","2026-08-01",
              "2026-09-01","2026-10-01","2026-11-01")


def read_exchange_bbo(csv_path):
    """Tardis exchange timestamp (microseconds), not local receipt timestamp."""
    rows=[]
    with Path(csv_path).open(encoding="utf-8",newline="") as file:
        for row in csv.DictReader(file):
            bid,ask=float(row["bid_price"]),float(row["ask_price"])
            if not (0 < bid < ask and math.isfinite(bid) and math.isfinite(ask)):
                raise ValueError("invalid BBO")
            rows.append((int(row["timestamp"])*1000,(bid+ask)/2))
    if not rows:
        raise ValueError("no quote observations")
    # Some exchange-time streams are slightly out of order in arrival order.
    rows.sort(key=lambda x:x[0])
    return (np.array([x[0] for x in rows],dtype=np.int64),
            np.array([x[1] for x in rows],dtype=np.float64))


def asof_exchange_mid(exchange_ns, mids, targets_ns, strict=False,
                      max_age_ms=MAX_EXCHANGE_QUOTE_AGE_MS):
    """Strict exclusion of equal-time initial quotes; no future quote access."""
    exchange_ns=np.asarray(exchange_ns,dtype=np.int64)
    mids=np.asarray(mids,dtype=np.float64)
    t=np.asarray(targets_ns,dtype=np.int64)
    if len(exchange_ns)!=len(mids) or not len(exchange_ns):
        raise ValueError("bad quote series")
    if np.any(np.diff(exchange_ns)<0):
        raise ValueError("quotes not monotone in exchange time")
    idx=np.searchsorted(exchange_ns,t,side="left" if strict else "right")-1
    safe=np.maximum(idx,0)
    age=t-exchange_ns[safe]
    valid=(idx>=0)&(age>=0)&(age<=max_age_ms*1_000_000)
    out=np.where(valid,mids[safe],np.nan)
    return out,valid,age


def evaluate_fill_markouts(fills,exchange_ns,quote_mid,horizons_ms=HORIZONS_MS):
    """Signed markout s*q*ctval*(M(t+h)-M(t)) in USDT.

    Execution 'spread capture' is s*q*ctval*(M(t)-fill_px).
    Maker fee is q*ctval*fill_px*maker_fee.
    Horizons are counterfactual valuations, NOT realized net strategy PnL.
    """
    fills=list(fills)
    n=len(fills)
    if n == 0:
        return {"n_fills":0,"horizons":{}}
    ts=np.array([r["exch_timestamp_ns"] for r in fills],dtype=np.int64)
    s=np.array([r["side_sign"] for r in fills],dtype=np.float64)
    p=np.array([r["execution_price_usdt_per_btc"] for r in fills],dtype=np.float64)
    q=np.array([r["executed_qty_contracts"] for r in fills],dtype=np.float64)
    if not np.all(np.isin(s,(-1,1))) or np.any(q<=0) or np.any(p<=0):
        raise ValueError("invalid fills")
    mid0,good0,_=asof_exchange_mid(exchange_ns,quote_mid,ts,strict=True)
    capture=s*q*CTVAL*(mid0-p)
    fee=q*CTVAL*p*MAKER_FEE
    answer={"n_fills":int(n),"n_valid_reference_mid":int(good0.sum()),
            "horizons":{}}
    for horizon in horizons_ms:
        if horizon <= 0:
            raise ValueError("horizon must be positive")
        future,good_future,_=asof_exchange_mid(
            exchange_ns,quote_mid,ts+horizon*1_000_000,strict=False)
        valid=good0 & good_future
        count=int(valid.sum())
        if not count:
            answer["horizons"][str(horizon)]={"n_valid":0,"n_censored":n}
            continue
        moved=s*q*CTVAL*(future-mid0)
        gross=capture+moved
        net=gross-fee
        adverse=moved[valid]<0
        answer["horizons"][str(horizon)]={
            "n_valid":count,"n_censored":n-count,
            "adverse_fill_fraction":float(np.mean(adverse)),
            "mean_spread_capture_usdt":float(np.mean(capture[valid])),
            "mean_signed_markout_usdt":float(np.mean(moved[valid])),
            "mean_maker_fee_usdt":float(np.mean(fee[valid])),
            "mean_horizon_net_proxy_usdt":float(np.mean(net[valid])),
            "sum_signed_markout_usdt":float(np.sum(moved[valid])),
            "sum_horizon_net_proxy_usdt":float(np.sum(net[valid])),
            "mean_signed_markout_bps":float(
                np.mean(10000*s[valid]*(future[valid]-mid0[valid])/mid0[valid])),
        }
    return answer


def run(day,report_dir):
    from hftbacktest.data.utils import tardis
    from hftbacktest.data.validation import validate_event_order

    target=Path(report_dir)/f"g0_fill_markout_{day}_60m.json"
    target.parent.mkdir(parents=True,exist_ok=True)
    record={"status":"started","day_utc":day,"scope":"diagnosis_50min_not_daily",
            "gamma_code":GAMMA,"horizons_ms":list(HORIZONS_MS),
            "max_exch_quote_age_ms":MAX_EXCHANGE_QUOTE_AGE_MS,
            "fill_timestamp_source":"hftbacktest order.exch_timestamp in ns",
            "BBO_time_source":"Tardis OKX book_ticker.timestamp in microseconds",
            "not_a_binance_effect_test":True}
    try:
        t0=int(datetime.strptime(day,"%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()*1e6)
        end_us=t0+(10+TRADE_MINUTES)*60*1_000_000
        with tempfile.TemporaryDirectory(prefix="g0_markout_") as td:
            root=Path(td)
            paths={};counts={}
            for kind in SCHEMAS:
                paths[kind]=root/(kind+".csv")
                counts[kind]=extract_window(kind,day,end_us,paths[kind],max_rows=2_500_000)
            record["feeds"]=counts
            record["unit_audit"]=unit_grid_audit(paths)
            exchange_ns,mids=read_exchange_bbo(paths["book_ticker"])
            events=tardis.convert_fuse(str(paths["trades"]),str(paths["incremental_book_L2"]),
                                       str(paths["book_ticker"]),tick_size=TICK,lot_size=LOT,
                                       output_filename=str(root/"fused.npz"),
                                       ss_buffer_size=max(1000,counts["incremental_book_L2"]["snapshot_rows"]+100),
                                       snapshot_mode="process")
            validate_event_order(events)
            record["converted_events"]=int(len(events))
            del events
            lb=float(np.min(mids))-1000
            ub=float(np.max(mids))+1000
            # run_mode also applies +/−1000 ROI margins; extra margin here is safe.
            result=run_mode(root/"fused.npz",lb,ub,TRADE_MINUTES,True,
                            gamma_code=GAMMA,capture_fills=True)
            fills=result.pop("fill_trace")
            record["g0"]=result
            record["markout"]=evaluate_fill_markouts(fills,exchange_ns,mids)
            record["total_official_fills"]=result["trading"]["engine_fills"]
            if len(fills)!=record["total_official_fills"]:
                raise RuntimeError("Fill timestamp trace does not reconcile")
            record["aggregate_trace_matches_official_engine"]=True
        record["status"]="complete"
    except Exception as exc:
        record["status"]="failed"
        record["error"]=f"{type(exc).__name__}: {exc}"
        raise
    finally:
        # Never upload per-order fills or Tardis raw BBO; aggregate only.
        target.write_text(json.dumps(record,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
        print(f"Markout {day}: {record['status']} -> {target}",flush=True)


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--day",default="2026-11-01",choices=FREE_DATES)
    parser.add_argument("--report-dir",default="reports")
    args=parser.parse_args()
    run(args.day,args.report_dir)


if __name__=="__main__":
    main()
