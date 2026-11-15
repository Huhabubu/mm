"""Binance-origin filtered Gate GLFT nine-model paired official HBT replay.

The pilot uses 10min warmup +50min trading, and full-day mode uses 10min
warmup +1430min trading. One HBT engine per model, no custom matching.
Call only after separately passing full-day four-feed QA.
"""
import argparse
import csv
import gzip
import io
import json
import math
import tempfile
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from check_okx_hft_data import SCHEMAS, require_unshifted_target_clock
from cross_venue_event_probe import read_binance_bbo, visible_timestamps
from g0_glft_event_driven import summarize
from gate_basis_weighted_moments import load_gate_mid, exact_moments_5s
from gate_origin_glft_strategy import run_gate_origin_glft
from qa_gate_btc_futures import fetch_contract

GATE_EXCHANGE="gate-io-futures"
GATE_SYMBOL="BTC_USDT"
ROOT="https://datasets.tardis.dev/v1"
TARGET_NOTIONAL_USDT=80.
RISK_AVERSION_PER_USDT=50.
INVENTORY_MULTIPLE=20
BINANCE_DELAY_MS=2.5
UA={"User-Agent":"mm-gate-glft-btc-hbt/1.0"}


def safe_gate_csv(kind, day, end_us, path, max_rows):
    """Stream losslessly into HBT Tardis CSV schema; no custom orderbook merge."""
    if kind not in SCHEMAS:
        raise ValueError(kind)
    source_kind="quotes" if kind=="book_ticker" else kind
    y,m,d=day.split("-")
    url=f"{ROOT}/{GATE_EXCHANGE}/{source_kind}/{y}/{m}/{d}/{GATE_SYMBOL}.csv.gz"
    fields=SCHEMAS[kind]
    metadata={"rows":0,"snapshot_rows":0,"initial_snapshot_bid":0,
              "initial_snapshot_ask":0,"initial_snapshot_complete":False,
              "invalid_rows":0,"local_backwards":0,"negative_feed_latency_rows":0,
              "min_feed_latency_us":None,"first_local_us":None,"last_local_us":None}
    first_snap=True
    first_snapshot_seen=False
    seen_post_snapshot=False
    prev=-1
    with urllib.request.urlopen(urllib.request.Request(url,headers=UA),timeout=240) as resp:
        with gzip.GzipFile(fileobj=resp) as zipped:
            with io.TextIOWrapper(zipped,encoding="utf-8",newline="") as stream:
                reader=csv.DictReader(stream)
                if not set(fields).issubset(set(reader.fieldnames or ())):
                    raise ValueError(f"Gate {kind} missing CSV columns")
                with Path(path).open("w",encoding="utf-8",newline="") as dst:
                    writer=csv.DictWriter(dst,fieldnames=fields)
                    writer.writeheader()
                    for row in reader:
                        ts=int(row["local_timestamp"])
                        if ts>=end_us:
                            break
                        if prev>=0 and ts<prev:
                            metadata["local_backwards"]+=1
                        prev=ts
                        if metadata["first_local_us"] is None:
                            metadata["first_local_us"]=ts
                        metadata["last_local_us"]=ts
                        if kind=="incremental_book_L2":
                            snapshot=row["is_snapshot"].lower()=="true"
                            if not first_snapshot_seen and not snapshot:
                                continue
                            if snapshot:
                                first_snapshot_seen=True
                                if not seen_post_snapshot:
                                    metadata["snapshot_rows"]+=1
                                    if row["side"]=="bid":
                                        metadata["initial_snapshot_bid"]+=1
                                    elif row["side"]=="ask":
                                        metadata["initial_snapshot_ask"]+=1
                            else:
                                seen_post_snapshot=True
                        if kind in ("trades","incremental_book_L2"):
                            px=float(row["price"])
                            amt=float(row["amount"])
                            if not (math.isfinite(px) and px>0 and math.isfinite(amt) and amt>=0):
                                metadata["invalid_rows"]+=1
                            if kind=="trades" and abs(amt-round(amt))>1e-6:
                                metadata["invalid_rows"]+=1
                        else:
                            bid=float(row["bid_price"]);ask=float(row["ask_price"])
                            if not (math.isfinite(bid) and math.isfinite(ask) and 0<bid<ask):
                                metadata["invalid_rows"]+=1
                        exchange_ts=int(row["timestamp"])
                        latency=ts-exchange_ts
                        if latency<0:
                            metadata["negative_feed_latency_rows"]+=1
                        mi=metadata["min_feed_latency_us"]
                        metadata["min_feed_latency_us"]=latency if mi is None else min(latency,mi)
                        writer.writerow({k:row[k] for k in fields})
                        metadata["rows"]+=1
                        if metadata["rows"]>max_rows:
                            raise ValueError(f"{kind} exceeds max {max_rows} lines")
                        if metadata["rows"]%3_000_000==0:
                            print("GATE_EXTRACT",day,kind,metadata["rows"],flush=True)
    metadata["initial_snapshot_complete"]=bool(
        first_snapshot_seen and seen_post_snapshot
        and metadata["initial_snapshot_bid"]>0 and metadata["initial_snapshot_ask"]>0)
    if (metadata["invalid_rows"] or metadata["local_backwards"] or
          metadata["negative_feed_latency_rows"] or metadata["rows"]==0 or
          (kind=="incremental_book_L2" and not metadata["initial_snapshot_complete"])):
        raise ValueError(f"Gate {kind} cannot be used in official HBT: {metadata}")
    return metadata


def scenario(mid, spec):
    tick=float(spec["order_price_round"])
    lot=int(spec["order_size_min"])
    ctval=float(spec["quanto_multiplier"])
    if not (tick>0 and lot>=1 and ctval>0):
        raise ValueError("Invalid Gate contract")
    qty=max(lot,round(TARGET_NOTIONAL_USDT/(mid*ctval*lot))*lot)
    return {"tick":tick,"lot":lot,"ctval":ctval,"qty":float(qty),
            "position_cap":float(qty*INVENTORY_MULTIPLE),
            "gamma_code":RISK_AVERSION_PER_USDT*ctval*tick,
            "approx_order_notional_usdt":qty*ctval*mid,
            "maker_fee_rate":float(spec["maker_fee_rate"]),
            "taker_fee_rate":float(spec["taker_fee_rate"])}


def build_asset(parts, low, high, spec):
    from hftbacktest import BacktestAsset
    margin=max(1000*spec["tick"], .02*(low+high)/2)
    return (BacktestAsset().data([str(x) for x in parts])
            .linear_asset(spec["ctval"])
            .constant_order_latency(1_000_000,1_000_000)
            .power_prob_queue_model(2.0).no_partial_fill_exchange()
            .trading_value_fee_model(spec["maker_fee_rate"],spec["taker_fee_rate"])
            .tick_size(spec["tick"]).lot_size(spec["lot"])
            .roi_lb(low-margin).roi_ub(high+margin)
            .last_trades_capacity(20000))


def variants():
    return [("glft",0.0,0)] + [
        (mode+"_"+str(beta).replace(".","p"),beta,code)
        for mode,code in (("both",1),("defensive",2))
        for beta in (.25,.5,.75,1.)]


def run(day, minutes=50, out="reports/gate_origin_glft"):
    from hftbacktest import Recorder,ROIVectorMarketDepthBacktest
    from hftbacktest.data.utils import tardis
    from hftbacktest.data.validation import validate_event_order
    date=datetime.strptime(day,"%Y-%m-%d").replace(tzinfo=timezone.utc)
    if date.day!=1 or minutes not in (50,1430):
        raise ValueError("Only first-of-month 50/1430 minute research runs supported")
    day_us=int(date.timestamp()*1_000_000)
    trade_start=day_us+600_000_000
    trade_stop=trade_start+minutes*60_000_000
    end_us=trade_stop+(5_000_000 if minutes==50 else 0)
    out_path=Path(out)
    out_path.mkdir(parents=True,exist_ok=True)
    report_target=out_path/f"{day}_{minutes}m.json"
    report={"day":day,"minutes_traded":minutes,"warmup_minutes":10,
            "baseline_window_seconds":600,"baseline_refit_seconds":5,
            "binance_extra_visible_delay_ms":BINANCE_DELAY_MS,
            "fee_inclusive_only":True,"status":"started",
            "sample_already_explored":True,"variants":{},
            "daily_pnl":minutes==1430}
    try:
        with tempfile.TemporaryDirectory(prefix="mm_gate_origin_") as temp:
            root=Path(temp)
            spec=fetch_contract()
            if spec["type"]!="direct":
                raise ValueError("Gate current contract not BTC direct USDT perpetual")
            report["gate_current_contract"]=spec
            paths={};counts={}
            for kind in SCHEMAS:
                path=root/f"{kind}.csv"
                print("GATE_DOWNLOAD",day,kind,flush=True)
                counts[kind]=safe_gate_csv(kind,day,end_us,path,
                         max_rows=90_000_000 if minutes==1430 else 6_000_000)
                paths[kind]=path
            require_unshifted_target_clock(counts)
            report["feed_stats"]=counts
            ot,om=load_gate_mid(paths["book_ticker"])
            b_local,bm=read_binance_bbo(day,end_us,symbol="BTCUSDT")
            bt=visible_timestamps(b_local,BINANCE_DELAY_MS)
            grid,basis,sigma,coverage=exact_moments_5s(
                bt,bm,ot,om,trade_start*1000,trade_stop*1000)
            report["history_baseline"]={
                "samples_5s":len(grid),
                "minimum_600s_history_coverage":float(np.min(coverage)),
                "initial_baseline_usdt_per_btc":float(basis[0]),
                "initial_sigma_usdt_per_btc":float(sigma[0])}
            pretrade=om[ot<trade_start*1000]
            if not len(pretrade):
                raise RuntimeError("Missing Gate pretrade midpoint")
            cfg=scenario(float(np.median(pretrade)),spec)
            report["scenario"]=cfg
            if minutes==1430:
                from tardis_segmented_hbt import convert_fuse_parts
                parts,details=convert_fuse_parts(
                    paths,counts,root,day_us,end_us,60,cfg["tick"],float(cfg["lot"]))
                report["segmented_parts"]=details
            else:
                npz=root/"fused.npz"
                hbt_events=tardis.convert_fuse(
                    str(paths["trades"]),str(paths["incremental_book_L2"]),
                    str(paths["book_ticker"]),tick_size=cfg["tick"],
                    lot_size=float(cfg["lot"]),output_filename=str(npz),
                    ss_buffer_size=max(1000,counts["incremental_book_L2"]["snapshot_rows"]+100),
                    snapshot_mode="process")
                validate_event_order(hbt_events)
                report["hbt_events"]=len(hbt_events)
                del hbt_events
                parts=[npz]
            low=float(np.min(om));high=float(np.max(om))
            tape_len=minutes*600+10
            for name,beta,mode in variants():
                print("GATE_MODEL_START",day,name,flush=True)
                engine=ROIVectorMarketDepthBacktest([build_asset(parts,low,high,cfg)])
                rec=Recorder(engine.num_assets,tape_len)
                try:
                    raw=run_gate_origin_glft(engine,bt,bm,minutes*600,
                        beta,grid,basis,sigma,mode,cfg["gamma_code"],
                        cfg["tick"],cfg["ctval"],cfg["qty"],cfg["position_cap"],
                        rec.recorder)
                    if raw[5]!=1:
                        raise RuntimeError("Gate GLFT pretrade A/k calibration failed")
                    summary=summarize(raw,minutes,True,beta,cfg["position_cap"],cfg["qty"])
                    counters=raw[-1]
                    summary["binance_origin_filter"]={
                        k:int(v) for k,v in zip(
                            ("binance_price_update_groups","eligible_gap_checks",
                             "above_1p96sigma_checks","directional_dominance_checks",
                             "binance_origin_activations","active_decisions",
                             "same_timestamp_ties_blocked"),counters)}
                    summary["quote_mode"]=name
                    records=rec.get(0)
                    if len(records)<minutes*600-20:
                        raise RuntimeError("Insufficient official Recorder samples")
                    if not np.all(np.diff(records["timestamp"])>0):
                        raise RuntimeError("Nonmonotonic official Recorder clock")
                    if not np.isfinite(summary["trading"]["end_net_marked_equity_usdt"]):
                        raise RuntimeError("Invalid official fee-inclusive net equity")
                    dest=out_path/"recorder"/day/name
                    dest.mkdir(parents=True,exist_ok=True)
                    rec.to_npz(str(dest/"recorder.npz"))
                    np.savez_compressed(dest/"fills.npz",
                        exchange_timestamp_ns=raw[-5],
                        side=raw[-4],
                        local_observed_ns=raw[-2][1],
                        exec_price=raw[-2][2],
                        exec_qty=raw[-2][3])
                    report["variants"][name]=summary
                    print("GATE_MODEL_OK",day,name,
                          "net_usdt",summary["trading"]["end_net_marked_equity_usdt"],
                          "fee_usdt",summary["trading"]["fee_usdt"],flush=True)
                finally:
                    engine.close()
            base=report["variants"]["glft"]["trading"]["end_net_marked_equity_usdt"]
            report["net_differences_vs_glft_usdt"]={
                k:float(v["trading"]["end_net_marked_equity_usdt"]-base)
                for k,v in report["variants"].items()}
            report["status"]="complete"
    except Exception as exc:
        report["status"]="failed"
        report["error"]=f"{type(exc).__name__}: {exc}"
        raise
    finally:
        report_target.write_text(json.dumps(report,ensure_ascii=False,indent=2)+"\n",
                                encoding="utf-8")
        print("GATE_RUN_REPORT",report_target,report["status"],flush=True)
    return report


if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--day",required=True)
    parser.add_argument("--minutes",type=int,choices=(50,1430),default=50)
    parser.add_argument("--out",default="reports/gate_origin_glft")
    args=parser.parse_args()
    run(args.day,args.minutes,args.out)
