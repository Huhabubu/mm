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
from gate_l2_snapshot_adapter import normalized_gate_l2_rows
from qa_gate_btc_futures import fetch_contract

GATE_EXCHANGE="gate-io-futures"
GATE_SYMBOL="BTC_USDT"
ROOT="https://datasets.tardis.dev/v1"
TARGET_NOTIONAL_USDT=80.
RISK_AVERSION_PER_USDT=50.
INVENTORY_MULTIPLE=20
BINANCE_DELAY_MS=2.5
UA={"User-Agent":"mm-gate-glft-btc-hbt/1.0"}


def safe_gate_csv(kind, day, end_us, path, max_rows, symbol=GATE_SYMBOL,
                  historical_tick=None, historical_lot=None):
    """Stream losslessly into HBT Tardis CSV schema; no custom orderbook merge."""
    if kind not in SCHEMAS:
        raise ValueError(kind)
    source_kind="quotes" if kind=="book_ticker" else kind
    y,m,d=day.split("-")
    url=f"{ROOT}/{GATE_EXCHANGE}/{source_kind}/{y}/{m}/{d}/{symbol}.csv.gz"
    fields=SCHEMAS[kind]
    metadata={"rows":0,"snapshot_rows":0,"initial_snapshot_bid":0,
              "initial_snapshot_ask":0,"initial_snapshot_complete":False,
              "invalid_rows":0,"local_backwards":0,"negative_feed_latency_rows":0,
              "min_feed_latency_us":None,"first_local_us":None,"last_local_us":None,
              "l2_format":None,"source_l2_rows":0,"emitted_l2_rows":0,
              "source_snapshot_groups":0,"removed_l2_levels":0}
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
                adapted=(normalized_gate_l2_rows(reader,metadata)
                         if kind=="incremental_book_L2" else reader)
                with Path(path).open("w",encoding="utf-8",newline="") as dst:
                    writer=csv.DictWriter(dst,fieldnames=fields)
                    writer.writeheader()
                    for row in adapted:
                        if symbol != GATE_SYMBOL and row.get("symbol") != symbol:
                            raise ValueError(f"{kind}: unexpected Tardis symbol {row.get('symbol')}")
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
                            if kind=="trades":
                                unit = 1.0 if historical_lot is None else historical_lot
                                if abs(amt/unit-round(amt/unit))>1e-6:
                                    metadata["invalid_rows"]+=1
                        else:
                            bid=float(row["bid_price"]);ask=float(row["ask_price"])
                            if not (math.isfinite(bid) and math.isfinite(ask) and 0<bid<ask):
                                metadata["invalid_rows"]+=1
                        if historical_tick is not None:
                            prices = ((float(row["price"]),)
                                      if kind in ("trades", "incremental_book_L2")
                                      else (float(row["bid_price"]), float(row["ask_price"])))
                            if any(abs(px/historical_tick-round(px/historical_tick)) > 1e-5
                                   for px in prices):
                                raise ValueError(
                                    f"{symbol} {day} {kind}: historical price incompatible "
                                    f"with current tick={historical_tick}")
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
    lot=float(spec["order_size_min"])
    ctval=float(spec["quanto_multiplier"])
    if not (tick>0 and lot>0 and ctval>0):
        raise ValueError("Invalid Gate contract")
    qty=max(lot,round(TARGET_NOTIONAL_USDT/(mid*ctval*lot))*lot)
    return {"tick":tick,"lot":lot,"ctval":ctval,"qty":float(qty),
            "position_cap":float(qty*INVENTORY_MULTIPLE),
            "gamma_code":RISK_AVERSION_PER_USDT*ctval*tick,
            "approx_order_notional_usdt":qty*ctval*mid,
            "tick_relative_bps":10_000*tick/mid,
            "quantity_units":"Gate contract quantity; decimal lots when enabled",
            "gamma_units":"inverse (contract * price_tick)",
            "maker_fee_rate":0.0002,  # Gate BTC perpetual VIP0 research scenario; charge, no rebate.
            "taker_fee_rate":0.0005,
            "api_maker_fee_rate":float(spec["maker_fee_rate"]),
            "api_taker_fee_rate":float(spec["taker_fee_rate"])}


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


def load_gate_bbo_update_clocks(path):
    """Gate native BBO updates are separate from trade feed events, in local time.

    Tardis timestamps are microseconds; convert both to HBT nanoseconds.
    Actual quotes values are not read ahead by the strategy.
    """
    local, exchange = [], []
    with Path(path).open(newline="", encoding="utf-8") as stream:
        for row in csv.DictReader(stream):
            ts=int(row["local_timestamp"])
            ex=int(row["timestamp"])
            if ts<ex or ex<=0 or (local and ts<local[-1]):
                raise ValueError("Invalid causal Gate BBO update timestamp")
            local.append(ts)
            exchange.append(ex)
    if not local:
        raise ValueError("Missing public Gate BBO update timestamps")
    return np.asarray(local,dtype=np.int64)*1000, np.asarray(exchange,dtype=np.int64)*1000


def run(day, minutes=50, out="reports/gate_origin_glft",
        symbol=GATE_SYMBOL, selected_models=None, core_options=None):
    from hftbacktest import Recorder,ROIVectorMarketDepthBacktest
    from hftbacktest.data.utils import tardis
    from hftbacktest.data.validation import validate_event_order
    if symbol not in ("BTC_USDT", "SOL_USDT", "DOGE_USDT", "ETH_USDT"):
        raise ValueError("Only studied Gate direct USDT perpetuals are supported")
    default_variants = variants()
    available = {name:(beta,mode) for name,beta,mode in default_variants}
    available["risk_side_pause"] = (1.0,3)
    chosen = (default_variants if selected_models is None else
              [(name,*available[name]) for name in selected_models])
    if "glft" not in (name for name,_,_ in chosen):
        raise ValueError("All comparisons require GLFT baseline")
    if not chosen or len(set(n for n,_,_ in chosen)) != len(chosen):
        raise ValueError("Invalid or repeated model selection")
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
    report={"day":day,"gate_symbol":symbol,
            "binance_symbol":symbol.replace("_",""),
            "minutes_traded":minutes,"warmup_minutes":10,
            "baseline_window_seconds":600,"baseline_refit_seconds":5,
            "binance_extra_visible_delay_ms":BINANCE_DELAY_MS,
            "fee_inclusive_only":True,"status":"started",
            "sample_already_explored":True,"variants":{},
            "daily_pnl":minutes==1430,
            "core_extension":core_options is not None}
    try:
        with tempfile.TemporaryDirectory(prefix="mm_gate_origin_") as temp:
            root=Path(temp)
            spec=fetch_contract(symbol)
            if spec["type"]!="direct":
                raise ValueError(f"Frozen Gate study contract not direct USDT perpetual: {symbol}")
            report["gate_frozen_contract"]=spec
            paths={};counts={}
            for kind in SCHEMAS:
                path=root/f"{kind}.csv"
                print("GATE_DOWNLOAD",day,kind,flush=True)
                counts[kind]=safe_gate_csv(
                    kind,day,end_us,path,
                    max_rows=90_000_000 if minutes==1430 else 6_000_000,
                    symbol=symbol,
                    historical_tick=(float(spec["order_price_round"])
                                     if symbol!=GATE_SYMBOL else None),
                    historical_lot=(float(spec["order_size_min"])
                                    if symbol!=GATE_SYMBOL else None))
                paths[kind]=path
            require_unshifted_target_clock(counts)
            report["feed_stats"]=counts
            ot,om=load_gate_mid(paths["book_ticker"])
            b_local,bm=read_binance_bbo(
                day,end_us,symbol=symbol.replace("_",""))
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
            if symbol!=GATE_SYMBOL:
                binance_pre= bm[bt<trade_start*1000]
                if not len(binance_pre):
                    raise ValueError("Missing Binance pretrade BBO")
                ratio=float(np.median(binance_pre)/np.median(pretrade))
                if not 0.9<ratio<1.1:
                    raise ValueError(f"Gate/Binance price-unit mismatch: {ratio}")
                report["pretrade_binance_to_gate_price_ratio"]=ratio
                if not 0.5*TARGET_NOTIONAL_USDT<=cfg["approx_order_notional_usdt"]<=1.5*TARGET_NOTIONAL_USDT:
                    raise ValueError(
                        f"Contract lot cannot match 80 USDT order: {cfg['approx_order_notional_usdt']}")
            report["scenario"]=cfg
            report["experimental_trigger_sigma_multiple"]=1.645
            report["fee_policy"]="VIP0_assumed_maker_plus_0.020pct_no_rebate"
            print("GATE_SCENARIO",day,json.dumps(cfg),flush=True)
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
            if core_options is not None:
                from core_glft_engine import run_core_glft
                core_options=dict(core_options)
                accepted = {"min_requote_ns","fit_mode","wait_book_after_fill",
                            "post_only_clamp_mode","cap_depth_at_visible_levels",
                            "forced_start_ns"}
                if set(core_options)-accepted:
                    raise ValueError("Unknown core options: "+repr(set(core_options)-accepted))
                if core_options.get("wait_book_after_fill",False):
                    bbo_local,bbo_exchange=load_gate_bbo_update_clocks(paths["book_ticker"])
                    core_options["gate_book_local_ns"]=bbo_local
                    core_options["gate_book_exchange_ns"]=bbo_exchange
                depth_format=counts["incremental_book_L2"]["l2_format"]
                core_options["depth_is_20_level_snapshot"]=(
                    depth_format=="per_message_full_snapshots_rewritten_to_deltas")
                shared_start=int(core_options.get("forced_start_ns",0))
                if core_options.get("fit_mode","legacy_half_tick")=="physical_1tick_poisson" and shared_start==0:
                    # First fit is feed-only: no one can place orders before it.
                    probe=ROIVectorMarketDepthBacktest([build_asset(parts,low,high,cfg)])
                    try:
                        check=np.zeros(7,np.float64)
                        run_core_glft(probe,bt,bm,minutes*600,
                            0.,grid,basis,sigma,0,cfg["gamma_code"],cfg["tick"],
                            cfg["ctval"],cfg["qty"],cfg["position_cap"],None,
                            trade_stop*1000 if minutes==1430 else 0,
                            diagnostics=check, **core_options)
                        if check[3]<0 or check[5]!=1:
                            raise RuntimeError("No successful physical GLFT calibration; exclude missing day")
                        shared_start=int(check[3])
                    finally:
                        probe.close()
                    core_options["forced_start_ns"]=shared_start
                report["shared_first_eligible_entry_ns"]=shared_start if shared_start else None
                report["core_options"]={key:val for key,val in core_options.items()
                                       if key not in ("gate_book_local_ns","gate_book_exchange_ns")}
            tape_len=minutes*600+10
            for name,beta,mode in chosen:
                print("GATE_MODEL_START",day,name,flush=True)
                engine=ROIVectorMarketDepthBacktest([build_asset(parts,low,high,cfg)])
                rec=Recorder(engine.num_assets,tape_len)
                try:
                    if core_options is None:
                        strategy = run_gate_origin_glft
                        if mode == 3:
                            from gate_stage5a_pause_strategy import run_gate_stage5a_pause
                            strategy = run_gate_stage5a_pause
                        raw=strategy(engine,bt,bm,minutes*600,
                            beta,grid,basis,sigma,mode,cfg["gamma_code"],
                            cfg["tick"],cfg["ctval"],cfg["qty"],cfg["position_cap"],
                            rec.recorder,trade_stop*1000 if minutes==1430 else 0)
                    else:
                        diagnostic=np.zeros(7,np.float64)
                        raw=run_core_glft(engine,bt,bm,minutes*600,
                            beta,grid,basis,sigma,mode,cfg["gamma_code"],
                            cfg["tick"],cfg["ctval"],cfg["qty"],cfg["position_cap"],
                            rec.recorder,trade_stop*1000 if minutes==1430 else 0,
                            diagnostics=diagnostic, **core_options)
                    if raw[5]!=1:
                        raise RuntimeError("Gate GLFT pretrade A/k calibration failed")
                    summary=summarize(
                        raw,minutes,True,beta,cfg["position_cap"],cfg["qty"],
                        tail_tolerance_samples=(10 if symbol!=GATE_SYMBOL else 5))
                    if symbol!=GATE_SYMBOL:
                        summary["end_of_day_missing_100ms_samples"] = max(
                            0,6000+minutes*600-int(raw[0]))
                    if minutes==1430 and summary["end_time_ns"] < trade_stop*1000-1_000_000_000:
                        raise RuntimeError("Full-day HBT replay stopped more than 1s before UTC midnight")
                    counters=raw[-1]
                    summary["binance_origin_filter"]={
                        k:int(v) for k,v in zip(
                            ("binance_price_update_groups","eligible_gap_checks",
                             "above_threshold_checks","directional_dominance_checks",
                             "binance_origin_activations","active_decisions",
                             "same_timestamp_ties_blocked"),counters)}
                    summary["sigma_threshold"]=1.645
                    if core_options is not None:
                        summary["max_ops_per_sec"]=int(diagnostic[0])
                        summary["resting_time_ratio"]={
                            "bid":float(diagnostic[1]),
                            "ask":float(diagnostic[2]),
                            "definition":"time-weighted locally-cancellable order presence; not exchange-confirmed true resting",
                            "verified_exchange_resting":False}
                        summary["initial_fit_ns"]=(int(diagnostic[3]) if diagnostic[3]>=0 else None)
                        summary["calibration_wait_ns"]=int(diagnostic[4])
                        summary["market_sync_pending_at_stop"]=bool(diagnostic[6])
                    summary["quote_mode"]=name
                    if symbol!=GATE_SYMBOL:
                        summary["trading"]["end_mid_usdt_per_base"] = (
                            summary["trading"].pop("end_mid_usdt_per_btc"))
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
                    if symbol!=GATE_SYMBOL:
                        from analyze_gate_1645_fill_quality import model_day
                        fill = {
                            "exchange_timestamp_ns":raw[-5],
                            "side":raw[-4],
                            "local_observed_ns":raw[-2][1],
                            "exec_price":raw[-2][2],
                            "exec_qty":raw[-2][3],
                        }
                        _, quality = model_day(name,fill,{"trading":summary["trading"]},
                                               ot,om,trade_stop*1000,cfg["ctval"])
                        summary["quote_quality"]=quality
                    report["variants"][name]=summary
                    print("GATE_MODEL_OK",day,name,
                          "net_usdt",summary["trading"]["end_net_marked_equity_usdt"],
                          "fee_usdt",summary["trading"]["fee_usdt"],
                          "fills",summary["trading"]["engine_fills"],
                          "signal_activations",summary["binance_origin_filter"]["binance_origin_activations"],
                          "active_decisions",summary["binance_origin_filter"]["active_decisions"],
                          flush=True)
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
    parser.add_argument("--symbol",choices=("BTC_USDT","SOL_USDT","DOGE_USDT","ETH_USDT"),
                        default=GATE_SYMBOL)
    parser.add_argument("--models",default=None,
                        help="Comma-separated named variants; omitted retains BTC nine-model behavior")
    parser.add_argument("--core-config",type=Path,default=None,
                        help="Optional JSON of explicit 2B switch settings; omitted runs legacy")

    args=parser.parse_args()
    selection=None if args.models is None else [s.strip() for s in args.models.split(",")]
    options=(None if args.core_config is None else
             json.loads(args.core_config.read_text(encoding="utf-8")))
    if options is not None and not isinstance(options,dict):
        parser.error("--core-config JSON must be an object")
    run(args.day,args.minutes,args.out,args.symbol,selection,options)
