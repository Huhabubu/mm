"""Bitget 4B official HBT: 50m smoke or 1430m continuous exchange replay.

Use the frozen Gate 5.1 order target as the same-day base-coin size target,
not an independently rounded "80 USDT" number. Bitget order minimum and
quantity increment are candidate specs, not proven historical exchange rules.

Uses hftbacktest.convert_fuse, official queue/latency/fill model, 2bps maker.
For DOGE: raw L2 1e-6 tick, legal GTX quoting 1e-5 via quote_tick_mult=10.
"""
import argparse
import csv
import json
import math
import tempfile
from datetime import datetime,timezone
from pathlib import Path
import numpy as np

from core_glft_engine import run_core_glft
from check_okx_hft_data import SCHEMAS
from cross_venue_event_probe import read_binance_bbo,visible_timestamps
from gate_basis_weighted_moments import load_gate_mid,exact_moments_5s
from run_gate_btc_origin_glft import (
    safe_gate_csv, scenario as gate_scenario, fetch_contract,
    load_gate_bbo_update_clocks, TARGET_NOTIONAL_USDT, RISK_AVERSION_PER_USDT,
    INVENTORY_MULTIPLE, BINANCE_DELAY_MS
)
from g0_glft_event_driven import summarize
from tardis_segmented_hbt import convert_fuse_parts
from bitget_4b_posthoc import source_risk_events, posthoc_slices

MODELS=(("glft",0.,0),("both_1p0",1.,1),("risk_side_pause",1.,3))

def config_for(symbol, cfg_path):
    cfg=json.loads(Path(cfg_path).read_text(encoding="utf-8"))
    if symbol not in cfg["symbols"]:
        raise ValueError("Unexpected Bitget futures symbol: "+symbol)
    spec=cfg["symbols"][symbol]
    if not (cfg["maker_rate"]==0.0002 and
            cfg["historical_spec_verified"] is False and
            spec["contract_value_base"]==1):
        raise ValueError("Invalid frozen Bitget simulation units/fee claim")
    n=spec["quote_tick_mult"]
    if n<1 or int(n)!=n or not math.isclose(
            spec["hbt_tick_size"]*n,spec["legal_quote_tick"],rel_tol=1e-10):
        raise ValueError("Invalid dual tick configuration")
    if symbol=="DOGEUSDT" and (n!=10 or spec["hbt_tick_size"]!=0.000001):
        raise ValueError("DOGE raw 6-decimal L2 tick must not be rounded away")
    return cfg,spec

def exact_gate_equivalent(gate_pretrade_mid, gate_spec, bitget_spec):
    """Nearest legal Bitget quantity to Gate's frozen same-day base target.

    Historical name retained for existing imports; strict equality is reported
    as an explicit boolean and never assumed for rounded assets.
    """
    frozen=gate_scenario(gate_pretrade_mid,gate_spec)
    gate_base_qty=float(frozen["qty"])*float(frozen["ctval"])
    gate_base_cap=float(frozen["position_cap"])*float(frozen["ctval"])
    step=float(bitget_spec["bitget_lot_size_base"])
    minimum=float(bitget_spec["min_order_qty_base"])
    if not (gate_base_qty>0 and gate_base_cap>0 and step>0 and minimum>0):
        raise ValueError("Invalid input quantity/contract units")
    # Nearest legal increment, deterministic half-up tie and positive minimum.
    min_steps=max(1,math.ceil(minimum/step-1e-10))
    steps=max(min_steps,math.floor(gate_base_qty/step+0.5+1e-10))
    bitget_qty=steps*step
    if bitget_qty < minimum-1e-9 or not math.isclose(
            bitget_qty/step,round(bitget_qty/step),abs_tol=1e-8):
        raise ValueError("Bitget nearest quantity is off the candidate grid")
    error=bitget_qty-gate_base_qty
    gate_multiple=gate_base_cap/gate_base_qty
    if not math.isclose(gate_multiple,INVENTORY_MULTIPLE,rel_tol=1e-10):
        raise ValueError("Unexpected frozen Gate inventory multiple")
    bitget_cap=bitget_qty*gate_multiple
    return {
        "gate_frozen_scenario":frozen,
        "gate_reference_mid_usdt":gate_pretrade_mid,
        "gate_order_qty_contracts":frozen["qty"],
        "gate_quanto_multiplier_base_per_contract":frozen["ctval"],
        "gate_max_pos_contracts":frozen["position_cap"],
        "gate_order_qty_base":gate_base_qty,
        "gate_max_pos_base":gate_base_cap,
        "bitget_order_qty_base":bitget_qty,
        "bitget_max_pos_base":bitget_cap,
        "bitget_candidate_order_step_base":step,
        "gate_target_qty_base":gate_base_qty,
        "bitget_actual_qty_base":bitget_qty,
        "quantity_signed_difference_base":error,
        "quantity_abs_difference_base":abs(error),
        "quantity_relative_difference":error/gate_base_qty,
        "strict_base_amount_equal":math.isclose(error,0.,abs_tol=1e-9),
        "position_cap_over_order_qty_gate":gate_multiple,
        "position_cap_over_order_qty_bitget":bitget_cap/bitget_qty,
        "gate_order_nominal_usdt":frozen["approx_order_notional_usdt"],
        "bitget_nominal_at_gate_reference_usdt":bitget_qty*gate_pretrade_mid,
        "nominal_difference_at_gate_reference_usdt":error*gate_pretrade_mid,
        "nominal_relative_difference":error/gate_base_qty,
    }

def source_l2_grid_check(path,raw_tick,legal_tick):
    """Read extracted historical L2 prices, never quantize or merge rows."""
    total=offlegal=offraw=0
    min_px,max_px=math.inf,0.
    unusual=[]
    bid_levels,ask_levels={},{}
    prev_local=None
    snap_in_progress=False
    has_snapshot=False
    snapshot_bids=snapshot_asks=0
    first_snapshot_complete=False
    checked_group_ns=-1
    full_group_checks=crossed_full_groups=0
    with Path(path).open(newline="",encoding="utf-8") as file:
        for row in csv.DictReader(file):
            p=float(row["price"])
            local=int(row["local_timestamp"])*1000
            if prev_local is not None and local!=prev_local:
                # A Tardis WebSocket update is often split across many L2
                # price-level CSV rows at the SAME local timestamp. Only
                # check the completed previous message, never its transient
                # partially applied intermediate book.
                if (has_snapshot and not snap_in_progress
                        and local-checked_group_ns>=30_000_000):
                    checked_group_ns=prev_local
                    if bid_levels and ask_levels:
                        full_group_checks+=1
                        if max(bid_levels)>=min(ask_levels):
                            crossed_full_groups+=1
            prev_local=local
            total+=1
            if p<=0 or not math.isfinite(p):
                raise ValueError("Invalid L2 source price")
            min_px=min(min_px,p);max_px=max(max_px,p)
            if abs(p/raw_tick-round(p/raw_tick))>2e-5:
                offraw+=1
            if abs(p/legal_tick-round(p/legal_tick))>2e-5:
                offlegal+=1
                if len(unusual)<20:
                    unusual.append(row["price"])
            snap=row["is_snapshot"]=="true"
            side=row["side"]
            if snap and not snap_in_progress:
                bid_levels.clear();ask_levels.clear()
                snapshot_bids=snapshot_asks=0
                snap_in_progress=True
            elif not snap and snap_in_progress:
                if not (snapshot_bids and snapshot_asks):
                    raise ValueError("Bitget raw initial/resubscribe snapshot missing a side")
                has_snapshot=True
                first_snapshot_complete=True
                snap_in_progress=False
            if snap:
                if side=="bid": snapshot_bids+=1
                elif side=="ask":snapshot_asks+=1
            if side not in ("bid","ask"):
                raise ValueError("Unexpected L2 side")
            book=bid_levels if side=="bid" else ask_levels
            size=float(row["amount"])
            if size<0 or not math.isfinite(size):
                raise ValueError("Invalid L2 depth")
            if size>0: book[p]=size
            else: book.pop(p,None)
    if snap_in_progress and snapshot_bids and snapshot_asks:
        first_snapshot_complete=True
    if offraw or total<1:
        raise ValueError(f"Lossless raw L2 grid rejected: {offraw}/{total}")
    if not first_snapshot_complete or full_group_checks<1 or crossed_full_groups:
        raise ValueError("Crossed or incomplete grouped Bitget L2 replay state")
    return {
        "rows":total,"price_min":min_px,"price_max":max_px,
        "off_legal_quote_grid_rows":offlegal,
        "off_raw_hbt_grid_rows":offraw,
        "first_20_off_legal_prices_raw_string":unusual,
        "source_rows_preserved_as_raw_ticks":True,
        "l2_post_message_book_checks":full_group_checks,
        "l2_crossed_post_message_book_checks":crossed_full_groups,
        "initial_snapshot_complete":first_snapshot_complete,
        "grouping_rule":"only completed raw local_timestamp WS message, sampled at 30ms maximum frequency",
    }

def hbt_asset(parts,spec,low,high,fee_maker,fee_taker):
    from hftbacktest import BacktestAsset
    tick=float(spec["hbt_tick_size"])
    lot=float(spec["bitget_lot_size_base"])
    lower=max(tick,low-25*tick)
    upper=high+25*tick
    if (upper-lower)/tick>5_000_000:
        raise RuntimeError("ROI from full raw L2 exceeds safe 5M-tick allocation")
    return (BacktestAsset().data([str(p) for p in parts]).linear_asset(1.)
            .constant_order_latency(1_000_000,1_000_000)
            .power_prob_queue_model(2.).no_partial_fill_exchange()
            .trading_value_fee_model(fee_maker,fee_taker)
            .tick_size(tick).lot_size(lot)
            .roi_lb(lower).roi_ub(upper).last_trades_capacity(20000))

def run(day,symbol,out_dir,config_path,minutes=50):
    from hftbacktest import Recorder,ROIVectorMarketDepthBacktest
    from hftbacktest.data.utils import tardis
    from hftbacktest.data.validation import validate_event_order
    all_days=tuple(f"2026-{month:02d}-01" for month in range(6,12))
    if (minutes==50 and (day!="2026-08-01" or symbol not in ("BTCUSDT","DOGEUSDT"))):
        raise ValueError("Frozen smoke mode only permits BTC/DOGE August 2026 50m")
    if minutes==1430 and day not in all_days:
        raise ValueError("Full day requires 2026 June-November month-first dates")
    if minutes not in (50,1430):
        raise ValueError("Only 50m smoke or 1430m trading after 10m warmup")
    config,spec=config_for(symbol,config_path)
    day_dt=datetime.strptime(day,"%Y-%m-%d").replace(tzinfo=timezone.utc)
    day_us=int(day_dt.timestamp()*1_000_000)
    trade_start_us=day_us+600_000_000
    trade_stop_us=trade_start_us+minutes*60_000_000
    end_us=trade_stop_us+(5_000_000 if minutes==50 else 0)
    out=Path(out_dir);out.mkdir(parents=True,exist_ok=True)
    output=out/f"{symbol}_{day}_{minutes}m.json"
    report={
        "venue":"bitget-futures","symbol":symbol,"day":day,"minutes_traded":minutes,
        "status":"started",
        "stage":("Bitget 4B 1430-minute full UTC day" if minutes==1430
                 else "Bitget 4B 50-minute smoke"),
        "no_full_day":minutes==50,"okx_frozen":True,
        "hbt_official":True,"simulated_maker_fee_rate":config["maker_rate"],
        "simulated_taker_fee_rate":config["taker_rate"],
        "historical_spec_verified":False,
        "quote_tick_mult":spec["quote_tick_mult"],
        "hbt_raw_price_tick":spec["hbt_tick_size"],
        "legal_order_quote_tick":spec["legal_quote_tick"],
        "models":{},
    }
    try:
        with tempfile.TemporaryDirectory(prefix="bitget_hbt_4b_") as temp:
            root=Path(temp)
            gate_book=root/"gate_reference_book_ticker.csv"
            gate_counts=safe_gate_csv(
                "book_ticker",day,trade_start_us,gate_book,1_000_000,
                symbol=spec["gate_symbol"])
            gate_t,gate_mid=load_gate_mid(gate_book)
            before=gate_mid[gate_t<trade_start_us*1000]
            if not len(before):
                raise ValueError("Gate reference midpoint missing before shared warmup")
            gate_med=float(np.median(before))
            gate_frozen=fetch_contract(spec["gate_symbol"])
            quantity=exact_gate_equivalent(gate_med,gate_frozen,spec)
            report["gate_base_quantity_reference"]=quantity
            report["frozen_bitget_contract"]=spec
            report["comparison_of_venue_quantity_unit"]={
                "gate":"contract_count times frozen quanto_multiplier",
                "bitget":"base currency amount",
                "same_base_amount_asserted":bool(quantity["strict_base_amount_equal"]),
            }
            paths={};counts={}
            for kind in SCHEMAS:
                path=root/(kind+".csv")
                print("BITGET_4B_DOWNLOAD",symbol,day,kind,flush=True)
                counts[kind]=safe_gate_csv(
                    kind,day,end_us,path,
                    90_000_000 if minutes==1430 else 6_000_000,symbol=symbol,
                    historical_tick=float(spec["hbt_tick_size"]),
                    # Exchange prints can be partial fills below the order step.
                    historical_lot=None,
                    source_exchange="bitget-futures")
                paths[kind]=path
            report["feed_stats"]=counts
            lossless=source_l2_grid_check(
                paths["incremental_book_L2"],
                float(spec["hbt_tick_size"]),float(spec["legal_quote_tick"]))
            report["lossless_raw_L2_check"]=lossless
            if (symbol=="DOGEUSDT" and day=="2026-08-01"
                    and lossless["off_legal_quote_grid_rows"]<1):
                raise ValueError("DOGE August known six-digit source L2 did not survive extraction")
            risk_events=source_risk_events(
                paths["incremental_book_L2"],counts["incremental_book_L2"]["l2_format"])
            report["posthoc_window_source"]={
                "l2_format":risk_events["snapshot_mode"],
                "extra_native_snapshots":risk_events["native_extra_snapshot_count"],
                "l2_gap_gt_1000ms":risk_events["l2_gap_count"],
                "reconnect_from_normalized_csv_only":True}
            bt_local,bt_mid=read_binance_bbo(day,end_us,symbol=symbol)
            visible=visible_timestamps(bt_local,BINANCE_DELAY_MS)
            ot,om=load_gate_mid(paths["book_ticker"])  # Bitget BBO as target venue
            grid,basis,sigma,coverage=exact_moments_5s(
                visible,bt_mid,ot,om,trade_start_us*1000,trade_stop_us*1000)
            report["signal_baseline_coverage_min"]=float(np.min(coverage))
            pretrade_target=om[ot<trade_start_us*1000]
            pretrade_binance=bt_mid[visible<trade_start_us*1000]
            if not len(pretrade_target) or not len(pretrade_binance):
                raise ValueError("Missing pretrade cross-venue price reference")
            ratio=float(np.median(pretrade_binance)/np.median(pretrade_target))
            if not 0.9<ratio<1.1:
                raise ValueError(f"Binance/Bitget inconsistent base-price units: {ratio}")
            report["pretrade_binance_to_bitget_price_ratio"]=ratio
            if minutes==1430:
                parts,segments=convert_fuse_parts(
                    paths,counts,root,day_us,end_us,60,
                    float(spec["hbt_tick_size"]),float(spec["bitget_lot_size_base"]))
                report["segmented_parts"]=segments
                report["hbt_converted_events"]=sum(x["hbt_events"] for x in segments)
            else:
                converted=root/"fused.npz"
                hbt_events=tardis.convert_fuse(
                    str(paths["trades"]),str(paths["incremental_book_L2"]),
                    str(paths["book_ticker"]),
                    tick_size=float(spec["hbt_tick_size"]),
                    lot_size=float(spec["bitget_lot_size_base"]),
                    output_filename=str(converted),
                    ss_buffer_size=max(1000,counts["incremental_book_L2"]["snapshot_rows"]+100),
                    snapshot_mode="process")
                validate_event_order(hbt_events)
                report["hbt_converted_events"]=int(len(hbt_events))
                if len(hbt_events)<100:
                    raise ValueError("No official HBT events converted")
                # The August DOGE smoke must preserve exact historical off-grid prices.
                if symbol=="DOGEUSDT":
                    sample_off=float(lossless["first_20_off_legal_prices_raw_string"][0])
                    if "px" not in (hbt_events.dtype.names or ()):
                        raise ValueError("Official HBT event array lacks lossless px field")
                    if not np.any(np.abs(hbt_events["px"]-sample_off)
                                  <float(spec["hbt_tick_size"])*.01):
                        raise ValueError("Original six-digit DOGE L2 price not preserved")
                del hbt_events
                parts=[converted]
            low,high=float(lossless["price_min"]),float(lossless["price_max"])
            gamma_code=RISK_AVERSION_PER_USDT*float(spec["legal_quote_tick"])
            q=quantity["bitget_order_qty_base"]
            cap=quantity["bitget_max_pos_base"]
            clocks_local,clocks_exchange=load_gate_bbo_update_clocks(paths["book_ticker"])
            options={
                "min_requote_ns":0,
                "fit_mode":"physical_1tick_poisson",
                "wait_book_after_fill":True,
                "post_only_clamp_mode":"unified",
                "cap_depth_at_visible_levels":False,
                "quote_tick_mult":int(spec["quote_tick_mult"]),
                "cross_timestamp_tie_mode":"microsecond",
                "gate_book_local_ns":clocks_local,
                "gate_book_exchange_ns":clocks_exchange,
                "depth_is_20_level_snapshot":False,
                "forced_start_ns":0,
            }
            # The calibration witness has NO orders because cap=0;
            # the fit parameters still use the actual intended order size q.
            witness=ROIVectorMarketDepthBacktest([hbt_asset(
                parts,spec,low,high,config["maker_rate"],config["taker_rate"])])
            try:
                probe_diag=np.zeros(11,np.float64)
                raw_probe=run_core_glft(witness,visible,bt_mid,minutes*600,
                    0.,grid,basis,sigma,0,gamma_code,
                    float(spec["hbt_tick_size"]),1.,q,0.,None,
                    trade_stop_us*1000 if minutes==1430 else 0,
                    diagnostics=probe_diag,**options)
                if probe_diag[5]!=1 or probe_diag[3]<0:
                    raise ValueError("No valid first physical GLFT calibration: no model can enter")
                shared_start=int(raw_probe[-3])+int(probe_diag[3])
            finally:
                witness.close()
            options["forced_start_ns"]=shared_start
            report["core_options"]={k:v for k,v in options.items()
                if k not in ("gate_book_local_ns","gate_book_exchange_ns")}
            report["shared_first_eligible_entry_ns"]=shared_start
            report["scenario"]={
                "tick_size":spec["hbt_tick_size"],"quote_tick_mult":spec["quote_tick_mult"],
                "legal_quote_tick":spec["legal_quote_tick"],
                "ctval":1.,"qty":q,"position_cap":cap,"gamma_code":gamma_code,
                "maker_fee_rate":config["maker_rate"],"taker_fee_rate":config["taker_rate"],
                "hbt_queue_model":"power_prob_queue_model(2.0)",
                "hbt_exchange_model":"no_partial_fill_exchange",
                "hbt_constant_order_latency_ns":1_000_000,
            }
            for name,beta,mode in MODELS:
                print("BITGET_4B_MODEL_START",symbol,day,name,flush=True)
                engine=ROIVectorMarketDepthBacktest([hbt_asset(
                    parts,spec,low,high,config["maker_rate"],config["taker_rate"])])
                recorder=Recorder(engine.num_assets,minutes*600+10)
                try:
                    d=np.zeros(12,np.float64)
                    raw=run_core_glft(engine,visible,bt_mid,minutes*600,
                        beta,grid,basis,sigma,mode,gamma_code,
                        float(spec["hbt_tick_size"]),1.,q,cap,
                        recorder.recorder,
                        trade_stop_us*1000 if minutes==1430 else 0,
                        diagnostics=d,**options)
                    if d[5]!=1:
                        raise ValueError("Shared GLFT calibration invalid")
                    first_fit=int(raw[-3])+int(d[3])
                    if first_fit!=shared_start:
                        raise ValueError("Cross-model initial_fit_ns disagree")
                    summary=summarize(raw,minutes,True,beta,cap,q,
                                      tail_tolerance_samples=(20 if minutes==1430 else 10))
                    if (minutes==1430
                            and summary["end_time_ns"] < trade_stop_us*1000-1_000_000_000):
                        raise ValueError("Full-day official HBT ends >1s before UTC midnight")
                    records=recorder.get(0)
                    effective_begin=max(int(raw[-3])+600*100_000_000,
                                        int(options["forced_start_ns"]))
                    expected=max(0,(int(raw[-2][0])-effective_begin)//100_000_000)
                    if len(records)<max(0,expected-20):
                        raise ValueError("Missing official active-session Recorder samples")
                    if np.any(np.diff(records["timestamp"])<=0):
                        raise ValueError("Nonmonotonic official Recorder time")
                    trading=summary["trading"]
                    # HBT linear_asset(1): positions are already base coin.
                    for old,new in (("end_position_contracts","end_position_base"),
                                    ("max_abs_position_contracts","max_abs_position_base"),
                                    ("mean_abs_inventory_contracts","mean_abs_inventory_base"),
                                    ("rms_inventory_contracts","rms_inventory_base")):
                        if old in trading:
                            trading[new]=trading.pop(old)
                    trading["position_std_base"]=float(np.std(records["position"]))
                    trading["position_mean_base"]=float(np.mean(records["position"]))
                    trading["pos_std"]=trading["position_std_base"]
                    trading["pos_std_over_order_qty"]=trading["pos_std"]/q
                    active_duration=max(len(records),1)*.1
                    trading["mean_abs_inventory_base"]=float(raw[23])/active_duration
                    trading["rms_inventory_base"]=math.sqrt(max(0.,float(raw[24]))/active_duration)
                    trading["active_inventory_samples"]=len(records)
                    trading["inventory_observation_scope"]="post_forced_entry_only"
                    turnover=float(trading["trading_value_usdt"])
                    net=float(trading["end_net_marked_equity_usdt"])
                    fee=float(trading["fee_usdt"])
                    gross=net+fee
                    trading["gross_marked_pnl_usdt"]=gross
                    trading["gross_bps"]=10000*gross/turnover if turnover>0 else None
                    trading["gross_marked_pnl_per_turnover_bps"]=(
                        10000*gross/turnover if turnover>0 else None)
                    trading["net_marked_pnl_per_turnover_bps"]=(
                        10000*net/turnover if turnover>0 else None)
                    if abs(fee-0.0002*turnover)>1e-6:
                        raise ValueError("2bps maker fee ledger/turnover identity failed")
                    if trading["max_abs_position_base"]>cap+q+1e-9:
                        raise ValueError("Position cap exceeded")
                    summary["max_ops_per_sec"]=int(d[0])
                    summary["observed_same_microsecond_price_change_pairs"]=int(d[11])
                    summary["microsecond_collisions"]=int(d[11])
                    summary["binance_origin_filter"]={
                        k:int(v) for k,v in zip(
                            ("binance_price_update_groups","eligible_gap_checks",
                             "above_threshold_checks","directional_dominance_checks",
                             "binance_origin_activations","active_decisions",
                             "same_timestamp_ties_blocked"),raw[-1])}
                    summary["resting_time_ratio"]={
                        "bid":float(d[1]),"ask":float(d[2]),
                        "verified_exchange_resting":False,
                        "definition":"locally cancellable order presence, not actual exchange resting"}
                    summary["initial_fit_ns"]=first_fit
                    summary["calibration_wait_ns"]=int(d[4])
                    summary["quote_mode"]=name
                    summary["trading"]["end_mid_usdt_per_base"]=(
                        trading.pop("end_mid_usdt_per_btc"))
                    from analyze_gate_1645_fill_quality import model_day
                    fill={"exchange_timestamp_ns":raw[-5],
                          "side":raw[-4],
                          "local_observed_ns":raw[-2][1],
                          "exec_price":raw[-2][2],
                          "exec_qty":raw[-2][3]}
                    _,quality=model_day(name,fill,{"trading":trading},
                                        ot,om,trade_stop_us*1000,1.)
                    summary["quote_quality"]=quality
                    summary["posthoc_slices"]=posthoc_slices(
                        fill,ot,om,trade_start_us*1000,trade_stop_us*1000,risk_events)
                    report["models"][name]=summary
                    # Standalone smoke evidence: three explicit official
                    # fill arrays, no unrealistically invented executions.
                    fill_dest=out/f"{symbol}_{day}_{name}_fills.npz"
                    np.savez_compressed(fill_dest,**fill)
                    print("BITGET_4B_MODEL_OK",symbol,name,
                          "filled",trading["engine_fills"],
                          "net",net,"fee",fee,"quote_tick_mult",
                          spec["quote_tick_mult"],flush=True)
                finally:
                    engine.close()
            report["status"]="complete"
    except Exception as exc:
        report["status"]="failed"
        report["error"]=f"{type(exc).__name__}: {exc}"
        raise
    finally:
        output.write_text(json.dumps(report,ensure_ascii=False,indent=2)+"\n",
                          encoding="utf-8")
        print("BITGET_4B_REPORT",symbol,output,report["status"],flush=True)
    return report

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--symbol",required=True,choices=("BTCUSDT","ETHUSDT","SOLUSDT","DOGEUSDT"))
    parser.add_argument("--day",default="2026-08-01")
    parser.add_argument("--minutes",type=int,default=50,choices=(50,1430))
    parser.add_argument("--config",default="configs/bitget_core_4b_smoke.json")
    parser.add_argument("--out",default="reports/bitget_4b_smoke")
    args=parser.parse_args()
    run(args.day,args.symbol,args.out,args.config,args.minutes)
