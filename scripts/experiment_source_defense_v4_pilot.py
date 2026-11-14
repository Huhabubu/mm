"""2026-06-01 three-coin adaptive sparse defense: official HBT, ablation first.

BTC and 1000PEPE: original 10min G0 calibration + next 50min.
SOL: research 20min 3+ level estimator + next 50min, ALSO for G0.
Two fixed volatility-z combinations crossed with cancel/widen, and filters
none / HBT Queue Shield / Binance taker flow / both. No dense grid search.
All net reported values include maker 2bps (no invented rebates).
"""
import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
from hftbacktest import Recorder, ROIVectorMarketDepthBacktest
from hftbacktest.data.utils import tardis
from hftbacktest.data.validation import validate_event_order

from check_okx_hft_data import SCHEMAS, extract_window, require_unshifted_target_clock
from cross_venue_event_probe import read_binance_bbo, visible_timestamps
from run_beta_one_direct_gap import causal_60s_basis, read_local_bbo
from run_multivenue_glft import MARKETS, build_asset, order_scenario, validate_record
from experiment_source_defense_robust_admission import contract
from experiment_source_defense_engine import run_sparse_glft, summarize as engine_summarize
from experiment_source_defense_signal import past_grid_time_ema
from experiment_source_defense_trade_flow import read_binance_taker_trades
from experiment_source_defense_pilot import annotate_fills, classify_opportunities
from experiment_source_defense_step1_diagnostic import fill_group_summary
from experiment_source_defense_aggregate import block_ci

COINS=("BTC","SOL","1000PEPE")
DAY="2026-06-01"
EXTRA_DELAY_MS=2.5
HORIZONS=(100,500,1000,5000)

# Fixed two illustrative z-settings; score positive only if test evidence says so.
# z multipliers act on pretrade 100ms OKX mid-price return standard deviation.
PRESET=(
    ("z1",1,1.5,1.0,.5,75_000_000),
    ("z2",2,2.5,1.5,1.0,100_000_000),
)
FILTERS=(("none",False,False),("shield",True,False),
         ("trades",False,True),("both",True,True))
ACTIONS=(("cancel",2),("widen",1))


def matched_stats(base,test,ctval):
    """G0-only vs shared: observational pair, not exact original order IDs."""
    dropped,kept=classify_opportunities(base,test)
    added,_=classify_opportunities(test,base)
    out={"Added":fill_group_summary(test,added,ctval),
         "Dropped":fill_group_summary(base,dropped,ctval),
         "Kept":fill_group_summary(base,kept,ctval),
         "selectivity":{}}
    for h in HORIZONS:
        a=out["Dropped"]["by_horizon"][f"{h}ms"]
        b=out["Kept"]["by_horizon"][f"{h}ms"]
        lift=(b["mean_markout_bps"]-a["mean_markout_bps"]
              if a["mean_markout_bps"] is not None and
                 b["mean_markout_bps"] is not None else None)
        tail=(b["p10_markout_bps"]-a["p10_markout_bps"]
              if a["p10_markout_bps"] is not None and
                 b["p10_markout_bps"] is not None else None)
        out["selectivity"][f"{h}ms"]={
            "lift_mean_bps":lift,"p10_kept_minus_dropped_bps":tail,
            "within_one_day_block_CI":block_ci(base,test,h)}
    return out


def research_summary(raw,minutes,scenario,train_steps):
    s=engine_summarize(raw,minutes,True,0.,scenario["position_cap"],
                       scenario["order_qty"])
    # The frozen summary's inventory denominator assumes TRAIN=6000.
    # Correct only this research-run reporting when pretrade is 12000.
    if train_steps!=6000:
        trading_seconds=(raw[0]-train_steps)*.1
        if trading_seconds<=0:raise ValueError("No SOL aligned trading samples")
        s["trading"]["mean_abs_inventory_contracts"]=raw[23]/trading_seconds
        s["trading"]["rms_inventory_contracts"]=math.sqrt(raw[24]/trading_seconds)
    s["research_calibration"]={
        "pretrade_minutes":train_steps/600,
        "trade_start_UTC":"00:20" if train_steps==12000 else "00:10",
        "trade_end_UTC":"01:10" if train_steps==12000 else "01:00",
        "fit_kind":"causal_3plus_level_prior_conditional" if train_steps==12000
                   else "original_5plus_level",
        "early_date_prior_k":None,
        "no_prior_needed_if_initial_fit_5plus_levels":True}
    return s


def scenarios():
    xs=[("g0",None,None,None,None,None,None)]
    for ident,half,z_gap,z_imp,width,hold in PRESET:
        for action,mode in ACTIONS:
            for label,shield,trades in FILTERS:
                xs.append((f"{ident}_{action}_{label}",ident,half,z_gap,z_imp,
                           width,hold,mode,shield,trades))
    return xs


def run(coin,report_root):
    if coin not in COINS:raise ValueError("Unknown preregistered coin")
    if coin=="1000PEPE":
        spec,_=contract("1000PEPE","okex-swap")
        bin_symbol="1000PEPEUSDT"
        bn_divisor=1000.
    else:
        spec=MARKETS[("okx",coin)]
        bin_symbol=coin+"USDT"
        bn_divisor=1.
    train_steps=12000 if coin=="SOL" else 6000
    train_seconds=train_steps//10
    run_seconds=3000
    date0=int(datetime.fromisoformat(DAY).replace(
        tzinfo=timezone.utc).timestamp()*1_000_000)
    end_us=date0+(train_seconds+run_seconds+5)*1_000_000
    out=Path(report_root)/f"{DAY}_okx_{coin}"
    out.mkdir(parents=True,exist_ok=True)
    report={
        "coin":coin,"day":DAY,"venue":"okex-swap",
        "instrument":spec.symbol,"lot":spec.lot,"ctval":spec.ctval,
        "tick_size_usdt":spec.tick,"target_price_unit":"USDT/PEPE" if coin=="1000PEPE" else f"USDT/{coin}",
        "Binance_quote_divisor":bn_divisor,"pretrade_minutes":train_seconds/60,
        "test_window_utc":f"00:{train_seconds//60:02d} - 01:{(train_seconds+run_seconds)//60-60:02d}" if coin=="SOL" else "00:10 - 01:00",
        "test_start_epoch_us":date0+train_seconds*1_000_000,
        "test_end_epoch_us":date0+(train_seconds+run_seconds)*1_000_000,
        "binance_extra_delay_ms":EXTRA_DELAY_MS,"order_request_ms":1,
        "order_ack_ms":1,"maker_fee_bps":2.0,
        "fixed_adaptive_presets":[
            {"id":n,"ema_half_life_s":h,"z_gap":g,"z_impulse":i,
             "widen_bps":w,"hold_ms":hold/1.e6}
            for n,h,g,i,w,hold in PRESET],
        "filter_settings":{"queue_pretrade_percentile":.5,
                           "trade_window_ms":100,
                           "taker_notional_total_min_USDT":100.,
                           "signed_trade_notional_fraction_min":.35,
                           "gap_floor_bps":.3,
                           "gap_floor_at_least_one_tick":True},
        "queue_depth_source":"official HBT best-tick L2 quantity, pretrade P50 only",
        "Binance_trade_side":"Tardis normalized liquidity taker side",
        "scenarios":{},
        "scope_warning":"Exploratory one day, not independent sample; no match of causal selection alpha",
        "historical_contract_spec_verified":False if coin=="1000PEPE" else None}
    with TemporaryDirectory(prefix=f"source_v4_{coin}_") as folder:
        root=Path(folder)
        paths={kind:root/(kind+".csv") for kind in SCHEMAS}
        counts={}
        for kind in SCHEMAS:
            counts[kind]=extract_window(kind,DAY,end_us,paths[kind],
                            max_rows=5_000_000,exchange=spec.exchange,symbol=spec.symbol)
        require_unshifted_target_clock(counts)
        fused=root/"tape.npz"
        events=tardis.convert_fuse(
            str(paths["trades"]),str(paths["incremental_book_L2"]),
            str(paths["book_ticker"]),tick_size=spec.tick,lot_size=spec.lot,
            output_filename=str(fused),
            ss_buffer_size=max(1200,counts["incremental_book_L2"]["snapshot_rows"]+100),
            snapshot_mode="process")
        validate_event_order(events)
        del events
        ot,om=read_local_bbo(paths["book_ticker"])
        if not np.all(om>0):raise ValueError("Bad target mid")
        b0,bm_raw=read_binance_bbo(DAY,end_us,symbol=bin_symbol)
        bt=visible_timestamps(b0,EXTRA_DELAY_MS)
        bm=bm_raw/bn_divisor
        trade_t,trade_signed,trade_total,trade_qa=read_binance_taker_trades(
            DAY,end_us,bin_symbol,EXTRA_DELAY_MS,max_rows=5_000_000)
        report["binance_trade_loader_qa"]=trade_qa
        original_t,original_b=causal_60s_basis(ot,om,bt,bm)
        emas={h:past_grid_time_ema(ot,om,bt,bm,h) for h in (1,2)}
        warm_end_ns=(date0+train_seconds*1_000_000)*1000
        for h,(t,v) in emas.items():
            if not np.any(np.isfinite(v[t<warm_end_ns])):
                raise ValueError(f"EMA {h}s not ready at trading start")
        med=float(np.median(om[ot<warm_end_ns]))
        scenario=order_scenario(spec,med)
        report["order_scenario"]=scenario
        low,high=float(np.min(om)),float(np.max(om))
        annotated={}
        configs=scenarios()
        for cfg in configs:
            name=cfg[0]
            mode=cfg[7] if len(cfg)>7 else 0
            engine=ROIVectorMarketDepthBacktest([build_asset(fused,low,high,spec)])
            recorder=Recorder(engine.num_assets,run_seconds*10+10)
            try:
                kw=dict(
                    rolling=True, gamma_code=scenario["gamma_code"],
                    read_binance=True,beta=0.0,okx_native_ns=ot,
                    okx_native_mid=om,basis_grid_ns=original_t,
                    basis_values=original_b,
                    tick_size=spec.tick,contract_value=spec.ctval,
                    order_qty=scenario["order_qty"],
                    position_cap=scenario["position_cap"],
                    recorder=recorder.recorder,sparse_mode=mode,
                    # Numba compiles both branch types at G0: arrays must not
                    # be None even when sparse_mode==0 (read-only unused).
                    ema_grid_ns=emas[1][0],ema_values=emas[1][1],
                    train_steps=train_steps,
                    robust_fit=(coin=="SOL"),
                    earlier_day_prior_k=np.nan,
                    trade_visible_ns=trade_t,
                    trade_signed_prefix=trade_signed,
                    trade_total_prefix=trade_total)
                if mode:
                    _,ident,half,z_gap,z_imp,width,hold,mode,shield,trades=cfg
                    kw.update(dict(
                        ema_grid_ns=emas[half][0],ema_values=emas[half][1],
                        adaptive_z_gap=z_gap,adaptive_z_imp=z_imp,
                        widen_bps=width,
                        defense_hold_ns=hold,defense_cooldown_ns=hold,
                        impulse_window_ns=100_000_000,
                        use_queue_shield=shield,queue_percentile=.5,
                        use_trade_flow=trades,trade_signed_ratio=.35,
                        trade_min_notional_usdt=100.))
                raw=run_sparse_glft(engine,bt,bm,run_seconds*10,**kw)
                if int(raw[5])!=1:
                    raise RuntimeError("Initial G0/research GLFT fit unavailable")
                res=research_summary(raw,50,scenario,train_steps)
                validate_record(recorder.get(0),res)
                if res["samples"]<train_steps+run_seconds*10-5:
                    raise RuntimeError("Truncated 20min/50min experiment")
                tr=res["trading"]
                if abs(tr["fee_usdt"]-tr["trading_value_usdt"]*.0002)>max(
                        1e-6,abs(tr["trading_value_usdt"])*1e-9):
                    raise RuntimeError("Official maker ledger inconsistent")
                fn=annotate_fills(raw,ot,om,spec.tick,spec.ctval)
                annotated[name]=fn
                np.savez_compressed(out/(name+"_fill_diagnostics.npz"),**fn)
                report["scenarios"][name]={
                    "status":"complete","calibration":res["research_calibration"],
                    "initial_GLFT":res["initial_parameters"],
                    "rolling_fit_trace":res["calibration_trace"],
                    "trading":tr,
                    "sampled_thresholds":{
                        "sigma_100ms_bps":float(raw[36][0]),
                        "adaptive_gap_bps":float(raw[36][1]),
                        "adaptive_impulse_bps":float(raw[36][2]),
                        "predeclared_absolute_floor_bps":float(raw[36][3]),
                        "pretrade_HBT_bid_qty_p50":float(raw[36][4]) if np.isfinite(raw[36][4]) else None,
                        "pretrade_HBT_ask_qty_p50":float(raw[36][5]) if np.isfinite(raw[36][5]) else None},
                    "counters":raw[35].tolist()}
                print("V4",coin,name,"FILLS",tr["engine_fills"],"CANCELS",
                      tr["cancel_requests"],"NET_FEE_INCL",
                      tr["end_net_marked_equity_usdt"],"GATE",
                      report["scenarios"][name]["sampled_thresholds"],flush=True)
            finally:
                engine.close()
        g0=report["scenarios"]["g0"]
        base_tr=g0["trading"]
        base_trace=g0["rolling_fit_trace"]
        g0f=annotated["g0"]
        for name,z in report["scenarios"].items():
            if z["rolling_fit_trace"]!=base_trace:
                raise AssertionError(f"Non-identical G0 vs model fit trace {name}")
            if name=="g0":continue
            tr=z["trading"]
            z["matched_fill_analysis"]=matched_stats(g0f,annotated[name],spec.ctval)
            z["fill_retention"]=tr["engine_fills"]/base_tr["engine_fills"]
            a=z["matched_fill_analysis"]["selectivity"]
            z["acceptance"]={
                "retention_gte_75pct":z["fill_retention"]>=.75,
                "retention_gte_80pct":z["fill_retention"]>=.80,
                "cancel_no_more_than_1p5_G0":tr["cancel_requests"]<=1.5*base_tr["cancel_requests"],
                "lift_100ms_positive":(
                    a["100ms"]["lift_mean_bps"] is not None and a["100ms"]["lift_mean_bps"]>0),
                "lift_500ms_positive":(
                    a["500ms"]["lift_mean_bps"] is not None and a["500ms"]["lift_mean_bps"]>0),
                "p10_100ms_positive":(
                    a["100ms"]["p10_kept_minus_dropped_bps"] is not None and
                    a["100ms"]["p10_kept_minus_dropped_bps"]>0),
                "p10_500ms_positive":(
                    a["500ms"]["p10_kept_minus_dropped_bps"] is not None and
                    a["500ms"]["p10_kept_minus_dropped_bps"]>0),
                "added_500ms_avg_bps":z["matched_fill_analysis"]["Added"]["by_horizon"]["500ms"]["mean_markout_bps"],
                "no_claim_of_causal_alpha":True}
        report["G0_baseline_fills"]=base_tr["engine_fills"]
        report["G0_baseline_cancel_requests"]=base_tr["cancel_requests"]
        report["all_model_pretrade_GLFT_identical"]=True
    target=out/"summary.json"
    target.write_text(json.dumps(report,ensure_ascii=False,indent=2,allow_nan=False),encoding="utf8")
    print("V4_SAVED",target)
    return report


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--coin",required=True,choices=COINS)
    parser.add_argument("--out",default="reports/source_defense/v4")
    args=parser.parse_args()
    run(args.coin,args.out)


if __name__=="__main__":
    main()
