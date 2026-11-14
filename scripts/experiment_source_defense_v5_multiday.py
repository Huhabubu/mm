"""V5 multiday source defense: six monthly free Tardis first-of-month dates.

Models per MARKET×DAY: source-blind G0, G0 with external timestamp-only clock
audit, fixed Z1 cancel+P50 queue shield, same + strictly causal pretrade
Binance P80 net-aggressor amount, local OKX-move placebo, FIVE source-blind
Random-Pause G0 seeds trained on separate 2026-05-01 market.

All matching, latency, queue state and fills are from official HBT 2.4.4.
Never write raw tape to repository; per-fill NPZ only temporary CI artifacts.
Net marked PnL includes maker 2bps. No causal Selection Alpha claims.
"""
import argparse
import hashlib
import json
import math
from datetime import datetime,timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
from hftbacktest import Recorder, ROIVectorMarketDepthBacktest
from hftbacktest.data.utils import tardis
from hftbacktest.data.validation import validate_event_order

from check_okx_hft_data import SCHEMAS, extract_window, require_unshifted_target_clock
from cross_venue_event_probe import read_binance_bbo,visible_timestamps
from run_beta_one_direct_gap import causal_60s_basis,read_local_bbo
from run_multivenue_glft import MARKETS,build_asset,order_scenario,validate_record
from experiment_source_defense_robust_admission import contract
from experiment_source_defense_signal import past_grid_time_ema
from experiment_source_defense_trade_flow import read_binance_taker_trades
from experiment_source_defense_engine import run_sparse_glft
from experiment_source_defense_v4_pilot import annotate_fills,research_summary

MONTH_DAYS=("2026-06-01","2026-07-01","2026-08-01",
            "2026-09-01","2026-10-01","2026-11-01")
MARKET_KEYS=(("SOL","okex-swap"),("DOGE","okex-swap"),
             ("1000PEPE","okex-swap"),("1000PEPE","bitget-futures"))
PAUSE_SEEDS=(11,29,47,73,101)
HOLD_NS=75_000_000
SLOT_NS=50_000_000
DELAY_MS=2.5


def market(coin,venue):
    if (coin,venue) not in MARKET_KEYS:
        raise ValueError("Only preregistered admitted market pairs")
    if coin=="SOL":
        spec=MARKETS[("okx","SOL")]
    else:
        spec,_=contract(coin,venue)  # WARNING current live spec, no historical archive.
    return spec,("1000PEPEUSDT" if coin=="1000PEPE" else coin+"USDT"), (
        1000. if coin=="1000PEPE" else 1.)


def prior_from_stored_admission(source,coin,venue,day):
    if coin=="1000PEPE":
        return np.nan,{"fit_kind":"original_5levels_10min","earlier_k_source_day":None}
    data=json.loads(Path(source).read_text(encoding="utf8"))
    group=data["groups"][coin+"_"+venue]
    record=next((x for x in group["date_results"] if x["day"]==day),None)
    if record is None or not record["robust_20min_valid"]:
        raise ValueError("Cannot trade without earlier valid Step3 research 20min GLFT admission")
    fit=record["robust_20min_fit"]
    levels=record["n_levels20"]
    if levels<5:
        dated=fit.get("prior_source_day")
        k=fit.get("prior_k_per_tick")
        if not dated or dated>=day or k is None or not .01<=float(k)<=5.:
            raise ValueError("Three/four-level research fit needs strictly earlier dated k")
        return float(k),{"fit_kind":"3or4level_prior_regularized_20min",
                         "earlier_k_source_day":dated,
                         "earlier_k_per_tick":float(k),
                         "positive_20min_bins":levels}
    return np.nan,{"fit_kind":"5pluslevel_research_20min",
                   "earlier_k_source_day":None,
                   "positive_20min_bins":levels}


def prepared(day,coin,venue,root,extra_delay_ms=DELAY_MS,with_trades=True):
    spec,symbol,divisor=market(coin,venue)
    pretrade_s=1200 if coin in ("SOL","DOGE") else 600
    day_us=int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp()*1_000_000)
    end_us=day_us+(pretrade_s+3005)*1_000_000
    target={}
    for kind in SCHEMAS:
        path=root/(kind+".csv")
        target[kind]=extract_window(kind,day,end_us,path,
                   exchange=spec.exchange,symbol=spec.symbol,max_rows=8_000_000)
    require_unshifted_target_clock(target)
    tape=root/"fused.npz"
    events=tardis.convert_fuse(str(root/"trades.csv"),
             str(root/"incremental_book_L2.csv"),str(root/"book_ticker.csv"),
             tick_size=spec.tick,lot_size=spec.lot,output_filename=str(tape),
             ss_buffer_size=max(1200,target["incremental_book_L2"]["snapshot_rows"]+100),
             snapshot_mode="process")
    validate_event_order(events)
    del events
    ot,om=read_local_bbo(root/"book_ticker.csv")
    if not len(om) or not np.all(np.isfinite(om)) or not np.all(om>0):
        raise ValueError("Missing/invalid native target BBO")
    b0,bmraw=read_binance_bbo(day,end_us,symbol=symbol)
    bt=visible_timestamps(b0,extra_delay_ms)
    bm=bmraw/divisor
    bt0=np.empty(0,dtype=np.int64)
    bm0=np.empty(0,dtype=np.float64)
    basis_t,basis=causal_60s_basis(ot,om,bt,bm)
    ema_t,ema=past_grid_time_ema(ot,om,bt,bm,1.)
    cutoff_ns=(day_us+pretrade_s*1_000_000)*1000
    if not np.any(np.isfinite(ema[ema_t<cutoff_ns])):
        raise ValueError("Missing pretrade causal EMA signal")
    mid=float(np.median(om[ot<cutoff_ns]))
    scenario=order_scenario(spec,mid)
    if with_trades:
        tt,ss,vv,qa=read_binance_taker_trades(
            day,end_us,symbol,extra_delay_ms,max_rows=8_000_000)
    else:
        tt=np.empty(0,np.int64)
        ss=vv=np.array([0.])
        qa={"trade_rows":0,"not_downloaded_for_source_blind_may_training":True}
    return {
        "spec":spec,"symbol":symbol,"divisor":divisor,
        "start_ns":day_us*1000,
        "start_us":day_us,"pretrade_s":pretrade_s,
        "trade_start_utc":"00:20" if pretrade_s==1200 else "00:10",
        "trade_end_utc":"01:10" if pretrade_s==1200 else "01:00",
        "tape":tape,"low":float(np.min(om)),"high":float(np.max(om)),
        "ot":ot,"om":om,"bt":bt,"bm":bm,"bt0":bt0,"bm0":bm0,
        "basis_t":basis_t,"basis":basis,"ema_t":ema_t,"ema":ema,
        "tt":tt,"ss":ss,"vv":vv,"trade_qa":qa,
        "scenario":scenario,"data_counts":target}


def fixed_model_options(name,may_calibration):
    opts={"sparse_mode":0}
    external_clock=True
    if name=="g0":
        external_clock=False
    elif name=="g0_source_clock":
        external_clock=True
    elif name in ("z1_cancel_qs","z1_cancel_qs_tf_p80","okx_local_placebo"):
        opts={
            "sparse_mode":3 if name=="okx_local_placebo" else 2,
            "adaptive_z_gap":1.5,"adaptive_z_imp":1.0,
            "ema_grid_ns":None,"ema_values":None,
            "widen_bps":.5,
            "defense_hold_ns":HOLD_NS,"defense_cooldown_ns":HOLD_NS,
            "impulse_window_ns":100_000_000,
            "use_queue_shield":True,"queue_percentile":.5,
            "use_trade_flow":name.endswith("tf_p80"),
            "trade_intensity_quantile":.8 if name.endswith("tf_p80") else 0.
        }
    elif name.startswith("pause_seed_"):
        if may_calibration is None:
            raise ValueError("Random-Pause requires a May-only precommitted calibration")
        seed=int(name.split("_")[-1])
        if seed not in PAUSE_SEEDS:raise ValueError("Unknown fixed pause seed")
        opts={
            "sparse_mode":4,
            "random_pause_probability":may_calibration["per_slot_probability"],
            "random_pause_hold_ns":may_calibration["mean_episode_duration_ns"],
            "random_pause_slot_ns":SLOT_NS,
            "random_pause_seed":seed,
            "random_pause_ask_probability":may_calibration["ask_duration_fraction"],
            "defense_cooldown_ns":0
        }
        external_clock=False
    else:
        raise ValueError("Unregistered experimental mode "+name)
    return opts,external_clock


def fixed_variants(purpose,may_calibration):
    if purpose=="calibrate":
        return ("g0","z1_cancel_qs")
    return ("g0","g0_source_clock",
            "z1_cancel_qs","z1_cancel_qs_tf_p80","okx_local_placebo") + (
                tuple("pause_seed_"+str(z) for z in PAUSE_SEEDS)
                if may_calibration is not None else ())


def execute_one(data,coin,venue,day,opts,source_clock=True,prior_k=np.nan):
    spec=data["spec"]
    train_steps=data["pretrade_s"]*10
    e=ROIVectorMarketDepthBacktest([build_asset(
        data["tape"],data["low"],data["high"],spec)])
    recorder=Recorder(e.num_assets,30000+10)
    try:
        kw={
            "rolling":True,"gamma_code":data["scenario"]["gamma_code"],
            "read_binance":source_clock,"beta":0.,
            "okx_native_ns":data["ot"],"okx_native_mid":data["om"],
            "basis_grid_ns":data["basis_t"],"basis_values":data["basis"],
            "ema_grid_ns":data["ema_t"],"ema_values":data["ema"],
            "tick_size":spec.tick,"contract_value":spec.ctval,
            "order_qty":data["scenario"]["order_qty"],
            "position_cap":data["scenario"]["position_cap"],
            "recorder":recorder.recorder,
            "train_steps":train_steps,"robust_fit":coin in ("SOL","DOGE"),
            "earlier_day_prior_k":prior_k,
            "trade_visible_ns":data["tt"],
            "trade_signed_prefix":data["ss"],
            "trade_total_prefix":data["vv"],
            **opts}
        if opts.get("ema_grid_ns") is None:
            kw["ema_grid_ns"]=data["ema_t"]
            kw["ema_values"]=data["ema"]
        bt=data["bt"] if source_clock else data["bt0"]
        bm=data["bm"] if source_clock else data["bm0"]
        raw=run_sparse_glft(e,bt,bm,30000,**kw)
        if int(raw[5])!=1:
            raise RuntimeError("No identified GLFT parameters in pretrade window")
        result=research_summary(raw,50,data["scenario"],train_steps)
        validate_record(recorder.get(0),result)
        if result["samples"]<train_steps+30000-5:
            raise RuntimeError("Truncated HBT 50min replay")
        tr=result["trading"]
        if abs(tr["fee_usdt"]-.0002*tr["trading_value_usdt"])>max(1e-6,tr["trading_value_usdt"]*1e-9):
            raise RuntimeError("Non-maker fee notional in HBT ledger")
        fills=annotate_fills(raw,data["ot"],data["om"],spec.tick,spec.ctval)
        trace=json.dumps(result["calibration_trace"],sort_keys=True).encode("utf8")
        model={
            "status":"complete",
            "initial_parameters":result["initial_parameters"],
            "calibration_trace_sha256":hashlib.sha256(trace).hexdigest(),
            "trading":tr,
            "thresholds":{
                "sigma_100ms_bps":float(raw[36][0]),
                "gap_bps":float(raw[36][1]),
                "impulse_bps":float(raw[36][2]),
                "floor_bps":float(raw[36][3]),
                "best_bid_qty_p50":float(raw[36][4]) if np.isfinite(raw[36][4]) else None,
                "best_ask_qty_p50":float(raw[36][5]) if np.isfinite(raw[36][5]) else None,
                "binance_buy_net_P80_usdt":float(raw[36][6]) if np.isfinite(raw[36][6]) else None,
                "binance_sell_net_P80_usdt":float(raw[36][7]) if np.isfinite(raw[36][7]) else None},
            "counters":raw[35].tolist(),
            "defense_duty":{
                "active_ns":float(raw[37][0]),"episodes":int(raw[37][1]),
                "active_fraction_50min":float(raw[37][2]),
                "mean_episode_duration_ns":float(raw[37][3]),
                "bid_active_ns":float(raw[37][4]),
                "ask_active_ns":float(raw[37][5]),
                "bid_episodes":int(raw[37][6]),
                "ask_episodes":int(raw[37][7])},
            "fee_scenarios": {
                "maker_2bps_net_usdt":tr["end_net_marked_equity_usdt"],
                "maker_0bps_net_usdt_diagnostic":tr["end_net_marked_equity_usdt"]+tr["fee_usdt"],
                "maker_minus0p2bps_net_usdt":tr["end_net_marked_equity_usdt"]+
                           tr["fee_usdt"]+0.00002*tr["trading_value_usdt"]}
        }
        return model,fills
    finally:
        e.close()


def calibration_from_may(row):
    tr=row["trading"]
    duty=row["defense_duty"]
    active=duty["active_fraction_50min"]
    episodes=duty["episodes"]
    if not (0<active<.5 and episodes>=10):
        raise ValueError("May source defense activity cannot identify bounded random-pause fraction")
    mean=int(round(duty["mean_episode_duration_ns"]/10_000_000))*10_000_000
    if not 10_000_000<=mean<=500_000_000:
        raise ValueError("May mean protection episode outside predeclared range")
    # Simple fixed-grid renewal approximation with zero pause cooldown:
    # duty ≈ hold/(hold + slot/p). This is NEVER calibrated with June+ PnL/fills.
    probability=min(.9,max(.001,active*SLOT_NS/(mean*(1.-active))))
    side_total=duty["bid_active_ns"]+duty["ask_active_ns"]
    return {
        "training_day":"2026-05-01",
        "training_models":"source defense z1_cancel_qs, no June/other test data",
        "training_trade_window_utc":"00:20–01:10" if tr["engine_fills"]>=0 else None,
        "pretrade_defense_occupancy_fraction":active,
        "pretrade_episode_count":episodes,
        "mean_episode_duration_ns":mean,
        "ask_duration_fraction":duty["ask_active_ns"]/side_total,
        "per_slot_probability":probability,
        "slot_ns":SLOT_NS,
        "fixed_seeds":list(PAUSE_SEEDS),
        "training_fills":tr["engine_fills"],
        "fixed_from_earlier_day_only":True,
        "random_G0_makes_no_Binance_price_or_direction_reads":True,
        "expected_occupancy_not_forced_to_match_each_test_day":True}


def run(day,coin,venue,purpose,output,admission_json=None,may_pause_json=None):
    if (coin,venue) not in MARKET_KEYS:raise ValueError("Unregistered market")
    if purpose=="calibrate":
        if day!="2026-05-01":raise ValueError("May-only training")
    elif purpose=="day":
        if day not in MONTH_DAYS:raise ValueError("Only six prespecified monthly dates")
        if not admission_json:raise ValueError("Prior-date admission record required")
    else:raise ValueError("Unknown purpose")
    out=Path(output);out.mkdir(parents=True,exist_ok=True)
    if purpose=="day":
        k,prior_meta=prior_from_stored_admission(admission_json,coin,venue,day)
        may=None
        if may_pause_json and Path(may_pause_json).exists():
            archived=json.loads(Path(may_pause_json).read_text(encoding="utf8"))
            if archived.get("status")!="complete" or archived.get("coin")!=coin or archived.get("venue")!=venue:
                raise ValueError("May pause calibration mismatch")
            may=archived["random_pause_preset"]
    else:
        k=np.nan
        prior_meta={"fit_kind":"May pretraining only; 20min at own history, no earlier k available"}
        may=None
    with TemporaryDirectory(prefix=f"v5_{coin}_{venue}_{day}_") as temp:
        data=prepared(day,coin,venue,Path(temp),with_trades=(purpose=="day"))
        report={
            "status":"complete","day":day,"coin":coin,"venue":venue,
            "purpose":purpose,"market":data["spec"].symbol,
            "historical_contract_specs_verified":False,
            "tick":data["spec"].tick,"lot":data["spec"].lot,
            "contract_value_in_target_base_coin":data["spec"].ctval,
            "binance_price_divisor":data["divisor"],
            "pretrade_minutes":data["pretrade_s"]/60,
            "trade_window_utc":data["trade_start_utc"]+"–"+data["trade_end_utc"],
            "trading_fee_maker_bps":2.,
            "official_hftbacktest_version":"2.4.4",
            "queue_model":"power_prob_queue_model(2.0), no_partial_fill",
            "binance_extra_delay_ms":DELAY_MS,
            "order_request_delay_ms":1.,
            "order_response_delay_ms":1.,
            "order_scenario":data["scenario"],
            "binance_taker_trade_qa":data["trade_qa"],
            "prior_status":prior_meta,
            "may_random_pause_preset":may,
            "variants":{}}
        candidate_trace=None
        for name in fixed_variants(purpose,may):
            opts,clock=fixed_model_options(name,may)
            model,fills=execute_one(data,coin,venue,day,opts,clock,k)
            if candidate_trace is None:candidate_trace=model["calibration_trace_sha256"]
            if model["calibration_trace_sha256"]!=candidate_trace:
                raise RuntimeError("Comparators have different GLFT refit histories")
            report["variants"][name]=model
            # Fill diagnostics may go into CI artifact; NEVER commit to Git.
            np.savez_compressed(out/(name+"_fills.npz"),**fills)
            print("V5",day,coin,venue,name,"FILLS",
                  model["trading"]["engine_fills"],"CANCEL",
                  model["trading"]["cancel_requests"],"NET_AFTER_FEE",
                  model["trading"]["end_net_marked_equity_usdt"],
                  "ACTIVE",model["defense_duty"]["active_fraction_50min"],
                  "T_P80",model["thresholds"]["binance_buy_net_P80_usdt"],
                  model["thresholds"]["binance_sell_net_P80_usdt"],
                  flush=True)
        report["all_models_glft_fit_identical"]=True
        if purpose=="calibrate":
            report["random_pause_preset"]=calibration_from_may(
                report["variants"]["z1_cancel_qs"])
        else:
            a=report["variants"]["z1_cancel_qs"]
            b=report["variants"]["z1_cancel_qs_tf_p80"]
            report["high_intensity_P80_effect_check"]={
                "queue_only_fills":a["trading"]["engine_fills"],
                "queue_plus_high_flow_fills":b["trading"]["engine_fills"],
                "high_flow_filtered_gate_count":b["counters"][14],
                "high_flow_accepted_gate_count":b["counters"][15],
                "source_strategy_paused_fraction":a["defense_duty"]["active_fraction_50min"],
                "random_pause_seed_count":len([x for x in report["variants"] if x.startswith("pause_seed")]),
                "result_identical_to_queue_only":
                    (a["trading"]["engine_fills"]==b["trading"]["engine_fills"] and
                     a["trading"]["end_net_marked_equity_usdt"]==
                     b["trading"]["end_net_marked_equity_usdt"])}
        with (out/"case.json").open("w",encoding="utf8") as w:
            json.dump(report,w,ensure_ascii=False,indent=2,allow_nan=False)
        print("V5_CASE_SUMMARY",out/"case.json",flush=True)
        return report


if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--day",required=True)
    p.add_argument("--coin",required=True)
    p.add_argument("--venue",required=True)
    p.add_argument("--purpose",choices=("calibrate","day"),required=True)
    p.add_argument("--out",required=True)
    p.add_argument("--admission-json",default=None)
    p.add_argument("--may-pause-json",default=None)
    q=p.parse_args()
    run(q.day,q.coin,q.venue,q.purpose,q.out,q.admission_json,q.may_pause_json)
