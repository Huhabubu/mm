"""V6 minimal official HBT mechanism replay with exact defense episode timestamps.

Mode core: G0 source-blind, fixed June-May-v5 Z1+QueueShield, +60s
precomputed episode replay (source blind at *execution*); only SOL six
days or BTC/DOGE/PEPE June. Additional SOL June gamma 1.5x and BTC
June 0ms/2.5ms high08/P80 activation clocks are small independent probes.
Raw fill traces are kept in memory only; commit compact JSON not NPZ.
"""
import argparse
import json
import math
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
from hftbacktest import Recorder,ROIVectorMarketDepthBacktest

from run_multivenue_glft import build_asset,validate_record
from run_beta_one_direct_gap import causal_60s_basis
from experiment_source_defense_signal import past_grid_time_ema
from experiment_source_defense_engine import run_sparse_glft
from experiment_source_defense_v5_multiday import (
    prepared,prior_from_stored_admission,fixed_model_options,
    MONTH_DAYS)
from experiment_source_defense_v4_pilot import annotate_fills,research_summary
from experiment_source_defense_pilot import classify_opportunities

HORIZONS=(100,500,1000)
SHIFT_NS=60_000_000_000
TAIL_NS=200_000_000
GAMMA_SCALE=1.5
EMPTY=np.empty(0,np.int64)


def clamp_shift(starts,ends,sides,stop_ns,delta_ns=SHIFT_NS):
    """Physical +60s without modulo wrap or future event leak.

    Truncates original last 60s defense episodes. Explicitly reports lost
    exposure; does NOT deceptively claim an identical full-window duty cycle.
    """
    starts=np.asarray(starts,np.int64)
    ends=np.asarray(ends,np.int64)
    sides=np.asarray(sides,np.int64)
    if not (len(starts)==len(ends)==len(sides)):
        raise ValueError("Original episode trace shape mismatch")
    if np.any(ends<=starts) or np.any(np.diff(starts)<0) or np.any(
            starts[1:]<ends[:-1]):
        raise ValueError("Episode stream invalid/overlapping")
    if np.any((sides!=-1)&(sides!=1)):
        raise ValueError("Invalid defense side")
    ns=starts+delta_ns
    ne=np.minimum(ends+delta_ns,int(stop_ns))
    ok=(ns<int(stop_ns))&(ne>ns)
    original=int(np.sum(ends-starts))
    retained=int(np.sum(ne[ok]-ns[ok]))
    return (ns[ok],ne[ok],sides[ok],
        {"original_episodes":len(starts),
         "shifted_episodes_in_window":int(ok.sum()),
         "original_active_ns":original,
         "shifted_active_ns":retained,
         "missing_from_last_60sec_active_ns":original-retained,
         "active_fraction_preserved":retained/original if original else None,
         "future_leakage_avoided_by_no_wrap":True,
         "exact_50min_full_exposure_matched":retained==original})


def asof_direct(t,side,starts,ends,sides):
    """At G0 fill exchange time, is SAME dangerous side locally protected?"""
    if not len(starts):return np.zeros(len(t),bool)
    i=np.searchsorted(starts,t,side="right")-1
    v=i>=0
    j=np.maximum(i,0)
    return v&(t<ends[j])&(side==-sides[j])


def asof_urgent_tail(t,side,urgent_t,urgent_side,tail_ns=TAIL_NS):
    if not len(urgent_t):return np.zeros(len(t),bool)
    hit=np.zeros(len(t),bool)
    for target_side in (1,-1):
        ct=urgent_t[urgent_side==target_side]
        if not len(ct):continue
        ii=np.searchsorted(ct,t,side="right")-1
        v=ii>=0
        idx=np.maximum(ii,0)
        # Only after command and within 200ms. ACK cancellation may lag.
        hit|=(v&(t>=ct[idx])&(t<ct[idx]+tail_ns)&(side==-target_side))
    return hit


def summarize_mask(fills,mask):
    out={"count":int(np.sum(mask)),
         "share_all_group_fills":float(np.mean(mask)) if len(mask) else 0.}
    for h in HORIZONS:
        fresh=mask&np.asarray(fills[f"fresh_{h}ms"],bool)
        values=np.asarray(fills[f"markout_{h}ms_bps"],float)
        m=values[fresh&np.isfinite(values)]
        spread=np.asarray(fills["spread_capture_bps"],float)[fresh&np.isfinite(values)]
        out[f"{h}ms"]={
            "fresh_n":len(m),
            "markout_mean_bps":float(m.mean()) if len(m) else None,
            "markout_p10_bps":float(np.quantile(m,.1)) if len(m) else None,
            "adverse_selection_cost_mean_bps":float(-m.mean()) if len(m) else None,
            "gross_edge_mean_bps":float(np.mean(spread+m)) if len(m) else None}
    return out


def groups(g0,new,episodes,strict_clock="exchange"):
    drop,keep=classify_opportunities(g0,new)
    add,_=classify_opportunities(new,g0)
    st,en,sd,ct,cs=episodes
    t=np.asarray(g0["exchange_timestamp_ns"] if strict_clock=="exchange"
                 else g0["local_observed_ns"],np.int64)
    side=np.asarray(g0["side_sign"],np.int64)
    in_episode=asof_direct(t,side,st,en,sd)
    in_tail=asof_urgent_tail(t,side,ct,cs)
    ga=drop&(in_episode|in_tail)
    gb=drop&~ga
    only_active=drop&in_episode
    only_tail=drop&~in_episode&in_tail
    if int(ga.sum()+gb.sum()+keep.sum())!=len(drop):
        raise AssertionError("G0 group partition inconsistent")
    return {
        "classification_clock":strict_clock,
        "exchange_fill_vs_local_action_clock_mismatch_caveat":True,
        "Group_A_active_or_200ms_post_cancel":summarize_mask(g0,ga),
        "Group_A_strictly_active":summarize_mask(g0,only_active),
        "Group_A_200ms_tail_only":summarize_mask(g0,only_tail),
        "Group_B_no_local_defense_or_cancel_tail":summarize_mask(g0,gb),
        "Kept":summarize_mask(g0,keep),
        "Added":summarize_mask(new,add),
        "Dropped_all":summarize_mask(g0,drop),
        "A_plus_B_equals_Dropped":int(ga.sum()+gb.sum())==int(drop.sum()),
        "not_exact_order_counterfactual":True,
        "not_verified_cancel_ACK_by_time_membership":True,
        "Group_B_attribution_not_proven_by_timing_alone":True}


def extra_shift_data(d):
    """Same HBT historical target tape, externally visible Binance 0ms instead of2.5."""
    z=dict(d)
    z["bt"]=d["bt"]-2_500_000
    z["tt"]=d["tt"]-2_500_000
    z["basis_t"],z["basis"]=causal_60s_basis(
        z["ot"],z["om"],z["bt"],z["bm"])
    z["ema_t"],z["ema"]=past_grid_time_ema(
        z["ot"],z["om"],z["bt"],z["bm"],1.)
    return z


def model_run(data,mode,prior_k=np.nan,gamma_scale=1.,
              source_clock=None,lagged=None):
    spec=data["spec"]
    train_steps=data["pretrade_s"]*10
    scenario=data["scenario"]
    e=ROIVectorMarketDepthBacktest([build_asset(
        data["tape"],data["low"],data["high"],spec)])
    rec=Recorder(e.num_assets,30010)
    options={
        "rolling":True,"gamma_code":scenario["gamma_code"]*gamma_scale,
        "read_binance":mode not in ("g0","shift60"),
        "beta":0.,"okx_native_ns":data["ot"],"okx_native_mid":data["om"],
        "basis_grid_ns":data["basis_t"],"basis_values":data["basis"],
        "ema_grid_ns":data["ema_t"],"ema_values":data["ema"],
        "tick_size":spec.tick,"contract_value":spec.ctval,
        "order_qty":scenario["order_qty"],
        "position_cap":scenario["position_cap"],"recorder":rec.recorder,
        "train_steps":train_steps,"robust_fit":data["spec"].coin in ("SOL","DOGE"),
        "earlier_day_prior_k":prior_k,
        "trade_visible_ns":data["tt"],
        "trade_signed_prefix":data["ss"],"trade_total_prefix":data["vv"],
        "capture_defense_episodes":True,
        "time_shift_starts_ns":EMPTY,"time_shift_ends_ns":EMPTY,
        "time_shift_sides":EMPTY}
    if mode=="g0":
        options["sparse_mode"]=0
    elif mode=="source":
        opts,_=fixed_model_options("z1_cancel_qs",None)
        opts.pop("ema_grid_ns",None)
        opts.pop("ema_values",None)
        options.update(opts)
    elif mode=="high08":
        options.update({
            "sparse_mode":2,
            "gap_gate_bps":.8,"impulse_gate_bps":.3,
            "hysteresis_exit_th_bps":.1,
            "use_queue_shield":True,"queue_percentile":.5,
            "use_trade_flow":True,"trade_intensity_quantile":.8,
            "defense_hold_ns":75_000_000,
            "defense_cooldown_ns":75_000_000})
    elif mode=="shift60":
        if lagged is None:raise ValueError("Need past source episode trace")
        options.update({"sparse_mode":5,
                        "time_shift_starts_ns":lagged[0],
                        "time_shift_ends_ns":lagged[1],
                        "time_shift_sides":lagged[2]})
    else:raise ValueError("Unsupported fixed audit mode")
    clock=mode not in ("g0","shift60") if source_clock is None else source_clock
    bt=data["bt"] if clock else data["bt0"]
    bm=data["bm"] if clock else data["bm0"]
    if not clock:
        options["read_binance"]=False
    try:
        raw=run_sparse_glft(e,bt,bm,30000,**options)
        if int(raw[5])!=1:raise ValueError("G0 initial calibration failed")
        r=research_summary(raw,50,scenario,train_steps)
        validate_record(rec.get(0),r)
        fills=annotate_fills(raw,data["ot"],data["om"],spec.tick,spec.ctval)
        trace=raw[38]
        for v in trace:
            if not isinstance(v,np.ndarray):
                raise AssertionError("Missing source episode array")
        if len(trace[0])!=len(trace[1]) or len(trace[0])!=len(trace[2]):
            raise AssertionError("Partial defense trace")
        if (abs(r["trading"]["fee_usdt"]-r["trading"]["trading_value_usdt"]*.0002)>
                max(1e-6,r["trading"]["trading_value_usdt"]*1e-9)):
            raise AssertionError("Maker ledger mismatch")
        return r,fills,trace
    finally:
        e.close()


def compact(r,episode,coin,as_of_coin):
    tr=r["trading"];st,en,side,urgent,urgent_side=episode
    return {
        "fills":tr["engine_fills"],"cancels":tr["cancel_requests"],
        "fee_included_net_usdt":tr["end_net_marked_equity_usdt"],
        "fee_usdt":tr["fee_usdt"],"turnover_usdt":tr["trading_value_usdt"],
        "terminal_inventory":tr["end_position_contracts"],
        "mean_abs_inventory":tr["mean_abs_inventory_contracts"],
        "initial_GLFT":r["initial_parameters"],
        "last_GLFT":r["last_parameters"],
        "defense_episode_count":len(st),
        "defense_duration_ms":float(np.sum(en-st)/1e6),
        "urgent_cancel_request_count":len(urgent),
        "urgent_cancel_request_by_side":{
            "bid":int(np.sum(urgent_side==-1)),
            "ask":int(np.sum(urgent_side==1))},
        "mean_live_fills_per_order":float(tr["engine_fills"]/max(1,tr["submissions"])),
        "first_8_episodes_observed_ns":[{
            "start":int(a),"end":int(b),"side":int(c)}
            for a,b,c in zip(st[:8],en[:8],side[:8])],
        "source_clock":"source-free" if as_of_coin else "Binance local+extra lag"}


def short_metrics(r,fill,episode,sourceblind):
    z=compact(r,episode,"","source-free" if sourceblind else "")
    z["fill_mechanism"] =summarize_mask(fill,np.ones(len(fill["side_sign"]),bool))
    return z


def nearest_episode_difference_ms(a,b,limit_ms=20.):
    """Match events by local time+side, nearest neighbor. Not order identity."""
    st0,en0,si0,cn0,cs0=a
    st1,en1,si1,cn1,cs1=b
    def compare(t0,s0,t1,s1):
        diffs=[]
        for side in (1,-1):
            left=np.asarray(t0)[np.asarray(s0)==side]
            right=np.asarray(t1)[np.asarray(s1)==side]
            if not len(right):continue
            for v in left:
                ix=np.searchsorted(right,v)
                opts=[right[i] for i in (ix-1,ix) if 0<=i<len(right)]
                if opts:
                    d=min(opts,key=lambda x:abs(x-v))-v
                    if abs(d)<=limit_ms*1e6:diffs.append(d/1e6)
        return {
            "events_0ms":len(t0),"events_2p5ms":len(t1),
            "nearest_same_side_matched_20ms":len(diffs),
            "difference_2p5_minus_0ms":{
                "mean_ms":float(np.mean(diffs)) if diffs else None,
                "median_ms":float(np.median(diffs)) if diffs else None,
                "p10_ms":float(np.quantile(diffs,.1)) if diffs else None,
                "p90_ms":float(np.quantile(diffs,.9)) if diffs else None}}
    return {
        "activation":compare(st0,si0,st1,si1),
        "emergency_cancel_requested":compare(cn0,cs0,cn1,cs1)}


def run(day,coin,venue,out,admission_json):
    if day not in MONTH_DAYS or (coin,venue) not in (
        ("SOL","okex-swap"),("DOGE","okex-swap"),
        ("1000PEPE","okex-swap"),("BTC","okex-swap")):
        raise ValueError("Only selected mechanism market days")
    if coin!="SOL" and day!="2026-06-01":
        raise ValueError("Non-SOL cases are single June preflight only")
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    if coin in ("SOL","DOGE"):
        prior,prior_meta=prior_from_stored_admission(
            admission_json,coin,venue,day)
    else:prior=np.nan;prior_meta={"fit_kind":"original_5plus"}
    with TemporaryDirectory(prefix="v6_epclock_") as work:
        d=prepared(day,coin,venue,Path(work),with_trades=(coin=="BTC"))
        base,bf,be=model_run(d,"g0",prior)
        signal,sf,se=model_run(d,"source",prior)
        a=groups(bf,sf,se,"exchange")
        local=groups(bf,sf,se,"local_observed")
        tr=signal["trading"]
        report={
            "day":day,"coin":coin,"venue":venue,
            "pretrade_minutes":d["pretrade_s"]/60,
            "trade_window_UTC":d["trade_start_utc"]+"–"+d["trade_end_utc"],
            "initial_calibration_prior":prior_meta,
            "HBT_official":True,"maker_fee_bps":2.,
            "g0":short_metrics(base,bf,be,True),
            "source":short_metrics(signal,sf,se,False),
            "source_vs_G0_fill_groups_exchange_clock":a,
            "source_vs_G0_fill_groups_local_observation_clock":local,
            "G0_and_source_first_A_k_sigma_equal":{
                key:base["initial_parameters"][key]==signal["initial_parameters"][key]
                for key in ("A_per_second","k_per_tick","sigma_tick_per_sqrt_second")},
            "clock_limitations":[
                "G0 fill exchange timestamp vs local defense action timestamp are different clocks",
                "Emergency request timing is not HBT exchange acknowledgment",
                "Group B names unmatched nondefense-window fills; cannot uniquely identify path-dependent causation",
                "Exact fill-time-and-side matching differs from same order ID matching"]
        }
        if coin=="SOL":
            st,en,side,_,_=se
            stop_ns=int(signal["start_time_ns"])+(d["pretrade_s"]+3000)*1_000_000_000
            lagged=clamp_shift(st,en,side,stop_ns)
            shift,xf,xe=model_run(d,"shift60",prior,lagged=lagged[:3])
            report["time_shift_60s"]={
                "clock_rule":"schedule at source transition time+60s, do not wrap last-minute forward events into test start",
                "window_exposure_equality":lagged[3],
                "strategy":short_metrics(shift,xf,xe,True),
                "source_vs_G0_fill_groups_exchange_clock":groups(bf,xf,xe),
                "G0_vs_shift60_compared_on_exact_UTC_window":True,
                "source_episode_matches_shifted_schedule_without_out_of_window_wrap":True}
            if day=="2026-06-01":
                # 50 -> 75 USDT^-1: exact same pretrade intensity, cash/queue model.
                z0,zt,ztr=model_run(d,"g0",prior,GAMMA_SCALE)
                z1,zf,z1tr=model_run(d,"source",prior,GAMMA_SCALE)
                report["gamma_SOL_exploratory"]= {
                    "baseline_gamma_USDT_inverse":50.,
                    "alternative_gamma_USDT_inverse":50.*GAMMA_SCALE,
                    "gamma_increase_not_guaranteed_to_raise_halfspread":True,
                    "g0_high_gamma":short_metrics(z0,zt,ztr,True),
                    "source_high_gamma":short_metrics(z1,zf,z1tr,False),
                    "source_high_gamma_vs_G0_high_gamma":groups(zt,zf,z1tr),
                    "retention_high_gamma":z1["trading"]["engine_fills"]/z0["trading"]["engine_fills"],
                    "retention_original_gamma":tr["engine_fills"]/base["trading"]["engine_fills"]}
        elif coin=="BTC" and day=="2026-06-01":
            high25,hf25,he25=model_run(d,"high08",prior)
            z=extra_shift_data(d)
            high0,hf0,he0=model_run(z,"high08",prior)
            eq=(hf25["exchange_timestamp_ns"].tolist()==hf0["exchange_timestamp_ns"].tolist() and
                hf25["side_sign"].tolist()==hf0["side_sign"].tolist() and
                hf25["exec_price"].tolist()==hf0["exec_price"].tolist() and
                hf25["exec_qty"].tolist()==hf0["exec_qty"].tolist())
            report["btc_high08_latency_event_identity"]={
                "theoretical_extra_delay_0ms":short_metrics(high0,hf0,he0,False),
                "extra_delay_2p5ms":short_metrics(high25,hf25,he25,False),
                "all_fills_same_exchange_time_side_price_qty":eq,
                "net_equity_exact_equal":high0["trading"]["end_net_marked_equity_usdt"]==
                                         high25["trading"]["end_net_marked_equity_usdt"],
                "defense_activation_and_urgent_cancel_shift":
                    nearest_episode_difference_ms(he0,he25),
                "explanation_limit":"The same exchange fills can occur despite shifted cancel requests, because orders, ACKs and queue paths are discrete; real propagation latency not identified"}
        # Tiny per-fill mechanism artifact kept only in CI, never committed.
        # Exact six-day pooled P10 requires actual fill observations, not the
        # average of six daily P10 statistics.
        packed={}
        fill_sources={"g0":bf,"source":sf}
        if coin=="SOL":
            fill_sources["shift60"]=xf
        for name,fill in fill_sources.items():
            for key in ("exchange_timestamp_ns","local_observed_ns","side_sign",
                        "spread_capture_bps"):
                packed[name+"__"+key]=np.asarray(fill[key])
            for h in HORIZONS:
                for key in (f"markout_{h}ms_bps",f"fresh_{h}ms",
                            f"gross_edge_{h}ms_bps"):
                    packed[name+"__"+key]=np.asarray(fill[key])
        if coin=="SOL":
            packed["source__episode_start"]=se[0]
            packed["source__episode_end"]=se[1]
            packed["source__episode_side"]=se[2]
            packed["source__urgent_time"]=se[3]
            packed["source__urgent_side"]=se[4]
            packed["shift60__episode_start"]=xe[0]
            packed["shift60__episode_end"]=xe[1]
            packed["shift60__episode_side"]=xe[2]
            packed["shift60__urgent_time"]=xe[3]
            packed["shift60__urgent_side"]=xe[4]
        np.savez_compressed(out/"fill_groups_CI_only.npz",**packed)
        filename=out/"case.json"
        filename.write_text(json.dumps(report,ensure_ascii=False,indent=2,allow_nan=False),encoding="utf8")
        print("V6_MECHANISM",day,coin,
              "G0",base["trading"]["engine_fills"],
              "SOURCE",tr["engine_fills"],
              "DIRECT_A",a["Group_A_active_or_200ms_post_cancel"]["count"],
              "B_PATH_OTHER",a["Group_B_no_local_defense_or_cancel_tail"]["count"],
              "EPISODES",len(se[0]),
              flush=True)
        if "time_shift_60s" in report:
            p=report["time_shift_60s"]["strategy"]
            print("V6_SHIFT",day,coin,p["fills"],
                  report["time_shift_60s"]["window_exposure_equality"],flush=True)
        return report


if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--day",required=True)
    p.add_argument("--coin",required=True)
    p.add_argument("--venue",default="okex-swap")
    p.add_argument("--out",required=True)
    p.add_argument("--admission-json",default="reports/source_defense/step3_fit/step3_fit_admission.json")
    q=p.parse_args()
    run(q.day,q.coin,q.venue,q.out,q.admission_json)
