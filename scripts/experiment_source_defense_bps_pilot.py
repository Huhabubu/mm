"""Step2 strictly target-mid-relative bps sparse single-day official HBT pilot.

This runner is independent of the historical tick-based experiment scripts.
No main-branch changes or test-period threshold optimization.


No baseline files modified. Identical Tardis fused market tape, GLFT A/k/sigma,
risk, inventory and queue to the corrected main-branch 2.5ms baseline.

Modes: original G0, beta=1, fast EMA source-gated widen/cancel, OKX placebo.
All *economic* comparisons report official marked PnL AFTER the same maker fee;
spread capture / markouts are explicitly separate mechanism diagnostics.
"""
import argparse
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
from hftbacktest import Recorder, ROIVectorMarketDepthBacktest
from hftbacktest.data.utils import tardis
from hftbacktest.data.validation import validate_event_order

from check_okx_hft_data import SCHEMAS, extract_window, require_unshifted_target_clock
from cross_venue_event_probe import read_binance_bbo, visible_timestamps
from run_beta_one_direct_gap import NS, causal_60s_basis, read_local_bbo
from run_multivenue_glft import MARKETS, build_asset, order_scenario, validate_record
from g0_glft_event_driven import run_event_glft, summarize
from experiment_source_defense_engine import run_sparse_glft
from experiment_source_defense_signal import past_grid_time_ema

HORIZONS_MS=(100,500,1000,5000)
DELAY_MS=2.5
# Fixed from the *descriptive* Step1 0.5–1.0bps high-loss G0 bins.
# These are 3 discrete, predeclared exploratory candidates, NOT a grid search.
BPS_CONFIGS = (
    ("c1", 1, .5, .2, .1, .5, 50_000_000),
    ("c2", 1, .5, .5, .15, 1.0, 75_000_000),
    ("c3", 2, 1.0, .5, .2, 1.5, 100_000_000),
)



def annotate_fills(raw, ts, native_mid, tick, ctval):
    """Metrics at local observed fill receipt and as-of target future quotes."""
    exch=np.asarray(raw[31],dtype=np.int64)
    side=np.asarray(raw[32],dtype=np.int64)
    local=np.asarray(raw[34][1],dtype=np.int64)
    px=np.asarray(raw[34][2],dtype=float)
    qty=np.asarray(raw[34][3],dtype=float)
    from hftbacktest import BUY
    signs=np.where(side==BUY,1.,-1.)
    if not (len(exch)==len(side)==len(local)==len(px)==len(qty)):
        raise ValueError("Inconsistent official fill arrays")
    out={"exchange_timestamp_ns":exch,"local_observed_ns":local,
         "side_sign":signs.astype(np.int8),"exec_price":px,"exec_qty":qty}
    if len(local)==0:
        for h in HORIZONS_MS:
            out[f"fresh_{h}ms"]=np.zeros(0,dtype=bool)
            out[f"markout_{h}ms_bps"]=np.empty(0)
            out[f"spread_capture_{h}ms_bps"]=np.empty(0)
            out[f"adverse_selection_cost_{h}ms_bps"]=np.empty(0)
            out[f"gross_edge_{h}ms_bps"]=np.empty(0)
        return out
    a=np.searchsorted(ts,local,side="right")-1
    safe=np.maximum(a,0)
    age=local-ts[safe]
    valid=(a>=0)&(age>=0)&(age<=500_000_000)
    mid=native_mid[safe]
    spread=signs*(mid-px)/mid*10000.
    out["spread_capture_bps"]=spread
    out["native_mid_at_fill_observation"]=mid
    out["notional_usdt"]=qty*ctval*mid
    for h in HORIZONS_MS:
        future=local+h*1_000_000
        j=np.searchsorted(ts,future,side="right")-1
        sj=np.maximum(j,0)
        age_future=future-ts[sj]
        fresh=(valid&(j>=0)&(age_future>=0)&(age_future<=500_000_000))
        mark=signs*(native_mid[sj]-mid)/mid*10000.
        out[f"fresh_{h}ms"]=fresh
        out[f"markout_{h}ms_bps"]=mark
        out[f"adverse_selection_cost_{h}ms_bps"]=-mark
        out[f"spread_capture_{h}ms_bps"]=spread.copy()
        out[f"gross_edge_{h}ms_bps"]=spread+mark
    return out


def classify_opportunities(baseline, test):
    """One-to-one same EXCHANGE TIME and SIDE; not cancel effectiveness."""
    from collections import Counter
    counts=Counter(zip(test["exchange_timestamp_ns"].tolist(),
                       test["side_sign"].tolist()))
    mask=np.zeros(len(baseline["side_sign"]),dtype=bool)
    for i,key in enumerate(zip(baseline["exchange_timestamp_ns"].tolist(),
                               baseline["side_sign"].tolist())):
        if counts[key]>0:
            mask[i]=True
            counts[key]-=1
    return ~mask,mask


def fill_stats(x,mask,h):
    valid=mask&x[f"fresh_{h}ms"]
    mark=x[f"markout_{h}ms_bps"][valid]
    capture=x["spread_capture_bps"][valid]
    gross=x[f"gross_edge_{h}ms_bps"][valid]
    return {"matched_group_fills":int(mask.sum()),
            "valid_markout_fills":int(valid.sum()),
            "mean_markout_bps":float(np.mean(mark)) if len(mark) else None,
            "p05_markout_bps":float(np.quantile(mark,.05)) if len(mark) else None,
            "p10_markout_bps":float(np.quantile(mark,.10)) if len(mark) else None,
            "toxic_below_minus2bps_fraction":float(np.mean(mark<-2.)) if len(mark) else None,
            "spread_capture_mean_bps":float(np.mean(capture)) if len(mark) else None,
            "adverse_cost_mean_bps":float(np.mean(-mark)) if len(mark) else None,
            "gross_edge_mean_bps":float(np.mean(gross)) if len(mark) else None}


def attribution_of_paired_fills(baseline, test):
    dropped,kept=classify_opportunities(baseline,test)
    res={"base_only_count":int(dropped.sum()),"base_shared_count":int(kept.sum()),
         "variant_only_count":int(len(test["side_sign"])-kept.sum()),
         "by_horizon":{}}
    for h in HORIZONS_MS:
        x=fill_stats(baseline,dropped,h)
        y=fill_stats(baseline,kept,h)
        lift=(y["mean_markout_bps"]-x["mean_markout_bps"]
              if x["mean_markout_bps"] is not None and
                 y["mean_markout_bps"] is not None else None)
        res["by_horizon"][str(h)+"ms"]={"base_only":x,"base_kept":y,
                                         "selectivity_lift_kept_minus_dropped_bps":lift}
    # Predeclared bottom-20% baseline 500ms markout definition. The cutoff is
    # analysis-only, not available to either strategy when it places orders.
    valid=baseline["fresh_500ms"].astype(bool)
    mark=baseline["markout_500ms_bps"]
    cutoff=float(np.quantile(mark[valid],.2)) if np.any(valid) else None
    if cutoff is not None:
        toxic=valid&(mark<=cutoff)
        safe=valid&(mark>cutoff)
        recall=float(np.mean(dropped[toxic])) if np.any(toxic) else None
        sacrifice=float(np.mean(dropped[safe])) if np.any(safe) else None
    else:
        recall=sacrifice=None
    res["bottom_20pct_500ms_toxic_cutoff_bps"]=cutoff
    res["toxic_recall_descriptive"]=recall
    res["safe_sacrifice_descriptive"]=sacrifice
    res["causal_caveat"]="Matched exchange time and side only, not identical orders or cancel ACK"
    return res


def run(day,coin,minutes=50,out_dir="reports/source_defense/step2_bps"):
    if day!="2026-06-01" or coin not in ("BTC","SOL") or minutes!=50:
        raise ValueError("Research pilot is preregistered for 2026-06-01 BTC/SOL 50min")
    spec=MARKETS[("okx",coin)]
    start=int(datetime.strptime(day,"%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()*1e6)
    end=start+(600+minutes*60)*1_000_000+5_000_000
    output=Path(out_dir)/f"{day}_okx_{coin}"
    output.mkdir(parents=True,exist_ok=True)
    report={"day":day,"coin":coin,"venue":"okx",
            "scope":"exploratory 50min not independent full day",
            "delay_binance_extra_ms":2.5,"order_request_ms":1,"order_reply_ms":1,
            "min_side_action_ms":20,"maker_fee_bps":2.,
            "hbt_queue":"power_prob_2.0_no_partial_fill",
            "sparse_params":{"time_ema_half_lives_s":[1,2],"ema_grid_ms":10,
                 "impulse_window_ms":100,
                 "predeclared_candidates":[
                     {"id":n,"half_life_s":h,"gap_gate_bps":g,
                      "impulse_gate_bps":i,"hysteresis_exit_bps":ex,
                      "widen_bps":w,"hold_ms":hn/1.e6}
                     for n,h,g,i,ex,w,hn in BPS_CONFIGS]},

            "variants":{}}
    with tempfile.TemporaryDirectory(prefix="sparse_pilot_") as tmp:
        folder=Path(tmp);paths={}
        counts={}
        for k in SCHEMAS:
            paths[k]=folder/(k+".csv")
            counts[k]=extract_window(k,day,end,paths[k],max_rows=3_000_000,
                                      exchange=spec.exchange,symbol=spec.symbol)
        require_unshifted_target_clock(counts)
        fused=folder/"fused.npz"
        events=tardis.convert_fuse(str(paths["trades"]),
                                  str(paths["incremental_book_L2"]),
                                  str(paths["book_ticker"]),tick_size=spec.tick,
                                  lot_size=spec.lot,output_filename=str(fused),
                                  ss_buffer_size=max(1200,counts["incremental_book_L2"]["snapshot_rows"]+100),
                                  snapshot_mode="process")
        validate_event_order(events)
        del events
        ot,om=read_local_bbo(paths["book_ticker"])
        b_raw,bm=read_binance_bbo(day,end,symbol=coin+"USDT")
        bt=visible_timestamps(b_raw,DELAY_MS)
        original_t,original_b=causal_60s_basis(ot,om,bt,bm)
        ema={h:past_grid_time_ema(ot,om,bt,bm,h) for h in (1,2)}
        warmup_time=start*1000+600*NS
        if not np.any(np.isfinite(original_b[original_t<warmup_time])):
            raise ValueError("No original 60s signal basis at start")
        for h,(g,b) in ema.items():
            if not np.any(np.isfinite(b[g<warmup_time])):
                raise ValueError(f"EMA {h}s missing warmup")
        mid=float(np.median(om[ot<warmup_time]))
        scenario=order_scenario(spec,mid)
        low,high=float(np.min(om)),float(np.max(om))
        annotations={}
        all_variants=[("glft",0,0.,0,None),("beta1",0,1.,0,None)]
        for config in BPS_CONFIGS:
            n,h,g,i,exit_bps,w,hold_ns=config
            all_variants.extend([
                (n+"_widen",h,0.,1,config),
                (n+"_cancel",h,0.,2,config),
                (n+"_okx_placebo",h,0.,3,config),
            ])
        for name,h,beta,mode,config in all_variants:
            engine=ROIVectorMarketDepthBacktest([build_asset(fused,low,high,spec)])
            recorder=Recorder(engine.num_assets,minutes*600+10)
            try:
                arguments=(engine,bt,bm,minutes*600,True,scenario["gamma_code"],
                           True,beta,ot,om,original_t,original_b,0.,0.,
                           10.,250_000_000,0.,None,0,spec.tick,spec.ctval,
                           scenario["order_qty"],scenario["position_cap"],
                           recorder.recorder)
                if mode==0:
                    raw=run_event_glft(*arguments)
                else:
                    grid,values=ema[h]
                    n,h,g,i,exit_bps,w,hold_ns=config
                    raw=run_sparse_glft(*arguments,
                            sparse_mode=mode,ema_grid_ns=grid,ema_values=values,
                            impulse_window_ns=100_000_000,
                            gap_gate_bps=g,impulse_gate_bps=i,
                            hysteresis_exit_th_bps=exit_bps,
                            widen_bps=w,defense_hold_ns=hold_ns,
                            defense_cooldown_ns=hold_ns)
                if int(raw[5])!=1:
                    report["variants"][name]={"status":"no_initial_fit"}
                    raise ValueError("Pilot market failed original GLFT fit")
                summary=summarize(raw,minutes,True,beta,scenario["position_cap"],
                                  scenario["order_qty"])
                rec=recorder.get(0)
                validate_record(rec,summary)
                trading=summary["trading"]
                marked_gross=trading["end_net_marked_equity_usdt"]+trading["fee_usdt"]
                turnover=trading["trading_value_usdt"]
                # Official GTX fills are maker-only. Verify exactly against
                # the official ledger BEFORE applying experimental fee shifts.
                if abs(trading["fee_usdt"]-.0002*turnover)>max(1e-6,turnover*1e-9):
                    raise RuntimeError("Not maker-only fee ledger; cannot reprice fee scenarios")
                fills=annotate_fills(raw,ot,om,spec.tick,spec.ctval)
                annotations[name]=fills
                np.savez_compressed(output/f"{name}_fill_diagnostics.npz",**fills)
                sparse_stats=(raw[35].tolist() if mode else None)
                report["variants"][name]={
                    "status":"complete",
                    "original_glft_initial_parameters":summary["initial_parameters"],
                    "calibration_trace":summary["calibration_trace"],
                    "trading":trading,
                    "fee_scenarios_net_marked_usdt":{
                        "maker_2bps":trading["end_net_marked_equity_usdt"],
                        "maker_0bps_diagnostic":marked_gross,
                        "maker_minus0p2bps_rebate_scenario":marked_gross+.00002*turnover},
                    "sparse_counters":sparse_stats}
                print("PILOT",day,coin,name,
                      "NET",trading["end_net_marked_equity_usdt"],
                      "FILLS",trading["engine_fills"],
                      "CANCEL",trading["cancel_requests"],
                      "GATE",sparse_stats[:3] if sparse_stats else None,flush=True)
            finally:
                engine.close()
        base=report["variants"]["glft"]
        trace=base["calibration_trace"]
        for name,item in report["variants"].items():
            if item["calibration_trace"]!=trace:
                raise RuntimeError("Different GLFT estimator across experimental modes")
        original_count=base["trading"]["engine_fills"]
        assert original_count>0
        for name,h,beta,mode,config in all_variants:
            if name=="glft":continue
            item=report["variants"][name]
            tr=item["trading"]
            adverse=attribution_of_paired_fills(annotations["glft"],annotations[name])
            report["variants"][name]["relative_to_glft"]={
                "fill_retention":tr["engine_fills"]/original_count,
                "net_marked_delta_usdt":tr["end_net_marked_equity_usdt"]-
                                          base["trading"]["end_net_marked_equity_usdt"],
                "cancel_requests":tr["cancel_requests"],
                "selectivity":adverse}
            # Do not select a candidate based on these already-explored samples.
            s100=adverse["by_horizon"]["100ms"]["selectivity_lift_kept_minus_dropped_bps"]
            s500=adverse["by_horizon"]["500ms"]["selectivity_lift_kept_minus_dropped_bps"]
            report["variants"][name]["pilot_acceptance_descriptive_only"]={
                "cancels_at_most_5500":tr["cancel_requests"]<=5500,
                "fills_at_least_80pct_baseline":tr["engine_fills"]/original_count>=.8,
                "fills_at_least_75pct_baseline":tr["engine_fills"]/original_count>=.75,
                "selectivity_lift_positive_100_and_500ms":(
                    s100 is not None and s500 is not None and s100>0 and s500>0),
                "cannot_establish_statistical_significance_from_one_explored_day":True}
    report["official_engine"]="hftbacktest==2.4.4; no custom matcher"
    report["effect_claim"]="Descriptive scenario only; no independent sample out of sample"
    target=output/"summary.json"
    target.write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding="utf-8")
    print("SAVED",target,flush=True)
    return report


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--day",default="2026-06-01")
    p.add_argument("--coin",required=True,choices=["BTC","SOL"])
    p.add_argument("--out",default="reports/source_defense/step2_bps")
    a=p.parse_args()
    run(a.day,a.coin,50,a.out)


if __name__=="__main__":
    main()
