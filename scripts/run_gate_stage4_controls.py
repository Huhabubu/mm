"""Gate BTC stage4 pre-period training and locked 6-day control HBT evaluation.

Two *non-Binance-direction* controls:
A static symmetric outward GLFT quote shift (fixed from 2026-05-01).
B Gate-only local fitted sigma > threshold pauses both sides (fixed May training).

All controls beta=0; they receive the SAME external feed event clock as
the frozen beta=0 GLFT, but Binance midpoint is never read in pricing.
"""
import argparse
import csv
import json
import math
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
from run_gate_btc_origin_glft import (
    SCHEMAS, safe_gate_csv, require_unshifted_target_clock,
    load_gate_mid, read_binance_bbo, visible_timestamps,
    exact_moments_5s, fetch_contract, scenario, build_asset, BINANCE_DELAY_MS,
)
from gate_origin_glft_strategy import run_gate_origin_glft
from gate_stage4_controls import run_gate_stage4_control
from g0_glft_event_driven import summarize
from tardis_segmented_hbt import convert_fuse_parts
from analyze_gate_1645_fill_quality import label_fills, model_day

TRAIN_DAY="2026-05-01"
EVAL_DAYS=tuple("2026-%02d-01" % m for m in range(6,12))
WIDEN_RATIOS=(0.10,0.25,0.50,1.00)
VOL_QUANTILES=(0.55,0.70,0.85,0.95)
TARGET_NAMES=("glft","both_1p0")
CONTROL_NAMES=("static_widen","gate_sigma_pause")


def compact_summary(summary):
    result=dict(summary)
    result.pop("calibration_trace",None)
    return result


def fill_array(raw):
    return {
        "exchange_timestamp_ns":np.asarray(raw[-5],np.int64),
        "side":np.asarray(raw[-4],np.int64),
        "local_observed_ns":np.asarray(raw[-2][1],np.int64),
        "exec_price":np.asarray(raw[-2][2],np.float64),
        "exec_qty":np.asarray(raw[-2][3],np.float64),
    }


def assert_baseline_identity(a,b):
    x,y=fill_array(a),fill_array(b)
    for key in x:
        if not np.array_equal(x[key],y[key]):
            raise ValueError("stage4 control0 is not exact official baseline: "+key)
    for i in (12,14,15,16,17,18): # inventory, cash, fee, turnover, final mid, final equity
        if not math.isclose(float(a[i]),float(b[i]),abs_tol=1e-9,rel_tol=0):
            raise ValueError(f"stage4 control0 ledger mismatch at raw[{i}]")


def select_closest(training_rows,target_fills,kind):
    rows=[x for x in training_rows if x["kind"]==kind]
    if len(rows)!=4:
        raise ValueError("Incomplete prespecified 4-point May-only grid")
    return min(rows,key=lambda x:(abs(x["fills"]-target_fills),x["candidate_index"]))


def load_locked(path):
    d=json.loads(Path(path).read_text(encoding="utf-8"))
    if (d.get("training_day")!=TRAIN_DAY or d.get("training_window_minutes")!=1430
            or d.get("preselected_widen_ratios")!=list(WIDEN_RATIOS)
            or d.get("preselected_gate_vol_quantiles")!=list(VOL_QUANTILES)
            or sorted(d.get("selected_controls",{}))!=sorted(CONTROL_NAMES)
            or d.get("training_status")!="locked"):
        raise ValueError("Stage4 pre-period lock invalid or changed")
    return d


def market_data(root,day,minutes,required_contract=None,symbol="BTC_USDT"):
    from hftbacktest import ROIVectorMarketDepthBacktest
    if minutes!=1430:
        raise ValueError("Stage4 only trains and evaluates identical 1430min UTC sessions")
    date=datetime.strptime(day,"%Y-%m-%d").replace(tzinfo=timezone.utc)
    begin_us=int(date.timestamp()*1_000_000)
    trade_start=begin_us+600_000_000
    stop=begin_us+86_400_000_000
    spec=fetch_contract(symbol)
    paths={}; counts={}
    for kind in SCHEMAS:
        file=root/f"{kind}.csv"
        print("STAGE4_FEED",day,kind,flush=True)
        counts[kind]=safe_gate_csv(kind,day,stop,file,90_000_000,
            symbol=symbol,
            historical_tick=(float(spec["order_price_round"]) if symbol!="BTC_USDT" else None),
            historical_lot=(float(spec["order_size_min"]) if symbol!="BTC_USDT" else None))
        paths[kind]=file
    require_unshifted_target_clock(counts)
    gt,gm=load_gate_mid(paths["book_ticker"])
    bt,bm=read_binance_bbo(day,stop,symbol=symbol.replace("_",""))
    vis=visible_timestamps(bt,BINANCE_DELAY_MS)
    grid,basis,sigma,coverage=exact_moments_5s(
        vis,bm,gt,gm,trade_start*1000,stop*1000)
    prices=gm[gt<trade_start*1000]
    if len(prices)==0:
        raise ValueError("No 10min Gate price warmup")
    cfg=scenario(float(np.median(prices)),spec)
    if required_contract is not None:
        for field in ("tick","lot","ctval","qty","position_cap","gamma_code",
                      "maker_fee_rate","taker_fee_rate"):
            a=float(required_contract[field])
            b=float(cfg[field])
            if not math.isclose(a,b,rel_tol=0,abs_tol=1e-12):
                raise ValueError(f"Contract/official replay mismatch: {field} {a} != {b}")
    parts,details=convert_fuse_parts(
        paths,counts,root,begin_us,stop,60,cfg["tick"],float(cfg["lot"]))
    return {"day":day,"symbol":symbol,"minutes":minutes,"params":cfg,"gt":gt,"gm":gm,
            "visible":vis,"binance_mid":bm,"grid":grid,
            "basis":basis,"sigma":sigma,"coverage":coverage,
            "parts":parts,"low":float(np.min(gm)),"high":float(np.max(gm)),
            "feed_stats":counts,"stop_ns":stop*1000}


def simulate(data,model,kind,extra=0.0,gate_sigma_ceiling=float("inf"),
             output_dir=None):
    from hftbacktest import Recorder, ROIVectorMarketDepthBacktest
    cfg=data["params"]
    eng=ROIVectorMarketDepthBacktest([build_asset(data["parts"],data["low"],data["high"],cfg)])
    rec=Recorder(eng.num_assets,data["minutes"]*600+10) if output_dir else None
    try:
        args=(eng,data["visible"],data["binance_mid"],data["minutes"]*600)
        more=(data["grid"],data["basis"],data["sigma"],0,
              cfg["gamma_code"],cfg["tick"],cfg["ctval"],cfg["qty"],
              cfg["position_cap"],rec.recorder if rec else None,data["stop_ns"])
        if kind=="original_glft":
            raw=run_gate_origin_glft(*args,0.0,*more)
        elif kind=="original_beta1":
            ref_args=(data["grid"],data["basis"],data["sigma"],1,
                      cfg["gamma_code"],cfg["tick"],cfg["ctval"],cfg["qty"],
                      cfg["position_cap"],rec.recorder if rec else None,data["stop_ns"])
            raw=run_gate_origin_glft(*args,1.0,*ref_args)
        else:
            mode={"reference_copy":0,"static_widen":1,"gate_sigma_pause":2}[kind]
            raw=run_gate_stage4_control(*args,0.0,*more,mode,extra,gate_sigma_ceiling)
        if raw[5]!=1:
            raise ValueError(f"Initial 10min Gate GLFT A/k invalid: {data['day']} {kind}")
        # Only May training starts ~0.7s after UTC midnight due to its
        # first replayable L2 event; require <1s of initial-clock truncation.
        # Never relax the frozen June-Nov 1430min official acceptance gate.
        summary_minutes=data["minutes"]
        if data["day"]==TRAIN_DAY:
            expected=6000+data["minutes"]*600
            if not (expected-10 <= raw[0] <= expected):
                raise ValueError(
                    f"May first feed starts too late: samples={raw[0]}, expected={expected}")
            # This changes summarize()'s strict minimum-sample predicate ONLY.
            # Its actual sample denominators, end times, executions, and ledger
            # always come from raw HBT; May uses all data until UTC midnight.
            summary_minutes=data["minutes"]-1
        s=summarize(raw,summary_minutes,True,
                    1.0 if kind=="original_beta1" else 0.0,
                    cfg["position_cap"],cfg["qty"],
                    tail_tolerance_samples=(10 if data.get("symbol")!="BTC_USDT" else 5))
        if s["end_time_ns"] < data["stop_ns"]-1_000_000_000:
            raise ValueError("Official HBT truncated before day end")
        if output_dir is not None:
            out=Path(output_dir)/model
            out.mkdir(parents=True,exist_ok=True)
            rec.to_npz(str(out/"recorder.npz"))
            arr=fill_array(raw)
            np.savez_compressed(out/"fills.npz",**arr)
            annotated,quality=model_day(model,arr,{"trading":s["trading"]},
                                        data["gt"],data["gm"],data["stop_ns"],
                                        cfg["ctval"])
            np.savez_compressed(out/"annotated.npz",**annotated)
            compact_summary(s)["stage4_quality"]=quality
            # Return quality as field without altering original HBT trading results.
            s["stage4_quality"]=quality
        print("STAGE4_MODEL",data["day"],model,"fills",s["trading"]["engine_fills"],
              "zero_fee",s["trading"]["end_net_marked_equity_usdt"]+s["trading"]["fee_usdt"],
              "net_fee",s["trading"]["end_net_marked_equity_usdt"],flush=True)
        return raw,s
    finally:
        eng.close()


def train(day,output,symbol="BTC_USDT"):
    if day!=TRAIN_DAY:
        raise ValueError("TRAIN DAY LOCK: only May 01 2026")
    output.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="stage4_train_may_") as tmp:
        d=market_data(Path(tmp),day,1430,symbol=symbol)
        raw_glft,s_glft=simulate(d,"glft","original_glft")
        raw_ref,s_ref=simulate(d,"both_1p0","original_beta1")
        raw_copy,s_copy=simulate(d,"glft_clone","reference_copy")
        assert_baseline_identity(raw_glft,raw_copy)
        trace=raw_glft[6]
        if len(trace)<100 or not np.isfinite(trace[:,3:5]).all():
            raise ValueError("No usable May 2026 baseline GLFT fit trace")
        # Dimensional controls chosen entirely in the pre-period.
        # Rounding to 1e-6 tick preserves reproducible numerical parameters.
        median_half=float(np.median(trace[:,4]))
        sigma_samples=trace[:,3]
        target=int(s_ref["trading"]["engine_fills"])
        rows=[]
        for k,ratio in enumerate(WIDEN_RATIOS):
            v=float(round(median_half*ratio,6))
            _,s=simulate(d,f"widen_{k}","static_widen",extra=v)
            rows.append({"kind":"static_widen","candidate_index":k,
                         "widen_ratio_vs_train_glft_half":ratio,
                         "extra_half_ticks":v,
                         "fills":int(s["trading"]["engine_fills"]),
                         "net_usdt":s["trading"]["end_net_marked_equity_usdt"]})
        for k,q in enumerate(VOL_QUANTILES):
            value=float(round(float(np.quantile(sigma_samples,q)),6))
            _,s=simulate(d,f"gate_sigma_{k}","gate_sigma_pause",
                         gate_sigma_ceiling=value)
            rows.append({"kind":"gate_sigma_pause","candidate_index":k,
                         "train_glft_sigma_quantile":q,
                         "local_sigma_ceiling":value,
                         "fills":int(s["trading"]["engine_fills"]),
                         "net_usdt":s["trading"]["end_net_marked_equity_usdt"]})
        chosen_a=select_closest(rows,target,"static_widen")
        chosen_b=select_closest(rows,target,"gate_sigma_pause")
        result={"training_status":"locked","training_day":day,
                "training_window_minutes":1430,
                "beta1_train_target_fills":target,
                "beta0_train_fills":int(s_glft["trading"]["engine_fills"]),
                "train_median_half_ticks":median_half,
                "training_may_sigma_tick_quantiles":{
                    str(q):float(np.quantile(sigma_samples,q)) for q in VOL_QUANTILES},
                "preselected_widen_ratios":list(WIDEN_RATIOS),
                "preselected_gate_vol_quantiles":list(VOL_QUANTILES),
                "training_grid":rows,
                "selected_controls":{
                    "static_widen":chosen_a,
                    "gate_sigma_pause":chosen_b,
                },
                "training_selected_objective":"minimize abs(May near-complete full-day model fill count - May beta1 near-complete full-day fill count) separately per class",
                "may_hbt_sample_count":int(raw_glft[0]),
                "may_hbt_expected_utc_samples":6000+1430*600,
                "may_hbt_utc_boundary_shortfall_s":float((6000+1430*600-raw_glft[0])*0.1),
                "may_hbt_start_timestamp_ns":int(raw_glft[-3]),
                "may_hbt_end_timestamp_ns":int(raw_glft[-2][0]),
                "replay_controls":"May clone beta=0 exactly matches original GLFT fill arrays and end cash, fee, position and equity",
                "signal_usage":"no Binance mid/direction in control quotes; Binance event clock retained as in original beta=0 GLFT",
                "scenario":d["params"],"source_day":"Tardis 2026-05-01 only", "symbol":symbol,
                "no_evaluation_optimization":True}
        (output/"stage4_train_lock.json").write_text(
            json.dumps(result,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
        print("STAGE4_MAY_LOCK",json.dumps(result["selected_controls"],ensure_ascii=False),flush=True)


def evaluate(day,lock_file,frozen_dir,output,symbol="BTC_USDT"):
    if day not in EVAL_DAYS:
        raise ValueError("Stage4 eval only fixed June-Nov 2026 first days")
    lock=load_locked(lock_file)
    if lock.get("symbol","BTC_USDT") != symbol:
        raise ValueError("May prelock was fitted for another Gate symbol")
    out=output
    out.mkdir(parents=True,exist_ok=True)
    report=json.loads((frozen_dir/f"{day}_1430m.json").read_text())
    if (report["status"]!="complete" or
        report["binance_extra_visible_delay_ms"]!=2.5 or
        report["experimental_trigger_sigma_multiple"]!=1.645 or
        not set(TARGET_NAMES).issubset(report["variants"]) or
        report.get("gate_symbol","BTC_USDT")!=symbol):
        raise ValueError("Wrong official fixed Gate reference scenario")
    with tempfile.TemporaryDirectory(prefix=f"stage4_eval_{day}_") as tmp:
        d=market_data(Path(tmp),day,1430,report["scenario"],symbol=symbol)
        for kind in SCHEMAS:
            for field in ("rows","first_local_us","last_local_us"):
                if d["feed_stats"][kind][field]!=report["feed_stats"][kind][field]:
                    raise ValueError(f"Evaluation feed drift: {day} {kind} {field}")
        chosen=lock["selected_controls"]
        results={}
        for name,config in chosen.items():
            _,s=simulate(d,name,name,
                extra=config.get("extra_half_ticks",0.0),
                gate_sigma_ceiling=config.get("local_sigma_ceiling",float("inf")),
                output_dir=out)
            results[name]=compact_summary(s)
        # original GLFT and beta1 are immutable and only read for side-by-side table.
        immutable={}
        for name in TARGET_NAMES:
            t=report["variants"][name]["trading"]
            immutable[name]={
                "trading":{
                    "engine_fills":t["engine_fills"],
                    "fee_usdt":t["fee_usdt"],
                    "end_net_marked_equity_usdt":t["end_net_marked_equity_usdt"],
                    "trading_value_usdt":t["trading_value_usdt"],
                    "mean_abs_inventory_contracts":t["mean_abs_inventory_contracts"],
                    "rms_inventory_contracts":t["rms_inventory_contracts"],
                    "max_abs_position_contracts":t["max_abs_position_contracts"],
                    "cancel_requests":t["cancel_requests"],
                    "submissions":t["submissions"],
                }
            }
        out_report={
            "status":"complete","day":day,"minutes_traded":1430,
            "training_lock_day":lock["training_day"],
            "selected_controls":chosen,"scenario":d["params"],
            "immutable_frozen_reference":immutable,
            "controls":results,
            "same_source_gate_qa":True,
            "binance_direction_disabled_in_controls":True,
            "hbt_2_4_4":True}
        (out/f"{day}_stage4.json").write_text(json.dumps(out_report,indent=2,ensure_ascii=False)+"\n",encoding="utf-8")
        print("STAGE4_EVAL_DAY_COMPLETE",day,flush=True)


if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("--mode",choices=["train","eval"],required=True)
    ap.add_argument("--day",required=True)
    ap.add_argument("--lock")
    ap.add_argument("--frozen")
    ap.add_argument("--out",required=True)
    ap.add_argument("--symbol",default="BTC_USDT",choices=("BTC_USDT","ETH_USDT"))
    a=ap.parse_args()
    if a.mode=="train":
        train(a.day,Path(a.out),a.symbol)
    else:
        if not a.lock or not a.frozen:
            ap.error("eval requires immutable May --lock and frozen per-date --frozen")
        evaluate(a.day,a.lock,Path(a.frozen),Path(a.out),a.symbol)
