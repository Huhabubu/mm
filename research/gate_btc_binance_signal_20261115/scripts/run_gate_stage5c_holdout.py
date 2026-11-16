"""Stage5C untouched historical April 01 HBT holdout after pre-April mode lock.

Selection of ONE mode uses ONLY six previous explored June-Nov Stage5A days.
April data is unavailable to the selection command. No lookahead or retuning.
"""
import argparse
import hashlib
import json
import math
import tempfile
from pathlib import Path
import numpy as np
from hftbacktest import Recorder, ROIVectorMarketDepthBacktest
from gate_origin_glft_strategy import run_gate_origin_glft
from gate_stage5a_pause_strategy import run_gate_stage5a_pause
from run_gate_btc_origin_glft import build_asset
from run_gate_stage4_controls import market_data, fill_array, compact_summary
from analyze_gate_1645_fill_quality import model_day
from g0_glft_event_driven import summarize

HOLDOUT="2026-04-01"
RANK_ORDER=("both_1p0","defensive_1p0","risk_side_pause")
ORDER_CODES={"glft":(0.,0),"both_1p0":(1.,1),
             "defensive_1p0":(1.,2),"risk_side_pause":(1.,3)}


def choose_mode(stage5a):
    if (stage5a.get("status")!="complete" or len(stage5a.get("evaluation_days",[]))!=6
        or stage5a.get("no_gamma_sigma_beta_changes") is not True):
        raise ValueError("Stage5C requires all six frozen 5A exploration days")
    if stage5a["evaluation_days"] != [
        f"2026-{m:02d}-01" for m in range(6,12)]:
        raise ValueError("Stage5C only allowed to select from June–November")
    av=stage5a["day_equal_mean"]
    base=float(av["both_1p0"]["rms_inventory"])
    cap=1.25*base
    eligible=[]
    checks=[]
    for priority,name in enumerate(RANK_ORDER):
        if name not in av:
            raise ValueError("Missing frozen 5A candidate "+name)
        pnl=float(av[name]["fee_net_pnl_usdt"])
        rms=float(av[name]["rms_inventory"])
        if not math.isfinite(pnl) or not math.isfinite(rms):
            raise ValueError("Invalid 5A risk and fee net outcome")
        accepted=rms<=cap+1e-12
        checks.append({"mode":name,"net_day_equal_usdt":pnl,
                       "inventory_rms_day_equal":rms,
                       "rms_allowed":cap,"eligible":accepted})
        if accepted:
            eligible.append((pnl,-priority,name))
    if not eligible:
        raise ValueError("No risk-budget-feasible candidate")
    selected=max(eligible)[2]
    return selected,checks


def lock(stage5a_path,out_path):
    b=stage5a_path.read_bytes()
    stage5a=json.loads(b)
    selected,checks=choose_mode(stage5a)
    x={"status":"locked","selected_mode":selected,
       "holdout_date":HOLDOUT,
       "only_selection_source":"explored Stage5A 2026-06 through 2026-11 first days",
       "stage5a_sha256":hashlib.sha256(b).hexdigest(),
       "rule":"fee-inclusive max daily equal mean, inventory RMS <=1.25 x bilateral, deterministic priority bilateral/defensive/pause",
       "candidates":checks,"locked_before_holdout_data":True,
       "chronology":"historically earlier robustness test; not forward chronological validation"}
    out_path.parent.mkdir(parents=True,exist_ok=True)
    out_path.write_text(json.dumps(x,indent=2,ensure_ascii=False)+"\n",encoding="utf-8")
    print("STAGE5C_LOCK_BEFORE_APRIL",selected,flush=True)


def replay(data,model,out):
    cfg=data["params"]
    beta,code=ORDER_CODES[model]
    eng=ROIVectorMarketDepthBacktest([
        build_asset(data["parts"],data["low"],data["high"],cfg)])
    rec=Recorder(eng.num_assets,1430*600+10)
    try:
        args=(eng,data["visible"],data["binance_mid"],1430*600,
              beta,data["grid"],data["basis"],data["sigma"],code,
              cfg["gamma_code"],cfg["tick"],cfg["ctval"],
              cfg["qty"],cfg["position_cap"],rec.recorder,data["stop_ns"])
        run=run_gate_stage5a_pause if model=="risk_side_pause" else run_gate_origin_glft
        raw=run(*args)
        expected=6000+1430*600
        if not expected-10<=raw[0]<=expected:
            raise ValueError("April HBT start-clock shortfall >1s, or missing data")
        # Accept only <=1s original first-event boundary offset, never invent
        # missing market events; all actual metrics still use original raw trace.
        s=summarize(raw,1429,True,beta,cfg["position_cap"],cfg["qty"])
        if raw[5]!=1 or s["end_time_ns"]<data["stop_ns"]-1_000_000_000:
            raise ValueError("Holdout GLFT A/k calibration or end coverage failed")
        fills=fill_array(raw)
        annotated,quality=model_day(model,fills,{"trading":s["trading"]},
                                    data["gt"],data["gm"],data["stop_ns"],cfg["ctval"])
        dst=out/model
        dst.mkdir(parents=True,exist_ok=True)
        rec.to_npz(str(dst/"recorder.npz"))
        np.savez_compressed(dst/"fills.npz",**fills)
        np.savez_compressed(dst/"annotated.npz",**annotated)
        info=compact_summary(s)
        info["quality"]=quality
        info["first_event_sample_count"]=int(raw[0])
        print("STAGE5C_HOLDOUT_MODEL",model,"net",s["trading"]["end_net_marked_equity_usdt"],
              "fills",s["trading"]["engine_fills"],flush=True)
        return info
    finally:
        eng.close()


def evaluate(lock_file,out):
    locked=json.loads(lock_file.read_text(encoding="utf-8"))
    if (locked.get("status")!="locked" or
        locked.get("selected_mode") not in RANK_ORDER or
        locked.get("holdout_date")!=HOLDOUT or
        locked.get("locked_before_holdout_data") is not True or
        len(locked.get("stage5a_sha256",""))!=64):
        raise ValueError("Stage5C selection not correctly locked")
    out.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="stage5c_untouched_april_") as temp:
        data=market_data(Path(temp),HOLDOUT,1430)
        models=list(dict.fromkeys(("glft","both_1p0",locked["selected_mode"])))
        results={m:replay(data,m,out) for m in models}
        report={"status":"complete","holdout_date":HOLDOUT,
                "selected_mode":locked["selected_mode"],
                "strategy_selection_lock":locked,
                "first_time_reading_april_data_after_mode_locked":True,
                "mode_chosen_using_later_calendar_data":True,
                "no_future_within_april_input_prices":True,
                "historical_holdout_not_forward_out_of_sample":True,
                "current_spec_not_historical_spec_proven":True,
                "no_funding_or_end_flatten":True,
                "Tardis_QA":data["feed_stats"],
                "simulation_spec":data["params"],
                "models":results}
        (out/"stage5c_april_holdout.json").write_text(
            json.dumps(report,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    print("STAGE5C_APRIL_HOLDOUT_COMPLETE",flush=True)


if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--mode",choices=("lock","eval"),required=True)
    p.add_argument("--summary",type=Path)
    p.add_argument("--lock",type=Path)
    p.add_argument("--out",type=Path,required=True)
    a=p.parse_args()
    if a.mode=="lock":
        if a.summary is None:
            p.error("lock requires --summary from frozen Stage5A")
        lock(a.summary,a.out)
    else:
        if a.lock is None:
            p.error("eval requires --lock; never select using April data")
        evaluate(a.lock,a.out)
