"""Gate BTC historical replay: compare frozen legacy and shared-core HBT behavior.

No synthetic fills. Reads the same Tardis UTC month-first market data, creates
independent official HBT exchanges, compares every actual fill attribute, sample
Recorder state, counts and exact terminal fee-inclusive equity.
Does NOT claim order-request identity until request-level tracing is implemented.
"""
import argparse
import math
import tempfile
import subprocess
import importlib.util
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
from hftbacktest import Recorder, ROIVectorMarketDepthBacktest
from hftbacktest.data.utils import tardis
from hftbacktest.data.validation import validate_event_order

from core_glft_engine import run_core_glft
from instrument_gate_action_regression import instrument_action_requests
from run_gate_btc_origin_glft import (
    SCHEMAS, BINANCE_DELAY_MS, build_asset, exact_moments_5s,
    fetch_contract, load_gate_mid, read_binance_bbo, require_unshifted_target_clock,
    safe_gate_csv, scenario, visible_timestamps,
)


def assert_exact_legacy_result(old, new, old_rec, new_rec):
    for i, (a, b) in enumerate(zip(old, new)):
        # At raw[-2][4], core supports a longer optional diagnostics probe
        # schema, but probes have no impact on actual market-making actions.
        if i == len(old)-2:
            if len(a) != len(b) or any(not np.array_equal(x,y,equal_nan=True)
                                       for x,y in zip(a[:4],b[:4])):
                raise AssertionError("fill local clocks/prices/quantities mismatch")
            continue
        if isinstance(a,np.ndarray):
            if not np.array_equal(a,b,equal_nan=True):
                raise AssertionError(f"exact HBT result mismatch raw[{i}]")
        elif isinstance(a,tuple):
            raise AssertionError(f"Unexpected tuple at raw[{i}]")
        elif isinstance(a,(float,np.floating)) and math.isnan(a):
            if not math.isnan(b):
                raise AssertionError(f"NaN mismatch raw[{i}]")
        elif a != b:
            raise AssertionError(f"exact HBT result mismatch raw[{i}]: {a} != {b}")
    for key in old_rec.dtype.names:
        if not np.array_equal(old_rec[key],new_rec[key],equal_nan=True):
            raise AssertionError(f"Recorder trace differs: {key}")
    print("CORE_LEGACY_GOLDEN_EXACT",len(old[-5]),"fills",
          old[7],"submissions",old[8],"cancels",
          old[18],"fee_inclusive_end_equity")


def run_one(day="2026-11-01",minutes=50,trace_actions=False):
    y=datetime.strptime(day,"%Y-%m-%d").replace(tzinfo=timezone.utc)
    if y.day!=1 or minutes!=50:
        raise ValueError("One historical month-first 50-minute deterministic probe only")
    day_us=int(y.timestamp()*1_000_000)
    trade_start_us=day_us+600_000_000
    trade_stop_us=trade_start_us+minutes*60_000_000
    end_us=trade_stop_us+5_000_000
    spec=fetch_contract("BTC_USDT")
    with tempfile.TemporaryDirectory(prefix="gate_core_parity_") as temp:
        root=Path(temp)
        paths,counts={},{}
        for kind in SCHEMAS:
            paths[kind]=root/f"{kind}.csv"
            counts[kind]=safe_gate_csv(kind,day,end_us,paths[kind],6_000_000)
        require_unshifted_target_clock(counts)
        ot,om=load_gate_mid(paths["book_ticker"])
        b_local,bm=read_binance_bbo(day,end_us,symbol="BTCUSDT")
        bt=visible_timestamps(b_local,BINANCE_DELAY_MS)
        grid,basis,sigma,_=exact_moments_5s(
            bt,bm,ot,om,trade_start_us*1000,trade_stop_us*1000)
        pre=om[ot<trade_start_us*1000]
        cfg=scenario(float(np.median(pre)),spec)
        fused=root/"fused.npz"
        events=tardis.convert_fuse(
            str(paths["trades"]),str(paths["incremental_book_L2"]),
            str(paths["book_ticker"]),tick_size=cfg["tick"],
            lot_size=float(cfg["lot"]),output_filename=str(fused),
            ss_buffer_size=max(1000,counts["incremental_book_L2"]["snapshot_rows"]+100),
            snapshot_mode="process")
        validate_event_order(events)
        del events
        low=float(np.min(om));high=float(np.max(om))
        # The original implementation is frozen at the exact Stage1 completion
        # commit. It remains a reference even after production imports migrate.
        original_source = subprocess.check_output(
            ["git","show",
             "1b709c5e25874da91b80cc0307a5a4c16ffeb992:scripts/gate_origin_glft_strategy.py"],
            text=True)
        here=Path(__file__).resolve().parent
        frozen_path=root/"frozen_gate_origin_1b709c5.py"
        frozen_path.write_text(original_source,encoding="utf-8")
        spec=importlib.util.spec_from_file_location("frozen_gate_origin_1b709c5",frozen_path)
        frozen_module=importlib.util.module_from_spec(spec)
        spec.loader.exec_module(frozen_module)
        functions=(frozen_module.run_gate_origin_glft,run_core_glft)
        if trace_actions:
            functions=(
                instrument_action_requests(
                    frozen_path,root/"trace_legacy.py","run_gate_origin_glft"),
                instrument_action_requests(
                    here/"core_glft_engine.py",root/"trace_core.py","run_core_glft"))
        for beta,mode in ((0.,0),(1.,1),(1.,2)):
            values=[]
            traces=[]
            for function in functions:
                engine=ROIVectorMarketDepthBacktest(
                    [build_asset([fused],low,high,cfg)])
                rec=Recorder(engine.num_assets,minutes*600+10)
                try:
                    output=function(
                        engine,bt,bm,minutes*600,beta,grid,basis,sigma,mode,
                        cfg["gamma_code"],cfg["tick"],cfg["ctval"],
                        cfg["qty"],cfg["position_cap"],rec.recorder,
                        trade_stop_us*1000)
                    if trace_actions:
                        raw, request_trace = output
                        traces.append(request_trace)
                    else:
                        raw = output
                    recording=rec.get(0).copy()
                    values.append((raw,recording))
                finally:
                    engine.close()
            assert_exact_legacy_result(
                values[0][0],values[1][0],values[0][1],values[1][1])
            if trace_actions:
                if not np.array_equal(traces[0],traces[1]):
                    left,right=traces
                    if len(left)!=len(right):
                        raise AssertionError("Unequal number of exact order requests")
                    # Rust HashMap iteration is not ordered: independent HBT
                    # instances can issue concurrent cancels in either order.
                    # Preserve EVERY request's nanosecond, verb, ID and price.
                    # Normalize ONLY the permutation inside a single timestamp.
                    def normalize(x):
                        order=np.lexsort((x[:,3],x[:,2],x[:,1],x[:,0]))
                        return x[order]
                    norm_old,norm_new=normalize(left),normalize(right)
                    if not np.array_equal(norm_old,norm_new):
                        ix=np.flatnonzero(np.any(norm_old!=norm_new,axis=1))
                        at=int(ix[0]) if len(ix) else -1
                        print("ACTION_REQUEST_CONTENT_DIVERGENCE",
                              day,beta,mode,"index",at,
                              "old",norm_old[at].tolist() if at>=0 else None,
                              "core",norm_new[at].tolist() if at>=0 else None,
                              flush=True)
                        raise AssertionError("Order request content differs at exact timestamp")
                    print("CORE_LEGACY_SAME_NANOSECOND_REQUEST_PERMUTATION",
                          day,beta,mode,
                          "position_differences",int(np.sum(np.any(left!=right,axis=1))),
                          "request_count",len(left),flush=True)
                else:
                    print("CORE_LEGACY_REQUEST_SEQUENCE_IDENTICAL",
                          day,beta,mode,len(traces[0]),flush=True)
                self_count = len(traces[0])
                if self_count != values[0][0][7] + values[0][0][8]:
                    raise AssertionError("Request trace misses a submit/cancel action")
                print("CORE_LEGACY_EXACT_REQUEST_TRACE",day,beta,mode,self_count,flush=True)
            print("CORE_LEGACY_MATCH_MODEL",day,beta,mode,flush=True)
        compare_stage_wrappers_on_replay({
            "cfg":cfg,"fused":fused,"low":low,"high":high,"bt":bt,"bm":bm,
            "grid":grid,"basis":basis,"sigma":sigma,"minutes":minutes,
            "stop_ns":trade_stop_us*1000},root,trace_actions)


def compare_stage_wrappers_on_replay(data, root, trace_actions):
    """Additional Stage3/Stage4/Stage5A original-vs-core full request parity.

    Data contains converted HBT events and preprocessing outputs; no feed
    is re-downloaded for these cases.
    """
    from hftbacktest import Recorder, ROIVectorMarketDepthBacktest
    cases=[
        ("stage3_shadow","gate_stage3_shadow_strategy.py",
         "run_gate_stage3_shadow",0.0,0,{"shadow_gate":True}),
        ("stage4_reference","gate_stage4_controls.py",
         "run_gate_stage4_control",0.0,0,{"control_kind":0}),
        ("stage4_widen","gate_stage4_controls.py",
         "run_gate_stage4_control",0.0,0,{"control_kind":1,"extra_half_ticks":15.471552}),
        ("stage4_pause","gate_stage4_controls.py",
         "run_gate_stage4_control",0.0,0,{"control_kind":2,"local_sigma_ceiling":43.510298}),
        ("stage5a_pause","gate_stage5a_pause_strategy.py",
         "run_gate_stage5a_pause",1.0,3,{}),
    ]
    here=Path(__file__).resolve().parent
    for name,filename,fn_name,beta,mode,extra in cases:
        src=subprocess.check_output([
            "git","show",
            "1b709c5e25874da91b80cc0307a5a4c16ffeb992:scripts/"+filename],
            text=True)
        frozen_path=root/f"frozen_{name}.py"
        frozen_path.write_text(src,encoding="utf-8")
        funcs=[]
        if trace_actions:
            funcs.append(instrument_action_requests(
                frozen_path,root/f"trace_{name}.py",fn_name))
            funcs.append(instrument_action_requests(
                here/"core_glft_engine.py",root/f"trace_core_{name}.py","run_core_glft"))
        else:
            spec=importlib.util.spec_from_file_location("frozen_"+name,frozen_path)
            frozen_module=importlib.util.module_from_spec(spec)
            spec.loader.exec_module(frozen_module)
            funcs=[getattr(frozen_module,fn_name),run_core_glft]
        results=[]
        for index,fn in enumerate(funcs):
            cfg=data["cfg"]
            eng=ROIVectorMarketDepthBacktest([
                build_asset([data["fused"]],data["low"],data["high"],cfg)])
            rec=Recorder(eng.num_assets,data["minutes"]*600+10)
            try:
                output=fn(eng,data["bt"],data["bm"],data["minutes"]*600,beta,
                          data["grid"],data["basis"],data["sigma"],mode,
                          cfg["gamma_code"],cfg["tick"],cfg["ctval"],
                          cfg["qty"],cfg["position_cap"],rec.recorder,
                          data["stop_ns"],**extra)
                raw, actions = output if trace_actions else (output, None)
                results.append((raw,rec.get(0).copy(),actions))
            finally:
                eng.close()
        assert_exact_legacy_result(results[0][0],results[1][0],
                                   results[0][1],results[1][1])
        if trace_actions:
            left,right=results[0][2],results[1][2]
            def canonical(x):
                return x[np.lexsort((x[:,3],x[:,2],x[:,1],x[:,0]))]
            if not np.array_equal(canonical(left),canonical(right)):
                raise AssertionError(f"Stage-specific request mismatch: {name}")
            print("STAGE_OLD_MODE_EXACT_REQUEST_CONTENT",name,len(left),flush=True)
        print("STAGE_OLD_MODE_LEGACY_PARITY",name,flush=True)


if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--day",default="2026-11-01")
    p.add_argument("--minutes",type=int,default=50)
    p.add_argument("--trace-actions",action="store_true")
    x=p.parse_args()
    run_one(x.day,x.minutes,x.trace_actions)
