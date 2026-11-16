"""Gate 1.645sigma stage3: full-day, immutable HBT baseline with shadow signal probes.

Full source feeds are downloaded/replayed by official HBT; queue and matching
are unchanged. Baseline trading result MUST match frozen official archive
bit-for-bit on fill timestamps/sides/prices/quantities and strict end equity.
"""
import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import numpy as np

from run_gate_btc_origin_glft import (
    SCHEMAS, safe_gate_csv, require_unshifted_target_clock,
    load_gate_mid, read_binance_bbo, visible_timestamps, exact_moments_5s,
    scenario, fetch_contract, build_asset, BINANCE_DELAY_MS,
)
from gate_stage3_shadow_strategy import run_gate_stage3_shadow
from g0_glft_event_driven import summarize

MIN_CANCEL_NS = 20_000_000
ORDER_REQUEST_NS = 1_000_000
GUARD_NS = 1_000
STEP_NS = 100_000_000
HORIZONS = (100,500,1000)
GROUPS = (
    "inactive_or_no_effect",
    "threatened",
    "opposite",
)
QUOTE_POLICY = "benchmark_Gate_GLFT_shadow_full_depth"


def strict_fill_arrays(path):
    with np.load(path, allow_pickle=False) as f:
        return {key: f[key].copy() for key in
                ("exchange_timestamp_ns","side","local_observed_ns",
                 "exec_price","exec_qty")}


def assert_exact_replay(raw, frozen, fee_equity, fees, end_position, position_cap, order_qty):
    actual = {
        "exchange_timestamp_ns": raw[-5],
        "side": raw[-4],
        "local_observed_ns": raw[-2][1],
        "exec_price": raw[-2][2],
        "exec_qty": raw[-2][3],
    }
    for k,reference in frozen.items():
        a = actual[k]
        if (a.shape != reference.shape
                or not np.array_equal(a, reference)):
            mismatch=np.flatnonzero(a != reference)[:3] if a.shape == reference.shape else []
            raise ValueError(f"Frozen HBT fill parity failed: {k}, "
                             f"actual={a.shape}, expected={reference.shape}, "
                             f"mismatch={mismatch}")
    summary = summarize(raw,1430,True,0.0,position_cap,order_qty)
    t=summary["trading"]
    for label,a,b in (
        ("equity", t["end_net_marked_equity_usdt"], fee_equity),
        ("fees", t["fee_usdt"], fees),
        ("position", t["end_position_contracts"], end_position),
    ):
        if not math.isclose(a,b,rel_tol=0,abs_tol=1e-7):
            raise ValueError(f"Frozen HBT {label} parity failed: {a} != {b}")
    return actual, t


def probe_deadlines(frozen):
    exch=np.asarray(frozen["exchange_timestamp_ns"],dtype=np.int64)
    deadline=exch - ORDER_REQUEST_NS - GUARD_NS
    unique, inverse=np.unique(deadline,return_inverse=True)
    if len(unique)<2 or np.any(np.diff(unique)<=0):
        raise ValueError("Probe grid invalid")
    return unique,inverse


def pair_fill_ids(baseline, comparison):
    """Strict multiset pairing, exchange execution clock + direction."""
    from collections import defaultdict,deque
    remain=defaultdict(deque)
    n=len(comparison["side"])
    for j in range(n):
        remain[(int(comparison["exchange_timestamp_ns"][j]),
                int(comparison["side"][j]))].append(j)
    shared=np.zeros(len(baseline["side"]),dtype=bool)
    for i in range(len(shared)):
        key=(int(baseline["exchange_timestamp_ns"][i]),
             int(baseline["side"][i]))
        if remain[key]:
            remain[key].popleft()
            shared[i]=True
    return shared


def signal_categories(fills, rows, inverse, probes):
    n=len(fills["side"])
    if rows.shape!=(len(probes),25) or len(inverse)!=n:
        raise ValueError("Stage3 shadow probe buffer malformed")
    r=rows[inverse]
    side=np.asarray(fills["side"],dtype=np.int64)
    effective=r[:,7]
    # A: target-risk side: positive gap -> SELL, negative gap -> BUY.
    active=np.isfinite(effective)&(effective!=0)&(r[:,10]>0.5)
    threatened=active & (((effective>0)&(side==-1)) | ((effective<0)&(side==1)))
    opposite=active & ~threatened
    category=np.where(threatened,"threatened",
               np.where(opposite,"opposite","inactive_or_no_effect"))
    order_tick=np.where(side==1,r[:,0],r[:,1])
    n_cancellable=np.where(side==1,r[:,4],r[:,5])
    action_time=np.where(side==1,r[:,18],r[:,19])
    shadow_tick=np.where(side==1,r[:,16],r[:,17])
    deadline=fills["exchange_timestamp_ns"]-ORDER_REQUEST_NS-GUARD_NS
    # HBT local order state, theoretical ability to request cancel before fill.
    has_order=(n_cancellable>0) & np.isfinite(order_tick)
    quote_would_change=has_order & np.isfinite(shadow_tick) & (order_tick!=shadow_tick)
    allowed_by_rate=deadline-action_time >= MIN_CANCEL_NS
    can_attempt=(threatened & has_order & quote_would_change & allowed_by_rate)
    # Activation must be at strictly earlier decision, not on the fill clock.
    activation_age_ms=np.where(
        (r[:,12]>=0) & (r[:,12] < deadline),
        (deadline-r[:,12])/1_000_000,
        np.nan)
    last_decision_age_ms=np.where(
        (r[:,11]>=0) & (r[:,11] <= deadline),
        (deadline-r[:,11])/1_000_000,
        np.nan)
    return {
        "category": category,
        "threatened": threatened,
        "opposite": opposite,
        "effective_gap_usdt": effective,
        "observed_gap_usdt": r[:,8],
        "sigma_usdt": r[:,9],
        "shadow_signal_active": active,
        "shadow_last_decision_age_ms": last_decision_age_ms,
        "shadow_activation_age_ms": activation_age_ms,
        "order_cancellable_in_local_engine": has_order,
        "shadow_quote_would_change": quote_would_change,
        "side_action_rate_ready": allowed_by_rate,
        "theoretical_pre_fill_cancel_request": can_attempt,
        "snapshot_bid_price_tick": r[:,0],
        "snapshot_ask_price_tick": r[:,1],
        "shadow_target_bid_tick": r[:,16],
        "shadow_target_ask_tick": r[:,17],
        "order_request_deadline_ns": deadline,
    }


def group_stat(x, valid, markout):
    idx=valid
    vals=markout[idx].astype(np.float64)
    vals=vals[np.isfinite(vals)]
    if not len(vals):
        return {"n":0,"mean_markout_bps":None,"negative_fraction":None,
                "p10_bps":None,"worst10_mean_bps":None}
    count=max(1,int(math.ceil(0.1*len(vals))))
    return {"n":len(vals),
            "mean_markout_bps":float(np.mean(vals)),
            "negative_fraction":float(np.mean(vals<0)),
            "p10_bps":float(np.quantile(vals,.1)),
            "worst10_mean_bps":float(np.mean(np.sort(vals)[:count]))}


def summarize_classified(day, baseline, variant, annotated, diagnostics):
    from collections import Counter
    n=len(baseline["side"])
    for key in ("exchange_timestamp_ns","side","local_observed_ns","exec_price","exec_qty"):
        if not np.array_equal(annotated[key],baseline[key]):
            raise ValueError(f"Stage2 markout not aligned with frozen baseline: {key}")
    shared=pair_fill_ids(baseline,variant)
    count=Counter(diagnostics["category"].tolist())
    out={"day":day,"total_fills":n,"shared_same_clock_side":int(shared.sum()),
         "baseline_only":int((~shared).sum()),
         "categories":{},"day_comparisons":{}}
    for h in HORIZONS:
        markout=annotated[f"markout_{h}ms_bps"]
        fresh=annotated[f"fresh_{h}ms"].astype(bool)
        one={}
        for group in GROUPS:
            selected=(diagnostics["category"]==group)
            one[group]={
                "all_fills":int(selected.sum()),
                "baseline_only":int(np.sum(selected & ~shared)),
                "shared":int(np.sum(selected & shared)),
                "fresh":group_stat(markout,selected & fresh,markout),
                "baseline_only_fresh":group_stat(markout,selected & ~shared & fresh,markout),
                "shared_fresh":group_stat(markout,selected & shared & fresh,markout),
            }
            if group=="threatened":
                c=diagnostics["theoretical_pre_fill_cancel_request"] & selected
                one[group]["theoretical_cancellable_count"]=int(c.sum())
                one[group]["theoretical_cancellable_baseline_only"]=int(np.sum(c&~shared))
                one[group]["theoretical_cancellable_fresh"]=group_stat(markout,c&fresh,markout)
        out["categories"][str(h)]=one
    out["shadow_total_effective_at_deadline"]=int(np.sum(diagnostics["shadow_signal_active"]))
    out["shadow_total_pre_fill_theoretical_cancellable"]=int(np.sum(diagnostics["theoretical_pre_fill_cancel_request"]))
    return out


def day_run(day, frozen_dir, annotated_dir, output):
    from hftbacktest import Recorder, ROIVectorMarketDepthBacktest
    from hftbacktest.data.utils import tardis
    from hftbacktest.data.validation import validate_event_order
    from tardis_segmented_hbt import convert_fuse_parts

    date=datetime.strptime(day,"%Y-%m-%d").replace(tzinfo=timezone.utc)
    start_us=int(date.timestamp()*1_000_000)
    trade_start=start_us+600_000_000
    trade_stop=start_us+86_400_000_000
    output.mkdir(parents=True,exist_ok=True)
    report=json.loads((frozen_dir/f"{day}_1430m.json").read_text())
    if (report["status"]!="complete" or report["minutes_traded"]!=1430
            or report["experimental_trigger_sigma_multiple"]!=1.645
            or report["binance_extra_visible_delay_ms"]!=2.5
            or report["scenario"]["maker_fee_rate"]!=0.0002):
        raise ValueError("Frozen full-day scenario mismatch")
    frozen=strict_fill_arrays(frozen_dir/"recorder"/day/"glft"/"fills.npz")
    comparison=strict_fill_arrays(frozen_dir/"recorder"/day/"both_1p0"/"fills.npz")
    with np.load(annotated_dir/f"{day}_glft_annotated.npz") as d:
        annotated={key:d[key].copy() for key in d.files}
    probes,inverse=probe_deadlines(frozen)
    with tempfile.TemporaryDirectory(prefix="gate_stage3_") as temp:
        root=Path(temp)
        spec=fetch_contract()
        paths={};counts={}
        for kind in SCHEMAS:
            path=root/(kind+".csv")
            print("STAGE3_DOWNLOAD",day,kind,flush=True)
            counts[kind]=safe_gate_csv(kind,day,trade_stop,path,90_000_000)
            paths[kind]=path
        require_unshifted_target_clock(counts)
        for k in counts:
            original=report["feed_stats"][k]
            for field in ("rows","first_local_us","last_local_us"):
                if counts[k][field]!=original[field]:
                    raise ValueError(f"Input feed changed: {k} {field}")
        gate_t,gate_mid=load_gate_mid(paths["book_ticker"])
        bin_t,bin_mid=read_binance_bbo(day,trade_stop,symbol="BTCUSDT")
        visible=visible_timestamps(bin_t,BINANCE_DELAY_MS)
        grid,basis,sigma,cover=exact_moments_5s(
            visible,bin_mid,gate_t,gate_mid,trade_start*1000,trade_stop*1000)
        cfg=scenario(float(np.median(gate_mid[gate_t<trade_start*1000])),spec)
        for key in ("tick","ctval","qty","position_cap","gamma_code",
                    "maker_fee_rate","taker_fee_rate"):
            if not math.isclose(cfg[key],report["scenario"][key],abs_tol=1e-12):
                raise ValueError(f"Frozen contract/scenario changed: {key}")
        parts,details=convert_fuse_parts(
            paths,counts,root,start_us,trade_stop,60,
            cfg["tick"],float(cfg["lot"]))
        low=float(np.min(gate_mid));high=float(np.max(gate_mid))
        engine=ROIVectorMarketDepthBacktest([build_asset(parts,low,high,cfg)])
        try:
            rec=Recorder(engine.num_assets,1430*600+10)
            raw=run_gate_stage3_shadow(
                engine,visible,bin_mid,1430*600,0.0,
                grid,basis,sigma,0,cfg["gamma_code"],
                cfg["tick"],cfg["ctval"],cfg["qty"],cfg["position_cap"],
                rec.recorder,trade_stop*1000,probes,True)
            t=report["variants"]["glft"]["trading"]
            actual,vals=assert_exact_replay(
                raw,frozen,t["end_net_marked_equity_usdt"],
                t["fee_usdt"],t["end_position_contracts"],cfg["position_cap"],cfg["qty"])
            if int(raw[5])!=1:
                raise ValueError("GLFT calibration stopped working")
            probe_rows=raw[-2][4]
            label=signal_categories(frozen,probe_rows,inverse,probes)
            day_summary=summarize_classified(day,frozen,comparison,annotated,label)
            # Diagnostic event counts are from baseline shadow; reference beta1
            # native signal telemetry from frozen files for contextual comparison.
            keys=("binance_price_update_groups","eligible_gap_checks",
                  "above_1p96sigma_checks","directional_dominance_checks",
                  "binance_origin_activations","active_decisions",
                  "same_timestamp_ties_blocked")
            day_summary["shadow_baseline_counters"]={
                k:int(v) for k,v in zip(keys,raw[-1])}
            day_summary["frozen_beta1_native_counters"]=report["variants"]["both_1p0"]["binance_origin_filter"]
            day_summary["exact_parity"]={"all_fills":len(frozen["side"]),
                "baseline_fee_net_usdt":vals["end_net_marked_equity_usdt"],
                "baseline_maker_fee_usdt":vals["fee_usdt"],
                "baseline_end_position_contracts":vals["end_position_contracts"]}
            day_summary["input_gates"]={
                "Tardis_native_quotes_from_frozen_source":True,
                "HBT_baseline_fill_arrays_exactly_equal":True,
                "HBT_baseline_net_exactly_equal_within_1e-7":True,
                "HBT_fees_exactly_equal_within_1e-7":True,
                "proxy_uses_HBT_fused_depth_for_G":True,
                "probe_has_no_order_action":True}
            # Fill-aligned labels; only HBT orders are matched by official engine.
            np.savez_compressed(output/f"{day}_stage3_classified.npz",
                **label,baseline_only=~pair_fill_ids(frozen,comparison),
                exchange_timestamp_ns=frozen["exchange_timestamp_ns"],
                side=frozen["side"],
                markout_100ms_bps=annotated["markout_100ms_bps"],
                fresh_100ms=annotated["fresh_100ms"])
            (output/f"{day}_stage3.json").write_text(
                json.dumps(day_summary,indent=2,ensure_ascii=False)+"\n",encoding="utf-8")
            print("STAGE3_DAY_PARITY_AND_SHADOW_OK",day,
                  "fills",len(frozen["side"]),
                  "categories",{g:day_summary["categories"]["100"][g]["all_fills"] for g in GROUPS},
                  "can_cancel",day_summary["shadow_total_pre_fill_theoretical_cancellable"],
                  flush=True)
        finally:
            engine.close()


if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--day",required=True)
    p.add_argument("--frozen",required=True)
    p.add_argument("--annotated",required=True)
    p.add_argument("--out",required=True)
    a=p.parse_args()
    day_run(a.day,Path(a.frozen),Path(a.annotated),Path(a.out))
