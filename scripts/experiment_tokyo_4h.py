"""Pure-Tokyo four-hour exploratory HBT run; only the three locked month-first days.

Order-id / cancel-ACK attribution is conservative: same HBT synthetic order ID,
original submit timestamp and qty in both replays, defense-side cancellation
ACK locally observed before the G0 exchange fill. Nonmatches are not "avoided".
The engine itself always uses official HBT; no home-built matching.
"""
import argparse
import bisect
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
from hftbacktest.data.utils import tardis
from hftbacktest.data.validation import validate_event_order
from check_okx_hft_data import SCHEMAS, extract_window, require_unshifted_target_clock
from cross_venue_event_probe import read_binance_bbo, visible_timestamps
from run_beta_one_direct_gap import causal_60s_basis, read_local_bbo
from run_multivenue_glft import order_scenario
from experiment_source_defense_signal import past_grid_time_ema
from experiment_source_defense_trade_flow import read_binance_taker_trades
from experiment_source_defense_v5_multiday import market, prior_from_stored_admission
from experiment_source_defense_v6_mechanism import model_run, groups, summarize_mask
from experiment_source_defense_pilot import classify_opportunities

DAYS = ("2026-09-01", "2026-10-01", "2026-11-01")
MARKETS = (("SOL","okex-swap"),("DOGE","okex-swap"),
           ("1000PEPE","okex-swap"),("1000PEPE","bitget-futures"))
WARMUP_S = 1200
TRADE_STEPS = 144000
DURATION_S = 14400
MAX_ROWS_PER_FEED = 8_000_000
DELAY_MS = (0.,1.,2.5)
GAMMA_SCALES = (1.,1.5)


def prepared_four_hours(day, coin, venue, root):
    """Same V5 data and conversion contracts, extended only to 00:20-04:20."""
    if day not in DAYS or (coin,venue) not in MARKETS:
        raise ValueError("Four-hour scope excludes unseen dates and markets")
    spec,symbol,divisor = market(coin,venue)
    day_us=int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp()*1e6)
    end_us=day_us+(WARMUP_S+DURATION_S+5)*1_000_000
    counts={}
    for kind in SCHEMAS:
        counts[kind]=extract_window(kind,day,end_us,root/(kind+".csv"),
              exchange=spec.exchange,symbol=spec.symbol,max_rows=MAX_ROWS_PER_FEED)
    require_unshifted_target_clock(counts)
    tape=root/"fused.npz"
    events=tardis.convert_fuse(str(root/"trades.csv"),
             str(root/"incremental_book_L2.csv"),str(root/"book_ticker.csv"),
             tick_size=spec.tick,lot_size=spec.lot,output_filename=str(tape),
             ss_buffer_size=max(1200,counts["incremental_book_L2"]["snapshot_rows"]+100),
             snapshot_mode="process")
    validate_event_order(events)
    n_events=len(events)
    del events
    ot,om=read_local_bbo(root/"book_ticker.csv")
    braw,bmid=read_binance_bbo(day,end_us,symbol=symbol)
    bt=visible_timestamps(braw,2.5)
    bm=bmid/divisor
    bt0=np.empty(0,np.int64);bm0=np.empty(0,np.float64)
    bt_bas,basis=causal_60s_basis(ot,om,bt,bm)
    ema_t,ema=past_grid_time_ema(ot,om,bt,bm,1.)
    warm_end=(day_us+WARMUP_S*1_000_000)*1000
    if not np.any(np.isfinite(ema[ema_t<warm_end])):
        raise ValueError("No causal EMA during 20-minute pretrade")
    tt,ss,vv,trade_qa=read_binance_taker_trades(
        day,end_us,symbol,2.5,max_rows=MAX_ROWS_PER_FEED)
    scenario=order_scenario(spec,float(np.median(om[ot<warm_end])))
    return {
        "spec":spec,"symbol":symbol,"divisor":divisor,
        "start_ns":day_us*1000,"start_us":day_us,"pretrade_s":WARMUP_S,
        "trade_start_utc":"00:20","trade_end_utc":"04:20",
        "tape":tape,"low":float(np.min(om)),"high":float(np.max(om)),
        "ot":ot,"om":om,"bt":bt,"bm":bm,"bt0":bt0,"bm0":bm0,
        "basis_t":bt_bas,"basis":basis,"ema_t":ema_t,"ema":ema,
        "tt":tt,"ss":ss,"vv":vv,"trade_qa":trade_qa,
        "scenario":scenario,"data_counts":counts,"fused_events":n_events}


def with_delay(d, delay):
    if delay not in DELAY_MS:raise ValueError("Only predeclared extra delays")
    x=dict(d)
    x["bt"]=d["bt"]+round((delay-2.5)*1_000_000)
    x["tt"]=d["tt"]+round((delay-2.5)*1_000_000)
    x["basis_t"],x["basis"]=causal_60s_basis(x["ot"],x["om"],x["bt"],x["bm"])
    x["ema_t"],x["ema"]=past_grid_time_ema(x["ot"],x["om"],x["bt"],x["bm"],1.)
    return x


def compact(r,trace):
    tr=r["trading"]
    st,en,side,urgent_t,urgent_side=trace
    if len(st)!=len(en) or len(st)!=len(side):
        raise AssertionError("Truncated defense events")
    return {"fills":tr["engine_fills"],"cancel_requests":tr["cancel_requests"],
            "submissions":tr["submissions"],
            "fee_usdt":tr["fee_usdt"],"turnover_usdt":tr["trading_value_usdt"],
            "gross_marked_usdt":tr["end_net_marked_equity_usdt"]+tr["fee_usdt"],
            "net_after_2bps_usdt":tr["end_net_marked_equity_usdt"],
            "terminal_inventory":tr["end_position_contracts"],
            "defense_episodes":int(len(st)),
            "defense_active_ms":float(r.get("defense_duty",{}).get("active_ns",0)/1e6)
                if "defense_duty" in r else float(np.sum(en-st)/1e6),
            "defense_duty_fraction":float(np.sum(en-st)/(TRADE_STEPS*100_000_000)),
            "urgent_cancel_requests":int(len(urgent_t)),
            "initial_GLFT":r["initial_parameters"],
            "last_GLFT":r["last_parameters"]}


def exact_ack_attribution(g0,sf,gt,st,s_episode):
    """Lower-bound matching exactly same submitted order from two HBT replays.

    g0/sf are annotated fills (order trace order is one-to-one by fill index).
    gt/st are raw[39] order trace tuples, s_episode is raw[38].
    """
    drop,keep=classify_opportunities(g0,sf)
    g_oid=np.asarray(gt[0],np.int64)
    if len(g_oid)!=len(drop):
        raise AssertionError("G0 fill ID trace and annotated fills out of alignment")
    base_sub=defaultdict(list)
    for oid,t,q in zip(gt[5],gt[6],gt[7]):
        base_sub[int(oid)].append((int(t),float(q)))
    source_sub=defaultdict(list)
    for oid,t,q in zip(st[5],st[6],st[7]):
        source_sub[int(oid)].append((int(t),float(q)))
    for seq in (base_sub,source_sub):
        for key in seq:seq[key].sort()
    cancel_by_oid=defaultdict(list)
    request_t=s_episode[3];dangerous=s_episode[4]
    for ix,(oid,price,q,ack) in enumerate(zip(st[1],st[2],st[3],st[4])):
        if int(ack)>=0:
            cancel_by_oid[int(oid)].append((int(request_t[ix]),int(ack),
                                            int(dangerous[ix]),int(price),float(q)))
    for oid in cancel_by_oid:cancel_by_oid[oid].sort()

    confirmed=np.zeros(len(drop),bool)
    candidate_ack=np.zeros(len(drop),bool)
    for i in np.flatnonzero(drop):
        oid=int(g_oid[i])
        fill_time=int(g0["exchange_timestamp_ns"][i])
        side=int(g0["side_sign"][i])
        val=float(g0["exec_qty"][i])
        submits=base_sub.get(oid,[])
        if not submits:continue
        bi=bisect.bisect_left(submits,(fill_time,-float("inf")))-1
        if bi<0:continue
        original_t,original_qty=submits[bi]
        if not np.isclose(original_qty,val,rtol=0,atol=1e-9):continue
        src_submits=source_sub.get(oid,[])
        if not any(t==original_t and abs(q-original_qty)<=1e-9 for t,q in src_submits):
            continue
        for req,ack,direction,price,qty in cancel_by_oid.get(oid,[]):
            if (req>=original_t and req<ack<fill_time and direction==-side
                and abs(qty-original_qty)<=1e-9 and
                price==int(round(g0["exec_price"][i]/(
                    g0.get("tick_size",1.))))):
                # Disallow a source submission after the identical original
                # submission and before this cancel request (reused HBT ID).
                if any(original_t<t<=req for t,_ in src_submits):
                    continue
                # Defense must be active at request, no 200ms tail shortcut.
                start,end,dside=s_episode[:3]
                pos=np.searchsorted(start,req,side="right")-1
                if pos>=0 and req<end[pos] and int(dside[pos])==-side:
                    confirmed[i]=True
                    break
            if req<ack<fill_time and direction==-side:
                candidate_ack[i]=True
    ambiguous=drop&~confirmed
    result={
        "strict_same_submit_order_id_cancel_ACK_before_G0_fill":summarize_mask(g0,confirmed),
        "G0_only_not_strictly_attributed":summarize_mask(g0,ambiguous),
        "G0_and_defense_kept":summarize_mask(g0,keep),
        "new_strategy_added":summarize_mask(sf,classify_opportunities(sf,g0)[0]),
        "candidate_acked_same_price_derived_id_without_full_lineage":int(np.sum(candidate_ack&ambiguous)),
        "ack_conservative_local_before_G0_exchange_time":True,
        "HBT_order_id_is_price_derived_and_can_be_reused":True,
        "no_claim_about_unmatched_queue_loss_causality":True,
        "not_cross_exchange_real_ack":True
    }
    if result["strict_same_submit_order_id_cancel_ACK_before_G0_fill"]["count"]+result["G0_only_not_strictly_attributed"]["count"]!=int(drop.sum()):
        raise AssertionError("Strict/non-strict dropped partition invalid")
    return result


def run(day,coin,venue,out,admission,stage="core"):
    if day not in DAYS or (coin,venue) not in MARKETS:
        raise ValueError("Only locked Tokyo explored dates")
    if stage not in ("core","z075","delay"):raise ValueError("Invalid predefined stage")
    prior,prior_meta=(prior_from_stored_admission(admission,coin,venue,day)
                      if coin in ("SOL","DOGE") else (np.nan,{"fit_kind":"5plus_20min"}))
    with TemporaryDirectory(prefix="tokyo_4h_") as work:
        d=prepared_four_hours(day,coin,venue,Path(work))
        result={"date":day,"coin":coin,"venue":venue,"stage":stage,
                "training_utc":"00:00–00:20","trading_utc":"00:20–04:20",
                "trade_steps":TRADE_STEPS,"pretrade_minutes":20,
                "delay_ms":2.5,"maker_fee_bps":2.,
                "gamma_standard":"50 USDT^-1 as encoded by market specification",
                "gamma_robust": "75 USDT^-1 (1.5x) only in gamma-robust cases",
                "prior":prior_meta,"data_counts":d["data_counts"],
                "binance_trade_qa":d["trade_qa"],
                "fused_events":d["fused_events"],
                "variants":{}}
        baseline_trace=[]
        base,bf,be=model_run(d,"g0",prior,trade_steps=TRADE_STEPS,
                               order_trace_sink=baseline_trace)
        bf["tick_size"]=d["spec"].tick
        high_gamma_baseline=None
        result["variants"]["g0"]=compact(base,be)
        if stage=="core":
            cases=(("z1","source",1.),
                   ("z1_p80","source_p80",1.),
                   ("g0_gamma75","g0",1.5),
                   ("z1_gamma75","source",1.5))
        elif stage=="z075":
            if coin not in ("SOL","DOGE"):raise ValueError("Z0.75 only for sparse SOL/DOGE")
            cases=(("z075","source_z075",1.),)
        else:
            cases=tuple((f"z1_delay_{ms:g}ms","source",1.) for ms in DELAY_MS)
        for name,mode,gamma_scale in cases:
            if mode=="g0" and gamma_scale==1:
                raise AssertionError("Duplicate G0")
            extra=float(name.split("_delay_")[-1][:-2]) if "_delay_" in name else 2.5
            dd=with_delay(d,extra) if extra!=2.5 else d
            order_trace=[]
            stat,ff,episode=model_run(
                dd,mode,prior,gamma_scale=gamma_scale,
                trade_steps=TRADE_STEPS,order_trace_sink=order_trace)
            ff["tick_size"]=d["spec"].tick
            comp=compact(stat,episode)
            comp["retention_vs_standard_g0"]=comp["fills"]/max(result["variants"]["g0"]["fills"],1)
            comp["cancel_delta_vs_standard_g0"]=comp["cancel_requests"]-result["variants"]["g0"]["cancel_requests"]
            comp["gross_delta_vs_standard_g0_usdt"]=comp["gross_marked_usdt"]-result["variants"]["g0"]["gross_marked_usdt"]
            comparator_fills=bf
            comparator_trace=baseline_trace[0]
            if name=="g0_gamma75":
                high_gamma_baseline=(ff,order_trace[0])
            if name=="z1_gamma75":
                if high_gamma_baseline is None:
                    raise AssertionError("Gamma75 source must follow matched gamma75 G0")
                comparator_fills,comparator_trace=high_gamma_baseline
            if mode=="g0":
                comp["group_window_observational"]=None
                comp["order_level"]=None
            else:
                comp["group_window_observational"]=groups(comparator_fills,ff,episode,"exchange")
                comp["order_level"]=exact_ack_attribution(comparator_fills,ff,comparator_trace,order_trace[0],episode)
            if gamma_scale==1 and any(stat["initial_parameters"][k]!=base["initial_parameters"][k]
                                     for k in ("A_per_second","k_per_tick","sigma_tick_per_sqrt_second")):
                raise AssertionError("G0 vs source A/k/sigma mismatch")
            result["variants"][name]=comp
            print("TOKYO4H",day,coin,venue,name,"fills",comp["fills"],
                  "duty",comp["defense_duty_fraction"],
                  "net2bps",comp["net_after_2bps_usdt"],flush=True)
        out=Path(out);out.mkdir(parents=True,exist_ok=True)
        (out/"case.json").write_text(json.dumps(result,indent=2,ensure_ascii=False,allow_nan=False))
        return result


if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--day",required=True)
    p.add_argument("--coin",required=True)
    p.add_argument("--venue",required=True)
    p.add_argument("--stage",choices=("core","z075","delay"),default="core")
    p.add_argument("--admission",default="reports/source_defense/step3_fit/step3_fit_admission.json")
    p.add_argument("--out",required=True)
    a=p.parse_args()
    run(a.day,a.coin,a.venue,a.out,a.admission,a.stage)
