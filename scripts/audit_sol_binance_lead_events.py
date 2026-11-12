"""Raw-event audit: does Binance SOL move before unchanged OKX SOL quotes?

This diagnostic does not modify trading or use a synthetic matching engine.
A source event at local receipt time t is not visible to the model until
t+2.5ms+1ns; the OKX order request then requires a further simulated 1ms.
All prices are USDT per SOL. OKX tick is 0.01 USDT/SOL.
"""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np

from check_okx_hft_data import extract_window
from cross_venue_event_probe import read_binance_bbo, visible_timestamps
from run_beta_one_direct_gap import causal_60s_basis, read_local_bbo
from run_multivenue_glft import MARKETS

NS = 1_000_000_000
MAX_AGE_NS = 250_000_000
EXTRA_DELAY_NS = 2_500_001
TARGET = MARKETS[("okx", "SOL")]
assert TARGET.symbol == "SOL-USDT-SWAP"
assert TARGET.tick == .01 and TARGET.ctval == 1. and TARGET.lot == .01
THRESHOLDS_BPS = (1., 2., 5., 10.)
HORIZONS_NS = {"50ms": 50_000_000, "100ms": 100_000_000,
               "500ms": 500_000_000, "1000ms": NS}
SURVIVAL_NS = {"1ms":1_000_000,"2.5ms":2_500_001,"3.5ms":3_500_001,
               "5ms":5_000_000,"10ms":10_000_000,"20ms":20_000_000}


def asof(ts, when, inclusive=False):
    """Observed quote index; strict-before by default, equal permitted when requested."""
    return np.searchsorted(ts, when, side="right" if inclusive else "left") - 1


def stats(x):
    arr=np.asarray(x,dtype=float)
    arr=arr[np.isfinite(arr)]
    if not len(arr):
        return {"n":0}
    return {"n":len(arr),"p50":float(np.quantile(arr,.5)),
            "p90":float(np.quantile(arr,.9)),
            "p99":float(np.quantile(arr,.99)),
            "max":float(np.max(arr))}


def count_summary(candidate, segment_id, raw_ts, visible_okx_changed,
                  visible_good, survival_ns, event_ns, interval_ns,
                  raw_g_bps, visible_g_bps, raw_spread_bps):
    """One first threshold breach per constant-price OKX BBO segment."""
    ids=np.flatnonzero(candidate)
    if not len(ids):
        return {"event_count":0,"unique_okx_unchanged_segments":0,
                "visible_good_count":0,
                "visible_still_same_okx_price_count":0,
                "visible_gap_still_at_least_10bps_count":0,
                "next_okx_price_change_ms":stats([]),
                "first_per_okx_segment_next_change_ms":stats([]),
                "follow_survival":{k:{"n":0,"fraction":None} for k in SURVIVAL_NS},
                "examples":[]}
    segments=segment_id[ids]
    first=np.r_[True, segments[1:]!=segments[:-1]]
    unique=ids[first]
    # The diagnostic's per-message counts may include repeated events before
    # the same OKX change. The per-segment number counts only the FIRST.
    # If a new segment is seen later, it has a new integer segment ID.
    eligible_delayed=visible_good[ids]
    same = eligible_delayed & (~visible_okx_changed[ids])
    triggered = (same & (np.abs(visible_g_bps[ids]) >= 10.))
    out={
        "event_count":int(len(ids)),
        "unique_okx_unchanged_segments":int(len(unique)),
        "visible_good_count":int(np.count_nonzero(eligible_delayed)),
        "visible_still_same_okx_price_count":int(np.count_nonzero(same)),
        "visible_gap_still_at_least_10bps_count":int(np.count_nonzero(triggered)),
        "next_okx_price_change_ms":stats(interval_ns[ids]/1e6),
        "first_per_okx_segment_next_change_ms":stats(interval_ns[unique]/1e6),
        "follow_survival":{
            label:{"n":int(np.count_nonzero(interval_ns[unique]>delta)),
                   "fraction":float(np.mean(interval_ns[unique]>delta))}
            for label,delta in SURVIVAL_NS.items()
        },
        "examples":[{"binance_local_utc":datetime.fromtimestamp(
                      int(event_ns[i])/1e9,tz=timezone.utc).isoformat(),
                     "raw_bps":float(raw_spread_bps[i]),
                     "raw_G_bps":float(raw_g_bps[i]),
                     "visible_G_bps":(float(visible_g_bps[i])
                                      if np.isfinite(visible_g_bps[i]) else None),
                     "target_next_price_change_ms":float(interval_ns[i]/1e6)}
                    for i in unique[:5]]
    }
    return out


def run_one(day):
    base_us=int(datetime.strptime(day,"%Y-%m-%d").replace(
        tzinfo=timezone.utc).timestamp()*1_000_000)
    end_us=base_us+(3600+5)*1_000_000
    with TemporaryDirectory(prefix=f"sol_event_lead_{day}_") as tmp:
        path=Path(tmp)/"okx_bbo.csv"
        ex=extract_window("book_ticker",day,end_us,path,
                          max_rows=3_000_000,exchange=TARGET.exchange,
                          symbol=TARGET.symbol)
        ot,om=read_local_bbo(path)
        bt_raw,bm=read_binance_bbo(day,end_us,symbol="SOLUSDT")
    bt_visible=visible_timestamps(bt_raw,2.5)
    if not np.array_equal(bt_visible-bt_raw,
                          np.full(len(bt_raw),EXTRA_DELAY_NS,dtype=np.int64)):
        raise ValueError("Unexpected Binance extra delay semantics")
    baseline_time,baseline=causal_60s_basis(ot,om,bt_visible,bm)
    start=base_us*1000+600*NS
    stop=base_us*1000+3600*NS
    all_i=np.flatnonzero((bt_raw>=start)&(bt_raw<stop))
    # Source BBO messages with an actual change in Binance mid.
    moved=np.r_[False, bm[1:]!=bm[:-1]]
    ids=all_i[moved[all_i]]
    if len(ids)==0:
        raise ValueError("No source price-changing events")
    t=bt_raw[ids]
    tv=bt_visible[ids]
    okx_i=asof(ot,t)
    okx_v=asof(ot,tv)
    basis_i=asof(baseline_time,t)
    basis_v=asof(baseline_time,tv)
    o=np.maximum(okx_i,0)
    ov=np.maximum(okx_v,0)
    gi=np.maximum(basis_i,0)
    gv=np.maximum(basis_v,0)
    # A raw at-collector-snapshot signal and a post-delay target-state signal:
    # both require strictly past OKX BBO and strictly past completed basis.
    rawgood=((okx_i>=0)&(basis_i>=0)&
             (t-ot[o] >=0)&(t-ot[o]<=MAX_AGE_NS)&
             np.isfinite(baseline[gi]))
    visiblegood=((okx_v>=0)&(basis_v>=0)&
                 (tv-ot[ov]>=0)&(tv-ot[ov]<=MAX_AGE_NS)&
                 np.isfinite(baseline[gv]))
    raw_g=np.full(len(ids),np.nan)
    visible_g=np.full(len(ids),np.nan)
    raw_spread=np.full(len(ids),np.nan)
    raw_spread[rawgood]=1e4*(bm[ids[rawgood]]-om[o[rawgood]])/om[o[rawgood]]
    raw_g[rawgood]=1e4*(bm[ids[rawgood]]-om[o[rawgood]]-
                        baseline[gi[rawgood]])/om[o[rawgood]]
    visible_g[visiblegood]=1e4*(bm[ids[visiblegood]]-om[ov[visiblegood]]-
                               baseline[gv[visiblegood]])/om[ov[visiblegood]]
    target_changes=np.r_[True,om[1:]!=om[:-1]]
    change_ts=ot[target_changes]
    changes_before=asof(change_ts,t)
    # Time of NEXT target mid change. The strictly-after relation avoids
    # assuming exchange ordering for quotes sharing a local timestamp.
    next_ci=np.searchsorted(change_ts,t,side="right")
    target_follow_ns=np.full(len(ids),np.inf)
    has_next=next_ci<len(change_ts)
    target_follow_ns[has_next]=change_ts[next_ci[has_next]]-t[has_next]
    # Query after source event and up to signal visibility inclusive.
    changes_until_visibility=(
        np.searchsorted(change_ts,tv,side="right")-
        np.searchsorted(change_ts,t,side="right"))
    visibility_changed=changes_until_visibility>0
    target_segment=np.maximum(changes_before,0)
    out={"day":day,"okx_bbo_rows":len(ot),"binance_bbo_rows":len(bt_raw),
         "binance_price_change_events":int(len(ids)),
         "raw_valid_events":int(np.count_nonzero(rawgood)),
         "visible_valid_events":int(np.count_nonzero(visiblegood)),
         "raw_spread_absolute_bps":stats(np.abs(raw_spread)),
         "raw_G_absolute_bps":stats(np.abs(raw_g)),
         "visible_G_absolute_bps":stats(np.abs(visible_g)),
         "raw_spread_at_least_10bps":int(np.count_nonzero(rawgood&
                                                    (np.abs(raw_spread)>=10.))),
         "raw_G_at_least_10bps":int(np.count_nonzero(rawgood&
                                                    (np.abs(raw_g)>=10.))),
         "visible_G_at_least_10bps":int(np.count_nonzero(visiblegood&
                                                   (np.abs(visible_g)>=10.))),
         "raw_G_ge10_then_below10_after_delay":int(np.count_nonzero(
             rawgood&visiblegood&(np.abs(raw_g)>=10.)&(np.abs(visible_g)<10.))),
         "source_price_change_abs_bps":stats(1e4*np.abs(
             bm[ids]-bm[ids-1])/bm[ids-1]),
         "target_changes_during_binance_2p5ms_delay":int(np.count_nonzero(
             rawgood&visibility_changed)),
         "horizons":{}}
    for h_name,h_ns in HORIZONS_NS.items():
        anchor=t-h_ns
        ib=asof(bt_raw,anchor)
        ostart=asof(ot,anchor)
        bb=np.maximum(ib,0)
        oo=np.maximum(ostart,0)
        start_fresh=((ib>=0)&(ostart>=0)&
                     (anchor-bt_raw[bb] >=0)&(anchor-bt_raw[bb]<=MAX_AGE_NS)&
                     (anchor-ot[oo]>=0)&(anchor-ot[oo]<=MAX_AGE_NS))
        valid=start_fresh&rawgood
        # No *actual OKX native midpoint changes* at any point during (t-h,t].
        target_no_move=(
            np.searchsorted(change_ts,t,side="right")-
            np.searchsorted(change_ts,anchor,side="right"))==0
        price_delta=np.full(len(ids),np.nan)
        price_delta[valid]=1e4*(
            bm[ids[valid]]-bm[bb[valid]])/bm[bb[valid]]
        intact=valid&target_no_move
        horizon={"valid_both_venues":int(np.count_nonzero(valid)),
                 "valid_target_unchanged":int(np.count_nonzero(intact)),
                 "max_abs_binance_price_move_bps":stats(np.abs(price_delta)),
                 "max_abs_when_target_unchanged":stats(np.abs(price_delta[intact])),
                 "thresholds":{}}
        for thr in THRESHOLDS_BPS:
            event=intact&(np.abs(price_delta)>=thr)
            key=f"{thr:g}bps"
            horizon["thresholds"][key]=count_summary(
                event,target_segment,t,visibility_changed,visiblegood,
                SURVIVAL_NS,t,target_follow_ns,raw_g,visible_g,raw_spread)
        out["horizons"][h_name]=horizon
    return out


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--day",required=True)
    p.add_argument("--out",default="reports/sol_lead")
    args=p.parse_args()
    report=run_one(args.day)
    out=Path(args.out)
    out.mkdir(parents=True,exist_ok=True)
    (out/f"{args.day}.json").write_text(
        json.dumps(report,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    print("DAY",args.day,
          "BINANCE_PRICE_EVENT",report["binance_price_change_events"],
          "RAW_VALID",report["raw_valid_events"],
          "RAW_SPREAD_GE10",report["raw_spread_at_least_10bps"],
          "RAW_G_GE10",report["raw_G_at_least_10bps"],
          "VISIBLE_G_GE10",report["visible_G_at_least_10bps"],flush=True)
    for name,a in report["horizons"].items():
        print("HORIZON",args.day,name,
              "BINANCE_MAX_BPS",a["max_abs_binance_price_move_bps"]["max"],
              "STILL_TARGET_MAX_BPS",a["max_abs_when_target_unchanged"]["max"]
                if a["max_abs_when_target_unchanged"]["n"] else None,
              "SOURCE10",a["thresholds"]["10bps"]["event_count"],
              "UNIQUE10",a["thresholds"]["10bps"]["unique_okx_unchanged_segments"],
              "SOURCE5",a["thresholds"]["5bps"]["event_count"],
              "SOURCE2",a["thresholds"]["2bps"]["event_count"],flush=True)
    print("FILE",out/f"{args.day}.json",flush=True)


if __name__=="__main__":
    main()
