"""V6 read-only clock, local-vs-exchange timestamps and pre-jump microprice.

Reads native Tardis book_ticker CSVs only. Never uses quote engine/exec, never
changes V5, no forecast claim from matching successive cross-venue prices.
Time fields are provider-normalized microseconds; each clock's exchange time
may be generated from a distinct venue with unknown synchronization errors.
"""
import argparse
import csv
import gzip
import io
import json
import urllib.request
from collections import defaultdict
from datetime import datetime,timezone
from pathlib import Path

import numpy as np
from inspect_tardis import BASE_URL

DATES=("2026-06-01","2026-07-01")
COINS=("BTC","SOL","DOGE","1000PEPE")
MARKETS={
    "BTC":(("binance-futures","BTCUSDT",1.),("okex-swap","BTC-USDT-SWAP",1.)),
    "SOL":(("binance-futures","SOLUSDT",1.),("okex-swap","SOL-USDT-SWAP",1.)),
    "DOGE":(("binance-futures","DOGEUSDT",1.),("okex-swap","DOGE-USDT-SWAP",1.)),
    "1000PEPE":(("binance-futures","1000PEPEUSDT",1000.),
                ("okex-swap","PEPE-USDT-SWAP",1.),
                ("bitget-futures","PEPEUSDT",1.))
}


def read_native(day,venue,symbol,price_divisor=1.,seconds=4205,opener=urllib.request.urlopen):
    """Finite <71min Tardis book ticker, original data order, not forward-filled."""
    yyyy,mm,dd=day.split("-")
    path=f"{BASE_URL}/{venue}/book_ticker/{yyyy}/{mm}/{dd}/{symbol}.csv.gz"
    start=int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp()*1e6)
    end=start+seconds*1_000_000
    arrays=[[],[],[],[],[],[],[]]
    with opener(urllib.request.Request(path,headers={"User-Agent":"research-v6-clock/1.0"}),timeout=120) as response:
        with io.TextIOWrapper(gzip.GzipFile(fileobj=response),encoding="utf8",newline="") as inp:
            reader=csv.DictReader(inp)
            needed={"local_timestamp","timestamp","bid_price","ask_price","bid_amount","ask_amount"}
            if not needed.issubset(reader.fieldnames or []):
                raise ValueError(f"Invalid Tardis BBO fields for {venue} {symbol}")
            prev=-1
            for row in reader:
                l=int(row["local_timestamp"])
                if l>=end:break
                if l<start:continue
                if l<prev:raise ValueError("Local time reversed")
                prev=l
                e=int(row["timestamp"])
                bid=float(row["bid_price"])/price_divisor
                ask=float(row["ask_price"])/price_divisor
                bidq=float(row["bid_amount"])
                askq=float(row["ask_amount"])
                if not (0<bid<ask and bidq>=0 and askq>=0 and e>0):
                    raise ValueError("Nonphysical BBO")
                for slot,val in zip(arrays,(e,l,bid,ask,bidq,askq,l-e)):
                    slot.append(val)
    if len(arrays[0])<100:
        raise ValueError("Too few BBO rows "+path)
    return {
        "exchange_us":np.asarray(arrays[0],np.int64),
        "local_us":np.asarray(arrays[1],np.int64),
        "bid":np.asarray(arrays[2],float),
        "ask":np.asarray(arrays[3],float),
        "bidqty":np.asarray(arrays[4],float),
        "askqty":np.asarray(arrays[5],float),
        "collector_lag_us":np.asarray(arrays[6],np.int64),
        "source_url":path
    }


def distribution(a):
    a=np.asarray(a,dtype=float)
    if not len(a):return {"n":0}
    q=np.quantile(a,[.1,.5,.9])
    return {
        "n":len(a),"p10_ms":float(q[0]),"median_ms":float(q[1]),
        "mean_ms":float(a.mean()),"p90_ms":float(q[2]),
        "minimum_ms":float(a.min()),"maximum_ms":float(a.max()),
        "negative_fraction":float(np.mean(a<0))
    }


def clock_report(x):
    return distribution(x["collector_lag_us"]/1e3)


def change_events(x,clock):
    ts=np.asarray(x[clock+"_us"],np.int64)
    if np.any(np.diff(ts)<0):
        # Exchange timestamp may be unsorted inside exchange BBO stream.
        order=np.argsort(ts,kind="stable")
        ts=ts[order];mid=(x["bid"][order]+x["ask"][order])/2.
    else:
        mid=(x["bid"]+x["ask"])/2.
    dx=np.diff(mid)
    i=np.flatnonzero(dx!=0)+1
    return ts[i],np.sign(dx[i-1]).astype(np.int8)


def next_same_direction_follow_us(source,target,max_ms=100):
    """One earliest sign-consistent target change for each source event.

    No uniqueness guarantee; this is observational clock alignment only, NOT
    causal transmission (multiple source events may reuse same target jump).
    """
    lags=[]
    st,ss=source
    tt,sg=target
    for sign in (-1,1):
        a=st[ss==sign]
        b=tt[sg==sign]
        if not len(a) or not len(b):continue
        idx=np.searchsorted(b,a,side="left")
        valid=idx<len(b)
        diff=b[idx[valid]]-a[valid]
        lags.extend(diff[(diff>=0)&(diff<=max_ms*1000)].tolist())
    out=distribution(np.asarray(lags,dtype=float)/1000)
    out["source_mid_changes"]=int(len(st))
    out["match_fraction_per_source"]=len(lags)/len(st) if len(st) else 0.
    out["max_forward_same_sign_ms"]=max_ms
    out["nonunique_matching"]=True
    return out



def signed_nearest_same_direction_ms(source,target,max_ms=100):
    """For each source move, nearest same-sign target move in ±100ms.

    Signed lag = target timestamp minus Binance timestamp; positive suggests
    source precedes the *matched* target row, negative suggests source follows.
    Nonunique matches, clock synchronization and event-frequency bias mean
    neither direction identifies physical propagation or true causality.
    """
    a,sg=source
    b,bs=target
    diffs=[]
    for sign in (-1,1):
        src=a[sg==sign]
        tgt=b[bs==sign]
        if not len(src) or not len(tgt):continue
        ii=np.searchsorted(tgt,src,side="left")
        before=np.maximum(ii-1,0)
        after=np.minimum(ii,len(tgt)-1)
        diff_before=tgt[before]-src
        diff_after=tgt[after]-src
        choose_before=np.abs(diff_before)<=np.abs(diff_after)
        ds=np.where(choose_before,diff_before,diff_after)
        diffs.extend(ds[np.abs(ds)<=max_ms*1000].tolist())
    x=np.asarray(diffs,float)/1000
    out=distribution(x)
    out.update({
        "source_move_count":int(len(a)),
        "matched_source_fraction":float(len(x)/len(a)) if len(a) else 0.,
        "binance_before_target_fraction_of_matched":float(np.mean(x>0)) if len(x) else None,
        "binance_after_target_fraction_of_matched":float(np.mean(x<0)) if len(x) else None,
        "exact_timestamp_tie_fraction":float(np.mean(x==0)) if len(x) else None,
        "lag_sign":"target_time minus Binance_time; positive=B first",
        "same_target_can_match_multiple_source_moves":True,
        "max_absolute_lag_ms":max_ms,
        "collector_clock_offsets_not_corrected":True})
    return out


def microprice(bid,ask,bidq,askq):
    """Weighted mid = (ask*bidqty + bid*askqty)/(bidqty+askqty)."""
    den=bidq+askq
    return np.divide(ask*bidq+bid*askq,den,
                     out=(bid+ask)/2.,where=den>0)


def prejumps(bn,target,offset_ms,post_ms=100,extra_delay_ms=2.5):
    """Feature strictly before Binance local BBO jump; target future is label.

    Event selection uses Binance jumps known only after they happen. We test
    whether already available 5-50ms early BBO imbalance points in the right
    direction of *future* Binance jump; don't trade on preselected jump events.
    """
    tt=bn["local_us"]+int(extra_delay_ms*1000)
    mid=(bn["bid"]+bn["ask"])/2.
    dx=np.diff(mid)
    ev=np.flatnonzero(dx!=0)+1
    if not len(ev):return {"n":0}
    # One jump per 20ms BBO-local bucket; de-dupe event-density effects.
    buckets=tt[ev]//20000
    ev=ev[np.r_[True,np.diff(buckets)!=0]]
    when=tt[ev]
    prior=np.searchsorted(tt,when-offset_ms*1000,side="left")-1
    valid=(prior>=0)
    valid&=tt[ev]-tt[np.maximum(prior,0)]<=250000+offset_ms*1000
    valid&=tt[ev]-tt[np.maximum(prior,0)]>offset_ms*1000
    obs=ev[valid];prev=prior[valid];when=when[valid]
    p=microprice(bn["bid"][prev],bn["ask"][prev],
                 bn["bidqty"][prev],bn["askqty"][prev])
    basis=(bn["bid"][prev]+bn["ask"][prev])/2.
    signal=(p-basis)/basis*1e4
    imbalance=(bn["bidqty"][prev]-bn["askqty"][prev])/np.maximum(
        bn["bidqty"][prev]+bn["askqty"][prev],1e-12)
    jump_dir=np.sign(mid[obs]-mid[obs-1])
    target_ts=target["local_us"]
    target_mid=(target["bid"]+target["ask"])/2
    before=np.searchsorted(target_ts,when,side="left")-1
    after=np.searchsorted(target_ts,when+post_ms*1000,side="right")-1
    safe0=np.maximum(before,0)
    safe1=np.maximum(after,0)
    good=(before>=0)&(after>=0)&(when-target_ts[safe0]>=0)&(
        when-target_ts[safe0]<=250000)&(
        when+post_ms*1000-target_ts[safe1]>=0)&(
        when+post_ms*1000-target_ts[safe1]<=250000)
    signed_target=jump_dir[good]*(target_mid[safe1[good]]-
                                 target_mid[safe0[good]])/target_mid[safe0[good]]*1e4
    sig=signal[good]
    imb=imbalance[good]
    jd=jump_dir[good]
    aligned=sig*jd>0
    opposed=sig*jd<0
    out={
        "lag_prior_to_source_jump_ms":offset_ms,
        "sample_count":int(len(sig)),
        "microprice_zero_fraction":float(np.mean(sig==0)) if len(sig) else None,
        "prior_microprice_abs_bps":distribution(np.abs(sig)),
        "prior_qty_imbalance_abs":distribution(np.abs(imb)),
        "aligned_count":int(aligned.sum()),
        "opposed_count":int(opposed.sum()),
        "aligned_fraction_excluding_zeros":float(np.mean(aligned[aligned|opposed])) if np.any(aligned|opposed) else None,
        "100ms_target_signed_markout_mean_when_aligned_bps":float(np.mean(signed_target[aligned])) if np.any(aligned) else None,
        "100ms_target_signed_markout_mean_when_opposed_bps":float(np.mean(signed_target[opposed])) if np.any(opposed) else None,
        "target_label_relative_to_future_Binance_jump":True,
        "claim_limit":"conditioned on a future Binance jump; predictive validity requires all-event independent test"
    }
    return out


def case(day,coin):
    sources=MARKETS[coin]
    bn=read_native(day,*sources[0])
    targets=[]
    for venue,symbol,div in sources[1:]:
        x=read_native(day,venue,symbol,div)
        record={
            "source_venue":venue,
            "source_symbol":symbol,
            "binance_symbol":sources[0][1],
            "date":day,"coin":coin,
            "binance_local_minus_exchange":clock_report(bn),
            "target_local_minus_exchange":clock_report(x),
            "binance_vs_target_median_ms_difference":
                clock_report(bn)["median_ms"]-clock_report(x)["median_ms"],
            "event_link_by_clock":{
                clock:next_same_direction_follow_us(change_events(bn,clock),
                                                    change_events(x,clock))
                for clock in ("local","exchange")},
            "signed_nearest_event_lag_by_clock":{
                clock:signed_nearest_same_direction_ms(change_events(bn,clock),
                                                       change_events(x,clock))
                for clock in ("local","exchange")},
            "native_BBO_clocks_not_physical_propagation":True,
            "binance_native_quote_price_divisor":sources[0][2]
        }
        if day=="2026-06-01" and coin in ("BTC","SOL"):
            record["binance_prejump_microprice"]={
                f"{delay}ms":prejumps(bn,x,delay) for delay in (5,10,20,50)}
        targets.append(record)
    return targets


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--day",required=True,choices=DATES)
    p.add_argument("--coin",required=True,choices=COINS)
    p.add_argument("--out",required=True)
    a=p.parse_args()
    reports=case(a.day,a.coin)
    out=Path(a.out);out.mkdir(parents=True,exist_ok=True)
    obj={"day":a.day,"coin":a.coin,"targets":reports,
         "clock_definition":"Tardis normalized local_timestamp and timestamp, microseconds",
         "not_identified":"collector offset is affected by exchange clock, routing, sampling and collector sync; cross-venue physical latency cannot be inferred",
         "sample_window_UTC":"00:00 to 01:10:05 (all first-of-month days)",
         "raw_order_book_tick_and_amount_no_fused_HBT_substitution":True}
    (out/"clock_microprice_case.json").write_text(
        json.dumps(obj,ensure_ascii=False,indent=2,allow_nan=False),encoding="utf8")
    for r in reports:
        print("CLOCK_V6",a.coin,a.day,r["source_venue"],
              "BNC",r["binance_local_minus_exchange"],
              "TGT",r["target_local_minus_exchange"],
              "LOCAL",r["event_link_by_clock"]["local"]["median_ms"],
              "EXCH",r["event_link_by_clock"]["exchange"]["median_ms"],
              flush=True)


if __name__=="__main__":
    main()
