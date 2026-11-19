"""BTC 2026-10-01 Bitget vs Binance BBO capture lag: per-hour and 5-min tails.

Reads REAL full-day standardized Tardis quotes / book_ticker; separate
distribution for each feed. Measures clock deltas, NOT one-way network latency.
This is a diagnosis only; nothing is run through HBT.
"""
import argparse
import csv
from datetime import datetime,timezone
import gzip
import io
import json
from pathlib import Path
import urllib.request
from audit_cross_exchange_4a import LagStats, url_for, USER_AGENT

TARGET_DAY="2026-10-01"
SOURCES=(
    ("bitget_quotes","bitget-futures","quotes","BTCUSDT"),
    ("binance_book_ticker","binance-futures","book_ticker","BTCUSDT"),
)

def scan(label,exchange,kind,symbol,day):
    dt=datetime.strptime(day,"%Y-%m-%d").replace(tzinfo=timezone.utc)
    begin=int(dt.timestamp()*1_000_000)
    end=begin+86_400_000_000
    hourly=[LagStats() for _ in range(24)]
    five=[{"n":0,"over_20ms":0,"over_50ms":0,"over_75ms":0,
           "over_100ms":0,"lag_sum_ms":0.0} for _ in range(288)]
    total=0
    for_attempts=0
    url=url_for(exchange,kind,day,symbol)
    req=urllib.request.Request(url,headers={"User-Agent":USER_AGENT})
    with urllib.request.urlopen(req,timeout=180) as resp:
        with gzip.GzipFile(fileobj=resp) as dec:
            with io.TextIOWrapper(dec,encoding="utf-8",newline="") as stream:
                rd=csv.DictReader(stream)
                for row in rd:
                    local=int(row["local_timestamp"])
                    if local>=end:
                        break
                    if local<begin:
                        continue
                    ex=int(row["timestamp"])
                    if ex<=0:
                        raise ValueError(f"{label}: invalid exchange timestamp")
                    lag_us=local-ex
                    idx=max(0,min(23,int((local-begin)//3_600_000_000)))
                    h5=max(0,min(287,int((local-begin)//300_000_000)))
                    hourly[idx].add(lag_us,idx)
                    total+=1
                    if lag_us<0:
                        continue
                    z=five[h5]
                    z["n"]+=1
                    z["lag_sum_ms"]+=lag_us/1000
                    for threshold,key in ((20_000,"over_20ms"),(50_000,"over_50ms"),
                                          (75_000,"over_75ms"),(100_000,"over_100ms")):
                        if lag_us>threshold:
                            z[key]+=1
                    if total%3_000_000==0:
                        print("OCT_CLOCK_PROGRESS",label,total,flush=True)
    rows=[]
    for h,metric in enumerate(hourly):
        z=metric.result()
        over50=sum(five[q]["over_50ms"] for q in range(h*12,(h+1)*12))
        over75=sum(five[q]["over_75ms"] for q in range(h*12,(h+1)*12))
        rows.append({
            "hour_utc":h,**{k:z.get(k) for k in ("n_nonnegative","n_negative",
                                                 "p50_ms","p95_ms","p99_ms","max_ms")},
            "over_50ms_count":over50,"over_75ms_count":over75,
            "over_50ms_fraction":over50/z["n_nonnegative"] if z["n_nonnegative"] else None,
        })
    for i,z in enumerate(five):
        z["start_utc"]=f"{i//12:02d}:{(i%12)*5:02d}"
        z["over_50ms_fraction"]=z["over_50ms"]/z["n"] if z["n"] else None
        z["over_75ms_fraction"]=z["over_75ms"]/z["n"] if z["n"] else None
        z["mean_lag_ms"]=round(z.pop("lag_sum_ms")/z["n"],4) if z["n"] else None
    big_total=sum(z["over_50ms"] for z in five)
    big75=sum(z["over_75ms"] for z in five)
    top3=sorted((r["over_50ms_count"] for r in rows),reverse=True)[:3]
    return {
        "source":url,"observations":total,
        "hours_with_data":sum(z["n_nonnegative"]>0 for z in rows),
        "hours_with_p95_gt_50ms":sum(isinstance(z["p95_ms"],(float,int)) and z["p95_ms"]>50 for z in rows),
        "hours_with_more_than_5pct_lag_gt_50ms":sum(
            (z["over_50ms_fraction"] or 0)>.05 for z in rows),
        "hours_with_more_than_10pct_lag_gt_50ms":sum(
            (z["over_50ms_fraction"] or 0)>.10 for z in rows),
        "total_over_50ms_count":big_total,
        "total_over_75ms_count":big75,
        "fraction_over_50ms":big_total/total if total else None,
        "fraction_over_75ms":big75/total if total else None,
        "top3_hours_share_of_all_gt50ms":sum(top3)/big_total if big_total else None,
        "hourly":rows,"five_minute":five,
    }

def classify(bitget,binance):
    b=bitget
    # Explicit descriptive rule; no causal inference from classification.
    broad=b["hours_with_more_than_5pct_lag_gt_50ms"]>=12
    concentrated=b["top3_hours_share_of_all_gt50ms"]>=.70
    if broad and not concentrated:
        typ="broad_multi_hour_tail"
    elif concentrated:
        typ="concentrated_in_top_three_hours"
    else:
        typ="mixed_or_transitioning"
    return {
        "descriptive_tail_classification":typ,
        "rule":"broad if >=12 UTC hours exceed 5% lag>50ms and top3 hours hold <70% exceedances; concentrated if top3 hold >=70%; otherwise mixed",
        "no_network_causality_inferred":True,
        "timestamp_convention_is_venue_specific":True,
        "bitget_p95_20261001_prior_full_day_ms":73.25,
        "binance_p95_20261001_prior_full_day_ms":4.15,
    }

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--out",type=Path,required=True)
    args=p.parse_args()
    out=args.out
    out.parent.mkdir(parents=True,exist_ok=True)
    sources={}
    for name,exchange,kind,symbol in SOURCES:
        print("OCT_CLOCK_BEGIN",name,flush=True)
        sources[name]=scan(name,exchange,kind,symbol,TARGET_DAY)
        print("OCT_CLOCK_END",name,sources[name]["observations"],
              sources[name]["hours_with_p95_gt_50ms"],flush=True)
    result={
        "date":TARGET_DAY,"source":"Actual Tardis full-day standardized CSVs",
        "metric":"Tardis local_timestamp minus venue exchange timestamp in microseconds; shown ms",
        "sources":sources,
        "diagnosis":classify(sources["bitget_quotes"],sources["binance_book_ticker"]),
        "limitation":"Different exchanges and BBO feed types can have different publish clocks and sampling cadence; not a pure network latency measure.",
        "no_hbt":True
    }
    out.write_text(json.dumps(result,ensure_ascii=False,indent=2,
                              allow_nan=False)+"\n",encoding="utf-8")
    print("OCT_CLOCK_FORENSIC_RESULT",result["diagnosis"],flush=True)

if __name__=="__main__":
    main()
