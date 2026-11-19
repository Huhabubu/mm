"""Read-only DOGEUSDT precise price-grid anomaly audit on three blocked days.

Separately checks FLOAT tolerance used in initial 4A and Decimal exact
multiples of 0.00001, 0.000001, 0.0000001. Captures up to 20 examples,
event timestamps, snapshot/updates, and per-hour concentrations. No HBT.
"""
import argparse
from datetime import datetime,timezone
from decimal import Decimal
from collections import Counter
import csv,gzip,io,json
from pathlib import Path
import urllib.request

from audit_cross_exchange_4a import on_grid, url_for, USER_AGENT

SYMBOL="DOGEUSDT"
DAYS=("2026-07-01","2026-08-01","2026-11-01")
TICKS=(Decimal("0.00001"),Decimal("0.000001"),Decimal("0.0000001"))

def inspect(day,kind):
    if day not in DAYS or kind not in ("incremental_book_L2","trades"):
        raise ValueError("This is a bounded price-grid diagnosis of the three DOGE block dates only")
    url=url_for("bitget-futures",kind,day,SYMBOL)
    t=datetime.strptime(day,"%Y-%m-%d").replace(tzinfo=timezone.utc)
    start_us=int(t.timestamp()*1_000_000)
    stop_us=start_us+86_400_000_000
    result={"feed_url":url,"kind":kind,"rows":0,"off_grid_float_count":0,
            "off_grid_decimal_1e5_count":0,"off_grid_decimal_1e6_count":0,
            "off_grid_decimal_1e7_count":0,"off_grid_snapshot_count":0,
            "off_grid_delta_count":0,"off_grid_hour_counts":[0]*24,
            "off_grid_examples":[],"off_grid_unique_timestamps":0,
            "first_off_grid_local_us":None,"last_off_grid_local_us":None,
            "first_local_us":None,"last_local_us":None}
    groups=set()
    with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":USER_AGENT}),timeout=180) as response:
        with gzip.GzipFile(fileobj=response) as dec:
            with io.TextIOWrapper(dec,encoding="utf-8",newline="") as stream:
                for row in csv.DictReader(stream):
                    local=int(row["local_timestamp"])
                    if local<start_us:continue
                    if local>=stop_us:break
                    result["rows"]+=1
                    result["first_local_us"]=result["first_local_us"] or local
                    result["last_local_us"]=local
                    price_str=row["price"]
                    # The original audit uses this same float tolerance.
                    if on_grid(float(price_str),float(TICKS[0])):
                        continue
                    result["off_grid_float_count"]+=1
                    price=Decimal(price_str)
                    for tick,field in zip(TICKS,("off_grid_decimal_1e5_count",
                         "off_grid_decimal_1e6_count","off_grid_decimal_1e7_count")):
                        if price%tick!=0:
                            result[field]+=1
                    snap=row.get("is_snapshot")
                    if snap=="true":result["off_grid_snapshot_count"]+=1
                    elif snap=="false":result["off_grid_delta_count"]+=1
                    groups.add(local)
                    hour=min(23,max(0,(local-start_us)//3_600_000_000))
                    result["off_grid_hour_counts"][int(hour)]+=1
                    result["first_off_grid_local_us"]=result["first_off_grid_local_us"] or local
                    result["last_off_grid_local_us"]=local
                    if len(result["off_grid_examples"])<20:
                        result["off_grid_examples"].append({
                            "local_us":local,
                            "local_iso_utc":datetime.fromtimestamp(local/1_000_000,timezone.utc).isoformat(),
                            "side":row.get("side"),
                            "raw_price":price_str,
                            "remainder_1e5":str(price%TICKS[0]),
                            "is_snapshot":snap,
                            "qty":row["amount"],
                        })
                    if result["rows"]%4_000_000==0:
                        print("DOGE_GRID_PROGRESS",day,kind,result["rows"],flush=True)
    result["off_grid_unique_timestamps"]=len(groups)
    result["end_shortfall_seconds"]=(
        (stop_us-result["last_local_us"])/1_000_000
        if result["last_local_us"] is not None else None)
    return result

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--day",required=True,choices=DAYS)
    p.add_argument("--out",required=True,type=Path)
    a=p.parse_args()
    out=a.out;out.parent.mkdir(parents=True,exist_ok=True)
    info={"symbol":SYMBOL,"day":a.day,"read_only":True,
          "price_candidate_tick":"0.00001",
          "historical_exchange_tick_change_confirmed":False,
          "sources":{}}
    for kind in ("trades","incremental_book_L2"):
        print("DOGE_GRID_BEGIN",a.day,kind,flush=True)
        info["sources"][kind]=inspect(a.day,kind)
        print("DOGE_GRID_RESULT",a.day,kind,
              info["sources"][kind]["off_grid_float_count"],flush=True)
    out.write_text(json.dumps(info,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")

if __name__=="__main__":
    main()
