"""Read-only full-day audit of normalized Tardis files; cannot prove raw update-ID continuity."""
import argparse
import csv
import gzip
import heapq
import io
import json
import math
import urllib.request
from datetime import datetime,timezone
from pathlib import Path

def stamp(us):
    return datetime.fromtimestamp(us/1e6,timezone.utc).isoformat(timespec="milliseconds")

def scan(day,venue,symbol,kind):
    url=f"https://datasets.tardis.dev/v1/{venue}/{kind}/{day[:4]}/{day[5:7]}/{day[8:]}/{symbol}.csv.gz"
    start=int(datetime.strptime(day,"%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()*1e6)
    end=start+86400_000_000
    s={"url":url,"rows":0,"bad_rows":0,"local_reversals":0,"out_of_day":0,
       "negative_latency_rows":0,"first_local":None,"last_local":None,
       "max_gap_ms":0,"gap_gt_100ms":0,"gap_gt_500ms":0,
       "gap_gt_1000ms":0,"gap_gt_5000ms":0,
       "hour_rows":[0]*24,"quarter_messages":[0]*96,
       "snapshot_groups":[],"deltas_before_snapshot":0,"largest_gaps":[]}
    before=None;group_ts=None;top=[];snapshot=None;has_snapshot=False
    def close():
        nonlocal snapshot
        if snapshot:
            snapshot["complete_sides"]=bool(snapshot["bid_rows"] and snapshot["ask_rows"])
            snapshot["at_utc"]=stamp(snapshot.pop("at_us"))
            s["snapshot_groups"].append(snapshot)
            snapshot=None
    req=urllib.request.Request(url,headers={"User-Agent":"mm-tardis-paper-qa/1.0"})
    with urllib.request.urlopen(req,timeout=180) as resp:
        with io.TextIOWrapper(gzip.GzipFile(fileobj=resp),encoding="utf-8",newline="") as stream:
            reader=csv.DictReader(stream)
            common={"exchange","symbol","timestamp","local_timestamp"}
            fields=({"is_snapshot","side","price","amount"} if kind=="incremental_book_L2"
                    else {"bid_price","ask_price","bid_amount","ask_amount"} if kind=="book_ticker"
                    else {"side","price","amount"})
            if (common|fields)-set(reader.fieldnames or ()):
                raise ValueError("missing required columns")
            for row in reader:
                try:
                    t=int(row["local_timestamp"]);ex=int(row["timestamp"])
                    if row["exchange"]!=venue or row["symbol"]!=symbol or t<=0 or ex<=0:
                        raise ValueError("invalid source/time")
                    if kind=="incremental_book_L2":
                        snap=row["is_snapshot"].lower()
                        if snap not in ("true","false") or row["side"] not in ("bid","ask"):
                            raise ValueError("bad L2 flags")
                        p=float(row["price"]);q=float(row["amount"])
                        if not (math.isfinite(p) and p>0 and math.isfinite(q) and q>=0):
                            raise ValueError("bad L2 level")
                    elif kind=="book_ticker":
                        b,a,bq,aq=[float(row[k]) for k in ("bid_price","ask_price","bid_amount","ask_amount")]
                        if not (all(map(math.isfinite,(b,a,bq,aq))) and 0<b<a and bq>=0 and aq>=0):
                            raise ValueError("bad BBO")
                    else:
                        p=float(row["price"]);q=float(row["amount"])
                        if row["side"] not in ("buy","sell") or not (math.isfinite(p) and p>0 and math.isfinite(q) and q>0):
                            raise ValueError("bad trade")
                except (TypeError,ValueError,KeyError,OverflowError):
                    s["bad_rows"]+=1
                    continue
                s["rows"]+=1
                if s["first_local"] is None: s["first_local"]=stamp(t)
                s["last_local"]=stamp(t)
                if t<ex:s["negative_latency_rows"]+=1
                if not start<=t<end:s["out_of_day"]+=1
                else:
                    bucket=(t-start)//900_000_000
                    s["hour_rows"][bucket//4]+=1
                    if t!=group_ts: s["quarter_messages"][bucket]+=1
                if before is not None:
                    diff=t-before
                    if diff<0:s["local_reversals"]+=1
                    if diff>0:
                        s["max_gap_ms"]=max(s["max_gap_ms"],round(diff/1000,3))
                        for ms in (100,500,1000,5000):
                            if diff>ms*1000:s[f"gap_gt_{ms}ms"]+=1
                        if diff>500_000:
                            x=(diff,before,t)
                            if len(top)<20:heapq.heappush(top,x)
                            elif x>top[0]:heapq.heapreplace(top,x)
                if kind=="incremental_book_L2":
                    if snap=="true":
                        if snapshot is None:
                            snapshot={"at_us":t,"bid_rows":0,"ask_rows":0,
                                      "preceding_gap_ms":None if before is None else round((t-before)/1000,3)}
                        snapshot[f"{row['side']}_rows"]+=1
                        has_snapshot=True
                    else:
                        close()
                        if not has_snapshot:s["deltas_before_snapshot"]+=1
                group_ts=t
                before=t
                if s["rows"]%10_000_000==0:
                    print(f"PROGRESS {day} {venue} {symbol} {kind} {s['rows']} rows",flush=True)
    close()
    s["largest_gaps"]=[{"ms":round(diff/1000,3),"from_utc":stamp(a),"to_utc":stamp(b)}
                       for diff,a,b in sorted(top,reverse=True)]
    s["resnapshot_count"]=max(0,len(s["snapshot_groups"])-1)
    s["invalid_snapshot_groups"]=sum(not v["complete_sides"] for v in s["snapshot_groups"])
    s["empty_15m_slots"]=[i for i,n in enumerate(s["quarter_messages"]) if n==0]
    s["format_status"]=("flagged" if s["bad_rows"] or s["local_reversals"] or s["out_of_day"]
                        or s["invalid_snapshot_groups"] or not s["rows"]
                        or (kind=="incremental_book_L2" and not has_snapshot)
                        else "passed")
    return s

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--day",required=True);p.add_argument("--venue",required=True)
    p.add_argument("--symbol",required=True);p.add_argument("--kinds",default="incremental_book_L2,book_ticker,trades")
    x=p.parse_args()
    result={"date":x.day,"venue":x.venue,"symbol":x.symbol,
            "limitations":"Normalized Tardis CSV has no exchange update-ID/connection metadata; silent loss not excluded.",
            "feeds":{},"status":"running"}
    dest=Path("reports/tardis_full_day")/f"{x.day}_{x.venue}_{x.symbol}.json"
    dest.parent.mkdir(parents=True,exist_ok=True)
    try:
        for kind in x.kinds.split(","):
            print("START",x.day,x.venue,x.symbol,kind,flush=True)
            result["feeds"][kind]=scan(x.day,x.venue,x.symbol,kind)
            s=result["feeds"][kind]
            print("END",kind,"rows",s["rows"],"resnapshots",s["resnapshot_count"],
                  "max_gap_ms",s["max_gap_ms"],"status",s["format_status"],flush=True)
        result["status"]="completed"
    except Exception as e:
        result["status"]="partial_error"
        result["error"]=f"{type(e).__name__}: {e}"
        raise
    finally:
        dest.write_text(json.dumps(result,ensure_ascii=False,indent=2)+"\n")
        print("REPORT",dest,flush=True)
if __name__=="__main__":main()
