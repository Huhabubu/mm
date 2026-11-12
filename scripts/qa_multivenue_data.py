"""Bounded factual QA of four Tardis futures datasets and live contract specs.

No strategy trading. Never report a synthetic specification as historical truth.
"""
import argparse
import csv
import gzip
import io
import json
import math
import urllib.error
import urllib.request
from collections import Counter
from datetime import datetime, timezone

BASE = "https://datasets.tardis.dev/v1"
CASES = (
    ("okex-swap", "BTC-USDT-SWAP", "BTC"),
    ("okex-swap", "SOL-USDT-SWAP", "SOL"),
    ("bitget-futures", "BTCUSDT", "BTC"),
    ("bitget-futures", "SOLUSDT", "SOL"),
)
KINDS = ("incremental_book_L2", "trades", "book_ticker")
UA = {"User-Agent": "mm-paper-venue-qa/1.0"}


def json_url(url):
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=25) as stream:
        return json.load(stream)


def current_spec(venue, coin):
    try:
        if venue == "okex-swap":
            x = json_url("https://www.okx.com/api/v5/public/instruments?"
                         f"instType=SWAP&instId={coin}-USDT-SWAP")
            assert x.get("code") == "0", x.get("code")
            z = x["data"][0]
            return {"source": "live_OKX_API", "tickSz": z.get("tickSz"),
                    "lotSz": z.get("lotSz"), "minSz": z.get("minSz"),
                    "ctVal": z.get("ctVal"), "ctValCcy": z.get("ctValCcy"),
                    "ctType": z.get("ctType"), "state": z.get("state")}
        x = json_url("https://api.bitget.com/api/v2/mix/market/contracts?"
                     "productType=USDT-FUTURES")
        assert x.get("code") == "00000", x.get("code")
        z = next(t for t in x["data"] if t["symbol"] == coin+"USDT")
        return {"source": "live_Bitget_API", "pricePlace": z.get("pricePlace"),
                "priceEndStep": z.get("priceEndStep"),
                "sizeMultiplier": z.get("sizeMultiplier"),
                "minTradeNum": z.get("minTradeNum"),
                "minTradeUSDT": z.get("minTradeUSDT"),
                "makerFeeRate": z.get("makerFeeRate"),
                "takerFeeRate": z.get("takerFeeRate"),
                "symbolStatus": z.get("symbolStatus")}
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}", "spec_unverified": True}


def sample(venue, symbol, day, kind, seconds=75):
    yyyy, mm, dd = day.split("-")
    url = f"{BASE}/{venue}/{kind}/{yyyy}/{mm}/{dd}/{symbol}.csv.gz"
    end_us = int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp()*1e6)+seconds*1_000_000
    d = {"url":url, "rows":0, "bad":0, "nonmonotonic":0, "snapshots":0,
         "sides":Counter(), "min_positive_amount":None, "prices":[],
         "first_local_us":None, "last_local_us":None,
         "initial_snapshot_side_counts":Counter(), "first_snapshot_us":None}
    prev = None
    was_snap = False
    with urllib.request.urlopen(urllib.request.Request(url,headers=UA),timeout=120) as response:
        with io.TextIOWrapper(gzip.GzipFile(fileobj=response),newline="",encoding="utf-8") as stream:
            reader = csv.DictReader(stream)
            d["columns"] = reader.fieldnames
            for row in reader:
                t=int(row["local_timestamp"])
                if t>=end_us:break
                d["rows"]+=1
                if d["rows"]>2_000_000:raise RuntimeError("QA bounded rows exceeded")
                if d["first_local_us"] is None:d["first_local_us"]=t
                d["last_local_us"]=t
                if prev is not None and t<prev:d["nonmonotonic"]+=1
                prev=t
                try:
                    price=float(row["bid_price"] if kind=="book_ticker" else row["price"])
                    amt=float(row["bid_amount"] if kind=="book_ticker" else row["amount"])
                    if not math.isfinite(price) or price<=0 or not math.isfinite(amt) or amt<0:
                        d["bad"]+=1
                    if amt>0 and (d["min_positive_amount"] is None or amt<d["min_positive_amount"]):
                        d["min_positive_amount"]=amt
                    if len(d["prices"])<1000:d["prices"].append(price)
                    if kind!="book_ticker":d["sides"][row["side"]]+=1
                    if kind=="incremental_book_L2":
                        snap=row["is_snapshot"].lower()=="true"
                        if snap:
                            d["snapshots"]+=1
                            if d["first_snapshot_us"] is None:d["first_snapshot_us"]=t
                            if not was_snap:d["initial_snapshot_side_counts"][row["side"]]+=1
                        elif d["first_snapshot_us"] is not None:
                            was_snap=True
                except Exception:d["bad"]+=1
    d["sides"]=dict(d["sides"])
    d["initial_snapshot_side_counts"]=dict(d["initial_snapshot_side_counts"])
    d["price_examples"]=d.pop("prices")[:8]
    d["complete_initial_snapshot"]=bool(d["first_snapshot_us"] and
                d["initial_snapshot_side_counts"].get("bid") and
                d["initial_snapshot_side_counts"].get("ask") and was_snap)
    return d


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--day",default="2026-11-01")
    p.add_argument("--seconds",type=int,default=75)
    args=p.parse_args()
    results={}
    for venue,symbol,coin in CASES:
        print("QA",venue,symbol,flush=True)
        key=f"{venue}:{symbol}"
        group={"spec":current_spec(venue,coin),"feeds":{}}
        for kind in KINDS:
            try:group["feeds"][kind]=sample(venue,symbol,args.day,kind,args.seconds)
            except Exception as exc:group["feeds"][kind]={"error":f"{type(exc).__name__}: {exc}"}
        results[key]=group
        print(json.dumps({key:group},ensure_ascii=False),flush=True)
    from pathlib import Path
    Path("reports").mkdir(exist_ok=True)
    Path("reports/venue_qa_2026-11-01.json").write_text(
        json.dumps(results,indent=2,ensure_ascii=False,default=str)+"\n",encoding="utf-8")
    problems=[]
    for key,group in results.items():
        for kind, feed in group["feeds"].items():
            if ("error" in feed or not feed.get("rows") or feed["bad"] or
                feed["nonmonotonic"] or
                (kind=="incremental_book_L2" and not feed["complete_initial_snapshot"])):
                problems.append(f"{key}:{kind} invalid or missing")
    if problems:raise RuntimeError("; ".join(problems))


if __name__=="__main__":
    main()
