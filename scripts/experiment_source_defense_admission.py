"""Read-only Tardis and live-metadata admission *pre-screen*, not GLFT proof.

Each target contract must be assessed on its own quote precision. Live specs are
NOT historical specs. Tardis 75-second samples do not establish 50min intensity
or six-day GLFT calibration success.
"""
import argparse
import csv
import gzip
import io
import json
import math
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from qa_multivenue_data import current_spec

ROOT="https://datasets.tardis.dev/v1"
MARKETS={
    "DOGE":("DOGE-USDT-SWAP","DOGEUSDT"),
    "SUI":("SUI-USDT-SWAP","SUIUSDT"),
    "XRP":("XRP-USDT-SWAP","XRPUSDT"),
    "AVAX":("AVAX-USDT-SWAP","AVAXUSDT"),
    "ENA":("ENA-USDT-SWAP","ENAUSDT"),
    # OKX/Bitget PEPE contract may not use the same per-contract coin
    # multiplier as Binance 1000PEPE. Never compare raw cross-venue mids
    # until a historic multiplier audit has been performed.
    "1000PEPE":("PEPE-USDT-SWAP","PEPEUSDT"),
}
UA={"User-Agent":"mm-paper-candidate-screen/1.0"}
NS=1_000_000


def live_tick(spec, venue):
    if "error" in spec:
        return None
    try:
        tick=(float(spec["tickSz"]) if venue=="okex-swap" else
              float(spec["priceEndStep"])*10**(-int(spec["pricePlace"])))
        return tick if math.isfinite(tick) and tick>0 else None
    except (KeyError,ValueError,OverflowError,TypeError):
        return None


def scan_bbo(venue,symbol,day,seconds=75):
    start=int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp()*NS)
    yyyy,mm,dd=day.split("-")
    url=f"{ROOT}/{venue}/book_ticker/{yyyy}/{mm}/{dd}/{symbol}.csv.gz"
    rows=0;changes=0;bad=0;last_ts=-1;mid0=None;prev=None;tick_candidate=None
    last_mid=None;min_mid=math.inf;max_mid=0.0
    with urllib.request.urlopen(urllib.request.Request(url,headers=UA),timeout=120) as response:
        with io.TextIOWrapper(gzip.GzipFile(fileobj=response),encoding="utf-8",newline="") as fd:
            reader=csv.DictReader(fd)
            for row in reader:
                t=int(row["local_timestamp"])
                if t>=start+seconds*NS:break
                bid=float(row["bid_price"]);ask=float(row["ask_price"])
                if t<last_ts or not (math.isfinite(bid) and math.isfinite(ask) and 0<bid<ask):
                    bad+=1
                last_ts=t
                mid=.5*(bid+ask)
                if mid0 is None:mid0=mid
                min_mid=min(min_mid,mid);max_mid=max(max_mid,mid)
                if last_mid is not None and abs(mid-last_mid)>1e-12:changes+=1
                if prev is not None:
                    for price,earlier in ((bid,prev[0]),(ask,prev[1])):
                        diff=abs(price-earlier)
                        if diff>1e-12:
                            tick_candidate=diff if tick_candidate is None else min(diff,tick_candidate)
                prev=bid,ask;last_mid=mid;rows+=1
                if rows>500_000:raise RuntimeError("Excessive BBO rows during bounded scan")
    return {"rows":rows,"bad_rows":bad,"mid_price":mid0,
            "min_mid":min_mid if rows else None,"max_mid":max_mid if rows else None,
            "mid_changes":changes,"observed_min_price_step_upper_bound":tick_candidate}


def inspect(day,coin,venue,seconds=75):
    if coin not in MARKETS:raise ValueError("Unknown candidate")
    if venue not in ("okex-swap","bitget-futures"):raise ValueError("Unknown venue")
    if seconds<15 or seconds>600:raise ValueError("Bounded pre-screen only")
    symbol=MARKETS[coin][0 if venue=="okex-swap" else 1]
    lookup_coin="PEPE" if coin=="1000PEPE" else coin
    meta=current_spec(venue,lookup_coin)
    target_tick=live_tick(meta,venue)
    try:
        bbo=scan_bbo(venue,symbol,day,seconds)
    except Exception as e:
        bbo={"error":f"{type(e).__name__}: {e}"}
    mid=bbo.get("mid_price")
    relative=1e4*target_tick/mid if target_tick and mid and mid>0 else None
    verdict=("unverified" if relative is None or bbo.get("error") or bbo.get("bad_rows") or not bbo.get("rows")
             else "eligible_tick_only" if .1<=relative<=1.0 else "reject_tick_range")
    return {"day":day,"coin":coin,"venue":venue,"symbol":symbol,
            "sample_seconds":seconds,"live_instrument_spec":meta,
            "current_spec_not_archived_spec":True,
            "reported_live_tick":target_tick,
            "relative_tick_bps_live_spec_over_historical_mid":relative,
            "historical_price_step_is_only_upper_bound":True,
            "bbo_sample":bbo,"verdict":verdict,
            "binance_symbol_price_normalization_required":coin=="1000PEPE",
            "not_proven":["6/6 GLFT fit","50min executed trades",
                          "Binance price leader impulse quality","historical tick metadata"]}


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--coin",choices=MARKETS,required=True)
    p.add_argument("--venue",choices=("okex-swap","bitget-futures"),required=True)
    p.add_argument("--day",default="2026-06-01")
    p.add_argument("--seconds",type=int,default=75)
    p.add_argument("--out",default="reports/source_defense/admission")
    a=p.parse_args()
    result=inspect(a.day,a.coin,a.venue,a.seconds)
    out=Path(a.out);out.mkdir(parents=True,exist_ok=True)
    file=out/f"{a.coin}_{a.venue}_{a.day}.json"
    file.write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding="utf-8")
    print(json.dumps(result,ensure_ascii=False))
    # Missing data is a bounded eligibility finding, not a green build failure.


if __name__=="__main__":
    main()
