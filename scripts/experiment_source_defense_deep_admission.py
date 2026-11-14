"""Official HBT 10-minute initial-fit admission plus bounded 50-minute trade count.

Only DOGE/XRP, which passed the independent 75s relative-tick pre-screen.
No strategy orders are submitted. Current live contract specs are provisional
until historical contract precision and amount units are independently checked.
"""
import argparse
import csv
import gzip
import io
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import urllib.request

import numpy as np
from hftbacktest.data.utils import tardis
from hftbacktest.data.validation import validate_event_order

from check_okx_hft_data import SCHEMAS, extract_window, require_unshifted_target_clock
from qa_multivenue_data import current_spec
from experiment_source_defense_admission import MARKETS, live_tick, ROOT, UA
from run_multivenue_glft import Market, diagnose_uncalibrated_tape
from run_beta_one_direct_gap import read_local_bbo

NS=1_000_000
WARMUP_SECONDS=600
TRADE_SECONDS=3000


def trade_activity(venue,symbol,day):
    yyyy,mm,dd=day.split("-")
    url=f"{ROOT}/{venue}/trades/{yyyy}/{mm}/{dd}/{symbol}.csv.gz"
    start_us=int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp()*NS)
    cnt=0;buys=0;sells=0;total_base_qty=0.;bad=0;last_time=None
    with urllib.request.urlopen(urllib.request.Request(url,headers=UA),timeout=120) as response:
        with io.TextIOWrapper(gzip.GzipFile(fileobj=response),encoding="utf-8",newline="") as f:
            for row in csv.DictReader(f):
                t=int(row["local_timestamp"])
                if t>=start_us+(WARMUP_SECONDS+TRADE_SECONDS)*NS:break
                if t<start_us+WARMUP_SECONDS*NS:continue
                if last_time is not None and t<last_time:bad+=1
                last_time=t
                q=float(row["amount"])
                if not math.isfinite(q) or q<=0:bad+=1
                total_base_qty+=max(q,0) if math.isfinite(q) else 0.
                cnt+=1
                buys+=row["side"]=="buy";sells+=row["side"]=="sell"
                if cnt>2_000_000:
                    raise RuntimeError("Too many 50min trades for bounded admission scan")
    return {"trades_50min":cnt,"buy_trades":buys,"sell_trades":sells,
            "amount_sum_raw_units":total_base_qty,"nonmonotonic_or_bad_trade_rows":bad,
            "note":"Trade count is NOT proxy for fill count or GLFT distance-bin identifiability"}


def inspect(day,coin,venue):
    if coin not in ("DOGE","XRP"):raise ValueError("Only prescreen-approved DOGE/XRP")
    if venue not in ("okex-swap","bitget-futures"):raise ValueError("Invalid venue")
    spec_meta=current_spec(venue,coin)
    tick=live_tick(spec_meta,venue)
    if tick is None:raise ValueError("No trusted current tick for historical QA")
    if venue=="okex-swap":
        lot=float(spec_meta["lotSz"])
        ctval=float(spec_meta["ctVal"])
        if spec_meta["ctValCcy"]!=coin:raise ValueError("Unsupported OKX contract value currency")
    else:
        lot=float(spec_meta["sizeMultiplier"])
        ctval=1.0
    if not (0<tick and 0<lot and 0<ctval):
        raise ValueError("Nonpositive target instrument unit")
    symbol=MARKETS[coin][0 if venue=="okex-swap" else 1]
    s=Market(venue,symbol,coin,tick,lot,ctval,.0002,.0005 if venue=="okex-swap" else .0006,
             "live spec for prescreen, historical not yet certified")
    date_us=int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp()*NS)
    end_us=date_us+ (WARMUP_SECONDS+5)*NS
    with TemporaryDirectory(prefix="sparse_coin_fit_") as tmp:
        folder=Path(tmp)
        paths={kind:folder/f"{kind}.csv" for kind in SCHEMAS}
        counts={}
        for kind in SCHEMAS:
            counts[kind]=extract_window(kind,day,end_us,paths[kind],max_rows=3_500_000,
                                         exchange=venue,symbol=symbol)
        require_unshifted_target_clock(counts)
        ot,om=read_local_bbo(paths["book_ticker"])
        # For historical quote precision, any price that cannot be represented
        # in the nominated *current* tick must fail closed.
        px=np.asarray(om)
        max_round_error=float(np.max(np.abs(px/tick*2 - np.round(px/tick*2))))
        if max_round_error>1e-5:
            raise ValueError("Mid-price violates current two-sided tick ladder; historical precision changed")
        low,high=float(np.min(px)),float(np.max(px))
        converted=folder/"fused.npz"
        events=tardis.convert_fuse(str(paths["trades"]),
                    str(paths["incremental_book_L2"]),str(paths["book_ticker"]),
                    tick_size=tick,lot_size=lot,output_filename=str(converted),
                    ss_buffer_size=max(1200,counts["incremental_book_L2"]["snapshot_rows"]+100),
                    snapshot_mode="process")
        validate_event_order(events)
        event_count=len(events)
        del events
        fit=diagnose_uncalibrated_tape(converted,low,high,s)
    activity=trade_activity(venue,symbol,day)
    return {"day":day,"coin":coin,"exchange":venue,"symbol":symbol,
            "contract_spec_assumption":spec_meta,
            "tick":tick,"lot_size":lot,"contract_value":ctval,
            "training_10min_feed_counts":counts,"official_hbt_fused_events":event_count,
            "official_hbt_10min_glft_initial_fit":fit,
            "trade_activity":activity,
            "admission_next_gate":(
                "candidate_fit_success_not_yet_crossvenue_test" if fit["valid"] else
                "fails_original_G0_identifiability"),
            "not_assessed":["source impulse 100ms sign","maker net PnL",
                            "historical instrument contract specification",
                            "future-day independent test"]}


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--coin",required=True,choices=["DOGE","XRP"])
    p.add_argument("--venue",required=True,choices=["okex-swap","bitget-futures"])
    p.add_argument("--day",required=True)
    p.add_argument("--out",default="reports/source_defense/deep_admission")
    a=p.parse_args()
    out=Path(a.out);out.mkdir(parents=True,exist_ok=True)
    result=inspect(a.day,a.coin,a.venue)
    target=out/f"{a.coin}_{a.venue}_{a.day}.json"
    target.write_text(json.dumps(result,indent=2,ensure_ascii=False),encoding="utf-8")
    print("DEEP_ADMISSION",json.dumps({
        "day":a.day,"coin":a.coin,"venue":a.venue,"fit":result["official_hbt_10min_glft_initial_fit"],
        "trades_50min":result["trade_activity"]["trades_50min"]},ensure_ascii=False))


if __name__=="__main__":
    main()
