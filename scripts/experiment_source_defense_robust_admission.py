"""Step3: read-only 6-date 10/20min official HBT robust GLFT admission.

No orders or trading PnL. Same 100ms trade-through estimator as main; 20min
pretrade changes its hypothetical trading start to 00:20 UTC. Record compact
70-distance-bin sufficient stats so the following aggregation may apply
strictly earlier-date priors without future leakage.
"""
import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
from hftbacktest import BUY_EVENT, ROIVectorMarketDepthBacktest
from hftbacktest.data.utils import tardis
from hftbacktest.data.validation import validate_event_order
from numba import njit

from check_okx_hft_data import SCHEMAS, extract_window, require_unshifted_target_clock
from experiment_source_defense_admission import live_tick
from experiment_source_defense_deep_admission import trade_activity
from qa_multivenue_data import current_spec
from run_multivenue_glft import Market, build_asset, order_scenario
from run_beta_one_direct_gap import read_local_bbo
from g0_glft_rolling import fit_recent_arrivals
from experiment_source_defense_robust_fit import sufficient_stats, research_fit

COINS=("SOL","DOGE","XRP","1000PEPE")
VENUES=("okex-swap","bitget-futures")
N_STEPS=12000
WARMUP_NS=1_200_000_000_000


@njit
def capture_1200s(engine, tick):
    arrivals=np.full(N_STEPS,np.nan)
    changes=np.full(N_STEPS,np.nan)
    last_mid=np.nan
    if engine.elapse(0)!=0:
        raise ValueError("Cannot initialize HBT historical tape")
    for j in range(N_STEPS):
        if engine.elapse(100_000_000)!=0:
            raise ValueError("HBT target data ended before 20min pretrade")
        far=-np.inf
        if np.isfinite(last_mid):
            for tr in engine.last_trades(0):
                pt=tr.px/tick
                if tr.ev & BUY_EVENT == BUY_EVENT:
                    far=max(far,pt-last_mid)
                else:
                    far=max(far,last_mid-pt)
        engine.clear_last_trades(0)
        arrivals[j]=far
        book=engine.depth(0)
        bid,ask=book.best_bid_tick,book.best_ask_tick
        if bid>0 and ask>bid:
            mid=(bid+ask)/2.
            changes[j]=mid-last_mid if np.isfinite(last_mid) else np.nan
            last_mid=mid
        else:
            changes[j]=np.nan
            last_mid=np.nan
    return arrivals,changes


def contract(coin,venue):
    if coin not in COINS or venue not in VENUES:
        raise ValueError("Unregistered research case")
    spec_symbol="PEPE" if coin=="1000PEPE" else coin
    spec=current_spec(venue,spec_symbol)
    if "error" in spec:raise ValueError("Live contract spec missing: "+str(spec))
    tick=live_tick(spec,venue)
    if venue=="okex-swap":
        lot=float(spec["lotSz"])
        ctval=float(spec["ctVal"])
        if spec.get("ctValCcy")!=spec_symbol:
            raise ValueError("Unverified OKX ctValCcy; refuse contract-unit inference")
        symbol=spec_symbol+"-USDT-SWAP"
    else:
        lot=float(spec["sizeMultiplier"])
        ctval=1.
        symbol=spec_symbol+"USDT"
    if not tick or tick<=0 or lot<=0 or ctval<=0:
        raise ValueError("Nonphysical contract specification")
    market=Market(venue,symbol,coin,tick,lot,ctval,.0002,.0005 if venue=="okex-swap" else .0006,
                  "current-live-spec; historical contract metadata unverified")
    return market,spec


def trial(day,coin,venue):
    market,meta=contract(coin,venue)
    us=int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp()*1_000_000)
    end=us+1205*1_000_000
    with TemporaryDirectory(prefix="research_fit_") as d:
        root=Path(d)
        paths={k:root/f"{k}.csv" for k in SCHEMAS}
        counts={}
        for kind in SCHEMAS:
            counts[kind]=extract_window(
                kind,day,end,paths[kind],max_rows=4_000_000,
                exchange=market.exchange,symbol=market.symbol)
        require_unshifted_target_clock(counts)
        ot,om=read_local_bbo(paths["book_ticker"])
        if len(om)<1200:
            raise ValueError("Too few native BBO updates for 20min study")
        # Ensure archived historical quote *observations* lie on the proposed
        # live tick grid; this still does not certify historical min tick.
        bad=float(np.max(np.abs(2*om/market.tick-np.round(2*om/market.tick))))
        if bad>1.e-4:
            raise ValueError(f"Historical quotes cannot be represented in nominated live tick: {bad}")
        low,high=float(np.min(om)),float(np.max(om))
        file=root/"tape.npz"
        evt=tardis.convert_fuse(
            str(paths["trades"]),str(paths["incremental_book_L2"]),
            str(paths["book_ticker"]),tick_size=market.tick,
            lot_size=market.lot,output_filename=str(file),
            ss_buffer_size=max(1200,counts["incremental_book_L2"]["snapshot_rows"]+100),
            snapshot_mode="process")
        validate_event_order(evt)
        events=len(evt)
        del evt
        engine=ROIVectorMarketDepthBacktest([build_asset(file,low,high,market)])
        try:
            arrivals,changes=capture_1200s(engine,market.tick)
        finally:
            engine.close()
    stat10=sufficient_stats(arrivals[:6000],changes[:6000])
    stat20=sufficient_stats(arrivals,changes)
    orig=fit_recent_arrivals(arrivals[:6000],changes[:6000])
    prelim10=research_fit(stat10,None)
    prelim20=research_fit(stat20,None)
    mid=float(np.median(om[ot < us*1000 +600*1_000_000_000]))
    scenario=order_scenario(market,mid)
    try:
        activity=trade_activity(venue,market.symbol,day)
    except Exception as exc:
        activity={"error":f"{type(exc).__name__}: {exc}","trades_50min":None}
    return {
        "status":"stats_complete",
        "coin":coin,"day":day,"venue":venue,"market":market.symbol,
        "historical_contract_metadata_provisional":True,
        "contract_live_spec":meta,
        "price_tick":market.tick,"lot_size":market.lot,
        "ctval_native_coin_per_contract":market.ctval,
        "order_notional_scenario":scenario,
        "source_price_divisor":1000 if coin=="1000PEPE" else 1,
        "source_raw_quantity_multiplier_base_coin":1000 if coin=="1000PEPE" else 1,
        "target_price_unit":"USDT/PEPE" if coin=="1000PEPE" else "USDT/"+coin,
        "training_book_rows":counts,
        "converted_hbt_event_count":events,
        "original_10min_fit":{
            "valid":bool(orig[0]),"A":float(orig[1]),"k":float(orig[2]),
            "sigma":float(orig[3]),"positive_levels":int(orig[4]),
            "crossing_intervals":int(orig[5]),"extreme_intervals":int(orig[6])},
        "research_10min_without_prior":prelim10,
        "research_20min_without_prior":prelim20,
        "sufficient_stats_10min":stat10,
        "sufficient_stats_20min":stat20,
        "trade_activity_original_00_10_to_01_00":activity,
        "hypothetical_20min_training_trade_start":"UTC00:20, not comparable with UTC00:10 baseline",
        "no_orders_placed":True}


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--day",required=True)
    p.add_argument("--coin",required=True,choices=COINS)
    p.add_argument("--venue",required=True,choices=VENUES)
    p.add_argument("--out",default="reports/source_defense/step3_cases")
    a=p.parse_args()
    try:
        payload=trial(a.day,a.coin,a.venue)
    except Exception as exc:
        payload={"day":a.day,"coin":a.coin,"venue":a.venue,
                 "status":"failed_to_extract_historical_sufficient_stats",
                 "error":f"{type(exc).__name__}: {exc}",
                 "original_fit_valid":False,
                 "research_fit_valid":False,
                 "fail_closed":True}
    out=Path(a.out);out.mkdir(parents=True,exist_ok=True)
    path=out/f"{a.coin}_{a.venue}_{a.day}.json"
    path.write_text(json.dumps(payload,ensure_ascii=False,indent=2),encoding="utf8")
    print("FIT_ADMISSION",json.dumps({
       "day":a.day,"coin":a.coin,"venue":a.venue,"status":payload["status"],
       "original_fit_valid":payload.get("original_10min_fit",{}).get("valid"),
       "n10":payload.get("sufficient_stats_10min",{}).get("positive_distance_levels"),
       "n20":payload.get("sufficient_stats_20min",{}).get("positive_distance_levels"),
       "error":payload.get("error")},ensure_ascii=False))


if __name__=="__main__":
    main()
