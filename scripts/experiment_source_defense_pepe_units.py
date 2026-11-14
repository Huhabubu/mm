"""Step3 standalone 1000PEPEUSDT vs PEPE contract unit/scale check.

No trading or fitting. Source prices in USDT/1000PEPE are converted to
USDT/PEPE by /1000; base quantity is multiplied by 1000. Live contract
specifications cannot certify historical lot changes.
"""
import argparse
import csv
import gzip
import io
import json
import math
from datetime import datetime,timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import urllib.request

import numpy as np

from experiment_source_defense_admission import live_tick
from qa_multivenue_data import current_spec
from check_okx_hft_data import extract_window
from run_beta_one_direct_gap import read_local_bbo
from cross_venue_event_probe import read_binance_bbo

BASE="https://datasets.tardis.dev/v1"


def check_units(day,venue):
    if venue not in ("okex-swap","bitget-futures"):
        raise ValueError("Unknown target venue")
    spec=current_spec(venue,"PEPE")
    if "error" in spec:raise ValueError("Target PEPE contract metadata unavailable: "+str(spec))
    tick=live_tick(spec,venue)
    if venue=="okex-swap":
        ctval=float(spec["ctVal"])
        lot=float(spec["lotSz"])
        ctvalccy=spec.get("ctValCcy")
        if ctvalccy!="PEPE":
            raise ValueError(f"Unverified OKX contract size unit: {ctvalccy}")
        symbol="PEPE-USDT-SWAP"
        raw_qty_unit="OKX contract count"
    else:
        ctval=1.0
        lot=float(spec["sizeMultiplier"])
        ctvalccy="PEPE (base units; provisional)"
        symbol="PEPEUSDT"
        raw_qty_unit="Bitget base PEPE amount (requires historical specs)"
    if not (tick and tick>0 and lot>0 and ctval>0):
        raise ValueError("Invalid target unit ladder")
    day_us=int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp()*1_000_000)
    end=day_us+75_000_000
    with TemporaryDirectory(prefix="pepe_units_") as temp:
        target=Path(temp)/"target.csv"
        meta=extract_window("book_ticker",day,end,target,
               exchange=venue,symbol=symbol,max_rows=200_000)
        ot,om=read_local_bbo(target)
        bt,bm=read_binance_bbo(day,end,symbol="1000PEPEUSDT")
    if len(bm)<2 or len(om)<2:
        raise ValueError("Cannot verify 1000PEPE price multiplier without both historical streams")
    # Price check at first-minute median; NOT a latent no-arbitrage assumption.
    src=float(np.median(bm))
    target_px=float(np.median(om))
    norm=src/1000.
    gap_ratio=abs(norm/target_px-1.)
    # Live-spec estimated $80 notional order, nearest valid lot; no orders placed.
    raw_lots=80./(target_px*ctval*lot)
    lots=max(1,int(math.floor(raw_lots+.5)))
    qty=lots*lot
    actual_notional=qty*ctval*target_px
    target_tick_bps=1.e4*tick/target_px
    binance_base_volume_multiplier=1000.0
    return {
        "day":day,"venue":venue,"target_symbol":symbol,
        "binance_symbol":"1000PEPEUSDT",
        "binance_raw_price_usdt_per_1000pepe":src,
        "binance_normalized_price_usdt_per_pepe":norm,
        "binance_price_divisor":1000,
        "binance_raw_qty_unit":"1000PEPE (1000 native PEPE units)",
        "binance_raw_qty_to_target_base_pepe":binance_base_volume_multiplier,
        "target_mid_usdt_per_pepe":target_px,
        "crossvenue_normalized_median_fraction_gap":gap_ratio,
        "normalization_scale_check_pass_lt_5pct":bool(gap_ratio<.05),
        "target_contract_current_live_spec":spec,
        "historical_spec_unverified":True,
        "target_qty_unit":raw_qty_unit,"tick_usdt_per_pepe":tick,
        "lot_size_raw_target_qty":lot,"contract_value_pepe_per_contract":ctval,
        "contract_value_currency":ctvalccy,
        "relative_tick_bps":target_tick_bps,
        "example_requested_notional_usdt":80,
        "example_qty_raw_lots":lots,
        "example_qty_raw_target_units":qty,
        "example_order_base_pepe_qty":qty*ctval,
        "example_order_notional_usdt":actual_notional,
        "target_feed_counts_75s":meta,
        "admission_status":"price_normalization_only; historical metadata and fitter remain to verify"}


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--day",default="2026-06-01")
    p.add_argument("--venue",required=True,choices=("okex-swap","bitget-futures"))
    p.add_argument("--out",default="reports/source_defense/pepe_units")
    a=p.parse_args()
    item=check_units(a.day,a.venue)
    out=Path(a.out);out.mkdir(parents=True,exist_ok=True)
    (out/f"{a.venue}_{a.day}.json").write_text(
        json.dumps(item,indent=2,ensure_ascii=False),encoding="utf8")
    print("PEPE_UNIT_CHECK",json.dumps({
        "venue":a.venue,"normalized_price_ratio":item["crossvenue_normalized_median_fraction_gap"],
        "target_tick":item["tick_usdt_per_pepe"],"target_lot":item["lot_size_raw_target_qty"],
        "ctval":item["contract_value_pepe_per_contract"],
        "target_raw_qty":item["example_qty_raw_target_units"],
        "normalized_pepe_qty":item["example_order_base_pepe_qty"],
        "target_notional_usdt":item["example_order_notional_usdt"],
        "historical_unverified":True},ensure_ascii=False))


if __name__=="__main__":
    main()
