"""Gate BTC historical replay: compare frozen legacy and shared-core HBT behavior.

No synthetic fills. Reads the same Tardis UTC month-first market data, creates
independent official HBT exchanges, compares every actual fill attribute, sample
Recorder state, counts and exact terminal fee-inclusive equity.
Does NOT claim order-request identity until request-level tracing is implemented.
"""
import argparse
import math
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
from hftbacktest import Recorder, ROIVectorMarketDepthBacktest
from hftbacktest.data.utils import tardis
from hftbacktest.data.validation import validate_event_order

from core_glft_engine import run_core_glft
from gate_origin_glft_strategy import run_gate_origin_glft
from run_gate_btc_origin_glft import (
    SCHEMAS, BINANCE_DELAY_MS, build_asset, exact_moments_5s,
    fetch_contract, load_gate_mid, read_binance_bbo, require_unshifted_target_clock,
    safe_gate_csv, scenario, visible_timestamps,
)


def assert_exact_legacy_result(old, new, old_rec, new_rec):
    for i, (a, b) in enumerate(zip(old, new)):
        # At raw[-2][4], core supports a longer optional diagnostics probe
        # schema, but probes have no impact on actual market-making actions.
        if i == len(old)-2:
            if len(a) != len(b) or any(not np.array_equal(x,y,equal_nan=True)
                                       for x,y in zip(a[:4],b[:4])):
                raise AssertionError("fill local clocks/prices/quantities mismatch")
            continue
        if isinstance(a,np.ndarray):
            if not np.array_equal(a,b,equal_nan=True):
                raise AssertionError(f"exact HBT result mismatch raw[{i}]")
        elif isinstance(a,tuple):
            raise AssertionError(f"Unexpected tuple at raw[{i}]")
        elif isinstance(a,(float,np.floating)) and math.isnan(a):
            if not math.isnan(b):
                raise AssertionError(f"NaN mismatch raw[{i}]")
        elif a != b:
            raise AssertionError(f"exact HBT result mismatch raw[{i}]: {a} != {b}")
    for key in old_rec.dtype.names:
        if not np.array_equal(old_rec[key],new_rec[key],equal_nan=True):
            raise AssertionError(f"Recorder trace differs: {key}")
    print("CORE_LEGACY_GOLDEN_EXACT",len(old[-5]),"fills",
          old[7],"submissions",old[8],"cancels",
          old[18],"fee_inclusive_end_equity")


def run_one(day="2026-11-01",minutes=50):
    y=datetime.strptime(day,"%Y-%m-%d").replace(tzinfo=timezone.utc)
    if y.day!=1 or minutes!=50:
        raise ValueError("One historical month-first 50-minute deterministic probe only")
    day_us=int(y.timestamp()*1_000_000)
    trade_start_us=day_us+600_000_000
    trade_stop_us=trade_start_us+minutes*60_000_000
    end_us=trade_stop_us+5_000_000
    spec=fetch_contract("BTC_USDT")
    with tempfile.TemporaryDirectory(prefix="gate_core_parity_") as temp:
        root=Path(temp)
        paths,counts={},{}
        for kind in SCHEMAS:
            paths[kind]=root/f"{kind}.csv"
            counts[kind]=safe_gate_csv(kind,day,end_us,paths[kind],6_000_000)
        require_unshifted_target_clock(counts)
        ot,om=load_gate_mid(paths["book_ticker"])
        b_local,bm=read_binance_bbo(day,end_us,symbol="BTCUSDT")
        bt=visible_timestamps(b_local,BINANCE_DELAY_MS)
        grid,basis,sigma,_=exact_moments_5s(
            bt,bm,ot,om,trade_start_us*1000,trade_stop_us*1000)
        pre=om[ot<trade_start_us*1000]
        cfg=scenario(float(np.median(pre)),spec)
        fused=root/"fused.npz"
        events=tardis.convert_fuse(
            str(paths["trades"]),str(paths["incremental_book_L2"]),
            str(paths["book_ticker"]),tick_size=cfg["tick"],
            lot_size=float(cfg["lot"]),output_filename=str(fused),
            ss_buffer_size=max(1000,counts["incremental_book_L2"]["snapshot_rows"]+100),
            snapshot_mode="process")
        validate_event_order(events)
        del events
        low=float(np.min(om));high=float(np.max(om))
        for beta,mode in ((0.,0),(1.,1),(1.,2)):
            values=[]
            for function in (run_gate_origin_glft,run_core_glft):
                engine=ROIVectorMarketDepthBacktest(
                    [build_asset([fused],low,high,cfg)])
                rec=Recorder(engine.num_assets,minutes*600+10)
                try:
                    raw=function(
                        engine,bt,bm,minutes*600,beta,grid,basis,sigma,mode,
                        cfg["gamma_code"],cfg["tick"],cfg["ctval"],
                        cfg["qty"],cfg["position_cap"],rec.recorder,
                        trade_stop_us*1000)
                    recording=rec.get(0).copy()
                    values.append((raw,recording))
                finally:
                    engine.close()
            assert_exact_legacy_result(
                values[0][0],values[1][0],values[0][1],values[1][1])
            print("CORE_LEGACY_MATCH_MODEL",day,beta,mode,flush=True)


if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--day",default="2026-11-01")
    p.add_argument("--minutes",type=int,default=50)
    x=p.parse_args()
    run_one(x.day,x.minutes)
