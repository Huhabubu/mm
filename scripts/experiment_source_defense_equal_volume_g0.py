"""Training-date-only volume matching for an independent no-Binance G0 baseline.

Choose extra static half spread using *only May 1* simulated G0 fill ratio
target 80%. June 1 uses the frozen May selection. HBT is the only matcher.
Never iterate June bps until <2% is achieved: report failure honestly.
"""
import argparse
import json
import math
from datetime import datetime,timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
from hftbacktest import ROIVectorMarketDepthBacktest
from hftbacktest.data.utils import tardis
from hftbacktest.data.validation import validate_event_order

from check_okx_hft_data import SCHEMAS, extract_window, require_unshifted_target_clock
from cross_venue_event_probe import read_binance_bbo, visible_timestamps
from run_beta_one_direct_gap import causal_60s_basis, read_local_bbo
from run_multivenue_glft import MARKETS, build_asset, order_scenario
from g0_glft_event_driven import run_event_glft, summarize

CANDIDATE_HALF_SPREAD_BPS=(0.0,.05,.1,.2,.4,.8)
TRAIN_DAY="2026-05-01"
TEST_DAY="2026-06-01"
TRAIN_FILL_RETENTION_TARGET=.80


def run_once(engine,bt,bm,ot,om,basis_t,basis,scenario,tick,ctval,
             extra_half_ticks):
    raw=run_event_glft(engine,bt,bm,30_000,True,scenario["gamma_code"],
               True,0.,ot,om,basis_t,basis,0.,0.,10.,250_000_000,
               float(extra_half_ticks),None,0,tick,ctval,
               scenario["order_qty"],scenario["position_cap"],None)
    if int(raw[5])!=1:
        raise RuntimeError("No original GLFT calibration on matching train/test date")
    return summarize(raw,50,True,0.,scenario["position_cap"],scenario["order_qty"])


def replay(day,half_bps_values):
    spec=MARKETS[("okx","BTC")]
    start=int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp()*1e6)
    end=start+3605*1_000_000
    with TemporaryDirectory(prefix="equal_volume_g0_") as temp:
        folder=Path(temp)
        paths={k:folder/(k+".csv") for k in SCHEMAS}
        counts={}
        for kind in SCHEMAS:
            counts[kind]=extract_window(kind,day,end,paths[kind],
              max_rows=3_000_000,exchange=spec.exchange,symbol=spec.symbol)
        require_unshifted_target_clock(counts)
        fused=folder/"tape.npz"
        evt=tardis.convert_fuse(str(paths["trades"]),str(paths["incremental_book_L2"]),
                                str(paths["book_ticker"]),tick_size=spec.tick,
                                lot_size=spec.lot,output_filename=str(fused),
                                ss_buffer_size=max(1200,counts["incremental_book_L2"]["snapshot_rows"]+100),
                                snapshot_mode="process")
        validate_event_order(evt)
        del evt
        ot,om=read_local_bbo(paths["book_ticker"])
        bt0,bm=read_binance_bbo(day,end,symbol="BTCUSDT")
        bt=visible_timestamps(bt0,2.5)
        basis_t,basis=causal_60s_basis(ot,om,bt,bm)
        training_end=start*1000+600*1_000_000_000
        mid=float(np.median(om[ot<training_end]))
        scenario=order_scenario(spec,mid)
        result={}
        for x in half_bps_values:
            ticks=int(round(mid*float(x)*1e-4/spec.tick))
            if x>0:ticks=max(1,ticks)
            engine=ROIVectorMarketDepthBacktest([build_asset(fused,
                            float(om.min()),float(om.max()),spec)])
            try:
                summary=run_once(engine,bt,bm,ot,om,basis_t,basis,scenario,
                                 spec.tick,spec.ctval,ticks)
                tr=summary["trading"]
                result[str(x)]={
                    "half_spread_bps_training_median":float(x),
                    "extra_half_ticks":ticks,
                    "official_fills":tr["engine_fills"],
                    "fee_usdt":tr["fee_usdt"],
                    "gross_pnl_usdt":tr["end_net_marked_equity_usdt"]+tr["fee_usdt"],
                    "net_pnl_including_fees_usdt":tr["end_net_marked_equity_usdt"],
                    "turnover_usdt":tr["trading_value_usdt"],
                    "cancel_requests":tr["cancel_requests"],
                    "end_inventory_contracts":tr["end_position_contracts"],
                    "original_glft_parameters":summary["initial_parameters"]}
            finally:
                engine.close()
            print("G0_EQUAL_VOLUME",day,x,ticks,result[str(x)]["official_fills"],
                  result[str(x)]["net_pnl_including_fees_usdt"],flush=True)
    return result


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--mode",required=True,choices=("train","test"))
    parser.add_argument("--selection",default="")
    parser.add_argument("--out",default="reports/source_defense/equal_volume")
    args=parser.parse_args()
    out=Path(args.out);out.mkdir(parents=True,exist_ok=True)
    if args.mode=="train":
        result=replay(TRAIN_DAY,CANDIDATE_HALF_SPREAD_BPS)
        n0=result["0.0"]["official_fills"]
        if n0<=0:raise ValueError("G0 training had zero fills")
        best=min(CANDIDATE_HALF_SPREAD_BPS,
                 key=lambda x:(abs(result[str(x)]["official_fills"]/n0-
                                   TRAIN_FILL_RETENTION_TARGET),x))
        selection={
            "source_train_day":TRAIN_DAY,
            "selected_static_extra_half_bps":best,
            "target_predeclared_retention":TRAIN_FILL_RETENTION_TARGET,
            "observed_train_retention":result[str(best)]["official_fills"]/n0,
            "training_results":result,
            "test_day_data_used_to_select_parameter":False,
            "no_directional_binance_signal":True}
        (out/"equal_volume_train.json").write_text(
            json.dumps(selection,ensure_ascii=False,indent=2),encoding="utf8")
        print("TRAIN_G0_SELECTION",json.dumps({
            k:v for k,v in selection.items() if k!="training_results"}))
    else:
        if not args.selection:
            raise ValueError("Need a May-only frozen selection")
        selection=json.loads(Path(args.selection).read_text(encoding="utf8"))
        if (selection["source_train_day"]!=TRAIN_DAY or
                selection["test_day_data_used_to_select_parameter"]):
            raise ValueError("Only earlier-date training selection permitted")
        bps=selection["selected_static_extra_half_bps"]
        test=replay(TEST_DAY,(0.0,bps))
        if str(bps) not in test:
            raise ValueError("Failed to replay selected widened G0")
        # June C3 results were already frozen by the earlier independent
        # official HBT pilot; only for validation, not for parameter choice.
        result={"calibration_date":TRAIN_DAY,"evaluation_date":TEST_DAY,
                "frozen_selected_half_spread_bps":bps,"test_result":test,
                "match_to_c3_cancel_requires_independent_report":True,
                "fixed_selection_calibrated_before_test":True}
        (out/"equal_volume_test.json").write_text(
            json.dumps(result,ensure_ascii=False,indent=2),encoding="utf8")
        print("TEST_G0_EQUAL_VOLUME",json.dumps({
            "frozen_half_bps":bps,"g0_fills":test["0.0"]["official_fills"],
            "equal_g0_fills":test[str(bps)]["official_fills"],
            "equal_g0_fee":test[str(bps)]["fee_usdt"],
            "equal_g0_net":test[str(bps)]["net_pnl_including_fees_usdt"]}))


if __name__=="__main__":
    main()
