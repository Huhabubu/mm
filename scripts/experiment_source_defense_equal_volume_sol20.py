"""Research SOL equal-volume G0: May-only integer half-tick calibration.

Uses original official HBT event-loop, BOTH arms use 20min pretrade and the
research 3-level GLFT estimator. The training target 80% is predeclared and
independent of June's observed c1 candidate. Frozen May-selected integer
half-spread is then evaluated against June z1_cancel_shield; failure to match
fee/fill <=3% is explicitly reported, not optimized on June.
"""
import argparse
import json
from datetime import datetime,timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
from hftbacktest import ROIVectorMarketDepthBacktest
from hftbacktest.data.utils import tardis
from hftbacktest.data.validation import validate_event_order

from check_okx_hft_data import SCHEMAS, extract_window, require_unshifted_target_clock
from run_multivenue_glft import MARKETS,build_asset,order_scenario
from run_beta_one_direct_gap import read_local_bbo,causal_60s_basis
from cross_venue_event_probe import read_binance_bbo,visible_timestamps
from experiment_source_defense_signal import past_grid_time_ema
from experiment_source_defense_engine import run_sparse_glft,summarize
from experiment_source_defense_v4_pilot import research_summary

MAY="2026-05-01"
JUNE="2026-06-01"
HALF_TICKS=(0,1,2,3,4)
TRAIN_STEPS=12000
TRADE_STEPS=30000
TRAIN_TARGET_RETENTION=.8


def replay(day,half_ticks):
    if day not in (MAY,JUNE):
        raise ValueError("Equal-volume experiment only May train/June test")
    spec=MARKETS[("okx","SOL")]
    start=int(datetime.fromisoformat(day).replace(
        tzinfo=timezone.utc).timestamp()*1_000_000)
    end=start+4205*1_000_000
    result={}
    with TemporaryDirectory(prefix="equalvol_sol20_") as temp:
        root=Path(temp)
        paths={k:root/(k+".csv") for k in SCHEMAS}
        feed={}
        for k in SCHEMAS:
            feed[k]=extract_window(k,day,end,paths[k],max_rows=5_000_000,
                                   exchange=spec.exchange,symbol=spec.symbol)
        require_unshifted_target_clock(feed)
        fused=root/"tape.npz"
        ev=tardis.convert_fuse(
            str(paths["trades"]),str(paths["incremental_book_L2"]),
            str(paths["book_ticker"]),tick_size=spec.tick,lot_size=spec.lot,
            output_filename=str(fused),
            ss_buffer_size=max(1200,feed["incremental_book_L2"]["snapshot_rows"]+100),
            snapshot_mode="process")
        validate_event_order(ev);del ev
        ot,om=read_local_bbo(paths["book_ticker"])
        b0,bm=read_binance_bbo(day,end,symbol="SOLUSDT")
        bt=visible_timestamps(b0,2.5)
        basis_t,basis=causal_60s_basis(ot,om,bt,bm)
        ema_t,ema=past_grid_time_ema(ot,om,bt,bm,1.)
        mid=float(np.median(om[ot<(start+1200*1_000_000)*1000]))
        scenario=order_scenario(spec,mid)
        for width in half_ticks:
            engine=ROIVectorMarketDepthBacktest([
                build_asset(fused,float(np.min(om)),float(np.max(om)),spec)])
            try:
                # No exposure to taker trades or Binance direction: beta=0,
                # sparse_mode=0. Pass typed arrays to Numba for unused branches.
                raw=run_sparse_glft(
                    engine,bt,bm,TRADE_STEPS,
                    rolling=True,gamma_code=scenario["gamma_code"],
                    read_binance=True,beta=0.,
                    okx_native_ns=ot,okx_native_mid=om,
                    basis_grid_ns=basis_t,basis_values=basis,
                    extra_half_ticks=float(width),
                    tick_size=spec.tick,contract_value=spec.ctval,
                    order_qty=scenario["order_qty"],
                    position_cap=scenario["position_cap"],
                    ema_grid_ns=ema_t,ema_values=ema,
                    sparse_mode=0,train_steps=TRAIN_STEPS,
                    robust_fit=True,earlier_day_prior_k=np.nan,
                    trade_visible_ns=np.empty(0,np.int64),
                    trade_signed_prefix=np.array([0.]),
                    trade_total_prefix=np.array([0.]))
                if raw[5]!=1:
                    raise RuntimeError("SOL research 20min initial fit failed in "+day)
                summary=research_summary(raw,50,scenario,TRAIN_STEPS)
                tr=summary["trading"]
                if abs(tr["fee_usdt"]-tr["trading_value_usdt"]*.0002)>1e-5:
                    raise AssertionError("Official maker ledger mismatch")
                result[str(width)]={
                    "day":day,"extra_half_ticks":width,
                    "fills":tr["engine_fills"],
                    "fees_usdt":tr["fee_usdt"],
                    "turnover_usdt":tr["trading_value_usdt"],
                    "net_usdt_including_2bps_fee":tr["end_net_marked_equity_usdt"],
                    "inventory_terminal_contracts":tr["end_position_contracts"],
                    "first_GLFT":summary["initial_parameters"],
                    "trade_window":"UTC00:20-01:10",
                    "no_external_direction":True}
                print("SOL20_EQUAL",day,width,
                      tr["engine_fills"],tr["fee_usdt"],
                      tr["end_net_marked_equity_usdt"],flush=True)
            finally:
                engine.close()
    return result


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--mode",choices=("train","test"),required=True)
    p.add_argument("--selection")
    p.add_argument("--target-summary")
    p.add_argument("--out",default="reports/source_defense/v4_equal_volume_SOL")
    a=p.parse_args()
    out=Path(a.out);out.mkdir(parents=True,exist_ok=True)
    if a.mode=="train":
        results=replay(MAY,HALF_TICKS)
        zero=results["0"]["fills"]
        if not zero:raise RuntimeError("Zero May G0 fills")
        chosen=min(HALF_TICKS,
             key=lambda t:(abs(results[str(t)]["fills"]/zero-
                               TRAIN_TARGET_RETENTION),t))
        d={"training_day":MAY,"candidate_training_day_data":False,
           "chosen_integer_half_ticks":chosen,
           "target_predeclared_retention":TRAIN_TARGET_RETENTION,
           "train_retention":results[str(chosen)]["fills"]/zero,
           "original_and_variants_fit":"20min 3plus research, UTC00:20-01:10",
           "calibration_source":"May original G0 only","results":results}
        (out/"sol20_equal_volume_train.json").write_text(
            json.dumps(d,ensure_ascii=False,indent=2,allow_nan=False))
        print("SOL20_TRAIN_SELECTED",json.dumps({k:v for k,v in d.items() if k!="results"}))
    else:
        if not a.selection or not a.target_summary:
            raise ValueError("Frozen earlier day May selection and June V4 results required")
        prior=json.loads(Path(a.selection).read_text())
        if prior["training_day"]!=MAY or prior["candidate_training_day_data"]:
            raise ValueError("Forbidden June tuning")
        chosen=int(prior["chosen_integer_half_ticks"])
        if chosen not in HALF_TICKS:
            raise ValueError("Selected half tick not in prior grid")
        reference=json.loads(Path(a.target_summary).read_text())
        target=reference["cases"]["SOL"]["models"]["z1_cancel_shield"]
        # Fixed z1_cancel_shield identified as June descriptive mechanical
        # candidate; its fill count is NEVER used to select May halfspread.
        result=replay(JUNE,(0,chosen) if chosen else (0,))
        base=result["0"]
        own=result[str(chosen)]
        if base["fills"]!=reference["cases"]["SOL"]["models"]["g0"]["fills"]:
            raise AssertionError("Mismatch between official June SOL 20min G0 windows")
        fill_err=abs(own["fills"]/target["fills"]-1)
        fee_err=abs(own["fees_usdt"]/target["fees_usdt"]-1)
        gross_own=own["net_usdt_including_2bps_fee"]+own["fees_usdt"]
        gross_target=(target["net_pnl_after_maker2bps_usdt"]+
                      target["fees_usdt"])
        d={
            "calibration_day":MAY,"test_day":JUNE,
            "frozen_integer_extra_half_ticks":chosen,
            "test_trade_window":"UTC00:20-01:10",
            "research_GLFT_3plus_same_for_all":True,
            "training_target_independent_of_June":True,
            "test_original_G0":base,
            "test_prior_widen_G0":own,
            "fixed_June_candidate":"z1_cancel_shield",
            "candidate_fills":target["fills"],"candidate_fees_usdt":target["fees_usdt"],
            "candidate_net_usdt_after_2bps":target["net_pnl_after_maker2bps_usdt"],
            "candidate_gross_pnl_usdt_diagnostic":gross_target,
            "g0_gross_pnl_usdt_diagnostic":gross_own,
            "test_fill_relative_mismatch":fill_err,
            "test_fee_relative_mismatch":fee_err,
            "strict_equal_volume_2pct":fill_err<=.02 and fee_err<=.02,
            "approx_equal_volume_3pct":fill_err<=.03 and fee_err<=.03,
            "selection_alpha_causal_identified":False,
            "limitations":"May-only G0 fit targeting 80percent cannot guarantee June count/fee equalization"}
        (out/"sol20_equal_volume_test.json").write_text(
            json.dumps(d,indent=2,ensure_ascii=False,allow_nan=False))
        md=(
            "# SOL 20min Research GLFT：May训练固定G0半价差，June严格等成交量检验\n\n"
            f"五月按预注册80%目标选择加宽 {chosen} 个原生Tick。\n"
            f"六月同窗原G0成交 {base['fills']} 笔，前期训练G0 {own['fills']} 笔，"
            f"候选z1_cancel_shield {target['fills']} 笔。\n"
            f"成交差 {fill_err:.2%}，手续费差 {fee_err:.2%}；"
            f"<=2%严格匹配 {d['strict_equal_volume_2pct']}，"
            f"<=3%近似匹配 {d['approx_equal_volume_3pct']}。\n"
            "未匹配不得称为Equal-Volume G0；会计毛差包含库存与队列，"
            "不能宣称纯因果Selection Alpha。\n")
        (out/"sol20_equal_volume_test.md").write_text(md)
        print(md)


if __name__=="__main__":
    main()
