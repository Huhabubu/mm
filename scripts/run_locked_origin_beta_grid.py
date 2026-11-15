"""Paired official HBT GLFT replay: 600s time-weighted basis and locked origins.

Only the cross-venue signal changes. All beta variants retain identical native
market tapes, GLFT model calibration, inventory limits, latency, and maker fee.
"""
import argparse
import json
import tempfile
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from check_okx_hft_data import SCHEMAS, extract_window, require_unshifted_target_clock
from cross_venue_event_probe import read_binance_bbo, visible_timestamps
from g0_glft_event_driven import run_event_glft, summarize
from locked_origin_basis import NS, prepare_signal
from run_beta_one_direct_gap import read_local_bbo
from run_multivenue_glft import MARKETS, build_asset, order_scenario, diagnose_uncalibrated_tape

BETAS = (0.0, 0.25, 0.5, 0.75, 1.0)


def summarize_signal(signal, trade_end_ns):
    times = signal["source_event_ns"]
    passed = signal["source_pass"]
    over = signal["source_over_196"]
    ready = signal["anchor_ready"]
    ends = np.r_[times[1:], trade_end_ns]
    elapsed = np.maximum(0, np.minimum(ends, trade_end_ns) - times)
    prev = np.r_[False, passed[:-1]]
    onsets = int(np.count_nonzero(passed & ~prev))
    return {
        "basis_updates": len(signal["basis_ns"]),
        "basis_600s_time_coverage_min": float(np.min(signal["basis_time_coverage"])),
        "basis_std_min_usdt_per_btc": float(np.min(signal["basis_sigma"])),
        "basis_std_median_usdt_per_btc": float(np.median(signal["basis_sigma"])),
        "basis_std_max_usdt_per_btc": float(np.max(signal["basis_sigma"])),
        "native_bbo_union_events": len(times),
        "events_with_locked_anchor": int(np.count_nonzero(ready)),
        "events_above_1p96_sigma_with_anchor": int(np.count_nonzero(over)),
        "events_passing_binance_origin_gate": int(np.count_nonzero(passed)),
        "distinct_gate_onsets": onsets,
        "gate_active_time_seconds": float(np.sum(elapsed[passed]) / NS),
        "gate_active_fraction_time": float(np.sum(elapsed[passed]) /
                                           max(trade_end_ns - signal["basis_ns"][0], 1)),
        "note": "Native-BBO source events; final HBT target-mid revalidation can reject more.",
    }


def run(day, minutes=50, delay_ms=2.5, out="reports"):
    from hftbacktest import ROIVectorMarketDepthBacktest
    from hftbacktest.data.utils import tardis
    from hftbacktest.data.validation import validate_event_order

    spec=MARKETS[("okx","BTC")]
    if datetime.strptime(day, "%Y-%m-%d").day != 1 or minutes not in (5,50):
        raise ValueError("Use free Tardis first-of-month day with 5 or 50min trade")
    start_ns=int(datetime.strptime(day,"%Y-%m-%d").replace(
        tzinfo=timezone.utc).timestamp()) * NS
    first_ns=start_ns+600*NS
    last_ns=first_ns+minutes*60*NS
    end_us=(last_ns+5_000_000)//1000
    report={"status":"started","day":day,"venue":"okx","coin":"BTC",
            "warmup_minutes":10,"trade_minutes":minutes,
            "beta_grid":list(BETAS),"extra_binance_delay_ms":delay_ms,
            "window_seconds":600,"refit_seconds":5,
            "basis":"continuous time weighted Binance-native minus OKX-native midpoint",
            "sigma":"continuous time weighted population std of 600s raw spread",
            "normal_band_sigma":1.0, "signal_threshold_sigma":1.96,
            "source_rule":"freeze both native mids outside 1sigma, trigger on >1.96sigma with Binance signed dominant",
            "no_quote_age_exclusion":True,
            "legacy_p90_p50_not_used":True,
            "market":asdict(spec),
            "net_equity_includes_official_hbt_fees":True,
            "funding_and_liquidation_costs_not_modelled":True,
            "prior_monthly_data_explored_not_new_holdout":True}
    result_path=Path(out)/f"locked_origin_beta_grid_okx_BTC_{day}_{minutes}m.json"
    result_path.parent.mkdir(parents=True,exist_ok=True)
    try:
        with tempfile.TemporaryDirectory(prefix="mm_locked_beta_") as workdir:
            root=Path(workdir)
            paths={}
            counts={}
            for kind in SCHEMAS:
                paths[kind]=root/f"{kind}.csv"
                counts[kind]=extract_window(
                    kind,day,end_us,paths[kind],max_rows=3_000_000,
                    exchange=spec.exchange,symbol=spec.symbol)
            require_unshifted_target_clock(counts)
            npz=root/"fused.npz"
            events=tardis.convert_fuse(
                str(paths["trades"]),str(paths["incremental_book_L2"]),
                str(paths["book_ticker"]),tick_size=spec.tick,lot_size=spec.lot,
                output_filename=str(npz),
                ss_buffer_size=max(1200,counts["incremental_book_L2"]["snapshot_rows"]+100),
                snapshot_mode="process")
            validate_event_order(events)
            total_events=len(events)
            del events
            ot,om=read_local_bbo(paths["book_ticker"])
            bt_local,bm=read_binance_bbo(day,end_us,symbol="BTCUSDT")
            bt=visible_timestamps(bt_local,delay_ms)
            signal=prepare_signal(bt,bm,ot,om,first_ns,last_ns)
            signal_info=summarize_signal(signal,last_ns)
            if signal_info["basis_updates"] != minutes*12:
                raise RuntimeError("5sec baseline clock count mismatch")
            if signal_info["events_passing_binance_origin_gate"] == 0:
                raise RuntimeError("No source-origin candidates; check sigma/window")
            pretrade=om[ot<first_ns]
            if not len(pretrade):
                raise RuntimeError("Missing initial OKX mid")
            scenario=order_scenario(spec,float(np.median(pretrade)))
            low,high=float(np.min(om)),float(np.max(om))
            report.update({"tardis_rows":counts,"fused_hbt_events":int(total_events),
                           "binance_bbo_rows":len(bt),"okx_native_bbo_rows":len(ot),
                           "scenario":scenario,"signal_diagnostic":signal_info,
                           "variants":{}})
            for beta in BETAS:
                key=f"beta_{beta:g}"
                engine=ROIVectorMarketDepthBacktest([build_asset(npz,low,high,spec)])
                try:
                    raw=run_event_glft(
                        engine,bt,bm,minutes*600,
                        rolling=True,gamma_code=scenario["gamma_code"],
                        read_binance=True,beta=beta,
                        okx_native_ns=ot,okx_native_mid=om,
                        basis_grid_ns=signal["basis_ns"],
                        basis_values=signal["basis_mean"],
                        filter_enter_ticks=0.0,
                        wall_search_ticks=0,tick_size=spec.tick,
                        contract_value=spec.ctval,
                        order_qty=scenario["order_qty"],
                        position_cap=scenario["position_cap"],
                        locked_source_ns=signal["source_event_ns"],
                        locked_base_b=signal["base_b"],
                        locked_base_m=signal["base_m"],
                        locked_source_allowed=signal["source_pass"],
                        locked_basis_sigma=signal["basis_sigma"])
                    if int(raw[5])!=1:
                        report["status"]="no_glft_calibration"
                        report["calibration_diagnostic"]=diagnose_uncalibrated_tape(
                            npz,low,high,spec)
                        print("NO_GLFT_CALIBRATION",day,report["calibration_diagnostic"],flush=True)
                        return report
                    item=summarize(raw,minutes,True,beta,
                                   scenario["position_cap"],scenario["order_qty"])
                    report["variants"][key]=item
                    t=item["trading"]
                    print("BETA_RESULT",day,beta,"NET",t["end_net_marked_equity_usdt"],
                          "FEE",t["fee_usdt"],"FILLS",t["engine_fills"],
                          "CANCELS",t["cancel_requests"],"SUBMITS",t["submissions"],
                          flush=True)
                finally:
                    engine.close()
            ref=report["variants"]["beta_0"]
            for key,item in report["variants"].items():
                if (item["calibration_trace"]!=ref["calibration_trace"] or
                    item["samples"]!=ref["samples"] or
                    item["time_count"]["binance"]!=ref["time_count"]["binance"]):
                    raise RuntimeError(f"Changed non-signal control for {key}")
            net0=ref["trading"]["end_net_marked_equity_usdt"]
            report["paired_net_delta_vs_beta0"]={
                k:float(x["trading"]["end_net_marked_equity_usdt"]-net0)
                for k,x in report["variants"].items()}
            report["status"]="complete"
    except Exception as ex:
        report["status"]="failed"
        report["error"]=f"{type(ex).__name__}: {ex}"
        raise
    finally:
        result_path.write_text(json.dumps(report,ensure_ascii=False,indent=2)+"\n",
                               encoding="utf-8")
        print("GATE_BETA_REPORT",day,report["status"],str(result_path),flush=True)
    return report


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--day",default="2026-11-01")
    p.add_argument("--minutes",type=int,default=50,choices=(5,50))
    p.add_argument("--extra-binance-delay-ms",type=float,default=2.5)
    p.add_argument("--report-dir",default="reports")
    args=p.parse_args()
    run(args.day,args.minutes,args.extra_binance_delay_ms,args.report_dir)


if __name__=="__main__":
    main()
