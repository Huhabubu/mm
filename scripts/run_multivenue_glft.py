"""Four market/symbol GLFT comparisons using official HBT and official Recorder.

This runner leaves the original BTC/OKX pipeline unchanged. Contract specs are
predeclared scenarios, verified against live public metadata, not archived specs.
No custom matching, synthetic execution, or fee-free headline outcomes.
"""
import argparse
import json
import math
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from check_okx_hft_data import SCHEMAS, extract_window
from cross_venue_event_probe import read_binance_bbo, visible_timestamps
from g0_glft_event_driven import run_event_glft, summarize
from run_beta_one_direct_gap import (
    NS, causal_60s_basis, quote_gap_coverage, read_local_bbo,
)

# 2026-11-12 public-spec scenarios; independently audit historical specifications.
# Bitget Tardis quantity is denominated in base coin; OKX is in contract count.
@dataclass(frozen=True)
class Market:
    exchange: str
    symbol: str
    coin: str
    tick: float
    lot: float
    ctval: float
    maker_fee: float
    taker_fee: float
    source: str


MARKETS = {
    ("okx", "BTC"): Market("okex-swap", "BTC-USDT-SWAP", "BTC", .1, .01, .01,
                           .0002, .0005, "2026-11-12 OKX public/instruments"),
    ("okx", "SOL"): Market("okex-swap", "SOL-USDT-SWAP", "SOL", .01, .01, 1.,
                           .0002, .0005, "2026-11-12 OKX public/instruments"),
    ("bitget", "BTC"): Market("bitget-futures", "BTCUSDT", "BTC", .1, .0001, 1.,
                              .0002, .0006, "2026-11-12 Bitget mix/market/contracts"),
    ("bitget", "SOL"): Market("bitget-futures", "SOLUSDT", "SOL", .001, .1, 1.,
                              .0002, .0006, "2026-11-12 Bitget mix/market/contracts"),
}

TARGET_ORDER_NOTIONAL_USDT = 80.0
RISK_AVERSION_USDT = 50.0  # 1/USDT; legacy BTC OKX code gamma 0.05
INVENTORY_ORDER_MULTIPLE = 20
MAX_WALL_TICKS = 50


def order_scenario(spec, pretrade_mid):
    if pretrade_mid <= 0 or not np.isfinite(pretrade_mid):
        raise ValueError("Invalid pretrade mid-price")
    raw_lots = TARGET_ORDER_NOTIONAL_USDT / (pretrade_mid*spec.ctval*spec.lot)
    qty = max(spec.lot, math.floor(raw_lots + .5)*spec.lot)
    qty = round(qty/spec.lot)*spec.lot
    # Bitget's documented minimum order notional for the specified pairs is $5.
    if qty*spec.ctval*pretrade_mid < 5.0:
        raise ValueError("Trade size below Bitget minimum notional")
    gamma_code = RISK_AVERSION_USDT * spec.ctval * spec.tick
    return {"order_qty":float(qty), "position_cap":float(INVENTORY_ORDER_MULTIPLE*qty),
            "gamma_code":float(gamma_code),
            "initial_order_notional_usdt":float(qty*spec.ctval*pretrade_mid),
            "gamma_usdt_inv":RISK_AVERSION_USDT, "target_order_notional_usdt":TARGET_ORDER_NOTIONAL_USDT}


def build_asset(npz, lo, hi, spec):
    from hftbacktest import BacktestAsset
    margin = max(1000*spec.tick, .02*(lo+hi)/2.0)
    return (BacktestAsset().data([str(npz)]).linear_asset(spec.ctval)
            .constant_order_latency(1_000_000, 1_000_000)
            .power_prob_queue_model(2.0).no_partial_fill_exchange()
            .trading_value_fee_model(spec.maker_fee, spec.taker_fee)
            .tick_size(spec.tick).lot_size(spec.lot)
            .roi_lb(lo-margin).roi_ub(hi+margin)
            .last_trades_capacity(20_000))


def validate_record(rec, summary):
    if len(rec) < 100:
        raise ValueError("HBT official Recorder captured too few samples")
    if not np.all(np.diff(rec["timestamp"]) > 0):
        raise ValueError("Recorder timestamps not strictly increasing")
    if not np.all(np.isfinite(rec["price"])):
        raise ValueError("Nonfinite Recorder BBO mid-price")
    if np.any(np.diff(rec["num_trades"]) < 0):
        raise ValueError("Recorder trade count decreased")
    if rec["num_trades"][-1] > summary["trading"]["engine_fills"]:
        raise ValueError("Recorder trades exceed official HBT fills")
    if rec["fee"][-1] > summary["trading"]["fee_usdt"] + 1e-7:
        raise ValueError("Recorder fees exceed official HBT fees")


def run(day, exchange, coin, minutes=50, delay_ms=25,
        report_dir="reports", capture_recorder=True):
    from hftbacktest import Recorder, ROIVectorMarketDepthBacktest
    from hftbacktest.data.utils import tardis
    from hftbacktest.data.validation import validate_event_order

    spec = MARKETS[(exchange, coin)]
    if minutes not in (5, 50):
        raise ValueError("Pilot supports 5min or 50min after 10min warmup")
    start_us = int(datetime.strptime(day, "%Y-%m-%d").replace(
        tzinfo=timezone.utc).timestamp())*1_000_000
    end_us = start_us + (600 + minutes*60)*1_000_000 + 500_000
    report_path = Path(report_dir)
    report_path.mkdir(parents=True, exist_ok=True)
    label = f"{exchange}_{coin}_{day}_{minutes}m"
    recorder_dir = report_path/"recorder"/label
    if capture_recorder:
        recorder_dir.mkdir(parents=True, exist_ok=True)

    report = {"status":"started", "day":day, "market":asdict(spec),
              "minutes":minutes, "warmup_minutes":10,
              "extra_binance_delay_ms":delay_ms,
              "model_versions":["glft", "glft_binance", "glft_local_peak",
                                "glft_binance_local_peak"],
              "preexisting_history_explored":True,
              "net_equity_is_mark_to_mid_after_fees":True,
              "funding_and_final_liquidation_not_simulated":True}
    try:
        with tempfile.TemporaryDirectory(prefix=f"mm_{exchange}_{coin}_") as tmp:
            root = Path(tmp)
            paths = {}
            counts = {}
            for kind in SCHEMAS:
                paths[kind] = root/(kind+".csv")
                counts[kind] = extract_window(
                    kind, day, end_us, paths[kind], max_rows=3_000_000,
                    exchange=spec.exchange, symbol=spec.symbol)
            output = root/"fused.npz"
            events = tardis.convert_fuse(
                str(paths["trades"]), str(paths["incremental_book_L2"]),
                str(paths["book_ticker"]), tick_size=spec.tick, lot_size=spec.lot,
                output_filename=str(output),
                ss_buffer_size=max(1200, counts["incremental_book_L2"]["snapshot_rows"]+100),
                snapshot_mode="process")
            validate_event_order(events)
            n_events = len(events)
            del events
            ot, om = read_local_bbo(paths["book_ticker"])
            bt_local, bm = read_binance_bbo(day, end_us, symbol=coin+"USDT")
            bt = visible_timestamps(bt_local, delay_ms)
            basis_ts, basis = causal_60s_basis(ot, om, bt, bm)
            if not np.any(np.isfinite(basis)):
                raise RuntimeError("No valid causal 60s Binance-target basis")
            first_trade_ns = start_us*1000+600*NS
            last_trade_ns = first_trade_ns+minutes*60*NS
            audit=quote_gap_coverage(ot,om,bt,bm,basis_ts,basis,
                                     first_trade_ns,last_trade_ns)
            pretrade = om[ot < first_trade_ns]
            if not len(pretrade):
                raise RuntimeError("Missing pretrade target venue midpoint")
            scenario = order_scenario(spec, float(np.median(pretrade)))
            low, high = float(np.min(om)), float(np.max(om))
            report.update({"data_counts": counts, "hbt_event_count":n_events,
                           "signal_rows":len(bt), "signal_coverage":audit,
                           "scenario":scenario, "variants":{}})

            for label_model,beta,peak in (
                ("glft",0.0,0),("glft_binance",1.0,0),
                ("glft_local_peak",0.0,MAX_WALL_TICKS),
                ("glft_binance_local_peak",1.0,MAX_WALL_TICKS)):
                engine = ROIVectorMarketDepthBacktest([build_asset(output,low,high,spec)])
                rec = Recorder(engine.num_assets, minutes*600+10) if capture_recorder else None
                try:
                    raw=run_event_glft(
                        engine,bt,bm,minutes*600,
                        True,scenario["gamma_code"],True,beta,
                        ot,om,basis_ts,basis,
                        0.0,0.0,10.0,250_000_000,0.0,None,
                        peak,spec.tick,spec.ctval,
                        scenario["order_qty"],scenario["position_cap"],
                        rec.recorder if rec else None)
                    summary = summarize(raw,minutes,True,beta,
                                        scenario["position_cap"],scenario["order_qty"])
                    if capture_recorder:
                        records=rec.get(0)
                        validate_record(records,summary)
                        out=recorder_dir/(label_model+".npz")
                        rec.to_npz(str(out))
                        fill_ts, fill_side=raw[-4],raw[-3]
                        final_ts,observed,price,qty,probes=raw[-1]
                        if len(fill_ts)!=summary["trading"]["engine_fills"]:
                            raise RuntimeError("Official fills not reconciled")
                        fill_out=recorder_dir/(label_model+"_fills.npz")
                        np.savez_compressed(fill_out,
                            exchange_timestamp_ns=fill_ts,
                            local_observed_ns=observed,
                            side=fill_side,exec_price=price,exec_qty=qty)
                        summary["recorder"] = {
                            "file":str(out),"fill_file":str(fill_out),
                            "samples":int(len(records)),
                            "fields":list(records.dtype.names),
                            "first_timestamp_ns":int(records["timestamp"][0]),
                            "last_timestamp_ns":int(records["timestamp"][-1])}
                    report["variants"][label_model]=summary
                finally:
                    engine.close()
                print("VARIANT",exchange,coin,day,label_model,
                      "fills",summary["trading"]["engine_fills"],
                      "gross",summary["trading"]["end_net_marked_equity_usdt"]+
                              summary["trading"]["fee_usdt"],
                      "fee",summary["trading"]["fee_usdt"],
                      "net",summary["trading"]["end_net_marked_equity_usdt"],
                      flush=True)
            base=report["variants"]["glft"]
            for name,v in report["variants"].items():
                assert base["calibration_trace"] == v["calibration_trace"]
                assert base["samples"] == v["samples"]
                assert base["time_count"]["binance"] == v["time_count"]["binance"]
            report["status"]="complete"
    except Exception as exc:
        report["status"]="failed"
        report["error"]=f"{type(exc).__name__}: {exc}"
        raise
    finally:
        p=report_path/(label+".json")
        p.write_text(json.dumps(report,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
        print("REPORT",str(p),report["status"],flush=True)
    return report


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--day",default="2026-11-01")
    p.add_argument("--exchange",choices=("okx","bitget"),required=True)
    p.add_argument("--coin",choices=("BTC","SOL"),required=True)
    p.add_argument("--minutes",type=int,choices=(5,50),default=5)
    p.add_argument("--extra-binance-delay-ms",type=int,default=25)
    p.add_argument("--report-dir",default="reports")
    p.add_argument("--no-recorder",action="store_true")
    args=p.parse_args()
    if datetime.strptime(args.day,"%Y-%m-%d").day != 1:
        p.error("Tardis free day must be first day of the month")
    run(args.day,args.exchange,args.coin,args.minutes,
        args.extra_binance_delay_ms,args.report_dir,not args.no_recorder)


if __name__ == "__main__":
    main()
