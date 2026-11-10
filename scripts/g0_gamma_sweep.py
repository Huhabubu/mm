"""Fixed Gamma (CARA USDT^-1) sensitivity on 6 exploratory OKX free sample dates.

Uses existing official hftbacktest G0 rolling strategy unchanged apart from
explicit gamma_code input. Independent engine replay for each risk setting.
"""
import argparse
import csv
import json
import math
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from hftbacktest.data.utils import tardis
from hftbacktest.data.validation import validate_event_order
from check_okx_hft_data import SCHEMAS, extract_window
from g0_glft_baseline import (CTVAL, TICK, GAMMA, LOT, MAKER_FEE, TAKER_FEE,
                              ORDER_QTY, POSITION_CAP, unit_grid_audit)
from g0_glft_rolling import run_mode

GAMMA_USDT_GRID = (0.001, 0.01, 0.05, 0.5, 5.0, 50.0)
FREE_DATES = ("2026-06-01","2026-07-01","2026-08-01",
              "2026-09-01","2026-10-01","2026-11-01")
TRADE_MINUTES = 50
FIT_MINUTES = 10


def gamma_usdt_to_code(gamma_usdt, contract_value=CTVAL, tick_size=TICK):
    """Convert 1/USDT risk aversion to 1/(contract*tick)."""
    if not all(math.isfinite(x) and x > 0 for x in
               (gamma_usdt, contract_value, tick_size)):
        raise ValueError("Gamma, face value and tick must be finite and positive")
    return gamma_usdt * contract_value * tick_size


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--day",choices=FREE_DATES,required=True)
    parser.add_argument("--report-dir",default="reports")
    args=parser.parse_args()
    day=datetime.strptime(args.day,"%Y-%m-%d").replace(tzinfo=timezone.utc)
    out=Path(args.report_dir)/f"g0_gamma_sweep_{args.day}_60m.json"
    out.parent.mkdir(parents=True,exist_ok=True)
    report={"status":"started","day_utc":args.day,
            "scope":"exploratory_one_hour_not_daily",
            "sample_train_utc":"00:00-00:10","sample_trade_utc":"00:10-01:00",
            "gamma_grid_usdt_inverse":list(GAMMA_USDT_GRID),
            "risk_budget_specified":False,"selected_optimal_gamma":None,
            "scenario":{"contract_btc_per_contract":CTVAL,"tick_usdt_per_btc":TICK,
                        "lot_contracts":LOT,"order_qty_contracts":ORDER_QTY,
                        "inventory_cap_contracts":POSITION_CAP,
                        "maker_fee":MAKER_FEE,"taker_fee":TAKER_FEE,
                        "quote_interval_ms":100,"rolling_fit_window_sec":600,
                        "rolling_refit_interval_sec":5,
                        "order_entry_latency_ms":1,"order_response_latency_ms":1,
                        "queue_model":"power_prob(2.0)","partial_fill":"no_partial",
                        "historical_instrument_metadata_verified":False,
                        "actual_account_fee_verified":False},
            "scenarios":[]}
    try:
        with tempfile.TemporaryDirectory(prefix="gamma_sweep_") as folder:
            root=Path(folder)
            end_us=int(day.timestamp()*1_000_000)+(FIT_MINUTES+TRADE_MINUTES)*60*1_000_000
            paths={}; counts={}
            for kind in SCHEMAS:
                paths[kind]=root/(kind+".csv")
                counts[kind]=extract_window(kind,args.day,end_us,paths[kind],max_rows=2_500_000)
            report["feed_counts"]=counts
            report["unit_grid_audit"]=unit_grid_audit(paths)
            npz=root/"fused.npz"
            events=tardis.convert_fuse(str(paths["trades"]),str(paths["incremental_book_L2"]),
                                      str(paths["book_ticker"]),tick_size=TICK,lot_size=LOT,
                                      output_filename=str(npz),
                                      ss_buffer_size=max(1000,counts["incremental_book_L2"]["snapshot_rows"]+100),
                                      snapshot_mode="process")
            validate_event_order(events)
            report["converted_events"]=int(len(events))
            del events
            lb,ub=math.inf,-math.inf
            with paths["book_ticker"].open(encoding="utf-8",newline="") as file:
                for row in csv.DictReader(file):
                    lb=min(lb,float(row["bid_price"]))
                    ub=max(ub,float(row["ask_price"]))
            if not all(math.isfinite(x) for x in (lb,ub)) or lb >= ub:
                raise ValueError("invalid BBO bounds")
            common_fit=None
            for gamma_usdt in GAMMA_USDT_GRID:
                gamma_code=gamma_usdt_to_code(gamma_usdt)
                mode=run_mode(npz,lb,ub,TRADE_MINUTES,True,gamma_code=gamma_code)
                trace=mode.pop("calibration_trace")
                fit=[(r["A_per_second"],r["k_per_real_tick"],
                      r["sigma_tick_per_sqrt_second"]) for r in trace]
                if common_fit is None:
                    common_fit=fit
                elif common_fit!=fit:
                    raise RuntimeError("A/k/sigma changed with gamma: paired test invalid")
                mode["gamma_usdt_inverse"]=gamma_usdt
                mode["gamma_code"]=gamma_code
                mode["risk_tolerance_usdt"]=1/gamma_usdt
                mode["calibration_trace_count"]=len(trace)
                report["scenarios"].append(mode)
            if not math.isclose(report["scenarios"][-1]["gamma_code"],GAMMA,
                                abs_tol=1e-14,rel_tol=0):
                raise RuntimeError("legacy gamma anchor not 0.05")
        report["status"]="complete"
    except Exception as exc:
        report["status"]="failed"
        report["error"]=f"{type(exc).__name__}: {exc}"
        raise
    finally:
        out.write_text(json.dumps(report,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
        print(f"G0 gamma sweep {args.day}: {report['status']} ({out})",flush=True)


if __name__=="__main__":
    main()
