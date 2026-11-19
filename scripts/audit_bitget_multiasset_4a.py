"""Read-only Bitget 4A normalized CSV scan for ETHUSDT/SOLUSDT/DOGEUSDT.

Reuses EXACTLY the previous BTC stage4A audit_stream and its gate criteria.
No HBT simulation or raw WebSocket reconstructor is introduced here.
"""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

from audit_cross_exchange_4a import audit_stream, url_for

SYMBOLS=("ETHUSDT","SOLUSDT","DOGEUSDT")
DAYS=tuple(f"2026-{month:02d}-01" for month in range(6,12))

def run(symbol,day,config,out):
    if symbol not in SYMBOLS or day not in DAYS:
        raise ValueError("Audit restricted to requested Bitget three assets and six UTC days")
    cfg=json.loads(Path(config).read_text(encoding="utf-8"))
    spec=cfg["contracts"][symbol]
    if spec["historical_date_effective_verified"] is not False:
        raise ValueError("No historical contract effective-date certification exists")
    start=int(datetime.strptime(day,"%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()*1_000_000)
    end=start+86_400_000_000
    report={
        "venue":"bitget-futures","symbol":symbol,"day":day,
        "status":"started","utc_interval":"[00:00:00,24:00:00)",
        "feeds":{},"contract_candidate":spec,
        "source":"actual free Tardis per-symbol CSV.GZ, streamed full UTC day",
        "no_hbt_or_strategy_fills":True,
        "historical_contract_date_effective_verified":False,
        "comparison_maker_fee_rate":cfg["research_maker_fee_rate"],
        "exchange_standard_maker_fee_rate":cfg["standard_tier_maker_rate"],
        "exchange_standard_taker_fee_rate":cfg["standard_tier_taker_rate"],
        "native_l2_packet_sequence_verified":False,
    }
    out=Path(out);out.parent.mkdir(parents=True,exist_ok=True)
    try:
        feeds=(("bitget-futures","trades",symbol),
               ("bitget-futures","incremental_book_L2",symbol),
               ("bitget-futures","quotes",symbol),
               ("binance-futures","book_ticker",symbol))
        for exchange,kind,sym in feeds:
            label="binance_book_ticker" if exchange=="binance-futures" else kind
            print("BITGET_4A_BEGIN",symbol,day,exchange,kind,flush=True)
            try:
                record=audit_stream(
                    exchange,sym,kind,day,start,end,
                    float(spec["tick_size_usdt"]) if exchange!="binance-futures" else 0.0,
                    float(spec["qty_step_candidate_base"]) if kind=="trades" else 0.0)
            except Exception as exc:
                record={
                    "source_url":url_for(exchange,kind,day,sym),
                    "normalized_gate_status":"blocked",
                    "blocking_checks":[f"{type(exc).__name__}: {exc}"],
                    "rows":0,"fatal_feed_exception":True,
                }
            report["feeds"][label]=record
            print("BITGET_4A_FEED",symbol,day,kind,
                  record["normalized_gate_status"],record.get("rows"),
                  record.get("blocking_checks"),flush=True)
        a=report["feeds"].get("quotes",{}).get("lag",{})
        b=report["feeds"].get("binance_book_ticker",{}).get("lag",{})
        delta={}
        for key in ("p50_ms","p95_ms","p99_ms"):
            av,bv=a.get(key),b.get(key)
            delta[key.replace("_ms","_delta_ms")]=round(av-bv,4) if (
                isinstance(av,(int,float)) and isinstance(bv,(int,float))) else None
        report["capture_lag_comparison"]={
            "target_feed":"bitget-futures normalized quotes",
            "reference_feed":"binance-futures native book_ticker",
            "not_network_latency":True,
            **delta
        }
        p95,p99=delta.get("p95_delta_ms"),delta.get("p99_delta_ms")
        report["relative_lag_tail_requires_review"]=bool(
            (p95 is not None and abs(p95)>30) or
            (p99 is not None and abs(p99)>75))
        report["normalized_feeds_passed"]=all(
            report["feeds"][k].get("normalized_gate_status")=="passed"
            for k in ("trades","incremental_book_L2","quotes","binance_book_ticker"))
        report["status"]=(
            "passed_normalized_gate_with_unresolved_metadata"
            if report["normalized_feeds_passed"] else "blocked")
        report["eligible_for_4b"]=False
    except Exception as exc:
        report["status"]="failed"
        report["fatal_error"]=f"{type(exc).__name__}: {exc}"
        raise
    finally:
        out.write_text(json.dumps(report,ensure_ascii=False,indent=2,allow_nan=False)+"\n",
                       encoding="utf-8")
        print("BITGET_4A_RESULT",symbol,day,report["status"],out,flush=True)
    return report

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--symbol",choices=SYMBOLS,required=True)
    p.add_argument("--day",choices=DAYS,required=True)
    p.add_argument("--config",type=Path,default=Path("configs/bitget_multiasset_contract_specs_4a.json"))
    p.add_argument("--out",type=Path,required=True)
    args=p.parse_args()
    run(args.symbol,args.day,args.config,args.out)

if __name__=="__main__":
    main()
