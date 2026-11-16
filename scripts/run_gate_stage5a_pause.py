"""Stage5A full-day official-HBT: frozen defensive_1p0 vs one-side pause.

Only new action: when a valid Binance-origin signal is active, cancel and
withhold the risk-side quote until signal lapses. Nonrisk side uses native GLFT.
"""
import argparse
import json
import math
import tempfile
from pathlib import Path

import numpy as np
from hftbacktest import ROIVectorMarketDepthBacktest, Recorder
from g0_glft_event_driven import summarize
from gate_origin_glft_strategy import run_gate_origin_glft
from gate_stage5a_pause_strategy import run_gate_stage5a_pause
from run_gate_btc_origin_glft import SCHEMAS, build_asset
from run_gate_stage4_controls import market_data, fill_array, compact_summary
from analyze_gate_1645_fill_quality import model_day

DAYS=tuple(f"2026-{m:02d}-01" for m in range(6,12))
FROZEN=("glft","both_1p0","defensive_1p0")
NEW=("risk_side_pause",)


def same_trading(original, observed, day):
    """The original single-sided outward model must reproduce its frozen ledger."""
    for k in ("engine_fills","end_position_contracts","balance_usdt",
              "fee_usdt","end_net_marked_equity_usdt","trading_value_usdt"):
        a,b=original[k],observed[k]
        if not math.isclose(float(a),float(b),abs_tol=1e-7,rel_tol=0):
            raise ValueError(f"Frozen defensive_1p0 changed: {day} {k} {a} != {b}")


def replay(data,name,official_mode,output):
    cfg=data["params"]
    en=ROIVectorMarketDepthBacktest(
        [build_asset(data["parts"],data["low"],data["high"],cfg)])
    rec=Recorder(en.num_assets,data["minutes"]*600+10)
    try:
        args=(en,data["visible"],data["binance_mid"],data["minutes"]*600,
              1.0,data["grid"],data["basis"],data["sigma"],official_mode,
              cfg["gamma_code"],cfg["tick"],cfg["ctval"],cfg["qty"],
              cfg["position_cap"],rec.recorder,data["stop_ns"])
        if name=="defensive_1p0":
            raw=run_gate_origin_glft(*args)
        elif name=="risk_side_pause":
            raw=run_gate_stage5a_pause(*args)
        else:
            raise ValueError(name)
        s=summarize(raw,data["minutes"],True,1.,cfg["position_cap"],cfg["qty"])
        if raw[5]!=1 or s["end_time_ns"]<data["stop_ns"]-1_000_000_000:
            raise ValueError(f"Truncated or invalid full-day run: {data['day']} {name}")
        fill=fill_array(raw)
        annotated,quality=model_day(name,fill,{"trading":s["trading"]},
                                   data["gt"],data["gm"],data["stop_ns"],cfg["ctval"])
        dest=output/name
        dest.mkdir(parents=True,exist_ok=True)
        rec.to_npz(str(dest/"recorder.npz"))
        np.savez_compressed(dest/"fills.npz",**fill)
        np.savez_compressed(dest/"annotated.npz",**annotated)
        result=compact_summary(s)
        result["quality"]=quality
        result["signal_counts"]=dict(zip(
           ("binance_price_update_groups","eligible_gap_checks",
            "above_sigma_threshold","direction_dominant","first_activations",
            "active_decisions","same_timestamp_excluded"),
           (int(x) for x in raw[-1])))
        print("STAGE5A",data["day"],name,"fills",s["trading"]["engine_fills"],
              "net",s["trading"]["end_net_marked_equity_usdt"],flush=True)
        return result
    finally:
        en.close()


def evaluate(day,frozen_dir,output):
    if day not in DAYS:
        raise ValueError("Only frozen June–November 1 full-day exploratory cases")
    output=Path(output)
    output.mkdir(parents=True,exist_ok=True)
    f=Path(frozen_dir)/f"{day}_1430m.json"
    official=json.loads(f.read_text(encoding="utf-8"))
    if (official["status"]!="complete" or official["minutes_traded"]!=1430
       or official["experimental_trigger_sigma_multiple"]!=1.645
       or official["binance_extra_visible_delay_ms"]!=2.5
       or abs(official["scenario"]["maker_fee_rate"]-0.0002)>1e-12
       or not set(FROZEN).issubset(official["variants"])):
        raise ValueError("Frozen 1.645sigma Gate HBT original archive mismatch")
    with tempfile.TemporaryDirectory(prefix="stage5a_"+day+"_") as tmp:
        data=market_data(Path(tmp),day,1430,official["scenario"])
        for kind in SCHEMAS:
            for key in ("rows","first_local_us","last_local_us"):
                a=data["feed_stats"][kind][key]
                b=official["feed_stats"][kind][key]
                if a!=b:
                    raise ValueError(f"Feed QA differs from frozen original: {day} {kind}.{key}")
        # Re-run existing unilateral price OUTWARD model to obtain its native
        # per-fill Markout and prove original simulations are unchanged.
        defensive=replay(data,"defensive_1p0",2,output)
        same_trading(official["variants"]["defensive_1p0"]["trading"],
                     defensive["trading"],day)
        pause=replay(data,"risk_side_pause",3,output)
        if defensive["signal_counts"]["first_activations"] != pause["signal_counts"]["first_activations"]:
            raise ValueError(f"Different causal signal activation stream on {day}")
        result={
            "day":day,"status":"complete","minutes_traded":1430,
            "signal_gate":"frozen Binance-origin 600s basis, 1sigma anchor, 1.645sigma activation",
            "parameter_changes":"none","new_mode":"positive -> suspend ask, negative -> suspend bid, unaffected side original GLFT",
            "source_data_qa_same_as_frozen":True,
            "defensive_1p0_replay_exact_frozen_accounting":True,
            "fee_maker_rate":0.0002,"binance_extra_visible_latency_ms":2.5,
            "frozen_official":{name:{"trading":official["variants"][name]["trading"]}
                               for name in FROZEN},
            "rerun_defensive":defensive,"risk_side_pause":pause,
        }
        (output/f"{day}_stage5a.json").write_text(
            json.dumps(result,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
        print("STAGE5A_SUCCESS",day,flush=True)


if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("--day",required=True)
    ap.add_argument("--frozen",required=True)
    ap.add_argument("--out",required=True)
    a=ap.parse_args()
    evaluate(a.day,Path(a.frozen),Path(a.out))
