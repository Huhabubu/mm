"""V6.1: +60s placebo with exactly equal *retained* pulse schedule.

Clipping the last 60s before shifting avoids using later same-day signals at
test-window start. To eliminate remaining duty/count mismatch, BOTH arms replay
the same truncated original episode schedule, one at original time and one
at +60s. Neither arm consumes Binance prices/clock during HBT execution.
The observed full-source model remains an ancillary (different) benchmark.
"""
import argparse
import json
from datetime import datetime,timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np

from experiment_source_defense_v5_multiday import prepared,prior_from_stored_admission
from experiment_source_defense_v6_mechanism import (
    model_run,groups,short_metrics,SHIFT_NS)

DAYS=("2026-06-01","2026-07-01","2026-08-01",
      "2026-09-01","2026-10-01","2026-11-01")


def schedule_pair(starts,ends,sides,stop_ns):
    """One-to-one source pulses and +60s pulses, equal duration by construction."""
    st=np.asarray(starts,np.int64);en=np.asarray(ends,np.int64)
    side=np.asarray(sides,np.int64)
    if not (len(st)==len(en)==len(side)):raise ValueError("Episode shape mismatch")
    if np.any(en<=st) or np.any(st[1:]<en[:-1]):
        raise ValueError("Overlapping or backward original source episodes")
    limit=stop_ns-SHIFT_NS
    kept=st<limit
    a=st[kept]
    b=np.minimum(en[kept],limit)
    sd=side[kept]
    if np.any(b<=a):raise ValueError("Bad clipped episode")
    src=(a,b,sd)
    placebo=(a+SHIFT_NS,b+SHIFT_NS,sd)
    assert len(a)==len(placebo[0])
    assert int(np.sum(b-a))==int(np.sum(placebo[1]-placebo[0]))
    return src,placebo,{
        "original_full_episode_count":len(st),
        "original_full_active_ns":int(np.sum(en-st)),
        "retained_episode_count":len(a),
        "retained_source_active_ns":int(np.sum(b-a)),
        "shifted_active_ns":int(np.sum(placebo[1]-placebo[0])),
        "retained_fraction_of_original":float(np.sum(b-a)/np.sum(en-st)) if len(st) else None,
        "same_episode_count":len(a)==len(placebo[0]),
        "same_total_scheduled_duration":int(np.sum(b-a))==int(np.sum(placebo[1]-placebo[0])),
        "source_and_shift_execution_binance_clock_disabled":True,
        "no_circular_wrap_or_future_feed_use":True}


def run(day,original_case,artifact,output,admission):
    if day not in DAYS:raise ValueError("Only six fixed SOL days")
    parent=json.loads(Path(original_case).read_text(encoding="utf8"))
    if parent["day"]!=day or parent["coin"]!="SOL" or parent["venue"]!="okex-swap":
        raise ValueError("Original V6 source episode case mismatch")
    with np.load(artifact) as a:
        st=a["source__episode_start"].copy()
        en=a["source__episode_end"].copy()
        si=a["source__episode_side"].copy()
    start=int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp()*1e9)
    stop=start+(1200+3000)*1_000_000_000
    source,shift,meta=schedule_pair(st,en,si,stop)
    k,_=prior_from_stored_admission(admission,"SOL","okex-swap",day)
    with TemporaryDirectory(prefix="v6_matched_exposure_") as folder:
        data=prepared(day,"SOL","okex-swap",Path(folder),with_trades=False)
        src,sfills,strace=model_run(data,"shift60",k,lagged=source)
        plc,pfills,ptrace=model_run(data,"shift60",k,lagged=shift)
        actual1=short_metrics(src,sfills,strace,True)
        actual2=short_metrics(plc,pfills,ptrace,True)
        actual_equal=(actual1["defense_episode_count"]==actual2["defense_episode_count"] and
                      abs(actual1["defense_duration_ms"]-actual2["defense_duration_ms"])<1e-6)
        report={
            "coin":"SOL","venue":"okex-swap","day":day,
            "pretrade_minutes":20,"trade_window_UTC":"00:20–01:10",
            "Maker_fee_bps":2.,"official_HBT":True,
            "same_original_source_schedule_but_time_shifted_by_60s":True,
            "schedule_control":meta,
            "actual_HBT_episode_count_and_duration_equal":actual_equal,
            "source_schedule_no_current_Binance_input":actual1,
            "shift_60s_schedule_no_current_Binance_input":actual2,
            "source_minus_shift_net_after_fee_USDT":
                actual1["fee_included_net_usdt"]-actual2["fee_included_net_usdt"],
            "live_original_source_fills_for_reference":parent["source"]["fills"],
            "G0_reference_fills":parent["g0"]["fills"],
            "methodology_limits":[
                "Both scheduled arms use source episodes observed in an earlier full original strategy replay, not a prospective runnable signal",
                "Trimming last 60s retains exact same pulses/duration in two arms, but removes original terminal episodes",
                "Even exact pause exposure does not match executed fills or inventory, so not pure causal Selection Alpha",
                "HBT queue latency and order ACK remain modeled (1ms request / 1ms response), fee2bps included",
                "Six monthly days previously used in admission tests, not fully held out"]}
        out=Path(output);out.mkdir(parents=True,exist_ok=True)
        (out/"case.json").write_text(json.dumps(report,indent=2,ensure_ascii=False,allow_nan=False))
        z={}
        for name,fill in (("source_time",sfills),("shift60_time",pfills)):
            for key in ("exchange_timestamp_ns","local_observed_ns","side_sign","spread_capture_bps"):
                z[name+"__"+key]=fill[key]
            for h in (100,500,1000):
                for key in (f"markout_{h}ms_bps",f"fresh_{h}ms",f"gross_edge_{h}ms_bps"):
                    z[name+"__"+key]=fill[key]
        np.savez_compressed(out/"fills_CI_only.npz",**z)
        print("MATCHED_EXPOSURE",day,
              "PULSE",meta["retained_episode_count"],
              "ACTUAL_EQUAL",actual_equal,
              "FILL_SRC",actual1["fills"],
              "FILL_SHIFT",actual2["fills"],
              "PNL_DIFF",report["source_minus_shift_net_after_fee_USDT"],flush=True)
        return report


if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--day",required=True,choices=DAYS)
    p.add_argument("--original-case",required=True)
    p.add_argument("--original-artifact",required=True)
    p.add_argument("--admission-json",
                   default="reports/source_defense/step3_fit/step3_fit_admission.json")
    p.add_argument("--out",required=True)
    a=p.parse_args()
    run(a.day,a.original_case,a.original_artifact,a.out,a.admission_json)
