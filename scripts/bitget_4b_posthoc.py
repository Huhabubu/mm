"""Post-hoc fill-quality diagnostics for Bitget 4B, never used by execution.

All windows are in Tardis local-observation nanoseconds and are constructed
before inspecting fills. Removing fills from this table DOES NOT re-run HBT.
"""
import csv
from pathlib import Path

import numpy as np
from analyze_gate_1645_fill_quality import HORIZONS, label_fills, stats

WINDOW_NS = 5_000_000_000
L2_GAP_NS = 1_000_000_000


def source_risk_events(l2_csv, l2_format):
    """Find extra *native* snapshots and >1s inter-message L2 gaps.

    Per-message full snapshots normalized into deltas are not reconnects:
    a reconnect cannot be inferred solely from those regular full snapshots.
    """
    previous = None
    prev_snapshot = False
    snapshot_groups = 0
    extra = []
    gaps = []
    with Path(l2_csv).open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            stamp = int(row["local_timestamp"]) * 1000
            snap = row["is_snapshot"].lower() == "true"
            if previous is not None and stamp == previous:
                continue
            if previous is not None and stamp < previous:
                raise ValueError("L2 local timestamp went backwards")
            if previous is not None and stamp - previous > L2_GAP_NS:
                gaps.append(stamp)
                gaps.append(previous)
            if snap and not prev_snapshot:
                snapshot_groups += 1
                if snapshot_groups > 1 and l2_format == "initial_snapshot_plus_native_deltas":
                    extra.append(stamp)
            prev_snapshot = snap
            previous = stamp
    if snapshot_groups < 1:
        raise ValueError("Missing L2 initial snapshot")
    # Each gap represents a low-information interval. Exclude ±5 seconds
    # around BOTH ends; additionally exclude the entire gap itself.
    return {"extra_snapshot_times_ns": np.asarray(extra, dtype=np.int64),
            "gap_endpoint_times_ns": np.asarray(gaps, dtype=np.int64),
            "l2_gap_count": len(gaps)//2,
            "native_extra_snapshot_count": len(extra),
            "snapshot_mode": l2_format,
            "raw_reconnect_packets_verified": False}


def merged_windows(events, first_ns, last_ns):
    intervals=[]
    for arr in (events["extra_snapshot_times_ns"],):
        intervals.extend((max(first_ns,int(t)-WINDOW_NS),
                          min(last_ns,int(t)+WINDOW_NS)) for t in arr)
    gap=events["gap_endpoint_times_ns"]
    for left,right in zip(gap[0::2],gap[1::2]):
        intervals.append((max(first_ns,int(left)-WINDOW_NS),
                          min(last_ns,int(right)+WINDOW_NS)))
    intervals.sort()
    out=[]
    for a,b in intervals:
        if b<a:
            continue
        if out and a<=out[-1][1]:
            out[-1]=(out[-1][0],max(b,out[-1][1]))
        else:
            out.append((a,b))
    return np.asarray(out,dtype=np.int64).reshape(-1,2)


def included_mask(fill_local_ns, windows):
    ts=np.asarray(fill_local_ns,dtype=np.int64)
    if not len(windows):
        return np.ones(len(ts),dtype=bool)
    starts=windows[:,0]
    idx=np.searchsorted(starts,ts,side="right")-1
    safe=np.maximum(idx,0)
    return (idx<0)|(ts>windows[safe,1])


def slice_quality(fill, target_ts, target_mid, trade_end_ns, ctval, excluded):
    annotated,_=label_fills(fill,target_ts,target_mid,trade_end_ns,ctval)
    total=len(annotated["side"])
    valid=~np.asarray(excluded,dtype=bool)
    if len(valid)!=total:
        raise ValueError("Incorrect mask dimensions")
    w=annotated["fill_notional_usdt"]
    excluded_notional=float(w[~valid].sum())
    all_notional=float(w.sum())
    out={"all_fills":total,"excluded_fills":int((~valid).sum()),
         "excluded_fill_fraction":float((~valid).sum()/total) if total else None,
         "excluded_notional_usdt":excluded_notional,
         "excluded_notional_fraction":excluded_notional/all_notional if all_notional else None,
         "kept_fills":int(valid.sum()),
         "kept_notional_usdt":float(w[valid].sum()),
         "horizons":{}}
    for h in HORIZONS:
        eligible=(valid & annotated[f"total_fresh_{h}ms"] &
                  np.isfinite(annotated[f"total_markout_{h}ms_bps"]))
        updated=eligible & annotated[f"future_quote_updated_{h}ms"]
        key=str(h)
        s=stats(annotated[f"total_markout_{h}ms_bps"][eligible],w[eligible])
        u=stats(annotated[f"total_markout_{h}ms_bps"][updated],w[updated])
        out["horizons"][key]={
            "total_markout_bps":s["mean_bps"] if s else None,
            "total_markout_n":int(eligible.sum()),
            "total_markout_after_quote_update_bps":u["mean_bps"] if u else None,
            "total_markout_updated_n":int(updated.sum())}
    return out


def posthoc_slices(fill, target_ts, target_mid, trade_start_ns, trade_end_ns,
                   risk_events):
    timestamps=np.asarray(fill["local_observed_ns"],dtype=np.int64)
    windows=merged_windows(risk_events,trade_start_ns,trade_end_ns)
    kept=included_mask(timestamps,windows)
    start_day=trade_start_ns-600_000_000_000
    oct_utc_two_hours=((timestamps>=start_day+3_600_000_000_000)
                       & (timestamps<start_day+10_800_000_000_000))
    return {
        "l2_reconnect_gap_pm5s":{
            "window_count":len(windows),
            "window_seconds":float((windows[:,1]-windows[:,0]).sum()/1e9),
            "native_extra_snapshot_count":risk_events["native_extra_snapshot_count"],
            "l2_gap_gt_1000ms_count":risk_events["l2_gap_count"],
            "derived_from":"Tardis normalized L2 local timestamps, not raw WS packet sequence",
            "drop_fills_only_not_replayed":True,
            "quality":slice_quality(fill,target_ts,target_mid,trade_end_ns,1.,~kept)},
        "oct_01_02utc":{
            "window_utc":"[01:00,03:00)",
            "drop_fills_only_not_replayed":True,
            "quality":slice_quality(fill,target_ts,target_mid,trade_end_ns,1.,oct_utc_two_hours)}
    }
