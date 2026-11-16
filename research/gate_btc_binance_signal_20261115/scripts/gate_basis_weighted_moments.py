"""Strictly causal 600s time-weighted Binance-Gate mean and sigma on 5s clock."""
import csv
import math
from pathlib import Path
import numpy as np

NS=1_000_000_000


def load_gate_mid(path):
    times=[]
    mids=[]
    prev=-1
    with Path(path).open(encoding="utf-8",newline="") as f:
        reader=csv.DictReader(f)
        required={"local_timestamp","bid_price","ask_price"}
        if not required.issubset(set(reader.fieldnames or [])):
            raise ValueError("Missing Gate quotes fields")
        for row in reader:
            ts=int(row["local_timestamp"])*1000
            b,a=float(row["bid_price"]),float(row["ask_price"])
            if not (math.isfinite(a) and math.isfinite(b) and 0<b<a and ts>=prev):
                raise ValueError("Invalid/unsorted Gate BBO")
            if ts==prev:
                mids[-1]=(a+b)*.5
            else:
                times.append(ts);mids.append((a+b)*.5)
            prev=ts
    return np.asarray(times,dtype=np.int64),np.asarray(mids,dtype=np.float64)


def exact_moments_5s(bin_ts,bin_mid,gate_ts,gate_mid,grid_start,grid_stop):
    """Return (5s times, time-weighted means, standard deviations, coverage).

    As-of at event times, hold the last visible price until the next event.
    Prefix integrals are used for speed, but F(t) uses events at/before t only.
    """
    for ts,mid in ((bin_ts,bin_mid),(gate_ts,gate_mid)):
        if len(ts)!=len(mid) or not len(ts) or np.any(np.diff(ts)<0):
            raise ValueError("Invalid timestamp/mid arrays")
    events=np.union1d(bin_ts,gate_ts)
    ib=np.searchsorted(bin_ts,events,side="right")-1
    ig=np.searchsorted(gate_ts,events,side="right")-1
    keep=(ib>=0)&(ig>=0)
    events,ib,ig=events[keep],ib[keep],ig[keep]
    if len(events)<2:
        raise ValueError("Insufficient paired events")
    spreads=bin_mid[ib]-gate_mid[ig]
    # Prefix integral between consecutive event instants, no interpolation.
    dt=np.diff(events).astype(np.float64)/NS
    cum=np.r_[0.,np.cumsum(dt*spreads[:-1],dtype=np.float64)]
    cum2=np.r_[0.,np.cumsum(dt*(spreads[:-1]**2),dtype=np.float64)]
    grid=np.arange(grid_start,grid_stop,5*NS,dtype=np.int64)
    if len(grid)==0:
        raise ValueError("No 5s grid")
    older=grid-600*NS

    def f(t,integral,values):
        idx=np.searchsorted(events,t,side="right")-1
        safe=np.maximum(idx,0)
        # All t must be within the source timeline for exact integration.
        if np.any(t>events[-1]):
            raise ValueError("Insufficient future raw BBO coverage of grid")
        elapsed=np.maximum(t-events[safe],0).astype(np.float64)/NS
        return np.where(idx>=0,integral[safe]+elapsed*values[safe],0.)

    ft=f(grid,cum,spreads)
    fl=f(older,cum,spreads)
    f2t=f(grid,cum2,spreads**2)
    f2l=f(older,cum2,spreads**2)
    effective=(grid-np.maximum(older,events[0])).astype(np.float64)/NS
    coverage=effective/600.
    if np.any(coverage<.99):
        raise ValueError("Insufficient paired quote history for baseline")
    basis=(ft-fl)/effective
    variance=np.maximum((f2t-f2l)/effective - basis*basis,0.)
    sigma=np.sqrt(variance)
    if np.any(~np.isfinite(basis)) or np.any(~np.isfinite(sigma)):
        raise ValueError("Nonfinite basis/sigma")
    return grid,basis,sigma,coverage
