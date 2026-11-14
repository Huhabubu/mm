"""Fixed-time causal EMA and source-impulse gates, research only.

Unlike averaging the count of BBO events, sampling at fixed 10ms instants
limits density-weighting. Each input BBO is STRICTLY older than its grid time.
The current 60s GLFT baseline remains untouched.
"""
import math

import numpy as np

NS=1_000_000_000
MAX_AGE_NS=250_000_000


def past_grid_time_ema(okx_time, okx_mid, bin_time, bin_mid,
                       half_life_seconds, grid_step_ns=10_000_000):
    """Build historical 10ms EMA B_t from strictly prior, fresh BBO pairs.

    Index at grid time T becomes available at T, but the trading callback
    conservatively uses only grid indices STRICTLY before decision T.
    A warmup of 10 half-lives is required for an effective value.
    """
    if not (math.isfinite(half_life_seconds) and half_life_seconds>0):
        raise ValueError("Positive EMA half-life required")
    if grid_step_ns<=0 or grid_step_ns>NS:
        raise ValueError("Invalid fixed sampling step")
    ot=np.asarray(okx_time,dtype=np.int64)
    bt=np.asarray(bin_time,dtype=np.int64)
    om=np.asarray(okx_mid,dtype=np.float64)
    bm=np.asarray(bin_mid,dtype=np.float64)
    if (len(ot)==0 or len(bt)==0 or len(ot)!=len(om) or
            len(bt)!=len(bm) or np.any(np.diff(ot)<0) or np.any(np.diff(bt)<0)):
        raise ValueError("Empty, mismatched or nonordered prices")
    step=int(grid_step_ns)
    begin=((max(int(ot[0]),int(bt[0]))+step-1)//step)*step
    end=min(int(ot[-1]),int(bt[-1]))
    grid=np.arange(begin,end+1,step,dtype=np.int64)
    if not len(grid):
        raise ValueError("No overlapping data clock")
    oi=np.searchsorted(ot,grid,side="left")-1
    bi=np.searchsorted(bt,grid,side="left")-1
    safe_o=np.maximum(oi,0);safe_b=np.maximum(bi,0)
    age_o=grid-ot[safe_o];age_b=grid-bt[safe_b]
    valid=((oi>=0)&(bi>=0)&(age_o>=0)&(age_b>=0)&
           (age_o<=MAX_AGE_NS)&(age_b<=MAX_AGE_NS))
    gaps=bm[safe_b]-om[safe_o]
    valid&=np.isfinite(gaps)
    basis=np.full(len(grid),np.nan,dtype=float)
    current=0.;last_time=-1;first_valid=-1
    half_life_ns=float(half_life_seconds)*NS
    for j in np.flatnonzero(valid):
        t=int(grid[j])
        if last_time<0:
            current=float(gaps[j])
            first_valid=t
        else:
            weight=-math.expm1(-math.log(2)*(t-last_time)/half_life_ns)
            current+=weight*(float(gaps[j])-current)
        last_time=t
        if t-first_valid >= 10*half_life_ns:
            basis[j]=current
    return grid,basis


def source_gate(gap_ticks, impulse_ticks, gap_threshold_ticks=1.0,
                impulse_threshold_ticks=1.0):
    """Dimensionless simple same-direction gate; no price data or clocks read."""
    return (math.isfinite(gap_ticks) and math.isfinite(impulse_ticks) and
            abs(gap_ticks)>=gap_threshold_ticks and
            abs(impulse_ticks)>=impulse_threshold_ticks and
            gap_ticks*impulse_ticks>0)


def defend_side(gap_ticks, leader_impulse_ticks, local_impulse_ticks, mode,
                gap_threshold_ticks=1.0, impulse_threshold_ticks=1.0):
    """Return +1=ask danger, -1=bid danger, 0=no defense.

    mode=1/2 uses Binance same-sign recent impulse. mode=3 replaces source
    with opposite-sign OKX local impulse (OKX-triggered placebo).
    """
    if mode not in (1,2,3):return 0
    impulse=(-local_impulse_ticks if mode==3 else leader_impulse_ticks)
    if source_gate(gap_ticks,impulse,gap_threshold_ticks,impulse_threshold_ticks):
        return 1 if gap_ticks>0 else -1
    return 0
