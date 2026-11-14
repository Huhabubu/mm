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



# All active research decisions below use bps (1 bp = 1e-4 of target mid).
# Mid/tick prices are always in USDT per target base coin.
from numba import njit


@njit
def source_gate_bps(gap_bps, impulse_bps, gap_threshold_bps,
                    impulse_threshold_bps):
    """Require strictly causal same-direction price impulse and residual gap."""
    return (np.isfinite(gap_bps) and np.isfinite(impulse_bps) and
            abs(gap_bps) >= gap_threshold_bps and
            abs(impulse_bps) >= impulse_threshold_bps and
            gap_bps * impulse_bps > 0.)


@njit
def defend_side_bps(gap_bps, leader_impulse_bps, local_impulse_bps, mode,
                    gap_threshold_bps, impulse_threshold_bps):
    """+1=ask danger, -1=bid danger, 0=no danger; mode3 is OKX placebo."""
    if mode != 1 and mode != 2 and mode != 3:
        return 0
    impulse = -local_impulse_bps if mode == 3 else leader_impulse_bps
    if source_gate_bps(gap_bps, impulse, gap_threshold_bps,
                       impulse_threshold_bps):
        return 1 if gap_bps > 0 else -1
    return 0


@njit
def widen_ticks_from_bps(mid_usdt, tick_usdt, widen_bps):
    """Convert target-mid-relative bps action into one or more price ticks."""
    if (not np.isfinite(mid_usdt) or mid_usdt <= 0. or
            not np.isfinite(tick_usdt) or tick_usdt <= 0. or
            not np.isfinite(widen_bps) or widen_bps <= 0.):
        raise ValueError("Invalid bps quote action")
    count = int(round(mid_usdt * widen_bps * 1.e-4 / tick_usdt))
    return max(1, count)


@njit
def next_defense_state(now_ns, active_side, active_since_ns, last_end_ns,
                       candidate_side, gap_bps, impulse_bps,
                       hysteresis_exit_th_bps, hold_window_ns, cooldown_ns):
    """Bound hold to one activation; both-low signals release immediately.

    A *continuously valid* same-direction fresh impulse can remain active;
    once it vanishes the nonrenewable hold expires after hold_window_ns.
    Never forward-shift active_since_ns on each trigger.
    """
    if active_side == 0:
        if candidate_side != 0 and now_ns - last_end_ns >= cooldown_ns:
            return candidate_side, now_ns, last_end_ns
        return 0, active_since_ns, last_end_ns

    if candidate_side == active_side:
        return active_side, active_since_ns, last_end_ns
    if candidate_side != 0 and candidate_side != active_side:
        return 0, active_since_ns, now_ns

    both_low = (np.isfinite(gap_bps) and np.isfinite(impulse_bps) and
                abs(gap_bps) <= hysteresis_exit_th_bps and
                abs(impulse_bps) <= hysteresis_exit_th_bps)
    if both_low or now_ns - active_since_ns >= hold_window_ns:
        return 0, active_since_ns, now_ns
    return active_side, active_since_ns, last_end_ns



@njit
def adaptive_bps_gates(sigma_100ms_bps, mid_usdt, tick_usdt,
                       z_gap=2.0, z_impulse=1.5):
    """Pretrade 100ms return SD only; gate floor >= max(0.3bp, 1 price tick)."""
    if (not np.isfinite(sigma_100ms_bps) or sigma_100ms_bps<0 or
            not np.isfinite(mid_usdt) or mid_usdt<=0 or
            not np.isfinite(tick_usdt) or tick_usdt<=0 or
            z_gap<=0 or z_impulse<=0):
        raise ValueError("Invalid historical-volatility adaptive threshold")
    floor=max(.3,10000.*tick_usdt/mid_usdt)
    return (max(floor,z_gap*sigma_100ms_bps),
            max(floor,z_impulse*sigma_100ms_bps),floor)


@njit
def queue_shield_allows(side, gap_bps, gap_threshold_bps,
                        bid_qty, ask_qty, bid_quantile, ask_quantile,
                        extreme_multiplier=2.0):
    """Only the dangerous side's own best-quote liquidity matters.

    Inactive/invalid reference depth fails closed, never invents a deep wall.
    A >=2x gap impulse is predeclared extreme and bypasses the shield.
    """
    if side==0:
        return False
    if np.isfinite(gap_bps) and abs(gap_bps)>=extreme_multiplier*gap_threshold_bps:
        return True
    qty=ask_qty if side==1 else bid_qty
    quant=ask_quantile if side==1 else bid_quantile
    return bool(np.isfinite(qty) and qty>=0 and
                np.isfinite(quant) and quant>0 and
                qty<=quant)
