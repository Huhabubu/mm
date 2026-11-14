"""Step3 robust GLFT intensity fit from causally observed 100ms data.

The archived five-positive-distance GLFT estimator is NEVER changed.
A three-to-four-level fit is explicitly research-only and requires a
strictly earlier UTC-date k prior. A longer pretrade window delays the
strategy start from 00:10 to 00:20 UTC; it never reads the future test tape.
"""
import math

import numpy as np

from g0_glft_rolling import FIT_BINS, FIT_STEP_TICKS, MIN_CROSSING_INTERVALS

K_MIN=0.01  # per actual price tick
K_MAX=5.0
MIN_QUALITY_LEVELS=3
SHRINK_LEVEL_EQUIVALENT=5.0
SAMPLE_SECONDS=0.1


def sufficient_stats(arrivals, changes):
    """Persist sufficient historical counts (not high-volume tape arrays)."""
    arrivals=np.asarray(arrivals,dtype=float)
    changes=np.asarray(changes,dtype=float)
    finite=np.isfinite(arrivals)
    crossing=finite & (arrivals > FIT_STEP_TICKS+1.e-7)
    observed=arrivals[crossing]
    levels=np.arange(1,FIT_BINS+1,dtype=float)*FIT_STEP_TICKS
    # This is cumulative quote-distance crossing, same as frozen GLFT base.
    # Each trade-through interval is one observation, not 70 independent fills.
    counts=np.sum(observed[:,None]>levels[None,:]+1.e-7,axis=0).astype(int)
    v=changes[np.isfinite(changes)]
    return {
        "window_seconds":len(arrivals)*SAMPLE_SECONDS,
        "samples":len(arrivals),
        "counts_by_distance":counts.tolist(),
        "positive_distance_levels":int((counts>0).sum()),
        "crossing_100ms_intervals":int(crossing.sum()),
        "volatility_valid_samples":int(len(v)),
        "volatility_sum_tick":float(v.sum()),
        "volatility_sum_sq_tick":float(np.dot(v,v)),
        "max_trade_through_ticks":float(np.max(observed)) if len(observed) else None,
        "threshold_step_ticks":float(FIT_STEP_TICKS),
        "cumulative_thresholds_dependent":True,
    }


def research_fit(stats, prior_k=None, require_earlier_day=True):
    """Return candidate A/k/sigma or an explicit fail-closed reason.

    For n>=5, prior independent OLS can be estimated without a prior;
    for 3..4 nonzero distance levels, a legitimate prior is *mandatory*.
    """
    counts=np.asarray(stats["counts_by_distance"],float)
    if len(counts)!=FIT_BINS:
        raise ValueError("Unexpected number of price distance bins")
    n=int(np.count_nonzero(counts>0))
    crossings=int(stats["crossing_100ms_intervals"])
    vcount=int(stats["volatility_valid_samples"])
    samples=int(stats["samples"])
    if samples<=0 or stats["window_seconds"]<=0:
        raise ValueError("Invalid sample window")
    basis={"valid":False,"method":"research_fail_closed",
           "positive_levels":n,"crossing_intervals":crossings,
           "volatility_samples":vcount,"prior_k_per_tick":prior_k,
           "window_seconds":float(stats["window_seconds"])}
    if n<MIN_QUALITY_LEVELS:
        return {**basis,"reason":"fewer_than_three_positive_distance_levels"}
    if crossings<MIN_CROSSING_INTERVALS or vcount<samples//2:
        return {**basis,"reason":"too_few_independent_crossings_or_volatility_samples"}
    x=(np.flatnonzero(counts>0)+1)*FIT_STEP_TICKS
    y=np.log(counts[counts>0]/float(stats["window_seconds"]))
    centered=x-np.mean(x)
    denom=float(np.sum(centered**2))
    if denom<=0:
        return {**basis,"reason":"no_distance_regression_variation"}
    raw_k=float(-np.dot(centered,y-np.mean(y))/denom)
    if not math.isfinite(raw_k) or raw_k<=0:
        return {**basis,"reason":"nonpositive_unregularized_intensity_decay","raw_k":raw_k}
    if n<5:
        if (prior_k is None or not math.isfinite(prior_k) or
                not K_MIN<=prior_k<=K_MAX):
            return {**basis,"reason":"sparse_levels_without_causal_prior","raw_k":raw_k}
        # Avoid mistaking 4 cumulative quote thresholds for 4 independent
        # samples: the prior strength is expressed in equivalent *levels*.
        k=float((n*raw_k+SHRINK_LEVEL_EQUIVALENT*prior_k)/
                (n+SHRINK_LEVEL_EQUIVALENT))
        method="sparse_causal_prior_shrinkage"
    else:
        k=raw_k
        method="five_plus_levels_original_form"
    if not K_MIN<=k<=K_MAX:
        return {**basis,"reason":"k_outside_predeclared_broad_bounds",
                "raw_k":raw_k,"candidate_k":k}
    log_a=float(np.mean(y+k*x))
    a=math.exp(log_a) if -600<log_a<600 else math.inf
    mean=float(stats["volatility_sum_tick"]/vcount)
    variance=max(0.,float(stats["volatility_sum_sq_tick"]/vcount)-mean*mean)
    sigma=math.sqrt(variance/SAMPLE_SECONDS)
    if not (1.e-8<a<=1.e4 and 0<sigma<=1.e6 and
            math.isfinite(a) and math.isfinite(sigma)):
        return {**basis,"reason":"nonphysical_a_or_sigma",
                "candidate_a":a,"candidate_sigma":sigma}
    return {**basis,"valid":True,"method":method,"reason":"research_fit_identified",
            "A_per_s":a,"k_per_tick":k,
            "raw_k_per_tick":raw_k,"sigma_tick_per_sqrt_s":sigma,
            "shrinkage_equivalent_levels":SHRINK_LEVEL_EQUIVALENT if n<5 else 0.,
            "causal_prior_used":bool(n<5)}


def chronological_reliability(rows,variant="10min"):
    """Sequentially fit each day using ONLY prior days of the same venue/coin."""
    seen=set()
    history_k=None
    last_prior_day=None
    out=[]
    for day,stats in sorted(rows,key=lambda x:x[0]):
        if day in seen:raise ValueError("Duplicate input date")
        seen.add(day)
        fit=research_fit(stats,prior_k=history_k)
        fit["day"]=day
        fit["prior_source_day"]=last_prior_day if fit.get("causal_prior_used") else None
        fit["variant"]=variant
        if fit["valid"]:
            history_k=fit["k_per_tick"]
            last_prior_day=day
        out.append(fit)
    return out
