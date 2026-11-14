"""V6 microprice opportunity-set control, BTC/SOL June only.

Contrasts jump-conditioned 5-50ms precursors with *all* fixed 10ms source
grid instants, so near-100% pre-jump sign hit can't masquerade as a deployable
signal; checks post-previous-jump >=50ms quiet-history filter.
Tardis native BBO, no trades, no order execution, no new strategy params.
"""
import argparse
import json
from datetime import datetime,timezone
from pathlib import Path

import numpy as np
from experiment_source_defense_v6_clock_microprice import (
    read_native,microprice,MARKETS,prejumps
)

DAY="2026-06-01"
COINS=("BTC","SOL")
NS_US=1_000_000


def quiet_prejump(bn,target,lag_ms,quiet_ms=50,extra_delay_ms=2.5):
    b=bn
    tt=b["local_us"]+int(extra_delay_ms*1000)
    mid=(b["bid"]+b["ask"])/2.
    changes=np.flatnonzero(np.diff(mid)!=0)+1
    buckets=tt[changes]//20000
    changes=changes[np.r_[True,np.diff(buckets)!=0]]
    when=tt[changes]
    prior=np.searchsorted(tt,when-lag_ms*1000,side="left")-1
    valid=prior>=0
    prior=np.maximum(prior,0)
    previous_change_idx=np.searchsorted(changes,prior,side="right")-1
    prior_change=np.maximum(previous_change_idx,0)
    prior_change_time=tt[changes[prior_change]]
    # Need a previous jump and then a >=50ms quiet period before feature.
    valid&=(previous_change_idx>=0)&(
        tt[prior]-prior_change_time>=quiet_ms*1000)
    valid&=(tt[changes]-tt[prior]>=lag_ms*1000)
    valid&=(tt[changes]-tt[prior]<=lag_ms*1000+250000)
    idx=changes[valid];before=prior[valid]
    mp=microprice(b["bid"][before],b["ask"][before],
                  b["bidqty"][before],b["askqty"][before])
    center=mid[before]
    signal=np.sign(mp-center)
    true_sign=np.sign(mid[idx]-mid[idx-1])
    good=signal!=0
    hit=float(np.mean(signal[good]==true_sign[good])) if np.any(good) else None
    return {
        "lag_before_Binance_jump_ms":lag_ms,"previous_BN_jump_quiet_ms":quiet_ms,
        "eligible_jumps_with_strict_quiet":len(idx),
        "nonzero_signal_n":int(good.sum()),
        "future_jump_direction_hit_fraction":hit,
        "future_event_conditioned":True,
        "may_be_sparse_or_zero_when_BN_BBO_updates_fast":True}


def unconditional_grid(bn,target,extra_delay_ms=2.5,step_ms=10,horizon_ms=100):
    """Sample ALL grid times with strictly past source/target, 250ms fresh."""
    bt=bn["local_us"]+int(extra_delay_ms*1000)
    ot=target["local_us"]
    start=max(int(bt[0]),int(ot[0]))+1_000_000
    end=min(int(bt[-1]),int(ot[-1]))-horizon_ms*1000
    step=step_ms*1000
    t=np.arange((start+step-1)//step*step,end,step,dtype=np.int64)
    bi=np.searchsorted(bt,t,side="left")-1
    oi=np.searchsorted(ot,t,side="left")-1
    future=t+horizon_ms*1000
    fi=np.searchsorted(ot,future,side="right")-1
    ib=np.maximum(bi,0);io=np.maximum(oi,0);ifut=np.maximum(fi,0)
    good=(bi>=0)&(oi>=0)&(fi>=0)
    good&=(t-bt[ib]>=0)&(t-bt[ib]<=250000)
    good&=(t-ot[io]>=0)&(t-ot[io]<=250000)
    good&=(future-ot[ifut]>=0)&(future-ot[ifut]<=250000)
    bmid=(bn["bid"][ib]+bn["ask"][ib])/2.
    mp=microprice(bn["bid"][ib],bn["ask"][ib],
                  bn["bidqty"][ib],bn["askqty"][ib])
    feature=(mp-bmid)/bmid*1e4
    omid=(target["bid"]+target["ask"])/2.
    target_return=(omid[ifut]-omid[io])/omid[io]*1e4
    f=feature[good];y=target_return[good]
    is_buy=f>0;is_sell=f<0
    signed=np.sign(f)*y
    # Separate short-window BBO quantity-imbalance *change* feature: strictly
    # prior to grid T and T-50ms. No future Binance midpoint required.
    past_t=t-50_000
    pi=np.searchsorted(bt,past_t,side="left")-1
    ip=np.maximum(pi,0)
    qty_now=bn["bidqty"][ib]+bn["askqty"][ib]
    qty_past=bn["bidqty"][ip]+bn["askqty"][ip]
    imbalance_now=np.divide(
        bn["bidqty"][ib]-bn["askqty"][ib],qty_now,
        out=np.zeros(len(t)),where=qty_now>0)
    imbalance_past=np.divide(
        bn["bidqty"][ip]-bn["askqty"][ip],qty_past,
        out=np.zeros(len(t)),where=qty_past>0)
    change=imbalance_now-imbalance_past
    valid_change=good & (pi>=0) & (past_t-bt[ip]>=0) & (
        past_t-bt[ip]<=250_000) & (qty_now>0) & (qty_past>0)
    d=change[valid_change]
    future_y=target_return[valid_change]
    nonzero_d=d!=0
    signed_qty=np.sign(d[nonzero_d])*future_y[nonzero_d]
    # do not optimize thresholds on test labels; all valid nonzero observations.
    return {
        "fixed_sample_grid_ms":step_ms,
        "target_future_horizon_ms":horizon_ms,
        "strict_asof_max_age_ms":250,
        "all_grid_times":len(t),
        "usable_source_and_target_times":len(f),
        "nonzero_feature_n":int(np.sum(f!=0)),
        "positive_microprice_fraction":float(np.mean(is_buy)) if len(f) else None,
        "negative_microprice_fraction":float(np.mean(is_sell)) if len(f) else None,
        "target_100ms_return_mean_given_positive_microprice_bps":
            float(np.mean(y[is_buy])) if np.any(is_buy) else None,
        "target_100ms_return_mean_given_negative_microprice_bps":
            float(np.mean(y[is_sell])) if np.any(is_sell) else None,
        "source_signed_target_return_mean_all_nonzero_bps":
            float(np.mean(signed[f!=0])) if np.any(f!=0) else None,
        "source_signed_target_return_p10_bps":
            float(np.quantile(signed[f!=0],.1)) if np.any(f!=0) else None,
        "qty_imbalance_change_lookback_ms":50,
        "qty_imbalance_change_valid_10ms_grid_n":int(np.sum(valid_change)),
        "qty_imbalance_change_nonzero_n":int(np.sum(nonzero_d)),
        "qty_imbalance_change_signed_100ms_target_mean_bps":
            float(np.mean(signed_qty)) if len(signed_qty) else None,
        "qty_imbalance_change_signed_100ms_target_p10_bps":
            float(np.quantile(signed_qty,.1)) if len(signed_qty) else None,
        "market_execution_not_modeled":True,
        "overlapping_10ms_future_returns_autocorrelated":True,
        "not_out_of_sample":True}


def run(coin,out):
    if coin not in COINS:raise ValueError("Only BTC/SOL June controls")
    bspec,ospec=MARKETS[coin][:2]
    bn=read_native(DAY,*bspec)
    ok=read_native(DAY,*ospec)
    obj={
        "date":DAY,"coin":coin,"source":"native Binance book_ticker",
        "target":"native OKX book_ticker",
        "assumed_external_visible_lag_ms":2.5,
        "all_event_10ms_grid_control":unconditional_grid(bn,ok),
        "strict_quiet_history_before_future_jump":{
            str(lag)+"ms":quiet_prejump(bn,ok,lag) for lag in (5,10,20,50)},
        "jump_conditioned_original":{
            str(lag)+"ms":prejumps(bn,ok,lag) for lag in (5,10,20,50)},
        "warning":"A 97percent hit among *selected future jump events* can be event-conditioned selection bias; all fixed-time decisions are primary"
    }
    root=Path(out);root.mkdir(parents=True,exist_ok=True)
    (root/"microprice_opportunity_control.json").write_text(
        json.dumps(obj,indent=2,ensure_ascii=False,allow_nan=False),encoding="utf8")
    return obj


def markdown(rows):
    out=["# V6 BTC/SOL Microprice：事件选择偏差与无跳变时间点对照","",
         "静态先验10ms网格、严格早于当前的Binance/OKX行情（上限250ms），"
         "与提前5/10/20/50ms的未来Binance跳变条件化样本并列。",
         "未来回报不扣手续费，仅物理BBO研究，不能替代官方HBT盈利分析。","",
         "|币种|10ms全部有效决策|Microprice方向100ms均值bp|50ms盘口数量失衡变化方向100ms均值bp|前5ms跳变命中率|前5ms且之前50ms无BN跳变命中率|无跳变冷静窗数|",
         "|---|---:|---:|---:|---:|---:|---:|"]
    for a in rows:
        z=a["all_event_10ms_grid_control"]
        u=a["jump_conditioned_original"]["5ms"]
        q=a["strict_quiet_history_before_future_jump"]["5ms"]
        def p(v):return f"{v:.1%}" if v is not None else "N/A"
        m=z["source_signed_target_return_mean_all_nonzero_bps"]
        out.append(f"|{a['coin']}|{z['usable_source_and_target_times']:,}|"
                   f"{m:+.4f}|" if m is not None else
                   f"|{a['coin']}|{z['usable_source_and_target_times']:,}|N/A|")
        dd=z["qty_imbalance_change_signed_100ms_target_mean_bps"]
        delta=f"{dd:+.4f}" if dd is not None else "N/A"
        out[-1]+=f"{delta}|{p(u['aligned_fraction_excluding_zeros'])}|{p(q['future_jump_direction_hit_fraction'])}|{q['eligible_jumps_with_strict_quiet']}|"
    out+=["","数据中对未来Binance跳变做条件筛选会产生显著的选择偏差，"
          "原始跳变条件化命中率高并不自动意味着真实策略能够提前发单。"
          "单日全网格重叠回报具有高自相关，不能用普通IID标准误判断显著性。"]
    return "\n".join(out)+"\n"


if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--coin",required=True,choices=COINS)
    p.add_argument("--out",required=True)
    a=p.parse_args()
    x=run(a.coin,a.out)
    print("MICROPRICE_CONTROL",a.coin,x["all_event_10ms_grid_control"],
          x["strict_quiet_history_before_future_jump"],flush=True)
