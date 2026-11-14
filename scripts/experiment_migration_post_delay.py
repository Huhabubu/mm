"""Post-OKX-AWS-Tokyo 0/1/2.5ms delay probe with official HBT.

Fixed G0 / Z1 cancel-QS / Z1 widen-QS, matched initial A,k,sigma.
All PnL net of 2bps; queue proxy is orders/cancel requests, NOT measured
priority lost at exchange. No full-24h replay and no latency causal claim.
"""
import argparse
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
from run_beta_one_direct_gap import causal_60s_basis
from experiment_source_defense_signal import past_grid_time_ema
from experiment_source_defense_v5_multiday import (
    prepared, prior_from_stored_admission
)
from experiment_source_defense_v6_mechanism import model_run, groups, short_metrics

DAYS=("2026-09-01","2026-10-01","2026-11-01")
COINS=("SOL","DOGE","1000PEPE")
DELAYS=(0.0,1.0,2.5)
MODES=("source","widen")


def with_delay(base, ms):
    if ms not in DELAYS:
        raise ValueError("Delay beyond precommitted Tokyo probe")
    d = dict(base)
    # prepared() uses extra 2.5ms+1ns; adjust the same Binance tape, preserving
    # all original exchange/local stamps and avoiding duplicate tape conversion.
    difference = int(round((ms - 2.5) * 1_000_000))
    d["bt"] = base["bt"] + difference
    # No taker trade gate is used by either fixed z1 mode.
    d["basis_t"], d["basis"] = causal_60s_basis(
        d["ot"],d["om"],d["bt"],d["bm"])
    d["ema_t"],d["ema"] = past_grid_time_ema(
        d["ot"],d["om"],d["bt"],d["bm"],1.)
    return d


def run(day, coin, out, admission):
    if day not in DAYS or coin not in COINS:
        raise ValueError("Only 3 pure-Tokyo dates and 3 OKX markets")
    venue="okex-swap"
    prior, meta = (prior_from_stored_admission(admission,coin,venue,day)
                   if coin in ("SOL","DOGE") else (np.nan,{"fit_kind":"original_5plus"}))
    with TemporaryDirectory(prefix="migration_postdelay_") as work:
        d = prepared(day,coin,venue,Path(work),extra_delay_ms=2.5,with_trades=False)
        base,bf,be = model_run(d,"g0",prior)
        results = {}
        for delay in DELAYS:
            variant = with_delay(d,delay)
            for mode in MODES:
                s,sf,se=model_run(variant,mode,prior)
                key=f"{mode}_{delay:g}ms"
                a=groups(bf,sf,se,"exchange")
                options=s["trading"]
                results[key]={
                    "extra_binance_delay_ms":delay,
                    "action": "z1_cancel_qs" if mode=="source" else "z1_widen_qs",
                    "strategy":short_metrics(s,sf,se,False),
                    "source_vs_G0_fill_groups_exchange_clock":a,
                    "retention_fills_vs_G0": options["engine_fills"]/base["trading"]["engine_fills"]
                        if base["trading"]["engine_fills"] else None,
                    "request_count_delta_vs_G0": {
                        "cancel":options["cancel_requests"]-base["trading"]["cancel_requests"],
                        "submit":options["submissions"]-base["trading"]["submissions"]
                    },
                    "matching_initial_A_k_sigma":all(
                        s["initial_parameters"][k]==base["initial_parameters"][k]
                        for k in ("A_per_second","k_per_tick","sigma_tick_per_sqrt_second"))
                }
                if not results[key]["matching_initial_A_k_sigma"]:
                    raise AssertionError("Control GLFT fit differs")
        report={"day":day,"coin":coin,"venue":venue,
                "pretrade_minutes":d["pretrade_s"]/60,
                "trade_window_UTC":d["trade_start_utc"]+"–"+d["trade_end_utc"],
                "prior":meta,"G0":short_metrics(base,bf,be,True),
                "models":results,"maker_fee_bps":2.,
                "HBT_official":True,
                "order_request_delay_ms":1.0,"order_response_delay_ms":1.0,
                "binance_extras":list(DELAYS),
                "limits":"Clock-local defense vs exchange-fill alignment uncalibrated; canceled orders not linked to exchange cancel ACK, order counts do not measure exact queue rank."}
        out=Path(out);out.mkdir(parents=True,exist_ok=True)
        (out/"case.json").write_text(json.dumps(report,ensure_ascii=False,indent=2,allow_nan=False))
        print("POST_DELAY",coin,day,{k:(x["strategy"]["fills"],x["retention_fills_vs_G0"])
              for k,x in results.items()},flush=True)


def aggregate(root):
    cases={}
    for path in Path(root).rglob("case.json"):
        x=json.loads(path.read_text())
        key=(x["day"],x["coin"])
        if key in cases:raise ValueError("Duplicate case "+str(key))
        cases[key]=x
    expected={(d,c) for d in DAYS for c in COINS}
    if set(cases)!=expected:
        raise ValueError("Incomplete pure-Tokyo sensitivity: "+str(sorted(expected-set(cases))))
    summary={}
    for coin in COINS:
        summary[coin]={}
        for mode in MODES:
            for delay in DELAYS:
                k=f"{mode}_{delay:g}ms"
                rows=[cases[(day,coin)]["models"][k] for day in DAYS]
                base=[cases[(day,coin)]["G0"] for day in DAYS]
                group={}
                for category in ("Group_A_active_or_200ms_post_cancel",
                                 "Kept","Group_B_no_local_defense_or_cancel_tail",
                                 "Dropped_all"):
                    group[category]={}
                    for horizon in (100,500):
                        specs=[r["source_vs_G0_fill_groups_exchange_clock"][category][f"{horizon}ms"]
                               for r in rows]
                        valid=[(s["fresh_n"],s["markout_mean_bps"]) for s in specs if s["markout_mean_bps"] is not None]
                        n=sum(x[0] for x in valid)
                        group[category][f"{horizon}ms"]={
                            "fresh_n":n,
                            "mean_markout_bps":sum(count*m for count,m in valid)/n if n else None
                        }
                summary[coin][k]={
                    "days":list(DAYS),
                    "G0_fills_total":sum(x["fills"] for x in base),
                    "source_fills_total":sum(r["strategy"]["fills"] for r in rows),
                    "fill_retention_pooled":sum(r["strategy"]["fills"] for r in rows)/sum(x["fills"] for x in base),
                    "source_net_2bps_total_USDT":sum(r["strategy"]["fee_included_net_usdt"] for r in rows),
                    "G0_net_2bps_total_USDT":sum(x["fee_included_net_usdt"] for x in base),
                    "defense_episodes_total":sum(r["strategy"]["defense_episode_count"] for r in rows),
                    "defense_duration_ms_total":sum(r["strategy"]["defense_duration_ms"] for r in rows),
                    "urgent_cancel_requests_total":sum(r["strategy"]["urgent_cancel_request_count"] for r in rows),
                    "cancel_request_delta_vs_G0":sum(r["request_count_delta_vs_G0"]["cancel"] for r in rows),
                    "submit_request_delta_vs_G0":sum(r["request_count_delta_vs_G0"]["submit"] for r in rows),
                    "group_markouts":group,
                    "day_fills":{day:r["strategy"]["fills"] for day,r in zip(DAYS,rows)},
                    "day_G0_fills":{day:b["fills"] for day,b in zip(DAYS,base)}
                }
    return {"cases":[cases[(d,c)] for d in DAYS for c in COINS],
            "summary":summary,"delays_ms":list(DELAYS),
            "fee_all_net":2.0,
            "limits":"No real RTT latency or exchange cancel ACK. Nine 50-minute historical-day windows; local defense vs exchange fill clock mismatch; gross markouts diagnostic only. Queue cost proxies are cancel/submit counts, not exact queue rank."}


def markdown(x):
    L=["# 纯东京期：0 / 1 / 2.5ms额外Binance可见延迟", "",
       "2026-09/10/11三个已探索月首日、各50分钟；统一Maker 2bps、订单1ms发送+1ms回报、官方HBT。", "",
       "Queue Shield为现有P50挂单队列保护；队列代价只提供订单撤改数量代理，缺失真实队列优先级变动证据。", "",
       "|市场|模型|BN额外延迟|G0成交|模型成交|保留率|Group A 100/500ms (bps)|撤单差vs G0|提交差vs G0|净PnL 3日USDT|",
       "|---|---|---:|---:|---:|---:|---|---:|---:|---:|"]
    for coin,data in x["summary"].items():
        for key,row in data.items():
            g=row["group_markouts"]["Group_A_active_or_200ms_post_cancel"]
            def val(h):
                v=g[f"{h}ms"]["mean_markout_bps"]
                return "NA" if v is None else f"{v:+.3f}"
            mode, delay=key.split("_",1)
            L.append(f"|{coin} OKX|{('cancel+QS' if mode=='source' else 'widen+QS')}|{delay}|{row['G0_fills_total']}|{row['source_fills_total']}|{row['fill_retention_pooled']:.1%}|{val(100)}/{val(500)}|{row['cancel_request_delta_vs_G0']:+d}|{row['submit_request_delta_vs_G0']:+d}|{row['source_net_2bps_total_USDT']:+.4f}|")
    L.extend(["", "以每市场相同G0和固定Z1门槛对比三种Binance额外等待。Group A的定义是原G0独有成交与新策略同危险侧防御窗口/200ms撤单余波重合，不等同已成功阻止的交易所成交。",
              "三天全部经过前期探索，不能宣称独立样本外统计显著性；1ms订单延迟仍为仿真假设。"])
    return "\n".join(L)+"\n"


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--mode",choices=("case","aggregate"),required=True)
    p.add_argument("--day")
    p.add_argument("--coin")
    p.add_argument("--source")
    p.add_argument("--admission",default="reports/source_defense/step3_fit/step3_fit_admission.json")
    p.add_argument("--out",required=True)
    a=p.parse_args()
    out=Path(a.out);out.mkdir(parents=True,exist_ok=True)
    if a.mode=="case":
        run(a.day,a.coin,out,a.admission)
    else:
        x=aggregate(a.source)
        (out/"migration_tokyo_delay.json").write_text(json.dumps(x,ensure_ascii=False,indent=2,allow_nan=False))
        (out/"migration_tokyo_delay.md").write_text(markdown(x))
        print(markdown(x))


if __name__=="__main__":
    main()
