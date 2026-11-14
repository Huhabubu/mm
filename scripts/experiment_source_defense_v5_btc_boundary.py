"""BTC 2026-06-01 one-day source-lag boundary and stronger gated filter test.

Same target official HBT tape, original G0, exact pretrade 10min parameters,
same 50min window and maker fee. Two assumed *extra* Binance visibility
latencies: 0ms vs 2.5ms (zero is an optimistic theoretical lower bound,
not measured co-located exchange latency). Trading order latency remains 1ms.
No geography/arbitrage causal inference possible with one historical day.
"""
import argparse
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np

from run_beta_one_direct_gap import causal_60s_basis
from experiment_source_defense_signal import past_grid_time_ema
from experiment_source_defense_v5_multiday import prepared,execute_one
from experiment_source_defense_v4_pilot import matched_stats

DATE="2026-06-01"
CASES=(
    "g0",
    "z1_cancel_qs",
    "high08_cancel_p80",
    "high10_widen_p80",
)


def high_options(name):
    if name=="g0":
        return {"sparse_mode":0},False
    if name=="z1_cancel_qs":
        return {"sparse_mode":2,"adaptive_z_gap":1.5,
                "adaptive_z_imp":1.0,"use_queue_shield":True,
                "queue_percentile":.5,"defense_hold_ns":75_000_000,
                "defense_cooldown_ns":75_000_000},True
    if name=="high08_cancel_p80":
        return {"sparse_mode":2,"gap_gate_bps":.8,
                "impulse_gate_bps":.3,
                "hysteresis_exit_th_bps":.1,
                "use_queue_shield":True,"queue_percentile":.5,
                "use_trade_flow":True,"trade_intensity_quantile":.8,
                "defense_hold_ns":75_000_000,"defense_cooldown_ns":75_000_000},True
    if name=="high10_widen_p80":
        return {"sparse_mode":1,"gap_gate_bps":1.,
                "impulse_gate_bps":.3,
                "hysteresis_exit_th_bps":.1,
                "widen_bps":1.,
                "use_queue_shield":True,"queue_percentile":.5,
                "use_trade_flow":True,"trade_intensity_quantile":.8,
                "defense_hold_ns":75_000_000,"defense_cooldown_ns":75_000_000},True
    raise ValueError("Unknown preregistered BTC boundary candidate")


def zero_delay_rebase(source):
    d=dict(source)
    # Binance BBO source timestamps are the same; only assumed transport lag changes.
    d["bt"]=source["bt"]-2_500_000
    d["tt"]=source["tt"]-2_500_000
    d["basis_t"],d["basis"]=causal_60s_basis(
        d["ot"],d["om"],d["bt"],d["bm"])
    d["ema_t"],d["ema"]=past_grid_time_ema(
        d["ot"],d["om"],d["bt"],d["bm"],1.)
    return d


def run(root):
    out=Path(root);out.mkdir(parents=True,exist_ok=True)
    with TemporaryDirectory(prefix="btc_boundary_v5_") as temporary:
        d=prepared(DATE,"BTC","okex-swap",Path(temporary),
                   extra_delay_ms=2.5,with_trades=True)
        ref=None
        report={
            "day":DATE,"coin":"BTC","venue":"okex-swap",
            "scope":"one already explored day only, no out-of-sample significance",
            "maker_fee_bps":2.,"official_hftbacktest":True,
            "trade_window_utc":"00:10–01:00",
            "extra_binance_feed_delay_tested_ms":[0.,2.5],
            "order_request_and_ack_ms":[1.,1.],
            "predeclared_models":list(CASES),
            "zero_ms_interpretation":"hypothetical no-extra-lag lower bound, not geographical trading speed evidence",
            "results":{}
        }
        for delay,data in [(2.5,d),(0.,zero_delay_rebase(d))]:
            results={}
            rawfills={}
            for name in CASES:
                opts,clock=high_options(name)
                model,fills=execute_one(data,"BTC","okex-swap",DATE,opts,clock,np.nan)
                results[name]=model
                rawfills[name]=fills
                print("BTC_BOUNDARY",delay,name,
                      model["trading"]["engine_fills"],
                      model["trading"]["cancel_requests"],
                      model["trading"]["end_net_marked_equity_usdt"],flush=True)
            trace=results["g0"]["calibration_trace_sha256"]
            if any(v["calibration_trace_sha256"]!=trace for v in results.values()):
                raise ValueError("Control price fit varied within latency case")
            g0=rawfills["g0"]
            n0=results["g0"]["trading"]["engine_fills"]
            compact={}
            for key,z in results.items():
                t=z["trading"]
                item={
                    "fills":t["engine_fills"],
                    "retention_vs_source_blind_g0":t["engine_fills"]/n0,
                    "cancel_requests":t["cancel_requests"],
                    "net_after_maker2bps_usdt":t["end_net_marked_equity_usdt"],
                    "fee_usdt":t["fee_usdt"],
                    "turnover_usdt":t["trading_value_usdt"],
                    "pretrade_thresholds":z["thresholds"],
                    "signal_gate_and_urgent_cancel_counts":z["counters"],
                    "defense_duty":z["defense_duty"]}
                if key!="g0":
                    paired=matched_stats(g0,rawfills[key],.01)
                    item["marked_fill_comparison"]=paired
                    s100=paired["selectivity"]["100ms"]
                    s500=paired["selectivity"]["500ms"]
                    item["passes_minimum_mechanical_conditions"]=(
                        item["retention_vs_source_blind_g0"]>=.75 and
                        item["cancel_requests"]<=1.5*results["g0"]["trading"]["cancel_requests"] and
                        s100["lift_mean_bps"] is not None and s100["lift_mean_bps"]>0 and
                        s500["lift_mean_bps"] is not None and s500["lift_mean_bps"]>0)
                compact[key]=item
            if delay==2.5:ref=compact["g0"]
            else:
                if ref["fills"]!=compact["g0"]["fills"] or abs(
                        ref["net_after_maker2bps_usdt"]-
                        compact["g0"]["net_after_maker2bps_usdt"])>1e-9:
                    raise AssertionError("Source-blind G0 changed when only Binance lag changed")
            report["results"][str(delay)+"ms"]=compact
        for name in CASES[1:]:
            a=report["results"]["0.0ms"][name]
            b=report["results"]["2.5ms"][name]
            def l(z,h):
                return z["marked_fill_comparison"]["selectivity"][h]["lift_mean_bps"]
            report.setdefault("delay_delta_0_minus_2p5",{})[name]={
                "fill_delta":a["fills"]-b["fills"],
                "net_after_fee_delta_usdt":a["net_after_maker2bps_usdt"]-
                                             b["net_after_maker2bps_usdt"],
                "selectivity_delta_100ms_bps":(
                    l(a,"100ms")-l(b,"100ms")
                    if l(a,"100ms") is not None and l(b,"100ms") is not None else None),
                "selectivity_delta_500ms_bps":(
                    l(a,"500ms")-l(b,"500ms")
                    if l(a,"500ms") is not None and l(b,"500ms") is not None else None)}
    path=out/"btc_latency_boundary.json"
    path.write_text(json.dumps(report,ensure_ascii=False,indent=2,allow_nan=False),encoding="utf8")
    lines=[
        "# BTC 2026-06-01 单日：0ms vs 2.5ms Binance附加可见延迟及高强度触发",
        "原始HBT、10min预热、UTC00:10–01:00、Maker2bps扣费口径一致。两档Binance额外延迟只改动源信号可见时刻，不改变1ms下单/回报模型。",
        "0ms代表理论最优额外运输延迟，不可视为实测交易所地理/套利竞争证据。",
        "",
        "|额外BN延迟|模型|成交/保留率|撤单|扣费净PnL USDT|100ms Lift|500ms Lift|",
        "|---|---|---:|---:|---:|---:|---:|"]
    for delay in ("0.0ms","2.5ms"):
        x=report["results"][delay]
        for key,v in x.items():
            def fmt(h):
                m=v.get("marked_fill_comparison")
                z=m["selectivity"][h]["lift_mean_bps"] if m else None
                return f"{z:+.4f}" if z is not None else "N/A"
            lines.append(f"|{delay}|{key}|{v['fills']} / "
                         f"{v['retention_vs_source_blind_g0']:.1%}|"
                         f"{v['cancel_requests']}|"
                         f"{v['net_after_maker2bps_usdt']:+.4f}|"
                         f"{fmt('100ms')}|{fmt('500ms')}|")
    lines+=["", "### 解释",
            "两延迟条件必须检验同窗 G0 在单边不读源价格/消息的条件下完全相同。",
            "本试验只能识别额外Binance可见延迟变化对这一天策略输出的敏感性，无法分离真实跨所物理速度、队列ACK或HBT模型误差。",
            "所有正向Lift均为同笔时间/方向配对描述性指标，不能直接称真实因果避免毒单。"]
    md="\n".join(lines)+"\n"
    (out/"btc_latency_boundary.md").write_text(md,encoding="utf8")
    print(md)
    return report


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--out",default="reports/source_defense/v5_btc_boundary")
    run(p.parse_args().out)


if __name__=="__main__":
    main()
