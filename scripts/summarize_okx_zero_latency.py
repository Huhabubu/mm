"""Summarize paired, repaired-code OKX zero vs 2.5ms delay official HBT runs.

24 day-coin-profile cases; all models within a case use identical market
data and their own official matching engine. No custom matching.
"""
import argparse
import csv
import json
import statistics
from pathlib import Path

DAYS = tuple(f"2026-{m:02d}-01" for m in range(6, 12))
COINS = ("BTC", "SOL")
PROFILES = {"zero": (0.0, 0, 0), "legacy_latency": (2.5, 1_000_000, 1_000_000)}
MODELS = ("glft", "glft_binance", "glft_local_peak", "glft_binance_local_peak")
PAIRS = (("glft","glft_binance"),("glft_local_peak","glft_binance_local_peak"))


def metric(report, model):
    t=report["variants"][model]["trading"]
    net=float(t["end_net_marked_equity_usdt"])
    fee=float(t["fee_usdt"])
    return {
        "gross":net+fee,
        "fee":fee,
        "net":net,
        "fills":int(t["engine_fills"]),
        "cancel_requests":int(t["cancel_requests"]),
        "submit_requests":int(t["submit_requests"]),
    }


def summarize(root):
    cases={}
    for profile, parameters in PROFILES.items():
        for coin in COINS:
            for day in DAYS:
                path=Path(root)/"cases"/profile/coin/f"{day}.json"
                if not path.is_file():
                    raise FileNotFoundError(path)
                data=json.loads(path.read_text(encoding="utf-8"))
                key=(profile,coin,day)
                assert data["market"]["exchange"]=="okex-swap"
                assert data["market"]["coin"]==coin
                assert data["day"]==day
                assert data["minutes"]==50
                assert data["extra_binance_delay_ms"]==parameters[0]
                assert data["order_request_latency_ns"]==parameters[1]
                assert data["order_response_latency_ns"]==parameters[2]
                assert data["same_side_min_action_interval_ns"]==20_000_000
                assert data["market"]["maker_fee"]==.0002
                assert data["market"]["taker_fee"]==.0005
                assert data["status"] in ("complete","no_glft_calibration"),(key,data["status"])
                if data["status"]=="complete":
                    assert set(data["variants"])==set(MODELS)
                    cal=data["variants"]["glft"]["calibration_trace"]
                    for name in MODELS:
                        assert data["variants"][name]["calibration_trace"]==cal
                        assert data["variants"][name]["trading"]["fee_usdt"]>=0
                        assert data["variants"][name]["recorder"]["samples"]>=29000
                cases[key]=data

    # Calibration is market-only; pairing invalid calibration with a noninvalid
    # case is evidence of inconsistent event time or a changed model.
    for coin in COINS:
        for day in DAYS:
            a=cases[("zero",coin,day)]
            b=cases[("legacy_latency",coin,day)]
            if a["status"]!=b["status"]:
                raise RuntimeError(f"Different GLFT calibration status across latency profiles: {coin} {day}")
            if a["status"]=="complete":
                assert a["variants"]["glft"]["calibration_trace"]==b["variants"]["glft"]["calibration_trace"]
                assert a["scenario"]==b["scenario"]

    summary_rows=[]
    daily_rows=[]
    for coin in COINS:
        for profile in PROFILES:
            valid=[cases[(profile,coin,day)] for day in DAYS
                   if cases[(profile,coin,day)]["status"]=="complete"]
            if not valid:
                raise RuntimeError(f"No valid cases {profile} {coin}")
            for day in DAYS:
                data=cases[(profile,coin,day)]
                for i,(base,guided) in enumerate(PAIRS):
                    if data["status"]!="complete":
                        daily_rows.append({"coin":coin,"profile":profile,"day":day,
                                          "pair":"basic" if i==0 else "peak",
                                          "status":"no_glft_calibration"})
                        continue
                    x,y=metric(data,base),metric(data,guided)
                    daily_rows.append({
                        "coin":coin,"profile":profile,"day":day,
                        "pair":"basic" if i==0 else "peak", "status":"complete",
                        "base_model":base,"binance_model":guided,
                        "base_gross":x["gross"],"binance_gross":y["gross"],
                        "base_fee":x["fee"],"binance_fee":y["fee"],
                        "base_net":x["net"],"binance_net":y["net"],
                        "binance_minus_base_gross":y["gross"]-x["gross"],
                        "binance_minus_base_fee":y["fee"]-x["fee"],
                        "binance_minus_base_net":y["net"]-x["net"],
                        "base_fills":x["fills"],"binance_fills":y["fills"],
                        "base_cancel_requests":x["cancel_requests"],
                        "binance_cancel_requests":y["cancel_requests"],
                        "base_submit_requests":x["submit_requests"],
                        "binance_submit_requests":y["submit_requests"],
                    })
            for model in MODELS:
                mv=[metric(d,model) for d in valid]
                mean=lambda name: float(statistics.mean(x[name] for x in mv))
                summary_rows.append({"coin":coin,"profile":profile,"model":model,
                                     "valid_days":len(valid),"gross":mean("gross"),
                                     "fee":mean("fee"),"net":mean("net"),
                                     "fills":mean("fills"),
                                     "cancel_requests":mean("cancel_requests"),
                                     "submit_requests":mean("submit_requests")})
    pair_summaries=[]
    for coin in COINS:
        for pair in ("basic","peak"):
            grouped={}
            for profile in PROFILES:
                entries=[r for r in daily_rows if r["coin"]==coin and r["profile"]==profile
                         and r["pair"]==pair and r["status"]=="complete"]
                mean=lambda col:float(statistics.mean(x[col] for x in entries))
                grouped[profile]={"valid_days":len(entries),
                                  "delta_net_mean":mean("binance_minus_base_net"),
                                  "delta_gross_mean":mean("binance_minus_base_gross"),
                                  "delta_fee_mean":mean("binance_minus_base_fee"),
                                  "delta_fills_mean":mean("binance_fills")-mean("base_fills"),
                                  "delta_cancels_mean":mean("binance_cancel_requests")-mean("base_cancel_requests"),
                                  "improved_days":sum(x["binance_minus_base_net"]>0 for x in entries)}
            assert grouped["zero"]["valid_days"]==grouped["legacy_latency"]["valid_days"]
            pair_summaries.append({
                "coin":coin,"pair":pair,
                "zero":grouped["zero"],"legacy_latency":grouped["legacy_latency"],
                "difference_in_binance_signal_effect_net":
                    grouped["zero"]["delta_net_mean"]-grouped["legacy_latency"]["delta_net_mean"]
            })
    missing=[{"coin":coin,"day":day,
              "reason":cases[("zero",coin,day)].get("calibration_diagnostic")}
             for coin in COINS for day in DAYS
             if cases[("zero",coin,day)]["status"]!="complete"]
    return {"protocol":"OKX BTC SOL repaired-code paired zero vs 2.5ms event HBT",
            "cases":len(cases),"summary_rows":summary_rows,
            "daily_rows":daily_rows,"pair_summaries":pair_summaries,
            "missing":missing}


def markdown(d):
    z=["# 修复后GLFT：OKX BTC/SOL 严格零延迟 vs 原2.5ms/1ms场景",
       "",
       "每日期10min预热+50min真实Tardis历史行情，6月至11月每月1日。"
       "全部官方hftbacktest==2.4.4，仅测试Binance可见时间和OKX订单请求/回报延迟；"
       "每侧20ms操作间隔、手续费、GLFT参数与成交队列模型不变。"
       "两个场景都从**已修复P0同步目标中间价**的同一提交产生，不用旧版2.5ms数字替代。",
       "",
       "| 市场 | 模型 | 延迟情景 | 有效日期 | 毛收益 USDT/50min | 手续费 | 含费净收益 | 成交笔数 | 撤单请求 |",
       "|---|---|---|---:|---:|---:|---:|---:|"]
    for r in d["summary_rows"]:
        z.append(f"| OKX {r['coin']} | {r['model']} | {r['profile']} | {r['valid_days']} "
                 f"| {r['gross']:+.6f} | {r['fee']:.6f} | {r['net']:+.6f} "
                 f"| {r['fills']:.2f} | {r['cancel_requests']:.2f} |")
    z+=["","## Binance 信号对 GLFT 的日均增量（同情景β1减β0）","",
        "| 市场 | 加局部峰值 | 有效日期 | 0延迟净改善 | 2.5ms行情+1ms请求/回复净改善 | 零延迟下额外改善 |",
        "|---|---|---:|---:|---:|---:|"]
    for p in d["pair_summaries"]:
        z.append(f"| OKX {p['coin']} | {'否' if p['pair']=='basic' else '是'} "
                 f"| {p['zero']['valid_days']} | {p['zero']['delta_net_mean']:+.6f} "
                 f"| {p['legacy_latency']['delta_net_mean']:+.6f} "
                 f"| {p['difference_in_binance_signal_effect_net']:+.6f} |")
    z+=["","## 解释约束","",
        "- 所有表中金额都是扣手续费的官方HBT净盯市收益；毛收益=净收益+模拟手续费。"
        "没有纳入资金费率或期末真实平仓滑点。",
        "- β0、β1共用同一个Binance事件时钟，因此β0仍可能被Binance消息唤醒重新检查OKX报价，"
        "各情景内部的β1−β0差值比跨情景直接对比单个策略净收益更合适。",
        "- 零延迟是严格的**假想执行上界**，按Binance Tardis local_timestamp精确可见（取消额外1ns），"
        "OKX的request/response也是0ns；底层OKX历史市场数据本身仍受采集传播延迟，"
        "模拟没有修正真实跨交易所采集设备时钟同步。",
        "- 20ms同侧下单/撤单间隔没有取消；不是零间隔下的策略。"
        "需要另外的实验才能知道其约束作用，不能把剩余问题全部归因为网络延迟。",
        "- OKX SOL不能拟合的日期保持缺失状态，不当作零收益。"
        "每日报告及原生Recorder/逐笔模拟成交在Action附件可复核。",
        "- 6日期均为先前已探索月首日的50min窗口，不能宣称统计显著性或实盘盈利。",
        "",
        "## GLFT 校准不可识别的日期",""]
    for v in d["missing"]:
        z.append(f"- {v['coin']} {v['day']}：no_glft_calibration")
    return "\n".join(z)+"\n"


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--source",default="gathered")
    p.add_argument("--out",default="reports/zero_latency_compare")
    args=p.parse_args()
    d=summarize(args.source)
    out=Path(args.out);out.mkdir(parents=True,exist_ok=True)
    (out/"summary.json").write_text(json.dumps(d,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    (out/"summary.md").write_text(markdown(d),encoding="utf-8")
    fields=set().union(*(set(r) for r in d["daily_rows"]))
    with (out/"daily.csv").open("w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=["coin","profile","day","pair","status"]+
                         sorted(fields-set(("coin","profile","day","pair","status"))))
        w.writeheader()
        w.writerows(d["daily_rows"])
    print("VALIDATION_OK",d["cases"],"SCENARIOS",len(d["missing"]),"NON_IDENTIFIABLE_DATES",flush=True)
    for p in d["pair_summaries"]:
        print("PAIRED",p["coin"],p["pair"],
              "DAYS",p["zero"]["valid_days"],
              "DELTA_ZERO",p["zero"]["delta_net_mean"],
              "DELTA_DELAYED",p["legacy_latency"]["delta_net_mean"],
              "LATENCY_IMPACT",p["difference_in_binance_signal_effect_net"],flush=True)
    for r in d["summary_rows"]:
        print("MODEL",r["coin"],r["profile"],r["model"],
              "NET",r["net"],"GROSS",r["gross"],
              "FEES",r["fee"],"FILLS",r["fills"],"CANCELS",r["cancel_requests"],flush=True)


if __name__=="__main__":
    main()
