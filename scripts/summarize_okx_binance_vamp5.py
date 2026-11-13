"""Paired daily evaluation of BTC/SOL OKX B with identical top-5 source event clocks.

Each day is one independent 50 minute HBT run. Never impute calibration failure
as zero PnL. The legacy native book_ticker baseline has a different event clock
and is not an isolated price-weighting control.
"""
import argparse
import csv
import json
from pathlib import Path
from statistics import mean

DAYS=tuple(f"2026-{m:02d}-01" for m in range(6,12))
COINS=("BTC","SOL")
BASE="glft"
MID="glft_binance_defensive_snapshot_mid"
VAMP="glft_binance_defensive_vamp5"
MODELS=(BASE,MID,VAMP)
TICKS={"BTC":.1,"SOL":.01}


def measures(r,model):
    t=r["variants"][model]["trading"]
    net=float(t["end_net_marked_equity_usdt"])
    fee=float(t["fee_usdt"])
    return {
        "net_usdt":net,"fee_usdt":fee,"gross_usdt":net+fee,
        "fills":int(t["engine_fills"]),
        "cancels":int(t["cancel_requests"]),
        "submissions":int(t["submissions"]),
        "mean_abs_inventory_contracts":float(t["mean_abs_inventory_contracts"]),
        "rms_inventory_contracts":float(t["rms_inventory_contracts"]),
    }


def aggregate(folder):
    data={"market_model_means":[],"daily":[],"pairs":[],"missing_calibration":[]}
    for coin in COINS:
        valid=[]
        for day in DAYS:
            path=Path(folder)/"cases"/coin/(day+".json")
            if not path.is_file():
                raise FileNotFoundError(path)
            r=json.loads(path.read_text(encoding="utf-8"))
            assert r["day"]==day and r["minutes"]==50 and r["warmup_minutes"]==10
            assert r["market"]["exchange"]=="okex-swap"
            assert r["market"]["coin"]==coin and r["market"]["tick"]==TICKS[coin]
            assert r["binance_signal_source"]=="snapshot_5"
            assert r["binance_snapshot5"]["source"]=="binance-futures/book_snapshot_5"
            assert r["binance_snapshot5"]["same_event_clock_for_mid_and_vamp"]
            assert r["extra_binance_delay_ms"]==2.5
            assert r["order_request_latency_ns"]==1_000_000
            assert r["order_response_latency_ns"]==1_000_000
            assert r["same_side_min_action_interval_ns"]==20_000_000
            assert r["market"]["maker_fee"]==.0002 and r["market"]["taker_fee"]==.0005
            assert r["status"] in ("complete","no_glft_calibration"),(coin,day,r["status"])
            if r["status"]=="no_glft_calibration":
                assert not r["calibration_diagnostic"]["valid"]
                data["missing_calibration"].append({"coin":coin,"day":day})
                continue
            assert set(r["variants"])==set(MODELS)
            assert r["signal_rows"]==r["binance_snapshot5"]["rows"]
            base=r["variants"][BASE]
            cal=base["calibration_trace"]
            for m in MODELS:
                v=r["variants"][m]
                assert v["calibration_trace"]==cal
                assert v["samples"]==base["samples"]
                assert v["time_count"]["binance"]==base["time_count"]["binance"]
                assert v["recorder"]["samples"]>=29000
                assert v["signal_price_kind"]==("vamp5" if m==VAMP else "mid")
                assert v["trading"]["fee_usdt"]>=0.
            valid.append((day,r))
            x,y=measures(r,MID),measures(r,VAMP)
            data["daily"].append({
                "day":day,"coin":coin,
                "mid_net_usdt":x["net_usdt"],"vamp_net_usdt":y["net_usdt"],
                "vamp_minus_mid_net_usdt":y["net_usdt"]-x["net_usdt"],
                "vamp_minus_mid_gross_usdt":y["gross_usdt"]-x["gross_usdt"],
                "vamp_minus_mid_fee_usdt":y["fee_usdt"]-x["fee_usdt"],
                "mid_fills":x["fills"],"vamp_fills":y["fills"],
                "mid_cancels":x["cancels"],"vamp_cancels":y["cancels"],
                "snapshot5_rows":r["signal_rows"],
                "min_vamp_minus_mid_usdt":r["binance_snapshot5"]["raw_vamp_minus_mid_min_usdt"],
                "max_vamp_minus_mid_usdt":r["binance_snapshot5"]["raw_vamp_minus_mid_max_usdt"],
            })
        expected=2 if coin=="SOL" else 6
        if len(valid)!=expected:
            raise RuntimeError(f"GLFT calibration coverage changed for {coin}: {len(valid)}")
        for m in MODELS:
            z=[measures(r,m) for _,r in valid]
            rec={"coin":coin,"model":m,"valid_days":len(valid)}
            rec.update({k:float(mean(v[k] for v in z)) for k in z[0]})
            data["market_model_means"].append(rec)
        sub=[r for r in data["daily"] if r["coin"]==coin]
        data["pairs"].append({
            "coin":coin,"valid_days":len(valid),
            "vamp_better_days":sum(r["vamp_minus_mid_net_usdt"]>0 for r in sub),
            "delta_net_mean_usdt":float(mean(r["vamp_minus_mid_net_usdt"] for r in sub)),
            "delta_gross_mean_usdt":float(mean(r["vamp_minus_mid_gross_usdt"] for r in sub)),
            "delta_fee_mean_usdt":float(mean(r["vamp_minus_mid_fee_usdt"] for r in sub)),
            "delta_fills_mean":float(mean(r["vamp_fills"]-r["mid_fills"] for r in sub)),
            "delta_cancels_mean":float(mean(r["vamp_cancels"]-r["mid_cancels"] for r in sub)),
        })
    assert len(data["daily"])==8
    assert len(data["missing_calibration"])==4
    return data


def format_report(d):
    z=["# Binance五档原始VAMP与五档快照普通中间价：OKX单边保护配对回测","",
       "基于已修复P0报价中心的官方hftbacktest 2.4.4。Tardis Binance Futures "
       "book_snapshot_5（重建的五档L2数据）；两种信号**完全共享同一快照事件时钟**，"
       "两者各自按过去60秒历史偏离重新估计因果基差。5档价格与数量按交叉数量原始加权，"
       "不作截断/距离衰减/价格夹紧。目标OKX原生L2/trades/BBO与成交/队列模型不变。"
       "方案B保留alpha=0、beta=1、2.5ms额外行情可见延迟、1ms订单请求及回报、"
       "20ms同侧操作间隔、Maker2bps/Taker5bps费用。",
       "",
       "**重要：** 旧方案B使用独立原生book_ticker频道、当前实验共同使用"
       "重建五档快照频道；不能把旧BBO策略与新VAMP策略差额全部归因于挂单量。"
       "真正用于测量加权增量的，是同一五档事件时钟上的五档普通中间价 vs 五档VAMP。",
       "",
       "## 日均含手续费净盯市收益（USDT/有效日50分钟）","",
       "| 做市市场 | 有效日 | 纯GLFT（同事件钟） | 五档普通中间价+B | 五档VAMP+B | VAMP−普通中间价 | VAMP占优天数 |",
       "|---|---:|---:|---:|---:|---:|---:|"]
    for coin in COINS:
        r={m["model"]:m for m in d["market_model_means"] if m["coin"]==coin}
        p=next(x for x in d["pairs"] if x["coin"]==coin)
        z.append(f"| OKX {coin} | {p['valid_days']} "
                 f"| {r[BASE]['net_usdt']:+.6f} "
                 f"| {r[MID]['net_usdt']:+.6f} "
                 f"| **{r[VAMP]['net_usdt']:+.6f}** "
                 f"| {p['delta_net_mean_usdt']:+.6f} "
                 f"| {p['vamp_better_days']}/{p['valid_days']} |")
    z+=["","## 两种五档信号的交易强度与费用","",
        "| 市场 | 普通中间价成交/日 | VAMP成交/日 | 普通中间价手续费 | VAMP手续费 | VAMP毛收益变化 | VAMP净收益变化 |",
        "|---|---:|---:|---:|---:|---:|---:|"]
    for coin in COINS:
        r={m["model"]:m for m in d["market_model_means"] if m["coin"]==coin}
        p=next(x for x in d["pairs"] if x["coin"]==coin)
        z.append(f"| OKX {coin} "
                 f"| {r[MID]['fills']:.2f} | {r[VAMP]['fills']:.2f} "
                 f"| {r[MID]['fee_usdt']:.6f} | {r[VAMP]['fee_usdt']:.6f} "
                 f"| {p['delta_gross_mean_usdt']:+.6f} | {p['delta_net_mean_usdt']:+.6f} |")
    z+=["","## 每日期独立比较","",
        "| 日期 | 市场 | VAMP−普通中间价净收益(USDT) | VAMP成交 | 普通中间价成交 | VAMP−普通中间价原始价格范围(USDT/币) |",
        "|---|---|---:|---:|---:|---|"]
    for r in d["daily"]:
        z.append(f"| {r['day']} | {r['coin']} "
                 f"| {r['vamp_minus_mid_net_usdt']:+.6f} "
                 f"| {r['vamp_fills']} | {r['mid_fills']} "
                 f"| {r['min_vamp_minus_mid_usdt']:+.6f} ~ {r['max_vamp_minus_mid_usdt']:+.6f} |")
    z+=["","## 研究界限","",
        "- 当前六个首日片段是既往反复探索的历史，OKX SOL仅两日可拟合GLFT，不能把4个无效日当0收益，缺乏样本外统计显著性。",
        "- 同一五档L2行情有可能在最优报价数量不变时也更新，信号处理次数高于原生book_ticker；"
        "单独比较新VAMP与旧book_ticker代码无法分辨来源时钟差异。"
        "本组内部两个方案共享同一原始五档事件时间与数据可见延迟。",
        "- VAMP可能出最优买卖价界，但本研究不截断；基差仍为严格过去60s的因果历史均值。",
        "- 策略盈亏计模拟手续费、不计资金费率、期末真实平仓成本与实际交易所限频。",
        "- 成交减少引发的手续费节省/库存变动与挂单量信息优势需要进一步配对识别；"
        "不能直接把净收益差解释为Binance高频价格发现能力。",
    ]
    return "\n".join(z)+"\n"


def main():
    a=argparse.ArgumentParser()
    a.add_argument("--root",default="gathered")
    a.add_argument("--out",default="reports/okx_vamp5_sixdate")
    x=a.parse_args()
    d=aggregate(x.root)
    out=Path(x.out);out.mkdir(parents=True,exist_ok=True)
    (out/"summary.json").write_text(json.dumps(d,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    (out/"report.md").write_text(format_report(d),encoding="utf-8")
    with (out/"daily.csv").open("w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=list(d["daily"][0]))
        w.writeheader();w.writerows(d["daily"])
    print("OKX_VAMP5_VERIFIED_COMPLETE_CASES",len(d["daily"]),
          "CALIBRATION_FAILURES",len(d["missing_calibration"]),flush=True)
    for p in d["pairs"]:
        print("OKX_VAMP5_EFFECT",p["coin"],"DAYS",p["valid_days"],
              "NET_DELTA",p["delta_net_mean_usdt"],
              "GROSS_DELTA",p["delta_gross_mean_usdt"],
              "FEE_DELTA",p["delta_fee_mean_usdt"],
              "FILL_DELTA",p["delta_fills_mean"],
              "WIN_DAYS",p["vamp_better_days"],flush=True)
    for row in d["market_model_means"]:
        print("OKX_VAMP5_MODEL",row["coin"],row["model"],
              "NET",row["net_usdt"],"FEES",row["fee_usdt"],
              "FILLS",row["fills"],"CANCELS",row["cancels"],flush=True)


if __name__=="__main__":
    main()
