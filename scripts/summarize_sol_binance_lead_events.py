"""Aggregate six native-event Binance->OKX SOL leading opportunity diagnostics."""
import argparse
import json
from pathlib import Path

DAYS=tuple(f"2026-{x:02d}-01" for x in range(6,12))
HORIZONS=("50ms","100ms","500ms","1000ms")
THRESHOLDS=("1bps","2bps","5bps","10bps")


def summarize(rows):
    out={"days":len(rows),"by_day":rows,"horizons":{}}
    for name in HORIZONS:
        part={}
        for thr in THRESHOLDS:
            z=[r["horizons"][name]["thresholds"][thr] for r in rows]
            part[thr]={
                "raw_binance_price_change_events_with_unmoved_okx":
                    sum(x["event_count"] for x in z),
                "first_event_per_okx_price_stable_interval":
                    sum(x["unique_okx_unchanged_segments"] for x in z),
                "still_valid_after_2p5ms":
                    sum(x["visible_good_count"] for x in z),
                "okx_still_unchanged_after_2p5ms":
                    sum(x["first_visible_still_same_okx_price_count"] for x in z),
                "still_ge_10bps_corrected_gap_after_2p5ms":
                    sum(x["visible_gap_still_at_least_10bps_count"] for x in z),
                "first_event_survival_counts":{
                    lag:sum(x["follow_survival"][lag]["n"] for x in z)
                    for lag in ("1ms","2.5ms","3.5ms","5ms","10ms","20ms")},
                "sample_events":[e for r in rows for e in r["horizons"][name]["thresholds"][thr]["examples"]][:10]
            }
        out["horizons"][name]={
            "binance_horizon_price_abs_max_bps":
                max(r["horizons"][name]["max_abs_binance_price_move_bps"].get("max",0) for r in rows),
            "binance_horizon_price_abs_max_when_okx_unchanged_bps":
                max(r["horizons"][name]["max_abs_when_target_unchanged"].get("max",0) for r in rows),
            "apparent_unfiltered_ge10bps":sum(r["horizons"][name]["apparent_unfiltered_ge10bps"] for r in rows),
            "apparent_unfiltered_ge10bps_first_per_okx_segment":sum(
                r["horizons"][name]["apparent_unfiltered_ge10bps_first_per_okx_segment"] for r in rows),
            "apparent_ge10bps_with_fresh_source_target_quotes":sum(
                r["horizons"][name]["apparent_ge10bps_with_fresh_source_target_quotes"] for r in rows),
            "apparent_ge10bps_with_fresh_source_target_quotes_but_no_basis":sum(
                r["horizons"][name]["apparent_ge10bps_with_fresh_source_target_quotes_but_no_basis"] for r in rows),
            "apparent_ge10bps_excluded_by_freshness_or_basis":sum(
                r["horizons"][name]["apparent_ge10bps_excluded_by_freshness_or_basis"] for r in rows),
            "apparent_ge10bps_examples":[e for r in rows for e in r["horizons"][name]["apparent_ge10bps_first_examples"]][:12],
            "thresholds":part
        }
    out["total_binance_changed_price_event_batches"]=sum(r["binance_price_change_events"] for r in rows)
    out["valid_raw_event_batches"]=sum(r["raw_valid_events"] for r in rows)
    out["valid_delayed_event_batches"]=sum(r["visible_valid_events"] for r in rows)
    for key in ("raw_spread_at_least_10bps","raw_G_at_least_10bps",
                "visible_G_at_least_10bps","raw_G_ge10_then_below10_after_delay",
                "target_changes_during_binance_2p5ms_delay"):
        out[key]=sum(r[key] for r in rows)
    out["raw_spread_absolute_max_bps"]=max(r["raw_spread_absolute_bps"].get("max",0) for r in rows)
    out["raw_G_absolute_max_bps"]=max(r["raw_G_absolute_bps"].get("max",0) for r in rows)
    out["visible_G_absolute_max_bps"]=max(r["visible_G_absolute_bps"].get("max",0) for r in rows)
    out["single_source_event_price_change_abs_max_bps"]=max(
        r["source_price_change_abs_bps"].get("max",0) for r in rows)
    return out


def markdown(s):
    z=["# Binance SOL先变动10bps而OKX价格未变：逐条原生事件审计",
       "",
       "**来源：** Tardis Binance SOLUSDT 与 OKX SOL-USDT-SWAP 原生BBO，"
       "2026年6—11月每月1日UTC00:10—01:00。"
       "使用本地接收时间，Binance模拟额外可见延迟2.5ms+1ns；"
       "OKX只要求最近原生报价≤250ms，历史60秒跨所基差有效。",
       "",
       "**定义：** 对每个Binance实际中间价发生变化的、且该微秒的**最后一条**"
       "行情事件，比较Binance过去50/100/500/1000ms的自身价格变动，"
       "要求该完整窗口OKX原生BBO的**中间价一次都没变**，"
       "并要求窗口起点与事件当时的两所报价新鲜。"
       "阈值1/2/5/10bps：1bps=0.01%，10bps=0.1%。"
       "对同一OKX无价格变动区间报告首次符合条件的事件数，避免重复消息膨胀。",
       "",
       "## 逐事件统计汇总",
       "",
       f"Binance原生**价格变动的不同时间戳事件批次**："
       f"{s['total_binance_changed_price_event_batches']:,}；"
       f"其中同时满足历史基差和当时OKX原生BBO新鲜度："
       f"{s['valid_raw_event_batches']:,}。"
       f"额外2.5ms后仍有效：{s['valid_delayed_event_batches']:,}。",
       "",
       "| 观察Binance自身变动窗口 | 该窗口Binance最大自身涨跌(bps) | OKX原生中间价完全不变时Binance最大涨跌(bps) | OKX不动且Binance涨跌≥10bps的事件数 | OKX不动且涨跌≥10bps的独立稳定区间数 |",
       "|---|---:|---:|---:|---:|"]
    for horizon,a in s["horizons"].items():
        t=a["thresholds"]["10bps"]
        z.append(f"| {horizon} | {a['binance_horizon_price_abs_max_bps']:.4f} "
                 f"| {a['binance_horizon_price_abs_max_when_okx_unchanged_bps']:.4f} "
                 f"| **{t['raw_binance_price_change_events_with_unmoved_okx']}** "
                 f"| {t['first_event_per_okx_price_stable_interval']} |")
    z+=["","## 不施加250ms有效性限制时的表面10bps领先信号（不能直接交易）", "",
        "下表是第二层检查：允许目标交易所原生BBO已过时，仅要求"
        "读到过一个OKX价位且区间内原生记录没有价格变化。"
        "必须与上文满足250ms双市场报价新鲜度及历史基差有效的结果区分。",
        "",
        "| Binance过去变动窗口 | 仅表面未动的10bps事件条数 | 对应的不同OKX稳定价区间数 | 双所起止BBO都新鲜的候选事件 | 因250ms/基差约束不满足而被排除的事件 |",
        "|---|---:|---:|---:|---:|"]
    for h,a in s["horizons"].items():
        z.append(f"| {h} | {a['apparent_unfiltered_ge10bps']} "
                 f"| {a['apparent_unfiltered_ge10bps_first_per_okx_segment']} "
                 f"| {a['apparent_ge10bps_with_fresh_source_target_quotes']} "
                 f"| {a['apparent_ge10bps_excluded_by_freshness_or_basis']} |")
    z+=["","## 不同强度的Binance领先、OKX未变事件","","| 过去窗口 | ≥1bps | ≥2bps | ≥5bps | ≥10bps |",
        "|---|---:|---:|---:|---:|"]
    for horizon,a in s["horizons"].items():
        z.append(f"| {horizon} | "+ " | ".join(
                 str(a["thresholds"][thr]["first_event_per_okx_price_stable_interval"])
                 for thr in THRESHOLDS)+" |")
    z+=["",
        "## 同时刻跨所原始价差/残差，以及2.5ms延迟损失",
        "",
        f"- 原始Tardis接收时刻，所有有效Binance报价变更事件中："
        f"原始价差绝对值最大{s['raw_spread_absolute_max_bps']:.4f}bps，"
        f"减60秒基差的异常G最大{s['raw_G_absolute_max_bps']:.4f}bps。",
        f"- **2.5ms后可见时刻**，对应策略使用的原生BBO近似G最大"
        f"{s['visible_G_absolute_max_bps']:.4f}bps。",
        f"- 原始事件中原始跨所价差≥10bps的事件数："
        f"{s['raw_spread_at_least_10bps']}；去均值G≥10bps的事件数："
        f"{s['raw_G_at_least_10bps']}；经过2.5ms后仍G≥10bps的事件数："
        f"{s['visible_G_at_least_10bps']}。",
        f"- 原始G≥10bps但2.5ms后降到10bps以下的同一消息数："
        f"{s['raw_G_ge10_then_below10_after_delay']}。"
        "并非所有这类减弱都由OKX跟随造成，60秒基差时间网格在边界也可能更新。",
        "",
        "### 领先机会在模拟延迟下是否存续（首个每区间事件）",
        "",
        "| Binance回看窗口 | 事件级别阈值 | 首次领先机会 | 2.5ms后OKX仍没改价且两所报价可用 | 2.5ms+1ms后OKX仍未改价次数 |",
        "|---|---|---:|---:|---:|"]
    for h in HORIZONS:
        for k in ("2bps","5bps","10bps"):
            z0=s["horizons"][h]["thresholds"][k]
            z.append(f"| {h} | {k} | {z0['first_event_per_okx_price_stable_interval']} "
                     f"| {z0['okx_still_unchanged_after_2p5ms']} "
                     f"| {z0['first_event_survival_counts']['3.5ms']} |")
    z+=["",
        "## 因果与执行限制",
        "",
        "1. 本统计按Tardis两个交易所的**本地接收时间**排序。跨所真实交易所撮合时间、"
        "数据采集服务器时钟同步和链路延迟均需额外核验。观察到的领先仅限于该数据时间轴。",
        "2. Binance信号在策略中只能从本地接收时刻后2.5ms开始使用；"
        "OKX撤单最早在发送请求后另经HBT假设的1ms请求延迟生效，"
        "订单回报模型另有1ms。策略还受同侧操作至少20ms间隔约束。"
        "故2.5ms+1ms只是没有遇到限频、订单允许撤单时的理想下限，"
        "不能视为每次都会取消成功。",
        "3. 数据诊断比较的是**原生OKX BBO中间价**；HBT实际报价中心用引擎当前盘口"
        "（L2/trades/BBO融合数据），它与原生BBO在每个时刻不一定相同。"
        "这是**市场级领先证据**，不是“已成功撤单并规避成交”的证明。",
        "4. 不预设Binance每次先跳10bps。小于10bps的领先事件仍可影响做市；"
        "按当前OKX-SOL tick=0.01USDT/SOL，也要比较整数tick取整后挂单是否改变。",
        "5. 为避免重复回报，针对相同微秒的Binance BBO事件仅取**最后一条消息**，"
        "与主策略在同一时刻连续消费所有Binance记录的行为一致。",
    ]
    return "\n".join(z)+"\n"


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--source",default="gathered")
    p.add_argument("--out",default="reports/sol_lead_aggregated")
    args=p.parse_args()
    days=[json.loads((Path(args.source)/f"{d}.json").read_text(encoding="utf-8"))
          for d in DAYS]
    z=summarize(days)
    out=Path(args.out);out.mkdir(parents=True,exist_ok=True)
    (out/"summary.json").write_text(json.dumps(z,ensure_ascii=False,indent=2)+"\n",
                                    encoding="utf-8")
    (out/"report.md").write_text(markdown(z),encoding="utf-8")
    print("LEAD_SUMMARY","BINANCE_PRICE_EVENT",z["total_binance_changed_price_event_batches"],
          "RAW_VALID",z["valid_raw_event_batches"],
          "RAW_SPREAD_GE10",z["raw_spread_at_least_10bps"],
          "RAW_G_GE10",z["raw_G_at_least_10bps"],
          "VISIBLE_G_GE10",z["visible_G_at_least_10bps"],
          "RAW_G_MAX",z["raw_G_absolute_max_bps"],
          "VISIBLE_G_MAX",z["visible_G_absolute_max_bps"],
          flush=True)
    for h,q in z["horizons"].items():
        print("LEAD_HORIZON",h,
              "BIN_MAX",q["binance_horizon_price_abs_max_bps"],
              "UNMOVED_OKX_MAX",q["binance_horizon_price_abs_max_when_okx_unchanged_bps"],
              "APPARENT_UNFILTERED10",q["apparent_unfiltered_ge10bps"],
              "APPARENT_FRESH10",q["apparent_ge10bps_with_fresh_source_target_quotes"],
              "THRESHOLD_COUNTS",
              json.dumps({t:q["thresholds"][t]["first_event_per_okx_price_stable_interval"]
                          for t in THRESHOLDS},sort_keys=True),
              flush=True)


if __name__=="__main__":
    main()
