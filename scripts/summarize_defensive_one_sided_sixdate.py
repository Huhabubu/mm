"""Repaired GLFT six-date comparison: beta0 vs symmetric beta1 vs defensive beta1.

2.5ms full 4-market comparison and exact-zero OKX BTC/SOL comparison.
All numbers from individual official hftbacktest, each day equal weight.
"""
import argparse
import json
import math
import statistics
from pathlib import Path

DAYS=tuple(f"2026-{m:02d}-01" for m in range(6,12))
CASES=(("2p5","okx","BTC"),("2p5","okx","SOL"),
       ("2p5","bitget","BTC"),("2p5","bitget","SOL"),
       ("zero","okx","BTC"),("zero","okx","SOL"))
MODELS=("glft","glft_binance","glft_binance_defensive",
        "glft_local_peak","glft_binance_local_peak",
        "glft_binance_defensive_local_peak")
GROUPS=(("basic","glft","glft_binance","glft_binance_defensive"),
        ("peak","glft_local_peak","glft_binance_local_peak",
         "glft_binance_defensive_local_peak"))
# Verified previous P0-repaired results, to prove unchanged baseline paths.
REFERENCE_NET={
("2p5","okx","BTC","glft"):-6.133448016666647,
("2p5","okx","BTC","glft_binance"):-3.3002848596666916,
("2p5","okx","SOL","glft"):-3.7528545600000083,
("2p5","okx","SOL","glft_binance"):-2.976751599999969,
("2p5","bitget","BTC","glft"):-3.0888049226666245,
("2p5","bitget","BTC","glft_binance"):-2.3825696796666698,
("2p5","bitget","SOL","glft"):-7.605588989999936,
("2p5","bitget","SOL","glft_binance"):-7.43353081666657,
("zero","okx","BTC","glft"):-6.189969,
("zero","okx","BTC","glft_binance"):-3.166037,
("zero","okx","SOL","glft"):-3.799488,
("zero","okx","SOL","glft_binance"):-2.828154
}


def read_case(path,profile,venue,coin,day):
    r=json.loads(path.read_text(encoding="utf-8"))
    assert r["status"] in ("complete","no_glft_calibration"),(path,r["status"])
    assert r["day"]==day and r["minutes"]==50 and r["warmup_minutes"]==10
    assert r["market"]["coin"]==coin
    assert r["market"]["exchange"]==("okex-swap" if venue=="okx" else "bitget-futures")
    assert r["extra_binance_delay_ms"]==(2.5 if profile=="2p5" else 0.0)
    assert r["order_request_latency_ns"]==(1_000_000 if profile=="2p5" else 0)
    assert r["order_response_latency_ns"]==(1_000_000 if profile=="2p5" else 0)
    assert r["same_side_min_action_interval_ns"]==20_000_000
    assert r["defensive_alpha"]==0.0 and r["defensive_beta"]==1.0
    assert r["market"]["maker_fee"]==.0002
    assert r["market"]["taker_fee"]==(.0005 if venue=="okx" else .0006)
    if r["status"]=="complete":
        assert set(r["variants"])==set(MODELS)
        cal=r["variants"]["glft"]["calibration_trace"]
        for m in MODELS:
            item=r["variants"][m]
            assert item["calibration_trace"]==cal
            assert item["time_count"]["binance"]==r["variants"]["glft"]["time_count"]["binance"]
            assert item["recorder"]["samples"]>=29000
            assert item["trading"]["fee_usdt"]>=0
    return r


def metrics(r,m):
    x=r["variants"][m]["trading"]
    net=float(x["end_net_marked_equity_usdt"])
    fee=float(x["fee_usdt"])
    return {
        "net":net,"fees":fee,"gross":net+fee,
        "fills":int(x["engine_fills"]),
        "cancels":int(x["cancel_requests"]),
        "submissions":int(x["submissions"]),
        "mean_abs_inventory":float(x["mean_abs_inventory_contracts"]),
        "rms_inventory":float(x["rms_inventory_contracts"]),
        "max_drawdown":float(x["max_drawdown_usdt"])
    }


def aggregate(root):
    cases={}
    missing=[]
    for profile,venue,coin in CASES:
        for day in DAYS:
            path=Path(root)/"cases"/profile/venue/coin/(day+".json")
            if not path.exists():raise FileNotFoundError(str(path))
            r=read_case(path,profile,venue,coin,day)
            cases[(profile,venue,coin,day)]=r
            if r["status"]!="complete":
                missing.append({"profile":profile,"venue":venue,"coin":coin,
                                "day":day,"status":r["status"]})
    assert len(cases)==36
    result={"total_cases":len(cases),"missing":missing,"by_market":[],"by_day":[],"effects":[]}
    for profile,venue,coin in CASES:
        valid=[(day,cases[(profile,venue,coin,day)]) for day in DAYS
               if cases[(profile,venue,coin,day)]["status"]=="complete"]
        assert len(valid)==(2 if venue=="okx" and coin=="SOL" else 6)
        mean=lambda vals: float(statistics.mean(vals))
        for model in MODELS:
            vals=[metrics(r,model) for _,r in valid]
            z={"profile":profile,"venue":venue,"coin":coin,"model":model,
               "valid_days":len(valid)}
            z.update({k:mean([v[k] for v in vals]) for k in vals[0]})
            result["by_market"].append(z)
            key=(profile,venue,coin,model)
            if key in REFERENCE_NET:
                if not math.isclose(z["net"],REFERENCE_NET[key],abs_tol=1e-5):
                    raise RuntimeError(f"Old P0-repaired baseline drifted: {key} {z['net']} != {REFERENCE_NET[key]}")
        for day,r in valid:
            for label,base,sym,defense in GROUPS:
                row={"profile":profile,"venue":venue,"coin":coin,
                     "day":day,"group":label}
                for key,m in (("glft",base),("symmetric",sym),("defensive",defense)):
                    row[key]=metrics(r,m)
                for name,m in (("defensive_minus_glft",base),
                               ("defensive_minus_symmetric",sym)):
                    row[name+"_net"]=row["defensive"]["net"]-metrics(r,m)["net"]
                    row[name+"_gross"]=row["defensive"]["gross"]-metrics(r,m)["gross"]
                    row[name+"_fees"]=row["defensive"]["fees"]-metrics(r,m)["fees"]
                result["by_day"].append(row)
        for label,base,sym,defense in GROUPS:
            sub=[r for r in result["by_day"] if
                 r["profile"]==profile and r["venue"]==venue and r["coin"]==coin and r["group"]==label]
            assert len(sub)==len(valid)
            z={"profile":profile,"venue":venue,"coin":coin,"group":label,
               "valid_days":len(valid),
               "defensive_better_than_sym_days":sum(
                   r["defensive_minus_symmetric_net"]>0 for r in sub),
               "defensive_better_than_glft_days":sum(
                   r["defensive_minus_glft_net"]>0 for r in sub)}
            for measure in ("net","gross","fees","fills","cancels","submissions",
                            "mean_abs_inventory","rms_inventory"):
                for variant in ("glft","symmetric","defensive"):
                    z[variant+"_"+measure]=mean([r[variant][measure] for r in sub])
            result["effects"].append(z)
    return result


def report_markdown(d):
    rows=d["effects"]
    z=[
        "# 方案B：Binance异常价差指导 GLFT 单边防御报价（α=0、β=1）",
        "",
        "当前GLFT修复后同一份目标盘口中间价，2.5ms Binance额外可见延迟、"
        "1ms请求、1ms回报、每侧20ms操作间隔，四市场；另外OKX BTC/SOL严格全零延迟。"
        "成交、排队、费用全部使用官方hftbacktest==2.4.4。"
        "对每个市场/日期独立回测四原模型+两单边方案，无额外阈值、无报价参数调优。",
        "",
        "**方案B规则：** G>0只升高卖单报价，买单保持GLFT；G<0只降低买单报价，"
        "卖单保持GLFT。GLFT库存项、风险厌恶、半价差不变，先取α=0不顺势追价。"
        "B与对称平移版均使用同一G和同一事件驱动触发；过期信号变0。",
        "",
        "## 每个市场主模型：含手续费净收益，USDT/有效日50min",
        "",
        "| 目标市场 | 延迟情景 | 有效天数 | 原GLFT | 原对称Binance | 单边防御B | B−对称 | B−GLFT | B优于对称的日期 |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for x in rows:
        if x["group"]!="basic":continue
        z.append(f"| {x['venue']} {x['coin']} | {x['profile']} | {x['valid_days']} "
                 f"| {x['glft_net']:+.6f} | {x['symmetric_net']:+.6f} "
                 f"| **{x['defensive_net']:+.6f}** "
                 f"| {x['defensive_net']-x['symmetric_net']:+.6f} "
                 f"| {x['defensive_net']-x['glft_net']:+.6f} "
                 f"| {x['defensive_better_than_sym_days']}/{x['valid_days']} |")
    z+=["",
        "## 主模型成交与手续费（每个有效日期50min均值）","",
        "| 目标市场 | 延迟情景 | 原GLFT成交 | 对称成交 | 防御成交 | 对称手续费 | 防御手续费 | 对称毛收益 | 防御毛收益 |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|"]
    for x in rows:
        if x["group"]!="basic":continue
        z.append(f"| {x['venue']} {x['coin']} | {x['profile']} "
                 f"| {x['glft_fills']:.1f} | {x['symmetric_fills']:.1f} "
                 f"| {x['defensive_fills']:.1f} "
                 f"| {x['symmetric_fees']:.6f} | {x['defensive_fees']:.6f} "
                 f"| {x['symmetric_gross']:+.6f} | {x['defensive_gross']:+.6f} |")
    z+=["","## 增加局部峰值锚定的对照","",
        "| 目标市场 | 延迟 | GLFT+峰 | 对称Binance+峰 | 单边防御B+峰 | 防御B−对称 |",
        "|---|---|---:|---:|---:|---:|"]
    for x in rows:
        if x["group"]!="peak":continue
        z.append(f"| {x['venue']} {x['coin']} | {x['profile']} "
                 f"| {x['glft_net']:+.6f} | {x['symmetric_net']:+.6f} "
                 f"| {x['defensive_net']:+.6f} "
                 f"| {x['defensive_net']-x['symmetric_net']:+.6f} |")
    z+=["","## 方法审计","",
        "- 两种Binance版本均使用相同60秒因果历史基差与同一个官方HBT实时目标盘口；"
        "B只改变报价左右两侧的信号应用，不改变参数拟合、价格档、仓位、手续费。",
        "- 报告全部为**扣手续费净收益**，毛收益只用来解释成交数量/费用作用；"
        "订单最终未考虑资金费率和真实平仓滑点。",
        "- 先前修复后GLFT及对称β1的每日平均净收益在每个市场都与已完成的历史回放"
        "通过1e−5USDT精度的一致性检查；不会混入旧版P0报表。",
        "- OKX SOL历史仅两日可估计GLFT；其他四日保持校准失败，不计入收益。",
        "- 六个月首日每段50min是多次探索过的样本，不是全天或样本外。"
        "若B通过减少成交改善净收益，仍需匹配成交量/库存暴露与撤单频率，"
        "才能将改善归因于识别逆向选择。",
        "",
        "## 逐日差异（主模型：防御B−对称，USDT/50min）","",
        "| 日期 | 市场 | 延迟 | B−对称 | B−GLFT |",
        "|---|---|---|---:|---:|"]
    for row in d["by_day"]:
        if row["group"]=="basic":
            z.append(f"| {row['day']} | {row['venue']} {row['coin']} "
                     f"| {row['profile']} | {row['defensive_minus_symmetric_net']:+.6f} "
                     f"| {row['defensive_minus_glft_net']:+.6f} |")
    return "\n".join(z)+"\n"


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--root",default="gathered")
    p.add_argument("--out",default="reports/defensive_sixdate")
    a=p.parse_args()
    d=aggregate(a.root)
    path=Path(a.out);path.mkdir(parents=True,exist_ok=True)
    (path/"summary.json").write_text(json.dumps(d,indent=2,ensure_ascii=False)+"\n",encoding="utf-8")
    (path/"report.md").write_text(report_markdown(d),encoding="utf-8")
    print("DEFENSIVE_CASES",d["total_cases"],"MISSING_GLFT",len(d["missing"]),flush=True)
    for x in d["effects"]:
        print("DEFENSIVE_EFFECT",x["profile"],x["venue"],x["coin"],x["group"],
              "DAYS",x["valid_days"],"BASE",x["glft_net"],
              "SYM",x["symmetric_net"],"DEF",x["defensive_net"],
              "DELTA_DEF_SYM",x["defensive_net"]-x["symmetric_net"],
              "DELTA_DEF_GLFT",x["defensive_net"]-x["glft_net"],
              "FILL_SYM",x["symmetric_fills"],"FILL_DEF",x["defensive_fills"],
              "FEE_SYM",x["symmetric_fees"],"FEE_DEF",x["defensive_fees"],
              "GROSS_SYM",x["symmetric_gross"],"GROSS_DEF",x["defensive_gross"],
              "WIN_DAYS",x["defensive_better_than_sym_days"],flush=True)


if __name__=="__main__":
    main()
