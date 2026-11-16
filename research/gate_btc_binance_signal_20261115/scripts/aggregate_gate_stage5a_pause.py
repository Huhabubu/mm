"""Stage5A six-day, day-equal frozen four-model official HBT comparison."""
import argparse
import csv
import json
from pathlib import Path
from statistics import mean

from aggregate_gate_stage4_controls import read_stage2

DAYS=tuple(f"2026-{m:02d}-01" for m in range(6,12))
NAMES=("glft","both_1p0","defensive_1p0","risk_side_pause")
HORIZONS=(100,500,1000)


def summarize(root,stage2_file,out_dir):
    q=read_stage2(stage2_file)
    rows=[]
    all_days={}
    for day in DAYS:
        matches=list(Path(root).rglob(day+"_stage5a.json"))
        if len(matches)!=1:
            raise ValueError("Missing/nonunique stage5a day: "+day)
        z=json.loads(matches[0].read_text(encoding="utf-8"))
        if (z["status"]!="complete" or z["day"]!=day
            or not z["source_data_qa_same_as_frozen"]
            or not z["defensive_1p0_replay_exact_frozen_accounting"]
            or z["fee_maker_rate"]!=0.0002
            or z["binance_extra_visible_latency_ms"]!=2.5):
            raise ValueError("5A protocol/QA failed: "+day)
        all_days[day]={}
        for name in NAMES:
            if name=="risk_side_pause":
                mod=z["risk_side_pause"]
            elif name=="defensive_1p0":
                mod=z["rerun_defensive"]
            else:
                mod=z["frozen_official"][name]
            t=mod["trading"]
            r={
                "date":day,"model":name,"fills":int(t["engine_fills"]),
                "fee_net_pnl_usdt":float(t["end_net_marked_equity_usdt"]),
                "maker_fee_usdt":float(t["fee_usdt"]),
                "notional_usdt":float(t["trading_value_usdt"]),
                "mean_abs_inventory":float(t["mean_abs_inventory_contracts"]),
                "rms_inventory":float(t["rms_inventory_contracts"]),
                "max_abs_inventory":float(t["max_abs_position_contracts"]),
                "cancel_requests":int(t["cancel_requests"]),
                "submissions":int(t["submissions"]),
            }
            r["diagnostic_fee_free_pnl_usdt"]=r["fee_net_pnl_usdt"]+r["maker_fee_usdt"]
            if r["maker_fee_usdt"]<0 or r["notional_usdt"]<=0:
                raise ValueError("Wrong fee/volume")
            for h in HORIZONS:
                if name in ("glft","both_1p0"):
                    k=q[(day,name,"all",h)]
                    capture=float(k["capture_bps"])
                    markout=float(k["mean_markout_bps"])
                    worst=float(k["worst10_mean_bps"])
                    coverage=float(k["coverage"])
                    n=int(float(k["fresh_fills"]))
                else:
                    hh=mod["quality"]["groups"]["all"]["horizons"][str(h)]
                    capture=float(hh["capture_on_same_fresh_fills"]["mean_bps"])
                    markout=float(hh["fresh"]["mean_bps"])
                    worst=float(hh["fresh"]["worst10_mean_bps"])
                    coverage=float(hh["fresh_fraction"])
                    n=int(hh["fresh_count"])
                r[f"{h}ms_capture_bps"]=capture
                r[f"{h}ms_markout_bps"]=markout
                r[f"{h}ms_worst10_bps"]=worst
                r[f"{h}ms_quote_coverage"]=coverage
                r[f"{h}ms_fresh_n"]=n
            rows.append(r)
            all_days[day][name]=r
    out_dir=Path(out_dir)
    out_dir.mkdir(parents=True,exist_ok=True)
    with (out_dir/"per_day.csv").open("w",newline="",encoding="utf-8") as fd:
        wr=csv.DictWriter(fd,fieldnames=list(rows[0]))
        wr.writeheader()
        wr.writerows(rows)
    metrics=[k for k in rows[0] if k not in ("date","model")]
    avg={name:{m:mean(all_days[d][name][m] for d in DAYS) for m in metrics}
         for name in NAMES}
    paired={}
    for competitor in ("glft","both_1p0","defensive_1p0"):
        diffs={d:all_days[d]["risk_side_pause"]["fee_net_pnl_usdt"]-
                 all_days[d][competitor]["fee_net_pnl_usdt"] for d in DAYS}
        paired[competitor]={"daily_fee_net_usdt":diffs,
                 "mean_fee_net_usdt":mean(diffs.values()),
                 "days_pause_better":sum(x>0 for x in diffs.values())}
    payload={"status":"complete","evaluation_days":list(DAYS),"original_frozen":True,
             "new_mode":"only cancels/withholds risk-side original-GLFT quote on effective origin signal",
             "no_gamma_sigma_beta_changes":True,
             "days":all_days,"day_equal_mean":avg,"paired_pause_vs":paired,
             "cautions":["All six dates previously explored","No forced-end flattening or funding",
                         "Existing defensive prices and queues differ from pause",
                         "Same event arrival, but endogenous order reply events differ",
                         "Markout is post-fill diagnostic and not total portfolio PnL"]}
    (out_dir/"summary.json").write_text(
        json.dumps(payload,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    lines=["# 阶段⑤A：Binance方向危险侧单侧暂停，对比已有单侧外移和双边平移","",
           "以下均为六个已探索历史完整日，Maker实际支付0.02%，HBT2.4.4原生撮合，保持固定原信号和GLFT参数。旧GLFT/双边β1读取冻结原报告；defensive_1p0同源重新运行并严格复现冻结账目。","",
           "| 六日等权平均（每1430min） | 原GLFT | 双边β1 | 单侧外移β1 | 单侧暂停β1 |",
           "|---|---:|---:|---:|---:|"]
    nice=[
        ("fee_net_pnl_usdt","手续费后净收益 USDT",3),
        ("fills","成交笔数",1),("notional_usdt","成交额 USDT",1),
        ("maker_fee_usdt","手续费 USDT",3),
        ("mean_abs_inventory","平均绝对库存 张",3),
        ("rms_inventory","库存均方根 张",3),
        ("max_abs_inventory","最大库存张数(逐日最大值再平均)",2),
        ("cancel_requests","撤单请求数",1),("submissions","提交订单数",1),
        ("100ms_capture_bps","100ms有效成交半价差 bps",4),
        ("100ms_markout_bps","100ms Markout bps",4),
        ("100ms_worst10_bps","100ms最差10% Markout bps",4),
        ("500ms_markout_bps","500ms Markout bps",4),
        ("1000ms_markout_bps","1s Markout bps",4),
        ("100ms_quote_coverage","100ms报价有效覆盖率",4)
    ]
    for k,label,n in nice:
        lines.append("| "+label+" | "+" | ".join(
            f"{avg[m][k]:,.{n}f}" for m in NAMES)+" |")
    lines+=["","## 单侧暂停相对各冻结策略（含Maker手续费）"]
    for name in ("glft","both_1p0","defensive_1p0"):
        x=paired[name]
        lines.append(f"- 相对{name}每日平均净收益差 **{x['mean_fee_net_usdt']:+.3f} USDT**，6日胜出 **{x['days_pause_better']}/6**。")
    lines+=["","所有模式的绝对收益须保留负值；没有加入资金费率或期末强制平仓；仅有6个已探索日，不存在真正样本外。新模式的交易所排队与成交由官方HBT执行，不能将少成交归因于具体撤单确认，除非完成独立订单ID/交易所失效时刻审计。"]
    (out_dir/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")
    print("STAGE5A_SIXDAY_COMPLETE",json.dumps(
         {k:{t:avg[k][t] for t in ("fee_net_pnl_usdt","fills","100ms_markout_bps")}
          for k in NAMES},ensure_ascii=False),flush=True)


if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("--daily",required=True)
    ap.add_argument("--stage2",required=True)
    ap.add_argument("--out",required=True)
    a=ap.parse_args()
    summarize(a.daily,a.stage2,a.out)
