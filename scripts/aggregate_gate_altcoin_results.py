"""Aggregate actual completed Gate SOL/DOGE paired HBT days; never fill missing with zero."""
import argparse
import json
from collections import defaultdict
from pathlib import Path

SYMBOLS=("SOL_USDT","DOGE_USDT")
DAYS=tuple(f"2026-{month:02d}-01" for month in range(6,12))
MODELS=("glft","both_1p0","defensive_1p0","risk_side_pause")

def summarize(root):
    found = {}
    for path in Path(root).rglob("*_1430m.json"):
        data=json.loads(path.read_text(encoding="utf-8"))
        key=(data.get("gate_symbol"),data.get("day"))
        if key[0] not in SYMBOLS or key[1] not in DAYS:
            continue
        if key in found:
            raise ValueError(f"Duplicate full-day result: {key}")
        found[key]=(path,data)
    rows=[]
    for symbol in SYMBOLS:
        for day in DAYS:
            pair=(symbol,day)
            if pair not in found:
                rows.append({"symbol":symbol,"day":day,"status":"missing_full_day"})
                continue
            path,d=found[pair]
            row={"symbol":symbol,"day":day,"status":d.get("status"),
                 "file":str(path)}
            if d.get("status")!="complete":
                row["error"]=d.get("error")
            else:
                assert d.get("minutes_traded")==1430
                assert d.get("binance_extra_visible_delay_ms")==2.5
                assert abs(d["scenario"]["maker_fee_rate"]-0.0002)<1e-12
                assert all(name in d["variants"] for name in MODELS)
                models={}
                for name in MODELS:
                    m=d["variants"][name]
                    trade=m["trading"]
                    quality=m["quote_quality"]["groups"]["all"]["horizons"]
                    def markout(h):
                        r=quality[str(h)]
                        return (r["fresh"]["mean_bps"] if r["fresh"] else None)
                    models[name]={"net_usdt":trade["end_net_marked_equity_usdt"],
                                  "fee_usdt":trade["fee_usdt"],
                                  "fills":trade["engine_fills"],
                                  "trading_value_usdt":trade["trading_value_usdt"],
                                  "inventory_rms_contracts":trade["rms_inventory_contracts"],
                                  "markout_100ms_bps":markout(100),
                                  "markout_500ms_bps":markout(500),
                                  "markout_1000ms_bps":markout(1000),
                                  "fresh_100ms_coverage":quality["100"]["fresh_fraction"]}
                row["models"]=models
                row["delta_both_vs_glft_net_usdt"]=(
                    models["both_1p0"]["net_usdt"]-models["glft"]["net_usdt"])
                row["delta_pause_vs_glft_net_usdt"]=(
                    models["risk_side_pause"]["net_usdt"]-models["glft"]["net_usdt"])
                row["tick_relative_bps"]=d["scenario"]["tick_relative_bps"]
                row["qty"]=d["scenario"]["qty"]
                row["ctval"]=d["scenario"]["ctval"]
            rows.append(row)
    return rows

def write_summary(rows,out):
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    (out/"per_day.json").write_text(json.dumps(rows,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    lines=["# Gate SOL/DOGE 六个历史月首日跨所回测","",
           "统一设定：Binance 额外2.5ms、订单往返各1ms、同侧20ms、Maker +0.02%；",
           "每日期前10min预热、后1430min回测；严格只对完成的日期计算日均，失败/缺失记NA。","",
           "| 品种 | 日期 | 状态 | GLFT净收益 USDT | 双边β1净收益 USDT | 单侧暂停净收益 USDT | 双边相对改善 USDT |",
           "|---|---|---|---:|---:|---:|---:|"]
    for row in rows:
        m=row.get("models")
        if m:
            lines.append(f'| {row["symbol"]} | {row["day"]} | complete | '
                         f'{m["glft"]["net_usdt"]:.3f} | {m["both_1p0"]["net_usdt"]:.3f} | '
                         f'{m["risk_side_pause"]["net_usdt"]:.3f} | {row["delta_both_vs_glft_net_usdt"]:+.3f} |')
        else:
            lines.append(f'| {row["symbol"]} | {row["day"]} | {row["status"]} | NA | NA | NA | NA |')
    for symbol in SYMBOLS:
        valid=[x for x in rows if x["symbol"]==symbol and x["status"]=="complete"]
        lines+=["",f"## {symbol}: 完成 {len(valid)}/6 天"]
        if valid:
            for model in MODELS:
                avg=lambda k:sum(v["models"][model][k] for v in valid)/len(valid)
                values=[v["models"][model]["markout_100ms_bps"] for v in valid]
                mo=sum(values)/len(values) if None not in values else None
                lines.append(f'- {model}: 日均含费净收益 {avg("net_usdt"):.3f} USDT，'
                             f'日均手续费 {avg("fee_usdt"):.3f} USDT，'
                             f'日均成交 {avg("fills"):.1f} 笔，'
                             f'100ms成交后Markout {mo:.3f}bps' if mo is not None
                             else f'- {model}: 有效成交Markout缺失')
        lines+=["","样本已用于研发探索；不构成独立样本外或统计显著性结论。"]
    (out/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--root",required=True)
    p.add_argument("--out",required=True)
    a=p.parse_args()
    rows=summarize(a.root)
    write_summary(rows,a.out)
    print("GATE_ALTCOIN_AGGREGATE",len([r for r in rows if r["status"]=="complete"]),"/12",flush=True)

if __name__=="__main__":
    main()
