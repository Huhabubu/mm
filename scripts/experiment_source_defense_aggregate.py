"""Aggregate independent 2026-06-01 official HBT sparse defense pilot outputs.

All metrics are descriptive on a previously explored date. The 30s-block
bootstrap CI is within-day sensitivity, not cross-date statistical evidence.
"""
import argparse
import json
from pathlib import Path

import numpy as np

from experiment_source_defense_pilot import classify_opportunities

ALPHA=.05
BOOT=1000
BLOCK_NS=30_000_000_000
PILOT_NAMES=("glft","beta1","sparse_1s_widen","sparse_1s_cancel",
             "sparse_1s_local_placebo","sparse_2s_widen","sparse_2s_cancel")


def block_ci(baseline, variant, h, seed=20261114):
    drop,keep=classify_opportunities(baseline,variant)
    fresh=baseline[f"fresh_{h}ms"].astype(bool)
    n=len(fresh)
    if not n or not np.any(drop&fresh) or not np.any(keep&fresh):
        return {"lower":None,"upper":None,"n_blocks":0,"nonempty_draws":0}
    block=(baseline["exchange_timestamp_ns"]-
           np.min(baseline["exchange_timestamp_ns"]))//BLOCK_NS
    uniq, inverse=np.unique(block,return_inverse=True)
    marks=baseline[f"markout_{h}ms_bps"]
    good=(fresh&np.isfinite(marks))
    kd=(keep&good).astype(np.int64)
    dd=(drop&good).astype(np.int64)
    ns=len(uniq)
    counts_k=np.bincount(inverse,weights=kd,minlength=ns)
    counts_d=np.bincount(inverse,weights=dd,minlength=ns)
    sums_k=np.bincount(inverse,weights=np.where(kd,marks,0),minlength=ns)
    sums_d=np.bincount(inverse,weights=np.where(dd,marks,0),minlength=ns)
    rng=np.random.default_rng(seed)
    idx=rng.integers(0,ns,size=(BOOT,ns),endpoint=False)
    ck=counts_k[idx].sum(axis=1)
    cd=counts_d[idx].sum(axis=1)
    usable=(ck>0)&(cd>0)
    values=sums_k[idx].sum(axis=1)[usable]/ck[usable]-(
           sums_d[idx].sum(axis=1)[usable]/cd[usable])
    return {"lower":float(np.quantile(values,.025)) if len(values)>200 else None,
            "upper":float(np.quantile(values,.975)) if len(values)>200 else None,
            "n_blocks":ns,"nonempty_draws":len(values),
            "assumption":"30s within-one-day block bootstrap; NOT independent-day inference"}


def load_npz(path):
    with np.load(path) as z:return {k:z[k].copy() for k in z.files}


def sum_case(d,root):
    source=d["variants"]["glft"]["trading"]
    gross0=source["end_net_marked_equity_usdt"]+source["fee_usdt"]
    t0=source["trading_value_usdt"]
    if t0<=0:raise ValueError("Nonpositive baseline turnover")
    rows={}
    b=load_npz(root/"glft_fill_diagnostics.npz")
    for name in PILOT_NAMES:
        z=d["variants"][name]
        tr=z["trading"]
        gross=tr["end_net_marked_equity_usdt"]+tr["fee_usdt"]
        ratio=tr["trading_value_usdt"]/t0
        vol=gross0*(ratio-1)
        residual=gross-gross0*ratio
        fee_saved=source["fee_usdt"]-tr["fee_usdt"]
        net_delta=tr["end_net_marked_equity_usdt"]-source["end_net_marked_equity_usdt"]
        if abs(vol+residual+fee_saved-net_delta)>1e-7:
            raise AssertionError("Accounting bridge failed")
        row={"net_usdt":tr["end_net_marked_equity_usdt"],
             "net_delta_vs_glft_usdt":net_delta,
             "fees_usdt":tr["fee_usdt"],
             "fills":tr["engine_fills"],
             "cancels":tr["cancel_requests"],
             "fill_retention_vs_glft":tr["engine_fills"]/source["engine_fills"],
             "fee_scenarios":z["fee_scenarios_net_marked_usdt"],
             "exact_accounting_bridge":{
                 "fee_saved_usdt":fee_saved,
                 "turnover_ratio":ratio,
                 "volume_scaled_gross_change_usdt":vol,
                 "gross_residual_after_turnover_scale_usdt":residual,
                 "warning":"Residual includes inventory/quote/queue effects, not causal selection alpha"},
             "horizons":{}}
        if name!="glft":
            compare=z["relative_to_glft"]["selectivity"]
            t=load_npz(root/f"{name}_fill_diagnostics.npz")
            for h in (100,500,1000,5000):
                dx=compare["by_horizon"][f"{h}ms"]
                row["horizons"][f"{h}ms"]={
                    "dropped":dx["base_only"],
                    "kept":dx["base_kept"],
                    "selectivity_lift_bps":dx["selectivity_lift_kept_minus_dropped_bps"],
                    "within_day_30s_block_bootstrap":block_ci(b,t,h),
                }
            row["toxic_recall"]=compare["toxic_recall_descriptive"]
            row["safe_sacrifice"]=compare["safe_sacrifice_descriptive"]
            row["redlines"]=z["pilot_acceptance_descriptive_only"]
        rows[name]=row
    return rows


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--source",required=True)
    p.add_argument("--out",required=True)
    args=p.parse_args()
    cases={}
    for file in Path(args.source).rglob("summary.json"):
        d=json.loads(file.read_text(encoding="utf-8"))
        if not isinstance(d,dict) or "variants" not in d:continue
        cases[d["coin"]]=sum_case(d,file.parent)
    if set(cases)!={"BTC","SOL"}:
        raise RuntimeError(f"Missing BTC/SOL pilot: {list(cases)}")
    lines=["# 单日门控试验：官方HBT扣费收益与选择性证据",
           "", "基于2026-06-01此前已探索50分钟。所有净收益含固定2bps maker费率。",
           "价差分解、零费率/返佣情景仅是机制和成本敏感性，不可当作正式盈利证明。",
           "成交同刻匹配不是订单ID因果撤单。Bootstrap仅30秒区块内的单日抽样，不能推断独立日显著性。",
           "", "|币种|策略|撤单|成交|保留率|净收益(USDT)|100ms Lift(bps)|500ms Lift(bps)|30s区块Bootstrap 500ms CI|",
           "|---|---|---:|---:|---:|---:|---:|---:|---|"]
    for coin in ("BTC","SOL"):
        for name,row in cases[coin].items():
            def fmt(v):
                return f"{v:+.4f}" if v is not None else "N/A"
            x=row["horizons"].get("100ms",{}).get("selectivity_lift_bps")
            y=row["horizons"].get("500ms",{}).get("selectivity_lift_bps")
            ci=row["horizons"].get("500ms",{}).get("within_day_30s_block_bootstrap",{})
            interval=('[%s,%s]'%(fmt(ci.get('lower')),fmt(ci.get('upper')))
                      if ci else 'N/A')
            lines.append(f"|{coin}|{name}|{row['cancels']:,}|{row['fills']:,}|"
                         f"{row['fill_retention_vs_glft']:.1%}|{row['net_usdt']:+.4f}|"
                         f"{fmt(x)}|{fmt(y)}|{interval}|")
    lines+=["", "## 规则和结论边界","",
            "- 撤单3500—5500、成交保留75%—95%以及Lift>0是该单日研究的描述性检查，"
            "严格样本外与逐独立日检验仍需开展。未过红线不扩所有日期。",
            "- P5、P10与markout<-2bps的比例、Toxic Recall/Safe Sacrifice及"
            "每笔Spread Capture/Adverse Selection Cost/Gross Edge均见summary.json。",
            "- 另存精确恒等式：净改善＝手续费减少＋按实际成交名义金额同比缩放的"
            "基准毛收益变化＋剩余毛收益差。**最后一项包含库存与执行效应，不得称为"
            "因果Selection Alpha**。",
            "- 同窗OKX-Triggered placebo保留了单侧结构，"
            "可比较方向事件来源而不增加人为刷新频率差异。"]
    report="\n".join(lines)+"\n"
    out=Path(args.out);out.mkdir(parents=True,exist_ok=True)
    (out/"summary.json").write_text(json.dumps(cases,indent=2,ensure_ascii=False),encoding="utf-8")
    (out/"report.md").write_text(report,encoding="utf-8")
    print(report)


if __name__=="__main__":
    main()
