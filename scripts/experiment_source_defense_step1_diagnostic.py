"""Step 1 read-only diagnostics from June-1 official HBT fill artifacts.

No strategy execution, fitting, tuning or source branch mutation. Binance
'active impulse' means a visible Binance BBO midpoint movement, NOT confirmed
aggressive trade flow. Classification at exchange fill time -1ms is an
*as-of pre-fill proxy*, never asserted as the order's original decision time.
"""
import argparse
import json
from pathlib import Path

import numpy as np

from cross_venue_event_probe import read_binance_bbo, visible_timestamps
from check_okx_hft_data import extract_window
from run_beta_one_direct_gap import read_local_bbo
from experiment_source_defense_signal import past_grid_time_ema
from experiment_source_defense_pilot import classify_opportunities, HORIZONS_MS

GAP_BINS = (0., .2, .5, 1., 2., float("inf"))
BIN_LABELS = ("0~0.2", "0.2~0.5", "0.5~1.0", "1.0~2.0", ">2.0")


def load_fills(folder, model):
    with np.load(folder/f"{model}_fill_diagnostics.npz") as z:
        return {k:z[k].copy() for k in z.files}


def fill_group_summary(fills, mask, contract_value):
    sel=np.flatnonzero(mask)
    amounts=fills["exec_qty"][sel]*fills["exec_price"][sel]*contract_value
    result={"fill_count":len(sel),"notional_usdt":float(amounts.sum()),
            "mean_notional_usdt":float(amounts.mean()) if len(sel) else None,
            "by_horizon":{}}
    for h in HORIZONS_MS:
        fresh=mask & fills[f"fresh_{h}ms"].astype(bool)
        mark=fills[f"markout_{h}ms_bps"][fresh]
        notional=(fills["exec_qty"][fresh]*fills["exec_price"][fresh]*
                  contract_value)
        clean=np.isfinite(mark)&np.isfinite(notional)&(notional>0)
        mark=mark[clean];notional=notional[clean]
        result["by_horizon"][str(h)+"ms"]={
            "fresh_count":len(mark),
            "mean_markout_bps":float(mark.mean()) if len(mark) else None,
            "notional_weighted_markout_bps":(
                float(np.average(mark,weights=notional)) if len(mark) else None),
            "markout_dollar_exposure_proxy":(
                float(np.sum(mark*notional/10000.)) if len(mark) else None),
            "p10_markout_bps":float(np.quantile(mark,.1)) if len(mark) else None,
            "p05_markout_bps":float(np.quantile(mark,.05)) if len(mark) else None,
            "fraction_markout_below_minus2bps":(
                float(np.mean(mark < -2)) if len(mark) else None),
            "fresh_notional_usdt":float(notional.sum())}
    return result


def position_valuation_proxy(fills, ot, om, contract_value, report_end_ns):
    """Noncausal descriptive inventory MTM on native BBO, exchange fill clock.

    This is NOT official HBT PnL. Integrate inventory before each BBO mid
    change, taking each fill as effective at its exchange timestamp.
    """
    if not len(ot):raise ValueError("Missing market BBO history")
    ts=np.asarray(fills["exchange_timestamp_ns"],dtype=np.int64)
    side=np.asarray(fills["side_sign"],dtype=np.float64)
    qty=np.asarray(fills["exec_qty"],dtype=np.float64)
    ordered=np.argsort(ts,kind="stable")
    ts,inc=ts[ordered],(side*qty)[ordered]
    end=int(report_end_ns)
    start=int(ts[0]) if len(ts) else int(ot[0])
    from_idx=np.searchsorted(ot,start,side="right")-1
    to_idx=np.searchsorted(ot,end,side="right")-1
    if from_idx<0 or to_idx<=from_idx:
        return {"status":"insufficient_native_bbo_for_holding_proxy"}
    t=ot[from_idx:to_idx+1];m=om[from_idx:to_idx+1]
    # q prior to each mid change, including all exchange executions up to
    # the earlier BBO timestamp. Same-clock events may have unknown order.
    at=np.searchsorted(ts,t[:-1],side="right")
    c=np.r_[0.,np.cumsum(inc)]
    pre_q=c[at]
    changes=np.diff(m)
    pnl=float(np.dot(pre_q,changes)*contract_value)
    gross_price_variation_usdt=float(
        np.dot(np.abs(pre_q),np.abs(changes))*contract_value)
    terminal_q=float(np.sum(inc))
    terminal_marked_inventory_usdt=terminal_q*float(m[-1])*contract_value
    return {"status":"native_bbo_mid_holding_proxy",
            "first_fill_ns":start,"last_available_mid_ns":int(t[-1]),
            "terminal_inventory_contracts":terminal_q,
            "terminal_inventory_signed_usdt":terminal_marked_inventory_usdt,
            "inventory_holding_mtm_drift_usdt_proxy":pnl,
            "abs_position_weighted_abs_mid_changes_usdt":gross_price_variation_usdt,
            "note":"Standalone holding exposure proxy. Different from official HBT fused-mid gross PnL; use official net ledger for total returns."}


def prefill_binance_gap_buckets(g0, ot, om, bt, bm, grid, ema, tick):
    """Classify *G0* original fills using only states visible before fill.

    Binance midprice movement window 100ms, same direction as fast residual
    gap. Native target BBO is substituted for unavailable HBT decision mid.
    """
    fill_ts=np.asarray(g0["exchange_timestamp_ns"],np.int64)
    t=fill_ts-1_000_000 # use strictly pre-exchange fill instant -1ms
    bi=np.searchsorted(bt,t,side="left")-1
    oi=np.searchsorted(ot,t,side="left")-1
    gi=np.searchsorted(grid,t,side="left")-1
    window=t-100_000_000
    wi=np.searchsorted(bt,window,side="right")-1
    bidx=np.maximum(bi,0);oidx=np.maximum(oi,0);gidx=np.maximum(gi,0)
    widx=np.maximum(wi,0)
    valid=(bi>=0)&(oi>=0)&(gi>=0)&(wi>=0)
    valid &= (t-bt[bidx]>=0)&(t-bt[bidx]<=250_000_000)
    valid &= (t-ot[oidx]>=0)&(t-ot[oidx]<=250_000_000)
    valid &= (window-bt[widx]>=0)&(window-bt[widx]<=250_000_000)
    valid &= np.isfinite(ema[gidx])
    target=om[oidx]
    valid &= target>0
    g_bps=1.e4*(bm[bidx]-target-ema[gidx])/np.maximum(target,1.e-12)
    imp_bps=1.e4*(bm[bidx]-bm[widx])/np.maximum(target,1.e-12)
    imp_ticks=(bm[bidx]-bm[widx])/tick
    gate=(valid & (np.sign(g_bps)==np.sign(imp_bps)) &
          (np.abs(imp_ticks)>=1.0-1.e-10)&(np.abs(g_bps)>0))
    labels=np.empty(len(t),dtype=object)
    labels[:]="unavailable"
    labels[valid&~gate]="未触发"
    labels[gate]=">2.0"
    abs_gap=np.abs(g_bps)
    for k,label in enumerate(BIN_LABELS):
        hit=gate & (abs_gap>=GAP_BINS[k]) & (abs_gap<GAP_BINS[k+1])
        labels[hit]=label
    result={"asof_clock":"exchange_fill_timestamp_minus_1ms; NOT original order decision",
            "proxy":"native target BBO, visible delayed Binance mid, strictly prior EMA",
            "binance_impulse_definition":"100ms Binance BBO mid movement >= 1 target tick, sign(gap)==sign(impulse)",
            "buckets":[]}
    order=("unavailable","未触发")+BIN_LABELS
    for label in order:
        selected=(labels==label)
        count=int(selected.sum())
        row={"bin":label,"fills":count,
             "fraction_all_g0_fills":count/len(t) if len(t) else None,
             "mean_abs_gap_bps":float(np.mean(abs_gap[selected])) if count and label!="unavailable" else None,
             "by_horizon":{}}
        for h in (100,500,1000):
            v=selected & g0[f"fresh_{h}ms"].astype(bool)
            marks=g0[f"markout_{h}ms_bps"][v]
            marks=marks[np.isfinite(marks)]
            row["by_horizon"][f"{h}ms"]={
                "fresh_count":len(marks),
                "mean_markout_bps":float(np.mean(marks)) if len(marks) else None,
                "p10_markout_bps":float(np.quantile(marks,.1)) if len(marks) else None}
        result["buckets"].append(row)
    if sum(row["fills"] for row in result["buckets"])!=len(t):
        raise AssertionError("G0 bucket partition not exhaustive")
    return result


def audit_case(folder,coin):
    report=json.loads((folder/"summary.json").read_text(encoding="utf8"))
    variants=report["variants"]
    tick={"BTC":.1,"SOL":.01}[coin]
    ctval=.01 if coin=="BTC" else 1.
    date=report["day"]
    from datetime import datetime,timezone
    end_us=int(datetime.fromisoformat(date).replace(
        tzinfo=timezone.utc).timestamp()*1e6)+3600*1_000_000+5_000_000
    from tempfile import TemporaryDirectory
    with TemporaryDirectory(prefix="g0_prefill_asof_") as tmp:
        path=Path(tmp)/"target_bbo.csv"
        extract_window("book_ticker",date,end_us,path,max_rows=3_000_000,
                       exchange="okex-swap",symbol=f"{coin}-USDT-SWAP")
        ot,om=read_local_bbo(path)
        raw_bt,bm=read_binance_bbo(date,end_us,symbol=f"{coin}USDT")
        bt=visible_timestamps(raw_bt,2.5)
        grid,ema=past_grid_time_ema(ot,om,bt,bm,1.0)
        g0=load_fills(folder,"glft")
        summary={
          "coin":coin,"day":date,
          "g0_buckets":prefill_binance_gap_buckets(g0,ot,om,bt,bm,grid,ema,tick),
          "variant_comparison":{},
          "base_inventory":{},
          "notes":[
              "Analysis uses existing 2tick pilot official fills, no strategy replay",
              "Binance midpoint change is not signed aggressive trade volume",
              "Attribution pre-fill proxy is NOT exact order submission/cancel moment",
              "A matched G0-only/variant-only fill is not proven avoided/executed same order",
              "Native-BBO inventory holding drift differs from fused-target HBT PnL",
              "Residual after turnover scaling is not identifiable causal selection alpha"] }
        g0tr=variants["glft"]["trading"]
        end_ns=int(g0tr.get("end_time_ns",0)) if "end_time_ns" in g0tr else int(ot[-1])
        # Official HBT trading end is the first 60min simulation window end.
        end_ns=min(int(ot[-1]),int(report["variants"]["glft"].get(
            "end_time_ns",ot[-1])))
        base_hold=position_valuation_proxy(g0,ot,om,ctval,end_ns)
        summary["base_inventory"]={
            "official_terminal_contracts":g0tr["end_position_contracts"],
            "official_mean_abs_contracts":g0tr["mean_abs_inventory_contracts"],
            "official_rms_contracts":g0tr["rms_inventory_contracts"],
            "official_max_drawdown_usdt":g0tr["max_drawdown_usdt"],
            "native_bbo_holding_proxy":base_hold}
        for name,z in variants.items():
            if name=="glft":continue
            fills=load_fills(folder,name)
            base_only,base_shared=classify_opportunities(g0,fills)
            added,variant_shared=classify_opportunities(fills,g0)
            if int(base_shared.sum())!=int(variant_shared.sum()):
                raise AssertionError("Shared pairs differ")
            a=fill_group_summary(fills,added,ctval)
            dropped=fill_group_summary(g0,base_only,ctval)
            kept=fill_group_summary(g0,base_shared,ctval)
            tr=z["trading"]
            end_ns=min(int(ot[-1]),int(report["variants"]["glft"].get(
                "end_time_ns",ot[-1])))
            inventory=position_valuation_proxy(fills,ot,om,ctval,end_ns)
            reported_pos=float(tr["end_position_contracts"])
            if abs(inventory.get("terminal_inventory_contracts",reported_pos)-reported_pos)>1.e-5:
                raise AssertionError("Official fill path terminal inventory mismatches HBT")
            summary["variant_comparison"][name]={
                "added":a,"dropped":dropped,"kept":kept,
                "official_terminal_position_contracts":reported_pos,
                "official_mean_abs_position_contracts":tr["mean_abs_inventory_contracts"],
                "official_rms_position_contracts":tr["rms_inventory_contracts"],
                "official_position_change_vs_g0_contracts":reported_pos-
                     float(g0tr["end_position_contracts"]),
                "official_max_drawdown_usdt":tr["max_drawdown_usdt"],
                "native_bbo_holding_proxy":inventory,
                "holding_proxy_difference_vs_g0_usdt":(
                    inventory.get("inventory_holding_mtm_drift_usdt_proxy",0.)-
                    base_hold.get("inventory_holding_mtm_drift_usdt_proxy",0.)),
                "selection_alpha_identifiability":"No causal identification from fill groups and BBO drift proxies"}
    return summary


def report_markdown(cases):
    lines=["# Step 1 只读：新增成交/库存漂移与基准 G0 的 Binance 价差分桶",
           "", "**2026-06-01、OKX BTC/SOL，复用此前官方HBT 2tick pilot逐笔成交；本轮无交易策略回放或调参。**",
           "所有日期均为探索样本。前成交标签以成交交易所时间减1ms的已可见原生BBO计算，"
           "不是原始挂单决定时刻；Binance盘口中间价变动不等于确认主动吃单。",
           ""]
    for coin in ("BTC","SOL"):
        r=cases[coin]
        lines +=[f"## {coin}","",
                "|G0 原始成交 Binance触发/幅度桶|成交|占比|100ms Markout均值/P10|500ms均值/P10|1s均值/P10|",
                "|---|---:|---:|---|---|---|"]
        for row in r["g0_buckets"]["buckets"]:
            def hval(h):
                z=row["by_horizon"][f"{h}ms"]
                a=z["mean_markout_bps"];b=z["p10_markout_bps"]
                return (f"{a:+.4f}/{b:+.4f}" if a is not None and b is not None else "N/A")
            lines.append(f"|{row['bin']}|{row['fills']}|{row['fraction_all_g0_fills']:.1%}|"
                         f"{hval(100)}|{hval(500)}|{hval(1000)}|")
        lines+=["", "|策略|Added笔数|Added名义USDT|Added 100ms/500ms Markout(bps)|官方末仓(张)|均值绝对库存(张)|本地BBO库存MTM漂移相对G0(USDT，代理)|",
                "|---|---:|---:|---|---:|---:|---:|"]
        for name,z in r["variant_comparison"].items():
            a=z["added"]
            m100=a["by_horizon"]["100ms"]["mean_markout_bps"]
            m500=a["by_horizon"]["500ms"]["mean_markout_bps"]
            fmt=lambda v:f"{v:+.4f}" if v is not None else "N/A"
            lines.append(f"|{name}|{a['fill_count']}|{a['notional_usdt']:.2f}|{fmt(m100)}/{fmt(m500)}|"
                         f"{z['official_terminal_position_contracts']:+.4f}|"
                         f"{z['official_mean_abs_position_contracts']:.4f}|"
                         f"{z['holding_proxy_difference_vs_g0_usdt']:+.4f}|")
        lines+=[""]
    lines+=["## 解释约束","",
           "- Markout为在本地收到成交回报后未来中间价的方向调整变动；负数是对成交方向不利。",
           "- Added金额为官方成交名义额，不等于多出来的实际利润；仅按同刻同向独立匹配。",
           "- 库存MTM漂移仅为目标原生BBO价格变化乘官方成交重建库存的只读代理，不与官方融合L2/trades做市PNL严格恒等。",
           "- 不能凭单日Added markout和库存漂移唯一区分‘Selection Alpha’因果来源；真正识别需等量无方向对照、订单时间线以及新日重复。",
           "- 有效样本桶过小/触发缺失时不以单笔均值选择bps阈值。"]
    return "\n".join(lines)+"\n"


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--source",required=True)
    p.add_argument("--out",required=True)
    args=p.parse_args()
    cases={}
    for f in Path(args.source).rglob("summary.json"):
        z=json.loads(f.read_text(encoding="utf8"))
        if "coin" in z and "variants" in z:
            coin=z["coin"]
            cases[coin]=audit_case(f.parent,coin)
    if set(cases)!={"BTC","SOL"}:
        raise RuntimeError("Required original BTC/SOL official fill artifacts not found")
    out=Path(args.out)
    out.mkdir(parents=True,exist_ok=True)
    (out/"step1_diagnostics.json").write_text(
        json.dumps(cases,indent=2,ensure_ascii=False),encoding="utf8")
    md=report_markdown(cases)
    (out/"step1_diagnostics.md").write_text(md,encoding="utf8")
    print(md)


if __name__=="__main__":
    main()
