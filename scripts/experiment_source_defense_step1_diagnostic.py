


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
