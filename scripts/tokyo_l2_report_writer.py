"""Machine-readable JSON/CSV and Markdown report writer for unified Tokyo L2 research runs."""
from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any


def _fmt(v: Any, digits: int = 4, suffix: str = "") -> str:
    if v is None:
        return "N/A"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return f"{v:,}{suffix}"
    if isinstance(v, float):
        return f"{v:.{digits}f}{suffix}"
    return str(v)


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        cols = fieldnames or []
        with path.open("w", newline="", encoding="utf-8") as fh:
            if cols:
                csv.DictWriter(fh, fieldnames=cols).writeheader()
        return
    cols = fieldnames or list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def write_unified_reports(report: dict[str, Any], out_dir: Path) -> dict[str, str]:
    """Emit unified_report.json, CSV summary tables, and research_report.md into `out_dir`."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    written: dict[str, str] = {}

    json_path = out_dir / "unified_report.json"
    json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")
    written["json"] = json_path.name

    days = report.get("days") or {}
    models_rows: list[dict[str, Any]] = []
    stage1_rows: list[dict[str, Any]] = []
    stage2_rows: list[dict[str, Any]] = []
    stage3_rows: list[dict[str, Any]] = []
    stage5b_rows: list[dict[str, Any]] = []
    anomaly_rows: list[dict[str, Any]] = []

    def _s2_tm(s2m: dict[str, Any], group: str, h: str) -> dict[str, Any]:
        g = (s2m.get("groups") or {}).get(group) or {}
        hmap = g.get("horizons") or {}
        hv = hmap.get(str(h)) or hmap.get(f"{h}ms") or {}
        return hv.get("total_markout") or {}

    for date_utc, drep in sorted(days.items()):
        bt = drep.get("research") or {}
        s5a = bt.get("stage5a") or {}
        s2 = (bt.get("stage2") or {}).get("models") or {}
        for r in s5a.get("table") or []:
            fs = r.get("fee_scenarios") or {}
            rebate_net = (fs.get("maker_minus0p5bps") or {}).get("net_pnl_usdt")
            s2m = s2.get(r["model"]) or {}
            tm100 = r["total_markout_100ms_bps"] if r.get("total_markout_100ms_bps") is not None else _s2_tm(s2m, "all", "100").get("mean_bps")
            tm500 = r["total_markout_500ms_bps"] if r.get("total_markout_500ms_bps") is not None else _s2_tm(s2m, "all", "500").get("mean_bps")
            tm1000 = r["total_markout_1000ms_bps"] if r.get("total_markout_1000ms_bps") is not None else _s2_tm(s2m, "all", "1000").get("mean_bps")
            models_rows.append({
                "date": date_utc,
                "model": r["model"],
                "fills": r["fills"],
                "buy_fills": r["buy_fills"],
                "sell_fills": r["sell_fills"],
                "turnover_usdt": round(r["turnover_usdt"], 4),
                "gross_marked_pnl_usdt": round(r["gross_marked_pnl_usdt"], 6),
                "gross_pnl_bps": round(r["gross_pnl_bps"], 6) if r["gross_pnl_bps"] is not None else None,
                "breakeven_maker_bps": round(r["breakeven_maker_bps"], 6) if r["breakeven_maker_bps"] is not None else None,
                "net_marked_pnl_usdt_2bps": round(r["net_marked_pnl_usdt_2bps"], 6),
                "fee_usdt_2bps": round(r["fee_usdt_2bps"], 6),
                "net_marked_pnl_usdt_rebate_0p5bps": round(rebate_net, 6) if rebate_net is not None else None,
                "total_markout_100ms_bps": tm100,
                "total_markout_500ms_bps": tm500,
                "total_markout_1000ms_bps": tm1000,
                "position_std_contracts": round(r["position_std_contracts"], 6),
                "rms_inventory_contracts": round(r["rms_inventory_contracts"], 6),
                "max_abs_position_contracts": r["max_abs_position_contracts"],
                "end_position_contracts": r["end_position_contracts"],
                "submits": r["submits"],
                "cancels": r["cancels"],
                "modify_requests": r["modify_requests"],
                "max_order_ops_per_s": r["max_order_ops_per_s"],
                "signal_activations": r["signal_activations"],
                "ties_excluded": r["ties_excluded"],
            })

        s1 = (bt.get("stage1") or {}).get("models") or {}
        for mname, s1m in s1.items():
            stage1_rows.append(s1m)

        for mname, s2m in s2.items():
            g_all = (s2m.get("groups") or {}).get("all") or {}
            for h_key, h_val in (g_all.get("horizons") or {}).items():
                tm_all = h_val.get("total_markout") or {}
                tm_buy = _s2_tm(s2m, "buy", h_key)
                tm_sell = _s2_tm(s2m, "sell", h_key)
                tm_upd = h_val.get("total_markout_after_quote_update") or {}
                stage2_rows.append({
                    "date": date_utc,
                    "model": mname,
                    "horizon_ms": h_key,
                    "fills": s2m.get("hbt_fills"),
                    "total_fresh_n": h_val.get("total_fresh_count"),
                    "total_fresh_fraction": h_val.get("total_fresh_fraction"),
                    "total_markout_mean_bps": tm_all.get("mean_bps"),
                    "total_markout_notional_weighted_bps": tm_all.get("notional_weighted_mean_bps"),
                    "total_markout_neg_frac": tm_all.get("negative_fraction"),
                    "total_markout_p10_bps": tm_all.get("p10_bps"),
                    "total_markout_worst10_bps": tm_all.get("worst10_mean_bps"),
                    "buy_mean_bps": tm_buy.get("mean_bps"),
                    "sell_mean_bps": tm_sell.get("mean_bps"),
                    "updated_quote_n": h_val.get("future_quote_updated_count"),
                    "updated_quote_mean_bps": tm_upd.get("mean_bps"),
                })

        s3 = bt.get("stage3") or {}
        for h_key, cats in (s3.get("categories") or {}).items():
            for grp, gv in cats.items():
                tm = gv.get("total_markout_fresh") or {}
                stage3_rows.append({
                    "date": date_utc,
                    "horizon_ms": h_key,
                    "category": grp,
                    "all_fills": gv.get("all_fills"),
                    "fill_fraction": gv.get("fill_fraction"),
                    "primary_comparison_model": gv.get("primary_comparison_model"),
                    "shared": gv.get("shared"),
                    "baseline_only": gv.get("baseline_only"),
                    "total_markout_n": tm.get("n"),
                    "total_markout_mean_bps": tm.get("mean_bps"),
                    "total_markout_neg_frac": tm.get("negative_fraction"),
                    "total_markout_p10_bps": tm.get("p10_bps"),
                    "total_markout_worst10_bps": tm.get("worst10_mean_bps"),
                    "theoretical_cancellable_count": gv.get("theoretical_cancellable_count"),
                    "theoretical_cancellable_fraction": gv.get("theoretical_cancellable_fraction_of_threatened"),
                })

        s5b = bt.get("stage5b") or {}
        for r in s5b.get("rows") or []:
            stage5b_rows.append(r)

        for mname, mobj in (bt.get("models") or {}).items():
            ai = mobj.get("anomaly_impact") or {}
            for ep in ai.get("per_episode") or []:
                anomaly_rows.append({"date": date_utc, "model": mname, **ep})

    for fname, rows in (
        ("models_summary.csv", models_rows),
        ("stage1_attribution.csv", stage1_rows),
        ("stage2_markout.csv", stage2_rows),
        ("stage3_shadow.csv", stage3_rows),
        ("stage5b_same_fill.csv", stage5b_rows),
        ("anomalies.csv", anomaly_rows),
    ):
        _write_csv(out_dir / fname, rows)
        written[fname] = fname

    md_path = out_dir / "research_report.md"
    md_path.write_text(render_markdown_report(report), encoding="utf-8")
    written["markdown"] = md_path.name
    return written


def render_markdown_report(report: dict[str, Any]) -> str:
    cfg = report.get("config") or {}
    lines: list[str] = []
    lines.append(f"# Unified Backtest & Five-Stage Research Report: `{cfg.get('experiment_name', 'tokyo_l2')}`")
    lines.append("")
    lines.append("## 1. Experiment Configuration & Provenance")
    lines.append("")
    lines.append(f"- **Data Source**: `{cfg.get('data_source')}`")
    lines.append(f"- **Exchange Pair**: `{cfg.get('signal_exchange')}` (`{cfg.get('signal_native_symbol')}`) -> `{cfg.get('making_exchange')}` (`{cfg.get('making_native_symbol')}`)")
    lines.append(f"- **Base Symbol**: `{cfg.get('base_symbol')}`")
    lines.append(f"- **Eval Dates**: `{cfg.get('dates', {}).get('eval_dates')}` | **Train Dates**: `{cfg.get('dates', {}).get('train_dates')}` | **Holdout Dates**: `{cfg.get('dates', {}).get('holdout_dates')}`")
    lat = cfg.get("latency") or {}
    oc = cfg.get("order_controls") or {}
    lines.append(
        f"- **Latency & Order Controls**: signal extra delay `{lat.get('binance_extra_visible_delay_ns', 0) / 1e6:.3f} ms`, "
        f"order entry `{lat.get('order_entry_latency_ns', 0) / 1e6:.3f} ms`, "
        f"order response `{lat.get('order_response_latency_ns', 0) / 1e6:.3f} ms` "
        f"({lat.get('order_latency_assumption_note', 'simulated')}), "
        f"min requote cooldown `{oc.get('min_requote_ns', 0) / 1e6:.3f} ms`, native modify `0`"
    )
    lines.append(f"- **Config SHA-256**: `{cfg.get('config_sha256')}`")
    lines.append(f"- **Status**: `{report.get('status')}` | **Started UTC**: `{report.get('started_utc')}` | **Finished UTC**: `{report.get('finished_utc')}`")
    lines.append("")

    for date_utc, drep in sorted((report.get("days") or {}).items()):
        lines.append(f"---")
        lines.append(f"## 2. Trading Day: `{date_utc}`")
        lines.append("")

        cache_info = drep.get("cache") or {}
        if cache_info:
            lines.append(f"- **Conversion Cache**: `hit={cache_info.get('cache_hit')}` (`{cache_info.get('reason')}`), key=`{cache_info.get('cache_key')}`")
        qa = drep.get("qa_and_data") or {}
        if qa:
            lines.append(f"- **Archive & QA**: status=`{qa.get('stream_status')}`, failed_items=`{qa.get('stream_failed_items')}`, partitions=`{qa.get('partitions_verified')}`")
        anom = drep.get("anomalies") or {}
        if anom:
            lines.append(f"- **Interruptions / Anomalies**: `{anom.get('episode_count', 0)}` episodes (diagnostic only; official HBT fills preserved)")
        lines.append("")

        bt = drep.get("research") or {}
        ep = bt.get("effective_parameters") or {}
        sc = ep.get("scenario") or {}
        lines.append(
            f"- **Contract & Sizing**: tick=`{sc.get('tick')}` (legal=`{sc.get('legal_quote_tick')}`, mult=`{sc.get('quote_tick_mult')}`), "
            f"lot=`{sc.get('lot')}`, ctval=`{sc.get('ctval')}`, qty=`{sc.get('qty')}`, cap=`{sc.get('position_cap')}`, "
            f"gamma_code=`{sc.get('gamma_code')}`, shared_start_ns=`{ep.get('shared_first_eligible_entry_ns')}`"
        )
        lines.append("")

        # Stage 5A Core Model Comparison Table
        s5a = bt.get("stage5a") or {}
        s2 = (bt.get("stage2") or {}).get("models") or {}

        def _md_s2_tm(mname: str, grp: str, h: str) -> dict[str, Any]:
            g = ((s2.get(mname) or {}).get("groups") or {}).get(grp) or {}
            hv = (g.get("horizons") or {}).get(str(h)) or {}
            return hv.get("total_markout") or {}

        if s5a.get("table"):
            lines.append("### Stage 5A: Model Comparison (Zero-Fee Gross Primary + Fee Sensitivity)")
            lines.append("")
            lines.append("| Model | Fills | Turnover (USDT) | Gross PnL (USDT) | Gross (bps) | Net +2bps (USDT) | TM 100ms | TM 500ms | TM 1000ms | Pos Std | Max Abs | Submits | Cancels | Max Ops/s |")
            lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
            for r in s5a["table"]:
                tm100 = r["total_markout_100ms_bps"] if r.get("total_markout_100ms_bps") is not None else _md_s2_tm(r["model"], "all", "100").get("mean_bps")
                tm500 = r["total_markout_500ms_bps"] if r.get("total_markout_500ms_bps") is not None else _md_s2_tm(r["model"], "all", "500").get("mean_bps")
                tm1000 = r["total_markout_1000ms_bps"] if r.get("total_markout_1000ms_bps") is not None else _md_s2_tm(r["model"], "all", "1000").get("mean_bps")
                lines.append(
                    f"| `{r['model']}` | {_fmt(r['fills'])} | {_fmt(r['turnover_usdt'], 2)} | "
                    f"{_fmt(r['gross_marked_pnl_usdt'], 4)} | {_fmt(r['gross_pnl_bps'], 4)} | "
                    f"{_fmt(r['net_marked_pnl_usdt_2bps'], 4)} | "
                    f"{_fmt(tm100, 4)} | {_fmt(tm500, 4)} | {_fmt(tm1000, 4)} | "
                    f"{_fmt(r['position_std_contracts'], 4)} | {_fmt(r['max_abs_position_contracts'], 1)} | "
                    f"{_fmt(r['submits'])} | {_fmt(r['cancels'])} | {_fmt(r['max_order_ops_per_s'])} |"
                )
            lines.append("")

        # Stage 1 Accounting Attribution
        s1 = (bt.get("stage1") or {}).get("models") or {}
        if s1:
            lines.append("### Stage 1: PnL Attribution & Accounting Closure (Conventions A & B)")
            lines.append("")
            lines.append("| Model | Zero-Fee PnL | Spread Capture (A) | Inventory PnL (A) | Residual A | Spread Capture (B) | Inventory PnL (B) | Residual B | RMS Pos |")
            lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
            for mname, m in s1.items():
                lines.append(
                    f"| `{mname}` | {_fmt(m['zero_fee_pnl_usdt'], 4)} | {_fmt(m['spread_capture_usdt'], 4)} | "
                    f"{_fmt(m['inventory_price_pnl_usdt'], 4)} | {_fmt(m['accounting_residual_a_usdt'], 6)} | "
                    f"{_fmt(m['next_sample_capture_usdt'], 4)} | {_fmt(m['next_sample_inventory_pnl_usdt'], 4)} | "
                    f"{_fmt(m['accounting_residual_b_usdt'], 6)} | {_fmt(m['rms_position_contracts'], 4)} |"
                )
            lines.append("")

        # Stage 2 Execution-Price Total Markout & Fill Quality
        if s2:
            lines.append("### Stage 2: Execution-Price Total Markout & Fill Quality (All / Buy / Sell / Tails)")
            lines.append("")
            lines.append("| Model | Horizon | Fresh N | Mean (bps) | Notional-Wtd | Buy (bps) | Sell (bps) | Neg Frac | P10 (bps) | Worst10 (bps) |")
            lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
            for mname, s2m in s2.items():
                hmap = ((s2m.get("groups") or {}).get("all") or {}).get("horizons") or {}
                for h in ("100", "500", "1000"):
                    if h not in hmap:
                        continue
                    hv = hmap[h]
                    tm = hv.get("total_markout") or {}
                    tmb = _md_s2_tm(mname, "buy", h)
                    tms = _md_s2_tm(mname, "sell", h)
                    lines.append(
                        f"| `{mname}` | {h}ms | {_fmt(hv.get('total_fresh_count'))} | "
                        f"{_fmt(tm.get('mean_bps'), 4)} | {_fmt(tm.get('notional_weighted_mean_bps'), 4)} | "
                        f"{_fmt(tmb.get('mean_bps'), 4)} | {_fmt(tms.get('mean_bps'), 4)} | "
                        f"{_fmt(tm.get('negative_fraction'), 4)} | {_fmt(tm.get('p10_bps'), 4)} | "
                        f"{_fmt(tm.get('worst10_mean_bps'), 4)} |"
                    )
            lines.append("")

        # Stage 3 Shadow Audit
        s3 = bt.get("stage3") or {}
        if s3.get("status") == "complete":
            lc = s3.get("order_lifecycle_and_price_consistency") or {}
            lines.append("### Stage 3: Baseline GLFT Read-Only Shadow Signal Audit")
            lines.append("")
            lines.append(
                f"- **Parity Verified**: `{s3.get('parity_verified')}` | **Total GLFT Fills**: `{s3.get('total_fills')}` | "
                f"**Categories**: `{s3.get('category_counts')}`"
            )
            lines.append(
                f"- **Order Lifecycle & Price Consistency**: open_at_deadline=`{_fmt(lc.get('open_order_present_fraction'), 4)}`, "
                f"cancellable=`{_fmt(lc.get('cancellable_order_fraction'), 4)}`, "
                f"price_exact_match_among_open=`{_fmt(lc.get('price_exact_match_rate_among_open'), 4)}`, "
                f"threatened_theoretical_cancellable=`{lc.get('threatened_theoretical_pre_fill_cancellable')}/{lc.get('threatened_count')}` "
                f"(`{_fmt(lc.get('threatened_theoretical_cancellable_fraction'), 4)}`)"
            )
            lines.append("")
            lines.append("| Category | Fills | Fraction | 100ms TM (bps) | 500ms TM (bps) | 1000ms TM (bps) | 500ms Neg Frac | 500ms Worst10 |")
            lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
            c100 = (s3.get("categories") or {}).get("100") or {}
            c500 = (s3.get("categories") or {}).get("500") or {}
            c1000 = (s3.get("categories") or {}).get("1000") or {}
            for grp in ("threatened", "inactive_or_no_effect", "opposite"):
                g1 = c100.get(grp) or {}
                g5 = c500.get(grp) or {}
                g10 = c1000.get(grp) or {}
                tm1 = g1.get("total_markout_fresh") or {}
                tm5 = g5.get("total_markout_fresh") or {}
                tm10 = g10.get("total_markout_fresh") or {}
                lines.append(
                    f"| `{grp}` | {_fmt(g5.get('all_fills'))} | {_fmt(g5.get('fill_fraction'), 4)} | "
                    f"{_fmt(tm1.get('mean_bps'), 4)} | {_fmt(tm5.get('mean_bps'), 4)} | {_fmt(tm10.get('mean_bps'), 4)} | "
                    f"{_fmt(tm5.get('negative_fraction'), 4)} | {_fmt(tm5.get('worst10_mean_bps'), 4)} |"
                )
            lines.append("")

        # Stage 5B Same-Fill Decomposition
        s5b = bt.get("stage5b") or {}
        if s5b.get("status") == "complete":
            lines.append("### Stage 5B: Same-Fill Execution Economics Decomposition (500ms Horizon)")
            lines.append("")
            lines.append("| Slice | Fills | Total Markout (bps) | TM - 2bps | TM + 0.5bps | Capture (bps) | Drift (bps) | Updated-Quote TM |")
            lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
            for cls_name, hmap in (s5b.get("by_class") or {}).items():
                r5 = hmap.get("500") or {}
                lines.append(
                    f"| `{cls_name}` | {_fmt(r5.get('total_fills'))} | {_fmt(r5.get('total_markout_bps'), 4)} | "
                    f"{_fmt(r5.get('total_markout_minus_maker2bps_bps'), 4)} | "
                    f"{_fmt(r5.get('total_markout_plus_rebate0p5bps_bps'), 4)} | "
                    f"{_fmt(r5.get('capture_bps'), 4)} | {_fmt(r5.get('markout_bps'), 4)} | "
                    f"{_fmt(r5.get('updated_quote_total_markout_bps'), 4)} |"
                )
            lines.append("")

        # Stage 4 & Stage 5C Honest Governance Status
        s4 = bt.get("stage4") or {}
        s5c = bt.get("stage5c") or {}
        lines.append("### Stage 4 & Stage 5C Governance Status")
        lines.append("")
        lines.append(f"- **Stage 4 (Non-Binance Controls)**: `status={s4.get('status')}`, `executed={s4.get('executed')}` — {s4.get('reason', '')}")
        lines.append(f"- **Stage 5C (Holdout Validation)**: `status={s5c.get('status')}`, `executed={s5c.get('executed')}` — {s5c.get('reason', '')}")
        lines.append("")

    return "\n".join(lines) + "\n"
