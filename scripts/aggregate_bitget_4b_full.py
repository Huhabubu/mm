"""Strict 24/24 independent-day Bitget 4B archive; no archived Gate edits.

Consumes only actual successful official HBT JSON + fill traces from this run.
Writes a separate report directory, rejecting missing/duplicate asset days.
"""
import argparse
import csv
import hashlib
import json
import math
import os
from pathlib import Path

from aggregate_cross_exchange_r1 import aggregate_reports, independent_day_means

SYMBOLS=("BTCUSDT","ETHUSDT","SOLUSDT","DOGEUSDT")
DAYS=tuple(f"2026-{m:02d}-01" for m in range(6,12))
MODELS=("glft","both_1p0","risk_side_pause")
DOGE_STRICT=("2026-06-01","2026-09-01","2026-10-01")
CUTS=("l2_reconnect_gap_pm5s","oct_01_02utc")


def average(items):
    z=[float(v) for v in items if v is not None]
    return sum(z)/len(z) if z else None


def sha256(path):
    h=hashlib.sha256()
    with Path(path).open("rb") as f:
        for block in iter(lambda:f.read(1024*1024),b""):
            h.update(block)
    return h.hexdigest()


def assert_report(report,symbol,day):
    if (report["status"]!="complete" or report["symbol"]!=symbol
            or report["day"]!=day or report["venue"]!="bitget-futures"
            or report["minutes_traded"]!=1430 or report["no_full_day"]
            or set(report["models"])!=set(MODELS)):
        raise ValueError(f"Wrong/incomplete Bitget full-day report {symbol} {day}")
    opts=report["core_options"]
    required={"min_requote_ns":0,"fit_mode":"physical_1tick_poisson",
              "wait_book_after_fill":True,"post_only_clamp_mode":"unified",
              "cap_depth_at_visible_levels":False,
              "cross_timestamp_tie_mode":"microsecond"}
    if any(opts.get(k)!=v for k,v in required.items()):
        raise ValueError(f"R1 switch mismatch for {symbol}/{day}")
    if (report["lossless_raw_L2_check"]["off_raw_hbt_grid_rows"]!=0
            or report["lossless_raw_L2_check"]["l2_crossed_post_message_book_checks"]!=0
            or not report["lossless_raw_L2_check"]["initial_snapshot_complete"]):
        raise ValueError(f"Lossless L2 / sampled quote cross check failed: {symbol}/{day}")
    qty=report["gate_base_quantity_reference"]
    if not math.isclose(qty["position_cap_over_order_qty_gate"],
                        qty["position_cap_over_order_qty_bitget"],abs_tol=1e-9):
        raise ValueError("Inventory multiple mismatch")
    if not math.isclose(qty["quantity_signed_difference_base"],
                        qty["bitget_actual_qty_base"]-qty["gate_target_qty_base"],
                        abs_tol=1e-9):
        raise ValueError("Quantity difference ledger mismatch")
    start=report["shared_first_eligible_entry_ns"]
    if not start or any(report["models"][m]["initial_fit_ns"]!=start for m in MODELS):
        raise ValueError("Models do not share initial fit")
    if abs(report["scenario"]["maker_fee_rate"]-.0002)>1e-12:
        raise ValueError("Maker 2bps mismatch")
    for name in MODELS:
        v=report["models"][name];t=v["trading"]
        gross=t["gross_marked_pnl_usdt"]
        if not math.isclose(gross-t["fee_usdt"],t["end_net_marked_equity_usdt"],abs_tol=1e-6):
            raise ValueError("Gross/fee/net ledger fails")
        if not math.isclose(t["fee_usdt"],.0002*t["trading_value_usdt"],abs_tol=1e-6):
            raise ValueError("Maker ledger/turnover fails")
        if t["active_inventory_samples"]<1:
            raise ValueError("Empty post-entry inventory record")
        if t["max_abs_position_base"]>report["scenario"]["position_cap"]+report["scenario"]["qty"]+1e-8:
            raise ValueError("Inventory exceeded position cap")
        if v["microsecond_collisions"]<0 or v["binance_origin_filter"]["same_timestamp_ties_blocked"]<0:
            raise ValueError("Negative C1 event counters")
        if v["quote_quality"]["hbt_fills"]!=t["engine_fills"]:
            raise ValueError("HBT fill/Markout mismatch")
        if not set(CUTS).issubset(v["posthoc_slices"]):
            raise ValueError("Missing prespecified posthoc slices")
    return True


def make_row(report,name,base):
    v=report["models"][name]
    z=v["quote_quality"]["groups"]["all"]["horizons"]
    row=dict(base)
    row["max_ops_per_sec"]=v["max_ops_per_sec"]
    row["resting_bid_ratio"]=v["resting_time_ratio"]["bid"]
    row["resting_ask_ratio"]=v["resting_time_ratio"]["ask"]
    row["microsecond_collisions"]=v["microsecond_collisions"]
    row["same_timestamp_ties_blocked"]=v["binance_origin_filter"]["same_timestamp_ties_blocked"]
    row["initial_fit_ns"]=v["initial_fit_ns"]
    for h in (100,500,1000):
        hz=z[str(h)]
        mark=hz["total_markout"]
        row[f"total_markout_{h}ms_bps"]=mark["mean_bps"] if mark else None
        row[f"total_markout_{h}ms_n"]=hz["total_fresh_count"]
        updated=hz["total_markout_after_quote_update"]
        if h==100:
            row["total_markout_100ms_updated_bps"]=(updated["mean_bps"] if updated else None)
            row["total_markout_100ms_updated_n"]=hz["future_quote_updated_count"]
    for cut in CUTS:
        slice=v["posthoc_slices"][cut]
        quality=slice["quality"]
        row[cut+"_excluded_fills"]=quality["excluded_fills"]
        row[cut+"_excluded_fill_fraction"]=quality["excluded_fill_fraction"]
        row[cut+"_excluded_notional_usdt"]=quality["excluded_notional_usdt"]
        row[cut+"_excluded_notional_fraction"]=quality["excluded_notional_fraction"]
        row[cut+"_window_count"]=slice.get("window_count")
        row[cut+"_window_seconds"]=slice.get("window_seconds")
        for h in (100,500,1000):
            row[f"{cut}_kept_{h}ms_bps"]=quality["horizons"][str(h)]["total_markout_bps"]
            row[f"{cut}_kept_{h}ms_n"]=quality["horizons"][str(h)]["total_markout_n"]
        row[f"{cut}_kept_100ms_updated_bps"]=quality["horizons"]["100"]["total_markout_after_quote_update_bps"]
    return row


def group_means(rows,days):
    chosen=[r for r in rows if r["day"] in days]
    output=[]
    keys=("gross_bps","net_usdt","gross_usdt","fee_usdt",
          "turnover_usdt","pos_std_base","pos_std_over_order_qty",
          "total_markout_100ms_bps","total_markout_100ms_updated_bps",
          "total_markout_500ms_bps","total_markout_1000ms_bps",
          "net_delta_vs_glft_usdt","fee_saved_usdt","vol_shrink_gross_usdt",
          "quality_gross_usdt","max_ops_per_sec","resting_bid_ratio",
          "resting_ask_ratio","microsecond_collisions","same_timestamp_ties_blocked",
          "net_fee_0bps_usdt","net_fee_minus_0p5bps_usdt","net_fee_plus_2bps_usdt")
    for sym in SYMBOLS:
        sub=[r for r in chosen if r["symbol"]==sym]
        baseline={r["day"]:r for r in sub if r["model"]=="glft"}
        for model in MODELS:
            z=[r for r in sub if r["model"]==model]
            if len(z)!=len(days):
                raise ValueError(f"Missing group-day coverage {sym}/{model} {len(z)}/{len(days)}")
            res={"symbol":sym,"model":model,"valid_days":len(z)}
            for key in keys:
                res[key]=average(r.get(key) for r in z)
            if model=="glft":
                res["gross_bps_positive_days"]=None
                res["gross_bps_vs_glft_win_days"]=None
            else:
                res["gross_bps_vs_glft_win_days"]=sum(
                    r["gross_bps"]>baseline[r["day"]]["gross_bps"] for r in z)
                res["gross_bps_positive_days"]=sum(r["gross_bps"]>0 for r in z)
            res["net_positive_days"]=sum(r["net_usdt"]>0 for r in z)
            output.append(res)
    return output


def paired_posthoc(rows,days,cut):
    """Direction of Markout difference only; not a coherent counterfactual PnL."""
    result=[]
    for sym in SYMBOLS:
        for model in MODELS[1:]:
            sub=[]
            for day in days:
                idx={(r["symbol"],r["day"],r["model"]):r for r in rows}
                m=idx[(sym,day,model)]
                b=idx[(sym,day,"glft")]
                z={"day":day,"symbol":sym,"model":model,
                   "model_fills_excluded":m[cut+"_excluded_fills"],
                   "model_notional_excluded_fraction":m[cut+"_excluded_notional_fraction"],
                   "glft_notional_excluded_fraction":b[cut+"_excluded_notional_fraction"]}
                for h in (100,500,1000):
                    k=f"{cut}_kept_{h}ms_bps"
                    z[f"delta_markout_{h}ms_bps"]=(
                        m[k]-b[k] if m[k] is not None and b[k] is not None else None)
                sub.append(z)
            out={"symbol":sym,"model":model,"scope":cut,
                 "days":len(sub),"per_day":sub}
            for h in (100,500,1000):
                key=f"delta_markout_{h}ms_bps"
                values=[r[key] for r in sub if r[key] is not None]
                out[key+"_day_mean"]=average(values)
                out[key+"_improved_days"]=sum(x>0 for x in values)
                out[key+"_valid_days"]=len(values)
            result.append(out)
    return result


def write_csv(path,rows):
    if not rows:
        raise ValueError("Empty CSV")
    with path.open("w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def build(root,out,run_id,source_sha):
    root=Path(root);out=Path(out);out.mkdir(parents=True,exist_ok=True)
    reports=[]; manifest=[]
    for sym in SYMBOLS:
        for day in DAYS:
            name=f"{sym}_{day}_1430m.json"
            matches=list(root.rglob(name))
            if len(matches)!=1:
                raise ValueError(f"Expected exactly one official HBT report: {name}, found {len(matches)}")
            path=matches[0]
            data=json.loads(path.read_text(encoding="utf-8"))
            assert_report(data,sym,day)
            reports.append((data,str(path)))
            source={"symbol":sym,"day":day,"actions_run_id":int(run_id),
                    "source_commit_sha":source_sha,
                    "artifact_name":f"bitget-4b-day-{sym}-{day}",
                    "report_filename":path.name,
                    "report_sha256":sha256(path),
                    "fill_files":{}}
            for model in MODELS:
                fn=f"{sym}_{day}_{model}_fills.npz"
                candidates=list(path.parent.glob(fn))
                if len(candidates)!=1:
                    raise ValueError(f"Missing exactly one raw fill trace {fn}")
                source["fill_files"][model]={"filename":fn,"sha256":sha256(candidates[0])}
            manifest.append(source)
    rows=aggregate_reports(reports)
    if len(rows)!=72:
        raise ValueError(f"Expected 24 days × 3 models, got {len(rows)}")
    ix={(r["symbol"],r["day"],r["model"]):r for r in rows}
    allrows=[make_row(report,model,ix[(report["symbol"],report["day"],model)])
             for report,_ in reports for model in MODELS]
    for r in allrows:
        if r["model"]!="glft" and abs(r["decomposition_residual_usdt"])>1e-8:
            raise ValueError("Fee/volume/quality decomposition mismatch")
    quantity=[]
    for d,_ in reports:
        q=d["gate_base_quantity_reference"]
        quantity.append({"symbol":d["symbol"],"day":d["day"],
                         "gate_reference_mid_usdt":q["gate_reference_mid_usdt"],
                         "gate_target_qty_base":q["gate_target_qty_base"],
                         "bitget_actual_qty_base":q["bitget_actual_qty_base"],
                         "qty_signed_difference_base":q["quantity_signed_difference_base"],
                         "qty_relative_difference":q["quantity_relative_difference"],
                         "gate_order_notional_usdt":q["gate_order_nominal_usdt"],
                         "bitget_notional_at_gate_mid_usdt":q["bitget_nominal_at_gate_reference_usdt"],
                         "notional_difference_usdt":q["nominal_difference_at_gate_reference_usdt"],
                         "strict_base_amount_equal":q["strict_base_amount_equal"],
                         "gate_max_pos_base":q["gate_max_pos_base"],
                         "bitget_max_pos_base":q["bitget_max_pos_base"],
                         "max_pos_over_order_qty":q["position_cap_over_order_qty_bitget"],
                         "bitget_candidate_step_base":q["bitget_candidate_order_step_base"]})
    write_csv(out/"per_day_models.csv",allrows)
    write_csv(out/"qty_alignment.csv",quantity)
    means6=group_means(allrows,DAYS)
    means5=group_means(allrows,tuple(d for d in DAYS if d!="2026-10-01"))
    doge6=[r for r in means6 if r["symbol"]=="DOGEUSDT"]
    doge3=[r for r in group_means(allrows,DOGE_STRICT) if r["symbol"]=="DOGEUSDT"]
    # Posthoc L2 and October diagnostics, including actual fill percentage.
    cuts_all={cut:paired_posthoc(allrows,DAYS,cut) for cut in CUTS[:1]}
    october=paired_posthoc(allrows,("2026-10-01",),"oct_01_02utc")
    payload={"status":"complete","source_actions_run_id":int(run_id),
             "source_commit_sha":source_sha,"completed_asset_days":len(reports),
             "models_per_day":len(MODELS),"day_equal_weighted_6":means6,
             "day_equal_weighted_excluding_october_5":means5,
             "doge_strict_3":doge3,"doge_dual_grid_6":doge6,
             "posthoc_l2_pm5s":cuts_all["l2_reconnect_gap_pm5s"],
             "posthoc_october_2h":october,
             "model_rows":allrows,
             "fee_scenarios_bps":[0,-.5,2],
             "accounting":"net_delta=fee_saved+vol_shrink_gross+quality_gross",
             "posthoc_note":"Dropping fills for quality diagnosis does not recompute coherent HBT PnL",
             "data_note":"Native WS reconnect sequence not verified from normalized Tardis CSV"}
    (out/"summary.json").write_text(json.dumps(payload,ensure_ascii=False,indent=2,
                                                 allow_nan=False)+"\n",encoding="utf-8")
    (out/"manifest.json").write_text(json.dumps({
        "source_actions_run_id":int(run_id),"source_commit_sha":source_sha,
        "inputs":manifest,"all_reports_sha256_verified":True,
        "expected_asset_days":24,"expected_model_rows":72,
        "historical_bitget_contract_metadata_verified":False,
        "frozen_gate_results_modified":False},
        ensure_ascii=False,indent=2,allow_nan=False)+"\n",encoding="utf-8")
    lines=[
        "# Bitget 4B 四币×六日正式回测",
        "",
        f"源 Actions Run {run_id}；代码提交 \`{source_sha}\`。24个完整UTC资产日，72条模型日结果，"
        "独立HBT引擎、同日共享初次有效校准；Maker +2bps。",
        "",
        "**单位**：gross_bps为每双边成交额的盯市毛收益；"
        "Total Markout为实际成交价对未来中间价的方向化收益；"
        "pos_std/order_qty为标准化库存波动。全部日均为每天等权。",
        "",
        "## 六日等权模型主指标",
        "",
        "|资产|模型|Gross bps|Gross改善日/6|500ms Total Markout|1000ms Total Markout|Net USDT/日|pos_std/单笔|",
        "|---|---|---:|---:|---:|---:|---:|---:|"]
    for x in means6:
        fmt=lambda k:("NA" if x.get(k) is None else f"{x[k]:.4f}")
        lines.append(f"|{x['symbol']}|{x['model']}|{fmt('gross_bps')}|"
                     f"{x.get('gross_bps_vs_glft_win_days') or '-'}|"
                     f"{fmt('total_markout_500ms_bps')}|{fmt('total_markout_1000ms_bps')}|"
                     f"{fmt('net_usdt')}|{fmt('pos_std_over_order_qty')}|")
    lines.extend(["","## 同日GLFT净改善三项账簿恒等式（USDT/日）","","|资产|模型|净收益差|费率节省|缩量毛损变化|单位毛收益改善|",
                  "|---|---|---:|---:|---:|---:|"])
    for x in means6:
        if x["model"]=="glft":continue
        fmt=lambda k:f"{x[k]:+.5f}"
        lines.append(f"|{x['symbol']}|{x['model']}|{fmt('net_delta_vs_glft_usdt')}|"
                     f"{fmt('fee_saved_usdt')}|{fmt('vol_shrink_gross_usdt')}|"
                     f"{fmt('quality_gross_usdt')}|")
    lines.extend(["","## 三项事后敏感性",
                  "",
                  "1. L2额外快照与相邻更新时间差>1000ms的±5秒，"
                  "见summary.json的posthoc_l2_pm5s；逐日窗口内成交笔数和成交额比例保存在per_day_models.csv。",
                  "2. 6日与剔除10月后的5日等权，分别见day_equal_weighted_6与day_equal_weighted_excluding_october_5；"
                  "10-01日01:00–02:59 UTC的剔除诊断见posthoc_october_2h。",
                  "3. DOGE严格3日与双层网格6日，分别见doge_strict_3与doge_dual_grid_6。",
                  "",
                  "上述剔除只重算成交后质量，不重新运行撮合或重平衡库存；不得用它作为策略净收益敏感性结论。",
                  "报价零交叉校验指完整L2消息后抽样，无原始逐包认证。合约步长与手续费均使用研究假定，"
                  "各日期经过既往探索；统计不能称独立样本外因果收益。"])
    (out/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")
    print("BITGET_4B_24_ASSET_DAY_ARCHIVE_OK",len(reports),len(allrows),flush=True)
    return payload


if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--root",required=True)
    p.add_argument("--out",default="reports/chapter5_bitget_4b_20261120")
    p.add_argument("--run-id",default=os.environ.get("GITHUB_RUN_ID",""))
    p.add_argument("--sha",default=os.environ.get("GITHUB_SHA",""))
    a=p.parse_args()
    if not a.run_id.isdigit() or len(a.sha)!=40:
        p.error("Requires actual Actions run ID and full source commit SHA")
    build(a.root,a.out,a.run_id,a.sha)
