"""Chapter 5 *actual* HBT day-level results; fail closed and retain missing dates.

Source: independent matrix-job JSON from full Tardis HBT 2.4.4, including
execution-price total Markout computed by the Stage1-accepted quote asof rules.
No synthetic outcomes, inferred fill rates or automatically zero-filled days.
"""
import argparse
import csv
import json
import math
import hashlib
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

DAYS = tuple(f"2026-{m:02d}-01" for m in range(6, 12))
FIRST5 = DAYS[:5]
SYMBOLS = ("BTC_USDT", "SOL_USDT", "DOGE_USDT", "ETH_USDT")
MODELS = ("glft", "both_1p0", "risk_side_pause")
HORIZONS = (100, 500, 1000)
# Sources verified against the completed GitHub Actions runs; these are
# source-job SHA values, not the later report publication commit.
SOURCE_RUN = {
    "BTC_USDT": (44443409775, "8ca127aab552e96923a46b0979af85fe6bca88c6"),
    "SOL_USDT": (44445139030, "c4c711104079449bb8321aad618bec005964855f"),
    "DOGE_USDT": (44445139030, "c4c711104079449bb8321aad618bec005964855f"),
    "ETH_USDT": (44445139030, "c4c711104079449bb8321aad618bec005964855f"),
}

def source_evidence(key, path):
    """Tie a real JSON to the downloaded run, named matrix artifact, and digest.

    Actions downloads in the finalization workflow are pinned by run_id;
    artifact name/relative path are independently checked here. A run ID
    cannot be established from the JSON alone, only from the pinned download.
    """
    symbol, day, case = key
    run_id, commit = SOURCE_RUN[symbol]
    prefix = "chapter5-case" if symbol == "BTC_USDT" else "chapter5-fixed-case"
    expected = f"{prefix}-{symbol}-{case}-{day}"
    p = Path(path)
    if expected not in p.parts or p.name != f"{day}_1430m.json":
        raise ValueError(f"Invalid artifact name/path for {key}: {path}, expected {expected}")
    if symbol != "BTC_USDT" and case != "base":
        raise ValueError(f"Repaired altcoin run can only contain base cases: {key}")
    return {
        "run_id": run_id,
        "run_url": f"https://github.com/Huhabubu/mm/actions/runs/{run_id}",
        "source_head_sha": commit,
        "artifact_name": expected,
        "artifact_path": str(p),
        "json_sha256": hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() else None,
    }

def expect_cases():
    return {(s,d,"base") for s in SYMBOLS for d in DAYS} | {
        ("BTC_USDT", d, "cap") for d in FIRST5}

def numeric(v):
    if v is None:
        return None
    value = float(v)
    if not math.isfinite(value):
        raise ValueError(f"Non-finite numeric output: {v}")
    return value

def mean(values):
    a = [numeric(v) for v in values if v is not None]
    return sum(a)/len(a) if a else None

def fmt(x, digits=4):
    return "NA" if x is None else f"{x:+.{digits}f}"

def validate_case(report, path):
    symbol,day = report.get("gate_symbol"),report.get("day")
    config=report.get("core_options",{})
    if symbol not in SYMBOLS or day not in DAYS or not isinstance(config,dict):
        raise ValueError(f"Unexpected symbol/day/options: {path}")
    # Classify using the actual downloaded artifact folder, including
    # the distinct repaired cross-asset run; never trust just a JSON switch.
    marker=next((p for p in path.parts if p.startswith("chapter5-case-")
                 or p.startswith("chapter5-fixed-case-")),None)
    if marker is None:
        raise ValueError(f"Unprovenanced official HBT input: {path}")
    if marker.endswith(f"-cap-{day}"):
        case="cap"
    elif marker.endswith(f"-base-{day}"):
        case="base"
    else:
        raise ValueError(f"Artifact case identity inconsistent with day: {path}")
    key=(symbol,day,case)
    if key not in expect_cases():
        raise ValueError(f"Unexpected experimental case {key}: {path}")
    evidence=source_evidence(key,path)
    if report.get("status") != "complete":
        return key,{"status":"failed","error":report.get("error","unknown"),
                    "path":str(path),"evidence":evidence}
    if config.get("cap_depth_at_visible_levels") is not (case=="cap"):
        raise ValueError(f"Internal cap setting differs from job identity: {key}")
    if report.get("minutes_traded") != 1430 or report.get("binance_extra_visible_delay_ms") != 2.5:
        raise ValueError(f"Wrong full day / Binance delay: {key}")
    if not report.get("core_extension") or report.get("sigma_threshold",1.645)!=1.645:
        raise ValueError(f"Not a Stage2B core replay: {key}")
    settings={
        "min_requote_ns":0,
        "fit_mode":"physical_1tick_poisson",
        "wait_book_after_fill":True,
        "post_only_clamp_mode":"unified",
        "cap_depth_at_visible_levels":case=="cap",
    }
    for k,v in settings.items():
        if config.get(k)!=v:
            raise ValueError(f"Wrong frozen setting {key}:{k}, observed {config.get(k)}")
    if numeric(report["scenario"]["maker_fee_rate"])!=0.0002:
        raise ValueError(f"Wrong positive 2bps maker assumption: {key}")
    target=set(MODELS if case=="base" else MODELS[:2])
    if set(report["variants"]) != target:
        raise ValueError(f"Missing/extra strategy models {key}: {set(report['variants'])}")
    start=report.get("shared_first_eligible_entry_ns")
    expected_ns=int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp())*10**9+600*10**9
    if not isinstance(start,int) or start<expected_ns:
        raise ValueError(f"Invalid shared entry deadline {key}: {start}")
    wait=(start-expected_ns)/1e9
    for model,v in report["variants"].items():
        if v.get("initial_fit_ns") != start:
            raise ValueError(f"Models do not have identical first calibration time: {key}:{model}")
        # HBT starts at its first replayable native market event, which can
        # occur several hundred milliseconds after UTC midnight. Core's
        # calibration_wait_ns is relative to that t0+600s local replay clock;
        # the chapter5 wait_s above is relative to UTC 00:10:00 precisely.
        t0=v.get("start_time_ns")
        if not isinstance(t0,int):
            raise ValueError(f"Missing exact HBT first event nanosecond: {key}:{model}")
        expected_engine_wait=max(0,start-(t0+600_000_000_000))
        if abs(v.get("calibration_wait_ns",-1)-expected_engine_wait)>1_000_000:
            raise ValueError(f"Engine-clock calibration waiting time mismatch: {key}:{model}")
        t=v["trading"]
        n=int(t["engine_fills"])
        if n != int(t["buy_fills"])+int(t["sell_fills"]):
            raise ValueError(f"Fill count mismatch: {key}/{model}")
        net=numeric(t["end_net_marked_equity_usdt"])
        fee=numeric(t["fee_usdt"])
        gross=numeric(t["gross_marked_pnl_usdt"])
        turnover=numeric(t["trading_value_usdt"])
        if fee<0 or abs(gross-(net+fee))>1e-6 or turnover<0:
            raise ValueError(f"Invalid fee/gross/turnover accounting: {key}/{model}")
        if abs(fee-0.0002*turnover)>1e-6:
            raise ValueError(f"Maker fee not 2bps of turnover: {key}/{model}")
        if turnover>0:
            gross_bps=numeric(t["gross_marked_pnl_per_turnover_bps"])
            net_bps=numeric(t["net_marked_pnl_per_turnover_bps"])
            if (abs(gross_bps-10000*gross/turnover)>1e-8
                    or abs(net_bps-10000*net/turnover)>1e-8
                    or abs((gross_bps-net_bps)-2.0)>1e-8):
                raise ValueError(f"Gross/Net bps denominator or fees mismatch: {key}/{model}")
        if n and turnover<=0:
            raise ValueError(f"Nonpositive turnover on traded day: {key}/{model}")
        if v.get("max_ops_per_sec") is None or v.get("resting_time_ratio",{}).get("verified_exchange_resting") is not False:
            raise ValueError(f"Missing honest execution diagnostics: {key}/{model}")
        if v.get("quote_quality",{}).get("hbt_fills")!=n:
            raise ValueError(f"Markout annotated fills don't match official HBT: {key}/{model}")
        for h in HORIZONS:
            z=v["quote_quality"]["groups"]["all"]["horizons"][str(h)]
            if z["total_fresh_count"]>n or z["future_quote_updated_count"]>z["total_fresh_count"]:
                raise ValueError(f"Impossible Markout quote freshness: {key}/{model}/{h}")
        if case=="cap":
            d=v.get("depth_cap_diagnostic",{})
            if not d.get("enabled") or not d.get("verified_source_20level"):
                raise ValueError(f"Cannot claim visible-depth cap on this source: {key}/{model}")
        else:
            if v.get("depth_cap_diagnostic",{}).get("enabled"):
                raise ValueError(f"Base replay accidentally capped: {key}/{model}")
    return key,{"status":"complete","path":str(path),"report":report,
                "start":start,"wait_s":wait,"evidence":evidence}

def collect(root):
    found={}
    for p in sorted(Path(root).rglob("*_1430m.json")):
        d=json.loads(p.read_text(encoding="utf-8"))
        key,result=validate_case(d,p)
        if key in found:
            raise ValueError(f"Duplicate independent full day experiment {key}")
        found[key]=result
    if not found:
        raise ValueError(f"No actual full-day JSON artifacts found in {root}")
    for key in sorted(expect_cases()-set(found)):
        found[key]={"status":"missing","error":"No official HBT artifact for expected day"}
    return found

def row_for(key, model, result):
    symbol,day,case=key
    base={
        "symbol":symbol,"day":day,"case":case,"model":model,
        "status":result["status"],"error":result.get("error",""),
        "source_artifact":result.get("path",""),
        "source_run_id":result.get("evidence",{}).get("run_id",""),
        "source_run_url":result.get("evidence",{}).get("run_url",""),
        "source_head_sha":result.get("evidence",{}).get("source_head_sha",""),
        "source_artifact_name":result.get("evidence",{}).get("artifact_name",""),
        "source_json_sha256":result.get("evidence",{}).get("json_sha256",""),
        "first_fit_ns":"","calibration_wait_s":"",
        "gross_pnl_usdt":"","gross_pnl_turnover_bps":"",
        "net_pnl_usdt":"","net_pnl_turnover_bps":"",
        "maker_fee_usdt":"","fills":"","turnover_usdt":"",
        "position_std_contracts":"","max_abs_position_contracts":"",
        "max_ops_per_sec":"","resting_bid_ratio":"","resting_ask_ratio":"",
        "depth_cap_checks":"","depth_cap_changed":"","depth_cap_changed_fraction":"",
        "depth_cap_insufficient":"","depth_cap_insufficient_fraction":"",
        "delta_net_vs_glft_usdt":"","net_improved_vs_glft":"",
    }
    for h in HORIZONS:
        base.update({f"markout_{h}ms_bps":"",f"markout_{h}ms_count":"",
                     f"markout_{h}ms_future_quote_updated_bps":"",
                     f"markout_{h}ms_future_quote_updated_count":"",
                     f"delta_markout_{h}ms_vs_glft_bps":"",
                     f"markout_{h}ms_improved_vs_glft":"",
                     f"delta_markout_{h}ms_updated_vs_glft_bps":"",
                     f"markout_{h}ms_updated_improved_vs_glft":""})
    if result["status"]!="complete":
        return base
    v=result["report"]["variants"][model]
    t=v["trading"]
    base.update({
        "status":"complete","first_fit_ns":int(result["start"]),
        "calibration_wait_s":result["wait_s"],
        "gross_pnl_usdt":t["gross_marked_pnl_usdt"],
        "gross_pnl_turnover_bps":t["gross_marked_pnl_per_turnover_bps"],
        "net_pnl_usdt":t["end_net_marked_equity_usdt"],
        "net_pnl_turnover_bps":t["net_marked_pnl_per_turnover_bps"],
        "maker_fee_usdt":t["fee_usdt"],
        "fills":t["engine_fills"],
        "turnover_usdt":t["trading_value_usdt"],
        "position_std_contracts":t["position_std_contracts"],
        "max_abs_position_contracts":t["max_abs_position_contracts"],
        "max_ops_per_sec":v["max_ops_per_sec"],
        "resting_bid_ratio":v["resting_time_ratio"]["bid"],
        "resting_ask_ratio":v["resting_time_ratio"]["ask"],
    })
    cap=v.get("depth_cap_diagnostic",{})
    base.update({
        "depth_cap_checks":cap.get("quote_decision_checks",""),
        "depth_cap_changed":cap.get("price_adjustment_checks",""),
        "depth_cap_changed_fraction":cap.get("price_adjustment_fraction",""),
        "depth_cap_insufficient":cap.get("insufficient_depth_protection_checks",""),
        "depth_cap_insufficient_fraction":cap.get("insufficient_depth_fraction",""),
    })
    for h in HORIZONS:
        z=v["quote_quality"]["groups"]["all"]["horizons"][str(h)]
        primary=z["total_markout"]
        updated=z["total_markout_after_quote_update"]
        base[f"markout_{h}ms_bps"]=primary["mean_bps"] if primary else ""
        base[f"markout_{h}ms_count"]=z["total_fresh_count"]
        base[f"markout_{h}ms_future_quote_updated_bps"]=updated["mean_bps"] if updated else ""
        base[f"markout_{h}ms_future_quote_updated_count"]=z["future_quote_updated_count"]
    return base

def make_rows(found):
    rows=[]
    for key in sorted(expect_cases()):
        for model in (MODELS if key[2]=="base" else MODELS[:2]):
            rows.append(row_for(key,model,found[key]))
    # Derived per-day wins are paired against that SAME day and exact
    # base/cap configuration; these are not within-day causal matched fills.
    index={(r["symbol"],r["day"],r["case"],r["model"]):r for r in rows}
    for row in rows:
        if row["model"]=="glft" or row["status"]!="complete":
            continue
        base=index[(row["symbol"],row["day"],row["case"],"glft")]
        if base["status"]!="complete":
            continue
        diff=float(row["net_pnl_usdt"])-float(base["net_pnl_usdt"])
        row["delta_net_vs_glft_usdt"]=diff
        row["net_improved_vs_glft"]=int(diff>0)
        for h in HORIZONS:
            for suffix,name in (
                ("",""),
                ("_future_quote_updated","_updated"),
            ):
                col=f"markout_{h}ms{suffix}_bps"
                if row[col]=="" or base[col]=="":
                    continue
                delta=float(row[col])-float(base[col])
                row[f"delta_markout_{h}ms{name}_vs_glft_bps"]=delta
                row[f"markout_{h}ms{name}_improved_vs_glft"]=int(delta>0)
    return rows

def summarize_models(rows, symbol, days, case="base"):
    subset=[r for r in rows if r["symbol"]==symbol and r["day"] in days
            and r["case"]==case]
    out={}
    keys=("gross_pnl_usdt","gross_pnl_turnover_bps","net_pnl_usdt",
          "net_pnl_turnover_bps","maker_fee_usdt","fills","turnover_usdt",
          "position_std_contracts","max_abs_position_contracts",
          "max_ops_per_sec","resting_bid_ratio","resting_ask_ratio",
          "depth_cap_changed_fraction","depth_cap_insufficient_fraction")
    for model in (MODELS if case=="base" else MODELS[:2]):
        z=[r for r in subset if r["model"]==model and r["status"]=="complete"]
        item={"valid_days":len(z),"target_days":len(days)}
        for k in keys:
            item[k]=mean(r[k] for r in z if r[k]!="")
        for h in HORIZONS:
            item[f"markout_{h}ms_bps"]=mean(
                r[f"markout_{h}ms_bps"] for r in z if r[f"markout_{h}ms_bps"]!="")
            item[f"markout_{h}ms_future_quote_updated_bps"]=mean(
                r[f"markout_{h}ms_future_quote_updated_bps"] for r in z
                if r[f"markout_{h}ms_future_quote_updated_bps"]!="")
            item[f"markout_{h}ms_days_with_data"]=sum(
                r[f"markout_{h}ms_bps"]!="" for r in z)
        if case=="cap":
            item["depth_cap_total_decision_checks"]=sum(int(r["depth_cap_checks"]) for r in z)
            item["depth_cap_total_changed_checks"]=sum(int(r["depth_cap_changed"]) for r in z)
            item["depth_cap_total_insufficient_checks"]=sum(int(r["depth_cap_insufficient"]) for r in z)
            denom=item["depth_cap_total_decision_checks"]
            item["depth_cap_pooled_changed_fraction"]=(
                item["depth_cap_total_changed_checks"]/denom if denom else None)
            item["depth_cap_pooled_insufficient_fraction"]=(
                item["depth_cap_total_insufficient_checks"]/denom if denom else None)
        out[model]=item
    return out

def paired_net(rows, symbol, days, case="base"):
    index={(r["day"],r["model"]):r for r in rows if r["symbol"]==symbol
           and r["case"]==case and r["day"] in days and r["status"]=="complete"}
    result={}
    for model in (MODELS[1:] if case=="base" else ("both_1p0",)):
        values=[]
        daily={}
        for day in days:
            if (day,"glft") in index and (day,model) in index:
                diff=float(index[(day,model)]["net_pnl_usdt"])-float(index[(day,"glft")]["net_pnl_usdt"])
                values.append(diff);daily[day]=diff
        result[model]={"paired_days":len(values),"target_days":len(days),
                       "mean_net_delta_usdt":mean(values),
                       "positive_days":sum(x>0 for x in values),
                       "daily_net_delta_usdt":daily,
                       "daily_net_improved":{d:int(v>0) for d,v in daily.items()},
                       "strict_positive_fraction":sum(v>0 for v in values)/len(values) if values else None,
                       "nonpositive_days":sum(v<=0 for v in values)}
    return result

def paired_markout(rows, symbol, days, case="base"):
    """Same-day descriptive comparison of separately filled model portfolios."""
    index={(r["day"],r["model"]):r for r in rows
           if r["symbol"]==symbol and r["day"] in days
           and r["case"]==case and r["status"]=="complete"}
    result={}
    for model in (MODELS[1:] if case=="base" else ("both_1p0",)):
        horizons={}
        for h in HORIZONS:
            for suffix,name in (("","all_future_fresh"),
                                ("_future_quote_updated","future_quote_updated")):
                field=f"markout_{h}ms{suffix}_bps"
                pairs={}
                for day in days:
                    if (day,"glft") not in index or (day,model) not in index:
                        continue
                    x=index[(day,model)][field]
                    y=index[(day,"glft")][field]
                    if x=="" or y=="":
                        continue
                    pairs[day]=float(x)-float(y)
                values=list(pairs.values())
                horizons[f"{h}ms_{name}"]={
                    "paired_days":len(values),
                    "target_days":len(days),
                    "positive_days":sum(v>0 for v in values),
                    "nonpositive_days":sum(v<=0 for v in values),
                    "mean_delta_markout_bps":mean(values),
                    "daily_delta_markout_bps":pairs,
                    "note":"conditional fill-set comparison, not matched-fill causal proof",
                }
        result[model]=horizons
    return result

def calibration(found):
    out={}
    for symbol in SYMBOLS:
        days=[]
        for day in DAYS:
            r=found[(symbol,day,"base")]
            days.append({"day":day,"status":r["status"],
                         "wait_s":r.get("wait_s"),
                         "first_fit_ns":r.get("start"),
                         "error":r.get("error")})
        out[symbol]={
            "valid_days":sum(d["status"]=="complete" for d in days),
            "requested_days":6,"per_day":days,
            "no_successful_calibration_days":[d["day"] for d in days
                if "calibration" in (d.get("error") or "").lower()],
        }
    return out

def build_summary(found,rows):
    groups={
        "BTC_six_days":("BTC_USDT",DAYS,"base"),
        "BTC_first5_20level":("BTC_USDT",FIRST5,"base"),
        "BTC_Nov1_400initial_plus_deltas":("BTC_USDT",(DAYS[-1],),"base"),
        "BTC_first5_cap20":("BTC_USDT",FIRST5,"cap"),
        "SOL_six_days":("SOL_USDT",DAYS,"base"),
        "DOGE_six_days":("DOGE_USDT",DAYS,"base"),
        "ETH_six_days_optional":("ETH_USDT",DAYS,"base"),
    }
    output={}
    for name,(symbol,days,case) in groups.items():
        output[name]={
            "model_stats":summarize_models(rows,symbol,days,case),
            "paired_vs_glft":paired_net(rows,symbol,days,case),
            "paired_markout_vs_glft":paired_markout(rows,symbol,days,case),
            "source_days":list(days),"cap_depth":case=="cap",
            "all_requested_days_complete":all(
                found[(symbol,d,case)]["status"]=="complete" for d in days)
        }
    absent=[{"symbol":key[0],"day":key[1],"case":key[2],
             "status":val["status"],"error":val.get("error")}
            for key,val in sorted(found.items()) if val["status"]!="complete"]
    provenance=[]
    for key,item in sorted(found.items()):
        evidence=item.get("evidence")
        if item["status"]=="complete" and (
                not evidence or not evidence.get("json_sha256")):
            raise ValueError(f"Missing independently verified input digest/provenance: {key}")
        provenance.append({
            "symbol":key[0],"day":key[1],"case":key[2],
            "status":item["status"],**(evidence or {}),
        })
    return {
        "status":"complete" if not absent else "incomplete",
        "source":"actual full-day official hftbacktest 2.4.4 per-symbol artifacts",
        "never_modified_main_btc_baseline":True,
        "dates":list(DAYS),"symbols":list(SYMBOLS),
        "requested_cases":len(expect_cases()),
        "complete_cases":sum(v["status"]=="complete" for v in found.values()),
        "missing_or_failed_cases":absent,
        "source_run_ids":{"btc_and_depth_cap":SOURCE_RUN["BTC_USDT"][0],
                          "repaired_altcoins":SOURCE_RUN["SOL_USDT"][0]},
        "case_provenance":provenance,
        "fee":{"maker_rate":0.0002,"maker_bps":2.0,
               "assumption":"positive 2bps HBT fee, not certified historical exchange fee"},
        "pnl_bps_denominator":"official HBT two-sided trading_value_usdt (turnover), NOT initial equity",
        "markout_definition":"side*(future Gate midpoint - exact HBT execution price)/execution price*10000",
        "quote_clock":"local HBT fill observation -> Gate last quote with local_timestamp<=t+h; future quote max age 500ms",
        "quote_updated_subset":"last Gate quote used at horizon has local_timestamp strictly after local fill observation",
        "means":"per-day outcomes then equal-weight mean over completed dates; missing excluded, never zero",
        "calibration_wait_definition":"shared initial_fit_ns minus UTC 00:10:00; engine's own calibration_wait_ns is separately validated against HBT replay t0+600s",
        "resting_time_ratio_note":"local cancellable HBT order presence proxy, not exchange verified time-on-book",
        "cross_asset_inventory_units":"position std and max position are in each Gate contract unit, not directly comparable across symbols",
        "depth_note":"2026-11-01 initial ~400 L2 then native increments is NOT certified full deep queue for all day",
        "calibration":calibration(found),
        "groups":output,
    }

def write_csv(path,rows):
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open("w",newline="",encoding="utf-8") as f:
        writer=csv.DictWriter(f,fieldnames=list(rows[0]))
        writer.writeheader();writer.writerows(rows)

def render_report(data):
    names={"BTC_six_days":"BTC 六日等权",
           "BTC_first5_20level":"BTC 前5日20档快照",
           "BTC_Nov1_400initial_plus_deltas":"BTC 11月1日初始400档+增量",
           "BTC_first5_cap20":"BTC 前5日可见20档限价",
           "SOL_six_days":"SOL 六日","DOGE_six_days":"DOGE 六日",
           "ETH_six_days_optional":"ETH 六日（可选扩展）"}
    lines=[
        "# 论文第5章：Gate × Binance 多币种 GLFT 执行与深度稳健性",
        "",
        "**数据**：实际HBT全日回放，2026年6—11月每月1日。每个模型同日首次有效校准后同步入场。"
        "旧BTC主文模型/结果保持冻结，所有新结果仅用于第5章补充分析。",
        "",
        f"**完成进度：{data['complete_cases']}/{data['requested_cases']} 个独立日×品种×深度配置，状态 {data['status']}。**",
        "",
        "收益单位：USDT/日；bps以该模型当天双边成交名义额（turnover）为分母。",
        "Gross=含Maker手续费净损益+HBT手续费，Net=HBT含2bps Maker手续费期末盯市净损益。"
        "含期末未清仓库存盯市，未计资金费和主动日末平仓费用。",
        "100/500/1000ms Total Markout 采用阶段一已经验收的**成交价基准**，附未来报价确实更新的独立子集。",
        "下表每个数字先计算完整日指标，再按**独立有效日期等权**平均；NA表示无有效数据。",
        "",
        "| 数据分组 | 模型 | 完成日 | Gross USDT | Gross bps/turnover | Net USDT | Net bps/turnover | 成交/日 | Turnover USDT | 持仓标准差(张) | 最大绝对持仓(张) | max ops/s | 买/卖本地挂单在线率 |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"
    ]
    for name,blob in data["groups"].items():
        for model,z in blob["model_stats"].items():
            ratio=(f"{z['resting_bid_ratio']:.3f}/{z['resting_ask_ratio']:.3f}"
                   if z["resting_bid_ratio"] is not None and z["resting_ask_ratio"] is not None
                   else "NA")
            lines.append(f"| {names[name]} | {model} | {z['valid_days']}/{z['target_days']}"
                         f" | {fmt(z['gross_pnl_usdt'],2)} | {fmt(z['gross_pnl_turnover_bps'])} | {fmt(z['net_pnl_usdt'],2)}"
                         f" | {fmt(z['net_pnl_turnover_bps'])}"
                         f" | {fmt(z['fills'],1)} | {fmt(z['turnover_usdt'],1)}"
                         f" | {fmt(z['position_std_contracts'])}"
                         f" | {fmt(z['max_abs_position_contracts'],2)}"
                         f" | {fmt(z['max_ops_per_sec'],1)} | {ratio} |")
    lines.extend(["","## 成交后总Markout：未来报价500ms内有效，及真实后继更新子集","",
        "| 分组 | 模型 | 100ms | 100ms已更新 | 500ms | 500ms已更新 | 1000ms | 1000ms已更新 |",
        "|---|---|---:|---:|---:|---:|---:|---:|"])
    for name,blob in data["groups"].items():
        for model,z in blob["model_stats"].items():
            fields=[]
            for h in HORIZONS:
                fields.extend([fmt(z[f"markout_{h}ms_bps"]),
                               fmt(z[f"markout_{h}ms_future_quote_updated_bps"])])
            lines.append(f"| {names[name]} | {model} | "+" | ".join(fields)+" |")
    lines.extend(["","## 各品种日等权净收益改善（模型−GLFT）","",
        "| 分组 | 模型 | 可比较日 | 均值差 USDT/日 | 差值为正天数 |",
        "|---|---|---:|---:|---:|"])
    for name,blob in data["groups"].items():
        for model,z in blob["paired_vs_glft"].items():
            lines.append(f"| {names[name]} | {model} | {z['paired_days']}/{z['target_days']}"
                         f" | {fmt(z['mean_net_delta_usdt'],2)} | {z['positive_days']} |")
    lines.extend(["","## 逐日配对改善比例：同日策略与GLFT对照","",
        "只有严格正差计为改善。净收益比较已扣Maker 2bps；"
        "方向化Total Markout数值越大代表成交后价格越有利。",
        "Markout胜率来自不同成交集合的条件比较，不能据此宣称逐笔规避逆向选择的因果效果。",
        "",
        "| 数据分组 | 信号模型 | 含费净收益改善日 | 100ms Markout改善 | 100ms未来报价已更新改善 | 500ms改善 | 1000ms改善 |",
        "|---|---|---:|---:|---:|---:|---:|"])
    for name,blob in data["groups"].items():
        for model,by_h in blob["paired_markout_vs_glft"].items():
            p=blob["paired_vs_glft"][model]
            def w(h,scope):
                v=by_h[f"{h}ms_{scope}"]
                return f'{v["positive_days"]}/{v["paired_days"]}'
            lines.append(
                f'| {names[name]} | {model} | {p["positive_days"]}/{p["paired_days"]}'
                f' | {w(100,"all_future_fresh")}'
                f' | {w(100,"future_quote_updated")}'
                f' | {w(500,"all_future_fresh")}'
                f' | {w(1000,"all_future_fresh")} |')
    lines.extend(["","## 20档限制触发统计","",
        "| 模型 | 20档评价日 | 检查次数 | 实际截断次数 | 深度不足保护次数 | 触发实际截断比例(合计) | 深度不足保护比例(合计) |",
        "|---|---:|---:|---:|---:|---:|---:|"])
    for model,z in data["groups"]["BTC_first5_cap20"]["model_stats"].items():
        lines.append(f"| {model} | {z['valid_days']}/5"
                     f" | {z['depth_cap_total_decision_checks']}"
                     f" | {z['depth_cap_total_changed_checks']}"
                     f" | {z['depth_cap_total_insufficient_checks']}"
                     f" | {fmt(z['depth_cap_pooled_changed_fraction'])}"
                     f" | {fmt(z['depth_cap_pooled_insufficient_fraction'])} |")
    lines.extend(["","上表比例=所有已完成日触发次数/对应所有检查次数；JSON也保留逐日比例等权平均，二者分母不同。" 
                  "盘口不足20档时禁止新增两侧报价，并允许撤销存量订单；"
                  "若该对照日数据无法确认20档快照格式，整日失败并明确标出。","",
                  "## 冷启动覆盖与等待","",
                  "| 品种 | 完整日/6 | 逐日等待秒数（6/7/8/9/10/11月） |",
                  "|---|---:|---|"])
    for sym,v in data["calibration"].items():
        waits=[("NA" if x["wait_s"] is None else f"{x['wait_s']:.3f}")
               for x in v["per_day"]]
        lines.append(f"| {sym} | {v['valid_days']}/6 | "+", ".join(waits)+" |")
    if data["missing_or_failed_cases"]:
        lines.extend(["","## 缺失及失败情况（绝不填零）","",
                      "| 品种 | 日期 | 组别 | 状态 | 报错 |",
                      "|---|---|---|---|---|"])
        for x in data["missing_or_failed_cases"]:
            message=str(x["error"]).replace("|","/")[:300]
            lines.append(f"| {x['symbol']} | {x['day']} | {x['case']} | {x['status']} | {message} |")
    lines.extend(["","## 29组完整日来源与GitHub Actions Run ID","",
        "归档流程按固定Run ID下载官方原始artifacts；逐一检查文件夹名称、"
        "币种、日期、深度配置及SHA-256摘要。完整来源文件摘要在manifest.json。",
        "",
        "| 品种 | 日期 | 配置 | GitHub Actions Run ID | 归档文件名 |",
        "|---|---|---|---:|---|"])
    for item in data["case_provenance"]:
        run_id=item.get("run_id")
        link=(f'[#{run_id}]({item["run_url"]})' if run_id else "NA")
        lines.append(f'| {item["symbol"]} | {item["day"]} | {item["case"]} | {link}'
                     f' | `{item.get("artifact_name","NA")}` |')
    lines.extend(["","## 学术局限","",
        "六个月首日曾被用于模型选择/探索，**不能称为独立样本外验证**。"
        "不同模型成交集合不同，成交后Markout是条件描述性结果，不能推出规避逆向选择的因果成功次数。"
        "在模拟设定下Maker收2bps，仍缺实际逐时手续费等级、资金费、期末强制平仓、"
        "交易所真实订单队列和时钟误差独立校验。"
        "resting_time_ratio为本地确认可撤订单的**代理占比**，未证明交易所真实在线率。"
        "持仓标准差和最大库存均以该币种合约张数计量，不同币种应优先对比收益/成交额bps而非直接比张数。 "
        "11月400档仅证明初始快照档数较多，整日深档队列可观测性仍有限。",
        "",
        "输入逐日原始文件/任务失败信息均可在GitHub Actions artifacts复核。"
    ])
    return "\n".join(lines)+"\n"

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--root",required=True,type=Path)
    p.add_argument("--out",required=True,type=Path)
    args=p.parse_args()
    found=collect(args.root)
    rows=make_rows(found)
    summ=build_summary(found,rows)
    args.out.mkdir(parents=True,exist_ok=True)
    write_csv(args.out/"per_day_models.csv",rows)
    manifest={
        "status":summ["status"],
        "requested_cases":summ["requested_cases"],
        "complete_cases":summ["complete_cases"],
        "source_run_ids":summ["source_run_ids"],
        "cases":summ["case_provenance"],
        "input_frozen_stage2_commit":"92f9b96d12af2ffd9847a883e49f6804e47dc9b0",
        "official_full_day_minutes_per_case":1430,
        "old_btc_main_reports_modified":False,
        "old_failed_altcoin_replays_excluded":True,
        "source_explanation":"Run IDs derived from pinned workflow artifact downloads; SHA256 values hash each original archived daily JSON.",
    }
    (args.out/"manifest.json").write_text(
        json.dumps(manifest,ensure_ascii=False,indent=2,allow_nan=False)+"\n",
        encoding="utf-8")
    (args.out/"summary.json").write_text(
        json.dumps(summ,ensure_ascii=False,indent=2,allow_nan=False)+"\n",
        encoding="utf-8")
    (args.out/"report.md").write_text(render_report(summ),encoding="utf-8")
    print("CHAPTER5_AGGREGATE_STATUS",summ["status"],summ["complete_cases"],
          "/",summ["requested_cases"],flush=True)
    for x in summ["missing_or_failed_cases"]:
        print("CHAPTER5_MISSING_CASE",x,flush=True)
    return 0 if summ["status"]=="complete" else 3

if __name__=="__main__":
    raise SystemExit(main())
