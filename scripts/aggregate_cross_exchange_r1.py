"""R1 cross-venue, per-day comparable unit metrics and exact accounting.

Read only newly supplied per-run JSON. Never modify frozen Chapter 5 results.
All three Maker fee scenarios reprice the SAME HBT trade path and turnover;
they are static maps, not separate queue/strategy simulations.
"""
import argparse
import csv
import json
import math
from pathlib import Path

SCENARIOS=(0.,-0.5,2.0)
HORIZONS=(500,1000)


def number(x, label):
    if x is None or not math.isfinite(float(x)):
        raise ValueError("Missing/nonfinite "+label)
    return float(x)


def model_row(report, model, venue, path=""):
    is_bitget="models" in report
    v=report["models" if is_bitget else "variants"][model]
    t=v["trading"]
    spec=report["scenario"]
    qty=number(spec["qty"],"order_qty")
    ctval=number(spec["ctval"],"contract_value")
    base_order_qty=qty*ctval
    if base_order_qty<=0:
        raise ValueError("Nonpositive order_qty")
    turnover=number(t["trading_value_usdt"],"turnover")
    fee=number(t["fee_usdt"],"fee")
    net=number(t["end_net_marked_equity_usdt"],"net")
    gross=net+fee
    if turnover<=0:
        raise ValueError("Missing positive turnover for gross_bps")
    gross_bps=10000*gross/turnover
    if abs(fee-turnover*number(spec["maker_fee_rate"],"maker_fee_rate"))>1e-6:
        raise ValueError("Fee ledger / trading_value mismatch")
    std=number(t["position_std_base"] if is_bitget else
               t["position_std_contracts"]*ctval,"position std")
    if std<0:
        raise ValueError("Negative position std")
    symbol=report["symbol"] if is_bitget else report["gate_symbol"]
    out={"venue":venue,"symbol":symbol,"day":report["day"],
         "model":model,"source":str(path),
         "turnover_usdt":turnover,"fee_usdt":fee,"net_usdt":net,
         "gross_usdt":gross,"gross_bps":gross_bps,
         "pos_std_base":std,"order_qty_base":base_order_qty,
         "pos_std_over_order_qty":std/base_order_qty,
         "net_fee_0bps_usdt":gross,
         "net_fee_minus_0p5bps_usdt":gross+turnover*.5/10000,
         "net_fee_plus_2bps_usdt":gross-turnover*2/10000,
         "net_delta_vs_glft_usdt":None,
         "fee_saved_usdt":None,"vol_shrink_gross_usdt":None,
         "quality_gross_usdt":None,"decomposition_residual_usdt":None}
    if is_bitget:
        q=report.get("gate_base_quantity_reference",{})
        out.update({"gate_target_qty_base":q.get("gate_target_qty_base"),
                    "bitget_actual_qty_base":q.get("bitget_actual_qty_base"),
                    "qty_signed_difference_base":q.get("quantity_signed_difference_base"),
                    "nominal_difference_at_gate_mid_usdt":q.get(
                        "nominal_difference_at_gate_reference_usdt"),
                    "position_cap_over_order_qty":q.get(
                        "position_cap_over_order_qty_bitget")})
    else:
        out.update({"gate_target_qty_base":base_order_qty,
                    "bitget_actual_qty_base":None,
                    "qty_signed_difference_base":None,
                    "nominal_difference_at_gate_mid_usdt":None,
                    "position_cap_over_order_qty":number(
                        spec["position_cap"],"position_cap")/qty})
    for h in HORIZONS:
        z=v["quote_quality"]["groups"]["all"]["horizons"][str(h)]
        mark=z.get("total_markout")
        out[f"total_markout_{h}ms_bps"]=(
            float(mark["mean_bps"]) if mark and mark.get("mean_bps") is not None else None)
        out[f"total_markout_{h}ms_n"]=z["total_fresh_count"]
    return out


def decompose(base, candidate):
    """Net improvement of candidate over same-day glft; all terms in USDT."""
    if (base["venue"],base["symbol"],base["day"])!=(
            candidate["venue"],candidate["symbol"],candidate["day"]):
        raise ValueError("Reference and candidate must share venue/symbol/day")
    vb=base["turnover_usdt"];vm=candidate["turnover_usdt"]
    gb=base["gross_bps"];gm=candidate["gross_bps"]
    fee_saved=base["fee_usdt"]-candidate["fee_usdt"]
    vol_shrink=(vb-vm)*(-gb/10000)
    quality=vm*(gm-gb)/10000
    delta=candidate["net_usdt"]-base["net_usdt"]
    residual=delta-(fee_saved+vol_shrink+quality)
    if abs(residual)>1e-8*max(1,abs(delta)):
        raise ValueError(f"Net USDT identity does not close: {residual}")
    return {"net_delta_vs_glft_usdt":delta,
            "fee_saved_usdt":fee_saved,
            "vol_shrink_gross_usdt":vol_shrink,
            "quality_gross_usdt":quality,
            "decomposition_residual_usdt":residual}


def aggregate_reports(reports):
    """Input is (report, source_name), optionally across many independent days."""
    rows=[]
    for report,path in reports:
        if report.get("status")!="complete":
            continue
        variants=report.get("models",report.get("variants"))
        if not variants or "glft" not in variants:
            continue
        venue=report.get("venue","gate-io-futures")
        if "models" in report and venue!="bitget-futures":
            raise ValueError("Bitget models with wrong venue")
        for name in variants:
            rows.append(model_row(report,name,venue,path))
    index={}
    for r in rows:
        key=(r["venue"],r["symbol"],r["day"],r["model"])
        if key in index:
            raise ValueError("Duplicate model day "+str(key))
        index[key]=r
    for r in rows:
        if r["model"]=="glft":
            continue
        key=(r["venue"],r["symbol"],r["day"],"glft")
        r.update(decompose(index[key],r))
    return sorted(rows,key=lambda r:(r["venue"],r["symbol"],r["day"],r["model"]))


def independent_day_means(rows):
    keys=("gross_bps","pos_std_base","pos_std_over_order_qty",
          "total_markout_500ms_bps","total_markout_1000ms_bps",
          "net_usdt","net_delta_vs_glft_usdt","fee_saved_usdt",
          "vol_shrink_gross_usdt","quality_gross_usdt",
          "net_fee_0bps_usdt","net_fee_minus_0p5bps_usdt","net_fee_plus_2bps_usdt")
    grouped={}
    for r in rows:
        grouped.setdefault((r["venue"],r["symbol"],r["model"]),[]).append(r)
    out=[]
    for key,items in sorted(grouped.items()):
        row={"venue":key[0],"symbol":key[1],"model":key[2],
             "valid_days":len({r["day"] for r in items})}
        for k in keys:
            values=[r[k] for r in items if r[k] is not None]
            row[k]=sum(values)/len(values) if values else None
        out.append(row)
    return out


def write(root, output):
    reports=[]
    for path in Path(root).rglob("*.json"):
        try:
            d=json.loads(path.read_text(encoding="utf-8"))
        except (UnicodeError, json.JSONDecodeError):
            continue
        if (d.get("status")=="complete"
                and ("models" in d or "variants" in d)
                and "scenario" in d):
            reports.append((d,str(path)))
    if not reports:
        raise ValueError("No completed compatible HBT reports")
    rows=aggregate_reports(reports)
    output=Path(output);output.mkdir(parents=True,exist_ok=True)
    if not rows:
        raise ValueError("No valid model rows")
    for name,table in (("per_day_unit_metrics.csv",rows),
                       ("independent_day_means.csv",independent_day_means(rows))):
        with (output/name).open("w",newline="",encoding="utf-8") as file:
            writer=csv.DictWriter(file,fieldnames=list(table[0]))
            writer.writeheader();writer.writerows(table)
    (output/"r1_summary.json").write_text(json.dumps(
        {"rows":rows,"independent_day_means":independent_day_means(rows),
         "maker_bps_scenarios":[0,-0.5,2],
         "fee_scenario_method":"static repricing of same actual HBT fills",
         "accounting":"net_delta=fee_saved+vol_shrink_gross+quality_gross",
         "markout_method":"execution-price Total Markout, available HBT fills only"},
        ensure_ascii=False,indent=2,allow_nan=False)+"\n",encoding="utf-8")
    print("R1_UNIT_METRICS_ROWS",len(rows),flush=True)
    return rows


if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--root",required=True)
    p.add_argument("--out",required=True)
    a=p.parse_args()
    write(a.root,a.out)
