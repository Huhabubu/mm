"""Recompute Stage5B on frozen official stage2 fills and stage3 GLFT shadow labels.

Input stage3_delivery.zip contains the six-day original-GLFT shadow category NPZ.
Input stage2_dir contains the six Gate native quote annotated zips, named
gate_stage2_verified_MM.zip. Stage4 summary JSON contains daily HBT ledger.
No strategy, order matching, future signal labels or live repository mutation.
"""
import argparse
import csv
import io
import json
from pathlib import Path
from zipfile import ZipFile
import numpy as np

DAYS=tuple(f"2026-{m:02d}-01" for m in range(6,12))
CATS=("threatened","inactive_or_no_effect","opposite")
HORIZONS=(100,500,1000)


def check_equal_stage2_stage3(c, f, day):
    """Require exact same HBT original GLFT fill order, exchange time and side."""
    for key in ("exchange_timestamp_ns","side"):
        if not np.array_equal(c[key],f[key]):
            raise ValueError(f"Stage2/3 same-fill mismatch on {day}: {key}")
    return c["category"]


def summarize_horizon(f, mask, day, cat, h):
    """Compare new execution-price total markout with frozen legacy decomposition.

    The main total calculation is independent of a stale entry midpoint.
    Legacy capture+drift is retained but uses its original stricter eligibility.
    """
    needed = (f"total_fresh_{h}ms", f"total_markout_{h}ms_bps",
              f"future_quote_updated_{h}ms")
    if any(key not in f for key in needed):
        raise ValueError(
            f"Stage2 annotations must be regenerated with total markout for {day}")
    total_ok = (mask & f[f"total_fresh_{h}ms"]
                & np.isfinite(f[f"total_markout_{h}ms_bps"]))
    old_ok = (mask & f[f"fresh_{h}ms"]
              & np.isfinite(f[f"markout_{h}ms_bps"])
              & np.isfinite(f["capture_halfspread_bps"]))
    if not total_ok.any() or not old_ok.any():
        raise ValueError(f"No valid Gate quote valuation for {day}, {cat}, {h}")
    total = f[f"total_markout_{h}ms_bps"][total_ok]
    capture = f["capture_halfspread_bps"][old_ok]
    mark = f[f"markout_{h}ms_bps"][old_ok]
    updated_ok = total_ok & f[f"future_quote_updated_{h}ms"]
    age = f["quote_age_at_observation_ms"]
    young = total_ok & (age >= 0) & (age < 100)
    middle = total_ok & (age >= 100) & (age <= 250)
    def mean_or_none(x):
        return float(np.mean(x)) if len(x) else None
    return {
        "day": day, "class": cat, "horizon_ms": h,
        "total_fills": int(mask.sum()),
        "total_fresh_fills": int(total_ok.sum()),
        "total_fresh_fraction": float(total_ok.sum()/mask.sum()) if mask.any() else None,
        "total_markout_bps": float(total.mean()),
        "total_markout_minus_maker2bps_bps": float(total.mean()-2.0),
        "updated_quote_fills": int(updated_ok.sum()),
        "updated_quote_total_markout_bps": mean_or_none(
            f[f"total_markout_{h}ms_bps"][updated_ok]),
        "entry_quote_lt100_fills": int(young.sum()),
        "entry_quote_lt100_total_markout_bps": mean_or_none(
            f[f"total_markout_{h}ms_bps"][young]),
        "entry_quote_100to250_fills": int(middle.sum()),
        "entry_quote_100to250_total_markout_bps": mean_or_none(
            f[f"total_markout_{h}ms_bps"][middle]),
        "fresh_fills": int(old_ok.sum()),
        "capture_bps": float(capture.mean()),
        "markout_bps": float(mark.mean()),  # legacy entry-mid drift
        "capture_plus_markout_bps": float((capture+mark).mean()),
        "short_horizon_maker_2bps_midmark": float((capture+mark).mean()-2.0),
    }


def calculate(stage3_zip,stage2_dir,stage4_summary,out_dir):
    out_dir.mkdir(parents=True,exist_ok=True)
    cases=[]
    s4=json.loads(stage4_summary.read_text(encoding="utf-8"))
    with ZipFile(stage3_zip) as z3:
        for day in DAYS:
            with np.load(io.BytesIO(z3.read(
              f"daily/{day}/{day}_stage3_classified.npz")),allow_pickle=False) as z:
                c={k:z[k].copy() for k in ("category","exchange_timestamp_ns","side")}
            with ZipFile(stage2_dir/f"gate_stage2_verified_{day[5:7]}.zip") as z2:
                with np.load(io.BytesIO(z2.read(f"{day}_glft_annotated.npz")),
                             allow_pickle=False) as z:
                    f={k:z[k].copy() for k in z.files}
            groups=check_equal_stage2_stage3(c,f,day)
            if len(groups)!=s4["daily_results"][day]["glft"]["fills"]:
                raise ValueError(f"GLFT official trading count mismatch on {day}")
            for cat in CATS:
                mask=groups==cat
                for h in HORIZONS:
                    cases.append(summarize_horizon(f, mask, day, cat, h))
    with (out_dir/"stage5b_same_fill_by_day.csv").open("w",newline="",encoding="utf-8") as fd:
        w=csv.DictWriter(fd,fieldnames=list(cases[0]));w.writeheader();w.writerows(cases)
    group_sixday={}
    for cat in CATS:
        group_sixday[cat]={}
        for h in HORIZONS:
            rr=[r for r in cases if r["class"]==cat and r["horizon_ms"]==h]
            assert len(rr)==6
            cols = ("total_markout_bps","total_markout_minus_maker2bps_bps",
                    "total_fresh_fills","total_fresh_fraction",
                    "updated_quote_total_markout_bps","updated_quote_fills",
                    "entry_quote_lt100_total_markout_bps","entry_quote_lt100_fills",
                    "entry_quote_100to250_total_markout_bps","entry_quote_100to250_fills",
                    "capture_bps","markout_bps","capture_plus_markout_bps",
                    "short_horizon_maker_2bps_midmark","fresh_fills")
            group_sixday[cat][str(h)]={
                col:(float(np.mean([x[col] for x in rr if x[col] is not None]))
                     if any(x[col] is not None for x in rr) else None)
                for col in cols
            }
    breakeven=[]
    for day in DAYS:
        for model,row in s4["daily_results"][day].items():
            fee=(row["zero_fee_pnl_usdt"]/row["notional_usdt"])*10000
            breakeven.append({"day":day,"model":model,"breakeven_maker_bps":fee})
    with (out_dir/"stage5b_break_even_by_day.csv").open("w",newline="",encoding="utf-8") as fd:
        w=csv.DictWriter(fd,fieldnames=list(breakeven[0]));w.writeheader();w.writerows(breakeven)
    fee_sixday={}
    for model,v in s4["equal_day_summary"].items():
        avg=v["day_equal_mean"]
        fee_sixday[model]={
            "day_equal_fee_bps":float(np.mean([
                x["breakeven_maker_bps"] for x in breakeven if x["model"]==model])),
            "pooled_nominal_fee_bps":10000*avg["zero_fee_pnl_usdt"]/avg["notional_usdt"]
        }
    out={
        "status":"complete","review_days":list(DAYS),
        "groups":group_sixday,"break_even_fee":fee_sixday,
        "primary": "execution-price total markout, 500ms/1000ms, future quote age <=500ms; 2bps maker-fee valuation",
        "legacy": "capture + entry-mid drift on original both-endpoint-fresh fill set",
        "warning":"Future midpoint is observed as-of local fill report plus horizon; execution-price total markout is a midprice valuation, not realized portfolio PnL; 2bps maker fee deducted only in explicitly named column; old capture+drift sample may differ; six historical days are exploratory",
    }
    (out_dir/"summary.json").write_text(json.dumps(out,indent=2,ensure_ascii=False)+"\n",encoding="utf-8")
    print("STAGE5B_SAME_FILL_COMPLETE",json.dumps(out["groups"],ensure_ascii=False))


if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--stage3-zip",type=Path,required=True)
    p.add_argument("--stage2-dir",type=Path,required=True)
    p.add_argument("--stage4-json",type=Path,required=True)
    p.add_argument("--out",type=Path,required=True)
    a=p.parse_args()
    calculate(a.stage3_zip,a.stage2_dir,a.stage4_json,a.out)
