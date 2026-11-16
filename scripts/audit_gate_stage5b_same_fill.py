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
                    eligible=(mask & f[f"fresh_{h}ms"]
                            & np.isfinite(f[f"markout_{h}ms_bps"])
                            & np.isfinite(f["capture_halfspread_bps"]))
                    capture=f["capture_halfspread_bps"][eligible]
                    mark=f[f"markout_{h}ms_bps"][eligible]
                    if not len(capture):
                        raise ValueError(f"No fresh Gate native quotes on {day}, {cat}, {h}")
                    cases.append({
                        "day":day,"class":cat,"horizon_ms":h,
                        "total_fills":int(mask.sum()),"fresh_fills":int(eligible.sum()),
                        "capture_bps":float(capture.mean()),
                        "markout_bps":float(mark.mean()),
                        "capture_plus_markout_bps":float((capture+mark).mean()),
                        "short_horizon_maker_2bps_midmark":float((capture+mark).mean()-2.0),
                    })
    with (out_dir/"stage5b_same_fill_by_day.csv").open("w",newline="",encoding="utf-8") as fd:
        w=csv.DictWriter(fd,fieldnames=list(cases[0]));w.writeheader();w.writerows(cases)
    group_sixday={}
    for cat in CATS:
        group_sixday[cat]={}
        for h in HORIZONS:
            rr=[r for r in cases if r["class"]==cat and r["horizon_ms"]==h]
            assert len(rr)==6
            group_sixday[cat][str(h)]={
                col:float(np.mean([x[col] for x in rr]))
                for col in ("capture_bps","markout_bps","capture_plus_markout_bps",
                            "short_horizon_maker_2bps_midmark","fresh_fills")
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
        "warning":"Each quote valuation is at local fill-report as-of time; short-horizon midmark is not full-portfolio PnL; fixed 2bp maker cost; historic six days previously explored",
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
