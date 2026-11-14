"""Audit why the one allowed Z0.75 did or did not change effective gates.

Use only already explored 09/10/11 SOL V5 20min pretrade case JSON.
No raw Tardis, no out-of-sample dates, and no new threshold search.
"""
import argparse
import json
from pathlib import Path

DAYS=("2026-09-01","2026-10-01","2026-11-01")


def run(root):
    cases={}
    for p in Path(root).rglob("case.json"):
        obj=json.loads(p.read_text())
        if obj.get("day") not in DAYS or obj.get("coin")!="SOL" or obj.get("venue")!="okex-swap":
            raise ValueError("Only known SOL OKX pure Tokyo cases")
        if obj["day"] in cases:raise ValueError("duplicate pretrade day")
        if float(obj.get("pretrade_minutes",0))!=20.:
            raise ValueError("20min pretrade not the matched four-hour warmup")
        cases[obj["day"]]=obj
    if set(cases)!=set(DAYS):
        raise ValueError("Need all three exact dates for Z floor audit")
    items=[]
    for day in DAYS:
        x=cases[day]["variants"]["z1_cancel_qs"]["thresholds"]
        sigma=float(x["sigma_100ms_bps"])
        floor=float(x["floor_bps"])
        z1=[max(floor,1.5*sigma),max(floor,1.*sigma)]
        z075=[max(floor,1.125*sigma),max(floor,.75*sigma)]
        recorded=[float(x["gap_bps"]),float(x["impulse_bps"])]
        if max(abs(a-b) for a,b in zip(z1,recorded))>1e-9:
            raise AssertionError("Historical same-warmup Z1 gates mismatch formula")
        items.append({
            "day":day,"pretrade_sigma_100ms_bps":sigma,"floor_bps":floor,
            "z1_gap_impulse_bps":z1,
            "z075_gap_impulse_bps":z075,
            "both_effective_gates_identical":all(abs(a-b)<1e-12 for a,b in zip(z1,z075))
        })
    return {"days":items,"same_three_day_effective_gates":all(
                x["both_effective_gates_identical"] for x in items),
            "method":"pretrade-only sigma and precommitted max(.3bps, one target tick) floor",
            "scope":"3 explored post-migration SOL first days, 20 minute warmup only",
            "cannot_substitute_raw_HBT_order_response_audit":True}


def markdown(result):
    rows=["# SOL 纯东京 Z1 vs Z0.75 预热门槛下限核对", "",
          "现有自适应价格门槛 = max(波动率系数×预热100ms回报标准差, max(0.3bps,目标一tick对应bps))。",
          "|日期|预热100ms σ(bps)|硬下限(bps)|Z1 gap/impulse|Z0.75 gap/impulse|两档有效门槛相同|",
          "|---|---:|---:|---|---|---|"]
    for x in result["days"]:
        a=x["z1_gap_impulse_bps"];b=x["z075_gap_impulse_bps"]
        rows.append(f"|{x['day']}|{x['pretrade_sigma_100ms_bps']:.4f}|{x['floor_bps']:.4f}|{a[0]:.4f}/{a[1]:.4f}|{b[0]:.4f}/{b[1]:.4f}|{x['both_effective_gates_identical']}|")
    rows.extend(["",f"三天有效门槛都相同：{result['same_three_day_effective_gates']}。",
                 "若三天有效门槛均相同且对应HBT执行轨迹也一致，Z0.75没有新增独立防御能力；"
                 "按预先限定的最多3档规则本轮停止探索，不继续调低硬下限。"])
    return "\n".join(rows)+"\n"


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--source",required=True)
    p.add_argument("--out",required=True)
    a=p.parse_args()
    obj=run(a.source)
    dest=Path(a.out);dest.mkdir(parents=True,exist_ok=True)
    (dest/"tokyo_z075_effective_gates.json").write_text(
        json.dumps(obj,indent=2,ensure_ascii=False,allow_nan=False))
    (dest/"tokyo_z075_effective_gates.md").write_text(markdown(obj))
    print(markdown(obj))


if __name__=="__main__":
    main()
