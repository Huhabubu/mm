"""Three-pre vs three-post calendar partition of archived V5 and V6 cases.

Strictly observed 50-minute windows. No recalibration, no pre/post pool leakage.
All monetary 'gross' means official HBT marked net equity plus Maker fees.
"""
import argparse
import json
from pathlib import Path
from collections import defaultdict
import numpy as np

DAYS = ("2026-06-01", "2026-07-01", "2026-08-01",
        "2026-09-01", "2026-10-01", "2026-11-01")
MARKETS = (("SOL", "okex-swap"), ("DOGE", "okex-swap"),
           ("1000PEPE", "okex-swap"), ("1000PEPE", "bitget-futures"))
GROUPS = {"Group_A": "Group_A_active_or_200ms_post_cancel",
          "Kept": "Kept",
          "Group_B": "Group_B_no_local_defense_or_cancel_tail",
          "Dropped_all": "Dropped_all",
          "Added": "Added"}
HORIZONS = (100, 500)


def load_cases(root, distinct_source):
    cases = {}
    for path in Path(root).rglob("case.json"):
        x = json.loads(path.read_text())
        if "day" not in x or "coin" not in x or "venue" not in x:
            continue
        k = (x["day"], x["coin"], x["venue"])
        if k in cases:
            raise ValueError(f"Repeated {distinct_source} case {k}: {path}")
        cases[k] = x
    return cases


def pooled_horizon(rows, group, h):
    pairs = [(r["source_vs_G0_fill_groups_exchange_clock"][GROUPS[group]][f"{h}ms"]["fresh_n"],
              r["source_vs_G0_fill_groups_exchange_clock"][GROUPS[group]][f"{h}ms"]["markout_mean_bps"])
             for r in rows]
    n = sum(v[0] for v in pairs)
    daily = [m for _, m in pairs if m is not None]
    return {"fresh_n": n,
            "pooled_mean_bps": sum(i*m for i, m in pairs if m is not None)/n if n else None,
            "equal_day_mean_bps": float(np.mean(daily)) if daily else None,
            "valid_days_for_equal_day": len(daily)}


def by_period(rows):
    daycount = len(rows)
    v5 = [a for a,b in rows]
    v6 = [b for a,b in rows]
    assert daycount == 3
    for a,b in rows:
        x, y = a["variants"]["g0"]["trading"], a["variants"]["z1_cancel_qs"]["trading"]
        if b["g0"]["fills"] != x["engine_fills"] or b["source"]["fills"] != y["engine_fills"]:
            raise AssertionError("V5/V6 fill count mismatch (G0 or source)")
        if abs(b["g0"]["fee_included_net_usdt"] - x["end_net_marked_equity_usdt"]) > 1e-4:
            raise AssertionError("V5/V6 baseline marked net mismatch")
        if abs(b["source"]["fee_included_net_usdt"] - y["end_net_marked_equity_usdt"]) > 1e-4:
            raise AssertionError("V5/V6 defense marked net mismatch")

    gt = [a["variants"]["g0"]["trading"] for a in v5]
    st = [a["variants"]["z1_cancel_qs"]["trading"] for a in v5]
    base_count = sum(a["engine_fills"] for a in gt)
    source_count = sum(a["engine_fills"] for a in st)
    metrics = {}
    for tag, trs in (("g0", gt), ("z1_cancel_qs", st)):
        net = sum(a["end_net_marked_equity_usdt"] for a in trs)
        fee = sum(a["fee_usdt"] for a in trs)
        metrics[tag] = {
            "total_fills": sum(a["engine_fills"] for a in trs),
            "mean_daily_fills": sum(a["engine_fills"] for a in trs)/daycount,
            "gross_marked_USDT_total": net + fee,
            "gross_marked_USDT_daily_equal_mean": (net + fee)/daycount,
            "net_after_2bps_USDT_total": net,
            "net_after_2bps_USDT_daily_equal_mean": net/daycount,
            "fee_USDT_total": fee,
            "cancel_requests_total": sum(a["cancel_requests"] for a in trs),
            "submissions_total": sum(a["submissions"] for a in trs)
        }
    metrics["fill_retention_total_ratio"] = source_count/base_count if base_count else None
    metrics["defense_active_fraction_equal_day_mean"] = float(np.mean([
        a["variants"]["z1_cancel_qs"]["defense_duty"]["active_fraction_50min"]
        for a in v5]))
    grouped = {}
    for name, key in GROUPS.items():
        grouped[name] = {
            "count_total": sum(r["source_vs_G0_fill_groups_exchange_clock"][key]["count"] for r in v6),
            **{f"{h}ms": pooled_horizon(v6, name, h) for h in HORIZONS}
        }
    for i,r in enumerate(v6):
        a = r["source_vs_G0_fill_groups_exchange_clock"]
        if a["Group_A_active_or_200ms_post_cancel"]["count"] + a["Group_B_no_local_defense_or_cancel_tail"]["count"] != a["Dropped_all"]["count"]:
            raise AssertionError("Group A/B does not partition dropped")
    selectivity = {}
    for h in HORIZONS:
        keep, drop = grouped["Kept"][f"{h}ms"], grouped["Dropped_all"][f"{h}ms"]
        selectivity[f"{h}ms"] = {
            "pooled_kept_minus_all_dropped_bps": (keep["pooled_mean_bps"] - drop["pooled_mean_bps"]
                if keep["pooled_mean_bps"] is not None and drop["pooled_mean_bps"] is not None else None),
            "daily_lift_bps": [
                (r["source_vs_G0_fill_groups_exchange_clock"]["Kept"][f"{h}ms"]["markout_mean_bps"] -
                 r["source_vs_G0_fill_groups_exchange_clock"]["Dropped_all"][f"{h}ms"]["markout_mean_bps"])
                if (r["source_vs_G0_fill_groups_exchange_clock"]["Kept"][f"{h}ms"]["markout_mean_bps"] is not None and
                    r["source_vs_G0_fill_groups_exchange_clock"]["Dropped_all"][f"{h}ms"]["markout_mean_bps"] is not None)
                else None for r in v6]
        }
    for h in HORIZONS:
        entry = selectivity[f"{h}ms"]
        daily_valid = [v for v in entry["daily_lift_bps"] if v is not None]
        entry["equal_day_mean_lift_bps"] = float(np.mean(daily_valid)) if daily_valid else None
        entry["valid_days_for_equal_day_lift"] = len(daily_valid)
    shifted = [r["time_shift_60s"]["strategy"] for r in v6]
    metrics["shift60_observational"] = {
        "source_total_fills": sum(r["source"]["fills"] for r in v6),
        "shift_total_fills": sum(r["fills"] for r in shifted),
        "source_total_net_after_2bps_USDT": sum(r["source"]["fee_included_net_usdt"] for r in v6),
        "shift_total_net_after_2bps_USDT": sum(r["fee_included_net_usdt"] for r in shifted),
        "shift_episodes_retained_fraction_per_day": [
            r["time_shift_60s"]["window_exposure_equality"]["active_fraction_preserved"] for r in v6],
        "shift_is_strict_equal_exposure": all(
            r["time_shift_60s"]["window_exposure_equality"]["exact_50min_full_exposure_matched"]
            for r in v6)
    }
    return {"days": [a["day"] for a in v5], "economics": metrics,
            "group_markouts": grouped, "selectivity_lift": selectivity}


def run(v5root, v6root, solroot):
    five = load_cases(v5root, "V5")
    six = load_cases(v6root, "new V6")
    six.update(load_cases(solroot, "stored SOL V6"))
    expected = {(d,c,v) for d in DAYS for c,v in MARKETS}
    if set(five) != expected or set(six) != expected:
        raise ValueError(f"Coverage incomplete V5 missing={sorted(expected-set(five))}; V6 missing={sorted(expected-set(six))}; excess={sorted((set(five)|set(six))-expected)}")
    out = {}
    for coin,venue in MARKETS:
        market = f"{coin}_{venue}"
        out[market] = {}
        for title, days in (("pre", DAYS[:3]), ("post", DAYS[3:])):
            out[market][title] = by_period([(five[(d,coin,venue)], six[(d,coin,venue)]) for d in days])
    return {"markets": out, "pre_dates": DAYS[:3], "post_dates": DAYS[3:],
            "group_clock": "V6 exchange_fill_time vs local defense action time; both not independently clock corrected",
            "gross_definition": "official HBT marked net equity + official 2bps Maker fees",
            "limitations": [
                "V5 50-minute sampled days, not 24h and not strict untouched holdout",
                "Only three independent UTC first-of-month dates per group",
                "Group A = same-side defense-active or 200ms after local cancel request; ACK not proved",
                "Group B can include queue/path-dependent dropped fills",
                "60s shifted replay does not preserve the last minute of defense; unequal exposure can remain",
                "Fees included in ALL net PnL; gross is merely diagnostic",
                "Instrument specifications are not independently archived for every historical date"
            ]}


def markdown(x):
    L = ["# Pre 06/07/08 vs Post 09/10/11：V5/V6 防御分组审计", "",
         "每组3个独立日期、每天50分钟。毛盯市权益=含2bps Maker费用净权益+官方费用；净权益始终扣费。以下USDT为每日期等权均值。", "",
         "|市场|时期|G0成交|G0毛USDT/日|G0净USDT/日|Z1防御激活|成交保留率|Z1撤单-G0撤单|",
         "|---|---|---:|---:|---:|---:|---:|---:|"]
    for m,pr in x["markets"].items():
        for name,r in pr.items():
            e=r["economics"]; g=e["g0"]; z=e["z1_cancel_qs"]
            L.append(f"|{m}|{name}|{g['total_fills']}|{g['gross_marked_USDT_daily_equal_mean']:+.4f}|{g['net_after_2bps_USDT_daily_equal_mean']:+.4f}|{e['defense_active_fraction_equal_day_mean']:.2%}|{e['fill_retention_total_ratio']:.2%}|{z['cancel_requests_total']-g['cancel_requests_total']:+d}|")
    L.extend(["", "## Group A（窗内）／Kept／Group B（窗外）Markout", "",
              "单位bps，负值越大代表成交后价格越不利。Group A/Kept/B 三组Markout先展示按合格成交笔数加权的pooled结果；另同时保留按日期等权的group Markout在JSON中。",
              "成交时间和方向一对一配对是观察性分组，Group A按本地防御/撤单时间窗分类，未证实交易所撤单确认。", "",
              "|市场|时期|A笔数/100ms/500ms pooled|Kept笔数/100ms/500ms pooled|B笔数/100ms/500ms pooled|Overall Lift pooled100/500ms|Overall Lift 按日等权100/500ms|",
              "|---|---|---|---|---|---|---|"])
    def f(group,h):
        t=group[f"{h}ms"]["pooled_mean_bps"]
        return "NA" if t is None else f"{t:+.3f}"
    for m,pr in x["markets"].items():
        for name,r in pr.items():
            z=r["group_markouts"]; sl=r["selectivity_lift"]
            def cell(k):
                return f"{z[k]['count_total']}/{f(z[k],100)}/{f(z[k],500)}"
            def lift(h):
                v=sl[f"{h}ms"]["pooled_kept_minus_all_dropped_bps"]
                return "NA" if v is None else f"{v:+.3f}"
            def lift_equal(h):
                v=sl[f"{h}ms"]["equal_day_mean_lift_bps"]
                return "NA" if v is None else f"{v:+.3f}"
            L.append(f"|{m}|{name}|{cell('Group_A')}|{cell('Kept')}|{cell('Group_B')}|{lift(100)}/{lift(500)}|{lift_equal(100)}/{lift_equal(500)}|")
    L.extend(["", "## +60s 回顾性时移对照", "",
              "同一防御脉冲后移60秒、不循环回绕，末一分钟可能损失部分脉冲；Source与Shift成交规模可能不同。", "",
              "|市场|时期|Source成交/Shift成交|Source净USDT/3日|Shift净USDT/3日|计划曝光全相等|",
              "|---|---|---:|---:|---:|---|"])
    for m,pr in x["markets"].items():
        for name,r in pr.items():
            s=r["economics"]["shift60_observational"]
            L.append(f"|{m}|{name}|{s['source_total_fills']}/{s['shift_total_fills']}|{s['source_total_net_after_2bps_USDT']:+.4f}|{s['shift_total_net_after_2bps_USDT']:+.4f}|{'是' if s['shift_is_strict_equal_exposure'] else '否'}|")
    L.extend(["", "### 解释边界", "",
              "这是前后各三个已被探索的月首日描述性对照，无法独立识别8月4日迁移的因果效应；少成交、省费用、队列重置和后续库存路径必须单独区分。",
              "Selectivity Lift = Kept均值−全部Dropped均值。pooled先在各组按合格成交数量加权后做差；按日等权先做每日期Kept−Dropped差，再对日期做算术平均。不同加权产生的符号变化必须同时呈现。",
              "不能只拿窗内Group A与Kept比较后声称整体获益。"])
    return "\n".join(L)+"\n"


if __name__ == "__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--v5",required=True)
    p.add_argument("--v6-new",required=True)
    p.add_argument("--sol-stored",default="reports/source_defense/v6_mechanism_cases/SOL")
    p.add_argument("--out",required=True)
    q=p.parse_args()
    out=Path(q.out);out.mkdir(parents=True,exist_ok=True)
    obj=run(q.v5,q.v6_new,q.sol_stored)
    (out/"migration_v5v6_groups.json").write_text(json.dumps(obj,indent=2,ensure_ascii=False,allow_nan=False))
    (out/"migration_v5v6_groups.md").write_text(markdown(obj))
    print(markdown(obj))
