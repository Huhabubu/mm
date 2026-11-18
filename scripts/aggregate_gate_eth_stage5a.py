"""Aggregate frozen ETH four-mode full-day HBT results as Stage5A evidence.

Uses immutable prior run files. No re-simulation or BTC parameter transfer.
This is a net-return execution-mode comparison, not a causal attribution.
"""
import argparse
import csv
import json
from pathlib import Path
from statistics import mean

DAYS = tuple(f"2026-{month:02d}-01" for month in range(6, 12))
MODES = ("glft", "both_1p0", "defensive_1p0", "risk_side_pause")
METRICS = {
    "fee_net_pnl_usdt": "end_net_marked_equity_usdt",
    "maker_fee_usdt": "fee_usdt",
    "fills": "engine_fills",
    "rms_inventory": "rms_inventory_contracts",
    "mean_abs_inventory": "mean_abs_inventory_contracts",
    "notional_usdt": "trading_value_usdt",
}


def aggregate(root, out):
    rows = []
    for day in DAYS:
        case = root / ("gate-eth-" + day) / "full_day" / (day + "_1430m.json")
        r = json.loads(case.read_text(encoding="utf-8"))
        if (r.get("status") != "complete"
                or r.get("gate_symbol") != "ETH_USDT"
                or r.get("binance_symbol") != "ETHUSDT"
                or r.get("minutes_traded") != 1430
                or r.get("binance_extra_visible_delay_ms") != 2.5
                or abs(r["scenario"]["maker_fee_rate"] - .0002) > 1e-12
                or set(r["variants"]) != set(MODES)):
            raise ValueError("Non-ETH or incomplete frozen case: " + day)
        for mode in MODES:
            t = r["variants"][mode]["trading"]
            row = {"day": day, "model": mode}
            row.update({label: float(t[key]) for label, key in METRICS.items()})
            row["diagnostic_fee_free_pnl_usdt"] = row["fee_net_pnl_usdt"] + row["maker_fee_usdt"]
            if row["maker_fee_usdt"] < 0 or row["fills"] <= 0:
                raise ValueError("Invalid frozen fee/fill: " + day + " " + mode)
            rows.append(row)
    day_equal = {
        mode: {
            key: mean(row[key] for row in rows if row["model"] == mode)
            for key in (*METRICS, "diagnostic_fee_free_pnl_usdt")
        }
        for mode in MODES
    }
    result = {
        "status": "complete",
        "gate_symbol": "ETH_USDT",
        "source": "Huhabubu/mm GitHub Actions #44114624542 immutable official-HBT four-model six-day archive",
        "evaluation_days": list(DAYS),
        "no_gamma_sigma_beta_changes": True,
        "day_equal_mean": day_equal,
        "caution": "All days previously explored; fee savings and position-path changes require Stage2-4 mechanism audits",
    }
    out.mkdir(parents=True, exist_ok=True)
    (out / "summary.json").write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    with (out / "per_day.csv").open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print("GATE_ETH_STAGE5A_FROZEN_6_OF_6", json.dumps(day_equal, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--root", required=True, type=Path)
    p.add_argument("--out", required=True, type=Path)
    a = p.parse_args()
    aggregate(a.root, a.out)
