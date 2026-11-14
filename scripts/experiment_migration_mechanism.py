"""V6 time-window mechanism for the missing DOGE and PEPE (OKX/Bitget) dates.

Reuses the official HBT engine, same fixed Z1 QS and 2.5ms scenario as V5.
The source/shift replay is observational, with local-action/exchange-fill clock caveat.
"""
import argparse
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import numpy as np

from experiment_source_defense_v5_multiday import (
    prepared, prior_from_stored_admission, MONTH_DAYS
)
from experiment_source_defense_v6_mechanism import (
    model_run, groups, short_metrics, clamp_shift
)

MARKETS = (("DOGE", "okex-swap"),
           ("1000PEPE", "okex-swap"),
           ("1000PEPE", "bitget-futures"))


def run(day, coin, venue, out, admission):
    if day not in MONTH_DAYS or (coin, venue) not in MARKETS:
        raise ValueError("Only missing V6 DOGE/PEPE cases admitted")
    prior, meta = (prior_from_stored_admission(admission, coin, venue, day)
                   if coin == "DOGE" else (np.nan, {"fit_kind": "original_5plus"}))
    with TemporaryDirectory(prefix="migration_mech_") as directory:
        data = prepared(day, coin, venue, Path(directory), with_trades=False)
        g0, gfill, ge = model_run(data, "g0", prior)
        source, sfill, se = model_run(data, "source", prior)
        start, end, side, _, _ = se
        stop_ns = int(source["start_time_ns"]) + (data["pretrade_s"]+3000)*1_000_000_000
        ss, ee, side2, exposure = clamp_shift(start, end, side, stop_ns)
        shift, xfill, xe = model_run(data, "shift60", prior, lagged=(ss, ee, side2))
        report = {
            "day": day, "coin": coin, "venue": venue,
            "pretrade_minutes": data["pretrade_s"]/60,
            "trade_window_UTC": data["trade_start_utc"]+"–"+data["trade_end_utc"],
            "initial_calibration_prior": meta,
            "HBT_official": True, "maker_fee_bps": 2.,
            "extra_binance_delay_ms": 2.5,
            "g0": short_metrics(g0, gfill, ge, True),
            "source": short_metrics(source, sfill, se, False),
            "source_vs_G0_fill_groups_exchange_clock": groups(gfill, sfill, se, "exchange"),
            "source_vs_G0_fill_groups_local_observation_clock": groups(gfill, sfill, se, "local_observed"),
            "time_shift_60s": {
                "window_exposure_equality": exposure,
                "strategy": short_metrics(shift, xfill, xe, True),
                "G0_vs_shift60_compared_on_exact_UTC_window": True
            },
            "G0_and_source_first_A_k_sigma_equal": {
                key: g0["initial_parameters"][key] == source["initial_parameters"][key]
                for key in ("A_per_second", "k_per_tick", "sigma_tick_per_sqrt_second")
            },
            "limits": "Nonunique time+side fill pairing. Group A is window overlap not exchange ACK-confirmed causal cancellation. Raw +60s loses some last-minute episodes; no wrap."
        }
        if not all(report["G0_and_source_first_A_k_sigma_equal"].values()):
            raise AssertionError("Not the same G0 calibration")
        out = Path(out)
        out.mkdir(parents=True, exist_ok=True)
        (out/"case.json").write_text(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False))
        print("MIGRATION_MECH", day, coin, venue, "G0", report["g0"]["fills"],
              "SOURCE", report["source"]["fills"], "SHIFT", report["time_shift_60s"]["strategy"]["fills"])


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--day", required=True)
    p.add_argument("--coin", required=True)
    p.add_argument("--venue", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--admission", default="reports/source_defense/step3_fit/step3_fit_admission.json")
    a = p.parse_args()
    run(a.day, a.coin, a.venue, a.out, a.admission)
