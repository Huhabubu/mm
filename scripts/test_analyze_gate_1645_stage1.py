"""Synthetic accounting tests: no matching, no strategy changes."""
import io
import unittest
import zipfile

import numpy as np

from analyze_gate_1645_stage1 import analyze_case


def fixture(bad_cash=False):
    stamps = np.array([0, 100, 200, 300], dtype=np.int64) * 1_000_000
    state = np.zeros(4, dtype=[
        ("timestamp", "<i8"), ("price", "<f8"),
        ("position", "<f8"), ("balance", "<f8")])
    state["timestamp"] = stamps
    state["price"] = [100, 101, 102, 101]
    state["position"] = [0, 0, 2, 1]
    state["balance"] = [0, 0, -20.1, -9.95 + (0.1 if bad_cash else 0)]
    fills = {
        "exchange_timestamp_ns": np.array([149, 249]) * 1_000_000,
        "local_observed_ns": np.array([150, 250]) * 1_000_000,
        "side": np.array([1, -1]),
        "exec_price": np.array([100.5, 101.5]),
        "exec_qty": np.array([2., 1.]),
    }
    fee = (2 * 100.5 + 101.5) * 0.1 * 0.0002
    report = {
        "day": "2026-06-01", "status": "complete", "minutes_traded": 1430,
        "experimental_trigger_sigma_multiple": 1.645,
        "binance_extra_visible_delay_ms": 2.5,
        "scenario": {"ctval": 0.1, "maker_fee_rate": 0.0002},
        "variants": {"glft": {"end_time_ns": 999_000_000, "trading": {
            "engine_fills": 2, "end_mid_usdt_per_btc": 101.2,
            "end_position_contracts": 1.0, "balance_usdt": -9.95,
            "fee_usdt": fee, "end_net_marked_equity_usdt": 0.17 - fee,
        }}},
    }
    data = io.BytesIO()
    with zipfile.ZipFile(data, "w") as archive:
        buf = io.BytesIO()
        np.savez_compressed(buf, **{"0": state})
        archive.writestr(
            "recorder/2026-06-01/glft/recorder.npz", buf.getvalue())
        buf = io.BytesIO()
        np.savez_compressed(buf, **fills)
        archive.writestr(
            "recorder/2026-06-01/glft/fills.npz", buf.getvalue())
    data.seek(0)
    return data, report


class AttributionTest(unittest.TestCase):
    def test_two_identities_close_and_differ(self):
        data, report = fixture()
        with zipfile.ZipFile(data) as archive:
            x = analyze_case(
                archive, "2026-06-01", "glft", report,
                min_recorder_points=4)
        self.assertAlmostEqual(x["spread_capture_usdt"], 0.05)
        self.assertAlmostEqual(x["inventory_price_pnl_usdt"], 0.12)
        self.assertAlmostEqual(x["next_sample_capture_usdt"], 0.35)
        self.assertAlmostEqual(
            x["next_sample_inventory_price_pnl_usdt"], -0.18)
        self.assertAlmostEqual(x["official_zero_fee_usdt"], 0.17)

    def test_recorder_cash_mismatch_fails(self):
        data, report = fixture(bad_cash=True)
        with zipfile.ZipFile(data) as archive:
            with self.assertRaisesRegex(ValueError, "Recorder/cash/fill"):
                analyze_case(
                    archive, "2026-06-01", "glft", report,
                    min_recorder_points=4)

    def test_wrong_fee_scenario_fails(self):
        data, report = fixture()
        report["scenario"]["maker_fee_rate"] = -0.0001
        with zipfile.ZipFile(data) as archive:
            with self.assertRaisesRegex(ValueError, "positive 2bps"):
                analyze_case(
                    archive, "2026-06-01", "glft", report,
                    min_recorder_points=4)


if __name__ == "__main__":
    unittest.main()
