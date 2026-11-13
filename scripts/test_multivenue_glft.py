"""Deterministic cross-market contract-scale and risk accounting checks.

These are unit scenarios, not market outcomes or historical specifications.
"""
import math
import unittest

import numpy as np

from cross_venue_event_probe import visible_timestamps
from run_multivenue_glft import MARKETS, order_scenario, MAX_WALL_TICKS
from run_beta_one_direct_gap import quote_gap_coverage, NS


class CrossMarketScaleTests(unittest.TestCase):
    def test_market_matrix_is_two_venues_times_two_coins(self):
        self.assertEqual(set(MARKETS),{
            ("okx","BTC"),("okx","SOL"),("bitget","BTC"),("bitget","SOL")})
        self.assertEqual(MAX_WALL_TICKS,50)

    def test_legacy_btc_okx_gamma_and_notional_are_preserved(self):
        spec=MARKETS[("okx","BTC")]
        x=order_scenario(spec,77500)
        self.assertAlmostEqual(x["gamma_code"],.05,places=12)
        self.assertAlmostEqual(x["order_qty"],.1,places=12)
        self.assertAlmostEqual(x["position_cap"],2.,places=12)
        self.assertAlmostEqual(x["initial_order_notional_usdt"],77.5,places=7)

    def test_bitget_and_sol_order_quantities_use_distinct_lots(self):
        observed_mid={"BTC":77500.0,"SOL":95.4}
        for (venue,coin),spec in MARKETS.items():
            q=order_scenario(spec,observed_mid[coin])
            self.assertAlmostEqual(q["order_qty"]/spec.lot,
                                   round(q["order_qty"]/spec.lot),places=7)
            self.assertGreaterEqual(q["initial_order_notional_usdt"],5)
            self.assertLessEqual(q["initial_order_notional_usdt"],120)
            self.assertAlmostEqual(q["gamma_code"],
                                   50*spec.ctval*spec.tick,places=10)
            self.assertAlmostEqual(q["position_cap"],20*q["order_qty"],places=10)
        self.assertAlmostEqual(order_scenario(MARKETS[("bitget","BTC")],77500)
                               ["order_qty"],.001,places=7)

    def test_true_zero_binance_visibility_keeps_exact_timestamp(self):
        clock = np.array([1_000_000_000, 1_000_000_000,
                          1_000_001_000], dtype=np.int64)
        np.testing.assert_array_equal(
            visible_timestamps(clock, extra_delay_ms=0.0), clock)
        # Existing positive-delay behavior remains unchanged.
        np.testing.assert_array_equal(
            visible_timestamps(clock, extra_delay_ms=1.0)-clock,
            np.full(3, 1_000_001, dtype=np.int64))

    def test_zero_order_request_response_latency_reaches_official_asset(self):
        from unittest.mock import patch
        from run_multivenue_glft import build_asset

        with patch("hftbacktest.BacktestAsset") as asset_type:
            asset=asset_type.return_value
            # BacktestAsset fluent methods are mock objects; verify the
            # boundary call to official HBT rather than a custom matcher.
            build_asset("dummy.npz", 90.0, 100.0,
                        MARKETS[("okx", "SOL")],
                        request_latency_ns=0,response_latency_ns=0)
            asset.data.return_value.linear_asset.return_value.                constant_order_latency.assert_called_once_with(0,0)

        with self.assertRaisesRegex(ValueError, "nonnegative"):
            build_asset("dummy.npz", 90.0, 100.0,
                        MARKETS[("okx", "SOL")],
                        request_latency_ns=-1,response_latency_ns=0)

    def test_fractional_2_5ms_binance_delay_is_exact(self):
        clock = np.array([1_000_000_000, 1_000_000_000], dtype=np.int64)
        actual = visible_timestamps(clock, extra_delay_ms=2.5)
        np.testing.assert_array_equal(actual - clock, [2_500_001, 2_500_001])

    def test_signal_coverage_uses_market_tick_not_legacy_btc_tick(self):
        # Synthetic native gap 0.02 USDT per unit: two OKX SOL ticks,
        # twenty Bitget SOL ticks, and less than one BTC tick.
        ot = np.arange(11, dtype=np.int64)*100_000_000 + 700*NS
        bt = ot + 5_000_000
        basis_ts = np.array([699*NS], dtype=np.int64)
        basis = np.array([0.])
        for (venue, coin), spec in MARKETS.items():
            with self.subTest(venue=venue, coin=coin):
                report = quote_gap_coverage(
                    ot, np.full(len(ot), 100.), bt,
                    np.full(len(bt), 100.02), basis_ts, basis,
                    700*NS+100_000_000, 700*NS+1_000_000_000,
                    tick_size=spec.tick)
                self.assertGreater(report["usable_gap_points"], 0)
                self.assertEqual(report["fraction_abs_gap_at_least_one_tick"],
                                 1. if coin == "SOL" else 0.)

    def test_error_when_price_invalid(self):
        with self.assertRaises(ValueError):
            order_scenario(MARKETS[("okx","BTC")],float("nan"))
        with self.assertRaises(ValueError):
            order_scenario(MARKETS[("okx","BTC")],0.0)


if __name__=="__main__":
    unittest.main()
