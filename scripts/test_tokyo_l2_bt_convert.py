"""Unit checks for the input narrowing in tokyo_l2_bt_convert (no network, no collector import)."""
import json
from pathlib import Path
import types
import unittest

import tokyo_l2_bt_convert as c


def stats():
    return {"records_in": 0, "records_kept": 0, "gate_mixed_symbol_trade_messages": 0,
            "prefilter_rx_seq_violations": 0}


def rec(seq, kind, payload, escaped=False):
    return {"rx_seq": seq, "kind": kind, "payload": payload if escaped else json.dumps(payload)}


CONFIG = {"exchanges": {
    "binance_futures": {"enabled": True, "symbols": ["BTCUSDT", "ETHUSDT"],
                        "public_channels": ["bookTicker", "depth@0ms"], "market_channels": ["aggTrade"]},
    "bybit_linear": {"enabled": True, "symbols": ["BTCUSDT"], "channels": ["orderbook.50"]},
    "gate_futures": {"enabled": True, "symbols": ["BTC_USDT", "ETH_USDT"],
                     "channels": ["futures.obu", "futures.trades", "futures.book_ticker"]},
}}


class NarrowConfigTests(unittest.TestCase):
    def test_only_target_pair_and_binance_bbo(self):
        n = c.narrowed_config(CONFIG, "BTC_USDT")
        self.assertEqual({k for k, v in n["exchanges"].items() if v["enabled"]},
                         {"binance_futures", "gate_futures"})
        b, g = n["exchanges"]["binance_futures"], n["exchanges"]["gate_futures"]
        self.assertEqual((b["symbols"], b["public_channels"], b["market_channels"]),
                         (["BTCUSDT"], ["bookTicker"], []))
        self.assertEqual(g["symbols"], ["BTC_USDT"])
        self.assertEqual(g["channels"], CONFIG["exchanges"]["gate_futures"]["channels"])
        self.assertEqual(CONFIG["exchanges"]["bybit_linear"]["enabled"], True)  # input untouched

    def test_rejects_uncollected_symbol(self):
        with self.assertRaises(ValueError):
            c.narrowed_config(CONFIG, "SOL_USDT")
        with self.assertRaises(ValueError):
            c.native_symbols("BTCUSDT")


class FilterTests(unittest.TestCase):
    def test_binance_keeps_target_bbo_and_control_meta_only(self):
        recs = [
            rec(1, "META", {"event": "GAP_START", "symbol": "ALL"}),
            rec(2, "DATA", {"stream": "btcusdt@bookTicker", "data": {"e": "bookTicker", "s": "BTCUSDT"}}),
            rec(3, "DATA", {"stream": "ethusdt@bookTicker", "data": {"e": "bookTicker", "s": "ETHUSDT"}}),
            rec(4, "DATA", {"stream": "btcusdt@depth@0ms", "data": {"e": "depthUpdate", "s": "BTCUSDT"}}),
            rec(5, "META", {"event": "REST_SNAPSHOT", "symbol": "BTCUSDT"}),
            rec(6, "DATA", {"stream": "btcusdt@aggTrade", "data": {"e": "aggTrade", "s": "BTCUSDT"}}),
            rec(7, "META", {"event": "LOCAL_LOSS", "symbol": "ETHUSDT"}),
            rec(8, "META", {"event": "LOCAL_LOSS", "symbol": "BTCUSDT"}),
        ]
        st = stats()
        kept = c.filter_records(recs, "binance_futures", "BTC_USDT", st)
        self.assertEqual([r["rx_seq"] for r in kept], [1, 2, 8])
        self.assertEqual((st["records_in"], st["records_kept"], st["prefilter_rx_seq_violations"]), (8, 3, 0))

    def test_gate_splits_mixed_trade_batch_and_keeps_escaped(self):
        recs = [
            rec(1, "DATA_ESCAPED", {"channel": "futures.obu", "event": "update",
                                    "result": {"s": "ob.BTC_USDT.400"}}, escaped=True),
            rec(2, "DATA", {"channel": "futures.trades", "event": "update",
                            "result": [{"contract": "BTC_USDT", "id": 1}, {"contract": "ETH_USDT", "id": 2}]}),
            rec(3, "DATA", {"channel": "futures.book_ticker", "event": "update", "result": {"s": "ETH_USDT"}}),
            rec(4, "META", {"event": "DEPTH_UNAVAILABLE", "symbol": "ETH_USDT"}),
            rec(5, "META", {"event": "SWITCH"}),
        ]
        st = stats()
        kept = c.filter_records(recs, "gate_futures", "BTC_USDT", st)
        self.assertEqual([r["rx_seq"] for r in kept], [1, 2, 5])
        self.assertIs(kept[0], recs[0])
        self.assertEqual(json.loads(kept[1]["payload"])["result"], [{"contract": "BTC_USDT", "id": 1}])
        self.assertEqual(json.loads(recs[1]["payload"])["result"][1]["contract"], "ETH_USDT")  # not mutated
        self.assertEqual(st["gate_mixed_symbol_trade_messages"], 1)

    def test_full_file_rx_seq_checked_before_dropping(self):
        recs = [
            rec(1, "META", {"event": "SWITCH"}),
            rec(3, "DATA", {"channel": "futures.book_ticker", "event": "update", "result": {"s": "ETH_USDT"}}),
            rec(3, "DATA", {"channel": "futures.book_ticker", "event": "update", "result": {"s": "ETH_USDT"}}),
            rec(4, "META", {"event": "SWITCH"}),
        ]
        st = stats()
        kept = c.filter_records(recs, "gate_futures", "BTC_USDT", st)
        self.assertEqual([r["rx_seq"] for r in kept], [1, 4])   # violation only in dropped records
        self.assertEqual(st["prefilter_rx_seq_violations"], 1)

    def test_install_filter_routes_by_partition_exchange(self):
        raw = {"binance_futures": [rec(1, "DATA", {"stream": "ethusdt@bookTicker",
                                                    "data": {"e": "bookTicker", "s": "ETHUSDT"}})]}
        fake = types.SimpleNamespace(parse_tsv_zst_records=lambda p: raw[Path(p).parent.parent.name])
        st = stats()
        c.install_filter(fake, "BTC_USDT", st)
        self.assertEqual(fake.parse_tsv_zst_records(Path("raw/binance_futures/2026-11-23/x.tsv.zst")), [])
        with self.assertRaises(ValueError):
            fake.parse_tsv_zst_records(Path("raw/bybit_linear/2026-11-23/x.tsv.zst"))

    def test_expected_output_names(self):
        out = c.expected_outputs(Path("o"), "BTC_USDT", "2026-11-23")
        self.assertEqual({k: v.name for k, v in out.items()}, {
            "binance_bbo": "binance_bbo_BTC_20261123.npz",
            "gate_l2_hbt": "gate_futures_l2_BTC_20261123.npz",
            "gate_l1_bbo": "gate_futures_l1_BTC_20261123.npz",
        })


if __name__ == "__main__":
    unittest.main()
