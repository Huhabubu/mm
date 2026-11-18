"""Offline stage2A frozen-contract and core configuration gates."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from contract_specs import load_contract_spec, CONFIG_PATH
from qa_gate_btc_futures import fetch_contract


class ContractSpecsTests(unittest.TestCase):
    def test_all_four_gate_units_offline(self):
        expected={
            "BTC_USDT":(0.1,0.0001,1.0),
            "SOL_USDT":(0.01,1.0,0.1),
            "DOGE_USDT":(0.00001,10.0,1.0),
            "ETH_USDT":(0.01,0.01,0.1),
        }
        with patch("urllib.request.urlopen",side_effect=AssertionError(
                "No network access during contract load")):
            for symbol, values in expected.items():
                c=fetch_contract(symbol)
                self.assertEqual((c["order_price_round"],c["quanto_multiplier"],
                                  c["order_size_min"]),values)
                self.assertEqual(c["maker_fee_rate"],0.0002)
                self.assertEqual(c["taker_fee_rate"],0.0005)

    def test_missing_configuration_errors_explicitly(self):
        with self.assertRaisesRegex(KeyError,"Missing frozen contract configuration"):
            fetch_contract("XRP_USDT")
        with self.assertRaisesRegex(FileNotFoundError,"Missing frozen contract config"):
            load_contract_spec("BTC_USDT",config_path="/absent/not_here.json")
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/"spec.json"
            p.write_text(json.dumps({"schema_version":1,"gate":{"BTC_USDT":{
                "name":"BTC_USDT","type":"direct","order_price_round":0,
                "quanto_multiplier":1,"order_size_min":1,
                "maker_fee_rate":0.0002,"taker_fee_rate":0.0005}}}))
            with self.assertRaisesRegex(ValueError,"Nonpositive"):
                load_contract_spec("BTC_USDT",config_path=p)

    def test_unverified_binance_contracts_cannot_be_traded(self):
        for sym in ("BTCUSDT","SOLUSDT","DOGEUSDT","ETHUSDT"):
            value=load_contract_spec(sym,venue="binance",for_trading=False)
            self.assertTrue(value["reference_only"])
            for key in ("tick_size","quanto_multiplier","order_size_min",
                        "maker_fee_rate","taker_fee_rate"):
                self.assertIsNone(value[key])
            with self.assertRaisesRegex(ValueError,"BBO only"):
                load_contract_spec(sym,venue="binance")


if __name__=="__main__":
    unittest.main()
