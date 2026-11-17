"""Cross-symbol Gate safety tests; all market inputs below are synthetic."""
import csv
import gzip
import io
import math
import tempfile
from pathlib import Path
from unittest.mock import patch

from qa_gate_btc_futures import fetch_contract
from run_gate_btc_origin_glft import (
    TARGET_NOTIONAL_USDT, RISK_AVERSION_PER_USDT, safe_gate_csv,
    scenario, variants,
)

def contract(symbol, ctval, tick, lot):
    return dict(name=symbol, type="direct",
                quanto_multiplier=str(ctval), order_price_round=str(tick),
                order_size_min=str(lot), order_size_max="100000",
                maker_fee_rate="0.0002", taker_fee_rate="0.0005",
                config_change_time=0, enable_decimal=lot<1)

def test_contract_api_symbol():
    response = contract("SOL_USDT", 0.1, 0.01, 0.1)
    class Handle(io.BytesIO):
        pass
    def fake(req, timeout=None):
        assert req.full_url.endswith("/contracts/SOL_USDT")
        import json
        return Handle(json.dumps(response).encode())
    with patch("qa_gate_btc_futures.urllib.request.urlopen", fake):
        assert fetch_contract("SOL_USDT")["order_size_min"] == "0.1"
    try:
        fetch_contract("XRP_USDT")
    except ValueError:
        pass
    else:
        raise AssertionError("Unexpected symbol accepted")

def test_units(symbol, ctval, tick, lot, mid):
    cfg = scenario(mid, contract(symbol,ctval,tick,lot))
    assert math.isclose(cfg["gamma_code"],
                        RISK_AVERSION_PER_USDT*ctval*tick, rel_tol=1e-12)
    assert math.isclose(cfg["approx_order_notional_usdt"],
                        cfg["qty"]*ctval*mid, rel_tol=1e-12)
    assert math.isclose(cfg["position_cap"], 20*cfg["qty"],rel_tol=1e-12)
    assert abs(cfg["qty"]/lot-round(cfg["qty"]/lot)) < 1e-7
    assert abs(cfg["approx_order_notional_usdt"]-TARGET_NOTIONAL_USDT) < 0.5*ctval*lot*mid+1e-8
    assert math.isclose(cfg["tick_relative_bps"],10000*tick/mid,rel_tol=1e-12)
    assert cfg["maker_fee_rate"] == 0.0002

def test_trade_unit_grid():
    stamp=1780272000000000
    cols=("exchange","symbol","timestamp","local_timestamp","id","side","price","amount")
    def payload(qty, px="100.01", symbol="SOL_USDT"):
        rows=[dict(exchange="gate-io-futures",symbol=symbol,
                   timestamp=str(stamp),local_timestamp=str(stamp+1000+j),
                   id=str(j),side="buy",price=px,amount=str(qty))
              for j in range(2)]
        stream=io.StringIO();w=csv.DictWriter(stream,fieldnames=cols)
        w.writeheader();w.writerows(rows)
        return gzip.compress(stream.getvalue().encode())
    def check(binary, should_pass):
        with tempfile.TemporaryDirectory() as d:
            with patch("run_gate_btc_origin_glft.urllib.request.urlopen",
                       lambda req,timeout=None:io.BytesIO(binary)):
                try:
                    stats=safe_gate_csv("trades","2026-06-01",stamp+10_000,
                                        Path(d)/"trades.csv",100,
                                        symbol="SOL_USDT",
                                        historical_tick=0.01,historical_lot=0.1)
                    assert should_pass and stats["rows"]==2
                except ValueError:
                    if should_pass: raise
    check(payload("0.2"),True)
    check(payload("0.15"),False)
    check(payload("0.2","100.015"),False)
    check(payload("0.2",symbol="DOGE_USDT"),False)

def main():
    assert len(variants())==9 and variants()[0]==("glft",0.0,0)
    test_contract_api_symbol()
    test_units("BTC_USDT",0.0001,0.1,1,80000)
    test_units("SOL_USDT",0.1,0.01,0.1,125)
    test_units("DOGE_USDT",10,0.00001,0.1,0.18)
    test_trade_unit_grid()
    print("GATE_ALTCOIN_UNITS_OK")

if __name__=="__main__":
    main()
