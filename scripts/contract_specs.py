"""Fail-closed, offline research contract specifications.

Gate fields are recorded HBT *simulation* contract units and explicitly positive
study fee assumptions. They are NOT certified date-effective exchange fees.
Binance is read-only market data in this study and has no execution contract.
"""
import json
import math
from pathlib import Path

CONFIG_PATH = Path(__file__).resolve().parent.parent / "configs" / "contract_specs.json"
REQUIRED_GATE = ("name", "type", "order_price_round", "quanto_multiplier",
                 "order_size_min", "maker_fee_rate", "taker_fee_rate")


def load_contract_spec(symbol, venue="gate", config_path=None, for_trading=True):
    path = CONFIG_PATH if config_path is None else Path(config_path)
    if not path.is_file():
        raise FileNotFoundError(f"Missing frozen contract config: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("schema_version") != 1:
        raise ValueError(f"Unsupported contract config version: {path}")
    section = "gate" if venue == "gate" else (
        "binance_reference" if venue == "binance" else None)
    if section is None:
        raise ValueError(f"Unsupported contract venue: {venue}")
    item = data.get(section, {}).get(symbol)
    if item is None:
        raise KeyError(f"Missing frozen contract configuration: {venue}:{symbol} ({path})")
    if section == "binance_reference":
        if for_trading:
            raise ValueError(
                f"Binance {symbol} is reference-market BBO only: historical "
                "tick/lot/fees have not been verified for simulated Binance orders")
        if not item.get("reference_only", False):
            raise ValueError("Binance reference contract must be marked reference_only")
        return dict(item)
    if any(key not in item for key in REQUIRED_GATE):
        raise ValueError(f"Incomplete Gate frozen contract configuration: {symbol}")
    if item["name"] != symbol or item["type"] != "direct":
        raise ValueError(f"Unexpected frozen Gate contract identity/type: {symbol}")
    result = dict(item)
    for key in ("order_price_round", "quanto_multiplier", "order_size_min",
                "maker_fee_rate", "taker_fee_rate"):
        value = float(result[key])
        if not math.isfinite(value):
            raise ValueError(f"Invalid frozen contract field {symbol}.{key}")
        result[key] = value
    if any(result[k] <= 0 for k in ("order_price_round", "quanto_multiplier",
                                    "order_size_min")):
        raise ValueError(f"Nonpositive Gate frozen price/quantity units: {symbol}")
    if result["maker_fee_rate"] < 0 or result["taker_fee_rate"] < 0:
        raise ValueError("Study fees must be nonnegative charges; maker rebate needs separate scenario")
    return result
