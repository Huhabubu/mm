"""Exchange-specific symbol, raw-frame, and contract-spec adapters for Tokyo L2 backtesting.

Separates exchange-specific raw wire formats and contract sizing rules
(Binance Futures leader; Gate Futures, Bybit Linear, Bitget v3 makers) from
generic strategy execution (`scripts/core_glft_engine.py`) and five-stage
research analytics (`scripts/tokyo_l2_research_stages.py`).
"""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
import tempfile
import time
from typing import Any

SIGNAL_EXCHANGES = ("binance_futures",)
MAKING_EXCHANGES = ("gate_futures", "bybit_linear", "bitget_v3")
SUPPORTED_EXCHANGES = SIGNAL_EXCHANGES + MAKING_EXCHANGES

TRADE_COMPARE_FIELDS_BY_EXCHANGE: dict[str, tuple[str, ...]] = {
    "gate_futures": ("contract", "id", "price", "size", "create_time_ms", "create_time", "is_internal"),
    "bybit_linear": ("s", "i", "p", "v", "S", "T", "BT", "RPI"),
    "bitget_v3": ("i", "tradeId", "p", "P", "px", "v", "sz", "S", "side", "T", "ts", "isRPI"),
}

STRATEGY_CHANNELS_BY_EXCHANGE: dict[str, tuple[str, ...]] = {
    "binance_futures": ("bookTicker", "book_ticker", "@bookTicker"),
    "gate_futures": ("futures.book_ticker", "futures.obu", "futures.trades"),
    "bybit_linear": ("orderbook.1", "orderbook.50", "publicTrade"),
    "bitget_v3": ("books1", "books50", "publicTrade"),
}


def to_base_symbol(symbol: str | None) -> str:
    """Normalize an exchange-native or base USDT perpetual symbol to base asset (e.g. 'BTC')."""
    text = str(symbol or "").strip().upper()
    if text.endswith("_USDT"):
        text = text[:-5]
    elif text.endswith("-USDT-SWAP"):
        text = text[:-10]
    elif text.endswith("-USDT"):
        text = text[:-5]
    elif text.endswith("USDT") and len(text) > 4:
        text = text[:-4]
    if not text:
        raise ValueError(f"cannot extract base asset from symbol={symbol!r}")
    return text


def to_native_symbol(exchange: str, symbol: str) -> str:
    """Map base or native symbol to the exchange's native USDT perpetual symbol."""
    base = to_base_symbol(symbol)
    if exchange == "gate_futures":
        return f"{base}_USDT"
    if exchange in ("binance_futures", "bybit_linear", "bitget_v3"):
        return f"{base}USDT"
    raise ValueError(f"unsupported exchange: {exchange!r}")


def strategy_channels(signal_exchange: str, making_exchange: str) -> set[tuple[str, str]]:
    """Strategy-relevant (exchange, channel) pairs used for targeted QA/protocol checks."""
    out: set[tuple[str, str]] = set()
    for ex in (signal_exchange, making_exchange):
        if ex not in STRATEGY_CHANNELS_BY_EXCHANGE:
            raise ValueError(f"unsupported exchange for strategy channels: {ex!r}")
        for ch in STRATEGY_CHANNELS_BY_EXCHANGE[ex]:
            out.add((ex, ch))
    return out


def trade_compare_fields(exchange: str) -> tuple[str, ...]:
    if exchange not in TRADE_COMPARE_FIELDS_BY_EXCHANGE:
        raise ValueError(f"unsupported making exchange: {exchange!r}")
    return TRADE_COMPARE_FIELDS_BY_EXCHANGE[exchange]


def is_follower_trade_raw(exchange: str, raw_line: str) -> bool:
    """Fast pre-JSON substring test for trade updates used by handover connection tracking."""
    if exchange == "gate_futures":
        return "futures.trades" in raw_line and "update" in raw_line
    if exchange == "bybit_linear":
        return "publicTrade." in raw_line
    if exchange == "bitget_v3":
        return "publicTrade" in raw_line
    raise ValueError(f"unsupported making exchange: {exchange!r}")


def _parse_px_qty_pairs(raw_levels: Any) -> list[tuple[float, float]]:
    pairs: list[tuple[float, float]] = []
    if not isinstance(raw_levels, list):
        return pairs
    for item in raw_levels:
        if isinstance(item, dict):
            px_v = item.get("p", item.get("price"))
            qty_v = item.get("s", item.get("size", item.get("qty")))
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            px_v, qty_v = item[0], item[1]
        else:
            continue
        if px_v is None or qty_v is None:
            continue
        pairs.append((float(px_v), float(qty_v)))
    return pairs


def parse_follower_payload(
    exchange: str,
    obj: dict[str, Any],
    wall_ns: int,
    base_symbol: str,
) -> dict[str, Any] | None:
    """Parse one decoded JSON payload from a making exchange into a normalized event descriptor.

    Returns None when the frame is unrelated to `base_symbol` or is an unsupported channel.
    Event kinds returned:
      - {"kind": "depth", "is_snapshot": bool, "seq": int|None, "parent": int|None,
         "exch_ts_ns": int, "raw_bids": list, "raw_asks": list, "coverage_obj": dict}
      - {"kind": "l1", "exch_ts_ns": int, "bid_px": float, "bid_qty": float,
         "ask_px": float, "ask_qty": float, "coverage_obj": dict}
      - {"kind": "trades", "trades": list[dict], "mixed_symbols": bool, "coverage_obj": dict}
        where each item in `trades` has:
        {"dedup_key": str, "raw_trade": dict, "exch_ts_ns": int, "is_buy": bool,
         "price": float, "qty": float, "flags": int, "special_flags": list[str]}
    """
    if not isinstance(obj, dict):
        return None
    if exchange == "gate_futures":
        return _parse_gate_payload(obj, wall_ns, base_symbol)
    if exchange == "bybit_linear":
        return _parse_bybit_payload(obj, wall_ns, base_symbol)
    if exchange == "bitget_v3":
        return _parse_bitget_payload(obj, wall_ns, base_symbol)
    raise ValueError(f"unsupported making exchange: {exchange!r}")


def _parse_gate_payload(obj: dict[str, Any], wall_ns: int, base_symbol: str) -> dict[str, Any] | None:
    from qa_convert import TRADE_FLAG_IS_INTERNAL

    channel = str(obj.get("channel") or "")
    result = obj.get("result")
    if channel == "futures.trades":
        trades = result if isinstance(result, list) else ([result] if isinstance(result, dict) else [])
        mine = [t for t in trades if isinstance(t, dict) and to_base_symbol(t.get("contract")) == base_symbol]
        if not mine:
            return None
        cov_obj = {**obj, "result": mine} if len(mine) != len(trades) else obj
        parsed = []
        if obj.get("event", "") == "update":
            for tr in mine:
                exch_ts = int(tr.get("create_time_ms") or obj.get("time_ms") or (wall_ns // 1_000_000)) * 1_000_000
                raw_size = float(tr["size"])
                flags = 0
                special: list[str] = []
                if bool(tr.get("is_internal", False)):
                    flags |= TRADE_FLAG_IS_INTERNAL
                    special.append("is_internal")
                parsed.append({
                    "dedup_key": f"{tr.get('contract') or ''}:{tr['id']}",
                    "raw_trade": tr,
                    "exch_ts_ns": exch_ts,
                    "is_buy": raw_size >= 0.0,
                    "price": float(tr["price"]),
                    "qty": abs(raw_size),
                    "flags": flags,
                    "special_flags": special,
                })
        return {"kind": "trades", "trades": parsed, "mixed_symbols": len(mine) != len(trades), "coverage_obj": cov_obj}

    if channel == "futures.obu" and isinstance(result, dict):
        sym_raw = str(result.get("s") or "")
        if not sym_raw.startswith("ob.") or to_base_symbol(sym_raw.split(".")[1]) != base_symbol:
            return None
        if obj.get("event", "") != "update":
            return {"kind": "ignored_non_update", "coverage_obj": obj}
        exch_ms = int(result.get("t") or obj.get("time_ms") or (wall_ns // 1_000_000))
        return {
            "kind": "depth",
            "is_snapshot": bool(result.get("full", False)),
            "seq": int(result["u"]) if result.get("u") is not None else None,
            "parent": int(result["U"]) if result.get("U") is not None else None,
            "exch_ts_ns": exch_ms * 1_000_000,
            "raw_bids": result.get("b"),
            "raw_asks": result.get("a"),
            "coverage_obj": obj,
        }

    if channel == "futures.book_ticker" and isinstance(result, dict):
        if to_base_symbol(result.get("s")) != base_symbol:
            return None
        if obj.get("event", "") != "update":
            return {"kind": "ignored_non_update", "coverage_obj": obj}
        exch_ts = int(result.get("t") or obj.get("time_ms") or (wall_ns // 1_000_000)) * 1_000_000
        return {
            "kind": "l1",
            "exch_ts_ns": exch_ts,
            "bid_px": float(result["b"]),
            "bid_qty": float(result["B"]),
            "ask_px": float(result["a"]),
            "ask_qty": float(result["A"]),
            "coverage_obj": obj,
        }
    return None


def _parse_bybit_payload(obj: dict[str, Any], wall_ns: int, base_symbol: str) -> dict[str, Any] | None:
    from qa_convert import TRADE_FLAG_BYBIT_BT, TRADE_FLAG_RPI

    topic = str(obj.get("topic") or "")
    if not topic:
        return None
    if topic.startswith("orderbook.50."):
        data = obj.get("data")
        if not isinstance(data, dict):
            return None
        sym = to_base_symbol(str(data.get("s") or topic.split(".")[-1]))
        if sym != base_symbol:
            return None
        exch_ms = int(obj.get("cts") or obj.get("ts") or (wall_ns // 1_000_000))
        seq = int(data["u"]) if data.get("u") is not None else None
        is_snap = obj.get("type") == "snapshot" or seq == 1
        return {
            "kind": "depth",
            "is_snapshot": is_snap,
            "seq": seq,
            "parent": None,
            "exch_ts_ns": exch_ms * 1_000_000,
            "raw_bids": data.get("b"),
            "raw_asks": data.get("a"),
            "coverage_obj": obj,
        }

    if topic.startswith("orderbook.1."):
        data = obj.get("data")
        if not isinstance(data, dict):
            return None
        sym = to_base_symbol(str(data.get("s") or topic.split(".")[-1]))
        if sym != base_symbol:
            return None
        exch_ms = int(obj.get("cts") or obj.get("ts") or (wall_ns // 1_000_000))
        bids = _parse_px_qty_pairs(data.get("b") or [])
        asks = _parse_px_qty_pairs(data.get("a") or [])
        if not (bids and asks):
            return {"kind": "ignored_non_update", "coverage_obj": obj}
        return {
            "kind": "l1",
            "exch_ts_ns": exch_ms * 1_000_000,
            "bid_px": bids[0][0],
            "bid_qty": bids[0][1],
            "ask_px": asks[0][0],
            "ask_qty": asks[0][1],
            "coverage_obj": obj,
        }

    if topic.startswith("publicTrade."):
        trades = obj.get("data") or []
        if not isinstance(trades, list):
            return None
        mine = []
        for tr in trades:
            if not isinstance(tr, dict):
                continue
            raw_sym = str(tr.get("s") or topic.split(".")[-1])
            if to_base_symbol(raw_sym) != base_symbol:
                continue
            mine.append((raw_sym, tr))
        if not mine:
            return None
        parsed = []
        for raw_sym, tr in mine:
            tid = str(tr["i"])
            exch_ms = int(tr.get("T") or obj.get("ts") or (wall_ns // 1_000_000))
            side_str = str(tr.get("S", "")).lower()
            flags = 0
            special: list[str] = []
            if bool(tr.get("BT", False)):
                flags |= TRADE_FLAG_BYBIT_BT
                special.append("BT")
            if bool(tr.get("RPI", False)):
                flags |= TRADE_FLAG_RPI
                special.append("RPI")
            parsed.append({
                "dedup_key": f"{raw_sym}:{tid}",
                "raw_trade": tr,
                "exch_ts_ns": exch_ms * 1_000_000,
                "is_buy": side_str == "buy",
                "price": float(tr["p"]),
                "qty": float(tr["v"]),
                "flags": flags,
                "special_flags": special,
            })
        cov_obj = {**obj, "data": [t for _, t in mine]} if len(mine) != len(trades) else obj
        return {"kind": "trades", "trades": parsed, "mixed_symbols": len(mine) != len(trades), "coverage_obj": cov_obj}

    return None


def _parse_bitget_payload(obj: dict[str, Any], wall_ns: int, base_symbol: str) -> dict[str, Any] | None:
    from qa_convert import TRADE_FLAG_RPI

    arg = obj.get("arg")
    data_list = obj.get("data")
    if not isinstance(arg, dict) or not isinstance(data_list, list):
        return None
    channel = str(arg.get("topic") or arg.get("channel") or "")
    inst_id = str(arg.get("symbol") or arg.get("instId") or "")
    if not inst_id or to_base_symbol(inst_id) != base_symbol:
        return None
    action = str(obj.get("action") or "")

    if channel == "books50":
        is_snap = action == "snapshot"
        items = []
        for item in data_list:
            if not isinstance(item, dict):
                continue
            exch_ms = int(item.get("ts") or obj.get("ts") or (wall_ns // 1_000_000))
            raw_bids = item["b"] if "b" in item else item.get("bids")
            raw_asks = item["a"] if "a" in item else item.get("asks")
            items.append({
                "is_snapshot": is_snap,
                "seq": int(item["seq"]) if item.get("seq") is not None else None,
                "parent": int(item["pseq"]) if item.get("pseq") is not None else None,
                "exch_ts_ns": exch_ms * 1_000_000,
                "raw_bids": raw_bids,
                "raw_asks": raw_asks,
            })
        if not items:
            return None
        return {"kind": "depth_batch", "items": items, "coverage_obj": obj}

    if channel == "books1":
        rows = []
        for item in data_list:
            if not isinstance(item, dict):
                continue
            exch_ms = int(item.get("ts") or obj.get("ts") or (wall_ns // 1_000_000))
            bids = _parse_px_qty_pairs(item.get("b") or item.get("bids") or [])
            asks = _parse_px_qty_pairs(item.get("a") or item.get("asks") or [])
            if bids and asks:
                rows.append({
                    "exch_ts_ns": exch_ms * 1_000_000,
                    "bid_px": bids[0][0],
                    "bid_qty": bids[0][1],
                    "ask_px": asks[0][0],
                    "ask_qty": asks[0][1],
                })
        if not rows:
            return {"kind": "ignored_non_update", "coverage_obj": obj}
        return {"kind": "l1_batch", "items": rows, "coverage_obj": obj}

    if channel == "publicTrade":
        parsed = []
        for tr in data_list:
            if not isinstance(tr, dict):
                continue
            tid = str(tr.get("i") or tr.get("tradeId") or "")
            if not tid:
                continue
            exch_ms = int(tr.get("T") or tr.get("ts") or obj.get("ts") or (wall_ns // 1_000_000))
            side_str = str(tr.get("S") or tr.get("side") or "").lower()
            price = tr["p"] if "p" in tr else (tr["P"] if "P" in tr else tr["px"])
            qty = tr["v"] if "v" in tr else tr["sz"]
            flags = 0
            special: list[str] = []
            is_rpi = tr.get("isRPI")
            if is_rpi is True or str(is_rpi).strip().lower() in {"yes", "true", "1"}:
                flags |= TRADE_FLAG_RPI
                special.append("RPI")
            parsed.append({
                "dedup_key": f"{inst_id}:{tid}",
                "raw_trade": tr,
                "exch_ts_ns": exch_ms * 1_000_000,
                "is_buy": side_str == "buy",
                "price": float(price),
                "qty": float(qty),
                "flags": flags,
                "special_flags": special,
            })
        return {"kind": "trades", "trades": parsed, "mixed_symbols": False, "coverage_obj": obj}

    return None


def parse_live_contract(
    exchange: str,
    raw_payload: Any,
    native_symbol: str,
    *,
    quote_tick_mult: int = 1,
    hbt_tick_size_override: float | None = None,
) -> dict[str, Any]:
    """Normalize a collector REST instrument snapshot into a canonical contract spec dict.

    Matches `scripts/tokyo_l2_bt_fetch.py` (Gate) and `scripts/tokyo_l2_replay_qa.py` (Bybit/Bitget).
    """
    if exchange == "gate_futures":
        row = next((x for x in raw_payload if x.get("name") == native_symbol), None)
        if row is None:
            raise ValueError(f"Gate instrument {native_symbol} not found in snapshot")
        step = row.get("order_size_step") or row.get("order_size_round")
        lot = float(step) if step else (1.0 if not row.get("enable_decimal") else float(row["order_size_min"]))
        min_size = float(row["order_size_min"])
        tick = float(row["order_price_round"])
        ctval = float(row["quanto_multiplier"])
        raw_fields = {
            k: row.get(k) for k in (
                "name", "type", "order_price_round", "quanto_multiplier",
                "order_size_min", "order_size_round", "order_size_step",
                "enable_decimal", "maker_fee_rate", "taker_fee_rate",
            )
        }
        lot_source = "step" if step else "minimum_order_size"
        api_maker = float(row.get("maker_fee_rate") or 0.0)
        api_taker = float(row.get("taker_fee_rate") or 0.0)
    elif exchange == "bybit_linear":
        pages = raw_payload.get("pages") if isinstance(raw_payload, dict) else None
        rows = [x for p in pages for x in p["result"]["list"]] if pages else raw_payload["result"]["list"]
        row = next((x for x in rows if x.get("symbol") == native_symbol), None)
        if row is None:
            raise ValueError(f"Bybit instrument {native_symbol} not found in snapshot")
        tick = float(row["priceFilter"]["tickSize"])
        lot = float(row["lotSizeFilter"]["qtyStep"])
        min_size = float(row["lotSizeFilter"]["minOrderQty"])
        ctval = 1.0
        raw_fields = {
            "symbol": row.get("symbol"),
            "priceFilter": row.get("priceFilter"),
            "lotSizeFilter": row.get("lotSizeFilter"),
        }
        lot_source = "qtyStep"
        api_maker = float(row.get("makerFeeRate") or 0.0)
        api_taker = float(row.get("takerFeeRate") or 0.0)
    elif exchange == "bitget_v3":
        rows = raw_payload["data"] if isinstance(raw_payload, dict) else raw_payload
        row = next((x for x in rows if (x.get("symbol") or x.get("instId")) == native_symbol), None)
        if row is None:
            raise ValueError(f"Bitget instrument {native_symbol} not found in snapshot")
        tick_raw = row.get("priceMultiplier") or row.get("tickSize")
        if tick_raw is None and row.get("pricePlace") is not None:
            end_step = float(row.get("priceEndStep") or 1)
            tick_raw = end_step * (10.0 ** (-int(row["pricePlace"])))
        lot_raw = row.get("quantityMultiplier") or row.get("sizeMultiplier")
        min_raw = row.get("minOrderQty") or row.get("minTradeNum")
        tick = float(tick_raw)
        lot = float(lot_raw)
        min_size = float(min_raw)
        ctval = 1.0
        raw_fields = {
            k: row.get(k) for k in (
                "symbol", "instId", "priceMultiplier", "pricePlace", "priceEndStep",
                "quantityMultiplier", "sizeMultiplier", "minOrderQty", "minTradeNum",
                "makerFeeRate", "takerFeeRate",
            )
        }
        lot_source = "quantityMultiplier"
        api_maker = float(row.get("makerFeeRate") or 0.0)
        api_taker = float(row.get("takerFeeRate") or 0.0)
    else:
        raise ValueError(f"unsupported making exchange: {exchange!r}")

    qmult = int(quote_tick_mult)
    if qmult < 1:
        raise ValueError(f"quote_tick_mult must be >= 1, got {qmult}")
    if hbt_tick_size_override is not None:
        hbt_tick = float(hbt_tick_size_override)
        legal_tick = tick
        if not math.isclose(hbt_tick * qmult, legal_tick, rel_tol=1e-9):
            raise ValueError(
                f"dual-grid mismatch: hbt_tick({hbt_tick}) * quote_tick_mult({qmult}) != legal_tick({legal_tick})"
            )
    else:
        hbt_tick = tick
        legal_tick = tick * qmult

    return normalize_contract_spec({
        "exchange": exchange,
        "native_symbol": native_symbol,
        "base_symbol": to_base_symbol(native_symbol),
        "tick_size": hbt_tick,
        "legal_quote_tick": legal_tick,
        "quote_tick_mult": qmult,
        "lot_size": lot,
        "min_order_size": min_size,
        "contract_value": ctval,
        "lot_source": lot_source,
        "api_maker_fee_rate": api_maker,
        "api_taker_fee_rate": api_taker,
        "raw_fields": raw_fields,
    })


def normalize_contract_spec(spec: dict[str, Any]) -> dict[str, Any]:
    """Validate and canonicalize a contract specification dictionary."""
    exchange = str(spec["exchange"])
    if exchange not in MAKING_EXCHANGES:
        raise ValueError(f"unsupported making exchange: {exchange!r}")
    native_symbol = str(spec.get("native_symbol") or to_native_symbol(exchange, str(spec["base_symbol"])))
    base_symbol = to_base_symbol(spec.get("base_symbol") or native_symbol)
    tick = float(spec["tick_size"])
    qmult = int(spec.get("quote_tick_mult", 1))
    legal_tick = float(spec.get("legal_quote_tick", tick * qmult))
    lot = float(spec["lot_size"])
    min_size = float(spec.get("min_order_size", lot))
    ctval = float(spec.get("contract_value", 1.0))
    if not (tick > 0 and legal_tick > 0 and lot > 0 and min_size > 0 and ctval > 0 and qmult >= 1):
        raise ValueError(f"invalid contract spec values: {spec}")
    if not math.isclose(tick * qmult, legal_tick, rel_tol=1e-9):
        raise ValueError(f"tick_size({tick}) * quote_tick_mult({qmult}) != legal_quote_tick({legal_tick})")
    out = {
        "exchange": exchange,
        "native_symbol": native_symbol,
        "base_symbol": base_symbol,
        "tick_size": tick,
        "legal_quote_tick": legal_tick,
        "quote_tick_mult": qmult,
        "lot_size": lot,
        "min_order_size": min_size,
        "contract_value": ctval,
        "lot_source": str(spec.get("lot_source") or "explicit"),
        "api_maker_fee_rate": float(spec.get("api_maker_fee_rate") or 0.0),
        "api_taker_fee_rate": float(spec.get("api_taker_fee_rate") or 0.0),
    }
    if "raw_fields" in spec:
        out["raw_fields"] = spec["raw_fields"]
    return out


def contract_spec_digest(spec: dict[str, Any]) -> str:
    """Deterministic SHA-256 of the economic contract specification (for cache verification)."""
    canonical = {
        k: spec[k] for k in (
            "exchange", "native_symbol", "base_symbol", "tick_size",
            "legal_quote_tick", "quote_tick_mult", "lot_size",
            "min_order_size", "contract_value",
        )
    }
    return hashlib.sha256(json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def load_live_contract_from_meta(
    remote: str | None,
    meta_dir: Path,
    exchange: str,
    native_symbol: str,
    start_ns: int,
    *,
    quote_tick_mult: int = 1,
    hbt_tick_size_override: float | None = None,
) -> dict[str, Any]:
    """Load the latest exchange instrument snapshot observed at or before `start_ns`.

    Works with either a live rclone `remote` (downloads the matching raw spec JSON)
    or local `meta_dir / 'specs' / <raw_file>` when `remote` is None (for offline tests).
    """
    if exchange not in MAKING_EXCHANGES:
        raise ValueError(f"unsupported making exchange: {exchange!r}")
    specs_dir = Path(meta_dir) / "specs"
    metas = []
    for path in sorted(specs_dir.glob(f"{exchange}_*.meta.json")):
        meta = json.loads(path.read_text(encoding="utf-8"))
        if meta.get("exchange") == exchange and int(meta.get("reply_wall_ns") or 0) <= int(start_ns):
            metas.append((int(meta["reply_wall_ns"]), path.name, meta))
    if not metas:
        raise ValueError(f"no {exchange} instrument snapshot observed at or before start_ns={start_ns}")
    reply_ns, meta_name, meta = max(metas, key=lambda x: (x[0], x[1]))
    raw_name = str(meta["raw_file"])
    if Path(raw_name).name != raw_name or not raw_name.startswith(exchange + "_"):
        raise ValueError(f"invalid instrument snapshot path: {raw_name!r}")
    local_raw = specs_dir / raw_name
    if local_raw.is_file():
        raw_bytes = local_raw.read_bytes()
    elif remote:
        import hourly_incremental_qa as archive
        with tempfile.TemporaryDirectory() as tmp:
            raw_path = Path(tmp) / raw_name
            archive._download_with_retry(remote, "meta/specs/" + raw_name, raw_path, retries=3, sleeper=time.sleep)
            raw_bytes = raw_path.read_bytes()
    else:
        raise FileNotFoundError(f"missing local raw instrument snapshot {local_raw} and no remote provided")
    actual_sha = hashlib.sha256(raw_bytes).hexdigest()
    if actual_sha != meta["sha256"]:
        raise ValueError(f"instrument snapshot digest mismatch for {raw_name}: {actual_sha} != {meta['sha256']}")
    spec = parse_live_contract(
        exchange, json.loads(raw_bytes), native_symbol,
        quote_tick_mult=quote_tick_mult, hbt_tick_size_override=hbt_tick_size_override,
    )
    return {
        "contract": spec,
        "meta_file": meta_name,
        "reply_wall_ns": reply_ns,
        "raw_sha256": actual_sha,
        "spec_sha256": contract_spec_digest(spec),
    }


def compute_strategy_scenario(
    pretrade_mid: float,
    contract_spec: dict[str, Any],
    *,
    target_notional_usdt: float = 80.0,
    risk_aversion_per_usdt: float = 50.0,
    inventory_multiple: int = 20,
    maker_fee_rate: float = 0.0002,
    taker_fee_rate: float = 0.0005,
) -> dict[str, Any]:
    """Compute frozen-equivalent GLFT sizing and gamma_code for any supported exchange/symbol.

    Matches `kaggle/tasks/full_day_glft.py::gate_scenario_from_live` identically on Gate BTC
    and `scripts/run_bitget_core_glft.py` on Bitget linear / dual-grid contracts.
    """
    spec = normalize_contract_spec(contract_spec)
    if not (math.isfinite(pretrade_mid) and pretrade_mid > 0):
        raise ValueError(f"invalid pretrade_mid: {pretrade_mid}")
    tick = spec["tick_size"]
    legal_tick = spec["legal_quote_tick"]
    qmult = spec["quote_tick_mult"]
    lot = spec["lot_size"]
    min_size = spec["min_order_size"]
    ctval = spec["contract_value"]

    min_steps = max(1, math.ceil(min_size / lot - 1e-10))
    raw_steps = round(target_notional_usdt / (pretrade_mid * ctval * lot))
    steps = max(min_steps, int(raw_steps))
    qty = float(steps * lot)
    if qty < min_size - 1e-12:
        raise ValueError(f"computed qty {qty} < min_order_size {min_size}")
    position_cap = float(qty * inventory_multiple)
    gamma_code = float(risk_aversion_per_usdt * ctval * legal_tick)
    return {
        "exchange": spec["exchange"],
        "native_symbol": spec["native_symbol"],
        "base_symbol": spec["base_symbol"],
        "tick": tick,
        "legal_quote_tick": legal_tick,
        "quote_tick_mult": qmult,
        "lot": lot,
        "min_order_size": min_size,
        "ctval": ctval,
        "qty": qty,
        "position_cap": position_cap,
        "gamma_code": gamma_code,
        "approx_order_notional_usdt": round(qty * ctval * pretrade_mid, 4),
        "pretrade_mid_usdt": round(pretrade_mid, 4),
        "tick_relative_bps": round(10_000.0 * legal_tick / pretrade_mid, 6),
        "maker_fee_rate": float(maker_fee_rate),
        "taker_fee_rate": float(taker_fee_rate),
        "api_maker_fee_rate": float(spec["api_maker_fee_rate"]),
        "api_taker_fee_rate": float(spec["api_taker_fee_rate"]),
        "spec_sha256": contract_spec_digest(spec),
    }
