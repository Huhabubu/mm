"""Validate archived depth update chains independently of the live collector."""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
import json
import math
from pathlib import Path
from typing import Any

import zstandard as zstd

BINANCE = "binance_futures"
GATE = "gate_futures"
BYBIT = "bybit_linear"
BITGET = "bitget_v3"


def symbol_key(value: str) -> str:
    return value.upper().replace("_", "")


def rows(path: Path):
    with path.open("rb") as raw, zstd.open(raw, "rt", encoding="utf-8") as reader:
        for line in reader:
            wall, mono, conn, epoch, seq, kind, payload = line.rstrip("\n").split("\t", 6)
            obj = json.loads(payload)
            if kind == "DATA_ESCAPED" and isinstance(obj, str):
                obj = json.loads(obj)
            if not isinstance(obj, dict):
                raise ValueError("archive payload is not a JSON object")
            yield int(wall), conn, int(epoch), kind, obj


@dataclass
class Depth:
    symbol: str
    connection: tuple[str, str, int]
    mode: str
    seq: int
    parent: int | None = None
    bids: list | None = None
    asks: list | None = None
    exchange_ms: Any = None


def depth_event(exchange: str, boot: str, conn: str, epoch: int, kind: str, obj: dict) -> Depth | None:
    key = (boot, conn, epoch)
    if exchange == BINANCE:
        if kind == "META":
            event = obj.get("event")
            if event in {"REST_SNAPSHOT_ANCHOR", "REST_SNAPSHOT"}:
                data = obj.get("snapshot") or obj["raw"]
                return Depth(
                    symbol_key(obj["symbol"]), key, "snapshot", int(data["lastUpdateId"]),
                    bids=data.get("bids", []), asks=data.get("asks", []),
                    exchange_ms=data.get("T") or data.get("E"),
                )
            if event != "DEPTH_BRIDGED_DELTA":
                return None
            obj = obj["raw"]
            if isinstance(obj, str):
                obj = json.loads(obj)
        data = obj.get("data", obj)
        if data.get("e") != "depthUpdate":
            return None
        return Depth(symbol_key(data["s"]), key, "delta", int(data["u"]), int(data["pu"]),
                     data.get("b", []), data.get("a", []), (data.get("T") or data.get("E"), int(data["U"])))
    if kind == "META":
        return None
    if exchange == GATE and obj.get("channel") == "futures.obu" and obj.get("event") == "update":
        data = obj["result"]
        s = data["s"].split(".")
        if len(s) != 3 or s[0] != "ob" or s[-1] != "50":
            return None
        snap = bool(data.get("full"))
        return Depth(symbol_key(s[1]), key, "snapshot" if snap else "delta", int(data["u"]),
                     None if snap else int(data["U"]), data.get("b", []), data.get("a", []), data.get("t"))
    if exchange == BYBIT and obj.get("topic", "").startswith("orderbook.50."):
        data = obj["data"]
        seq = int(data["u"])
        mode = "snapshot" if seq == 1 else obj["type"]
        return Depth(symbol_key(data.get("s") or obj["topic"].split(".")[-1]), key, mode, seq,
                     bids=data.get("b", []), asks=data.get("a", []), exchange_ms=obj.get("cts") or obj.get("ts"))
    if exchange == BITGET:
        arg = obj.get("arg") or {}
        if (arg.get("topic") or arg.get("channel")) != "books50":
            return None
        if len(obj.get("data") or []) != 1:
            raise ValueError("books50 expected a single depth record for the symbol")
        data = obj["data"][0]
        mode = "snapshot" if obj["action"] == "snapshot" else obj["action"]
        return Depth(symbol_key(arg.get("symbol") or arg["instId"]), key, mode, int(data["seq"]),
                     None if mode == "snapshot" else int(data["pseq"]),
                     data.get("b") or data.get("bids") or [], data.get("a") or data.get("asks") or [],
                     data.get("ts") or obj.get("ts"))
    return None


@dataclass
class Chain:
    connection: tuple[str, str, int] | None = None
    seq: int | None = None
    awaiting_delta: bool = False

    def apply(self, exchange: str, event: Depth) -> str | None:
        if event.mode == "snapshot":
            self.connection, self.seq = event.connection, event.seq
            self.awaiting_delta = exchange in {BINANCE, BITGET}
            return None
        if event.mode not in {"delta", "update"}:
            return "invalid_depth_action"
        if self.seq is None or self.connection != event.connection:
            self.seq = None
            return "missing_same_connection_anchor"
        if exchange == BINANCE:
            first = event.exchange_ms[1]
            valid = first <= self.seq <= event.seq if self.awaiting_delta else event.parent == self.seq
            valid = valid and first <= event.seq and event.seq >= self.seq
        elif exchange == GATE:
            valid = event.parent == self.seq + 1 and event.seq >= event.parent
        elif exchange == BITGET:
            valid = event.parent <= self.seq <= event.seq if self.awaiting_delta else event.parent == self.seq
            valid = valid and event.parent < event.seq
        else:
            valid = event.seq >= self.seq
        if not valid:
            self.seq = None
            return "exchange_update_chain_break"
        self.seq, self.awaiting_delta = event.seq, False
        return None


@dataclass
class Book:
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)

    def apply(self, event: Depth):
        if event.mode == "snapshot":
            self.bids.clear()
            self.asks.clear()
        for side, updates in ((self.bids, event.bids), (self.asks, event.asks)):
            for price, size, *_ in updates or ():
                px, qty = float(price), float(size)
                if not math.isfinite(px) or not math.isfinite(qty) or px <= 0 or qty < 0:
                    raise ValueError("invalid price or size in archived depth update")
                if qty:
                    side[px] = qty
                else:
                    side.pop(px, None)

    def crossed(self) -> bool:
        return bool(self.bids and self.asks and max(self.bids) >= min(self.asks))


def verify_gate_sizes(event: Depth) -> bool:
    return all(isinstance(level[1], str) for levels in (event.bids or [], event.asks or []) for level in levels)


@dataclass
class ProtocolReport:
    frames: Counter = field(default_factory=Counter)
    issues: Counter = field(default_factory=Counter)

    def count(self, exchange: str, symbol: str, issue: str):
        self.issues[(exchange, symbol, issue)] += 1

    def json(self):
        return {
            "frames": [{"exchange": e, "symbol": s, "count": n} for (e, s), n in sorted(self.frames.items())],
            "issues": [
                {"exchange": e, "symbol": s, "issue": issue, "count": n}
                for (e, s, issue), n in sorted(self.issues.items())
            ],
        }
