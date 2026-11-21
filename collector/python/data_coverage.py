"""Expected market coverage for complete UTC days."""
from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


def enabled_exchanges(config: dict[str, Any]) -> dict[str, Any]:
    exchanges = {
        name: exchange for name, exchange in config["exchanges"].items()
        if exchange.get("enabled")
    }
    if not exchanges or any(not exchange.get("symbols") for exchange in exchanges.values()):
        raise ValueError("coverage requires enabled exchanges with configured symbols")
    return exchanges


def expected_streams(config: dict[str, Any]) -> set[tuple[str, str, str]]:
    expected = set()
    for name, exchange in enabled_exchanges(config).items():
        channels = (
            exchange["public_channels"] + exchange["market_channels"]
            if name == "binance_futures" else exchange["channels"]
        )
        if not channels:
            raise ValueError(f"{name}: no channels configured")
        for channel in channels:
            stream = "futures.obu" if channel == "futures.obu.50" else channel
            expected.update((name, symbol.upper(), stream) for symbol in exchange["symbols"])
    return expected


def _data_streams(exchange: str, payload: dict[str, Any]) -> set[tuple[str, str, str]]:
    if exchange == "binance_futures":
        data = payload.get("data", payload)
        channel = {"depthUpdate": "depth@100ms"}.get(data.get("e"), data.get("e"))
        if not channel:
            channel = payload.get("stream", "").partition("@")[2]
        symbols = [data.get("s")]
    elif exchange == "bybit_linear":
        channel, _, symbol = payload.get("topic", "").rpartition(".")
        symbols = [symbol]
    elif exchange == "bitget_v3":
        arg = payload.get("arg", {})
        channel = arg.get("topic") or arg.get("channel")
        symbols = [arg.get("symbol") or arg.get("instId")]
    elif exchange == "gate_futures":
        channel = payload.get("channel")
        result = payload.get("result")
        if channel == "futures.trades":
            trades = result if isinstance(result, list) else [result]
            symbols = [trade.get("contract") for trade in trades if isinstance(trade, dict)]
        else:
            symbol = result.get("s", "") if isinstance(result, dict) else ""
            symbols = [symbol.split(".")[1] if symbol.startswith("ob.") else symbol]
    else:
        return set()
    return {(exchange, sym.upper(), channel) for sym in symbols if sym and channel}


class CompleteDayCoverage:
    def __init__(self, config: dict[str, Any], date_utc: str) -> None:
        self.date = date.fromisoformat(date_utc).isoformat()
        start = datetime.fromisoformat(self.date).replace(tzinfo=timezone.utc)
        self.start_s = int(start.timestamp())
        self.expected = expected_streams(config)
        max_gap_s = int(config.get("meta_daemon", {}).get("collector_stale_seconds", 300))
        if max_gap_s <= 0:
            raise ValueError("collector_stale_seconds must be positive")
        self.max_gap_ns = max_gap_s * 1_000_000_000
        self.partitions: set[tuple[str, int]] = set()
        self.hours_by_stream: dict[tuple[str, str, str], set[int]] = defaultdict(set)
        self.timing: dict[tuple[str, str, str], tuple[int, int, int]] = {}

    def observe_partition(self, manifest: dict[str, Any]) -> None:
        if manifest["date_utc"] == self.date:
            self.partitions.add((manifest["exchange"], int(manifest["hour_utc"])))

    def observe_record(self, exchange: str, record: dict[str, Any]) -> None:
        if record["kind"] not in ("DATA", "DATA_ESCAPED"):
            return
        wall_ns = record["recv_wall_ns"]
        hour = (wall_ns // 1_000_000_000 - self.start_s) // 3600
        if 0 <= hour < 24:
            for stream in _data_streams(exchange, json.loads(record["payload"])) & self.expected:
                self.hours_by_stream[stream].add(hour)
                first, last, gap = self.timing.get(stream, (wall_ns, wall_ns, 0))
                self.timing[stream] = (min(first, wall_ns), max(last, wall_ns), max(gap, wall_ns - last))

    def report(self, meta_dir: Path | None) -> dict[str, Any]:
        certified_minutes = set()
        if meta_dir is not None:
            path = meta_dir / "chrony" / f"chrony_{self.date.replace('-', '')}.csv"
            if path.is_file():
                with path.open(encoding="utf-8", newline="") as handle:
                    for row in csv.DictReader(handle):
                        state = row.get("certified_le_1ms", row.get("certified_under_1ms", ""))
                        wall = row.get("wall_ns") or row.get("ts_utc_ns")
                        if wall and state.strip().lower() == "true":
                            minute = (int(wall) // 1_000_000_000 - self.start_s) // 60
                            if 0 <= minute < 1440:
                                certified_minutes.add(minute)
        hours = set(range(24))
        missing_partitions = [
            {"exchange": exchange, "hour_utc": hour}
            for exchange in sorted({key[0] for key in self.expected}) for hour in range(24)
            if (exchange, hour) not in self.partitions
        ]
        missing_stream_hours = [
            {"exchange": ex, "symbol": sym, "channel": channel, "hours_utc": missing}
            for ex, sym, channel in sorted(self.expected)
            if (missing := sorted(hours - self.hours_by_stream[(ex, sym, channel)]))
        ]
        start_ns = self.start_s * 1_000_000_000
        end_ns = start_ns + 86_400_000_000_000
        stream_gaps = []
        for (exchange, symbol, channel), (first, last, internal_gap) in sorted(self.timing.items()):
            longest = max(first - start_ns, end_ns - last, internal_gap)
            if longest > self.max_gap_ns:
                stream_gaps.append({
                    "exchange": exchange, "symbol": symbol, "channel": channel,
                    "max_gap_seconds": longest / 1e9,
                })
        complete = bool(
            len(certified_minutes) == 1440 and not missing_partitions and not missing_stream_hours and not stream_gaps
        )
        return {
            "status": "complete" if complete else "incomplete",
            "date_utc": self.date,
            "expected_stream_count": len(self.expected),
            "missing_partitions": missing_partitions,
            "missing_stream_hours": missing_stream_hours,
            "stream_gaps": stream_gaps,
            "allowed_gap_seconds": self.max_gap_ns / 1e9,
            "clock_certified_minutes": len(certified_minutes),
        }
