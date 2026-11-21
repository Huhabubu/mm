"""Shared channel taxonomy, stream health rules, and complete UTC day coverage."""
from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

CHANNEL_CATEGORY_CONTINUOUS_QUOTE = "continuous_quote"
CHANNEL_CATEGORY_DEPTH = "depth"
CHANNEL_CATEGORY_EVENT_TRADE = "event_trade"
CHANNEL_CATEGORY_OTHER = "other"

_CONTINUOUS_QUOTE_CHANNELS = frozenset({
    "bookTicker",
    "orderbook.1",
    "books1",
    "futures.book_ticker",
})
_DEPTH_CHANNELS = frozenset({
    "depth@100ms",
    "orderbook.50",
    "books50",
    "futures.obu",
    "futures.obu.50",
})
_EVENT_TRADE_CHANNELS = frozenset({
    "aggTrade",
    "publicTrade",
    "futures.trades",
})


def classify_channel(exchange: str, channel: str) -> str:
    """Shared channel taxonomy used by both live monitoring and offline/incremental QA."""
    _ = exchange
    if channel in _CONTINUOUS_QUOTE_CHANNELS:
        return CHANNEL_CATEGORY_CONTINUOUS_QUOTE
    if channel in _DEPTH_CHANNELS:
        return CHANNEL_CATEGORY_DEPTH
    if channel in _EVENT_TRADE_CHANNELS:
        return CHANNEL_CATEGORY_EVENT_TRADE
    return CHANNEL_CATEGORY_OTHER


def stream_owning_endpoint(exchange: str, channel: str) -> str:
    """Return the exact collector WS endpoint owning `(exchange, channel)`."""
    if exchange == "binance_futures":
        return "binance_market" if channel == "aggTrade" else "binance_public"
    if exchange == "bybit_linear":
        return "bybit_public"
    if exchange == "bitget_v3":
        return "bitget_public"
    if exchange == "gate_futures":
        return "gate_public"
    return f"{exchange}_public"


def channel_silence_thresholds(
    config: dict[str, Any] | None = None,
    *,
    default_stale_seconds: float | None = None,
) -> dict[str, float]:
    """Per-category silence thresholds shared by monitoring and QA.

    Continuous quote and depth feeds use strict silence ceilings; event-driven trade
    feeds separate normal quiet intervals (requiring channel-specific connection,
    subscription, and protocol evidence) from long droughts that require review.
    """
    meta = (config or {}).get("meta_daemon", {}) if isinstance(config, dict) else {}
    base = float(
        default_stale_seconds
        if default_stale_seconds is not None
        else meta.get("collector_stale_seconds", 300)
    )
    if base <= 0:
        raise ValueError("collector_stale_seconds must be positive")
    quote_s = float(meta.get("continuous_quote_stale_seconds", base))
    depth_s = float(meta.get("depth_stale_seconds", base))
    trade_s = float(meta.get("event_trade_stale_seconds", base))
    trade_quiet_max_s = float(meta.get("event_trade_quiet_max_seconds", max(trade_s, 1800.0)))
    for name, val in (
        ("continuous_quote_stale_seconds", quote_s),
        ("depth_stale_seconds", depth_s),
        ("event_trade_stale_seconds", trade_s),
        ("event_trade_quiet_max_seconds", trade_quiet_max_s),
    ):
        if val <= 0:
            raise ValueError(f"{name} must be positive")
    return {
        CHANNEL_CATEGORY_CONTINUOUS_QUOTE: quote_s,
        CHANNEL_CATEGORY_DEPTH: depth_s,
        CHANNEL_CATEGORY_EVENT_TRADE: trade_s,
        "event_trade_quiet_max": trade_quiet_max_s,
        CHANNEL_CATEGORY_OTHER: base,
    }


def evaluate_stream_activity_entry(
    *,
    exchange: str,
    symbol: str,
    channel: str,
    now_ns: int,
    entry: dict[str, Any] | None,
    endpoint_info: dict[str, Any] | None,
    thresholds: dict[str, float],
) -> dict[str, Any]:
    """Evaluate one expected `(exchange, symbol, channel)` across all four required states:
    1) `ws_connected` (长连接已建立)
    2) `subscription_evidenced` (订阅有证据)
    3) `received_valid_market_data` (收到有效行情)
    4) `written` (已写入)

    Sibling channel activity on the same exchange is NEVER treated as proof for this channel.
    """
    category = classify_channel(exchange, channel)
    endpoint = stream_owning_endpoint(exchange, channel)
    raw = entry or {}
    ep = endpoint_info or {}

    written_frames = int(raw.get("written_frames") or 0)
    last_written_ns = int(raw.get("last_written_wall_ns") or 0) if written_frames > 0 else None
    written_age_s = (
        (now_ns - last_written_ns) / 1e9
        if last_written_ns is not None and last_written_ns > 0
        else None
    )

    received_frames = int(raw.get("received_frames") or 0)
    if received_frames == 0 and written_frames > 0:
        # Legacy schema v1 telemetry only published written_frames.
        received_frames = written_frames
    last_received_raw = raw.get("last_received_wall_ns")
    last_received_ns = (
        int(last_received_raw)
        if last_received_raw is not None and int(last_received_raw) > 0
        else last_written_ns
    )
    buffered_wait_anchor_frames = int(raw.get("buffered_wait_anchor_frames") or 0)

    ws_connected: bool | None = raw.get("ws_connected")
    if ws_connected is None and "ws_connected" in ep:
        ws_connected = bool(ep["ws_connected"])

    protocol_healthy: bool | None = raw.get("protocol_healthy")
    if protocol_healthy is None and "protocol_healthy" in ep:
        protocol_healthy = bool(ep["protocol_healthy"])

    subscription_evidenced: bool | None = raw.get("subscription_evidenced")
    subscription_evidence = str(raw.get("subscription_evidence") or ("unknown" if subscription_evidenced is None else ""))
    if subscription_evidenced is None and written_frames > 0:
        subscription_evidenced = True
        subscription_evidence = "legacy_written_observed"

    received_valid = bool(raw.get("received_valid_market_data")) if "received_valid_market_data" in raw else (received_frames > 0)
    written = bool(raw.get("written")) if "written" in raw else (written_frames > 0)

    cat_limit = thresholds.get(category, thresholds[CHANNEL_CATEGORY_OTHER])
    trade_quiet_max = thresholds["event_trade_quiet_max"]

    written_fresh = written and written_age_s is not None and (-2.0 <= written_age_s <= cat_limit)

    if written_fresh:
        status = "ok"
        reason = "fresh_written"
    elif category in (CHANNEL_CATEGORY_CONTINUOUS_QUOTE, CHANNEL_CATEGORY_DEPTH):
        status = "confirmed_outage"
        if ws_connected is False:
            reason = "ws_disconnected"
        elif category == CHANNEL_CATEGORY_DEPTH and buffered_wait_anchor_frames > 0 and not written:
            reason = "depth_wait_anchor_not_written"
        elif protocol_healthy is False:
            reason = "protocol_unhealthy"
        elif not written:
            reason = "zero_written_frames"
        elif written_age_s is not None and written_age_s < -2.0:
            reason = "written_timestamp_in_future"
        else:
            reason = f"written_stale>{cat_limit:g}s"
    elif category == CHANNEL_CATEGORY_EVENT_TRADE:
        if ws_connected is False:
            status = "confirmed_outage"
            reason = "trade_endpoint_disconnected"
        elif protocol_healthy is False:
            status = "confirmed_outage"
            reason = "trade_endpoint_protocol_unhealthy"
        elif ws_connected is True and subscription_evidenced is True and protocol_healthy is True:
            if written_age_s is not None and (-2.0 <= written_age_s <= trade_quiet_max):
                status = "quiet_evidenced"
                reason = f"quiet_trade_within_{trade_quiet_max:g}s_with_channel_evidence"
            elif written_frames == 0 and subscription_evidence == "explicit_ack":
                status = "quiet_evidenced"
                reason = "quiet_trade_zero_fills_with_explicit_subscribe_ack"
            else:
                status = "needs_review"
                reason = (
                    f"quiet_trade_exceeded_{trade_quiet_max:g}s"
                    if written_age_s is not None
                    else "quiet_trade_without_explicit_ack_or_prior_trade"
                )
        else:
            status = "needs_review"
            missing_parts = []
            if ws_connected is not True:
                missing_parts.append("ws_connected")
            if subscription_evidenced is not True:
                missing_parts.append("subscription_evidence")
            if protocol_healthy is not True:
                missing_parts.append("protocol_healthy")
            reason = "insufficient_channel_evidence:" + ",".join(missing_parts or ["unknown"])
    else:
        status = "confirmed_outage"
        reason = "stale_or_missing"

    return {
        "exchange": exchange,
        "symbol": symbol,
        "channel": channel,
        "stream_key": f"{exchange}/{symbol}/{channel}",
        "category": category,
        "endpoint": endpoint,
        "ws_connected": ws_connected,
        "protocol_healthy": protocol_healthy,
        "subscription_evidenced": subscription_evidenced,
        "subscription_evidence": subscription_evidence,
        "received_valid_market_data": received_valid,
        "received_frames": received_frames,
        "last_received_wall_ns": last_received_ns,
        "buffered_wait_anchor_frames": buffered_wait_anchor_frames,
        "written": written,
        "written_frames": written_frames,
        "last_written_wall_ns": last_written_ns,
        "written_age_seconds": round(written_age_s, 3) if written_age_s is not None else None,
        "depth_state": raw.get("depth_state"),
        "status": status,
        "reason": reason,
    }


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
        self.thresholds = channel_silence_thresholds(config)
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
            {
                "exchange": ex,
                "symbol": sym,
                "channel": channel,
                "category": classify_channel(ex, channel),
                "hours_utc": missing,
            }
            for ex, sym, channel in sorted(self.expected)
            if (missing := sorted(hours - self.hours_by_stream[(ex, sym, channel)]))
        ]
        start_ns = self.start_s * 1_000_000_000
        end_ns = start_ns + 86_400_000_000_000
        stream_gaps = []
        for (exchange, symbol, channel), (first, last, internal_gap) in sorted(self.timing.items()):
            category = classify_channel(exchange, channel)
            limit_ns = int(self.thresholds.get(category, self.max_gap_ns / 1e9) * 1_000_000_000)
            longest = max(first - start_ns, end_ns - last, internal_gap)
            if longest > limit_ns:
                stream_gaps.append({
                    "exchange": exchange,
                    "symbol": symbol,
                    "channel": channel,
                    "category": category,
                    "max_gap_seconds": longest / 1e9,
                    "allowed_gap_seconds": limit_ns / 1e9,
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
            "category_thresholds_seconds": self.thresholds,
            "clock_certified_minutes": len(certified_minutes),
        }
