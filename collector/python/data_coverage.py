"""Shared channel taxonomy, stream health rules, and complete UTC day coverage."""
from __future__ import annotations

import csv
import json
import re
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


VALID_PARTITION_MINUTES = (5, 15, 30, 60)
_NS_PER_SEC = 1_000_000_000
_NS_PER_MIN = 60 * _NS_PER_SEC
_SUBHOUR_STEM_RE = re.compile(r"_(\d{8})_(\d{2})(\d{2})_p(\d+)m(?:_|$)")
_HOUR_STEM_RE = re.compile(r"_(\d{8})_(\d{2})(?:_|$)")


def configured_partition_minutes(config: dict[str, Any] | None) -> int:
    """Return configured UTC partition window size in minutes (defaults to 60 if omitted)."""
    dep = (config or {}).get("deployment", {}) if isinstance(config, dict) else {}
    raw = dep.get("partition_minutes", 60)
    try:
        mins = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid deployment.partition_minutes: {raw!r}") from exc
    if mins not in VALID_PARTITION_MINUTES:
        raise ValueError(f"deployment.partition_minutes must be one of {VALID_PARTITION_MINUTES}, got {mins}")
    return mins


def format_window_id(start_epoch_s: int) -> str:
    dt = datetime.fromtimestamp(start_epoch_s, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%MZ")


def parse_window_start_s(spec: str) -> int:
    """Parse `YYYY-MM-DDTHH`, `YYYY-MM-DDTHH:MM`, or `YYYY-MM-DDTHH:MMZ` into UTC epoch seconds."""
    text = spec.strip()
    if text.endswith("Z"):
        text = text[:-1]
    for fmt in ("%Y-%m-%dT%H:%M", "%Y-%m-%dT%H"):
        try:
            dt = datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except ValueError:
            continue
    raise ValueError(f"invalid UTC window specifier: {spec!r} (expected YYYY-MM-DDTHH or YYYY-MM-DDTHH:MMZ)")


def window_descriptor(start_epoch_s: int, partition_minutes: int) -> dict[str, Any]:
    if partition_minutes not in VALID_PARTITION_MINUTES:
        raise ValueError(f"invalid partition_minutes: {partition_minutes}")
    step_s = partition_minutes * 60
    aligned_s = (int(start_epoch_s) // step_s) * step_s
    end_s = aligned_s + step_s
    dt = datetime.fromtimestamp(aligned_s, tz=timezone.utc)
    return {
        "partition_minutes": partition_minutes,
        "date_utc": dt.strftime("%Y-%m-%d"),
        "yyyymmdd": dt.strftime("%Y%m%d"),
        "hour_utc": dt.strftime("%H"),
        "minute_utc": dt.strftime("%M"),
        "window_id": dt.strftime("%Y-%m-%dT%H:%MZ"),
        "planned_start_s": aligned_s,
        "planned_end_s": end_s,
        "planned_start_wall_ns": aligned_s * _NS_PER_SEC,
        "planned_end_wall_ns": end_s * _NS_PER_SEC,
    }


def previous_complete_window(now_s: float, partition_minutes: int) -> dict[str, Any]:
    """Return the immediately preceding complete UTC-aligned `[start, end)` window."""
    if partition_minutes not in VALID_PARTITION_MINUTES:
        raise ValueError(f"invalid partition_minutes: {partition_minutes}")
    step_s = partition_minutes * 60
    cur_start_s = (int(now_s) // step_s) * step_s
    return window_descriptor(cur_start_s - step_s, partition_minutes)


def iter_expected_windows(
    start_spec: str,
    end_exclusive_s: float,
    partition_minutes: int,
) -> list[dict[str, Any]]:
    """Enumerate all UTC-aligned `[start, end)` windows starting >= `start_spec` whose `end <= end_exclusive_s`."""
    if partition_minutes not in VALID_PARTITION_MINUTES:
        raise ValueError(f"invalid partition_minutes: {partition_minutes}")
    step_s = partition_minutes * 60
    start_raw_s = parse_window_start_s(start_spec)
    # Align up if start_spec wasn't already on a partition boundary
    rem = start_raw_s % step_s
    cur_s = start_raw_s if rem == 0 else start_raw_s + (step_s - rem)
    windows: list[dict[str, Any]] = []
    limit_s = int(end_exclusive_s)
    while cur_s + step_s <= limit_s:
        windows.append(window_descriptor(cur_s, partition_minutes))
        cur_s += step_s
    return windows


def normalize_manifest_window(
    manifest: dict[str, Any],
    *,
    fallback_file_name: str | None = None,
) -> dict[str, Any]:
    """Normalize both legacy v1 hourly manifests and v2 short-partition/segmented manifests."""
    exchange = str(manifest["exchange"])
    date_utc = str(manifest["date_utc"])
    hour_utc = str(manifest["hour_utc"]).zfill(2)
    file_name = str(manifest.get("file_name") or fallback_file_name or "")
    stem = Path(file_name).name
    for suf in (".tsv.zst", ".manifest.json"):
        if stem.endswith(suf):
            stem = stem[: -len(suf)]

    raw_pm = manifest.get("partition_minutes")
    raw_min = manifest.get("minute_utc")
    if raw_pm is not None:
        partition_minutes = int(raw_pm)
    else:
        m_sub = _SUBHOUR_STEM_RE.search(stem)
        partition_minutes = int(m_sub.group(4)) if m_sub else 60
    if partition_minutes not in VALID_PARTITION_MINUTES:
        raise ValueError(f"unsupported partition_minutes in manifest: {partition_minutes}")

    if raw_min is not None and str(raw_min) != "":
        minute_utc = str(raw_min).zfill(2)
    else:
        m_sub = _SUBHOUR_STEM_RE.search(stem)
        minute_utc = m_sub.group(3) if m_sub else "00"

    day_start = int(datetime.fromisoformat(date_utc).replace(tzinfo=timezone.utc).timestamp())
    computed_start_s = day_start + int(hour_utc) * 3600 + int(minute_utc) * 60
    computed_start_ns = computed_start_s * _NS_PER_SEC
    computed_end_ns = computed_start_ns + partition_minutes * _NS_PER_MIN

    planned_start_ns = int(manifest.get("planned_start_wall_ns") or computed_start_ns)
    planned_end_ns = int(manifest.get("planned_end_wall_ns") or computed_end_ns)
    window_id = str(manifest.get("window_id") or format_window_id(planned_start_ns // _NS_PER_SEC))
    segment_id = str(manifest.get("segment_id") or manifest.get("partition_id") or stem)
    boot_id = str(manifest.get("boot_id") or "unknown_boot")
    run_id = manifest.get("run_id")

    return {
        "exchange": exchange,
        "date_utc": date_utc,
        "hour_utc": hour_utc,
        "minute_utc": minute_utc,
        "partition_minutes": partition_minutes,
        "window_id": window_id,
        "planned_start_wall_ns": planned_start_ns,
        "planned_end_wall_ns": planned_end_ns,
        "planned_start_s": planned_start_ns // _NS_PER_SEC,
        "planned_end_s": planned_end_ns // _NS_PER_SEC,
        "boot_id": boot_id,
        "run_id": str(run_id) if run_id not in (None, "") else None,
        "segment_id": segment_id,
        "file_name": file_name,
        "is_startup_stub": bool(manifest.get("is_startup_stub", False)),
        "settled_by_timer": bool(manifest.get("settled_by_timer", False)),
        "coverage_incomplete": bool(manifest.get("coverage_incomplete", False)),
        "late_frames_accepted": int(manifest.get("late_frames_accepted") or 0),
        "late_frames_rejected": int(manifest.get("late_frames_rejected") or 0),
    }


def parse_partition_window_from_rel_path(rel_path: str) -> dict[str, Any] | None:
    """Best-effort window parser from legacy hourly or new sub-hour/segmented manifest path."""
    parts = rel_path.strip("/").split("/")
    if parts and parts[0] == "raw":
        parts = parts[1:]
    if len(parts) != 3:
        return None
    exchange, date_utc, filename = parts
    stem = filename
    for suf in (".manifest.json", ".tsv.zst", ".tsv.zst.partial"):
        if stem.endswith(suf):
            stem = stem[: -len(suf)]
            break
    m_sub = _SUBHOUR_STEM_RE.search(stem)
    if m_sub:
        ymd, hh, mm, pm_s = m_sub.groups()
        pm = int(pm_s)
        if pm in VALID_PARTITION_MINUTES:
            day_s = int(datetime.strptime(ymd, "%Y%m%d").replace(tzinfo=timezone.utc).timestamp())
            start_s = day_s + int(hh) * 3600 + int(mm) * 60
            return {"exchange": exchange, "segment_id": stem, **window_descriptor(start_s, pm)}
    m_hr = _HOUR_STEM_RE.search(stem)
    if m_hr:
        ymd, hh = m_hr.groups()
        day_s = int(datetime.strptime(ymd, "%Y%m%d").replace(tzinfo=timezone.utc).timestamp())
        start_s = day_s + int(hh) * 3600
        return {"exchange": exchange, "segment_id": stem, **window_descriptor(start_s, 60)}
    return None


def effective_config_hash(config: dict[str, Any]) -> str:
    """Deterministic SHA-256 fingerprint of effective collection & QA parameters."""
    import hashlib

    enabled = enabled_exchanges(config)
    canonical = {
        "partition_minutes": configured_partition_minutes(config),
        "thresholds": channel_silence_thresholds(config),
        "exchanges": {
            ex: {
                "symbols": sorted(str(s).upper() for s in spec.get("symbols", [])),
                "public_channels": sorted(spec.get("public_channels", [])),
                "market_channels": sorted(spec.get("market_channels", [])),
                "channels": sorted(spec.get("channels", [])),
            }
            for ex, spec in sorted(enabled.items())
        },
    }
    return hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def record_monotonic_producer_key(
    exchange: str,
    kind: str,
    conn_id: str,
    epoch: int,
    payload_obj: Any,
) -> tuple[str, str, int]:
    """Identify the monotonic clock producer `(producer, conn_id, epoch)` for a canonical record.

    Within a single Rust process, separate Tokio tasks (`binance_public` A/B, `binance_market` A/B,
    REST snapshot fetchers, and watchdog timers) stamp `recv_mono_ns` before sending to the shared
    `mpsc` channel. Receive monotonic clock monotonicity MUST hold per `(producer, conn_id, epoch)`,
    while file write order (`rx_seq`) is strictly monotonic across the entire file.
    """
    if kind == "META":
        if isinstance(payload_obj, dict):
            ev = str(payload_obj.get("event") or "")
            ep = str(payload_obj.get("endpoint") or "meta")
            if ev in ("REST_SNAPSHOT", "REST_SNAPSHOT_ANCHOR", "DEPTH_BRIDGED_DELTA", "SNAPSHOT_BRIDGE_RETRY"):
                return (f"{ep}:rest_snapshot", conn_id, epoch)
            if ev in ("SILENCE_DIAGNOSTIC", "GAP_START", "GAP_END", "LOCAL_LOSS", "LATE_FRAME_REJECTED"):
                return (f"{ep}:meta_control", conn_id, epoch)
            return (f"{ep}:meta", conn_id, epoch)
        return ("meta", conn_id, epoch)

    if exchange == "binance_futures" and isinstance(payload_obj, dict):
        data = payload_obj.get("data", payload_obj)
        stream = str(payload_obj.get("stream") or "")
        ev = str(data.get("e") or "") if isinstance(data, dict) else ""
        if ev == "aggTrade" or "@aggTrade" in stream:
            return ("binance_market", conn_id, epoch)
        return ("binance_public", conn_id, epoch)

    return (f"{exchange}_ws", conn_id, epoch)


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
        self.partition_minutes = configured_partition_minutes(config)
        self.window_seconds = self.partition_minutes * 60
        self.windows_per_day = 86_400 // self.window_seconds
        self.expected = expected_streams(config)
        self.thresholds = channel_silence_thresholds(config)
        max_gap_s = int(config.get("meta_daemon", {}).get("collector_stale_seconds", 300))
        if max_gap_s <= 0:
            raise ValueError("collector_stale_seconds must be positive")
        self.max_gap_ns = max_gap_s * _NS_PER_SEC
        self.partitions: set[tuple[str, int]] = set()
        self.window_segments: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
        self.windows_by_stream: dict[tuple[str, str, str], set[int]] = defaultdict(set)
        self.hours_by_stream: dict[tuple[str, str, str], set[int]] = defaultdict(set)
        self.timing: dict[tuple[str, str, str], tuple[int, int, int]] = {}

    def observe_partition(self, manifest: dict[str, Any]) -> None:
        norm = normalize_manifest_window(manifest)
        if norm["date_utc"] != self.date:
            return
        idx = (norm["planned_start_s"] - self.start_s) // self.window_seconds
        if 0 <= idx < self.windows_per_day:
            self.partitions.add((norm["exchange"], idx))
            self.window_segments[(norm["exchange"], idx)].append(norm)
            # If a legacy 60m partition covers a day configured with shorter windows, mark all covered sub-windows
            if norm["partition_minutes"] > self.partition_minutes:
                span = norm["partition_minutes"] // self.partition_minutes
                for offset in range(span):
                    if 0 <= idx + offset < self.windows_per_day:
                        self.partitions.add((norm["exchange"], idx + offset))

    def observe_record(self, exchange: str, record: dict[str, Any]) -> None:
        if record["kind"] not in ("DATA", "DATA_ESCAPED"):
            return
        wall_ns = record["recv_wall_ns"]
        sec_offset = wall_ns // _NS_PER_SEC - self.start_s
        hour = sec_offset // 3600
        win_idx = sec_offset // self.window_seconds
        if 0 <= hour < 24 and 0 <= win_idx < self.windows_per_day:
            payload = record["payload"]
            payload_obj = json.loads(payload) if isinstance(payload, str) else payload
            for stream in _data_streams(exchange, payload_obj) & self.expected:
                self.hours_by_stream[stream].add(hour)
                self.windows_by_stream[stream].add(win_idx)
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
                            minute = (int(wall) // _NS_PER_SEC - self.start_s) // 60
                            if 0 <= minute < 1440:
                                certified_minutes.add(minute)
        missing_partitions = []
        for exchange in sorted({key[0] for key in self.expected}):
            for idx in range(self.windows_per_day):
                if (exchange, idx) not in self.partitions:
                    w_start_s = self.start_s + idx * self.window_seconds
                    wd = window_descriptor(w_start_s, self.partition_minutes)
                    item: dict[str, Any] = {
                        "exchange": exchange,
                        "window_id": wd["window_id"],
                        "hour_utc": int(wd["hour_utc"]),
                        "minute_utc": int(wd["minute_utc"]),
                    }
                    missing_partitions.append(item)
        hours = set(range(24))
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
        all_windows = set(range(self.windows_per_day))
        missing_stream_windows = [
            {
                "exchange": ex,
                "symbol": sym,
                "channel": channel,
                "category": classify_channel(ex, channel),
                "missing_window_count": len(missing_w),
                "window_ids": [
                    format_window_id(self.start_s + idx * self.window_seconds) for idx in missing_w[:12]
                ],
            }
            for ex, sym, channel in sorted(self.expected)
            if (
                classify_channel(ex, channel) != CHANNEL_CATEGORY_EVENT_TRADE
                and (missing_w := sorted(all_windows - self.windows_by_stream[(ex, sym, channel)]))
            )
        ]
        start_ns = self.start_s * _NS_PER_SEC
        end_ns = start_ns + 86_400 * _NS_PER_SEC
        stream_gaps = []
        for (exchange, symbol, channel), (first, last, internal_gap) in sorted(self.timing.items()):
            category = classify_channel(exchange, channel)
            limit_s = (
                self.thresholds["event_trade_quiet_max"]
                if category == CHANNEL_CATEGORY_EVENT_TRADE
                else self.thresholds.get(category, self.max_gap_ns / 1e9)
            )
            limit_ns = int(limit_s * _NS_PER_SEC)
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
            len(certified_minutes) == 1440
            and not missing_partitions
            and not missing_stream_hours
            and not missing_stream_windows
            and not stream_gaps
        )
        return {
            "status": "complete" if complete else "incomplete",
            "date_utc": self.date,
            "partition_minutes": self.partition_minutes,
            "expected_windows_per_day": self.windows_per_day,
            "expected_stream_count": len(self.expected),
            "missing_partitions": missing_partitions,
            "missing_stream_hours": missing_stream_hours,
            "missing_stream_windows": missing_stream_windows,
            "stream_gaps": stream_gaps,
            "allowed_gap_seconds": self.max_gap_ns / 1e9,
            "category_thresholds_seconds": self.thresholds,
            "clock_certified_minutes": len(certified_minutes),
        }
