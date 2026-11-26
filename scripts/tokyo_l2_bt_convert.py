#!/usr/bin/env python3
"""Convert one fetched Tokyo L2 UTC day to HBT NPZ for ONE Gate symbol and its Binance leader.

The converter itself is the frozen collector `qa_convert.run_qa_convert` (unchanged):
SHA-256 + zstd integrity, rx_seq / recv_mono monotonicity, LOCAL_LOSS / GAP / chrony
gates, complete-UTC-day coverage, strict depth-chain rebuild and the HBT 2.4.4
event ordering are all its own logic.

Only its input is narrowed, so one (symbol, day) fits a single GitHub runner:
- records of other symbols are dropped before ingestion;
- Binance keeps only @bookTicker (the leader BBO). Its depth and aggTrade are
  diagnostics in qa_convert and are not read by the backtest;
- complete-day coverage is evaluated against the same narrowed stream set.
Filtering never reorders records, so the monotonic checks stay meaningful.
"""
from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path
import sys
import time
from typing import Any, Callable, Iterable

LEADER = "binance_futures"
FOLLOWER = "gate_futures"
BINANCE_DEPTH_META = {
    "REST_SNAPSHOT", "REST_SNAPSHOT_ANCHOR", "DEPTH_BRIDGED_DELTA",
    "DEPTH_UNAVAILABLE", "SNAPSHOT_BRIDGE_RETRY",
}


def base_asset(raw: str) -> str:
    """Same normalization as qa_convert.canonical_asset (kept import-free for tests)."""
    s = str(raw or "").upper()
    if s.startswith("OB."):
        parts = s.split(".")
        if len(parts) >= 2:
            s = parts[1]
    if s.endswith("_USDT"):
        return s[:-5]
    if s.endswith("USDT"):
        return s[:-4]
    return s


def native_symbols(gate_symbol: str) -> tuple[str, str, str]:
    if not gate_symbol.endswith("_USDT"):
        raise ValueError("Gate symbol must be <BASE>_USDT")
    base = gate_symbol[:-5]
    return base, f"{base}USDT", gate_symbol


def narrowed_config(
    config: dict[str, Any],
    gate_symbol: str,
    making_exchange: str = FOLLOWER,
    include_leader: bool = True,
) -> dict[str, Any]:
    if making_exchange == FOLLOWER:
        _, binance_sym, follower_sym = native_symbols(gate_symbol)
    else:
        base = base_asset(gate_symbol)
        binance_sym = f"{base}USDT"
        follower_sym = f"{base}USDT"
    out = copy.deepcopy(config)
    enabled_set = {making_exchange} | ({LEADER} if include_leader else set())
    for name, ex in out["exchanges"].items():
        ex["enabled"] = name in enabled_set
    f = out["exchanges"][making_exchange]
    if follower_sym not in f["symbols"]:
        raise ValueError(f"{gate_symbol} ({making_exchange}) is not collected by this config")
    f["symbols"] = [follower_sym]
    if include_leader:
        b = out["exchanges"][LEADER]
        if binance_sym not in b["symbols"]:
            raise ValueError(f"{binance_sym} ({LEADER}) is not collected by this config")
        if "bookTicker" not in b["public_channels"]:
            raise ValueError("Binance bookTicker is not collected")
        b["symbols"] = [binance_sym]
        b["public_channels"] = ["bookTicker"]
        b["market_channels"] = []
    return out


def narrowed_binance_config(config: dict[str, Any], base_symbols: Iterable[str]) -> dict[str, Any]:
    """Collector config narrowed to Binance @bookTicker for one or more base symbols."""
    syms = [f"{base_asset(s)}USDT" for s in base_symbols]
    if not syms:
        raise ValueError("base_symbols must not be empty")
    out = copy.deepcopy(config)
    for name, ex in out["exchanges"].items():
        ex["enabled"] = name == LEADER
    b = out["exchanges"][LEADER]
    missing = [s for s in syms if s not in b["symbols"]]
    if missing:
        raise ValueError(f"Binance symbols not collected by config: {missing}")
    if "bookTicker" not in b["public_channels"]:
        raise ValueError("Binance bookTicker is not collected")
    b["symbols"] = sorted(set(syms))
    b["public_channels"] = ["bookTicker"]
    b["market_channels"] = []
    return out


def _meta_keep(obj: Any, exchange: str, base: str) -> bool:
    if not isinstance(obj, dict):
        return True
    if exchange == LEADER and str(obj.get("event") or "") in BINANCE_DEPTH_META:
        return False
    raw = str(obj.get("symbol") or "ALL").strip()
    return not raw or raw.upper() == "ALL" or base_asset(raw) == base


def _data_keep(obj: Any, exchange: str, base: str, stats: dict[str, int]) -> bool | dict[str, Any]:
    """True = keep as is, False = drop, dict = keep with this narrowed payload."""
    if not isinstance(obj, dict):
        return False
    if exchange == LEADER:
        data = obj.get("data") if isinstance(obj.get("data"), dict) else obj
        is_bbo = data.get("e") == "bookTicker" or str(obj.get("stream", "")).endswith("@bookTicker")
        return bool(is_bbo and base_asset(data.get("s")) == base)
    channel = obj.get("channel", "")
    result = obj.get("result")
    if channel == "futures.trades":
        trades = result if isinstance(result, list) else [result]
        mine = [t for t in trades if isinstance(t, dict) and base_asset(t.get("contract")) == base]
        if not mine:
            return False
        if len(mine) == len(trades):
            return True
        # Batched multi-contract frame: keep only target trades so no partial
        # buffers/NPZ are created for other contracts.
        stats["gate_mixed_symbol_trade_messages"] += 1
        return {**obj, "result": mine}
    if isinstance(result, dict) and "s" in result:
        return base_asset(result["s"]) == base
    return False


def filter_records(records: Iterable[dict[str, Any]], exchange: str, gate_symbol: str,
                   stats: dict[str, int]) -> list[dict[str, Any]]:
    base, binance_sym, gate_sym = native_symbols(gate_symbol)
    needle = binance_sym if exchange == LEADER else gate_sym
    kept = []
    prev_rx_seq: int | None = None
    for rec in records:
        stats["records_in"] += 1
        # qa_convert checks rx_seq only on the records it sees; check the full file first so
        # dropped records cannot hide a non-monotonic / duplicated write order.
        rx_seq = int(rec["rx_seq"])
        if prev_rx_seq is not None and rx_seq <= prev_rx_seq:
            stats["prefilter_rx_seq_violations"] += 1
        prev_rx_seq = rx_seq
        payload = rec["payload"]
        text = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False)
        obj_of = (lambda: json.loads(payload)) if isinstance(payload, str) else (lambda: payload)
        if rec["kind"] == "META":
            keep = _meta_keep(obj_of(), exchange, base)
        elif needle not in text:         # every target-symbol frame names its symbol
            keep = False
        else:
            keep = _data_keep(obj_of(), exchange, base, stats)
        if keep:
            if isinstance(keep, dict):
                rec = {**rec, "payload": json.dumps(keep, ensure_ascii=False)}
            kept.append(rec)
    stats["records_kept"] += len(kept)
    return kept


def install_filter(qa_convert, gate_symbol: str, stats: dict[str, int]) -> Callable:
    original = qa_convert.parse_tsv_zst_records

    def parse_filtered(data_path: Path):
        exchange = Path(data_path).parent.parent.name
        if exchange not in (LEADER, FOLLOWER):
            raise ValueError(f"unexpected partition path {data_path}")
        return filter_records(original(data_path), exchange, gate_symbol, stats)

    qa_convert.parse_tsv_zst_records = parse_filtered
    return original


def expected_outputs(out: Path, gate_symbol: str, date_utc: str) -> dict[str, Path]:
    base = native_symbols(gate_symbol)[0]
    ymd = date_utc.replace("-", "")
    return {
        "binance_bbo": out / f"binance_bbo_{base}_{ymd}.npz",
        "gate_l2_hbt": out / f"{FOLLOWER}_l2_{base}_{ymd}.npz",
        "gate_l1_bbo": out / f"{FOLLOWER}_l1_{base}_{ymd}.npz",
    }


def _peak_rss_mb() -> float | None:
    try:
        import resource
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    except Exception:
        return None


def main() -> int:
    p = argparse.ArgumentParser(description="Narrowed qa_convert for one Gate symbol + Binance leader")
    p.add_argument("--fetched", type=Path, required=True, help="output dir of tokyo_l2_bt_fetch.py")
    p.add_argument("--config", type=Path, required=True)
    p.add_argument("--date-utc", required=True)
    p.add_argument("--gate-symbol", required=True)
    p.add_argument("--out", type=Path, required=True)
    args = p.parse_args()

    import qa_convert

    stats = {"records_in": 0, "records_kept": 0, "gate_mixed_symbol_trade_messages": 0,
             "prefilter_rx_seq_violations": 0}
    install_filter(qa_convert, args.gate_symbol, stats)
    cfg = narrowed_config(json.loads(args.config.read_text(encoding="utf-8")), args.gate_symbol)
    started = time.time()
    result = qa_convert.run_qa_convert(
        args.fetched, args.out, meta_dir=args.fetched / "meta",
        strict_zero_loss=True, coverage_config=cfg, coverage_date_utc=args.date_utc,
    )
    base = native_symbols(args.gate_symbol)[0]
    summary: dict[str, Any] = {
        "date_utc": args.date_utc, "gate_symbol": args.gate_symbol,
        "converter": "collector/python/qa_convert.run_qa_convert (unchanged)",
        "input_narrowing": {"exchanges": [LEADER, FOLLOWER], "binance_streams": ["bookTicker"],
                            "symbol_base": base, **stats},
        "qa_convert_status": result.get("status"),
        "elapsed_seconds": round(time.time() - started, 1),
        "peak_rss_mb": _peak_rss_mb(),
    }
    ok = result.get("status") == "ok"
    if stats["prefilter_rx_seq_violations"]:
        ok = False                       # write order broken in records the narrowed run never saw
    if ok:
        outputs = expected_outputs(args.out, args.gate_symbol, args.date_utc)
        rows = json.loads((args.out / "eligibility_calendar.json").read_text(encoding="utf-8"))
        target = [r for r in rows if r["date_utc"] == args.date_utc and r["symbol"] == base
                  and r["exchange"] in (LEADER, FOLLOWER)]
        summary["calendar"] = [{k: r[k] for k in ("exchange", "symbol", "date_utc", "eligibility_status",
                                                  "depth_rebuild_status", "unverified_reasons")} for r in target]
        summary["outputs"] = {k: str(v) for k, v in outputs.items() if v.is_file()}
        missing = [k for k, v in outputs.items() if not v.is_file()]
        if missing:
            summary["missing_outputs"] = missing
            ok = False
    else:
        summary["qa_convert_failed_items"] = result.get("failed_items")
        summary["qa_convert_coverage"] = result.get("coverage")
    summary["status"] = "ok" if ok else "blocked"
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "convert_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
                                                  encoding="utf-8")
    print("CONVERT_SUMMARY", json.dumps(summary, ensure_ascii=False)[:20000], flush=True)
    return 0 if ok else 2


if __name__ == "__main__":
    sys.exit(main())
