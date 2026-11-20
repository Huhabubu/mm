"""Deterministic regression tests for HBT 2.4.4 event ordering & direction flag handling in qa_convert.py.

Reproduces and verifies the real Gate Futures EC2 P1 QA failure (`ValueError: exchange events are out of order.`
at `validate_event_order(ordered)`) caused by pre-setting `EXCH_EVENT | LOCAL_EVENT` before
`hftbacktest.data.validation.correct_event_order()` splits out-of-order rows via bitwise OR.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pytest
import zstandard as zstd
from hftbacktest import (
    BUY_EVENT,
    DEPTH_CLEAR_EVENT,
    DEPTH_EVENT,
    DEPTH_SNAPSHOT_EVENT,
    EXCH_EVENT,
    LOCAL_EVENT,
    SELL_EVENT,
    TRADE_EVENT,
    event_dtype,
)
from hftbacktest.data.validation import correct_event_order, validate_event_order

REPO_ROOT = Path(__file__).resolve().parents[2]
PYTHON_DIR = REPO_ROOT / "collector" / "python"
if str(PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(PYTHON_DIR))

from archive_sync import file_hashes  # noqa: E402
import qa_convert as qa_mod  # noqa: E402
from qa_convert import (  # noqa: E402
    TRADE_FLAG_BYBIT_BT,
    TRADE_FLAG_IS_INTERNAL,
    TRADE_FLAG_RPI,
    _find_first_order_conflict,
    _order_and_validate_hbt_events,
    run_qa_convert,
)


def _write_partition(
    sealed_root: Path,
    *,
    exchange: str,
    date_utc: str,
    hour_utc: str,
    records: list[tuple[int, int, str, int, int, str, str]],
) -> Path:
    yyyymmdd = date_utc.replace("-", "")
    stem = f"{exchange}_{yyyymmdd}_{hour_utc}"
    part_dir = sealed_root / "raw" / exchange / date_utc
    part_dir.mkdir(parents=True, exist_ok=True)
    dpath = part_dir / f"{stem}.tsv.zst"
    mpath = part_dir / f"{stem}.manifest.json"

    raw = "".join(f"{r[0]}\t{r[1]}\t{r[2]}\t{r[3]}\t{r[4]}\t{r[5]}\t{r[6]}\n" for r in records)
    dpath.write_bytes(zstd.ZstdCompressor(level=3).compress(raw.encode("utf-8")))
    h = file_hashes(dpath)
    manifest = {
        "schema_version": 1,
        "exchange": exchange,
        "date_utc": date_utc,
        "hour_utc": hour_utc,
        "boot_id": "order_reg_boot",
        "file_name": dpath.name,
        "bytes": h["bytes"],
        "sha256": h["sha256"],
        "zstd_verified": True,
        "start_wall_ns": records[0][0],
        "end_wall_ns": records[-1][0],
        "first_rx_seq": records[0][4],
        "last_rx_seq": records[-1][4],
        "data_lines": sum(1 for r in records if r[5] == "DATA"),
        "data_escaped_lines": 0,
        "meta_lines": sum(1 for r in records if r[5] == "META"),
        "local_loss_lines": 0,
        "gap_start_lines": 0,
        "switch_lines": 0,
    }
    mpath.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return mpath


def test_reproduce_preexisting_direction_flags_bug_and_verify_helper_fix() -> None:
    """Reproduce exact ValueError when EXCH_EVENT|LOCAL_EVENT are pre-set, then prove helper fixes it cleanly."""
    pre_flagged = np.array(
        [
            (DEPTH_CLEAR_EVENT | EXCH_EVENT | LOCAL_EVENT, 200_000_000, 1_000_000_000, 0.0, 0.0, 0, 0, 0.0),
            (DEPTH_SNAPSHOT_EVENT | BUY_EVENT | EXCH_EVENT | LOCAL_EVENT, 200_000_000, 1_000_000_000, 95000.0, 1.5, 0, 0, 0.0),
            # Interleaved trade arriving later locally but stamped earlier by the exchange:
            (TRADE_EVENT | BUY_EVENT | EXCH_EVENT | LOCAL_EVENT, 100_000_000, 1_001_000_000, 95000.1, 0.25, 0, TRADE_FLAG_IS_INTERNAL, 0.0),
            (DEPTH_EVENT | SELL_EVENT | EXCH_EVENT | LOCAL_EVENT, 250_000_000, 1_002_000_000, 95000.2, 2.0, 0, 0, 0.0),
        ],
        dtype=event_dtype,
    )

    # 1. Reproduce legacy bug directly against hftbacktest.correct_event_order():
    buggy_ordered = correct_event_order(
        pre_flagged,
        np.argsort(pre_flagged["exch_ts"], kind="mergesort"),
        np.argsort(pre_flagged["local_ts"], kind="mergesort"),
    )
    with pytest.raises(ValueError, match="exchange events are out of order"):
        validate_event_order(buggy_ordered)

    # 2. Verify _order_and_validate_hbt_events strips direction flags defensively & preserves all semantic bits:
    ordered, diag = _order_and_validate_hbt_events(
        pre_flagged,
        exchange="gate_futures",
        symbol="BTC",
        date_utc="2026-11-20",
        stream_kind="follower_l2_hbt",
    )
    validate_event_order(ordered)

    # Zero dropped events: exact input count on BOTH exchange and local timelines
    exch_rows = ordered[(ordered["ev"] & EXCH_EVENT) == EXCH_EVENT]
    local_rows = ordered[(ordered["ev"] & LOCAL_EVENT) == LOCAL_EVENT]
    assert len(exch_rows) == len(pre_flagged)
    assert len(local_rows) == len(pre_flagged)

    # Original timestamps strictly preserved (zero timestamp fabrication)
    assert sorted(int(x) for x in exch_rows["exch_ts"]) == sorted(int(x) for x in pre_flagged["exch_ts"])
    assert sorted(int(x) for x in local_rows["local_ts"]) == sorted(int(x) for x in pre_flagged["local_ts"])

    # Split trade event has 1 EXCH-only copy and 1 LOCAL-only copy, both keeping TRADE_EVENT | BUY_EVENT & ival
    trade_rows = ordered[(ordered["ev"] & 0xFF) == TRADE_EVENT]
    assert len(trade_rows) == 2
    assert bool(trade_rows[0]["ev"] & EXCH_EVENT) and not bool(trade_rows[0]["ev"] & LOCAL_EVENT)
    assert bool(trade_rows[1]["ev"] & LOCAL_EVENT) and not bool(trade_rows[1]["ev"] & EXCH_EVENT)
    assert all(bool(r["ev"] & BUY_EVENT) and int(r["ival"]) == TRADE_FLAG_IS_INTERNAL for r in trade_rows)

    assert diag["raw_exch_ts_reversals"] == 1
    assert diag["raw_local_ts_reversals"] == 0
    assert diag["split_extra_rows"] == 1
    assert diag["first_raw_exch_reversal"]["prev_index"] == 1
    assert diag["first_raw_exch_reversal"]["curr_index"] == 2


def test_gate_futures_out_of_order_exch_ts_with_monotonic_local_ts_end_to_end(tmp_path: Path) -> None:
    """End-to-end Gate Futures regression: reversed exch_ts across obu/trades with monotonic local_ts."""
    sealed = tmp_path / "sealed"
    base_wall = 1_795_168_800_000_000_000  # 2026-11-20T10:00:00Z
    base_ms = base_wall // 1_000_000

    records = [
        (
            base_wall + 1_000_000,
            1_000_000,
            "A",
            1,
            1,
            "DATA",
            json.dumps(
                {
                    "time": base_ms // 1000,
                    "time_ms": base_ms + 200,
                    "channel": "futures.obu",
                    "event": "update",
                    "result": {
                        "t": base_ms + 200,
                        "full": True,
                        "s": "ob.BTC_USDT.50",
                        "u": 100,
                        "b": [["95000.0", "1.5"]],
                        "a": [["95000.1", "1.2"]],
                    },
                }
            ),
        ),
        (
            # Arrives LATER locally (+2ms), but exchange trade timestamp is EARLIER (+50ms < +200ms)
            base_wall + 2_000_000,
            2_000_000,
            "A",
            1,
            2,
            "DATA",
            json.dumps(
                {
                    "time": base_ms // 1000,
                    "time_ms": base_ms + 201,
                    "channel": "futures.trades",
                    "event": "update",
                    "result": [
                        {
                            "id": 9001,
                            "contract": "BTC_USDT",
                            "size": "0.25",
                            "price": "95000.1",
                            "create_time_ms": base_ms + 50,
                            "is_internal": True,
                        }
                    ],
                }
            ),
        ),
        (
            base_wall + 3_000_000,
            3_000_000,
            "A",
            1,
            3,
            "DATA",
            json.dumps(
                {
                    "time": base_ms // 1000,
                    "time_ms": base_ms + 210,
                    "channel": "futures.obu",
                    "event": "update",
                    "result": {
                        "t": base_ms + 210,
                        "full": False,
                        "s": "ob.BTC_USDT.50",
                        "U": 101,
                        "u": 101,
                        "b": [["95000.0", "2.1"]],
                        "a": [],
                    },
                }
            ),
        ),
        (
            # Second reversal: sell trade arrives later locally (+4ms) with exch_ts +180ms < +210ms
            base_wall + 4_000_000,
            4_000_000,
            "A",
            1,
            4,
            "DATA",
            json.dumps(
                {
                    "time": base_ms // 1000,
                    "time_ms": base_ms + 212,
                    "channel": "futures.trades",
                    "event": "update",
                    "result": [
                        {
                            "id": 9002,
                            "contract": "BTC_USDT",
                            "size": "-0.5",
                            "price": "95000.0",
                            "create_time_ms": base_ms + 180,
                            "is_internal": False,
                        }
                    ],
                }
            ),
        ),
    ]

    mpath = _write_partition(sealed, exchange="gate_futures", date_utc="2026-11-20", hour_utc="10", records=records)
    out_dir = tmp_path / "qa_out"
    res = run_qa_convert(mpath, out_dir, allow_unverified_local=True, strict_zero_loss=True)
    assert res["status"] == "ok"
    assert res["eligible_asset_days"] == 1

    l2_art = next(a for a in res["artifacts"] if a["kind"] == "follower_l2_hbt")
    assert l2_art["order_diagnostics"]["raw_exch_ts_reversals"] == 2
    assert l2_art["order_diagnostics"]["raw_local_ts_reversals"] == 0
    assert l2_art["order_diagnostics"]["input_rows"] == 6  # clear + bid + ask + trade1 + bid_delta + trade2
    assert l2_art["order_diagnostics"]["split_extra_rows"] == 2

    arr = np.load(l2_art["path"])["data"]
    validate_event_order(arr)
    assert int(np.sum((arr["ev"] & EXCH_EVENT) == EXCH_EVENT)) == 6
    assert int(np.sum((arr["ev"] & LOCAL_EVENT) == LOCAL_EVENT)) == 6

    # Check both trade prices & exact exchange timestamps survived verbatim
    trades_exch = arr[((arr["ev"] & 0xFF) == TRADE_EVENT) & ((arr["ev"] & EXCH_EVENT) == EXCH_EVENT)]
    assert list(int(x) for x in trades_exch["exch_ts"]) == [(base_ms + 50) * 1_000_000, (base_ms + 180) * 1_000_000]
    assert bool(trades_exch[0]["ev"] & BUY_EVENT) and int(trades_exch[0]["ival"]) == TRADE_FLAG_IS_INTERNAL
    assert bool(trades_exch[1]["ev"] & SELL_EVENT) and np.isclose(trades_exch[1]["qty"], 0.5)


def test_binance_diagnostic_depth_out_of_order_exch_ts_with_monotonic_local_ts(tmp_path: Path) -> None:
    """Binance diagnostic depth branch uses the exact same ordering path and must handle reversed exch_ts."""
    sealed = tmp_path / "sealed"
    base_wall = 1_795_168_800_000_000_000
    base_ms = base_wall // 1_000_000

    records = [
        (
            base_wall + 1_000_000,
            1_000_000,
            "A",
            1,
            1,
            "META",
            json.dumps(
                {
                    "event": "REST_SNAPSHOT",
                    "symbol": "BTCUSDT",
                    "raw": {
                        "lastUpdateId": 500,
                        "E": base_ms + 300,
                        "bids": [["95000.0", "4.0"]],
                        "asks": [["95000.1", "3.0"]],
                    },
                }
            ),
        ),
        (
            # Earlier exch_ts (+150ms) arriving after snapshot (+300ms) locally
            base_wall + 2_000_000,
            2_000_000,
            "A",
            1,
            2,
            "DATA",
            json.dumps(
                {
                    "stream": "btcusdt@depth@100ms",
                    "data": {
                        "e": "depthUpdate",
                        "E": base_ms + 150,
                        "T": base_ms + 150,
                        "s": "BTCUSDT",
                        "U": 500,
                        "u": 501,
                        "pu": 499,
                        "b": [["95000.0", "4.5"]],
                        "a": [],
                    },
                }
            ),
        ),
        (
            base_wall + 3_000_000,
            3_000_000,
            "A",
            1,
            3,
            "DATA",
            json.dumps(
                {
                    "stream": "btcusdt@depth@100ms",
                    "data": {
                        "e": "depthUpdate",
                        "E": base_ms + 350,
                        "T": base_ms + 350,
                        "s": "BTCUSDT",
                        "U": 502,
                        "u": 502,
                        "pu": 501,
                        "b": [],
                        "a": [["95000.1", "2.5"]],
                    },
                }
            ),
        ),
    ]

    mpath = _write_partition(sealed, exchange="binance_futures", date_utc="2026-11-20", hour_utc="10", records=records)
    out_dir = tmp_path / "qa_binance_out"
    res = run_qa_convert(mpath, out_dir, allow_unverified_local=True, strict_zero_loss=True)
    assert res["status"] == "ok"

    dep_art = next(a for a in res["artifacts"] if a["kind"] == "binance_depth_diag")
    assert dep_art["order_diagnostics"]["raw_exch_ts_reversals"] == 1
    assert dep_art["order_diagnostics"]["input_rows"] == 5  # clear + snap_bid + snap_ask + delta1 + delta2
    arr = np.load(dep_art["path"])["data"]
    validate_event_order(arr)
    assert int(np.sum((arr["ev"] & EXCH_EVENT) == EXCH_EVENT)) == 5
    assert int(np.sum((arr["ev"] & LOCAL_EVENT) == LOCAL_EVENT)) == 5


def test_bybit_and_bitget_out_of_order_exch_ts_with_monotonic_local_ts(tmp_path: Path) -> None:
    """Bybit Linear and Bitget UTA v3 handle reversed exch_ts across book/trades while preserving special flags."""
    sealed = tmp_path / "sealed"
    base_wall = 1_795_168_800_000_000_000
    base_ms = base_wall // 1_000_000

    bybit_records = [
        (
            base_wall + 1_000_000,
            1_000_000,
            "A",
            1,
            1,
            "DATA",
            json.dumps(
                {
                    "topic": "orderbook.50.BTCUSDT",
                    "type": "snapshot",
                    "ts": base_ms + 200,
                    "cts": base_ms + 200,
                    "data": {"s": "BTCUSDT", "b": [["95000.0", "1.0"]], "a": [["95000.1", "1.0"]], "u": 10, "seq": 100},
                }
            ),
        ),
        (
            base_wall + 2_000_000,
            2_000_000,
            "A",
            1,
            2,
            "DATA",
            json.dumps(
                {
                    "topic": "publicTrade.BTCUSDT",
                    "type": "snapshot",
                    "ts": base_ms + 201,
                    "data": [
                        {
                            "T": base_ms + 80,
                            "s": "BTCUSDT",
                            "S": "Buy",
                            "v": "0.2",
                            "p": "95000.1",
                            "i": "by-1",
                            "BT": True,
                            "RPI": True,
                            "seq": 99,
                        }
                    ],
                }
            ),
        ),
    ]
    bitget_records = [
        (
            base_wall + 1_000_000,
            1_000_000,
            "A",
            1,
            1,
            "DATA",
            json.dumps(
                {
                    "action": "snapshot",
                    "arg": {"instType": "usdt-futures", "topic": "books50", "symbol": "BTCUSDT"},
                    "data": [{"a": [["95000.1", "2.0"]], "b": [["95000.0", "2.0"]], "ts": str(base_ms + 200), "seq": 10, "pseq": 0}],
                    "ts": base_ms + 200,
                }
            ),
        ),
        (
            base_wall + 2_000_000,
            2_000_000,
            "A",
            1,
            2,
            "DATA",
            json.dumps(
                {
                    "action": "update",
                    "arg": {"instType": "usdt-futures", "topic": "publicTrade", "symbol": "BTCUSDT"},
                    "data": [{"T": str(base_ms + 70), "P": "95000.1", "v": "0.4", "S": "sell", "i": "bg-1", "isRPI": True}],
                    "ts": base_ms + 201,
                }
            ),
        ),
    ]

    _write_partition(sealed, exchange="bybit_linear", date_utc="2026-11-20", hour_utc="10", records=bybit_records)
    _write_partition(sealed, exchange="bitget_v3", date_utc="2026-11-20", hour_utc="10", records=bitget_records)

    res = run_qa_convert(sealed, tmp_path / "qa_both", allow_unverified_local=True, strict_zero_loss=True)
    assert res["status"] == "ok"
    assert res["eligible_asset_days"] == 2

    by_arr = np.load(tmp_path / "qa_both" / "bybit_linear_l2_BTC_20261120.npz")["data"]
    bg_arr = np.load(tmp_path / "qa_both" / "bitget_v3_l2_BTC_20261120.npz")["data"]
    validate_event_order(by_arr)
    validate_event_order(bg_arr)

    by_tr = by_arr[((by_arr["ev"] & 0xFF) == TRADE_EVENT) & ((by_arr["ev"] & EXCH_EVENT) == EXCH_EVENT)]
    assert int(by_tr[0]["ival"]) == (TRADE_FLAG_BYBIT_BT | TRADE_FLAG_RPI)
    bg_tr = bg_arr[((bg_arr["ev"] & 0xFF) == TRADE_EVENT) & ((bg_arr["ev"] & EXCH_EVENT) == EXCH_EVENT)]
    assert int(bg_tr[0]["ival"]) == TRADE_FLAG_RPI


def test_conflict_diagnostics_report_exchange_symbol_and_first_conflict_position(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If validate_event_order ever fails, error message must pinpoint exchange, symbol, and first conflict."""
    bad_ordered = np.array(
        [
            (DEPTH_EVENT | BUY_EVENT | EXCH_EVENT | LOCAL_EVENT, 300_000_000, 1_000_000_000, 95000.0, 1.0, 0, 0, 0.0),
            (TRADE_EVENT | SELL_EVENT | EXCH_EVENT | LOCAL_EVENT, 100_000_000, 1_001_000_000, 95000.0, 0.5, 0, 0, 0.0),
        ],
        dtype=event_dtype,
    )
    conflict = _find_first_order_conflict(bad_ordered)
    assert conflict is not None
    assert conflict["axis"] == "exch_ts"
    assert conflict["prev_index"] == 0
    assert conflict["curr_index"] == 1
    assert conflict["prev_exch_ts"] == 300_000_000
    assert conflict["curr_exch_ts"] == 100_000_000

    # Inject a defective sorter returning bad_ordered so validate_event_order raises and enriches the error
    monkeypatch.setattr(qa_mod, "correct_event_order", lambda arr, ei, li: bad_ordered)
    with pytest.raises(ValueError) as exc_info:
        _order_and_validate_hbt_events(
            bad_ordered,
            exchange="gate_futures",
            symbol="SOL",
            date_utc="2026-11-20",
            stream_kind="follower_l2_hbt",
        )
    msg = str(exc_info.value)
    assert "exchange=gate_futures" in msg
    assert "symbol=SOL" in msg
    assert "date_utc=2026-11-20" in msg
    assert '"prev_index": 0' in msg
    assert '"curr_index": 1' in msg
    assert '"prev_exch_ts": 300000000' in msg
    assert '"curr_exch_ts": 100000000' in msg
