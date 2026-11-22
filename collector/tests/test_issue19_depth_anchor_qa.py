"""Issue #19 Python QA, trial15 archive exclusion, PR #18 equivalence, and hourly QA tests."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import zstandard as zstd

COLLECTOR_DIR = Path(__file__).resolve().parents[1]
PYTHON_DIR = COLLECTOR_DIR / "python"
if str(PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(PYTHON_DIR))

import data_coverage as dc  # noqa: E402
import hourly_incremental_qa as hqa  # noqa: E402
from qa_convert import run_qa_convert  # noqa: E402


GATE_SIX = ["BTC_USDT", "ETH_USDT", "SOL_USDT", "XRP_USDT", "DOGE_USDT", "SUI_USDT"]


def _fresh_rust_binary() -> Path:
    subprocess.run(
        ["cargo", "build", "--manifest-path", str(COLLECTOR_DIR / "Cargo.toml"), "--bin", "tokyo-l2-collector"],
        check=True,
        capture_output=True,
        text=True,
    )
    return COLLECTOR_DIR / "target/debug/tokyo-l2-collector"


def _norm_sym(sym: str) -> str:
    return sym.replace("_", "").upper()


def _base_px(sym: str) -> float:
    s = _norm_sym(sym)
    if s.startswith("BTC"):
        return 90000.0
    if s.startswith("ETH"):
        return 3000.0
    if s.startswith("SOL"):
        return 200.0
    if s.startswith("XRP"):
        return 2.0
    if s.startswith("DOGE"):
        return 1.0
    return 5.0


def _write_custom_15m_partition(
    root: Path,
    *,
    exchange: str,
    window_start_s: int,
    boot_id: str,
    run_id: str,
    rows: list[tuple[int, int, str, int, int, str, dict]],
    segment_suffix: str = "seg001",
) -> tuple[Path, Path, dict]:
    wd = dc.window_descriptor(window_start_s, 15)
    date_utc = wd["date_utc"]
    ymd = date_utc.replace("-", "")
    hh = wd["hour_utc"]
    mm = wd["minute_utc"]
    stem = f"{exchange}_{ymd}_{hh}{mm}_p15m_{run_id}_{segment_suffix}"

    day_dir = root / "raw" / exchange / date_utc
    day_dir.mkdir(parents=True, exist_ok=True)
    data_path = day_dir / f"{stem}.tsv.zst"
    mf_path = day_dir / f"{stem}.manifest.json"

    tsv_lines: list[str] = []
    data_lines = meta_lines = loss_lines = gap_lines = switch_lines = 0
    first_wall = last_wall = None
    for wall_ns, mono_ns, conn_id, epoch, rx_seq, kind, payload in rows:
        tsv_lines.append(
            "\t".join([str(wall_ns), str(mono_ns), conn_id, str(epoch), str(rx_seq), kind, json.dumps(payload, separators=(",", ":"))])
        )
        first_wall = wall_ns if first_wall is None else min(first_wall, wall_ns)
        last_wall = wall_ns if last_wall is None else max(last_wall, wall_ns)
        if kind == "META":
            meta_lines += 1
            ev = payload.get("event") if isinstance(payload, dict) else None
            if ev == "LOCAL_LOSS":
                loss_lines += 1
            elif ev == "GAP_START":
                gap_lines += 1
            elif ev == "SWITCH":
                switch_lines += 1
        else:
            data_lines += 1

    raw = ("\n".join(tsv_lines) + "\n").encode("utf-8")
    compressed = zstd.ZstdCompressor().compress(raw)
    data_path.write_bytes(compressed)
    sha256 = hashlib.sha256(compressed).hexdigest()

    manifest = {
        "schema_version": 2,
        "exchange": exchange,
        "date_utc": date_utc,
        "hour_utc": hh,
        "minute_utc": mm,
        "partition_minutes": 15,
        "window_id": wd["window_id"],
        "planned_start_wall_ns": wd["planned_start_wall_ns"],
        "planned_end_wall_ns": wd["planned_end_wall_ns"],
        "run_id": run_id,
        "segment_id": stem,
        "boot_id": boot_id,
        "file_name": data_path.name,
        "bytes": len(compressed),
        "sha256": sha256,
        "zstd_verified": True,
        "start_wall_ns": first_wall,
        "end_wall_ns": last_wall,
        "first_rx_seq": rows[0][4],
        "last_rx_seq": rows[-1][4],
        "data_lines": data_lines,
        "data_escaped_lines": 0,
        "meta_lines": meta_lines,
        "local_loss_lines": loss_lines,
        "gap_start_lines": gap_lines,
        "switch_lines": switch_lines,
        "is_startup_stub": False,
        "settled_by_timer": False,
        "coverage_incomplete": False,
        "late_frames_accepted": 0,
        "late_frames_rejected": 0,
    }
    mf_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return data_path, mf_path, manifest


def _read_tsv_zst(path: Path) -> list[dict]:
    out = []
    with path.open("rb") as f:
        text = zstd.ZstdDecompressor().stream_reader(f).read().decode("utf-8")
    for line in text.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t", 6)
        out.append(
            {
                "recv_wall_ns": int(parts[0]),
                "recv_mono_ns": int(parts[1]),
                "conn_id": parts[2],
                "epoch": int(parts[3]),
                "rx_seq": int(parts[4]),
                "kind": parts[5],
                "payload": json.loads(parts[6]),
            }
        )
    return out


class PR18ProtocolValidator:
    """Exact PR #18 tokyo_l2_protocol_qa Chain + Book invariants."""

    def __init__(self) -> None:
        self.chains: dict[tuple[str, str], dict] = {}
        self.books: dict[tuple[str, str], dict[str, dict[float, float]]] = {}
        self.issues: list[dict] = []

    def _record_issue(self, code: str, exchange: str, symbol: str, detail: str) -> None:
        self.issues.append({"code": code, "exchange": exchange, "symbol": symbol, "detail": detail})

    def _reset(self, exchange: str, sym: str | None) -> None:
        if sym is None or sym in ("", "ALL"):
            for k in [k for k in self.chains if k[0] == exchange]:
                self.chains.pop(k, None)
        else:
            self.chains.pop((exchange, sym), None)

    def _apply_book(self, exchange: str, sym: str, reset: bool, bids: list, asks: list) -> None:
        book = self.books.setdefault((exchange, sym), {"b": {}, "a": {}})
        if reset:
            book["b"].clear()
            book["a"].clear()
        for px_s, sz_s in bids:
            px, sz = float(px_s), float(sz_s)
            if sz <= 0:
                book["b"].pop(px, None)
            else:
                book["b"][px] = sz
        for px_s, sz_s in asks:
            px, sz = float(px_s), float(sz_s)
            if sz <= 0:
                book["a"].pop(px, None)
            else:
                book["a"][px] = sz
        if book["b"] and book["a"]:
            bb = max(book["b"])
            ba = min(book["a"])
            if bb >= ba:
                self._record_issue("crossed_book", exchange, sym, f"best_bid={bb} >= best_ask={ba}")

    def validate_partition(self, exchange: str, boot_id: str, path: Path) -> None:
        for rec in _read_tsv_zst(path):
            p = rec["payload"]
            conn = (boot_id, rec["conn_id"], rec["epoch"])
            if rec["kind"] == "META":
                ev = p.get("event", "")
                if ev in ("LOCAL_LOSS", "GAP_START", "DEPTH_UNAVAILABLE", "SWITCH"):
                    raw_sym = p.get("symbol")
                    sym = _norm_sym(raw_sym) if isinstance(raw_sym, str) and raw_sym not in ("", "ALL") else None
                    self._reset(exchange, sym)
                elif ev == "REST_SNAPSHOT_ANCHOR" and exchange == "binance_futures":
                    sym = _norm_sym(p["symbol"])
                    snap = p["snapshot"]
                    s_uid = int(p["lastUpdateId"])
                    self.chains[(exchange, sym)] = {
                        "conn": conn,
                        "mode": "binance",
                        "prev_seq": s_uid,
                        "awaiting": True,
                        "snap_uid": s_uid,
                    }
                    self._apply_book(exchange, sym, True, snap.get("bids", []), snap.get("asks", []))
                elif ev == "DEPTH_BRIDGED_DELTA" and exchange == "binance_futures":
                    sym = _norm_sym(p["symbol"])
                    d = json.loads(p["raw"])
                    d = d.get("data", d)
                    self._apply_binance_delta(sym, conn, d)
                continue

            if exchange == "gate_futures" and p.get("channel") == "futures.obu":
                r = p["result"]
                sym = _norm_sym(r["s"].split(".")[1])
                is_full = bool(r.get("full"))
                u = int(r["u"])
                if is_full:
                    self.chains[(exchange, sym)] = {"conn": conn, "prev_seq": u}
                    self._apply_book(exchange, sym, True, r.get("b", []), r.get("a", []))
                else:
                    fu = int(r["U"])
                    c = self.chains.get((exchange, sym))
                    if c is None or c["conn"] != conn:
                        self._record_issue("missing_same_connection_anchor", exchange, sym, f"delta conn={conn} U={fu} u={u}")
                        continue
                    if not (fu == c["prev_seq"] + 1 and u >= fu):
                        self._record_issue("exchange_update_chain_break", exchange, sym, f"U={fu} prev={c['prev_seq']} u={u}")
                    c["prev_seq"] = u
                    self._apply_book(exchange, sym, False, r.get("b", []), r.get("a", []))

            elif exchange == "bybit_linear" and str(p.get("topic", "")).startswith("orderbook.50."):
                sym = _norm_sym(p["topic"].split(".")[-1])
                d = p["data"]
                u = int(d["u"])
                if p.get("type") == "snapshot" or u == 1:
                    self.chains[(exchange, sym)] = {"conn": conn, "prev_seq": u}
                    self._apply_book(exchange, sym, True, d.get("b", []), d.get("a", []))
                else:
                    c = self.chains.get((exchange, sym))
                    if c is None or c["conn"] != conn:
                        self._record_issue("missing_same_connection_anchor", exchange, sym, f"delta conn={conn} u={u}")
                        continue
                    if u < c["prev_seq"]:
                        self._record_issue("exchange_update_chain_break", exchange, sym, f"u={u} < prev={c['prev_seq']}")
                    c["prev_seq"] = u
                    self._apply_book(exchange, sym, False, d.get("b", []), d.get("a", []))

            elif exchange == "bitget_v3" and p.get("arg", {}).get("topic") == "books50":
                sym = _norm_sym(p["arg"]["symbol"])
                item = p["data"][0]
                seq = int(item["seq"])
                if p.get("action") == "snapshot":
                    self.chains[(exchange, sym)] = {"conn": conn, "prev_seq": seq, "awaiting": True}
                    self._apply_book(exchange, sym, True, item.get("b", []), item.get("a", []))
                else:
                    pseq = int(item["pseq"])
                    c = self.chains.get((exchange, sym))
                    if c is None or c["conn"] != conn:
                        self._record_issue("missing_same_connection_anchor", exchange, sym, f"update conn={conn} pseq={pseq} seq={seq}")
                        continue
                    ok = (
                        pseq < seq
                        and ((pseq <= c["prev_seq"] <= seq) if c["awaiting"] else pseq == c["prev_seq"])
                    )
                    if not ok:
                        self._record_issue(
                            "exchange_update_chain_break",
                            exchange,
                            sym,
                            f"pseq={pseq} seq={seq} prev={c['prev_seq']} awaiting={c['awaiting']}",
                        )
                    c["awaiting"] = False
                    c["prev_seq"] = seq
                    self._apply_book(exchange, sym, False, item.get("b", []), item.get("a", []))

            elif exchange == "binance_futures" and "@depth" in str(p.get("stream", "")):
                d = p.get("data", p)
                sym = _norm_sym(d["s"])
                self._apply_binance_delta(sym, conn, d)

    def _apply_binance_delta(self, sym: str, conn: tuple[str, str, int], d: dict) -> None:
        fu, lu, pu = int(d["U"]), int(d["u"]), int(d["pu"])
        c = self.chains.get(("binance_futures", sym))
        if c is None or c["conn"] != conn:
            self._record_issue("missing_same_connection_anchor", "binance_futures", sym, f"delta conn={conn} U={fu} u={lu}")
            return
        if c["awaiting"]:
            if not (fu <= c["snap_uid"] <= lu):
                self._record_issue("exchange_update_chain_break", "binance_futures", sym, f"U={fu} S={c['snap_uid']} u={lu}")
            c["awaiting"] = False
        elif pu != c["prev_seq"]:
            self._record_issue("exchange_update_chain_break", "binance_futures", sym, f"pu={pu} prev={c['prev_seq']}")
        c["prev_seq"] = lu
        self._apply_book("binance_futures", sym, False, d.get("b", []), d.get("a", []))


def test_issue19_qa_convert_excludes_trial15_damaged_gate_markets_preserves_valid_and_bbo(tmp_path: Path):
    """Damaged trial15-like archive (B deltas on 5 Gate symbols without B-line snapshot, SUI intact)
    must mark those 5 markets execution_unverified / unrebuildable_excluded without blocking SUI or L1 BBO.
    """
    data_root = tmp_path / "archive"
    out_dir = tmp_path / "hbt_out"
    w0_s = int(datetime(2026, 11, 21, 14, 0, tzinfo=timezone.utc).timestamp())
    t0 = w0_s * 1_000_000_000
    mono = 1_000_000
    seq = 1
    rows: list[tuple[int, int, str, int, int, str, dict]] = []

    def add(dt_ms: int, conn: str, ep: int, kind: str, payload: dict) -> None:
        nonlocal mono, seq
        mono += 100_000
        rows.append((t0 + dt_ms * 1_000_000, mono, conn, ep, seq, kind, payload))
        seq += 1

    # Initially A anchors all 6 markets.
    for i, sym in enumerate(GATE_SIX):
        px = _base_px(sym)
        add(10 + i, "A", 1, "DATA", {
            "channel": "futures.obu", "event": "update",
            "result": {"t": (t0 // 1_000_000) + 10 + i, "full": True, "s": f"ob.{sym}.50", "u": 100, "b": [[f"{px:.2f}", "2.0"]], "a": [[f"{px + 1.0:.2f}", "2.0"]]},
        })
        add(20 + i, "A", 1, "DATA", {
            "channel": "futures.book_ticker", "event": "update",
            "result": {"s": sym, "b": f"{px:.2f}", "B": "5.0", "a": f"{px + 1.0:.2f}", "A": "4.0"},
        })

    # Switch A -> B: only SUI_USDT gets a B-line snapshot + chained delta; BTC..DOGE write B-line deltas WITHOUT B snapshot!
    add(500, "B", 1, "META", {"event": "SWITCH", "endpoint": "gate_public", "from": "A", "to": "B", "reason": "initial_or_standby_ready_first"})
    add(510, "B", 1, "DATA", {
        "channel": "futures.obu", "event": "update",
        "result": {"t": (t0 // 1_000_000) + 510, "full": True, "s": "ob.SUI_USDT.50", "u": 500, "b": [["5.00", "2.0"]], "a": [["6.00", "2.0"]]},
    })
    add(520, "B", 1, "DATA", {
        "channel": "futures.obu", "event": "update",
        "result": {"t": (t0 // 1_000_000) + 520, "full": False, "s": "ob.SUI_USDT.50", "U": 501, "u": 501, "b": [["5.00", "3.0"]], "a": []},
    })
    add(525, "B", 1, "DATA", {
        "channel": "futures.trades", "event": "update",
        "result": [{"contract": "SUI_USDT", "id": 1, "size": "1.0", "price": "5.50"}],
    })

    for idx, sym in enumerate(GATE_SIX[:5]):
        px = _base_px(sym)
        add(550 + idx * 10, "B", 1, "DATA", {
            "channel": "futures.obu", "event": "update",
            "result": {"t": (t0 // 1_000_000) + 550 + idx * 10, "full": False, "s": f"ob.{sym}.50", "U": 101, "u": 101, "b": [[f"{px:.2f}", "2.5"]], "a": []},
        })
        add(555 + idx * 10, "B", 1, "DATA", {
            "channel": "futures.book_ticker", "event": "update",
            "result": {"s": sym, "b": f"{px:.2f}", "B": "5.0", "a": f"{px + 1.0:.2f}", "A": "4.0"},
        })

    _write_custom_15m_partition(
        data_root,
        exchange="gate_futures",
        window_start_s=w0_s,
        boot_id="trial15_20261121T1400Z_boot",
        run_id="trial15_20261121T1400Z",
        rows=rows,
    )

    summary = run_qa_convert(data_root, out_dir, allow_unverified_local=True)
    assert summary["status"] == "ok"
    assert summary["eligible_asset_days"] == 1
    assert summary["execution_unverified_asset_days"] == 5
    assert len(summary["unrebuildable_depth_markets"]) == 5

    cal = json.loads(Path(summary["calendar_path"]).read_text(encoding="utf-8"))
    by_sym = {r["symbol"]: r for r in cal}
    sui = by_sym["SUI"]
    assert sui["eligibility_status"] == "eligible"
    assert sui["depth_rebuild_status"] == "ok"
    assert "follower_l2_hbt" in sui["files"]
    assert Path(sui["files"]["follower_l2_hbt"]).is_file()

    for sym in ["BTC", "ETH", "SOL", "XRP", "DOGE"]:
        row = by_sym[sym]
        assert row["eligibility_status"] == "execution_unverified"
        assert row["depth_rebuild_status"] == "unrebuildable_excluded"
        assert "missing_same_connection_anchor" in row["unrebuildable_depth_reasons"]
        assert row["excluded_unrebuildable_depth_rows"] >= 1
        assert row["hbt_event_count"] == 0
        assert "follower_l2_hbt" not in row["files"]
        assert "follower_l2_edges" not in row["files"]
        assert "follower_l1_bbo" in row["files"]


def test_issue19_qa_convert_15m_cross_window_same_connection_continuity_stays_eligible(tmp_path: Path):
    """Normal 15m cross-window continuation (14:00Z -> 14:15Z) on the SAME (boot_id, conn, epoch) stays eligible."""
    data_root = tmp_path / "archive"
    out_dir = tmp_path / "hbt_out"
    w0_s = int(datetime(2026, 11, 21, 14, 0, tzinfo=timezone.utc).timestamp())
    w1_s = w0_s + 900
    t0 = w0_s * 1_000_000_000
    t1 = w1_s * 1_000_000_000

    # Window 0: snapshots u=100 + delta u=101 on (A, 1) with monotonic mono_ns
    rows0 = []
    seq = 1
    mono = 100
    for i, sym in enumerate(["BTC_USDT", "SUI_USDT"]):
        px = _base_px(sym)
        rows0.append((t0 + (10 + i * 2) * 1_000_000, mono, "A", 1, seq, "DATA", {
            "channel": "futures.obu", "event": "update",
            "result": {"t": (t0 // 1_000_000) + 10 + i * 2, "full": True, "s": f"ob.{sym}.50", "u": 100, "b": [[f"{px:.2f}", "2.0"]], "a": [[f"{px + 1.0:.2f}", "2.0"]]},
        }))
        seq += 1
        mono += 10
        rows0.append((t0 + (20 + i * 2) * 1_000_000, mono, "A", 1, seq, "DATA", {
            "channel": "futures.obu", "event": "update",
            "result": {"t": (t0 // 1_000_000) + 20 + i * 2, "full": False, "s": f"ob.{sym}.50", "U": 101, "u": 101, "b": [[f"{px:.2f}", "2.5"]], "a": []},
        }))
        seq += 1
        mono += 10

    # Window 1 (14:15Z): continuation delta U=102 u=102 on SAME (A, 1), NO reset needed!
    rows1 = []
    for i, sym in enumerate(["BTC_USDT", "SUI_USDT"]):
        px = _base_px(sym)
        rows1.append((t1 + (10 + i * 2) * 1_000_000, mono, "A", 1, seq, "DATA", {
            "channel": "futures.obu", "event": "update",
            "result": {"t": (t1 // 1_000_000) + 10 + i * 2, "full": False, "s": f"ob.{sym}.50", "U": 102, "u": 102, "b": [[f"{px:.2f}", "3.1"]], "a": []},
        }))
        seq += 1
        mono += 10

    _write_custom_15m_partition(data_root, exchange="gate_futures", window_start_s=w0_s, boot_id="cont_boot", run_id="cont_run", rows=rows0, segment_suffix="seg001")
    _write_custom_15m_partition(data_root, exchange="gate_futures", window_start_s=w1_s, boot_id="cont_boot", run_id="cont_run", rows=rows1, segment_suffix="seg002")

    summary = run_qa_convert(data_root, out_dir, allow_unverified_local=True)
    assert summary["status"] == "ok"
    assert summary["unrebuildable_depth_markets"] == []
    assert summary["eligible_asset_days"] == 2
    assert summary["execution_unverified_asset_days"] == 0
    cal = json.loads(Path(summary["calendar_path"]).read_text(encoding="utf-8"))
    for row in cal:
        assert row["eligibility_status"] == "eligible"
        assert row["depth_rebuild_status"] == "ok"
        assert row["hbt_event_count"] >= 3


def test_issue19_replay_fixture_pr18_protocol_qa_equivalence_and_hourly_qa(tmp_path: Path):
    """Run real compiled Rust binary replay-fixture across Gate(6-sym startup/gap/planned), Bybit,
    Bitget, and Binance, certify sealed .tsv.zst archives against PR #18 Protocol QA + qa_convert,
    and verify hourly_incremental_qa behavior on clean, planned-rotation, and unplanned-gap partitions.
    """
    rust_bin = _fresh_rust_binary()
    cfg = json.loads((COLLECTOR_DIR / "specs/config.trial_15m.json").read_text(encoding="utf-8"))
    out_root = tmp_path / "replay_out"
    cfg["deployment"]["data_root"] = str(out_root)
    cfg["exchanges"]["bybit_linear"]["symbols"] = ["BTCUSDT", "ETHUSDT"]
    cfg["exchanges"]["bitget_v3"]["symbols"] = ["BTCUSDT", "ETHUSDT"]
    cfg["exchanges"]["binance_futures"]["symbols"] = ["BTCUSDT", "ETHUSDT"]
    cfg_path = tmp_path / "config.json"
    cfg_path.write_text(json.dumps(cfg, indent=2), encoding="utf-8")

    base_wall = int(datetime(2026, 11, 21, 14, 0, tzinfo=timezone.utc).timestamp()) * 1_000_000_000
    events: list[dict] = []
    mono = 1_000_000

    def emit(dt_ms: int, ev: dict) -> None:
        nonlocal mono
        mono += 100_000
        ev["recv_wall_ns"] = base_wall + dt_ms * 1_000_000
        ev["recv_mono_ns"] = mono
        events.append(ev)

    t = 10
    # --- 1. Gate 6-market startup promotion (B becomes Live first; BTC..DOGE refreshed; planned rotation B->A) ---
    for sym in GATE_SIX[:5]:
        px = _base_px(sym)
        emit(t, {"op": "frame", "endpoint": "gate_public", "conn_id": "B", "epoch": 1, "payload": json.dumps({"channel": "futures.obu", "event": "update", "result": {"full": True, "s": f"ob.{sym}.50", "u": 100, "b": [[f"{px:.2f}", "2.0"]], "a": [[f"{px + 1.0:.2f}", "2.0"]]}})})
        t += 1
    emit(t, {"op": "frame", "endpoint": "gate_public", "conn_id": "B", "epoch": 1, "payload": json.dumps({"channel": "futures.obu", "event": "update", "result": {"full": True, "s": "ob.SUI_USDT.50", "u": 500, "b": [["5.00", "2.0"]], "a": [["6.00", "2.0"]]}})})
    t += 2
    # Stale delta on B before fresh snapshot must be withheld by CausalSelector; SUI_USDT delta writes cleanly!
    emit(t, {"op": "frame", "endpoint": "gate_public", "conn_id": "B", "epoch": 1, "payload": json.dumps({"channel": "futures.obu", "event": "update", "result": {"full": False, "s": "ob.BTC_USDT.50", "U": 101, "u": 101, "b": [["90000.00", "9.9"]], "a": []}})})
    emit(t + 1, {"op": "frame", "endpoint": "gate_public", "conn_id": "B", "epoch": 1, "payload": json.dumps({"channel": "futures.obu", "event": "update", "result": {"full": False, "s": "ob.SUI_USDT.50", "U": 501, "u": 501, "b": [["5.00", "2.5"]], "a": []}})})
    t += 5
    for sym in GATE_SIX[:5]:
        px = _base_px(sym)
        emit(t, {"op": "frame", "endpoint": "gate_public", "conn_id": "B", "epoch": 1, "payload": json.dumps({"channel": "futures.obu", "event": "update", "result": {"full": True, "s": f"ob.{sym}.50", "u": 200, "b": [[f"{px:.2f}", "3.0"]], "a": [[f"{px + 1.0:.2f}", "3.0"]]}})})
        emit(t + 1, {"op": "frame", "endpoint": "gate_public", "conn_id": "B", "epoch": 1, "payload": json.dumps({"channel": "futures.obu", "event": "update", "result": {"full": False, "s": f"ob.{sym}.50", "U": 201, "u": 201, "b": [[f"{px:.2f}", "3.2"]], "a": []}})})
        t += 3
    # Planned rotation B -> A (make A Live first, rotate B, refresh A with full=true)
    for sym in GATE_SIX:
        px = _base_px(sym)
        emit(t, {"op": "frame", "endpoint": "gate_public", "conn_id": "A", "epoch": 1, "payload": json.dumps({"channel": "futures.obu", "event": "update", "result": {"full": True, "s": f"ob.{sym}.50", "u": 300, "b": [[f"{px:.2f}", "2.0"]], "a": [[f"{px + 1.0:.2f}", "2.0"]]}})})
        t += 1
    emit(t, {"op": "planned_rotation", "endpoint": "gate_public", "target": "B"})
    t += 2
    for sym in GATE_SIX:
        px = _base_px(sym)
        emit(t, {"op": "frame", "endpoint": "gate_public", "conn_id": "A", "epoch": 1, "payload": json.dumps({"channel": "futures.obu", "event": "update", "result": {"full": True, "s": f"ob.{sym}.50", "u": 400, "b": [[f"{px:.2f}", "4.0"]], "a": [[f"{px + 1.0:.2f}", "4.0"]]}})})
        emit(t + 1, {"op": "frame", "endpoint": "gate_public", "conn_id": "A", "epoch": 1, "payload": json.dumps({"channel": "futures.obu", "event": "update", "result": {"full": False, "s": f"ob.{sym}.50", "U": 401, "u": 401, "b": [[f"{px:.2f}", "4.2"]], "a": []}})})
        emit(t + 2, {"op": "frame", "endpoint": "gate_public", "conn_id": "A", "epoch": 1, "payload": json.dumps({"channel": "futures.book_ticker", "event": "update", "result": {"s": sym, "b": f"{px:.2f}", "B": "5.0", "a": f"{px + 1.0:.2f}", "A": "4.0"}})})
        t += 4

    # --- 2. Bybit (A+B startup, failover u regression -> B refresh + u==1 restart) ---
    for sym in ["BTCUSDT", "ETHUSDT"]:
        px = _base_px(sym)
        for c in ("A", "B"):
            emit(t, {"op": "frame", "endpoint": "bybit_public", "conn_id": c, "epoch": 1, "payload": json.dumps({"topic": f"orderbook.50.{sym}", "type": "snapshot", "ts": 1795183200000 + t, "data": {"s": sym, "u": 100, "b": [[f"{px:.1f}", "2.0"]], "a": [[f"{px + 1.0:.1f}", "2.0"]]}})})
            t += 1
    emit(t, {"op": "frame", "endpoint": "bybit_public", "conn_id": "A", "epoch": 1, "payload": json.dumps({"topic": "orderbook.50.BTCUSDT", "type": "delta", "ts": 1795183200000 + t, "data": {"s": "BTCUSDT", "u": 105, "b": [["90000.0", "2.1"]], "a": []}})})
    t += 2
    # u=50 < 105 triggers failover A -> B; then B gets stale delta (blocked), u==1 reset snapshot, and delta
    emit(t, {"op": "frame", "endpoint": "bybit_public", "conn_id": "A", "epoch": 1, "payload": json.dumps({"topic": "orderbook.50.BTCUSDT", "type": "delta", "ts": 1795183200000 + t, "data": {"s": "BTCUSDT", "u": 50, "b": [["90000.0", "1.1"]], "a": []}})})
    t += 2
    for sym in ["BTCUSDT", "ETHUSDT"]:
        px = _base_px(sym)
        emit(t, {"op": "frame", "endpoint": "bybit_public", "conn_id": "B", "epoch": 1, "payload": json.dumps({"topic": f"orderbook.50.{sym}", "type": "delta", "ts": 1795183200000 + t, "data": {"s": sym, "u": 1, "b": [[f"{px:.1f}", "3.0"]], "a": [[f"{px + 1.0:.1f}", "3.0"]]}})})
        emit(t + 1, {"op": "frame", "endpoint": "bybit_public", "conn_id": "B", "epoch": 1, "payload": json.dumps({"topic": f"orderbook.50.{sym}", "type": "delta", "ts": 1795183200000 + t + 1, "data": {"s": sym, "u": 12, "b": [[f"{px:.1f}", "3.4"]], "a": []}})})
        emit(t + 2, {"op": "frame", "endpoint": "bybit_public", "conn_id": "B", "epoch": 1, "payload": json.dumps({"topic": f"orderbook.1.{sym}", "type": "snapshot", "ts": 1795183200000 + t + 2, "data": {"s": sym, "u": 12, "b": [[f"{px:.1f}", "1.0"]], "a": [[f"{px + 1.0:.1f}", "1.0"]]}})})
        t += 4

    # --- 3. Bitget (A+B startup, post-cutover pseq>=seq fault -> B refresh -> pseq/seq updates) ---
    for sym in ["BTCUSDT", "ETHUSDT"]:
        px = _base_px(sym)
        for c in ("A", "B"):
            emit(t, {"op": "frame", "endpoint": "bitget_public", "conn_id": c, "epoch": 1, "payload": json.dumps({"action": "snapshot", "arg": {"instType": "usdt-futures", "topic": "books50", "symbol": sym}, "data": [{"seq": 100, "pseq": 0, "b": [[f"{px:.1f}", "2.0"]], "a": [[f"{px + 1.0:.1f}", "2.0"]]}]})})
            t += 1
    emit(t, {"op": "frame", "endpoint": "bitget_public", "conn_id": "A", "epoch": 1, "payload": json.dumps({"action": "update", "arg": {"instType": "usdt-futures", "topic": "books50", "symbol": "BTCUSDT"}, "data": [{"seq": 105, "pseq": 105, "b": [["90000.0", "1.1"]], "a": []}]})})
    t += 2
    for sym in ["BTCUSDT", "ETHUSDT"]:
        px = _base_px(sym)
        emit(t, {"op": "frame", "endpoint": "bitget_public", "conn_id": "B", "epoch": 1, "payload": json.dumps({"action": "snapshot", "arg": {"instType": "usdt-futures", "topic": "books50", "symbol": sym}, "data": [{"seq": 200, "pseq": 0, "b": [[f"{px:.1f}", "3.0"]], "a": [[f"{px + 1.0:.1f}", "3.0"]]}]})})
        emit(t + 1, {"op": "frame", "endpoint": "bitget_public", "conn_id": "B", "epoch": 1, "payload": json.dumps({"action": "update", "arg": {"instType": "usdt-futures", "topic": "books50", "symbol": sym}, "data": [{"seq": 205, "pseq": 199, "b": [[f"{px:.1f}", "3.5"]], "a": []}]})})
        emit(t + 2, {"op": "frame", "endpoint": "bitget_public", "conn_id": "B", "epoch": 1, "payload": json.dumps({"action": "snapshot", "arg": {"instType": "usdt-futures", "topic": "books1", "symbol": sym}, "data": [{"seq": 1, "pseq": 0, "b": [[f"{px:.1f}", "1.0"]], "a": [[f"{px + 1.0:.1f}", "1.0"]]}]})})
        t += 4

    # --- 4. Binance (REST snapshot bridge on A & B, disconnect A -> failover B REST refresh, bookTicker uninterrupted) ---
    for sym in ["BTCUSDT", "ETHUSDT"]:
        px = _base_px(sym)
        for c in ("A", "B"):
            emit(t, {"op": "frame", "endpoint": "binance_public", "conn_id": c, "epoch": 1, "payload": json.dumps({"stream": f"{sym.lower()}@depth@100ms", "data": {"e": "depthUpdate", "s": sym, "U": 100, "u": 104, "pu": 99, "b": [[f"{px:.2f}", "6.0"]], "a": []}})})
            emit(t + 1, {"op": "rest_snapshot", "endpoint": "binance_public", "conn_id": c, "symbol": sym, "last_update_id": 102, "raw_json": json.dumps({"lastUpdateId": 102, "bids": [[f"{px:.2f}", "5.0"]], "asks": [[f"{px + 1.0:.2f}", "5.0"]]})})
            t += 2
    emit(t, {"op": "disconnect", "endpoint": "binance_public", "conn_id": "A", "reason": "fault_a"})
    t += 2
    for sym in ["BTCUSDT", "ETHUSDT"]:
        px = _base_px(sym)
        emit(t, {"op": "frame", "endpoint": "binance_public", "conn_id": "B", "epoch": 1, "payload": json.dumps({"stream": f"{sym.lower()}@bookTicker", "data": {"e": "bookTicker", "u": 105, "s": sym, "b": f"{px:.2f}", "B": "1.0", "a": f"{px + 1.0:.2f}", "A": "1.0"}})})
        emit(t + 1, {"op": "frame", "endpoint": "binance_public", "conn_id": "B", "epoch": 1, "payload": json.dumps({"stream": f"{sym.lower()}@depth@100ms", "data": {"e": "depthUpdate", "s": sym, "U": 105, "u": 108, "pu": 104, "b": [[f"{px:.2f}", "7.0"]], "a": []}})})
        emit(t + 2, {"op": "rest_snapshot", "endpoint": "binance_public", "conn_id": "B", "symbol": sym, "last_update_id": 106, "raw_json": json.dumps({"lastUpdateId": 106, "bids": [[f"{px:.2f}", "5.0"]], "asks": [[f"{px + 1.0:.2f}", "5.0"]]})})
        t += 4

    fixture_path = tmp_path / "fixture.jsonl"
    fixture_path.write_text("\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8")
    proc = subprocess.run(
        [str(rust_bin), "replay-fixture", "--config", str(cfg_path), "--fixture", str(fixture_path), "--out", str(out_root), "--boot-id", "issue19_replay_boot"],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, f"replay-fixture failed:\n{proc.stdout}\n{proc.stderr}"
    replay_summary = json.loads(proc.stdout)
    assert replay_summary["status"] == "ok"

    # Run exact PR #18 ProtocolValidator across ALL 4 exchanges' sealed .tsv.zst partitions!
    validator = PR18ProtocolValidator()
    for mf_str in replay_summary["sealed_manifests"]:
        mf_path = Path(mf_str)
        mf = json.loads(mf_path.read_text(encoding="utf-8"))
        data_path = mf_path.with_name(mf["file_name"])
        validator.validate_partition(mf["exchange"], mf["boot_id"], data_path)
    assert validator.issues == [], f"PR #18 protocol issues detected: {validator.issues}"

    # Verify qa_convert passes cleanly across all replayed exchanges/markets!
    conv_summary = run_qa_convert(out_root, tmp_path / "hbt_out", allow_unverified_local=True)
    assert conv_summary["status"] == "ok"
    assert conv_summary["unrebuildable_depth_markets"] == []
    assert conv_summary["execution_unverified_asset_days"] == 0

    # Verify hourly_incremental_qa.check_partition distinguishes clean, planned-rotation-recovered, and unplanned DEPTH_UNAVAILABLE!
    qa_cfg = {
        "deployment": {"partition_minutes": 15},
        "meta_daemon": {
            "collector_stale_seconds": 3600,
            "continuous_quote_stale_seconds": 3600,
            "depth_stale_seconds": 3600,
            "event_trade_stale_seconds": 3600,
            "require_stream_activity": False,
        },
        "exchanges": {
            "gate_futures": {
                "enabled": True,
                "symbols": ["BTC_USDT"],
                "channels": ["futures.obu.50", "futures.book_ticker", "futures.trades"],
            }
        },
    }
    w0_s = int(datetime(2026, 11, 21, 14, 0, tzinfo=timezone.utc).timestamp())
    t0 = w0_s * 1_000_000_000

    def gate_base_rows(extra_meta: list[tuple[int, dict]]) -> list[tuple[int, int, str, int, int, str, dict]]:
        rows = [
            (t0 + 1_000_000, 100, "A", 1, 1, "DATA", {"channel": "futures.book_ticker", "event": "update", "result": {"s": "BTC_USDT", "b": "90000.0", "B": "1", "a": "90001.0", "A": "1"}}),
            (t0 + 2_000_000, 200, "A", 1, 2, "DATA", {"channel": "futures.obu", "event": "update", "result": {"full": True, "s": "ob.BTC_USDT.50", "u": 100, "b": [["90000.0", "1"]], "a": [["90001.0", "1"]]}}),
            (t0 + 3_000_000, 300, "A", 1, 3, "DATA", {"channel": "futures.trades", "event": "update", "result": [{"contract": "BTC_USDT", "id": 1, "size": "1", "price": "90000.0"}]}),
        ]
        seq = 4
        for off_ms, meta_p in extra_meta:
            rows.append((t0 + off_ms * 1_000_000, off_ms * 1000, "B", 1, seq, "META", meta_p))
            seq += 1
        return rows

    dp_clean, _, mf_clean = _write_custom_15m_partition(tmp_path / "qa_p", exchange="gate_futures", window_start_s=w0_s, boot_id="qa_c", run_id="r", rows=gate_base_rows([]), segment_suffix="clean")
    res_clean = hqa.check_partition(dp_clean, mf_clean, qa_cfg)
    assert res_clean["qa_status"] == "passed", res_clean["problems"]

    # Fake META-only DEPTH_RECOVERED without an actual new-connection snapshot + continuous delta MUST fail QA!
    dp_plan_meta_only, _, mf_plan_meta_only = _write_custom_15m_partition(
        tmp_path / "qa_p",
        exchange="gate_futures",
        window_start_s=w0_s,
        boot_id="qa_p_meta_only",
        run_id="r",
        rows=gate_base_rows([
            (10, {"event": "DEPTH_UNAVAILABLE", "symbol": "BTC_USDT", "planned": True, "reason": "planned_rotation"}),
            (20, {"event": "DEPTH_RECOVERED", "symbol": "BTC_USDT"}),
        ]),
        segment_suffix="planned_meta_only",
    )
    res_plan_meta_only = hqa.check_partition(dp_plan_meta_only, mf_plan_meta_only, qa_cfg)
    assert res_plan_meta_only["qa_status"] == "failed"
    assert res_plan_meta_only["depth_unavailable_events"] >= 1

    # New-connection snapshot alone without a subsequent chained delta on (new_conn, new_epoch) MUST still fail QA!
    dp_plan_snap_only, _, mf_plan_snap_only = _write_custom_15m_partition(
        tmp_path / "qa_p",
        exchange="gate_futures",
        window_start_s=w0_s,
        boot_id="qa_p_snap_only",
        run_id="r",
        rows=[
            (t0 + 1_000_000, 100, "A", 1, 1, "DATA", {"channel": "futures.book_ticker", "event": "update", "result": {"s": "BTC_USDT", "b": "90000.0", "B": "1", "a": "90001.0", "A": "1"}}),
            (t0 + 2_000_000, 200, "A", 1, 2, "DATA", {"channel": "futures.obu", "event": "update", "result": {"full": True, "s": "ob.BTC_USDT.50", "u": 100, "b": [["90000.0", "1"]], "a": [["90001.0", "1"]]}}),
            (t0 + 3_000_000, 300, "A", 1, 3, "DATA", {"channel": "futures.trades", "event": "update", "result": [{"contract": "BTC_USDT", "id": 1, "size": "1", "price": "90000.0"}]}),
            (t0 + 10_000_000, 1000, "B", 1, 4, "META", {"event": "SWITCH", "symbol": "ALL", "from": "A", "to": "B", "reason": "planned_rotation"}),
            (t0 + 11_000_000, 1100, "B", 1, 5, "META", {"event": "DEPTH_UNAVAILABLE", "symbol": "BTC_USDT", "planned": True, "reason": "planned_rotation"}),
            (t0 + 50_000_000, 5000, "B", 1, 6, "DATA", {"channel": "futures.obu", "event": "update", "result": {"full": True, "s": "ob.BTC_USDT.50", "u": 200, "b": [["90000.0", "2"]], "a": [["90001.0", "2"]]}}),
            (t0 + 51_000_000, 5100, "B", 1, 7, "META", {"event": "DEPTH_RECOVERED", "symbol": "BTC_USDT"}),
        ],
        segment_suffix="planned_snap_only",
    )
    res_plan_snap_only = hqa.check_partition(dp_plan_snap_only, mf_plan_snap_only, qa_cfg)
    assert res_plan_snap_only["qa_status"] == "failed"
    assert res_plan_snap_only["depth_unavailable_events"] >= 1

    # Broken delta after new-connection snapshot MUST fail QA!
    dp_plan_broken_delta, _, mf_plan_broken_delta = _write_custom_15m_partition(
        tmp_path / "qa_p",
        exchange="gate_futures",
        window_start_s=w0_s,
        boot_id="qa_p_broken_delta",
        run_id="r",
        rows=[
            (t0 + 1_000_000, 100, "A", 1, 1, "DATA", {"channel": "futures.book_ticker", "event": "update", "result": {"s": "BTC_USDT", "b": "90000.0", "B": "1", "a": "90001.0", "A": "1"}}),
            (t0 + 2_000_000, 200, "A", 1, 2, "DATA", {"channel": "futures.obu", "event": "update", "result": {"full": True, "s": "ob.BTC_USDT.50", "u": 100, "b": [["90000.0", "1"]], "a": [["90001.0", "1"]]}}),
            (t0 + 3_000_000, 300, "A", 1, 3, "DATA", {"channel": "futures.trades", "event": "update", "result": [{"contract": "BTC_USDT", "id": 1, "size": "1", "price": "90000.0"}]}),
            (t0 + 10_000_000, 1000, "B", 1, 4, "META", {"event": "SWITCH", "symbol": "ALL", "from": "A", "to": "B", "reason": "planned_rotation"}),
            (t0 + 11_000_000, 1100, "B", 1, 5, "META", {"event": "DEPTH_UNAVAILABLE", "symbol": "BTC_USDT", "planned": True, "reason": "planned_rotation"}),
            (t0 + 50_000_000, 5000, "B", 1, 6, "DATA", {"channel": "futures.obu", "event": "update", "result": {"full": True, "s": "ob.BTC_USDT.50", "u": 200, "b": [["90000.0", "2"]], "a": [["90001.0", "2"]]}}),
            (t0 + 55_000_000, 5500, "B", 1, 7, "DATA", {"channel": "futures.obu", "event": "update", "result": {"full": False, "s": "ob.BTC_USDT.50", "U": 205, "u": 205, "b": [["90000.0", "3"]], "a": []}}),
            (t0 + 56_000_000, 5600, "B", 1, 8, "META", {"event": "DEPTH_RECOVERED", "symbol": "BTC_USDT"}),
        ],
        segment_suffix="planned_broken_delta",
    )
    res_plan_broken_delta = hqa.check_partition(dp_plan_broken_delta, mf_plan_broken_delta, qa_cfg)
    assert res_plan_broken_delta["qa_status"] == "failed"
    assert res_plan_broken_delta["depth_unavailable_events"] >= 1

    # Invalid B-line snapshots (missing b/a, empty b/a, numeric size, crossed book) MUST NEVER recover
    # planned rotation even when followed by a contiguous delta and DEPTH_RECOVERED META!
    for label, bad_result in [
        ("missing_ba", {"full": True, "s": "ob.BTC_USDT.50", "u": 200}),
        ("empty_ba", {"full": True, "s": "ob.BTC_USDT.50", "u": 200, "b": [], "a": []}),
        ("numeric_size", {"full": True, "s": "ob.BTC_USDT.50", "u": 200, "b": [["90000.0", 2.0]], "a": [["90001.0", "2.0"]]}),
        ("crossed", {"full": True, "s": "ob.BTC_USDT.50", "u": 200, "b": [["90002.0", "2.0"]], "a": [["90001.0", "2.0"]]}),
    ]:
        dp_bad_snap, _, mf_bad_snap = _write_custom_15m_partition(
            tmp_path / "qa_p",
            exchange="gate_futures",
            window_start_s=w0_s,
            boot_id=f"qa_p_bad_{label}",
            run_id="r",
            rows=[
                (t0 + 1_000_000, 100, "A", 1, 1, "DATA", {"channel": "futures.book_ticker", "event": "update", "result": {"s": "BTC_USDT", "b": "90000.0", "B": "1", "a": "90001.0", "A": "1"}}),
                (t0 + 2_000_000, 200, "A", 1, 2, "DATA", {"channel": "futures.obu", "event": "update", "result": {"full": True, "s": "ob.BTC_USDT.50", "u": 100, "b": [["90000.0", "1"]], "a": [["90001.0", "1"]]}}),
                (t0 + 3_000_000, 300, "A", 1, 3, "DATA", {"channel": "futures.trades", "event": "update", "result": [{"contract": "BTC_USDT", "id": 1, "size": "1", "price": "90000.0"}]}),
                (t0 + 10_000_000, 1000, "B", 1, 4, "META", {"event": "SWITCH", "symbol": "ALL", "from": "A", "to": "B", "reason": "planned_rotation"}),
                (t0 + 11_000_000, 1100, "B", 1, 5, "META", {"event": "DEPTH_UNAVAILABLE", "symbol": "BTC_USDT", "planned": True, "reason": "planned_rotation"}),
                (t0 + 50_000_000, 5000, "B", 1, 6, "DATA", {"channel": "futures.obu", "event": "update", "result": bad_result}),
                (t0 + 60_000_000, 6000, "B", 1, 7, "DATA", {"channel": "futures.obu", "event": "update", "result": {"full": False, "s": "ob.BTC_USDT.50", "U": 201, "u": 201, "b": [["90000.0", "2.5"]], "a": []}}),
                (t0 + 61_000_000, 6100, "B", 1, 8, "META", {"event": "DEPTH_RECOVERED", "symbol": "BTC_USDT"}),
            ],
            segment_suffix=f"planned_bad_{label}",
        )
        res_bad_snap = hqa.check_partition(dp_bad_snap, mf_bad_snap, qa_cfg)
        assert res_bad_snap["qa_status"] == "failed", (label, res_bad_snap)
        assert res_bad_snap["depth_unavailable_events"] >= 1, (label, res_bad_snap)
        assert any("invalid depth snapshot" in p for p in res_bad_snap["problems"]), (label, res_bad_snap["problems"])
        assert all(not iv["recovered"] for iv in res_bad_snap["planned_depth_unavailable_intervals"]), (label, res_bad_snap)

    # Genuine planned rotation with on-disk B-line snapshot AND continuous B-line delta passes while preserving depth gap interval!
    dp_plan, _, mf_plan = _write_custom_15m_partition(
        tmp_path / "qa_p",
        exchange="gate_futures",
        window_start_s=w0_s,
        boot_id="qa_p_real",
        run_id="r",
        rows=[
            (t0 + 1_000_000, 100, "A", 1, 1, "DATA", {"channel": "futures.book_ticker", "event": "update", "result": {"s": "BTC_USDT", "b": "90000.0", "B": "1", "a": "90001.0", "A": "1"}}),
            (t0 + 2_000_000, 200, "A", 1, 2, "DATA", {"channel": "futures.obu", "event": "update", "result": {"full": True, "s": "ob.BTC_USDT.50", "u": 100, "b": [["90000.0", "1"]], "a": [["90001.0", "1"]]}}),
            (t0 + 3_000_000, 300, "A", 1, 3, "DATA", {"channel": "futures.trades", "event": "update", "result": [{"contract": "BTC_USDT", "id": 1, "size": "1", "price": "90000.0"}]}),
            (t0 + 4_000_000, 400, "A", 1, 4, "DATA", {"channel": "futures.obu", "event": "update", "result": {"full": False, "s": "ob.BTC_USDT.50", "U": 101, "u": 101, "b": [["90000.0", "1.5"]], "a": []}}),
            (t0 + 10_000_000, 1000, "B", 1, 5, "META", {"event": "SWITCH", "symbol": "ALL", "from": "A", "to": "B", "reason": "planned_rotation"}),
            (t0 + 11_000_000, 1100, "B", 1, 6, "META", {"event": "DEPTH_UNAVAILABLE", "symbol": "BTC_USDT", "planned": True, "reason": "planned_rotation"}),
            (t0 + 15_000_000, 1500, "B", 1, 7, "DATA", {"channel": "futures.book_ticker", "event": "update", "result": {"s": "BTC_USDT", "b": "90000.0", "B": "1", "a": "90001.0", "A": "1"}}),
            (t0 + 50_000_000, 5000, "B", 1, 8, "DATA", {"channel": "futures.obu", "event": "update", "result": {"full": True, "s": "ob.BTC_USDT.50", "u": 200, "b": [["90000.0", "2"]], "a": [["90001.0", "2"]]}}),
            (t0 + 60_000_000, 6000, "B", 1, 9, "DATA", {"channel": "futures.obu", "event": "update", "result": {"full": False, "s": "ob.BTC_USDT.50", "U": 201, "u": 201, "b": [["90000.0", "2.5"]], "a": []}}),
            (t0 + 61_000_000, 6100, "B", 1, 10, "META", {"event": "DEPTH_RECOVERED", "symbol": "BTC_USDT"}),
            (t0 + 70_000_000, 7000, "B", 1, 11, "DATA", {"channel": "futures.trades", "event": "update", "result": [{"contract": "BTC_USDT", "id": 2, "size": "1", "price": "90000.0"}]}),
            (t0 + 80_000_000, 8000, "B", 1, 12, "DATA", {"channel": "futures.obu", "event": "update", "result": {"full": False, "s": "ob.BTC_USDT.50", "U": 202, "u": 202, "b": [["90000.0", "2.6"]], "a": []}}),
        ],
        segment_suffix="planned",
    )
    res_plan = hqa.check_partition(dp_plan, mf_plan, qa_cfg)
    assert res_plan["qa_status"] == "passed", res_plan["problems"]
    assert res_plan["depth_unavailable_events"] == 0
    assert res_plan["planned_depth_refresh_events"] == 1
    assert len(res_plan["planned_depth_unavailable_intervals"]) == 1
    iv = res_plan["planned_depth_unavailable_intervals"][0]
    assert iv["symbol"] == "BTC_USDT"
    assert iv["recovered"] is True
    assert iv["duration_seconds"] >= 0.035
    assert res_plan["max_gap_by_category"]["depth"] is not None
    assert res_plan["max_gap_by_category"]["depth"] >= 0.045

    dp_unplan, _, mf_unplan = _write_custom_15m_partition(
        tmp_path / "qa_p",
        exchange="gate_futures",
        window_start_s=w0_s,
        boot_id="qa_u",
        run_id="r",
        rows=gate_base_rows([
            (10, {"event": "DEPTH_UNAVAILABLE", "symbol": "BTC_USDT", "reason": "standby_promoted_fresh_anchor"}),
            (20, {"event": "DEPTH_RECOVERED", "symbol": "BTC_USDT"}),
        ]),
        segment_suffix="unplanned",
    )
    res_unplan = hqa.check_partition(dp_unplan, mf_unplan, qa_cfg)
    assert res_unplan["qa_status"] == "failed"
    assert res_unplan["depth_unavailable_events"] == 1


def test_issue19_qa_convert_same_day_restart_boot_id_isolation(tmp_path: Path):
    """Same-UTC-day collector restart reusing (A, 1) must isolate depth anchors by boot_id:
    - cross-boot delta without new-boot snapshot is rejected as missing_same_connection_anchor
    - clean same-day restart with fresh new-boot snapshot remains eligible across Gate, Bybit, and Binance.
    """
    w0_s = int(datetime(2026, 11, 21, 14, 0, tzinfo=timezone.utc).timestamp())
    w1_s = w0_s + 900
    t0 = w0_s * 1_000_000_000
    t1 = w1_s * 1_000_000_000

    def write_same_day_pair(root: Path, include_boot2_snapshot: bool) -> None:
        # 1. Gate Futures (BTC_USDT -> canonical asset BTC)
        gate_b1 = [
            (t0 + 1_000_000, 100, "A", 1, 1, "DATA", {"channel": "futures.book_ticker", "event": "update", "result": {"s": "BTC_USDT", "b": "90000.00", "B": "5", "a": "90001.00", "A": "5"}}),
            (t0 + 2_000_000, 200, "A", 1, 2, "DATA", {"channel": "futures.trades", "event": "update", "result": [{"contract": "BTC_USDT", "id": 1, "size": "1", "price": "90000.00"}]}),
            (t0 + 3_000_000, 300, "A", 1, 3, "DATA", {"channel": "futures.obu", "event": "update", "result": {"t": (t0 // 1_000_000) + 3, "full": True, "s": "ob.BTC_USDT.50", "u": 100, "b": [["90000.00", "2.0"]], "a": [["90001.00", "2.0"]]}}),
            (t0 + 4_000_000, 400, "A", 1, 4, "DATA", {"channel": "futures.obu", "event": "update", "result": {"t": (t0 // 1_000_000) + 4, "full": False, "s": "ob.BTC_USDT.50", "U": 101, "u": 101, "b": [["90000.00", "2.5"]], "a": []}}),
        ]
        gate_b2: list[tuple[int, int, str, int, int, str, dict]] = [
            (t1 + 1_000_000, 100, "A", 1, 1, "DATA", {"channel": "futures.book_ticker", "event": "update", "result": {"s": "BTC_USDT", "b": "90000.00", "B": "5", "a": "90001.00", "A": "5"}}),
            (t1 + 2_000_000, 200, "A", 1, 2, "DATA", {"channel": "futures.trades", "event": "update", "result": [{"contract": "BTC_USDT", "id": 2, "size": "1", "price": "90000.00"}]}),
        ]
        if include_boot2_snapshot:
            gate_b2.extend([
                (t1 + 3_000_000, 300, "A", 1, 3, "DATA", {"channel": "futures.obu", "event": "update", "result": {"t": (t1 // 1_000_000) + 3, "full": True, "s": "ob.BTC_USDT.50", "u": 500, "b": [["90000.00", "3.0"]], "a": [["90001.00", "3.0"]]}}),
                (t1 + 4_000_000, 400, "A", 1, 4, "DATA", {"channel": "futures.obu", "event": "update", "result": {"t": (t1 // 1_000_000) + 4, "full": False, "s": "ob.BTC_USDT.50", "U": 501, "u": 501, "b": [["90000.00", "3.2"]], "a": []}}),
            ])
        else:
            # Reuses (A, 1) and even continues u=102 from boot_1 without a boot_2 snapshot!
            gate_b2.append(
                (t1 + 3_000_000, 300, "A", 1, 3, "DATA", {"channel": "futures.obu", "event": "update", "result": {"t": (t1 // 1_000_000) + 3, "full": False, "s": "ob.BTC_USDT.50", "U": 102, "u": 102, "b": [["90000.00", "9.9"]], "a": []}})
            )
        _write_custom_15m_partition(root, exchange="gate_futures", window_start_s=w0_s, boot_id="boot_1", run_id="r1", rows=gate_b1, segment_suffix="b1")
        _write_custom_15m_partition(root, exchange="gate_futures", window_start_s=w1_s, boot_id="boot_2", run_id="r2", rows=gate_b2, segment_suffix="b2")

        # 2. Bybit Linear (ETHUSDT -> canonical asset ETH)
        bybit_b1 = [
            (t0 + 1_000_000, 100, "A", 1, 1, "DATA", {"topic": "orderbook.1.ETHUSDT", "type": "snapshot", "ts": (t0 // 1_000_000) + 1, "data": {"s": "ETHUSDT", "u": 10, "seq": 10, "b": [["3000.0", "2"]], "a": [["3001.0", "2"]]}}),
            (t0 + 2_000_000, 200, "A", 1, 2, "DATA", {"topic": "publicTrade.ETHUSDT", "type": "snapshot", "ts": (t0 // 1_000_000) + 2, "data": [{"T": (t0 // 1_000_000) + 2, "s": "ETHUSDT", "S": "Buy", "v": "0.1", "p": "3000.0", "i": "e1"}]}),
            (t0 + 3_000_000, 300, "A", 1, 3, "DATA", {"topic": "orderbook.50.ETHUSDT", "type": "snapshot", "ts": (t0 // 1_000_000) + 3, "data": {"s": "ETHUSDT", "u": 100, "seq": 1000, "b": [["3000.0", "2.0"]], "a": [["3001.0", "2.0"]]}}),
            (t0 + 4_000_000, 400, "A", 1, 4, "DATA", {"topic": "orderbook.50.ETHUSDT", "type": "delta", "ts": (t0 // 1_000_000) + 4, "data": {"s": "ETHUSDT", "u": 101, "seq": 1010, "b": [["3000.0", "2.5"]], "a": []}}),
        ]
        bybit_b2: list[tuple[int, int, str, int, int, str, dict]] = [
            (t1 + 1_000_000, 100, "A", 1, 1, "DATA", {"topic": "orderbook.1.ETHUSDT", "type": "snapshot", "ts": (t1 // 1_000_000) + 1, "data": {"s": "ETHUSDT", "u": 20, "seq": 20, "b": [["3000.0", "2"]], "a": [["3001.0", "2"]]}}),
            (t1 + 2_000_000, 200, "A", 1, 2, "DATA", {"topic": "publicTrade.ETHUSDT", "type": "snapshot", "ts": (t1 // 1_000_000) + 2, "data": [{"T": (t1 // 1_000_000) + 2, "s": "ETHUSDT", "S": "Buy", "v": "0.1", "p": "3000.0", "i": "e2"}]}),
        ]
        if include_boot2_snapshot:
            bybit_b2.extend([
                (t1 + 3_000_000, 300, "A", 1, 3, "DATA", {"topic": "orderbook.50.ETHUSDT", "type": "snapshot", "ts": (t1 // 1_000_000) + 3, "data": {"s": "ETHUSDT", "u": 500, "seq": 5000, "b": [["3000.0", "3.0"]], "a": [["3001.0", "3.0"]]}}),
                (t1 + 4_000_000, 400, "A", 1, 4, "DATA", {"topic": "orderbook.50.ETHUSDT", "type": "delta", "ts": (t1 // 1_000_000) + 4, "data": {"s": "ETHUSDT", "u": 501, "seq": 5010, "b": [["3000.0", "3.2"]], "a": []}}),
            ])
        else:
            # Reuses (A, 1) and even continues monotonic u=102 without a boot_2 snapshot!
            bybit_b2.append(
                (t1 + 3_000_000, 300, "A", 1, 3, "DATA", {"topic": "orderbook.50.ETHUSDT", "type": "delta", "ts": (t1 // 1_000_000) + 3, "data": {"s": "ETHUSDT", "u": 102, "seq": 1020, "b": [["3000.0", "9.9"]], "a": []}})
            )
        _write_custom_15m_partition(root, exchange="bybit_linear", window_start_s=w0_s, boot_id="boot_1", run_id="r1", rows=bybit_b1, segment_suffix="b1")
        _write_custom_15m_partition(root, exchange="bybit_linear", window_start_s=w1_s, boot_id="boot_2", run_id="r2", rows=bybit_b2, segment_suffix="b2")

        # 3. Binance Futures (SOLUSDT -> canonical asset SOL)
        bin_b1 = [
            (t0 + 1_000_000, 100, "A", 1, 1, "DATA", {"stream": "solusdt@bookTicker", "data": {"e": "bookTicker", "u": 10, "s": "SOLUSDT", "b": "200.00", "B": "5", "a": "201.00", "A": "5"}}),
            (t0 + 2_000_000, 200, "A", 1, 2, "META", {"event": "REST_SNAPSHOT_ANCHOR", "endpoint": "binance_public", "conn_id": "A", "epoch": 1, "symbol": "SOLUSDT", "snapshot": {"lastUpdateId": 102, "bids": [["200.00", "2.0"]], "asks": [["201.00", "2.0"]]}}),
            (t0 + 3_000_000, 300, "A", 1, 3, "META", {"event": "DEPTH_BRIDGED_DELTA", "endpoint": "binance_public", "conn_id": "A", "epoch": 1, "symbol": "SOLUSDT", "U": 100, "u": 104, "pu": 99, "raw": json.dumps({"stream": "solusdt@depth@100ms", "data": {"e": "depthUpdate", "s": "SOLUSDT", "U": 100, "u": 104, "pu": 99, "b": [["200.00", "2.5"]], "a": []}})}),
            (t0 + 4_000_000, 400, "A", 1, 4, "DATA", {"stream": "solusdt@depth@100ms", "data": {"e": "depthUpdate", "s": "SOLUSDT", "U": 105, "u": 106, "pu": 104, "b": [["200.00", "2.6"]], "a": []}}),
        ]
        bin_b2: list[tuple[int, int, str, int, int, str, dict]] = [
            (t1 + 1_000_000, 100, "A", 1, 1, "DATA", {"stream": "solusdt@bookTicker", "data": {"e": "bookTicker", "u": 20, "s": "SOLUSDT", "b": "200.00", "B": "5", "a": "201.00", "A": "5"}}),
        ]
        if include_boot2_snapshot:
            bin_b2.extend([
                (t1 + 2_000_000, 200, "A", 1, 2, "META", {"event": "REST_SNAPSHOT_ANCHOR", "endpoint": "binance_public", "conn_id": "A", "epoch": 1, "symbol": "SOLUSDT", "snapshot": {"lastUpdateId": 502, "bids": [["200.00", "3.0"]], "asks": [["201.00", "3.0"]]}}),
                (t1 + 3_000_000, 300, "A", 1, 3, "META", {"event": "DEPTH_BRIDGED_DELTA", "endpoint": "binance_public", "conn_id": "A", "epoch": 1, "symbol": "SOLUSDT", "U": 500, "u": 504, "pu": 499, "raw": json.dumps({"stream": "solusdt@depth@100ms", "data": {"e": "depthUpdate", "s": "SOLUSDT", "U": 500, "u": 504, "pu": 499, "b": [["200.00", "3.2"]], "a": []}})}),
                (t1 + 4_000_000, 400, "A", 1, 4, "DATA", {"stream": "solusdt@depth@100ms", "data": {"e": "depthUpdate", "s": "SOLUSDT", "U": 505, "u": 506, "pu": 504, "b": [["200.00", "3.5"]], "a": []}}),
            ])
        else:
            # Reuses (A, 1) and even continues pu=106 from boot_1 without a boot_2 REST_SNAPSHOT_ANCHOR!
            bin_b2.append(
                (t1 + 2_000_000, 200, "A", 1, 2, "DATA", {"stream": "solusdt@depth@100ms", "data": {"e": "depthUpdate", "s": "SOLUSDT", "U": 107, "u": 108, "pu": 106, "b": [["200.00", "9.9"]], "a": []}})
            )
        _write_custom_15m_partition(root, exchange="binance_futures", window_start_s=w0_s, boot_id="boot_1", run_id="r1", rows=bin_b1, segment_suffix="b1")
        _write_custom_15m_partition(root, exchange="binance_futures", window_start_s=w1_s, boot_id="boot_2", run_id="r2", rows=bin_b2, segment_suffix="b2")

    # (a) Cross-boot splice without new-boot snapshot MUST be rejected on all 3 exchanges
    bad_root = tmp_path / "same_day_bad"
    write_same_day_pair(bad_root, include_boot2_snapshot=False)
    bad_summary = run_qa_convert(bad_root, tmp_path / "hbt_bad", allow_unverified_local=True)
    assert bad_summary["status"] == "ok"
    assert bad_summary["execution_unverified_asset_days"] == 3
    assert len(bad_summary["unrebuildable_depth_markets"]) == 3
    bad_cal = {r["symbol"]: r for r in json.loads(Path(bad_summary["calendar_path"]).read_text(encoding="utf-8"))}
    for sym in ("BTC", "ETH", "SOL"):
        row = bad_cal[sym]
        assert row["eligibility_status"] == "execution_unverified", row
        assert row["depth_rebuild_status"] == "unrebuildable_excluded", row
        assert "missing_same_connection_anchor" in row["unrebuildable_depth_reasons"], row
        assert row["excluded_unrebuildable_depth_rows"] == 1, row
        assert "follower_l2_hbt" not in row["files"], row
        assert "binance_depth_diag" not in row["files"], row

    # (b) Clean same-day restart with fresh new-boot snapshot MUST remain eligible on all 3 exchanges
    good_root = tmp_path / "same_day_good"
    write_same_day_pair(good_root, include_boot2_snapshot=True)
    good_summary = run_qa_convert(good_root, tmp_path / "hbt_good", allow_unverified_local=True)
    assert good_summary["status"] == "ok"
    assert good_summary["unrebuildable_depth_markets"] == []
    assert good_summary["execution_unverified_asset_days"] == 0
    assert good_summary["eligible_asset_days"] == 3
    good_cal = {r["symbol"]: r for r in json.loads(Path(good_summary["calendar_path"]).read_text(encoding="utf-8"))}
    for sym in ("BTC", "ETH", "SOL"):
        row = good_cal[sym]
        assert row["eligibility_status"] == "eligible", row
        assert row["depth_rebuild_status"] == "ok", row


def test_issue19_qa_convert_unrecovered_planned_rotation_blocks_and_gap_clears_stale_book_in_hbt_replay(tmp_path: Path):
    """Verify both backtest-converter guarantees for planned rotation:
    1) Unrecovered planned rotation (no B snapshot, META-only DEPTH_RECOVERED, or invalid B snapshot missing b/a)
       blocks admission (`execution_unverified`, `depth_unavailable_unrecovered`) and omits depth .npz files.
    2) Recovered planned rotation emits DEPTH_CLEAR_EVENT at cutover so HFTBacktest 2.4.4 replay sees an empty book
       inside the gap and NEVER fills orders against stale pre-switch A-line depth.
    """
    import math
    import numpy as np
    from hftbacktest import BacktestAsset, ROIVectorMarketDepthBacktest, GTC, LIMIT

    w0_s = int(datetime(2026, 11, 21, 14, 0, tzinfo=timezone.utc).timestamp())
    t0 = w0_s * 1_000_000_000
    t0_ms = w0_s * 1000

    # --- 1. Unrecovered planned rotations MUST block admission and never export depth .npz ---
    for case_name, trailing_rows in [
        ("no_b_snapshot", []),
        (
            "meta_only_recovered",
            [
                (t0 + 2_500_000_000, 2500, "B", 1, 7, "META", {"event": "DEPTH_RECOVERED", "symbol": "BTC_USDT"}),
            ],
        ),
        (
            "invalid_b_snapshot_missing_ba",
            [
                (t0 + 2_500_000_000, 2500, "B", 1, 7, "DATA", {"channel": "futures.obu", "event": "update", "result": {"t": t0_ms + 2500, "full": True, "s": "ob.BTC_USDT.50", "u": 200}}),
                (t0 + 2_600_000_000, 2600, "B", 1, 8, "META", {"event": "DEPTH_RECOVERED", "symbol": "BTC_USDT"}),
            ],
        ),
    ]:
        root = tmp_path / f"unrec_{case_name}"
        rows = [
            (t0 + 1_000_000_000, 1000, "A", 1, 1, "DATA", {"channel": "futures.book_ticker", "event": "update", "result": {"s": "BTC_USDT", "b": "90000.0", "B": "5", "a": "90001.0", "A": "5"}}),
            (t0 + 1_000_000_000, 1001, "A", 1, 2, "DATA", {"channel": "futures.obu", "event": "update", "result": {"t": t0_ms + 1000, "full": True, "s": "ob.BTC_USDT.50", "u": 100, "b": [["90000.0", "10.0"]], "a": [["90001.0", "10.0"]]}}),
            (t0 + 1_100_000_000, 1100, "A", 1, 3, "DATA", {"channel": "futures.obu", "event": "update", "result": {"t": t0_ms + 1100, "full": False, "s": "ob.BTC_USDT.50", "U": 101, "u": 101, "b": [["90000.0", "11.0"]], "a": []}}),
            (t0 + 2_000_000_000, 2000, "B", 1, 4, "META", {"event": "SWITCH", "symbol": "ALL", "from": "A", "to": "B", "reason": "planned_rotation"}),
            (t0 + 2_000_000_000, 2001, "B", 1, 5, "META", {"event": "DEPTH_UNAVAILABLE", "symbol": "BTC_USDT", "planned": True, "reason": "planned_rotation", "conn_id": "B", "epoch": 1}),
            (t0 + 2_200_000_000, 2200, "B", 1, 6, "DATA", {"channel": "futures.book_ticker", "event": "update", "result": {"s": "BTC_USDT", "b": "90010.0", "B": "5", "a": "90011.0", "A": "5"}}),
            *trailing_rows,
        ]
        _write_custom_15m_partition(root, exchange="gate_futures", window_start_s=w0_s, boot_id=f"boot_{case_name}", run_id="r", rows=rows, segment_suffix=case_name)
        summary = run_qa_convert(root, tmp_path / f"out_{case_name}", allow_unverified_local=True)
        assert summary["status"] == "ok"
        assert summary["eligible_asset_days"] == 0, (case_name, summary)
        assert summary["execution_unverified_asset_days"] == 1, (case_name, summary)
        cal = json.loads(Path(summary["calendar_path"]).read_text(encoding="utf-8"))
        assert len(cal) == 1
        row = cal[0]
        assert row["eligibility_status"] == "execution_unverified", (case_name, row)
        assert row["depth_rebuild_status"] == "unrebuildable_excluded", (case_name, row)
        assert "depth_unavailable_unrecovered" in row["unverified_reasons"], (case_name, row)
        assert "follower_l2_hbt" not in row["files"], (case_name, row)
        assert "follower_l2_edges" not in row["files"], (case_name, row)
        assert "follower_l1_bbo" in row["files"], (case_name, row)

    # Also verify Binance unrecovered planned rotation blocks admission and omits binance_depth_diag!
    bin_root = tmp_path / "unrec_binance"
    bin_rows = [
        (t0 + 1_000_000_000, 1000, "A", 1, 1, "DATA", {"stream": "btcusdt@bookTicker", "data": {"e": "bookTicker", "u": 10, "s": "BTCUSDT", "b": "90000.0", "B": "5", "a": "90001.0", "A": "5"}}),
        (t0 + 1_000_000_000, 1001, "A", 1, 2, "META", {"event": "REST_SNAPSHOT_ANCHOR", "endpoint": "binance_public", "conn_id": "A", "epoch": 1, "symbol": "BTCUSDT", "snapshot": {"lastUpdateId": 102, "bids": [["90000.0", "10.0"]], "asks": [["90001.0", "10.0"]]}}),
        (t0 + 1_100_000_000, 1100, "A", 1, 3, "META", {"event": "DEPTH_BRIDGED_DELTA", "endpoint": "binance_public", "conn_id": "A", "epoch": 1, "symbol": "BTCUSDT", "U": 100, "u": 104, "pu": 99, "raw": json.dumps({"stream": "btcusdt@depth@100ms", "data": {"e": "depthUpdate", "s": "BTCUSDT", "U": 100, "u": 104, "pu": 99, "b": [["90000.0", "11.0"]], "a": []}})}),
        (t0 + 2_000_000_000, 2000, "B", 1, 4, "META", {"event": "SWITCH", "symbol": "ALL", "from": "A", "to": "B", "reason": "planned_rotation"}),
        (t0 + 2_000_000_000, 2001, "B", 1, 5, "META", {"event": "DEPTH_UNAVAILABLE", "symbol": "BTCUSDT", "planned": True, "reason": "planned_rotation", "conn_id": "B", "epoch": 1}),
    ]
    _write_custom_15m_partition(bin_root, exchange="binance_futures", window_start_s=w0_s, boot_id="boot_bin_unrec", run_id="r", rows=bin_rows, segment_suffix="bin_unrec")
    bin_summary = run_qa_convert(bin_root, tmp_path / "out_bin_unrec", allow_unverified_local=True)
    assert bin_summary["eligible_asset_days"] == 0
    assert bin_summary["execution_unverified_asset_days"] == 1
    bin_row = json.loads(Path(bin_summary["calendar_path"]).read_text(encoding="utf-8"))[0]
    assert bin_row["eligibility_status"] == "execution_unverified"
    assert "depth_unavailable_unrecovered" in bin_row["unverified_reasons"]
    assert "binance_depth_diag" not in bin_row["files"]

    # --- 2. Recovered planned rotation across snapshot receive latencies (high / equal / low B latency):
    #     local view clears during gap & recovers cleanly WITHOUT fabricating an EXCH_EVENT clear that
    #     could sort after a higher-latency B-line snapshot and wipe the simulated matching exchange! ---
    from hftbacktest import DEPTH_CLEAR_EVENT, EXCH_EVENT, LOCAL_EVENT
    from qa_convert import audit_fills_against_depth_unavailable_intervals, evaluate_50_level_coverage

    ms = 1_000_000
    for lat_name, a_exch_ms, a_loc_ms, b_exch_ms, b_loc_ms in [
        # Exact reviewer repro: A has 10ms latency (1090/1100), switch at 2000ms, B snapshot has 100ms latency (1950/2050)!
        ("high_lat_b_snapshot", 1090, 1100, 1950, 2050),
        # Equal latency: A has 10ms latency, B has 10ms latency after 2s gap
        ("equal_lat_b_snapshot", 1090, 1100, 3990, 4000),
        # Lower latency on B: A has 100ms latency, B has 5ms latency
        ("low_lat_b_snapshot", 1000, 1100, 3995, 4000),
    ]:
        rec_root = tmp_path / f"rec_planned_{lat_name}"
        rec_rows = [
            (t0 + a_loc_ms * ms, 1000, "A", 1, 1, "DATA", {"channel": "futures.book_ticker", "event": "update", "result": {"s": "BTC_USDT", "b": "90000.0", "B": "5", "a": "90001.0", "A": "5"}}),
            (t0 + a_loc_ms * ms, 1001, "A", 1, 2, "DATA", {"channel": "futures.obu", "event": "update", "result": {"t": t0_ms + a_exch_ms, "full": True, "s": "ob.BTC_USDT.50", "u": 100, "b": [["90000.0", "10.0"]], "a": [["90001.0", "10.0"]]}}),
            (t0 + (a_loc_ms + 50) * ms, 1100, "A", 1, 3, "DATA", {"channel": "futures.obu", "event": "update", "result": {"t": t0_ms + a_exch_ms + 50, "full": False, "s": "ob.BTC_USDT.50", "U": 101, "u": 101, "b": [["90000.0", "12.0"]], "a": []}}),
            # Planned rotation cutover at t0 + 2000ms -> emits strictly LOCAL_EVENT | DEPTH_CLEAR_EVENT
            (t0 + 2000 * ms, 2000, "B", 1, 4, "META", {"event": "SWITCH", "symbol": "ALL", "from": "A", "to": "B", "reason": "planned_rotation"}),
            (t0 + 2000 * ms, 2001, "B", 1, 5, "META", {"event": "DEPTH_UNAVAILABLE", "symbol": "BTC_USDT", "planned": True, "reason": "planned_rotation", "conn_id": "B", "epoch": 1}),
            # Fresh B-line snapshot at shifted prices (90050 / 90051) + continuous deltas
            (t0 + b_loc_ms * ms, 4000, "B", 1, 6, "DATA", {"channel": "futures.obu", "event": "update", "result": {"t": t0_ms + b_exch_ms, "full": True, "s": "ob.BTC_USDT.50", "u": 200, "b": [["90050.0", "10.0"]], "a": [["90051.0", "10.0"]]}}),
            (t0 + (b_loc_ms + 1) * ms, 4001, "B", 1, 7, "META", {"event": "DEPTH_RECOVERED", "symbol": "BTC_USDT"}),
            (t0 + (b_loc_ms + 50) * ms, 4100, "B", 1, 8, "DATA", {"channel": "futures.obu", "event": "update", "result": {"t": t0_ms + b_exch_ms + 50, "full": False, "s": "ob.BTC_USDT.50", "U": 201, "u": 201, "b": [["90050.0", "11.0"]], "a": []}}),
            (t0 + (b_loc_ms + 500) * ms, 4500, "B", 1, 9, "DATA", {"channel": "futures.obu", "event": "update", "result": {"t": t0_ms + b_exch_ms + 500, "full": False, "s": "ob.BTC_USDT.50", "U": 202, "u": 202, "b": [["90050.0", "12.0"]], "a": []}}),
        ]
        _write_custom_15m_partition(rec_root, exchange="gate_futures", window_start_s=w0_s, boot_id=f"boot_{lat_name}", run_id="r", rows=rec_rows, segment_suffix=lat_name)
        rec_summary = run_qa_convert(rec_root, tmp_path / f"out_{lat_name}", allow_unverified_local=True)
        assert rec_summary["status"] == "ok"
        assert rec_summary["eligible_asset_days"] == 1, (lat_name, rec_summary)
        rec_row = json.loads(Path(rec_summary["calendar_path"]).read_text(encoding="utf-8"))[0]
        assert rec_row["eligibility_status"] == "eligible", (lat_name, rec_row)
        assert rec_row["depth_rebuild_status"] == "ok", (lat_name, rec_row)
        assert rec_row["gap_execution_policy"] == "local_clear_only_retain_gap_fills_as_unverified"
        l2_path = Path(rec_row["files"]["follower_l2_hbt"])
        assert l2_path.is_file()

        data = np.load(l2_path)["data"]
        # Verify cutover clear is strictly LOCAL_EVENT-only (NEVER EXCH_EVENT)!
        exch_clears = [r for r in data if (int(r["ev"]) & int(EXCH_EVENT)) and (int(r["ev"]) & 0xFF) == int(DEPTH_CLEAR_EVENT)]
        local_only_clears = [
            r for r in data
            if (int(r["ev"]) & int(LOCAL_EVENT))
            and not (int(r["ev"]) & int(EXCH_EVENT))
            and (int(r["ev"]) & 0xFF) == int(DEPTH_CLEAR_EVENT)
        ]
        assert len(exch_clears) == 2, (lat_name, "expected only genuine A & B snapshot clears on exchange side")
        assert len(local_only_clears) == 1, (lat_name, "expected exactly 1 LOCAL_EVENT-only clear at cutover")
        assert int(local_only_clears[0]["local_ts"]) == t0 + 2000 * ms

        asset = (
            BacktestAsset()
            .data([data])
            .linear_asset(1.0)
            .constant_order_latency(1 * ms, 1 * ms)
            .risk_adverse_queue_model()
            .no_partial_fill_exchange()
            .trading_value_fee_model(0.0, 0.0)
            .tick_size(0.1)
            .lot_size(1.0)
            .roi_lb(89900.0)
            .roi_ub(90200.0)
        )
        hbt = ROIVectorMarketDepthBacktest([asset])
        assert hbt.elapse(1) == 0
        # Pre-rotation (t0 + 1500ms): A-line book active
        assert hbt.elapse((t0 + 1500 * ms) - hbt.current_timestamp) == 0
        assert hbt.depth(0).best_bid == 90000.0, lat_name
        assert hbt.depth(0).best_ask == 90001.0, lat_name

        # Inside gap (t0 + 2020ms): local book MUST be empty (nan, nan)
        assert hbt.elapse((t0 + 2020 * ms) - hbt.current_timestamp) == 0
        assert math.isnan(hbt.depth(0).best_bid), (lat_name, hbt.depth(0).best_bid)
        assert math.isnan(hbt.depth(0).best_ask), (lat_name, hbt.depth(0).best_ask)

        # Post-recovery (t0 + b_loc_ms + 200ms): BOTH local book AND simulated exchange matching engine
        # MUST hold B-line's fresh snapshot (90050 / 90051) — exchange side must NOT have been wiped!
        post_ms = b_loc_ms + 200
        assert hbt.elapse((t0 + post_ms * ms) - hbt.current_timestamp) == 0
        assert hbt.depth(0).best_bid == 90050.0, lat_name
        assert hbt.depth(0).best_ask == 90051.0, lat_name
        hbt.submit_buy_order(0, 2, 90051.0, 1.0, GTC, LIMIT, False)
        assert hbt.elapse(50 * ms) == 0
        assert hbt.position(0) == 1.0, f"{lat_name}: exchange matching book was wiped after B snapshot recovery!"

    # --- 3. Pre-switch resting passive order + public trade inside rotation gap:
    #     HftBacktest retains pre-existing resting orders after clear and fills them on trade-through;
    #     explicit gap-fill audit MUST retain the fill in raw accounting AND flag it unverified! ---
    trade_gap_root = tmp_path / "rec_planned_resting_trade_gap"
    trade_gap_rows = [
        (t0 + 1000 * ms, 1000, "A", 1, 1, "DATA", {"channel": "futures.book_ticker", "event": "update", "result": {"s": "BTC_USDT", "b": "90000.0", "B": "5", "a": "90001.0", "A": "5"}}),
        (t0 + 1000 * ms, 1001, "A", 1, 2, "DATA", {"channel": "futures.obu", "event": "update", "result": {"t": t0_ms + 995, "full": True, "s": "ob.BTC_USDT.50", "u": 100, "b": [["90000.0", "10.0"]], "a": [["90001.0", "10.0"]]}}),
        (t0 + 1100 * ms, 1100, "A", 1, 3, "DATA", {"channel": "futures.obu", "event": "update", "result": {"t": t0_ms + 1095, "full": False, "s": "ob.BTC_USDT.50", "U": 101, "u": 101, "b": [["90000.0", "10.0"]], "a": []}}),
        # Planned rotation cutover at t0 + 2000ms
        (t0 + 2000 * ms, 2000, "B", 1, 4, "META", {"event": "SWITCH", "symbol": "ALL", "from": "A", "to": "B", "reason": "planned_rotation"}),
        (t0 + 2000 * ms, 2001, "B", 1, 5, "META", {"event": "DEPTH_UNAVAILABLE", "symbol": "BTC_USDT", "planned": True, "reason": "planned_rotation", "conn_id": "B", "epoch": 1}),
        # Public sell trade at 90000.0 (size=-20.0) arrives INSIDE the gap at t0 + 2500ms!
        (t0 + 2500 * ms, 2500, "B", 1, 6, "DATA", {"channel": "futures.trades", "event": "update", "result": [{"contract": "BTC_USDT", "id": 9001, "size": "-20.0", "price": "90000.0", "create_time_ms": t0_ms + 2495}]}),
        # Fresh B-line snapshot arrives at t0 + 4000ms + continuous deltas through t0 + 5000ms
        (t0 + 4000 * ms, 4000, "B", 1, 7, "DATA", {"channel": "futures.obu", "event": "update", "result": {"t": t0_ms + 3995, "full": True, "s": "ob.BTC_USDT.50", "u": 200, "b": [["90050.0", "10.0"]], "a": [["90051.0", "10.0"]]}}),
        (t0 + 4001 * ms, 4001, "B", 1, 8, "META", {"event": "DEPTH_RECOVERED", "symbol": "BTC_USDT"}),
        (t0 + 4100 * ms, 4100, "B", 1, 9, "DATA", {"channel": "futures.obu", "event": "update", "result": {"t": t0_ms + 4095, "full": False, "s": "ob.BTC_USDT.50", "U": 201, "u": 201, "b": [["90050.0", "11.0"]], "a": []}}),
        (t0 + 5000 * ms, 5000, "B", 1, 10, "DATA", {"channel": "futures.obu", "event": "update", "result": {"t": t0_ms + 4995, "full": False, "s": "ob.BTC_USDT.50", "U": 202, "u": 202, "b": [["90050.0", "12.0"]], "a": []}}),
    ]
    _write_custom_15m_partition(trade_gap_root, exchange="gate_futures", window_start_s=w0_s, boot_id="boot_rest_gap_trade", run_id="r", rows=trade_gap_rows, segment_suffix="gap_trade")
    tg_summary = run_qa_convert(trade_gap_root, tmp_path / "out_gap_trade", allow_unverified_local=True)
    assert tg_summary["status"] == "ok"
    tg_row = json.loads(Path(tg_summary["calendar_path"]).read_text(encoding="utf-8"))[0]
    assert tg_row["depth_rebuild_status"] == "ok"
    assert len(tg_row["depth_unavailable_intervals"]) == 1

    tg_data = np.load(tg_row["files"]["follower_l2_hbt"])["data"]
    tg_asset = (
        BacktestAsset()
        .data([tg_data])
        .linear_asset(1.0)
        .constant_order_latency(1 * ms, 1 * ms)
        .risk_adverse_queue_model()
        .no_partial_fill_exchange()
        .trading_value_fee_model(0.0, 0.0)
        .tick_size(0.1)
        .lot_size(1.0)
        .roi_lb(89900.0)
        .roi_ub(90200.0)
    )
    hbt_tg = ROIVectorMarketDepthBacktest([tg_asset])
    hbt_tg.elapse(1)
    # Pre-rotation (t0 + 1500ms): place passive resting buy at best bid 90000.0
    hbt_tg.elapse((t0 + 1500 * ms) - hbt_tg.current_timestamp)
    hbt_tg.submit_buy_order(0, 10, 90000.0, 1.0, GTC, LIMIT, False)
    # Inside gap BEFORE public trade (t0 + 2200ms): local book cleared, resting order still open (pos=0)
    hbt_tg.elapse((t0 + 2200 * ms) - hbt_tg.current_timestamp)
    assert math.isnan(hbt_tg.depth(0).best_bid)
    assert math.isnan(hbt_tg.depth(0).best_ask)
    assert hbt_tg.position(0) == 0.0
    # Inside gap AFTER public sell trade at 90000.0 (t0 + 2600ms): resting order fills in NoPartialFillExchange!
    hbt_tg.elapse((t0 + 2600 * ms) - hbt_tg.current_timestamp)
    assert hbt_tg.position(0) == 1.0
    ord_gap = hbt_tg.orders(0).get(10)
    assert ord_gap is not None and ord_gap.exec_qty == 1.0 and ord_gap.exec_price == 90000.0
    assert int(ord_gap.exch_timestamp) == t0 + 2495 * ms
    gap_fill_rec = {
        "order_id": 10,
        "exch_ts_ns": int(ord_gap.exch_timestamp),
        "observed_ts_ns": int(hbt_tg.current_timestamp),
        "side": "buy",
        "price": float(ord_gap.exec_price),
        "qty": float(ord_gap.exec_qty),
    }

    # Post-recovery (t0 + 4500ms): submit & fill a normal verified order on B-line's fresh book (90050/90051)
    assert hbt_tg.elapse((t0 + 4500 * ms) - hbt_tg.current_timestamp) == 0
    assert hbt_tg.depth(0).best_bid == 90050.0 and hbt_tg.depth(0).best_ask == 90051.0
    hbt_tg.submit_buy_order(0, 20, 90051.0, 1.0, GTC, LIMIT, False)
    assert hbt_tg.elapse(100 * ms) == 0
    assert hbt_tg.position(0) == 2.0
    ord_ok = hbt_tg.orders(0).get(20)
    assert ord_ok is not None and ord_ok.exec_qty == 1.0 and ord_ok.exec_price == 90051.0
    assert int(ord_ok.exch_timestamp) >= t0 + 4500 * ms
    post_fill_rec = {
        "order_id": 20,
        "exch_ts_ns": int(ord_ok.exch_timestamp),
        "observed_ts_ns": int(hbt_tg.current_timestamp),
        "side": "buy",
        "price": float(ord_ok.exec_price),
        "qty": float(ord_ok.exec_qty),
    }

    # Explicit backtest fill accounting: raw fills retained (total=2), gap fill flagged unverified!
    audit = audit_fills_against_depth_unavailable_intervals(
        [gap_fill_rec, post_fill_rec],
        tg_row["depth_unavailable_intervals"],
    )
    assert audit["status"] == "execution_unverified", audit
    assert audit["policy"] == "retain_raw_fill_and_flag_unverified"
    assert audit["total_fills"] == 2
    assert audit["unverified_gap_fills"] == 1
    assert audit["verified_fills"] == 1
    assert audit["unverified_gap_fill_qty"] == 1.0
    assert audit["items"][0]["execution_status"] == "depth_unavailable_unverified"
    assert audit["items"][0]["queue_assumption"] == "UNOBSERVABLE_DEPTH_GAP"
    assert audit["items"][1]["execution_status"] == "verified"
    assert audit["items"][1]["queue_assumption"] == "OBSERVABLE_BOOK"

    # Edge-sample coverage also labels the gap fill DEPTH_UNAVAILABLE and post-recovery fill VISIBLE_WITHIN_50!
    edges_npz = np.load(tg_row["files"]["follower_l2_edges"])
    edge_samples = [
        (
            int(edges_npz["local_ts_ns"][i]),
            float(edges_npz["best_bid"][i]),
            float(edges_npz["edge_bid_50"][i]),
            float(edges_npz["best_ask"][i]),
            float(edges_npz["edge_ask_50"][i]),
            int(edges_npz["visible_bid_levels"][i]),
            int(edges_npz["visible_ask_levels"][i]),
        )
        for i in range(len(edges_npz["local_ts_ns"]))
    ]
    cov = evaluate_50_level_coverage(
        [
            {"ts_ns": gap_fill_rec["observed_ts_ns"], "side": "buy", "price": 90000.0},
            {"ts_ns": post_fill_rec["observed_ts_ns"], "side": "buy", "price": 90051.0},
        ],
        edge_samples,
    )
    assert cov["depth_unavailable"] == 1, cov
    assert cov["visible_within_50"] == 1, cov
    assert cov["items"][0]["depth_coverage"] == "DEPTH_UNAVAILABLE"
    assert cov["items"][0]["queue_assumption"] == "UNOBSERVABLE_DEPTH_GAP"
    assert cov["items"][1]["depth_coverage"] == "VISIBLE_WITHIN_50"



