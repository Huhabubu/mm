use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{accept_async, accept_hdr_async};

use tokyo_l2_collector::causal_selector::{CausalSelector, SelectorAction};
use tokyo_l2_collector::config::CollectorConfig;
use tokyo_l2_collector::live::run_live_collector;
use tokyo_l2_collector::storage::read_tsv_zst_records;
use tokyo_l2_collector::types::{CanonicalRecord, ConnId, ConnState, EndpointKind, ExchangeId, RawFrame, RecordKind};

const GATE_SIX_SYMS: [&str; 6] = [
    "BTC_USDT",
    "ETH_USDT",
    "SOL_USDT",
    "XRP_USDT",
    "DOGE_USDT",
    "SUI_USDT",
];

const BINANCE_SIX_SYMS: [&str; 6] = [
    "BTCUSDT",
    "ETHUSDT",
    "XRPUSDT",
    "SOLUSDT",
    "DOGEUSDT",
    "SUIUSDT",
];

fn px_tick(px: &str) -> i64 {
    let val: f64 = px.parse().expect("valid decimal price");
    (val * 10_000.0).round() as i64
}

#[derive(Default, Clone)]
struct SimpleBook {
    bids: BTreeMap<i64, f64>,
    asks: BTreeMap<i64, f64>,
}

impl SimpleBook {
    fn reset_str_levels(&mut self, bids: &[Value], asks: &[Value]) {
        self.bids.clear();
        self.asks.clear();
        self.apply_str_levels(bids, asks);
    }

    fn apply_str_levels(&mut self, bids: &[Value], asks: &[Value]) {
        for lvl in bids {
            let arr = lvl.as_array().expect("bid level array");
            let px = px_tick(arr[0].as_str().expect("bid px str"));
            let sz: f64 = arr[1].as_str().expect("bid sz str").parse().unwrap();
            if sz <= 0.0 {
                self.bids.remove(&px);
            } else {
                self.bids.insert(px, sz);
            }
        }
        for lvl in asks {
            let arr = lvl.as_array().expect("ask level array");
            let px = px_tick(arr[0].as_str().expect("ask px str"));
            let sz: f64 = arr[1].as_str().expect("ask sz str").parse().unwrap();
            if sz <= 0.0 {
                self.asks.remove(&px);
            } else {
                self.asks.insert(px, sz);
            }
        }
    }

    fn assert_non_crossed(&self, context: &str) {
        if let (Some((&bb, _)), Some((&ba, _))) = (self.bids.iter().next_back(), self.asks.iter().next()) {
            assert!(
                bb < ba,
                "{context}: crossed or locked book best_bid={bb} >= best_ask={ba}"
            );
        }
    }
}

fn raw_frame(
    exchange: ExchangeId,
    endpoint: EndpointKind,
    conn_id: ConnId,
    epoch: u64,
    wall_ns: i64,
    mono_ns: u64,
    payload: String,
) -> RawFrame {
    RawFrame {
        recv_wall_ns: wall_ns,
        recv_mono_ns: mono_ns,
        exchange,
        endpoint,
        conn_id,
        epoch,
        payload,
    }
}

fn collect_archive_records(data_root: &std::path::Path, exchange_dir: &str) -> Vec<CanonicalRecord> {
    let base = data_root.join("raw").join(exchange_dir);
    let mut files = Vec::new();
    for day_entry in std::fs::read_dir(&base).unwrap() {
        let day_dir = day_entry.unwrap().path();
        if day_dir.is_dir() {
            for f in std::fs::read_dir(day_dir).unwrap() {
                let p = f.unwrap().path();
                if p.extension().and_then(|e| e.to_str()) == Some("zst") {
                    files.push(p);
                }
            }
        }
    }
    files.sort();
    assert!(!files.is_empty(), "expected sealed .tsv.zst file under {}", base.display());
    let mut out = Vec::new();
    for path in files {
        for (rec, _) in read_tsv_zst_records(&path).unwrap() {
            out.push(rec);
        }
    }
    out
}

fn meta_reset_symbols(payload: &Value) -> Option<Option<String>> {
    let ev = payload.get("event").and_then(Value::as_str)?;
    if matches!(ev, "LOCAL_LOSS" | "GAP_START" | "DEPTH_UNAVAILABLE" | "SWITCH") {
        let sym = payload.get("symbol").and_then(Value::as_str).map(|s| s.to_uppercase());
        if matches!(sym.as_deref(), None | Some("ALL") | Some("")) {
            return Some(None);
        }
        return Some(sym);
    }
    None
}

#[derive(Default)]
struct ArchiveAuditStats {
    depth_snapshots: HashMap<String, usize>,
    depth_deltas: HashMap<String, usize>,
    bbo_frames: HashMap<String, usize>,
    trade_frames: HashMap<String, usize>,
    final_books: HashMap<String, SimpleBook>,
    meta_events: Vec<Value>,
}

fn verify_gate_records_strict(records: &[CanonicalRecord], expected_syms: &[&str]) -> ArchiveAuditStats {
    #[derive(Clone)]
    struct SymChain {
        conn_id: ConnId,
        epoch: u64,
        prev_u: u64,
        book: SimpleBook,
    }
    let mut chains: HashMap<String, SymChain> = HashMap::new();
    let mut stats = ArchiveAuditStats::default();

    for rec in records {
        let v: Value = serde_json::from_str(&rec.payload).expect("valid JSON payload");
        if rec.kind == RecordKind::Meta {
            if let Some(scope) = meta_reset_symbols(&v) {
                match scope {
                    None => chains.clear(),
                    Some(s) => {
                        chains.remove(&s);
                    }
                }
            }
            stats.meta_events.push(v);
            continue;
        }
        let channel = v.get("channel").and_then(Value::as_str).unwrap_or("");
        match channel {
            "futures.book_ticker" => {
                let sym = v["result"]["s"].as_str().unwrap().to_uppercase();
                *stats.bbo_frames.entry(sym).or_default() += 1;
            }
            "futures.trades" => {
                if let Some(arr) = v["result"].as_array() {
                    if let Some(sym) = arr.first().and_then(|x| x["contract"].as_str()) {
                        *stats.trade_frames.entry(sym.to_uppercase()).or_default() += 1;
                    }
                }
            }
            "futures.obu" => {
                let r = &v["result"];
                let s_name = r["s"].as_str().unwrap();
                let sym = s_name.split('.').nth(1).unwrap().to_uppercase();
                let is_full = r.get("full").and_then(Value::as_bool).unwrap_or(false);
                let u = r["u"].as_u64().unwrap();
                let bids = r.get("b").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]);
                let asks = r.get("a").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]);
                if is_full {
                    let mut book = SimpleBook::default();
                    book.reset_str_levels(bids, asks);
                    book.assert_non_crossed(&format!("gate {sym} snapshot u={u}"));
                    chains.insert(
                        sym.clone(),
                        SymChain {
                            conn_id: rec.conn_id,
                            epoch: rec.epoch,
                            prev_u: u,
                            book: book.clone(),
                        },
                    );
                    stats.final_books.insert(sym.clone(), book);
                    *stats.depth_snapshots.entry(sym).or_default() += 1;
                } else {
                    let first_u = r["U"].as_u64().expect("gate delta must carry U");
                    let chain = chains.get_mut(&sym).unwrap_or_else(|| {
                        panic!(
                            "missing_same_connection_anchor for Gate {sym} delta conn={:?} epoch={} U={first_u} u={u}",
                            rec.conn_id, rec.epoch
                        )
                    });
                    assert_eq!(
                        (chain.conn_id, chain.epoch),
                        (rec.conn_id, rec.epoch),
                        "cross-connection Gate delta splice on {sym}: chain=({:?},{}) delta=({:?},{})",
                        chain.conn_id, chain.epoch, rec.conn_id, rec.epoch
                    );
                    assert_eq!(
                        first_u,
                        chain.prev_u + 1,
                        "Gate U chain break on {sym}: U={first_u} prev_u={}",
                        chain.prev_u
                    );
                    assert!(u >= first_u, "Gate u < U on {sym}");
                    chain.prev_u = u;
                    chain.book.apply_str_levels(bids, asks);
                    chain.book.assert_non_crossed(&format!("gate {sym} delta u={u}"));
                    stats.final_books.insert(sym.clone(), chain.book.clone());
                    *stats.depth_deltas.entry(sym).or_default() += 1;
                }
            }
            _ => {}
        }
    }

    for &sym in expected_syms {
        assert!(
            *stats.depth_snapshots.get(sym).unwrap_or(&0) >= 1,
            "missing Gate snapshot for {sym}"
        );
        assert!(
            *stats.depth_deltas.get(sym).unwrap_or(&0) >= 1,
            "missing Gate delta for {sym}"
        );
        let book = stats.final_books.get(sym).expect("final book missing");
        assert!(
            !book.bids.is_empty() && !book.asks.is_empty(),
            "empty Gate side on {sym}"
        );
    }
    stats
}

fn verify_bybit_records_strict(records: &[CanonicalRecord], expected_syms: &[&str]) -> ArchiveAuditStats {
    #[derive(Clone)]
    struct SymChain {
        conn_id: ConnId,
        epoch: u64,
        prev_u: u64,
        book: SimpleBook,
    }
    let mut chains: HashMap<String, SymChain> = HashMap::new();
    let mut stats = ArchiveAuditStats::default();

    for rec in records {
        let v: Value = serde_json::from_str(&rec.payload).unwrap();
        if rec.kind == RecordKind::Meta {
            if let Some(scope) = meta_reset_symbols(&v) {
                match scope {
                    None => chains.clear(),
                    Some(s) => {
                        chains.remove(&s);
                    }
                }
            }
            stats.meta_events.push(v);
            continue;
        }
        let topic = v.get("topic").and_then(Value::as_str).unwrap_or("");
        if let Some(sym) = topic.strip_prefix("orderbook.1.") {
            *stats.bbo_frames.entry(sym.to_uppercase()).or_default() += 1;
        } else if let Some(sym) = topic.strip_prefix("publicTrade.") {
            *stats.trade_frames.entry(sym.to_uppercase()).or_default() += 1;
        } else if let Some(sym_raw) = topic.strip_prefix("orderbook.50.") {
            let sym = sym_raw.to_uppercase();
            let msg_type = v["type"].as_str().unwrap();
            let d = &v["data"];
            let u = d["u"].as_u64().unwrap();
            let bids = d.get("b").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]);
            let asks = d.get("a").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]);
            if msg_type == "snapshot" || u == 1 {
                let mut book = SimpleBook::default();
                book.reset_str_levels(bids, asks);
                book.assert_non_crossed(&format!("bybit {sym} snapshot u={u}"));
                chains.insert(
                    sym.clone(),
                    SymChain {
                        conn_id: rec.conn_id,
                        epoch: rec.epoch,
                        prev_u: u,
                        book: book.clone(),
                    },
                );
                stats.final_books.insert(sym.clone(), book);
                *stats.depth_snapshots.entry(sym).or_default() += 1;
            } else {
                let chain = chains.get_mut(&sym).unwrap_or_else(|| {
                    panic!(
                        "missing_same_connection_anchor for Bybit {sym} delta conn={:?} epoch={} u={u}",
                        rec.conn_id, rec.epoch
                    )
                });
                assert_eq!((chain.conn_id, chain.epoch), (rec.conn_id, rec.epoch));
                assert!(u >= chain.prev_u, "Bybit u regression on {sym}: {u} < {}", chain.prev_u);
                chain.prev_u = u;
                chain.book.apply_str_levels(bids, asks);
                chain.book.assert_non_crossed(&format!("bybit {sym} delta u={u}"));
                stats.final_books.insert(sym.clone(), chain.book.clone());
                *stats.depth_deltas.entry(sym).or_default() += 1;
            }
        }
    }

    for &sym in expected_syms {
        assert!(*stats.depth_snapshots.get(sym).unwrap_or(&0) >= 1, "missing Bybit snapshot for {sym}");
        assert!(*stats.depth_deltas.get(sym).unwrap_or(&0) >= 1, "missing Bybit delta for {sym}");
    }
    stats
}

fn verify_bitget_records_strict(records: &[CanonicalRecord], expected_syms: &[&str], require_updates: bool) -> ArchiveAuditStats {
    #[derive(Clone)]
    struct SymChain {
        conn_id: ConnId,
        epoch: u64,
        prev_seq: u64,
        awaiting_first_update: bool,
        book: SimpleBook,
    }
    let mut chains: HashMap<String, SymChain> = HashMap::new();
    let mut stats = ArchiveAuditStats::default();

    for rec in records {
        let v: Value = serde_json::from_str(&rec.payload).unwrap();
        if rec.kind == RecordKind::Meta {
            if let Some(scope) = meta_reset_symbols(&v) {
                match scope {
                    None => chains.clear(),
                    Some(s) => {
                        chains.remove(&s);
                    }
                }
            }
            stats.meta_events.push(v);
            continue;
        }
        let topic = v.pointer("/arg/topic").and_then(Value::as_str).unwrap_or("");
        let sym = v.pointer("/arg/symbol").and_then(Value::as_str).unwrap_or("").to_uppercase();
        if topic == "books1" {
            *stats.bbo_frames.entry(sym).or_default() += 1;
        } else if topic == "publicTrade" {
            *stats.trade_frames.entry(sym).or_default() += 1;
        } else if topic == "books50" {
            let action = v["action"].as_str().unwrap();
            let item = &v["data"][0];
            let seq = item["seq"].as_u64().unwrap();
            let bids = item.get("b").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]);
            let asks = item.get("a").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]);
            if action == "snapshot" {
                let mut book = SimpleBook::default();
                book.reset_str_levels(bids, asks);
                book.assert_non_crossed(&format!("bitget {sym} snapshot seq={seq}"));
                chains.insert(
                    sym.clone(),
                    SymChain {
                        conn_id: rec.conn_id,
                        epoch: rec.epoch,
                        prev_seq: seq,
                        awaiting_first_update: true,
                        book: book.clone(),
                    },
                );
                stats.final_books.insert(sym.clone(), book);
                *stats.depth_snapshots.entry(sym).or_default() += 1;
            } else {
                let pseq = item["pseq"].as_u64().expect("bitget update must have pseq");
                assert!(pseq < seq, "Bitget invariant pseq < seq violated on {sym}: pseq={pseq} seq={seq}");
                let chain = chains.get_mut(&sym).unwrap_or_else(|| {
                    panic!(
                        "missing_same_connection_anchor for Bitget {sym} update conn={:?} epoch={} pseq={pseq} seq={seq}",
                        rec.conn_id, rec.epoch
                    )
                });
                assert_eq!((chain.conn_id, chain.epoch), (rec.conn_id, rec.epoch));
                if chain.awaiting_first_update {
                    assert!(
                        pseq <= chain.prev_seq && chain.prev_seq <= seq,
                        "Bitget first update bridge failed on {sym}: pseq={pseq} prev={} seq={seq}",
                        chain.prev_seq
                    );
                    chain.awaiting_first_update = false;
                } else {
                    assert_eq!(
                        pseq, chain.prev_seq,
                        "Bitget update pseq break on {sym}: pseq={pseq} prev={}",
                        chain.prev_seq
                    );
                }
                chain.prev_seq = seq;
                chain.book.apply_str_levels(bids, asks);
                chain.book.assert_non_crossed(&format!("bitget {sym} update seq={seq}"));
                stats.final_books.insert(sym.clone(), chain.book.clone());
                *stats.depth_deltas.entry(sym).or_default() += 1;
            }
        }
    }

    for &sym in expected_syms {
        assert!(*stats.depth_snapshots.get(sym).unwrap_or(&0) >= 1, "missing Bitget snapshot for {sym}");
        if require_updates {
            assert!(*stats.depth_deltas.get(sym).unwrap_or(&0) >= 1, "missing Bitget update for {sym}");
        }
    }
    stats
}

fn verify_binance_records_strict(records: &[CanonicalRecord], expected_syms: &[&str]) -> ArchiveAuditStats {
    #[derive(Clone)]
    struct BinChain {
        conn_id: ConnId,
        epoch: u64,
        last_update_id: u64,
        awaiting_bridge: bool,
        prev_u: u64,
        book: SimpleBook,
    }
    fn apply_depth_delta(
        chains: &mut HashMap<String, BinChain>,
        stats: &mut ArchiveAuditStats,
        sym: &str,
        conn_id: ConnId,
        epoch: u64,
        d: &Value,
        is_bridged: bool,
    ) {
        let first_u = d["U"].as_u64().unwrap();
        let last_u = d["u"].as_u64().unwrap();
        let pu = d["pu"].as_u64().unwrap();
        let bids = d.get("b").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]);
        let asks = d.get("a").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]);
        let chain = chains.get_mut(sym).unwrap_or_else(|| {
            panic!(
                "missing_same_connection_anchor for Binance {sym} depth delta (bridged={is_bridged}) conn={conn_id:?} epoch={epoch} U={first_u} u={last_u}"
            )
        });
        assert_eq!((chain.conn_id, chain.epoch), (conn_id, epoch));
        if chain.awaiting_bridge {
            assert!(
                first_u <= chain.last_update_id && chain.last_update_id <= last_u,
                "Binance bridge invariant failed on {sym}: U={first_u} <= S={} <= u={last_u}",
                chain.last_update_id
            );
            chain.awaiting_bridge = false;
        } else {
            assert_eq!(pu, chain.prev_u, "Binance pu break on {sym}: pu={pu} prev_u={}", chain.prev_u);
        }
        chain.prev_u = last_u;
        chain.book.apply_str_levels(bids, asks);
        chain.book.assert_non_crossed(&format!("binance {sym} delta u={last_u}"));
        stats.final_books.insert(sym.to_string(), chain.book.clone());
        *stats.depth_deltas.entry(sym.to_string()).or_default() += 1;
    }

    let mut chains: HashMap<String, BinChain> = HashMap::new();
    let mut stats = ArchiveAuditStats::default();

    for rec in records {
        let v: Value = serde_json::from_str(&rec.payload).unwrap();
        if rec.kind == RecordKind::Meta {
            let ev = v.get("event").and_then(Value::as_str).unwrap_or("");
            if ev == "REST_SNAPSHOT_ANCHOR" {
                let sym = v["symbol"].as_str().unwrap().to_uppercase();
                let last_update_id = v["lastUpdateId"].as_u64().unwrap();
                let bids = v.pointer("/snapshot/bids").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]);
                let asks = v.pointer("/snapshot/asks").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]);
                let mut book = SimpleBook::default();
                book.reset_str_levels(bids, asks);
                book.assert_non_crossed(&format!("binance {sym} rest snapshot {last_update_id}"));
                chains.insert(
                    sym.clone(),
                    BinChain {
                        conn_id: rec.conn_id,
                        epoch: rec.epoch,
                        last_update_id,
                        awaiting_bridge: true,
                        prev_u: last_update_id,
                        book: book.clone(),
                    },
                );
                stats.final_books.insert(sym.clone(), book);
                *stats.depth_snapshots.entry(sym).or_default() += 1;
            } else if ev == "DEPTH_BRIDGED_DELTA" {
                let sym = v["symbol"].as_str().unwrap().to_uppercase();
                let raw_str = v["raw"].as_str().unwrap();
                let raw_val: Value = serde_json::from_str(raw_str).unwrap();
                let data = raw_val.get("data").unwrap_or(&raw_val);
                apply_depth_delta(&mut chains, &mut stats, &sym, rec.conn_id, rec.epoch, data, true);
            } else if let Some(scope) = meta_reset_symbols(&v) {
                match scope {
                    None => chains.clear(),
                    Some(s) => {
                        chains.remove(&s);
                    }
                }
            }
            stats.meta_events.push(v);
            continue;
        }
        let stream = v.get("stream").and_then(Value::as_str).unwrap_or("");
        let data = v.get("data").unwrap_or(&v);
        if stream.ends_with("@bookTicker") {
            let sym = data["s"].as_str().unwrap().to_uppercase();
            *stats.bbo_frames.entry(sym).or_default() += 1;
        } else if stream.ends_with("@aggTrade") {
            let sym = data["s"].as_str().unwrap().to_uppercase();
            *stats.trade_frames.entry(sym).or_default() += 1;
        } else if stream.contains("@depth") {
            let sym = data["s"].as_str().unwrap().to_uppercase();
            apply_depth_delta(&mut chains, &mut stats, &sym, rec.conn_id, rec.epoch, data, false);
        }
    }

    for &sym in expected_syms {
        assert!(*stats.depth_snapshots.get(sym).unwrap_or(&0) >= 1, "missing Binance REST snapshot for {sym}");
        assert!(*stats.depth_deltas.get(sym).unwrap_or(&0) >= 1, "missing Binance delta for {sym}");
    }
    stats
}

fn gate_snapshot_frame(sym: &str, u: u64, base_px: f64) -> String {
    json!({
        "channel": "futures.obu",
        "event": "update",
        "result": {
            "t": 1796256000000i64 + u as i64,
            "full": true,
            "s": format!("ob.{sym}.50"),
            "u": u,
            "b": [[format!("{:.2}", base_px), "2.0"]],
            "a": [[format!("{:.2}", base_px + 1.0), "2.0"]]
        }
    })
    .to_string()
}

fn gate_delta_frame(sym: &str, first_u: u64, last_u: u64, base_px: f64, sz: &str) -> String {
    json!({
        "channel": "futures.obu",
        "event": "update",
        "result": {
            "t": 1796256000000i64 + last_u as i64,
            "full": false,
            "s": format!("ob.{sym}.50"),
            "U": first_u,
            "u": last_u,
            "b": [[format!("{:.2}", base_px), sz]],
            "a": []
        }
    })
    .to_string()
}

fn gate_bbo_frame(sym: &str, base_px: f64) -> String {
    json!({
        "channel": "futures.book_ticker",
        "event": "update",
        "result": {
            "s": sym,
            "b": format!("{:.2}", base_px),
            "B": "5.0",
            "a": format!("{:.2}", base_px + 1.0),
            "A": "4.0"
        }
    })
    .to_string()
}

fn gate_trade_frame(sym: &str, tid: u64, base_px: f64) -> String {
    json!({
        "channel": "futures.trades",
        "event": "update",
        "result": [{"contract": sym, "id": tid, "size": "1.0", "price": format!("{:.2}", base_px)}]
    })
    .to_string()
}

fn sym_base_px(sym: &str) -> f64 {
    if sym.starts_with("BTC") {
        90000.0
    } else if sym.starts_with("ETH") {
        3000.0
    } else if sym.starts_with("SOL") {
        200.0
    } else if sym.starts_with("XRP") {
        2.0
    } else if sym.starts_with("DOGE") {
        1.0
    } else {
        5.0
    }
}

#[test]
fn test_issue19_gate_6symbol_startup_promotion_causal_and_anti_pingpong() {
    let syms: Vec<String> = GATE_SIX_SYMS.iter().map(|s| s.to_string()).collect();
    let mut sel = CausalSelector::new(EndpointKind::GatePublic, syms, 3, 5);
    let mut archive: Vec<CanonicalRecord> = Vec::new();
    let mut wall = 1_796_256_000_000_000_000i64;
    let mut mono = 1_000_000u64;

    // A only gets partial snapshots so A stays WaitAnchor.
    for (idx, sym) in GATE_SIX_SYMS[..2].iter().enumerate() {
        wall += 1_000_000;
        mono += 1_000_000;
        let out = sel.on_frame(raw_frame(
            ExchangeId::GateFutures,
            EndpointKind::GatePublic,
            ConnId::A,
            1,
            wall,
            mono,
            gate_snapshot_frame(sym, 50 + idx as u64, sym_base_px(sym)),
        ));
        archive.extend(out.records);
    }

    // Reproduce trial15 bug: Standby B gets BTC..DOGE snapshots AND deltas first (all dropped while standby),
    // THEN SUI_USDT snapshot pushes B to ConnState::Live FIRST -> promotes A -> B!
    for sym in &GATE_SIX_SYMS[..5] {
        let px = sym_base_px(sym);
        for payload in [
            gate_snapshot_frame(sym, 100, px),
            gate_delta_frame(sym, 101, 101, px, "2.5"),
        ] {
            wall += 1_000_000;
            mono += 1_000_000;
            let out = sel.on_frame(raw_frame(
                ExchangeId::GateFutures,
                EndpointKind::GatePublic,
                ConnId::B,
                1,
                wall,
                mono,
                payload,
            ));
            assert!(
                out.records.is_empty(),
                "standby B frames before Live promotion must not write to archive"
            );
        }
    }

    wall += 1_000_000;
    mono += 1_000_000;
    let prom_out = sel.on_frame(raw_frame(
        ExchangeId::GateFutures,
        EndpointKind::GatePublic,
        ConnId::B,
        1,
        wall,
        mono,
        gate_snapshot_frame("SUI_USDT", 500, sym_base_px("SUI_USDT")),
    ));
    assert_eq!(sel.primary(), ConnId::B);
    assert!(prom_out
        .records
        .iter()
        .any(|r| r.payload.contains("\"SWITCH\"") && r.payload.contains("\"initial_or_standby_ready_first\"")));

    let resub_syms = prom_out
        .actions
        .iter()
        .find_map(|a| match a {
            SelectorAction::Resubscribe { conn_id, symbols, .. } if *conn_id == ConnId::B => {
                Some(symbols.clone())
            }
            _ => None,
        })
        .expect("expected Resubscribe action for 5 unanchored Gate symbols on B");
    assert_eq!(resub_syms.len(), 5);
    assert!(!resub_syms.contains(&"SUI_USDT".to_string()));
    archive.extend(prom_out.records);

    // Queue asynchronous refresh (simulating unsubscribe in flight).
    sel.mark_refresh_queued(ConnId::B, 1, 99, &resub_syms);

    // 1. Stale pre-subscribe full=true AND delta on BTC_USDT MUST be dropped while L1 BBO, trades, and SUI_USDT flow!
    for stale_payload in [
        gate_snapshot_frame("BTC_USDT", 102, sym_base_px("BTC_USDT")),
        gate_delta_frame("BTC_USDT", 102, 102, sym_base_px("BTC_USDT"), "3.1"),
    ] {
        wall += 1_000_000;
        mono += 1_000_000;
        let out = sel.on_frame(raw_frame(
            ExchangeId::GateFutures,
            EndpointKind::GatePublic,
            ConnId::B,
            1,
            wall,
            mono,
            stale_payload,
        ));
        assert!(out.records.is_empty(), "pre-subscribe stale depth must be fenced");
    }

    for live_payload in [
        gate_delta_frame("SUI_USDT", 501, 501, sym_base_px("SUI_USDT"), "4.0"),
        gate_bbo_frame("BTC_USDT", sym_base_px("BTC_USDT")),
        gate_trade_frame("BTC_USDT", 9001, sym_base_px("BTC_USDT")),
    ] {
        wall += 1_000_000;
        mono += 1_000_000;
        let out = sel.on_frame(raw_frame(
            ExchangeId::GateFutures,
            EndpointKind::GatePublic,
            ConnId::B,
            1,
            wall,
            mono,
            live_payload,
        ));
        assert_eq!(out.records.len(), 1, "SUI depth, L1 BBO, and trades must remain uninterrupted");
        archive.extend(out.records);
    }

    // 2. Anti-ping-pong: A finishing its remaining 4 symbols while B refreshes must NOT steal primary back!
    for (idx, sym) in GATE_SIX_SYMS[2..].iter().enumerate() {
        wall += 1_000_000;
        mono += 1_000_000;
        let out = sel.on_frame(raw_frame(
            ExchangeId::GateFutures,
            EndpointKind::GatePublic,
            ConnId::A,
            1,
            wall,
            mono,
            gate_snapshot_frame(sym, 60 + idx as u64, sym_base_px(sym)),
        ));
        assert_eq!(sel.primary(), ConnId::B, "A becoming Live must not ping-pong primary away from B");
        assert!(out.records.is_empty());
    }
    assert_eq!(sel.conn_state(ConnId::A), ConnState::Live);

    // 3. Re-subscribe ACK sent; stale deltas arriving BEFORE fresh full=true remain blocked,
    //    then fresh full=true + chained deltas recover all 5 symbols cleanly!
    sel.on_depth_refresh_subscribed(ConnId::B, 1, 99);
    wall += 1_000_000;
    mono += 1_000_000;
    let pre_snap_delta = sel.on_frame(raw_frame(
        ExchangeId::GateFutures,
        EndpointKind::GatePublic,
        ConnId::B,
        1,
        wall,
        mono,
        gate_delta_frame("ETH_USDT", 102, 102, sym_base_px("ETH_USDT"), "9.9"),
    ));
    assert!(
        !pre_snap_delta.records.iter().any(|r| r.kind == RecordKind::Data),
        "delta after subscribe before fresh snapshot must be blocked"
    );

    for sym in &resub_syms {
        let px = sym_base_px(sym);
        wall += 1_000_000;
        mono += 1_000_000;
        let snap_out = sel.on_frame(raw_frame(
            ExchangeId::GateFutures,
            EndpointKind::GatePublic,
            ConnId::B,
            1,
            wall,
            mono,
            gate_snapshot_frame(sym, 1000, px),
        ));
        assert!(snap_out.records.iter().any(|r| r.payload.contains("\"DEPTH_RECOVERED\"")));
        archive.extend(snap_out.records);

        wall += 1_000_000;
        mono += 1_000_000;
        let delta_out = sel.on_frame(raw_frame(
            ExchangeId::GateFutures,
            EndpointKind::GatePublic,
            ConnId::B,
            1,
            wall,
            mono,
            gate_delta_frame(sym, 1001, 1001, px, "3.3"),
        ));
        archive.extend(delta_out.records);
    }

    let audit = verify_gate_records_strict(&archive, &GATE_SIX_SYMS);
    assert_eq!(audit.bbo_frames.get("BTC_USDT").copied().unwrap_or(0), 1);
    assert_eq!(audit.trade_frames.get("BTC_USDT").copied().unwrap_or(0), 1);
}

#[test]
fn test_issue19_gate_failover_dual_gap_and_planned_rotation_all_six_symbols() {
    let syms: Vec<String> = GATE_SIX_SYMS.iter().map(|s| s.to_string()).collect();
    let mut sel = CausalSelector::new(EndpointKind::GatePublic, syms, 3, 5);
    let mut archive: Vec<CanonicalRecord> = Vec::new();
    let mut wall = 1_796_256_100_000_000_000i64;
    let mut mono = 1_000_000u64;

    // Make both A(epoch 1) and B(epoch 1) Live initially across all 6 symbols.
    for conn in [ConnId::A, ConnId::B] {
        for sym in &GATE_SIX_SYMS {
            wall += 1_000_000;
            mono += 1_000_000;
            let out = sel.on_frame(raw_frame(
                ExchangeId::GateFutures,
                EndpointKind::GatePublic,
                conn,
                1,
                wall,
                mono,
                gate_snapshot_frame(sym, 100, sym_base_px(sym)),
            ));
            archive.extend(out.records);
        }
    }
    assert_eq!(sel.primary(), ConnId::A);

    // 1. Failover (handle_primary_fault): inject sequence break on A -> SWITCH A -> B,
    //    forcing fresh Resubscribe across all 6 symbols on B.
    wall += 1_000_000;
    mono += 1_000_000;
    let fault_out = sel.on_frame(raw_frame(
        ExchangeId::GateFutures,
        EndpointKind::GatePublic,
        ConnId::A,
        1,
        wall,
        mono,
        gate_delta_frame("BTC_USDT", 999, 999, sym_base_px("BTC_USDT"), "1.0"),
    ));
    assert_eq!(sel.primary(), ConnId::B);
    let failover_syms = fault_out
        .actions
        .iter()
        .find_map(|a| match a {
            SelectorAction::Resubscribe { conn_id, symbols, .. } if *conn_id == ConnId::B => {
                Some(symbols.clone())
            }
            _ => None,
        })
        .expect("failover must trigger depth Resubscribe on B");
    assert_eq!(failover_syms.len(), 6);
    archive.extend(fault_out.records);

    sel.mark_refresh_queued(ConnId::B, 1, 10, &failover_syms);
    sel.on_depth_refresh_subscribed(ConnId::B, 1, 10);
    for sym in &GATE_SIX_SYMS {
        let px = sym_base_px(sym);
        for p in [gate_snapshot_frame(sym, 200, px), gate_delta_frame(sym, 201, 201, px, "2.2")] {
            wall += 1_000_000;
            mono += 1_000_000;
            archive.extend(
                sel.on_frame(raw_frame(
                    ExchangeId::GateFutures,
                    EndpointKind::GatePublic,
                    ConnId::B,
                    1,
                    wall,
                    mono,
                    p,
                ))
                .records,
            );
        }
    }

    // 2. Dual-gap recovery: disconnect both A & B -> GAP_START -> A reconnects epoch 2 with 5 symbols before SUI_USDT.
    wall += 1_000_000;
    mono += 1_000_000;
    archive.extend(sel.on_disconnect(ConnId::A, wall, mono, "net_a").records);
    wall += 1_000_000;
    mono += 1_000_000;
    archive.extend(sel.on_disconnect(ConnId::B, wall, mono, "net_b").records);
    assert!(sel.in_gap());

    sel.on_reconnect(ConnId::A, 2, mono + 500_000);
    for sym in &GATE_SIX_SYMS[..5] {
        wall += 1_000_000;
        mono += 1_000_000;
        let out = sel.on_frame(raw_frame(
            ExchangeId::GateFutures,
            EndpointKind::GatePublic,
            ConnId::A,
            2,
            wall,
            mono,
            gate_snapshot_frame(sym, 300, sym_base_px(sym)),
        ));
        assert!(out.records.is_empty());
    }
    wall += 1_000_000;
    mono += 1_000_000;
    let gap_end = sel.on_frame(raw_frame(
        ExchangeId::GateFutures,
        EndpointKind::GatePublic,
        ConnId::A,
        2,
        wall,
        mono,
        gate_snapshot_frame("SUI_USDT", 300, sym_base_px("SUI_USDT")),
    ));
    assert!(!sel.in_gap());
    assert_eq!(sel.primary(), ConnId::A);
    let gap_syms = gap_end
        .actions
        .iter()
        .find_map(|a| match a {
            SelectorAction::Resubscribe { conn_id, symbols, .. } if *conn_id == ConnId::A => {
                Some(symbols.clone())
            }
            _ => None,
        })
        .expect("GAP_END must refresh 5 unanchored Gate symbols on A");
    assert_eq!(gap_syms.len(), 5);
    archive.extend(gap_end.records);

    sel.mark_refresh_queued(ConnId::A, 2, 11, &gap_syms);
    sel.on_depth_refresh_subscribed(ConnId::A, 2, 11);
    for sym in &gap_syms {
        let px = sym_base_px(sym);
        for p in [gate_snapshot_frame(sym, 400, px), gate_delta_frame(sym, 401, 401, px, "2.8")] {
            wall += 1_000_000;
            mono += 1_000_000;
            archive.extend(
                sel.on_frame(raw_frame(
                    ExchangeId::GateFutures,
                    EndpointKind::GatePublic,
                    ConnId::A,
                    2,
                    wall,
                    mono,
                    p,
                ))
                .records,
            );
        }
    }

    // Re-connect B on epoch 2 and make it Live standby before planned rotation.
    sel.on_reconnect(ConnId::B, 2, mono + 500_000);
    for sym in &GATE_SIX_SYMS {
        wall += 1_000_000;
        mono += 1_000_000;
        sel.on_frame(raw_frame(
            ExchangeId::GateFutures,
            EndpointKind::GatePublic,
            ConnId::B,
            2,
            wall,
            mono,
            gate_snapshot_frame(sym, 450, sym_base_px(sym)),
        ));
    }

    // 3. Planned rotation: rotate A while B is Live -> SWITCH A -> B, planned=true DEPTH_UNAVAILABLE,
    //    and A reconnects epoch 3 Live BEFORE B finishes refreshing -> anti-ping-pong keeps B primary!
    wall += 1_000_000;
    mono += 1_000_000;
    let rot_out = sel.on_planned_rotation(ConnId::A, wall, mono);
    assert_eq!(sel.primary(), ConnId::B);
    assert!(rot_out.records.iter().any(|r| {
        r.payload.contains("\"DEPTH_UNAVAILABLE\"")
            && r.payload.contains("\"planned\":true")
            && r.payload.contains("\"planned_rotation\"")
    }));
    let rot_syms = rot_out
        .actions
        .iter()
        .find_map(|a| match a {
            SelectorAction::Resubscribe { conn_id, symbols, .. } if *conn_id == ConnId::B => {
                Some(symbols.clone())
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(rot_syms.len(), 6);
    archive.extend(rot_out.records);

    sel.mark_refresh_queued(ConnId::B, 2, 12, &rot_syms);
    // A reconnects epoch 3 and turns Live while B is refreshing.
    sel.on_reconnect(ConnId::A, 3, mono + 100_000);
    for sym in &GATE_SIX_SYMS {
        wall += 1_000_000;
        mono += 1_000_000;
        sel.on_frame(raw_frame(
            ExchangeId::GateFutures,
            EndpointKind::GatePublic,
            ConnId::A,
            3,
            wall,
            mono,
            gate_snapshot_frame(sym, 500, sym_base_px(sym)),
        ));
    }
    assert_eq!(sel.primary(), ConnId::B, "planned rotation must not ping-pong back to A");

    sel.on_depth_refresh_subscribed(ConnId::B, 2, 12);
    for sym in &GATE_SIX_SYMS {
        let px = sym_base_px(sym);
        for p in [gate_snapshot_frame(sym, 600, px), gate_delta_frame(sym, 601, 601, px, "3.7")] {
            wall += 1_000_000;
            mono += 1_000_000;
            archive.extend(
                sel.on_frame(raw_frame(
                    ExchangeId::GateFutures,
                    EndpointKind::GatePublic,
                    ConnId::B,
                    2,
                    wall,
                    mono,
                    p,
                ))
                .records,
            );
        }
    }

    let audit = verify_gate_records_strict(&archive, &GATE_SIX_SYMS);
    let recovered_cnt = audit
        .meta_events
        .iter()
        .filter(|m| m["event"] == "DEPTH_RECOVERED")
        .count();
    assert!(recovered_cnt >= 17, "expected >=17 Gate DEPTH_RECOVERED events (6 failover + 5 gap + 6 planned), got {recovered_cnt}");
}

#[test]
fn test_issue19_bybit_orderbook50_promotion_failover_and_u1_reset() {
    let syms = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()];
    let mut sel = CausalSelector::new(EndpointKind::BybitPublic, syms, 3, 5);
    let mut archive: Vec<CanonicalRecord> = Vec::new();
    let mut wall = 1_796_256_200_000_000_000i64;
    let mut mono = 1_000_000u64;

    let bybit_snap = |sym: &str, u: u64, px: f64| {
        json!({
            "topic": format!("orderbook.50.{sym}"),
            "type": "snapshot",
            "ts": 1796256200000i64 + u as i64,
            "data": {"s": sym, "u": u, "seq": u * 10, "b": [[format!("{px:.1}"), "2.0"]], "a": [[format!("{:.1}", px + 1.0), "2.0"]]}
        })
        .to_string()
    };
    let bybit_delta = |sym: &str, u: u64, px: f64, sz: &str| {
        json!({
            "topic": format!("orderbook.50.{sym}"),
            "type": "delta",
            "ts": 1796256200000i64 + u as i64,
            "data": {"s": sym, "u": u, "seq": u * 10, "b": [[format!("{px:.1}"), sz]], "a": []}
        })
        .to_string()
    };
    let bybit_bbo = |sym: &str, u: u64, px: f64| {
        json!({
            "topic": format!("orderbook.1.{sym}"),
            "type": "snapshot",
            "ts": 1796256200000i64,
            "data": {"s": sym, "u": u, "seq": u, "b": [[format!("{px:.1}"), "1.0"]], "a": [[format!("{:.1}", px + 1.0), "1.0"]]}
        })
        .to_string()
    };
    let bybit_trade = |sym: &str, tid: &str, px: f64| {
        json!({
            "topic": format!("publicTrade.{sym}"),
            "type": "snapshot",
            "ts": 1796256200000i64,
            "data": [{"T": 1796256200000i64, "s": sym, "S": "Buy", "v": "0.1", "p": format!("{px:.1}"), "i": tid}]
        })
        .to_string()
    };

    // Standby B gets BTCUSDT first (dropped), then ETHUSDT pushes B Live first -> promotes A -> B.
    for payload in [
        bybit_snap("BTCUSDT", 50, 90000.0),
        bybit_delta("BTCUSDT", 55, 90000.0, "2.5"),
    ] {
        wall += 1_000_000;
        mono += 1_000_000;
        assert!(sel
            .on_frame(raw_frame(
                ExchangeId::BybitLinear,
                EndpointKind::BybitPublic,
                ConnId::B,
                1,
                wall,
                mono,
                payload,
            ))
            .records
            .is_empty());
    }
    wall += 1_000_000;
    mono += 1_000_000;
    let prom = sel.on_frame(raw_frame(
        ExchangeId::BybitLinear,
        EndpointKind::BybitPublic,
        ConnId::B,
        1,
        wall,
        mono,
        bybit_snap("ETHUSDT", 60, 3000.0),
    ));
    assert_eq!(sel.primary(), ConnId::B);
    archive.extend(prom.records);

    sel.mark_refresh_queued(ConnId::B, 1, 20, &["BTCUSDT".to_string()]);
    // Stale pre-subscribe delta dropped while L1 and publicTrade keep flowing!
    wall += 1_000_000;
    mono += 1_000_000;
    assert!(sel
        .on_frame(raw_frame(
            ExchangeId::BybitLinear,
            EndpointKind::BybitPublic,
            ConnId::B,
            1,
            wall,
            mono,
            bybit_delta("BTCUSDT", 58, 90000.0, "3.0"),
        ))
        .records
        .is_empty());
    for ok_p in [bybit_bbo("BTCUSDT", 10, 90000.0), bybit_trade("BTCUSDT", "t-1", 90000.5)] {
        wall += 1_000_000;
        mono += 1_000_000;
        let o = sel.on_frame(raw_frame(
            ExchangeId::BybitLinear,
            EndpointKind::BybitPublic,
            ConnId::B,
            1,
            wall,
            mono,
            ok_p,
        ));
        assert_eq!(o.records.len(), 1);
        archive.extend(o.records);
    }

    // Bybit server restart (type=delta, u=1) resets anchor cleanly & recovers DEPTH_RECOVERED!
    sel.on_depth_refresh_subscribed(ConnId::B, 1, 20);
    wall += 1_000_000;
    mono += 1_000_000;
    let u1_reset = sel.on_frame(raw_frame(
        ExchangeId::BybitLinear,
        EndpointKind::BybitPublic,
        ConnId::B,
        1,
        wall,
        mono,
        bybit_delta("BTCUSDT", 1, 90000.0, "4.0"),
    ));
    assert!(u1_reset.records.iter().any(|r| r.payload.contains("\"DEPTH_RECOVERED\"")));
    archive.extend(u1_reset.records);
    // Bybit allows u jumps within aggregation!
    wall += 1_000_000;
    mono += 1_000_000;
    archive.extend(
        sel.on_frame(raw_frame(
            ExchangeId::BybitLinear,
            EndpointKind::BybitPublic,
            ConnId::B,
            1,
            wall,
            mono,
            bybit_delta("BTCUSDT", 25, 90000.0, "4.5"),
        ))
        .records,
    );
    wall += 1_000_000;
    mono += 1_000_000;
    archive.extend(
        sel.on_frame(raw_frame(
            ExchangeId::BybitLinear,
            EndpointKind::BybitPublic,
            ConnId::B,
            1,
            wall,
            mono,
            bybit_delta("ETHUSDT", 70, 3000.0, "3.1"),
        ))
        .records,
    );

    // Prepare standby A Live, then trigger fault on primary B (u=5 < prev_u=25) -> failover B -> A -> refresh both.
    for (s, px) in [("BTCUSDT", 90000.0), ("ETHUSDT", 3000.0)] {
        wall += 1_000_000;
        mono += 1_000_000;
        sel.on_frame(raw_frame(
            ExchangeId::BybitLinear,
            EndpointKind::BybitPublic,
            ConnId::A,
            1,
            wall,
            mono,
            bybit_snap(s, 100, px),
        ));
    }
    wall += 1_000_000;
    mono += 1_000_000;
    let fail = sel.on_frame(raw_frame(
        ExchangeId::BybitLinear,
        EndpointKind::BybitPublic,
        ConnId::B,
        1,
        wall,
        mono,
        bybit_delta("BTCUSDT", 5, 90000.0, "1.1"),
    ));
    assert_eq!(sel.primary(), ConnId::A);
    archive.extend(fail.records);

    sel.mark_refresh_queued(ConnId::A, 1, 21, &["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
    sel.on_depth_refresh_subscribed(ConnId::A, 1, 21);
    for (s, px) in [("BTCUSDT", 90000.0), ("ETHUSDT", 3000.0)] {
        for p in [bybit_snap(s, 200, px), bybit_delta(s, 220, px, "6.0")] {
            wall += 1_000_000;
            mono += 1_000_000;
            archive.extend(
                sel.on_frame(raw_frame(
                    ExchangeId::BybitLinear,
                    EndpointKind::BybitPublic,
                    ConnId::A,
                    1,
                    wall,
                    mono,
                    p,
                ))
                .records,
            );
        }
    }

    let audit = verify_bybit_records_strict(&archive, &["BTCUSDT", "ETHUSDT"]);
    assert_eq!(audit.bbo_frames.get("BTCUSDT").copied().unwrap_or(0), 1);
    assert_eq!(audit.trade_frames.get("BTCUSDT").copied().unwrap_or(0), 1);
}

#[test]
fn test_issue19_bitget_books50_snapshot_only_and_post_cutover_pseq_seq() {
    let syms = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()];
    let mut sel = CausalSelector::new(EndpointKind::BitgetPublic, syms, 3, 5);
    let mut archive: Vec<CanonicalRecord> = Vec::new();
    let mut wall = 1_796_256_300_000_000_000i64;
    let mut mono = 1_000_000u64;

    let bg_snap = |topic: &str, sym: &str, seq: u64, px: f64| {
        json!({
            "action": "snapshot",
            "arg": {"instType": "usdt-futures", "topic": topic, "symbol": sym},
            "data": [{"seq": seq, "pseq": 0, "b": [[format!("{px:.1}"), "2.0"]], "a": [[format!("{:.1}", px + 1.0), "2.0"]]}]
        })
        .to_string()
    };
    let bg_update = |sym: &str, pseq: u64, seq: u64, px: f64, sz: &str| {
        json!({
            "action": "update",
            "arg": {"instType": "usdt-futures", "topic": "books50", "symbol": sym},
            "data": [{"seq": seq, "pseq": pseq, "b": [[format!("{px:.1}"), sz]], "a": []}]
        })
        .to_string()
    };
    let bg_trade = |sym: &str, tid: &str, px: f64| {
        json!({
            "action": "snapshot",
            "arg": {"instType": "usdt-futures", "topic": "publicTrade", "symbol": sym},
            "data": [{"i": tid, "p": format!("{px:.1}"), "v": "0.1"}]
        })
        .to_string()
    };

    // Pre-cutover continuous snapshot-only mode on A & B.
    for conn in [ConnId::A, ConnId::B] {
        for (s, px) in [("BTCUSDT", 90000.0), ("ETHUSDT", 3000.0)] {
            wall += 1_000_000;
            mono += 1_000_000;
            archive.extend(
                sel.on_frame(raw_frame(
                    ExchangeId::BitgetV3,
                    EndpointKind::BitgetPublic,
                    conn,
                    1,
                    wall,
                    mono,
                    bg_snap("books50", s, 100, px),
                ))
                .records,
            );
        }
    }
    // Second snapshot frame in snapshot-only mode continues cleanly on primary A.
    wall += 1_000_000;
    mono += 1_000_000;
    archive.extend(
        sel.on_frame(raw_frame(
            ExchangeId::BitgetV3,
            EndpointKind::BitgetPublic,
            ConnId::A,
            1,
            wall,
            mono,
            bg_snap("books50", "BTCUSDT", 101, 90000.0),
        ))
        .records,
    );

    // Post-cutover incremental updates on A, then inject pseq >= seq violation -> failover A -> B!
    wall += 1_000_000;
    mono += 1_000_000;
    archive.extend(
        sel.on_frame(raw_frame(
            ExchangeId::BitgetV3,
            EndpointKind::BitgetPublic,
            ConnId::A,
            1,
            wall,
            mono,
            bg_update("BTCUSDT", 100, 105, 90000.0, "2.5"),
        ))
        .records,
    );
    wall += 1_000_000;
    mono += 1_000_000;
    let bad_pseq = sel.on_frame(raw_frame(
        ExchangeId::BitgetV3,
        EndpointKind::BitgetPublic,
        ConnId::A,
        1,
        wall,
        mono,
        bg_update("BTCUSDT", 106, 106, 90000.0, "1.0"),
    ));
    assert_eq!(sel.primary(), ConnId::B);
    archive.extend(bad_pseq.records);

    sel.mark_refresh_queued(ConnId::B, 1, 30, &["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
    // Stale pre-subscribe update fenced while books1 & publicTrade flow uninterrupted!
    wall += 1_000_000;
    mono += 1_000_000;
    assert!(sel
        .on_frame(raw_frame(
            ExchangeId::BitgetV3,
            EndpointKind::BitgetPublic,
            ConnId::B,
            1,
            wall,
            mono,
            bg_update("BTCUSDT", 100, 101, 90000.0, "8.8"),
        ))
        .records
        .is_empty());
    for ok_p in [bg_snap("books1", "BTCUSDT", 1, 90000.0), bg_trade("BTCUSDT", "bg-t1", 90000.2)] {
        wall += 1_000_000;
        mono += 1_000_000;
        let o = sel.on_frame(raw_frame(
            ExchangeId::BitgetV3,
            EndpointKind::BitgetPublic,
            ConnId::B,
            1,
            wall,
            mono,
            ok_p,
        ));
        assert_eq!(o.records.len(), 1);
        archive.extend(o.records);
    }

    sel.on_depth_refresh_subscribed(ConnId::B, 1, 30);
    for (s, px) in [("BTCUSDT", 90000.0), ("ETHUSDT", 3000.0)] {
        for p in [
            bg_snap("books50", s, 200, px),
            bg_update(s, 199, 205, px, "3.1"),
            bg_update(s, 205, 209, px, "3.4"),
        ] {
            wall += 1_000_000;
            mono += 1_000_000;
            archive.extend(
                sel.on_frame(raw_frame(
                    ExchangeId::BitgetV3,
                    EndpointKind::BitgetPublic,
                    ConnId::B,
                    1,
                    wall,
                    mono,
                    p,
                ))
                .records,
            );
        }
    }

    let audit = verify_bitget_records_strict(&archive, &["BTCUSDT", "ETHUSDT"], true);
    assert_eq!(audit.bbo_frames.get("BTCUSDT").copied().unwrap_or(0), 1);
    assert_eq!(audit.trade_frames.get("BTCUSDT").copied().unwrap_or(0), 1);
}

#[test]
fn test_issue19_binance_failover_and_multi_symbol_gap_recovery() {
    let syms: Vec<String> = BINANCE_SIX_SYMS.iter().map(|s| s.to_string()).collect();
    let mut sel = CausalSelector::new(EndpointKind::BinancePublic, syms, 3, 5);
    let mut archive: Vec<CanonicalRecord> = Vec::new();
    let mut wall = 1_796_256_400_000_000_000i64;
    let mut mono = 1_000_000u64;

    let bin_delta = |sym: &str, fu: u64, lu: u64, pu: u64, px: f64, sz: &str| {
        json!({
            "stream": format!("{}@depth@100ms", sym.to_lowercase()),
            "data": {"e": "depthUpdate", "s": sym, "U": fu, "u": lu, "pu": pu, "b": [[format!("{px:.2}"), sz]], "a": []}
        })
        .to_string()
    };
    let bin_rest = |last_u: u64, px: f64| {
        json!({
            "lastUpdateId": last_u,
            "bids": [[format!("{px:.2}"), "5.0"]],
            "asks": [[format!("{:.2}", px + 1.0), "5.0"]]
        })
        .to_string()
    };
    let bin_bbo = |sym: &str, px: f64| {
        json!({
            "stream": format!("{}@bookTicker", sym.to_lowercase()),
            "data": {"e": "bookTicker", "u": 1, "s": sym, "b": format!("{px:.2}"), "B": "1.0", "a": format!("{:.2}", px + 1.0), "A": "1.0"}
        })
        .to_string()
    };

    // Anchor both A(epoch 1) and B(epoch 1) across all 6 symbols.
    for conn in [ConnId::A, ConnId::B] {
        for sym in &BINANCE_SIX_SYMS {
            let px = sym_base_px(sym);
            wall += 1_000_000;
            mono += 1_000_000;
            sel.on_frame(raw_frame(
                ExchangeId::BinanceFutures,
                EndpointKind::BinancePublic,
                conn,
                1,
                wall,
                mono,
                bin_delta(sym, 100, 104, 99, px, "6.0"),
            ));
            wall += 1_000_000;
            mono += 1_000_000;
            archive.extend(
                sel.on_rest_snapshot(conn, sym, 102, &bin_rest(102, px), wall, mono)
                    .records,
            );
        }
    }
    assert_eq!(sel.primary(), ConnId::A);

    // Primary A disconnects -> failover SWITCH A -> B triggers FetchRestSnapshot for all 6 symbols on B!
    wall += 1_000_000;
    mono += 1_000_000;
    let fail_out = sel.on_disconnect(ConnId::A, wall, mono, "primary_drop");
    assert_eq!(sel.primary(), ConnId::B);
    let rest_reqs: HashSet<String> = fail_out
        .actions
        .iter()
        .filter_map(|a| match a {
            SelectorAction::FetchRestSnapshot { conn_id: ConnId::B, symbol } => Some(symbol.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(rest_reqs.len(), 6);
    archive.extend(fail_out.records);

    // During depth refresh wait on B: buffered deltas are buffered cleanly (not leaked), while bookTicker flows!
    for sym in &BINANCE_SIX_SYMS {
        let px = sym_base_px(sym);
        wall += 1_000_000;
        mono += 1_000_000;
        let bbo_out = sel.on_frame(raw_frame(
            ExchangeId::BinanceFutures,
            EndpointKind::BinancePublic,
            ConnId::B,
            1,
            wall,
            mono,
            bin_bbo(sym, px),
        ));
        assert_eq!(bbo_out.records.len(), 1);
        archive.extend(bbo_out.records);

        wall += 1_000_000;
        mono += 1_000_000;
        let buf_d = sel.on_frame(raw_frame(
            ExchangeId::BinanceFutures,
            EndpointKind::BinancePublic,
            ConnId::B,
            1,
            wall,
            mono,
            bin_delta(sym, 105, 108, 104, px, "7.0"),
        ));
        assert!(
            buf_d.records.is_empty(),
            "Binance deltas before fresh failover REST snapshot must buffer cleanly"
        );
    }

    for sym in &BINANCE_SIX_SYMS {
        let px = sym_base_px(sym);
        wall += 1_000_000;
        mono += 1_000_000;
        let r_out = sel.on_rest_snapshot(ConnId::B, sym, 106, &bin_rest(106, px), wall, mono);
        assert!(r_out
            .records
            .iter()
            .any(|r| r.payload.contains("\"REST_SNAPSHOT_ANCHOR\"")
                && !r.payload.contains("\"unavailable_since_wall_ns\":null")));
        assert!(r_out.records.iter().any(|r| r.payload.contains("\"DEPTH_BRIDGED_DELTA\"")));
        archive.extend(r_out.records);
    }

    // Also verify dual gap recovery -> GAP_END + FetchRestSnapshot for all remaining symbols.
    wall += 1_000_000;
    mono += 1_000_000;
    archive.extend(sel.on_disconnect(ConnId::B, wall, mono, "b_drop").records);
    assert!(sel.in_gap());

    sel.on_reconnect(ConnId::A, 2, mono + 500_000);
    for sym in &BINANCE_SIX_SYMS {
        let px = sym_base_px(sym);
        wall += 1_000_000;
        mono += 1_000_000;
        sel.on_frame(raw_frame(
            ExchangeId::BinanceFutures,
            EndpointKind::BinancePublic,
            ConnId::A,
            2,
            wall,
            mono,
            bin_delta(sym, 200, 204, 199, px, "8.0"),
        ));
    }
    let mut post_gap_refresh_syms = Vec::new();
    for (idx, sym) in BINANCE_SIX_SYMS.iter().enumerate() {
        let px = sym_base_px(sym);
        wall += 1_000_000;
        mono += 1_000_000;
        let out = sel.on_rest_snapshot(ConnId::A, sym, 202, &bin_rest(202, px), wall, mono);
        if idx < BINANCE_SIX_SYMS.len() - 1 {
            assert!(sel.in_gap(), "GAP_END waits until all 6 symbols finish initial REST anchor");
        } else {
            assert!(!sel.in_gap());
            assert!(out.records.iter().any(|r| r.payload.contains("\"GAP_END\"")));
            for act in &out.actions {
                if let SelectorAction::FetchRestSnapshot { conn_id: ConnId::A, symbol } = act {
                    post_gap_refresh_syms.push(symbol.clone());
                }
            }
        }
        archive.extend(out.records);
    }
    assert_eq!(post_gap_refresh_syms.len(), 5, "GAP_END must trigger fresh REST anchor for remaining 5 symbols");
    for sym in &post_gap_refresh_syms {
        let px = sym_base_px(sym);
        wall += 1_000_000;
        mono += 1_000_000;
        sel.on_frame(raw_frame(
            ExchangeId::BinanceFutures,
            EndpointKind::BinancePublic,
            ConnId::A,
            2,
            wall,
            mono,
            bin_delta(sym, 205, 209, 204, px, "8.5"),
        ));
        wall += 1_000_000;
        mono += 1_000_000;
        archive.extend(
            sel.on_rest_snapshot(ConnId::A, sym, 206, &bin_rest(206, px), wall, mono)
                .records,
        );
    }

    let audit = verify_binance_records_strict(&archive, &BINANCE_SIX_SYMS);
    assert!(audit.bbo_frames.len() == 6);
}

#[tokio::test]
async fn test_issue19_live_four_exchange_depth_refresh_and_archive_rebuild() {
    let gate_syms: Vec<String> = GATE_SIX_SYMS.iter().map(|s| s.to_string()).collect();
    let two_syms = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()];

    // 1. Gate mock WS (6 symbols; validates X-Gate-Size-Decimal: 1 header, handles unsubscribe->ACK->subscribe->fresh full=true, injects fault for failover + stale depth during unsubscribe)
    let gate_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gate_port = gate_listener.local_addr().unwrap().port();
    let gate_header_ok = Arc::new(AtomicUsize::new(0));
    let gate_conn_seq = Arc::new(AtomicUsize::new(0));
    let gate_stop = Arc::new(AtomicBool::new(false));
    let mut bg_tasks = Vec::new();
    {
        let hdr_ok = Arc::clone(&gate_header_ok);
        let cseq = Arc::clone(&gate_conn_seq);
        let stop = Arc::clone(&gate_stop);
        let syms = gate_syms.clone();
        bg_tasks.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let Ok((stream, _)) = tokio::time::timeout(Duration::from_millis(100), gate_listener.accept()).await.unwrap_or(Err(std::io::Error::new(std::io::ErrorKind::TimedOut, ""))) else {
                    continue;
                };
                let hdr = Arc::clone(&hdr_ok);
                let idx = cseq.fetch_add(1, Ordering::Relaxed);
                let syms_c = syms.clone();
                let stop_c = Arc::clone(&stop);
                tokio::spawn(async move {
                    let cb = |req: &Request, resp: Response| -> Result<Response, ErrorResponse> {
                        if req.headers().get("x-gate-size-decimal").and_then(|v| v.to_str().ok()) == Some("1") {
                            hdr.fetch_add(1, Ordering::Relaxed);
                        }
                        Ok(resp)
                    };
                    let Ok(mut ws) = accept_hdr_async(stream, cb).await else {
                        return;
                    };
                    let mut u_map: HashMap<String, u64> = syms_c.iter().map(|s| (s.clone(), 100)).collect();
                    let mut active_depth: HashSet<String> = HashSet::new();
                    let mut subscribed_once = false;
                    let mut tick = 0u64;
                    let fault_tick = if idx == 0 { 8 } else { 22 };
                    let mut interval = tokio::time::interval(Duration::from_millis(50));
                    interval.tick().await;

                    loop {
                        if stop_c.load(Ordering::Relaxed) {
                            break;
                        }
                        tokio::select! {
                            msg = ws.next() => {
                                match msg {
                                    Some(Ok(Message::Text(t))) => {
                                        let Ok(v) = serde_json::from_str::<Value>(&t) else { continue };
                                        let ch = v["channel"].as_str().unwrap_or("");
                                        let ev = v["event"].as_str().unwrap_or("");
                                        if ch == "futures.ping" {
                                            let _ = ws.send(Message::Text(json!({"channel": "futures.pong"}).to_string())).await;
                                        } else if ch == "futures.obu" && ev == "unsubscribe" {
                                            if let Some(arr) = v["payload"].as_array() {
                                                for item in arr {
                                                    if let Some(tok) = item.as_str() {
                                                        if let Some(sym) = tok.split('.').nth(1).map(|s| s.to_uppercase()) {
                                                            active_depth.remove(&sym);
                                                            let cur_u = *u_map.get(&sym).unwrap_or(&100);
                                                            // Emit stale pre-subscribe delta + BBO right before ACK
                                                            let _ = ws.send(Message::Text(gate_delta_frame(&sym, cur_u + 1, cur_u + 1, sym_base_px(&sym), "9.9"))).await;
                                                            let _ = ws.send(Message::Text(gate_bbo_frame(&sym, sym_base_px(&sym)))).await;
                                                        }
                                                    }
                                                }
                                            }
                                            let ack = json!({"channel": "futures.obu", "event": "unsubscribe", "id": v["id"], "result": {"status": "success"}});
                                            if ws.send(Message::Text(ack.to_string())).await.is_err() {
                                                break;
                                            }
                                        } else if ch == "futures.obu" && ev == "subscribe" {
                                            let ack = json!({"channel": "futures.obu", "event": "subscribe", "payload": v["payload"], "result": {"status": "success"}});
                                            let _ = ws.send(Message::Text(ack.to_string())).await;
                                            if let Some(arr) = v["payload"].as_array() {
                                                for item in arr {
                                                    if let Some(tok) = item.as_str() {
                                                        if let Some(sym) = tok.split('.').nth(1).map(|s| s.to_uppercase()) {
                                                            let next_snap_u = u_map.get(&sym).copied().unwrap_or(100) + 1000;
                                                            u_map.insert(sym.clone(), next_snap_u);
                                                            active_depth.insert(sym.clone());
                                                            if ws.send(Message::Text(gate_snapshot_frame(&sym, next_snap_u, sym_base_px(&sym)))).await.is_err() {
                                                                return;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            if active_depth.len() == syms_c.len() {
                                                subscribed_once = true;
                                            }
                                        } else if ev == "subscribe" {
                                            let ack = json!({"channel": ch, "event": "subscribe", "payload": v["payload"], "result": {"status": "success"}});
                                            let _ = ws.send(Message::Text(ack.to_string())).await;
                                        }
                                    }
                                    Some(Ok(_)) => {}
                                    _ => break,
                                }
                            }
                            _ = interval.tick() => {
                                if !subscribed_once {
                                    continue;
                                }
                                tick += 1;
                                for (s_i, sym) in syms_c.iter().enumerate() {
                                    let px = sym_base_px(sym);
                                    let _ = ws.send(Message::Text(gate_bbo_frame(sym, px))).await;
                                    let _ = ws.send(Message::Text(gate_trade_frame(sym, tick * 10 + s_i as u64, px))).await;
                                    if active_depth.contains(sym) {
                                        if tick == fault_tick && s_i == 0 {
                                            // Deterministic sequence break triggering failover + refresh
                                            let bad = gate_delta_frame(sym, 999_999, 999_999, px, "1.1");
                                            if ws.send(Message::Text(bad)).await.is_err() {
                                                return;
                                            }
                                            active_depth.remove(sym);
                                        } else {
                                            let cur = *u_map.get(sym).unwrap();
                                            let next = cur + 1;
                                            u_map.insert(sym.clone(), next);
                                            let sz = format!("{:.1}", 2.0 + (tick % 5) as f64 * 0.1);
                                            if ws.send(Message::Text(gate_delta_frame(sym, next, next, px, &sz))).await.is_err() {
                                                return;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                });
            }
        }));
    }

    // 2. Bybit mock WS (2 symbols; handles orderbook.50 unsubscribe->ACK->subscribe->fresh snapshot, fault at tick 8 / 22)
    let bybit_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bybit_port = bybit_listener.local_addr().unwrap().port();
    let bybit_conn_seq = Arc::new(AtomicUsize::new(0));
    {
        let cseq = Arc::clone(&bybit_conn_seq);
        let stop = Arc::clone(&gate_stop);
        let syms = two_syms.clone();
        bg_tasks.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let Ok((stream, _)) = tokio::time::timeout(Duration::from_millis(100), bybit_listener.accept()).await.unwrap_or(Err(std::io::Error::new(std::io::ErrorKind::TimedOut, ""))) else {
                    continue;
                };
                let idx = cseq.fetch_add(1, Ordering::Relaxed);
                let syms_c = syms.clone();
                let stop_c = Arc::clone(&stop);
                tokio::spawn(async move {
                    let Ok(mut ws) = accept_async(stream).await else { return };
                    let mut u_map: HashMap<String, u64> = syms_c.iter().map(|s| (s.clone(), 100)).collect();
                    let mut active_depth: HashSet<String> = HashSet::new();
                    let mut subscribed_once = false;
                    let mut tick = 0u64;
                    let fault_tick = if idx == 0 { 8 } else { 22 };
                    let mut interval = tokio::time::interval(Duration::from_millis(50));
                    interval.tick().await;

                    loop {
                        if stop_c.load(Ordering::Relaxed) {
                            break;
                        }
                        tokio::select! {
                            msg = ws.next() => {
                                match msg {
                                    Some(Ok(Message::Text(t))) => {
                                        let Ok(v) = serde_json::from_str::<Value>(&t) else { continue };
                                        let op = v["op"].as_str().unwrap_or("");
                                        if op == "ping" {
                                            let _ = ws.send(Message::Text(json!({"op": "pong"}).to_string())).await;
                                        } else if op == "unsubscribe" {
                                            if let Some(args) = v["args"].as_array() {
                                                for a in args {
                                                    if let Some(sym) = a.as_str().and_then(|s| s.strip_prefix("orderbook.50.")) {
                                                        active_depth.remove(sym);
                                                        let cur_u = *u_map.get(sym).unwrap_or(&100);
                                                        let _ = ws.send(Message::Text(json!({"topic": format!("orderbook.50.{sym}"), "type": "delta", "data": {"s": sym, "u": cur_u + 1, "b": [[format!("{:.1}", sym_base_px(sym)), "9.9"]], "a": []}}).to_string())).await;
                                                        let _ = ws.send(Message::Text(json!({"topic": format!("orderbook.1.{sym}"), "type": "snapshot", "data": {"s": sym, "u": cur_u + 1, "b": [[format!("{:.1}", sym_base_px(sym)), "1.0"]], "a": [[format!("{:.1}", sym_base_px(sym) + 1.0), "1.0"]]}}).to_string())).await;
                                                    }
                                                }
                                            }
                                            let _ = ws.send(Message::Text(json!({"op": "unsubscribe", "success": true, "req_id": v["req_id"]}).to_string())).await;
                                        } else if op == "subscribe" {
                                            let _ = ws.send(Message::Text(json!({"op": "subscribe", "success": true}).to_string())).await;
                                            if let Some(args) = v["args"].as_array() {
                                                for a in args {
                                                    if let Some(sym) = a.as_str().and_then(|s| s.strip_prefix("orderbook.50.")) {
                                                        let next_u = u_map.get(sym).copied().unwrap_or(100) + 1000;
                                                        u_map.insert(sym.to_string(), next_u);
                                                        active_depth.insert(sym.to_string());
                                                        let px = sym_base_px(sym);
                                                        let _ = ws.send(Message::Text(json!({
                                                            "topic": format!("orderbook.50.{sym}"),
                                                            "type": "snapshot",
                                                            "data": {"s": sym, "u": next_u, "b": [[format!("{px:.1}"), "2.0"]], "a": [[format!("{:.1}", px + 1.0), "2.0"]]}
                                                        }).to_string())).await;
                                                    } else if let Some(sym) = a.as_str().and_then(|s| s.strip_prefix("orderbook.1.")) {
                                                        let px = sym_base_px(sym);
                                                        let _ = ws.send(Message::Text(json!({
                                                            "topic": format!("orderbook.1.{sym}"),
                                                            "type": "snapshot",
                                                            "data": {"s": sym, "u": 100, "b": [[format!("{px:.1}"), "1.0"]], "a": [[format!("{:.1}", px + 1.0), "1.0"]]}
                                                        }).to_string())).await;
                                                    }
                                                }
                                            }
                                            subscribed_once = true;
                                        }
                                    }
                                    Some(Ok(_)) => {}
                                    _ => break,
                                }
                            }
                            _ = interval.tick() => {
                                if !subscribed_once {
                                    continue;
                                }
                                tick += 1;
                                for (s_i, sym) in syms_c.iter().enumerate() {
                                    let px = sym_base_px(sym);
                                    let _ = ws.send(Message::Text(json!({
                                        "topic": format!("orderbook.1.{sym}"),
                                        "type": "snapshot",
                                        "data": {"s": sym, "u": 100 + tick, "b": [[format!("{px:.1}"), "1.0"]], "a": [[format!("{:.1}", px + 1.0), "1.0"]]}
                                    }).to_string())).await;
                                    let _ = ws.send(Message::Text(json!({
                                        "topic": format!("publicTrade.{sym}"),
                                        "type": "snapshot",
                                        "data": [{"T": 1796256200000i64, "s": sym, "S": "Buy", "v": "0.1", "p": format!("{px:.1}"), "i": format!("b-{idx}-{tick}-{s_i}")}]
                                    }).to_string())).await;
                                    if active_depth.contains(sym) {
                                        if tick == fault_tick && s_i == 0 {
                                            // Regression u=2 < prev_u forces invalidation -> SWITCH + Refresh
                                            let _ = ws.send(Message::Text(json!({
                                                "topic": format!("orderbook.50.{sym}"),
                                                "type": "delta",
                                                "data": {"s": sym, "u": 2, "b": [[format!("{px:.1}"), "1.1"]], "a": []}
                                            }).to_string())).await;
                                            active_depth.remove(sym);
                                        } else {
                                            let next = *u_map.get(sym).unwrap() + 1;
                                            u_map.insert(sym.clone(), next);
                                            let _ = ws.send(Message::Text(json!({
                                                "topic": format!("orderbook.50.{sym}"),
                                                "type": "delta",
                                                "data": {"s": sym, "u": next, "b": [[format!("{px:.1}"), "2.4"]], "a": []}
                                            }).to_string())).await;
                                        }
                                    }
                                }
                            }
                        }
                    }
                });
            }
        }));
    }

    // 3. Bitget mock WS (2 symbols; handles books50 unsubscribe->ACK->subscribe->fresh snapshot + pseq/seq updates)
    let bitget_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bitget_port = bitget_listener.local_addr().unwrap().port();
    let bitget_conn_seq = Arc::new(AtomicUsize::new(0));
    {
        let cseq = Arc::clone(&bitget_conn_seq);
        let stop = Arc::clone(&gate_stop);
        let syms = two_syms.clone();
        bg_tasks.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let Ok((stream, _)) = tokio::time::timeout(Duration::from_millis(100), bitget_listener.accept()).await.unwrap_or(Err(std::io::Error::new(std::io::ErrorKind::TimedOut, ""))) else {
                    continue;
                };
                let idx = cseq.fetch_add(1, Ordering::Relaxed);
                let syms_c = syms.clone();
                let stop_c = Arc::clone(&stop);
                tokio::spawn(async move {
                    let Ok(mut ws) = accept_async(stream).await else { return };
                    let mut seq_map: HashMap<String, u64> = syms_c.iter().map(|s| (s.clone(), 500)).collect();
                    let mut active_depth: HashSet<String> = HashSet::new();
                    let mut subscribed_once = false;
                    let mut tick = 0u64;
                    let fault_tick = if idx == 0 { 8 } else { 22 };
                    let mut interval = tokio::time::interval(Duration::from_millis(50));
                    interval.tick().await;

                    loop {
                        if stop_c.load(Ordering::Relaxed) {
                            break;
                        }
                        tokio::select! {
                            msg = ws.next() => {
                                match msg {
                                    Some(Ok(Message::Text(t))) => {
                                        if t.trim() == "ping" {
                                            let _ = ws.send(Message::Text("pong".to_string())).await;
                                            continue;
                                        }
                                        let Ok(v) = serde_json::from_str::<Value>(&t) else { continue };
                                        let op = v["op"].as_str().unwrap_or("");
                                        if op == "unsubscribe" {
                                            if let Some(args) = v["args"].as_array() {
                                                for arg in args {
                                                    let sym = arg["symbol"].as_str().unwrap_or("").to_uppercase();
                                                    active_depth.remove(&sym);
                                                    let prev = *seq_map.get(&sym).unwrap_or(&500);
                                                    let px = sym_base_px(&sym);
                                                    let _ = ws.send(Message::Text(json!({
                                                        "action": "update",
                                                        "arg": arg,
                                                        "data": [{"seq": prev + 1, "pseq": prev, "b": [[format!("{px:.1}"), "9.9"]], "a": []}]
                                                    }).to_string())).await;
                                                    let _ = ws.send(Message::Text(json!({
                                                        "action": "snapshot",
                                                        "arg": {"instType": "usdt-futures", "topic": "books1", "symbol": sym},
                                                        "data": [{"seq": 1, "pseq": 0, "b": [[format!("{px:.1}"), "1.0"]], "a": [[format!("{:.1}", px + 1.0), "1.0"]]}]
                                                    }).to_string())).await;
                                                    let _ = ws.send(Message::Text(json!({"event": "unsubscribe", "arg": arg}).to_string())).await;
                                                }
                                            }
                                        } else if op == "subscribe" {
                                            if let Some(args) = v["args"].as_array() {
                                                for arg in args {
                                                    let _ = ws.send(Message::Text(json!({"event": "subscribe", "arg": arg}).to_string())).await;
                                                    if arg["topic"] == "books50" {
                                                        let sym = arg["symbol"].as_str().unwrap_or("").to_uppercase();
                                                        let next_seq = seq_map.get(&sym).copied().unwrap_or(500) + 1000;
                                                        seq_map.insert(sym.clone(), next_seq);
                                                        active_depth.insert(sym.clone());
                                                        let px = sym_base_px(&sym);
                                                        let _ = ws.send(Message::Text(json!({
                                                            "action": "snapshot",
                                                            "arg": arg,
                                                            "data": [{"seq": next_seq, "pseq": 0, "b": [[format!("{px:.1}"), "2.0"]], "a": [[format!("{:.1}", px + 1.0), "2.0"]]}]
                                                        }).to_string())).await;
                                                    }
                                                }
                                            }
                                            subscribed_once = true;
                                        }
                                    }
                                    Some(Ok(_)) => {}
                                    _ => break,
                                }
                            }
                            _ = interval.tick() => {
                                if !subscribed_once {
                                    continue;
                                }
                                tick += 1;
                                for (s_i, sym) in syms_c.iter().enumerate() {
                                    let px = sym_base_px(sym);
                                    let _ = ws.send(Message::Text(json!({
                                        "action": "snapshot",
                                        "arg": {"instType": "usdt-futures", "topic": "books1", "symbol": sym},
                                        "data": [{"seq": 1 + tick, "pseq": 0, "b": [[format!("{px:.1}"), "1.0"]], "a": [[format!("{:.1}", px + 1.0), "1.0"]]}]
                                    }).to_string())).await;
                                    let _ = ws.send(Message::Text(json!({
                                        "action": "snapshot",
                                        "arg": {"instType": "usdt-futures", "topic": "publicTrade", "symbol": sym},
                                        "data": [{"i": format!("bg-{idx}-{tick}-{s_i}"), "p": format!("{px:.1}"), "v": "0.1"}]
                                    }).to_string())).await;
                                    if active_depth.contains(sym) {
                                        let pseq = *seq_map.get(sym).unwrap();
                                        if tick == fault_tick && s_i == 0 {
                                            // Violates pseq < seq -> forces invalidation -> SWITCH + Refresh
                                            let _ = ws.send(Message::Text(json!({
                                                "action": "update",
                                                "arg": {"instType": "usdt-futures", "topic": "books50", "symbol": sym},
                                                "data": [{"seq": pseq, "pseq": pseq, "b": [[format!("{px:.1}"), "1.1"]], "a": []}]
                                            }).to_string())).await;
                                            active_depth.remove(sym);
                                        } else {
                                            let seq = pseq + 1;
                                            seq_map.insert(sym.clone(), seq);
                                            let _ = ws.send(Message::Text(json!({
                                                "action": "update",
                                                "arg": {"instType": "usdt-futures", "topic": "books50", "symbol": sym},
                                                "data": [{"seq": seq, "pseq": pseq, "b": [[format!("{px:.1}"), "2.5"]], "a": []}]
                                            }).to_string())).await;
                                        }
                                    }
                                }
                            }
                        }
                    }
                });
            }
        }));
    }

    // 4. Binance mock WS (/public/stream + /market/stream) + mock HTTP REST (/fapi/v1/depth)
    let bin_u_tracker: Arc<Mutex<HashMap<String, u64>>> = Arc::new(Mutex::new(HashMap::from([
        ("BTCUSDT".to_string(), 100),
        ("ETHUSDT".to_string(), 100),
    ])));
    let bin_rest_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bin_rest_port = bin_rest_listener.local_addr().unwrap().port();
    {
        let u_trk = Arc::clone(&bin_u_tracker);
        let stop = Arc::clone(&gate_stop);
        bg_tasks.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let Ok((mut stream, _)) = tokio::time::timeout(Duration::from_millis(100), bin_rest_listener.accept()).await.unwrap_or(Err(std::io::Error::new(std::io::ErrorKind::TimedOut, ""))) else {
                    continue;
                };
                let trk = Arc::clone(&u_trk);
                tokio::spawn(async move {
                    let mut buf = [0u8; 2048];
                    let n = stream.read(&mut buf).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let sym = req
                        .split("symbol=")
                        .nth(1)
                        .and_then(|s| s.split(['&', ' ']).next())
                        .unwrap_or("BTCUSDT")
                        .to_uppercase();
                    let snap_u = trk.lock().unwrap().get(&sym).copied().unwrap_or(102);
                    // Wait slightly so WS pushes >= 1 delta crossing snap_u + 1.
                    tokio::time::sleep(Duration::from_millis(110)).await;
                    let px = sym_base_px(&sym);
                    let body = json!({
                        "lastUpdateId": snap_u,
                        "E": 1796256000100i64,
                        "T": 1796256000100i64,
                        "bids": [[format!("{px:.2}"), "5.0"]],
                        "asks": [[format!("{:.2}", px + 1.0), "5.0"]]
                    })
                    .to_string();
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(resp.as_bytes()).await;
                });
            }
        }));
    }

    let bin_pub_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bin_pub_port = bin_pub_listener.local_addr().unwrap().port();
    let bin_conn_seq = Arc::new(AtomicUsize::new(0));
    {
        let u_trk = Arc::clone(&bin_u_tracker);
        let cseq = Arc::clone(&bin_conn_seq);
        let stop = Arc::clone(&gate_stop);
        let syms = two_syms.clone();
        let (bcast_tx, _) = tokio::sync::broadcast::channel::<Vec<String>>(128);
        let bcast_prod = bcast_tx.clone();
        let stop_prod = Arc::clone(&stop);
        let syms_prod = syms.clone();
        let u_trk_prod = Arc::clone(&u_trk);
        bg_tasks.push(tokio::spawn(async move {
            let mut prev_u = 99u64;
            let mut tick = 0u64;
            let mut interval = tokio::time::interval(Duration::from_millis(50));
            interval.tick().await;
            while !stop_prod.load(Ordering::Relaxed) {
                interval.tick().await;
                tick += 1;
                let fu = prev_u + 1;
                let lu = fu + 2;
                let mut batch = Vec::with_capacity(syms_prod.len() * 2);
                for sym in &syms_prod {
                    let px = sym_base_px(sym);
                    batch.push(
                        json!({
                            "stream": format!("{}@bookTicker", sym.to_lowercase()),
                            "data": {"e": "bookTicker", "u": lu, "s": sym, "b": format!("{px:.2}"), "B": "1.0", "a": format!("{:.2}", px + 1.0), "A": "1.0"}
                        })
                        .to_string(),
                    );
                    batch.push(
                        json!({
                            "stream": format!("{}@depth@100ms", sym.to_lowercase()),
                            "data": {"e": "depthUpdate", "E": 1796256000000i64 + tick as i64 * 50, "T": 1796256000000i64 + tick as i64 * 50, "s": sym, "U": fu, "u": lu, "pu": prev_u, "b": [[format!("{px:.2}"), "4.2"]], "a": []}
                        })
                        .to_string(),
                    );
                    if let Ok(mut guard) = u_trk_prod.lock() {
                        guard.insert(sym.clone(), lu);
                    }
                }
                prev_u = lu;
                let _ = bcast_tx.send(batch);
            }
        }));
        bg_tasks.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let Ok((stream, _)) = tokio::time::timeout(Duration::from_millis(100), bin_pub_listener.accept()).await.unwrap_or(Err(std::io::Error::new(std::io::ErrorKind::TimedOut, ""))) else {
                    continue;
                };
                let idx = cseq.fetch_add(1, Ordering::Relaxed);
                let mut rx_bcast = bcast_prod.subscribe();
                let stop_c = Arc::clone(&stop);
                tokio::spawn(async move {
                    let Ok(ws) = accept_async(stream).await else { return };
                    let (mut tx, mut rx) = ws.split();
                    tokio::spawn(async move { while let Some(Ok(_)) = rx.next().await {} });
                    let mut ticks_seen = 0u64;
                    let close_tick = match idx {
                        0 => 12,
                        1 => 40,
                        _ => u64::MAX,
                    };
                    while !stop_c.load(Ordering::Relaxed) {
                        match rx_bcast.recv().await {
                            Ok(batch) => {
                                ticks_seen += 1;
                                if ticks_seen == close_tick {
                                    let _ = tx.close().await;
                                    break;
                                }
                                for msg in batch {
                                    if tx.send(Message::Text(msg)).await.is_err() {
                                        return;
                                    }
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        }
                    }
                });
            }
        }));
    }

    let bin_mkt_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bin_mkt_port = bin_mkt_listener.local_addr().unwrap().port();
    {
        let stop = Arc::clone(&gate_stop);
        let syms = two_syms.clone();
        bg_tasks.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let Ok((stream, _)) = tokio::time::timeout(Duration::from_millis(100), bin_mkt_listener.accept()).await.unwrap_or(Err(std::io::Error::new(std::io::ErrorKind::TimedOut, ""))) else {
                    continue;
                };
                let syms_c = syms.clone();
                let stop_c = Arc::clone(&stop);
                tokio::spawn(async move {
                    let Ok(ws) = accept_async(stream).await else { return };
                    let (mut tx, mut rx) = ws.split();
                    tokio::spawn(async move { while let Some(Ok(_)) = rx.next().await {} });
                    let mut agg_id = 1000u64;
                    while !stop_c.load(Ordering::Relaxed) {
                        for sym in &syms_c {
                            agg_id += 1;
                            let px = sym_base_px(sym);
                            let _ = tx.send(Message::Text(json!({
                                "stream": format!("{}@aggTrade", sym.to_lowercase()),
                                "data": {"e": "aggTrade", "E": 1796256000100i64, "a": agg_id, "s": sym, "p": format!("{px:.2}"), "q": "0.1", "f": agg_id, "l": agg_id, "T": 1796256000100i64, "m": false}
                            }).to_string())).await;
                        }
                        tokio::time::sleep(Duration::from_millis(60)).await;
                    }
                });
            }
        }));
    }

    let tmp = TempDir::new().unwrap();
    let cfg_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("specs/config.trial_15m.json");
    let mut cfg = CollectorConfig::load(&cfg_path).unwrap();
    cfg.deployment.data_root = tmp.path().to_string_lossy().to_string();
    cfg.exchanges.gate_futures.enabled = true;
    cfg.exchanges.gate_futures.ws_url = format!("ws://127.0.0.1:{gate_port}/ws");
    cfg.exchanges.gate_futures.symbols = gate_syms.clone();

    cfg.exchanges.bybit_linear.enabled = true;
    cfg.exchanges.bybit_linear.ws_url = format!("ws://127.0.0.1:{bybit_port}/ws");
    cfg.exchanges.bybit_linear.symbols = two_syms.clone();

    cfg.exchanges.bitget_v3.enabled = true;
    cfg.exchanges.bitget_v3.ws_url = format!("ws://127.0.0.1:{bitget_port}/ws");
    cfg.exchanges.bitget_v3.symbols = two_syms.clone();

    cfg.exchanges.binance_futures.enabled = true;
    cfg.exchanges.binance_futures.public_ws_url = format!("ws://127.0.0.1:{bin_pub_port}/public/stream");
    cfg.exchanges.binance_futures.market_ws_url = format!("ws://127.0.0.1:{bin_mkt_port}/market/stream");
    cfg.exchanges.binance_futures.rest_depth_url = format!("http://127.0.0.1:{bin_rest_port}/fapi/v1/depth");
    cfg.exchanges.binance_futures.symbols = two_syms.clone();

    run_live_collector(cfg, "issue19_4ex_live".to_string(), Some(5))
        .await
        .expect("4-exchange live run must seal cleanly");
    gate_stop.store(true, Ordering::Relaxed);
    for t in bg_tasks {
        t.abort();
    }

    assert!(gate_header_ok.load(Ordering::Relaxed) >= 2, "Gate X-Gate-Size-Decimal: 1 header must be sent");

    // Independently validate sealed .tsv.zst archives for all 4 exchanges!
    let gate_recs = collect_archive_records(tmp.path(), "gate_futures");
    let gate_audit = verify_gate_records_strict(&gate_recs, &GATE_SIX_SYMS);
    assert!(gate_audit.meta_events.iter().any(|m| m["event"] == "SWITCH"));
    assert!(gate_audit.meta_events.iter().any(|m| m["event"] == "DEPTH_RECOVERED"));
    assert!(!gate_recs.iter().any(|r| r.payload.contains("\"9.9\"")), "stale Gate depth leaked past DepthRefresh");
    assert!(*gate_audit.bbo_frames.get("BTC_USDT").unwrap_or(&0) >= 5);
    assert!(*gate_audit.trade_frames.get("BTC_USDT").unwrap_or(&0) >= 5);

    let bybit_recs = collect_archive_records(tmp.path(), "bybit_linear");
    let bybit_audit = verify_bybit_records_strict(&bybit_recs, &["BTCUSDT", "ETHUSDT"]);
    assert!(bybit_audit.meta_events.iter().any(|m| m["event"] == "SWITCH"));
    assert!(bybit_audit.meta_events.iter().any(|m| m["event"] == "DEPTH_RECOVERED"));
    assert!(!bybit_recs.iter().any(|r| r.payload.contains("\"9.9\"")), "stale Bybit depth leaked past DepthRefresh");
    assert!(*bybit_audit.bbo_frames.get("BTCUSDT").unwrap_or(&0) >= 5);
    assert!(*bybit_audit.trade_frames.get("BTCUSDT").unwrap_or(&0) >= 5);

    let bitget_recs = collect_archive_records(tmp.path(), "bitget_v3");
    let bitget_audit = verify_bitget_records_strict(&bitget_recs, &["BTCUSDT", "ETHUSDT"], true);
    assert!(bitget_audit.meta_events.iter().any(|m| m["event"] == "SWITCH"));
    assert!(bitget_audit.meta_events.iter().any(|m| m["event"] == "DEPTH_RECOVERED"));
    assert!(!bitget_recs.iter().any(|r| r.payload.contains("\"9.9\"")), "stale Bitget depth leaked past DepthRefresh");
    assert!(*bitget_audit.bbo_frames.get("BTCUSDT").unwrap_or(&0) >= 5);
    assert!(*bitget_audit.trade_frames.get("BTCUSDT").unwrap_or(&0) >= 5);

    let binance_recs = collect_archive_records(tmp.path(), "binance_futures");
    let binance_audit = verify_binance_records_strict(&binance_recs, &["BTCUSDT", "ETHUSDT"]);
    assert!(binance_audit.meta_events.iter().any(|m| m["event"] == "SWITCH"));
    assert!(binance_audit.meta_events.iter().any(|m| m["event"] == "REST_SNAPSHOT_ANCHOR"));
    assert!(*binance_audit.bbo_frames.get("BTCUSDT").unwrap_or(&0) >= 5);
    assert!(*binance_audit.trade_frames.get("BTCUSDT").unwrap_or(&0) >= 5);
}
