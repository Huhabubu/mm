use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

use tokyo_l2_collector::causal_selector::{CausalSelector, SelectorAction};
use tokyo_l2_collector::config::CollectorConfig;
use tokyo_l2_collector::live::run_live_collector;
use tokyo_l2_collector::storage::{read_tsv_zst_records, HourlyTsvZstWriter};
use tokyo_l2_collector::types::{
    CanonicalRecord, ConnId, ConnState, EndpointKind, ExchangeId, RawFrame, RecordKind,
};

fn mk_frame(
    wall_ns: i64,
    mono_ns: u64,
    endpoint: EndpointKind,
    conn_id: ConnId,
    epoch: u64,
    payload: &str,
) -> RawFrame {
    RawFrame {
        recv_wall_ns: wall_ns,
        recv_mono_ns: mono_ns,
        exchange: endpoint.exchange(),
        endpoint,
        conn_id,
        epoch,
        payload: payload.to_string(),
    }
}

#[derive(Debug, Default)]
struct RebuiltBook {
    conn_id: Option<String>,
    epoch: Option<u64>,
    last_update_id: Option<u64>,
    last_u: Option<u64>,
    awaiting_first: bool,
    bids: BTreeMap<String, f64>,
    asks: BTreeMap<String, f64>,
    snapshot_count: usize,
    bridged_count: usize,
    live_count: usize,
    bridged_timestamps: Vec<(i64, i64, u64, u64)>,
    unavailable_events: usize,
}

fn apply_side(book: &mut BTreeMap<String, f64>, levels: &Value) {
    if let Some(arr) = levels.as_array() {
        for item in arr {
            if let Some(pair) = item.as_array() {
                if pair.len() >= 2 {
                    let px = pair[0].as_str().unwrap_or_default().to_string();
                    let qty: f64 = pair[1].as_str().unwrap_or("0").parse().unwrap_or(0.0);
                    if qty <= 0.0 {
                        book.remove(&px);
                    } else {
                        book.insert(px, qty);
                    }
                }
            }
        }
    }
}

fn rebuild_binance_book(records: &[CanonicalRecord], symbol: &str) -> Result<RebuiltBook, String> {
    let target = symbol.to_uppercase();
    let mut b = RebuiltBook::default();

    for rec in records {
        let obj: Value =
            serde_json::from_str(&rec.payload).map_err(|e| format!("bad json: {e}"))?;
        if rec.kind == RecordKind::Meta {
            let ev = obj.get("event").and_then(Value::as_str).unwrap_or("");
            match ev {
                "REST_SNAPSHOT_ANCHOR" => {
                    let sym = obj.get("symbol").and_then(Value::as_str).unwrap_or("");
                    if sym.to_uppercase() != target {
                        continue;
                    }
                    let snap = obj
                        .get("snapshot")
                        .ok_or_else(|| "missing snapshot object".to_string())?;
                    b.bids.clear();
                    b.asks.clear();
                    apply_side(&mut b.bids, &snap["bids"]);
                    apply_side(&mut b.asks, &snap["asks"]);
                    b.conn_id = Some(
                        obj.get("conn_id")
                            .and_then(Value::as_str)
                            .unwrap_or(rec.conn_id.as_str())
                            .to_string(),
                    );
                    b.epoch = Some(
                        obj.get("epoch")
                            .and_then(Value::as_u64)
                            .unwrap_or(rec.epoch),
                    );
                    b.last_update_id = Some(
                        obj.get("lastUpdateId")
                            .and_then(Value::as_u64)
                            .or_else(|| snap.get("lastUpdateId").and_then(Value::as_u64))
                            .ok_or_else(|| "missing lastUpdateId".to_string())?,
                    );
                    b.last_u = None;
                    b.awaiting_first = true;
                    b.snapshot_count += 1;
                }
                "DEPTH_BRIDGED_DELTA" => {
                    let sym = obj.get("symbol").and_then(Value::as_str).unwrap_or("");
                    if sym.to_uppercase() != target {
                        continue;
                    }
                    let d_conn = obj
                        .get("conn_id")
                        .and_then(Value::as_str)
                        .unwrap_or(rec.conn_id.as_str());
                    let d_epoch = obj
                        .get("epoch")
                        .and_then(Value::as_u64)
                        .unwrap_or(rec.epoch);
                    if b.conn_id.as_deref() != Some(d_conn) || b.epoch != Some(d_epoch) {
                        return Err(format!(
                            "bridged delta conn/epoch mismatch: ({d_conn},{d_epoch}) vs ({:?},{:?})",
                            b.conn_id, b.epoch
                        ));
                    }
                    let orig_wall = obj["orig_recv_wall_ns"]
                        .as_i64()
                        .ok_or_else(|| "missing orig_recv_wall_ns".to_string())?;
                    let ready_wall = obj["ready_wall_ns"]
                        .as_i64()
                        .ok_or_else(|| "missing ready_wall_ns".to_string())?;
                    let orig_mono = obj["orig_recv_mono_ns"]
                        .as_u64()
                        .ok_or_else(|| "missing orig_recv_mono_ns".to_string())?;
                    let ready_mono = obj["ready_mono_ns"]
                        .as_u64()
                        .ok_or_else(|| "missing ready_mono_ns".to_string())?;
                    if ready_wall < orig_wall || ready_mono < orig_mono {
                        return Err("bridged delta ready timestamp precedes original receive".into());
                    }
                    let raw_str = obj["raw"]
                        .as_str()
                        .ok_or_else(|| "missing raw ws string".to_string())?;
                    let raw_ws: Value = serde_json::from_str(raw_str)
                        .map_err(|e| format!("bad bridged raw ws: {e}"))?;
                    let data = raw_ws.get("data").unwrap_or(&raw_ws);
                    let first_u = data["U"].as_u64().ok_or("missing U")?;
                    let last_u = data["u"].as_u64().ok_or("missing u")?;
                    let prev_u = data["pu"].as_u64().ok_or("missing pu")?;
                    let snap_uid = b.last_update_id.ok_or("no snapshot id")?;
                    if b.awaiting_first {
                        if !(first_u <= snap_uid && snap_uid <= last_u) {
                            return Err(format!(
                                "bridged first delta violates U<=S<=u: U={first_u}, S={snap_uid}, u={last_u}"
                            ));
                        }
                        b.awaiting_first = false;
                    } else if b.last_u != Some(prev_u) {
                        return Err(format!(
                            "bridged pu break: pu={prev_u} != prev_u={:?}",
                            b.last_u
                        ));
                    }
                    b.last_u = Some(last_u);
                    apply_side(&mut b.bids, &data["b"]);
                    apply_side(&mut b.asks, &data["a"]);
                    b.bridged_count += 1;
                    b.bridged_timestamps
                        .push((orig_wall, ready_wall, orig_mono, ready_mono));
                }
                "DEPTH_UNAVAILABLE" | "SNAPSHOT_BRIDGE_RETRY" => {
                    let sym = obj.get("symbol").and_then(Value::as_str).unwrap_or("");
                    if sym.to_uppercase() == target {
                        b.unavailable_events += 1;
                        b.conn_id = None;
                        b.epoch = None;
                    }
                }
                "SWITCH" => {
                    let to_conn = obj.get("to").and_then(Value::as_str).unwrap_or("");
                    if b.conn_id.as_deref() != Some(to_conn) {
                        b.conn_id = None;
                        b.epoch = None;
                    }
                }
                _ => {}
            }
            continue;
        }

        let stream = obj.get("stream").and_then(Value::as_str).unwrap_or("");
        let data = obj.get("data").unwrap_or(&obj);
        if data.get("e").and_then(Value::as_str) == Some("depthUpdate")
            || stream.contains("@depth")
        {
            let sym = data.get("s").and_then(Value::as_str).unwrap_or("");
            if sym.to_uppercase() != target {
                continue;
            }
            if b.conn_id.as_deref() != Some(rec.conn_id.as_str()) || b.epoch != Some(rec.epoch) {
                return Err(format!(
                    "live delta conn/epoch mismatch: ({},{}) vs ({:?},{:?})",
                    rec.conn_id.as_str(),
                    rec.epoch,
                    b.conn_id,
                    b.epoch
                ));
            }
            let first_u = data["U"].as_u64().ok_or("missing U")?;
            let last_u = data["u"].as_u64().ok_or("missing u")?;
            let prev_u = data["pu"].as_u64().ok_or("missing pu")?;
            let snap_uid = b.last_update_id.ok_or("no snapshot id")?;
            if b.awaiting_first {
                if last_u < snap_uid {
                    continue;
                }
                if !(first_u <= snap_uid && snap_uid <= last_u) {
                    return Err(format!(
                        "first live delta violates U<=S<=u: U={first_u}, S={snap_uid}, u={last_u}"
                    ));
                }
                b.awaiting_first = false;
            } else if b.last_u != Some(prev_u) {
                return Err(format!(
                    "live pu break: pu={prev_u} != prev_u={:?}",
                    b.last_u
                ));
            }
            b.last_u = Some(last_u);
            apply_side(&mut b.bids, &data["b"]);
            apply_side(&mut b.asks, &data["a"]);
            b.live_count += 1;
        }
    }

    Ok(b)
}

#[test]
fn test_binance_buffered_deltas_preserved_and_level_by_level_book_rebuild() {
    let dir = TempDir::new().unwrap();
    let mut writer = HourlyTsvZstWriter::new(
        dir.path().to_path_buf(),
        ExchangeId::BinanceFutures,
        "boot_bridge".to_string(),
        3,
    );
    let mut sel = CausalSelector::new(
        EndpointKind::BinancePublic,
        vec!["BTCUSDT".to_string()],
        3,
        5,
    );
    let t0 = 1_796_256_000_000_000_000i64; // 2026-12-03T00:00:00Z

    // Buffer 3 WS deltas on conn A BEFORE REST snapshot arrives:
    // - d0 (u=98 < S=100): must be dropped as stale
    // - d1 (U=99..u=102): modifies bid 90000.0 (1.0 -> 3.5) and deletes ask 90001.0 (5.0 -> 0.0)
    // - d2 (U=103..u=105, pu=102): adds new bid 89999.5 (4.0) and modifies ask 90001.5 (1.0 -> 2.25)
    let d0 = r#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1000,"T":999,"s":"BTCUSDT","U":95,"u":98,"pu":94,"b":[["88888.0","9.9"]],"a":[]}}"#;
    let d1 = r#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1100,"T":1099,"s":"BTCUSDT","U":99,"u":102,"pu":98,"b":[["90000.0","3.5"]],"a":[["90001.0","0.0"]]}}"#;
    let d2 = r#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1200,"T":1199,"s":"BTCUSDT","U":103,"u":105,"pu":102,"b":[["89999.5","4.0"]],"a":[["90001.5","2.25"]]}}"#;

    for (idx, payload) in [(1, d0), (2, d1), (3, d2)] {
        let out = sel.on_frame(mk_frame(
            t0 + idx * 100_000_000,
            (idx as u64) * 100_000_000,
            EndpointKind::BinancePublic,
            ConnId::A,
            1,
            payload,
        ));
        assert!(out.records.is_empty(), "WaitAnchor must buffer without emitting unanchored depth");
    }

    // REST snapshot arrives later at t0 + 500ms with lastUpdateId=100
    let snap_json = json!({
        "lastUpdateId": 100,
        "E": 1050,
        "T": 1049,
        "bids": [["90000.0", "1.0"], ["89999.0", "2.0"]],
        "asks": [["90001.0", "5.0"], ["90001.5", "1.0"]]
    })
    .to_string();
    let snap_ready_wall = t0 + 500_000_000;
    let snap_ready_mono = 500_000_000u64;
    let snap_out = sel.on_rest_snapshot(
        ConnId::A,
        "BTCUSDT",
        100,
        &snap_json,
        snap_ready_wall,
        snap_ready_mono,
    );
    assert!(snap_out.snapshot_bridge_ok.is_some());
    // Must emit REST_SNAPSHOT_ANCHOR + 2 DEPTH_BRIDGED_DELTA records in order
    assert_eq!(snap_out.records.len(), 3);
    for rec in snap_out.records {
        writer.write_record(&rec).unwrap();
    }

    // Live delta d3 arrives at t0 + 600ms (deletes bid 89999.0, adds ask 90002.0)
    let d3 = r#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1300,"T":1299,"s":"BTCUSDT","U":106,"u":108,"pu":105,"b":[["89999.0","0.0"]],"a":[["90002.0","7.5"]]}}"#;
    let live_out = sel.on_frame(mk_frame(
        t0 + 600_000_000,
        600_000_000,
        EndpointKind::BinancePublic,
        ConnId::A,
        1,
        d3,
    ));
    assert_eq!(live_out.records.len(), 1);
    for rec in live_out.records {
        writer.write_record(&rec).unwrap();
    }

    let sealed = writer.seal_current().unwrap().expect("must seal partition");
    let archived: Vec<CanonicalRecord> = read_tsv_zst_records(&sealed.data_path)
        .unwrap()
        .into_iter()
        .map(|(rec, _)| rec)
        .collect();
    assert_eq!(archived.len(), 4);

    let book = rebuild_binance_book(&archived, "BTCUSDT").expect("archive must reconstruct cleanly");
    assert_eq!(book.snapshot_count, 1);
    assert_eq!(book.bridged_count, 2);
    assert_eq!(book.live_count, 1);
    assert_eq!(book.conn_id.as_deref(), Some("A"));
    assert_eq!(book.epoch, Some(1));
    assert_eq!(book.last_u, Some(108));

    // Verify original WS receive timestamps are strictly preserved AND distinct from snapshot-ready visibility time
    assert_eq!(book.bridged_timestamps.len(), 2);
    assert_eq!(book.bridged_timestamps[0].0, t0 + 200_000_000);
    assert_eq!(book.bridged_timestamps[0].1, snap_ready_wall);
    assert!(book.bridged_timestamps[0].0 < book.bridged_timestamps[0].1);
    assert_eq!(book.bridged_timestamps[1].0, t0 + 300_000_000);
    assert_eq!(book.bridged_timestamps[1].1, snap_ready_wall);
    assert!(book.bridged_timestamps[1].0 < book.bridged_timestamps[1].1);

    // Exact level-by-level verification (d0 stale 88888.0 NOT applied; d1/d2/d3 ALL applied!)
    let mut expected_bids = BTreeMap::new();
    expected_bids.insert("90000.0".to_string(), 3.5);
    expected_bids.insert("89999.5".to_string(), 4.0);
    let mut expected_asks = BTreeMap::new();
    expected_asks.insert("90001.5".to_string(), 2.25);
    expected_asks.insert("90002.0".to_string(), 7.5);
    assert_eq!(book.bids, expected_bids);
    assert_eq!(book.asks, expected_asks);
}

#[test]
fn test_binance_ab_independent_validation_switch_fresh_anchor_and_unavailable_intervals() {
    let mut sel = CausalSelector::new(
        EndpointKind::BinancePublic,
        vec!["BTCUSDT".to_string()],
        3,
        5,
    );
    let t0 = 1_796_256_000_000_000_000i64;
    let mut records: Vec<CanonicalRecord> = Vec::new();

    // Bring A live independently
    let d1_a = r#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1000,"T":999,"s":"BTCUSDT","U":100,"u":105,"pu":99,"b":[["90000.0","2.0"]],"a":[["90001.0","1.5"]]}}"#;
    sel.on_frame(mk_frame(t0 + 10, 10, EndpointKind::BinancePublic, ConnId::A, 1, d1_a));
    let snap_a = json!({"lastUpdateId": 102, "E": 1001, "T": 1000, "bids": [["90000.0", "1.0"]], "asks": [["90001.0", "1.0"]]}).to_string();
    records.extend(sel.on_rest_snapshot(ConnId::A, "BTCUSDT", 102, &snap_a, t0 + 20, 20).records);
    assert_eq!(sel.conn_state(ConnId::A), ConnState::Live);

    // Bring B live independently (standby: does NOT emit into A's canonical stream!)
    let d1_b = r#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1000,"T":999,"s":"BTCUSDT","U":100,"u":105,"pu":99,"b":[["90000.0","2.0"]],"a":[["90001.0","1.5"]]}}"#;
    sel.on_frame(mk_frame(t0 + 25, 25, EndpointKind::BinancePublic, ConnId::B, 1, d1_b));
    let out_b_snap = sel.on_rest_snapshot(ConnId::B, "BTCUSDT", 102, &snap_a, t0 + 30, 30);
    assert!(out_b_snap.records.is_empty(), "standby snapshot must not pollute primary archive stream");
    assert_eq!(sel.conn_state(ConnId::B), ConnState::Live);

    // Inject pu mismatch on A -> emits DEPTH_UNAVAILABLE + SWITCH to B + FetchRestSnapshot on B!
    let bad_a = r#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1100,"T":1099,"s":"BTCUSDT","U":110,"u":115,"pu":109,"b":[],"a":[]}}"#;
    let out_fault = sel.on_frame(mk_frame(t0 + 40, 40, EndpointKind::BinancePublic, ConnId::A, 1, bad_a));
    assert_eq!(sel.primary(), ConnId::B);
    assert!(out_fault
        .records
        .iter()
        .any(|r| r.payload.contains("\"DEPTH_UNAVAILABLE\"")));
    assert!(out_fault
        .records
        .iter()
        .any(|r| r.payload.contains("\"SWITCH\"")));
    assert!(out_fault.actions.iter().any(|a| matches!(
        a,
        SelectorAction::FetchRestSnapshot { conn_id: ConnId::B, symbol } if symbol == "BTCUSDT"
    )));
    records.extend(out_fault.records);

    // B must buffer deltas until B's fresh same-connection REST snapshot arrives (zero A->B splice!)
    let d2_b = r#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1200,"T":1199,"s":"BTCUSDT","U":116,"u":120,"pu":115,"b":[["90000.5","6.0"]],"a":[["90001.0","3.0"]]}}"#;
    let out_buf_b = sel.on_frame(mk_frame(t0 + 50, 50, EndpointKind::BinancePublic, ConnId::B, 1, d2_b));
    assert!(out_buf_b.records.is_empty(), "post-switch B must wait for its own REST snapshot");

    // First send a stale snapshot to B (bridge failure -> SNAPSHOT_BRIDGE_RETRY + refetch action)
    let stale_snap_b = json!({"lastUpdateId": 110, "E": 1150, "T": 1149, "bids": [["90000.0", "1.0"]], "asks": [["90001.0", "1.0"]]}).to_string();
    let out_stale = sel.on_rest_snapshot(ConnId::B, "BTCUSDT", 110, &stale_snap_b, t0 + 55, 55);
    assert!(out_stale.snapshot_bridge_failed.is_some());
    assert!(out_stale
        .records
        .iter()
        .any(|r| r.payload.contains("\"SNAPSHOT_BRIDGE_RETRY\"")));
    records.extend(out_stale.records);

    // Now send valid fresh snapshot S=118 to B -> emits B's REST_SNAPSHOT_ANCHOR + B's DEPTH_BRIDGED_DELTA
    let fresh_snap_b = json!({"lastUpdateId": 118, "E": 1205, "T": 1204, "bids": [["90000.0", "1.0"]], "asks": [["90001.0", "1.0"]]}).to_string();
    let out_ok_b = sel.on_rest_snapshot(ConnId::B, "BTCUSDT", 118, &fresh_snap_b, t0 + 60, 60);
    assert!(out_ok_b.snapshot_bridge_ok.is_some());
    assert_eq!(out_ok_b.records.len(), 2);
    records.extend(out_ok_b.records);

    // Live delta on B
    let d3_b = r#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1300,"T":1299,"s":"BTCUSDT","U":121,"u":122,"pu":120,"b":[["90000.0","0.0"]],"a":[]}}"#;
    let out_live_b = sel.on_frame(mk_frame(t0 + 70, 70, EndpointKind::BinancePublic, ConnId::B, 1, d3_b));
    assert_eq!(out_live_b.records.len(), 1);
    records.extend(out_live_b.records);

    // Verify strict same-connection reconstruction across the entire A -> SWITCH -> B archive
    let book = rebuild_binance_book(&records, "BTCUSDT").expect("same-connection A->B rebuild must succeed");
    assert_eq!(book.snapshot_count, 2);
    assert_eq!(book.bridged_count, 2);
    assert_eq!(book.live_count, 1);
    assert_eq!(book.conn_id.as_deref(), Some("B"));
    assert!(book.unavailable_events >= 2);
    assert_eq!(book.bids.get("90000.5"), Some(&6.0));
    assert!(!book.bids.contains_key("90000.0"));
    assert_eq!(book.asks.get("90001.0"), Some(&3.0));
}

#[tokio::test]
async fn test_binance_live_six_symbols_recovery_from_failures_writes_valid_depth_and_rebuilds_book() {
    let symbols = vec![
        "BTCUSDT".to_string(),
        "ETHUSDT".to_string(),
        "XRPUSDT".to_string(),
        "SOLUSDT".to_string(),
        "DOGEUSDT".to_string(),
        "SUIUSDT".to_string(),
    ];

    // Mock Binance REST /fapi/v1/depth server injecting diverse failure modes before recovery
    let rest_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rest_addr = rest_listener.local_addr().unwrap();
    let attempt_counts: Arc<Mutex<HashMap<String, usize>>> = Arc::new(Mutex::new(HashMap::new()));
    let attempt_counts_srv = attempt_counts.clone();

    let rest_task = tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = rest_listener.accept().await else {
                break;
            };
            let counts = attempt_counts_srv.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 2048];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                if n == 0 {
                    return;
                }
                let req = String::from_utf8_lossy(&buf[..n]);
                let first_line = req.lines().next().unwrap_or("");
                let path = first_line.split_whitespace().nth(1).unwrap_or("");
                let sym = path
                    .split("symbol=")
                    .nth(1)
                    .and_then(|s| s.split('&').next())
                    .unwrap_or("BTCUSDT")
                    .to_uppercase();

                let attempt = {
                    let mut guard = counts.lock().unwrap();
                    let entry = guard.entry(sym.clone()).or_insert(0);
                    *entry += 1;
                    *entry
                };

                // Distinct per-symbol initial failure modes before succeeding:
                // - BTCUSDT attempt 1 -> HTTP 200 invalid/corrupt JSON
                // - DOGEUSDT attempt 1 -> abrupt TCP drop (transport error)
                // - ETHUSDT attempt 1 -> HTTP 500 Internal Server Error
                // - SOLUSDT attempt 1 -> HTTP 200 stale lastUpdateId=50 (bridge failure!)
                // - SUIUSDT attempt 1 -> immediate HTTP 200 success
                // - XRPUSDT attempt 1 -> HTTP 429 with Retry-After: 1 (triggers shared cooldown -> SingleProbe -> GradualRamp)
                if sym == "DOGEUSDT" && attempt == 1 {
                    drop(sock);
                    return;
                }

                let (status_line, extra_headers, body) = match (sym.as_str(), attempt) {
                    ("BTCUSDT", 1) => (
                        "HTTP/1.1 200 OK",
                        "X-MBX-USED-WEIGHT-1M: 100\r\n",
                        r#"{"not_a_snapshot":true}"#.to_string(),
                    ),
                    ("ETHUSDT", 1) => (
                        "HTTP/1.1 500 Internal Server Error",
                        "X-MBX-USED-WEIGHT-1M: 120\r\n",
                        r#"{"code":-1000,"msg":"server error"}"#.to_string(),
                    ),
                    ("XRPUSDT", 1) => (
                        "HTTP/1.1 429 Too Many Requests",
                        "Retry-After: 1\r\nX-MBX-USED-WEIGHT-1M: 1200\r\n",
                        r#"{"code":-1003,"msg":"Too many requests"}"#.to_string(),
                    ),
                    ("SOLUSDT", 1) => (
                        "HTTP/1.1 200 OK",
                        "X-MBX-USED-WEIGHT-1M: 160\r\n",
                        json!({
                            "lastUpdateId": 50,
                            "E": 1796256000050i64,
                            "T": 1796256000049i64,
                            "bids": [["100.0", "1.0"]],
                            "asks": [["101.0", "1.0"]]
                        })
                        .to_string(),
                    ),
                    _ => {
                        let base_px: f64 = match sym.as_str() {
                            "BTCUSDT" => 90000.0,
                            "ETHUSDT" => 3000.0,
                            "XRPUSDT" => 2.0,
                            "SOLUSDT" => 200.0,
                            "DOGEUSDT" => 0.30,
                            _ => 4.0,
                        };
                        (
                            "HTTP/1.1 200 OK",
                            "X-MBX-USED-WEIGHT-1M: 200\r\n",
                            json!({
                                "lastUpdateId": 102,
                                "E": 1796256000100i64,
                                "T": 1796256000099i64,
                                "bids": [[format!("{:.2}", base_px), "10.0"], [format!("{:.2}", base_px - 0.10), "20.0"]],
                                "asks": [[format!("{:.2}", base_px + 0.10), "15.0"], [format!("{:.2}", base_px + 0.20), "25.0"]]
                            })
                            .to_string(),
                        )
                    }
                };

                let resp = format!(
                    "{status_line}\r\nContent-Type: application/json\r\n{extra_headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.shutdown().await;
            });
        }
    });

    // Mock Binance /public/stream WS server
    let pub_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let pub_addr = pub_listener.local_addr().unwrap();
    let syms_pub = symbols.clone();
    let pub_task = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = pub_listener.accept().await else {
                break;
            };
            let syms = syms_pub.clone();
            tokio::spawn(async move {
                let Ok(ws) = accept_async(stream).await else {
                    return;
                };
                let (mut tx, mut rx) = ws.split();
                tokio::spawn(async move {
                    while let Some(Ok(_)) = rx.next().await {}
                });

                // 1. Send bookTicker for all 6 symbols
                for sym in &syms {
                    let msg = json!({
                        "stream": format!("{}@bookTicker", sym.to_lowercase()),
                        "data": {
                            "e": "bookTicker", "u": 1, "s": sym,
                            "b": "100.0", "B": "1.0", "a": "100.1", "A": "1.0",
                            "T": 1796256000050i64, "E": 1796256000050i64
                        }
                    })
                    .to_string();
                    if tx.send(Message::Text(msg.into())).await.is_err() {
                        return;
                    }
                }

                // 2. Send 2 initial buffered depth deltas (bridging lastUpdateId=102) for each symbol
                //    modifying distinct price levels!
                for sym in &syms {
                    let base_px: f64 = match sym.as_str() {
                        "BTCUSDT" => 90000.0,
                        "ETHUSDT" => 3000.0,
                        "XRPUSDT" => 2.0,
                        "SOLUSDT" => 200.0,
                        "DOGEUSDT" => 0.30,
                        _ => 4.0,
                    };
                    let d1 = json!({
                        "stream": format!("{}@depth@100ms", sym.to_lowercase()),
                        "data": {
                            "e": "depthUpdate", "E": 1796256000101i64, "T": 1796256000100i64,
                            "s": sym, "U": 100, "u": 103, "pu": 99,
                            "b": [[format!("{:.2}", base_px), "12.5"]],
                            "a": [[format!("{:.2}", base_px + 0.10), "0.0"]]
                        }
                    })
                    .to_string();
                    let d2 = json!({
                        "stream": format!("{}@depth@100ms", sym.to_lowercase()),
                        "data": {
                            "e": "depthUpdate", "E": 1796256000201i64, "T": 1796256000200i64,
                            "s": sym, "U": 104, "u": 106, "pu": 103,
                            "b": [[format!("{:.2}", base_px - 0.05), "7.5"]],
                            "a": [[format!("{:.2}", base_px + 0.20), "33.0"]]
                        }
                    })
                    .to_string();
                    let _ = tx.send(Message::Text(d1.into())).await;
                    let _ = tx.send(Message::Text(d2.into())).await;
                }

                // 3. Keep streaming continuous pu-chained live depth updates every 100ms
                let mut prev_u = 106u64;
                for tick in 0..130u64 {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    let first_u = prev_u + 1;
                    let last_u = first_u + 1;
                    for sym in &syms {
                        let base_px: f64 = match sym.as_str() {
                            "BTCUSDT" => 90000.0,
                            "ETHUSDT" => 3000.0,
                            "XRPUSDT" => 2.0,
                            "SOLUSDT" => 200.0,
                            "DOGEUSDT" => 0.30,
                            _ => 4.0,
                        };
                        let live_d = json!({
                            "stream": format!("{}@depth@100ms", sym.to_lowercase()),
                            "data": {
                                "e": "depthUpdate",
                                "E": 1796256000300i64 + (tick as i64) * 100,
                                "T": 1796256000299i64 + (tick as i64) * 100,
                                "s": sym, "U": first_u, "u": last_u, "pu": prev_u,
                                "b": [[format!("{:.2}", base_px - 0.10), format!("{:.1}", 20.0 + (tick as f64) * 0.1)]],
                                "a": [[format!("{:.2}", base_px + 0.30), "5.0"]]
                            }
                        })
                        .to_string();
                        if tx.send(Message::Text(live_d.into())).await.is_err() {
                            return;
                        }
                    }
                    prev_u = last_u;
                }
            });
        }
    });

    // Mock Binance /market/stream WS server (aggTrade for all 6 symbols)
    let mkt_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mkt_addr = mkt_listener.local_addr().unwrap();
    let syms_mkt = symbols.clone();
    let mkt_task = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = mkt_listener.accept().await else {
                break;
            };
            let syms = syms_mkt.clone();
            tokio::spawn(async move {
                let Ok(ws) = accept_async(stream).await else {
                    return;
                };
                let (mut tx, mut rx) = ws.split();
                tokio::spawn(async move {
                    while let Some(Ok(_)) = rx.next().await {}
                });
                for (i, sym) in syms.iter().enumerate() {
                    let msg = json!({
                        "stream": format!("{}@aggTrade", sym.to_lowercase()),
                        "data": {
                            "e": "aggTrade", "E": 1796256000100i64, "s": sym,
                            "a": 5000 + i, "p": "100.0", "q": "0.5",
                            "f": 100 + i, "l": 100 + i, "T": 1796256000099i64, "m": false
                        }
                    })
                    .to_string();
                    let _ = tx.send(Message::Text(msg.into())).await;
                }
                tokio::time::sleep(std::time::Duration::from_secs(12)).await;
            });
        }
    });

    let tmp = TempDir::new().unwrap();
    let mut cfg = CollectorConfig::load(std::path::Path::new("specs/config.example.json")).unwrap();
    cfg.deployment.data_root = tmp.path().to_string_lossy().to_string();
    cfg.exchanges.bybit_linear.enabled = false;
    cfg.exchanges.bitget_v3.enabled = false;
    cfg.exchanges.gate_futures.enabled = false;
    cfg.exchanges.binance_futures.public_ws_url = format!("ws://{}/public/stream", pub_addr);
    cfg.exchanges.binance_futures.market_ws_url = format!("ws://{}/market/stream", mkt_addr);
    cfg.exchanges.binance_futures.rest_depth_url = format!("http://{}/fapi/v1/depth", rest_addr);

    run_live_collector(cfg, "binance_6sym_recovery".to_string(), Some(10))
        .await
        .expect("collector must run and seal cleanly");

    rest_task.abort();
    pub_task.abort();
    mkt_task.abort();

    // Verify sealed Binance partition exists and contains valid depth records + anchors for ALL 6 symbols!
    let mut data_files = Vec::new();
    for entry in std::fs::read_dir(tmp.path().join("raw/binance_futures")).unwrap() {
        let day_dir = entry.unwrap().path();
        for f in std::fs::read_dir(day_dir).unwrap() {
            let p = f.unwrap().path();
            if p.extension().and_then(|e| e.to_str()) == Some("zst") {
                data_files.push(p);
            }
        }
    }
    assert!(!data_files.is_empty(), "expected sealed binance .tsv.zst file");
    let mut all_records: Vec<CanonicalRecord> = Vec::new();
    for df in &data_files {
        all_records.extend(
            read_tsv_zst_records(df)
                .unwrap()
                .into_iter()
                .map(|(rec, _)| rec),
        );
    }

    // Check every symbol recovered from failures, emitted anchor + bridged deltas + live depth@100ms,
    // and reconstructs to the exact expected price levels!
    for sym in &symbols {
        let book = rebuild_binance_book(&all_records, sym)
            .unwrap_or_else(|e| panic!("book rebuild failed for {sym}: {e}"));
        assert!(book.snapshot_count >= 1, "{sym}: missing REST_SNAPSHOT_ANCHOR");
        assert!(book.bridged_count >= 2, "{sym}: missing DEPTH_BRIDGED_DELTA records");
        assert!(book.live_count >= 1, "{sym}: missing live depth@100ms DATA records");

        let base_px: f64 = match sym.as_str() {
            "BTCUSDT" => 90000.0,
            "ETHUSDT" => 3000.0,
            "XRPUSDT" => 2.0,
            "SOLUSDT" => 200.0,
            "DOGEUSDT" => 0.30,
            _ => 4.0,
        };
        // d1 updated base_px bid to 12.5 and deleted base_px+0.10 ask
        assert_eq!(book.bids.get(&format!("{:.2}", base_px)), Some(&12.5), "{sym}: d1 bid update missing");
        assert!(!book.asks.contains_key(&format!("{:.2}", base_px + 0.10)), "{sym}: d1 ask deletion missing");
        // d2 added base_px-0.05 bid at 7.5 and updated base_px+0.20 ask to 33.0
        assert_eq!(book.bids.get(&format!("{:.2}", base_px - 0.05)), Some(&7.5), "{sym}: d2 bid add missing");
        assert_eq!(book.asks.get(&format!("{:.2}", base_px + 0.20)), Some(&33.0), "{sym}: d2 ask update missing");
        // live deltas added base_px+0.30 ask at 5.0
        assert_eq!(book.asks.get(&format!("{:.2}", base_px + 0.30)), Some(&5.0), "{sym}: live ask add missing");
    }

    // Verify every injected failure mode actually fired and retried cleanly
    {
        let counts = attempt_counts.lock().unwrap();
        for sym in ["BTCUSDT", "ETHUSDT", "XRPUSDT", "SOLUSDT", "DOGEUSDT"] {
            assert!(
                counts.get(sym).copied().unwrap_or(0) >= 2,
                "{sym} should have retried after initial injected failure, counts={:?}",
                *counts
            );
        }
        assert!(counts.get("SUIUSDT").copied().unwrap_or(0) >= 1);
    }

    // Verify 4-state stream_activity.json reports all 18 Binance streams connected, evidenced, received, written!
    let sa_path = tmp.path().join("meta/stream_activity.json");
    let sa: Value = serde_json::from_str(&std::fs::read_to_string(&sa_path).unwrap()).unwrap();
    assert_eq!(sa["schema_version"], 2);
    let streams = sa["streams"].as_array().unwrap();
    for sym in &symbols {
        for ch in ["bookTicker", "depth@100ms", "aggTrade"] {
            let entry = streams
                .iter()
                .find(|s| s["symbol"] == *sym && s["channel"] == ch)
                .unwrap_or_else(|| panic!("missing stream_activity entry for {sym}/{ch}"));
            assert_eq!(entry["ws_connected"], true, "{sym}/{ch} ws_connected");
            assert_eq!(entry["subscription_evidenced"], true, "{sym}/{ch} subscription_evidenced");
            assert_eq!(entry["received_valid_market_data"], true, "{sym}/{ch} received_valid_market_data");
            assert_eq!(entry["written"], true, "{sym}/{ch} written");
        }
    }
}
