use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener as StdTcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::Message;
use tokyo_l2_collector::storage::{verify_zstd_and_sha256, HourlyTsvZstWriter, PartitionManifest};
use tokyo_l2_collector::types::{CanonicalRecord, ConnId, ExchangeId, RecordKind};

fn now_wall_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

fn collect_files_recursive(dir: &Path, out: &mut Vec<PathBuf>) {
    if let Ok(rd) = fs::read_dir(dir) {
        for entry in rd.flatten() {
            let p = entry.path();
            if p.is_dir() {
                collect_files_recursive(&p, out);
            } else if p.is_file() {
                out.push(p);
            }
        }
    }
}

fn wait_for_all_four_partials(data_root: &Path, min_bytes: u64, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    let expected = ["binance_futures", "bybit_linear", "bitget_v3", "gate_futures"];
    while Instant::now() < deadline {
        let mut files = Vec::new();
        collect_files_recursive(&data_root.join("raw"), &mut files);
        let mut ready = HashSet::new();
        for f in files {
            let name = f.file_name().and_then(|s| s.to_str()).unwrap_or("");
            if name.ends_with(".tsv.zst.partial") {
                if let Ok(md) = fs::metadata(&f) {
                    if md.len() >= min_bytes {
                        for ex in &expected {
                            if name.starts_with(ex) {
                                ready.insert(*ex);
                            }
                        }
                    }
                }
            }
        }
        if ready.len() == expected.len() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

fn assert_all_four_exchanges_sealed(data_root: &Path, expected_reason: &str) {
    let mut all_files = Vec::new();
    collect_files_recursive(&data_root.join("raw"), &mut all_files);

    let partials: Vec<PathBuf> = all_files
        .iter()
        .filter(|p| {
            p.file_name()
                .and_then(|s| s.to_str())
                .map(|n| n.ends_with(".partial"))
                .unwrap_or(false)
        })
        .cloned()
        .collect();
    assert!(
        partials.is_empty(),
        "Expected ZERO leftover .partial files after graceful shutdown, found: {:?}",
        partials
    );

    let mut sealed_exchanges: HashSet<String> = HashSet::new();
    let expected_exchanges = ["binance_futures", "bybit_linear", "bitget_v3", "gate_futures"];

    for p in &all_files {
        let name = p.file_name().and_then(|s| s.to_str()).unwrap_or("");
        if name.ends_with(".manifest.json") {
            let raw = fs::read_to_string(p).expect("read manifest");
            let manifest: PartitionManifest =
                serde_json::from_str(&raw).expect("parse PartitionManifest");
            assert!(manifest.zstd_verified, "manifest zstd_verified must be true");
            assert!(
                manifest.data_lines > 0,
                "exchange {} must have >0 sealed data lines",
                manifest.exchange
            );

            let data_path = p.parent().unwrap().join(&manifest.file_name);
            assert!(
                data_path.is_file(),
                "missing sealed data file {}",
                data_path.display()
            );
            let (bytes, sha256) =
                verify_zstd_and_sha256(&data_path).expect("zstd verify + sha256 must pass");
            assert_eq!(bytes, manifest.bytes, "byte count mismatch for {}", manifest.exchange);
            assert_eq!(sha256, manifest.sha256, "sha256 mismatch for {}", manifest.exchange);
            sealed_exchanges.insert(manifest.exchange);
        }
    }

    for ex in &expected_exchanges {
        assert!(
            sealed_exchanges.contains(*ex),
            "Exchange {} was NOT sealed! Sealed set: {:?}, all files: {:?}",
            ex,
            sealed_exchanges,
            all_files
        );
    }

    // Verify crash-auditable disk shutdown summary exists and reports 4/4 sealed, 0 failed.
    let mut meta_files = Vec::new();
    collect_files_recursive(&data_root.join("meta"), &mut meta_files);
    let audit_files: Vec<PathBuf> = meta_files
        .into_iter()
        .filter(|p| {
            p.file_name()
                .and_then(|s| s.to_str())
                .map(|n| n.starts_with("shutdown_") && n.ends_with(".json"))
                .unwrap_or(false)
        })
        .collect();
    assert_eq!(
        audit_files.len(),
        1,
        "Expected exactly 1 shutdown audit JSON in meta/, found {:?}",
        audit_files
    );
    let audit: Value = serde_json::from_str(&fs::read_to_string(&audit_files[0]).unwrap()).unwrap();
    assert_eq!(audit["shutdown_reason"].as_str(), Some(expected_reason));
    assert_eq!(audit["sealed_count"].as_u64(), Some(4));
    assert_eq!(audit["failed_count"].as_u64(), Some(0));
}

struct MockExchangeCluster {
    stop: Arc<AtomicBool>,
    bin_pub_port: u16,
    bin_mkt_port: u16,
    bin_rest_port: u16,
    bybit_port: u16,
    bitget_port: u16,
    gate_port: u16,
    total_ws_conns: Arc<AtomicUsize>,
    gate_header_seen: Arc<AtomicUsize>,
    handles: Vec<tokio::task::JoinHandle<()>>,
    rest_thread: Option<std::thread::JoinHandle<()>>,
}

impl MockExchangeCluster {
    async fn start(frame_interval_ms: u64) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let total_ws_conns = Arc::new(AtomicUsize::new(0));
        let gate_header_seen = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();

        // 1. Binance REST /fapi/v1/depth mock (std thread to keep Cargo tokio features minimal)
        let rest_listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        rest_listener.set_nonblocking(true).unwrap();
        let bin_rest_port = rest_listener.local_addr().unwrap().port();
        let stop_rest = Arc::clone(&stop);
        let rest_thread = std::thread::spawn(move || {
            let body = r#"{"lastUpdateId":1005,"bids":[["95000.0","5.0"]],"asks":[["95000.1","4.0"]]}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            while !stop_rest.load(Ordering::Relaxed) {
                match rest_listener.accept() {
                    Ok((mut stream, _)) => {
                        let _ = stream.set_read_timeout(Some(Duration::from_millis(200)));
                        let mut buf = [0u8; 1024];
                        let _ = stream.read(&mut buf);
                        let _ = stream.write_all(resp.as_bytes());
                        let _ = stream.flush();
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        });

        // 2. Binance /public/stream WS mock
        let bin_pub_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bin_pub_port = bin_pub_listener.local_addr().unwrap().port();
        {
            let stop_c = Arc::clone(&stop);
            let conns = Arc::clone(&total_ws_conns);
            handles.push(tokio::spawn(async move {
                while !stop_c.load(Ordering::Relaxed) {
                    let accept_res = tokio::select! {
                        r = bin_pub_listener.accept() => r,
                        _ = tokio::time::sleep(Duration::from_millis(100)) => continue,
                    };
                    if let Ok((stream, _)) = accept_res {
                        let stop_s = Arc::clone(&stop_c);
                        let conns_s = Arc::clone(&conns);
                        tokio::spawn(async move {
                            if let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await {
                                conns_s.fetch_add(1, Ordering::Relaxed);
                                let mut seq: u64 = 1000;
                                let mut prev_u: u64 = 999;
                                while !stop_s.load(Ordering::Relaxed) {
                                    let now_ms = (now_wall_ns() / 1_000_000) as u64;
                                    let fu = seq;
                                    let lu = seq + 5;
                                    let depth_msg = json!({
                                        "stream": "btcusdt@depth@100ms",
                                        "data": {
                                            "e": "depthUpdate",
                                            "E": now_ms,
                                            "T": now_ms,
                                            "s": "BTCUSDT",
                                            "U": fu,
                                            "u": lu,
                                            "pu": prev_u,
                                            "b": [["95000.0", "3.5"], ["94999.9", "2.1"]],
                                            "a": [["95000.1", "2.9"], ["95000.2", "4.4"]]
                                        }
                                    })
                                    .to_string();
                                    if ws.send(Message::Text(depth_msg)).await.is_err() {
                                        break;
                                    }
                                    prev_u = lu;
                                    seq = lu + 1;

                                    let bt_msg = json!({
                                        "stream": "btcusdt@bookTicker",
                                        "data": {
                                            "e": "bookTicker",
                                            "u": lu,
                                            "s": "BTCUSDT",
                                            "b": "95000.0",
                                            "B": "3.5",
                                            "a": "95000.1",
                                            "A": "2.9",
                                            "T": now_ms,
                                            "E": now_ms
                                        }
                                    })
                                    .to_string();
                                    if ws.send(Message::Text(bt_msg)).await.is_err() {
                                        break;
                                    }
                                    tokio::time::sleep(Duration::from_millis(frame_interval_ms)).await;
                                }
                            }
                        });
                    }
                }
            }));
        }

        // 3. Binance /market/stream WS mock
        let bin_mkt_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bin_mkt_port = bin_mkt_listener.local_addr().unwrap().port();
        {
            let stop_c = Arc::clone(&stop);
            let conns = Arc::clone(&total_ws_conns);
            handles.push(tokio::spawn(async move {
                while !stop_c.load(Ordering::Relaxed) {
                    let accept_res = tokio::select! {
                        r = bin_mkt_listener.accept() => r,
                        _ = tokio::time::sleep(Duration::from_millis(100)) => continue,
                    };
                    if let Ok((stream, _)) = accept_res {
                        let stop_s = Arc::clone(&stop_c);
                        let conns_s = Arc::clone(&conns);
                        tokio::spawn(async move {
                            if let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await {
                                conns_s.fetch_add(1, Ordering::Relaxed);
                                let mut agg_id: u64 = 50_000;
                                while !stop_s.load(Ordering::Relaxed) {
                                    let now_ms = (now_wall_ns() / 1_000_000) as u64;
                                    agg_id += 1;
                                    let msg = json!({
                                        "stream": "btcusdt@aggTrade",
                                        "data": {
                                            "e": "aggTrade",
                                            "E": now_ms,
                                            "a": agg_id,
                                            "s": "BTCUSDT",
                                            "p": "95000.1",
                                            "q": "0.15",
                                            "f": agg_id * 2,
                                            "l": agg_id * 2 + 1,
                                            "T": now_ms,
                                            "m": false
                                        }
                                    })
                                    .to_string();
                                    if ws.send(Message::Text(msg)).await.is_err() {
                                        break;
                                    }
                                    tokio::time::sleep(Duration::from_millis(frame_interval_ms)).await;
                                }
                            }
                        });
                    }
                }
            }));
        }

        // 4. Bybit v5 linear WS mock
        let bybit_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bybit_port = bybit_listener.local_addr().unwrap().port();
        {
            let stop_c = Arc::clone(&stop);
            let conns = Arc::clone(&total_ws_conns);
            handles.push(tokio::spawn(async move {
                while !stop_c.load(Ordering::Relaxed) {
                    let accept_res = tokio::select! {
                        r = bybit_listener.accept() => r,
                        _ = tokio::time::sleep(Duration::from_millis(100)) => continue,
                    };
                    if let Ok((stream, _)) = accept_res {
                        let stop_s = Arc::clone(&stop_c);
                        let conns_s = Arc::clone(&conns);
                        tokio::spawn(async move {
                            if let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await {
                                conns_s.fetch_add(1, Ordering::Relaxed);
                                // Consume initial subscribe message
                                let _ = tokio::time::timeout(Duration::from_millis(500), ws.next()).await;
                                let now_ms = (now_wall_ns() / 1_000_000) as u64;
                                for topic in ["orderbook.50.BTCUSDT", "orderbook.1.BTCUSDT"] {
                                    let snap = json!({
                                        "topic": topic,
                                        "type": "snapshot",
                                        "ts": now_ms,
                                        "cts": now_ms,
                                        "data": {
                                            "s": "BTCUSDT",
                                            "b": [["95000.0", "2.0"]],
                                            "a": [["95000.1", "1.8"]],
                                            "u": 100,
                                            "seq": 1000
                                        }
                                    })
                                    .to_string();
                                    if ws.send(Message::Text(snap)).await.is_err() {
                                        return;
                                    }
                                }
                                let mut u: u64 = 100;
                                while !stop_s.load(Ordering::Relaxed) {
                                    u += 1;
                                    let now_ms = (now_wall_ns() / 1_000_000) as u64;
                                    let delta = json!({
                                        "topic": "orderbook.50.BTCUSDT",
                                        "type": "delta",
                                        "ts": now_ms,
                                        "cts": now_ms,
                                        "data": {
                                            "s": "BTCUSDT",
                                            "b": [["95000.0", "2.1"]],
                                            "a": [],
                                            "u": u,
                                            "seq": 1000 + u
                                        }
                                    })
                                    .to_string();
                                    if ws.send(Message::Text(delta)).await.is_err() {
                                        break;
                                    }
                                    let trade = json!({
                                        "topic": "publicTrade.BTCUSDT",
                                        "type": "snapshot",
                                        "ts": now_ms,
                                        "data": [{
                                            "T": now_ms,
                                            "s": "BTCUSDT",
                                            "S": "Buy",
                                            "v": "0.2",
                                            "p": "95000.1",
                                            "i": format!("bybit-t-{}", u),
                                            "BT": false,
                                            "RPI": false,
                                            "seq": 2000 + u
                                        }]
                                    })
                                    .to_string();
                                    if ws.send(Message::Text(trade)).await.is_err() {
                                        break;
                                    }
                                    tokio::time::sleep(Duration::from_millis(frame_interval_ms)).await;
                                }
                            }
                        });
                    }
                }
            }));
        }

        // 5. Bitget UTA v3 public WS mock
        let bitget_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bitget_port = bitget_listener.local_addr().unwrap().port();
        {
            let stop_c = Arc::clone(&stop);
            let conns = Arc::clone(&total_ws_conns);
            handles.push(tokio::spawn(async move {
                while !stop_c.load(Ordering::Relaxed) {
                    let accept_res = tokio::select! {
                        r = bitget_listener.accept() => r,
                        _ = tokio::time::sleep(Duration::from_millis(100)) => continue,
                    };
                    if let Ok((stream, _)) = accept_res {
                        let stop_s = Arc::clone(&stop_c);
                        let conns_s = Arc::clone(&conns);
                        tokio::spawn(async move {
                            if let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await {
                                conns_s.fetch_add(1, Ordering::Relaxed);
                                let _ = tokio::time::timeout(Duration::from_millis(500), ws.next()).await;
                                let now_ms = (now_wall_ns() / 1_000_000) as u64;
                                for topic in ["books50", "books1"] {
                                    let snap = json!({
                                        "action": "snapshot",
                                        "arg": {
                                            "instType": "usdt-futures",
                                            "topic": topic,
                                            "symbol": "BTCUSDT"
                                        },
                                        "data": [{
                                            "a": [["95000.1", "2.5"]],
                                            "b": [["95000.0", "2.7"]],
                                            "ts": now_ms.to_string(),
                                            "seq": 5000,
                                            "pseq": 0
                                        }],
                                        "ts": now_ms
                                    })
                                    .to_string();
                                    if ws.send(Message::Text(snap)).await.is_err() {
                                        return;
                                    }
                                }
                                let mut seq: u64 = 5000;
                                while !stop_s.load(Ordering::Relaxed) {
                                    let pseq = seq;
                                    seq += 1;
                                    let now_ms = (now_wall_ns() / 1_000_000) as u64;
                                    let upd = json!({
                                        "action": "update",
                                        "arg": {
                                            "instType": "usdt-futures",
                                            "topic": "books50",
                                            "symbol": "BTCUSDT"
                                        },
                                        "data": [{
                                            "a": [["95000.1", "2.4"]],
                                            "b": [],
                                            "ts": now_ms.to_string(),
                                            "seq": seq,
                                            "pseq": pseq
                                        }],
                                        "ts": now_ms
                                    })
                                    .to_string();
                                    if ws.send(Message::Text(upd)).await.is_err() {
                                        break;
                                    }
                                    tokio::time::sleep(Duration::from_millis(frame_interval_ms)).await;
                                }
                            }
                        });
                    }
                }
            }));
        }

        // 6. Gate Futures v4 WS mock (verifying mandatory X-Gate-Size-Decimal: 1 header!)
        let gate_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let gate_port = gate_listener.local_addr().unwrap().port();
        {
            let stop_c = Arc::clone(&stop);
            let conns = Arc::clone(&total_ws_conns);
            let hdr_seen = Arc::clone(&gate_header_seen);
            handles.push(tokio::spawn(async move {
                while !stop_c.load(Ordering::Relaxed) {
                    let accept_res = tokio::select! {
                        r = gate_listener.accept() => r,
                        _ = tokio::time::sleep(Duration::from_millis(100)) => continue,
                    };
                    if let Ok((stream, _)) = accept_res {
                        let stop_s = Arc::clone(&stop_c);
                        let conns_s = Arc::clone(&conns);
                        let hdr_s = Arc::clone(&hdr_seen);
                        tokio::spawn(async move {
                            let cb = |req: &Request, resp: Response| -> Result<Response, ErrorResponse> {
                                if let Some(v) = req.headers().get("x-gate-size-decimal") {
                                    if v.to_str().ok() == Some("1") {
                                        hdr_s.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                                Ok(resp)
                            };
                            if let Ok(mut ws) = tokio_tungstenite::accept_hdr_async(stream, cb).await {
                                conns_s.fetch_add(1, Ordering::Relaxed);
                                // Consume subscribe frames
                                for _ in 0..3 {
                                    let _ = tokio::time::timeout(Duration::from_millis(200), ws.next()).await;
                                }
                                let now_ms = (now_wall_ns() / 1_000_000) as u64;
                                let snap = json!({
                                    "time": now_ms / 1000,
                                    "time_ms": now_ms,
                                    "channel": "futures.obu",
                                    "event": "update",
                                    "result": {
                                        "t": now_ms,
                                        "full": true,
                                        "s": "ob.BTC_USDT.50",
                                        "u": 500,
                                        "b": [["95000.0", "1.5"]],
                                        "a": [["95000.1", "1.2"]]
                                    }
                                })
                                .to_string();
                                if ws.send(Message::Text(snap)).await.is_err() {
                                    return;
                                }
                                let mut u: u64 = 500;
                                while !stop_s.load(Ordering::Relaxed) {
                                    u += 1;
                                    let now_ms = (now_wall_ns() / 1_000_000) as u64;
                                    let delta = json!({
                                        "time": now_ms / 1000,
                                        "time_ms": now_ms,
                                        "channel": "futures.obu",
                                        "event": "update",
                                        "result": {
                                            "t": now_ms,
                                            "full": false,
                                            "s": "ob.BTC_USDT.50",
                                            "U": u,
                                            "u": u,
                                            "b": [["95000.0", "1.6"]],
                                            "a": []
                                        }
                                    })
                                    .to_string();
                                    if ws.send(Message::Text(delta)).await.is_err() {
                                        break;
                                    }
                                    tokio::time::sleep(Duration::from_millis(frame_interval_ms)).await;
                                }
                            }
                        });
                    }
                }
            }));
        }

        Self {
            stop,
            bin_pub_port,
            bin_mkt_port,
            bin_rest_port,
            bybit_port,
            bitget_port,
            gate_port,
            total_ws_conns,
            gate_header_seen,
            handles,
            rest_thread: Some(rest_thread),
        }
    }

    fn write_config(&self, path: &Path, data_root: &Path) {
        let cfg = json!({
            "schema_version": 1,
            "spec_version": "v1.4",
            "mode": "offline_shutdown_regression_test",
            "deployment": {
                "aws_region": "ap-northeast-1",
                "aws_physical_az_id": "apne1-az4",
                "data_root": data_root.to_string_lossy(),
                "zstd_level": 3,
                "bounded_queue_capacity": 65536,
                "silence_watchdog_seconds": 3,
                "planned_rotation_utc": {
                    "A": "00:00:00",
                    "B": "12:00:00"
                }
            },
            "archive": {
                "rclone_remote": "gdrive_main:tokyo_l2_data",
                "local_retention_hours": 24,
                "disk_alert_pct": 70.0,
                "verify_remote_hash": true,
                "forbid_remote_overwrite_on_hash_mismatch": true
            },
            "meta_daemon": {
                "rtt_interval_seconds": 300,
                "chrony_interval_seconds": 60,
                "chrony_max_error_ms": 1.0,
                "dual_outage_alert_seconds": 5,
                "alert_webhook_env": "TOKYO_L2_ALERT_WEBHOOK_URL"
            },
            "exchanges": {
                "binance_futures": {
                    "enabled": true,
                    "public_ws_url": format!("ws://127.0.0.1:{}/public/stream", self.bin_pub_port),
                    "market_ws_url": format!("ws://127.0.0.1:{}/market/stream", self.bin_mkt_port),
                    "rest_exchange_info_url": format!("http://127.0.0.1:{}/fapi/v1/exchangeInfo", self.bin_rest_port),
                    "rest_depth_url": format!("http://127.0.0.1:{}/fapi/v1/depth", self.bin_rest_port),
                    "depth_snapshot_limit": 1000,
                    "symbols": ["BTCUSDT"],
                    "public_channels": ["bookTicker", "depth@100ms"],
                    "market_channels": ["aggTrade"]
                },
                "bybit_linear": {
                    "enabled": true,
                    "ws_url": format!("ws://127.0.0.1:{}/v5/public/linear", self.bybit_port),
                    "rest_instruments_url": format!("http://127.0.0.1:{}/v5/market/instruments-info", self.bin_rest_port),
                    "ping_interval_seconds": 20,
                    "symbols": ["BTCUSDT"],
                    "channels": ["orderbook.50", "orderbook.1", "publicTrade"]
                },
                "bitget_v3": {
                    "enabled": true,
                    "ws_url": format!("ws://127.0.0.1:{}/v3/ws/public", self.bitget_port),
                    "rest_instruments_url": format!("http://127.0.0.1:{}/api/v3/market/instruments", self.bin_rest_port),
                    "ping_interval_seconds": 25,
                    "symbols": ["BTCUSDT"],
                    "channels": ["books50", "books1", "publicTrade"]
                },
                "gate_futures": {
                    "enabled": true,
                    "ws_url": format!("ws://127.0.0.1:{}/v4/ws/usdt", self.gate_port),
                    "rest_contracts_url": format!("http://127.0.0.1:{}/api/v4/futures/usdt/contracts", self.bin_rest_port),
                    "required_headers": {
                        "X-Gate-Size-Decimal": "1"
                    },
                    "ping_interval_seconds": 15,
                    "symbols": ["BTC_USDT"],
                    "channels": ["futures.obu.50", "futures.book_ticker", "futures.trades"]
                }
            }
        });
        fs::write(path, serde_json::to_vec_pretty(&cfg).unwrap()).unwrap();
    }

    fn shutdown(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        for h in self.handles.drain(..) {
            h.abort();
        }
        if let Some(t) = self.rest_thread.take() {
            let _ = t.join();
        }
    }
}

#[test]
fn test_four_exchange_multimb_seal_integrity_and_partial_preservation() {
    let tmp = TempDir::new().unwrap();
    let data_root = tmp.path().join("data");
    let base_wall = 1_795_164_781_000_000_000_i64; // 2026-11-20T08:53:01Z

    let mut writers: BTreeMap<ExchangeId, HourlyTsvZstWriter> = BTreeMap::new();
    let counts = [
        (ExchangeId::BinanceFutures, 40_000u64),
        (ExchangeId::BybitLinear, 18_000u64),
        (ExchangeId::BitgetV3, 18_000u64),
        (ExchangeId::GateFutures, 35_285u64),
    ];

    for (ex, count) in counts {
        let mut w = HourlyTsvZstWriter::new(data_root.clone(), ex, "boot_multimb".to_string(), 3);
        for i in 0..count {
            let rec = CanonicalRecord {
                recv_wall_ns: base_wall + (i as i64) * 1_000_000,
                recv_mono_ns: i * 1_000_000,
                conn_id: ConnId::A,
                epoch: 1,
                rx_seq: i + 1,
                kind: RecordKind::Data,
                payload: format!(
                    r#"{{"ex":"{}","seq":{},"px":"95{:03}.{:02}","bids":[["95000.0","{}.123"],["94999.5","{}.456"]],"asks":[["95000.1","{}.789"],["95000.5","{}.321"]]}}"#,
                    ex.as_dir_name(),
                    i,
                    i % 1000,
                    i % 100,
                    (i * 7) % 97,
                    (i * 13) % 89,
                    (i * 17) % 83,
                    (i * 23) % 79
                ),
            };
            w.write_record(&rec).unwrap();
        }
        writers.insert(ex, w);
    }

    let seal_start = Instant::now();
    let mut sealed_order = Vec::new();
    for (ex, mut writer) in writers {
        let sealed = writer.seal_current().unwrap().expect("must seal");
        assert_eq!(sealed.manifest.exchange, ex.as_dir_name());
        let (bytes, sha256) = verify_zstd_and_sha256(&sealed.data_path).unwrap();
        assert_eq!(bytes, sealed.manifest.bytes);
        assert_eq!(sha256, sealed.manifest.sha256);
        assert!(bytes > 100_000, "expected realistic multi-hundred-KB compressed output");
        sealed_order.push(ex);
    }
    let elapsed = seal_start.elapsed();
    assert_eq!(
        sealed_order,
        vec![
            ExchangeId::BinanceFutures,
            ExchangeId::BybitLinear,
            ExchangeId::BitgetV3,
            ExchangeId::GateFutures,
        ],
        "BTreeMap must guarantee deterministic sealing order"
    );
    assert!(
        elapsed < Duration::from_secs(15),
        "4-exchange multi-MB seal took unexpectedly long: {:?}",
        elapsed
    );

    // Ensure a pre-existing .partial file in another fresh hour is NEVER overwritten or truncated.
    let partial_hour_wall = base_wall + 3_600_000_000_000_i64; // 09:53:01Z
    let existing_partial = data_root
        .join("raw/binance_futures/2026-11-20/binance_futures_20261120_09.tsv.zst.partial");
    let sentinel_bytes = b"PRESERVE_EXISTING_PARTIAL_FORENSIC_BYTES";
    fs::write(&existing_partial, sentinel_bytes).unwrap();

    let mut w_guard = HourlyTsvZstWriter::new(
        data_root.clone(),
        ExchangeId::BinanceFutures,
        "boot_preserve_check".to_string(),
        3,
    );
    let rec = CanonicalRecord {
        recv_wall_ns: partial_hour_wall,
        recv_mono_ns: 1,
        conn_id: ConnId::A,
        epoch: 1,
        rx_seq: 1,
        kind: RecordKind::Data,
        payload: r#"{"test":1}"#.to_string(),
    };
    let err = w_guard
        .write_record(&rec)
        .expect_err("writer MUST refuse to overwrite existing .partial file");
    assert!(err.contains("Refusing to overwrite existing unsealed partial file"));
    assert_eq!(
        fs::read(&existing_partial).unwrap(),
        sentinel_bytes,
        "existing .partial file bytes must remain completely untouched"
    );
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_live_binary_gnu_timeout_sigint_seals_all_four_exchanges() {
    let cluster = MockExchangeCluster::start(5).await;
    let tmp = TempDir::new().unwrap();
    let cfg_path = tmp.path().join("config.json");
    let data_root = tmp.path().join("data");
    cluster.write_config(&cfg_path, &data_root);

    let bin = env!("CARGO_BIN_EXE_tokyo-l2-collector");
    let output = tokio::task::spawn_blocking(move || {
        Command::new("timeout")
            .args([
                "--preserve-status",
                "--signal=INT",
                "--kill-after=30s",
                "4s",
                "env",
                "RUST_LOG=info",
                "TOKYO_L2_ALLOW_LOCALHOST_TEST_ENDPOINTS=1",
                bin,
                "collect",
                "--config",
                cfg_path.to_str().unwrap(),
                "--run-id",
                "test_gnu_timeout_sigint",
                "--allow-live-run",
            ])
            .output()
            .expect("failed to execute GNU timeout + tokyo-l2-collector")
    })
    .await
    .unwrap();

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "Expected exit 0 from GNU timeout --preserve-status --signal=INT, got {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        stdout,
        stderr
    );
    assert!(
        cluster.total_ws_conns.load(Ordering::Relaxed) >= 10,
        "Expected all 10 dual-active WS connections, saw {}",
        cluster.total_ws_conns.load(Ordering::Relaxed)
    );
    assert!(
        cluster.gate_header_seen.load(Ordering::Relaxed) >= 2,
        "Expected X-Gate-Size-Decimal: 1 on both Gate connections"
    );

    assert!(
        stderr.contains("Received shutdown trigger (reason=SIGINT)"),
        "Missing SIGINT shutdown log in stderr:\n{}",
        stderr
    );
    for ex in ["binance_futures", "bybit_linear", "bitget_v3", "gate_futures"] {
        assert!(
            stderr.contains(&format!("Sealed {}", ex)),
            "Missing Sealed {} log line in stderr:\n{}",
            ex,
            stderr
        );
    }

    assert_all_four_exchanges_sealed(&data_root, "SIGINT");
    cluster.shutdown();
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_live_binary_sigterm_seals_all_four_exchanges() {
    let cluster = MockExchangeCluster::start(5).await;
    let tmp = TempDir::new().unwrap();
    let cfg_path = tmp.path().join("config.json");
    let data_root = tmp.path().join("data");
    cluster.write_config(&cfg_path, &data_root);

    let bin = env!("CARGO_BIN_EXE_tokyo-l2-collector");
    let data_root_c = data_root.clone();
    let (status, stderr) = tokio::task::spawn_blocking(move || {
        let child = Command::new(bin)
            .args([
                "collect",
                "--config",
                cfg_path.to_str().unwrap(),
                "--run-id",
                "test_sigterm",
                "--allow-live-run",
            ])
            .env("RUST_LOG", "info")
            .env("TOKYO_L2_ALLOW_LOCALHOST_TEST_ENDPOINTS", "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn collector");
        let pid = child.id();

        assert!(
            wait_for_all_four_partials(&data_root_c, 256, Duration::from_secs(15)),
            "Timed out waiting for all 4 exchanges to create .partial files"
        );

        let kill_status = Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .status()
            .expect("send SIGTERM");
        assert!(kill_status.success());

        let out = child.wait_with_output().expect("wait collector");
        (out.status, String::from_utf8_lossy(&out.stderr).to_string())
    })
    .await
    .unwrap();

    assert!(
        status.success(),
        "Expected clean exit 0 on SIGTERM, got {:?}\nstderr:\n{}",
        status,
        stderr
    );
    assert!(stderr.contains("Received shutdown trigger (reason=SIGTERM)"));
    assert_all_four_exchanges_sealed(&data_root, "SIGTERM");
    cluster.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_live_binary_internal_duration_secs_seals_all_four_exchanges() {
    let cluster = MockExchangeCluster::start(5).await;
    let tmp = TempDir::new().unwrap();
    let cfg_path = tmp.path().join("config.json");
    let data_root = tmp.path().join("data");
    cluster.write_config(&cfg_path, &data_root);

    let bin = env!("CARGO_BIN_EXE_tokyo-l2-collector");
    let output = tokio::task::spawn_blocking(move || {
        Command::new(bin)
            .args([
                "collect",
                "--config",
                cfg_path.to_str().unwrap(),
                "--run-id",
                "test_duration_secs",
                "--allow-live-run",
                "--duration-secs",
                "4",
            ])
            .env("RUST_LOG", "info")
            .env("TOKYO_L2_ALLOW_LOCALHOST_TEST_ENDPOINTS", "1")
            .output()
            .expect("run collector with --duration-secs")
    })
    .await
    .unwrap();

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "Expected exit 0 from --duration-secs 4, got {:?}\nstderr:\n{}",
        output.status,
        stderr
    );
    assert!(stderr.contains("Received shutdown trigger (reason=duration_elapsed)"));
    assert_all_four_exchanges_sealed(&data_root, "duration_elapsed");
    cluster.shutdown();
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_live_binary_repeated_sigint_and_closed_stderr_still_seals_all_four() {
    // Simulates the exact EC2 hazard: high-rate 10-conn traffic + stderr pipe closing
    // when SIGINT hits (e.g. `tee` killed by process-group SIGINT) + duplicate SIGINTs.
    let cluster = MockExchangeCluster::start(2).await;
    let tmp = TempDir::new().unwrap();
    let cfg_path = tmp.path().join("config.json");
    let data_root = tmp.path().join("data");
    cluster.write_config(&cfg_path, &data_root);

    let bin = env!("CARGO_BIN_EXE_tokyo-l2-collector");
    let data_root_c = data_root.clone();
    let status = tokio::task::spawn_blocking(move || {
        let mut child = Command::new(bin)
            .args([
                "collect",
                "--config",
                cfg_path.to_str().unwrap(),
                "--run-id",
                "test_broken_pipe_dup_sigint",
                "--allow-live-run",
            ])
            .env("RUST_LOG", "info")
            .env("TOKYO_L2_ALLOW_LOCALHOST_TEST_ENDPOINTS", "1")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn collector");
        let pid = child.id();

        assert!(
            wait_for_all_four_partials(&data_root_c, 512, Duration::from_secs(15)),
            "Timed out waiting for all 4 exchanges to accumulate data"
        );

        // Close the stderr pipe reader BEFORE sending SIGINTs (simulates `tee` dying on SIGINT).
        drop(child.stderr.take());

        for _ in 0..3 {
            let _ = Command::new("kill")
                .args(["-INT", &pid.to_string()])
                .status();
            std::thread::sleep(Duration::from_millis(25));
        }

        child.wait().expect("wait collector exit")
    })
    .await
    .unwrap();

    assert!(
        status.success(),
        "Collector MUST still exit 0 and seal all 4 exchanges even when stderr pipe breaks and duplicate SIGINTs arrive; got {:?}",
        status
    );
    assert_all_four_exchanges_sealed(&data_root, "SIGINT");
    cluster.shutdown();
}
