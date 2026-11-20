use crate::causal_selector::{CausalSelector, SelectorAction};
use crate::config::CollectorConfig;
use crate::protocol::{binance::BinanceValidator, bitget::BitgetValidator, bybit::BybitValidator, gate::GateValidator};
use crate::storage::HourlyTsvZstWriter;
use crate::types::{ConnId, EndpointKind, ExchangeId, RawFrame};
use chrono::{Timelike, Utc};
use futures_util::{SinkExt, StreamExt};
use http::HeaderValue;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

fn wall_clock_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

#[derive(Debug)]
enum EngineEvent {
    Frame(RawFrame),
    Disconnect {
        endpoint: EndpointKind,
        conn_id: ConnId,
        wall_ns: i64,
        mono_ns: u64,
        reason: String,
    },
    Reconnect {
        endpoint: EndpointKind,
        conn_id: ConnId,
        epoch: u64,
        mono_ns: u64,
    },
    RestSnapshot {
        endpoint: EndpointKind,
        conn_id: ConnId,
        symbol: String,
        last_update_id: u64,
        raw_json: String,
        wall_ns: i64,
        mono_ns: u64,
    },
    LocalLoss {
        endpoint: EndpointKind,
        dropped: u64,
        wall_ns: i64,
        mono_ns: u64,
        reason: String,
    },
    PlannedRotation {
        target: ConnId,
        wall_ns: i64,
        mono_ns: u64,
    },
    WatchdogTick {
        wall_ns: i64,
        mono_ns: u64,
    },
}

#[derive(Debug, Clone)]
enum ConnCommand {
    Reconnect(String),
    Resubscribe(String),
}

pub async fn run_live_collector(config: CollectorConfig, run_id: String) -> Result<(), String> {
    let mono_origin = Instant::now();
    let boot_id = format!("{}_{}", run_id, wall_clock_ns());
    let data_root = PathBuf::from(&config.deployment.data_root);
    let cap = config.deployment.bounded_queue_capacity;
    let (tx, mut rx) = mpsc::channel::<EngineEvent>(cap);

    let mut selectors: HashMap<EndpointKind, CausalSelector> = HashMap::new();
    let mut writers: HashMap<ExchangeId, HourlyTsvZstWriter> = HashMap::new();
    let mut cmd_txs: HashMap<(EndpointKind, ConnId), mpsc::Sender<ConnCommand>> = HashMap::new();
    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| format!("http client build failed: {}", e))?;

    let mut endpoints_to_spawn: Vec<(EndpointKind, Vec<String>)> = Vec::new();
    if config.exchanges.binance_futures.enabled {
        let syms = config.exchanges.binance_futures.symbols.clone();
        endpoints_to_spawn.push((EndpointKind::BinancePublic, syms.clone()));
        endpoints_to_spawn.push((EndpointKind::BinanceMarket, syms));
    }
    if config.exchanges.bybit_linear.enabled {
        endpoints_to_spawn.push((
            EndpointKind::BybitPublic,
            config.exchanges.bybit_linear.symbols.clone(),
        ));
    }
    if config.exchanges.bitget_v3.enabled {
        endpoints_to_spawn.push((
            EndpointKind::BitgetPublic,
            config.exchanges.bitget_v3.symbols.clone(),
        ));
    }
    if config.exchanges.gate_futures.enabled {
        endpoints_to_spawn.push((
            EndpointKind::GatePublic,
            config.exchanges.gate_futures.symbols.clone(),
        ));
    }

    let cfg_arc = Arc::new(config.clone());

    for (ep, syms) in endpoints_to_spawn {
        let ex = ep.exchange();
        writers.entry(ex).or_insert_with(|| {
            HourlyTsvZstWriter::new(
                data_root.clone(),
                ex,
                boot_id.clone(),
                config.deployment.zstd_level,
            )
        });
        selectors.insert(
            ep,
            CausalSelector::new(
                ep,
                syms.clone(),
                config.deployment.silence_watchdog_seconds,
                config.meta_daemon.dual_outage_alert_seconds,
            ),
        );

        for conn_id in [ConnId::A, ConnId::B] {
            let (ctx, crx) = mpsc::channel::<ConnCommand>(64);
            cmd_txs.insert((ep, conn_id), ctx);
            let tx_c = tx.clone();
            let cfg_c = Arc::clone(&cfg_arc);
            let syms_c = syms.clone();
            let http_c = http_client.clone();
            tokio::spawn(async move {
                ws_connection_loop(ep, conn_id, syms_c, cfg_c, http_c, mono_origin, tx_c, crx)
                    .await;
            });
        }
    }

    // Periodic 1s watchdog + planned 00:00 UTC (A) / 12:00 UTC (B) rotation scheduler
    let tx_timer = tx.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        let mut last_rotated_a_day: Option<String> = None;
        let mut last_rotated_b_day: Option<String> = None;
        loop {
            interval.tick().await;
            let now_utc = Utc::now();
            let wall_ns = wall_clock_ns();
            let mono_ns = mono_origin.elapsed().as_nanos() as u64;
            let _ = tx_timer.try_send(EngineEvent::WatchdogTick { wall_ns, mono_ns });

            let day_key = now_utc.format("%Y-%m-%d").to_string();
            if now_utc.hour() == 0 && now_utc.minute() == 0 && now_utc.second() < 5 {
                if last_rotated_a_day.as_deref() != Some(&day_key) {
                    last_rotated_a_day = Some(day_key.clone());
                    let _ = tx_timer.try_send(EngineEvent::PlannedRotation {
                        target: ConnId::A,
                        wall_ns,
                        mono_ns,
                    });
                }
            }
            if now_utc.hour() == 12 && now_utc.minute() == 0 && now_utc.second() < 5 {
                if last_rotated_b_day.as_deref() != Some(&day_key) {
                    last_rotated_b_day = Some(day_key);
                    let _ = tx_timer.try_send(EngineEvent::PlannedRotation {
                        target: ConnId::B,
                        wall_ns,
                        mono_ns,
                    });
                }
            }
        }
    });

    info!(
        "tokyo-l2-collector live engine started boot_id={} data_root={}",
        boot_id,
        data_root.display()
    );

    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                info!("Received shutdown signal, sealing active hourly partitions...");
                break;
            }
            Some(ev) = rx.recv() => {
                let mut outputs: Vec<(EndpointKind, crate::causal_selector::SelectorOutput)> = Vec::new();
                match ev {
                    EngineEvent::Frame(f) => {
                        let ep = f.endpoint;
                        if let Some(sel) = selectors.get_mut(&ep) {
                            outputs.push((ep, sel.on_frame(f)));
                        }
                    }
                    EngineEvent::Disconnect { endpoint, conn_id, wall_ns, mono_ns, reason } => {
                        if let Some(sel) = selectors.get_mut(&endpoint) {
                            outputs.push((endpoint, sel.on_disconnect(conn_id, wall_ns, mono_ns, &reason)));
                        }
                    }
                    EngineEvent::Reconnect { endpoint, conn_id, epoch, mono_ns } => {
                        if let Some(sel) = selectors.get_mut(&endpoint) {
                            sel.on_reconnect(conn_id, epoch, mono_ns);
                        }
                    }
                    EngineEvent::RestSnapshot { endpoint, conn_id, symbol, last_update_id, raw_json, wall_ns, mono_ns } => {
                        if let Some(sel) = selectors.get_mut(&endpoint) {
                            outputs.push((endpoint, sel.on_rest_snapshot(conn_id, &symbol, last_update_id, &raw_json, wall_ns, mono_ns)));
                        }
                    }
                    EngineEvent::LocalLoss { endpoint, dropped, wall_ns, mono_ns, reason } => {
                        if let Some(sel) = selectors.get_mut(&endpoint) {
                            outputs.push((endpoint, sel.on_local_loss(dropped, wall_ns, mono_ns, &reason)));
                        }
                    }
                    EngineEvent::PlannedRotation { target, wall_ns, mono_ns } => {
                        for (&ep, sel) in selectors.iter_mut() {
                            outputs.push((ep, sel.on_planned_rotation(target, wall_ns, mono_ns)));
                        }
                    }
                    EngineEvent::WatchdogTick { wall_ns, mono_ns } => {
                        for (&ep, sel) in selectors.iter_mut() {
                            outputs.push((ep, sel.check_silence_watchdog(wall_ns, mono_ns, 30_000_000_000)));
                        }
                    }
                }

                for (ep, out) in outputs {
                    let ex = ep.exchange();
                    if let Some(writer) = writers.get_mut(&ex) {
                        for rec in &out.records {
                            if let Err(e) = writer.write_record(rec) {
                                error!("Disk write error on {}: {}", ex, e);
                            }
                        }
                    }
                    for act in out.actions {
                        match act {
                            SelectorAction::Reconnect { conn_id, reason } => {
                                if let Some(ctx) = cmd_txs.get(&(ep, conn_id)) {
                                    let _ = ctx.try_send(ConnCommand::Reconnect(reason));
                                }
                            }
                            SelectorAction::Resubscribe { conn_id, reason } => {
                                if let Some(ctx) = cmd_txs.get(&(ep, conn_id)) {
                                    let _ = ctx.try_send(ConnCommand::Resubscribe(reason));
                                }
                            }
                            SelectorAction::FetchRestSnapshot { conn_id, symbol } => {
                                if ep == EndpointKind::BinancePublic {
                                    let tx_s = tx.clone();
                                    let http_s = http_client.clone();
                                    let url = format!(
                                        "{}?symbol={}&limit={}",
                                        config.exchanges.binance_futures.rest_depth_url,
                                        symbol,
                                        config.exchanges.binance_futures.depth_snapshot_limit
                                    );
                                    tokio::spawn(async move {
                                        if let Ok(resp) = http_s.get(&url).send().await {
                                            if let Ok(text) = resp.text().await {
                                                let wall_ns = wall_clock_ns();
                                                let mono_ns = mono_origin.elapsed().as_nanos() as u64;
                                                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                                                    if let Some(id) = v.get("lastUpdateId").and_then(|x| x.as_u64()) {
                                                        let _ = tx_s.try_send(EngineEvent::RestSnapshot {
                                                            endpoint: ep,
                                                            conn_id,
                                                            symbol,
                                                            last_update_id: id,
                                                            raw_json: text,
                                                            wall_ns,
                                                            mono_ns,
                                                        });
                                                    }
                                                }
                                            }
                                        }
                                    });
                                }
                            }
                            SelectorAction::Alert { severity, message } => {
                                error!("[ALERT {}] {}", severity, message);
                            }
                        }
                    }
                }
            }
        }
    }

    for (ex, writer) in writers.iter_mut() {
        match writer.seal_current() {
            Ok(Some(s)) => info!(
                "Sealed {} -> {} (sha256={})",
                ex,
                s.data_path.display(),
                s.manifest.sha256
            ),
            Ok(None) => {}
            Err(e) => error!("Failed sealing {}: {}", ex, e),
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn ws_connection_loop(
    endpoint: EndpointKind,
    conn_id: ConnId,
    symbols: Vec<String>,
    config: Arc<CollectorConfig>,
    http_client: reqwest::Client,
    mono_origin: Instant,
    tx: mpsc::Sender<EngineEvent>,
    mut cmd_rx: mpsc::Receiver<ConnCommand>,
) {
    let mut epoch = 0u64;
    let mut backoff_ms = 500u64;

    loop {
        epoch += 1;
        let url = match endpoint {
            EndpointKind::BinancePublic => BinanceValidator::build_public_stream_url(
                &config.exchanges.binance_futures.public_ws_url,
                &symbols,
            ),
            EndpointKind::BinanceMarket => BinanceValidator::build_market_stream_url(
                &config.exchanges.binance_futures.market_ws_url,
                &symbols,
            ),
            EndpointKind::BybitPublic => config.exchanges.bybit_linear.ws_url.clone(),
            EndpointKind::BitgetPublic => config.exchanges.bitget_v3.ws_url.clone(),
            EndpointKind::GatePublic => config.exchanges.gate_futures.ws_url.clone(),
        };

        let mut req = match url.as_str().into_client_request() {
            Ok(r) => r,
            Err(e) => {
                error!("Invalid WS URL {}: {}", url, e);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        // Mandatory Gate X-Gate-Size-Decimal: 1 HTTP handshake header!
        if endpoint == EndpointKind::GatePublic {
            for (k, v) in &config.exchanges.gate_futures.required_headers {
                if let (Ok(hn), Ok(hv)) = (
                    http::header::HeaderName::from_bytes(k.as_bytes()),
                    HeaderValue::from_str(v),
                ) {
                    req.headers_mut().insert(hn, hv);
                }
            }
        }

        match tokio_tungstenite::connect_async(req).await {
            Ok((ws_stream, _)) => {
                backoff_ms = 500;
                let mono_ns = mono_origin.elapsed().as_nanos() as u64;
                let _ = tx.try_send(EngineEvent::Reconnect {
                    endpoint,
                    conn_id,
                    epoch,
                    mono_ns,
                });
                info!(
                    "Connected {} conn={} epoch={}",
                    endpoint.as_str(),
                    conn_id.as_str(),
                    epoch
                );

                let (mut write, mut read) = ws_stream.split();

                // Send subscriptions
                if let Err(e) = send_subscriptions(endpoint, &symbols, &mut write).await {
                    warn!("Subscribe write error on {} {:?}: {}", endpoint.as_str(), conn_id, e);
                    continue;
                }

                // For Binance /public, spawn initial REST 1000-level snapshot fetch for each symbol
                // AFTER WS stream is open and buffering deltas.
                if endpoint == EndpointKind::BinancePublic {
                    for sym in &symbols {
                        let tx_s = tx.clone();
                        let http_s = http_client.clone();
                        let sym_c = sym.clone();
                        let rest_url = format!(
                            "{}?symbol={}&limit={}",
                            config.exchanges.binance_futures.rest_depth_url,
                            sym,
                            config.exchanges.binance_futures.depth_snapshot_limit
                        );
                        tokio::spawn(async move {
                            tokio::time::sleep(Duration::from_millis(350)).await;
                            if let Ok(resp) = http_s.get(&rest_url).send().await {
                                if let Ok(text) = resp.text().await {
                                    let wall_ns = wall_clock_ns();
                                    let mono_ns = mono_origin.elapsed().as_nanos() as u64;
                                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                                        if let Some(id) = v.get("lastUpdateId").and_then(|x| x.as_u64()) {
                                            let _ = tx_s.try_send(EngineEvent::RestSnapshot {
                                                endpoint,
                                                conn_id,
                                                symbol: sym_c,
                                                last_update_id: id,
                                                raw_json: text,
                                                wall_ns,
                                                mono_ns,
                                            });
                                        }
                                    }
                                }
                            }
                        });
                    }
                }

                let ping_secs = match endpoint {
                    EndpointKind::BybitPublic => config.exchanges.bybit_linear.ping_interval_seconds,
                    EndpointKind::BitgetPublic => config.exchanges.bitget_v3.ping_interval_seconds,
                    EndpointKind::GatePublic => config.exchanges.gate_futures.ping_interval_seconds,
                    _ => 30,
                };
                let mut ping_timer = tokio::time::interval(Duration::from_secs(ping_secs.max(5)));
                let disconnect_reason: String;

                loop {
                    tokio::select! {
                        _ = ping_timer.tick() => {
                            let ping_res = match endpoint {
                                EndpointKind::BybitPublic => {
                                    write.send(Message::Text(BybitValidator::build_ping_message().to_string())).await
                                }
                                EndpointKind::BitgetPublic => {
                                    write.send(Message::Text(BitgetValidator::build_ping_text().to_string())).await
                                }
                                EndpointKind::GatePublic => {
                                    let now_s = Utc::now().timestamp();
                                    write.send(Message::Text(GateValidator::build_ping_message(now_s).to_string())).await
                                }
                                _ => write.send(Message::Ping(Vec::new())).await,
                            };
                            if let Err(e) = ping_res {
                                disconnect_reason = format!("ping_error: {}", e);
                                break;
                            }
                        }
                        Some(cmd) = cmd_rx.recv() => {
                            match cmd {
                                ConnCommand::Reconnect(r) => {
                                    disconnect_reason = format!("cmd_reconnect: {}", r);
                                    break;
                                }
                                ConnCommand::Resubscribe(r) => {
                                    warn!(
                                        "Resubscribing {} conn={} reason={}",
                                        endpoint.as_str(),
                                        conn_id.as_str(),
                                        r
                                    );
                                    if let Err(e) = send_subscriptions(endpoint, &symbols, &mut write).await {
                                        disconnect_reason = format!("resubscribe_error: {}", e);
                                        break;
                                    }
                                }
                            }
                        }
                        maybe_msg = read.next() => {
                            // HOT PATH: stamp recv_wall_ns and recv_mono_ns BEFORE any JSON parsing!
                            let wall_ns = wall_clock_ns();
                            let mono_ns = mono_origin.elapsed().as_nanos() as u64;
                            match maybe_msg {
                                Some(Ok(Message::Text(text))) => {
                                    let frame = RawFrame {
                                        recv_wall_ns: wall_ns,
                                        recv_mono_ns: mono_ns,
                                        exchange: endpoint.exchange(),
                                        endpoint,
                                        conn_id,
                                        epoch,
                                        payload: text,
                                    };
                                    if tx.try_send(EngineEvent::Frame(frame)).is_err() {
                                        // Never silently drop: force LOCAL_LOSS event notification.
                                        let _ = tx.send(EngineEvent::LocalLoss {
                                            endpoint,
                                            dropped: 1,
                                            wall_ns,
                                            mono_ns,
                                            reason: "bounded_channel_full".to_string(),
                                        }).await;
                                    }
                                }
                                Some(Ok(Message::Ping(p))) => {
                                    let _ = write.send(Message::Pong(p)).await;
                                }
                                Some(Ok(Message::Pong(_))) => {}
                                Some(Ok(Message::Close(_))) => {
                                    disconnect_reason = "ws_close_frame".to_string();
                                    break;
                                }
                                Some(Err(e)) => {
                                    disconnect_reason = format!("ws_read_error: {}", e);
                                    break;
                                }
                                None => {
                                    disconnect_reason = "ws_stream_ended".to_string();
                                    break;
                                }
                                _ => {}
                            }
                        }
                    }
                }

                let wall_ns = wall_clock_ns();
                let mono_ns = mono_origin.elapsed().as_nanos() as u64;
                let _ = tx
                    .send(EngineEvent::Disconnect {
                        endpoint,
                        conn_id,
                        wall_ns,
                        mono_ns,
                        reason: disconnect_reason,
                    })
                    .await;
            }
            Err(e) => {
                warn!(
                    "WS connect failed {} conn={}: {} (backoff {}ms)",
                    endpoint.as_str(),
                    conn_id.as_str(),
                    e,
                    backoff_ms
                );
            }
        }

        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        backoff_ms = (backoff_ms * 2).min(15_000);
    }
}

async fn send_subscriptions<S>(
    endpoint: EndpointKind,
    symbols: &[String],
    write: &mut S,
) -> Result<(), String>
where
    S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    match endpoint {
        EndpointKind::BinancePublic | EndpointKind::BinanceMarket => {
            // Streams already encoded in combined /stream?streams=... URL.
            Ok(())
        }
        EndpointKind::BybitPublic => {
            let msg = BybitValidator::build_subscribe_message(symbols).to_string();
            write
                .send(Message::Text(msg))
                .await
                .map_err(|e| e.to_string())
        }
        EndpointKind::BitgetPublic => {
            let msg = BitgetValidator::build_subscribe_message(symbols).to_string();
            write
                .send(Message::Text(msg))
                .await
                .map_err(|e| e.to_string())
        }
        EndpointKind::GatePublic => {
            let now_s = Utc::now().timestamp();
            for msg in GateValidator::build_subscribe_messages(symbols, now_s) {
                write
                    .send(Message::Text(msg.to_string()))
                    .await
                    .map_err(|e| e.to_string())?;
            }
            Ok(())
        }
    }
}
