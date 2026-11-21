use crate::binance_snapshot::{
    classify_http_response, BinanceSnapshotScheduler, ResponseDisposition,
    ScheduledSnapshotRequest, SnapshotAttemptOutcome, SnapshotSchedulerConfig,
};
use crate::causal_selector::{CausalSelector, SelectorAction};
use crate::config::CollectorConfig;
use crate::protocol::{
    binance::BinanceValidator, bitget::BitgetValidator, bybit::BybitValidator,
    gate::GateValidator,
};
use crate::storage::{HourlyTsvZstWriter, WriterOptions};
use crate::types::{ConnId, EndpointKind, ExchangeId, RawFrame};
use chrono::{Timelike, Utc};
use futures_util::{SinkExt, StreamExt};
use http::HeaderValue;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Write as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, watch, Semaphore};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

fn wall_clock_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

fn wall_clock_ms() -> i64 {
    wall_clock_ns() / 1_000_000
}

fn flush_stderr() {
    let _ = std::io::stderr().flush();
}

#[derive(Debug)]
enum EngineEvent {
    Frame(RawFrame),
    Disconnect {
        endpoint: EndpointKind,
        conn_id: ConnId,
        epoch: u64,
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
    Subscribed {
        endpoint: EndpointKind,
        conn_id: ConnId,
        epoch: u64,
    },
    RestSnapshotAttempt {
        request_id: u64,
        endpoint: EndpointKind,
        conn_id: ConnId,
        epoch: u64,
        symbol: String,
        outcome: SnapshotAttemptOutcome,
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

#[derive(Debug)]
struct ConnCommand {
    epoch: u64,
    action: ConnCommandAction,
}

#[derive(Debug)]
enum ConnCommandAction {
    Reconnect(String),
    Resubscribe(String),
}

async fn next_command_for_epoch(
    commands: &mut mpsc::Receiver<ConnCommand>,
    epoch: u64,
) -> Option<ConnCommandAction> {
    while let Some(command) = commands.recv().await {
        if command.epoch == epoch {
            return Some(command.action);
        }
    }
    None
}

fn active_selector_for_epoch(
    selectors: &mut HashMap<EndpointKind, CausalSelector>,
    endpoint: EndpointKind,
    conn_id: ConnId,
    epoch: u64,
) -> Option<&mut CausalSelector> {
    selectors
        .get_mut(&endpoint)
        .filter(|selector| selector.connected_epoch(conn_id) == Some(epoch))
}

const INITIAL_RETRY_MS: u64 = 500;
const MAX_RETRY_MS: u64 = 15_000;
const STABLE_SESSION: Duration = Duration::from_secs(30);
const PING_TIMEOUT_NS: u64 = 30_000_000_000;

fn next_retry_delay(backoff_ms: &mut u64, subscribed_uptime: Option<Duration>) -> Duration {
    if subscribed_uptime.is_some_and(|uptime| uptime >= STABLE_SESSION) {
        *backoff_ms = INITIAL_RETRY_MS;
    }
    let delay = Duration::from_millis(*backoff_ms);
    *backoff_ms = (*backoff_ms * 2).min(MAX_RETRY_MS);
    delay
}

async fn send_lifecycle_event(
    tx: &mpsc::Sender<EngineEvent>,
    stop: &mut watch::Receiver<bool>,
    event: EngineEvent,
) -> bool {
    tokio::select! {
        biased;
        _ = stop.wait_for(|requested| *requested) => false,
        result = tx.send(event) => result.is_ok(),
    }
}

fn classify_stream_category(channel: &str) -> &'static str {
    match channel {
        "bookTicker" | "orderbook.1" | "books1" | "futures.book_ticker" => "continuous_quote",
        "depth@100ms" | "orderbook.50" | "books50" | "futures.obu" | "futures.obu.50" => "depth",
        "aggTrade" | "publicTrade" | "futures.trades" => "event_trade",
        _ => "other",
    }
}

fn owning_endpoint(exchange: &str, channel: &str) -> EndpointKind {
    match (exchange, channel) {
        ("binance_futures", "aggTrade") => EndpointKind::BinanceMarket,
        ("binance_futures", _) => EndpointKind::BinancePublic,
        ("bybit_linear", _) => EndpointKind::BybitPublic,
        ("bitget_v3", _) => EndpointKind::BitgetPublic,
        _ => EndpointKind::GatePublic,
    }
}

#[derive(Default)]
struct StreamTelemetryRow {
    endpoint: Option<EndpointKind>,
    last_received_wall_ns: Option<i64>,
    received_frames: u64,
    last_buffered_wall_ns: Option<i64>,
    buffered_wait_anchor_frames: u64,
    last_written_wall_ns: Option<i64>,
    written_frames: u64,
    /// Per `(conn_id, epoch)` explicit subscription ACK wall_ns.
    ack_epochs: HashMap<(ConnId, u64), i64>,
    /// Per `(conn_id, epoch)` valid market/buffered frame observed wall_ns.
    observed_epochs: HashMap<(ConnId, u64), i64>,
}

#[derive(Default)]
struct StreamActivity {
    boot_id: String,
    streams: BTreeMap<(String, String, String), StreamTelemetryRow>,
    subscribe_requested_epochs: HashMap<(EndpointKind, ConnId), u64>,
    last_publish_mono_ns: u64,
    pending_publish: Option<tokio::task::JoinHandle<()>>,
    max_queue_depth: usize,
}

impl StreamActivity {
    fn from_config(config: &CollectorConfig, boot_id: String) -> Self {
        let mut s = Self {
            boot_id,
            ..Self::default()
        };
        let mut register = |exchange: &str, symbols: &[String], channels: &[String]| {
            for sym in symbols {
                let sym_u = sym.to_uppercase();
                for ch in channels {
                    let norm_ch = if ch == "futures.obu.50" {
                        "futures.obu".to_string()
                    } else {
                        ch.clone()
                    };
                    let ep = owning_endpoint(exchange, &norm_ch);
                    let row = s
                        .streams
                        .entry((exchange.to_string(), sym_u.clone(), norm_ch))
                        .or_default();
                    row.endpoint = Some(ep);
                }
            }
        };
        if config.exchanges.binance_futures.enabled {
            register(
                "binance_futures",
                &config.exchanges.binance_futures.symbols,
                &config.exchanges.binance_futures.public_channels,
            );
            register(
                "binance_futures",
                &config.exchanges.binance_futures.symbols,
                &config.exchanges.binance_futures.market_channels,
            );
        }
        if config.exchanges.bybit_linear.enabled {
            register(
                "bybit_linear",
                &config.exchanges.bybit_linear.symbols,
                &config.exchanges.bybit_linear.channels,
            );
        }
        if config.exchanges.bitget_v3.enabled {
            register(
                "bitget_v3",
                &config.exchanges.bitget_v3.symbols,
                &config.exchanges.bitget_v3.channels,
            );
        }
        if config.exchanges.gate_futures.enabled {
            register(
                "gate_futures",
                &config.exchanges.gate_futures.symbols,
                &config.exchanges.gate_futures.channels,
            );
        }
        s
    }

    fn record_subscribe_requested(&mut self, endpoint: EndpointKind, conn_id: ConnId, epoch: u64) {
        self.subscribe_requested_epochs
            .insert((endpoint, conn_id), epoch);
    }

    fn clear_conn_epoch(&mut self, endpoint: EndpointKind, conn_id: ConnId, epoch: u64) {
        if self.subscribe_requested_epochs.get(&(endpoint, conn_id)) == Some(&epoch) {
            self.subscribe_requested_epochs.remove(&(endpoint, conn_id));
        }
        for row in self.streams.values_mut() {
            if row.endpoint == Some(endpoint) {
                row.ack_epochs.remove(&(conn_id, epoch));
                row.observed_epochs.remove(&(conn_id, epoch));
            }
        }
    }

    fn record_subscription_ack(
        &mut self,
        ex: ExchangeId,
        endpoint: EndpointKind,
        conn_id: ConnId,
        epoch: u64,
        symbol: String,
        channel: String,
        wall_ns: i64,
    ) {
        let row = self
            .streams
            .entry((ex.as_dir_name().to_string(), symbol.to_uppercase(), channel))
            .or_default();
        row.endpoint = Some(endpoint);
        row.ack_epochs.insert((conn_id, epoch), wall_ns);
    }

    fn record_received(
        &mut self,
        ex: ExchangeId,
        endpoint: EndpointKind,
        conn_id: ConnId,
        epoch: u64,
        symbol: String,
        channel: String,
        wall_ns: i64,
    ) {
        let row = self
            .streams
            .entry((ex.as_dir_name().to_string(), symbol.to_uppercase(), channel))
            .or_default();
        row.endpoint = Some(endpoint);
        row.last_received_wall_ns = Some(
            row.last_received_wall_ns
                .map_or(wall_ns, |prev| prev.max(wall_ns)),
        );
        row.received_frames = row.received_frames.saturating_add(1);
        row.observed_epochs.insert((conn_id, epoch), wall_ns);
    }

    fn record_buffered_wait_anchor(
        &mut self,
        ex: ExchangeId,
        endpoint: EndpointKind,
        conn_id: ConnId,
        epoch: u64,
        symbol: String,
        channel: String,
        wall_ns: i64,
    ) {
        let row = self
            .streams
            .entry((ex.as_dir_name().to_string(), symbol.to_uppercase(), channel))
            .or_default();
        row.endpoint = Some(endpoint);
        row.last_buffered_wall_ns = Some(
            row.last_buffered_wall_ns
                .map_or(wall_ns, |prev| prev.max(wall_ns)),
        );
        row.buffered_wait_anchor_frames = row.buffered_wait_anchor_frames.saturating_add(1);
        row.observed_epochs.insert((conn_id, epoch), wall_ns);
    }

    fn record_written(&mut self, ex: ExchangeId, symbol: String, channel: String, wall_ns: i64) {
        let ex_str = ex.as_dir_name().to_string();
        let ep = owning_endpoint(&ex_str, &channel);
        let row = self
            .streams
            .entry((ex_str, symbol.to_uppercase(), channel))
            .or_default();
        row.endpoint.get_or_insert(ep);
        row.last_written_wall_ns = Some(
            row.last_written_wall_ns
                .map_or(wall_ns, |prev| prev.max(wall_ns)),
        );
        row.written_frames = row.written_frames.saturating_add(1);
    }

    fn build_payload(
        &mut self,
        wall_ns: i64,
        mono_ns: u64,
        selectors: &HashMap<EndpointKind, CausalSelector>,
    ) -> serde_json::Value {
        let mut endpoints_json = serde_json::Map::new();
        for (&ep, sel) in selectors {
            endpoints_json.insert(
                ep.as_str().to_string(),
                serde_json::json!({
                    "exchange": ep.exchange().as_dir_name(),
                    "primary": sel.primary().as_str(),
                    "in_gap": sel.in_gap(),
                    "ws_connected": sel.any_connected(),
                    "protocol_healthy": sel.protocol_healthy(mono_ns, PING_TIMEOUT_NS),
                    "conn_a": {
                        "connected": sel.is_connected(ConnId::A),
                        "epoch": sel.conn_epoch(ConnId::A),
                        "state": format!("{:?}", sel.conn_state(ConnId::A)),
                        "last_msg_age_ms": if sel.last_msg_mono_ns(ConnId::A) > 0 {
                            Some(mono_ns.saturating_sub(sel.last_msg_mono_ns(ConnId::A)) / 1_000_000)
                        } else { None },
                        "last_pong_age_ms": if sel.last_pong_mono_ns(ConnId::A) > 0 {
                            Some(mono_ns.saturating_sub(sel.last_pong_mono_ns(ConnId::A)) / 1_000_000)
                        } else { None },
                    },
                    "conn_b": {
                        "connected": sel.is_connected(ConnId::B),
                        "epoch": sel.conn_epoch(ConnId::B),
                        "state": format!("{:?}", sel.conn_state(ConnId::B)),
                        "last_msg_age_ms": if sel.last_msg_mono_ns(ConnId::B) > 0 {
                            Some(mono_ns.saturating_sub(sel.last_msg_mono_ns(ConnId::B)) / 1_000_000)
                        } else { None },
                        "last_pong_age_ms": if sel.last_pong_mono_ns(ConnId::B) > 0 {
                            Some(mono_ns.saturating_sub(sel.last_pong_mono_ns(ConnId::B)) / 1_000_000)
                        } else { None },
                    },
                }),
            );
        }

        let streams: Vec<_> = self
            .streams
            .iter()
            .map(|((exchange, symbol, channel), row)| {
                let ep = row
                    .endpoint
                    .unwrap_or_else(|| owning_endpoint(exchange, channel));
                let sel = selectors.get(&ep);
                let ws_connected = sel.is_some_and(|s| s.any_connected());
                let protocol_healthy =
                    sel.is_some_and(|s| s.protocol_healthy(mono_ns, PING_TIMEOUT_NS));
                let active_conn_epochs: Vec<(ConnId, u64)> = sel
                    .map(|s| {
                        [ConnId::A, ConnId::B]
                            .into_iter()
                            .filter_map(|c| s.connected_epoch(c).map(|e| (c, e)))
                            .collect()
                    })
                    .unwrap_or_default();

                let has_explicit_ack = active_conn_epochs
                    .iter()
                    .any(|ce| row.ack_epochs.contains_key(ce));
                let has_frame_observed = active_conn_epochs
                    .iter()
                    .any(|ce| row.observed_epochs.contains_key(ce));
                let has_subscribe_sent = active_conn_epochs
                    .iter()
                    .any(|(c, e)| self.subscribe_requested_epochs.get(&(ep, *c)) == Some(e));

                let (subscription_evidenced, subscription_evidence) = if has_explicit_ack {
                    (true, "explicit_ack")
                } else if has_frame_observed {
                    (true, "frame_observed")
                } else if ws_connected
                    && matches!(ep, EndpointKind::BinancePublic | EndpointKind::BinanceMarket)
                    && has_subscribe_sent
                {
                    (false, "combined_url_requested")
                } else if ws_connected && has_subscribe_sent {
                    (false, "subscribe_sent_unacked")
                } else {
                    (false, "none")
                };

                let depth_state = if channel == "depth@100ms" {
                    sel.and_then(|s| s.symbol_depth_state(s.primary(), symbol))
                        .map(|st| format!("{:?}", st))
                } else {
                    None
                };

                let total_received_frames = row
                    .received_frames
                    .saturating_add(row.buffered_wait_anchor_frames);
                let last_received_wall_ns = match (row.last_received_wall_ns, row.last_buffered_wall_ns) {
                    (Some(a), Some(b)) => Some(a.max(b)),
                    (Some(a), None) => Some(a),
                    (None, Some(b)) => Some(b),
                    (None, None) => None,
                };

                serde_json::json!({
                    "exchange": exchange,
                    "symbol": symbol,
                    "channel": channel,
                    "category": classify_stream_category(channel),
                    "endpoint": ep.as_str(),
                    "ws_connected": ws_connected,
                    "protocol_healthy": protocol_healthy,
                    "subscription_evidenced": subscription_evidenced,
                    "subscription_evidence": subscription_evidence,
                    "received_valid_market_data": total_received_frames > 0,
                    "last_received_wall_ns": last_received_wall_ns,
                    "received_frames": total_received_frames,
                    "buffered_wait_anchor_frames": row.buffered_wait_anchor_frames,
                    "last_buffered_wall_ns": row.last_buffered_wall_ns,
                    "written": row.written_frames > 0,
                    "last_written_wall_ns": row.last_written_wall_ns.unwrap_or(0),
                    "written_frames": row.written_frames,
                    "depth_state": depth_state,
                })
            })
            .collect();

        let payload = serde_json::json!({
            "schema_version": 2,
            "boot_id": self.boot_id,
            "sampled_wall_ns": wall_ns,
            "max_sampled_queue_depth": self.max_queue_depth,
            "endpoints": endpoints_json,
            "streams": streams,
        });
        self.max_queue_depth = 0;
        payload
    }

    fn publish(
        &mut self,
        data_root: &std::path::Path,
        wall_ns: i64,
        mono_ns: u64,
        queue_depth: usize,
        selectors: &HashMap<EndpointKind, CausalSelector>,
    ) {
        self.max_queue_depth = self.max_queue_depth.max(queue_depth);
        if self.last_publish_mono_ns > 0
            && (mono_ns.saturating_sub(self.last_publish_mono_ns) < 5_000_000_000
                || self
                    .pending_publish
                    .as_ref()
                    .is_some_and(|task| !task.is_finished()))
        {
            return;
        }
        self.last_publish_mono_ns = mono_ns;
        let directory = data_root.join("meta");
        let payload = self.build_payload(wall_ns, mono_ns, selectors);
        self.pending_publish = Some(tokio::task::spawn_blocking(move || {
            let path = directory.join("stream_activity.json");
            let temporary = directory.join("stream_activity.json.partial");
            let result = std::fs::create_dir_all(&directory)
                .and_then(|_| std::fs::write(&temporary, payload.to_string()))
                .and_then(|_| std::fs::rename(temporary, path));
            if let Err(error) = result {
                warn!("Could not publish stream activity: {error}");
            }
        }));
    }

    async fn flush_final(
        &mut self,
        data_root: &std::path::Path,
        wall_ns: i64,
        mono_ns: u64,
        selectors: &HashMap<EndpointKind, CausalSelector>,
    ) {
        if let Some(prev) = self.pending_publish.take() {
            let _ = prev.await;
        }
        let directory = data_root.join("meta");
        let payload = self.build_payload(wall_ns, mono_ns, selectors);
        let _ = tokio::task::spawn_blocking(move || {
            let path = directory.join("stream_activity.json");
            let temporary = directory.join("stream_activity.json.partial");
            let result = std::fs::create_dir_all(&directory)
                .and_then(|_| std::fs::write(&temporary, payload.to_string()))
                .and_then(|_| std::fs::rename(temporary, path));
            if let Err(error) = result {
                warn!("Could not publish final stream activity: {error}");
            }
        })
        .await;
    }
}

async fn fetch_binance_snapshot_once(
    http: reqwest::Client,
    url: String,
    attempt_timeout: Duration,
    mut stop_rx: watch::Receiver<bool>,
) -> Option<SnapshotAttemptOutcome> {
    if *stop_rx.borrow() {
        return None;
    }
    let fut = async move {
        let resp = match http.get(&url).send().await {
            Ok(r) => r,
            Err(e) => {
                if e.is_timeout() {
                    return SnapshotAttemptOutcome::Timeout;
                }
                return SnapshotAttemptOutcome::TransportError {
                    message: e.to_string(),
                };
            }
        };
        let status = resp.status().as_u16();
        let headers = resp.headers().clone();
        let body = match resp.text().await {
            Ok(t) => t,
            Err(e) => {
                if e.is_timeout() {
                    return SnapshotAttemptOutcome::Timeout;
                }
                return SnapshotAttemptOutcome::TransportError {
                    message: format!("body read error: {}", e),
                };
            }
        };
        classify_http_response(status, &headers, body, wall_clock_ms())
    };

    tokio::select! {
        biased;
        _ = stop_rx.wait_for(|requested| *requested) => None,
        timed = tokio::time::timeout(attempt_timeout, fut) => {
            match timed {
                Ok(outcome) => Some(outcome),
                Err(_) => Some(SnapshotAttemptOutcome::Timeout),
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn dispatch_binance_snapshots(
    scheduler: &mut BinanceSnapshotScheduler,
    snapshot_tasks: &mut HashMap<u64, tokio::task::JoinHandle<()>>,
    selectors: &HashMap<EndpointKind, CausalSelector>,
    config: &CollectorConfig,
    mono_origin: Instant,
    tx: &mpsc::Sender<EngineEvent>,
    http_client: &reqwest::Client,
    stop_rx: &watch::Receiver<bool>,
) {
    if !config.exchanges.binance_futures.enabled {
        return;
    }
    let now_mono = mono_origin.elapsed();
    let preferred_primary = selectors
        .get(&EndpointKind::BinancePublic)
        .map(|s| s.primary());
    let requests: Vec<ScheduledSnapshotRequest> =
        scheduler.poll_dispatch(now_mono, preferred_primary);
    let attempt_timeout = scheduler.config().attempt_timeout;

    for req in requests {
        let url = format!(
            "{}?symbol={}&limit={}",
            config.exchanges.binance_futures.rest_depth_url,
            req.symbol,
            config.exchanges.binance_futures.depth_snapshot_limit
        );
        let http_s = http_client.clone();
        let tx_s = tx.clone();
        let mut stop_s = stop_rx.clone();
        let req_id = req.request_id;
        let conn_id = req.conn_id;
        let epoch = req.epoch;
        let symbol = req.symbol.clone();

        let handle = tokio::spawn(async move {
            let Some(outcome) =
                fetch_binance_snapshot_once(http_s, url, attempt_timeout, stop_s.clone()).await
            else {
                return;
            };
            let wall_ns = wall_clock_ns();
            let mono_ns = mono_origin.elapsed().as_nanos() as u64;
            let _ = send_lifecycle_event(
                &tx_s,
                &mut stop_s,
                EngineEvent::RestSnapshotAttempt {
                    request_id: req_id,
                    endpoint: EndpointKind::BinancePublic,
                    conn_id,
                    epoch,
                    symbol,
                    outcome,
                    wall_ns,
                    mono_ns,
                },
            )
            .await;
        });
        snapshot_tasks.insert(req_id, handle);
    }
    snapshot_tasks.retain(|_, h| !h.is_finished());
}

fn abort_snapshot_tasks(
    snapshot_tasks: &mut HashMap<u64, tokio::task::JoinHandle<()>>,
    ids: impl IntoIterator<Item = u64>,
) {
    for id in ids {
        if let Some(handle) = snapshot_tasks.remove(&id) {
            handle.abort();
        }
    }
}

pub async fn run_live_collector(
    config: CollectorConfig,
    run_id: String,
    duration_secs: Option<u64>,
) -> Result<(), String> {
    // Register OS shutdown signal listeners BEFORE spawning any network tasks and keep
    // them alive through the entire sealing phase so SIGINT/SIGTERM never revert to SIG_DFL.
    #[cfg(unix)]
    let (mut sigint, mut sigterm) = {
        use tokio::signal::unix::{signal, SignalKind};
        let s_int = signal(SignalKind::interrupt())
            .map_err(|e| format!("failed to install SIGINT handler: {}", e))?;
        let s_term = signal(SignalKind::terminate())
            .map_err(|e| format!("failed to install SIGTERM handler: {}", e))?;
        (s_int, s_term)
    };

    let mono_origin = Instant::now();
    let process_start_wall_ns = wall_clock_ns();
    let boot_id = format!("{}_{}", run_id, process_start_wall_ns);
    let data_root = PathBuf::from(&config.deployment.data_root);
    let cap = config.deployment.bounded_queue_capacity;
    let (tx, mut rx) = mpsc::channel::<EngineEvent>(cap);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let mut selectors: HashMap<EndpointKind, CausalSelector> = HashMap::new();
    let mut writers: BTreeMap<ExchangeId, HourlyTsvZstWriter> = BTreeMap::new();
    let seal_limit = Arc::new(Semaphore::new(1));
    let mut activity = StreamActivity::from_config(&config, boot_id.clone());
    let mut cmd_txs: HashMap<(EndpointKind, ConnId), mpsc::Sender<ConnCommand>> = HashMap::new();
    let mut bg_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    let mut binance_scheduler = BinanceSnapshotScheduler::new(SnapshotSchedulerConfig::for_limit(
        config.exchanges.binance_futures.depth_snapshot_limit,
    ));
    let mut snapshot_tasks: HashMap<u64, tokio::task::JoinHandle<()>> = HashMap::new();
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
    let writer_opts = WriterOptions::from_config(
        &config,
        Some(run_id.clone()),
        true,
        Some(process_start_wall_ns),
    );

    for (ep, syms) in endpoints_to_spawn {
        let ex = ep.exchange();
        writers.entry(ex).or_insert_with(|| {
            let mut writer = HourlyTsvZstWriter::with_options(
                data_root.clone(),
                ex,
                boot_id.clone(),
                config.deployment.zstd_level,
                writer_opts.clone(),
            );
            writer.enable_background_sealing(Arc::clone(&seal_limit));
            writer
        });
        let mut selector = CausalSelector::new(
            ep,
            syms.clone(),
            config.deployment.silence_watchdog_seconds,
            config.meta_daemon.dual_outage_alert_seconds,
        );
        selector.start_disconnected();
        selectors.insert(ep, selector);

        for conn_id in [ConnId::A, ConnId::B] {
            let (ctx, crx) = mpsc::channel::<ConnCommand>(64);
            cmd_txs.insert((ep, conn_id), ctx);
            let tx_c = tx.clone();
            let cfg_c = Arc::clone(&cfg_arc);
            let syms_c = syms.clone();
            let stop_rx = shutdown_rx.clone();
            bg_handles.push(tokio::spawn(async move {
                ws_connection_loop(
                    ep,
                    conn_id,
                    syms_c,
                    cfg_c,
                    mono_origin,
                    tx_c,
                    crx,
                    stop_rx,
                )
                .await;
            }));
        }
    }

    // Periodic 1s watchdog + planned 00:00 UTC (A) / 12:00 UTC (B) rotation scheduler
    let tx_timer = tx.clone();
    let mut timer_stop = shutdown_rx.clone();
    bg_handles.push(tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        let mut last_rotated_a_day: Option<String> = None;
        let mut last_rotated_b_day: Option<String> = None;
        loop {
            tokio::select! {
                biased;
                _ = timer_stop.changed() => {
                    if *timer_stop.borrow() {
                        break;
                    }
                }
                _ = interval.tick() => {
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
            }
        }
    }));

    info!(
        "tokyo-l2-collector live engine started boot_id={} data_root={} duration_secs={:?}",
        boot_id,
        data_root.display(),
        duration_secs
    );
    flush_stderr();

    let duration_sleep = async {
        match duration_secs {
            Some(secs) => tokio::time::sleep(Duration::from_secs(secs)).await,
            None => std::future::pending::<()>().await,
        }
    };
    tokio::pin!(duration_sleep);

    let shutdown_reason: &'static str;

    #[cfg(unix)]
    loop {
        let now_mono = mono_origin.elapsed();
        let next_snap_sleep = binance_scheduler
            .next_wakeup_mono(now_mono)
            .map(|target| target.saturating_sub(now_mono));
        tokio::select! {
            biased;
            _ = sigint.recv() => {
                shutdown_reason = "SIGINT";
                break;
            }
            _ = sigterm.recv() => {
                shutdown_reason = "SIGTERM";
                break;
            }
            _ = &mut duration_sleep => {
                shutdown_reason = "duration_elapsed";
                break;
            }
            _ = async {
                if let Some(delay) = next_snap_sleep {
                    tokio::time::sleep(delay).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                dispatch_binance_snapshots(
                    &mut binance_scheduler,
                    &mut snapshot_tasks,
                    &selectors,
                    &config,
                    mono_origin,
                    &tx,
                    &http_client,
                    &shutdown_rx,
                );
            }
            maybe_ev = rx.recv() => {
                let Some(ev) = maybe_ev else {
                    shutdown_reason = "channel_closed";
                    break;
                };
                process_engine_event(
                    ev,
                    &config,
                    mono_origin,
                    &tx,
                    &http_client,
                    &shutdown_rx,
                    &mut selectors,
                    &mut writers,
                    &mut activity,
                    &data_root,
                    &cmd_txs,
                    &mut binance_scheduler,
                    &mut snapshot_tasks,
                    true,
                );
            }
        }
    }

    #[cfg(not(unix))]
    {
        let ctrl_c = tokio::signal::ctrl_c();
        tokio::pin!(ctrl_c);
        loop {
            let now_mono = mono_origin.elapsed();
            let next_snap_sleep = binance_scheduler
                .next_wakeup_mono(now_mono)
                .map(|target| target.saturating_sub(now_mono));
            tokio::select! {
                biased;
                _ = &mut ctrl_c => {
                    shutdown_reason = "CTRL_C";
                    break;
                }
                _ = &mut duration_sleep => {
                    shutdown_reason = "duration_elapsed";
                    break;
                }
                _ = async {
                    if let Some(delay) = next_snap_sleep {
                        tokio::time::sleep(delay).await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => {
                    dispatch_binance_snapshots(
                        &mut binance_scheduler,
                        &mut snapshot_tasks,
                        &selectors,
                        &config,
                        mono_origin,
                        &tx,
                        &http_client,
                        &shutdown_rx,
                    );
                }
                maybe_ev = rx.recv() => {
                    let Some(ev) = maybe_ev else {
                        shutdown_reason = "channel_closed";
                        break;
                    };
                    process_engine_event(
                        ev,
                        &config,
                        mono_origin,
                        &tx,
                        &http_client,
                        &shutdown_rx,
                        &mut selectors,
                        &mut writers,
                        &mut activity,
                        &data_root,
                        &cmd_txs,
                        &mut binance_scheduler,
                        &mut snapshot_tasks,
                        true,
                    );
                }
            }
        }
    }

    let seal_start = Instant::now();
    info!(
        "Received shutdown trigger (reason={}), stopping {} background tasks and {} snapshot tasks, sealing {} exchange writers...",
        shutdown_reason,
        bg_handles.len(),
        snapshot_tasks.len(),
        writers.len()
    );
    flush_stderr();

    // 1. Stop all background WebSocket / timer / REST tasks immediately so producers cease
    //    competing for CPU, sockets, or channel capacity during sealing.
    let _ = shutdown_tx.send(true);
    for h in &bg_handles {
        h.abort();
    }
    for (_, h) in snapshot_tasks.drain() {
        h.abort();
    }
    tokio::task::yield_now().await;
    drop(tx);
    rx.close();

    // 2. Drain any already-buffered events in rx without spawning new side-effect actions.
    let (dummy_tx, _) = mpsc::channel::<EngineEvent>(1);
    let mut drained = 0u64;
    while let Ok(ev) = rx.try_recv() {
        drained += 1;
        process_engine_event(
            ev,
            &config,
            mono_origin,
            &dummy_tx,
            &http_client,
            &shutdown_rx,
            &mut selectors,
            &mut writers,
            &mut activity,
            &data_root,
            &cmd_txs,
            &mut binance_scheduler,
            &mut snapshot_tasks,
            false,
        );
    }
    info!("Drained {} queued events prior to final seal", drained);
    flush_stderr();
    activity
        .flush_final(
            &data_root,
            wall_clock_ns(),
            mono_origin.elapsed().as_nanos() as u64,
            &selectors,
        )
        .await;

    // 3. Seal EVERY exchange deterministically in spawn_blocking (off Tokio worker threads),
    //    continuing across all exchanges even if one fails.
    let mut rollover_failures = Vec::new();
    for (&exchange, writer) in &mut writers {
        for task in writer.take_background_seals() {
            match task.await {
                Ok(result) => {
                    if let Err(error) = result {
                        rollover_failures.push(format!("{exchange}: {error}"));
                    }
                }
                Err(error) => rollover_failures.push(format!("{exchange}: {error}")),
            }
        }
        if writer.pending_write_loss() > 0 {
            rollover_failures.push(format!(
                "{exchange}: {} unrecorded lost frames",
                writer.pending_write_loss()
            ));
        }
    }
    let audit_root = data_root.clone();
    let audit_boot_id = boot_id.clone();
    let seal_res = tokio::task::spawn_blocking(move || {
        let mut sealed_records: Vec<serde_json::Value> = Vec::new();
        let mut failed_records = rollover_failures;
        let mut empty_exchanges: Vec<String> = Vec::new();

        for (ex, mut writer) in writers {
            let ex_start = Instant::now();
            if !writer.has_open_partition() {
                info!("Exchange {} had no open partition to seal", ex);
                empty_exchanges.push(ex.as_dir_name().to_string());
                flush_stderr();
                continue;
            }
            info!("Sealing active partition for {}...", ex);
            flush_stderr();
            match writer.seal_all() {
                Ok(sealed_list) if !sealed_list.is_empty() => {
                    let elapsed_ms = ex_start.elapsed().as_millis() as u64;
                    for s in sealed_list {
                        info!(
                            "Sealed {} -> {} (window={}, bytes={}, data_lines={}, meta_lines={}, sha256={}, elapsed_ms={})",
                            ex,
                            s.data_path.display(),
                            s.manifest.window_id,
                            s.manifest.bytes,
                            s.manifest.data_lines,
                            s.manifest.meta_lines,
                            s.manifest.sha256,
                            elapsed_ms
                        );
                        flush_stderr();
                        sealed_records.push(serde_json::json!({
                            "exchange": ex.as_dir_name(),
                            "window_id": s.manifest.window_id,
                            "partition_minutes": s.manifest.partition_minutes,
                            "data_path": s.data_path.to_string_lossy(),
                            "manifest_path": s.manifest_path.to_string_lossy(),
                            "bytes": s.manifest.bytes,
                            "data_lines": s.manifest.data_lines,
                            "data_escaped_lines": s.manifest.data_escaped_lines,
                            "meta_lines": s.manifest.meta_lines,
                            "local_loss_lines": s.manifest.local_loss_lines,
                            "sha256": s.manifest.sha256,
                            "elapsed_ms": elapsed_ms
                        }));
                    }
                }
                Ok(_) => {
                    empty_exchanges.push(ex.as_dir_name().to_string());
                }
                Err(e) => {
                    let msg = format!("{}: {}", ex, e);
                    error!("Failed sealing {}: {}", ex, e);
                    flush_stderr();
                    failed_records.push(msg);
                }
            }
        }

        let total_ms = seal_start.elapsed().as_millis() as u64;
        // Persist a crash-auditable shutdown seal summary on disk even if external stderr pipe closed.
        let meta_dir = audit_root.join("meta");
        if std::fs::create_dir_all(&meta_dir).is_ok() {
            let audit_path = meta_dir.join(format!("shutdown_{}.json", audit_boot_id));
            let audit = serde_json::json!({
                "boot_id": audit_boot_id,
                "shutdown_reason": shutdown_reason,
                "drained_events": drained,
                "sealed_count": sealed_records.len(),
                "empty_count": empty_exchanges.len(),
                "failed_count": failed_records.len(),
                "total_seal_elapsed_ms": total_ms,
                "sealed": sealed_records,
                "empty": empty_exchanges,
                "failed": failed_records
            });
            if let Ok(bytes) = serde_json::to_vec_pretty(&audit) {
                let _ = std::fs::write(&audit_path, bytes);
            }
        }

        info!(
            "Shutdown seal complete (reason={}, sealed={}, empty={}, failed={}, elapsed_ms={})",
            shutdown_reason,
            sealed_records.len(),
            empty_exchanges.len(),
            failed_records.len(),
            total_ms
        );
        flush_stderr();

        if failed_records.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "sealing failed for {} exchange(s): {}",
                failed_records.len(),
                failed_records.join("; ")
            ))
        }
    })
    .await
    .map_err(|e| format!("shutdown sealing task panicked: {}", e))?;

    // Explicitly keep signal listeners alive until sealing finishes.
    #[cfg(unix)]
    drop((sigint, sigterm));

    seal_res
}

#[allow(clippy::too_many_arguments)]
fn process_engine_event(
    ev: EngineEvent,
    config: &CollectorConfig,
    mono_origin: Instant,
    tx: &mpsc::Sender<EngineEvent>,
    http_client: &reqwest::Client,
    stop_rx: &watch::Receiver<bool>,
    selectors: &mut HashMap<EndpointKind, CausalSelector>,
    writers: &mut BTreeMap<ExchangeId, HourlyTsvZstWriter>,
    activity: &mut StreamActivity,
    data_root: &std::path::Path,
    cmd_txs: &HashMap<(EndpointKind, ConnId), mpsc::Sender<ConnCommand>>,
    binance_scheduler: &mut BinanceSnapshotScheduler,
    snapshot_tasks: &mut HashMap<u64, tokio::task::JoinHandle<()>>,
    allow_actions: bool,
) {
    let mut outputs: Vec<(EndpointKind, crate::causal_selector::SelectorOutput, Option<(ConnId, u64, i64)>)> = Vec::new();
    match ev {
        EngineEvent::Frame(f) => {
            let ep = f.endpoint;
            let meta = (f.conn_id, f.epoch, f.recv_wall_ns);
            if let Some(sel) = selectors.get_mut(&ep) {
                outputs.push((ep, sel.on_frame(f), Some(meta)));
            }
        }
        EngineEvent::Disconnect {
            endpoint,
            conn_id,
            epoch,
            wall_ns,
            mono_ns,
            reason,
        } => {
            if endpoint == EndpointKind::BinancePublic {
                let cancelled = binance_scheduler.cancel_connection(conn_id, Some(epoch));
                abort_snapshot_tasks(snapshot_tasks, cancelled);
            }
            activity.clear_conn_epoch(endpoint, conn_id, epoch);
            if let Some(sel) = active_selector_for_epoch(selectors, endpoint, conn_id, epoch) {
                outputs.push((
                    endpoint,
                    sel.on_disconnect(conn_id, wall_ns, mono_ns, &reason),
                    None,
                ));
            }
        }
        EngineEvent::Reconnect {
            endpoint,
            conn_id,
            epoch,
            mono_ns,
        } => {
            if let Some(sel) = selectors.get_mut(&endpoint) {
                sel.on_reconnect(conn_id, epoch, mono_ns);
            }
            if endpoint == EndpointKind::BinancePublic && allow_actions {
                let cancelled = binance_scheduler.register_connection_epoch(
                    conn_id,
                    epoch,
                    &config.exchanges.binance_futures.symbols,
                    mono_origin.elapsed(),
                );
                abort_snapshot_tasks(snapshot_tasks, cancelled);
            }
        }
        EngineEvent::Subscribed {
            endpoint,
            conn_id,
            epoch,
        } => {
            activity.record_subscribe_requested(endpoint, conn_id, epoch);
        }
        EngineEvent::RestSnapshotAttempt {
            request_id,
            endpoint,
            conn_id,
            epoch,
            symbol,
            outcome,
            wall_ns,
            mono_ns,
        } => {
            snapshot_tasks.remove(&request_id);
            let now_mono = mono_origin.elapsed();
            let now_wall_ms = wall_ns / 1_000_000;
            let disp = binance_scheduler.on_response(
                request_id,
                conn_id,
                epoch,
                &symbol,
                outcome,
                now_mono,
                now_wall_ms,
            );
            match disp {
                ResponseDisposition::IgnoredStale { reason } => {
                    info!(
                        "Ignoring stale Binance depth snapshot response conn={} epoch={} symbol={} req_id={}: {}",
                        conn_id.as_str(),
                        epoch,
                        symbol,
                        request_id,
                        reason
                    );
                }
                ResponseDisposition::ScheduledRetry {
                    status_code,
                    reason,
                    retry_in,
                    shared_cooldown_active,
                    ..
                } => {
                    let used_w = binance_scheduler.effective_used_weight(now_mono);
                    warn!(
                        "Binance depth snapshot failed conn={} epoch={} symbol={} status={:?} reason=\"{}\" retry_in_ms={} shared_cooldown={} used_weight_1m={}",
                        conn_id.as_str(),
                        epoch,
                        symbol,
                        status_code,
                        reason,
                        retry_in.as_millis(),
                        shared_cooldown_active,
                        used_w
                    );
                    flush_stderr();
                }
                ResponseDisposition::ApplySnapshot {
                    last_update_id,
                    raw_json,
                } => {
                    if let Some(sel) = active_selector_for_epoch(selectors, endpoint, conn_id, epoch)
                    {
                        let sel_out = sel.on_rest_snapshot(
                            conn_id,
                            &symbol,
                            last_update_id,
                            &raw_json,
                            wall_ns,
                            mono_ns,
                        );
                        if let Some((c, ep_num, ref sym, bridged, sym_live)) =
                            sel_out.snapshot_bridge_ok
                        {
                            info!(
                                "Binance depth snapshot recovered conn={} epoch={} symbol={} channel=depth@100ms lastUpdateId={} bridged_deltas={} symbol_live={} conn_state={:?}",
                                c.as_str(),
                                ep_num,
                                sym,
                                last_update_id,
                                bridged,
                                sym_live,
                                sel.conn_state(c)
                            );
                            flush_stderr();
                        }
                        if let Some((c, ep_num, ref sym, ref reason)) =
                            sel_out.snapshot_bridge_failed
                        {
                            let next_mono = binance_scheduler.on_bridge_failure(
                                c,
                                ep_num,
                                sym,
                                now_mono,
                                reason.clone(),
                            );
                            warn!(
                                "Binance depth snapshot bridge failed conn={} epoch={} symbol={} reason=\"{}\" retry_in_ms={}",
                                c.as_str(),
                                ep_num,
                                sym,
                                reason,
                                next_mono
                                    .map(|m| m.saturating_sub(now_mono).as_millis())
                                    .unwrap_or(0)
                            );
                            flush_stderr();
                        }
                        outputs.push((endpoint, sel_out, Some((conn_id, epoch, wall_ns))));
                    }
                }
            }
        }
        EngineEvent::LocalLoss {
            endpoint,
            dropped,
            wall_ns,
            mono_ns,
            reason,
        } => {
            if let Some(sel) = selectors.get_mut(&endpoint) {
                outputs.push((
                    endpoint,
                    sel.on_local_loss(dropped, wall_ns, mono_ns, &reason),
                    None,
                ));
            }
        }
        EngineEvent::PlannedRotation {
            target,
            wall_ns,
            mono_ns,
        } => {
            for (&ep, sel) in selectors.iter_mut() {
                outputs.push((ep, sel.on_planned_rotation(target, wall_ns, mono_ns), None));
            }
        }
        EngineEvent::WatchdogTick { wall_ns, mono_ns } => {
            if allow_actions {
                activity.publish(
                    data_root,
                    wall_clock_ns(),
                    mono_ns,
                    tx.max_capacity() - tx.capacity(),
                    selectors,
                );
                for (exchange, writer) in writers.iter_mut() {
                    if let Err(error) = writer.settle_expired_windows(wall_ns) {
                        error!("Failed settling expired window for {}: {}", exchange, error);
                    }
                    for result in writer.poll_background_seals() {
                        match result {
                            Ok(sealed) => info!(
                                "Rolled and sealed {} -> {} (window={}, bytes={}, data_lines={}, sha256={})",
                                exchange,
                                sealed.data_path.display(),
                                sealed.manifest.window_id,
                                sealed.manifest.bytes,
                                sealed.manifest.data_lines,
                                sealed.manifest.sha256
                            ),
                            Err(error) => error!("Failed sealing {}: {}", exchange, error),
                        }
                    }
                }
            }
            for (&ep, sel) in selectors.iter_mut() {
                outputs.push((
                    ep,
                    sel.check_silence_watchdog(wall_ns, mono_ns, PING_TIMEOUT_NS),
                    None,
                ));
            }
        }
    }

    for (ep, out, frame_meta) in outputs {
        let ex = ep.exchange();
        if let Some((conn_id, epoch, wall_ns)) = frame_meta {
            for (sym, chan) in out.subscription_acks {
                activity.record_subscription_ack(ex, ep, conn_id, epoch, sym, chan, wall_ns);
            }
            for (sym, chan) in out.received_streams {
                activity.record_received(ex, ep, conn_id, epoch, sym, chan, wall_ns);
            }
            if let Some((sym, chan)) = out.buffered_wait_anchor {
                activity.record_buffered_wait_anchor(ex, ep, conn_id, epoch, sym, chan, wall_ns);
            }
        }

        let mut written_wall_ns = None;
        if let Some(writer) = writers.get_mut(&ex) {
            for rec in &out.records {
                match writer.write_record(rec) {
                    Ok(Some(s)) => {
                        info!(
                            "Rolled and sealed {} -> {} (bytes={}, data_lines={}, sha256={})",
                            ex,
                            s.data_path.display(),
                            s.manifest.bytes,
                            s.manifest.data_lines,
                            s.manifest.sha256
                        );
                        flush_stderr();
                        if !matches!(rec.kind, crate::types::RecordKind::Meta) {
                            written_wall_ns = Some(rec.recv_wall_ns);
                        }
                    }
                    Ok(None) => {
                        if !matches!(rec.kind, crate::types::RecordKind::Meta) {
                            written_wall_ns = Some(rec.recv_wall_ns);
                        }
                    }
                    Err(e) => {
                        if writer.pending_write_loss() == 0 {
                            error!("Disk write error on {}: LOCAL_LOSS: {}", ex, e);
                            flush_stderr();
                        }
                        writer.note_write_loss();
                    }
                }
            }
        }
        if let Some(wall_ns) = written_wall_ns {
            if !out.written_streams.is_empty() {
                for (symbol, channel) in out.written_streams {
                    activity.record_written(ex, symbol, channel, wall_ns);
                }
            } else if let Some((symbol, channel)) = out.market_data {
                activity.record_written(ex, symbol, channel, wall_ns);
            }
        }
        if !allow_actions {
            continue;
        }
        for act in out.actions {
            match act {
                SelectorAction::Reconnect { conn_id, reason } => {
                    if let (Some(ctx), Some(selector)) =
                        (cmd_txs.get(&(ep, conn_id)), selectors.get_mut(&ep))
                    {
                        if let Some(epoch) = selector.connected_epoch(conn_id) {
                            let command = ConnCommand {
                                epoch,
                                action: ConnCommandAction::Reconnect(reason),
                            };
                            if ctx.try_send(command).is_err() {
                                selector.allow_reconnect_retry(conn_id);
                                warn!(
                                    "Reconnect command was not queued for {} conn={} epoch={}",
                                    ep.as_str(),
                                    conn_id.as_str(),
                                    epoch
                                );
                            }
                        }
                    }
                }
                SelectorAction::Resubscribe { conn_id, reason } => {
                    if let (Some(ctx), Some(selector)) =
                        (cmd_txs.get(&(ep, conn_id)), selectors.get_mut(&ep))
                    {
                        if let Some(epoch) = selector.connected_epoch(conn_id) {
                            let command = ConnCommand {
                                epoch,
                                action: ConnCommandAction::Resubscribe(reason),
                            };
                            if ctx.try_send(command).is_ok() && ep == EndpointKind::GatePublic {
                                selector.prepare_depth_refresh(conn_id);
                            }
                        }
                    }
                }
                SelectorAction::FetchRestSnapshot { conn_id, symbol } => {
                    if ep == EndpointKind::BinancePublic {
                        let Some(epoch) = selectors
                            .get(&ep)
                            .and_then(|selector| selector.connected_epoch(conn_id))
                        else {
                            continue;
                        };
                        let supersede = out.snapshot_bridge_failed.is_none();
                        if let Some(old_id) = binance_scheduler.trigger(
                            conn_id,
                            epoch,
                            &symbol,
                            mono_origin.elapsed(),
                            "selector_resync",
                            supersede,
                        ) {
                            abort_snapshot_tasks(snapshot_tasks, [old_id]);
                        }
                    }
                }
                SelectorAction::Alert { severity, message } => {
                    error!("[ALERT {}] {}", severity, message);
                    flush_stderr();
                }
            }
        }
    }

    if allow_actions {
        dispatch_binance_snapshots(
            binance_scheduler,
            snapshot_tasks,
            selectors,
            config,
            mono_origin,
            tx,
            http_client,
            stop_rx,
        );
    }
}

#[allow(clippy::too_many_arguments)]
async fn ws_connection_loop(
    endpoint: EndpointKind,
    conn_id: ConnId,
    symbols: Vec<String>,
    config: Arc<CollectorConfig>,
    mono_origin: Instant,
    tx: mpsc::Sender<EngineEvent>,
    mut cmd_rx: mpsc::Receiver<ConnCommand>,
    mut stop_rx: watch::Receiver<bool>,
) {
    let mut epoch = 0u64;
    let mut backoff_ms = INITIAL_RETRY_MS;

    loop {
        if *stop_rx.borrow() {
            return;
        }
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
                tokio::select! {
                    biased;
                    _ = stop_rx.changed() => return,
                    _ = tokio::time::sleep(Duration::from_secs(5)) => continue,
                }
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

        let connect_res = tokio::select! {
            biased;
            _ = stop_rx.changed() => return,
            res = tokio_tungstenite::connect_async(req) => res,
        };

        let mut subscribed_uptime = None;
        match connect_res {
            Ok((ws_stream, _)) => {
                let mono_ns = mono_origin.elapsed().as_nanos() as u64;
                if !send_lifecycle_event(
                    &tx,
                    &mut stop_rx,
                    EngineEvent::Reconnect {
                        endpoint,
                        conn_id,
                        epoch,
                        mono_ns,
                    },
                )
                .await
                {
                    return;
                }
                info!(
                    "Connected {} conn={} epoch={}",
                    endpoint.as_str(),
                    conn_id.as_str(),
                    epoch
                );
                flush_stderr();

                let (mut write, mut read) = ws_stream.split();

                let disconnect_reason = 'session: {
                    let subscribed_at =
                        match subscribe_socket(endpoint, &symbols, &mut write, &mut stop_rx).await {
                            Some(Ok(subscribed_at)) => subscribed_at,
                            Some(Err(reason)) => {
                                warn!(
                                    "Subscribe write error on {} {:?}: {}",
                                    endpoint.as_str(),
                                    conn_id,
                                    reason
                                );
                                break 'session reason;
                            }
                            None => return,
                        };
                    if !send_lifecycle_event(
                        &tx,
                        &mut stop_rx,
                        EngineEvent::Subscribed {
                            endpoint,
                            conn_id,
                            epoch,
                        },
                    )
                    .await
                    {
                        return;
                    }

                    let ping_secs = match endpoint {
                        EndpointKind::BybitPublic => {
                            config.exchanges.bybit_linear.ping_interval_seconds
                        }
                        EndpointKind::BitgetPublic => {
                            config.exchanges.bitget_v3.ping_interval_seconds
                        }
                        EndpointKind::GatePublic => {
                            config.exchanges.gate_futures.ping_interval_seconds
                        }
                        _ => 30,
                    };
                    let mut ping_timer =
                        tokio::time::interval(Duration::from_secs(ping_secs.max(5)));
                    let disconnect_reason: String;
                    let mut gate_refresh: Option<GateDepthRefresh> = None;

                    loop {
                        let refresh_deadline =
                            gate_refresh.as_ref().map(|refresh| refresh.deadline);
                        tokio::select! {
                            biased;
                            _ = stop_rx.changed() => {
                                if *stop_rx.borrow() {
                                    return;
                                }
                            }
                            _ = async {
                                if let Some(deadline) = refresh_deadline {
                                    tokio::time::sleep_until(deadline).await;
                                } else {
                                    std::future::pending::<()>().await;
                                }
                            } => {
                                disconnect_reason = "gate_depth_refresh_timeout".to_string();
                                break;
                            }
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
                            Some(cmd) = next_command_for_epoch(&mut cmd_rx, epoch) => {
                                match cmd {
                                    ConnCommandAction::Reconnect(r) => {
                                        disconnect_reason = format!("cmd_reconnect: {}", r);
                                        break;
                                    }
                                    ConnCommandAction::Resubscribe(r) => {
                                        warn!(
                                            "Resubscribing {} conn={} reason={}",
                                            endpoint.as_str(),
                                            conn_id.as_str(),
                                            r
                                        );
                                        if endpoint == EndpointKind::GatePublic {
                                            if gate_refresh.is_none() {
                                                match GateDepthRefresh::start(&symbols, &mut write).await {
                                                    Ok(refresh) => gate_refresh = Some(refresh),
                                                    Err(error) => {
                                                        disconnect_reason = format!("gate_unsubscribe_error: {error}");
                                                        break;
                                                    }
                                                }
                                            }
                                        } else if let Err(e) = send_subscriptions(endpoint, &symbols, &mut write).await {
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
                                        if let Some(refresh) = gate_refresh.as_mut() {
                                            match refresh.on_message(&text, &symbols, &mut write).await {
                                                Ok(true) => gate_refresh = None,
                                                Ok(false) => {}
                                                Err(error) => {
                                                    disconnect_reason = format!("gate_depth_refresh_error: {error}");
                                                    break;
                                                }
                                            }
                                        }
                                        let frame = RawFrame {
                                            recv_wall_ns: wall_ns,
                                            recv_mono_ns: mono_ns,
                                            exchange: endpoint.exchange(),
                                            endpoint,
                                            conn_id,
                                            epoch,
                                            payload: text,
                                        };
                                        match tx.try_send(EngineEvent::Frame(frame)) {
                                            Ok(()) => {}
                                            Err(mpsc::error::TrySendError::Full(_)) => {
                                                // Never silently drop: force LOCAL_LOSS event notification.
                                                let _ = tx.send(EngineEvent::LocalLoss {
                                                    endpoint,
                                                    dropped: 1,
                                                    wall_ns,
                                                    mono_ns,
                                                    reason: "bounded_channel_full".to_string(),
                                                }).await;
                                            }
                                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                                return;
                                            }
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
                    subscribed_uptime = Some(subscribed_at.elapsed());
                    disconnect_reason
                };

                if !send_lifecycle_event(
                    &tx,
                    &mut stop_rx,
                    EngineEvent::Disconnect {
                        endpoint,
                        conn_id,
                        epoch,
                        wall_ns: wall_clock_ns(),
                        mono_ns: mono_origin.elapsed().as_nanos() as u64,
                        reason: disconnect_reason,
                    },
                )
                .await
                {
                    return;
                }
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

        let delay = next_retry_delay(&mut backoff_ms, subscribed_uptime);
        tokio::select! {
            biased;
            _ = stop_rx.wait_for(|requested| *requested) => return,
            _ = tokio::time::sleep(delay) => {}
        }
    }
}

struct GateDepthRefresh {
    deadline: tokio::time::Instant,
    awaiting_ack: HashSet<u64>,
    awaiting_snapshot: HashSet<String>,
}

impl GateDepthRefresh {
    async fn start<S>(symbols: &[String], write: &mut S) -> Result<Self, String>
    where
        S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
    {
        let now = Utc::now();
        let mut awaiting_ack = HashSet::new();
        let awaiting_snapshot = symbols
            .iter()
            .map(|symbol| format!("ob.{}.50", symbol.to_uppercase()))
            .collect();
        for (index, mut frame) in
            GateValidator::build_depth_messages(symbols, now.timestamp(), "unsubscribe")
                .into_iter()
                .enumerate()
        {
            let id = (now.timestamp_millis() as u64) * 100 + index as u64;
            frame["id"] = id.into();
            awaiting_ack.insert(id);
            write
                .send(Message::Text(frame.to_string()))
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(Self {
            deadline: tokio::time::Instant::now() + Duration::from_secs(10),
            awaiting_ack,
            awaiting_snapshot,
        })
    }

    async fn on_message<S>(
        &mut self,
        text: &str,
        symbols: &[String],
        write: &mut S,
    ) -> Result<bool, String>
    where
        S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
    {
        let Ok(frame) = serde_json::from_str::<serde_json::Value>(text) else {
            return Ok(false);
        };
        if frame.get("channel").and_then(|v| v.as_str()) != Some("futures.obu") {
            return Ok(false);
        }
        if !self.awaiting_ack.is_empty() {
            if frame.get("event").and_then(|v| v.as_str()) == Some("unsubscribe") {
                if let Some(id) = frame.get("id").and_then(|v| v.as_u64()) {
                    if self.awaiting_ack.remove(&id) {
                        if frame.get("error").is_some_and(|v| !v.is_null())
                            || frame.pointer("/result/status").and_then(|v| v.as_str())
                                != Some("success")
                        {
                            return Err("Gate rejected depth unsubscribe".into());
                        }
                        if self.awaiting_ack.is_empty() {
                            for message in GateValidator::build_depth_messages(
                                symbols,
                                Utc::now().timestamp(),
                                "subscribe",
                            ) {
                                write
                                    .send(Message::Text(message.to_string()))
                                    .await
                                    .map_err(|e| e.to_string())?;
                            }
                        }
                    }
                }
            }
        } else if frame.get("error").is_some_and(|v| !v.is_null()) {
            return Err("Gate rejected depth resubscribe".into());
        } else if frame.pointer("/result/full").and_then(|v| v.as_bool()) == Some(true) {
            if let Some(stream) = frame.pointer("/result/s").and_then(|v| v.as_str()) {
                self.awaiting_snapshot.remove(stream);
            }
        }
        Ok(self.awaiting_ack.is_empty() && self.awaiting_snapshot.is_empty())
    }
}

async fn subscribe_socket<S>(
    endpoint: EndpointKind,
    symbols: &[String],
    write: &mut S,
    stop: &mut watch::Receiver<bool>,
) -> Option<Result<Instant, String>>
where
    S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let result = tokio::select! {
        biased;
        _ = stop.wait_for(|requested| *requested) => return None,
        result = send_subscriptions(endpoint, symbols, write) => result,
    };
    Some(
        result
            .map(|()| Instant::now())
            .map_err(|error| format!("subscribe_write_error: {error}")),
    )
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use std::sync::Mutex;

    #[tokio::test]
    async fn command_queue_rejects_late_reconnect_and_resubscribe_for_previous_epoch() {
        let (tx, mut rx) = mpsc::channel(4);
        let next = tokio::spawn(async move { next_command_for_epoch(&mut rx, 2).await });
        tx.send(ConnCommand {
            epoch: 1,
            action: ConnCommandAction::Reconnect("old".into()),
        })
        .await
        .unwrap();
        tx.send(ConnCommand {
            epoch: 1,
            action: ConnCommandAction::Resubscribe("old".into()),
        })
        .await
        .unwrap();
        tx.send(ConnCommand {
            epoch: 2,
            action: ConnCommandAction::Resubscribe("current".into()),
        })
        .await
        .unwrap();
        assert!(
            matches!(next.await.unwrap(), Some(ConnCommandAction::Resubscribe(reason)) if reason == "current")
        );
    }

    #[test]
    fn late_rest_and_disconnect_events_do_not_target_the_new_epoch() {
        let endpoint = EndpointKind::BinancePublic;
        let mut selector = CausalSelector::new(endpoint, vec!["BTCUSDT".into()], 3, 5);
        selector.start_disconnected();
        let mut selectors = HashMap::from([(endpoint, selector)]);
        assert!(active_selector_for_epoch(&mut selectors, endpoint, ConnId::A, 1).is_none());
        let selector = selectors.get_mut(&endpoint).unwrap();
        selector.on_reconnect(ConnId::A, 1, 10);
        selector.on_disconnect(ConnId::A, 10, 11, "old socket closed");
        assert!(active_selector_for_epoch(&mut selectors, endpoint, ConnId::A, 1).is_none());

        selectors
            .get_mut(&endpoint)
            .unwrap()
            .on_reconnect(ConnId::A, 2, 20);
        assert!(active_selector_for_epoch(&mut selectors, endpoint, ConnId::A, 1).is_none());
        assert!(active_selector_for_epoch(&mut selectors, endpoint, ConnId::A, 2).is_some());
    }

    #[tokio::test]
    async fn subscription_sink_write_failure_is_a_disconnect_reason() {
        let sink = futures_util::sink::unfold((), |(), _: Message| {
            std::future::ready(Err::<(), _>(
                tokio_tungstenite::tungstenite::Error::ConnectionClosed,
            ))
        });
        let mut sink = Box::pin(sink);
        let (_stop, mut stop_rx) = watch::channel(false);
        let result = subscribe_socket(
            EndpointKind::GatePublic,
            &["BTC_USDT".into()],
            &mut sink,
            &mut stop_rx,
        )
        .await
        .unwrap();
        assert!(result.unwrap_err().starts_with("subscribe_write_error:"));
    }

    #[test]
    fn failed_or_short_subscriptions_increase_retry_delay_until_a_stable_session() {
        let mut backoff_ms = INITIAL_RETRY_MS;
        assert_eq!(
            next_retry_delay(&mut backoff_ms, None),
            Duration::from_millis(500)
        );
        assert_eq!(
            next_retry_delay(&mut backoff_ms, None),
            Duration::from_secs(1)
        );
        assert_eq!(
            next_retry_delay(&mut backoff_ms, Some(Duration::from_millis(20))),
            Duration::from_secs(2),
        );
        for _ in 0..10 {
            let _ = next_retry_delay(&mut backoff_ms, None);
        }
        assert_eq!(
            next_retry_delay(&mut backoff_ms, None),
            Duration::from_secs(15)
        );
        assert_eq!(
            next_retry_delay(&mut backoff_ms, Some(STABLE_SESSION)),
            Duration::from_millis(500)
        );
    }

    #[tokio::test]
    async fn gate_refresh_waits_for_unsubscribe_then_requests_only_depth() {
        let frames = Arc::new(Mutex::new(Vec::<Value>::new()));
        let captured = Arc::clone(&frames);
        let sink = futures_util::sink::unfold((), move |(), msg: Message| {
            let Message::Text(text) = msg else {
                panic!("expected a Gate text frame")
            };
            captured
                .lock()
                .unwrap()
                .push(serde_json::from_str(&text).unwrap());
            std::future::ready(Ok::<(), tokio_tungstenite::tungstenite::Error>(()))
        });
        let mut sink = Box::pin(sink);
        let symbols = vec!["BTC_USDT".to_string(), "ETH_USDT".to_string()];
        let mut refresh = GateDepthRefresh::start(&symbols, &mut sink).await.unwrap();
        let ids: Vec<_> = frames
            .lock()
            .unwrap()
            .iter()
            .map(|row| row["id"].clone())
            .collect();
        assert_eq!(ids.len(), 2);
        assert!(frames
            .lock()
            .unwrap()
            .iter()
            .all(|row| row["event"] == "unsubscribe"));

        for (index, id) in ids.into_iter().enumerate() {
            let ack = json!({"id": id, "channel": "futures.obu", "event": "unsubscribe",
                "result": {"status": "success"}});
            assert!(!refresh
                .on_message(&ack.to_string(), &symbols, &mut sink)
                .await
                .unwrap());
            assert_eq!(
                frames.lock().unwrap().len(),
                if index == 0 { 2 } else { 4 }
            );
        }
        assert!(frames.lock().unwrap()[2..]
            .iter()
            .all(|row| row["event"] == "subscribe" && row["channel"] == "futures.obu"));
        for (index, symbol) in symbols.iter().enumerate() {
            let raw = json!({"channel": "futures.obu", "result": {
                "full": true, "s": format!("ob.{symbol}.50")
            }});
            assert_eq!(
                refresh
                    .on_message(&raw.to_string(), &symbols, &mut sink)
                    .await
                    .unwrap(),
                index == 1
            );
        }
    }
}
