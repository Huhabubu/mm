use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectorConfig {
    pub schema_version: u32,
    pub spec_version: String,
    pub mode: String,
    pub deployment: DeploymentConfig,
    pub archive: ArchiveConfig,
    pub meta_daemon: MetaDaemonConfig,
    pub exchanges: ExchangesConfig,
}

fn default_partition_minutes() -> u32 {
    60
}

fn default_late_frame_grace_ms() -> u64 {
    2000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentConfig {
    pub aws_region: String,
    pub aws_physical_az_id: String,
    pub data_root: String,
    pub zstd_level: i32,
    pub bounded_queue_capacity: usize,
    pub silence_watchdog_seconds: u64,
    #[serde(default = "default_partition_minutes")]
    pub partition_minutes: u32,
    #[serde(default = "default_late_frame_grace_ms")]
    pub late_frame_grace_ms: u64,
    #[serde(default)]
    pub run_id: Option<String>,
    #[serde(default)]
    pub cutover_window_utc: Option<String>,
    pub planned_rotation_utc: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveConfig {
    pub rclone_remote: String,
    pub local_retention_hours: u64,
    pub disk_alert_pct: f64,
    pub verify_remote_hash: bool,
    pub forbid_remote_overwrite_on_hash_mismatch: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaDaemonConfig {
    pub rtt_interval_seconds: u64,
    pub chrony_interval_seconds: u64,
    pub chrony_max_error_ms: f64,
    pub dual_outage_alert_seconds: u64,
    pub alert_webhook_env: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangesConfig {
    pub binance_futures: BinanceConfig,
    pub bybit_linear: BybitConfig,
    pub bitget_v3: BitgetConfig,
    pub gate_futures: GateConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceConfig {
    pub enabled: bool,
    pub public_ws_url: String,
    pub market_ws_url: String,
    pub rest_exchange_info_url: String,
    pub rest_depth_url: String,
    pub depth_snapshot_limit: u32,
    pub symbols: Vec<String>,
    pub public_channels: Vec<String>,
    pub market_channels: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BybitConfig {
    pub enabled: bool,
    pub ws_url: String,
    pub rest_instruments_url: String,
    pub ping_interval_seconds: u64,
    pub symbols: Vec<String>,
    pub channels: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitgetConfig {
    pub enabled: bool,
    pub ws_url: String,
    pub rest_instruments_url: String,
    pub ping_interval_seconds: u64,
    pub symbols: Vec<String>,
    pub channels: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GateConfig {
    pub enabled: bool,
    pub ws_url: String,
    pub rest_contracts_url: String,
    pub required_headers: BTreeMap<String, String>,
    pub ping_interval_seconds: u64,
    pub symbols: Vec<String>,
    pub channels: Vec<String>,
}

impl CollectorConfig {
    pub fn load(path: &Path) -> Result<Self, String> {
        let raw = fs::read_to_string(path)
            .map_err(|e| format!("Failed to read config {}: {}", path.display(), e))?;
        serde_json::from_str(&raw)
            .map_err(|e| format!("Invalid JSON in config {}: {}", path.display(), e))
    }

    pub fn validate(&self) -> Result<(), Vec<String>> {
        self.validate_with_options(false)
    }

    pub fn validate_with_options(&self, allow_localhost_test: bool) -> Result<(), Vec<String>> {
        let is_local = |url: &str| {
            allow_localhost_test
                && (url.starts_with("ws://127.0.0.1:") || url.starts_with("http://127.0.0.1:"))
        };
        let mut errs = Vec::new();
        if self.schema_version != 1 {
            errs.push(format!("Unsupported schema_version: {}", self.schema_version));
        }
        if !(1..=19).contains(&self.deployment.zstd_level) {
            errs.push(format!("Invalid zstd_level: {}", self.deployment.zstd_level));
        }
        if self.deployment.bounded_queue_capacity < 64 {
            errs.push(format!(
                "bounded_queue_capacity too small: {}",
                self.deployment.bounded_queue_capacity
            ));
        }
        if !matches!(self.deployment.partition_minutes, 5 | 15 | 30 | 60) {
            errs.push(format!(
                "Invalid deployment.partition_minutes: {} (must be one of 5, 15, 30, 60)",
                self.deployment.partition_minutes
            ));
        }
        let max_grace_ms = (self.deployment.partition_minutes as u64)
            .saturating_mul(30_000)
            .min(60_000);
        if self.deployment.late_frame_grace_ms > max_grace_ms {
            errs.push(format!(
                "Invalid deployment.late_frame_grace_ms: {} (must be <= {} ms)",
                self.deployment.late_frame_grace_ms, max_grace_ms
            ));
        }
        if !self.archive.forbid_remote_overwrite_on_hash_mismatch {
            errs.push("archive.forbid_remote_overwrite_on_hash_mismatch must be true".to_string());
        }
        if !self.archive.verify_remote_hash {
            errs.push("archive.verify_remote_hash must be true".to_string());
        }

        let mut any_enabled = false;

        // Binance USDS-M strict 2026-04-23 split enforcement
        let b = &self.exchanges.binance_futures;
        if b.enabled {
            any_enabled = true;
            if b.symbols.is_empty() {
                errs.push("binance_futures enabled with empty symbols".to_string());
            }
            if !b.public_ws_url.contains("fstream.binance.com/public/stream")
                && !(is_local(&b.public_ws_url) && b.public_ws_url.contains("/public/stream"))
            {
                errs.push(format!(
                    "binance_futures.public_ws_url must use /public/stream, got {}",
                    b.public_ws_url
                ));
            }
            if !b.market_ws_url.contains("fstream.binance.com/market/stream")
                && !(is_local(&b.market_ws_url) && b.market_ws_url.contains("/market/stream"))
            {
                errs.push(format!(
                    "binance_futures.market_ws_url must use /market/stream, got {}",
                    b.market_ws_url
                ));
            }
            if b.public_channels.iter().any(|c| c.contains("aggTrade")) {
                errs.push("binance_futures: aggTrade MUST NOT be placed on /public/stream".to_string());
            }
            if b.public_channels.iter().any(|c| c.contains("depth@0ms")) {
                errs.push("binance_futures: depth@0ms is disallowed by v1.4 channel simplification; use depth@100ms".to_string());
            }
            if !b.public_channels.iter().any(|c| c == "bookTicker") {
                errs.push("binance_futures.public_channels must include bookTicker".to_string());
            }
            if !b.public_channels.iter().any(|c| c == "depth@100ms") {
                errs.push("binance_futures.public_channels must include depth@100ms".to_string());
            }
            if !b.market_channels.iter().any(|c| c == "aggTrade") {
                errs.push("binance_futures.market_channels must include aggTrade".to_string());
            }
            if b.depth_snapshot_limit != 1000 {
                errs.push("binance_futures.depth_snapshot_limit must be 1000".to_string());
            }
        }

        // Bybit Linear v5
        let by = &self.exchanges.bybit_linear;
        if by.enabled {
            any_enabled = true;
            if by.symbols.is_empty() {
                errs.push("bybit_linear enabled with empty symbols".to_string());
            }
            if !by.ws_url.contains("stream.bybit.com/v5/public/linear") && !is_local(&by.ws_url) {
                errs.push(format!("bybit_linear.ws_url invalid: {}", by.ws_url));
            }
            for req in ["orderbook.50", "orderbook.1", "publicTrade"] {
                if !by.channels.iter().any(|c| c == req) {
                    errs.push(format!("bybit_linear.channels missing {}", req));
                }
            }
            if by.ping_interval_seconds == 0 || by.ping_interval_seconds > 25 {
                errs.push("bybit_linear.ping_interval_seconds must be in 1..=25 (recommended 20)".to_string());
            }
        }

        // Bitget UTA v3
        let bg = &self.exchanges.bitget_v3;
        if bg.enabled {
            any_enabled = true;
            if bg.symbols.is_empty() {
                errs.push("bitget_v3 enabled with empty symbols".to_string());
            }
            if !bg.ws_url.contains("ws.bitget.com/v3/ws/public") && !is_local(&bg.ws_url) {
                errs.push(format!("bitget_v3.ws_url must be UTA v3 public endpoint, got {}", bg.ws_url));
            }
            if bg.channels.iter().any(|c| c == "trade") {
                errs.push("bitget_v3: legacy topic 'trade' is invalid in UTA v3; use 'publicTrade'".to_string());
            }
            if bg.channels.iter().any(|c| c.contains("rpi")) {
                errs.push("bitget_v3: rpi-books must not be subscribed".to_string());
            }
            for req in ["books50", "books1", "publicTrade"] {
                if !bg.channels.iter().any(|c| c == req) {
                    errs.push(format!("bitget_v3.channels missing {}", req));
                }
            }
        }

        // Gate.io Futures v4
        let g = &self.exchanges.gate_futures;
        if g.enabled {
            any_enabled = true;
            if g.symbols.is_empty() {
                errs.push("gate_futures enabled with empty symbols".to_string());
            }
            if !g.ws_url.contains("fx-ws.gateio.ws/v4/ws/usdt") && !is_local(&g.ws_url) {
                errs.push(format!("gate_futures.ws_url invalid: {}", g.ws_url));
            }
            match g.required_headers.get("X-Gate-Size-Decimal") {
                Some(v) if v == "1" => {}
                _ => errs.push(
                    "gate_futures.required_headers MUST include \"X-Gate-Size-Decimal\": \"1\""
                        .to_string(),
                ),
            }
            if g.channels.iter().any(|c| c.contains("400") || c.contains("rpi")) {
                errs.push("gate_futures: 400-level and rpi channels are excluded by v1.4 spec".to_string());
            }
            for req in ["futures.obu.50", "futures.book_ticker", "futures.trades"] {
                if !g.channels.iter().any(|c| c == req) {
                    errs.push(format!("gate_futures.channels missing {}", req));
                }
            }
            for sym in &g.symbols {
                if !sym.contains("_USDT") {
                    errs.push(format!(
                        "gate_futures symbol must use underscore convention (e.g. BTC_USDT), got {}",
                        sym
                    ));
                }
            }
        }

        if !any_enabled {
            errs.push("At least one exchange must be enabled".to_string());
        }

        if errs.is_empty() {
            Ok(())
        } else {
            Err(errs)
        }
    }

    /// Resolve the single effective batch `run_id` across CLI `--run-id`, `TOKYO_L2_RUN_ID`, and
    /// `deployment.run_id` in `config.json`. Fails closed if multiple non-empty values disagree
    /// or if all sources are empty.
    pub fn resolve_effective_run_id(
        &self,
        cli_run_id: Option<&str>,
        env_run_id: Option<&str>,
    ) -> Result<String, String> {
        let cli_clean = cli_run_id.map(str::trim).filter(|s| !s.is_empty());
        let env_clean = env_run_id.map(str::trim).filter(|s| !s.is_empty());
        let cfg_clean = self
            .deployment
            .run_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());

        let mut sources: Vec<(&str, &str)> = Vec::new();
        if let Some(v) = cli_clean {
            sources.push(("cli(--run-id)", v));
        }
        if let Some(v) = env_clean {
            sources.push(("env(TOKYO_L2_RUN_ID)", v));
        }
        if let Some(v) = cfg_clean {
            sources.push(("config(deployment.run_id)", v));
        }

        if sources.is_empty() {
            return Err(
                "missing effective run_id: specify --run-id, TOKYO_L2_RUN_ID in live.env, or deployment.run_id in config.json"
                    .to_string(),
            );
        }

        let first_val = sources[0].1;
        if sources.iter().any(|(_, v)| *v != first_val) {
            let detail = sources
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(format!(
                "conflicting run_id across collector entrypoints ({})",
                detail
            ));
        }

        Ok(first_val.to_string())
    }
}
