use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;
use tokyo_l2_collector::causal_selector::CausalSelector;
use tokyo_l2_collector::config::CollectorConfig;
use tokyo_l2_collector::live::run_live_collector;
use tokyo_l2_collector::storage::{HourlyTsvZstWriter, WriterOptions};
use tokyo_l2_collector::types::{BlockedReport, ConnId, EndpointKind, ExchangeId, RawFrame};

#[derive(Parser, Debug)]
#[command(
    name = "tokyo-l2-collector",
    version,
    about = "AWS Tokyo co-city dual-active causal L2 market data collector"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Offline configuration and protocol contract validation (no network).
    Preflight {
        #[arg(long)]
        config: PathBuf,
        #[arg(long, default_value_t = true)]
        offline: bool,
    },
    /// Run live dual-active collector (requires explicit live authorization flag/env).
    Collect {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        run_id: String,
        #[arg(long, default_value_t = false)]
        allow_live_run: bool,
        /// Optional clean internal duration timer (seconds) for bounded P2 smoke/first-frame runs.
        #[arg(long)]
        duration_secs: Option<u64>,
    },
    /// Deterministically replay an offline JSONL multi-connection fixture through CausalSelector + HourlyTsvZstWriter.
    ReplayFixture {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        fixture: PathBuf,
        #[arg(long)]
        out: PathBuf,
        #[arg(long, default_value = "offline_boot_1")]
        boot_id: String,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum FixtureEvent {
    Frame {
        recv_wall_ns: i64,
        recv_mono_ns: u64,
        endpoint: EndpointKind,
        conn_id: ConnId,
        epoch: u64,
        payload: String,
    },
    RestSnapshot {
        recv_wall_ns: i64,
        recv_mono_ns: u64,
        endpoint: EndpointKind,
        conn_id: ConnId,
        symbol: String,
        last_update_id: u64,
        raw_json: String,
    },
    Disconnect {
        recv_wall_ns: i64,
        recv_mono_ns: u64,
        endpoint: EndpointKind,
        conn_id: ConnId,
        reason: String,
    },
    Reconnect {
        recv_mono_ns: u64,
        endpoint: EndpointKind,
        conn_id: ConnId,
        epoch: u64,
    },
    PlannedRotation {
        recv_wall_ns: i64,
        recv_mono_ns: u64,
        endpoint: EndpointKind,
        target: ConnId,
    },
    LocalLoss {
        recv_wall_ns: i64,
        recv_mono_ns: u64,
        endpoint: EndpointKind,
        dropped: u64,
        reason: String,
    },
}

#[derive(Debug, Serialize)]
struct ReplaySummary {
    status: String,
    boot_id: String,
    sealed_manifests: Vec<String>,
    total_records_written: u64,
}

use std::io::Write as _;

/// Non-panicking stderr writer for `tracing_subscriber::fmt`.
///
/// By default, `tracing_subscriber::fmt()` enables `log_internal_errors(true)` and calls
/// `eprintln!(...)` when `write_all` fails (e.g. `EPIPE` / `BrokenPipe` after an upstream
/// `tee` or log pipe exits on `SIGINT`, or `EIO` on PTY hangup). Calling `eprintln!` on a
/// closed stderr panics with exit code 101 and aborts the multi-exchange shutdown sealing loop.
struct BestEffortStderr;

impl std::io::Write for BestEffortStderr {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match std::io::stderr().write(buf) {
            Ok(n) => Ok(n),
            Err(_) => Ok(buf.len()),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let _ = std::io::stderr().flush();
        Ok(())
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(|| BestEffortStderr)
        .log_internal_errors(false)
        .init();

    let cli = Cli::parse();
    match cli.command {
        Commands::Preflight { config, offline } => {
            if !offline {
                let rep = BlockedReport::new(
                    "preflight",
                    vec!["online preflight requires explicit P2 authorization".to_string()],
                    vec!["Run with --offline during P0/P1".to_string()],
                );
                println!("{}", serde_json::to_string_pretty(&rep).unwrap());
                return ExitCode::from(2);
            }
            let cfg = match CollectorConfig::load(&config) {
                Ok(c) => c,
                Err(e) => {
                    let rep = BlockedReport::new(
                        "preflight",
                        vec![e],
                        vec!["Provide a valid collector JSON configuration".to_string()],
                    );
                    println!("{}", serde_json::to_string_pretty(&rep).unwrap());
                    return ExitCode::from(2);
                }
            };
            if let Err(errs) = cfg.validate() {
                let rep = BlockedReport::new(
                    "preflight",
                    errs,
                    vec!["Fix all protocol/config violations listed in failed_items".to_string()],
                );
                println!("{}", serde_json::to_string_pretty(&rep).unwrap());
                return ExitCode::from(2);
            }
            let ok = json!({
                "status": "ok",
                "phase": "preflight_offline_rust",
                "mode": cfg.mode,
                "spec_version": cfg.spec_version,
                "partition_minutes": cfg.deployment.partition_minutes,
                "late_frame_grace_ms": cfg.deployment.late_frame_grace_ms,
                "enabled_exchanges": {
                    "binance_futures": cfg.exchanges.binance_futures.enabled,
                    "bybit_linear": cfg.exchanges.bybit_linear.enabled,
                    "bitget_v3": cfg.exchanges.bitget_v3.enabled,
                    "gate_futures": cfg.exchanges.gate_futures.enabled
                }
            });
            println!("{}", serde_json::to_string_pretty(&ok).unwrap());
            ExitCode::SUCCESS
        }
        Commands::Collect {
            config,
            run_id,
            allow_live_run,
            duration_secs,
        } => {
            let env_auth = std::env::var("TOKYO_L2_LIVE_AUTHORIZED")
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false);
            if !allow_live_run && !env_auth {
                let rep = BlockedReport::new(
                    "collect",
                    vec!["Live collection blocked: client P2/P3 authorization not confirmed".to_string()],
                    vec!["Obtain client signoff and pass --allow-live-run or set TOKYO_L2_LIVE_AUTHORIZED=1".to_string()],
                );
                println!("{}", serde_json::to_string_pretty(&rep).unwrap());
                return ExitCode::from(3);
            }
            let cfg = match CollectorConfig::load(&config) {
                Ok(c) => c,
                Err(e) => {
                    let rep = BlockedReport::new("collect", vec![e], vec!["Fix config file".to_string()]);
                    println!("{}", serde_json::to_string_pretty(&rep).unwrap());
                    return ExitCode::from(2);
                }
            };
            let allow_local_test = std::env::var("TOKYO_L2_ALLOW_LOCALHOST_TEST_ENDPOINTS")
                .map(|v| v == "1")
                .unwrap_or(false);
            if let Err(errs) = cfg.validate_with_options(allow_local_test) {
                let rep = BlockedReport::new("collect", errs, vec!["Fix config validation failures".to_string()]);
                println!("{}", serde_json::to_string_pretty(&rep).unwrap());
                return ExitCode::from(2);
            }
            match run_live_collector(cfg, run_id, duration_secs).await {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => {
                    let rep = BlockedReport::new("collect", vec![e], vec!["Inspect runtime error".to_string()]);
                    let _ = writeln!(std::io::stdout(), "{}", serde_json::to_string_pretty(&rep).unwrap());
                    ExitCode::from(4)
                }
            }
        }
        Commands::ReplayFixture {
            config,
            fixture,
            out,
            boot_id,
        } => match run_replay_fixture(&config, &fixture, &out, &boot_id) {
            Ok(summary) => {
                println!("{}", serde_json::to_string_pretty(&summary).unwrap());
                ExitCode::SUCCESS
            }
            Err(e) => {
                let rep = BlockedReport::new(
                    "replay_fixture",
                    vec![e],
                    vec!["Verify fixture JSONL and config".to_string()],
                );
                println!("{}", serde_json::to_string_pretty(&rep).unwrap());
                ExitCode::from(2)
            }
        },
    }
}

fn run_replay_fixture(
    config_path: &PathBuf,
    fixture_path: &PathBuf,
    out_dir: &PathBuf,
    boot_id: &str,
) -> Result<ReplaySummary, String> {
    let cfg = CollectorConfig::load(config_path)?;
    cfg.validate().map_err(|errs| errs.join("; "))?;

    let mut selectors: HashMap<EndpointKind, CausalSelector> = HashMap::new();
    let mut writers: HashMap<ExchangeId, HourlyTsvZstWriter> = HashMap::new();

    let writer_opts = WriterOptions::from_config(
        &cfg,
        Some(boot_id.to_string()),
        cfg.deployment.partition_minutes < 60,
        None,
    );

    let mut add_ep = |ep: EndpointKind, syms: Vec<String>| {
        let ex = ep.exchange();
        writers.entry(ex).or_insert_with(|| {
            HourlyTsvZstWriter::with_options(
                out_dir.clone(),
                ex,
                boot_id.to_string(),
                cfg.deployment.zstd_level,
                writer_opts.clone(),
            )
        });
        selectors.insert(
            ep,
            CausalSelector::new(
                ep,
                syms,
                cfg.deployment.silence_watchdog_seconds,
                cfg.meta_daemon.dual_outage_alert_seconds,
            ),
        );
    };

    if cfg.exchanges.binance_futures.enabled {
        add_ep(
            EndpointKind::BinancePublic,
            cfg.exchanges.binance_futures.symbols.clone(),
        );
        add_ep(
            EndpointKind::BinanceMarket,
            cfg.exchanges.binance_futures.symbols.clone(),
        );
    }
    if cfg.exchanges.bybit_linear.enabled {
        add_ep(
            EndpointKind::BybitPublic,
            cfg.exchanges.bybit_linear.symbols.clone(),
        );
    }
    if cfg.exchanges.bitget_v3.enabled {
        add_ep(
            EndpointKind::BitgetPublic,
            cfg.exchanges.bitget_v3.symbols.clone(),
        );
    }
    if cfg.exchanges.gate_futures.enabled {
        add_ep(
            EndpointKind::GatePublic,
            cfg.exchanges.gate_futures.symbols.clone(),
        );
    }

    let content = fs::read_to_string(fixture_path)
        .map_err(|e| format!("read fixture {} failed: {}", fixture_path.display(), e))?;

    let mut total_written = 0u64;
    for (idx, line) in content.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let ev: FixtureEvent = serde_json::from_str(trimmed)
            .map_err(|e| format!("fixture line {} invalid JSON: {}", idx + 1, e))?;
        let (ep, output) = match ev {
            FixtureEvent::Frame {
                recv_wall_ns,
                recv_mono_ns,
                endpoint,
                conn_id,
                epoch,
                payload,
            } => {
                let sel = selectors
                    .get_mut(&endpoint)
                    .ok_or_else(|| format!("endpoint {:?} not enabled in config", endpoint))?;
                let frame = RawFrame {
                    recv_wall_ns,
                    recv_mono_ns,
                    exchange: endpoint.exchange(),
                    endpoint,
                    conn_id,
                    epoch,
                    payload,
                };
                (endpoint, sel.on_frame(frame))
            }
            FixtureEvent::RestSnapshot {
                recv_wall_ns,
                recv_mono_ns,
                endpoint,
                conn_id,
                symbol,
                last_update_id,
                raw_json,
            } => {
                let sel = selectors
                    .get_mut(&endpoint)
                    .ok_or_else(|| format!("endpoint {:?} not enabled", endpoint))?;
                (
                    endpoint,
                    sel.on_rest_snapshot(
                        conn_id,
                        &symbol,
                        last_update_id,
                        &raw_json,
                        recv_wall_ns,
                        recv_mono_ns,
                    ),
                )
            }
            FixtureEvent::Disconnect {
                recv_wall_ns,
                recv_mono_ns,
                endpoint,
                conn_id,
                reason,
            } => {
                let sel = selectors
                    .get_mut(&endpoint)
                    .ok_or_else(|| format!("endpoint {:?} not enabled", endpoint))?;
                (
                    endpoint,
                    sel.on_disconnect(conn_id, recv_wall_ns, recv_mono_ns, &reason),
                )
            }
            FixtureEvent::Reconnect {
                recv_mono_ns,
                endpoint,
                conn_id,
                epoch,
            } => {
                let sel = selectors
                    .get_mut(&endpoint)
                    .ok_or_else(|| format!("endpoint {:?} not enabled", endpoint))?;
                sel.on_reconnect(conn_id, epoch, recv_mono_ns);
                continue;
            }
            FixtureEvent::PlannedRotation {
                recv_wall_ns,
                recv_mono_ns,
                endpoint,
                target,
            } => {
                let sel = selectors
                    .get_mut(&endpoint)
                    .ok_or_else(|| format!("endpoint {:?} not enabled", endpoint))?;
                (
                    endpoint,
                    sel.on_planned_rotation(target, recv_wall_ns, recv_mono_ns),
                )
            }
            FixtureEvent::LocalLoss {
                recv_wall_ns,
                recv_mono_ns,
                endpoint,
                dropped,
                reason,
            } => {
                let sel = selectors
                    .get_mut(&endpoint)
                    .ok_or_else(|| format!("endpoint {:?} not enabled", endpoint))?;
                (
                    endpoint,
                    sel.on_local_loss(dropped, recv_wall_ns, recv_mono_ns, &reason),
                )
            }
        };

        let writer = writers
            .get_mut(&ep.exchange())
            .ok_or_else(|| format!("missing writer for {:?}", ep.exchange()))?;
        for rec in &output.records {
            writer.write_record(rec)?;
            total_written += 1;
        }
    }

    let mut manifests = Vec::new();
    for writer in writers.values_mut() {
        writer.seal_all()?;
        for s in writer.sealed_partitions() {
            manifests.push(s.manifest_path.to_string_lossy().to_string());
        }
    }
    manifests.sort();

    Ok(ReplaySummary {
        status: "ok".to_string(),
        boot_id: boot_id.to_string(),
        sealed_manifests: manifests,
        total_records_written: total_written,
    })
}
