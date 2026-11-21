use crate::config::CollectorConfig;
use crate::types::{CanonicalRecord, ConnId, ExchangeId, RecordKind};
use chrono::{DateTime, Datelike, Timelike, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_MIN: i64 = 60 * NS_PER_SEC;
const STARTUP_STUB_GRACE_NS: i64 = 5 * NS_PER_SEC;
const INCOMPLETE_TAIL_THRESHOLD_NS: i64 = 5 * NS_PER_SEC;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct HourPartitionKey {
    pub date: String,     // YYYY-MM-DD
    pub yyyymmdd: String, // YYYYMMDD
    pub hour: String,     // HH (00..23)
}

impl HourPartitionKey {
    pub fn from_wall_ns(wall_ns: i64) -> Self {
        let secs = wall_ns.div_euclid(NS_PER_SEC);
        let nsecs = wall_ns.rem_euclid(NS_PER_SEC) as u32;
        let dt = DateTime::<Utc>::from_timestamp(secs, nsecs)
            .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap());
        Self {
            date: format!("{:04}-{:02}-{:02}", dt.year(), dt.month(), dt.day()),
            yyyymmdd: format!("{:04}{:02}{:02}", dt.year(), dt.month(), dt.day()),
            hour: format!("{:02}", dt.hour()),
        }
    }
}

/// UTC-aligned half-open `[planned_start_wall_ns, planned_end_wall_ns)` partition window key
/// supporting 5, 15, 30, and 60 minute windows.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionWindowKey {
    pub partition_minutes: u32,
    pub planned_start_wall_ns: i64,
    pub planned_end_wall_ns: i64,
    pub date: String,      // YYYY-MM-DD
    pub yyyymmdd: String,  // YYYYMMDD
    pub hour: String,      // HH (00..23)
    pub minute: String,    // MM (00, 15, 30, 45, ...)
    pub window_id: String, // YYYY-MM-DDTHH:MMZ
}

impl PartitionWindowKey {
    pub fn from_wall_ns(wall_ns: i64, partition_minutes: u32) -> Self {
        let mins = match partition_minutes {
            5 | 15 | 30 | 60 => partition_minutes,
            _ => 60,
        };
        let window_ns = (mins as i64) * NS_PER_MIN;
        let planned_start_wall_ns = wall_ns.div_euclid(window_ns) * window_ns;
        let planned_end_wall_ns = planned_start_wall_ns.saturating_add(window_ns);
        let secs = planned_start_wall_ns.div_euclid(NS_PER_SEC);
        let dt = DateTime::<Utc>::from_timestamp(secs, 0)
            .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap());
        let date = format!("{:04}-{:02}-{:02}", dt.year(), dt.month(), dt.day());
        let yyyymmdd = format!("{:04}{:02}{:02}", dt.year(), dt.month(), dt.day());
        let hour = format!("{:02}", dt.hour());
        let minute = format!("{:02}", dt.minute());
        let window_id = format!("{}T{}:{}Z", date, hour, minute);
        Self {
            partition_minutes: mins,
            planned_start_wall_ns,
            planned_end_wall_ns,
            date,
            yyyymmdd,
            hour,
            minute,
            window_id,
        }
    }
}

impl Ord for PartitionWindowKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.planned_start_wall_ns
            .cmp(&other.planned_start_wall_ns)
            .then_with(|| self.partition_minutes.cmp(&other.partition_minutes))
    }
}

impl PartialOrd for PartitionWindowKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

fn default_manifest_partition_minutes() -> u32 {
    60
}

fn default_minute_utc() -> String {
    "00".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionManifest {
    pub schema_version: u32,
    pub exchange: String,
    pub date_utc: String,
    pub hour_utc: String,
    #[serde(default = "default_minute_utc")]
    pub minute_utc: String,
    #[serde(default = "default_manifest_partition_minutes")]
    pub partition_minutes: u32,
    #[serde(default)]
    pub window_id: String,
    #[serde(default)]
    pub planned_start_wall_ns: Option<i64>,
    #[serde(default)]
    pub planned_end_wall_ns: Option<i64>,
    #[serde(default)]
    pub run_id: Option<String>,
    #[serde(default)]
    pub segment_id: String,
    pub boot_id: String,
    pub file_name: String,
    pub bytes: u64,
    pub sha256: String,
    pub zstd_verified: bool,
    pub start_wall_ns: Option<i64>,
    pub end_wall_ns: Option<i64>,
    pub first_rx_seq: Option<u64>,
    pub last_rx_seq: Option<u64>,
    pub data_lines: u64,
    pub data_escaped_lines: u64,
    pub meta_lines: u64,
    pub local_loss_lines: u64,
    pub gap_start_lines: u64,
    pub switch_lines: u64,
    #[serde(default)]
    pub is_startup_stub: bool,
    #[serde(default)]
    pub settled_by_timer: bool,
    #[serde(default)]
    pub coverage_incomplete: bool,
    #[serde(default)]
    pub late_frames_accepted: u64,
    #[serde(default)]
    pub late_frames_rejected: u64,
    #[serde(default)]
    pub head_gap_ms: Option<i64>,
    #[serde(default)]
    pub tail_gap_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedPartition {
    pub data_path: PathBuf,
    pub manifest_path: PathBuf,
    pub manifest: PartitionManifest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteRecordOutcome {
    pub sealed: Option<SealedPartition>,
    pub accepted_as_market_data: bool,
    pub rejected_late_target_window_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct WriterOptions {
    pub partition_minutes: u32,
    pub run_id: Option<String>,
    pub allow_segmented_restart: bool,
    pub late_frame_grace_ns: i64,
    pub process_start_wall_ns: Option<i64>,
}

impl Default for WriterOptions {
    fn default() -> Self {
        Self {
            partition_minutes: 60,
            run_id: None,
            allow_segmented_restart: false,
            late_frame_grace_ns: 0,
            process_start_wall_ns: None,
        }
    }
}

impl WriterOptions {
    pub fn from_config(
        cfg: &CollectorConfig,
        run_id: Option<String>,
        allow_segmented_restart: bool,
        process_start_wall_ns: Option<i64>,
    ) -> Self {
        let eff_run_id = run_id
            .filter(|s| !s.trim().is_empty())
            .or_else(|| cfg.deployment.run_id.clone().filter(|s| !s.trim().is_empty()));
        Self {
            partition_minutes: cfg.deployment.partition_minutes,
            run_id: eff_run_id,
            allow_segmented_restart,
            late_frame_grace_ns: (cfg.deployment.late_frame_grace_ms as i64)
                .saturating_mul(1_000_000),
            process_start_wall_ns,
        }
    }
}

fn sanitize_tag(raw: &str, max_len: usize) -> String {
    let mut out = String::with_capacity(raw.len().min(max_len));
    let mut prev_underscore = false;
    for ch in raw.chars() {
        if out.len() >= max_len {
            break;
        }
        if ch.is_ascii_alphanumeric() || ch == '-' {
            out.push(ch);
            prev_underscore = false;
        } else if !prev_underscore && !out.is_empty() {
            out.push('_');
            prev_underscore = true;
        }
    }
    let trimmed = out.trim_matches('_').to_string();
    if trimmed.is_empty() {
        "seg".to_string()
    } else {
        trimmed
    }
}

/// Format a CanonicalRecord into a lossless single TSV-Raw line (no Base64).
pub fn encode_tsv_line(rec: &CanonicalRecord) -> String {
    format!(
        "{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
        rec.recv_wall_ns,
        rec.recv_mono_ns,
        rec.conn_id.as_str(),
        rec.epoch,
        rec.rx_seq,
        rec.kind.as_str(),
        rec.payload
    )
}

/// Parse a TSV-Raw line back into a CanonicalRecord, restoring exact raw UTF-8 bytes
/// if DATA_ESCAPED was used for rare payloads containing literal CR/LF/TAB.
pub fn decode_tsv_line(line: &str) -> Result<(CanonicalRecord, String), String> {
    let trimmed = line.strip_suffix('\n').unwrap_or(line);
    let mut parts = trimmed.splitn(7, '\t');
    let recv_wall_ns = parts
        .next()
        .ok_or("missing recv_wall_ns")?
        .parse::<i64>()
        .map_err(|e| format!("bad recv_wall_ns: {}", e))?;
    let recv_mono_ns = parts
        .next()
        .ok_or("missing recv_mono_ns")?
        .parse::<u64>()
        .map_err(|e| format!("bad recv_mono_ns: {}", e))?;
    let conn_id_str = parts.next().ok_or("missing conn_id")?;
    let conn_id = ConnId::from_str(conn_id_str)
        .ok_or_else(|| format!("invalid conn_id: {}", conn_id_str))?;
    let epoch = parts
        .next()
        .ok_or("missing epoch")?
        .parse::<u64>()
        .map_err(|e| format!("bad epoch: {}", e))?;
    let rx_seq = parts
        .next()
        .ok_or("missing rx_seq")?
        .parse::<u64>()
        .map_err(|e| format!("bad rx_seq: {}", e))?;
    let kind_str = parts.next().ok_or("missing kind")?;
    let kind =
        RecordKind::from_str(kind_str).ok_or_else(|| format!("invalid kind: {}", kind_str))?;
    let raw_col = parts.next().ok_or("missing payload")?.to_string();

    let restored_payload = if kind == RecordKind::DataEscaped {
        serde_json::from_str::<String>(&raw_col)
            .map_err(|e| format!("failed to unescape DATA_ESCAPED payload: {}", e))?
    } else {
        raw_col.clone()
    };

    Ok((
        CanonicalRecord {
            recv_wall_ns,
            recv_mono_ns,
            conn_id,
            epoch,
            rx_seq,
            kind,
            payload: raw_col,
        },
        restored_payload,
    ))
}

struct OpenPartition {
    key: PartitionWindowKey,
    segment_id: String,
    partial_path: PathBuf,
    final_path: PathBuf,
    manifest_path: PathBuf,
    encoder: zstd::stream::write::Encoder<'static, BufWriter<File>>,
    start_wall_ns: Option<i64>,
    end_wall_ns: Option<i64>,
    max_mono_ns: u64,
    first_rx_seq: Option<u64>,
    last_rx_seq: Option<u64>,
    data_lines: u64,
    data_escaped_lines: u64,
    meta_lines: u64,
    local_loss_lines: u64,
    gap_start_lines: u64,
    switch_lines: u64,
    is_first_window: bool,
    is_startup_stub: bool,
    settled_by_timer: bool,
    late_frames_accepted: u64,
    late_frames_rejected: u64,
}

struct BackgroundSeal {
    records: u64,
    task: JoinHandle<Result<SealedPartition, String>>,
}

/// Seamless UTC-aligned TSV-Raw Zstd writer (5/15/30/60m) with bounded cross-window late-frame
/// handling, quiet-exchange boundary settlement, restart-safe immutable segments, and atomic
/// `.partial` -> verify -> rename + manifest sealing.
pub struct HourlyTsvZstWriter {
    data_root: PathBuf,
    exchange: ExchangeId,
    boot_id: String,
    zstd_level: i32,
    options: WriterOptions,
    opened_window_count: u64,
    next_rx_seq: u64,
    current: Option<OpenPartition>,
    draining: Option<OpenPartition>,
    sealed: Vec<SealedPartition>,
    seal_limit: Option<Arc<Semaphore>>,
    background_seals: Vec<BackgroundSeal>,
    pending_write_loss: u64,
}

impl HourlyTsvZstWriter {
    /// Legacy 60-minute single-file constructor (preserves exact legacy naming & overwrite refusal).
    pub fn new(data_root: PathBuf, exchange: ExchangeId, boot_id: String, zstd_level: i32) -> Self {
        Self::with_options(
            data_root,
            exchange,
            boot_id,
            zstd_level,
            WriterOptions::default(),
        )
    }

    /// Configurable partition writer supporting 5/15/30/60m windows, bounded late frames,
    /// quiet-exchange settlement, and restart-safe segment naming.
    pub fn with_options(
        data_root: PathBuf,
        exchange: ExchangeId,
        boot_id: String,
        zstd_level: i32,
        options: WriterOptions,
    ) -> Self {
        Self {
            data_root,
            exchange,
            boot_id,
            zstd_level,
            options,
            opened_window_count: 0,
            next_rx_seq: 0,
            current: None,
            draining: None,
            sealed: Vec::new(),
            seal_limit: None,
            background_seals: Vec::new(),
            pending_write_loss: 0,
        }
    }

    pub fn sealed_partitions(&self) -> &[SealedPartition] {
        &self.sealed
    }

    pub fn has_open_partition(&self) -> bool {
        self.current.is_some() || self.draining.is_some()
    }

    pub fn has_draining_partition(&self) -> bool {
        self.draining.is_some()
    }

    pub fn partition_minutes(&self) -> u32 {
        self.options.partition_minutes
    }

    pub fn enable_background_sealing(&mut self, limit: Arc<Semaphore>) {
        self.seal_limit = Some(limit);
    }

    pub fn poll_background_seals(&mut self) -> Vec<Result<SealedPartition, String>> {
        use futures_util::FutureExt;

        let mut pending = Vec::new();
        let mut completed = Vec::new();
        for mut seal in self.background_seals.drain(..) {
            match (&mut seal.task).now_or_never() {
                Some(result) => {
                    let result = result
                        .map_err(|e| format!("seal task failed: {e}"))
                        .and_then(|r| r);
                    if result.is_err() {
                        self.pending_write_loss =
                            self.pending_write_loss.saturating_add(seal.records);
                    }
                    completed.push(result);
                }
                None => pending.push(seal),
            }
        }
        self.background_seals = pending;
        completed
    }

    pub fn take_background_seals(&mut self) -> Vec<JoinHandle<Result<SealedPartition, String>>> {
        self.background_seals
            .drain(..)
            .map(|seal| seal.task)
            .collect()
    }

    pub fn note_write_loss(&mut self) {
        self.pending_write_loss = self.pending_write_loss.saturating_add(1);
    }

    pub fn pending_write_loss(&self) -> u64 {
        self.pending_write_loss
    }

    fn dispatch_seal(&mut self, open: OpenPartition) -> Result<Option<SealedPartition>, String> {
        if let Some(limit) = self.seal_limit.clone() {
            if self.background_seals.len() >= 8 {
                return Err("partition seal backlog exceeded eight partitions".into());
            }
            let records = open.data_lines + open.data_escaped_lines + open.meta_lines;
            let exchange = self.exchange;
            let boot_id = self.boot_id.clone();
            let run_id = self.options.run_id.clone();
            let task = tokio::spawn(async move {
                let _permit = limit.acquire_owned().await.map_err(|e| e.to_string())?;
                tokio::task::spawn_blocking(move || {
                    Self::seal_partition(open, exchange, &boot_id, run_id.as_deref())
                })
                .await
                .map_err(|e| format!("blocking seal failed: {e}"))?
            });
            self.background_seals.push(BackgroundSeal { records, task });
            Ok(None)
        } else {
            let sealed =
                Self::seal_partition(open, self.exchange, &self.boot_id, self.options.run_id.as_deref())?;
            self.sealed.push(sealed.clone());
            Ok(Some(sealed))
        }
    }

    /// Settle expired draining windows (past late-frame grace) and quiet-exchange active windows
    /// whose boundary + late-frame grace has elapsed with no new post-boundary frames.
    pub fn settle_expired_windows(
        &mut self,
        now_wall_ns: i64,
    ) -> Result<Vec<SealedPartition>, String> {
        let grace = self.options.late_frame_grace_ns;
        let mut sealed_now = Vec::new();

        let drain_expired = self
            .draining
            .as_ref()
            .is_some_and(|d| now_wall_ns >= d.key.planned_end_wall_ns.saturating_add(grace));
        if drain_expired {
            if let Some(open) = self.draining.take() {
                if let Some(s) = self.dispatch_seal(open)? {
                    sealed_now.push(s);
                }
            }
        }

        let current_expired = self
            .current
            .as_ref()
            .is_some_and(|c| now_wall_ns >= c.key.planned_end_wall_ns.saturating_add(grace));
        if current_expired {
            if let Some(mut open) = self.current.take() {
                open.settled_by_timer = true;
                if let Some(s) = self.dispatch_seal(open)? {
                    sealed_now.push(s);
                }
            }
        }

        Ok(sealed_now)
    }

    pub fn write_record(&mut self, rec: &CanonicalRecord) -> Result<Option<SealedPartition>, String> {
        self.write_record_with_outcome(rec).map(|o| o.sealed)
    }

    pub fn write_record_with_outcome(
        &mut self,
        rec: &CanonicalRecord,
    ) -> Result<WriteRecordOutcome, String> {
        let key = PartitionWindowKey::from_wall_ns(rec.recv_wall_ns, self.options.partition_minutes);
        let grace = self.options.late_frame_grace_ns;
        let mut just_sealed: Option<SealedPartition> = None;
        let is_market_data = !matches!(rec.kind, RecordKind::Meta);

        // First flush any draining previous window whose bounded grace period has elapsed.
        if self
            .draining
            .as_ref()
            .is_some_and(|d| rec.recv_wall_ns >= d.key.planned_end_wall_ns.saturating_add(grace))
        {
            if let Some(old_drain) = self.draining.take() {
                if let Some(s) = self.dispatch_seal(old_drain)? {
                    just_sealed = Some(s);
                }
            }
        }

        match self.current.as_ref().map(|c| key.cmp(&c.key)) {
            None => {
                self.open_window(key)?;
                self.flush_pending_loss_into_current(rec)?;
                self.append_to_current(rec)?;
                return Ok(WriteRecordOutcome {
                    sealed: just_sealed,
                    accepted_as_market_data: is_market_data,
                    rejected_late_target_window_id: None,
                });
            }
            Some(std::cmp::Ordering::Equal) => {
                self.flush_pending_loss_into_current(rec)?;
                self.append_to_current(rec)?;
                return Ok(WriteRecordOutcome {
                    sealed: just_sealed,
                    accepted_as_market_data: is_market_data,
                    rejected_late_target_window_id: None,
                });
            }
            Some(std::cmp::Ordering::Greater) => {
                // Rollover into newer UTC window: keep previous window open in `draining` while
                // within bounded grace, otherwise hand off to background/sync seal immediately.
                if let Some(prev_drain) = self.draining.take() {
                    if let Some(s) = self.dispatch_seal(prev_drain)? {
                        just_sealed = Some(s);
                    }
                }
                let prev_current = self.current.take().expect("current must exist on rollover");
                if grace > 0
                    && rec.recv_wall_ns < prev_current.key.planned_end_wall_ns.saturating_add(grace)
                {
                    self.draining = Some(prev_current);
                } else if let Some(s) = self.dispatch_seal(prev_current)? {
                    just_sealed = Some(s);
                }
                self.open_window(key)?;
                self.flush_pending_loss_into_current(rec)?;
                self.append_to_current(rec)?;
                return Ok(WriteRecordOutcome {
                    sealed: just_sealed,
                    accepted_as_market_data: is_market_data,
                    rejected_late_target_window_id: None,
                });
            }
            Some(std::cmp::Ordering::Less) => {
                // Late frame belonging to an earlier UTC window.
                let fits_draining = self.draining.as_ref().is_some_and(|d| d.key == key);
                if fits_draining {
                    self.append_to_draining(rec)?;
                    return Ok(WriteRecordOutcome {
                        sealed: just_sealed,
                        accepted_as_market_data: is_market_data,
                        rejected_late_target_window_id: None,
                    });
                }
                if grace == 0 && self.seal_limit.is_some() {
                    return Err("late frame for an earlier UTC window".into());
                }
                // Bounded late-frame rejection: record explicit auditable META inside `current`
                // without violating `current`'s `[planned_start, planned_end)` timestamp invariant,
                // and NEVER mark this rejected frame as a successful market-data write.
                let (cur_window_id, meta_wall_ns, meta_mono_ns) = {
                    let cur = self.current.as_mut().expect("current must exist");
                    cur.late_frames_rejected = cur.late_frames_rejected.saturating_add(1);
                    let w_ns = cur
                        .end_wall_ns
                        .unwrap_or(cur.key.planned_start_wall_ns)
                        .clamp(cur.key.planned_start_wall_ns, cur.key.planned_end_wall_ns - 1);
                    (cur.key.window_id.clone(), w_ns, cur.max_mono_ns.max(rec.recv_mono_ns))
                };
                let target_wid = key.window_id.clone();
                let rejected_meta = CanonicalRecord {
                    recv_wall_ns: meta_wall_ns,
                    recv_mono_ns: meta_mono_ns,
                    conn_id: rec.conn_id,
                    epoch: rec.epoch,
                    rx_seq: 0,
                    kind: RecordKind::Meta,
                    payload: serde_json::json!({
                        "event": "LATE_FRAME_REJECTED",
                        "dropped_frames": 1,
                        "orig_recv_wall_ns": rec.recv_wall_ns,
                        "orig_recv_mono_ns": rec.recv_mono_ns,
                        "orig_kind": rec.kind.as_str(),
                        "target_window_id": target_wid,
                        "loss_attributed_window_id": key.window_id,
                        "target_planned_start_wall_ns": key.planned_start_wall_ns,
                        "target_planned_end_wall_ns": key.planned_end_wall_ns,
                        "target_partition_minutes": key.partition_minutes,
                        "current_window_id": cur_window_id,
                        "grace_ms": grace / 1_000_000,
                    })
                    .to_string(),
                };
                self.append_to_current(&rejected_meta)?;
                return Ok(WriteRecordOutcome {
                    sealed: just_sealed,
                    accepted_as_market_data: false,
                    rejected_late_target_window_id: Some(key.window_id),
                });
            }
        }
    }

    fn flush_pending_loss_into_current(&mut self, rec: &CanonicalRecord) -> Result<(), String> {
        if self.pending_write_loss > 0 {
            let marker = CanonicalRecord {
                kind: RecordKind::Meta,
                payload: serde_json::json!({
                    "event": "LOCAL_LOSS",
                    "dropped_frames": self.pending_write_loss,
                    "reason": "disk_write_or_seal_failed"
                })
                .to_string(),
                ..rec.clone()
            };
            self.append_to_current(&marker)?;
            self.pending_write_loss = 0;
        }
        Ok(())
    }

    fn append_to_open(
        next_rx_seq: &mut u64,
        open: &mut OpenPartition,
        rec: &CanonicalRecord,
        is_late_accepted: bool,
    ) -> Result<(), String> {
        *next_rx_seq += 1;
        let mut disk_rec = rec.clone();
        disk_rec.rx_seq = *next_rx_seq;

        let line = encode_tsv_line(&disk_rec);
        open.encoder
            .write_all(line.as_bytes())
            .map_err(|e| format!("zstd write failed: {}", e))?;

        if matches!(rec.kind, RecordKind::Data | RecordKind::DataEscaped) {
            open.start_wall_ns = Some(
                open.start_wall_ns
                    .map_or(disk_rec.recv_wall_ns, |prev| prev.min(disk_rec.recv_wall_ns)),
            );
            open.end_wall_ns = Some(
                open.end_wall_ns
                    .map_or(disk_rec.recv_wall_ns, |prev| prev.max(disk_rec.recv_wall_ns)),
            );
        }
        open.max_mono_ns = open.max_mono_ns.max(disk_rec.recv_mono_ns);
        if open.first_rx_seq.is_none() {
            open.first_rx_seq = Some(disk_rec.rx_seq);
        }
        open.last_rx_seq = Some(disk_rec.rx_seq);
        if is_late_accepted {
            open.late_frames_accepted = open.late_frames_accepted.saturating_add(1);
        }

        match rec.kind {
            RecordKind::Data => open.data_lines += 1,
            RecordKind::DataEscaped => open.data_escaped_lines += 1,
            RecordKind::Meta => {
                open.meta_lines += 1;
                if rec.payload.contains("\"LOCAL_LOSS\"") {
                    open.local_loss_lines += 1;
                }
                if rec.payload.contains("\"GAP_START\"") {
                    open.gap_start_lines += 1;
                }
                if rec.payload.contains("\"SWITCH\"") {
                    open.switch_lines += 1;
                }
            }
        }

        Ok(())
    }

    fn append_to_current(&mut self, rec: &CanonicalRecord) -> Result<(), String> {
        let open = self.current.as_mut().expect("current partition must be open");
        Self::append_to_open(&mut self.next_rx_seq, open, rec, false)
    }

    fn append_to_draining(&mut self, rec: &CanonicalRecord) -> Result<(), String> {
        let open = self
            .draining
            .as_mut()
            .expect("draining partition must be open");
        Self::append_to_open(&mut self.next_rx_seq, open, rec, true)
    }

    /// Flush, fsync, verify zstd integrity, hash SHA-256, atomically rename, and emit manifest
    /// for all open partitions (`draining` first, then `current`).
    pub fn seal_all(&mut self) -> Result<Vec<SealedPartition>, String> {
        let mut out = Vec::new();
        if let Some(drain) = self.draining.take() {
            let s = Self::seal_partition(
                drain,
                self.exchange,
                &self.boot_id,
                self.options.run_id.as_deref(),
            )?;
            self.sealed.push(s.clone());
            out.push(s);
        }
        if let Some(cur) = self.current.take() {
            let s = Self::seal_partition(
                cur,
                self.exchange,
                &self.boot_id,
                self.options.run_id.as_deref(),
            )?;
            self.sealed.push(s.clone());
            out.push(s);
        }
        Ok(out)
    }

    /// Flush, fsync, verify zstd integrity, hash SHA-256, atomically rename, and emit manifest.
    /// On any failure, leaves the `.partial` file untouched (never promotes corrupt/unverified data).
    pub fn seal_current(&mut self) -> Result<Option<SealedPartition>, String> {
        if let Some(drain) = self.draining.take() {
            let s = Self::seal_partition(
                drain,
                self.exchange,
                &self.boot_id,
                self.options.run_id.as_deref(),
            )?;
            self.sealed.push(s);
        }
        let Some(open) = self.current.take() else {
            return Ok(None);
        };

        let sealed = Self::seal_partition(
            open,
            self.exchange,
            &self.boot_id,
            self.options.run_id.as_deref(),
        )?;
        self.sealed.push(sealed.clone());
        Ok(Some(sealed))
    }

    fn seal_partition(
        open: OpenPartition,
        exchange: ExchangeId,
        boot_id: &str,
        run_id: Option<&str>,
    ) -> Result<SealedPartition, String> {
        let mut buf_writer = open
            .encoder
            .finish()
            .map_err(|e| format!("zstd finish failed: {}", e))?;
        buf_writer
            .flush()
            .map_err(|e| format!("flush failed: {}", e))?;
        let file = buf_writer
            .into_inner()
            .map_err(|e| format!("into_inner failed: {}", e))?;
        file.sync_all()
            .map_err(|e| format!("fsync failed: {}", e))?;
        drop(file);

        // Mandatory zstd full decompression test + SHA-256 computation before atomic rename
        let (bytes, sha256) = verify_zstd_and_sha256(&open.partial_path)?;

        if open.final_path.exists() {
            return Err(format!(
                "Refusing to overwrite existing sealed partition {} (keeping partial {})",
                open.final_path.display(),
                open.partial_path.display()
            ));
        }

        fs::rename(&open.partial_path, &open.final_path).map_err(|e| {
            format!(
                "atomic rename {} -> {} failed: {}",
                open.partial_path.display(),
                open.final_path.display(),
                e
            )
        })?;

        let file_name = open
            .final_path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string();

        let head_gap_ms = open.start_wall_ns.map(|s| {
            s.saturating_sub(open.key.planned_start_wall_ns)
                .max(0)
                / 1_000_000
        });
        let tail_gap_ms = open.end_wall_ns.map(|e| {
            open.key
                .planned_end_wall_ns
                .saturating_sub(e)
                .max(0)
                / 1_000_000
        });
        let is_startup_stub = open.is_startup_stub
            || (open.is_first_window
                && open.start_wall_ns.is_some_and(|s| {
                    s > open
                        .key
                        .planned_start_wall_ns
                        .saturating_add(STARTUP_STUB_GRACE_NS)
                }));
        let tail_incomplete = open.settled_by_timer
            && open.end_wall_ns.map_or(true, |e| {
                open.key.planned_end_wall_ns.saturating_sub(e) > INCOMPLETE_TAIL_THRESHOLD_NS
            });
        let market_data_records = open.data_lines + open.data_escaped_lines;
        let insufficient_market_data = market_data_records < 2;
        let coverage_incomplete = is_startup_stub || tail_incomplete || insufficient_market_data;

        let manifest = PartitionManifest {
            schema_version: 2,
            exchange: exchange.as_dir_name().to_string(),
            date_utc: open.key.date.clone(),
            hour_utc: open.key.hour.clone(),
            minute_utc: open.key.minute.clone(),
            partition_minutes: open.key.partition_minutes,
            window_id: open.key.window_id.clone(),
            planned_start_wall_ns: Some(open.key.planned_start_wall_ns),
            planned_end_wall_ns: Some(open.key.planned_end_wall_ns),
            run_id: run_id.map(str::to_string),
            segment_id: open.segment_id,
            boot_id: boot_id.to_string(),
            file_name,
            bytes,
            sha256,
            zstd_verified: true,
            start_wall_ns: open.start_wall_ns,
            end_wall_ns: open.end_wall_ns,
            first_rx_seq: open.first_rx_seq,
            last_rx_seq: open.last_rx_seq,
            data_lines: open.data_lines,
            data_escaped_lines: open.data_escaped_lines,
            meta_lines: open.meta_lines,
            local_loss_lines: open.local_loss_lines,
            gap_start_lines: open.gap_start_lines,
            switch_lines: open.switch_lines,
            is_startup_stub,
            settled_by_timer: open.settled_by_timer,
            coverage_incomplete,
            late_frames_accepted: open.late_frames_accepted,
            late_frames_rejected: open.late_frames_rejected,
            head_gap_ms,
            tail_gap_ms,
        };

        let manifest_tmp = open.manifest_path.with_extension("json.partial");
        let manifest_bytes = serde_json::to_vec_pretty(&manifest)
            .map_err(|e| format!("serialize manifest failed: {}", e))?;
        {
            let mut mf = File::create(&manifest_tmp)
                .map_err(|e| format!("create manifest tmp failed: {}", e))?;
            mf.write_all(&manifest_bytes)
                .map_err(|e| format!("write manifest failed: {}", e))?;
            mf.write_all(b"\n")
                .map_err(|e| format!("write manifest newline failed: {}", e))?;
            mf.sync_all()
                .map_err(|e| format!("fsync manifest failed: {}", e))?;
        }
        fs::rename(&manifest_tmp, &open.manifest_path)
            .map_err(|e| format!("atomic rename manifest failed: {}", e))?;

        Ok(SealedPartition {
            data_path: open.final_path,
            manifest_path: open.manifest_path,
            manifest,
        })
    }

    fn resolve_segment_paths(
        &self,
        dir: &Path,
        key: &PartitionWindowKey,
    ) -> Result<(String, PathBuf, PathBuf, PathBuf), String> {
        let ex_name = self.exchange.as_dir_name();

        if !self.options.allow_segmented_restart && key.partition_minutes == 60 {
            let stem = format!("{}_{}_{}", ex_name, key.yyyymmdd, key.hour);
            let final_path = dir.join(format!("{}.tsv.zst", stem));
            let partial_path = dir.join(format!("{}.tsv.zst.partial", stem));
            let manifest_path = dir.join(format!("{}.manifest.json", stem));

            if final_path.exists() {
                return Err(format!(
                    "Refusing to overwrite existing immutable sealed partition {}",
                    final_path.display()
                ));
            }
            if partial_path.exists() {
                return Err(format!(
                    "Refusing to overwrite existing unsealed partial file {} (preserve/quarantine before restarting in the same UTC hour)",
                    partial_path.display()
                ));
            }
            return Ok((stem, final_path, partial_path, manifest_path));
        }

        let mut boot_hasher = Sha256::new();
        boot_hasher.update(self.boot_id.as_bytes());
        let boot_hex = format!("{:x}", boot_hasher.finalize());
        let boot_prefix = sanitize_tag(self.options.run_id.as_deref().unwrap_or(&self.boot_id), 16);
        let boot_tag = format!("{}_{}", boot_prefix, &boot_hex[..8]);
        let base_stem = if key.partition_minutes == 60 {
            format!("{}_{}_{}_{}", ex_name, key.yyyymmdd, key.hour, boot_tag)
        } else {
            format!(
                "{}_{}_{}{}_p{}m_{}",
                ex_name, key.yyyymmdd, key.hour, key.minute, key.partition_minutes, boot_tag
            )
        };

        for seg_idx in 1..=999u32 {
            let stem = if seg_idx == 1 {
                base_stem.clone()
            } else {
                format!("{}_s{}", base_stem, seg_idx)
            };
            let final_path = dir.join(format!("{}.tsv.zst", stem));
            let partial_path = dir.join(format!("{}.tsv.zst.partial", stem));
            let manifest_path = dir.join(format!("{}.manifest.json", stem));
            if !final_path.exists() && !partial_path.exists() && !manifest_path.exists() {
                return Ok((stem, final_path, partial_path, manifest_path));
            }
        }

        Err(format!(
            "Exhausted immutable segment slots for {} in {}",
            base_stem,
            dir.display()
        ))
    }

    fn open_window(&mut self, key: PartitionWindowKey) -> Result<(), String> {
        let dir = self
            .data_root
            .join("raw")
            .join(self.exchange.as_dir_name())
            .join(&key.date);
        fs::create_dir_all(&dir)
            .map_err(|e| format!("create_dir_all {} failed: {}", dir.display(), e))?;

        let (segment_id, final_path, partial_path, manifest_path) =
            self.resolve_segment_paths(&dir, &key)?;

        let file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&partial_path)
            .map_err(|e| format!("create {} failed: {}", partial_path.display(), e))?;
        let encoder = zstd::stream::write::Encoder::new(BufWriter::new(file), self.zstd_level)
            .map_err(|e| format!("zstd encoder init failed: {}", e))?;

        let is_first_window = self.opened_window_count == 0;
        self.opened_window_count = self.opened_window_count.saturating_add(1);
        let is_startup_stub = is_first_window
            && self.options.process_start_wall_ns.is_some_and(|start_ns| {
                start_ns > key.planned_start_wall_ns.saturating_add(STARTUP_STUB_GRACE_NS)
            });

        self.current = Some(OpenPartition {
            key,
            segment_id,
            partial_path,
            final_path,
            manifest_path,
            encoder,
            start_wall_ns: None,
            end_wall_ns: None,
            max_mono_ns: 0,
            first_rx_seq: None,
            last_rx_seq: None,
            data_lines: 0,
            data_escaped_lines: 0,
            meta_lines: 0,
            local_loss_lines: 0,
            gap_start_lines: 0,
            switch_lines: 0,
            is_first_window,
            is_startup_stub,
            settled_by_timer: false,
            late_frames_accepted: 0,
            late_frames_rejected: 0,
        });
        Ok(())
    }
}

/// Full zstd stream decompression integrity test + exact compressed file byte count & SHA-256.
pub fn verify_zstd_and_sha256(path: &Path) -> Result<(u64, String), String> {
    let mut file = File::open(path)
        .map_err(|e| format!("open {} for sha256 failed: {}", path.display(), e))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 65536];
    let mut total_bytes = 0u64;
    loop {
        let n = file
            .read(&mut buf)
            .map_err(|e| format!("read {} failed: {}", path.display(), e))?;
        if n == 0 {
            break;
        }
        total_bytes += n as u64;
        hasher.update(&buf[..n]);
    }
    let sha_hex = format!("{:x}", hasher.finalize());

    // Stream-decompress entire file to validate zstd frame integrity (equivalent to `zstd -t`).
    let file2 = File::open(path)
        .map_err(|e| format!("open {} for zstd test failed: {}", path.display(), e))?;
    let mut decoder = zstd::stream::read::Decoder::new(BufReader::new(file2))
        .map_err(|e| format!("zstd decoder init failed: {}", e))?;
    loop {
        let n = decoder
            .read(&mut buf)
            .map_err(|e| format!("zstd integrity decompression failed: {}", e))?;
        if n == 0 {
            break;
        }
    }

    Ok((total_bytes, sha_hex))
}

/// Read and decode all records from a sealed `.tsv.zst` file (for tests & verification).
pub fn read_tsv_zst_records(path: &Path) -> Result<Vec<(CanonicalRecord, String)>, String> {
    let file = File::open(path).map_err(|e| format!("open {} failed: {}", path.display(), e))?;
    let decoder = zstd::stream::read::Decoder::new(BufReader::new(file))
        .map_err(|e| format!("zstd decoder failed: {}", e))?;
    let reader = BufReader::new(decoder);
    let mut records = Vec::new();
    for (idx, line_res) in reader.lines().enumerate() {
        let line = line_res.map_err(|e| format!("read line {} failed: {}", idx + 1, e))?;
        if line.is_empty() {
            continue;
        }
        records.push(decode_tsv_line(&line).map_err(|e| format!("line {}: {}", idx + 1, e))?);
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rollover_accepts_new_hour_while_previous_seal_is_waiting() {
        let root = tempfile::tempdir().unwrap();
        let mut writer = HourlyTsvZstWriter::new(
            root.path().into(),
            ExchangeId::GateFutures,
            "test".into(),
            3,
        );
        let limit = Arc::new(Semaphore::new(0));
        writer.enable_background_sealing(Arc::clone(&limit));
        let mut record = CanonicalRecord {
            recv_wall_ns: 0,
            recv_mono_ns: 0,
            conn_id: ConnId::A,
            epoch: 1,
            rx_seq: 0,
            kind: RecordKind::Data,
            payload: "{}".into(),
        };
        writer.write_record(&record).unwrap();
        record.recv_wall_ns = 3_600_000_000_000;
        assert!(writer.write_record(&record).unwrap().is_none());
        assert_eq!(writer.background_seals.len(), 1);
        assert!(writer.poll_background_seals().is_empty());
        writer.note_write_loss();
        writer.write_record(&record).unwrap();
        limit.add_permits(1);
        let task = writer.take_background_seals().pop().unwrap();
        let previous = task.await.unwrap().unwrap();
        assert_eq!(previous.manifest.data_lines, 1);
        let current = writer.seal_current().unwrap().unwrap();
        assert_eq!(current.manifest.data_lines, 2);
        assert_eq!(current.manifest.local_loss_lines, 1);
    }

    #[test]
    fn fifteen_minute_bounded_late_frame_quiet_settlement_midnight_and_same_window_restart() {
        let root = tempfile::tempdir().unwrap();
        // 2026-11-21T23:45:00Z
        let w0_start = 1_795_304_700_000_000_000i64;
        let w1_start = w0_start + 15 * NS_PER_MIN; // 2026-11-22T00:00:00Z (UTC midnight!)

        let opts = WriterOptions {
            partition_minutes: 15,
            run_id: Some("trial_batch_1".to_string()),
            allow_segmented_restart: true,
            late_frame_grace_ns: 2 * NS_PER_SEC,
            process_start_wall_ns: Some(w0_start + 60 * NS_PER_SEC), // started 1m into w0 -> startup stub!
        };
        let mut w = HourlyTsvZstWriter::with_options(
            root.path().into(),
            ExchangeId::BinanceFutures,
            "trial_batch_1_bootA".into(),
            3,
            opts,
        );

        let mk = |wall_ns: i64, mono_ns: u64, payload: &str| CanonicalRecord {
            recv_wall_ns: wall_ns,
            recv_mono_ns: mono_ns,
            conn_id: ConnId::A,
            epoch: 1,
            rx_seq: 0,
            kind: RecordKind::Data,
            payload: payload.into(),
        };

        // 1. Frame in 23:45Z window
        w.write_record(&mk(w0_start + 60 * NS_PER_SEC, 10, r#"{"n":1}"#))
            .unwrap();
        // 2. First frame of 00:00Z (next UTC day!) dequeued BEFORE tail frame of 23:45Z
        assert!(w
            .write_record(&mk(w1_start + 200_000_000, 20, r#"{"n":2}"#))
            .unwrap()
            .is_none());
        assert!(w.has_draining_partition());

        // 3. Tail frame of 23:45Z arrives 300ms after rollover (within 2s grace!) -> accepted into draining!
        assert!(w
            .write_record(&mk(w1_start - 10_000_000, 19, r#"{"n":3}"#))
            .unwrap()
            .is_none());

        // 4. Frame at w1_start + 2.5s triggers sealing of draining 23:45Z window
        let sealed_w0 = w
            .write_record(&mk(w1_start + 2_500_000_000, 30, r#"{"n":4}"#))
            .unwrap()
            .expect("draining window must seal once grace elapsed");
        assert_eq!(sealed_w0.manifest.date_utc, "2026-11-21");
        assert_eq!(sealed_w0.manifest.hour_utc, "23");
        assert_eq!(sealed_w0.manifest.minute_utc, "45");
        assert_eq!(sealed_w0.manifest.partition_minutes, 15);
        assert_eq!(sealed_w0.manifest.window_id, "2026-11-21T23:45Z");
        assert_eq!(sealed_w0.manifest.is_startup_stub, true);
        assert_eq!(sealed_w0.manifest.late_frames_accepted, 1);
        assert_eq!(sealed_w0.manifest.data_lines, 2);
        assert_eq!(sealed_w0.manifest.run_id.as_deref(), Some("trial_batch_1"));

        // 5. Very late frame for already-sealed 23:45Z -> rejected explicitly with audit META in w1
        w.write_record(&mk(w1_start - 5_000_000, 31, r#"{"n":"too_late"}"#))
            .unwrap();

        // 6. Quiet exchange settlement: no further frames arrive past w1 end + 2s grace -> timer settles w1!
        let timer_sealed = w
            .settle_expired_windows(w1_start + 15 * NS_PER_MIN + 2 * NS_PER_SEC)
            .unwrap();
        assert_eq!(timer_sealed.len(), 1);
        let m1 = &timer_sealed[0].manifest;
        assert_eq!(m1.date_utc, "2026-11-22");
        assert_eq!(m1.window_id, "2026-11-22T00:00Z");
        assert_eq!(m1.is_startup_stub, false);
        assert_eq!(m1.settled_by_timer, true);
        assert_eq!(m1.coverage_incomplete, true);
        assert_eq!(m1.late_frames_rejected, 1);

        // 7. Same-window restart in 2026-11-22T00:00Z produces a SECOND non-overwriting immutable segment!
        let mut w_restart = HourlyTsvZstWriter::with_options(
            root.path().into(),
            ExchangeId::BinanceFutures,
            "trial_batch_1_bootB".into(),
            3,
            WriterOptions {
                partition_minutes: 15,
                run_id: Some("trial_batch_1".to_string()),
                allow_segmented_restart: true,
                late_frame_grace_ns: 2 * NS_PER_SEC,
                process_start_wall_ns: Some(w1_start + 5 * NS_PER_MIN),
            },
        );
        w_restart
            .write_record(&mk(w1_start + 5 * NS_PER_MIN, 1, r#"{"n":"restart"}"#))
            .unwrap();
        let seg2 = w_restart.seal_current().unwrap().unwrap();
        assert_ne!(seg2.data_path, timer_sealed[0].data_path);
        assert!(timer_sealed[0].data_path.exists());
        assert!(seg2.data_path.exists());
        assert_eq!(seg2.manifest.window_id, "2026-11-22T00:00Z");
    }
}
