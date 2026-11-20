use crate::types::{CanonicalRecord, ConnId, ExchangeId, RecordKind};
use chrono::{DateTime, Datelike, Timelike, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HourPartitionKey {
    pub date: String,     // YYYY-MM-DD
    pub yyyymmdd: String, // YYYYMMDD
    pub hour: String,     // HH (00..23)
}

impl HourPartitionKey {
    pub fn from_wall_ns(wall_ns: i64) -> Self {
        let secs = wall_ns.div_euclid(1_000_000_000);
        let nsecs = wall_ns.rem_euclid(1_000_000_000) as u32;
        let dt = DateTime::<Utc>::from_timestamp(secs, nsecs)
            .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap());
        Self {
            date: format!("{:04}-{:02}-{:02}", dt.year(), dt.month(), dt.day()),
            yyyymmdd: format!("{:04}{:02}{:02}", dt.year(), dt.month(), dt.day()),
            hour: format!("{:02}", dt.hour()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionManifest {
    pub schema_version: u32,
    pub exchange: String,
    pub date_utc: String,
    pub hour_utc: String,
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedPartition {
    pub data_path: PathBuf,
    pub manifest_path: PathBuf,
    pub manifest: PartitionManifest,
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

struct OpenHour {
    key: HourPartitionKey,
    partial_path: PathBuf,
    final_path: PathBuf,
    manifest_path: PathBuf,
    encoder: zstd::stream::write::Encoder<'static, BufWriter<File>>,
    start_wall_ns: Option<i64>,
    end_wall_ns: Option<i64>,
    first_rx_seq: Option<u64>,
    last_rx_seq: Option<u64>,
    data_lines: u64,
    data_escaped_lines: u64,
    meta_lines: u64,
    local_loss_lines: u64,
    gap_start_lines: u64,
    switch_lines: u64,
}

/// Seamless UTC-hourly TSV-Raw Zstd writer with atomic .partial -> verify -> rename + manifest.
pub struct HourlyTsvZstWriter {
    data_root: PathBuf,
    exchange: ExchangeId,
    boot_id: String,
    zstd_level: i32,
    next_rx_seq: u64,
    current: Option<OpenHour>,
    sealed: Vec<SealedPartition>,
}

impl HourlyTsvZstWriter {
    pub fn new(data_root: PathBuf, exchange: ExchangeId, boot_id: String, zstd_level: i32) -> Self {
        Self {
            data_root,
            exchange,
            boot_id,
            zstd_level,
            next_rx_seq: 0,
            current: None,
            sealed: Vec::new(),
        }
    }

    pub fn sealed_partitions(&self) -> &[SealedPartition] {
        &self.sealed
    }

    pub fn has_open_partition(&self) -> bool {
        self.current.is_some()
    }

    pub fn write_record(&mut self, rec: &CanonicalRecord) -> Result<Option<SealedPartition>, String> {
        let key = HourPartitionKey::from_wall_ns(rec.recv_wall_ns);
        let mut just_sealed = None;

        let need_rotate = match &self.current {
            Some(open) => open.key != key,
            None => false,
        };
        if need_rotate {
            just_sealed = self.seal_current()?;
        }
        if self.current.is_none() {
            self.open_hour(key)?;
        }

        self.next_rx_seq += 1;
        let mut disk_rec = rec.clone();
        disk_rec.rx_seq = self.next_rx_seq;

        let open = self.current.as_mut().expect("current hour must be open");
        let line = encode_tsv_line(&disk_rec);
        open.encoder
            .write_all(line.as_bytes())
            .map_err(|e| format!("zstd write failed: {}", e))?;

        if open.start_wall_ns.is_none() {
            open.start_wall_ns = Some(disk_rec.recv_wall_ns);
        }
        open.end_wall_ns = Some(disk_rec.recv_wall_ns);
        if open.first_rx_seq.is_none() {
            open.first_rx_seq = Some(disk_rec.rx_seq);
        }
        open.last_rx_seq = Some(disk_rec.rx_seq);

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

        Ok(just_sealed)
    }

    /// Flush, fsync, verify zstd integrity, hash SHA-256, atomically rename, and emit manifest.
    /// On any failure, leaves the `.partial` file untouched (never promotes corrupt/unverified data).
    pub fn seal_current(&mut self) -> Result<Option<SealedPartition>, String> {
        let Some(open) = self.current.take() else {
            return Ok(None);
        };

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
        let manifest = PartitionManifest {
            schema_version: 1,
            exchange: self.exchange.as_dir_name().to_string(),
            date_utc: open.key.date.clone(),
            hour_utc: open.key.hour.clone(),
            boot_id: self.boot_id.clone(),
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

        let sealed = SealedPartition {
            data_path: open.final_path,
            manifest_path: open.manifest_path,
            manifest,
        };
        self.sealed.push(sealed.clone());
        Ok(Some(sealed))
    }

    fn open_hour(&mut self, key: HourPartitionKey) -> Result<(), String> {
        let dir = self
            .data_root
            .join("raw")
            .join(self.exchange.as_dir_name())
            .join(&key.date);
        fs::create_dir_all(&dir)
            .map_err(|e| format!("create_dir_all {} failed: {}", dir.display(), e))?;

        let stem = format!(
            "{}_{}_{}",
            self.exchange.as_dir_name(),
            key.yyyymmdd,
            key.hour
        );
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

        let file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&partial_path)
            .map_err(|e| format!("create {} failed: {}", partial_path.display(), e))?;
        let encoder = zstd::stream::write::Encoder::new(BufWriter::new(file), self.zstd_level)
            .map_err(|e| format!("zstd encoder init failed: {}", e))?;

        self.current = Some(OpenHour {
            key,
            partial_path,
            final_path,
            manifest_path,
            encoder,
            start_wall_ns: None,
            end_wall_ns: None,
            first_rx_seq: None,
            last_rx_seq: None,
            data_lines: 0,
            data_escaped_lines: 0,
            meta_lines: 0,
            local_loss_lines: 0,
            gap_start_lines: 0,
            switch_lines: 0,
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
    let file = File::open(path)
        .map_err(|e| format!("open {} failed: {}", path.display(), e))?;
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
