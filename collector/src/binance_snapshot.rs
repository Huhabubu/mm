use crate::types::ConnId;
use chrono::DateTime;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::Duration;

/// Official Binance USDS-M Futures `/fapi/v1/depth` IP request weight by `limit`.
/// Reference: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/general-info#limits
pub fn binance_depth_request_weight(limit: u32) -> u32 {
    match limit {
        0..=50 => 2,
        51..=100 => 5,
        101..=500 => 10,
        _ => 20,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SnapshotKey {
    pub conn_id: ConnId,
    pub epoch: u64,
    pub symbol: String,
}

impl SnapshotKey {
    pub fn new(conn_id: ConnId, epoch: u64, symbol: impl Into<String>) -> Self {
        Self {
            conn_id,
            epoch,
            symbol: symbol.into().to_uppercase(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotSchedulerConfig {
    /// Official Binance USDS-M Futures IP REQUEST_WEIGHT ceiling per minute (2400).
    pub ip_weight_limit_per_min: u32,
    /// Headroom reserved for other requests sharing the same egress IP (e.g. meta_daemon exchangeInfo).
    pub reserved_headroom_weight: u32,
    /// Request weight per `/fapi/v1/depth` call (20 for limit=1000).
    pub request_weight: u32,
    /// Rolling window for IP weight accounting (60s).
    pub weight_window: Duration,
    /// Maximum concurrent in-flight snapshot requests in normal mode.
    pub max_in_flight_normal: usize,
    /// Minimum spacing between dispatches in normal mode to avoid burst spikes.
    pub min_dispatch_spacing: Duration,
    /// Initial delay after WS connect so depth updates start buffering before fetching REST snapshot.
    pub initial_connect_delay: Duration,
    /// Per-attempt HTTP timeout.
    pub attempt_timeout: Duration,
    /// Base exponential backoff for transient errors (5xx, timeout, transport, invalid JSON, bridge failure).
    pub base_backoff: Duration,
    /// Cap for ordinary exponential backoff (exchange-provided Retry-After / ban deadlines override this cap).
    pub max_backoff: Duration,
    /// Conservative shared cooldown when HTTP 429 omits Retry-After / ban timestamp.
    pub conservative_429_cooldown: Duration,
    /// Conservative shared cooldown when HTTP 418 omits Retry-After / ban timestamp.
    pub conservative_418_cooldown: Duration,
    /// Extra safety margin added to parsed exchange ban deadlines.
    pub ban_safety_margin: Duration,
    /// Spacing between requests while ramping up after cooldown + single probe.
    pub ramp_dispatch_spacing: Duration,
    /// Consecutive successful requests after cooldown before restoring normal concurrency & spacing.
    pub ramp_successes_required: u32,
}

impl SnapshotSchedulerConfig {
    pub fn for_limit(depth_snapshot_limit: u32) -> Self {
        Self {
            ip_weight_limit_per_min: 2400,
            reserved_headroom_weight: 600,
            request_weight: binance_depth_request_weight(depth_snapshot_limit),
            weight_window: Duration::from_secs(60),
            max_in_flight_normal: 2,
            min_dispatch_spacing: Duration::from_millis(120),
            initial_connect_delay: Duration::from_millis(350),
            attempt_timeout: Duration::from_secs(10),
            base_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
            conservative_429_cooldown: Duration::from_secs(60),
            conservative_418_cooldown: Duration::from_secs(180),
            ban_safety_margin: Duration::from_secs(1),
            ramp_dispatch_spacing: Duration::from_secs(2),
            ramp_successes_required: 3,
        }
    }

    pub fn max_usable_weight(&self) -> u32 {
        self.ip_weight_limit_per_min
            .saturating_sub(self.reserved_headroom_weight)
            .max(self.request_weight)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryGate {
    Normal,
    CoolingDown {
        until_mono: Duration,
        reason: String,
        consecutive_rate_limit_hits: u32,
    },
    SingleProbe {
        probe_request_id: Option<u64>,
        consecutive_rate_limit_hits: u32,
    },
    GradualRamp {
        successes: u32,
        next_allowed_mono: Duration,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledSnapshotRequest {
    pub request_id: u64,
    pub conn_id: ConnId,
    pub epoch: u64,
    pub symbol: String,
    pub attempt: u32,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotAttemptOutcome {
    Success {
        last_update_id: u64,
        raw_json: String,
        used_weight_1m: Option<u32>,
    },
    RateLimited429 {
        retry_after: Option<Duration>,
        banned_until_ms: Option<i64>,
        used_weight_1m: Option<u32>,
        body_excerpt: String,
    },
    IpBanned418 {
        retry_after: Option<Duration>,
        banned_until_ms: Option<i64>,
        used_weight_1m: Option<u32>,
        body_excerpt: String,
    },
    HttpError {
        status: u16,
        retry_after: Option<Duration>,
        used_weight_1m: Option<u32>,
        body_excerpt: String,
    },
    Timeout,
    TransportError {
        message: String,
    },
    InvalidResponse {
        message: String,
        used_weight_1m: Option<u32>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResponseDisposition {
    IgnoredStale {
        reason: String,
    },
    ApplySnapshot {
        last_update_id: u64,
        raw_json: String,
    },
    ScheduledRetry {
        status_code: Option<u16>,
        reason: String,
        retry_in: Duration,
        retry_at_mono: Duration,
        shared_cooldown_active: bool,
    },
}

#[derive(Debug, Clone)]
enum EntryState {
    Queued {
        earliest_mono: Duration,
        attempts: u32,
        reason: String,
    },
    InFlight {
        request_id: u64,
        attempts: u32,
        retrigger_reason: Option<String>,
    },
}

#[derive(Debug, Clone)]
struct ServerWeightSample {
    used_weight: u32,
    observed_mono: Duration,
    dispatched_weight_since: u32,
}

/// Shared, deterministic Binance depth snapshot scheduler across all symbols and A/B connections.
///
/// Guarantees:
/// - Shared IP weight budget with explicit headroom for other egress callers (`X-MBX-USED-WEIGHT-1M` + local ledger).
/// - At most one in-flight snapshot request per `(conn_id, epoch, symbol)`; duplicate triggers coalesce.
/// - Stale connection/epoch responses and superseded request responses are strictly rejected.
/// - Exchange-provided `Retry-After` / `IP banned until` deadlines act as minimum pause durations and
///   override ordinary exponential backoff caps; missing headers trigger conservative shared cooldown.
/// - After cooldown expires, enforces single-probe recovery followed by gradual ramp-up before full concurrency.
pub struct BinanceSnapshotScheduler {
    cfg: SnapshotSchedulerConfig,
    next_request_id: u64,
    active_epochs: HashMap<ConnId, u64>,
    entries: BTreeMap<SnapshotKey, EntryState>,
    weight_ledger: VecDeque<(Duration, u32)>,
    server_weight: Option<ServerWeightSample>,
    last_dispatch_mono: Option<Duration>,
    gate: RecoveryGate,
}

impl BinanceSnapshotScheduler {
    pub fn new(cfg: SnapshotSchedulerConfig) -> Self {
        Self {
            cfg,
            next_request_id: 0,
            active_epochs: HashMap::new(),
            entries: BTreeMap::new(),
            weight_ledger: VecDeque::new(),
            server_weight: None,
            last_dispatch_mono: None,
            gate: RecoveryGate::Normal,
        }
    }

    pub fn config(&self) -> &SnapshotSchedulerConfig {
        &self.cfg
    }

    pub fn recovery_gate(&self) -> &RecoveryGate {
        &self.gate
    }

    pub fn active_epoch(&self, conn_id: ConnId) -> Option<u64> {
        self.active_epochs.get(&conn_id).copied()
    }

    pub fn in_flight_count(&self) -> usize {
        self.entries
            .values()
            .filter(|e| matches!(e, EntryState::InFlight { .. }))
            .count()
    }

    pub fn pending_count(&self) -> usize {
        self.entries.len()
    }

    pub fn effective_used_weight(&mut self, now_mono: Duration) -> u32 {
        self.prune_weight_ledger(now_mono);
        let local_used: u32 = self.weight_ledger.iter().map(|(_, w)| *w).sum();
        let server_used = match &self.server_weight {
            Some(s) if now_mono.saturating_sub(s.observed_mono) < self.cfg.weight_window => {
                s.used_weight.saturating_add(s.dispatched_weight_since)
            }
            _ => 0,
        };
        local_used.max(server_used)
    }

    fn prune_weight_ledger(&mut self, now_mono: Duration) {
        while let Some((ts, _)) = self.weight_ledger.front() {
            if now_mono.saturating_sub(*ts) >= self.cfg.weight_window {
                self.weight_ledger.pop_front();
            } else {
                break;
            }
        }
        if let Some(s) = &self.server_weight {
            if now_mono.saturating_sub(s.observed_mono) >= self.cfg.weight_window {
                self.server_weight = None;
            }
        }
    }

    fn record_server_weight(&mut self, used: Option<u32>, now_mono: Duration) {
        if let Some(w) = used {
            self.server_weight = Some(ServerWeightSample {
                used_weight: w,
                observed_mono: now_mono,
                dispatched_weight_since: 0,
            });
        }
    }

    /// Register a newly connected `(conn_id, epoch)`, cancelling any older epoch's queued/in-flight
    /// requests and scheduling initial snapshots for all symbols. Returns cancelled request IDs.
    pub fn register_connection_epoch(
        &mut self,
        conn_id: ConnId,
        epoch: u64,
        symbols: &[String],
        now_mono: Duration,
    ) -> Vec<u64> {
        let cancelled = self.cancel_connection_internal(conn_id, Some(epoch));
        self.active_epochs.insert(conn_id, epoch);
        let earliest = now_mono.saturating_add(self.cfg.initial_connect_delay);
        for sym in symbols {
            let key = SnapshotKey::new(conn_id, epoch, sym);
            self.entries.entry(key).or_insert(EntryState::Queued {
                earliest_mono: earliest,
                attempts: 0,
                reason: "initial_connect".to_string(),
            });
        }
        cancelled
    }

    /// Cancel all queued and in-flight requests for `conn_id` (e.g. on disconnect).
    /// Returns cancelled in-flight `request_id`s so caller can abort their tasks.
    pub fn cancel_connection(&mut self, conn_id: ConnId, expected_epoch: Option<u64>) -> Vec<u64> {
        if let Some(exp) = expected_epoch {
            if self.active_epochs.get(&conn_id).copied() == Some(exp) {
                self.active_epochs.remove(&conn_id);
            }
        } else {
            self.active_epochs.remove(&conn_id);
        }
        self.cancel_connection_internal(conn_id, None)
    }

    fn cancel_connection_internal(
        &mut self,
        conn_id: ConnId,
        keep_epoch: Option<u64>,
    ) -> Vec<u64> {
        let mut cancelled = Vec::new();
        let keys_to_remove: Vec<SnapshotKey> = self
            .entries
            .keys()
            .filter(|k| k.conn_id == conn_id && Some(k.epoch) != keep_epoch)
            .cloned()
            .collect();
        for key in keys_to_remove {
            if let Some(EntryState::InFlight { request_id, .. }) = self.entries.remove(&key) {
                cancelled.push(request_id);
                if let RecoveryGate::SingleProbe {
                    probe_request_id: Some(pid),
                    ..
                } = &mut self.gate
                {
                    if *pid == request_id {
                        *pid = 0; // replaced below
                    }
                }
            }
        }
        if let RecoveryGate::SingleProbe {
            probe_request_id, ..
        } = &mut self.gate
        {
            if probe_request_id.is_some_and(|pid| pid == 0 || cancelled.contains(&pid)) {
                *probe_request_id = None;
            }
        }
        cancelled
    }

    /// Trigger (or coalesce) a snapshot request for `(conn_id, epoch, symbol)`.
    /// If `supersede_in_flight` is true and a request is currently in-flight for the same key,
    /// the in-flight request ID is invalidated and returned so caller can abort it and any late
    /// response from it will be rejected.
    pub fn trigger(
        &mut self,
        conn_id: ConnId,
        epoch: u64,
        symbol: &str,
        now_mono: Duration,
        reason: impl Into<String>,
        supersede_in_flight: bool,
    ) -> Option<u64> {
        if self.active_epochs.get(&conn_id).copied() != Some(epoch) {
            return None;
        }
        let reason = reason.into();
        let key = SnapshotKey::new(conn_id, epoch, symbol);
        let earliest = match &self.gate {
            RecoveryGate::CoolingDown { until_mono, .. } => now_mono.max(*until_mono),
            _ => now_mono,
        };
        match self.entries.get_mut(&key) {
            Some(EntryState::Queued {
                reason: existing_reason,
                ..
            }) => {
                // Coalesce duplicate trigger into existing queued entry without bypassing backoff/cooldown.
                *existing_reason = reason;
                None
            }
            Some(EntryState::InFlight {
                request_id,
                attempts,
                retrigger_reason,
                ..
            }) => {
                if supersede_in_flight {
                    let old_id = *request_id;
                    let prev_attempts = *attempts;
                    if let RecoveryGate::SingleProbe {
                        probe_request_id, ..
                    } = &mut self.gate
                    {
                        if *probe_request_id == Some(old_id) {
                            *probe_request_id = None;
                        }
                    }
                    self.entries.insert(
                        key,
                        EntryState::Queued {
                            earliest_mono: earliest,
                            attempts: prev_attempts,
                            reason,
                        },
                    );
                    Some(old_id)
                } else {
                    // Coalesce while in-flight: at most 1 request in-flight per (conn, epoch, symbol).
                    *retrigger_reason = Some(reason);
                    None
                }
            }
            None => {
                self.entries.insert(
                    key,
                    EntryState::Queued {
                        earliest_mono: earliest,
                        attempts: 0,
                        reason,
                    },
                );
                None
            }
        }
    }

    /// Dispatch any eligible queued snapshot requests respecting shared budget, cooldown,
    /// single-probe gate, gradual ramp, and per-key in-flight uniqueness.
    pub fn poll_dispatch(
        &mut self,
        now_mono: Duration,
        preferred_primary: Option<ConnId>,
    ) -> Vec<ScheduledSnapshotRequest> {
        self.prune_weight_ledger(now_mono);

        // Advance CoolingDown -> SingleProbe when cooldown deadline arrives.
        if let RecoveryGate::CoolingDown {
            until_mono,
            consecutive_rate_limit_hits,
            ..
        } = &self.gate
        {
            if now_mono < *until_mono {
                return Vec::new();
            }
            let hits = *consecutive_rate_limit_hits;
            self.gate = RecoveryGate::SingleProbe {
                probe_request_id: None,
                consecutive_rate_limit_hits: hits,
            };
        }

        let in_flight = self.in_flight_count();
        let max_new = match &self.gate {
            RecoveryGate::CoolingDown { .. } => 0,
            RecoveryGate::SingleProbe {
                probe_request_id, ..
            } => {
                if probe_request_id.is_some() || in_flight > 0 {
                    0
                } else {
                    1
                }
            }
            RecoveryGate::GradualRamp {
                next_allowed_mono, ..
            } => {
                if now_mono < *next_allowed_mono || in_flight >= 1 {
                    0
                } else {
                    1
                }
            }
            RecoveryGate::Normal => {
                let spacing_ok = self.last_dispatch_mono.is_none_or(|last| {
                    now_mono.saturating_sub(last) >= self.cfg.min_dispatch_spacing
                });
                if !spacing_ok {
                    0
                } else {
                    // Dispatch at most 1 per spacing interval when spacing > 0 so requests spread smoothly.
                    let slots = self.cfg.max_in_flight_normal.saturating_sub(in_flight);
                    if self.cfg.min_dispatch_spacing.is_zero() {
                        slots
                    } else {
                        slots.min(1)
                    }
                }
            }
        };

        if max_new == 0 {
            return Vec::new();
        }

        let mut dispatched = Vec::new();
        let max_usable = self.cfg.max_usable_weight();

        while dispatched.len() < max_new {
            let used = self.effective_used_weight(now_mono);
            if used.saturating_add(self.cfg.request_weight) > max_usable {
                break;
            }

            // Pick highest-priority ready queued entry:
            // 1) Preferred primary connection first, then standby
            // 2) Earliest scheduled time (fair round-robin across symbols)
            // 3) Lexicographic key
            let chosen_key = self
                .entries
                .iter()
                .filter_map(|(k, v)| match v {
                    EntryState::Queued { earliest_mono, .. } if *earliest_mono <= now_mono => {
                        let prio = if preferred_primary == Some(k.conn_id) {
                            0u8
                        } else {
                            1u8
                        };
                        Some((prio, *earliest_mono, k.clone()))
                    }
                    _ => None,
                })
                .min()
                .map(|(_, _, k)| k);

            let Some(key) = chosen_key else {
                break;
            };

            let (attempts, reason) = match self.entries.get(&key) {
                Some(EntryState::Queued {
                    attempts, reason, ..
                }) => (*attempts, reason.clone()),
                _ => break,
            };

            self.next_request_id += 1;
            let request_id = self.next_request_id;
            self.entries.insert(
                key.clone(),
                EntryState::InFlight {
                    request_id,
                    attempts,
                    retrigger_reason: None,
                },
            );
            self.weight_ledger
                .push_back((now_mono, self.cfg.request_weight));
            if let Some(sw) = &mut self.server_weight {
                sw.dispatched_weight_since = sw
                    .dispatched_weight_since
                    .saturating_add(self.cfg.request_weight);
            }
            self.last_dispatch_mono = Some(now_mono);

            match &mut self.gate {
                RecoveryGate::SingleProbe {
                    probe_request_id, ..
                } => {
                    *probe_request_id = Some(request_id);
                }
                RecoveryGate::GradualRamp {
                    next_allowed_mono, ..
                } => {
                    *next_allowed_mono = now_mono.saturating_add(self.cfg.ramp_dispatch_spacing);
                }
                _ => {}
            }

            dispatched.push(ScheduledSnapshotRequest {
                request_id,
                conn_id: key.conn_id,
                epoch: key.epoch,
                symbol: key.symbol,
                attempt: attempts + 1,
                reason,
            });
        }

        dispatched
    }

    /// Compute the earliest monotonic timestamp when the scheduler may be able to dispatch work.
    pub fn next_wakeup_mono(&mut self, now_mono: Duration) -> Option<Duration> {
        self.prune_weight_ledger(now_mono);
        let earliest_queued = self
            .entries
            .values()
            .filter_map(|e| match e {
                EntryState::Queued { earliest_mono, .. } => Some(*earliest_mono),
                EntryState::InFlight { .. } => None,
            })
            .min()?;

        let mut ready_at = earliest_queued.max(now_mono);

        match &self.gate {
            RecoveryGate::CoolingDown { until_mono, .. } => {
                ready_at = ready_at.max(*until_mono);
            }
            RecoveryGate::SingleProbe {
                probe_request_id: Some(_),
                ..
            } => {
                // Waiting for the single probe to complete; no timer wakeup needed beyond attempt timeout.
                return None;
            }
            RecoveryGate::SingleProbe {
                probe_request_id: None,
                ..
            } => {
                if self.in_flight_count() > 0 {
                    return None;
                }
            }
            RecoveryGate::GradualRamp {
                next_allowed_mono, ..
            } => {
                if self.in_flight_count() >= 1 {
                    return None;
                }
                ready_at = ready_at.max(*next_allowed_mono);
            }
            RecoveryGate::Normal => {
                if self.in_flight_count() >= self.cfg.max_in_flight_normal {
                    return None;
                }
                if let Some(last) = self.last_dispatch_mono {
                    ready_at = ready_at.max(last.saturating_add(self.cfg.min_dispatch_spacing));
                }
            }
        }

        // If shared weight budget is currently exhausted, wake when whichever bottleneck expires.
        let local_used: u32 = self.weight_ledger.iter().map(|(_, w)| *w).sum();
        let server_used = match &self.server_weight {
            Some(s) if now_mono.saturating_sub(s.observed_mono) < self.cfg.weight_window => {
                s.used_weight.saturating_add(s.dispatched_weight_since)
            }
            _ => 0,
        };
        let max_usable = self.cfg.max_usable_weight();
        let local_blocks = local_used.saturating_add(self.cfg.request_weight) > max_usable;
        let server_blocks = server_used.saturating_add(self.cfg.request_weight) > max_usable;
        if local_blocks || server_blocks {
            let local_unlock = if local_blocks {
                self.weight_ledger
                    .front()
                    .map(|(ts, _)| ts.saturating_add(self.cfg.weight_window))
            } else {
                None
            };
            let server_unlock = if server_blocks {
                self.server_weight
                    .as_ref()
                    .map(|s| s.observed_mono.saturating_add(self.cfg.weight_window))
            } else {
                None
            };
            let budget_unlock = match (local_unlock, server_unlock) {
                (Some(a), Some(b)) => a.max(b),
                (Some(a), None) => a,
                (None, Some(b)) => b,
                (None, None) => now_mono.saturating_add(Duration::from_secs(1)),
            };
            ready_at = ready_at.max(budget_unlock);
        }

        Some(ready_at)
    }

    fn compute_exchange_wait(
        &self,
        retry_after: Option<Duration>,
        banned_until_ms: Option<i64>,
        now_wall_ms: i64,
    ) -> Option<Duration> {
        let ban_wait = banned_until_ms.map(|until_ms| {
            let rem_ms = until_ms.saturating_sub(now_wall_ms).max(0) as u64;
            Duration::from_millis(rem_ms).saturating_add(self.cfg.ban_safety_margin)
        });
        match (retry_after, ban_wait) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        }
    }

    fn enter_shared_cooldown(
        &mut self,
        now_mono: Duration,
        wait: Duration,
        reason: String,
    ) -> Duration {
        let prev_hits = match &self.gate {
            RecoveryGate::CoolingDown {
                consecutive_rate_limit_hits,
                ..
            }
            | RecoveryGate::SingleProbe {
                consecutive_rate_limit_hits,
                ..
            } => *consecutive_rate_limit_hits,
            _ => 0,
        };
        let existing_until = match &self.gate {
            RecoveryGate::CoolingDown { until_mono, .. } => *until_mono,
            _ => now_mono,
        };
        let until = existing_until.max(now_mono.saturating_add(wait));
        self.gate = RecoveryGate::CoolingDown {
            until_mono: until,
            reason,
            consecutive_rate_limit_hits: prev_hits.saturating_add(1),
        };
        // Push back all already-queued tasks so none fire before shared cooldown ends.
        for state in self.entries.values_mut() {
            if let EntryState::Queued { earliest_mono, .. } = state {
                if *earliest_mono < until {
                    *earliest_mono = until;
                }
            }
        }
        until
    }

    fn exponential_backoff(&self, attempts: u32) -> Duration {
        let shift = attempts.min(10);
        let mult = 1u32.checked_shl(shift).unwrap_or(u32::MAX);
        self.cfg
            .base_backoff
            .saturating_mul(mult)
            .min(self.cfg.max_backoff)
    }

    fn consecutive_rate_limit_hits(&self) -> u32 {
        match &self.gate {
            RecoveryGate::CoolingDown {
                consecutive_rate_limit_hits,
                ..
            }
            | RecoveryGate::SingleProbe {
                consecutive_rate_limit_hits,
                ..
            } => *consecutive_rate_limit_hits,
            _ => 0,
        }
    }

    /// Process the HTTP/transport outcome of a previously dispatched request.
    pub fn on_response(
        &mut self,
        request_id: u64,
        conn_id: ConnId,
        epoch: u64,
        symbol: &str,
        outcome: SnapshotAttemptOutcome,
        now_mono: Duration,
        now_wall_ms: i64,
    ) -> ResponseDisposition {
        // Always absorb IP-wide weight & ban/rate-limit signals even if this response is for an old epoch!
        match &outcome {
            SnapshotAttemptOutcome::Success { used_weight_1m, .. }
            | SnapshotAttemptOutcome::InvalidResponse { used_weight_1m, .. } => {
                self.record_server_weight(*used_weight_1m, now_mono);
            }
            SnapshotAttemptOutcome::RateLimited429 {
                retry_after,
                banned_until_ms,
                used_weight_1m,
                body_excerpt,
            } => {
                self.record_server_weight(
                    used_weight_1m.or(Some(self.cfg.max_usable_weight())),
                    now_mono,
                );
                let hits = self.consecutive_rate_limit_hits();
                let wait = self
                    .compute_exchange_wait(*retry_after, *banned_until_ms, now_wall_ms)
                    .unwrap_or_else(|| {
                        let scale = 1u32 << hits.min(3);
                        self.cfg
                            .conservative_429_cooldown
                            .saturating_mul(scale)
                            .min(Duration::from_secs(600))
                    });
                self.enter_shared_cooldown(
                    now_mono,
                    wait,
                    format!("http_429: {}", body_excerpt),
                );
            }
            SnapshotAttemptOutcome::IpBanned418 {
                retry_after,
                banned_until_ms,
                used_weight_1m,
                body_excerpt,
            } => {
                self.record_server_weight(
                    used_weight_1m.or(Some(self.cfg.max_usable_weight())),
                    now_mono,
                );
                let hits = self.consecutive_rate_limit_hits();
                let wait = self
                    .compute_exchange_wait(*retry_after, *banned_until_ms, now_wall_ms)
                    .unwrap_or_else(|| {
                        let scale = 1u32 << hits.min(4);
                        self.cfg
                            .conservative_418_cooldown
                            .saturating_mul(scale)
                            .min(Duration::from_secs(3600))
                    });
                self.enter_shared_cooldown(
                    now_mono,
                    wait,
                    format!("http_418_ip_ban: {}", body_excerpt),
                );
            }
            SnapshotAttemptOutcome::HttpError {
                status,
                retry_after,
                used_weight_1m,
                body_excerpt,
            } => {
                self.record_server_weight(*used_weight_1m, now_mono);
                if *status == 403 {
                    let wait = retry_after.unwrap_or(self.cfg.conservative_429_cooldown);
                    self.enter_shared_cooldown(
                        now_mono,
                        wait,
                        format!("http_403_waf: {}", body_excerpt),
                    );
                }
            }
            SnapshotAttemptOutcome::Timeout | SnapshotAttemptOutcome::TransportError { .. } => {}
        }

        // Reject stale connection/epoch or superseded request_id.
        if self.active_epochs.get(&conn_id).copied() != Some(epoch) {
            return ResponseDisposition::IgnoredStale {
                reason: format!(
                    "stale epoch {} for conn={} (active={:?})",
                    epoch,
                    conn_id.as_str(),
                    self.active_epochs.get(&conn_id)
                ),
            };
        }

        let key = SnapshotKey::new(conn_id, epoch, symbol);
        let (attempts, retrigger_reason) = match self.entries.get(&key) {
            Some(EntryState::InFlight {
                request_id: active_id,
                attempts,
                retrigger_reason,
                ..
            }) if *active_id == request_id => (*attempts, retrigger_reason.clone()),
            Some(EntryState::InFlight {
                request_id: active_id,
                ..
            }) => {
                return ResponseDisposition::IgnoredStale {
                    reason: format!(
                        "superseded request_id={} (active={})",
                        request_id, active_id
                    ),
                };
            }
            Some(EntryState::Queued { .. }) => {
                return ResponseDisposition::IgnoredStale {
                    reason: format!(
                        "superseded request_id={} (newer attempt already queued)",
                        request_id
                    ),
                };
            }
            None => {
                return ResponseDisposition::IgnoredStale {
                    reason: format!(
                        "request_id={} no longer tracked (already completed or cancelled)",
                        request_id
                    ),
                };
            }
        };

        // If this request was the active SingleProbe, release probe slot on completion.
        if let RecoveryGate::SingleProbe {
            probe_request_id, ..
        } = &mut self.gate
        {
            if *probe_request_id == Some(request_id) {
                *probe_request_id = None;
            }
        }

        match outcome {
            SnapshotAttemptOutcome::Success {
                last_update_id,
                raw_json,
                ..
            } => {
                self.entries.remove(&key);
                // Advance post-cooldown recovery gate on verified 200 OK snapshot.
                match &self.gate {
                    RecoveryGate::SingleProbe { .. } => {
                        if self.cfg.ramp_successes_required <= 1 {
                            self.gate = RecoveryGate::Normal;
                        } else {
                            self.gate = RecoveryGate::GradualRamp {
                                successes: 1,
                                next_allowed_mono: now_mono
                                    .saturating_add(self.cfg.ramp_dispatch_spacing),
                            };
                        }
                    }
                    RecoveryGate::GradualRamp { successes, .. } => {
                        let next_succ = successes + 1;
                        if next_succ >= self.cfg.ramp_successes_required {
                            self.gate = RecoveryGate::Normal;
                        } else {
                            self.gate = RecoveryGate::GradualRamp {
                                successes: next_succ,
                                next_allowed_mono: now_mono
                                    .saturating_add(self.cfg.ramp_dispatch_spacing),
                            };
                        }
                    }
                    RecoveryGate::CoolingDown { .. } | RecoveryGate::Normal => {}
                }
                // Note: if retrigger_reason was coalesced while in-flight, the caller will still
                // apply this fresh snapshot first; if bridging fails, `on_bridge_result(Err)` re-queues it.
                let _ = retrigger_reason;
                ResponseDisposition::ApplySnapshot {
                    last_update_id,
                    raw_json,
                }
            }
            SnapshotAttemptOutcome::RateLimited429 { body_excerpt, .. } => {
                let until = match &self.gate {
                    RecoveryGate::CoolingDown { until_mono, .. } => *until_mono,
                    _ => now_mono.saturating_add(self.cfg.conservative_429_cooldown),
                };
                self.entries.insert(
                    key,
                    EntryState::Queued {
                        earliest_mono: until,
                        attempts: attempts.saturating_add(1),
                        reason: "retry_after_429".to_string(),
                    },
                );
                ResponseDisposition::ScheduledRetry {
                    status_code: Some(429),
                    reason: format!("HTTP 429 rate limited: {}", body_excerpt),
                    retry_in: until.saturating_sub(now_mono),
                    retry_at_mono: until,
                    shared_cooldown_active: true,
                }
            }
            SnapshotAttemptOutcome::IpBanned418 { body_excerpt, .. } => {
                let until = match &self.gate {
                    RecoveryGate::CoolingDown { until_mono, .. } => *until_mono,
                    _ => now_mono.saturating_add(self.cfg.conservative_418_cooldown),
                };
                self.entries.insert(
                    key,
                    EntryState::Queued {
                        earliest_mono: until,
                        attempts: attempts.saturating_add(1),
                        reason: "retry_after_418_ban".to_string(),
                    },
                );
                ResponseDisposition::ScheduledRetry {
                    status_code: Some(418),
                    reason: format!("HTTP 418 IP banned: {}", body_excerpt),
                    retry_in: until.saturating_sub(now_mono),
                    retry_at_mono: until,
                    shared_cooldown_active: true,
                }
            }
            SnapshotAttemptOutcome::HttpError {
                status,
                retry_after,
                body_excerpt,
                ..
            } => {
                let backoff = self.exponential_backoff(attempts);
                let wait = retry_after.map_or(backoff, |ra| ra.max(backoff));
                let cooldown_until = match &self.gate {
                    RecoveryGate::CoolingDown { until_mono, .. } => *until_mono,
                    _ => now_mono,
                };
                let retry_at = now_mono.saturating_add(wait).max(cooldown_until);
                self.entries.insert(
                    key,
                    EntryState::Queued {
                        earliest_mono: retry_at,
                        attempts: attempts.saturating_add(1),
                        reason: format!("retry_http_{}", status),
                    },
                );
                ResponseDisposition::ScheduledRetry {
                    status_code: Some(status),
                    reason: format!("HTTP {}: {}", status, body_excerpt),
                    retry_in: retry_at.saturating_sub(now_mono),
                    retry_at_mono: retry_at,
                    shared_cooldown_active: matches!(self.gate, RecoveryGate::CoolingDown { .. }),
                }
            }
            SnapshotAttemptOutcome::Timeout => {
                let wait = self.exponential_backoff(attempts);
                let retry_at = self.respect_cooldown(now_mono.saturating_add(wait));
                self.entries.insert(
                    key,
                    EntryState::Queued {
                        earliest_mono: retry_at,
                        attempts: attempts.saturating_add(1),
                        reason: "retry_timeout".to_string(),
                    },
                );
                ResponseDisposition::ScheduledRetry {
                    status_code: None,
                    reason: format!("attempt timeout ({:?})", self.cfg.attempt_timeout),
                    retry_in: retry_at.saturating_sub(now_mono),
                    retry_at_mono: retry_at,
                    shared_cooldown_active: matches!(self.gate, RecoveryGate::CoolingDown { .. }),
                }
            }
            SnapshotAttemptOutcome::TransportError { message } => {
                let wait = self.exponential_backoff(attempts);
                let retry_at = self.respect_cooldown(now_mono.saturating_add(wait));
                self.entries.insert(
                    key,
                    EntryState::Queued {
                        earliest_mono: retry_at,
                        attempts: attempts.saturating_add(1),
                        reason: "retry_transport_error".to_string(),
                    },
                );
                ResponseDisposition::ScheduledRetry {
                    status_code: None,
                    reason: format!("transport error: {}", message),
                    retry_in: retry_at.saturating_sub(now_mono),
                    retry_at_mono: retry_at,
                    shared_cooldown_active: matches!(self.gate, RecoveryGate::CoolingDown { .. }),
                }
            }
            SnapshotAttemptOutcome::InvalidResponse { message, .. } => {
                let wait = self.exponential_backoff(attempts);
                let retry_at = self.respect_cooldown(now_mono.saturating_add(wait));
                self.entries.insert(
                    key,
                    EntryState::Queued {
                        earliest_mono: retry_at,
                        attempts: attempts.saturating_add(1),
                        reason: "retry_invalid_response".to_string(),
                    },
                );
                ResponseDisposition::ScheduledRetry {
                    status_code: Some(200),
                    reason: format!("invalid snapshot response: {}", message),
                    retry_in: retry_at.saturating_sub(now_mono),
                    retry_at_mono: retry_at,
                    shared_cooldown_active: matches!(self.gate, RecoveryGate::CoolingDown { .. }),
                }
            }
        }
    }

    /// Schedule a retry after a REST snapshot failed to bridge with buffered WS deltas.
    pub fn on_bridge_failure(
        &mut self,
        conn_id: ConnId,
        epoch: u64,
        symbol: &str,
        now_mono: Duration,
        reason: impl Into<String>,
    ) -> Option<Duration> {
        if self.active_epochs.get(&conn_id).copied() != Some(epoch) {
            return None;
        }
        let key = SnapshotKey::new(conn_id, epoch, symbol);
        let reason = reason.into();
        let retry_at = self.respect_cooldown(now_mono.saturating_add(self.cfg.base_backoff));
        let existing_attempts = match self.entries.get(&key) {
            Some(EntryState::Queued { attempts, .. }) => Some(*attempts),
            _ => None,
        };
        let queued_target = existing_attempts.map(|a| {
            let backoff = self.exponential_backoff(a);
            self.respect_cooldown(now_mono.saturating_add(backoff))
        });
        match self.entries.get_mut(&key) {
            Some(EntryState::Queued {
                earliest_mono,
                attempts,
                reason: r,
            }) => {
                *attempts = attempts.saturating_add(1);
                if let Some(target) = queued_target {
                    *earliest_mono = (*earliest_mono).max(target);
                }
                *r = reason;
                Some(*earliest_mono)
            }
            Some(EntryState::InFlight {
                retrigger_reason, ..
            }) => {
                *retrigger_reason = Some(reason);
                None
            }
            None => {
                self.entries.insert(
                    key,
                    EntryState::Queued {
                        earliest_mono: retry_at,
                        attempts: 1,
                        reason,
                    },
                );
                Some(retry_at)
            }
        }
    }

    fn respect_cooldown(&self, candidate: Duration) -> Duration {
        match &self.gate {
            RecoveryGate::CoolingDown { until_mono, .. } => candidate.max(*until_mono),
            _ => candidate,
        }
    }
}

/// Parse HTTP `Retry-After` header (integer seconds or RFC 2822 / HTTP-date).
pub fn parse_retry_after_header(value: &str, now_wall_ms: i64) -> Option<Duration> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(secs) = trimmed.parse::<u64>() {
        return Some(Duration::from_secs(secs.max(1)));
    }
    if let Ok(dt) = DateTime::parse_from_rfc2822(trimmed) {
        let delta_ms = dt.timestamp_millis().saturating_sub(now_wall_ms).max(1000) as u64;
        return Some(Duration::from_millis(delta_ms));
    }
    None
}

/// Parse Binance IP ban deadline timestamp (ms or s) from JSON or plain-text error bodies, e.g.
/// `{"code":-1003,"msg":"Way too much request weight used; IP banned until 1744874068494. ..."}`
pub fn parse_ban_until_ms(body: &str) -> Option<i64> {
    let lower = body.to_ascii_lowercase();
    let idx = lower.find("banned until")?;
    let tail = &body[idx + "banned until".len()..];
    let digits: String = tail
        .chars()
        .skip_while(|c| !c.is_ascii_digit())
        .take_while(|c| c.is_ascii_digit())
        .collect();
    if digits.len() < 10 {
        return None;
    }
    let raw = digits.parse::<i64>().ok()?;
    if digits.len() >= 13 {
        Some(raw)
    } else {
        Some(raw.saturating_mul(1000))
    }
}

/// Parse Binance `X-MBX-USED-WEIGHT-1M` header value.
pub fn parse_used_weight_header(headers: &http::HeaderMap) -> Option<u32> {
    headers
        .get("x-mbx-used-weight-1m")
        .or_else(|| headers.get("x-mbx-used-weight"))
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<u32>().ok())
}

/// Validate a Binance `/fapi/v1/depth` JSON response body and return `lastUpdateId`.
pub fn parse_depth_snapshot_body(text: &str) -> Result<u64, String> {
    let v: serde_json::Value =
        serde_json::from_str(text).map_err(|e| format!("invalid JSON: {}", e))?;
    if let Some(code) = v.get("code").and_then(|x| x.as_i64()) {
        let msg = v
            .get("msg")
            .and_then(|x| x.as_str())
            .unwrap_or("binance error");
        return Err(format!("binance API error code={}: {}", code, msg));
    }
    let last_update_id = v
        .get("lastUpdateId")
        .and_then(|x| x.as_u64())
        .ok_or_else(|| "missing integer lastUpdateId".to_string())?;
    if !v.get("bids").is_some_and(|x| x.is_array()) {
        return Err("missing bids array".to_string());
    }
    if !v.get("asks").is_some_and(|x| x.is_array()) {
        return Err("missing asks array".to_string());
    }
    Ok(last_update_id)
}

pub fn classify_http_response(
    status: u16,
    headers: &http::HeaderMap,
    body: String,
    now_wall_ms: i64,
) -> SnapshotAttemptOutcome {
    let used_weight_1m = parse_used_weight_header(headers);
    let retry_after = headers
        .get(http::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| parse_retry_after_header(s, now_wall_ms));
    let banned_until_ms = parse_ban_until_ms(&body);
    let excerpt: String = body.chars().take(200).collect();

    match status {
        200 => match parse_depth_snapshot_body(&body) {
            Ok(last_update_id) => SnapshotAttemptOutcome::Success {
                last_update_id,
                raw_json: body,
                used_weight_1m,
            },
            Err(message) => SnapshotAttemptOutcome::InvalidResponse {
                message,
                used_weight_1m,
            },
        },
        418 => SnapshotAttemptOutcome::IpBanned418 {
            retry_after,
            banned_until_ms,
            used_weight_1m,
            body_excerpt: excerpt,
        },
        429 => SnapshotAttemptOutcome::RateLimited429 {
            retry_after,
            banned_until_ms,
            used_weight_1m,
            body_excerpt: excerpt,
        },
        other => SnapshotAttemptOutcome::HttpError {
            status: other,
            retry_after,
            used_weight_1m,
            body_excerpt: excerpt,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn six_symbols() -> Vec<String> {
        vec![
            "BTCUSDT".into(),
            "ETHUSDT".into(),
            "XRPUSDT".into(),
            "SOLUSDT".into(),
            "DOGEUSDT".into(),
            "SUIUSDT".into(),
        ]
    }

    #[test]
    fn parses_retry_after_and_ban_until_deadline() {
        let now_wall_ms = 1_795_220_000_000i64;
        assert_eq!(
            parse_retry_after_header("125", now_wall_ms),
            Some(Duration::from_secs(125))
        );
        let body = r#"{"code":-1003,"msg":"Way too much request weight used; IP banned until 1795220300000. Please use the websocket for live updates to avoid bans."}"#;
        assert_eq!(parse_ban_until_ms(body), Some(1_795_220_300_000));
        // HTTP-date format
        let dt_str = "Sat, 21 Nov 2026 09:00:00 GMT";
        let dt_ms = DateTime::parse_from_rfc2822(dt_str)
            .unwrap()
            .timestamp_millis();
        assert_eq!(
            parse_retry_after_header(dt_str, dt_ms - 45_000),
            Some(Duration::from_secs(45))
        );
    }

    #[test]
    fn ban_deadline_overrides_exponential_backoff_cap_and_recovers_via_single_probe_then_ramp() {
        let mut cfg = SnapshotSchedulerConfig::for_limit(1000);
        cfg.max_backoff = Duration::from_secs(30);
        cfg.initial_connect_delay = Duration::ZERO;
        cfg.min_dispatch_spacing = Duration::ZERO;
        cfg.ramp_dispatch_spacing = Duration::from_secs(2);
        cfg.ramp_successes_required = 3;

        let mut sched = BinanceSnapshotScheduler::new(cfg);
        let syms = six_symbols();
        sched.register_connection_epoch(ConnId::A, 1, &syms, Duration::ZERO);
        sched.register_connection_epoch(ConnId::B, 1, &syms, Duration::ZERO);

        let batch = sched.poll_dispatch(Duration::ZERO, Some(ConnId::A));
        assert_eq!(batch.len(), 2);
        let req0 = &batch[0];
        let req1 = &batch[1];

        // Server returns HTTP 418 banning IP for 300s (> max_backoff=30s!)
        let now_wall_ms = 1_800_000_000_000i64;
        let ban_until_ms = now_wall_ms + 300_000;
        let disp = sched.on_response(
            req0.request_id,
            req0.conn_id,
            req0.epoch,
            &req0.symbol,
            SnapshotAttemptOutcome::IpBanned418 {
                retry_after: Some(Duration::from_secs(120)),
                banned_until_ms: Some(ban_until_ms),
                used_weight_1m: Some(2400),
                body_excerpt: format!("IP banned until {}", ban_until_ms),
            },
            Duration::from_secs(1),
            now_wall_ms,
        );
        match disp {
            ResponseDisposition::ScheduledRetry {
                status_code,
                retry_in,
                shared_cooldown_active,
                ..
            } => {
                assert_eq!(status_code, Some(418));
                assert!(shared_cooldown_active);
                // 300s ban + 1s safety margin > 120s Retry-After and >> 30s max_backoff
                assert_eq!(retry_in, Duration::from_secs(301));
            }
            other => panic!("expected retry, got {:?}", other),
        }

        // Complete req1 with a timeout during cooldown; NO requests may dispatch for ANY symbol or A/B until t=302s!
        let _ = sched.on_response(
            req1.request_id,
            req1.conn_id,
            req1.epoch,
            &req1.symbol,
            SnapshotAttemptOutcome::Timeout,
            Duration::from_secs(2),
            now_wall_ms + 1_000,
        );
        assert!(sched.poll_dispatch(Duration::from_secs(100), Some(ConnId::A)).is_empty());
        assert!(sched.poll_dispatch(Duration::from_secs(301), Some(ConnId::A)).is_empty());

        // At t=302s, cooldown expires -> ONLY A SINGLE PROBE is dispatched!
        let probe = sched.poll_dispatch(Duration::from_secs(302), Some(ConnId::A));
        assert_eq!(probe.len(), 1);
        assert!(matches!(
            sched.recovery_gate(),
            RecoveryGate::SingleProbe {
                probe_request_id: Some(_),
                ..
            }
        ));
        // While probe is in-flight, no second request may dispatch!
        assert!(sched.poll_dispatch(Duration::from_secs(303), Some(ConnId::A)).is_empty());

        // Single probe succeeds -> enters GradualRamp (spacing=2s, max 1 in-flight)!
        let ok_disp = sched.on_response(
            probe[0].request_id,
            probe[0].conn_id,
            probe[0].epoch,
            &probe[0].symbol,
            SnapshotAttemptOutcome::Success {
                last_update_id: 1000,
                raw_json: r#"{"lastUpdateId":1000,"bids":[],"asks":[]}"#.into(),
                used_weight_1m: Some(20),
            },
            Duration::from_secs(303),
            now_wall_ms + 302_000,
        );
        assert!(matches!(ok_disp, ResponseDisposition::ApplySnapshot { .. }));
        assert!(matches!(
            sched.recovery_gate(),
            RecoveryGate::GradualRamp { successes: 1, .. }
        ));
        // Before ramp spacing (2s), nothing dispatches
        assert!(sched.poll_dispatch(Duration::from_secs(304), Some(ConnId::A)).is_empty());

        // Second & third ramp requests succeed one-by-one, then gate returns to Normal (concurrency=2)!
        let ramp2 = sched.poll_dispatch(Duration::from_secs(305), Some(ConnId::A));
        assert_eq!(ramp2.len(), 1);
        let _ = sched.on_response(
            ramp2[0].request_id,
            ramp2[0].conn_id,
            ramp2[0].epoch,
            &ramp2[0].symbol,
            SnapshotAttemptOutcome::Success {
                last_update_id: 1001,
                raw_json: r#"{"lastUpdateId":1001,"bids":[],"asks":[]}"#.into(),
                used_weight_1m: Some(40),
            },
            Duration::from_secs(305),
            now_wall_ms + 304_000,
        );
        assert!(matches!(
            sched.recovery_gate(),
            RecoveryGate::GradualRamp { successes: 2, .. }
        ));

        let ramp3 = sched.poll_dispatch(Duration::from_secs(307), Some(ConnId::A));
        assert_eq!(ramp3.len(), 1);
        let _ = sched.on_response(
            ramp3[0].request_id,
            ramp3[0].conn_id,
            ramp3[0].epoch,
            &ramp3[0].symbol,
            SnapshotAttemptOutcome::Success {
                last_update_id: 1002,
                raw_json: r#"{"lastUpdateId":1002,"bids":[],"asks":[]}"#.into(),
                used_weight_1m: Some(60),
            },
            Duration::from_secs(307),
            now_wall_ms + 306_000,
        );
        assert_eq!(sched.recovery_gate(), &RecoveryGate::Normal);

        // Now Normal mode permits max_in_flight_normal=2!
        let normal_batch = sched.poll_dispatch(Duration::from_secs(307), Some(ConnId::A));
        assert_eq!(normal_batch.len(), 2);
    }

    #[test]
    fn rate_limit_without_retry_after_uses_conservative_shared_cooldown() {
        let mut cfg = SnapshotSchedulerConfig::for_limit(1000);
        cfg.initial_connect_delay = Duration::ZERO;
        cfg.min_dispatch_spacing = Duration::ZERO;
        cfg.conservative_429_cooldown = Duration::from_secs(60);

        let mut sched = BinanceSnapshotScheduler::new(cfg);
        sched.register_connection_epoch(ConnId::A, 1, &["BTCUSDT".into(), "ETHUSDT".into()], Duration::ZERO);
        let batch = sched.poll_dispatch(Duration::ZERO, Some(ConnId::A));
        assert_eq!(batch.len(), 2);

        // 429 missing Retry-After and missing ban timestamp -> conservative 60s shared cooldown!
        let d = sched.on_response(
            batch[0].request_id,
            ConnId::A,
            1,
            &batch[0].symbol,
            SnapshotAttemptOutcome::RateLimited429 {
                retry_after: None,
                banned_until_ms: None,
                used_weight_1m: None,
                body_excerpt: "Too many requests".into(),
            },
            Duration::from_secs(1),
            1_800_000_000_000,
        );
        assert!(matches!(
            d,
            ResponseDisposition::ScheduledRetry {
                status_code: Some(429),
                retry_in,
                shared_cooldown_active: true,
                ..
            } if retry_in == Duration::from_secs(60)
        ));
        // Finish second in-flight request with invalid JSON
        let _ = sched.on_response(
            batch[1].request_id,
            ConnId::A,
            1,
            &batch[1].symbol,
            SnapshotAttemptOutcome::InvalidResponse {
                message: "bad json".into(),
                used_weight_1m: None,
            },
            Duration::from_secs(1),
            1_800_000_000_000,
        );
        // Blocked until t=61s
        assert!(sched.poll_dispatch(Duration::from_secs(60), Some(ConnId::A)).is_empty());
        // Single probe allowed at t=61s
        let probe = sched.poll_dispatch(Duration::from_secs(61), Some(ConnId::A));
        assert_eq!(probe.len(), 1);
    }

    #[test]
    fn shared_ip_budget_respects_headroom_and_server_used_weight_header() {
        let mut cfg = SnapshotSchedulerConfig::for_limit(1000);
        cfg.ip_weight_limit_per_min = 2400;
        cfg.reserved_headroom_weight = 600; // max usable = 1800
        cfg.request_weight = 20;
        cfg.initial_connect_delay = Duration::ZERO;
        cfg.min_dispatch_spacing = Duration::ZERO;

        let mut sched = BinanceSnapshotScheduler::new(cfg);
        sched.register_connection_epoch(ConnId::A, 1, &["BTCUSDT".into(), "ETHUSDT".into()], Duration::ZERO);
        sched.register_connection_epoch(ConnId::B, 1, &["BTCUSDT".into(), "ETHUSDT".into()], Duration::ZERO);

        let b1 = sched.poll_dispatch(Duration::ZERO, Some(ConnId::A));
        assert_eq!(b1.len(), 2);

        // Response reports X-MBX-USED-WEIGHT-1M = 1790 -> adding weight 20 would exceed usable 1800!
        let _ = sched.on_response(
            b1[0].request_id,
            b1[0].conn_id,
            1,
            &b1[0].symbol,
            SnapshotAttemptOutcome::Success {
                last_update_id: 100,
                raw_json: r#"{"lastUpdateId":100,"bids":[],"asks":[]}"#.into(),
                used_weight_1m: Some(1790),
            },
            Duration::from_secs(1),
            1_800_000_000_000,
        );
        // Must hold back BOTH A and B until the 60s server weight observation window expires!
        assert!(sched.poll_dispatch(Duration::from_secs(2), Some(ConnId::A)).is_empty());
        assert_eq!(
            sched.next_wakeup_mono(Duration::from_secs(2)),
            Some(Duration::from_secs(61))
        );
        // At t=61s, window clears -> remaining request dispatches cleanly!
        let b2 = sched.poll_dispatch(Duration::from_secs(61), Some(ConnId::A));
        assert_eq!(b2.len(), 1);
    }

    #[test]
    fn coalesces_same_epoch_triggers_and_rejects_stale_epoch_or_superseded_responses() {
        let mut cfg = SnapshotSchedulerConfig::for_limit(1000);
        cfg.initial_connect_delay = Duration::ZERO;
        cfg.min_dispatch_spacing = Duration::ZERO;
        let mut sched = BinanceSnapshotScheduler::new(cfg);

        sched.register_connection_epoch(ConnId::A, 1, &["BTCUSDT".into()], Duration::ZERO);
        // Duplicate trigger while queued coalesces (still 1 pending)
        assert!(sched.trigger(ConnId::A, 1, "BTCUSDT", Duration::ZERO, "dup1", false).is_none());
        assert_eq!(sched.pending_count(), 1);

        let batch = sched.poll_dispatch(Duration::ZERO, Some(ConnId::A));
        assert_eq!(batch.len(), 1);
        let first_req_id = batch[0].request_id;

        // Duplicate non-superseding trigger while in-flight coalesces (still 1 in-flight!)
        assert!(sched.trigger(ConnId::A, 1, "BTCUSDT", Duration::from_millis(10), "dup2", false).is_none());
        assert_eq!(sched.in_flight_count(), 1);
        assert!(sched.poll_dispatch(Duration::from_millis(20), Some(ConnId::A)).is_empty());

        // Superseding trigger invalidates first_req_id and schedules replacement
        let superseded = sched.trigger(ConnId::A, 1, "BTCUSDT", Duration::from_millis(30), "resync", true);
        assert_eq!(superseded, Some(first_req_id));
        let batch2 = sched.poll_dispatch(Duration::from_millis(30), Some(ConnId::A));
        assert_eq!(batch2.len(), 1);
        assert_ne!(batch2[0].request_id, first_req_id);

        // Late response for superseded first_req_id MUST be ignored!
        let late = sched.on_response(
            first_req_id,
            ConnId::A,
            1,
            "BTCUSDT",
            SnapshotAttemptOutcome::Success {
                last_update_id: 500,
                raw_json: r#"{"lastUpdateId":500,"bids":[],"asks":[]}"#.into(),
                used_weight_1m: Some(20),
            },
            Duration::from_millis(40),
            1_800_000_000_000,
        );
        assert!(matches!(late, ResponseDisposition::IgnoredStale { .. }));

        // Advance connection A to epoch 2 -> cancels epoch 1 request and rejects late epoch 1 response!
        let cancelled = sched.register_connection_epoch(ConnId::A, 2, &["BTCUSDT".into()], Duration::from_millis(50));
        assert_eq!(cancelled, vec![batch2[0].request_id]);
        let stale_epoch_resp = sched.on_response(
            batch2[0].request_id,
            ConnId::A,
            1,
            "BTCUSDT",
            SnapshotAttemptOutcome::Success {
                last_update_id: 600,
                raw_json: r#"{"lastUpdateId":600,"bids":[],"asks":[]}"#.into(),
                used_weight_1m: Some(40),
            },
            Duration::from_millis(60),
            1_800_000_000_000,
        );
        assert!(matches!(stale_epoch_resp, ResponseDisposition::IgnoredStale { .. }));
    }

    #[test]
    fn six_symbols_recover_independently_across_diverse_failures() {
        let mut cfg = SnapshotSchedulerConfig::for_limit(1000);
        cfg.initial_connect_delay = Duration::ZERO;
        cfg.min_dispatch_spacing = Duration::ZERO;
        cfg.max_in_flight_normal = 2;
        cfg.base_backoff = Duration::from_secs(1);

        let mut sched = BinanceSnapshotScheduler::new(cfg);
        let syms = six_symbols();
        sched.register_connection_epoch(ConnId::A, 1, &syms, Duration::ZERO);

        let mut recovered = Vec::new();
        let mut failed_once: HashMap<String, bool> = HashMap::new();
        let mut now = Duration::ZERO;
        let base_wall_ms = 1_800_000_000_000i64;

        for _step in 0..40 {
            let batch = sched.poll_dispatch(now, Some(ConnId::A));
            if batch.is_empty() {
                if let Some(wake) = sched.next_wakeup_mono(now) {
                    now = wake;
                    continue;
                } else {
                    break;
                }
            }
            for req in batch {
                let first_fail = !failed_once.contains_key(&req.symbol);
                if first_fail {
                    failed_once.insert(req.symbol.clone(), true);
                    let outcome = match req.symbol.as_str() {
                        "BTCUSDT" => SnapshotAttemptOutcome::Timeout,
                        "ETHUSDT" => SnapshotAttemptOutcome::TransportError {
                            message: "connection reset".into(),
                        },
                        "XRPUSDT" => SnapshotAttemptOutcome::HttpError {
                            status: 503,
                            retry_after: None,
                            used_weight_1m: Some(60),
                            body_excerpt: "Service Unavailable".into(),
                        },
                        "SOLUSDT" => SnapshotAttemptOutcome::InvalidResponse {
                            message: "missing lastUpdateId".into(),
                            used_weight_1m: Some(80),
                        },
                        "DOGEUSDT" => SnapshotAttemptOutcome::RateLimited429 {
                            retry_after: Some(Duration::from_secs(5)),
                            banned_until_ms: None,
                            used_weight_1m: Some(100),
                            body_excerpt: "Too many requests".into(),
                        },
                        _ => SnapshotAttemptOutcome::Success {
                            last_update_id: 2000,
                            raw_json: r#"{"lastUpdateId":2000,"bids":[],"asks":[]}"#.into(),
                            used_weight_1m: Some(120),
                        },
                    };
                    let d = sched.on_response(
                        req.request_id,
                        ConnId::A,
                        1,
                        &req.symbol,
                        outcome,
                        now,
                        base_wall_ms + now.as_millis() as i64,
                    );
                    if matches!(d, ResponseDisposition::ApplySnapshot { .. }) {
                        // Simulate bridge failure once for SUIUSDT!
                        sched.on_bridge_failure(ConnId::A, 1, &req.symbol, now, "stale_snapshot");
                    }
                } else {
                    let d = sched.on_response(
                        req.request_id,
                        ConnId::A,
                        1,
                        &req.symbol,
                        SnapshotAttemptOutcome::Success {
                            last_update_id: 3000,
                            raw_json: r#"{"lastUpdateId":3000,"bids":[],"asks":[]}"#.into(),
                            used_weight_1m: Some(40),
                        },
                        now,
                        base_wall_ms + now.as_millis() as i64,
                    );
                    assert!(matches!(d, ResponseDisposition::ApplySnapshot { .. }));
                    recovered.push(req.symbol);
                }
            }
            now = now.saturating_add(Duration::from_millis(100));
        }
        recovered.sort();
        let mut expected = six_symbols();
        expected.sort();
        assert_eq!(recovered, expected);
        assert_eq!(sched.pending_count(), 0);
    }
}
