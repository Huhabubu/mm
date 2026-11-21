use crate::protocol::{make_validator, FrameVerdict, ProtocolValidator};
use crate::types::{
    CanonicalRecord, ConnId, ConnState, EndpointKind, ExchangeId, RawFrame, RecordKind,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SelectorAction {
    /// Request REST snapshot refresh (Binance) for (conn_id, symbol).
    FetchRestSnapshot { conn_id: ConnId, symbol: String },
    /// Request channel resubscription on a connection to get a clean full snapshot anchor.
    Resubscribe { conn_id: ConnId, reason: String },
    /// Reconnect the specified connection (e.g. planned rotation or fatal error).
    Reconnect { conn_id: ConnId, reason: String },
    /// Trigger an alert (e.g. dual outage > 5s or LOCAL_LOSS).
    Alert { severity: String, message: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectorOutput {
    pub records: Vec<CanonicalRecord>,
    pub actions: Vec<SelectorAction>,
    pub market_data: Option<(String, String)>,
}

struct ConnRuntime {
    validator: Box<dyn ProtocolValidator>,
    epoch: u64,
    connected: bool,
    last_msg_mono_ns: u64,
    last_pong_mono_ns: u64,
    last_symbol_mono_ns: HashMap<String, u64>,
    resync_requested: bool,
}

impl ConnRuntime {
    fn new(endpoint: EndpointKind, symbols: Vec<String>) -> Self {
        Self {
            validator: make_validator(endpoint, symbols),
            epoch: 1,
            connected: true,
            last_msg_mono_ns: 0,
            last_pong_mono_ns: 0,
            last_symbol_mono_ns: HashMap::new(),
            resync_requested: false,
        }
    }

    fn state(&self) -> ConnState {
        if !self.connected {
            return ConnState::Invalid;
        }
        self.validator.state()
    }
}

/// Strictly causal A/B active-active selector for a single exchange endpoint.
///
/// Guarantees:
/// 1. Conn A and Conn B maintain completely independent protocol state machines.
/// 2. Deltas from A and B are NEVER spliced across connections.
/// 3. Only the currently active primary's raw DATA frames are emitted to disk.
/// 4. Planned rotations switch ONLY when standby is LIVE (0 ms gap).
/// 5. Faults switch immediately to a LIVE standby (with SWITCH + fresh anchor trigger),
///    or emit explicit GAP_START / GAP_END if neither connection is LIVE.
pub struct CausalSelector {
    exchange: ExchangeId,
    endpoint: EndpointKind,
    primary: ConnId,
    conn_a: ConnRuntime,
    conn_b: ConnRuntime,
    rx_seq: u64,
    in_gap: bool,
    gap_start_mono_ns: Option<u64>,
    silence_threshold_ns: u64,
    dual_outage_alert_ns: u64,
    dual_outage_alerted: bool,
    /// Counters for audit/manifest diagnostics.
    pub switch_count: u64,
    pub gap_count: u64,
    pub local_loss_count: u64,
}

impl CausalSelector {
    pub fn new(
        endpoint: EndpointKind,
        symbols: Vec<String>,
        silence_watchdog_seconds: u64,
        dual_outage_alert_seconds: u64,
    ) -> Self {
        Self {
            exchange: endpoint.exchange(),
            endpoint,
            primary: ConnId::A,
            conn_a: ConnRuntime::new(endpoint, symbols.clone()),
            conn_b: ConnRuntime::new(endpoint, symbols),
            rx_seq: 0,
            in_gap: false,
            gap_start_mono_ns: None,
            silence_threshold_ns: silence_watchdog_seconds.saturating_mul(1_000_000_000),
            dual_outage_alert_ns: dual_outage_alert_seconds.saturating_mul(1_000_000_000),
            dual_outage_alerted: false,
            switch_count: 0,
            gap_count: 0,
            local_loss_count: 0,
        }
    }

    pub fn exchange(&self) -> ExchangeId {
        self.exchange
    }

    pub fn endpoint(&self) -> EndpointKind {
        self.endpoint
    }

    pub fn primary(&self) -> ConnId {
        self.primary
    }

    pub fn in_gap(&self) -> bool {
        self.in_gap
    }

    pub fn conn_state(&self, id: ConnId) -> ConnState {
        self.runtime(id).state()
    }

    pub(crate) fn start_disconnected(&mut self) {
        self.conn_a.connected = false;
        self.conn_b.connected = false;
    }

    pub(crate) fn connected_epoch(&self, id: ConnId) -> Option<u64> {
        let connection = self.runtime(id);
        connection.connected.then_some(connection.epoch)
    }

    pub(crate) fn allow_reconnect_retry(&mut self, id: ConnId) {
        self.runtime_mut(id).resync_requested = false;
    }

    fn runtime(&self, id: ConnId) -> &ConnRuntime {
        match id {
            ConnId::A => &self.conn_a,
            ConnId::B => &self.conn_b,
        }
    }

    fn runtime_mut(&mut self, id: ConnId) -> &mut ConnRuntime {
        match id {
            ConnId::A => &mut self.conn_a,
            ConnId::B => &mut self.conn_b,
        }
    }

    fn next_seq(&mut self) -> u64 {
        self.rx_seq += 1;
        self.rx_seq
    }

    fn emit_meta(
        &mut self,
        wall_ns: i64,
        mono_ns: u64,
        conn_id: ConnId,
        payload: serde_json::Value,
    ) -> CanonicalRecord {
        let epoch = self.runtime(conn_id).epoch;
        let rx_seq = self.next_seq();
        CanonicalRecord {
            recv_wall_ns: wall_ns,
            recv_mono_ns: mono_ns,
            conn_id,
            epoch,
            rx_seq,
            kind: RecordKind::Meta,
            payload: payload.to_string(),
        }
    }

    fn emit_data(&mut self, frame: &RawFrame) -> CanonicalRecord {
        let rx_seq = self.next_seq();
        let needs_escape = frame
            .payload
            .bytes()
            .any(|b| b == b'\n' || b == b'\r' || b == b'\t');
        let (kind, payload) = if needs_escape {
            (
                RecordKind::DataEscaped,
                serde_json::to_string(&frame.payload).expect("string JSON escaping cannot fail"),
            )
        } else {
            (RecordKind::Data, frame.payload.clone())
        };
        CanonicalRecord {
            recv_wall_ns: frame.recv_wall_ns,
            recv_mono_ns: frame.recv_mono_ns,
            conn_id: frame.conn_id,
            epoch: frame.epoch,
            rx_seq,
            kind,
            payload,
        }
    }

    /// Process a single raw frame stamped before JSON parsing.
    pub fn on_frame(&mut self, frame: RawFrame) -> SelectorOutput {
        let mut out = SelectorOutput {
            records: Vec::new(),
            actions: Vec::new(),
            market_data: None,
        };

        let conn_id = frame.conn_id;
        let wall_ns = frame.recv_wall_ns;
        let mono_ns = frame.recv_mono_ns;

        let current_epoch = self.runtime(conn_id).epoch;
        if frame.epoch < current_epoch || (frame.epoch == current_epoch && !self.runtime(conn_id).connected) {
            return out;
        }
        if frame.epoch > current_epoch {
            // Replay callers may start a new epoch with the first frame rather than a lifecycle event.
            self.on_reconnect(conn_id, frame.epoch, mono_ns);
        }

        let verdict = {
            let rt = self.runtime_mut(conn_id);
            rt.last_msg_mono_ns = mono_ns;
            rt.validator.on_text_frame(&frame.payload)
        };

        match verdict {
            FrameVerdict::Control { note } => {
                if note.contains("pong") {
                    self.runtime_mut(conn_id).last_pong_mono_ns = mono_ns;
                }
            }
            FrameVerdict::BufferedWaitAnchor { symbol: _ } => {
                // Buffered inside this connection's independent validator waiting for REST snapshot.
            }
            FrameVerdict::AcceptData {
                symbol,
                channel,
                is_anchor_reset: _,
                diagnostic: _,
            } => {
                self.runtime_mut(conn_id)
                    .last_symbol_mono_ns
                    .insert(symbol.clone(), mono_ns);

                let conn_is_live = self.runtime(conn_id).state() == ConnState::Live;

                // If we were in a gap and this connection just became LIVE, recover causally now.
                if self.in_gap && conn_is_live {
                    let prev_primary = self.primary;
                    self.primary = conn_id;
                    self.in_gap = false;
                    self.gap_start_mono_ns = None;
                    self.dual_outage_alerted = false;
                    out.records.push(self.emit_meta(
                        wall_ns,
                        mono_ns,
                        conn_id,
                        json!({
                            "event": "GAP_END",
                            "endpoint": self.endpoint.as_str(),
                            "recovered_conn": conn_id.as_str(),
                            "previous_primary": prev_primary.as_str()
                        }),
                    ));
                    out.records.push(self.emit_data(&frame));
                    out.market_data = Some((symbol, channel));
                    return out;
                }

                // If primary is not LIVE (e.g. startup WaitAnchor) and standby became LIVE first, promote standby.
                if conn_id != self.primary
                    && conn_is_live
                    && self.runtime(self.primary).state() != ConnState::Live
                    && !self.in_gap
                {
                    let old_primary = self.primary;
                    self.primary = conn_id;
                    self.switch_count += 1;
                    out.records.push(self.emit_meta(
                        wall_ns,
                        mono_ns,
                        conn_id,
                        json!({
                            "event": "SWITCH",
                            "endpoint": self.endpoint.as_str(),
                            "from": old_primary.as_str(),
                            "to": conn_id.as_str(),
                            "reason": "initial_or_standby_ready_first",
                            "symbol": "ALL"
                        }),
                    ));
                    out.records.push(self.emit_data(&frame));
                    out.market_data = Some((symbol, channel));
                    return out;
                }

                // Normal single-stream emission: ONLY write active primary to disk.
                if conn_id == self.primary && !self.in_gap {
                    out.records.push(self.emit_data(&frame));
                    out.market_data = Some((symbol, channel));
                }
            }
            FrameVerdict::Violation {
                symbol,
                reason,
                needs_resubscribe,
                needs_snapshot_refetch,
            } => {
                if needs_snapshot_refetch {
                    if let Some(sym) = symbol.clone() {
                        out.actions
                            .push(SelectorAction::FetchRestSnapshot { conn_id, symbol: sym });
                    }
                }
                if needs_resubscribe {
                    out.actions.push(SelectorAction::Resubscribe {
                        conn_id,
                        reason: reason.clone(),
                    });
                }
                if conn_id == self.primary {
                    self.handle_primary_fault(wall_ns, mono_ns, &reason, &mut out);
                } else {
                    out.records.push(self.emit_meta(
                        wall_ns,
                        mono_ns,
                        conn_id,
                        json!({
                            "event": "STANDBY_INVALID",
                            "endpoint": self.endpoint.as_str(),
                            "conn": conn_id.as_str(),
                            "reason": reason
                        }),
                    ));
                }
            }
        }

        out
    }

    /// Apply REST depth snapshot (Binance /public) to a specific connection independently.
    pub fn on_rest_snapshot(
        &mut self,
        conn_id: ConnId,
        symbol: &str,
        last_update_id: u64,
        raw_snapshot_json: &str,
        wall_ns: i64,
        mono_ns: u64,
    ) -> SelectorOutput {
        let mut out = SelectorOutput {
            records: Vec::new(),
            actions: Vec::new(),
            market_data: None,
        };

        let res = self
            .runtime_mut(conn_id)
            .validator
            .on_rest_snapshot(symbol, last_update_id);

        match res {
            Ok(bridged) => {
                let conn_live = self.runtime(conn_id).state() == ConnState::Live;
                if conn_id == self.primary || (self.in_gap && conn_live) {
                    if self.in_gap && conn_live {
                        let prev_primary = self.primary;
                        self.primary = conn_id;
                        self.in_gap = false;
                        self.gap_start_mono_ns = None;
                        self.dual_outage_alerted = false;
                        out.records.push(self.emit_meta(
                            wall_ns,
                            mono_ns,
                            conn_id,
                            json!({
                                "event": "GAP_END",
                                "endpoint": self.endpoint.as_str(),
                                "recovered_conn": conn_id.as_str(),
                                "previous_primary": prev_primary.as_str()
                            }),
                        ));
                    }
                    // Emit raw REST snapshot anchor into canonical stream at true current clock.
                    let rx_seq = self.next_seq();
                    let epoch = self.runtime(conn_id).epoch;
                    let wrapper = json!({
                        "event": "REST_SNAPSHOT_ANCHOR",
                        "endpoint": self.endpoint.as_str(),
                        "symbol": symbol,
                        "lastUpdateId": last_update_id,
                        "bridged_buffered_deltas": bridged,
                        "snapshot": serde_json::from_str::<serde_json::Value>(raw_snapshot_json)
                            .unwrap_or_else(|_| json!({"raw_parse_error": true}))
                    });
                    out.records.push(CanonicalRecord {
                        recv_wall_ns: wall_ns,
                        recv_mono_ns: mono_ns,
                        conn_id,
                        epoch,
                        rx_seq,
                        kind: RecordKind::Meta,
                        payload: wrapper.to_string(),
                    });
                }
            }
            Err(err) => {
                out.actions.push(SelectorAction::FetchRestSnapshot {
                    conn_id,
                    symbol: symbol.to_string(),
                });
                out.records.push(self.emit_meta(
                    wall_ns,
                    mono_ns,
                    conn_id,
                    json!({
                        "event": "SNAPSHOT_BRIDGE_RETRY",
                        "endpoint": self.endpoint.as_str(),
                        "conn": conn_id.as_str(),
                        "symbol": symbol,
                        "reason": err
                    }),
                ));
            }
        }
        out
    }

    /// Notify selector that a connection disconnected or encountered read error.
    pub fn on_disconnect(
        &mut self,
        conn_id: ConnId,
        wall_ns: i64,
        mono_ns: u64,
        reason: &str,
    ) -> SelectorOutput {
        let mut out = SelectorOutput {
            records: Vec::new(),
            actions: Vec::new(),
            market_data: None,
        };
        {
            let rt = self.runtime_mut(conn_id);
            rt.connected = false;
            rt.validator.reset();
        }
        if conn_id == self.primary {
            self.handle_primary_fault(wall_ns, mono_ns, reason, &mut out);
        } else {
            out.records.push(self.emit_meta(
                wall_ns,
                mono_ns,
                conn_id,
                json!({
                    "event": "STANDBY_DISCONNECT",
                    "endpoint": self.endpoint.as_str(),
                    "conn": conn_id.as_str(),
                    "reason": reason
                }),
            ));
        }
        out
    }

    pub fn prepare_depth_refresh(&mut self, conn_id: ConnId) {
        self.runtime_mut(conn_id).validator.prepare_depth_refresh();
    }

    /// Notify selector that a connection reconnected cleanly with a new epoch.
    pub fn on_reconnect(&mut self, conn_id: ConnId, new_epoch: u64, mono_ns: u64) {
        let rt = self.runtime_mut(conn_id);
        rt.epoch = new_epoch;
        rt.connected = true;
        rt.last_msg_mono_ns = mono_ns;
        rt.last_pong_mono_ns = mono_ns;
        rt.resync_requested = false;
        rt.validator.reset();
    }

    /// Trigger staggered planned rotation (00:00 UTC for A, 12:00 UTC for B).
    /// Zero-gap rule: if rotating primary, ONLY switch when standby is LIVE.
    pub fn on_planned_rotation(
        &mut self,
        target: ConnId,
        wall_ns: i64,
        mono_ns: u64,
    ) -> SelectorOutput {
        let mut out = SelectorOutput {
            records: Vec::new(),
            actions: Vec::new(),
            market_data: None,
        };

        if self.connected_epoch(target).is_none() {
            return out;
        }
        if target == self.primary {
            let standby = target.peer();
            if self.runtime(standby).state() == ConnState::Live {
                self.primary = standby;
                self.switch_count += 1;
                out.records.push(self.emit_meta(
                    wall_ns,
                    mono_ns,
                    standby,
                    json!({
                        "event": "SWITCH",
                        "endpoint": self.endpoint.as_str(),
                        "from": target.as_str(),
                        "to": standby.as_str(),
                        "reason": "planned_rotation",
                        "symbol": "ALL",
                        "trigger_fresh_anchor": true
                    }),
                ));
                // Trigger fresh anchor on new primary for seamless offline replay without backdating.
                out.actions.push(SelectorAction::Resubscribe {
                    conn_id: standby,
                    reason: "post_switch_fresh_anchor".to_string(),
                });
                out.actions.push(SelectorAction::Reconnect {
                    conn_id: target,
                    reason: "planned_rotation".to_string(),
                });
            } else {
                // Defer rotation rather than creating a self-inflicted gap!
                out.records.push(self.emit_meta(
                    wall_ns,
                    mono_ns,
                    target,
                    json!({
                        "event": "ROTATION_DEFERRED",
                        "endpoint": self.endpoint.as_str(),
                        "target": target.as_str(),
                        "standby_state": format!("{:?}", self.runtime(standby).state())
                    }),
                ));
            }
        } else {
            // Rotating standby directly does not disturb primary stream at all.
            out.actions.push(SelectorAction::Reconnect {
                conn_id: target,
                reason: "planned_standby_rotation".to_string(),
            });
        }

        out
    }

    /// Smart silence watchdog (3s diagnostic rule, Section 3.2.3).
    /// Does NOT blindly kick a connection when a quiet symbol has no updates if:
    /// - ping/pong is healthy AND other symbols on the same connection are still pushing.
    /// Never restarts a healthy standby simultaneously.
    pub fn check_silence_watchdog(
        &mut self,
        now_wall_ns: i64,
        now_mono_ns: u64,
        ping_timeout_ns: u64,
    ) -> SelectorOutput {
        let mut out = SelectorOutput {
            records: Vec::new(),
            actions: Vec::new(),
            market_data: None,
        };

        // Check dual outage > 5s alert condition
        if self.in_gap {
            if let Some(start) = self.gap_start_mono_ns {
                if !self.dual_outage_alerted
                    && now_mono_ns.saturating_sub(start) >= self.dual_outage_alert_ns
                {
                    self.dual_outage_alerted = true;
                    out.actions.push(SelectorAction::Alert {
                        severity: "CRITICAL".to_string(),
                        message: format!(
                            "{} dual A/B outage exceeded {}s",
                            self.endpoint.as_str(),
                            self.dual_outage_alert_ns / 1_000_000_000
                        ),
                    });
                }
            }
        }

        for conn_id in [ConnId::A, ConnId::B] {
            let peer = conn_id.peer();
            let (connected, last_msg, last_pong, already_resync) = {
                let rt = self.runtime(conn_id);
                (
                    rt.connected,
                    rt.last_msg_mono_ns,
                    rt.last_pong_mono_ns,
                    rt.resync_requested,
                )
            };
            if !connected || last_msg == 0 || already_resync {
                continue;
            }
            let conn_silent_ns = now_mono_ns.saturating_sub(last_msg);
            if conn_silent_ns >= self.silence_threshold_ns {
                let pong_age_ns = now_mono_ns.saturating_sub(last_pong);
                let pong_timed_out = last_pong > 0 && pong_age_ns >= ping_timeout_ns;
                let peer_live = self.runtime(peer).state() == ConnState::Live;

                out.records.push(self.emit_meta(
                    now_wall_ns,
                    now_mono_ns,
                    conn_id,
                    json!({
                        "event": "SILENCE_DIAGNOSTIC",
                        "endpoint": self.endpoint.as_str(),
                        "conn": conn_id.as_str(),
                        "silent_ms": conn_silent_ns / 1_000_000,
                        "pong_timed_out": pong_timed_out,
                        "peer_live": peer_live
                    }),
                ));

                // Only reconnect if protocol heartbeat timed out OR whole connection wedged well past silence threshold.
                if pong_timed_out || conn_silent_ns >= self.silence_threshold_ns.saturating_mul(3) {
                    self.runtime_mut(conn_id).resync_requested = true;
                    if conn_id == self.primary {
                        self.handle_primary_fault(
                            now_wall_ns,
                            now_mono_ns,
                            "watchdog_connection_wedged",
                            &mut out,
                        );
                    }
                    out.actions.push(SelectorAction::Reconnect {
                        conn_id,
                        reason: "watchdog_connection_wedged".to_string(),
                    });
                    // Never restart both connections simultaneously if peer is healthy.
                    if peer_live {
                        break;
                    }
                }
            }
        }

        out
    }

    /// Record explicit LOCAL_LOSS when bounded queue overflows or disk write stalls.
    pub fn on_local_loss(
        &mut self,
        dropped_frames: u64,
        wall_ns: i64,
        mono_ns: u64,
        reason: &str,
    ) -> SelectorOutput {
        self.local_loss_count = self.local_loss_count.saturating_add(dropped_frames);
        let rec = self.emit_meta(
            wall_ns,
            mono_ns,
            self.primary,
            json!({
                "event": "LOCAL_LOSS",
                "endpoint": self.endpoint.as_str(),
                "dropped_frames": dropped_frames,
                "reason": reason
            }),
        );
        SelectorOutput {
            records: vec![rec],
            market_data: None,
            actions: vec![SelectorAction::Alert {
                severity: "CRITICAL".to_string(),
                message: format!(
                    "LOCAL_LOSS on {}: dropped={} reason={}",
                    self.endpoint.as_str(),
                    dropped_frames,
                    reason
                ),
            }],
        }
    }

    fn handle_primary_fault(
        &mut self,
        wall_ns: i64,
        mono_ns: u64,
        reason: &str,
        out: &mut SelectorOutput,
    ) {
        let old_primary = self.primary;
        let standby = old_primary.peer();
        if self.runtime(standby).state() == ConnState::Live {
            self.primary = standby;
            self.switch_count += 1;
            out.records.push(self.emit_meta(
                wall_ns,
                mono_ns,
                standby,
                json!({
                    "event": "SWITCH",
                    "endpoint": self.endpoint.as_str(),
                    "from": old_primary.as_str(),
                    "to": standby.as_str(),
                    "reason": reason,
                    "symbol": "ALL",
                    "trigger_fresh_anchor": true
                }),
            ));
            // Request fresh anchor on new primary so offline book rebuilds cleanly without splicing A & B.
            out.actions.push(SelectorAction::Resubscribe {
                conn_id: standby,
                reason: "post_failover_fresh_anchor".to_string(),
            });
        } else if !self.in_gap {
            self.in_gap = true;
            self.gap_start_mono_ns = Some(mono_ns);
            self.gap_count += 1;
            out.records.push(self.emit_meta(
                wall_ns,
                mono_ns,
                old_primary,
                json!({
                    "event": "GAP_START",
                    "endpoint": self.endpoint.as_str(),
                    "primary": old_primary.as_str(),
                    "standby_state": format!("{:?}", self.runtime(standby).state()),
                    "reason": reason
                }),
            ));
        }
    }
}
