use crate::protocol::{FrameVerdict, ProtocolValidator};
use crate::types::{ConnState, EndpointKind};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Debug, Clone)]
struct BybitBookState {
    state: ConnState,
    prev_u: Option<u64>,
    refreshing: bool,
}

impl BybitBookState {
    fn new() -> Self {
        Self {
            state: ConnState::WaitAnchor,
            prev_u: None,
            refreshing: false,
        }
    }
}

pub struct BybitValidator {
    expected_symbols: Vec<String>,
    book50_states: HashMap<String, BybitBookState>,
    book1_states: HashMap<String, BybitBookState>,
    seen_trade_ids: HashMap<String, VecDeque<String>>,
    seen_trade_set: HashMap<String, HashSet<String>>,
    pending_sub_acks: Vec<(String, String)>,
    overall_invalid: bool,
    fatal_invalid: bool,
}

impl BybitValidator {
    pub fn new(symbols: Vec<String>) -> Self {
        let upper: Vec<String> = symbols.into_iter().map(|s| s.to_uppercase()).collect();
        let mut book50_states = HashMap::new();
        let mut book1_states = HashMap::new();
        for s in &upper {
            book50_states.insert(s.clone(), BybitBookState::new());
            book1_states.insert(s.clone(), BybitBookState::new());
        }
        Self {
            expected_symbols: upper,
            book50_states,
            book1_states,
            seen_trade_ids: HashMap::new(),
            seen_trade_set: HashMap::new(),
            pending_sub_acks: Vec::new(),
            overall_invalid: false,
            fatal_invalid: false,
        }
    }

    pub fn build_depth_message(symbols: &[String], op: &str, req_id: &str) -> Value {
        let args: Vec<String> = symbols
            .iter()
            .map(|s| format!("orderbook.50.{}", s.to_uppercase()))
            .collect();
        json!({"req_id": req_id, "op": op, "args": args})
    }

    pub fn build_subscribe_message(symbols: &[String]) -> Value {
        let mut args = Vec::with_capacity(symbols.len() * 3);
        for s in symbols {
            let sym = s.to_uppercase();
            args.push(format!("orderbook.50.{}", sym));
            args.push(format!("orderbook.1.{}", sym));
            args.push(format!("publicTrade.{}", sym));
        }
        json!({"op": "subscribe", "args": args})
    }

    pub fn build_ping_message() -> Value {
        json!({"op": "ping"})
    }

    fn track_trade_id(&mut self, sym: &str, trade_id: &str) -> bool {
        let q = self.seen_trade_ids.entry(sym.to_string()).or_default();
        let set = self.seen_trade_set.entry(sym.to_string()).or_default();
        if set.contains(trade_id) {
            return true;
        }
        set.insert(trade_id.to_string());
        q.push_back(trade_id.to_string());
        if q.len() > 16384 {
            if let Some(old) = q.pop_front() {
                set.remove(&old);
            }
        }
        false
    }

    fn handle_orderbook(&mut self, topic: &str, is_l1: bool, v: &Value) -> FrameVerdict {
        let sym = topic.rsplit('.').next().unwrap_or("").to_uppercase();
        if sym.is_empty() {
            self.overall_invalid = true;
            return FrameVerdict::Violation {
                symbol: None,
                reason: format!("bybit orderbook topic missing symbol: {}", topic),
                needs_resubscribe: true,
                needs_snapshot_refetch: false,
            };
        }
        let msg_type = v.get("type").and_then(Value::as_str).unwrap_or("");
        let data = match v.get("data") {
            Some(d) => d,
            None => {
                self.overall_invalid = true;
                return FrameVerdict::Violation {
                    symbol: Some(sym),
                    reason: "bybit orderbook missing data object".to_string(),
                    needs_resubscribe: true,
                    needs_snapshot_refetch: false,
                };
            }
        };
        let u = match data.get("u").and_then(Value::as_u64) {
            Some(val) => val,
            None => {
                self.overall_invalid = true;
                return FrameVerdict::Violation {
                    symbol: Some(sym),
                    reason: "bybit orderbook missing integer u".to_string(),
                    needs_resubscribe: true,
                    needs_snapshot_refetch: false,
                };
            }
        };

        let map = if is_l1 {
            &mut self.book1_states
        } else {
            &mut self.book50_states
        };
        let st = map.entry(sym.clone()).or_insert_with(BybitBookState::new);
        let channel = if is_l1 { "orderbook.1" } else { "orderbook.50" }.to_string();

        // Bybit spec: type == "snapshot" OR u == 1 (service restart) MUST full-reset local book.
        // Also for orderbook.1, 3s silence re-pushes snapshot with identical u.
        if msg_type == "snapshot" || u == 1 {
            let diag = if u == 1 && st.prev_u.is_some() {
                Some("bybit_server_restart_u_eq_1_reset".to_string())
            } else if is_l1 && st.prev_u == Some(u) {
                Some("bybit_l1_3s_silent_snapshot_repush".to_string())
            } else {
                None
            };
            st.prev_u = Some(u);
            st.state = ConnState::Live;
            st.refreshing = false;
            if !is_l1
                && !self.fatal_invalid
                && self
                    .book50_states
                    .values()
                    .all(|s| s.state != ConnState::Invalid)
            {
                self.overall_invalid = false;
            }
            return FrameVerdict::AcceptData {
                symbol: sym,
                channel,
                is_anchor_reset: true,
                diagnostic: diag,
            };
        }

        if msg_type == "delta" {
            if !is_l1 && st.refreshing {
                return FrameVerdict::BufferedWaitAnchor { symbol: sym };
            }
            if st.state != ConnState::Live {
                return FrameVerdict::Violation {
                    symbol: Some(sym),
                    reason: format!("bybit {} delta arrived before snapshot anchor", channel),
                    needs_resubscribe: true,
                    needs_snapshot_refetch: false,
                };
            }
            // NEVER check u == prev_u + 1 (Bybit jumps u within 20ms aggregation!).
            // Enforce monotonic non-decreasing u only.
            if let Some(prev) = st.prev_u {
                if u < prev {
                    st.state = ConnState::Invalid;
                    st.prev_u = None;
                    self.overall_invalid = true;
                    return FrameVerdict::Violation {
                        symbol: Some(sym),
                        reason: format!("bybit {} u regressed: u={} < prev_u={}", channel, u, prev),
                        needs_resubscribe: true,
                        needs_snapshot_refetch: false,
                    };
                }
            }
            st.prev_u = Some(u);
            return FrameVerdict::AcceptData {
                symbol: sym,
                channel,
                is_anchor_reset: false,
                diagnostic: None,
            };
        }

        self.overall_invalid = true;
        FrameVerdict::Violation {
            symbol: Some(sym),
            reason: format!("bybit unknown orderbook type={}", msg_type),
            needs_resubscribe: true,
            needs_snapshot_refetch: false,
        }
    }
}

impl ProtocolValidator for BybitValidator {
    fn endpoint(&self) -> EndpointKind {
        EndpointKind::BybitPublic
    }

    fn state(&self) -> ConnState {
        if self.overall_invalid {
            return ConnState::Invalid;
        }
        if self.expected_symbols.is_empty() {
            return ConnState::WaitAnchor;
        }
        if self.expected_symbols.iter().all(|s| {
            self.book50_states
                .get(s)
                .map(|b| b.state == ConnState::Live)
                .unwrap_or(false)
        }) {
            ConnState::Live
        } else {
            ConnState::WaitAnchor
        }
    }

    fn on_text_frame(&mut self, raw: &str) -> FrameVerdict {
        let v: Value = match serde_json::from_str(raw) {
            Ok(v) => v,
            Err(e) => {
                self.overall_invalid = true;
                return FrameVerdict::Violation {
                    symbol: None,
                    reason: format!("invalid bybit JSON: {}", e),
                    needs_resubscribe: true,
                    needs_snapshot_refetch: false,
                };
            }
        };

        if let Some(op) = v.get("op").and_then(Value::as_str) {
            if op == "pong" || op == "ping" {
                return FrameVerdict::Control {
                    note: format!("bybit heartbeat {}", op),
                };
            }
            if op == "subscribe" {
                if v.get("success").and_then(Value::as_bool) == Some(true) {
                    if let Some(args) = v.get("args").and_then(Value::as_array) {
                        for item in args {
                            if let Some(topic) = item.as_str() {
                                if let Some((chan, sym)) = topic.rsplit_once('.') {
                                    self.pending_sub_acks
                                        .push((sym.to_uppercase(), chan.to_string()));
                                }
                            }
                        }
                    } else {
                        for sym in &self.expected_symbols {
                            for chan in ["orderbook.50", "orderbook.1", "publicTrade"] {
                                self.pending_sub_acks.push((sym.clone(), chan.to_string()));
                            }
                        }
                    }
                    return FrameVerdict::Control {
                        note: "bybit subscribe ack".to_string(),
                    };
                }
                self.overall_invalid = true;
                return FrameVerdict::Violation {
                    symbol: None,
                    reason: format!("bybit subscribe failed: {}", raw),
                    needs_resubscribe: true,
                    needs_snapshot_refetch: false,
                };
            }
            if op == "unsubscribe" {
                if v.get("success").and_then(Value::as_bool) == Some(true) {
                    return FrameVerdict::Control {
                        note: "bybit unsubscribe ack".to_string(),
                    };
                }
                self.overall_invalid = true;
                return FrameVerdict::Violation {
                    symbol: None,
                    reason: format!("bybit unsubscribe failed: {}", raw),
                    needs_resubscribe: false,
                    needs_snapshot_refetch: false,
                };
            }
        }

        let topic = match v.get("topic").and_then(Value::as_str) {
            Some(t) => t,
            None => {
                return FrameVerdict::Control {
                    note: "bybit non-topic control message".to_string(),
                }
            }
        };

        if topic.starts_with("orderbook.50.") {
            return self.handle_orderbook(topic, false, &v);
        }
        if topic.starts_with("orderbook.1.") {
            return self.handle_orderbook(topic, true, &v);
        }
        if topic.starts_with("publicTrade.") {
            let sym = topic.rsplit('.').next().unwrap_or("").to_uppercase();
            let items = match v.get("data").and_then(Value::as_array) {
                Some(arr) => arr,
                None => {
                    return FrameVerdict::Violation {
                        symbol: Some(sym),
                        reason: "bybit publicTrade missing data array".to_string(),
                        needs_resubscribe: false,
                        needs_snapshot_refetch: false,
                    }
                }
            };
            let mut dups = 0usize;
            for item in items {
                let tid = match item.get("i").and_then(Value::as_str) {
                    Some(id) => id,
                    None => {
                        return FrameVerdict::Violation {
                            symbol: Some(sym),
                            reason: "bybit publicTrade item missing trade id 'i'".to_string(),
                            needs_resubscribe: false,
                            needs_snapshot_refetch: false,
                        }
                    }
                };
                if self.track_trade_id(&sym, tid) {
                    dups += 1;
                }
            }
            return FrameVerdict::AcceptData {
                symbol: sym,
                channel: "publicTrade".to_string(),
                is_anchor_reset: false,
                diagnostic: if dups > 0 {
                    Some(format!("bybit_duplicate_trade_ids_in_frame={}", dups))
                } else {
                    None
                },
            };
        }

        FrameVerdict::Control {
            note: format!("bybit unrecognized topic {}", topic),
        }
    }

    fn take_subscription_acks(&mut self) -> Vec<(String, String)> {
        std::mem::take(&mut self.pending_sub_acks)
    }

    fn expected_symbols(&self) -> Vec<String> {
        self.expected_symbols.clone()
    }

    fn symbol_depth_state(&self, symbol: &str) -> Option<ConnState> {
        self.book50_states
            .get(&symbol.to_uppercase())
            .map(|st| if st.refreshing { ConnState::Resync } else { st.state })
    }

    fn prepare_depth_refresh(&mut self) {
        self.overall_invalid = false;
        self.fatal_invalid = false;
        self.pending_sub_acks.clear();
        for st in self.book50_states.values_mut() {
            *st = BybitBookState {
                state: ConnState::WaitAnchor,
                prev_u: None,
                refreshing: true,
            };
        }
    }

    fn prepare_symbol_depth_refresh(&mut self, symbol: &str) {
        if self.overall_invalid
            && self
                .book50_states
                .values()
                .all(|s| s.state != ConnState::Invalid)
        {
            self.fatal_invalid = true;
        }
        let sym = symbol.to_uppercase();
        let st = self
            .book50_states
            .entry(sym)
            .or_insert_with(BybitBookState::new);
        if st.state != ConnState::Invalid {
            st.state = ConnState::WaitAnchor;
        }
        st.prev_u = None;
        st.refreshing = true;
        self.overall_invalid = self.overall_invalid
            || self
                .book50_states
                .values()
                .any(|s| s.state == ConnState::Invalid);
    }

    fn reset(&mut self) {
        self.overall_invalid = false;
        self.fatal_invalid = false;
        self.pending_sub_acks.clear();
        for st in self.book50_states.values_mut() {
            *st = BybitBookState::new();
        }
        for st in self.book1_states.values_mut() {
            *st = BybitBookState::new();
        }
    }

    fn live_symbol_count(&self) -> usize {
        self.book50_states
            .values()
            .filter(|b| b.state == ConnState::Live)
            .count()
    }

    fn expected_symbol_count(&self) -> usize {
        self.expected_symbols.len()
    }
}
