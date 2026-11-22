use crate::protocol::{FrameVerdict, ProtocolValidator};
use crate::types::{ConnState, EndpointKind};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Debug, Clone)]
struct GateObuState {
    state: ConnState,
    prev_u: Option<u64>,
    refreshing: bool,
}

impl GateObuState {
    fn new() -> Self {
        Self {
            state: ConnState::WaitAnchor,
            prev_u: None,
            refreshing: false,
        }
    }
}

pub struct GateValidator {
    expected_symbols: Vec<String>,
    obu_states: HashMap<String, GateObuState>,
    seen_trade_ids: HashMap<String, VecDeque<u64>>,
    seen_trade_set: HashMap<String, HashSet<u64>>,
    pending_sub_acks: Vec<(String, String)>,
    extra_accepted_streams: Vec<(String, String)>,
    overall_invalid: bool,
    fatal_invalid: bool,
}

impl GateValidator {
    pub fn new(symbols: Vec<String>) -> Self {
        let upper: Vec<String> = symbols.into_iter().map(|s| s.to_uppercase()).collect();
        let mut obu_states = HashMap::new();
        for s in &upper {
            obu_states.insert(s.clone(), GateObuState::new());
        }
        Self {
            expected_symbols: upper,
            obu_states,
            seen_trade_ids: HashMap::new(),
            seen_trade_set: HashMap::new(),
            pending_sub_acks: Vec::new(),
            extra_accepted_streams: Vec::new(),
            overall_invalid: false,
            fatal_invalid: false,
        }
    }

    pub fn build_depth_messages(symbols: &[String], now_sec: i64, event: &str) -> Vec<Value> {
        symbols.iter().map(|symbol| json!({
            "time": now_sec,
            "channel": "futures.obu",
            "event": event,
            "payload": [format!("ob.{}.50", symbol.to_uppercase())]
        })).collect()
    }

    /// Build subscribe frames for Gate Futures v4 (obu.50, book_ticker, trades).
    pub fn build_subscribe_messages(symbols: &[String], now_sec: i64) -> Vec<Value> {
        let mut msgs = Self::build_depth_messages(symbols, now_sec, "subscribe");
        let upper: Vec<String> = symbols.iter().map(|s| s.to_uppercase()).collect();
        if !upper.is_empty() {
            msgs.push(json!({
                "time": now_sec,
                "channel": "futures.book_ticker",
                "event": "subscribe",
                "payload": upper
            }));
            msgs.push(json!({
                "time": now_sec,
                "channel": "futures.trades",
                "event": "subscribe",
                "payload": upper
            }));
        }
        msgs
    }

    pub fn build_ping_message(now_sec: i64) -> Value {
        json!({"time": now_sec, "channel": "futures.ping"})
    }

    /// Extract contract symbol from Gate futures.obu stream name "ob.BTC_USDT.50" -> "BTC_USDT".
    fn parse_obu_symbol(stream_name: &str) -> Option<String> {
        let parts: Vec<&str> = stream_name.split('.').collect();
        if parts.len() == 3 && parts[0] == "ob" {
            Some(parts[1].to_uppercase())
        } else {
            None
        }
    }

    /// Under X-Gate-Size-Decimal: 1, all Gate sizes MUST be JSON strings (never truncated ints).
    fn check_decimal_size_string(v: &Value) -> bool {
        match v.as_str() {
            Some(s) => !s.is_empty() && s.parse::<f64>().map(|x| x.is_finite()).unwrap_or(false),
            None => false,
        }
    }

    fn track_trade_id(&mut self, contract: &str, trade_id: u64) -> bool {
        let q = self.seen_trade_ids.entry(contract.to_string()).or_default();
        let set = self.seen_trade_set.entry(contract.to_string()).or_default();
        if set.contains(&trade_id) {
            return true;
        }
        set.insert(trade_id);
        q.push_back(trade_id);
        if q.len() > 16384 {
            if let Some(old) = q.pop_front() {
                set.remove(&old);
            }
        }
        false
    }

    fn handle_obu(&mut self, result: &Value) -> FrameVerdict {
        let stream_name = match result.get("s").and_then(Value::as_str) {
            Some(s) => s,
            None => {
                self.overall_invalid = true;
                return FrameVerdict::Violation {
                    symbol: None,
                    reason: "gate futures.obu missing result.s".to_string(),
                    needs_resubscribe: true,
                    needs_snapshot_refetch: false,
                };
            }
        };
        let sym = match Self::parse_obu_symbol(stream_name) {
            Some(s) => s,
            None => {
                self.overall_invalid = true;
                return FrameVerdict::Violation {
                    symbol: None,
                    reason: format!("gate futures.obu unexpected stream name {}", stream_name),
                    needs_resubscribe: true,
                    needs_snapshot_refetch: false,
                };
            }
        };

        let is_full = result.get("full").and_then(Value::as_bool).unwrap_or(false);
        if let Err(e) = crate::protocol::validate_book_sides(
            result.get("b"),
            result.get("a"),
            is_full,
            is_full,
            true,
            if is_full { "gate futures.obu snapshot" } else { "gate futures.obu" },
        ) {
            let st = self
                .obu_states
                .entry(sym.clone())
                .or_insert_with(GateObuState::new);
            st.state = ConnState::Invalid;
            st.prev_u = None;
            self.overall_invalid = true;
            return FrameVerdict::Violation {
                symbol: Some(sym),
                reason: e,
                needs_resubscribe: true,
                needs_snapshot_refetch: false,
            };
        }

        let u = match result.get("u").and_then(Value::as_u64) {
            Some(n) => n,
            None => {
                self.overall_invalid = true;
                return FrameVerdict::Violation {
                    symbol: Some(sym),
                    reason: "gate futures.obu missing integer u".to_string(),
                    needs_resubscribe: true,
                    needs_snapshot_refetch: false,
                };
            }
        };
        let st = self
            .obu_states
            .entry(sym.clone())
            .or_insert_with(GateObuState::new);

        if is_full {
            // Accept full=true initial or re-pushed snapshot even if it arrives before subscribe ACK.
            st.prev_u = Some(u);
            st.state = ConnState::Live;
            st.refreshing = false;
            if !self.fatal_invalid && self.obu_states.values().all(|s| s.state != ConnState::Invalid) {
                self.overall_invalid = false;
            }
            return FrameVerdict::AcceptData {
                symbol: sym,
                channel: "futures.obu".to_string(),
                is_anchor_reset: true,
                diagnostic: None,
            };
        }

        if st.refreshing {
            return FrameVerdict::BufferedWaitAnchor { symbol: sym };
        }
        if st.state != ConnState::Live {
            return FrameVerdict::Violation {
                symbol: Some(sym),
                reason: "gate futures.obu delta arrived before full=true snapshot".to_string(),
                needs_resubscribe: true,
                needs_snapshot_refetch: false,
            };
        }

        let first_u = match result.get("U").and_then(Value::as_u64) {
            Some(n) => n,
            None => {
                st.state = ConnState::Invalid;
                self.overall_invalid = true;
                return FrameVerdict::Violation {
                    symbol: Some(sym),
                    reason: "gate futures.obu incremental frame missing U".to_string(),
                    needs_resubscribe: true,
                    needs_snapshot_refetch: false,
                };
            }
        };

        let expected_first_u = st.prev_u.unwrap_or(0).saturating_add(1);
        if first_u == expected_first_u && u >= first_u {
            st.prev_u = Some(u);
            FrameVerdict::AcceptData {
                symbol: sym,
                channel: "futures.obu".to_string(),
                is_anchor_reset: false,
                diagnostic: None,
            }
        } else {
            let prev = st.prev_u;
            st.state = ConnState::Invalid;
            st.prev_u = None;
            self.overall_invalid = true;
            FrameVerdict::Violation {
                symbol: Some(sym),
                reason: format!(
                    "gate futures.obu sequence break: U={} u={} expected U=prev_u+1 (prev_u={:?})",
                    first_u, u, prev
                ),
                needs_resubscribe: true,
                needs_snapshot_refetch: false,
            }
        }
    }
}

impl ProtocolValidator for GateValidator {
    fn endpoint(&self) -> EndpointKind {
        EndpointKind::GatePublic
    }

    fn state(&self) -> ConnState {
        if self.overall_invalid {
            return ConnState::Invalid;
        }
        if self.expected_symbols.is_empty() {
            return ConnState::WaitAnchor;
        }
        if self.expected_symbols.iter().all(|s| {
            self.obu_states
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
                    reason: format!("invalid gate JSON: {}", e),
                    needs_resubscribe: true,
                    needs_snapshot_refetch: false,
                };
            }
        };

        if let Some(err) = v.get("error") {
            if !err.is_null() {
                self.overall_invalid = true;
                return FrameVerdict::Violation {
                    symbol: None,
                    reason: format!("gate server error: {}", err),
                    needs_resubscribe: true,
                    needs_snapshot_refetch: false,
                };
            }
        }

        let channel = v.get("channel").and_then(Value::as_str).unwrap_or("");
        let event = v.get("event").and_then(Value::as_str).unwrap_or("");

        if channel == "futures.pong" || channel == "futures.ping" {
            return FrameVerdict::Control {
                note: format!("gate heartbeat {}", channel),
            };
        }
        if event == "subscribe" || event == "unsubscribe" {
            if event == "subscribe"
                && v.pointer("/result/status").and_then(Value::as_str) == Some("success")
                && matches!(channel, "futures.obu" | "futures.book_ticker" | "futures.trades")
            {
                let mut matched_any = false;
                if let Some(arr) = v.get("payload").and_then(Value::as_array) {
                    for item in arr {
                        if let Some(tok) = item.as_str() {
                            let sym = if channel == "futures.obu" {
                                Self::parse_obu_symbol(tok)
                            } else {
                                Some(tok.to_uppercase())
                            };
                            if let Some(s) = sym {
                                self.pending_sub_acks.push((s, channel.to_string()));
                                matched_any = true;
                            }
                        }
                    }
                }
                if !matched_any {
                    for sym in &self.expected_symbols {
                        self.pending_sub_acks.push((sym.clone(), channel.to_string()));
                    }
                }
            }
            return FrameVerdict::Control {
                note: format!("gate {} ack for {}", event, channel),
            };
        }
        if event != "update" && event != "all" {
            return FrameVerdict::Control {
                note: format!("gate non-data event={} channel={}", event, channel),
            };
        }

        let result = match v.get("result") {
            Some(r) => r,
            None => {
                self.overall_invalid = true;
                return FrameVerdict::Violation {
                    symbol: None,
                    reason: format!("gate {} update missing result", channel),
                    needs_resubscribe: false,
                    needs_snapshot_refetch: false,
                };
            }
        };

        match channel {
            "futures.obu" => self.handle_obu(result),
            "futures.book_ticker" => {
                let sym = match result.get("s").and_then(Value::as_str) {
                    Some(s) => s.to_uppercase(),
                    None => {
                        return FrameVerdict::Violation {
                            symbol: None,
                            reason: "gate futures.book_ticker missing result.s".to_string(),
                            needs_resubscribe: false,
                            needs_snapshot_refetch: false,
                        }
                    }
                };
                if let (Some(bsz), Some(asz)) = (result.get("B"), result.get("A")) {
                    if !Self::check_decimal_size_string(bsz) || !Self::check_decimal_size_string(asz) {
                        self.overall_invalid = true;
                        return FrameVerdict::Violation {
                            symbol: Some(sym),
                            reason: format!(
                                "gate book_ticker B/A not decimal strings (missing X-Gate-Size-Decimal: 1?): B={} A={}",
                                bsz, asz
                            ),
                            needs_resubscribe: true,
                            needs_snapshot_refetch: false,
                        };
                    }
                }
                FrameVerdict::AcceptData {
                    symbol: sym,
                    channel: "futures.book_ticker".to_string(),
                    is_anchor_reset: false,
                    diagnostic: None,
                }
            }
            "futures.trades" => {
                let arr = match result.as_array() {
                    Some(a) => a,
                    None => {
                        return FrameVerdict::Violation {
                            symbol: None,
                            reason: "gate futures.trades result is not array".to_string(),
                            needs_resubscribe: false,
                            needs_snapshot_refetch: false,
                        }
                    }
                };
                let mut first_sym = String::new();
                let mut seen_syms = HashSet::new();
                let mut dups = 0usize;
                for item in arr {
                    let sym = match item.get("contract").and_then(Value::as_str) {
                        Some(s) => s.to_uppercase(),
                        None => {
                            return FrameVerdict::Violation {
                                symbol: None,
                                reason: "gate trade missing contract".to_string(),
                                needs_resubscribe: false,
                                needs_snapshot_refetch: false,
                            }
                        }
                    };
                    if first_sym.is_empty() {
                        first_sym = sym.clone();
                        seen_syms.insert(sym.clone());
                    } else if seen_syms.insert(sym.clone()) {
                        self.extra_accepted_streams
                            .push((sym.clone(), "futures.trades".to_string()));
                    }
                    let sz = match item.get("size") {
                        Some(s) => s,
                        None => {
                            return FrameVerdict::Violation {
                                symbol: Some(sym),
                                reason: "gate trade missing size".to_string(),
                                needs_resubscribe: false,
                                needs_snapshot_refetch: false,
                            }
                        }
                    };
                    if !Self::check_decimal_size_string(sz) {
                        self.overall_invalid = true;
                        return FrameVerdict::Violation {
                            symbol: Some(sym),
                            reason: format!(
                                "gate trade size is not a decimal string (X-Gate-Size-Decimal: 1 missing?): {}",
                                sz
                            ),
                            needs_resubscribe: true,
                            needs_snapshot_refetch: false,
                        };
                    }
                    let tid = match item.get("id").and_then(Value::as_u64) {
                        Some(id) => id,
                        None => {
                            return FrameVerdict::Violation {
                                symbol: Some(sym),
                                reason: "gate trade missing numeric id".to_string(),
                                needs_resubscribe: false,
                                needs_snapshot_refetch: false,
                            }
                        }
                    };
                    if self.track_trade_id(&sym, tid) {
                        dups += 1;
                    }
                }
                FrameVerdict::AcceptData {
                    symbol: first_sym,
                    channel: "futures.trades".to_string(),
                    is_anchor_reset: false,
                    diagnostic: if dups > 0 {
                        Some(format!("gate_duplicate_trade_ids_in_frame={}", dups))
                    } else {
                        None
                    },
                }
            }
            _ => FrameVerdict::Control {
                note: format!("gate unrecognized channel {}", channel),
            },
        }
    }

    fn take_subscription_acks(&mut self) -> Vec<(String, String)> {
        std::mem::take(&mut self.pending_sub_acks)
    }

    fn take_extra_accepted_streams(&mut self) -> Vec<(String, String)> {
        std::mem::take(&mut self.extra_accepted_streams)
    }

    fn expected_symbols(&self) -> Vec<String> {
        self.expected_symbols.clone()
    }

    fn symbol_depth_state(&self, symbol: &str) -> Option<ConnState> {
        self.obu_states
            .get(&symbol.to_uppercase())
            .map(|st| if st.refreshing { ConnState::Resync } else { st.state })
    }

    fn prepare_depth_refresh(&mut self) {
        self.reset();
        for state in self.obu_states.values_mut() {
            state.refreshing = true;
        }
    }

    fn prepare_symbol_depth_refresh(&mut self, symbol: &str) {
        if self.overall_invalid && self.obu_states.values().all(|s| s.state != ConnState::Invalid) {
            self.fatal_invalid = true;
        }
        let sym = symbol.to_uppercase();
        let st = self
            .obu_states
            .entry(sym)
            .or_insert_with(GateObuState::new);
        if st.state != ConnState::Invalid {
            st.state = ConnState::WaitAnchor;
        }
        st.prev_u = None;
        st.refreshing = true;
        self.overall_invalid =
            self.overall_invalid || self.obu_states.values().any(|s| s.state == ConnState::Invalid);
    }

    fn reset(&mut self) {
        self.overall_invalid = false;
        self.fatal_invalid = false;
        self.pending_sub_acks.clear();
        self.extra_accepted_streams.clear();
        for st in self.obu_states.values_mut() {
            *st = GateObuState::new();
        }
    }

    fn live_symbol_count(&self) -> usize {
        self.obu_states
            .values()
            .filter(|b| b.state == ConnState::Live)
            .count()
    }

    fn expected_symbol_count(&self) -> usize {
        self.expected_symbols.len()
    }
}
