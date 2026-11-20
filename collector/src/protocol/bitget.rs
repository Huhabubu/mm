use crate::protocol::{FrameVerdict, ProtocolValidator};
use crate::types::{ConnState, EndpointKind};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Debug, Clone)]
struct BitgetBookState {
    state: ConnState,
    seq_snapshot: Option<u64>,
    prev_seq: Option<u64>,
    awaiting_first_delta: bool,
}

impl BitgetBookState {
    fn new() -> Self {
        Self {
            state: ConnState::WaitAnchor,
            seq_snapshot: None,
            prev_seq: None,
            awaiting_first_delta: false,
        }
    }
}

pub struct BitgetValidator {
    expected_symbols: Vec<String>,
    books50_states: HashMap<String, BitgetBookState>,
    books1_states: HashMap<String, BitgetBookState>,
    seen_trade_ids: HashMap<String, VecDeque<String>>,
    seen_trade_set: HashMap<String, HashSet<String>>,
    overall_invalid: bool,
}

impl BitgetValidator {
    pub fn new(symbols: Vec<String>) -> Self {
        let upper: Vec<String> = symbols.into_iter().map(|s| s.to_uppercase()).collect();
        let mut books50_states = HashMap::new();
        let mut books1_states = HashMap::new();
        for s in &upper {
            books50_states.insert(s.clone(), BitgetBookState::new());
            books1_states.insert(s.clone(), BitgetBookState::new());
        }
        Self {
            expected_symbols: upper,
            books50_states,
            books1_states,
            seen_trade_ids: HashMap::new(),
            seen_trade_set: HashMap::new(),
            overall_invalid: false,
        }
    }

    pub fn build_subscribe_message(symbols: &[String]) -> Value {
        let mut args = Vec::with_capacity(symbols.len() * 3);
        for s in symbols {
            let sym = s.to_uppercase();
            for topic in ["books50", "books1", "publicTrade"] {
                args.push(json!({
                    "instType": "usdt-futures",
                    "topic": topic,
                    "symbol": sym
                }));
            }
        }
        json!({"op": "subscribe", "args": args})
    }

    pub fn build_ping_text() -> &'static str {
        "ping"
    }

    fn parse_u64_flex(v: Option<&Value>) -> Option<u64> {
        let val = v?;
        if let Some(n) = val.as_u64() {
            Some(n)
        } else if let Some(s) = val.as_str() {
            s.parse::<u64>().ok()
        } else {
            None
        }
    }

    fn track_trade_id(&mut self, sym: &str, tid: &str) -> bool {
        let q = self.seen_trade_ids.entry(sym.to_string()).or_default();
        let set = self.seen_trade_set.entry(sym.to_string()).or_default();
        if set.contains(tid) {
            return true;
        }
        set.insert(tid.to_string());
        q.push_back(tid.to_string());
        if q.len() > 16384 {
            if let Some(old) = q.pop_front() {
                set.remove(&old);
            }
        }
        false
    }

    fn handle_book(&mut self, topic: &str, sym: String, action: &str, data: &[Value]) -> FrameVerdict {
        let item = match data.first() {
            Some(x) => x,
            None => {
                self.overall_invalid = true;
                return FrameVerdict::Violation {
                    symbol: Some(sym),
                    reason: format!("bitget {} empty data array", topic),
                    needs_resubscribe: true,
                    needs_snapshot_refetch: false,
                };
            }
        };
        let seq = match Self::parse_u64_flex(item.get("seq")) {
            Some(n) => n,
            None => {
                self.overall_invalid = true;
                return FrameVerdict::Violation {
                    symbol: Some(sym),
                    reason: format!("bitget {} missing seq", topic),
                    needs_resubscribe: true,
                    needs_snapshot_refetch: false,
                };
            }
        };
        let pseq = Self::parse_u64_flex(item.get("pseq"));

        let map = if topic == "books1" {
            &mut self.books1_states
        } else {
            &mut self.books50_states
        };
        let st = map.entry(sym.clone()).or_insert_with(BitgetBookState::new);

        // Dynamic per-frame action branching: supports both pre-2026-12-01 (all snapshot)
        // and post-2026-12-01 (initial snapshot + incremental update with pseq/seq).
        if action == "snapshot" {
            st.seq_snapshot = Some(seq);
            st.prev_seq = Some(seq);
            st.awaiting_first_delta = true;
            st.state = ConnState::Live;
            return FrameVerdict::AcceptData {
                symbol: sym,
                channel: topic.to_string(),
                is_anchor_reset: true,
                diagnostic: None,
            };
        }

        if action == "update" {
            if st.state != ConnState::Live {
                return FrameVerdict::Violation {
                    symbol: Some(sym),
                    reason: format!("bitget {} update arrived before snapshot anchor", topic),
                    needs_resubscribe: true,
                    needs_snapshot_refetch: false,
                };
            }
            let pseq_val = match pseq {
                Some(p) => p,
                None => {
                    st.state = ConnState::Invalid;
                    self.overall_invalid = true;
                    return FrameVerdict::Violation {
                        symbol: Some(sym),
                        reason: format!("bitget {} update missing pseq", topic),
                        needs_resubscribe: true,
                        needs_snapshot_refetch: false,
                    };
                }
            };
            // Standalone pseq == 0 cannot bridge a missing frame when snapshot/prev seq > 0.
            if pseq_val == 0 && st.prev_seq.unwrap_or(0) > 0 {
                st.state = ConnState::Invalid;
                st.prev_seq = None;
                st.seq_snapshot = None;
                self.overall_invalid = true;
                return FrameVerdict::Violation {
                    symbol: Some(sym),
                    reason: format!(
                        "bitget {} standalone pseq=0 cannot bridge prev_seq={:?}",
                        topic, st.prev_seq
                    ),
                    needs_resubscribe: true,
                    needs_snapshot_refetch: false,
                };
            }

            if st.awaiting_first_delta {
                let s_snap = st.seq_snapshot.unwrap_or(0);
                if pseq_val <= s_snap && s_snap <= seq {
                    st.awaiting_first_delta = false;
                    st.prev_seq = Some(seq);
                    return FrameVerdict::AcceptData {
                        symbol: sym,
                        channel: topic.to_string(),
                        is_anchor_reset: false,
                        diagnostic: None,
                    };
                } else {
                    st.state = ConnState::Invalid;
                    st.prev_seq = None;
                    st.seq_snapshot = None;
                    self.overall_invalid = true;
                    return FrameVerdict::Violation {
                        symbol: Some(sym),
                        reason: format!(
                            "bitget {} first update failed bridge: pseq={} seq_snap={} seq={}",
                            topic, pseq_val, s_snap, seq
                        ),
                        needs_resubscribe: true,
                        needs_snapshot_refetch: false,
                    };
                }
            } else if st.prev_seq == Some(pseq_val) {
                st.prev_seq = Some(seq);
                return FrameVerdict::AcceptData {
                    symbol: sym,
                    channel: topic.to_string(),
                    is_anchor_reset: false,
                    diagnostic: None,
                };
            } else {
                let expected = st.prev_seq;
                st.state = ConnState::Invalid;
                st.prev_seq = None;
                st.seq_snapshot = None;
                self.overall_invalid = true;
                return FrameVerdict::Violation {
                    symbol: Some(sym),
                    reason: format!(
                        "bitget {} pseq gap: got pseq={}, expected prev_seq={:?}",
                        topic, pseq_val, expected
                    ),
                    needs_resubscribe: true,
                    needs_snapshot_refetch: false,
                };
            }
        }

        self.overall_invalid = true;
        FrameVerdict::Violation {
            symbol: Some(sym),
            reason: format!("bitget {} unknown action={}", topic, action),
            needs_resubscribe: true,
            needs_snapshot_refetch: false,
        }
    }
}

impl ProtocolValidator for BitgetValidator {
    fn endpoint(&self) -> EndpointKind {
        EndpointKind::BitgetPublic
    }

    fn state(&self) -> ConnState {
        if self.overall_invalid {
            return ConnState::Invalid;
        }
        if self.expected_symbols.is_empty() {
            return ConnState::WaitAnchor;
        }
        if self.expected_symbols.iter().all(|s| {
            self.books50_states
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
        if raw.trim() == "pong" {
            return FrameVerdict::Control {
                note: "bitget pong".to_string(),
            };
        }
        let v: Value = match serde_json::from_str(raw) {
            Ok(v) => v,
            Err(e) => {
                self.overall_invalid = true;
                return FrameVerdict::Violation {
                    symbol: None,
                    reason: format!("invalid bitget JSON: {}", e),
                    needs_resubscribe: true,
                    needs_snapshot_refetch: false,
                };
            }
        };

        if let Some(ev) = v.get("event").and_then(Value::as_str) {
            if ev == "subscribe" {
                return FrameVerdict::Control {
                    note: "bitget subscribe ack".to_string(),
                };
            }
            if ev == "error" {
                self.overall_invalid = true;
                return FrameVerdict::Violation {
                    symbol: None,
                    reason: format!("bitget error frame: {}", raw),
                    needs_resubscribe: true,
                    needs_snapshot_refetch: false,
                };
            }
            return FrameVerdict::Control {
                note: format!("bitget event={}", ev),
            };
        }

        let arg = match v.get("arg") {
            Some(a) => a,
            None => {
                return FrameVerdict::Control {
                    note: "bitget non-arg control frame".to_string(),
                }
            }
        };
        let topic = arg
            .get("topic")
            .or_else(|| arg.get("channel"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let sym = arg
            .get("symbol")
            .or_else(|| arg.get("instId"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_uppercase();
        let action = v.get("action").and_then(Value::as_str).unwrap_or("");
        let data = match v.get("data").and_then(Value::as_array) {
            Some(d) => d,
            None => {
                return FrameVerdict::Violation {
                    symbol: if sym.is_empty() { None } else { Some(sym) },
                    reason: format!("bitget {} missing data array", topic),
                    needs_resubscribe: false,
                    needs_snapshot_refetch: false,
                }
            }
        };

        if topic == "books50" || topic == "books1" {
            return self.handle_book(topic, sym, action, data);
        }

        if topic == "publicTrade" {
            let mut dups = 0usize;
            for item in data {
                let tid = match item.get("i").and_then(|x| {
                    x.as_str()
                        .map(String::from)
                        .or_else(|| x.as_u64().map(|n| n.to_string()))
                }) {
                    Some(id) => id,
                    None => {
                        return FrameVerdict::Violation {
                            symbol: Some(sym),
                            reason: "bitget publicTrade item missing trade id 'i'".to_string(),
                            needs_resubscribe: false,
                            needs_snapshot_refetch: false,
                        }
                    }
                };
                if self.track_trade_id(&sym, &tid) {
                    dups += 1;
                }
            }
            return FrameVerdict::AcceptData {
                symbol: sym,
                channel: "publicTrade".to_string(),
                is_anchor_reset: false,
                diagnostic: if dups > 0 {
                    Some(format!("bitget_duplicate_trade_ids_in_frame={}", dups))
                } else {
                    None
                },
            };
        }

        FrameVerdict::Control {
            note: format!("bitget unrecognized topic={}", topic),
        }
    }

    fn reset(&mut self) {
        self.overall_invalid = false;
        for st in self.books50_states.values_mut() {
            *st = BitgetBookState::new();
        }
        for st in self.books1_states.values_mut() {
            *st = BitgetBookState::new();
        }
    }

    fn live_symbol_count(&self) -> usize {
        self.books50_states
            .values()
            .filter(|b| b.state == ConnState::Live)
            .count()
    }

    fn expected_symbol_count(&self) -> usize {
        self.expected_symbols.len()
    }
}
