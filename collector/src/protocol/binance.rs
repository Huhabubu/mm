use crate::protocol::{BufferedDepthFrame, FrameVerdict, ProtocolValidator, RestSnapshotBridge};
use crate::types::{ConnId, ConnState, EndpointKind, RawFrame};
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};

pub const DEFAULT_MAX_DEPTH_BUFFER_LEN: usize = 4096;

#[derive(Debug, Clone)]
struct DepthSymbolState {
    state: ConnState,
    prev_u: Option<u64>,
    pending_snapshot_id: Option<u64>,
    unavailable_since_wall_ns: Option<i64>,
    buffer_overflow_count: u64,
    buffer: VecDeque<BufferedDepthFrame>,
}

impl DepthSymbolState {
    fn new() -> Self {
        Self {
            state: ConnState::WaitAnchor,
            prev_u: None,
            pending_snapshot_id: None,
            unavailable_since_wall_ns: None,
            buffer_overflow_count: 0,
            buffer: VecDeque::new(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct FrameMeta {
    recv_wall_ns: i64,
    recv_mono_ns: u64,
    conn_id: ConnId,
    epoch: u64,
}

impl Default for FrameMeta {
    fn default() -> Self {
        Self {
            recv_wall_ns: 0,
            recv_mono_ns: 0,
            conn_id: ConnId::A,
            epoch: 1,
        }
    }
}

pub struct BinanceValidator {
    endpoint: EndpointKind,
    expected_symbols: Vec<String>,
    depth_states: HashMap<String, DepthSymbolState>,
    market_live_symbols: HashSet<String>,
    seen_agg_ids: HashMap<String, VecDeque<u64>>,
    seen_agg_set: HashMap<String, HashSet<u64>>,
    pending_sub_acks: Vec<(String, String)>,
    max_buffer_len: usize,
    overall_invalid: bool,
}

impl BinanceValidator {
    pub fn new_public(symbols: Vec<String>) -> Self {
        Self::new_public_with_buffer_cap(symbols, DEFAULT_MAX_DEPTH_BUFFER_LEN)
    }

    pub fn new_public_with_buffer_cap(symbols: Vec<String>, max_buffer_len: usize) -> Self {
        let upper: Vec<String> = symbols.into_iter().map(|s| s.to_uppercase()).collect();
        let mut depth_states = HashMap::new();
        for s in &upper {
            depth_states.insert(s.clone(), DepthSymbolState::new());
        }
        Self {
            endpoint: EndpointKind::BinancePublic,
            expected_symbols: upper,
            depth_states,
            market_live_symbols: HashSet::new(),
            seen_agg_ids: HashMap::new(),
            seen_agg_set: HashMap::new(),
            pending_sub_acks: Vec::new(),
            max_buffer_len: max_buffer_len.max(1),
            overall_invalid: false,
        }
    }

    pub fn new_market(symbols: Vec<String>) -> Self {
        let upper: Vec<String> = symbols.into_iter().map(|s| s.to_uppercase()).collect();
        Self {
            endpoint: EndpointKind::BinanceMarket,
            expected_symbols: upper,
            depth_states: HashMap::new(),
            market_live_symbols: HashSet::new(),
            seen_agg_ids: HashMap::new(),
            seen_agg_set: HashMap::new(),
            pending_sub_acks: Vec::new(),
            max_buffer_len: DEFAULT_MAX_DEPTH_BUFFER_LEN,
            overall_invalid: false,
        }
    }

    /// Build combined stream URL for Binance /public/stream (bookTicker + depth@100ms).
    pub fn build_public_stream_url(base_url: &str, symbols: &[String]) -> String {
        let mut streams = Vec::with_capacity(symbols.len() * 2);
        for s in symbols {
            let low = s.to_lowercase();
            streams.push(format!("{}@bookTicker", low));
            streams.push(format!("{}@depth@100ms", low));
        }
        format!(
            "{}?streams={}",
            base_url.trim_end_matches('?'),
            streams.join("/")
        )
    }

    /// Build combined stream URL for Binance /market/stream (aggTrade only).
    pub fn build_market_stream_url(base_url: &str, symbols: &[String]) -> String {
        let streams: Vec<String> = symbols
            .iter()
            .map(|s| format!("{}@aggTrade", s.to_lowercase()))
            .collect();
        format!(
            "{}?streams={}",
            base_url.trim_end_matches('?'),
            streams.join("/")
        )
    }

    fn record_agg_id(&mut self, sym: &str, agg_id: u64) -> bool {
        let q = self.seen_agg_ids.entry(sym.to_string()).or_default();
        let set = self.seen_agg_set.entry(sym.to_string()).or_default();
        if set.contains(&agg_id) {
            return true;
        }
        set.insert(agg_id);
        q.push_back(agg_id);
        if q.len() > 8192 {
            if let Some(old) = q.pop_front() {
                set.remove(&old);
            }
        }
        false
    }

    fn refresh_overall_invalid(&mut self) {
        self.overall_invalid = self
            .depth_states
            .values()
            .any(|d| d.state == ConnState::Invalid);
    }

    fn parse_binance_stream_token(token: &str) -> Option<(String, String)> {
        let (sym_raw, chan_raw) = token.split_once('@')?;
        let sym = sym_raw.trim().to_uppercase();
        let chan = chan_raw.trim().to_string();
        if sym.is_empty() || chan.is_empty() {
            return None;
        }
        Some((sym, chan))
    }

    fn handle_depth_update(&mut self, data: &Value, raw: &str, meta: FrameMeta) -> FrameVerdict {
        let sym = match data.get("s").and_then(Value::as_str) {
            Some(s) => s.to_uppercase(),
            None => {
                self.overall_invalid = true;
                return FrameVerdict::Violation {
                    symbol: None,
                    reason: "binance depthUpdate missing symbol 's'".to_string(),
                    needs_resubscribe: false,
                    needs_snapshot_refetch: true,
                };
            }
        };
        let first_u = data.get("U").and_then(Value::as_u64);
        let last_u = data.get("u").and_then(Value::as_u64);
        let prev_u = data.get("pu").and_then(Value::as_u64);
        let (first_u, last_u, prev_u) = match (first_u, last_u, prev_u) {
            (Some(fu), Some(lu), Some(pu)) => (fu, lu, pu),
            _ => {
                self.overall_invalid = true;
                return FrameVerdict::Violation {
                    symbol: Some(sym),
                    reason: "binance depthUpdate missing U/u/pu integers".to_string(),
                    needs_resubscribe: false,
                    needs_snapshot_refetch: true,
                };
            }
        };

        let buffered_frame = BufferedDepthFrame {
            first_u,
            last_u,
            prev_u,
            recv_wall_ns: meta.recv_wall_ns,
            recv_mono_ns: meta.recv_mono_ns,
            conn_id: meta.conn_id,
            epoch: meta.epoch,
            payload: raw.to_string(),
        };

        let max_len = self.max_buffer_len;
        let st = self
            .depth_states
            .entry(sym.clone())
            .or_insert_with(DepthSymbolState::new);

        match st.state {
            ConnState::WaitAnchor | ConnState::Resync => {
                if let Some(snap_id) = st.pending_snapshot_id {
                    if last_u < snap_id {
                        return FrameVerdict::BufferedWaitAnchor { symbol: sym };
                    }
                    if first_u <= snap_id && snap_id <= last_u {
                        st.prev_u = Some(last_u);
                        st.pending_snapshot_id = None;
                        st.unavailable_since_wall_ns = None;
                        st.state = ConnState::Live;
                        st.buffer.clear();
                        self.refresh_overall_invalid();
                        return FrameVerdict::AcceptData {
                            symbol: sym,
                            channel: "depth@100ms".to_string(),
                            is_anchor_reset: true,
                            diagnostic: None,
                        };
                    } else {
                        // Snapshot is strictly older than earliest available WS delta -> refetch!
                        st.pending_snapshot_id = None;
                        st.state = ConnState::Resync;
                        if meta.recv_wall_ns > 0 {
                            st.unavailable_since_wall_ns.get_or_insert(meta.recv_wall_ns);
                        }
                        if st.buffer.len() >= max_len {
                            st.buffer.clear();
                            st.buffer_overflow_count += 1;
                        }
                        st.buffer.push_back(buffered_frame);
                        return FrameVerdict::Violation {
                            symbol: Some(sym),
                            reason: format!(
                                "binance depth bridge failed: U={} > snapshot S={}",
                                first_u, snap_id
                            ),
                            needs_resubscribe: false,
                            needs_snapshot_refetch: true,
                        };
                    }
                }
                if st.buffer.len() >= max_len {
                    // Buffer overflow while awaiting REST snapshot: clear stale prefix, mark Resync & unavailable interval.
                    st.buffer_overflow_count += 1;
                    if meta.recv_wall_ns > 0 {
                        st.unavailable_since_wall_ns.get_or_insert(meta.recv_wall_ns);
                    }
                    st.buffer.clear();
                    st.pending_snapshot_id = None;
                    st.state = ConnState::Resync;
                    st.buffer.push_back(buffered_frame);
                    return FrameVerdict::Violation {
                        symbol: Some(sym.clone()),
                        reason: format!(
                            "binance depth buffer overflow for {} (capacity={}); resync required",
                            sym, max_len
                        ),
                        needs_resubscribe: false,
                        needs_snapshot_refetch: true,
                    };
                }
                if st.state == ConnState::WaitAnchor && st.unavailable_since_wall_ns.is_none() && meta.recv_wall_ns > 0 {
                    st.unavailable_since_wall_ns = Some(meta.recv_wall_ns);
                }
                st.buffer.push_back(buffered_frame);
                FrameVerdict::BufferedWaitAnchor { symbol: sym }
            }
            ConnState::Live => {
                if st.prev_u == Some(prev_u) {
                    st.prev_u = Some(last_u);
                    FrameVerdict::AcceptData {
                        symbol: sym,
                        channel: "depth@100ms".to_string(),
                        is_anchor_reset: false,
                        diagnostic: None,
                    }
                } else {
                    let expected = st.prev_u;
                    st.state = ConnState::Invalid;
                    st.prev_u = None;
                    st.pending_snapshot_id = None;
                    if meta.recv_wall_ns > 0 {
                        st.unavailable_since_wall_ns = Some(meta.recv_wall_ns);
                    }
                    st.buffer.clear();
                    st.buffer.push_back(buffered_frame);
                    self.overall_invalid = true;
                    FrameVerdict::Violation {
                        symbol: Some(sym),
                        reason: format!(
                            "binance pu mismatch: got pu={}, expected prev_u={:?}",
                            prev_u, expected
                        ),
                        needs_resubscribe: false,
                        needs_snapshot_refetch: true,
                    }
                }
            }
            ConnState::Invalid => {
                // Keep buffering incoming WS deltas for this symbol while awaiting snapshot resync
                // so that once the scheduled REST snapshot arrives, bridging succeeds cleanly without
                // spamming a Violation on every 100ms frame.
                if st.buffer.len() >= max_len {
                    st.buffer_overflow_count += 1;
                    st.buffer.clear();
                }
                st.buffer.push_back(buffered_frame);
                FrameVerdict::BufferedWaitAnchor { symbol: sym }
            }
        }
    }

    fn process_frame(&mut self, raw: &str, meta: FrameMeta) -> FrameVerdict {
        let v: Value = match serde_json::from_str(raw) {
            Ok(v) => v,
            Err(e) => {
                self.overall_invalid = true;
                return FrameVerdict::Violation {
                    symbol: None,
                    reason: format!("invalid binance JSON: {}", e),
                    needs_resubscribe: true,
                    needs_snapshot_refetch: false,
                };
            }
        };

        // Combined stream wrapper {"stream": "...", "data": {...}}
        let (stream_name, data) = if let Some(d) = v.get("data") {
            (v.get("stream").and_then(Value::as_str).unwrap_or(""), d)
        } else if v.get("result").is_some() && v.get("id").is_some() {
            if let Some(arr) = v.get("result").and_then(Value::as_array) {
                for item in arr {
                    if let Some(tok) = item.as_str() {
                        if let Some((sym, chan)) = Self::parse_binance_stream_token(tok) {
                            self.pending_sub_acks.push((sym, chan));
                        }
                    }
                }
            }
            return FrameVerdict::Control {
                note: "binance subscribe ack".to_string(),
            };
        } else {
            ("", &v)
        };

        let event_type = data.get("e").and_then(Value::as_str).unwrap_or("");
        if event_type == "depthUpdate" || stream_name.contains("@depth") {
            if self.endpoint != EndpointKind::BinancePublic {
                return FrameVerdict::Violation {
                    symbol: None,
                    reason: "depthUpdate received on non-public Binance endpoint".to_string(),
                    needs_resubscribe: false,
                    needs_snapshot_refetch: false,
                };
            }
            return self.handle_depth_update(data, raw, meta);
        }

        if event_type == "bookTicker" || stream_name.contains("@bookTicker") {
            if self.endpoint != EndpointKind::BinancePublic {
                return FrameVerdict::Violation {
                    symbol: None,
                    reason: "bookTicker received on non-public Binance endpoint".to_string(),
                    needs_resubscribe: false,
                    needs_snapshot_refetch: false,
                };
            }
            let sym = match data.get("s").and_then(Value::as_str) {
                Some(s) => s.to_uppercase(),
                None => {
                    return FrameVerdict::Violation {
                        symbol: None,
                        reason: "binance bookTicker missing symbol 's'".to_string(),
                        needs_resubscribe: false,
                        needs_snapshot_refetch: false,
                    }
                }
            };
            if data.get("b").is_none() || data.get("a").is_none() {
                return FrameVerdict::Violation {
                    symbol: Some(sym),
                    reason: "binance bookTicker missing bid/ask price".to_string(),
                    needs_resubscribe: false,
                    needs_snapshot_refetch: false,
                };
            }
            return FrameVerdict::AcceptData {
                symbol: sym,
                channel: "bookTicker".to_string(),
                is_anchor_reset: false,
                diagnostic: None,
            };
        }

        if event_type == "aggTrade" || stream_name.contains("@aggTrade") {
            if self.endpoint != EndpointKind::BinanceMarket {
                self.overall_invalid = true;
                return FrameVerdict::Violation {
                    symbol: None,
                    reason: "aggTrade MUST NOT arrive on /public/stream".to_string(),
                    needs_resubscribe: false,
                    needs_snapshot_refetch: false,
                };
            }
            let sym = match data.get("s").and_then(Value::as_str) {
                Some(s) => s.to_uppercase(),
                None => {
                    return FrameVerdict::Violation {
                        symbol: None,
                        reason: "binance aggTrade missing symbol 's'".to_string(),
                        needs_resubscribe: false,
                        needs_snapshot_refetch: false,
                    }
                }
            };
            let agg_id = match data.get("a").and_then(Value::as_u64) {
                Some(id) => id,
                None => {
                    return FrameVerdict::Violation {
                        symbol: Some(sym),
                        reason: "binance aggTrade missing aggregate id 'a'".to_string(),
                        needs_resubscribe: false,
                        needs_snapshot_refetch: false,
                    }
                }
            };
            if data.get("f").is_none() || data.get("l").is_none() {
                return FrameVerdict::Violation {
                    symbol: Some(sym),
                    reason: "binance aggTrade missing f/l first/last trade ids".to_string(),
                    needs_resubscribe: false,
                    needs_snapshot_refetch: false,
                };
            }
            self.market_live_symbols.insert(sym.clone());
            let dup = self.record_agg_id(&sym, agg_id);
            return FrameVerdict::AcceptData {
                symbol: sym,
                channel: "aggTrade".to_string(),
                is_anchor_reset: false,
                diagnostic: if dup {
                    Some(format!("duplicate binance aggTrade id={}", agg_id))
                } else {
                    None
                },
            };
        }

        FrameVerdict::Control {
            note: format!("unhandled binance control frame: {}", event_type),
        }
    }
}

impl ProtocolValidator for BinanceValidator {
    fn endpoint(&self) -> EndpointKind {
        self.endpoint
    }

    fn state(&self) -> ConnState {
        if self.overall_invalid {
            return ConnState::Invalid;
        }
        match self.endpoint {
            EndpointKind::BinancePublic => {
                if self.expected_symbols.is_empty() {
                    return ConnState::WaitAnchor;
                }
                if self.expected_symbols.iter().all(|s| {
                    self.depth_states
                        .get(s)
                        .map(|d| d.state == ConnState::Live)
                        .unwrap_or(false)
                }) {
                    ConnState::Live
                } else if self
                    .depth_states
                    .values()
                    .any(|d| d.state == ConnState::Invalid)
                {
                    ConnState::Invalid
                } else if self
                    .depth_states
                    .values()
                    .any(|d| d.state == ConnState::Resync)
                {
                    ConnState::Resync
                } else {
                    ConnState::WaitAnchor
                }
            }
            EndpointKind::BinanceMarket => {
                if !self.expected_symbols.is_empty()
                    && self
                        .expected_symbols
                        .iter()
                        .all(|s| self.market_live_symbols.contains(s))
                {
                    ConnState::Live
                } else {
                    ConnState::WaitAnchor
                }
            }
            _ => ConnState::Invalid,
        }
    }

    fn on_text_frame(&mut self, raw: &str) -> FrameVerdict {
        self.process_frame(raw, FrameMeta::default())
    }

    fn on_frame_with_meta(&mut self, frame: &RawFrame) -> FrameVerdict {
        self.process_frame(
            &frame.payload,
            FrameMeta {
                recv_wall_ns: frame.recv_wall_ns,
                recv_mono_ns: frame.recv_mono_ns,
                conn_id: frame.conn_id,
                epoch: frame.epoch,
            },
        )
    }

    fn on_rest_snapshot(&mut self, symbol: &str, last_update_id: u64) -> Result<usize, String> {
        self.on_rest_snapshot_detailed(symbol, last_update_id)
            .map(|res| res.bridged_frames.len())
    }

    fn on_rest_snapshot_detailed(
        &mut self,
        symbol: &str,
        last_update_id: u64,
    ) -> Result<RestSnapshotBridge, String> {
        let sym = symbol.to_uppercase();
        let st = self
            .depth_states
            .entry(sym.clone())
            .or_insert_with(DepthSymbolState::new);

        // Drop all buffered events with u < S
        let mut dropped_before_snapshot = 0usize;
        while let Some(front) = st.buffer.front() {
            if front.last_u < last_update_id {
                st.buffer.pop_front();
                dropped_before_snapshot += 1;
            } else {
                break;
            }
        }

        if st.buffer.is_empty() {
            // Snapshot arrived ahead of matching WS delta: wait for first WS frame with U <= S <= u
            st.pending_snapshot_id = Some(last_update_id);
            st.state = ConnState::WaitAnchor;
            let unavail = st.unavailable_since_wall_ns;
            self.refresh_overall_invalid();
            return Ok(RestSnapshotBridge {
                bridged_frames: Vec::new(),
                awaiting_first_ws_delta: true,
                unavailable_since_wall_ns: unavail,
                dropped_before_snapshot,
            });
        }

        let first = st.buffer.pop_front().unwrap();
        if !(first.first_u <= last_update_id && last_update_id <= first.last_u) {
            // Snapshot is stale (S < first.U): must refetch!
            let (first_u, last_u, first_wall) = (first.first_u, first.last_u, first.recv_wall_ns);
            st.buffer.push_front(first);
            st.state = ConnState::Resync;
            st.pending_snapshot_id = None;
            if first_wall > 0 {
                st.unavailable_since_wall_ns.get_or_insert(first_wall);
            }
            self.refresh_overall_invalid();
            return Err(format!(
                "binance REST snapshot stale for {}: S={} not in [U={}, u={}]",
                sym, last_update_id, first_u, last_u
            ));
        }

        let mut cur_u = first.last_u;
        let mut bridged_frames = Vec::with_capacity(st.buffer.len() + 1);
        bridged_frames.push(first);
        while let Some(next) = st.buffer.pop_front() {
            if next.prev_u != cur_u {
                let next_pu = next.prev_u;
                let next_wall = next.recv_wall_ns;
                st.state = ConnState::Resync;
                st.prev_u = None;
                st.pending_snapshot_id = None;
                st.buffer.clear();
                if next_wall > 0 {
                    st.unavailable_since_wall_ns.get_or_insert(next_wall);
                }
                self.refresh_overall_invalid();
                return Err(format!(
                    "binance buffered depth gap during bridge for {}: pu={} != prev_u={}",
                    sym, next_pu, cur_u
                ));
            }
            cur_u = next.last_u;
            bridged_frames.push(next);
        }

        st.prev_u = Some(cur_u);
        st.pending_snapshot_id = None;
        st.state = ConnState::Live;
        let unavailable_since_wall_ns = st.unavailable_since_wall_ns.take();
        self.refresh_overall_invalid();
        Ok(RestSnapshotBridge {
            bridged_frames,
            awaiting_first_ws_delta: false,
            unavailable_since_wall_ns,
            dropped_before_snapshot,
        })
    }

    fn take_subscription_acks(&mut self) -> Vec<(String, String)> {
        std::mem::take(&mut self.pending_sub_acks)
    }

    fn expected_symbols(&self) -> Vec<String> {
        self.expected_symbols.clone()
    }

    fn symbol_depth_state(&self, symbol: &str) -> Option<ConnState> {
        self.depth_states.get(&symbol.to_uppercase()).map(|s| s.state)
    }

    fn prepare_depth_refresh(&mut self) {
        self.overall_invalid = false;
        for st in self.depth_states.values_mut() {
            st.state = ConnState::Resync;
            st.prev_u = None;
            st.pending_snapshot_id = None;
            st.buffer.clear();
        }
    }

    fn prepare_symbol_depth_refresh(&mut self, symbol: &str) {
        let sym = symbol.to_uppercase();
        if let Some(st) = self.depth_states.get_mut(&sym) {
            st.state = ConnState::Resync;
            st.prev_u = None;
            st.pending_snapshot_id = None;
            st.buffer.clear();
        }
        self.refresh_overall_invalid();
    }

    fn reset(&mut self) {
        self.overall_invalid = false;
        self.market_live_symbols.clear();
        self.pending_sub_acks.clear();
        for st in self.depth_states.values_mut() {
            *st = DepthSymbolState::new();
        }
    }

    fn live_symbol_count(&self) -> usize {
        match self.endpoint {
            EndpointKind::BinancePublic => self
                .depth_states
                .values()
                .filter(|d| d.state == ConnState::Live)
                .count(),
            EndpointKind::BinanceMarket => self.market_live_symbols.len(),
            _ => 0,
        }
    }

    fn expected_symbol_count(&self) -> usize {
        self.expected_symbols.len()
    }
}
