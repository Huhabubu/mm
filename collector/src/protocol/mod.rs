pub mod binance;
pub mod bitget;
pub mod bybit;
pub mod gate;

use crate::types::{ConnId, ConnState, EndpointKind, RawFrame};
use serde::{Deserialize, Serialize};

/// Preserved raw WS depth frame buffered while awaiting a REST snapshot anchor (Binance).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BufferedDepthFrame {
    pub first_u: u64,
    pub last_u: u64,
    pub prev_u: u64,
    pub recv_wall_ns: i64,
    pub recv_mono_ns: u64,
    pub conn_id: ConnId,
    pub epoch: u64,
    pub payload: String,
}

/// Detailed result of applying a REST depth snapshot against buffered WS depth deltas.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RestSnapshotBridge {
    /// Ordered buffered WS depth deltas required to bridge the REST snapshot to current live state.
    pub bridged_frames: Vec<BufferedDepthFrame>,
    /// True if snapshot arrived ahead of the first bridging WS delta (still awaiting `U <= S <= u`).
    pub awaiting_first_ws_delta: bool,
    /// Start timestamp (wall ns) of the unavailable depth interval that this snapshot resolved, if any.
    pub unavailable_since_wall_ns: Option<i64>,
    /// Number of buffered deltas with `u < lastUpdateId` discarded per Binance spec.
    pub dropped_before_snapshot: usize,
}

/// Outcome of feeding a single raw WS frame into a connection's independent protocol validator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FrameVerdict {
    /// Control frame (ACK, pong, ping reply) - does not write to DATA stream.
    Control {
        note: String,
    },
    /// Valid market data frame accepted for canonical emission when this connection is primary.
    AcceptData {
        symbol: String,
        channel: String,
        /// True if this frame established or reset a clean full-book anchor.
        is_anchor_reset: bool,
        /// Optional diagnostic warning (e.g. duplicate trade id observed; raw frame still preserved).
        diagnostic: Option<String>,
    },
    /// Depth delta buffered while waiting for REST snapshot anchor (Binance only).
    BufferedWaitAnchor {
        symbol: String,
    },
    /// Protocol or sequence continuity violation - connection becomes INVALID for that stream/endpoint.
    Violation {
        symbol: Option<String>,
        reason: String,
        needs_resubscribe: bool,
        needs_snapshot_refetch: bool,
    },
}

pub trait ProtocolValidator: Send {
    fn endpoint(&self) -> EndpointKind;
    fn state(&self) -> ConnState;
    fn on_text_frame(&mut self, raw: &str) -> FrameVerdict;

    /// Validate a raw frame with original receive timestamps, connection ID, and epoch preserved.
    fn on_frame_with_meta(&mut self, frame: &RawFrame) -> FrameVerdict {
        self.on_text_frame(&frame.payload)
    }

    /// Apply a REST depth snapshot anchor (used by Binance /public).
    fn on_rest_snapshot(&mut self, _symbol: &str, _last_update_id: u64) -> Result<usize, String> {
        Ok(0)
    }

    /// Detailed REST depth snapshot bridge returning preserved raw buffered deltas in sequence order.
    fn on_rest_snapshot_detailed(
        &mut self,
        symbol: &str,
        last_update_id: u64,
    ) -> Result<RestSnapshotBridge, String> {
        let _ = self.on_rest_snapshot(symbol, last_update_id)?;
        Ok(RestSnapshotBridge::default())
    }

    /// Drain any explicit `(symbol, channel)` subscription ACKs observed on the latest control frame.
    fn take_subscription_acks(&mut self) -> Vec<(String, String)> {
        Vec::new()
    }

    /// Drain any additional `(symbol, channel)` streams observed in a multi-symbol batch frame.
    fn take_extra_accepted_streams(&mut self) -> Vec<(String, String)> {
        Vec::new()
    }

    /// Expected configured symbols for this endpoint.
    fn expected_symbols(&self) -> Vec<String> {
        Vec::new()
    }

    /// Optional per-symbol depth anchor state (Binance).
    fn symbol_depth_state(&self, _symbol: &str) -> Option<ConnState> {
        None
    }

    /// Invalidate depth anchors before a controlled refresh on the same connection.
    fn prepare_depth_refresh(&mut self) {}
    /// Invalidate a single symbol's depth anchor before fetching a fresh same-connection snapshot.
    fn prepare_symbol_depth_refresh(&mut self, _symbol: &str) {}
    /// Reset validator state on reconnect / fresh epoch.
    fn reset(&mut self);
    /// Return symbols currently in LIVE state.
    fn live_symbol_count(&self) -> usize;
    fn expected_symbol_count(&self) -> usize;
}

pub fn make_validator(endpoint: EndpointKind, symbols: Vec<String>) -> Box<dyn ProtocolValidator> {
    match endpoint {
        EndpointKind::BinancePublic => Box::new(binance::BinanceValidator::new_public(symbols)),
        EndpointKind::BinanceMarket => Box::new(binance::BinanceValidator::new_market(symbols)),
        EndpointKind::BybitPublic => Box::new(bybit::BybitValidator::new(symbols)),
        EndpointKind::BitgetPublic => Box::new(bitget::BitgetValidator::new(symbols)),
        EndpointKind::GatePublic => Box::new(gate::GateValidator::new(symbols)),
    }
}
