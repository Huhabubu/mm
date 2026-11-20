pub mod binance;
pub mod bitget;
pub mod bybit;
pub mod gate;

use crate::types::{ConnState, EndpointKind};
use serde::{Deserialize, Serialize};

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
    /// Apply a REST depth snapshot anchor (used by Binance /public).
    fn on_rest_snapshot(&mut self, _symbol: &str, _last_update_id: u64) -> Result<usize, String> {
        Ok(0)
    }
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
