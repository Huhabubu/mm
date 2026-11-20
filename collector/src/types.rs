use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExchangeId {
    BinanceFutures,
    BybitLinear,
    BitgetV3,
    GateFutures,
}

impl ExchangeId {
    pub fn as_dir_name(self) -> &'static str {
        match self {
            ExchangeId::BinanceFutures => "binance_futures",
            ExchangeId::BybitLinear => "bybit_linear",
            ExchangeId::BitgetV3 => "bitget_v3",
            ExchangeId::GateFutures => "gate_futures",
        }
    }

    pub fn from_dir_name(s: &str) -> Option<Self> {
        match s {
            "binance_futures" => Some(ExchangeId::BinanceFutures),
            "bybit_linear" => Some(ExchangeId::BybitLinear),
            "bitget_v3" => Some(ExchangeId::BitgetV3),
            "gate_futures" => Some(ExchangeId::GateFutures),
            _ => None,
        }
    }
}

impl fmt::Display for ExchangeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_dir_name())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointKind {
    BinancePublic,
    BinanceMarket,
    BybitPublic,
    BitgetPublic,
    GatePublic,
}

impl EndpointKind {
    pub fn exchange(self) -> ExchangeId {
        match self {
            EndpointKind::BinancePublic | EndpointKind::BinanceMarket => ExchangeId::BinanceFutures,
            EndpointKind::BybitPublic => ExchangeId::BybitLinear,
            EndpointKind::BitgetPublic => ExchangeId::BitgetV3,
            EndpointKind::GatePublic => ExchangeId::GateFutures,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            EndpointKind::BinancePublic => "binance_public",
            EndpointKind::BinanceMarket => "binance_market",
            EndpointKind::BybitPublic => "bybit_public",
            EndpointKind::BitgetPublic => "bitget_public",
            EndpointKind::GatePublic => "gate_public",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ConnId {
    A,
    B,
}

impl ConnId {
    pub fn peer(self) -> Self {
        match self {
            ConnId::A => ConnId::B,
            ConnId::B => ConnId::A,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            ConnId::A => "A",
            ConnId::B => "B",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "A" => Some(ConnId::A),
            "B" => Some(ConnId::B),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ConnState {
    WaitAnchor,
    Live,
    Invalid,
    Resync,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RecordKind {
    Data,
    DataEscaped,
    Meta,
}

impl RecordKind {
    pub fn as_str(self) -> &'static str {
        match self {
            RecordKind::Data => "DATA",
            RecordKind::DataEscaped => "DATA_ESCAPED",
            RecordKind::Meta => "META",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "DATA" => Some(RecordKind::Data),
            "DATA_ESCAPED" => Some(RecordKind::DataEscaped),
            "META" => Some(RecordKind::Meta),
            _ => None,
        }
    }
}

/// Incoming raw application frame stamped IMMEDIATELY before JSON parsing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawFrame {
    pub recv_wall_ns: i64,
    pub recv_mono_ns: u64,
    pub exchange: ExchangeId,
    pub endpoint: EndpointKind,
    pub conn_id: ConnId,
    pub epoch: u64,
    pub payload: String,
}

/// Canonical output record emitted by the causal selector into the single-stream TSV-Raw writer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalRecord {
    pub recv_wall_ns: i64,
    pub recv_mono_ns: u64,
    pub conn_id: ConnId,
    pub epoch: u64,
    pub rx_seq: u64,
    pub kind: RecordKind,
    pub payload: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockedReport {
    pub status: String,
    pub phase: String,
    pub failed_items: Vec<String>,
    pub minimal_fixes: Vec<String>,
}

impl BlockedReport {
    pub fn new(phase: &str, failed_items: Vec<String>, minimal_fixes: Vec<String>) -> Self {
        Self {
            status: "blocked".to_string(),
            phase: phase.to_string(),
            failed_items,
            minimal_fixes,
        }
    }
}
