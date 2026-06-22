//! System health enums.

use serde::{Deserialize, Serialize};

/// Oracle/data source health state for sliding-window evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceHealth {
    Healthy,
    Degraded,
    Down,
}

/// Graceful shutdown lifecycle stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShutdownStage {
    NotStarted,
    SignalReceived,
    Draining,
    Stopped,
}

/// WebSocket shard connection lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShardConnectionStatus {
    Connected,
    Reconnecting { attempt: u32 },
    Disconnected,
}
