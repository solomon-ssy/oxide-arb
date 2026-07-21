//! WebSocket connection and streaming errors.

#[cfg(feature = "serde")]
use serde_json::Error as SerdeJsonError;
use thiserror::Error;

/// Errors from the CLOB WebSocket sharded connection manager.
#[derive(Debug, Error)]
pub enum WsError {
    #[error("Connection failed on shard {shard_id}: {reason}")]
    ConnectionFailed { shard_id: usize, reason: String },

    #[error("Connection closed (shard {shard_id}, code: {code:?})")]
    ConnectionClosed { shard_id: usize, code: Option<u16> },

    #[error("Subscription failed for {token_count} tokens on shard {shard_id}: {reason}")]
    SubscriptionFailed {
        shard_id: usize,
        token_count: usize,
        reason: String,
    },

    #[error("Reconnection exhausted on shard {shard_id} after {attempts} attempts")]
    ReconnectionExhausted { shard_id: usize, attempts: u32 },

    #[error("Ping timeout on shard {shard_id} (no pong in {deadline_ms}ms)")]
    PingTimeout { shard_id: usize, deadline_ms: u64 },

    #[cfg(feature = "serde")]
    #[error("Message parse error: {0}")]
    MessageParse(#[from] SerdeJsonError),

    #[cfg(not(feature = "serde"))]
    #[error("Message parse error: {0}")]
    MessageParse(String),

    #[error("Channel send failed: {0}")]
    ChannelSend(String),
}
