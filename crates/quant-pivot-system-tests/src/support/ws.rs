//! CLOB websocket health doubles owned by system tests.

use std::sync::Arc;

use quant_pivot_api::ws::{ShardHealthSummary, WsShardHealthPort};

/// Fixed shard connectivity for integration tests (no live socket or manager).
#[derive(Debug, Clone, Copy)]
pub struct WsShardHealth {
    summary: ShardHealthSummary,
    last_message_age_ms: Option<u64>,
}

impl WsShardHealth {
    /// One connected shard with a fresh message — satisfies market-data readiness.
    #[must_use]
    pub fn operational() -> Arc<dyn WsShardHealthPort> {
        Self::with_message_age(Some(0))
    }

    /// Configure shard connectivity and optional last-message age.
    #[must_use]
    pub fn with_message_age(last_message_age_ms: Option<u64>) -> Arc<dyn WsShardHealthPort> {
        Arc::new(Self {
            summary: ShardHealthSummary {
                total: 1,
                disconnected: 0,
                oldest_disconnected_secs: None,
                connected_ratio_bps: 10_000,
            },
            last_message_age_ms,
        })
    }

    /// Connected shards with a custom summary and message age.
    #[must_use]
    pub fn custom(
        summary: ShardHealthSummary,
        last_message_age_ms: Option<u64>,
    ) -> Arc<dyn WsShardHealthPort> {
        Arc::new(Self {
            summary,
            last_message_age_ms,
        })
    }
}

impl WsShardHealthPort for WsShardHealth {
    fn shard_health(&self) -> ShardHealthSummary {
        self.summary
    }

    fn last_message_age_ms(&self) -> Option<u64> {
        self.last_message_age_ms
    }
}
