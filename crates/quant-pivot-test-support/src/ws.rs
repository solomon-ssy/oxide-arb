//! CLOB websocket health test doubles.

use quant_pivot_api::ws::{ShardHealthSummary, WsShardHealthPort};
use std::sync::Arc;

/// Fixed shard connectivity for integration tests (no live socket or manager).
#[derive(Debug, Clone, Copy)]
pub struct FixedWsShardHealth(ShardHealthSummary);

impl FixedWsShardHealth {
    /// One connected shard — satisfies `system_status` market-data readiness.
    #[must_use]
    pub fn operational() -> Arc<dyn WsShardHealthPort> {
        Arc::new(Self(ShardHealthSummary {
            total: 1,
            disconnected: 0,
            oldest_disconnected_secs: None,
            connected_ratio_bps: 10_000,
        }))
    }
}

impl WsShardHealthPort for FixedWsShardHealth {
    fn shard_health(&self) -> ShardHealthSummary {
        self.0
    }
}
