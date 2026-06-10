//! Execution structural configuration (`[execution]`, deploy).
//!
//! Channel capacities and shard counts are bound to task/channel construction
//! at startup and require a restart. Operational execution tunables (timeouts,
//! funnel, coalescer, latency SLOs) are runtime configuration
//! (`runtime_config::ExecutionRuntimeConfig`).

use serde::Deserialize;

/// Execution structural parameters.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ExecutionDeployConfig {
    /// Sharded book-apply / execution-runner topology.
    pub book_apply: BookApplyConfig,
}

/// Sharded book-apply workers (sized for a 500-market single host).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BookApplyConfig {
    /// Number of book-apply / execution-runner shards. Markets hash to a
    /// shard, so this bounds intra-market ordering domains and worker
    /// parallelism. Default: `4`.
    pub shard_count: usize,
    /// Per-shard bounded channel capacity; overflow triggers backpressure
    /// accounting (never blocks the WS event loop). Default: `2048`.
    pub channel_capacity: usize,
}

impl Default for BookApplyConfig {
    fn default() -> Self {
        Self {
            shard_count: default_book_shard_count(),
            channel_capacity: default_book_channel_capacity(),
        }
    }
}

const fn default_book_shard_count() -> usize {
    4
}
const fn default_book_channel_capacity() -> usize {
    2048
}
