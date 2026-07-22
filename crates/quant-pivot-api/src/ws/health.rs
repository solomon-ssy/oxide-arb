//! Aggregated per-shard connection state.
//!
//! Shards report `connected` transitions here; the manager exposes a compact
//! [`ShardHealthSummary`] so operators get one periodic aggregate line (via
//! the core `HealthChecker`) instead of per-shard reconnect log spam.

use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    time::Instant,
};

use parking_lot::RwLock;
use quant_pivot_models::domain::governance::lifecycle::WS_MARKET_DATA_STALE_THRESHOLD_MS;

/// Per-shard connection slots, indexed by `shard_id`.
#[derive(Default)]
pub struct ShardHealthBoard {
    slots: RwLock<Vec<ShardSlot>>,
}

#[derive(Clone, Copy)]
struct ShardSlot {
    connected: bool,
    /// When the slot last transitioned into its current state.
    since: Instant,
}

impl ShardHealthBoard {
    /// Ensure a slot exists for `shard_id` (called once per spawned shard).
    pub fn register(&self, shard_id: usize) {
        let mut slots = self.slots.write();
        while slots.len() <= shard_id {
            slots.push(ShardSlot {
                connected: false,
                since: Instant::now(),
            });
        }
        drop(slots);
    }

    /// Record a connection state transition for `shard_id` (idempotent).
    pub fn set_connected(&self, shard_id: usize, connected: bool) {
        let mut slots = self.slots.write();
        if let Some(slot) = slots.get_mut(shard_id)
            && slot.connected != connected
        {
            *slot = ShardSlot {
                connected,
                since: Instant::now(),
            };
        }
    }

    /// Aggregate snapshot across all registered shards.
    #[must_use]
    pub fn summary(&self) -> ShardHealthSummary {
        let slots = self.slots.read();
        let mut disconnected = 0usize;
        let mut oldest_disconnected_secs = None;
        for slot in slots.iter().filter(|slot| !slot.connected) {
            disconnected += 1;
            let secs = slot.since.elapsed().as_secs();
            oldest_disconnected_secs =
                Some(oldest_disconnected_secs.map_or(secs, |oldest: u64| oldest.max(secs)));
        }
        ShardHealthSummary {
            total: slots.len(),
            disconnected,
            oldest_disconnected_secs,
            connected_ratio_bps: connected_ratio_bps(slots.len(), disconnected),
        }
    }
}

/// Snapshot of shard connectivity for health checks and aggregate logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardHealthSummary {
    /// Shards spawned so far.
    pub total: usize,
    /// Shards currently without a live connection.
    pub disconnected: usize,
    /// Age (seconds) of the longest-standing disconnection, if any.
    pub oldest_disconnected_secs: Option<u64>,
    /// Connected shard ratio in basis points (`10_000` means all connected).
    pub connected_ratio_bps: u32,
}

impl Display for ShardHealthSummary {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        if self.total == 0 {
            return write!(f, "no WS shards spawned");
        }
        match self.oldest_disconnected_secs {
            Some(oldest) if self.disconnected > 0 => write!(
                f,
                "{}/{} WS shards disconnected (oldest {}s)",
                self.disconnected, self.total, oldest
            ),
            _ => write!(f, "all {} WS shards connected", self.total),
        }
    }
}

/// Read-only shard connectivity projection for health and system-status assembly.
pub trait WsShardHealthPort: Send + Sync {
    fn shard_health(&self) -> ShardHealthSummary;

    /// Milliseconds since the last CLOB websocket message on any shard.
    fn last_message_age_ms(&self) -> Option<u64>;

    /// Whether market-data is healthy: global traffic fresh within
    /// [`WS_MARKET_DATA_STALE_THRESHOLD_MS`] AND every spawned shard connected.
    ///
    /// Single source of truth for the connection-liveness exemption: while
    /// healthy, a quiet-but-valid book is the current venue truth and stays
    /// usable; only connection failure turns an aged book into a stale one.
    fn market_data_healthy(&self) -> bool {
        let summary = self.shard_health();
        let coverage_ready = summary.total == 0 || summary.disconnected == 0;
        let traffic_fresh = self
            .last_message_age_ms()
            .is_some_and(|age| age < WS_MARKET_DATA_STALE_THRESHOLD_MS);
        traffic_fresh && coverage_ready
    }
}

fn connected_ratio_bps(total: usize, disconnected: usize) -> u32 {
    if total == 0 {
        return 10_000;
    }
    let connected = total.saturating_sub(disconnected);
    let bps = connected.saturating_mul(10_000) / total;
    u32::try_from(bps).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::ShardHealthBoard;

    #[test]
    fn summary_tracks_transitions() {
        let board = ShardHealthBoard::default();
        board.register(0);
        board.register(1);

        let summary = board.summary();
        assert_eq!(summary.total, 2);
        assert_eq!(summary.disconnected, 2);

        board.set_connected(0, true);
        board.set_connected(1, true);
        let summary = board.summary();
        assert_eq!(summary.disconnected, 0);
        assert!(summary.oldest_disconnected_secs.is_none());
        assert_eq!(summary.to_string(), "all 2 WS shards connected");

        board.set_connected(1, false);
        let summary = board.summary();
        assert_eq!(summary.disconnected, 1);
        assert!(summary.oldest_disconnected_secs.is_some());
    }
}
