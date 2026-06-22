//! Aggregated per-shard connection state.
//!
//! Shards report `connected` transitions here; the manager exposes a compact
//! [`ShardHealthSummary`] so operators get one periodic aggregate line (via
//! the core `HealthChecker`) instead of per-shard reconnect log spam.

use oxide_arb_models::types::TokenId;
use parking_lot::RwLock;
use std::{
    collections::{HashMap, HashSet},
    fmt::{self, Display, Formatter},
    time::Instant,
};

/// Per-shard connection slots, indexed by `shard_id`.
#[derive(Default)]
pub struct ShardHealthBoard {
    slots: RwLock<Vec<ShardSlot>>,
}

/// Token-level market-data freshness observed by the CLOB WS transport.
#[derive(Default)]
pub struct TokenFreshnessBoard {
    tokens: RwLock<HashMap<TokenId, TokenFreshnessSlot>>,
}

#[derive(Clone, Copy)]
struct ShardSlot {
    connected: bool,
    /// When the slot last transitioned into its current state.
    since: Instant,
}

#[derive(Clone, Copy)]
struct TokenFreshnessSlot {
    last_message_at: Instant,
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
        if let Some(slot) = slots.get_mut(shard_id) {
            if slot.connected != connected {
                *slot = ShardSlot {
                    connected,
                    since: Instant::now(),
                };
            }
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

impl TokenFreshnessBoard {
    /// Record that a token received at least one normalized WS event.
    pub fn mark_token(&self, token_id: &TokenId, at: Instant) {
        self.tokens.write().insert(
            token_id.clone(),
            TokenFreshnessSlot {
                last_message_at: at,
            },
        );
    }

    /// Milliseconds since the last WS event for a token.
    #[must_use]
    pub fn token_age_ms(&self, token_id: &TokenId) -> Option<u64> {
        self.tokens
            .read()
            .get(token_id)
            .and_then(|slot| u64::try_from(slot.last_message_at.elapsed().as_millis()).ok())
    }

    /// Drop freshness slots for tokens no longer present in the transport union.
    pub fn prune_tokens(&self, active_tokens: &HashSet<TokenId>) {
        self.tokens
            .write()
            .retain(|token_id, _| active_tokens.contains(token_id));
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
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
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
