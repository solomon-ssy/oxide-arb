//! Durable-trade integrity snapshot for admission and recovery gates.
//!
//! Published via `ArcSwap` in `quant-pivot-core` so pre-trade checks and the
//! execution FSM read a single zero-I/O view of blocking trades and in-flight
//! reservations.

use crate::{enums::legacy::LegacyExecutionMode, types::Usd};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Point-in-time view of unresolved durable trades and reservation backlog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradeIntegritySnapshot {
    /// Rows that must be reconciled or terminalized before new Live/Paper entries.
    pub blocking_count: u32,
    /// Subset flagged `needs_reconcile` (subset of blocking in normal operation).
    pub needs_reconcile_count: u32,
    /// `Intent` rows left after a crash before venue submit completed.
    pub intent_orphan_count: u32,
    /// Age of the oldest blocking row, or zero when the queue is empty.
    pub oldest_blocking_age_secs: u64,
    /// Active in-memory reservations (includes rehydrated rows).
    pub active_reservation_count: u32,
    pub reserved_usd: Usd,
    pub checked_at: DateTime<Utc>,
}

impl TradeIntegritySnapshot {
    /// Empty snapshot used before the first refresh or rehydrate.
    #[must_use]
    pub const fn zero(checked_at: DateTime<Utc>) -> Self {
        Self {
            blocking_count: 0,
            needs_reconcile_count: 0,
            intent_orphan_count: 0,
            oldest_blocking_age_secs: 0,
            active_reservation_count: 0,
            reserved_usd: Usd::ZERO,
            checked_at,
        }
    }

    /// Whether new entries should be denied for `mode`.
    #[must_use]
    pub const fn blocks_admission(&self, mode: LegacyExecutionMode) -> bool {
        match mode {
            LegacyExecutionMode::Live | LegacyExecutionMode::Paper => self.blocking_count > 0,
            LegacyExecutionMode::DryRun => false,
        }
    }
}
