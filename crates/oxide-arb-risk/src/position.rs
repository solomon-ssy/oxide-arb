//! Potential-loss ledger.
//!
//! [`PotentialLossLedger`] tracks worst-case loss for unsettled positions so
//! the risk engine can account for committed capital.

use chrono::Utc;
use oxide_arb_models::{
    domain::potential_loss::PotentialLossInfo,
    enums::common::LedgerStatus,
    types::{LedgerId, Usd},
};
use std::collections::HashMap;

// ── Potential Loss Ledger ───────────────────────────────────────────────────

/// Tracks maximum potential loss for positions that have not yet settled.
///
/// The risk engine subtracts `total_potential_loss()` from available capital
/// to ensure worst-case exposure is always accounted for. Maintains a running
/// total for O(1) queries on the hot path.
#[derive(Clone)]
pub struct PotentialLossLedger {
    entries: HashMap<LedgerId, PotentialLossInfo>,
    running_total: Usd,
}

impl PotentialLossLedger {
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            running_total: Usd::ZERO,
        }
    }

    /// Bootstrap from a list of entries (e.g. loaded from persistence).
    #[must_use]
    pub fn from_entries(entries: Vec<PotentialLossInfo>) -> Self {
        let running_total = entries
            .iter()
            .filter(|e| e.is_active())
            .map(|e| e.max_loss_usd)
            .sum();
        let map = entries
            .into_iter()
            .map(|e| (e.ledger_id.clone(), e))
            .collect();
        Self {
            entries: map,
            running_total,
        }
    }

    /// Record a new potential-loss entry.
    pub fn record_entry(&mut self, entry: PotentialLossInfo) {
        if self.entries.contains_key(&entry.ledger_id) {
            tracing::debug!(
                entry_id = %entry.ledger_id,
                "potential loss entry already recorded"
            );
            return;
        }
        if entry.is_active() {
            self.running_total += entry.max_loss_usd;
        }
        tracing::info!(
            entry_id = %entry.ledger_id,
            market_id = %entry.market_id,
            max_loss = %entry.max_loss_usd,
            running_total = %self.running_total,
            "potential loss entry recorded"
        );
        self.entries.insert(entry.ledger_id.clone(), entry);
    }

    /// Mark an entry as resolved (position settled or closed).
    pub fn resolve(&mut self, ledger_id: &LedgerId) {
        if let Some(entry) = self.entries.get_mut(ledger_id) {
            if entry.is_active() {
                self.running_total = (self.running_total - entry.max_loss_usd).max(Usd::ZERO);
            }
            entry.status = LedgerStatus::Resolved;
            entry.resolved_at = Some(Utc::now());
            tracing::info!(
                ledger_id = %ledger_id,
                running_total = %self.running_total,
                "potential loss entry resolved"
            );
        }
    }

    /// Sum of `max_loss` across all active (unresolved) entries.
    #[must_use]
    #[inline]
    pub const fn total_potential_loss(&self) -> Usd {
        self.running_total
    }

    #[must_use]
    pub fn active_count(&self) -> usize {
        self.entries.values().filter(|e| e.is_active()).count()
    }

    #[must_use]
    pub fn active_entries(&self) -> Vec<&PotentialLossInfo> {
        self.entries.values().filter(|e| e.is_active()).collect()
    }
}

impl Default for PotentialLossLedger {
    fn default() -> Self {
        Self::new()
    }
}
