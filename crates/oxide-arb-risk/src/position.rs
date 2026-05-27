//! Position tracking and potential-loss ledger.
//!
//! [`PositionTracker`] maintains a per-market exposure view derived from
//! [`RiskMetrics`]. [`PotentialLossLedger`] tracks worst-case loss for
//! unsettled positions so the risk engine can account for committed capital.

use crate::traits::RiskMetrics;
use chrono::{DateTime, Utc};
use oxide_arb_models::{
    domain::{potential_loss::PotentialLossInfo, risk::MarketExposure},
    enums::common::LedgerStatus,
    types::{LedgerId, MarketId, Usd},
};
use std::collections::HashMap;

// ── Position Tracker ────────────────────────────────────────────────────────

/// Aggregated per-market exposure view, refreshed from [`RiskMetrics`].
///
/// Not a ledger of individual positions — it is a cached summary for
/// fast lookup during pre-trade checks.
pub struct PositionTracker {
    market_exposures: HashMap<MarketId, MarketExposure>,
    total_position_value: Usd,
    last_refresh: DateTime<Utc>,
}

impl PositionTracker {
    #[must_use]
    pub fn new() -> Self {
        Self {
            market_exposures: HashMap::new(),
            total_position_value: Usd::ZERO,
            last_refresh: Utc::now(),
        }
    }

    /// Re-derive exposure data from the authoritative [`RiskMetrics`] source.
    pub fn refresh(&mut self, metrics: &dyn RiskMetrics) {
        self.market_exposures.clear();

        let positions = metrics.open_positions();
        let mut total = Usd::ZERO;

        let mut by_market: HashMap<MarketId, Usd> = HashMap::new();
        for pos in &positions {
            *by_market.entry(pos.market_id.clone()).or_insert(Usd::ZERO) += pos.total_cost_usd;
        }

        for (market_id, position_value) in &by_market {
            let full_exposure = metrics.market_exposure(market_id);
            let reserved = (full_exposure - *position_value).max(Usd::ZERO);
            let exposure = MarketExposure {
                market_id: market_id.clone(),
                position_value: *position_value,
                reserved_value: reserved,
                total_exposure: *position_value + reserved,
            };
            total += exposure.total_exposure;
            self.market_exposures.insert(market_id.clone(), exposure);
        }

        self.total_position_value = total;
        self.last_refresh = Utc::now();
        tracing::info!(
            markets = self.market_exposures.len(),
            total_value = %self.total_position_value,
            "position tracker refreshed"
        );
    }

    /// Exposure in a single market. Returns `Usd::ZERO` if unknown.
    #[must_use]
    #[inline]
    pub fn market_exposure(&self, market_id: &MarketId) -> Usd {
        self.market_exposures
            .get(market_id)
            .map_or(Usd::ZERO, |e| e.total_exposure)
    }

    #[must_use]
    #[inline]
    pub const fn total_position_value(&self) -> Usd {
        self.total_position_value
    }

    #[must_use]
    pub fn all_exposures(&self) -> Vec<&MarketExposure> {
        self.market_exposures.values().collect()
    }
}

impl Default for PositionTracker {
    fn default() -> Self {
        Self::new()
    }
}

// ── Potential Loss Ledger ───────────────────────────────────────────────────

/// Tracks maximum potential loss for positions that have not yet settled.
///
/// The risk engine subtracts `total_potential_loss()` from available capital
/// to ensure worst-case exposure is always accounted for. Maintains a running
/// total for O(1) queries on the hot path.
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
