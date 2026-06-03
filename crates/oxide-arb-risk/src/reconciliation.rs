//! Periodic ledger reconciliation.
//!
//! [`LedgerReconciler`] compares internal risk engine state against the
//! authoritative exchange / on-chain balances and positions. Any drift
//! beyond the configured tolerance is classified as a mismatch and
//! reported as Warning or Critical.

use crate::{
    traits::{BalanceQuerier, RiskMetrics},
    types::{ReconciliationMismatch, ReconciliationReport},
};
use chrono::Utc;
use num_traits::ToPrimitive;
use oxide_arb_error::OxideResult;
use oxide_arb_models::{
    enums::risk::ReconciliationStatus,
    types::{MarketId, Usd},
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::{collections::HashSet, time::Instant};

/// Compares internal vs. external balances and positions.
pub struct LedgerReconciler {
    tolerance: Usd,
}

impl LedgerReconciler {
    #[must_use]
    pub const fn new(tolerance_usd: Decimal) -> Self {
        Self {
            tolerance: Usd::new(tolerance_usd),
        }
    }

    /// Run a full reconciliation cycle via [`BalanceQuerier`] I/O.
    ///
    /// Prefer [`reconcile_fetched`](Self::reconcile_fetched) in production when
    /// the caller already has fresh balance/position data.
    pub async fn reconcile(
        &self,
        metrics: &dyn RiskMetrics,
        querier: &dyn BalanceQuerier,
    ) -> OxideResult<ReconciliationReport> {
        let (ext_available, ext_locked) = querier.query_balance().await?;
        let ext_positions = querier.query_positions().await?;
        Ok(self.reconcile_fetched(
            metrics,
            metrics.cash_balance(),
            ext_available,
            ext_locked,
            &ext_positions,
        ))
    }

    /// Reconcile using pre-fetched external data — **zero additional I/O**.
    ///
    /// Used by [`RiskMetricsRefreshService`] to piggyback reconciliation on the
    /// same CLOB balance + PG position fetch that updates the snapshot.
    pub fn reconcile_fetched(
        &self,
        metrics: &dyn RiskMetrics,
        internal_balance: Usd,
        ext_available: Usd,
        ext_locked: Usd,
        ext_positions: &[(MarketId, Usd)],
    ) -> ReconciliationReport {
        let start = Instant::now();

        let external_balance = ext_available + ext_locked;
        let internal_exposure = metrics.total_exposure();
        let reserved = metrics.reserved_usd();

        let mut mismatches = Vec::new();

        let balance_drift = internal_balance - external_balance;
        if balance_drift.abs() > self.tolerance {
            mismatches.push(ReconciliationMismatch::BalanceDrift {
                internal: internal_balance,
                external: external_balance,
                drift: balance_drift,
            });
        }

        let positions = metrics.open_positions();
        let mut external_exposure = Usd::ZERO;

        for (ext_market_id, ext_value) in ext_positions {
            external_exposure += *ext_value;
            let internal_value = metrics.market_exposure(ext_market_id);
            let drift = internal_value - *ext_value;

            if drift.abs() > self.tolerance {
                mismatches.push(ReconciliationMismatch::PositionDrift {
                    market_id: ext_market_id.clone(),
                    internal: internal_value,
                    external: *ext_value,
                    drift,
                });
            }
        }

        let ext_market_set: HashSet<_> = ext_positions.iter().map(|(m, _)| m.clone()).collect();

        for pos in &positions {
            if !ext_market_set.contains(&pos.market_id) {
                let internal_value = metrics.market_exposure(&pos.market_id);
                if internal_value.abs() > self.tolerance {
                    mismatches.push(ReconciliationMismatch::PositionDrift {
                        market_id: pos.market_id.clone(),
                        internal: internal_value,
                        external: Usd::ZERO,
                        drift: internal_value,
                    });
                }
            }
        }

        let critical_threshold = self.tolerance.inner() * dec!(10);
        let status = Self::classify(&mismatches, critical_threshold);

        let duration_ms = ToPrimitive::to_u64(&start.elapsed().as_millis()).unwrap_or(u64::MAX);

        match status {
            ReconciliationStatus::Ok => {
                tracing::info!(duration_ms, "reconciliation OK — no mismatches");
            }
            ReconciliationStatus::Warning => {
                tracing::warn!(
                    duration_ms,
                    mismatch_count = mismatches.len(),
                    "reconciliation completed with warnings"
                );
            }
            ReconciliationStatus::Critical => {
                tracing::warn!(
                    duration_ms,
                    mismatch_count = mismatches.len(),
                    "reconciliation CRITICAL — large drift detected"
                );
            }
        }

        ReconciliationReport {
            status,
            mismatches,
            internal_balance,
            external_balance,
            internal_exposure,
            external_exposure,
            reserved,
            tolerance: self.tolerance,
            checked_at: Utc::now(),
            duration_ms,
        }
    }

    fn classify(
        mismatches: &[ReconciliationMismatch],
        critical_threshold: Decimal,
    ) -> ReconciliationStatus {
        if mismatches.is_empty() {
            return ReconciliationStatus::Ok;
        }

        let has_critical = mismatches
            .iter()
            .any(|m| m.drift_abs().inner() >= critical_threshold);

        if has_critical {
            ReconciliationStatus::Critical
        } else {
            ReconciliationStatus::Warning
        }
    }
}
