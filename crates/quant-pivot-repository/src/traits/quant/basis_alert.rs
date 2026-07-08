//! Basis-cross-check exceedance alert repository trait (11.2.2 remediation R6).

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{BasisAlertInfo, BasisAlertListQuery, NewBasisAlert, Paginated},
    types::MarketId,
};

/// Append-only persistence port for the basis-cross-check exceedance feed.
#[async_trait::async_trait]
pub trait BasisAlertRepository: Send + Sync {
    /// Record one exceedance (never updated or deleted).
    async fn record(&self, alert: NewBasisAlert) -> Result<BasisAlertInfo, StorageError>;

    /// The most recently recorded alert for `market_id`, if any — drives the
    /// per-market cooldown so a persistent divergence raises one alert per
    /// `alert_cooldown_secs` window, not one per report round.
    async fn latest_for_market(
        &self,
        market_id: &MarketId,
    ) -> Result<Option<BasisAlertInfo>, StorageError>;

    /// Page the alert feed, newest first.
    async fn page(
        &self,
        query: BasisAlertListQuery,
    ) -> Result<Paginated<BasisAlertInfo>, StorageError>;
}
