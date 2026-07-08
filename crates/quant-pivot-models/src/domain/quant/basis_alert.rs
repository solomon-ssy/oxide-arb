//! Basis-cross-check exceedance alert persistence DTOs (11.2.2 remediation R6).
//!
//! One row per `(market, as_of)` where `domain.crypto.basis_vs_resolution_source`
//! exceeded the governed `domain.crypto.cross_check.max_basis_bps` threshold —
//! the feature-source (Binance) and the settlement oracle (Chainlink) disagreed
//! enough to warrant operator review. Append-only, immutable: an operator
//! "acknowledging" an alert is a UI/read concern, never a row mutation.

use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

use crate::types::{BasisAlertId, Bps, MarketId};

/// Persisted basis-exceedance alert row.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::quant_basis_alert::Entity")]
pub struct BasisAlertInfo {
    pub alert_id: BasisAlertId,
    pub market_id: MarketId,
    pub instrument_key: String,
    pub oracle_instrument_key: String,
    pub basis_bps: Bps,
    pub threshold_bps: Bps,
    pub as_of: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(BasisAlertInfo, crate::entities::quant_basis_alert::Model, {
    alert_id, market_id, instrument_key, oracle_instrument_key, basis_bps,
    threshold_bps, as_of, created_at,
});

/// Insert payload for `quant_basis_alert`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_basis_alert::ActiveModel")]
pub struct NewBasisAlert {
    pub alert_id: BasisAlertId,
    pub market_id: MarketId,
    pub instrument_key: String,
    pub oracle_instrument_key: String,
    pub basis_bps: Bps,
    pub threshold_bps: Bps,
    pub as_of: DateTime<Utc>,
}
