//! Report-level data-quality snapshot payload types.

use rust_decimal::Decimal;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{
    enums::quant::DataQualityStatus,
    types::{FeatureVectorId, MarketId, Probability, TokenId, report_payload::DataQualitySummary},
};

/// Per-token DQ row inside one report fire snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenDataQualityRecord {
    /// Exact immutable feature vector whose state this row summarizes.
    /// Looking a vector up by market and decision time is ambiguous when two
    /// report rounds execute concurrently, so every fresh-boot row carries the
    /// immutable identifier directly.
    pub feature_vector_id: FeatureVectorId,
    pub token_id: TokenId,
    pub market_id: MarketId,
    pub status: DataQualityStatus,
    pub book_age_ms: u64,
    pub crossed: bool,
    pub empty: bool,
    pub fact_lag_ms: Option<u64>,
    pub missing_required: Vec<String>,
}

/// JSONB column wrapper for `quant_report_data_quality_snapshot.tokens_json`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(transparent)]
pub struct ReportDataQualityTokens(pub Vec<TokenDataQualityRecord>);

impl ReportDataQualityTokens {
    /// Aggregate token rows into the report-summary DQ block.
    #[must_use]
    pub fn summary(&self) -> DataQualitySummary {
        let mut out = DataQualitySummary::default();
        for record in &self.0 {
            match record.status {
                DataQualityStatus::Fresh => out.fresh_count += 1,
                DataQualityStatus::Acceptable => out.acceptable_count += 1,
                DataQualityStatus::Degraded => out.degraded_count += 1,
                DataQualityStatus::Stale => out.stale_count += 1,
                DataQualityStatus::Insufficient => out.insufficient_count += 1,
            }
        }
        out
    }
}

impl DataQualityStatus {
    /// Map feature DQ classification to a normalized score in `[0, 1]`.
    #[must_use]
    pub fn data_quality_score(self) -> Probability {
        let value = match self {
            Self::Fresh => Decimal::ONE,
            Self::Acceptable => Decimal::new(85, 2),
            Self::Degraded => Decimal::new(6, 1),
            Self::Stale => Decimal::new(3, 1),
            Self::Insufficient => Decimal::ZERO,
        };
        Probability::new(value)
    }
}
