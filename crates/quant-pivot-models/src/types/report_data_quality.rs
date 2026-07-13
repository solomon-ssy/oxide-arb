//! Report-level data-quality snapshot payload types.

use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{
    enums::quant::DataQualityStatus,
    jsonb_active,
    types::{FeatureVectorId, MarketId, Probability, TokenId, report_payload::DataQualitySummary},
};
use rust_decimal::Decimal;

/// Per-token DQ row inside one report fire snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenDataQualityRecord {
    /// Exact immutable feature vector whose state this row summarizes.
    ///
    /// This binding is required for report-scoped parity. Looking a vector up
    /// by market and decision time is ambiguous when two report rounds execute
    /// concurrently and must never be used as serving evidence. `None` is
    /// accepted only while decoding pre-v10 audit history; every new report
    /// commit rejects it and replay reports it as legacy-unbound.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feature_vector_id: Option<FeatureVectorId>,
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
pub struct ReportDataQualityTokens(pub Vec<TokenDataQualityRecord>);

jsonb_active!(ReportDataQualityTokens);

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

/// Map feature DQ classification to a normalized score in `[0, 1]`.
#[must_use]
pub fn data_quality_score(status: DataQualityStatus) -> Probability {
    let value = match status {
        DataQualityStatus::Fresh => Decimal::ONE,
        DataQualityStatus::Acceptable => Decimal::new(85, 2),
        DataQualityStatus::Degraded => Decimal::new(6, 1),
        DataQualityStatus::Stale => Decimal::new(3, 1),
        DataQualityStatus::Insufficient => Decimal::ZERO,
    };
    Probability::new(value)
}
