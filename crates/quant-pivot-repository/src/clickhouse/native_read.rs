//! Audited native `ClickHouse` reads that do not belong to the quant fact port.

use std::sync::Arc;

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::{QuantReportRecommendationFactRow, ReportMarketFunnelRow},
    types::RecommendationReportId,
};
use quant_pivot_storage::clickhouse::ClickHousePool;

use crate::clickhouse::query_limits::{REPORT_FUNNEL_VERIFY, REPORT_RECOMMENDATION_VERIFY};

/// Concrete owner for operational and acceptance-only native reads.
pub struct ChNativeReadRepository {
    pool: Arc<ClickHousePool>,
}

impl ChNativeReadRepository {
    #[must_use]
    pub const fn new(pool: Arc<ClickHousePool>) -> Self {
        Self { pool }
    }

    pub async fn report_recommendation_rows(
        &self,
        report_id: &RecommendationReportId,
    ) -> Result<Vec<QuantReportRecommendationFactRow>, StorageError> {
        REPORT_RECOMMENDATION_VERIFY
            .query(
                self.pool.client(),
                "SELECT ?fields FROM quant_report_recommendation_fact FINAL \
                 WHERE recommendation_report_id = ? \
                 ORDER BY rank, recommendation_id",
            )
            .bind(*report_id)
            .fetch_all::<QuantReportRecommendationFactRow>()
            .await
            .map_err(StorageError::from)
    }

    pub async fn report_funnel_rows(
        &self,
        report_id: &RecommendationReportId,
    ) -> Result<Vec<ReportMarketFunnelRow>, StorageError> {
        REPORT_FUNNEL_VERIFY
            .query(
                self.pool.client(),
                "SELECT ?fields FROM quant_report_market_funnel FINAL \
                 WHERE recommendation_report_id = ? ORDER BY market_id",
            )
            .bind(*report_id)
            .fetch_all::<ReportMarketFunnelRow>()
            .await
            .map_err(StorageError::from)
    }
}
