//! Audited native `ClickHouse` reads that do not belong to the quant fact port.

use std::sync::Arc;

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::{QuantReportRecommendationFactRow, ReportMarketFunnelRow, TradeTapeRow},
    config::MAX_TRADE_TAPE_RECONCILIATION_ROWS,
    types::RecommendationReportId,
};
use quant_pivot_storage::clickhouse::ClickHousePool;

use crate::clickhouse::query_limits::{
    REPORT_FUNNEL_VERIFY, REPORT_RECOMMENDATION_VERIFY, TRADE_TAPE_RECONCILIATION,
};

/// Concrete owner for operational and acceptance-only native reads.
pub struct ChNativeReadRepository {
    pool: Arc<ClickHousePool>,
}

impl ChNativeReadRepository {
    #[must_use]
    pub const fn new(pool: Arc<ClickHousePool>) -> Self {
        Self { pool }
    }

    /// Read the latest reconciliation inputs with both SQL and client-side
    /// overflow checks. `hard_row_limit + 1` is intentional so overflow is
    /// reported instead of silently truncated.
    pub async fn trade_tape_reconciliation_rows(
        &self,
        from_ms: i64,
        to_ms: i64,
        hard_row_limit: usize,
    ) -> Result<Vec<TradeTapeRow>, StorageError> {
        if hard_row_limit > MAX_TRADE_TAPE_RECONCILIATION_ROWS {
            return Err(StorageError::invariant_violation(
                Some("quant_trade_tape"),
                format!(
                    "trade reconciliation row limit {hard_row_limit} exceeds hard maximum {MAX_TRADE_TAPE_RECONCILIATION_ROWS}"
                ),
            ));
        }
        let query_limit = hard_row_limit.checked_add(1).ok_or_else(|| {
            StorageError::invariant_violation(
                Some("quant_trade_tape"),
                "trade reconciliation row limit overflow",
            )
        })?;
        let query_limit = u64::try_from(query_limit).map_err(|error| {
            StorageError::invariant_violation(
                Some("quant_trade_tape"),
                format!("trade reconciliation row limit is not representable: {error}"),
            )
        })?;
        if query_limit > TRADE_TAPE_RECONCILIATION.max_result_rows() {
            return Err(StorageError::invariant_violation(
                Some("quant_trade_tape"),
                format!(
                    "trade reconciliation query limit {query_limit} exceeds server result limit {}",
                    TRADE_TAPE_RECONCILIATION.max_result_rows()
                ),
            ));
        }
        let rows = TRADE_TAPE_RECONCILIATION
            .query(
                self.pool.client(),
                "SELECT ?fields FROM quant_trade_tape \
                 WHERE event_time >= fromUnixTimestamp64Milli(?) \
                 AND event_time < fromUnixTimestamp64Milli(?) \
                 AND ingestion_time <= fromUnixTimestamp64Milli(?) \
                 ORDER BY ingestion_time DESC, revision DESC \
                 LIMIT 1 BY market_id, token_id, participant_role, event_time, source_event_id, participant_address \
                 LIMIT ?",
            )
            .bind(from_ms)
            .bind(to_ms)
            .bind(to_ms)
            .bind(query_limit)
            .fetch_all::<TradeTapeRow>()
            .await?;
        if rows.len() > hard_row_limit {
            return Err(StorageError::invariant_violation(
                Some("quant_trade_tape"),
                "trade reconciliation input exceeds the configured hard row limit",
            ));
        }
        Ok(rows)
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
