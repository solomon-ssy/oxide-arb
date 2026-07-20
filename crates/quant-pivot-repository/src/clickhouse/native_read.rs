//! Audited native `ClickHouse` reads that do not belong to the quant fact port.

use std::sync::Arc;

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::{QuantReportRecommendationFactRow, ReportMarketFunnelRow, TradeTapeRow},
    config::MAX_TRADE_TAPE_RECONCILIATION_ROWS,
    types::RecommendationReportId,
};
use quant_pivot_storage::clickhouse::ClickHousePool;

use crate::sql_contract_registry::{
    PHASE119_FACT_IDEMPOTENCY, PHASE119_GISTEMP_EVIDENCE, REPORT_FUNNEL_VERIFY,
    REPORT_RECOMMENDATION_VERIFY, TRADE_TAPE_RECONCILIATION,
};

/// Fixed Phase 11.9 fact family whose idempotency is measured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FactEvidenceTable {
    DomainCrypto,
    CryptoPriceReport,
    WeatherObservation,
    WeatherForecast,
}

impl FactEvidenceTable {
    /// Canonical physical table name used in the evidence manifest.
    #[must_use]
    pub const fn table_name(self) -> &'static str {
        match self {
            Self::DomainCrypto => "quant_domain_observation",
            Self::CryptoPriceReport => "quant_crypto_price_report",
            Self::WeatherObservation => "quant_weather_observation_fact",
            Self::WeatherForecast => "quant_weather_forecast_fact",
        }
    }

    const fn queries(self) -> [&'static str; 4] {
        match self {
            Self::DomainCrypto => [
                "SELECT count() FROM quant_domain_observation WHERE family = 'crypto'",
                "SELECT uniqExact(tuple(source_id, instrument_key, metric, event_time)) FROM quant_domain_observation WHERE family = 'crypto'",
                "SELECT toUInt64(count() - uniqExact(tuple(source_id, instrument_key, metric, event_time))) FROM quant_domain_observation WHERE family = 'crypto'",
                "SELECT toUInt64(0)",
            ],
            Self::CryptoPriceReport => [
                "SELECT count() FROM quant_crypto_price_report",
                "SELECT uniqExact(tuple(source_id, instrument_key, source_sequence, event_time, report_hash)) FROM quant_crypto_price_report",
                "SELECT toUInt64(count() - uniqExact(tuple(source_id, instrument_key, source_sequence, event_time, report_hash))) FROM quant_crypto_price_report",
                "SELECT toUInt64(0)",
            ],
            Self::WeatherObservation => [
                "SELECT count() FROM quant_weather_observation_fact",
                "SELECT uniqExact(tuple(source_id, instrument_key, variable, observed_at, report_hash)) FROM quant_weather_observation_fact",
                "SELECT toUInt64(count() - uniqExact(tuple(source_id, instrument_key, variable, observed_at, report_hash))) FROM quant_weather_observation_fact",
                "SELECT toUInt64(count() - uniqExact(tuple(source_id, instrument_key, variable, observed_at, revision))) FROM quant_weather_observation_fact",
            ],
            Self::WeatherForecast => [
                "SELECT count() FROM quant_weather_forecast_fact",
                "SELECT uniqExact(tuple(source_id, instrument_key, variable, reference_time, valid_time, ifNull(member, 65535), report_hash)) FROM quant_weather_forecast_fact",
                "SELECT toUInt64(count() - uniqExact(tuple(source_id, instrument_key, variable, reference_time, valid_time, ifNull(member, 65535), report_hash))) FROM quant_weather_forecast_fact",
                "SELECT toUInt64(count() - uniqExact(tuple(source_id, instrument_key, variable, reference_time, valid_time, ifNull(member, 65535), revision))) FROM quant_weather_forecast_fact",
            ],
        }
    }
}

/// Raw counts used to prove idempotent fact ingestion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FactIdempotencyCounts {
    pub physical_rows: u64,
    pub logical_keys: u64,
    pub duplicate_rows: u64,
    pub revision_conflicts: u64,
}

/// Raw GISTEMP timestamps used by the Phase 11.9 evidence manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GistempHistoricalTimeRaw {
    pub row_count: u64,
    pub earliest_local_date_epoch_days: Option<i32>,
    pub earliest_observed_at_ms: Option<i64>,
    pub earliest_valid_from_ms: Option<i64>,
    pub earliest_valid_to_ms: Option<i64>,
    pub null_valid_from_rows: u64,
    pub null_valid_to_rows: u64,
}

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
        if query_limit > TRADE_TAPE_RECONCILIATION.result_row_budget() {
            return Err(StorageError::invariant_violation(
                Some("quant_trade_tape"),
                format!(
                    "trade reconciliation query limit {query_limit} exceeds SQL contract budget {}",
                    TRADE_TAPE_RECONCILIATION.result_row_budget()
                ),
            ));
        }
        let rows = TRADE_TAPE_RECONCILIATION
            .clickhouse_query(
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
            .clickhouse_query(
                self.pool.client(),
                "SELECT ?fields FROM quant_report_recommendation_fact FINAL \
                 WHERE recommendation_report_id = ? \
                 ORDER BY rank, recommendation_id",
            )
            .bind(report_id.clone())
            .fetch_all::<QuantReportRecommendationFactRow>()
            .await
            .map_err(StorageError::from)
    }

    pub async fn report_funnel_rows(
        &self,
        report_id: &RecommendationReportId,
    ) -> Result<Vec<ReportMarketFunnelRow>, StorageError> {
        REPORT_FUNNEL_VERIFY
            .clickhouse_query(
                self.pool.client(),
                "SELECT ?fields FROM quant_report_market_funnel FINAL \
                 WHERE recommendation_report_id = ? ORDER BY market_id",
            )
            .bind(report_id.clone())
            .fetch_all::<ReportMarketFunnelRow>()
            .await
            .map_err(StorageError::from)
    }

    pub async fn fact_idempotency(
        &self,
        table: FactEvidenceTable,
    ) -> Result<FactIdempotencyCounts, StorageError> {
        let [
            physical_rows,
            logical_keys,
            duplicate_rows,
            revision_conflicts,
        ] = table.queries();
        let (physical_rows, logical_keys, duplicate_rows, revision_conflicts) = tokio::try_join!(
            self.evidence_u64(physical_rows),
            self.evidence_u64(logical_keys),
            self.evidence_u64(duplicate_rows),
            self.evidence_u64(revision_conflicts),
        )?;
        Ok(FactIdempotencyCounts {
            physical_rows,
            logical_keys,
            duplicate_rows,
            revision_conflicts,
        })
    }

    pub async fn gistemp_historical_time(&self) -> Result<GistempHistoricalTimeRaw, StorageError> {
        let row_count = self
            .gistemp_u64(
                "SELECT count() FROM quant_weather_observation_fact WHERE source_id = 'nasa_gistemp'",
            )
            .await?;
        if row_count == 0 {
            return Ok(GistempHistoricalTimeRaw {
                row_count,
                earliest_local_date_epoch_days: None,
                earliest_observed_at_ms: None,
                earliest_valid_from_ms: None,
                earliest_valid_to_ms: None,
                null_valid_from_rows: 0,
                null_valid_to_rows: 0,
            });
        }
        let (
            earliest_local_date_epoch_days,
            earliest_observed_at_ms,
            earliest_valid_from_ms,
            earliest_valid_to_ms,
            null_valid_from_rows,
            null_valid_to_rows,
        ) = tokio::try_join!(
            self.gistemp_i32(
                "SELECT min(local_date) FROM quant_weather_observation_fact WHERE source_id = 'nasa_gistemp'",
            ),
            self.gistemp_i64(
                "SELECT min(observed_at) FROM quant_weather_observation_fact WHERE source_id = 'nasa_gistemp'",
            ),
            self.gistemp_i64(
                "SELECT min(assumeNotNull(valid_from)) FROM quant_weather_observation_fact WHERE source_id = 'nasa_gistemp' AND valid_from IS NOT NULL",
            ),
            self.gistemp_i64(
                "SELECT min(assumeNotNull(valid_to)) FROM quant_weather_observation_fact WHERE source_id = 'nasa_gistemp' AND valid_to IS NOT NULL",
            ),
            self.gistemp_u64(
                "SELECT count() FROM quant_weather_observation_fact WHERE source_id = 'nasa_gistemp' AND valid_from IS NULL",
            ),
            self.gistemp_u64(
                "SELECT count() FROM quant_weather_observation_fact WHERE source_id = 'nasa_gistemp' AND valid_to IS NULL",
            ),
        )?;
        Ok(GistempHistoricalTimeRaw {
            row_count,
            earliest_local_date_epoch_days: Some(earliest_local_date_epoch_days),
            earliest_observed_at_ms: Some(earliest_observed_at_ms),
            earliest_valid_from_ms: Some(earliest_valid_from_ms),
            earliest_valid_to_ms: Some(earliest_valid_to_ms),
            null_valid_from_rows,
            null_valid_to_rows,
        })
    }

    async fn evidence_u64(&self, sql: &'static str) -> Result<u64, StorageError> {
        PHASE119_FACT_IDEMPOTENCY
            .clickhouse_query(self.pool.client(), sql)
            .fetch_one::<u64>()
            .await
            .map_err(StorageError::from)
    }

    async fn gistemp_u64(&self, sql: &'static str) -> Result<u64, StorageError> {
        PHASE119_GISTEMP_EVIDENCE
            .clickhouse_query(self.pool.client(), sql)
            .fetch_one::<u64>()
            .await
            .map_err(StorageError::from)
    }

    async fn gistemp_i32(&self, sql: &'static str) -> Result<i32, StorageError> {
        PHASE119_GISTEMP_EVIDENCE
            .clickhouse_query(self.pool.client(), sql)
            .fetch_one::<i32>()
            .await
            .map_err(StorageError::from)
    }

    async fn gistemp_i64(&self, sql: &'static str) -> Result<i64, StorageError> {
        PHASE119_GISTEMP_EVIDENCE
            .clickhouse_query(self.pool.client(), sql)
            .fetch_one::<i64>()
            .await
            .map_err(StorageError::from)
    }
}
