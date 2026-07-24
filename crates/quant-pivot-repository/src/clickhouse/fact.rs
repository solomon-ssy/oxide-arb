//! `ClickHouse` fact writers built on the storage `ChWriteManager`.
//!
//! Every fact stream — book ingest rows and quant pipeline rows alike — is
//! persisted here through one durable sink (`ChWriteManager::write_batch`):
//! permit-bounded concurrency, bounded retries with backoff, and per-table
//! Prometheus metrics. Producers buffer through an `AsyncWriter` and flush into
//! these writers, so the hot path never blocks on `ClickHouse`.

use std::{marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use clickhouse::{RowOwned, RowWrite};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::{
        QuantCapitalAllocationEventRow, QuantExecutionEventRow, QuantExitSignalEvaluationEventRow,
        QuantFactorEventRow, QuantFeatureEventRow, QuantFeatureParityEventRow,
        QuantModelInputEventRow, QuantPositionEventRow, QuantReportRecommendationFactRow,
        QuantServingEvidenceCompletionRow, QuantSignalCandidateEventRow, ReportMarketFunnelRow,
    },
    types::ContentHash,
};
use quant_pivot_storage::clickhouse::{ChWriteManager, ClickHousePool};

use crate::traits::{FactWriter, QuantFactRepository};

/// Generic single-table `ClickHouse` fact sink.
///
/// One instance is bound to one target table; cloning is cheap (`Arc` handles).
/// Book fact writers (tick / book-snapshot / l2-replay / microstructure /
/// decision-context) are just `ChFactWriter<Row>` values with distinct tables.
pub struct ChFactWriter<T> {
    pool: Arc<ClickHousePool>,
    write_manager: Arc<ChWriteManager>,
    table: &'static str,
    async_insert: bool,
    _row: PhantomData<fn(T)>,
}

impl<T> ChFactWriter<T> {
    #[must_use]
    pub const fn new(
        pool: Arc<ClickHousePool>,
        write_manager: Arc<ChWriteManager>,
        table: &'static str,
    ) -> Self {
        Self {
            pool,
            write_manager,
            table,
            async_insert: false,
            _row: PhantomData,
        }
    }

    /// Build a writer using server-side async insert with durable acknowledgement.
    #[must_use]
    pub const fn new_async_insert(
        pool: Arc<ClickHousePool>,
        write_manager: Arc<ChWriteManager>,
        table: &'static str,
    ) -> Self {
        Self {
            pool,
            write_manager,
            table,
            async_insert: true,
            _row: PhantomData,
        }
    }
}

#[async_trait]
impl<T> FactWriter<T> for ChFactWriter<T>
where
    T: RowOwned + RowWrite + Send + Sync + 'static,
{
    async fn write_batch(&self, rows: Vec<T>) -> Result<(), StorageError> {
        if self.async_insert {
            self.write_manager
                .write_async_insert_batch_borrowed(self.pool.client(), self.table, &rows)
                .await
        } else {
            self.write_manager
                .write_batch_borrowed(self.pool.client(), self.table, &rows)
                .await
        }
    }

    async fn write_batch_borrowed(&self, rows: &[T]) -> Result<(), StorageError>
    where
        T: Clone,
    {
        if self.async_insert {
            self.write_manager
                .write_async_insert_batch_borrowed(self.pool.client(), self.table, rows)
                .await
        } else {
            self.write_manager
                .write_batch_borrowed(self.pool.client(), self.table, rows)
                .await
        }
    }

    async fn write_batch_idempotent(
        &self,
        deduplication_token: &ContentHash,
        rows: Vec<T>,
    ) -> Result<(), StorageError> {
        let deduplication_token = deduplication_token.to_string();
        self.write_manager
            .write_batch_deduplicated(self.pool.client(), self.table, &deduplication_token, rows)
            .await
    }
}

/// `ClickHouse`-backed implementation of the quant pipeline fact repository.
pub struct ChQuantFactRepository {
    pool: Arc<ClickHousePool>,
    write_manager: Arc<ChWriteManager>,
}

impl ChQuantFactRepository {
    #[must_use]
    pub const fn new(pool: Arc<ClickHousePool>, write_manager: Arc<ChWriteManager>) -> Self {
        Self {
            pool,
            write_manager,
        }
    }
}

#[async_trait]
impl QuantFactRepository for ChQuantFactRepository {
    async fn insert_feature_events(
        &self,
        rows: Vec<QuantFeatureEventRow>,
    ) -> Result<(), StorageError> {
        self.write_manager
            .write_batch(self.pool.client(), "quant_feature_event", rows)
            .await
    }

    async fn insert_factor_events(
        &self,
        rows: Vec<QuantFactorEventRow>,
    ) -> Result<(), StorageError> {
        self.write_manager
            .write_batch(self.pool.client(), "quant_factor_event", rows)
            .await
    }

    async fn insert_model_input_events(
        &self,
        rows: Vec<QuantModelInputEventRow>,
    ) -> Result<(), StorageError> {
        self.write_manager
            .write_batch(self.pool.client(), "quant_model_input_event", rows)
            .await
    }

    async fn insert_serving_evidence_completions(
        &self,
        rows: Vec<QuantServingEvidenceCompletionRow>,
    ) -> Result<(), StorageError> {
        self.write_manager
            .write_batch(
                self.pool.client(),
                "quant_serving_evidence_completion",
                rows,
            )
            .await
    }

    async fn insert_feature_parity_events(
        &self,
        rows: Vec<QuantFeatureParityEventRow>,
    ) -> Result<(), StorageError> {
        self.write_manager
            .write_batch(self.pool.client(), "quant_feature_parity_event", rows)
            .await
    }

    async fn insert_signal_candidate_events(
        &self,
        rows: Vec<QuantSignalCandidateEventRow>,
    ) -> Result<(), StorageError> {
        self.write_manager
            .write_batch(self.pool.client(), "quant_signal_candidate_event", rows)
            .await
    }

    async fn insert_report_recommendation_facts(
        &self,
        rows: Vec<QuantReportRecommendationFactRow>,
    ) -> Result<(), StorageError> {
        self.write_manager
            .write_batch(self.pool.client(), "quant_report_recommendation_fact", rows)
            .await
    }

    async fn insert_report_market_funnel(
        &self,
        rows: Vec<ReportMarketFunnelRow>,
    ) -> Result<(), StorageError> {
        self.write_manager
            .write_batch(self.pool.client(), "quant_report_market_funnel", rows)
            .await
    }

    async fn insert_execution_events(
        &self,
        rows: Vec<QuantExecutionEventRow>,
    ) -> Result<(), StorageError> {
        self.write_manager
            .write_batch(self.pool.client(), "quant_execution_event", rows)
            .await
    }

    async fn insert_capital_allocation_events(
        &self,
        rows: Vec<QuantCapitalAllocationEventRow>,
    ) -> Result<(), StorageError> {
        self.write_manager
            .write_batch(self.pool.client(), "quant_capital_allocation_event", rows)
            .await
    }

    async fn insert_position_events(
        &self,
        rows: Vec<QuantPositionEventRow>,
    ) -> Result<(), StorageError> {
        self.write_manager
            .write_batch(self.pool.client(), "quant_position_event", rows)
            .await
    }

    async fn insert_exit_signal_evaluation_events(
        &self,
        rows: Vec<QuantExitSignalEvaluationEventRow>,
    ) -> Result<(), StorageError> {
        self.write_manager
            .write_batch(
                self.pool.client(),
                "quant_exit_signal_evaluation_event",
                rows,
            )
            .await
    }
}
