//! Persistence boundaries for parity runs, governed latch state, and CH evidence.

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::{
        QuantFeatureEventRow, QuantModelInputEventRow, QuantServingEvidenceCompletionRow,
    },
    domain::{
        api::{
            FeatureIntegrityCounts, FeatureParityEventListQuery, FeatureParityEventView,
            FeatureParityRunListQuery,
        },
        pagination::Paginated,
        quant::{
            CompleteFeatureParityRun, FeatureParityRunInfo, FeatureParityStateInfo,
            FrozenFeatureParitySubject, NewFeatureParityRun, NewFrozenModelParitySubject,
            NewResearchJob, ResearchJobInfo,
        },
    },
    enums::quant::FeatureParityRunKind,
    types::{
        FeatureParityRunId, FeatureVectorId, ModelRunId, ModelVersionId, RecommendationReportId,
        TrainingDatasetId,
    },
};

/// Actor provenance for an append-only latch transition.
#[derive(Debug, Clone)]
pub struct FeatureParityLatchActor {
    pub actor: Option<String>,
    pub acting_role: String,
    pub reason: String,
}

/// Atomic result of freezing a full serving window and publishing its job.
#[derive(Debug, Clone)]
pub enum EnqueueFrozenFeatureParityOutcome {
    NotEligible,
    Enqueued {
        run: Box<FeatureParityRunInfo>,
        job: Box<ResearchJobInfo>,
    },
}

/// Postgres parity-run and latch ledger.
#[async_trait::async_trait]
pub trait FeatureParityRepository: Send + Sync {
    /// Return the `PostgreSQL` clock used by parity state transitions.
    async fn database_time(&self) -> Result<DateTime<Utc>, StorageError>;

    async fn create_run(
        &self,
        run: NewFeatureParityRun,
    ) -> Result<FeatureParityRunInfo, StorageError>;

    /// Atomically bind an offline full proof to the exact model artifact and
    /// training-dataset generation before verification begins.
    async fn create_frozen_model_run(
        &self,
        run: NewFeatureParityRun,
        subject: NewFrozenModelParitySubject,
    ) -> Result<FeatureParityRunInfo, StorageError>;

    /// Atomically create the parity run and its durable `feature_parity` job.
    async fn enqueue_run(
        &self,
        run: NewFeatureParityRun,
        job: NewResearchJob,
    ) -> Result<(FeatureParityRunInfo, ResearchJobInfo), StorageError>;

    /// In one transaction, enumerate the exact serving window, freeze every
    /// subject and market membership, then insert the run/job. No ledger row is
    /// created when the window contains no serving subject.
    async fn enqueue_frozen_full(
        &self,
        run: NewFeatureParityRun,
        job: NewResearchJob,
    ) -> Result<EnqueueFrozenFeatureParityOutcome, StorageError>;

    /// Load only the immutable subjects bound to this run. Executors must not
    /// rediscover live subjects from a time-window query.
    async fn load_frozen_subjects(
        &self,
        run_id: &FeatureParityRunId,
    ) -> Result<Vec<FrozenFeatureParitySubject>, StorageError>;

    async fn find_run(
        &self,
        run_id: &FeatureParityRunId,
    ) -> Result<Option<FeatureParityRunInfo>, StorageError>;

    async fn page_runs(
        &self,
        query: FeatureParityRunListQuery,
    ) -> Result<Paginated<FeatureParityRunInfo>, StorageError>;

    async fn latest_run(
        &self,
        kind: FeatureParityRunKind,
    ) -> Result<Option<FeatureParityRunInfo>, StorageError>;

    /// Latest unbound runtime full replay. Subject-bound dataset/model proofs
    /// must never suppress the 24-hour serving replay scheduler or replace its
    /// top-level integrity summary.
    async fn latest_unbound_full(&self) -> Result<Option<FeatureParityRunInfo>, StorageError> {
        Ok(None)
    }

    /// Exact active full-run window lookup used to make the automatic 24-hour
    /// scheduler idempotent across process restarts and multiple app replicas.
    /// Terminal runs are immutable history and must not block a governed replay.
    async fn find_full_window(
        &self,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> Result<Option<FeatureParityRunInfo>, StorageError>;

    /// The unique sampled parity run atomically committed with a serving report.
    async fn find_sampled_report(
        &self,
        report_id: &RecommendationReportId,
    ) -> Result<Option<FeatureParityRunInfo>, StorageError>;

    /// Most recent full frozen-artifact verification for one immutable model
    /// version and its exact training dataset.
    async fn latest_full_for_model(
        &self,
        _model_version_id: &ModelVersionId,
        _training_dataset_id: &TrainingDatasetId,
    ) -> Result<Option<FeatureParityRunInfo>, StorageError> {
        Ok(None)
    }

    async fn mark_running(
        &self,
        run_id: &FeatureParityRunId,
    ) -> Result<FeatureParityRunInfo, StorageError>;

    async fn complete_run(
        &self,
        run_id: &FeatureParityRunId,
        result: CompleteFeatureParityRun,
    ) -> Result<FeatureParityRunInfo, StorageError>;

    /// Mark fail-closed report/intent containment complete. Only a terminal
    /// mismatched/failed run may receive this idempotent acknowledgement.
    async fn mark_containment_complete(
        &self,
        run_id: &FeatureParityRunId,
    ) -> Result<FeatureParityRunInfo, StorageError>;

    async fn current_state(&self) -> Result<Option<FeatureParityStateInfo>, StorageError>;

    /// Atomically append a failed integrity incident derived from an existing
    /// parity proof and open the global latch. Used when governance cannot
    /// restore consistent registry/live/durable state after a model switch.
    async fn record_integrity_failure(
        &self,
        _source_run_id: &FeatureParityRunId,
        _reason: String,
    ) -> Result<(FeatureParityRunInfo, FeatureParityStateInfo), StorageError> {
        Err(StorageError::invariant_violation(
            Some("quant_feature_parity_state"),
            "feature parity repository cannot record governance integrity failures",
        ))
    }

    /// Clear the latch only after a newer successful full replay, under a DB lock.
    async fn acknowledge_latch(
        &self,
        recovery_run_id: &FeatureParityRunId,
        actor: FeatureParityLatchActor,
    ) -> Result<FeatureParityStateInfo, StorageError>;
}

/// `ClickHouse` row-level parity evidence read surface.
#[async_trait::async_trait]
pub trait FeatureParityEventRepository: Send + Sync {
    async fn page_events(
        &self,
        query: FeatureParityEventListQuery,
    ) -> Result<Paginated<FeatureParityEventView>, StorageError>;

    async fn summary_counts(&self) -> Result<FeatureIntegrityCounts, StorageError>;
}

/// Durable online evidence consumed by deterministic parity replay.
///
/// These reads expose only exact serving facts. Re-materialization remains in
/// core so the replay side cannot accidentally read its own online projection.
#[async_trait::async_trait]
pub trait ServingEvidenceRepository: Send + Sync {
    /// Run-scoped durable completion markers. Missing markers mean the writer
    /// has not completed that run, irrespective of newer facts in either
    /// serving stream.
    async fn completions_for_runs(
        &self,
        model_run_ids: &[ModelRunId],
    ) -> Result<Vec<QuantServingEvidenceCompletionRow>, StorageError>;

    async fn model_inputs_for_runs(
        &self,
        model_run_ids: &[ModelRunId],
    ) -> Result<Vec<QuantModelInputEventRow>, StorageError>;

    async fn feature_cells_for_vectors(
        &self,
        feature_vector_ids: &[FeatureVectorId],
    ) -> Result<Vec<QuantFeatureEventRow>, StorageError>;
}
