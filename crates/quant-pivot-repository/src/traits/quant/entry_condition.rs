use chrono::{DateTime, NaiveDate, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::EntryConditionEvaluationEventRow,
    domain::quant::{
        ApplyEntryConditionEvaluation, ApplyEntryConditionEvaluationOutcome,
        CryptoPriceProjectionInfo, EntryConditionArtifactInfo, EntryConditionAuditInfo,
        EntryConditionInstanceInfo, NewEntryConditionArtifact, NewEntryConditionInstance,
        WeatherDailyTemperatureProjectionInfo,
    },
    types::{
        ContentHash, DomainInstrumentKey, DomainSourceId, EntryConditionArtifactId,
        EntryConditionInstanceId, RecommendationId, WeatherTemperatureStatistic, WorkerId,
    },
};

/// Persistence boundary for recommendation-level condition state.
#[async_trait::async_trait]
pub trait EntryConditionRepository: Send + Sync {
    async fn insert_artifact(
        &self,
        artifact: NewEntryConditionArtifact,
    ) -> Result<EntryConditionArtifactInfo, StorageError>;

    async fn create_instance(
        &self,
        instance: NewEntryConditionInstance,
        now: DateTime<Utc>,
    ) -> Result<EntryConditionInstanceInfo, StorageError>;

    async fn find_artifact(
        &self,
        artifact_id: &EntryConditionArtifactId,
    ) -> Result<Option<EntryConditionArtifactInfo>, StorageError>;

    async fn find_instance(
        &self,
        instance_id: &EntryConditionInstanceId,
    ) -> Result<Option<EntryConditionInstanceInfo>, StorageError>;

    async fn find_by_recommendation(
        &self,
        recommendation_id: &RecommendationId,
    ) -> Result<Option<EntryConditionInstanceInfo>, StorageError>;

    async fn audits(
        &self,
        instance_id: &EntryConditionInstanceId,
    ) -> Result<Vec<EntryConditionAuditInfo>, StorageError>;

    async fn find_crypto_projection(
        &self,
        source_id: &DomainSourceId,
        instrument_key: &DomainInstrumentKey,
    ) -> Result<Option<CryptoPriceProjectionInfo>, StorageError>;

    async fn find_weather_projection(
        &self,
        source_id: &DomainSourceId,
        instrument_key: &DomainInstrumentKey,
        station: &str,
        local_date: NaiveDate,
        temperature_statistic: WeatherTemperatureStatistic,
    ) -> Result<Option<WeatherDailyTemperatureProjectionInfo>, StorageError>;

    /// Atomically expire due active instances and append one audit per transition.
    async fn expire_due(
        &self,
        now: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<EntryConditionInstanceInfo>, StorageError>;

    /// Earliest persisted evaluation/expiry deadline among active instances.
    /// Used only as a latency wake; lease and expiry queries remain authoritative.
    async fn next_wakeup_at(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Option<DateTime<Utc>>, StorageError>;

    /// Lease one due instance using `FOR UPDATE SKIP LOCKED`.
    async fn lease_next(
        &self,
        worker_id: WorkerId,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> Result<Option<EntryConditionInstanceInfo>, StorageError>;

    async fn renew_lease(
        &self,
        instance_id: &EntryConditionInstanceId,
        worker_id: WorkerId,
        lease_epoch: i64,
        lease_expires_at: DateTime<Utc>,
    ) -> Result<bool, StorageError>;

    async fn apply_evaluation(
        &self,
        instance_id: &EntryConditionInstanceId,
        worker_id: WorkerId,
        evaluation: ApplyEntryConditionEvaluation,
    ) -> Result<ApplyEntryConditionEvaluationOutcome, StorageError>;

    async fn claim_pending_evaluations(
        &self,
        worker_id: WorkerId,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<EntryConditionEvaluationEventRow>, StorageError>;

    async fn mark_evaluation_published(
        &self,
        evaluation_id: &ContentHash,
        worker_id: WorkerId,
        published_at: DateTime<Utc>,
    ) -> Result<(), StorageError>;

    async fn mark_evaluation_failed(
        &self,
        evaluation_id: &ContentHash,
        worker_id: WorkerId,
        detail: String,
    ) -> Result<(), StorageError>;

    /// Permanently invalidate a leased instance whose immutable contract can
    /// no longer be verified.
    async fn invalidate(
        &self,
        instance_id: &EntryConditionInstanceId,
        worker_id: WorkerId,
        expected_revision: i64,
        expected_lease_epoch: i64,
        detail: String,
        now: DateTime<Utc>,
    ) -> Result<EntryConditionInstanceInfo, StorageError>;
}
