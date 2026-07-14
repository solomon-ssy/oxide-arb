use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        ApplyEntryConditionEvaluation, CryptoPriceProjectionInfo, EntryConditionArtifactInfo,
        EntryConditionAuditInfo, EntryConditionInstanceInfo, NewEntryConditionArtifact,
        NewEntryConditionInstance, WeatherDailyHighProjectionInfo,
    },
    types::{
        DomainInstrumentKey, DomainSourceId, EntryConditionArtifactId, EntryConditionInstanceId,
        RecommendationId,
    },
};
use uuid::Uuid;

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
        local_date: chrono::NaiveDate,
    ) -> Result<Option<WeatherDailyHighProjectionInfo>, StorageError>;

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
        worker_id: Uuid,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> Result<Option<EntryConditionInstanceInfo>, StorageError>;

    async fn renew_lease(
        &self,
        instance_id: &EntryConditionInstanceId,
        worker_id: Uuid,
        lease_epoch: i64,
        lease_expires_at: DateTime<Utc>,
    ) -> Result<bool, StorageError>;

    async fn apply_evaluation(
        &self,
        instance_id: &EntryConditionInstanceId,
        worker_id: Uuid,
        evaluation: ApplyEntryConditionEvaluation,
    ) -> Result<EntryConditionInstanceInfo, StorageError>;

    /// Permanently invalidate a leased instance whose immutable contract can
    /// no longer be verified.
    async fn invalidate(
        &self,
        instance_id: &EntryConditionInstanceId,
        worker_id: Uuid,
        expected_revision: i64,
        expected_lease_epoch: i64,
        detail: String,
        now: DateTime<Utc>,
    ) -> Result<EntryConditionInstanceInfo, StorageError>;
}
