//! Unified calibration-artifact ledger repository trait.

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        api::CalibrationArtifactListQuery,
        pagination::Paginated,
        quant::{
            CalibrationArtifactInfo, ModelScoreCalibrationCommit,
            ModelScoreCalibrationCommitOutcome, NewCalibrationArtifact,
        },
    },
    types::{CalibrationArtifactId, ContentHash, calibration::PublishedWeatherStationLeadBias},
};

/// Persistence port for the append-only, content-addressed calibration-
/// artifact ledger (model score, market-price bias, and Weather lead bias).
#[async_trait::async_trait]
pub trait CalibrationArtifactRepository: Send + Sync {
    /// Insert a new calibration-artifact row, returning the persisted projection.
    async fn create(
        &self,
        artifact: NewCalibrationArtifact,
    ) -> Result<CalibrationArtifactInfo, StorageError>;

    /// Atomically append one canonical `model_score` artifact and transition
    /// its locked `Running` Calibration run to `Succeeded` with the artifact
    /// hash. Exact retries return `ExistingExact`; every other collision fails
    /// closed and rolls back.
    async fn commit_model_score(
        &self,
        commit: ModelScoreCalibrationCommit,
    ) -> Result<ModelScoreCalibrationCommitOutcome, StorageError>;

    /// Look up a calibration artifact by id.
    async fn find_by_id(
        &self,
        artifact_id: &CalibrationArtifactId,
    ) -> Result<Option<CalibrationArtifactInfo>, StorageError>;

    /// Resolve an artifact after an idempotent content-addressed create race.
    async fn find_by_content_hash(
        &self,
        content_hash: &ContentHash,
    ) -> Result<Option<CalibrationArtifactInfo>, StorageError>;

    /// Page the ledger for the operator catalog, newest (`created_at`) first.
    async fn page(
        &self,
        query: CalibrationArtifactListQuery,
    ) -> Result<Paginated<CalibrationArtifactInfo>, StorageError>;

    /// Immutable Weather calibration publications visible at or before `at`.
    async fn published_weather_through(
        &self,
        at: DateTime<Utc>,
    ) -> Result<Vec<PublishedWeatherStationLeadBias>, StorageError>;

    /// Mark an artifact `active` (idempotent) — recorded when an operator
    /// binds/activates it (bias-table runtime-config ref, or a model
    /// version's `return_model`).
    ///
    /// `market_price_bias` is single-active: every other `market_price_bias`
    /// row is deactivated in the same transaction, since it is referenced by
    /// exactly one global runtime-config pointer. `model_score` has no such
    /// exclusivity — immutable model versions used by different route
    /// generations may each bind (and keep active) a different calibrator.
    async fn mark_active(
        &self,
        artifact_id: &CalibrationArtifactId,
    ) -> Result<CalibrationArtifactInfo, StorageError>;
}
