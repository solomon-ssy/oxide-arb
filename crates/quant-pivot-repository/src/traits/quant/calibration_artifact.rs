//! Unified calibration-artifact ledger repository trait (Phase 11.3 §3.4).

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        CalibrationArtifactInfo, CalibrationArtifactListQuery, NewCalibrationArtifact, Paginated,
    },
    types::CalibrationArtifactId,
};

/// Persistence port for the append-only, content-addressed calibration-
/// artifact ledger (`model_score` and `market_price_bias` kinds alike).
#[async_trait::async_trait]
pub trait CalibrationArtifactRepository: Send + Sync {
    /// Insert a new calibration-artifact row, returning the persisted projection.
    async fn create(
        &self,
        artifact: NewCalibrationArtifact,
    ) -> Result<CalibrationArtifactInfo, StorageError>;

    /// Look up a calibration artifact by id.
    async fn find_by_id(
        &self,
        artifact_id: &CalibrationArtifactId,
    ) -> Result<Option<CalibrationArtifactInfo>, StorageError>;

    /// Page the ledger for the operator catalog, newest (`created_at`) first.
    async fn page(
        &self,
        query: CalibrationArtifactListQuery,
    ) -> Result<Paginated<CalibrationArtifactInfo>, StorageError>;

    /// Mark an artifact `active` (idempotent) — recorded when an operator
    /// binds/activates it (bias-table runtime-config ref, or a model
    /// version's `return_model`).
    async fn mark_active(
        &self,
        artifact_id: &CalibrationArtifactId,
    ) -> Result<CalibrationArtifactInfo, StorageError>;
}
