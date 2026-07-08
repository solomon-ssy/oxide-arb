//! [`CoreCalibrationArtifactLoader`]: resolves `model_score` calibration artifacts.
//!
//! Loads a [`CalibrationArtifactId`] into compute-domain [`ResolvedCalibration`] for
//! `quant-pivot-research`'s `DefaultModelRuntimeFactory` (Phase 11.3 §5).
//!
//! Implements the research-crate-owned [`CalibrationArtifactLoader`] port over
//! the persistence-crate `CalibrationArtifactRepository` — the same
//! dependency-inversion shape as [`crate::artifact`]'s `ArtifactStore` for
//! model bytes, keeping `quant-pivot-research` free of any persistence-crate
//! dependency.

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{enums::quant::CalibrationKind, types::CalibrationArtifactId};
use quant_pivot_repository::traits::CalibrationArtifactRepository;
use quant_pivot_research::model::{
    CalibrationArtifactLoader, MonotoneMapping, ResolvedCalibration,
};

/// Payload shape stored in `quant_calibration_artifact.payload_json` for
/// `kind = model_score` rows.
///
/// Mirrors [`crate::service::model_calibration_fit`]'s persist step — the single
/// source of truth for this JSON shape.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ModelScoreCalibrationPayload {
    pub mapping: MonotoneMapping,
    pub reliability: quant_pivot_research::model::ReliabilityReport,
}

/// Loads and validates `model_score` calibration artifacts.
pub struct CoreCalibrationArtifactLoader {
    repo: Arc<dyn CalibrationArtifactRepository>,
}

impl CoreCalibrationArtifactLoader {
    #[must_use]
    pub const fn new(repo: Arc<dyn CalibrationArtifactRepository>) -> Self {
        Self { repo }
    }
}

#[async_trait]
impl CalibrationArtifactLoader for CoreCalibrationArtifactLoader {
    async fn load(&self, artifact_id: &CalibrationArtifactId) -> QuantResult<ResolvedCalibration> {
        let info = self
            .repo
            .find_by_id(artifact_id)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| {
                QuantError::from(ResearchError::DatasetBuild {
                    detail: format!(
                        "calibration artifact `{artifact_id}` not found — a `Calibrated` \
                         return model must never load with a missing calibrator"
                    ),
                })
            })?;
        if info.kind != CalibrationKind::ModelScore {
            return Err(QuantError::from(ResearchError::DatasetBuild {
                detail: format!(
                    "calibration artifact `{artifact_id}` is kind `{}`, expected `model_score`",
                    info.kind.as_str()
                ),
            }));
        }
        let payload: ModelScoreCalibrationPayload = serde_json::from_value(info.payload_json)
            .map_err(|error| {
                QuantError::from(ResearchError::DatasetBuild {
                    detail: format!(
                        "calibration artifact `{artifact_id}` payload deserialization failed: {error}"
                    ),
                })
            })?;
        Ok(ResolvedCalibration {
            artifact_id: artifact_id.clone(),
            mapping: payload.mapping,
            reliability: payload.reliability,
        })
    }
}
