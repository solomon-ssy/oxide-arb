//! Immutable model-version derivation lineage.

use quant_pivot_error::hashing::CanonicalDigestError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    enums::quant::ModelVersionDerivationKind,
    hashing::CanonicalDigest,
    types::{
        BacktestReportId, CalibrationArtifactId, ContentHash, ModelVersionId,
        calibration::ScoreMultiplierCalibrationReport,
    },
};

const MODEL_VERSION_DERIVATION_SCHEMA_VERSION: u32 = 1;
const MODEL_VERSION_DERIVATION_HASH_DOMAIN: &str = "quant-pivot/model-version-derivation";

/// Caller-facing derivation command for a new immutable model version.
///
/// IDs that need referential integrity are decomposed into native FK columns by
/// the persistence DTO. Only the atomic score-calibration evidence stays JSONB.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum ModelVersionDerivation {
    Training,
    ScoreMultiplierCalibration {
        parent_model_version_id: ModelVersionId,
        source_backtest_report_id: BacktestReportId,
        report: ScoreMultiplierCalibrationReport,
    },
    ReturnCalibration {
        parent_model_version_id: ModelVersionId,
        calibration_artifact_id: CalibrationArtifactId,
    },
}

impl ModelVersionDerivation {
    /// Reconstruct and verify one persisted, FK-backed lineage projection.
    pub fn from_persistence(
        kind: ModelVersionDerivationKind,
        parent_model_version_id: Option<ModelVersionId>,
        source_backtest_report_id: Option<BacktestReportId>,
        calibration_artifact_id: Option<CalibrationArtifactId>,
        report: Option<ScoreMultiplierCalibrationReport>,
        stored_evidence_hash: Option<ContentHash>,
    ) -> Result<Self, ModelVersionDerivationError> {
        let invalid_shape = || ModelVersionDerivationError::InvalidShape {
            kind,
            detail: "relational columns do not match the derivation kind".to_owned(),
        };
        let derivation = match kind {
            ModelVersionDerivationKind::Training => {
                if parent_model_version_id.is_some()
                    || source_backtest_report_id.is_some()
                    || calibration_artifact_id.is_some()
                    || report.is_some()
                    || stored_evidence_hash.is_some()
                {
                    return Err(invalid_shape());
                }
                Self::Training
            }
            ModelVersionDerivationKind::ScoreMultiplierCalibration => {
                let (Some(parent_model_version_id), Some(source_backtest_report_id), Some(report)) =
                    (parent_model_version_id, source_backtest_report_id, report)
                else {
                    return Err(invalid_shape());
                };
                if calibration_artifact_id.is_some() || stored_evidence_hash.is_none() {
                    return Err(invalid_shape());
                }
                Self::ScoreMultiplierCalibration {
                    parent_model_version_id,
                    source_backtest_report_id,
                    report,
                }
            }
            ModelVersionDerivationKind::ReturnCalibration => {
                let (Some(parent_model_version_id), Some(calibration_artifact_id)) =
                    (parent_model_version_id, calibration_artifact_id)
                else {
                    return Err(invalid_shape());
                };
                if source_backtest_report_id.is_some()
                    || report.is_some()
                    || stored_evidence_hash.is_none()
                {
                    return Err(invalid_shape());
                }
                Self::ReturnCalibration {
                    parent_model_version_id,
                    calibration_artifact_id,
                }
            }
        };
        let expected = derivation.evidence_hash()?;
        if expected != stored_evidence_hash {
            return Err(ModelVersionDerivationError::EvidenceHashMismatch {
                expected,
                actual: stored_evidence_hash,
            });
        }
        Ok(derivation)
    }

    #[must_use]
    pub const fn kind(&self) -> ModelVersionDerivationKind {
        match self {
            Self::Training => ModelVersionDerivationKind::Training,
            Self::ScoreMultiplierCalibration { .. } => {
                ModelVersionDerivationKind::ScoreMultiplierCalibration
            }
            Self::ReturnCalibration { .. } => ModelVersionDerivationKind::ReturnCalibration,
        }
    }

    #[must_use]
    pub const fn parent_model_version_id(&self) -> Option<&ModelVersionId> {
        match self {
            Self::Training => None,
            Self::ScoreMultiplierCalibration {
                parent_model_version_id,
                ..
            }
            | Self::ReturnCalibration {
                parent_model_version_id,
                ..
            } => Some(parent_model_version_id),
        }
    }

    #[must_use]
    pub const fn source_backtest_report_id(&self) -> Option<&BacktestReportId> {
        match self {
            Self::ScoreMultiplierCalibration {
                source_backtest_report_id,
                ..
            } => Some(source_backtest_report_id),
            Self::Training | Self::ReturnCalibration { .. } => None,
        }
    }

    #[must_use]
    pub const fn calibration_artifact_id(&self) -> Option<&CalibrationArtifactId> {
        match self {
            Self::ReturnCalibration {
                calibration_artifact_id,
                ..
            } => Some(calibration_artifact_id),
            Self::Training | Self::ScoreMultiplierCalibration { .. } => None,
        }
    }

    #[must_use]
    pub const fn score_multiplier_report(&self) -> Option<&ScoreMultiplierCalibrationReport> {
        match self {
            Self::ScoreMultiplierCalibration { report, .. } => Some(report),
            Self::Training | Self::ReturnCalibration { .. } => None,
        }
    }

    /// Validate the closed derivation evidence before persistence.
    pub fn validate(&self) -> Result<(), ModelVersionDerivationError> {
        if let Self::ScoreMultiplierCalibration { report, .. } = self {
            report
                .validate()
                .map_err(ModelVersionDerivationError::InvalidReport)?;
        }
        Ok(())
    }

    /// Canonical hash of derived-version evidence. A root training version has
    /// no separate evidence document; its dataset/run/artifact relations are
    /// already first-class columns and ledgers.
    pub fn evidence_hash(&self) -> Result<Option<ContentHash>, ModelVersionDerivationError> {
        self.validate()?;
        match self {
            Self::Training => Ok(None),
            Self::ScoreMultiplierCalibration { .. } | Self::ReturnCalibration { .. } => {
                Ok(Some(CanonicalDigest::content_hash_typed(
                    MODEL_VERSION_DERIVATION_HASH_DOMAIN,
                    MODEL_VERSION_DERIVATION_SCHEMA_VERSION,
                    self,
                )?))
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum ModelVersionDerivationError {
    #[error("invalid {kind} model-version derivation: {detail}")]
    InvalidShape {
        kind: ModelVersionDerivationKind,
        detail: String,
    },
    #[error("invalid score-multiplier calibration report: {0}")]
    InvalidReport(String),
    #[error(
        "model-version derivation evidence hash mismatch: expected {expected:?}, got {actual:?}"
    )]
    EvidenceHashMismatch {
        expected: Option<ContentHash>,
        actual: Option<ContentHash>,
    },
    #[error(transparent)]
    Hash(#[from] CanonicalDigestError),
}

#[cfg(test)]
mod tests {
    use super::ModelVersionDerivation;
    use crate::{
        enums::quant::ModelVersionDerivationKind,
        types::{CalibrationArtifactId, ContentHash, ModelVersionId},
    };

    #[test]
    fn return_calibration_lineage_round_trips_with_hash_verification() {
        let parent = ModelVersionId::from_v7();
        let artifact = CalibrationArtifactId::from_v7();
        let derivation = ModelVersionDerivation::ReturnCalibration {
            parent_model_version_id: parent,
            calibration_artifact_id: artifact,
        };
        let hash = derivation.evidence_hash().expect("lineage hash");
        let restored = ModelVersionDerivation::from_persistence(
            ModelVersionDerivationKind::ReturnCalibration,
            Some(parent),
            None,
            Some(artifact),
            None,
            hash,
        )
        .expect("verified lineage");
        assert_eq!(restored, derivation);
    }

    #[test]
    fn return_calibration_lineage_rejects_tampered_hash() {
        let derivation = ModelVersionDerivation::ReturnCalibration {
            parent_model_version_id: ModelVersionId::from_v7(),
            calibration_artifact_id: CalibrationArtifactId::from_v7(),
        };
        let error = ModelVersionDerivation::from_persistence(
            derivation.kind(),
            derivation.parent_model_version_id().copied(),
            None,
            derivation.calibration_artifact_id().copied(),
            None,
            Some(
                ContentHash::parse(&format!("blake3:{}", "0".repeat(64))).expect("canonical hash"),
            ),
        )
        .expect_err("tampered hash");
        assert!(error.to_string().contains("hash mismatch"));
    }

    #[test]
    fn training_lineage_rejects_hidden_foreign_keys() {
        assert!(
            ModelVersionDerivation::from_persistence(
                ModelVersionDerivationKind::Training,
                Some(ModelVersionId::from_v7()),
                None,
                None,
                None,
                None,
            )
            .is_err()
        );
    }
}
