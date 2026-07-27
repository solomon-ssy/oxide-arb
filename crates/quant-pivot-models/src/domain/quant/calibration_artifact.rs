//! Unified calibration-artifact persistence DTOs.
//!
//! One append-only table backs every empirical calibration artifact: `kind =
//! model_score` (`ProbabilityCalibrator`), `kind = market_price_bias`
//! (`FavoriteLongshotBiasTable`), and `kind = weather_station_lead_bias`
//! (frozen station × forecast-lead correction). The kind-specific shape is a
//! closed tagged document whose tag must match the relational `kind` column.

use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromJsonQueryResult};
use serde::{Deserialize, Serialize};

use crate::{
    domain::quant::ModelRunInfo,
    entities::quant_calibration_artifact,
    enums::quant::CalibrationKind,
    types::{
        CalibrationArtifactId, ContentHash, ModelRunId,
        calibration::{
            MarketPriceBiasPayload, ModelScoreCalibrationPayload, WeatherStationLeadBiasArtifactV1,
        },
    },
};

/// Closed payload family persisted in `quant_calibration_artifact.payload`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(
    deny_unknown_fields,
    tag = "kind",
    content = "payload",
    rename_all = "snake_case"
)]
pub enum CalibrationArtifactPayload {
    ModelScore(Box<ModelScoreCalibrationPayload>),
    MarketPriceBias(MarketPriceBiasPayload),
    WeatherStationLeadBias(WeatherStationLeadBiasArtifactV1),
}

impl CalibrationArtifactPayload {
    #[must_use]
    pub const fn kind(&self) -> CalibrationKind {
        match self {
            Self::ModelScore(_) => CalibrationKind::ModelScore,
            Self::MarketPriceBias(_) => CalibrationKind::MarketPriceBias,
            Self::WeatherStationLeadBias(_) => CalibrationKind::WeatherStationLeadBias,
        }
    }
}

/// Frozen, content-addressed calibration-artifact row.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_calibration_artifact::Entity")]
pub struct CalibrationArtifactInfo {
    pub artifact_id: CalibrationArtifactId,
    pub kind: CalibrationKind,
    pub content_hash: ContentHash,
    pub fit_window_start: DateTime<Utc>,
    pub fit_window_end: DateTime<Utc>,
    pub calibration_split_hash: ContentHash,
    pub sample_count: i64,
    pub payload: CalibrationArtifactPayload,
    pub active: bool,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    CalibrationArtifactInfo,
    quant_calibration_artifact::Model,
    {
        artifact_id,
        kind,
        content_hash,
        fit_window_start,
        fit_window_end,
        calibration_split_hash,
        sample_count,
        payload,
        active,
        created_at,
    }
);

impl CalibrationArtifactInfo {
    /// Reverify the complete immutable `model_score` payload and self-hash.
    pub fn verify_model_score(&self) -> Result<&ModelScoreCalibrationPayload, String> {
        if self.kind != CalibrationKind::ModelScore
            || self.payload.kind() != CalibrationKind::ModelScore
        {
            return Err("calibration artifact is not a model_score payload".to_owned());
        }
        let CalibrationArtifactPayload::ModelScore(payload) = &self.payload else {
            return Err("calibration kind and payload discriminator differ".to_owned());
        };
        let sample_count = u64::try_from(self.sample_count)
            .map_err(|error| format!("calibration sample_count must be non-negative: {error}"))?;
        payload.validate(sample_count)?;
        let expected = payload.content_hash(
            self.fit_window_start,
            self.fit_window_end,
            &self.calibration_split_hash,
        )?;
        if expected != self.content_hash {
            return Err(format!(
                "model-score calibration hash mismatch: stored {}, recomputed {expected}",
                self.content_hash
            ));
        }
        Ok(payload)
    }
}

/// Insert payload for `quant_calibration_artifact`.
///
/// Covers every `ActiveModel` column except the DB-managed `created_at`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_calibration_artifact::ActiveModel")]
pub struct NewCalibrationArtifact {
    pub artifact_id: CalibrationArtifactId,
    pub kind: CalibrationKind,
    pub content_hash: ContentHash,
    pub fit_window_start: DateTime<Utc>,
    pub fit_window_end: DateTime<Utc>,
    pub calibration_split_hash: ContentHash,
    pub sample_count: i64,
    pub payload: CalibrationArtifactPayload,
    pub active: bool,
}

impl NewCalibrationArtifact {
    /// Reverify a newly fitted `model_score` payload before repository commit.
    pub fn verify_model_score(&self) -> Result<&ModelScoreCalibrationPayload, String> {
        if self.kind != CalibrationKind::ModelScore
            || self.payload.kind() != CalibrationKind::ModelScore
        {
            return Err("calibration artifact is not a model_score payload".to_owned());
        }
        if self.active {
            return Err("model-score calibration must be committed inactive".to_owned());
        }
        let CalibrationArtifactPayload::ModelScore(payload) = &self.payload else {
            return Err("calibration kind and payload discriminator differ".to_owned());
        };
        let sample_count = u64::try_from(self.sample_count)
            .map_err(|error| format!("calibration sample_count must be non-negative: {error}"))?;
        payload.validate(sample_count)?;
        let expected = payload.content_hash(
            self.fit_window_start,
            self.fit_window_end,
            &self.calibration_split_hash,
        )?;
        if expected != self.content_hash {
            return Err(format!(
                "model-score calibration hash mismatch: stored {}, recomputed {expected}",
                self.content_hash
            ));
        }
        Ok(payload)
    }
}

/// One repository-owned atomic append-and-terminal transition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelScoreCalibrationCommit {
    pub model_run_id: ModelRunId,
    pub artifact: NewCalibrationArtifact,
}

/// Idempotent outcome of a canonical model-score artifact commit.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "outcome")]
pub enum ModelScoreCalibrationCommitOutcome {
    Inserted {
        artifact: CalibrationArtifactInfo,
        model_run: ModelRunInfo,
    },
    ExistingExact {
        artifact: CalibrationArtifactInfo,
        model_run: ModelRunInfo,
    },
}

impl ModelScoreCalibrationCommitOutcome {
    #[must_use]
    pub const fn artifact(&self) -> &CalibrationArtifactInfo {
        match self {
            Self::Inserted { artifact, .. } | Self::ExistingExact { artifact, .. } => artifact,
        }
    }

    #[must_use]
    pub const fn model_run(&self) -> &ModelRunInfo {
        match self {
            Self::Inserted { model_run, .. } | Self::ExistingExact { model_run, .. } => model_run,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CalibrationArtifactPayload;

    #[test]
    fn persisted_rejects_unknown_fields() {
        let unknown_kind = serde_json::json!({
            "kind": "custom_calibrator",
            "payload": {}
        });
        assert!(serde_json::from_value::<CalibrationArtifactPayload>(unknown_kind).is_err());

        let unknown_field = serde_json::json!({
            "kind": "market_price_bias",
            "payload": { "by_category": {}, "unexpected": true }
        });
        assert!(serde_json::from_value::<CalibrationArtifactPayload>(unknown_field).is_err());
    }
}
