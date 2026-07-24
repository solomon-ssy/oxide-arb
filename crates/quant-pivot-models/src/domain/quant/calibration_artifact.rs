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
    entities::quant_calibration_artifact,
    enums::quant::CalibrationKind,
    types::{
        CalibrationArtifactId, ContentHash,
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
    ModelScore(ModelScoreCalibrationPayload),
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
