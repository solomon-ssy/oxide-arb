//! Unified calibration-artifact persistence DTOs (Phase 11.3 §3.4).
//!
//! One append-only table backs every empirical calibration artifact: `kind =
//! model_score` (`ProbabilityCalibrator`) and `kind = market_price_bias`
//! (`FavoriteLongshotBiasTable`, folded in from Phase 11.2.1 — no standalone
//! table, no alias). The kind-specific shape lives in `payload_json`; callers
//! branch on `kind` to deserialize the matching payload type.

use crate::{
    entities::quant_calibration_artifact,
    enums::quant::CalibrationKind,
    types::{CalibrationArtifactId, ContentHash},
};
use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

/// Frozen, content-addressed calibration-artifact row.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::quant_calibration_artifact::Entity")]
pub struct CalibrationArtifactInfo {
    pub artifact_id: CalibrationArtifactId,
    pub kind: CalibrationKind,
    pub content_hash: ContentHash,
    pub fit_window_start: DateTime<Utc>,
    pub fit_window_end: DateTime<Utc>,
    pub calibration_split_hash: ContentHash,
    pub sample_count: i64,
    pub payload_json: serde_json::Value,
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
        payload_json,
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
    pub payload_json: serde_json::Value,
    pub active: bool,
}
