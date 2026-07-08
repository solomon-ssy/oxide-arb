//! Unified calibration-artifact admin HTTP contract (Phase 11.3 §3.4).
//!
//! Read surface for the content-addressed `CalibrationArtifact` catalog
//! (both `kind`s) plus the governed mutations: fitting a new `market_price_bias`
//! table (async research job, unchanged from Phase 11.2.1), fitting a new
//! `model_score` calibrator (async research job, new), activating a bias table
//! into `factors.structural.favorite_longshot.bias_table_ref`, and binding a
//! `model_score` calibrator to a model version's return model (routed with
//! model governance — see `BindCalibrationRequest`).

use chrono::{DateTime, Utc};
use quant_pivot_macros::NormalizePageQuery;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::{
    domain::{CalibrationArtifactInfo, pagination::PageRequest},
    enums::quant::{CalibrationKind, CalibrationMethod, DownsideSource},
    types::{CalibrationArtifactId, ContentHash, ModelVersionId, TrainingDatasetId},
};

/// Inbound body for `POST /research/calibration-artifacts/fit-bias-table`.
///
/// The fit window bounds the settlement spine sampled from `ClickHouse`; the
/// governed `reason` is recorded on the operation log. `Serialize` is derived so
/// the request can be frozen into the durable research job's `params_json`.
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[validate(schema(function = "validate_fit_bias_table_request"))]
pub struct FitBiasTableRequest {
    /// Inclusive lower bound of the fit sample window.
    pub window_start: DateTime<Utc>,
    /// Exclusive upper bound of the fit sample window.
    pub window_end: DateTime<Utc>,
    /// Operator reason recorded on the operation log.
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
}

crate::half_open_window_request!(FitBiasTableRequest);

/// Inbound body for `POST /research/calibration-artifacts/fit-model-calibrator`.
///
/// `calibration_dataset_id` must reference a `Built`/`Ready`
/// `purpose = calibration` `TrainingDataset` whose window is disjoint and
/// embargoed relative to `model_version_id`'s own training-dataset window
/// (enforced server-side, fail-closed — Phase 11.3 §4).
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct FitModelCalibratorRequest {
    /// The candidate model version to calibrate.
    pub model_version_id: ModelVersionId,
    /// The independent held-out calibration dataset to fit on.
    pub calibration_dataset_id: TrainingDatasetId,
    /// Isotonic (>= `min_samples_isotonic`) or Platt (small-sample / sigmoid).
    pub method: CalibrationMethod,
    /// Operator reason recorded on the operation log.
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
}

/// Inbound body for `POST /research/calibration-artifacts/{id}/activate`
/// (`market_price_bias` artifacts only).
///
/// Activation stages a new runtime-config version whose
/// `factors.structural.favorite_longshot.bias_table_ref` points at this table.
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct ActivateCalibrationArtifactRequest {
    /// Operator reason recorded on the operation log and config activation.
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
}

/// Inbound body for `POST /research/models/{model_version_id}/bind-calibration`
/// (`model_score` artifacts only — routed with model governance).
///
/// Creates a new candidate model version whose `return_model` is
/// `Calibrated { calibrator_ref, downside_source }` (Phase 11.3 §5).
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct BindCalibrationRequest {
    /// The `model_score` calibration artifact to bind.
    pub calibrator_ref: CalibrationArtifactId,
    /// Downside (bps) source for the derived return estimate.
    pub downside_source: DownsideSource,
    /// Operator reason recorded on the operation log.
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
}

/// Paginated filter for the append-only calibration-artifact ledger catalog.
#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct CalibrationArtifactListQuery {
    /// Filter by artifact family; `None` returns both kinds.
    pub kind: Option<CalibrationKind>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

/// Calibration-artifact summary row for the unified catalog grid (no payload).
#[derive(Debug, Clone, Serialize)]
pub struct CalibrationArtifactSummaryView {
    pub artifact_id: CalibrationArtifactId,
    pub kind: CalibrationKind,
    pub content_hash: ContentHash,
    pub fit_window_start: DateTime<Utc>,
    pub fit_window_end: DateTime<Utc>,
    pub sample_count: i64,
    pub active: bool,
    pub created_at: DateTime<Utc>,
}

impl From<CalibrationArtifactInfo> for CalibrationArtifactSummaryView {
    fn from(info: CalibrationArtifactInfo) -> Self {
        Self {
            artifact_id: info.artifact_id,
            kind: info.kind,
            content_hash: info.content_hash,
            fit_window_start: info.fit_window_start,
            fit_window_end: info.fit_window_end,
            sample_count: info.sample_count,
            active: info.active,
            created_at: info.created_at,
        }
    }
}

/// Full calibration-artifact detail: provenance plus the kind-specific payload.
///
/// Rendered client-side per `kind` — `market_price_bias` mirrors the former
/// `by_category` shape; `model_score` carries the monotone map + reliability
/// report.
#[derive(Debug, Clone, Serialize)]
pub struct CalibrationArtifactDetailView {
    pub artifact_id: CalibrationArtifactId,
    pub kind: CalibrationKind,
    pub content_hash: ContentHash,
    pub fit_window_start: DateTime<Utc>,
    pub fit_window_end: DateTime<Utc>,
    pub calibration_split_hash: ContentHash,
    pub sample_count: i64,
    pub active: bool,
    pub created_at: DateTime<Utc>,
    pub payload_json: serde_json::Value,
}

impl From<CalibrationArtifactInfo> for CalibrationArtifactDetailView {
    fn from(info: CalibrationArtifactInfo) -> Self {
        Self {
            artifact_id: info.artifact_id,
            kind: info.kind,
            content_hash: info.content_hash,
            fit_window_start: info.fit_window_start,
            fit_window_end: info.fit_window_end,
            calibration_split_hash: info.calibration_split_hash,
            sample_count: info.sample_count,
            active: info.active,
            created_at: info.created_at,
            payload_json: info.payload_json,
        }
    }
}
