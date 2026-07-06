//! Favorite-longshot bias-table admin HTTP contract (Phase 11.2.1).
//!
//! Read surface for the content-addressed bias-table artifacts plus the governed
//! mutations that fit a new table (async research job) and activate one as the
//! `factors.structural.favorite_longshot.bias_table_ref` a runtime-config
//! version consumes.

use chrono::{DateTime, Utc};
use quant_pivot_macros::NormalizePageQuery;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::{
    domain::{FavoriteLongshotBiasTableInfo, pagination::PageRequest},
    types::{ContentHash, FavoriteLongshotBiasTableId},
};

/// Inbound body for `POST /research/bias-tables/fit`.
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

/// Inbound body for `POST /research/bias-tables/{id}/activate`.
///
/// Activation stages a new runtime-config version whose
/// `factors.structural.favorite_longshot.bias_table_ref` points at this table.
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct ActivateBiasTableRequest {
    /// Operator reason recorded on the operation log and config activation.
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
}

/// Paginated filter for the append-only bias-table ledger catalog.
#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct BiasTableListQuery {
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

/// Bias-table summary row for the catalog grid (no per-bin payload).
#[derive(Debug, Clone, Serialize)]
pub struct BiasTableSummaryView {
    pub bias_table_id: FavoriteLongshotBiasTableId,
    pub content_hash: ContentHash,
    pub fit_window_start: DateTime<Utc>,
    pub fit_window_end: DateTime<Utc>,
    pub category_count: i64,
    pub total_sample_count: i64,
    pub created_at: DateTime<Utc>,
}

impl From<FavoriteLongshotBiasTableInfo> for BiasTableSummaryView {
    fn from(info: FavoriteLongshotBiasTableInfo) -> Self {
        Self {
            bias_table_id: info.bias_table_id,
            content_hash: info.content_hash,
            fit_window_start: info.fit_window_start,
            fit_window_end: info.fit_window_end,
            category_count: info.category_count,
            total_sample_count: info.total_sample_count,
            created_at: info.created_at,
        }
    }
}

/// One `(category, price_bucket)` empirical-bias record (wire mirror of the
/// research `PriceBiasBin`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceBiasBinView {
    pub price_lo: Decimal,
    pub price_hi: Decimal,
    pub implied_mid: Decimal,
    pub realized_frequency: Decimal,
    pub bias: Decimal,
    pub bias_ci: (Decimal, Decimal),
    pub sample_count: u64,
}

/// A per-category empirical-bias curve (wire mirror of the research
/// `CategoryBiasCurve`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoryBiasCurveView {
    pub bins: Vec<PriceBiasBinView>,
    pub ic: Decimal,
    pub ic_significant: bool,
    pub sample_count: u64,
}

/// Full bias-table detail: provenance plus the per-category curves.
#[derive(Debug, Clone, Serialize)]
pub struct BiasTableDetailView {
    pub bias_table_id: FavoriteLongshotBiasTableId,
    pub content_hash: ContentHash,
    pub fit_window_start: DateTime<Utc>,
    pub fit_window_end: DateTime<Utc>,
    pub calibration_split_hash: ContentHash,
    pub category_count: i64,
    pub total_sample_count: i64,
    pub created_at: DateTime<Utc>,
    /// Per-category curves keyed by market-category wire slug.
    pub by_category: serde_json::Value,
}

impl From<FavoriteLongshotBiasTableInfo> for BiasTableDetailView {
    fn from(info: FavoriteLongshotBiasTableInfo) -> Self {
        Self {
            bias_table_id: info.bias_table_id,
            content_hash: info.content_hash,
            fit_window_start: info.fit_window_start,
            fit_window_end: info.fit_window_end,
            calibration_split_hash: info.calibration_split_hash,
            category_count: info.category_count,
            total_sample_count: info.total_sample_count,
            created_at: info.created_at,
            by_category: info.by_category,
        }
    }
}
