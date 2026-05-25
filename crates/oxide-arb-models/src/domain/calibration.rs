//! Endgame calibration domain models.
//!
//! Calibration data drives confidence-adjusted position sizing by providing
//! historical resolution rates bucketed by market category, price zone, and
//! convergence duration. Zones are finer near 1.0 because small price
//! differences at the extreme have outsized impact on expected return.

use crate::enums::calibration::{DurationBucket, PriceZone};
use crate::enums::common::MarketCategory;
use crate::types::{MarketId, Price, Probability};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

/// Composite key for a calibration bucket.
///
/// The triple `(category, price_zone, duration_bucket)` uniquely identifies
/// a calibration bucket both in-memory and in the database.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BucketKey {
    pub category: MarketCategory,
    pub price_zone: PriceZone,
    pub duration_bucket: DurationBucket,
}

// ── Read models ─────────────────────────────────────────────────────

/// Snapshot of calibration data frozen at the time an opportunity was detected.
///
/// Captured so that sizing and risk decisions are auditable against the exact
/// calibration state that produced them. No field should be mutated after
/// construction. Not a DB projection — see `CalibrationBucketInfo` for that.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalibrationSnapshot {
    pub bucket_key: BucketKey,
    /// Empirical Bayes posterior mean: `(alpha + correct) / (alpha + beta + total)`.
    pub posterior_mean: Decimal,
    /// Number of historical observations in the matched bucket.
    pub sample_size: u32,
    /// Beta prior alpha at detection time (for audit reproducibility).
    pub alpha_prior: Decimal,
    /// Beta prior beta at detection time (for audit reproducibility).
    pub beta_prior: Decimal,
    /// Which fallback tier produced this entry (1=exact, 2=category+zone,
    /// 3=zone-only, 4=global prior).
    pub fallback_tier: u8,
    /// Output of `ConfidenceFusion`: dynamic-weight blend of calibrator
    /// posterior and real-time convergence confidence.
    pub fused_probability: Decimal,
}

/// DB row projection matching `entities::calibration::Model` columns exactly.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::calibration::Entity")]
pub struct CalibrationBucketInfo {
    pub id: i32,
    pub category: MarketCategory,
    pub price_zone: PriceZone,
    pub duration_bucket: DurationBucket,
    pub total_count: i32,
    pub correct_count: i32,
    pub alpha_prior: Probability,
    pub beta_prior: Probability,
    pub posterior_mean: Option<Probability>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(CalibrationBucketInfo, crate::entities::calibration::Model, {
    id, category, price_zone, duration_bucket, total_count, correct_count,
    alpha_prior, beta_prior, posterior_mean, updated_at,
});

info_from_model!(CalibrationOutcomeInfo, crate::entities::calibration_outcome::Model, {
    id, market_id, category, price_zone, duration_bucket, predicted_yes,
    actual_yes, entry_price, confidence_at_entry, convergence_secs,
    resolved_at, created_at,
});

// ── Write DTOs ──────────────────────────────────────────────────────

/// Upsert payload for the `endgame_calibration_bucket` table.
///
/// The `id` column is auto-increment, so it is omitted here. For
/// ON CONFLICT upserts keyed on `(category, price_zone, duration_bucket)`,
/// only the updateable columns are included.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "super::super::entities::calibration::ActiveModel")]
pub struct UpsertCalibration {
    pub category: MarketCategory,
    pub price_zone: PriceZone,
    pub duration_bucket: DurationBucket,
    pub total_count: i32,
    pub correct_count: i32,
    pub alpha_prior: Probability,
    pub beta_prior: Probability,
    pub posterior_mean: Option<Probability>,
    pub updated_at: DateTime<Utc>,
}

/// DB row projection for calibration outcomes.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::calibration_outcome::Entity")]
pub struct CalibrationOutcomeInfo {
    pub id: i64,
    pub market_id: MarketId,
    pub category: MarketCategory,
    pub price_zone: PriceZone,
    pub duration_bucket: DurationBucket,
    pub predicted_yes: bool,
    pub actual_yes: Option<bool>,
    pub entry_price: Price,
    pub confidence_at_entry: Probability,
    pub convergence_secs: i32,
    pub resolved_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// Insert payload for a calibration outcome record.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "super::super::entities::calibration_outcome::ActiveModel")]
pub struct NewCalibrationOutcome {
    pub market_id: MarketId,
    pub category: MarketCategory,
    pub price_zone: PriceZone,
    pub duration_bucket: DurationBucket,
    pub predicted_yes: bool,
    pub actual_yes: Option<bool>,
    pub entry_price: Price,
    pub confidence_at_entry: Probability,
    pub convergence_secs: i32,
    pub resolved_at: Option<DateTime<Utc>>,
}

// ── Value objects ───────────────────────────────────────────────────

/// Context about how a market resolved, used for calibration feedback.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolutionContext {
    pub bucket_key: BucketKey,
    /// Whether our predicted outcome was correct.
    pub prediction_correct: bool,
    /// Actual settlement price (0 or 1 for binary markets).
    pub settlement_price: Decimal,
}
