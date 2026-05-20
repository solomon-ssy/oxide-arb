//! Endgame calibration domain models.
//!
//! Calibration data drives confidence-adjusted position sizing by providing
//! historical resolution rates bucketed by price zone and time-to-expiry.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Price zone for endgame calibration bucketing.
///
/// Markets near 0 or 1 have different resolution dynamics than mid-range
/// markets. Zones are defined so the bucket boundaries are non-overlapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PriceZone {
    /// Price ∈ [0.00, 0.10) — deep out of the money.
    DeepNo,
    /// Price ∈ [0.10, 0.30) — leaning no.
    LeanNo,
    /// Price ∈ [0.30, 0.70) — uncertain.
    Mid,
    /// Price ∈ [0.70, 0.90) — leaning yes.
    LeanYes,
    /// Price ∈ [0.90, 1.00] — deep in the money.
    DeepYes,
}

/// Duration bucket for endgame calibration.
///
/// How long until the market is expected to resolve. Shorter durations
/// generally have higher confidence in predicted outcomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurationBucket {
    /// < 1 hour to resolution.
    Imminent,
    /// 1–6 hours.
    Short,
    /// 6–24 hours.
    Medium,
    /// 1–7 days.
    Long,
    /// > 7 days.
    Extended,
}

/// Composite key for a calibration bucket.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BucketKey {
    pub price_zone: PriceZone,
    pub duration_bucket: DurationBucket,
}

/// Snapshot of calibration data at the time an opportunity was detected.
///
/// Frozen at detection time so the sizing decision is auditable against
/// the exact calibration state that produced it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalibrationSnapshot {
    pub bucket_key: BucketKey,
    /// Historical resolution rate for this bucket (0.0–1.0).
    pub resolution_rate: Decimal,
    /// Number of historical observations in this bucket.
    pub sample_size: u32,
    /// Confidence adjustment factor applied to Kelly sizing.
    pub confidence_adjust: Decimal,
}

/// Context about how a market resolved, used for calibration feedback.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolutionContext {
    pub bucket_key: BucketKey,
    /// Whether our predicted outcome was correct.
    pub prediction_correct: bool,
    /// Actual settlement price (0 or 1 for binary markets).
    pub settlement_price: Decimal,
}
