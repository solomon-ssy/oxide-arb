//! Closed calibration-artifact payload value objects.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    enums::common::MarketCategory,
    types::{
        CalibrationArtifactId, ContentHash, IcaoStation, ModelVersionId, Price, Probability,
        TrainingDatasetId, WeatherTemperatureStatistic,
    },
};

/// The fitted monotone score-to-probability mapping.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "method")]
pub enum MonotoneMapping {
    Isotonic { knots: Vec<IsotonicKnot> },
    Platt { a: Decimal, b: Decimal },
}

/// One isotonic-regression knot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IsotonicKnot {
    pub score: Decimal,
    pub probability: Decimal,
}

/// One calibrated-probability reliability bucket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReliabilityBin {
    pub predicted_lo: Decimal,
    pub predicted_hi: Decimal,
    pub sample_count: u64,
    pub mean_predicted: Probability,
    pub empirical_frequency: Probability,
    pub wilson_ci: (Probability, Probability),
    pub mean_adverse_excursion_bps: Option<Decimal>,
}

/// Reliability diagnostics computed on the independent calibration split.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReliabilityReport {
    pub bins: Vec<ReliabilityBin>,
    pub brier_score: Decimal,
    pub log_loss: Decimal,
    pub ece: Decimal,
    pub n_samples: u64,
}

impl ReliabilityReport {
    /// Return the bucket containing a calibrated probability.
    #[must_use]
    pub fn bin_for(&self, calibrated_probability: Decimal) -> Option<&ReliabilityBin> {
        self.bins.iter().find(|bin| {
            calibrated_probability >= bin.predicted_lo
                && (calibrated_probability < bin.predicted_hi
                    || (bin.predicted_hi == Decimal::ONE && calibrated_probability <= Decimal::ONE))
        })
    }
}

/// Self-contained payload for a model-score calibrator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelScoreCalibrationPayload {
    pub model_version_id: ModelVersionId,
    pub calibration_dataset_id: TrainingDatasetId,
    pub mapping: MonotoneMapping,
    pub reliability: ReliabilityReport,
}

/// One empirical price-bias bin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PriceBiasBin {
    pub price_lo: Price,
    pub price_hi: Price,
    pub implied_mid: Price,
    pub realized_frequency: Probability,
    pub bias: Decimal,
    pub bias_ci: (Decimal, Decimal),
    pub sample_count: u64,
}

/// One time-to-resolution bucket of a category bias curve.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TtrBucketCurve {
    pub ttr_lo_secs: u64,
    pub ttr_hi_secs: Option<u64>,
    pub bins: Vec<PriceBiasBin>,
    pub ic: Decimal,
    pub ic_significant: bool,
    pub sample_count: u64,
}

/// A category's empirical bias curves by time to resolution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CategoryBiasCurve {
    pub by_ttr: Vec<TtrBucketCurve>,
    pub sample_count: u64,
}

/// Fixed payload for a favorite-longshot market-price bias artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketPriceBiasPayload {
    pub by_category: BTreeMap<MarketCategory, CategoryBiasCurve>,
}

/// Frozen mean forecast-minus-observation bias for one exact lead.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WeatherLeadBiasV1 {
    pub lead_hours: u16,
    pub sample_count: u32,
    pub bias_celsius: Decimal,
}

/// Frozen lead-bias table for one station.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WeatherStationBiasV1 {
    pub station: IcaoStation,
    pub temperature_statistic: WeatherTemperatureStatistic,
    pub leads: Vec<WeatherLeadBiasV1>,
}

/// Versioned, content-addressed weather calibration payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WeatherStationLeadBiasArtifactV1 {
    pub schema_version: u32,
    pub methodology: String,
    pub methodology_hash: ContentHash,
    pub grid_hashes: Vec<ContentHash>,
    pub source_hashes: Vec<ContentHash>,
    pub stations: Vec<WeatherStationBiasV1>,
}

/// One immutable weather-calibration publication on the PIT timeline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishedWeatherStationLeadBias {
    pub artifact_id: CalibrationArtifactId,
    pub content_hash: ContentHash,
    pub fit_window_start: DateTime<Utc>,
    pub fit_window_end: DateTime<Utc>,
    pub sample_count: i64,
    pub published_at: DateTime<Utc>,
    pub payload: WeatherStationLeadBiasArtifactV1,
}
