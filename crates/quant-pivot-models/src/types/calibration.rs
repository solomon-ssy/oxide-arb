//! Closed calibration-artifact payload value objects.

use std::{
    collections::{BTreeMap, HashSet},
    hash::Hash,
};

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{
    enums::{common::MarketCategory, quant::DataQualityStatus},
    types::{
        CalibrationArtifactId, ContentHash, IcaoStation, ModelVersionId, Price, Probability,
        SchemaVersion, TrainingDatasetId, WeatherTemperatureStatistic, feature::NullReason,
    },
};

/// Current immutable schema/methodology for score-multiplier calibration.
pub const SCORE_MULTIPLIER_CALIBRATION_METHODOLOGY_VERSION: SchemaVersion = SchemaVersion::FIRST;

/// Data-quality stratum evidence for one multiplier-calibration derivation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataQualityStratumFit {
    pub status: DataQualityStatus,
    pub sample_count: u64,
    pub mean_realized_bps: Option<Decimal>,
}

/// Stable identity of one liquidity calibration bucket.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "bucket")]
pub enum LiquidityCalibrationBucket {
    Tier { min_liquidity_usd: Decimal },
    Floor,
}

/// Liquidity stratum evidence for one multiplier-calibration derivation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiquidityStratumFit {
    pub bucket: LiquidityCalibrationBucket,
    pub sample_count: u64,
    pub mean_realized_bps: Option<Decimal>,
}

/// Stable identity of one horizon calibration bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HorizonCalibrationBucket {
    TooSoon,
    InWindow,
    TooLate,
}

/// Horizon stratum evidence for one multiplier-calibration derivation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HorizonStratumFit {
    pub bucket: HorizonCalibrationBucket,
    pub sample_count: u64,
    pub mean_realized_bps: Option<Decimal>,
}

/// Stable identity of one substitution calibration bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "bucket")]
pub enum SubstitutionCalibrationBucket {
    Reason { reason: NullReason },
    Clean,
}

/// Substitution stratum evidence for one multiplier-calibration derivation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubstitutionStratumFit {
    pub bucket: SubstitutionCalibrationBucket,
    pub sample_count: u64,
    pub mean_realized_bps: Option<Decimal>,
}

/// Immutable, hash-addressed evidence emitted by score-multiplier calibration.
///
/// This is intentionally a typed JSONB document: consumers load and hash the
/// complete report, while strata are not independently updated or queried.
/// Parent model and source backtest remain relational columns on the model
/// version so lineage and referential integrity are never hidden in JSON.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct ScoreMultiplierCalibrationReport {
    pub methodology_version: SchemaVersion,
    pub minimum_stratum_samples: u64,
    pub total_samples: u64,
    pub data_quality_strata: Vec<DataQualityStratumFit>,
    pub liquidity_strata: Vec<LiquidityStratumFit>,
    pub horizon_strata: Vec<HorizonStratumFit>,
    pub substitution_strata: Vec<SubstitutionStratumFit>,
}

impl ScoreMultiplierCalibrationReport {
    /// Validate the complete closed report shape and its evidence semantics.
    pub fn validate(&self) -> Result<(), String> {
        if self.methodology_version != SCORE_MULTIPLIER_CALIBRATION_METHODOLOGY_VERSION {
            return Err(format!(
                "unsupported methodology_version {}; expected {}",
                self.methodology_version, SCORE_MULTIPLIER_CALIBRATION_METHODOLOGY_VERSION
            ));
        }
        if self.minimum_stratum_samples == 0 {
            return Err("minimum_stratum_samples must be positive".to_owned());
        }
        if self.total_samples == 0 {
            return Err("total_samples must be positive".to_owned());
        }

        validate_data_quality_strata(&self.data_quality_strata)?;
        validate_unique_complete(
            self.horizon_strata.iter().map(|fit| fit.bucket),
            [
                HorizonCalibrationBucket::TooSoon,
                HorizonCalibrationBucket::InWindow,
                HorizonCalibrationBucket::TooLate,
            ],
            "horizon",
        )?;

        let expected_substitutions = [
            NullReason::SourceUnavailable,
            NullReason::StaleBeyondPolicy,
            NullReason::OutOfValidRange,
            NullReason::InsufficientHistory,
            NullReason::NotApplicable,
            NullReason::LegBookMissing,
            NullReason::TradeTapeUnavailable,
            NullReason::InsufficientTradeTape,
            NullReason::InsufficientRoleCoverage,
            NullReason::DomainSourceUnavailable,
            NullReason::LinkageUnresolved,
        ];
        let mut substitution_buckets: HashSet<_> = self
            .substitution_strata
            .iter()
            .map(|fit| fit.bucket)
            .collect();
        if substitution_buckets.len() != self.substitution_strata.len()
            || !substitution_buckets.remove(&SubstitutionCalibrationBucket::Clean)
            || substitution_buckets.len() != expected_substitutions.len()
            || expected_substitutions.iter().any(|reason| {
                !substitution_buckets
                    .contains(&SubstitutionCalibrationBucket::Reason { reason: *reason })
            })
        {
            return Err(
                "substitution strata must contain every null reason exactly once plus clean"
                    .to_owned(),
            );
        }

        let liquidity_buckets: HashSet<_> = self
            .liquidity_strata
            .iter()
            .map(|fit| fit.bucket.clone())
            .collect();
        if liquidity_buckets.len() != self.liquidity_strata.len()
            || !liquidity_buckets.contains(&LiquidityCalibrationBucket::Floor)
        {
            return Err(
                "liquidity strata must be unique and contain exactly one floor bucket".to_owned(),
            );
        }

        for (name, fits) in [
            (
                "data_quality",
                self.data_quality_strata
                    .iter()
                    .map(|fit| (fit.sample_count, fit.mean_realized_bps))
                    .collect::<Vec<_>>(),
            ),
            (
                "liquidity",
                self.liquidity_strata
                    .iter()
                    .map(|fit| (fit.sample_count, fit.mean_realized_bps))
                    .collect(),
            ),
            (
                "horizon",
                self.horizon_strata
                    .iter()
                    .map(|fit| (fit.sample_count, fit.mean_realized_bps))
                    .collect(),
            ),
            (
                "substitution",
                self.substitution_strata
                    .iter()
                    .map(|fit| (fit.sample_count, fit.mean_realized_bps))
                    .collect(),
            ),
        ] {
            for (sample_count, mean) in fits {
                if sample_count > self.total_samples {
                    return Err(format!(
                        "{name} stratum sample_count {sample_count} exceeds total_samples {}",
                        self.total_samples
                    ));
                }
                if (sample_count >= self.minimum_stratum_samples) != mean.is_some() {
                    return Err(format!(
                        "{name} stratum mean presence disagrees with minimum_stratum_samples"
                    ));
                }
            }
        }
        Ok(())
    }
}

fn validate_data_quality_strata(fits: &[DataQualityStratumFit]) -> Result<(), String> {
    validate_unique_complete(
        fits.iter().map(|fit| fit.status),
        [
            DataQualityStatus::Fresh,
            DataQualityStatus::Acceptable,
            DataQualityStatus::Degraded,
            DataQualityStatus::Stale,
            DataQualityStatus::Insufficient,
        ],
        "data_quality",
    )
}

fn validate_unique_complete<T, const N: usize>(
    actual: impl Iterator<Item = T>,
    expected: [T; N],
    name: &str,
) -> Result<(), String>
where
    T: Copy + Eq + Hash,
{
    let actual: Vec<_> = actual.collect();
    let distinct: HashSet<_> = actual.iter().copied().collect();
    if actual.len() != N
        || distinct.len() != N
        || expected.iter().any(|value| !distinct.contains(value))
    {
        return Err(format!("{name} strata are incomplete or duplicated"));
    }
    Ok(())
}

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

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;
    use serde_json::Value;

    use super::{
        DataQualityStratumFit, HorizonCalibrationBucket, HorizonStratumFit,
        LiquidityCalibrationBucket, LiquidityStratumFit,
        SCORE_MULTIPLIER_CALIBRATION_METHODOLOGY_VERSION, ScoreMultiplierCalibrationReport,
        SubstitutionCalibrationBucket, SubstitutionStratumFit,
    };
    use crate::{enums::quant::DataQualityStatus, types::feature::NullReason};

    fn report() -> ScoreMultiplierCalibrationReport {
        let data_quality_strata = [
            DataQualityStatus::Fresh,
            DataQualityStatus::Acceptable,
            DataQualityStatus::Degraded,
            DataQualityStatus::Stale,
            DataQualityStatus::Insufficient,
        ]
        .into_iter()
        .map(|status| DataQualityStratumFit {
            status,
            sample_count: 3,
            mean_realized_bps: Some(dec!(1)),
        })
        .collect();
        let horizon_strata = [
            HorizonCalibrationBucket::TooSoon,
            HorizonCalibrationBucket::InWindow,
            HorizonCalibrationBucket::TooLate,
        ]
        .into_iter()
        .map(|bucket| HorizonStratumFit {
            bucket,
            sample_count: 3,
            mean_realized_bps: Some(dec!(1)),
        })
        .collect();
        let reasons = [
            NullReason::SourceUnavailable,
            NullReason::StaleBeyondPolicy,
            NullReason::OutOfValidRange,
            NullReason::InsufficientHistory,
            NullReason::NotApplicable,
            NullReason::LegBookMissing,
            NullReason::TradeTapeUnavailable,
            NullReason::InsufficientTradeTape,
            NullReason::InsufficientRoleCoverage,
            NullReason::DomainSourceUnavailable,
            NullReason::LinkageUnresolved,
        ];
        let mut substitution_strata: Vec<_> = reasons
            .into_iter()
            .map(|reason| SubstitutionStratumFit {
                bucket: SubstitutionCalibrationBucket::Reason { reason },
                sample_count: 3,
                mean_realized_bps: Some(dec!(1)),
            })
            .collect();
        substitution_strata.push(SubstitutionStratumFit {
            bucket: SubstitutionCalibrationBucket::Clean,
            sample_count: 3,
            mean_realized_bps: Some(dec!(1)),
        });
        ScoreMultiplierCalibrationReport {
            methodology_version: SCORE_MULTIPLIER_CALIBRATION_METHODOLOGY_VERSION,
            minimum_stratum_samples: 3,
            total_samples: 3,
            data_quality_strata,
            liquidity_strata: vec![
                LiquidityStratumFit {
                    bucket: LiquidityCalibrationBucket::Tier {
                        min_liquidity_usd: dec!(1000),
                    },
                    sample_count: 3,
                    mean_realized_bps: Some(dec!(1)),
                },
                LiquidityStratumFit {
                    bucket: LiquidityCalibrationBucket::Floor,
                    sample_count: 3,
                    mean_realized_bps: Some(dec!(1)),
                },
            ],
            horizon_strata,
            substitution_strata,
        }
    }

    #[test]
    fn score_multiplier_report_accepts_complete_closed_evidence() {
        report().validate().expect("complete report");
    }

    #[test]
    fn score_multiplier_report_rejects_duplicate_strata() {
        let mut report = report();
        report
            .data_quality_strata
            .push(report.data_quality_strata[0].clone());
        assert!(report.validate().is_err());
    }

    #[test]
    fn score_multiplier_report_rejects_mean_below_sample_floor() {
        let mut report = report();
        report.horizon_strata[0].sample_count = 2;
        assert!(report.validate().is_err());
    }

    #[test]
    fn score_multiplier_report_rejects_unknown_json_fields() {
        let mut value = serde_json::to_value(report()).expect("serialize report");
        value
            .as_object_mut()
            .expect("report object")
            .insert("future_magic".to_owned(), Value::Bool(true));
        assert!(serde_json::from_value::<ScoreMultiplierCalibrationReport>(value).is_err());
    }
}
