//! Closed calibration-artifact payload value objects.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    enums::{common::MarketCategory, model::ModelFamily},
    hashing::CanonicalDigest,
    types::{
        CalibrationArtifactId, ContentHash, DecisionPolicySnapshotId, IcaoStation, ModelSpecId,
        ModelVersionId, PayoutRatio, Price, Probability, ResearchProfileRef, TrainingDatasetId,
        WeatherTemperatureStatistic,
    },
};

/// Breaking schema version for model-score calibrator provenance.
pub const MODEL_SCORE_CALIBRATION_FORMAT_VERSION: u32 = 1;

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

/// Frozen frequency evidence for Polymarket's non-binary split resolution.
///
/// The score calibrator estimates `P(win | winner_take_all)` on the binary
/// subset. This separate Bernoulli event models whether the market resolves to
/// an `Unknown/50-50` payout at all, so a split is never coerced to either win
/// or loss and never disappears from scenario tail risk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SplitPayoutRateEvidence {
    pub total_sample_count: u64,
    pub split_sample_count: u64,
    pub empirical_probability: Probability,
    pub wilson_ci: (Probability, Probability),
    pub split_payout_ratio: PayoutRatio,
}

impl SplitPayoutRateEvidence {
    /// Validate the split ledger against the binary calibration population.
    pub fn validate(&self, binary_sample_count: u64) -> Result<(), String> {
        let expected_binary = self
            .total_sample_count
            .checked_sub(self.split_sample_count)
            .ok_or_else(|| {
                "split sample count exceeds the total calibration population".to_owned()
            })?;
        let probability = self.empirical_probability.inner();
        let interval = (self.wilson_ci.0.inner(), self.wilson_ci.1.inner());
        if self.total_sample_count == 0
            || expected_binary != binary_sample_count
            || probability < Decimal::ZERO
            || probability > Decimal::ONE
            || interval.0 < Decimal::ZERO
            || interval.0 > probability
            || interval.1 < probability
            || interval.1 > Decimal::ONE
            || self.split_payout_ratio.inner() != Decimal::new(5, 1)
        {
            return Err(
                "split-payout rate evidence is incomplete or inconsistent with calibration samples"
                    .to_owned(),
            );
        }
        let empirical =
            Decimal::from(self.split_sample_count) / Decimal::from(self.total_sample_count);
        if probability != empirical.round_dp(18) {
            return Err(
                "split-payout empirical probability differs from its exact counts".to_owned(),
            );
        }
        Ok(())
    }
}

/// Complete calibrated distribution over a binary token's terminal payout.
///
/// `winner_take_all_win_probability` is conditional on a non-split result.
/// Combining it with the explicitly estimated split mass yields unconditional
/// loss/split/win probabilities and expected payout without conflating either
/// quantity with `P(profit)` after entry costs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CalibratedPayoutDistribution {
    pub winner_take_all_win_probability: Probability,
    pub split_probability: Probability,
    pub split_probability_interval: (Probability, Probability),
    pub split_payout_ratio: PayoutRatio,
}

impl CalibratedPayoutDistribution {
    pub fn validate(&self) -> Result<(), String> {
        let conditional_win = self.winner_take_all_win_probability.inner();
        let split = self.split_probability.inner();
        let interval = (
            self.split_probability_interval.0.inner(),
            self.split_probability_interval.1.inner(),
        );
        if conditional_win < Decimal::ZERO
            || conditional_win > Decimal::ONE
            || split < Decimal::ZERO
            || split > Decimal::ONE
            || interval.0 < Decimal::ZERO
            || interval.0 > split
            || interval.1 < split
            || interval.1 > Decimal::ONE
            || self.split_payout_ratio.inner() != Decimal::new(5, 1)
        {
            return Err("calibrated payout distribution is outside its valid simplex".to_owned());
        }
        Ok(())
    }

    /// Unconditional terminal win probability.
    #[must_use]
    pub fn win_probability(self) -> Probability {
        Probability::new(
            (Decimal::ONE - self.split_probability.inner())
                * self.winner_take_all_win_probability.inner(),
        )
    }

    /// Unconditional terminal loss probability.
    #[must_use]
    pub fn loss_probability(self) -> Probability {
        Probability::new(
            (Decimal::ONE - self.split_probability.inner())
                * (Decimal::ONE - self.winner_take_all_win_probability.inner()),
        )
    }

    /// Expected terminal collateral received per token.
    #[must_use]
    pub fn expected_payout(self) -> Probability {
        Probability::new(
            self.win_probability().inner()
                + self.split_probability.inner() * self.split_payout_ratio.inner(),
        )
    }
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

    /// Resolve a positive downside estimate for one calibrated probability.
    ///
    /// Reliability bins intentionally omit empty equal-width buckets. A
    /// monotone calibrator can still interpolate through such a bucket, so an
    /// exact lookup is not a total serving function. Exact occupied buckets
    /// use their own frozen mean MAE. Sparse gaps and tails use the worst
    /// absolute mean MAE across all occupied buckets, forming a conservative
    /// envelope from observed evidence instead of inventing a business zero
    /// or degrading an otherwise valid shadow inference.
    #[must_use]
    pub fn conservative_downside_bps(&self, calibrated_probability: Decimal) -> Option<Decimal> {
        if let Some(exact) = self
            .bin_for(calibrated_probability)
            .and_then(|bin| bin.mean_adverse_excursion_bps)
        {
            return Some(exact.abs());
        }
        self.bins
            .iter()
            .filter_map(|bin| bin.mean_adverse_excursion_bps)
            .map(|value| value.abs())
            .max()
    }

    /// Validate the complete reliability contract against the persisted
    /// artifact sample count.
    pub fn validate(&self, expected_samples: u64) -> Result<(), String> {
        if self.n_samples == 0 || self.n_samples != expected_samples || self.bins.is_empty() {
            return Err(format!(
                "reliability sample contract is invalid: expected={expected_samples}, report={}, bins={}",
                self.n_samples,
                self.bins.len()
            ));
        }
        if self.brier_score < Decimal::ZERO
            || self.brier_score > Decimal::ONE
            || self.log_loss < Decimal::ZERO
            || self.ece < Decimal::ZERO
            || self.ece > Decimal::ONE
        {
            return Err("reliability metrics are outside their valid ranges".to_owned());
        }
        let mut sample_total = 0_u64;
        let mut previous_hi = Decimal::ZERO;
        for bin in &self.bins {
            if bin.sample_count == 0
                || bin.predicted_lo < Decimal::ZERO
                || bin.predicted_lo >= bin.predicted_hi
                || bin.predicted_hi > Decimal::ONE
                || bin.predicted_lo < previous_hi
                || bin.mean_predicted.inner() < bin.predicted_lo
                || bin.mean_predicted.inner() > bin.predicted_hi
                || bin.empirical_frequency.inner() < Decimal::ZERO
                || bin.empirical_frequency.inner() > Decimal::ONE
                || bin.wilson_ci.0.inner() < Decimal::ZERO
                || bin.wilson_ci.0.inner() > bin.wilson_ci.1.inner()
                || bin.wilson_ci.1.inner() > Decimal::ONE
            {
                return Err(format!(
                    "reliability bin [{}, {}] is structurally invalid",
                    bin.predicted_lo, bin.predicted_hi
                ));
            }
            sample_total = sample_total
                .checked_add(bin.sample_count)
                .ok_or_else(|| "reliability sample count overflow".to_owned())?;
            previous_hi = bin.predicted_hi;
        }
        if sample_total != self.n_samples {
            return Err(format!(
                "reliability bins contain {sample_total} samples, expected {}",
                self.n_samples
            ));
        }
        Ok(())
    }
}

/// Exact source-model commitment consumed by one calibration fit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelScoreCalibrationModelBinding {
    pub model_version_id: ModelVersionId,
    pub artifact_hash: ContentHash,
    pub serving_contract_hash: ContentHash,
    pub model_spec_id: ModelSpecId,
    pub model_spec_definition_hash: ContentHash,
    pub model_family: ModelFamily,
    pub profile_ref: ResearchProfileRef,
    pub category_scope: Option<MarketCategory>,
    pub prediction_horizon_secs: u64,
    pub training_dataset_id: TrainingDatasetId,
    pub training_dataset_hash: ContentHash,
}

/// Exact frozen Calibration Dataset and Source Slice bytes consumed by a fit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelScoreCalibrationDatasetBinding {
    pub calibration_dataset_id: TrainingDatasetId,
    pub dataset_hash: ContentHash,
    pub manifest_hash: ContentHash,
    pub artifact_bytes_hash: ContentHash,
    pub source_slice_manifest_hash: ContentHash,
    pub feature_schema_hash: ContentHash,
    pub factor_schema_hash: ContentHash,
    pub label_schema_hash: ContentHash,
}

/// Exact decision-policy snapshot whose calibration methodology governed a fit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelScoreCalibrationPolicyBinding {
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub snapshot_hash: ContentHash,
}

/// Complete immutable provenance for one model-score calibration fit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelScoreCalibrationFitContract {
    pub model: ModelScoreCalibrationModelBinding,
    pub calibration_dataset: ModelScoreCalibrationDatasetBinding,
    pub policy_snapshot: ModelScoreCalibrationPolicyBinding,
}

impl ModelScoreCalibrationFitContract {
    pub fn validate(&self) -> Result<(), String> {
        if self.model.model_family != ModelFamily::WeightedFactor {
            return Err(
                "model-score calibration requires a weighted-factor source model".to_owned(),
            );
        }
        if self.model.prediction_horizon_secs == 0 {
            return Err(
                "model-score calibration requires a non-zero prediction horizon".to_owned(),
            );
        }
        if self.model.training_dataset_id == self.calibration_dataset.calibration_dataset_id {
            return Err(
                "model-score calibration dataset must differ from the model training dataset"
                    .to_owned(),
            );
        }
        Ok(())
    }
}

/// Self-contained payload for a model-score calibrator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelScoreCalibrationPayload {
    pub format_version: u32,
    pub fit_contract: ModelScoreCalibrationFitContract,
    pub mapping: MonotoneMapping,
    pub reliability: ReliabilityReport,
    pub split_payout_rate: SplitPayoutRateEvidence,
}

impl ModelScoreCalibrationPayload {
    pub fn validate_contract(&self) -> Result<(), String> {
        if self.format_version != MODEL_SCORE_CALIBRATION_FORMAT_VERSION {
            return Err(format!(
                "unsupported model-score calibration format {}; expected {}",
                self.format_version, MODEL_SCORE_CALIBRATION_FORMAT_VERSION
            ));
        }
        self.fit_contract.validate()
    }

    /// Validate the immutable fit contract, fitted transform, and reliability
    /// evidence as one closed payload.
    pub fn validate(&self, expected_samples: u64) -> Result<(), String> {
        self.validate_contract()?;
        self.mapping.validate()?;
        self.reliability.validate(expected_samples)?;
        self.split_payout_rate.validate(expected_samples)?;
        if self.reliability.bins.iter().any(|bin| {
            bin.mean_adverse_excursion_bps
                .is_none_or(|value| value > Decimal::ZERO)
        }) {
            return Err(
                "model-score calibration requires non-positive frozen MAE evidence in every occupied reliability bin"
                    .to_owned(),
            );
        }
        Ok(())
    }

    /// Compute the canonical artifact hash from every immutable persisted
    /// semantic field.
    pub fn content_hash(
        &self,
        fit_window_start: DateTime<Utc>,
        fit_window_end: DateTime<Utc>,
        calibration_split_hash: &ContentHash,
    ) -> Result<ContentHash, String> {
        #[derive(Serialize)]
        struct FitWindow {
            from: DateTime<Utc>,
            to: DateTime<Utc>,
        }

        #[derive(Serialize)]
        struct Canonical<'a> {
            fit_window: FitWindow,
            calibration_split_hash: &'a ContentHash,
            payload: &'a ModelScoreCalibrationPayload,
        }

        if fit_window_start >= fit_window_end {
            return Err("model-score calibration fit window must be half-open".to_owned());
        }
        CanonicalDigest::content_hash_json(&Canonical {
            fit_window: FitWindow {
                from: fit_window_start,
                to: fit_window_end,
            },
            calibration_split_hash,
            payload: self,
        })
        .map_err(|error| format!("model-score calibration content hash failed: {error}"))
    }
}

impl MonotoneMapping {
    /// Validate the frozen transform graph before persistence or inference.
    pub fn validate(&self) -> Result<(), String> {
        let Self::Isotonic { knots } = self else {
            return Ok(());
        };
        if knots.is_empty() {
            return Err("isotonic calibration mapping has no fitted knots".to_owned());
        }
        for knot in knots {
            if knot.probability < Decimal::ZERO || knot.probability > Decimal::ONE {
                return Err(format!(
                    "isotonic probability {} at score {} is outside [0, 1]",
                    knot.probability, knot.score
                ));
            }
        }
        for pair in knots.windows(2) {
            let [left, right] = pair else {
                continue;
            };
            if left.score >= right.score {
                return Err(format!(
                    "isotonic scores must be strictly increasing, got {} then {}",
                    left.score, right.score
                ));
            }
            if left.probability > right.probability {
                return Err(format!(
                    "isotonic probabilities must be non-decreasing, got {} then {}",
                    left.probability, right.probability
                ));
            }
        }
        Ok(())
    }
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
    use rust_decimal::Decimal;

    use super::{CalibratedPayoutDistribution, SplitPayoutRateEvidence};
    use crate::types::{PayoutRatio, Probability};

    impl PayoutRatio {
        fn split_fixture() -> Self {
            Self::try_new(Decimal::new(5, 1)).expect("canonical split payout")
        }
    }

    #[test]
    fn split_evidence_binds_population() {
        let evidence = SplitPayoutRateEvidence {
            total_sample_count: 10,
            split_sample_count: 1,
            empirical_probability: Probability::new(Decimal::new(1, 1)),
            wilson_ci: (
                Probability::new(Decimal::new(1, 2)),
                Probability::new(Decimal::new(3, 1)),
            ),
            split_payout_ratio: PayoutRatio::split_fixture(),
        };

        assert!(evidence.validate(9).is_ok());
        assert!(evidence.validate(8).is_err());
    }

    #[test]
    fn payout_preserves_three_states() {
        let distribution = CalibratedPayoutDistribution {
            winner_take_all_win_probability: Probability::new(Decimal::new(8, 1)),
            split_probability: Probability::new(Decimal::new(1, 1)),
            split_probability_interval: (
                Probability::new(Decimal::new(5, 2)),
                Probability::new(Decimal::new(2, 1)),
            ),
            split_payout_ratio: PayoutRatio::split_fixture(),
        };

        assert!(distribution.validate().is_ok());
        assert_eq!(distribution.win_probability().inner(), Decimal::new(72, 2));
        assert_eq!(distribution.loss_probability().inner(), Decimal::new(18, 2));
        assert_eq!(distribution.expected_payout().inner(), Decimal::new(77, 2));
    }
}
