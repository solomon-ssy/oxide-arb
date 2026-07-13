//! Governed trade-policy artifact contracts.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{
    enums::{
        common::MarketCategory,
        quant::{
            ExitSettlementMode, FillRequirement, PriceComparison, RedeemPolicy, TradePolicyStatus,
        },
    },
    jsonb_active,
    types::{
        Bps, ContentHash, Price, RuntimeConfigVersionId, TradePolicyArtifactId, TrainingDatasetId,
        Usd,
    },
};

/// Breaking wire version for the standalone policy artifact family.
pub const TRADE_POLICY_ARTIFACT_FORMAT_VERSION: u32 = 1;

/// Immutable statistical and execution-quality thresholds used for publication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradePolicyQualityGate {
    pub min_cohort_samples: u64,
    pub min_executable_coverage: Decimal,
    pub min_full_l2_coverage: Decimal,
    pub min_cpcv_paths: u32,
    pub min_deflated_sharpe_ratio: Decimal,
    pub max_probability_of_backtest_overfitting: Decimal,
    pub max_ambiguous_touch_rate: Decimal,
    pub max_depth_failure_rate: Decimal,
    pub min_lower_confidence_utility_bps: Bps,
}

impl TradePolicyQualityGate {
    fn validate(&self) -> Result<(), String> {
        if self.min_cohort_samples == 0 || self.min_cpcv_paths == 0 {
            return Err("trade-policy sample and CPCV path gates must be positive".to_owned());
        }
        for (name, value, allow_zero) in [
            (
                "min_executable_coverage",
                self.min_executable_coverage,
                false,
            ),
            ("min_full_l2_coverage", self.min_full_l2_coverage, false),
            (
                "max_probability_of_backtest_overfitting",
                self.max_probability_of_backtest_overfitting,
                true,
            ),
            (
                "max_ambiguous_touch_rate",
                self.max_ambiguous_touch_rate,
                true,
            ),
            ("max_depth_failure_rate", self.max_depth_failure_rate, true),
        ] {
            let lower_ok = allow_zero && value == Decimal::ZERO || value > Decimal::ZERO;
            if !lower_ok || value > Decimal::ONE {
                return Err(format!(
                    "{name} must be in {}",
                    if allow_zero { "[0, 1]" } else { "(0, 1]" }
                ));
            }
        }
        if self.min_lower_confidence_utility_bps <= Bps::ZERO {
            return Err("min_lower_confidence_utility_bps must be positive".to_owned());
        }
        Ok(())
    }
}

/// Fully specified, reproducible policy-fit request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct TradePolicyFitContract {
    pub source_dataset_id: TrainingDatasetId,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub fit_window_start: DateTime<Utc>,
    pub fit_window_end: DateTime<Utc>,
    /// Latest point-in-time instant from which any fit evidence may be consumed.
    pub pit_cutoff: DateTime<Utc>,
    pub embargo_secs: u64,
    pub notional_tiers: Vec<Usd>,
    pub maximum_scale_out_targets: u8,
    pub quality_gate: TradePolicyQualityGate,
}

impl TradePolicyFitContract {
    pub fn validate(&self) -> Result<(), String> {
        if self.fit_window_start >= self.fit_window_end {
            return Err("trade-policy fit window must be half-open and non-empty".to_owned());
        }
        if self.fit_window_end > self.pit_cutoff {
            return Err("trade-policy fit window must end at or before pit_cutoff".to_owned());
        }
        if self.notional_tiers.is_empty()
            || self.notional_tiers.iter().any(|tier| !tier.is_positive())
        {
            return Err("trade-policy notional tiers must be non-empty and positive".to_owned());
        }
        if self
            .notional_tiers
            .windows(2)
            .any(|tiers| tiers[0] >= tiers[1])
        {
            return Err("trade-policy notional tiers must be strictly increasing".to_owned());
        }
        if self.maximum_scale_out_targets > 3 {
            return Err("trade-policy supports at most three scale-out targets".to_owned());
        }
        self.quality_gate.validate()
    }
}

/// Versioned provenance for one fitted cohort dimension.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradePolicyCohortDimension {
    pub methodology_id: String,
    pub methodology_hash: ContentHash,
    pub bucket_id: String,
}

/// Deterministic cohort selector for prediction-market trajectory policies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradePolicyCohortKey {
    pub category: MarketCategory,
    pub horizon_secs: u64,
    pub entry_price_min: Price,
    pub entry_price_max: Price,
    pub notional_tier: Usd,
    pub liquidity: TradePolicyCohortDimension,
    pub volatility: TradePolicyCohortDimension,
}

/// Entry-condition template expressed relative to the executable entry basis.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum EntryTriggerTemplate {
    Immediate,
    PriceOffset {
        comparison: PriceComparison,
        threshold_offset_bps: Bps,
        confirmation_secs: u64,
        max_observation_gap_ms: u64,
    },
}

/// Venue execution template selected out of sample for a cohort.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum EntryOrderTemplate {
    Passive { post_only: bool },
    Aggressive { fill_requirement: FillRequirement },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScaleOutTemplate {
    pub target_id: String,
    pub trigger_return_bps: Bps,
    pub target_cumulative_exit_pct: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrailingStopTemplate {
    pub trail_bps: Bps,
    pub activation_return_bps: Bps,
}

/// One deterministic parent-cohort step used to shrink a sparse leaf.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradePolicyShrinkStep {
    pub parent_cohort_index: u32,
    pub relaxed_dimension: TradePolicyShrinkDimension,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradePolicyShrinkDimension {
    Category,
    Volatility,
    Liquidity,
    EntryPrice,
}

/// One fitted entry/exit policy cohort.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradePolicyCohort {
    pub key: TradePolicyCohortKey,
    pub entry_trigger: EntryTriggerTemplate,
    pub entry_order: EntryOrderTemplate,
    pub max_slippage_bps: Bps,
    pub max_book_age_ms: u64,
    pub upper_barrier_bps: Bps,
    pub lower_barrier_bps: Bps,
    pub vertical_barrier_secs: u64,
    pub scale_out_targets: Vec<ScaleOutTemplate>,
    pub trailing_stop: Option<TrailingStopTemplate>,
    pub min_score_retention: Decimal,
    pub min_expected_return_bps: Bps,
    pub require_execution_eligibility: bool,
    pub settlement_mode: ExitSettlementMode,
    pub redeem_policy: RedeemPolicy,
    pub sample_count: u64,
    pub effective_sample_size: Decimal,
    pub executable_sample_count: u64,
    pub executable_coverage: Decimal,
    pub lower_confidence_utility_bps: Option<Bps>,
    pub shrink_path: Vec<TradePolicyShrinkStep>,
}

/// Validation evidence. Missing evidence is represented by `None`, never a fake zero.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradePolicyValidationEvidence {
    pub trial_ledger_hash: Option<ContentHash>,
    pub cpcv_path_count: Option<u32>,
    pub deflated_sharpe_ratio: Option<Decimal>,
    pub probability_of_backtest_overfitting: Option<Decimal>,
    pub effective_sample_size: Option<Decimal>,
    pub ambiguous_touch_rate: Option<Decimal>,
    pub depth_failure_rate: Option<Decimal>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutablePriceBasis {
    FullL2Vwap,
}

/// Typed reason why an execution-fidelity input is unavailable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradePolicyEvidenceGap {
    FullL2EntryUnavailable,
    FullL2ExitUnavailable,
    PitFeeModelUnavailable,
    TrialLedgerUnavailable,
    CpcvUnavailable,
    AmbiguousTouchEvidenceUnavailable,
    DepthFailureEvidenceUnavailable,
}

/// Executability fidelity used while fitting the artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradePolicyExecutionEvidence {
    pub entry_basis: Option<ExecutablePriceBasis>,
    pub exit_basis: Option<ExecutablePriceBasis>,
    pub full_l2_sample_count: u64,
    pub degraded_top_of_book_sample_count: u64,
    pub full_l2_coverage: Option<Decimal>,
    pub fee_model_hash: Option<ContentHash>,
    pub gaps: Vec<TradePolicyEvidenceGap>,
}

/// Canonical proof of the exact label projection visible at the PIT cutoff.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradePolicyPitCutoffEvidence {
    pub filtered_sample_count: u64,
    pub labels_matured_by_cutoff: u64,
    pub labels_excluded_after_cutoff: u64,
    pub filtered_sample_hash: ContentHash,
}

/// Authoritative publication denial derived from immutable payload evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum TradePolicyPublicationBlocker {
    UnsupportedFormat { actual: u32 },
    InvalidFitContract { detail: String },
    MissingPitCutoffEvidence,
    EmptyCohorts,
    MissingFullL2EntryBasis,
    MissingFullL2ExitBasis,
    MissingFullL2Coverage,
    InsufficientFullL2Coverage,
    MissingFeeModel,
    MissingTrialLedger,
    MissingCpcvPathCount,
    InsufficientCpcvPaths,
    MissingDeflatedSharpeRatio,
    DeflatedSharpeRatioBelowGate,
    MissingProbabilityOfBacktestOverfitting,
    ProbabilityOfBacktestOverfittingAboveGate,
    MissingAmbiguousTouchRate,
    AmbiguousTouchRateAboveGate,
    MissingDepthFailureRate,
    DepthFailureRateAboveGate,
    InsufficientCohortSamples { cohort_index: u32 },
    InsufficientCohortCoverage { cohort_index: u32 },
    MissingUtilityLowerBound { cohort_index: u32 },
    UtilityLowerBoundBelowGate { cohort_index: u32 },
    InvalidParentProvenance { cohort_index: u32 },
}

/// Immutable content hashed and stored for a governed entry/exit policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct TradePolicyArtifactPayload {
    pub format_version: u32,
    pub fit_contract: TradePolicyFitContract,
    pub source_dataset_hash: ContentHash,
    pub feature_schema_hash: ContentHash,
    pub label_schema_hash: ContentHash,
    pub fill_simulator_version: String,
    pub pit_cutoff_evidence: Option<TradePolicyPitCutoffEvidence>,
    pub execution_evidence: TradePolicyExecutionEvidence,
    pub cohorts: Vec<TradePolicyCohort>,
    pub validation: TradePolicyValidationEvidence,
}

impl TradePolicyArtifactPayload {
    /// Derive every hard publication blocker from immutable evidence and gates.
    #[must_use]
    pub fn publication_blockers(&self) -> Vec<TradePolicyPublicationBlocker> {
        let mut blockers = self.contract_and_execution_blockers();
        blockers.extend(self.validation_blockers());
        blockers.extend(self.cohort_blockers());
        blockers
    }

    fn contract_and_execution_blockers(&self) -> Vec<TradePolicyPublicationBlocker> {
        let mut blockers = Vec::new();
        if self.format_version != TRADE_POLICY_ARTIFACT_FORMAT_VERSION {
            blockers.push(TradePolicyPublicationBlocker::UnsupportedFormat {
                actual: self.format_version,
            });
        }
        if let Err(detail) = self.fit_contract.validate() {
            blockers.push(TradePolicyPublicationBlocker::InvalidFitContract { detail });
        }
        if self.pit_cutoff_evidence.is_none() {
            blockers.push(TradePolicyPublicationBlocker::MissingPitCutoffEvidence);
        }
        if self.cohorts.is_empty() {
            blockers.push(TradePolicyPublicationBlocker::EmptyCohorts);
        }
        if self.execution_evidence.entry_basis != Some(ExecutablePriceBasis::FullL2Vwap) {
            blockers.push(TradePolicyPublicationBlocker::MissingFullL2EntryBasis);
        }
        if self.execution_evidence.exit_basis != Some(ExecutablePriceBasis::FullL2Vwap) {
            blockers.push(TradePolicyPublicationBlocker::MissingFullL2ExitBasis);
        }
        match self.execution_evidence.full_l2_coverage {
            None => blockers.push(TradePolicyPublicationBlocker::MissingFullL2Coverage),
            Some(value) if value < self.fit_contract.quality_gate.min_full_l2_coverage => {
                blockers.push(TradePolicyPublicationBlocker::InsufficientFullL2Coverage);
            }
            Some(_) => {}
        }
        if self.execution_evidence.fee_model_hash.is_none() {
            blockers.push(TradePolicyPublicationBlocker::MissingFeeModel);
        }
        blockers
    }

    fn validation_blockers(&self) -> Vec<TradePolicyPublicationBlocker> {
        let mut blockers = Vec::new();
        let validation = &self.validation;
        let gate = &self.fit_contract.quality_gate;
        if validation.trial_ledger_hash.is_none() {
            blockers.push(TradePolicyPublicationBlocker::MissingTrialLedger);
        }
        match validation.cpcv_path_count {
            None => blockers.push(TradePolicyPublicationBlocker::MissingCpcvPathCount),
            Some(value) if value < gate.min_cpcv_paths => {
                blockers.push(TradePolicyPublicationBlocker::InsufficientCpcvPaths);
            }
            Some(_) => {}
        }
        match validation.deflated_sharpe_ratio {
            None => blockers.push(TradePolicyPublicationBlocker::MissingDeflatedSharpeRatio),
            Some(value) if value < gate.min_deflated_sharpe_ratio => {
                blockers.push(TradePolicyPublicationBlocker::DeflatedSharpeRatioBelowGate);
            }
            Some(_) => {}
        }
        match validation.probability_of_backtest_overfitting {
            None => blockers
                .push(TradePolicyPublicationBlocker::MissingProbabilityOfBacktestOverfitting),
            Some(value) if value > gate.max_probability_of_backtest_overfitting => {
                blockers
                    .push(TradePolicyPublicationBlocker::ProbabilityOfBacktestOverfittingAboveGate);
            }
            Some(_) => {}
        }
        match validation.ambiguous_touch_rate {
            None => blockers.push(TradePolicyPublicationBlocker::MissingAmbiguousTouchRate),
            Some(value) if value > gate.max_ambiguous_touch_rate => {
                blockers.push(TradePolicyPublicationBlocker::AmbiguousTouchRateAboveGate);
            }
            Some(_) => {}
        }
        match validation.depth_failure_rate {
            None => blockers.push(TradePolicyPublicationBlocker::MissingDepthFailureRate),
            Some(value) if value > gate.max_depth_failure_rate => {
                blockers.push(TradePolicyPublicationBlocker::DepthFailureRateAboveGate);
            }
            Some(_) => {}
        }
        blockers
    }

    fn cohort_blockers(&self) -> Vec<TradePolicyPublicationBlocker> {
        let mut blockers = Vec::new();
        let gate = &self.fit_contract.quality_gate;
        for (index, cohort) in self.cohorts.iter().enumerate() {
            let Ok(cohort_index) = u32::try_from(index) else {
                continue;
            };
            if cohort.sample_count < gate.min_cohort_samples {
                blockers.push(TradePolicyPublicationBlocker::InsufficientCohortSamples {
                    cohort_index,
                });
            }
            if cohort.executable_coverage < gate.min_executable_coverage {
                blockers.push(TradePolicyPublicationBlocker::InsufficientCohortCoverage {
                    cohort_index,
                });
            }
            match cohort.lower_confidence_utility_bps {
                None => {
                    blockers.push(TradePolicyPublicationBlocker::MissingUtilityLowerBound {
                        cohort_index,
                    });
                }
                Some(value) if value < gate.min_lower_confidence_utility_bps => {
                    blockers.push(TradePolicyPublicationBlocker::UtilityLowerBoundBelowGate {
                        cohort_index,
                    });
                }
                Some(_) => {}
            }
            if cohort
                .shrink_path
                .iter()
                .any(|step| step.parent_cohort_index >= cohort_index)
            {
                blockers
                    .push(TradePolicyPublicationBlocker::InvalidParentProvenance { cohort_index });
            }
        }
        blockers
    }

    #[must_use]
    pub fn is_publishable(&self) -> bool {
        self.publication_blockers().is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradePolicyArtifact {
    pub artifact_id: TradePolicyArtifactId,
    pub content_hash: ContentHash,
    pub status: TradePolicyStatus,
    pub payload: TradePolicyArtifactPayload,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

jsonb_active!(TradePolicyFitContract, TradePolicyArtifactPayload);

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use rust_decimal_macros::dec;

    use super::{
        TRADE_POLICY_ARTIFACT_FORMAT_VERSION, TradePolicyArtifactPayload,
        TradePolicyExecutionEvidence, TradePolicyFitContract, TradePolicyPublicationBlocker,
        TradePolicyQualityGate, TradePolicyValidationEvidence,
    };
    use crate::types::{Bps, ContentHash, RuntimeConfigVersionId, TrainingDatasetId, Usd};

    fn hash(digit: char) -> ContentHash {
        ContentHash::parse(format!("blake3:{}", digit.to_string().repeat(64)))
            .expect("canonical hash")
    }

    fn contract() -> TradePolicyFitContract {
        let fit_window_end = Utc.timestamp_opt(1_700_086_400, 0).single().expect("time");
        TradePolicyFitContract {
            source_dataset_id: TrainingDatasetId::from_v7(),
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            fit_window_start: Utc.timestamp_opt(1_700_000_000, 0).single().expect("time"),
            fit_window_end,
            pit_cutoff: fit_window_end,
            embargo_secs: 3_600,
            notional_tiers: vec![Usd::new(dec!(25)), Usd::new(dec!(100))],
            maximum_scale_out_targets: 3,
            quality_gate: TradePolicyQualityGate {
                min_cohort_samples: 100,
                min_executable_coverage: dec!(0.8),
                min_full_l2_coverage: dec!(0.8),
                min_cpcv_paths: 16,
                min_deflated_sharpe_ratio: dec!(0.5),
                max_probability_of_backtest_overfitting: dec!(0.2),
                max_ambiguous_touch_rate: dec!(0.01),
                max_depth_failure_rate: dec!(0.05),
                min_lower_confidence_utility_bps: Bps::new(dec!(1)),
            },
        }
    }

    #[test]
    fn fit_contract_rejects_labels_beyond_pit_cutoff() {
        let mut value = contract();
        value.pit_cutoff = value.fit_window_end - chrono::Duration::seconds(1);
        assert!(value.validate().is_err());
    }

    #[test]
    fn absent_evidence_and_empty_cohorts_cannot_self_attest_publication() {
        let payload = TradePolicyArtifactPayload {
            format_version: TRADE_POLICY_ARTIFACT_FORMAT_VERSION,
            fit_contract: contract(),
            source_dataset_hash: hash('1'),
            feature_schema_hash: hash('2'),
            label_schema_hash: hash('3'),
            fill_simulator_version: "unavailable-v1".to_owned(),
            pit_cutoff_evidence: None,
            execution_evidence: TradePolicyExecutionEvidence {
                entry_basis: None,
                exit_basis: None,
                full_l2_sample_count: 0,
                degraded_top_of_book_sample_count: 100,
                full_l2_coverage: None,
                fee_model_hash: None,
                gaps: Vec::new(),
            },
            cohorts: Vec::new(),
            validation: TradePolicyValidationEvidence {
                trial_ledger_hash: None,
                cpcv_path_count: None,
                deflated_sharpe_ratio: None,
                probability_of_backtest_overfitting: None,
                effective_sample_size: None,
                ambiguous_touch_rate: None,
                depth_failure_rate: None,
            },
        };

        let blockers = payload.publication_blockers();
        assert!(blockers.contains(&TradePolicyPublicationBlocker::EmptyCohorts));
        assert!(blockers.contains(&TradePolicyPublicationBlocker::MissingPitCutoffEvidence));
        assert!(blockers.contains(&TradePolicyPublicationBlocker::MissingFeeModel));
        assert!(blockers.contains(&TradePolicyPublicationBlocker::MissingTrialLedger));
        assert!(!payload.is_publishable());
    }
}
