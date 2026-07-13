//! Governed trade-policy artifact contracts.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{
    enums::{
        common::MarketCategory,
        quant::{FillRequirement, PriceComparison, TradePolicyStatus},
    },
    jsonb_active,
    types::{
        Bps, ContentHash, Price, RuntimeConfigVersionId, TradePolicyArtifactId, TrainingDatasetId,
        Usd,
    },
};

/// Breaking wire version for the standalone policy artifact family.
pub const TRADE_POLICY_ARTIFACT_FORMAT_VERSION: u32 = 1;

/// Fully specified, reproducible policy-fit request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct TradePolicyFitContract {
    pub source_dataset_id: TrainingDatasetId,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub fit_window_start: DateTime<Utc>,
    pub fit_window_end: DateTime<Utc>,
    pub embargo_secs: u64,
    pub notional_tiers: Vec<Usd>,
    pub maximum_scale_out_targets: u8,
    pub minimum_executable_coverage: Decimal,
}

impl TradePolicyFitContract {
    pub fn validate(&self) -> Result<(), String> {
        if self.fit_window_start >= self.fit_window_end {
            return Err("trade-policy fit window must be half-open and non-empty".to_owned());
        }
        if self.notional_tiers.is_empty()
            || self.notional_tiers.iter().any(|tier| !tier.is_positive())
        {
            return Err("trade-policy notional tiers must be non-empty and positive".to_owned());
        }
        if self.maximum_scale_out_targets > 3 {
            return Err("trade-policy supports at most three scale-out targets".to_owned());
        }
        if self.minimum_executable_coverage <= Decimal::ZERO
            || self.minimum_executable_coverage > Decimal::ONE
        {
            return Err("minimum_executable_coverage must be in (0, 1]".to_owned());
        }
        Ok(())
    }
}

/// Deterministic cohort selector for prediction-market trajectory policies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradePolicyCohortKey {
    pub category: MarketCategory,
    pub horizon_secs: u64,
    pub entry_price_min: Price,
    pub entry_price_max: Price,
    pub notional_tier: Usd,
    pub liquidity_tier: String,
    pub volatility_regime: String,
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

/// Relative cumulative scale-out target stored in an artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScaleOutTemplate {
    pub target_id: String,
    pub trigger_return_bps: Bps,
    pub target_cumulative_exit_pct: Decimal,
}

/// Optional trailing policy stored relative to the executable entry basis.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrailingStopTemplate {
    pub trail_bps: Bps,
    pub activation_return_bps: Bps,
}

/// One fitted entry/exit policy cohort.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradePolicyCohort {
    pub key: TradePolicyCohortKey,
    pub entry_trigger: EntryTriggerTemplate,
    pub entry_order: EntryOrderTemplate,
    pub upper_barrier_bps: Bps,
    pub lower_barrier_bps: Bps,
    pub vertical_barrier_secs: u64,
    pub scale_out_targets: Vec<ScaleOutTemplate>,
    pub trailing_stop: Option<TrailingStopTemplate>,
    pub min_score_retention: Decimal,
    pub min_expected_return_bps: Bps,
    pub require_execution_eligibility: bool,
    pub sample_count: u64,
    pub executable_sample_count: u64,
    pub executable_coverage: Decimal,
    pub lower_confidence_utility_bps: Decimal,
    pub parent_cohort_index: Option<u32>,
}

/// Validation evidence required before a policy can be published.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradePolicyValidation {
    pub cpcv_path_count: u32,
    pub deflated_sharpe_ratio: Decimal,
    pub probability_of_backtest_overfitting: Decimal,
    pub executable_coverage: Decimal,
    pub passed: bool,
    pub failure_reasons: Vec<String>,
}

/// Executability fidelity used while fitting the artifact.
///
/// A top-of-book-only dataset is recorded explicitly and can never satisfy the
/// publication gate; it is still useful for `ReportOnly` shadow comparison.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradePolicyExecutionEvidence {
    pub entry_basis: String,
    pub exit_basis: String,
    pub full_l2_sample_count: u64,
    pub degraded_top_of_book_sample_count: u64,
    pub full_l2_coverage: Decimal,
    pub fees_included: bool,
    pub degradation_reasons: Vec<String>,
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
    pub fee_model_hash: ContentHash,
    pub execution_evidence: TradePolicyExecutionEvidence,
    pub cohorts: Vec<TradePolicyCohort>,
    pub validation: TradePolicyValidation,
}

impl TradePolicyArtifactPayload {
    /// Return every hard publication blocker derived from immutable evidence.
    #[must_use]
    pub fn publication_blockers(&self) -> Vec<String> {
        let mut blockers = self.validation.failure_reasons.clone();
        if !self.validation.passed {
            blockers.push("policy validation has not passed".to_owned());
        }
        if self.execution_evidence.full_l2_coverage < self.fit_contract.minimum_executable_coverage
        {
            blockers.push(format!(
                "full L2 coverage {} is below required {}",
                self.execution_evidence.full_l2_coverage,
                self.fit_contract.minimum_executable_coverage
            ));
        }
        if !self.execution_evidence.fees_included {
            blockers.push("executable return fitting does not include venue fees".to_owned());
        }
        blockers.sort();
        blockers.dedup();
        blockers
    }
}

/// Catalog view that combines immutable payload with mutable governance state.
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
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{
        TRADE_POLICY_ARTIFACT_FORMAT_VERSION, TradePolicyArtifactPayload,
        TradePolicyExecutionEvidence, TradePolicyFitContract, TradePolicyValidation,
    };
    use crate::types::{ContentHash, RuntimeConfigVersionId, TrainingDatasetId, Usd};

    fn hash(digit: char) -> ContentHash {
        ContentHash::parse(format!("blake3:{}", digit.to_string().repeat(64)))
            .expect("canonical hash")
    }

    fn contract() -> TradePolicyFitContract {
        TradePolicyFitContract {
            source_dataset_id: TrainingDatasetId::from_v7(),
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            fit_window_start: Utc.timestamp_opt(1_700_000_000, 0).single().expect("time"),
            fit_window_end: Utc.timestamp_opt(1_700_086_400, 0).single().expect("time"),
            embargo_secs: 3_600,
            notional_tiers: vec![Usd::new(dec!(25)), Usd::new(dec!(100))],
            maximum_scale_out_targets: 3,
            minimum_executable_coverage: dec!(0.8),
        }
    }

    #[test]
    fn fit_contract_rejects_invalid_window_tiers_and_coverage() {
        let mut value = contract();
        value.fit_window_end = value.fit_window_start;
        assert!(value.validate().is_err());

        let mut value = contract();
        value.notional_tiers.clear();
        assert!(value.validate().is_err());

        let mut value = contract();
        value.minimum_executable_coverage = Decimal::ZERO;
        assert!(value.validate().is_err());
    }

    #[test]
    fn degraded_execution_evidence_blocks_publication_even_if_statistics_pass() {
        let payload = TradePolicyArtifactPayload {
            format_version: TRADE_POLICY_ARTIFACT_FORMAT_VERSION,
            fit_contract: contract(),
            source_dataset_hash: hash('1'),
            feature_schema_hash: hash('2'),
            label_schema_hash: hash('3'),
            fill_simulator_version: "top-of-book-degraded-v1".to_owned(),
            fee_model_hash: hash('4'),
            execution_evidence: TradePolicyExecutionEvidence {
                entry_basis: "decision_best_ask".to_owned(),
                exit_basis: "forward_best_bid".to_owned(),
                full_l2_sample_count: 0,
                degraded_top_of_book_sample_count: 100,
                full_l2_coverage: Decimal::ZERO,
                fees_included: false,
                degradation_reasons: vec!["top-of-book only".to_owned()],
            },
            cohorts: Vec::new(),
            validation: TradePolicyValidation {
                cpcv_path_count: 16,
                deflated_sharpe_ratio: dec!(0.99),
                probability_of_backtest_overfitting: dec!(0.1),
                executable_coverage: dec!(0.9),
                passed: true,
                failure_reasons: Vec::new(),
            },
        };

        let blockers = payload.publication_blockers();
        assert!(blockers.iter().any(|reason| reason.contains("full L2")));
        assert!(blockers.iter().any(|reason| reason.contains("venue fees")));
    }
}
