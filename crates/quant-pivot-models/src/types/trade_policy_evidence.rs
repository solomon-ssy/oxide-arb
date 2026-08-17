//! Typed row contracts for sealed trade-policy evidence objects.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    enums::{common::Side, execution::ExitReason, quant::OutcomeSide},
    types::{
        Bps, ContentHash, MarketId, Price, Shares, TokenId, TradePolicyCohortKey,
        TrainingExampleId, Usd,
    },
};

pub const POLICY_EVIDENCE_OBJECT_FORMAT_VERSION: u32 = 2;

/// One monthly expanding-window Polymarket OOS fold for the parameter-free
/// deadline-resolution and fitted DR-AS volatility benchmarks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StructuralVolatilityOosFoldRow {
    pub fold_index: u32,
    pub training_window_start: DateTime<Utc>,
    pub training_window_end: DateTime<Utc>,
    pub test_window_start: DateTime<Utc>,
    pub test_window_end: DateTime<Utc>,
    pub training_sample_count: u64,
    pub forecast_count: u64,
    pub test_volume_weight: Usd,
    pub fitted_nonnegative_k: Decimal,
    pub deadline_vw_interval_score: Decimal,
    pub dr_as_vw_interval_score: Decimal,
    pub deadline_volume_weighted_coverage: Decimal,
    pub dr_as_volume_weighted_coverage: Decimal,
}

/// Compact artifact-level identity and validity result for the structural
/// volatility benchmark. This is risk-model evidence, never an entry policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StructuralVolatilityOosEvidence {
    pub methodology_hash: ContentHash,
    pub active_update_only: bool,
    pub activity_proxy: String,
    pub minimum_contract_observations: u32,
    pub fold_count: u32,
    pub forecast_count: u64,
    pub deadline_vw_interval_score: Decimal,
    pub dr_as_vw_interval_score: Decimal,
    pub deadline_volume_weighted_coverage: Decimal,
    pub dr_as_volume_weighted_coverage: Decimal,
    pub valid: bool,
}

/// Why one candidate replay cannot enter the common-support matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradePolicyReplayGap {
    EmptyTimeline,
    NonMonotonicTimeline,
    EntryConditionUnavailable,
    EntryNotTriggered,
    EntryBookUnavailable,
    EntryBookStale,
    EntryDepthInsufficient,
    PassiveTradeCoverageUnavailable,
    PitFeeScheduleUnavailable,
    PitMakerRebateUnavailable,
    PassiveTermsDrift,
    PassiveCancelFillRace,
    ExitBookUnavailable,
    ExitBookStale,
    ExitDepthInsufficient,
    SignalReinferenceUnavailable,
    ResolutionEvidenceUnavailable,
    ResidualPositionUnresolved,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradePolicyEvidenceLiquidityRole {
    Maker,
    Taker,
    Resolution,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradePolicyEvidenceFillOutcome {
    Filled,
    Partial,
    Unfilled,
}

/// Why no maker-rebate schedule was visible at the replay boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradePolicyMakerRebateUnavailableReason {
    NotYetVisible,
    FeesFlagMissing,
    EnabledScheduleMissing,
    ScheduleIncomplete,
    InvalidSchedule,
    DisabledSchedulePresent,
    SourceMismatch,
}

/// Immutable maker-rebate lineage attached to a simulated maker fill.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", deny_unknown_fields)]
pub enum TradePolicyMakerRebateEvidence {
    NoProgram {
        terms_hash: ContentHash,
    },
    Available {
        terms_hash: ContentHash,
    },
    Unavailable {
        reason: TradePolicyMakerRebateUnavailableReason,
    },
}

/// Point-in-time inputs proven available for one replay observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradePolicyObservationCapability {
    FullL2,
    PitFeeSchedule,
    PitMakerRebateEvidence,
    ModelReinference,
    WeatherLinkage,
}

/// Governed latency scenarios for common-candidate support.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradePolicyLatencyScenario {
    Base1x,
    Stress2x,
}

/// Eligibility and immutable lineage of one source observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradePolicyObservationEligibilityRow {
    pub example_id: TrainingExampleId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub decision_at: DateTime<Utc>,
    pub label_horizon_end: DateTime<Utc>,
    pub cohort_hash: ContentHash,
    pub candidate_count: u32,
    pub available_capabilities: BTreeSet<TradePolicyObservationCapability>,
    pub common_candidate_eligible_scenarios: BTreeSet<TradePolicyLatencyScenario>,
}

/// One entry, exit, or resolution leg produced by the shared replay kernel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradePolicyFillEvidenceRow {
    pub example_id: TrainingExampleId,
    pub cohort_hash: ContentHash,
    pub candidate_id: String,
    pub outcome_side: OutcomeSide,
    pub latency_multiplier: Decimal,
    pub leg_ordinal: u32,
    pub side: Side,
    pub exit_reason: Option<ExitReason>,
    pub triggered_at: DateTime<Utc>,
    pub filled_at: DateTime<Utc>,
    pub liquidity_role: TradePolicyEvidenceLiquidityRole,
    pub outcome: TradePolicyEvidenceFillOutcome,
    pub requested_shares: Option<Shares>,
    pub filled_shares: Shares,
    pub vwap: Option<Price>,
    pub gross_amount: Usd,
    pub execution_fee_usd: Usd,
    pub expected_maker_rebate_accrual_usd: Usd,
    pub risk_cash_delta: Decimal,
    pub fee_schedule_hash: Option<ContentHash>,
    pub maker_rebate_evidence: Option<TradePolicyMakerRebateEvidence>,
    pub stream_session_id: Option<Uuid>,
    pub token_sequence: Option<u64>,
    pub source_event_hash: Option<ContentHash>,
}

/// Availability state for one point-in-time replay evidence family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradePolicyEvidenceCoverage {
    NotRequired,
    Covered,
    Missing,
}

impl TradePolicyEvidenceCoverage {
    #[must_use]
    pub const fn is_covered(self) -> bool {
        matches!(self, Self::Covered)
    }

    #[must_use]
    pub const fn is_applicable(self) -> bool {
        !matches!(self, Self::NotRequired)
    }
}

impl From<bool> for TradePolicyEvidenceCoverage {
    fn from(covered: bool) -> Self {
        if covered {
            Self::Covered
        } else {
            Self::Missing
        }
    }
}

/// Terminal executable result for one observation/candidate/latency scenario.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradePolicyCandidateTrialRow {
    pub example_id: TrainingExampleId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub candidate_id: String,
    pub cohort_hash: ContentHash,
    pub outcome_side: OutcomeSide,
    pub latency_multiplier: Decimal,
    pub entry_triggered_at: Option<DateTime<Utc>>,
    pub entered_at: Option<DateTime<Utc>>,
    pub terminal_at: Option<DateTime<Utc>>,
    pub terminal_reason: Option<ExitReason>,
    pub entry_fill_ratio: Decimal,
    pub entry_fill_latency_ms: Option<u64>,
    pub post_fill_markout_bps: Option<Bps>,
    pub exit_fill_ratio: Decimal,
    pub entry_filled_shares: Shares,
    pub exited_shares: Shares,
    pub execution_fee_usd: Usd,
    pub expected_maker_rebate_accrual_usd: Usd,
    pub expected_net_return_bps: Option<Decimal>,
    pub risk_net_return_bps: Option<Decimal>,
    pub ambiguous_touch: bool,
    pub full_l2_coverage: TradePolicyEvidenceCoverage,
    pub fee_coverage: TradePolicyEvidenceCoverage,
    pub passive_rebate_evidence_coverage: TradePolicyEvidenceCoverage,
    pub passive_reconciled_trade_coverage: Option<TradePolicyEvidenceCoverage>,
    pub gap: Option<TradePolicyReplayGap>,
}

/// Candidate-level aggregate inside one immutable cohort and latency scenario.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradePolicyCohortTrialRow {
    pub cohort: TradePolicyCohortKey,
    pub cohort_hash: ContentHash,
    pub candidate_id: String,
    pub latency_multiplier: Decimal,
    pub sample_count: u64,
    pub effective_sample_size: Decimal,
    pub weighted_mean_expected_return_bps: Decimal,
    pub weighted_mean_risk_return_bps: Decimal,
    pub expected_sharpe_ratio: Decimal,
    pub executable_coverage: Decimal,
    pub full_l2_coverage: Decimal,
    pub fee_catalog_coverage: Decimal,
    pub passive_rebate_evidence_coverage: Option<Decimal>,
    pub ambiguous_touch_rate: Decimal,
    pub depth_failure_rate: Decimal,
}

/// One complete φ-path, never one of the 56 test combinations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradePolicyCpcvPathRow {
    pub cohort_hash: ContentHash,
    pub latency_multiplier: Decimal,
    pub path_index: u32,
    pub expected_group_returns: Vec<Decimal>,
    pub risk_group_returns: Vec<Decimal>,
    pub expected_sharpe_ratio: Decimal,
    pub risk_max_drawdown: Decimal,
    pub risk_tail_loss: Decimal,
}

/// Explicit excluded observation/candidate scenario and its evidence reason.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradePolicyCoverageGapRow {
    pub example_id: TrainingExampleId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub candidate_id: Option<String>,
    pub cohort_hash: Option<ContentHash>,
    pub latency_multiplier: Option<Decimal>,
    pub decision_at: DateTime<Utc>,
    pub gap: TradePolicyReplayGap,
    pub detail: String,
}

/// Final method/gate summary for one cohort and latency scenario.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradePolicyStatisticalSummaryRow {
    pub cohort_hash: ContentHash,
    pub selected_candidate_id: String,
    pub latency_multiplier: Decimal,
    pub sample_count: u64,
    pub common_sample_count: u64,
    pub common_candidate_support: Decimal,
    pub effective_sample_size: Decimal,
    pub cpcv_combination_count: u64,
    pub cpcv_path_count: u32,
    pub deflated_sharpe_ratio: Decimal,
    pub dsr_benchmark_sharpe: Decimal,
    pub probability_of_backtest_overfitting: Decimal,
    pub lower_confidence_utility_bps: Bps,
    pub passed: bool,
}
