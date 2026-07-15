//! Typed row contracts for sealed trade-policy evidence objects.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    enums::{common::Side, execution::ExitReason, quant::OutcomeSide},
    types::{
        Bps, ContentHash, MarketId, Price, Shares, TokenId, TradePolicyCohortKey,
        TrainingExampleId, Usd,
    },
};

pub const POLICY_EVIDENCE_OBJECT_FORMAT_VERSION: u32 = 1;

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

/// Point-in-time inputs proven available for one replay observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradePolicyObservationCapability {
    FullL2,
    PitFeeSchedule,
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
    pub fee: Usd,
    pub cash_delta: Decimal,
    pub fee_schedule_hash: Option<ContentHash>,
    pub stream_session_id: Option<uuid::Uuid>,
    pub token_sequence: Option<u64>,
    pub source_event_hash: Option<ContentHash>,
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
    pub exit_fill_ratio: Decimal,
    pub entry_filled_shares: Shares,
    pub exited_shares: Shares,
    pub total_fees: Usd,
    pub net_return_bps: Option<Decimal>,
    pub ambiguous_touch: bool,
    pub full_l2: bool,
    pub fee_covered: bool,
    pub passive_reconciled_trade_covered: Option<bool>,
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
    pub weighted_mean_return_bps: Decimal,
    pub sharpe_ratio: Decimal,
    pub executable_coverage: Decimal,
    pub full_l2_coverage: Decimal,
    pub fee_catalog_coverage: Decimal,
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
    pub group_returns: Vec<Decimal>,
    pub sharpe_ratio: Decimal,
    pub max_drawdown: Decimal,
    pub tail_loss: Decimal,
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
