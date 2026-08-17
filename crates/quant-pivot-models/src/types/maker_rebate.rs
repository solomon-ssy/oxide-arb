//! Account evidence and objective-credit semantics for maker rebates.

use chrono::{DateTime, NaiveDate, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::types::{ContentHash, Usd};

/// Point-in-time health of the account evidence required to value an
/// unreceived maker rebate in the expected objective.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MakerRebateValuationHealth {
    Healthy,
    Stale,
    Incomplete,
    Unavailable,
}

impl MakerRebateValuationHealth {
    #[must_use]
    pub const fn metric_label(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Stale => "stale",
            Self::Incomplete => "incomplete",
            Self::Unavailable => "unavailable",
        }
    }
}

/// Typed reason why nominal maker accrual receives zero objective credit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MakerRebateObjectiveZeroReason {
    BelowPayoutThreshold,
    ReconciliationStale,
    ReconciliationIncomplete,
    ReconciliationUnavailable,
}

impl MakerRebateObjectiveZeroReason {
    #[must_use]
    pub const fn metric_label(self) -> &'static str {
        match self {
            Self::BelowPayoutThreshold => "below_threshold",
            Self::ReconciliationStale => "reconciliation_stale",
            Self::ReconciliationIncomplete => "reconciliation_incomplete",
            Self::ReconciliationUnavailable => "reconciliation_unavailable",
        }
    }
}

/// Frozen payout-lag evidence. Lag is measured from the close of the maker
/// fill's UTC program day, never from a later API observation timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "kind", deny_unknown_fields)]
pub enum MakerRebateDelayBasis {
    ConservativeFallback {
        lag_from_program_close_secs: u64,
    },
    ObservedP95 {
        lag_from_program_close_secs: u64,
        complete_program_days: u32,
    },
}

impl MakerRebateDelayBasis {
    #[must_use]
    pub const fn lag_secs(self) -> u64 {
        match self {
            Self::ConservativeFallback {
                lag_from_program_close_secs,
            }
            | Self::ObservedP95 {
                lag_from_program_close_secs,
                ..
            } => lag_from_program_close_secs,
        }
    }
}

/// Confirmed local maker-fill accrual already accumulated on one UTC program
/// day before the current report's hypothetical fills are added.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MakerRebateProgramDayBaseline {
    pub program_date: NaiveDate,
    pub confirmed_accrual_usd: Usd,
}

/// Operator-facing status of a recommendation's maker-rebate objective.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "state", deny_unknown_fields)]
pub enum MakerRebateObjectiveStatus {
    NotApplicable,
    NoProgram,
    Zero {
        reason: MakerRebateObjectiveZeroReason,
    },
    ScenarioWeighted {
        credited_probability_bps: u32,
    },
}

/// Day-local payout eligibility for one promoted joint scenario.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MakerRebateScenarioCreditStatus {
    NotApplicable,
    NoAccrual,
    BelowDailyThreshold,
    Credited,
}

/// Frozen account-level evidence used by all passive tiers in one report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MakerRebateValuationEvidence {
    pub as_of: DateTime<Utc>,
    pub health: MakerRebateValuationHealth,
    pub program_day_baselines: Vec<MakerRebateProgramDayBaseline>,
    pub payout_threshold_usd: Usd,
    pub delay_basis: MakerRebateDelayBasis,
    pub evidence_hash: ContentHash,
}

impl MakerRebateValuationEvidence {
    /// Evidence-level reason that prevents all maker rebate objective credit.
    #[must_use]
    pub const fn evidence_zero_reason(&self) -> Option<MakerRebateObjectiveZeroReason> {
        match self.health {
            MakerRebateValuationHealth::Healthy => None,
            MakerRebateValuationHealth::Stale => {
                Some(MakerRebateObjectiveZeroReason::ReconciliationStale)
            }
            MakerRebateValuationHealth::Incomplete => {
                Some(MakerRebateObjectiveZeroReason::ReconciliationIncomplete)
            }
            MakerRebateValuationHealth::Unavailable => {
                Some(MakerRebateObjectiveZeroReason::ReconciliationUnavailable)
            }
        }
    }

    #[must_use]
    pub fn baseline_for(&self, program_date: NaiveDate) -> Usd {
        self.program_day_baselines
            .iter()
            .find(|baseline| baseline.program_date == program_date)
            .map_or(Usd::ZERO, |baseline| baseline.confirmed_accrual_usd)
    }
}
