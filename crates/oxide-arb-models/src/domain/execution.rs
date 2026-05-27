//! Execution pipeline domain types.

use crate::{
    enums::{
        common::{Side, StalenessLevel},
        execution::ExecutionOutcome,
    },
    types::{
        Bps, EventId, ExecutionId, MarketId, OpportunityId, OrderId, Price, ReservationId, Shares,
        TokenId, Usd,
    },
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::fmt::Display;

/// All information needed to place a single order.
#[derive(Debug, Clone, Serialize)]
pub struct ExecutionPlan {
    pub execution_id: ExecutionId,
    pub opportunity_id: OpportunityId,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_id: TokenId,
    pub side: Side,
    pub shares: Shares,
    pub limit_price: Price,
    pub estimated_cost: Usd,
    pub estimated_fee: Usd,
    pub neg_risk: bool,
    pub reservation_id: ReservationId,
    pub detected_at: DateTime<Utc>,
    pub planned_at: DateTime<Utc>,
}

/// Lightweight execution outcome for pipeline results — no clone of full [`ExecutionOutcome`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum ExecutionOutcomeSummary {
    Filled { order_id: OrderId },
    Miss,
    Failed,
}

impl ExecutionOutcomeSummary {
    #[must_use]
    pub fn from_outcome(outcome: &ExecutionOutcome) -> Self {
        match outcome {
            ExecutionOutcome::Filled { order_id, .. } => Self::Filled {
                order_id: order_id.clone(),
            },
            ExecutionOutcome::Miss { .. } => Self::Miss,
            ExecutionOutcome::Failed { .. } => Self::Failed,
        }
    }
}

/// Result of the full execution pipeline.
#[derive(Debug, Clone, Serialize)]
pub struct ExecutionResult {
    pub outcome_summary: Option<ExecutionOutcomeSummary>,
    pub rejection_reason: Option<String>,
    pub rejection_stage: Option<String>,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
}

impl ExecutionResult {
    #[must_use]
    pub fn completed(summary: ExecutionOutcomeSummary) -> Self {
        let now = Utc::now();
        Self {
            outcome_summary: Some(summary),
            rejection_reason: None,
            rejection_stage: None,
            started_at: now,
            completed_at: now,
        }
    }

    #[must_use]
    pub fn rejected(stage: &str, reason: impl Display) -> Self {
        let now = Utc::now();
        Self {
            outcome_summary: None,
            rejection_reason: Some(reason.to_string()),
            rejection_stage: Some(stage.into()),
            started_at: now,
            completed_at: now,
        }
    }

    #[must_use]
    pub const fn is_filled(&self) -> bool {
        matches!(
            self.outcome_summary,
            Some(ExecutionOutcomeSummary::Filled { .. })
        )
    }

    #[must_use]
    pub const fn is_miss(&self) -> bool {
        matches!(self.outcome_summary, Some(ExecutionOutcomeSummary::Miss))
    }

    #[must_use]
    pub const fn is_rejected(&self) -> bool {
        self.rejection_reason.is_some()
    }
}

/// Validation result snapshot.
#[derive(Debug, Clone, Serialize)]
pub struct ValidationResult {
    pub current_price: Price,
    pub staleness: StalenessLevel,
    pub slippage_bps: Bps,
    pub validated_at: DateTime<Utc>,
}

/// Handle to an active capital reservation.
#[derive(Debug, Clone)]
pub struct ReservationHandle {
    pub id: ReservationId,
    pub amount: Usd,
    pub market_id: MarketId,
}
