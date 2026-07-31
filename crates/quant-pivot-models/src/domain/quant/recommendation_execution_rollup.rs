//! Final recommendation-level execution truth sealed from every terminal attempt.

use chrono::{DateTime, Utc};
use quant_pivot_error::hashing::CanonicalDigestError;
use sea_orm::DerivePartialModel;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    entities::{
        quant_recommendation_execution_rollup, quant_recommendation_execution_rollup_attempt,
    },
    enums::quant::ExecutionAttemptTerminalState,
    hashing::CanonicalDigest,
    types::{ContentHash, OrderIntentId, RecommendationId, Shares, Usd},
};

use super::ExecutionAttemptOutcomeInfo;

/// Repository-ready aggregate before database availability and final digest assignment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewRecommendationExecutionRollup {
    pub recommendation_id: RecommendationId,
    pub intent_count: i32,
    pub attempt_count: i32,
    pub unfilled_attempt_count: i32,
    pub partially_filled_attempt_count: i32,
    pub fully_filled_attempt_count: i32,
    pub total_requested_shares: Shares,
    pub total_filled_shares: Shares,
    pub total_entry_fee_usd: Option<Usd>,
    pub total_exit_fee_usd: Option<Usd>,
    pub total_settlement_payout_usd: Option<Usd>,
    pub total_realized_pnl_usd: Usd,
    pub first_attempt_terminal_at: Option<DateTime<Utc>>,
    pub last_attempt_terminal_at: Option<DateTime<Utc>>,
    pub terminal_at: DateTime<Utc>,
    pub source_observed_at: DateTime<Utc>,
    pub attempt_set_hash: ContentHash,
}

impl NewRecommendationExecutionRollup {
    /// Deterministically aggregate every submitted terminal attempt.
    pub fn aggregate(
        recommendation_id: RecommendationId,
        intent_count: usize,
        terminal_at: DateTime<Utc>,
        source_observed_at: DateTime<Utc>,
        mut attempts: Vec<ExecutionAttemptOutcomeInfo>,
    ) -> Result<RecommendationExecutionRollupSeal, RecommendationExecutionRollupContractError> {
        let intent_count = i32::try_from(intent_count)
            .map_err(|_| RecommendationExecutionRollupContractError::CountOverflow)?;
        attempts.sort_by_key(|attempt| (attempt.terminal_at, attempt.order_intent_id.as_uuid()));
        if attempts
            .windows(2)
            .any(|pair| pair[0].order_intent_id == pair[1].order_intent_id)
            || attempts.iter().any(|attempt| {
                attempt.recommendation_id != recommendation_id
                    || attempt.terminal_at > terminal_at
                    || attempt.available_at > source_observed_at
            })
        {
            return Err(RecommendationExecutionRollupContractError::AttemptSetMismatch);
        }
        for attempt in &attempts {
            attempt.validate().map_err(|error| {
                RecommendationExecutionRollupContractError::InvalidAttempt(error.to_string())
            })?;
        }
        let attempt_count = i32::try_from(attempts.len())
            .map_err(|_| RecommendationExecutionRollupContractError::CountOverflow)?;
        let unfilled_attempt_count =
            count_attempts(&attempts, ExecutionAttemptTerminalState::Unfilled)?;
        let partially_filled_attempt_count =
            count_attempts(&attempts, ExecutionAttemptTerminalState::PartiallyFilled)?;
        let fully_filled_attempt_count =
            count_attempts(&attempts, ExecutionAttemptTerminalState::FullyFilled)?;
        if attempt_count > intent_count {
            return Err(RecommendationExecutionRollupContractError::AttemptSetMismatch);
        }
        let bindings = attempts
            .iter()
            .enumerate()
            .map(
                |(sequence, attempt)| -> Result<
                    NewRecommendationExecutionRollupAttempt,
                    RecommendationExecutionRollupContractError,
                > {
                    Ok(NewRecommendationExecutionRollupAttempt {
                        recommendation_id,
                        sequence: i32::try_from(sequence).map_err(|_| {
                            RecommendationExecutionRollupContractError::CountOverflow
                        })?,
                        order_intent_id: attempt.order_intent_id,
                        attempt_outcome_hash: attempt.outcome_hash,
                        terminal_at: attempt.terminal_at,
                    })
                },
            )
            .collect::<Result<Vec<_>, _>>()?;
        let attempt_set_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/recommendation-execution-attempt-set",
            1,
            &bindings,
        )?;
        let first_attempt_terminal_at = attempts.first().map(|attempt| attempt.terminal_at);
        let last_attempt_terminal_at = attempts.last().map(|attempt| attempt.terminal_at);
        let filled_attempts = attempts
            .iter()
            .filter(|attempt| attempt.filled_shares.is_positive())
            .collect::<Vec<_>>();
        let settlement_payouts = attempts
            .iter()
            .filter_map(|attempt| attempt.settlement_payout_usd)
            .collect::<Vec<_>>();
        let rollup = Self {
            recommendation_id,
            intent_count,
            attempt_count,
            unfilled_attempt_count,
            partially_filled_attempt_count,
            fully_filled_attempt_count,
            total_requested_shares: attempts
                .iter()
                .map(|attempt| attempt.requested_shares)
                .sum(),
            total_filled_shares: attempts.iter().map(|attempt| attempt.filled_shares).sum(),
            total_entry_fee_usd: sum_optional(attempts.iter().map(|attempt| attempt.entry_fee_usd)),
            total_exit_fee_usd: sum_optional(
                filled_attempts.iter().map(|attempt| attempt.exit_fee_usd),
            ),
            total_settlement_payout_usd: (!settlement_payouts.is_empty())
                .then(|| settlement_payouts.into_iter().sum()),
            total_realized_pnl_usd: attempts
                .iter()
                .map(|attempt| attempt.realized_pnl_usd.unwrap_or(Usd::ZERO))
                .sum(),
            first_attempt_terminal_at,
            last_attempt_terminal_at,
            terminal_at,
            source_observed_at,
            attempt_set_hash,
        };
        rollup.validate()?;
        Ok(RecommendationExecutionRollupSeal { rollup, bindings })
    }

    /// Compute the final immutable digest with `PostgreSQL` availability.
    pub fn expected_rollup_hash(
        &self,
        available_at: DateTime<Utc>,
    ) -> Result<ContentHash, RecommendationExecutionRollupContractError> {
        self.validate()?;
        if available_at < self.source_observed_at {
            return Err(RecommendationExecutionRollupContractError::InvalidTimeline);
        }
        Ok(CanonicalDigest::content_hash_typed(
            "quant-pivot/recommendation-execution-rollup",
            1,
            &RecommendationExecutionRollupHashInput::from_new(self, available_at),
        )?)
    }

    /// Validate count, economic, and lifecycle invariants.
    pub fn validate(&self) -> Result<(), RecommendationExecutionRollupContractError> {
        if self.intent_count < 0
            || self.attempt_count < 0
            || self.attempt_count > self.intent_count
            || self.unfilled_attempt_count < 0
            || self.partially_filled_attempt_count < 0
            || self.fully_filled_attempt_count < 0
            || self.unfilled_attempt_count
                + self.partially_filled_attempt_count
                + self.fully_filled_attempt_count
                != self.attempt_count
        {
            return Err(RecommendationExecutionRollupContractError::InvalidCounts);
        }
        if self.total_requested_shares.is_negative()
            || self.total_filled_shares.is_negative()
            || self.total_filled_shares > self.total_requested_shares
            || self
                .total_entry_fee_usd
                .is_some_and(|value| value.is_negative())
            || self
                .total_exit_fee_usd
                .is_some_and(|value| value.is_negative())
            || self
                .total_settlement_payout_usd
                .is_some_and(|value| value.is_negative())
        {
            return Err(RecommendationExecutionRollupContractError::InvalidEconomics);
        }
        let attempt_timeline = match (
            self.attempt_count,
            self.first_attempt_terminal_at,
            self.last_attempt_terminal_at,
        ) {
            (0, None, None) => true,
            (count, Some(first), Some(last)) if count > 0 => first <= last,
            _ => false,
        };
        if !attempt_timeline
            || self
                .last_attempt_terminal_at
                .is_some_and(|last| last > self.terminal_at)
            || self.terminal_at > self.source_observed_at
        {
            return Err(RecommendationExecutionRollupContractError::InvalidTimeline);
        }
        Ok(())
    }
}

fn sum_optional(mut values: impl Iterator<Item = Option<Usd>>) -> Option<Usd> {
    values.try_fold(Usd::ZERO, |total, value| value.map(|value| total + value))
}

fn count_attempts(
    attempts: &[ExecutionAttemptOutcomeInfo],
    state: ExecutionAttemptTerminalState,
) -> Result<i32, RecommendationExecutionRollupContractError> {
    i32::try_from(
        attempts
            .iter()
            .filter(|attempt| attempt.terminal_state == state)
            .count(),
    )
    .map_err(|_| RecommendationExecutionRollupContractError::CountOverflow)
}

/// One ordered binding included by a final recommendation rollup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewRecommendationExecutionRollupAttempt {
    pub recommendation_id: RecommendationId,
    pub sequence: i32,
    pub order_intent_id: OrderIntentId,
    pub attempt_outcome_hash: ContentHash,
    pub terminal_at: DateTime<Utc>,
}

/// Aggregate plus its complete ordered attempt membership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecommendationExecutionRollupSeal {
    pub rollup: NewRecommendationExecutionRollup,
    pub bindings: Vec<NewRecommendationExecutionRollupAttempt>,
}

/// Immutable final recommendation execution rollup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_recommendation_execution_rollup::Entity")]
pub struct RecommendationExecutionRollupInfo {
    pub recommendation_id: RecommendationId,
    pub intent_count: i32,
    pub attempt_count: i32,
    pub unfilled_attempt_count: i32,
    pub partially_filled_attempt_count: i32,
    pub fully_filled_attempt_count: i32,
    pub total_requested_shares: Shares,
    pub total_filled_shares: Shares,
    pub total_entry_fee_usd: Option<Usd>,
    pub total_exit_fee_usd: Option<Usd>,
    pub total_settlement_payout_usd: Option<Usd>,
    pub total_realized_pnl_usd: Usd,
    pub first_attempt_terminal_at: Option<DateTime<Utc>>,
    pub last_attempt_terminal_at: Option<DateTime<Utc>>,
    pub terminal_at: DateTime<Utc>,
    pub source_observed_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub attempt_set_hash: ContentHash,
    pub rollup_hash: ContentHash,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    RecommendationExecutionRollupInfo,
    quant_recommendation_execution_rollup::Model,
    {
        recommendation_id,
        intent_count,
        attempt_count,
        unfilled_attempt_count,
        partially_filled_attempt_count,
        fully_filled_attempt_count,
        total_requested_shares,
        total_filled_shares,
        total_entry_fee_usd,
        total_exit_fee_usd,
        total_settlement_payout_usd,
        total_realized_pnl_usd,
        first_attempt_terminal_at,
        last_attempt_terminal_at,
        terminal_at,
        source_observed_at,
        available_at,
        attempt_set_hash,
        rollup_hash,
        created_at,
    }
);

impl RecommendationExecutionRollupInfo {
    /// Validate content, timeline, and final content address.
    pub fn validate(&self) -> Result<(), RecommendationExecutionRollupContractError> {
        let candidate = self.as_new();
        candidate.validate()?;
        if self.available_at < self.source_observed_at || self.created_at != self.available_at {
            return Err(RecommendationExecutionRollupContractError::InvalidTimeline);
        }
        let expected = candidate.expected_rollup_hash(self.available_at)?;
        if expected != self.rollup_hash {
            return Err(
                RecommendationExecutionRollupContractError::RollupHashMismatch {
                    expected,
                    actual: self.rollup_hash,
                },
            );
        }
        Ok(())
    }

    /// Restore the producer fields used for exact idempotency comparison.
    #[must_use]
    pub const fn as_new(&self) -> NewRecommendationExecutionRollup {
        NewRecommendationExecutionRollup {
            recommendation_id: self.recommendation_id,
            intent_count: self.intent_count,
            attempt_count: self.attempt_count,
            unfilled_attempt_count: self.unfilled_attempt_count,
            partially_filled_attempt_count: self.partially_filled_attempt_count,
            fully_filled_attempt_count: self.fully_filled_attempt_count,
            total_requested_shares: self.total_requested_shares,
            total_filled_shares: self.total_filled_shares,
            total_entry_fee_usd: self.total_entry_fee_usd,
            total_exit_fee_usd: self.total_exit_fee_usd,
            total_settlement_payout_usd: self.total_settlement_payout_usd,
            total_realized_pnl_usd: self.total_realized_pnl_usd,
            first_attempt_terminal_at: self.first_attempt_terminal_at,
            last_attempt_terminal_at: self.last_attempt_terminal_at,
            terminal_at: self.terminal_at,
            source_observed_at: self.source_observed_at,
            attempt_set_hash: self.attempt_set_hash,
        }
    }
}

/// Stored attempt membership row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_recommendation_execution_rollup_attempt::Entity")]
pub struct RecommendationExecutionRollupAttemptInfo {
    pub recommendation_id: RecommendationId,
    pub sequence: i32,
    pub order_intent_id: OrderIntentId,
    pub attempt_outcome_hash: ContentHash,
    pub terminal_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    RecommendationExecutionRollupAttemptInfo,
    quant_recommendation_execution_rollup_attempt::Model,
    {
        recommendation_id,
        sequence,
        order_intent_id,
        attempt_outcome_hash,
        terminal_at,
        created_at,
    }
);

/// A valid recommendation graph that cannot yet be sealed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionRollupDeferredReason {
    RecommendationAuthorityOpen,
    SourceAvailableAfterCutoff,
    IntentStillOpen,
    AttemptOutcomeMissing,
    OrderNotTerminal,
    PositionNotTerminal,
}

/// Result of one final recommendation rollup reconciliation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionRollupReconciliationResult {
    Inserted(RecommendationExecutionRollupInfo),
    AlreadyPresent(RecommendationExecutionRollupInfo),
    Deferred(ExecutionRollupDeferredReason),
}

/// Point-in-time execution rollup coverage for feedback truth freeze.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionRollupBarrier {
    pub cutoff: DateTime<Utc>,
    pub eligible_unsealed_count: u64,
    pub sealed_through: DateTime<Utc>,
}

impl ExecutionRollupBarrier {
    #[must_use]
    pub fn is_complete(self) -> bool {
        self.eligible_unsealed_count == 0 && self.sealed_through >= self.cutoff
    }
}

/// Invalid final rollup content.
#[derive(Debug, Error)]
pub enum RecommendationExecutionRollupContractError {
    #[error("execution rollup count exceeds its supported range")]
    CountOverflow,
    #[error("attempt set is duplicated, foreign, or outside the frozen frontier")]
    AttemptSetMismatch,
    #[error("attempt outcome failed validation: {0}")]
    InvalidAttempt(String),
    #[error("execution rollup counts are inconsistent")]
    InvalidCounts,
    #[error("execution rollup economics are negative or overfilled")]
    InvalidEconomics,
    #[error("execution rollup timeline is inconsistent")]
    InvalidTimeline,
    #[error("rollup hash mismatch: expected {expected}, got {actual}")]
    RollupHashMismatch {
        expected: ContentHash,
        actual: ContentHash,
    },
    #[error(transparent)]
    Hashing(#[from] CanonicalDigestError),
}

#[derive(Serialize)]
struct RecommendationExecutionRollupHashInput<'a> {
    recommendation_id: RecommendationId,
    intent_count: i32,
    attempt_count: i32,
    unfilled_attempt_count: i32,
    partially_filled_attempt_count: i32,
    fully_filled_attempt_count: i32,
    total_requested_shares: Shares,
    total_filled_shares: Shares,
    total_entry_fee_usd: Option<Usd>,
    total_exit_fee_usd: Option<Usd>,
    total_settlement_payout_usd: Option<Usd>,
    total_realized_pnl_usd: Usd,
    first_attempt_terminal_at: Option<DateTime<Utc>>,
    last_attempt_terminal_at: Option<DateTime<Utc>>,
    terminal_at: DateTime<Utc>,
    source_observed_at: DateTime<Utc>,
    available_at: DateTime<Utc>,
    attempt_set_hash: ContentHash,
    _contract: &'a str,
}

impl RecommendationExecutionRollupHashInput<'_> {
    const fn from_new(
        rollup: &NewRecommendationExecutionRollup,
        available_at: DateTime<Utc>,
    ) -> RecommendationExecutionRollupHashInput<'static> {
        RecommendationExecutionRollupHashInput {
            recommendation_id: rollup.recommendation_id,
            intent_count: rollup.intent_count,
            attempt_count: rollup.attempt_count,
            unfilled_attempt_count: rollup.unfilled_attempt_count,
            partially_filled_attempt_count: rollup.partially_filled_attempt_count,
            fully_filled_attempt_count: rollup.fully_filled_attempt_count,
            total_requested_shares: rollup.total_requested_shares,
            total_filled_shares: rollup.total_filled_shares,
            total_entry_fee_usd: rollup.total_entry_fee_usd,
            total_exit_fee_usd: rollup.total_exit_fee_usd,
            total_settlement_payout_usd: rollup.total_settlement_payout_usd,
            total_realized_pnl_usd: rollup.total_realized_pnl_usd,
            first_attempt_terminal_at: rollup.first_attempt_terminal_at,
            last_attempt_terminal_at: rollup.last_attempt_terminal_at,
            terminal_at: rollup.terminal_at,
            source_observed_at: rollup.source_observed_at,
            available_at,
            attempt_set_hash: rollup.attempt_set_hash,
            _contract: "recommendation_execution_rollup_v1",
        }
    }
}
