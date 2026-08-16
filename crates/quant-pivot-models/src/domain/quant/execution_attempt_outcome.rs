//! Immutable execution-attempt outcome persistence contracts.

use chrono::{DateTime, Utc};
use quant_pivot_error::hashing::CanonicalDigestError;
use rust_decimal::Decimal;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    entities::quant_execution_attempt_outcome,
    enums::{
        execution::{ExitReason, PositionLedgerState},
        quant::{
            ExecutionAttemptNoFillReason, ExecutionAttemptTerminalState, ExecutionOrderState,
            QuantRuntimeMode,
        },
    },
    hashing::CanonicalDigest,
    types::{
        ContentHash, ExecutionAccountId, ExecutionOrderId, MarketId, OrderIntentId, PositionId,
        Price, RecommendationId, ReconciliationId, SchemaVersion, Shares, TokenId, Usd,
    },
};

/// Insert payload for the WORM recommendation-execution outcome ledger.
///
/// Database availability and the final outcome digest are repository-owned.
/// The producer supplies only terminal economic facts and their frozen source
/// lineage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_execution_attempt_outcome::ActiveModel")]
pub struct NewExecutionAttemptOutcome {
    pub recommendation_id: RecommendationId,
    pub order_intent_id: OrderIntentId,
    pub entry_execution_order_id: ExecutionOrderId,
    pub entry_reconciliation_id: ReconciliationId,
    pub position_id: Option<PositionId>,
    pub execution_account_id: ExecutionAccountId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub runtime_mode: QuantRuntimeMode,
    pub terminal_state: ExecutionAttemptTerminalState,
    pub no_fill_reason: Option<ExecutionAttemptNoFillReason>,
    pub entry_order_state: ExecutionOrderState,
    pub requested_shares: Shares,
    pub filled_shares: Shares,
    pub entry_avg_price: Option<Price>,
    pub entry_fee_usd: Option<Usd>,
    pub entry_filled_at: Option<DateTime<Utc>>,
    pub position_terminal_state: Option<PositionLedgerState>,
    pub exit_reason: Option<ExitReason>,
    pub exit_filled_shares: Option<Shares>,
    pub exit_avg_price: Option<Price>,
    pub exit_fee_usd: Option<Usd>,
    pub exit_at: Option<DateTime<Utc>>,
    pub settlement_payout_usd: Option<Usd>,
    pub realized_pnl_usd: Option<Usd>,
    /// Time the execution lifecycle became terminal.
    pub terminal_at: DateTime<Utc>,
    /// Frozen source frontier selected by the reconciliation producer.
    pub source_checkpoint_hash: ContentHash,
    /// Digest of the exact intent/order/reconciliation/position/exit source graph.
    pub execution_fact_hash: ContentHash,
    pub execution_fact_schema_version: SchemaVersion,
}

impl NewExecutionAttemptOutcome {
    /// Validate immutable execution semantics before any database work.
    pub fn validate(&self) -> Result<(), ExecutionAttemptOutcomeContractError> {
        validate_fields(
            &ExecutionOutcomeHashInput::from_new(self, self.terminal_at, self.terminal_at),
            None,
        )
    }

    /// Compute the digest after the repository freezes source visibility and availability.
    pub fn expected_outcome_hash(
        &self,
        source_observed_at: DateTime<Utc>,
        available_at: DateTime<Utc>,
    ) -> Result<ContentHash, ExecutionAttemptOutcomeContractError> {
        let input = ExecutionOutcomeHashInput::from_new(self, source_observed_at, available_at);
        validate_fields(&input, None)?;
        Ok(ContentHash::try_from(&input)?)
    }

    /// Exact fill ratio derived from canonical share facts.
    pub fn fill_ratio(&self) -> Result<Decimal, ExecutionAttemptOutcomeContractError> {
        Decimal::try_from(ExecutionFill {
            requested: self.requested_shares,
            filled: self.filled_shares,
        })
    }
}

/// Immutable recommendation-execution outcome row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_execution_attempt_outcome::Entity")]
pub struct ExecutionAttemptOutcomeInfo {
    pub recommendation_id: RecommendationId,
    pub order_intent_id: OrderIntentId,
    pub entry_execution_order_id: ExecutionOrderId,
    pub entry_reconciliation_id: ReconciliationId,
    pub position_id: Option<PositionId>,
    pub execution_account_id: ExecutionAccountId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub runtime_mode: QuantRuntimeMode,
    pub terminal_state: ExecutionAttemptTerminalState,
    pub no_fill_reason: Option<ExecutionAttemptNoFillReason>,
    pub entry_order_state: ExecutionOrderState,
    pub requested_shares: Shares,
    pub filled_shares: Shares,
    pub entry_avg_price: Option<Price>,
    pub entry_fee_usd: Option<Usd>,
    pub entry_filled_at: Option<DateTime<Utc>>,
    pub position_terminal_state: Option<PositionLedgerState>,
    pub exit_reason: Option<ExitReason>,
    pub exit_filled_shares: Option<Shares>,
    pub exit_avg_price: Option<Price>,
    pub exit_fee_usd: Option<Usd>,
    pub exit_at: Option<DateTime<Utc>>,
    pub settlement_payout_usd: Option<Usd>,
    pub realized_pnl_usd: Option<Usd>,
    pub terminal_at: DateTime<Utc>,
    pub source_observed_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub source_checkpoint_hash: ContentHash,
    pub execution_fact_hash: ContentHash,
    pub execution_fact_schema_version: SchemaVersion,
    pub outcome_hash: ContentHash,
    pub created_at: DateTime<Utc>,
}

impl ExecutionAttemptOutcomeInfo {
    /// Recompute the canonical immutable digest.
    pub fn expected_outcome_hash(
        &self,
    ) -> Result<ContentHash, ExecutionAttemptOutcomeContractError> {
        let input = ExecutionOutcomeHashInput::from_info(self);
        validate_fields(&input, Some(self.created_at))?;
        Ok(ContentHash::try_from(&input)?)
    }

    /// Validate the stored row and its tamper-evident digest.
    pub fn validate(&self) -> Result<(), ExecutionAttemptOutcomeContractError> {
        let expected = self.expected_outcome_hash()?;
        if expected != self.outcome_hash {
            return Err(ExecutionAttemptOutcomeContractError::OutcomeHashMismatch {
                expected,
                actual: self.outcome_hash,
            });
        }
        Ok(())
    }

    /// Whether an idempotent retry carries the exact stored source derivation.
    #[must_use]
    pub fn has_same_derivation(&self, candidate: &NewExecutionAttemptOutcome) -> bool {
        self.recommendation_id == candidate.recommendation_id
            && self.order_intent_id == candidate.order_intent_id
            && self.entry_execution_order_id == candidate.entry_execution_order_id
            && self.entry_reconciliation_id == candidate.entry_reconciliation_id
            && self.position_id == candidate.position_id
            && self.execution_account_id == candidate.execution_account_id
            && self.market_id == candidate.market_id
            && self.token_id == candidate.token_id
            && self.runtime_mode == candidate.runtime_mode
            && self.terminal_state == candidate.terminal_state
            && self.no_fill_reason == candidate.no_fill_reason
            && self.entry_order_state == candidate.entry_order_state
            && self.requested_shares == candidate.requested_shares
            && self.filled_shares == candidate.filled_shares
            && self.entry_avg_price == candidate.entry_avg_price
            && self.entry_fee_usd == candidate.entry_fee_usd
            && self.entry_filled_at == candidate.entry_filled_at
            && self.position_terminal_state == candidate.position_terminal_state
            && self.exit_reason == candidate.exit_reason
            && self.exit_filled_shares == candidate.exit_filled_shares
            && self.exit_avg_price == candidate.exit_avg_price
            && self.exit_fee_usd == candidate.exit_fee_usd
            && self.exit_at == candidate.exit_at
            && self.settlement_payout_usd == candidate.settlement_payout_usd
            && self.realized_pnl_usd == candidate.realized_pnl_usd
            && self.terminal_at == candidate.terminal_at
            && self.source_checkpoint_hash == candidate.source_checkpoint_hash
            && self.execution_fact_hash == candidate.execution_fact_hash
            && self.execution_fact_schema_version == candidate.execution_fact_schema_version
    }

    /// Exact fill ratio derived from canonical share facts.
    pub fn fill_ratio(&self) -> Result<Decimal, ExecutionAttemptOutcomeContractError> {
        Decimal::try_from(ExecutionFill {
            requested: self.requested_shares,
            filled: self.filled_shares,
        })
    }
}

info_from_model!(
    ExecutionAttemptOutcomeInfo,
    quant_execution_attempt_outcome::Model,
    {
        recommendation_id,
        order_intent_id,
        entry_execution_order_id,
        entry_reconciliation_id,
        position_id,
        execution_account_id,
        market_id,
        token_id,
        runtime_mode,
        terminal_state,
        no_fill_reason,
        entry_order_state,
        requested_shares,
        filled_shares,
        entry_avg_price,
        entry_fee_usd,
        entry_filled_at,
        position_terminal_state,
        exit_reason,
        exit_filled_shares,
        exit_avg_price,
        exit_fee_usd,
        exit_at,
        settlement_payout_usd,
        realized_pnl_usd,
        terminal_at,
        source_observed_at,
        available_at,
        source_checkpoint_hash,
        execution_fact_hash,
        execution_fact_schema_version,
        outcome_hash,
        created_at,
    }
);

/// Invalid immutable execution-outcome content.
#[derive(Debug, Error)]
pub enum ExecutionAttemptOutcomeContractError {
    #[error("market_id and token_id must both be non-empty")]
    EmptyIdentity,
    #[error("ReportOnly and other non-submitting modes cannot produce an execution outcome")]
    ExecutionNotAuthorized,
    #[error("execution timeline is invalid")]
    InvalidTimeline,
    #[error("requested shares must be positive, got {actual}")]
    InvalidRequestedShares { actual: Shares },
    #[error("filled shares must be within 0..=requested ({requested}), got {actual}")]
    InvalidFilledShares { requested: Shares, actual: Shares },
    #[error("terminal state {terminal_state:?} is inconsistent with fill and entry-order facts")]
    TerminalStateMismatch {
        terminal_state: ExecutionAttemptTerminalState,
    },
    #[error("unfilled execution outcome contains a filled-position economic value")]
    UnfilledEconomicsPresent,
    #[error("filled execution outcome is missing required entry or terminal-position facts")]
    FilledEconomicsMissing,
    #[error("closed position outcome must contain one complete, balanced exit aggregate")]
    InvalidClosedPosition,
    #[error(
        "settled position outcome requires ResolutionRedeem, payout, and an optional complete partial exchange-exit aggregate"
    )]
    InvalidSettledPosition,
    #[error("{field} price must be within 0..=1, got {actual}")]
    InvalidPrice { field: &'static str, actual: Price },
    #[error("{field} must be non-negative, got {actual}")]
    NegativeAmount {
        field: &'static str,
        actual: Decimal,
    },
    #[error("execution outcome hash mismatch: expected {expected}, got {actual}")]
    OutcomeHashMismatch {
        expected: ContentHash,
        actual: ContentHash,
    },
    #[error(transparent)]
    CanonicalDigest(#[from] CanonicalDigestError),
}

#[derive(Serialize)]
struct ExecutionOutcomeHashInput<'a> {
    contract: &'static str,
    recommendation_id: RecommendationId,
    order_intent_id: OrderIntentId,
    entry_execution_order_id: ExecutionOrderId,
    entry_reconciliation_id: ReconciliationId,
    position_id: Option<PositionId>,
    execution_account_id: ExecutionAccountId,
    market_id: &'a MarketId,
    token_id: &'a TokenId,
    runtime_mode: QuantRuntimeMode,
    terminal_state: ExecutionAttemptTerminalState,
    no_fill_reason: Option<ExecutionAttemptNoFillReason>,
    entry_order_state: ExecutionOrderState,
    requested_shares: Shares,
    filled_shares: Shares,
    entry_avg_price: Option<Price>,
    entry_fee_usd: Option<Usd>,
    entry_filled_at: Option<DateTime<Utc>>,
    position_terminal_state: Option<PositionLedgerState>,
    exit_reason: Option<ExitReason>,
    exit_filled_shares: Option<Shares>,
    exit_avg_price: Option<Price>,
    exit_fee_usd: Option<Usd>,
    exit_at: Option<DateTime<Utc>>,
    settlement_payout_usd: Option<Usd>,
    realized_pnl_usd: Option<Usd>,
    terminal_at: DateTime<Utc>,
    source_observed_at: DateTime<Utc>,
    available_at: DateTime<Utc>,
    source_checkpoint_hash: ContentHash,
    execution_fact_hash: ContentHash,
    execution_fact_schema_version: SchemaVersion,
}

impl<'a> ExecutionOutcomeHashInput<'a> {
    fn from_new(
        outcome: &'a NewExecutionAttemptOutcome,
        source_observed_at: DateTime<Utc>,
        available_at: DateTime<Utc>,
    ) -> Self {
        Self {
            contract: "execution_attempt_outcome_v1",
            recommendation_id: outcome.recommendation_id,
            order_intent_id: outcome.order_intent_id,
            entry_execution_order_id: outcome.entry_execution_order_id,
            entry_reconciliation_id: outcome.entry_reconciliation_id,
            position_id: outcome.position_id,
            execution_account_id: outcome.execution_account_id,
            market_id: &outcome.market_id,
            token_id: &outcome.token_id,
            runtime_mode: outcome.runtime_mode,
            terminal_state: outcome.terminal_state,
            no_fill_reason: outcome.no_fill_reason,
            entry_order_state: outcome.entry_order_state,
            requested_shares: outcome.requested_shares.normalized(),
            filled_shares: outcome.filled_shares.normalized(),
            entry_avg_price: outcome.entry_avg_price.map(Price::normalized),
            entry_fee_usd: outcome.entry_fee_usd.map(Usd::normalized),
            entry_filled_at: outcome.entry_filled_at,
            position_terminal_state: outcome.position_terminal_state,
            exit_reason: outcome.exit_reason,
            exit_filled_shares: outcome.exit_filled_shares.map(Shares::normalized),
            exit_avg_price: outcome.exit_avg_price.map(Price::normalized),
            exit_fee_usd: outcome.exit_fee_usd.map(Usd::normalized),
            exit_at: outcome.exit_at,
            settlement_payout_usd: outcome.settlement_payout_usd.map(Usd::normalized),
            realized_pnl_usd: outcome.realized_pnl_usd.map(Usd::normalized),
            terminal_at: outcome.terminal_at,
            source_observed_at,
            available_at,
            source_checkpoint_hash: outcome.source_checkpoint_hash,
            execution_fact_hash: outcome.execution_fact_hash,
            execution_fact_schema_version: outcome.execution_fact_schema_version,
        }
    }

    fn from_info(outcome: &'a ExecutionAttemptOutcomeInfo) -> Self {
        Self {
            contract: "execution_attempt_outcome_v1",
            recommendation_id: outcome.recommendation_id,
            order_intent_id: outcome.order_intent_id,
            entry_execution_order_id: outcome.entry_execution_order_id,
            entry_reconciliation_id: outcome.entry_reconciliation_id,
            position_id: outcome.position_id,
            execution_account_id: outcome.execution_account_id,
            market_id: &outcome.market_id,
            token_id: &outcome.token_id,
            runtime_mode: outcome.runtime_mode,
            terminal_state: outcome.terminal_state,
            no_fill_reason: outcome.no_fill_reason,
            entry_order_state: outcome.entry_order_state,
            requested_shares: outcome.requested_shares.normalized(),
            filled_shares: outcome.filled_shares.normalized(),
            entry_avg_price: outcome.entry_avg_price.map(Price::normalized),
            entry_fee_usd: outcome.entry_fee_usd.map(Usd::normalized),
            entry_filled_at: outcome.entry_filled_at,
            position_terminal_state: outcome.position_terminal_state,
            exit_reason: outcome.exit_reason,
            exit_filled_shares: outcome.exit_filled_shares.map(Shares::normalized),
            exit_avg_price: outcome.exit_avg_price.map(Price::normalized),
            exit_fee_usd: outcome.exit_fee_usd.map(Usd::normalized),
            exit_at: outcome.exit_at,
            settlement_payout_usd: outcome.settlement_payout_usd.map(Usd::normalized),
            realized_pnl_usd: outcome.realized_pnl_usd.map(Usd::normalized),
            terminal_at: outcome.terminal_at,
            source_observed_at: outcome.source_observed_at,
            available_at: outcome.available_at,
            source_checkpoint_hash: outcome.source_checkpoint_hash,
            execution_fact_hash: outcome.execution_fact_hash,
            execution_fact_schema_version: outcome.execution_fact_schema_version,
        }
    }
}

fn validate_fields(
    outcome: &ExecutionOutcomeHashInput<'_>,
    created_at: Option<DateTime<Utc>>,
) -> Result<(), ExecutionAttemptOutcomeContractError> {
    if outcome.market_id.as_str().trim().is_empty() || outcome.token_id.as_str().trim().is_empty() {
        return Err(ExecutionAttemptOutcomeContractError::EmptyIdentity);
    }
    if !outcome.runtime_mode.allows_order_submission() {
        return Err(ExecutionAttemptOutcomeContractError::ExecutionNotAuthorized);
    }
    SchemaVersion::try_new(outcome.execution_fact_schema_version.get())?;
    if outcome.terminal_at > outcome.source_observed_at
        || outcome.source_observed_at > outcome.available_at
        || created_at.is_some_and(|created_at| outcome.available_at > created_at)
    {
        return Err(ExecutionAttemptOutcomeContractError::InvalidTimeline);
    }
    if !outcome.requested_shares.is_positive() {
        return Err(
            ExecutionAttemptOutcomeContractError::InvalidRequestedShares {
                actual: outcome.requested_shares,
            },
        );
    }
    if outcome.filled_shares.is_negative() || outcome.filled_shares > outcome.requested_shares {
        return Err(ExecutionAttemptOutcomeContractError::InvalidFilledShares {
            requested: outcome.requested_shares,
            actual: outcome.filled_shares,
        });
    }
    validate_optional_price("entry_avg_price", outcome.entry_avg_price)?;
    validate_optional_price("exit_avg_price", outcome.exit_avg_price)?;
    validate_non_negative("entry_fee_usd", outcome.entry_fee_usd.map(Usd::inner))?;
    validate_non_negative("exit_fee_usd", outcome.exit_fee_usd.map(Usd::inner))?;
    validate_non_negative(
        "settlement_payout_usd",
        outcome.settlement_payout_usd.map(Usd::inner),
    )?;
    match outcome.terminal_state {
        ExecutionAttemptTerminalState::Unfilled => validate_unfilled(outcome),
        ExecutionAttemptTerminalState::PartiallyFilled => {
            if !outcome.filled_shares.is_positive()
                || outcome.filled_shares >= outcome.requested_shares
                || !matches!(
                    outcome.entry_order_state,
                    ExecutionOrderState::Cancelled | ExecutionOrderState::Failed
                )
            {
                return Err(
                    ExecutionAttemptOutcomeContractError::TerminalStateMismatch {
                        terminal_state: outcome.terminal_state,
                    },
                );
            }
            validate_filled(outcome)
        }
        ExecutionAttemptTerminalState::FullyFilled => {
            if outcome.filled_shares != outcome.requested_shares
                || outcome.entry_order_state != ExecutionOrderState::Filled
            {
                return Err(
                    ExecutionAttemptOutcomeContractError::TerminalStateMismatch {
                        terminal_state: outcome.terminal_state,
                    },
                );
            }
            validate_filled(outcome)
        }
    }
}

const fn validate_unfilled(
    outcome: &ExecutionOutcomeHashInput<'_>,
) -> Result<(), ExecutionAttemptOutcomeContractError> {
    if !outcome.filled_shares.is_zero()
        || outcome.no_fill_reason.is_none()
        || !matches!(
            outcome.entry_order_state,
            ExecutionOrderState::Cancelled | ExecutionOrderState::Failed
        )
    {
        return Err(
            ExecutionAttemptOutcomeContractError::TerminalStateMismatch {
                terminal_state: outcome.terminal_state,
            },
        );
    }
    if outcome.position_id.is_some()
        || outcome.entry_avg_price.is_some()
        || outcome.entry_filled_at.is_some()
        || outcome.position_terminal_state.is_some()
        || outcome.exit_reason.is_some()
        || outcome.exit_filled_shares.is_some()
        || outcome.exit_avg_price.is_some()
        || outcome.exit_fee_usd.is_some()
        || outcome.exit_at.is_some()
        || outcome.settlement_payout_usd.is_some()
        || outcome.realized_pnl_usd.is_some()
    {
        return Err(ExecutionAttemptOutcomeContractError::UnfilledEconomicsPresent);
    }
    Ok(())
}

fn validate_filled(
    outcome: &ExecutionOutcomeHashInput<'_>,
) -> Result<(), ExecutionAttemptOutcomeContractError> {
    if outcome.no_fill_reason.is_some()
        || outcome.position_id.is_none()
        || outcome.entry_avg_price.is_none()
        || outcome.entry_filled_at.is_none()
        || outcome.position_terminal_state.is_none()
        || outcome.realized_pnl_usd.is_none()
        || outcome
            .entry_filled_at
            .is_some_and(|filled_at| filled_at > outcome.terminal_at)
    {
        return Err(ExecutionAttemptOutcomeContractError::FilledEconomicsMissing);
    }
    match outcome.position_terminal_state {
        Some(PositionLedgerState::Closed) => validate_closed(outcome),
        Some(PositionLedgerState::Settled) => validate_settled(outcome),
        Some(PositionLedgerState::Open | PositionLedgerState::Closing) | None => {
            Err(ExecutionAttemptOutcomeContractError::FilledEconomicsMissing)
        }
    }
}

fn validate_closed(
    outcome: &ExecutionOutcomeHashInput<'_>,
) -> Result<(), ExecutionAttemptOutcomeContractError> {
    if outcome.exit_reason.is_none()
        || outcome.exit_reason == Some(ExitReason::ResolutionRedeem)
        || outcome.exit_filled_shares != Some(outcome.filled_shares)
        || outcome.exit_avg_price.is_none()
        || outcome.exit_at != Some(outcome.terminal_at)
        || outcome.settlement_payout_usd.is_some()
    {
        return Err(ExecutionAttemptOutcomeContractError::InvalidClosedPosition);
    }
    Ok(())
}

fn validate_settled(
    outcome: &ExecutionOutcomeHashInput<'_>,
) -> Result<(), ExecutionAttemptOutcomeContractError> {
    if outcome.exit_reason != Some(ExitReason::ResolutionRedeem)
        || outcome.settlement_payout_usd.is_none()
    {
        return Err(ExecutionAttemptOutcomeContractError::InvalidSettledPosition);
    }
    match outcome.exit_filled_shares {
        None if outcome.exit_avg_price.is_none()
            && outcome.exit_fee_usd.is_none()
            && outcome.exit_at.is_none() =>
        {
            Ok(())
        }
        Some(exit_shares)
            if exit_shares.is_positive()
                && exit_shares < outcome.filled_shares
                && outcome.exit_avg_price.is_some()
                && outcome
                    .exit_at
                    .is_some_and(|exit_at| exit_at <= outcome.terminal_at) =>
        {
            Ok(())
        }
        None | Some(_) => Err(ExecutionAttemptOutcomeContractError::InvalidSettledPosition),
    }
}

fn validate_optional_price(
    field: &'static str,
    value: Option<Price>,
) -> Result<(), ExecutionAttemptOutcomeContractError> {
    if let Some(actual) = value
        && (actual.is_negative() || actual > Price::ONE)
    {
        return Err(ExecutionAttemptOutcomeContractError::InvalidPrice { field, actual });
    }
    Ok(())
}

const fn validate_non_negative(
    field: &'static str,
    value: Option<Decimal>,
) -> Result<(), ExecutionAttemptOutcomeContractError> {
    if let Some(actual) = value
        && actual.is_sign_negative()
    {
        return Err(ExecutionAttemptOutcomeContractError::NegativeAmount { field, actual });
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct ExecutionFill {
    requested: Shares,
    filled: Shares,
}

impl TryFrom<ExecutionFill> for Decimal {
    type Error = ExecutionAttemptOutcomeContractError;

    fn try_from(fill: ExecutionFill) -> Result<Self, Self::Error> {
        if !fill.requested.is_positive() {
            return Err(
                ExecutionAttemptOutcomeContractError::InvalidRequestedShares {
                    actual: fill.requested,
                },
            );
        }
        if fill.filled.is_negative() || fill.filled > fill.requested {
            return Err(ExecutionAttemptOutcomeContractError::InvalidFilledShares {
                requested: fill.requested,
                actual: fill.filled,
            });
        }
        Ok(fill.filled.inner() / fill.requested.inner())
    }
}

impl TryFrom<&ExecutionOutcomeHashInput<'_>> for ContentHash {
    type Error = CanonicalDigestError;

    fn try_from(input: &ExecutionOutcomeHashInput<'_>) -> Result<Self, Self::Error> {
        CanonicalDigest::content_hash_json(input)
    }
}
