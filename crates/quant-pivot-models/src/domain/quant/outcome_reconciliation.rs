//! Deterministic recommendation-execution source-graph reconciliation.

use chrono::{DateTime, Utc};
use quant_pivot_error::hashing::CanonicalDigestError;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::{
    ExecutionAttemptOutcomeInfo, ExecutionOrderInfo, NewExecutionAttemptOutcome, OrderIntentInfo,
    RecommendationResolutionOutcomeInfo, ReconciliationInfo, StrategyPositionLot,
    settlement::SettlementRedeemLotInfo,
};
use crate::{
    enums::{
        common::Side,
        execution::{
            ExecutionOrderPhase, PositionLedgerState, ReconciliationResult, VenueOrderStatus,
        },
        quant::{ExecutionAttemptNoFillReason, ExecutionAttemptTerminalState, ExecutionOrderState},
    },
    hashing::CanonicalDigest,
    types::{
        AccountChainExecutionId, ContentHash, ExecutionOrderId, MarketId, OrderIntentId, Price,
        RecommendationId, ReconciliationId, SchemaVersion, SettlementRedeemLotId, Shares,
        StrategyPositionLotId, TokenId, Usd,
    },
};

/// A complete `PostgreSQL` source graph for one submitted order intent.
///
/// The repository loads and row-locks this graph in one transaction. The
/// derivation is intentionally pure so input ordering, retry timing, and worker
/// interleaving cannot alter the sealed outcome.
#[derive(Debug, Clone)]
pub struct ExecutionAttemptSourceGraph {
    pub recommendation_id: RecommendationId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub intent: OrderIntentInfo,
    pub orders: Vec<ExecutionOrderInfo>,
    pub reconciliations: Vec<ReconciliationInfo>,
    pub account_execution_fees: Vec<AccountExecutionFeeFact>,
    pub position: Option<StrategyPositionLot>,
    pub settlement_lot: Option<SettlementRedeemLotInfo>,
}

/// Exact finalized fee bound through an append-only system-order association.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AccountExecutionFeeFact {
    pub account_chain_execution_id: AccountChainExecutionId,
    pub execution_order_id: ExecutionOrderId,
    pub exact_fee_usd: Usd,
    pub source_event_hash: ContentHash,
    pub available_at: DateTime<Utc>,
}

/// A source may be valid but not yet complete enough to seal a WORM outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionAttemptDeferredReason {
    SourceAvailableAfterCutoff,
    EntryOrderMissing,
    EntryOrderNotSubmitted,
    EntryOrderNotTerminal,
    EntryReconciliationMissing,
    EntryReconciliationPending,
    EntryReconciliationUnresolvable,
    FilledPositionMissing,
    PositionNotTerminal,
    ExitOrderNotTerminal,
    ExitReconciliationMissing,
    ExitReconciliationPending,
    ExitReconciliationUnresolvable,
    AccountChainExecutionMissing,
    SettlementLotMissing,
}

/// Pure derivation result before database availability is assigned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionAttemptDerivation {
    Ready(Box<NewExecutionAttemptOutcome>),
    Deferred(ExecutionAttemptDeferredReason),
}

/// Result of one repository-owned reconciliation attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionAttemptReconciliationResult {
    Inserted(ExecutionAttemptOutcomeInfo),
    AlreadyPresent(ExecutionAttemptOutcomeInfo),
    Deferred(ExecutionAttemptDeferredReason),
}

/// A canonical resolution fact may legitimately arrive after a frozen pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolutionOutcomeDeferredReason {
    CanonicalFactUnavailableAtCutoff,
}

/// Result of one resolution reconciliation attempt at a frozen cutoff.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolutionOutcomeReconciliationResult {
    Inserted(RecommendationResolutionOutcomeInfo),
    AlreadyPresent(RecommendationResolutionOutcomeInfo),
    Deferred(ResolutionOutcomeDeferredReason),
}

/// One terminal catalog-backed recommendation missing its immutable resolution outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecommendationResolutionReconciliationCandidate {
    pub recommendation_id: RecommendationId,
    pub market_id: MarketId,
}

/// One leased recommendation-resolution task from the durable `PostgreSQL` queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolutionOutcomeTaskClaim {
    pub candidate: RecommendationResolutionReconciliationCandidate,
    pub attempt_count: i32,
}

/// One actually submitted terminal intent missing its immutable execution outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionAttemptReconciliationCandidate {
    pub order_intent_id: OrderIntentId,
    pub recommendation_id: RecommendationId,
}

/// One leased execution-attempt task from the durable `PostgreSQL` queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionAttemptTaskClaim {
    pub candidate: ExecutionAttemptReconciliationCandidate,
    pub attempt_count: i32,
}

/// Point-in-time attempt-outcome coverage for feedback truth freeze.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionAttemptBarrier {
    pub cutoff: DateTime<Utc>,
    pub eligible_unsealed_count: u64,
    pub sealed_through: DateTime<Utc>,
}

impl ExecutionAttemptBarrier {
    #[must_use]
    pub fn is_complete(self) -> bool {
        self.eligible_unsealed_count == 0 && self.sealed_through >= self.cutoff
    }
}

/// One leased recommendation-rollup task from the durable `PostgreSQL` queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionRollupTaskClaim {
    pub recommendation_id: RecommendationId,
    pub attempt_count: i32,
}

/// Lease-owner settlement for durable reconciliation work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutcomeTaskSettlement {
    Completed,
    RetryAfter { delay_secs: u64, error: String },
}

/// A malformed or contradictory source graph must fail closed.
#[derive(Debug, Error)]
pub enum ExecutionAttemptReconciliationError {
    #[error("execution source identity mismatch: {detail}")]
    IdentityMismatch { detail: &'static str },
    #[error("execution source graph contains multiple entry orders")]
    MultipleEntryOrders,
    #[error("execution source graph contains duplicate execution order {execution_order_id}")]
    DuplicateOrder {
        execution_order_id: ExecutionOrderId,
    },
    #[error(
        "execution source graph contains duplicate reconciliation for order {execution_order_id}"
    )]
    DuplicateReconciliation {
        execution_order_id: ExecutionOrderId,
    },
    #[error(
        "execution source graph contains reconciliation {reconciliation_id} for an unknown order"
    )]
    OrphanReconciliation { reconciliation_id: ReconciliationId },
    #[error("terminal execution source is contradictory: {detail}")]
    ContradictoryTerminalSource { detail: &'static str },
    #[error(
        "terminal exit aggregate is unbalanced: entry={entry}, exchange_exit={exchange_exit}, settlement={settlement}"
    )]
    UnbalancedTerminalShares {
        entry: Shares,
        exchange_exit: Shares,
        settlement: Shares,
    },
    #[error(transparent)]
    CanonicalDigest(#[from] CanonicalDigestError),
}

impl ExecutionAttemptSourceGraph {
    /// Derive a canonical outcome or a stable late-source reason.
    pub fn derive(
        mut self,
    ) -> Result<ExecutionAttemptDerivation, ExecutionAttemptReconciliationError> {
        self.normalize_and_validate_graph()?;
        let Some(entry) = self.entry_order()? else {
            return Ok(ExecutionAttemptDerivation::Deferred(
                ExecutionAttemptDeferredReason::EntryOrderMissing,
            ));
        };
        if entry.submitted_at.is_none() {
            return Ok(ExecutionAttemptDerivation::Deferred(
                ExecutionAttemptDeferredReason::EntryOrderNotSubmitted,
            ));
        }
        if !entry.state.is_terminal() {
            return Ok(ExecutionAttemptDerivation::Deferred(
                ExecutionAttemptDeferredReason::EntryOrderNotTerminal,
            ));
        }
        let Some(entry_reconciliation) = self.reconciliation_for(entry.execution_order_id) else {
            return Ok(ExecutionAttemptDerivation::Deferred(
                ExecutionAttemptDeferredReason::EntryReconciliationMissing,
            ));
        };
        if let Some(reason) = deferred_reconciliation_reason(entry_reconciliation.result, true) {
            return Ok(ExecutionAttemptDerivation::Deferred(reason));
        }
        if entry_reconciliation.resolved_at.is_none() {
            return Err(
                ExecutionAttemptReconciliationError::ContradictoryTerminalSource {
                    detail: "terminal entry reconciliation has no resolved_at",
                },
            );
        }

        let filled_shares = entry_reconciliation.venue_filled_shares.ok_or(
            ExecutionAttemptReconciliationError::ContradictoryTerminalSource {
                detail: "terminal entry reconciliation has no filled-share fact",
            },
        )?;
        let terminal_state = terminal_state(entry, entry_reconciliation.result, filled_shares)?;
        if terminal_state == ExecutionAttemptTerminalState::Unfilled {
            return self.derive_unfilled(entry, entry_reconciliation, filled_shares);
        }
        self.derive_filled(entry, entry_reconciliation, terminal_state, filled_shares)
    }

    /// Maximum database source visibility bound used by the repository seal.
    #[must_use]
    pub fn source_observed_at(&self) -> DateTime<Utc> {
        let mut observed_at = self.intent.updated_at;
        for order in &self.orders {
            observed_at = observed_at.max(order.updated_at);
        }
        for reconciliation in &self.reconciliations {
            observed_at = observed_at.max(reconciliation.updated_at);
        }
        for fee in &self.account_execution_fees {
            observed_at = observed_at.max(fee.available_at);
        }
        if let Some(position) = &self.position {
            observed_at = observed_at.max(position.updated_at);
        }
        if let Some(settlement_lot) = &self.settlement_lot {
            observed_at = observed_at.max(settlement_lot.created_at);
        }
        observed_at
    }

    fn normalize_and_validate_graph(&mut self) -> Result<(), ExecutionAttemptReconciliationError> {
        if self.intent.recommendation_id != self.recommendation_id {
            return Err(ExecutionAttemptReconciliationError::IdentityMismatch {
                detail: "intent recommendation differs from requested recommendation",
            });
        }
        if self.intent.entry_order_json.token_id != self.token_id
            || self.intent.entry_order_json.side != Side::Buy
        {
            return Err(ExecutionAttemptReconciliationError::IdentityMismatch {
                detail: "intent entry order differs from the recommendation token",
            });
        }
        for order in &self.orders {
            validate_order_identity(self, order)?;
        }
        self.orders
            .sort_by_key(|order| (order.created_at, order.execution_order_id.as_uuid()));
        self.reconciliations.sort_by_key(|reconciliation| {
            (
                reconciliation.created_at,
                reconciliation.execution_order_id.as_uuid(),
                reconciliation.reconciliation_id.as_uuid(),
            )
        });
        self.account_execution_fees.sort_by_key(|fee| {
            (
                fee.execution_order_id.as_uuid(),
                fee.account_chain_execution_id.as_uuid(),
            )
        });
        for orders in self.orders.windows(2) {
            if orders[0].execution_order_id == orders[1].execution_order_id {
                return Err(ExecutionAttemptReconciliationError::DuplicateOrder {
                    execution_order_id: orders[0].execution_order_id,
                });
            }
        }
        for reconciliations in self.reconciliations.windows(2) {
            if reconciliations[0].execution_order_id == reconciliations[1].execution_order_id {
                return Err(
                    ExecutionAttemptReconciliationError::DuplicateReconciliation {
                        execution_order_id: reconciliations[0].execution_order_id,
                    },
                );
            }
        }
        for reconciliation in &self.reconciliations {
            if !self.orders.iter().any(|order| {
                order.execution_order_id == reconciliation.execution_order_id
                    && order.order_intent_id == reconciliation.order_intent_id
            }) {
                return Err(ExecutionAttemptReconciliationError::OrphanReconciliation {
                    reconciliation_id: reconciliation.reconciliation_id,
                });
            }
        }
        Ok(())
    }

    fn entry_order(
        &self,
    ) -> Result<Option<&ExecutionOrderInfo>, ExecutionAttemptReconciliationError> {
        let mut entries = self
            .orders
            .iter()
            .filter(|order| order.order_phase == ExecutionOrderPhase::Entry);
        let entry = entries.next();
        if entries.next().is_some() {
            return Err(ExecutionAttemptReconciliationError::MultipleEntryOrders);
        }
        Ok(entry)
    }

    fn reconciliation_for(
        &self,
        execution_order_id: ExecutionOrderId,
    ) -> Option<&ReconciliationInfo> {
        self.reconciliations
            .iter()
            .find(|reconciliation| reconciliation.execution_order_id == execution_order_id)
    }

    fn exact_fee(&self, execution_order_id: ExecutionOrderId) -> Option<Usd> {
        let fees = self
            .account_execution_fees
            .iter()
            .filter(|fee| fee.execution_order_id == execution_order_id)
            .map(|fee| fee.exact_fee_usd)
            .collect::<Vec<_>>();
        (!fees.is_empty()).then(|| fees.into_iter().sum())
    }

    fn derive_unfilled(
        &self,
        entry: &ExecutionOrderInfo,
        reconciliation: &ReconciliationInfo,
        filled_shares: Shares,
    ) -> Result<ExecutionAttemptDerivation, ExecutionAttemptReconciliationError> {
        if !filled_shares.is_zero()
            || self.position.is_some()
            || self
                .orders
                .iter()
                .any(|order| order.order_phase == ExecutionOrderPhase::Exit)
        {
            return Err(
                ExecutionAttemptReconciliationError::ContradictoryTerminalSource {
                    detail: "unfilled entry contains position or exit economics",
                },
            );
        }
        let terminal_at = reconciliation.resolved_at.ok_or(
            ExecutionAttemptReconciliationError::ContradictoryTerminalSource {
                detail: "unfilled reconciliation has no terminal time",
            },
        )?;
        let no_fill_reason = no_fill_reason(entry, reconciliation.result)?;
        let (source_checkpoint_hash, execution_fact_hash) = self.source_hashes()?;
        Ok(ExecutionAttemptDerivation::Ready(Box::new(
            NewExecutionAttemptOutcome {
                recommendation_id: self.recommendation_id,
                order_intent_id: self.intent.order_intent_id,
                entry_execution_order_id: entry.execution_order_id,
                entry_reconciliation_id: reconciliation.reconciliation_id,
                strategy_position_lot_id: None,
                execution_account_id: self.intent.execution_account_id,
                market_id: self.market_id.clone(),
                token_id: self.token_id.clone(),
                terminal_state: ExecutionAttemptTerminalState::Unfilled,
                no_fill_reason: Some(no_fill_reason),
                entry_order_state: entry.state,
                requested_shares: entry.shares,
                filled_shares,
                entry_avg_price: None,
                entry_fee_usd: None,
                entry_filled_at: None,
                position_terminal_state: None,
                exit_reason: None,
                exit_filled_shares: None,
                exit_avg_price: None,
                exit_fee_usd: None,
                exit_at: None,
                settlement_payout_usd: None,
                realized_pnl_usd: None,
                terminal_at,
                source_checkpoint_hash,
                execution_fact_hash,
                execution_fact_schema_version: SchemaVersion::FIRST,
            },
        )))
    }

    fn derive_filled(
        &self,
        entry: &ExecutionOrderInfo,
        entry_reconciliation: &ReconciliationInfo,
        terminal_state: ExecutionAttemptTerminalState,
        filled_shares: Shares,
    ) -> Result<ExecutionAttemptDerivation, ExecutionAttemptReconciliationError> {
        let Some(position) = &self.position else {
            return Ok(ExecutionAttemptDerivation::Deferred(
                ExecutionAttemptDeferredReason::FilledPositionMissing,
            ));
        };
        if !matches!(
            position.state,
            PositionLedgerState::Closed | PositionLedgerState::Settled
        ) {
            return Ok(ExecutionAttemptDerivation::Deferred(
                ExecutionAttemptDeferredReason::PositionNotTerminal,
            ));
        }
        validate_lot_identity(self, position)?;
        let terminal_at = position.closed_at.ok_or(
            ExecutionAttemptReconciliationError::ContradictoryTerminalSource {
                detail: "terminal position has no closed_at",
            },
        )?;
        let entry_avg_price = entry_reconciliation.venue_avg_price.ok_or(
            ExecutionAttemptReconciliationError::ContradictoryTerminalSource {
                detail: "filled entry reconciliation has no average price",
            },
        )?;
        let Some(entry_fee_usd) = self.exact_fee(entry.execution_order_id) else {
            return Ok(ExecutionAttemptDerivation::Deferred(
                ExecutionAttemptDeferredReason::AccountChainExecutionMissing,
            ));
        };
        let exit_aggregate = match self.derive_exit_aggregate()? {
            ExitAggregateDerivation::Ready(aggregate) => aggregate,
            ExitAggregateDerivation::Deferred(reason) => {
                return Ok(ExecutionAttemptDerivation::Deferred(reason));
            }
        };
        if position.state == PositionLedgerState::Settled && self.settlement_lot.is_none() {
            return Ok(ExecutionAttemptDerivation::Deferred(
                ExecutionAttemptDeferredReason::SettlementLotMissing,
            ));
        }
        let settlement_shares = self.settlement_shares(position)?;
        let total_terminal_shares = exit_aggregate.shares + settlement_shares;
        if total_terminal_shares != filled_shares {
            return Err(
                ExecutionAttemptReconciliationError::UnbalancedTerminalShares {
                    entry: filled_shares,
                    exchange_exit: exit_aggregate.shares,
                    settlement: settlement_shares,
                },
            );
        }
        validate_terminal_route(position.state, &exit_aggregate, settlement_shares)?;

        let (source_checkpoint_hash, execution_fact_hash) = self.source_hashes()?;
        let has_exchange_exit = exit_aggregate.shares.is_positive();
        let exit_fee_usd = if has_exchange_exit {
            exit_aggregate.fee
        } else {
            Some(Usd::ZERO)
        };
        let Some(exit_fee_for_pnl) = exit_fee_usd else {
            return Ok(ExecutionAttemptDerivation::Deferred(
                ExecutionAttemptDeferredReason::AccountChainExecutionMissing,
            ));
        };
        let exit_proceeds_usd = exit_aggregate.shares * exit_aggregate.avg_price - exit_fee_for_pnl;
        let settlement_payout_usd = self.settlement_lot.as_ref().map(|lot| lot.payout_usd);
        let terminal_proceeds_usd = exit_proceeds_usd + settlement_payout_usd.unwrap_or(Usd::ZERO);
        let realized_pnl_usd =
            terminal_proceeds_usd - (filled_shares * entry_avg_price + entry_fee_usd);
        Ok(ExecutionAttemptDerivation::Ready(Box::new(
            NewExecutionAttemptOutcome {
                recommendation_id: self.recommendation_id,
                order_intent_id: self.intent.order_intent_id,
                entry_execution_order_id: entry.execution_order_id,
                entry_reconciliation_id: entry_reconciliation.reconciliation_id,
                strategy_position_lot_id: Some(position.strategy_position_lot_id),
                execution_account_id: self.intent.execution_account_id,
                market_id: self.market_id.clone(),
                token_id: self.token_id.clone(),
                terminal_state,
                no_fill_reason: None,
                entry_order_state: entry.state,
                requested_shares: entry.shares,
                filled_shares,
                entry_avg_price: Some(entry_avg_price),
                entry_fee_usd: Some(entry_fee_usd),
                entry_filled_at: entry.filled_at,
                position_terminal_state: Some(position.state),
                exit_reason: self.intent.exit_reason,
                exit_filled_shares: has_exchange_exit.then_some(exit_aggregate.shares),
                exit_avg_price: has_exchange_exit.then_some(exit_aggregate.avg_price),
                exit_fee_usd: has_exchange_exit.then_some(exit_fee_for_pnl),
                exit_at: has_exchange_exit.then_some(exit_aggregate.last_fill_at),
                settlement_payout_usd,
                realized_pnl_usd: Some(realized_pnl_usd),
                terminal_at,
                source_checkpoint_hash,
                execution_fact_hash,
                execution_fact_schema_version: SchemaVersion::FIRST,
            },
        )))
    }

    fn derive_exit_aggregate(
        &self,
    ) -> Result<ExitAggregateDerivation, ExecutionAttemptReconciliationError> {
        let mut shares = Shares::ZERO;
        let mut weighted_price = Decimal::ZERO;
        let mut fee = Some(Usd::ZERO);
        let mut last_fill_at = None;
        for order in self
            .orders
            .iter()
            .filter(|order| order.order_phase == ExecutionOrderPhase::Exit)
        {
            if !order.state.is_terminal() {
                return Ok(ExitAggregateDerivation::Deferred(
                    ExecutionAttemptDeferredReason::ExitOrderNotTerminal,
                ));
            }
            let Some(reconciliation) = self.reconciliation_for(order.execution_order_id) else {
                return Ok(ExitAggregateDerivation::Deferred(
                    ExecutionAttemptDeferredReason::ExitReconciliationMissing,
                ));
            };
            if let Some(reason) = deferred_reconciliation_reason(reconciliation.result, false) {
                return Ok(ExitAggregateDerivation::Deferred(reason));
            }
            let resolved_at = reconciliation.resolved_at.ok_or(
                ExecutionAttemptReconciliationError::ContradictoryTerminalSource {
                    detail: "terminal exit reconciliation has no resolved_at",
                },
            )?;
            match reconciliation.result {
                ReconciliationResult::Filled | ReconciliationResult::PartiallyFilled => {
                    let filled = reconciliation.venue_filled_shares.ok_or(
                        ExecutionAttemptReconciliationError::ContradictoryTerminalSource {
                            detail: "filled exit reconciliation has no share fact",
                        },
                    )?;
                    let avg_price = reconciliation.venue_avg_price.ok_or(
                        ExecutionAttemptReconciliationError::ContradictoryTerminalSource {
                            detail: "filled exit reconciliation has no average price",
                        },
                    )?;
                    if !filled.is_positive() {
                        return Err(
                            ExecutionAttemptReconciliationError::ContradictoryTerminalSource {
                                detail: "filled exit reconciliation has non-positive shares",
                            },
                        );
                    }
                    shares += filled;
                    weighted_price += filled.inner() * avg_price.inner();
                    let Some(actual_fee) = self.exact_fee(order.execution_order_id) else {
                        return Ok(ExitAggregateDerivation::Deferred(
                            ExecutionAttemptDeferredReason::AccountChainExecutionMissing,
                        ));
                    };
                    fee = fee.map(|total| total + actual_fee);
                    last_fill_at =
                        Some(last_fill_at.map_or(resolved_at, |current: DateTime<Utc>| {
                            current.max(resolved_at)
                        }));
                }
                ReconciliationResult::NotFilled | ReconciliationResult::Cancelled => {}
                ReconciliationResult::Pending | ReconciliationResult::Unresolvable => {
                    unreachable!("deferred results returned before aggregation")
                }
            }
        }
        let avg_price = if shares.is_positive() {
            Price::new(weighted_price / shares.inner())
        } else {
            Price::ZERO
        };
        Ok(ExitAggregateDerivation::Ready(ExitAggregate {
            shares,
            avg_price,
            fee,
            last_fill_at: last_fill_at.unwrap_or(self.intent.updated_at),
        }))
    }

    fn settlement_shares(
        &self,
        position: &StrategyPositionLot,
    ) -> Result<Shares, ExecutionAttemptReconciliationError> {
        match position.state {
            PositionLedgerState::Settled => {
                let lot = self.settlement_lot.as_ref().ok_or(
                    ExecutionAttemptReconciliationError::ContradictoryTerminalSource {
                        detail: "settled position has no settlement lot",
                    },
                )?;
                if lot.strategy_position_lot_id != position.strategy_position_lot_id
                    || lot.order_intent_id != self.intent.order_intent_id
                    || lot.token_id != self.token_id
                {
                    return Err(ExecutionAttemptReconciliationError::IdentityMismatch {
                        detail: "settlement lot differs from the terminal position",
                    });
                }
                Ok(lot.shares_redeemed)
            }
            PositionLedgerState::Closed => {
                if self.settlement_lot.is_some() {
                    return Err(
                        ExecutionAttemptReconciliationError::ContradictoryTerminalSource {
                            detail: "closed position unexpectedly has a settlement lot",
                        },
                    );
                }
                Ok(Shares::ZERO)
            }
            PositionLedgerState::Open | PositionLedgerState::Closing => Ok(Shares::ZERO),
        }
    }

    fn source_hashes(
        &self,
    ) -> Result<(ContentHash, ContentHash), ExecutionAttemptReconciliationError> {
        let checkpoints = SourceCheckpointView::from_graph(self);
        let source_checkpoint_hash = CanonicalDigest::content_hash_json(&checkpoints)?;
        let execution_fact_hash =
            CanonicalDigest::content_hash_json(&ExecutionFactGraphView::from_graph(self))?;
        Ok((source_checkpoint_hash, execution_fact_hash))
    }
}

fn validate_order_identity(
    graph: &ExecutionAttemptSourceGraph,
    order: &ExecutionOrderInfo,
) -> Result<(), ExecutionAttemptReconciliationError> {
    if order.order_intent_id != graph.intent.order_intent_id
        || order.market_id != graph.market_id
        || order.token_id != graph.token_id
    {
        return Err(ExecutionAttemptReconciliationError::IdentityMismatch {
            detail: "execution order differs from the submitted intent",
        });
    }
    let expected_side = match order.order_phase {
        ExecutionOrderPhase::Entry => Side::Buy,
        ExecutionOrderPhase::Exit => Side::Sell,
    };
    if order.side != expected_side
        || order.prepared_order_json.side != expected_side
        || order.prepared_order_json.token_id != order.token_id
        || order.prepared_order_json.expected_filled_shares != order.shares
        || order.prepared_order_json.worst_price != order.price
    {
        return Err(ExecutionAttemptReconciliationError::IdentityMismatch {
            detail: "execution order differs from its prepared venue order",
        });
    }
    Ok(())
}

fn terminal_state(
    entry: &ExecutionOrderInfo,
    result: ReconciliationResult,
    filled_shares: Shares,
) -> Result<ExecutionAttemptTerminalState, ExecutionAttemptReconciliationError> {
    match (entry.state, result) {
        (
            ExecutionOrderState::Failed | ExecutionOrderState::Cancelled,
            ReconciliationResult::NotFilled | ReconciliationResult::Cancelled,
        ) if filled_shares.is_zero() => Ok(ExecutionAttemptTerminalState::Unfilled),
        (
            ExecutionOrderState::Cancelled | ExecutionOrderState::Failed,
            ReconciliationResult::PartiallyFilled,
        ) if filled_shares.is_positive() && filled_shares < entry.shares => {
            Ok(ExecutionAttemptTerminalState::PartiallyFilled)
        }
        (ExecutionOrderState::Filled, ReconciliationResult::Filled)
            if filled_shares == entry.shares =>
        {
            Ok(ExecutionAttemptTerminalState::FullyFilled)
        }
        _ => Err(
            ExecutionAttemptReconciliationError::ContradictoryTerminalSource {
                detail: "entry order, reconciliation result, and filled shares disagree",
            },
        ),
    }
}

fn no_fill_reason(
    entry: &ExecutionOrderInfo,
    result: ReconciliationResult,
) -> Result<ExecutionAttemptNoFillReason, ExecutionAttemptReconciliationError> {
    match entry.venue_status {
        Some(VenueOrderStatus::Rejected) => Ok(ExecutionAttemptNoFillReason::VenueRejected),
        Some(VenueOrderStatus::Cancelled) => Ok(ExecutionAttemptNoFillReason::VenueCancelled),
        Some(VenueOrderStatus::Expired) => Ok(ExecutionAttemptNoFillReason::VenueExpired),
        None if result == ReconciliationResult::NotFilled => {
            Ok(ExecutionAttemptNoFillReason::ReconciledNotFilled)
        }
        _ => Err(
            ExecutionAttemptReconciliationError::ContradictoryTerminalSource {
                detail: "unfilled entry has no supported terminal venue reason",
            },
        ),
    }
}

const fn deferred_reconciliation_reason(
    result: ReconciliationResult,
    entry: bool,
) -> Option<ExecutionAttemptDeferredReason> {
    match (entry, result) {
        (true, ReconciliationResult::Pending) => {
            Some(ExecutionAttemptDeferredReason::EntryReconciliationPending)
        }
        (true, ReconciliationResult::Unresolvable) => {
            Some(ExecutionAttemptDeferredReason::EntryReconciliationUnresolvable)
        }
        (false, ReconciliationResult::Pending) => {
            Some(ExecutionAttemptDeferredReason::ExitReconciliationPending)
        }
        (false, ReconciliationResult::Unresolvable) => {
            Some(ExecutionAttemptDeferredReason::ExitReconciliationUnresolvable)
        }
        (
            _,
            ReconciliationResult::Filled
            | ReconciliationResult::NotFilled
            | ReconciliationResult::PartiallyFilled
            | ReconciliationResult::Cancelled,
        ) => None,
    }
}

fn validate_lot_identity(
    graph: &ExecutionAttemptSourceGraph,
    position: &StrategyPositionLot,
) -> Result<(), ExecutionAttemptReconciliationError> {
    if position.order_intent_id != Some(graph.intent.order_intent_id)
        || position.execution_account_id != graph.intent.execution_account_id
        || position.market_id != graph.market_id
        || position.token_id != graph.token_id
    {
        return Err(ExecutionAttemptReconciliationError::IdentityMismatch {
            detail: "position identity differs from the submitted intent",
        });
    }
    Ok(())
}

fn validate_terminal_route(
    state: PositionLedgerState,
    exchange_exit: &ExitAggregate,
    settlement_shares: Shares,
) -> Result<(), ExecutionAttemptReconciliationError> {
    match state {
        PositionLedgerState::Closed
            if exchange_exit.shares.is_positive() && settlement_shares.is_zero() =>
        {
            Ok(())
        }
        PositionLedgerState::Settled if settlement_shares.is_positive() => Ok(()),
        PositionLedgerState::Open
        | PositionLedgerState::Closing
        | PositionLedgerState::Closed
        | PositionLedgerState::Settled => Err(
            ExecutionAttemptReconciliationError::ContradictoryTerminalSource {
                detail: "position terminal state does not match exchange/settlement sources",
            },
        ),
    }
}

struct ExitAggregate {
    shares: Shares,
    avg_price: Price,
    fee: Option<Usd>,
    last_fill_at: DateTime<Utc>,
}

enum ExitAggregateDerivation {
    Ready(ExitAggregate),
    Deferred(ExecutionAttemptDeferredReason),
}

#[derive(Serialize)]
struct SourceCheckpointView {
    contract: &'static str,
    recommendation_id: RecommendationId,
    order_intent_id: OrderIntentId,
    intent_updated_at: DateTime<Utc>,
    orders: Vec<(ExecutionOrderId, DateTime<Utc>)>,
    reconciliations: Vec<(ReconciliationId, ExecutionOrderId, DateTime<Utc>)>,
    account_execution_fees: Vec<(AccountChainExecutionId, ExecutionOrderId, DateTime<Utc>)>,
    position: Option<(StrategyPositionLotId, DateTime<Utc>)>,
    settlement_lot: Option<(SettlementRedeemLotId, DateTime<Utc>)>,
}

impl SourceCheckpointView {
    fn from_graph(graph: &ExecutionAttemptSourceGraph) -> Self {
        Self {
            contract: "recommendation_execution_source_checkpoint_v1",
            recommendation_id: graph.recommendation_id,
            order_intent_id: graph.intent.order_intent_id,
            intent_updated_at: graph.intent.updated_at,
            orders: graph
                .orders
                .iter()
                .map(|order| (order.execution_order_id, order.updated_at))
                .collect(),
            reconciliations: graph
                .reconciliations
                .iter()
                .map(|reconciliation| {
                    (
                        reconciliation.reconciliation_id,
                        reconciliation.execution_order_id,
                        reconciliation.updated_at,
                    )
                })
                .collect(),
            account_execution_fees: graph
                .account_execution_fees
                .iter()
                .map(|fee| {
                    (
                        fee.account_chain_execution_id,
                        fee.execution_order_id,
                        fee.available_at,
                    )
                })
                .collect(),
            position: graph
                .position
                .as_ref()
                .map(|position| (position.strategy_position_lot_id, position.updated_at)),
            settlement_lot: graph
                .settlement_lot
                .as_ref()
                .map(|lot| (lot.settlement_redeem_lot_id, lot.created_at)),
        }
    }
}

#[derive(Serialize)]
struct ExecutionFactGraphView<'a> {
    contract: &'static str,
    recommendation_id: RecommendationId,
    market_id: &'a MarketId,
    token_id: &'a TokenId,
    intent: &'a OrderIntentInfo,
    orders: &'a [ExecutionOrderInfo],
    reconciliations: &'a [ReconciliationInfo],
    account_execution_fees: &'a [AccountExecutionFeeFact],
    position: Option<&'a StrategyPositionLot>,
    settlement_lot: Option<&'a SettlementRedeemLotInfo>,
}

impl<'a> ExecutionFactGraphView<'a> {
    const fn from_graph(graph: &'a ExecutionAttemptSourceGraph) -> Self {
        Self {
            contract: "recommendation_execution_fact_graph_v1",
            recommendation_id: graph.recommendation_id,
            market_id: &graph.market_id,
            token_id: &graph.token_id,
            intent: &graph.intent,
            orders: graph.orders.as_slice(),
            reconciliations: graph.reconciliations.as_slice(),
            account_execution_fees: graph.account_execution_fees.as_slice(),
            position: graph.position.as_ref(),
            settlement_lot: graph.settlement_lot.as_ref(),
        }
    }
}
