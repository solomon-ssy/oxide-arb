//! Postgres-backed settlement redemption ledger repository.

use std::{collections::HashMap, slice};

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{
    StorageError,
    entity::{
        QUANT_EXECUTION_ACCOUNT, QUANT_ORDER_INTENT, QUANT_SETTLEMENT_AUTHORIZATION,
        QUANT_SETTLEMENT_CHAIN_SUBMISSION, QUANT_SETTLEMENT_GOVERNED_ACTION,
        QUANT_SETTLEMENT_REDEEM,
    },
};
use quant_pivot_models::{
    domain::{
        api::settlement_redeem::{SettlementRedeemListQuery, SettlementRedeemSummary},
        data_plane::DomainEventPayload,
        pagination::{PageWindow, Paginated},
        quant::{
            settlement::{
                ApproveSettlementAuthorization, BeginSettlementDispatch, ConfirmSettlementRedeem,
                NewSettlementAuthorization, NewSettlementRedeem,
                PersistPreparedSettlementSubmission, PersistSettlementPreflight,
                RecordEoaSettlementBroadcast, RecordRelayerSettlementAcceptance,
                RecordRelayerSettlementChainHash, RequireSettlementReconciliation,
                RevokeSettlementAuthorization, ScheduleSettlementRetry, ScheduleSettlementWork,
                SettlementChainSubmissionInfo, SettlementRedeemInfo, SettlementRedeemLotInfo,
                SettlementWorkClaim, StageSettlementAuthorization,
            },
            settlement_inventory::{
                MarkSettlementInventoryAbsent, NewSettlementInventoryLot,
                RefreshSettlementInventory, SettlementDiscoveryCandidate, SettlementDiscoveryLot,
                SettlementInventoryLotInfo,
            },
        },
    },
    entities::{
        market::{Column as MarketColumn, Entity as MarketEntity},
        quant_domain_event_outbox::{
            ActiveModel as QuantDomainEventOutboxActiveModel,
            Column as QuantDomainEventOutboxColumn, Entity as QuantDomainEventOutboxEntity,
        },
        quant_execution_account::{
            Column as QuantExecutionAccountColumn, Entity as QuantExecutionAccountEntity,
            Model as QuantExecutionAccountModel,
        },
        quant_execution_order::{
            Column as QuantExecutionOrderColumn, Entity as QuantExecutionOrderEntity,
            Relation as QuantExecutionOrderRelation,
        },
        quant_order_intent::{Column as QuantOrderIntentColumn, Entity as QuantOrderIntentEntity},
        quant_position::{
            Column as QuantPositionColumn, Entity as QuantPositionEntity,
            Relation as QuantPositionRelation,
        },
        quant_settlement_authorization::{
            Column as QuantSettlementAuthorizationColumn,
            Entity as QuantSettlementAuthorizationEntity,
            Model as QuantSettlementAuthorizationModel,
        },
        quant_settlement_chain_submission::{
            Column as QuantSettlementChainSubmissionColumn,
            Entity as QuantSettlementChainSubmissionEntity,
            Model as QuantSettlementChainSubmissionModel,
        },
        quant_settlement_governed_action::{
            Entity as QuantSettlementGovernedActionEntity,
            Model as QuantSettlementGovernedActionModel,
        },
        quant_settlement_inventory_lot::{
            Column as QuantSettlementInventoryLotColumn,
            Entity as QuantSettlementInventoryLotEntity,
        },
        quant_settlement_redeem::{
            ActiveModel as QuantSettlementRedeemActiveModel, Column, Entity,
            Model as QuantSettlementRedeemModel,
        },
        quant_settlement_redeem_lot::{
            Column as QuantSettlementRedeemLotColumn, Entity as QuantSettlementRedeemLotEntity,
        },
    },
    enums::{
        execution::{ExitReason, ExitState, PositionLedgerState},
        market::MarketStatus,
        quant::{ExecutionOrderState, OutcomeSide},
        settlement::{
            SettlementAuthorizationState, SettlementCaseState, SettlementEffectivePolicy,
            SettlementGovernedActionKind, SettlementGovernedActionState, SettlementReadinessStatus,
            SettlementReconciliationState, SettlementRoute, SettlementSubmissionKind,
            SettlementSubmissionPurpose, SettlementSubmissionState,
        },
    },
    types::{
        ContentHash, EvmAddress, ExecutionAccountId, ExitPolicySpec, MarketId, OrderIntentId,
        PositionId, SettlementAuthorizationId, SettlementChainSubmissionId, SettlementRedeemId,
        Shares, TokenId, Usd, WorkerId,
        settlement_payload::{
            SettlementChainReceiptEvidence, SettlementFailureEvidence, SettlementPayoutVector,
            SettlementReadinessEvidence,
        },
    },
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, ConnectionTrait, DatabaseConnection,
    EntityTrait, ExprTrait, FromQueryResult, IntoActiveModel, IsolationLevel, JoinType,
    PaginatorTrait, QueryFilter, QueryOrder, QuerySelect, RelationTrait, TransactionTrait,
    sea_query::{Expr, LockBehavior, LockType, OnConflict, Query},
};

use crate::{
    postgres::{
        error,
        quant::{capital_allocation::complete_exit_capital, position},
        query::paginate_mapped,
        write::insert_many_chunked,
    },
    traits::quant::settlement_redeem::SettlementRedeemRepository,
};

const ACTIVE_SUBMISSION_STATES: [SettlementSubmissionState; 4] = [
    SettlementSubmissionState::Prepared,
    SettlementSubmissionState::Dispatching,
    SettlementSubmissionState::AwaitingChainHash,
    SettlementSubmissionState::AwaitingFinality,
];
const UNSETTLED_EXECUTION_ORDER_STATES: [ExecutionOrderState; 6] = [
    ExecutionOrderState::Planned,
    ExecutionOrderState::Accepted,
    ExecutionOrderState::Submitted,
    ExecutionOrderState::PartiallyFilled,
    ExecutionOrderState::CancelRequested,
    ExecutionOrderState::Ambiguous,
];

const OPEN_INVENTORY_STATES: [PositionLedgerState; 2] =
    [PositionLedgerState::Open, PositionLedgerState::Closing];

#[derive(Debug, FromQueryResult)]
struct SettlementDiscoveryScopeRow {
    market_id: MarketId,
    execution_account_id: ExecutionAccountId,
}

#[derive(Debug, FromQueryResult)]
struct SettlementDiscoveryRow {
    position_id: PositionId,
    order_intent_id: OrderIntentId,
    execution_account_id: ExecutionAccountId,
    intent_execution_account_id: ExecutionAccountId,
    token_id: TokenId,
    side: OutcomeSide,
    shares: Shares,
    cost_basis_usd: Usd,
    position_version_at: DateTime<Utc>,
    exit_policy_json: ExitPolicySpec,
    intent_version_at: DateTime<Utc>,
    market_id: MarketId,
    yes_token_id: TokenId,
    no_token_id: TokenId,
    resolution_outcome: String,
    resolved_at: DateTime<Utc>,
    resolution_content_hash: ContentHash,
    neg_risk: bool,
}

async fn discovery_scopes(
    db: &impl ConnectionTrait,
    limit: u64,
) -> Result<Vec<SettlementDiscoveryScopeRow>, StorageError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    QuantPositionEntity::find()
        .join(JoinType::InnerJoin, QuantPositionRelation::Market.def())
        .select_only()
        .column(QuantPositionColumn::MarketId)
        .column(QuantPositionColumn::ExecutionAccountId)
        .filter(QuantPositionColumn::State.is_in(OPEN_INVENTORY_STATES))
        .filter(MarketColumn::Status.eq(MarketStatus::Settled))
        .filter(MarketColumn::Outcome.is_not_null())
        .filter(MarketColumn::ResolvedAt.is_not_null())
        .filter(Expr::cust(
            "NOT EXISTS (SELECT 1 FROM quant_settlement_redeem AS redeem WHERE redeem.market_id = quant_position.market_id AND redeem.execution_account_id = quant_position.execution_account_id)",
        ))
        .group_by(QuantPositionColumn::MarketId)
        .group_by(QuantPositionColumn::ExecutionAccountId)
        .order_by_asc(QuantPositionColumn::MarketId)
        .order_by_asc(QuantPositionColumn::ExecutionAccountId)
        .limit(limit)
        .into_model::<SettlementDiscoveryScopeRow>()
        .all(db)
        .await
        .map_err(StorageError::from)
}

fn scope_condition(scopes: &[SettlementDiscoveryScopeRow]) -> Condition {
    scopes.iter().fold(Condition::any(), |condition, scope| {
        condition.add(
            Condition::all()
                .add(QuantPositionColumn::MarketId.eq(scope.market_id.clone()))
                .add(QuantPositionColumn::ExecutionAccountId.eq(scope.execution_account_id)),
        )
    })
}

async fn discovery_rows(
    db: &impl ConnectionTrait,
    scopes: &[SettlementDiscoveryScopeRow],
) -> Result<Vec<SettlementDiscoveryRow>, StorageError> {
    if scopes.is_empty() {
        return Ok(Vec::new());
    }
    QuantPositionEntity::find()
        .join(
            JoinType::InnerJoin,
            QuantPositionRelation::OrderIntent.def(),
        )
        .join(JoinType::InnerJoin, QuantPositionRelation::Market.def())
        .select_only()
        .column_as(
            Expr::col((QuantPositionEntity, QuantPositionColumn::PositionId)),
            "position_id",
        )
        .column_as(
            Expr::col((QuantPositionEntity, QuantPositionColumn::OrderIntentId)),
            "order_intent_id",
        )
        .column_as(
            Expr::col((QuantPositionEntity, QuantPositionColumn::ExecutionAccountId)),
            "execution_account_id",
        )
        .column_as(
            Expr::col((
                QuantOrderIntentEntity,
                QuantOrderIntentColumn::ExecutionAccountId,
            )),
            "intent_execution_account_id",
        )
        .column_as(
            Expr::col((QuantPositionEntity, QuantPositionColumn::TokenId)),
            "token_id",
        )
        .column_as(
            Expr::col((QuantPositionEntity, QuantPositionColumn::Side)),
            "side",
        )
        .column_as(
            Expr::col((QuantPositionEntity, QuantPositionColumn::Shares)),
            "shares",
        )
        .column_as(
            Expr::col((QuantPositionEntity, QuantPositionColumn::CostUsd)),
            "cost_basis_usd",
        )
        .column_as(
            Expr::col((QuantPositionEntity, QuantPositionColumn::UpdatedAt)),
            "position_version_at",
        )
        .column_as(
            Expr::col((
                QuantOrderIntentEntity,
                QuantOrderIntentColumn::ExitPolicyJson,
            )),
            "exit_policy_json",
        )
        .column_as(
            Expr::col((QuantOrderIntentEntity, QuantOrderIntentColumn::UpdatedAt)),
            "intent_version_at",
        )
        .column_as(
            Expr::col((QuantPositionEntity, QuantPositionColumn::MarketId)),
            "market_id",
        )
        .column_as(
            Expr::col((MarketEntity, MarketColumn::YesTokenId)),
            "yes_token_id",
        )
        .column_as(
            Expr::col((MarketEntity, MarketColumn::NoTokenId)),
            "no_token_id",
        )
        .column_as(
            Expr::col((MarketEntity, MarketColumn::Outcome)),
            "resolution_outcome",
        )
        .column_as(
            Expr::col((MarketEntity, MarketColumn::ResolvedAt)),
            "resolved_at",
        )
        .column_as(
            Expr::col((MarketEntity, MarketColumn::ContentHash)),
            "resolution_content_hash",
        )
        .column_as(Expr::col((MarketEntity, MarketColumn::NegRisk)), "neg_risk")
        .filter(scope_condition(scopes))
        .filter(QuantPositionColumn::State.is_in(OPEN_INVENTORY_STATES))
        .filter(MarketColumn::Status.eq(MarketStatus::Settled))
        .filter(MarketColumn::Outcome.is_not_null())
        .filter(MarketColumn::ResolvedAt.is_not_null())
        .order_by_asc(QuantPositionColumn::MarketId)
        .order_by_asc(QuantPositionColumn::ExecutionAccountId)
        .order_by_asc(QuantPositionColumn::PositionId)
        .into_model::<SettlementDiscoveryRow>()
        .all(db)
        .await
        .map_err(StorageError::from)
}

fn assemble_discovery_candidates(
    rows: Vec<SettlementDiscoveryRow>,
) -> Result<Vec<SettlementDiscoveryCandidate>, StorageError> {
    let mut candidates: HashMap<(MarketId, ExecutionAccountId), SettlementDiscoveryCandidate> =
        HashMap::new();
    for row in rows {
        if row.execution_account_id != row.intent_execution_account_id {
            return Err(error::state_conflict(
                QUANT_ORDER_INTENT,
                Some(row.order_intent_id),
                "position and intent execution-account lineage diverged",
            ));
        }
        let key = (row.market_id.clone(), row.execution_account_id);
        let candidate = candidates
            .entry(key)
            .or_insert_with(|| SettlementDiscoveryCandidate {
                market_id: row.market_id.clone(),
                yes_token_id: row.yes_token_id.clone(),
                no_token_id: row.no_token_id.clone(),
                execution_account_id: row.execution_account_id,
                route: if row.neg_risk {
                    SettlementRoute::NegRiskV2
                } else {
                    SettlementRoute::StandardV2
                },
                resolution_outcome: row.resolution_outcome.clone(),
                resolved_at: row.resolved_at,
                resolution_content_hash: row.resolution_content_hash,
                lots: Vec::new(),
            });
        if candidate.resolution_outcome != row.resolution_outcome
            || candidate.yes_token_id != row.yes_token_id
            || candidate.no_token_id != row.no_token_id
            || candidate.resolved_at != row.resolved_at
            || candidate.resolution_content_hash != row.resolution_content_hash
            || candidate.route
                != if row.neg_risk {
                    SettlementRoute::NegRiskV2
                } else {
                    SettlementRoute::StandardV2
                }
        {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_REDEEM,
                None::<SettlementRedeemId>,
                "one market/account inventory query returned inconsistent resolution identity",
            ));
        }
        candidate.lots.push(SettlementDiscoveryLot {
            position_id: row.position_id,
            order_intent_id: row.order_intent_id,
            execution_account_id: row.execution_account_id,
            token_id: row.token_id,
            side: row.side,
            shares: row.shares,
            cost_basis_usd: row.cost_basis_usd,
            settlement_mode: row.exit_policy_json.settlement_mode,
            redeem_policy: row.exit_policy_json.redeem_policy,
            position_version_at: row.position_version_at,
            intent_version_at: row.intent_version_at,
        });
    }
    let mut result = candidates.into_values().collect::<Vec<_>>();
    result.sort_by(|left, right| {
        left.market_id
            .to_string()
            .cmp(&right.market_id.to_string())
            .then_with(|| {
                left.execution_account_id
                    .to_string()
                    .cmp(&right.execution_account_id.to_string())
            })
    });
    Ok(result)
}

async fn inventory_candidate(
    db: &impl ConnectionTrait,
    market_id: &MarketId,
    execution_account_id: ExecutionAccountId,
) -> Result<Option<SettlementDiscoveryCandidate>, StorageError> {
    let scope = SettlementDiscoveryScopeRow {
        market_id: market_id.clone(),
        execution_account_id,
    };
    let mut candidates = assemble_discovery_candidates(discovery_rows(db, &[scope]).await?)?;
    Ok(candidates.pop())
}

async fn unsettled_execution_order_count(
    db: &impl ConnectionTrait,
    market_id: &MarketId,
    execution_account_id: ExecutionAccountId,
) -> Result<u64, StorageError> {
    QuantExecutionOrderEntity::find()
        .join(
            JoinType::InnerJoin,
            QuantExecutionOrderRelation::OrderIntent.def(),
        )
        .filter(QuantExecutionOrderColumn::MarketId.eq(market_id.clone()))
        .filter(QuantOrderIntentColumn::ExecutionAccountId.eq(execution_account_id))
        .filter(QuantExecutionOrderColumn::State.is_in(UNSETTLED_EXECUTION_ORDER_STATES))
        .count(db)
        .await
        .map_err(StorageError::from)
}

pub struct PgSettlementRedeemRepository {
    db: DatabaseConnection,
}

async fn load_redeem_context(
    db: &impl ConnectionTrait,
    models: &[QuantSettlementRedeemModel],
) -> Result<
    (
        HashMap<ExecutionAccountId, QuantExecutionAccountModel>,
        HashMap<SettlementAuthorizationId, QuantSettlementAuthorizationModel>,
    ),
    StorageError,
> {
    if models.is_empty() {
        return Ok((HashMap::new(), HashMap::new()));
    }
    let account_ids = models
        .iter()
        .map(|model| model.execution_account_id)
        .collect::<Vec<_>>();
    let authorization_ids = models
        .iter()
        .filter_map(|model| model.current_authorization_id)
        .collect::<Vec<_>>();
    let accounts = QuantExecutionAccountEntity::find()
        .filter(QuantExecutionAccountColumn::ExecutionAccountId.is_in(account_ids))
        .all(db)
        .await
        .map_err(StorageError::from)?
        .into_iter()
        .map(|account| (account.execution_account_id, account))
        .collect();
    let authorizations = if authorization_ids.is_empty() {
        HashMap::new()
    } else {
        QuantSettlementAuthorizationEntity::find()
            .filter(
                QuantSettlementAuthorizationColumn::SettlementAuthorizationId
                    .is_in(authorization_ids),
            )
            .all(db)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(|authorization| (authorization.settlement_authorization_id, authorization))
            .collect()
    };
    Ok((accounts, authorizations))
}

fn assemble_redeem(
    model: QuantSettlementRedeemModel,
    accounts: &HashMap<ExecutionAccountId, QuantExecutionAccountModel>,
    authorizations: &HashMap<SettlementAuthorizationId, QuantSettlementAuthorizationModel>,
) -> Result<SettlementRedeemInfo, StorageError> {
    let account = accounts.get(&model.execution_account_id).ok_or_else(|| {
        error::state_conflict(
            QUANT_EXECUTION_ACCOUNT,
            Some(model.execution_account_id),
            "settlement case references a missing execution account",
        )
    })?;
    let authorization = match model.current_authorization_id {
        Some(authorization_id) => {
            let authorization = authorizations.get(&authorization_id).ok_or_else(|| {
                error::state_conflict(
                    QUANT_SETTLEMENT_AUTHORIZATION,
                    Some(authorization_id),
                    "settlement case references a missing authorization attempt",
                )
            })?;
            if authorization.settlement_redeem_id != model.settlement_redeem_id {
                return Err(error::state_conflict(
                    QUANT_SETTLEMENT_AUTHORIZATION,
                    Some(authorization_id),
                    "current authorization belongs to another settlement case",
                ));
            }
            Some(authorization)
        }
        None => None,
    };
    Ok(SettlementRedeemInfo {
        settlement_redeem_id: model.settlement_redeem_id,
        market_id: model.market_id,
        yes_token_id: model.yes_token_id,
        no_token_id: model.no_token_id,
        execution_account_id: model.execution_account_id,
        resolution_content_hash: model.resolution_content_hash,
        resolution_outcome: model.resolution_outcome,
        resolved_at: model.resolved_at,
        funder_address: account.funder_address.clone(),
        wallet_kind: account.wallet_kind,
        route: model.route,
        effective_policy: model.effective_policy,
        inventory_digest: model.inventory_digest,
        contributor_lots_digest: model.contributor_lots_digest,
        state: model.state,
        readiness_status: model.readiness_status,
        readiness_evidence_json: model.readiness_evidence_json,
        target_adapter: model.target_adapter,
        target_code_hash: model.target_code_hash,
        deployment_digest: model.deployment_digest,
        deployment_evidence_version: model.deployment_evidence_version,
        verified_block_number: model.verified_block_number,
        verified_block_hash: model.verified_block_hash,
        current_authorization_id: model.current_authorization_id,
        authorization_state: authorization
            .map_or(SettlementAuthorizationState::NotRequired, |authorization| {
                authorization.state
            }),
        authorization_digest: authorization.map(|authorization| authorization.scope_digest),
        authorization_expires_at: authorization.map(|authorization| authorization.expires_at),
        authorized_by: authorization.and_then(|authorization| authorization.approved_by),
        authorized_at: authorization.and_then(|authorization| authorization.approved_at),
        authorization_revoked_at: authorization.and_then(|authorization| authorization.revoked_at),
        authorization_consumed_at: authorization
            .and_then(|authorization| authorization.consumed_at),
        reconciliation_state: model.reconciliation_state,
        payout_vector_json: model.payout_vector_json,
        balance_before_json: model.balance_before_json,
        balance_after_json: model.balance_after_json,
        expected_payout_usd: model.expected_payout_usd,
        actual_payout_usd: model.actual_payout_usd,
        gas_fee_pol: model.gas_fee_pol,
        failure_code: model.failure_code,
        attempt_count: model.attempt_count,
        retry_count: model.retry_count,
        next_attempt_at: model.next_attempt_at,
        claim_owner: model.claim_owner,
        lease_expires_at: model.lease_expires_at,
        last_error: model.last_error,
        prepared_at: model.prepared_at,
        submitted_at: model.submitted_at,
        confirmed_at: model.confirmed_at,
        failed_at: model.failed_at,
        created_at: model.created_at,
        updated_at: model.updated_at,
    })
}

async fn assemble_one_redeem(
    db: &impl ConnectionTrait,
    model: QuantSettlementRedeemModel,
) -> Result<SettlementRedeemInfo, StorageError> {
    let (accounts, authorizations) = load_redeem_context(db, slice::from_ref(&model)).await?;
    assemble_redeem(model, &accounts, &authorizations)
}

fn validate_inventory_rows(
    settlement_redeem_id: SettlementRedeemId,
    execution_account_id: ExecutionAccountId,
    inventory_digest: ContentHash,
    contributor_lots_digest: ContentHash,
    frozen_lots: &[SettlementDiscoveryLot],
    rows: &[NewSettlementInventoryLot],
) -> Result<(), StorageError> {
    if rows.len() != frozen_lots.len() {
        return Err(error::invariant_violation(
            Some(QUANT_SETTLEMENT_REDEEM),
            "settlement inventory rows do not match the durable open-lot set",
        ));
    }
    let mut by_position = HashMap::with_capacity(rows.len());
    for row in rows {
        if row.settlement_redeem_id != settlement_redeem_id
            || row.execution_account_id != execution_account_id
            || row.inventory_digest != inventory_digest
            || row.contributor_lots_digest != contributor_lots_digest
            || by_position.insert(row.position_id, row).is_some()
        {
            return Err(error::invariant_violation(
                Some(QUANT_SETTLEMENT_REDEEM),
                "settlement inventory row identity or digest is inconsistent",
            ));
        }
    }
    for lot in frozen_lots {
        let row = by_position.get(&lot.position_id).ok_or_else(|| {
            error::invariant_violation(
                Some(QUANT_SETTLEMENT_REDEEM),
                "settlement inventory omitted a durable open lot",
            )
        })?;
        if row.order_intent_id != lot.order_intent_id
            || row.execution_account_id != lot.execution_account_id
            || row.token_id != lot.token_id
            || row.side != lot.side
            || row.shares != lot.shares
            || row.cost_basis_usd != lot.cost_basis_usd
            || row.settlement_mode != lot.settlement_mode
            || row.redeem_policy != lot.redeem_policy
            || row.position_version_at != lot.position_version_at
            || row.intent_version_at != lot.intent_version_at
        {
            return Err(error::invariant_violation(
                Some(QUANT_SETTLEMENT_REDEEM),
                "settlement inventory row diverges from the durable position and intent",
            ));
        }
    }
    Ok(())
}

fn validate_pristine_discovery(redeem: &NewSettlementRedeem) -> Result<(), StorageError> {
    if redeem.resolution_outcome.trim().is_empty()
        || redeem.resolution_outcome != redeem.resolution_outcome.trim()
        || redeem.state != SettlementCaseState::Discovered
        || redeem.readiness_status != SettlementReadinessStatus::Unchecked
        || redeem.readiness_evidence_json != SettlementReadinessEvidence::default()
        || redeem.target_adapter.is_some()
        || redeem.target_code_hash.is_some()
        || redeem.deployment_digest.is_some()
        || redeem.deployment_evidence_version.is_some()
        || redeem.verified_block_number.is_some()
        || redeem.verified_block_hash.is_some()
        || redeem.current_authorization_id.is_some()
        || redeem.reconciliation_state != SettlementReconciliationState::NotRequired
        || redeem.payout_vector_json != SettlementPayoutVector::unresolved()
        || redeem.balance_before_json.is_some()
        || redeem.balance_after_json.is_some()
        || redeem.expected_payout_usd.is_some()
        || redeem.actual_payout_usd.is_some()
        || redeem.gas_fee_pol.is_some()
        || redeem.failure_code.is_some()
        || redeem.attempt_count != 0
        || redeem.retry_count != 0
        || redeem.next_attempt_at.is_some()
        || redeem.claim_owner.is_some()
        || redeem.lease_expires_at.is_some()
        || redeem.last_error.is_some()
        || redeem.prepared_at.is_some()
        || redeem.submitted_at.is_some()
        || redeem.confirmed_at.is_some()
        || redeem.failed_at.is_some()
        || redeem.resolved_at > redeem.created_at
        || redeem.created_at != redeem.updated_at
    {
        return Err(error::invariant_violation(
            Some(QUANT_SETTLEMENT_REDEEM),
            "newly discovered settlement case must be pristine and capability-free",
        ));
    }
    Ok(())
}

fn validate_preflight_command(command: &PersistSettlementPreflight) -> Result<(), StorageError> {
    let capability_complete = command.target_adapter.is_some()
        && command.target_code_hash.is_some()
        && command.deployment_digest.is_some()
        && command.deployment_evidence_version.is_some()
        && command.verified_block_number.is_some_and(|block| block > 0)
        && command.verified_block_hash.is_some();
    let ready = command.readiness_status == SettlementReadinessStatus::Ready
        && command.readiness_evidence.reasons.is_empty()
        && capability_complete
        && command.payout_vector.denominator.as_str() != "0"
        && command.balance_before.is_some()
        && command.expected_payout_usd.is_some()
        && command.failure_code.is_none()
        && command.next_attempt_at.is_none();
    let blocked = command.readiness_status == SettlementReadinessStatus::Blocked
        && !command.readiness_evidence.reasons.is_empty()
        && !capability_complete
        && command.target_adapter.is_none()
        && command.target_code_hash.is_none()
        && command.deployment_digest.is_none()
        && command.deployment_evidence_version.is_none()
        && command.verified_block_number.is_none()
        && command.verified_block_hash.is_none()
        && command.payout_vector == SettlementPayoutVector::unresolved()
        && command.balance_before.is_none()
        && command.expected_payout_usd.is_none()
        && command.failure_code.is_some()
        && command
            .next_attempt_at
            .is_some_and(|retry_at| retry_at > command.observed_at);
    if !ready && !blocked {
        return Err(error::invariant_violation(
            Some(QUANT_SETTLEMENT_REDEEM),
            "settlement preflight must be either complete Ready evidence or closed Blocked evidence",
        ));
    }
    Ok(())
}

fn validate_frozen_case(
    redeem: &NewSettlementRedeem,
    candidate: SettlementDiscoveryCandidate,
    rows: &[NewSettlementInventoryLot],
) -> Result<(), StorageError> {
    validate_pristine_discovery(redeem)?;
    if redeem.market_id != candidate.market_id
        || redeem.yes_token_id != candidate.yes_token_id
        || redeem.no_token_id != candidate.no_token_id
        || redeem.execution_account_id != candidate.execution_account_id
        || redeem.route != candidate.route
        || redeem.resolution_content_hash != candidate.resolution_content_hash
        || redeem.resolution_outcome != candidate.resolution_outcome
        || redeem.resolved_at != candidate.resolved_at
    {
        return Err(error::invariant_violation(
            Some(QUANT_SETTLEMENT_REDEEM),
            "discovered case identity diverges from the durable resolved market",
        ));
    }
    let frozen = candidate.freeze().map_err(|source| {
        error::invariant_violation(
            Some(QUANT_SETTLEMENT_REDEEM),
            format!("cannot freeze durable settlement inventory: {source}"),
        )
    })?;
    if redeem.effective_policy != frozen.effective_policy
        || redeem.inventory_digest != frozen.inventory_digest
        || redeem.contributor_lots_digest != frozen.contributor_lots_digest
    {
        return Err(error::invariant_violation(
            Some(QUANT_SETTLEMENT_REDEEM),
            "discovered case policy or digest diverges from the canonical inventory",
        ));
    }
    validate_inventory_rows(
        redeem.settlement_redeem_id,
        redeem.execution_account_id,
        redeem.inventory_digest,
        redeem.contributor_lots_digest,
        &frozen.lots,
        rows,
    )
}

async fn current_authorization(
    db: &impl ConnectionTrait,
    model: &QuantSettlementRedeemModel,
) -> Result<Option<QuantSettlementAuthorizationModel>, StorageError> {
    let Some(authorization_id) = model.current_authorization_id else {
        return Ok(None);
    };
    let authorization = QuantSettlementAuthorizationEntity::find_by_id(authorization_id)
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| {
            error::state_conflict(
                QUANT_SETTLEMENT_AUTHORIZATION,
                Some(authorization_id),
                "settlement case references a missing authorization attempt",
            )
        })?;
    if authorization.settlement_redeem_id != model.settlement_redeem_id {
        return Err(error::state_conflict(
            QUANT_SETTLEMENT_AUTHORIZATION,
            Some(authorization_id),
            "current authorization belongs to another settlement case",
        ));
    }
    Ok(Some(authorization))
}

async fn invalidate_authorization_for_inventory_change(
    db: &impl ConnectionTrait,
    model: &QuantSettlementRedeemModel,
    observed_at: DateTime<Utc>,
) -> Result<(), StorageError> {
    let Some(authorization) = current_authorization(db, model).await? else {
        return Ok(());
    };
    if authorization.state == SettlementAuthorizationState::Consumed {
        return Err(error::state_conflict(
            QUANT_SETTLEMENT_REDEEM,
            Some(model.settlement_redeem_id),
            "consumed authorization forbids inventory invalidation",
        ));
    }
    if matches!(
        authorization.state,
        SettlementAuthorizationState::Pending | SettlementAuthorizationState::Approved
    ) {
        let mut expired = authorization.into_active_model();
        expired.state = ActiveValue::Set(SettlementAuthorizationState::Expired);
        expired.expired_at = ActiveValue::Set(Some(observed_at));
        expired.update(db).await.map_err(StorageError::from)?;
    }
    Ok(())
}

fn reset_inventory_dependent_state(
    active: &mut QuantSettlementRedeemActiveModel,
    state: SettlementCaseState,
) {
    active.state = ActiveValue::Set(state);
    active.readiness_status = ActiveValue::Set(SettlementReadinessStatus::Unchecked);
    active.readiness_evidence_json = ActiveValue::Set(SettlementReadinessEvidence::default());
    active.target_adapter = ActiveValue::Set(None);
    active.target_code_hash = ActiveValue::Set(None);
    active.deployment_digest = ActiveValue::Set(None);
    active.deployment_evidence_version = ActiveValue::Set(None);
    active.verified_block_number = ActiveValue::Set(None);
    active.verified_block_hash = ActiveValue::Set(None);
    active.current_authorization_id = ActiveValue::Set(None);
    active.reconciliation_state = ActiveValue::Set(SettlementReconciliationState::NotRequired);
    active.payout_vector_json = ActiveValue::Set(SettlementPayoutVector::unresolved());
    active.balance_before_json = ActiveValue::Set(None);
    active.balance_after_json = ActiveValue::Set(None);
    active.expected_payout_usd = ActiveValue::Set(None);
    active.actual_payout_usd = ActiveValue::Set(None);
    active.gas_fee_pol = ActiveValue::Set(None);
    active.failure_code = ActiveValue::Set(None);
    active.attempt_count = ActiveValue::Set(0);
    active.retry_count = ActiveValue::Set(0);
    active.next_attempt_at = ActiveValue::Set(None);
    active.claim_owner = ActiveValue::Set(None);
    active.lease_expires_at = ActiveValue::Set(None);
    active.last_error = ActiveValue::Set(None);
    active.prepared_at = ActiveValue::Set(None);
    active.submitted_at = ActiveValue::Set(None);
    active.confirmed_at = ActiveValue::Set(None);
    active.failed_at = ActiveValue::Set(None);
}

async fn claim_next(
    db: &DatabaseConnection,
    owner: &WorkerId,
    now: DateTime<Utc>,
    lease_expires_at: DateTime<Utc>,
    class: SettlementClaimClass,
) -> Result<Option<SettlementWorkClaim>, StorageError> {
    if lease_expires_at <= now {
        return Err(error::invariant_violation(
            Some(QUANT_SETTLEMENT_REDEEM),
            "settlement claim lease must expire after database now",
        ));
    }
    let txn = db.begin().await.map_err(StorageError::from)?;
    let active_ids = Query::select()
        .column(QuantSettlementChainSubmissionColumn::SettlementRedeemId)
        .from(QuantSettlementChainSubmissionEntity)
        .and_where(QuantSettlementChainSubmissionColumn::SettlementRedeemId.is_not_null())
        .and_where(
            QuantSettlementChainSubmissionColumn::Purpose.eq(SettlementSubmissionPurpose::Redeem),
        )
        .and_where(QuantSettlementChainSubmissionColumn::State.is_in(ACTIVE_SUBMISSION_STATES))
        .to_owned();
    let lease_available = Condition::any()
        .add(Column::ClaimOwner.is_null())
        .add(Column::LeaseExpiresAt.lte(now));
    let claimable_authorization_ids = Query::select()
        .column(QuantSettlementAuthorizationColumn::SettlementAuthorizationId)
        .from(QuantSettlementAuthorizationEntity)
        .cond_where(
            Condition::any()
                .add(
                    Condition::all()
                        .add(
                            QuantSettlementAuthorizationColumn::State
                                .eq(SettlementAuthorizationState::Approved),
                        )
                        .add(QuantSettlementAuthorizationColumn::ExpiresAt.gt(now)),
                )
                .add(QuantSettlementAuthorizationColumn::State.is_in([
                    SettlementAuthorizationState::Revoked,
                    SettlementAuthorizationState::Expired,
                ]))
                .add(
                    Condition::all()
                        .add(QuantSettlementAuthorizationColumn::State.is_in([
                            SettlementAuthorizationState::Pending,
                            SettlementAuthorizationState::Approved,
                        ]))
                        .add(QuantSettlementAuthorizationColumn::ExpiresAt.lte(now)),
                ),
        )
        .to_owned();
    let mut query = Entity::find()
        .filter(lease_available)
        .order_by_asc(Column::CreatedAt);
    query = match class {
        SettlementClaimClass::Recovery => query
            .filter(Column::SettlementRedeemId.in_subquery(active_ids))
            .filter(
                Condition::any()
                    .add(Column::NextAttemptAt.is_null())
                    .add(Column::NextAttemptAt.lte(now)),
            ),
        SettlementClaimClass::Preflight => query
            .filter(Column::SettlementRedeemId.not_in_subquery(active_ids))
            .filter(Column::ReadinessStatus.ne(SettlementReadinessStatus::Ready))
            .filter(Column::State.is_in([
                SettlementCaseState::Discovered,
                SettlementCaseState::RetryScheduled,
            ]))
            .filter(
                Condition::any()
                    .add(Column::NextAttemptAt.is_null())
                    .add(Column::NextAttemptAt.lte(now)),
            ),
        SettlementClaimClass::Submission => query
            .filter(Column::SettlementRedeemId.not_in_subquery(active_ids))
            .filter(Column::ReadinessStatus.eq(SettlementReadinessStatus::Ready))
            .filter(Column::EffectivePolicy.eq(SettlementEffectivePolicy::AutomaticEligible))
            .filter(Column::State.is_in([
                SettlementCaseState::Discovered,
                SettlementCaseState::Prepared,
                SettlementCaseState::RetryScheduled,
            ]))
            .filter(
                Condition::any()
                    .add(Column::CurrentAuthorizationId.is_null())
                    .add(Column::CurrentAuthorizationId.in_subquery(claimable_authorization_ids)),
            )
            .filter(
                Condition::any()
                    .add(Column::NextAttemptAt.is_null())
                    .add(Column::NextAttemptAt.lte(now)),
            ),
    };
    let candidate = query
        .lock_with_behavior(LockType::Update, LockBehavior::SkipLocked)
        .one(&txn)
        .await
        .map_err(StorageError::from)?;
    let Some(model) = candidate else {
        txn.commit().await.map_err(StorageError::from)?;
        return Ok(None);
    };
    let redeem_id = model.settlement_redeem_id;
    let mut active = model.into_active_model();
    active.claim_owner = ActiveValue::Set(Some(*owner));
    active.lease_expires_at = ActiveValue::Set(Some(lease_expires_at));
    let claimed = active.update(&txn).await.map_err(StorageError::from)?;
    let submission = active_submission(&txn, redeem_id).await?;
    if (class == SettlementClaimClass::Recovery) != submission.is_some() {
        return Err(error::state_conflict(
            QUANT_SETTLEMENT_REDEEM,
            Some(redeem_id),
            "settlement claim class changed while row was locked",
        ));
    }
    let mut authorization = current_authorization(&txn, &claimed).await?;
    if authorization.as_ref().is_some_and(|authorization| {
        matches!(
            authorization.state,
            SettlementAuthorizationState::Pending | SettlementAuthorizationState::Approved
        ) && authorization.expires_at <= now
    }) {
        let mut expired = authorization
            .take()
            .ok_or_else(|| {
                error::invariant_violation(
                    Some(QUANT_SETTLEMENT_AUTHORIZATION),
                    "expired authorization disappeared during claim",
                )
            })?
            .into_active_model();
        expired.state = ActiveValue::Set(SettlementAuthorizationState::Expired);
        expired.expired_at = ActiveValue::Set(Some(now));
        authorization = Some(expired.update(&txn).await.map_err(StorageError::from)?);
    }
    let redeem = assemble_one_redeem(&txn, claimed).await?;
    txn.commit().await.map_err(StorageError::from)?;
    Ok(Some(SettlementWorkClaim {
        redeem,
        authorization: authorization.map(Into::into),
        active_submission: submission.map(Into::into),
    }))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SettlementClaimClass {
    Recovery,
    Preflight,
    Submission,
}

async fn active_submission(
    db: &impl ConnectionTrait,
    settlement_redeem_id: SettlementRedeemId,
) -> Result<Option<QuantSettlementChainSubmissionModel>, StorageError> {
    QuantSettlementChainSubmissionEntity::find()
        .filter(QuantSettlementChainSubmissionColumn::SettlementRedeemId.eq(settlement_redeem_id))
        .filter(
            QuantSettlementChainSubmissionColumn::Purpose.eq(SettlementSubmissionPurpose::Redeem),
        )
        .filter(QuantSettlementChainSubmissionColumn::State.is_in(ACTIVE_SUBMISSION_STATES))
        .one(db)
        .await
        .map_err(StorageError::from)
}

async fn lock_case(
    db: &impl ConnectionTrait,
    settlement_redeem_id: SettlementRedeemId,
) -> Result<QuantSettlementRedeemModel, StorageError> {
    Entity::find_by_id(settlement_redeem_id)
        .lock_exclusive()
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| error::not_found(QUANT_SETTLEMENT_REDEEM, settlement_redeem_id))
}

async fn lock_dispatching_submission(
    db: &impl ConnectionTrait,
    settlement_redeem_id: SettlementRedeemId,
    submission_id: SettlementChainSubmissionId,
    expected_envelope_hash: ContentHash,
) -> Result<QuantSettlementChainSubmissionModel, StorageError> {
    let submission = QuantSettlementChainSubmissionEntity::find_by_id(submission_id)
        .lock_exclusive()
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| error::not_found(QUANT_SETTLEMENT_CHAIN_SUBMISSION, submission_id))?;
    if submission.settlement_redeem_id != Some(settlement_redeem_id)
        || submission.state != SettlementSubmissionState::Dispatching
        || submission.signed_envelope_hash != Some(expected_envelope_hash)
    {
        return Err(error::state_conflict(
            QUANT_SETTLEMENT_CHAIN_SUBMISSION,
            Some(submission_id),
            "dispatch acceptance envelope/case/state compare-and-swap failed",
        ));
    }
    Ok(submission)
}

fn require_live_claim(
    model: &QuantSettlementRedeemModel,
    owner: &WorkerId,
    now: DateTime<Utc>,
) -> Result<(), StorageError> {
    if model.claim_owner.as_ref() != Some(owner)
        || model
            .lease_expires_at
            .is_none_or(|lease_expires_at| lease_expires_at <= now)
    {
        return Err(error::state_conflict(
            QUANT_SETTLEMENT_REDEEM,
            Some(model.settlement_redeem_id),
            "settlement case lease is absent, expired, or owned by another worker",
        ));
    }
    Ok(())
}

async fn require_execution_quiescence(
    db: &impl ConnectionTrait,
    model: &QuantSettlementRedeemModel,
) -> Result<(), StorageError> {
    // Keep shared locks on every order/intent whose venue outcome can still
    // change inventory until the caller's transaction commits. This includes
    // a partial fill until its remainder is terminally resolved.
    // Submission-result and reconciliation writes take an exclusive order
    // lock before changing the position, so a late fill cannot land between
    // this check and the settlement CAS.
    let orders = QuantExecutionOrderEntity::find()
        .join(
            JoinType::InnerJoin,
            QuantExecutionOrderRelation::OrderIntent.def(),
        )
        .filter(QuantExecutionOrderColumn::MarketId.eq(model.market_id.clone()))
        .filter(QuantOrderIntentColumn::ExecutionAccountId.eq(model.execution_account_id))
        .filter(QuantExecutionOrderColumn::State.is_in(UNSETTLED_EXECUTION_ORDER_STATES))
        .lock_shared()
        .all(db)
        .await
        .map_err(StorageError::from)?;
    let unsettled = orders.len();
    if unsettled > 0 {
        return Err(error::state_conflict(
            QUANT_SETTLEMENT_REDEEM,
            Some(model.settlement_redeem_id),
            format!(
                "{unsettled} unsettled execution order(s) can still change the frozen inventory"
            ),
        ));
    }
    Ok(())
}

async fn require_current_inventory(
    db: &impl ConnectionTrait,
    model: &QuantSettlementRedeemModel,
) -> Result<(), StorageError> {
    let candidate = inventory_candidate(db, &model.market_id, model.execution_account_id)
        .await?
        .ok_or_else(|| {
            error::state_conflict(
                QUANT_SETTLEMENT_REDEEM,
                Some(model.settlement_redeem_id),
                "durable open settlement inventory disappeared before money admission",
            )
        })?;
    let exact_resolution = candidate.market_id == model.market_id
        && candidate.yes_token_id == model.yes_token_id
        && candidate.no_token_id == model.no_token_id
        && candidate.execution_account_id == model.execution_account_id
        && candidate.route == model.route
        && candidate.resolution_content_hash == model.resolution_content_hash
        && candidate.resolution_outcome == model.resolution_outcome
        && candidate.resolved_at == model.resolved_at;
    let frozen = candidate.freeze().map_err(|source| {
        error::invariant_violation(
            Some(QUANT_SETTLEMENT_REDEEM),
            format!("cannot verify current settlement inventory: {source}"),
        )
    })?;
    if !exact_resolution
        || frozen.effective_policy != model.effective_policy
        || frozen.inventory_digest != model.inventory_digest
        || frozen.contributor_lots_digest != model.contributor_lots_digest
    {
        return Err(error::state_conflict(
            QUANT_SETTLEMENT_REDEEM,
            Some(model.settlement_redeem_id),
            "current open inventory or effective policy changed after the case was frozen",
        ));
    }
    Ok(())
}

fn require_current_ready_scope(
    model: &QuantSettlementRedeemModel,
    expected_target_adapter: &EvmAddress,
    expected_deployment_digest: ContentHash,
) -> Result<(), StorageError> {
    if model.effective_policy != SettlementEffectivePolicy::AutomaticEligible
        || model.readiness_status != SettlementReadinessStatus::Ready
        || model.target_adapter.as_ref() != Some(expected_target_adapter)
        || model.deployment_digest != Some(expected_deployment_digest)
        || model.target_code_hash.is_none()
        || model.deployment_evidence_version.is_none()
        || model.verified_block_number.is_none()
        || model.verified_block_hash.is_none()
    {
        return Err(error::state_conflict(
            QUANT_SETTLEMENT_REDEEM,
            Some(model.settlement_redeem_id),
            "settlement inventory policy or current verified capability no longer permits a new submission",
        ));
    }
    Ok(())
}

fn require_prepared_submission_scope(
    model: &QuantSettlementRedeemModel,
    authorization: Option<&QuantSettlementAuthorizationModel>,
    canary: Option<&QuantSettlementGovernedActionModel>,
    command: &PersistPreparedSettlementSubmission,
) -> Result<(), StorageError> {
    let submission = &command.submission;
    let expected_ordinal = model.attempt_count.checked_add(1).ok_or_else(|| {
        error::invariant_violation(
            Some(QUANT_SETTLEMENT_REDEEM),
            "settlement attempt ordinal overflow",
        )
    })?;
    require_current_ready_scope(
        model,
        &submission.target_adapter,
        submission.deployment_digest,
    )?;
    let exact_capability = submission.purpose == SettlementSubmissionPurpose::Redeem
        && submission.settlement_redeem_id == Some(model.settlement_redeem_id)
        && submission.settlement_governed_action_id.is_none()
        && submission.canary_action_id == command.expected_canary_action_id
        && submission.state == SettlementSubmissionState::Prepared
        && submission.route == model.route
        && Some(submission.target_adapter.clone()) == model.target_adapter
        && Some(submission.target_code_hash.clone()) == model.target_code_hash
        && Some(submission.deployment_digest) == model.deployment_digest
        && Some(submission.deployment_evidence_version.clone())
            == model.deployment_evidence_version
        && model
            .verified_block_number
            .is_some_and(|block| submission.verified_block_number >= block)
        && submission.attempt_ordinal == expected_ordinal
        && submission.signed_envelope.is_some()
        && submission.signed_envelope_hash.is_some()
        && submission.prepared_nonce.is_some();
    if !exact_capability {
        return Err(error::state_conflict(
            QUANT_SETTLEMENT_CHAIN_SUBMISSION,
            Some(submission.settlement_chain_submission_id),
            "prepared submission does not exactly match the leased current capability",
        ));
    }
    match command.expected_authorization_digest {
        Some(digest)
            if authorization.is_some_and(|authorization| {
                authorization.state == SettlementAuthorizationState::Approved
                    && authorization.scope_digest == digest
                    && authorization.expires_at > command.persisted_at
            }) => {}
        None if authorization.is_none_or(|authorization| {
            matches!(
                authorization.state,
                SettlementAuthorizationState::Expired | SettlementAuthorizationState::Revoked
            )
        }) => {}
        _ => {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_REDEEM,
                Some(model.settlement_redeem_id),
                "settlement authorization is absent, expired, consumed, or digest-mismatched",
            ));
        }
    }
    match (command.expected_canary_action_id, canary) {
        (Some(expected_id), Some(canary))
            if canary.settlement_governed_action_id == expected_id
                && canary.execution_account_id == model.execution_account_id
                && canary.settlement_redeem_id == Some(model.settlement_redeem_id)
                && canary.kind == SettlementGovernedActionKind::CanaryGrant
                && canary.state == SettlementGovernedActionState::Authorized
                && canary.route == Some(model.route)
                && canary.target_adapter == model.target_adapter
                && canary.deployment_digest == model.deployment_digest
                && canary.deployment_evidence_version == model.deployment_evidence_version
                && canary.authorization_digest == command.expected_authorization_digest
                && canary.expires_at > command.persisted_at
                && canary
                    .payout_ceiling_usd
                    .zip(model.expected_payout_usd)
                    .is_some_and(|(ceiling, expected)| ceiling >= expected) => {}
        (None, None) => {}
        _ => {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_GOVERNED_ACTION,
                command.expected_canary_action_id,
                "canary grant is absent, expired, consumed, or scope-mismatched",
            ));
        }
    }
    Ok(())
}

fn validate_confirmation_write(write: &ConfirmSettlementRedeem) -> Result<(), StorageError> {
    if write.lots.is_empty() {
        return Err(error::invariant_violation(
            Some(QUANT_SETTLEMENT_REDEEM),
            "confirmed settlement redeem must close at least one lot",
        ));
    }
    let lot_payout = write.lots.iter().map(|lot| lot.lot.payout_usd).sum::<Usd>();
    if lot_payout != write.actual_payout_usd
        || write
            .lots
            .iter()
            .any(|lot| lot.lot.settlement_redeem_id != write.settlement_redeem_id)
    {
        return Err(error::invariant_violation(
            Some(QUANT_SETTLEMENT_REDEEM),
            "settlement lot identities/payouts do not exactly match the confirmation",
        ));
    }
    let DomainEventPayload::SettlementRedeemConfirmed(event_payload) = &write.outbox_event.payload
    else {
        return Err(error::invariant_violation(
            Some(QUANT_SETTLEMENT_REDEEM),
            "settlement confirmation requires a settlement outbox payload",
        ));
    };
    if !write.outbox_event.validate_integrity()
        || event_payload.settlement_redeem_id != write.settlement_redeem_id
        || event_payload.settlement_chain_submission_id != write.settlement_chain_submission_id
        || event_payload.transaction_hash != write.receipt_evidence_json.transaction_hash
        || event_payload.actual_payout_usd != write.actual_payout_usd
        || usize::try_from(event_payload.lot_count).ok() != Some(write.lots.len())
    {
        return Err(error::invariant_violation(
            Some(QUANT_SETTLEMENT_REDEEM),
            "settlement outbox identity or content digest is invalid",
        ));
    }
    Ok(())
}

impl PgSettlementRedeemRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl SettlementRedeemRepository for PgSettlementRedeemRepository {
    async fn find_by_id(
        &self,
        settlement_redeem_id: &SettlementRedeemId,
    ) -> Result<Option<SettlementRedeemInfo>, StorageError> {
        let model = Entity::find_by_id(*settlement_redeem_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?;
        match model {
            Some(model) => assemble_one_redeem(&self.db, model).await.map(Some),
            None => Ok(None),
        }
    }

    async fn page(
        &self,
        query: SettlementRedeemListQuery,
    ) -> Result<Paginated<SettlementRedeemSummary>, StorageError> {
        let page: Paginated<QuantSettlementRedeemModel> = paginate_mapped(
            Entity::find()
                .filter(page_condition(&query))
                .order_by_desc(Column::CreatedAt),
            &self.db,
            PageWindow::from_query(&query),
            |model| model,
        )
        .await?;
        let (accounts, authorizations) = load_redeem_context(&self.db, &page.items).await?;
        let redeems = page
            .items
            .into_iter()
            .map(|model| assemble_redeem(model, &accounts, &authorizations))
            .collect::<Result<Vec<_>, _>>()?;
        let counts = inventory_lot_counts_for(
            &self.db,
            redeems
                .iter()
                .map(|redeem| (redeem.settlement_redeem_id, redeem.inventory_digest)),
        )
        .await?;
        let items = redeems
            .into_iter()
            .map(|redeem| {
                let inventory_lot_count = counts
                    .get(&redeem.settlement_redeem_id)
                    .copied()
                    .unwrap_or(0);
                SettlementRedeemSummary {
                    redeem,
                    inventory_lot_count,
                }
            })
            .collect();
        Ok(Paginated::new(items, page.total, page.page, page.size))
    }

    async fn list_lots_by_redeem(
        &self,
        settlement_redeem_id: &SettlementRedeemId,
    ) -> Result<Vec<SettlementRedeemLotInfo>, StorageError> {
        QuantSettlementRedeemLotEntity::find()
            .filter(QuantSettlementRedeemLotColumn::SettlementRedeemId.eq(*settlement_redeem_id))
            .order_by_asc(QuantSettlementRedeemLotColumn::CreatedAt)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn find_submission_by_id(
        &self,
        submission_id: &SettlementChainSubmissionId,
    ) -> Result<Option<SettlementChainSubmissionInfo>, StorageError> {
        QuantSettlementChainSubmissionEntity::find_by_id(*submission_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn list_submissions_by_redeem(
        &self,
        settlement_redeem_id: &SettlementRedeemId,
    ) -> Result<Vec<SettlementChainSubmissionInfo>, StorageError> {
        QuantSettlementChainSubmissionEntity::find()
            .filter(
                QuantSettlementChainSubmissionColumn::SettlementRedeemId.eq(*settlement_redeem_id),
            )
            .order_by_asc(QuantSettlementChainSubmissionColumn::CreatedAt)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn find_by_market_account(
        &self,
        market_id: &MarketId,
        execution_account_id: &ExecutionAccountId,
    ) -> Result<Option<SettlementRedeemInfo>, StorageError> {
        let model = Entity::find()
            .filter(Column::MarketId.eq(market_id.clone()))
            .filter(Column::ExecutionAccountId.eq(*execution_account_id))
            .one(&self.db)
            .await
            .map_err(StorageError::from)?;
        match model {
            Some(model) => assemble_one_redeem(&self.db, model).await.map(Some),
            None => Ok(None),
        }
    }

    async fn find_discovery_candidates(
        &self,
        limit: u64,
    ) -> Result<Vec<SettlementDiscoveryCandidate>, StorageError> {
        let scopes = discovery_scopes(&self.db, limit).await?;
        assemble_discovery_candidates(discovery_rows(&self.db, &scopes).await?)
    }

    async fn load_inventory_candidate(
        &self,
        market_id: &MarketId,
        execution_account_id: &ExecutionAccountId,
    ) -> Result<Option<SettlementDiscoveryCandidate>, StorageError> {
        inventory_candidate(&self.db, market_id, *execution_account_id).await
    }

    async fn list_refreshable_inventory_cases(
        &self,
        limit: u64,
    ) -> Result<Vec<SettlementRedeemInfo>, StorageError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let models = Entity::find()
            .filter(Column::State.is_in([
                SettlementCaseState::Discovered,
                SettlementCaseState::Prepared,
                SettlementCaseState::RetryScheduled,
                SettlementCaseState::NotRequired,
            ]))
            .filter(Expr::cust(
                "NOT EXISTS (SELECT 1 FROM quant_settlement_chain_submission AS submission WHERE submission.settlement_redeem_id = quant_settlement_redeem.settlement_redeem_id)",
            ))
            .order_by_asc(Column::UpdatedAt)
            .limit(limit)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        let (accounts, authorizations) = load_redeem_context(&self.db, &models).await?;
        models
            .into_iter()
            .map(|model| assemble_redeem(model, &accounts, &authorizations))
            .collect()
    }

    async fn insert_discovered_case(
        &self,
        redeem: NewSettlementRedeem,
        lots: Vec<NewSettlementInventoryLot>,
    ) -> Result<SettlementRedeemInfo, StorageError> {
        let txn = self
            .db
            .begin_with_config(Some(IsolationLevel::Serializable), None)
            .await
            .map_err(StorageError::from)?;
        let candidate = inventory_candidate(&txn, &redeem.market_id, redeem.execution_account_id)
            .await?
            .ok_or_else(|| {
                error::state_conflict(
                    QUANT_SETTLEMENT_REDEEM,
                    Some(redeem.settlement_redeem_id),
                    "resolved market/account no longer has redeemable open inventory",
                )
            })?;
        validate_frozen_case(&redeem, candidate, &lots)?;
        let model = Entity::insert(redeem.into_active_model())
            .exec_with_returning(&txn)
            .await
            .map_err(|source| {
                error::map_unique(
                    source,
                    QUANT_SETTLEMENT_REDEEM,
                    "market_id,execution_account_id",
                )
            })?;
        insert_many_chunked::<QuantSettlementInventoryLotEntity, _>(&txn, lots).await?;
        let assembled = assemble_one_redeem(&txn, model).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(assembled)
    }

    async fn refresh_discovered_inventory(
        &self,
        command: RefreshSettlementInventory,
    ) -> Result<SettlementRedeemInfo, StorageError> {
        let txn = self
            .db
            .begin_with_config(Some(IsolationLevel::Serializable), None)
            .await
            .map_err(StorageError::from)?;
        let model = lock_case(&txn, command.settlement_redeem_id).await?;
        if model.inventory_digest != command.expected_inventory_digest {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_REDEEM,
                Some(model.settlement_redeem_id),
                "settlement inventory compare-and-swap failed",
            ));
        }
        if !matches!(
            model.state,
            SettlementCaseState::Discovered
                | SettlementCaseState::Prepared
                | SettlementCaseState::RetryScheduled
                | SettlementCaseState::NotRequired
        ) || model
            .lease_expires_at
            .is_some_and(|lease_expires_at| lease_expires_at > command.observed_at)
        {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_REDEEM,
                Some(model.settlement_redeem_id),
                "claimed or post-prepare settlement inventory cannot be refreshed",
            ));
        }
        if QuantSettlementChainSubmissionEntity::find()
            .filter(
                QuantSettlementChainSubmissionColumn::SettlementRedeemId
                    .eq(model.settlement_redeem_id),
            )
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .is_some()
        {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_REDEEM,
                Some(model.settlement_redeem_id),
                "durable submission identity forbids inventory refresh",
            ));
        }
        invalidate_authorization_for_inventory_change(&txn, &model, command.observed_at).await?;
        let candidate = inventory_candidate(&txn, &model.market_id, model.execution_account_id)
            .await?
            .ok_or_else(|| {
                error::state_conflict(
                    QUANT_SETTLEMENT_REDEEM,
                    Some(model.settlement_redeem_id),
                    "settlement inventory disappeared before refresh",
                )
            })?;
        if command.resolution_content_hash != candidate.resolution_content_hash
            || command.yes_token_id != candidate.yes_token_id
            || command.no_token_id != candidate.no_token_id
            || command.resolution_outcome != candidate.resolution_outcome
            || command.resolved_at != candidate.resolved_at
            || model.route != candidate.route
        {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_REDEEM,
                Some(model.settlement_redeem_id),
                "resolved market identity changed before inventory refresh",
            ));
        }
        let frozen = candidate.freeze().map_err(|source| {
            error::invariant_violation(
                Some(QUANT_SETTLEMENT_REDEEM),
                format!("cannot refresh durable settlement inventory: {source}"),
            )
        })?;
        if command.observed_at < command.resolved_at
            || frozen.lots.iter().any(|lot| {
                lot.position_version_at > command.observed_at
                    || lot.intent_version_at > command.observed_at
            })
        {
            return Err(error::invariant_violation(
                Some(QUANT_SETTLEMENT_REDEEM),
                "inventory refresh observation predates its durable contributors",
            ));
        }
        if command.effective_policy != frozen.effective_policy
            || command.inventory_digest != frozen.inventory_digest
            || command.contributor_lots_digest != frozen.contributor_lots_digest
        {
            return Err(error::invariant_violation(
                Some(QUANT_SETTLEMENT_REDEEM),
                "inventory refresh policy or digest is not canonical",
            ));
        }
        validate_inventory_rows(
            model.settlement_redeem_id,
            model.execution_account_id,
            command.inventory_digest,
            command.contributor_lots_digest,
            &frozen.lots,
            &command.lots,
        )?;
        insert_many_chunked::<QuantSettlementInventoryLotEntity, _>(&txn, command.lots).await?;

        let mut active = model.into_active_model();
        active.yes_token_id = ActiveValue::Set(command.yes_token_id);
        active.no_token_id = ActiveValue::Set(command.no_token_id);
        active.resolution_content_hash = ActiveValue::Set(command.resolution_content_hash);
        active.resolution_outcome = ActiveValue::Set(command.resolution_outcome);
        active.resolved_at = ActiveValue::Set(command.resolved_at);
        active.effective_policy = ActiveValue::Set(command.effective_policy);
        active.inventory_digest = ActiveValue::Set(command.inventory_digest);
        active.contributor_lots_digest = ActiveValue::Set(command.contributor_lots_digest);
        reset_inventory_dependent_state(&mut active, SettlementCaseState::Discovered);
        let updated = active.update(&txn).await.map_err(StorageError::from)?;
        let assembled = assemble_one_redeem(&txn, updated).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(assembled)
    }

    async fn mark_inventory_absent(
        &self,
        command: MarkSettlementInventoryAbsent,
    ) -> Result<SettlementRedeemInfo, StorageError> {
        let txn = self
            .db
            .begin_with_config(Some(IsolationLevel::Serializable), None)
            .await
            .map_err(StorageError::from)?;
        let model = lock_case(&txn, command.settlement_redeem_id).await?;
        if model.inventory_digest != command.expected_inventory_digest {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_REDEEM,
                Some(model.settlement_redeem_id),
                "absent-inventory compare-and-swap failed",
            ));
        }
        if !matches!(
            model.state,
            SettlementCaseState::Discovered
                | SettlementCaseState::Prepared
                | SettlementCaseState::RetryScheduled
                | SettlementCaseState::NotRequired
        ) || model
            .lease_expires_at
            .is_some_and(|lease_expires_at| lease_expires_at > command.observed_at)
        {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_REDEEM,
                Some(model.settlement_redeem_id),
                "claimed or post-submission settlement case cannot become not-required",
            ));
        }
        if QuantSettlementChainSubmissionEntity::find()
            .filter(
                QuantSettlementChainSubmissionColumn::SettlementRedeemId
                    .eq(model.settlement_redeem_id),
            )
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .is_some()
        {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_REDEEM,
                Some(model.settlement_redeem_id),
                "durable submission identity forbids absent-inventory transition",
            ));
        }
        if inventory_candidate(&txn, &model.market_id, model.execution_account_id)
            .await?
            .is_some()
        {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_REDEEM,
                Some(model.settlement_redeem_id),
                "durable open inventory still exists",
            ));
        }
        invalidate_authorization_for_inventory_change(&txn, &model, command.observed_at).await?;
        let mut active = model.into_active_model();
        reset_inventory_dependent_state(&mut active, SettlementCaseState::NotRequired);
        let updated = active.update(&txn).await.map_err(StorageError::from)?;
        let assembled = assemble_one_redeem(&txn, updated).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(assembled)
    }

    async fn list_current_inventory(
        &self,
        settlement_redeem_id: &SettlementRedeemId,
    ) -> Result<Vec<SettlementInventoryLotInfo>, StorageError> {
        let redeem = Entity::find_by_id(*settlement_redeem_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| error::not_found(QUANT_SETTLEMENT_REDEEM, settlement_redeem_id))?;
        QuantSettlementInventoryLotEntity::find()
            .filter(QuantSettlementInventoryLotColumn::SettlementRedeemId.eq(*settlement_redeem_id))
            .filter(QuantSettlementInventoryLotColumn::InventoryDigest.eq(redeem.inventory_digest))
            .order_by_asc(QuantSettlementInventoryLotColumn::PositionId)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn count_unsettled_execution_orders(
        &self,
        market_id: &MarketId,
        execution_account_id: &ExecutionAccountId,
    ) -> Result<u64, StorageError> {
        unsettled_execution_order_count(&self.db, market_id, *execution_account_id).await
    }

    async fn claim_next_recovery(
        &self,
        owner: &WorkerId,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> Result<Option<SettlementWorkClaim>, StorageError> {
        claim_next(
            &self.db,
            owner,
            now,
            lease_expires_at,
            SettlementClaimClass::Recovery,
        )
        .await
    }

    async fn claim_next_preflight(
        &self,
        owner: &WorkerId,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> Result<Option<SettlementWorkClaim>, StorageError> {
        claim_next(
            &self.db,
            owner,
            now,
            lease_expires_at,
            SettlementClaimClass::Preflight,
        )
        .await
    }

    async fn claim_next_new_submission(
        &self,
        owner: &WorkerId,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> Result<Option<SettlementWorkClaim>, StorageError> {
        claim_next(
            &self.db,
            owner,
            now,
            lease_expires_at,
            SettlementClaimClass::Submission,
        )
        .await
    }

    async fn renew_claim(
        &self,
        settlement_redeem_id: &SettlementRedeemId,
        owner: &WorkerId,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        if lease_expires_at <= now {
            return Err(error::invariant_violation(
                Some(QUANT_SETTLEMENT_REDEEM),
                "settlement claim renewal must extend beyond database now",
            ));
        }
        let updated = Entity::update_many()
            .col_expr(Column::LeaseExpiresAt, Expr::value(Some(lease_expires_at)))
            .filter(Column::SettlementRedeemId.eq(*settlement_redeem_id))
            .filter(Column::ClaimOwner.eq(*owner))
            .filter(Column::LeaseExpiresAt.gt(now))
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(updated.rows_affected == 1)
    }

    async fn release_claim(
        &self,
        settlement_redeem_id: &SettlementRedeemId,
        owner: &WorkerId,
    ) -> Result<bool, StorageError> {
        let updated = Entity::update_many()
            .col_expr(Column::ClaimOwner, Expr::value(None::<WorkerId>))
            .col_expr(Column::LeaseExpiresAt, Expr::value(None::<DateTime<Utc>>))
            .filter(Column::SettlementRedeemId.eq(*settlement_redeem_id))
            .filter(Column::ClaimOwner.eq(*owner))
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(updated.rows_affected == 1)
    }

    async fn persist_preflight(
        &self,
        command: PersistSettlementPreflight,
    ) -> Result<SettlementRedeemInfo, StorageError> {
        validate_preflight_command(&command)?;
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let model = lock_case(&txn, command.settlement_redeem_id).await?;
        require_live_claim(&model, &command.owner, command.observed_at)?;
        if command.observed_at < model.resolved_at
            || model.inventory_digest != command.expected_inventory_digest
            || active_submission(&txn, model.settlement_redeem_id)
                .await?
                .is_some()
        {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_REDEEM,
                Some(model.settlement_redeem_id),
                "settlement preflight inventory changed or a submission already exists",
            ));
        }
        if command.readiness_status == SettlementReadinessStatus::Ready {
            require_execution_quiescence(&txn, &model).await?;
            require_current_inventory(&txn, &model).await?;
        }
        invalidate_authorization_for_inventory_change(&txn, &model, command.observed_at).await?;
        let retry_count = if command.readiness_status == SettlementReadinessStatus::Ready {
            0
        } else {
            model.retry_count.checked_add(1).ok_or_else(|| {
                error::invariant_violation(
                    Some(QUANT_SETTLEMENT_REDEEM),
                    "settlement preflight retry count overflow",
                )
            })?
        };
        let mut active = model.into_active_model();
        active.state = ActiveValue::Set(
            if command.readiness_status == SettlementReadinessStatus::Ready {
                SettlementCaseState::Discovered
            } else {
                SettlementCaseState::RetryScheduled
            },
        );
        active.readiness_status = ActiveValue::Set(command.readiness_status);
        active.readiness_evidence_json = ActiveValue::Set(command.readiness_evidence);
        active.target_adapter = ActiveValue::Set(command.target_adapter);
        active.target_code_hash = ActiveValue::Set(command.target_code_hash);
        active.deployment_digest = ActiveValue::Set(command.deployment_digest);
        active.deployment_evidence_version = ActiveValue::Set(command.deployment_evidence_version);
        active.verified_block_number = ActiveValue::Set(command.verified_block_number);
        active.verified_block_hash = ActiveValue::Set(command.verified_block_hash);
        active.current_authorization_id = ActiveValue::Set(None);
        active.payout_vector_json = ActiveValue::Set(command.payout_vector);
        active.balance_before_json = ActiveValue::Set(command.balance_before);
        active.expected_payout_usd = ActiveValue::Set(command.expected_payout_usd);
        active.failure_code = ActiveValue::Set(command.failure_code);
        active.retry_count = ActiveValue::Set(retry_count);
        active.next_attempt_at = ActiveValue::Set(command.next_attempt_at);
        active.last_error = ActiveValue::Set(None);
        active.prepared_at = ActiveValue::Set(None);
        active.claim_owner = ActiveValue::Set(None);
        active.lease_expires_at = ActiveValue::Set(None);
        active.updated_at = ActiveValue::Set(command.observed_at);
        let updated = active.update(&txn).await.map_err(StorageError::from)?;
        let assembled = assemble_one_redeem(&txn, updated).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(assembled)
    }

    async fn schedule_retry(
        &self,
        command: ScheduleSettlementRetry,
    ) -> Result<SettlementRedeemInfo, StorageError> {
        if command.detail.trim().is_empty()
            || command.detail.len() > 2_048
            || command.next_attempt_at <= command.observed_at
        {
            return Err(error::invariant_violation(
                Some(QUANT_SETTLEMENT_REDEEM),
                "settlement retry requires bounded detail and a future next_attempt_at",
            ));
        }
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let model = lock_case(&txn, command.settlement_redeem_id).await?;
        require_live_claim(&model, &command.owner, command.observed_at)?;
        let submission = active_submission(&txn, model.settlement_redeem_id).await?;
        if submission
            .as_ref()
            .map(|value| value.settlement_chain_submission_id)
            != command.settlement_chain_submission_id
        {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_REDEEM,
                Some(model.settlement_redeem_id),
                "settlement retry durable submission identity changed",
            ));
        }
        if let Some(submission) = submission {
            let mut active = submission.into_active_model();
            let mut history = active.failure_history_json.take().unwrap_or_default();
            history.entries.push(SettlementFailureEvidence {
                code: command.failure_code,
                detail: command.detail.clone(),
                observed_at: command.observed_at,
            });
            active.failure_code = ActiveValue::Set(Some(command.failure_code));
            active.failure_history_json = ActiveValue::Set(history);
            active.last_error = ActiveValue::Set(Some(command.detail.clone()));
            active.update(&txn).await.map_err(StorageError::from)?;
        }
        let retry_count = model.retry_count.checked_add(1).ok_or_else(|| {
            error::invariant_violation(
                Some(QUANT_SETTLEMENT_REDEEM),
                "settlement retry count overflow",
            )
        })?;
        let mut active = model.into_active_model();
        if command.settlement_chain_submission_id.is_none() {
            active.state = ActiveValue::Set(SettlementCaseState::RetryScheduled);
        }
        active.failure_code = ActiveValue::Set(Some(command.failure_code));
        active.retry_count = ActiveValue::Set(retry_count);
        active.next_attempt_at = ActiveValue::Set(Some(command.next_attempt_at));
        active.last_error = ActiveValue::Set(Some(command.detail));
        active.claim_owner = ActiveValue::Set(None);
        active.lease_expires_at = ActiveValue::Set(None);
        active.updated_at = ActiveValue::Set(command.observed_at);
        let updated = active.update(&txn).await.map_err(StorageError::from)?;
        let assembled = assemble_one_redeem(&txn, updated).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(assembled)
    }

    async fn schedule_work(
        &self,
        command: ScheduleSettlementWork,
    ) -> Result<SettlementRedeemInfo, StorageError> {
        if command.next_attempt_at <= command.observed_at {
            return Err(error::invariant_violation(
                Some(QUANT_SETTLEMENT_REDEEM),
                "settlement poll must be scheduled in the future",
            ));
        }
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let model = lock_case(&txn, command.settlement_redeem_id).await?;
        require_live_claim(&model, &command.owner, command.observed_at)?;
        let submission = active_submission(&txn, model.settlement_redeem_id).await?;
        if submission
            .as_ref()
            .map(|value| value.settlement_chain_submission_id)
            != command.settlement_chain_submission_id
        {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_REDEEM,
                Some(model.settlement_redeem_id),
                "settlement poll durable submission identity changed",
            ));
        }
        let mut active = model.into_active_model();
        active.next_attempt_at = ActiveValue::Set(Some(command.next_attempt_at));
        active.claim_owner = ActiveValue::Set(None);
        active.lease_expires_at = ActiveValue::Set(None);
        active.updated_at = ActiveValue::Set(command.observed_at);
        let updated = active.update(&txn).await.map_err(StorageError::from)?;
        let assembled = assemble_one_redeem(&txn, updated).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(assembled)
    }

    async fn stage_authorization(
        &self,
        command: StageSettlementAuthorization,
    ) -> Result<SettlementRedeemInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let model = lock_case(&txn, command.settlement_redeem_id).await?;
        require_live_claim(&model, &command.owner, command.staged_at)?;
        if command.expires_at <= command.staged_at {
            return Err(error::invariant_violation(
                Some(QUANT_SETTLEMENT_REDEEM),
                "settlement authorization expiry must be later than staged_at",
            ));
        }
        require_current_ready_scope(
            &model,
            &command.expected_target_adapter,
            command.expected_deployment_digest,
        )?;
        require_execution_quiescence(&txn, &model).await?;
        require_current_inventory(&txn, &model).await?;
        if model.balance_before_json.is_none() || model.expected_payout_usd.is_none() {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_REDEEM,
                Some(model.settlement_redeem_id),
                "authorization cannot be staged before exact balance and payout preflight",
            ));
        }
        if let Some(current) = current_authorization(&txn, &model).await? {
            match current.state {
                SettlementAuthorizationState::Consumed => {
                    return Err(error::state_conflict(
                        QUANT_SETTLEMENT_AUTHORIZATION,
                        Some(current.settlement_authorization_id),
                        "consumed authorization cannot be replaced",
                    ));
                }
                SettlementAuthorizationState::Pending | SettlementAuthorizationState::Approved
                    if current.expires_at > command.staged_at =>
                {
                    return Err(error::state_conflict(
                        QUANT_SETTLEMENT_AUTHORIZATION,
                        Some(current.settlement_authorization_id),
                        "live authorization attempt must be revoked or consumed before renewal",
                    ));
                }
                SettlementAuthorizationState::Pending | SettlementAuthorizationState::Approved => {
                    let mut expired = current.into_active_model();
                    expired.state = ActiveValue::Set(SettlementAuthorizationState::Expired);
                    expired.expired_at = ActiveValue::Set(Some(command.staged_at));
                    expired.update(&txn).await.map_err(StorageError::from)?;
                }
                SettlementAuthorizationState::Revoked
                | SettlementAuthorizationState::Expired
                | SettlementAuthorizationState::NotRequired => {}
            }
        }
        if active_submission(&txn, model.settlement_redeem_id)
            .await?
            .is_some()
        {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_REDEEM,
                Some(model.settlement_redeem_id),
                "authorization cannot be staged after a durable submission exists",
            ));
        }

        let previous = QuantSettlementAuthorizationEntity::find()
            .filter(
                QuantSettlementAuthorizationColumn::SettlementRedeemId
                    .eq(model.settlement_redeem_id),
            )
            .order_by_desc(QuantSettlementAuthorizationColumn::AttemptOrdinal)
            .one(&txn)
            .await
            .map_err(StorageError::from)?;
        let attempt_ordinal = match previous {
            Some(authorization) => {
                authorization
                    .attempt_ordinal
                    .checked_add(1)
                    .ok_or_else(|| {
                        error::invariant_violation(
                            Some(QUANT_SETTLEMENT_AUTHORIZATION),
                            "settlement authorization attempt ordinal overflow",
                        )
                    })?
            }
            None => 1,
        };
        let authorization_id = SettlementAuthorizationId::from_v7();
        QuantSettlementAuthorizationEntity::insert(
            NewSettlementAuthorization {
                settlement_authorization_id: authorization_id,
                settlement_redeem_id: model.settlement_redeem_id,
                attempt_ordinal,
                state: SettlementAuthorizationState::Pending,
                scope_digest: command.digest,
                staged_by: command.owner,
                expires_at: command.expires_at,
                approved_by: None,
                approved_at: None,
                revoked_by: None,
                revoked_at: None,
                consumed_at: None,
                expired_at: None,
            }
            .into_active_model(),
        )
        .exec(&txn)
        .await
        .map_err(StorageError::from)?;

        let mut active = model.into_active_model();
        active.state = ActiveValue::Set(SettlementCaseState::Prepared);
        active.prepared_at = ActiveValue::Set(Some(command.staged_at));
        active.current_authorization_id = ActiveValue::Set(Some(authorization_id));
        let staged = active.update(&txn).await.map_err(StorageError::from)?;
        let assembled = assemble_one_redeem(&txn, staged).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(assembled)
    }

    async fn approve_authorization(
        &self,
        command: ApproveSettlementAuthorization,
    ) -> Result<SettlementRedeemInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let model = lock_case(&txn, command.settlement_redeem_id).await?;
        if model.effective_policy != SettlementEffectivePolicy::AutomaticEligible {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_REDEEM,
                Some(model.settlement_redeem_id),
                "manual-only settlement inventory cannot approve a new submission authorization",
            ));
        }
        require_execution_quiescence(&txn, &model).await?;
        require_current_inventory(&txn, &model).await?;
        let authorization = current_authorization(&txn, &model).await?.ok_or_else(|| {
            error::state_conflict(
                QUANT_SETTLEMENT_REDEEM,
                Some(model.settlement_redeem_id),
                "settlement case has no authorization attempt",
            )
        })?;
        if authorization.state == SettlementAuthorizationState::Approved
            && authorization.scope_digest == command.digest
            && authorization.approved_by == Some(command.actor)
        {
            let assembled = assemble_one_redeem(&txn, model).await?;
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(assembled);
        }
        if authorization.state != SettlementAuthorizationState::Pending
            || authorization.scope_digest != command.digest
        {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_REDEEM,
                Some(model.settlement_redeem_id),
                "authorization approval digest/state compare-and-swap failed",
            ));
        }
        if authorization.expires_at <= command.approved_at {
            let mut expired = authorization.into_active_model();
            expired.state = ActiveValue::Set(SettlementAuthorizationState::Expired);
            expired.expired_at = ActiveValue::Set(Some(command.approved_at));
            expired.update(&txn).await.map_err(StorageError::from)?;
            txn.commit().await.map_err(StorageError::from)?;
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_REDEEM,
                Some(command.settlement_redeem_id),
                "settlement authorization expired before approval",
            ));
        }

        let mut active = authorization.into_active_model();
        active.state = ActiveValue::Set(SettlementAuthorizationState::Approved);
        active.approved_by = ActiveValue::Set(Some(command.actor));
        active.approved_at = ActiveValue::Set(Some(command.approved_at));
        active.update(&txn).await.map_err(StorageError::from)?;
        let approved = assemble_one_redeem(&txn, model).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(approved)
    }

    async fn revoke_authorization(
        &self,
        command: RevokeSettlementAuthorization,
    ) -> Result<SettlementRedeemInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let model = lock_case(&txn, command.settlement_redeem_id).await?;
        let authorization = current_authorization(&txn, &model).await?.ok_or_else(|| {
            error::state_conflict(
                QUANT_SETTLEMENT_REDEEM,
                Some(model.settlement_redeem_id),
                "settlement case has no authorization attempt",
            )
        })?;
        if authorization.state == SettlementAuthorizationState::Revoked
            && authorization.scope_digest == command.digest
        {
            let assembled = assemble_one_redeem(&txn, model).await?;
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(assembled);
        }
        if authorization.state != SettlementAuthorizationState::Approved
            || authorization.scope_digest != command.digest
        {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_REDEEM,
                Some(model.settlement_redeem_id),
                "authorization revoke digest/state compare-and-swap failed",
            ));
        }
        if authorization
            .approved_at
            .is_some_and(|authorized_at| command.revoked_at < authorized_at)
        {
            return Err(error::invariant_violation(
                Some(QUANT_SETTLEMENT_REDEEM),
                "authorization revocation cannot predate approval",
            ));
        }
        let mut active = authorization.into_active_model();
        active.state = ActiveValue::Set(SettlementAuthorizationState::Revoked);
        active.revoked_by = ActiveValue::Set(Some(command.actor));
        active.revoked_at = ActiveValue::Set(Some(command.revoked_at));
        active.update(&txn).await.map_err(StorageError::from)?;
        let revoked = assemble_one_redeem(&txn, model).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(revoked)
    }

    async fn persist_prepared_submission(
        &self,
        command: PersistPreparedSettlementSubmission,
    ) -> Result<SettlementChainSubmissionInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let redeem_id = command.submission.settlement_redeem_id.ok_or_else(|| {
            error::invariant_violation(
                Some(QUANT_SETTLEMENT_CHAIN_SUBMISSION),
                "redeem submission must reference exactly one settlement case",
            )
        })?;
        let model = lock_case(&txn, redeem_id).await?;
        require_live_claim(&model, &command.owner, command.persisted_at)?;
        let authorization = current_authorization(&txn, &model).await?;
        let canary = match command.expected_canary_action_id {
            Some(action_id) => Some(
                QuantSettlementGovernedActionEntity::find_by_id(action_id)
                    .lock_exclusive()
                    .one(&txn)
                    .await
                    .map_err(StorageError::from)?
                    .ok_or_else(|| error::not_found(QUANT_SETTLEMENT_GOVERNED_ACTION, action_id))?,
            ),
            None => None,
        };
        require_prepared_submission_scope(
            &model,
            authorization.as_ref(),
            canary.as_ref(),
            &command,
        )?;
        require_execution_quiescence(&txn, &model).await?;
        require_current_inventory(&txn, &model).await?;

        let inserted = QuantSettlementChainSubmissionEntity::insert(
            command.submission.clone().into_active_model(),
        )
        .exec_with_returning(&txn)
        .await
        .map_err(StorageError::from)?;

        let mut active = model.into_active_model();
        active.state = ActiveValue::Set(SettlementCaseState::Prepared);
        active.prepared_at = ActiveValue::Set(Some(command.persisted_at));
        active.attempt_count = ActiveValue::Set(command.submission.attempt_ordinal);
        active.retry_count = ActiveValue::Set(0);
        active.verified_block_number =
            ActiveValue::Set(Some(command.submission.verified_block_number));
        active.verified_block_hash =
            ActiveValue::Set(Some(command.submission.verified_block_hash.clone()));
        active.update(&txn).await.map_err(StorageError::from)?;
        if command.expected_authorization_digest.is_some() {
            let authorization = authorization.ok_or_else(|| {
                error::state_conflict(
                    QUANT_SETTLEMENT_REDEEM,
                    Some(redeem_id),
                    "approved authorization disappeared before durable submission",
                )
            })?;
            let mut authorization_active = authorization.into_active_model();
            authorization_active.state = ActiveValue::Set(SettlementAuthorizationState::Consumed);
            authorization_active.consumed_at = ActiveValue::Set(Some(command.persisted_at));
            authorization_active
                .update(&txn)
                .await
                .map_err(StorageError::from)?;
        }
        if let Some(canary) = canary {
            let mut canary_active = canary.into_active_model();
            canary_active.state = ActiveValue::Set(SettlementGovernedActionState::Consumed);
            canary_active.consumed_at = ActiveValue::Set(Some(command.persisted_at));
            canary_active.next_attempt_at = ActiveValue::Set(None);
            canary_active.claim_owner = ActiveValue::Set(None);
            canary_active.lease_expires_at = ActiveValue::Set(None);
            canary_active
                .update(&txn)
                .await
                .map_err(StorageError::from)?;
        }
        txn.commit().await.map_err(StorageError::from)?;
        Ok(inserted.into())
    }

    async fn begin_dispatch(
        &self,
        command: BeginSettlementDispatch,
    ) -> Result<SettlementChainSubmissionInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let model = lock_case(&txn, command.settlement_redeem_id).await?;
        require_live_claim(&model, &command.owner, command.dispatching_at)?;
        if model.state != SettlementCaseState::Prepared {
            return Err(error::illegal_transition(
                QUANT_SETTLEMENT_REDEEM,
                Some(model.settlement_redeem_id),
                model.state,
                SettlementCaseState::Submitted,
            ));
        }
        let submission = QuantSettlementChainSubmissionEntity::find_by_id(
            command.settlement_chain_submission_id,
        )
        .lock_exclusive()
        .one(&txn)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| {
            error::not_found(
                QUANT_SETTLEMENT_CHAIN_SUBMISSION,
                command.settlement_chain_submission_id,
            )
        })?;
        let exact_scope = submission.settlement_redeem_id == Some(command.settlement_redeem_id)
            && submission.state == SettlementSubmissionState::Prepared
            && submission.target_adapter == command.expected_target_adapter
            && submission.deployment_digest == command.expected_deployment_digest
            && submission.calldata_hash == command.expected_calldata_hash
            && submission.signed_envelope_hash == Some(command.expected_signed_envelope_hash);
        if !exact_scope {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_CHAIN_SUBMISSION,
                Some(submission.settlement_chain_submission_id),
                "prepare-to-dispatch target/digest/calldata/envelope CAS failed",
            ));
        }

        let mut submission_active = submission.into_active_model();
        submission_active.state = ActiveValue::Set(SettlementSubmissionState::Dispatching);
        submission_active.dispatched_at = ActiveValue::Set(Some(command.dispatching_at));
        let dispatching = submission_active
            .update(&txn)
            .await
            .map_err(StorageError::from)?;
        let mut case_active = model.into_active_model();
        case_active.state = ActiveValue::Set(SettlementCaseState::Submitted);
        case_active.submitted_at = ActiveValue::Set(Some(command.dispatching_at));
        case_active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(dispatching.into())
    }

    async fn record_eoa_broadcast(
        &self,
        command: RecordEoaSettlementBroadcast,
    ) -> Result<SettlementChainSubmissionInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let model = lock_case(&txn, command.settlement_redeem_id).await?;
        require_live_claim(&model, &command.owner, command.observed_at)?;
        let submission = lock_dispatching_submission(
            &txn,
            command.settlement_redeem_id,
            command.settlement_chain_submission_id,
            command.expected_signed_envelope_hash,
        )
        .await?;
        if submission.kind != SettlementSubmissionKind::DirectEoa
            || submission.transaction_hash.is_none()
            || submission.relayer_transaction_id.is_some()
        {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_CHAIN_SUBMISSION,
                Some(submission.settlement_chain_submission_id),
                "EOA broadcast acceptance requires the frozen local transaction hash only",
            ));
        }
        let mut active = submission.into_active_model();
        active.state = ActiveValue::Set(SettlementSubmissionState::AwaitingFinality);
        active.chain_hash_observed_at = ActiveValue::Set(Some(command.observed_at));
        let awaiting = active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(awaiting.into())
    }

    async fn record_relayer_acceptance(
        &self,
        command: RecordRelayerSettlementAcceptance,
    ) -> Result<SettlementChainSubmissionInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let model = lock_case(&txn, command.settlement_redeem_id).await?;
        require_live_claim(&model, &command.owner, command.observed_at)?;
        let submission = lock_dispatching_submission(
            &txn,
            command.settlement_redeem_id,
            command.settlement_chain_submission_id,
            command.expected_signed_envelope_hash,
        )
        .await?;
        if submission.kind != SettlementSubmissionKind::Relayer
            || submission.transaction_hash.is_some()
            || submission.relayer_transaction_id.is_some()
        {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_CHAIN_SUBMISSION,
                Some(submission.settlement_chain_submission_id),
                "relayer acceptance requires an unbound opaque relayer identity",
            ));
        }
        let mut active = submission.into_active_model();
        active.state = ActiveValue::Set(SettlementSubmissionState::AwaitingChainHash);
        active.relayer_transaction_id = ActiveValue::Set(Some(command.relayer_transaction_id));
        let awaiting = active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(awaiting.into())
    }

    async fn record_relayer_chain_hash(
        &self,
        command: RecordRelayerSettlementChainHash,
    ) -> Result<SettlementChainSubmissionInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let model = lock_case(&txn, command.settlement_redeem_id).await?;
        require_live_claim(&model, &command.owner, command.observed_at)?;
        let submission = QuantSettlementChainSubmissionEntity::find_by_id(
            command.settlement_chain_submission_id,
        )
        .lock_exclusive()
        .one(&txn)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| {
            error::not_found(
                QUANT_SETTLEMENT_CHAIN_SUBMISSION,
                command.settlement_chain_submission_id,
            )
        })?;
        if submission.settlement_redeem_id != Some(command.settlement_redeem_id)
            || submission.kind != SettlementSubmissionKind::Relayer
            || submission.state != SettlementSubmissionState::AwaitingChainHash
            || submission.relayer_transaction_id.as_ref()
                != Some(&command.expected_relayer_transaction_id)
            || submission.transaction_hash.is_some()
        {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_CHAIN_SUBMISSION,
                Some(command.settlement_chain_submission_id),
                "relayer chain hash does not match the durable opaque identity and state",
            ));
        }
        let mut active = submission.into_active_model();
        active.state = ActiveValue::Set(SettlementSubmissionState::AwaitingFinality);
        active.transaction_hash = ActiveValue::Set(Some(command.transaction_hash));
        active.chain_hash_observed_at = ActiveValue::Set(Some(command.observed_at));
        let awaiting = active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(awaiting.into())
    }

    async fn confirm(
        &self,
        write: ConfirmSettlementRedeem,
    ) -> Result<SettlementRedeemInfo, StorageError> {
        validate_confirmation_write(&write)?;

        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let redeem = Entity::find_by_id(write.settlement_redeem_id)
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| error::not_found(QUANT_SETTLEMENT_REDEEM, write.settlement_redeem_id))?;

        if redeem.state == SettlementCaseState::Confirmed {
            let submission = QuantSettlementChainSubmissionEntity::find_by_id(
                write.settlement_chain_submission_id,
            )
            .one(&txn)
            .await
            .map_err(StorageError::from)?;
            if submission.is_none_or(|submission| {
                submission.state != SettlementSubmissionState::Confirmed
                    || submission.receipt_evidence_json.as_ref()
                        != Some(&SettlementChainReceiptEvidence::Redeem(Box::new(
                            write.receipt_evidence_json.clone(),
                        )))
            }) {
                return Err(error::state_conflict(
                    QUANT_SETTLEMENT_REDEEM,
                    Some(write.settlement_redeem_id),
                    "confirmed case does not match the requested submission evidence",
                ));
            }
            let assembled = assemble_one_redeem(&txn, redeem).await?;
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(assembled);
        }
        require_live_claim(&redeem, &write.owner, write.confirmed_at)?;
        if !matches!(
            redeem.state,
            SettlementCaseState::Submitted | SettlementCaseState::ReconciliationRequired
        ) {
            return Err(error::illegal_transition(
                QUANT_SETTLEMENT_REDEEM,
                Some(write.settlement_redeem_id),
                format!("{:?}", redeem.state),
                format!("{:?}", SettlementCaseState::Confirmed),
            ));
        }
        let submission =
            QuantSettlementChainSubmissionEntity::find_by_id(write.settlement_chain_submission_id)
                .lock_exclusive()
                .one(&txn)
                .await
                .map_err(StorageError::from)?
                .ok_or_else(|| {
                    error::not_found(
                        QUANT_SETTLEMENT_CHAIN_SUBMISSION,
                        write.settlement_chain_submission_id,
                    )
                })?;
        if submission.settlement_redeem_id != Some(write.settlement_redeem_id)
            || submission.purpose != SettlementSubmissionPurpose::Redeem
            || submission.state != SettlementSubmissionState::AwaitingFinality
            || submission.transaction_hash.as_ref()
                != Some(&write.receipt_evidence_json.transaction_hash)
        {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_CHAIN_SUBMISSION,
                Some(write.settlement_chain_submission_id),
                "settlement confirmation submission/case/state/hash mismatch",
            ));
        }

        let mut submission_active = submission.into_active_model();
        submission_active.state = ActiveValue::Set(SettlementSubmissionState::Confirmed);
        submission_active.failure_code = ActiveValue::Set(None);
        submission_active.receipt_evidence_json = ActiveValue::Set(Some(
            SettlementChainReceiptEvidence::Redeem(Box::new(write.receipt_evidence_json)),
        ));
        submission_active.last_error = ActiveValue::Set(None);
        submission_active.confirmed_at = ActiveValue::Set(Some(write.confirmed_at));
        submission_active
            .update(&txn)
            .await
            .map_err(StorageError::from)?;

        let mut redeem_active = redeem.into_active_model();
        redeem_active.state = ActiveValue::Set(SettlementCaseState::Confirmed);
        redeem_active.balance_after_json = ActiveValue::Set(Some(write.balance_after_json));
        redeem_active.actual_payout_usd = ActiveValue::Set(Some(write.actual_payout_usd));
        redeem_active.gas_fee_pol = ActiveValue::Set(write.gas_fee_pol);
        redeem_active.confirmed_at = ActiveValue::Set(Some(write.confirmed_at));
        redeem_active.reconciliation_state =
            ActiveValue::Set(SettlementReconciliationState::Reconciled);
        redeem_active.failure_code = ActiveValue::Set(None);
        redeem_active.next_attempt_at = ActiveValue::Set(None);
        redeem_active.claim_owner = ActiveValue::Set(None);
        redeem_active.lease_expires_at = ActiveValue::Set(None);
        redeem_active.last_error = ActiveValue::Set(None);
        let confirmed = redeem_active
            .update(&txn)
            .await
            .map_err(StorageError::from)?;

        for lot_write in write.lots {
            let intent_id = lot_write.lot.order_intent_id;
            QuantSettlementRedeemLotEntity::insert(lot_write.lot.into_active_model())
                .exec(&txn)
                .await
                .map_err(StorageError::from)?;
            position::apply_exit(&txn, &intent_id, lot_write.position_exit).await?;
            complete_exit_capital(&txn, &intent_id, "resolution redeem".to_owned()).await?;
            mark_intent_redeemed(&txn, &intent_id).await?;
        }

        QuantDomainEventOutboxEntity::insert(QuantDomainEventOutboxActiveModel {
            event_id: ActiveValue::Set(write.outbox_event.id),
            envelope_json: ActiveValue::Set(write.outbox_event),
            published_at: ActiveValue::Set(None),
            publish_attempts: ActiveValue::Set(0),
            claim_owner: ActiveValue::Set(None),
            lease_expires_at: ActiveValue::Set(None),
            last_error: ActiveValue::Set(None),
            ..Default::default()
        })
        .on_conflict(
            OnConflict::column(QuantDomainEventOutboxColumn::EventId)
                .do_nothing()
                .to_owned(),
        )
        .exec(&txn)
        .await
        .map_err(StorageError::from)?;

        let assembled = assemble_one_redeem(&txn, confirmed).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(assembled)
    }

    async fn require_reconciliation(
        &self,
        write: RequireSettlementReconciliation,
    ) -> Result<SettlementRedeemInfo, StorageError> {
        if write.detail.trim().is_empty() {
            return Err(error::invariant_violation(
                Some(QUANT_SETTLEMENT_REDEEM),
                "settlement reconciliation detail must not be empty",
            ));
        }
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let redeem = lock_case(&txn, write.settlement_redeem_id).await?;
        require_live_claim(&redeem, &write.owner, write.observed_at)?;
        let submission =
            QuantSettlementChainSubmissionEntity::find_by_id(write.settlement_chain_submission_id)
                .lock_exclusive()
                .one(&txn)
                .await
                .map_err(StorageError::from)?
                .ok_or_else(|| {
                    error::not_found(
                        QUANT_SETTLEMENT_CHAIN_SUBMISSION,
                        write.settlement_chain_submission_id,
                    )
                })?;
        if redeem.state == SettlementCaseState::Confirmed
            || submission.settlement_redeem_id != Some(write.settlement_redeem_id)
            || !ACTIVE_SUBMISSION_STATES.contains(&submission.state)
        {
            return Err(error::state_conflict(
                QUANT_SETTLEMENT_REDEEM,
                Some(write.settlement_redeem_id),
                "reconciliation evidence does not match an active durable submission",
            ));
        }
        let mut submission_active = submission.into_active_model();
        let mut history = submission_active
            .failure_history_json
            .take()
            .unwrap_or_default();
        history.entries.push(SettlementFailureEvidence {
            code: write.failure_code,
            detail: write.detail.clone(),
            observed_at: write.observed_at,
        });
        submission_active.failure_code = ActiveValue::Set(Some(write.failure_code));
        submission_active.failure_history_json = ActiveValue::Set(history);
        submission_active.last_error = ActiveValue::Set(Some(write.detail.clone()));
        submission_active.state = ActiveValue::Set(SettlementSubmissionState::Failed);
        submission_active.updated_at = ActiveValue::Set(write.observed_at);
        submission_active
            .update(&txn)
            .await
            .map_err(StorageError::from)?;

        let mut redeem_active = redeem.into_active_model();
        redeem_active.state = ActiveValue::Set(SettlementCaseState::ReconciliationRequired);
        redeem_active.reconciliation_state =
            ActiveValue::Set(SettlementReconciliationState::EvidenceMismatch);
        redeem_active.failure_code = ActiveValue::Set(Some(write.failure_code));
        redeem_active.next_attempt_at = ActiveValue::Set(None);
        redeem_active.last_error = ActiveValue::Set(Some(write.detail));
        redeem_active.claim_owner = ActiveValue::Set(None);
        redeem_active.lease_expires_at = ActiveValue::Set(None);
        let updated = redeem_active
            .update(&txn)
            .await
            .map_err(StorageError::from)?;
        let assembled = assemble_one_redeem(&txn, updated).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(assembled)
    }
}

#[derive(Debug, FromQueryResult)]
struct LotCountRow {
    settlement_redeem_id: SettlementRedeemId,
    inventory_lot_count: i64,
}

/// Count contributors for each case's exact current immutable inventory.
async fn inventory_lot_counts_for(
    db: &DatabaseConnection,
    scopes: impl Iterator<Item = (SettlementRedeemId, ContentHash)>,
) -> Result<HashMap<SettlementRedeemId, i64>, StorageError> {
    let scopes: Vec<(SettlementRedeemId, ContentHash)> = scopes.collect();
    if scopes.is_empty() {
        return Ok(HashMap::new());
    }
    let current_inventory = scopes.into_iter().fold(
        Condition::any(),
        |condition, (settlement_redeem_id, inventory_digest)| {
            condition.add(
                Condition::all()
                    .add(
                        QuantSettlementInventoryLotColumn::SettlementRedeemId
                            .eq(settlement_redeem_id),
                    )
                    .add(QuantSettlementInventoryLotColumn::InventoryDigest.eq(inventory_digest)),
            )
        },
    );
    let rows = QuantSettlementInventoryLotEntity::find()
        .select_only()
        .column(QuantSettlementInventoryLotColumn::SettlementRedeemId)
        .column_as(
            Expr::col(QuantSettlementInventoryLotColumn::SettlementInventoryLotId).count(),
            "inventory_lot_count",
        )
        .filter(current_inventory)
        .group_by(QuantSettlementInventoryLotColumn::SettlementRedeemId)
        .into_model::<LotCountRow>()
        .all(db)
        .await
        .map_err(StorageError::from)?;
    Ok(rows
        .into_iter()
        .map(|row| (row.settlement_redeem_id, row.inventory_lot_count))
        .collect())
}

async fn mark_intent_redeemed(
    db: &impl ConnectionTrait,
    intent_id: &OrderIntentId,
) -> Result<(), StorageError> {
    let intent = QuantOrderIntentEntity::find_by_id(*intent_id)
        .lock_exclusive()
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| error::not_found(QUANT_ORDER_INTENT, intent_id))?;
    let mut active = intent.into_active_model();
    active.exit_state = ActiveValue::Set(ExitState::Exited);
    active.exit_reason = ActiveValue::Set(Some(ExitReason::ResolutionRedeem));
    let mut scale_out_state = active.scale_out_state.take().unwrap_or_default();
    scale_out_state.pending_target = None;
    active.scale_out_state = ActiveValue::Set(scale_out_state);
    active
        .update(db)
        .await
        .map_err(StorageError::from)
        .map(|_| ())
}

fn page_condition(query: &SettlementRedeemListQuery) -> Condition {
    Condition::all()
        .add_option(query.state.map(|state| Column::State.eq(state)))
        .add_option(
            query
                .market_id
                .clone()
                .map(|market_id| Column::MarketId.eq(market_id)),
        )
        .add_option(query.from.map(|from| Column::CreatedAt.gte(from)))
        .add_option(query.to.map(|to| Column::CreatedAt.lte(to)))
}
