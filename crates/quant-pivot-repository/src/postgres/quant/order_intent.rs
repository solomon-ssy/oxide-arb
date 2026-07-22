//! Postgres-backed order intent repository.
//!
//! Every money-moving mutation is atomic over the intent FSM
//! (`quant_order_intent`) and the capital FSM (`quant_capital_allocation`) in one
//! transaction: an intent and its reservation are written, narrowed, or released
//! together or not at all. Background-origin terminal transitions (`expire` /
//! `invalidate`) also write their `operation_log` row inside the same
//! transaction so the audit can never drift from the money state.

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{
    StorageError,
    entity::{QUANT_CAPITAL_ALLOCATION, QUANT_ORDER_INTENT, QUANT_RECOMMENDATION},
};
use quant_pivot_models::{
    domain::{
        api::OrderIntentListQuery,
        governance::NewOperationLog,
        pagination::{PageWindow, Paginated},
        quant::{
            ApproveOrderIntent, ApproveOrderIntentOutcome, IntentCreationLimits,
            NewCapitalAllocation, NewOrderIntent, OrderIntentInfo, RecommendationInfo,
            RecommendationReportInfo, evaluate_intent_approval_invalidation,
        },
    },
    entities::{
        decision_policy_snapshot::Entity as DecisionPolicySnapshotEntity,
        operation_log::Entity as OperationLogEntity,
        policy_activation::{
            Column as PolicyActivationColumn, Relation as PolicyActivationRelation,
        },
        quant_capital_allocation::Entity as QuantCapitalAllocationEntity,
        quant_entry_condition_instance::Entity as QuantEntryConditionInstanceEntity,
        quant_order_intent::{Column, Entity as QuantOrderIntentEntity, Model, Relation},
        quant_recommendation::{
            Column as QuantRecommendationColumn, Entity, Model as QuantRecommendationModel,
        },
        quant_recommendation_report::Entity as QuantRecommendationReportEntity,
        system_kill_switch::Entity as SystemKillSwitchEntity,
    },
    enums::{
        execution::{ApprovalInvalidation, CapitalAllocationState},
        operation_log::OperationCategory,
        quant::{ApprovalStatus, OrderIntentStatus, QuantRuntimeMode, RecommendationStatus},
        rbac::ResourceType,
    },
    types::{
        DecisionPolicySnapshotId, EntryOrderSpec, OperationDetailDocument, OperationLogId,
        OrderIntentId, RecommendationId, RecommendationReportId, ScaleOutState, Usd,
    },
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, ConnectionTrait, DatabaseConnection,
    EntityTrait, IntoActiveModel, JoinType, PaginatorTrait, QueryFilter, QueryOrder, QuerySelect,
    RelationTrait, TransactionTrait,
};

use crate::{
    postgres::{
        error,
        quant::{
            capital_allocation::{
                capital_invariant_ok, load_capital, release_capital, validate_non_negative,
            },
            entry_condition::invalidate_for_intent_terminal,
        },
        query::{find_models_by_id_chunks, paginate_mapped},
        state_hash,
    },
    traits::OrderIntentRepository,
};

/// Statuses a TTL sweep may expire.
const EXPIRABLE_STATUSES: [OrderIntentStatus; 3] = [
    OrderIntentStatus::PendingApproval,
    OrderIntentStatus::Approved,
    OrderIntentStatus::ApprovedByPolicy,
];

/// Singleton row id for `system_kill_switch`.
const SYSTEM_KILL_SWITCH_ID: i32 = 1;
const ENTRY_CONDITION_ENTITY: &str = "quant_entry_condition_instance";

/// Postgres-backed order intent repository.
pub struct PgOrderIntentRepository {
    db: DatabaseConnection,
}

impl PgOrderIntentRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl OrderIntentRepository for PgOrderIntentRepository {
    async fn create_with_allocation(
        &self,
        intent: NewOrderIntent,
        allocation: NewCapitalAllocation,
        limits: Option<IntentCreationLimits>,
    ) -> Result<OrderIntentInfo, StorageError> {
        validate_new_intent_and_allocation(&intent, &allocation)?;
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let rec_row = Entity::find_by_id(intent.recommendation_id)
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| error::not_found(QUANT_RECOMMENDATION, intent.recommendation_id))?;
        if rec_row.research_profile_artifact_id != intent.research_profile_artifact_id {
            return Err(error::invariant_violation(
                Some(QUANT_ORDER_INTENT),
                "order intent profile must equal its recommendation profile artifact",
            ));
        }
        if !rec_row.status.allows_new_intent() {
            return Err(error::state_conflict(
                QUANT_RECOMMENDATION,
                Some(&intent.recommendation_id),
                format!(
                    "recommendation is {} (not actionable for intent creation)",
                    rec_row.status.as_str()
                ),
            ));
        }
        lock_kill_switch_for_entry(&txn).await?;
        if let Some(limits) = limits.as_ref() {
            enforce_creation_limits(&txn, &intent, &allocation, &rec_row, limits).await?;
        }
        if find_blocking_intent_for_recommendation(&txn, &intent.recommendation_id)
            .await?
            .is_some()
        {
            return Err(error::duplicate(
                QUANT_ORDER_INTENT,
                intent.recommendation_id,
            ));
        }
        let condition = QuantEntryConditionInstanceEntity::find_by_id(intent.condition_instance_id)
            .lock_shared()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::NotFound {
                entity: "quant_entry_condition_instance",
                id: intent.condition_instance_id.to_string(),
            })?;
        if condition.recommendation_id != intent.recommendation_id {
            return Err(error::invariant_violation(
                Some(QUANT_ORDER_INTENT),
                "order intent must reference its recommendation's condition instance",
            ));
        }
        if rec_row.status == RecommendationStatus::Published {
            let mut rec_active = rec_row.into_active_model();
            rec_active.status = ActiveValue::Set(RecommendationStatus::IntentCreated);
            rec_active.update(&txn).await.map_err(StorageError::from)?;
        }
        let mut intent_active = intent.into_active_model();
        intent_active.scale_out_state = ActiveValue::Set(ScaleOutState::default());
        let intent_model = QuantOrderIntentEntity::insert(intent_active)
            .exec_with_returning(&txn)
            .await
            .map_err(StorageError::from)?;
        QuantCapitalAllocationEntity::insert(allocation.into_active_model())
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(intent_model.into())
    }

    async fn approve(
        &self,
        intent_id: &OrderIntentId,
        approval: ApproveOrderIntent,
        entry_override: Option<EntryOrderSpec>,
        allocated_override: Option<Usd>,
        now: DateTime<Utc>,
    ) -> Result<ApproveOrderIntentOutcome, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let row = lock_terminal_graph(&txn, intent_id).await?;
        if row.status != OrderIntentStatus::PendingApproval {
            return Err(error::state_conflict(
                QUANT_ORDER_INTENT,
                Some(intent_id),
                format!("cannot approve intent from status {}", row.status.as_str()),
            ));
        }

        let (rec, report) = load_recommendation_with_report(&txn, &row.recommendation_id).await?;
        let active_policy_snapshot_id = load_current_policy_snapshot_id(&txn).await?;
        let kill_switch_allows_entry = load_kill_switch_allows_entry(&txn).await?;

        let invalidation = if row.expires_at <= now {
            Some(ApprovalInvalidation::IntentExpired)
        } else {
            match active_policy_snapshot_id.as_ref() {
                Some(active_snapshot_id) => evaluate_intent_approval_invalidation(
                    &rec,
                    &report,
                    kill_switch_allows_entry,
                    active_snapshot_id,
                    &row.decision_policy_snapshot_id,
                    &row.risk_envelope_hash,
                    now,
                ),
                None => Some(ApprovalInvalidation::RuntimeConfigChanged),
            }
        };

        if let Some(reason) = invalidation {
            let intent_model =
                transition_invalidated(&txn, intent_id, row, reason, now, false).await?;
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(ApproveOrderIntentOutcome::Invalidated(
                intent_model.into(),
                reason,
            ));
        }

        let intent_model = apply_approval(
            &txn,
            intent_id,
            row,
            approval,
            entry_override,
            allocated_override,
        )
        .await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(ApproveOrderIntentOutcome::Approved(intent_model.into()))
    }

    async fn reject(
        &self,
        intent_id: &OrderIntentId,
        reason: String,
        rejected_at: DateTime<Utc>,
        operation_log: NewOperationLog,
    ) -> Result<OrderIntentInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let row = lock_terminal_graph(&txn, intent_id).await?;
        validate_intent_transition(row.status, OrderIntentStatus::Rejected, intent_id)?;
        let before_info: OrderIntentInfo = row.clone().into();
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(OrderIntentStatus::Rejected);
        active.approval_status = ActiveValue::Set(ApprovalStatus::Rejected);
        active.status_reason = ActiveValue::Set(Some(reason.clone()));
        let intent_model = active.update(&txn).await.map_err(StorageError::from)?;
        invalidate_for_intent_terminal(
            &txn,
            &intent_model.condition_instance_id,
            intent_id,
            format!("intent rejected: {reason}"),
            rejected_at,
        )
        .await?;
        release_capital(&txn, intent_id, format!("rejected: {reason}")).await?;
        insert_terminal_operation_log(&txn, operation_log, &before_info, &intent_model).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(intent_model.into())
    }

    async fn cancel(
        &self,
        intent_id: &OrderIntentId,
        reason: String,
        cancelled_at: DateTime<Utc>,
        operation_log: NewOperationLog,
    ) -> Result<OrderIntentInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let row = lock_terminal_graph(&txn, intent_id).await?;
        validate_intent_transition(row.status, OrderIntentStatus::Cancelled, intent_id)?;
        let before_info: OrderIntentInfo = row.clone().into();
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(OrderIntentStatus::Cancelled);
        active.status_reason = ActiveValue::Set(Some(reason.clone()));
        let intent_model = active.update(&txn).await.map_err(StorageError::from)?;
        invalidate_for_intent_terminal(
            &txn,
            &intent_model.condition_instance_id,
            intent_id,
            format!("intent cancelled: {reason}"),
            cancelled_at,
        )
        .await?;
        release_capital(&txn, intent_id, format!("cancelled: {reason}")).await?;
        insert_terminal_operation_log(&txn, operation_log, &before_info, &intent_model).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(intent_model.into())
    }

    async fn expire(
        &self,
        intent_id: &OrderIntentId,
        expired_at: DateTime<Utc>,
        operation_log: NewOperationLog,
    ) -> Result<OrderIntentInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let row = lock_terminal_graph(&txn, intent_id).await?;
        if row.status == OrderIntentStatus::Expired {
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(row.into());
        }
        validate_intent_transition(row.status, OrderIntentStatus::Expired, intent_id)?;
        let before_info: OrderIntentInfo = row.clone().into();
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(OrderIntentStatus::Expired);
        active.status_reason = ActiveValue::Set(Some("intent expired".to_owned()));
        let intent_model = active.update(&txn).await.map_err(StorageError::from)?;
        invalidate_for_intent_terminal(
            &txn,
            &intent_model.condition_instance_id,
            intent_id,
            "intent expired".to_owned(),
            expired_at,
        )
        .await?;
        release_capital(&txn, intent_id, "expired".to_owned()).await?;
        let after_info: OrderIntentInfo = intent_model.clone().into();
        let operation_log =
            state_hash::apply_transition_hashes(operation_log, &before_info, &after_info)?;
        OperationLogEntity::insert(operation_log.into_active_model())
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(intent_model.into())
    }

    async fn invalidate(
        &self,
        intent_id: &OrderIntentId,
        reason: ApprovalInvalidation,
        invalidated_at: DateTime<Utc>,
        operation_log: NewOperationLog,
    ) -> Result<OrderIntentInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let row = lock_terminal_graph(&txn, intent_id).await?;
        let before_info: OrderIntentInfo = row.clone().into();
        let intent_model =
            transition_invalidated(&txn, intent_id, row, reason, invalidated_at, true).await?;
        let after_info: OrderIntentInfo = intent_model.clone().into();
        let operation_log =
            state_hash::apply_transition_hashes(operation_log, &before_info, &after_info)?;
        OperationLogEntity::insert(operation_log.into_active_model())
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(intent_model.into())
    }

    async fn find_by_id(
        &self,
        intent_id: &OrderIntentId,
    ) -> Result<Option<OrderIntentInfo>, StorageError> {
        QuantOrderIntentEntity::find_by_id(*intent_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn find_by_ids(
        &self,
        intent_ids: &[OrderIntentId],
    ) -> Result<Vec<OrderIntentInfo>, StorageError> {
        find_models_by_id_chunks::<QuantOrderIntentEntity, _, _>(
            &self.db,
            intent_ids,
            Column::OrderIntentId,
        )
        .await
        .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn page(
        &self,
        query: OrderIntentListQuery,
    ) -> Result<Paginated<OrderIntentInfo>, StorageError> {
        paginate_mapped(
            QuantOrderIntentEntity::find()
                .filter(page_condition(&query))
                .order_by_desc(Column::CreatedAt),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }

    async fn find_expired(&self, now: DateTime<Utc>) -> Result<Vec<OrderIntentInfo>, StorageError> {
        QuantOrderIntentEntity::find()
            .filter(Column::ExpiresAt.lte(now))
            .filter(Column::Status.is_in(EXPIRABLE_STATUSES))
            .order_by_asc(Column::ExpiresAt)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn upcoming_expirations(
        &self,
        before: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<(OrderIntentId, DateTime<Utc>)>, StorageError> {
        QuantOrderIntentEntity::find()
            .filter(Column::ExpiresAt.lte(before))
            .filter(Column::Status.is_in(EXPIRABLE_STATUSES))
            .order_by_asc(Column::ExpiresAt)
            .limit(limit)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| {
                rows.into_iter()
                    .map(|row| (row.order_intent_id, row.expires_at))
                    .collect()
            })
    }

    async fn find_active_by_recommendation(
        &self,
        recommendation_id: &RecommendationId,
    ) -> Result<Option<OrderIntentInfo>, StorageError> {
        find_blocking_intent_for_recommendation(&self.db, recommendation_id)
            .await
            .map(|row| row.map(Into::into))
    }

    async fn find_active_intents_by_recommendation(
        &self,
        recommendation_id: &RecommendationId,
    ) -> Result<Vec<OrderIntentInfo>, StorageError> {
        QuantOrderIntentEntity::find()
            .filter(Column::RecommendationId.eq(*recommendation_id))
            .filter(Column::Status.is_in(OrderIntentStatus::PRE_SUBMISSION_ACTIVE))
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn find_active_by_report(
        &self,
        report_id: &RecommendationReportId,
    ) -> Result<Vec<OrderIntentInfo>, StorageError> {
        intents_for_report(
            &self.db,
            report_id,
            OrderIntentStatus::PRE_SUBMISSION_ACTIVE,
        )
        .await
    }

    async fn find_blocking_by_report(
        &self,
        report_id: &RecommendationReportId,
    ) -> Result<Vec<OrderIntentInfo>, StorageError> {
        intents_for_report(
            &self.db,
            report_id,
            OrderIntentStatus::SIBLING_INTENT_BLOCKING,
        )
        .await
    }

    async fn count_open(&self) -> Result<u64, StorageError> {
        QuantOrderIntentEntity::find()
            .filter(Column::Status.is_in(OrderIntentStatus::OPEN))
            .count(&self.db)
            .await
            .map_err(StorageError::from)
    }

    async fn find_attribution_candidates(
        &self,
        statuses: Vec<OrderIntentStatus>,
        limit: u64,
    ) -> Result<Vec<OrderIntentInfo>, StorageError> {
        if statuses.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let eligible_state = Condition::any()
            .add(Column::Status.is_in(OrderIntentStatus::UNFILLED_TERMINAL))
            .add(Column::Status.is_in(OrderIntentStatus::FILLED_TERMINAL));
        // Inner-join recommendation so orphaned intents never enter the sweep.
        // Position is intentionally not joined: eligibility is status-driven and
        // the builder re-loads the lot (if any) before writing WORM attribution.
        QuantOrderIntentEntity::find()
            .join(JoinType::InnerJoin, Relation::Recommendation.def())
            .filter(Column::Status.is_in(statuses))
            .filter(eligible_state)
            .order_by_asc(Column::UpdatedAt)
            .limit(limit)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }
}

fn validate_new_intent_and_allocation(
    intent: &NewOrderIntent,
    allocation: &NewCapitalAllocation,
) -> Result<(), StorageError> {
    if !matches!(
        intent.status,
        OrderIntentStatus::PendingApproval | OrderIntentStatus::ApprovedByPolicy
    ) {
        return Err(error::invariant_violation(
            Some(QUANT_ORDER_INTENT),
            format!(
                "order intent must be created as pending_approval or approved_by_policy, got {}",
                intent.status.as_str()
            ),
        ));
    }
    if allocation.order_intent_id != intent.order_intent_id {
        return Err(error::invariant_violation(
            Some(QUANT_CAPITAL_ALLOCATION),
            "capital allocation must reference its own order intent",
        ));
    }
    if allocation.state != CapitalAllocationState::Allocated {
        return Err(error::invariant_violation(
            Some(QUANT_CAPITAL_ALLOCATION),
            format!(
                "new capital allocation must start as allocated, got {}",
                allocation.state.as_str()
            ),
        ));
    }
    validate_non_negative(
        allocation.allocated_usd,
        allocation.locked_usd,
        allocation.spent_usd,
        allocation.released_usd,
    )?;
    if !capital_invariant_ok(
        allocation.planned_usd,
        allocation.allocated_usd,
        allocation.locked_usd,
        allocation.spent_usd,
        allocation.released_usd,
    ) {
        return Err(error::invariant_violation(
            Some(QUANT_CAPITAL_ALLOCATION),
            "capital allocation violates the reserve invariant on create",
        ));
    }
    Ok(())
}

fn page_condition(query: &OrderIntentListQuery) -> Condition {
    // A multi-status queue preset (`statuses`) supersedes the single `status`.
    let status_filter = match query.statuses.as_deref() {
        Some(statuses) if !statuses.is_empty() => {
            Some(Column::Status.is_in(statuses.iter().copied()))
        }
        _ => query.status.map(|status| Column::Status.eq(status)),
    };
    Condition::all()
        .add_option(status_filter)
        .add_option(
            query
                .approval_status
                .map(|approval| Column::ApprovalStatus.eq(approval)),
        )
        .add_option(query.runtime_mode.map(|mode| Column::RuntimeMode.eq(mode)))
        .add_option(
            query
                .recommendation_id
                .map(|id| Column::RecommendationId.eq(id)),
        )
        .add_option(query.from.map(|from| Column::CreatedAt.gte(from)))
        .add_option(query.to.map(|to| Column::CreatedAt.lt(to)))
}

pub async fn load_intent_for_update(
    db: &impl ConnectionTrait,
    intent_id: &OrderIntentId,
) -> Result<Model, StorageError> {
    QuantOrderIntentEntity::find_by_id(*intent_id)
        .lock_exclusive()
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| StorageError::NotFound {
            entity: "order_intent",
            id: intent_id.to_string(),
        })
}

/// Load recommendation + owning report in one round-trip (`find_also_related`).
/// Fail-closed when either side is missing.
async fn load_recommendation_with_report(
    db: &impl ConnectionTrait,
    recommendation_id: &RecommendationId,
) -> Result<(RecommendationInfo, RecommendationReportInfo), StorageError> {
    let (rec, report) = Entity::find_by_id(*recommendation_id)
        .find_also_related(QuantRecommendationReportEntity)
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| StorageError::NotFound {
            entity: "recommendation",
            id: recommendation_id.to_string(),
        })?;
    let report = report.ok_or_else(|| StorageError::NotFound {
        entity: "recommendation_report",
        id: rec.recommendation_report_id.to_string(),
    })?;
    Ok((rec.into(), report.into()))
}

async fn load_current_policy_snapshot_id(
    db: &impl ConnectionTrait,
) -> Result<Option<DecisionPolicySnapshotId>, StorageError> {
    DecisionPolicySnapshotEntity::find()
        .join_rev(
            JoinType::InnerJoin,
            PolicyActivationRelation::Snapshot.def(),
        )
        .order_by_desc(PolicyActivationColumn::ActivatedAt)
        .one(db)
        .await
        .map_err(StorageError::from)
        .map(|row| row.map(|version| version.decision_policy_snapshot_id))
}

async fn load_kill_switch_allows_entry(db: &impl ConnectionTrait) -> Result<bool, StorageError> {
    Ok(SystemKillSwitchEntity::find_by_id(SYSTEM_KILL_SWITCH_ID)
        .lock_shared()
        .one(db)
        .await
        .map_err(StorageError::from)?
        .is_some_and(|row| row.state.allows_new_entry()))
}

async fn lock_kill_switch_for_entry(db: &impl ConnectionTrait) -> Result<(), StorageError> {
    let row = SystemKillSwitchEntity::find_by_id(SYSTEM_KILL_SWITCH_ID)
        .lock_exclusive()
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| {
            error::state_conflict(
                "system_kill_switch",
                Some(&SYSTEM_KILL_SWITCH_ID),
                "kill-switch singleton is missing; new entry fails closed",
            )
        })?;
    if !row.state.allows_new_entry() {
        return Err(error::state_conflict(
            "system_kill_switch",
            Some(&SYSTEM_KILL_SWITCH_ID),
            format!(
                "kill switch is {}; new entry is blocked",
                row.state.as_str()
            ),
        ));
    }
    Ok(())
}

async fn enforce_creation_limits(
    db: &impl ConnectionTrait,
    intent: &NewOrderIntent,
    allocation: &NewCapitalAllocation,
    recommendation: &QuantRecommendationModel,
    limits: &IntentCreationLimits,
) -> Result<(), StorageError> {
    if intent.runtime_mode != QuantRuntimeMode::SemiAuto
        || limits.max_open_intents == 0
        || !limits.max_total_cash_per_report.is_positive()
        || recommendation.recommendation_report_id != limits.recommendation_report_id
    {
        return Err(error::invariant_violation(
            Some(QUANT_ORDER_INTENT),
            "SemiAuto intent creation limits are invalid or bound to another report",
        ));
    }
    let open = QuantOrderIntentEntity::find()
        .filter(Column::Status.is_in(OrderIntentStatus::OPEN))
        .count(db)
        .await
        .map_err(StorageError::from)?;
    if open >= u64::from(limits.max_open_intents) {
        return Err(error::state_conflict(
            QUANT_ORDER_INTENT,
            None::<&OrderIntentId>,
            format!(
                "SemiAuto canary open-intent cap {} is exhausted",
                limits.max_open_intents
            ),
        ));
    }
    let existing = intents_for_report_all(db, &limits.recommendation_report_id).await?;
    let total = existing
        .iter()
        .map(|row| row.entry_order_json.notional().inner())
        .try_fold(allocation.planned_usd.inner(), |sum, value| {
            sum.checked_add(value).ok_or_else(|| {
                error::invariant_violation(
                    Some(QUANT_ORDER_INTENT),
                    "SemiAuto canary report notional sum overflowed Decimal",
                )
            })
        })?;
    if total > limits.max_total_cash_per_report.inner() {
        return Err(error::state_conflict(
            QUANT_ORDER_INTENT,
            None::<&OrderIntentId>,
            format!(
                "SemiAuto canary report total {total} exceeds {}",
                limits.max_total_cash_per_report
            ),
        ));
    }
    Ok(())
}

async fn intents_for_report_all(
    db: &impl ConnectionTrait,
    report_id: &RecommendationReportId,
) -> Result<Vec<OrderIntentInfo>, StorageError> {
    QuantOrderIntentEntity::find()
        .join(JoinType::InnerJoin, Relation::Recommendation.def())
        .filter(QuantRecommendationColumn::RecommendationReportId.eq(*report_id))
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|rows| rows.into_iter().map(Into::into).collect())
}

async fn intents_for_report<const N: usize>(
    db: &impl ConnectionTrait,
    report_id: &RecommendationReportId,
    statuses: [OrderIntentStatus; N],
) -> Result<Vec<OrderIntentInfo>, StorageError> {
    QuantOrderIntentEntity::find()
        .filter(Column::Status.is_in(statuses))
        .join(JoinType::InnerJoin, Relation::Recommendation.def())
        .filter(QuantRecommendationColumn::RecommendationReportId.eq(*report_id))
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|rows| rows.into_iter().map(Into::into).collect())
}

async fn find_blocking_intent_for_recommendation(
    db: &impl ConnectionTrait,
    recommendation_id: &RecommendationId,
) -> Result<Option<Model>, StorageError> {
    QuantOrderIntentEntity::find()
        .filter(Column::RecommendationId.eq(*recommendation_id))
        .filter(Column::Status.is_in(OrderIntentStatus::SIBLING_INTENT_BLOCKING))
        .one(db)
        .await
        .map_err(StorageError::from)
}

async fn apply_approval(
    db: &impl ConnectionTrait,
    intent_id: &OrderIntentId,
    row: Model,
    approval: ApproveOrderIntent,
    entry_override: Option<EntryOrderSpec>,
    allocated_override: Option<Usd>,
) -> Result<Model, StorageError> {
    let mut active = row.into_active_model();
    active.status = ActiveValue::Set(OrderIntentStatus::Approved);
    active.approval_status = ActiveValue::Set(ApprovalStatus::Approved);
    active.approved_by = ActiveValue::Set(Some(approval.approved_by));
    active.approval_reason = ActiveValue::Set(Some(approval.approval_reason));
    active.approved_at = ActiveValue::Set(Some(approval.approved_at));
    if let Some(entry) = entry_override {
        active.entry_order_json = ActiveValue::Set(entry);
    }
    let intent_model = active.update(db).await.map_err(StorageError::from)?;

    if let Some(new_allocated) = allocated_override {
        let cap = load_capital(db, intent_id).await?;
        if new_allocated > cap.allocated_usd {
            return Err(error::invariant_violation(
                Some(QUANT_CAPITAL_ALLOCATION),
                format!("approval cannot increase reserved capital for intent {intent_id}"),
            ));
        }
        validate_non_negative(
            new_allocated,
            cap.locked_usd,
            cap.spent_usd,
            cap.released_usd,
        )?;
        if !capital_invariant_ok(
            cap.planned_usd,
            new_allocated,
            cap.locked_usd,
            cap.spent_usd,
            cap.released_usd,
        ) {
            return Err(error::invariant_violation(
                Some(QUANT_CAPITAL_ALLOCATION),
                format!(
                    "downscaled allocation violates the reserve invariant for intent {intent_id}"
                ),
            ));
        }
        let mut cap_active = cap.into_active_model();
        cap_active.allocated_usd = ActiveValue::Set(new_allocated);
        cap_active.reason = ActiveValue::Set(format!("approved downscale to {new_allocated}"));
        cap_active.update(db).await.map_err(StorageError::from)?;
    }

    Ok(intent_model)
}

/// Invalidate an intent and release capital. When `validate_transition` is true
/// the FSM guard runs (background / cascade paths); approval-time invalidation
/// skips it because the row was already locked as `PendingApproval`.
async fn transition_invalidated(
    db: &impl ConnectionTrait,
    intent_id: &OrderIntentId,
    row: Model,
    reason: ApprovalInvalidation,
    occurred_at: DateTime<Utc>,
    validate_transition: bool,
) -> Result<Model, StorageError> {
    if validate_transition {
        validate_intent_transition(row.status, OrderIntentStatus::Invalidated, intent_id)?;
    }
    let mut active = row.into_active_model();
    active.status = ActiveValue::Set(OrderIntentStatus::Invalidated);
    active.status_reason = ActiveValue::Set(Some(reason.to_string()));
    let intent_model = active.update(db).await.map_err(StorageError::from)?;
    invalidate_for_intent_terminal(
        db,
        &intent_model.condition_instance_id,
        intent_id,
        format!("intent invalidated: {}", reason.as_str()),
        occurred_at,
    )
    .await?;
    release_capital(db, intent_id, format!("invalidated: {}", reason.as_str())).await?;
    Ok(intent_model)
}

async fn insert_terminal_operation_log(
    db: &impl ConnectionTrait,
    operation_log: NewOperationLog,
    before: &OrderIntentInfo,
    after: &Model,
) -> Result<(), StorageError> {
    let after_info: OrderIntentInfo = after.clone().into();
    let operation_log = state_hash::apply_transition_hashes(operation_log, before, &after_info)?;
    OperationLogEntity::insert(operation_log.into_active_model())
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    Ok(())
}

/// Atomically invalidate every pre-submission intent for an already locked
/// recommendation. The caller owns the transaction and must lock the parent
/// recommendation first. Intents are locked in primary-key order before their
/// condition and capital rows, preserving the global terminal-command order.
pub async fn invalidate_pre_submission_for_recommendation(
    db: &impl ConnectionTrait,
    recommendation_id: &RecommendationId,
    reason: ApprovalInvalidation,
    occurred_at: DateTime<Utc>,
    parent_log: &NewOperationLog,
) -> Result<Vec<OrderIntentInfo>, StorageError> {
    let rows = QuantOrderIntentEntity::find()
        .filter(Column::RecommendationId.eq(*recommendation_id))
        .filter(Column::Status.is_in(OrderIntentStatus::PRE_SUBMISSION_ACTIVE))
        .order_by_asc(Column::OrderIntentId)
        .lock_exclusive()
        .all(db)
        .await
        .map_err(StorageError::from)?;
    let mut invalidated = Vec::with_capacity(rows.len());
    for row in rows {
        let intent_id = row.order_intent_id;
        validate_intent_transition(row.status, OrderIntentStatus::Invalidated, &intent_id)?;
        let before_info: OrderIntentInfo = row.clone().into();
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(OrderIntentStatus::Invalidated);
        active.status_reason = ActiveValue::Set(Some(reason.to_string()));
        let model = active.update(db).await.map_err(StorageError::from)?;
        invalidate_for_intent_terminal(
            db,
            &model.condition_instance_id,
            &intent_id,
            format!("intent invalidated: {}", reason.as_str()),
            occurred_at,
        )
        .await?;
        release_capital(db, &intent_id, format!("invalidated: {}", reason.as_str())).await?;

        let after_info: OrderIntentInfo = model.clone().into();
        let intent_log = state_hash::apply_transition_hashes(
            terminal_intent_operation_log(parent_log, &intent_id, reason)?,
            &before_info,
            &after_info,
        )?;
        OperationLogEntity::insert(intent_log.into_active_model())
            .exec(db)
            .await
            .map_err(StorageError::from)?;
        invalidated.push(after_info);
    }
    Ok(invalidated)
}

fn terminal_intent_operation_log(
    parent: &NewOperationLog,
    intent_id: &OrderIntentId,
    reason: ApprovalInvalidation,
) -> Result<NewOperationLog, StorageError> {
    Ok(NewOperationLog {
        id: OperationLogId::from_v7(),
        request_id: format!("{}:intent:{intent_id}", parent.request_id).into(),
        actor_user_id: parent.actor_user_id,
        actor_username: parent.actor_username.clone(),
        acting_role: parent.acting_role.clone(),
        category: OperationCategory::Governance,
        action: "quant.intent.invalidate".into(),
        resource_type: Some(ResourceType::OrderIntent),
        resource_id: Some(intent_id.to_string()),
        http_method: parent.http_method,
        http_path: format!("/system/quant/intent/{intent_id}/invalidate"),
        http_status: parent.http_status,
        outcome: parent.outcome,
        client_ip: parent.client_ip,
        user_agent: parent.user_agent.clone(),
        latency_ms: parent.latency_ms,
        detail: OperationDetailDocument::from_serializable(&serde_json::json!({
            "reason": reason.as_str(),
            "parent_action": parent.action,
            "parent_resource_id": parent.resource_id,
        }))
        .map_err(|error| StorageError::Codec(error.to_string()))?,
        before_hash: None,
        after_hash: None,
        governance_audit_event_id: parent.governance_audit_event_id,
        governance_audit_sequence: parent.governance_audit_sequence,
    })
}

pub async fn lock_terminal_graph(
    db: &impl ConnectionTrait,
    intent_id: &OrderIntentId,
) -> Result<Model, StorageError> {
    let probe = load_intent(db, intent_id).await?;
    let recommendation = Entity::find_by_id(probe.recommendation_id)
        .lock_exclusive()
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| error::not_found(QUANT_RECOMMENDATION, probe.recommendation_id))?;
    let intent = load_intent_for_update(db, intent_id).await?;
    if recommendation.recommendation_id != intent.recommendation_id {
        return Err(error::state_conflict(
            QUANT_ORDER_INTENT,
            Some(intent_id),
            "intent recommendation changed while acquiring terminal graph locks",
        ));
    }
    let condition = QuantEntryConditionInstanceEntity::find_by_id(intent.condition_instance_id)
        .lock_exclusive()
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| error::not_found(ENTRY_CONDITION_ENTITY, intent.condition_instance_id))?;
    if condition.recommendation_id != recommendation.recommendation_id {
        return Err(error::invariant_violation(
            Some(ENTRY_CONDITION_ENTITY),
            "condition recommendation does not match terminal intent graph",
        ));
    }
    let _capital = load_capital(db, intent_id).await?;
    Ok(intent)
}

pub async fn load_intent(
    db: &impl ConnectionTrait,
    intent_id: &OrderIntentId,
) -> Result<Model, StorageError> {
    QuantOrderIntentEntity::find_by_id(*intent_id)
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| error::not_found(QUANT_ORDER_INTENT, intent_id))
}

pub fn validate_intent_transition(
    current: OrderIntentStatus,
    next: OrderIntentStatus,
    intent_id: &OrderIntentId,
) -> Result<(), StorageError> {
    let valid = matches!(
        (current, next),
        (
            OrderIntentStatus::Draft,
            OrderIntentStatus::PendingApproval | OrderIntentStatus::ApprovedByPolicy,
        ) | (
            OrderIntentStatus::PendingApproval,
            OrderIntentStatus::Approved
                | OrderIntentStatus::Rejected
                | OrderIntentStatus::Cancelled
                | OrderIntentStatus::Expired
                | OrderIntentStatus::Invalidated,
        ) | (
            OrderIntentStatus::Approved | OrderIntentStatus::ApprovedByPolicy,
            OrderIntentStatus::AdmissionPending
                | OrderIntentStatus::AdmissionRejected
                | OrderIntentStatus::Cancelled
                | OrderIntentStatus::Expired
                | OrderIntentStatus::Invalidated,
        ) | (
            // `AdmissionPending -> Approved/ApprovedByPolicy` releases the claim
            // on a transient admission defer so the dispatcher retries.
            OrderIntentStatus::AdmissionPending,
            OrderIntentStatus::Approved
                | OrderIntentStatus::ApprovedByPolicy
                | OrderIntentStatus::Submitted
                | OrderIntentStatus::AdmissionRejected
                | OrderIntentStatus::Invalidated,
        ) | (
            OrderIntentStatus::Submitted,
            OrderIntentStatus::PartiallyFilled
                | OrderIntentStatus::Filled
                | OrderIntentStatus::Failed
                | OrderIntentStatus::Cancelled,
        ) | (
            OrderIntentStatus::PartiallyFilled,
            OrderIntentStatus::Filled | OrderIntentStatus::Failed | OrderIntentStatus::Cancelled
        )
    );
    if valid {
        return Ok(());
    }
    Err(error::illegal_transition(
        QUANT_ORDER_INTENT,
        Some(intent_id),
        current,
        next,
    ))
}
