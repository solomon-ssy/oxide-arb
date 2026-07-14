//! Postgres-backed order intent repository.
//!
//! Every money-moving mutation is atomic over the intent FSM
//! (`quant_order_intent`) and the capital FSM (`quant_capital_allocation`) in one
//! transaction: an intent and its reservation are written, narrowed, or released
//! together or not at all. Background-origin terminal transitions (`expire` /
//! `invalidate`) also write their `operation_log` row inside the same
//! transaction so the audit can never drift from the money state.

use crate::{
    postgres::{
        error,
        quant::capital_allocation::{
            capital_invariant_ok, load_capital, release_capital, validate_non_negative,
        },
        quant::entry_condition::invalidate_for_intent_terminal,
        query::{find_models_by_id_chunks, paginate_mapped},
        state_hash,
    },
    traits::OrderIntentRepository,
};
use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{
        ApproveOrderIntent, ApproveOrderIntentOutcome, NewCapitalAllocation, NewOperationLog,
        NewOrderIntent, OrderIntentInfo, OrderIntentListQuery, PageWindow, Paginated,
        RecommendationInfo, RecommendationReportInfo, evaluate_intent_approval_invalidation,
    },
    entities::{
        operation_log, quant_capital_allocation, quant_entry_condition_instance,
        quant_order_intent, quant_recommendation, quant_recommendation_report,
        runtime_config_activation, runtime_config_version, system_kill_switch,
    },
    enums::{
        execution::{ApprovalInvalidation, CapitalAllocationState},
        operation_log::OperationCategory,
        quant::{ApprovalStatus, OrderIntentStatus, RecommendationStatus},
        rbac::ResourceType,
    },
    types::{
        EntryOrderSpec, OperationLogId, OrderIntentId, RecommendationId, RecommendationReportId,
        RuntimeConfigVersionId, ScaleOutState, Usd,
    },
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, ConnectionTrait, DatabaseConnection,
    EntityTrait, IntoActiveModel, JoinType, PaginatorTrait, QueryFilter, QueryOrder, QuerySelect,
    RelationTrait, TransactionTrait,
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
    ) -> Result<OrderIntentInfo, StorageError> {
        validate_new_intent_and_allocation(&intent, &allocation)?;
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let rec_row = quant_recommendation::Entity::find_by_id(intent.recommendation_id.clone())
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                error::not_found(entity::QUANT_RECOMMENDATION, &intent.recommendation_id)
            })?;
        if !rec_row.status.is_actionable_for_intent() {
            return Err(error::state_conflict(
                entity::QUANT_RECOMMENDATION,
                Some(&intent.recommendation_id),
                format!(
                    "recommendation is {} (not actionable for intent creation)",
                    rec_row.status.as_str()
                ),
            ));
        }
        if find_blocking_intent_for_recommendation(&txn, &intent.recommendation_id)
            .await?
            .is_some()
        {
            return Err(error::duplicate(
                entity::QUANT_ORDER_INTENT,
                intent.recommendation_id,
            ));
        }
        let condition = quant_entry_condition_instance::Entity::find_by_id(
            intent.condition_instance_id.clone(),
        )
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
                Some(entity::QUANT_ORDER_INTENT),
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
        let intent_model = quant_order_intent::Entity::insert(intent_active)
            .exec_with_returning(&txn)
            .await
            .map_err(StorageError::from)?;
        quant_capital_allocation::Entity::insert(allocation.into_active_model())
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
                entity::QUANT_ORDER_INTENT,
                Some(intent_id),
                format!("cannot approve intent from status {}", row.status.as_str()),
            ));
        }

        let (rec, report) = load_recommendation_with_report(&txn, &row.recommendation_id).await?;
        let active_config_version_id = load_current_config_version_id(&txn).await?;
        let kill_switch_allows_entry = load_kill_switch_allows_entry(&txn).await?;

        let invalidation = match active_config_version_id.as_ref() {
            Some(active_version_id) => evaluate_intent_approval_invalidation(
                &rec,
                &report,
                kill_switch_allows_entry,
                active_version_id,
                &row.runtime_config_version_id,
                &row.risk_envelope_hash,
                now,
            ),
            None => Some(ApprovalInvalidation::RuntimeConfigChanged),
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
        operation_log::Entity::insert(operation_log.into_active_model())
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
        operation_log::Entity::insert(operation_log.into_active_model())
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
        quant_order_intent::Entity::find_by_id(intent_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn find_by_ids(
        &self,
        intent_ids: &[OrderIntentId],
    ) -> Result<Vec<OrderIntentInfo>, StorageError> {
        find_models_by_id_chunks::<quant_order_intent::Entity, _, _>(
            &self.db,
            intent_ids,
            quant_order_intent::Column::OrderIntentId,
        )
        .await
        .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn page(
        &self,
        query: OrderIntentListQuery,
    ) -> Result<Paginated<OrderIntentInfo>, StorageError> {
        paginate_mapped(
            quant_order_intent::Entity::find()
                .filter(page_condition(&query))
                .order_by_desc(quant_order_intent::Column::CreatedAt),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }

    async fn find_expired(&self, now: DateTime<Utc>) -> Result<Vec<OrderIntentInfo>, StorageError> {
        quant_order_intent::Entity::find()
            .filter(quant_order_intent::Column::ExpiresAt.lte(now))
            .filter(quant_order_intent::Column::Status.is_in(EXPIRABLE_STATUSES))
            .order_by_asc(quant_order_intent::Column::ExpiresAt)
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
        quant_order_intent::Entity::find()
            .filter(quant_order_intent::Column::ExpiresAt.lte(before))
            .filter(quant_order_intent::Column::Status.is_in(EXPIRABLE_STATUSES))
            .order_by_asc(quant_order_intent::Column::ExpiresAt)
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
        quant_order_intent::Entity::find()
            .filter(quant_order_intent::Column::RecommendationId.eq(recommendation_id.clone()))
            .filter(
                quant_order_intent::Column::Status.is_in(OrderIntentStatus::PRE_SUBMISSION_ACTIVE),
            )
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
        quant_order_intent::Entity::find()
            .filter(quant_order_intent::Column::Status.is_in(OrderIntentStatus::OPEN))
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
            .add(quant_order_intent::Column::Status.is_in(OrderIntentStatus::UNFILLED_TERMINAL))
            .add(quant_order_intent::Column::Status.is_in(OrderIntentStatus::FILLED_TERMINAL));
        // Inner-join recommendation so orphaned intents never enter the sweep.
        // Position is intentionally not joined: eligibility is status-driven and
        // the builder re-loads the lot (if any) before writing WORM attribution.
        quant_order_intent::Entity::find()
            .join(
                JoinType::InnerJoin,
                quant_order_intent::Relation::Recommendation.def(),
            )
            .filter(quant_order_intent::Column::Status.is_in(statuses))
            .filter(eligible_state)
            .order_by_asc(quant_order_intent::Column::UpdatedAt)
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
            Some(entity::QUANT_ORDER_INTENT),
            format!(
                "order intent must be created as pending_approval or approved_by_policy, got {}",
                intent.status.as_str()
            ),
        ));
    }
    if allocation.order_intent_id != intent.order_intent_id {
        return Err(error::invariant_violation(
            Some(entity::QUANT_CAPITAL_ALLOCATION),
            "capital allocation must reference its own order intent",
        ));
    }
    if allocation.state != CapitalAllocationState::Allocated {
        return Err(error::invariant_violation(
            Some(entity::QUANT_CAPITAL_ALLOCATION),
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
            Some(entity::QUANT_CAPITAL_ALLOCATION),
            "capital allocation violates the reserve invariant on create",
        ));
    }
    Ok(())
}

fn page_condition(query: &OrderIntentListQuery) -> Condition {
    // A multi-status queue preset (`statuses`) supersedes the single `status`.
    let status_filter = match query.statuses.as_deref() {
        Some(statuses) if !statuses.is_empty() => {
            Some(quant_order_intent::Column::Status.is_in(statuses.iter().copied()))
        }
        _ => query
            .status
            .map(|status| quant_order_intent::Column::Status.eq(status)),
    };
    Condition::all()
        .add_option(status_filter)
        .add_option(
            query
                .approval_status
                .map(|approval| quant_order_intent::Column::ApprovalStatus.eq(approval)),
        )
        .add_option(
            query
                .runtime_mode
                .map(|mode| quant_order_intent::Column::RuntimeMode.eq(mode)),
        )
        .add_option(
            query
                .recommendation_id
                .clone()
                .map(|id| quant_order_intent::Column::RecommendationId.eq(id)),
        )
        .add_option(
            query
                .from
                .map(|from| quant_order_intent::Column::CreatedAt.gte(from)),
        )
        .add_option(
            query
                .to
                .map(|to| quant_order_intent::Column::CreatedAt.lt(to)),
        )
}

pub async fn load_intent_for_update(
    db: &impl ConnectionTrait,
    intent_id: &OrderIntentId,
) -> Result<quant_order_intent::Model, StorageError> {
    quant_order_intent::Entity::find_by_id(intent_id.clone())
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
    let (rec, report) = quant_recommendation::Entity::find_by_id(recommendation_id.clone())
        .find_also_related(quant_recommendation_report::Entity)
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

async fn load_current_config_version_id(
    db: &impl ConnectionTrait,
) -> Result<Option<RuntimeConfigVersionId>, StorageError> {
    runtime_config_version::Entity::find()
        .join_rev(
            JoinType::InnerJoin,
            runtime_config_activation::Relation::Version.def(),
        )
        .order_by_desc(runtime_config_activation::Column::ActivatedAt)
        .one(db)
        .await
        .map_err(StorageError::from)
        .map(|row| row.map(|version| version.runtime_config_version_id))
}

async fn load_kill_switch_allows_entry(db: &impl ConnectionTrait) -> Result<bool, StorageError> {
    Ok(
        system_kill_switch::Entity::find_by_id(SYSTEM_KILL_SWITCH_ID)
            .one(db)
            .await
            .map_err(StorageError::from)?
            .is_some_and(|row| row.state.allows_new_entry()),
    )
}

async fn intents_for_report<const N: usize>(
    db: &impl ConnectionTrait,
    report_id: &RecommendationReportId,
    statuses: [OrderIntentStatus; N],
) -> Result<Vec<OrderIntentInfo>, StorageError> {
    quant_order_intent::Entity::find()
        .filter(quant_order_intent::Column::Status.is_in(statuses))
        .join(
            JoinType::InnerJoin,
            quant_order_intent::Relation::Recommendation.def(),
        )
        .filter(quant_recommendation::Column::RecommendationReportId.eq(report_id.clone()))
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|rows| rows.into_iter().map(Into::into).collect())
}

async fn find_blocking_intent_for_recommendation(
    db: &impl ConnectionTrait,
    recommendation_id: &RecommendationId,
) -> Result<Option<quant_order_intent::Model>, StorageError> {
    quant_order_intent::Entity::find()
        .filter(quant_order_intent::Column::RecommendationId.eq(recommendation_id.clone()))
        .filter(
            quant_order_intent::Column::Status.is_in(OrderIntentStatus::SIBLING_INTENT_BLOCKING),
        )
        .one(db)
        .await
        .map_err(StorageError::from)
}

async fn apply_approval(
    db: &impl ConnectionTrait,
    intent_id: &OrderIntentId,
    row: quant_order_intent::Model,
    approval: ApproveOrderIntent,
    entry_override: Option<EntryOrderSpec>,
    allocated_override: Option<Usd>,
) -> Result<quant_order_intent::Model, StorageError> {
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
                Some(entity::QUANT_CAPITAL_ALLOCATION),
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
                Some(entity::QUANT_CAPITAL_ALLOCATION),
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
    row: quant_order_intent::Model,
    reason: ApprovalInvalidation,
    occurred_at: DateTime<Utc>,
    validate_transition: bool,
) -> Result<quant_order_intent::Model, StorageError> {
    if validate_transition {
        validate_intent_transition(row.status, OrderIntentStatus::Invalidated, intent_id)?;
    }
    let mut active = row.into_active_model();
    active.status = ActiveValue::Set(OrderIntentStatus::Invalidated);
    active.status_reason = ActiveValue::Set(Some(reason.as_str().to_owned()));
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
    after: &quant_order_intent::Model,
) -> Result<(), StorageError> {
    let after_info: OrderIntentInfo = after.clone().into();
    let operation_log = state_hash::apply_transition_hashes(operation_log, before, &after_info)?;
    operation_log::Entity::insert(operation_log.into_active_model())
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
    let rows = quant_order_intent::Entity::find()
        .filter(quant_order_intent::Column::RecommendationId.eq(recommendation_id.clone()))
        .filter(quant_order_intent::Column::Status.is_in(OrderIntentStatus::PRE_SUBMISSION_ACTIVE))
        .order_by_asc(quant_order_intent::Column::OrderIntentId)
        .lock_exclusive()
        .all(db)
        .await
        .map_err(StorageError::from)?;
    let mut invalidated = Vec::with_capacity(rows.len());
    for row in rows {
        let intent_id = row.order_intent_id.clone();
        validate_intent_transition(row.status, OrderIntentStatus::Invalidated, &intent_id)?;
        let before_info: OrderIntentInfo = row.clone().into();
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(OrderIntentStatus::Invalidated);
        active.status_reason = ActiveValue::Set(Some(reason.as_str().to_owned()));
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
            terminal_intent_operation_log(parent_log, &intent_id, reason),
            &before_info,
            &after_info,
        )?;
        operation_log::Entity::insert(intent_log.into_active_model())
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
) -> NewOperationLog {
    NewOperationLog {
        id: OperationLogId::from_v7(),
        request_id: format!("{}:intent:{intent_id}", parent.request_id),
        actor_user_id: parent.actor_user_id.clone(),
        actor_username: parent.actor_username.clone(),
        acting_role: parent.acting_role.clone(),
        category: OperationCategory::Governance,
        action: "quant.intent.invalidate".to_owned(),
        resource_type: Some(ResourceType::OrderIntent),
        resource_id: Some(intent_id.to_string()),
        http_method: parent.http_method.clone(),
        http_path: format!("/system/quant/intent/{intent_id}/invalidate"),
        http_status: parent.http_status,
        outcome: parent.outcome,
        client_ip: parent.client_ip.clone(),
        user_agent: parent.user_agent.clone(),
        latency_ms: parent.latency_ms,
        detail: serde_json::json!({
            "reason": reason.as_str(),
            "parent_action": parent.action,
            "parent_resource_id": parent.resource_id,
        }),
        before_hash: None,
        after_hash: None,
        governance_audit_event_id: parent.governance_audit_event_id.clone(),
        governance_audit_sequence: parent.governance_audit_sequence,
    }
}

pub async fn lock_terminal_graph(
    db: &impl ConnectionTrait,
    intent_id: &OrderIntentId,
) -> Result<quant_order_intent::Model, StorageError> {
    let probe = load_intent(db, intent_id).await?;
    let recommendation = quant_recommendation::Entity::find_by_id(probe.recommendation_id.clone())
        .lock_exclusive()
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| error::not_found(entity::QUANT_RECOMMENDATION, &probe.recommendation_id))?;
    let intent = load_intent_for_update(db, intent_id).await?;
    if recommendation.recommendation_id != intent.recommendation_id {
        return Err(error::state_conflict(
            entity::QUANT_ORDER_INTENT,
            Some(intent_id),
            "intent recommendation changed while acquiring terminal graph locks",
        ));
    }
    let condition =
        quant_entry_condition_instance::Entity::find_by_id(intent.condition_instance_id.clone())
            .lock_exclusive()
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                error::not_found(ENTRY_CONDITION_ENTITY, &intent.condition_instance_id)
            })?;
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
) -> Result<quant_order_intent::Model, StorageError> {
    quant_order_intent::Entity::find_by_id(intent_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| error::not_found(entity::QUANT_ORDER_INTENT, intent_id))
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
            // on a transient admission defer so the dispatcher retries (05.4).
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
        entity::QUANT_ORDER_INTENT,
        Some(intent_id),
        current,
        next,
    ))
}
