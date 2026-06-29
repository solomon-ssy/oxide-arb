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
        query::paginate_mapped,
        state_hash,
    },
    traits::OrderIntentRepository,
};
use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{
        ApproveOrderIntent, ApproveOrderIntentOutcome, NewCapitalAllocation, NewOperationLog,
        NewOrderIntent, OrderIntentInfo, OrderIntentListQuery, Paginated, RecommendationInfo,
        RecommendationReportInfo, evaluate_intent_approval_invalidation,
    },
    entities::{
        operation_log, quant_capital_allocation, quant_order_intent, quant_recommendation,
        quant_recommendation_report, runtime_config_activation, runtime_config_version,
        system_kill_switch,
    },
    enums::{
        execution::{ApprovalInvalidation, CapitalAllocationState},
        quant::{ApprovalStatus, OrderIntentStatus, RecommendationStatus},
    },
    types::{
        EntryOrderSpec, ExecutedPartialExitNodes, OrderIntentId, RecommendationId,
        RecommendationReportId, Usd,
    },
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, ConnectionTrait, DatabaseConnection,
    EntityTrait, IntoActiveModel, JoinType, QueryFilter, QueryOrder, QuerySelect, RelationTrait,
    TransactionTrait,
};

/// Statuses a TTL sweep may expire.
const EXPIRABLE_STATUSES: [OrderIntentStatus; 5] = [
    OrderIntentStatus::PendingApproval,
    OrderIntentStatus::Approved,
    OrderIntentStatus::ApprovedByPolicy,
    OrderIntentStatus::AdmissionPending,
    OrderIntentStatus::AdmissionRejected,
];

/// Singleton row id for `system_kill_switch`.
const SYSTEM_KILL_SWITCH_ID: i32 = 1;

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
        if rec_row.status == RecommendationStatus::Published {
            let mut rec_active = rec_row.into_active_model();
            rec_active.status = ActiveValue::Set(RecommendationStatus::IntentCreated);
            rec_active.update(&txn).await.map_err(StorageError::from)?;
        }
        let mut intent_active = intent.into_active_model();
        intent_active.executed_partial_exit_node_ids =
            ActiveValue::Set(ExecutedPartialExitNodes::default());
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
        let row = load_intent_for_update(&txn, intent_id).await?;
        if row.status != OrderIntentStatus::PendingApproval {
            return Err(error::state_conflict(
                entity::QUANT_ORDER_INTENT,
                Some(intent_id),
                format!("cannot approve intent from status {}", row.status.as_str()),
            ));
        }

        let rec = load_recommendation(&txn, &row.recommendation_id).await?;
        let report = load_report(&txn, &rec.recommendation_report_id).await?;
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
            let intent_model = transition_invalidated(&txn, intent_id, row, reason, false).await?;
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
    ) -> Result<OrderIntentInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let row = load_intent(&txn, intent_id).await?;
        validate_intent_transition(row.status, OrderIntentStatus::Rejected, intent_id)?;
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(OrderIntentStatus::Rejected);
        active.approval_status = ActiveValue::Set(ApprovalStatus::Rejected);
        active.status_reason = ActiveValue::Set(Some(reason.clone()));
        let intent_model = active.update(&txn).await.map_err(StorageError::from)?;
        release_capital(&txn, intent_id, format!("rejected: {reason}")).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(intent_model.into())
    }

    async fn cancel(
        &self,
        intent_id: &OrderIntentId,
        reason: String,
    ) -> Result<OrderIntentInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let row = load_intent(&txn, intent_id).await?;
        validate_intent_transition(row.status, OrderIntentStatus::Cancelled, intent_id)?;
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(OrderIntentStatus::Cancelled);
        active.status_reason = ActiveValue::Set(Some(reason.clone()));
        let intent_model = active.update(&txn).await.map_err(StorageError::from)?;
        release_capital(&txn, intent_id, format!("cancelled: {reason}")).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(intent_model.into())
    }

    async fn expire(
        &self,
        intent_id: &OrderIntentId,
        operation_log: NewOperationLog,
    ) -> Result<OrderIntentInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let row = load_intent(&txn, intent_id).await?;
        validate_intent_transition(row.status, OrderIntentStatus::Expired, intent_id)?;
        let before_info: OrderIntentInfo = row.clone().into();
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(OrderIntentStatus::Expired);
        active.status_reason = ActiveValue::Set(Some("intent expired".to_owned()));
        let intent_model = active.update(&txn).await.map_err(StorageError::from)?;
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
        operation_log: NewOperationLog,
    ) -> Result<OrderIntentInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let row = load_intent(&txn, intent_id).await?;
        let before_info: OrderIntentInfo = row.clone().into();
        let intent_model = transition_invalidated(&txn, intent_id, row, reason, true).await?;
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

    async fn page(
        &self,
        query: OrderIntentListQuery,
    ) -> Result<Paginated<OrderIntentInfo>, StorageError> {
        paginate_mapped(
            quant_order_intent::Entity::find()
                .filter(page_condition(&query))
                .order_by_desc(quant_order_intent::Column::CreatedAt),
            &self.db,
            &query.page,
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
        quant_order_intent::Entity::find()
            .filter(
                quant_order_intent::Column::Status.is_in(OrderIntentStatus::PRE_SUBMISSION_ACTIVE),
            )
            .join(
                JoinType::InnerJoin,
                quant_order_intent::Relation::Recommendation.def(),
            )
            .filter(quant_recommendation::Column::RecommendationReportId.eq(report_id.clone()))
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
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
        quant_order_intent::Entity::find()
            .join(
                JoinType::InnerJoin,
                quant_order_intent::Relation::Recommendation.def(),
            )
            .join(
                JoinType::LeftJoin,
                quant_order_intent::Relation::Position.def(),
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

fn page_condition(query: &OrderIntentListQuery) -> Condition {
    Condition::all()
        .add_option(
            query
                .status
                .map(|status| quant_order_intent::Column::Status.eq(status)),
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

async fn load_recommendation(
    db: &impl ConnectionTrait,
    recommendation_id: &RecommendationId,
) -> Result<RecommendationInfo, StorageError> {
    quant_recommendation::Entity::find_by_id(recommendation_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| StorageError::NotFound {
            entity: "recommendation",
            id: recommendation_id.to_string(),
        })
        .map(Into::into)
}

async fn load_report(
    db: &impl ConnectionTrait,
    report_id: &RecommendationReportId,
) -> Result<RecommendationReportInfo, StorageError> {
    quant_recommendation_report::Entity::find_by_id(report_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| StorageError::NotFound {
            entity: "recommendation_report",
            id: report_id.to_string(),
        })
        .map(Into::into)
}

async fn load_current_config_version_id(
    db: &impl ConnectionTrait,
) -> Result<Option<quant_pivot_models::types::RuntimeConfigVersionId>, StorageError> {
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
    validate_transition: bool,
) -> Result<quant_order_intent::Model, StorageError> {
    if validate_transition {
        validate_intent_transition(row.status, OrderIntentStatus::Invalidated, intent_id)?;
    }
    let mut active = row.into_active_model();
    active.status = ActiveValue::Set(OrderIntentStatus::Invalidated);
    active.status_reason = ActiveValue::Set(Some(reason.as_str().to_owned()));
    let intent_model = active.update(db).await.map_err(StorageError::from)?;
    release_capital(db, intent_id, format!("invalidated: {}", reason.as_str())).await?;
    Ok(intent_model)
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
                | OrderIntentStatus::Invalidated,
        ) | (
            OrderIntentStatus::Approved | OrderIntentStatus::ApprovedByPolicy,
            OrderIntentStatus::AdmissionPending,
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
        ) | (
            _,
            OrderIntentStatus::Cancelled
                | OrderIntentStatus::Failed
                | OrderIntentStatus::Expired
                | OrderIntentStatus::Invalidated,
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
