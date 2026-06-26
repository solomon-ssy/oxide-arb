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
        quant::capital_allocation::{capital_invariant_ok, validate_non_negative},
        query::paginate_mapped,
    },
    traits::OrderIntentRepository,
};
use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        ApproveOrderIntent, NewCapitalAllocation, NewOperationLog, NewOrderIntent, OrderIntentInfo,
        OrderIntentListQuery, Paginated,
    },
    entities::{operation_log, quant_capital_allocation, quant_order_intent, quant_recommendation},
    enums::{
        execution::{ApprovalInvalidation, CapitalAllocationState},
        quant::{ApprovalStatus, OrderIntentStatus},
    },
    types::{EntryOrderSpec, OrderIntentId, RecommendationId, RecommendationReportId, Usd},
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, ConnectionTrait, DatabaseConnection,
    EntityTrait, FromQueryResult, IntoActiveModel, QueryFilter, QueryOrder, QuerySelect,
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

/// Pre-submission statuses a report-termination cascade may invalidate.
const ACTIVE_STATUSES: [OrderIntentStatus; 4] = [
    OrderIntentStatus::PendingApproval,
    OrderIntentStatus::Approved,
    OrderIntentStatus::ApprovedByPolicy,
    OrderIntentStatus::AdmissionPending,
];

/// Postgres-backed order intent repository.
pub struct PgOrderIntentRepository {
    db: DatabaseConnection,
}

impl PgOrderIntentRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[derive(Debug, FromQueryResult)]
struct RecommendationIdRow {
    recommendation_id: RecommendationId,
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
            return Err(StorageError::Conflict(format!(
                "order intent must be created as pending_approval or approved_by_policy, got {}",
                intent.status.as_str()
            )));
        }
        if allocation.order_intent_id != intent.order_intent_id {
            return Err(StorageError::Conflict(
                "capital allocation must reference its own order intent".to_owned(),
            ));
        }
        if allocation.state != CapitalAllocationState::Allocated {
            return Err(StorageError::Conflict(format!(
                "new capital allocation must start as allocated, got {}",
                allocation.state.as_str()
            )));
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
            return Err(StorageError::Conflict(
                "capital allocation violates the reserve invariant on create".to_owned(),
            ));
        }

        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let intent_model = quant_order_intent::Entity::insert(intent.into_active_model())
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
    ) -> Result<OrderIntentInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let row = load_intent(&txn, intent_id).await?;
        if row.status != OrderIntentStatus::PendingApproval {
            return Err(StorageError::Conflict(format!(
                "cannot approve intent {intent_id} from status {}",
                row.status.as_str()
            )));
        }

        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(OrderIntentStatus::Approved);
        active.approval_status = ActiveValue::Set(ApprovalStatus::Approved);
        active.approved_by = ActiveValue::Set(Some(approval.approved_by));
        active.approval_reason = ActiveValue::Set(Some(approval.approval_reason));
        active.approved_at = ActiveValue::Set(Some(approval.approved_at));
        if let Some(entry) = entry_override {
            active.entry_order_json = ActiveValue::Set(entry);
        }
        let intent_model = active.update(&txn).await.map_err(StorageError::from)?;

        if let Some(new_allocated) = allocated_override {
            let cap = load_capital(&txn, intent_id).await?;
            if new_allocated > cap.allocated_usd {
                return Err(StorageError::Conflict(format!(
                    "approval cannot increase reserved capital for intent {intent_id}"
                )));
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
                return Err(StorageError::Conflict(format!(
                    "downscaled allocation violates the reserve invariant for intent {intent_id}"
                )));
            }
            let mut cap_active = cap.into_active_model();
            cap_active.allocated_usd = ActiveValue::Set(new_allocated);
            cap_active.reason = ActiveValue::Set(format!("approved downscale to {new_allocated}"));
            cap_active.update(&txn).await.map_err(StorageError::from)?;
        }

        txn.commit().await.map_err(StorageError::from)?;
        Ok(intent_model.into())
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
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(OrderIntentStatus::Expired);
        active.status_reason = ActiveValue::Set(Some("intent expired".to_owned()));
        let intent_model = active.update(&txn).await.map_err(StorageError::from)?;
        release_capital(&txn, intent_id, "expired".to_owned()).await?;
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
        validate_intent_transition(row.status, OrderIntentStatus::Invalidated, intent_id)?;
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(OrderIntentStatus::Invalidated);
        active.status_reason = ActiveValue::Set(Some(reason.as_str().to_owned()));
        let intent_model = active.update(&txn).await.map_err(StorageError::from)?;
        release_capital(&txn, intent_id, format!("invalidated: {}", reason.as_str())).await?;
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

    async fn find_active_by_report(
        &self,
        report_id: &RecommendationReportId,
    ) -> Result<Vec<OrderIntentInfo>, StorageError> {
        let recommendation_ids = quant_recommendation::Entity::find()
            .filter(quant_recommendation::Column::RecommendationReportId.eq(report_id.clone()))
            .select_only()
            .column(quant_recommendation::Column::RecommendationId)
            .into_model::<RecommendationIdRow>()
            .all(&self.db)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(|row| row.recommendation_id)
            .collect::<Vec<_>>();
        if recommendation_ids.is_empty() {
            return Ok(Vec::new());
        }
        quant_order_intent::Entity::find()
            .filter(quant_order_intent::Column::Status.is_in(ACTIVE_STATUSES))
            .filter(quant_order_intent::Column::RecommendationId.is_in(recommendation_ids))
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn get_for_update(
        &self,
        intent_id: &OrderIntentId,
    ) -> Result<Option<OrderIntentInfo>, StorageError> {
        quant_order_intent::Entity::find_by_id(intent_id.clone())
            .lock_exclusive()
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn transition(
        &self,
        intent_id: &OrderIntentId,
        next: OrderIntentStatus,
    ) -> Result<OrderIntentInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let row = load_intent(&txn, intent_id).await?;
        validate_intent_transition(row.status, next, intent_id)?;
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(next);
        let intent_model = active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(intent_model.into())
    }

    async fn mark_admission_rejected(
        &self,
        intent_id: &OrderIntentId,
        status_reason: String,
        admission_trace_ref: Option<String>,
    ) -> Result<OrderIntentInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let row = load_intent(&txn, intent_id).await?;
        validate_intent_transition(row.status, OrderIntentStatus::AdmissionRejected, intent_id)?;
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(OrderIntentStatus::AdmissionRejected);
        active.status_reason = ActiveValue::Set(Some(status_reason));
        active.admission_trace_ref = ActiveValue::Set(admission_trace_ref);
        let intent_model = active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(intent_model.into())
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

async fn load_intent(
    db: &impl ConnectionTrait,
    intent_id: &OrderIntentId,
) -> Result<quant_order_intent::Model, StorageError> {
    quant_order_intent::Entity::find_by_id(intent_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| StorageError::NotFound {
            entity: "order_intent",
            id: intent_id.to_string(),
        })
}

async fn load_capital(
    db: &impl ConnectionTrait,
    intent_id: &OrderIntentId,
) -> Result<quant_capital_allocation::Model, StorageError> {
    quant_capital_allocation::Entity::find()
        .filter(quant_capital_allocation::Column::OrderIntentId.eq(intent_id.clone()))
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| {
            StorageError::Conflict(format!(
                "capital allocation not found for intent: {intent_id}"
            ))
        })
}

/// Release the still-reserved capital of an intent's allocation (full release).
///
/// Sets `released_usd` to the reserve basis and the row to `Released`; a broken
/// invariant forces `Impaired` (fail-closed: never free corrupted budget).
async fn release_capital(
    db: &impl ConnectionTrait,
    intent_id: &OrderIntentId,
    reason: String,
) -> Result<(), StorageError> {
    let cap = load_capital(db, intent_id).await?;
    let released_usd = cap.allocated_usd.max(cap.locked_usd);
    validate_non_negative(
        cap.allocated_usd,
        cap.locked_usd,
        cap.spent_usd,
        released_usd,
    )?;
    let (state, reason) = if capital_invariant_ok(
        cap.planned_usd,
        cap.allocated_usd,
        cap.locked_usd,
        cap.spent_usd,
        released_usd,
    ) {
        (CapitalAllocationState::Released, reason)
    } else {
        (
            CapitalAllocationState::Impaired,
            format!("impaired: {reason}"),
        )
    };
    let mut active = cap.into_active_model();
    active.state = ActiveValue::Set(state);
    active.released_usd = ActiveValue::Set(released_usd);
    active.reason = ActiveValue::Set(reason);
    active.update(db).await.map_err(StorageError::from)?;
    Ok(())
}

fn validate_intent_transition(
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
            OrderIntentStatus::AdmissionPending,
            OrderIntentStatus::Submitted
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
    Err(StorageError::Conflict(format!(
        "invalid order intent transition for {intent_id}: {} -> {}",
        current.as_str(),
        next.as_str()
    )))
}
