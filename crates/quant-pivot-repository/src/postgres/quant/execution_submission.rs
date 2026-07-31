//! PostgreSQL-backed, money-critical execution-submission repository.
//!
//! Every method owns exactly one transaction spanning the execution order,
//! order intent, capital allocation, position ledger, recommendation, and
//! reconciliation tables, reusing the shared `&impl ConnectionTrait` helpers so
//! a submission's money state can never partially apply. Venue network I/O
//! happens between [`create_entry_order`] and
//! [`record_submission_result`] — never inside a transaction.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{
    StorageError,
    entity::{
        QUANT_EXECUTION_ORDER, QUANT_EXECUTION_TRADE_REF, QUANT_EXECUTION_TRANSACTION_REF,
        QUANT_ORDER_INTENT, QUANT_RECOMMENDATION, QUANT_RECOMMENDATION_REPORT,
    },
};
use quant_pivot_models::{
    domain::quant::{
        EntryConditionClaim, EntryConditionInstanceInfo, ExecutionIdentityEnrichment,
        ExecutionIdentityRefs, ExecutionOrderIdentityRefs, ExecutionOrderInfo, ExecutionTradeRef,
        ExecutionTransactionRef, ExitLedgerWrite, NewExecutionOrder, NewExecutionTradeRef,
        NewExecutionTransactionRef, NewReconciliation, OrderIntentInfo, ReconciliationLedgerWrite,
        SubmissionLedgerWrite,
    },
    entities::{
        quant_execution_order::{Column, Entity as QuantExecutionOrderEntity},
        quant_execution_trade_ref::{
            Column as QuantExecutionTradeRefColumn, Entity as QuantExecutionTradeRefEntity,
        },
        quant_execution_transaction_ref::{
            Column as QuantExecutionTransactionRefColumn,
            Entity as QuantExecutionTransactionRefEntity,
        },
        quant_order_intent::Model,
        quant_recommendation::Entity,
        quant_recommendation_report::Entity as QuantRecommendationReportEntity,
        quant_reconciliation::{
            Column as QuantReconciliationColumn, Entity as QuantReconciliationEntity,
        },
    },
    enums::{
        execution::{ExecutionOrderPhase, ExitReason, ExitState, VenueTradeStatus},
        quant::{
            ExecutionOrderState, OrderIntentStatus, RecommendationReportStatus,
            RecommendationStatus, ReportKind,
        },
    },
    types::{
        EvmTransactionHash, ExecutionOrderId, ExecutionTradeRefId, ExecutionTransactionRefId,
        ExitReinferenceObservation, FeatureParityStateId, OrderIntentId, PendingScaleOut, Price,
        RecommendationId, RecommendationReportId, ReconciliationId, ResearchProfileArtifactId,
        ResearchProfileId,
    },
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait,
    IntoActiveModel, QueryFilter, QueryOrder, QuerySelect, TransactionTrait,
};

use crate::{
    postgres::{
        error, primitives,
        quant::{
            capital_allocation::PgCapitalAllocationRepository,
            entry_condition::{
                claim_for_submission as claim_entry_condition, invalidate_for_intent_terminal,
                require_consumed_for_intent, revert_consumed_for_intent,
            },
            execution_order::validate_execution_order_transition,
            feature_parity::PgFeatureParityRepository,
            order_intent::{PgOrderIntentRepository, validate_intent_transition},
            position::PgPositionRepository,
            report_scope::ReportScope,
        },
        write::insert_many_chunked,
    },
    traits::ExecutionSubmissionRepository,
};

/// In-flight execution-order states scanned by boot recovery.
const DANGLING_STATES: [ExecutionOrderState; 2] = [
    ExecutionOrderState::Submitted,
    ExecutionOrderState::Ambiguous,
];

/// Exit-order states that must not overlap on the same intent (double-exit guard).
const IN_FLIGHT_EXIT_STATES: [ExecutionOrderState; 2] = [
    ExecutionOrderState::Submitted,
    ExecutionOrderState::Ambiguous,
];

/// Postgres-backed execution-submission repository.
pub struct PgExecutionSubmissionRepository {
    db: DatabaseConnection,
}

impl PgExecutionSubmissionRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

struct SubmissionReportScope {
    report_id: RecommendationReportId,
    recommendation_id: RecommendationId,
    profile_id: ResearchProfileId,
    research_profile_artifact_id: ResearchProfileArtifactId,
    report_kind: ReportKind,
}

impl PgExecutionSubmissionRepository {
    async fn insert_identity_refs(
        db: &impl ConnectionTrait,
        execution_order_id: ExecutionOrderId,
        refs: ExecutionIdentityRefs,
    ) -> Result<(), StorageError> {
        let unique_trade_ids = refs.trade_ids.iter().cloned().collect::<BTreeSet<_>>();
        if unique_trade_ids.len() != refs.trade_ids.len() {
            return Err(StorageError::invariant_violation(
                Some(QUANT_EXECUTION_TRADE_REF),
                "placement response contains duplicate venue trade ids",
            ));
        }
        if !unique_trade_ids.is_empty() {
            let trades = unique_trade_ids
                .into_iter()
                .map(|venue_trade_id| NewExecutionTradeRef {
                    execution_trade_ref_id: ExecutionTradeRefId::from_v7(),
                    execution_order_id,
                    venue_trade_id,
                    trade_status: None,
                    transaction_hash: None,
                    observed_at: refs.observed_at,
                })
                .collect();
            insert_many_chunked::<QuantExecutionTradeRefEntity, NewExecutionTradeRef>(db, trades)
                .await
                .map_err(|storage_error| match storage_error {
                    StorageError::Database(database_error) => error::map_unique(
                        database_error,
                        QUANT_EXECUTION_TRADE_REF,
                        "venue_trade_id",
                    ),
                    other => other,
                })?;
        }

        let unique_transaction_hashes =
            refs.transaction_hashes.into_iter().collect::<BTreeSet<_>>();
        if !unique_transaction_hashes.is_empty() {
            let transactions = unique_transaction_hashes
                .into_iter()
                .map(|transaction_hash| NewExecutionTransactionRef {
                    execution_transaction_ref_id: ExecutionTransactionRefId::from_v7(),
                    execution_order_id,
                    transaction_hash,
                    observed_at: refs.observed_at,
                })
                .collect();
            insert_many_chunked::<QuantExecutionTransactionRefEntity, NewExecutionTransactionRef>(
                db,
                transactions,
            )
            .await
            .map_err(|storage_error| match storage_error {
                StorageError::Database(database_error) => error::map_unique(
                    database_error,
                    QUANT_EXECUTION_TRANSACTION_REF,
                    "execution_order_id,transaction_hash",
                ),
                other => other,
            })?;
        }
        Ok(())
    }
}

fn validate_trade_status_transition(
    trade_ref: &ExecutionTradeRef,
    next: VenueTradeStatus,
) -> Result<(), StorageError> {
    let Some(current) = trade_ref.trade_status else {
        return Ok(());
    };
    let allowed = match current {
        VenueTradeStatus::Matched => true,
        VenueTradeStatus::Retrying | VenueTradeStatus::Mined => {
            !matches!(next, VenueTradeStatus::Matched)
        }
        VenueTradeStatus::Confirmed => next == VenueTradeStatus::Confirmed,
        VenueTradeStatus::Failed => next == VenueTradeStatus::Failed,
    };
    if allowed {
        Ok(())
    } else {
        Err(StorageError::illegal_transition(
            QUANT_EXECUTION_TRADE_REF,
            Some(&trade_ref.venue_trade_id),
            format!("{current:?}"),
            format!("{next:?}"),
        ))
    }
}

impl PgExecutionSubmissionRepository {
    async fn insert_missing_tx_ref(
        db: &impl ConnectionTrait,
        execution_order_id: ExecutionOrderId,
        transaction_hash: EvmTransactionHash,
        observed_at: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        let exists = QuantExecutionTransactionRefEntity::find()
            .filter(QuantExecutionTransactionRefColumn::ExecutionOrderId.eq(execution_order_id))
            .filter(
                QuantExecutionTransactionRefColumn::TransactionHash.eq(transaction_hash.clone()),
            )
            .one(db)
            .await
            .map_err(StorageError::from)?
            .is_some();
        if exists {
            return Ok(());
        }
        QuantExecutionTransactionRefEntity::insert(
            NewExecutionTransactionRef {
                execution_transaction_ref_id: ExecutionTransactionRefId::from_v7(),
                execution_order_id,
                transaction_hash,
                observed_at,
            }
            .into_active_model(),
        )
        .exec(db)
        .await
        .map_err(|database_error| {
            error::map_unique(
                database_error,
                QUANT_EXECUTION_TRANSACTION_REF,
                "execution_order_id,transaction_hash",
            )
        })?;
        Ok(())
    }
}

impl PgExecutionSubmissionRepository {
    async fn probe_submission_scope(
        db: &impl ConnectionTrait,
        intent_id: &OrderIntentId,
    ) -> Result<SubmissionReportScope, StorageError> {
        let intent = PgOrderIntentRepository::load_intent(db, intent_id).await?;
        let recommendation = Entity::find_by_id(intent.recommendation_id)
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(QUANT_RECOMMENDATION, intent.recommendation_id)
            })?;
        let report =
            QuantRecommendationReportEntity::find_by_id(recommendation.recommendation_report_id)
                .one(db)
                .await
                .map_err(StorageError::from)?
                .ok_or_else(|| {
                    StorageError::not_found(
                        QUANT_RECOMMENDATION_REPORT,
                        recommendation.recommendation_report_id,
                    )
                })?;
        Ok(SubmissionReportScope {
            report_id: report.recommendation_report_id,
            recommendation_id: recommendation.recommendation_id,
            profile_id: report.research_profile_artifact_id.profile_ref().id,
            research_profile_artifact_id: report.research_profile_artifact_id,
            report_kind: report.report_kind,
        })
    }
}

impl PgExecutionSubmissionRepository {
    async fn lock_submission_graph(
        db: &impl ConnectionTrait,
        intent_id: &OrderIntentId,
        scope: &SubmissionReportScope,
    ) -> Result<Model, StorageError> {
        let report = QuantRecommendationReportEntity::find_by_id(scope.report_id)
            .lock_exclusive()
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_RECOMMENDATION_REPORT, scope.report_id))?;
        if report.research_profile_artifact_id != scope.research_profile_artifact_id
            || report.report_kind != scope.report_kind
            || report.status != RecommendationReportStatus::Published
        {
            return Err(StorageError::state_conflict(
                QUANT_RECOMMENDATION_REPORT,
                Some(&scope.report_id),
                "parent report is no longer the published entry authority",
            ));
        }
        let recommendation = Entity::find_by_id(scope.recommendation_id)
            .lock_exclusive()
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(QUANT_RECOMMENDATION, scope.recommendation_id)
            })?;
        if recommendation.recommendation_report_id != scope.report_id
            || !recommendation.status.allows_new_intent()
        {
            return Err(StorageError::state_conflict(
                QUANT_RECOMMENDATION,
                Some(&scope.recommendation_id),
                "recommendation is no longer actionable under its parent report",
            ));
        }
        let intent = PgOrderIntentRepository::load_intent_for_update(db, intent_id).await?;
        if intent.recommendation_id != scope.recommendation_id {
            return Err(StorageError::state_conflict(
                QUANT_ORDER_INTENT,
                Some(intent_id),
                "intent recommendation changed while acquiring submission graph locks",
            ));
        }
        Ok(intent)
    }
}

#[async_trait::async_trait]
impl ExecutionSubmissionRepository for PgExecutionSubmissionRepository {
    async fn claim_for_submission(
        &self,
        claim: EntryConditionClaim,
    ) -> Result<(OrderIntentInfo, EntryConditionInstanceInfo), StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let intent_id = &claim.order_intent_id;
        let scope = Self::probe_submission_scope(&txn, intent_id).await?;
        ReportScope::new(&scope.profile_id, scope.report_kind)
            .acquire(&txn)
            .await?;
        let row = Self::lock_submission_graph(&txn, intent_id, &scope).await?;
        if !matches!(
            row.status,
            OrderIntentStatus::Approved | OrderIntentStatus::ApprovedByPolicy
        ) {
            return Err(StorageError::state_conflict(
                QUANT_ORDER_INTENT,
                Some(intent_id),
                format!("intent is not submittable from {}", row.status.as_str()),
            ));
        }
        if row.expires_at <= claim.claimed_at {
            return Err(StorageError::state_conflict(
                QUANT_ORDER_INTENT,
                Some(intent_id),
                "intent has expired and cannot be submitted",
            ));
        }
        if row.condition_instance_id != claim.condition_instance_id {
            return Err(StorageError::state_conflict(
                QUANT_ORDER_INTENT,
                Some(intent_id),
                "intent/condition pair changed before atomic claim",
            ));
        }
        let condition = claim_entry_condition(&txn, &claim).await?;
        if condition.recommendation_id != row.recommendation_id {
            return Err(StorageError::invariant_violation(
                Some(QUANT_ORDER_INTENT),
                "condition recommendation does not match intent recommendation",
            ));
        }
        validate_intent_transition(row.status, OrderIntentStatus::AdmissionPending, intent_id)?;
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(OrderIntentStatus::AdmissionPending);
        let intent_model = active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok((intent_model.into(), condition.into()))
    }

    async fn revert_claim(
        &self,
        intent_id: &OrderIntentId,
    ) -> Result<OrderIntentInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let row = PgOrderIntentRepository::lock_terminal_graph(&txn, intent_id).await?;
        // No-op if the claim is already gone (e.g. report-cascade invalidation).
        if row.status != OrderIntentStatus::AdmissionPending {
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(row.into());
        }
        let revert_to = revert_target_status(&row);
        validate_intent_transition(row.status, revert_to, intent_id)?;
        revert_consumed_for_intent(&txn, &row.condition_instance_id, intent_id, Utc::now()).await?;
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(revert_to);
        let intent_model = active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(intent_model.into())
    }

    async fn reject_admission(
        &self,
        intent_id: &OrderIntentId,
        status_reason: String,
        admission_trace_ref: Option<String>,
    ) -> Result<OrderIntentInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let row = PgOrderIntentRepository::lock_terminal_graph(&txn, intent_id).await?;
        validate_intent_transition(row.status, OrderIntentStatus::AdmissionRejected, intent_id)?;
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(OrderIntentStatus::AdmissionRejected);
        active.status_reason = ActiveValue::Set(Some(status_reason.clone()));
        active.admission_trace_ref = ActiveValue::Set(admission_trace_ref);
        let intent_model = active.update(&txn).await.map_err(StorageError::from)?;
        invalidate_for_intent_terminal(
            &txn,
            &intent_model.condition_instance_id,
            intent_id,
            format!("admission rejected: {status_reason}"),
            Utc::now(),
        )
        .await?;
        PgCapitalAllocationRepository::release_capital(
            &txn,
            intent_id,
            format!("admission rejected: {status_reason}"),
        )
        .await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(intent_model.into())
    }

    async fn create_entry_order(
        &self,
        order: NewExecutionOrder,
        feature_parity_state_id: &FeatureParityStateId,
    ) -> Result<ExecutionOrderInfo, StorageError> {
        let intent_id = order.order_intent_id;
        let txn = self.db.begin().await.map_err(StorageError::from)?;

        // Scope lock linearizes write-ahead submission against report supersession.
        // The read-only probe is never authoritative; every parent row is
        // re-locked and revalidated after the advisory lock is held.
        let scope = Self::probe_submission_scope(&txn, &intent_id).await?;
        ReportScope::new(&scope.profile_id, scope.report_kind)
            .acquire(&txn)
            .await?;
        let intent = Self::lock_submission_graph(&txn, &intent_id, &scope).await?;
        PgFeatureParityRepository::verify_clear_latch_generation(&txn, feature_parity_state_id)
            .await?;
        if intent.status != OrderIntentStatus::AdmissionPending {
            return Err(StorageError::state_conflict(
                QUANT_ORDER_INTENT,
                Some(&intent_id),
                format!(
                    "intent must be admission_pending to create an entry order, got {}",
                    intent.status.as_str()
                ),
            ));
        }
        let recommendation_id = intent.recommendation_id;
        require_consumed_for_intent(&txn, &intent.condition_instance_id, &intent_id).await?;

        // Write-ahead the venue intent: the row exists in `Submitted` before any
        // network call, so a crash mid-submit is recoverable via reconciliation.
        let execution_order = QuantExecutionOrderEntity::insert(order.into_active_model())
            .exec_with_returning(&txn)
            .await
            .map_err(StorageError::from)?;

        PgCapitalAllocationRepository::lock_capital(
            &txn,
            &intent_id,
            "locked for submission".to_owned(),
        )
        .await?;

        validate_intent_transition(intent.status, OrderIntentStatus::Submitted, &intent_id)?;
        let mut intent_active = intent.into_active_model();
        intent_active.status = ActiveValue::Set(OrderIntentStatus::Submitted);
        intent_active
            .update(&txn)
            .await
            .map_err(StorageError::from)?;

        Self::advance_recommendation_executed(&txn, &recommendation_id).await?;

        txn.commit().await.map_err(StorageError::from)?;
        Ok(execution_order.into())
    }

    async fn record_submission_result(
        &self,
        execution_order_id: &ExecutionOrderId,
        write: SubmissionLedgerWrite,
    ) -> Result<ExecutionOrderInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;

        let order = QuantExecutionOrderEntity::find_by_id(*execution_order_id)
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_EXECUTION_ORDER, execution_order_id))?;
        validate_execution_order_transition(order.state, write.state, execution_order_id)?;
        let intent_id = order.order_intent_id;
        let entry_phase = order.order_phase == ExecutionOrderPhase::Entry;

        // Lock the intent so its status advances atomically with the ledger.
        let intent = PgOrderIntentRepository::load_intent_for_update(&txn, &intent_id).await?;

        let mut order_active = order.into_active_model();
        order_active.state = ActiveValue::Set(write.state);
        order_active.venue_order_id = ActiveValue::Set(write.venue_order_id);
        order_active.venue_status = ActiveValue::Set(write.venue_status);
        order_active.submitted_at = ActiveValue::Set(Some(write.submitted_at));
        order_active.filled_at = ActiveValue::Set(write.filled_at);
        order_active.cancelled_at = ActiveValue::Set(write.cancelled_at);
        order_active.error_message = ActiveValue::Set(write.error_message);
        let execution_order = order_active
            .update(&txn)
            .await
            .map_err(StorageError::from)?;

        Self::insert_identity_refs(&txn, *execution_order_id, write.identity_refs).await?;

        PgCapitalAllocationRepository::settle_capital(
            &txn,
            &intent_id,
            &write.capital,
            "submission result".to_owned(),
        )
        .await?;

        let entry_fill_shares = if entry_phase {
            write.fill.as_ref().map(|fill| fill.shares)
        } else {
            None
        };

        if let Some(fill) = write.fill {
            PgPositionRepository::apply_fill(&txn, fill).await?;
        }

        // Entry fill freezes the denominator shared by every scale-out source.
        let prior_status = intent.status;
        let prior_scale_out_state = intent.scale_out_state.clone();
        let mut intent_active = intent.into_active_model();
        if let Some(shares) = entry_fill_shares {
            let mut state = prior_scale_out_state.clone();
            if state.denominator_shares.is_none() {
                state.denominator_shares = Some(shares);
                intent_active.scale_out_state = ActiveValue::Set(state);
            }
        }

        // Only transition the intent when the target differs (resting `Open` and
        // `Ambiguous` keep the intent at `Submitted`).
        if write.intent_status != prior_status {
            validate_intent_transition(prior_status, write.intent_status, &intent_id)?;
            intent_active.status = ActiveValue::Set(write.intent_status);
        }
        if !matches!(intent_active.status, ActiveValue::NotSet)
            || !matches!(intent_active.scale_out_state, ActiveValue::NotSet)
        {
            intent_active
                .update(&txn)
                .await
                .map_err(StorageError::from)?;
        }

        if let Some(reconciliation) = write.reconciliation {
            QuantReconciliationEntity::insert(reconciliation.into_active_model())
                .exec(&txn)
                .await
                .map_err(StorageError::from)?;
        }

        txn.commit().await.map_err(StorageError::from)?;
        Ok(execution_order.into())
    }

    async fn load_identity_refs(
        &self,
        execution_order_id: &ExecutionOrderId,
    ) -> Result<ExecutionOrderIdentityRefs, StorageError> {
        let exists = QuantExecutionOrderEntity::find_by_id(*execution_order_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .is_some();
        if !exists {
            return Err(StorageError::not_found(
                QUANT_EXECUTION_ORDER,
                execution_order_id,
            ));
        }
        let trades = QuantExecutionTradeRefEntity::find()
            .filter(QuantExecutionTradeRefColumn::ExecutionOrderId.eq(*execution_order_id))
            .order_by_asc(QuantExecutionTradeRefColumn::CreatedAt)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(ExecutionTradeRef::from)
            .collect();
        let transactions = QuantExecutionTransactionRefEntity::find()
            .filter(QuantExecutionTransactionRefColumn::ExecutionOrderId.eq(*execution_order_id))
            .order_by_asc(QuantExecutionTransactionRefColumn::CreatedAt)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(ExecutionTransactionRef::from)
            .collect();
        Ok(ExecutionOrderIdentityRefs {
            trades,
            transactions,
        })
    }

    async fn enrich_identity_refs(
        &self,
        execution_order_id: &ExecutionOrderId,
        enrichment: ExecutionIdentityEnrichment,
    ) -> Result<ExecutionOrderIdentityRefs, StorageError> {
        let unique_trade_ids = enrichment
            .trades
            .iter()
            .map(|trade| trade.venue_trade_id.clone())
            .collect::<BTreeSet<_>>();
        if unique_trade_ids.len() != enrichment.trades.len() {
            return Err(StorageError::invariant_violation(
                Some(QUANT_EXECUTION_TRADE_REF),
                "identity enrichment contains duplicate venue trade ids",
            ));
        }

        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let order = QuantExecutionOrderEntity::find_by_id(*execution_order_id)
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_EXECUTION_ORDER, execution_order_id))?;
        if let Some(discovered_order_id) = enrichment.discovered_order_id {
            if order
                .venue_order_id
                .as_ref()
                .is_some_and(|current| current != &discovered_order_id)
            {
                return Err(StorageError::state_conflict(
                    QUANT_EXECUTION_ORDER,
                    Some(execution_order_id),
                    "venue order identity cannot be replaced by reconciliation discovery",
                ));
            }
            if order.venue_order_id.is_none() {
                let mut active = order.into_active_model();
                active.venue_order_id = ActiveValue::Set(Some(discovered_order_id));
                active.update(&txn).await.map_err(StorageError::from)?;
            }
        }

        for observation in enrichment.trades {
            if matches!(
                observation.trade_status,
                VenueTradeStatus::Mined | VenueTradeStatus::Confirmed
            ) && observation.transaction_hash.is_none()
            {
                return Err(StorageError::invariant_violation(
                    Some(QUANT_EXECUTION_TRADE_REF),
                    format!(
                        "trade {} is {:?} without a transaction hash",
                        observation.venue_trade_id, observation.trade_status
                    ),
                ));
            }
            let existing = QuantExecutionTradeRefEntity::find()
                .filter(
                    QuantExecutionTradeRefColumn::VenueTradeId
                        .eq(observation.venue_trade_id.clone()),
                )
                .lock_exclusive()
                .one(&txn)
                .await
                .map_err(StorageError::from)?;
            if let Some(existing) = existing {
                if existing.execution_order_id != *execution_order_id {
                    return Err(StorageError::duplicate(
                        QUANT_EXECUTION_TRADE_REF,
                        observation.venue_trade_id,
                    ));
                }
                let trade_ref = ExecutionTradeRef::from(existing.clone());
                validate_trade_status_transition(&trade_ref, observation.trade_status)?;
                if trade_ref.trade_status == Some(VenueTradeStatus::Confirmed)
                    && trade_ref.transaction_hash != observation.transaction_hash
                {
                    return Err(StorageError::state_conflict(
                        QUANT_EXECUTION_TRADE_REF,
                        Some(&trade_ref.venue_trade_id),
                        "confirmed trade transaction hash is immutable",
                    ));
                }
                let transaction_hash = observation
                    .transaction_hash
                    .clone()
                    .or(trade_ref.transaction_hash);
                let mut active = existing.into_active_model();
                active.trade_status = ActiveValue::Set(Some(observation.trade_status));
                active.transaction_hash = ActiveValue::Set(transaction_hash.clone());
                active.observed_at = ActiveValue::Set(enrichment.observed_at);
                active.update(&txn).await.map_err(StorageError::from)?;
                if let Some(transaction_hash) = transaction_hash {
                    Self::insert_missing_tx_ref(
                        &txn,
                        *execution_order_id,
                        transaction_hash,
                        enrichment.observed_at,
                    )
                    .await?;
                }
            } else {
                let transaction_hash = observation.transaction_hash.clone();
                QuantExecutionTradeRefEntity::insert(
                    NewExecutionTradeRef {
                        execution_trade_ref_id: ExecutionTradeRefId::from_v7(),
                        execution_order_id: *execution_order_id,
                        venue_trade_id: observation.venue_trade_id,
                        trade_status: Some(observation.trade_status),
                        transaction_hash: transaction_hash.clone(),
                        observed_at: enrichment.observed_at,
                    }
                    .into_active_model(),
                )
                .exec(&txn)
                .await
                .map_err(|database_error| {
                    error::map_unique(database_error, QUANT_EXECUTION_TRADE_REF, "venue_trade_id")
                })?;
                if let Some(transaction_hash) = transaction_hash {
                    Self::insert_missing_tx_ref(
                        &txn,
                        *execution_order_id,
                        transaction_hash,
                        enrichment.observed_at,
                    )
                    .await?;
                }
            }
        }
        txn.commit().await.map_err(StorageError::from)?;
        self.load_identity_refs(execution_order_id).await
    }

    async fn mark_exit_manual(
        &self,
        intent_id: &OrderIntentId,
        reason: ExitReason,
    ) -> Result<(), StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let intent = PgOrderIntentRepository::load_intent_for_update(&txn, intent_id).await?;
        let mut active = intent.into_active_model();
        active.exit_state = ActiveValue::Set(ExitState::ManualRequired);
        active.exit_reason = ActiveValue::Set(Some(reason));
        active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(())
    }

    async fn touch_exit_monitor(
        &self,
        intent_id: &OrderIntentId,
        next_check_at: DateTime<Utc>,
        peak_mark_price: Option<Price>,
        last_signal_recheck_at: Option<DateTime<Utc>>,
        latest_reinference: Option<ExitReinferenceObservation>,
    ) -> Result<(), StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let intent = PgOrderIntentRepository::load_intent_for_update(&txn, intent_id).await?;
        let promote = intent.exit_state == ExitState::NotStarted;
        let mut active = intent.into_active_model();
        if promote {
            active.exit_state = ActiveValue::Set(ExitState::Monitoring);
        }
        active.next_check_at = ActiveValue::Set(Some(next_check_at));
        if let Some(peak) = peak_mark_price {
            active.peak_mark_price = ActiveValue::Set(Some(peak));
        }
        if let Some(recheck) = last_signal_recheck_at {
            active.last_signal_recheck_at = ActiveValue::Set(Some(recheck));
        }
        if let Some(observation) = latest_reinference {
            active.latest_reinference_json = ActiveValue::Set(Some(observation));
        }
        active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(())
    }

    async fn create_exit_order(
        &self,
        order: NewExecutionOrder,
        exit_reason: ExitReason,
        pending_scale_out: Option<PendingScaleOut>,
    ) -> Result<ExecutionOrderInfo, StorageError> {
        if pending_scale_out.is_some()
            && !matches!(
                exit_reason,
                ExitReason::PartialExit | ExitReason::Opportunistic
            )
        {
            return Err(StorageError::invariant_violation(
                Some(QUANT_ORDER_INTENT),
                format!(
                    "pending scale-out requires PartialExit or Opportunistic reason, got {}",
                    exit_reason.as_str()
                ),
            ));
        }
        let intent_id = order.order_intent_id;
        let txn = self.db.begin().await.map_err(StorageError::from)?;

        // At most one in-flight exit order per intent — prevents oversell when a
        // partial exit is re-triggered while a resting GTC/Ambiguous order exists.
        let inflight = QuantExecutionOrderEntity::find()
            .filter(Column::OrderIntentId.eq(intent_id))
            .filter(Column::OrderPhase.eq(ExecutionOrderPhase::Exit))
            .filter(Column::State.is_in(IN_FLIGHT_EXIT_STATES))
            .one(&txn)
            .await
            .map_err(StorageError::from)?;
        if inflight.is_some() {
            return Err(StorageError::state_conflict(
                QUANT_ORDER_INTENT,
                Some(&intent_id),
                "intent already has an in-flight exit order (Submitted/Ambiguous)",
            ));
        }

        // Write-ahead the venue exit intent: the Exit order row exists in
        // `Submitted` before any network call (crash-recoverable via recon).
        let execution_order = QuantExecutionOrderEntity::insert(order.into_active_model())
            .exec_with_returning(&txn)
            .await
            .map_err(StorageError::from)?;

        // Mark the lot `Open -> Closing` and advance the intent exit FSM.
        PgPositionRepository::mark_closing(&txn, &intent_id).await?;
        let intent = PgOrderIntentRepository::load_intent_for_update(&txn, &intent_id).await?;
        let mut scale_out_state = intent.scale_out_state.clone();
        scale_out_state.pending_target = pending_scale_out;
        let mut intent_active = intent.into_active_model();
        intent_active.exit_state = ActiveValue::Set(ExitState::OrderSubmitted);
        intent_active.exit_reason = ActiveValue::Set(Some(exit_reason));
        intent_active.scale_out_state = ActiveValue::Set(scale_out_state);
        intent_active
            .update(&txn)
            .await
            .map_err(StorageError::from)?;

        txn.commit().await.map_err(StorageError::from)?;
        Ok(execution_order.into())
    }

    async fn record_exit_result(
        &self,
        execution_order_id: &ExecutionOrderId,
        write: ExitLedgerWrite,
    ) -> Result<ExecutionOrderInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;

        let order = QuantExecutionOrderEntity::find_by_id(*execution_order_id)
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_EXECUTION_ORDER, execution_order_id))?;

        // Idempotency guard: a terminal exit order already settled its position +
        // capital. Never re-apply.
        if order.state.is_terminal() {
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(order.into());
        }

        validate_execution_order_transition(order.state, write.order_state, execution_order_id)?;
        let intent_id = order.order_intent_id;
        let existing_venue_order_id = order.venue_order_id.clone();

        let mut order_active = order.into_active_model();
        order_active.state = ActiveValue::Set(write.order_state);
        order_active.venue_order_id =
            ActiveValue::Set(write.venue_order_id.clone().or(existing_venue_order_id));
        order_active.venue_status = ActiveValue::Set(write.venue_status);
        order_active.filled_at = ActiveValue::Set(write.filled_at);
        order_active.cancelled_at = ActiveValue::Set(write.cancelled_at);
        order_active.error_message = ActiveValue::Set(write.error_message.clone());
        let execution_order = order_active
            .update(&txn)
            .await
            .map_err(StorageError::from)?;

        Self::insert_identity_refs(&txn, *execution_order_id, write.identity_refs).await?;

        // Reduce/close the lot on a (partial) fill; complete capital on full exit.
        let exit_fill_shares = write.position_exit.as_ref().map(|exit| exit.shares);
        if let Some(exit) = write.position_exit {
            PgPositionRepository::apply_exit(&txn, &intent_id, exit).await?;
            if write.fully_exited {
                PgCapitalAllocationRepository::complete_exit_capital(
                    &txn,
                    &intent_id,
                    "exit settled".to_owned(),
                )
                .await?;
            }
        }

        // Revert a failed/cancelled exit attempt so the lot is re-monitored.
        if write.revert_to_open {
            PgPositionRepository::revert_lot_to_open(&txn, &intent_id).await?;
        }

        // Advance the intent's exit FSM (status is unchanged — entry already filled).
        let intent = PgOrderIntentRepository::load_intent_for_update(&txn, &intent_id).await?;
        let mut scale_out_state = intent.scale_out_state.clone();
        if write.revert_to_open {
            scale_out_state.pending_target = None;
        } else if let Some(filled) = exit_fill_shares
            && !write.fully_exited
        {
            scale_out_state.record(filled);
        }
        let mut intent_active = intent.into_active_model();
        intent_active.exit_state = ActiveValue::Set(write.exit_state);
        intent_active.exit_reason = ActiveValue::Set(Some(write.exit_reason));
        intent_active.scale_out_state = ActiveValue::Set(scale_out_state);
        intent_active
            .update(&txn)
            .await
            .map_err(StorageError::from)?;

        if let Some(reconciliation) = write.reconciliation {
            QuantReconciliationEntity::insert(reconciliation.into_active_model())
                .exec(&txn)
                .await
                .map_err(StorageError::from)?;
        }

        txn.commit().await.map_err(StorageError::from)?;
        Ok(execution_order.into())
    }

    async fn recover_dangling(&self, limit: u64) -> Result<Vec<ExecutionOrderInfo>, StorageError> {
        QuantExecutionOrderEntity::find()
            .filter(Column::State.is_in(DANGLING_STATES))
            .order_by_asc(Column::CreatedAt)
            .limit(limit)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn apply_reconciliation(
        &self,
        execution_order_id: &ExecutionOrderId,
        mut write: ReconciliationLedgerWrite,
    ) -> Result<ExecutionOrderInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;

        let order = QuantExecutionOrderEntity::find_by_id(*execution_order_id)
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_EXECUTION_ORDER, execution_order_id))?;

        // Idempotency guard: a terminal order has already had its capital and
        // position settled (at submit or by a prior reconciliation). Never
        // re-apply — return it unchanged.
        if order.state.is_terminal() {
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(order.into());
        }

        validate_execution_order_transition(order.state, write.order_state, execution_order_id)?;
        let intent_id = order.order_intent_id;
        let order_intent_id_for_recon = intent_id;
        let existing_venue_order_id = order.venue_order_id.clone();
        let is_exit = order.order_phase == ExecutionOrderPhase::Exit;

        // Lock the intent so its status advances atomically with the ledger.
        let intent = PgOrderIntentRepository::load_intent_for_update(&txn, &intent_id).await?;

        let mut order_active = order.into_active_model();
        order_active.state = ActiveValue::Set(write.order_state);
        order_active.venue_order_id =
            ActiveValue::Set(write.venue_order_id.clone().or(existing_venue_order_id));
        order_active.venue_status = ActiveValue::Set(write.venue_status);
        order_active.filled_at = ActiveValue::Set(write.filled_at);
        order_active.cancelled_at = ActiveValue::Set(write.cancelled_at);
        order_active.error_message = ActiveValue::Set(write.error_message.clone());
        let execution_order = order_active
            .update(&txn)
            .await
            .map_err(StorageError::from)?;

        if is_exit {
            // Exit-order reconciliation: the entry intent stays terminal; correct
            // the lot via `apply_exit` (never `apply_fill`) and complete the
            // capital `Spent -> Released` on a full exit.
            let exit_fill_shares = write.exit.as_ref().map(|exit| exit.shares);
            let revert_lot = write.revert_lot;
            if let Some(exit) = write.exit.take() {
                PgPositionRepository::apply_exit(&txn, &intent_id, exit).await?;
                if write.exit_fully {
                    PgCapitalAllocationRepository::complete_exit_capital(
                        &txn,
                        &intent_id,
                        "exit reconciliation".to_owned(),
                    )
                    .await?;
                }
            }
            if revert_lot {
                PgPositionRepository::revert_lot_to_open(&txn, &intent_id).await?;
            }
            let mut scale_out_state = intent.scale_out_state.clone();
            if revert_lot {
                scale_out_state.pending_target = None;
            } else if let Some(filled) = exit_fill_shares
                && !write.exit_fully
            {
                scale_out_state.record(filled);
            }
            let mut intent_active = intent.into_active_model();
            if let Some(exit_state) = write.exit_state {
                intent_active.exit_state = ActiveValue::Set(exit_state);
            }
            intent_active.scale_out_state = ActiveValue::Set(scale_out_state);
            intent_active
                .update(&txn)
                .await
                .map_err(StorageError::from)?;
        } else {
            PgCapitalAllocationRepository::reconcile_capital(
                &txn,
                &intent_id,
                &write.capital,
                "reconciliation".to_owned(),
            )
            .await?;

            if let Some(fill) = write.fill.take() {
                PgPositionRepository::apply_fill(&txn, fill).await?;
            }

            if write.intent_status != intent.status {
                validate_intent_transition(intent.status, write.intent_status, &intent_id)?;
                let mut intent_active = intent.into_active_model();
                intent_active.status = ActiveValue::Set(write.intent_status);
                intent_active
                    .update(&txn)
                    .await
                    .map_err(StorageError::from)?;
            }
        }

        Self::upsert_reconciliation_summary(
            &txn,
            execution_order_id,
            &order_intent_id_for_recon,
            write,
        )
        .await?;

        txn.commit().await.map_err(StorageError::from)?;
        Ok(execution_order.into())
    }
}

impl PgExecutionSubmissionRepository {
    /// Upsert the single reconciliation summary row for an execution order.
    ///
    /// Updates the existing row in place (e.g. an `Ambiguous` order's submit-time
    /// `Pending` row), appending the freshly-collected evidence to the chain so the
    /// row stays append-only (WORM). Inserts a fresh row for an order that never
    /// had one (a resting `Open` order). The unique index on `execution_order_id`
    /// guarantees at most one summary per order.
    async fn upsert_reconciliation_summary(
        db: &impl ConnectionTrait,
        execution_order_id: &ExecutionOrderId,
        order_intent_id: &OrderIntentId,
        write: ReconciliationLedgerWrite,
    ) -> Result<(), StorageError> {
        let existing = QuantReconciliationEntity::find()
            .filter(QuantReconciliationColumn::ExecutionOrderId.eq(*execution_order_id))
            .one(db)
            .await
            .map_err(StorageError::from)?;

        if let Some(row) = existing {
            let mut chain = row.evidence_json.clone();
            for evidence in write.evidence.into_inner() {
                chain.push(evidence);
            }
            let mut active = row.into_active_model();
            active.result = ActiveValue::Set(write.result);
            active.evidence_json = ActiveValue::Set(chain);
            active.venue_filled_shares = ActiveValue::Set(write.venue_filled_shares);
            active.venue_avg_price = ActiveValue::Set(write.venue_avg_price);
            active.expected_cash_delta_usd = ActiveValue::Set(write.expected_cash_delta_usd);
            active.venue_cash_delta_usd = ActiveValue::Set(write.venue_cash_delta_usd);
            active.realized_pnl_usd = ActiveValue::Set(write.realized_pnl_usd);
            active.expected_fee_usd = ActiveValue::Set(write.expected_fee_usd);
            active.observed_fee_usd = ActiveValue::Set(write.observed_fee_usd);
            active.fee_delta_usd = ActiveValue::Set(write.fee_delta_usd);
            active.resolved_by = ActiveValue::Set(write.resolved_by);
            active.resolved_at = ActiveValue::Set(write.resolved_at);
            active.update(db).await.map_err(StorageError::from)?;
            return Ok(());
        }

        let new = NewReconciliation {
            reconciliation_id: ReconciliationId::from_v7(),
            execution_order_id: *execution_order_id,
            order_intent_id: *order_intent_id,
            result: write.result,
            evidence_json: write.evidence,
            venue_filled_shares: write.venue_filled_shares,
            venue_avg_price: write.venue_avg_price,
            expected_cash_delta_usd: write.expected_cash_delta_usd,
            venue_cash_delta_usd: write.venue_cash_delta_usd,
            realized_pnl_usd: write.realized_pnl_usd,
            expected_fee_usd: write.expected_fee_usd,
            observed_fee_usd: write.observed_fee_usd,
            fee_delta_usd: write.fee_delta_usd,
            resolved_by: write.resolved_by,
            resolved_at: write.resolved_at,
        };
        QuantReconciliationEntity::insert(new.into_active_model())
            .exec(db)
            .await
            .map_err(StorageError::from)?;
        Ok(())
    }
}

/// Restore the pre-claim approval status after a transient admission defer.
///
/// Auto-execution intents (`policy_id` set) revert to `ApprovedByPolicy` so the
/// dispatcher worker can retry; semi-auto manual approvals revert to `Approved`.
const fn revert_target_status(row: &Model) -> OrderIntentStatus {
    if row.policy_id.is_some() {
        OrderIntentStatus::ApprovedByPolicy
    } else {
        OrderIntentStatus::Approved
    }
}

impl PgExecutionSubmissionRepository {
    /// Advance a recommendation to `Executed` on submission (idempotent forward-only:
    /// terminal `Revoked`/`Expired` rows are left untouched).
    async fn advance_recommendation_executed(
        db: &impl ConnectionTrait,
        recommendation_id: &RecommendationId,
    ) -> Result<(), StorageError> {
        let row = Entity::find_by_id(*recommendation_id)
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::NotFound {
                entity: "recommendation",
                id: recommendation_id.to_string(),
            })?;
        if row.status.allows_new_intent() {
            let mut active = row.into_active_model();
            active.status = ActiveValue::Set(RecommendationStatus::Executed);
            active.status_changed_at = ActiveValue::Set(primitives::statement_timestamp(db).await?);
            active.update(db).await.map_err(StorageError::from)?;
        }
        Ok(())
    }
}
