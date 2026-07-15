//! Postgres-backed recommendation report repository.

use std::{collections::HashSet, convert::identity};

use super::equity_snapshot::insert_equity_snapshot_monotonic;
use crate::{
    postgres::{
        error,
        quant::{feature_parity, order_intent::invalidate_pre_submission_for_recommendation},
        query::paginate_mapped,
        state_hash,
    },
    traits::RecommendationReportRepository,
};
use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{
        FeatureParityJobParams, NewEntryConditionArtifact, NewEntryConditionAudit,
        NewEntryConditionInstance, NewOperationLog, NewRecommendation, NewRecommendationReport,
        NewReportDataQualitySnapshot, NewReportFactDelivery, NewReportFeatureParity,
        NewReportTransaction, OrderIntentInfo, PageWindow, Paginated, QuantReportListQuery,
        RecommendationReportInfo, ReportDataQualitySnapshotInfo, ReportFactDeliveryInfo,
    },
    entities::{
        operation_log, quant_account_snapshot, quant_entry_condition_artifact,
        quant_entry_condition_audit, quant_entry_condition_instance, quant_feature_parity_run,
        quant_portfolio_plan, quant_recommendation, quant_recommendation_report,
        quant_report_data_quality_snapshot, quant_report_fact_delivery, quant_research_job,
    },
    enums::{
        execution::ApprovalInvalidation,
        quant::{
            EntryConditionAuditAction, EntryConditionState, FeatureParityRunKind,
            FeatureParityRunStatus, RecommendationReportStatus, RecommendationStatus,
            ReportFactDeliveryStatus, ReportKind, ResearchJobKind, ResearchJobStatus,
        },
    },
    types::{
        EntryConditionArtifactId, EntryConditionAuditId, EntryConditionPlan, FeatureParityStateId,
        ModelRunId, RecommendationReportId, RecommendationTradePlan, ReportDataQualitySnapshotId,
    },
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, DatabaseConnection, DatabaseTransaction,
    EntityTrait, IntoActiveModel, PaginatorTrait, QueryFilter, QueryOrder, QuerySelect,
    TransactionTrait,
    sea_query::{LockBehavior, LockType},
};

use uuid::Uuid;

const REPORT_FACT_RETRY_MAX_SECS: i64 = 300;
const REPORT_FACT_ERROR_MAX_CHARS: usize = 4_096;

/// Postgres-backed recommendation report repository.
pub struct PgRecommendationReportRepository {
    db: DatabaseConnection,
}

impl PgRecommendationReportRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

async fn claimed_fact_delivery(
    txn: &DatabaseTransaction,
    report_id: &RecommendationReportId,
    worker_id: Uuid,
) -> Result<quant_report_fact_delivery::Model, StorageError> {
    let row = quant_report_fact_delivery::Entity::find_by_id(report_id.clone())
        .lock_exclusive()
        .one(txn)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| StorageError::not_found(entity::QUANT_RECOMMENDATION_REPORT, report_id))?;
    if row.status != ReportFactDeliveryStatus::Delivering || row.claim_owner != Some(worker_id) {
        return Err(StorageError::state_conflict(
            entity::QUANT_RECOMMENDATION_REPORT,
            Some(report_id),
            "report fact delivery is not leased by this worker",
        ));
    }
    Ok(row)
}

async fn verify_feature_parity_commit(
    txn: &DatabaseTransaction,
    expected_state_id: &FeatureParityStateId,
) -> Result<(), StorageError> {
    feature_parity::verify_clear_latch_generation(txn, expected_state_id).await
}

fn validate_sampled_feature_parity(
    report: &NewRecommendationReport,
    parity: Option<&NewReportFeatureParity>,
) -> Result<(), StorageError> {
    let parity = parity.ok_or_else(|| {
        StorageError::invariant_violation(
            Some(entity::QUANT_FEATURE_PARITY_RUN),
            "every committed report requires atomic sampled parity",
        )
    })?;
    let run = &parity.run;
    let job = &parity.job;
    let params: FeatureParityJobParams =
        serde_json::from_value(job.params_json.clone()).map_err(|error| {
            StorageError::invariant_violation(
                Some(entity::QUANT_RESEARCH_JOB),
                format!("invalid feature_parity job params: {error}"),
            )
        })?;
    if run.kind != FeatureParityRunKind::Sampled
        || run.status != FeatureParityRunStatus::Queued
        || run.report_id.as_ref() != Some(&report.recommendation_report_id)
        || run.model_version_id.as_ref() != Some(&report.model_version_id)
        || run.training_dataset_id.is_some()
        || run.window_start != report.decision_at
        || run.window_end <= run.window_start
        || run.feature_contract_hash.is_none()
    {
        return Err(StorageError::invariant_violation(
            Some(entity::QUANT_FEATURE_PARITY_RUN),
            "sampled parity run must be queued and bound to the exact report/model/decision window",
        ));
    }
    if job.kind != ResearchJobKind::FeatureParity
        || job.status != ResearchJobStatus::Queued
        || job.runtime_config_version_id.as_ref() != Some(&report.runtime_config_version_id)
        || job.model_spec_id.is_some()
        || job.parent_job_id.is_some()
        || job.recovery_attempt != 0
        || job.max_recovery_attempts < 0
        || params.parity_run_id != run.run_id
        || params.materialization_timeout_secs == 0
        || params.request.window_start != Some(run.window_start)
        || params.request.window_end != Some(run.window_end)
        || params.request.reason != run.reason
    {
        return Err(StorageError::invariant_violation(
            Some(entity::QUANT_RESEARCH_JOB),
            "sampled parity research job must exactly bind the report parity run and runtime config",
        ));
    }
    Ok(())
}

fn validate_report_data_quality(
    report: &NewRecommendationReport,
    dq: &NewReportDataQualitySnapshot,
) -> Result<(), StorageError> {
    let wrong_snapshot = dq.report_data_quality_snapshot_id != report.data_quality_snapshot_ref;
    let wrong_decision = dq.decision_at != report.decision_at;
    let wrong_config = dq.runtime_config_version_id != report.runtime_config_version_id;
    if wrong_snapshot || wrong_decision || wrong_config {
        return Err(StorageError::invariant_violation(
            Some(entity::QUANT_RECOMMENDATION_REPORT),
            "report DQ snapshot must bind the exact report decision and runtime config",
        ));
    }
    let mut vector_ids = HashSet::new();
    let mut markets = HashSet::new();
    for record in &dq.tokens_json.0 {
        let vector_id = record.feature_vector_id.as_ref().ok_or_else(|| {
            StorageError::invariant_violation(
                Some(entity::QUANT_RECOMMENDATION_REPORT),
                format!(
                    "new report DQ row for market {} has no exact feature-vector binding",
                    record.market_id
                ),
            )
        })?;
        if !vector_ids.insert(vector_id.clone()) || !markets.insert(record.market_id.clone()) {
            return Err(StorageError::invariant_violation(
                Some(entity::QUANT_RECOMMENDATION_REPORT),
                "report DQ snapshot contains duplicate feature-vector or market bindings",
            ));
        }
    }
    Ok(())
}

fn validate_entry_conditions(
    recommendations: &[NewRecommendation],
    artifacts: &[NewEntryConditionArtifact],
    instances: &[NewEntryConditionInstance],
) -> Result<(), StorageError> {
    if recommendations.len() != instances.len() {
        return Err(StorageError::invariant_violation(
            Some(entity::QUANT_RECOMMENDATION_REPORT),
            "report commit requires exactly one condition instance per recommendation",
        ));
    }
    validate_condition_artifacts(artifacts)?;
    validate_condition_instances(instances)?;
    for recommendation in recommendations {
        let instance = instances
            .iter()
            .find(|instance| instance.recommendation_id == recommendation.recommendation_id)
            .ok_or_else(|| {
                StorageError::invariant_violation(
                    Some("quant_entry_condition_instance"),
                    "recommendation has no condition instance",
                )
            })?;
        validate_recommendation_condition(recommendation, instance, artifacts)?;
    }
    Ok(())
}

fn validate_condition_artifacts(
    artifacts: &[NewEntryConditionArtifact],
) -> Result<(), StorageError> {
    let mut artifact_ids = HashSet::new();
    for artifact in artifacts {
        let canonical = artifact
            .payload_json
            .clone()
            .canonicalize()
            .map_err(|error| {
                StorageError::invariant_violation(
                    Some("quant_entry_condition_artifact"),
                    error.to_string(),
                )
            })?;
        let content_hash = canonical.canonical_content_hash().map_err(|error| {
            StorageError::invariant_violation(
                Some("quant_entry_condition_artifact"),
                error.to_string(),
            )
        })?;
        if artifact.payload_json != canonical
            || artifact.content_hash != content_hash
            || artifact.artifact_id != EntryConditionArtifactId::from_content_hash(&content_hash)
            || !artifact_ids.insert(artifact.artifact_id.clone())
        {
            return Err(StorageError::invariant_violation(
                Some("quant_entry_condition_artifact"),
                "condition artifact is not canonical, content-addressed, and unique",
            ));
        }
    }
    Ok(())
}

fn validate_condition_instances(
    instances: &[NewEntryConditionInstance],
) -> Result<(), StorageError> {
    let mut recommendation_ids = HashSet::new();
    for instance in instances {
        if !recommendation_ids.insert(instance.recommendation_id.clone()) {
            return Err(StorageError::invariant_violation(
                Some("quant_entry_condition_instance"),
                "duplicate recommendation condition instance",
            ));
        }
        if instance.revision != 0
            || instance.lease_epoch != 0
            || instance.claimed_by_intent_id.is_some()
            || instance.consumed_at.is_some()
        {
            return Err(StorageError::invariant_violation(
                Some("quant_entry_condition_instance"),
                "new report condition instance has non-initial lifecycle state",
            ));
        }
    }
    Ok(())
}

fn validate_recommendation_condition(
    recommendation: &NewRecommendation,
    instance: &NewEntryConditionInstance,
    artifacts: &[NewEntryConditionArtifact],
) -> Result<(), StorageError> {
    match &recommendation.trade_plan {
        RecommendationTradePlan::Frozen { entry, .. } => match &entry.condition {
            EntryConditionPlan::Immediate
                if instance.state == EntryConditionState::NotRequired
                    && instance.artifact_id.is_none()
                    && instance.artifact_hash.is_none() => {}
            EntryConditionPlan::Conditional {
                artifact_id,
                content_hash,
            } if instance.state == EntryConditionState::Waiting
                && instance.artifact_id.as_ref() == Some(artifact_id)
                && instance.artifact_hash.as_ref() == Some(content_hash)
                && artifacts.iter().any(|artifact| {
                    artifact.artifact_id == *artifact_id
                        && artifact.content_hash == *content_hash
                        && artifact.payload_json.binding.recommendation_id
                            == recommendation.recommendation_id
                }) => {}
            _ => {
                return Err(StorageError::invariant_violation(
                    Some("quant_entry_condition_instance"),
                    "recommendation trade plan and condition instance disagree",
                ));
            }
        },
        RecommendationTradePlan::Unavailable { .. }
            if instance.state == EntryConditionState::Invalidated
                && instance.artifact_id.is_none()
                && instance.artifact_hash.is_none() => {}
        RecommendationTradePlan::Unavailable { .. } => {
            return Err(StorageError::invariant_violation(
                Some("quant_entry_condition_instance"),
                "unavailable trade plan must create an invalidated shadow instance",
            ));
        }
    }
    Ok(())
}

fn new_condition_audit(
    instance: &NewEntryConditionInstance,
    occurred_at: DateTime<Utc>,
) -> NewEntryConditionAudit {
    NewEntryConditionAudit {
        audit_id: EntryConditionAuditId::from_v7(),
        condition_instance_id: instance.condition_instance_id.clone(),
        revision: 0,
        action: EntryConditionAuditAction::Created,
        from_state: None,
        to_state: instance.state,
        truth_json: instance.truth_json.clone(),
        evaluation_hash: None,
        input_fingerprint: None,
        continuity_hash: None,
        lease_epoch: 0,
        detail: Some("created atomically with recommendation report".to_owned()),
        occurred_at,
    }
}

async fn insert_sampled_feature_parity(
    txn: &DatabaseTransaction,
    parity: Option<NewReportFeatureParity>,
    report_id: &RecommendationReportId,
) -> Result<(), StorageError> {
    let Some(parity) = parity else {
        return Ok(());
    };
    let parity_key = report_id.to_string();
    quant_feature_parity_run::Entity::insert(parity.run.into_active_model())
        .exec(txn)
        .await
        .map_err(|error| error::map_unique(error, entity::QUANT_FEATURE_PARITY_RUN, &parity_key))?;
    quant_research_job::Entity::insert(parity.job.into_active_model())
        .exec(txn)
        .await
        .map_err(StorageError::from)?;
    Ok(())
}

fn validate_report_transaction(
    transaction: &NewReportTransaction,
) -> Result<DateTime<Utc>, StorageError> {
    if transaction.recommendations.iter().any(|recommendation| {
        recommendation.recommendation_report_id != transaction.report.recommendation_report_id
            || recommendation.profile_ref != transaction.report.profile_ref
    }) {
        return Err(StorageError::invariant_violation(
            Some(entity::QUANT_RECOMMENDATION_REPORT),
            "every recommendation must bind the exact report id and research profile",
        ));
    }
    validate_sampled_feature_parity(
        &transaction.report,
        transaction.sampled_feature_parity.as_ref(),
    )?;
    validate_report_data_quality(&transaction.report, &transaction.data_quality_snapshot)?;
    validate_entry_conditions(
        &transaction.recommendations,
        &transaction.entry_condition_artifacts,
        &transaction.entry_condition_instances,
    )?;
    validate_fact_delivery(&transaction.report, transaction.fact_delivery.as_ref())?;
    transaction.report.published_at.ok_or_else(|| {
        StorageError::invariant_violation(
            Some(entity::QUANT_RECOMMENDATION_REPORT),
            "published report requires published_at for condition audit",
        )
    })
}

fn validate_fact_delivery(
    report: &NewRecommendationReport,
    delivery: Option<&NewReportFactDelivery>,
) -> Result<(), StorageError> {
    let delivery = delivery.ok_or_else(|| {
        StorageError::invariant_violation(
            Some(entity::QUANT_RECOMMENDATION_REPORT),
            "report commit requires an atomic report-fact delivery row",
        )
    })?;
    if delivery.recommendation_report_id != report.recommendation_report_id
        || delivery.status != ReportFactDeliveryStatus::Pending
        || delivery.bundle_bytes <= 0
        || delivery.recommendation_row_count < 0
        || delivery.funnel_row_count < 0
    {
        return Err(StorageError::invariant_violation(
            Some(entity::QUANT_RECOMMENDATION_REPORT),
            "report-fact delivery must be pending, non-empty, and bind the exact report id",
        ));
    }
    Ok(())
}

#[async_trait::async_trait]
impl RecommendationReportRepository for PgRecommendationReportRepository {
    async fn create_report(
        &self,
        transaction: NewReportTransaction,
    ) -> Result<RecommendationReportInfo, StorageError> {
        let condition_created_at = validate_report_transaction(&transaction)?;
        let NewReportTransaction {
            feature_parity_state_id,
            account_snapshot,
            equity_snapshot,
            data_quality_snapshot,
            portfolio_plan,
            report,
            recommendations,
            entry_condition_artifacts,
            entry_condition_instances,
            sampled_feature_parity,
            fact_delivery,
            operation_log,
        } = transaction;

        let feature_parity_state_id = feature_parity_state_id.ok_or_else(|| {
            StorageError::invariant_violation(
                Some(entity::QUANT_RECOMMENDATION_REPORT),
                "report commit requires a durable feature-parity clear generation",
            )
        })?;

        let txn = self.db.begin().await.map_err(StorageError::from)?;
        verify_feature_parity_commit(&txn, &feature_parity_state_id).await?;

        // Insert FK targets before the report header, then the report's children.
        quant_account_snapshot::Entity::insert(account_snapshot.into_active_model())
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        insert_equity_snapshot_monotonic(&txn, equity_snapshot).await?;
        quant_report_data_quality_snapshot::Entity::insert(
            data_quality_snapshot.into_active_model(),
        )
        .exec(&txn)
        .await
        .map_err(StorageError::from)?;
        quant_portfolio_plan::Entity::insert(portfolio_plan.into_active_model())
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        let report_model = quant_recommendation_report::Entity::insert(report.into_active_model())
            .exec_with_returning(&txn)
            .await
            .map_err(StorageError::from)?;
        let fact_delivery = fact_delivery.ok_or_else(|| {
            StorageError::invariant_violation(
                Some(entity::QUANT_RECOMMENDATION_REPORT),
                "validated report-fact delivery disappeared before insert",
            )
        })?;
        quant_report_fact_delivery::Entity::insert(fact_delivery.into_active_model())
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        if !entry_condition_artifacts.is_empty() {
            quant_entry_condition_artifact::Entity::insert_many(
                entry_condition_artifacts
                    .into_iter()
                    .map(IntoActiveModel::into_active_model),
            )
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        }
        insert_sampled_feature_parity(
            &txn,
            sampled_feature_parity,
            &report_model.recommendation_report_id,
        )
        .await?;
        if !recommendations.is_empty() {
            quant_recommendation::Entity::insert_many(
                recommendations
                    .into_iter()
                    .map(IntoActiveModel::into_active_model),
            )
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        }
        if !entry_condition_instances.is_empty() {
            quant_entry_condition_instance::Entity::insert_many(
                entry_condition_instances
                    .iter()
                    .cloned()
                    .map(IntoActiveModel::into_active_model),
            )
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
            quant_entry_condition_audit::Entity::insert_many(
                entry_condition_instances
                    .iter()
                    .map(|instance| new_condition_audit(instance, condition_created_at))
                    .map(IntoActiveModel::into_active_model),
            )
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        }
        operation_log::Entity::insert(operation_log.into_active_model())
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;

        txn.commit().await.map_err(StorageError::from)?;
        Ok(report_model.into())
    }

    async fn find_by_id(
        &self,
        report_id: &RecommendationReportId,
    ) -> Result<Option<RecommendationReportInfo>, StorageError> {
        quant_recommendation_report::Entity::find_by_id(report_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn find_fact_delivery(
        &self,
        report_id: &RecommendationReportId,
    ) -> Result<Option<ReportFactDeliveryInfo>, StorageError> {
        quant_report_fact_delivery::Entity::find_by_id(report_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn claim_fact_delivery(
        &self,
        worker_id: Uuid,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> Result<Option<ReportFactDeliveryInfo>, StorageError> {
        if lease_expires_at <= now {
            return Err(StorageError::invariant_violation(
                Some(entity::QUANT_RECOMMENDATION_REPORT),
                "report fact delivery lease must expire after claim time",
            ));
        }
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let row = quant_report_fact_delivery::Entity::find()
            .filter(
                Condition::any()
                    .add(
                        quant_report_fact_delivery::Column::Status
                            .eq(ReportFactDeliveryStatus::Pending),
                    )
                    .add(
                        Condition::all()
                            .add(
                                quant_report_fact_delivery::Column::Status
                                    .eq(ReportFactDeliveryStatus::Retrying),
                            )
                            .add(quant_report_fact_delivery::Column::NextAttemptAt.lte(now)),
                    )
                    .add(
                        Condition::all()
                            .add(
                                quant_report_fact_delivery::Column::Status
                                    .eq(ReportFactDeliveryStatus::Delivering),
                            )
                            .add(quant_report_fact_delivery::Column::LeaseExpiresAt.lte(now)),
                    ),
            )
            .order_by_asc(quant_report_fact_delivery::Column::CreatedAt)
            .lock_with_behavior(LockType::Update, LockBehavior::SkipLocked)
            .one(&txn)
            .await
            .map_err(StorageError::from)?;
        let Some(row) = row else {
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(None);
        };
        let attempt_count = row.attempt_count.checked_add(1).ok_or_else(|| {
            StorageError::invariant_violation(
                Some(entity::QUANT_RECOMMENDATION_REPORT),
                "report fact delivery attempt count overflow",
            )
        })?;
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(ReportFactDeliveryStatus::Delivering);
        active.attempt_count = ActiveValue::Set(attempt_count);
        active.claim_owner = ActiveValue::Set(Some(worker_id));
        active.lease_expires_at = ActiveValue::Set(Some(lease_expires_at));
        active.next_attempt_at = ActiveValue::Set(None);
        active.last_error = ActiveValue::Set(None);
        active.updated_at = ActiveValue::Set(now);
        let claimed = active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(Some(claimed.into()))
    }

    async fn fail_fact_delivery(
        &self,
        report_id: &RecommendationReportId,
        worker_id: Uuid,
        status: ReportFactDeliveryStatus,
        error: &str,
    ) -> Result<ReportFactDeliveryInfo, StorageError> {
        if !matches!(
            status,
            ReportFactDeliveryStatus::Retrying | ReportFactDeliveryStatus::Failed
        ) {
            return Err(StorageError::invariant_violation(
                Some(entity::QUANT_RECOMMENDATION_REPORT),
                "failed report fact delivery must transition to retrying or failed",
            ));
        }
        if error.trim().is_empty() {
            return Err(StorageError::invariant_violation(
                Some(entity::QUANT_RECOMMENDATION_REPORT),
                "failed report fact delivery requires a non-empty diagnostic",
            ));
        }
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let row = claimed_fact_delivery(&txn, report_id, worker_id).await?;
        let now = Utc::now();
        let next_attempt_at = if status == ReportFactDeliveryStatus::Retrying {
            Some(now + report_fact_retry_delay(row.attempt_count))
        } else {
            None
        };
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(status);
        active.claim_owner = ActiveValue::Set(None);
        active.lease_expires_at = ActiveValue::Set(None);
        active.next_attempt_at = ActiveValue::Set(next_attempt_at);
        active.last_error = ActiveValue::Set(Some(
            error.chars().take(REPORT_FACT_ERROR_MAX_CHARS).collect(),
        ));
        active.updated_at = ActiveValue::Set(now);
        let updated = active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(updated.into())
    }

    async fn verify_fact_delivery(
        &self,
        report_id: &RecommendationReportId,
        worker_id: Uuid,
        verified_at: DateTime<Utc>,
    ) -> Result<ReportFactDeliveryInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let row = claimed_fact_delivery(&txn, report_id, worker_id).await?;
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(ReportFactDeliveryStatus::Verified);
        active.claim_owner = ActiveValue::Set(None);
        active.lease_expires_at = ActiveValue::Set(None);
        active.next_attempt_at = ActiveValue::Set(None);
        active.last_error = ActiveValue::Set(None);
        active.verified_at = ActiveValue::Set(Some(verified_at));
        active.updated_at = ActiveValue::Set(verified_at);
        let updated = active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(updated.into())
    }

    async fn claim_fact_announcement(
        &self,
        worker_id: Uuid,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> Result<Option<ReportFactDeliveryInfo>, StorageError> {
        if lease_expires_at <= now {
            return Err(StorageError::invariant_violation(
                Some(entity::QUANT_RECOMMENDATION_REPORT),
                "report fact announcement lease must expire after claim time",
            ));
        }
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let row = quant_report_fact_delivery::Entity::find()
            .filter(
                quant_report_fact_delivery::Column::Status.eq(ReportFactDeliveryStatus::Verified),
            )
            .filter(quant_report_fact_delivery::Column::AnnouncedAt.is_null())
            .filter(
                Condition::any()
                    .add(quant_report_fact_delivery::Column::LeaseExpiresAt.is_null())
                    .add(quant_report_fact_delivery::Column::LeaseExpiresAt.lte(now)),
            )
            .order_by_asc(quant_report_fact_delivery::Column::VerifiedAt)
            .lock_with_behavior(LockType::Update, LockBehavior::SkipLocked)
            .one(&txn)
            .await
            .map_err(StorageError::from)?;
        let Some(row) = row else {
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(None);
        };
        let mut active = row.into_active_model();
        active.claim_owner = ActiveValue::Set(Some(worker_id));
        active.lease_expires_at = ActiveValue::Set(Some(lease_expires_at));
        active.updated_at = ActiveValue::Set(now);
        let claimed = active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(Some(claimed.into()))
    }

    async fn acknowledge_fact_announcement(
        &self,
        report_id: &RecommendationReportId,
        worker_id: Uuid,
        announced_at: DateTime<Utc>,
    ) -> Result<ReportFactDeliveryInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let row = quant_report_fact_delivery::Entity::find_by_id(report_id.clone())
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(entity::QUANT_RECOMMENDATION_REPORT, report_id)
            })?;
        if row.status != ReportFactDeliveryStatus::Verified
            || row.announced_at.is_some()
            || row.claim_owner != Some(worker_id)
        {
            return Err(StorageError::state_conflict(
                entity::QUANT_RECOMMENDATION_REPORT,
                Some(report_id),
                "report fact announcement is not leased by this worker",
            ));
        }
        let mut active = row.into_active_model();
        active.announced_at = ActiveValue::Set(Some(announced_at));
        active.claim_owner = ActiveValue::Set(None);
        active.lease_expires_at = ActiveValue::Set(None);
        active.updated_at = ActiveValue::Set(announced_at);
        let updated = active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(updated.into())
    }

    async fn find_by_model_run_id(
        &self,
        model_run_id: &ModelRunId,
    ) -> Result<Option<RecommendationReportInfo>, StorageError> {
        quant_recommendation_report::Entity::find()
            .filter(quant_recommendation_report::Column::ModelRunId.eq(model_run_id.clone()))
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn list_committed_between(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<RecommendationReportInfo>, StorageError> {
        if to <= from {
            return Err(StorageError::invariant_violation(
                Some(entity::QUANT_RECOMMENDATION_REPORT),
                "committed report window requires to > from",
            ));
        }
        quant_recommendation_report::Entity::find()
            .filter(quant_recommendation_report::Column::DecisionAt.gte(from))
            .filter(quant_recommendation_report::Column::DecisionAt.lt(to))
            .order_by_asc(quant_recommendation_report::Column::DecisionAt)
            .order_by_asc(quant_recommendation_report::Column::RecommendationReportId)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn find_data_quality_snapshot(
        &self,
        report_id: &RecommendationReportId,
    ) -> Result<Option<ReportDataQualitySnapshotInfo>, StorageError> {
        let Some(snapshot_id) = quant_recommendation_report::Entity::find_by_id(report_id.clone())
            .select_only()
            .column(quant_recommendation_report::Column::DataQualitySnapshotRef)
            .into_tuple::<ReportDataQualitySnapshotId>()
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
        else {
            return Ok(None);
        };
        quant_report_data_quality_snapshot::Entity::find_by_id(snapshot_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn page(
        &self,
        query: QuantReportListQuery,
    ) -> Result<Paginated<RecommendationReportInfo>, StorageError> {
        paginate_mapped(
            quant_recommendation_report::Entity::find()
                .filter(page_condition(&query))
                .order_by_desc(quant_recommendation_report::Column::PublishedAt)
                .order_by_desc(quant_recommendation_report::Column::CreatedAt),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }

    async fn latest_published(
        &self,
        kind: ReportKind,
    ) -> Result<Option<RecommendationReportInfo>, StorageError> {
        quant_recommendation_report::Entity::find()
            .inner_join(quant_report_fact_delivery::Entity)
            .filter(quant_recommendation_report::Column::ReportKind.eq(kind))
            .filter(quant_recommendation_report::Column::Status.is_in([
                RecommendationReportStatus::Published,
                RecommendationReportStatus::PublishedEmpty,
            ]))
            .filter(
                quant_report_fact_delivery::Column::Status.eq(ReportFactDeliveryStatus::Verified),
            )
            .order_by_desc(quant_recommendation_report::Column::PublishedAt)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn find_by_trigger_key(
        &self,
        trigger_key: &str,
    ) -> Result<Option<RecommendationReportInfo>, StorageError> {
        quant_recommendation_report::Entity::find()
            .filter(quant_recommendation_report::Column::TriggerKey.eq(trigger_key))
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn find_actionable_ids_between(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<RecommendationReportId>, StorageError> {
        if to <= from {
            return Err(StorageError::invariant_violation(
                Some(entity::QUANT_RECOMMENDATION_REPORT),
                "actionable report window requires to > from",
            ));
        }
        quant_recommendation_report::Entity::find()
            .inner_join(quant_report_fact_delivery::Entity)
            .select_only()
            .column(quant_recommendation_report::Column::RecommendationReportId)
            .filter(quant_recommendation_report::Column::DecisionAt.gte(from))
            .filter(quant_recommendation_report::Column::DecisionAt.lt(to))
            .filter(quant_recommendation_report::Column::Status.is_in([
                RecommendationReportStatus::Published,
                RecommendationReportStatus::PublishedEmpty,
            ]))
            .filter(
                quant_report_fact_delivery::Column::Status.eq(ReportFactDeliveryStatus::Verified),
            )
            .order_by_asc(quant_recommendation_report::Column::DecisionAt)
            .into_tuple::<RecommendationReportId>()
            .all(&self.db)
            .await
            .map_err(StorageError::from)
    }

    async fn find_expirable(
        &self,
        now: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<RecommendationReportId>, StorageError> {
        quant_recommendation_report::Entity::find()
            .inner_join(quant_report_fact_delivery::Entity)
            .filter(quant_recommendation_report::Column::Status.is_in([
                RecommendationReportStatus::Published,
                RecommendationReportStatus::PublishedEmpty,
            ]))
            .filter(
                quant_report_fact_delivery::Column::Status.eq(ReportFactDeliveryStatus::Verified),
            )
            .filter(quant_recommendation_report::Column::ValidUntil.lte(now))
            .order_by_asc(quant_recommendation_report::Column::ValidUntil)
            .limit(limit)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| {
                rows.into_iter()
                    .map(|row| row.recommendation_report_id)
                    .collect()
            })
    }

    async fn roll_up_to_expired(
        &self,
        report_id: &RecommendationReportId,
        expired_at: DateTime<Utc>,
        operation_log: NewOperationLog,
    ) -> Result<Option<RecommendationReportInfo>, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;

        let Some(report) = quant_recommendation_report::Entity::find_by_id(report_id.clone())
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(error::not_found(
                entity::QUANT_RECOMMENDATION_REPORT,
                report_id,
            ));
        };
        if !matches!(
            report.status,
            RecommendationReportStatus::Published | RecommendationReportStatus::PublishedEmpty
        ) {
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(None);
        }

        // Roll up only when no recommendation is still actionable.
        let actionable = quant_recommendation::Entity::find()
            .filter(quant_recommendation::Column::RecommendationReportId.eq(report_id.clone()))
            .filter(quant_recommendation::Column::Status.is_in([
                RecommendationStatus::Published,
                RecommendationStatus::IntentCreated,
            ]))
            .count(&txn)
            .await
            .map_err(StorageError::from)?;
        if actionable > 0 {
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(None);
        }

        let before_info: RecommendationReportInfo = report.clone().into();
        let mut active = report.into_active_model();
        active.status = ActiveValue::Set(RecommendationReportStatus::Expired);
        active.status_reason = ActiveValue::Set(Some("ttl_expired".to_owned()));
        active.expired_at = ActiveValue::Set(Some(expired_at));
        let model = active.update(&txn).await.map_err(StorageError::from)?;
        let after_info: RecommendationReportInfo = model.clone().into();

        let operation_log =
            state_hash::apply_transition_hashes(operation_log, &before_info, &after_info)?;
        operation_log::Entity::insert(operation_log.into_active_model())
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;

        txn.commit().await.map_err(StorageError::from)?;
        Ok(Some(model.into()))
    }

    async fn revoke(
        &self,
        report_id: &RecommendationReportId,
        reason: &str,
        revoked_at: DateTime<Utc>,
        operation_log: NewOperationLog,
    ) -> Result<(RecommendationReportInfo, Vec<OrderIntentInfo>), StorageError> {
        transition_report_status(
            &self.db,
            report_id,
            RecommendationReportStatus::Revoked,
            RecommendationStatus::Revoked,
            reason,
            revoked_at,
            operation_log,
        )
        .await
    }
}

fn report_fact_retry_delay(attempt_count: i32) -> chrono::Duration {
    let exponent = u32::try_from(attempt_count.saturating_sub(1)).map_or(u32::MAX, identity);
    let seconds = 1_i64
        .checked_shl(exponent.min(62))
        .map_or(i64::MAX, identity)
        .min(REPORT_FACT_RETRY_MAX_SECS);
    chrono::Duration::seconds(seconds)
}

fn page_condition(query: &QuantReportListQuery) -> Condition {
    Condition::all()
        .add_option(
            query
                .kind
                .map(|kind| quant_recommendation_report::Column::ReportKind.eq(kind)),
        )
        .add_option(
            query
                .status
                .map(|status| quant_recommendation_report::Column::Status.eq(status)),
        )
        .add_option(
            query.trigger_kind.map(|trigger_kind| {
                quant_recommendation_report::Column::TriggerKind.eq(trigger_kind)
            }),
        )
        .add_option(
            query.runtime_mode.map(|runtime_mode| {
                quant_recommendation_report::Column::RuntimeMode.eq(runtime_mode)
            }),
        )
        .add_option(
            query
                .from
                .map(|from| quant_recommendation_report::Column::DecisionAt.gte(from)),
        )
        .add_option(
            query
                .to
                .map(|to| quant_recommendation_report::Column::DecisionAt.lt(to)),
        )
}

async fn transition_report_status(
    db: &DatabaseConnection,
    report_id: &RecommendationReportId,
    report_status: RecommendationReportStatus,
    recommendation_status: RecommendationStatus,
    reason: &str,
    occurred_at: DateTime<Utc>,
    operation_log: NewOperationLog,
) -> Result<(RecommendationReportInfo, Vec<OrderIntentInfo>), StorageError> {
    let txn = db.begin().await.map_err(StorageError::from)?;

    let Some(row) = quant_recommendation_report::Entity::find_by_id(report_id.clone())
        .lock_exclusive()
        .one(&txn)
        .await
        .map_err(StorageError::from)?
    else {
        return Err(error::not_found(
            entity::QUANT_RECOMMENDATION_REPORT,
            report_id,
        ));
    };
    let idempotent = row.status == report_status;
    if !idempotent
        && !matches!(
            row.status,
            RecommendationReportStatus::Published | RecommendationReportStatus::PublishedEmpty
        )
    {
        return Err(error::illegal_transition(
            entity::QUANT_RECOMMENDATION_REPORT,
            Some(report_id),
            row.status,
            report_status,
        ));
    }

    let recommendations = quant_recommendation::Entity::find()
        .filter(quant_recommendation::Column::RecommendationReportId.eq(report_id.clone()))
        .filter(quant_recommendation::Column::Status.is_in([
            RecommendationStatus::Published,
            RecommendationStatus::IntentCreated,
        ]))
        .order_by_asc(quant_recommendation::Column::RecommendationId)
        .lock_exclusive()
        .all(&txn)
        .await
        .map_err(StorageError::from)?;
    let invalidation = match report_status {
        RecommendationReportStatus::Revoked => ApprovalInvalidation::ReportRevoked,
        _ => {
            return Err(error::invariant_violation(
                Some(entity::QUANT_RECOMMENDATION_REPORT),
                "composite report terminal command only supports revoke",
            ));
        }
    };
    let mut invalidated_intents = Vec::new();
    for recommendation in recommendations {
        invalidated_intents.extend(
            invalidate_pre_submission_for_recommendation(
                &txn,
                &recommendation.recommendation_id,
                invalidation,
                occurred_at,
                &operation_log,
            )
            .await?,
        );
        let mut active = recommendation.into_active_model();
        active.status = ActiveValue::Set(recommendation_status);
        active.update(&txn).await.map_err(StorageError::from)?;
    }

    if idempotent {
        let info = row.into();
        txn.commit().await.map_err(StorageError::from)?;
        return Ok((info, invalidated_intents));
    }

    let before_info: RecommendationReportInfo = row.clone().into();
    let mut active = row.into_active_model();
    active.status = ActiveValue::Set(report_status);
    active.status_reason = ActiveValue::Set(Some(reason.to_owned()));
    active.revoked_at = ActiveValue::Set(Some(occurred_at));
    let report_model = active.update(&txn).await.map_err(StorageError::from)?;
    let after_info: RecommendationReportInfo = report_model.clone().into();

    let operation_log =
        state_hash::apply_transition_hashes(operation_log, &before_info, &after_info)?;
    operation_log::Entity::insert(operation_log.into_active_model())
        .exec(&txn)
        .await
        .map_err(StorageError::from)?;

    txn.commit().await.map_err(StorageError::from)?;
    Ok((report_model.into(), invalidated_intents))
}

#[cfg(test)]
mod tests {
    use super::{REPORT_FACT_RETRY_MAX_SECS, page_condition, report_fact_retry_delay};
    use quant_pivot_models::{
        domain::{QuantReportListQuery, pagination::PageRequest},
        entities::quant_recommendation_report,
        enums::quant::{
            QuantRuntimeMode, RecommendationReportStatus, ReportKind, ReportTriggerKind,
        },
    };
    use sea_orm::{DbBackend, EntityTrait, QueryFilter, QueryTrait};

    #[test]
    fn page_condition_adds_optional_filters_to_sql() {
        let query = QuantReportListQuery {
            kind: Some(ReportKind::TopN),
            status: Some(RecommendationReportStatus::Published),
            trigger_kind: Some(ReportTriggerKind::Scheduled),
            runtime_mode: Some(QuantRuntimeMode::ReportOnly),
            from: None,
            to: None,
            page: PageRequest::default(),
        };

        let sql = quant_recommendation_report::Entity::find()
            .filter(page_condition(&query))
            .build(DbBackend::Postgres)
            .to_string();

        assert!(sql.contains(r#""quant_recommendation_report"."report_kind" ="#));
        assert!(sql.contains(r#""quant_recommendation_report"."status" ="#));
        assert!(sql.contains(r#""quant_recommendation_report"."trigger_kind" ="#));
        assert!(sql.contains(r#""quant_recommendation_report"."runtime_mode" ="#));
    }

    #[test]
    fn report_fact_retry_backoff_is_bounded() {
        assert_eq!(report_fact_retry_delay(1).num_seconds(), 1);
        assert_eq!(report_fact_retry_delay(4).num_seconds(), 8);
        assert_eq!(
            report_fact_retry_delay(i32::MAX).num_seconds(),
            REPORT_FACT_RETRY_MAX_SECS
        );
    }
}
