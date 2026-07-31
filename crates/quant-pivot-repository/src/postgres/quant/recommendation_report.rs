//! Postgres-backed recommendation report repository.

use std::{collections::HashSet, convert::identity};

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::storage::{
    StorageError,
    entity::{
        QUANT_FEATURE_PARITY_RUN, QUANT_RECOMMENDATION_REPORT, QUANT_REPORT_RUN, QUANT_RESEARCH_JOB,
    },
};
use quant_pivot_models::{
    domain::{
        api::QuantReportListQuery,
        governance::NewOperationLog,
        pagination::{PageWindow, Paginated},
        quant::{
            FactDeliverySettlement, NewEntryConditionArtifact, NewEntryConditionAudit,
            NewEntryConditionInstance, NewRecommendation, NewRecommendationReport,
            NewReportDataQualitySnapshot, NewReportFactDelivery, NewReportFeatureParity,
            NewReportTransaction, OrderIntentInfo, PreparedReportOutcome, PublishReportOutcome,
            RecommendationReportInfo, ReportDataQualitySnapshotInfo, ReportFactDeliveryInfo,
            ReportRunClaim,
        },
    },
    entities::{
        operation_log::Entity as OperationLogEntity,
        quant_account_snapshot::Entity as QuantAccountSnapshotEntity,
        quant_entry_condition_artifact::Entity as QuantEntryConditionArtifactEntity,
        quant_entry_condition_audit::Entity as QuantEntryConditionAuditEntity,
        quant_entry_condition_instance::Entity as QuantEntryConditionInstanceEntity,
        quant_feature_parity_run::Entity as QuantFeatureParityRunEntity,
        quant_portfolio_plan::Entity as QuantPortfolioPlanEntity,
        quant_recommendation::{Column, Entity as QuantRecommendationEntity},
        quant_recommendation_report::{
            Column as QuantRecommendationReportColumn, Entity as QuantRecommendationReportEntity,
            Model as QuantRecommendationReportModel,
        },
        quant_report_data_quality_snapshot::Entity as QuantReportDataQualitySnapshotEntity,
        quant_report_fact_delivery::{Column as QuantReportFactDeliveryColumn, Entity, Model},
        quant_report_run::Entity as QuantReportRunEntity,
        quant_research_job::Entity as QuantResearchJobEntity,
        research_profile_artifact::{
            Column as ResearchProfileArtifactColumn, Entity as ResearchProfileArtifactEntity,
        },
    },
    enums::{
        execution::ApprovalInvalidation,
        operation_log::{OperationCategory, OperationHttpMethod, OperationOutcome},
        quant::{
            EntryConditionAuditAction, EntryConditionState, FeatureParityRunKind,
            FeatureParityRunStatus, RecommendationReportStatus, RecommendationStatus,
            ReportFactDeliveryStatus, ReportKind, ReportRunStatus, ResearchJobKind,
            ResearchJobStatus,
        },
        rbac::ResourceType,
    },
    runtime_config::MAX_REPORT_TOP_N,
    types::{
        EntryConditionArtifactId, EntryConditionAuditId, EntryConditionPlan, ModelRunId,
        OperationDetailDocument, OperationLogId, RecommendationReportId, RecommendationTradePlan,
        ReportDataQualitySnapshotId, ResearchJobParams, ResearchProfileId, WorkerId,
    },
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, ConnectionTrait, DatabaseConnection,
    DatabaseTransaction, EntityTrait, IntoActiveModel, PaginatorTrait, QueryFilter, QueryOrder,
    QuerySelect, TransactionTrait,
    sea_query::{Expr, LockBehavior, LockType},
};

use super::equity_snapshot::insert_equity_snapshot_monotonic;
use crate::{
    postgres::{
        error, primitives,
        quant::{
            feature_parity::PgFeatureParityRepository, model_registry::PgModelRegistryRepository,
            order_intent::PgOrderIntentRepository, report_scope::ReportScope,
        },
        query::paginate_mapped,
        state_hash,
        write::insert_many_chunked,
    },
    traits::RecommendationReportRepository,
};

const REPORT_FACT_RETRY_MAX_SECS: i64 = 300;
const REPORT_FACT_ERROR_MAX_CHARS: usize = 4_096;

fn report_fact_lease_duration(lease_secs: u64) -> Result<Duration, StorageError> {
    let seconds = i64::try_from(lease_secs).map_err(|error| {
        StorageError::invariant_violation(
            Some(QUANT_RECOMMENDATION_REPORT),
            format!("report fact lease exceeds i64 seconds: {error}"),
        )
    })?;
    if seconds <= 0 {
        return Err(StorageError::invariant_violation(
            Some(QUANT_RECOMMENDATION_REPORT),
            "report fact lease must be greater than zero",
        ));
    }
    Ok(Duration::seconds(seconds))
}

/// Postgres-backed recommendation report repository.
pub struct PgRecommendationReportRepository {
    db: DatabaseConnection,
}

impl PgRecommendationReportRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

enum FactDeliveryClaim {
    Held(Model),
    Lost(Model),
}

impl PgRecommendationReportRepository {
    async fn fact_delivery_claim(
        txn: &DatabaseTransaction,
        report_id: &RecommendationReportId,
        worker_id: WorkerId,
        now: DateTime<Utc>,
    ) -> Result<FactDeliveryClaim, StorageError> {
        let row = Entity::find_by_id(*report_id)
            .lock_exclusive()
            .one(txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_RECOMMENDATION_REPORT, report_id))?;
        if row.status != ReportFactDeliveryStatus::Delivering
            || row.claim_owner != Some(worker_id)
            || row.lease_expires_at.is_none_or(|expires| expires <= now)
        {
            return Ok(FactDeliveryClaim::Lost(row));
        }
        Ok(FactDeliveryClaim::Held(row))
    }
}

fn publication_operation_log(
    action: &str,
    report_id: &RecommendationReportId,
    successor_report_id: Option<&RecommendationReportId>,
) -> Result<NewOperationLog, StorageError> {
    Ok(NewOperationLog {
        id: OperationLogId::from_v7(),
        request_id: format!("quant-report:{action}:{report_id}").into(),
        actor_user_id: None,
        actor_username: Some("system".to_owned()),
        acting_role: Some("report_fact_delivery".into()),
        category: OperationCategory::QuantReport,
        action: format!("report.{action}").into(),
        resource_type: Some(ResourceType::QuantReport),
        resource_id: Some(report_id.to_string()),
        http_method: OperationHttpMethod::System,
        http_path: format!("/system/quant/report/{report_id}/{action}"),
        http_status: 200,
        outcome: OperationOutcome::Success,
        client_ip: None,
        user_agent: None,
        latency_ms: 0,
        detail: OperationDetailDocument::from_serializable(&serde_json::json!({
            "successor_report_id": successor_report_id.map(ToString::to_string),
        }))
        .map_err(|error| StorageError::Codec(error.to_string()))?,
        before_hash: None,
        after_hash: None,
        governance_audit_event_id: None,
        governance_audit_sequence: None,
    })
}

impl PgRecommendationReportRepository {
    async fn insert_report_transition_log(
        txn: &DatabaseTransaction,
        action: &str,
        before: &QuantRecommendationReportModel,
        after: &QuantRecommendationReportModel,
        successor_report_id: Option<&RecommendationReportId>,
    ) -> Result<(), StorageError> {
        let before_info: RecommendationReportInfo = before.clone().into();
        let after_info: RecommendationReportInfo = after.clone().into();
        let log = state_hash::apply_transition_hashes(
            publication_operation_log(
                action,
                &before.recommendation_report_id,
                successor_report_id,
            )?,
            &before_info,
            &after_info,
        )?;
        OperationLogEntity::insert(log.into_active_model())
            .exec(txn)
            .await
            .map_err(StorageError::from)?;
        Ok(())
    }
}

fn validate_sampled_feature_parity(
    report: &NewRecommendationReport,
    parity: Option<&NewReportFeatureParity>,
) -> Result<(), StorageError> {
    let parity = parity.ok_or_else(|| {
        StorageError::invariant_violation(
            Some(QUANT_FEATURE_PARITY_RUN),
            "every committed report requires atomic sampled parity",
        )
    })?;
    let run = &parity.run;
    let job = &parity.job;
    let ResearchJobParams::FeatureParity(params) = &job.params_json else {
        return Err(StorageError::invariant_violation(
            Some(QUANT_RESEARCH_JOB),
            "sampled parity job requires typed feature parity params",
        ));
    };
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
            Some(QUANT_FEATURE_PARITY_RUN),
            "sampled parity run must be queued and bound to the exact report/model/decision window",
        ));
    }
    if job.kind != ResearchJobKind::FeatureParity
        || job.status != ResearchJobStatus::Queued
        || job.decision_policy_snapshot_id.as_ref() != Some(&report.decision_policy_snapshot_id)
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
            Some(QUANT_RESEARCH_JOB),
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
    let wrong_config = dq.decision_policy_snapshot_id != report.decision_policy_snapshot_id;
    if wrong_snapshot || wrong_decision || wrong_config {
        return Err(StorageError::invariant_violation(
            Some(QUANT_RECOMMENDATION_REPORT),
            "report DQ snapshot must bind the exact report decision and runtime config",
        ));
    }
    let mut vector_ids = HashSet::new();
    let mut markets = HashSet::new();
    for record in &dq.tokens_json.0 {
        let vector_id = record.feature_vector_id.as_ref().ok_or_else(|| {
            StorageError::invariant_violation(
                Some(QUANT_RECOMMENDATION_REPORT),
                format!(
                    "new report DQ row for market {} has no exact feature-vector binding",
                    record.market_id
                ),
            )
        })?;
        if !vector_ids.insert(*vector_id) || !markets.insert(record.market_id.clone()) {
            return Err(StorageError::invariant_violation(
                Some(QUANT_RECOMMENDATION_REPORT),
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
            Some(QUANT_RECOMMENDATION_REPORT),
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
            || !artifact_ids.insert(artifact.artifact_id)
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
        if !recommendation_ids.insert(instance.recommendation_id) {
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
        condition_instance_id: instance.condition_instance_id,
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

impl PgRecommendationReportRepository {
    async fn insert_sampled_feature_parity(
        txn: &DatabaseTransaction,
        parity: Option<NewReportFeatureParity>,
        report: &QuantRecommendationReportModel,
    ) -> Result<(), StorageError> {
        let Some(parity) = parity else {
            return Ok(());
        };
        let parity_key = report.recommendation_report_id.to_string();
        let run_id = parity.run.run_id;
        QuantFeatureParityRunEntity::insert(parity.run.into_active_model())
            .exec(txn)
            .await
            .map_err(|error| error::map_unique(error, QUANT_FEATURE_PARITY_RUN, &parity_key))?;
        PgFeatureParityRepository::insert_frozen_report_subject(txn, &run_id, report).await?;
        QuantResearchJobEntity::insert(parity.job.into_active_model())
            .exec(txn)
            .await
            .map_err(StorageError::from)?;
        Ok(())
    }
}

fn validate_report_transaction(
    transaction: &NewReportTransaction,
) -> Result<DateTime<Utc>, StorageError> {
    let max_top_n = usize::try_from(MAX_REPORT_TOP_N).map_err(|error| {
        StorageError::invariant_violation(
            Some(QUANT_RECOMMENDATION_REPORT),
            format!("report cascade budget does not fit this platform: {error}"),
        )
    })?;
    if transaction.report.top_n <= 0
        || i64::from(transaction.report.top_n) > i64::from(MAX_REPORT_TOP_N)
        || transaction.recommendations.len() > max_top_n
    {
        return Err(StorageError::invariant_violation(
            Some(QUANT_RECOMMENDATION_REPORT),
            format!("report recommendation set exceeds the {MAX_REPORT_TOP_N}-row cascade budget"),
        ));
    }
    if transaction.report.status != RecommendationReportStatus::Prepared
        || transaction.report.published_at.is_some()
        || transaction.report.successor_report_id.is_some()
        || transaction.report.superseded_at.is_some()
        || transaction.report.obsoleted_at.is_some()
        || transaction.report.revoked_at.is_some()
        || transaction.report.expired_at.is_some()
    {
        return Err(StorageError::invariant_violation(
            Some(QUANT_RECOMMENDATION_REPORT),
            "new report must be a scope-normalized Prepared artifact with no lifecycle timestamps",
        ));
    }
    if transaction.recommendations.iter().any(|recommendation| {
        recommendation.recommendation_report_id != transaction.report.recommendation_report_id
            || recommendation.research_profile_artifact_id
                != transaction.report.research_profile_artifact_id
            || recommendation.status != RecommendationStatus::Prepared
    }) {
        return Err(StorageError::invariant_violation(
            Some(QUANT_RECOMMENDATION_REPORT),
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
    Ok(transaction.report.decision_at)
}

fn validate_fact_delivery(
    report: &NewRecommendationReport,
    delivery: Option<&NewReportFactDelivery>,
) -> Result<(), StorageError> {
    let delivery = delivery.ok_or_else(|| {
        StorageError::invariant_violation(
            Some(QUANT_RECOMMENDATION_REPORT),
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
            Some(QUANT_RECOMMENDATION_REPORT),
            "report-fact delivery must be pending, non-empty, and bind the exact report id",
        ));
    }
    Ok(())
}

impl PgRecommendationReportRepository {
    async fn transition_recommendations_for_report(
        db: &impl ConnectionTrait,
        report_id: &RecommendationReportId,
        from: &[RecommendationStatus],
        to: RecommendationStatus,
    ) -> Result<u64, StorageError> {
        if from.is_empty() {
            return Ok(0);
        }
        QuantRecommendationEntity::update_many()
            .col_expr(Column::Status, Expr::value(to))
            .col_expr(Column::StatusChangedAt, Expr::cust("statement_timestamp()"))
            .filter(Column::RecommendationReportId.eq(*report_id))
            .filter(Column::Status.is_in(from.iter().copied()))
            .exec(db)
            .await
            .map(|result| result.rows_affected)
            .map_err(StorageError::from)
    }
}

impl PgRecommendationReportRepository {
    async fn obsolete_stale_candidate(
        txn: &DatabaseTransaction,
        candidate: QuantRecommendationReportModel,
        delivery: Model,
        current: &QuantRecommendationReportModel,
        now: DateTime<Utc>,
    ) -> Result<PublishReportOutcome, StorageError> {
        Self::transition_recommendations_for_report(
            txn,
            &candidate.recommendation_report_id,
            &[RecommendationStatus::Prepared],
            RecommendationStatus::Obsolete,
        )
        .await?;

        let mut report_active = candidate.clone().into_active_model();
        report_active.status = ActiveValue::Set(RecommendationReportStatus::Obsolete);
        report_active.successor_report_id =
            ActiveValue::Set(Some(current.recommendation_report_id));
        report_active.obsoleted_at = ActiveValue::Set(Some(now));
        report_active.status_reason = ActiveValue::Set(Some("not_newer_than_current".to_owned()));
        let obsolete = report_active
            .update(txn)
            .await
            .map_err(StorageError::from)?;

        let mut delivery_active = delivery.into_active_model();
        delivery_active.status = ActiveValue::Set(ReportFactDeliveryStatus::Cancelled);
        delivery_active.claim_owner = ActiveValue::Set(None);
        delivery_active.lease_expires_at = ActiveValue::Set(None);
        delivery_active.next_attempt_at = ActiveValue::Set(None);
        delivery_active.last_error = ActiveValue::Set(None);
        delivery_active.updated_at = ActiveValue::Set(now);
        let cancelled = delivery_active
            .update(txn)
            .await
            .map_err(StorageError::from)?;
        Self::insert_report_transition_log(
            txn,
            "obsolete",
            &candidate,
            &obsolete,
            Some(&current.recommendation_report_id),
        )
        .await?;

        Ok(PublishReportOutcome {
            report: obsolete.into(),
            delivery: cancelled.into(),
            superseded_reports: Vec::new(),
            obsoleted_reports: Vec::new(),
            invalidated_intents: Vec::new(),
        })
    }
}

impl PgRecommendationReportRepository {
    async fn publish_candidate_recommendations(
        txn: &DatabaseTransaction,
        report_id: &RecommendationReportId,
    ) -> Result<(), StorageError> {
        Self::transition_recommendations_for_report(
            txn,
            report_id,
            &[RecommendationStatus::Prepared],
            RecommendationStatus::Published,
        )
        .await?;
        Ok(())
    }
}

impl PgRecommendationReportRepository {
    async fn supersede_current_report(
        txn: &DatabaseTransaction,
        current: QuantRecommendationReportModel,
        successor_report_id: &RecommendationReportId,
        now: DateTime<Utc>,
    ) -> Result<(RecommendationReportInfo, Vec<OrderIntentInfo>), StorageError> {
        let parent_log = publication_operation_log(
            "supersede",
            &current.recommendation_report_id,
            Some(successor_report_id),
        )?;
        let recommendations = QuantRecommendationEntity::find()
            .filter(Column::RecommendationReportId.eq(current.recommendation_report_id))
            .filter(Column::Status.is_in([
                RecommendationStatus::Published,
                RecommendationStatus::IntentCreated,
            ]))
            .order_by_asc(Column::RecommendationId)
            .lock_exclusive()
            .all(txn)
            .await
            .map_err(StorageError::from)?;
        let mut invalidated_intents = Vec::new();
        for recommendation in &recommendations {
            invalidated_intents.extend(
                PgOrderIntentRepository::invalidate_pre_submission(
                    txn,
                    &recommendation.recommendation_id,
                    ApprovalInvalidation::ReportSuperseded,
                    now,
                    &parent_log,
                )
                .await?,
            );
        }
        Self::transition_recommendations_for_report(
            txn,
            &current.recommendation_report_id,
            &[
                RecommendationStatus::Published,
                RecommendationStatus::IntentCreated,
            ],
            RecommendationStatus::Superseded,
        )
        .await?;

        let mut current_active = current.clone().into_active_model();
        current_active.status = ActiveValue::Set(RecommendationReportStatus::Superseded);
        current_active.successor_report_id = ActiveValue::Set(Some(*successor_report_id));
        current_active.superseded_at = ActiveValue::Set(Some(now));
        current_active.status_reason = ActiveValue::Set(Some("newer_report_published".to_owned()));
        let superseded = current_active
            .update(txn)
            .await
            .map_err(StorageError::from)?;
        Self::insert_report_transition_log(
            txn,
            "supersede",
            &current,
            &superseded,
            Some(successor_report_id),
        )
        .await?;
        Ok((superseded.into(), invalidated_intents))
    }
}

impl PgRecommendationReportRepository {
    async fn obsolete_older_prepared_reports(
        txn: &DatabaseTransaction,
        published: &QuantRecommendationReportModel,
        now: DateTime<Utc>,
    ) -> Result<Vec<RecommendationReportInfo>, StorageError> {
        let older_prepared = QuantRecommendationReportEntity::find()
            .filter(
                QuantRecommendationReportColumn::ResearchProfileArtifactId
                    .eq(published.research_profile_artifact_id.clone()),
            )
            .filter(QuantRecommendationReportColumn::ReportKind.eq(published.report_kind))
            .filter(
                QuantRecommendationReportColumn::Status.eq(RecommendationReportStatus::Prepared),
            )
            .filter(
                QuantRecommendationReportColumn::RecommendationReportId
                    .ne(published.recommendation_report_id),
            )
            .filter(QuantRecommendationReportColumn::DecisionAt.lte(published.decision_at))
            .order_by_asc(QuantRecommendationReportColumn::DecisionAt)
            .order_by_asc(QuantRecommendationReportColumn::RecommendationReportId)
            .lock_exclusive()
            .all(txn)
            .await
            .map_err(StorageError::from)?;
        let mut obsoleted_reports = Vec::with_capacity(older_prepared.len());
        for older in older_prepared {
            Self::obsolete_prepared_report(txn, older, &published.recommendation_report_id, now)
                .await
                .map(|report| obsoleted_reports.push(report))?;
        }
        Ok(obsoleted_reports)
    }
}

impl PgRecommendationReportRepository {
    async fn obsolete_prepared_report(
        txn: &DatabaseTransaction,
        older: QuantRecommendationReportModel,
        successor_report_id: &RecommendationReportId,
        now: DateTime<Utc>,
    ) -> Result<RecommendationReportInfo, StorageError> {
        Self::transition_recommendations_for_report(
            txn,
            &older.recommendation_report_id,
            &[RecommendationStatus::Prepared],
            RecommendationStatus::Obsolete,
        )
        .await?;
        if let Some(older_delivery) = Entity::find_by_id(older.recommendation_report_id)
            .lock_exclusive()
            .one(txn)
            .await
            .map_err(StorageError::from)?
            && !matches!(
                older_delivery.status,
                ReportFactDeliveryStatus::Verified | ReportFactDeliveryStatus::Cancelled
            )
        {
            let mut active = older_delivery.into_active_model();
            active.status = ActiveValue::Set(ReportFactDeliveryStatus::Cancelled);
            active.claim_owner = ActiveValue::Set(None);
            active.lease_expires_at = ActiveValue::Set(None);
            active.next_attempt_at = ActiveValue::Set(None);
            active.updated_at = ActiveValue::Set(now);
            active.update(txn).await.map_err(StorageError::from)?;
        }
        let mut older_active = older.clone().into_active_model();
        older_active.status = ActiveValue::Set(RecommendationReportStatus::Obsolete);
        older_active.successor_report_id = ActiveValue::Set(Some(*successor_report_id));
        older_active.obsoleted_at = ActiveValue::Set(Some(now));
        older_active.status_reason =
            ActiveValue::Set(Some("newer_report_published_first".to_owned()));
        let obsolete = older_active.update(txn).await.map_err(StorageError::from)?;
        Self::insert_report_transition_log(
            txn,
            "obsolete",
            &older,
            &obsolete,
            Some(successor_report_id),
        )
        .await?;
        Ok(obsolete.into())
    }
}

impl PgRecommendationReportRepository {
    async fn verify_delivery(
        txn: &DatabaseTransaction,
        delivery: Model,
        now: DateTime<Utc>,
    ) -> Result<ReportFactDeliveryInfo, StorageError> {
        let mut delivery_active = delivery.into_active_model();
        delivery_active.status = ActiveValue::Set(ReportFactDeliveryStatus::Verified);
        delivery_active.claim_owner = ActiveValue::Set(None);
        delivery_active.lease_expires_at = ActiveValue::Set(None);
        delivery_active.next_attempt_at = ActiveValue::Set(None);
        delivery_active.last_error = ActiveValue::Set(None);
        delivery_active.verified_at = ActiveValue::Set(Some(now));
        delivery_active.updated_at = ActiveValue::Set(now);
        delivery_active
            .update(txn)
            .await
            .map(Into::into)
            .map_err(StorageError::from)
    }
}

#[async_trait::async_trait]
impl RecommendationReportRepository for PgRecommendationReportRepository {
    async fn create_prepared_report(
        &self,
        run_claim: ReportRunClaim,
        transaction: NewReportTransaction,
    ) -> Result<PreparedReportOutcome, StorageError> {
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
                Some(QUANT_RECOMMENDATION_REPORT),
                "report commit requires a durable feature-parity clear generation",
            )
        })?;

        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let commit_at = primitives::statement_timestamp(&txn).await?;
        let run = QuantReportRunEntity::find_by_id(run_claim.report_run_id)
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_REPORT_RUN, run_claim.report_run_id))?;
        if run.status != ReportRunStatus::Running
            || run.lease_owner != Some(run_claim.lease_owner)
            || run.lease_expires_at != Some(run_claim.lease_expires_at)
            || run_claim.lease_expires_at <= commit_at
            || run.output_report_id.is_some()
            || run.decision_policy_snapshot_id.as_ref() != Some(&report.decision_policy_snapshot_id)
            || run.decision_at != Some(report.decision_at)
            || run.top_n != Some(report.top_n)
        {
            return Err(StorageError::state_conflict(
                QUANT_REPORT_RUN,
                Some(&run_claim.report_run_id),
                "report prepare commit lost its exact running lease or frozen inputs",
            ));
        }
        PgFeatureParityRepository::verify_clear_latch_generation(&txn, &feature_parity_state_id)
            .await?;
        let profile_ref = report.research_profile_artifact_id.profile_ref();
        let profile_artifact_id =
            PgModelRegistryRepository::ensure_profile(&txn, &profile_ref).await?;
        if profile_artifact_id != report.research_profile_artifact_id {
            return Err(StorageError::invariant_violation(
                Some(QUANT_RECOMMENDATION_REPORT),
                "report profile identity differs from the canonical research artifact",
            ));
        }

        // Insert FK targets before the report header, then the report's children.
        QuantAccountSnapshotEntity::insert(account_snapshot.into_active_model())
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        insert_equity_snapshot_monotonic(&txn, equity_snapshot).await?;
        QuantReportDataQualitySnapshotEntity::insert(data_quality_snapshot.into_active_model())
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        QuantPortfolioPlanEntity::insert(portfolio_plan.into_active_model())
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        let report_model = QuantRecommendationReportEntity::insert(report.into_active_model())
            .exec_with_returning(&txn)
            .await
            .map_err(StorageError::from)?;
        let fact_delivery = fact_delivery.ok_or_else(|| {
            StorageError::invariant_violation(
                Some(QUANT_RECOMMENDATION_REPORT),
                "validated report-fact delivery disappeared before insert",
            )
        })?;
        Entity::insert(fact_delivery.into_active_model())
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        insert_many_chunked::<QuantEntryConditionArtifactEntity, _>(
            &txn,
            entry_condition_artifacts,
        )
        .await?;
        Self::insert_sampled_feature_parity(&txn, sampled_feature_parity, &report_model).await?;
        insert_many_chunked::<QuantRecommendationEntity, _>(&txn, recommendations).await?;
        if !entry_condition_instances.is_empty() {
            insert_many_chunked::<QuantEntryConditionInstanceEntity, _>(
                &txn,
                entry_condition_instances.clone(),
            )
            .await?;
            insert_many_chunked::<QuantEntryConditionAuditEntity, _>(
                &txn,
                entry_condition_instances
                    .iter()
                    .map(|instance| new_condition_audit(instance, condition_created_at))
                    .collect(),
            )
            .await?;
        }
        OperationLogEntity::insert(operation_log.into_active_model())
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;

        let mut run_active = run.into_active_model();
        run_active.status = ActiveValue::Set(ReportRunStatus::Succeeded);
        run_active.output_report_id = ActiveValue::Set(Some(report_model.recommendation_report_id));
        run_active.finished_at = ActiveValue::Set(Some(commit_at));
        run_active.lease_owner = ActiveValue::Set(None);
        run_active.lease_expires_at = ActiveValue::Set(None);
        let run_model = run_active.update(&txn).await.map_err(StorageError::from)?;

        txn.commit().await.map_err(StorageError::from)?;
        Ok(PreparedReportOutcome {
            report: report_model.into(),
            run: run_model.into(),
        })
    }

    async fn find_by_id(
        &self,
        report_id: &RecommendationReportId,
    ) -> Result<Option<RecommendationReportInfo>, StorageError> {
        QuantRecommendationReportEntity::find_by_id(*report_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn find_predecessor_id(
        &self,
        report_id: &RecommendationReportId,
    ) -> Result<Option<RecommendationReportId>, StorageError> {
        QuantRecommendationReportEntity::find()
            .filter(QuantRecommendationReportColumn::SuccessorReportId.eq(*report_id))
            .filter(
                QuantRecommendationReportColumn::Status.eq(RecommendationReportStatus::Superseded),
            )
            .order_by_desc(QuantRecommendationReportColumn::SupersededAt)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(|model| model.recommendation_report_id))
    }

    async fn find_fact_delivery(
        &self,
        report_id: &RecommendationReportId,
    ) -> Result<Option<ReportFactDeliveryInfo>, StorageError> {
        Entity::find_by_id(*report_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn claim_fact_delivery(
        &self,
        worker_id: WorkerId,
        lease_secs: u64,
    ) -> Result<Option<ReportFactDeliveryInfo>, StorageError> {
        let lease_duration = report_fact_lease_duration(lease_secs)?;
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let database_now = primitives::statement_timestamp(&txn).await?;
        let effective_lease_expires_at = database_now + lease_duration;
        let row = Entity::find()
            .filter(
                Condition::any()
                    .add(
                        QuantReportFactDeliveryColumn::Status.eq(ReportFactDeliveryStatus::Pending),
                    )
                    .add(
                        Condition::all()
                            .add(
                                QuantReportFactDeliveryColumn::Status
                                    .eq(ReportFactDeliveryStatus::Retrying),
                            )
                            .add(QuantReportFactDeliveryColumn::NextAttemptAt.lte(database_now)),
                    )
                    .add(
                        Condition::all()
                            .add(
                                QuantReportFactDeliveryColumn::Status
                                    .eq(ReportFactDeliveryStatus::Delivering),
                            )
                            .add(QuantReportFactDeliveryColumn::LeaseExpiresAt.lte(database_now)),
                    ),
            )
            .order_by_asc(QuantReportFactDeliveryColumn::CreatedAt)
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
                Some(QUANT_RECOMMENDATION_REPORT),
                "report fact delivery attempt count overflow",
            )
        })?;
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(ReportFactDeliveryStatus::Delivering);
        active.attempt_count = ActiveValue::Set(attempt_count);
        active.claim_owner = ActiveValue::Set(Some(worker_id));
        active.lease_expires_at = ActiveValue::Set(Some(effective_lease_expires_at));
        active.next_attempt_at = ActiveValue::Set(None);
        active.last_error = ActiveValue::Set(None);
        active.updated_at = ActiveValue::Set(database_now);
        let claimed = active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(Some(claimed.into()))
    }

    async fn fail_fact_delivery(
        &self,
        report_id: &RecommendationReportId,
        worker_id: WorkerId,
        status: ReportFactDeliveryStatus,
        error: &str,
    ) -> Result<FactDeliverySettlement<ReportFactDeliveryInfo>, StorageError> {
        if !matches!(
            status,
            ReportFactDeliveryStatus::Retrying | ReportFactDeliveryStatus::Failed
        ) {
            return Err(StorageError::invariant_violation(
                Some(QUANT_RECOMMENDATION_REPORT),
                "failed report fact delivery must transition to retrying or failed",
            ));
        }
        if error.trim().is_empty() {
            return Err(StorageError::invariant_violation(
                Some(QUANT_RECOMMENDATION_REPORT),
                "failed report fact delivery requires a non-empty diagnostic",
            ));
        }
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let now = primitives::statement_timestamp(&txn).await?;
        let row = match Self::fact_delivery_claim(&txn, report_id, worker_id, now).await? {
            FactDeliveryClaim::Held(row) => row,
            FactDeliveryClaim::Lost(row) => {
                txn.commit().await.map_err(StorageError::from)?;
                return Ok(FactDeliverySettlement::ClaimLost(Box::new(row.into())));
            }
        };
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
        Ok(FactDeliverySettlement::Applied(updated.into()))
    }

    async fn retry_fact_delivery(
        &self,
        report_id: &RecommendationReportId,
        occurred_at: DateTime<Utc>,
    ) -> Result<ReportFactDeliveryInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let report_probe = QuantRecommendationReportEntity::find_by_id(*report_id)
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_RECOMMENDATION_REPORT, report_id))?;
        ReportScope::new(
            &report_probe.research_profile_artifact_id.profile_ref().id,
            report_probe.report_kind,
        )
        .acquire(&txn)
        .await?;
        let now = primitives::statement_timestamp(&txn).await?;
        if occurred_at > now + Duration::seconds(5) {
            return Err(StorageError::invariant_violation(
                Some(QUANT_RECOMMENDATION_REPORT),
                "publication retry occurrence time cannot be in the future",
            ));
        }
        let report = QuantRecommendationReportEntity::find_by_id(*report_id)
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_RECOMMENDATION_REPORT, report_id))?;
        if report.status != RecommendationReportStatus::Prepared
            || report.research_profile_artifact_id != report_probe.research_profile_artifact_id
            || report.report_kind != report_probe.report_kind
        {
            return Err(StorageError::state_conflict(
                QUANT_RECOMMENDATION_REPORT,
                Some(report_id),
                "publication retry requires the same immutable Prepared report scope",
            ));
        }
        let delivery = Entity::find_by_id(*report_id)
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_RECOMMENDATION_REPORT, report_id))?;
        if delivery.status != ReportFactDeliveryStatus::Failed {
            return Err(StorageError::state_conflict(
                QUANT_RECOMMENDATION_REPORT,
                Some(report_id),
                "publication retry requires a Failed fact delivery",
            ));
        }
        let mut active = delivery.into_active_model();
        active.status = ActiveValue::Set(ReportFactDeliveryStatus::Retrying);
        active.claim_owner = ActiveValue::Set(None);
        active.lease_expires_at = ActiveValue::Set(None);
        active.next_attempt_at = ActiveValue::Set(Some(now));
        active.updated_at = ActiveValue::Set(now);
        let updated = active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(updated.into())
    }

    async fn verify_and_publish_report(
        &self,
        report_id: &RecommendationReportId,
        worker_id: WorkerId,
        occurred_at: DateTime<Utc>,
    ) -> Result<FactDeliverySettlement<PublishReportOutcome>, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let candidate_probe = QuantRecommendationReportEntity::find_by_id(*report_id)
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_RECOMMENDATION_REPORT, report_id))?;
        ReportScope::new(
            &candidate_probe
                .research_profile_artifact_id
                .profile_ref()
                .id,
            candidate_probe.report_kind,
        )
        .acquire(&txn)
        .await?;
        let now = primitives::statement_timestamp(&txn).await?;
        if occurred_at > now + Duration::seconds(5) {
            return Err(StorageError::invariant_violation(
                Some(QUANT_RECOMMENDATION_REPORT),
                "publication occurrence time cannot be in the future",
            ));
        }
        let delivery = match Self::fact_delivery_claim(&txn, report_id, worker_id, now).await? {
            FactDeliveryClaim::Held(row) => row,
            FactDeliveryClaim::Lost(row) => {
                txn.commit().await.map_err(StorageError::from)?;
                return Ok(FactDeliverySettlement::ClaimLost(Box::new(row.into())));
            }
        };
        let candidate = QuantRecommendationReportEntity::find_by_id(*report_id)
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_RECOMMENDATION_REPORT, report_id))?;
        if candidate.status != RecommendationReportStatus::Prepared {
            return Err(StorageError::illegal_transition(
                QUANT_RECOMMENDATION_REPORT,
                Some(report_id),
                candidate.status,
                RecommendationReportStatus::Published,
            ));
        }
        if candidate.research_profile_artifact_id != candidate_probe.research_profile_artifact_id
            || candidate.report_kind != candidate_probe.report_kind
        {
            return Err(StorageError::state_conflict(
                QUANT_RECOMMENDATION_REPORT,
                Some(report_id),
                "report scope changed while acquiring publication lock",
            ));
        }

        let current = QuantRecommendationReportEntity::find()
            .filter(
                QuantRecommendationReportColumn::ResearchProfileArtifactId
                    .eq(candidate.research_profile_artifact_id.clone()),
            )
            .filter(QuantRecommendationReportColumn::ReportKind.eq(candidate.report_kind))
            .filter(
                QuantRecommendationReportColumn::Status.eq(RecommendationReportStatus::Published),
            )
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?;

        if let Some(current) = current
            .as_ref()
            .filter(|row| row.decision_at >= candidate.decision_at)
        {
            let outcome =
                Self::obsolete_stale_candidate(&txn, candidate, delivery, current, now).await?;
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(FactDeliverySettlement::Applied(outcome));
        }

        Self::publish_candidate_recommendations(&txn, report_id).await?;
        let mut invalidated_intents = Vec::new();
        let mut superseded_reports = Vec::new();
        if let Some(current) = current {
            let (superseded, invalidated) =
                Self::supersede_current_report(&txn, current, report_id, now).await?;
            superseded_reports.push(superseded);
            invalidated_intents = invalidated;
        }

        // The prior current must leave the partial-unique Published set before
        // the candidate enters it. Both writes remain in this transaction.
        let mut candidate_active = candidate.clone().into_active_model();
        candidate_active.status = ActiveValue::Set(RecommendationReportStatus::Published);
        candidate_active.published_at = ActiveValue::Set(Some(now));
        candidate_active.status_reason = ActiveValue::Set(None);
        let published = candidate_active
            .update(&txn)
            .await
            .map_err(StorageError::from)?;
        Self::insert_report_transition_log(&txn, "publish", &candidate, &published, None).await?;

        let obsoleted_reports =
            Self::obsolete_older_prepared_reports(&txn, &published, now).await?;
        let verified = Self::verify_delivery(&txn, delivery, now).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(FactDeliverySettlement::Applied(PublishReportOutcome {
            report: published.into(),
            delivery: verified,
            superseded_reports,
            obsoleted_reports,
            invalidated_intents,
        }))
    }

    async fn claim_fact_announcement(
        &self,
        worker_id: WorkerId,
        lease_secs: u64,
    ) -> Result<Option<ReportFactDeliveryInfo>, StorageError> {
        let lease_duration = report_fact_lease_duration(lease_secs)?;
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let database_now = primitives::statement_timestamp(&txn).await?;
        let effective_lease_expires_at = database_now + lease_duration;
        let row = Entity::find()
            .filter(QuantReportFactDeliveryColumn::Status.eq(ReportFactDeliveryStatus::Verified))
            .filter(QuantReportFactDeliveryColumn::AnnouncedAt.is_null())
            .filter(
                Condition::any()
                    .add(QuantReportFactDeliveryColumn::LeaseExpiresAt.is_null())
                    .add(QuantReportFactDeliveryColumn::LeaseExpiresAt.lte(database_now)),
            )
            .order_by_asc(QuantReportFactDeliveryColumn::VerifiedAt)
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
        active.lease_expires_at = ActiveValue::Set(Some(effective_lease_expires_at));
        active.updated_at = ActiveValue::Set(database_now);
        let claimed = active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(Some(claimed.into()))
    }

    async fn acknowledge_fact_announcement(
        &self,
        report_id: &RecommendationReportId,
        worker_id: WorkerId,
    ) -> Result<ReportFactDeliveryInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let now = primitives::statement_timestamp(&txn).await?;
        let row = Entity::find_by_id(*report_id)
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_RECOMMENDATION_REPORT, report_id))?;
        if row.status != ReportFactDeliveryStatus::Verified
            || row.announced_at.is_some()
            || row.claim_owner != Some(worker_id)
            || row.lease_expires_at.is_none_or(|expires| expires <= now)
        {
            return Err(StorageError::state_conflict(
                QUANT_RECOMMENDATION_REPORT,
                Some(report_id),
                "report fact announcement is not leased by this worker",
            ));
        }
        let mut active = row.into_active_model();
        active.announced_at = ActiveValue::Set(Some(now));
        active.claim_owner = ActiveValue::Set(None);
        active.lease_expires_at = ActiveValue::Set(None);
        active.updated_at = ActiveValue::Set(now);
        let updated = active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(updated.into())
    }

    async fn find_by_model_run(
        &self,
        model_run_id: &ModelRunId,
    ) -> Result<Option<RecommendationReportInfo>, StorageError> {
        QuantRecommendationReportEntity::find()
            .filter(QuantRecommendationReportColumn::ModelRunId.eq(*model_run_id))
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
                Some(QUANT_RECOMMENDATION_REPORT),
                "committed report window requires to > from",
            ));
        }
        QuantRecommendationReportEntity::find()
            .filter(QuantRecommendationReportColumn::DecisionAt.gte(from))
            .filter(QuantRecommendationReportColumn::DecisionAt.lt(to))
            .order_by_asc(QuantRecommendationReportColumn::DecisionAt)
            .order_by_asc(QuantRecommendationReportColumn::RecommendationReportId)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn find_data_quality_snapshot(
        &self,
        report_id: &RecommendationReportId,
    ) -> Result<Option<ReportDataQualitySnapshotInfo>, StorageError> {
        let Some(snapshot_id) = QuantRecommendationReportEntity::find_by_id(*report_id)
            .select_only()
            .column(QuantRecommendationReportColumn::DataQualitySnapshotRef)
            .into_tuple::<ReportDataQualitySnapshotId>()
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
        else {
            return Ok(None);
        };
        QuantReportDataQualitySnapshotEntity::find_by_id(snapshot_id)
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
            QuantRecommendationReportEntity::find()
                .inner_join(ResearchProfileArtifactEntity)
                .filter(page_condition(&query))
                .order_by_desc(QuantRecommendationReportColumn::PublishedAt)
                .order_by_desc(QuantRecommendationReportColumn::CreatedAt),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }

    async fn current(
        &self,
        profile_id: &ResearchProfileId,
        kind: ReportKind,
    ) -> Result<Option<RecommendationReportInfo>, StorageError> {
        QuantRecommendationReportEntity::find()
            .inner_join(Entity)
            .inner_join(ResearchProfileArtifactEntity)
            .filter(ResearchProfileArtifactColumn::ResearchProfileId.eq(profile_id))
            .filter(QuantRecommendationReportColumn::ReportKind.eq(kind))
            .filter(
                QuantRecommendationReportColumn::Status.eq(RecommendationReportStatus::Published),
            )
            .filter(QuantReportFactDeliveryColumn::Status.eq(ReportFactDeliveryStatus::Verified))
            .order_by_desc(QuantRecommendationReportColumn::PublishedAt)
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
                Some(QUANT_RECOMMENDATION_REPORT),
                "actionable report window requires to > from",
            ));
        }
        QuantRecommendationReportEntity::find()
            .inner_join(Entity)
            .select_only()
            .column(QuantRecommendationReportColumn::RecommendationReportId)
            .filter(QuantRecommendationReportColumn::DecisionAt.gte(from))
            .filter(QuantRecommendationReportColumn::DecisionAt.lt(to))
            .filter(
                QuantRecommendationReportColumn::Status.eq(RecommendationReportStatus::Published),
            )
            .filter(QuantReportFactDeliveryColumn::Status.eq(ReportFactDeliveryStatus::Verified))
            .order_by_asc(QuantRecommendationReportColumn::DecisionAt)
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
        QuantRecommendationReportEntity::find()
            .inner_join(Entity)
            .filter(
                QuantRecommendationReportColumn::Status.eq(RecommendationReportStatus::Published),
            )
            .filter(QuantReportFactDeliveryColumn::Status.eq(ReportFactDeliveryStatus::Verified))
            .filter(QuantRecommendationReportColumn::ValidUntil.lte(now))
            .order_by_asc(QuantRecommendationReportColumn::ValidUntil)
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
        let probe = QuantRecommendationReportEntity::find_by_id(*report_id)
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_RECOMMENDATION_REPORT, report_id))?;
        ReportScope::new(
            &probe.research_profile_artifact_id.profile_ref().id,
            probe.report_kind,
        )
        .acquire(&txn)
        .await?;
        let now = primitives::statement_timestamp(&txn).await?;
        if expired_at > now + Duration::seconds(5) {
            return Err(StorageError::invariant_violation(
                Some(QUANT_RECOMMENDATION_REPORT),
                "report expiry occurrence time cannot be in the future",
            ));
        }

        let Some(report) = QuantRecommendationReportEntity::find_by_id(*report_id)
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(StorageError::not_found(
                QUANT_RECOMMENDATION_REPORT,
                report_id,
            ));
        };
        if report.research_profile_artifact_id != probe.research_profile_artifact_id
            || report.report_kind != probe.report_kind
        {
            return Err(StorageError::state_conflict(
                QUANT_RECOMMENDATION_REPORT,
                Some(report_id),
                "report scope changed while acquiring expiry lock",
            ));
        }
        if report.status != RecommendationReportStatus::Published {
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(None);
        }

        // Roll up only when every recommendation explicitly completes the
        // report roll-up contract; unknown/prepared/actionable states fail closed.
        let rollup_blockers = QuantRecommendationEntity::find()
            .filter(Column::RecommendationReportId.eq(*report_id))
            .filter(Column::Status.is_not_in(RecommendationStatus::REPORT_ROLLUP_COMPLETE))
            .count(&txn)
            .await
            .map_err(StorageError::from)?;
        if rollup_blockers > 0 {
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(None);
        }

        let before_info: RecommendationReportInfo = report.clone().into();
        let mut active = report.into_active_model();
        active.status = ActiveValue::Set(RecommendationReportStatus::Expired);
        active.status_reason = ActiveValue::Set(Some("ttl_expired".to_owned()));
        active.expired_at = ActiveValue::Set(Some(now));
        let model = active.update(&txn).await.map_err(StorageError::from)?;
        let after_info: RecommendationReportInfo = model.clone().into();

        let operation_log =
            state_hash::apply_transition_hashes(operation_log, &before_info, &after_info)?;
        OperationLogEntity::insert(operation_log.into_active_model())
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
        Box::pin(Self::transition_report_status(
            &self.db,
            report_id,
            RecommendationReportStatus::Revoked,
            RecommendationStatus::Revoked,
            reason,
            revoked_at,
            operation_log,
        ))
        .await
    }
}

fn report_fact_retry_delay(attempt_count: i32) -> Duration {
    let exponent = u32::try_from(attempt_count.saturating_sub(1)).map_or(u32::MAX, identity);
    let seconds = 1_i64
        .checked_shl(exponent.min(62))
        .map_or(i64::MAX, identity)
        .min(REPORT_FACT_RETRY_MAX_SECS);
    Duration::seconds(seconds)
}

fn page_condition(query: &QuantReportListQuery) -> Condition {
    Condition::all()
        .add_option(query.profile_id.as_ref().map(|profile_id| {
            ResearchProfileArtifactColumn::ResearchProfileId.eq(profile_id.clone())
        }))
        .add_option(
            query
                .kind
                .map(|kind| QuantRecommendationReportColumn::ReportKind.eq(kind)),
        )
        .add_option(
            query
                .status
                .map(|status| QuantRecommendationReportColumn::Status.eq(status)),
        )
        .add_option(
            query
                .runtime_mode
                .map(|runtime_mode| QuantRecommendationReportColumn::RuntimeMode.eq(runtime_mode)),
        )
        .add_option(
            query
                .from
                .map(|from| QuantRecommendationReportColumn::DecisionAt.gte(from)),
        )
        .add_option(
            query
                .to
                .map(|to| QuantRecommendationReportColumn::DecisionAt.lt(to)),
        )
}

impl PgRecommendationReportRepository {
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
        let probe = QuantRecommendationReportEntity::find_by_id(*report_id)
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_RECOMMENDATION_REPORT, report_id))?;
        ReportScope::new(
            &probe.research_profile_artifact_id.profile_ref().id,
            probe.report_kind,
        )
        .acquire(&txn)
        .await?;
        let now = primitives::statement_timestamp(&txn).await?;
        if occurred_at > now + Duration::seconds(5) {
            return Err(StorageError::invariant_violation(
                Some(QUANT_RECOMMENDATION_REPORT),
                "report lifecycle occurrence time cannot be in the future",
            ));
        }

        let Some(row) = QuantRecommendationReportEntity::find_by_id(*report_id)
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(StorageError::not_found(
                QUANT_RECOMMENDATION_REPORT,
                report_id,
            ));
        };
        if row.research_profile_artifact_id != probe.research_profile_artifact_id
            || row.report_kind != probe.report_kind
        {
            return Err(StorageError::state_conflict(
                QUANT_RECOMMENDATION_REPORT,
                Some(report_id),
                "report scope changed while acquiring lifecycle lock",
            ));
        }
        let idempotent = row.status == report_status;
        if !idempotent && !row.status.allows_transition_to(report_status) {
            return Err(StorageError::illegal_transition(
                QUANT_RECOMMENDATION_REPORT,
                Some(report_id),
                row.status,
                report_status,
            ));
        }

        let recommendations = QuantRecommendationEntity::find()
            .filter(Column::RecommendationReportId.eq(*report_id))
            .filter(Column::Status.is_in([
                RecommendationStatus::Prepared,
                RecommendationStatus::Published,
                RecommendationStatus::IntentCreated,
            ]))
            .order_by_asc(Column::RecommendationId)
            .lock_exclusive()
            .all(&txn)
            .await
            .map_err(StorageError::from)?;
        let invalidation = match report_status {
            RecommendationReportStatus::Revoked => ApprovalInvalidation::ReportRevoked,
            _ => {
                return Err(StorageError::invariant_violation(
                    Some(QUANT_RECOMMENDATION_REPORT),
                    "composite report terminal command only supports revoke",
                ));
            }
        };
        let mut invalidated_intents = Vec::new();
        for recommendation in &recommendations {
            invalidated_intents.extend(
                PgOrderIntentRepository::invalidate_pre_submission(
                    &txn,
                    &recommendation.recommendation_id,
                    invalidation,
                    now,
                    &operation_log,
                )
                .await?,
            );
        }
        Self::transition_recommendations_for_report(
            &txn,
            report_id,
            &[
                RecommendationStatus::Prepared,
                RecommendationStatus::Published,
                RecommendationStatus::IntentCreated,
            ],
            recommendation_status,
        )
        .await?;

        if idempotent {
            let info = row.into();
            txn.commit().await.map_err(StorageError::from)?;
            return Ok((info, invalidated_intents));
        }

        if let Some(delivery) = Entity::find_by_id(*report_id)
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            && !matches!(
                delivery.status,
                ReportFactDeliveryStatus::Verified | ReportFactDeliveryStatus::Cancelled
            )
        {
            let mut active = delivery.into_active_model();
            active.status = ActiveValue::Set(ReportFactDeliveryStatus::Cancelled);
            active.claim_owner = ActiveValue::Set(None);
            active.lease_expires_at = ActiveValue::Set(None);
            active.next_attempt_at = ActiveValue::Set(None);
            active.updated_at = ActiveValue::Set(now);
            active.update(&txn).await.map_err(StorageError::from)?;
        }

        let before_info: RecommendationReportInfo = row.clone().into();
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(report_status);
        active.status_reason = ActiveValue::Set(Some(reason.to_owned()));
        active.revoked_at = ActiveValue::Set(Some(now));
        let report_model = active.update(&txn).await.map_err(StorageError::from)?;
        let after_info: RecommendationReportInfo = report_model.clone().into();

        let operation_log =
            state_hash::apply_transition_hashes(operation_log, &before_info, &after_info)?;
        OperationLogEntity::insert(operation_log.into_active_model())
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;

        txn.commit().await.map_err(StorageError::from)?;
        Ok((report_model.into(), invalidated_intents))
    }
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::{
        domain::{api::QuantReportListQuery, pagination::PageRequest},
        entities::{
            quant_recommendation_report::Entity,
            research_profile_artifact::Entity as ResearchProfileArtifactEntity,
        },
        enums::quant::{QuantRuntimeMode, RecommendationReportStatus, ReportKind},
        types::ResearchProfileId,
    };
    use sea_orm::{DbBackend, EntityTrait, QueryFilter, QueryTrait};

    use super::{REPORT_FACT_RETRY_MAX_SECS, page_condition, report_fact_retry_delay};

    #[test]
    fn page_adds_optional_sql() {
        let query = QuantReportListQuery {
            kind: Some(ReportKind::TopN),
            status: Some(RecommendationReportStatus::Published),
            profile_id: Some(ResearchProfileId::new("weather_v1")),
            runtime_mode: Some(QuantRuntimeMode::ReportOnly),
            from: None,
            to: None,
            page: PageRequest::default(),
        };

        let sql = Entity::find()
            .inner_join(ResearchProfileArtifactEntity)
            .filter(page_condition(&query))
            .build(DbBackend::Postgres)
            .to_string();

        assert!(sql.contains(r#""quant_recommendation_report"."report_kind" ="#));
        assert!(sql.contains(r#""quant_recommendation_report"."status" ="#));
        assert!(sql.contains(r#""research_profile_artifact"."research_profile_id" ="#));
        assert!(sql.contains(r#""quant_recommendation_report"."runtime_mode" ="#));
    }

    #[test]
    fn report_fact_retry_bounded() {
        assert_eq!(report_fact_retry_delay(1).num_seconds(), 1);
        assert_eq!(report_fact_retry_delay(4).num_seconds(), 8);
        assert_eq!(
            report_fact_retry_delay(i32::MAX).num_seconds(),
            REPORT_FACT_RETRY_MAX_SECS
        );
    }
}
