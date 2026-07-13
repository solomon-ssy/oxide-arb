//! Postgres-backed recommendation report repository.

use super::equity_snapshot::insert_equity_snapshot_monotonic;
use crate::{
    postgres::{error, quant::feature_parity, query::paginate_mapped, state_hash},
    traits::RecommendationReportRepository,
};
use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{
        FeatureParityJobParams, NewOperationLog, NewRecommendationReport,
        NewReportDataQualitySnapshot, NewReportFeatureParity, NewReportTransaction, PageWindow,
        Paginated, QuantReportListQuery, RecommendationReportInfo, ReportDataQualitySnapshotInfo,
    },
    entities::{
        operation_log, quant_account_snapshot, quant_feature_parity_run, quant_portfolio_plan,
        quant_recommendation, quant_recommendation_report, quant_report_data_quality_snapshot,
        quant_research_job,
    },
    enums::quant::{
        FeatureParityRunKind, FeatureParityRunStatus, RecommendationReportStatus,
        RecommendationStatus, ReportKind, ResearchJobKind, ResearchJobStatus,
    },
    schema::column,
    types::{
        FeatureParityStateId, ModelRunId, RecommendationReportId, ReportDataQualitySnapshotId,
    },
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, DatabaseConnection, DatabaseTransaction,
    EntityTrait, IntoActiveModel, PaginatorTrait, QueryFilter, QueryOrder, QuerySelect,
    TransactionTrait,
};

use std::collections::HashSet;

/// Postgres-backed recommendation report repository.
pub struct PgRecommendationReportRepository {
    db: DatabaseConnection,
}

impl PgRecommendationReportRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
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

#[async_trait::async_trait]
impl RecommendationReportRepository for PgRecommendationReportRepository {
    async fn create_report(
        &self,
        transaction: NewReportTransaction,
    ) -> Result<RecommendationReportInfo, StorageError> {
        let NewReportTransaction {
            feature_parity_state_id,
            account_snapshot,
            equity_snapshot,
            data_quality_snapshot,
            portfolio_plan,
            report,
            recommendations,
            sampled_feature_parity,
            operation_log,
        } = transaction;

        validate_sampled_feature_parity(&report, sampled_feature_parity.as_ref())?;
        validate_report_data_quality(&report, &data_quality_snapshot)?;
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
        if let Some(parity) = sampled_feature_parity {
            let parity_key = report_model.recommendation_report_id.to_string();
            quant_feature_parity_run::Entity::insert(parity.run.into_active_model())
                .exec(&txn)
                .await
                .map_err(|error| {
                    error::map_unique(error, entity::QUANT_FEATURE_PARITY_RUN, &parity_key)
                })?;
            quant_research_job::Entity::insert(parity.job.into_active_model())
                .exec(&txn)
                .await
                .map_err(StorageError::from)?;
        }
        if !recommendations.is_empty() {
            let rows = recommendations
                .into_iter()
                .map(IntoActiveModel::into_active_model)
                .collect::<Vec<quant_recommendation::ActiveModel>>();
            quant_recommendation::Entity::insert_many(rows)
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
            .filter(quant_recommendation_report::Column::ReportKind.eq(kind))
            .filter(quant_recommendation_report::Column::Status.is_in([
                RecommendationReportStatus::Published,
                RecommendationReportStatus::PublishedEmpty,
            ]))
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
            .select_only()
            .column(quant_recommendation_report::Column::RecommendationReportId)
            .filter(quant_recommendation_report::Column::DecisionAt.gte(from))
            .filter(quant_recommendation_report::Column::DecisionAt.lt(to))
            .filter(quant_recommendation_report::Column::Status.is_in([
                RecommendationReportStatus::Published,
                RecommendationReportStatus::PublishedEmpty,
            ]))
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
            .filter(quant_recommendation_report::Column::Status.is_in([
                RecommendationReportStatus::Published,
                RecommendationReportStatus::PublishedEmpty,
            ]))
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
    ) -> Result<RecommendationReportInfo, StorageError> {
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
) -> Result<RecommendationReportInfo, StorageError> {
    let txn = db.begin().await.map_err(StorageError::from)?;

    let Some(row) = quant_recommendation_report::Entity::find_by_id(report_id.clone())
        .one(&txn)
        .await
        .map_err(StorageError::from)?
    else {
        return Err(error::not_found(
            entity::QUANT_RECOMMENDATION_REPORT,
            report_id,
        ));
    };
    if row.status == report_status {
        let info = row.into();
        txn.commit().await.map_err(StorageError::from)?;
        return Ok(info);
    }
    if !matches!(
        row.status,
        RecommendationReportStatus::Published | RecommendationReportStatus::PublishedEmpty
    ) {
        return Err(error::illegal_transition(
            entity::QUANT_RECOMMENDATION_REPORT,
            Some(report_id),
            row.status,
            report_status,
        ));
    }

    let before_info: RecommendationReportInfo = row.clone().into();
    let mut active = row.into_active_model();
    active.status = ActiveValue::Set(report_status);
    active.status_reason = ActiveValue::Set(Some(reason.to_owned()));
    match report_status {
        RecommendationReportStatus::Revoked => {
            active.revoked_at = ActiveValue::Set(Some(occurred_at));
        }
        RecommendationReportStatus::Expired => {
            active.expired_at = ActiveValue::Set(Some(occurred_at));
        }
        _ => {}
    }
    let report_model = active.update(&txn).await.map_err(StorageError::from)?;
    let after_info: RecommendationReportInfo = report_model.clone().into();

    // Only transition still-actionable recommendations; terminal ones
    // (`Executed` / `Attributed` / `Expired` / `Revoked`) are left intact.
    quant_recommendation::Entity::update_many()
        .filter(quant_recommendation::Column::RecommendationReportId.eq(report_id.clone()))
        .filter(quant_recommendation::Column::Status.is_in([
            RecommendationStatus::Published,
            RecommendationStatus::IntentCreated,
        ]))
        .col_expr(
            quant_recommendation::Column::Status,
            column::pg_enum_value(&recommendation_status),
        )
        .exec(&txn)
        .await
        .map_err(StorageError::from)?;

    let operation_log =
        state_hash::apply_transition_hashes(operation_log, &before_info, &after_info)?;
    operation_log::Entity::insert(operation_log.into_active_model())
        .exec(&txn)
        .await
        .map_err(StorageError::from)?;

    txn.commit().await.map_err(StorageError::from)?;
    Ok(report_model.into())
}

#[cfg(test)]
mod tests {
    use super::page_condition;
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
}
