//! Postgres-backed recommendation report repository.

use super::equity_snapshot::insert_equity_snapshot_monotonic;
use crate::{
    postgres::{error, state_hash},
    traits::RecommendationReportRepository,
};
use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{
        NewOperationLog, NewReportTransaction, PageWindow, Paginated, QuantReportListQuery,
        RecommendationReportInfo,
    },
    entities::{
        operation_log, quant_account_snapshot, quant_portfolio_plan, quant_recommendation,
        quant_recommendation_report, quant_report_data_quality_snapshot,
    },
    enums::quant::{RecommendationReportStatus, RecommendationStatus, ReportKind},
    schema::column,
    types::RecommendationReportId,
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, DatabaseConnection, EntityTrait,
    IntoActiveModel, PaginatorTrait, QueryFilter, QueryOrder, QuerySelect, TransactionTrait,
};

use crate::postgres::query::paginate_mapped;

/// Postgres-backed recommendation report repository.
pub struct PgRecommendationReportRepository {
    db: DatabaseConnection,
}

impl PgRecommendationReportRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl RecommendationReportRepository for PgRecommendationReportRepository {
    async fn create_report(
        &self,
        transaction: NewReportTransaction,
    ) -> Result<RecommendationReportInfo, StorageError> {
        let NewReportTransaction {
            account_snapshot,
            equity_snapshot,
            data_quality_snapshot,
            portfolio_plan,
            report,
            recommendations,
            operation_log,
        } = transaction;

        let txn = self.db.begin().await.map_err(StorageError::from)?;

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
                .map(|from| quant_recommendation_report::Column::CreatedAt.gte(from)),
        )
        .add_option(
            query
                .to
                .map(|to| quant_recommendation_report::Column::CreatedAt.lt(to)),
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
    use quant_pivot_models::entities::quant_recommendation_report;
    use quant_pivot_models::{
        domain::{QuantReportListQuery, pagination::PageRequest},
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
