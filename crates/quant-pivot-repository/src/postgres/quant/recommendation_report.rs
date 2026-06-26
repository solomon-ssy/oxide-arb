//! Postgres-backed recommendation report repository.

use crate::traits::RecommendationReportRepository;
use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{NewOperationLog, NewReportTransaction, RecommendationReportInfo},
    entities::{
        operation_log, quant_account_snapshot, quant_portfolio_plan, quant_recommendation,
        quant_recommendation_report,
    },
    enums::quant::{RecommendationReportStatus, RecommendationStatus, ReportKind},
    types::RecommendationReportId,
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel,
    QueryFilter, QueryOrder, TransactionTrait, sea_query::Expr,
};

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

    async fn expire(
        &self,
        report_id: &RecommendationReportId,
        reason: &str,
        expired_at: DateTime<Utc>,
        operation_log: NewOperationLog,
    ) -> Result<RecommendationReportInfo, StorageError> {
        transition_report_status(
            &self.db,
            report_id,
            RecommendationReportStatus::Expired,
            RecommendationStatus::Expired,
            reason,
            expired_at,
            operation_log,
        )
        .await
    }
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
        return Err(StorageError::Conflict(format!(
            "recommendation report not found: {report_id}"
        )));
    };
    if !matches!(
        row.status,
        RecommendationReportStatus::Published | RecommendationReportStatus::PublishedEmpty
    ) {
        return Err(StorageError::Conflict(format!(
            "cannot transition report {report_id} from status {} to {}",
            row.status.as_str(),
            report_status.as_str()
        )));
    }

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

    quant_recommendation::Entity::update_many()
        .filter(quant_recommendation::Column::RecommendationReportId.eq(report_id.clone()))
        .col_expr(
            quant_recommendation::Column::Status,
            Expr::value(recommendation_status),
        )
        .exec(&txn)
        .await
        .map_err(StorageError::from)?;

    operation_log::Entity::insert(operation_log.into_active_model())
        .exec(&txn)
        .await
        .map_err(StorageError::from)?;

    txn.commit().await.map_err(StorageError::from)?;
    Ok(report_model.into())
}
