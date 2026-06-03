use crate::traits::ReconciliationRepository;
use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{NewReconciliationReport, ReconciliationReportInfo},
    entities::reconciliation_report::{Column, Entity},
};
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter, QueryOrder,
};

pub struct PgReconciliationRepository {
    db: DatabaseConnection,
}

impl PgReconciliationRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl ReconciliationRepository for PgReconciliationRepository {
    async fn create(&self, report: NewReconciliationReport) -> Result<(), StorageError> {
        Entity::insert(report.into_active_model())
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(())
    }

    async fn latest_before(
        &self,
        before: DateTime<Utc>,
    ) -> Result<Option<ReconciliationReportInfo>, StorageError> {
        Entity::find()
            .filter(Column::CheckedAt.lte(before))
            .order_by_desc(Column::CheckedAt)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn find_between(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<ReconciliationReportInfo>, StorageError> {
        Entity::find()
            .filter(Column::CheckedAt.gte(start))
            .filter(Column::CheckedAt.lt(end))
            .order_by_asc(Column::CheckedAt)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }
}
