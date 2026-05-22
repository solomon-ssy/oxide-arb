//! `PostgreSQL` implementation of [`ReportRepository`].

use crate::traits::ReportRepository;
use chrono::NaiveDate;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::report::NewReport,
    entities::report::{self, Column as ReportColumn, Entity as ReportEntity},
    enums::common::ReportType,
};
use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter, QueryOrder,
    QuerySelect,
};

pub struct PgReportRepository {
    db: DatabaseConnection,
}

impl PgReportRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

impl ReportRepository for PgReportRepository {
    async fn create(&self, report: NewReport) -> Result<(), StorageError> {
        ReportEntity::insert(report.into_active_model())
            .on_conflict(
                OnConflict::column(ReportColumn::Id)
                    .update_columns([ReportColumn::Payload, ReportColumn::CreatedAt])
                    .to_owned(),
            )
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(())
    }

    async fn save_daily(
        &self,
        date: NaiveDate,
        payload: serde_json::Value,
    ) -> Result<(), StorageError> {
        self.create(NewReport::daily(date, payload)).await
    }

    async fn save_weekly(
        &self,
        week_start: NaiveDate,
        week_end: NaiveDate,
        payload: serde_json::Value,
    ) -> Result<(), StorageError> {
        self.create(NewReport::weekly(week_start, week_end, payload))
            .await
    }

    async fn find_by_type(
        &self,
        report_type: ReportType,
        limit: u64,
    ) -> Result<Vec<report::Model>, StorageError> {
        ReportEntity::find()
            .filter(ReportColumn::ReportType.eq(report_type))
            .order_by_desc(ReportColumn::PeriodStart)
            .limit(limit)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
    }

    async fn find_latest(
        &self,
        report_type: ReportType,
    ) -> Result<Option<report::Model>, StorageError> {
        ReportEntity::find()
            .filter(ReportColumn::ReportType.eq(report_type))
            .order_by_desc(ReportColumn::PeriodStart)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
    }
}
