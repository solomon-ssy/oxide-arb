//! Migration: create `report` table for daily/weekly trading report persistence.

use oxide_arb_models::idens::report::Report;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        super::migrate_schema(
            manager,
            create_tables(),
            create_indexes(),
            specials(manager),
        )
        .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        super::drop_tables(manager, drop_tables()).await
    }
}

fn create_tables() -> Vec<TableCreateStatement> {
    vec![
        Table::create()
            .table(Report::Table)
            .if_not_exists()
            .col(ColumnDef::new(Report::Id).text().not_null().primary_key())
            .col(ColumnDef::new(Report::ReportType).text().not_null())
            .col(ColumnDef::new(Report::PeriodStart).date().not_null())
            .col(ColumnDef::new(Report::PeriodEnd).date().not_null())
            .col(ColumnDef::new(Report::Payload).json_binary().not_null())
            .col(
                ColumnDef::new(Report::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .to_owned(),
    ]
}

fn create_indexes() -> Vec<IndexCreateStatement> {
    vec![
        Index::create()
            .name("idx_report_type_period")
            .table(Report::Table)
            .col(Report::ReportType)
            .col((Report::PeriodStart, IndexOrder::Desc))
            .to_owned(),
    ]
}

async fn specials(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    super::noop(manager).await
}

fn drop_tables() -> Vec<TableDropStatement> {
    vec![Table::drop().table(Report::Table).to_owned()]
}
