//! Migration: create `reconciliation_report` table for balance reconciliation results.

use oxide_arb_models::idens::reconciliation_report::ReconciliationReport;
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
            .table(ReconciliationReport::Table)
            .if_not_exists()
            .col(
                ColumnDef::new(ReconciliationReport::Id)
                    .big_integer()
                    .not_null()
                    .auto_increment()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(ReconciliationReport::Status)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(ReconciliationReport::Mismatches)
                    .json_binary()
                    .not_null(),
            )
            .col(
                ColumnDef::new(ReconciliationReport::InternalBalance)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(ReconciliationReport::ExternalBalance)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(ReconciliationReport::InternalExposure)
                    .text()
                    .not_null()
                    .default("0"),
            )
            .col(
                ColumnDef::new(ReconciliationReport::ExternalExposure)
                    .text()
                    .not_null()
                    .default("0"),
            )
            .col(
                ColumnDef::new(ReconciliationReport::Reserved)
                    .text()
                    .not_null()
                    .default("0"),
            )
            .col(
                ColumnDef::new(ReconciliationReport::Tolerance)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(ReconciliationReport::CheckedAt)
                    .timestamp_with_time_zone()
                    .not_null(),
            )
            .col(
                ColumnDef::new(ReconciliationReport::DurationMs)
                    .big_integer()
                    .not_null(),
            )
            .to_owned(),
    ]
}

const fn create_indexes() -> Vec<IndexCreateStatement> {
    vec![]
}

async fn specials(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    super::noop(manager).await
}

fn drop_tables() -> Vec<TableDropStatement> {
    vec![Table::drop().table(ReconciliationReport::Table).to_owned()]
}
