//! Migration: create `emergency_snapshot` table for circuit-breaker state captures.

use oxide_arb_models::idens::emergency_snapshot::EmergencySnapshot;
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
            .table(EmergencySnapshot::Table)
            .if_not_exists()
            .col(
                ColumnDef::new(EmergencySnapshot::Id)
                    .big_integer()
                    .not_null()
                    .auto_increment()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(EmergencySnapshot::TriggerLevel)
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(EmergencySnapshot::Reason).text().not_null())
            .col(
                ColumnDef::new(EmergencySnapshot::RiskState)
                    .json_binary()
                    .not_null(),
            )
            .col(
                ColumnDef::new(EmergencySnapshot::OpenPositionsCount)
                    .integer()
                    .not_null(),
            )
            .col(
                ColumnDef::new(EmergencySnapshot::OpenReservationsCount)
                    .integer()
                    .not_null(),
            )
            .col(
                ColumnDef::new(EmergencySnapshot::TriggeredAt)
                    .timestamp_with_time_zone()
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
    vec![Table::drop().table(EmergencySnapshot::Table).to_owned()]
}
