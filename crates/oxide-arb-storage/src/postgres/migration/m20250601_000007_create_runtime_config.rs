use super::migrate_up;
use oxide_arb_models::idens::runtime_config::RuntimeConfig;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        migrate_up(
            manager,
            create_tables(),
            create_indexes(),
            specials(manager),
            seeding_data(manager),
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
            .table(RuntimeConfig::Table)
            .if_not_exists()
            .col(
                ColumnDef::new(RuntimeConfig::Key)
                    .text()
                    .not_null()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(RuntimeConfig::Value)
                    .json_binary()
                    .not_null(),
            )
            .col(ColumnDef::new(RuntimeConfig::Description).text().null())
            .col(
                ColumnDef::new(RuntimeConfig::UpdatedBy)
                    .text()
                    .not_null()
                    .default("system"),
            )
            .col(super::timestamp_with_write_default(
                RuntimeConfig::UpdatedAt,
            ))
            .to_owned(),
    ]
}

const fn create_indexes() -> Vec<IndexCreateStatement> {
    Vec::new()
}

async fn specials(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    super::noop(manager).await
}

async fn seeding_data(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    super::noop(manager).await
}

fn drop_tables() -> Vec<TableDropStatement> {
    vec![Table::drop().table(RuntimeConfig::Table).to_owned()]
}
