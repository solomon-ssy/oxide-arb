//! Migration: create `blacklist_entry` table for market/token blacklisting.

use oxide_arb_models::idens::blacklist_entry::BlacklistEntry;
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
            .table(BlacklistEntry::Table)
            .if_not_exists()
            .col(
                ColumnDef::new(BlacklistEntry::MarketId)
                    .text()
                    .not_null()
                    .primary_key(),
            )
            .col(ColumnDef::new(BlacklistEntry::TokenId).text().null())
            .col(ColumnDef::new(BlacklistEntry::Scope).text().not_null())
            .col(ColumnDef::new(BlacklistEntry::Reason).text().not_null())
            .col(
                ColumnDef::new(BlacklistEntry::ExpiresAt)
                    .timestamp_with_time_zone()
                    .null(),
            )
            .col(
                ColumnDef::new(BlacklistEntry::MissCount)
                    .integer()
                    .not_null()
                    .default(0),
            )
            .col(super::timestamp_with_write_default(
                BlacklistEntry::CreatedAt,
            ))
            .col(super::timestamp_with_write_default(
                BlacklistEntry::UpdatedAt,
            ))
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
    vec![Table::drop().table(BlacklistEntry::Table).to_owned()]
}
