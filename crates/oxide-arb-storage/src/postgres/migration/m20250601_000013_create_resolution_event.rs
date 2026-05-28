use super::migrate_up;
use oxide_arb_models::idens::{market::Market, resolution_event::ResolutionEvent};
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
            .table(ResolutionEvent::Table)
            .if_not_exists()
            .col(
                ColumnDef::new(ResolutionEvent::ResolutionId)
                    .text()
                    .not_null()
                    .primary_key(),
            )
            .col(ColumnDef::new(ResolutionEvent::MarketId).text().not_null())
            .col(ColumnDef::new(ResolutionEvent::Outcome).text().not_null())
            .col(ColumnDef::new(ResolutionEvent::Source).text().not_null())
            .col(
                ColumnDef::new(ResolutionEvent::GammaAgrees)
                    .boolean()
                    .null(),
            )
            .col(ColumnDef::new(ResolutionEvent::CtfAgrees).boolean().null())
            .col(
                ColumnDef::new(ResolutionEvent::Evidence)
                    .json_binary()
                    .null(),
            )
            .col(
                ColumnDef::new(ResolutionEvent::ResolvedAt)
                    .timestamp_with_time_zone()
                    .not_null(),
            )
            .col(super::timestamp_with_write_default(
                ResolutionEvent::CreatedAt,
            ))
            .foreign_key(
                ForeignKey::create()
                    .name("fk_resolution_market")
                    .from(ResolutionEvent::Table, ResolutionEvent::MarketId)
                    .to(Market::Table, Market::MarketId)
                    .on_delete(ForeignKeyAction::Restrict),
            )
            .to_owned(),
    ]
}

fn create_indexes() -> Vec<IndexCreateStatement> {
    vec![
        Index::create()
            .name("idx_resolution_market_created_at")
            .table(ResolutionEvent::Table)
            .col(ResolutionEvent::MarketId)
            .col(ResolutionEvent::CreatedAt)
            .to_owned(),
        Index::create()
            .name("idx_resolution_market_source_created_at")
            .table(ResolutionEvent::Table)
            .col(ResolutionEvent::MarketId)
            .col(ResolutionEvent::Source)
            .col(ResolutionEvent::CreatedAt)
            .to_owned(),
    ]
}

async fn specials(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    super::noop(manager).await
}

async fn seeding_data(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    super::noop(manager).await
}

fn drop_tables() -> Vec<TableDropStatement> {
    vec![Table::drop().table(ResolutionEvent::Table).to_owned()]
}
