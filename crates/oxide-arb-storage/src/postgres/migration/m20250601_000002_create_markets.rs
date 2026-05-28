use super::{execute_sql, migrate_up};
use oxide_arb_models::{
    enums::{common::TickSize, market::MarketStatus},
    idens::{event::Event, market::Market},
};
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
            .table(Market::Table)
            .if_not_exists()
            .col(
                ColumnDef::new(Market::MarketId)
                    .text()
                    .not_null()
                    .primary_key(),
            )
            .col(ColumnDef::new(Market::EventId).text().not_null())
            .col(ColumnDef::new(Market::Question).text().not_null())
            .col(ColumnDef::new(Market::Slug).text().not_null())
            .col(ColumnDef::new(Market::Category).text().not_null())
            .col(
                ColumnDef::new(Market::Status)
                    .text()
                    .not_null()
                    .default(MarketStatus::Active),
            )
            .col(ColumnDef::new(Market::Outcome).text().null())
            .col(ColumnDef::new(Market::YesTokenId).text().not_null())
            .col(ColumnDef::new(Market::NoTokenId).text().not_null())
            .col(
                ColumnDef::new(Market::TickSize)
                    .text()
                    .not_null()
                    .default(TickSize::Hundredth),
            )
            .col(
                ColumnDef::new(Market::NegRisk)
                    .boolean()
                    .not_null()
                    .default(false),
            )
            .col(
                ColumnDef::new(Market::EndDate)
                    .timestamp_with_time_zone()
                    .null(),
            )
            .col(
                ColumnDef::new(Market::ResolvedAt)
                    .timestamp_with_time_zone()
                    .null(),
            )
            .col(
                ColumnDef::new(Market::FeesEnabled)
                    .boolean()
                    .not_null()
                    .default(true),
            )
            .col(ColumnDef::new(Market::FeeRate).decimal().null())
            .col(ColumnDef::new(Market::FeeExponent).decimal().null())
            .col(ColumnDef::new(Market::FeeTakerOnly).boolean().null())
            .col(ColumnDef::new(Market::FeeRebateRate).decimal().null())
            .col(ColumnDef::new(Market::FeeSource).text().null())
            .col(
                ColumnDef::new(Market::FeeObservedAt)
                    .timestamp_with_time_zone()
                    .null(),
            )
            .col(
                ColumnDef::new(Market::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(Market::UpdatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .foreign_key(
                ForeignKey::create()
                    .name("fk_market_event")
                    .from(Market::Table, Market::EventId)
                    .to(Event::Table, Event::EventId)
                    .on_delete(ForeignKeyAction::Restrict),
            )
            .to_owned(),
    ]
}

fn create_indexes() -> Vec<IndexCreateStatement> {
    vec![
        Index::create()
            .name("idx_markets_event_id")
            .table(Market::Table)
            .col(Market::EventId)
            .to_owned(),
        Index::create()
            .name("idx_markets_status")
            .table(Market::Table)
            .col(Market::Status)
            .to_owned(),
        Index::create()
            .name("idx_markets_yes_token")
            .table(Market::Table)
            .col(Market::YesTokenId)
            .to_owned(),
        Index::create()
            .name("idx_markets_no_token")
            .table(Market::Table)
            .col(Market::NoTokenId)
            .to_owned(),
    ]
}

async fn specials(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    // Endgame candidate scanning: status='active', end_date IS NOT NULL, end_date < ?
    execute_sql(
        manager,
        ["CREATE INDEX IF NOT EXISTS idx_markets_active_endgame \
         ON market (end_date) \
         WHERE status = 'active' AND end_date IS NOT NULL"],
    )
    .await
}

async fn seeding_data(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    super::noop(manager).await
}

fn drop_tables() -> Vec<TableDropStatement> {
    vec![Table::drop().table(Market::Table).to_owned()]
}
