use super::migrate_up;
use oxide_arb_models::idens::event::Event;
use oxide_arb_models::idens::market::Market;
use oxide_arb_models::idens::trade::Trade;
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
            .table(Trade::Table)
            .if_not_exists()
            .col(
                ColumnDef::new(Trade::TradeId)
                    .text()
                    .not_null()
                    .primary_key(),
            )
            .col(ColumnDef::new(Trade::ExecutionId).text().not_null())
            .col(ColumnDef::new(Trade::OpportunityId).text().not_null())
            .col(ColumnDef::new(Trade::MarketId).text().not_null())
            .col(ColumnDef::new(Trade::EventId).text().not_null())
            .col(ColumnDef::new(Trade::TokenId).text().not_null())
            .col(ColumnDef::new(Trade::Side).text().not_null())
            .col(ColumnDef::new(Trade::Shares).text().not_null())
            .col(ColumnDef::new(Trade::Price).text().not_null())
            .col(ColumnDef::new(Trade::CostUsd).text().not_null())
            .col(ColumnDef::new(Trade::FeeUsd).text().not_null())
            .col(ColumnDef::new(Trade::DetectedEdgeBps).text().null())
            .col(ColumnDef::new(Trade::DetectedProfitUsd).text().null())
            .col(ColumnDef::new(Trade::NetProfitUsd).text().null())
            .col(ColumnDef::new(Trade::OrderId).text().null())
            .col(ColumnDef::new(Trade::TxHash).text().null())
            .col(ColumnDef::new(Trade::Outcome).text().not_null())
            .col(ColumnDef::new(Trade::ExecutionMode).text().not_null())
            .col(ColumnDef::new(Trade::LatencyMs).integer().null())
            .col(ColumnDef::new(Trade::ErrorMessage).text().null())
            .col(
                ColumnDef::new(Trade::ConfirmedAt)
                    .timestamp_with_time_zone()
                    .null(),
            )
            .col(
                ColumnDef::new(Trade::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(Trade::UpdatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .foreign_key(
                ForeignKey::create()
                    .name("fk_trade_market")
                    .from(Trade::Table, Trade::MarketId)
                    .to(Market::Table, Market::MarketId)
                    .on_delete(ForeignKeyAction::Restrict),
            )
            .foreign_key(
                ForeignKey::create()
                    .name("fk_trade_event")
                    .from(Trade::Table, Trade::EventId)
                    .to(Event::Table, Event::EventId)
                    .on_delete(ForeignKeyAction::Restrict),
            )
            .to_owned(),
    ]
}

fn create_indexes() -> Vec<IndexCreateStatement> {
    vec![
        Index::create()
            .name("idx_trades_execution_id")
            .table(Trade::Table)
            .col(Trade::ExecutionId)
            .to_owned(),
        Index::create()
            .name("idx_trades_opportunity_id")
            .table(Trade::Table)
            .col(Trade::OpportunityId)
            .to_owned(),
        Index::create()
            .name("idx_trades_market_id")
            .table(Trade::Table)
            .col(Trade::MarketId)
            .to_owned(),
        Index::create()
            .name("idx_trades_event_id")
            .table(Trade::Table)
            .col(Trade::EventId)
            .to_owned(),
        Index::create()
            .name("idx_trades_outcome")
            .table(Trade::Table)
            .col(Trade::Outcome)
            .to_owned(),
        Index::create()
            .name("idx_trades_created_at")
            .table(Trade::Table)
            .col((Trade::CreatedAt, IndexOrder::Desc))
            .to_owned(),
        Index::create()
            .name("idx_trades_market_id_created")
            .table(Trade::Table)
            .col(Trade::MarketId)
            .col((Trade::CreatedAt, IndexOrder::Desc))
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
    vec![Table::drop().table(Trade::Table).to_owned()]
}
