use super::{execute_sql, migrate_up};
use oxide_arb_models::{
    enums::common::{PositionStatus, SettlementAccountingStatus},
    idens::{market::Market, position::Position, trade::Trade},
    types::Usd,
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
            .table(Position::Table)
            .if_not_exists()
            .col(
                ColumnDef::new(Position::PositionId)
                    .text()
                    .not_null()
                    .primary_key(),
            )
            .col(ColumnDef::new(Position::TradeId).text().not_null())
            .col(ColumnDef::new(Position::MarketId).text().not_null())
            .col(ColumnDef::new(Position::TokenId).text().not_null())
            .col(ColumnDef::new(Position::Side).text().not_null())
            .col(ColumnDef::new(Position::Shares).text().not_null())
            .col(ColumnDef::new(Position::AvgEntryPrice).text().not_null())
            .col(ColumnDef::new(Position::TotalCostUsd).text().not_null())
            .col(ColumnDef::new(Position::TotalFeesUsd).text().not_null())
            .col(default_zero_usd(Position::UnrealizedPnl))
            .col(default_zero_usd(Position::RealizedPnl))
            .col(
                ColumnDef::new(Position::Status)
                    .text()
                    .not_null()
                    .default(PositionStatus::Open),
            )
            .col(
                ColumnDef::new(Position::OpenedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(nullable_timestamp(Position::ClosedAt))
            .col(nullable_timestamp(Position::SettledAt))
            .col(ColumnDef::new(Position::WinningTokenId).text().null())
            .col(ColumnDef::new(Position::SettlementPayoutUsd).text().null())
            .col(ColumnDef::new(Position::RedeemTxHash).text().null())
            .col(
                ColumnDef::new(Position::RedeemStatus)
                    .text()
                    .not_null()
                    .default("not_required"),
            )
            .col(
                ColumnDef::new(Position::RedeemAttempts)
                    .integer()
                    .not_null()
                    .default(0),
            )
            .col(ColumnDef::new(Position::OracleVerdict).json_binary().null())
            .col(ColumnDef::new(Position::SettlementTrigger).text().null())
            .col(
                ColumnDef::new(Position::SettlementAccountingStatus)
                    .text()
                    .not_null()
                    .default(SettlementAccountingStatus::Pending),
            )
            .col(
                ColumnDef::new(Position::SettlementAccountingError)
                    .text()
                    .null(),
            )
            .col(nullable_timestamp(Position::SettlementAccountedAt))
            .col(ColumnDef::new(Position::RedeemTerminalReason).text().null())
            .foreign_key(
                ForeignKey::create()
                    .name("fk_position_market")
                    .from(Position::Table, Position::MarketId)
                    .to(Market::Table, Market::MarketId)
                    .on_delete(ForeignKeyAction::Restrict),
            )
            .foreign_key(
                ForeignKey::create()
                    .name("fk_position_trade")
                    .from(Position::Table, Position::TradeId)
                    .to(Trade::Table, Trade::TradeId)
                    .on_delete(ForeignKeyAction::Restrict),
            )
            .to_owned(),
    ]
}

fn default_zero_usd(column: Position) -> ColumnDef {
    let mut col = ColumnDef::new(column);
    col.text().not_null().default(Usd::ZERO);
    col
}

fn nullable_timestamp(column: Position) -> ColumnDef {
    let mut col = ColumnDef::new(column);
    col.timestamp_with_time_zone().null();
    col
}

fn create_indexes() -> Vec<IndexCreateStatement> {
    vec![
        Index::create()
            .name("idx_positions_market_id")
            .table(Position::Table)
            .col(Position::MarketId)
            .to_owned(),
        Index::create()
            .name("idx_positions_status")
            .table(Position::Table)
            .col(Position::Status)
            .to_owned(),
        Index::create()
            .name("idx_position_trade_id")
            .table(Position::Table)
            .col(Position::TradeId)
            .unique()
            .to_owned(),
    ]
}

async fn specials(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    execute_sql(
        manager,
        [
            "CREATE UNIQUE INDEX idx_positions_open_market \
         ON position (market_id, token_id, side) \
         WHERE status = 'open'",
            "CREATE INDEX idx_position_redeem_retry \
         ON position (redeem_status) \
         WHERE status = 'open' AND redeem_status IN ('pending', 'failed')",
        ],
    )
    .await
}

async fn seeding_data(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    super::noop(manager).await
}

fn drop_tables() -> Vec<TableDropStatement> {
    vec![Table::drop().table(Position::Table).to_owned()]
}
