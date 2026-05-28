use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Index, Table, TableCreateStatement},
};

use crate::{
    enums::common::{PositionStatus, SettlementAccountingStatus},
    idens::{market::Market, trade::Trade},
    schema::{
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
    },
    types::Usd,
};

#[oxide_schema]
pub enum Position {
    Table,
    PositionId,
    TradeId,
    MarketId,
    TokenId,
    Side,
    Shares,
    AvgEntryPrice,
    TotalCostUsd,
    TotalFeesUsd,
    UnrealizedPnl,
    RealizedPnl,
    Status,
    OpenedAt,
    ClosedAt,
    SettledAt,
    WinningTokenId,
    SettlementPayoutUsd,
    RedeemTxHash,
    RedeemStatus,
    RedeemAttempts,
    OracleVerdict,
    SettlementTrigger,
    SettlementAccountingStatus,
    SettlementAccountingError,
    SettlementAccountedAt,
    RedeemTerminalReason,
}

pub fn table() -> TableCreateStatement {
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
        .col(crate::schema::timestamp_with_write_default(
            Position::OpenedAt,
        ))
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
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        simple_index(
            "idx_positions_market_id",
            Position::MarketId,
            "market lookup",
        ),
        simple_index("idx_positions_status", Position::Status, "status filters"),
        IndexSpec::sea_query(
            "idx_position_trade_id",
            position_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_position_trade_id")
                .table(Position::Table)
                .col(Position::TradeId)
                .unique()
                .to_owned(),
            "one position per trade",
        ),
        IndexSpec::raw(
            "idx_positions_open_market",
            position_table_name,
            IndexBuildMode::Transactional,
            "CREATE UNIQUE INDEX idx_positions_open_market \
             ON position (market_id, token_id, side) \
             WHERE status = 'open'",
            "prevent duplicate open positions per market/token/side",
        ),
        IndexSpec::raw(
            "idx_position_redeem_retry",
            position_table_name,
            IndexBuildMode::Transactional,
            "CREATE INDEX idx_position_redeem_retry \
             ON position (redeem_status) \
             WHERE status = 'open' AND redeem_status IN ('pending', 'failed')",
            "find open positions needing redeem retry",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(market_table_name),
        TableDependency::foreign_key(trade_table_name),
    ]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
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

fn simple_index(name: &'static str, column: Position, purpose: &'static str) -> IndexSpec {
    IndexSpec::sea_query(
        name,
        position_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name(name)
            .table(Position::Table)
            .col(column)
            .to_owned(),
        purpose,
    )
}

fn position_table_name() -> String {
    Position::Table.to_string()
}

fn market_table_name() -> String {
    Market::Table.to_string()
}

fn trade_table_name() -> String {
    Trade::Table.to_string()
}
