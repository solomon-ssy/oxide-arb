use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table, TableCreateStatement,
    },
};

use crate::{
    idens::{event::Event, market::Market},
    schema::{
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
    },
};

#[oxide_schema]
pub enum Trade {
    Table,
    TradeId,
    ExecutionId,
    OpportunityId,
    MarketId,
    EventId,
    TokenId,
    Side,
    Shares,
    Price,
    CostUsd,
    FeeUsd,
    DetectedEdgeBps,
    DetectedProfitUsd,
    NetProfitUsd,
    OrderId,
    TxHash,
    Outcome,
    ExecutionMode,
    LatencyMs,
    ErrorMessage,
    ConfirmedAt,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
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
        .col(crate::schema::timestamp_with_write_default(
            Trade::CreatedAt,
        ))
        .col(crate::schema::timestamp_with_write_default(
            Trade::UpdatedAt,
        ))
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
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        simple_index(
            "idx_trades_execution_id",
            Trade::ExecutionId,
            "execution lookup",
        ),
        simple_index(
            "idx_trades_opportunity_id",
            Trade::OpportunityId,
            "opportunity lookup",
        ),
        simple_index("idx_trades_market_id", Trade::MarketId, "market lookup"),
        simple_index("idx_trades_event_id", Trade::EventId, "event lookup"),
        simple_index("idx_trades_outcome", Trade::Outcome, "outcome filters"),
        IndexSpec::sea_query(
            "idx_trades_created_at",
            trade_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_trades_created_at")
                .table(Trade::Table)
                .col((Trade::CreatedAt, IndexOrder::Desc))
                .to_owned(),
            "recent trade scans",
        ),
        IndexSpec::sea_query(
            "idx_trades_market_id_created",
            trade_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_trades_market_id_created")
                .table(Trade::Table)
                .col(Trade::MarketId)
                .col((Trade::CreatedAt, IndexOrder::Desc))
                .to_owned(),
            "market trade history ordered by recency",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(market_table_name),
        TableDependency::foreign_key(event_table_name),
    ]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn simple_index(name: &'static str, column: Trade, purpose: &'static str) -> IndexSpec {
    IndexSpec::sea_query(
        name,
        trade_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name(name)
            .table(Trade::Table)
            .col(column)
            .to_owned(),
        purpose,
    )
}

fn trade_table_name() -> String {
    Trade::Table.to_string()
}

fn market_table_name() -> String {
    Market::Table.to_string()
}

fn event_table_name() -> String {
    Event::Table.to_string()
}
