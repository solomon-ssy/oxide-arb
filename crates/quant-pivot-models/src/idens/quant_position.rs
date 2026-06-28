use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Index, Table, TableCreateStatement},
};

use crate::{
    enums::{
        common::MarketCategory,
        execution::PositionLedgerState,
        quant::{AccountSource, OutcomeSide},
    },
    idens::{event::Event, market::Market, quant_order_intent::QuantOrderIntent},
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[quant_schema(lifecycle = "ledger")]
pub enum QuantPosition {
    Table,
    PositionId,
    OrderIntentId,
    TokenId,
    MarketId,
    EventId,
    Category,
    Side,
    State,
    Shares,
    AvgPrice,
    CostUsd,
    RealizedPnlUsd,
    Source,
    OpenedAt,
    UpdatedAt,
    ClosedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantPosition::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantPosition::PositionId))
        .col(column::uuid_fk(QuantPosition::OrderIntentId))
        .col(column::token_id(QuantPosition::TokenId))
        .col(column::market_id(QuantPosition::MarketId))
        .col(column::text_id_null(QuantPosition::EventId))
        .col(column::pg_enum::<MarketCategory>(QuantPosition::Category))
        .col(column::pg_enum::<OutcomeSide>(QuantPosition::Side))
        .col(column::pg_enum::<PositionLedgerState>(QuantPosition::State))
        .col(column::shares(QuantPosition::Shares))
        .col(column::price(QuantPosition::AvgPrice))
        .col(column::usd(QuantPosition::CostUsd))
        .col(column::usd(QuantPosition::RealizedPnlUsd))
        .col(column::pg_enum::<AccountSource>(QuantPosition::Source))
        .col(timestamp_with_write_default(QuantPosition::OpenedAt))
        .col(timestamp_with_write_default(QuantPosition::UpdatedAt))
        .col(
            ColumnDef::new(QuantPosition::ClosedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_position_intent")
                .from(QuantPosition::Table, QuantPosition::OrderIntentId)
                .to(QuantOrderIntent::Table, QuantOrderIntent::OrderIntentId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_position_market")
                .from(QuantPosition::Table, QuantPosition::MarketId)
                .to(Market::Table, Market::MarketId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_position_event")
                .from(QuantPosition::Table, QuantPosition::EventId)
                .to(Event::Table, Event::EventId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "uq_quant_position_intent",
            quant_position_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uq_quant_position_intent")
                .table(QuantPosition::Table)
                .col(QuantPosition::OrderIntentId)
                .unique()
                .to_owned(),
            "one position lot per entry intent",
        ),
        IndexSpec::sea_query(
            "idx_quant_position_token",
            quant_position_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_position_token")
                .table(QuantPosition::Table)
                .col(QuantPosition::TokenId)
                .to_owned(),
            "position lots by token (aggregate view)",
        ),
        IndexSpec::sea_query(
            "idx_quant_position_market",
            quant_position_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_position_market")
                .table(QuantPosition::Table)
                .col(QuantPosition::MarketId)
                .to_owned(),
            "positions by market",
        ),
        IndexSpec::sea_query(
            "idx_quant_position_state",
            quant_position_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_position_state")
                .table(QuantPosition::Table)
                .col(QuantPosition::State)
                .to_owned(),
            "positions by lifecycle state",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(quant_order_intent_table_name),
        TableDependency::foreign_key(market_table_name),
        TableDependency::foreign_key(event_table_name),
    ]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_position_table_name() -> String {
    QuantPosition::Table.to_string()
}

fn quant_order_intent_table_name() -> String {
    QuantOrderIntent::Table.to_string()
}

fn market_table_name() -> String {
    Market::Table.to_string()
}

fn event_table_name() -> String {
    Event::Table.to_string()
}
