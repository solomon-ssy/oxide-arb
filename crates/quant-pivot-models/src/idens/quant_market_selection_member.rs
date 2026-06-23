use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Index, Table, TableCreateStatement},
};

use crate::{
    idens::{event::Event, market::Market, quant_market_selection::QuantMarketSelection},
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
    },
};

#[quant_schema(lifecycle = "control")]
pub enum QuantMarketSelectionMember {
    Table,
    MarketSelectionId,
    MarketId,
    EventId,
    Category,
    Status,
    PrimaryTokenId,
    SecondaryTokenId,
    LiquidityUsd,
    Volume24hUsd,
    Reason,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantMarketSelectionMember::Table)
        .if_not_exists()
        .col(column::uuid_fk(
            QuantMarketSelectionMember::MarketSelectionId,
        ))
        .col(column::market_id(QuantMarketSelectionMember::MarketId))
        .col(column::text_id(QuantMarketSelectionMember::EventId))
        .col(
            ColumnDef::new(QuantMarketSelectionMember::Category)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantMarketSelectionMember::Status)
                .text()
                .not_null(),
        )
        .col(column::token_id(QuantMarketSelectionMember::PrimaryTokenId))
        .col(column::token_id_null(
            QuantMarketSelectionMember::SecondaryTokenId,
        ))
        .col(column::usd_null(QuantMarketSelectionMember::LiquidityUsd))
        .col(column::usd_null(QuantMarketSelectionMember::Volume24hUsd))
        .col(
            ColumnDef::new(QuantMarketSelectionMember::Reason)
                .text()
                .not_null(),
        )
        .primary_key(
            Index::create()
                .col(QuantMarketSelectionMember::MarketSelectionId)
                .col(QuantMarketSelectionMember::MarketId),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_market_selection_member_snapshot")
                .from(
                    QuantMarketSelectionMember::Table,
                    QuantMarketSelectionMember::MarketSelectionId,
                )
                .to(
                    QuantMarketSelection::Table,
                    QuantMarketSelection::MarketSelectionId,
                )
                .on_delete(ForeignKeyAction::Cascade),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_market_selection_member_market")
                .from(
                    QuantMarketSelectionMember::Table,
                    QuantMarketSelectionMember::MarketId,
                )
                .to(Market::Table, Market::MarketId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_market_selection_member_event")
                .from(
                    QuantMarketSelectionMember::Table,
                    QuantMarketSelectionMember::EventId,
                )
                .to(Event::Table, Event::EventId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_quant_market_selection_member_market",
            quant_market_selection_member_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_market_selection_member_market")
                .table(QuantMarketSelectionMember::Table)
                .col(QuantMarketSelectionMember::MarketId)
                .to_owned(),
            "selection membership by market",
        ),
        IndexSpec::sea_query(
            "idx_quant_market_selection_member_event",
            quant_market_selection_member_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_market_selection_member_event")
                .table(QuantMarketSelectionMember::Table)
                .col(QuantMarketSelectionMember::EventId)
                .to_owned(),
            "selection membership by event",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(quant_market_selection_table_name),
        TableDependency::foreign_key(market_table_name),
        TableDependency::foreign_key(event_table_name),
    ]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_market_selection_member_table_name() -> String {
    QuantMarketSelectionMember::Table.to_string()
}

fn quant_market_selection_table_name() -> String {
    QuantMarketSelection::Table.to_string()
}

fn market_table_name() -> String {
    Market::Table.to_string()
}

fn event_table_name() -> String {
    Event::Table.to_string()
}
