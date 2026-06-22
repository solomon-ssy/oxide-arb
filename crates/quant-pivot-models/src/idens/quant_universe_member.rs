use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Index, Table, TableCreateStatement},
};

use crate::{
    idens::{event::Event, market::Market, quant_universe_snapshot::QuantUniverseSnapshot},
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
    },
};

#[quant_schema(lifecycle = "control")]
pub enum QuantUniverseMember {
    Table,
    UniverseSnapshotId,
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
        .table(QuantUniverseMember::Table)
        .if_not_exists()
        .col(column::uuid_fk(QuantUniverseMember::UniverseSnapshotId))
        .col(column::market_id(QuantUniverseMember::MarketId))
        .col(column::text_id(QuantUniverseMember::EventId))
        .col(
            ColumnDef::new(QuantUniverseMember::Category)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantUniverseMember::Status)
                .text()
                .not_null(),
        )
        .col(column::token_id(QuantUniverseMember::PrimaryTokenId))
        .col(column::token_id_null(QuantUniverseMember::SecondaryTokenId))
        .col(column::usd_null(QuantUniverseMember::LiquidityUsd))
        .col(column::usd_null(QuantUniverseMember::Volume24hUsd))
        .col(
            ColumnDef::new(QuantUniverseMember::Reason)
                .text()
                .not_null(),
        )
        .primary_key(
            Index::create()
                .col(QuantUniverseMember::UniverseSnapshotId)
                .col(QuantUniverseMember::MarketId),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_universe_member_snapshot")
                .from(
                    QuantUniverseMember::Table,
                    QuantUniverseMember::UniverseSnapshotId,
                )
                .to(
                    QuantUniverseSnapshot::Table,
                    QuantUniverseSnapshot::UniverseSnapshotId,
                )
                .on_delete(ForeignKeyAction::Cascade),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_universe_member_market")
                .from(QuantUniverseMember::Table, QuantUniverseMember::MarketId)
                .to(Market::Table, Market::MarketId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_universe_member_event")
                .from(QuantUniverseMember::Table, QuantUniverseMember::EventId)
                .to(Event::Table, Event::EventId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_quant_universe_member_market",
            quant_universe_member_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_universe_member_market")
                .table(QuantUniverseMember::Table)
                .col(QuantUniverseMember::MarketId)
                .to_owned(),
            "universe membership by market",
        ),
        IndexSpec::sea_query(
            "idx_quant_universe_member_event",
            quant_universe_member_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_universe_member_event")
                .table(QuantUniverseMember::Table)
                .col(QuantUniverseMember::EventId)
                .to_owned(),
            "universe membership by event",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(quant_universe_snapshot_table_name),
        TableDependency::foreign_key(market_table_name),
        TableDependency::foreign_key(event_table_name),
    ]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_universe_member_table_name() -> String {
    QuantUniverseMember::Table.to_string()
}

fn quant_universe_snapshot_table_name() -> String {
    QuantUniverseSnapshot::Table.to_string()
}

fn market_table_name() -> String {
    Market::Table.to_string()
}

fn event_table_name() -> String {
    Event::Table.to_string()
}
