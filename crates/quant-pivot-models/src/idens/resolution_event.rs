use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Index, Table, TableCreateStatement},
};

use crate::{
    idens::market::Market,
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[quant_schema(lifecycle = "core")]
pub enum ResolutionEvent {
    Table,
    ResolutionId,
    MarketId,
    Outcome,
    Source,
    GammaAgrees,
    CtfAgrees,
    Evidence,
    ResolvedAt,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(ResolutionEvent::Table)
        .if_not_exists()
        .col(column::uuid_pk(ResolutionEvent::ResolutionId))
        .col(column::market_id(ResolutionEvent::MarketId))
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
        .col(timestamp_with_write_default(ResolutionEvent::CreatedAt))
        .foreign_key(
            ForeignKey::create()
                .name("fk_resolution_market")
                .from(ResolutionEvent::Table, ResolutionEvent::MarketId)
                .to(Market::Table, Market::MarketId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_resolution_market_created_at",
            resolution_event_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_resolution_market_created_at")
                .table(ResolutionEvent::Table)
                .col(ResolutionEvent::MarketId)
                .col(ResolutionEvent::CreatedAt)
                .to_owned(),
            "resolution events by market and creation time",
        ),
        IndexSpec::sea_query(
            "idx_resolution_market_source_created_at",
            resolution_event_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_resolution_market_source_created_at")
                .table(ResolutionEvent::Table)
                .col(ResolutionEvent::MarketId)
                .col(ResolutionEvent::Source)
                .col(ResolutionEvent::CreatedAt)
                .to_owned(),
            "resolution events by market/source/time",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![TableDependency::foreign_key(market_table_name)]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn resolution_event_table_name() -> String {
    ResolutionEvent::Table.to_string()
}

fn market_table_name() -> String {
    Market::Table.to_string()
}
