use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Index, Table, TableCreateStatement},
};

use crate::{
    enums::{common::TickSize, market::MarketStatus},
    idens::event::Event,
    schema::{
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[oxide_schema(lifecycle = "core")]
pub enum Market {
    Table,
    MarketId,
    EventId,
    Question,
    Slug,
    Category,
    Status,
    Outcome,
    YesTokenId,
    NoTokenId,
    TickSize,
    NegRisk,
    EndDate,
    ResolvedAt,
    FeesEnabled,
    FeeRate,
    FeeExponent,
    FeeTakerOnly,
    FeeRebateRate,
    FeeSource,
    FeeObservedAt,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
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
        .col(timestamp_with_write_default(Market::CreatedAt))
        .col(timestamp_with_write_default(Market::UpdatedAt))
        .foreign_key(
            ForeignKey::create()
                .name("fk_market_event")
                .from(Market::Table, Market::EventId)
                .to(Event::Table, Event::EventId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    let simple = [
        (
            "idx_markets_event_id",
            Market::EventId,
            "market lookup by event",
        ),
        (
            "idx_markets_status",
            Market::Status,
            "market status filters",
        ),
        (
            "idx_markets_yes_token",
            Market::YesTokenId,
            "YES token lookup",
        ),
        ("idx_markets_no_token", Market::NoTokenId, "NO token lookup"),
    ];

    let mut indexes = simple
        .into_iter()
        .map(|(name, column, purpose)| {
            IndexSpec::sea_query(
                name,
                market_table_name,
                IndexBuildMode::Transactional,
                Index::create()
                    .name(name)
                    .table(Market::Table)
                    .col(column)
                    .to_owned(),
                purpose,
            )
        })
        .collect::<Vec<_>>();

    indexes.push(IndexSpec::raw(
        "idx_markets_active_endgame",
        market_table_name,
        IndexBuildMode::Transactional,
        "CREATE INDEX IF NOT EXISTS idx_markets_active_endgame \
         ON market (end_date) \
         WHERE status = 'active' AND end_date IS NOT NULL",
        "scanner hot path for active endgame candidates",
    ));

    indexes
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![TableDependency::foreign_key(event_table_name)]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn market_table_name() -> String {
    Market::Table.to_string()
}

fn event_table_name() -> String {
    Event::Table.to_string()
}
