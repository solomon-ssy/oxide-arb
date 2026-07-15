use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Index, Table, TableCreateStatement},
};

use crate::{
    enums::{
        common::{MarketCategory, TickSize},
        market::MarketStatus,
    },
    idens::event::Event,
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[quant_schema(lifecycle = "core")]
pub enum Market {
    Table,
    MarketId,
    EventId,
    Question,
    Slug,
    /// Market rules text (resolution-source grounding anchor; 11.2.2).
    Description,
    Categories,
    Status,
    Outcome,
    YesTokenId,
    NoTokenId,
    TickSize,
    NegRisk,
    StartDate,
    EndDate,
    ResolvedAt,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(Market::Table)
        .if_not_exists()
        .col(column::market_id_pk(Market::MarketId))
        .col(column::text_id(Market::EventId))
        .col(ColumnDef::new(Market::Question).text().not_null())
        .col(ColumnDef::new(Market::Slug).text().not_null())
        .col(ColumnDef::new(Market::Description).text().null())
        .col(column::pg_enum_array::<MarketCategory>(Market::Categories))
        .col(column::pg_enum_default::<MarketStatus>(
            Market::Status,
            &MarketStatus::Active,
        ))
        .col(ColumnDef::new(Market::Outcome).text().null())
        .col(column::token_id(Market::YesTokenId))
        .col(column::token_id(Market::NoTokenId))
        .col(column::pg_enum_default::<TickSize>(
            Market::TickSize,
            &TickSize::Hundredth,
        ))
        .col(
            ColumnDef::new(Market::NegRisk)
                .boolean()
                .not_null()
                .default(false),
        )
        .col(
            ColumnDef::new(Market::StartDate)
                .timestamp_with_time_zone()
                .null(),
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
         WHERE status = 'active'::qp_market_status AND end_date IS NOT NULL",
        "scanner hot path for active endgame candidates",
    ));

    indexes.push(IndexSpec::raw(
        "idx_markets_categories",
        market_table_name,
        IndexBuildMode::Transactional,
        "CREATE INDEX IF NOT EXISTS idx_markets_categories \
         ON market USING GIN (categories)",
        "category membership filters on the qp_market_category[] column",
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
