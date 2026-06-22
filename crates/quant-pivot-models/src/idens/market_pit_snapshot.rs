use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ColumnType, Expr, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table,
        TableCreateStatement,
    },
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

#[oxide_schema(lifecycle = "audit")]
pub enum MarketPitSnapshot {
    Table,
    MarketPitSnapshotId,
    MarketId,
    EventId,
    Question,
    Slug,
    Categories,
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
    PayloadHash,
    ObservedAt,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    let mut table = Table::create();
    table.table(MarketPitSnapshot::Table).if_not_exists();
    add_identity_columns(&mut table);
    add_market_replay_columns(&mut table);
    add_fee_columns(&mut table);
    add_audit_columns(&mut table);
    table
        .foreign_key(
            ForeignKey::create()
                .name("fk_market_pit_snapshot_market")
                .from(MarketPitSnapshot::Table, MarketPitSnapshot::MarketId)
                .to(Market::Table, Market::MarketId)
                .on_delete(ForeignKeyAction::Cascade),
        )
        .to_owned()
}

fn add_identity_columns(table: &mut TableCreateStatement) {
    table
        .col(column::uuid_pk(MarketPitSnapshot::MarketPitSnapshotId))
        .col(column::market_id(MarketPitSnapshot::MarketId));
}

fn add_market_replay_columns(table: &mut TableCreateStatement) {
    table
        .col(column::text_id(MarketPitSnapshot::EventId))
        .col(
            ColumnDef::new(MarketPitSnapshot::Question)
                .text()
                .not_null(),
        )
        .col(ColumnDef::new(MarketPitSnapshot::Slug).text().not_null())
        .col(
            ColumnDef::new(MarketPitSnapshot::Categories)
                .array(ColumnType::Text)
                .not_null()
                .default(Expr::cust("'{}'::text[]")),
        )
        .col(ColumnDef::new(MarketPitSnapshot::Status).text().not_null())
        .col(ColumnDef::new(MarketPitSnapshot::Outcome).text().null())
        .col(column::token_id(MarketPitSnapshot::YesTokenId))
        .col(column::token_id(MarketPitSnapshot::NoTokenId))
        .col(
            ColumnDef::new(MarketPitSnapshot::TickSize)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(MarketPitSnapshot::NegRisk)
                .boolean()
                .not_null(),
        )
        .col(
            ColumnDef::new(MarketPitSnapshot::EndDate)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(MarketPitSnapshot::ResolvedAt)
                .timestamp_with_time_zone()
                .null(),
        );
}

fn add_fee_columns(table: &mut TableCreateStatement) {
    table
        .col(
            ColumnDef::new(MarketPitSnapshot::FeesEnabled)
                .boolean()
                .not_null(),
        )
        .col(
            ColumnDef::new(MarketPitSnapshot::FeeRate)
                .decimal_len(20, 18)
                .null(),
        )
        .col(
            ColumnDef::new(MarketPitSnapshot::FeeExponent)
                .decimal_len(20, 18)
                .null(),
        )
        .col(
            ColumnDef::new(MarketPitSnapshot::FeeTakerOnly)
                .boolean()
                .null(),
        )
        .col(
            ColumnDef::new(MarketPitSnapshot::FeeRebateRate)
                .decimal_len(20, 18)
                .null(),
        )
        .col(ColumnDef::new(MarketPitSnapshot::FeeSource).text().null())
        .col(
            ColumnDef::new(MarketPitSnapshot::FeeObservedAt)
                .timestamp_with_time_zone()
                .null(),
        );
}

fn add_audit_columns(table: &mut TableCreateStatement) {
    table
        .col(
            ColumnDef::new(MarketPitSnapshot::PayloadHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(MarketPitSnapshot::ObservedAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(timestamp_with_write_default(MarketPitSnapshot::CreatedAt));
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_market_pit_snapshot_market_observed",
        market_pit_snapshot_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_market_pit_snapshot_market_observed")
            .table(MarketPitSnapshot::Table)
            .col(MarketPitSnapshot::MarketId)
            .col((MarketPitSnapshot::ObservedAt, IndexOrder::Desc))
            .to_owned(),
        "market PIT snapshots by market and observation time",
    )]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![TableDependency::foreign_key(market_table_name)]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn market_pit_snapshot_table_name() -> String {
    MarketPitSnapshot::Table.to_string()
}

fn market_table_name() -> String {
    Market::Table.to_string()
}
