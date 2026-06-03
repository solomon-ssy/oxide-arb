use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, Index, IndexOrder, Table, TableCreateStatement},
};

use crate::schema::{
    dependency::TableDependency,
    index::{IndexBuildMode, IndexSpec},
    seed::SeedSpec,
    timestamp_with_write_default,
};

#[oxide_schema(lifecycle = "audit")]
pub enum TokenBalanceSnapshot {
    Table,
    TokenBalanceSnapshotId,
    HolderAddress,
    MarketId,
    TokenId,
    Side,
    InternalShares,
    ExternalShares,
    DriftShares,
    Source,
    BlockNumber,
    ReconciliationReportId,
    ObservedAt,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(TokenBalanceSnapshot::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(TokenBalanceSnapshot::TokenBalanceSnapshotId)
                .text()
                .not_null()
                .primary_key(),
        )
        .col(
            ColumnDef::new(TokenBalanceSnapshot::HolderAddress)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(TokenBalanceSnapshot::MarketId)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(TokenBalanceSnapshot::TokenId)
                .text()
                .not_null(),
        )
        .col(ColumnDef::new(TokenBalanceSnapshot::Side).text().not_null())
        .col(
            ColumnDef::new(TokenBalanceSnapshot::InternalShares)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(TokenBalanceSnapshot::ExternalShares)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(TokenBalanceSnapshot::DriftShares)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(TokenBalanceSnapshot::Source)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(TokenBalanceSnapshot::BlockNumber)
                .big_integer()
                .null(),
        )
        .col(
            ColumnDef::new(TokenBalanceSnapshot::ReconciliationReportId)
                .big_integer()
                .null(),
        )
        .col(
            ColumnDef::new(TokenBalanceSnapshot::ObservedAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            TokenBalanceSnapshot::CreatedAt,
        ))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_token_balance_snapshot_token_observed",
        token_balance_snapshot_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_token_balance_snapshot_token_observed")
            .table(TokenBalanceSnapshot::Table)
            .col(TokenBalanceSnapshot::HolderAddress)
            .col(TokenBalanceSnapshot::MarketId)
            .col(TokenBalanceSnapshot::TokenId)
            .col((TokenBalanceSnapshot::ObservedAt, IndexOrder::Desc))
            .to_owned(),
        "token balance snapshots by holder, market, token, and observation time",
    )]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn token_balance_snapshot_table_name() -> String {
    TokenBalanceSnapshot::Table.to_string()
}
