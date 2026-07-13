use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table, TableCreateStatement,
    },
};

use crate::{
    idens::{catalog_sync_batch::CatalogSyncBatch, event_catalog_version::EventCatalogVersion},
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

// A preparing batch may finalize only its temporal visibility columns. The
// migration installs a catalog-specific terminal immutability trigger; once
// the parent batch is committed or failed the row is WORM.
#[quant_schema(lifecycle = "ledger")]
pub enum MarketCatalogVersion {
    Table,
    MarketCatalogVersionId,
    CatalogSyncBatchId,
    EventCatalogVersionId,
    MarketId,
    EventId,
    SourceEffectiveAt,
    SourceTimestampQuality,
    SourceCreatedAt,
    AvailableAt,
    Origin,
    ContentHash,
    Payload,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(MarketCatalogVersion::Table)
        .if_not_exists()
        .col(column::uuid_pk(
            MarketCatalogVersion::MarketCatalogVersionId,
        ))
        .col(column::uuid_fk(MarketCatalogVersion::CatalogSyncBatchId))
        .col(column::uuid_fk(MarketCatalogVersion::EventCatalogVersionId))
        .col(column::market_id(MarketCatalogVersion::MarketId))
        .col(column::text_id(MarketCatalogVersion::EventId))
        .col(
            ColumnDef::new(MarketCatalogVersion::SourceEffectiveAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(MarketCatalogVersion::SourceTimestampQuality)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(MarketCatalogVersion::SourceCreatedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(MarketCatalogVersion::AvailableAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(MarketCatalogVersion::Origin)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(MarketCatalogVersion::ContentHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(MarketCatalogVersion::Payload)
                .json_binary()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            MarketCatalogVersion::CreatedAt,
        ))
        .foreign_key(
            ForeignKey::create()
                .name("fk_market_catalog_version_batch")
                .from(
                    MarketCatalogVersion::Table,
                    MarketCatalogVersion::CatalogSyncBatchId,
                )
                .to(
                    CatalogSyncBatch::Table,
                    CatalogSyncBatch::CatalogSyncBatchId,
                )
                .on_delete(ForeignKeyAction::Restrict),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_market_catalog_version_event_version")
                .from(
                    MarketCatalogVersion::Table,
                    MarketCatalogVersion::EventCatalogVersionId,
                )
                .to(
                    EventCatalogVersion::Table,
                    EventCatalogVersion::EventCatalogVersionId,
                )
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_market_catalog_version_batch",
            market_catalog_version_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_market_catalog_version_batch")
                .table(MarketCatalogVersion::Table)
                .col(MarketCatalogVersion::CatalogSyncBatchId)
                .to_owned(),
            "catalog version visibility finalization by batch",
        ),
        IndexSpec::sea_query(
            "idx_market_catalog_version_content",
            market_catalog_version_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_market_catalog_version_content")
                .table(MarketCatalogVersion::Table)
                .col(MarketCatalogVersion::MarketId)
                .col(MarketCatalogVersion::ContentHash)
                .to_owned(),
            "market catalog content audit lookup",
        ),
        IndexSpec::sea_query(
            "idx_market_catalog_version_pit",
            market_catalog_version_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_market_catalog_version_pit")
                .table(MarketCatalogVersion::Table)
                .col(MarketCatalogVersion::MarketId)
                .col((MarketCatalogVersion::SourceEffectiveAt, IndexOrder::Desc))
                .col((MarketCatalogVersion::AvailableAt, IndexOrder::Desc))
                .col((
                    MarketCatalogVersion::MarketCatalogVersionId,
                    IndexOrder::Desc,
                ))
                .to_owned(),
            "stable bitemporal market catalog lookup",
        ),
        IndexSpec::sea_query(
            "idx_market_catalog_version_event_pit",
            market_catalog_version_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_market_catalog_version_event_pit")
                .table(MarketCatalogVersion::Table)
                .col(MarketCatalogVersion::EventId)
                .col((MarketCatalogVersion::SourceEffectiveAt, IndexOrder::Desc))
                .to_owned(),
            "event membership lookup at a point in time",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(catalog_sync_batch_table_name),
        TableDependency::foreign_key(event_catalog_version_table_name),
    ]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn market_catalog_version_table_name() -> String {
    MarketCatalogVersion::Table.to_string()
}

fn catalog_sync_batch_table_name() -> String {
    CatalogSyncBatch::Table.to_string()
}

fn event_catalog_version_table_name() -> String {
    EventCatalogVersion::Table.to_string()
}
