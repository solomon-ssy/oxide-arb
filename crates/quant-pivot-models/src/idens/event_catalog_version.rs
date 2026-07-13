use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table, TableCreateStatement,
    },
};

use crate::{
    idens::catalog_sync_batch::CatalogSyncBatch,
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
pub enum EventCatalogVersion {
    Table,
    EventCatalogVersionId,
    CatalogSyncBatchId,
    EventId,
    SourceEffectiveAt,
    SourceTimestampQuality,
    AvailableAt,
    Origin,
    ContentHash,
    Payload,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(EventCatalogVersion::Table)
        .if_not_exists()
        .col(column::uuid_pk(EventCatalogVersion::EventCatalogVersionId))
        .col(column::uuid_fk(EventCatalogVersion::CatalogSyncBatchId))
        .col(column::text_id(EventCatalogVersion::EventId))
        .col(
            ColumnDef::new(EventCatalogVersion::SourceEffectiveAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(EventCatalogVersion::SourceTimestampQuality)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(EventCatalogVersion::AvailableAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(EventCatalogVersion::Origin)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(EventCatalogVersion::ContentHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(EventCatalogVersion::Payload)
                .json_binary()
                .not_null(),
        )
        .col(timestamp_with_write_default(EventCatalogVersion::CreatedAt))
        .foreign_key(
            ForeignKey::create()
                .name("fk_event_catalog_version_batch")
                .from(
                    EventCatalogVersion::Table,
                    EventCatalogVersion::CatalogSyncBatchId,
                )
                .to(
                    CatalogSyncBatch::Table,
                    CatalogSyncBatch::CatalogSyncBatchId,
                )
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_event_catalog_version_batch",
            event_catalog_version_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_event_catalog_version_batch")
                .table(EventCatalogVersion::Table)
                .col(EventCatalogVersion::CatalogSyncBatchId)
                .to_owned(),
            "catalog version visibility finalization by batch",
        ),
        IndexSpec::sea_query(
            "idx_event_catalog_version_content",
            event_catalog_version_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_event_catalog_version_content")
                .table(EventCatalogVersion::Table)
                .col(EventCatalogVersion::EventId)
                .col(EventCatalogVersion::ContentHash)
                .to_owned(),
            "event catalog content audit lookup",
        ),
        IndexSpec::sea_query(
            "idx_event_catalog_version_pit",
            event_catalog_version_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_event_catalog_version_pit")
                .table(EventCatalogVersion::Table)
                .col(EventCatalogVersion::EventId)
                .col((EventCatalogVersion::SourceEffectiveAt, IndexOrder::Desc))
                .col((EventCatalogVersion::AvailableAt, IndexOrder::Desc))
                .col((EventCatalogVersion::EventCatalogVersionId, IndexOrder::Desc))
                .to_owned(),
            "stable bitemporal event catalog lookup",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![TableDependency::foreign_key(catalog_sync_batch_table_name)]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn event_catalog_version_table_name() -> String {
    EventCatalogVersion::Table.to_string()
}

fn catalog_sync_batch_table_name() -> String {
    CatalogSyncBatch::Table.to_string()
}
