use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, Expr, Index, IndexOrder, Table, TableCreateStatement},
};

use crate::schema::{
    column,
    dependency::TableDependency,
    index::{IndexBuildMode, IndexSpec},
    seed::SeedSpec,
};

/// Append-only ledger for one committed Gamma catalog synchronization.
#[quant_schema(lifecycle = "ledger")]
pub enum CatalogSyncBatch {
    Table,
    CatalogSyncBatchId,
    SyncKind,
    Status,
    SourceCursor,
    StartedAt,
    FetchedAt,
    CommittedAt,
    EventCount,
    MarketCount,
    RejectedCount,
    BatchHash,
    FailureStage,
    FailureDetail,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(CatalogSyncBatch::Table)
        .if_not_exists()
        .col(column::uuid_pk(CatalogSyncBatch::CatalogSyncBatchId))
        .col(ColumnDef::new(CatalogSyncBatch::SyncKind).text().not_null())
        .col(ColumnDef::new(CatalogSyncBatch::Status).text().not_null())
        .col(
            ColumnDef::new(CatalogSyncBatch::SourceCursor)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(CatalogSyncBatch::StartedAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(CatalogSyncBatch::FetchedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(CatalogSyncBatch::CommittedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(CatalogSyncBatch::EventCount)
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(CatalogSyncBatch::MarketCount)
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(CatalogSyncBatch::RejectedCount)
                .big_integer()
                .not_null(),
        )
        .col(ColumnDef::new(CatalogSyncBatch::BatchHash).text().null())
        .col(ColumnDef::new(CatalogSyncBatch::FailureStage).text().null())
        .col(
            ColumnDef::new(CatalogSyncBatch::FailureDetail)
                .text()
                .null(),
        )
        .col(crate::schema::timestamp_with_write_default(
            CatalogSyncBatch::CreatedAt,
        ))
        .col(crate::schema::timestamp_with_write_default(
            CatalogSyncBatch::UpdatedAt,
        ))
        .check(Expr::cust(
            "status IN ('preparing', 'committed', 'failed') AND \
             event_count >= 0 AND market_count >= 0 AND rejected_count >= 0 AND \
             ((status = 'preparing' AND fetched_at IS NOT NULL AND committed_at IS NULL AND \
               batch_hash IS NOT NULL AND failure_stage IS NULL AND failure_detail IS NULL) OR \
              (status = 'committed' AND fetched_at IS NOT NULL AND committed_at IS NOT NULL AND \
               batch_hash IS NOT NULL AND failure_stage IS NULL AND failure_detail IS NULL) OR \
              (status = 'failed' AND committed_at IS NULL AND \
               failure_stage IS NOT NULL AND length(failure_stage) BETWEEN 1 AND 64 AND \
               failure_detail IS NOT NULL AND length(failure_detail) BETWEEN 1 AND 2048))",
        ))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "uq_catalog_sync_batch_hash",
            catalog_sync_batch_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uq_catalog_sync_batch_hash")
                .table(CatalogSyncBatch::Table)
                .col(CatalogSyncBatch::BatchHash)
                .unique()
                .to_owned(),
            "idempotent catalog batch ingest",
        ),
        IndexSpec::sea_query(
            "idx_catalog_sync_batch_committed",
            catalog_sync_batch_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_catalog_sync_batch_committed")
                .table(CatalogSyncBatch::Table)
                .col(CatalogSyncBatch::Status)
                .col((CatalogSyncBatch::CommittedAt, IndexOrder::Desc))
                .to_owned(),
            "catalog coverage and latest committed batch",
        ),
        IndexSpec::sea_query(
            "idx_catalog_sync_batch_started",
            catalog_sync_batch_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_catalog_sync_batch_started")
                .table(CatalogSyncBatch::Table)
                .col((CatalogSyncBatch::StartedAt, IndexOrder::Desc))
                .to_owned(),
            "catalog attempt audit and abandoned preparation recovery",
        ),
    ]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn catalog_sync_batch_table_name() -> String {
    CatalogSyncBatch::Table.to_string()
}
