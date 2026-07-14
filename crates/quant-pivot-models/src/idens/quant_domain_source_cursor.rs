use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, Index, Table, TableCreateStatement},
};

use crate::schema::{
    column,
    dependency::TableDependency,
    index::{IndexBuildMode, IndexSpec},
    seed::SeedSpec,
    timestamp_with_write_default,
};

// Durable ingest checkpoint for one `(source, instrument)` external domain
// stream (Phase 11.2.2). The cursor advances only after ClickHouse acknowledges
// the observation batch, so a crash never skips candles / oracle rounds.
#[quant_schema(lifecycle = "runtime")]
pub enum QuantDomainSourceCursor {
    Table,
    SourceId,
    InstrumentKey,
    CheckpointJson,
    CheckpointHash,
    Status,
    /// Human-readable detail from the most recent failed tick, or `NULL` when
    /// the last tick for this instrument succeeded (R10 ingest hardening).
    /// Cleared on the next successful tick — never accumulates stale errors.
    LastError,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantDomainSourceCursor::Table)
        .if_not_exists()
        .col(column::text_id(QuantDomainSourceCursor::SourceId))
        .col(column::text_id(QuantDomainSourceCursor::InstrumentKey))
        .col(
            ColumnDef::new(QuantDomainSourceCursor::CheckpointJson)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantDomainSourceCursor::CheckpointHash)
                .text()
                .not_null(),
        )
        .col(column::text_id(QuantDomainSourceCursor::Status))
        .col(
            ColumnDef::new(QuantDomainSourceCursor::LastError)
                .text()
                .null(),
        )
        .col(timestamp_with_write_default(
            QuantDomainSourceCursor::CreatedAt,
        ))
        .col(timestamp_with_write_default(
            QuantDomainSourceCursor::UpdatedAt,
        ))
        .primary_key(
            Index::create()
                .col(QuantDomainSourceCursor::SourceId)
                .col(QuantDomainSourceCursor::InstrumentKey),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_quant_domain_source_cursor_status",
        quant_domain_source_cursor_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_quant_domain_source_cursor_status")
            .table(QuantDomainSourceCursor::Table)
            .col(QuantDomainSourceCursor::Status)
            .to_owned(),
        "domain ingest health by cursor status",
    )]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_domain_source_cursor_table_name() -> String {
    QuantDomainSourceCursor::Table.to_string()
}
