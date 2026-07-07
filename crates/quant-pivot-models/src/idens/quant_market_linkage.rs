use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table, TableCreateStatement,
    },
};

use crate::{
    enums::domain::{DomainFamily, LinkageStatus, ResolverTier},
    idens::market::Market,
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

// Append-only, content-addressed, bitemporal market → external-subject linkage
// ledger (Phase 11.2.2). One row per resolver outcome for a
// `(market, metadata_hash, resolver_version, tier)` derivation; the PIT read
// picks the latest row with `derived_at <= as_of`. The full outcome (subject +
// instrument binding + grounding proof, or the unresolved reason) lives in one
// canonical JSONB payload; scalar provenance (family, status, tier, version,
// hashes, instrument key) is typed so governance queries never deserialize the
// payload. Rows are never updated or deleted (SCD2 knowledge history).
#[quant_schema(lifecycle = "ledger")]
pub enum QuantMarketLinkage {
    Table,
    LinkageId,
    MarketId,
    DomainFamily,
    Status,
    ResolverTier,
    ResolverVersion,
    Confidence,
    Outcome,
    InstrumentKey,
    MetadataHash,
    ContentHash,
    DerivedAt,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantMarketLinkage::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantMarketLinkage::LinkageId))
        .col(column::market_id(QuantMarketLinkage::MarketId))
        .col(column::pg_enum::<DomainFamily>(
            QuantMarketLinkage::DomainFamily,
        ))
        .col(column::pg_enum::<LinkageStatus>(QuantMarketLinkage::Status))
        .col(column::pg_enum::<ResolverTier>(
            QuantMarketLinkage::ResolverTier,
        ))
        .col(
            ColumnDef::new(QuantMarketLinkage::ResolverVersion)
                .integer()
                .not_null(),
        )
        .col(column::probability(QuantMarketLinkage::Confidence))
        .col(
            ColumnDef::new(QuantMarketLinkage::Outcome)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantMarketLinkage::InstrumentKey)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(QuantMarketLinkage::MetadataHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantMarketLinkage::ContentHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantMarketLinkage::DerivedAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(timestamp_with_write_default(QuantMarketLinkage::CreatedAt))
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_market_linkage_market")
                .from(QuantMarketLinkage::Table, QuantMarketLinkage::MarketId)
                .to(Market::Table, Market::MarketId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "uq_quant_market_linkage_content_hash",
            quant_market_linkage_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uq_quant_market_linkage_content_hash")
                .table(QuantMarketLinkage::Table)
                .col(QuantMarketLinkage::ContentHash)
                .unique()
                .to_owned(),
            "idempotent append: one row per content-addressed resolver outcome",
        ),
        IndexSpec::sea_query(
            "idx_quant_market_linkage_market_derived",
            quant_market_linkage_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_market_linkage_market_derived")
                .table(QuantMarketLinkage::Table)
                .col(QuantMarketLinkage::MarketId)
                .col((QuantMarketLinkage::DerivedAt, IndexOrder::Desc))
                .to_owned(),
            "PIT valid-at read: latest linkage per market at as_of",
        ),
        IndexSpec::sea_query(
            "idx_quant_market_linkage_status_derived",
            quant_market_linkage_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_market_linkage_status_derived")
                .table(QuantMarketLinkage::Table)
                .col(QuantMarketLinkage::Status)
                .col((QuantMarketLinkage::DerivedAt, IndexOrder::Desc))
                .to_owned(),
            "governance unresolved-queue and status filters",
        ),
        IndexSpec::sea_query(
            "idx_quant_market_linkage_instrument",
            quant_market_linkage_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_market_linkage_instrument")
                .table(QuantMarketLinkage::Table)
                .col(QuantMarketLinkage::InstrumentKey)
                .to_owned(),
            "linkages by external instrument (ingest-scope derivation)",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![TableDependency::foreign_key(market_table_name)]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_market_linkage_table_name() -> String {
    QuantMarketLinkage::Table.to_string()
}

fn market_table_name() -> String {
    Market::Table.to_string()
}
