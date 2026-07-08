use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table, TableCreateStatement,
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

// Append-only basis cross-check exceedance ledger (Phase 11.2.2 remediation
// R6). One row per `(market, as_of)` where the feature-source (Binance) vs
// settlement-oracle (Chainlink) basis exceeded the governed threshold —
// `domain.crypto.cross_check.max_basis_bps` at write time. `threshold_bps` is
// captured per row (not just read from live config) so historical alerts
// remain interpretable after an operator changes the threshold. The row
// itself is never updated once written except through the single governed
// `acknowledge` mutation (R6 review-queue closed loop): an operator marks an
// alert as triaged, recording who and when — this is the *only* mutation
// this ledger ever accepts; every other column is write-once.
#[quant_schema(lifecycle = "ledger")]
pub enum QuantBasisAlert {
    Table,
    AlertId,
    MarketId,
    InstrumentKey,
    OracleInstrumentKey,
    BasisBps,
    ThresholdBps,
    AsOf,
    Acknowledged,
    AcknowledgedAt,
    AcknowledgedBy,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantBasisAlert::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantBasisAlert::AlertId))
        .col(column::market_id(QuantBasisAlert::MarketId))
        .col(
            ColumnDef::new(QuantBasisAlert::InstrumentKey)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantBasisAlert::OracleInstrumentKey)
                .text()
                .not_null(),
        )
        .col(column::bps(QuantBasisAlert::BasisBps))
        .col(column::bps(QuantBasisAlert::ThresholdBps))
        .col(
            ColumnDef::new(QuantBasisAlert::AsOf)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantBasisAlert::Acknowledged)
                .boolean()
                .not_null()
                .default(false),
        )
        .col(
            ColumnDef::new(QuantBasisAlert::AcknowledgedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(QuantBasisAlert::AcknowledgedBy)
                .text()
                .null(),
        )
        .col(timestamp_with_write_default(QuantBasisAlert::CreatedAt))
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_basis_alert_market")
                .from(QuantBasisAlert::Table, QuantBasisAlert::MarketId)
                .to(Market::Table, Market::MarketId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_quant_basis_alert_market_as_of",
            quant_basis_alert_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_basis_alert_market_as_of")
                .table(QuantBasisAlert::Table)
                .col(QuantBasisAlert::MarketId)
                .col((QuantBasisAlert::AsOf, IndexOrder::Desc))
                .to_owned(),
            "per-market basis-alert history, newest first",
        ),
        IndexSpec::sea_query(
            "idx_quant_basis_alert_as_of",
            quant_basis_alert_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_basis_alert_as_of")
                .table(QuantBasisAlert::Table)
                .col((QuantBasisAlert::AsOf, IndexOrder::Desc))
                .to_owned(),
            "governance feed: recent basis alerts across all markets",
        ),
        IndexSpec::sea_query(
            "idx_quant_basis_alert_open",
            quant_basis_alert_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_basis_alert_open")
                .table(QuantBasisAlert::Table)
                .col(QuantBasisAlert::Acknowledged)
                .col((QuantBasisAlert::AsOf, IndexOrder::Desc))
                .to_owned(),
            "R6 review queue: unacknowledged alerts, newest first",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![TableDependency::foreign_key(market_table_name)]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_basis_alert_table_name() -> String {
    QuantBasisAlert::Table.to_string()
}

fn market_table_name() -> String {
    Market::Table.to_string()
}
