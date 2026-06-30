use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Index, Table, TableCreateStatement},
};

use crate::{
    enums::{execution::SettlementRedeemState, quant::ExecutionWalletKind},
    idens::market::Market,
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[quant_schema(lifecycle = "ledger")]
pub enum QuantSettlementRedeem {
    Table,
    SettlementRedeemId,
    MarketId,
    FunderAddress,
    WalletKind,
    State,
    TxHash,
    IndexSetsJson,
    PayoutVectorJson,
    BalanceBeforeJson,
    BalanceAfterJson,
    PayoutUsd,
    GasFeePol,
    AttemptCount,
    NextAttemptAt,
    LastError,
    SubmittedAt,
    ConfirmedAt,
    FailedAt,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantSettlementRedeem::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantSettlementRedeem::SettlementRedeemId))
        .col(column::market_id(QuantSettlementRedeem::MarketId))
        .col(
            ColumnDef::new(QuantSettlementRedeem::FunderAddress)
                .text()
                .not_null(),
        )
        .col(column::pg_enum::<ExecutionWalletKind>(
            QuantSettlementRedeem::WalletKind,
        ))
        .col(column::pg_enum::<SettlementRedeemState>(
            QuantSettlementRedeem::State,
        ))
        .col(ColumnDef::new(QuantSettlementRedeem::TxHash).text().null())
        .col(
            ColumnDef::new(QuantSettlementRedeem::IndexSetsJson)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantSettlementRedeem::PayoutVectorJson)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantSettlementRedeem::BalanceBeforeJson)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantSettlementRedeem::BalanceAfterJson)
                .json_binary()
                .null(),
        )
        .col(column::usd_default_zero(QuantSettlementRedeem::PayoutUsd))
        .col(
            ColumnDef::new(QuantSettlementRedeem::GasFeePol)
                .decimal_len(38, 18)
                .null(),
        )
        .col(
            ColumnDef::new(QuantSettlementRedeem::AttemptCount)
                .integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(QuantSettlementRedeem::NextAttemptAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(QuantSettlementRedeem::LastError)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(QuantSettlementRedeem::SubmittedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(QuantSettlementRedeem::ConfirmedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(QuantSettlementRedeem::FailedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(timestamp_with_write_default(
            QuantSettlementRedeem::CreatedAt,
        ))
        .col(timestamp_with_write_default(
            QuantSettlementRedeem::UpdatedAt,
        ))
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_settlement_redeem_market")
                .from(
                    QuantSettlementRedeem::Table,
                    QuantSettlementRedeem::MarketId,
                )
                .to(Market::Table, Market::MarketId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "uq_quant_settlement_redeem_market_funder",
            quant_settlement_redeem_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uq_quant_settlement_redeem_market_funder")
                .table(QuantSettlementRedeem::Table)
                .col(QuantSettlementRedeem::MarketId)
                .col(QuantSettlementRedeem::FunderAddress)
                .unique()
                .to_owned(),
            "one system-managed redeem batch per condition and funder",
        ),
        IndexSpec::sea_query(
            "idx_quant_settlement_redeem_state_next_attempt",
            quant_settlement_redeem_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_settlement_redeem_state_next_attempt")
                .table(QuantSettlementRedeem::Table)
                .col(QuantSettlementRedeem::State)
                .col(QuantSettlementRedeem::NextAttemptAt)
                .to_owned(),
            "redeem worker retry scan",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![TableDependency::foreign_key(market_table_name)]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_settlement_redeem_table_name() -> String {
    QuantSettlementRedeem::Table.to_string()
}

fn market_table_name() -> String {
    Market::Table.to_string()
}
