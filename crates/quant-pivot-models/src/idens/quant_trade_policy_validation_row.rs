use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, Expr, ForeignKey, ForeignKeyAction, Index, Table, TableCreateStatement,
    },
};

use crate::{
    idens::quant_trade_policy_validation::QuantTradePolicyValidation,
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[quant_schema(lifecycle = "audit")]
pub enum QuantTradePolicyValidationRow {
    Table,
    ValidationRunId,
    RowOrdinal,
    EvidenceKind,
    RecordKey,
    ExampleId,
    MarketId,
    TokenId,
    DecisionAt,
    ExpectedRowHash,
    ActualRowHash,
    Passed,
    DiagnosticKind,
    Detail,
    RowHash,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantTradePolicyValidationRow::Table)
        .if_not_exists()
        .col(column::uuid_fk(
            QuantTradePolicyValidationRow::ValidationRunId,
        ))
        .col(
            ColumnDef::new(QuantTradePolicyValidationRow::RowOrdinal)
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantTradePolicyValidationRow::EvidenceKind)
                .string_len(32)
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantTradePolicyValidationRow::RecordKey)
                .text()
                .not_null(),
        )
        .col(column::uuid_null(QuantTradePolicyValidationRow::ExampleId))
        .col(ColumnDef::new(QuantTradePolicyValidationRow::MarketId).text())
        .col(ColumnDef::new(QuantTradePolicyValidationRow::TokenId).text())
        .col(ColumnDef::new(QuantTradePolicyValidationRow::DecisionAt).timestamp_with_time_zone())
        .col(ColumnDef::new(QuantTradePolicyValidationRow::ExpectedRowHash).text())
        .col(ColumnDef::new(QuantTradePolicyValidationRow::ActualRowHash).text())
        .col(
            ColumnDef::new(QuantTradePolicyValidationRow::Passed)
                .boolean()
                .not_null(),
        )
        .col(ColumnDef::new(QuantTradePolicyValidationRow::DiagnosticKind).string_len(64))
        .col(ColumnDef::new(QuantTradePolicyValidationRow::Detail).text())
        .col(
            ColumnDef::new(QuantTradePolicyValidationRow::RowHash)
                .text()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            QuantTradePolicyValidationRow::CreatedAt,
        ))
        .primary_key(
            Index::create()
                .col(QuantTradePolicyValidationRow::ValidationRunId)
                .col(QuantTradePolicyValidationRow::RowOrdinal),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_trade_policy_validation_row_run")
                .from(
                    QuantTradePolicyValidationRow::Table,
                    QuantTradePolicyValidationRow::ValidationRunId,
                )
                .to(
                    QuantTradePolicyValidation::Table,
                    QuantTradePolicyValidation::ValidationRunId,
                )
                .on_delete(ForeignKeyAction::Restrict),
        )
        .check(Expr::cust(
            "row_ordinal >= 0
             AND record_key <> ''
             AND evidence_kind IN (
               'observation_eligibility', 'fills', 'candidate_trials',
               'cohort_trials', 'cpcv_paths', 'coverage_gaps',
               'statistical_summaries'
             )
             AND (expected_row_hash IS NOT NULL OR actual_row_hash IS NOT NULL)
             AND (
               (passed AND expected_row_hash = actual_row_hash
                AND diagnostic_kind IS NULL AND detail IS NULL)
               OR (NOT passed AND diagnostic_kind IS NOT NULL AND detail IS NOT NULL)
             )",
        ))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_quant_trade_policy_validation_row_result",
        table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_quant_trade_policy_validation_row_result")
            .table(QuantTradePolicyValidationRow::Table)
            .col(QuantTradePolicyValidationRow::ValidationRunId)
            .col(QuantTradePolicyValidationRow::Passed)
            .col(QuantTradePolicyValidationRow::RowOrdinal)
            .to_owned(),
        "stable validation row drilldown and failure filtering",
    )]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![TableDependency::foreign_key(validation_table_name)]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn table_name() -> String {
    QuantTradePolicyValidationRow::Table.to_string()
}

fn validation_table_name() -> String {
    QuantTradePolicyValidation::Table.to_string()
}
