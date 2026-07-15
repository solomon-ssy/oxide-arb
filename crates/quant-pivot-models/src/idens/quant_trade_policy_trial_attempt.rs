use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, Expr, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table,
        TableCreateStatement,
    },
};

use crate::{
    enums::quant::{TradePolicyTrialScope, TradePolicyTrialStatus},
    idens::quant_research_job::QuantResearchJob,
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[quant_schema(lifecycle = "audit")]
pub enum QuantTradePolicyTrialAttempt {
    Table,
    TrialAttemptId,
    FitJobId,
    AttemptOrdinal,
    ExperimentFamilyHash,
    ResearchProgramHash,
    CandidateId,
    CandidateHash,
    Scope,
    FoldIndex,
    PathIndex,
    Status,
    MetricsJson,
    EvidenceUri,
    EvidenceHash,
    EvidenceRowCount,
    FailureDetail,
    RowHash,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantTradePolicyTrialAttempt::Table)
        .if_not_exists()
        .col(column::uuid_pk(
            QuantTradePolicyTrialAttempt::TrialAttemptId,
        ))
        .col(column::uuid_fk(QuantTradePolicyTrialAttempt::FitJobId))
        .col(
            ColumnDef::new(QuantTradePolicyTrialAttempt::AttemptOrdinal)
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantTradePolicyTrialAttempt::ExperimentFamilyHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantTradePolicyTrialAttempt::ResearchProgramHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantTradePolicyTrialAttempt::CandidateId)
                .string_len(128)
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantTradePolicyTrialAttempt::CandidateHash)
                .text()
                .not_null(),
        )
        .col(column::pg_enum::<TradePolicyTrialScope>(
            QuantTradePolicyTrialAttempt::Scope,
        ))
        .col(ColumnDef::new(QuantTradePolicyTrialAttempt::FoldIndex).integer())
        .col(ColumnDef::new(QuantTradePolicyTrialAttempt::PathIndex).integer())
        .col(column::pg_enum::<TradePolicyTrialStatus>(
            QuantTradePolicyTrialAttempt::Status,
        ))
        .col(ColumnDef::new(QuantTradePolicyTrialAttempt::MetricsJson).json_binary())
        .col(ColumnDef::new(QuantTradePolicyTrialAttempt::EvidenceUri).text())
        .col(ColumnDef::new(QuantTradePolicyTrialAttempt::EvidenceHash).text())
        .col(ColumnDef::new(QuantTradePolicyTrialAttempt::EvidenceRowCount).big_integer())
        .col(ColumnDef::new(QuantTradePolicyTrialAttempt::FailureDetail).text())
        .col(
            ColumnDef::new(QuantTradePolicyTrialAttempt::RowHash)
                .text()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            QuantTradePolicyTrialAttempt::CreatedAt,
        ))
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_trade_policy_trial_attempt_job")
                .from(
                    QuantTradePolicyTrialAttempt::Table,
                    QuantTradePolicyTrialAttempt::FitJobId,
                )
                .to(QuantResearchJob::Table, QuantResearchJob::JobId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .check(Expr::cust(
            "attempt_ordinal >= 0 AND length(btrim(candidate_id)) BETWEEN 1 AND 128
             AND (fold_index IS NULL OR fold_index >= 0)
             AND (path_index IS NULL OR path_index >= 0)
             AND (evidence_row_count IS NULL OR evidence_row_count >= 0)",
        ))
        .check(Expr::cust(
            "(scope IN ('candidate', 'latency_stress') AND fold_index IS NULL AND path_index IS NULL)
             OR (scope = 'fold' AND fold_index IS NOT NULL AND path_index IS NULL)
             OR (scope = 'path' AND fold_index IS NULL AND path_index IS NOT NULL)",
        ))
        .check(Expr::cust(
            "(status = 'succeeded' AND metrics_json IS NOT NULL AND failure_detail IS NULL
               AND evidence_uri IS NOT NULL AND evidence_hash IS NOT NULL
               AND evidence_row_count IS NOT NULL)
             OR (status IN ('failed', 'cancelled') AND metrics_json IS NULL
               AND length(btrim(failure_detail)) BETWEEN 1 AND 8192)",
        ))
        .check(Expr::cust(
            "(evidence_uri IS NULL AND evidence_hash IS NULL AND evidence_row_count IS NULL)
             OR (evidence_uri IS NOT NULL AND evidence_hash IS NOT NULL
               AND evidence_row_count IS NOT NULL)",
        ))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "uq_quant_trade_policy_trial_attempt_ordinal",
            table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uq_quant_trade_policy_trial_attempt_ordinal")
                .table(QuantTradePolicyTrialAttempt::Table)
                .col(QuantTradePolicyTrialAttempt::FitJobId)
                .col(QuantTradePolicyTrialAttempt::AttemptOrdinal)
                .unique()
                .to_owned(),
            "one immutable ordered content binding per fit attempt ordinal",
        ),
        IndexSpec::sea_query(
            "idx_quant_trade_policy_trial_attempt_drilldown",
            table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_trade_policy_trial_attempt_drilldown")
                .table(QuantTradePolicyTrialAttempt::Table)
                .col(QuantTradePolicyTrialAttempt::FitJobId)
                .col(QuantTradePolicyTrialAttempt::Status)
                .col(QuantTradePolicyTrialAttempt::Scope)
                .col((
                    QuantTradePolicyTrialAttempt::AttemptOrdinal,
                    IndexOrder::Asc,
                ))
                .to_owned(),
            "stable failed/cancelled trial drilldown",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![TableDependency::foreign_key(research_job_table_name)]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn table_name() -> String {
    QuantTradePolicyTrialAttempt::Table.to_string()
}

fn research_job_table_name() -> String {
    QuantResearchJob::Table.to_string()
}
