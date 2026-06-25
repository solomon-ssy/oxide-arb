use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, ForeignKeyCreateStatement, Index, IndexOrder,
        IntoIden, Table, TableCreateStatement,
    },
};

use crate::{
    idens::{
        quant_backtest_report::QuantBacktestReport, quant_model_run::QuantModelRun,
        quant_model_version::QuantModelVersion,
    },
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

// Append-only, content-addressed pairwise model-comparison report: one row per
// `compare_reports` run over a baseline + candidate replayed across the same
// window. The scalar deltas (rank IC, hit rate, realized PnL, score correlation,
// side disagreement) live in typed columns so a promotion gate can filter on
// them; the per-category diff is JSONB. References both backtest reports + the
// candidate model run. Immutable analytical output ⇒ `report` lifecycle.
#[quant_schema(lifecycle = "report")]
pub enum QuantModelComparisonReport {
    Table,
    ComparisonReportId,
    BaselineModelVersionId,
    CandidateModelVersionId,
    BaselineReportId,
    CandidateReportId,
    ModelRunId,
    RankIcDelta,
    HitRateDelta,
    RealizedPnlDelta,
    ScoreCorrelation,
    SideDisagreementRate,
    CommonSamples,
    CategoryBreakdownDiff,
    ComparisonHash,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantModelComparisonReport::Table)
        .if_not_exists()
        .col(column::uuid_pk(
            QuantModelComparisonReport::ComparisonReportId,
        ))
        .col(column::uuid_fk(
            QuantModelComparisonReport::BaselineModelVersionId,
        ))
        .col(column::uuid_fk(
            QuantModelComparisonReport::CandidateModelVersionId,
        ))
        .col(column::uuid_fk(
            QuantModelComparisonReport::BaselineReportId,
        ))
        .col(column::uuid_fk(
            QuantModelComparisonReport::CandidateReportId,
        ))
        .col(column::uuid_fk(QuantModelComparisonReport::ModelRunId))
        .col(column::ratio(QuantModelComparisonReport::RankIcDelta))
        .col(column::ratio(QuantModelComparisonReport::HitRateDelta))
        .col(column::usd(QuantModelComparisonReport::RealizedPnlDelta))
        .col(column::ratio(QuantModelComparisonReport::ScoreCorrelation))
        .col(column::ratio(
            QuantModelComparisonReport::SideDisagreementRate,
        ))
        .col(
            ColumnDef::new(QuantModelComparisonReport::CommonSamples)
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantModelComparisonReport::CategoryBreakdownDiff)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantModelComparisonReport::ComparisonHash)
                .text()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            QuantModelComparisonReport::CreatedAt,
        ))
        .foreign_key(&mut fk(
            "fk_quant_model_comparison_report_baseline_version",
            QuantModelComparisonReport::BaselineModelVersionId,
            QuantModelVersion::Table,
            QuantModelVersion::ModelVersionId,
        ))
        .foreign_key(&mut fk(
            "fk_quant_model_comparison_report_candidate_version",
            QuantModelComparisonReport::CandidateModelVersionId,
            QuantModelVersion::Table,
            QuantModelVersion::ModelVersionId,
        ))
        .foreign_key(&mut fk(
            "fk_quant_model_comparison_report_baseline_report",
            QuantModelComparisonReport::BaselineReportId,
            QuantBacktestReport::Table,
            QuantBacktestReport::BacktestReportId,
        ))
        .foreign_key(&mut fk(
            "fk_quant_model_comparison_report_candidate_report",
            QuantModelComparisonReport::CandidateReportId,
            QuantBacktestReport::Table,
            QuantBacktestReport::BacktestReportId,
        ))
        .foreign_key(&mut fk(
            "fk_quant_model_comparison_report_model_run",
            QuantModelComparisonReport::ModelRunId,
            QuantModelRun::Table,
            QuantModelRun::ModelRunId,
        ))
        .to_owned()
}

/// Build one `Restrict` foreign key from a comparison-report column.
fn fk(
    name: &str,
    from_col: QuantModelComparisonReport,
    to_table: impl IntoIden + 'static,
    to_col: impl IntoIden + 'static,
) -> ForeignKeyCreateStatement {
    ForeignKey::create()
        .name(name)
        .from(QuantModelComparisonReport::Table, from_col)
        .to(to_table, to_col)
        .on_delete(ForeignKeyAction::Restrict)
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_quant_model_comparison_report_candidate_created",
            quant_model_comparison_report_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_model_comparison_report_candidate_created")
                .table(QuantModelComparisonReport::Table)
                .col(QuantModelComparisonReport::CandidateModelVersionId)
                .col((QuantModelComparisonReport::CreatedAt, IndexOrder::Desc))
                .to_owned(),
            "comparison reports by candidate version and recency",
        ),
        IndexSpec::sea_query(
            "uq_quant_model_comparison_report_hash",
            quant_model_comparison_report_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uq_quant_model_comparison_report_hash")
                .table(QuantModelComparisonReport::Table)
                .col(QuantModelComparisonReport::ComparisonHash)
                .unique()
                .to_owned(),
            "one row per content-addressed comparison hash",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(quant_model_version_table_name),
        TableDependency::foreign_key(quant_backtest_report_table_name),
        TableDependency::foreign_key(quant_model_run_table_name),
    ]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_model_comparison_report_table_name() -> String {
    QuantModelComparisonReport::Table.to_string()
}

fn quant_model_version_table_name() -> String {
    QuantModelVersion::Table.to_string()
}

fn quant_backtest_report_table_name() -> String {
    QuantBacktestReport::Table.to_string()
}

fn quant_model_run_table_name() -> String {
    QuantModelRun::Table.to_string()
}
