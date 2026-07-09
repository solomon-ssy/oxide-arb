use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table, TableCreateStatement,
    },
};

use crate::{
    idens::{
        quant_model_run::QuantModelRun, quant_model_version::QuantModelVersion,
        runtime_config_version::RuntimeConfigVersion,
    },
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

// Append-only, content-addressed point-in-time backtest report: one row per
// `Backtester` run over a frozen `(model_version, runtime_config_version,
// window)`. Scalar metrics live in typed columns so the 3.7 quality gate can
// filter on them directly; the larger sub-structures (expected vs realized,
// per-category breakdown, PnL simulation) are JSONB. Immutable analytical output
// ⇒ `report` lifecycle (mirrors `quant_recommendation_report`).
#[quant_schema(lifecycle = "report")]
pub enum QuantBacktestReport {
    Table,
    BacktestReportId,
    ModelVersionId,
    ModelRunId,
    RuntimeConfigVersionId,
    WindowStart,
    WindowEnd,
    Coverage,
    SampleCount,
    MissingFeatureCount,
    RankIc,
    Sharpe,
    HitRate,
    ExpectedVsRealized,
    MaxDrawdown,
    Turnover,
    LiquidityFeasibility,
    CategoryBreakdown,
    TailLoss,
    ReportPnlSimulation,
    ReportHash,
    ParquetUri,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantBacktestReport::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantBacktestReport::BacktestReportId))
        .col(column::uuid_fk(QuantBacktestReport::ModelVersionId))
        .col(column::uuid_fk(QuantBacktestReport::ModelRunId))
        .col(column::uuid_fk(QuantBacktestReport::RuntimeConfigVersionId))
        .col(
            ColumnDef::new(QuantBacktestReport::WindowStart)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantBacktestReport::WindowEnd)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(column::ratio(QuantBacktestReport::Coverage))
        .col(
            ColumnDef::new(QuantBacktestReport::SampleCount)
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantBacktestReport::MissingFeatureCount)
                .big_integer()
                .not_null(),
        )
        .col(column::ratio(QuantBacktestReport::RankIc))
        .col(column::ratio(QuantBacktestReport::Sharpe))
        .col(column::probability(QuantBacktestReport::HitRate))
        .col(
            ColumnDef::new(QuantBacktestReport::ExpectedVsRealized)
                .json_binary()
                .not_null(),
        )
        .col(column::ratio(QuantBacktestReport::MaxDrawdown))
        .col(column::ratio(QuantBacktestReport::Turnover))
        .col(column::probability(
            QuantBacktestReport::LiquidityFeasibility,
        ))
        .col(
            ColumnDef::new(QuantBacktestReport::CategoryBreakdown)
                .json_binary()
                .not_null(),
        )
        .col(column::ratio(QuantBacktestReport::TailLoss))
        .col(
            ColumnDef::new(QuantBacktestReport::ReportPnlSimulation)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantBacktestReport::ReportHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantBacktestReport::ParquetUri)
                .text()
                .null(),
        )
        .col(timestamp_with_write_default(QuantBacktestReport::CreatedAt))
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_backtest_report_model_version")
                .from(
                    QuantBacktestReport::Table,
                    QuantBacktestReport::ModelVersionId,
                )
                .to(QuantModelVersion::Table, QuantModelVersion::ModelVersionId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_backtest_report_model_run")
                .from(QuantBacktestReport::Table, QuantBacktestReport::ModelRunId)
                .to(QuantModelRun::Table, QuantModelRun::ModelRunId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_backtest_report_runtime_config")
                .from(
                    QuantBacktestReport::Table,
                    QuantBacktestReport::RuntimeConfigVersionId,
                )
                .to(
                    RuntimeConfigVersion::Table,
                    RuntimeConfigVersion::RuntimeConfigVersionId,
                )
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_quant_backtest_report_version_created",
            quant_backtest_report_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_backtest_report_version_created")
                .table(QuantBacktestReport::Table)
                .col(QuantBacktestReport::ModelVersionId)
                .col((QuantBacktestReport::CreatedAt, IndexOrder::Desc))
                .to_owned(),
            "backtest reports by model version and recency",
        ),
        IndexSpec::sea_query(
            "uq_quant_backtest_report_hash",
            quant_backtest_report_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uq_quant_backtest_report_hash")
                .table(QuantBacktestReport::Table)
                .col(QuantBacktestReport::ReportHash)
                .unique()
                .to_owned(),
            "one row per content-addressed backtest report hash",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(quant_model_version_table_name),
        TableDependency::foreign_key(quant_model_run_table_name),
        TableDependency::foreign_key(runtime_config_version_table_name),
    ]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_backtest_report_table_name() -> String {
    QuantBacktestReport::Table.to_string()
}

fn quant_model_version_table_name() -> String {
    QuantModelVersion::Table.to_string()
}

fn quant_model_run_table_name() -> String {
    QuantModelRun::Table.to_string()
}

fn runtime_config_version_table_name() -> String {
    RuntimeConfigVersion::Table.to_string()
}
