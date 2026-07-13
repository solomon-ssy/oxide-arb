use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, ForeignKeyCreateStatement, Index, IndexOrder,
        Table, TableCreateStatement,
    },
};

use crate::{
    idens::{
        quant_model_run::QuantModelRun, quant_model_version::QuantModelVersion,
        quant_training_dataset::QuantTrainingDataset, runtime_config_version::RuntimeConfigVersion,
    },
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

// Append-only Combinatorial Purged Cross-Validation result (Phase 11.5): one
// row per `CombinatorialPurgedBacktester` + governed trial-grid run over a
// frozen `(model_version, training_dataset, runtime_config)` triple. Scalar
// alpha-significance metrics (`median_rank_ic`, `deflated_sharpe`, `pbo`) live
// in typed columns so the quality gate can filter on them directly; the
// reconstructed φ paths + Sharpe distribution are JSONB. Immutable analytical
// output ⇒ `report` lifecycle (mirrors `quant_backtest_report`).
#[quant_schema(lifecycle = "report")]
pub enum QuantBacktestPathSet {
    Table,
    PathSetId,
    ModelVersionId,
    ModelRunId,
    TrainingDatasetId,
    RuntimeConfigVersionId,
    WindowStart,
    WindowEnd,
    PathCount,
    CombinationCount,
    MedianRankIc,
    SharpeDistribution,
    Paths,
    DeflatedSharpe,
    DsrBenchmarkSharpe,
    Pbo,
    MinTrackRecordLengthSecs,
    TrialCount,
    TrialGridCount,
    CoordSearchEffectiveN,
    /// Content-addressed digest of the path-set payload (paths + distribution +
    /// gate scalars). Enables audit/replay association beyond the random UUID.
    PathSetHash,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    let mut stmt = path_set_columns();
    path_set_apply_foreign_keys(&mut stmt);
    stmt
}

fn path_set_columns() -> TableCreateStatement {
    Table::create()
        .table(QuantBacktestPathSet::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantBacktestPathSet::PathSetId))
        .col(column::uuid_fk(QuantBacktestPathSet::ModelVersionId))
        .col(column::uuid_fk(QuantBacktestPathSet::ModelRunId))
        .col(column::uuid_fk(QuantBacktestPathSet::TrainingDatasetId))
        .col(column::uuid_fk(
            QuantBacktestPathSet::RuntimeConfigVersionId,
        ))
        .col(
            ColumnDef::new(QuantBacktestPathSet::WindowStart)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantBacktestPathSet::WindowEnd)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantBacktestPathSet::PathCount)
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantBacktestPathSet::CombinationCount)
                .big_integer()
                .not_null(),
        )
        .col(column::ratio(QuantBacktestPathSet::MedianRankIc))
        .col(
            ColumnDef::new(QuantBacktestPathSet::SharpeDistribution)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantBacktestPathSet::Paths)
                .json_binary()
                .not_null(),
        )
        .col(column::ratio(QuantBacktestPathSet::DeflatedSharpe))
        .col(column::ratio(QuantBacktestPathSet::DsrBenchmarkSharpe))
        .col(column::ratio(QuantBacktestPathSet::Pbo))
        .col(
            ColumnDef::new(QuantBacktestPathSet::MinTrackRecordLengthSecs)
                .big_integer()
                .null(),
        )
        .col(
            ColumnDef::new(QuantBacktestPathSet::TrialCount)
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantBacktestPathSet::TrialGridCount)
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantBacktestPathSet::CoordSearchEffectiveN)
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantBacktestPathSet::PathSetHash)
                .text()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            QuantBacktestPathSet::CreatedAt,
        ))
        .to_owned()
}

fn path_set_apply_foreign_keys(stmt: &mut TableCreateStatement) {
    let mut model_version_fk = model_version_fk();
    stmt.foreign_key(&mut model_version_fk);
    let mut model_run_fk = model_run_fk();
    stmt.foreign_key(&mut model_run_fk);
    let mut training_dataset_fk = training_dataset_fk();
    stmt.foreign_key(&mut training_dataset_fk);
    let mut runtime_config_fk = runtime_config_fk();
    stmt.foreign_key(&mut runtime_config_fk);
}

fn model_version_fk() -> ForeignKeyCreateStatement {
    ForeignKey::create()
        .name("fk_quant_backtest_path_set_model_version")
        .from(
            QuantBacktestPathSet::Table,
            QuantBacktestPathSet::ModelVersionId,
        )
        .to(QuantModelVersion::Table, QuantModelVersion::ModelVersionId)
        .on_delete(ForeignKeyAction::Restrict)
        .to_owned()
}

fn model_run_fk() -> ForeignKeyCreateStatement {
    ForeignKey::create()
        .name("fk_quant_backtest_path_set_model_run")
        .from(
            QuantBacktestPathSet::Table,
            QuantBacktestPathSet::ModelRunId,
        )
        .to(QuantModelRun::Table, QuantModelRun::ModelRunId)
        .on_delete(ForeignKeyAction::Restrict)
        .to_owned()
}

fn training_dataset_fk() -> ForeignKeyCreateStatement {
    ForeignKey::create()
        .name("fk_quant_backtest_path_set_training_dataset")
        .from(
            QuantBacktestPathSet::Table,
            QuantBacktestPathSet::TrainingDatasetId,
        )
        .to(
            QuantTrainingDataset::Table,
            QuantTrainingDataset::TrainingDatasetId,
        )
        .on_delete(ForeignKeyAction::Restrict)
        .to_owned()
}

fn runtime_config_fk() -> ForeignKeyCreateStatement {
    ForeignKey::create()
        .name("fk_quant_backtest_path_set_runtime_config")
        .from(
            QuantBacktestPathSet::Table,
            QuantBacktestPathSet::RuntimeConfigVersionId,
        )
        .to(
            RuntimeConfigVersion::Table,
            RuntimeConfigVersion::RuntimeConfigVersionId,
        )
        .on_delete(ForeignKeyAction::Restrict)
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_quant_backtest_path_set_version_created",
        quant_backtest_path_set_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_quant_backtest_path_set_version_created")
            .table(QuantBacktestPathSet::Table)
            .col(QuantBacktestPathSet::ModelVersionId)
            .col((QuantBacktestPathSet::CreatedAt, IndexOrder::Desc))
            .to_owned(),
        "CPCV path sets by model version and recency",
    )]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(quant_model_version_table_name),
        TableDependency::foreign_key(quant_model_run_table_name),
        TableDependency::foreign_key(quant_training_dataset_table_name),
        TableDependency::foreign_key(runtime_config_version_table_name),
    ]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_backtest_path_set_table_name() -> String {
    QuantBacktestPathSet::Table.to_string()
}

fn quant_model_version_table_name() -> String {
    QuantModelVersion::Table.to_string()
}

fn quant_model_run_table_name() -> String {
    QuantModelRun::Table.to_string()
}

fn quant_training_dataset_table_name() -> String {
    QuantTrainingDataset::Table.to_string()
}

fn runtime_config_version_table_name() -> String {
    RuntimeConfigVersion::Table.to_string()
}
