use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, Index, IndexOrder, Table, TableCreateStatement},
};

use crate::{
    enums::quant::CalibrationKind,
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

// Append-only, content-addressed calibration-artifact ledger (Phase 11.3
// §3.4). Unifies every empirical calibration artifact in the system: `kind =
// model_score` (a `ProbabilityCalibrator` mapping model score → `P(win)`,
// fit on an independent held-out split) and `kind = market_price_bias`
// (formerly the standalone Phase 11.2.1 `quant_favorite_longshot_bias_table`,
// dropped — no compatibility shim). The kind-specific payload (monotone map +
// reliability report, or per-category bias curves) lives in a single JSONB
// column; scalar fit provenance (window, split hash, sample count) is typed
// so the governance catalog can filter without deserializing the payload.
// Immutable analytical output ⇒ `report` lifecycle. `active` tracks whether
// an operator has bound this artifact (runtime-config `bias_table_ref` for
// `market_price_bias`, or a model version's `return_model` for `model_score`).
#[quant_schema(lifecycle = "report")]
pub enum QuantCalibrationArtifact {
    Table,
    ArtifactId,
    Kind,
    ContentHash,
    FitWindowStart,
    FitWindowEnd,
    CalibrationSplitHash,
    SampleCount,
    PayloadJson,
    Active,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantCalibrationArtifact::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantCalibrationArtifact::ArtifactId))
        .col(column::pg_enum::<CalibrationKind>(
            QuantCalibrationArtifact::Kind,
        ))
        .col(
            ColumnDef::new(QuantCalibrationArtifact::ContentHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantCalibrationArtifact::FitWindowStart)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantCalibrationArtifact::FitWindowEnd)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantCalibrationArtifact::CalibrationSplitHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantCalibrationArtifact::SampleCount)
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantCalibrationArtifact::PayloadJson)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantCalibrationArtifact::Active)
                .boolean()
                .not_null()
                .default(false),
        )
        .col(timestamp_with_write_default(
            QuantCalibrationArtifact::CreatedAt,
        ))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "uq_quant_calibration_artifact_hash",
            quant_calibration_artifact_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uq_quant_calibration_artifact_hash")
                .table(QuantCalibrationArtifact::Table)
                .col(QuantCalibrationArtifact::ContentHash)
                .unique()
                .to_owned(),
            "one row per content-addressed calibration-artifact hash",
        ),
        IndexSpec::sea_query(
            "idx_quant_calibration_artifact_kind_created",
            quant_calibration_artifact_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_calibration_artifact_kind_created")
                .table(QuantCalibrationArtifact::Table)
                .col(QuantCalibrationArtifact::Kind)
                .col((QuantCalibrationArtifact::CreatedAt, IndexOrder::Desc))
                .to_owned(),
            "calibration artifacts by kind and recency",
        ),
    ]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_calibration_artifact_table_name() -> String {
    QuantCalibrationArtifact::Table.to_string()
}
