use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, Expr, ForeignKey, ForeignKeyAction, Index, Table, TableCreateStatement,
    },
};

use crate::{
    enums::quant::SourceSliceStatus,
    idens::runtime_config_version::RuntimeConfigVersion,
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[quant_schema(lifecycle = "ledger")]
pub enum QuantSourceSlice {
    Table,
    SourceSliceId,
    IdentityHash,
    ProfileRef,
    EvaluationTrack,
    ResearchProgramHash,
    RuntimeConfigVersionId,
    RuntimeConfigHash,
    WindowStart,
    WindowEnd,
    PitCutoff,
    ReaderContractVersion,
    SchemaContractVersion,
    Status,
    ManifestUri,
    ManifestHash,
    ManifestJson,
    FailureDetail,
    CompletedAt,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantSourceSlice::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantSourceSlice::SourceSliceId))
        .col(
            ColumnDef::new(QuantSourceSlice::IdentityHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantSourceSlice::ProfileRef)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantSourceSlice::EvaluationTrack)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantSourceSlice::ResearchProgramHash)
                .text()
                .not_null(),
        )
        .col(column::uuid_fk(QuantSourceSlice::RuntimeConfigVersionId))
        .col(
            ColumnDef::new(QuantSourceSlice::RuntimeConfigHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantSourceSlice::WindowStart)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantSourceSlice::WindowEnd)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantSourceSlice::PitCutoff)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantSourceSlice::ReaderContractVersion)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantSourceSlice::SchemaContractVersion)
                .text()
                .not_null(),
        )
        .col(column::pg_enum::<SourceSliceStatus>(QuantSourceSlice::Status))
        .col(ColumnDef::new(QuantSourceSlice::ManifestUri).text())
        .col(ColumnDef::new(QuantSourceSlice::ManifestHash).text())
        .col(ColumnDef::new(QuantSourceSlice::ManifestJson).json_binary())
        .col(ColumnDef::new(QuantSourceSlice::FailureDetail).text())
        .col(ColumnDef::new(QuantSourceSlice::CompletedAt).timestamp_with_time_zone())
        .col(timestamp_with_write_default(QuantSourceSlice::CreatedAt))
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_source_slice_runtime_config")
                .from(
                    QuantSourceSlice::Table,
                    QuantSourceSlice::RuntimeConfigVersionId,
                )
                .to(
                    RuntimeConfigVersion::Table,
                    RuntimeConfigVersion::RuntimeConfigVersionId,
                )
                .on_delete(ForeignKeyAction::Restrict),
        )
        .check(Expr::cust(
            "window_start < window_end AND window_end <= pit_cutoff
             AND evaluation_track IN ('research_only', 'semi_auto_candidate')
             AND (
                (status = 'materializing' AND manifest_uri IS NULL AND manifest_hash IS NULL
                    AND manifest_json IS NULL AND failure_detail IS NULL AND completed_at IS NULL)
                OR (status = 'ready' AND manifest_uri IS NOT NULL AND manifest_hash IS NOT NULL
                    AND manifest_json IS NOT NULL AND failure_detail IS NULL AND completed_at IS NOT NULL)
                OR (status = 'failed' AND manifest_uri IS NULL AND manifest_hash IS NULL
                    AND manifest_json IS NULL AND failure_detail IS NOT NULL AND completed_at IS NOT NULL)
             )",
        ))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "uq_quant_source_slice_identity",
            table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uq_quant_source_slice_identity")
                .table(QuantSourceSlice::Table)
                .col(QuantSourceSlice::IdentityHash)
                .unique()
                .to_owned(),
            "one materialization ledger row per canonical source identity",
        ),
        IndexSpec::sea_query(
            "idx_quant_source_slice_status_created",
            table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_source_slice_status_created")
                .table(QuantSourceSlice::Table)
                .col(QuantSourceSlice::Status)
                .col(QuantSourceSlice::CreatedAt)
                .to_owned(),
            "source-slice recovery and operator status scans",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![TableDependency::foreign_key(
        runtime_config_version_table_name,
    )]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn table_name() -> String {
    QuantSourceSlice::Table.to_string()
}

fn runtime_config_version_table_name() -> String {
    RuntimeConfigVersion::Table.to_string()
}
