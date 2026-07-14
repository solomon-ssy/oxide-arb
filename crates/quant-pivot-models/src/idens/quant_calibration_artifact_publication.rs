use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Index, Table, TableCreateStatement},
};

use crate::{
    enums::quant::CalibrationKind,
    idens::quant_calibration_artifact::QuantCalibrationArtifact,
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[quant_schema(lifecycle = "audit")]
pub enum QuantCalibrationArtifactPublication {
    Table,
    PublicationId,
    ArtifactId,
    Kind,
    PublishedAt,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantCalibrationArtifactPublication::Table)
        .if_not_exists()
        .col(column::uuid_pk(
            QuantCalibrationArtifactPublication::PublicationId,
        ))
        .col(column::uuid_fk(
            QuantCalibrationArtifactPublication::ArtifactId,
        ))
        .col(column::pg_enum::<CalibrationKind>(
            QuantCalibrationArtifactPublication::Kind,
        ))
        .col(
            ColumnDef::new(QuantCalibrationArtifactPublication::PublishedAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            QuantCalibrationArtifactPublication::CreatedAt,
        ))
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_calibration_publication_artifact")
                .from(
                    QuantCalibrationArtifactPublication::Table,
                    QuantCalibrationArtifactPublication::ArtifactId,
                )
                .to(
                    QuantCalibrationArtifact::Table,
                    QuantCalibrationArtifact::ArtifactId,
                )
                .on_delete(ForeignKeyAction::Restrict)
                .on_update(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_quant_calibration_publication_pit",
        table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_quant_calibration_publication_pit")
            .table(QuantCalibrationArtifactPublication::Table)
            .col(QuantCalibrationArtifactPublication::Kind)
            .col(QuantCalibrationArtifactPublication::PublishedAt)
            .col(QuantCalibrationArtifactPublication::PublicationId)
            .to_owned(),
        "PIT calibration publication timeline",
    )]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![TableDependency::foreign_key(
        calibration_artifact_table_name,
    )]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn table_name() -> String {
    QuantCalibrationArtifactPublication::Table.to_string()
}

fn calibration_artifact_table_name() -> String {
    QuantCalibrationArtifact::Table.to_string()
}
