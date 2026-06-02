use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, Index, IndexOrder, Table, TableCreateStatement},
};

use crate::{
    enums::control_factor::FactorStatus,
    schema::{
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[oxide_schema(lifecycle = "control")]
pub enum ControlFactorValue {
    Table,
    FactorId,
    FactorType,
    Dimensions,
    Payload,
    Evidence,
    Status,
    GeneratedAt,
    ExpiresAt,
    Owner,
    SchemaVersion,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(ControlFactorValue::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(ControlFactorValue::FactorId)
                .text()
                .not_null()
                .primary_key(),
        )
        .col(
            ColumnDef::new(ControlFactorValue::FactorType)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorValue::Dimensions)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorValue::Payload)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorValue::Evidence)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorValue::Status)
                .text()
                .not_null()
                .default(FactorStatus::Draft),
        )
        .col(
            ColumnDef::new(ControlFactorValue::GeneratedAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorValue::ExpiresAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(ColumnDef::new(ControlFactorValue::Owner).text().not_null())
        .col(
            ColumnDef::new(ControlFactorValue::SchemaVersion)
                .integer()
                .not_null(),
        )
        .col(timestamp_with_write_default(ControlFactorValue::CreatedAt))
        .col(timestamp_with_write_default(ControlFactorValue::UpdatedAt))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_control_factor_value_type_status",
            control_factor_value_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_control_factor_value_type_status")
                .table(ControlFactorValue::Table)
                .col(ControlFactorValue::FactorType)
                .col(ControlFactorValue::Status)
                .to_owned(),
            "factor lookup by type and lifecycle status",
        ),
        IndexSpec::sea_query(
            "idx_control_factor_value_expires_at",
            control_factor_value_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_control_factor_value_expires_at")
                .table(ControlFactorValue::Table)
                .col((ControlFactorValue::ExpiresAt, IndexOrder::Asc))
                .to_owned(),
            "factor expiry scans",
        ),
    ]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn control_factor_value_table_name() -> String {
    ControlFactorValue::Table.to_string()
}
