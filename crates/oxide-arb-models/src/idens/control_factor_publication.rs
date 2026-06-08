use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, Index, IndexOrder, Table, TableCreateStatement},
};

use crate::{
    enums::control_factor::PublicationStatus,
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[oxide_schema(lifecycle = "control")]
pub enum ControlFactorPublication {
    Table,
    PublicationId,
    Mode,
    PreviousPublicationId,
    Status,
    EffectiveFrom,
    ExpiresAt,
    ApprovedBy,
    ApprovalReason,
    IdempotencyKey,
    PublicationHash,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(ControlFactorPublication::Table)
        .if_not_exists()
        .col(column::uuid_pk(ControlFactorPublication::PublicationId))
        .col(
            ColumnDef::new(ControlFactorPublication::Mode)
                .text()
                .not_null(),
        )
        .col(column::uuid_null(
            ControlFactorPublication::PreviousPublicationId,
        ))
        .col(
            ColumnDef::new(ControlFactorPublication::Status)
                .text()
                .not_null()
                .default(PublicationStatus::Pending),
        )
        .col(
            ColumnDef::new(ControlFactorPublication::EffectiveFrom)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorPublication::ExpiresAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorPublication::ApprovedBy)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(ControlFactorPublication::ApprovalReason)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorPublication::IdempotencyKey)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorPublication::PublicationHash)
                .text()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            ControlFactorPublication::CreatedAt,
        ))
        .col(timestamp_with_write_default(
            ControlFactorPublication::UpdatedAt,
        ))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::raw(
            "idx_control_factor_publication_active_mode",
            publication_table_name,
            IndexBuildMode::Transactional,
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_control_factor_publication_active_mode \
             ON control_factor_publication (mode) \
             WHERE status = 'active'",
            "one active control-factor publication per mode",
        ),
        IndexSpec::sea_query(
            "idx_control_factor_publication_effective",
            publication_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_control_factor_publication_effective")
                .table(ControlFactorPublication::Table)
                .col(ControlFactorPublication::Mode)
                .col(ControlFactorPublication::Status)
                .col((ControlFactorPublication::EffectiveFrom, IndexOrder::Desc))
                .to_owned(),
            "publication lookup by mode and effective time",
        ),
        IndexSpec::sea_query(
            "uniq_control_factor_publication_idempotency",
            publication_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uniq_control_factor_publication_idempotency")
                .table(ControlFactorPublication::Table)
                .col(ControlFactorPublication::IdempotencyKey)
                .unique()
                .to_owned(),
            "idempotent publication creation key",
        ),
    ]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn publication_table_name() -> String {
    ControlFactorPublication::Table.to_string()
}
