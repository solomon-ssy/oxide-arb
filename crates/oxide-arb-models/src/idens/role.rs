//! `role` table — RBAC roles. `code` is the Casbin policy subject.

use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, Index, Table, TableCreateStatement},
};

use crate::{
    enums::rbac::RoleStatus,
    schema::{
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
    seed::rbac::roles,
};

/// RBAC role. `code` (not the UUID id) is the stable Casbin policy subject.
#[oxide_schema(lifecycle = "control")]
pub enum Role {
    Table,
    Id,
    Code,
    Name,
    Description,
    Kind,
    Status,
    Sort,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(Role::Table)
        .if_not_exists()
        .col(ColumnDef::new(Role::Id).text().not_null().primary_key())
        .col(ColumnDef::new(Role::Code).text().not_null())
        .col(ColumnDef::new(Role::Name).text().not_null())
        .col(ColumnDef::new(Role::Description).text().null())
        .col(ColumnDef::new(Role::Kind).text().not_null())
        .col(
            ColumnDef::new(Role::Status)
                .text()
                .not_null()
                .default(RoleStatus::Enabled),
        )
        .col(ColumnDef::new(Role::Sort).integer().not_null().default(0))
        .col(timestamp_with_write_default(Role::CreatedAt))
        .col(timestamp_with_write_default(Role::UpdatedAt))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "uq_role_code",
        role_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("uq_role_code")
            .table(Role::Table)
            .col(Role::Code)
            .unique()
            .to_owned(),
        "unique role code (Casbin subject)",
    )]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub fn seed_units() -> Vec<SeedSpec> {
    vec![roles::ROLES_SEED]
}

pub fn role_table_name() -> String {
    Role::Table.to_string()
}
