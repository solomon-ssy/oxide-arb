//! `role` table — RBAC roles. `code` is the Casbin policy subject.

use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, Index, Table, TableCreateStatement},
};

use crate::{
    enums::rbac::{RoleKind, RoleStatus},
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
    seed::rbac::roles,
};

/// Maximum stored length of a role code (Casbin subject).
const ROLE_CODE_LEN: u32 = 32;

/// RBAC role. `code` (not the UUID id) is the stable Casbin policy subject.
#[quant_schema(lifecycle = "control")]
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
        .col(column::uuid_pk(Role::Id))
        .col(
            ColumnDef::new(Role::Code)
                .string_len(ROLE_CODE_LEN)
                .not_null(),
        )
        .col(ColumnDef::new(Role::Name).text().not_null())
        .col(ColumnDef::new(Role::Description).text().null())
        .col(column::pg_enum::<RoleKind>(Role::Kind))
        .col(column::pg_enum_default::<RoleStatus>(
            Role::Status,
            &RoleStatus::Enabled,
        ))
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
