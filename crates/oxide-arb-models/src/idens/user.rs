//! `user` table — RBAC account identities.

use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, Index, Table, TableCreateStatement},
};

use crate::{
    enums::rbac::UserStatus,
    schema::{
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
    seed::rbac::admin_user,
};

/// RBAC user account. The primary key is the stable Casbin subject.
#[oxide_schema(lifecycle = "control")]
pub enum User {
    Table,
    Id,
    Username,
    PasswordHash,
    Nickname,
    Avatar,
    Email,
    Phone,
    Status,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(User::Table)
        .if_not_exists()
        .col(ColumnDef::new(User::Id).text().not_null().primary_key())
        .col(ColumnDef::new(User::Username).text().not_null())
        .col(ColumnDef::new(User::PasswordHash).text().not_null())
        .col(ColumnDef::new(User::Nickname).text().not_null())
        .col(ColumnDef::new(User::Avatar).text().null())
        .col(ColumnDef::new(User::Email).text().null())
        .col(ColumnDef::new(User::Phone).text().null())
        .col(
            ColumnDef::new(User::Status)
                .text()
                .not_null()
                .default(UserStatus::Active),
        )
        .col(timestamp_with_write_default(User::CreatedAt))
        .col(timestamp_with_write_default(User::UpdatedAt))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "uq_user_username",
        user_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("uq_user_username")
            .table(User::Table)
            .col(User::Username)
            .unique()
            .to_owned(),
        "unique account username",
    )]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub fn seed_units() -> Vec<SeedSpec> {
    vec![admin_user::ADMIN_USER_SEED]
}

pub fn user_table_name() -> String {
    User::Table.to_string()
}
