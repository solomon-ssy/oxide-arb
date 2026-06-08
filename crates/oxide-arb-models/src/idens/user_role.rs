//! `user_role` table — user→role assignments (mirrors Casbin `g`).

use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Index, Table, TableCreateStatement},
};

use crate::{
    idens::{role::Role, user::User},
    schema::{
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
    seed::rbac::user_role,
};

/// Explicit user→role join. The Casbin `g` grouping policy is kept in sync by
/// the repository layer; this table is the relational source of truth.
#[oxide_schema(lifecycle = "control")]
pub enum UserRole {
    Table,
    Id,
    UserId,
    RoleId,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(UserRole::Table)
        .if_not_exists()
        .col(ColumnDef::new(UserRole::Id).text().not_null().primary_key())
        .col(ColumnDef::new(UserRole::UserId).text().not_null())
        .col(ColumnDef::new(UserRole::RoleId).text().not_null())
        .col(timestamp_with_write_default(UserRole::CreatedAt))
        .foreign_key(
            ForeignKey::create()
                .name("fk_user_role_user")
                .from(UserRole::Table, UserRole::UserId)
                .to(User::Table, User::Id)
                .on_delete(ForeignKeyAction::Cascade),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_user_role_role")
                .from(UserRole::Table, UserRole::RoleId)
                .to(Role::Table, Role::Id)
                .on_delete(ForeignKeyAction::Cascade),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "uq_user_role",
            user_role_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uq_user_role")
                .table(UserRole::Table)
                .col(UserRole::UserId)
                .col(UserRole::RoleId)
                .unique()
                .to_owned(),
            "one assignment per (user, role)",
        ),
        IndexSpec::sea_query(
            "idx_user_role_role",
            user_role_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_user_role_role")
                .table(UserRole::Table)
                .col(UserRole::RoleId)
                .to_owned(),
            "reverse lookup: users in a role",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(user_table_name),
        TableDependency::foreign_key(role_table_name),
    ]
}

pub fn seed_units() -> Vec<SeedSpec> {
    vec![user_role::USER_ROLE_SEED]
}

pub fn user_role_table_name() -> String {
    UserRole::Table.to_string()
}

fn user_table_name() -> String {
    User::Table.to_string()
}

fn role_table_name() -> String {
    Role::Table.to_string()
}
