//! `user_role` table — user→role assignments (mirrors Casbin `g`).

use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ForeignKey, ForeignKeyAction, Index, Table, TableCreateStatement},
};

use crate::{
    idens::{role::Role, user::User},
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
    seed::rbac::user_role,
};

/// Explicit user→role join. The Casbin `g` grouping policy is kept in sync by
/// the repository layer; this table is the relational source of truth.
///
/// The composite primary key `(user_id, role_id)` is the natural key and its
/// own uniqueness guarantee — there is no surrogate join-row id.
#[oxide_schema(lifecycle = "control")]
pub enum UserRole {
    Table,
    UserId,
    RoleId,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(UserRole::Table)
        .if_not_exists()
        .col(column::uuid_fk(UserRole::UserId))
        .col(column::uuid_fk(UserRole::RoleId))
        .col(timestamp_with_write_default(UserRole::CreatedAt))
        .primary_key(Index::create().col(UserRole::UserId).col(UserRole::RoleId))
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
    vec![IndexSpec::sea_query(
        "idx_user_role_role",
        user_role_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_user_role_role")
            .table(UserRole::Table)
            .col(UserRole::RoleId)
            .to_owned(),
        "reverse lookup: users in a role",
    )]
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
