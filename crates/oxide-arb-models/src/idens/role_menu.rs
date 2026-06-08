//! `role_menu` table — role→menu visibility assignments.

use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ForeignKey, ForeignKeyAction, Index, Table, TableCreateStatement},
};

use crate::{
    idens::{menu::Menu, role::Role},
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
    seed::rbac::role_menu,
};

/// Explicit role→menu join controlling which menu nodes a role can see.
///
/// The composite primary key `(role_id, menu_id)` is the natural key and its
/// own uniqueness guarantee — there is no surrogate join-row id.
#[oxide_schema(lifecycle = "control")]
pub enum RoleMenu {
    Table,
    RoleId,
    MenuId,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(RoleMenu::Table)
        .if_not_exists()
        .col(column::uuid_fk(RoleMenu::RoleId))
        .col(column::uuid_fk(RoleMenu::MenuId))
        .col(timestamp_with_write_default(RoleMenu::CreatedAt))
        .primary_key(Index::create().col(RoleMenu::RoleId).col(RoleMenu::MenuId))
        .foreign_key(
            ForeignKey::create()
                .name("fk_role_menu_role")
                .from(RoleMenu::Table, RoleMenu::RoleId)
                .to(Role::Table, Role::Id)
                .on_delete(ForeignKeyAction::Cascade),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_role_menu_menu")
                .from(RoleMenu::Table, RoleMenu::MenuId)
                .to(Menu::Table, Menu::Id)
                .on_delete(ForeignKeyAction::Cascade),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_role_menu_menu",
        role_menu_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_role_menu_menu")
            .table(RoleMenu::Table)
            .col(RoleMenu::MenuId)
            .to_owned(),
        "reverse lookup: roles that can see a menu",
    )]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(role_table_name),
        TableDependency::foreign_key(menu_table_name),
    ]
}

pub fn seed_units() -> Vec<SeedSpec> {
    vec![role_menu::ROLE_MENU_SEED]
}

pub fn role_menu_table_name() -> String {
    RoleMenu::Table.to_string()
}

fn role_table_name() -> String {
    Role::Table.to_string()
}

fn menu_table_name() -> String {
    Menu::Table.to_string()
}
