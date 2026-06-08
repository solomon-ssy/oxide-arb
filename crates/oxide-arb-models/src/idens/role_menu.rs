//! `role_menu` table — role→menu visibility assignments.

use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Index, Table, TableCreateStatement},
};

use crate::{
    idens::{menu::Menu, role::Role},
    schema::{
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
    seed::rbac::role_menu,
};

/// Explicit role→menu join controlling which menu nodes a role can see.
#[oxide_schema(lifecycle = "control")]
pub enum RoleMenu {
    Table,
    Id,
    RoleId,
    MenuId,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(RoleMenu::Table)
        .if_not_exists()
        .col(ColumnDef::new(RoleMenu::Id).text().not_null().primary_key())
        .col(ColumnDef::new(RoleMenu::RoleId).text().not_null())
        .col(ColumnDef::new(RoleMenu::MenuId).text().not_null())
        .col(timestamp_with_write_default(RoleMenu::CreatedAt))
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
        "uq_role_menu",
        role_menu_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("uq_role_menu")
            .table(RoleMenu::Table)
            .col(RoleMenu::RoleId)
            .col(RoleMenu::MenuId)
            .unique()
            .to_owned(),
        "one assignment per (role, menu)",
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
