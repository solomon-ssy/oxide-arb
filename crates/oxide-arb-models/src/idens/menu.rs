//! `menu` table — frontend navigation tree + button-level permission points.

use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, Index, Table, TableCreateStatement},
};

use crate::{
    enums::rbac::RoleStatus,
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
    seed::rbac::menus,
};

/// Navigation menu tree node.
///
/// `parent_id` forms a self-referential tree (root nodes have a NULL parent).
/// Tree integrity is maintained by the application; there is no self-FK so menu
/// deletion does not require cascade reasoning at the database level.
#[oxide_schema(lifecycle = "control")]
pub enum Menu {
    Table,
    Id,
    ParentId,
    Name,
    Kind,
    Path,
    Component,
    Title,
    Icon,
    PermissionCode,
    Sort,
    KeepAlive,
    HideInMenu,
    Status,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(Menu::Table)
        .if_not_exists()
        .col(column::uuid_pk(Menu::Id))
        .col(column::uuid_null(Menu::ParentId))
        .col(ColumnDef::new(Menu::Name).text().not_null())
        .col(ColumnDef::new(Menu::Kind).text().not_null())
        .col(ColumnDef::new(Menu::Path).text().null())
        .col(ColumnDef::new(Menu::Component).text().null())
        .col(ColumnDef::new(Menu::Title).text().not_null())
        .col(ColumnDef::new(Menu::Icon).text().null())
        .col(ColumnDef::new(Menu::PermissionCode).text().null())
        .col(ColumnDef::new(Menu::Sort).integer().not_null().default(0))
        .col(
            ColumnDef::new(Menu::KeepAlive)
                .boolean()
                .not_null()
                .default(false),
        )
        .col(
            ColumnDef::new(Menu::HideInMenu)
                .boolean()
                .not_null()
                .default(false),
        )
        .col(
            ColumnDef::new(Menu::Status)
                .text()
                .not_null()
                .default(RoleStatus::Enabled),
        )
        .col(timestamp_with_write_default(Menu::CreatedAt))
        .col(timestamp_with_write_default(Menu::UpdatedAt))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_menu_parent",
        menu_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_menu_parent")
            .table(Menu::Table)
            .col(Menu::ParentId)
            .col(Menu::Sort)
            .to_owned(),
        "menu children in display order",
    )]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub fn seed_units() -> Vec<SeedSpec> {
    vec![menus::MENUS_SEED]
}

pub fn menu_table_name() -> String {
    Menu::Table.to_string()
}
