//! `casbin_rule` table — Casbin policy storage (`p` and `g` lines).
//!
//! Unlike the rest of the schema this table keeps an integer auto-increment PK
//! (Casbin's adapter convention) rather than a `TypedId`. The unique index over
//! `(ptype, v0..v5)` is the DB-side guarantee of exact policy de-duplication
//! (fixing ng-gateway's `ptype`-only de-dup defect).

use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, Index, Table, TableCreateStatement},
};

use crate::{
    schema::{
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
    },
    seed::rbac::casbin,
};

/// Casbin policy rows. `ptype` is `p` (permission) or `g` (grouping); `v0..v5`
/// hold the policy tuple fields.
#[oxide_schema(lifecycle = "control")]
pub enum CasbinRule {
    Table,
    Id,
    Ptype,
    V0,
    V1,
    V2,
    V3,
    V4,
    V5,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(CasbinRule::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(CasbinRule::Id)
                .big_integer()
                .not_null()
                .auto_increment()
                .primary_key(),
        )
        .col(ColumnDef::new(CasbinRule::Ptype).text().not_null())
        .col(casbin_value_column(CasbinRule::V0))
        .col(casbin_value_column(CasbinRule::V1))
        .col(casbin_value_column(CasbinRule::V2))
        .col(casbin_value_column(CasbinRule::V3))
        .col(casbin_value_column(CasbinRule::V4))
        .col(casbin_value_column(CasbinRule::V5))
        .to_owned()
}

/// A Casbin policy value column.
///
/// `NOT NULL DEFAULT ''` (rather than nullable) so the `uq_casbin_rule` unique
/// index de-duplicates exactly under standard semantics — Postgres treats NULLs
/// as distinct, which would silently defeat de-duplication on unused fields.
fn casbin_value_column(column: CasbinRule) -> ColumnDef {
    let mut col = ColumnDef::new(column);
    col.text().not_null().default("");
    col
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_casbin_ptype",
            casbin_rule_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_casbin_ptype")
                .table(CasbinRule::Table)
                .col(CasbinRule::Ptype)
                .to_owned(),
            "policy lines by type",
        ),
        IndexSpec::sea_query(
            "idx_casbin_v0",
            casbin_rule_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_casbin_v0")
                .table(CasbinRule::Table)
                .col(CasbinRule::V0)
                .to_owned(),
            "policy lines by subject (role code / user id)",
        ),
        IndexSpec::sea_query(
            "uq_casbin_rule",
            casbin_rule_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uq_casbin_rule")
                .table(CasbinRule::Table)
                .col(CasbinRule::Ptype)
                .col(CasbinRule::V0)
                .col(CasbinRule::V1)
                .col(CasbinRule::V2)
                .col(CasbinRule::V3)
                .col(CasbinRule::V4)
                .col(CasbinRule::V5)
                .unique()
                .to_owned(),
            "exact policy de-duplication (ptype, v0..v5)",
        ),
    ]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub fn seed_units() -> Vec<SeedSpec> {
    vec![casbin::CASBIN_SEED]
}

pub fn casbin_rule_table_name() -> String {
    CasbinRule::Table.to_string()
}
