use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Index, Table, TableCreateStatement},
};

use crate::{
    idens::{
        control_factor_publication::ControlFactorPublication,
        control_factor_value::ControlFactorValue,
    },
    schema::{
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[oxide_schema(lifecycle = "control")]
pub enum ControlFactorPublicationFactor {
    Table,
    Id,
    PublicationId,
    FactorId,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(ControlFactorPublicationFactor::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(ControlFactorPublicationFactor::Id)
                .big_integer()
                .not_null()
                .auto_increment()
                .primary_key(),
        )
        .col(
            ColumnDef::new(ControlFactorPublicationFactor::PublicationId)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorPublicationFactor::FactorId)
                .text()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            ControlFactorPublicationFactor::CreatedAt,
        ))
        .foreign_key(
            ForeignKey::create()
                .name("fk_control_factor_publication_factor_publication")
                .from(
                    ControlFactorPublicationFactor::Table,
                    ControlFactorPublicationFactor::PublicationId,
                )
                .to(
                    ControlFactorPublication::Table,
                    ControlFactorPublication::PublicationId,
                )
                .on_delete(ForeignKeyAction::Cascade),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_control_factor_publication_factor_factor")
                .from(
                    ControlFactorPublicationFactor::Table,
                    ControlFactorPublicationFactor::FactorId,
                )
                .to(ControlFactorValue::Table, ControlFactorValue::FactorId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_control_factor_publication_factor_pub",
            publication_factor_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_control_factor_publication_factor_pub")
                .table(ControlFactorPublicationFactor::Table)
                .col(ControlFactorPublicationFactor::PublicationId)
                .to_owned(),
            "publication membership by publication",
        ),
        IndexSpec::raw(
            "idx_control_factor_publication_factor_unique",
            publication_factor_table_name,
            IndexBuildMode::Transactional,
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_control_factor_publication_factor_unique \
             ON control_factor_publication_factor (publication_id, factor_id)",
            "prevent duplicate factor membership inside one publication",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(publication_table_name),
        TableDependency::foreign_key(factor_value_table_name),
    ]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn publication_factor_table_name() -> String {
    ControlFactorPublicationFactor::Table.to_string()
}

fn publication_table_name() -> String {
    ControlFactorPublication::Table.to_string()
}

fn factor_value_table_name() -> String {
    ControlFactorValue::Table.to_string()
}
