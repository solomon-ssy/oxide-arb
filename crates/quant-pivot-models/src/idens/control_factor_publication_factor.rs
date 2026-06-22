use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ForeignKey, ForeignKeyAction, Index, Table, TableCreateStatement},
};

use crate::{
    idens::{
        control_factor_publication::ControlFactorPublication,
        control_factor_value::ControlFactorValue,
    },
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

/// Publication↔factor membership join.
///
/// The composite primary key `(publication_id, factor_id)` is the natural key
/// and its own uniqueness guarantee — there is no surrogate join-row id.
#[oxide_schema(lifecycle = "control")]
pub enum ControlFactorPublicationFactor {
    Table,
    PublicationId,
    FactorId,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(ControlFactorPublicationFactor::Table)
        .if_not_exists()
        .col(column::uuid_fk(
            ControlFactorPublicationFactor::PublicationId,
        ))
        .col(column::uuid_fk(ControlFactorPublicationFactor::FactorId))
        .col(timestamp_with_write_default(
            ControlFactorPublicationFactor::CreatedAt,
        ))
        .primary_key(
            Index::create()
                .col(ControlFactorPublicationFactor::PublicationId)
                .col(ControlFactorPublicationFactor::FactorId),
        )
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
    vec![IndexSpec::sea_query(
        "idx_control_factor_publication_factor_factor",
        publication_factor_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_control_factor_publication_factor_factor")
            .table(ControlFactorPublicationFactor::Table)
            .col(ControlFactorPublicationFactor::FactorId)
            .to_owned(),
        "reverse lookup: publications containing a factor",
    )]
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
