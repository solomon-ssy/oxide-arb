use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Table, TableCreateStatement},
};

use crate::{
    enums::quant::RecommendationOutcome,
    idens::quant_recommendation::QuantRecommendation,
    schema::{
        column, dependency::TableDependency, index::IndexSpec, seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[quant_schema(lifecycle = "audit")]
pub enum QuantRecommendationAttribution {
    Table,
    RecommendationId,
    Outcome,
    EntryOutcomeJson,
    ExitOutcomeJson,
    RealizedPnlUsd,
    MaxAdverseExcursionBps,
    MaxFavorableExcursionBps,
    LabelAvailableAt,
    AttributionJson,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantRecommendationAttribution::Table)
        .if_not_exists()
        .col(column::uuid_pk(
            QuantRecommendationAttribution::RecommendationId,
        ))
        .col(column::pg_enum::<RecommendationOutcome>(
            QuantRecommendationAttribution::Outcome,
        ))
        .col(
            ColumnDef::new(QuantRecommendationAttribution::EntryOutcomeJson)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantRecommendationAttribution::ExitOutcomeJson)
                .json_binary()
                .not_null(),
        )
        .col(column::usd_null(
            QuantRecommendationAttribution::RealizedPnlUsd,
        ))
        .col(
            ColumnDef::new(QuantRecommendationAttribution::MaxAdverseExcursionBps)
                .decimal_len(20, 8)
                .null(),
        )
        .col(
            ColumnDef::new(QuantRecommendationAttribution::MaxFavorableExcursionBps)
                .decimal_len(20, 8)
                .null(),
        )
        .col(
            ColumnDef::new(QuantRecommendationAttribution::LabelAvailableAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(QuantRecommendationAttribution::AttributionJson)
                .json_binary()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            QuantRecommendationAttribution::CreatedAt,
        ))
        .col(timestamp_with_write_default(
            QuantRecommendationAttribution::UpdatedAt,
        ))
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_recommendation_attribution")
                .from(
                    QuantRecommendationAttribution::Table,
                    QuantRecommendationAttribution::RecommendationId,
                )
                .to(
                    QuantRecommendation::Table,
                    QuantRecommendation::RecommendationId,
                )
                .on_delete(ForeignKeyAction::Cascade),
        )
        .to_owned()
}

pub const fn indexes() -> Vec<IndexSpec> {
    Vec::new()
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![TableDependency::foreign_key(
        quant_recommendation_table_name,
    )]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_recommendation_table_name() -> String {
    QuantRecommendation::Table.to_string()
}
