use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, Index, IndexOrder, Table, TableCreateStatement},
};

use crate::schema::{
    dependency::TableDependency,
    index::{IndexBuildMode, IndexSpec},
    seed::SeedSpec,
    timestamp_with_write_default,
};

#[oxide_schema(lifecycle = "audit")]
pub enum ControlFactorShadowDecision {
    Table,
    ShadowDecisionId,
    PublicationId,
    OpportunityId,
    MarketId,
    DecisionType,
    LiveDecision,
    ShadowDecision,
    Delta,
    DecidedAt,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(ControlFactorShadowDecision::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(ControlFactorShadowDecision::ShadowDecisionId)
                .text()
                .not_null()
                .primary_key(),
        )
        .col(
            ColumnDef::new(ControlFactorShadowDecision::PublicationId)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorShadowDecision::OpportunityId)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(ControlFactorShadowDecision::MarketId)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorShadowDecision::DecisionType)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorShadowDecision::LiveDecision)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorShadowDecision::ShadowDecision)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorShadowDecision::Delta)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorShadowDecision::DecidedAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            ControlFactorShadowDecision::CreatedAt,
        ))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_control_factor_shadow_decision_publication",
        shadow_decision_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_control_factor_shadow_decision_publication")
            .table(ControlFactorShadowDecision::Table)
            .col(ControlFactorShadowDecision::PublicationId)
            .col((ControlFactorShadowDecision::DecidedAt, IndexOrder::Desc))
            .to_owned(),
        "shadow decisions by publication and decision time",
    )]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn shadow_decision_table_name() -> String {
    ControlFactorShadowDecision::Table.to_string()
}
