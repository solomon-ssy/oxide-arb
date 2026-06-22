use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table, TableCreateStatement,
    },
};

use crate::{
    idens::control_factor_publication::ControlFactorPublication,
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[quant_schema(lifecycle = "audit")]
pub enum ControlFactorShadowDecision {
    Table,
    ShadowDecisionId,
    PublicationId,
    OpportunityId,
    EventId,
    MarketId,
    DecisionType,
    BaselineDecision,
    ShadowDecision,
    Delta,
    AffectedFactorIds,
    DecidedAt,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(ControlFactorShadowDecision::Table)
        .if_not_exists()
        .col(column::uuid_pk(
            ControlFactorShadowDecision::ShadowDecisionId,
        ))
        .col(column::uuid_fk(ControlFactorShadowDecision::PublicationId))
        .col(column::uuid_fk(ControlFactorShadowDecision::OpportunityId))
        .col(column::text_id(ControlFactorShadowDecision::EventId))
        .col(column::market_id(ControlFactorShadowDecision::MarketId))
        .col(
            ColumnDef::new(ControlFactorShadowDecision::DecisionType)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorShadowDecision::BaselineDecision)
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
            ColumnDef::new(ControlFactorShadowDecision::AffectedFactorIds)
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
        .foreign_key(
            ForeignKey::create()
                .name("fk_control_factor_shadow_decision_publication")
                .from(
                    ControlFactorShadowDecision::Table,
                    ControlFactorShadowDecision::PublicationId,
                )
                .to(
                    ControlFactorPublication::Table,
                    ControlFactorPublication::PublicationId,
                )
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
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
        ),
        IndexSpec::sea_query(
            "idx_control_factor_shadow_decision_market",
            shadow_decision_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_control_factor_shadow_decision_market")
                .table(ControlFactorShadowDecision::Table)
                .col(ControlFactorShadowDecision::MarketId)
                .col((ControlFactorShadowDecision::DecidedAt, IndexOrder::Desc))
                .to_owned(),
            "shadow decisions by market and decision time",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![TableDependency::foreign_key(publication_table_name)]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn shadow_decision_table_name() -> String {
    ControlFactorShadowDecision::Table.to_string()
}

fn publication_table_name() -> String {
    ControlFactorPublication::Table.to_string()
}
