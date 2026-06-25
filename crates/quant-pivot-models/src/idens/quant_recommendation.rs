use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Index, Table, TableCreateStatement},
};

use crate::{
    idens::{event::Event, market::Market, quant_recommendation_report::QuantRecommendationReport},
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[quant_schema(lifecycle = "report")]
pub enum QuantRecommendation {
    Table,
    RecommendationId,
    RecommendationReportId,
    Rank,
    MarketId,
    EventId,
    TokenId,
    Side,
    CompositeScore,
    RiskAdjustedScore,
    Confidence,
    EntryPlan,
    SizingPlan,
    ExitPlan,
    RiskEnvelope,
    FactorBreakdown,
    EvidenceRefs,
    ExecutionEligibility,
    ValidFrom,
    ValidUntil,
    Status,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantRecommendation::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantRecommendation::RecommendationId))
        .col(column::uuid_fk(QuantRecommendation::RecommendationReportId))
        .col(
            ColumnDef::new(QuantRecommendation::Rank)
                .integer()
                .not_null(),
        )
        .col(column::market_id(QuantRecommendation::MarketId))
        .col(column::text_id(QuantRecommendation::EventId))
        .col(column::token_id(QuantRecommendation::TokenId))
        .col(ColumnDef::new(QuantRecommendation::Side).text().not_null())
        .col(column::probability(QuantRecommendation::CompositeScore))
        .col(column::probability(QuantRecommendation::RiskAdjustedScore))
        .col(column::probability(QuantRecommendation::Confidence))
        .col(
            ColumnDef::new(QuantRecommendation::EntryPlan)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantRecommendation::SizingPlan)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantRecommendation::ExitPlan)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantRecommendation::RiskEnvelope)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantRecommendation::FactorBreakdown)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantRecommendation::EvidenceRefs)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantRecommendation::ExecutionEligibility)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantRecommendation::ValidFrom)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantRecommendation::ValidUntil)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantRecommendation::Status)
                .text()
                .not_null(),
        )
        .col(timestamp_with_write_default(QuantRecommendation::CreatedAt))
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_recommendation_report")
                .from(
                    QuantRecommendation::Table,
                    QuantRecommendation::RecommendationReportId,
                )
                .to(
                    QuantRecommendationReport::Table,
                    QuantRecommendationReport::RecommendationReportId,
                )
                .on_delete(ForeignKeyAction::Cascade),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_recommendation_market")
                .from(QuantRecommendation::Table, QuantRecommendation::MarketId)
                .to(Market::Table, Market::MarketId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_recommendation_event")
                .from(QuantRecommendation::Table, QuantRecommendation::EventId)
                .to(Event::Table, Event::EventId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "uq_quant_recommendation_report_rank",
            quant_recommendation_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uq_quant_recommendation_report_rank")
                .table(QuantRecommendation::Table)
                .col(QuantRecommendation::RecommendationReportId)
                .col(QuantRecommendation::Rank)
                .unique()
                .to_owned(),
            "one recommendation per rank inside a report",
        ),
        IndexSpec::sea_query(
            "uq_quant_recommendation_report_market_token",
            quant_recommendation_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uq_quant_recommendation_report_market_token")
                .table(QuantRecommendation::Table)
                .col(QuantRecommendation::RecommendationReportId)
                .col(QuantRecommendation::MarketId)
                .col(QuantRecommendation::TokenId)
                .unique()
                .to_owned(),
            "one recommendation per market token inside a report",
        ),
        IndexSpec::sea_query(
            "idx_quant_recommendation_market_status",
            quant_recommendation_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_recommendation_market_status")
                .table(QuantRecommendation::Table)
                .col(QuantRecommendation::MarketId)
                .col(QuantRecommendation::Status)
                .to_owned(),
            "recommendations by market and lifecycle status",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(quant_recommendation_report_table_name),
        TableDependency::foreign_key(market_table_name),
        TableDependency::foreign_key(event_table_name),
    ]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_recommendation_table_name() -> String {
    QuantRecommendation::Table.to_string()
}

fn quant_recommendation_report_table_name() -> String {
    QuantRecommendationReport::Table.to_string()
}

fn market_table_name() -> String {
    Market::Table.to_string()
}

fn event_table_name() -> String {
    Event::Table.to_string()
}
