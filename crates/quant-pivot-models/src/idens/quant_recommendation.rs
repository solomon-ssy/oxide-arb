use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, ForeignKeyCreateStatement, Index, IntoIden, Table,
        TableCreateStatement,
    },
};

use crate::{
    enums::quant::{OutcomeSide, RecommendationStatus},
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
    ProfileRef,
    RecommendationReportId,
    Rank,
    MarketId,
    EventId,
    TokenId,
    OutcomeSide,
    CompositeScore,
    RiskAdjustedScore,
    Confidence,
    ExpectedReturnBps,
    DownsideBps,
    Identity,
    MarketContext,
    RankBeforePortfolio,
    LiquidityScore,
    DataQualityScore,
    ModelScorePercentile,
    TradePlan,
    FactorBreakdown,
    EvidenceRefs,
    ExecutionEligibility,
    ValidFrom,
    ValidUntil,
    Status,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    let mut table = Table::create();

    table.table(QuantRecommendation::Table).if_not_exists();
    add_identity_columns(&mut table);
    add_score_columns(&mut table);
    add_context_columns(&mut table);
    add_plan_columns(&mut table);
    add_lifecycle_columns(&mut table);
    add_foreign_keys(&mut table);

    table
}

fn add_identity_columns(table: &mut TableCreateStatement) {
    table
        .col(column::uuid_pk(QuantRecommendation::RecommendationId))
        .col(
            ColumnDef::new(QuantRecommendation::ProfileRef)
                .json_binary()
                .not_null(),
        )
        .col(column::uuid_fk(QuantRecommendation::RecommendationReportId))
        .col(
            ColumnDef::new(QuantRecommendation::Rank)
                .integer()
                .not_null(),
        )
        .col(column::market_id(QuantRecommendation::MarketId))
        .col(column::text_id(QuantRecommendation::EventId))
        .col(column::token_id(QuantRecommendation::TokenId))
        .col(column::pg_enum::<OutcomeSide>(
            QuantRecommendation::OutcomeSide,
        ));
}

fn add_score_columns(table: &mut TableCreateStatement) {
    table
        .col(column::probability(QuantRecommendation::CompositeScore))
        .col(column::probability(QuantRecommendation::RiskAdjustedScore))
        .col(column::probability(QuantRecommendation::Confidence))
        .col(column::bps(QuantRecommendation::ExpectedReturnBps))
        .col(column::bps(QuantRecommendation::DownsideBps));
}

fn add_context_columns(table: &mut TableCreateStatement) {
    table
        .col(
            ColumnDef::new(QuantRecommendation::Identity)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantRecommendation::MarketContext)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantRecommendation::RankBeforePortfolio)
                .integer()
                .not_null(),
        )
        .col(column::probability(QuantRecommendation::LiquidityScore))
        .col(column::probability(QuantRecommendation::DataQualityScore))
        .col(column::probability(
            QuantRecommendation::ModelScorePercentile,
        ));
}

fn add_plan_columns(table: &mut TableCreateStatement) {
    table
        .col(
            ColumnDef::new(QuantRecommendation::TradePlan)
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
        );
}

fn add_lifecycle_columns(table: &mut TableCreateStatement) {
    table
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
        .col(column::pg_enum::<RecommendationStatus>(
            QuantRecommendation::Status,
        ))
        .col(timestamp_with_write_default(QuantRecommendation::CreatedAt));
}

fn add_foreign_keys(table: &mut TableCreateStatement) {
    table
        .foreign_key(&mut fk_cascade(
            "fk_quant_recommendation_report",
            QuantRecommendation::RecommendationReportId,
            QuantRecommendationReport::Table,
            QuantRecommendationReport::RecommendationReportId,
        ))
        .foreign_key(&mut fk_restrict(
            "fk_quant_recommendation_market",
            QuantRecommendation::MarketId,
            Market::Table,
            Market::MarketId,
        ))
        .foreign_key(&mut fk_restrict(
            "fk_quant_recommendation_event",
            QuantRecommendation::EventId,
            Event::Table,
            Event::EventId,
        ));
}

fn fk_cascade(
    name: &str,
    from_col: QuantRecommendation,
    to_table: impl IntoIden + 'static,
    to_col: impl IntoIden + 'static,
) -> ForeignKeyCreateStatement {
    ForeignKey::create()
        .name(name)
        .from(QuantRecommendation::Table, from_col)
        .to(to_table, to_col)
        .on_delete(ForeignKeyAction::Cascade)
        .to_owned()
}

fn fk_restrict(
    name: &str,
    from_col: QuantRecommendation,
    to_table: impl IntoIden + 'static,
    to_col: impl IntoIden + 'static,
) -> ForeignKeyCreateStatement {
    ForeignKey::create()
        .name(name)
        .from(QuantRecommendation::Table, from_col)
        .to(to_table, to_col)
        .on_delete(ForeignKeyAction::Restrict)
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
        IndexSpec::sea_query(
            "idx_quant_recommendation_status_valid_until",
            quant_recommendation_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_recommendation_status_valid_until")
                .table(QuantRecommendation::Table)
                .col(QuantRecommendation::Status)
                .col(QuantRecommendation::ValidUntil)
                .to_owned(),
            "per-recommendation TTL expiry sweep / deadline scheduler",
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
