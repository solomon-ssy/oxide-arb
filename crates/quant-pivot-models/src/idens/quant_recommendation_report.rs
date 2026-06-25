use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, ForeignKeyCreateStatement, Index, IndexOrder,
        IntoIden, Table, TableCreateStatement,
    },
};

use crate::{
    idens::{
        quant_account_snapshot::QuantAccountSnapshot, quant_market_selection::QuantMarketSelection,
        quant_model_version::QuantModelVersion, quant_portfolio_plan::QuantPortfolioPlan,
    },
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[quant_schema(lifecycle = "report")]
pub enum QuantRecommendationReport {
    Table,
    RecommendationReportId,
    ReportKind,
    AsOf,
    HorizonSecs,
    RuntimeMode,
    RuntimeConfigVersionId,
    ModelVersionId,
    MarketSelectionId,
    PortfolioPlanId,
    TopN,
    Status,
    AccountSource,
    CapitalBaseUsd,
    AccountSnapshotRef,
    SummaryJson,
    PublishedAt,
    RevokedAt,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantRecommendationReport::Table)
        .if_not_exists()
        .col(column::uuid_pk(
            QuantRecommendationReport::RecommendationReportId,
        ))
        .col(
            ColumnDef::new(QuantRecommendationReport::ReportKind)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantRecommendationReport::AsOf)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantRecommendationReport::HorizonSecs)
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantRecommendationReport::RuntimeMode)
                .text()
                .not_null(),
        )
        .col(column::uuid_fk(
            QuantRecommendationReport::RuntimeConfigVersionId,
        ))
        .col(column::uuid_fk(QuantRecommendationReport::ModelVersionId))
        .col(column::uuid_fk(
            QuantRecommendationReport::MarketSelectionId,
        ))
        .col(column::uuid_fk(QuantRecommendationReport::PortfolioPlanId))
        .col(
            ColumnDef::new(QuantRecommendationReport::TopN)
                .integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantRecommendationReport::Status)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantRecommendationReport::AccountSource)
                .text()
                .not_null(),
        )
        .col(column::usd(QuantRecommendationReport::CapitalBaseUsd))
        .col(column::uuid_fk(
            QuantRecommendationReport::AccountSnapshotRef,
        ))
        .col(
            ColumnDef::new(QuantRecommendationReport::SummaryJson)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantRecommendationReport::PublishedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(QuantRecommendationReport::RevokedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(timestamp_with_write_default(
            QuantRecommendationReport::CreatedAt,
        ))
        .foreign_key(&mut fk_restrict(
            "fk_quant_recommendation_report_model_version",
            QuantRecommendationReport::ModelVersionId,
            QuantModelVersion::Table,
            QuantModelVersion::ModelVersionId,
        ))
        .foreign_key(&mut fk_restrict(
            "fk_quant_recommendation_report_selection",
            QuantRecommendationReport::MarketSelectionId,
            QuantMarketSelection::Table,
            QuantMarketSelection::MarketSelectionId,
        ))
        .foreign_key(&mut fk_restrict(
            "fk_quant_recommendation_report_portfolio",
            QuantRecommendationReport::PortfolioPlanId,
            QuantPortfolioPlan::Table,
            QuantPortfolioPlan::PortfolioPlanId,
        ))
        .foreign_key(&mut fk_restrict(
            "fk_quant_recommendation_report_account_snapshot",
            QuantRecommendationReport::AccountSnapshotRef,
            QuantAccountSnapshot::Table,
            QuantAccountSnapshot::AccountSnapshotId,
        ))
        .to_owned()
}

/// Build a `RESTRICT` foreign key from a report column to another table's key.
fn fk_restrict(
    name: &str,
    from_col: QuantRecommendationReport,
    to_table: impl IntoIden + 'static,
    to_col: impl IntoIden + 'static,
) -> ForeignKeyCreateStatement {
    ForeignKey::create()
        .name(name)
        .from(QuantRecommendationReport::Table, from_col)
        .to(to_table, to_col)
        .on_delete(ForeignKeyAction::Restrict)
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_quant_recommendation_report_kind_as_of",
            quant_recommendation_report_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_recommendation_report_kind_as_of")
                .table(QuantRecommendationReport::Table)
                .col(QuantRecommendationReport::ReportKind)
                .col((QuantRecommendationReport::AsOf, IndexOrder::Desc))
                .to_owned(),
            "reports by kind and recency",
        ),
        IndexSpec::sea_query(
            "idx_quant_recommendation_report_status_as_of",
            quant_recommendation_report_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_recommendation_report_status_as_of")
                .table(QuantRecommendationReport::Table)
                .col(QuantRecommendationReport::Status)
                .col((QuantRecommendationReport::AsOf, IndexOrder::Desc))
                .to_owned(),
            "reports by lifecycle status",
        ),
        IndexSpec::sea_query(
            "idx_quant_recommendation_report_model_as_of",
            quant_recommendation_report_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_recommendation_report_model_as_of")
                .table(QuantRecommendationReport::Table)
                .col(QuantRecommendationReport::ModelVersionId)
                .col((QuantRecommendationReport::AsOf, IndexOrder::Desc))
                .to_owned(),
            "reports by model version",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(quant_model_version_table_name),
        TableDependency::foreign_key(quant_market_selection_table_name),
        TableDependency::foreign_key(quant_portfolio_plan_table_name),
        TableDependency::foreign_key(quant_account_snapshot_table_name),
    ]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_recommendation_report_table_name() -> String {
    QuantRecommendationReport::Table.to_string()
}

fn quant_model_version_table_name() -> String {
    QuantModelVersion::Table.to_string()
}

fn quant_market_selection_table_name() -> String {
    QuantMarketSelection::Table.to_string()
}

fn quant_portfolio_plan_table_name() -> String {
    QuantPortfolioPlan::Table.to_string()
}

fn quant_account_snapshot_table_name() -> String {
    QuantAccountSnapshot::Table.to_string()
}
