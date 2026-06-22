use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table, TableCreateStatement,
    },
};

use crate::{
    idens::{
        quant_model_version::QuantModelVersion, quant_portfolio_plan::QuantPortfolioPlan,
        quant_universe_snapshot::QuantUniverseSnapshot,
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
    UniverseSnapshotId,
    PortfolioPlanId,
    TopN,
    Status,
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
            QuantRecommendationReport::UniverseSnapshotId,
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
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_recommendation_report_model_version")
                .from(
                    QuantRecommendationReport::Table,
                    QuantRecommendationReport::ModelVersionId,
                )
                .to(QuantModelVersion::Table, QuantModelVersion::ModelVersionId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_recommendation_report_universe")
                .from(
                    QuantRecommendationReport::Table,
                    QuantRecommendationReport::UniverseSnapshotId,
                )
                .to(
                    QuantUniverseSnapshot::Table,
                    QuantUniverseSnapshot::UniverseSnapshotId,
                )
                .on_delete(ForeignKeyAction::Restrict),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_recommendation_report_portfolio")
                .from(
                    QuantRecommendationReport::Table,
                    QuantRecommendationReport::PortfolioPlanId,
                )
                .to(
                    QuantPortfolioPlan::Table,
                    QuantPortfolioPlan::PortfolioPlanId,
                )
                .on_delete(ForeignKeyAction::Restrict),
        )
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
        TableDependency::foreign_key(quant_universe_snapshot_table_name),
        TableDependency::foreign_key(quant_portfolio_plan_table_name),
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

fn quant_universe_snapshot_table_name() -> String {
    QuantUniverseSnapshot::Table.to_string()
}

fn quant_portfolio_plan_table_name() -> String {
    QuantPortfolioPlan::Table.to_string()
}
