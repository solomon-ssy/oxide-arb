use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Index, Table, TableCreateStatement},
};

use crate::{
    enums::quant::ReportFactDeliveryStatus,
    idens::quant_recommendation_report::QuantRecommendationReport,
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[quant_schema(lifecycle = "ledger")]
pub enum QuantReportFactDelivery {
    Table,
    RecommendationReportId,
    Status,
    BundleUri,
    BundleHash,
    BundleBytes,
    RecommendationRowCount,
    RecommendationRowChainHash,
    FunnelRowCount,
    FunnelRowChainHash,
    AttemptCount,
    ClaimOwner,
    LeaseExpiresAt,
    NextAttemptAt,
    LastError,
    VerifiedAt,
    AnnouncedAt,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantReportFactDelivery::Table)
        .if_not_exists()
        .col(column::uuid_pk(
            QuantReportFactDelivery::RecommendationReportId,
        ))
        .col(column::pg_enum::<ReportFactDeliveryStatus>(
            QuantReportFactDelivery::Status,
        ))
        .col(
            ColumnDef::new(QuantReportFactDelivery::BundleUri)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantReportFactDelivery::BundleHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantReportFactDelivery::BundleBytes)
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantReportFactDelivery::RecommendationRowCount)
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantReportFactDelivery::RecommendationRowChainHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantReportFactDelivery::FunnelRowCount)
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantReportFactDelivery::FunnelRowChainHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantReportFactDelivery::AttemptCount)
                .integer()
                .not_null()
                .default(0),
        )
        .col(column::uuid_null(QuantReportFactDelivery::ClaimOwner))
        .col(
            ColumnDef::new(QuantReportFactDelivery::LeaseExpiresAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(QuantReportFactDelivery::NextAttemptAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(QuantReportFactDelivery::LastError)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(QuantReportFactDelivery::VerifiedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(QuantReportFactDelivery::AnnouncedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(timestamp_with_write_default(
            QuantReportFactDelivery::CreatedAt,
        ))
        .col(timestamp_with_write_default(
            QuantReportFactDelivery::UpdatedAt,
        ))
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_report_fact_delivery_report")
                .from(
                    QuantReportFactDelivery::Table,
                    QuantReportFactDelivery::RecommendationReportId,
                )
                .to(
                    QuantRecommendationReport::Table,
                    QuantRecommendationReport::RecommendationReportId,
                )
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_quant_report_fact_delivery_pending",
        table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_quant_report_fact_delivery_pending")
            .table(QuantReportFactDelivery::Table)
            .col(QuantReportFactDelivery::Status)
            .col(QuantReportFactDelivery::NextAttemptAt)
            .col(QuantReportFactDelivery::LeaseExpiresAt)
            .col(QuantReportFactDelivery::CreatedAt)
            .to_owned(),
        "claimable report fact delivery queue",
    )]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![TableDependency::foreign_key(report_table_name)]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn table_name() -> String {
    QuantReportFactDelivery::Table.to_string()
}

fn report_table_name() -> String {
    QuantRecommendationReport::Table.to_string()
}
