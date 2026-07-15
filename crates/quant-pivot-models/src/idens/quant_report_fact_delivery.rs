use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, Expr, ForeignKey, ForeignKeyAction, Index, Table, TableCreateStatement,
    },
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
        .check(Expr::cust(
            "bundle_bytes > 0 AND recommendation_row_count >= 0 AND funnel_row_count >= 0 AND attempt_count >= 0",
        ))
        .check(Expr::cust(
            "last_error IS NULL OR char_length(last_error) BETWEEN 1 AND 4096",
        ))
        .check(Expr::cust(
            "(claim_owner IS NULL) = (lease_expires_at IS NULL)",
        ))
        .check(Expr::cust(
            "(status = 'pending'::qp_report_fact_delivery_status AND attempt_count = 0 AND claim_owner IS NULL AND next_attempt_at IS NULL AND last_error IS NULL AND verified_at IS NULL AND announced_at IS NULL) OR status <> 'pending'::qp_report_fact_delivery_status",
        ))
        .check(Expr::cust(
            "(status = 'delivering'::qp_report_fact_delivery_status AND attempt_count > 0 AND claim_owner IS NOT NULL AND next_attempt_at IS NULL AND last_error IS NULL AND verified_at IS NULL AND announced_at IS NULL) OR status <> 'delivering'::qp_report_fact_delivery_status",
        ))
        .check(Expr::cust(
            "(status = 'retrying'::qp_report_fact_delivery_status AND attempt_count > 0 AND claim_owner IS NULL AND next_attempt_at IS NOT NULL AND last_error IS NOT NULL AND verified_at IS NULL AND announced_at IS NULL) OR status <> 'retrying'::qp_report_fact_delivery_status",
        ))
        .check(Expr::cust(
            "(status = 'failed'::qp_report_fact_delivery_status AND attempt_count > 0 AND claim_owner IS NULL AND next_attempt_at IS NULL AND last_error IS NOT NULL AND verified_at IS NULL AND announced_at IS NULL) OR status <> 'failed'::qp_report_fact_delivery_status",
        ))
        .check(Expr::cust(
            "(status = 'verified'::qp_report_fact_delivery_status AND attempt_count > 0 AND next_attempt_at IS NULL AND last_error IS NULL AND verified_at IS NOT NULL) OR status <> 'verified'::qp_report_fact_delivery_status",
        ))
        .check(Expr::cust(
            "(status = 'cancelled'::qp_report_fact_delivery_status AND claim_owner IS NULL AND next_attempt_at IS NULL AND verified_at IS NULL AND announced_at IS NULL) OR status <> 'cancelled'::qp_report_fact_delivery_status",
        ))
        .check(Expr::cust(
            "announced_at IS NULL OR (status = 'verified'::qp_report_fact_delivery_status AND claim_owner IS NULL AND announced_at >= verified_at)",
        ))
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
