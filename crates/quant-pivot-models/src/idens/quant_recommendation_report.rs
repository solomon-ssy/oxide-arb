use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, ForeignKeyCreateStatement, Index, IndexOrder,
        IntoIden, Table, TableCreateStatement,
    },
};

use crate::{
    enums::quant::{
        AccountSource, QuantRuntimeMode, RecommendationReportStatus, ReportKind, ReportTriggerKind,
    },
    idens::{
        quant_account_snapshot::QuantAccountSnapshot, quant_equity_snapshot::QuantEquitySnapshot,
        quant_market_selection::QuantMarketSelection, quant_model_run::QuantModelRun,
        quant_model_version::QuantModelVersion, quant_portfolio_plan::QuantPortfolioPlan,
        quant_report_data_quality_snapshot::QuantReportDataQualitySnapshot,
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
    ProfileRef,
    ReportKind,
    TriggerKind,
    TriggerKey,
    TriggerTime,
    KnowledgeLagSecs,
    DecisionAt,
    HorizonSecs,
    RuntimeMode,
    RuntimeConfigVersionId,
    ModelRunId,
    ModelVersionId,
    MarketSelectionId,
    PortfolioPlanId,
    TopN,
    Status,
    AccountSource,
    CapitalBaseUsd,
    AccountSnapshotRef,
    EquitySnapshotRef,
    DataQualitySnapshotRef,
    SummaryJson,
    PublishedAt,
    ValidUntil,
    RevokedAt,
    ExpiredAt,
    StatusReason,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    let mut table = Table::create();

    table
        .table(QuantRecommendationReport::Table)
        .if_not_exists();
    add_identity_columns(&mut table);
    add_runtime_columns(&mut table);
    add_payload_columns(&mut table);
    add_lifecycle_columns(&mut table);
    add_foreign_keys(&mut table);

    table
}

fn add_identity_columns(table: &mut TableCreateStatement) {
    table
        .col(column::uuid_pk(
            QuantRecommendationReport::RecommendationReportId,
        ))
        .col(
            ColumnDef::new(QuantRecommendationReport::ProfileRef)
                .json_binary()
                .not_null(),
        )
        .col(column::pg_enum::<ReportKind>(
            QuantRecommendationReport::ReportKind,
        ))
        .col(column::pg_enum::<ReportTriggerKind>(
            QuantRecommendationReport::TriggerKind,
        ))
        .col(
            ColumnDef::new(QuantRecommendationReport::TriggerKey)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantRecommendationReport::TriggerTime)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantRecommendationReport::KnowledgeLagSecs)
                .big_integer()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            QuantRecommendationReport::CreatedAt,
        ));
}

fn add_runtime_columns(table: &mut TableCreateStatement) {
    table
        .col(
            ColumnDef::new(QuantRecommendationReport::DecisionAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantRecommendationReport::HorizonSecs)
                .big_integer()
                .not_null(),
        )
        .col(column::pg_enum::<QuantRuntimeMode>(
            QuantRecommendationReport::RuntimeMode,
        ))
        .col(column::uuid_fk(
            QuantRecommendationReport::RuntimeConfigVersionId,
        ))
        .col(column::uuid_null(QuantRecommendationReport::ModelRunId))
        .col(column::uuid_fk(QuantRecommendationReport::ModelVersionId))
        .col(column::uuid_fk(
            QuantRecommendationReport::MarketSelectionId,
        ))
        .col(column::uuid_fk(QuantRecommendationReport::PortfolioPlanId));
}

fn add_payload_columns(table: &mut TableCreateStatement) {
    table
        .col(
            ColumnDef::new(QuantRecommendationReport::TopN)
                .integer()
                .not_null(),
        )
        .col(column::pg_enum::<RecommendationReportStatus>(
            QuantRecommendationReport::Status,
        ))
        .col(column::pg_enum::<AccountSource>(
            QuantRecommendationReport::AccountSource,
        ))
        .col(column::usd(QuantRecommendationReport::CapitalBaseUsd))
        .col(column::uuid_fk(
            QuantRecommendationReport::AccountSnapshotRef,
        ))
        .col(column::uuid_fk(
            QuantRecommendationReport::EquitySnapshotRef,
        ))
        .col(column::uuid_fk(
            QuantRecommendationReport::DataQualitySnapshotRef,
        ))
        .col(
            ColumnDef::new(QuantRecommendationReport::SummaryJson)
                .json_binary()
                .not_null(),
        );
}

fn add_lifecycle_columns(table: &mut TableCreateStatement) {
    table
        .col(
            ColumnDef::new(QuantRecommendationReport::PublishedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(QuantRecommendationReport::ValidUntil)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(QuantRecommendationReport::RevokedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(QuantRecommendationReport::ExpiredAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(QuantRecommendationReport::StatusReason)
                .text()
                .null(),
        );
}

fn add_foreign_keys(table: &mut TableCreateStatement) {
    table
        .foreign_key(&mut fk_restrict(
            "fk_quant_recommendation_report_model_run",
            QuantRecommendationReport::ModelRunId,
            QuantModelRun::Table,
            QuantModelRun::ModelRunId,
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
        .foreign_key(&mut fk_restrict(
            "fk_quant_recommendation_report_equity_snapshot",
            QuantRecommendationReport::EquitySnapshotRef,
            QuantEquitySnapshot::Table,
            QuantEquitySnapshot::EquitySnapshotId,
        ))
        .foreign_key(&mut fk_restrict(
            "fk_quant_recommendation_report_dq_snapshot",
            QuantRecommendationReport::DataQualitySnapshotRef,
            QuantReportDataQualitySnapshot::Table,
            QuantReportDataQualitySnapshot::ReportDataQualitySnapshotId,
        ));
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
            "uq_quant_recommendation_report_trigger_key",
            quant_recommendation_report_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uq_quant_recommendation_report_trigger_key")
                .table(QuantRecommendationReport::Table)
                .col(QuantRecommendationReport::TriggerKey)
                .unique()
                .to_owned(),
            "report trigger idempotency key",
        ),
        IndexSpec::sea_query(
            "uq_quant_recommendation_report_model_run",
            quant_recommendation_report_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uq_quant_recommendation_report_model_run")
                .table(QuantRecommendationReport::Table)
                .col(QuantRecommendationReport::ModelRunId)
                .unique()
                .to_owned(),
            "one committed report per serving model run",
        ),
        IndexSpec::sea_query(
            "idx_quant_recommendation_report_trigger_time",
            quant_recommendation_report_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_recommendation_report_trigger_time")
                .table(QuantRecommendationReport::Table)
                .col(QuantRecommendationReport::TriggerKind)
                .col((QuantRecommendationReport::TriggerTime, IndexOrder::Desc))
                .to_owned(),
            "reports by trigger source and time",
        ),
        IndexSpec::sea_query(
            "idx_quant_recommendation_report_kind_decision_at",
            quant_recommendation_report_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_recommendation_report_kind_decision_at")
                .table(QuantRecommendationReport::Table)
                .col(QuantRecommendationReport::ReportKind)
                .col((QuantRecommendationReport::DecisionAt, IndexOrder::Desc))
                .to_owned(),
            "reports by kind and recency",
        ),
        IndexSpec::sea_query(
            "idx_quant_recommendation_report_decision_at_id",
            quant_recommendation_report_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_recommendation_report_decision_at_id")
                .table(QuantRecommendationReport::Table)
                .col(QuantRecommendationReport::DecisionAt)
                .col(QuantRecommendationReport::RecommendationReportId)
                .to_owned(),
            "deterministic runtime full-parity report enumeration",
        ),
        IndexSpec::sea_query(
            "idx_quant_recommendation_report_status_decision_at",
            quant_recommendation_report_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_recommendation_report_status_decision_at")
                .table(QuantRecommendationReport::Table)
                .col(QuantRecommendationReport::Status)
                .col((QuantRecommendationReport::DecisionAt, IndexOrder::Desc))
                .to_owned(),
            "reports by lifecycle status",
        ),
        IndexSpec::sea_query(
            "idx_quant_recommendation_report_model_decision_at",
            quant_recommendation_report_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_recommendation_report_model_decision_at")
                .table(QuantRecommendationReport::Table)
                .col(QuantRecommendationReport::ModelVersionId)
                .col((QuantRecommendationReport::DecisionAt, IndexOrder::Desc))
                .to_owned(),
            "reports by model version",
        ),
        IndexSpec::sea_query(
            "idx_quant_recommendation_report_valid_until",
            quant_recommendation_report_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_recommendation_report_valid_until")
                .table(QuantRecommendationReport::Table)
                .col(QuantRecommendationReport::Status)
                .col(QuantRecommendationReport::ValidUntil)
                .to_owned(),
            "report roll-up expiry backstop sweep",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(quant_model_run_table_name),
        TableDependency::foreign_key(quant_model_version_table_name),
        TableDependency::foreign_key(quant_market_selection_table_name),
        TableDependency::foreign_key(quant_portfolio_plan_table_name),
        TableDependency::foreign_key(quant_account_snapshot_table_name),
        TableDependency::foreign_key(quant_equity_snapshot_table_name),
        TableDependency::foreign_key(quant_report_data_quality_snapshot_table_name),
    ]
}

fn quant_report_data_quality_snapshot_table_name() -> String {
    QuantReportDataQualitySnapshot::Table.to_string()
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

fn quant_model_run_table_name() -> String {
    QuantModelRun::Table.to_string()
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

fn quant_equity_snapshot_table_name() -> String {
    QuantEquitySnapshot::Table.to_string()
}
