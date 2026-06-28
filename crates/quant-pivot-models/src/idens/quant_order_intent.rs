use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, Expr, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table,
        TableCreateStatement,
    },
};

use crate::{
    enums::{
        execution::{ExitReason, ExitState, OrderIntentKind},
        quant::{ApprovalStatus, OrderIntentStatus, QuantRuntimeMode},
    },
    idens::{
        quant_model_version::QuantModelVersion, quant_recommendation::QuantRecommendation,
        runtime_config_version::RuntimeConfigVersion,
    },
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[quant_schema(lifecycle = "ledger")]
pub enum QuantOrderIntent {
    Table,
    OrderIntentId,
    RecommendationId,
    RuntimeMode,
    RuntimeConfigVersionId,
    ModelVersionId,
    IntentKind,
    Status,
    ApprovalStatus,
    ApprovedBy,
    ApprovalReason,
    ApprovedAt,
    PolicyId,
    PolicyHash,
    StatusReason,
    AdmissionTraceRef,
    EntryOrderJson,
    ExitPolicyJson,
    RiskEnvelopeHash,
    ExpiresAt,
    ExitState,
    ExitReason,
    NextCheckAt,
    PeakMarkPrice,
    LastSignalRecheckAt,
    ExecutedPartialExitNodeIds,
    PendingPartialExitNodeId,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    let mut stmt = Table::create();
    stmt.table(QuantOrderIntent::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantOrderIntent::OrderIntentId))
        .col(column::uuid_fk(QuantOrderIntent::RecommendationId))
        .col(column::pg_enum::<QuantRuntimeMode>(
            QuantOrderIntent::RuntimeMode,
        ))
        .col(column::uuid_fk(QuantOrderIntent::RuntimeConfigVersionId))
        .col(column::uuid_fk(QuantOrderIntent::ModelVersionId))
        .col(column::pg_enum::<OrderIntentKind>(
            QuantOrderIntent::IntentKind,
        ))
        .col(column::pg_enum::<OrderIntentStatus>(
            QuantOrderIntent::Status,
        ))
        .col(column::pg_enum::<ApprovalStatus>(
            QuantOrderIntent::ApprovalStatus,
        ))
        .col(column::uuid_null(QuantOrderIntent::ApprovedBy))
        .col(
            ColumnDef::new(QuantOrderIntent::ApprovalReason)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(QuantOrderIntent::ApprovedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(ColumnDef::new(QuantOrderIntent::PolicyId).text().null())
        .col(ColumnDef::new(QuantOrderIntent::PolicyHash).text().null())
        .col(ColumnDef::new(QuantOrderIntent::StatusReason).text().null())
        .col(
            ColumnDef::new(QuantOrderIntent::AdmissionTraceRef)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(QuantOrderIntent::EntryOrderJson)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantOrderIntent::ExitPolicyJson)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantOrderIntent::RiskEnvelopeHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantOrderIntent::ExpiresAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(column::pg_enum_default::<ExitState>(
            QuantOrderIntent::ExitState,
            &ExitState::NotStarted,
        ))
        .col(column::pg_enum_null::<ExitReason>(
            QuantOrderIntent::ExitReason,
        ))
        .col(
            ColumnDef::new(QuantOrderIntent::NextCheckAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(column::price_null(QuantOrderIntent::PeakMarkPrice))
        .col(
            ColumnDef::new(QuantOrderIntent::LastSignalRecheckAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(QuantOrderIntent::ExecutedPartialExitNodeIds)
                .json_binary()
                .not_null()
                .default(Expr::cust("'[]'::jsonb")),
        )
        .col(
            ColumnDef::new(QuantOrderIntent::PendingPartialExitNodeId)
                .text()
                .null(),
        )
        .col(timestamp_with_write_default(QuantOrderIntent::CreatedAt))
        .col(timestamp_with_write_default(QuantOrderIntent::UpdatedAt));
    add_foreign_keys(&mut stmt);
    stmt
}

/// The three governed references an order intent freezes (recommendation,
/// config version, model version), all `ON DELETE RESTRICT`.
fn add_foreign_keys(stmt: &mut TableCreateStatement) {
    stmt.foreign_key(
        ForeignKey::create()
            .name("fk_quant_order_intent_recommendation")
            .from(QuantOrderIntent::Table, QuantOrderIntent::RecommendationId)
            .to(
                QuantRecommendation::Table,
                QuantRecommendation::RecommendationId,
            )
            .on_delete(ForeignKeyAction::Restrict),
    )
    .foreign_key(
        ForeignKey::create()
            .name("fk_quant_order_intent_runtime_config_version")
            .from(
                QuantOrderIntent::Table,
                QuantOrderIntent::RuntimeConfigVersionId,
            )
            .to(
                RuntimeConfigVersion::Table,
                RuntimeConfigVersion::RuntimeConfigVersionId,
            )
            .on_delete(ForeignKeyAction::Restrict),
    )
    .foreign_key(
        ForeignKey::create()
            .name("fk_quant_order_intent_model_version")
            .from(QuantOrderIntent::Table, QuantOrderIntent::ModelVersionId)
            .to(QuantModelVersion::Table, QuantModelVersion::ModelVersionId)
            .on_delete(ForeignKeyAction::Restrict),
    );
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_quant_order_intent_recommendation",
            quant_order_intent_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_order_intent_recommendation")
                .table(QuantOrderIntent::Table)
                .col(QuantOrderIntent::RecommendationId)
                .to_owned(),
            "order intents by recommendation",
        ),
        IndexSpec::sea_query(
            "idx_quant_order_intent_status_expires",
            quant_order_intent_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_order_intent_status_expires")
                .table(QuantOrderIntent::Table)
                .col(QuantOrderIntent::Status)
                .col((QuantOrderIntent::ExpiresAt, IndexOrder::Asc))
                .to_owned(),
            "order intents by status and expiry",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(quant_recommendation_table_name),
        TableDependency::foreign_key(runtime_config_version_table_name),
        TableDependency::foreign_key(quant_model_version_table_name),
    ]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_order_intent_table_name() -> String {
    QuantOrderIntent::Table.to_string()
}

fn quant_recommendation_table_name() -> String {
    QuantRecommendation::Table.to_string()
}

fn runtime_config_version_table_name() -> String {
    RuntimeConfigVersion::Table.to_string()
}

fn quant_model_version_table_name() -> String {
    QuantModelVersion::Table.to_string()
}
