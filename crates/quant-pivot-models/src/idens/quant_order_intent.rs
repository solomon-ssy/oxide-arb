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
        quant_entry_condition_instance::QuantEntryConditionInstance,
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
    ConditionInstanceId,
    EntryOrderJson,
    ExitPolicyJson,
    RiskEnvelopeHash,
    ExpiresAt,
    ExitState,
    ExitReason,
    NextCheckAt,
    PeakMarkPrice,
    LastSignalRecheckAt,
    LatestReinferenceJson,
    ScaleOutState,
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
        ));
    add_approval_columns(&mut stmt);
    add_frozen_policy_columns(&mut stmt);
    add_exit_columns(&mut stmt);
    stmt.col(timestamp_with_write_default(QuantOrderIntent::CreatedAt))
        .col(timestamp_with_write_default(QuantOrderIntent::UpdatedAt));
    add_foreign_keys(&mut stmt);
    stmt
}

fn add_approval_columns(stmt: &mut TableCreateStatement) {
    stmt.col(column::uuid_null(QuantOrderIntent::ApprovedBy))
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
        .col(ColumnDef::new(QuantOrderIntent::StatusReason).text().null());
}

fn add_frozen_policy_columns(stmt: &mut TableCreateStatement) {
    stmt.col(
        ColumnDef::new(QuantOrderIntent::AdmissionTraceRef)
            .text()
            .null(),
    )
    .col(column::uuid_fk(QuantOrderIntent::ConditionInstanceId))
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
    );
}

fn add_exit_columns(stmt: &mut TableCreateStatement) {
    stmt.col(column::pg_enum_default::<ExitState>(
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
        ColumnDef::new(QuantOrderIntent::LatestReinferenceJson)
            .json_binary()
            .null(),
    )
    .col(
        ColumnDef::new(QuantOrderIntent::ScaleOutState)
            .json_binary()
            .not_null()
            .default(Expr::cust(
                "'{\"denominator_shares\": null, \"cumulative_exited_shares\": \"0\", \"settled_target_ids\": [], \"pending_target\": null}'::jsonb",
            )),
    );
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
            .name("fk_quant_order_intent_condition_instance")
            .from(
                QuantOrderIntent::Table,
                QuantOrderIntent::ConditionInstanceId,
            )
            .to(
                QuantEntryConditionInstance::Table,
                QuantEntryConditionInstance::ConditionInstanceId,
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
        TableDependency::foreign_key(quant_entry_condition_instance_table_name),
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

fn quant_entry_condition_instance_table_name() -> String {
    QuantEntryConditionInstance::Table.to_string()
}

fn runtime_config_version_table_name() -> String {
    RuntimeConfigVersion::Table.to_string()
}

fn quant_model_version_table_name() -> String {
    QuantModelVersion::Table.to_string()
}
