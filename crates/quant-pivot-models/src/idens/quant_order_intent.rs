use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table, TableCreateStatement,
    },
};

use crate::{
    enums::{
        execution::OrderIntentKind,
        quant::{ApprovalStatus, OrderIntentStatus, QuantRuntimeMode},
    },
    idens::quant_recommendation::QuantRecommendation,
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
    IntentKind,
    Status,
    ApprovalStatus,
    ApprovedBy,
    ApprovalReason,
    ApprovedAt,
    EntryOrderJson,
    ExitPolicyJson,
    RiskEnvelopeHash,
    ExpiresAt,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantOrderIntent::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantOrderIntent::OrderIntentId))
        .col(column::uuid_fk(QuantOrderIntent::RecommendationId))
        .col(column::pg_enum::<QuantRuntimeMode>(
            QuantOrderIntent::RuntimeMode,
        ))
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
        .col(timestamp_with_write_default(QuantOrderIntent::CreatedAt))
        .col(timestamp_with_write_default(QuantOrderIntent::UpdatedAt))
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_order_intent_recommendation")
                .from(QuantOrderIntent::Table, QuantOrderIntent::RecommendationId)
                .to(
                    QuantRecommendation::Table,
                    QuantRecommendation::RecommendationId,
                )
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
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
    vec![TableDependency::foreign_key(
        quant_recommendation_table_name,
    )]
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
