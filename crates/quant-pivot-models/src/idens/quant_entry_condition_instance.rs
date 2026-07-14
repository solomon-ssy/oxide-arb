use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, Expr, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table,
        TableCreateStatement,
    },
};

use crate::{
    enums::quant::EntryConditionState,
    idens::{
        quant_entry_condition_artifact::QuantEntryConditionArtifact,
        quant_recommendation::QuantRecommendation,
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
pub enum QuantEntryConditionInstance {
    Table,
    ConditionInstanceId,
    RecommendationId,
    ArtifactId,
    ArtifactHash,
    State,
    TruthJson,
    Revision,
    EvaluationHash,
    InputFingerprint,
    ContinuityHash,
    ConfirmationStartedAt,
    LastEvaluatedAt,
    NextEvaluationAt,
    ExpiresAt,
    LeaseOwner,
    LeaseExpiresAt,
    LeaseEpoch,
    ClaimedByIntentId,
    ClaimAdmissionStateVersion,
    ConsumedAt,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    let mut table = Table::create()
        .table(QuantEntryConditionInstance::Table)
        .if_not_exists()
        .to_owned();
    add_identity_columns(&mut table);
    add_evaluation_columns(&mut table);
    add_lifecycle_columns(&mut table);
    add_foreign_keys(&mut table);
    table
}

fn add_identity_columns(table: &mut TableCreateStatement) {
    table
        .col(column::uuid_pk(
            QuantEntryConditionInstance::ConditionInstanceId,
        ))
        .col(column::uuid_fk(
            QuantEntryConditionInstance::RecommendationId,
        ))
        .col(column::uuid_null(QuantEntryConditionInstance::ArtifactId))
        .col(
            ColumnDef::new(QuantEntryConditionInstance::ArtifactHash)
                .text()
                .null(),
        )
        .col(column::pg_enum::<EntryConditionState>(
            QuantEntryConditionInstance::State,
        ));
}

fn add_evaluation_columns(table: &mut TableCreateStatement) {
    table
        .col(
            ColumnDef::new(QuantEntryConditionInstance::TruthJson)
                .json_binary()
                .null(),
        )
        .col(
            ColumnDef::new(QuantEntryConditionInstance::Revision)
                .big_integer()
                .not_null()
                .default(Expr::value(0_i64)),
        )
        .col(
            ColumnDef::new(QuantEntryConditionInstance::EvaluationHash)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(QuantEntryConditionInstance::InputFingerprint)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(QuantEntryConditionInstance::ContinuityHash)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(QuantEntryConditionInstance::ConfirmationStartedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(QuantEntryConditionInstance::LastEvaluatedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(QuantEntryConditionInstance::NextEvaluationAt)
                .timestamp_with_time_zone()
                .null(),
        );
}

fn add_lifecycle_columns(table: &mut TableCreateStatement) {
    table
        .col(
            ColumnDef::new(QuantEntryConditionInstance::ExpiresAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(column::uuid_null(QuantEntryConditionInstance::LeaseOwner))
        .col(
            ColumnDef::new(QuantEntryConditionInstance::LeaseExpiresAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(QuantEntryConditionInstance::LeaseEpoch)
                .big_integer()
                .not_null()
                .default(Expr::value(0_i64)),
        )
        .col(column::uuid_null(
            QuantEntryConditionInstance::ClaimedByIntentId,
        ))
        .col(
            ColumnDef::new(QuantEntryConditionInstance::ClaimAdmissionStateVersion)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(QuantEntryConditionInstance::ConsumedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(timestamp_with_write_default(
            QuantEntryConditionInstance::CreatedAt,
        ))
        .col(timestamp_with_write_default(
            QuantEntryConditionInstance::UpdatedAt,
        ));
}

fn add_foreign_keys(table: &mut TableCreateStatement) {
    table
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_entry_condition_instance_recommendation")
                .from(
                    QuantEntryConditionInstance::Table,
                    QuantEntryConditionInstance::RecommendationId,
                )
                .to(
                    QuantRecommendation::Table,
                    QuantRecommendation::RecommendationId,
                )
                .on_delete(ForeignKeyAction::Restrict),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_entry_condition_instance_artifact")
                .from(
                    QuantEntryConditionInstance::Table,
                    QuantEntryConditionInstance::ArtifactId,
                )
                .to(
                    QuantEntryConditionArtifact::Table,
                    QuantEntryConditionArtifact::ArtifactId,
                )
                .on_delete(ForeignKeyAction::Restrict),
        );
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "uq_quant_entry_condition_instance_recommendation",
            table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uq_quant_entry_condition_instance_recommendation")
                .table(QuantEntryConditionInstance::Table)
                .col(QuantEntryConditionInstance::RecommendationId)
                .unique()
                .to_owned(),
            "exactly one condition instance per published recommendation",
        ),
        IndexSpec::sea_query(
            "idx_quant_entry_condition_instance_due",
            table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_entry_condition_instance_due")
                .table(QuantEntryConditionInstance::Table)
                .col(QuantEntryConditionInstance::State)
                .col((
                    QuantEntryConditionInstance::NextEvaluationAt,
                    IndexOrder::Asc,
                ))
                .col((QuantEntryConditionInstance::ExpiresAt, IndexOrder::Asc))
                .to_owned(),
            "condition worker due queue",
        ),
        IndexSpec::sea_query(
            "idx_quant_entry_condition_instance_lease",
            table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_entry_condition_instance_lease")
                .table(QuantEntryConditionInstance::Table)
                .col(QuantEntryConditionInstance::LeaseExpiresAt)
                .to_owned(),
            "expired lease takeover scan",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(recommendation_table_name),
        TableDependency::foreign_key(artifact_table_name),
    ]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn table_name() -> String {
    QuantEntryConditionInstance::Table.to_string()
}

fn recommendation_table_name() -> String {
    QuantRecommendation::Table.to_string()
}

fn artifact_table_name() -> String {
    QuantEntryConditionArtifact::Table.to_string()
}
