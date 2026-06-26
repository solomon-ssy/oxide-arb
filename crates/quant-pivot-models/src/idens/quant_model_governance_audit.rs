use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table, TableCreateStatement,
    },
};

use crate::{
    enums::quant::{ModelGovernanceAction, PublicationStatus},
    idens::{quant_model_version::QuantModelVersion, quant_training_dataset::QuantTrainingDataset},
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

// Append-only governance audit trail for the model-version lifecycle: one row per
// publish / retire / rollback action and per gated dataset promotion. Records the
// actor, reason, before/after status + artifact hash, whether the quality gate
// passed, the rollback target, and the shadow window — every fact needed to
// reconstruct a money-affecting governance decision. WORM `audit` lifecycle.
//
// `actor_role` is recorded for provenance only this phase; hard role enforcement
// is deferred to the Phase 07 web route wiring.
#[quant_schema(lifecycle = "audit")]
pub enum QuantModelGovernanceAudit {
    Table,
    AuditId,
    ModelVersionId,
    TrainingDatasetId,
    Action,
    ActorUsername,
    ActorRole,
    Reason,
    BeforeStatus,
    AfterStatus,
    BeforeHash,
    AfterHash,
    QualityGatePassed,
    RollbackTargetVersionId,
    ShadowWindowSecs,
    DetailJson,
    AuditEventId,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantModelGovernanceAudit::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantModelGovernanceAudit::AuditId))
        .col(column::uuid_null(QuantModelGovernanceAudit::ModelVersionId))
        .col(column::uuid_null(
            QuantModelGovernanceAudit::TrainingDatasetId,
        ))
        .col(column::pg_enum::<ModelGovernanceAction>(
            QuantModelGovernanceAudit::Action,
        ))
        .col(
            ColumnDef::new(QuantModelGovernanceAudit::ActorUsername)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantModelGovernanceAudit::ActorRole)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(QuantModelGovernanceAudit::Reason)
                .text()
                .not_null(),
        )
        .col(column::pg_enum::<PublicationStatus>(
            QuantModelGovernanceAudit::BeforeStatus,
        ))
        .col(column::pg_enum::<PublicationStatus>(
            QuantModelGovernanceAudit::AfterStatus,
        ))
        .col(
            ColumnDef::new(QuantModelGovernanceAudit::BeforeHash)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(QuantModelGovernanceAudit::AfterHash)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(QuantModelGovernanceAudit::QualityGatePassed)
                .boolean()
                .not_null(),
        )
        .col(column::uuid_null(
            QuantModelGovernanceAudit::RollbackTargetVersionId,
        ))
        .col(
            ColumnDef::new(QuantModelGovernanceAudit::ShadowWindowSecs)
                .big_integer()
                .null(),
        )
        .col(
            ColumnDef::new(QuantModelGovernanceAudit::DetailJson)
                .json_binary()
                .not_null(),
        )
        .col(column::uuid_null(QuantModelGovernanceAudit::AuditEventId))
        .col(timestamp_with_write_default(
            QuantModelGovernanceAudit::CreatedAt,
        ))
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_model_governance_audit_model_version")
                .from(
                    QuantModelGovernanceAudit::Table,
                    QuantModelGovernanceAudit::ModelVersionId,
                )
                .to(QuantModelVersion::Table, QuantModelVersion::ModelVersionId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_model_governance_audit_training_dataset")
                .from(
                    QuantModelGovernanceAudit::Table,
                    QuantModelGovernanceAudit::TrainingDatasetId,
                )
                .to(
                    QuantTrainingDataset::Table,
                    QuantTrainingDataset::TrainingDatasetId,
                )
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_quant_model_governance_audit_version_created",
        quant_model_governance_audit_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_quant_model_governance_audit_version_created")
            .table(QuantModelGovernanceAudit::Table)
            .col(QuantModelGovernanceAudit::ModelVersionId)
            .col((QuantModelGovernanceAudit::CreatedAt, IndexOrder::Desc))
            .to_owned(),
        "governance audit rows by model version and recency",
    )]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(quant_model_version_table_name),
        TableDependency::foreign_key(quant_training_dataset_table_name),
    ]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_model_governance_audit_table_name() -> String {
    QuantModelGovernanceAudit::Table.to_string()
}

fn quant_model_version_table_name() -> String {
    QuantModelVersion::Table.to_string()
}

fn quant_training_dataset_table_name() -> String {
    QuantTrainingDataset::Table.to_string()
}
