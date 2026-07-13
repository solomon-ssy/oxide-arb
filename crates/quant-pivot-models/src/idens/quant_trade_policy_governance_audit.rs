use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, Index, IndexOrder, Table, TableCreateStatement},
};

use crate::{
    enums::quant::{TradePolicyGovernanceAction, TradePolicyStatus},
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[quant_schema(lifecycle = "audit")]
pub enum QuantTradePolicyGovernanceAudit {
    Table,
    AuditId,
    ArtifactId,
    Action,
    FromStatus,
    ToStatus,
    ContentHash,
    ActorId,
    Reason,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantTradePolicyGovernanceAudit::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantTradePolicyGovernanceAudit::AuditId))
        .col(column::uuid_fk(QuantTradePolicyGovernanceAudit::ArtifactId))
        .col(column::pg_enum::<TradePolicyGovernanceAction>(
            QuantTradePolicyGovernanceAudit::Action,
        ))
        .col(column::pg_enum::<TradePolicyStatus>(
            QuantTradePolicyGovernanceAudit::FromStatus,
        ))
        .col(column::pg_enum::<TradePolicyStatus>(
            QuantTradePolicyGovernanceAudit::ToStatus,
        ))
        .col(
            ColumnDef::new(QuantTradePolicyGovernanceAudit::ContentHash)
                .text()
                .not_null(),
        )
        .col(column::uuid_fk(QuantTradePolicyGovernanceAudit::ActorId))
        .col(
            ColumnDef::new(QuantTradePolicyGovernanceAudit::Reason)
                .text()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            QuantTradePolicyGovernanceAudit::CreatedAt,
        ))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_quant_trade_policy_audit_artifact_created",
        table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_quant_trade_policy_audit_artifact_created")
            .table(QuantTradePolicyGovernanceAudit::Table)
            .col(QuantTradePolicyGovernanceAudit::ArtifactId)
            .col((QuantTradePolicyGovernanceAudit::CreatedAt, IndexOrder::Desc))
            .to_owned(),
        "append-only governance history per trade-policy artifact",
    )]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn table_name() -> String {
    QuantTradePolicyGovernanceAudit::Table.to_string()
}
