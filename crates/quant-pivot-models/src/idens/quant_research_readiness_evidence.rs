use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, Expr, Index, IndexOrder, Table, TableCreateStatement},
};

use crate::{
    enums::quant::ResearchReadinessEvidenceKind,
    schema::{
        column::pg_enum,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[quant_schema(lifecycle = "audit")]
pub enum QuantResearchReadinessEvidence {
    Table,
    EvidenceId,
    Kind,
    ScopeHash,
    WindowStart,
    WindowEnd,
    ObservedAt,
    ExpiresAt,
    PayloadJson,
    PayloadHash,
    ArtifactUri,
    ArtifactVersion,
    AttestationKeyId,
    AttestationMac,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantResearchReadinessEvidence::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(QuantResearchReadinessEvidence::EvidenceId)
                .uuid()
                .not_null()
                .primary_key(),
        )
        .col(pg_enum::<ResearchReadinessEvidenceKind>(
            QuantResearchReadinessEvidence::Kind,
        ))
        .col(
            ColumnDef::new(QuantResearchReadinessEvidence::ScopeHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantResearchReadinessEvidence::WindowStart)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantResearchReadinessEvidence::WindowEnd)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantResearchReadinessEvidence::ObservedAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantResearchReadinessEvidence::ExpiresAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantResearchReadinessEvidence::PayloadJson)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantResearchReadinessEvidence::PayloadHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantResearchReadinessEvidence::ArtifactUri)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantResearchReadinessEvidence::ArtifactVersion)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantResearchReadinessEvidence::AttestationKeyId)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantResearchReadinessEvidence::AttestationMac)
                .text()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            QuantResearchReadinessEvidence::CreatedAt,
        ))
        .check(Expr::cust(
            "window_start < window_end AND window_end <= observed_at
             AND observed_at < expires_at
             AND artifact_version <> '' AND attestation_key_id <> ''",
        ))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "uq_quant_research_readiness_evidence_payload",
            table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uq_quant_research_readiness_evidence_payload")
                .table(QuantResearchReadinessEvidence::Table)
                .col(QuantResearchReadinessEvidence::Kind)
                .col(QuantResearchReadinessEvidence::ScopeHash)
                .col(QuantResearchReadinessEvidence::PayloadHash)
                .unique()
                .to_owned(),
            "deduplicate identical signed readiness observations",
        ),
        IndexSpec::sea_query(
            "idx_quant_research_readiness_evidence_latest",
            table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_research_readiness_evidence_latest")
                .table(QuantResearchReadinessEvidence::Table)
                .col(QuantResearchReadinessEvidence::Kind)
                .col((QuantResearchReadinessEvidence::ObservedAt, IndexOrder::Desc))
                .to_owned(),
            "latest valid evidence lookup by kind",
        ),
    ]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn table_name() -> String {
    QuantResearchReadinessEvidence::Table.to_string()
}
