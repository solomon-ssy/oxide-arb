use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, Index, IndexOrder, Table, TableCreateStatement},
};

use crate::schema::{
    dependency::TableDependency,
    index::{IndexBuildMode, IndexSpec},
    seed::SeedSpec,
    timestamp_with_write_default,
};

#[oxide_schema(lifecycle = "audit")]
pub enum RiskAuditEvent {
    Table,
    Id,
    EventType,
    OpportunityId,
    TradeId,
    Payload,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(RiskAuditEvent::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(RiskAuditEvent::Id)
                .big_integer()
                .not_null()
                .auto_increment()
                .primary_key(),
        )
        .col(ColumnDef::new(RiskAuditEvent::EventType).text().not_null())
        .col(ColumnDef::new(RiskAuditEvent::OpportunityId).text().null())
        .col(ColumnDef::new(RiskAuditEvent::TradeId).text().null())
        .col(
            ColumnDef::new(RiskAuditEvent::Payload)
                .json_binary()
                .not_null(),
        )
        .col(timestamp_with_write_default(RiskAuditEvent::CreatedAt))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_risk_audit_event_created_at",
        risk_audit_event_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_risk_audit_event_created_at")
            .table(RiskAuditEvent::Table)
            .col((RiskAuditEvent::CreatedAt, IndexOrder::Desc))
            .to_owned(),
        "risk audit events by recency",
    )]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn risk_audit_event_table_name() -> String {
    RiskAuditEvent::Table.to_string()
}
