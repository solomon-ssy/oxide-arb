//! `operation_log` table — append-only general operation/activity log.
//!
//! Distinct from the governance hash chain: this captures every mutating/auth
//! HTTP operation for forensics. It is WORM (auto append-only trigger via the
//! `audit` lifecycle) and intentionally carries **no** foreign keys so that
//! deleting a user never rewrites or removes its historical activity. The
//! actor's username and acting role are denormalized for the same reason.

use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, Expr, Index, IndexOrder, Table, TableCreateStatement},
};

use crate::schema::{
    column,
    dependency::TableDependency,
    index::{IndexBuildMode, IndexSpec},
    seed::SeedSpec,
    timestamp_with_write_default,
};

/// Append-only operation log row.
#[oxide_schema(lifecycle = "audit")]
pub enum OperationLog {
    Table,
    Id,
    OccurredAt,
    RequestId,
    ActorUserId,
    ActorUsername,
    ActingRole,
    Category,
    Action,
    ResourceType,
    ResourceId,
    HttpMethod,
    HttpPath,
    HttpStatus,
    Outcome,
    ClientIp,
    UserAgent,
    LatencyMs,
    Detail,
    GovernanceAuditEventId,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(OperationLog::Table)
        .if_not_exists()
        .col(column::uuid_pk(OperationLog::Id))
        .col(timestamp_with_write_default(OperationLog::OccurredAt))
        .col(ColumnDef::new(OperationLog::RequestId).text().not_null())
        .col(column::uuid_null(OperationLog::ActorUserId))
        .col(ColumnDef::new(OperationLog::ActorUsername).text().null())
        .col(ColumnDef::new(OperationLog::ActingRole).text().null())
        .col(ColumnDef::new(OperationLog::Category).text().not_null())
        .col(ColumnDef::new(OperationLog::Action).text().not_null())
        .col(ColumnDef::new(OperationLog::ResourceType).text().null())
        .col(ColumnDef::new(OperationLog::ResourceId).text().null())
        .col(ColumnDef::new(OperationLog::HttpMethod).text().not_null())
        .col(ColumnDef::new(OperationLog::HttpPath).text().not_null())
        .col(
            ColumnDef::new(OperationLog::HttpStatus)
                .small_integer()
                .not_null(),
        )
        .col(ColumnDef::new(OperationLog::Outcome).text().not_null())
        .col(ColumnDef::new(OperationLog::ClientIp).text().null())
        .col(ColumnDef::new(OperationLog::UserAgent).text().null())
        .col(ColumnDef::new(OperationLog::LatencyMs).integer().not_null())
        .col(
            ColumnDef::new(OperationLog::Detail)
                .json_binary()
                .not_null()
                .default(Expr::cust("'{}'::jsonb")),
        )
        .col(column::uuid_null(OperationLog::GovernanceAuditEventId))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_oplog_occurred",
            operation_log_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_oplog_occurred")
                .table(OperationLog::Table)
                .col((OperationLog::OccurredAt, IndexOrder::Desc))
                .to_owned(),
            "operation log by recency",
        ),
        IndexSpec::sea_query(
            "idx_oplog_actor",
            operation_log_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_oplog_actor")
                .table(OperationLog::Table)
                .col(OperationLog::ActorUserId)
                .col((OperationLog::OccurredAt, IndexOrder::Desc))
                .to_owned(),
            "operation log by actor in recency order",
        ),
        IndexSpec::sea_query(
            "idx_oplog_category",
            operation_log_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_oplog_category")
                .table(OperationLog::Table)
                .col(OperationLog::Category)
                .col((OperationLog::OccurredAt, IndexOrder::Desc))
                .to_owned(),
            "operation log by category in recency order",
        ),
        IndexSpec::sea_query(
            "idx_oplog_resource",
            operation_log_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_oplog_resource")
                .table(OperationLog::Table)
                .col(OperationLog::ResourceType)
                .col(OperationLog::ResourceId)
                .to_owned(),
            "operation log by affected resource",
        ),
        IndexSpec::sea_query(
            "idx_oplog_request",
            operation_log_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_oplog_request")
                .table(OperationLog::Table)
                .col(OperationLog::RequestId)
                .to_owned(),
            "correlate operation log with X-Request-Id",
        ),
    ]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

pub fn operation_log_table_name() -> String {
    OperationLog::Table.to_string()
}
