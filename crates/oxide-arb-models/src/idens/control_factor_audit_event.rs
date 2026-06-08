use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, Index, IndexOrder, Table, TableCreateStatement},
};

use crate::schema::{
    column,
    dependency::TableDependency,
    index::{IndexBuildMode, IndexSpec},
    seed::SeedSpec,
    timestamp_with_write_default,
};

/// Append-only, globally-sequenced, tamper-evident governance audit log.
///
/// The chain is verified by recomputing `event_hash` from the canonical event
/// content and `prev_event_hash`, and by asserting `sequence` is contiguous.
/// There is intentionally **no** foreign key to factor/publication: a `SetNull`
/// cascade would mutate an audit row and break append-only + hash-chain
/// invariants, so resources are referenced generically via
/// `resource_type` + `resource_id`.
#[oxide_schema(lifecycle = "audit")]
pub enum ControlFactorAuditEvent {
    Table,
    EventId,
    Sequence,
    EventType,
    Actor,
    ActorRole,
    ResourceType,
    ResourceId,
    RequestId,
    Reason,
    BeforeHash,
    AfterHash,
    Diff,
    PrevEventHash,
    EventHash,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(ControlFactorAuditEvent::Table)
        .if_not_exists()
        .col(column::uuid_pk(ControlFactorAuditEvent::EventId))
        .col(
            ColumnDef::new(ControlFactorAuditEvent::Sequence)
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorAuditEvent::EventType)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorAuditEvent::Actor)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorAuditEvent::ActorRole)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorAuditEvent::ResourceType)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorAuditEvent::ResourceId)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorAuditEvent::RequestId)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorAuditEvent::Reason)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorAuditEvent::BeforeHash)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(ControlFactorAuditEvent::AfterHash)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(ControlFactorAuditEvent::Diff)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorAuditEvent::PrevEventHash)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(ControlFactorAuditEvent::EventHash)
                .text()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            ControlFactorAuditEvent::CreatedAt,
        ))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "uniq_control_factor_audit_event_sequence",
            audit_event_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uniq_control_factor_audit_event_sequence")
                .table(ControlFactorAuditEvent::Table)
                .col(ControlFactorAuditEvent::Sequence)
                .unique()
                .to_owned(),
            "global monotonic audit chain sequence",
        ),
        IndexSpec::sea_query(
            "uniq_control_factor_audit_event_hash",
            audit_event_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uniq_control_factor_audit_event_hash")
                .table(ControlFactorAuditEvent::Table)
                .col(ControlFactorAuditEvent::EventHash)
                .unique()
                .to_owned(),
            "audit event hash uniqueness",
        ),
        IndexSpec::sea_query(
            "uniq_control_factor_audit_event_idempotency",
            audit_event_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uniq_control_factor_audit_event_idempotency")
                .table(ControlFactorAuditEvent::Table)
                .col(ControlFactorAuditEvent::RequestId)
                .col(ControlFactorAuditEvent::EventType)
                .col(ControlFactorAuditEvent::ResourceId)
                .unique()
                .to_owned(),
            "audit event idempotency key (request_id, event_type, resource_id)",
        ),
        IndexSpec::sea_query(
            "idx_control_factor_audit_event_resource",
            audit_event_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_control_factor_audit_event_resource")
                .table(ControlFactorAuditEvent::Table)
                .col(ControlFactorAuditEvent::ResourceType)
                .col(ControlFactorAuditEvent::ResourceId)
                .col(ControlFactorAuditEvent::Sequence)
                .to_owned(),
            "audit events by resource in chain order",
        ),
        IndexSpec::sea_query(
            "idx_control_factor_audit_event_created_at",
            audit_event_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_control_factor_audit_event_created_at")
                .table(ControlFactorAuditEvent::Table)
                .col((ControlFactorAuditEvent::CreatedAt, IndexOrder::Desc))
                .to_owned(),
            "control-factor audit events by recency",
        ),
    ]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn audit_event_table_name() -> String {
    ControlFactorAuditEvent::Table.to_string()
}

#[cfg(test)]
mod tests {
    use super::table;
    use sea_orm::sea_query::PostgresQueryBuilder;

    #[test]
    fn audit_table_is_append_only_hash_chain() {
        let sql = table().to_string(PostgresQueryBuilder);
        for column in [
            "sequence",
            "prev_event_hash",
            "event_hash",
            "resource_type",
            "resource_id",
            "request_id",
        ] {
            assert!(sql.contains(column), "audit table must define `{column}`");
        }
        // A foreign key with ON DELETE SET NULL would mutate audit rows, breaking
        // both append-only and the hash chain. The audit log must reference
        // resources generically instead.
        assert!(
            !sql.to_lowercase().contains("foreign key"),
            "audit table must not declare foreign keys"
        );
    }
}
