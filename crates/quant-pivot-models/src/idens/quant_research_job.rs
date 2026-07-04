use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table, TableCreateStatement,
    },
};

use crate::{
    enums::quant::{ResearchJobKind, ResearchJobStatus},
    idens::{quant_model_spec::QuantModelSpec, runtime_config_version::RuntimeConfigVersion},
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

// Durable async research-job ledger. Each row is one long-running task (dataset
// build / model train / backtest) that a `ResearchJobWorker` leases and executes
// off the HTTP hot path. `runtime` lifecycle: identity is immutable but status,
// progress, lease, and heartbeat mutate through a bounded lifecycle
// (`queued → running → {succeeded | failed | cancelled}`), with crash recovery
// reclaiming orphaned `running` rows back to `queued` (bounded by
// `recovery_attempt`).
#[quant_schema(lifecycle = "runtime")]
pub enum QuantResearchJob {
    Table,
    JobId,
    Kind,
    Status,
    ModelSpecId,
    RuntimeConfigVersionId,
    ParamsJson,
    ProgressJson,
    ResultRef,
    ErrorJson,
    CoverageJson,
    RequestedBy,
    ActingRole,
    ParentJobId,
    RecoveryAttempt,
    MaxRecoveryAttempts,
    LeaseOwner,
    LeaseExpiresAt,
    StartedAt,
    FinishedAt,
    HeartbeatAt,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantResearchJob::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantResearchJob::JobId))
        .col(column::pg_enum::<ResearchJobKind>(QuantResearchJob::Kind))
        .col(column::pg_enum_default::<ResearchJobStatus>(
            QuantResearchJob::Status,
            &ResearchJobStatus::Queued,
        ))
        .col(column::uuid_null(QuantResearchJob::ModelSpecId))
        .col(column::uuid_null(QuantResearchJob::RuntimeConfigVersionId))
        .col(
            ColumnDef::new(QuantResearchJob::ParamsJson)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantResearchJob::ProgressJson)
                .json_binary()
                .null(),
        )
        .col(column::uuid_null(QuantResearchJob::ResultRef))
        .col(
            ColumnDef::new(QuantResearchJob::ErrorJson)
                .json_binary()
                .null(),
        )
        .col(
            ColumnDef::new(QuantResearchJob::CoverageJson)
                .json_binary()
                .null(),
        )
        .col(ColumnDef::new(QuantResearchJob::RequestedBy).text().null())
        .col(
            ColumnDef::new(QuantResearchJob::ActingRole)
                .text()
                .not_null(),
        )
        .col(column::uuid_null(QuantResearchJob::ParentJobId))
        .col(
            ColumnDef::new(QuantResearchJob::RecoveryAttempt)
                .integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(QuantResearchJob::MaxRecoveryAttempts)
                .integer()
                .not_null(),
        )
        .col(ColumnDef::new(QuantResearchJob::LeaseOwner).text().null())
        .col(
            ColumnDef::new(QuantResearchJob::LeaseExpiresAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(QuantResearchJob::StartedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(QuantResearchJob::FinishedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(QuantResearchJob::HeartbeatAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(timestamp_with_write_default(QuantResearchJob::CreatedAt))
        .col(timestamp_with_write_default(QuantResearchJob::UpdatedAt))
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_research_job_model_spec")
                .from(QuantResearchJob::Table, QuantResearchJob::ModelSpecId)
                .to(QuantModelSpec::Table, QuantModelSpec::ModelSpecId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_research_job_runtime_config")
                .from(
                    QuantResearchJob::Table,
                    QuantResearchJob::RuntimeConfigVersionId,
                )
                .to(
                    RuntimeConfigVersion::Table,
                    RuntimeConfigVersion::RuntimeConfigVersionId,
                )
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_quant_research_job_status_created",
            quant_research_job_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_research_job_status_created")
                .table(QuantResearchJob::Table)
                .col(QuantResearchJob::Status)
                .col((QuantResearchJob::CreatedAt, IndexOrder::Desc))
                .to_owned(),
            "research jobs by status and recency",
        ),
        IndexSpec::sea_query(
            "idx_quant_research_job_kind_status",
            quant_research_job_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_research_job_kind_status")
                .table(QuantResearchJob::Table)
                .col(QuantResearchJob::Kind)
                .col(QuantResearchJob::Status)
                .to_owned(),
            "research jobs by kind and status (queue lease + concurrency caps)",
        ),
        IndexSpec::sea_query(
            "idx_quant_research_job_lease",
            quant_research_job_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_research_job_lease")
                .table(QuantResearchJob::Table)
                .col(QuantResearchJob::Status)
                .col(QuantResearchJob::LeaseExpiresAt)
                .to_owned(),
            "orphaned running jobs (boot recovery sweep)",
        ),
        IndexSpec::sea_query(
            "idx_quant_research_job_parent",
            quant_research_job_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_research_job_parent")
                .table(QuantResearchJob::Table)
                .col(QuantResearchJob::ParentJobId)
                .to_owned(),
            "retry lineage by parent job",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(quant_model_spec_table_name),
        TableDependency::foreign_key(runtime_config_version_table_name),
    ]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_research_job_table_name() -> String {
    QuantResearchJob::Table.to_string()
}

fn quant_model_spec_table_name() -> String {
    QuantModelSpec::Table.to_string()
}

fn runtime_config_version_table_name() -> String {
    RuntimeConfigVersion::Table.to_string()
}
