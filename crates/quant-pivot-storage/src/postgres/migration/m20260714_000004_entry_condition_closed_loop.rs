//! Phase 11.7.1 ordered-fold, WORM, and outbox lease migration.

use sea_orm_migration::prelude::*;

use crate::postgres::migration::execute_sql;

#[derive(DeriveMigrationName)]
pub struct Migration;

const CALIBRATION_PUBLICATION_SQL: [&str; 5] = [
    "ALTER TYPE qp_calibration_kind \
     ADD VALUE IF NOT EXISTS 'weather_station_lead_bias'",
    "CREATE TABLE IF NOT EXISTS quant_calibration_artifact_publication (\
        publication_id UUID PRIMARY KEY, \
        artifact_id UUID NOT NULL \
            REFERENCES quant_calibration_artifact(artifact_id) \
            ON DELETE RESTRICT ON UPDATE RESTRICT, \
        kind qp_calibration_kind NOT NULL, \
        published_at TIMESTAMPTZ NOT NULL, \
        created_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp()\
     )",
    "CREATE INDEX IF NOT EXISTS idx_quant_calibration_publication_pit \
     ON quant_calibration_artifact_publication \
     (kind, published_at, publication_id)",
    "DROP TRIGGER IF EXISTS \
     trg_quant_calibration_artifact_publication_append_only \
     ON quant_calibration_artifact_publication",
    "CREATE TRIGGER trg_quant_calibration_artifact_publication_append_only \
     BEFORE UPDATE OR DELETE ON quant_calibration_artifact_publication \
     FOR EACH ROW EXECUTE FUNCTION trigger_deny_write()",
];

const ARCHIVE_LIFECYCLE_SQL: [&str; 10] = [
    "CREATE TABLE IF NOT EXISTS quant_archive_partition_manifest (\
        manifest_id UUID PRIMARY KEY, \
        table_name TEXT NOT NULL, \
        partition_key TEXT NOT NULL, \
        retention_days INTEGER NOT NULL, \
        row_count BIGINT NOT NULL, \
        parquet_uri TEXT NOT NULL, \
        byte_hash TEXT NOT NULL, \
        content_hash TEXT NOT NULL, \
        manifest_hash TEXT NOT NULL, \
        sealed_at TIMESTAMPTZ NOT NULL, \
        created_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(), \
        CONSTRAINT uq_quant_archive_partition_manifest_partition \
            UNIQUE (table_name, partition_key), \
        CONSTRAINT uq_quant_archive_partition_manifest_hash \
            UNIQUE (manifest_hash)\
     )",
    "CREATE TABLE IF NOT EXISTS quant_archive_partition_drop_audit (\
        audit_id UUID PRIMARY KEY, \
        manifest_id UUID NOT NULL UNIQUE \
            REFERENCES quant_archive_partition_manifest(manifest_id) \
            ON DELETE RESTRICT ON UPDATE RESTRICT, \
        dropped_at TIMESTAMPTZ NOT NULL, \
        created_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp()\
     )",
    "CREATE TABLE IF NOT EXISTS quant_archive_partition_drop_command (\
        manifest_id UUID PRIMARY KEY \
            REFERENCES quant_archive_partition_manifest(manifest_id) \
            ON DELETE RESTRICT ON UPDATE RESTRICT, \
        claim_owner UUID NULL, \
        lease_expires_at TIMESTAMPTZ NULL, \
        attempts INTEGER NOT NULL DEFAULT 0, \
        last_error TEXT NULL, \
        completed_at TIMESTAMPTZ NULL, \
        created_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(), \
        updated_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp()\
     )",
    "CREATE INDEX IF NOT EXISTS idx_quant_archive_partition_drop_command_claim \
     ON quant_archive_partition_drop_command \
     (completed_at, lease_expires_at, created_at)",
    "DROP TRIGGER IF EXISTS trg_quant_archive_partition_drop_command_updated_at \
     ON quant_archive_partition_drop_command",
    "CREATE TRIGGER trg_quant_archive_partition_drop_command_updated_at \
     BEFORE UPDATE ON quant_archive_partition_drop_command \
     FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at()",
    "DROP TRIGGER IF EXISTS trg_quant_archive_partition_manifest_append_only \
     ON quant_archive_partition_manifest",
    "CREATE TRIGGER trg_quant_archive_partition_manifest_append_only \
     BEFORE UPDATE OR DELETE ON quant_archive_partition_manifest \
     FOR EACH ROW EXECUTE FUNCTION trigger_deny_write()",
    "DROP TRIGGER IF EXISTS trg_quant_archive_partition_drop_audit_append_only \
     ON quant_archive_partition_drop_audit",
    "CREATE TRIGGER trg_quant_archive_partition_drop_audit_append_only \
     BEFORE UPDATE OR DELETE ON quant_archive_partition_drop_audit \
     FOR EACH ROW EXECUTE FUNCTION trigger_deny_write()",
];

const ENTRY_EVALUATION_SQL: [&str; 12] = [
    "CREATE TABLE IF NOT EXISTS quant_entry_condition_evaluation_outbox (\
        outbox_id UUID PRIMARY KEY, \
        evaluation_id TEXT NOT NULL UNIQUE, \
        event_json JSONB NOT NULL, \
        published_at TIMESTAMPTZ NULL, \
        publish_attempts INTEGER NOT NULL DEFAULT 0, \
        claim_owner UUID NULL, \
        lease_expires_at TIMESTAMPTZ NULL, \
        last_error TEXT NULL, \
        created_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(), \
        updated_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp()\
     )",
    "CREATE INDEX IF NOT EXISTS \
     idx_quant_entry_condition_evaluation_outbox_pending \
     ON quant_entry_condition_evaluation_outbox \
     (published_at, lease_expires_at, created_at)",
    "DROP TRIGGER IF EXISTS \
     trg_quant_entry_condition_evaluation_outbox_updated_at \
     ON quant_entry_condition_evaluation_outbox",
    "CREATE TRIGGER trg_quant_entry_condition_evaluation_outbox_updated_at \
     BEFORE UPDATE ON quant_entry_condition_evaluation_outbox \
     FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at()",
    "ALTER TABLE quant_entry_condition_instance \
     ADD COLUMN IF NOT EXISTS fold_state_json JSONB NOT NULL \
     DEFAULT '{\"crypto\":[]}'::jsonb",
    "ALTER TABLE quant_domain_event_outbox \
     ADD COLUMN IF NOT EXISTS claim_owner UUID NULL",
    "ALTER TABLE quant_domain_event_outbox \
     ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ NULL",
    "DROP TRIGGER IF EXISTS \
     trg_quant_entry_condition_artifact_append_only \
     ON quant_entry_condition_artifact",
    "CREATE TRIGGER trg_quant_entry_condition_artifact_append_only \
     BEFORE UPDATE OR DELETE ON quant_entry_condition_artifact \
     FOR EACH ROW EXECUTE FUNCTION trigger_deny_write()",
    "DROP TRIGGER IF EXISTS \
     trg_quant_entry_condition_audit_append_only \
     ON quant_entry_condition_audit",
    "CREATE TRIGGER trg_quant_entry_condition_audit_append_only \
     BEFORE UPDATE OR DELETE ON quant_entry_condition_audit \
     FOR EACH ROW EXECUTE FUNCTION trigger_deny_write()",
    "CREATE INDEX IF NOT EXISTS idx_quant_domain_event_outbox_claim \
     ON quant_domain_event_outbox \
     (published_at, lease_expires_at, created_at)",
];

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        execute_sql(manager, CALIBRATION_PUBLICATION_SQL).await?;
        execute_sql(manager, ARCHIVE_LIFECYCLE_SQL).await?;
        execute_sql(manager, ENTRY_EVALUATION_SQL).await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        execute_sql(
            manager,
            [
                "DROP TABLE IF EXISTS quant_calibration_artifact_publication",
                "DROP TABLE IF EXISTS quant_archive_partition_drop_audit",
                "DROP TABLE IF EXISTS quant_archive_partition_drop_command",
                "DROP TABLE IF EXISTS quant_archive_partition_manifest",
                "DROP TABLE IF EXISTS quant_entry_condition_evaluation_outbox",
                "DROP INDEX IF EXISTS idx_quant_domain_event_outbox_claim",
                "DROP TRIGGER IF EXISTS \
                 trg_quant_entry_condition_audit_append_only \
                 ON quant_entry_condition_audit",
                "DROP TRIGGER IF EXISTS \
                 trg_quant_entry_condition_artifact_append_only \
                 ON quant_entry_condition_artifact",
                "ALTER TABLE quant_domain_event_outbox \
                 DROP COLUMN IF EXISTS lease_expires_at",
                "ALTER TABLE quant_domain_event_outbox \
                 DROP COLUMN IF EXISTS claim_owner",
                "ALTER TABLE quant_entry_condition_instance \
                 DROP COLUMN IF EXISTS fold_state_json",
            ],
        )
        .await
    }
}
