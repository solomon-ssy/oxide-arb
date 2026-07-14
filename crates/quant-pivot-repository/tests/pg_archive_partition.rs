//! Seal-first archive ledger integration tests (`PostgreSQL` + testcontainers).

use chrono::{Duration, Utc};
use quant_pivot_models::{
    domain::NewArchivePartitionManifest,
    hashing::CanonicalDigest,
    types::{ArtifactUri, ContentHash},
};
use quant_pivot_repository::{
    postgres::PgArchivePartitionRepository, traits::ArchivePartitionRepository,
};
use quant_pivot_storage::postgres::migration::{Migrator, MigratorTrait};
use quant_pivot_test_support::pg::setup_pg;
use sea_orm::{ConnectionTrait, DbBackend, Statement};
use uuid::Uuid;

fn hash(seed: char) -> ContentHash {
    ContentHash::parse(format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn seal_is_required_before_drop_claim_and_worm_rows_reject_mutation() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let repo = PgArchivePartitionRepository::new(db.clone());
    let worker = Uuid::now_v7();
    let now = Utc::now();

    assert!(
        repo.claim_pending_drop(worker, now, now + Duration::minutes(5))
            .await
            .expect("empty claim")
            .is_none(),
        "an unsealed partition must not produce a destructive command"
    );

    let table_name = "quant_crypto_price_report".to_owned();
    let partition_key = "202401".to_owned();
    let retention_days = 90;
    let row_count = 42;
    let partition_start_at = now - Duration::days(180);
    let partition_end_at = now - Duration::days(150);
    let parquet_uri =
        ArtifactUri::parse("file:///tmp/quant_crypto_price_report-202401.parquet").expect("uri");
    let byte_hash = hash('1');
    let content_hash = hash('2');
    let source_schema_hash = hash('3');
    let parquet_byte_count = 1_024;
    let object_etag: Option<String> = None;
    let object_version_id: Option<String> = None;
    let sealed_at = now;
    let manifest_hash = CanonicalDigest::content_hash_json(&(
        &table_name,
        &partition_key,
        partition_start_at,
        partition_end_at,
        retention_days,
        row_count,
        &parquet_uri,
        parquet_byte_count,
        &object_etag,
        &object_version_id,
        &byte_hash,
        &content_hash,
        &source_schema_hash,
        sealed_at,
    ))
    .expect("manifest hash");
    let sealed = repo
        .seal_manifest(NewArchivePartitionManifest {
            manifest_id: Uuid::now_v7(),
            table_name,
            partition_key,
            partition_start_at,
            partition_end_at,
            retention_days,
            row_count,
            parquet_uri,
            parquet_byte_count,
            object_etag,
            object_version_id,
            byte_hash,
            content_hash,
            source_schema_hash,
            manifest_hash,
            sealed_at,
        })
        .await
        .expect("seal manifest");
    assert_eq!(
        repo.find_manifests_in_range(
            "quant_crypto_price_report",
            partition_start_at,
            partition_end_at,
        )
        .await
        .expect("range query")
        .len(),
        1
    );

    let second_repo = PgArchivePartitionRepository::new(db.clone());
    let second_worker = Uuid::now_v7();
    let (first, second) = tokio::join!(
        repo.claim_pending_drop(worker, now, now + Duration::minutes(5)),
        second_repo.claim_pending_drop(second_worker, now, now + Duration::minutes(5),),
    );
    assert_eq!(
        usize::from(first.expect("first claim").is_some())
            + usize::from(second.expect("second claim").is_some()),
        1,
        "SKIP LOCKED lease must have exactly one owner"
    );

    let manifest_update = db
        .execute(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE quant_archive_partition_manifest SET row_count = row_count + 1 \
             WHERE manifest_id = $1",
            [sealed.manifest_id.into()],
        ))
        .await;
    assert!(manifest_update.is_err(), "manifest UPDATE must be denied");

    let claimed = repo
        .claim_pending_drop(
            worker,
            now + Duration::minutes(6),
            now + Duration::minutes(11),
        )
        .await
        .expect("reclaim expired lease")
        .expect("claimed manifest");
    let audit = repo
        .complete_drop(claimed.manifest_id, worker, now + Duration::minutes(6))
        .await
        .expect("complete drop");
    let audit_delete = db
        .execute(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "DELETE FROM quant_archive_partition_drop_audit WHERE audit_id = $1",
            [audit.audit_id.into()],
        ))
        .await;
    assert!(audit_delete.is_err(), "drop audit DELETE must be denied");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn closed_loop_migration_rolls_back_and_reapplies() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection();

    Migrator::down(db, Some(1))
        .await
        .expect("rollback latest migration");
    let absent = db
        .execute(Statement::from_string(
            DbBackend::Postgres,
            "SELECT 1 FROM quant_archive_partition_manifest LIMIT 1",
        ))
        .await;
    assert!(absent.is_err(), "rollback must remove closed-loop tables");

    Migrator::up(db, None)
        .await
        .expect("reapply latest migration");
    db.execute(Statement::from_string(
        DbBackend::Postgres,
        "SELECT 1 FROM quant_archive_partition_manifest LIMIT 1",
    ))
    .await
    .expect("reapplied archive manifest table");
}
