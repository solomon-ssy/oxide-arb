//! Cross-database infrastructure contracts on one disposable shared stack.

use std::{future::Future, pin::Pin};

use quant_pivot_system_tests::resources::with_resource_suite;

#[path = "infrastructure/clickhouse/repository_reads.rs"]
mod infrastructure_clickhouse_repository_reads;
#[path = "infrastructure/clickhouse/storage.rs"]
mod infrastructure_clickhouse_storage;
#[path = "infrastructure/postgres/schema_migration.rs"]
mod infrastructure_postgres_schema_migration;
#[path = "infrastructure/postgres/schema_mutation.rs"]
mod infrastructure_postgres_schema_mutation;
#[path = "infrastructure/redis/backend.rs"]
mod infrastructure_redis_backend;
#[path = "infrastructure/redis/tiered_cache.rs"]
mod infrastructure_redis_tiered_cache;

type Scenario = Pin<Box<dyn Future<Output = ()>>>;

macro_rules! run_scenarios {
    ($($scenario:path),+ $(,)?) => {
        $(
            run_scenario(stringify!($scenario), Box::pin($scenario())).await;
        )+
    };
}

async fn run_scenario(name: &str, scenario: Scenario) {
    eprintln!("running infrastructure scenario: {name}");
    scenario.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn infrastructure_contracts_share_stack() {
    with_resource_suite(async {
        run_scenarios!(
            infrastructure_redis_tiered_cache::tiered_l2_hit_l1,
            infrastructure_redis_tiered_cache::tiered_both_returns_none,
            infrastructure_redis_tiered_cache::tiered_set_populates_levels,
            infrastructure_postgres_schema_migration::migration_plan_empty_database,
            infrastructure_postgres_schema_migration::immutable_baseline_idempotent_rejected,
            infrastructure_postgres_schema_migration::boot_rejects_unknown_schema,
            infrastructure_postgres_schema_migration::immutable_migrations_empty_database,
            infrastructure_postgres_schema_migration::legacy_sqlx_ledger_forbidden,
            infrastructure_postgres_schema_migration::migration_artifact_checksum_rejected,
            infrastructure_postgres_schema_migration::unknown_future_native_rejected,
            infrastructure_postgres_schema_migration::native_enum_drift_rejected,
            infrastructure_postgres_schema_migration::column_definition_drift_rejected,
            infrastructure_postgres_schema_migration::index_definition_drift_rejected,
            infrastructure_postgres_schema_migration::constraint_definition_drift_rejected,
            infrastructure_postgres_schema_migration::trigger_definition_drift_rejected,
            infrastructure_postgres_schema_migration::grant_drift_is_rejected,
            infrastructure_redis_backend::redis_set_get_roundtrip,
            infrastructure_redis_backend::redis_missing_returns_none,
            infrastructure_redis_backend::redis_delete_removes_entry,
            infrastructure_redis_backend::redis_mget_mset,
            infrastructure_redis_backend::redis_health_check,
            infrastructure_redis_backend::preproduction_cleanup_namespace_exact,
            infrastructure_redis_backend::preproduction_rejects_concurrent_writer,
            infrastructure_clickhouse_repository_reads::ch_read_orders_tiebreaker,
            infrastructure_clickhouse_repository_reads::scans_reject_unavailable_rows,
            infrastructure_clickhouse_repository_reads::resolution_pit_bounded,
            infrastructure_clickhouse_repository_reads::weather_long_form_preserving,
            infrastructure_clickhouse_repository_reads::trade_preserves_after_merge,
            infrastructure_clickhouse_repository_reads::replacing_merge_tree_row,
            infrastructure_postgres_schema_mutation::empty_postgres_bootstraps_verifies,
            infrastructure_postgres_schema_mutation::schema_cancels_after_loss,
            infrastructure_postgres_schema_mutation::reset_rejects_unknown_never,
            infrastructure_clickhouse_storage::first_creates_missing_schema,
            infrastructure_clickhouse_storage::deployment_runtime_rejects_held,
            infrastructure_clickhouse_storage::clean_boot_rejects_database,
            infrastructure_clickhouse_storage::preproduction_rejects_without_dropping,
            infrastructure_clickhouse_storage::clickhouse_health_check,
            infrastructure_clickhouse_storage::clickhouse_schema_idempotent,
            infrastructure_clickhouse_storage::native_query_limits_rejects,
            infrastructure_clickhouse_storage::canonical_evidence_no_ttl,
            infrastructure_clickhouse_storage::runtime_schema_rejects_ttl,
            infrastructure_clickhouse_storage::runtime_schema_rejects_drift,
            infrastructure_clickhouse_storage::runtime_verification_rejects_drift,
            infrastructure_clickhouse_storage::clickhouse_fact_uses_columns,
            infrastructure_clickhouse_storage::crypto_price_matches_schema,
            infrastructure_clickhouse_storage::domain_event_matches_schema,
            infrastructure_clickhouse_storage::report_fact_accepts_snapshot,
            infrastructure_clickhouse_storage::trade_tape_direct_roundtrip,
            infrastructure_clickhouse_storage::last_trade_projects_once,
            infrastructure_clickhouse_storage::async_writer_shutdown_buffer,
            infrastructure_clickhouse_storage::async_writer_channel_buffer,
        );
    })
    .await
    .expect("start disposable infrastructure stack");
}
