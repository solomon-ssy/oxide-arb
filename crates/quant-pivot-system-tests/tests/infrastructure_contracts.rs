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

async fn run_scenario(name: &str, scenario: Scenario) {
    eprintln!("running infrastructure scenario: {name}");
    scenario.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn infrastructure_contracts_share_one_disposable_stack() {
    with_resource_suite(async {
        run_scenario("infrastructure_redis_tiered_cache::tiered_l2_hit_backfills_l1", Box::pin(infrastructure_redis_tiered_cache::tiered_l2_hit_backfills_l1())).await;
        run_scenario("infrastructure_redis_tiered_cache::tiered_both_miss_returns_none", Box::pin(infrastructure_redis_tiered_cache::tiered_both_miss_returns_none())).await;
        run_scenario("infrastructure_redis_tiered_cache::tiered_set_populates_both_levels", Box::pin(infrastructure_redis_tiered_cache::tiered_set_populates_both_levels())).await;
        run_scenario("infrastructure_postgres_schema_migration::migration_plan_is_read_only_on_empty_database", Box::pin(infrastructure_postgres_schema_migration::migration_plan_is_read_only_on_empty_database())).await;
        run_scenario("infrastructure_postgres_schema_migration::immutable_baseline_is_idempotent_and_drift_is_rejected", Box::pin(infrastructure_postgres_schema_migration::immutable_baseline_is_idempotent_and_drift_is_rejected())).await;
        run_scenario("infrastructure_postgres_schema_migration::boot_baseline_rejects_a_nonempty_unknown_schema", Box::pin(infrastructure_postgres_schema_migration::boot_baseline_rejects_a_nonempty_unknown_schema())).await;
        run_scenario("infrastructure_postgres_schema_migration::immutable_migrations_round_trip_on_empty_database", Box::pin(infrastructure_postgres_schema_migration::immutable_migrations_round_trip_on_empty_database())).await;
        run_scenario("infrastructure_postgres_schema_migration::legacy_sqlx_ledger_is_forbidden", Box::pin(infrastructure_postgres_schema_migration::legacy_sqlx_ledger_is_forbidden())).await;
        run_scenario("infrastructure_postgres_schema_migration::migration_artifact_checksum_tamper_is_rejected", Box::pin(infrastructure_postgres_schema_migration::migration_artifact_checksum_tamper_is_rejected())).await;
        run_scenario("infrastructure_postgres_schema_migration::unknown_future_native_migration_is_rejected", Box::pin(infrastructure_postgres_schema_migration::unknown_future_native_migration_is_rejected())).await;
        run_scenario("infrastructure_postgres_schema_migration::native_enum_drift_is_rejected", Box::pin(infrastructure_postgres_schema_migration::native_enum_drift_is_rejected())).await;
        run_scenario("infrastructure_postgres_schema_migration::column_definition_drift_is_rejected", Box::pin(infrastructure_postgres_schema_migration::column_definition_drift_is_rejected())).await;
        run_scenario("infrastructure_postgres_schema_migration::index_definition_drift_is_rejected", Box::pin(infrastructure_postgres_schema_migration::index_definition_drift_is_rejected())).await;
        run_scenario("infrastructure_postgres_schema_migration::constraint_definition_drift_is_rejected", Box::pin(infrastructure_postgres_schema_migration::constraint_definition_drift_is_rejected())).await;
        run_scenario("infrastructure_postgres_schema_migration::trigger_definition_drift_is_rejected", Box::pin(infrastructure_postgres_schema_migration::trigger_definition_drift_is_rejected())).await;
        run_scenario("infrastructure_postgres_schema_migration::grant_drift_is_rejected", Box::pin(infrastructure_postgres_schema_migration::grant_drift_is_rejected())).await;
        run_scenario("infrastructure_redis_backend::redis_set_get_roundtrip", Box::pin(infrastructure_redis_backend::redis_set_get_roundtrip())).await;
        run_scenario("infrastructure_redis_backend::redis_get_missing_returns_none", Box::pin(infrastructure_redis_backend::redis_get_missing_returns_none())).await;
        run_scenario("infrastructure_redis_backend::redis_delete_removes_entry", Box::pin(infrastructure_redis_backend::redis_delete_removes_entry())).await;
        run_scenario("infrastructure_redis_backend::redis_mget_mset", Box::pin(infrastructure_redis_backend::redis_mget_mset())).await;
        run_scenario("infrastructure_redis_backend::redis_health_check", Box::pin(infrastructure_redis_backend::redis_health_check())).await;
        run_scenario("infrastructure_redis_backend::preproduction_cleanup_is_namespace_exact", Box::pin(infrastructure_redis_backend::preproduction_cleanup_is_namespace_exact())).await;
        run_scenario("infrastructure_redis_backend::preproduction_cleanup_fails_closed_with_a_concurrent_writer", Box::pin(infrastructure_redis_backend::preproduction_cleanup_fails_closed_with_a_concurrent_writer())).await;
        run_scenario("infrastructure_clickhouse_repository_reads::ch_read_orders_by_event_time_with_tiebreaker", Box::pin(infrastructure_clickhouse_repository_reads::ch_read_orders_by_event_time_with_tiebreaker())).await;
        run_scenario("infrastructure_clickhouse_repository_reads::historical_scans_reject_rows_not_yet_available", Box::pin(infrastructure_clickhouse_repository_reads::historical_scans_reject_rows_not_yet_available())).await;
        run_scenario("infrastructure_clickhouse_repository_reads::resolution_at_is_pit_bounded", Box::pin(infrastructure_clickhouse_repository_reads::resolution_at_is_pit_bounded())).await;
        run_scenario("infrastructure_clickhouse_repository_reads::weather_long_form_facts_are_pit_visible_and_revision_preserving", Box::pin(infrastructure_clickhouse_repository_reads::weather_long_form_facts_are_pit_visible_and_revision_preserving())).await;
        run_scenario("infrastructure_clickhouse_repository_reads::trade_tape_preserves_prior_revisions_after_merge", Box::pin(infrastructure_clickhouse_repository_reads::trade_tape_preserves_prior_revisions_after_merge())).await;
        run_scenario("infrastructure_clickhouse_repository_reads::replacing_merge_tree_readers_return_one_latest_logical_row", Box::pin(infrastructure_clickhouse_repository_reads::replacing_merge_tree_readers_return_one_latest_logical_row())).await;
        run_scenario("infrastructure_postgres_schema_mutation::empty_postgres_bootstraps_once_and_verifies", Box::pin(infrastructure_postgres_schema_mutation::empty_postgres_bootstraps_once_and_verifies())).await;
        run_scenario("infrastructure_postgres_schema_mutation::schema_mutation_lease_is_exclusive_and_cancels_after_session_loss", Box::pin(infrastructure_postgres_schema_mutation::schema_mutation_lease_is_exclusive_and_cancels_after_session_loss())).await;
        run_scenario("infrastructure_postgres_schema_mutation::reset_rejects_unknown_sessions_and_never_forces_them_closed", Box::pin(infrastructure_postgres_schema_mutation::reset_rejects_unknown_sessions_and_never_forces_them_closed())).await;
        run_scenario("infrastructure_clickhouse_storage::first_deployment_creates_missing_database_and_schema", Box::pin(infrastructure_clickhouse_storage::first_deployment_creates_missing_database_and_schema())).await;
        run_scenario("infrastructure_clickhouse_storage::deployment_and_runtime_fail_closed_while_schema_lock_is_held", Box::pin(infrastructure_clickhouse_storage::deployment_and_runtime_fail_closed_while_schema_lock_is_held())).await;
        run_scenario("infrastructure_clickhouse_storage::clean_boot_rejects_nonempty_unmanaged_database", Box::pin(infrastructure_clickhouse_storage::clean_boot_rejects_nonempty_unmanaged_database())).await;
        run_scenario("infrastructure_clickhouse_storage::preproduction_reset_rejects_active_clickhouse_queries_without_dropping", Box::pin(infrastructure_clickhouse_storage::preproduction_reset_rejects_active_clickhouse_queries_without_dropping())).await;
        run_scenario("infrastructure_clickhouse_storage::clickhouse_health_check", Box::pin(infrastructure_clickhouse_storage::clickhouse_health_check())).await;
        run_scenario("infrastructure_clickhouse_storage::clickhouse_schema_idempotent", Box::pin(infrastructure_clickhouse_storage::clickhouse_schema_idempotent())).await;
        run_scenario("infrastructure_clickhouse_storage::native_query_result_limits_fail_closed", Box::pin(infrastructure_clickhouse_storage::native_query_result_limits_fail_closed())).await;
        run_scenario("infrastructure_clickhouse_storage::canonical_evidence_tables_have_no_delete_ttl", Box::pin(infrastructure_clickhouse_storage::canonical_evidence_tables_have_no_delete_ttl())).await;
        run_scenario("infrastructure_clickhouse_storage::runtime_schema_verification_rejects_unmanaged_raw_ttl", Box::pin(infrastructure_clickhouse_storage::runtime_schema_verification_rejects_unmanaged_raw_ttl())).await;
        run_scenario("infrastructure_clickhouse_storage::runtime_schema_verification_rejects_migration_ledger_drift", Box::pin(infrastructure_clickhouse_storage::runtime_schema_verification_rejects_migration_ledger_drift())).await;
        run_scenario("infrastructure_clickhouse_storage::runtime_schema_verification_rejects_semantic_column_drift", Box::pin(infrastructure_clickhouse_storage::runtime_schema_verification_rejects_semantic_column_drift())).await;
        run_scenario("infrastructure_clickhouse_storage::clickhouse_fact_contract_uses_decimal_and_enum_columns", Box::pin(infrastructure_clickhouse_storage::clickhouse_fact_contract_uses_decimal_and_enum_columns())).await;
        run_scenario("infrastructure_clickhouse_storage::crypto_price_report_rust_row_matches_clickhouse_schema", Box::pin(infrastructure_clickhouse_storage::crypto_price_report_rust_row_matches_clickhouse_schema())).await;
        run_scenario("infrastructure_clickhouse_storage::domain_event_rust_row_matches_clickhouse_schema", Box::pin(infrastructure_clickhouse_storage::domain_event_rust_row_matches_clickhouse_schema())).await;
        run_scenario("infrastructure_clickhouse_storage::report_fact_schema_accepts_decision_snapshot_and_superseded_censor", Box::pin(infrastructure_clickhouse_storage::report_fact_schema_accepts_decision_snapshot_and_superseded_censor())).await;
        run_scenario("infrastructure_clickhouse_storage::trade_tape_direct_insert_roundtrip", Box::pin(infrastructure_clickhouse_storage::trade_tape_direct_insert_roundtrip())).await;
        run_scenario("infrastructure_clickhouse_storage::last_trade_ledger_retry_projects_exactly_once", Box::pin(infrastructure_clickhouse_storage::last_trade_ledger_retry_projects_exactly_once())).await;
        run_scenario("infrastructure_clickhouse_storage::async_writer_shutdown_drains_buffer", Box::pin(infrastructure_clickhouse_storage::async_writer_shutdown_drains_buffer())).await;
        run_scenario("infrastructure_clickhouse_storage::async_writer_channel_close_drains_buffer", Box::pin(infrastructure_clickhouse_storage::async_writer_channel_close_drains_buffer())).await;
    })
    .await
    .expect("start disposable infrastructure stack");
}
