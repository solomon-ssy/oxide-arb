//! Cross-crate core business scenarios owned by the system-test boundary.

#[path = "core/catalog_bootstrap.rs"]
mod catalog_bootstrap;
#[path = "core/equity_snapshot.rs"]
mod equity_snapshot;
#[path = "core/factor_pipeline.rs"]
mod factor_pipeline;
#[path = "core/feature_pipeline.rs"]
mod feature_pipeline;
#[path = "core/health_readiness.rs"]
mod health_readiness;
#[path = "core/market_selection.rs"]
mod market_selection;
#[path = "core/model_governance.rs"]
mod model_governance;
#[path = "core/model_runtime.rs"]
mod model_runtime;
#[path = "core/model_training_backtest.rs"]
mod model_training_backtest;
#[path = "core/outcome_reconciliation.rs"]
mod outcome_reconciliation;
#[path = "core/outcome_reconciliation_producer.rs"]
mod outcome_reconciliation_producer;
#[path = "core/participant_concentration.rs"]
mod participant_concentration;
#[path = "core/report_pipeline.rs"]
mod report_pipeline;
#[path = "core/training_dataset.rs"]
mod training_dataset;
#[path = "core/weather_linkage.rs"]
mod weather_linkage;

use quant_pivot_system_tests::postgres;
macro_rules! scenario {
    ($module:ident::$function:ident) => {{
        let name = concat!(stringify!($module), "::", stringify!($function));
        eprintln!("core business scenario started: {name}");
        $module::$function().await;
        eprintln!("core business scenario passed: {name}");
    }};
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn core_business_scenarios_server() {
    Box::pin(postgres::with_postgres_suite(async {
        scenario!(catalog_bootstrap::model_spec_service_spec);
        scenario!(catalog_bootstrap::factor_register_publish_catalog);

        scenario!(equity_snapshot::account_no_history_drawdown);
        scenario!(equity_snapshot::resolve_drawdown_concurrent_history);
        scenario!(equity_snapshot::equity_snapshot_real_pnl);

        scenario!(factor_pipeline::create_definition_values_run);
        scenario!(factor_pipeline::unpublished_factor_definitions_pipeline);
        scenario!(factor_pipeline::factor_event_writer_batches);

        scenario!(feature_pipeline::insufficient_vectors_audited_input);
        scenario!(feature_pipeline::create_feature_vector_find);

        scenario!(health_readiness::ws_skipped_catalog_warming);
        scenario!(health_readiness::ws_skipped_while_connecting);
        scenario!(health_readiness::ws_reports_message_fresh);
        scenario!(health_readiness::ws_unhealthy_message_stale);
        scenario!(health_readiness::ws_unhealthy_shards_disconnected);

        scenario!(market_selection::provider_selector_mapper_trip);

        scenario!(model_governance::publish_requires_quality_pass);
        scenario!(model_governance::publish_without_training_transition);
        scenario!(model_governance::publish_requires_backtest_report);
        scenario!(model_governance::publish_requires_shadow_stability);
        scenario!(model_governance::publish_succeeds_without_immutable);
        scenario!(model_governance::retire_unrouted_without_routing);
        scenario!(model_governance::rejects_routed_model_retirement);
        scenario!(model_governance::uncalibrated_return_cannot_publish);
        scenario!(model_governance::bind_calibration_creates_model);
        scenario!(model_governance::publish_rescans_not_findings);
        scenario!(model_governance::sell_publish_requires_set);
        scenario!(model_governance::sell_publish_succeeds_set);

        scenario!(model_runtime::online_loop_selection_candidates);
        scenario!(model_runtime::inference_failure_keeps_active);
        scenario!(model_runtime::hot_changes_not_artifact);
        scenario!(model_runtime::inference_rejects_retired_model);
        scenario!(model_runtime::model_run_create_fail);

        scenario!(model_training_backtest::train_backtest_calibrate_e2e);
        scenario!(model_training_backtest::train_cpcv_persists_decomposition);

        scenario!(participant_concentration::whale_trade_tape_monitor);

        scenario!(report_pipeline::ad_hoc_publishes_recommendations);
        scenario!(report_pipeline::ad_hoc_idempotent_key);
        scenario!(report_pipeline::empty_selection_publishes_report);
        scenario!(report_pipeline::missing_non_empty_report);
        scenario!(report_pipeline::account_fails_without_row);
        scenario!(report_pipeline::revoke_after_publish);
        scenario!(report_pipeline::evidence_refs_rank_populated);
        scenario!(report_pipeline::report_persists_real_history);

        scenario!(training_dataset::historical_pit_no_build);
        scenario!(training_dataset::calibration_dataset_rejects_overlap);
        scenario!(training_dataset::build_before_no_row);
        scenario!(training_dataset::pit_selection_excludes_market);
        scenario!(training_dataset::pit_excludes_requires_feature);
        scenario!(training_dataset::pit_selection_includes_available);
        scenario!(training_dataset::plan_estimates_keep_rate);
        scenario!(training_dataset::dataset_builder_rejects_features);
        scenario!(training_dataset::settlement_not_before_resolution);
        scenario!(training_dataset::settlement_label_after_resolution);
        scenario!(training_dataset::plan_build_reuses_id);
        scenario!(training_dataset::build_status_no_mature);
        scenario!(training_dataset::build_failed_zero_examples);
        scenario!(training_dataset::build_book_decode_failures);
        scenario!(training_dataset::settlement_label_without_resolution);
        scenario!(training_dataset::model_version_training_typed);
        scenario!(training_dataset::plan_count_respects_sources);

        scenario!(weather_linkage::single_sibling_validates_group);
    }))
    .await
    .expect("start shared core-business PostgreSQL suite");
}
