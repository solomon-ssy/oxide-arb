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
async fn core_business_scenarios_share_one_postgres_server() {
    Box::pin(postgres::with_postgres_suite(async {
        scenario!(catalog_bootstrap::model_spec_service_authors_draft_spec);
        scenario!(catalog_bootstrap::factor_register_then_publish_batch_seeds_catalog);

        scenario!(equity_snapshot::new_account_no_history_is_neutral_drawdown);
        scenario!(equity_snapshot::resolve_drawdown_re_read_picks_up_concurrent_history);
        scenario!(equity_snapshot::equity_snapshot_records_real_equity_and_pnl);

        scenario!(factor_pipeline::create_definition_and_values_then_list_for_run);
        scenario!(factor_pipeline::unpublished_factor_definitions_block_pipeline);
        scenario!(factor_pipeline::factor_event_writer_batches);

        scenario!(feature_pipeline::insufficient_vectors_are_audited_but_partitioned_from_model_input);
        scenario!(feature_pipeline::create_feature_vector_then_find);

        scenario!(health_readiness::ws_skipped_during_catalog_warming);
        scenario!(health_readiness::ws_skipped_while_market_data_connecting);
        scenario!(health_readiness::ws_reports_message_age_when_fresh);
        scenario!(health_readiness::ws_unhealthy_when_message_stale);
        scenario!(health_readiness::ws_unhealthy_when_shards_disconnected);

        scenario!(market_selection::provider_selector_mapper_persist_round_trip);

        scenario!(model_governance::publish_requires_quality_gate_pass);
        scenario!(model_governance::publish_without_training_dataset_is_illegal_transition);
        scenario!(model_governance::publish_requires_backtest_report);
        scenario!(model_governance::publish_requires_shadow_stability);
        scenario!(model_governance::publish_succeeds_without_mutating_routing_then_version_is_immutable);
        scenario!(model_governance::retire_unrouted_published_version_audits_without_mutating_routing);
        scenario!(model_governance::retire_routed_published_version_is_rejected_fail_closed);
        scenario!(model_governance::uncalibrated_return_model_cannot_publish);
        scenario!(model_governance::bind_calibration_creates_candidate_version_with_calibrated_return_model);
        scenario!(model_governance::publish_rescans_leakage_not_default_findings);
        scenario!(model_governance::sell_publish_requires_bound_cpcv_path_set);
        scenario!(model_governance::sell_publish_succeeds_with_bound_cpcv_path_set);

        scenario!(model_runtime::online_loop_selection_to_signal_candidates);
        scenario!(model_runtime::inference_degradation_shadow_failure_keeps_active);
        scenario!(model_runtime::hot_update_changes_candidate_weights_not_published_artifact);
        scenario!(model_runtime::inference_rejects_retired_active_model);
        scenario!(model_runtime::model_run_create_find_succeed_fail);

        scenario!(model_training_backtest::train_then_backtest_then_calibrate_e2e);
        scenario!(model_training_backtest::train_then_cpcv_persists_path_set_with_dsr_n_decomposition);

        scenario!(participant_concentration::whale_trade_tape_scores_feature_factor_and_monitor);

        scenario!(report_pipeline::ad_hoc_publishes_report_with_recommendations);
        scenario!(report_pipeline::ad_hoc_idempotent_on_trigger_key);
        scenario!(report_pipeline::empty_selection_publishes_formal_report);
        scenario!(report_pipeline::missing_trade_policy_publishes_explicit_non_actionable_empty_report);
        scenario!(report_pipeline::account_unavailable_fails_without_report_row);
        scenario!(report_pipeline::revoke_after_publish);
        scenario!(report_pipeline::evidence_refs_and_rank_scores_populated);
        scenario!(report_pipeline::report_persists_real_drawdown_from_equity_history);

        scenario!(training_dataset::historical_pit_no_look_ahead_via_dataset_build);
        scenario!(training_dataset::calibration_dataset_build_fails_closed_on_purge_overlap);
        scenario!(training_dataset::build_cancelled_before_spine_yields_cancelled_and_no_row);
        scenario!(training_dataset::pit_selection_excludes_disabled_category_market);
        scenario!(training_dataset::pit_selection_excludes_crypto_market_when_model_requires_unavailable_domain_feature);
        scenario!(training_dataset::pit_selection_includes_crypto_market_when_domain_feature_is_resolved_and_available);
        scenario!(training_dataset::plan_estimates_pit_keep_rate);
        scenario!(training_dataset::dataset_builder_rejects_future_features);
        scenario!(training_dataset::settlement_label_not_mature_before_resolution);
        scenario!(training_dataset::settlement_label_available_after_resolution);
        scenario!(training_dataset::plan_build_reuses_training_dataset_id);
        scenario!(training_dataset::build_status_insufficient_labels_when_no_labels_mature);
        scenario!(training_dataset::build_status_failed_when_zero_examples);
        scenario!(training_dataset::build_records_book_decode_failures);
        scenario!(training_dataset::settlement_label_visible_without_micro_past_resolution);
        scenario!(training_dataset::model_version_training_dataset_id_is_typed);
        scenario!(training_dataset::plan_count_respects_sample_sources);

        scenario!(weather_linkage::single_sibling_request_validates_and_atomically_appends_the_complete_group);
    }))
    .await
    .expect("start shared core-business PostgreSQL suite");
}
