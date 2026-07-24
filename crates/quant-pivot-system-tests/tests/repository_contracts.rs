//! Repository persistence contracts against one disposable `PostgreSQL` server.

use std::{future::Future, pin::Pin};

use quant_pivot_system_tests::postgres::with_postgres_suite;

#[path = "repository/governance/access_control.rs"]
mod access_control;
#[path = "repository/accounting/account_capital.rs"]
mod account_capital;
#[path = "repository/research/backtest_path_set.rs"]
mod backtest_path_set;
#[path = "repository/research/backtest_report.rs"]
mod backtest_report;
#[path = "repository/execution/basis_alert.rs"]
mod basis_alert;
#[path = "repository/research/calibration_artifact.rs"]
mod calibration_artifact;
#[path = "repository/catalog/catalog_ledger.rs"]
mod catalog_ledger;
#[path = "repository/research/comparison_report.rs"]
mod comparison_report;
#[path = "repository/catalog/domain_projection.rs"]
mod domain_projection;
#[path = "repository/catalog/domain_source_cursor.rs"]
mod domain_source_cursor;
#[path = "repository/catalog/domain_source_expectation.rs"]
mod domain_source_expectation;
#[path = "repository/execution/entry_condition_evaluation.rs"]
mod entry_condition_evaluation;
#[path = "repository/accounting/equity_snapshot.rs"]
mod equity_snapshot;
#[path = "repository/execution/execution_submission.rs"]
mod execution_submission;
#[path = "repository/research/factor_revision.rs"]
mod factor_revision;
#[path = "repository/research/feature_parity.rs"]
mod feature_parity;
#[path = "repository/research/feedback_cohort.rs"]
mod feedback_cohort;
#[path = "repository/catalog/market_linkage.rs"]
mod market_linkage;
#[path = "repository/catalog/market_page.rs"]
mod market_page;
#[path = "repository/catalog/market_selection.rs"]
mod market_selection;
#[path = "repository/governance/model_governance.rs"]
mod model_governance;
#[path = "repository/research/model_registry.rs"]
mod model_registry;
#[path = "repository/governance/policy_governance.rs"]
mod policy_governance;
#[path = "repository/accounting/portfolio_optimizer.rs"]
mod portfolio_optimizer;
#[path = "repository/accounting/recommendation_execution_outcome.rs"]
mod recommendation_execution_outcome;
#[path = "repository/accounting/recommendation_resolution_outcome.rs"]
mod recommendation_resolution_outcome;
#[path = "repository/execution/report_scheduler.rs"]
mod report_scheduler;
#[path = "repository/research/research_job.rs"]
mod research_job;
#[path = "repository/research/research_readiness.rs"]
mod research_readiness;
#[path = "repository/governance/runtime_control.rs"]
mod runtime_control;
#[path = "repository/research/trade_policy_trial.rs"]
mod trade_policy_trial;
#[path = "repository/research/training_dataset.rs"]
mod training_dataset;
#[path = "repository/governance/typed_persistence.rs"]
mod typed_persistence;
#[path = "repository/catalog/weather_daily_temperature.rs"]
mod weather_daily_temperature;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repository_persistence_contracts_share_one_postgres_stack() {
    Box::pin(with_postgres_suite(async {
        scenario("account_capital::account_snapshot_repo_create_find", Box::pin(account_capital::account_snapshot_repo_create_find())).await;
        scenario("account_capital::reserved_capital_reader_returns_zero_when_empty", Box::pin(account_capital::reserved_capital_reader_returns_zero_when_empty())).await;
        scenario("account_capital::report_transaction_persists_chain_and_reserved_capital_sums_pending_intents", Box::pin(account_capital::report_transaction_persists_chain_and_reserved_capital_sums_pending_intents())).await;
        scenario("account_capital::find_expirable_returns_published_reports_before_cutoff_only", Box::pin(account_capital::find_expirable_returns_published_reports_before_cutoff_only())).await;
        scenario("account_capital::report_fact_delivery_recovers_retry_and_expired_lease_without_early_claim", Box::pin(account_capital::report_fact_delivery_recovers_retry_and_expired_lease_without_early_claim())).await;
        scenario("account_capital::execution_order_and_reconciliation_repositories_round_trip", Box::pin(account_capital::execution_order_and_reconciliation_repositories_round_trip())).await;
        scenario("account_capital::capital_and_kill_switch_repositories_round_trip", Box::pin(account_capital::capital_and_kill_switch_repositories_round_trip())).await;
        scenario("equity_snapshot::equity_snapshot_repo_create_latest_hwm", Box::pin(equity_snapshot::equity_snapshot_repo_create_latest_hwm())).await;
        scenario("equity_snapshot::high_water_mark_is_monotonic_max", Box::pin(equity_snapshot::high_water_mark_is_monotonic_max())).await;
        scenario("equity_snapshot::drawdown_pct_is_hwm_minus_equity_over_hwm", Box::pin(equity_snapshot::drawdown_pct_is_hwm_minus_equity_over_hwm())).await;
        scenario("equity_snapshot::realized_pnl_cumulative_matches_position_ledger_sum", Box::pin(equity_snapshot::realized_pnl_cumulative_matches_position_ledger_sum())).await;
        scenario("portfolio_optimizer::optimizer_meta_persisted_in_plan_row", Box::pin(portfolio_optimizer::optimizer_meta_persisted_in_plan_row())).await;
        scenario("catalog_ledger::correction_is_invisible_until_its_availability_time", Box::pin(catalog_ledger::correction_is_invisible_until_its_availability_time())).await;
        scenario("catalog_ledger::batch_snapshot_observes_one_exact_event_revision_and_membership", Box::pin(catalog_ledger::batch_snapshot_observes_one_exact_event_revision_and_membership())).await;
        scenario("catalog_ledger::batch_snapshot_rejects_decisions_before_catalog_coverage", Box::pin(catalog_ledger::batch_snapshot_rejects_decisions_before_catalog_coverage())).await;
        scenario("catalog_ledger::concurrent_batch_reads_never_observe_a_torn_catalog_commit", Box::pin(catalog_ledger::concurrent_batch_reads_never_observe_a_torn_catalog_commit())).await;
        scenario("catalog_ledger::failed_attempt_is_audited_but_never_creates_catalog_coverage", Box::pin(catalog_ledger::failed_attempt_is_audited_but_never_creates_catalog_coverage())).await;
        scenario("catalog_ledger::identical_reconcile_only_appends_batch_audit", Box::pin(catalog_ledger::identical_reconcile_only_appends_batch_audit())).await;
        scenario("catalog_ledger::projection_upsert_updates_filter_reasons_atomically_with_status", Box::pin(catalog_ledger::projection_upsert_updates_filter_reasons_atomically_with_status())).await;
        scenario("catalog_ledger::object_payload_hash_drift_is_rejected_before_catalog_commit", Box::pin(catalog_ledger::object_payload_hash_drift_is_rejected_before_catalog_commit())).await;
        scenario("domain_projection::crypto_source_sequence_roundtrips_through_postgres_bigint", Box::pin(domain_projection::crypto_source_sequence_roundtrips_through_postgres_bigint())).await;
        scenario("domain_projection::crypto_source_sequence_above_postgres_bigint_is_rejected_before_write", Box::pin(domain_projection::crypto_source_sequence_above_postgres_bigint_is_rejected_before_write())).await;
        scenario("domain_source_expectation::expected_source_exists_before_cursor_and_transitions_optimistically", Box::pin(domain_source_expectation::expected_source_exists_before_cursor_and_transitions_optimistically())).await;
        scenario("domain_source_expectation::natural_key_upsert_updates_one_stable_expectation", Box::pin(domain_source_expectation::natural_key_upsert_updates_one_stable_expectation())).await;
        scenario("market_linkage::valid_at_never_sees_a_revision_effective_after_the_source_cutoff", Box::pin(market_linkage::valid_at_never_sees_a_revision_effective_after_the_source_cutoff())).await;
        scenario("market_linkage::valid_at_for_markets_matches_valid_at_batched", Box::pin(market_linkage::valid_at_for_markets_matches_valid_at_batched())).await;
        scenario("market_linkage::backdated_row_is_invisible_before_database_availability", Box::pin(market_linkage::backdated_row_is_invisible_before_database_availability())).await;
        scenario("market_linkage::append_batch_rolls_back_the_entire_group_when_any_member_is_invalid", Box::pin(market_linkage::append_batch_rolls_back_the_entire_group_when_any_member_is_invalid())).await;
        scenario("market_page::market_page_filters_by_event_id_and_category", Box::pin(market_page::market_page_filters_by_event_id_and_category())).await;
        scenario("market_selection::create_snapshot_then_find_and_list_members", Box::pin(market_selection::create_snapshot_then_find_and_list_members())).await;
        scenario("weather_daily_temperature::weather_projection_tracks_maximum_and_minimum_with_independent_events", Box::pin(weather_daily_temperature::weather_projection_tracks_maximum_and_minimum_with_independent_events())).await;
        scenario("basis_alert::record_persists_and_round_trips", Box::pin(basis_alert::record_persists_and_round_trips())).await;
        scenario("basis_alert::latest_for_market_picks_the_newest_as_of", Box::pin(basis_alert::latest_for_market_picks_the_newest_as_of())).await;
        scenario("basis_alert::batched_latest_returns_one_newest_alert_per_market", Box::pin(basis_alert::batched_latest_returns_one_newest_alert_per_market())).await;
        scenario("basis_alert::page_filters_by_market_and_time_range", Box::pin(basis_alert::page_filters_by_market_and_time_range())).await;
        scenario("basis_alert::acknowledge_marks_the_alert_and_is_idempotent", Box::pin(basis_alert::acknowledge_marks_the_alert_and_is_idempotent())).await;
        scenario("basis_alert::acknowledge_missing_alert_fails_closed", Box::pin(basis_alert::acknowledge_missing_alert_fails_closed())).await;
        scenario("entry_condition_evaluation::semantic_revision_and_outbox_claims_are_atomic_and_deduplicated", Box::pin(entry_condition_evaluation::semantic_revision_and_outbox_claims_are_atomic_and_deduplicated())).await;
        scenario("execution_submission::claim_guards_against_double_submit", Box::pin(execution_submission::claim_guards_against_double_submit())).await;
        scenario("execution_submission::entry_condition_artifact_and_audit_are_database_worm", Box::pin(execution_submission::entry_condition_artifact_and_audit_are_database_worm())).await;
        scenario("execution_submission::concurrent_approval_has_one_winner_and_one_amount_truth", Box::pin(execution_submission::concurrent_approval_has_one_winner_and_one_amount_truth())).await;
        scenario("execution_submission::expiry_is_atomic_and_idempotent_across_capital_and_audit", Box::pin(execution_submission::expiry_is_atomic_and_idempotent_across_capital_and_audit())).await;
        scenario("execution_submission::expiry_and_cancel_race_has_one_terminal_owner", Box::pin(execution_submission::expiry_and_cancel_race_has_one_terminal_owner())).await;
        scenario("execution_submission::expiry_and_submission_claim_race_has_one_owner", Box::pin(execution_submission::expiry_and_submission_claim_race_has_one_owner())).await;
        scenario("execution_submission::report_revoke_atomically_terminates_intent_condition_and_capital", Box::pin(execution_submission::report_revoke_atomically_terminates_intent_condition_and_capital())).await;
        scenario("execution_submission::report_revoke_and_cancel_race_has_one_intent_terminal_audit", Box::pin(execution_submission::report_revoke_and_cancel_race_has_one_intent_terminal_audit())).await;
        scenario("execution_submission::create_entry_locks_capital_and_advances_intent", Box::pin(execution_submission::create_entry_locks_capital_and_advances_intent())).await;
        scenario("execution_submission::supersession_wins_before_submission_and_releases_capital", Box::pin(execution_submission::supersession_wins_before_submission_and_releases_capital())).await;
        scenario("execution_submission::submitted_order_survives_later_supersession", Box::pin(execution_submission::submitted_order_survives_later_supersession())).await;
        scenario("execution_submission::prepared_report_is_not_actionable_before_fact_verification", Box::pin(execution_submission::prepared_report_is_not_actionable_before_fact_verification())).await;
        scenario("execution_submission::verified_publication_atomically_supersedes_prior_current", Box::pin(execution_submission::verified_publication_atomically_supersedes_prior_current())).await;
        scenario("execution_submission::fact_failure_leaves_existing_current_untouched", Box::pin(execution_submission::fact_failure_leaves_existing_current_untouched())).await;
        scenario("execution_submission::concurrent_publications_leave_one_current_per_scope", Box::pin(execution_submission::concurrent_publications_leave_one_current_per_scope())).await;
        scenario("execution_submission::out_of_order_verification_obsoletes_older_candidate", Box::pin(execution_submission::out_of_order_verification_obsoletes_older_candidate())).await;
        scenario("execution_submission::cancelled_delivery_settlement_returns_claim_lost", Box::pin(execution_submission::cancelled_delivery_settlement_returns_claim_lost())).await;
        scenario("execution_submission::empty_report_is_published_and_becomes_current", Box::pin(execution_submission::empty_report_is_published_and_becomes_current())).await;
        scenario("execution_submission::lost_lease_prevents_report_commit_and_marks_abandoned", Box::pin(execution_submission::lost_lease_prevents_report_commit_and_marks_abandoned())).await;
        scenario("execution_submission::stale_parity_generation_blocks_entry_write_ahead", Box::pin(execution_submission::stale_parity_generation_blocks_entry_write_ahead())).await;
        scenario("execution_submission::create_entry_advances_recommendation_to_executed", Box::pin(execution_submission::create_entry_advances_recommendation_to_executed())).await;
        scenario("execution_submission::reject_admission_releases_capital_and_marks_rejected", Box::pin(execution_submission::reject_admission_releases_capital_and_marks_rejected())).await;
        scenario("execution_submission::revert_claim_restores_approved_by_policy_for_auto_intent", Box::pin(execution_submission::revert_claim_restores_approved_by_policy_for_auto_intent())).await;
        scenario("execution_submission::partial_fill_splits_capital_while_locked", Box::pin(execution_submission::partial_fill_splits_capital_while_locked())).await;
        scenario("execution_submission::position_upsert_weighted_average_cost", Box::pin(execution_submission::position_upsert_weighted_average_cost())).await;
        scenario("execution_submission::full_fill_spends_capital_and_writes_position", Box::pin(execution_submission::full_fill_spends_capital_and_writes_position())).await;
        scenario("execution_submission::ambiguous_holds_capital_and_enqueues_reconciliation", Box::pin(execution_submission::ambiguous_holds_capital_and_enqueues_reconciliation())).await;
        scenario("execution_submission::rejected_releases_capital_without_position", Box::pin(execution_submission::rejected_releases_capital_without_position())).await;
        scenario("execution_submission::recover_dangling_returns_in_flight_orders", Box::pin(execution_submission::recover_dangling_returns_in_flight_orders())).await;
        scenario("execution_submission::create_advances_recommendation_to_intent_created", Box::pin(execution_submission::create_advances_recommendation_to_intent_created())).await;
        scenario("execution_submission::create_rejects_when_recommendation_executed", Box::pin(execution_submission::create_rejects_when_recommendation_executed())).await;
        scenario("execution_submission::create_rejects_when_submitted_intent_blocks", Box::pin(execution_submission::create_rejects_when_submitted_intent_blocks())).await;
        scenario("execution_submission::reconcile_ambiguous_to_filled_spends_capital_and_writes_position", Box::pin(execution_submission::reconcile_ambiguous_to_filled_spends_capital_and_writes_position())).await;
        scenario("execution_submission::reconcile_ambiguous_to_not_filled_releases_capital", Box::pin(execution_submission::reconcile_ambiguous_to_not_filled_releases_capital())).await;
        scenario("execution_submission::reconcile_unresolvable_impairs_capital_and_leaves_order_ambiguous", Box::pin(execution_submission::reconcile_unresolvable_impairs_capital_and_leaves_order_ambiguous())).await;
        scenario("execution_submission::reconcile_partial_fill_splits_capital_and_writes_position", Box::pin(execution_submission::reconcile_partial_fill_splits_capital_and_writes_position())).await;
        scenario("execution_submission::reconcile_correction_is_idempotent", Box::pin(execution_submission::reconcile_correction_is_idempotent())).await;
        scenario("execution_submission::operator_resolve_impaired_to_filled_spends_capital", Box::pin(execution_submission::operator_resolve_impaired_to_filled_spends_capital())).await;
        scenario("execution_submission::entry_fill_freezes_scale_out_denominator", Box::pin(execution_submission::entry_fill_freezes_scale_out_denominator())).await;
        scenario("execution_submission::exit_full_releases_capital_with_realized_pnl", Box::pin(execution_submission::exit_full_releases_capital_with_realized_pnl())).await;
        scenario("execution_submission::exit_partial_keeps_capital_spent_and_reduces_lot", Box::pin(execution_submission::exit_partial_keeps_capital_spent_and_reduces_lot())).await;
        scenario("execution_submission::exit_rejects_second_in_flight_order", Box::pin(execution_submission::exit_rejects_second_in_flight_order())).await;
        scenario("report_scheduler::two_coordinators_claim_one_global_run", Box::pin(report_scheduler::two_coordinators_claim_one_global_run())).await;
        scenario("report_scheduler::restart_coalesces_latest_and_records_aggregate_gap", Box::pin(report_scheduler::restart_coalesces_latest_and_records_aggregate_gap())).await;
        scenario("report_scheduler::config_change_skips_old_queued_occurrence", Box::pin(report_scheduler::config_change_skips_old_queued_occurrence())).await;
        scenario("access_control::user_crud_paging_and_delete", Box::pin(access_control::user_crud_paging_and_delete())).await;
        scenario("access_control::role_crud_and_builtin_protection", Box::pin(access_control::role_crud_and_builtin_protection())).await;
        scenario("access_control::menu_tree_accessibility_and_delete_guard", Box::pin(access_control::menu_tree_accessibility_and_delete_guard())).await;
        scenario("access_control::assign_roles_replaces_join_and_casbin_grouping", Box::pin(access_control::assign_roles_replaces_join_and_casbin_grouping())).await;
        scenario("access_control::assign_permissions_validates_and_round_trips", Box::pin(access_control::assign_permissions_validates_and_round_trips())).await;
        scenario("access_control::set_permissions_for_unknown_role_is_not_found", Box::pin(access_control::set_permissions_for_unknown_role_is_not_found())).await;
        scenario("access_control::casbin_adapter_matches_full_tuple", Box::pin(access_control::casbin_adapter_matches_full_tuple())).await;
        scenario("access_control::enforce_reflects_assignments_and_super_admin_bypass", Box::pin(access_control::enforce_reflects_assignments_and_super_admin_bypass())).await;
        scenario("access_control::role_disable_revokes_then_enable_rebuilds_grouping", Box::pin(access_control::role_disable_revokes_then_enable_rebuilds_grouping())).await;
        scenario("access_control::assigning_a_disabled_role_writes_no_grouping", Box::pin(access_control::assigning_a_disabled_role_writes_no_grouping())).await;
        scenario("access_control::operation_log_appends_and_pages_and_is_worm", Box::pin(access_control::operation_log_appends_and_pages_and_is_worm())).await;
        scenario("model_governance::quant_shadow_comparison_migration_and_crud", Box::pin(model_governance::quant_shadow_comparison_migration_and_crud())).await;
        scenario("model_governance::quant_model_governance_audit_migration_and_crud", Box::pin(model_governance::quant_model_governance_audit_migration_and_crud())).await;
        scenario("policy_governance::active_resources_are_loaded_in_one_typed_set_and_approvals_are_single_use", Box::pin(policy_governance::active_resources_are_loaded_in_one_typed_set_and_approvals_are_single_use())).await;
        scenario("policy_governance::outbox_failure_rolls_back_activation_snapshot_guard_and_approval_consumption", Box::pin(policy_governance::outbox_failure_rolls_back_activation_snapshot_guard_and_approval_consumption())).await;
        scenario("policy_governance::rollback_records_a_new_generation_when_content_hash_matches_history", Box::pin(policy_governance::rollback_records_a_new_generation_when_content_hash_matches_history())).await;
        scenario("policy_governance::concurrent_resource_activations_fail_stale_then_rebase_without_lost_updates", Box::pin(policy_governance::concurrent_resource_activations_fail_stale_then_rebase_without_lost_updates())).await;
        scenario("typed_persistence::typed_jsonb_rejects_postgres_corruption_without_fallback", Box::pin(typed_persistence::typed_jsonb_rejects_postgres_corruption_without_fallback())).await;
        scenario("typed_persistence::semantic_text_revalidates_postgres_decode_and_database_checks", Box::pin(typed_persistence::semantic_text_revalidates_postgres_decode_and_database_checks())).await;
        scenario("backtest_path_set::quant_backtest_path_set_migration_and_crud", Box::pin(backtest_path_set::quant_backtest_path_set_migration_and_crud())).await;
        scenario("backtest_report::quant_backtest_report_migration_and_crud", Box::pin(backtest_report::quant_backtest_report_migration_and_crud())).await;
        scenario("calibration_artifact::create_duplicate_content_hash_maps_to_storage_duplicate", Box::pin(calibration_artifact::create_duplicate_content_hash_maps_to_storage_duplicate())).await;
        scenario("calibration_artifact::mark_active_missing_artifact_is_not_found", Box::pin(calibration_artifact::mark_active_missing_artifact_is_not_found())).await;
        scenario("calibration_artifact::activate_market_price_bias_deactivates_previous_active", Box::pin(calibration_artifact::activate_market_price_bias_deactivates_previous_active())).await;
        scenario("calibration_artifact::activate_model_score_does_not_deactivate_other_model_score_artifacts", Box::pin(calibration_artifact::activate_model_score_does_not_deactivate_other_model_score_artifacts())).await;
        scenario("calibration_artifact::activate_model_score_does_not_deactivate_active_market_price_bias", Box::pin(calibration_artifact::activate_model_score_does_not_deactivate_active_market_price_bias())).await;
        scenario("comparison_report::quant_model_comparison_report_migration_and_crud", Box::pin(comparison_report::quant_model_comparison_report_migration_and_crud())).await;
        scenario("factor_revision::registration_is_insert_only_and_publication_retires_prior_revision", Box::pin(factor_revision::registration_is_insert_only_and_publication_retires_prior_revision())).await;
        scenario("factor_revision::batch_publication_is_atomic_and_rejects_content_address_collisions", Box::pin(factor_revision::batch_publication_is_atomic_and_rejects_content_address_collisions())).await;
        scenario("feature_parity::cold_window_is_not_eligible_and_writes_no_run_or_job", Box::pin(feature_parity::cold_window_is_not_eligible_and_writes_no_run_or_job())).await;
        scenario("feature_parity::full_window_is_unique_only_while_a_run_is_active", Box::pin(feature_parity::full_window_is_unique_only_while_a_run_is_active())).await;
        scenario("feature_parity::recovery_must_cover_every_open_incident_since_latest_clear", Box::pin(feature_parity::recovery_must_cover_every_open_incident_since_latest_clear())).await;
        scenario("model_registry::create_model_spec_duplicate_name_maps_to_storage_duplicate", Box::pin(model_registry::create_model_spec_duplicate_name_maps_to_storage_duplicate())).await;
        scenario("model_registry::model_spec_rejects_forged_hash_and_is_append_only", Box::pin(model_registry::model_spec_rejects_forged_hash_and_is_append_only())).await;
        scenario("model_registry::create_model_version_allocates_monotonic_versions_under_lock", Box::pin(model_registry::create_model_version_allocates_monotonic_versions_under_lock())).await;
        scenario("model_registry::find_and_page_versions_join_model_family_from_spec", Box::pin(model_registry::find_and_page_versions_join_model_family_from_spec())).await;
        scenario("model_registry::model_version_typed_documents_fail_closed_at_database_boundary", Box::pin(model_registry::model_version_typed_documents_fail_closed_at_database_boundary())).await;
        scenario("model_registry::published_artifacts_coexist_until_model_routing_moves_and_retirement_is_explicit", Box::pin(model_registry::published_artifacts_coexist_until_model_routing_moves_and_retirement_is_explicit())).await;
        scenario("model_registry::published_picker_catalog_is_one_typed_join_with_side_and_scope_filters", Box::pin(model_registry::published_picker_catalog_is_one_typed_join_with_side_and_scope_filters())).await;
        scenario("research_job::job_kind_must_match_tagged_params", Box::pin(research_job::job_kind_must_match_tagged_params())).await;
        scenario("research_job::finalize_requires_running_lease_owner", Box::pin(research_job::finalize_requires_running_lease_owner())).await;
        scenario("research_job::stale_owner_finalize_is_rejected_after_reclaim", Box::pin(research_job::stale_owner_finalize_is_rejected_after_reclaim())).await;
        scenario("research_job::requeue_inflight_requeues_own_running_row_and_bumps_recovery", Box::pin(research_job::requeue_inflight_requeues_own_running_row_and_bumps_recovery())).await;
        scenario("research_job::requeue_inflight_ignores_other_owners_running_rows", Box::pin(research_job::requeue_inflight_ignores_other_owners_running_rows())).await;
        scenario("research_job::requeue_inflight_quarantines_at_recovery_cap", Box::pin(research_job::requeue_inflight_quarantines_at_recovery_cap())).await;
        scenario("research_job::double_finalize_returns_state_conflict", Box::pin(research_job::double_finalize_returns_state_conflict())).await;
        scenario("research_readiness::readiness_evidence_is_scoped_expiring_idempotent_and_append_only", Box::pin(research_readiness::readiness_evidence_is_scoped_expiring_idempotent_and_append_only())).await;
        scenario("research_readiness::readiness_evidence_rejects_payload_hash_or_kind_tampering", Box::pin(research_readiness::readiness_evidence_rejects_payload_hash_or_kind_tampering())).await;
        scenario("research_readiness::shadow_latency_observation_returns_missing_dimensions_without_fallbacks", Box::pin(research_readiness::shadow_latency_observation_returns_missing_dimensions_without_fallbacks())).await;
        scenario("trade_policy_trial::trial_ledger_is_ordered_idempotent_cutoff_bound_and_append_only", Box::pin(trade_policy_trial::trial_ledger_is_ordered_idempotent_cutoff_bound_and_append_only())).await;
        scenario("trade_policy_trial::trial_ledger_rejects_row_hash_or_terminal_shape_tampering", Box::pin(trade_policy_trial::trial_ledger_rejects_row_hash_or_terminal_shape_tampering())).await;
        scenario("training_dataset::quant_training_dataset_migration_and_crud", Box::pin(training_dataset::quant_training_dataset_migration_and_crud())).await;
        scenario("training_dataset::training_dataset_plan_rejects_model_spec_definition_drift", Box::pin(training_dataset::training_dataset_plan_rejects_model_spec_definition_drift())).await;
        scenario("training_dataset::training_dataset_status_transitions_enforce_state_machine", Box::pin(training_dataset::training_dataset_status_transitions_enforce_state_machine())).await;
        scenario("training_dataset::model_version_training_dataset_foreign_key", Box::pin(training_dataset::model_version_training_dataset_foreign_key())).await;
    }))
    .await
    .expect("start shared repository PostgreSQL suite");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn execution_identity_refs_are_atomic_unique_and_concurrent() {
    Box::pin(with_postgres_suite(
        execution_submission::execution_identity_refs_are_atomic_unique_and_concurrent(),
    ))
    .await
    .expect("start execution identity PostgreSQL suite");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recommendation_resolution_outcome_repository_contracts() {
    Box::pin(with_postgres_suite(async {
        scenario(
            "recommendation_resolution_outcome::reconcile_fact_is_idempotent_and_conflicting_content_fails_closed",
            Box::pin(
                recommendation_resolution_outcome::reconcile_fact_is_idempotent_and_conflicting_content_fails_closed(),
            ),
        )
        .await;
        scenario(
            "recommendation_resolution_outcome::database_owned_availability_and_tampering_are_enforced",
            Box::pin(
                recommendation_resolution_outcome::database_owned_availability_and_tampering_are_enforced(),
            ),
        )
        .await;
        scenario(
            "recommendation_resolution_outcome::available_at_keyset_is_total_ordered_and_cutoff_bound",
            Box::pin(
                recommendation_resolution_outcome::available_at_keyset_is_total_ordered_and_cutoff_bound(),
            ),
        )
        .await;
        scenario(
            "recommendation_resolution_outcome::reconciliation_candidates_are_terminal_keyset_and_outcome_aware",
            Box::pin(
                recommendation_resolution_outcome::reconciliation_candidates_are_terminal_keyset_and_outcome_aware(),
            ),
        )
        .await;
    }))
    .await
    .expect("start recommendation-resolution outcome PostgreSQL suite");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recommendation_execution_outcome_repository_contracts() {
    Box::pin(with_postgres_suite(async {
        scenario(
            "recommendation_execution_outcome::terminal_shapes_preserve_real_zero_and_null_semantics",
            Box::pin(
                recommendation_execution_outcome::terminal_shapes_preserve_real_zero_and_null_semantics(),
            ),
        )
        .await;
        scenario(
            "recommendation_execution_outcome::invalid_state_and_report_only_fail_closed",
            Box::pin(
                recommendation_execution_outcome::invalid_state_and_report_only_fail_closed(),
            ),
        )
        .await;
        scenario(
            "recommendation_execution_outcome::reconcile_is_idempotent_worm_and_tamper_evident",
            Box::pin(
                recommendation_execution_outcome::reconcile_is_idempotent_worm_and_tamper_evident(),
            ),
        )
        .await;
        scenario(
            "recommendation_execution_outcome::reconciliation_candidates_require_submitted_terminal_source",
            Box::pin(
                recommendation_execution_outcome::reconciliation_candidates_require_submitted_terminal_source(),
            ),
        )
        .await;
    }))
    .await
    .expect("start recommendation-execution outcome PostgreSQL suite");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn feedback_cohort_repository_contracts() {
    Box::pin(with_postgres_suite(async {
        scenario(
            "feedback_cohort::candidate_page_is_keyset_ordered_and_cutoff_frozen",
            Box::pin(feedback_cohort::candidate_page_is_keyset_ordered_and_cutoff_frozen()),
        )
        .await;
        scenario(
            "feedback_cohort::cohort_truth_planes_are_orthogonal_and_submission_exact",
            Box::pin(feedback_cohort::cohort_truth_planes_are_orthogonal_and_submission_exact()),
        )
        .await;
        scenario(
            "feedback_cohort::cutoff_excludes_late_truth_and_late_candidates_across_pages",
            Box::pin(
                feedback_cohort::cutoff_excludes_late_truth_and_late_candidates_across_pages(),
            ),
        )
        .await;
        scenario(
            "feedback_cohort::keyset_reads_more_than_ten_thousand_without_duplicates",
            Box::pin(feedback_cohort::keyset_reads_more_than_ten_thousand_without_duplicates()),
        )
        .await;
    }))
    .await
    .expect("start feedback-cohort PostgreSQL suite");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn domain_source_cursor_repository_contracts() {
    Box::pin(with_postgres_suite(async {
        scenario(
            "domain_source_cursor::compare_and_set_validates_hash_and_has_one_concurrent_winner",
            Box::pin(
                domain_source_cursor::compare_and_set_validates_hash_and_has_one_concurrent_winner(
                ),
            ),
        )
        .await;
    }))
    .await
    .expect("start domain-source cursor PostgreSQL suite");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runtime_control_singleton_cas_is_atomic_audited_and_fail_closed() {
    Box::pin(with_postgres_suite(
        runtime_control::singleton_cas_is_atomic_audited_and_fail_closed(),
    ))
    .await
    .expect("start runtime-control PostgreSQL suite");
}

async fn scenario(name: &str, future: Pin<Box<dyn Future<Output = ()>>>) {
    eprintln!("repository scenario started: {name}");
    future.await;
    eprintln!("repository scenario passed: {name}");
}
