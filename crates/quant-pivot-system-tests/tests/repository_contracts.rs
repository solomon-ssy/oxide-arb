//! Repository persistence contracts against one disposable `PostgreSQL` server.

use std::{future::Future, pin::Pin};

use quant_pivot_system_tests::postgres::with_postgres_suite;

macro_rules! run_scenarios {
    ($($scenario:path),+ $(,)?) => {
        $(
            scenario(stringify!($scenario), Box::pin($scenario())).await;
        )+
    };
}

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
async fn repository_persistence_contracts_stack() {
    Box::pin(with_postgres_suite(async {
        run_scenarios!(
            account_capital::account_snapshot_repo_find,
            account_capital::reserved_returns_zero_empty,
            account_capital::report_transaction_persists_intents,
            account_capital::find_returns_before_only,
            account_capital::report_recovers_without_claim,
            account_capital::execution_order_reconciliation_trip,
            account_capital::capital_kill_switch_trip,
            equity_snapshot::equity_snapshot_repo_hwm,
            equity_snapshot::high_water_mark_max,
            equity_snapshot::drawdown_pct_hwm_hwm,
            equity_snapshot::realized_pnl_matches_sum,
            portfolio_optimizer::optimizer_meta_persisted_row,
            catalog_ledger::correction_invisible_until_time,
            catalog_ledger::batch_snapshot_observes_membership,
            catalog_ledger::batch_rejects_before_coverage,
            catalog_ledger::concurrent_reads_never_commit,
            catalog_ledger::failed_never_creates_coverage,
            catalog_ledger::identical_reconcile_only_audit,
            catalog_ledger::projection_upsert_updates_status,
            catalog_ledger::object_rejected_before_commit,
            domain_projection::crypto_source_sequence_bigint,
            domain_projection::crypto_rejected_before_write,
            domain_source_expectation::source_exists_before_optimistically,
            domain_source_expectation::natural_key_updates_expectation,
            market_linkage::valid_never_after_cutoff,
            market_linkage::valid_markets_matches_batched,
            market_linkage::backdated_row_before_availability,
            market_linkage::append_batch_rolls_invalid,
            market_page::market_page_filters_category,
            market_selection::create_snapshot_find_members,
            weather_daily_temperature::weather_projection_tracks_events,
            basis_alert::record_persists_round_trips,
            basis_alert::latest_market_picks_newest,
            basis_alert::batched_latest_returns_market,
            basis_alert::page_filters_market_range,
            basis_alert::acknowledge_marks_alert_idempotent,
            basis_alert::acknowledge_missing_alert_rejects,
            entry_condition_evaluation::semantic_revision_atomic_deduplicated,
            execution_submission::claim_guards_against_submit,
            execution_submission::entry_condition_artifact_worm,
            execution_submission::concurrent_approval_one_truth,
            execution_submission::expiry_atomic_idempotent_audit,
            execution_submission::expiry_cancel_race_owner,
            execution_submission::expiry_submission_claim_owner,
            execution_submission::report_revoke_atomically_capital,
            execution_submission::report_revoke_cancel_audit,
            execution_submission::create_entry_advances_intent,
            execution_submission::supersession_wins_before_capital,
            execution_submission::submitted_order_survives_supersession,
            execution_submission::report_not_before_verification,
            execution_submission::verified_publication_atomically_current,
            execution_submission::fact_failure_leaves_untouched,
            execution_submission::concurrent_publications_leave_scope,
            execution_submission::out_order_verification_candidate,
            execution_submission::cancelled_delivery_returns_lost,
            execution_submission::empty_report_published_current,
            execution_submission::lost_lease_prevents_abandoned,
            execution_submission::stale_parity_blocks_ahead,
            execution_submission::create_entry_advances_executed,
            execution_submission::reject_admission_releases_rejected,
            execution_submission::revert_claim_restores_intent,
            execution_submission::partial_fill_splits_locked,
            execution_submission::position_upsert_weighted_cost,
            execution_submission::full_fill_writes_position,
            execution_submission::ambiguous_holds_capital_reconciliation,
            execution_submission::rejected_releases_without_position,
            execution_submission::recover_dangling_returns_orders,
            execution_submission::create_advances_recommendation_created,
            execution_submission::create_rejects_recommendation_executed,
            execution_submission::create_rejects_submitted_blocks,
            execution_submission::reconcile_ambiguous_writes_position,
            execution_submission::reconcile_ambiguous_not_capital,
            execution_submission::reconcile_unresolvable_impairs_ambiguous,
            execution_submission::reconcile_partial_writes_position,
            execution_submission::reconcile_correction_is_idempotent,
            execution_submission::operator_resolve_impaired_capital,
            execution_submission::entry_fill_freezes_denominator,
            execution_submission::exit_full_releases_pnl,
            execution_submission::exit_partial_keeps_lot,
            execution_submission::exit_rejects_second_order,
            report_scheduler::two_coordinators_claim_run,
            report_scheduler::restart_coalesces_latest_gap,
            report_scheduler::config_change_skips_occurrence,
            access_control::user_crud_paging_delete,
            access_control::role_crud_builtin_protection,
            access_control::menu_tree_accessibility_guard,
            access_control::assign_roles_replaces_grouping,
            access_control::assign_permissions_validates_trips,
            access_control::set_unknown_not_found,
            access_control::casbin_adapter_matches_tuple,
            access_control::enforce_reflects_assignments_bypass,
            access_control::role_disable_revokes_grouping,
            access_control::assigning_writes_no_grouping,
            access_control::operation_log_appends_worm,
            model_governance::quant_shadow_comparison_crud,
            model_governance::quant_model_governance_crud,
            policy_governance::active_resources_loaded_use,
            policy_governance::outbox_failure_rolls_consumption,
            policy_governance::rollback_generation_matches_history,
            policy_governance::concurrent_rebase_preserves_updates,
            typed_persistence::typed_rejects_without_fallback,
            typed_persistence::semantic_text_revalidates_checks,
            backtest_path_set::quant_backtest_set_crud,
            backtest_report::quant_backtest_report_crud,
            calibration_artifact::create_duplicate_maps_duplicate,
            calibration_artifact::mark_missing_not_found,
            calibration_artifact::activate_market_price_active,
            calibration_artifact::model_activation_isolated,
            calibration_artifact::bias_activation_isolated,
            comparison_report::quant_model_comparison_crud,
            factor_revision::registration_insert_only_revision,
            factor_revision::batch_atomic_rejects_collisions,
            feature_parity::cold_not_no_job,
            feature_parity::full_window_unique_active,
            feature_parity::recovery_cover_open_clear,
            model_registry::create_model_duplicate_duplicate,
            model_registry::model_spec_rejects_only,
            model_registry::create_model_version_lock,
            model_registry::find_page_versions_spec,
            model_registry::model_version_rejects_boundary,
            model_registry::published_artifacts_coexist_explicit,
            model_registry::published_picker_catalog_filters,
            research_job::job_kind_match_params,
            research_job::finalize_requires_running_owner,
            research_job::stale_rejected_after_reclaim,
            research_job::requeue_inflight_requeues_recovery,
            research_job::requeue_inflight_ignores_rows,
            research_job::requeue_inflight_quarantines_cap,
            research_job::double_finalize_returns_conflict,
            research_readiness::readiness_evidence_scoped_only,
            research_readiness::readiness_evidence_rejects_tampering,
            research_readiness::shadow_missing_without_fallbacks,
            trade_policy_trial::trial_ledger_ordered_only,
            trade_policy_trial::trial_ledger_rejects_tampering,
            training_dataset::quant_training_dataset_crud,
            training_dataset::training_dataset_rejects_drift,
            training_dataset::training_dataset_status_machine,
            training_dataset::model_version_training_key,
        );
    }))
    .await
    .expect("start shared repository PostgreSQL suite");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn execution_atomic_unique_concurrent() {
    Box::pin(with_postgres_suite(
        execution_submission::execution_atomic_unique_concurrent(),
    ))
    .await
    .expect("start execution identity PostgreSQL suite");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recommendation_resolution_outcome_contracts() {
    Box::pin(with_postgres_suite(async {
        run_scenarios!(
            recommendation_resolution_outcome::reconcile_fact_idempotent_rejects,
            recommendation_resolution_outcome::database_owned_availability_enforced,
            recommendation_resolution_outcome::keyset_total_ordered_bound,
            recommendation_resolution_outcome::reconciliation_candidates_terminal_aware,
        );
    }))
    .await
    .expect("start recommendation-resolution outcome PostgreSQL suite");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recommendation_execution_outcome_contracts() {
    Box::pin(with_postgres_suite(async {
        run_scenarios!(
            recommendation_execution_outcome::terminal_preserve_zero_semantics,
            recommendation_execution_outcome::invalid_state_report_rejects,
            recommendation_execution_outcome::reconcile_idempotent_worm_evident,
            recommendation_execution_outcome::reconciliation_candidates_require_source,
        );
    }))
    .await
    .expect("start recommendation-execution outcome PostgreSQL suite");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn feedback_cohort_repository_contracts() {
    Box::pin(with_postgres_suite(async {
        run_scenarios!(
            feedback_cohort::candidate_page_keyset_frozen,
            feedback_cohort::cohort_truth_planes_exact,
            feedback_cohort::cutoff_excludes_late_pages,
            feedback_cohort::keyset_reads_without_duplicates,
        );
    }))
    .await
    .expect("start feedback-cohort PostgreSQL suite");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn domain_source_cursor_contracts() {
    Box::pin(with_postgres_suite(async {
        run_scenarios!(domain_source_cursor::compare_validates_concurrent_winner,);
    }))
    .await
    .expect("start domain-source cursor PostgreSQL suite");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runtime_control_atomic_rejects() {
    Box::pin(with_postgres_suite(
        runtime_control::singleton_cas_atomic_rejects(),
    ))
    .await
    .expect("start runtime-control PostgreSQL suite");
}

async fn scenario(name: &str, future: Pin<Box<dyn Future<Output = ()>>>) {
    eprintln!("repository scenario started: {name}");
    future.await;
    eprintln!("repository scenario passed: {name}");
}
