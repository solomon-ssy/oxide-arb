//! `SeaORM` entity definitions for the quant-pivot database schema.

// SeaORM 2 generates `ActiveModel` with `PartialEq`; `ActiveValue<T>` cannot
// implement `Eq` because it represents Set/Unchanged/NotSet mutation state.
#![expect(
    clippy::derive_partial_eq_without_eq,
    reason = "generated SeaORM ActiveModel intentionally has PartialEq without Eq"
)]

pub mod casbin_rule;
pub mod catalog_event_change;
pub mod catalog_event_object;
pub mod catalog_market_change;
pub mod catalog_market_object;
pub mod catalog_sync_batch;
pub mod catalog_sync_rejection;
#[cfg(feature = "persistence-entities")]
pub mod clob_market_info_version;
pub mod decision_policy_snapshot;
pub mod event;
pub mod market;
pub mod menu;
pub mod operation_log;
pub mod policy_activation;
pub mod policy_activation_audit;
pub mod policy_activation_event_outbox;
pub mod policy_activation_guard;
pub mod policy_approval;
pub mod policy_profile_artifact;
pub mod policy_revision;
pub mod quant_account_snapshot;
pub mod quant_attribution_artifact;
pub mod quant_backtest_path_set;
pub mod quant_backtest_report;
pub mod quant_basis_alert;
pub mod quant_calibration_artifact;
#[cfg(feature = "persistence-entities")]
pub mod quant_calibration_artifact_publication;
pub mod quant_capital_allocation;
#[cfg_attr(not(feature = "persistence-entities"), allow(dead_code))]
pub mod quant_crypto_price_projection;
#[cfg_attr(not(feature = "persistence-entities"), allow(dead_code))]
pub mod quant_domain_event_outbox;
pub mod quant_domain_source_cursor;
pub mod quant_domain_source_expectation;
pub mod quant_drift_report;
pub mod quant_entry_condition_artifact;
pub mod quant_entry_condition_audit;
#[cfg_attr(not(feature = "persistence-entities"), allow(dead_code))]
pub mod quant_entry_condition_evaluation_outbox;
pub mod quant_entry_condition_instance;
pub mod quant_equity_snapshot;
pub mod quant_exchange_history_chunk;
pub mod quant_exchange_history_plan;
pub mod quant_exchange_history_quarantine;
pub mod quant_exchange_history_quarantine_resolution;
pub mod quant_execution_account;
pub mod quant_execution_attempt_outcome;
pub mod quant_execution_attempt_reconciliation_task;
pub mod quant_execution_fee_measurement;
pub mod quant_execution_fill;
pub mod quant_execution_order;
pub mod quant_execution_rollup_reconciliation_task;
pub mod quant_execution_trade_ref;
pub mod quant_execution_transaction_ref;
pub mod quant_factor_definition;
pub mod quant_factor_value;
pub mod quant_feature_parity_candidate;
pub mod quant_feature_parity_run;
pub mod quant_feature_parity_state;
pub mod quant_feature_parity_subject;
pub mod quant_feature_vector;
pub mod quant_feedback_coordinator_fault;
pub mod quant_feedback_cycle;
pub mod quant_feedback_evaluation_use;
pub mod quant_feedback_event_outbox;
pub mod quant_feedback_promotion_permit;
pub mod quant_feedback_recipe_template;
pub mod quant_feedback_scheduler_state;
pub mod quant_feedback_stage_event;
pub mod quant_feedback_trigger_event;
pub mod quant_fresh_boot_run;
pub mod quant_fresh_boot_run_event;
pub mod quant_history_fit_seal;
pub mod quant_history_fit_seal_chunk;
pub mod quant_history_serving_head_seal;
pub mod quant_history_serving_head_seal_chunk;
pub mod quant_market_linkage;
#[cfg_attr(not(feature = "persistence-entities"), allow(dead_code))]
pub mod quant_market_linkage_source;
pub mod quant_market_selection;
pub mod quant_market_selection_member;
pub mod quant_model_candidate_manifest;
pub mod quant_model_comparison_report;
pub mod quant_model_governance_audit;
pub mod quant_model_route_shadow_binding;
pub mod quant_model_run;
pub mod quant_model_spec;
pub mod quant_model_version;
pub mod quant_order_intent;
pub mod quant_portfolio_plan;
pub mod quant_position;
pub mod quant_recommendation;
pub mod quant_recommendation_execution_rollup;
pub mod quant_recommendation_execution_rollup_attempt;
pub mod quant_recommendation_report;
pub mod quant_recommendation_resolution_outcome;
pub mod quant_reconciliation;
pub mod quant_report_data_quality_snapshot;
pub mod quant_report_fact_delivery;
pub mod quant_report_route_run;
pub mod quant_report_run;
pub mod quant_report_schedule_gap;
pub mod quant_report_schedule_state;
pub mod quant_research_job;
pub mod quant_research_readiness_evidence;
pub mod quant_resolution_observation_inbox;
pub mod quant_resolution_observation_projection;
pub mod quant_resolution_outcome_reconciliation_task;
pub mod quant_resolution_projection_remediation;
pub mod quant_settlement_authorization;
pub mod quant_settlement_chain_submission;
pub mod quant_settlement_external_cursor;
pub mod quant_settlement_governed_action;
pub mod quant_settlement_inventory_lot;
pub mod quant_settlement_redeem;
pub mod quant_settlement_redeem_lot;
pub mod quant_shadow_comparison;
pub mod quant_source_slice;
pub mod quant_trade_policy_artifact;
pub mod quant_trade_policy_governance_audit;
pub mod quant_trade_policy_trial_attempt;
pub mod quant_trade_policy_validation;
pub mod quant_trade_policy_validation_row;
pub mod quant_training_dataset;
pub mod quant_venue_incentive_event;
pub mod quant_venue_incentive_reconciliation_scan;
#[cfg_attr(not(feature = "persistence-entities"), allow(dead_code))]
pub mod quant_weather_daily_temperature_projection;
#[cfg_attr(not(feature = "persistence-entities"), allow(dead_code))]
pub mod quant_weather_observation_current;
pub mod research_profile_artifact;
pub mod role;
pub mod role_menu;
#[cfg(feature = "persistence-entities")]
pub mod seed_application;
pub mod system_runtime_control;
pub mod system_runtime_control_transition;
pub mod user;
pub mod user_role;
