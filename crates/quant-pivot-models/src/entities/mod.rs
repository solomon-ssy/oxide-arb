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
#[cfg(feature = "repository")]
pub mod clob_market_info_version;
pub mod decision_policy_snapshot;
pub mod event;
pub mod market;
pub mod menu;
pub mod operation_log;
pub mod policy_activation;
pub mod policy_activation_guard;
pub mod policy_approval;
pub mod policy_revision;
pub mod quant_account_snapshot;
pub mod quant_backtest_path_set;
pub mod quant_backtest_report;
pub mod quant_basis_alert;
pub mod quant_calibration_artifact;
#[cfg(feature = "repository")]
pub mod quant_calibration_artifact_publication;
pub mod quant_capital_allocation;
#[cfg_attr(not(feature = "repository"), allow(dead_code))]
pub mod quant_crypto_price_projection;
#[cfg_attr(not(feature = "repository"), allow(dead_code))]
pub mod quant_domain_event_outbox;
pub mod quant_domain_source_cursor;
pub mod quant_domain_source_expectation;
pub mod quant_entry_condition_artifact;
pub mod quant_entry_condition_audit;
#[cfg_attr(not(feature = "repository"), allow(dead_code))]
pub mod quant_entry_condition_evaluation_outbox;
pub mod quant_entry_condition_instance;
pub mod quant_equity_snapshot;
pub mod quant_execution_order;
pub mod quant_factor_definition;
pub mod quant_factor_value;
pub mod quant_feature_parity_candidate;
pub mod quant_feature_parity_run;
pub mod quant_feature_parity_state;
pub mod quant_feature_parity_subject;
pub mod quant_feature_vector;
pub mod quant_market_linkage;
#[cfg_attr(not(feature = "repository"), allow(dead_code))]
pub mod quant_market_linkage_source;
pub mod quant_market_selection;
pub mod quant_market_selection_member;
pub mod quant_model_comparison_report;
pub mod quant_model_governance_audit;
pub mod quant_model_run;
pub mod quant_model_spec;
pub mod quant_model_version;
pub mod quant_order_intent;
pub mod quant_portfolio_plan;
pub mod quant_position;
pub mod quant_recommendation;
pub mod quant_recommendation_attribution;
pub mod quant_recommendation_report;
pub mod quant_reconciliation;
pub mod quant_report_data_quality_snapshot;
pub mod quant_report_fact_delivery;
pub mod quant_report_run;
pub mod quant_report_schedule_gap;
pub mod quant_report_schedule_state;
pub mod quant_research_job;
pub mod quant_research_readiness_evidence;
pub mod quant_settlement_redeem;
pub mod quant_settlement_redeem_lot;
pub mod quant_shadow_comparison;
pub mod quant_source_slice;
pub mod quant_trade_policy_artifact;
pub mod quant_trade_policy_governance_audit;
pub mod quant_trade_policy_trial_attempt;
pub mod quant_trade_policy_validation;
pub mod quant_trade_policy_validation_row;
pub mod quant_trade_tape_block_cursor;
pub mod quant_training_dataset;
#[cfg_attr(not(feature = "repository"), allow(dead_code))]
pub mod quant_weather_daily_temperature_projection;
#[cfg_attr(not(feature = "repository"), allow(dead_code))]
pub mod quant_weather_observation_current;
pub mod role;
pub mod role_menu;
#[cfg(feature = "repository")]
pub mod seed_application;
pub mod system_bootstrap_transition;
pub mod system_kill_switch;
pub mod system_production_baseline;
pub mod system_runtime_state;
pub mod user;
pub mod user_role;
