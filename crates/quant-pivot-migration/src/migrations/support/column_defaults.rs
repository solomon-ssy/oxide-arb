//! Database defaults retained by the clean boot schema.
//!
//! `SeaORM`'s entity-first registry intentionally models columns and relations,
//! while `PostgreSQL`-specific default expressions are applied here with
//! `SeaQuery`. Statements are batched per table; only native casts and the
//! `PostgreSQL` statement clock use audited literal expressions.

use std::collections::BTreeMap;

use sea_orm::sea_query::{Alias, ColumnDef, Expr, SimpleExpr, Table};
use sea_orm_migration::prelude::{DbErr, SchemaManager};

pub(in crate::migrations) const SOURCE: &[u8] = include_bytes!("column_defaults.rs");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DefaultValue {
    StatementTimestamp,
    CurrentTimestamp,
    Boolean(bool),
    Integer(i64),
    Text(&'static str),
    PostgresLiteral(&'static str),
}

impl DefaultValue {
    fn expression(self) -> SimpleExpr {
        match self {
            Self::StatementTimestamp => Expr::cust("statement_timestamp()"),
            Self::CurrentTimestamp => Expr::current_timestamp(),
            Self::Boolean(value) => Expr::value(value),
            Self::Integer(value) => Expr::value(value),
            Self::Text(value) => Expr::value(value),
            Self::PostgresLiteral(value) => Expr::cust(value),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ColumnDefaultSpec {
    table: &'static str,
    column: &'static str,
    value: DefaultValue,
}

const COLUMN_DEFAULTS: &[ColumnDefaultSpec] = &[
    ColumnDefaultSpec {
        table: "casbin_rule",
        column: "v0",
        value: DefaultValue::Text(""),
    },
    ColumnDefaultSpec {
        table: "casbin_rule",
        column: "v1",
        value: DefaultValue::Text(""),
    },
    ColumnDefaultSpec {
        table: "casbin_rule",
        column: "v2",
        value: DefaultValue::Text(""),
    },
    ColumnDefaultSpec {
        table: "casbin_rule",
        column: "v3",
        value: DefaultValue::Text(""),
    },
    ColumnDefaultSpec {
        table: "casbin_rule",
        column: "v4",
        value: DefaultValue::Text(""),
    },
    ColumnDefaultSpec {
        table: "casbin_rule",
        column: "v5",
        value: DefaultValue::Text(""),
    },
    ColumnDefaultSpec {
        table: "catalog_event_change",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "catalog_event_object",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "catalog_market_change",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "catalog_market_object",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "catalog_sync_batch",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "catalog_sync_batch",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "catalog_sync_rejection",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "clob_market_info_version",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "event",
        column: "status",
        value: DefaultValue::PostgresLiteral("'active'::qp_event_status"),
    },
    ColumnDefaultSpec {
        table: "event",
        column: "tags",
        value: DefaultValue::PostgresLiteral("'{}'::text[]"),
    },
    ColumnDefaultSpec {
        table: "event",
        column: "neg_risk",
        value: DefaultValue::Boolean(false),
    },
    ColumnDefaultSpec {
        table: "event",
        column: "catalog_market_ids",
        value: DefaultValue::PostgresLiteral("'{}'::text[]"),
    },
    ColumnDefaultSpec {
        table: "event",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "event",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "market",
        column: "categories",
        value: DefaultValue::PostgresLiteral("'{}'::qp_market_category[]"),
    },
    ColumnDefaultSpec {
        table: "market",
        column: "status",
        value: DefaultValue::PostgresLiteral("'active'::qp_market_status"),
    },
    ColumnDefaultSpec {
        table: "market",
        column: "filter_reasons",
        value: DefaultValue::PostgresLiteral("'{}'::qp_catalog_filter_reason[]"),
    },
    ColumnDefaultSpec {
        table: "market",
        column: "tick_size",
        value: DefaultValue::PostgresLiteral("'0.01'::qp_tick_size"),
    },
    ColumnDefaultSpec {
        table: "market",
        column: "neg_risk",
        value: DefaultValue::Boolean(false),
    },
    ColumnDefaultSpec {
        table: "market",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "market",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "menu",
        column: "sort",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "menu",
        column: "keep_alive",
        value: DefaultValue::Boolean(false),
    },
    ColumnDefaultSpec {
        table: "menu",
        column: "hide_in_menu",
        value: DefaultValue::Boolean(false),
    },
    ColumnDefaultSpec {
        table: "menu",
        column: "affix_tab",
        value: DefaultValue::Boolean(false),
    },
    ColumnDefaultSpec {
        table: "menu",
        column: "status",
        value: DefaultValue::PostgresLiteral("'enabled'::qp_role_status"),
    },
    ColumnDefaultSpec {
        table: "menu",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "menu",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "operation_log",
        column: "occurred_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "operation_log",
        column: "detail",
        value: DefaultValue::PostgresLiteral("'{}'::jsonb"),
    },
    ColumnDefaultSpec {
        table: "quant_account_snapshot",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_attribution_artifact",
        column: "available_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_attribution_artifact",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_model_candidate_manifest",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_backtest_path_set",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_backtest_report",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_basis_alert",
        column: "acknowledged",
        value: DefaultValue::Boolean(false),
    },
    ColumnDefaultSpec {
        table: "quant_basis_alert",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_calibration_artifact",
        column: "active",
        value: DefaultValue::Boolean(false),
    },
    ColumnDefaultSpec {
        table: "quant_calibration_artifact",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_calibration_artifact_publication",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_capital_allocation",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_capital_allocation",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_crypto_price_projection",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_domain_event_outbox",
        column: "publish_attempts",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "quant_domain_event_outbox",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_domain_event_outbox",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_domain_source_cursor",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_domain_source_cursor",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_domain_source_expectation",
        column: "created_at",
        value: DefaultValue::CurrentTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_domain_source_expectation",
        column: "updated_at",
        value: DefaultValue::CurrentTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_entry_condition_artifact",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_entry_condition_audit",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_entry_condition_evaluation_outbox",
        column: "publish_attempts",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "quant_entry_condition_evaluation_outbox",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_entry_condition_evaluation_outbox",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_entry_condition_instance",
        column: "revision",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "quant_entry_condition_instance",
        column: "fold_state_json",
        value: DefaultValue::PostgresLiteral("'{\"crypto\": []}'::jsonb"),
    },
    ColumnDefaultSpec {
        table: "quant_entry_condition_instance",
        column: "lease_epoch",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "quant_entry_condition_instance",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_entry_condition_instance",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_equity_snapshot",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_execution_order",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_execution_order",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_execution_trade_ref",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_execution_trade_ref",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_execution_transaction_ref",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_factor_definition",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_factor_value",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_feedback_cycle",
        column: "status",
        value: DefaultValue::PostgresLiteral("'queued'::qp_feedback_cycle_status"),
    },
    ColumnDefaultSpec {
        table: "quant_feedback_cycle",
        column: "generation",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "quant_feedback_cycle",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_feedback_cycle",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_feedback_coordinator_fault",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_feedback_scheduler_state",
        column: "attempt",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "quant_feedback_scheduler_state",
        column: "coalesced_gap_count",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "quant_feedback_scheduler_state",
        column: "paused",
        value: DefaultValue::Boolean(false),
    },
    ColumnDefaultSpec {
        table: "quant_feedback_scheduler_state",
        column: "pause_revision",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "quant_feedback_scheduler_state",
        column: "revision",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "quant_feedback_scheduler_state",
        column: "settlement_failure_count",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "quant_feedback_scheduler_state",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_feedback_scheduler_state",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_feedback_evaluation_use",
        column: "reserved_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_feedback_evaluation_use",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_feedback_event_outbox",
        column: "publish_attempts",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "quant_feedback_event_outbox",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_feedback_event_outbox",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_feedback_promotion_permit",
        column: "revision",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "quant_feedback_promotion_permit",
        column: "issued_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_feedback_promotion_permit",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_feedback_recipe_template",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_feedback_stage_event",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_feedback_trigger_event",
        column: "occurred_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_feedback_trigger_event",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_model_route_shadow_binding",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_model_route_shadow_binding",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_drift_report",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_feature_parity_candidate",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_feature_parity_run",
        column: "status",
        value: DefaultValue::PostgresLiteral("'queued'::qp_feature_parity_run_status"),
    },
    ColumnDefaultSpec {
        table: "quant_feature_parity_run",
        column: "total_count",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "quant_feature_parity_run",
        column: "compared_count",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "quant_feature_parity_run",
        column: "matched_count",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "quant_feature_parity_run",
        column: "mismatched_count",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "quant_feature_parity_run",
        column: "pending_materialization_count",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "quant_feature_parity_run",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_feature_parity_run",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_feature_parity_state",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_feature_parity_subject",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_feature_vector",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_market_linkage",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_market_linkage_source",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_market_selection",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_model_comparison_report",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_model_governance_audit",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_model_run",
        column: "status",
        value: DefaultValue::PostgresLiteral("'running'::qp_model_run_status"),
    },
    ColumnDefaultSpec {
        table: "quant_model_run",
        column: "started_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_model_spec",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_model_version",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_order_intent",
        column: "exit_state",
        value: DefaultValue::PostgresLiteral("'not_started'::qp_exit_state"),
    },
    ColumnDefaultSpec {
        table: "quant_order_intent",
        column: "scale_out_state",
        value: DefaultValue::PostgresLiteral(
            "'{\"pending_target\": null, \"denominator_shares\": null, \"settled_target_ids\": [], \"cumulative_exited_shares\": \"0\"}'::jsonb",
        ),
    },
    ColumnDefaultSpec {
        table: "quant_order_intent",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_order_intent",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_portfolio_plan",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_position",
        column: "opened_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_position",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_recommendation",
        column: "status_changed_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_recommendation",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_execution_attempt_outcome",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_execution_attempt_reconciliation_task",
        column: "status",
        value: DefaultValue::PostgresLiteral("'pending'::qp_outcome_reconciliation_task_status"),
    },
    ColumnDefaultSpec {
        table: "quant_execution_attempt_reconciliation_task",
        column: "attempt_count",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "quant_execution_attempt_reconciliation_task",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_execution_attempt_reconciliation_task",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_execution_rollup_reconciliation_task",
        column: "status",
        value: DefaultValue::PostgresLiteral("'pending'::qp_outcome_reconciliation_task_status"),
    },
    ColumnDefaultSpec {
        table: "quant_execution_rollup_reconciliation_task",
        column: "attempt_count",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "quant_execution_rollup_reconciliation_task",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_execution_rollup_reconciliation_task",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_resolution_outcome_reconciliation_task",
        column: "status",
        value: DefaultValue::PostgresLiteral("'pending'::qp_outcome_reconciliation_task_status"),
    },
    ColumnDefaultSpec {
        table: "quant_resolution_outcome_reconciliation_task",
        column: "attempt_count",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "quant_resolution_outcome_reconciliation_task",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_resolution_outcome_reconciliation_task",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_recommendation_execution_rollup",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_recommendation_execution_rollup_attempt",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_recommendation_resolution_outcome",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_recommendation_report",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_reconciliation",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_reconciliation",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_resolution_observation_inbox",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_resolution_observation_projection",
        column: "status",
        value: DefaultValue::PostgresLiteral("'pending'::qp_resolution_projection_status"),
    },
    ColumnDefaultSpec {
        table: "quant_resolution_observation_projection",
        column: "revision",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "quant_resolution_observation_projection",
        column: "attempt_count",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "quant_resolution_observation_projection",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_resolution_observation_projection",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_resolution_projection_remediation",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_report_data_quality_snapshot",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_report_fact_delivery",
        column: "attempt_count",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "quant_report_fact_delivery",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_report_fact_delivery",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_report_schedule_state",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_report_schedule_state",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_research_job",
        column: "status",
        value: DefaultValue::PostgresLiteral("'queued'::qp_research_job_status"),
    },
    ColumnDefaultSpec {
        table: "quant_research_job",
        column: "recovery_attempt",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "quant_research_job",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_research_job",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_research_readiness_evidence",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_execution_account",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_settlement_redeem",
        column: "attempt_count",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "quant_settlement_redeem",
        column: "retry_count",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "quant_settlement_redeem",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_settlement_redeem",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_settlement_authorization",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_settlement_chain_submission",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_settlement_chain_submission",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_settlement_governed_action",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_settlement_governed_action",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_settlement_external_cursor",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_settlement_inventory_lot",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_settlement_redeem_lot",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_shadow_comparison",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_source_slice",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_trade_policy_artifact",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_trade_policy_artifact",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_trade_policy_governance_audit",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_trade_policy_trial_attempt",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_trade_policy_validation",
        column: "total_rows",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "quant_trade_policy_validation",
        column: "passed_rows",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "quant_trade_policy_validation",
        column: "failed_rows",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "quant_trade_policy_validation",
        column: "started_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_trade_policy_validation",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_trade_policy_validation_row",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_trade_tape_block_cursor",
        column: "last_finalized_block",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "quant_trade_tape_block_cursor",
        column: "last_log_index",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "quant_trade_tape_block_cursor",
        column: "head_lag_blocks",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "quant_trade_tape_block_cursor",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_trade_tape_block_cursor",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_training_dataset",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_weather_daily_temperature_projection",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "quant_weather_observation_current",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "role",
        column: "status",
        value: DefaultValue::PostgresLiteral("'enabled'::qp_role_status"),
    },
    ColumnDefaultSpec {
        table: "role",
        column: "sort",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "role",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "role",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "role_menu",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "seed_application",
        column: "applied_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "system_runtime_control",
        column: "quant_runtime_mode",
        value: DefaultValue::PostgresLiteral("'report_only'::qp_quant_runtime_mode"),
    },
    ColumnDefaultSpec {
        table: "system_runtime_control",
        column: "settlement_write_policy",
        value: DefaultValue::PostgresLiteral("'disabled'::qp_settlement_write_policy"),
    },
    ColumnDefaultSpec {
        table: "system_runtime_control",
        column: "kill_switch_state",
        value: DefaultValue::PostgresLiteral("'closed'::qp_kill_switch_state"),
    },
    ColumnDefaultSpec {
        table: "system_runtime_control",
        column: "changed_by",
        value: DefaultValue::Text("bootstrap"),
    },
    ColumnDefaultSpec {
        table: "system_runtime_control",
        column: "reason",
        value: DefaultValue::Text("fresh boot safe defaults"),
    },
    ColumnDefaultSpec {
        table: "system_runtime_control",
        column: "kill_switch_requires_ack",
        value: DefaultValue::Boolean(false),
    },
    ColumnDefaultSpec {
        table: "system_runtime_control",
        column: "changed_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "system_runtime_control",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "system_runtime_control",
        column: "revision",
        value: DefaultValue::Integer(0),
    },
    ColumnDefaultSpec {
        table: "user",
        column: "status",
        value: DefaultValue::PostgresLiteral("'active'::qp_user_status"),
    },
    ColumnDefaultSpec {
        table: "user",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "user",
        column: "updated_at",
        value: DefaultValue::StatementTimestamp,
    },
    ColumnDefaultSpec {
        table: "user_role",
        column: "created_at",
        value: DefaultValue::StatementTimestamp,
    },
];

pub(in crate::migrations) async fn apply(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    let mut grouped = BTreeMap::<&str, Vec<&ColumnDefaultSpec>>::new();
    for spec in COLUMN_DEFAULTS {
        grouped.entry(spec.table).or_default().push(spec);
    }
    for (table, specs) in grouped {
        let mut statement = Table::alter();
        statement.table((Alias::new("public"), Alias::new(table)));
        for spec in specs {
            statement.modify_column(
                ColumnDef::new(Alias::new(spec.column)).default(spec.value.expression()),
            );
        }
        manager.alter_table(statement.clone()).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{COLUMN_DEFAULTS, DefaultValue};

    #[test]
    fn defaults_unique_literals_expressions() {
        let mut keys = BTreeSet::new();
        for spec in COLUMN_DEFAULTS {
            assert!(!spec.table.is_empty());
            assert!(!spec.column.is_empty());
            assert!(keys.insert((spec.table, spec.column)));
            if let DefaultValue::PostgresLiteral(value) = spec.value {
                assert!(!value.is_empty());
                assert!(!value.contains(';'));
            }
        }
    }

    #[test]
    fn promotion_permit_defaults_owned() {
        let defaults = COLUMN_DEFAULTS
            .iter()
            .filter(|spec| spec.table == "quant_feedback_promotion_permit")
            .map(|spec| (spec.column, spec.value))
            .collect::<Vec<_>>();
        assert_eq!(
            defaults,
            vec![
                ("revision", DefaultValue::Integer(0)),
                ("issued_at", DefaultValue::StatementTimestamp),
                ("updated_at", DefaultValue::StatementTimestamp),
            ]
        );
    }
}
