use sea_orm_migration::prelude::*;

use super::v1;

pub const SOURCE: &[u8] = include_bytes!("worm_triggers.rs");

const TRIGGERS: &[v1::TriggerSpec] = &[
    v1::TriggerSpec {
        name: "trg_decision_policy_snapshot_append_only",
        table: "decision_policy_snapshot",
        events: v1::TriggerEvents::DeleteOrUpdate,
        program: v1::TriggerProgram::DenyWrite,
    },
    v1::TriggerSpec {
        name: "trg_policy_activation_append_only",
        table: "policy_activation",
        events: v1::TriggerEvents::DeleteOrUpdate,
        program: v1::TriggerProgram::DenyWrite,
    },
    v1::TriggerSpec {
        name: "trg_policy_approval_append_only",
        table: "policy_approval",
        events: v1::TriggerEvents::DeleteOrUpdate,
        program: v1::TriggerProgram::DenyWrite,
    },
    v1::TriggerSpec {
        name: "trg_system_production_baseline_append_only",
        table: "system_production_baseline",
        events: v1::TriggerEvents::DeleteOrUpdate,
        program: v1::TriggerProgram::DenyWrite,
    },
    v1::TriggerSpec {
        name: "trg_quant_domain_source_expectation_updated_at",
        table: "quant_domain_source_expectation",
        events: v1::TriggerEvents::Update,
        program: v1::TriggerProgram::SetUpdatedAt,
    },
    v1::TriggerSpec {
        name: "trg_catalog_event_change_append_only",
        table: "catalog_event_change",
        events: v1::TriggerEvents::DeleteOrUpdate,
        program: v1::TriggerProgram::DenyWrite,
    },
    v1::TriggerSpec {
        name: "trg_catalog_event_object_append_only",
        table: "catalog_event_object",
        events: v1::TriggerEvents::DeleteOrUpdate,
        program: v1::TriggerProgram::DenyWrite,
    },
    v1::TriggerSpec {
        name: "trg_catalog_market_change_append_only",
        table: "catalog_market_change",
        events: v1::TriggerEvents::DeleteOrUpdate,
        program: v1::TriggerProgram::DenyWrite,
    },
    v1::TriggerSpec {
        name: "trg_catalog_market_object_append_only",
        table: "catalog_market_object",
        events: v1::TriggerEvents::DeleteOrUpdate,
        program: v1::TriggerProgram::DenyWrite,
    },
    v1::TriggerSpec {
        name: "trg_catalog_sync_batch_updated_at",
        table: "catalog_sync_batch",
        events: v1::TriggerEvents::Update,
        program: v1::TriggerProgram::SetUpdatedAt,
    },
    v1::TriggerSpec {
        name: "trg_catalog_sync_rejection_append_only",
        table: "catalog_sync_rejection",
        events: v1::TriggerEvents::DeleteOrUpdate,
        program: v1::TriggerProgram::DenyWrite,
    },
    v1::TriggerSpec {
        name: "trg_event_updated_at",
        table: "event",
        events: v1::TriggerEvents::Update,
        program: v1::TriggerProgram::SetUpdatedAt,
    },
    v1::TriggerSpec {
        name: "trg_market_updated_at",
        table: "market",
        events: v1::TriggerEvents::Update,
        program: v1::TriggerProgram::SetUpdatedAt,
    },
    v1::TriggerSpec {
        name: "trg_menu_updated_at",
        table: "menu",
        events: v1::TriggerEvents::Update,
        program: v1::TriggerProgram::SetUpdatedAt,
    },
    v1::TriggerSpec {
        name: "trg_operation_log_append_only",
        table: "operation_log",
        events: v1::TriggerEvents::DeleteOrUpdate,
        program: v1::TriggerProgram::DenyWrite,
    },
    v1::TriggerSpec {
        name: "trg_quant_calibration_artifact_publication_append_only",
        table: "quant_calibration_artifact_publication",
        events: v1::TriggerEvents::DeleteOrUpdate,
        program: v1::TriggerProgram::DenyWrite,
    },
    v1::TriggerSpec {
        name: "trg_quant_capital_allocation_updated_at",
        table: "quant_capital_allocation",
        events: v1::TriggerEvents::Update,
        program: v1::TriggerProgram::SetUpdatedAt,
    },
    v1::TriggerSpec {
        name: "trg_quant_crypto_price_projection_updated_at",
        table: "quant_crypto_price_projection",
        events: v1::TriggerEvents::Update,
        program: v1::TriggerProgram::SetUpdatedAt,
    },
    v1::TriggerSpec {
        name: "trg_quant_domain_event_outbox_updated_at",
        table: "quant_domain_event_outbox",
        events: v1::TriggerEvents::Update,
        program: v1::TriggerProgram::SetUpdatedAt,
    },
    v1::TriggerSpec {
        name: "trg_quant_domain_source_cursor_updated_at",
        table: "quant_domain_source_cursor",
        events: v1::TriggerEvents::Update,
        program: v1::TriggerProgram::SetUpdatedAt,
    },
    v1::TriggerSpec {
        name: "trg_quant_entry_condition_artifact_append_only",
        table: "quant_entry_condition_artifact",
        events: v1::TriggerEvents::DeleteOrUpdate,
        program: v1::TriggerProgram::DenyWrite,
    },
    v1::TriggerSpec {
        name: "trg_quant_entry_condition_audit_append_only",
        table: "quant_entry_condition_audit",
        events: v1::TriggerEvents::DeleteOrUpdate,
        program: v1::TriggerProgram::DenyWrite,
    },
    v1::TriggerSpec {
        name: "trg_quant_entry_condition_evaluation_outbox_updated_at",
        table: "quant_entry_condition_evaluation_outbox",
        events: v1::TriggerEvents::Update,
        program: v1::TriggerProgram::SetUpdatedAt,
    },
    v1::TriggerSpec {
        name: "trg_quant_entry_condition_instance_updated_at",
        table: "quant_entry_condition_instance",
        events: v1::TriggerEvents::Update,
        program: v1::TriggerProgram::SetUpdatedAt,
    },
    v1::TriggerSpec {
        name: "trg_quant_execution_order_updated_at",
        table: "quant_execution_order",
        events: v1::TriggerEvents::Update,
        program: v1::TriggerProgram::SetUpdatedAt,
    },
    v1::TriggerSpec {
        name: "trg_quant_factor_definition_updated_at",
        table: "quant_factor_definition",
        events: v1::TriggerEvents::Update,
        program: v1::TriggerProgram::SetUpdatedAt,
    },
    v1::TriggerSpec {
        name: "trg_quant_feature_parity_candidate_append_only",
        table: "quant_feature_parity_candidate",
        events: v1::TriggerEvents::DeleteOrUpdate,
        program: v1::TriggerProgram::DenyWrite,
    },
    v1::TriggerSpec {
        name: "trg_quant_feature_parity_run_updated_at",
        table: "quant_feature_parity_run",
        events: v1::TriggerEvents::Update,
        program: v1::TriggerProgram::SetUpdatedAt,
    },
    v1::TriggerSpec {
        name: "trg_quant_feature_parity_state_append_only",
        table: "quant_feature_parity_state",
        events: v1::TriggerEvents::DeleteOrUpdate,
        program: v1::TriggerProgram::DenyWrite,
    },
    v1::TriggerSpec {
        name: "trg_quant_feature_parity_subject_append_only",
        table: "quant_feature_parity_subject",
        events: v1::TriggerEvents::DeleteOrUpdate,
        program: v1::TriggerProgram::DenyWrite,
    },
    v1::TriggerSpec {
        name: "trg_quant_model_governance_audit_append_only",
        table: "quant_model_governance_audit",
        events: v1::TriggerEvents::DeleteOrUpdate,
        program: v1::TriggerProgram::DenyWrite,
    },
    v1::TriggerSpec {
        name: "trg_quant_model_spec_updated_at",
        table: "quant_model_spec",
        events: v1::TriggerEvents::Update,
        program: v1::TriggerProgram::SetUpdatedAt,
    },
    v1::TriggerSpec {
        name: "trg_quant_order_intent_updated_at",
        table: "quant_order_intent",
        events: v1::TriggerEvents::Update,
        program: v1::TriggerProgram::SetUpdatedAt,
    },
    v1::TriggerSpec {
        name: "trg_quant_position_updated_at",
        table: "quant_position",
        events: v1::TriggerEvents::Update,
        program: v1::TriggerProgram::SetUpdatedAt,
    },
    v1::TriggerSpec {
        name: "trg_quant_recommendation_attribution_append_only",
        table: "quant_recommendation_attribution",
        events: v1::TriggerEvents::DeleteOrUpdate,
        program: v1::TriggerProgram::DenyWrite,
    },
    v1::TriggerSpec {
        name: "trg_quant_reconciliation_updated_at",
        table: "quant_reconciliation",
        events: v1::TriggerEvents::Update,
        program: v1::TriggerProgram::SetUpdatedAt,
    },
    v1::TriggerSpec {
        name: "trg_quant_report_fact_delivery_updated_at",
        table: "quant_report_fact_delivery",
        events: v1::TriggerEvents::Update,
        program: v1::TriggerProgram::SetUpdatedAt,
    },
    v1::TriggerSpec {
        name: "trg_quant_report_schedule_state_updated_at",
        table: "quant_report_schedule_state",
        events: v1::TriggerEvents::Update,
        program: v1::TriggerProgram::SetUpdatedAt,
    },
    v1::TriggerSpec {
        name: "trg_quant_research_job_updated_at",
        table: "quant_research_job",
        events: v1::TriggerEvents::Update,
        program: v1::TriggerProgram::SetUpdatedAt,
    },
    v1::TriggerSpec {
        name: "trg_quant_research_readiness_evidence_append_only",
        table: "quant_research_readiness_evidence",
        events: v1::TriggerEvents::DeleteOrUpdate,
        program: v1::TriggerProgram::DenyWrite,
    },
    v1::TriggerSpec {
        name: "trg_quant_settlement_redeem_updated_at",
        table: "quant_settlement_redeem",
        events: v1::TriggerEvents::Update,
        program: v1::TriggerProgram::SetUpdatedAt,
    },
    v1::TriggerSpec {
        name: "trg_quant_shadow_comparison_append_only",
        table: "quant_shadow_comparison",
        events: v1::TriggerEvents::DeleteOrUpdate,
        program: v1::TriggerProgram::DenyWrite,
    },
    v1::TriggerSpec {
        name: "trg_quant_trade_policy_artifact_updated_at",
        table: "quant_trade_policy_artifact",
        events: v1::TriggerEvents::Update,
        program: v1::TriggerProgram::SetUpdatedAt,
    },
    v1::TriggerSpec {
        name: "trg_quant_trade_policy_governance_audit_append_only",
        table: "quant_trade_policy_governance_audit",
        events: v1::TriggerEvents::DeleteOrUpdate,
        program: v1::TriggerProgram::DenyWrite,
    },
    v1::TriggerSpec {
        name: "trg_quant_trade_policy_trial_attempt_append_only",
        table: "quant_trade_policy_trial_attempt",
        events: v1::TriggerEvents::DeleteOrUpdate,
        program: v1::TriggerProgram::DenyWrite,
    },
    v1::TriggerSpec {
        name: "trg_quant_trade_policy_validation_row_append_only",
        table: "quant_trade_policy_validation_row",
        events: v1::TriggerEvents::DeleteOrUpdate,
        program: v1::TriggerProgram::DenyWrite,
    },
    v1::TriggerSpec {
        name: "trg_quant_trade_tape_block_cursor_updated_at",
        table: "quant_trade_tape_block_cursor",
        events: v1::TriggerEvents::Update,
        program: v1::TriggerProgram::SetUpdatedAt,
    },
    v1::TriggerSpec {
        name: "trg_quant_weather_daily_temperature_projection_updated_at",
        table: "quant_weather_daily_temperature_projection",
        events: v1::TriggerEvents::Update,
        program: v1::TriggerProgram::SetUpdatedAt,
    },
    v1::TriggerSpec {
        name: "trg_quant_weather_observation_current_updated_at",
        table: "quant_weather_observation_current",
        events: v1::TriggerEvents::Update,
        program: v1::TriggerProgram::SetUpdatedAt,
    },
    v1::TriggerSpec {
        name: "trg_role_updated_at",
        table: "role",
        events: v1::TriggerEvents::Update,
        program: v1::TriggerProgram::SetUpdatedAt,
    },
    v1::TriggerSpec {
        name: "trg_system_bootstrap_transition_append_only",
        table: "system_bootstrap_transition",
        events: v1::TriggerEvents::DeleteOrUpdate,
        program: v1::TriggerProgram::DenyWrite,
    },
    v1::TriggerSpec {
        name: "trg_system_kill_switch_updated_at",
        table: "system_kill_switch",
        events: v1::TriggerEvents::Update,
        program: v1::TriggerProgram::SetUpdatedAt,
    },
    v1::TriggerSpec {
        name: "trg_system_runtime_state_updated_at",
        table: "system_runtime_state",
        events: v1::TriggerEvents::Update,
        program: v1::TriggerProgram::SetUpdatedAt,
    },
    v1::TriggerSpec {
        name: "trg_user_updated_at",
        table: "user",
        events: v1::TriggerEvents::Update,
        program: v1::TriggerProgram::SetUpdatedAt,
    },
];

pub async fn apply(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    v1::create_trigger_programs(manager).await?;
    for spec in TRIGGERS {
        v1::create_trigger(manager, *spec).await?;
    }
    Ok(())
}
