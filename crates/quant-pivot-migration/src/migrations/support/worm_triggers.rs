use sea_orm_migration::prelude::*;

use super::{
    v1,
    v1::{TriggerEvents, TriggerProgram, TriggerSpec},
};

pub const SOURCE: &[u8] = include_bytes!("worm_triggers.rs");

const TRIGGERS: &[TriggerSpec] = &[
    TriggerSpec {
        name: "trg_decision_policy_snapshot_append_only",
        table: "decision_policy_snapshot",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_policy_activation_append_only",
        table: "policy_activation",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_policy_activation_audit_append_only",
        table: "policy_activation_audit",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_policy_activation_event_outbox_append_only",
        table: "policy_activation_event_outbox",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_policy_approval_append_only",
        table: "policy_approval",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_policy_profile_artifact_append_only",
        table: "policy_profile_artifact",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_research_profile_artifact_append_only",
        table: "research_profile_artifact",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_domain_source_expectation_updated_at",
        table: "quant_domain_source_expectation",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_catalog_event_change_append_only",
        table: "catalog_event_change",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_catalog_event_object_append_only",
        table: "catalog_event_object",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_catalog_market_change_append_only",
        table: "catalog_market_change",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_catalog_market_object_append_only",
        table: "catalog_market_object",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_catalog_sync_batch_updated_at",
        table: "catalog_sync_batch",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_catalog_sync_rejection_append_only",
        table: "catalog_sync_rejection",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_event_updated_at",
        table: "event",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_market_updated_at",
        table: "market",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_menu_updated_at",
        table: "menu",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_operation_log_append_only",
        table: "operation_log",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_backtest_report_append_only",
        table: "quant_backtest_report",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_backtest_path_set_append_only",
        table: "quant_backtest_path_set",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_calibration_artifact_lifecycle_guard",
        table: "quant_calibration_artifact",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::GuardCalibrationArtifact,
    },
    TriggerSpec {
        name: "trg_quant_calibration_artifact_publication_append_only",
        table: "quant_calibration_artifact_publication",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_capital_allocation_updated_at",
        table: "quant_capital_allocation",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_quant_crypto_price_projection_updated_at",
        table: "quant_crypto_price_projection",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_quant_domain_event_outbox_updated_at",
        table: "quant_domain_event_outbox",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_quant_domain_source_cursor_updated_at",
        table: "quant_domain_source_cursor",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_quant_entry_condition_artifact_append_only",
        table: "quant_entry_condition_artifact",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_entry_condition_audit_append_only",
        table: "quant_entry_condition_audit",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_entry_condition_evaluation_outbox_updated_at",
        table: "quant_entry_condition_evaluation_outbox",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_quant_entry_condition_instance_updated_at",
        table: "quant_entry_condition_instance",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_quant_execution_order_updated_at",
        table: "quant_execution_order",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_quant_execution_trade_ref_updated_at",
        table: "quant_execution_trade_ref",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_quant_execution_transaction_ref_append_only",
        table: "quant_execution_transaction_ref",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_factor_definition_append_only",
        table: "quant_factor_definition",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_factor_value_append_only",
        table: "quant_factor_value",
        events: TriggerEvents::InsertOrDeleteOrUpdate,
        program: TriggerProgram::GuardFactorValue,
    },
    TriggerSpec {
        name: "trg_quant_attribution_artifact_append_only",
        table: "quant_attribution_artifact",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_model_candidate_manifest_append_only",
        table: "quant_model_candidate_manifest",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_feedback_cycle_lifecycle_guard",
        table: "quant_feedback_cycle",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::GuardFeedbackCycle,
    },
    TriggerSpec {
        name: "trg_quant_feedback_scheduler_updated_at",
        table: "quant_feedback_scheduler_state",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_quant_feedback_evaluation_use_append_only",
        table: "quant_feedback_evaluation_use",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_feedback_coordinator_fault_append_only",
        table: "quant_feedback_coordinator_fault",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_feedback_event_outbox_guard",
        table: "quant_feedback_event_outbox",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::GuardFeedbackOutbox,
    },
    TriggerSpec {
        name: "trg_quant_feedback_promotion_permit_guard",
        table: "quant_feedback_promotion_permit",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::GuardPromotionPermit,
    },
    TriggerSpec {
        name: "trg_quant_feedback_recipe_template_append_only",
        table: "quant_feedback_recipe_template",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_model_route_shadow_binding_guard",
        table: "quant_model_route_shadow_binding",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::GuardModelRouteShadowBinding,
    },
    TriggerSpec {
        name: "trg_quant_feedback_stage_event_append_only",
        table: "quant_feedback_stage_event",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_feedback_trigger_event_append_only",
        table: "quant_feedback_trigger_event",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_drift_report_append_only",
        table: "quant_drift_report",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_feature_parity_candidate_append_only",
        table: "quant_feature_parity_candidate",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_feature_vector_append_only",
        table: "quant_feature_vector",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_feature_parity_run_updated_at",
        table: "quant_feature_parity_run",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_quant_feature_parity_state_append_only",
        table: "quant_feature_parity_state",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_feature_parity_subject_append_only",
        table: "quant_feature_parity_subject",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_model_governance_audit_append_only",
        table: "quant_model_governance_audit",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_model_comparison_report_append_only",
        table: "quant_model_comparison_report",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_model_run_lifecycle_guard",
        table: "quant_model_run",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::GuardModelRun,
    },
    TriggerSpec {
        name: "trg_quant_model_spec_append_only",
        table: "quant_model_spec",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_model_version_append_only",
        table: "quant_model_version",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_order_intent_updated_at",
        table: "quant_order_intent",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_quant_position_updated_at",
        table: "quant_position",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_quant_execution_attempt_outcome_append_only",
        table: "quant_execution_attempt_outcome",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_execution_attempt_task_updated_at",
        table: "quant_execution_attempt_reconciliation_task",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_quant_execution_rollup_task_updated_at",
        table: "quant_execution_rollup_reconciliation_task",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_quant_resolution_outcome_task_updated_at",
        table: "quant_resolution_outcome_reconciliation_task",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_quant_execution_rollup_append_only",
        table: "quant_recommendation_execution_rollup",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_execution_rollup_attempt_append_only",
        table: "quant_recommendation_execution_rollup_attempt",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_recommendation_resolution_outcome_append_only",
        table: "quant_recommendation_resolution_outcome",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_reconciliation_updated_at",
        table: "quant_reconciliation",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_quant_resolution_inbox_append_only",
        table: "quant_resolution_observation_inbox",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_resolution_projection_updated_at",
        table: "quant_resolution_observation_projection",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_quant_resolution_remediation_append_only",
        table: "quant_resolution_projection_remediation",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_report_fact_delivery_updated_at",
        table: "quant_report_fact_delivery",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_quant_report_schedule_state_updated_at",
        table: "quant_report_schedule_state",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_quant_research_job_lifecycle_guard",
        table: "quant_research_job",
        events: TriggerEvents::Update,
        program: TriggerProgram::GuardResearchJob,
    },
    TriggerSpec {
        name: "trg_quant_research_readiness_evidence_append_only",
        table: "quant_research_readiness_evidence",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_settlement_external_cursor_updated_at",
        table: "quant_settlement_external_cursor",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_quant_settlement_governed_action_updated_at",
        table: "quant_settlement_governed_action",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_quant_settlement_chain_submission_updated_at",
        table: "quant_settlement_chain_submission",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_quant_settlement_redeem_updated_at",
        table: "quant_settlement_redeem",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_quant_settlement_inventory_lot_append_only",
        table: "quant_settlement_inventory_lot",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_shadow_comparison_append_only",
        table: "quant_shadow_comparison",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_source_slice_lifecycle_guard",
        table: "quant_source_slice",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::GuardSourceSlice,
    },
    TriggerSpec {
        name: "trg_quant_trade_policy_artifact_updated_at",
        table: "quant_trade_policy_artifact",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_quant_trade_policy_governance_audit_append_only",
        table: "quant_trade_policy_governance_audit",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_trade_policy_trial_attempt_append_only",
        table: "quant_trade_policy_trial_attempt",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_trade_policy_validation_row_append_only",
        table: "quant_trade_policy_validation_row",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_quant_trade_tape_block_cursor_updated_at",
        table: "quant_trade_tape_block_cursor",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_quant_training_dataset_lifecycle_guard",
        table: "quant_training_dataset",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::GuardTrainingDataset,
    },
    TriggerSpec {
        name: "trg_quant_weather_daily_temperature_projection_updated_at",
        table: "quant_weather_daily_temperature_projection",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_quant_weather_observation_current_updated_at",
        table: "quant_weather_observation_current",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_role_updated_at",
        table: "role",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_system_runtime_control_transition_append_only",
        table: "system_runtime_control_transition",
        events: TriggerEvents::DeleteOrUpdate,
        program: TriggerProgram::DenyWrite,
    },
    TriggerSpec {
        name: "trg_system_runtime_control_updated_at",
        table: "system_runtime_control",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
    TriggerSpec {
        name: "trg_user_updated_at",
        table: "user",
        events: TriggerEvents::Update,
        program: TriggerProgram::SetUpdatedAt,
    },
];

pub async fn apply(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    v1::create_trigger_programs(manager).await?;
    for spec in TRIGGERS {
        v1::create_trigger(manager, *spec).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{TRIGGERS, TriggerEvents, TriggerProgram};

    #[test]
    fn factor_values_are_worm() {
        let trigger = TRIGGERS
            .iter()
            .find(|trigger| trigger.name == "trg_quant_factor_value_append_only")
            .expect("factor-value append-only trigger");
        assert_eq!(trigger.table, "quant_factor_value");
        assert_eq!(trigger.events, TriggerEvents::InsertOrDeleteOrUpdate);
        assert_eq!(trigger.program, TriggerProgram::GuardFactorValue);
    }

    #[test]
    fn factor_definitions_are_worm() {
        let trigger = TRIGGERS
            .iter()
            .find(|trigger| trigger.name == "trg_quant_factor_definition_append_only")
            .expect("factor-definition append-only trigger");
        assert_eq!(trigger.table, "quant_factor_definition");
        assert_eq!(trigger.events, TriggerEvents::DeleteOrUpdate);
        assert_eq!(trigger.program, TriggerProgram::DenyWrite);
    }

    #[test]
    fn backtest_sets_are_worm() {
        let trigger = TRIGGERS
            .iter()
            .find(|trigger| trigger.name == "trg_quant_backtest_path_set_append_only")
            .expect("backtest-path-set append-only trigger");
        assert_eq!(trigger.table, "quant_backtest_path_set");
        assert_eq!(trigger.events, TriggerEvents::DeleteOrUpdate);
        assert_eq!(trigger.program, TriggerProgram::DenyWrite);
    }

    #[test]
    fn research_reports_are_worm() {
        for name in [
            "trg_quant_backtest_report_append_only",
            "trg_quant_model_comparison_report_append_only",
        ] {
            let trigger = TRIGGERS
                .iter()
                .find(|trigger| trigger.name == name)
                .expect("research report append-only trigger");
            assert_eq!(trigger.events, TriggerEvents::DeleteOrUpdate);
            assert_eq!(trigger.program, TriggerProgram::DenyWrite);
        }
    }

    #[test]
    fn calibration_mutates_only_active() {
        let trigger = TRIGGERS
            .iter()
            .find(|trigger| trigger.name == "trg_quant_calibration_artifact_lifecycle_guard")
            .expect("calibration-artifact lifecycle trigger");
        assert_eq!(trigger.table, "quant_calibration_artifact");
        assert_eq!(trigger.events, TriggerEvents::DeleteOrUpdate);
        assert_eq!(trigger.program, TriggerProgram::GuardCalibrationArtifact);
    }

    #[test]
    fn feature_vectors_are_worm() {
        let trigger = TRIGGERS
            .iter()
            .find(|trigger| trigger.name == "trg_quant_feature_vector_append_only")
            .expect("feature-vector append-only trigger");
        assert_eq!(trigger.table, "quant_feature_vector");
        assert_eq!(trigger.events, TriggerEvents::DeleteOrUpdate);
        assert_eq!(trigger.program, TriggerProgram::DenyWrite);
    }

    #[test]
    fn model_contract_is_worm() {
        let trigger = TRIGGERS
            .iter()
            .find(|trigger| trigger.name == "trg_quant_model_version_append_only")
            .expect("model-version append-only trigger");
        assert_eq!(trigger.table, "quant_model_version");
        assert_eq!(trigger.events, TriggerEvents::DeleteOrUpdate);
        assert_eq!(trigger.program, TriggerProgram::DenyWrite);
    }

    #[test]
    fn feedback_evidence_is_worm() {
        for name in [
            "trg_quant_feedback_evaluation_use_append_only",
            "trg_quant_feedback_stage_event_append_only",
            "trg_quant_drift_report_append_only",
        ] {
            let trigger = TRIGGERS
                .iter()
                .find(|trigger| trigger.name == name)
                .expect("feedback evidence append-only trigger");
            assert_eq!(trigger.events, TriggerEvents::DeleteOrUpdate);
            assert_eq!(trigger.program, TriggerProgram::DenyWrite);
        }
        let outbox = TRIGGERS
            .iter()
            .find(|trigger| trigger.name == "trg_quant_feedback_event_outbox_guard")
            .expect("feedback outbox lifecycle guard");
        assert_eq!(outbox.events, TriggerEvents::DeleteOrUpdate);
        assert_eq!(outbox.program, TriggerProgram::GuardFeedbackOutbox);
        let cycle = TRIGGERS
            .iter()
            .find(|trigger| trigger.name == "trg_quant_feedback_cycle_lifecycle_guard")
            .expect("feedback-cycle lifecycle trigger");
        assert_eq!(cycle.events, TriggerEvents::DeleteOrUpdate);
        assert_eq!(cycle.program, TriggerProgram::GuardFeedbackCycle);
    }

    #[test]
    fn promotion_permit_revoke_guarded() {
        let permit = TRIGGERS
            .iter()
            .find(|trigger| trigger.name == "trg_quant_feedback_promotion_permit_guard")
            .expect("promotion-permit lifecycle guard");
        assert_eq!(permit.events, TriggerEvents::DeleteOrUpdate);
        assert_eq!(permit.program, TriggerProgram::GuardPromotionPermit);
    }
}
