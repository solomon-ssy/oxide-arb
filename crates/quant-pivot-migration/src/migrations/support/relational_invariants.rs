use sea_orm_migration::prelude::*;

use super::{
    v1,
    v1::{ConstraintKind, ConstraintSpec},
};

pub const SOURCE: &[u8] = include_bytes!("relational_invariants.rs");
const MODEL_TRAINING_OBJECTIVE_CHECK: &str = r"CHECK (
    jsonb_typeof(training_objective) = 'object'
    AND training_objective ?& ARRAY['format_version', 'definition']
    AND training_objective - ARRAY['format_version', 'definition'] = '{}'::jsonb
    AND (training_objective ->> 'format_version')::integer = 2
    AND jsonb_typeof(training_objective -> 'definition') = 'object'
    AND (
        (
            training_objective -> 'definition' ->> 'kind' = 'learning_to_rank'
            AND (training_objective -> 'definition') ?& ARRAY['kind', 'spec']
            AND (training_objective -> 'definition') - ARRAY['kind', 'spec'] = '{}'::jsonb
            AND jsonb_typeof(training_objective -> 'definition' -> 'spec') = 'object'
            AND (training_objective -> 'definition' -> 'spec')
                ?& ARRAY[
                    'rank_loss',
                    'optimizer',
                    'lambda_tail',
                    'tail_fraction',
                    'lambda_turnover',
                    'lambda_l2',
                    'ndcg_k',
                    'pseudo_top_n'
                ]
            AND (training_objective -> 'definition' -> 'spec')
                - ARRAY[
                    'rank_loss',
                    'optimizer',
                    'lambda_tail',
                    'tail_fraction',
                    'lambda_turnover',
                    'lambda_l2',
                    'ndcg_k',
                    'pseudo_top_n'
                ] = '{}'::jsonb
        )
        OR (
            training_objective -> 'definition' ->> 'kind' = 'classical_pointwise'
            AND (training_objective -> 'definition')
                ?& ARRAY['kind', 'model_kind', 'validation_metric']
            AND (training_objective -> 'definition')
                - ARRAY['kind', 'model_kind', 'validation_metric'] = '{}'::jsonb
            AND jsonb_typeof(training_objective -> 'definition' -> 'model_kind') = 'string'
            AND training_objective -> 'definition' ->> 'validation_metric'
                = 'mean_rolling_fold_rank_ic'
        )
        OR (
            training_objective -> 'definition' ->> 'kind' = 'governed_sell_estimator'
            AND (training_objective -> 'definition') ?& ARRAY['kind', 'fit_status']
            AND (training_objective -> 'definition') - ARRAY['kind', 'fit_status'] = '{}'::jsonb
            AND training_objective -> 'definition' ->> 'fit_status'
                = 'oof_predictions_required'
        )
        OR (
            training_objective -> 'definition' ->> 'kind' = 'hand_authored'
            AND (training_objective -> 'definition') ?& ARRAY['kind', 'rationale']
            AND (training_objective -> 'definition') - ARRAY['kind', 'rationale'] = '{}'::jsonb
            AND jsonb_typeof(training_objective -> 'definition' -> 'rationale') = 'string'
            AND char_length(btrim(training_objective -> 'definition' ->> 'rationale'))
                BETWEEN 1 AND 2048
        )
    )
)";
const MODEL_VERSION_METRICS_CHECK: &str = r"CHECK (
    jsonb_typeof(metrics) = 'object'
    AND metrics ?& ARRAY['format_version', 'definition']
    AND metrics - ARRAY['format_version', 'definition'] = '{}'::jsonb
    AND (metrics ->> 'format_version')::integer = 2
    AND jsonb_typeof(metrics -> 'definition') = 'object'
    AND (
        (
            metrics -> 'definition' ->> 'kind' = 'learning_to_rank'
            AND training_objective -> 'definition' ->> 'kind' = 'learning_to_rank'
            AND (metrics -> 'definition')
                ?& ARRAY['kind', 'in_sample', 'validation', 'artifact_lineage']
            AND (metrics -> 'definition')
                - ARRAY['kind', 'in_sample', 'validation', 'artifact_lineage'] = '{}'::jsonb
            AND jsonb_typeof(metrics -> 'definition' -> 'in_sample') = 'object'
            AND jsonb_typeof(metrics -> 'definition' -> 'validation') = 'object'
            AND jsonb_typeof(metrics -> 'definition' -> 'artifact_lineage') = 'object'
            AND metrics -> 'definition' -> 'artifact_lineage' ->> 'kind' = 'factor_native'
        )
        OR (
            metrics -> 'definition' ->> 'kind' = 'classical_pointwise'
            AND training_objective -> 'definition' ->> 'kind' = 'classical_pointwise'
            AND (metrics -> 'definition')
                ?& ARRAY[
                    'kind',
                    'model_kind',
                    'in_sample',
                    'validation',
                    'feature_importances',
                    'artifact_lineage'
                ]
            AND (metrics -> 'definition')
                - ARRAY[
                    'kind',
                    'model_kind',
                    'in_sample',
                    'validation',
                    'feature_importances',
                    'artifact_lineage'
                ] = '{}'::jsonb
            AND metrics -> 'definition' ->> 'model_kind'
                = training_objective -> 'definition' ->> 'model_kind'
            AND jsonb_typeof(metrics -> 'definition' -> 'in_sample') = 'object'
            AND jsonb_typeof(metrics -> 'definition' -> 'validation') = 'object'
            AND jsonb_typeof(metrics -> 'definition' -> 'feature_importances') = 'array'
            AND jsonb_typeof(metrics -> 'definition' -> 'artifact_lineage') = 'object'
            AND metrics -> 'definition' -> 'artifact_lineage' ->> 'kind'
                = 'fitted_feature_matrix'
        )
        OR (
            metrics -> 'definition' ->> 'kind' = 'governed_sell_estimator'
            AND training_objective -> 'definition' ->> 'kind' = 'governed_sell_estimator'
            AND (metrics -> 'definition')
                ?& ARRAY['kind', 'preparation', 'artifact_lineage']
            AND (metrics -> 'definition')
                - ARRAY['kind', 'preparation', 'artifact_lineage'] = '{}'::jsonb
            AND jsonb_typeof(metrics -> 'definition' -> 'preparation') = 'object'
            AND (metrics -> 'definition' -> 'preparation')
                ?& ARRAY['resolved_label_rows', 'position_state_rows', 'fit_status']
            AND (metrics -> 'definition' -> 'preparation')
                - ARRAY['resolved_label_rows', 'position_state_rows', 'fit_status'] = '{}'::jsonb
            AND (metrics -> 'definition' -> 'preparation' ->> 'resolved_label_rows')::numeric >= 0
            AND (metrics -> 'definition' -> 'preparation' ->> 'position_state_rows')::numeric >= 0
            AND metrics -> 'definition' -> 'preparation' ->> 'fit_status'
                = training_objective -> 'definition' ->> 'fit_status'
            AND metrics -> 'definition' -> 'preparation' ->> 'fit_status'
                = 'oof_predictions_required'
            AND jsonb_typeof(metrics -> 'definition' -> 'artifact_lineage') = 'object'
            AND metrics -> 'definition' -> 'artifact_lineage' ->> 'kind' = 'factor_native'
        )
        OR (
            metrics -> 'definition' ->> 'kind' = 'not_measured'
            AND training_objective -> 'definition' ->> 'kind' = 'hand_authored'
            AND (metrics -> 'definition') ?& ARRAY['kind', 'rationale']
            AND (metrics -> 'definition') - ARRAY['kind', 'rationale'] = '{}'::jsonb
            AND jsonb_typeof(metrics -> 'definition' -> 'rationale') = 'string'
            AND char_length(btrim(metrics -> 'definition' ->> 'rationale'))
                BETWEEN 1 AND 2048
        )
    )
)";
const DRIFT_REPORT_CHECK: &str = r"CHECK (
    (
        (
            kind = 'data'::qp_feedback_drift_kind
            AND metric = ANY (
                ARRAY[
                    'population_stability_index'::qp_feedback_drift_metric,
                    'kolmogorov_smirnov_p_value'::qp_feedback_drift_metric
                ]
            )
        )
        OR (
            kind = 'concept'::qp_feedback_drift_kind
            AND metric = 'rank_ic_drop'::qp_feedback_drift_metric
        )
        OR (
            kind = 'label'::qp_feedback_drift_kind
            AND metric = 'jensen_shannon_divergence'::qp_feedback_drift_metric
        )
    )
    AND baseline_window_start < baseline_window_end
    AND baseline_window_end <= evaluation_window_start
    AND evaluation_window_start < evaluation_window_end
    AND evaluation_window_end <= label_cutoff
    AND label_cutoff <= observed_at
    AND observed_at <= created_at
    AND sample_count >= 0
    AND threshold NOT IN (
        'NaN'::numeric,
        'Infinity'::numeric,
        '-Infinity'::numeric
    )
    AND threshold > 0::numeric
    AND (
        metric = 'population_stability_index'::qp_feedback_drift_metric
        OR threshold <= 1::numeric
    )
    AND (
        observed_value IS NULL
        OR (
            observed_value NOT IN (
                'NaN'::numeric,
                'Infinity'::numeric,
                '-Infinity'::numeric
            )
            AND observed_value >= 0::numeric
            AND (
                metric = 'population_stability_index'::qp_feedback_drift_metric
                OR observed_value <= 1::numeric
            )
        )
    )
    AND (
        (
            assessment = 'insufficient_evidence'::qp_feedback_drift_assessment
            AND observed_value IS NULL
        )
        OR (
            assessment <> 'insufficient_evidence'::qp_feedback_drift_assessment
            AND observed_value IS NOT NULL
            AND sample_count > 0
            AND (
                (
                    metric = 'kolmogorov_smirnov_p_value'::qp_feedback_drift_metric
                    AND (
                        (
                            assessment =
                                'threshold_exceeded'::qp_feedback_drift_assessment
                            AND observed_value <= threshold
                        )
                        OR (
                            assessment =
                                'within_threshold'::qp_feedback_drift_assessment
                            AND observed_value > threshold
                        )
                    )
                )
                OR (
                    metric <> 'kolmogorov_smirnov_p_value'::qp_feedback_drift_metric
                    AND (
                        (
                            assessment =
                                'threshold_exceeded'::qp_feedback_drift_assessment
                            AND observed_value >= threshold
                        )
                        OR (
                            assessment =
                                'within_threshold'::qp_feedback_drift_assessment
                            AND observed_value < threshold
                        )
                    )
                )
            )
        )
    )
    AND octet_length(detail_uri) BETWEEN 1 AND 4096
    AND detail_uri ~ '^[a-z][a-z0-9+.-]*://.+$'::text
    AND detail_hash ~ '^blake3:[0-9a-f]{64}$'::text
    AND report_hash ~ '^blake3:[0-9a-f]{64}$'::text
)";
const CONSTRAINTS: &[ConstraintSpec] = &[
    ConstraintSpec {
        name: "catalog_event_object_schema_version_check",
        table: "catalog_event_object",
        kind: ConstraintKind::Check,
        definition: "CHECK ((schema_version > 0))",
    },
    ConstraintSpec {
        name: "catalog_market_object_schema_version_check",
        table: "catalog_market_object",
        kind: ConstraintKind::Check,
        definition: "CHECK ((schema_version > 0))",
    },
    ConstraintSpec {
        name: "catalog_sync_batch_check",
        table: "catalog_sync_batch",
        kind: ConstraintKind::Check,
        definition: "CHECK (((event_count >= 0) AND (market_count >= 0) AND (rejected_count >= 0) AND (((status = 'committed'::qp_catalog_sync_status) AND (fetched_at IS NOT NULL) AND (committed_at IS NOT NULL) AND (batch_hash IS NOT NULL) AND (failure_stage IS NULL) AND (failure_detail IS NULL)) OR ((status = 'failed'::qp_catalog_sync_status) AND (committed_at IS NULL) AND (failure_stage IS NOT NULL) AND (failure_detail IS NOT NULL) AND ((length(failure_detail) >= 1) AND (length(failure_detail) <= 2048))))))",
    },
    ConstraintSpec {
        name: "catalog_sync_rejection_detail_check",
        table: "catalog_sync_rejection",
        kind: ConstraintKind::Check,
        definition: "CHECK (((char_length(detail) >= 1) AND (char_length(detail) <= 2048)))",
    },
    ConstraintSpec {
        name: "market_check",
        table: "market",
        kind: ConstraintKind::Check,
        definition: "CHECK (((status <> 'active'::qp_market_status) OR (cardinality(filter_reasons) = 0)))",
    },
    ConstraintSpec {
        name: "market_check1",
        table: "market",
        kind: ConstraintKind::Check,
        definition: "CHECK (((status <> 'filtered'::qp_market_status) OR (cardinality(filter_reasons) > 0)))",
    },
    ConstraintSpec {
        name: "quant_factor_definition_definition_hash_check",
        table: "quant_factor_definition",
        kind: ConstraintKind::Check,
        definition: "CHECK ((definition_hash ~ '^blake3:[0-9a-f]{64}$'::text))",
    },
    ConstraintSpec {
        name: "quant_factor_definition_feature_contract_hash_check",
        table: "quant_factor_definition",
        kind: ConstraintKind::Check,
        definition: "CHECK ((feature_contract_hash ~ '^blake3:[0-9a-f]{64}$'::text))",
    },
    ConstraintSpec {
        name: "ck_quant_factor_definition_document",
        table: "quant_factor_definition",
        kind: ConstraintKind::Check,
        definition: "CHECK ((public.validate_factor_definition_document(definition) AND ((definition ->> 'name'::text) = name) AND ((definition ->> 'family'::text) = (factor_family)::text) AND (scope = CASE WHEN factor_family = 'structural'::qp_factor_family THEN 'structural'::qp_factor_definition_scope WHEN factor_family = 'domain_crypto'::qp_factor_family THEN 'domain_crypto'::qp_factor_definition_scope WHEN factor_family = 'domain_weather'::qp_factor_family THEN 'domain_weather'::qp_factor_definition_scope ELSE 'generic'::qp_factor_definition_scope END) AND (input_schema_version >= 1) AND (output_schema_version >= 1)))",
    },
    ConstraintSpec {
        name: "ck_quant_factor_value_explanation",
        table: "quant_factor_value",
        kind: ConstraintKind::Check,
        definition: "CHECK (public.validate_factor_explanation(explanation))",
    },
    ConstraintSpec {
        name: "ck_quant_factor_value_state_tuple",
        table: "quant_factor_value",
        kind: ConstraintKind::Check,
        definition: "CHECK (((confidence <> ALL (ARRAY['NaN'::numeric, 'Infinity'::numeric, '-Infinity'::numeric])) AND (confidence BETWEEN 0 AND 1) AND ((raw_value IS NULL) OR (raw_value <> ALL (ARRAY['NaN'::numeric, 'Infinity'::numeric, '-Infinity'::numeric]))) AND ((normalized_score IS NULL) OR ((normalized_score <> ALL (ARRAY['NaN'::numeric, 'Infinity'::numeric, '-Infinity'::numeric])) AND (normalized_score BETWEEN 0 AND 1))) AND (((value_state = 'scored'::qp_factor_value_state) AND (raw_value IS NOT NULL) AND (normalized_score IS NOT NULL) AND (normalization_source IS NOT NULL) AND (indeterminate_reason IS NULL)) OR ((value_state = ANY (ARRAY['missing_input'::qp_factor_value_state, 'not_applicable'::qp_factor_value_state])) AND (raw_value IS NULL) AND (normalized_score IS NULL) AND (normalization_source IS NULL) AND (indeterminate_reason IS NULL) AND (confidence = 0)) OR ((value_state = 'indeterminate'::qp_factor_value_state) AND (normalized_score IS NULL) AND (normalization_source IS NULL) AND (indeterminate_reason IS NOT NULL) AND (confidence = 0)))))",
    },
    ConstraintSpec {
        name: "uq_quant_factor_value_run_vector_definition",
        table: "quant_factor_value",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (model_run_id, feature_vector_id, factor_definition_id)",
    },
    ConstraintSpec {
        name: "ck_quant_feedback_cycle_identity",
        table: "quant_feedback_cycle",
        kind: ConstraintKind::Check,
        definition: concat!(
            "CHECK ((jsonb_typeof(idempotency_key) = 'object'::text AND idempotency_key ?& ARRAY['format_version'::text, 'trigger_family'::text, 'profile_ref'::text, 'feedback_policy_hash'::text, 'label_cutoff'::text, 'capability_registry_hashes'::text, 'champion_model_version_id'::text, 'champion_serving_contract_hash'::text, 'candidate_family'::text] AND (idempotency_key - ARRAY['format_version'::text, 'trigger_family'::text, 'profile_ref'::text, 'feedback_policy_hash'::text, 'label_cutoff'::text, 'capability_registry_hashes'::text, 'champion_model_version_id'::text, 'champion_serving_contract_hash'::text, 'candidate_family'::text]) = '{}'::jsonb AND (idempotency_key ->> 'format_version'::text)::integer = 1 AND idempotency_key ->> 'trigger_family'::text = trigger_family::text AND idempotency_key -> 'profile_ref'::text = profile_ref AND idempotency_key ->> 'feedback_policy_hash'::text = feedback_policy_hash AND (idempotency_key ->> 'label_cutoff'::text)::timestamp with time zone = label_cutoff AND idempotency_key -> 'capability_registry_hashes'::text = capability_registry_hashes AND (idempotency_key ->> 'champion_model_version_id'::text)::uuid = champion_model_version_id AND idempotency_key ->> 'champion_serving_contract_hash'::text = champion_serving_contract_hash AND idempotency_key -> 'candidate_family'::text = candidate_family AND jsonb_typeof(candidate_family) = 'object'::text AND candidate_family ?& ARRAY['format_version'::text, 'candidate_family_hash'::text, 'shared_evaluation'::text, 'comparison_contract'::text, 'candidates'::text] AND (candidate_family - ARRAY['format_version'::text, 'candidate_family_hash'::text, 'shared_evaluation'::text, 'comparison_contract'::text, 'candidates'::text]) = '{}'::jsonb AND (candidate_family ->> 'format_version'::text)::integer = 1 AND candidate_family ->> 'candidate_family_hash'::text = candidate_family_hash",
            " AND jsonb_typeof(candidate_family -> 'comparison_contract'::text) = 'object'::text AND (candidate_family -> 'comparison_contract'::text) ?& ARRAY['format_version'::text, 'comparison_contract_hash'::text, 'statistic'::text, 'alternative'::text, 'resampling'::text, 'stepdown'::text, 'p_value'::text, 'ties'::text, 'generator'::text, 'minimum_observations'::text, 'bootstrap_repetitions'::text, 'block_length'::text, 'bootstrap_seed'::text, 'minimum_effect_bps'::text, 'confidence'::text, 'effect_precision_dp'::text] AND ((candidate_family -> 'comparison_contract'::text) - ARRAY['format_version'::text, 'comparison_contract_hash'::text, 'statistic'::text, 'alternative'::text, 'resampling'::text, 'stepdown'::text, 'p_value'::text, 'ties'::text, 'generator'::text, 'minimum_observations'::text, 'bootstrap_repetitions'::text, 'block_length'::text, 'bootstrap_seed'::text, 'minimum_effect_bps'::text, 'confidence'::text, 'effect_precision_dp'::text]) = '{}'::jsonb",
            " AND (candidate_family -> 'comparison_contract'::text ->> 'format_version'::text)::integer = 1 AND candidate_family -> 'comparison_contract'::text ->> 'comparison_contract_hash'::text ~ '^blake3:[0-9a-f]{64}$'::text AND candidate_family -> 'comparison_contract'::text ->> 'statistic'::text = 'mean_decision_tick_net_return_bps'::text AND candidate_family -> 'comparison_contract'::text ->> 'alternative'::text = 'candidate_greater_than_champion'::text AND candidate_family -> 'comparison_contract'::text ->> 'resampling'::text = 'circular_fixed_block'::text AND candidate_family -> 'comparison_contract'::text ->> 'stepdown'::text = 'romano_wolf_basic'::text AND candidate_family -> 'comparison_contract'::text ->> 'p_value'::text = 'plus_one_greater_or_equal'::text AND candidate_family -> 'comparison_contract'::text ->> 'ties'::text = 'equal_statistic_group'::text AND candidate_family -> 'comparison_contract'::text ->> 'generator'::text = 'blake3_counter_rejection_v1'::text",
            " AND (candidate_family -> 'comparison_contract'::text ->> 'minimum_observations'::text)::numeric > 0 AND (candidate_family -> 'comparison_contract'::text ->> 'bootstrap_repetitions'::text)::numeric >= 1000 AND (candidate_family -> 'comparison_contract'::text ->> 'block_length'::text)::numeric BETWEEN 1 AND (candidate_family -> 'comparison_contract'::text ->> 'minimum_observations'::text)::numeric AND (candidate_family -> 'comparison_contract'::text ->> 'bootstrap_seed'::text)::numeric BETWEEN 0 AND 18446744073709551615 AND (candidate_family -> 'comparison_contract'::text ->> 'minimum_effect_bps'::text)::numeric > 0 AND (candidate_family -> 'comparison_contract'::text ->> 'confidence'::text)::numeric > 0 AND (candidate_family -> 'comparison_contract'::text ->> 'confidence'::text)::numeric < 1 AND (candidate_family -> 'comparison_contract'::text ->> 'effect_precision_dp'::text)::integer = 12",
            " AND jsonb_typeof(candidate_family -> 'shared_evaluation'::text) = 'object'::text AND candidate_family -> 'shared_evaluation'::text ->> 'purpose'::text = 'evaluation'::text AND jsonb_typeof(candidate_family -> 'candidates'::text) = 'array'::text AND jsonb_array_length(candidate_family -> 'candidates'::text) BETWEEN 1 AND 32 AND jsonb_typeof(profile_ref) = 'object'::text AND profile_ref ?& ARRAY['id'::text, 'version'::text, 'content_hash'::text] AND (profile_ref - ARRAY['id'::text, 'version'::text, 'content_hash'::text]) = '{}'::jsonb AND profile_ref ->> 'id'::text ~ '^[a-z0-9_]+$'::text AND (profile_ref ->> 'version'::text)::bigint > 0 AND profile_ref ->> 'content_hash'::text = profile_hash AND research_profile_artifact_id = (((('rpa:'::text || (profile_ref ->> 'id'::text)) || ':'::text) || (profile_ref ->> 'version'::text)) || ':'::text) || profile_hash AND public.validate_content_hash_array(capability_registry_hashes) AND idempotency_hash ~ '^blake3:[0-9a-f]{64}$'::text AND profile_hash ~ '^blake3:[0-9a-f]{64}$'::text AND feedback_policy_hash ~ '^blake3:[0-9a-f]{64}$'::text AND champion_serving_contract_hash ~ '^blake3:[0-9a-f]{64}$'::text AND candidate_family_hash ~ '^blake3:[0-9a-f]{64}$'::text AND label_cutoff <= created_at AND updated_at >= created_at AND generation >= 0))",
        ),
    },
    ConstraintSpec {
        name: "ck_quant_feedback_cycle_state",
        table: "quant_feedback_cycle",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((lease_owner IS NULL) = (lease_expires_at IS NULL)) AND ((started_at IS NULL) OR (started_at >= created_at)) AND ((lease_expires_at IS NULL) OR (started_at IS NOT NULL AND lease_expires_at > started_at)) AND ((cancel_requested_at IS NULL) OR (cancel_requested_at >= created_at AND (completed_at IS NULL OR cancel_requested_at <= completed_at))) AND ((completed_at IS NULL) OR (completed_at >= created_at AND completed_at <= updated_at AND (started_at IS NULL OR completed_at >= started_at))) AND ((terminal_reason_code IS NULL) OR (terminal_reason_code ~ '^[a-z][a-z0-9_.]{0,127}$'::text)) AND (((status = 'queued'::qp_feedback_cycle_status) AND decision IS NULL AND terminal_reason_code IS NULL AND started_at IS NULL AND completed_at IS NULL AND lease_owner IS NULL) OR ((status = 'running'::qp_feedback_cycle_status) AND decision IS NULL AND terminal_reason_code IS NULL AND started_at IS NOT NULL AND completed_at IS NULL AND lease_owner IS NOT NULL) OR ((status = 'succeeded'::qp_feedback_cycle_status) AND decision IS NOT NULL AND terminal_reason_code IS NOT NULL AND started_at IS NOT NULL AND completed_at IS NOT NULL AND lease_owner IS NULL) OR ((status = 'failed'::qp_feedback_cycle_status) AND decision IS NULL AND terminal_reason_code IS NOT NULL AND started_at IS NOT NULL AND completed_at IS NOT NULL AND lease_owner IS NULL) OR ((status = 'cancelled'::qp_feedback_cycle_status) AND decision IS NULL AND terminal_reason_code IS NOT NULL AND completed_at IS NOT NULL AND lease_owner IS NULL))))",
    },
    ConstraintSpec {
        name: "uq_quant_feedback_cycle_cutoff",
        table: "quant_feedback_cycle",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (feedback_cycle_id, label_cutoff)",
    },
    ConstraintSpec {
        name: "uq_quant_feedback_cycle_evaluation_lineage",
        table: "quant_feedback_cycle",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (feedback_cycle_id, research_profile_artifact_id, label_cutoff, champion_model_version_id, champion_serving_contract_hash, candidate_family_hash)",
    },
    ConstraintSpec {
        name: "uq_quant_training_dataset_evaluation_identity",
        table: "quant_training_dataset",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (training_dataset_id, purpose, dataset_hash, artifact_bytes_hash)",
    },
    ConstraintSpec {
        name: "fk_quant_feedback_cycle_profile",
        table: "quant_feedback_cycle",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (research_profile_artifact_id) REFERENCES public.research_profile_artifact(research_profile_artifact_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_feedback_cycle_champion",
        table: "quant_feedback_cycle",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (champion_model_version_id) REFERENCES public.quant_model_version(model_version_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "ck_quant_research_job_feedback_lineage",
        table: "quant_research_job",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((feedback_cycle_id IS NULL) = (feedback_stage IS NULL)) AND ((feedback_stage IS NULL) OR (feedback_stage <> 'trigger'::qp_feedback_stage)) AND ((parent_job_id IS NULL) OR (parent_job_id <> job_id))))",
    },
    ConstraintSpec {
        name: "uq_quant_research_job_feedback_lineage_key",
        table: "quant_research_job",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (job_id, feedback_cycle_id, feedback_stage)",
    },
    ConstraintSpec {
        name: "fk_quant_research_job_cycle",
        table: "quant_research_job",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (feedback_cycle_id) REFERENCES public.quant_feedback_cycle(feedback_cycle_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_research_job_parent",
        table: "quant_research_job",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (parent_job_id) REFERENCES public.quant_research_job(job_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_research_job_parent_lineage",
        table: "quant_research_job",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (parent_job_id, feedback_cycle_id, feedback_stage) REFERENCES public.quant_research_job(job_id, feedback_cycle_id, feedback_stage) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "ck_quant_feedback_stage_event",
        table: "quant_feedback_stage_event",
        kind: ConstraintKind::Check,
        definition: "CHECK ((event_sequence > 0 AND occurred_at <= created_at AND event_hash ~ '^blake3:[0-9a-f]{64}$'::text AND ((evidence_uri IS NULL AND evidence_hash IS NULL) OR (evidence_uri IS NOT NULL AND evidence_hash IS NOT NULL AND octet_length(evidence_uri) >= 1 AND octet_length(evidence_uri) <= 4096 AND evidence_uri ~ '^[a-z][a-z0-9+.-]*://.+$'::text AND evidence_hash ~ '^blake3:[0-9a-f]{64}$'::text)) AND ((actor IS NULL) OR (octet_length(actor) >= 1 AND octet_length(actor) <= 256 AND actor = btrim(actor) AND actor !~ '[[:cntrl:]]'::text)) AND ((reason_code IS NULL) OR (reason_code ~ '^[a-z][a-z0-9_.]{0,127}$'::text)) AND (((event_kind = 'triggered'::qp_feedback_stage_event_kind) AND stage = 'trigger'::qp_feedback_stage AND research_job_id IS NULL AND actor IS NOT NULL AND reason_code IS NOT NULL) OR ((event_kind = 'cancellation_requested'::qp_feedback_stage_event_kind) AND stage <> 'trigger'::qp_feedback_stage AND actor IS NOT NULL AND reason_code IS NOT NULL) OR ((event_kind = ANY (ARRAY['job_linked'::qp_feedback_stage_event_kind, 'started'::qp_feedback_stage_event_kind])) AND stage <> 'trigger'::qp_feedback_stage AND research_job_id IS NOT NULL AND actor IS NULL AND reason_code IS NULL) OR ((event_kind = 'succeeded'::qp_feedback_stage_event_kind) AND stage <> 'trigger'::qp_feedback_stage AND research_job_id IS NOT NULL AND actor IS NULL AND reason_code IS NULL AND evidence_uri IS NOT NULL) OR ((event_kind = ANY (ARRAY['failed'::qp_feedback_stage_event_kind, 'cancelled'::qp_feedback_stage_event_kind, 'lease_recovered'::qp_feedback_stage_event_kind])) AND stage <> 'trigger'::qp_feedback_stage AND research_job_id IS NOT NULL AND actor IS NULL AND reason_code IS NOT NULL))))",
    },
    ConstraintSpec {
        name: "fk_quant_feedback_stage_cycle",
        table: "quant_feedback_stage_event",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (feedback_cycle_id) REFERENCES public.quant_feedback_cycle(feedback_cycle_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_feedback_stage_job_lineage",
        table: "quant_feedback_stage_event",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (research_job_id, feedback_cycle_id, stage) REFERENCES public.quant_research_job(job_id, feedback_cycle_id, feedback_stage) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "ck_quant_feedback_event_outbox",
        table: "quant_feedback_event_outbox",
        kind: ConstraintKind::Check,
        definition: "CHECK ((revision > 0 AND publish_attempts >= 0 AND created_at <= updated_at AND ((claim_owner IS NULL) = (lease_expires_at IS NULL)) AND (published_at IS NULL OR (published_at >= created_at AND claim_owner IS NULL AND lease_expires_at IS NULL AND last_error IS NULL)) AND (last_error IS NULL OR (octet_length(last_error) >= 1 AND octet_length(last_error) <= 2048 AND last_error = btrim(last_error) AND last_error !~ '[[:cntrl:]]'::text))))",
    },
    ConstraintSpec {
        name: "ck_quant_drift_report",
        table: "quant_drift_report",
        kind: ConstraintKind::Check,
        definition: DRIFT_REPORT_CHECK,
    },
    ConstraintSpec {
        name: "fk_quant_drift_report_cycle",
        table: "quant_drift_report",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (feedback_cycle_id, label_cutoff) REFERENCES public.quant_feedback_cycle(feedback_cycle_id, label_cutoff) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "ck_quant_feedback_evaluation_use",
        table: "quant_feedback_evaluation_use",
        kind: ConstraintKind::Check,
        definition: "CHECK ((purpose = 'promotion_comparison'::qp_feedback_evaluation_purpose AND dataset_purpose = 'evaluation'::qp_dataset_purpose AND jsonb_typeof(profile_ref) = 'object'::text AND profile_ref ?& ARRAY['id'::text, 'version'::text, 'content_hash'::text] AND (profile_ref - ARRAY['id'::text, 'version'::text, 'content_hash'::text]) = '{}'::jsonb AND profile_ref ->> 'id'::text ~ '^[a-z0-9_]+$'::text AND (profile_ref ->> 'version'::text)::bigint > 0 AND research_profile_artifact_id = (((('rpa:'::text || (profile_ref ->> 'id'::text)) || ':'::text) || (profile_ref ->> 'version'::text)) || ':'::text) || (profile_ref ->> 'content_hash'::text) AND evaluation_window_start < evaluation_window_end AND evaluation_window_end <= label_cutoff AND label_cutoff <= reserved_at AND reserved_at = created_at AND evaluation_dataset_hash ~ '^blake3:[0-9a-f]{64}$'::text AND evaluation_artifact_bytes_hash ~ '^blake3:[0-9a-f]{64}$'::text AND cohort_manifest_hash ~ '^blake3:[0-9a-f]{64}$'::text AND champion_serving_contract_hash ~ '^blake3:[0-9a-f]{64}$'::text AND candidate_family_hash ~ '^blake3:[0-9a-f]{64}$'::text AND comparison_contract_hash ~ '^blake3:[0-9a-f]{64}$'::text AND semantic_use_hash ~ '^blake3:[0-9a-f]{64}$'::text AND octet_length(cpcv_artifact_uri) BETWEEN 1 AND 4096 AND cpcv_artifact_uri ~ '^[a-z][a-z0-9+.-]*://.+$'::text AND cpcv_artifact_hash ~ '^blake3:[0-9a-f]{64}$'::text AND evaluation_use_hash ~ '^blake3:[0-9a-f]{64}$'::text))",
    },
    ConstraintSpec {
        name: "fk_quant_feedback_evaluation_cycle",
        table: "quant_feedback_evaluation_use",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (feedback_cycle_id, research_profile_artifact_id, label_cutoff, champion_model_version_id, champion_serving_contract_hash, candidate_family_hash) REFERENCES public.quant_feedback_cycle(feedback_cycle_id, research_profile_artifact_id, label_cutoff, champion_model_version_id, champion_serving_contract_hash, candidate_family_hash) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_feedback_evaluation_profile",
        table: "quant_feedback_evaluation_use",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (research_profile_artifact_id) REFERENCES public.research_profile_artifact(research_profile_artifact_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_feedback_evaluation_dataset",
        table: "quant_feedback_evaluation_use",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (evaluation_dataset_id, dataset_purpose, evaluation_dataset_hash, evaluation_artifact_bytes_hash) REFERENCES public.quant_training_dataset(training_dataset_id, purpose, dataset_hash, artifact_bytes_hash) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_feedback_evaluation_champion",
        table: "quant_feedback_evaluation_use",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (champion_model_version_id) REFERENCES public.quant_model_version(model_version_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "ck_quant_feature_parity_candidate_ordinal",
        table: "quant_feature_parity_candidate",
        kind: ConstraintKind::Check,
        definition: "CHECK ((ordinal >= 0))",
    },
    ConstraintSpec {
        name: "ck_quant_calibration_artifact_payload_kind",
        table: "quant_calibration_artifact",
        kind: ConstraintKind::Check,
        definition: "CHECK (((jsonb_typeof(payload) = 'object'::text) AND ((payload ->> 'kind'::text) = (kind)::text) AND (jsonb_typeof((payload -> 'payload'::text)) = 'object'::text)))",
    },
    ConstraintSpec {
        name: "uq_quant_feature_parity_candidate_subject_market",
        table: "quant_feature_parity_candidate",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (parity_subject_id, market_id)",
    },
    ConstraintSpec {
        name: "ck_quant_feature_parity_subject_identity",
        table: "quant_feature_parity_subject",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((subject_kind = 'model_run'::qp_parity_subject_kind) AND (model_run_id IS NOT NULL) AND (recommendation_report_id IS NULL) AND (model_version_id IS NULL) AND (training_dataset_id IS NULL) AND (market_selection_id IS NOT NULL) AND (decision_at IS NOT NULL) AND (selection_hash IS NOT NULL)) OR ((subject_kind = 'recommendation_report'::qp_parity_subject_kind) AND (model_run_id IS NULL) AND (recommendation_report_id IS NOT NULL) AND (model_version_id IS NULL) AND (training_dataset_id IS NULL) AND (market_selection_id IS NOT NULL) AND (decision_at IS NOT NULL) AND (selection_hash IS NOT NULL)) OR ((subject_kind = 'model_version'::qp_parity_subject_kind) AND (model_run_id IS NULL) AND (recommendation_report_id IS NULL) AND (model_version_id IS NOT NULL) AND (training_dataset_id IS NOT NULL) AND (market_selection_id IS NULL) AND (decision_at IS NULL) AND (selection_hash IS NULL))))",
    },
    ConstraintSpec {
        name: "ck_quant_feature_parity_run_semantic_codes",
        table: "quant_feature_parity_run",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((acting_role)::text ~ '^[a-z][a-z0-9_]{0,63}$'::text) AND ((failure_code IS NULL) OR ((failure_code)::text ~ '^[a-z][a-z0-9_]{0,127}$'::text))))",
    },
    ConstraintSpec {
        name: "ck_quant_feature_parity_state_acting_role",
        table: "quant_feature_parity_state",
        kind: ConstraintKind::Check,
        definition: "CHECK (((acting_role IS NULL) OR ((acting_role)::text ~ '^[a-z][a-z0-9_]{0,63}$'::text)))",
    },
    ConstraintSpec {
        name: "ck_quant_feature_vector_documents",
        table: "quant_feature_vector",
        kind: ConstraintKind::Check,
        definition: "CHECK (((jsonb_typeof(decision_boundary) = 'object'::text) AND (jsonb_typeof(payload) = 'object'::text) AND (payload ? 'generic'::text) AND (payload ? 'domain'::text) AND (jsonb_typeof((payload -> 'generic'::text)) = 'object'::text) AND (jsonb_typeof((payload -> 'domain'::text)) = ANY (ARRAY['object'::text, 'null'::text])) AND (jsonb_typeof(source_refs) = 'array'::text) AND (jsonb_typeof(decision_capture) = 'object'::text) AND (jsonb_typeof((decision_capture -> 'snapshot'::text)) = 'object'::text) AND (decision_capture_hash ~ '^blake3:[0-9a-f]{64}$'::text)))",
    },
    ConstraintSpec {
        name: "ck_quant_market_linkage_outcome",
        table: "quant_market_linkage",
        kind: ConstraintKind::Check,
        definition: "CHECK (((jsonb_typeof(outcome) = 'object'::text) AND ((((status = 'unresolved'::qp_linkage_status) AND ((outcome ->> 'status'::text) = 'unresolved'::text) AND (jsonb_typeof((outcome -> 'reason'::text)) = 'object'::text) AND ((outcome -> 'reason'::text) ? 'code'::text) AND ((outcome #>> '{reason,code}'::text[]) = ANY (ARRAY['no_deterministic_template'::text, 'candidate_rejected'::text]))) OR ((status = ANY (ARRAY['resolved'::qp_linkage_status, 'overridden'::qp_linkage_status])) AND ((outcome ->> 'status'::text) = 'resolved'::text) AND (jsonb_typeof((outcome -> 'subject'::text)) = 'object'::text) AND (jsonb_typeof((outcome -> 'source_bindings'::text)) = 'array'::text) AND (jsonb_typeof((outcome -> 'grounding'::text)) = 'object'::text))))))",
    },
    ConstraintSpec {
        name: "ck_operation_log_detail_document",
        table: "operation_log",
        kind: ConstraintKind::Check,
        definition: "CHECK (((jsonb_typeof(detail) = 'object'::text) AND (pg_column_size(detail) <= 65536)))",
    },
    ConstraintSpec {
        name: "ck_quant_model_governance_audit_detail_action",
        table: "quant_model_governance_audit",
        kind: ConstraintKind::Check,
        definition: "CHECK (((jsonb_typeof(detail) = 'object'::text) AND ((detail ->> 'action'::text) = (action)::text)))",
    },
    ConstraintSpec {
        name: "ck_quant_model_run_terminal",
        table: "quant_model_run",
        kind: ConstraintKind::Check,
        definition: "CHECK (((window_start <= window_end) AND (window_end <= (started_at + '00:00:02'::interval)) AND (started_at IS NOT NULL) AND (input_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND (((status = 'running'::qp_model_run_status) AND (output_hash IS NULL) AND (error_code IS NULL) AND (error_message IS NULL) AND (finished_at IS NULL)) OR ((status = 'succeeded'::qp_model_run_status) AND (output_hash IS NOT NULL) AND (output_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND (error_code IS NULL) AND (error_message IS NULL) AND (finished_at IS NOT NULL) AND (finished_at >= started_at)) OR ((status = 'failed'::qp_model_run_status) AND (output_hash IS NULL) AND (error_code IS NOT NULL) AND (error_code <> 'cancelled_by_operator'::qp_model_run_error_code) AND (error_message IS NOT NULL) AND (char_length(btrim(error_message)) BETWEEN 1 AND 4096) AND (finished_at IS NOT NULL) AND (finished_at >= started_at)) OR ((status = 'cancelled'::qp_model_run_status) AND (output_hash IS NULL) AND (error_code = 'cancelled_by_operator'::qp_model_run_error_code) AND (error_message IS NOT NULL) AND (char_length(btrim(error_message)) BETWEEN 1 AND 4096) AND (finished_at IS NOT NULL) AND (finished_at >= started_at)))))",
    },
    ConstraintSpec {
        name: "quant_model_spec_check",
        table: "quant_model_spec",
        kind: ConstraintKind::Check,
        definition: "CHECK (((jsonb_typeof(input_contract) = 'object'::text) AND (jsonb_typeof((input_contract -> 'inputs'::text)) = 'array'::text) AND (jsonb_array_length((input_contract -> 'inputs'::text)) > 0) AND (jsonb_typeof(training_contract) = 'object'::text) AND (jsonb_typeof((training_contract -> 'target_label_name'::text)) = 'string'::text) AND ((length((training_contract ->> 'target_label_name'::text)) >= 1) AND (length((training_contract ->> 'target_label_name'::text)) <= 128)) AND ((((training_contract ->> 'validation_folds'::text))::integer >= 2) AND (((training_contract ->> 'validation_folds'::text))::integer <= 20)) AND (definition_hash ~ '^blake3:[0-9a-f]{64}$'::text)))",
    },
    ConstraintSpec {
        name: "ck_quant_model_spec_thesis",
        table: "quant_model_spec",
        kind: ConstraintKind::Check,
        definition: "CHECK (((jsonb_typeof(thesis) = 'object'::text) AND (jsonb_typeof((thesis -> 'summary'::text)) = 'string'::text) AND (char_length(btrim((thesis ->> 'summary'::text))) BETWEEN 1 AND 512) AND ((thesis ->> 'summary'::text) = btrim((thesis ->> 'summary'::text))) AND (jsonb_typeof((thesis -> 'hypothesis'::text)) = 'string'::text) AND (char_length(btrim((thesis ->> 'hypothesis'::text))) BETWEEN 1 AND 2048) AND ((thesis ->> 'hypothesis'::text) = btrim((thesis ->> 'hypothesis'::text))) AND (jsonb_typeof((thesis -> 'limitations'::text)) = 'array'::text) AND (jsonb_array_length((thesis -> 'limitations'::text)) BETWEEN 1 AND 16)))",
    },
    ConstraintSpec {
        name: "ck_quant_model_spec_authoring_provenance",
        table: "quant_model_spec",
        kind: ConstraintKind::Check,
        definition: "CHECK (((char_length(btrim(created_by_label)) >= 1) AND (char_length(btrim(created_by_label)) <= 256) AND ((created_by_role IS NULL) OR ((char_length(btrim(created_by_role)) >= 1) AND (char_length(btrim(created_by_role)) <= 128))) AND (char_length(btrim(reason)) >= 1) AND (char_length(btrim(reason)) <= 2048)))",
    },
    ConstraintSpec {
        name: "ck_quant_model_version_quality_gate_report",
        table: "quant_model_version",
        kind: ConstraintKind::Check,
        definition: "CHECK (((quality_gate_report IS NULL) OR ((jsonb_typeof(quality_gate_report) = 'object'::text) AND (((quality_gate_report ->> 'format_version'::text))::integer = 1) AND ((quality_gate_report -> 'subject'::text) ->> 'kind'::text) = 'model_version'::text AND ((((quality_gate_report -> 'subject'::text) ->> 'id'::text))::uuid = model_version_id) AND (jsonb_typeof((quality_gate_report -> 'gates'::text)) = 'array'::text) AND (jsonb_typeof((quality_gate_report -> 'hard_failures'::text)) = 'array'::text) AND (jsonb_typeof((quality_gate_report -> 'soft_warnings'::text)) = 'array'::text) AND (jsonb_typeof((quality_gate_report -> 'passed'::text)) = 'boolean'::text) AND ((quality_gate_report ->> 'report_hash'::text) ~ '^blake3:[0-9a-f]{64}$'::text))))",
    },
    ConstraintSpec {
        name: "ck_quant_model_version_serving_contract",
        table: "quant_model_version",
        kind: ConstraintKind::Check,
        definition: "CHECK (((jsonb_typeof(serving_contract) = 'object'::text) AND (serving_contract ?& ARRAY['contract_version'::text, 'contract_hash'::text, 'bindings'::text]) AND ((serving_contract - ARRAY['contract_version'::text, 'contract_hash'::text, 'bindings'::text]) = '{}'::jsonb) AND (jsonb_typeof((serving_contract -> 'contract_version'::text)) = 'number'::text) AND (((serving_contract ->> 'contract_version'::text))::numeric = 2::numeric) AND (jsonb_typeof((serving_contract -> 'contract_hash'::text)) = 'string'::text) AND (jsonb_typeof((serving_contract -> 'bindings'::text)) = 'object'::text) AND (octet_length(serving_contract_hash) = 32) AND ((serving_contract ->> 'contract_hash'::text) = ('blake3:'::text || encode(serving_contract_hash, 'hex'::text)))))",
    },
    ConstraintSpec {
        name: "ck_quant_model_version_training_objective",
        table: "quant_model_version",
        kind: ConstraintKind::Check,
        definition: MODEL_TRAINING_OBJECTIVE_CHECK,
    },
    ConstraintSpec {
        name: "ck_quant_model_version_metrics",
        table: "quant_model_version",
        kind: ConstraintKind::Check,
        definition: MODEL_VERSION_METRICS_CHECK,
    },
    ConstraintSpec {
        name: "ck_quant_model_version_derivation",
        table: "quant_model_version",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((derivation_kind = 'training'::qp_model_version_derivation_kind) AND (parent_model_version_id IS NULL) AND (calibration_artifact_id IS NULL) AND (derivation_evidence_hash IS NULL)) OR ((derivation_kind = 'return_calibration'::qp_model_version_derivation_kind) AND (parent_model_version_id IS NOT NULL) AND (parent_model_version_id <> model_version_id) AND (calibration_artifact_id IS NOT NULL) AND (derivation_evidence_hash ~ '^blake3:[0-9a-f]{64}$'::text))))",
    },
    ConstraintSpec {
        name: "uq_quant_recommendation_identity",
        table: "quant_recommendation",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (recommendation_id, market_id, token_id)",
    },
    ConstraintSpec {
        name: "uq_quant_order_intent_execution_outcome_lineage",
        table: "quant_order_intent",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (order_intent_id, recommendation_id, execution_account_id, runtime_mode)",
    },
    ConstraintSpec {
        name: "uq_quant_execution_order_execution_outcome_lineage",
        table: "quant_execution_order",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (execution_order_id, order_intent_id, market_id, token_id)",
    },
    ConstraintSpec {
        name: "uq_quant_reconciliation_execution_outcome_lineage",
        table: "quant_reconciliation",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (reconciliation_id, execution_order_id, order_intent_id)",
    },
    ConstraintSpec {
        name: "uq_quant_position_execution_outcome_lineage",
        table: "quant_position",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (position_id, order_intent_id, execution_account_id, market_id, token_id)",
    },
    ConstraintSpec {
        name: "uq_quant_recommendation_execution_outcome_intent",
        table: "quant_recommendation_execution_outcome",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (order_intent_id)",
    },
    ConstraintSpec {
        name: "uq_quant_recommendation_execution_outcome_entry_order",
        table: "quant_recommendation_execution_outcome",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (entry_execution_order_id)",
    },
    ConstraintSpec {
        name: "uq_quant_recommendation_execution_outcome_entry_reconciliation",
        table: "quant_recommendation_execution_outcome",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (entry_reconciliation_id)",
    },
    ConstraintSpec {
        name: "uq_quant_recommendation_execution_outcome_position",
        table: "quant_recommendation_execution_outcome",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (position_id)",
    },
    ConstraintSpec {
        name: "fk_quant_recommendation_execution_outcome_recommendation_identity",
        table: "quant_recommendation_execution_outcome",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (recommendation_id, market_id, token_id) REFERENCES public.quant_recommendation(recommendation_id, market_id, token_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_recommendation_execution_outcome_intent_lineage",
        table: "quant_recommendation_execution_outcome",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (order_intent_id, recommendation_id, execution_account_id, runtime_mode) REFERENCES public.quant_order_intent(order_intent_id, recommendation_id, execution_account_id, runtime_mode) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_recommendation_execution_outcome_entry_order_lineage",
        table: "quant_recommendation_execution_outcome",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (entry_execution_order_id, order_intent_id, market_id, token_id) REFERENCES public.quant_execution_order(execution_order_id, order_intent_id, market_id, token_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_recommendation_execution_outcome_entry_reconciliation_lineage",
        table: "quant_recommendation_execution_outcome",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (entry_reconciliation_id, entry_execution_order_id, order_intent_id) REFERENCES public.quant_reconciliation(reconciliation_id, execution_order_id, order_intent_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_recommendation_execution_outcome_position_lineage",
        table: "quant_recommendation_execution_outcome",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (position_id, order_intent_id, execution_account_id, market_id, token_id) REFERENCES public.quant_position(position_id, order_intent_id, execution_account_id, market_id, token_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "ck_quant_recommendation_execution_outcome_contract",
        table: "quant_recommendation_execution_outcome",
        kind: ConstraintKind::Check,
        definition: "CHECK (((char_length(market_id) > 0) AND (char_length(token_id) > 0) AND (runtime_mode = ANY (ARRAY['semi_auto'::qp_quant_runtime_mode, 'auto_execution'::qp_quant_runtime_mode])) AND (requested_shares > (0)::numeric) AND (filled_shares >= (0)::numeric) AND (filled_shares <= requested_shares) AND ((entry_avg_price IS NULL) OR ((entry_avg_price >= (0)::numeric) AND (entry_avg_price <= (1)::numeric))) AND ((exit_avg_price IS NULL) OR ((exit_avg_price >= (0)::numeric) AND (exit_avg_price <= (1)::numeric))) AND ((entry_fee_usd IS NULL) OR (entry_fee_usd >= (0)::numeric)) AND ((exit_fee_usd IS NULL) OR (exit_fee_usd >= (0)::numeric)) AND ((settlement_payout_usd IS NULL) OR (settlement_payout_usd >= (0)::numeric)) AND ((max_adverse_excursion_bps IS NULL) OR (max_adverse_excursion_bps <= (0)::numeric)) AND ((max_favorable_excursion_bps IS NULL) OR (max_favorable_excursion_bps >= (0)::numeric)) AND (terminal_at <= source_observed_at) AND (source_observed_at <= available_at) AND (available_at <= created_at) AND (execution_fact_schema_version > 0) AND (source_checkpoint_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND (execution_fact_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND (outcome_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND (((terminal_state = 'unfilled'::qp_recommendation_execution_terminal_state) AND (filled_shares = (0)::numeric) AND (no_fill_reason IS NOT NULL) AND (entry_order_state = ANY (ARRAY['cancelled'::qp_execution_order_state, 'failed'::qp_execution_order_state])) AND (position_id IS NULL) AND (entry_avg_price IS NULL) AND (entry_filled_at IS NULL) AND (position_terminal_state IS NULL) AND (exit_reason IS NULL) AND (exit_filled_shares IS NULL) AND (exit_avg_price IS NULL) AND (exit_fee_usd IS NULL) AND (exit_at IS NULL) AND (settlement_payout_usd IS NULL) AND (realized_pnl_usd IS NULL) AND (max_adverse_excursion_bps IS NULL) AND (max_favorable_excursion_bps IS NULL)) OR ((terminal_state = ANY (ARRAY['partially_filled'::qp_recommendation_execution_terminal_state, 'fully_filled'::qp_recommendation_execution_terminal_state])) AND (no_fill_reason IS NULL) AND (position_id IS NOT NULL) AND (entry_avg_price IS NOT NULL) AND (entry_filled_at IS NOT NULL) AND (entry_filled_at <= terminal_at) AND (position_terminal_state = ANY (ARRAY['closed'::qp_position_ledger_state, 'settled'::qp_position_ledger_state])) AND (realized_pnl_usd IS NOT NULL) AND (((terminal_state = 'partially_filled'::qp_recommendation_execution_terminal_state) AND (filled_shares > (0)::numeric) AND (filled_shares < requested_shares) AND (entry_order_state = 'partially_filled'::qp_execution_order_state)) OR ((terminal_state = 'fully_filled'::qp_recommendation_execution_terminal_state) AND (filled_shares = requested_shares) AND (entry_order_state = 'filled'::qp_execution_order_state))) AND (((position_terminal_state = 'closed'::qp_position_ledger_state) AND (exit_reason IS NOT NULL) AND (exit_reason <> 'resolution_redeem'::qp_exit_reason) AND (exit_filled_shares = filled_shares) AND (exit_avg_price IS NOT NULL) AND (exit_at = terminal_at) AND (settlement_payout_usd IS NULL)) OR ((position_terminal_state = 'settled'::qp_position_ledger_state) AND (exit_reason = 'resolution_redeem'::qp_exit_reason) AND (settlement_payout_usd IS NOT NULL) AND (((exit_filled_shares IS NULL) AND (exit_avg_price IS NULL) AND (exit_fee_usd IS NULL) AND (exit_at IS NULL)) OR ((exit_filled_shares > (0)::numeric) AND (exit_filled_shares < filled_shares) AND (exit_avg_price IS NOT NULL) AND (exit_at IS NOT NULL) AND (exit_at <= terminal_at)))))))))",
    },
    ConstraintSpec {
        name: "fk_quant_recommendation_resolution_outcome_identity",
        table: "quant_recommendation_resolution_outcome",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (recommendation_id, market_id, token_id) REFERENCES public.quant_recommendation(recommendation_id, market_id, token_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "ck_quant_recommendation_resolution_outcome_contract",
        table: "quant_recommendation_resolution_outcome",
        kind: ConstraintKind::Check,
        definition: "CHECK (((char_length(market_id) > 0) AND (char_length(token_id) > 0) AND (resolved_at <= source_observed_at) AND (source_observed_at <= available_at) AND (available_at <= created_at) AND (resolution_fact_log_index >= 0) AND (resolution_fact_schema_version > 0) AND (source_checkpoint_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND (resolution_fact_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND (outcome_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND (token_payout_ratio >= (0)::numeric) AND (token_payout_ratio <= (1)::numeric) AND (((resolution_kind = 'winner_take_all'::qp_recommendation_resolution_kind) AND (token_payout_ratio = ANY (ARRAY[(0)::numeric, (1)::numeric]))) OR ((resolution_kind = 'split_payout'::qp_recommendation_resolution_kind) AND (token_payout_ratio > (0)::numeric) AND (token_payout_ratio < (1)::numeric)))))",
    },
    ConstraintSpec {
        name: "quant_recommendation_report_capital_base_usd_check",
        table: "quant_recommendation_report",
        kind: ConstraintKind::Check,
        definition: "CHECK ((capital_base_usd >= (0)::numeric))",
    },
    ConstraintSpec {
        name: "quant_recommendation_report_check1",
        table: "quant_recommendation_report",
        kind: ConstraintKind::Check,
        definition: "CHECK (((valid_until IS NULL) OR (valid_until > decision_at)))",
    },
    ConstraintSpec {
        name: "quant_recommendation_report_check10",
        table: "quant_recommendation_report",
        kind: ConstraintKind::Check,
        definition: "CHECK (((published_at IS NULL) OR (published_at >= decision_at)))",
    },
    ConstraintSpec {
        name: "quant_recommendation_report_check11",
        table: "quant_recommendation_report",
        kind: ConstraintKind::Check,
        definition: "CHECK (((superseded_at IS NULL) OR (superseded_at >= published_at)))",
    },
    ConstraintSpec {
        name: "quant_recommendation_report_check12",
        table: "quant_recommendation_report",
        kind: ConstraintKind::Check,
        definition: "CHECK (((obsoleted_at IS NULL) OR (obsoleted_at >= decision_at)))",
    },
    ConstraintSpec {
        name: "quant_recommendation_report_check13",
        table: "quant_recommendation_report",
        kind: ConstraintKind::Check,
        definition: "CHECK (((expired_at IS NULL) OR (expired_at >= published_at)))",
    },
    ConstraintSpec {
        name: "quant_recommendation_report_check14",
        table: "quant_recommendation_report",
        kind: ConstraintKind::Check,
        definition: "CHECK (((revoked_at IS NULL) OR (revoked_at >= decision_at)))",
    },
    ConstraintSpec {
        name: "quant_recommendation_report_check2",
        table: "quant_recommendation_report",
        kind: ConstraintKind::Check,
        definition: "CHECK (((successor_report_id IS NULL) OR (successor_report_id <> recommendation_report_id)))",
    },
    ConstraintSpec {
        name: "quant_recommendation_report_check3",
        table: "quant_recommendation_report",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((status = ANY (ARRAY['prepared'::qp_recommendation_report_status, 'published'::qp_recommendation_report_status])) AND (status_reason IS NULL)) OR ((status <> ALL (ARRAY['prepared'::qp_recommendation_report_status, 'published'::qp_recommendation_report_status])) AND (status_reason IS NOT NULL))))",
    },
    ConstraintSpec {
        name: "quant_recommendation_report_check4",
        table: "quant_recommendation_report",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((status = 'prepared'::qp_recommendation_report_status) AND (published_at IS NULL) AND (successor_report_id IS NULL) AND (superseded_at IS NULL) AND (obsoleted_at IS NULL) AND (revoked_at IS NULL) AND (expired_at IS NULL)) OR (status <> 'prepared'::qp_recommendation_report_status)))",
    },
    ConstraintSpec {
        name: "quant_recommendation_report_check5",
        table: "quant_recommendation_report",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((status = 'published'::qp_recommendation_report_status) AND (published_at IS NOT NULL) AND (successor_report_id IS NULL) AND (superseded_at IS NULL) AND (obsoleted_at IS NULL) AND (revoked_at IS NULL) AND (expired_at IS NULL)) OR (status <> 'published'::qp_recommendation_report_status)))",
    },
    ConstraintSpec {
        name: "quant_recommendation_report_check6",
        table: "quant_recommendation_report",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((status = 'superseded'::qp_recommendation_report_status) AND (published_at IS NOT NULL) AND (successor_report_id IS NOT NULL) AND (superseded_at IS NOT NULL) AND (obsoleted_at IS NULL) AND (revoked_at IS NULL) AND (expired_at IS NULL)) OR (status <> 'superseded'::qp_recommendation_report_status)))",
    },
    ConstraintSpec {
        name: "quant_recommendation_report_check7",
        table: "quant_recommendation_report",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((status = 'obsolete'::qp_recommendation_report_status) AND (published_at IS NULL) AND (successor_report_id IS NOT NULL) AND (superseded_at IS NULL) AND (obsoleted_at IS NOT NULL) AND (revoked_at IS NULL) AND (expired_at IS NULL)) OR (status <> 'obsolete'::qp_recommendation_report_status)))",
    },
    ConstraintSpec {
        name: "quant_recommendation_report_check8",
        table: "quant_recommendation_report",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((status = 'revoked'::qp_recommendation_report_status) AND (successor_report_id IS NULL) AND (superseded_at IS NULL) AND (obsoleted_at IS NULL) AND (revoked_at IS NOT NULL) AND (expired_at IS NULL)) OR (status <> 'revoked'::qp_recommendation_report_status)))",
    },
    ConstraintSpec {
        name: "quant_recommendation_report_check9",
        table: "quant_recommendation_report",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((status = 'expired'::qp_recommendation_report_status) AND (published_at IS NOT NULL) AND (successor_report_id IS NULL) AND (superseded_at IS NULL) AND (obsoleted_at IS NULL) AND (revoked_at IS NULL) AND (expired_at IS NOT NULL)) OR (status <> 'expired'::qp_recommendation_report_status)))",
    },
    ConstraintSpec {
        name: "quant_recommendation_report_horizon_secs_check",
        table: "quant_recommendation_report",
        kind: ConstraintKind::Check,
        definition: "CHECK ((horizon_secs > 0))",
    },
    ConstraintSpec {
        name: "quant_recommendation_report_status_reason_check",
        table: "quant_recommendation_report",
        kind: ConstraintKind::Check,
        definition: "CHECK (((status_reason IS NULL) OR ((char_length(status_reason) >= 1) AND (char_length(status_reason) <= 4096))))",
    },
    ConstraintSpec {
        name: "quant_recommendation_report_top_n_check",
        table: "quant_recommendation_report",
        kind: ConstraintKind::Check,
        definition: "CHECK (((top_n > 0) AND (top_n <= 1000)))",
    },
    ConstraintSpec {
        name: "quant_report_fact_delivery_check",
        table: "quant_report_fact_delivery",
        kind: ConstraintKind::Check,
        definition: "CHECK (((bundle_bytes > 0) AND (recommendation_row_count >= 0) AND (funnel_row_count >= 0) AND (attempt_count >= 0)))",
    },
    ConstraintSpec {
        name: "quant_report_fact_delivery_check1",
        table: "quant_report_fact_delivery",
        kind: ConstraintKind::Check,
        definition: "CHECK (((claim_owner IS NULL) = (lease_expires_at IS NULL)))",
    },
    ConstraintSpec {
        name: "quant_report_fact_delivery_check2",
        table: "quant_report_fact_delivery",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((status = 'pending'::qp_report_fact_delivery_status) AND (attempt_count = 0) AND (claim_owner IS NULL) AND (next_attempt_at IS NULL) AND (last_error IS NULL) AND (verified_at IS NULL) AND (announced_at IS NULL)) OR (status <> 'pending'::qp_report_fact_delivery_status)))",
    },
    ConstraintSpec {
        name: "quant_report_fact_delivery_check3",
        table: "quant_report_fact_delivery",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((status = 'delivering'::qp_report_fact_delivery_status) AND (attempt_count > 0) AND (claim_owner IS NOT NULL) AND (next_attempt_at IS NULL) AND (last_error IS NULL) AND (verified_at IS NULL) AND (announced_at IS NULL)) OR (status <> 'delivering'::qp_report_fact_delivery_status)))",
    },
    ConstraintSpec {
        name: "quant_report_fact_delivery_check4",
        table: "quant_report_fact_delivery",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((status = 'retrying'::qp_report_fact_delivery_status) AND (attempt_count > 0) AND (claim_owner IS NULL) AND (next_attempt_at IS NOT NULL) AND (last_error IS NOT NULL) AND (verified_at IS NULL) AND (announced_at IS NULL)) OR (status <> 'retrying'::qp_report_fact_delivery_status)))",
    },
    ConstraintSpec {
        name: "quant_report_fact_delivery_check5",
        table: "quant_report_fact_delivery",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((status = 'failed'::qp_report_fact_delivery_status) AND (attempt_count > 0) AND (claim_owner IS NULL) AND (next_attempt_at IS NULL) AND (last_error IS NOT NULL) AND (verified_at IS NULL) AND (announced_at IS NULL)) OR (status <> 'failed'::qp_report_fact_delivery_status)))",
    },
    ConstraintSpec {
        name: "quant_report_fact_delivery_check6",
        table: "quant_report_fact_delivery",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((status = 'verified'::qp_report_fact_delivery_status) AND (attempt_count > 0) AND (next_attempt_at IS NULL) AND (last_error IS NULL) AND (verified_at IS NOT NULL)) OR (status <> 'verified'::qp_report_fact_delivery_status)))",
    },
    ConstraintSpec {
        name: "quant_report_fact_delivery_check7",
        table: "quant_report_fact_delivery",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((status = 'cancelled'::qp_report_fact_delivery_status) AND (claim_owner IS NULL) AND (next_attempt_at IS NULL) AND (verified_at IS NULL) AND (announced_at IS NULL)) OR (status <> 'cancelled'::qp_report_fact_delivery_status)))",
    },
    ConstraintSpec {
        name: "quant_report_fact_delivery_check8",
        table: "quant_report_fact_delivery",
        kind: ConstraintKind::Check,
        definition: "CHECK (((announced_at IS NULL) OR ((status = 'verified'::qp_report_fact_delivery_status) AND (claim_owner IS NULL) AND (announced_at >= verified_at))))",
    },
    ConstraintSpec {
        name: "quant_report_fact_delivery_last_error_check",
        table: "quant_report_fact_delivery",
        kind: ConstraintKind::Check,
        definition: "CHECK (((last_error IS NULL) OR ((char_length(last_error) >= 1) AND (char_length(last_error) <= 4096))))",
    },
    ConstraintSpec {
        name: "quant_report_run_check",
        table: "quant_report_run",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((trigger_kind = 'scheduled'::qp_report_trigger_kind) AND (schedule_id IS NOT NULL) AND (request_id IS NULL) AND (scheduled_for IS NOT NULL)) OR ((trigger_kind = 'ad_hoc'::qp_report_trigger_kind) AND (schedule_id IS NULL) AND (request_id IS NOT NULL) AND (scheduled_for IS NULL))))",
    },
    ConstraintSpec {
        name: "quant_report_run_check1",
        table: "quant_report_run",
        kind: ConstraintKind::Check,
        definition: "CHECK (((status <> 'queued'::qp_report_run_status) OR (trigger_kind = 'ad_hoc'::qp_report_trigger_kind) OR ((top_n IS NULL) AND (knowledge_lag_secs IS NULL))))",
    },
    ConstraintSpec {
        name: "quant_report_run_check10",
        table: "quant_report_run",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((status = 'succeeded'::qp_report_run_status) AND (started_at IS NOT NULL) AND (decision_at IS NOT NULL) AND (heartbeat_at IS NOT NULL) AND (decision_policy_snapshot_id IS NOT NULL) AND (top_n IS NOT NULL) AND (knowledge_lag_secs IS NOT NULL) AND (terminal_reason IS NULL) AND (error_code IS NULL) AND (error_summary IS NULL)) OR (status <> 'succeeded'::qp_report_run_status)))",
    },
    ConstraintSpec {
        name: "quant_report_run_check11",
        table: "quant_report_run",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((status = 'failed'::qp_report_run_status) AND (started_at IS NOT NULL) AND (decision_at IS NOT NULL) AND (heartbeat_at IS NOT NULL) AND (decision_policy_snapshot_id IS NOT NULL) AND (top_n IS NOT NULL) AND (knowledge_lag_secs IS NOT NULL) AND (terminal_reason = 'build_failed'::qp_report_run_terminal_reason) AND (error_code IS NOT NULL) AND (error_summary IS NOT NULL)) OR (status <> 'failed'::qp_report_run_status)))",
    },
    ConstraintSpec {
        name: "quant_report_run_check12",
        table: "quant_report_run",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((status = 'abandoned'::qp_report_run_status) AND (started_at IS NOT NULL) AND (decision_at IS NOT NULL) AND (heartbeat_at IS NOT NULL) AND (decision_policy_snapshot_id IS NOT NULL) AND (top_n IS NOT NULL) AND (knowledge_lag_secs IS NOT NULL) AND (terminal_reason = 'lease_expired'::qp_report_run_terminal_reason) AND (error_code IS NULL) AND (error_summary IS NULL)) OR (status <> 'abandoned'::qp_report_run_status)))",
    },
    ConstraintSpec {
        name: "quant_report_run_check13",
        table: "quant_report_run",
        kind: ConstraintKind::Check,
        definition: "CHECK (((terminal_reason <> 'queue_expired'::qp_report_run_terminal_reason) OR (trigger_kind = 'ad_hoc'::qp_report_trigger_kind)))",
    },
    ConstraintSpec {
        name: "quant_report_run_check14",
        table: "quant_report_run",
        kind: ConstraintKind::Check,
        definition: "CHECK (((terminal_reason <> ALL (ARRAY['coalesced_by_newer_occurrence'::qp_report_run_terminal_reason, 'schedule_reconfigured'::qp_report_run_terminal_reason])) OR (trigger_kind = 'scheduled'::qp_report_trigger_kind)))",
    },
    ConstraintSpec {
        name: "quant_report_run_check15",
        table: "quant_report_run",
        kind: ConstraintKind::Check,
        definition: "CHECK (((started_at IS NULL) OR ((decision_at = started_at) AND (requested_at <= started_at))))",
    },
    ConstraintSpec {
        name: "quant_report_run_check16",
        table: "quant_report_run",
        kind: ConstraintKind::Check,
        definition: "CHECK (((finished_at IS NULL) OR (started_at IS NULL) OR (finished_at >= started_at)))",
    },
    ConstraintSpec {
        name: "quant_report_run_check17",
        table: "quant_report_run",
        kind: ConstraintKind::Check,
        definition: "CHECK (((lease_expires_at IS NULL) OR (lease_expires_at > heartbeat_at)))",
    },
    ConstraintSpec {
        name: "quant_report_run_check2",
        table: "quant_report_run",
        kind: ConstraintKind::Check,
        definition: "CHECK (((char_length(trigger_key) >= 1) AND (char_length(trigger_key) <= 512) AND ((schedule_id IS NULL) OR ((char_length(schedule_id) >= 1) AND (char_length(schedule_id) <= 128))) AND ((request_id IS NULL) OR ((char_length(request_id) >= 1) AND (char_length(request_id) <= 256)))))",
    },
    ConstraintSpec {
        name: "quant_report_run_check3",
        table: "quant_report_run",
        kind: ConstraintKind::Check,
        definition: "CHECK (((retry_of_run_id IS NULL) OR (trigger_kind = 'ad_hoc'::qp_report_trigger_kind)))",
    },
    ConstraintSpec {
        name: "quant_report_run_check4",
        table: "quant_report_run",
        kind: ConstraintKind::Check,
        definition: "CHECK (((scheduled_for IS NULL) OR (scheduled_for <= requested_at)))",
    },
    ConstraintSpec {
        name: "quant_report_run_check5",
        table: "quant_report_run",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((status = 'succeeded'::qp_report_run_status) AND (output_report_id IS NOT NULL)) OR ((status <> 'succeeded'::qp_report_run_status) AND (output_report_id IS NULL))))",
    },
    ConstraintSpec {
        name: "quant_report_run_check6",
        table: "quant_report_run",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((status = 'running'::qp_report_run_status) AND (lease_owner IS NOT NULL) AND (started_at IS NOT NULL) AND (decision_at IS NOT NULL) AND (heartbeat_at IS NOT NULL) AND (lease_expires_at IS NOT NULL) AND (decision_policy_snapshot_id IS NOT NULL) AND (top_n IS NOT NULL) AND (knowledge_lag_secs IS NOT NULL) AND (finished_at IS NULL) AND (terminal_reason IS NULL)) OR ((status <> 'running'::qp_report_run_status) AND (lease_owner IS NULL) AND (lease_expires_at IS NULL))))",
    },
    ConstraintSpec {
        name: "quant_report_run_check7",
        table: "quant_report_run",
        kind: ConstraintKind::Check,
        definition: "CHECK (((status = ANY (ARRAY['succeeded'::qp_report_run_status, 'failed'::qp_report_run_status, 'skipped'::qp_report_run_status, 'abandoned'::qp_report_run_status])) = (finished_at IS NOT NULL)))",
    },
    ConstraintSpec {
        name: "quant_report_run_check8",
        table: "quant_report_run",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((status = 'queued'::qp_report_run_status) AND (started_at IS NULL) AND (decision_at IS NULL) AND (heartbeat_at IS NULL) AND (finished_at IS NULL) AND (decision_policy_snapshot_id IS NULL) AND (output_report_id IS NULL) AND (terminal_reason IS NULL) AND (error_code IS NULL) AND (error_summary IS NULL)) OR (status <> 'queued'::qp_report_run_status)))",
    },
    ConstraintSpec {
        name: "quant_report_run_check9",
        table: "quant_report_run",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((status = 'skipped'::qp_report_run_status) AND (started_at IS NULL) AND (decision_at IS NULL) AND (heartbeat_at IS NULL) AND (decision_policy_snapshot_id IS NULL) AND (output_report_id IS NULL) AND (terminal_reason = ANY (ARRAY['coalesced_by_newer_occurrence'::qp_report_run_terminal_reason, 'schedule_reconfigured'::qp_report_run_terminal_reason, 'queue_expired'::qp_report_run_terminal_reason])) AND (error_code IS NULL) AND (error_summary IS NULL)) OR (status <> 'skipped'::qp_report_run_status)))",
    },
    ConstraintSpec {
        name: "quant_report_run_error_code_check",
        table: "quant_report_run",
        kind: ConstraintKind::Check,
        definition: "CHECK (((error_code IS NULL) OR ((error_code)::text ~ '^[a-z][a-z0-9_]{0,127}$'::text)))",
    },
    ConstraintSpec {
        name: "quant_report_run_error_summary_check",
        table: "quant_report_run",
        kind: ConstraintKind::Check,
        definition: "CHECK (((error_summary IS NULL) OR ((char_length(error_summary) >= 1) AND (char_length(error_summary) <= 4096))))",
    },
    ConstraintSpec {
        name: "ck_role_code_canonical",
        table: "role",
        kind: ConstraintKind::Check,
        definition: "CHECK (((code)::text ~ '^[a-z][a-z0-9_]{0,63}$'::text))",
    },
    ConstraintSpec {
        name: "ck_quant_report_run_trigger_key",
        table: "quant_report_run",
        kind: ConstraintKind::Check,
        definition: "CHECK (((length((trigger_key)::text) >= 1) AND (length((trigger_key)::text) <= 256) AND (octet_length((trigger_key)::text) = length((trigger_key)::text)) AND ((trigger_key)::text ~ '^[[:graph:]]+$'::text) AND (position(chr(92) in (trigger_key)::text) = 0) AND (position(chr(34) in (trigger_key)::text) = 0)))",
    },
    ConstraintSpec {
        name: "quant_report_run_knowledge_lag_secs_check",
        table: "quant_report_run",
        kind: ConstraintKind::Check,
        definition: "CHECK (((knowledge_lag_secs IS NULL) OR (knowledge_lag_secs >= 0)))",
    },
    ConstraintSpec {
        name: "quant_report_run_top_n_check",
        table: "quant_report_run",
        kind: ConstraintKind::Check,
        definition: "CHECK (((top_n IS NULL) OR ((top_n > 0) AND (top_n <= 1000))))",
    },
    ConstraintSpec {
        name: "quant_report_schedule_gap_check",
        table: "quant_report_schedule_gap",
        kind: ConstraintKind::Check,
        definition: "CHECK ((first_scheduled_for <= last_scheduled_for))",
    },
    ConstraintSpec {
        name: "quant_report_schedule_gap_detail_check",
        table: "quant_report_schedule_gap",
        kind: ConstraintKind::Check,
        definition: "CHECK (((detail IS NULL) OR ((char_length(detail) >= 1) AND (char_length(detail) <= 4096))))",
    },
    ConstraintSpec {
        name: "quant_report_schedule_gap_missed_count_check",
        table: "quant_report_schedule_gap",
        kind: ConstraintKind::Check,
        definition: "CHECK ((missed_count > 0))",
    },
    ConstraintSpec {
        name: "ck_quant_shadow_comparison_generation_identity",
        table: "quant_shadow_comparison",
        kind: ConstraintKind::Check,
        definition: "CHECK (((active_model_version_id <> shadow_model_version_id) AND (active_serving_contract_hash <> shadow_serving_contract_hash) AND (active_serving_contract_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND (shadow_serving_contract_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND (decision_policy_snapshot_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND (policy_bundle_generation > 0)))",
    },
    ConstraintSpec {
        name: "quant_research_readiness_evidence_check",
        table: "quant_research_readiness_evidence",
        kind: ConstraintKind::Check,
        definition: "CHECK (((window_start < window_end) AND (window_end <= observed_at) AND (observed_at < expires_at) AND (length((artifact_version)::text) BETWEEN 1 AND 256) AND (octet_length((artifact_version)::text) = length((artifact_version)::text)) AND ((artifact_version)::text ~ '^[[:graph:]]+$'::text) AND (position(chr(92) in (artifact_version)::text) = 0) AND (position(chr(34) in (artifact_version)::text) = 0) AND (length((attestation_key_id)::text) BETWEEN 1 AND 256) AND (octet_length((attestation_key_id)::text) = length((attestation_key_id)::text)) AND ((attestation_key_id)::text ~ '^[[:graph:]]+$'::text) AND (position(chr(92) in (attestation_key_id)::text) = 0) AND (position(chr(34) in (attestation_key_id)::text) = 0)))",
    },
    ConstraintSpec {
        name: "ck_quant_research_job_params_kind",
        table: "quant_research_job",
        kind: ConstraintKind::Check,
        definition: "CHECK (((jsonb_typeof(params_json) = 'object'::text) AND (jsonb_typeof((params_json -> 'params'::text)) = 'object'::text) AND ((params_json ->> 'kind'::text) = (kind)::text)))",
    },
    ConstraintSpec {
        name: "ck_quant_research_job_result_reference",
        table: "quant_research_job",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((result_kind IS NULL) = (result_ref IS NULL)) AND ((status = 'succeeded'::qp_research_job_status) OR ((result_kind IS NULL) AND (result_ref IS NULL))) AND ((result_kind IS NULL) OR ((kind = 'dataset_build'::qp_research_job_kind) AND (result_kind = 'training_dataset'::qp_research_job_result_kind)) OR ((kind = 'model_train'::qp_research_job_kind) AND (result_kind = 'model_version'::qp_research_job_result_kind)) OR ((kind = 'backtest'::qp_research_job_kind) AND (result_kind = 'backtest_report'::qp_research_job_result_kind)) OR ((kind = 'cpcv_backtest'::qp_research_job_kind) AND (result_kind = 'backtest_path_set'::qp_research_job_result_kind)) OR ((kind = ANY (ARRAY['bias_table_fit'::qp_research_job_kind, 'model_calibration_fit'::qp_research_job_kind])) AND (result_kind = 'calibration_artifact'::qp_research_job_result_kind)) OR ((kind = 'feature_parity'::qp_research_job_kind) AND (result_kind = 'feature_parity_run'::qp_research_job_result_kind)) OR ((kind = 'feedback_coverage'::qp_research_job_kind) AND (result_kind = 'feedback_coverage_artifact'::qp_research_job_result_kind)) OR ((kind = 'feedback_drift'::qp_research_job_kind) AND (result_kind = 'feedback_drift_artifact'::qp_research_job_result_kind)) OR ((kind = ANY (ARRAY['feedback_dataset_seal'::qp_research_job_kind, 'feedback_training'::qp_research_job_kind, 'feedback_calibration'::qp_research_job_kind, 'feedback_cpcv'::qp_research_job_kind])) AND (result_kind = 'feedback_learning_stage_artifact'::qp_research_job_result_kind)) OR ((kind = 'feedback_comparison'::qp_research_job_kind) AND (result_kind = 'feedback_comparison_artifact'::qp_research_job_result_kind)) OR ((kind = 'feedback_shadow_replay'::qp_research_job_kind) AND (result_kind = 'feedback_shadow_replay_artifact'::qp_research_job_result_kind)) OR ((kind = 'feedback_decision'::qp_research_job_kind) AND (result_kind = 'feedback_decision_artifact'::qp_research_job_result_kind)) OR ((kind = 'trade_policy_fit'::qp_research_job_kind) AND (result_kind = 'trade_policy_artifact'::qp_research_job_result_kind)) OR ((kind = 'trade_policy_validation'::qp_research_job_kind) AND (result_kind = 'trade_policy_validation_run'::qp_research_job_result_kind)))))",
    },
    ConstraintSpec {
        name: "ck_quant_research_job_artifact_reference",
        table: "quant_research_job",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((result_artifact_uri IS NULL) = (result_artifact_hash IS NULL)) AND ((result_artifact_uri IS NULL) OR ((octet_length(result_artifact_uri) >= 1) AND (octet_length(result_artifact_uri) <= 4096) AND (result_artifact_uri ~ '^[a-z][a-z0-9+.-]*://.+$'::text))) AND ((result_artifact_hash IS NULL) OR (result_artifact_hash ~ '^blake3:[0-9a-f]{64}$'::text)) AND (((result_kind = ANY (ARRAY['feedback_coverage_artifact'::qp_research_job_result_kind, 'feedback_drift_artifact'::qp_research_job_result_kind, 'feedback_learning_stage_artifact'::qp_research_job_result_kind, 'feedback_comparison_artifact'::qp_research_job_result_kind, 'feedback_shadow_replay_artifact'::qp_research_job_result_kind, 'feedback_decision_artifact'::qp_research_job_result_kind])) AND (result_artifact_uri IS NOT NULL)) OR ((result_kind <> ALL (ARRAY['feedback_coverage_artifact'::qp_research_job_result_kind, 'feedback_drift_artifact'::qp_research_job_result_kind, 'feedback_learning_stage_artifact'::qp_research_job_result_kind, 'feedback_comparison_artifact'::qp_research_job_result_kind, 'feedback_shadow_replay_artifact'::qp_research_job_result_kind, 'feedback_decision_artifact'::qp_research_job_result_kind])) AND (result_artifact_uri IS NULL)) OR ((result_kind IS NULL) AND (result_artifact_uri IS NULL)))))",
    },
    ConstraintSpec {
        name: "ck_quant_research_job_acting_role",
        table: "quant_research_job",
        kind: ConstraintKind::Check,
        definition: "CHECK (((acting_role)::text ~ '^[a-z][a-z0-9_]{0,63}$'::text))",
    },
    ConstraintSpec {
        name: "quant_source_slice_check",
        table: "quant_source_slice",
        kind: ConstraintKind::Check,
        definition: "CHECK (((window_start < window_end) AND (window_end <= pit_cutoff) AND ((reader_contract_version)::text ~ '^[A-Za-z0-9_.@-]{1,64}$'::text) AND ((schema_contract_version)::text ~ '^[A-Za-z0-9_.@-]{1,64}$'::text) AND (((status = 'materializing'::qp_source_slice_status) AND (manifest_uri IS NULL) AND (manifest_hash IS NULL) AND (manifest IS NULL) AND (failure_detail IS NULL) AND (completed_at IS NULL)) OR ((status = 'ready'::qp_source_slice_status) AND (manifest_uri IS NOT NULL) AND (manifest_hash IS NOT NULL) AND (manifest IS NOT NULL) AND (failure_detail IS NULL) AND (completed_at IS NOT NULL)) OR ((status = 'failed'::qp_source_slice_status) AND (manifest_uri IS NULL) AND (manifest_hash IS NULL) AND (manifest IS NULL) AND (failure_detail IS NOT NULL) AND (char_length(btrim(failure_detail)) BETWEEN 1 AND 4096) AND (completed_at IS NOT NULL)))))",
    },
    ConstraintSpec {
        name: "ck_quant_source_slice_manifest",
        table: "quant_source_slice",
        kind: ConstraintKind::Check,
        definition: "CHECK (((manifest IS NULL) OR ((jsonb_typeof(manifest) = 'object'::text) AND (manifest ?& ARRAY['format_version'::text, 'profile_ref'::text, 'evaluation_track'::text, 'research_program_hash'::text, 'window_start'::text, 'window_end'::text, 'pit_cutoff'::text, 'reader_contract_version'::text, 'schema_contract_version'::text, 'decision_policy_snapshot_id'::text, 'runtime_config_hash'::text, 'dataset_format_version'::text, 'capability_registry_hashes'::text, 'objects'::text]) AND (((manifest ->> 'format_version'::text))::integer = 3) AND ((manifest -> 'profile_ref'::text) = profile_ref) AND ((manifest ->> 'evaluation_track'::text) = (evaluation_track)::text) AND ((manifest ->> 'research_program_hash'::text) = research_program_hash) AND (((manifest ->> 'window_start'::text))::timestamp with time zone = window_start) AND (((manifest ->> 'window_end'::text))::timestamp with time zone = window_end) AND (((manifest ->> 'pit_cutoff'::text))::timestamp with time zone = pit_cutoff) AND ((manifest ->> 'reader_contract_version'::text) = (reader_contract_version)::text) AND ((manifest ->> 'schema_contract_version'::text) = (schema_contract_version)::text) AND (((manifest ->> 'decision_policy_snapshot_id'::text))::uuid = decision_policy_snapshot_id) AND ((manifest ->> 'runtime_config_hash'::text) = runtime_config_hash) AND (((manifest ->> 'dataset_format_version'::text))::integer = 3) AND (jsonb_typeof((manifest -> 'capability_registry_hashes'::text)) = 'array'::text) AND (jsonb_typeof((manifest -> 'objects'::text)) = 'array'::text) AND (jsonb_array_length((manifest -> 'objects'::text)) > 0))))",
    },
    ConstraintSpec {
        name: "ck_quant_execution_account_identity",
        table: "quant_execution_account",
        kind: ConstraintKind::Check,
        definition: "CHECK (((chain_id > 0) AND (funder_address ~ '^0x[0-9a-f]{40}$'::text) AND (owner_address ~ '^0x[0-9a-f]{40}$'::text) AND (controller_address ~ '^0x[0-9a-f]{40}$'::text) AND ((wallet_factory_address IS NULL) OR (wallet_factory_address ~ '^0x[0-9a-f]{40}$'::text)) AND ((wallet_implementation_code_hash IS NULL) OR (wallet_implementation_code_hash ~ '^0x[0-9a-f]{64}$'::text)) AND ((wallet_factory_address IS NULL) = (wallet_implementation_code_hash IS NULL)) AND (identity_digest ~ '^blake3:[0-9a-f]{64}$'::text) AND (((wallet_kind = 'eoa'::qp_execution_wallet_kind) AND (funder_address = owner_address) AND (owner_address = controller_address) AND (wallet_factory_address IS NULL)) OR ((wallet_kind = 'deposit_wallet'::qp_execution_wallet_kind) AND (wallet_factory_address IS NOT NULL)) OR (wallet_kind = ANY (ARRAY['proxy'::qp_execution_wallet_kind, 'gnosis_safe'::qp_execution_wallet_kind])))))",
    },
    ConstraintSpec {
        name: "uq_quant_order_intent_account_identity",
        table: "quant_order_intent",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (order_intent_id, execution_account_id)",
    },
    ConstraintSpec {
        name: "uq_quant_position_inventory_lineage",
        table: "quant_position",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (position_id, order_intent_id, execution_account_id)",
    },
    ConstraintSpec {
        name: "fk_quant_position_intent_account",
        table: "quant_position",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (order_intent_id, execution_account_id) REFERENCES public.quant_order_intent(order_intent_id, execution_account_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "uq_quant_settlement_redeem_account_identity",
        table: "quant_settlement_redeem",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (settlement_redeem_id, execution_account_id)",
    },
    ConstraintSpec {
        name: "ck_quant_settlement_redeem_evm_identity",
        table: "quant_settlement_redeem",
        kind: ConstraintKind::Check,
        definition: "CHECK (((yes_token_id ~ '^(0|[1-9][0-9]{0,77})$'::text) AND (no_token_id ~ '^(0|[1-9][0-9]{0,77})$'::text) AND (yes_token_id <> no_token_id) AND (resolution_content_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND (char_length(resolution_outcome) BETWEEN 1 AND 256) AND (resolution_outcome = btrim(resolution_outcome)) AND (resolved_at <= created_at) AND (inventory_digest ~ '^blake3:[0-9a-f]{64}$'::text) AND (contributor_lots_digest ~ '^blake3:[0-9a-f]{64}$'::text) AND ((target_adapter IS NULL) OR (target_adapter ~ '^0x[0-9a-f]{40}$'::text)) AND ((target_code_hash IS NULL) OR (target_code_hash ~ '^0x[0-9a-f]{64}$'::text)) AND ((verified_block_hash IS NULL) OR (verified_block_hash ~ '^0x[0-9a-f]{64}$'::text)) AND ((deployment_evidence_version IS NULL) OR (deployment_evidence_version ~ '^[A-Za-z0-9_.@-]{1,64}$'::text))))",
    },
    ConstraintSpec {
        name: "ck_quant_settlement_inventory_lot",
        table: "quant_settlement_inventory_lot",
        kind: ConstraintKind::Check,
        definition: "CHECK (((inventory_digest ~ '^blake3:[0-9a-f]{64}$'::text) AND (contributor_lots_digest ~ '^blake3:[0-9a-f]{64}$'::text) AND (shares > 0::numeric) AND (cost_basis_usd >= 0::numeric) AND (position_version_at <= created_at) AND (intent_version_at <= created_at)))",
    },
    ConstraintSpec {
        name: "fk_quant_settlement_inventory_case_account",
        table: "quant_settlement_inventory_lot",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (settlement_redeem_id, execution_account_id) REFERENCES public.quant_settlement_redeem(settlement_redeem_id, execution_account_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_settlement_inventory_position_lineage",
        table: "quant_settlement_inventory_lot",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (position_id, order_intent_id, execution_account_id) REFERENCES public.quant_position(position_id, order_intent_id, execution_account_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_settlement_inventory_intent_account",
        table: "quant_settlement_inventory_lot",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (order_intent_id, execution_account_id) REFERENCES public.quant_order_intent(order_intent_id, execution_account_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "ck_quant_settlement_redeem_readiness_capability",
        table: "quant_settlement_redeem",
        kind: ConstraintKind::Check,
        definition: "CHECK ((jsonb_typeof(readiness_evidence_json) = 'object'::text AND jsonb_typeof((readiness_evidence_json -> 'reasons'::text)) = 'array'::text AND jsonb_typeof((readiness_evidence_json -> 'advisories'::text)) = 'array'::text AND (((readiness_status = 'ready'::qp_settlement_readiness_status) AND (jsonb_array_length((readiness_evidence_json -> 'reasons'::text)) = 0) AND (target_adapter IS NOT NULL) AND (target_code_hash IS NOT NULL) AND (deployment_digest IS NOT NULL) AND (deployment_evidence_version IS NOT NULL) AND (verified_block_number > 0) AND (verified_block_hash IS NOT NULL)) OR ((readiness_status = 'blocked'::qp_settlement_readiness_status) AND (jsonb_array_length((readiness_evidence_json -> 'reasons'::text)) > 0) AND (target_adapter IS NULL) AND (target_code_hash IS NULL) AND (deployment_digest IS NULL) AND (deployment_evidence_version IS NULL) AND (verified_block_number IS NULL) AND (verified_block_hash IS NULL)) OR ((readiness_status = 'unchecked'::qp_settlement_readiness_status) AND (jsonb_array_length((readiness_evidence_json -> 'reasons'::text)) = 0) AND (target_adapter IS NULL) AND (target_code_hash IS NULL) AND (deployment_digest IS NULL) AND (deployment_evidence_version IS NULL) AND (verified_block_number IS NULL) AND (verified_block_hash IS NULL)))))",
    },
    ConstraintSpec {
        name: "ck_quant_settlement_redeem_current_target",
        table: "quant_settlement_redeem",
        kind: ConstraintKind::Check,
        definition: "CHECK (((target_adapter IS NULL) OR ((route = 'standard_v2'::qp_settlement_route) AND (target_adapter = '0xada100db00ca00073811820692005400218fce1f'::text) AND (target_code_hash = '0x93b965351d01c1a128821ac79fc98a18105daefb46bda0d1e5b52306d713aa4f'::text)) OR ((route = 'neg_risk_v2'::qp_settlement_route) AND (target_adapter = '0xada2005600dec949baf300f4c6120000bdb6eaab'::text) AND (target_code_hash = '0x3b892c7c2f80e7af69f28faf72a51c2d793f6b79b96011bdf0a1996319fcbe5b'::text))))",
    },
    ConstraintSpec {
        name: "ck_quant_settlement_authorization_lifecycle",
        table: "quant_settlement_authorization",
        kind: ConstraintKind::Check,
        definition: "CHECK (((attempt_ordinal > 0) AND (scope_digest ~ '^blake3:[0-9a-f]{64}$'::text) AND (expires_at > created_at) AND (state <> 'not_required'::qp_settlement_authorization_state) AND (((state = 'pending'::qp_settlement_authorization_state) AND (approved_by IS NULL) AND (approved_at IS NULL) AND (revoked_by IS NULL) AND (revoked_at IS NULL) AND (consumed_at IS NULL) AND (expired_at IS NULL)) OR ((state = 'approved'::qp_settlement_authorization_state) AND (approved_by IS NOT NULL) AND (approved_at IS NOT NULL) AND (revoked_by IS NULL) AND (revoked_at IS NULL) AND (consumed_at IS NULL) AND (expired_at IS NULL)) OR ((state = 'revoked'::qp_settlement_authorization_state) AND (approved_by IS NOT NULL) AND (approved_at IS NOT NULL) AND (revoked_by IS NOT NULL) AND (revoked_at IS NOT NULL) AND (consumed_at IS NULL) AND (expired_at IS NULL)) OR ((state = 'consumed'::qp_settlement_authorization_state) AND (approved_by IS NOT NULL) AND (approved_at IS NOT NULL) AND (revoked_by IS NULL) AND (revoked_at IS NULL) AND (consumed_at IS NOT NULL) AND (expired_at IS NULL)) OR ((state = 'expired'::qp_settlement_authorization_state) AND (revoked_by IS NULL) AND (revoked_at IS NULL) AND (consumed_at IS NULL) AND (expired_at IS NOT NULL) AND ((approved_by IS NULL) = (approved_at IS NULL))))))",
    },
    ConstraintSpec {
        name: "fk_quant_settlement_redeem_current_authorization",
        table: "quant_settlement_redeem",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (current_authorization_id) REFERENCES public.quant_settlement_authorization(settlement_authorization_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "ck_quant_settlement_redeem_lifecycle",
        table: "quant_settlement_redeem",
        kind: ConstraintKind::Check,
        definition: "CHECK (((attempt_count >= 0) AND (retry_count >= 0) AND ((claim_owner IS NULL) = (lease_expires_at IS NULL)) AND ((expected_payout_usd IS NULL) OR (expected_payout_usd >= 0::numeric)) AND ((actual_payout_usd IS NULL) OR (actual_payout_usd >= 0::numeric)) AND ((last_error IS NULL) OR (char_length(last_error) BETWEEN 1 AND 2048)) AND ((state <> 'prepared'::qp_settlement_case_state) OR ((readiness_status = 'ready'::qp_settlement_readiness_status) AND (balance_before_json IS NOT NULL) AND (expected_payout_usd IS NOT NULL) AND (prepared_at IS NOT NULL))) AND ((state <> 'submitted'::qp_settlement_case_state) OR (submitted_at IS NOT NULL)) AND ((state <> 'retry_scheduled'::qp_settlement_case_state) OR ((failure_code IS NOT NULL) AND (next_attempt_at IS NOT NULL))) AND ((state <> 'reconciliation_required'::qp_settlement_case_state) OR ((failure_code IS NOT NULL) AND (reconciliation_state <> 'not_required'::qp_settlement_reconciliation_state))) AND ((state <> 'manual_required'::qp_settlement_case_state) OR (failure_code IS NOT NULL)) AND ((state <> 'confirmed'::qp_settlement_case_state) OR ((confirmed_at IS NOT NULL) AND (actual_payout_usd IS NOT NULL) AND (balance_after_json IS NOT NULL) AND (claim_owner IS NULL) AND (lease_expires_at IS NULL)))))",
    },
    ConstraintSpec {
        name: "ck_quant_settlement_governed_action_scope",
        table: "quant_settlement_governed_action",
        kind: ConstraintKind::Check,
        definition: "CHECK (((scope_digest ~ '^blake3:[0-9a-f]{64}$'::text) AND (char_length(idempotency_key) >= 1) AND (char_length(idempotency_key) <= 128) AND (char_length(authorization_reason) >= 1) AND (char_length(authorization_reason) <= 500) AND (authorization_reason = btrim(authorization_reason)) AND ((revocation_reason IS NULL) OR ((char_length(revocation_reason) >= 1) AND (char_length(revocation_reason) <= 500) AND (revocation_reason = btrim(revocation_reason)))) AND ((revocation_reason IS NULL) = (revoked_by IS NULL)) AND (expires_at > authorized_at) AND (retry_count >= 0) AND ((target_adapter IS NULL) OR (target_adapter ~ '^0x[0-9a-f]{40}$'::text)) AND ((deployment_digest IS NULL) OR (deployment_digest ~ '^blake3:[0-9a-f]{64}$'::text)) AND ((authorization_digest IS NULL) OR (authorization_digest ~ '^blake3:[0-9a-f]{64}$'::text)) AND ((deployment_evidence_version IS NULL) OR (deployment_evidence_version ~ '^[A-Za-z0-9_.@-]{1,64}$'::text)) AND ((verified_block_hash IS NULL) OR (verified_block_hash ~ '^0x[0-9a-f]{64}$'::text)) AND ((verified_block_number IS NULL) = (verified_block_hash IS NULL)) AND ((claim_owner IS NULL) = (lease_expires_at IS NULL)) AND ((last_error IS NULL) OR ((char_length(last_error) >= 1) AND (char_length(last_error) <= 2048))) AND (((kind = 'outcome_token_approval'::qp_settlement_governed_action_kind) AND (settlement_redeem_id IS NULL) AND (route IS NOT NULL) AND (target_adapter IS NOT NULL) AND (deployment_digest IS NOT NULL) AND (deployment_evidence_version IS NOT NULL) AND (verified_block_number > 0) AND (desired_approval IS TRUE) AND (authorization_digest IS NULL) AND (payout_ceiling_usd IS NULL)) OR ((kind = 'outcome_token_revocation'::qp_settlement_governed_action_kind) AND (settlement_redeem_id IS NULL) AND (route IS NOT NULL) AND (target_adapter IS NOT NULL) AND (deployment_digest IS NOT NULL) AND (deployment_evidence_version IS NOT NULL) AND (verified_block_number > 0) AND (desired_approval IS FALSE) AND (authorization_digest IS NULL) AND (payout_ceiling_usd IS NULL)) OR ((kind = 'canary_grant'::qp_settlement_governed_action_kind) AND (settlement_redeem_id IS NOT NULL) AND (route IS NOT NULL) AND (target_adapter IS NOT NULL) AND (deployment_digest IS NOT NULL) AND (deployment_evidence_version IS NOT NULL) AND (verified_block_number > 0) AND (desired_approval IS NULL) AND (authorization_digest IS NOT NULL) AND (payout_ceiling_usd > 0::numeric)))))",
    },
    ConstraintSpec {
        name: "ck_quant_settlement_governed_action_lifecycle",
        table: "quant_settlement_governed_action",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((state = 'authorized'::qp_settlement_governed_action_state) AND (consumed_at IS NULL) AND (revoked_by IS NULL) AND (revoked_at IS NULL) AND (failure_code IS NULL) AND (next_attempt_at IS NOT NULL)) OR ((state = 'retry_scheduled'::qp_settlement_governed_action_state) AND (consumed_at IS NULL) AND (revoked_by IS NULL) AND (revoked_at IS NULL) AND (failure_code IS NOT NULL) AND (next_attempt_at IS NOT NULL)) OR ((state = 'consumed'::qp_settlement_governed_action_state) AND (consumed_at IS NOT NULL) AND (revoked_by IS NULL) AND (revoked_at IS NULL) AND (claim_owner IS NULL) AND (lease_expires_at IS NULL) AND (next_attempt_at IS NULL)) OR ((state = 'revoked'::qp_settlement_governed_action_state) AND (consumed_at IS NULL) AND (revoked_by IS NOT NULL) AND (revoked_at IS NOT NULL) AND (claim_owner IS NULL) AND (lease_expires_at IS NULL) AND (next_attempt_at IS NULL)) OR ((state = 'expired'::qp_settlement_governed_action_state) AND (consumed_at IS NULL) AND (revoked_by IS NULL) AND (revoked_at IS NULL) AND (claim_owner IS NULL) AND (lease_expires_at IS NULL) AND (next_attempt_at IS NULL)) OR ((state = ANY (ARRAY['reconciliation_required'::qp_settlement_governed_action_state, 'failed'::qp_settlement_governed_action_state])) AND (consumed_at IS NULL) AND (failure_code IS NOT NULL) AND (last_error IS NOT NULL) AND (claim_owner IS NULL) AND (lease_expires_at IS NULL) AND (next_attempt_at IS NULL))))",
    },
    ConstraintSpec {
        name: "ck_quant_settlement_external_cursor_identity",
        table: "quant_settlement_external_cursor",
        kind: ConstraintKind::Check,
        definition: "CHECK (((chain_id > 0) AND (target_adapter ~ '^0x[0-9a-f]{40}$'::text) AND (target_code_hash ~ '^0x[0-9a-f]{64}$'::text) AND (deployment_digest ~ '^blake3:[0-9a-f]{64}$'::text) AND (deployment_evidence_version ~ '^[A-Za-z0-9_.@-]{1,64}$'::text) AND (next_block_number >= 0) AND ((last_observed_block_number IS NULL) = (last_observed_block_hash IS NULL)) AND ((last_observed_block_number IS NULL) OR ((last_observed_block_number >= 0) AND (last_observed_block_hash ~ '^0x[0-9a-f]{64}$'::text) AND (next_block_number = (last_observed_block_number + 1))))))",
    },
    ConstraintSpec {
        name: "ck_quant_settlement_chain_submission_identity",
        table: "quant_settlement_chain_submission",
        kind: ConstraintKind::Check,
        definition: "CHECK (((target_adapter ~ '^0x[0-9a-f]{40}$'::text) AND (target_code_hash ~ '^0x[0-9a-f]{64}$'::text) AND (conditional_tokens = '0x4d97dcd97ec945f40cf65f87097ace5ea0476045'::text) AND (collateral_token = '0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb'::text) AND (usdce = '0x2791bca1f2de4661ed88a30c99a7a9449aa84174'::text) AND (call_target ~ '^0x[0-9a-f]{40}$'::text) AND (verified_block_number > 0) AND (verified_block_hash ~ '^0x[0-9a-f]{64}$'::text) AND ((prepared_block_number IS NULL) = (prepared_block_hash IS NULL)) AND ((prepared_block_number IS NULL) OR ((prepared_block_number > 0) AND (prepared_block_hash ~ '^0x[0-9a-f]{64}$'::text))) AND (calldata_hash ~ '^0x[0-9a-f]{64}$'::text) AND (octet_length(calldata) > 0) AND ((signed_envelope IS NULL) = (signed_envelope_hash IS NULL)) AND ((signed_envelope IS NULL) = (prepared_nonce IS NULL)) AND ((signed_envelope IS NULL) OR ((octet_length(signed_envelope) > 0) AND (signed_envelope_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND (prepared_nonce ~ '^(0|[1-9][0-9]{0,77})$'::text) AND ((char_length(prepared_nonce) < 78) OR (prepared_nonce <= '115792089237316195423570985008687907853269984665640564039457584007913129639935'::text)) AND ((gas_limit IS NULL) OR ((gas_limit ~ '^(0|[1-9][0-9]{0,77})$'::text) AND ((char_length(gas_limit) < 78) OR (gas_limit <= '115792089237316195423570985008687907853269984665640564039457584007913129639935'::text)))))) AND ((transaction_hash IS NULL) OR (transaction_hash ~ '^0x[0-9a-f]{64}$'::text)) AND (deployment_evidence_version ~ '^[A-Za-z0-9_.@-]{1,64}$'::text) AND (attempt_ordinal > 0) AND ((last_error IS NULL) OR (char_length(last_error) BETWEEN 1 AND 2048)) AND (jsonb_typeof(failure_history_json) = 'object'::text) AND (jsonb_typeof((failure_history_json -> 'entries'::text)) = 'array'::text)))",
    },
    ConstraintSpec {
        name: "ck_quant_settlement_chain_submission_parent",
        table: "quant_settlement_chain_submission",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((settlement_redeem_id IS NOT NULL)::integer + (settlement_governed_action_id IS NOT NULL)::integer) = 1 AND (((purpose = 'redeem'::qp_settlement_submission_purpose) AND (settlement_redeem_id IS NOT NULL) AND (settlement_governed_action_id IS NULL)) OR ((purpose = ANY (ARRAY['outcome_token_approval'::qp_settlement_submission_purpose, 'outcome_token_revocation'::qp_settlement_submission_purpose])) AND (settlement_redeem_id IS NULL) AND (settlement_governed_action_id IS NOT NULL))) AND ((canary_action_id IS NULL) OR ((purpose = 'redeem'::qp_settlement_submission_purpose) AND (settlement_redeem_id IS NOT NULL)))))",
    },
    ConstraintSpec {
        name: "fk_quant_settlement_chain_submission_canary_action",
        table: "quant_settlement_chain_submission",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (canary_action_id) REFERENCES public.quant_settlement_governed_action(settlement_governed_action_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "ck_quant_settlement_chain_submission_scope",
        table: "quant_settlement_chain_submission",
        kind: ConstraintKind::Check,
        definition: "CHECK (((((route = 'standard_v2'::qp_settlement_route) AND (target_adapter = '0xada100db00ca00073811820692005400218fce1f'::text) AND (target_code_hash = '0x93b965351d01c1a128821ac79fc98a18105daefb46bda0d1e5b52306d713aa4f'::text)) OR ((route = 'neg_risk_v2'::qp_settlement_route) AND (target_adapter = '0xada2005600dec949baf300f4c6120000bdb6eaab'::text) AND (target_code_hash = '0x3b892c7c2f80e7af69f28faf72a51c2d793f6b79b96011bdf0a1996319fcbe5b'::text))) AND ((kind <> 'externally_observed'::qp_settlement_submission_kind) OR (state = ANY (ARRAY['awaiting_finality'::qp_settlement_submission_state, 'confirmed'::qp_settlement_submission_state, 'failed'::qp_settlement_submission_state])))))",
    },
    ConstraintSpec {
        name: "ck_quant_settlement_chain_submission_state_identity",
        table: "quant_settlement_chain_submission",
        kind: ConstraintKind::Check,
        definition: "CHECK (((state NOT IN ('prepared'::qp_settlement_submission_state, 'dispatching'::qp_settlement_submission_state)) OR ((kind <> 'externally_observed'::qp_settlement_submission_kind) AND (prepared_block_number IS NOT NULL))) AND ((state <> 'awaiting_chain_hash'::qp_settlement_submission_state) OR ((kind = 'relayer'::qp_settlement_submission_kind) AND (relayer_transaction_id IS NOT NULL) AND (transaction_hash IS NULL))) AND ((state NOT IN ('awaiting_finality'::qp_settlement_submission_state, 'confirmed'::qp_settlement_submission_state)) OR ((transaction_hash IS NOT NULL) AND ((kind <> 'relayer'::qp_settlement_submission_kind) OR (relayer_transaction_id IS NOT NULL)))) AND ((kind <> 'direct_eoa'::qp_settlement_submission_kind) OR ((relayer_transaction_id IS NULL) AND (signed_envelope IS NOT NULL) AND (signed_envelope_hash IS NOT NULL) AND (prepared_nonce IS NOT NULL) AND (gas_limit IS NOT NULL) AND (transaction_hash IS NOT NULL))) AND ((kind <> 'relayer'::qp_settlement_submission_kind) OR ((signed_envelope IS NOT NULL) AND (signed_envelope_hash IS NOT NULL) AND (prepared_nonce IS NOT NULL))) AND ((kind <> 'externally_observed'::qp_settlement_submission_kind) OR ((signed_envelope IS NULL) AND (signed_envelope_hash IS NULL) AND (prepared_nonce IS NULL) AND (gas_limit IS NULL) AND (relayer_transaction_id IS NULL) AND (transaction_hash IS NOT NULL))) AND ((state <> 'confirmed'::qp_settlement_submission_state) OR ((confirmed_at IS NOT NULL) AND (receipt_evidence_json IS NOT NULL))) AND ((receipt_evidence_json IS NULL) OR (jsonb_typeof(receipt_evidence_json) = 'object'::text)))",
    },
    ConstraintSpec {
        name: "ck_quant_settlement_chain_submission_purpose_target",
        table: "quant_settlement_chain_submission",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((purpose = 'redeem'::qp_settlement_submission_purpose) AND (call_target = target_adapter)) OR ((purpose = ANY (ARRAY['outcome_token_approval'::qp_settlement_submission_purpose, 'outcome_token_revocation'::qp_settlement_submission_purpose])) AND (kind <> 'externally_observed'::qp_settlement_submission_kind) AND (call_target = conditional_tokens))))",
    },
    ConstraintSpec {
        name: "ck_quant_execution_trade_ref_identity",
        table: "quant_execution_trade_ref",
        kind: ConstraintKind::Check,
        definition: "CHECK (((char_length(venue_trade_id) >= 1) AND (char_length(venue_trade_id) <= 256) AND ((transaction_hash IS NULL) OR (transaction_hash ~ '^0x[0-9a-f]{64}$'::text)) AND ((trade_status IS NULL) OR (trade_status NOT IN ('mined'::qp_venue_trade_status, 'confirmed'::qp_venue_trade_status)) OR (transaction_hash IS NOT NULL))))",
    },
    ConstraintSpec {
        name: "ck_quant_execution_transaction_ref_hash",
        table: "quant_execution_transaction_ref",
        kind: ConstraintKind::Check,
        definition: "CHECK ((transaction_hash ~ '^0x[0-9a-f]{64}$'::text))",
    },
    ConstraintSpec {
        name: "ck_quant_trade_tape_block_cursor_contract_address",
        table: "quant_trade_tape_block_cursor",
        kind: ConstraintKind::Check,
        definition: "CHECK (((contract_address)::text ~ '^0x[0-9a-f]{40}$'::text))",
    },
    ConstraintSpec {
        name: "quant_trade_policy_trial_attempt_check",
        table: "quant_trade_policy_trial_attempt",
        kind: ConstraintKind::Check,
        definition: "CHECK (((attempt_ordinal >= 0) AND (length((candidate_id)::text) BETWEEN 1 AND 128) AND (octet_length((candidate_id)::text) = length((candidate_id)::text)) AND ((candidate_id)::text ~ '^[[:graph:]]+$'::text) AND (position(chr(92) in (candidate_id)::text) = 0) AND (position(chr(34) in (candidate_id)::text) = 0) AND ((fold_index IS NULL) OR (fold_index >= 0)) AND ((path_index IS NULL) OR (path_index >= 0)) AND ((evidence_row_count IS NULL) OR (evidence_row_count >= 0))))",
    },
    ConstraintSpec {
        name: "quant_trade_policy_trial_attempt_check1",
        table: "quant_trade_policy_trial_attempt",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((scope = ANY (ARRAY['candidate'::qp_trade_policy_trial_scope, 'latency_stress'::qp_trade_policy_trial_scope])) AND (fold_index IS NULL) AND (path_index IS NULL)) OR ((scope = 'fold'::qp_trade_policy_trial_scope) AND (fold_index IS NOT NULL) AND (path_index IS NULL)) OR ((scope = 'path'::qp_trade_policy_trial_scope) AND (fold_index IS NULL) AND (path_index IS NOT NULL))))",
    },
    ConstraintSpec {
        name: "quant_trade_policy_trial_attempt_check2",
        table: "quant_trade_policy_trial_attempt",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((status = 'succeeded'::qp_trade_policy_trial_status) AND (metrics_json IS NOT NULL) AND (failure_detail IS NULL) AND (evidence_uri IS NOT NULL) AND (evidence_hash IS NOT NULL) AND (evidence_row_count IS NOT NULL)) OR ((status = ANY (ARRAY['failed'::qp_trade_policy_trial_status, 'cancelled'::qp_trade_policy_trial_status])) AND (metrics_json IS NULL) AND ((length(btrim(failure_detail)) >= 1) AND (length(btrim(failure_detail)) <= 8192)))))",
    },
    ConstraintSpec {
        name: "quant_trade_policy_trial_attempt_check3",
        table: "quant_trade_policy_trial_attempt",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((evidence_uri IS NULL) AND (evidence_hash IS NULL) AND (evidence_row_count IS NULL)) OR ((evidence_uri IS NOT NULL) AND (evidence_hash IS NOT NULL) AND (evidence_row_count IS NOT NULL))))",
    },
    ConstraintSpec {
        name: "quant_trade_policy_validation_check",
        table: "quant_trade_policy_validation",
        kind: ConstraintKind::Check,
        definition: "CHECK (((total_rows >= 0) AND (passed_rows >= 0) AND (failed_rows >= 0) AND ((passed_rows + failed_rows) <= total_rows) AND ((length(btrim(reason)) >= 1) AND (length(btrim(reason)) <= 512)) AND (((status = 'running'::qp_trade_policy_validation_status) AND (validation_hash IS NULL) AND (failure_detail IS NULL) AND (completed_at IS NULL)) OR ((status = 'succeeded'::qp_trade_policy_validation_status) AND (validation_hash IS NOT NULL) AND (failure_detail IS NULL) AND (completed_at IS NOT NULL) AND (failed_rows = 0) AND (passed_rows = total_rows) AND (total_rows > 0)) OR ((status = ANY (ARRAY['failed'::qp_trade_policy_validation_status, 'cancelled'::qp_trade_policy_validation_status])) AND (validation_hash IS NOT NULL) AND (failure_detail IS NOT NULL) AND (completed_at IS NOT NULL)))))",
    },
    ConstraintSpec {
        name: "quant_trade_policy_validation_row_check",
        table: "quant_trade_policy_validation_row",
        kind: ConstraintKind::Check,
        definition: "CHECK (((row_ordinal >= 0) AND (record_key <> ''::text) AND ((evidence_kind)::text = ANY (ARRAY[('observation_eligibility'::character varying)::text, ('fills'::character varying)::text, ('candidate_trials'::character varying)::text, ('cohort_trials'::character varying)::text, ('cpcv_paths'::character varying)::text, ('coverage_gaps'::character varying)::text, ('statistical_summaries'::character varying)::text])) AND ((expected_row_hash IS NOT NULL) OR (actual_row_hash IS NOT NULL)) AND ((diagnostic_kind IS NULL) OR ((diagnostic_kind)::text ~ '^[a-z][a-z0-9_]{0,127}$'::text)) AND ((passed AND (expected_row_hash = actual_row_hash) AND (diagnostic_kind IS NULL) AND (detail IS NULL)) OR ((NOT passed) AND (diagnostic_kind IS NOT NULL) AND (detail IS NOT NULL)))))",
    },
    ConstraintSpec {
        name: "quant_training_dataset_check",
        table: "quant_training_dataset",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((status = ANY (ARRAY['planned'::qp_training_dataset_status, 'building'::qp_training_dataset_status])) AND (feature_schema_hash IS NOT NULL) AND (factor_schema_hash IS NOT NULL) AND (label_schema_hash IS NULL) AND (dataset_hash IS NULL) AND (manifest_hash IS NULL) AND (manifest IS NULL) AND (artifact_bytes_hash IS NULL) AND (parquet_uri IS NULL) AND (sample_count IS NULL) AND (coverage IS NULL) AND (failure_detail IS NULL) AND (completed_at IS NULL)) OR ((status = ANY (ARRAY['ready'::qp_training_dataset_status, 'expired'::qp_training_dataset_status])) AND (feature_schema_hash IS NOT NULL) AND (factor_schema_hash IS NOT NULL) AND (label_schema_hash IS NOT NULL) AND (dataset_hash IS NOT NULL) AND (manifest_hash IS NOT NULL) AND (manifest IS NOT NULL) AND (artifact_bytes_hash IS NOT NULL) AND (parquet_uri IS NOT NULL) AND (sample_count IS NOT NULL) AND (sample_count >= 0) AND (coverage IS NOT NULL) AND (failure_detail IS NULL) AND (completed_at IS NOT NULL)) OR ((status = 'insufficient_labels'::qp_training_dataset_status) AND (feature_schema_hash IS NOT NULL) AND (factor_schema_hash IS NOT NULL) AND (label_schema_hash IS NOT NULL) AND (dataset_hash IS NOT NULL) AND (manifest_hash IS NOT NULL) AND (manifest IS NOT NULL) AND (artifact_bytes_hash IS NOT NULL) AND (parquet_uri IS NOT NULL) AND (sample_count IS NOT NULL) AND (sample_count >= 0) AND (coverage IS NOT NULL) AND (failure_detail IS NOT NULL) AND (char_length(btrim(failure_detail)) BETWEEN 1 AND 4096) AND (completed_at IS NOT NULL)) OR ((status = 'failed'::qp_training_dataset_status) AND (feature_schema_hash IS NOT NULL) AND (factor_schema_hash IS NOT NULL) AND (failure_detail IS NOT NULL) AND (char_length(btrim(failure_detail)) BETWEEN 1 AND 4096) AND (completed_at IS NOT NULL) AND (((label_schema_hash IS NULL) AND (dataset_hash IS NULL) AND (manifest_hash IS NULL) AND (manifest IS NULL) AND (artifact_bytes_hash IS NULL) AND (parquet_uri IS NULL) AND (sample_count IS NULL) AND (coverage IS NULL)) OR ((label_schema_hash IS NOT NULL) AND (dataset_hash IS NOT NULL) AND (manifest_hash IS NOT NULL) AND (manifest IS NOT NULL) AND (artifact_bytes_hash IS NOT NULL) AND (parquet_uri IS NOT NULL) AND (sample_count IS NOT NULL) AND (sample_count >= 0) AND (coverage IS NOT NULL))))))",
    },
    ConstraintSpec {
        name: "ck_quant_training_dataset_lineage",
        table: "quant_training_dataset",
        kind: ConstraintKind::Check,
        definition: "CHECK (((window_start < window_end) AND (window_end <= pit_cutoff) AND (knowledge_lag_secs >= 0) AND (feature_schema_version >= 1) AND ((sample_interval_secs > 0) OR ((sample_interval_secs = 0) AND (cohort_manifest IS NOT NULL))) AND (jsonb_typeof(horizons_secs) = 'array'::text) AND (jsonb_array_length(horizons_secs) > 0) AND (jsonb_typeof(source_lineage) = 'object'::text) AND (source_lineage ?& ARRAY['format_version'::text, 'source_slice_id'::text, 'research_profile_artifact_id'::text, 'source_window_start'::text, 'source_window_end'::text, 'pit_cutoff'::text, 'decision_policy_snapshot_id'::text, 'capability_registry_hashes'::text]) AND (((source_lineage ->> 'format_version'::text))::integer = 1) AND (((source_lineage ->> 'source_slice_id'::text))::uuid = source_slice_id) AND ((source_lineage ->> 'research_profile_artifact_id'::text) = research_profile_artifact_id) AND (((source_lineage ->> 'source_window_start'::text))::timestamp with time zone <= window_start) AND (((source_lineage ->> 'source_window_end'::text))::timestamp with time zone >= window_end) AND (((source_lineage ->> 'pit_cutoff'::text))::timestamp with time zone = pit_cutoff) AND (((source_lineage ->> 'decision_policy_snapshot_id'::text))::uuid = decision_policy_snapshot_id) AND (jsonb_typeof((source_lineage -> 'capability_registry_hashes'::text)) = 'array'::text)))",
    },
    ConstraintSpec {
        name: "ck_quant_training_dataset_factor_plane",
        table: "quant_training_dataset",
        kind: ConstraintKind::Check,
        definition: "CHECK ((public.validate_factor_serving_plane(factor_serving_plane, feature_schema_hash, feature_schema_version, (model_family)::text) AND ((factor_serving_plane ->> 'factor_schema_hash'::text) = factor_schema_hash)))",
    },
    ConstraintSpec {
        name: "ck_quant_training_dataset_cohort",
        table: "quant_training_dataset",
        kind: ConstraintKind::Check,
        definition: "CHECK (((((feedback_cohort IS NULL) AND (cohort_manifest IS NULL)) OR ((feedback_cohort IS NOT NULL) AND (cohort_manifest IS NOT NULL) AND (jsonb_typeof(cohort_manifest) = 'object'::text) AND (cohort_manifest ?& ARRAY['format_version'::text, 'cohort'::text, 'window'::text, 'artifact'::text, 'counts'::text, 'capability_registry_hashes'::text]) AND (((cohort_manifest ->> 'format_version'::text))::integer = 1) AND ((cohort_manifest ->> 'cohort'::text) = (feedback_cohort)::text) AND (jsonb_typeof((cohort_manifest -> 'window'::text)) = 'object'::text) AND (jsonb_typeof((cohort_manifest -> 'artifact'::text)) = 'object'::text) AND (jsonb_typeof((cohort_manifest -> 'counts'::text)) = 'object'::text) AND (jsonb_typeof((cohort_manifest -> 'capability_registry_hashes'::text)) = 'array'::text) AND (((cohort_manifest #>> '{window,window_start}'::text[]))::timestamp with time zone = window_start) AND (((cohort_manifest #>> '{window,cutoff}'::text[]))::timestamp with time zone = window_end) AND (((cohort_manifest #>> '{artifact,row_count}'::text[]))::bigint = ((cohort_manifest #>> '{counts,included_count}'::text[]))::bigint) AND ((sample_count IS NULL) OR (((cohort_manifest #>> '{counts,included_count}'::text[]))::bigint = sample_count)))) AND ((purpose <> 'evaluation'::qp_dataset_purpose) OR ((feedback_cohort IS NOT NULL) AND (cohort_manifest IS NOT NULL))) AND ((purpose <> 'policy_fit'::qp_dataset_purpose) OR ((feedback_cohort IS NULL) AND (cohort_manifest IS NULL)))))",
    },
    ConstraintSpec {
        name: "ck_quant_training_dataset_manifest",
        table: "quant_training_dataset",
        kind: ConstraintKind::Check,
        definition: "CHECK (((manifest IS NULL) OR ((jsonb_typeof(manifest) = 'object'::text) AND (manifest ?& ARRAY['format_version'::text, 'training_dataset_id'::text, 'source_lineage'::text, 'cohort_manifest'::text, 'model_spec_id'::text, 'model_family'::text, 'model_spec_definition_hash'::text, 'trade_policy_artifact_id'::text, 'trade_policy_hash'::text, 'window_start'::text, 'window_end'::text, 'purpose'::text, 'knowledge_lag_secs'::text, 'sample_interval_secs'::text, 'horizons_secs'::text, 'feature_schema_version'::text, 'feature_schema_hash'::text, 'factor_serving_plane'::text, 'label_schema_hash'::text, 'semantic_dataset_hash'::text, 'source_fingerprint'::text, 'sample_count'::text]) AND ((manifest - ARRAY['format_version'::text, 'training_dataset_id'::text, 'source_lineage'::text, 'cohort_manifest'::text, 'model_spec_id'::text, 'model_family'::text, 'model_spec_definition_hash'::text, 'trade_policy_artifact_id'::text, 'trade_policy_hash'::text, 'window_start'::text, 'window_end'::text, 'purpose'::text, 'knowledge_lag_secs'::text, 'sample_interval_secs'::text, 'horizons_secs'::text, 'feature_schema_version'::text, 'feature_schema_hash'::text, 'factor_serving_plane'::text, 'label_schema_hash'::text, 'semantic_dataset_hash'::text, 'source_fingerprint'::text, 'sample_count'::text]) = '{}'::jsonb) AND (((manifest ->> 'format_version'::text))::integer = 3) AND (((manifest ->> 'training_dataset_id'::text))::uuid = training_dataset_id) AND ((manifest -> 'source_lineage'::text) = source_lineage) AND (((cohort_manifest IS NULL) AND ((manifest -> 'cohort_manifest'::text) = 'null'::jsonb)) OR ((cohort_manifest IS NOT NULL) AND ((manifest -> 'cohort_manifest'::text) = cohort_manifest))) AND (((manifest ->> 'model_spec_id'::text))::uuid = model_spec_id) AND ((manifest ->> 'model_family'::text) = (model_family)::text) AND ((manifest ->> 'model_spec_definition_hash'::text) = model_spec_definition_hash) AND ((((manifest -> 'trade_policy_artifact_id'::text) = 'null'::jsonb) AND ((manifest -> 'trade_policy_hash'::text) = 'null'::jsonb)) OR ((jsonb_typeof((manifest -> 'trade_policy_artifact_id'::text)) = 'string'::text) AND (((manifest ->> 'trade_policy_artifact_id'::text))::uuid IS NOT NULL) AND (jsonb_typeof((manifest -> 'trade_policy_hash'::text)) = 'string'::text) AND ((manifest ->> 'trade_policy_hash'::text) ~ '^blake3:[0-9a-f]{64}$'::text))) AND (((manifest ->> 'window_start'::text))::timestamp with time zone = window_start) AND (((manifest ->> 'window_end'::text))::timestamp with time zone = window_end) AND ((manifest ->> 'purpose'::text) = (purpose)::text) AND (((manifest ->> 'knowledge_lag_secs'::text))::bigint = knowledge_lag_secs) AND (((manifest ->> 'sample_interval_secs'::text))::bigint = sample_interval_secs) AND ((manifest -> 'horizons_secs'::text) = horizons_secs) AND (((manifest ->> 'feature_schema_version'::text))::integer = feature_schema_version) AND ((manifest ->> 'feature_schema_hash'::text) = feature_schema_hash) AND ((manifest -> 'factor_serving_plane'::text) = factor_serving_plane) AND ((factor_serving_plane ->> 'factor_schema_hash'::text) = factor_schema_hash) AND ((manifest ->> 'label_schema_hash'::text) = label_schema_hash) AND ((manifest ->> 'semantic_dataset_hash'::text) = dataset_hash) AND ((manifest ->> 'source_fingerprint'::text) ~ '^blake3:[0-9a-f]{64}$'::text) AND (((manifest ->> 'sample_count'::text))::bigint = sample_count))))",
    },
    ConstraintSpec {
        name: "ck_quant_training_dataset_coverage",
        table: "quant_training_dataset",
        kind: ConstraintKind::Check,
        definition: "CHECK (((coverage IS NULL) OR ((jsonb_typeof(coverage) = 'object'::text) AND (coverage ? 'built_examples'::text) AND (((coverage ->> 'built_examples'::text))::bigint = sample_count))))",
    },
    ConstraintSpec {
        name: "ck_quant_training_dataset_hashes",
        table: "quant_training_dataset",
        kind: ConstraintKind::Check,
        definition: "CHECK (((model_spec_definition_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND (feature_schema_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND (factor_schema_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND ((label_schema_hash IS NULL) OR (label_schema_hash ~ '^blake3:[0-9a-f]{64}$'::text)) AND ((dataset_hash IS NULL) OR (dataset_hash ~ '^blake3:[0-9a-f]{64}$'::text)) AND ((manifest_hash IS NULL) OR (manifest_hash ~ '^blake3:[0-9a-f]{64}$'::text)) AND ((artifact_bytes_hash IS NULL) OR (artifact_bytes_hash ~ '^blake3:[0-9a-f]{64}$'::text))))",
    },
    ConstraintSpec {
        name: "ck_quant_backtest_report_evaluation",
        table: "quant_backtest_report",
        kind: ConstraintKind::Check,
        definition: "CHECK (((window_start < window_end) AND (coverage >= 0::numeric) AND (coverage <= 1::numeric) AND (sample_count >= 0) AND (missing_feature_count >= 0) AND (hit_rate >= 0::numeric) AND (hit_rate <= 1::numeric) AND (liquidity_feasibility >= 0::numeric) AND (liquidity_feasibility <= 1::numeric) AND (jsonb_typeof(expected_vs_realized) = 'object'::text) AND (jsonb_typeof(category_breakdown) = 'array'::text) AND (jsonb_typeof(report_pnl_simulation) = 'object'::text) AND (report_hash ~ '^blake3:[0-9a-f]{64}$'::text)))",
    },
    ConstraintSpec {
        name: "fk_quant_training_dataset_profile",
        table: "quant_training_dataset",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (research_profile_artifact_id) REFERENCES public.research_profile_artifact(research_profile_artifact_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_training_dataset_source_slice",
        table: "quant_training_dataset",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (source_slice_id) REFERENCES public.quant_source_slice(source_slice_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_backtest_report_evaluation_dataset",
        table: "quant_backtest_report",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (evaluation_dataset_id) REFERENCES public.quant_training_dataset(training_dataset_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "ck_policy_approval_expiry",
        table: "policy_approval",
        kind: ConstraintKind::Check,
        definition: "CHECK (((expires_at IS NULL) OR (expires_at > decided_at)))",
    },
    ConstraintSpec {
        name: "ck_policy_approval_reason",
        table: "policy_approval",
        kind: ConstraintKind::Check,
        definition: "CHECK (((length(reason) >= 1) AND (length(reason) <= 2048)))",
    },
    ConstraintSpec {
        name: "ck_policy_approval_validation_subject",
        table: "policy_approval",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((decision <> 'approved'::qp_policy_approval_decision) OR (validation_subject IS NOT NULL)) AND ((validation_subject IS NULL) OR ((jsonb_typeof(validation_subject) = 'object'::text) AND (validation_subject ?& ARRAY['base_generation'::text, 'base_revision_vector'::text, 'candidate_bundle_hash'::text])))))",
    },
    ConstraintSpec {
        name: "ck_schema_migration_audit_algorithm",
        table: "schema_migration_audit",
        kind: ConstraintKind::Check,
        definition: "CHECK (((checksum_algorithm)::text = 'blake3-256'::text))",
    },
    ConstraintSpec {
        name: "ck_schema_migration_audit_artifact_length",
        table: "schema_migration_audit",
        kind: ConstraintKind::Check,
        definition: "CHECK ((artifact_length > 0))",
    },
    ConstraintSpec {
        name: "ck_schema_migration_audit_checksum",
        table: "schema_migration_audit",
        kind: ConstraintKind::Check,
        definition: "CHECK ((checksum ~ '^[0-9a-f]{64}$'::text))",
    },
    ConstraintSpec {
        name: "ck_policy_profile_artifact_identity",
        table: "policy_profile_artifact",
        kind: ConstraintKind::Check,
        definition: "CHECK (((schema_version = 1) AND (content_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND ((document ->> 'profile_kind'::text) = (kind)::text)))",
    },
    ConstraintSpec {
        name: "ck_research_profile_artifact_identity",
        table: "research_profile_artifact",
        kind: ConstraintKind::Check,
        definition: "CHECK (((version > 0) AND (research_profile_id ~ '^[a-z0-9_]+$'::text) AND (content_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND (research_profile_artifact_id = (((('rpa:'::text || research_profile_id) || ':'::text) || version::text) || ':'::text) || content_hash)))",
    },
    ConstraintSpec {
        name: "ck_system_runtime_control_singleton",
        table: "system_runtime_control",
        kind: ConstraintKind::Check,
        definition: "CHECK ((id = 1))",
    },
    ConstraintSpec {
        name: "ck_system_runtime_control_revision",
        table: "system_runtime_control",
        kind: ConstraintKind::Check,
        definition: "CHECK ((revision >= 0))",
    },
    ConstraintSpec {
        name: "ck_system_runtime_control_transition_revision",
        table: "system_runtime_control_transition",
        kind: ConstraintKind::Check,
        definition: "CHECK (((from_revision >= 0) AND (to_revision = (from_revision + 1))))",
    },
];

pub async fn apply(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    v1::create_validation_programs(manager).await?;
    for spec in CONSTRAINTS {
        v1::create_constraint(manager, *spec)
            .await
            .map_err(|error| {
                DbErr::Custom(format!(
                    "constraint `{}` on `{}` failed: {error}",
                    spec.name, spec.table
                ))
            })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{CONSTRAINTS, ConstraintKind, ConstraintSpec};

    fn constraint(name: &str) -> &'static ConstraintSpec {
        let Some(spec) = CONSTRAINTS.iter().find(|spec| spec.name == name) else {
            panic!("missing relational constraint {name}");
        };
        spec
    }

    #[test]
    fn check_sql_balances() {
        for spec in CONSTRAINTS
            .iter()
            .filter(|spec| spec.kind == ConstraintKind::Check)
        {
            let bytes = spec.definition.as_bytes();
            let mut index = 0;
            let mut in_quote = false;
            let mut depth = 0_i32;
            while index < bytes.len() {
                let byte = bytes[index];
                if byte == b'\'' {
                    if in_quote && bytes.get(index + 1) == Some(&b'\'') {
                        index += 2;
                        continue;
                    }
                    in_quote = !in_quote;
                    index += 1;
                    continue;
                }
                if !in_quote {
                    match byte {
                        b'(' => depth += 1,
                        b')' => {
                            depth -= 1;
                            assert!(
                                depth >= 0,
                                "constraint {} closes an unopened parenthesis",
                                spec.name
                            );
                        }
                        _ => {}
                    }
                }
                index += 1;
            }
            assert!(!in_quote, "constraint {} has an open SQL quote", spec.name);
            assert_eq!(
                depth, 0,
                "constraint {} has unbalanced parentheses",
                spec.name
            );
        }
    }

    #[test]
    fn dataset_v3_is_relational() {
        let source = constraint("ck_quant_source_slice_manifest").definition;
        assert!(source.contains("dataset_format_version'::text))::integer = 3"));
        assert!(!source.contains("dataset_format_version'::text))::integer = 2"));

        let dataset = constraint("ck_quant_training_dataset_manifest").definition;
        for binding in [
            "format_version'::text))::integer = 3",
            "model_family",
            "feature_schema_version",
            "factor_serving_plane",
            "manifest - ARRAY['format_version'",
            "source_fingerprint",
            "trade_policy_artifact_id",
        ] {
            assert!(
                dataset.contains(binding),
                "missing Dataset v3 binding {binding}"
            );
        }

        let plane = constraint("ck_quant_training_dataset_factor_plane").definition;
        assert!(plane.contains("validate_factor_serving_plane"));
        assert!(plane.contains("factor_schema_hash'::text) = factor_schema_hash"));
    }

    #[test]
    fn factor_revision_is_relational() {
        let definition = constraint("ck_quant_factor_definition_document").definition;
        for binding in [
            "validate_factor_definition_document",
            "definition ->> 'name'",
            "definition ->> 'family'",
            "scope = CASE",
            "input_schema_version >= 1",
            "output_schema_version >= 1",
        ] {
            assert!(
                definition.contains(binding),
                "missing factor-revision JSONB binding {binding}"
            );
        }
    }

    #[test]
    fn factor_values_are_relational() {
        assert_eq!(
            constraint("ck_quant_factor_value_explanation").definition,
            "CHECK (public.validate_factor_explanation(explanation))"
        );
        assert_eq!(
            constraint("uq_quant_factor_value_run_vector_definition").definition,
            "UNIQUE (model_run_id, feature_vector_id, factor_definition_id)"
        );
        let definition = constraint("ck_quant_factor_value_state_tuple").definition;
        assert!(
            !definition.contains("status = 'scored'"),
            "factor-value tuple must bind the authoritative value_state column"
        );
        for binding in [
            "value_state = 'scored'",
            "value_state = ANY",
            "'missing_input'",
            "'not_applicable'",
            "value_state = 'indeterminate'",
            "normalized_score BETWEEN 0 AND 1",
            "confidence BETWEEN 0 AND 1",
            "confidence = 0",
        ] {
            assert!(
                definition.contains(binding),
                "missing factor-value tuple binding {binding}"
            );
        }
    }

    #[test]
    fn model_run_terminals_relational() {
        let definition = constraint("ck_quant_model_run_terminal").definition;
        for binding in [
            "status = 'running'",
            "status = 'succeeded'",
            "status = 'failed'",
            "status = 'cancelled'",
            "error_code = 'cancelled_by_operator'",
            "error_code <> 'cancelled_by_operator'",
            "window_end <= (started_at + '00:00:02'::interval)",
            "finished_at >= started_at",
        ] {
            assert!(
                definition.contains(binding),
                "missing model-run terminal binding {binding}"
            );
        }
    }

    #[test]
    fn serving_hash_is_normalized() {
        let definition = constraint("ck_quant_model_version_serving_contract").definition;
        for binding in [
            "jsonb_typeof(serving_contract) = 'object'",
            "contract_version",
            "contract_hash",
            "bindings",
            "octet_length(serving_contract_hash) = 32",
            "encode(serving_contract_hash, 'hex'",
            "serving_contract - ARRAY",
        ] {
            assert!(
                definition.contains(binding),
                "missing model-serving persistence binding {binding}"
            );
        }
    }

    #[test]
    fn model_documents_are_closed() {
        let objective = constraint("ck_quant_model_version_training_objective").definition;
        for binding in [
            "format_version')::integer = 2",
            "learning_to_rank",
            "classical_pointwise",
            "governed_sell_estimator",
            "oof_predictions_required",
            "hand_authored",
            "training_objective - ARRAY",
        ] {
            assert!(
                objective.contains(binding),
                "missing training-objective persistence binding {binding}"
            );
        }

        let metrics = constraint("ck_quant_model_version_metrics").definition;
        for binding in [
            "format_version')::integer = 2",
            "governed_sell_estimator",
            "resolved_label_rows",
            "position_state_rows",
            "oof_predictions_required",
            "fitted_feature_matrix",
            "metrics - ARRAY",
        ] {
            assert!(
                metrics.contains(binding),
                "missing model-metrics persistence binding {binding}"
            );
        }
    }

    #[test]
    fn feedback_schema_is_relational() {
        let cycle = constraint("ck_quant_feedback_cycle_identity").definition;
        for binding in [
            "public.validate_content_hash_array(capability_registry_hashes)",
            "champion_serving_contract_hash",
            "candidate_family -> 'shared_evaluation'",
            "candidate_family -> 'comparison_contract'",
            "'romano_wolf_basic'",
            "'plus_one_greater_or_equal'",
            "'equal_statistic_group'",
            "'blake3_counter_rejection_v1'",
            "jsonb_array_length(candidate_family -> 'candidates'::text) BETWEEN 1 AND 32",
            "candidate_family_hash",
            "idempotency_hash",
            "research_profile_artifact_id",
        ] {
            assert!(cycle.contains(binding), "missing cycle binding {binding}");
        }
        let state = constraint("ck_quant_feedback_cycle_state").definition;
        for binding in [
            "status = 'queued'",
            "status = 'running'",
            "status = 'succeeded'",
            "status = 'failed'",
            "status = 'cancelled'",
            "decision IS NOT NULL",
            "lease_owner IS NOT NULL",
        ] {
            assert!(state.contains(binding), "missing cycle state {binding}");
        }

        let drift = constraint("ck_quant_drift_report").definition;
        for binding in [
            "kind = 'data'",
            "kind = 'concept'",
            "kind = 'label'",
            "baseline_window_end <= evaluation_window_start",
            "threshold NOT IN",
            "observed_value NOT IN",
            "kolmogorov_smirnov_p_value",
            "insufficient_evidence",
            "sample_count > 0",
        ] {
            assert!(drift.contains(binding), "missing drift binding {binding}");
        }

        let evaluation = constraint("ck_quant_feedback_evaluation_use").definition;
        for binding in [
            "purpose = 'promotion_comparison'",
            "dataset_purpose = 'evaluation'",
            "cohort_manifest_hash",
            "comparison_contract_hash",
            "semantic_use_hash",
            "cpcv_artifact_uri",
            "reserved_at = created_at",
            "evaluation_use_hash",
        ] {
            assert!(
                evaluation.contains(binding),
                "missing evaluation-use binding {binding}"
            );
        }
        let dataset_fk = constraint("fk_quant_feedback_evaluation_dataset").definition;
        assert!(dataset_fk.contains("evaluation_dataset_hash"));
        assert!(dataset_fk.contains("evaluation_artifact_bytes_hash"));

        let shadow = constraint("ck_quant_shadow_comparison_generation_identity").definition;
        for binding in [
            "active_model_version_id <> shadow_model_version_id",
            "active_serving_contract_hash <> shadow_serving_contract_hash",
            "decision_policy_snapshot_hash",
            "policy_bundle_generation > 0",
        ] {
            assert!(
                shadow.contains(binding),
                "missing shadow generation binding {binding}"
            );
        }

        let job_lineage = constraint("ck_quant_research_job_feedback_lineage").definition;
        for binding in [
            "(feedback_cycle_id IS NULL) = (feedback_stage IS NULL)",
            "feedback_stage <> 'trigger'",
            "parent_job_id <> job_id",
        ] {
            assert!(
                job_lineage.contains(binding),
                "missing research-job lineage binding {binding}"
            );
        }
        let parent_lineage = constraint("fk_quant_research_job_parent_lineage").definition;
        assert!(parent_lineage.contains("(parent_job_id, feedback_cycle_id, feedback_stage)"));
        assert!(parent_lineage.contains("(job_id, feedback_cycle_id, feedback_stage)"));
        let job_result = constraint("ck_quant_research_job_result_reference").definition;
        assert!(job_result.contains(
            "(kind = 'feedback_decision'::qp_research_job_kind) AND (result_kind = 'feedback_decision_artifact'::qp_research_job_result_kind)"
        ));
        let job_artifact = constraint("ck_quant_research_job_artifact_reference").definition;
        for binding in [
            "(result_artifact_uri IS NULL) = (result_artifact_hash IS NULL)",
            "octet_length(result_artifact_uri) >= 1",
            "octet_length(result_artifact_uri) <= 4096",
            "^[a-z][a-z0-9+.-]*://.+$",
            "^blake3:[0-9a-f]{64}$",
            "feedback_coverage_artifact",
            "feedback_drift_artifact",
            "feedback_learning_stage_artifact",
            "feedback_comparison_artifact",
            "feedback_shadow_replay_artifact",
            "feedback_decision_artifact",
        ] {
            assert!(
                job_artifact.contains(binding),
                "missing research-job artifact binding {binding}"
            );
        }
        let stage_job = constraint("fk_quant_feedback_stage_job_lineage").definition;
        assert!(stage_job.contains("(research_job_id, feedback_cycle_id, stage)"));
        assert!(stage_job.contains("(job_id, feedback_cycle_id, feedback_stage)"));

        for name in [
            "fk_quant_research_job_cycle",
            "fk_quant_research_job_parent",
            "fk_quant_research_job_parent_lineage",
            "fk_quant_feedback_stage_cycle",
            "fk_quant_feedback_stage_job_lineage",
            "fk_quant_drift_report_cycle",
            "fk_quant_feedback_evaluation_cycle",
            "fk_quant_feedback_evaluation_profile",
            "fk_quant_feedback_evaluation_dataset",
            "fk_quant_feedback_evaluation_champion",
        ] {
            let definition = constraint(name).definition;
            assert!(definition.contains("ON UPDATE NO ACTION"));
            assert!(definition.contains("ON DELETE NO ACTION"));
            assert!(!definition.contains("CASCADE"));
        }
    }

    #[test]
    fn feedback_bounds_are_explicit() {
        let outbox = constraint("ck_quant_feedback_event_outbox").definition;
        assert!(outbox.contains("octet_length(last_error) >= 1"));
        assert!(outbox.contains("octet_length(last_error) <= 2048"));
        assert!(!outbox.contains("octet_length(last_error) BETWEEN"));

        let job = constraint("ck_quant_research_job_artifact_reference").definition;
        assert!(job.contains("octet_length(result_artifact_uri) >= 1"));
        assert!(job.contains("octet_length(result_artifact_uri) <= 4096"));
        assert!(!job.contains("octet_length(result_artifact_uri) BETWEEN"));
    }
}
