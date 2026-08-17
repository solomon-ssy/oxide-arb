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
                = 'mean_rolling_fold_target_rank_ic'
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
            AND metric = 'target_rank_ic_drop'::qp_feedback_drift_metric
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
const PROMOTION_PERMIT_CHECK: &str = r"CHECK (
    octet_length(idempotency_key) >= 8
    AND octet_length(idempotency_key) <= 128
    AND idempotency_key ~ '^[!-~]+$'::text
    AND scope_hash ~ '^blake3:[0-9a-f]{64}$'::text
    AND issuance_hash ~ '^blake3:[0-9a-f]{64}$'::text
    AND jsonb_typeof(profile_ref) = 'object'::text
    AND profile_ref ?& ARRAY['id'::text, 'version'::text, 'content_hash'::text]
    AND profile_ref - ARRAY['id'::text, 'version'::text, 'content_hash'::text] = '{}'::jsonb
    AND profile_ref ->> 'id'::text ~ '^[a-z0-9_]+$'::text
    AND (profile_ref ->> 'version'::text)::bigint > 0
    AND profile_ref ->> 'content_hash'::text = profile_hash
    AND research_profile_artifact_id =
        'rpa:'::text
        || (profile_ref ->> 'id'::text)
        || ':'::text
        || (profile_ref ->> 'version'::text)
        || ':'::text
        || profile_hash
    AND (
        (
            category = 'crypto'::qp_market_category
            AND profile_ref ->> 'id'::text = 'crypto_price_15m'::text
        )
        OR (
            category = 'weather'::qp_market_category
            AND profile_ref ->> 'id'::text = 'weather_forecast_24h'::text
        )
    )
    AND expected_policy_generation > 0
    AND expected_runtime_control_revision >= 0
    AND profile_hash ~ '^blake3:[0-9a-f]{64}$'::text
    AND expected_snapshot_hash ~ '^blake3:[0-9a-f]{64}$'::text
    AND champion_serving_contract_hash ~ '^blake3:[0-9a-f]{64}$'::text
    AND candidate_model_version_id <> champion_model_version_id
    AND candidate_manifest_hash ~ '^blake3:[0-9a-f]{64}$'::text
    AND promotion_gate_hash ~ '^blake3:[0-9a-f]{64}$'::text
    AND non_route_policy_hash ~ '^blake3:[0-9a-f]{64}$'::text
    AND serving_constraints_hash ~ '^blake3:[0-9a-f]{64}$'::text
    AND preflight_hash ~ '^blake3:[0-9a-f]{64}$'::text
    AND (
        allowed_runtime_modes = ARRAY['report_only'::qp_quant_runtime_mode]
        OR allowed_runtime_modes = ARRAY['semi_auto'::qp_quant_runtime_mode]
        OR allowed_runtime_modes = ARRAY['auto_execution'::qp_quant_runtime_mode]
        OR allowed_runtime_modes = ARRAY[
            'report_only'::qp_quant_runtime_mode,
            'semi_auto'::qp_quant_runtime_mode
        ]
        OR allowed_runtime_modes = ARRAY[
            'report_only'::qp_quant_runtime_mode,
            'auto_execution'::qp_quant_runtime_mode
        ]
        OR allowed_runtime_modes = ARRAY[
            'semi_auto'::qp_quant_runtime_mode,
            'auto_execution'::qp_quant_runtime_mode
        ]
        OR allowed_runtime_modes = ARRAY[
            'report_only'::qp_quant_runtime_mode,
            'semi_auto'::qp_quant_runtime_mode,
            'auto_execution'::qp_quant_runtime_mode
        ]
    )
    AND octet_length(issued_by_username) >= 1
    AND octet_length(issued_by_username) <= 256
    AND issued_by_username = btrim(issued_by_username)
    AND issued_by_username !~ '[[:cntrl:]]'::text
    AND issued_by_role ~ '^[a-z0-9_]{1,64}$'::text
    AND octet_length(issuance_reason) >= 1
    AND octet_length(issuance_reason) <= 2048
    AND issuance_reason = btrim(issuance_reason)
    AND issuance_reason !~ '[[:cntrl:]]'::text
    AND expires_at > issued_at
    AND (
        (
            revoked_by_user_id IS NULL
            AND revoked_by_username IS NULL
            AND revoked_by_role IS NULL
            AND revocation_reason IS NULL
            AND revoked_at IS NULL
            AND revision = 0
            AND updated_at = issued_at
        )
        OR (
            revoked_by_user_id IS NOT NULL
            AND revoked_by_username IS NOT NULL
            AND octet_length(revoked_by_username) >= 1
            AND octet_length(revoked_by_username) <= 256
            AND revoked_by_username = btrim(revoked_by_username)
            AND revoked_by_username !~ '[[:cntrl:]]'::text
            AND revoked_by_role ~ '^[a-z0-9_]{1,64}$'::text
            AND revocation_reason IS NOT NULL
            AND octet_length(revocation_reason) >= 1
            AND octet_length(revocation_reason) <= 2048
            AND revocation_reason = btrim(revocation_reason)
            AND revocation_reason !~ '[[:cntrl:]]'::text
            AND revoked_at >= issued_at
            AND revision = 1
            AND updated_at = revoked_at
        )
    )
)";
const MARKET_SELECTION_EVIDENCE_CHECK: &str = r"CHECK (
    selector_hash ~ '^blake3:[0-9a-f]{64}$'
    AND jsonb_typeof(selector_evidence) = 'object'
    AND selector_evidence ?& ARRAY[
        'selector_hash',
        'contract_hash',
        'boundary_hash',
        'selection_policy_hash',
        'data_quality_policy_hash',
        'feature_schema_hash',
        'model_requirements_hash',
        'candidates_hash',
        'candidate_catalog_hash',
        'candidate_book_hash',
        'candidate_domain_hash',
        'candidate_decision_hash',
        'included_hash',
        'excluded_hash',
        'exclusion_summary_hash'
    ]
    AND selector_evidence - ARRAY[
        'selector_hash',
        'contract_hash',
        'boundary_hash',
        'selection_policy_hash',
        'data_quality_policy_hash',
        'feature_schema_hash',
        'model_requirements_hash',
        'candidates_hash',
        'candidate_catalog_hash',
        'candidate_book_hash',
        'candidate_domain_hash',
        'candidate_decision_hash',
        'included_hash',
        'excluded_hash',
        'exclusion_summary_hash'
    ] = '{}'::jsonb
    AND selector_evidence ->> 'selector_hash' = selector_hash
    AND selector_evidence ->> 'contract_hash' ~ '^blake3:[0-9a-f]{64}$'
    AND selector_evidence ->> 'boundary_hash' ~ '^blake3:[0-9a-f]{64}$'
    AND selector_evidence ->> 'selection_policy_hash' ~ '^blake3:[0-9a-f]{64}$'
    AND selector_evidence ->> 'data_quality_policy_hash' ~ '^blake3:[0-9a-f]{64}$'
    AND selector_evidence ->> 'feature_schema_hash' ~ '^blake3:[0-9a-f]{64}$'
    AND selector_evidence ->> 'model_requirements_hash' ~ '^blake3:[0-9a-f]{64}$'
    AND selector_evidence ->> 'candidates_hash' ~ '^blake3:[0-9a-f]{64}$'
    AND selector_evidence ->> 'candidate_catalog_hash' ~ '^blake3:[0-9a-f]{64}$'
    AND selector_evidence ->> 'candidate_book_hash' ~ '^blake3:[0-9a-f]{64}$'
    AND selector_evidence ->> 'candidate_domain_hash' ~ '^blake3:[0-9a-f]{64}$'
    AND selector_evidence ->> 'candidate_decision_hash' ~ '^blake3:[0-9a-f]{64}$'
    AND selector_evidence ->> 'included_hash' ~ '^blake3:[0-9a-f]{64}$'
    AND selector_evidence ->> 'excluded_hash' ~ '^blake3:[0-9a-f]{64}$'
    AND selector_evidence ->> 'exclusion_summary_hash' ~ '^blake3:[0-9a-f]{64}$'
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
        name: "ck_quant_market_selection_evidence",
        table: "quant_market_selection",
        kind: ConstraintKind::Check,
        definition: MARKET_SELECTION_EVIDENCE_CHECK,
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
        name: "ck_quant_attribution_artifact",
        table: "quant_attribution_artifact",
        kind: ConstraintKind::Check,
        definition: "CHECK ((artifact_hash ~ '^blake3:[0-9a-f]{64}$'::text AND octet_length(artifact_uri) BETWEEN 1 AND 4096 AND artifact_uri ~ '^[a-z][a-z0-9+.-]*://.+$'::text AND source_cutoff <= available_at AND available_at = created_at AND (((artifact_kind = ANY (ARRAY['prediction_explanation'::qp_attribution_artifact_kind, 'decision_intervention_replay'::qp_attribution_artifact_kind])) AND model_version_id IS NOT NULL AND recommendation_id IS NOT NULL AND order_intent_id IS NULL) OR ((artifact_kind = ANY (ARRAY['resolution_outcome_association'::qp_attribution_artifact_kind, 'execution_outcome_association'::qp_attribution_artifact_kind])) AND model_version_id IS NOT NULL AND recommendation_id IS NULL AND order_intent_id IS NULL) OR ((artifact_kind = ANY (ARRAY['execution_trajectory'::qp_attribution_artifact_kind, 'policy_counterfactual_outcome'::qp_attribution_artifact_kind])) AND model_version_id IS NULL AND recommendation_id IS NOT NULL AND order_intent_id IS NOT NULL))))",
    },
    ConstraintSpec {
        name: "uq_quant_order_intent_recommendation",
        table: "quant_order_intent",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (order_intent_id, recommendation_id)",
    },
    ConstraintSpec {
        name: "fk_quant_attribution_cycle",
        table: "quant_attribution_artifact",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (source_feedback_cycle_id) REFERENCES public.quant_feedback_cycle(feedback_cycle_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_attribution_model",
        table: "quant_attribution_artifact",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (model_version_id) REFERENCES public.quant_model_version(model_version_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_attribution_recommendation",
        table: "quant_attribution_artifact",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (recommendation_id) REFERENCES public.quant_recommendation(recommendation_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_attribution_intent",
        table: "quant_attribution_artifact",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (order_intent_id, recommendation_id) REFERENCES public.quant_order_intent(order_intent_id, recommendation_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "ck_quant_model_candidate_manifest",
        table: "quant_model_candidate_manifest",
        kind: ConstraintKind::Check,
        definition: concat!(
            "CHECK ((candidate_recipe_hash ~ '^blake3:[0-9a-f]{64}$'::text ",
            "AND promotion_gate_hash ~ '^blake3:[0-9a-f]{64}$'::text ",
            "AND manifest_hash ~ '^blake3:[0-9a-f]{64}$'::text ",
            "AND jsonb_typeof(document) = 'object'::text ",
            "AND (document ->> 'format_version'::text)::integer = 4 ",
            "AND (document ->> 'feedback_cycle_id'::text)::uuid = feedback_cycle_id ",
            "AND document ->> 'candidate_recipe_hash'::text = candidate_recipe_hash ",
            "AND (document ->> 'model_version_id'::text)::uuid = model_version_id ",
            "AND document ->> 'model_artifact_hash'::text ~ '^blake3:[0-9a-f]{64}$'::text ",
            "AND document ->> 'serving_contract_hash'::text ~ '^blake3:[0-9a-f]{64}$'::text ",
            "AND document ->> 'training_dataset_hash'::text ~ '^blake3:[0-9a-f]{64}$'::text ",
            "AND document ->> 'feature_schema_hash'::text ~ '^blake3:[0-9a-f]{64}$'::text ",
            "AND document ->> 'input_contract_hash'::text ~ '^blake3:[0-9a-f]{64}$'::text ",
            "AND document ->> 'input_transform_hash'::text ~ '^blake3:[0-9a-f]{64}$'::text ",
            "AND document ->> 'cpcv_path_set_hash'::text ~ '^blake3:[0-9a-f]{64}$'::text ",
            "AND document ->> 'feedback_policy_hash'::text ~ '^blake3:[0-9a-f]{64}$'::text ",
            "AND document ->> 'decision_policy_snapshot_hash'::text ~ '^blake3:[0-9a-f]{64}$'::text ",
            "AND jsonb_typeof(document -> 'explanation_validation'::text) = 'object'::text ",
            "AND (document -> 'explanation_validation'::text ->> 'format_version'::text)::integer = 3 ",
            "AND document -> 'explanation_validation'::text ->> 'input_contract_hash'::text = document ->> 'input_contract_hash'::text ",
            "AND document -> 'explanation_validation'::text ->> 'report_hash'::text ~ '^blake3:[0-9a-f]{64}$'::text ",
            "AND jsonb_typeof(document -> 'promotion_gate'::text) = 'object'::text ",
            "AND (document -> 'promotion_gate'::text ->> 'format_version'::text)::integer = 2 ",
            "AND document -> 'promotion_gate'::text ->> 'promotion_gate_hash'::text = promotion_gate_hash ",
            "AND (document -> 'promotion_gate'::text ->> 'feedback_cycle_id'::text)::uuid = feedback_cycle_id ",
            "AND document -> 'promotion_gate'::text ->> 'candidate_recipe_hash'::text = candidate_recipe_hash ",
            "AND (document -> 'promotion_gate'::text ->> 'candidate_model_version_id'::text)::uuid = model_version_id ",
            "AND document -> 'promotion_gate'::text -> 'profile_ref'::text = document -> 'profile_ref'::text ",
            "AND document -> 'promotion_gate'::text ->> 'category'::text = document ->> 'category'::text ",
            "AND document -> 'promotion_gate'::text ->> 'feedback_policy_hash'::text = document ->> 'feedback_policy_hash'::text ",
            "AND document -> 'promotion_gate'::text ->> 'decision_policy_snapshot_hash'::text = document ->> 'decision_policy_snapshot_hash'::text ",
            "AND document -> 'promotion_gate'::text ->> 'truth_freeze_hash'::text ~ '^blake3:[0-9a-f]{64}$'::text ",
            "AND document -> 'promotion_gate'::text ->> 'attribution_manifest_hash'::text ~ '^blake3:[0-9a-f]{64}$'::text ",
            "AND document -> 'promotion_gate'::text ->> 'validation_artifact_hash'::text ~ '^blake3:[0-9a-f]{64}$'::text ",
            "AND document -> 'promotion_gate'::text ->> 'quality_gate_report_hash'::text ~ '^blake3:[0-9a-f]{64}$'::text ",
            "AND document -> 'promotion_gate'::text ->> 'comparison_artifact_hash'::text ~ '^blake3:[0-9a-f]{64}$'::text ",
            "AND document -> 'promotion_gate'::text ->> 'cpcv_path_set_hash'::text = document ->> 'cpcv_path_set_hash'::text ",
            "AND document -> 'promotion_gate'::text ->> 'explanation_validation_hash'::text = document -> 'explanation_validation'::text ->> 'report_hash'::text))"
        ),
    },
    ConstraintSpec {
        name: "uq_quant_model_candidate_permit_binding",
        table: "quant_model_candidate_manifest",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (manifest_id, model_version_id, manifest_hash, promotion_gate_hash)",
    },
    ConstraintSpec {
        name: "uq_quant_model_candidate_identity_hash",
        table: "quant_model_candidate_manifest",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (manifest_id, manifest_hash)",
    },
    ConstraintSpec {
        name: "fk_quant_model_candidate_cycle",
        table: "quant_model_candidate_manifest",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (feedback_cycle_id) REFERENCES public.quant_feedback_cycle(feedback_cycle_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_model_candidate_version",
        table: "quant_model_candidate_manifest",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (model_version_id) REFERENCES public.quant_model_version(model_version_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "ck_quant_feedback_cycle_identity",
        table: "quant_feedback_cycle",
        kind: ConstraintKind::Check,
        definition: concat!(
            "CHECK ((jsonb_typeof(idempotency_key) = 'object'::text AND idempotency_key ?& ARRAY['format_version'::text, 'profile_ref'::text, 'feedback_policy_hash'::text, 'label_cutoff'::text, 'champion_model_version_id'::text, 'champion_serving_contract_hash'::text, 'champion_model_spec_id'::text, 'champion_model_spec_definition_hash'::text, 'champion_model_family'::text, 'route'::text, 'decision_policy_snapshot_id'::text, 'decision_policy_snapshot_hash'::text, 'policy_bundle_generation'::text, 'route_generation'::text, 'evaluation_mode'::text, 'parent_cycle_id'::text, 'forced_idempotency_key'::text] ",
            "AND (idempotency_key - ARRAY['format_version'::text, 'profile_ref'::text, 'feedback_policy_hash'::text, 'label_cutoff'::text, 'champion_model_version_id'::text, 'champion_serving_contract_hash'::text, 'champion_model_spec_id'::text, 'champion_model_spec_definition_hash'::text, 'champion_model_family'::text, 'route'::text, 'decision_policy_snapshot_id'::text, 'decision_policy_snapshot_hash'::text, 'policy_bundle_generation'::text, 'route_generation'::text, 'evaluation_mode'::text, 'parent_cycle_id'::text, 'forced_idempotency_key'::text]) = '{}'::jsonb ",
            "AND (idempotency_key ->> 'format_version')::integer = 1 AND idempotency_key -> 'profile_ref' = profile_ref AND idempotency_key ->> 'feedback_policy_hash' = feedback_policy_hash AND (idempotency_key ->> 'label_cutoff')::timestamp with time zone = label_cutoff AND (idempotency_key ->> 'champion_model_version_id')::uuid = champion_model_version_id AND idempotency_key ->> 'champion_serving_contract_hash' = champion_serving_contract_hash ",
            "AND (idempotency_key ->> 'champion_model_spec_id')::uuid = champion_model_spec_id AND idempotency_key ->> 'champion_model_spec_definition_hash' = champion_model_spec_definition_hash AND idempotency_key ->> 'champion_model_family' = champion_model_family::text AND idempotency_key -> 'route' = route AND (idempotency_key ->> 'decision_policy_snapshot_id')::uuid = decision_policy_snapshot_id AND idempotency_key ->> 'decision_policy_snapshot_hash' = decision_policy_snapshot_hash ",
            "AND (idempotency_key ->> 'policy_bundle_generation')::bigint = policy_bundle_generation AND (idempotency_key ->> 'route_generation')::bigint = route_generation AND idempotency_key ->> 'evaluation_mode' = evaluation_mode::text AND ((idempotency_key ->> 'parent_cycle_id')::uuid IS NOT DISTINCT FROM parent_cycle_id) AND (idempotency_key ->> 'forced_idempotency_key' IS NOT DISTINCT FROM forced_idempotency_key) ",
            "AND jsonb_typeof(profile_ref) = 'object'::text AND profile_ref ?& ARRAY['id'::text, 'version'::text, 'content_hash'::text] AND (profile_ref - ARRAY['id'::text, 'version'::text, 'content_hash'::text]) = '{}'::jsonb AND profile_ref ->> 'id' ~ '^[a-z0-9_]+$'::text AND (profile_ref ->> 'version')::bigint > 0 AND profile_ref ->> 'content_hash' = profile_hash AND research_profile_artifact_id = (((('rpa:'::text || (profile_ref ->> 'id')) || ':'::text) || (profile_ref ->> 'version')) || ':'::text) || profile_hash ",
            "AND route IN ('\"pooled\"'::jsonb, '\"crypto\"'::jsonb, '\"weather\"'::jsonb) AND idempotency_hash ~ '^blake3:[0-9a-f]{64}$'::text AND profile_hash ~ '^blake3:[0-9a-f]{64}$'::text AND feedback_policy_hash ~ '^blake3:[0-9a-f]{64}$'::text AND champion_serving_contract_hash ~ '^blake3:[0-9a-f]{64}$'::text AND champion_model_spec_definition_hash ~ '^blake3:[0-9a-f]{64}$'::text AND decision_policy_snapshot_hash ~ '^blake3:[0-9a-f]{64}$'::text ",
            "AND policy_bundle_generation > 0 AND route_generation > 0 AND parent_cycle_id IS DISTINCT FROM feedback_cycle_id AND ((evaluation_mode = 'conditional'::qp_feedback_evaluation_mode AND parent_cycle_id IS NULL AND forced_idempotency_key IS NULL) OR (evaluation_mode = 'forced_retraining'::qp_feedback_evaluation_mode AND parent_cycle_id IS NOT NULL AND forced_idempotency_key ~ '^[!-~]{8,128}$'::text)) AND label_cutoff <= created_at AND updated_at >= created_at AND generation >= 0))",
        ),
    },
    ConstraintSpec {
        name: "ck_quant_feedback_cycle_state",
        table: "quant_feedback_cycle",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((lease_owner IS NULL) = (lease_expires_at IS NULL)) AND ((started_at IS NULL) OR (started_at >= created_at)) AND ((lease_expires_at IS NULL) OR (started_at IS NOT NULL AND lease_expires_at > started_at)) AND ((stage_resume_after IS NULL) OR (started_at IS NOT NULL AND stage_resume_after > started_at)) AND ((cancel_requested_at IS NULL) OR (cancel_requested_at >= created_at AND (completed_at IS NULL OR cancel_requested_at <= completed_at))) AND ((completed_at IS NULL) OR (completed_at >= created_at AND completed_at <= updated_at AND (started_at IS NULL OR completed_at >= started_at))) AND ((terminal_reason_code IS NULL) OR (terminal_reason_code ~ '^[a-z][a-z0-9_.]{0,127}$'::text)) AND (((status = 'queued'::qp_feedback_cycle_status) AND decision IS NULL AND terminal_reason_code IS NULL AND started_at IS NULL AND completed_at IS NULL AND lease_owner IS NULL AND stage_resume_after IS NULL) OR ((status = 'running'::qp_feedback_cycle_status) AND decision IS NULL AND terminal_reason_code IS NULL AND started_at IS NOT NULL AND completed_at IS NULL AND (((lease_owner IS NOT NULL) AND stage_resume_after IS NULL) OR ((lease_owner IS NULL) AND stage_resume_after IS NOT NULL))) OR ((status = 'succeeded'::qp_feedback_cycle_status) AND decision IS NOT NULL AND terminal_reason_code IS NOT NULL AND started_at IS NOT NULL AND completed_at IS NOT NULL AND lease_owner IS NULL AND stage_resume_after IS NULL) OR ((status = 'failed'::qp_feedback_cycle_status) AND decision IS NULL AND terminal_reason_code IS NOT NULL AND started_at IS NOT NULL AND completed_at IS NOT NULL AND lease_owner IS NULL AND stage_resume_after IS NULL) OR ((status = 'cancelled'::qp_feedback_cycle_status) AND decision IS NULL AND terminal_reason_code IS NOT NULL AND completed_at IS NOT NULL AND lease_owner IS NULL AND stage_resume_after IS NULL) OR ((status = 'quarantined'::qp_feedback_cycle_status) AND decision IS NULL AND terminal_reason_code = 'invalid_coordinator_state'::text AND started_at IS NOT NULL AND completed_at IS NOT NULL AND lease_owner IS NULL AND stage_resume_after IS NULL))))",
    },
    ConstraintSpec {
        name: "uq_quant_feedback_cycle_cutoff",
        table: "quant_feedback_cycle",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (feedback_cycle_id, label_cutoff)",
    },
    ConstraintSpec {
        name: "uq_quant_feedback_cycle_trigger_lineage",
        table: "quant_feedback_cycle",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (feedback_cycle_id, evaluation_mode)",
    },
    ConstraintSpec {
        name: "uq_quant_feedback_cycle_evaluation_lineage",
        table: "quant_feedback_cycle",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (feedback_cycle_id, research_profile_artifact_id, label_cutoff, champion_model_version_id, champion_serving_contract_hash)",
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
        name: "fk_quant_feedback_cycle_champion_model_spec",
        table: "quant_feedback_cycle",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (champion_model_spec_id) REFERENCES public.quant_model_spec(model_spec_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_feedback_cycle_policy_snapshot",
        table: "quant_feedback_cycle",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (decision_policy_snapshot_id) REFERENCES public.decision_policy_snapshot(decision_policy_snapshot_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_feedback_cycle_parent",
        table: "quant_feedback_cycle",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (parent_cycle_id) REFERENCES public.quant_feedback_cycle(feedback_cycle_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "ck_quant_feedback_scheduler_state",
        table: "quant_feedback_scheduler_state",
        kind: ConstraintKind::Check,
        definition: "CHECK ((research_profile_id ~ '^[a-z0-9_]+$'::text AND profile_hash ~ '^blake3:[0-9a-f]{64}$'::text AND feedback_policy_hash ~ '^blake3:[0-9a-f]{64}$'::text AND cadence_secs > 0 AND cooldown_secs >= cadence_secs AND next_due_at > 'epoch'::timestamp with time zone AND attempt >= 0 AND pause_revision >= 0 AND revision >= 0 AND coalesced_gap_count >= 0 AND settlement_failure_count >= 0 AND ((lease_owner IS NULL) = (lease_expires_at IS NULL)) AND ((pending_cutoff IS NULL) = (pending_started_at IS NULL)) AND (pending_cutoff IS NULL OR pending_cutoff <= pending_started_at) AND ((last_cycle_id IS NULL) = (last_cutoff IS NULL)) AND ((retry_at IS NULL) = (last_error IS NULL)) AND ((retry_at IS NULL) = (last_failure_kind IS NULL)) AND ((lease_owner IS NULL AND retry_at IS NULL) OR pending_cutoff IS NOT NULL) AND ((last_coalesced_from IS NULL) = (last_coalesced_to IS NULL)) AND (last_coalesced_from IS NULL OR last_coalesced_from <= last_coalesced_to) AND ((coalesced_gap_count = 0) = (last_coalesced_from IS NULL)) AND ((last_settlement_failed_at IS NULL) = (last_settlement_error IS NULL)) AND ((settlement_failure_count = 0) = (last_settlement_failed_at IS NULL)) AND (last_error IS NULL OR (octet_length(last_error) >= 1 AND octet_length(last_error) <= 4096 AND btrim(last_error) <> ''::text)) AND (last_settlement_error IS NULL OR (octet_length(last_settlement_error) >= 1 AND octet_length(last_settlement_error) <= 4096 AND btrim(last_settlement_error) <> ''::text)) AND (((paused = false) AND pause_reason_code IS NULL AND pause_note IS NULL) OR ((paused = true) AND pause_reason_code ~ '^[a-z][a-z0-9_]{0,127}$'::text AND octet_length(pause_note) >= 1 AND octet_length(pause_note) <= 1024 AND btrim(pause_note) <> ''::text)) AND updated_at >= created_at))",
    },
    ConstraintSpec {
        name: "fk_quant_feedback_scheduler_profile",
        table: "quant_feedback_scheduler_state",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (research_profile_artifact_id) REFERENCES public.research_profile_artifact(research_profile_artifact_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_feedback_scheduler_cycle",
        table: "quant_feedback_scheduler_state",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (last_cycle_id) REFERENCES public.quant_feedback_cycle(feedback_cycle_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
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
        definition: "CHECK ((event_sequence > 0 AND occurred_at <= created_at AND event_hash ~ '^blake3:[0-9a-f]{64}$'::text AND ((evidence_uri IS NULL AND evidence_hash IS NULL) OR (evidence_uri IS NOT NULL AND evidence_hash IS NOT NULL AND octet_length(evidence_uri) >= 1 AND octet_length(evidence_uri) <= 4096 AND evidence_uri ~ '^[a-z][a-z0-9+.-]*://.+$'::text AND evidence_hash ~ '^blake3:[0-9a-f]{64}$'::text)) AND ((actor IS NULL) OR (octet_length(actor) >= 1 AND octet_length(actor) <= 256 AND actor = btrim(actor) AND actor !~ '[[:cntrl:]]'::text)) AND ((reason_code IS NULL) OR (reason_code ~ '^[a-z][a-z0-9_.]{0,127}$'::text)) AND (((event_kind = 'triggered'::qp_feedback_stage_event_kind) AND stage = 'trigger'::qp_feedback_stage AND trigger_family IS NOT NULL AND research_job_id IS NULL AND actor IS NOT NULL AND reason_code IS NOT NULL) OR ((event_kind = 'cancellation_requested'::qp_feedback_stage_event_kind) AND stage <> 'trigger'::qp_feedback_stage AND trigger_family IS NULL AND actor IS NOT NULL AND reason_code IS NOT NULL) OR ((event_kind = ANY (ARRAY['job_linked'::qp_feedback_stage_event_kind, 'started'::qp_feedback_stage_event_kind])) AND stage <> 'trigger'::qp_feedback_stage AND trigger_family IS NULL AND research_job_id IS NOT NULL AND actor IS NULL AND reason_code IS NULL) OR ((event_kind = 'succeeded'::qp_feedback_stage_event_kind) AND stage <> 'trigger'::qp_feedback_stage AND trigger_family IS NULL AND research_job_id IS NOT NULL AND actor IS NULL AND reason_code IS NULL AND evidence_uri IS NOT NULL) OR ((event_kind = ANY (ARRAY['failed'::qp_feedback_stage_event_kind, 'cancelled'::qp_feedback_stage_event_kind, 'lease_recovered'::qp_feedback_stage_event_kind])) AND stage <> 'trigger'::qp_feedback_stage AND trigger_family IS NULL AND research_job_id IS NOT NULL AND actor IS NULL AND reason_code IS NOT NULL))))",
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
        name: "uq_quant_feedback_stage_fault_head",
        table: "quant_feedback_stage_event",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (feedback_stage_event_id, feedback_cycle_id, event_sequence, stage, event_hash)",
    },
    ConstraintSpec {
        name: "ck_quant_feedback_coordinator_fault",
        table: "quant_feedback_coordinator_fault",
        kind: ConstraintKind::Check,
        definition: "CHECK ((lease_generation >= 0 AND fault_code = 'invalid_coordinator_state'::text AND octet_length(detail) BETWEEN 1 AND 2048 AND detail = btrim(detail) AND detail !~ '[[:cntrl:]]'::text AND detail_hash ~ '^blake3:[0-9a-f]{64}$'::text AND fault_hash ~ '^blake3:[0-9a-f]{64}$'::text AND observed_at = created_at AND (((active_stage IS NULL) AND (last_event_sequence IS NULL) AND (last_stage_event_id IS NULL) AND (last_stage_event_hash IS NULL)) OR ((active_stage IS NOT NULL) AND (last_event_sequence IS NOT NULL) AND (last_stage_event_id IS NOT NULL) AND (last_stage_event_hash ~ '^blake3:[0-9a-f]{64}$'::text)))))",
    },
    ConstraintSpec {
        name: "fk_quant_feedback_fault_cycle",
        table: "quant_feedback_coordinator_fault",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (feedback_cycle_id) REFERENCES public.quant_feedback_cycle(feedback_cycle_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_feedback_fault_timeline_head",
        table: "quant_feedback_coordinator_fault",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (last_stage_event_id, feedback_cycle_id, last_event_sequence, active_stage, last_stage_event_hash) REFERENCES public.quant_feedback_stage_event(feedback_stage_event_id, feedback_cycle_id, event_sequence, stage, event_hash) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "ck_quant_feedback_trigger_event",
        table: "quant_feedback_trigger_event",
        kind: ConstraintKind::Check,
        definition: "CHECK ((event_hash ~ '^blake3:[0-9a-f]{64}$'::text AND idempotency_key ~ '^[!-~]{8,128}$'::text AND occurred_at = created_at AND octet_length(actor_label) BETWEEN 1 AND 256 AND actor_label = btrim(actor_label) AND actor_label !~ '[[:cntrl:]]'::text AND reason_code ~ '^[a-z][a-z0-9_]{0,127}$'::text AND ((actor_user_id IS NULL AND actor_role IS NULL) OR (actor_user_id IS NOT NULL AND actor_role ~ '^[a-z][a-z0-9_]{0,63}$'::text))))",
    },
    ConstraintSpec {
        name: "fk_quant_feedback_trigger_cycle",
        table: "quant_feedback_trigger_event",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (feedback_cycle_id, evaluation_mode) REFERENCES public.quant_feedback_cycle(feedback_cycle_id, evaluation_mode) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_feedback_trigger_actor",
        table: "quant_feedback_trigger_event",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (actor_user_id) REFERENCES public.user(id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "ck_quant_feedback_recipe_template",
        table: "quant_feedback_recipe_template",
        kind: ConstraintKind::Check,
        definition: concat!(
            "CHECK ((revision > 0 AND template_hash ~ '^blake3:[0-9a-f]{64}$'::text AND jsonb_typeof(route) = 'string'::text AND route IN ('\"pooled\"'::jsonb, '\"crypto\"'::jsonb, '\"weather\"'::jsonb) AND octet_length(governance_reason) BETWEEN 1 AND 2048 AND governance_reason = btrim(governance_reason) AND governance_reason !~ '[[:cntrl:]]'::text ",
            "AND jsonb_typeof(template) = 'object'::text AND template ?& ARRAY['format_version'::text, 'recipe_template_id'::text, 'revision'::text, 'template_hash'::text, 'profile_ref'::text, 'route'::text, 'model_family'::text, 'training_spec'::text, 'calibration_spec'::text, 'cpcv_spec'::text, 'downside_spec'::text, 'diagnostic_spec'::text, 'responsive_triggers'::text, 'catalog_priority'::text, 'resource_budget'::text, 'status'::text, 'approved_by_user_id'::text, 'approved_by_role'::text, 'approved_at'::text, 'governance_reason'::text] AND (template - ARRAY['format_version'::text, 'recipe_template_id'::text, 'revision'::text, 'template_hash'::text, 'profile_ref'::text, 'route'::text, 'model_family'::text, 'training_spec'::text, 'calibration_spec'::text, 'cpcv_spec'::text, 'downside_spec'::text, 'diagnostic_spec'::text, 'responsive_triggers'::text, 'catalog_priority'::text, 'resource_budget'::text, 'status'::text, 'approved_by_user_id'::text, 'approved_by_role'::text, 'approved_at'::text, 'governance_reason'::text]) = '{}'::jsonb ",
            "AND (template ->> 'format_version')::integer = 3 AND (template ->> 'recipe_template_id')::uuid = recipe_template_id AND (template ->> 'revision')::integer = revision AND template ->> 'template_hash' = template_hash AND template -> 'route' = route AND template ->> 'model_family' = model_family::text AND (template -> 'training_spec' ->> 'model_spec_id')::uuid = model_spec_id AND template ->> 'status' = status::text AND (template ->> 'catalog_priority')::integer = catalog_priority AND template ->> 'governance_reason' = governance_reason ",
            "AND template -> 'training_spec' ->> 'spec_hash' ~ '^blake3:[0-9a-f]{64}$'::text AND template -> 'training_spec' ->> 'model_spec_definition_hash' ~ '^blake3:[0-9a-f]{64}$'::text AND (template -> 'training_spec' ->> 'training_window_days')::numeric > 0 AND template -> 'calibration_spec' ->> 'spec_hash' ~ '^blake3:[0-9a-f]{64}$'::text AND (template -> 'calibration_spec' ->> 'calibration_window_days')::numeric > 0 ",
            "AND jsonb_typeof(template -> 'cpcv_spec') = 'object'::text AND (template -> 'cpcv_spec') ?& ARRAY['spec_hash'::text, 'validation'::text, 'target_horizon_secs'::text, 'purge_embargo_secs'::text] AND ((template -> 'cpcv_spec') - ARRAY['spec_hash'::text, 'validation'::text, 'target_horizon_secs'::text, 'purge_embargo_secs'::text]) = '{}'::jsonb AND template -> 'cpcv_spec' ->> 'spec_hash' ~ '^blake3:[0-9a-f]{64}$'::text AND (template -> 'cpcv_spec' ->> 'target_horizon_secs')::numeric > 0 AND (template -> 'cpcv_spec' ->> 'purge_embargo_secs')::numeric > 0 ",
            "AND jsonb_typeof(template -> 'cpcv_spec' -> 'validation') = 'object'::text AND (template -> 'cpcv_spec' -> 'validation') ?& ARRAY['purge'::text, 'cpcv'::text, 'trials'::text, 'pbo'::text, 'gates'::text] AND ((template -> 'cpcv_spec' -> 'validation') - ARRAY['purge'::text, 'cpcv'::text, 'trials'::text, 'pbo'::text, 'gates'::text]) = '{}'::jsonb ",
            "AND jsonb_typeof(template -> 'cpcv_spec' -> 'validation' -> 'purge') = 'object'::text AND (template -> 'cpcv_spec' -> 'validation' -> 'purge') ?& ARRAY['embargo_pct'::text] AND ((template -> 'cpcv_spec' -> 'validation' -> 'purge') - ARRAY['embargo_pct'::text]) = '{}'::jsonb AND (template -> 'cpcv_spec' -> 'validation' -> 'purge' ->> 'embargo_pct')::numeric >= 0 AND (template -> 'cpcv_spec' -> 'validation' -> 'purge' ->> 'embargo_pct')::numeric < 1 ",
            "AND jsonb_typeof(template -> 'cpcv_spec' -> 'validation' -> 'cpcv') = 'object'::text AND (template -> 'cpcv_spec' -> 'validation' -> 'cpcv') ?& ARRAY['n_groups'::text, 'k_test'::text, 'nested_estimator_holdout_bps'::text, 'nested_estimator_min_groups'::text] AND ((template -> 'cpcv_spec' -> 'validation' -> 'cpcv') - ARRAY['n_groups'::text, 'k_test'::text, 'nested_estimator_holdout_bps'::text, 'nested_estimator_min_groups'::text]) = '{}'::jsonb AND (template -> 'cpcv_spec' -> 'validation' -> 'cpcv' ->> 'n_groups')::integer > 1 AND (template -> 'cpcv_spec' -> 'validation' -> 'cpcv' ->> 'k_test')::integer > 0 AND (template -> 'cpcv_spec' -> 'validation' -> 'cpcv' ->> 'k_test')::integer < (template -> 'cpcv_spec' -> 'validation' -> 'cpcv' ->> 'n_groups')::integer AND (template -> 'cpcv_spec' -> 'validation' -> 'cpcv' ->> 'nested_estimator_holdout_bps')::integer > 0 AND (template -> 'cpcv_spec' -> 'validation' -> 'cpcv' ->> 'nested_estimator_holdout_bps')::integer < 10000 AND (template -> 'cpcv_spec' -> 'validation' -> 'cpcv' ->> 'nested_estimator_min_groups')::integer >= 4 ",
            "AND jsonb_typeof(template -> 'cpcv_spec' -> 'validation' -> 'trials') = 'object'::text AND (template -> 'cpcv_spec' -> 'validation' -> 'trials') ?& ARRAY['lambda_multipliers'::text, 'rank_loss_kinds'::text, 'logistic_alpha_multipliers'::text, 'max_trials'::text] AND ((template -> 'cpcv_spec' -> 'validation' -> 'trials') - ARRAY['lambda_multipliers'::text, 'rank_loss_kinds'::text, 'logistic_alpha_multipliers'::text, 'max_trials'::text]) = '{}'::jsonb AND jsonb_array_length(template -> 'cpcv_spec' -> 'validation' -> 'trials' -> 'lambda_multipliers') > 0 AND jsonb_array_length(template -> 'cpcv_spec' -> 'validation' -> 'trials' -> 'rank_loss_kinds') > 0 AND jsonb_array_length(template -> 'cpcv_spec' -> 'validation' -> 'trials' -> 'logistic_alpha_multipliers') > 0 AND (template -> 'cpcv_spec' -> 'validation' -> 'trials' ->> 'max_trials')::integer > 0 ",
            "AND jsonb_typeof(template -> 'cpcv_spec' -> 'validation' -> 'pbo') = 'object'::text AND (template -> 'cpcv_spec' -> 'validation' -> 'pbo') ?& ARRAY['block_count'::text] AND ((template -> 'cpcv_spec' -> 'validation' -> 'pbo') - ARRAY['block_count'::text]) = '{}'::jsonb AND (template -> 'cpcv_spec' -> 'validation' -> 'pbo' ->> 'block_count')::integer >= 4 AND mod((template -> 'cpcv_spec' -> 'validation' -> 'pbo' ->> 'block_count')::integer, 2) = 0 ",
            "AND jsonb_typeof(template -> 'cpcv_spec' -> 'validation' -> 'gates') = 'object'::text AND (template -> 'cpcv_spec' -> 'validation' -> 'gates') ?& ARRAY['min_cpcv_paths'::text, 'target_rank_ic_min'::text, 'dsr_significance'::text, 'max_pbo'::text, 'max_turnover'::text, 'min_tail_loss_bps'::text] AND ((template -> 'cpcv_spec' -> 'validation' -> 'gates') - ARRAY['min_cpcv_paths'::text, 'target_rank_ic_min'::text, 'dsr_significance'::text, 'max_pbo'::text, 'max_turnover'::text, 'min_tail_loss_bps'::text]) = '{}'::jsonb AND (template -> 'cpcv_spec' -> 'validation' -> 'gates' ->> 'min_cpcv_paths')::integer >= 21 ",
            "AND template -> 'downside_spec' ->> 'spec_hash' ~ '^blake3:[0-9a-f]{64}$'::text AND jsonb_typeof(template -> 'diagnostic_spec' -> 'accepted_artifact_kinds') = 'array'::text AND jsonb_array_length(template -> 'diagnostic_spec' -> 'accepted_artifact_kinds') > 0 AND (template -> 'diagnostic_spec' ->> 'minimum_evidence_count')::numeric > 0 AND jsonb_typeof(template -> 'responsive_triggers') = 'array'::text AND jsonb_array_length(template -> 'responsive_triggers') > 0 AND jsonb_typeof(template -> 'resource_budget') = 'object'::text ",
            "AND (template -> 'resource_budget' ->> 'max_concurrency')::numeric > 0 AND (template -> 'resource_budget' ->> 'max_working_set_bytes')::numeric > 0 AND (template -> 'resource_budget' ->> 'max_resident_model_bytes')::numeric > 0 AND (template -> 'resource_budget' ->> 'deadline_secs')::numeric > 0 AND template -> 'profile_ref' ->> 'content_hash' ~ '^blake3:[0-9a-f]{64}$'::text AND research_profile_artifact_id = (((('rpa:'::text || (template -> 'profile_ref' ->> 'id')) || ':'::text) || (template -> 'profile_ref' ->> 'version')) || ':'::text) || (template -> 'profile_ref' ->> 'content_hash') ",
            "AND ((template ->> 'approved_by_user_id')::uuid IS NOT DISTINCT FROM approved_by_user_id) AND (template ->> 'approved_by_role' IS NOT DISTINCT FROM approved_by_role) AND ((template ->> 'approved_at')::timestamp with time zone IS NOT DISTINCT FROM approved_at) AND (((status = 'draft'::qp_feedback_recipe_template_status) AND approved_by_user_id IS NULL AND approved_by_role IS NULL AND approved_at IS NULL) OR ((status IN ('approved'::qp_feedback_recipe_template_status, 'retired'::qp_feedback_recipe_template_status)) AND approved_by_user_id IS NOT NULL AND approved_by_role ~ '^[a-z][a-z0-9_]{0,63}$'::text AND approved_at IS NOT NULL AND approved_at <= created_at))))",
        ),
    },
    ConstraintSpec {
        name: "fk_quant_feedback_recipe_profile",
        table: "quant_feedback_recipe_template",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (research_profile_artifact_id) REFERENCES public.research_profile_artifact(research_profile_artifact_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_feedback_recipe_model_spec",
        table: "quant_feedback_recipe_template",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (model_spec_id) REFERENCES public.quant_model_spec(model_spec_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_feedback_recipe_approver",
        table: "quant_feedback_recipe_template",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (approved_by_user_id) REFERENCES public.user(id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "uq_policy_activation_audit_generation",
        table: "policy_activation",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (policy_activation_id, audit_event_id, bundle_generation)",
    },
    ConstraintSpec {
        name: "ck_quant_model_route_shadow_binding",
        table: "quant_model_route_shadow_binding",
        kind: ConstraintKind::Check,
        definition: concat!(
            "CHECK ((jsonb_typeof(route) = 'string'::text ",
            "AND route IN ('\"crypto\"'::jsonb, '\"weather\"'::jsonb) ",
            "AND lifecycle_generation >= 0 AND binding_generation > 0 ",
            "AND champion_model_version_id <> candidate_model_version_id ",
            "AND champion_serving_contract_hash ~ '^blake3:[0-9a-f]{64}$'::text ",
            "AND candidate_serving_contract_hash ~ '^blake3:[0-9a-f]{64}$'::text ",
            "AND candidate_recipe_hash ~ '^blake3:[0-9a-f]{64}$'::text ",
            "AND candidate_manifest_hash ~ '^blake3:[0-9a-f]{64}$'::text ",
            "AND reserved_model_bytes > 0 AND committed_policy_generation > 0 ",
            "AND receipt_hash ~ '^blake3:[0-9a-f]{64}$'::text ",
            "AND jsonb_typeof(receipt) = 'object'::text ",
            "AND (receipt ->> 'format_version')::integer = 1 ",
            "AND (receipt ->> 'binding_id')::uuid = binding_id ",
            "AND receipt ->> 'receipt_hash' = receipt_hash ",
            "AND (receipt ->> 'feedback_cycle_id')::uuid = feedback_cycle_id ",
            "AND receipt -> 'route' = route ",
            "AND receipt ->> 'candidate_recipe_hash' = candidate_recipe_hash ",
            "AND (receipt ->> 'champion_model_version_id')::uuid = champion_model_version_id ",
            "AND receipt ->> 'champion_serving_contract_hash' = champion_serving_contract_hash ",
            "AND (receipt ->> 'candidate_model_version_id')::uuid = candidate_model_version_id ",
            "AND receipt ->> 'candidate_serving_contract_hash' = candidate_serving_contract_hash ",
            "AND (receipt ->> 'candidate_manifest_id')::uuid = candidate_manifest_id ",
            "AND receipt ->> 'candidate_manifest_hash' = candidate_manifest_hash ",
            "AND (receipt ->> 'binding_generation')::bigint = binding_generation ",
            "AND (receipt ->> 'reserved_model_bytes')::numeric = reserved_model_bytes ",
            "AND (receipt ->> 'committed_policy_generation')::bigint = committed_policy_generation ",
            "AND (receipt ->> 'policy_activation_id')::uuid = policy_activation_id ",
            "AND (receipt ->> 'audit_event_id')::uuid = audit_event_id ",
            "AND (receipt ->> 'bound_at')::timestamp with time zone = bound_at ",
            "AND bound_at <= created_at AND created_at <= updated_at ",
            "AND (((status = 'active'::qp_shadow_binding_status) AND terminated_at IS NULL ",
            "AND termination_policy_activation_id IS NULL AND termination_request_hash IS NULL ",
            "AND termination_reason_code IS NULL AND termination_note IS NULL ",
            "AND termination_actor_role IS NULL) ",
            "OR ((status IN ('rejected'::qp_shadow_binding_status, 'cancelled'::qp_shadow_binding_status)) AND terminated_at IS NOT NULL ",
            "AND termination_policy_activation_id IS NOT NULL ",
            "AND termination_request_hash ~ '^blake3:[0-9a-f]{64}$'::text ",
            "AND length(termination_reason_code) BETWEEN 1 AND 128 ",
            "AND length(termination_note) BETWEEN 1 AND 2048 ",
            "AND length(termination_actor_role) BETWEEN 1 AND 64 ",
            "AND terminated_at >= bound_at AND terminated_at <= updated_at) ",
            "OR ((status = 'promoted'::qp_shadow_binding_status) ",
            "AND terminated_at IS NOT NULL AND termination_policy_activation_id IS NOT NULL ",
            "AND termination_request_hash IS NULL AND termination_reason_code IS NULL ",
            "AND termination_note IS NULL AND termination_actor_role IS NULL ",
            "AND terminated_at >= bound_at AND terminated_at <= updated_at))))",
        ),
    },
    ConstraintSpec {
        name: "fk_quant_model_route_shadow_cycle",
        table: "quant_model_route_shadow_binding",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (feedback_cycle_id) REFERENCES public.quant_feedback_cycle(feedback_cycle_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_model_route_shadow_champion",
        table: "quant_model_route_shadow_binding",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (champion_model_version_id) REFERENCES public.quant_model_version(model_version_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_model_route_shadow_candidate",
        table: "quant_model_route_shadow_binding",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (candidate_model_version_id) REFERENCES public.quant_model_version(model_version_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_model_route_shadow_manifest",
        table: "quant_model_route_shadow_binding",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (candidate_manifest_id, candidate_manifest_hash) REFERENCES public.quant_model_candidate_manifest(manifest_id, manifest_hash) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_model_route_shadow_activation",
        table: "quant_model_route_shadow_binding",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (policy_activation_id, audit_event_id, committed_policy_generation) REFERENCES public.policy_activation(policy_activation_id, audit_event_id, bundle_generation) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_model_route_shadow_termination_activation",
        table: "quant_model_route_shadow_binding",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (termination_policy_activation_id) REFERENCES public.policy_activation(policy_activation_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "ck_quant_feedback_event_outbox",
        table: "quant_feedback_event_outbox",
        kind: ConstraintKind::Check,
        definition: "CHECK ((revision > 0 AND publish_attempts >= 0 AND created_at <= updated_at AND ((feedback_stage_event_id IS NULL) <> (feedback_trigger_event_id IS NULL)) AND ((claim_owner IS NULL) = (lease_expires_at IS NULL)) AND (published_at IS NULL OR (published_at >= created_at AND claim_owner IS NULL AND lease_expires_at IS NULL AND last_error IS NULL)) AND (last_error IS NULL OR (octet_length(last_error) >= 1 AND octet_length(last_error) <= 2048 AND last_error = btrim(last_error) AND last_error !~ '[[:cntrl:]]'::text))))",
    },
    ConstraintSpec {
        name: "ck_quant_feedback_promotion_permit",
        table: "quant_feedback_promotion_permit",
        kind: ConstraintKind::Check,
        definition: PROMOTION_PERMIT_CHECK,
    },
    ConstraintSpec {
        name: "fk_quant_feedback_permit_profile",
        table: "quant_feedback_promotion_permit",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (research_profile_artifact_id) REFERENCES public.research_profile_artifact(research_profile_artifact_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_feedback_permit_cycle",
        table: "quant_feedback_promotion_permit",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (feedback_cycle_id) REFERENCES public.quant_feedback_cycle(feedback_cycle_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_feedback_permit_snapshot",
        table: "quant_feedback_promotion_permit",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (expected_decision_policy_snapshot_id) REFERENCES public.decision_policy_snapshot(decision_policy_snapshot_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_feedback_permit_champion",
        table: "quant_feedback_promotion_permit",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (champion_model_version_id) REFERENCES public.quant_model_version(model_version_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_feedback_permit_candidate",
        table: "quant_feedback_promotion_permit",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (candidate_model_version_id) REFERENCES public.quant_model_version(model_version_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_feedback_permit_manifest",
        table: "quant_feedback_promotion_permit",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (candidate_manifest_id, candidate_model_version_id, candidate_manifest_hash, promotion_gate_hash) REFERENCES public.quant_model_candidate_manifest(manifest_id, model_version_id, manifest_hash, promotion_gate_hash) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_feedback_permit_issuer",
        table: "quant_feedback_promotion_permit",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (issued_by_user_id) REFERENCES public.user(id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_feedback_permit_revoker",
        table: "quant_feedback_promotion_permit",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (revoked_by_user_id) REFERENCES public.user(id) ON UPDATE NO ACTION ON DELETE NO ACTION",
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
        definition: "FOREIGN KEY (feedback_cycle_id, research_profile_artifact_id, label_cutoff, champion_model_version_id, champion_serving_contract_hash) REFERENCES public.quant_feedback_cycle(feedback_cycle_id, research_profile_artifact_id, label_cutoff, champion_model_version_id, champion_serving_contract_hash) ON UPDATE NO ACTION ON DELETE NO ACTION",
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
        name: "ck_policy_activation_model_governance",
        table: "policy_activation",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((activation_kind = 'model_promotion'::qp_policy_activation_kind) AND (promotion_permit_id IS NOT NULL) AND (promotion_transaction_hash IS NOT NULL) AND (model_governance_audit_id IS NOT NULL) AND (rollback_target_revision_id IS NOT NULL) AND (rollback_target_revision_id <> policy_revision_id) AND (rollback_target_revision_id <> previous_policy_revision_id)) OR ((activation_kind = 'model_bootstrap'::qp_policy_activation_kind) AND (promotion_permit_id IS NULL) AND (promotion_transaction_hash IS NULL) AND (model_governance_audit_id IS NOT NULL)) OR ((activation_kind <> ALL (ARRAY['model_promotion'::qp_policy_activation_kind, 'model_bootstrap'::qp_policy_activation_kind])) AND (promotion_permit_id IS NULL) AND (promotion_transaction_hash IS NULL) AND (model_governance_audit_id IS NULL))))",
    },
    ConstraintSpec {
        name: "ck_policy_activation_audit_model_governance",
        table: "policy_activation_audit",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((promotion_permit_id IS NULL) AND (promotion_transaction_hash IS NULL) AND (model_governance_audit_id IS NULL)) OR ((promotion_permit_id IS NULL) AND (promotion_transaction_hash IS NULL) AND (model_governance_audit_id IS NOT NULL)) OR ((promotion_permit_id IS NOT NULL) AND (promotion_transaction_hash IS NOT NULL) AND (model_governance_audit_id IS NOT NULL))))",
    },
    ConstraintSpec {
        name: "ck_policy_activation_outbox_model_governance",
        table: "policy_activation_event_outbox",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((promotion_permit_id IS NULL) AND (promotion_transaction_hash IS NULL) AND (model_governance_audit_id IS NULL)) OR ((promotion_permit_id IS NULL) AND (promotion_transaction_hash IS NULL) AND (model_governance_audit_id IS NOT NULL)) OR ((promotion_permit_id IS NOT NULL) AND (promotion_transaction_hash IS NOT NULL) AND (model_governance_audit_id IS NOT NULL))))",
    },
    ConstraintSpec {
        name: "uq_policy_activation_model_governance",
        table: "policy_activation",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (policy_activation_id, model_governance_audit_id)",
    },
    ConstraintSpec {
        name: "uq_policy_activation_promotion_binding",
        table: "policy_activation",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (policy_activation_id, promotion_permit_id, promotion_transaction_hash, model_governance_audit_id)",
    },
    ConstraintSpec {
        name: "fk_policy_activation_expected_revision",
        table: "policy_activation",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (expected_active_revision_id) REFERENCES public.policy_revision(policy_revision_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_policy_activation_previous_revision",
        table: "policy_activation",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (previous_policy_revision_id) REFERENCES public.policy_revision(policy_revision_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_policy_activation_rollback_target",
        table: "policy_activation",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (rollback_target_revision_id) REFERENCES public.policy_revision(policy_revision_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "uq_quant_model_governance_audit_promotion",
        table: "quant_model_governance_audit",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (audit_id, promotion_permit_id, promotion_transaction_hash)",
    },
    ConstraintSpec {
        name: "fk_policy_activation_audit_promotion",
        table: "policy_activation_audit",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (policy_activation_id, promotion_permit_id, promotion_transaction_hash, model_governance_audit_id) REFERENCES policy_activation(policy_activation_id, promotion_permit_id, promotion_transaction_hash, model_governance_audit_id)",
    },
    ConstraintSpec {
        name: "fk_policy_activation_outbox_promotion",
        table: "policy_activation_event_outbox",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (policy_activation_id, promotion_permit_id, promotion_transaction_hash, model_governance_audit_id) REFERENCES policy_activation(policy_activation_id, promotion_permit_id, promotion_transaction_hash, model_governance_audit_id)",
    },
    ConstraintSpec {
        name: "fk_policy_activation_promotion_audit",
        table: "policy_activation",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (model_governance_audit_id, promotion_permit_id, promotion_transaction_hash) REFERENCES quant_model_governance_audit(audit_id, promotion_permit_id, promotion_transaction_hash)",
    },
    ConstraintSpec {
        name: "fk_policy_activation_model_governance_audit",
        table: "policy_activation",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (model_governance_audit_id) REFERENCES quant_model_governance_audit(audit_id)",
    },
    ConstraintSpec {
        name: "fk_policy_activation_audit_model_governance",
        table: "policy_activation_audit",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (policy_activation_id, model_governance_audit_id) REFERENCES policy_activation(policy_activation_id, model_governance_audit_id)",
    },
    ConstraintSpec {
        name: "fk_policy_activation_outbox_model_governance",
        table: "policy_activation_event_outbox",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (policy_activation_id, model_governance_audit_id) REFERENCES policy_activation(policy_activation_id, model_governance_audit_id)",
    },
    ConstraintSpec {
        name: "ck_quant_model_governance_audit_detail_action",
        table: "quant_model_governance_audit",
        kind: ConstraintKind::Check,
        definition: "CHECK (((jsonb_typeof(detail) = 'object'::text) AND ((detail ->> 'action'::text) = (action)::text) AND (pg_column_size(detail) <= 65536)))",
    },
    ConstraintSpec {
        name: "ck_quant_model_governance_audit_promotion",
        table: "quant_model_governance_audit",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((action = 'promote_route'::qp_model_governance_action) AND (promotion_permit_id IS NOT NULL) AND (promotion_transaction_hash IS NOT NULL) AND (((detail #>> '{record,promotion_permit_id}'::text[]))::uuid = promotion_permit_id) AND ((detail #>> '{record,transaction_hash}'::text[]) = (promotion_transaction_hash)::text)) OR ((action <> 'promote_route'::qp_model_governance_action) AND (promotion_permit_id IS NULL) AND (promotion_transaction_hash IS NULL))))",
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
        definition: "CHECK (((jsonb_typeof(input_contract) = 'object'::text) AND (jsonb_typeof((input_contract -> 'inputs'::text)) = 'array'::text) AND (jsonb_array_length((input_contract -> 'inputs'::text)) > 0) AND (jsonb_typeof(training_contract) = 'object'::text) AND (training_contract ?& ARRAY['target'::text, 'validation_folds'::text, 'evaluation_trade_policy_artifact_id'::text]) AND ((training_contract - ARRAY['target'::text, 'validation_folds'::text, 'evaluation_trade_policy_artifact_id'::text]) = '{}'::jsonb) AND (jsonb_typeof((training_contract -> 'target'::text)) = 'object'::text) AND ((((training_contract #>> '{target,kind}'::text[]) = 'outcome_payout'::text) AND (((training_contract -> 'target'::text) - 'kind'::text) = '{}'::jsonb) AND (model_family = ANY (ARRAY['weighted_factor'::qp_model_family, 'classical_logistic_regression'::qp_model_family]))) OR (((training_contract #>> '{target,kind}'::text[]) = 'hold_vs_exit_alpha'::text) AND (((training_contract -> 'target'::text) - 'kind'::text) = '{}'::jsonb) AND (model_family = 'hold_vs_exit_weighted'::qp_model_family))) AND ((((training_contract ->> 'validation_folds'::text))::integer >= 2) AND (((training_contract ->> 'validation_folds'::text))::integer <= 20)) AND (((training_contract -> 'evaluation_trade_policy_artifact_id'::text) = 'null'::jsonb) OR ((jsonb_typeof((training_contract -> 'evaluation_trade_policy_artifact_id'::text)) = 'string'::text) AND (((training_contract ->> 'evaluation_trade_policy_artifact_id'::text))::uuid IS NOT NULL))) AND (definition_hash ~ '^blake3:[0-9a-f]{64}$'::text)))",
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
        name: "ck_quant_model_version_serving_contract",
        table: "quant_model_version",
        kind: ConstraintKind::Check,
        definition: "CHECK (((jsonb_typeof(serving_contract) = 'object'::text) AND (serving_contract ?& ARRAY['contract_version'::text, 'contract_hash'::text, 'bindings'::text]) AND ((serving_contract - ARRAY['contract_version'::text, 'contract_hash'::text, 'bindings'::text]) = '{}'::jsonb) AND (jsonb_typeof((serving_contract -> 'contract_version'::text)) = 'number'::text) AND (((serving_contract ->> 'contract_version'::text))::numeric = 3::numeric) AND (jsonb_typeof((serving_contract -> 'contract_hash'::text)) = 'string'::text) AND (jsonb_typeof((serving_contract -> 'bindings'::text)) = 'object'::text) AND (octet_length(serving_contract_hash) = 32) AND ((serving_contract ->> 'contract_hash'::text) = ('blake3:'::text || encode(serving_contract_hash, 'hex'::text)))))",
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
        name: "uq_quant_recommendation_report_tier",
        table: "quant_recommendation",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (recommendation_report_id, economic_tier_id)",
    },
    ConstraintSpec {
        name: "ck_quant_recommendation_status_timeline",
        table: "quant_recommendation",
        kind: ConstraintKind::Check,
        definition: "CHECK ((created_at <= status_changed_at) AND (valid_from <= valid_until))",
    },
    ConstraintSpec {
        name: "ck_quant_portfolio_plan_scenario_binding",
        table: "quant_portfolio_plan",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((scenario_artifact_id IS NULL) AND (scenario_artifact_hash IS NULL) AND (scenario_artifact_json IS NULL)) OR ((scenario_artifact_id IS NOT NULL) AND (scenario_artifact_hash IS NOT NULL) AND (scenario_artifact_json IS NOT NULL) AND (scenario_artifact_hash ~ '^blake3:[0-9a-f]{64}$'::text))))",
    },
    ConstraintSpec {
        name: "uq_quant_report_route_run_route",
        table: "quant_report_route_run",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (report_run_id, route)",
    },
    ConstraintSpec {
        name: "ck_quant_report_route_run_timeline",
        table: "quant_report_route_run",
        kind: ConstraintKind::Check,
        definition: "CHECK ((finished_at <= created_at) AND ((diagnostic_code IS NULL) OR (char_length(btrim(diagnostic_code)) BETWEEN 1 AND 128)))",
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
        name: "uq_quant_execution_attempt_outcome_entry_order",
        table: "quant_execution_attempt_outcome",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (entry_execution_order_id)",
    },
    ConstraintSpec {
        name: "uq_quant_execution_attempt_outcome_entry_reconciliation",
        table: "quant_execution_attempt_outcome",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (entry_reconciliation_id)",
    },
    ConstraintSpec {
        name: "uq_quant_execution_attempt_outcome_position",
        table: "quant_execution_attempt_outcome",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (position_id)",
    },
    ConstraintSpec {
        name: "uq_quant_execution_attempt_outcome_hash",
        table: "quant_execution_attempt_outcome",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (order_intent_id, outcome_hash)",
    },
    ConstraintSpec {
        name: "fk_quant_execution_attempt_outcome_recommendation_identity",
        table: "quant_execution_attempt_outcome",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (recommendation_id, market_id, token_id) REFERENCES public.quant_recommendation(recommendation_id, market_id, token_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_execution_attempt_outcome_intent_lineage",
        table: "quant_execution_attempt_outcome",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (order_intent_id, recommendation_id, execution_account_id, runtime_mode) REFERENCES public.quant_order_intent(order_intent_id, recommendation_id, execution_account_id, runtime_mode) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_execution_attempt_outcome_entry_order_lineage",
        table: "quant_execution_attempt_outcome",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (entry_execution_order_id, order_intent_id, market_id, token_id) REFERENCES public.quant_execution_order(execution_order_id, order_intent_id, market_id, token_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_execution_attempt_outcome_entry_reconciliation_lineage",
        table: "quant_execution_attempt_outcome",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (entry_reconciliation_id, entry_execution_order_id, order_intent_id) REFERENCES public.quant_reconciliation(reconciliation_id, execution_order_id, order_intent_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_execution_attempt_outcome_position_lineage",
        table: "quant_execution_attempt_outcome",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (position_id, order_intent_id, execution_account_id, market_id, token_id) REFERENCES public.quant_position(position_id, order_intent_id, execution_account_id, market_id, token_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "ck_quant_execution_attempt_outcome_contract",
        table: "quant_execution_attempt_outcome",
        kind: ConstraintKind::Check,
        definition: "CHECK (((char_length(market_id) > 0) AND (char_length(token_id) > 0) AND (runtime_mode = ANY (ARRAY['semi_auto'::qp_quant_runtime_mode, 'auto_execution'::qp_quant_runtime_mode])) AND (requested_shares > (0)::numeric) AND (filled_shares >= (0)::numeric) AND (filled_shares <= requested_shares) AND ((entry_avg_price IS NULL) OR ((entry_avg_price >= (0)::numeric) AND (entry_avg_price <= (1)::numeric))) AND ((exit_avg_price IS NULL) OR ((exit_avg_price >= (0)::numeric) AND (exit_avg_price <= (1)::numeric))) AND ((entry_fee_usd IS NULL) OR (entry_fee_usd >= (0)::numeric)) AND ((exit_fee_usd IS NULL) OR (exit_fee_usd >= (0)::numeric)) AND ((settlement_payout_usd IS NULL) OR (settlement_payout_usd >= (0)::numeric)) AND (terminal_at <= source_observed_at) AND (source_observed_at <= available_at) AND (available_at <= created_at) AND (execution_fact_schema_version > 0) AND (source_checkpoint_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND (execution_fact_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND (outcome_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND (((terminal_state = 'unfilled'::qp_execution_attempt_terminal_state) AND (filled_shares = (0)::numeric) AND (no_fill_reason IS NOT NULL) AND (entry_order_state = ANY (ARRAY['cancelled'::qp_execution_order_state, 'failed'::qp_execution_order_state])) AND (position_id IS NULL) AND (entry_avg_price IS NULL) AND (entry_filled_at IS NULL) AND (position_terminal_state IS NULL) AND (exit_reason IS NULL) AND (exit_filled_shares IS NULL) AND (exit_avg_price IS NULL) AND (exit_fee_usd IS NULL) AND (exit_at IS NULL) AND (settlement_payout_usd IS NULL) AND (realized_pnl_usd IS NULL)) OR ((terminal_state = ANY (ARRAY['partially_filled'::qp_execution_attempt_terminal_state, 'fully_filled'::qp_execution_attempt_terminal_state])) AND (no_fill_reason IS NULL) AND (position_id IS NOT NULL) AND (entry_avg_price IS NOT NULL) AND (entry_filled_at IS NOT NULL) AND (entry_filled_at <= terminal_at) AND (position_terminal_state = ANY (ARRAY['closed'::qp_position_ledger_state, 'settled'::qp_position_ledger_state])) AND (realized_pnl_usd IS NOT NULL) AND (((terminal_state = 'partially_filled'::qp_execution_attempt_terminal_state) AND (filled_shares > (0)::numeric) AND (filled_shares < requested_shares) AND (entry_order_state = ANY (ARRAY['cancelled'::qp_execution_order_state, 'failed'::qp_execution_order_state]))) OR ((terminal_state = 'fully_filled'::qp_execution_attempt_terminal_state) AND (filled_shares = requested_shares) AND (entry_order_state = 'filled'::qp_execution_order_state))) AND (((position_terminal_state = 'closed'::qp_position_ledger_state) AND (exit_reason IS NOT NULL) AND (exit_reason <> 'resolution_redeem'::qp_exit_reason) AND (exit_filled_shares = filled_shares) AND (exit_avg_price IS NOT NULL) AND (exit_at = terminal_at) AND (settlement_payout_usd IS NULL)) OR ((position_terminal_state = 'settled'::qp_position_ledger_state) AND (exit_reason = 'resolution_redeem'::qp_exit_reason) AND (settlement_payout_usd IS NOT NULL) AND (((exit_filled_shares IS NULL) AND (exit_avg_price IS NULL) AND (exit_fee_usd IS NULL) AND (exit_at IS NULL)) OR ((exit_filled_shares > (0)::numeric) AND (exit_filled_shares < filled_shares) AND (exit_avg_price IS NOT NULL) AND (exit_at IS NOT NULL) AND (exit_at <= terminal_at)))))))))",
    },
    ConstraintSpec {
        name: "ck_quant_execution_attempt_task_contract",
        table: "quant_execution_attempt_reconciliation_task",
        kind: ConstraintKind::Check,
        definition: "CHECK ((attempt_count >= 0) AND (ready_at <= created_at) AND (created_at <= updated_at) AND ((last_error IS NULL) OR (char_length(btrim(last_error)) BETWEEN 1 AND 4096)) AND (((status = 'pending'::qp_outcome_reconciliation_task_status) AND (claim_owner IS NULL) AND (lease_expires_at IS NULL) AND (next_attempt_at IS NULL) AND (last_error IS NULL) AND (completed_at IS NULL)) OR ((status = 'delivering'::qp_outcome_reconciliation_task_status) AND (claim_owner IS NOT NULL) AND (lease_expires_at > updated_at) AND (next_attempt_at IS NULL) AND (completed_at IS NULL)) OR ((status = 'retrying'::qp_outcome_reconciliation_task_status) AND (claim_owner IS NULL) AND (lease_expires_at IS NULL) AND (next_attempt_at > updated_at) AND (last_error IS NOT NULL) AND (completed_at IS NULL)) OR ((status = 'completed'::qp_outcome_reconciliation_task_status) AND (claim_owner IS NULL) AND (lease_expires_at IS NULL) AND (next_attempt_at IS NULL) AND (last_error IS NULL) AND (completed_at IS NOT NULL) AND (completed_at <= updated_at))))",
    },
    ConstraintSpec {
        name: "ck_quant_execution_rollup_task_contract",
        table: "quant_execution_rollup_reconciliation_task",
        kind: ConstraintKind::Check,
        definition: "CHECK ((attempt_count >= 0) AND (ready_at <= created_at) AND (created_at <= updated_at) AND ((last_error IS NULL) OR (char_length(btrim(last_error)) BETWEEN 1 AND 4096)) AND (((status = 'pending'::qp_outcome_reconciliation_task_status) AND (claim_owner IS NULL) AND (lease_expires_at IS NULL) AND (next_attempt_at IS NULL) AND (last_error IS NULL) AND (completed_at IS NULL)) OR ((status = 'delivering'::qp_outcome_reconciliation_task_status) AND (claim_owner IS NOT NULL) AND (lease_expires_at > updated_at) AND (next_attempt_at IS NULL) AND (completed_at IS NULL)) OR ((status = 'retrying'::qp_outcome_reconciliation_task_status) AND (claim_owner IS NULL) AND (lease_expires_at IS NULL) AND (next_attempt_at > updated_at) AND (last_error IS NOT NULL) AND (completed_at IS NULL)) OR ((status = 'completed'::qp_outcome_reconciliation_task_status) AND (claim_owner IS NULL) AND (lease_expires_at IS NULL) AND (next_attempt_at IS NULL) AND (last_error IS NULL) AND (completed_at IS NOT NULL) AND (completed_at <= updated_at))))",
    },
    ConstraintSpec {
        name: "ck_quant_resolution_outcome_task_contract",
        table: "quant_resolution_outcome_reconciliation_task",
        kind: ConstraintKind::Check,
        definition: "CHECK ((attempt_count >= 0) AND (ready_at <= created_at) AND (created_at <= updated_at) AND ((last_error IS NULL) OR (char_length(btrim(last_error)) BETWEEN 1 AND 4096)) AND (((status = 'pending'::qp_outcome_reconciliation_task_status) AND (claim_owner IS NULL) AND (lease_expires_at IS NULL) AND (next_attempt_at IS NULL) AND (last_error IS NULL) AND (completed_at IS NULL)) OR ((status = 'delivering'::qp_outcome_reconciliation_task_status) AND (claim_owner IS NOT NULL) AND (lease_expires_at > updated_at) AND (next_attempt_at IS NULL) AND (completed_at IS NULL)) OR ((status = 'retrying'::qp_outcome_reconciliation_task_status) AND (claim_owner IS NULL) AND (lease_expires_at IS NULL) AND (next_attempt_at > updated_at) AND (last_error IS NOT NULL) AND (completed_at IS NULL)) OR ((status = 'completed'::qp_outcome_reconciliation_task_status) AND (claim_owner IS NULL) AND (lease_expires_at IS NULL) AND (next_attempt_at IS NULL) AND (last_error IS NULL) AND (completed_at IS NOT NULL) AND (completed_at <= updated_at))))",
    },
    ConstraintSpec {
        name: "ck_quant_execution_rollup_contract",
        table: "quant_recommendation_execution_rollup",
        kind: ConstraintKind::Check,
        definition: "CHECK ((intent_count >= 0) AND (attempt_count >= 0) AND (attempt_count <= intent_count) AND (unfilled_attempt_count >= 0) AND (partially_filled_attempt_count >= 0) AND (fully_filled_attempt_count >= 0) AND ((unfilled_attempt_count + partially_filled_attempt_count + fully_filled_attempt_count) = attempt_count) AND (total_requested_shares >= (0)::numeric) AND (total_filled_shares >= (0)::numeric) AND (total_filled_shares <= total_requested_shares) AND ((total_entry_fee_usd IS NULL) OR (total_entry_fee_usd >= (0)::numeric)) AND ((total_exit_fee_usd IS NULL) OR (total_exit_fee_usd >= (0)::numeric)) AND ((total_settlement_payout_usd IS NULL) OR (total_settlement_payout_usd >= (0)::numeric)) AND (((attempt_count = 0) AND (first_attempt_terminal_at IS NULL) AND (last_attempt_terminal_at IS NULL)) OR ((attempt_count > 0) AND (first_attempt_terminal_at IS NOT NULL) AND (last_attempt_terminal_at IS NOT NULL) AND (first_attempt_terminal_at <= last_attempt_terminal_at) AND (last_attempt_terminal_at <= terminal_at))) AND (terminal_at <= source_observed_at) AND (source_observed_at <= available_at) AND (available_at <= created_at) AND (attempt_set_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND (rollup_hash ~ '^blake3:[0-9a-f]{64}$'::text))",
    },
    ConstraintSpec {
        name: "uq_quant_execution_rollup_attempt_intent",
        table: "quant_recommendation_execution_rollup_attempt",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (order_intent_id)",
    },
    ConstraintSpec {
        name: "fk_quant_execution_rollup_attempt_hash",
        table: "quant_recommendation_execution_rollup_attempt",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (order_intent_id, attempt_outcome_hash) REFERENCES public.quant_execution_attempt_outcome(order_intent_id, outcome_hash) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "ck_quant_execution_rollup_attempt_contract",
        table: "quant_recommendation_execution_rollup_attempt",
        kind: ConstraintKind::Check,
        definition: "CHECK ((sequence >= 0) AND (terminal_at <= created_at) AND (attempt_outcome_hash ~ '^blake3:[0-9a-f]{64}$'::text))",
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
        name: "uq_quant_resolution_inbox_checkpoint",
        table: "quant_resolution_observation_inbox",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (source_checkpoint_hash)",
    },
    ConstraintSpec {
        name: "uq_quant_resolution_inbox_raw_payload",
        table: "quant_resolution_observation_inbox",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (raw_payload_hash)",
    },
    ConstraintSpec {
        name: "uq_quant_resolution_inbox_source_event",
        table: "quant_resolution_observation_inbox",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (transaction_hash, log_index)",
    },
    ConstraintSpec {
        name: "uq_quant_resolution_inbox_lineage",
        table: "quant_resolution_observation_inbox",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (resolution_observation_id, source_checkpoint_hash)",
    },
    ConstraintSpec {
        name: "ck_quant_resolution_inbox_contract",
        table: "quant_resolution_observation_inbox",
        kind: ConstraintKind::Check,
        definition: "CHECK ((source_id = 'polymarket_ctf_resolution') AND (instrument_key = 'POLYMARKET_CTF:137:CONDITION_RESOLUTION') AND (char_length(market_id) > 0) AND (char_length(btrim(question_id)) > 0) AND (denominator ~ '^(0|[1-9][0-9]*)$') AND (denominator <> '0') AND (yes_numerator ~ '^(0|[1-9][0-9]*)$') AND (no_numerator ~ '^(0|[1-9][0-9]*)$') AND (yes_payout_ratio >= 0) AND (yes_payout_ratio <= 1) AND (no_payout_ratio >= 0) AND (no_payout_ratio <= 1) AND ((yes_payout_ratio + no_payout_ratio) = 1) AND (block_number > 0) AND (log_index >= 0) AND (provider_revision = block_hash) AND (resolved_at <= available_at) AND (available_at = created_at) AND (source_checkpoint_hash ~ '^blake3:[0-9a-f]{64}$') AND (raw_payload_hash ~ '^blake3:[0-9a-f]{64}$') AND (raw_uri LIKE 'polygon://resolution/%'))",
    },
    ConstraintSpec {
        name: "uq_quant_resolution_projection_checkpoint",
        table: "quant_resolution_observation_projection",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (source_checkpoint_hash)",
    },
    ConstraintSpec {
        name: "fk_quant_resolution_projection_lineage",
        table: "quant_resolution_observation_projection",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (resolution_observation_id, source_checkpoint_hash) REFERENCES public.quant_resolution_observation_inbox(resolution_observation_id, source_checkpoint_hash) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "ck_quant_resolution_projection_contract",
        table: "quant_resolution_observation_projection",
        kind: ConstraintKind::Check,
        definition: "CHECK ((revision >= 0) AND (attempt_count >= 0) AND ((last_error IS NULL) = (last_error_code IS NULL)) AND ((last_error IS NULL) OR (char_length(btrim(last_error)) BETWEEN 1 AND 4096)) AND (((status = 'pending'::qp_resolution_projection_status) AND (claim_owner IS NULL) AND (lease_expires_at IS NULL) AND (next_attempt_at IS NOT NULL) AND (last_error IS NULL) AND (canonical_fact_hash IS NULL) AND (verified_at IS NULL)) OR ((status = 'delivering'::qp_resolution_projection_status) AND (claim_owner IS NOT NULL) AND (lease_expires_at IS NOT NULL) AND (next_attempt_at IS NULL) AND (last_error IS NULL) AND (canonical_fact_hash IS NULL) AND (verified_at IS NULL)) OR ((status = 'retry_scheduled'::qp_resolution_projection_status) AND (claim_owner IS NULL) AND (lease_expires_at IS NULL) AND (next_attempt_at IS NOT NULL) AND (last_error IS NOT NULL) AND (canonical_fact_hash IS NULL) AND (verified_at IS NULL)) OR ((status = ANY (ARRAY['mapping_blocked'::qp_resolution_projection_status, 'quarantined'::qp_resolution_projection_status, 'excluded'::qp_resolution_projection_status])) AND (claim_owner IS NULL) AND (lease_expires_at IS NULL) AND (next_attempt_at IS NULL) AND (last_error IS NOT NULL) AND (canonical_fact_hash IS NULL) AND (verified_at IS NULL)) OR ((status = 'verified'::qp_resolution_projection_status) AND (claim_owner IS NULL) AND (lease_expires_at IS NULL) AND (next_attempt_at IS NULL) AND (last_error IS NULL) AND (canonical_fact_hash ~ '^blake3:[0-9a-f]{64}$') AND (verified_at IS NOT NULL))))",
    },
    ConstraintSpec {
        name: "ck_quant_resolution_remediation_contract",
        table: "quant_resolution_projection_remediation",
        kind: ConstraintKind::Check,
        definition: "CHECK ((expected_revision >= 0) AND (committed_revision = (expected_revision + 1)) AND (prior_status = ANY (ARRAY['mapping_blocked'::qp_resolution_projection_status, 'quarantined'::qp_resolution_projection_status])) AND (((action = 'requeue'::qp_resolution_remediation_action) AND (resulting_status = 'pending'::qp_resolution_projection_status)) OR ((action = 'exclude'::qp_resolution_remediation_action) AND (resulting_status = 'excluded'::qp_resolution_projection_status))) AND (request_hash ~ '^blake3:[0-9a-f]{64}$') AND (char_length(idempotency_key) BETWEEN 8 AND 128) AND (reason_code ~ '^[a-z0-9_]{1,128}$') AND (char_length(btrim(operator_note)) BETWEEN 1 AND 2048) AND (char_length(btrim(prior_error)) BETWEEN 1 AND 4096) AND (char_length(btrim(actor_username)) BETWEEN 1 AND 255) AND (actor_role ~ '^[a-z0-9_]{1,64}$'))",
    },
    ConstraintSpec {
        name: "quant_recommendation_report_capital_base_usd_check",
        table: "quant_recommendation_report",
        kind: ConstraintKind::Check,
        definition: "CHECK ((capital_base_usd >= (0)::numeric))",
    },
    ConstraintSpec {
        name: "ck_quant_recommendation_report_scenario_binding",
        table: "quant_recommendation_report",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((scenario_artifact_id IS NULL) AND (scenario_artifact_hash IS NULL)) OR ((scenario_artifact_id IS NOT NULL) AND (scenario_artifact_hash IS NOT NULL) AND (scenario_artifact_hash ~ '^blake3:[0-9a-f]{64}$'::text))))",
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
        name: "fk_quant_report_run_output_report",
        table: "quant_report_run",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (output_report_id) REFERENCES quant_recommendation_report(recommendation_report_id) ON UPDATE RESTRICT ON DELETE RESTRICT",
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
        definition: "CHECK (((champion_model_version_id <> candidate_model_version_id) AND (champion_serving_contract_hash <> candidate_serving_contract_hash) AND (champion_serving_contract_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND (candidate_serving_contract_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND (decision_policy_snapshot_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND (policy_bundle_generation > 0)))",
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
        definition: concat!(
            "CHECK ((((result_kind IS NULL) = (result_ref IS NULL)) ",
            "AND ((status = 'succeeded'::qp_research_job_status) OR ((result_kind IS NULL) AND (result_ref IS NULL))) ",
            "AND ((result_kind IS NULL) ",
            "OR ((kind = 'dataset_build'::qp_research_job_kind) AND (result_kind = 'training_dataset'::qp_research_job_result_kind)) ",
            "OR ((kind = 'model_train'::qp_research_job_kind) AND (result_kind = 'model_version'::qp_research_job_result_kind)) ",
            "OR ((kind = 'backtest'::qp_research_job_kind) AND (result_kind = 'backtest_report'::qp_research_job_result_kind)) ",
            "OR ((kind = 'cpcv_backtest'::qp_research_job_kind) AND (result_kind = 'backtest_path_set'::qp_research_job_result_kind)) ",
            "OR ((kind = ANY (ARRAY['bias_table_fit'::qp_research_job_kind, 'model_calibration_fit'::qp_research_job_kind])) AND (result_kind = 'calibration_artifact'::qp_research_job_result_kind)) ",
            "OR ((kind = 'feature_parity'::qp_research_job_kind) AND (result_kind = 'feature_parity_run'::qp_research_job_result_kind)) ",
            "OR ((kind = 'feedback_truth_freeze'::qp_research_job_kind) AND (result_kind = 'feedback_truth_freeze_artifact'::qp_research_job_result_kind)) ",
            "OR ((kind = 'feedback_coverage'::qp_research_job_kind) AND (result_kind = 'feedback_coverage_artifact'::qp_research_job_result_kind)) ",
            "OR ((kind = 'feedback_attribution'::qp_research_job_kind) AND (result_kind = 'feedback_attribution_manifest'::qp_research_job_result_kind)) ",
            "OR ((kind = 'feedback_drift'::qp_research_job_kind) AND (result_kind = 'feedback_drift_artifact'::qp_research_job_result_kind)) ",
            "OR ((kind = 'feedback_recipe_plan'::qp_research_job_kind) AND (result_kind = 'candidate_recipe_plan_artifact'::qp_research_job_result_kind)) ",
            "OR ((kind = ANY (ARRAY['feedback_dataset_seal'::qp_research_job_kind, 'feedback_training'::qp_research_job_kind, 'feedback_calibration'::qp_research_job_kind, 'feedback_cpcv'::qp_research_job_kind])) AND (result_kind = 'feedback_learning_stage_artifact'::qp_research_job_result_kind)) ",
            "OR ((kind = 'feedback_validation'::qp_research_job_kind) AND (result_kind = 'feedback_validation_artifact'::qp_research_job_result_kind)) ",
            "OR ((kind = 'feedback_comparison'::qp_research_job_kind) AND (result_kind = 'feedback_comparison_artifact'::qp_research_job_result_kind)) ",
            "OR ((kind = 'feedback_shadow_bind'::qp_research_job_kind) AND (result_kind = 'shadow_binding_artifact'::qp_research_job_result_kind)) ",
            "OR ((kind = 'feedback_shadow'::qp_research_job_kind) AND (result_kind = 'feedback_shadow_artifact'::qp_research_job_result_kind)) ",
            "OR ((kind = 'feedback_decision'::qp_research_job_kind) AND (result_kind = 'feedback_decision_artifact'::qp_research_job_result_kind)) ",
            "OR ((kind = 'trade_policy_fit'::qp_research_job_kind) AND (result_kind = 'trade_policy_artifact'::qp_research_job_result_kind)) ",
            "OR ((kind = 'trade_policy_validation'::qp_research_job_kind) AND (result_kind = 'trade_policy_validation_run'::qp_research_job_result_kind)))))"
        ),
    },
    ConstraintSpec {
        name: "ck_quant_research_job_artifact_reference",
        table: "quant_research_job",
        kind: ConstraintKind::Check,
        definition: concat!(
            "CHECK ((((result_artifact_uri IS NULL) = (result_artifact_hash IS NULL)) ",
            "AND ((result_artifact_uri IS NULL) OR ((octet_length(result_artifact_uri) >= 1) AND (octet_length(result_artifact_uri) <= 4096) AND (result_artifact_uri ~ '^[a-z][a-z0-9+.-]*://.+$'::text))) ",
            "AND ((result_artifact_hash IS NULL) OR (result_artifact_hash ~ '^blake3:[0-9a-f]{64}$'::text)) ",
            "AND (((result_kind = ANY (ARRAY[",
            "'feedback_truth_freeze_artifact'::qp_research_job_result_kind, ",
            "'feedback_coverage_artifact'::qp_research_job_result_kind, ",
            "'feedback_attribution_manifest'::qp_research_job_result_kind, ",
            "'feedback_drift_artifact'::qp_research_job_result_kind, ",
            "'candidate_recipe_plan_artifact'::qp_research_job_result_kind, ",
            "'feedback_learning_stage_artifact'::qp_research_job_result_kind, ",
            "'feedback_validation_artifact'::qp_research_job_result_kind, ",
            "'feedback_comparison_artifact'::qp_research_job_result_kind, ",
            "'shadow_binding_artifact'::qp_research_job_result_kind, ",
            "'feedback_shadow_artifact'::qp_research_job_result_kind, ",
            "'feedback_decision_artifact'::qp_research_job_result_kind",
            "])) AND (result_artifact_uri IS NOT NULL)) ",
            "OR ((result_kind <> ALL (ARRAY[",
            "'feedback_truth_freeze_artifact'::qp_research_job_result_kind, ",
            "'feedback_coverage_artifact'::qp_research_job_result_kind, ",
            "'feedback_attribution_manifest'::qp_research_job_result_kind, ",
            "'feedback_drift_artifact'::qp_research_job_result_kind, ",
            "'candidate_recipe_plan_artifact'::qp_research_job_result_kind, ",
            "'feedback_learning_stage_artifact'::qp_research_job_result_kind, ",
            "'feedback_validation_artifact'::qp_research_job_result_kind, ",
            "'feedback_comparison_artifact'::qp_research_job_result_kind, ",
            "'shadow_binding_artifact'::qp_research_job_result_kind, ",
            "'feedback_shadow_artifact'::qp_research_job_result_kind, ",
            "'feedback_decision_artifact'::qp_research_job_result_kind",
            "])) AND (result_artifact_uri IS NULL)) ",
            "OR ((result_kind IS NULL) AND (result_artifact_uri IS NULL)))))"
        ),
    },
    ConstraintSpec {
        name: "ck_quant_research_job_acting_role",
        table: "quant_research_job",
        kind: ConstraintKind::Check,
        definition: "CHECK (((acting_role)::text ~ '^[a-z][a-z0-9_]{0,63}$'::text))",
    },
    ConstraintSpec {
        name: "ck_quant_research_job_lifecycle",
        table: "quant_research_job",
        kind: ConstraintKind::Check,
        definition: concat!(
            "CHECK (((recovery_attempt >= 0) AND (max_recovery_attempts >= 0) ",
            "AND (recovery_attempt <= max_recovery_attempts) ",
            "AND ((lease_owner IS NULL) = (lease_expires_at IS NULL)) ",
            "AND ((finished_at IS NULL) OR (started_at IS NULL) OR (finished_at >= started_at)) ",
            "AND ((status = 'queued'::qp_research_job_status ",
            "AND next_attempt_at IS NULL AND lease_owner IS NULL AND progress_json IS NULL ",
            "AND error_json IS NULL AND started_at IS NULL AND heartbeat_at IS NULL AND finished_at IS NULL) ",
            "OR (status = 'awaiting_evidence'::qp_research_job_status ",
            "AND kind = 'feature_parity'::qp_research_job_kind ",
            "AND next_attempt_at IS NOT NULL AND next_attempt_at >= updated_at ",
            "AND lease_owner IS NULL AND progress_json IS NOT NULL AND error_json IS NULL ",
            "AND started_at IS NULL AND heartbeat_at IS NULL AND finished_at IS NULL) ",
            "OR (status = 'retry_scheduled'::qp_research_job_status ",
            "AND next_attempt_at IS NOT NULL AND next_attempt_at >= updated_at ",
            "AND lease_owner IS NULL AND progress_json IS NULL AND error_json IS NOT NULL ",
            "AND started_at IS NULL AND heartbeat_at IS NULL AND finished_at IS NULL) ",
            "OR (status = 'running'::qp_research_job_status ",
            "AND next_attempt_at IS NULL AND lease_owner IS NOT NULL AND error_json IS NULL ",
            "AND started_at IS NOT NULL AND heartbeat_at IS NOT NULL AND finished_at IS NULL) ",
            "OR (status = 'succeeded'::qp_research_job_status ",
            "AND next_attempt_at IS NULL AND lease_owner IS NULL AND error_json IS NULL ",
            "AND started_at IS NOT NULL AND finished_at IS NOT NULL) ",
            "OR (status = ANY (ARRAY['failed'::qp_research_job_status, 'cancelled'::qp_research_job_status]) ",
            "AND next_attempt_at IS NULL AND lease_owner IS NULL AND error_json IS NOT NULL ",
            "AND finished_at IS NOT NULL))))"
        ),
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
        definition: "CHECK (((fit_seal_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND ((manifest IS NULL) OR ((jsonb_typeof(manifest) = 'object'::text) AND (manifest ?& ARRAY['format_version'::text, 'profile_ref'::text, 'evaluation_track'::text, 'research_program_hash'::text, 'window_start'::text, 'window_end'::text, 'pit_cutoff'::text, 'reader_contract_version'::text, 'schema_contract_version'::text, 'decision_policy_snapshot_id'::text, 'runtime_config_hash'::text, 'fit_seal_id'::text, 'fit_seal_hash'::text, 'dataset_format_version'::text, 'capability_registry_hashes'::text, 'objects'::text]) AND (((manifest ->> 'format_version'::text))::integer = 5) AND ((manifest -> 'profile_ref'::text) = profile_ref) AND ((manifest ->> 'evaluation_track'::text) = (evaluation_track)::text) AND ((manifest ->> 'research_program_hash'::text) = research_program_hash) AND (((manifest ->> 'window_start'::text))::timestamp with time zone = window_start) AND (((manifest ->> 'window_end'::text))::timestamp with time zone = window_end) AND (((manifest ->> 'pit_cutoff'::text))::timestamp with time zone = pit_cutoff) AND ((manifest ->> 'reader_contract_version'::text) = (reader_contract_version)::text) AND ((manifest ->> 'schema_contract_version'::text) = (schema_contract_version)::text) AND (((manifest ->> 'decision_policy_snapshot_id'::text))::uuid = decision_policy_snapshot_id) AND ((manifest ->> 'runtime_config_hash'::text) = runtime_config_hash) AND (((manifest ->> 'fit_seal_id'::text))::uuid = fit_seal_id) AND ((manifest ->> 'fit_seal_hash'::text) = fit_seal_hash) AND (((manifest ->> 'dataset_format_version'::text))::integer = 4) AND (jsonb_typeof((manifest -> 'capability_registry_hashes'::text)) = 'array'::text) AND (jsonb_typeof((manifest -> 'objects'::text)) = 'array'::text) AND (jsonb_array_length((manifest -> 'objects'::text)) > 0)))))",
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
        definition: "CHECK (((char_length(venue_trade_id) >= 1) AND (char_length(venue_trade_id) <= 256) AND ((transaction_hash IS NULL) OR (transaction_hash ~ '^0x[0-9a-f]{64}$'::text)) AND ((trade_status IS NULL) OR (trade_status <> 'mined'::qp_venue_trade_status) OR (transaction_hash IS NOT NULL))))",
    },
    ConstraintSpec {
        name: "ck_quant_execution_fill_economics",
        table: "quant_execution_fill",
        kind: ConstraintKind::Check,
        definition: "CHECK ((char_length(venue_trade_id) >= 1) AND (char_length(venue_trade_id) <= 256) AND (venue_bucket_index >= 0) AND (char_length(venue_order_id) >= 1) AND (char_length(venue_order_id) <= 256) AND (shares > 0) AND (price > 0) AND (price <= 1) AND (principal_usd = round(shares * price, 28)) AND (evidence_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND (available_at >= matched_at))",
    },
    ConstraintSpec {
        name: "ck_quant_execution_fee_measurement_evidence",
        table: "quant_execution_fee_measurement",
        kind: ConstraintKind::Check,
        definition: "CHECK ((fee_usd >= 0) AND (char_length(source_identity) BETWEEN 1 AND 512) AND ((fee_rate_bps IS NULL) OR (fee_rate_bps >= 0)) AND ((exchange_address IS NULL) OR (exchange_address ~ '^0x[0-9a-f]{40}$'::text)) AND ((transaction_hash IS NULL) OR (transaction_hash ~ '^0x[0-9a-f]{64}$'::text)) AND (evidence_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND (available_at >= observed_at) AND (((stage = 'on_chain_settled'::qp_fee_measurement_stage) AND (chain_id > 0) AND (protocol_version = 2) AND (exchange_address IS NOT NULL) AND (transaction_hash IS NOT NULL) AND (log_index >= 0)) OR ((stage <> 'on_chain_settled'::qp_fee_measurement_stage) AND (chain_id IS NULL) AND (protocol_version IS NULL) AND (exchange_address IS NULL) AND (log_index IS NULL))))",
    },
    ConstraintSpec {
        name: "ck_quant_venue_incentive_event_evidence",
        table: "quant_venue_incentive_event",
        kind: ConstraintKind::Check,
        definition: "CHECK ((amount_usd >= 0) AND (char_length(source_partition) BETWEEN 1 AND 512) AND (char_length(source_identity) BETWEEN 1 AND 600) AND ((source_terms_hash IS NULL) OR (source_terms_hash ~ '^blake3:[0-9a-f]{64}$'::text)) AND ((transaction_hash IS NULL) OR (transaction_hash ~ '^0x[0-9a-f]{64}$'::text)) AND (evidence_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND (available_at >= observed_at) AND (((stage = 'estimated_accrual'::qp_venue_incentive_stage) AND (kind = 'maker_rebate'::qp_venue_incentive_kind) AND (execution_fill_id IS NOT NULL) AND (market_id IS NOT NULL) AND (source_terms_hash IS NOT NULL)) OR ((stage = 'venue_reported_accrual'::qp_venue_incentive_stage) AND (kind = 'maker_rebate'::qp_venue_incentive_kind) AND (execution_fill_id IS NULL) AND (market_id IS NOT NULL) AND (source_terms_hash IS NULL)) OR ((stage = 'wallet_credited'::qp_venue_incentive_stage) AND (execution_fill_id IS NULL) AND (source_terms_hash IS NULL))))",
    },
    ConstraintSpec {
        name: "ck_quant_venue_incentive_scan_result",
        table: "quant_venue_incentive_reconciliation_scan",
        kind: ConstraintKind::Check,
        definition: "CHECK ((completed_at >= started_at) AND (response_count >= 0) AND ((response_digest IS NULL) OR (response_digest ~ '^blake3:[0-9a-f]{64}$'::text)) AND ((error_code IS NULL) OR (char_length(error_code) BETWEEN 1 AND 128)) AND (((status = 'succeeded'::qp_venue_incentive_reconciliation_scan_status) AND (response_digest IS NOT NULL) AND (error_code IS NULL)) OR ((status = 'failed'::qp_venue_incentive_reconciliation_scan_status) AND (response_digest IS NULL) AND (response_count = 0) AND (error_code IS NOT NULL))))",
    },
    ConstraintSpec {
        name: "ck_quant_execution_transaction_ref_hash",
        table: "quant_execution_transaction_ref",
        kind: ConstraintKind::Check,
        definition: "CHECK ((transaction_hash ~ '^0x[0-9a-f]{64}$'::text))",
    },
    ConstraintSpec {
        name: "ck_quant_exchange_history_chunk_range",
        table: "quant_exchange_history_chunk",
        kind: ConstraintKind::Check,
        definition: "CHECK (((from_block > 0) AND (to_block >= from_block) AND (attempt_count >= 0) AND ((hypersync_count IS NULL) OR (hypersync_count >= 0)) AND ((attestor_count IS NULL) OR (attestor_count >= 0)) AND ((archive_height IS NULL) OR (archive_height >= to_block)) AND ((continuity_block IS NULL) OR (continuity_block = (from_block - 1)))))",
    },
    ConstraintSpec {
        name: "ck_quant_exchange_history_chunk_acceptance",
        table: "quant_exchange_history_chunk",
        kind: ConstraintKind::Check,
        definition: "CHECK (((status <> 'accepted'::qp_exchange_history_chunk_status) OR ((hypersync_count IS NOT NULL) AND (attestor_count IS NOT NULL) AND (hypersync_count = attestor_count) AND (hypersync_digest IS NOT NULL) AND (attestor_digest IS NOT NULL) AND (hypersync_digest = attestor_digest) AND (first_block_hash IS NOT NULL) AND (last_block_hash IS NOT NULL) AND (archive_height IS NOT NULL) AND (continuity_basis IS NOT NULL) AND (continuity_block IS NOT NULL) AND (continuity_hash IS NOT NULL) AND (accepted_at IS NOT NULL))))",
    },
    ConstraintSpec {
        name: "ck_quant_exchange_history_chunk_hashes",
        table: "quant_exchange_history_chunk",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((hypersync_digest IS NULL) OR (hypersync_digest ~ '^blake3:[0-9a-f]{64}$'::text)) AND ((attestor_digest IS NULL) OR (attestor_digest ~ '^blake3:[0-9a-f]{64}$'::text)) AND ((first_block_hash IS NULL) OR (first_block_hash ~ '^0x[0-9a-f]{64}$'::text)) AND ((last_block_hash IS NULL) OR (last_block_hash ~ '^0x[0-9a-f]{64}$'::text)) AND ((continuity_hash IS NULL) OR (continuity_hash ~ '^0x[0-9a-f]{64}$'::text))))",
    },
    ConstraintSpec {
        name: "ck_quant_exchange_history_plan_ranges",
        table: "quant_exchange_history_plan",
        kind: ConstraintKind::Check,
        definition: "CHECK (((chain_id = 137) AND (finalized_anchor_block > activation_through_block) AND (finalized_anchor_timestamp > 0) AND (retention_from_block > 0) AND (retention_from_block <= weather_required_from_block) AND (weather_required_from_block <= crypto_required_from_block) AND (crypto_required_from_block <= activation_from_block) AND (activation_from_block <= activation_through_block) AND (retention_through_block = (activation_from_block - 1))))",
    },
    ConstraintSpec {
        name: "ck_quant_exchange_history_plan_hashes",
        table: "quant_exchange_history_plan",
        kind: ConstraintKind::Check,
        definition: "CHECK (((policy_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND (bootstrap_profile_set_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND (finalized_anchor_hash ~ '^0x[0-9a-f]{64}$'::text)))",
    },
    ConstraintSpec {
        name: "ck_quant_exchange_history_quarantine_evidence",
        table: "quant_exchange_history_quarantine",
        kind: ConstraintKind::Check,
        definition: "CHECK ((evidence_hash ~ '^blake3:[0-9a-f]{64}$'::text AND jsonb_typeof(evidence) = 'object'::text AND evidence ? 'kind'::text AND ((kind = 'provider_mismatch'::qp_exchange_history_quarantine_kind AND evidence ->> 'kind'::text = 'provider_mismatch'::text) OR (kind = ANY (ARRAY['continuity_mismatch'::qp_exchange_history_quarantine_kind, 'parent_hash_mismatch'::qp_exchange_history_quarantine_kind]) AND evidence ->> 'kind'::text = 'continuity_mismatch'::text) OR (kind = ANY (ARRAY['decode_failure'::qp_exchange_history_quarantine_kind, 'unknown_token'::qp_exchange_history_quarantine_kind, 'missing_correlation'::qp_exchange_history_quarantine_kind, 'contract_mismatch'::qp_exchange_history_quarantine_kind]) AND evidence ->> 'kind'::text = 'projection_failure'::text) OR (kind = 'archive_probe_failure'::qp_exchange_history_quarantine_kind AND evidence ->> 'kind'::text = 'archive_probe_failure'::text))))",
    },
    ConstraintSpec {
        name: "ck_quant_history_fit_seal",
        table: "quant_history_fit_seal",
        kind: ConstraintKind::Check,
        definition: "CHECK ((window_from_block > 0 AND window_to_block >= window_from_block AND seal_hash ~ '^blake3:[0-9a-f]{64}$'::text AND policy_hash ~ '^blake3:[0-9a-f]{64}$'::text AND profile_hash ~ '^blake3:[0-9a-f]{64}$'::text AND cohort_hash ~ '^blake3:[0-9a-f]{64}$'::text))",
    },
    ConstraintSpec {
        name: "ck_quant_history_fit_seal_chunk",
        table: "quant_history_fit_seal_chunk",
        kind: ConstraintKind::Check,
        definition: "CHECK ((state_revision > 0 AND from_block > 0 AND to_block >= from_block))",
    },
    ConstraintSpec {
        name: "ck_quant_history_serving_head_seal",
        table: "quant_history_serving_head_seal",
        kind: ConstraintKind::Check,
        definition: "CHECK ((window_from_block > 0 AND accepted_through_block >= window_from_block AND effective_through_at <= created_at AND seal_hash ~ '^blake3:[0-9a-f]{64}$'::text AND policy_hash ~ '^blake3:[0-9a-f]{64}$'::text AND (previous_seal_id IS NULL OR previous_seal_id <> serving_head_seal_id)))",
    },
    ConstraintSpec {
        name: "ck_quant_history_serving_head_seal_chunk",
        table: "quant_history_serving_head_seal_chunk",
        kind: ConstraintKind::Check,
        definition: "CHECK ((state_revision > 0 AND from_block > 0 AND to_block >= from_block))",
    },
    ConstraintSpec {
        name: "ck_quant_exchange_history_quarantine_resolution",
        table: "quant_exchange_history_quarantine_resolution",
        kind: ConstraintKind::Check,
        definition: "CHECK ((evidence_hash ~ '^blake3:[0-9a-f]{64}$'::text AND char_length(btrim(actor)) BETWEEN 1 AND 128 AND actor !~ '[[:cntrl:]]'::text AND char_length(btrim(detail)) BETWEEN 1 AND 2048 AND octet_length(detail) = char_length(detail)))",
    },
    ConstraintSpec {
        name: "ck_quant_fresh_boot_run_identity",
        table: "quant_fresh_boot_run",
        kind: ConstraintKind::Check,
        definition: "CHECK ((profile_hash ~ '^blake3:[0-9a-f]{64}$'::text AND jsonb_typeof(route) = 'string'::text AND route #>> '{}' = ANY (ARRAY['pooled'::text, 'crypto'::text, 'weather'::text]) AND char_length(btrim(idempotency_key)) BETWEEN 8 AND 128 AND idempotency_key !~ '[[:space:][:cntrl:]]'::text AND revision >= 0 AND retry_count >= 0 AND started_at <= created_at AND created_at <= stage_entered_at AND stage_entered_at <= updated_at AND ((source_coverage_manifest IS NULL) = (source_coverage_hash IS NULL)) AND (source_coverage_manifest IS NULL OR jsonb_typeof(source_coverage_manifest) = 'object'::text) AND (source_coverage_hash IS NULL OR source_coverage_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND ((source_slice_id IS NULL) = (source_slice_hash IS NULL)) AND (source_slice_hash IS NULL OR source_slice_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND ((scenario_artifact_id IS NULL) = (scenario_artifact_hash IS NULL)) AND (scenario_artifact_hash IS NULL OR scenario_artifact_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND ((bootstrap_preflight IS NULL) = (bootstrap_preflight_hash IS NULL)) AND (bootstrap_preflight IS NULL OR jsonb_typeof(bootstrap_preflight) = 'object'::text) AND (bootstrap_preflight_hash IS NULL OR bootstrap_preflight_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND ((lease_owner IS NULL) = (lease_expires_at IS NULL)) AND (retry_detail IS NULL OR (char_length(btrim(retry_detail)) >= 1 AND char_length(btrim(retry_detail)) <= 2048 AND octet_length(retry_detail) = char_length(retry_detail))) AND (blocked_detail IS NULL OR (char_length(btrim(blocked_detail)) >= 1 AND char_length(btrim(blocked_detail)) <= 2048 AND octet_length(blocked_detail) = char_length(blocked_detail)))))",
    },
    ConstraintSpec {
        name: "ck_quant_fresh_boot_run_status",
        table: "quant_fresh_boot_run",
        kind: ConstraintKind::Check,
        definition: "CHECK ((((status = ANY (ARRAY['waiting_evidence'::qp_fresh_boot_status, 'retry_scheduled'::qp_fresh_boot_status])) AND retry_reason IS NOT NULL AND retry_detail IS NOT NULL AND next_attempt_at IS NOT NULL AND blocked_reason IS NULL AND blocked_detail IS NULL AND lease_owner IS NULL AND completed_at IS NULL) OR ((status = 'running'::qp_fresh_boot_status) AND retry_reason IS NULL AND retry_detail IS NULL AND next_attempt_at IS NULL AND blocked_reason IS NULL AND blocked_detail IS NULL AND completed_at IS NULL) OR ((status = 'blocked_terminal'::qp_fresh_boot_status) AND blocked_reason IS NOT NULL AND blocked_detail IS NOT NULL AND retry_reason IS NULL AND retry_detail IS NULL AND next_attempt_at IS NULL AND lease_owner IS NULL AND completed_at IS NOT NULL) OR ((status = 'superseded'::qp_fresh_boot_status) AND blocked_reason IS NOT NULL AND blocked_detail IS NOT NULL AND retry_reason IS NULL AND retry_detail IS NULL AND next_attempt_at IS NULL AND lease_owner IS NULL AND completed_at IS NOT NULL) OR ((status = 'succeeded'::qp_fresh_boot_status) AND stage = 'first_report_published'::qp_fresh_boot_stage AND retry_reason IS NULL AND retry_detail IS NULL AND next_attempt_at IS NULL AND blocked_reason IS NULL AND blocked_detail IS NULL AND lease_owner IS NULL AND completed_at IS NOT NULL AND first_report_id IS NOT NULL)))",
    },
    ConstraintSpec {
        name: "ck_quant_fresh_boot_run_stage",
        table: "quant_fresh_boot_run",
        kind: ConstraintKind::Check,
        definition: "CHECK (((stage = 'awaiting_source_coverage'::qp_fresh_boot_stage OR (source_coverage_manifest IS NOT NULL AND source_coverage_hash IS NOT NULL AND model_spec_id IS NOT NULL)) AND (stage < 'dataset_ready'::qp_fresh_boot_stage OR (training_dataset_id IS NOT NULL AND source_slice_id IS NOT NULL AND source_slice_hash IS NOT NULL)) AND (stage < 'training_ready'::qp_fresh_boot_stage OR source_model_version_id IS NOT NULL) AND (stage < 'calibration_dataset_ready'::qp_fresh_boot_stage OR calibration_dataset_id IS NOT NULL) AND (stage < 'calibration_ready'::qp_fresh_boot_stage OR (model_version_id IS NOT NULL AND calibration_id IS NOT NULL)) AND (stage < 'cpcv_ready'::qp_fresh_boot_stage OR path_set_id IS NOT NULL) AND (stage < 'parity_ready'::qp_fresh_boot_stage OR parity_run_id IS NOT NULL) AND (stage < 'scenario_ready'::qp_fresh_boot_stage OR (scenario_artifact_id IS NOT NULL AND scenario_artifact_hash IS NOT NULL)) AND (stage < 'bootstrap_preflight'::qp_fresh_boot_stage OR (bootstrap_preflight IS NOT NULL AND bootstrap_preflight_hash IS NOT NULL)) AND (stage < 'bootstrap_committed'::qp_fresh_boot_stage OR bootstrap_policy_activation_id IS NOT NULL) AND (stage < 'first_report_queued'::qp_fresh_boot_stage OR first_report_run_id IS NOT NULL) AND (stage < 'first_report_published'::qp_fresh_boot_stage OR first_report_id IS NOT NULL)))",
    },
    ConstraintSpec {
        name: "ck_quant_fresh_boot_run_active_job",
        table: "quant_fresh_boot_run",
        kind: ConstraintKind::Check,
        definition: "CHECK (((stage = ANY (ARRAY['dataset_queued'::qp_fresh_boot_stage, 'dataset_running'::qp_fresh_boot_stage, 'training_queued'::qp_fresh_boot_stage, 'training_running'::qp_fresh_boot_stage, 'calibration_dataset_queued'::qp_fresh_boot_stage, 'calibration_dataset_running'::qp_fresh_boot_stage, 'calibration_queued'::qp_fresh_boot_stage, 'calibration_running'::qp_fresh_boot_stage, 'cpcv_queued'::qp_fresh_boot_stage, 'cpcv_running'::qp_fresh_boot_stage])) = (active_job_id IS NOT NULL)))",
    },
    ConstraintSpec {
        name: "fk_quant_fresh_boot_run_profile",
        table: "quant_fresh_boot_run",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (research_profile_artifact_id) REFERENCES public.research_profile_artifact(research_profile_artifact_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_fresh_boot_run_policy",
        table: "quant_fresh_boot_run",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (decision_policy_snapshot_id) REFERENCES public.decision_policy_snapshot(decision_policy_snapshot_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_fresh_boot_run_spec",
        table: "quant_fresh_boot_run",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (model_spec_id) REFERENCES public.quant_model_spec(model_spec_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_fresh_boot_run_training_dataset",
        table: "quant_fresh_boot_run",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (training_dataset_id) REFERENCES public.quant_training_dataset(training_dataset_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_fresh_boot_run_calibration_dataset",
        table: "quant_fresh_boot_run",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (calibration_dataset_id) REFERENCES public.quant_training_dataset(training_dataset_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_fresh_boot_run_source_model",
        table: "quant_fresh_boot_run",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (source_model_version_id) REFERENCES public.quant_model_version(model_version_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_fresh_boot_run_model",
        table: "quant_fresh_boot_run",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (model_version_id) REFERENCES public.quant_model_version(model_version_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_fresh_boot_run_path_set",
        table: "quant_fresh_boot_run",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (path_set_id) REFERENCES public.quant_backtest_path_set(path_set_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_fresh_boot_run_calibration",
        table: "quant_fresh_boot_run",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (calibration_id) REFERENCES public.quant_calibration_artifact(artifact_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_fresh_boot_run_parity",
        table: "quant_fresh_boot_run",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (parity_run_id) REFERENCES public.quant_feature_parity_run(run_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_fresh_boot_run_active_job",
        table: "quant_fresh_boot_run",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (active_job_id) REFERENCES public.quant_research_job(job_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_fresh_boot_run_last_job",
        table: "quant_fresh_boot_run",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (last_job_id) REFERENCES public.quant_research_job(job_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_fresh_boot_run_supersedes",
        table: "quant_fresh_boot_run",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (supersedes_run_id) REFERENCES public.quant_fresh_boot_run(run_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_fresh_boot_run_activation",
        table: "quant_fresh_boot_run",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (bootstrap_policy_activation_id) REFERENCES public.policy_activation(policy_activation_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_fresh_boot_run_report_run",
        table: "quant_fresh_boot_run",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (first_report_run_id) REFERENCES public.quant_report_run(report_run_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_fresh_boot_run_report",
        table: "quant_fresh_boot_run",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (first_report_id) REFERENCES public.quant_recommendation_report(recommendation_report_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_fresh_boot_run_source_slice",
        table: "quant_fresh_boot_run",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (source_slice_id) REFERENCES public.quant_source_slice(source_slice_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "ck_quant_fresh_boot_run_event",
        table: "quant_fresh_boot_run_event",
        kind: ConstraintKind::Check,
        definition: "CHECK ((event_sequence >= 0 AND attempt >= 0 AND event_hash ~ '^blake3:[0-9a-f]{64}$'::text AND (evidence_hash IS NULL OR evidence_hash ~ '^blake3:[0-9a-f]{64}$'::text) AND char_length(btrim(actor)) BETWEEN 1 AND 128 AND actor !~ '[[:cntrl:]]'::text AND (detail IS NULL OR (char_length(btrim(detail)) >= 1 AND char_length(btrim(detail)) <= 2048 AND octet_length(detail) = char_length(detail)))))",
    },
    ConstraintSpec {
        name: "uq_quant_fresh_boot_run_event_sequence",
        table: "quant_fresh_boot_run_event",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (run_id, event_sequence)",
    },
    ConstraintSpec {
        name: "uq_quant_fresh_boot_run_event_hash",
        table: "quant_fresh_boot_run_event",
        kind: ConstraintKind::Unique,
        definition: "UNIQUE (event_hash)",
    },
    ConstraintSpec {
        name: "fk_quant_fresh_boot_run_event_run",
        table: "quant_fresh_boot_run_event",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (run_id) REFERENCES public.quant_fresh_boot_run(run_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
    },
    ConstraintSpec {
        name: "fk_quant_fresh_boot_run_event_job",
        table: "quant_fresh_boot_run_event",
        kind: ConstraintKind::ForeignKey,
        definition: "FOREIGN KEY (research_job_id) REFERENCES public.quant_research_job(job_id) ON UPDATE NO ACTION ON DELETE NO ACTION",
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
        definition: "CHECK (((window_start < window_end) AND (window_end <= pit_cutoff) AND (knowledge_lag_secs >= 0) AND (feature_schema_version >= 1) AND ((sample_interval_secs > 0) OR ((sample_interval_secs = 0) AND (cohort_manifest IS NOT NULL))) AND (jsonb_typeof(horizons_secs) = 'array'::text) AND (jsonb_array_length(horizons_secs) > 0) AND (jsonb_typeof(source_lineage) = 'object'::text) AND (source_lineage ?& ARRAY['format_version'::text, 'source_slice_id'::text, 'research_profile_artifact_id'::text, 'source_window_start'::text, 'source_window_end'::text, 'pit_cutoff'::text, 'decision_policy_snapshot_id'::text, 'fit_seal_id'::text, 'fit_seal_hash'::text, 'capability_registry_hashes'::text]) AND (((source_lineage ->> 'format_version'::text))::integer = 2) AND (((source_lineage ->> 'source_slice_id'::text))::uuid = source_slice_id) AND ((source_lineage ->> 'research_profile_artifact_id'::text) = research_profile_artifact_id) AND (((source_lineage ->> 'source_window_start'::text))::timestamp with time zone <= window_start) AND (((source_lineage ->> 'source_window_end'::text))::timestamp with time zone >= window_end) AND (((source_lineage ->> 'pit_cutoff'::text))::timestamp with time zone = pit_cutoff) AND (((source_lineage ->> 'decision_policy_snapshot_id'::text))::uuid = decision_policy_snapshot_id) AND (((source_lineage ->> 'fit_seal_id'::text))::uuid IS NOT NULL) AND ((source_lineage ->> 'fit_seal_hash'::text) ~ '^blake3:[0-9a-f]{64}$'::text) AND (jsonb_typeof((source_lineage -> 'capability_registry_hashes'::text)) = 'array'::text)))",
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
        definition: "CHECK (((manifest IS NULL) OR ((jsonb_typeof(manifest) = 'object'::text) AND (manifest ?& ARRAY['format_version'::text, 'training_dataset_id'::text, 'source_lineage'::text, 'cohort_manifest'::text, 'model_spec_id'::text, 'model_family'::text, 'model_spec_definition_hash'::text, 'trade_policy_artifact_id'::text, 'trade_policy_hash'::text, 'window_start'::text, 'window_end'::text, 'purpose'::text, 'knowledge_lag_secs'::text, 'sample_interval_secs'::text, 'horizons_secs'::text, 'feature_schema_version'::text, 'feature_schema_hash'::text, 'factor_serving_plane'::text, 'label_schema_hash'::text, 'semantic_dataset_hash'::text, 'source_fingerprint'::text, 'sample_count'::text]) AND ((manifest - ARRAY['format_version'::text, 'training_dataset_id'::text, 'source_lineage'::text, 'cohort_manifest'::text, 'model_spec_id'::text, 'model_family'::text, 'model_spec_definition_hash'::text, 'trade_policy_artifact_id'::text, 'trade_policy_hash'::text, 'window_start'::text, 'window_end'::text, 'purpose'::text, 'knowledge_lag_secs'::text, 'sample_interval_secs'::text, 'horizons_secs'::text, 'feature_schema_version'::text, 'feature_schema_hash'::text, 'factor_serving_plane'::text, 'label_schema_hash'::text, 'semantic_dataset_hash'::text, 'source_fingerprint'::text, 'sample_count'::text]) = '{}'::jsonb) AND (((manifest ->> 'format_version'::text))::integer = 4) AND (((manifest ->> 'training_dataset_id'::text))::uuid = training_dataset_id) AND ((manifest -> 'source_lineage'::text) = source_lineage) AND (((cohort_manifest IS NULL) AND ((manifest -> 'cohort_manifest'::text) = 'null'::jsonb)) OR ((cohort_manifest IS NOT NULL) AND ((manifest -> 'cohort_manifest'::text) = cohort_manifest))) AND (((manifest ->> 'model_spec_id'::text))::uuid = model_spec_id) AND ((manifest ->> 'model_family'::text) = (model_family)::text) AND ((manifest ->> 'model_spec_definition_hash'::text) = model_spec_definition_hash) AND ((((manifest -> 'trade_policy_artifact_id'::text) = 'null'::jsonb) AND ((manifest -> 'trade_policy_hash'::text) = 'null'::jsonb)) OR ((jsonb_typeof((manifest -> 'trade_policy_artifact_id'::text)) = 'string'::text) AND (((manifest ->> 'trade_policy_artifact_id'::text))::uuid IS NOT NULL) AND (jsonb_typeof((manifest -> 'trade_policy_hash'::text)) = 'string'::text) AND ((manifest ->> 'trade_policy_hash'::text) ~ '^blake3:[0-9a-f]{64}$'::text))) AND (((manifest ->> 'window_start'::text))::timestamp with time zone = window_start) AND (((manifest ->> 'window_end'::text))::timestamp with time zone = window_end) AND ((manifest ->> 'purpose'::text) = (purpose)::text) AND (((manifest ->> 'knowledge_lag_secs'::text))::bigint = knowledge_lag_secs) AND (((manifest ->> 'sample_interval_secs'::text))::bigint = sample_interval_secs) AND ((manifest -> 'horizons_secs'::text) = horizons_secs) AND (((manifest ->> 'feature_schema_version'::text))::integer = feature_schema_version) AND ((manifest ->> 'feature_schema_hash'::text) = feature_schema_hash) AND ((manifest -> 'factor_serving_plane'::text) = factor_serving_plane) AND ((factor_serving_plane ->> 'factor_schema_hash'::text) = factor_schema_hash) AND ((manifest ->> 'label_schema_hash'::text) = label_schema_hash) AND ((manifest ->> 'semantic_dataset_hash'::text) = dataset_hash) AND ((manifest ->> 'source_fingerprint'::text) ~ '^blake3:[0-9a-f]{64}$'::text) AND (((manifest ->> 'sample_count'::text))::bigint = sample_count))))",
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
        definition: concat!(
            "CHECK (window_start < window_end ",
            "AND coverage >= 0::numeric AND coverage <= 1::numeric ",
            "AND sample_count >= 0 AND missing_feature_count >= 0 ",
            "AND hit_rate >= 0::numeric AND hit_rate <= 1::numeric ",
            "AND liquidity_feasibility >= 0::numeric ",
            "AND liquidity_feasibility <= 1::numeric ",
            "AND jsonb_typeof(expected_vs_realized) = 'object'::text ",
            "AND jsonb_typeof(category_breakdown) = 'array'::text ",
            "AND jsonb_typeof(report_pnl_simulation) = 'object'::text ",
            "AND jsonb_typeof(portfolio_funnel) = 'object'::text ",
            "AND portfolio_funnel ?& ARRAY[",
            "'schema_version'::text, 'decision_tick_count'::text, ",
            "'emitted_candidate_count'::text, ",
            "'candidate_without_executable_tier_count'::text, ",
            "'executable_tier_count'::text, 'admission_rejected_tier_count'::text, ",
            "'admitted_tier_count'::text, 'selected_tier_count'::text, ",
            "'executed_entry_count'::text, 'resolved_allocation_count'::text, ",
            "'no_candidate_tick_count'::text, 'no_executable_tier_tick_count'::text, ",
            "'no_selection_tick_count'::text, 'selected_tick_count'::text, ",
            "'tier_exclusion_reasons'::text] ",
            "AND portfolio_funnel - ARRAY[",
            "'schema_version'::text, 'decision_tick_count'::text, ",
            "'emitted_candidate_count'::text, ",
            "'candidate_without_executable_tier_count'::text, ",
            "'executable_tier_count'::text, 'admission_rejected_tier_count'::text, ",
            "'admitted_tier_count'::text, 'selected_tier_count'::text, ",
            "'executed_entry_count'::text, 'resolved_allocation_count'::text, ",
            "'no_candidate_tick_count'::text, 'no_executable_tier_tick_count'::text, ",
            "'no_selection_tick_count'::text, 'selected_tick_count'::text, ",
            "'tier_exclusion_reasons'::text] = '{}'::jsonb ",
            "AND (portfolio_funnel ->> 'schema_version')::integer = 1 ",
            "AND (portfolio_funnel ->> 'executable_tier_count')::bigint = ",
            "(portfolio_funnel ->> 'admission_rejected_tier_count')::bigint + ",
            "(portfolio_funnel ->> 'admitted_tier_count')::bigint ",
            "AND (portfolio_funnel ->> 'selected_tier_count')::bigint = ",
            "(portfolio_funnel ->> 'executed_entry_count')::bigint ",
            "AND (portfolio_funnel ->> 'resolved_allocation_count')::bigint <= ",
            "(portfolio_funnel ->> 'executed_entry_count')::bigint ",
            "AND (portfolio_funnel ->> 'resolved_allocation_count')::bigint = sample_count ",
            "AND (portfolio_funnel ->> 'decision_tick_count')::bigint = ",
            "(portfolio_funnel ->> 'no_candidate_tick_count')::bigint + ",
            "(portfolio_funnel ->> 'no_executable_tier_tick_count')::bigint + ",
            "(portfolio_funnel ->> 'no_selection_tick_count')::bigint + ",
            "(portfolio_funnel ->> 'selected_tick_count')::bigint ",
            "AND jsonb_typeof(portfolio_funnel -> 'tier_exclusion_reasons') = 'array'::text ",
            "AND report_hash ~ '^blake3:[0-9a-f]{64}$'::text)"
        ),
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
    fn trade_hash_contract_precise() {
        let trade = constraint("ck_quant_execution_trade_ref_identity").definition;
        assert!(trade.contains("trade_status <> 'mined'"));
        assert!(!trade.contains("'confirmed'::qp_venue_trade_status"));
    }

    #[test]
    fn outcome_terminal_contract_precise() {
        let outcome = constraint("ck_quant_execution_attempt_outcome_contract").definition;
        assert!(outcome.contains(
            "entry_order_state = ANY (ARRAY['cancelled'::qp_execution_order_state, 'failed'::qp_execution_order_state])"
        ));
        assert!(
            !outcome.contains("entry_order_state = 'partially_filled'::qp_execution_order_state")
        );
    }

    #[test]
    fn artifact_versions_are_relational() {
        let source = constraint("ck_quant_source_slice_manifest").definition;
        assert!(source.contains("format_version'::text))::integer = 5"));
        assert!(source.contains("dataset_format_version'::text))::integer = 4"));
        assert!(!source.contains("dataset_format_version'::text))::integer = 3"));

        let dataset = constraint("ck_quant_training_dataset_manifest").definition;
        for binding in [
            "format_version'::text))::integer = 4",
            "model_family",
            "feature_schema_version",
            "factor_serving_plane",
            "manifest - ARRAY['format_version'",
            "source_fingerprint",
            "trade_policy_artifact_id",
        ] {
            assert!(
                dataset.contains(binding),
                "missing Dataset v4 binding {binding}"
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
    fn promotion_permit_schema_relational() {
        let permit = constraint("ck_quant_feedback_promotion_permit").definition;
        for binding in [
            "category = 'crypto'",
            "category = 'weather'",
            "crypto_price_15m",
            "weather_forecast_24h",
            "expected_policy_generation > 0",
            "allowed_runtime_modes =",
            "revoked_by_user_id IS NULL",
            "revoked_by_user_id IS NOT NULL",
            "revision = 0",
            "revision = 1",
            "updated_at = revoked_at",
        ] {
            assert!(permit.contains(binding), "missing permit binding {binding}");
        }

        for name in [
            "fk_quant_feedback_permit_profile",
            "fk_quant_feedback_permit_snapshot",
            "fk_quant_feedback_permit_champion",
            "fk_quant_feedback_permit_issuer",
            "fk_quant_feedback_permit_revoker",
        ] {
            let definition = constraint(name).definition;
            assert!(definition.contains("ON UPDATE NO ACTION"));
            assert!(definition.contains("ON DELETE NO ACTION"));
            assert!(!definition.contains("CASCADE"));
        }
    }

    #[test]
    fn permit_check_dump_stable() {
        let permit = constraint("ck_quant_feedback_promotion_permit").definition;
        assert!(!permit.contains(" BETWEEN "));
        assert!(!permit.contains("allowed_runtime_modes IN"));
        assert_eq!(permit.matches("allowed_runtime_modes =").count(), 7);
    }

    #[test]
    fn feedback_schema_is_relational() {
        let cycle = constraint("ck_quant_feedback_cycle_identity").definition;
        for binding in [
            "champion_serving_contract_hash",
            "champion_model_spec_definition_hash",
            "decision_policy_snapshot_hash",
            "policy_bundle_generation",
            "route_generation",
            "evaluation_mode",
            "parent_cycle_id",
            "forced_idempotency_key",
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
            "status = 'quarantined'",
            "invalid_coordinator_state",
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
            "champion_model_version_id <> candidate_model_version_id",
            "champion_serving_contract_hash <> candidate_serving_contract_hash",
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
        for binding in [
            "(kind = 'feedback_truth_freeze'::qp_research_job_kind) AND (result_kind = 'feedback_truth_freeze_artifact'::qp_research_job_result_kind)",
            "(kind = 'feedback_attribution'::qp_research_job_kind) AND (result_kind = 'feedback_attribution_manifest'::qp_research_job_result_kind)",
            "(kind = 'feedback_validation'::qp_research_job_kind) AND (result_kind = 'feedback_validation_artifact'::qp_research_job_result_kind)",
            "(kind = 'feedback_decision'::qp_research_job_kind) AND (result_kind = 'feedback_decision_artifact'::qp_research_job_result_kind)",
        ] {
            assert!(
                job_result.contains(binding),
                "missing research-job result binding {binding}"
            );
        }
        let job_artifact = constraint("ck_quant_research_job_artifact_reference").definition;
        for binding in [
            "(result_artifact_uri IS NULL) = (result_artifact_hash IS NULL)",
            "octet_length(result_artifact_uri) >= 1",
            "octet_length(result_artifact_uri) <= 4096",
            "^[a-z][a-z0-9+.-]*://.+$",
            "^blake3:[0-9a-f]{64}$",
            "feedback_truth_freeze_artifact",
            "feedback_coverage_artifact",
            "feedback_attribution_manifest",
            "feedback_drift_artifact",
            "feedback_learning_stage_artifact",
            "feedback_validation_artifact",
            "feedback_comparison_artifact",
            "feedback_shadow_artifact",
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
    fn recipe_closes_cpcv_contract() {
        let recipe = constraint("ck_quant_feedback_recipe_template").definition;
        for binding in [
            "format_version')::integer = 3",
            "'target_horizon_secs'",
            "'purge_embargo_secs'",
            "'validation') - ARRAY",
            "'cpcv') - ARRAY",
            "'nested_estimator_holdout_bps'",
            "'nested_estimator_min_groups'",
            "'nested_estimator_holdout_bps')::integer < 10000",
            "'nested_estimator_min_groups')::integer >= 4",
            "'trials') - ARRAY",
            "'pbo') - ARRAY",
            "'gates') - ARRAY",
            "'min_cpcv_paths')::integer >= 21",
            "'k_test')::integer <",
        ] {
            assert!(
                recipe.contains(binding),
                "missing recipe CPCV binding {binding}"
            );
        }
    }

    #[test]
    fn feedback_bounds_are_explicit() {
        let outbox = constraint("ck_quant_feedback_event_outbox").definition;
        assert!(outbox.contains("octet_length(last_error) >= 1"));
        assert!(outbox.contains("octet_length(last_error) <= 2048"));
        assert!(
            outbox.contains(
                "(feedback_stage_event_id IS NULL) <> (feedback_trigger_event_id IS NULL)"
            )
        );
        assert!(!outbox.contains("octet_length(last_error) BETWEEN"));

        let job = constraint("ck_quant_research_job_artifact_reference").definition;
        assert!(job.contains("octet_length(result_artifact_uri) >= 1"));
        assert!(job.contains("octet_length(result_artifact_uri) <= 4096"));
        assert!(!job.contains("octet_length(result_artifact_uri) BETWEEN"));
    }
}
