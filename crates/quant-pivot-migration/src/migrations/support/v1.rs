//! Sealed `PostgreSQL` DDL primitives not currently modeled by `SeaQuery`.

use sea_orm::{DbBackend, Statement};
use sea_orm_migration::prelude::*;

pub(in crate::migrations) const SOURCE: &[u8] = include_bytes!("v1.rs");

const EMPTY_BOOT_TARGET_SQL: &str = "WITH target_objects AS (\
    SELECT 'relation:' || c.relname AS object_name \
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
    WHERE n.nspname = 'public' \
      AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f') \
      AND c.relname <> 'seaql_migrations' \
    UNION ALL \
    SELECT 'type:' || t.typname FROM pg_type t \
    JOIN pg_namespace n ON n.oid = t.typnamespace \
    WHERE n.nspname = 'public' AND t.typtype IN ('e', 'd', 'r') \
    UNION ALL \
    SELECT 'function:' || p.proname FROM pg_proc p \
    JOIN pg_namespace n ON n.oid = p.pronamespace \
    WHERE n.nspname = 'public' \
    UNION ALL \
    SELECT 'trigger:' || g.tgname FROM pg_trigger g \
    JOIN pg_class c ON c.oid = g.tgrelid \
    JOIN pg_namespace n ON n.oid = c.relnamespace \
    WHERE n.nspname = 'public' AND NOT g.tgisinternal \
) SELECT COUNT(*)::bigint AS object_count, \
    COALESCE((SELECT string_agg(object_name, ', ') FROM (\
        SELECT object_name FROM target_objects ORDER BY object_name LIMIT 20\
    ) sample), '') AS object_summary FROM target_objects";

const FACTOR_DEFINITION_VALIDATOR_SQL: &str = r#"
CREATE FUNCTION public.validate_factor_definition_document(document jsonb)
RETURNS boolean
LANGUAGE plpgsql
IMMUTABLE
STRICT
AS $function$
DECLARE
    item jsonb;
    item_name text;
    previous_name text := NULL;
    semantic_key text;
BEGIN
    IF jsonb_typeof(document) <> 'object'
       OR NOT document ?& ARRAY[
           'name', 'family', 'input_features', 'output',
           'normalization', 'owner', 'required', 'computation'
       ]
       OR (document - ARRAY[
           'name', 'family', 'input_features', 'output',
           'normalization', 'owner', 'required', 'computation'
       ]) <> '{}'::jsonb
       OR jsonb_typeof(document->'name') <> 'string'
       OR octet_length(document->>'name') NOT BETWEEN 1 AND 256
       OR (document->>'name') !~ '^[a-z][a-z0-9]*([._][a-z0-9]+)*$'
       OR jsonb_typeof(document->'family') <> 'string'
       OR (document->>'family') <> ALL (ARRAY[
           'liquidity', 'microstructure', 'momentum', 'mean_reversion',
           'volatility', 'activity', 'resolution', 'data_quality', 'structural',
           'domain_crypto', 'domain_weather'
       ])
       OR jsonb_typeof(document->'input_features') <> 'array'
       OR jsonb_typeof(document->'output') <> 'object'
       OR jsonb_typeof(document#>'{output,output_kind}') <> 'string'
       OR (document#>>'{output,output_kind}')
          <> ALL (ARRAY['outcome_alpha', 'context', 'diagnostic'])
       OR (
           document#>>'{output,output_kind}' = 'outcome_alpha'
           AND (
               NOT (document->'output') ?& ARRAY['output_kind', 'orientation']
               OR ((document->'output') - ARRAY['output_kind', 'orientation']) <> '{}'::jsonb
               OR jsonb_typeof(document#>'{output,orientation}') <> 'string'
               OR (document#>>'{output,orientation}')
                  <> ALL (ARRAY['feature_token', 'canonical_yes'])
           )
       )
       OR (
           document#>>'{output,output_kind}' = 'context'
           AND (
               NOT (document->'output') ?& ARRAY['output_kind', 'effect']
               OR ((document->'output') - ARRAY['output_kind', 'effect']) <> '{}'::jsonb
               OR jsonb_typeof(document#>'{output,effect}') <> 'string'
               OR (document#>>'{output,effect}')
                  <> ALL (ARRAY['higher_is_supportive', 'lower_is_supportive'])
           )
       )
       OR (
           document#>>'{output,output_kind}' = 'diagnostic'
           AND (
               NOT (document->'output') ? 'output_kind'
               OR ((document->'output') - 'output_kind') <> '{}'::jsonb
           )
       )
       OR jsonb_typeof(document->'normalization') <> 'string'
       OR (document->>'normalization') <> ALL (ARRAY['winsorized_zscore', 'rank', 'min_max'])
       OR jsonb_typeof(document->'owner') <> 'string'
       OR octet_length(document->>'owner') NOT BETWEEN 1 AND 256
       OR (document->>'owner') <> btrim(document->>'owner')
       OR (document->>'owner') ~ '^[[:space:]]|[[:space:]]$'
       OR jsonb_typeof(document->'required') <> 'boolean'
       OR jsonb_typeof(document->'computation') <> 'object'
       OR NOT (document->'computation') ?& ARRAY['semantic_version', 'semantic_key']
       OR ((document->'computation') - ARRAY['semantic_version', 'semantic_key']) <> '{}'::jsonb
       OR jsonb_typeof(document#>'{computation,semantic_version}') <> 'number'
       OR (document#>>'{computation,semantic_version}') !~ '^[1-9][0-9]*$'
       OR (document#>>'{computation,semantic_version}')::numeric > 4294967295::numeric
       OR jsonb_typeof(document#>'{computation,semantic_key}') <> 'string'
    THEN
        RETURN FALSE;
    END IF;

    semantic_key := document#>>'{computation,semantic_key}';
    IF octet_length(semantic_key) NOT BETWEEN 1 AND 4096
       OR semantic_key <> btrim(semantic_key)
       OR semantic_key !~ '^[!-~]+$'
       OR position(chr(34) IN semantic_key) > 0
       OR position(chr(92) IN semantic_key) > 0
    THEN
        RETURN FALSE;
    END IF;

    FOR item IN SELECT value FROM jsonb_array_elements(document->'input_features')
    LOOP
        IF jsonb_typeof(item) <> 'string' THEN
            RETURN FALSE;
        END IF;
        item_name := item #>> '{}';
        IF octet_length(item_name) NOT BETWEEN 1 AND 256
           OR item_name !~ '^[a-z][a-z0-9]*([._][a-z0-9]+)*$'
           OR previous_name IS NOT NULL
              AND previous_name COLLATE "C" >= item_name COLLATE "C"
        THEN
            RETURN FALSE;
        END IF;
        previous_name := item_name;
    END LOOP;

    RETURN TRUE;
EXCEPTION WHEN OTHERS THEN
    RETURN FALSE;
END;
$function$"#;

const FACTOR_EXPLANATION_VALIDATOR_SQL: &str = r#"
CREATE FUNCTION public.validate_factor_explanation(document jsonb)
RETURNS boolean
LANGUAGE plpgsql
IMMUTABLE
STRICT
AS $function$
DECLARE
    item jsonb;
    feature_name text;
    previous_name text := NULL;
    contribution_text text;
    contribution_value numeric;
BEGIN
    IF jsonb_typeof(document) <> 'object'
       OR NOT document ?& ARRAY['headline', 'drivers']
       OR (document - ARRAY['headline', 'drivers']) <> '{}'::jsonb
       OR jsonb_typeof(document->'headline') <> 'string'
       OR octet_length(document->>'headline') NOT BETWEEN 1 AND 4096
       OR (document->>'headline') !~ '[^[:space:]]'
       OR jsonb_typeof(document->'drivers') <> 'array'
    THEN
        RETURN FALSE;
    END IF;

    FOR item IN SELECT value FROM jsonb_array_elements(document->'drivers')
    LOOP
        IF jsonb_typeof(item) <> 'object'
           OR NOT item ?& ARRAY['feature_name', 'contribution']
           OR (item - ARRAY['feature_name', 'contribution']) <> '{}'::jsonb
           OR jsonb_typeof(item->'feature_name') <> 'string'
           OR octet_length(item->>'feature_name') NOT BETWEEN 1 AND 256
           OR (item->>'feature_name') !~ '^[a-z][a-z0-9]*([._][a-z0-9]+)*$'
           OR jsonb_typeof(item->'contribution') <> 'string'
        THEN
            RETURN FALSE;
        END IF;
        feature_name := item->>'feature_name';
        contribution_text := item->>'contribution';
        IF previous_name IS NOT NULL
           AND previous_name COLLATE "C" >= feature_name COLLATE "C"
           OR octet_length(contribution_text) NOT BETWEEN 1 AND 128
           OR contribution_text !~ '^-?(0|[1-9][0-9]*)(\.[0-9]+)?$'
        THEN
            RETURN FALSE;
        END IF;
        contribution_value := contribution_text::numeric;
        IF scale(contribution_value) > 28
           OR abs(contribution_value)
              * power(10::numeric, scale(contribution_value))
              > 79228162514264337593543950335::numeric
        THEN
            RETURN FALSE;
        END IF;
        previous_name := feature_name;
    END LOOP;
    RETURN TRUE;
EXCEPTION WHEN OTHERS THEN
    RETURN FALSE;
END;
$function$"#;

const FACTOR_PLANE_VALIDATOR_SQL: &str = r#"
CREATE FUNCTION public.validate_factor_serving_plane(
    plane jsonb,
    feature_hash text,
    feature_version integer,
    model_family_name text
)
RETURNS boolean
LANGUAGE plpgsql
IMMUTABLE
STRICT
AS $function$
DECLARE
    item jsonb;
    definition jsonb;
    definition_name text;
    definition_id text;
    definition_hash text;
    previous_name text := NULL;
    seen_ids text[] := ARRAY[]::text[];
    seen_hashes text[] := ARRAY[]::text[];
    definition_count integer;
BEGIN
    IF jsonb_typeof(plane) <> 'object'
       OR NOT plane ?& ARRAY['format_version', 'factor_schema_hash', 'definitions']
       OR (plane - ARRAY['format_version', 'factor_schema_hash', 'definitions']) <> '{}'::jsonb
       OR jsonb_typeof(plane->'format_version') <> 'number'
       OR plane->>'format_version' <> '2'
       OR jsonb_typeof(plane->'factor_schema_hash') <> 'string'
       OR (plane->>'factor_schema_hash') !~ '^blake3:[0-9a-f]{64}$'
       OR jsonb_typeof(plane->'definitions') <> 'array'
       OR feature_hash !~ '^blake3:[0-9a-f]{64}$'
       OR feature_version < 1
    THEN
        RETURN FALSE;
    END IF;

    definition_count := jsonb_array_length(plane->'definitions');
    IF model_family_name = 'classical_logistic_regression'
    THEN
        IF definition_count <> 0 THEN
            RETURN FALSE;
        END IF;
    ELSIF model_family_name = ANY (ARRAY['weighted_factor', 'hold_vs_exit_weighted']) THEN
        IF definition_count = 0 THEN
            RETURN FALSE;
        END IF;
    ELSE
        RETURN FALSE;
    END IF;

    FOR item IN SELECT value FROM jsonb_array_elements(plane->'definitions')
    LOOP
        IF jsonb_typeof(item) <> 'object'
           OR NOT item ?& ARRAY[
               'revision_version', 'factor_definition_id', 'definition_hash',
               'feature_contract_hash', 'input_schema_version',
               'output_schema_version', 'definition'
           ]
           OR (item - ARRAY[
               'revision_version', 'factor_definition_id', 'definition_hash',
               'feature_contract_hash', 'input_schema_version',
               'output_schema_version', 'definition'
           ]) <> '{}'::jsonb
           OR jsonb_typeof(item->'revision_version') <> 'number'
           OR item->>'revision_version' <> '2'
           OR jsonb_typeof(item->'factor_definition_id') <> 'string'
           OR (item->>'factor_definition_id') !~
              '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
           OR jsonb_typeof(item->'definition_hash') <> 'string'
           OR (item->>'definition_hash') !~ '^blake3:[0-9a-f]{64}$'
           OR jsonb_typeof(item->'feature_contract_hash') <> 'string'
           OR item->>'feature_contract_hash' <> feature_hash
           OR jsonb_typeof(item->'input_schema_version') <> 'number'
           OR item->>'input_schema_version' !~ '^[1-9][0-9]*$'
           OR (item->>'input_schema_version')::integer <> feature_version
           OR jsonb_typeof(item->'output_schema_version') <> 'number'
           OR item->>'output_schema_version' !~ '^[1-9][0-9]*$'
           OR (item->>'output_schema_version')::numeric > 2147483647::numeric
           OR NOT public.validate_factor_definition_document(item->'definition')
        THEN
            RETURN FALSE;
        END IF;

        definition := item->'definition';
        definition_name := definition->>'name';
        definition_id := item->>'factor_definition_id';
        definition_hash := item->>'definition_hash';
        IF previous_name IS NOT NULL
           AND previous_name COLLATE "C" >= definition_name COLLATE "C"
           OR definition_id = ANY (seen_ids)
           OR definition_hash = ANY (seen_hashes)
        THEN
            RETURN FALSE;
        END IF;
        previous_name := definition_name;
        seen_ids := array_append(seen_ids, definition_id);
        seen_hashes := array_append(seen_hashes, definition_hash);
    END LOOP;
    RETURN TRUE;
EXCEPTION WHEN OTHERS THEN
    RETURN FALSE;
END;
$function$"#;

const CONTENT_HASH_ARRAY_VALIDATOR_SQL: &str = r#"
CREATE FUNCTION public.validate_content_hash_array(document jsonb)
RETURNS boolean
LANGUAGE plpgsql
IMMUTABLE
STRICT
AS $function$
DECLARE
    item jsonb;
    current_hash text;
    previous_hash text := NULL;
BEGIN
    IF jsonb_typeof(document) <> 'array' THEN
        RETURN FALSE;
    END IF;
    FOR item IN SELECT value FROM jsonb_array_elements(document)
    LOOP
        IF jsonb_typeof(item) <> 'string' THEN
            RETURN FALSE;
        END IF;
        current_hash := item #>> '{}';
        IF current_hash !~ '^blake3:[0-9a-f]{64}$'
           OR previous_hash IS NOT NULL
              AND previous_hash COLLATE "C" >= current_hash COLLATE "C"
        THEN
            RETURN FALSE;
        END IF;
        previous_hash := current_hash;
    END LOOP;
    RETURN TRUE;
EXCEPTION WHEN OTHERS THEN
    RETURN FALSE;
END;
$function$"#;

const FEEDBACK_CYCLE_GUARD_SQL: &str = "CREATE FUNCTION \
    public.trigger_guard_feedback_cycle() RETURNS trigger LANGUAGE plpgsql AS \
    $function$ BEGIN \
    IF TG_OP = 'DELETE' THEN \
    RAISE EXCEPTION 'feedback-cycle row is immutable after terminalization; DELETE is not permitted'; \
    END IF; \
    IF (to_jsonb(NEW) - ARRAY['status', 'decision', 'terminal_reason_code', 'generation', \
    'lease_owner', 'lease_expires_at', 'stage_resume_after', 'cancel_requested_at', 'started_at', 'completed_at', \
    'updated_at']) IS DISTINCT FROM \
    (to_jsonb(OLD) - ARRAY['status', 'decision', 'terminal_reason_code', 'generation', \
    'lease_owner', 'lease_expires_at', 'stage_resume_after', 'cancel_requested_at', 'started_at', 'completed_at', \
    'updated_at']) THEN \
    RAISE EXCEPTION 'feedback-cycle frozen identity cannot change'; \
    END IF; \
    IF OLD.status = 'succeeded'::public.qp_feedback_cycle_status \
    AND OLD.decision = 'candidate_ready'::public.qp_feedback_decision \
    AND NEW.status = 'succeeded'::public.qp_feedback_cycle_status \
    AND NEW.decision = 'promoted'::public.qp_feedback_decision THEN \
    IF NEW.generation <> OLD.generation + 1 \
    OR NEW.terminal_reason_code IS DISTINCT FROM OLD.terminal_reason_code \
    OR NEW.lease_owner IS DISTINCT FROM OLD.lease_owner \
    OR NEW.lease_expires_at IS DISTINCT FROM OLD.lease_expires_at \
    OR NEW.stage_resume_after IS DISTINCT FROM OLD.stage_resume_after \
    OR NEW.cancel_requested_at IS DISTINCT FROM OLD.cancel_requested_at \
    OR NEW.started_at IS DISTINCT FROM OLD.started_at \
    OR NEW.completed_at IS DISTINCT FROM OLD.completed_at \
    OR NOT EXISTS (SELECT 1 FROM public.policy_activation AS activation \
    INNER JOIN public.quant_model_governance_audit AS audit \
    ON audit.audit_id = activation.model_governance_audit_id \
    AND audit.promotion_permit_id = activation.promotion_permit_id \
    AND audit.promotion_transaction_hash = activation.promotion_transaction_hash \
    WHERE activation.activation_kind = 'model_promotion'::public.qp_policy_activation_kind \
    AND ((audit.detail #>> '{record,preflight,feedback_cycle_id}')::uuid) = OLD.feedback_cycle_id) \
    THEN RAISE EXCEPTION 'CandidateReady-to-Promoted requires one exact committed promotion graph'; \
    END IF; \
    NEW.updated_at = statement_timestamp(); \
    RETURN NEW; \
    END IF; \
    IF NEW.status = 'quarantined'::public.qp_feedback_cycle_status \
    AND (NEW.terminal_reason_code IS DISTINCT FROM 'invalid_coordinator_state'::text \
    OR NOT EXISTS (SELECT 1 FROM public.quant_feedback_coordinator_fault AS fault \
    WHERE fault.feedback_cycle_id = OLD.feedback_cycle_id \
    AND fault.lease_generation = OLD.generation \
    AND fault.worker_id = OLD.lease_owner)) THEN \
    RAISE EXCEPTION 'coordinator quarantine requires exact WORM fault evidence'; \
    END IF; \
    IF OLD.status IN ('succeeded'::public.qp_feedback_cycle_status, \
    'failed'::public.qp_feedback_cycle_status, 'cancelled'::public.qp_feedback_cycle_status, \
    'quarantined'::public.qp_feedback_cycle_status) \
    OR NEW.generation <> OLD.generation + 1 \
    OR NOT ((OLD.status = 'queued'::public.qp_feedback_cycle_status AND \
    NEW.status IN ('running'::public.qp_feedback_cycle_status, \
    'cancelled'::public.qp_feedback_cycle_status)) OR \
    (OLD.status = 'running'::public.qp_feedback_cycle_status AND \
    NEW.status IN ('running'::public.qp_feedback_cycle_status, \
    'succeeded'::public.qp_feedback_cycle_status, 'failed'::public.qp_feedback_cycle_status, \
    'cancelled'::public.qp_feedback_cycle_status, \
    'quarantined'::public.qp_feedback_cycle_status))) THEN \
    RAISE EXCEPTION 'illegal feedback-cycle CAS transition from % generation % to % generation %', \
    OLD.status, OLD.generation, NEW.status, NEW.generation; \
    END IF; \
    NEW.updated_at = statement_timestamp(); \
    RETURN NEW; END; $function$";

const FEEDBACK_OUTBOX_GUARD_SQL: &str = "CREATE FUNCTION \
    public.trigger_guard_feedback_outbox() RETURNS trigger LANGUAGE plpgsql AS \
    $function$ BEGIN \
    IF TG_OP = 'DELETE' THEN \
    RAISE EXCEPTION 'feedback outbox revision is durable; DELETE is not permitted'; \
    END IF; \
    IF OLD.published_at IS NOT NULL OR \
    (to_jsonb(NEW) - ARRAY['published_at', 'publish_attempts', 'claim_owner', \
    'lease_expires_at', 'last_error', 'updated_at']) IS DISTINCT FROM \
    (to_jsonb(OLD) - ARRAY['published_at', 'publish_attempts', 'claim_owner', \
    'lease_expires_at', 'last_error', 'updated_at']) OR \
    NEW.publish_attempts < OLD.publish_attempts OR \
    NEW.publish_attempts > OLD.publish_attempts + 1 THEN \
    RAISE EXCEPTION 'feedback outbox immutable identity or delivery lifecycle changed illegally'; \
    END IF; \
    NEW.updated_at = statement_timestamp(); \
    RETURN NEW; END; $function$";

const MODEL_ROUTE_SHADOW_BINDING_GUARD_SQL: &str = "CREATE FUNCTION \
    public.trigger_guard_model_route_shadow_binding() RETURNS trigger LANGUAGE plpgsql AS \
    $function$ BEGIN \
    IF TG_OP = 'DELETE' THEN \
    RAISE EXCEPTION 'route-owned shadow binding is durable; DELETE is not permitted'; \
    END IF; \
    IF (to_jsonb(NEW) - ARRAY['status', 'lifecycle_generation', 'terminated_at', 'updated_at', \
    'termination_policy_activation_id', 'termination_request_hash', \
    'termination_reason_code', 'termination_note', 'termination_actor_role']) \
    IS DISTINCT FROM \
    (to_jsonb(OLD) - ARRAY['status', 'lifecycle_generation', 'terminated_at', 'updated_at', \
    'termination_policy_activation_id', 'termination_request_hash', \
    'termination_reason_code', 'termination_note', 'termination_actor_role']) THEN \
    RAISE EXCEPTION 'route-owned shadow binding immutable identity or receipt cannot change'; \
    END IF; \
    IF OLD.status <> 'active'::public.qp_shadow_binding_status \
    OR NEW.status NOT IN ('rejected'::public.qp_shadow_binding_status, \
    'promoted'::public.qp_shadow_binding_status, 'cancelled'::public.qp_shadow_binding_status) \
    OR NEW.lifecycle_generation <> OLD.lifecycle_generation + 1 \
    OR NEW.terminated_at IS NOT NULL \
    OR NEW.updated_at IS DISTINCT FROM OLD.updated_at THEN \
    RAISE EXCEPTION 'illegal route-owned shadow binding terminal CAS transition'; \
    END IF; \
    IF NEW.status IN ('rejected'::public.qp_shadow_binding_status, \
    'cancelled'::public.qp_shadow_binding_status, \
    'promoted'::public.qp_shadow_binding_status) AND \
    (OLD.termination_policy_activation_id IS NOT NULL \
    OR NEW.termination_policy_activation_id IS NULL) THEN \
    RAISE EXCEPTION 'terminal shadow transition requires one immutable activation identity'; \
    ELSIF NEW.status IN ('rejected'::public.qp_shadow_binding_status, \
    'cancelled'::public.qp_shadow_binding_status) AND \
    (OLD.termination_request_hash IS NOT NULL OR OLD.termination_reason_code IS NOT NULL \
    OR OLD.termination_note IS NOT NULL OR OLD.termination_actor_role IS NOT NULL \
    OR NEW.termination_request_hash IS NULL OR NEW.termination_reason_code IS NULL \
    OR NEW.termination_note IS NULL OR NEW.termination_actor_role IS NULL) THEN \
    RAISE EXCEPTION 'governed shadow termination requires one immutable evidence set'; \
    ELSIF NEW.status = 'promoted'::public.qp_shadow_binding_status AND \
    (NEW.termination_request_hash IS NOT NULL OR NEW.termination_reason_code IS NOT NULL \
    OR NEW.termination_note IS NOT NULL OR NEW.termination_actor_role IS NOT NULL) THEN \
    RAISE EXCEPTION 'promotion shadow transition cannot add rejection or cancellation evidence'; \
    ELSIF NEW.status NOT IN ('rejected'::public.qp_shadow_binding_status, \
    'cancelled'::public.qp_shadow_binding_status, \
    'promoted'::public.qp_shadow_binding_status) AND \
    (NEW.termination_policy_activation_id IS DISTINCT FROM OLD.termination_policy_activation_id \
    OR NEW.termination_request_hash IS DISTINCT FROM OLD.termination_request_hash \
    OR NEW.termination_reason_code IS DISTINCT FROM OLD.termination_reason_code \
    OR NEW.termination_note IS DISTINCT FROM OLD.termination_note \
    OR NEW.termination_actor_role IS DISTINCT FROM OLD.termination_actor_role) THEN \
    RAISE EXCEPTION 'non-governed shadow transition cannot add termination evidence'; \
    END IF; \
    IF NEW.status IN ('rejected'::public.qp_shadow_binding_status, \
    'cancelled'::public.qp_shadow_binding_status, \
    'promoted'::public.qp_shadow_binding_status) THEN \
    SELECT activation.activated_at INTO NEW.terminated_at \
    FROM public.policy_activation AS activation \
    WHERE activation.policy_activation_id = NEW.termination_policy_activation_id \
    AND ((NEW.status = 'rejected'::public.qp_shadow_binding_status \
    AND activation.activation_kind = 'model_shadow_rejection'::public.qp_policy_activation_kind) \
    OR (NEW.status = 'cancelled'::public.qp_shadow_binding_status \
    AND activation.activation_kind = 'model_shadow_cancellation'::public.qp_policy_activation_kind) \
    OR (NEW.status = 'promoted'::public.qp_shadow_binding_status \
    AND activation.activation_kind = 'model_promotion'::public.qp_policy_activation_kind)); \
    IF NEW.terminated_at IS NULL THEN \
    RAISE EXCEPTION 'terminal shadow activation kind or timestamp is invalid'; \
    END IF; \
    ELSE \
    NEW.terminated_at = statement_timestamp(); \
    END IF; \
    NEW.updated_at = NEW.terminated_at; \
    RETURN NEW; END; $function$";

const PROMOTION_PERMIT_GUARD_SQL: &str = "CREATE FUNCTION \
    public.trigger_guard_promotion_permit() RETURNS trigger LANGUAGE plpgsql AS \
    $function$ BEGIN \
    IF TG_OP = 'DELETE' THEN \
    RAISE EXCEPTION 'promotion permit is durable; DELETE is not permitted'; \
    END IF; \
    IF NEW IS NOT DISTINCT FROM OLD THEN \
    RETURN OLD; \
    END IF; \
    IF (to_jsonb(NEW) - ARRAY['revoked_by_user_id', 'revoked_by_username', \
    'revoked_by_role', 'revocation_reason', 'revoked_at', 'revision', 'updated_at']) \
    IS DISTINCT FROM \
    (to_jsonb(OLD) - ARRAY['revoked_by_user_id', 'revoked_by_username', \
    'revoked_by_role', 'revocation_reason', 'revoked_at', 'revision', 'updated_at']) THEN \
    RAISE EXCEPTION 'promotion-permit immutable issuance cannot change'; \
    END IF; \
    IF OLD.revoked_by_user_id IS NOT NULL \
    OR OLD.revoked_by_username IS NOT NULL \
    OR OLD.revoked_by_role IS NOT NULL \
    OR OLD.revocation_reason IS NOT NULL \
    OR OLD.revoked_at IS NOT NULL \
    OR OLD.revision <> 0 THEN \
    RAISE EXCEPTION 'promotion permit is already revoked'; \
    END IF; \
    IF NEW.revoked_by_user_id IS NULL \
    OR NEW.revoked_by_username IS NULL \
    OR NEW.revoked_by_role IS NULL \
    OR NEW.revocation_reason IS NULL \
    OR NEW.revoked_at IS NOT NULL \
    OR NEW.revision <> OLD.revision + 1 \
    OR NEW.updated_at IS DISTINCT FROM OLD.updated_at THEN \
    RAISE EXCEPTION 'promotion permit revoke requires one complete DB-clock-owned CAS tuple'; \
    END IF; \
    NEW.revoked_at = statement_timestamp(); \
    NEW.updated_at = NEW.revoked_at; \
    RETURN NEW; END; $function$";

const DENY_WRITE_TRIGGER_SQL: &str = "CREATE FUNCTION public.trigger_deny_write() RETURNS trigger \
    LANGUAGE plpgsql AS $function$ BEGIN RAISE EXCEPTION \
    'table % is append-only (WORM); % is not permitted', TG_TABLE_NAME, TG_OP; END; $function$";

const CALIBRATION_ARTIFACT_GUARD_SQL: &str = "CREATE FUNCTION \
    public.trigger_guard_calibration_artifact() RETURNS trigger LANGUAGE plpgsql AS \
    $function$ BEGIN \
    IF TG_OP = 'DELETE' THEN \
    RAISE EXCEPTION 'calibration-artifact row is immutable; DELETE is not permitted'; \
    END IF; \
    IF (to_jsonb(NEW) - 'active') IS DISTINCT FROM (to_jsonb(OLD) - 'active') THEN \
    RAISE EXCEPTION 'calibration-artifact immutable identity and payload cannot change'; \
    END IF; \
    RETURN NEW; END; $function$";

const UPDATED_AT_TRIGGER_SQL: &str = "CREATE FUNCTION public.trigger_set_updated_at() RETURNS \
    trigger LANGUAGE plpgsql AS $function$ BEGIN IF (to_jsonb(NEW) - 'updated_at') IS DISTINCT FROM \
    (to_jsonb(OLD) - 'updated_at') THEN NEW.updated_at = statement_timestamp(); ELSE \
    NEW.updated_at = OLD.updated_at; END IF; RETURN NEW; END; $function$";

const RESEARCH_JOB_GUARD_SQL: &str = "CREATE FUNCTION \
    public.trigger_guard_research_job() RETURNS trigger LANGUAGE plpgsql AS \
    $function$ BEGIN \
    IF (to_jsonb(NEW) - ARRAY['status', 'progress_json', 'result_kind', 'result_ref', \
    'result_artifact_uri', 'result_artifact_hash', \
    'error_json', 'coverage_json', 'recovery_attempt', 'next_attempt_at', 'lease_owner', 'lease_expires_at', \
    'started_at', 'finished_at', 'heartbeat_at', 'updated_at']) IS DISTINCT FROM \
    (to_jsonb(OLD) - ARRAY['status', 'progress_json', 'result_kind', 'result_ref', \
    'result_artifact_uri', 'result_artifact_hash', \
    'error_json', 'coverage_json', 'recovery_attempt', 'next_attempt_at', 'lease_owner', 'lease_expires_at', \
    'started_at', 'finished_at', 'heartbeat_at', 'updated_at']) THEN \
    RAISE EXCEPTION 'research-job immutable identity and enqueue contract cannot change'; \
    END IF; \
    NEW.updated_at = statement_timestamp(); \
    RETURN NEW; END; $function$";

const ECONOMIC_TASK_GUARD_SQL: &str = "CREATE FUNCTION public.trigger_guard_economic_outcome_task() \
    RETURNS trigger LANGUAGE plpgsql AS $function$ BEGIN \
    IF TG_OP = 'DELETE' THEN \
    RAISE EXCEPTION 'economic replay task cannot be deleted'; END IF; \
    IF ROW(NEW.recommendation_id, NEW.horizon_at, NEW.created_at) IS DISTINCT FROM \
       ROW(OLD.recommendation_id, OLD.horizon_at, OLD.created_at) THEN \
    RAISE EXCEPTION 'economic replay task identity is immutable'; END IF; \
    IF OLD.replay_until IS NOT NULL AND \
       ROW(NEW.replay_until, NEW.source_cutoff_at, NEW.resolution_outcome_hash) IS DISTINCT FROM \
       ROW(OLD.replay_until, OLD.source_cutoff_at, OLD.resolution_outcome_hash) THEN \
    RAISE EXCEPTION 'economic replay first-claim boundary is immutable'; END IF; \
    IF OLD.status = 'completed'::public.qp_outcome_reconciliation_task_status THEN \
    RAISE EXCEPTION 'completed economic replay task is immutable'; END IF; \
    IF NEW.status = 'completed'::public.qp_outcome_reconciliation_task_status AND NOT EXISTS \
       (SELECT 1 FROM public.quant_recommendation_economic_outcome \
        WHERE recommendation_id = NEW.recommendation_id) THEN \
    RAISE EXCEPTION 'economic task completion requires its durable outcome'; END IF; \
    NEW.updated_at = statement_timestamp(); \
    RETURN NEW; END; $function$";

/// Fail closed unless `public` contains only the migration infrastructure.
/// `PostgreSQL`'s heterogeneous catalog cannot be expressed by `SeaQuery`, so the
/// complete static statement is sealed inside this versioned dialect module.
pub(in crate::migrations) async fn assert_empty_boot_target(
    manager: &SchemaManager<'_>,
) -> Result<(), DbErr> {
    let row = manager
        .get_connection()
        .query_one_raw(Statement::from_string(
            DbBackend::Postgres,
            EMPTY_BOOT_TARGET_SQL,
        ))
        .await?
        .ok_or_else(|| {
            DbErr::Custom("PostgreSQL catalog returned no boot preflight row".to_owned())
        })?;
    let object_count = row.try_get::<i64>("", "object_count")?;
    if object_count == 0 {
        return Ok(());
    }
    let object_summary = row.try_get::<String>("", "object_summary")?;
    Err(DbErr::Custom(format!(
        "boot migration requires an empty public schema; found {object_count} tables, views, materialized views, sequences, types, functions, or triggers ({object_summary}). Clear PostgreSQL and bootstrap again"
    )))
}

/// Table constraint kinds supported by the audited `PostgreSQL` extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::migrations) enum ConstraintKind {
    Check,
    ForeignKey,
    Unique,
}

/// Immutable table constraint captured from the canonical schema contract.
#[derive(Debug, Clone, Copy)]
pub(in crate::migrations) struct ConstraintSpec {
    pub name: &'static str,
    pub table: &'static str,
    pub kind: ConstraintKind,
    pub definition: &'static str,
}

/// The only trigger programs owned by the initial schema contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::migrations) enum TriggerProgram {
    DenyWrite,
    GuardCalibrationArtifact,
    GuardFeedbackCycle,
    GuardFeedbackOutbox,
    GuardModelRouteShadowBinding,
    GuardPromotionPermit,
    GuardFactorValue,
    GuardModelRun,
    GuardResearchJob,
    GuardEconomicOutcomeTask,
    GuardSourceSlice,
    GuardTrainingDataset,
    SetUpdatedAt,
}

impl TriggerProgram {
    const fn function_name(self) -> &'static str {
        match self {
            Self::DenyWrite => "trigger_deny_write",
            Self::GuardCalibrationArtifact => "trigger_guard_calibration_artifact",
            Self::GuardFeedbackCycle => "trigger_guard_feedback_cycle",
            Self::GuardFeedbackOutbox => "trigger_guard_feedback_outbox",
            Self::GuardModelRouteShadowBinding => "trigger_guard_model_route_shadow_binding",
            Self::GuardPromotionPermit => "trigger_guard_promotion_permit",
            Self::GuardFactorValue => "trigger_guard_factor_value",
            Self::GuardModelRun => "trigger_guard_model_run",
            Self::GuardResearchJob => "trigger_guard_research_job",
            Self::GuardEconomicOutcomeTask => "trigger_guard_economic_outcome_task",
            Self::GuardSourceSlice => "trigger_guard_source_slice",
            Self::GuardTrainingDataset => "trigger_guard_training_dataset",
            Self::SetUpdatedAt => "trigger_set_updated_at",
        }
    }
}

/// Trigger event sets supported by the initial schema contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::migrations) enum TriggerEvents {
    Update,
    DeleteOrUpdate,
    InsertOrDeleteOrUpdate,
}

impl TriggerEvents {
    const fn sql(self) -> &'static str {
        match self {
            Self::Update => "UPDATE",
            Self::DeleteOrUpdate => "DELETE OR UPDATE",
            Self::InsertOrDeleteOrUpdate => "INSERT OR DELETE OR UPDATE",
        }
    }
}

/// Immutable trigger binding captured from the canonical schema contract.
#[derive(Debug, Clone, Copy)]
pub(in crate::migrations) struct TriggerSpec {
    pub name: &'static str,
    pub table: &'static str,
    pub events: TriggerEvents,
    pub program: TriggerProgram,
}

pub(in crate::migrations) async fn create_constraint(
    manager: &SchemaManager<'_>,
    spec: ConstraintSpec,
) -> Result<(), DbErr> {
    (spec).validate_constraint_definition()?;
    execute(
        manager,
        format!(
            "ALTER TABLE {} ADD CONSTRAINT {} {}",
            qualified_table(spec.table),
            quote_identifier(spec.name),
            spec.definition
        ),
    )
    .await
}

pub(in crate::migrations) async fn create_validation_programs(
    manager: &SchemaManager<'_>,
) -> Result<(), DbErr> {
    for statement in [
        CONTENT_HASH_ARRAY_VALIDATOR_SQL,
        FACTOR_DEFINITION_VALIDATOR_SQL,
        FACTOR_EXPLANATION_VALIDATOR_SQL,
        FACTOR_PLANE_VALIDATOR_SQL,
    ] {
        execute(manager, statement.to_owned()).await?;
    }
    Ok(())
}

pub(in crate::migrations) async fn create_trigger_programs(
    manager: &SchemaManager<'_>,
) -> Result<(), DbErr> {
    for statement in [
        DENY_WRITE_TRIGGER_SQL,
        CALIBRATION_ARTIFACT_GUARD_SQL,
        FEEDBACK_CYCLE_GUARD_SQL,
        FEEDBACK_OUTBOX_GUARD_SQL,
        MODEL_ROUTE_SHADOW_BINDING_GUARD_SQL,
        PROMOTION_PERMIT_GUARD_SQL,
        RESEARCH_JOB_GUARD_SQL,
        ECONOMIC_TASK_GUARD_SQL,
        "CREATE FUNCTION public.trigger_guard_factor_value() RETURNS trigger LANGUAGE plpgsql AS \
         $function$ DECLARE \
         owning_kind public.qp_model_run_kind; \
         owning_window_start timestamptz; \
         owning_window_end timestamptz; \
         BEGIN \
         IF TG_OP <> 'INSERT' THEN \
         RAISE EXCEPTION 'factor-value ledger is append-only (WORM); % is not permitted', TG_OP; \
         END IF; \
         NEW.created_at = statement_timestamp(); \
         SELECT run_kind, window_start, window_end \
         INTO owning_kind, owning_window_start, owning_window_end \
         FROM public.quant_model_run \
         WHERE model_run_id = NEW.model_run_id \
         AND status = 'running'::public.qp_model_run_status \
         FOR UPDATE; \
         IF NOT FOUND THEN \
         RAISE EXCEPTION 'factor values require an owning Running model run'; \
         END IF; \
         IF ( \
         owning_kind IN ('live_inference'::public.qp_model_run_kind, \
         'shadow'::public.qp_model_run_kind) \
         AND (owning_window_start <> owning_window_end \
         OR NEW.decision_at <> owning_window_start) \
         ) OR ( \
         owning_kind NOT IN ('live_inference'::public.qp_model_run_kind, \
         'shadow'::public.qp_model_run_kind) \
         AND NEW.decision_at NOT BETWEEN owning_window_start AND owning_window_end \
         ) THEN \
         RAISE EXCEPTION 'factor value does not match owning model-run decision window'; \
         END IF; \
         PERFORM 1 FROM public.quant_feature_vector \
         WHERE feature_vector_id = NEW.feature_vector_id \
         AND market_id = NEW.market_id \
         AND decision_at = NEW.decision_at \
         FOR SHARE; \
         IF NOT FOUND THEN \
         RAISE EXCEPTION 'factor value does not match feature-vector market/decision lineage'; \
         END IF; \
         IF EXISTS ( \
         SELECT 1 FROM public.quant_factor_value \
         WHERE model_run_id = NEW.model_run_id \
         AND market_id = NEW.market_id \
         AND decision_at = NEW.decision_at \
         AND feature_vector_id <> NEW.feature_vector_id \
         ) THEN \
         RAISE EXCEPTION 'one factor plane cannot bind multiple feature vectors'; \
         END IF; \
         RETURN NEW; END; $function$",
        "CREATE FUNCTION public.trigger_guard_model_run() RETURNS trigger LANGUAGE plpgsql AS \
         $function$ BEGIN \
         IF TG_OP = 'DELETE' THEN \
         RAISE EXCEPTION 'model-run audit row is immutable; DELETE is not permitted'; \
         END IF; \
         IF OLD.status <> 'running'::public.qp_model_run_status OR \
         NEW.status NOT IN ('succeeded'::public.qp_model_run_status, \
         'failed'::public.qp_model_run_status, 'cancelled'::public.qp_model_run_status) THEN \
         RAISE EXCEPTION 'illegal model-run transition from % to %', OLD.status, NEW.status; \
         END IF; \
         IF (to_jsonb(NEW) - ARRAY['status', 'model_version_id', 'output_hash', 'error_code', \
         'error_message', 'finished_at']) IS DISTINCT FROM \
         (to_jsonb(OLD) - ARRAY['status', 'model_version_id', 'output_hash', 'error_code', \
         'error_message', 'finished_at']) THEN \
         RAISE EXCEPTION 'model-run immutable lineage cannot change during finalization'; \
         END IF; \
         IF NEW.finished_at IS NULL OR NEW.finished_at < OLD.started_at OR EXISTS ( \
         SELECT 1 FROM public.quant_factor_value \
         WHERE model_run_id = NEW.model_run_id AND created_at > NEW.finished_at \
         ) THEN \
         RAISE EXCEPTION 'model-run finish precedes its lifecycle start or an owned factor value'; \
         END IF; \
         RETURN NEW; END; $function$",
        "CREATE FUNCTION public.trigger_guard_source_slice() RETURNS trigger LANGUAGE plpgsql AS \
         $function$ BEGIN \
         IF TG_OP = 'DELETE' THEN \
         RAISE EXCEPTION 'source-slice ledger is immutable; DELETE is not permitted'; \
         END IF; \
         IF (to_jsonb(NEW) - ARRAY['status', 'manifest_uri', 'manifest_hash', 'manifest', \
         'failure_detail', 'completed_at']) IS DISTINCT FROM \
         (to_jsonb(OLD) - ARRAY['status', 'manifest_uri', 'manifest_hash', 'manifest', \
         'failure_detail', 'completed_at']) THEN \
         RAISE EXCEPTION 'source-slice identity is immutable'; \
         END IF; \
         IF OLD.status <> 'materializing'::public.qp_source_slice_status OR \
         NEW.status NOT IN ('ready'::public.qp_source_slice_status, \
         'failed'::public.qp_source_slice_status) THEN \
         RAISE EXCEPTION 'illegal source-slice transition from % to %', OLD.status, NEW.status; \
         END IF; \
         RETURN NEW; END; $function$",
        "CREATE FUNCTION public.trigger_guard_training_dataset() RETURNS trigger LANGUAGE plpgsql AS \
         $function$ BEGIN \
         IF TG_OP = 'DELETE' THEN \
         RAISE EXCEPTION 'training-dataset ledger is immutable; DELETE is not permitted'; \
         END IF; \
         IF (to_jsonb(NEW) - ARRAY['status', 'label_schema_hash', 'dataset_hash', \
         'manifest_hash', 'manifest', \
         'artifact_bytes_hash', 'parquet_uri', 'sample_count', 'coverage', \
         'failure_detail', 'completed_at']) IS DISTINCT FROM \
         (to_jsonb(OLD) - ARRAY['status', 'label_schema_hash', 'dataset_hash', \
         'manifest_hash', 'manifest', \
         'artifact_bytes_hash', 'parquet_uri', 'sample_count', 'coverage', \
         'failure_detail', 'completed_at']) THEN \
         RAISE EXCEPTION 'training-dataset plan identity is immutable'; \
         END IF; \
         IF OLD.status = 'planned'::public.qp_training_dataset_status AND \
         NEW.status = 'building'::public.qp_training_dataset_status THEN \
         IF (to_jsonb(NEW) - 'status') IS DISTINCT FROM (to_jsonb(OLD) - 'status') THEN \
         RAISE EXCEPTION 'planned-to-building may only change status'; END IF; \
         ELSIF OLD.status = 'planned'::public.qp_training_dataset_status AND \
         NEW.status = 'failed'::public.qp_training_dataset_status THEN \
         IF (to_jsonb(NEW) - ARRAY['status', 'failure_detail', 'completed_at']) IS DISTINCT FROM \
         (to_jsonb(OLD) - ARRAY['status', 'failure_detail', 'completed_at']) THEN \
         RAISE EXCEPTION 'planned-to-failed may only add terminal diagnostics'; END IF; \
         ELSIF OLD.status = 'building'::public.qp_training_dataset_status AND \
         NEW.status IN ('ready'::public.qp_training_dataset_status, \
         'insufficient_labels'::public.qp_training_dataset_status, \
         'failed'::public.qp_training_dataset_status) THEN \
         NULL; \
         ELSIF OLD.status = 'ready'::public.qp_training_dataset_status AND \
         NEW.status = 'expired'::public.qp_training_dataset_status THEN \
         IF (to_jsonb(NEW) - 'status') IS DISTINCT FROM (to_jsonb(OLD) - 'status') THEN \
         RAISE EXCEPTION 'ready-to-expired may only change status'; END IF; \
         ELSE \
         RAISE EXCEPTION 'illegal training-dataset transition from % to %', OLD.status, NEW.status; \
         END IF; \
         RETURN NEW; END; $function$",
        UPDATED_AT_TRIGGER_SQL,
    ] {
        execute(manager, statement.to_owned()).await?;
    }
    Ok(())
}

pub(in crate::migrations) async fn create_trigger(
    manager: &SchemaManager<'_>,
    spec: TriggerSpec,
) -> Result<(), DbErr> {
    execute(
        manager,
        format!(
            "CREATE TRIGGER {} BEFORE {} ON {} FOR EACH ROW EXECUTE FUNCTION public.{}()",
            quote_identifier(spec.name),
            spec.events.sql(),
            qualified_table(spec.table),
            quote_identifier(spec.program.function_name())
        ),
    )
    .await
}

pub(in crate::migrations) fn index_predicate(predicate: &'static str) -> Result<SimpleExpr, DbErr> {
    if predicate.is_empty() || predicate.contains(';') {
        return Err(DbErr::Custom("invalid partial-index predicate".to_owned()));
    }
    Ok(Expr::cust(predicate))
}

impl ConstraintSpec {
    fn validate_constraint_definition(self) -> Result<(), DbErr> {
        let valid = match self.kind {
            ConstraintKind::Check => self.definition.starts_with("CHECK ("),
            ConstraintKind::ForeignKey => self.definition.starts_with("FOREIGN KEY ("),
            ConstraintKind::Unique => self.definition.starts_with("UNIQUE ("),
        };
        if !valid || self.definition.contains(';') {
            return Err(DbErr::Custom(format!(
                "invalid {:?} definition for constraint `{}`",
                self.kind, self.name
            )));
        }
        Ok(())
    }
}

fn qualified_table(table: &str) -> String {
    format!("public.{}", quote_identifier(table))
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

async fn execute(manager: &SchemaManager<'_>, sql: String) -> Result<(), DbErr> {
    manager
        .get_connection()
        .execute_unprepared(&sql)
        .await
        .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::{
        CALIBRATION_ARTIFACT_GUARD_SQL, CONTENT_HASH_ARRAY_VALIDATOR_SQL, ConstraintKind,
        ConstraintSpec, FACTOR_DEFINITION_VALIDATOR_SQL, FACTOR_EXPLANATION_VALIDATOR_SQL,
        FACTOR_PLANE_VALIDATOR_SQL, index_predicate,
    };

    #[test]
    fn calibration_guard_freezes_payload() {
        for binding in [
            "TG_OP = 'DELETE'",
            "to_jsonb(NEW) - 'active'",
            "to_jsonb(OLD) - 'active'",
            "immutable identity and payload cannot change",
        ] {
            assert!(
                CALIBRATION_ARTIFACT_GUARD_SQL.contains(binding),
                "missing calibration-artifact guard binding {binding}"
            );
        }
    }

    #[test]
    fn content_hashes_are_canonical() {
        for binding in [
            "jsonb_typeof(document) <> 'array'",
            "'^blake3:[0-9a-f]{64}$'",
            "previous_hash COLLATE \"C\" >= current_hash COLLATE \"C\"",
        ] {
            assert!(
                CONTENT_HASH_ARRAY_VALIDATOR_SQL.contains(binding),
                "missing content-hash array binding {binding}"
            );
        }
    }

    #[test]
    fn factor_explanation_is_exact() {
        for binding in [
            "document ?& ARRAY['headline', 'drivers']",
            "document - ARRAY['headline', 'drivers']",
            "item ?& ARRAY['feature_name', 'contribution']",
            "item - ARRAY['feature_name', 'contribution']",
            "previous_name COLLATE \"C\" >= feature_name COLLATE \"C\"",
            "contribution_text::numeric",
            "scale(contribution_value) > 28",
            "79228162514264337593543950335::numeric",
        ] {
            assert!(
                FACTOR_EXPLANATION_VALIDATOR_SQL.contains(binding),
                "missing factor-explanation validator binding {binding}"
            );
        }
    }

    #[test]
    fn factor_contract_is_representable() {
        for binding in [
            "(document#>>'{computation,semantic_version}')::numeric > 4294967295::numeric",
            "(document->>'owner') ~ '^[[:space:]]|[[:space:]]$'",
            "octet_length(item_name) NOT BETWEEN 1 AND 256",
        ] {
            assert!(
                FACTOR_DEFINITION_VALIDATOR_SQL.contains(binding),
                "missing factor-definition validator binding {binding}"
            );
        }
        assert!(
            FACTOR_PLANE_VALIDATOR_SQL
                .contains("(item->>'output_schema_version')::numeric > 2147483647::numeric")
        );
    }

    #[test]
    fn rejects_constraint_mismatch_separator() {
        let unique_as_check = ConstraintSpec {
            name: "bad",
            table: "sample",
            kind: ConstraintKind::Check,
            definition: "UNIQUE (id)",
        };
        assert!((unique_as_check).validate_constraint_definition().is_err());

        let injected = ConstraintSpec {
            name: "bad",
            table: "sample",
            kind: ConstraintKind::Check,
            definition: "CHECK (id > 0); DROP TABLE sample",
        };
        assert!((injected).validate_constraint_definition().is_err());
    }

    #[test]
    fn rejects_empty_multi_predicate() {
        assert!(index_predicate("").is_err());
        assert!(index_predicate("status = 'active'; DROP TABLE sample").is_err());
    }
}
