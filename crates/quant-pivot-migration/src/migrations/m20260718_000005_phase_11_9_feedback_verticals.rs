use sea_orm_migration::prelude::*;

use crate::{MigrationSpec, audit, migration_spec};

use super::support::{phase_11_9, v1};

const NAME: &str = "m20260718_000005_phase_11_9_feedback_verticals";
const SOURCE: &[u8] = include_bytes!("m20260718_000005_phase_11_9_feedback_verticals.rs");

const TABLES: &[&str] = &[
    "quant_domain_source_expectation",
    "quant_feedback_run",
    "quant_feedback_run_stage",
    "quant_drift_report",
    "quant_factor_bundle_artifact",
    "quant_factor_governance_audit",
    "quant_profile_allocation_artifact",
];

const TRIGGERS: &[v1::TriggerSpec] = &[
    v1::TriggerSpec {
        name: "trg_quant_domain_source_expectation_updated_at",
        table: "quant_domain_source_expectation",
        events: v1::TriggerEvents::Update,
        program: v1::TriggerProgram::SetUpdatedAt,
    },
    v1::TriggerSpec {
        name: "trg_quant_feedback_run_updated_at",
        table: "quant_feedback_run",
        events: v1::TriggerEvents::Update,
        program: v1::TriggerProgram::SetUpdatedAt,
    },
    v1::TriggerSpec {
        name: "trg_quant_feedback_run_stage_append_only",
        table: "quant_feedback_run_stage",
        events: v1::TriggerEvents::DeleteOrUpdate,
        program: v1::TriggerProgram::DenyWrite,
    },
    v1::TriggerSpec {
        name: "trg_quant_drift_report_append_only",
        table: "quant_drift_report",
        events: v1::TriggerEvents::DeleteOrUpdate,
        program: v1::TriggerProgram::DenyWrite,
    },
    v1::TriggerSpec {
        name: "trg_quant_factor_bundle_artifact_append_only",
        table: "quant_factor_bundle_artifact",
        events: v1::TriggerEvents::DeleteOrUpdate,
        program: v1::TriggerProgram::DenyWrite,
    },
    v1::TriggerSpec {
        name: "trg_quant_factor_governance_audit_append_only",
        table: "quant_factor_governance_audit",
        events: v1::TriggerEvents::DeleteOrUpdate,
        program: v1::TriggerProgram::DenyWrite,
    },
    v1::TriggerSpec {
        name: "trg_quant_profile_allocation_artifact_append_only",
        table: "quant_profile_allocation_artifact",
        events: v1::TriggerEvents::DeleteOrUpdate,
        program: v1::TriggerProgram::DenyWrite,
    },
];

const CREATE_SQL: &str = r"
ALTER TABLE quant_market_linkage
    ADD COLUMN capability_registry_hash TEXT;

CREATE TYPE qp_domain_source_expectation_status AS ENUM (
    'not_started',
    'live',
    'stale',
    'credential_blocked',
    'error',
    'unsupported'
);

CREATE TABLE quant_domain_source_expectation (
    expectation_id UUID PRIMARY KEY,
    family qp_domain_family NOT NULL,
    source_id TEXT NOT NULL,
    instrument_key TEXT NOT NULL,
    capability_registry_hash TEXT NOT NULL,
    binding_hash TEXT NOT NULL,
    required BOOLEAN NOT NULL,
    credential_required BOOLEAN NOT NULL,
    freshness_secs BIGINT NOT NULL,
    affected_market_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    affected_profile_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    status qp_domain_source_expectation_status NOT NULL,
    status_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_quant_domain_source_expectation_binding
        UNIQUE (source_id, instrument_key),
    CONSTRAINT uq_quant_domain_source_expectation_binding_hash UNIQUE (binding_hash),
    CONSTRAINT ck_quant_domain_source_expectation_freshness CHECK (freshness_secs > 0),
    CONSTRAINT ck_quant_domain_source_expectation_blocker_reason CHECK (
        status NOT IN ('credential_blocked', 'error', 'unsupported')
        OR (status_reason IS NOT NULL AND btrim(status_reason) <> '')
    )
);

CREATE INDEX idx_quant_domain_source_expectation_family_status
    ON quant_domain_source_expectation (family, status, source_id, instrument_key);

CREATE TABLE quant_feedback_run (
    feedback_run_id UUID PRIMARY KEY,
    profile_id TEXT NOT NULL,
    category qp_market_category NOT NULL,
    trigger_kind TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    label_cutoff TIMESTAMPTZ NOT NULL,
    feedback_policy_hash TEXT NOT NULL,
    capability_registry_hash TEXT NOT NULL,
    runtime_config_version_id UUID,
    dataset_id UUID,
    challenger_model_version_id UUID,
    challenger_factor_bundle_id UUID,
    decision TEXT,
    decision_reason TEXT,
    requested_by TEXT,
    acting_role TEXT NOT NULL,
    cancel_requested_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_quant_feedback_run_trigger_kind
        CHECK (trigger_kind IN ('scheduled', 'label_maturity', 'data_drift', 'concept_drift', 'label_drift', 'manual')),
    CONSTRAINT ck_quant_feedback_run_status
        CHECK (status IN ('queued', 'running', 'passed', 'rejected', 'failed', 'cancelled')),
    CONSTRAINT ck_quant_feedback_run_decision
        CHECK (decision IS NULL OR decision IN ('promote', 'reject', 'insufficient_evidence'))
);

CREATE INDEX idx_quant_feedback_run_profile_created
    ON quant_feedback_run (profile_id, created_at DESC, feedback_run_id DESC);
CREATE INDEX idx_quant_feedback_run_status_created
    ON quant_feedback_run (status, created_at, feedback_run_id);

CREATE TABLE quant_feedback_run_stage (
    feedback_run_stage_id UUID PRIMARY KEY,
    feedback_run_id UUID NOT NULL REFERENCES quant_feedback_run(feedback_run_id),
    sequence INTEGER NOT NULL,
    stage_kind TEXT NOT NULL,
    status TEXT NOT NULL,
    evidence_hash TEXT NOT NULL,
    metrics_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    detail TEXT,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_quant_feedback_run_stage_sequence UNIQUE (feedback_run_id, sequence),
    CONSTRAINT ck_quant_feedback_run_stage_kind
        CHECK (stage_kind IN ('coverage', 'drift', 'dataset_seal', 'train', 'calibration_cpcv', 'champion_comparison', 'shadow_replay', 'decision')),
    CONSTRAINT ck_quant_feedback_run_stage_status
        CHECK (status IN ('running', 'passed', 'rejected', 'failed', 'cancelled'))
);

CREATE INDEX idx_quant_feedback_run_stage_timeline
    ON quant_feedback_run_stage (feedback_run_id, sequence);

CREATE TABLE quant_drift_report (
    drift_report_id UUID PRIMARY KEY,
    feedback_run_id UUID REFERENCES quant_feedback_run(feedback_run_id),
    profile_id TEXT NOT NULL,
    category qp_market_category NOT NULL,
    drift_kind TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    detected BOOLEAN NOT NULL,
    severity TEXT NOT NULL,
    metrics_json JSONB NOT NULL,
    report_hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_quant_drift_report_kind
        CHECK (drift_kind IN ('data', 'concept', 'label')),
    CONSTRAINT ck_quant_drift_report_severity
        CHECK (severity IN ('none', 'warning', 'critical')),
    CONSTRAINT ck_quant_drift_report_window CHECK (window_start < window_end)
);

CREATE INDEX idx_quant_drift_report_profile_window
    ON quant_drift_report (profile_id, window_end DESC, drift_report_id DESC);

CREATE TABLE quant_factor_bundle_artifact (
    factor_bundle_id UUID PRIMARY KEY,
    profile_id TEXT NOT NULL,
    category qp_market_category NOT NULL,
    feature_schema_version INTEGER NOT NULL,
    feature_contract_hash TEXT NOT NULL,
    definition_refs JSONB NOT NULL,
    bundle_hash TEXT NOT NULL UNIQUE,
    artifact_uri TEXT NOT NULL,
    status TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_quant_factor_bundle_artifact_status
        CHECK (status IN ('candidate', 'published', 'retired')),
    CONSTRAINT ck_quant_factor_bundle_artifact_schema CHECK (feature_schema_version > 0)
);

CREATE INDEX idx_quant_factor_bundle_profile_status
    ON quant_factor_bundle_artifact (profile_id, status, created_at DESC);

CREATE TABLE quant_factor_governance_audit (
    factor_governance_audit_id UUID PRIMARY KEY,
    feedback_run_id UUID REFERENCES quant_feedback_run(feedback_run_id),
    actor TEXT NOT NULL,
    acting_role TEXT NOT NULL,
    action TEXT NOT NULL,
    reason TEXT NOT NULL,
    model_version_id UUID NOT NULL,
    factor_bundle_id UUID NOT NULL REFERENCES quant_factor_bundle_artifact(factor_bundle_id),
    before_pointer_hash TEXT,
    after_pointer_hash TEXT NOT NULL,
    audit_hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_quant_factor_governance_action
        CHECK (action IN ('publish_bundle_with_model', 'reject_bundle', 'retire_bundle'))
);

CREATE INDEX idx_quant_factor_governance_timeline
    ON quant_factor_governance_audit (created_at DESC, factor_governance_audit_id DESC);

CREATE TABLE quant_profile_allocation_artifact (
    profile_allocation_id UUID PRIMARY KEY,
    candidate_family_hash TEXT NOT NULL,
    evaluation_window_hash TEXT NOT NULL,
    correction_method TEXT NOT NULL,
    allocation_json JSONB NOT NULL,
    artifact_hash TEXT NOT NULL UNIQUE,
    activation_status TEXT NOT NULL DEFAULT 'proposed',
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_quant_profile_allocation_correction
        CHECK (correction_method = 'romano_wolf_stepdown'),
    CONSTRAINT ck_quant_profile_allocation_status
        CHECK (activation_status IN ('proposed', 'activated', 'rejected'))
);
";

pub struct Migration;

impl MigrationName for Migration {
    fn name(&self) -> &str {
        NAME
    }
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        phase_11_9::execute_batch(manager, CREATE_SQL).await?;
        for trigger in TRIGGERS {
            v1::create_trigger(manager, *trigger).await?;
        }
        audit::record(manager, spec()).await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        audit::remove(manager, NAME).await?;
        for trigger in TRIGGERS.iter().rev() {
            v1::drop_trigger(manager, *trigger).await?;
        }
        for table in TABLES.iter().rev() {
            manager
                .drop_table(Table::drop().table(Alias::new(*table)).to_owned())
                .await?;
        }
        phase_11_9::execute_batch(
            manager,
            "DROP TYPE qp_domain_source_expectation_status; \
             ALTER TABLE quant_market_linkage DROP COLUMN capability_registry_hash",
        )
        .await?;
        Ok(())
    }
}

pub fn spec() -> MigrationSpec {
    migration_spec(NAME, &[SOURCE, phase_11_9::SOURCE, v1::SOURCE])
}
