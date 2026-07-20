//! Governed, independently revisioned Config resources.
//!
//! This is a clean-break API. A policy document is always strongly typed, a
//! draft must be validated and dependency-preflighted before approval, and an
//! activation must bind the exact approval, expected active revision,
//! short-lived preflight proof, and idempotency key.

use actix_web::{http::Method, web};
use chrono::{Duration, Utc};
use quant_pivot_error::config_validation::{
    ConfigValidationError, ConfigValidationReport, ConfigWarning,
};
use quant_pivot_models::{
    config::{DeployConfig, ProjectLifecyclePolicy, secret::SystemdCredentialRef},
    domain::{
        ActivatePolicyDraftRequest, ApprovePolicyDraftRequest, ConfigActivityQuery,
        ConfigActivityView, ConfigResourceSummaryView, ConfigResourcesView,
        ConfigSnapshotOptionsQuery, CoreEvent, CreatePolicyDraftRequest, CredentialHealthView,
        CurrentPolicyResourceView, DecisionPolicySnapshotOptionView, DeploymentConfigSnapshotView,
        DeploymentConfigView, DeploymentEndpointView, DeploymentIdentityView,
        DeploymentResourceBudgetView, DeploymentResourceLimitView, LifecycleCheckView,
        LifecycleView, NewDecisionPolicySnapshot, NewPolicyActivation, NewPolicyRevision,
        NewProductionBaseline, PolicyActivationCommit, PolicyActivationOutcome,
        PolicyActivationResultView, PolicyApprovalView, PolicyResourceSchemaView,
        PolicyRevisionInfo, PolicyRevisionListQuery, PolicyRevisionView, PolicyValidationView,
        PreparedPolicySnapshot, ProductionEvidenceInfo, RecordPolicyApproval,
        SchedulePreviewRequest, SchedulePreviewView, SealProductionRequest,
        ValidatePolicyDraftRequest, VerifiedSchemaFingerprints,
    },
    enums::{
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
        runtime_config::{
            CheckOutcome, ConfigAuditAction, ConfigResourceKind, CredentialHealthStatus,
            CredentialKind, DecisionPolicySnapshotSource, DeploymentEndpointKind,
            LifecycleBaseline, LifecycleCheckKind, PolicyActivationKind, PolicyActorKind,
            PolicyPreflightCheckKind, PolicyPreflightDetailCode, PolicyRevisionStatus,
            PolicyValidationCode, PolicyValidationSeverity, ProductionEvidenceKind,
            ProjectLifecycleState, ResourceBudgetKind, ResourceBudgetMetric, ResourceBudgetUnit,
        },
    },
    hashing::CanonicalDigest,
    runtime_config::{
        ActivePolicyBundle, DecisionPolicySnapshot, LifecycleCheckDetail,
        POLICY_RESOURCE_SCHEMA_VERSION, PolicyDocument, PolicyPreflightResult,
        PolicyRevisionBundle, PolicyValidationEvidence, PolicyValidationIssue,
        PolicyValidationSubject, ProductionSealCheck, ProductionSealEvidence, preview_fire_times,
        validate_runtime_config,
    },
    types::{
        AuditEventId, ContentHash, DecisionPolicySnapshotId, DeploymentEnvironment,
        PolicyActivationId, PolicyApprovalId, PolicyBundleGeneration, PolicyIdempotencyKey,
        PolicyPreflightToken, PolicyRevisionId, ProductionBaselineId,
        ProductionSealConfirmationPhrase, UserId,
    },
};
use serde::Serialize;
use uuid::Uuid;

use crate::{
    audit::OperationCtx,
    auth::casbin::Rule,
    error::WebError,
    extractors::{ActingRole, AuthedActor, RequestId, ValidatedJson},
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

const DEFAULT_LIST_LIMIT: u64 = 50;
const MAX_LIST_LIMIT: u64 = 200;
const PREFLIGHT_TTL_MINUTES: i64 = 10;
const POLICY_HASH_DOMAIN: &str = "quant-pivot/policy-revision";
const POLICY_ACTIVATION_HASH_DOMAIN: &str = "quant-pivot/policy-activation";

#[derive(Debug, Serialize)]
struct ConfigAuditDetail<'a> {
    resource_kind: ConfigResourceKind,
    policy_revision_id: &'a PolicyRevisionId,
    policy_approval_id: Option<&'a PolicyApprovalId>,
    policy_activation_id: Option<&'a PolicyActivationId>,
    activation_kind: Option<PolicyActivationKind>,
    acting_role: &'a str,
    request_id: &'a str,
}

#[derive(Debug, Serialize)]
struct ProductionSealAuditDetail<'a> {
    production_baseline_id: &'a ProductionBaselineId,
    environment: &'a str,
    build_commit: &'a str,
    policy_bundle_hash: &'a ContentHash,
    lifecycle_policy_hash: &'a ContentHash,
    reason: &'a str,
    acting_role: &'a str,
    request_id: &'a str,
}

#[derive(Debug, Serialize)]
struct PolicyActivationDigest<'a> {
    resource_kind: ConfigResourceKind,
    policy_revision_id: &'a PolicyRevisionId,
    policy_approval_id: &'a PolicyApprovalId,
    expected_bundle_generation: PolicyBundleGeneration,
    expected_active_revision_id: Option<&'a PolicyRevisionId>,
    candidate_snapshot_hash: &'a ContentHash,
    preflight_token_hash: &'a ContentHash,
    idempotency_key: &'a PolicyIdempotencyKey,
    activation_kind: PolicyActivationKind,
    actor_user_id: &'a UserId,
    actor_label: &'a str,
    reason: &'a str,
}

pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::GET,
            "/config/resources",
            Rule::ResourceOp(ResourceType::DecisionPolicySnapshot, Operation::Read),
            resources,
        ),
        spec(
            Method::GET,
            "/config/{kind}/current",
            Rule::ResourceOp(ResourceType::DecisionPolicySnapshot, Operation::Read),
            current_resource,
        ),
        spec(
            Method::GET,
            "/config/{kind}/schema",
            Rule::ResourceOp(ResourceType::DecisionPolicySnapshot, Operation::Read),
            resource_schema,
        ),
        spec(
            Method::GET,
            "/config/{kind}/revisions",
            Rule::ResourceOp(ResourceType::DecisionPolicySnapshot, Operation::Read),
            list_revisions,
        ),
        spec(
            Method::POST,
            "/config/{kind}/drafts",
            Rule::ActingRoleGoverned(ResourceType::DecisionPolicySnapshot, Operation::Create),
            create_draft,
        ),
        spec(
            Method::POST,
            "/config/{kind}/drafts/{id}/validate",
            Rule::ActingRoleGoverned(ResourceType::DecisionPolicySnapshot, Operation::Update),
            validate_draft,
        ),
        spec(
            Method::POST,
            "/config/{kind}/drafts/{id}/approve",
            Rule::ActingRoleGoverned(ResourceType::DecisionPolicySnapshot, Operation::Approve),
            approve_draft,
        ),
        spec(
            Method::POST,
            "/config/{kind}/drafts/{id}/activate",
            Rule::ActingRoleGoverned(ResourceType::DecisionPolicySnapshot, Operation::Activate),
            activate_draft,
        ),
        spec(
            Method::POST,
            "/config/{kind}/revisions/{id}/rollback",
            Rule::ActingRoleGoverned(ResourceType::DecisionPolicySnapshot, Operation::Rollback),
            rollback_revision,
        ),
        spec(
            Method::GET,
            "/config/activity",
            Rule::ResourceOp(ResourceType::DecisionPolicySnapshot, Operation::Read),
            activity,
        ),
        spec(
            Method::GET,
            "/config/snapshot-options",
            Rule::ResourceOp(ResourceType::DecisionPolicySnapshot, Operation::Read),
            snapshot_options,
        ),
        spec(
            Method::GET,
            "/config/deployment",
            Rule::ResourceOp(ResourceType::DecisionPolicySnapshot, Operation::Read),
            deployment,
        ),
        spec(
            Method::GET,
            "/config/lifecycle",
            Rule::ResourceOp(ResourceType::ConfigLifecycle, Operation::Read),
            lifecycle,
        ),
        spec(
            Method::POST,
            "/config/lifecycle/seal-production",
            Rule::ActingRoleGoverned(ResourceType::ConfigLifecycle, Operation::Seal),
            seal_production,
        ),
        spec(
            Method::POST,
            "/config/schedule-preview",
            Rule::ResourceOp(ResourceType::DecisionPolicySnapshot, Operation::Read),
            schedule_preview,
        ),
    ]
}

pub async fn resources(
    state: web::Data<AppState>,
) -> Result<WebResponse<ConfigResourcesView>, WebError> {
    let inventory = state.runtime_config.load_resource_inventory().await?;
    let summaries = inventory
        .resources
        .into_iter()
        .map(|resource| ConfigResourceSummaryView {
            kind: resource.resource_kind,
            schema_version: POLICY_RESOURCE_SCHEMA_VERSION,
            active_revision_id: resource.active_revision_id,
            active_revision_hash: resource.active_revision_hash,
            pending_approval_count: resource.pending_approval_count,
            effective_boundary: resource.resource_kind.apply_boundary(),
            restart_required: false,
            last_activated_at: resource.last_activated_at,
        })
        .collect();
    Ok(WebResponse::ok(ConfigResourcesView {
        resources: summaries,
        active_bundle_generation: inventory.bundle_generation,
        active_snapshot_id: inventory.active_snapshot_id,
        active_policy_bundle_hash: inventory.active_snapshot_hash,
    }))
}

pub async fn current_resource(
    state: web::Data<AppState>,
    kind: web::Path<ConfigResourceKind>,
) -> Result<WebResponse<CurrentPolicyResourceView>, WebError> {
    let kind = kind.into_inner();
    let current = state.runtime_config.load_current_resource(kind).await?;
    Ok(WebResponse::ok(match current {
        Some(current) => CurrentPolicyResourceView {
            resource: kind,
            revision: Some(current.revision.into()),
            activation: Some(current.activation.into()),
        },
        None => CurrentPolicyResourceView {
            resource: kind,
            revision: None,
            activation: None,
        },
    }))
}

pub async fn resource_schema(
    kind: web::Path<ConfigResourceKind>,
) -> Result<WebResponse<PolicyResourceSchemaView>, WebError> {
    let kind = kind.into_inner();
    Ok(WebResponse::ok(PolicyResourceSchemaView {
        kind,
        schema_version: POLICY_RESOURCE_SCHEMA_VERSION,
        json_schema: DecisionPolicySnapshot::resource_json_schema(kind),
        effective_boundary: kind.apply_boundary(),
        consumers: kind.consumers().to_vec(),
    }))
}

pub async fn list_revisions(
    state: web::Data<AppState>,
    kind: web::Path<ConfigResourceKind>,
    query: web::Query<PolicyRevisionListQuery>,
) -> Result<WebResponse<Vec<PolicyRevisionView>>, WebError> {
    let limit = bounded_limit(query.into_inner().limit);
    let revisions = state
        .runtime_config
        .list_revisions(kind.into_inner(), limit)
        .await?
        .into_iter()
        .map(Into::into)
        .collect();
    Ok(WebResponse::ok(revisions))
}

pub async fn create_draft(
    state: web::Data<AppState>,
    kind: web::Path<ConfigResourceKind>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<CreatePolicyDraftRequest>,
) -> Result<WebResponse<PolicyRevisionView>, WebError> {
    let kind = kind.into_inner();
    let body = body.into_inner();
    ensure_document_contract(kind, &body.document)?;
    let revision_hash = policy_document_hash(&body.document)?;
    let revision = state
        .runtime_config
        .create_revision(NewPolicyRevision {
            policy_revision_id: PolicyRevisionId::from_v7(),
            resource_kind: kind,
            schema_version: POLICY_RESOURCE_SCHEMA_VERSION,
            revision_hash: revision_hash.clone(),
            document: body.document,
            status: PolicyRevisionStatus::Draft,
            validation_evidence: None,
            validated_at: None,
            preflight_token_hash: None,
            preflight_expires_at: None,
            created_by_kind: PolicyActorKind::Operator,
            created_by_user_id: Some(actor_user_id(&actor)?),
            created_by_label: actor.claims.username.clone(),
            reason: body.reason,
        })
        .await?;
    op_ctx.set_action(
        OperationCategory::DecisionPolicySnapshot,
        ConfigAuditAction::DraftCreated.as_str(),
    );
    op_ctx.set_resource(
        ResourceType::DecisionPolicySnapshot,
        revision.policy_revision_id.to_string(),
    );
    op_ctx.set_state_hashes(None, Some(revision_hash));
    set_audit_detail(
        &op_ctx,
        &ConfigAuditDetail {
            resource_kind: kind,
            policy_revision_id: &revision.policy_revision_id,
            policy_approval_id: None,
            policy_activation_id: None,
            activation_kind: None,
            acting_role: &acting_role.0,
            request_id: &request_id.0,
        },
    )?;
    Ok(WebResponse::ok(revision.into()))
}

pub async fn validate_draft(
    state: web::Data<AppState>,
    path: web::Path<(ConfigResourceKind, PolicyRevisionId)>,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<ValidatePolicyDraftRequest>,
) -> Result<WebResponse<PolicyValidationView>, WebError> {
    let (kind, revision_id) = path.into_inner();
    let _body = body.into_inner();
    let revision = load_exact_revision(&state, kind, &revision_id).await?;
    if !matches!(
        revision.status,
        PolicyRevisionStatus::Draft | PolicyRevisionStatus::Validated
    ) {
        return Err(WebError::BadRequest(
            "only a draft or previously validated revision can be validated".to_owned(),
        ));
    }
    let active_bundle = state.runtime_config.load_current_bundle().await?;
    let (base_generation, base_revision_vector, mut candidate) = active_bundle.map_or_else(
        || {
            (
                PolicyBundleGeneration::FIRST,
                PolicyRevisionBundle::default(),
                DecisionPolicySnapshot::default(),
            )
        },
        |bundle| (bundle.generation, bundle.revision_vector, bundle.snapshot),
    );
    candidate
        .replace_resource_document(kind, revision.document.clone())
        .map_err(|error| WebError::BadRequest(error.to_string()))?;
    candidate.set_resource_revision_id(kind, revision_id.clone());

    let candidate_bundle_hash = candidate
        .persistence_hash()
        .map_err(|error| WebError::Internal(error.to_string()))?;
    let subject = PolicyValidationSubject {
        base_generation,
        base_revision_vector,
        candidate_bundle_hash,
    };
    let (evidence, prepared) = validate_and_prepare(&state, candidate, subject).await;
    let valid = evidence.is_valid();
    let (preflight_token, preflight_expires_at) = if valid {
        drop(prepared);
        let token = new_preflight_token()?;
        let token_hash = preflight_token_hash(&token)?;
        let expires_at = Utc::now() + Duration::minutes(PREFLIGHT_TTL_MINUTES);
        state
            .runtime_config
            .mark_revision_validated(&revision_id, evidence.clone(), token_hash, expires_at)
            .await?;
        (Some(token), Some(expires_at))
    } else {
        (None, None)
    };
    op_ctx.set_action(
        OperationCategory::DecisionPolicySnapshot,
        ConfigAuditAction::DraftValidated.as_str(),
    );
    op_ctx.set_resource(
        ResourceType::DecisionPolicySnapshot,
        revision_id.to_string(),
    );
    set_audit_detail(
        &op_ctx,
        &ConfigAuditDetail {
            resource_kind: kind,
            policy_revision_id: &revision_id,
            policy_approval_id: None,
            policy_activation_id: None,
            activation_kind: None,
            acting_role: &acting_role.0,
            request_id: &request_id.0,
        },
    )?;
    Ok(WebResponse::ok(PolicyValidationView {
        policy_revision_id: revision_id,
        resource_kind: kind,
        valid,
        validation_evidence: evidence,
        preflight_token,
        preflight_expires_at,
        effective_boundary: kind.apply_boundary(),
        affected_consumers: kind.consumers().to_vec(),
    }))
}

pub async fn approve_draft(
    state: web::Data<AppState>,
    path: web::Path<(ConfigResourceKind, PolicyRevisionId)>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<ApprovePolicyDraftRequest>,
) -> Result<WebResponse<PolicyApprovalView>, WebError> {
    let (kind, revision_id) = path.into_inner();
    let body = body.into_inner();
    if body.expires_at.is_some_and(|expiry| expiry <= Utc::now()) {
        return Err(WebError::BadRequest(
            "approval expiry must be in the future".to_owned(),
        ));
    }
    let approval = state
        .runtime_config
        .record_approval(RecordPolicyApproval {
            policy_approval_id: PolicyApprovalId::from_v7(),
            policy_revision_id: revision_id.clone(),
            resource_kind: kind,
            decision: body.decision,
            decided_by_kind: PolicyActorKind::Operator,
            decided_by_user_id: Some(actor_user_id(&actor)?),
            decided_by_label: actor.claims.username.clone(),
            reason: body.reason,
            decided_at: Utc::now(),
            expires_at: body.expires_at,
        })
        .await?;
    op_ctx.set_action(
        OperationCategory::DecisionPolicySnapshot,
        ConfigAuditAction::ApprovalRecorded.as_str(),
    );
    op_ctx.set_resource(
        ResourceType::DecisionPolicySnapshot,
        revision_id.to_string(),
    );
    op_ctx.set_state_hashes(None, Some(approval.revision_hash.clone()));
    set_audit_detail(
        &op_ctx,
        &ConfigAuditDetail {
            resource_kind: kind,
            policy_revision_id: &revision_id,
            policy_approval_id: Some(&approval.policy_approval_id),
            policy_activation_id: None,
            activation_kind: None,
            acting_role: &acting_role.0,
            request_id: &request_id.0,
        },
    )?;
    Ok(WebResponse::ok(approval.into()))
}

pub async fn activate_draft(
    state: web::Data<AppState>,
    path: web::Path<(ConfigResourceKind, PolicyRevisionId)>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<ActivatePolicyDraftRequest>,
) -> Result<WebResponse<PolicyActivationResultView>, WebError> {
    transition_revision(
        &state,
        path.into_inner(),
        body.into_inner(),
        PolicyTransitionContext {
            actor: &actor,
            acting_role: &acting_role,
            request_id: &request_id,
            op_ctx: &op_ctx,
            activation_kind: PolicyActivationKind::Promote,
        },
    )
    .await
    .map(WebResponse::ok)
}

pub async fn rollback_revision(
    state: web::Data<AppState>,
    path: web::Path<(ConfigResourceKind, PolicyRevisionId)>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<ActivatePolicyDraftRequest>,
) -> Result<WebResponse<PolicyActivationResultView>, WebError> {
    transition_revision(
        &state,
        path.into_inner(),
        body.into_inner(),
        PolicyTransitionContext {
            actor: &actor,
            acting_role: &acting_role,
            request_id: &request_id,
            op_ctx: &op_ctx,
            activation_kind: PolicyActivationKind::Rollback,
        },
    )
    .await
    .map(WebResponse::ok)
}

pub async fn activity(
    state: web::Data<AppState>,
    query: web::Query<ConfigActivityQuery>,
) -> Result<WebResponse<Vec<ConfigActivityView>>, WebError> {
    let limit = bounded_limit(query.into_inner().limit);
    let events = state
        .runtime_config
        .list_activity(limit)
        .await?
        .into_iter()
        .map(Into::into)
        .collect();
    Ok(WebResponse::ok(events))
}

pub async fn snapshot_options(
    state: web::Data<AppState>,
    query: web::Query<ConfigSnapshotOptionsQuery>,
) -> Result<WebResponse<Vec<DecisionPolicySnapshotOptionView>>, WebError> {
    let limit = bounded_limit(query.into_inner().limit);
    let options = state
        .runtime_config
        .list_snapshot_options(limit)
        .await?
        .into_iter()
        .map(Into::into)
        .collect();
    Ok(WebResponse::ok(options))
}

pub async fn schedule_preview(
    body: ValidatedJson<SchedulePreviewRequest>,
) -> Result<WebResponse<SchedulePreviewView>, WebError> {
    let body = body.into_inner();
    let next_fire_times = preview_fire_times(&body.cadence, Utc::now(), usize::from(body.count))
        .map_err(|error| WebError::BadRequest(error.to_string()))?;
    Ok(WebResponse::ok(SchedulePreviewView { next_fire_times }))
}

pub async fn lifecycle(state: web::Data<AppState>) -> Result<WebResponse<LifecycleView>, WebError> {
    lifecycle_view(&state).await.map(WebResponse::ok)
}

pub async fn deployment(
    state: web::Data<AppState>,
) -> Result<WebResponse<DeploymentConfigView>, WebError> {
    let deploy = &state.deploy;
    let artifact_store = &deploy.research.artifact_store;
    let artifact_address = artifact_store.endpoint.clone().unwrap_or_else(|| {
        if artifact_store.bucket.is_empty() {
            artifact_store.prefix.clone()
        } else {
            format!("s3://{}/{}", artifact_store.bucket, artifact_store.prefix)
        }
    });
    let endpoints = vec![
        DeploymentEndpointView {
            kind: DeploymentEndpointKind::WebBind,
            address: format!("{}:{}", deploy.web.listen_host, deploy.web.listen_port),
        },
        DeploymentEndpointView {
            kind: DeploymentEndpointKind::Postgres,
            address: format!(
                "{}:{}/{}",
                deploy.db.postgres.host, deploy.db.postgres.port, deploy.db.postgres.database
            ),
        },
        DeploymentEndpointView {
            kind: DeploymentEndpointKind::Clickhouse,
            address: format!(
                "{}/{}",
                deploy.db.clickhouse.url, deploy.db.clickhouse.database
            ),
        },
        DeploymentEndpointView {
            kind: DeploymentEndpointKind::Redis,
            address: deploy.cache.redis.endpoint(),
        },
        DeploymentEndpointView {
            kind: DeploymentEndpointKind::GammaApi,
            address: deploy.market_data.gamma.base_url.clone(),
        },
        DeploymentEndpointView {
            kind: DeploymentEndpointKind::ClobApi,
            address: deploy.polymarket.clob_base_url.clone(),
        },
        DeploymentEndpointView {
            kind: DeploymentEndpointKind::DataApi,
            address: deploy.market_data.data_api.base_url.clone(),
        },
        DeploymentEndpointView {
            kind: DeploymentEndpointKind::ArtifactStore,
            address: artifact_address,
        },
        DeploymentEndpointView {
            kind: DeploymentEndpointKind::DomainProvider,
            address: deploy.domain_sources.binance.rest_url.clone(),
        },
    ];
    Ok(WebResponse::ok(DeploymentConfigView {
        environment: deploy.lifecycle.environment.clone(),
        restart_required: true,
        snapshot: DeploymentConfigSnapshotView {
            endpoints,
            identity: DeploymentIdentityView {
                deployment_id: deploy.db.clickhouse.deployment_id.clone(),
                instance_id: deploy.db.clickhouse.cluster_id.clone(),
            },
            resource_budgets: deployment_resource_budgets(deploy),
        },
        credential_health: deployment_credential_health(deploy),
    }))
}

fn deployment_resource_budgets(deploy: &DeployConfig) -> Vec<DeploymentResourceBudgetView> {
    let postgres = &deploy.db.postgres;
    let clickhouse = &deploy.db.clickhouse;
    let research = &deploy.quant.research_jobs;
    let reports = &deploy.quant.workers;
    vec![
        DeploymentResourceBudgetView {
            kind: ResourceBudgetKind::Database,
            limits: vec![
                resource_limit(
                    ResourceBudgetMetric::MaxConcurrency,
                    u64::from(postgres.max_connections),
                    ResourceBudgetUnit::Count,
                ),
                resource_limit(
                    ResourceBudgetMetric::MinConcurrency,
                    u64::from(postgres.min_connections),
                    ResourceBudgetUnit::Count,
                ),
                resource_limit(
                    ResourceBudgetMetric::OperationTimeout,
                    postgres.acquire_timeout_secs.saturating_mul(1_000),
                    ResourceBudgetUnit::Milliseconds,
                ),
            ],
        },
        DeploymentResourceBudgetView {
            kind: ResourceBudgetKind::ClickhouseWriter,
            limits: vec![
                resource_limit(
                    ResourceBudgetMetric::MaxConcurrency,
                    usize_to_u64(clickhouse.max_concurrent_inserts),
                    ResourceBudgetUnit::Count,
                ),
                resource_limit(
                    ResourceBudgetMetric::BatchRows,
                    usize_to_u64(clickhouse.batch_size),
                    ResourceBudgetUnit::Rows,
                ),
                resource_limit(
                    ResourceBudgetMetric::OperationTimeout,
                    clickhouse.flush_interval_secs,
                    ResourceBudgetUnit::Seconds,
                ),
            ],
        },
        DeploymentResourceBudgetView {
            kind: ResourceBudgetKind::MarketDataIngest,
            limits: vec![
                resource_limit(
                    ResourceBudgetMetric::SubscriptionCapacity,
                    usize_to_u64(deploy.market_data.websocket.engine_max_subscription_tokens),
                    ResourceBudgetUnit::Tokens,
                ),
                resource_limit(
                    ResourceBudgetMetric::BatchRows,
                    usize_to_u64(deploy.domain_sources.binance.batch_size),
                    ResourceBudgetUnit::Rows,
                ),
            ],
        },
        DeploymentResourceBudgetView {
            kind: ResourceBudgetKind::Cache,
            limits: vec![
                resource_limit(
                    ResourceBudgetMetric::MaxConcurrency,
                    u64::from(deploy.cache.redis.pool_size),
                    ResourceBudgetUnit::Count,
                ),
                resource_limit(
                    ResourceBudgetMetric::CacheEntries,
                    deploy.cache.moka.max_capacity,
                    ResourceBudgetUnit::Entries,
                ),
                resource_limit(
                    ResourceBudgetMetric::OperationTimeout,
                    deploy.cache.operation_timeout_ms,
                    ResourceBudgetUnit::Milliseconds,
                ),
            ],
        },
        DeploymentResourceBudgetView {
            kind: ResourceBudgetKind::ResearchJobs,
            limits: vec![
                resource_limit(
                    ResourceBudgetMetric::MaxConcurrency,
                    usize_to_u64(research.global_concurrency),
                    ResourceBudgetUnit::Count,
                ),
                resource_limit(
                    ResourceBudgetMetric::LeaseDuration,
                    u64::try_from(research.lease_ttl_secs).unwrap_or_default(),
                    ResourceBudgetUnit::Seconds,
                ),
                resource_limit(
                    ResourceBudgetMetric::HeartbeatInterval,
                    research.heartbeat_secs,
                    ResourceBudgetUnit::Seconds,
                ),
            ],
        },
        DeploymentResourceBudgetView {
            kind: ResourceBudgetKind::ReportExecution,
            limits: vec![
                resource_limit(
                    ResourceBudgetMetric::QueueCapacity,
                    reports.report_ad_hoc_queue_capacity,
                    ResourceBudgetUnit::Count,
                ),
                resource_limit(
                    ResourceBudgetMetric::LeaseDuration,
                    reports.report_run_lease_secs,
                    ResourceBudgetUnit::Seconds,
                ),
                resource_limit(
                    ResourceBudgetMetric::HeartbeatInterval,
                    reports.report_run_heartbeat_secs,
                    ResourceBudgetUnit::Seconds,
                ),
            ],
        },
        DeploymentResourceBudgetView {
            kind: ResourceBudgetKind::Web,
            limits: vec![resource_limit(
                ResourceBudgetMetric::ConfiguredOrigins,
                usize_to_u64(deploy.web.cors_allowed_origins.len()),
                ResourceBudgetUnit::Count,
            )],
        },
    ]
}

const fn resource_limit(
    metric: ResourceBudgetMetric,
    value: u64,
    unit: ResourceBudgetUnit,
) -> DeploymentResourceLimitView {
    DeploymentResourceLimitView {
        metric,
        value,
        unit,
    }
}

fn deployment_credential_health(deploy: &DeployConfig) -> Vec<CredentialHealthView> {
    vec![
        credential_health(
            CredentialKind::PostgresRuntime,
            !deploy.db.postgres.password.is_empty(),
        ),
        credential_health(
            CredentialKind::ClickhouseRuntime,
            !deploy.db.clickhouse.password.is_empty(),
        ),
        credential_health(
            CredentialKind::RedisRuntime,
            !deploy.cache.redis.password.is_empty(),
        ),
        credential_health(
            CredentialKind::JwtSigning,
            deploy.web.jwt_signing_key_is_configured(),
        ),
        credential_health(
            CredentialKind::PolymarketPrivateKey,
            deploy.keys.private_key_present(),
        ),
        systemd_credential_health(
            CredentialKind::TelegramBotToken,
            &deploy.notifications.telegram.bot_token_credential,
            "notifications.telegram.bot_token_credential",
        ),
        systemd_credential_health(
            CredentialKind::WebhookAuthorization,
            &deploy.notifications.webhook.authorization_credential,
            "notifications.webhook.authorization_credential",
        ),
        credential_health(
            CredentialKind::EvidenceAttestation,
            !deploy.research.evidence_attestation.signing_key.is_empty(),
        ),
        credential_health(
            CredentialKind::PolymarketRelayer,
            deploy
                .polymarket
                .relayer
                .api_key
                .as_ref()
                .is_some_and(|credential| !credential.is_empty()),
        ),
        credential_health(
            CredentialKind::ChainlinkDataStreamsApiKey,
            deploy
                .domain_sources
                .chainlink_data_streams
                .api_key
                .as_ref()
                .is_some_and(|credential| !credential.is_empty()),
        ),
        credential_health(
            CredentialKind::ChainlinkDataStreamsApiSecret,
            deploy
                .domain_sources
                .chainlink_data_streams
                .api_secret
                .as_ref()
                .is_some_and(|credential| !credential.is_empty()),
        ),
    ]
}

const fn credential_health(credential: CredentialKind, configured: bool) -> CredentialHealthView {
    CredentialHealthView {
        credential,
        status: if configured {
            CredentialHealthStatus::Available
        } else {
            CredentialHealthStatus::NotConfigured
        },
    }
}

fn systemd_credential_health(
    credential: CredentialKind,
    reference: &SystemdCredentialRef,
    field: &str,
) -> CredentialHealthView {
    let status = if reference.name.trim().is_empty() {
        CredentialHealthStatus::NotConfigured
    } else if reference.resolve_optional(field).is_ok() {
        CredentialHealthStatus::Available
    } else {
        CredentialHealthStatus::Missing
    };
    CredentialHealthView { credential, status }
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

pub async fn seal_production(
    state: web::Data<AppState>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<SealProductionRequest>,
) -> Result<WebResponse<LifecycleView>, WebError> {
    let body = body.into_inner();
    if state
        .runtime_config
        .load_production_baseline()
        .await?
        .is_some()
    {
        return Err(WebError::Conflict(
            "the production baseline is already sealed and cannot be replaced".to_owned(),
        ));
    }
    if body.environment != state.deploy.lifecycle.environment {
        return Err(WebError::BadRequest(
            "request environment does not match the deployment environment".to_owned(),
        ));
    }
    let expected_phrase = production_seal_confirmation(&body.environment)?;
    if body.confirmation_phrase != expected_phrase {
        return Err(WebError::BadRequest(
            "production seal confirmation phrase does not match".to_owned(),
        ));
    }
    let source = ProjectLifecyclePolicy::compiled()?;
    if source.state != ProjectLifecycleState::PreProductionResettable
        || state.deploy.lifecycle.expected_state != ProjectLifecycleState::PreProductionResettable
    {
        return Err(WebError::Conflict(
            "production sealing is only allowed from pre_production_resettable".to_owned(),
        ));
    }
    let build_commit = state.build_identity.build_commit.clone();
    let active_bundle = state
        .runtime_config
        .load_current_bundle()
        .await?
        .ok_or_else(|| WebError::Conflict("no active policy bundle exists".to_owned()))?;
    let (evidence, live_schema) = production_seal_evidence(&state, Some(&active_bundle)).await?;
    let policy_bundle_hash = active_bundle.snapshot_hash.clone();
    if evidence
        .checks
        .iter()
        .any(|check| check.outcome != CheckOutcome::Passed)
    {
        return Err(WebError::Conflict(
            "all production seal preflight checks must pass".to_owned(),
        ));
    }
    let frozen_policy = ProjectLifecyclePolicy {
        state: ProjectLifecycleState::ProductionFrozen,
        baseline: LifecycleBaseline::Boot,
    };
    let lifecycle_policy_hash = frozen_policy.content_hash()?;
    let production_baseline_id = ProductionBaselineId::boot();
    let now = Utc::now();
    let baseline = state
        .runtime_config
        .seal_production_baseline(
            NewProductionBaseline {
                production_baseline_id: production_baseline_id.clone(),
                environment: body.environment.clone(),
                sealed_at: now,
                sealed_by_kind: PolicyActorKind::Operator,
                sealed_by_user_id: Some(actor_user_id(&actor)?),
                sealed_by_label: actor.claims.username.clone(),
                build_commit: build_commit.clone(),
                postgres_schema_fingerprint: live_schema.postgres_schema_fingerprint,
                clickhouse_schema_fingerprint: live_schema.clickhouse_schema_fingerprint,
                policy_bundle_generation: active_bundle.generation,
                decision_policy_snapshot_id: active_bundle.decision_policy_snapshot_id.clone(),
                policy_bundle_hash: policy_bundle_hash.clone(),
                lifecycle_policy_hash: lifecycle_policy_hash.clone(),
                evidence,
            },
            state.schema_verification.as_ref(),
            state.production_evidence_verification.as_ref(),
        )
        .await?;

    op_ctx.set_action(
        OperationCategory::Governance,
        ConfigAuditAction::ProductionSealed.as_str(),
    );
    op_ctx.set_resource(
        ResourceType::ConfigLifecycle,
        production_baseline_id.to_string(),
    );
    op_ctx.set_state_hashes(None, Some(lifecycle_policy_hash.clone()));
    set_audit_detail(
        &op_ctx,
        &ProductionSealAuditDetail {
            production_baseline_id: &production_baseline_id,
            environment: body.environment.as_str(),
            build_commit: build_commit.as_str(),
            policy_bundle_hash: &policy_bundle_hash,
            lifecycle_policy_hash: &lifecycle_policy_hash,
            reason: &body.reason,
            acting_role: &acting_role.0,
            request_id: &request_id.0,
        },
    )?;

    let mut view = lifecycle_view(&state).await?;
    view.production_baseline = Some(baseline.into());
    Ok(WebResponse::ok(view))
}

async fn lifecycle_view(state: &AppState) -> Result<LifecycleView, WebError> {
    let production_baseline = state.runtime_config.load_production_baseline().await?;
    let state_value = if production_baseline.is_some() {
        ProjectLifecycleState::ProductionFrozen
    } else {
        ProjectLifecycleState::PreProductionResettable
    };
    let active_bundle = state.runtime_config.load_current_bundle().await?;
    let (evidence, live_schema) = production_seal_evidence(state, active_bundle.as_ref()).await?;
    let checks = evidence
        .checks
        .iter()
        .map(|check| LifecycleCheckView {
            kind: check.kind,
            outcome: check.outcome,
            detail: check.detail.clone(),
        })
        .collect();
    let required_confirmation_phrase =
        if state_value == ProjectLifecycleState::PreProductionResettable {
            Some(production_seal_confirmation(
                &state.deploy.lifecycle.environment,
            )?)
        } else {
            None
        };
    Ok(LifecycleView {
        state: state_value,
        baseline: LifecycleBaseline::Boot,
        environment: state.deploy.lifecycle.environment.clone(),
        build_commit: Some(state.build_identity.build_commit.clone()),
        postgres_schema_fingerprint: Some(live_schema.postgres_schema_fingerprint),
        clickhouse_schema_fingerprint: Some(live_schema.clickhouse_schema_fingerprint),
        active_policy_bundle_hash: active_bundle.map(|bundle| bundle.snapshot_hash),
        checks,
        production_baseline: production_baseline.map(Into::into),
        required_confirmation_phrase,
    })
}

async fn production_seal_evidence(
    state: &AppState,
    active_bundle: Option<&ActivePolicyBundle>,
) -> Result<(ProductionSealEvidence, VerifiedSchemaFingerprints), WebError> {
    let now = Utc::now();
    let (live_schema, backup_evidence, config_e2e_evidence) = tokio::try_join!(
        async {
            state
                .schema_verification
                .verify_live()
                .await
                .map_err(WebError::from)
        },
        async {
            state
                .runtime_config
                .load_latest_production_evidence(ProductionEvidenceKind::BackupRestore)
                .await
                .map_err(WebError::from)
        },
        async {
            state
                .runtime_config
                .load_latest_production_evidence(ProductionEvidenceKind::ProtectedConfigEndToEnd)
                .await
                .map_err(WebError::from)
        },
    )?;
    let mut checks = vec![
        ProductionSealCheck {
            kind: LifecycleCheckKind::LifecycleContract,
            outcome: CheckOutcome::Passed,
            checked_at: now,
            detail: LifecycleCheckDetail::ContractMatched,
        },
        ProductionSealCheck {
            kind: LifecycleCheckKind::PostgresSchemaFingerprint,
            outcome: CheckOutcome::Passed,
            checked_at: now,
            detail: LifecycleCheckDetail::SchemaFingerprint {
                fingerprint: live_schema.postgres_schema_fingerprint.clone(),
            },
        },
        ProductionSealCheck {
            kind: LifecycleCheckKind::ClickhouseSchemaFingerprint,
            outcome: CheckOutcome::Passed,
            checked_at: now,
            detail: LifecycleCheckDetail::SchemaFingerprint {
                fingerprint: live_schema.clickhouse_schema_fingerprint.clone(),
            },
        },
        ProductionSealCheck {
            kind: LifecycleCheckKind::MigrationState,
            outcome: CheckOutcome::Passed,
            checked_at: now,
            detail: LifecycleCheckDetail::MigrationLedgersVerified,
        },
        ProductionSealCheck {
            kind: LifecycleCheckKind::CompiledBuildIdentity,
            outcome: if state.build_identity.clean {
                CheckOutcome::Passed
            } else {
                CheckOutcome::Failed
            },
            checked_at: now,
            detail: LifecycleCheckDetail::CompiledBuildIdentity {
                build_commit: state.build_identity.build_commit.clone(),
                clean: state.build_identity.clean,
            },
        },
        ProductionSealCheck {
            kind: LifecycleCheckKind::ActivePolicyBundle,
            outcome: if active_bundle.is_some() {
                CheckOutcome::Passed
            } else {
                CheckOutcome::Failed
            },
            checked_at: now,
            detail: active_bundle.map_or(
                LifecycleCheckDetail::MissingActivePolicyBundle,
                |bundle| LifecycleCheckDetail::PolicyBundle {
                    policy_bundle_hash: bundle.snapshot_hash.clone(),
                },
            ),
        },
    ];
    let (backup_artifact_valid, config_e2e_artifact_valid) = tokio::join!(
        production_evidence_artifact_is_valid(state, backup_evidence.as_ref()),
        production_evidence_artifact_is_valid(state, config_e2e_evidence.as_ref()),
    );
    checks.push(production_evidence_check(
        LifecycleCheckKind::BackupEvidence,
        backup_evidence.as_ref(),
        state,
        &live_schema,
        active_bundle,
        backup_artifact_valid,
        now,
    ));
    checks.push(production_evidence_check(
        LifecycleCheckKind::ConfigEndToEnd,
        config_e2e_evidence.as_ref(),
        state,
        &live_schema,
        active_bundle,
        config_e2e_artifact_valid,
        now,
    ));
    Ok((
        ProductionSealEvidence {
            checks,
            backup_evidence_hash: backup_evidence.map(|evidence| evidence.evidence_hash),
            config_e2e_evidence_hash: config_e2e_evidence.map(|evidence| evidence.evidence_hash),
        },
        live_schema,
    ))
}

fn production_evidence_check(
    kind: LifecycleCheckKind,
    evidence: Option<&ProductionEvidenceInfo>,
    state: &AppState,
    live_schema: &VerifiedSchemaFingerprints,
    active_bundle: Option<&ActivePolicyBundle>,
    artifact_valid: bool,
    checked_at: chrono::DateTime<Utc>,
) -> ProductionSealCheck {
    let matches_current_state = artifact_valid
        && evidence
            .zip(active_bundle)
            .is_some_and(|(evidence, bundle)| {
                let policy_bundle_generation = bundle.generation;
                evidence.build_commit == state.build_identity.build_commit
                    && evidence.postgres_schema_fingerprint
                        == live_schema.postgres_schema_fingerprint
                    && evidence.clickhouse_schema_fingerprint
                        == live_schema.clickhouse_schema_fingerprint
                    && evidence.policy_bundle_generation == policy_bundle_generation
                    && evidence.decision_policy_snapshot_id == bundle.decision_policy_snapshot_id
                    && evidence.policy_bundle_hash == bundle.snapshot_hash
            });
    ProductionSealCheck {
        kind,
        outcome: if matches_current_state {
            CheckOutcome::Passed
        } else {
            CheckOutcome::Failed
        },
        checked_at,
        detail: LifecycleCheckDetail::ExternalEvidence {
            evidence_hash: evidence.map(|evidence| evidence.evidence_hash.clone()),
        },
    }
}

async fn production_evidence_artifact_is_valid(
    state: &AppState,
    evidence: Option<&ProductionEvidenceInfo>,
) -> bool {
    let Some(evidence) = evidence else {
        return false;
    };
    state
        .production_evidence_verification
        .verify_artifact(&evidence.artifact_uri, &evidence.evidence_hash)
        .await
        .is_ok()
}

fn production_seal_confirmation(
    environment: &DeploymentEnvironment,
) -> Result<ProductionSealConfirmationPhrase, WebError> {
    ProductionSealConfirmationPhrase::parse(format!("SEAL {} AS PRODUCTION", environment.as_str()))
        .map_err(|error| WebError::Internal(error.to_string()))
}

struct PolicyTransitionContext<'a> {
    actor: &'a AuthedActor,
    acting_role: &'a ActingRole,
    request_id: &'a RequestId,
    op_ctx: &'a OperationCtx,
    activation_kind: PolicyActivationKind,
}

async fn transition_revision(
    state: &AppState,
    (kind, revision_id): (ConfigResourceKind, PolicyRevisionId),
    request: ActivatePolicyDraftRequest,
    context: PolicyTransitionContext<'_>,
) -> Result<PolicyActivationResultView, WebError> {
    let PolicyTransitionContext {
        actor,
        acting_role,
        request_id,
        op_ctx,
        activation_kind,
    } = context;
    let revision = load_exact_revision(state, kind, &revision_id).await?;
    if revision.status != PolicyRevisionStatus::Validated {
        return Err(WebError::BadRequest(
            "only a validated revision can be activated".to_owned(),
        ));
    }
    let current_bundle = state.runtime_config.load_current_bundle().await?;
    let current = current_bundle
        .as_ref()
        .map_or_else(DecisionPolicySnapshot::default, |bundle| {
            bundle.snapshot.clone()
        });
    let before_hash = policy_document_hash(&current.resource_document(kind))?;
    let mut candidate = current;
    candidate
        .replace_resource_document(kind, revision.document.clone())
        .map_err(|error| WebError::BadRequest(error.to_string()))?;
    candidate.set_resource_revision_id(kind, revision_id.clone());
    ensure_runtime_valid(&candidate)?;
    let prepared = state
        .runtime_config_apply
        .prepare(candidate.clone())
        .await?;
    let next_generation = request
        .expected_bundle_generation
        .checked_next()
        .map_err(|error| WebError::BadRequest(error.to_string()))?;
    let snapshot = new_snapshot(
        &candidate,
        actor,
        &request.reason,
        next_generation,
        match activation_kind {
            PolicyActivationKind::Rollback => DecisionPolicySnapshotSource::Rollback,
            PolicyActivationKind::Initial | PolicyActivationKind::Promote => {
                DecisionPolicySnapshotSource::Activation
            }
        },
    )?;
    let snapshot_id = snapshot.decision_policy_snapshot_id.clone();
    let preflight_token_hash = preflight_token_hash(&request.preflight_token)?;
    let actor_user_id = actor_user_id(actor)?;
    let activation_request_hash = CanonicalDigest::content_hash_typed(
        POLICY_ACTIVATION_HASH_DOMAIN,
        1,
        &PolicyActivationDigest {
            resource_kind: kind,
            policy_revision_id: &revision_id,
            policy_approval_id: &request.approval_id,
            expected_bundle_generation: request.expected_bundle_generation,
            expected_active_revision_id: request.expected_active_revision_id.as_ref(),
            candidate_snapshot_hash: &request.candidate_bundle_hash,
            preflight_token_hash: &preflight_token_hash,
            idempotency_key: &request.idempotency_key,
            activation_kind,
            actor_user_id: &actor_user_id,
            actor_label: &actor.claims.username,
            reason: &request.reason,
        },
    )
    .map_err(|error| WebError::Internal(error.to_string()))?;
    let audit_event_id = AuditEventId::from_v7();
    let commit = state
        .runtime_config
        .activate_resource(
            NewPolicyActivation {
                bundle_generation: next_generation,
                expected_bundle_generation: request.expected_bundle_generation,
                policy_activation_id: PolicyActivationId::from_v7(),
                resource_kind: kind,
                policy_revision_id: revision_id.clone(),
                decision_policy_snapshot_id: snapshot_id,
                policy_approval_id: request.approval_id,
                activated_by_kind: PolicyActorKind::Operator,
                activated_by_user_id: Some(actor_user_id),
                activated_by_label: actor.claims.username.clone(),
                reason: request.reason,
                activation_kind,
                expected_active_revision_id: request.expected_active_revision_id,
                previous_policy_revision_id: None,
                rollback_target_revision_id: (activation_kind == PolicyActivationKind::Rollback)
                    .then(|| revision_id.clone()),
                preflight_token_hash,
                idempotency_key: request.idempotency_key,
                activation_request_hash,
                audit_event_id: audit_event_id.clone(),
            },
            snapshot,
        )
        .await?;
    publish_activation_bundle(state, prepared, &commit).await?;

    let action = if activation_kind == PolicyActivationKind::Rollback {
        ConfigAuditAction::RevisionRolledBack
    } else {
        ConfigAuditAction::RevisionActivated
    };
    op_ctx.set_action(OperationCategory::DecisionPolicySnapshot, action.as_str());
    op_ctx.set_resource(
        ResourceType::DecisionPolicySnapshot,
        revision_id.to_string(),
    );
    op_ctx.set_state_hashes(Some(before_hash), Some(revision.revision_hash.clone()));
    set_audit_detail(
        op_ctx,
        &ConfigAuditDetail {
            resource_kind: kind,
            policy_revision_id: &revision_id,
            policy_approval_id: Some(&commit.activation.policy_approval_id),
            policy_activation_id: Some(&commit.activation.policy_activation_id),
            activation_kind: Some(activation_kind),
            acting_role: &acting_role.0,
            request_id: &request_id.0,
        },
    )?;
    op_ctx.link_governance(
        commit.activation.audit_event_id.clone(),
        commit.bundle.generation.get(),
    );
    state.events.publish(CoreEvent::ConfigActivated {
        version_id: commit.activation.decision_policy_snapshot_id.to_string(),
    });
    Ok(PolicyActivationResultView {
        activation: commit.activation.into(),
        applied_revision: revision.into(),
        activation_kind,
        outcome: commit.outcome,
        committed_generation: commit.bundle.generation,
        committed_snapshot_id: commit.bundle.decision_policy_snapshot_id,
        committed_snapshot_hash: commit.bundle.snapshot_hash,
        committed_revision_vector: commit.bundle.revision_vector,
    })
}

async fn publish_activation_bundle(
    state: &AppState,
    prepared: PreparedPolicySnapshot,
    commit: &PolicyActivationCommit,
) -> Result<(), WebError> {
    match commit.outcome {
        PolicyActivationOutcome::Committed => prepared.publish_bundle(commit.bundle.clone())?,
        PolicyActivationOutcome::ExactReplay => {
            if state
                .runtime_config
                .load_current_bundle()
                .await?
                .is_some_and(|bundle| bundle.generation == commit.bundle.generation)
            {
                state
                    .runtime_config_apply
                    .prepare(commit.bundle.snapshot.clone())
                    .await?
                    .publish_bundle(commit.bundle.clone())?;
            }
        }
    }
    Ok(())
}

async fn load_exact_revision(
    state: &AppState,
    kind: ConfigResourceKind,
    revision_id: &PolicyRevisionId,
) -> Result<PolicyRevisionInfo, WebError> {
    let revision = state
        .runtime_config
        .load_revision(revision_id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("policy revision not found: {revision_id}")))?;
    if revision.resource_kind != kind || revision.document.kind() != kind {
        return Err(WebError::NotFound(format!(
            "policy revision {revision_id} does not belong to {kind}"
        )));
    }
    Ok(revision)
}

async fn validate_and_prepare(
    state: &AppState,
    candidate: DecisionPolicySnapshot,
    subject: PolicyValidationSubject,
) -> (PolicyValidationEvidence, Option<PreparedPolicySnapshot>) {
    let report = validate_runtime_config(&candidate);
    let mut evidence = validation_evidence(&report);
    evidence.subject = Some(subject);
    if report.has_errors() {
        evidence.preflight.push(PolicyPreflightResult {
            check: PolicyPreflightCheckKind::ConsumerPreparation,
            outcome: CheckOutcome::NotApplicable,
            detail_code: PolicyPreflightDetailCode::ConsumerPreparationSkipped,
            failure_detail: None,
        });
        return (evidence, None);
    }
    match state.runtime_config_apply.prepare(candidate).await {
        Ok(prepared) => {
            evidence.preflight.push(PolicyPreflightResult {
                check: PolicyPreflightCheckKind::ConsumerPreparation,
                outcome: CheckOutcome::Passed,
                detail_code: PolicyPreflightDetailCode::ConsumerPreparationPassed,
                failure_detail: None,
            });
            (evidence, Some(prepared))
        }
        Err(error) => {
            evidence.issues.push(PolicyValidationIssue {
                severity: PolicyValidationSeverity::Error,
                code: PolicyValidationCode::DependencyUnavailable,
                path: "consumers".to_owned(),
                message: error.to_string(),
            });
            evidence.preflight.push(PolicyPreflightResult {
                check: PolicyPreflightCheckKind::ConsumerPreparation,
                outcome: CheckOutcome::Failed,
                detail_code: PolicyPreflightDetailCode::ConsumerPreparationFailed,
                failure_detail: Some(error.to_string()),
            });
            (evidence, None)
        }
    }
}

fn validation_evidence(report: &ConfigValidationReport) -> PolicyValidationEvidence {
    let mut issues = report
        .errors
        .iter()
        .map(|error| PolicyValidationIssue {
            severity: PolicyValidationSeverity::Error,
            code: validation_error_code(error),
            path: validation_error_path(error),
            message: error.to_string(),
        })
        .collect::<Vec<_>>();
    issues.extend(report.warnings.iter().map(|warning| PolicyValidationIssue {
        severity: PolicyValidationSeverity::Warning,
        code: PolicyValidationCode::SemanticConstraint,
        path: validation_warning_path(warning).to_owned(),
        message: warning.to_string(),
    }));
    PolicyValidationEvidence {
        subject: None,
        issues,
        preflight: vec![
            PolicyPreflightResult {
                check: PolicyPreflightCheckKind::TypedSchema,
                outcome: CheckOutcome::Passed,
                detail_code: PolicyPreflightDetailCode::TypedDocumentDecoded,
                failure_detail: None,
            },
            PolicyPreflightResult {
                check: PolicyPreflightCheckKind::SemanticValidation,
                outcome: if report.has_errors() {
                    CheckOutcome::Failed
                } else {
                    CheckOutcome::Passed
                },
                detail_code: if report.has_errors() {
                    PolicyPreflightDetailCode::SemanticValidationFailed
                } else {
                    PolicyPreflightDetailCode::SemanticValidationPassed
                },
                failure_detail: None,
            },
        ],
    }
}

const fn validation_error_code(error: &ConfigValidationError) -> PolicyValidationCode {
    if matches!(error, ConfigValidationError::MissingCredentials { .. }) {
        PolicyValidationCode::CredentialUnavailable
    } else {
        PolicyValidationCode::SemanticConstraint
    }
}

fn validation_error_path(error: &ConfigValidationError) -> String {
    match error {
        ConfigValidationError::InfeasibleRange { field_low, .. } => (*field_low).to_owned(),
        ConfigValidationError::InvalidKellyFraction(_) => "portfolio.kelly_fraction".to_owned(),
        ConfigValidationError::MissingCredentials { .. } => "credentials".to_owned(),
        ConfigValidationError::InvalidValue { field, .. } => (*field).to_owned(),
    }
}

const fn validation_warning_path(warning: &ConfigWarning) -> &'static str {
    match warning {
        ConfigWarning::LargeKellyFraction(_) => "portfolio.kelly_fraction",
        ConfigWarning::JwtSigningKeyUnconfigured => "web.jwt.signing_key",
    }
}

fn ensure_document_contract(
    kind: ConfigResourceKind,
    document: &PolicyDocument,
) -> Result<(), WebError> {
    if document.kind() != kind {
        return Err(WebError::BadRequest(format!(
            "document kind {} does not match path kind {kind}",
            document.kind()
        )));
    }
    if document.schema_version() != POLICY_RESOURCE_SCHEMA_VERSION {
        return Err(WebError::BadRequest(format!(
            "policy schema_version must equal {POLICY_RESOURCE_SCHEMA_VERSION}"
        )));
    }
    Ok(())
}

fn ensure_runtime_valid(candidate: &DecisionPolicySnapshot) -> Result<(), WebError> {
    let report = validate_runtime_config(candidate);
    if report.has_errors() {
        Err(WebError::BadRequest(report.to_string()))
    } else {
        Ok(())
    }
}

fn new_snapshot(
    snapshot: &DecisionPolicySnapshot,
    actor: &AuthedActor,
    reason: &str,
    bundle_generation: PolicyBundleGeneration,
    source: DecisionPolicySnapshotSource,
) -> Result<NewDecisionPolicySnapshot, WebError> {
    let revisions = &snapshot.revisions;
    let missing = || {
        WebError::BadRequest(
            "the active policy bundle is incomplete; bootstrap all six resources first".to_owned(),
        )
    };
    let snapshot_document = snapshot
        .persistence_document()
        .map_err(|error| WebError::Internal(error.to_string()))?;
    Ok(NewDecisionPolicySnapshot {
        bundle_generation,
        decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
        snapshot_hash: CanonicalDigest::content_hash_json(&snapshot_document)
            .map_err(|error| WebError::Internal(error.to_string()))?,
        recommendation_policy_revision_id: revisions
            .recommendation_policy
            .clone()
            .ok_or_else(missing)?,
        execution_risk_policy_revision_id: revisions
            .execution_risk_policy
            .clone()
            .ok_or_else(missing)?,
        model_routing_revision_id: revisions.model_routing.clone().ok_or_else(missing)?,
        report_schedule_revision_id: revisions.report_schedule.clone().ok_or_else(missing)?,
        operational_control_revision_id: revisions
            .operational_control
            .clone()
            .ok_or_else(missing)?,
        execution_authorization_revision_id: revisions
            .execution_authorization
            .clone()
            .ok_or_else(missing)?,
        snapshot: snapshot_document,
        source,
        created_by_kind: PolicyActorKind::Operator,
        created_by_user_id: Some(actor_user_id(actor)?),
        created_by_label: actor.claims.username.clone(),
        reason: reason.to_owned(),
    })
}

fn policy_document_hash(document: &PolicyDocument) -> Result<ContentHash, WebError> {
    CanonicalDigest::content_hash_typed(POLICY_HASH_DOMAIN, 1, document)
        .map_err(|error| WebError::Internal(error.to_string()))
}

fn new_preflight_token() -> Result<PolicyPreflightToken, WebError> {
    PolicyPreflightToken::parse(Uuid::now_v7().simple().to_string())
        .map_err(|error| WebError::Internal(error.to_string()))
}

fn preflight_token_hash(token: &PolicyPreflightToken) -> Result<ContentHash, WebError> {
    ContentHash::parse(CanonicalDigest::prefixed_bytes(token.as_str().as_bytes()))
        .map_err(|error| WebError::Internal(error.to_string()))
}

fn actor_user_id(actor: &AuthedActor) -> Result<UserId, WebError> {
    actor
        .claims
        .sub
        .parse()
        .map_err(|error| WebError::Internal(format!("authenticated subject is invalid: {error}")))
}

fn bounded_limit(limit: Option<u64>) -> u64 {
    limit.unwrap_or(DEFAULT_LIST_LIMIT).min(MAX_LIST_LIMIT)
}

fn set_audit_detail<T: Serialize>(op_ctx: &OperationCtx, detail: &T) -> Result<(), WebError> {
    op_ctx.set_detail(detail)
}
