//! Governed, independently revisioned Config resources.
//!
//! This is a clean-break API. A policy document is always strongly typed, a
//! draft must be validated and dependency-preflighted before approval, and an
//! activation must bind the exact approval, expected active revision,
//! short-lived preflight proof, and idempotency key.

use actix_web::{
    http::Method,
    web::{Data, Path, Query},
};
use chrono::{Duration, Utc};
use quant_pivot_error::config_validation::{
    ConfigValidationError, ConfigValidationReport, ConfigWarning,
};
use quant_pivot_models::{
    config::DeployConfig,
    domain::{
        api::{
            ActivatePolicyDraftRequest, ApprovePolicyDraftRequest, ConfigActivityQuery,
            ConfigActivityView, ConfigResourceSummaryView, ConfigResourcesView,
            ConfigSnapshotOptionsQuery, CreatePolicyDraftRequest, CredentialHealthView,
            CurrentPolicyResourceView, DecisionPolicySnapshotOptionView,
            DeploymentConfigSnapshotView, DeploymentConfigView, DeploymentEndpointView,
            DeploymentIdentityView, DeploymentResourceBudgetView, DeploymentResourceLimitView,
            PolicyActivationResultView, PolicyApprovalView, PolicyResourceSchemaView,
            PolicyRevisionListQuery, PolicyRevisionView, PolicyValidationView,
            SchedulePreviewRequest, SchedulePreviewView, ValidatePolicyDraftRequest,
        },
        governance::{
            NewDecisionPolicySnapshot, NewPolicyActivation, NewPolicyRevision,
            PolicyActivationCommit, PolicyActivationOutcome, PolicyRevisionInfo,
            RecordPolicyApproval,
        },
        ports::PreparedPolicySnapshot,
        runtime::CoreEvent,
    },
    enums::{
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
        runtime_config::{
            CheckOutcome, ConfigAuditAction, ConfigResourceKind, CredentialHealthStatus,
            CredentialKind, DecisionPolicySnapshotSource, DeploymentEndpointKind,
            PolicyActivationKind, PolicyActorKind, PolicyPreflightCheckKind,
            PolicyPreflightDetailCode, PolicyRevisionStatus, PolicyValidationCode,
            PolicyValidationSeverity, ResourceBudgetKind, ResourceBudgetMetric, ResourceBudgetUnit,
        },
    },
    hashing::CanonicalDigest,
    runtime_config::{
        DecisionPolicySnapshot, POLICY_RESOURCE_SCHEMA_VERSION, PolicyDocument,
        PolicyPreflightResult, PolicyRevisionBundle, PolicyValidationEvidence,
        PolicyValidationIssue, PolicyValidationSubject, preview_fire_times,
    },
    types::{
        AuditEventId, ContentHash, DecisionPolicySnapshotId, PolicyActivationId, PolicyApprovalId,
        PolicyBundleGeneration, PolicyIdempotencyKey, PolicyPreflightToken, PolicyRevisionId,
        UserId,
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
            Method::POST,
            "/config/schedule-preview",
            Rule::ResourceOp(ResourceType::DecisionPolicySnapshot, Operation::Read),
            schedule_preview,
        ),
    ]
}

pub async fn resources(
    state: Data<AppState>,
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
    state: Data<AppState>,
    kind: Path<ConfigResourceKind>,
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
    kind: Path<ConfigResourceKind>,
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
    state: Data<AppState>,
    kind: Path<ConfigResourceKind>,
    query: Query<PolicyRevisionListQuery>,
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
    state: Data<AppState>,
    kind: Path<ConfigResourceKind>,
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
            revision_hash,
            document: body.document,
            status: PolicyRevisionStatus::Draft,
            validation_evidence: None,
            validated_at: None,
            preflight_token_hash: None,
            preflight_expires_at: None,
            created_by_kind: PolicyActorKind::Operator,
            created_by_user_id: Some(actor.user_id().map_err(|error| {
                WebError::Internal(format!("authenticated subject is invalid: {error}"))
            })?),
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
    op_ctx.set_detail(&ConfigAuditDetail {
        resource_kind: kind,
        policy_revision_id: &revision.policy_revision_id,
        policy_approval_id: None,
        policy_activation_id: None,
        activation_kind: None,
        acting_role: &acting_role.0,
        request_id: &request_id.0,
    })?;
    Ok(WebResponse::ok(revision.into()))
}

pub async fn validate_draft(
    state: Data<AppState>,
    path: Path<(ConfigResourceKind, PolicyRevisionId)>,
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
    candidate.set_resource_revision_id(kind, revision_id);

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
        let token_hash = preflight_token_hash(&token);
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
    op_ctx.set_detail(&ConfigAuditDetail {
        resource_kind: kind,
        policy_revision_id: &revision_id,
        policy_approval_id: None,
        policy_activation_id: None,
        activation_kind: None,
        acting_role: &acting_role.0,
        request_id: &request_id.0,
    })?;
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
    state: Data<AppState>,
    path: Path<(ConfigResourceKind, PolicyRevisionId)>,
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
            policy_revision_id: revision_id,
            resource_kind: kind,
            decision: body.decision,
            decided_by_kind: PolicyActorKind::Operator,
            decided_by_user_id: Some(actor.user_id().map_err(|error| {
                WebError::Internal(format!("authenticated subject is invalid: {error}"))
            })?),
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
    op_ctx.set_state_hashes(None, Some(approval.revision_hash));
    op_ctx.set_detail(&ConfigAuditDetail {
        resource_kind: kind,
        policy_revision_id: &revision_id,
        policy_approval_id: Some(&approval.policy_approval_id),
        policy_activation_id: None,
        activation_kind: None,
        acting_role: &acting_role.0,
        request_id: &request_id.0,
    })?;
    Ok(WebResponse::ok(approval.into()))
}

pub async fn activate_draft(
    state: Data<AppState>,
    path: Path<(ConfigResourceKind, PolicyRevisionId)>,
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
    state: Data<AppState>,
    path: Path<(ConfigResourceKind, PolicyRevisionId)>,
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
    state: Data<AppState>,
    query: Query<ConfigActivityQuery>,
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
    state: Data<AppState>,
    query: Query<ConfigSnapshotOptionsQuery>,
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

pub async fn deployment(
    state: Data<AppState>,
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
        environment: deploy.deployment.environment.clone(),
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
        credential_health(CredentialKind::JwtSigning, deploy.web.has_jwt_signing_key()),
        credential_health(
            CredentialKind::PolymarketPrivateKey,
            deploy.keys.private_key_present(),
        ),
        credential_health(
            CredentialKind::TelegramBotToken,
            !deploy.notifications.telegram.bot_token.is_empty(),
        ),
        credential_health(
            CredentialKind::WebhookAuthorization,
            !deploy.notifications.webhook.authorization.is_empty(),
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

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
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
    candidate.set_resource_revision_id(kind, revision_id);
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
    let snapshot_id = snapshot.decision_policy_snapshot_id;
    let preflight_token_hash = preflight_token_hash(&request.preflight_token);
    let actor_user_id = actor.user_id().map_err(|error| {
        WebError::Internal(format!("authenticated subject is invalid: {error}"))
    })?;
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
                policy_revision_id: revision_id,
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
                    .then_some(revision_id),
                preflight_token_hash,
                idempotency_key: request.idempotency_key,
                activation_request_hash,
                audit_event_id,
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
    op_ctx.set_state_hashes(Some(before_hash), Some(revision.revision_hash));
    op_ctx.set_detail(&ConfigAuditDetail {
        resource_kind: kind,
        policy_revision_id: &revision_id,
        policy_approval_id: Some(&commit.activation.policy_approval_id),
        policy_activation_id: Some(&commit.activation.policy_activation_id),
        activation_kind: Some(activation_kind),
        acting_role: &acting_role.0,
        request_id: &request_id.0,
    })?;
    op_ctx.link_governance(
        commit.activation.audit_event_id,
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
    let report = candidate.validate_runtime_config();
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
    let report = candidate.validate_runtime_config();
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
        recommendation_policy_revision_id: revisions.recommendation_policy.ok_or_else(missing)?,
        execution_risk_policy_revision_id: revisions.execution_risk_policy.ok_or_else(missing)?,
        model_routing_revision_id: revisions.model_routing.ok_or_else(missing)?,
        report_schedule_revision_id: revisions.report_schedule.ok_or_else(missing)?,
        operational_control_revision_id: revisions.operational_control.ok_or_else(missing)?,
        execution_authorization_revision_id: revisions
            .execution_authorization
            .ok_or_else(missing)?,
        snapshot: snapshot_document,
        source,
        created_by_kind: PolicyActorKind::Operator,
        created_by_user_id: Some(actor.user_id().map_err(|error| {
            WebError::Internal(format!("authenticated subject is invalid: {error}"))
        })?),
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

fn preflight_token_hash(token: &PolicyPreflightToken) -> ContentHash {
    CanonicalDigest::content_hash_bytes(token.as_str().as_bytes())
}

fn bounded_limit(limit: Option<u64>) -> u64 {
    limit.unwrap_or(DEFAULT_LIST_LIMIT).min(MAX_LIST_LIMIT)
}
