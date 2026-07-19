//! Governed, independently revisioned Config resources.
//!
//! This is a clean-break API. A policy document is always strongly typed, a
//! draft must be validated and dependency-preflighted before approval, and an
//! activation must bind the exact approval, expected active revision,
//! short-lived preflight proof, and idempotency key.

use std::{cmp::Reverse, collections::BTreeMap};

use actix_web::{http::Method, web};
use chrono::{Duration, Utc};
use quant_pivot_error::config_validation::{
    ConfigValidationError, ConfigValidationReport, ConfigWarning,
};
use quant_pivot_models::{
    config::{DeployConfig, ProjectLifecyclePolicy, secret::SystemdCredentialRef},
    domain::{
        ActivatePolicyDraftRequest, ApprovePolicyDraftRequest, ConfigActivityQuery,
        ConfigActivityView, ConfigResourceSummaryView, ConfigResourcesView, CoreEvent,
        CreatePolicyDraftRequest, CredentialHealthView, CurrentPolicyResourceView,
        DeploymentConfigSnapshotView, DeploymentConfigView, DeploymentEndpointView,
        DeploymentIdentityView, DeploymentResourceBudgetView, DeploymentResourceLimitView,
        LifecycleCheckView, LifecycleView, NewDecisionPolicySnapshot, NewPolicyActivation,
        NewPolicyRevision, NewProductionBaseline, PolicyActivationResultView, PolicyApprovalView,
        PolicyResourceSchemaView, PolicyRevisionInfo, PolicyRevisionListQuery, PolicyRevisionView,
        PolicyValidationView, PreparedPolicySnapshot, RecordPolicyApproval, SchedulePreviewRequest,
        SchedulePreviewView, SealProductionRequest, ValidatePolicyDraftRequest,
    },
    enums::{
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
        runtime_config::{
            CheckOutcome, ConfigAuditAction, ConfigResourceKind, CredentialHealthStatus,
            CredentialKind, DecisionPolicySnapshotSource, DeploymentEndpointKind,
            LifecycleBaseline, LifecycleCheckKind, PolicyActivationKind, PolicyActorKind,
            PolicyPreflightCheckKind, PolicyPreflightDetailCode, PolicyRevisionStatus,
            PolicyValidationCode, PolicyValidationSeverity, ProjectLifecycleState,
            ResourceBudgetKind, ResourceBudgetMetric, ResourceBudgetUnit,
        },
    },
    hashing::CanonicalDigest,
    runtime_config::{
        DecisionPolicySnapshot, LifecycleCheckDetail, POLICY_RESOURCE_SCHEMA_VERSION,
        PolicyDocument, PolicyPreflightResult, PolicyValidationEvidence, PolicyValidationIssue,
        ProductionSealCheck, ProductionSealEvidence, preview_fire_times, validate_runtime_config,
    },
    types::{
        ContentHash, DecisionPolicySnapshotId, DeploymentEnvironment, PolicyActivationId,
        PolicyApprovalId, PolicyPreflightToken, PolicyRevisionId, ProductionBaselineId,
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
    let (activations, pending_counts) = tokio::try_join!(
        state.runtime_config.load_current_activations(),
        state.runtime_config.count_valid_approvals(),
    )?;
    let activation_by_kind = activations
        .into_iter()
        .map(|activation| (activation.resource_kind, activation))
        .collect::<BTreeMap<_, _>>();
    let current = state.runtime_config_apply.current();
    let mut summaries = Vec::with_capacity(ConfigResourceKind::ALL.len());
    for kind in ConfigResourceKind::ALL {
        let activation = activation_by_kind.get(&kind);
        let active_revision_hash = activation
            .map(|_| policy_document_hash(&current.resource_document(kind)))
            .transpose()?;
        summaries.push(ConfigResourceSummaryView {
            kind,
            schema_version: POLICY_RESOURCE_SCHEMA_VERSION,
            active_revision_id: activation.map(|row| row.policy_revision_id.clone()),
            active_revision_hash,
            pending_approval_count: pending_counts.get(&kind).copied().unwrap_or(0),
            effective_boundary: kind.apply_boundary(),
            restart_required: false,
            last_activated_at: activation.map(|row| row.activated_at),
        });
    }
    let active_policy_bundle_hash = Some(
        CanonicalDigest::content_hash_json(current.as_ref())
            .map_err(|error| WebError::Internal(error.to_string()))?,
    );
    Ok(WebResponse::ok(ConfigResourcesView {
        resources: summaries,
        active_policy_bundle_hash,
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
    op_ctx.set_state_hashes(None, Some(revision_hash.to_string()));
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
    let mut candidate = state.runtime_config_apply.current().as_ref().clone();
    candidate
        .replace_resource_document(kind, revision.document.clone())
        .map_err(|error| WebError::BadRequest(error.to_string()))?;
    candidate.set_resource_revision_id(kind, revision_id.clone());

    let (evidence, prepared) = validate_and_prepare(&state, candidate).await;
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
    op_ctx.set_state_hashes(None, Some(approval.revision_hash.to_string()));
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
    let (revisions, approvals, activations) = tokio::try_join!(
        state.runtime_config.list_all_revisions(limit),
        state.runtime_config.list_approvals(None, limit),
        state.runtime_config.list_activations(None, limit),
    )?;
    let mut events = Vec::with_capacity(revisions.len() + approvals.len() + activations.len());
    events.extend(
        revisions
            .into_iter()
            .map(|revision| ConfigActivityView::Revision(Box::new(revision.into()))),
    );
    events.extend(
        approvals
            .into_iter()
            .map(|approval| ConfigActivityView::Approval(approval.into())),
    );
    events.extend(
        activations
            .into_iter()
            .map(|activation| ConfigActivityView::Activation(activation.into())),
    );
    events.sort_by_key(|event| Reverse(activity_timestamp(event)));
    events.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
    Ok(WebResponse::ok(events))
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
    let build_commit = state.deploy.lifecycle.build_commit.clone().ok_or_else(|| {
        WebError::Conflict("deployment build_commit is required before production seal".to_owned())
    })?;
    let policy_bundle_hash =
        CanonicalDigest::content_hash_json(state.runtime_config_apply.current().as_ref())
            .map_err(|error| WebError::Internal(error.to_string()))?;
    let evidence = production_seal_evidence(&state, &policy_bundle_hash);
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
        .seal_production_baseline(NewProductionBaseline {
            production_baseline_id: production_baseline_id.clone(),
            environment: body.environment.clone(),
            sealed_at: now,
            sealed_by_kind: PolicyActorKind::Operator,
            sealed_by_user_id: Some(actor_user_id(&actor)?),
            sealed_by_label: actor.claims.username.clone(),
            build_commit: build_commit.clone(),
            postgres_schema_fingerprint: state.postgres_schema_fingerprint.clone(),
            clickhouse_schema_fingerprint: state.clickhouse_schema_fingerprint.clone(),
            policy_bundle_hash: policy_bundle_hash.clone(),
            lifecycle_policy_hash: lifecycle_policy_hash.clone(),
            evidence,
        })
        .await?;

    op_ctx.set_action(
        OperationCategory::Governance,
        ConfigAuditAction::ProductionSealed.as_str(),
    );
    op_ctx.set_resource(
        ResourceType::ConfigLifecycle,
        production_baseline_id.to_string(),
    );
    op_ctx.set_state_hashes(None, Some(lifecycle_policy_hash.to_string()));
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
    let active_policy_bundle_hash =
        CanonicalDigest::content_hash_json(state.runtime_config_apply.current().as_ref())
            .map_err(|error| WebError::Internal(error.to_string()))?;
    let evidence = production_seal_evidence(state, &active_policy_bundle_hash);
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
        build_commit: state.deploy.lifecycle.build_commit.clone(),
        postgres_schema_fingerprint: Some(state.postgres_schema_fingerprint.clone()),
        clickhouse_schema_fingerprint: Some(state.clickhouse_schema_fingerprint.clone()),
        active_policy_bundle_hash: Some(active_policy_bundle_hash),
        checks,
        production_baseline: production_baseline.map(Into::into),
        required_confirmation_phrase,
    })
}

fn production_seal_evidence(
    state: &AppState,
    policy_bundle_hash: &ContentHash,
) -> ProductionSealEvidence {
    let now = Utc::now();
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
                fingerprint: state.postgres_schema_fingerprint.clone(),
            },
        },
        ProductionSealCheck {
            kind: LifecycleCheckKind::ClickhouseSchemaFingerprint,
            outcome: CheckOutcome::Passed,
            checked_at: now,
            detail: LifecycleCheckDetail::SchemaFingerprint {
                fingerprint: state.clickhouse_schema_fingerprint.clone(),
            },
        },
        ProductionSealCheck {
            kind: LifecycleCheckKind::MigrationState,
            outcome: CheckOutcome::Passed,
            checked_at: now,
            detail: LifecycleCheckDetail::MigrationLedgersVerified,
        },
        ProductionSealCheck {
            kind: LifecycleCheckKind::ActivePolicyBundle,
            outcome: CheckOutcome::Passed,
            checked_at: now,
            detail: LifecycleCheckDetail::PolicyBundle {
                policy_bundle_hash: policy_bundle_hash.clone(),
            },
        },
    ];
    checks.push(external_evidence_check(
        LifecycleCheckKind::BackupEvidence,
        state.deploy.lifecycle.backup_evidence_hash.as_ref(),
        now,
    ));
    checks.push(external_evidence_check(
        LifecycleCheckKind::ConfigEndToEnd,
        state.deploy.lifecycle.config_e2e_evidence_hash.as_ref(),
        now,
    ));
    ProductionSealEvidence {
        checks,
        backup_evidence_hash: state.deploy.lifecycle.backup_evidence_hash.clone(),
        config_e2e_evidence_hash: state.deploy.lifecycle.config_e2e_evidence_hash.clone(),
    }
}

fn external_evidence_check(
    kind: LifecycleCheckKind,
    evidence_hash: Option<&ContentHash>,
    checked_at: chrono::DateTime<Utc>,
) -> ProductionSealCheck {
    ProductionSealCheck {
        kind,
        outcome: if evidence_hash.is_some() {
            CheckOutcome::Passed
        } else {
            CheckOutcome::Failed
        },
        checked_at,
        detail: LifecycleCheckDetail::ExternalEvidence {
            evidence_hash: evidence_hash.cloned(),
        },
    }
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
    let current = state.runtime_config_apply.current();
    let before_hash = policy_document_hash(&current.resource_document(kind))?;
    let mut candidate = current.as_ref().clone();
    candidate
        .replace_resource_document(kind, revision.document.clone())
        .map_err(|error| WebError::BadRequest(error.to_string()))?;
    candidate.set_resource_revision_id(kind, revision_id.clone());
    ensure_runtime_valid(&candidate)?;
    let prepared = state
        .runtime_config_apply
        .prepare(candidate.clone())
        .await?;
    let snapshot = new_snapshot(
        candidate,
        actor,
        &request.reason,
        match activation_kind {
            PolicyActivationKind::Rollback => DecisionPolicySnapshotSource::Rollback,
            PolicyActivationKind::Initial | PolicyActivationKind::Promote => {
                DecisionPolicySnapshotSource::Activation
            }
        },
    )?;
    let snapshot_id = snapshot.decision_policy_snapshot_id.clone();
    let activation = state
        .runtime_config
        .activate_resource(
            NewPolicyActivation {
                policy_activation_id: PolicyActivationId::from_v7(),
                resource_kind: kind,
                policy_revision_id: revision_id.clone(),
                decision_policy_snapshot_id: snapshot_id,
                policy_approval_id: request.approval_id,
                activated_by_kind: PolicyActorKind::Operator,
                activated_by_user_id: Some(actor_user_id(actor)?),
                activated_by_label: actor.claims.username.clone(),
                reason: request.reason,
                activation_kind,
                expected_active_revision_id: request.expected_active_revision_id,
                previous_policy_revision_id: None,
                rollback_target_revision_id: (activation_kind == PolicyActivationKind::Rollback)
                    .then(|| revision_id.clone()),
                preflight_token_hash: preflight_token_hash(&request.preflight_token)?,
                idempotency_key: request.idempotency_key,
                audit_event_id: None,
            },
            snapshot,
        )
        .await?;
    prepared.publish();

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
    op_ctx.set_state_hashes(
        Some(before_hash.to_string()),
        Some(revision.revision_hash.to_string()),
    );
    set_audit_detail(
        op_ctx,
        &ConfigAuditDetail {
            resource_kind: kind,
            policy_revision_id: &revision_id,
            policy_approval_id: Some(&activation.policy_approval_id),
            policy_activation_id: Some(&activation.policy_activation_id),
            activation_kind: Some(activation_kind),
            acting_role: &acting_role.0,
            request_id: &request_id.0,
        },
    )?;
    state.events.publish(CoreEvent::ConfigActivated {
        version_id: activation.decision_policy_snapshot_id.to_string(),
    });
    Ok(PolicyActivationResultView {
        activation: activation.into(),
        applied_revision: revision.into(),
        activation_kind,
    })
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
) -> (PolicyValidationEvidence, Option<PreparedPolicySnapshot>) {
    let report = validate_runtime_config(&candidate);
    let mut evidence = validation_evidence(&report);
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
    snapshot: DecisionPolicySnapshot,
    actor: &AuthedActor,
    reason: &str,
    source: DecisionPolicySnapshotSource,
) -> Result<NewDecisionPolicySnapshot, WebError> {
    let revisions = &snapshot.revisions;
    let missing = || {
        WebError::BadRequest(
            "the active policy bundle is incomplete; bootstrap all six resources first".to_owned(),
        )
    };
    Ok(NewDecisionPolicySnapshot {
        decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
        snapshot_hash: CanonicalDigest::content_hash_json(&snapshot)
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
        snapshot,
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

fn activity_timestamp(event: &ConfigActivityView) -> chrono::DateTime<Utc> {
    match event {
        ConfigActivityView::Revision(revision) => revision.created_at,
        ConfigActivityView::Approval(approval) => approval.decided_at,
        ConfigActivityView::Activation(activation) => activation.activated_at,
    }
}

fn set_audit_detail<T: Serialize>(op_ctx: &OperationCtx, detail: &T) -> Result<(), WebError> {
    op_ctx.set_detail(
        serde_json::to_value(detail).map_err(|error| WebError::Internal(error.to_string()))?,
    );
    Ok(())
}
