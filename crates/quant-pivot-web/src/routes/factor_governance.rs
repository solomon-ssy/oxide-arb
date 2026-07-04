//! Factor-definition publish / retire admin endpoints (Phase 05.7).

use actix_web::{http::Method, web};
use quant_pivot_models::{
    domain::{
        FactorCollinearityQuery, FactorCollinearityView, FactorDefinitionListQuery,
        FactorDefinitionView, GovernanceActor, Paginated, PublishFactorCommand,
        PublishFactorRequest, PublishFactorsBatchCommand, PublishFactorsBatchRequest,
        RegisterFactorDefinitionsCommand, RegisterFactorDefinitionsRequest, RetireFactorCommand,
        RetireFactorRequest,
    },
    enums::{
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
    },
    hashing::CanonicalDigest,
    runtime_config::NeutralizeDimension,
    types::FactorDefinitionId,
};
use serde::Serialize;

use crate::{
    audit::OperationCtx,
    auth::casbin::Rule,
    error::WebError,
    extractors::{ActingRole, AuthedActor, RequestId, ValidatedJson},
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

/// Factor-governance routes (publish / retire).
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::GET,
            "/research/factors",
            Rule::ResourceOp(ResourceType::FactorDefinition, Operation::Read),
            list,
        ),
        // Registered before `{id}` so the literal path is not captured as an id.
        spec(
            Method::GET,
            "/research/factors/collinearity",
            Rule::ResourceOp(ResourceType::FactorDefinition, Operation::Read),
            collinearity,
        ),
        // Literal paths registered before `{id}` so they are not captured as ids.
        spec(
            Method::POST,
            "/research/factors/register",
            Rule::ActingRoleGoverned(ResourceType::FactorDefinition, Operation::Create),
            register,
        ),
        spec(
            Method::POST,
            "/research/factors/publish-batch",
            Rule::ActingRoleGoverned(ResourceType::FactorDefinition, Operation::Publish),
            publish_batch,
        ),
        spec(
            Method::GET,
            "/research/factors/{id}",
            Rule::ResourceOp(ResourceType::FactorDefinition, Operation::Read),
            get_by_id,
        ),
        spec(
            Method::POST,
            "/research/factors/{id}/publish",
            Rule::ActingRoleGoverned(ResourceType::FactorDefinition, Operation::Publish),
            publish,
        ),
        spec(
            Method::POST,
            "/research/factors/{id}/retire",
            Rule::ActingRoleGoverned(ResourceType::FactorDefinition, Operation::Retire),
            retire,
        ),
    ]
}

/// `GET /api/research/factors` — paginated factor-definition governance catalog.
pub async fn list(
    state: web::Data<AppState>,
    query: web::Query<FactorDefinitionListQuery>,
) -> Result<WebResponse<Paginated<FactorDefinitionView>>, WebError> {
    let page = state
        .research_catalog
        .list_factors(query.into_inner())
        .await?
        .map(FactorDefinitionView::from);
    Ok(WebResponse::ok(page))
}

/// Default collinearity lookback: seven days of factor values.
const DEFAULT_COLLINEARITY_LOOKBACK_SECS: u64 = 7 * 24 * 60 * 60;

/// `GET /api/research/factors/collinearity` — Spearman collinearity report.
///
/// The tolerance defaults to the **active** `factors.orthogonalize.max_correlation`
/// so the report and the (future) publish gate share one threshold; an explicit
/// `threshold` query param overrides it. The `source` param selects the raw
/// (default) or normalized value plane.
pub async fn collinearity(
    state: web::Data<AppState>,
    query: web::Query<FactorCollinearityQuery>,
) -> Result<WebResponse<FactorCollinearityView>, WebError> {
    let query = query.into_inner();
    let lookback_secs = query
        .lookback_secs
        .unwrap_or(DEFAULT_COLLINEARITY_LOOKBACK_SECS);
    let threshold = if let Some(raw) = query.threshold {
        raw.trim()
            .parse::<rust_decimal::Decimal>()
            .map_err(|error| WebError::BadRequest(format!("invalid threshold `{raw}`: {error}")))?
    } else {
        let config = state.runtime_config_apply.current();
        config
            .factors
            .orthogonalize
            .max_correlation
            .value
            .trim()
            .parse::<rust_decimal::Decimal>()
            .map_err(|error| {
                WebError::BadRequest(format!(
                    "runtime factors.orthogonalize.max_correlation is invalid: {error}"
                ))
            })?
    };
    // Honor the runtime `factors.orthogonalize.neutralize_by` operator.
    let neutralize_by_category = state
        .runtime_config_apply
        .current()
        .factors
        .orthogonalize
        .neutralize_by
        .iter()
        .any(|dimension| matches!(dimension, NeutralizeDimension::Category));
    let source = query.source.unwrap_or_default();
    let report = state
        .research_catalog
        .factor_collinearity(lookback_secs, threshold, source, neutralize_by_category)
        .await?;
    Ok(WebResponse::ok(report))
}

/// `GET /api/research/factors/{id}` — single factor definition (detail drawer).
pub async fn get_by_id(
    state: web::Data<AppState>,
    id: web::Path<FactorDefinitionId>,
) -> Result<WebResponse<FactorDefinitionView>, WebError> {
    let info = state
        .research_catalog
        .find_factor(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("factor_definition not found: {id}")))?;
    Ok(WebResponse::ok(FactorDefinitionView::from(info)))
}

/// `POST /api/research/factors/{id}/publish` — promote a draft/retired definition.
pub async fn publish(
    state: web::Data<AppState>,
    id: web::Path<FactorDefinitionId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<PublishFactorRequest>,
) -> Result<WebResponse<FactorDefinitionView>, WebError> {
    let request = body.into_inner();
    let factor_definition_id = id.into_inner();
    let before = state
        .factor_governance
        .find_definition(&factor_definition_id)
        .await?
        .ok_or_else(|| {
            WebError::NotFound(format!(
                "factor_definition not found: {factor_definition_id}"
            ))
        })?;
    let published = state
        .factor_governance
        .publish(
            PublishFactorCommand {
                factor_definition_id: factor_definition_id.clone(),
                reason: request.reason.clone(),
            },
            governance_actor(&actor, &acting_role),
        )
        .await?;
    let before_hash = canonical_state_hash(&before)?;
    let after_hash = canonical_state_hash(&published)?;
    let view = FactorDefinitionView::from(published);
    op_ctx.set_action(OperationCategory::Governance, "factor.publish");
    op_ctx.set_resource(
        ResourceType::FactorDefinition,
        factor_definition_id.to_string(),
    );
    op_ctx.set_state_hashes(Some(before_hash), Some(after_hash));
    op_ctx.set_detail(serde_json::json!({
        "factor_definition_id": view.factor_definition_id,
        "status": view.status,
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "reason": request.reason,
    }));
    Ok(WebResponse::ok(view))
}

/// `POST /api/research/factors/{id}/retire` — retire a published definition.
pub async fn retire(
    state: web::Data<AppState>,
    id: web::Path<FactorDefinitionId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<RetireFactorRequest>,
) -> Result<WebResponse<FactorDefinitionView>, WebError> {
    let request = body.into_inner();
    let factor_definition_id = id.into_inner();
    let before = state
        .factor_governance
        .find_definition(&factor_definition_id)
        .await?
        .ok_or_else(|| {
            WebError::NotFound(format!(
                "factor_definition not found: {factor_definition_id}"
            ))
        })?;
    let retired = state
        .factor_governance
        .retire(
            RetireFactorCommand {
                factor_definition_id: factor_definition_id.clone(),
                reason: request.reason.clone(),
            },
            governance_actor(&actor, &acting_role),
        )
        .await?;
    let before_hash = canonical_state_hash(&before)?;
    let after_hash = canonical_state_hash(&retired)?;
    let view = FactorDefinitionView::from(retired);
    op_ctx.set_action(OperationCategory::Governance, "factor.retire");
    op_ctx.set_resource(
        ResourceType::FactorDefinition,
        factor_definition_id.to_string(),
    );
    op_ctx.set_state_hashes(Some(before_hash), Some(after_hash));
    op_ctx.set_detail(serde_json::json!({
        "factor_definition_id": view.factor_definition_id,
        "status": view.status,
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "reason": request.reason,
    }));
    Ok(WebResponse::ok(view))
}

/// `POST /api/research/factors/register` — register the enabled factor set.
///
/// Idempotent upsert of every enabled factor definition as `Draft` (preserving
/// the status of any already-registered definition). This is the explicit
/// bootstrap step that seeds the factor catalog so the operator can then publish
/// them — the online report path fails closed on non-`Published` definitions.
pub async fn register(
    state: web::Data<AppState>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<RegisterFactorDefinitionsRequest>,
) -> Result<WebResponse<Vec<FactorDefinitionView>>, WebError> {
    let request = body.into_inner();
    let config = state.runtime_config_apply.current();
    let registered = state
        .factor_governance
        .register_enabled_definitions(
            RegisterFactorDefinitionsCommand {
                factors: config.factors.clone(),
                features: config.features.clone(),
                reason: request.reason.clone(),
            },
            governance_actor(&actor, &acting_role),
        )
        .await?;
    let views: Vec<FactorDefinitionView> = registered
        .into_iter()
        .map(FactorDefinitionView::from)
        .collect();
    op_ctx.set_action(OperationCategory::Governance, "factor.register");
    op_ctx.set_resource(ResourceType::FactorDefinition, "*".to_owned());
    op_ctx.set_detail(serde_json::json!({
        "registered_count": views.len(),
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "reason": request.reason,
    }));
    Ok(WebResponse::ok(views))
}

/// `POST /api/research/factors/publish-batch` — publish a batch of definitions.
pub async fn publish_batch(
    state: web::Data<AppState>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<PublishFactorsBatchRequest>,
) -> Result<WebResponse<Vec<FactorDefinitionView>>, WebError> {
    let request = body.into_inner();
    let published = state
        .factor_governance
        .publish_batch(
            PublishFactorsBatchCommand {
                factor_definition_ids: request.factor_definition_ids.clone(),
                reason: request.reason.clone(),
            },
            governance_actor(&actor, &acting_role),
        )
        .await?;
    let views: Vec<FactorDefinitionView> = published
        .into_iter()
        .map(FactorDefinitionView::from)
        .collect();
    op_ctx.set_action(OperationCategory::Governance, "factor.publish_batch");
    op_ctx.set_resource(ResourceType::FactorDefinition, "*".to_owned());
    op_ctx.set_detail(serde_json::json!({
        "requested_count": request.factor_definition_ids.len(),
        "published_count": views.len(),
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "reason": request.reason,
    }));
    Ok(WebResponse::ok(views))
}

fn governance_actor(actor: &AuthedActor, acting_role: &ActingRole) -> GovernanceActor {
    GovernanceActor {
        username: actor.claims.username.clone(),
        role: Some(acting_role.0.clone()),
    }
}

fn canonical_state_hash<T: Serialize>(state: &T) -> Result<String, WebError> {
    CanonicalDigest::content_hash_json(state)
        .map(|hash| hash.as_str().to_owned())
        .map_err(|error| WebError::Internal(format!("canonical state hash failed: {error}")))
}
