//! Read-only factor-definition catalog endpoints.

use actix_web::{
    http::Method,
    web::{Data, Path, Query},
};
use quant_pivot_models::{
    domain::{
        api::{
            FactorCollinearityQuery, FactorCollinearityView, FactorDefinitionDetailQuery,
            FactorDefinitionDetailView, FactorDefinitionListQuery, FactorDefinitionView,
        },
        pagination::Paginated,
    },
    enums::rbac::{Operation, ResourceType},
    runtime_config::NeutralizeDimension,
    types::FactorDefinitionId,
};
use rust_decimal::Decimal;

use crate::{
    auth::casbin::Rule,
    error::WebError,
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

/// Read-only factor-definition catalog routes.
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
        spec(
            Method::GET,
            "/research/factors/{id}",
            Rule::ResourceOp(ResourceType::FactorDefinition, Operation::Read),
            get_by_id,
        ),
    ]
}

/// `GET /api/research/factors` — paginated immutable factor-definition catalog.
pub async fn list(
    state: Data<AppState>,
    query: Query<FactorDefinitionListQuery>,
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
/// so the report and training-time contract validation share one threshold; an
/// explicit `threshold` query param overrides it. The `source` param selects
/// the raw (default) or normalized value plane.
pub async fn collinearity(
    state: Data<AppState>,
    query: Query<FactorCollinearityQuery>,
) -> Result<WebResponse<FactorCollinearityView>, WebError> {
    let query = query.into_inner();
    let lookback_secs = query
        .lookback_secs
        .unwrap_or(DEFAULT_COLLINEARITY_LOOKBACK_SECS);
    let threshold = if let Some(raw) = query.threshold {
        raw.trim()
            .parse::<Decimal>()
            .map_err(|error| WebError::BadRequest(format!("invalid threshold `{raw}`: {error}")))?
    } else {
        let config = state.runtime_config_apply.current();
        config
            .profile_artifacts
            .scoring
            .definition
            .orthogonalize
            .max_correlation
            .value
    };
    // Honor the runtime `factors.orthogonalize.neutralize_by` operator.
    let neutralize_by_category = state
        .runtime_config_apply
        .current()
        .profile_artifacts
        .scoring
        .definition
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
    state: Data<AppState>,
    id: Path<FactorDefinitionId>,
    query: Query<FactorDefinitionDetailQuery>,
) -> Result<WebResponse<FactorDefinitionDetailView>, WebError> {
    let id = id.into_inner();
    let view = state
        .research_catalog
        .find_factor_detail(&id, query.into_inner())
        .await?
        .ok_or_else(|| WebError::NotFound(format!("factor_definition not found: {id}")))?;
    Ok(WebResponse::ok(view))
}
