//! Domain-source ingest cursor health endpoints (Phase 11.2.2).
//!
//! | Method | Path | Permission | Purpose |
//! |--------|------|------------|---------|
//! | GET | `/research/domain-sources` | `materialization:read` | All ingest cursors + lag |

use std::collections::HashMap;

use actix_web::{http::Method, web};
use chrono::Utc;
use quant_pivot_models::{
    domain::DomainSourceExpectationView,
    enums::rbac::{Operation, ResourceType},
    types::{DomainInstrumentKey, DomainSourceId},
};

use crate::{
    auth::casbin::Rule,
    error::WebError,
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

/// Domain-source cursor health routes.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![spec(
        Method::GET,
        "/research/domain-sources",
        Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
        list,
    )]
}

/// `GET /api/research/domain-sources` — ingest checkpoint health for every
/// `(source, instrument)` stream.
pub async fn list(
    state: web::Data<AppState>,
) -> Result<WebResponse<Vec<DomainSourceExpectationView>>, WebError> {
    let (expectations, cursors) = tokio::try_join!(
        state.domain_source_expectations.list_all(),
        state.domain_source_cursors.list_all(),
    )?;
    let mut cursors = cursors
        .into_iter()
        .map(|cursor| {
            (
                (cursor.source_id.clone(), cursor.instrument_key.clone()),
                cursor,
            )
        })
        .collect::<HashMap<(DomainSourceId, DomainInstrumentKey), _>>();
    let observed_at = Utc::now();
    let rows = expectations
        .into_iter()
        .map(|expectation| {
            let cursor = cursors.remove(&(
                expectation.source_id.clone(),
                expectation.instrument_key.clone(),
            ));
            DomainSourceExpectationView::from_expected_and_observed(
                expectation,
                cursor,
                observed_at,
            )
        })
        .collect();
    Ok(WebResponse::ok(rows))
}
