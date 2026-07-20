//! Training/serving feature-integrity evidence and governed recovery endpoints.
//!
//! | Method | Path | Authorization | Semantics |
//! |---|---|---|---|
//! | GET | `/research/feature-integrity/summary` | `materialization:read` | Latch and coverage roll-up |
//! | GET | `/research/feature-integrity/runs` | `materialization:read` | Durable replay-run ledger |
//! | GET | `/research/feature-integrity/events` | `materialization:read` | Stage-level comparison facts |
//! | POST | `/research/feature-integrity/runs/full` | governed `materialization:create` | Enqueue a full replay |
//! | POST | `/research/feature-integrity/latch/acknowledge` | governed `materialization:create` | Clear the latch with verified evidence |

use actix_web::{http::Method, web};
use quant_pivot_models::{
    domain::{
        AcknowledgeFeatureParityLatchRequest, FeatureIntegrityActionContext,
        FeatureIntegrityLatchView, FeatureIntegritySummaryView, FeatureParityEventListQuery,
        FeatureParityEventView, FeatureParityRunListQuery, FeatureParityRunView, Paginated,
        ResearchJobView, RunFullFeatureParityRequest,
    },
    enums::{
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
    },
};

use crate::{
    audit::OperationCtx,
    auth::casbin::Rule,
    error::WebError,
    extractors::{ActingRole, AuthedActor, RequestId, ValidatedJson},
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::GET,
            "/research/feature-integrity/summary",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            summary,
        ),
        spec(
            Method::GET,
            "/research/feature-integrity/runs",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            list_runs,
        ),
        spec(
            Method::GET,
            "/research/feature-integrity/events",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            list_events,
        ),
        spec(
            Method::POST,
            "/research/feature-integrity/runs/full",
            Rule::ActingRoleGoverned(ResourceType::Materialization, Operation::Create),
            run_full,
        ),
        spec(
            Method::POST,
            "/research/feature-integrity/latch/acknowledge",
            Rule::ActingRoleGoverned(ResourceType::Materialization, Operation::Create),
            acknowledge_latch,
        ),
    ]
}

pub async fn summary(
    state: web::Data<AppState>,
) -> Result<WebResponse<FeatureIntegritySummaryView>, WebError> {
    Ok(WebResponse::ok(state.feature_integrity.summary().await?))
}

pub async fn list_runs(
    state: web::Data<AppState>,
    query: web::Query<FeatureParityRunListQuery>,
) -> Result<WebResponse<Paginated<FeatureParityRunView>>, WebError> {
    Ok(WebResponse::ok(
        state
            .feature_integrity
            .list_runs(query.into_inner())
            .await?,
    ))
}

pub async fn list_events(
    state: web::Data<AppState>,
    query: web::Query<FeatureParityEventListQuery>,
) -> Result<WebResponse<Paginated<FeatureParityEventView>>, WebError> {
    Ok(WebResponse::ok(
        state
            .feature_integrity
            .list_events(query.into_inner())
            .await?,
    ))
}

pub async fn run_full(
    state: web::Data<AppState>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<RunFullFeatureParityRequest>,
) -> Result<WebResponse<ResearchJobView>, WebError> {
    let request = body.into_inner();
    let reason = request.reason.clone();
    let job = state
        .feature_integrity
        .request_full_run(
            request,
            FeatureIntegrityActionContext {
                actor: Some(actor.claims.sub),
                acting_role: acting_role.0.clone(),
            },
        )
        .await?;
    op_ctx.set_action(OperationCategory::Other, "research.feature_parity.full");
    op_ctx.set_resource(ResourceType::Materialization, job.job_id.to_string());
    op_ctx.set_detail(serde_json::json!({
        "job_id": job.job_id.to_string(),
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "reason": reason,
    }))?;
    Ok(WebResponse::accepted(job))
}

pub async fn acknowledge_latch(
    state: web::Data<AppState>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<AcknowledgeFeatureParityLatchRequest>,
) -> Result<WebResponse<FeatureIntegrityLatchView>, WebError> {
    let request = body.into_inner();
    let parity_run_id = request.parity_run_id.clone();
    let reason = request.reason.clone();
    let latch = state
        .feature_integrity
        .acknowledge_latch(
            request,
            FeatureIntegrityActionContext {
                actor: Some(actor.claims.sub),
                acting_role: acting_role.0.clone(),
            },
        )
        .await?;
    op_ctx.set_action(
        OperationCategory::Other,
        "research.feature_parity.latch_acknowledge",
    );
    op_ctx.set_resource(ResourceType::Materialization, parity_run_id.to_string());
    op_ctx.set_detail(serde_json::json!({
        "parity_run_id": parity_run_id.to_string(),
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "reason": reason,
    }))?;
    Ok(WebResponse::ok(latch))
}

#[cfg(test)]
mod tests {
    use actix_web::http::Method;
    use quant_pivot_models::enums::rbac::{Operation, ResourceType};

    use super::route_specs;
    use crate::auth::casbin::Rule;

    #[test]
    fn route_manifest_is_complete_and_fail_closed_by_operation() {
        let specs = route_specs();
        let expected = [
            (Method::GET, "/research/feature-integrity/summary", false),
            (Method::GET, "/research/feature-integrity/runs", false),
            (Method::GET, "/research/feature-integrity/events", false),
            (Method::POST, "/research/feature-integrity/runs/full", true),
            (
                Method::POST,
                "/research/feature-integrity/latch/acknowledge",
                true,
            ),
        ];

        assert_eq!(specs.len(), expected.len());
        for (method, path, governed) in expected {
            let Some(spec) = specs
                .iter()
                .find(|spec| spec.method == method && spec.path == path)
            else {
                panic!("missing feature-integrity route {method} {path}");
            };
            match (&spec.rule, governed) {
                (Rule::ResourceOp(ResourceType::Materialization, Operation::Read), false)
                | (
                    Rule::ActingRoleGoverned(ResourceType::Materialization, Operation::Create),
                    true,
                ) => {}
                (rule, _) => {
                    panic!("feature-integrity route {method} {path} has unexpected rule {rule:?}")
                }
            }
        }
    }
}
