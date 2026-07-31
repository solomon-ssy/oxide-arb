//! Read-only model evidence endpoints.

use actix_web::{
    http::Method,
    web::{Data, Path, Query},
};
use quant_pivot_models::{
    domain::api::{ModelCalibrationFitPreflightQuery, ModelCalibrationFitPreflightView},
    enums::rbac::{Operation, ResourceType},
    types::ModelVersionId,
};

use crate::{
    auth::casbin::Rule,
    error::WebError,
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

/// Model-evidence routes.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![spec(
        Method::GET,
        "/research/models/{id}/calibration-fit-preflight",
        Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
        calibration_fit_preflight,
    )]
}

/// Read-only disjoint + embargo check for the calibrator-fit wizard.
pub async fn calibration_fit_preflight(
    state: Data<AppState>,
    id: Path<ModelVersionId>,
    query: Query<ModelCalibrationFitPreflightQuery>,
) -> Result<WebResponse<ModelCalibrationFitPreflightView>, WebError> {
    let view = state
        .model_calibration_fit
        .preflight(&id.into_inner(), &query.into_inner().calibration_dataset_id)
        .await?;
    Ok(WebResponse::ok(view))
}
