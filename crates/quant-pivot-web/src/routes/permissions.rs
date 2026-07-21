//! Permission catalog endpoint (RBAC `Permission` resource).
//!
//! Exposes the static `resource × operation` matrix that bounds every role
//! permission assignment, so clients can render an accurate, always-valid
//! permission picker.

use actix_web::http::Method;
use quant_pivot_models::{
    domain::api::PermissionCatalogEntry,
    enums::rbac::{Operation, RESOURCE_OPERATIONS, ResourceType},
};

use crate::{
    auth::casbin::Rule,
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
};

/// Permission catalog route.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![spec(
        Method::GET,
        "/permissions/catalog",
        Rule::ResourceOp(ResourceType::Permission, Operation::Read),
        catalog,
    )]
}

/// `GET /api/permissions/catalog` — the full assignable permission matrix.
pub async fn catalog() -> WebResponse<Vec<PermissionCatalogEntry>> {
    let catalog = RESOURCE_OPERATIONS
        .iter()
        .map(|(resource, operations)| PermissionCatalogEntry {
            resource: *resource,
            operations: operations.to_vec(),
        })
        .collect();
    WebResponse::ok(catalog)
}
