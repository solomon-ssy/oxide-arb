//! Route registration and the route → authorization manifest.
//!
//! Versioning is header-driven (see [`version::ApiV1Guard`]): every endpoint
//! lives under the unversioned path `/api/...` and is selected for `v1` by the
//! `Accept-Api-Version: v1` header. Liveness/readiness probes and the WebSocket
//! upgrade (`/api/ws`) sit outside the versioned scope — probes for
//! orchestrators, WS because browsers cannot set handshake headers.
//!
//! # Single-source authorization manifest
//!
//! `protected_route_specs` is the *one* declarative list of every protected
//! route: its method, path pattern, authorization rule, and handler. Both
//! [`configure`] (which registers the actix routes) and [`PermChecker::route_rules`]
//! (which builds the [`PermChecker`]) derive from it, so every registered
//! protected route is guaranteed to have a rule **by construction** — there is
//! no way to add a route without also declaring how it is authorized.
//!
//! Each resource module owns its slice of the manifest via `RouteSpec` lists;
//! this module only aggregates them and wires the actix scopes.
//!
//! # Pipeline
//!
//! Public routes (`login`, `refresh`) and probes are registered outside the
//! authorized scope and never reach the checker. Everything else is wrapped by
//! [`authn`] (outer scope) then
//! [`authz`] (inner scope), so identity is always
//! established before authorization, and an unregistered protected route is
//! denied (fail-closed).

pub mod account;
pub mod auth;
pub mod basis_alerts;
pub mod calibration_artifacts;
pub mod dashboard;
pub mod data_quality;
pub mod domain_sources;
pub mod execution_orders;
pub mod factor_catalog;
pub mod feature_integrity;
pub mod feedback;
pub mod health;
pub mod incentives;
pub mod market_linkages;
pub mod markets;
pub mod menus;
pub mod metrics;
pub mod model_governance;
pub mod operation_logs;
pub mod permissions;
pub mod positions;
pub mod quant_intents;
pub mod quant_recommendations;
pub mod quant_reports;
pub mod reconciliations;
pub mod registry;
pub mod research_jobs;
pub mod research_models;
pub mod roles;
pub mod runtime_activities;
pub mod runtime_config;
pub mod settlement_redeems;
pub mod structural_monitor;
pub mod system;
pub mod trade_policies;
pub mod training_datasets;
pub mod users;
pub mod version;
pub mod ws;

use actix_web::{
    Resource,
    middleware::from_fn,
    web::{self, ServiceConfig},
};

use crate::{
    auth::casbin::PermChecker,
    middleware::{authn, authz},
    routes::{
        registry::{API_PREFIX, RouteSpec},
        version::ApiV1Guard,
    },
};

/// The single declarative manifest of every protected route, assembled from the
/// per-resource groups below.
fn protected_route_specs() -> Vec<RouteSpec> {
    let mut specs = Vec::new();
    specs.extend(auth::route_specs());
    specs.extend(ws::route_specs());
    specs.extend(users::route_specs());
    specs.extend(roles::route_specs());
    specs.extend(menus::route_specs());
    specs.extend(permissions::route_specs());
    specs.extend(runtime_config::route_specs());
    specs.extend(runtime_activities::route_specs());
    specs.extend(operation_logs::route_specs());
    specs.extend(system::route_specs());
    specs.extend(markets::route_specs());
    specs.extend(data_quality::route_specs());
    specs.extend(dashboard::route_specs());
    specs.extend(training_datasets::route_specs());
    specs.extend(research_models::route_specs());
    specs.extend(research_jobs::route_specs());
    specs.extend(calibration_artifacts::route_specs());
    specs.extend(trade_policies::route_specs());
    specs.extend(market_linkages::route_specs());
    specs.extend(basis_alerts::route_specs());
    specs.extend(domain_sources::route_specs());
    specs.extend(structural_monitor::route_specs());
    specs.extend(model_governance::route_specs());
    specs.extend(factor_catalog::route_specs());
    specs.extend(feedback::route_specs());
    specs.extend(feature_integrity::route_specs());
    specs.extend(quant_reports::route_specs());
    specs.extend(quant_recommendations::route_specs());
    specs.extend(account::route_specs());
    specs.extend(incentives::route_specs());
    specs.extend(positions::route_specs());
    specs.extend(execution_orders::route_specs());
    specs.extend(reconciliations::route_specs());
    specs.extend(settlement_redeems::route_specs());
    specs.extend(quant_intents::route_specs());
    specs
}

/// Build the route-level [`PermChecker`] from the manifest.
///
/// Keys are `(method, API_PREFIX + path)`, matching the pattern the authz
/// middleware reads from `ServiceRequest::match_pattern`.
impl PermChecker {
    /// Build the route-level authorization registry from the canonical manifest.
    #[must_use]
    pub fn route_rules() -> Self {
        let mut checker = Self::new();
        for spec in protected_route_specs() {
            checker.register(spec.method, format!("{API_PREFIX}{}", spec.path), spec.rule);
        }
        checker
    }
}

/// Group the manifest's routes into one actix [`Resource`] per path (preserving
/// declaration order) so multiple methods on the same path share a resource and
/// resolve by their method guards.
fn protected_resources() -> Vec<Resource> {
    let mut grouped: Vec<(&'static str, Vec<_>)> = Vec::new();
    for spec in protected_route_specs() {
        if let Some(entry) = grouped.iter_mut().find(|(path, _)| *path == spec.path) {
            entry.1.push(spec.route);
        } else {
            grouped.push((spec.path, vec![spec.route]));
        }
    }
    grouped
        .into_iter()
        .map(|(path, routes)| {
            routes
                .into_iter()
                .fold(web::resource(path), Resource::route)
        })
        .collect()
}

/// Register all routes onto the service config.
///
/// The protected routes live in two nested scopes so the middleware order is
/// unambiguous: the outer scope's `authn` runs first (establishing identity),
/// then the inner scope's `authz` authorizes against the manifest. Wrapped
/// scopes have an un-nameable type, so the whole tree is built inline.
pub fn configure(cfg: &mut ServiceConfig) {
    let authorized = protected_resources()
        .into_iter()
        .fold(web::scope("").wrap(from_fn(authz)), |scope, resource| {
            scope.service(resource)
        });
    let protected = web::scope("").wrap(from_fn(authn)).service(authorized);

    cfg.route("/health", web::get().to(health::health))
        .route("/startup", web::get().to(health::startup))
        .route("/ready", web::get().to(health::ready))
        .route("/metrics", web::get().to(metrics::metrics))
        .service(
            web::scope(API_PREFIX)
                // WebSocket upgrade consumes a short-lived single-use ticket.
                // It deliberately sits outside `ApiV1Guard` (browsers cannot set
                // `Accept-Api-Version` on the handshake) and outside Bearer `authn`
                // (self-authenticates in the handler).
                .configure(ws::configure)
                .service(
                    web::scope("")
                        .guard(ApiV1Guard)
                        .route("/auth/login", web::post().to(auth::login))
                        .route("/auth/refresh", web::post().to(auth::refresh))
                        .service(protected),
                ),
        );
}

#[cfg(test)]
mod tests {
    use actix_web::http::Method;
    use quant_pivot_models::enums::rbac::{Operation, ResourceType};

    use super::protected_route_specs;
    use crate::auth::casbin::Rule;

    #[test]
    fn feature_integrity_routes_manifest() {
        let paths = protected_route_specs()
            .into_iter()
            .filter_map(|spec| {
                spec.path
                    .starts_with("/research/feature-integrity/")
                    .then_some(spec.path)
            })
            .collect::<Vec<_>>();

        assert_eq!(
            paths,
            [
                "/research/feature-integrity/summary",
                "/research/feature-integrity/runs",
                "/research/feature-integrity/events",
                "/research/feature-integrity/runs/full",
                "/research/feature-integrity/latch/acknowledge",
            ]
        );
    }

    #[test]
    fn factor_routes_read_only() {
        let routes = protected_route_specs()
            .into_iter()
            .filter(|spec| spec.path.starts_with("/research/factors"))
            .map(|spec| (spec.method, spec.path, spec.rule))
            .collect::<Vec<_>>();

        assert_eq!(
            routes,
            [
                (
                    Method::GET,
                    "/research/factors",
                    Rule::ResourceOp(ResourceType::FactorDefinition, Operation::Read),
                ),
                (
                    Method::GET,
                    "/research/factors/collinearity",
                    Rule::ResourceOp(ResourceType::FactorDefinition, Operation::Read),
                ),
                (
                    Method::GET,
                    "/research/factors/{id}",
                    Rule::ResourceOp(ResourceType::FactorDefinition, Operation::Read),
                ),
            ]
        );
    }

    #[test]
    fn feedback_read_routes_manifest() {
        let routes = protected_route_specs()
            .into_iter()
            .filter(|spec| {
                matches!(
                    spec.path,
                    "/research/feedback-overview"
                        | "/research/feedback-cycles"
                        | "/research/feedback-cycles/{cycle_id}"
                        | "/research/drift-reports"
                        | "/research/feedback-schedulers"
                ) && spec.method == Method::GET
            })
            .map(|spec| (spec.method, spec.path, spec.rule))
            .collect::<Vec<_>>();

        assert_eq!(
            routes,
            [
                (
                    Method::GET,
                    "/research/feedback-overview",
                    Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
                ),
                (
                    Method::GET,
                    "/research/feedback-cycles",
                    Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
                ),
                (
                    Method::GET,
                    "/research/feedback-cycles/{cycle_id}",
                    Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
                ),
                (
                    Method::GET,
                    "/research/drift-reports",
                    Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
                ),
                (
                    Method::GET,
                    "/research/feedback-schedulers",
                    Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
                ),
            ]
        );
    }

    #[test]
    fn feedback_mutation_routes_manifest() {
        let routes = protected_route_specs()
            .into_iter()
            .filter(|spec| {
                matches!(
                    spec.path,
                    "/research/feedback-cycles"
                        | "/research/feedback-cycles/{cycle_id}/cancel"
                        | "/research/feedback-schedulers/{profile_id}/pause"
                        | "/research/feedback-schedulers/{profile_id}/resume"
                        | "/research/model-route-activation-permits"
                        | "/research/model-route-activation-permits/{permit_id}/revoke"
                        | "/research/model-route-bootstraps"
                        | "/research/model-route-shadow-bindings/{binding_id}/reject"
                        | "/research/model-route-activations"
                ) && spec.method == Method::POST
            })
            .map(|spec| (spec.method, spec.path, spec.rule))
            .collect::<Vec<_>>();

        assert_eq!(
            routes,
            [
                (
                    Method::POST,
                    "/research/feedback-cycles",
                    Rule::ActingRoleGoverned(ResourceType::Materialization, Operation::Create),
                ),
                (
                    Method::POST,
                    "/research/feedback-cycles/{cycle_id}/cancel",
                    Rule::ActingRoleGoverned(ResourceType::Materialization, Operation::Create),
                ),
                (
                    Method::POST,
                    "/research/feedback-schedulers/{profile_id}/pause",
                    Rule::ActingRoleGoverned(ResourceType::Materialization, Operation::Update),
                ),
                (
                    Method::POST,
                    "/research/feedback-schedulers/{profile_id}/resume",
                    Rule::ActingRoleGoverned(ResourceType::Materialization, Operation::Update),
                ),
                (
                    Method::POST,
                    "/research/model-route-activation-permits",
                    Rule::ActingRoleGoverned(ResourceType::Publication, Operation::Authorize),
                ),
                (
                    Method::POST,
                    "/research/model-route-activation-permits/{permit_id}/revoke",
                    Rule::ActingRoleGoverned(ResourceType::Publication, Operation::Retire),
                ),
                (
                    Method::POST,
                    "/research/model-route-bootstraps",
                    Rule::ActingRoleGoverned(ResourceType::Publication, Operation::Publish),
                ),
                (
                    Method::POST,
                    "/research/model-route-activations",
                    Rule::ActingRoleGoverned(ResourceType::Publication, Operation::Activate),
                ),
                (
                    Method::POST,
                    "/research/model-route-shadow-bindings/{binding_id}/reject",
                    Rule::ActingRoleGoverned(ResourceType::Publication, Operation::Reject),
                ),
            ]
        );
    }

    #[test]
    fn feedback_permit_read_manifest() {
        let routes = protected_route_specs()
            .into_iter()
            .filter(|spec| {
                spec.path == "/research/model-route-activation-permits"
                    && spec.method == Method::GET
            })
            .map(|spec| (spec.method, spec.path, spec.rule))
            .collect::<Vec<_>>();

        assert_eq!(
            routes,
            [(
                Method::GET,
                "/research/model-route-activation-permits",
                Rule::ResourceOp(ResourceType::Publication, Operation::Read),
            )]
        );
    }

    #[test]
    fn settlement_money_actions_only() {
        let routes = protected_route_specs()
            .into_iter()
            .filter(|spec| spec.path.starts_with("/quant/settlement-"))
            .map(|spec| (spec.method, spec.path, spec.rule))
            .collect::<Vec<_>>();

        assert_eq!(routes.len(), 12);
        for (method, path, rule) in routes {
            match (method, path, rule) {
                (
                    Method::POST,
                    "/quant/settlement-redeems/{id}/approve",
                    Rule::ActingRoleGoverned(ResourceType::SettlementRedeem, Operation::Approve),
                )
                | (
                    Method::POST,
                    "/quant/settlement-redeems/{id}/revoke-approval"
                    | "/quant/settlement-governed-actions/{id}/revoke",
                    Rule::ActingRoleGoverned(ResourceType::SettlementRedeem, Operation::Revoke),
                )
                | (
                    Method::POST,
                    "/quant/settlement-operator-approvals/apply"
                    | "/quant/settlement-canaries/apply",
                    Rule::ActingRoleGoverned(ResourceType::SettlementRedeem, Operation::Create),
                )
                | (
                    Method::GET | Method::POST,
                    _,
                    Rule::ResourceOp(ResourceType::SettlementRedeem, Operation::Read),
                ) => {}
                (_, unexpected_path, unexpected_rule) => panic!(
                    "unexpected settlement RBAC mapping for {unexpected_path}: {unexpected_rule:?}"
                ),
            }
        }
    }
}
