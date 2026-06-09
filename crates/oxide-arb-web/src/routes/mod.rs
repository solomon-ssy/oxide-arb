//! Route registration and the route → authorization manifest.
//!
//! Versioning is header-driven (see [`version::ApiV1Guard`]): every endpoint
//! lives under the unversioned path `/api/...` and is selected for `v1` by the
//! `Accept-Api-Version: v1` header. Liveness/readiness probes sit outside the
//! versioned scope so orchestrators can reach them without negotiating a
//! version.
//!
//! # Single-source authorization manifest
//!
//! [`protected_route_specs`] is the *one* declarative list of every protected
//! route: its method, path pattern, authorization [`Rule`], and handler. Both
//! [`configure`] (which registers the actix routes) and [`init_rbac_rules`]
//! (which builds the [`PermChecker`]) derive from it, so every registered
//! protected route is guaranteed to have a rule **by construction** — there is
//! no way to add a route without also declaring how it is authorized.
//!
//! Each resource module owns its slice of the manifest via [`RouteSpec`] lists;
//! this module only aggregates them and wires the actix scopes.
//!
//! # Pipeline
//!
//! Public routes (`login`, `refresh`) and probes are registered outside the
//! authorized scope and never reach the checker. Everything else is wrapped by
//! [`authn`](crate::middleware::authn) (outer scope) then
//! [`authz`](crate::middleware::authz) (inner scope), so identity is always
//! established before authorization, and an unregistered protected route is
//! denied (fail-closed).

pub mod analytics;
pub mod auth;
pub mod control_factors;
pub mod health;
pub mod markets;
pub mod menus;
pub mod metrics;
pub mod operation_logs;
pub mod opportunities;
pub mod permissions;
pub mod pnl;
pub mod registry;
pub mod replay;
pub mod risk;
pub mod roles;
pub mod runtime_config;
pub mod system;
pub mod trades;
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
    specs.extend(users::route_specs());
    specs.extend(roles::route_specs());
    specs.extend(menus::route_specs());
    specs.extend(permissions::route_specs());
    specs.extend(control_factors::route_specs());
    specs.extend(runtime_config::route_specs());
    specs.extend(operation_logs::route_specs());
    specs.extend(system::route_specs());
    specs.extend(risk::route_specs());
    specs.extend(markets::route_specs());
    specs.extend(opportunities::route_specs());
    specs.extend(trades::route_specs());
    specs.extend(pnl::route_specs());
    specs.extend(analytics::route_specs());
    specs.extend(replay::route_specs());
    specs
}

/// Build the route-level [`PermChecker`] from the manifest.
///
/// Keys are `(method, API_PREFIX + path)`, matching the pattern the authz
/// middleware reads from `ServiceRequest::match_pattern()`.
#[must_use]
pub fn init_rbac_rules() -> PermChecker {
    let mut checker = PermChecker::new();
    for spec in protected_route_specs() {
        checker.register(spec.method, format!("{API_PREFIX}{}", spec.path), spec.rule);
    }
    checker
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
        .route("/ready", web::get().to(health::ready))
        .route("/metrics", web::get().to(metrics::metrics))
        .service(
            web::scope(API_PREFIX)
                .guard(ApiV1Guard)
                .route("/auth/login", web::post().to(auth::login))
                .route("/auth/refresh", web::post().to(auth::refresh))
                // WebSocket upgrade authenticates via query token before upgrade
                // (browsers cannot set handshake headers), so it sits outside the
                // Bearer-header authn scope and self-authenticates. Wired through
                // `routes::ws` so every route is registered under `routes/`.
                .configure(ws::configure)
                .service(protected),
        );
}
