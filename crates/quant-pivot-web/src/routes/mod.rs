//! Route registration and authorization manifest (Phase 0).

pub mod auth;
pub mod health;
pub mod markets;
pub mod menus;
pub mod metrics;
pub mod operation_logs;
pub mod permissions;
pub mod registry;
pub mod roles;
pub mod runtime_config;
pub mod system;
pub mod users;
pub mod version;
pub mod ws;

use actix_web::{
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
    ws::handler::ws_upgrade,
};

fn protected_route_specs() -> Vec<RouteSpec> {
    let mut specs = Vec::new();
    specs.extend(auth::route_specs());
    specs.extend(users::route_specs());
    specs.extend(roles::route_specs());
    specs.extend(menus::route_specs());
    specs.extend(permissions::route_specs());
    specs.extend(runtime_config::route_specs());
    specs.extend(operation_logs::route_specs());
    specs.extend(system::route_specs());
    specs.extend(markets::route_specs());
    specs
}

#[must_use]
pub fn init_rbac_rules() -> PermChecker {
    let mut checker = PermChecker::new();
    for spec in protected_route_specs() {
        checker.register(spec.method, format!("{API_PREFIX}{}", spec.path), spec.rule);
    }
    checker
}

pub fn configure(cfg: &mut ServiceConfig) {
    cfg.service(
        web::scope("/api")
            .service(web::scope("/ws").route("", web::get().to(ws_upgrade)))
            .service(
                web::scope("")
                    .wrap(from_fn(authz))
                    .wrap(from_fn(authn))
                    .configure(|cfg| register_protected(cfg, protected_route_specs())),
            )
            .service(
                web::scope("")
                    .guard(ApiV1Guard)
                    .configure(auth::configure_public),
            ),
    )
    .service(web::scope("").configure(health::configure))
    .route("/metrics", web::get().to(metrics::metrics));
}

fn register_protected(cfg: &mut ServiceConfig, specs: Vec<RouteSpec>) {
    for spec in specs {
        cfg.service(
            actix_web::Resource::new(format!("{API_PREFIX}{}", spec.path)).route(spec.route),
        );
    }
}
