//! Route registration.
//!
//! Versioning is header-driven (see [`version::ApiV1Guard`]): every business
//! endpoint lives under the unversioned path `/api/auth/...` and is selected for
//! `v1` by the `Accept-Api-Version: v1` header. Liveness/readiness probes are
//! infrastructure concerns, registered outside the versioned scope so
//! orchestrators can reach them without negotiating an API version.
//!
//! Within the `v1` scope, public routes (`login`, `refresh`) sit alongside a
//! nested scope wrapped by [`crate::middleware::authn`] that carries the
//! protected routes (`logout`, `me`). The two carry disjoint paths, so each
//! concrete route resolves unambiguously while only the protected ones are
//! authenticated.

pub mod auth;
pub mod health;
pub mod version;

use actix_web::{middleware::from_fn, web};

use crate::{middleware::authn, routes::version::ApiV1Guard};

/// Register all Phase 6.3 routes onto the service config.
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.route("/health", web::get().to(health::health))
        .route("/ready", web::get().to(health::ready))
        .service(
            web::scope("/api/auth")
                .guard(ApiV1Guard)
                .route("/login", web::post().to(auth::login))
                .route("/refresh", web::post().to(auth::refresh))
                .service(
                    web::scope("")
                        .wrap(from_fn(authn))
                        .route("/logout", web::post().to(auth::logout))
                        .route("/me", web::get().to(auth::me)),
                ),
        );
}
