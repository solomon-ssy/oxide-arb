//! Header-based API version selection.
//!
//! The API is versioned through a request header rather than the URL path:
//! clients select a version with `Accept-Api-Version: v1` (or the legacy
//! `X-API-Version: v1`). [`ApiV1Guard`] gates the `v1` route scope, so a request
//! that omits the header — or asks for a version this build does not serve — is
//! simply not matched by the `v1` scope.
//!
//! Trust boundary: the version header steers **routing only**. It never feeds
//! any authentication or authorization decision.

use actix_web::guard::{Guard, GuardContext};

/// Preferred version-negotiation header.
const ACCEPT_API_VERSION: &str = "accept-api-version";

/// Legacy/compatibility version header, consulted only if the preferred one is
/// absent.
const X_API_VERSION: &str = "x-api-version";

/// The version token this guard accepts.
const V1: &[u8] = b"v1";

/// Routing guard that admits requests targeting API **v1**.
///
/// A request passes when either [`ACCEPT_API_VERSION`] or [`X_API_VERSION`]
/// carries the value `v1`; otherwise the guarded scope is skipped.
pub struct ApiV1Guard;

impl Guard for ApiV1Guard {
    fn check(&self, ctx: &GuardContext<'_>) -> bool {
        let headers = ctx.head().headers();
        headers
            .get(ACCEPT_API_VERSION)
            .or_else(|| headers.get(X_API_VERSION))
            .is_some_and(|value| value.as_bytes() == V1)
    }
}
