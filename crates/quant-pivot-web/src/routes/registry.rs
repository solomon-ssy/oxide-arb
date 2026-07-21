//! Route registration primitives shared by every route module.
//!
//! Each resource module owns its `RouteSpec` list; `super::protected_route_specs`
//! concatenates them into the single authorization manifest.

use actix_web::{FromRequest, Responder, Route, dev::Handler, http::Method, web};

use crate::auth::casbin::Rule;

/// Path prefix shared by every API route (the version is negotiated by header,
/// not encoded in the path). Authorization rule keys are this prefix plus the
/// route's pattern, matching actix's `match_pattern`.
pub(crate) const API_PREFIX: &str = "/api";

/// A protected route: its method, path pattern (relative to [`API_PREFIX`]),
/// authorization rule, and the actix route that serves it.
pub(crate) struct RouteSpec {
    pub method: Method,
    pub path: &'static str,
    pub rule: Rule,
    pub route: Route,
}

/// Build a [`RouteSpec`], deriving the actix route's method guard from `method`
/// so the registered route and the rule key can never disagree.
pub(crate) fn spec<F, Args>(method: Method, path: &'static str, rule: Rule, handler: F) -> RouteSpec
where
    F: Handler<Args>,
    Args: FromRequest + 'static,
    F::Output: Responder + 'static,
{
    let route = web::method(method.clone()).to(handler);
    RouteSpec {
        method,
        path,
        rule,
        route,
    }
}
