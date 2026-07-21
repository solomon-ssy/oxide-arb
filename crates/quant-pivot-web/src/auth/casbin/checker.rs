//! The route → rule registry consulted by the authz middleware.
//!
//! Keyed by `(method, matched-path-pattern)` — e.g. `(GET, "/api/users/{id}")`
//! — the registry is **fail-closed**: a protected route with no registered rule
//! is denied (`403`), eliminating the ng-gateway default-allow defect where an
//! unregistered route fell through to "permit".
//!
//! The registry is built once at startup from the single declarative route
//! manifest (see [`crate::routes`]), so every registered protected route is
//! guaranteed to have a rule by construction.

use std::collections::HashMap;

use actix_web::http::Method;

use crate::{
    auth::casbin::{
        rules::{AuthzOutcome, Rule},
        service::CasbinService,
    },
    error::WebError,
    extractors::ActorRoles,
    jwt::Claims,
};

/// The role code whose holders bypass all route-level authorization.
pub const SUPER_ADMIN_ROLE: &str = "super_admin";

/// Route-level authorization registry.
#[derive(Debug, Default)]
pub struct PermChecker {
    rules: HashMap<(Method, String), Rule>,
}

impl PermChecker {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `rule` for the `(method, path-pattern)` route.
    ///
    /// Panics on a duplicate `(method, path)` registration: the route manifest
    /// is a compile-time-shaped constant, so a duplicate is a programming error
    /// that must fail loudly at startup, never silently shadow a rule.
    pub fn register(&mut self, method: Method, path: impl Into<String>, rule: Rule) {
        let path = path.into();
        let key = (method, path);
        assert!(
            !self.rules.contains_key(&key),
            "duplicate authorization rule for {} {}",
            key.0,
            key.1
        );
        self.rules.insert(key, rule);
    }

    /// Number of registered rules (used by completeness tests).
    #[must_use]
    pub fn len(&self) -> usize {
        self.rules.len()
    }

    /// Whether the registry is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// Authorize a request against the registry.
    ///
    /// 1. An enabled `super_admin` bypasses authorization (including unregistered
    ///    routes). On a governed route the bypass still resolves an
    ///    `acting_role` for the audit envelope (the explicit `X-Acting-Role`
    ///    when held, else the literal `super_admin`) so governed handlers read a
    ///    uniform [`ActingRole`](crate::extractors::ActingRole).
    /// 2. Otherwise the `(method, matched_path)` rule is evaluated; a missing
    ///    rule is **denied** (fail-closed).
    pub async fn check(
        &self,
        method: &Method,
        matched_path: &str,
        claims: &Claims,
        roles: &ActorRoles,
        casbin: &CasbinService,
        acting_role: Option<&str>,
    ) -> Result<AuthzOutcome, WebError> {
        let rule = self.rules.get(&(method.clone(), matched_path.to_owned()));
        if roles.contains_enabled(SUPER_ADMIN_ROLE) {
            let acting_role = rule
                .filter(|rule| rule.is_governed())
                .map(|_| Rule::resolve_super_admin_acting_role(roles, acting_role));
            return Ok(AuthzOutcome { acting_role });
        }
        match rule {
            Some(rule) => rule.evaluate(claims, roles, casbin, acting_role).await,
            None => Err(WebError::Forbidden),
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use quant_pivot_models::{
        domain::rbac::RoleInfo,
        enums::rbac::{Operation, ResourceType, RoleKind, RoleStatus},
        types::RoleId,
    };

    use super::{
        AuthzOutcome, CasbinService, Claims, Method, PermChecker, Rule, SUPER_ADMIN_ROLE, WebError,
    };
    use crate::{extractors::ActorRoles, jwt::TokenUse};

    fn claims() -> Claims {
        Claims {
            jti: "jti".to_owned(),
            sub: "user-1".to_owned(),
            iss: "quant-pivot".to_owned(),
            aud: "quant-pivot-web".to_owned(),
            iat: 0,
            nbf: 0,
            exp: 0,
            username: "tester".to_owned(),
            token_use: TokenUse::Access,
            family_id: "family-1".to_owned(),
            session_exp: 4_102_444_800,
            generation: 0,
        }
    }

    fn roles(specs: &[(&str, RoleStatus)]) -> ActorRoles {
        ActorRoles::new(
            specs
                .iter()
                .map(|(code, status)| RoleInfo {
                    id: RoleId::from_v7(),
                    code: (*code).into(),
                    name: (*code).to_owned(),
                    description: None,
                    kind: RoleKind::Custom,
                    status: *status,
                    sort: 0,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                })
                .collect(),
        )
    }

    #[actix_web::test]
    async fn enabled_super_admin_bypasses_even_unregistered_routes() {
        let casbin = CasbinService::in_memory().await;
        let checker = PermChecker::new();
        let outcome: AuthzOutcome = checker
            .check(
                &Method::DELETE,
                "/api/anything/unregistered",
                &claims(),
                &roles(&[(SUPER_ADMIN_ROLE, RoleStatus::Enabled)]),
                &casbin,
                None,
            )
            .await
            .expect("super_admin bypasses everything");
        assert!(outcome.acting_role.is_none());
    }

    #[actix_web::test]
    async fn super_admin_on_governed_route_records_explicit_held_acting_role() {
        let casbin = CasbinService::in_memory().await;
        let mut checker = PermChecker::new();
        checker.register(
            Method::POST,
            "/api/runtime-config/{id}/activate",
            Rule::ActingRoleGoverned(ResourceType::DecisionPolicySnapshot, Operation::Activate),
        );
        let outcome = checker
            .check(
                &Method::POST,
                "/api/runtime-config/{id}/activate",
                &claims(),
                &roles(&[
                    (SUPER_ADMIN_ROLE, RoleStatus::Enabled),
                    ("risk_owner", RoleStatus::Enabled),
                ]),
                &casbin,
                Some("  risk_owner  "),
            )
            .await
            .expect("super_admin bypasses governed authorization");
        assert_eq!(outcome.acting_role.as_deref(), Some("risk_owner"));
    }

    #[actix_web::test]
    async fn super_admin_on_governed_route_without_held_role_records_super_admin() {
        let casbin = CasbinService::in_memory().await;
        let mut checker = PermChecker::new();
        checker.register(
            Method::POST,
            "/api/runtime-config/{id}/activate",
            Rule::ActingRoleGoverned(ResourceType::DecisionPolicySnapshot, Operation::Activate),
        );
        // No header at all → attributed to the literal super_admin.
        let bare = checker
            .check(
                &Method::POST,
                "/api/runtime-config/{id}/activate",
                &claims(),
                &roles(&[(SUPER_ADMIN_ROLE, RoleStatus::Enabled)]),
                &casbin,
                None,
            )
            .await
            .expect("super_admin bypass");
        assert_eq!(bare.acting_role.as_deref(), Some(SUPER_ADMIN_ROLE));
        // Header naming a role the caller does not hold → also super_admin.
        let unheld = checker
            .check(
                &Method::POST,
                "/api/runtime-config/{id}/activate",
                &claims(),
                &roles(&[(SUPER_ADMIN_ROLE, RoleStatus::Enabled)]),
                &casbin,
                Some("risk_owner"),
            )
            .await
            .expect("super_admin bypass");
        assert_eq!(unheld.acting_role.as_deref(), Some(SUPER_ADMIN_ROLE));
    }

    #[actix_web::test]
    async fn disabled_super_admin_does_not_bypass() {
        let casbin = CasbinService::in_memory().await;
        let checker = PermChecker::new();
        let result = checker
            .check(
                &Method::GET,
                "/api/users",
                &claims(),
                &roles(&[(SUPER_ADMIN_ROLE, RoleStatus::Disabled)]),
                &casbin,
                None,
            )
            .await;
        assert!(matches!(result, Err(WebError::Forbidden)));
    }

    #[actix_web::test]
    async fn unregistered_route_is_denied_fail_closed() {
        let casbin = CasbinService::in_memory().await;
        let checker = PermChecker::new();
        let result = checker
            .check(
                &Method::GET,
                "/api/users",
                &claims(),
                &roles(&[("viewer", RoleStatus::Enabled)]),
                &casbin,
                None,
            )
            .await;
        assert!(matches!(result, Err(WebError::Forbidden)));
    }

    #[actix_web::test]
    async fn registered_authenticated_only_rule_is_admitted() {
        let casbin = CasbinService::in_memory().await;
        let mut checker = PermChecker::new();
        checker.register(Method::GET, "/api/auth/me", Rule::AuthenticatedOnly);
        assert!(
            checker
                .check(
                    &Method::GET,
                    "/api/auth/me",
                    &claims(),
                    &roles(&[("viewer", RoleStatus::Enabled)]),
                    &casbin,
                    None,
                )
                .await
                .is_ok()
        );
    }

    #[test]
    #[should_panic(expected = "duplicate authorization rule")]
    fn duplicate_registration_panics() {
        let mut checker = PermChecker::new();
        checker.register(Method::GET, "/api/users", Rule::AuthenticatedOnly);
        checker.register(Method::GET, "/api/users", Rule::AuthenticatedOnly);
    }
}
