//! The per-route authorization contract.
//!
//! Every protected route is registered with exactly one [`Rule`]. There is no
//! `Public` variant by design: public routes live *outside* the authorized
//! scope and never reach the [`PermChecker`], so a route inside the scope can
//! never be accidentally marked public — the only way to admit it is an
//! explicit rule.
//!
//! [`PermChecker`]: super::checker::PermChecker

use crate::{
    auth::casbin::{checker::SUPER_ADMIN_ROLE, service::CasbinService},
    error::WebError,
    extractors::ActorRoles,
    jwt::Claims,
};
use quant_pivot_models::enums::rbac::{Operation, ResourceType};

/// The result of a successful authorization check.
///
/// Carries the resolved `acting_role` for governed endpoints so the middleware
/// can inject it into request extensions for the audit envelope; it is `None`
/// for the `super_admin` bypass and for non-governed rules.
#[derive(Debug, Clone, Default)]
pub struct AuthzOutcome {
    /// The explicit role the caller acted as (governed endpoints only).
    pub acting_role: Option<String>,
}

/// The authorization rule attached to a protected route.
#[derive(Debug, Clone)]
pub enum Rule {
    /// Any authenticated actor is admitted (authn already proved identity).
    /// Used for self-service endpoints such as `logout`, `me`, and the
    /// role-filtered accessible-menu tree.
    AuthenticatedOnly,

    /// The actor must hold (via any enabled role) the `(resource, operation)`
    /// permission. This is the standard RBAC check for read and management
    /// endpoints.
    ResourceOp(ResourceType, Operation),

    /// A governed, audited mutation: the caller must declare an explicit
    /// `acting_role`, hold it (enabled), and that role must itself carry the
    /// `(resource, operation)` permission. The resolved role is returned for the
    /// audit envelope.
    ///
    /// Defined and evaluated here in Phase 6.4; the governance routes that use
    /// it land in Phase 6.5.
    ActingRoleGoverned(ResourceType, Operation),

    /// A governed, audited mutation whose concrete operation is not known until
    /// the request body is parsed, so the operation-level authorization is
    /// **deferred to the handler**. The middleware still enforces the governed
    /// preconditions — an explicit `acting_role` that the caller holds (enabled)
    /// — and resolves it for the audit envelope; the handler then computes the
    /// exact `(resource, operation)` from the payload and authorizes it against
    /// the acting role (super-admin bypass preserved).
    ///
    /// Used by the kill-switch endpoint, where the required permission depends on
    /// the transition (`halt` / `resume` / `emergency`).
    ActingRoleDeferred(ResourceType),
}

impl Rule {
    /// Whether this rule authorizes a governed, audited mutation.
    #[must_use]
    pub const fn is_governed(&self) -> bool {
        matches!(
            self,
            Self::ActingRoleGoverned(..) | Self::ActingRoleDeferred(..)
        )
    }

    /// Resolves the `acting_role` recorded into the audit envelope when a
    /// `super_admin` bypasses authorization on a governed route.
    ///
    /// A `super_admin` may act on behalf of an explicit role: when the
    /// `X-Acting-Role` header names a role they actually hold (enabled), that
    /// role is recorded; otherwise the bypass is attributed to the literal
    /// `super_admin`. This keeps governed handlers uniform — they always read a
    /// resolved [`ActingRole`](crate::extractors::ActingRole) from extensions.
    #[must_use]
    pub fn resolve_super_admin_acting_role(
        roles: &ActorRoles,
        acting_role: Option<&str>,
    ) -> String {
        acting_role
            .map(str::trim)
            .filter(|role| !role.is_empty())
            .filter(|role| roles.contains_enabled(role))
            .map_or_else(|| SUPER_ADMIN_ROLE.to_owned(), ToOwned::to_owned)
    }

    /// Evaluate this rule for an already-authenticated actor.
    ///
    /// `acting_role` is the value of the `X-Acting-Role` header (if any),
    /// consulted only by [`Rule::ActingRoleGoverned`].
    pub async fn evaluate(
        &self,
        claims: &Claims,
        roles: &ActorRoles,
        casbin: &CasbinService,
        acting_role: Option<&str>,
    ) -> Result<AuthzOutcome, WebError> {
        match self {
            Self::AuthenticatedOnly => Ok(AuthzOutcome::default()),

            Self::ResourceOp(resource, operation) => {
                if casbin
                    .enforce(&claims.sub, resource.as_str(), operation.as_str())
                    .await?
                {
                    Ok(AuthzOutcome::default())
                } else {
                    Err(WebError::Forbidden)
                }
            }

            Self::ActingRoleGoverned(resource, operation) => {
                let acting_role = acting_role
                    .map(str::trim)
                    .filter(|role| !role.is_empty())
                    .ok_or_else(|| {
                        WebError::BadRequest("missing acting role (X-Acting-Role)".to_owned())
                    })?;

                // The caller must currently hold the role they claim to act as…
                if !roles.contains_enabled(acting_role) {
                    return Err(WebError::Forbidden);
                }
                // …and that role must carry the governed permission.
                if !casbin
                    .has_policy(acting_role, resource.as_str(), operation.as_str())
                    .await
                {
                    return Err(WebError::Forbidden);
                }
                Ok(AuthzOutcome {
                    acting_role: Some(acting_role.to_owned()),
                })
            }

            Self::ActingRoleDeferred(_resource) => {
                // Governed preconditions only: the operation-level authorization
                // is performed by the handler once the payload is known.
                let acting_role = acting_role
                    .map(str::trim)
                    .filter(|role| !role.is_empty())
                    .ok_or_else(|| {
                        WebError::BadRequest("missing acting role (X-Acting-Role)".to_owned())
                    })?;
                if !roles.contains_enabled(acting_role) {
                    return Err(WebError::Forbidden);
                }
                Ok(AuthzOutcome {
                    acting_role: Some(acting_role.to_owned()),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use quant_pivot_models::{
        domain::RoleInfo,
        enums::rbac::{Operation, ResourceType, RoleKind, RoleStatus},
        types::RoleId,
    };

    use super::{ActorRoles, CasbinService, Claims, Rule, WebError};
    use crate::jwt::TokenUse;

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

    fn role(code: &str, status: RoleStatus) -> RoleInfo {
        RoleInfo {
            id: RoleId::from_v7(),
            code: code.to_owned(),
            name: code.to_owned(),
            description: None,
            kind: RoleKind::Custom,
            status,
            sort: 0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn roles(specs: &[(&str, RoleStatus)]) -> ActorRoles {
        ActorRoles::new(
            specs
                .iter()
                .map(|(code, status)| role(code, *status))
                .collect(),
        )
    }

    #[actix_web::test]
    async fn authenticated_only_always_allows() {
        let casbin = CasbinService::in_memory().await;
        let outcome = Rule::AuthenticatedOnly
            .evaluate(&claims(), &roles(&[]), &casbin, None)
            .await
            .expect("authenticated-only admits any actor");
        assert!(outcome.acting_role.is_none());
    }

    #[actix_web::test]
    async fn resource_op_allows_with_matching_grant_and_denies_without() {
        let casbin = CasbinService::in_memory().await;
        casbin.add_test_grouping("user-1", "ops").await;
        casbin.add_test_policy("ops", "user", "read").await;
        let rule = Rule::ResourceOp(ResourceType::User, Operation::Read);

        assert!(
            rule.evaluate(&claims(), &roles(&[]), &casbin, None)
                .await
                .is_ok(),
            "a granted (resource, op) is allowed"
        );

        let denied = Rule::ResourceOp(ResourceType::User, Operation::Delete)
            .evaluate(&claims(), &roles(&[]), &casbin, None)
            .await;
        assert!(matches!(denied, Err(WebError::Forbidden)));
    }

    #[actix_web::test]
    async fn acting_role_missing_is_bad_request() {
        let casbin = CasbinService::in_memory().await;
        let result =
            Rule::ActingRoleGoverned(ResourceType::DecisionPolicySnapshot, Operation::Activate)
                .evaluate(
                    &claims(),
                    &roles(&[("risk_owner", RoleStatus::Enabled)]),
                    &casbin,
                    None,
                )
                .await;
        assert!(matches!(result, Err(WebError::BadRequest(_))));
    }

    #[actix_web::test]
    async fn acting_role_not_held_is_forbidden() {
        let casbin = CasbinService::in_memory().await;
        casbin
            .add_test_policy("risk_owner", "runtime_config", "activate")
            .await;
        let result =
            Rule::ActingRoleGoverned(ResourceType::DecisionPolicySnapshot, Operation::Activate)
                .evaluate(
                    &claims(),
                    &roles(&[("viewer", RoleStatus::Enabled)]),
                    &casbin,
                    Some("risk_owner"),
                )
                .await;
        assert!(matches!(result, Err(WebError::Forbidden)));
    }

    #[actix_web::test]
    async fn acting_role_held_but_disabled_is_forbidden() {
        let casbin = CasbinService::in_memory().await;
        casbin
            .add_test_policy("risk_owner", "runtime_config", "activate")
            .await;
        let result =
            Rule::ActingRoleGoverned(ResourceType::DecisionPolicySnapshot, Operation::Activate)
                .evaluate(
                    &claims(),
                    &roles(&[("risk_owner", RoleStatus::Disabled)]),
                    &casbin,
                    Some("risk_owner"),
                )
                .await;
        assert!(matches!(result, Err(WebError::Forbidden)));
    }

    #[actix_web::test]
    async fn acting_role_held_without_policy_is_forbidden() {
        let casbin = CasbinService::in_memory().await;
        let result =
            Rule::ActingRoleGoverned(ResourceType::DecisionPolicySnapshot, Operation::Activate)
                .evaluate(
                    &claims(),
                    &roles(&[("risk_owner", RoleStatus::Enabled)]),
                    &casbin,
                    Some("risk_owner"),
                )
                .await;
        assert!(matches!(result, Err(WebError::Forbidden)));
    }

    #[actix_web::test]
    async fn acting_role_held_with_policy_is_allowed_and_returns_role() {
        let casbin = CasbinService::in_memory().await;
        casbin
            .add_test_policy("risk_owner", "runtime_config", "activate")
            .await;
        let outcome =
            Rule::ActingRoleGoverned(ResourceType::DecisionPolicySnapshot, Operation::Activate)
                .evaluate(
                    &claims(),
                    &roles(&[("risk_owner", RoleStatus::Enabled)]),
                    &casbin,
                    Some("  risk_owner  "),
                )
                .await
                .expect("governed action with held role + policy is allowed");
        assert_eq!(outcome.acting_role.as_deref(), Some("risk_owner"));
    }

    #[actix_web::test]
    async fn deferred_missing_acting_role_is_bad_request() {
        let casbin = CasbinService::in_memory().await;
        let result = Rule::ActingRoleDeferred(ResourceType::System)
            .evaluate(
                &claims(),
                &roles(&[("operator", RoleStatus::Enabled)]),
                &casbin,
                None,
            )
            .await;
        assert!(matches!(result, Err(WebError::BadRequest(_))));
    }

    #[actix_web::test]
    async fn deferred_acting_role_not_held_is_forbidden() {
        let casbin = CasbinService::in_memory().await;
        let result = Rule::ActingRoleDeferred(ResourceType::System)
            .evaluate(
                &claims(),
                &roles(&[("viewer", RoleStatus::Enabled)]),
                &casbin,
                Some("operator"),
            )
            .await;
        assert!(matches!(result, Err(WebError::Forbidden)));
    }

    #[actix_web::test]
    async fn deferred_held_role_resolves_without_op_check() {
        // No policy is registered: the deferred rule authorizes the governed
        // preconditions only and leaves the op-level check to the handler.
        let casbin = CasbinService::in_memory().await;
        let outcome = Rule::ActingRoleDeferred(ResourceType::System)
            .evaluate(
                &claims(),
                &roles(&[("operator", RoleStatus::Enabled)]),
                &casbin,
                Some("  operator  "),
            )
            .await
            .expect("deferred governed action resolves the acting role");
        assert_eq!(outcome.acting_role.as_deref(), Some("operator"));
    }
}
