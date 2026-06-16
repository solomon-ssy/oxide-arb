//! Role-based access-control enums and the canonical permission catalog.
//!
//! [`ResourceType`] × [`Operation`] is the complete, fail-closed permission
//! space. [`RESOURCE_OPERATIONS`] is the single source of truth that maps every
//! resource to the operations defined for it; it drives:
//!
//! 1. seeding the built-in roles' Casbin `p` policies,
//! 2. validating role/permission assignment requests (no phantom resource×op
//!    combinations), and
//! 3. the `/permissions/catalog` endpoint.
//!
//! [`ResourceType`] and [`Operation`] additionally implement [`FromStr`] so the
//! Casbin `casbin_rule` table (which stores raw strings) can be reverse-mapped
//! back into typed values.

use std::str::FromStr;

use oxide_arb_error::rbac::RbacError;
use sea_orm::Iterable;

/// Casbin policy-row layout — the single source of truth for how RBAC policy
/// lines are encoded into the `casbin_rule` table's `(ptype, v0..v5)` shape.
///
/// Shared by the policy seed, the Postgres [`Adapter`] implementation, the
/// transactional policy-sync helpers, and the web enforcer model, so the
/// encoding can never drift between writer, reader, and matcher.
///
/// - Grouping (`g`) line: `v0 = user_id`, `v1 = role_code`.
/// - Permission (`p`) line: `v0 = role_code`, `v1 = resource`, `v2 = operation`,
///   and `v3` holds [`OBJECT_TYPE_RESOURCE`].
///
/// [`Adapter`]: https://docs.rs/casbin/latest/casbin/trait.Adapter.html
pub mod casbin {
    /// `ptype` discriminator for permission (`p`) lines.
    pub const PTYPE_POLICY: &str = "p";
    /// `ptype` discriminator for grouping (`g`) lines.
    pub const PTYPE_GROUPING: &str = "g";
    /// Object-type discriminator matched by the Casbin model (`p.typ`).
    pub const OBJECT_TYPE_RESOURCE: &str = "resource";
    /// Number of value columns on `casbin_rule` (`v0..v5`).
    pub const VALUE_COLUMNS: usize = 6;
    /// Policy sections persisted by a full `save_policy`, in `p`-then-`g` order.
    pub const SECTIONS: [&str; 2] = [PTYPE_POLICY, PTYPE_GROUPING];
}

active_string_enum! {
    /// Lifecycle state of a user account.
    pub enum UserStatus {
        Active => "active",
        Disabled => "disabled",
    }
}

active_string_enum! {
    /// Whether a role is system-provisioned or operator-created.
    pub enum RoleKind {
        Builtin => "builtin",
        Custom => "custom",
    }
}

active_string_enum! {
    /// Enabled/disabled flag shared by roles and menus.
    pub enum RoleStatus {
        Enabled => "enabled",
        Disabled => "disabled",
    }
}

active_string_enum! {
    /// Structural kind of a menu node in the navigation tree.
    pub enum MenuKind {
        Directory => "directory",
        Menu => "menu",
        Button => "button",
    }
}

active_string_enum! {
    /// Resource categories addressable by Casbin `p` policies.
    pub enum ResourceType {
        System => "system",
        Market => "market",
        Opportunity => "opportunity",
        Trade => "trade",
        Pnl => "pnl",
        Risk => "risk",
        Blacklist => "blacklist",
        RuntimeConfig => "runtime_config",
        ControlFactor => "control_factor",
        Publication => "publication",
        Materialization => "materialization",
        Replay => "replay",
        Analytics => "analytics",
        Audit => "audit",
        OperationLog => "operation_log",
        User => "user",
        Role => "role",
        Menu => "menu",
        Permission => "permission",
    }
}

active_string_enum! {
    /// Operation verbs in Casbin `p` policies.
    pub enum Operation {
        Read => "read",
        Create => "create",
        Update => "update",
        Delete => "delete",
        Assign => "assign",
        Halt => "halt",
        Resume => "resume",
        SwitchMode => "switch_mode",
        Reset => "reset",
        Reject => "reject",
        Shadow => "shadow",
        Publish => "publish",
        Rollback => "rollback",
        Activate => "activate",
        Enqueue => "enqueue",
        Emergency => "emergency",
    }
}

impl FromStr for ResourceType {
    type Err = RbacError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::iter()
            .find(|variant| variant.as_str() == s)
            .ok_or_else(|| RbacError::UnknownPermission {
                resource: s.to_owned(),
                operation: String::new(),
            })
    }
}

impl FromStr for Operation {
    type Err = RbacError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::iter()
            .find(|variant| variant.as_str() == s)
            .ok_or_else(|| RbacError::UnknownPermission {
                resource: String::new(),
                operation: s.to_owned(),
            })
    }
}

/// Parse a Casbin `(resource, operation)` token pair into typed values.
///
/// Returns a single [`RbacError::UnknownPermission`] carrying both raw tokens if
/// either side fails to parse, so the error message reflects the actual policy
/// row that could not be mapped.
pub fn parse_permission(
    resource: &str,
    operation: &str,
) -> Result<(ResourceType, Operation), RbacError> {
    match (
        ResourceType::from_str(resource),
        Operation::from_str(operation),
    ) {
        (Ok(res), Ok(op)) => Ok((res, op)),
        _ => Err(RbacError::UnknownPermission {
            resource: resource.to_owned(),
            operation: operation.to_owned(),
        }),
    }
}

/// Canonical permission catalog: every resource and the operations valid for it.
///
/// This is the authoritative definition of the permission space. Casbin policy
/// seeding, assignment validation, and the permission-catalog endpoint all
/// derive from this table — keep it aligned with the route authorization map.
pub static RESOURCE_OPERATIONS: &[(ResourceType, &[Operation])] = &[
    (
        ResourceType::System,
        &[
            Operation::Read,
            Operation::Halt,
            Operation::Resume,
            Operation::SwitchMode,
        ],
    ),
    (ResourceType::Market, &[Operation::Read, Operation::Update]),
    (ResourceType::Opportunity, &[Operation::Read]),
    (ResourceType::Trade, &[Operation::Read, Operation::Update]),
    (ResourceType::Pnl, &[Operation::Read]),
    (ResourceType::Risk, &[Operation::Read, Operation::Reset]),
    (
        ResourceType::Blacklist,
        &[Operation::Read, Operation::Create, Operation::Delete],
    ),
    (
        ResourceType::RuntimeConfig,
        &[
            Operation::Read,
            Operation::Create,
            Operation::Activate,
            Operation::Rollback,
        ],
    ),
    (
        ResourceType::ControlFactor,
        &[
            Operation::Read,
            Operation::Reject,
            Operation::Shadow,
            Operation::Publish,
            Operation::Emergency,
        ],
    ),
    (ResourceType::Publication, &[Operation::Rollback]),
    (ResourceType::Materialization, &[Operation::Enqueue]),
    (ResourceType::Replay, &[Operation::Read, Operation::Create]),
    (ResourceType::Analytics, &[Operation::Read]),
    (ResourceType::Audit, &[Operation::Read]),
    (ResourceType::OperationLog, &[Operation::Read]),
    (
        ResourceType::User,
        &[
            Operation::Read,
            Operation::Create,
            Operation::Update,
            Operation::Delete,
            Operation::Assign,
        ],
    ),
    (
        ResourceType::Role,
        &[
            Operation::Read,
            Operation::Create,
            Operation::Update,
            Operation::Delete,
            Operation::Assign,
        ],
    ),
    (
        ResourceType::Menu,
        &[
            Operation::Read,
            Operation::Create,
            Operation::Update,
            Operation::Delete,
        ],
    ),
    (ResourceType::Permission, &[Operation::Read]),
];

impl ResourceType {
    /// Return the operations defined for this resource in [`RESOURCE_OPERATIONS`].
    ///
    /// Returns an empty slice only if the catalog is missing this resource —
    /// which the completeness test forbids.
    #[must_use]
    pub fn operations(self) -> &'static [Operation] {
        RESOURCE_OPERATIONS
            .iter()
            .find(|(resource, _)| *resource == self)
            .map_or(&[], |(_, ops)| *ops)
    }

    /// Whether the given operation is defined for this resource.
    #[must_use]
    pub fn allows(self, operation: Operation) -> bool {
        self.operations().contains(&operation)
    }
}

#[cfg(test)]
mod tests {
    use super::{Operation, RESOURCE_OPERATIONS, ResourceType, parse_permission};
    use sea_orm::Iterable;
    use std::{collections::HashSet, str::FromStr};

    #[test]
    fn resource_type_round_trips_via_from_str() {
        for variant in ResourceType::iter() {
            let parsed = ResourceType::from_str(variant.as_str()).expect("known resource");
            assert_eq!(parsed, variant);
        }
    }

    #[test]
    fn operation_round_trips_via_from_str() {
        for variant in Operation::iter() {
            let parsed = Operation::from_str(variant.as_str()).expect("known operation");
            assert_eq!(parsed, variant);
        }
    }

    #[test]
    fn from_str_rejects_unknown_tokens() {
        assert!(ResourceType::from_str("nope").is_err());
        assert!(Operation::from_str("nope").is_err());
        assert!(parse_permission("nope", "read").is_err());
        assert!(parse_permission("system", "nope").is_err());
        assert!(parse_permission("system", "read").is_ok());
    }

    #[test]
    fn catalog_covers_every_resource_exactly_once() {
        let mut seen = HashSet::new();
        for (resource, _) in RESOURCE_OPERATIONS {
            assert!(seen.insert(*resource), "duplicate resource {resource:?}");
        }
        for variant in ResourceType::iter() {
            assert!(
                seen.contains(&variant),
                "RESOURCE_OPERATIONS missing {variant:?}"
            );
        }
        assert_eq!(seen.len(), ResourceType::iter().count());
    }

    #[test]
    fn catalog_operations_are_unique_per_resource() {
        for (resource, ops) in RESOURCE_OPERATIONS {
            let mut seen = HashSet::new();
            for op in *ops {
                assert!(
                    seen.insert(*op),
                    "duplicate operation {op:?} for {resource:?}"
                );
            }
            assert!(!ops.is_empty(), "{resource:?} must define at least one op");
        }
    }

    #[test]
    fn resource_allows_matches_catalog() {
        assert!(ResourceType::System.allows(Operation::Halt));
        assert!(!ResourceType::System.allows(Operation::Publish));
        assert!(ResourceType::ControlFactor.allows(Operation::Publish));
        assert!(!ResourceType::Opportunity.allows(Operation::Delete));
    }
}
