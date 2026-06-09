//! The Casbin model backing dynamic RBAC.
//!
//! A 4-tuple request/policy `(sub, obj, act, typ)` plus a `super_admin` bypass
//! baked into the matcher. The `g` grouping binds a **stable `user_id`** subject
//! to a role *code* (never a username literal — renaming a user must not move
//! their authority), and `p` binds a role code to a `(resource, operation,
//! "resource")` permission.
//!
//! This string is the single source of truth for the model shape and is kept
//! byte-for-byte in sync with the policy encoding in
//! [`oxide_arb_models::enums::rbac::casbin`] and the adapter/repository tests
//! (`crates/oxide-arb-repository/tests/pg_rbac.rs`).

/// Casbin model definition: 4-tuple matching with a `super_admin` short-circuit.
///
/// Matcher semantics: a request is allowed if the subject holds the
/// `super_admin` role, **or** there exists a `resource`-typed policy granting
/// the subject (via any role grouping) the requested `(obj, act)`.
pub const CASBIN_MODEL: &str = "\
[request_definition]
r = sub, obj, act, typ
[policy_definition]
p = sub, obj, act, typ
[role_definition]
g = _, _
[policy_effect]
e = some(where (p.eft == allow))
[matchers]
m = g(r.sub, \"super_admin\") || (p.typ == \"resource\" && g(r.sub, p.sub) && r.obj == p.obj && r.act == p.act)
";
