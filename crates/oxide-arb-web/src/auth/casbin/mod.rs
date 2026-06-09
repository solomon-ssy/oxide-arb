//! Dynamic RBAC authorization over Casbin.
//!
//! The pieces fit together as follows:
//!
//! - [`model`] holds the Casbin model (request/policy tuples + the
//!   `super_admin` bypass matcher).
//! - [`service::CasbinService`] wraps a live [`casbin::Enforcer`] (loaded from
//!   the shared `casbin_rule` table via the repository's `PgCasbinAdapter`). It
//!   is **read + reload only**: policy *writes* are owned by the repository
//!   transactions (so the relational join tables and the policy table can never
//!   diverge), and the service simply [`reload`](service::CasbinService::reload)s
//!   the in-memory enforcer after a successful write.
//! - [`rules::Rule`] is the per-route authorization contract, and
//!   [`checker::PermChecker`] is the route → rule registry that the authz
//!   middleware consults. Unregistered protected routes are **denied**
//!   (fail-closed).

pub mod checker;
pub mod model;
pub mod rules;
pub mod service;

pub use checker::PermChecker;
pub use rules::{AuthzOutcome, Rule};
pub use service::CasbinService;
