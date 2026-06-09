//! Authorization building blocks.
//!
//! Authentication (JWT, password, blacklist) lives in [`crate::jwt`]; this
//! module owns *authorization* — the Casbin enforcer wrapper, the route-level
//! permission registry, and the rule DSL that the authz middleware evaluates.

pub mod casbin;
