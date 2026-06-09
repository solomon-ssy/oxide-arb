//! Cross-cutting actix middleware.
//!
//! All middleware are implemented as `from_fn` adapters (no manual `Transform`
//! boilerplate). [`request_id`] runs app-wide; [`authn`] wraps the protected
//! route scope, and [`authz`] wraps the authorized inner scope nested inside it
//! (so authn always runs first and populates the identity authz consumes).

mod authn;
mod authz;
mod request_id;

pub use authn::authn;
pub use authz::authz;
pub use request_id::request_id;
