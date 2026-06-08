//! Cross-cutting actix middleware.
//!
//! Both middleware are implemented as `from_fn` adapters (no manual `Transform`
//! boilerplate). [`request_id`] runs app-wide; [`authn`] wraps only the
//! protected route scope.

mod authn;
mod request_id;

pub use authn::authn;
pub use request_id::request_id;
