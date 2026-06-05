//! Governance services: publication hashing, publication assembly/gates, and
//! the `ControlFactorRegistry` orchestration over the repository.

mod hash;
mod publication;
mod service;

pub use hash::PublicationHasher;
pub use publication::{PublicationDraft, PublicationManager};
pub use service::{ControlFactorRegistry, PublicationRequest};
