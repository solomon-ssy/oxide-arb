//! Seed graph and seed context errors.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SeedError {
    #[error("missing seed artifact `{key}`")]
    MissingArtifact { key: &'static str },

    #[error("seed artifact `{key}` has unexpected type")]
    TypeMismatch { key: &'static str },

    #[error("seed graph contains duplicate artifact producer for `{key}`")]
    DuplicateArtifactProducer { key: &'static str },

    #[error("seed graph dependency `{dependency}` has no producer")]
    MissingDependency { dependency: &'static str },

    #[error("seed graph contains a cycle")]
    Cycle,
}
