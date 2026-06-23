//! Content-addressed artifact storage.
//!
//! [`ArtifactStore`] abstracts where large research artifacts (training
//! datasets, serialized model weights, backtest reports) live. Postgres stores
//! only metadata + [`ContentHash`](quant_pivot_models::types::ContentHash) +
//! [`ArtifactUri`]; the bytes live behind this trait. The local backend
//! ([`LocalArtifactStore`]) writes `file://` URIs under a deploy-configured
//! root; an object-store backend (`s3://`) slots in later without changing the
//! trait or any caller.

mod local;

pub use local::LocalArtifactStore;

use async_trait::async_trait;
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::types::ArtifactUri;
use std::fmt::{self, Display, Formatter};

/// Logical grouping for an artifact, mapped to a sub-directory / key prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArtifactNamespace {
    /// Frozen training datasets (`datasets/`).
    Dataset,
    /// Serialized model artifacts (`models/`).
    Model,
    /// Point-in-time backtest reports (`backtests/`).
    Backtest,
}

impl ArtifactNamespace {
    /// Stable path segment for this namespace.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Dataset => "datasets",
            Self::Model => "models",
            Self::Backtest => "backtests",
        }
    }
}

impl Display for ArtifactNamespace {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A backend-agnostic artifact key, resolved to a concrete [`ArtifactUri`] by
/// the store.
///
/// The `id` and `extension` are validated to be filesystem/URI-safe (no path
/// separators, traversal, or empty segments) so a crafted key can never escape
/// the store root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactKey {
    namespace: ArtifactNamespace,
    id: String,
    extension: String,
}

impl ArtifactKey {
    /// Validate and build an artifact key.
    ///
    /// # Errors
    ///
    /// Returns [`ResearchError::InvalidArtifactKey`] when `id` or `extension`
    /// is empty or contains a path separator / traversal sequence.
    pub fn new(
        namespace: ArtifactNamespace,
        id: impl Into<String>,
        extension: impl Into<String>,
    ) -> QuantResult<Self> {
        let id = id.into();
        let extension = extension.into();
        Self::validate_segment("id", &id)?;
        Self::validate_segment("extension", &extension)?;
        Ok(Self {
            namespace,
            id,
            extension,
        })
    }

    /// The namespace this key belongs to.
    #[must_use]
    pub const fn namespace(&self) -> ArtifactNamespace {
        self.namespace
    }

    /// Relative path of this key within a store root: `<namespace>/<id>.<ext>`.
    #[must_use]
    pub fn relative_path(&self) -> String {
        format!("{}/{}.{}", self.namespace.as_str(), self.id, self.extension)
    }

    fn validate_segment(field: &'static str, value: &str) -> QuantResult<()> {
        let invalid = value.is_empty()
            || value.contains('/')
            || value.contains('\\')
            || value.contains("..")
            || value.contains('\0');
        if invalid {
            return Err(ResearchError::InvalidArtifactKey {
                detail: format!("{field} must be non-empty and free of path separators: {value:?}"),
            }
            .into());
        }
        Ok(())
    }
}

/// Persists and retrieves artifact bytes behind a content-addressed URI.
///
/// Implementations must be panic-free: a missing artifact or IO fault is a
/// typed [`ResearchError`], never a panic.
#[async_trait]
pub trait ArtifactStore: Send + Sync {
    /// Store `bytes` under `key`, returning the resolved location.
    async fn put(&self, key: ArtifactKey, bytes: &[u8]) -> QuantResult<ArtifactUri>;

    /// Read the bytes previously stored at `uri`.
    async fn get(&self, uri: &ArtifactUri) -> QuantResult<Vec<u8>>;

    /// Whether an artifact exists at `uri`.
    async fn exists(&self, uri: &ArtifactUri) -> QuantResult<bool>;
}
