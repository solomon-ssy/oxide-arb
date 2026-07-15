//! Content-addressed artifact storage.
//!
//! [`ArtifactStore`] abstracts where large research artifacts (training
//! datasets, serialized model weights, backtest reports) live. Postgres stores
//! only metadata + [`ContentHash`](quant_pivot_models::types::ContentHash) +
//! [`ArtifactUri`]; the bytes live behind this trait. The local backend
//! ([`LocalArtifactStore`]) writes `file://` URIs under a deploy-configured
//! root; production evidence uses a versioned, Object-Lock-enabled S3-compatible
//! backend.

mod local;
mod s3;

pub use local::LocalArtifactStore;
pub use s3::S3ArtifactStore;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{Stream, StreamExt, stream};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    config::{ArtifactStoreDeployConfig, ArtifactStoreKind},
    types::ArtifactUri,
};
use std::{
    fmt::{self, Display, Formatter},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

pub type ArtifactByteStream = Pin<Box<dyn Stream<Item = QuantResult<Bytes>> + Send>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArtifactDurability {
    pub remote: bool,
    pub versioned: bool,
    pub object_locked: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactObjectMetadata {
    pub byte_size: u64,
    pub etag: Option<String>,
    pub version_id: Option<String>,
    pub durability: ArtifactDurability,
}

impl ArtifactDurability {
    #[must_use]
    pub const fn permits_production_publish(self) -> bool {
        self.remote && self.versioned && self.object_locked
    }
}

/// Logical grouping for an artifact, mapped to a sub-directory / key prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArtifactNamespace {
    /// Frozen training datasets (`datasets/`).
    Dataset,
    /// Serialized model artifacts (`models/`).
    Model,
    /// Point-in-time backtest reports (`backtests/`).
    Backtest,
    /// Row-level executable-policy evidence bundles.
    PolicyEvidence,
    /// Immutable point-in-time source objects and manifests.
    SourceSlice,
    /// Atomic report-fact outbox bundles awaiting verified `ClickHouse` delivery.
    ReportFacts,
    /// Signed operational-readiness observations consumed by fit preflight.
    ReadinessEvidence,
}

impl ArtifactNamespace {
    /// Stable path segment for this namespace.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Dataset => "datasets",
            Self::Model => "models",
            Self::Backtest => "backtests",
            Self::PolicyEvidence => "policy-evidence",
            Self::SourceSlice => "source-slices",
            Self::ReportFacts => "report-facts",
            Self::ReadinessEvidence => "readiness-evidence",
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
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
            || value.contains("..");
        if invalid {
            return Err(ResearchError::InvalidArtifactKey {
                detail: format!(
                    "{field} must use ASCII letters, digits, dot, dash, or underscore without traversal: {value:?}"
                ),
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
    /// Stream an artifact into an atomic backend object.
    async fn put_stream(
        &self,
        key: ArtifactKey,
        stream: ArtifactByteStream,
    ) -> QuantResult<ArtifactUri>;

    /// Stream an artifact without materializing it as one process-sized buffer.
    async fn get_stream(&self, uri: &ArtifactUri) -> QuantResult<ArtifactByteStream>;

    /// Prove the durability properties required by a publication boundary.
    async fn durability(&self, uri: &ArtifactUri) -> QuantResult<ArtifactDurability>;

    /// Read immutable object identity used by evidence manifests.
    async fn metadata(&self, uri: &ArtifactUri) -> QuantResult<ArtifactObjectMetadata>;

    /// Issue a short-lived backend-signed GET URL. Production callers never
    /// expose canonical bucket URIs or proxy multi-gigabyte evidence through
    /// the application process.
    async fn signed_download_url(
        &self,
        uri: &ArtifactUri,
        valid_for: Duration,
    ) -> QuantResult<String>;

    /// Convenience for small objects; large evidence paths call
    /// [`Self::put_stream`] directly.
    async fn put(&self, key: ArtifactKey, bytes: &[u8]) -> QuantResult<ArtifactUri> {
        let bytes = Bytes::copy_from_slice(bytes);
        self.put_stream(key, Box::pin(stream::once(async move { Ok(bytes) })))
            .await
    }

    /// Convenience for bounded metadata/model objects.
    async fn get(&self, uri: &ArtifactUri) -> QuantResult<Vec<u8>> {
        let mut stream = self.get_stream(uri).await?;
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            bytes.extend_from_slice(&chunk?);
        }
        Ok(bytes)
    }

    /// Whether an artifact exists at `uri`.
    async fn exists(&self, uri: &ArtifactUri) -> QuantResult<bool>;

    /// Read the bytes stored under a content-addressed `key`, without a recorded
    /// URI. The key resolves to the same location [`Self::put`] would write, so a
    /// content-addressed artifact is retrievable from its hash alone.
    async fn get_by_key(&self, key: &ArtifactKey) -> QuantResult<Vec<u8>>;

    /// Whether an artifact exists under a content-addressed `key`.
    async fn exists_by_key(&self, key: &ArtifactKey) -> QuantResult<bool>;
}

pub fn build_artifact_store(
    config: &ArtifactStoreDeployConfig,
) -> QuantResult<Arc<dyn ArtifactStore>> {
    match config.kind {
        ArtifactStoreKind::Local => Ok(Arc::new(LocalArtifactStore::new(&config.prefix))),
        ArtifactStoreKind::S3 => Ok(Arc::new(S3ArtifactStore::new(config)?)),
    }
}
