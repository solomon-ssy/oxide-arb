//! Live schema verification seam used while the lifecycle lease is held.

use async_trait::async_trait;
use quant_pivot_error::QuantResult;

use crate::types::{ArtifactUri, ContentHash};

/// Exact `PostgreSQL` and `ClickHouse` schema fingerprints observed in one preflight.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedSchemaFingerprints {
    pub postgres_schema_fingerprint: ContentHash,
    pub clickhouse_schema_fingerprint: ContentHash,
}

/// Read-only live schema verification that can run inside the production-seal lease.
#[async_trait]
pub trait LifecycleSchemaVerificationPort: Send + Sync {
    async fn verify_live(&self) -> QuantResult<VerifiedSchemaFingerprints>;
}

/// Held deployment-wide lifecycle lease. Mutation callers must race their
/// write future against [`Self::cancelled`] and release explicitly.
#[async_trait]
pub trait LifecycleLeaseGuardPort: Send {
    async fn cancelled(&self);
    fn ensure_active(&self) -> QuantResult<()>;
    async fn release(self: Box<Self>) -> QuantResult<()>;
}

/// Acquire the canonical lifecycle lease used by schema/reset/seal mutations.
#[async_trait]
pub trait LifecycleLeaseProviderPort: Send + Sync {
    async fn acquire(&self) -> QuantResult<Box<dyn LifecycleLeaseGuardPort>>;
}

/// Re-hash a referenced immutable evidence artifact at the final persistence boundary.
#[async_trait]
pub trait ProductionEvidenceArtifactVerificationPort: Send + Sync {
    async fn verify_artifact(
        &self,
        artifact_uri: &ArtifactUri,
        expected_hash: &ContentHash,
    ) -> QuantResult<()>;
}
