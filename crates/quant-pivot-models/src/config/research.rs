//! Research-plane deploy configuration.
//!
//! Process-bound, restart-to-apply settings for the research plane. The
//! artifact store is Local for development or S3-compatible WORM storage for
//! production evidence and model artifacts.

use serde::Deserialize;

use super::secret::SecretText;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactStoreKind {
    Local,
    S3,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ArtifactStoreDeployConfig {
    pub kind: ArtifactStoreKind,
    pub bucket: String,
    /// Object key prefix, or the Local filesystem root.
    pub prefix: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub path_style: bool,
    pub require_object_lock: bool,
    pub require_versioning: bool,
}

impl Default for ArtifactStoreDeployConfig {
    fn default() -> Self {
        Self {
            kind: ArtifactStoreKind::Local,
            bucket: String::new(),
            prefix: "./var/artifacts".to_owned(),
            region: "us-east-1".to_owned(),
            endpoint: None,
            path_style: true,
            require_object_lock: false,
            require_versioning: false,
        }
    }
}

/// Deploy-time research plane settings.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ResearchDeployConfig {
    pub artifact_store: ArtifactStoreDeployConfig,
    pub evidence_attestation: EvidenceAttestationConfig,
    pub model_serving_registry: ModelServingRegistryConfig,
}

/// Restart-to-apply budgets for immutable model-serving runtime loads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ModelServingRegistryConfig {
    /// Maximum successfully validated contracts retained in process.
    pub max_cached_contracts: u64,
    /// Maximum cache-miss callers admitted across active and queued loads.
    pub max_pending_loads: usize,
    /// Maximum distinct cold loads performing repository/object-store I/O.
    pub max_concurrent_loads: usize,
    /// End-to-end deadline for one cold contract load.
    pub load_timeout_ms: u64,
    /// Total resident-memory reservation available to route-owned shadow
    /// bindings across all Buy routes.
    pub max_total_shadow_model_bytes: u64,
}

impl Default for ModelServingRegistryConfig {
    fn default() -> Self {
        Self {
            max_cached_contracts: 32,
            max_pending_loads: 64,
            max_concurrent_loads: 4,
            load_timeout_ms: 60_000,
            max_total_shadow_model_bytes: 2_147_483_648,
        }
    }
}

/// Dedicated keyed-BLAKE3 attestation identity for operational evidence.
/// This key is separate from venue signing and JWT credentials.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EvidenceAttestationConfig {
    /// Active lowercase-hex encoded 32-byte keyed-BLAKE3 key.
    pub signing_key: SecretText,
    /// Historical verification-only keys, newest first.
    pub previous_signing_keys: Vec<SecretText>,
}
