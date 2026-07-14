//! Research-plane deploy configuration.
//!
//! Process-bound, restart-to-apply settings for the research plane. The
//! artifact store is Local for development or S3-compatible WORM storage for
//! production evidence and model artifacts.

use serde::Deserialize;

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
}
