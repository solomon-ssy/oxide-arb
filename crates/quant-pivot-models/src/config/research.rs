//! Research-plane deploy configuration.
//!
//! Process-bound, restart-to-apply settings for the research plane. The
//! artifact-store root is where the local `ArtifactStore` backend writes
//! training datasets and serialized model artifacts, addressed as
//! `file://<artifact_root>/<namespace>/<id>.<ext>`. Swapping to object storage
//! later adds a sibling section without changing this one.

use serde::Deserialize;

/// Deploy-time research plane settings.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ResearchDeployConfig {
    /// Local filesystem root for the artifact store (datasets, model weights).
    ///
    /// Resolved relative to the process working directory when not absolute.
    pub artifact_root: String,
}

impl Default for ResearchDeployConfig {
    fn default() -> Self {
        Self {
            artifact_root: "./var/artifacts".to_owned(),
        }
    }
}
