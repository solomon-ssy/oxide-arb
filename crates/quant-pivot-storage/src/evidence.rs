//! Immutable local artifact verification for production lifecycle evidence.

use std::os::unix::fs::PermissionsExt;

use async_trait::async_trait;
use blake3::Hasher;
use quant_pivot_error::{QuantResult, storage::StorageError};
use quant_pivot_models::{
    domain::ports::ProductionEvidenceArtifactVerificationPort,
    types::{ArtifactUri, ContentHash},
};
use tokio::{fs::File, io::AsyncReadExt};
use url::Url;

pub struct FileProductionEvidenceVerifier;

#[async_trait]
impl ProductionEvidenceArtifactVerificationPort for FileProductionEvidenceVerifier {
    async fn verify_artifact(
        &self,
        artifact_uri: &ArtifactUri,
        expected_hash: &ContentHash,
    ) -> QuantResult<()> {
        let url = Url::parse(artifact_uri.as_str()).map_err(|error| {
            StorageError::invariant_violation(
                Some("system_production_evidence"),
                format!("invalid evidence artifact URI: {error}"),
            )
        })?;
        let path = url.to_file_path().map_err(|()| {
            StorageError::invariant_violation(
                Some("system_production_evidence"),
                "production evidence currently requires a local file:// artifact",
            )
        })?;
        let metadata = tokio::fs::symlink_metadata(&path).await.map_err(|error| {
            StorageError::invariant_violation(
                Some("system_production_evidence"),
                format!("evidence artifact metadata is unavailable: {error}"),
            )
        })?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(StorageError::invariant_violation(
                Some("system_production_evidence"),
                "evidence artifact must be a regular non-symlink file",
            )
            .into());
        }
        if metadata.permissions().mode() & 0o222 != 0 {
            return Err(StorageError::invariant_violation(
                Some("system_production_evidence"),
                "evidence artifact must be immutable to all users (mode 0444 or stricter)",
            )
            .into());
        }

        let mut file = File::open(&path).await.map_err(|error| {
            StorageError::invariant_violation(
                Some("system_production_evidence"),
                format!("evidence artifact cannot be opened: {error}"),
            )
        })?;
        let mut hasher = Hasher::new();
        let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
        loop {
            let read = file.read(&mut buffer).await.map_err(|error| {
                StorageError::invariant_violation(
                    Some("system_production_evidence"),
                    format!("evidence artifact cannot be read: {error}"),
                )
            })?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        let actual_hash = ContentHash::parse(format!("blake3:{}", hasher.finalize().to_hex()))
            .map_err(|error| {
                StorageError::invariant_violation(
                    Some("system_production_evidence"),
                    format!("computed evidence hash is invalid: {error}"),
                )
            })?;
        if &actual_hash != expected_hash {
            return Err(StorageError::state_conflict(
                "system_production_evidence",
                Some(artifact_uri),
                "evidence artifact content hash mismatch",
            )
            .into());
        }
        Ok(())
    }
}
