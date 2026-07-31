//! Immutable candidate-manifest repository.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::{ModelCandidateManifestInfo, NewModelCandidateManifest},
    types::{ContentHash, FeedbackCycleId, ModelCandidateManifestId},
};

#[derive(Debug, Clone)]
pub enum ModelCandidateManifestWriteOutcome {
    Inserted(ModelCandidateManifestInfo),
    AlreadyPresent(ModelCandidateManifestInfo),
}

#[async_trait::async_trait]
pub trait ModelCandidateManifestRepository: Send + Sync {
    async fn insert(
        &self,
        manifest: NewModelCandidateManifest,
    ) -> Result<ModelCandidateManifestWriteOutcome, StorageError>;

    async fn find_by_id(
        &self,
        manifest_id: &ModelCandidateManifestId,
    ) -> Result<Option<ModelCandidateManifestInfo>, StorageError>;

    async fn find_candidate(
        &self,
        feedback_cycle_id: FeedbackCycleId,
        candidate_recipe_hash: ContentHash,
    ) -> Result<Option<ModelCandidateManifestInfo>, StorageError>;
}
