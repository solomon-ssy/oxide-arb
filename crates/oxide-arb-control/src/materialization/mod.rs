//! Offline point-in-time materialization orchestration.

mod hash;
mod manifest;
mod pit;
mod runner;
mod stage;

pub use hash::{ArtifactHasher, DedupeKeyHasher, ManifestHasher};
pub use manifest::{ManifestBuilder, ManifestBuilderInput, SealedMaterializationManifest};
pub use oxide_arb_error::control::{MaterializationError, MaterializationResult};
pub use pit::{PointInTimeResolver, ResolverRepositories};
pub use runner::{MaterializationRunner, MaterializationRunnerDeps, RunExecutionOutcome};
pub use stage::{MaterializationStage, StageExecutionContext, StageReportBuilder};
