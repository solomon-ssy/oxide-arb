//! Strict W4-E07 evidence schema for canonical outcome backfill.

use std::{
    env, fs,
    path::{Path, PathBuf},
};

use chrono::{DateTime, Utc};
use quant_pivot_models::{hashing::CanonicalDigest, types::ContentHash};
use serde::Serialize;

pub const OUTCOME_BACKFILL_EVIDENCE_FORMAT_VERSION: u32 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PreflightAccess {
    ReadOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BackfillExecutionStatus {
    NotExecuted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaMutationAuthority {
    OperatorAuthorizationRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CurrentEnvironmentVerdict {
    pub status: &'static str,
    pub preflight_command: &'static str,
    pub preflight_access: PreflightAccess,
    pub blocker_code: &'static str,
    pub backfill_status: BackfillExecutionStatus,
    pub schema_mutation_authority: SchemaMutationAuthority,
    pub real_outcome_counts: Option<OutcomeCounts>,
    pub real_label_count: Option<u64>,
    pub recovery_commands: [&'static str; 3],
}

impl Default for CurrentEnvironmentVerdict {
    fn default() -> Self {
        Self {
            status: "blocked",
            preflight_command: "cargo xtask postgres-schema verify --config-dir config",
            preflight_access: PreflightAccess::ReadOnly,
            blocker_code: "postgres_schema_identity_mismatch",
            backfill_status: BackfillExecutionStatus::NotExecuted,
            schema_mutation_authority: SchemaMutationAuthority::OperatorAuthorizationRequired,
            real_outcome_counts: None,
            real_label_count: None,
            recovery_commands: [
                "cargo xtask postgres-schema plan --config-dir config",
                "cargo xtask postgres-schema apply --config-dir config",
                "cargo xtask postgres-schema verify --config-dir config",
            ],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct OutcomeCounts {
    pub resolution: u64,
    pub execution: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SourceFrontierEvidence {
    pub source_id: &'static str,
    pub start_block: u64,
    pub target_block: u64,
    pub target_block_hash: String,
    pub target_block_time: DateTime<Utc>,
    pub source_pages: u64,
    pub scanned_blocks: u64,
    pub observations: u64,
    pub unknown_markets: u64,
    pub conflicts: u64,
    pub physical_facts: u64,
    pub logical_facts: u64,
    pub physical_duplicates: u64,
    pub logical_duplicates: u64,
    pub revision_conflicts: u64,
    pub supersession_conflicts: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PlaneCountEvidence {
    pub catalog_scanned: u64,
    pub included: u64,
    pub excluded: u64,
    pub deferred: u64,
    pub conflicts: u64,
    pub physical_rows: u64,
    pub logical_rows: u64,
    pub physical_duplicates: u64,
    pub logical_duplicates: u64,
    pub revision_conflicts: u64,
    pub supersession_conflicts: u64,
}

impl PlaneCountEvidence {
    fn validate(&self) {
        assert_eq!(
            self.catalog_scanned,
            self.included + self.excluded + self.deferred
        );
        assert_eq!(self.conflicts, 0);
        assert_eq!(self.physical_rows, self.logical_rows);
        assert_eq!(self.physical_duplicates, 0);
        assert_eq!(self.logical_duplicates, 0);
        assert_eq!(self.revision_conflicts, 0);
        assert_eq!(self.supersession_conflicts, 0);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AvailabilityEvidence {
    pub first_available_at: DateTime<Utc>,
    pub last_available_at: DateTime<Utc>,
    pub earliest_unchanged_after_replay: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProfileBindingEvidence {
    pub research_profile_artifact_id: String,
    pub research_profile_id: String,
    pub profile_version: u32,
    pub profile_content_hash: String,
    pub resolution_outcomes: u64,
    pub execution_outcomes: u64,
    pub labels_emitted: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReplayEvidence {
    pub resolution_cutoff: DateTime<Utc>,
    pub execution_cutoff: DateTime<Utc>,
    pub first_resolution_inserts: u64,
    pub first_execution_inserts: u64,
    pub replay_resolution_inserts: u64,
    pub replay_execution_inserts: u64,
    pub resolution_rows_before_replay: u64,
    pub resolution_rows_after_replay: u64,
    pub execution_rows_before_replay: u64,
    pub execution_rows_after_replay: u64,
    pub exact_repository_results: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentHashEvidence {
    pub resolution_fact_hashes: Vec<String>,
    pub resolution_outcome_hashes: Vec<String>,
    pub execution_outcome_hashes: Vec<String>,
    pub aggregate_content_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OutcomeBackfillEvidenceInput {
    pub generated_at: DateTime<Utc>,
    pub source_frontier: SourceFrontierEvidence,
    pub resolution_plane: PlaneCountEvidence,
    pub execution_plane: PlaneCountEvidence,
    pub resolution_availability: AvailabilityEvidence,
    pub execution_availability: AvailabilityEvidence,
    pub profile_binding: ProfileBindingEvidence,
    pub replay: ReplayEvidence,
    pub content_hashes: ContentHashEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OutcomeBackfillEvidenceManifest {
    pub format_version: u32,
    pub evidence_scope: &'static str,
    pub generated_at: DateTime<Utc>,
    pub operational_activation_claimed: bool,
    pub usable_for_crypto_readiness: bool,
    pub usable_for_weather_readiness: bool,
    pub current_environment: CurrentEnvironmentVerdict,
    pub source_frontier: SourceFrontierEvidence,
    pub resolution_plane: PlaneCountEvidence,
    pub execution_plane: PlaneCountEvidence,
    pub resolution_availability: AvailabilityEvidence,
    pub execution_availability: AvailabilityEvidence,
    pub outcome_counts: OutcomeCounts,
    pub label_count: u64,
    pub profile_binding: ProfileBindingEvidence,
    pub replay: ReplayEvidence,
    pub content_hashes: ContentHashEvidence,
}

impl OutcomeBackfillEvidenceManifest {
    pub fn new(input: OutcomeBackfillEvidenceInput) -> Self {
        let manifest = Self {
            format_version: OUTCOME_BACKFILL_EVIDENCE_FORMAT_VERSION,
            evidence_scope: "disposable_contract_with_current_environment_preflight",
            generated_at: input.generated_at,
            operational_activation_claimed: false,
            usable_for_crypto_readiness: false,
            usable_for_weather_readiness: false,
            current_environment: CurrentEnvironmentVerdict::default(),
            source_frontier: input.source_frontier,
            resolution_plane: input.resolution_plane,
            execution_plane: input.execution_plane,
            resolution_availability: input.resolution_availability,
            execution_availability: input.execution_availability,
            outcome_counts: OutcomeCounts {
                resolution: input.profile_binding.resolution_outcomes,
                execution: input.profile_binding.execution_outcomes,
            },
            label_count: input.profile_binding.labels_emitted,
            profile_binding: input.profile_binding,
            replay: input.replay,
            content_hashes: input.content_hashes,
        };
        manifest.validate();
        manifest
    }

    pub fn validate(&self) {
        assert!(!self.operational_activation_claimed);
        assert!(!self.usable_for_crypto_readiness);
        assert!(!self.usable_for_weather_readiness);
        assert_eq!(self.current_environment.status, "blocked");
        assert_eq!(
            self.current_environment.preflight_access,
            PreflightAccess::ReadOnly
        );
        assert_eq!(
            self.current_environment.backfill_status,
            BackfillExecutionStatus::NotExecuted
        );
        assert_eq!(
            self.current_environment.schema_mutation_authority,
            SchemaMutationAuthority::OperatorAuthorizationRequired
        );
        assert_eq!(self.current_environment.real_outcome_counts, None);
        assert_eq!(self.current_environment.real_label_count, None);
        assert!(self.source_frontier.start_block < self.source_frontier.target_block);
        assert_eq!(
            self.source_frontier.scanned_blocks,
            self.source_frontier.target_block - self.source_frontier.start_block
        );
        assert_eq!(self.source_frontier.conflicts, 0);
        assert_eq!(
            self.source_frontier.physical_facts,
            self.source_frontier.logical_facts
        );
        assert_eq!(self.source_frontier.physical_duplicates, 0);
        assert_eq!(self.source_frontier.logical_duplicates, 0);
        assert_eq!(self.source_frontier.revision_conflicts, 0);
        assert_eq!(self.source_frontier.supersession_conflicts, 0);
        self.resolution_plane.validate();
        self.execution_plane.validate();
        assert_eq!(
            self.resolution_plane.included,
            self.outcome_counts.resolution
        );
        assert_eq!(self.execution_plane.included, self.outcome_counts.execution);
        assert_eq!(self.label_count, 0);
        assert_eq!(self.profile_binding.labels_emitted, 0);
        assert!(
            self.resolution_availability.first_available_at
                <= self.resolution_availability.last_available_at
        );
        assert!(
            self.execution_availability.first_available_at
                <= self.execution_availability.last_available_at
        );
        assert!(self.resolution_availability.earliest_unchanged_after_replay);
        assert!(self.execution_availability.earliest_unchanged_after_replay);
        assert_eq!(self.replay.replay_resolution_inserts, 0);
        assert_eq!(self.replay.replay_execution_inserts, 0);
        assert_eq!(
            self.replay.resolution_rows_before_replay,
            self.replay.resolution_rows_after_replay
        );
        assert_eq!(
            self.replay.execution_rows_before_replay,
            self.replay.execution_rows_after_replay
        );
        assert_eq!(
            self.replay.exact_repository_results,
            self.outcome_counts.resolution + self.outcome_counts.execution
        );
        assert_eq!(
            self.content_hashes.resolution_fact_hashes.len(),
            usize::try_from(self.source_frontier.observations)
                .expect("resolution observation count fits usize")
        );
        assert_eq!(
            self.content_hashes.resolution_outcome_hashes.len(),
            usize::try_from(self.outcome_counts.resolution)
                .expect("resolution outcome count fits usize")
        );
        assert_eq!(
            self.content_hashes.execution_outcome_hashes.len(),
            usize::try_from(self.outcome_counts.execution)
                .expect("execution outcome count fits usize")
        );
        assert!(ContentHash::parse(&self.content_hashes.aggregate_content_hash).is_ok());
    }

    pub fn write(&self) -> EvidenceArtifact {
        let output_dir = evidence_output_dir();
        fs::create_dir_all(&output_dir).unwrap_or_else(|error| {
            panic!(
                "create W4-E07 evidence directory {}: {error}",
                output_dir.display()
            )
        });
        let path = output_dir.join("outcome-backfill-evidence-v3.json");
        let mut bytes =
            serde_json::to_vec_pretty(self).expect("serialize W4-E07 outcome backfill evidence");
        bytes.push(b'\n');
        fs::write(&path, &bytes)
            .unwrap_or_else(|error| panic!("write W4-E07 evidence {}: {error}", path.display()));
        let content_hash = CanonicalDigest::content_hash_bytes(&bytes).to_string();
        eprintln!(
            "W4-E07 outcome backfill evidence: path={} content_hash={content_hash}",
            path.display()
        );
        EvidenceArtifact { path, content_hash }
    }
}

#[derive(Debug)]
pub struct EvidenceArtifact {
    pub path: PathBuf,
    pub content_hash: String,
}

fn evidence_output_dir() -> PathBuf {
    env::var_os("W4_E07_EVIDENCE_DIR").map_or_else(
        || Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/phase-11.9/w4-e07"),
        PathBuf::from,
    )
}
