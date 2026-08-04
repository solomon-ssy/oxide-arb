//! Machine-readable W4 decision-path evidence produced by the canonical fixtures.

use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::{Path, PathBuf},
};

use quant_pivot_models::{hashing::CanonicalDigest, types::ContentHash};
use serde::Serialize;
use serde_json::Value;

pub const DECISION_PATH_EVIDENCE_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionPath {
    NoAction,
    ChallengerRejected,
    CandidateReady,
    Promoted,
}

impl DecisionPath {
    #[must_use]
    pub const fn expected_decision(self) -> &'static str {
        match self {
            Self::NoAction => "no_action",
            Self::ChallengerRejected => "challenger_rejected",
            Self::CandidateReady => "candidate_ready",
            Self::Promoted => "promoted",
        }
    }

    #[must_use]
    pub const fn requires_permit(self) -> bool {
        matches!(self, Self::CandidateReady | Self::Promoted)
    }

    #[must_use]
    pub const fn requires_promotion(self) -> bool {
        matches!(self, Self::Promoted)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ExactDecisionIdentifiers {
    pub feedback_cycle_id: String,
    pub research_profile_artifact_id: String,
    pub profile_hash: String,
    pub champion_model_version_id: String,
    pub champion_model_spec_id: String,
    pub champion_training_dataset_id: Option<String>,
    pub candidate_model_version_id: String,
    pub candidate_model_spec_id: String,
    pub candidate_training_dataset_id: Option<String>,
    pub policy_generation_before: String,
    pub policy_generation_after: String,
    pub policy_snapshot_id_before: String,
    pub policy_snapshot_id_after: String,
    pub model_routing_revision_before: Option<String>,
    pub model_routing_revision_after: Option<String>,
    pub promotion_permit_id: Option<String>,
    pub policy_activation_id: Option<String>,
    pub promotion_transaction_hash: Option<String>,
    pub target_category: Option<String>,
    pub route_champion_before: Option<String>,
    pub route_champion_after: Option<String>,
    pub route_shadow_before: Option<String>,
    pub route_shadow_after: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DecisionArtifactEvidence {
    pub artifact_id: String,
    pub uri: String,
    pub bytes_hash: String,
    pub semantic_hash: String,
    pub decision_job_input_hash: Option<String>,
    pub champion_serving_contract_hash: String,
    pub candidate_serving_contract_hash: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PermitEvidence {
    pub persisted_permit: Value,
    pub status_at_action: String,
    pub lifecycle: PermitLifecycleEvidence,
    pub bindings: PermitBindingEvidence,
}

#[derive(Debug, Clone, Serialize)]
pub struct PermitLifecycleEvidence {
    pub active: bool,
    pub not_expired: bool,
    pub not_revoked: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct PermitBindingEvidence {
    pub scope_exact: bool,
    pub preflight_hash_exact: bool,
    pub runtime_mode_exact: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DeploymentAuthorityBoundary {
    pub signing_and_funder_owner: &'static str,
    pub mutation_surface_in_promotion_dependencies: bool,
    pub private_key_material_recorded: bool,
    pub funder_value_recorded: bool,
}

impl Default for DeploymentAuthorityBoundary {
    fn default() -> Self {
        Self {
            signing_and_funder_owner: "immutable_boot_deploy_config",
            mutation_surface_in_promotion_dependencies: false,
            private_key_material_recorded: false,
            funder_value_recorded: false,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct InvariantSnapshot {
    pub cycle: Value,
    pub policy_bundle: Value,
    pub runtime_control: Value,
    pub model_routes: Value,
    pub capital_allocations: Value,
    pub champion_model: Value,
    pub candidate_model: Value,
    pub parity_latch: Value,
    pub in_memory_serving_route: Value,
    pub policy_apply_readiness: Value,
    pub deployment_authority: DeploymentAuthorityBoundary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InvariantChange {
    pub path: String,
    pub before: Value,
    pub after: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InvariantDiff {
    pub changes: Vec<InvariantChange>,
}

impl InvariantDiff {
    pub fn between(before: &InvariantSnapshot, after: &InvariantSnapshot) -> Self {
        let before = serde_json::to_value(before).expect("serialize before invariant snapshot");
        let after = serde_json::to_value(after).expect("serialize after invariant snapshot");
        let mut changes = Vec::new();
        collect_changes("$", &before, &after, &mut changes);
        Self { changes }
    }

    #[must_use]
    pub fn any_below(&self, path: &str) -> bool {
        let nested = format!("{path}.");
        self.changes
            .iter()
            .any(|change| change.path == path || change.path.starts_with(&nested))
    }
}

fn collect_changes(path: &str, before: &Value, after: &Value, changes: &mut Vec<InvariantChange>) {
    match (before, after) {
        (Value::Object(before), Value::Object(after)) => {
            let keys = before
                .keys()
                .chain(after.keys())
                .cloned()
                .collect::<BTreeSet<_>>();
            for key in keys {
                let child = format!("{path}.{key}");
                collect_changes(
                    &child,
                    before.get(&key).unwrap_or(&Value::Null),
                    after.get(&key).unwrap_or(&Value::Null),
                    changes,
                );
            }
        }
        _ if before != after => changes.push(InvariantChange {
            path: path.to_owned(),
            before: before.clone(),
            after: after.clone(),
        }),
        _ => {}
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TimelineEventEvidence {
    pub sequence: i64,
    pub stage: String,
    pub event_kind: String,
    pub research_job_id: Option<String>,
    pub actor: Option<String>,
    pub reason_code: Option<String>,
    pub evidence_uri: Option<String>,
    pub evidence_hash: Option<String>,
    pub event_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RowCountSnapshot {
    pub rows: BTreeMap<String, u64>,
}

impl RowCountSnapshot {
    #[must_use]
    pub fn inserted_since(&self, previous: &Self) -> BTreeMap<String, u64> {
        self.rows
            .iter()
            .map(|(table, current)| {
                let previous = previous.rows.get(table).copied().unwrap_or_default();
                assert!(
                    *current >= previous,
                    "row count for {table} regressed from {previous} to {current}"
                );
                (table.clone(), current - previous)
            })
            .collect()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ReplayEvidence {
    pub first_result: String,
    pub exact_replay_result: String,
    pub counts_before: RowCountSnapshot,
    pub counts_after_first: RowCountSnapshot,
    pub counts_after_exact_replay: RowCountSnapshot,
    pub inserted_by_first: BTreeMap<String, u64>,
    pub inserted_by_exact_replay: BTreeMap<String, u64>,
}

impl ReplayEvidence {
    pub fn new(
        first_result: impl Into<String>,
        exact_replay_result: impl Into<String>,
        counts_before: RowCountSnapshot,
        counts_after_first: RowCountSnapshot,
        counts_after_exact_replay: RowCountSnapshot,
    ) -> Self {
        let inserted_by_first = counts_after_first.inserted_since(&counts_before);
        let inserted_by_exact_replay =
            counts_after_exact_replay.inserted_since(&counts_after_first);
        assert!(
            inserted_by_exact_replay.values().all(|count| *count == 0),
            "an exact decision-path replay inserted durable rows"
        );
        Self {
            first_result: first_result.into(),
            exact_replay_result: exact_replay_result.into(),
            counts_before,
            counts_after_first,
            counts_after_exact_replay,
            inserted_by_first,
            inserted_by_exact_replay,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RestartReadBackEvidence {
    pub fresh_repository_owners: bool,
    pub fresh_stage_adapter: bool,
    pub exact_match: bool,
    pub canonical_read_back_hash: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DecisionPathEvidence {
    pub path: DecisionPath,
    pub decision: String,
    pub decision_reason: String,
    pub exact_ids: ExactDecisionIdentifiers,
    pub decision_artifact: DecisionArtifactEvidence,
    pub permit: Option<PermitEvidence>,
    pub worm_timeline: Vec<TimelineEventEvidence>,
    pub before: InvariantSnapshot,
    pub after: InvariantSnapshot,
    pub invariant_diff: InvariantDiff,
    pub replay: ReplayEvidence,
    pub restart_read_back: RestartReadBackEvidence,
    pub fault_injection_rollback_verified: bool,
}

impl DecisionPathEvidence {
    fn validate_hashes(&self) {
        ContentHash::parse(&self.decision_artifact.bytes_hash)
            .expect("decision bytes hash uses canonical BLAKE3 text");
        ContentHash::parse(&self.decision_artifact.semantic_hash)
            .expect("decision semantic hash uses canonical BLAKE3 text");
        ContentHash::parse(&self.restart_read_back.canonical_read_back_hash)
            .expect("restart read-back hash uses canonical BLAKE3 text");
    }

    fn validate_identity(&self) {
        assert_eq!(self.decision, self.path.expected_decision());
        assert!(!self.decision_reason.trim().is_empty());
        assert!(!self.exact_ids.feedback_cycle_id.trim().is_empty());
        assert!(!self.decision_artifact.uri.trim().is_empty());
        self.validate_hashes();
        assert_eq!(self.permit.is_some(), self.path.requires_permit());
        assert_eq!(
            self.exact_ids.promotion_permit_id.is_some(),
            self.path.requires_permit()
        );
        assert_eq!(
            self.exact_ids.policy_activation_id.is_some(),
            self.path.requires_promotion()
        );
        assert_eq!(
            self.exact_ids.promotion_transaction_hash.is_some(),
            self.path.requires_promotion()
        );
    }

    fn validate_route_state(&self) {
        if self.path.requires_promotion() {
            assert_eq!(
                self.exact_ids.route_champion_before,
                Some(self.exact_ids.champion_model_version_id.clone())
            );
            assert_eq!(
                self.exact_ids.route_champion_after,
                Some(self.exact_ids.candidate_model_version_id.clone())
            );
            assert_eq!(
                self.exact_ids.route_shadow_before,
                Some(self.exact_ids.candidate_model_version_id.clone())
            );
            assert!(self.exact_ids.route_shadow_after.is_none());
            assert_ne!(
                self.exact_ids.policy_generation_before,
                self.exact_ids.policy_generation_after
            );
            assert_ne!(
                self.exact_ids.policy_snapshot_id_before,
                self.exact_ids.policy_snapshot_id_after
            );
            assert_ne!(
                self.exact_ids.model_routing_revision_before,
                self.exact_ids.model_routing_revision_after
            );
        } else {
            assert_eq!(
                self.exact_ids.route_champion_before,
                self.exact_ids.route_champion_after
            );
            assert_eq!(
                self.exact_ids.route_shadow_before,
                self.exact_ids.route_shadow_after
            );
            assert_eq!(
                self.exact_ids.policy_generation_before,
                self.exact_ids.policy_generation_after
            );
            assert_eq!(
                self.exact_ids.policy_snapshot_id_before,
                self.exact_ids.policy_snapshot_id_after
            );
            assert_eq!(
                self.exact_ids.model_routing_revision_before,
                self.exact_ids.model_routing_revision_after
            );
        }
    }

    fn validate_permit(&self) {
        assert_eq!(
            self.fault_injection_rollback_verified,
            self.path.requires_promotion()
        );
        if let Some(permit) = &self.permit {
            assert_eq!(permit.status_at_action, "active");
            assert!(permit.lifecycle.active);
            assert!(permit.lifecycle.not_expired);
            assert!(permit.lifecycle.not_revoked);
            assert!(permit.bindings.scope_exact);
            assert!(permit.bindings.preflight_hash_exact);
            assert!(permit.bindings.runtime_mode_exact);
        }
    }

    fn validate_replay(&self) {
        assert!(self.restart_read_back.fresh_repository_owners);
        assert!(self.restart_read_back.fresh_stage_adapter);
        assert!(self.restart_read_back.exact_match);
        assert!(
            self.replay
                .inserted_by_exact_replay
                .values()
                .all(|count| *count == 0)
        );
    }

    fn validate_timeline(&self) {
        assert!(!self.worm_timeline.is_empty());
        assert!(
            self.worm_timeline
                .windows(2)
                .all(|events| events[0].sequence < events[1].sequence)
        );
        let expected_stages = match self.path {
            DecisionPath::NoAction | DecisionPath::CandidateReady | DecisionPath::Promoted => [
                "trigger",
                "truth_freeze",
                "coverage",
                "attribution",
                "drift",
                "recipe_plan",
                "dataset_seal",
                "training",
                "calibration",
                "cpcv",
                "validation",
                "comparison",
                "shadow_bind",
                "shadow",
                "decision",
            ]
            .as_slice(),
            DecisionPath::ChallengerRejected => [
                "trigger",
                "truth_freeze",
                "coverage",
                "attribution",
                "drift",
                "recipe_plan",
                "dataset_seal",
                "training",
                "calibration",
                "cpcv",
                "validation",
                "comparison",
            ]
            .as_slice(),
        };
        assert_eq!(self.worm_timeline.len(), expected_stages.len());
        for (index, (event, expected_stage)) in
            self.worm_timeline.iter().zip(expected_stages).enumerate()
        {
            assert_eq!(
                event.sequence,
                i64::try_from(index + 1).expect("timeline sequence fits i64")
            );
            assert_eq!(&event.stage, expected_stage);
        }
        let terminal_event = self
            .worm_timeline
            .last()
            .expect("W4-E04 timeline has a terminal event");
        let expected_terminal_stage = if self.path == DecisionPath::ChallengerRejected {
            "comparison"
        } else {
            "decision"
        };
        assert_eq!(terminal_event.stage, expected_terminal_stage);
        assert_eq!(terminal_event.event_kind, "succeeded");
        assert_eq!(
            terminal_event.evidence_hash.as_deref(),
            Some(self.decision_artifact.bytes_hash.as_str())
        );
    }

    fn validate_insertions(&self) {
        let nonzero_insertions = self
            .replay
            .inserted_by_first
            .iter()
            .filter(|(_, count)| **count > 0)
            .map(|(table, count)| (table.as_str(), *count))
            .collect::<BTreeMap<_, _>>();
        let expected_insertions = match self.path {
            DecisionPath::NoAction => BTreeMap::from([
                ("quant_feedback_event_outbox", 1),
                ("quant_feedback_stage_event", 1),
                ("quant_research_job", 1),
            ]),
            DecisionPath::ChallengerRejected => BTreeMap::new(),
            DecisionPath::CandidateReady => BTreeMap::from([
                ("quant_feedback_event_outbox", 1),
                ("quant_feedback_promotion_permit", 1),
                ("quant_feedback_stage_event", 1),
                ("quant_research_job", 1),
            ]),
            DecisionPath::Promoted => BTreeMap::from([
                ("decision_policy_snapshot", 1),
                ("policy_activation", 1),
                ("policy_activation_audit", 1),
                ("policy_activation_event_outbox", 1),
                ("policy_approval", 1),
                ("policy_revision", 1),
                ("quant_model_governance_audit", 1),
            ]),
        };
        assert_eq!(nonzero_insertions, expected_insertions);
    }

    fn validate_invariants(&self) {
        let recomputed = InvariantDiff::between(&self.before, &self.after);
        assert_eq!(recomputed, self.invariant_diff);
        assert_eq!(
            self.before.deployment_authority,
            DeploymentAuthorityBoundary::default()
        );
        assert_eq!(
            self.after.deployment_authority,
            DeploymentAuthorityBoundary::default()
        );
    }

    pub fn validate(&self) {
        self.validate_identity();
        self.validate_route_state();
        self.validate_permit();
        self.validate_replay();
        self.validate_timeline();
        self.validate_insertions();
        self.validate_invariants();
    }
}

#[derive(Debug, Serialize)]
pub struct DecisionPathEvidenceManifest {
    pub format_version: u32,
    pub operational_activation_claimed: bool,
    pub postgres_transaction_reference: &'static str,
    pub postgres_row_lock_reference: &'static str,
    pub paths: Vec<DecisionPathEvidence>,
}

impl DecisionPathEvidenceManifest {
    pub fn new(paths: Vec<DecisionPathEvidence>) -> Self {
        let manifest = Self {
            format_version: DECISION_PATH_EVIDENCE_FORMAT_VERSION,
            operational_activation_claimed: false,
            postgres_transaction_reference: "https://www.postgresql.org/docs/current/tutorial-transactions.html",
            postgres_row_lock_reference: "https://www.postgresql.org/docs/current/explicit-locking.html#LOCKING-ROWS",
            paths,
        };
        manifest.validate();
        manifest
    }

    pub fn validate(&self) {
        assert_eq!(
            self.paths.iter().map(|path| path.path).collect::<Vec<_>>(),
            vec![
                DecisionPath::NoAction,
                DecisionPath::ChallengerRejected,
                DecisionPath::CandidateReady,
                DecisionPath::Promoted,
            ]
        );
        assert!(!self.operational_activation_claimed);
        for path in &self.paths {
            path.validate();
        }
    }

    pub fn write(&self) -> EvidenceArtifact {
        let output_dir = evidence_output_dir();
        fs::create_dir_all(&output_dir).unwrap_or_else(|error| {
            panic!(
                "create W4-E04 evidence directory {}: {error}",
                output_dir.display()
            )
        });
        let path = output_dir.join("decision-path-evidence-v1.json");
        let mut bytes =
            serde_json::to_vec_pretty(self).expect("serialize W4-E04 decision evidence");
        bytes.push(b'\n');
        fs::write(&path, &bytes)
            .unwrap_or_else(|error| panic!("write W4-E04 evidence {}: {error}", path.display()));
        let content_hash = CanonicalDigest::content_hash_bytes(&bytes).to_string();
        eprintln!(
            "W4-E04 decision evidence: path={} content_hash={content_hash}",
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
    env::var_os("W4_E04_EVIDENCE_DIR").map_or_else(
        || Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/phase-11.9/w4-e04"),
        PathBuf::from,
    )
}
