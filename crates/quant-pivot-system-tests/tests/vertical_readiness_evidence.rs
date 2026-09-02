//! Strict W4-E08/E09 current-environment vertical readiness verdicts.

use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::{Path, PathBuf},
};

use chrono::{DateTime, Utc};
use quant_pivot_models::{
    config::{
        DeployConfig, DeployConfigLoadRequest, WeatherVerticalBindingsConfig,
        builtin_weather_station_profiles,
    },
    enums::domain::DomainFamily,
    hashing::CanonicalDigest,
    runtime_config::QualityGateConfig,
    types::{
        CRYPTO_PRICE_15M_PROFILE_ID, CapabilityEligibility, ContentHash, DeploymentEnvironment,
        DomainCapabilityReasonCode, DomainContractCapability, DomainContractFamily,
        ResearchEvaluationTrack, ResearchProfileRef, WEATHER_FORECAST_24H_PROFILE_ID,
        builtin_research_profiles,
        domain_classification::{
            DomainCatalogClassificationArtifact, DomainMarketClassificationOutcome,
        },
    },
};
use quant_pivot_research::linkage::{
    WeatherStationRegistry, capability_registry::domain_capability_registry,
};
use quant_pivot_storage::clickhouse::ClickHousePool;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const CRYPTO_READINESS_EVIDENCE_FORMAT_VERSION: u32 = 1;
const WEATHER_READINESS_EVIDENCE_FORMAT_VERSION: u32 = 1;
const OUTCOME_BACKFILL_EVIDENCE_FORMAT_VERSION: u32 = 3;
const HISTORICAL_WEATHER_PIT_FORMAT_VERSION: u32 = 4;
const WEATHER_FAMILIES: [DomainContractFamily; 8] = [
    DomainContractFamily::WeatherDailyTemperature,
    DomainContractFamily::WeatherPrecipitation,
    DomainContractFamily::WeatherAqi,
    DomainContractFamily::WeatherTornado,
    DomainContractFamily::WeatherTropicalCyclone,
    DomainContractFamily::WeatherGlobalTemperature,
    DomainContractFamily::WeatherSeaIce,
    DomainContractFamily::WeatherWindExtreme,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Vertical {
    Crypto,
    Weather,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ImplementationVerdict {
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum OperationalActivationVerdict {
    Blocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum PublicationVerdict {
    Blocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ActivationClaim {
    NotClaimed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum FixtureDisposition {
    RejectedForOperationalReadiness,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CurrentEnvironmentStatus {
    Blocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PreflightAccess {
    ReadOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum BackfillExecutionStatus {
    NotExecuted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SchemaMutationAuthority {
    OperatorAuthorizationRequired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum UpstreamBlocker {
    PostgresSchemaIdentityMismatch,
    ClickhouseSchemaUnavailable,
    ClickhouseFactQueryUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum GateBlocker {
    CurrentTruthUnavailable,
    ResearchOnlyProfile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
enum CryptoGateKind {
    ProfileActivationEligibility,
    CurrentOutcomeBackfill,
    CurrentProfileBinding,
    CurrentFeedbackCycle,
    MatureLabels,
    Coverage,
    Calibration,
    Cpcv,
    DeflatedSharpeRatio,
    ProbabilityBacktestOverfitting,
    SameWindowComparison,
    ChainlinkResolutionEvidence,
    BinanceContinuityEvidence,
    PublishedShadowIdentity,
    ShadowObservationCount,
    ShadowObservationWindow,
    PromotionPermit,
    ServingContract,
    CurrentRouteGeneration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum EvidenceRequirement {
    ExecutionEligibleImmutableProfile,
    CurrentResolutionAndExecutionOutcomes,
    CurrentProfileArtifactBinding,
    CurrentFeedbackCycleLineage,
    MatureResolutionLabels,
    CoverageArtifact,
    CalibrationArtifact,
    CompleteCpcvPathSet,
    DeflatedSharpeReport,
    BacktestOverfittingReport,
    SameWindowComparisonArtifact,
    ChainlinkResolutionWindow,
    BinanceContinuityWindow,
    PublishedGenerationActiveShadowPair,
    PublishedGenerationShadowObservations,
    PublishedGenerationShadowDuration,
    ActivePromotionPermit,
    VerifiedModelServingContract,
    CurrentPublishedRouteGeneration,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "verdict", rename_all = "snake_case")]
enum GateVerdict {
    Blocked {
        blocker: GateBlocker,
        missing_evidence: EvidenceRequirement,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct CryptoGateEvidence {
    gate: CryptoGateKind,
    current: GateVerdict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
struct OutcomeCounts {
    resolution: u64,
    execution: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct E07CurrentEnvironment {
    status: CurrentEnvironmentStatus,
    preflight_command: String,
    preflight_access: PreflightAccess,
    blocker_code: UpstreamBlocker,
    backfill_status: BackfillExecutionStatus,
    schema_mutation_authority: SchemaMutationAuthority,
    real_outcome_counts: Option<OutcomeCounts>,
    real_label_count: Option<u64>,
    recovery_commands: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct E07ManifestProjection {
    format_version: u32,
    operational_activation_claimed: bool,
    usable_for_crypto_readiness: bool,
    usable_for_weather_readiness: bool,
    current_environment: E07CurrentEnvironment,
    outcome_counts: OutcomeCounts,
    label_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct OutcomePrerequisiteEvidence {
    manifest_path: String,
    manifest_sha256: String,
    manifest_content_hash: String,
    format_version: u32,
    current_environment: CurrentEnvironmentStatus,
    preflight_access: PreflightAccess,
    blocker: UpstreamBlocker,
    backfill_status: BackfillExecutionStatus,
    schema_mutation_authority: SchemaMutationAuthority,
    real_outcome_counts: Option<OutcomeCounts>,
    real_label_count: Option<u64>,
    disposable_contract_outcomes: OutcomeCounts,
    disposable_contract_labels: u64,
    disposable_contract_disposition: FixtureDisposition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum CurrentCryptoSnapshot {
    Unavailable {
        blocker: UpstreamBlocker,
        detail: &'static str,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct CanonicalCryptoContract {
    profile_id: String,
    profile_version: u32,
    profile_artifact_id: String,
    profile_content_hash: String,
    activation_eligibility: ResearchEvaluationTrack,
    feedback_policy_hash: String,
    minimum_mature_labels: u64,
    minimum_new_mature_labels: u64,
    minimum_coverage: String,
    comparison_minimum_observations: u64,
    shadow_minimum_observations: u64,
    cpcv_minimum_paths: u32,
    minimum_deflated_sharpe_ratio: String,
    maximum_probability_backtest_overfitting: String,
    required_shadow_window_secs: u64,
    minimum_shadow_decision_overlap: String,
    chainlink_execution_minimum_days: u32,
    chainlink_execution_minimum_samples: u64,
    binance_execution_minimum_days: u32,
    binance_execution_minimum_samples: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum RecoveryAccess {
    ReadOnly,
    Mutating,
    ServiceLifecycle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum RecoveryAuthorization {
    None,
    OperatorRequired,
    AuthenticatedReadRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct RecoveryStep {
    sequence: u32,
    access: RecoveryAccess,
    authorization: RecoveryAuthorization,
    command: &'static str,
    success_condition: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct MethodologyReference {
    topic: &'static str,
    url: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct CryptoReadinessEvidenceManifest {
    format_version: u32,
    vertical: Vertical,
    generated_at: DateTime<Utc>,
    implementation: ImplementationVerdict,
    publication: PublicationVerdict,
    operational_activation: OperationalActivationVerdict,
    activation_claim: ActivationClaim,
    outcome_prerequisite: OutcomePrerequisiteEvidence,
    canonical_contract: CanonicalCryptoContract,
    current_snapshot: CurrentCryptoSnapshot,
    gates: Vec<CryptoGateEvidence>,
    recovery: Vec<RecoveryStep>,
    methodology_references: Vec<MethodologyReference>,
}

struct E07Input {
    path: PathBuf,
    relative_path: String,
    sha256: String,
    content_hash: String,
    projection: E07ManifestProjection,
}

impl E07Input {
    fn from_env() -> Self {
        let path = PathBuf::from(
            env::var_os("W4_E07_EVIDENCE_MANIFEST")
                .expect("W4_E07_EVIDENCE_MANIFEST must identify the canonical v3 artifact"),
        );
        let expected_sha256 = env::var("W4_E07_EVIDENCE_SHA256")
            .expect("W4_E07_EVIDENCE_SHA256 must pin the canonical v3 bytes");
        let bytes = fs::read(&path)
            .unwrap_or_else(|error| panic!("read W4-E07 manifest {}: {error}", path.display()));
        let sha256 = hex::encode(Sha256::digest(&bytes));
        assert_eq!(
            sha256, expected_sha256,
            "W4-E07 manifest bytes differ from the pinned SHA-256"
        );
        let projection: E07ManifestProjection =
            serde_json::from_slice(&bytes).expect("decode W4-E07 manifest projection");
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .expect("system-test crate belongs to the workspace");
        let relative_path = path
            .strip_prefix(workspace)
            .unwrap_or(&path)
            .to_string_lossy()
            .into_owned();
        Self {
            path,
            relative_path,
            sha256,
            content_hash: CanonicalDigest::content_hash_bytes(&bytes).to_string(),
            projection,
        }
    }
}

impl CanonicalCryptoContract {
    fn load() -> Self {
        let profile = builtin_research_profiles()
            .expect("load immutable research profiles")
            .into_iter()
            .find(|profile| profile.profile_ref.id.as_str() == CRYPTO_PRICE_15M_PROFILE_ID)
            .expect("crypto_price_15m profile exists");
        profile.spec.validate().expect("Crypto profile is valid");
        let policy = &profile.spec.feedback_policy;
        let quality = &profile.spec.quality_gate;
        let runtime_gate = QualityGateConfig::default();
        Self {
            profile_id: profile.profile_ref.id.to_string(),
            profile_version: profile.profile_ref.version,
            profile_artifact_id: profile.profile_ref.artifact_id().to_string(),
            profile_content_hash: profile.profile_ref.content_hash.to_string(),
            activation_eligibility: profile.spec.activation_eligibility,
            feedback_policy_hash: policy
                .content_hash()
                .expect("hash immutable Crypto feedback policy")
                .to_string(),
            minimum_mature_labels: policy.minimum_mature_labels,
            minimum_new_mature_labels: policy.minimum_new_mature_labels,
            minimum_coverage: policy.minimum_coverage.normalize().to_string(),
            comparison_minimum_observations: policy.comparison_minimum_observations,
            shadow_minimum_observations: policy.shadow_minimum_observations,
            cpcv_minimum_paths: quality.min_cpcv_paths,
            minimum_deflated_sharpe_ratio: quality
                .min_deflated_sharpe_ratio
                .normalize()
                .to_string(),
            maximum_probability_backtest_overfitting: quality
                .max_probability_of_backtest_overfitting
                .normalize()
                .to_string(),
            required_shadow_window_secs: runtime_gate.required_shadow_window_secs,
            minimum_shadow_decision_overlap: runtime_gate
                .min_shadow_decision_overlap
                .value()
                .normalize()
                .to_string(),
            chainlink_execution_minimum_days: 14,
            chainlink_execution_minimum_samples: 2_000,
            binance_execution_minimum_days: 30,
            binance_execution_minimum_samples: 100_000,
        }
    }
}

impl CryptoReadinessEvidenceManifest {
    fn blocked(e07: &E07Input) -> Self {
        Self {
            format_version: CRYPTO_READINESS_EVIDENCE_FORMAT_VERSION,
            vertical: Vertical::Crypto,
            generated_at: Utc::now(),
            implementation: ImplementationVerdict::Closed,
            publication: PublicationVerdict::Blocked,
            operational_activation: OperationalActivationVerdict::Blocked,
            activation_claim: ActivationClaim::NotClaimed,
            outcome_prerequisite: OutcomePrerequisiteEvidence {
                manifest_path: e07.relative_path.clone(),
                manifest_sha256: e07.sha256.clone(),
                manifest_content_hash: e07.content_hash.clone(),
                format_version: e07.projection.format_version,
                current_environment: e07.projection.current_environment.status,
                preflight_access: e07.projection.current_environment.preflight_access,
                blocker: e07.projection.current_environment.blocker_code,
                backfill_status: e07.projection.current_environment.backfill_status,
                schema_mutation_authority: e07
                    .projection
                    .current_environment
                    .schema_mutation_authority,
                real_outcome_counts: e07.projection.current_environment.real_outcome_counts,
                real_label_count: e07.projection.current_environment.real_label_count,
                disposable_contract_outcomes: e07.projection.outcome_counts,
                disposable_contract_labels: e07.projection.label_count,
                disposable_contract_disposition:
                    FixtureDisposition::RejectedForOperationalReadiness,
            },
            canonical_contract: CanonicalCryptoContract::load(),
            current_snapshot: CurrentCryptoSnapshot::Unavailable {
                blocker: UpstreamBlocker::PostgresSchemaIdentityMismatch,
                detail: "current outcome/profile/cycle/artifact/permit/route/generation truth was not queried after the read-only schema preflight failed closed",
            },
            gates: Self::blocked_gates(),
            recovery: Self::recovery_steps(),
            methodology_references: Self::methodology_references(),
        }
    }

    fn blocked_gates() -> Vec<CryptoGateEvidence> {
        let unavailable = [
            (
                CryptoGateKind::CurrentOutcomeBackfill,
                EvidenceRequirement::CurrentResolutionAndExecutionOutcomes,
            ),
            (
                CryptoGateKind::CurrentProfileBinding,
                EvidenceRequirement::CurrentProfileArtifactBinding,
            ),
            (
                CryptoGateKind::CurrentFeedbackCycle,
                EvidenceRequirement::CurrentFeedbackCycleLineage,
            ),
            (
                CryptoGateKind::MatureLabels,
                EvidenceRequirement::MatureResolutionLabels,
            ),
            (
                CryptoGateKind::Coverage,
                EvidenceRequirement::CoverageArtifact,
            ),
            (
                CryptoGateKind::Calibration,
                EvidenceRequirement::CalibrationArtifact,
            ),
            (
                CryptoGateKind::Cpcv,
                EvidenceRequirement::CompleteCpcvPathSet,
            ),
            (
                CryptoGateKind::DeflatedSharpeRatio,
                EvidenceRequirement::DeflatedSharpeReport,
            ),
            (
                CryptoGateKind::ProbabilityBacktestOverfitting,
                EvidenceRequirement::BacktestOverfittingReport,
            ),
            (
                CryptoGateKind::SameWindowComparison,
                EvidenceRequirement::SameWindowComparisonArtifact,
            ),
            (
                CryptoGateKind::ChainlinkResolutionEvidence,
                EvidenceRequirement::ChainlinkResolutionWindow,
            ),
            (
                CryptoGateKind::BinanceContinuityEvidence,
                EvidenceRequirement::BinanceContinuityWindow,
            ),
            (
                CryptoGateKind::PublishedShadowIdentity,
                EvidenceRequirement::PublishedGenerationActiveShadowPair,
            ),
            (
                CryptoGateKind::ShadowObservationCount,
                EvidenceRequirement::PublishedGenerationShadowObservations,
            ),
            (
                CryptoGateKind::ShadowObservationWindow,
                EvidenceRequirement::PublishedGenerationShadowDuration,
            ),
            (
                CryptoGateKind::PromotionPermit,
                EvidenceRequirement::ActivePromotionPermit,
            ),
            (
                CryptoGateKind::ServingContract,
                EvidenceRequirement::VerifiedModelServingContract,
            ),
            (
                CryptoGateKind::CurrentRouteGeneration,
                EvidenceRequirement::CurrentPublishedRouteGeneration,
            ),
        ];
        let mut gates = vec![CryptoGateEvidence {
            gate: CryptoGateKind::ProfileActivationEligibility,
            current: GateVerdict::Blocked {
                blocker: GateBlocker::ResearchOnlyProfile,
                missing_evidence: EvidenceRequirement::ExecutionEligibleImmutableProfile,
            },
        }];
        gates.extend(
            unavailable
                .into_iter()
                .map(|(gate, missing_evidence)| CryptoGateEvidence {
                    gate,
                    current: GateVerdict::Blocked {
                        blocker: GateBlocker::CurrentTruthUnavailable,
                        missing_evidence,
                    },
                }),
        );
        gates
    }

    fn recovery_steps() -> Vec<RecoveryStep> {
        vec![
            RecoveryStep {
                sequence: 1,
                access: RecoveryAccess::ReadOnly,
                authorization: RecoveryAuthorization::None,
                command: "cargo xtask postgres-schema plan --config-file /absolute/path/to/quant-pivot.toml --expected-environment local-development",
                success_condition: "the immutable migration plan is reviewed without changing the target",
            },
            RecoveryStep {
                sequence: 2,
                access: RecoveryAccess::Mutating,
                authorization: RecoveryAuthorization::OperatorRequired,
                command: "cargo xtask postgres-schema apply --config-file /absolute/path/to/quant-pivot.toml --expected-environment local-development",
                success_condition: "the existing deploy owner applies the reviewed immutable plan",
            },
            RecoveryStep {
                sequence: 3,
                access: RecoveryAccess::ReadOnly,
                authorization: RecoveryAuthorization::None,
                command: "cargo xtask postgres-schema verify --config-file /absolute/path/to/quant-pivot.toml --expected-environment local-development",
                success_condition: "the current PostgreSQL identity and runtime contract verify exactly",
            },
            RecoveryStep {
                sequence: 4,
                access: RecoveryAccess::ServiceLifecycle,
                authorization: RecoveryAuthorization::OperatorRequired,
                command: "cargo run -p quant-pivot-bin -- --config-file /absolute/path/to/quant-pivot.toml --expected-environment local-development",
                success_condition: "the current binary starts on the verified schema and its canonical outcome/feedback workers run without typed failures",
            },
            RecoveryStep {
                sequence: 5,
                access: RecoveryAccess::ReadOnly,
                authorization: RecoveryAuthorization::AuthenticatedReadRequired,
                command: "curl --fail-with-body --header \"Authorization: Bearer ${ACCESS_TOKEN:?set ACCESS_TOKEN}\" http://127.0.0.1:8088/api/research/feedback-overview",
                success_condition: "the current Crypto profile, readiness snapshot, latest cycle, and coverage are authoritative and observable",
            },
            RecoveryStep {
                sequence: 6,
                access: RecoveryAccess::ReadOnly,
                authorization: RecoveryAuthorization::AuthenticatedReadRequired,
                command: "curl --fail-with-body --get --header \"Authorization: Bearer ${ACCESS_TOKEN:?set ACCESS_TOKEN}\" --data-urlencode \"profile_id=crypto_price_15m\" --data-urlencode \"page=1\" --data-urlencode \"size=20\" http://127.0.0.1:8088/api/research/feedback-cycles",
                success_condition: "the current Crypto cycle and immutable stage lineage can be selected without synthetic rows",
            },
            RecoveryStep {
                sequence: 7,
                access: RecoveryAccess::ReadOnly,
                authorization: RecoveryAuthorization::AuthenticatedReadRequired,
                command: "curl --fail-with-body --get --header \"Authorization: Bearer ${ACCESS_TOKEN:?set ACCESS_TOKEN}\" --data-urlencode \"intent=route_activation\" \"http://127.0.0.1:8088/api/research/models/${MODEL_VERSION_ID:?set MODEL_VERSION_ID}/quality-gate\"",
                success_condition: "the exact current model passes the server-owned route-activation scorecard, serving contract, and immutable artifact checks",
            },
        ]
    }

    fn methodology_references() -> Vec<MethodologyReference> {
        vec![
            MethodologyReference {
                topic: "probability_of_backtest_overfitting",
                url: "https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2326253",
            },
            MethodologyReference {
                topic: "deflated_sharpe_ratio",
                url: "https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf",
            },
            MethodologyReference {
                topic: "production_model_validation_and_monitoring",
                url: "https://developers.google.com/machine-learning/managing-ml-projects/production",
            },
        ]
    }

    fn validate(&self, input: &E07Input) {
        assert_eq!(
            self.format_version,
            CRYPTO_READINESS_EVIDENCE_FORMAT_VERSION
        );
        assert_eq!(self.implementation, ImplementationVerdict::Closed);
        assert_eq!(self.publication, PublicationVerdict::Blocked);
        assert_eq!(
            self.operational_activation,
            OperationalActivationVerdict::Blocked
        );
        assert_eq!(self.activation_claim, ActivationClaim::NotClaimed);
        assert_eq!(
            input.projection.format_version,
            OUTCOME_BACKFILL_EVIDENCE_FORMAT_VERSION
        );
        assert!(!input.projection.operational_activation_claimed);
        assert!(!input.projection.usable_for_crypto_readiness);
        assert!(!input.projection.usable_for_weather_readiness);
        assert_eq!(
            input.projection.current_environment.status,
            CurrentEnvironmentStatus::Blocked
        );
        assert_eq!(
            input.projection.current_environment.preflight_access,
            PreflightAccess::ReadOnly
        );
        assert_eq!(
            input.projection.current_environment.blocker_code,
            UpstreamBlocker::PostgresSchemaIdentityMismatch
        );
        assert_eq!(
            input.projection.current_environment.backfill_status,
            BackfillExecutionStatus::NotExecuted
        );
        assert_eq!(
            input
                .projection
                .current_environment
                .schema_mutation_authority,
            SchemaMutationAuthority::OperatorAuthorizationRequired
        );
        assert_eq!(
            input.projection.current_environment.real_outcome_counts,
            None
        );
        assert_eq!(input.projection.current_environment.real_label_count, None);
        assert_eq!(
            input.projection.current_environment.preflight_command,
            "cargo xtask postgres-schema verify --config-file /absolute/path/to/quant-pivot.toml --expected-environment local-development"
        );
        assert_eq!(
            input.projection.current_environment.recovery_commands,
            [
                "cargo xtask postgres-schema plan --config-file /absolute/path/to/quant-pivot.toml --expected-environment local-development",
                "cargo xtask postgres-schema apply --config-file /absolute/path/to/quant-pivot.toml --expected-environment local-development",
                "cargo xtask postgres-schema verify --config-file /absolute/path/to/quant-pivot.toml --expected-environment local-development",
            ]
        );
        assert_eq!(input.projection.label_count, 0);
        self.validate_contract();
        assert!(input.path.is_file());
    }

    fn validate_contract(&self) {
        Self::validate_parts(
            &self.canonical_contract,
            &self.gates,
            &self.recovery,
            &self.methodology_references,
        );
    }

    fn validate_parts(
        contract: &CanonicalCryptoContract,
        gates: &[CryptoGateEvidence],
        recovery: &[RecoveryStep],
        references: &[MethodologyReference],
    ) {
        assert_eq!(
            contract.activation_eligibility,
            ResearchEvaluationTrack::ExecutionCandidate
        );
        assert_eq!(contract.minimum_mature_labels, 500);
        assert_eq!(contract.minimum_coverage, "0.95");
        assert_eq!(contract.cpcv_minimum_paths, 21);
        assert_eq!(contract.minimum_deflated_sharpe_ratio, "0.95");
        assert_eq!(contract.maximum_probability_backtest_overfitting, "0.05");
        assert_eq!(contract.shadow_minimum_observations, 1_000);
        assert_eq!(contract.required_shadow_window_secs, 86_400);
        let distinct = gates
            .iter()
            .map(|evidence| evidence.gate)
            .collect::<BTreeSet<_>>();
        assert_eq!(gates.len(), 19);
        assert_eq!(distinct.len(), gates.len());
        assert!(
            gates
                .iter()
                .all(|evidence| matches!(evidence.current, GateVerdict::Blocked { .. }))
        );
        assert_eq!(
            recovery
                .iter()
                .filter(|step| step.access == RecoveryAccess::Mutating)
                .count(),
            1
        );
        assert!(recovery.iter().any(|step| {
            step.access == RecoveryAccess::Mutating
                && step.authorization == RecoveryAuthorization::OperatorRequired
                && step.command == "cargo xtask postgres-schema apply --config-file /absolute/path/to/quant-pivot.toml --expected-environment local-development"
        }));
        assert_eq!(references.len(), 3);
    }

    fn write(&self) -> ArtifactReceipt {
        let output_dir = env::var_os("W4_E08_EVIDENCE_DIR").map_or_else(
            || Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/phase-11.9/w4-e08"),
            PathBuf::from,
        );
        fs::create_dir_all(&output_dir).unwrap_or_else(|error| {
            panic!(
                "create W4-E08 evidence directory {}: {error}",
                output_dir.display()
            )
        });
        let path = output_dir.join("crypto-readiness-evidence-v1.json");
        let mut bytes =
            serde_json::to_vec_pretty(self).expect("serialize W4-E08 Crypto readiness evidence");
        bytes.push(b'\n');
        fs::write(&path, &bytes)
            .unwrap_or_else(|error| panic!("write W4-E08 evidence {}: {error}", path.display()));
        ArtifactReceipt {
            path,
            content_hash: CanonicalDigest::content_hash_bytes(&bytes).to_string(),
        }
    }
}

struct ArtifactReceipt {
    path: PathBuf,
    content_hash: String,
}

#[test]
fn crypto_readiness_contract() {
    let contract = CanonicalCryptoContract::load();
    let gates = CryptoReadinessEvidenceManifest::blocked_gates();
    let recovery = CryptoReadinessEvidenceManifest::recovery_steps();
    let references = CryptoReadinessEvidenceManifest::methodology_references();
    CryptoReadinessEvidenceManifest::validate_parts(&contract, &gates, &recovery, &references);
}

#[test]
#[ignore = "requires an explicitly pinned current W4-E07 evidence artifact"]
fn crypto_readiness_evidence() {
    let input = E07Input::from_env();
    let manifest = CryptoReadinessEvidenceManifest::blocked(&input);
    manifest.validate(&input);
    let receipt = manifest.write();
    eprintln!(
        "W4-E08 Crypto readiness evidence: path={} content_hash={}",
        receipt.path.display(),
        receipt.content_hash
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum NonCurrentDisposition {
    LineageOnlyNotCurrentReadiness,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum PitContractVerdict {
    Verified,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum WeatherGateBlocker {
    CurrentProfileTruthUnavailable,
    CurrentServingTruthUnavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum CurrentWeatherGate {
    Blocked { blocker: WeatherGateBlocker },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum ParserGate {
    Implemented,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum FactPlane {
    Observation,
    Forecast,
}

#[derive(Debug, Clone, Copy)]
struct FactSelector {
    plane: FactPlane,
    source_id: &'static str,
    variable: &'static str,
    required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, clickhouse::Row, Deserialize, Serialize)]
struct FactAggregateRow {
    source_id: String,
    variable: String,
    row_count: u64,
    first_available_at_ms: i64,
    last_available_at_ms: i64,
    availability_order_violations: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct FamilyFactRow {
    plane: FactPlane,
    source_id: String,
    variable: String,
    required: bool,
    row_count: u64,
    first_available_at_ms: Option<i64>,
    last_available_at_ms: Option<i64>,
    availability_order_violations: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum CurrentFactMatrix {
    Verified {
        queried_at: DateTime<Utc>,
        observation_rows: Vec<FactAggregateRow>,
        forecast_rows: Vec<FactAggregateRow>,
    },
    Unavailable {
        blocker: UpstreamBlocker,
        observation_rows: Option<u64>,
        forecast_rows: Option<u64>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum CurrentOfficialFacts {
    Verified {
        queried_at: DateTime<Utc>,
        rows: Vec<FamilyFactRow>,
    },
    Unavailable {
        blocker: UpstreamBlocker,
        observation_rows: Option<u64>,
        forecast_rows: Option<u64>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum CurrentMatureLabels {
    Unavailable {
        blocker: UpstreamBlocker,
        current_mature_labels: Option<u64>,
        minimum_mature_labels: u64,
        eligibility: CapabilityEligibility,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct FamilyPitManifest {
    verdict: PitContractVerdict,
    capability_registry_hash: ContentHash,
    observation_table: &'static str,
    forecast_table: &'static str,
    observation_event_time_field: &'static str,
    forecast_reference_time_field: &'static str,
    forecast_valid_time_field: &'static str,
    publication_time_field: &'static str,
    availability_time_field: &'static str,
    revision_field: &'static str,
    content_hash_field: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct WeatherFamilyReadiness {
    contract_family: DomainContractFamily,
    capability_contracts: Vec<DomainContractCapability>,
    pit_manifest: FamilyPitManifest,
    current_official_facts: CurrentOfficialFacts,
    current_mature_labels: CurrentMatureLabels,
    parser_gate: ParserGate,
    profile_gate: CurrentWeatherGate,
    serving_gate: CurrentWeatherGate,
    operational_activation: OperationalActivationVerdict,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct CanonicalWeatherContract {
    profile_id: String,
    profile_version: u32,
    profile_artifact_id: String,
    profile_content_hash: String,
    activation_eligibility: ResearchEvaluationTrack,
    feedback_policy_hash: String,
    minimum_mature_labels: u64,
    minimum_new_mature_labels: u64,
    minimum_coverage: String,
    comparison_minimum_observations: u64,
    shadow_minimum_observations: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct HistoricalFactIdentity {
    physical_rows: u64,
    logical_keys: u64,
    duplicate_rows: u64,
    revision_conflicts: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct HistoricalPitProjection {
    format_version: u32,
    capability_registry_hash: ContentHash,
    catalog_hash: ContentHash,
    weather_observation_rows: u64,
    weather_forecast_rows: u64,
    fact_idempotency: BTreeMap<String, HistoricalFactIdentity>,
    blockers: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
struct HistoricalOutcomeCounts {
    supported: u64,
    credential_blocked: u64,
    insufficient_evidence: u64,
    excluded: u64,
    unsupported_template: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct HistoricalFamilyEvidence {
    contract_family: DomainContractFamily,
    outcomes: HistoricalOutcomeCounts,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct HistoricalWeatherLineage {
    disposition: NonCurrentDisposition,
    capability_audit_path: String,
    capability_audit_sha256: String,
    capability_audit_content_hash: String,
    capability_audit_artifact_hash: ContentHash,
    pit_manifest_path: String,
    pit_manifest_sha256: String,
    pit_manifest_content_hash: String,
    capability_registry_hash: ContentHash,
    catalog_hash: ContentHash,
    weather_observation_rows: u64,
    weather_forecast_rows: u64,
    family_outcomes: Vec<HistoricalFamilyEvidence>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct OfficialSourceReference {
    source_id: &'static str,
    url: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct WeatherCurrentEnvironment {
    postgres_preflight_command: &'static str,
    postgres_preflight_access: PreflightAccess,
    postgres_blocker: UpstreamBlocker,
    clickhouse_preflight_command: &'static str,
    clickhouse_preflight_access: PreflightAccess,
    clickhouse_facts: CurrentFactMatrix,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct WeatherReadinessEvidenceManifest {
    format_version: u32,
    vertical: Vertical,
    generated_at: DateTime<Utc>,
    implementation: ImplementationVerdict,
    publication: PublicationVerdict,
    operational_activation: OperationalActivationVerdict,
    activation_claim: ActivationClaim,
    current_environment: WeatherCurrentEnvironment,
    canonical_contract: CanonicalWeatherContract,
    capability_registry_hash: ContentHash,
    families: Vec<WeatherFamilyReadiness>,
    historical_lineage: HistoricalWeatherLineage,
    recovery: Vec<RecoveryStep>,
    official_source_references: Vec<OfficialSourceReference>,
}

struct PinnedArtifact {
    relative_path: String,
    sha256: String,
    content_hash: String,
    bytes: Vec<u8>,
}

impl PinnedArtifact {
    fn from_env(path_variable: &str, sha_variable: &str) -> Self {
        let path = PathBuf::from(
            env::var_os(path_variable)
                .unwrap_or_else(|| panic!("{path_variable} must identify the canonical artifact")),
        );
        let expected_sha256 = env::var(sha_variable)
            .unwrap_or_else(|_| panic!("{sha_variable} must pin the canonical artifact bytes"));
        let bytes = fs::read(&path)
            .unwrap_or_else(|error| panic!("read pinned artifact {}: {error}", path.display()));
        let sha256 = hex::encode(Sha256::digest(&bytes));
        assert_eq!(
            sha256, expected_sha256,
            "pinned artifact bytes differ from the expected SHA-256"
        );
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .expect("system-test crate belongs to the workspace");
        let relative_path = path
            .strip_prefix(workspace)
            .unwrap_or(&path)
            .to_string_lossy()
            .into_owned();
        Self {
            relative_path,
            sha256,
            content_hash: CanonicalDigest::content_hash_bytes(&bytes).to_string(),
            bytes,
        }
    }
}

impl CanonicalWeatherContract {
    fn load() -> (Self, ResearchProfileRef) {
        let profile = builtin_research_profiles()
            .expect("load immutable research profiles")
            .into_iter()
            .find(|profile| profile.profile_ref.id.as_str() == WEATHER_FORECAST_24H_PROFILE_ID)
            .expect("weather_forecast_24h profile exists");
        profile.spec.validate().expect("Weather profile is valid");
        let policy = &profile.spec.feedback_policy;
        (
            Self {
                profile_id: profile.profile_ref.id.to_string(),
                profile_version: profile.profile_ref.version,
                profile_artifact_id: profile.profile_ref.artifact_id().to_string(),
                profile_content_hash: profile.profile_ref.content_hash.to_string(),
                activation_eligibility: profile.spec.activation_eligibility,
                feedback_policy_hash: policy
                    .content_hash()
                    .expect("hash immutable Weather feedback policy")
                    .to_string(),
                minimum_mature_labels: policy.minimum_mature_labels,
                minimum_new_mature_labels: policy.minimum_new_mature_labels,
                minimum_coverage: policy.minimum_coverage.normalize().to_string(),
                comparison_minimum_observations: policy.comparison_minimum_observations,
                shadow_minimum_observations: policy.shadow_minimum_observations,
            },
            profile.profile_ref,
        )
    }
}

impl CurrentFactMatrix {
    async fn load(deploy: &DeployConfig) -> Self {
        let pool = ClickHousePool::from_config(&deploy.db.clickhouse);
        if pool.verify_schema().await.is_err() {
            return Self::Unavailable {
                blocker: UpstreamBlocker::ClickhouseSchemaUnavailable,
                observation_rows: None,
                forecast_rows: None,
            };
        }
        let observation_rows = pool
            .client()
            .query(
                "SELECT source_id, variable, count() AS row_count, \
                 toUnixTimestamp64Milli(min(available_at)) AS first_available_at_ms, \
                 toUnixTimestamp64Milli(max(available_at)) AS last_available_at_ms, \
                 countIf(available_at < published_at) AS availability_order_violations \
                 FROM quant_weather_observation_fact \
                 GROUP BY source_id, variable ORDER BY source_id, variable",
            )
            .fetch_all::<FactAggregateRow>()
            .await;
        let forecast_rows = pool
            .client()
            .query(
                "SELECT source_id, variable, count() AS row_count, \
                 toUnixTimestamp64Milli(min(available_at)) AS first_available_at_ms, \
                 toUnixTimestamp64Milli(max(available_at)) AS last_available_at_ms, \
                 countIf(available_at < published_at) AS availability_order_violations \
                 FROM quant_weather_forecast_fact \
                 GROUP BY source_id, variable ORDER BY source_id, variable",
            )
            .fetch_all::<FactAggregateRow>()
            .await;
        match (observation_rows, forecast_rows) {
            (Ok(observation_rows), Ok(forecast_rows)) => Self::Verified {
                queried_at: Utc::now(),
                observation_rows,
                forecast_rows,
            },
            _ => Self::Unavailable {
                blocker: UpstreamBlocker::ClickhouseFactQueryUnavailable,
                observation_rows: None,
                forecast_rows: None,
            },
        }
    }
}

impl CurrentOfficialFacts {
    fn from_matrix(family: DomainContractFamily, matrix: &CurrentFactMatrix) -> Self {
        match matrix {
            CurrentFactMatrix::Unavailable {
                blocker,
                observation_rows,
                forecast_rows,
            } => Self::Unavailable {
                blocker: *blocker,
                observation_rows: *observation_rows,
                forecast_rows: *forecast_rows,
            },
            CurrentFactMatrix::Verified {
                queried_at,
                observation_rows,
                forecast_rows,
            } => {
                let rows = Self::selectors(family)
                    .iter()
                    .map(|selector| {
                        let source = match selector.plane {
                            FactPlane::Observation => observation_rows,
                            FactPlane::Forecast => forecast_rows,
                        };
                        let row = source.iter().find(|row| {
                            row.source_id == selector.source_id && row.variable == selector.variable
                        });
                        FamilyFactRow {
                            plane: selector.plane,
                            source_id: selector.source_id.to_owned(),
                            variable: selector.variable.to_owned(),
                            required: selector.required,
                            row_count: row.map_or(0, |row| row.row_count),
                            first_available_at_ms: row.map(|row| row.first_available_at_ms),
                            last_available_at_ms: row.map(|row| row.last_available_at_ms),
                            availability_order_violations: row
                                .map_or(0, |row| row.availability_order_violations),
                        }
                    })
                    .collect();
                Self::Verified {
                    queried_at: *queried_at,
                    rows,
                }
            }
        }
    }

    const fn temperature_selectors() -> &'static [FactSelector] {
        &[
            FactSelector {
                plane: FactPlane::Observation,
                source_id: "aviation_weather",
                variable: "temperature",
                required: true,
            },
            FactSelector {
                plane: FactPlane::Observation,
                source_id: "ghcnh",
                variable: "temperature",
                required: true,
            },
            FactSelector {
                plane: FactPlane::Observation,
                source_id: "ghcnd",
                variable: "temperature_maximum",
                required: true,
            },
            FactSelector {
                plane: FactPlane::Observation,
                source_id: "ghcnd",
                variable: "temperature_minimum",
                required: true,
            },
            FactSelector {
                plane: FactPlane::Observation,
                source_id: "hko_open_data",
                variable: "temperature_maximum",
                required: true,
            },
            FactSelector {
                plane: FactPlane::Observation,
                source_id: "hko_open_data",
                variable: "temperature_minimum",
                required: true,
            },
            FactSelector {
                plane: FactPlane::Forecast,
                source_id: "gefs",
                variable: "temperature_maximum",
                required: true,
            },
            FactSelector {
                plane: FactPlane::Forecast,
                source_id: "gefs",
                variable: "temperature_minimum",
                required: true,
            },
        ]
    }

    const fn selectors(family: DomainContractFamily) -> &'static [FactSelector] {
        match family {
            DomainContractFamily::WeatherDailyTemperature => Self::temperature_selectors(),
            DomainContractFamily::WeatherPrecipitation => &[
                FactSelector {
                    plane: FactPlane::Observation,
                    source_id: "hko_open_data",
                    variable: "precipitation",
                    required: true,
                },
                FactSelector {
                    plane: FactPlane::Forecast,
                    source_id: "gefs",
                    variable: "precipitation",
                    required: true,
                },
            ],
            DomainContractFamily::WeatherAqi => &[
                FactSelector {
                    plane: FactPlane::Observation,
                    source_id: "airnow",
                    variable: "aqi",
                    required: true,
                },
                FactSelector {
                    plane: FactPlane::Forecast,
                    source_id: "airnow",
                    variable: "aqi",
                    required: false,
                },
            ],
            DomainContractFamily::WeatherTornado => &[
                FactSelector {
                    plane: FactPlane::Observation,
                    source_id: "spc_storm_reports",
                    variable: "tornado_count",
                    required: true,
                },
                FactSelector {
                    plane: FactPlane::Observation,
                    source_id: "ncei_storm_events",
                    variable: "tornado_count",
                    required: true,
                },
                FactSelector {
                    plane: FactPlane::Observation,
                    source_id: "ncei_tornado_time_series",
                    variable: "tornado_count",
                    required: true,
                },
            ],
            DomainContractFamily::WeatherTropicalCyclone => &[
                FactSelector {
                    plane: FactPlane::Observation,
                    source_id: "nhc_advisory",
                    variable: "cyclone_intensity",
                    required: true,
                },
                FactSelector {
                    plane: FactPlane::Observation,
                    source_id: "nhc_hurdat2",
                    variable: "cyclone_intensity",
                    required: true,
                },
            ],
            DomainContractFamily::WeatherGlobalTemperature => &[FactSelector {
                plane: FactPlane::Observation,
                source_id: "nasa_gistemp",
                variable: "global_temperature_anomaly",
                required: true,
            }],
            DomainContractFamily::WeatherSeaIce => &[FactSelector {
                plane: FactPlane::Observation,
                source_id: "nsidc_sea_ice_index",
                variable: "sea_ice_extent",
                required: true,
            }],
            DomainContractFamily::WeatherWindExtreme => &[
                FactSelector {
                    plane: FactPlane::Observation,
                    source_id: "nws_observation",
                    variable: "wind_speed",
                    required: true,
                },
                FactSelector {
                    plane: FactPlane::Observation,
                    source_id: "nws_observation",
                    variable: "wind_gust",
                    required: true,
                },
                FactSelector {
                    plane: FactPlane::Observation,
                    source_id: "ghcnh",
                    variable: "wind_gust",
                    required: true,
                },
                FactSelector {
                    plane: FactPlane::Forecast,
                    source_id: "gefs",
                    variable: "wind_gust",
                    required: true,
                },
            ],
            DomainContractFamily::CryptoDirection
            | DomainContractFamily::CryptoThreshold
            | DomainContractFamily::CryptoBand => &[],
        }
    }
}

impl HistoricalOutcomeCounts {
    const fn record(&mut self, outcome: DomainMarketClassificationOutcome) {
        let counter = match outcome {
            DomainMarketClassificationOutcome::Supported => &mut self.supported,
            DomainMarketClassificationOutcome::CredentialBlocked { .. } => {
                &mut self.credential_blocked
            }
            DomainMarketClassificationOutcome::InsufficientEvidence { .. } => {
                &mut self.insufficient_evidence
            }
            DomainMarketClassificationOutcome::Excluded { .. } => &mut self.excluded,
            DomainMarketClassificationOutcome::UnsupportedTemplate { .. } => {
                &mut self.unsupported_template
            }
        };
        *counter = counter.checked_add(1).expect("historical count fits u64");
    }
}

impl HistoricalWeatherLineage {
    fn load() -> Self {
        let audit_input =
            PinnedArtifact::from_env("W4_E09_CAPABILITY_AUDIT", "W4_E09_CAPABILITY_AUDIT_SHA256");
        let audit: DomainCatalogClassificationArtifact =
            serde_json::from_slice(&audit_input.bytes).expect("decode historical capability audit");
        audit
            .validate()
            .expect("historical capability audit remains content-addressed");
        let pit_input =
            PinnedArtifact::from_env("W4_E09_PIT_MANIFEST", "W4_E09_PIT_MANIFEST_SHA256");
        let pit: HistoricalPitProjection =
            serde_json::from_slice(&pit_input.bytes).expect("decode historical PIT manifest");
        assert_eq!(pit.format_version, HISTORICAL_WEATHER_PIT_FORMAT_VERSION);
        assert!(pit.blockers.is_empty());
        assert_eq!(audit.capability_registry_hash, pit.capability_registry_hash);
        assert_eq!(audit.catalog_hash, pit.catalog_hash);
        for table in [
            "quant_weather_observation_fact",
            "quant_weather_forecast_fact",
        ] {
            let identity = pit
                .fact_idempotency
                .get(table)
                .unwrap_or_else(|| panic!("historical PIT manifest is missing `{table}`"));
            assert_eq!(identity.physical_rows, identity.logical_keys);
            assert_eq!(identity.duplicate_rows, 0);
            assert_eq!(identity.revision_conflicts, 0);
        }
        let mut counts = WEATHER_FAMILIES
            .into_iter()
            .map(|family| (family, HistoricalOutcomeCounts::default()))
            .collect::<BTreeMap<_, _>>();
        for row in audit
            .classifications
            .iter()
            .filter(|row| row.family == DomainFamily::Weather)
        {
            if let Some(family) = row.contract_family {
                counts
                    .get_mut(&family)
                    .expect("historical Weather family belongs to the closed registry")
                    .record(row.outcome);
            }
        }
        let family_outcomes = WEATHER_FAMILIES
            .into_iter()
            .map(|family| HistoricalFamilyEvidence {
                contract_family: family,
                outcomes: counts
                    .remove(&family)
                    .expect("historical family count was initialized"),
            })
            .collect();
        Self {
            disposition: NonCurrentDisposition::LineageOnlyNotCurrentReadiness,
            capability_audit_path: audit_input.relative_path,
            capability_audit_sha256: audit_input.sha256,
            capability_audit_content_hash: audit_input.content_hash,
            capability_audit_artifact_hash: audit.artifact_hash,
            pit_manifest_path: pit_input.relative_path,
            pit_manifest_sha256: pit_input.sha256,
            pit_manifest_content_hash: pit_input.content_hash,
            capability_registry_hash: pit.capability_registry_hash,
            catalog_hash: pit.catalog_hash,
            weather_observation_rows: pit.weather_observation_rows,
            weather_forecast_rows: pit.weather_forecast_rows,
            family_outcomes,
        }
    }
}

impl WeatherReadinessEvidenceManifest {
    fn build(
        bindings: &WeatherVerticalBindingsConfig,
        stations: &WeatherStationRegistry,
        current_facts: CurrentFactMatrix,
        historical_lineage: HistoricalWeatherLineage,
    ) -> Self {
        let (canonical_contract, profile_ref) = CanonicalWeatherContract::load();
        let registry = domain_capability_registry(
            &stations
                .registry_hash()
                .expect("hash current Weather station registry"),
            bindings,
        )
        .expect("build current immutable capability registry");
        let families = WEATHER_FAMILIES
            .into_iter()
            .map(|family| {
                let capability_contracts = registry
                    .contracts
                    .iter()
                    .filter(|contract| {
                        contract.family == DomainFamily::Weather
                            && contract.contract_family == family
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                assert!(
                    !capability_contracts.is_empty(),
                    "Weather family must have a capability contract"
                );
                assert!(capability_contracts.iter().all(|contract| {
                    contract.pit_available
                        && contract.profile.as_ref() == Some(&profile_ref)
                        && !contract.source_bindings.is_empty()
                }));
                validate_selectors(family, &capability_contracts);
                WeatherFamilyReadiness {
                    contract_family: family,
                    capability_contracts,
                    pit_manifest: FamilyPitManifest {
                        verdict: PitContractVerdict::Verified,
                        capability_registry_hash: registry.registry_hash,
                        observation_table: "quant_weather_observation_fact",
                        forecast_table: "quant_weather_forecast_fact",
                        observation_event_time_field: "observed_at",
                        forecast_reference_time_field: "reference_time",
                        forecast_valid_time_field: "valid_time",
                        publication_time_field: "published_at",
                        availability_time_field: "available_at",
                        revision_field: "revision",
                        content_hash_field: "report_hash",
                    },
                    current_official_facts: CurrentOfficialFacts::from_matrix(
                        family,
                        &current_facts,
                    ),
                    current_mature_labels: CurrentMatureLabels::Unavailable {
                        blocker: UpstreamBlocker::PostgresSchemaIdentityMismatch,
                        current_mature_labels: None,
                        minimum_mature_labels: canonical_contract.minimum_mature_labels,
                        eligibility: CapabilityEligibility::InsufficientEvidence {
                            reason_code: DomainCapabilityReasonCode::MatureLabelsUnavailable,
                        },
                    },
                    parser_gate: ParserGate::Implemented,
                    profile_gate: CurrentWeatherGate::Blocked {
                        blocker: WeatherGateBlocker::CurrentProfileTruthUnavailable,
                    },
                    serving_gate: CurrentWeatherGate::Blocked {
                        blocker: WeatherGateBlocker::CurrentServingTruthUnavailable,
                    },
                    operational_activation: OperationalActivationVerdict::Blocked,
                }
            })
            .collect();
        Self {
            format_version: WEATHER_READINESS_EVIDENCE_FORMAT_VERSION,
            vertical: Vertical::Weather,
            generated_at: Utc::now(),
            implementation: ImplementationVerdict::Closed,
            publication: PublicationVerdict::Blocked,
            operational_activation: OperationalActivationVerdict::Blocked,
            activation_claim: ActivationClaim::NotClaimed,
            current_environment: WeatherCurrentEnvironment {
                postgres_preflight_command: "cargo xtask postgres-schema verify --config-file /absolute/path/to/quant-pivot.toml --expected-environment local-development",
                postgres_preflight_access: PreflightAccess::ReadOnly,
                postgres_blocker: UpstreamBlocker::PostgresSchemaIdentityMismatch,
                clickhouse_preflight_command: "cargo xtask clickhouse-schema verify --config-file /absolute/path/to/quant-pivot.toml --expected-environment local-development",
                clickhouse_preflight_access: PreflightAccess::ReadOnly,
                clickhouse_facts: current_facts,
            },
            canonical_contract,
            capability_registry_hash: registry.registry_hash,
            families,
            historical_lineage,
            recovery: Self::weather_recovery(),
            official_source_references: Self::official_references(),
        }
    }

    fn blocked_contract(
        bindings: &WeatherVerticalBindingsConfig,
        stations: &WeatherStationRegistry,
    ) -> Self {
        let current_facts = CurrentFactMatrix::Unavailable {
            blocker: UpstreamBlocker::ClickhouseSchemaUnavailable,
            observation_rows: None,
            forecast_rows: None,
        };
        let historical_lineage = historical_fixture(bindings, stations);
        Self::build(bindings, stations, current_facts, historical_lineage)
    }

    fn validate(&self) {
        assert_eq!(
            self.format_version,
            WEATHER_READINESS_EVIDENCE_FORMAT_VERSION
        );
        assert_eq!(self.vertical, Vertical::Weather);
        assert_eq!(self.implementation, ImplementationVerdict::Closed);
        assert_eq!(self.publication, PublicationVerdict::Blocked);
        assert_eq!(
            self.operational_activation,
            OperationalActivationVerdict::Blocked
        );
        assert_eq!(self.activation_claim, ActivationClaim::NotClaimed);
        assert_eq!(
            self.canonical_contract.activation_eligibility,
            ResearchEvaluationTrack::ExecutionCandidate
        );
        assert_eq!(self.canonical_contract.minimum_mature_labels, 500);
        assert_eq!(self.canonical_contract.minimum_coverage, "0.95");
        assert_eq!(self.families.len(), WEATHER_FAMILIES.len());
        let distinct = self
            .families
            .iter()
            .map(|family| family.contract_family)
            .collect::<BTreeSet<_>>();
        assert_eq!(distinct, WEATHER_FAMILIES.into_iter().collect());
        for family in &self.families {
            assert_eq!(
                family.operational_activation,
                OperationalActivationVerdict::Blocked
            );
            assert!(matches!(
                family.current_mature_labels,
                CurrentMatureLabels::Unavailable {
                    current_mature_labels: None,
                    minimum_mature_labels: 500,
                    eligibility: CapabilityEligibility::InsufficientEvidence {
                        reason_code: DomainCapabilityReasonCode::MatureLabelsUnavailable,
                    },
                    ..
                }
            ));
            assert_eq!(
                family.pit_manifest.capability_registry_hash,
                self.capability_registry_hash
            );
        }
        assert_eq!(
            self.historical_lineage.disposition,
            NonCurrentDisposition::LineageOnlyNotCurrentReadiness
        );
        assert_eq!(self.historical_lineage.family_outcomes.len(), 8);
        assert!(
            self.historical_lineage
                .family_outcomes
                .iter()
                .all(|family| family.outcomes.unsupported_template == 0)
        );
        assert_eq!(
            self.recovery
                .iter()
                .filter(|step| step.access == RecoveryAccess::Mutating)
                .count(),
            2
        );
        assert!(
            self.recovery
                .iter()
                .filter(|step| step.access == RecoveryAccess::Mutating)
                .all(|step| step.authorization == RecoveryAuthorization::OperatorRequired)
        );
        let referenced_sources = self
            .official_source_references
            .iter()
            .map(|reference| reference.source_id)
            .collect::<BTreeSet<_>>();
        let capability_sources = self
            .families
            .iter()
            .flat_map(|family| &family.capability_contracts)
            .flat_map(|contract| &contract.source_bindings)
            .map(|binding| binding.source_id.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(referenced_sources, capability_sources);
    }

    fn weather_recovery() -> Vec<RecoveryStep> {
        vec![
            RecoveryStep {
                sequence: 1,
                access: RecoveryAccess::ReadOnly,
                authorization: RecoveryAuthorization::None,
                command: "cargo xtask postgres-schema plan --config-file /absolute/path/to/quant-pivot.toml --expected-environment local-development",
                success_condition: "the immutable PostgreSQL plan is reviewed without changing the target",
            },
            RecoveryStep {
                sequence: 2,
                access: RecoveryAccess::Mutating,
                authorization: RecoveryAuthorization::OperatorRequired,
                command: "cargo xtask postgres-schema apply --config-file /absolute/path/to/quant-pivot.toml --expected-environment local-development",
                success_condition: "the existing deploy owner applies the reviewed immutable PostgreSQL plan",
            },
            RecoveryStep {
                sequence: 3,
                access: RecoveryAccess::ReadOnly,
                authorization: RecoveryAuthorization::None,
                command: "cargo xtask postgres-schema verify --config-file /absolute/path/to/quant-pivot.toml --expected-environment local-development",
                success_condition: "the current PostgreSQL identity and runtime contract verify exactly",
            },
            RecoveryStep {
                sequence: 4,
                access: RecoveryAccess::Mutating,
                authorization: RecoveryAuthorization::OperatorRequired,
                command: "cargo xtask clickhouse-schema bootstrap --config-file /absolute/path/to/quant-pivot.toml --expected-environment local-development",
                success_condition: "the deploy owner bootstraps the sole schema into an absent or object-empty ClickHouse database",
            },
            RecoveryStep {
                sequence: 5,
                access: RecoveryAccess::ReadOnly,
                authorization: RecoveryAuthorization::None,
                command: "cargo xtask clickhouse-schema verify --config-file /absolute/path/to/quant-pivot.toml --expected-environment local-development",
                success_condition: "the current ClickHouse schema and Weather PIT fact tables verify exactly",
            },
            RecoveryStep {
                sequence: 6,
                access: RecoveryAccess::ServiceLifecycle,
                authorization: RecoveryAuthorization::OperatorRequired,
                command: "cargo run -p quant-pivot-bin -- --config-file /absolute/path/to/quant-pivot.toml --expected-environment local-development",
                success_condition: "the current binary reconciles the live catalog and official Weather source cursors without typed failures",
            },
            RecoveryStep {
                sequence: 7,
                access: RecoveryAccess::ReadOnly,
                authorization: RecoveryAuthorization::AuthenticatedReadRequired,
                command: "curl --fail-with-body --header \"Authorization: Bearer ${ACCESS_TOKEN:?set ACCESS_TOKEN}\" http://127.0.0.1:8088/api/research/feedback-overview",
                success_condition: "the current Weather profile, readiness snapshot, latest cycle, and coverage are authoritative and observable",
            },
            RecoveryStep {
                sequence: 8,
                access: RecoveryAccess::ReadOnly,
                authorization: RecoveryAuthorization::None,
                command: "cargo test -p quant-pivot-system-tests --test vertical_readiness_evidence weather_readiness_evidence -- --ignored --exact --nocapture",
                success_condition: "the current eight-family official-fact/PIT/label matrix is regenerated from pinned inputs without historical fallback",
            },
        ]
    }

    fn official_references() -> Vec<OfficialSourceReference> {
        vec![
            OfficialSourceReference {
                source_id: "airnow",
                url: "https://www.airnow.gov/about-airnow/",
            },
            OfficialSourceReference {
                source_id: "aviation_weather",
                url: "https://aviationweather.gov/data/api/",
            },
            OfficialSourceReference {
                source_id: "gefs",
                url: "https://www.emc.ncep.noaa.gov/emc/pages/numerical_forecast_systems/gefs.php",
            },
            OfficialSourceReference {
                source_id: "ghcnh",
                url: "https://www.ncei.noaa.gov/products/global-historical-climatology-network-hourly",
            },
            OfficialSourceReference {
                source_id: "ghcnd",
                url: "https://www.ncei.noaa.gov/products/land-based-station/global-historical-climatology-network-daily",
            },
            OfficialSourceReference {
                source_id: "hko_open_data",
                url: "https://www.hko.gov.hk/en/abouthko/opendata_intro.htm",
            },
            OfficialSourceReference {
                source_id: "nasa_gistemp",
                url: "https://data.giss.nasa.gov/gistemp/",
            },
            OfficialSourceReference {
                source_id: "ncei_storm_events",
                url: "https://www.ncei.noaa.gov/stormevents/",
            },
            OfficialSourceReference {
                source_id: "ncei_tornado_time_series",
                url: "https://www.ncei.noaa.gov/access/monitoring/tornadoes/time-series",
            },
            OfficialSourceReference {
                source_id: "nhc_advisory",
                url: "https://www.nhc.noaa.gov/data/",
            },
            OfficialSourceReference {
                source_id: "nhc_hurdat2",
                url: "https://www.nhc.noaa.gov/data/",
            },
            OfficialSourceReference {
                source_id: "nsidc_sea_ice_index",
                url: "https://nsidc.org/data/g02135/versions/4",
            },
            OfficialSourceReference {
                source_id: "nws_observation",
                url: "https://www.weather.gov/documentation/services-web-api",
            },
            OfficialSourceReference {
                source_id: "spc_storm_reports",
                url: "https://www.spc.noaa.gov/climo/reports/",
            },
        ]
    }

    fn write(&self) -> ArtifactReceipt {
        let output_dir = env::var_os("W4_E09_EVIDENCE_DIR").map_or_else(
            || Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/phase-11.9/w4-e09"),
            PathBuf::from,
        );
        fs::create_dir_all(&output_dir).unwrap_or_else(|error| {
            panic!(
                "create W4-E09 evidence directory {}: {error}",
                output_dir.display()
            )
        });
        let path = output_dir.join("weather-readiness-evidence-v1.json");
        let mut bytes =
            serde_json::to_vec_pretty(self).expect("serialize W4-E09 Weather readiness evidence");
        bytes.push(b'\n');
        fs::write(&path, &bytes)
            .unwrap_or_else(|error| panic!("write W4-E09 evidence {}: {error}", path.display()));
        ArtifactReceipt {
            path,
            content_hash: CanonicalDigest::content_hash_bytes(&bytes).to_string(),
        }
    }
}

fn validate_selectors(family: DomainContractFamily, contracts: &[DomainContractCapability]) {
    let capability_sources = contracts
        .iter()
        .flat_map(|contract| &contract.source_bindings)
        .map(|binding| binding.source_id.as_str())
        .collect::<BTreeSet<_>>();
    let selector_sources = CurrentOfficialFacts::selectors(family)
        .iter()
        .map(|selector| selector.source_id)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        selector_sources, capability_sources,
        "official fact selectors must cover exactly the capability sources"
    );
}

fn historical_fixture(
    bindings: &WeatherVerticalBindingsConfig,
    stations: &WeatherStationRegistry,
) -> HistoricalWeatherLineage {
    let registry = domain_capability_registry(
        &stations
            .registry_hash()
            .expect("hash fixture Weather station registry"),
        bindings,
    )
    .expect("build fixture capability registry");
    let content_hash =
        CanonicalDigest::content_hash_bytes(b"weather-readiness-contract-lineage-fixture");
    HistoricalWeatherLineage {
        disposition: NonCurrentDisposition::LineageOnlyNotCurrentReadiness,
        capability_audit_path: "contract-fixture/not-operational.json".to_owned(),
        capability_audit_sha256: "0".repeat(64),
        capability_audit_content_hash: content_hash.to_string(),
        capability_audit_artifact_hash: content_hash,
        pit_manifest_path: "contract-fixture/not-operational.json".to_owned(),
        pit_manifest_sha256: "0".repeat(64),
        pit_manifest_content_hash: content_hash.to_string(),
        capability_registry_hash: registry.registry_hash,
        catalog_hash: content_hash,
        weather_observation_rows: 0,
        weather_forecast_rows: 0,
        family_outcomes: WEATHER_FAMILIES
            .into_iter()
            .map(|family| HistoricalFamilyEvidence {
                contract_family: family,
                outcomes: HistoricalOutcomeCounts::default(),
            })
            .collect(),
    }
}

fn current_deploy() -> DeployConfig {
    let config_file = env::var_os("W4_E09_CONFIG_FILE").unwrap_or_else(|| {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../config/quant-pivot.toml")
            .into_os_string()
    });
    let expected_environment =
        env::var("W4_E09_EXPECTED_ENVIRONMENT").unwrap_or_else(|_| "local-development".to_owned());
    let environment = DeploymentEnvironment::parse(&expected_environment)
        .expect("W4-E09 expected environment is valid");
    let request = DeployConfigLoadRequest::new(PathBuf::from(config_file), environment);
    DeployConfig::load(&request).expect("load current deploy config without printing secrets")
}

#[test]
fn weather_readiness_contract() {
    let bindings = WeatherVerticalBindingsConfig::default();
    let stations = WeatherStationRegistry::try_new(builtin_weather_station_profiles())
        .expect("built-in Weather station registry");
    let manifest = WeatherReadinessEvidenceManifest::blocked_contract(&bindings, &stations);
    manifest.validate();
}

#[tokio::test]
#[ignore = "requires pinned W4-E07, historical capability/PIT, and current deploy evidence"]
async fn weather_readiness_evidence() {
    let e07 = E07Input::from_env();
    assert_eq!(
        e07.projection.current_environment.blocker_code,
        UpstreamBlocker::PostgresSchemaIdentityMismatch
    );
    assert!(!e07.projection.usable_for_crypto_readiness);
    assert!(!e07.projection.usable_for_weather_readiness);
    let deploy = current_deploy();
    let stations = WeatherStationRegistry::try_new(deploy.domain_sources.weather_stations.clone())
        .expect("current Weather station registry is valid");
    let current_facts = CurrentFactMatrix::load(&deploy).await;
    let historical_lineage = HistoricalWeatherLineage::load();
    let manifest = WeatherReadinessEvidenceManifest::build(
        &deploy.domain_sources.weather_vertical_bindings,
        &stations,
        current_facts,
        historical_lineage,
    );
    manifest.validate();
    let receipt = manifest.write();
    eprintln!(
        "W4-E09 Weather readiness evidence: path={} content_hash={}",
        receipt.path.display(),
        receipt.content_hash
    );
}
