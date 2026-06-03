//! Point-in-time materialization contracts shared by control-plane services.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    enums::{
        common::MarketCategory,
        control_factor::{
            ControlFactorType, EvidenceStageStatus, MaterializationErrorCode,
            MaterializationOutputPolicy, MaterializationRunKind, MaterializationStageName,
            RunTriggerType,
        },
    },
    types::{
        EventId, MarketId, MaterializationRunId, RuntimeConfigVersionId, StageReportId, TokenId,
    },
};

/// Immutable wall-clock window for a materialization run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeWindowSpec {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
}

impl TimeWindowSpec {
    #[must_use]
    pub const fn new(from: DateTime<Utc>, to: DateTime<Utc>) -> Self {
        Self { from, to }
    }

    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.from < self.to
    }
}

/// Stable market filter persisted in the run manifest.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct MarketFilterSpec {
    pub market_ids: Vec<MarketId>,
    pub event_ids: Vec<EventId>,
    pub token_ids: Vec<TokenId>,
    pub categories: Vec<MarketCategory>,
}

/// External or internal trigger metadata for a materialization run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "trigger_type")]
pub enum RunTrigger {
    Scheduled {
        schedule_id: String,
    },
    Backfill {
        request_id: String,
        reason: String,
        force_new_run: bool,
    },
    Incident {
        incident_id: String,
        reason: String,
        force_new_run: bool,
    },
    ConfigComparison {
        candidate_config_hash: String,
        reason: String,
    },
    ForensicReport {
        request_id: String,
        reason: String,
    },
}

impl RunTrigger {
    #[must_use]
    pub const fn trigger_type(&self) -> RunTriggerType {
        match self {
            Self::Scheduled { .. } => RunTriggerType::Scheduled,
            Self::Backfill { .. } => RunTriggerType::Backfill,
            Self::Incident { .. } => RunTriggerType::Incident,
            Self::ConfigComparison { .. } => RunTriggerType::ConfigComparison,
            Self::ForensicReport { .. } => RunTriggerType::ForensicReport,
        }
    }

    #[must_use]
    pub fn trigger_ref(&self) -> Option<&str> {
        match self {
            Self::Scheduled { schedule_id } => Some(schedule_id.as_str()),
            Self::Backfill { request_id, .. } | Self::ForensicReport { request_id, .. } => {
                Some(request_id.as_str())
            }
            Self::Incident { incident_id, .. } => Some(incident_id.as_str()),
            Self::ConfigComparison {
                candidate_config_hash,
                ..
            } => Some(candidate_config_hash.as_str()),
        }
    }

    #[must_use]
    pub const fn force_new_run(&self) -> bool {
        matches!(
            self,
            Self::Backfill {
                force_new_run: true,
                ..
            } | Self::Incident {
                force_new_run: true,
                ..
            }
        )
    }

    #[must_use]
    pub fn reason(&self) -> Option<&str> {
        match self {
            Self::Backfill { reason, .. }
            | Self::Incident { reason, .. }
            | Self::ConfigComparison { reason, .. }
            | Self::ForensicReport { reason, .. } => Some(reason.as_str()),
            Self::Scheduled { .. } => None,
        }
    }
}

/// Point-in-time input domains that may be required by later evidence stages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequiredInputDomain {
    MarketMetadata,
    TokenMapping,
    FeeSchedule,
    RuntimeConfig,
    CalibrationSnapshots,
    Trades,
    Positions,
    RiskState,
    BalanceSnapshot,
    TokenBalanceSnapshot,
    SettlementTruth,
    ReconciliationStatus,
}

/// Input requirements declared by the manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataRequirements {
    pub required_inputs: Vec<RequiredInputDomain>,
    pub production_required_inputs: Vec<RequiredInputDomain>,
    pub min_l2_coverage_ratio: Option<Decimal>,
    pub require_settlement_truth: bool,
    pub require_token_balances: bool,
}

impl DataRequirements {
    #[must_use]
    pub fn requires(&self, domain: RequiredInputDomain) -> bool {
        self.required_inputs.contains(&domain) || self.production_required_inputs.contains(&domain)
    }

    #[must_use]
    pub fn production_requires(&self, domain: RequiredInputDomain) -> bool {
        self.production_required_inputs.contains(&domain)
    }

    #[must_use]
    pub fn requires_settlement_truth(&self) -> bool {
        self.require_settlement_truth || self.requires(RequiredInputDomain::SettlementTruth)
    }

    #[must_use]
    pub fn requires_token_balances(&self) -> bool {
        self.require_token_balances || self.requires(RequiredInputDomain::TokenBalanceSnapshot)
    }

    #[must_use]
    pub const fn requires_l2_coverage(&self) -> bool {
        self.min_l2_coverage_ratio.is_some()
    }
}

/// Account boundary for PIT balance and token-balance evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayAccountScope {
    pub holder_address: String,
}

/// Runtime-config version pinning mode for PIT materialization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub enum RuntimeConfigRef {
    ActiveAt {
        at: DateTime<Utc>,
    },
    Version {
        version_id: RuntimeConfigVersionId,
        config_hash: String,
    },
    Hash {
        config_hash: String,
    },
}

/// Hash-only simulation policy reference for deterministic manifest identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimulationConfig {
    pub config_hash: String,
}

/// Hash-only quality gate policy reference for deterministic manifest identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QualityGatePolicyRef {
    pub policy_hash: String,
}

/// Immutable materialization manifest. Operator edits must create a new run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterializationRunManifest {
    pub run_id: MaterializationRunId,
    pub run_kind: MaterializationRunKind,
    pub trigger: RunTrigger,
    pub window: TimeWindowSpec,
    pub source_delay_secs: u64,
    pub markets: MarketFilterSpec,
    #[serde(default)]
    pub replay_account_scope: Option<ReplayAccountScope>,
    pub requested_factor_types: Vec<ControlFactorType>,
    pub data_requirements: DataRequirements,
    pub runtime_config_ref: RuntimeConfigRef,
    pub simulation_config: SimulationConfig,
    pub quality_gate_policy: QualityGatePolicyRef,
    pub output_policy: MaterializationOutputPolicy,
    pub code_git_sha: String,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
}

impl MaterializationRunManifest {
    #[must_use]
    pub const fn production_output_allowed(&self) -> bool {
        matches!(
            self.output_policy,
            MaterializationOutputPolicy::EmitDraftCandidates
                | MaterializationOutputPolicy::EmitDraftOnly
        ) && !matches!(self.run_kind, MaterializationRunKind::ForensicReport)
    }

    #[must_use]
    pub const fn trigger_type(&self) -> RunTriggerType {
        self.trigger.trigger_type()
    }
}

/// Input passed to one materialization stage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageInput<T> {
    pub run_id: MaterializationRunId,
    pub manifest: MaterializationRunManifest,
    pub prior: Option<T>,
}

/// Output returned by one materialization stage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageOutput<T> {
    pub stage_report: StageReportBody,
    pub artifact: Option<T>,
}

/// Stable digest identifying a materialized artifact.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ArtifactHash(pub String);

/// Stable digest identifying a repository query contract.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QueryFingerprint(pub String);

/// Artifact dependency recorded in a stage report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageArtifactRef {
    pub stage_name: MaterializationStageName,
    pub artifact_hash: ArtifactHash,
}

/// Coverage details attached to a stage report or PIT source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageCoverageReport {
    pub expected_rows: u64,
    pub observed_rows: u64,
    pub missing_rows: u64,
    pub coverage_ratio: Decimal,
    pub insufficient_reasons: Vec<String>,
}

impl StageCoverageReport {
    #[must_use]
    pub const fn complete(observed_rows: u64) -> Self {
        Self {
            expected_rows: observed_rows,
            observed_rows,
            missing_rows: 0,
            coverage_ratio: Decimal::ONE,
            insufficient_reasons: Vec::new(),
        }
    }

    #[must_use]
    pub fn is_sufficient(&self) -> bool {
        self.missing_rows == 0 && self.insufficient_reasons.is_empty()
    }
}

/// Non-fatal warning retained for audit and UI.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageWarning {
    pub code: String,
    pub message: String,
}

/// Fatal or coverage-affecting stage error retained for audit and retry policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageError {
    pub code: MaterializationErrorCode,
    pub message: String,
    pub retryable: bool,
    pub fatal_for_production: bool,
}

impl StageError {
    #[must_use]
    pub fn new(code: MaterializationErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            retryable: code.is_retryable(),
            fatal_for_production: code.is_fatal_for_production(),
        }
    }
}

/// Persisted body for a stage report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageReportBody {
    pub stage_report_id: StageReportId,
    pub run_id: MaterializationRunId,
    pub stage_name: MaterializationStageName,
    pub status: EvidenceStageStatus,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub input_artifact_hashes: Vec<StageArtifactRef>,
    pub output_artifact_hash: Option<ArtifactHash>,
    pub coverage: StageCoverageReport,
    pub metrics: serde_json::Value,
    pub records_read: u64,
    pub records_written: u64,
    pub warnings: Vec<StageWarning>,
    pub errors: Vec<StageError>,
    pub query_fingerprints: Vec<QueryFingerprint>,
}

/// Source-specific fallback behavior. Production resolvers should normally use `None`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputFallbackPolicy {
    None,
    ReportOnlyMissing,
}

/// One point-in-time source resolved for a run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PointInTimeInputSource {
    pub domain: RequiredInputDomain,
    pub source_table: String,
    pub source_repository: String,
    pub query_window: Option<TimeWindowSpec>,
    pub as_of: DateTime<Utc>,
    pub query_fingerprint: QueryFingerprint,
    pub row_count: u64,
    pub coverage: StageCoverageReport,
    pub snapshot_hash: Option<String>,
    pub fallback_policy: InputFallbackPolicy,
    pub production_required: bool,
    pub resolved: bool,
}

/// Missing input recorded in the PIT manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissingPointInTimeInput {
    pub domain: RequiredInputDomain,
    pub code: MaterializationErrorCode,
    pub detail: String,
    pub production_required: bool,
}

/// Versioned point-in-time inputs used to rebuild a historical decision context.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PointInTimeInputManifest {
    pub inputs: Vec<PointInTimeInputSource>,
    pub production_eligible: bool,
    pub missing_inputs: Vec<MissingPointInTimeInput>,
    pub fatal_errors: Vec<MaterializationErrorCode>,
    pub warnings: Vec<StageWarning>,
    pub manifest_hash: String,
}

impl PointInTimeInputManifest {
    #[must_use]
    pub fn is_production_eligible(&self) -> bool {
        self.production_eligible && self.fatal_errors.is_empty()
    }
}

/// Output artifact of the `resolve_inputs` stage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputResolutionReport {
    pub run_id: MaterializationRunId,
    pub window: TimeWindowSpec,
    pub manifest: PointInTimeInputManifest,
    pub market_contexts: Vec<MarketReplayContext>,
}

/// Market/token replay context resolved point-in-time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketReplayContext {
    pub market_id: MarketId,
    pub event_id: Option<EventId>,
    pub yes_token_id: TokenId,
    pub no_token_id: TokenId,
    pub category: Option<MarketCategory>,
    pub resolved_as_of: DateTime<Utc>,
    pub source_hash: String,
}

#[cfg(test)]
mod tests {
    use crate::enums::control_factor::MaterializationErrorCode;
    use std::str::FromStr;

    #[test]
    fn stable_error_code_round_trips_as_string() {
        let code = MaterializationErrorCode::InputMarketMappingMissing;
        let json = serde_json::to_string(&code).expect("serialize code");
        assert_eq!(json, "\"input.market_mapping_missing\"");
        let decoded: MaterializationErrorCode =
            serde_json::from_str(&json).expect("deserialize code");
        assert_eq!(decoded, code);
    }

    #[test]
    fn retryability_is_stable_for_l2_coverage() {
        let code =
            MaterializationErrorCode::from_str("ch.coverage_l2_insufficient").expect("known code");
        assert!(code.is_retryable());
        assert!(!code.is_fatal_for_production());
    }
}
