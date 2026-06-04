//! Point-in-time materialization contracts shared by control-plane services.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    clickhouse::CalibrationSnapshotRow,
    domain::{
        BalanceSnapshotInfo, PositionInfo, PotentialLossInfo, ReconciliationReportInfo,
        RiskAuditEventInfo, RuntimeConfigVersionInfo, TokenBalanceSnapshotInfo, TradeInfo,
        control_factor::{ControlFactorValue, QualityGateEvaluationReport},
        settlement::ResolutionEventInfo,
    },
    enums::{
        common::MarketCategory,
        control_factor::{
            ControlFactorType, EvidenceStageStatus, MaterializationErrorCode,
            MaterializationOutputPolicy, MaterializationRunKind, MaterializationStageName,
            QualityGateName, RunTriggerType,
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

/// Deterministic replay and stress policy pinned into a materialization manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimulationConfig {
    pub max_replay_gap_ms: u64,
    pub stale_book_after_ms: u64,
    pub snapshot_limit_per_token: usize,
    pub fill_models: Vec<ReplayFillModel>,
    pub latency_buckets: Vec<LatencyBucketSpec>,
    pub adverse_selection_bps: Vec<u32>,
    pub exit_policy: ExitSimulationPolicy,
}

impl SimulationConfig {
    #[must_use]
    pub fn production_default() -> Self {
        Self {
            max_replay_gap_ms: 1_000,
            stale_book_after_ms: 5_000,
            snapshot_limit_per_token: 1,
            fill_models: vec![
                ReplayFillModel::StrictFok,
                ReplayFillModel::LatencyShiftedFok,
                ReplayFillModel::DepthWeighted,
                ReplayFillModel::AdverseSelectionStress,
            ],
            latency_buckets: vec![
                LatencyBucketSpec {
                    name: "p50".to_owned(),
                    shift_ms: 100,
                },
                LatencyBucketSpec {
                    name: "p95".to_owned(),
                    shift_ms: 500,
                },
            ],
            adverse_selection_bps: vec![25, 50, 100],
            exit_policy: ExitSimulationPolicy::report_only_default(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayFillModel {
    StrictFok,
    DepthWeighted,
    LatencyShiftedFok,
    AdverseSelectionStress,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatencyBucketSpec {
    pub name: String,
    pub shift_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExitSimulationPolicy {
    pub enabled: bool,
    pub fixed_stop_bps: Option<u32>,
    pub trailing_stop_bps: Option<u32>,
    pub time_stop_secs: Option<u64>,
    pub zone_invalidation_grace_secs: Option<u64>,
    pub min_bid_depth_shares: Option<Decimal>,
}

impl ExitSimulationPolicy {
    #[must_use]
    pub const fn report_only_default() -> Self {
        Self {
            enabled: true,
            fixed_stop_bps: Some(500),
            trailing_stop_bps: Some(250),
            time_stop_secs: Some(86_400),
            zone_invalidation_grace_secs: Some(300),
            min_bid_depth_shares: None,
        }
    }
}

/// Quality gate policy pinned into a materialization manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QualityGatePolicy {
    pub policy_hash: String,
    pub defaults: QualityGateDefaults,
    pub bucket_risk: FactorGateThresholds,
    pub execution_quality: FactorGateThresholds,
    pub portfolio_risk: FactorGateThresholds,
    pub reconciliation_health: FactorGateThresholds,
    pub market_anomaly: FactorGateThresholds,
    pub enabled_gates: Vec<QualityGateName>,
}

impl QualityGatePolicy {
    /// Thresholds for synthetic smoke fixtures with a single market and few opportunities.
    #[must_use]
    pub fn smoke_acceptance() -> Self {
        let mut policy = Self {
            policy_hash: "blake3:smoke_phase54_quality_gate_policy".to_owned(),
            ..Self::default()
        };
        policy.defaults.max_single_market_share_bps = 10_000;
        policy.defaults.max_single_event_share_bps = 10_000;
        for thresholds in [
            &mut policy.bucket_risk,
            &mut policy.execution_quality,
            &mut policy.portfolio_risk,
            &mut policy.reconciliation_health,
            &mut policy.market_anomaly,
        ] {
            thresholds.min_opportunities = 1;
            thresholds.min_markets = 1;
            thresholds.min_settlements = 0;
        }
        policy
            .enabled_gates
            .retain(|gate| !matches!(gate, QualityGateName::Leakage | QualityGateName::TailRisk));
        policy
    }

    #[must_use]
    pub const fn thresholds_for(&self, factor_type: ControlFactorType) -> &FactorGateThresholds {
        match factor_type {
            ControlFactorType::BucketRisk => &self.bucket_risk,
            ControlFactorType::ExecutionQuality => &self.execution_quality,
            ControlFactorType::PortfolioRisk => &self.portfolio_risk,
            ControlFactorType::ReconciliationHealth => &self.reconciliation_health,
            ControlFactorType::MarketAnomaly => &self.market_anomaly,
        }
    }
}

impl Default for QualityGatePolicy {
    fn default() -> Self {
        Self {
            policy_hash: "blake3:default_phase54_quality_gate_policy".to_owned(),
            defaults: QualityGateDefaults {
                max_single_market_share_bps: 5_000,
                max_single_event_share_bps: 5_000,
                min_confidence_level_bps: 9_500,
                max_tail_drawdown_pct_bps: 3_000,
                require_owner: true,
                require_ttl: true,
            },
            bucket_risk: FactorGateThresholds {
                min_opportunities: 100,
                min_markets: 20,
                min_settlements: 50,
                min_l2_coverage_bps: None,
                default_ttl_secs: 14 * 24 * 60 * 60,
                min_shadow_opportunities: Some(50),
            },
            execution_quality: FactorGateThresholds {
                min_opportunities: 200,
                min_markets: 20,
                min_settlements: 0,
                min_l2_coverage_bps: Some(9_500),
                default_ttl_secs: 3 * 24 * 60 * 60,
                min_shadow_opportunities: Some(100),
            },
            portfolio_risk: FactorGateThresholds {
                min_opportunities: 100,
                min_markets: 10,
                min_settlements: 30,
                min_l2_coverage_bps: None,
                default_ttl_secs: 7 * 24 * 60 * 60,
                min_shadow_opportunities: None,
            },
            reconciliation_health: FactorGateThresholds {
                min_opportunities: 0,
                min_markets: 0,
                min_settlements: 0,
                min_l2_coverage_bps: None,
                default_ttl_secs: 2 * 60 * 60,
                min_shadow_opportunities: None,
            },
            market_anomaly: FactorGateThresholds {
                min_opportunities: 0,
                min_markets: 1,
                min_settlements: 0,
                min_l2_coverage_bps: None,
                default_ttl_secs: 6 * 60 * 60,
                min_shadow_opportunities: None,
            },
            enabled_gates: vec![
                QualityGateName::PointInTime,
                QualityGateName::UpstreamStage,
                QualityGateName::Coverage,
                QualityGateName::Sample,
                QualityGateName::Leakage,
                QualityGateName::Stability,
                QualityGateName::TailRisk,
                QualityGateName::Conservative,
                QualityGateName::Ttl,
                QualityGateName::Owner,
            ],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QualityGateDefaults {
    pub max_single_market_share_bps: u32,
    pub max_single_event_share_bps: u32,
    pub min_confidence_level_bps: u32,
    pub max_tail_drawdown_pct_bps: u32,
    pub require_owner: bool,
    pub require_ttl: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorGateThresholds {
    pub min_opportunities: u64,
    pub min_markets: u64,
    pub min_settlements: u64,
    pub min_l2_coverage_bps: Option<u32>,
    pub default_ttl_secs: u64,
    pub min_shadow_opportunities: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorBuildArtifact {
    pub run_id: MaterializationRunId,
    pub built_factors: Vec<ControlFactorValue>,
    pub report_only_factors: Vec<ControlFactorValue>,
    pub rejected_factors: Vec<ControlFactorValue>,
    pub warnings: Vec<StageWarning>,
}

impl FactorBuildArtifact {
    #[must_use]
    pub fn factor_count(&self) -> u64 {
        self.built_factors
            .len()
            .saturating_add(self.report_only_factors.len())
            .saturating_add(self.rejected_factors.len())
            .try_into()
            .unwrap_or(u64::MAX)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QualityGateEvaluationArtifact {
    pub run_id: MaterializationRunId,
    pub report: QualityGateEvaluationReport,
    pub factors: Vec<ControlFactorValue>,
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
    pub quality_gate_policy: QualityGatePolicy,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputResolutionReport {
    pub run_id: MaterializationRunId,
    pub window: TimeWindowSpec,
    pub manifest: PointInTimeInputManifest,
    pub market_contexts: Vec<MarketReplayContext>,
    pub source_bundle: EvidenceSourceBundle,
}

/// Market/token replay context resolved point-in-time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketReplayContext {
    pub market_id: MarketId,
    pub event_id: Option<EventId>,
    pub yes_token_id: TokenId,
    pub no_token_id: TokenId,
    pub category: Option<MarketCategory>,
    pub settlement_deadline: Option<DateTime<Utc>>,
    pub resolved_as_of: DateTime<Utc>,
    pub source_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvidenceSourceBundle {
    pub runtime_config: Option<RuntimeConfigVersionInfo>,
    pub calibration_snapshots: Vec<CalibrationSnapshotRow>,
    pub trades: Vec<TradeInfo>,
    pub positions: Vec<PositionInfo>,
    pub potential_loss_baseline: Vec<PotentialLossInfo>,
    pub potential_loss_changes: Vec<PotentialLossInfo>,
    pub risk_audit_events: Vec<RiskAuditEventInfo>,
    pub balance_snapshot: Option<BalanceSnapshotInfo>,
    pub token_balance_snapshots: Vec<TokenBalanceSnapshotInfo>,
    pub settlement_truth: Vec<ResolutionEventInfo>,
    pub reconciliation_reports: Vec<ReconciliationReportInfo>,
    pub query_fingerprints: Vec<QueryFingerprint>,
}

impl EvidenceSourceBundle {
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            runtime_config: None,
            calibration_snapshots: Vec::new(),
            trades: Vec::new(),
            positions: Vec::new(),
            potential_loss_baseline: Vec::new(),
            potential_loss_changes: Vec::new(),
            risk_audit_events: Vec::new(),
            balance_snapshot: None,
            token_balance_snapshots: Vec::new(),
            settlement_truth: Vec::new(),
            reconciliation_reports: Vec::new(),
            query_fingerprints: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::SimulationConfig;
    use crate::enums::control_factor::{MaterializationErrorCode, MaterializationStageName};

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

    #[test]
    fn phase_53_stage_names_round_trip() {
        assert_eq!(
            MaterializationStageName::ExitTokenEvidence.as_str(),
            "exit_token_evidence"
        );
        assert_eq!(
            MaterializationStageName::TrainingExampleBuild.as_str(),
            "training_example_build"
        );
        let encoded = serde_json::to_string(&MaterializationStageName::ExitTokenEvidence)
            .expect("serialize stage");
        assert_eq!(encoded, "\"exit_token_evidence\"");
    }

    #[test]
    fn simulation_config_is_typed_not_hash_only() {
        let config = SimulationConfig::production_default();
        assert!(config.max_replay_gap_ms > 0);
        assert!(config.stale_book_after_ms >= config.max_replay_gap_ms);
        assert!(!config.fill_models.is_empty());
        assert!(config.exit_policy.enabled);
    }
}
