//! Testable scheduling policy for offline materialization runs.
//!
//! The policy is pure data plus a handful of side-effect-free helpers. The
//! scheduler ([`super::MaterializationScheduler`]) consumes it on each tick to
//! decide which scheduled cadences are due and which are overdue / stale. None
//! of this module touches a wall clock or a repository, so every decision is
//! unit-testable with an injected `now`.

use chrono::{DateTime, Duration, Utc};
use oxide_arb_models::{
    domain::{
        MaterializationScheduleActivationView, MaterializationScheduleInactiveReasonView,
        MaterializationScheduleModeContractView,
        control_factor::{
            DataRequirements, MarketFilterSpec, QualityGatePolicy, ReplayAccountScope,
            RequiredInputDomain, RuntimeConfigRef, SimulationConfig,
        },
    },
    enums::{
        common::ExecutionMode,
        control_factor::{ControlFactorType, MaterializationOutputPolicy},
    },
};

/// One scheduled materialization cadence and its sealed-manifest inputs.
///
/// `cadence` is the wall-clock window length and re-run interval; the scheduler
/// pins it into the manifest as both the trigger interval and the dedupe basis.
#[derive(Debug, Clone)]
pub struct ScheduledMaterialization {
    /// Stable schedule identifier, persisted as the run `trigger_ref`.
    pub schedule_id: String,
    /// Cadence (window length and re-run interval).
    pub cadence: Duration,
    /// Source-delay applied to the trigger time to compute the window end.
    pub source_delay_secs: u64,
    /// Factor families this cadence should materialize.
    pub requested_factor_types: Vec<ControlFactorType>,
    /// Market filter pinned into the manifest.
    pub markets: MarketFilterSpec,
    /// Point-in-time input requirements pinned into the manifest.
    pub data_requirements: DataRequirements,
    /// Runtime-config version pinning mode.
    pub runtime_config_ref: RuntimeConfigRef,
    /// Deterministic replay / stress policy.
    pub simulation_config: SimulationConfig,
    /// Quality gate policy pinned into the manifest.
    pub quality_gate_policy: QualityGatePolicy,
    /// Output policy (draft emission vs report-only). The scheduler never
    /// publishes; this only governs the downstream execute worker.
    pub output_policy: MaterializationOutputPolicy,
    /// Optional account boundary for balance / token-balance evidence.
    pub replay_account_scope: Option<ReplayAccountScope>,
    /// Whether this cadence is active for the current execution mode.
    pub activation: ScheduleActivation,
    /// Stable operator-facing mode contract for this cadence.
    pub mode_contract: ScheduleModeContract,
}

/// Runtime activation state of a scheduled cadence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleActivation {
    Runnable,
    Inactive { reason: ScheduleInactiveReason },
}

impl ScheduleActivation {
    #[must_use]
    pub const fn is_runnable(self) -> bool {
        matches!(self, Self::Runnable)
    }
}

/// Why a schedule is inactive for the current execution mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleInactiveReason {
    UnsupportedExecutionMode,
    LiveOnlyEvidence,
    EvidenceWarmup,
}

/// Stable mode contract documented for operator surfaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleModeContract {
    AllModes,
    LiveOnly,
    LiveAfterEvidenceWarmup,
}

/// Full set of scheduled cadences plus shared attribution metadata.
#[derive(Debug, Clone)]
pub struct SchedulePolicy {
    /// Scheduled cadences evaluated on every tick.
    pub tasks: Vec<ScheduledMaterialization>,
    /// Attribution stamped into the manifest `created_by`.
    pub created_by: String,
    /// Code git SHA stamped into the manifest.
    pub code_git_sha: String,
}

impl SchedulePolicy {
    /// Mode-aware production schedule catalog covering the periodic cadences
    /// from master §6.
    ///
    /// `market-anomaly-event` is intentionally excluded: it is incident-driven
    /// (event-triggered runs) rather than periodic, so it is not part of the
    /// periodic scheduler.
    #[must_use]
    pub fn for_mode(
        mode: ExecutionMode,
        runtime_config_ref: RuntimeConfigRef,
        created_by: impl Into<String>,
        code_git_sha: impl Into<String>,
    ) -> Self {
        let source_delay_secs = 900;
        let tasks = vec![
            ScheduledMaterialization {
                schedule_id: "execution-quality-hourly".to_owned(),
                cadence: Duration::hours(1),
                source_delay_secs,
                requested_factor_types: vec![ControlFactorType::ExecutionQuality],
                markets: MarketFilterSpec::default(),
                data_requirements: data_requirements(&[
                    RequiredInputDomain::RuntimeConfig,
                    RequiredInputDomain::Trades,
                    RequiredInputDomain::CalibrationSnapshots,
                ]),
                runtime_config_ref: runtime_config_ref.clone(),
                simulation_config: SimulationConfig::production_default(),
                quality_gate_policy: QualityGatePolicy::default(),
                output_policy: MaterializationOutputPolicy::EmitDraftCandidates,
                replay_account_scope: None,
                activation: ScheduleActivation::Runnable,
                mode_contract: ScheduleModeContract::AllModes,
            },
            ScheduledMaterialization {
                schedule_id: "reconciliation-health-hourly".to_owned(),
                cadence: Duration::hours(1),
                source_delay_secs,
                requested_factor_types: vec![ControlFactorType::ReconciliationHealth],
                markets: MarketFilterSpec::default(),
                data_requirements: data_requirements(&[
                    RequiredInputDomain::RuntimeConfig,
                    RequiredInputDomain::ReconciliationStatus,
                    RequiredInputDomain::BalanceSnapshot,
                ]),
                runtime_config_ref: runtime_config_ref.clone(),
                simulation_config: SimulationConfig::production_default(),
                quality_gate_policy: QualityGatePolicy::default(),
                output_policy: MaterializationOutputPolicy::EmitDraftCandidates,
                replay_account_scope: None,
                activation: live_only_activation(mode, ScheduleInactiveReason::LiveOnlyEvidence),
                mode_contract: ScheduleModeContract::LiveOnly,
            },
            ScheduledMaterialization {
                schedule_id: "bucket-risk-daily".to_owned(),
                cadence: Duration::days(1),
                source_delay_secs,
                requested_factor_types: vec![ControlFactorType::BucketRisk],
                markets: MarketFilterSpec::default(),
                data_requirements: settlement_data_requirements(&[
                    RequiredInputDomain::RuntimeConfig,
                    RequiredInputDomain::Trades,
                    RequiredInputDomain::Positions,
                    RequiredInputDomain::SettlementTruth,
                ]),
                runtime_config_ref: runtime_config_ref.clone(),
                simulation_config: SimulationConfig::production_default(),
                quality_gate_policy: QualityGatePolicy::default(),
                output_policy: MaterializationOutputPolicy::EmitDraftCandidates,
                replay_account_scope: None,
                activation: ScheduleActivation::Runnable,
                mode_contract: ScheduleModeContract::AllModes,
            },
            ScheduledMaterialization {
                schedule_id: "portfolio-risk-daily".to_owned(),
                cadence: Duration::days(1),
                source_delay_secs,
                requested_factor_types: vec![ControlFactorType::PortfolioRisk],
                markets: MarketFilterSpec::default(),
                data_requirements: settlement_data_requirements(&[
                    RequiredInputDomain::RuntimeConfig,
                    RequiredInputDomain::Positions,
                    RequiredInputDomain::RiskState,
                    RequiredInputDomain::SettlementTruth,
                ]),
                runtime_config_ref,
                simulation_config: SimulationConfig::production_default(),
                quality_gate_policy: QualityGatePolicy::default(),
                output_policy: MaterializationOutputPolicy::EmitDraftCandidates,
                replay_account_scope: None,
                activation: live_only_activation(mode, ScheduleInactiveReason::EvidenceWarmup),
                mode_contract: ScheduleModeContract::LiveAfterEvidenceWarmup,
            },
        ];
        Self {
            tasks,
            created_by: created_by.into(),
            code_git_sha: code_git_sha.into(),
        }
    }
}

const fn live_only_activation(
    mode: ExecutionMode,
    inactive_reason: ScheduleInactiveReason,
) -> ScheduleActivation {
    match mode {
        ExecutionMode::Live => ScheduleActivation::Runnable,
        ExecutionMode::DryRun | ExecutionMode::Paper => ScheduleActivation::Inactive {
            reason: inactive_reason,
        },
    }
}

fn data_requirements(required: &[RequiredInputDomain]) -> DataRequirements {
    DataRequirements {
        required_inputs: required.to_vec(),
        production_required_inputs: required.to_vec(),
        min_l2_coverage_ratio: None,
        require_settlement_truth: false,
    }
}

fn settlement_data_requirements(required: &[RequiredInputDomain]) -> DataRequirements {
    DataRequirements {
        require_settlement_truth: true,
        ..data_requirements(required)
    }
}

/// Severity of a stale schedule (no recent *successful* run).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StaleSeverity {
    /// The schedule has never produced a successful run.
    NeverSucceeded,
    /// The last successful run is older than the staleness threshold.
    Stale,
}

/// Returns `true` when a new run should be enqueued for the cadence.
///
/// A schedule that has never run is always due; otherwise it is due once at
/// least `cadence` has elapsed since the most recent run (any status).
#[must_use]
pub fn is_due(now: DateTime<Utc>, last_run_at: Option<DateTime<Utc>>, cadence: Duration) -> bool {
    last_run_at.is_none_or(|last| now - last >= cadence)
}

/// Classifies staleness from the most recent *successful* run.
///
/// Returns `None` while a success exists within `2 * cadence`, otherwise the
/// appropriate [`StaleSeverity`]. A schedule that never succeeded is always
/// reported as [`StaleSeverity::NeverSucceeded`].
#[must_use]
pub fn staleness(
    now: DateTime<Utc>,
    last_success_at: Option<DateTime<Utc>>,
    cadence: Duration,
) -> Option<StaleSeverity> {
    match last_success_at {
        None => Some(StaleSeverity::NeverSucceeded),
        Some(last) if now - last > cadence * 2 => Some(StaleSeverity::Stale),
        Some(_) => None,
    }
}

/// Returns `true` when the most recent run (any status) is older than the
/// overdue threshold (`2 * cadence`), signalling missed cadence.
#[must_use]
pub fn is_overdue(now: DateTime<Utc>, last_run_at: DateTime<Utc>, cadence: Duration) -> bool {
    now - last_run_at > cadence * 2
}

/// Overdue / stale threshold in seconds (`2 * cadence`), clamped to non-negative.
#[must_use]
pub fn staleness_threshold_secs(cadence: Duration) -> u64 {
    u64::try_from((cadence * 2).num_seconds()).unwrap_or(0)
}

impl From<ScheduleInactiveReason> for MaterializationScheduleInactiveReasonView {
    fn from(reason: ScheduleInactiveReason) -> Self {
        match reason {
            ScheduleInactiveReason::UnsupportedExecutionMode => Self::UnsupportedExecutionMode,
            ScheduleInactiveReason::LiveOnlyEvidence => Self::LiveOnlyEvidence,
            ScheduleInactiveReason::EvidenceWarmup => Self::EvidenceWarmup,
        }
    }
}

impl From<ScheduleActivation> for MaterializationScheduleActivationView {
    fn from(activation: ScheduleActivation) -> Self {
        match activation {
            ScheduleActivation::Runnable => Self::Runnable,
            ScheduleActivation::Inactive { reason } => Self::Inactive {
                reason: reason.into(),
            },
        }
    }
}

impl From<ScheduleModeContract> for MaterializationScheduleModeContractView {
    fn from(contract: ScheduleModeContract) -> Self {
        match contract {
            ScheduleModeContract::AllModes => Self::AllModes,
            ScheduleModeContract::LiveOnly => Self::LiveOnly,
            ScheduleModeContract::LiveAfterEvidenceWarmup => Self::LiveAfterEvidenceWarmup,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use chrono::{Duration, TimeZone, Utc};
    use oxide_arb_test_support::materialization::scheduler_fixed_now;

    use oxide_arb_models::enums::common::ExecutionMode;

    use super::{
        RuntimeConfigRef, ScheduleActivation, SchedulePolicy, StaleSeverity, is_due, is_overdue,
        staleness,
    };

    fn at(hours: i64) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 6, 5, 0, 0, 0)
            .single()
            .expect("fixed timestamp")
            + Duration::hours(hours)
    }

    #[test]
    fn never_run_is_due() {
        assert!(is_due(at(0), None, Duration::hours(1)));
    }

    #[test]
    fn due_exactly_at_cadence_boundary() {
        let cadence = Duration::hours(1);
        assert!(is_due(at(1), Some(at(0)), cadence));
        assert!(!is_due(at(0) + Duration::minutes(59), Some(at(0)), cadence));
    }

    #[test]
    fn never_succeeded_is_always_stale() {
        assert_eq!(
            staleness(at(0), None, Duration::hours(1)),
            Some(StaleSeverity::NeverSucceeded)
        );
    }

    #[test]
    fn staleness_triggers_beyond_two_cadences() {
        let cadence = Duration::hours(1);
        assert_eq!(staleness(at(2), Some(at(0)), cadence), None);
        assert_eq!(
            staleness(at(2) + Duration::minutes(1), Some(at(0)), cadence),
            Some(StaleSeverity::Stale)
        );
    }

    #[test]
    fn overdue_triggers_beyond_two_cadences() {
        let cadence = Duration::hours(1);
        assert!(!is_overdue(at(2), at(0), cadence));
        assert!(is_overdue(at(2) + Duration::minutes(1), at(0), cadence));
    }

    #[test]
    fn mode_aware_catalog_excludes_market_anomaly() {
        let policy = SchedulePolicy::for_mode(
            ExecutionMode::Live,
            RuntimeConfigRef::ActiveAt {
                at: scheduler_fixed_now(),
            },
            "scheduler",
            "abc",
        );
        let ids: HashSet<&str> = policy
            .tasks
            .iter()
            .map(|task| task.schedule_id.as_str())
            .collect();
        assert_eq!(policy.tasks.len(), 4);
        assert!(ids.contains("execution-quality-hourly"));
        assert!(ids.contains("reconciliation-health-hourly"));
        assert!(ids.contains("bucket-risk-daily"));
        assert!(ids.contains("portfolio-risk-daily"));
        assert!(!ids.contains("market-anomaly-event"));
    }

    #[test]
    fn dry_run_disables_live_only_and_warmup_schedules() {
        let policy = SchedulePolicy::for_mode(
            ExecutionMode::DryRun,
            RuntimeConfigRef::ActiveAt {
                at: scheduler_fixed_now(),
            },
            "scheduler",
            "abc",
        );

        let recon = policy
            .tasks
            .iter()
            .find(|task| task.schedule_id == "reconciliation-health-hourly")
            .expect("reconciliation schedule");
        let portfolio = policy
            .tasks
            .iter()
            .find(|task| task.schedule_id == "portfolio-risk-daily")
            .expect("portfolio schedule");
        assert!(!recon.activation.is_runnable());
        assert!(!portfolio.activation.is_runnable());
    }

    #[test]
    fn live_enables_all_schedules() {
        let policy = SchedulePolicy::for_mode(
            ExecutionMode::Live,
            RuntimeConfigRef::ActiveAt {
                at: scheduler_fixed_now(),
            },
            "scheduler",
            "abc",
        );
        assert!(
            policy
                .tasks
                .iter()
                .all(|task| task.activation == ScheduleActivation::Runnable)
        );
    }
}
