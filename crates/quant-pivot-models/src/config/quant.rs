//! Quant pivot deploy configuration (`[quant]`, restart to apply).

use serde::Deserialize;

use crate::enums::quant::{ExecutionWalletKind, ResearchJobKind};

/// Quant-specific structural parameters.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QuantDeployConfig {
    /// Background worker topology.
    pub workers: QuantWorkersConfig,
    /// Venue account read configuration.
    pub account: QuantAccountDeployConfig,
    /// Async research-job engine tunables (concurrency, lease, recovery, guards).
    pub research_jobs: ResearchJobsConfig,
}

/// Async research-job engine tunables (`[quant.research_jobs]`, restart to apply).
///
/// These are operational/infra knobs (worker concurrency, lease/heartbeat
/// cadence, crash-recovery cap, dry-run/plan sampling, build resource guard) —
/// distinct from the frozen, per-dataset **runtime** config that governs
/// modeling semantics and is captured in the `dataset_hash`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ResearchJobsConfig {
    /// Maximum jobs executing concurrently across all kinds.
    pub global_concurrency: usize,
    /// Per-kind concurrency cap for dataset builds (the heaviest kind).
    pub dataset_build_concurrency: usize,
    /// Per-kind concurrency cap for model-training runs.
    pub model_train_concurrency: usize,
    /// Per-kind concurrency cap for backtests.
    pub backtest_concurrency: usize,
    /// Per-kind concurrency cap for favorite-longshot bias-table fits.
    pub bias_table_fit_concurrency: usize,
    /// Per-kind concurrency cap for model-score calibrator fits.
    pub model_calibration_fit_concurrency: usize,
    /// Per-kind concurrency cap for CPCV + trial-grid validation runs — the
    /// second-heaviest kind after dataset builds (rayon-parallel
    /// internally, so a low cap of `1` keeps host CPU predictable).
    pub cpcv_backtest_concurrency: usize,
    /// Per-kind cap for deterministic feature-parity replay.
    pub feature_parity_concurrency: usize,
    /// Mandatory page, concurrency, memory, and deadline envelope for durable
    /// feature-parity replay.
    #[serde(default = "FeatureParityComputeConfig::missing")]
    pub feature_parity_compute: FeatureParityComputeConfig,
    /// Per-kind cap for frozen feedback coverage scans.
    pub feedback_coverage_concurrency: usize,
    /// Per-kind cap for statistical drift computation.
    pub feedback_drift_concurrency: usize,
    /// Per-kind cap for each bounded feedback learning-stage batch.
    pub feedback_learning_concurrency: usize,
    /// Mandatory CPU/memory/deadline envelope for attribution and decision
    /// intervention replay.
    #[serde(default = "FeedbackAttributionComputeConfig::missing")]
    pub feedback_attribution_compute: FeedbackAttributionComputeConfig,
    /// Maximum feedback cycles concurrently owned by one resident coordinator.
    pub feedback_cycle_concurrency: usize,
    /// Per-kind cap for executable trade-policy fits.
    pub trade_policy_fit_concurrency: usize,
    /// Per-kind cap for independent row-level trade-policy validation.
    pub trade_policy_validation_concurrency: usize,
    /// Lease time-to-live: a lease not renewed within this window is reclaimable.
    pub lease_ttl_secs: i64,
    /// How often a running job renews its lease + emits a liveness heartbeat.
    pub heartbeat_secs: u64,
    /// Idle poll cadence when the queue is empty or capacity is saturated.
    pub poll_secs: u64,
    /// Bounded automatic crash-recovery re-queues before a job is quarantined.
    pub max_recovery_attempts: i32,
    /// Initial durable retry delay for typed transient execution failures.
    pub execution_retry_initial_secs: u64,
    /// Maximum durable retry delay after exponential backoff.
    pub execution_retry_max_secs: u64,
    /// Hard cap on the deterministic historical spine (selection × instants):
    /// a dry-run `plan` beyond this flags `hard_cap_exceeded`; a `build` fails closed.
    pub max_spine_samples: u64,
    /// Number of `as_of` slices sampled during `plan` to estimate the PIT
    /// selection keep-rate (`0` disables the estimate).
    pub plan_sample_slices: u32,
    /// Number of candidate markets sampled per slice for the keep-rate estimate
    /// (bounds the point-in-time reads a `plan` issues to `slices × markets`).
    pub plan_sample_markets: u32,
    /// Minimum interval between throttled progress writes (DB heartbeat + WS push).
    pub progress_min_interval_ms: u64,
    /// Elapsed DB-clock age after which a running feedback cycle emits a
    /// deduplicated scheduler-health alert. The cycle remains durable and is
    /// never synthetically terminalized.
    pub feedback_stuck_secs: u64,
    /// Maximum time one feedback scheduler alert may occupy the coordinator.
    pub feedback_alert_timeout_secs: u64,
    /// Durable-condition alert dedupe horizon.
    pub feedback_alert_dedupe_secs: u64,
    /// Bounded grace period a graceful shutdown waits for in-flight jobs to
    /// cooperatively unwind at a stage boundary before their rows are
    /// explicitly re-queued for the next epoch. Keep short: the build restarts
    /// from scratch on re-lease, so draining only lets near-complete work finish.
    pub shutdown_drain_secs: u64,
}

impl Default for ResearchJobsConfig {
    fn default() -> Self {
        Self {
            global_concurrency: 2,
            dataset_build_concurrency: 1,
            model_train_concurrency: 1,
            backtest_concurrency: 2,
            bias_table_fit_concurrency: 1,
            model_calibration_fit_concurrency: 1,
            cpcv_backtest_concurrency: 1,
            feature_parity_concurrency: 1,
            feature_parity_compute: FeatureParityComputeConfig::default(),
            feedback_coverage_concurrency: 1,
            feedback_drift_concurrency: 1,
            feedback_learning_concurrency: 1,
            feedback_attribution_compute: FeedbackAttributionComputeConfig::default(),
            feedback_cycle_concurrency: 2,
            trade_policy_fit_concurrency: 1,
            trade_policy_validation_concurrency: 1,
            lease_ttl_secs: 90,
            heartbeat_secs: 30,
            poll_secs: 3,
            max_recovery_attempts: 3,
            execution_retry_initial_secs: 2,
            execution_retry_max_secs: 60,
            max_spine_samples: 2_000_000,
            plan_sample_slices: 5,
            plan_sample_markets: 200,
            progress_min_interval_ms: 500,
            feedback_stuck_secs: 21_600,
            feedback_alert_timeout_secs: 2,
            feedback_alert_dedupe_secs: 900,
            shutdown_drain_secs: 3,
        }
    }
}

/// Mandatory deploy-time compute envelope for durable feature-parity replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeatureParityComputeConfig {
    /// Maximum frozen serving subjects replayed in one bounded page.
    pub page_size: u32,
    /// Maximum replay kernels admitted concurrently by the source.
    pub max_concurrency: usize,
    /// Logical peak working set reserved for each replay kernel.
    pub max_working_set_bytes: u64,
    /// End-to-end wall-clock deadline for one replay attempt.
    pub deadline_secs: u64,
}

impl Default for FeatureParityComputeConfig {
    fn default() -> Self {
        Self {
            page_size: 100,
            max_concurrency: 1,
            max_working_set_bytes: 4 * 1024 * 1024 * 1024,
            deadline_secs: 1_800,
        }
    }
}

impl FeatureParityComputeConfig {
    const fn missing() -> Self {
        Self {
            page_size: 0,
            max_concurrency: 0,
            max_working_set_bytes: 0,
            deadline_secs: 0,
        }
    }
}

/// Mandatory deploy-time compute envelope for attribution materialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackAttributionComputeConfig {
    /// Maximum model-learning cohort rows loaded into one materialization page.
    pub page_size: u32,
    /// Maximum asynchronous attribution groups in flight. CPU kernels remain
    /// exclusively governed by the process-wide compute executor.
    pub max_concurrency: usize,
    /// Logical peak working set reserved for each admitted kernel.
    pub max_working_set_bytes: u64,
    /// End-to-end wall-clock deadline for one attribution-manifest job.
    pub deadline_secs: u64,
}

impl Default for FeedbackAttributionComputeConfig {
    fn default() -> Self {
        Self {
            page_size: 250,
            max_concurrency: 4,
            max_working_set_bytes: 512 * 1024 * 1024,
            deadline_secs: 1_800,
        }
    }
}

impl FeedbackAttributionComputeConfig {
    const fn missing() -> Self {
        Self {
            page_size: 0,
            max_concurrency: 0,
            max_working_set_bytes: 0,
            deadline_secs: 0,
        }
    }
}

impl ResearchJobsConfig {
    /// The concurrency cap for one job kind.
    #[must_use]
    pub const fn kind_concurrency(&self, kind: ResearchJobKind) -> usize {
        match kind {
            ResearchJobKind::DatasetBuild => self.dataset_build_concurrency,
            ResearchJobKind::ModelTrain => self.model_train_concurrency,
            ResearchJobKind::Backtest => self.backtest_concurrency,
            ResearchJobKind::BiasTableFit => self.bias_table_fit_concurrency,
            ResearchJobKind::ModelCalibrationFit => self.model_calibration_fit_concurrency,
            ResearchJobKind::CpcvBacktest => self.cpcv_backtest_concurrency,
            ResearchJobKind::FeatureParity => self.feature_parity_concurrency,
            ResearchJobKind::FeedbackTruthFreeze
            | ResearchJobKind::FeedbackCoverage
            | ResearchJobKind::FeedbackAttribution
            | ResearchJobKind::FeedbackDrift
            | ResearchJobKind::FeedbackRecipePlan => self.feedback_coverage_concurrency,
            ResearchJobKind::FeedbackDatasetSeal
            | ResearchJobKind::FeedbackTraining
            | ResearchJobKind::FeedbackCalibration
            | ResearchJobKind::FeedbackCpcv
            | ResearchJobKind::FeedbackValidation
            | ResearchJobKind::FeedbackComparison
            | ResearchJobKind::FeedbackShadowBind
            | ResearchJobKind::FeedbackShadow
            | ResearchJobKind::FeedbackDecision => self.feedback_learning_concurrency,
            ResearchJobKind::TradePolicyFit => self.trade_policy_fit_concurrency,
            ResearchJobKind::TradePolicyValidation => self.trade_policy_validation_concurrency,
        }
    }

    /// Calculate the bounded delay for the next durable execution attempt.
    #[must_use]
    pub fn execution_retry_delay(&self, completed_attempts: i32) -> u64 {
        let shift = if completed_attempts <= 0 {
            0
        } else if completed_attempts >= 63 {
            63
        } else {
            completed_attempts.cast_unsigned()
        };
        self.execution_retry_initial_secs
            .saturating_mul(1_u64 << shift)
            .min(self.execution_retry_max_secs)
    }
}

/// Quant background worker structural parameters.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QuantWorkersConfig {
    /// `PostgreSQL` report schedule reconciliation cadence (seconds).
    pub report_schedule_poll_secs: u64,
    /// Durable report-run lease duration (seconds).
    pub report_run_lease_secs: u64,
    /// Heartbeat cadence for a claimed report run (seconds).
    pub report_run_heartbeat_secs: u64,
    /// Maximum queued ad-hoc report requests across all replicas.
    pub report_ad_hoc_queue_capacity: u64,
    /// Maximum time an ad-hoc request may remain queued (seconds).
    pub report_ad_hoc_queue_ttl_secs: u64,
    /// Report TTL expiry cadence (seconds).
    ///
    /// Serves two roles for report expiry (`expires_at` deadline frozen at
    /// publish): the `ReportDeadlineScheduler` reconcile/look-ahead interval
    /// (precise per-report wakes) **and** the decoupled `ReportExpireSweep`
    /// backstop cadence. The DB is the source of truth; the scheduler is a
    /// latency optimization and the sweep is its durable safety net. Unrelated to
    /// report fire scheduling (owned by the durable `PostgreSQL` coordinator).
    pub report_expire_sweep_secs: u64,
    /// Order-intent TTL expiry cadence (seconds).
    ///
    /// Serves as the `IntentDeadlineScheduler` reconcile/look-ahead interval
    /// (precise per-intent wakes that release reserved capital at `expires_at`)
    /// **and** the `IntentExpireSweep` backstop cadence. Every fire re-checks the
    /// DB and runs `CoreOrderIntentService::expire_due` (atomic status + capital
    /// release), so a missed or duplicated wake only affects latency.
    pub intent_expire_sweep_secs: u64,
    /// Auto-execution dispatcher poll-backstop cadence (seconds).
    ///
    /// The dispatcher is wake-driven: a fresh `ApprovedByPolicy` approval nudges
    /// it for near-immediate submit. This cadence is the **backstop** poll that
    /// catches missed wakes, retries admission defers, and drains crash-recovery
    /// work. The durable queue is Postgres (`ApprovedByPolicy` rows under a
    /// per-intent row lock). The current deployment runs a single dispatcher;
    /// repository locking remains authoritative across processes.
    pub execution_dispatch_secs: u64,
    /// Execution-breaker self-heal tick cadence (seconds).
    ///
    /// Drives `ExecutionBreaker::tick`, which recovers `Degraded -> Healthy`
    /// after the configured cooldown. `Halted` is latched and never auto-heals.
    pub execution_breaker_tick_secs: u64,
    /// Best-effort equity-history snapshot cadence (seconds).
    ///
    /// Report generation computes and persists its own authoritative equity
    /// snapshot synchronously. This worker only adds heartbeat/history points
    /// between reports.
    pub equity_snapshot_secs: u64,
}

impl Default for QuantWorkersConfig {
    fn default() -> Self {
        Self {
            report_schedule_poll_secs: 1,
            report_run_lease_secs: 120,
            report_run_heartbeat_secs: 30,
            report_ad_hoc_queue_capacity: 64,
            report_ad_hoc_queue_ttl_secs: 300,
            report_expire_sweep_secs: default_report_sweep_secs(),
            intent_expire_sweep_secs: default_intent_sweep_secs(),
            execution_dispatch_secs: default_execution_dispatch_secs(),
            execution_breaker_tick_secs: default_breaker_tick_secs(),
            equity_snapshot_secs: default_equity_snapshot_secs(),
        }
    }
}

/// Venue account read configuration (`[quant.account]`).
///
/// `report_only` is **not** dry-run: report sizing is built on the real venue
/// account. The funder address is required for keyless Data API position reads;
/// reports fail closed without it. auto-redeem is EOA-only, so
/// money-moving settlement redemption additionally requires this address to
/// equal the signer EOA. The private key is configured under `[keys]`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QuantAccountDeployConfig {
    /// Polymarket proxy/funder address used as `user=<funder>` for Data API
    /// position reads. Required to generate reports (all modes).
    pub funder: Option<String>,
    /// Wallet shape for money-moving on-chain operations.
    pub wallet_kind: ExecutionWalletKind,
}

const fn default_report_sweep_secs() -> u64 {
    300
}

const fn default_intent_sweep_secs() -> u64 {
    60
}

const fn default_execution_dispatch_secs() -> u64 {
    10
}

const fn default_breaker_tick_secs() -> u64 {
    5
}

const fn default_equity_snapshot_secs() -> u64 {
    300
}

#[cfg(test)]
mod tests {
    use super::ResearchJobsConfig;

    #[test]
    fn missing_compute_budgets_invalid() {
        let config: ResearchJobsConfig =
            toml::from_str("").expect("deserialize omitted attribution budget");
        assert_eq!(config.feature_parity_compute.page_size, 0);
        assert_eq!(config.feature_parity_compute.max_concurrency, 0);
        assert_eq!(config.feature_parity_compute.max_working_set_bytes, 0);
        assert_eq!(config.feature_parity_compute.deadline_secs, 0);
        assert_eq!(config.feedback_attribution_compute.page_size, 0);
        assert_eq!(config.feedback_attribution_compute.max_concurrency, 0);
        assert_eq!(config.feedback_attribution_compute.max_working_set_bytes, 0);
        assert_eq!(config.feedback_attribution_compute.deadline_secs, 0);
    }

    #[test]
    fn execution_retry_backoff_caps() {
        let config = ResearchJobsConfig {
            execution_retry_initial_secs: 2,
            execution_retry_max_secs: 10,
            ..ResearchJobsConfig::default()
        };
        assert_eq!(config.execution_retry_delay(0), 2);
        assert_eq!(config.execution_retry_delay(1), 4);
        assert_eq!(config.execution_retry_delay(2), 8);
        assert_eq!(config.execution_retry_delay(3), 10);
        assert_eq!(config.execution_retry_delay(i32::MAX), 10);
    }
}
