//! Quant pivot deploy configuration (`[quant]`, restart to apply).

use crate::enums::quant::{ExecutionWalletKind, ResearchJobKind};
use serde::Deserialize;

/// Quant-specific structural parameters for Phase 0+.
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
    /// Lease time-to-live: a lease not renewed within this window is reclaimable.
    pub lease_ttl_secs: i64,
    /// How often a running job renews its lease + emits a liveness heartbeat.
    pub heartbeat_secs: u64,
    /// Idle poll cadence when the queue is empty or capacity is saturated.
    pub poll_secs: u64,
    /// Bounded automatic crash-recovery re-queues before a job is quarantined.
    pub max_recovery_attempts: i32,
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
    /// Bounded grace period a graceful shutdown waits for in-flight jobs to
    /// cooperatively unwind (at a section/phase boundary) before their rows are
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
            lease_ttl_secs: 90,
            heartbeat_secs: 30,
            poll_secs: 3,
            max_recovery_attempts: 3,
            max_spine_samples: 2_000_000,
            plan_sample_slices: 5,
            plan_sample_markets: 200,
            progress_min_interval_ms: 500,
            shutdown_drain_secs: 5,
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
        }
    }
}

/// Quant background worker structural parameters.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QuantWorkersConfig {
    /// Report TTL expiry cadence (seconds).
    ///
    /// Serves two roles for report expiry (`expires_at` deadline frozen at
    /// publish): the `ReportDeadlineScheduler` reconcile/look-ahead interval
    /// (precise per-report wakes) **and** the decoupled `ReportExpireSweep`
    /// backstop cadence. The DB is the source of truth; the scheduler is a
    /// latency optimization and the sweep is its durable safety net. Unrelated to
    /// report fire scheduling (owned by `tokio-cron-scheduler`).
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
    /// per-intent row lock); single instance (multi-replica is Phase 8+).
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
            report_expire_sweep_secs: default_report_expire_sweep_secs(),
            intent_expire_sweep_secs: default_intent_expire_sweep_secs(),
            execution_dispatch_secs: default_execution_dispatch_secs(),
            execution_breaker_tick_secs: default_execution_breaker_tick_secs(),
            equity_snapshot_secs: default_equity_snapshot_secs(),
        }
    }
}

/// Venue account read configuration (`[quant.account]`).
///
/// `report_only` is **not** dry-run: report sizing is built on the real venue
/// account. The funder address is required for keyless Data API position reads;
/// reports fail closed without it. Phase05.10 auto-redeem is EOA-only, so
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

const fn default_report_expire_sweep_secs() -> u64 {
    300
}

const fn default_intent_expire_sweep_secs() -> u64 {
    60
}

const fn default_execution_dispatch_secs() -> u64 {
    10
}

const fn default_execution_breaker_tick_secs() -> u64 {
    5
}

const fn default_equity_snapshot_secs() -> u64 {
    300
}
