//! Quant pivot deploy configuration (`[quant]`, restart to apply).

use serde::Deserialize;

/// Quant-specific structural parameters for Phase 0+.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QuantDeployConfig {
    /// Background worker topology.
    pub workers: QuantWorkersConfig,
    /// Venue account read configuration.
    pub account: QuantAccountDeployConfig,
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
/// account. The funder (Polymarket proxy address, distinct from the signer EOA)
/// is required for keyless Data API position reads; reports fail closed without
/// it. The private key (read credential) is configured under `[keys]`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QuantAccountDeployConfig {
    /// Polymarket proxy/funder address used as `user=<funder>` for Data API
    /// position reads. Required to generate reports (all modes).
    pub funder: Option<String>,
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
