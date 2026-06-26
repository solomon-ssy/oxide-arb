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
    /// Report TTL expire-sweep cadence (seconds).
    ///
    /// Drives only the decoupled `ReportLifecycleService::expire_due_reports`
    /// sweep — **not** report fire scheduling, which is owned by
    /// `tokio-cron-scheduler` via `ReportScheduleRunner` (cadence comes from
    /// `runtime_config.reports.schedules[]`).
    pub report_expire_sweep_secs: u64,
}

impl Default for QuantWorkersConfig {
    fn default() -> Self {
        Self {
            report_expire_sweep_secs: default_report_expire_sweep_secs(),
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
