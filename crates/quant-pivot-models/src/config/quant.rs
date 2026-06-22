//! Quant pivot deploy configuration (`[quant]`, restart to apply).

use serde::Deserialize;

/// Quant-specific structural parameters for Phase 0+.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QuantDeployConfig {
    /// Background worker topology.
    pub workers: QuantWorkersConfig,
    /// Execution-adjacent deploy flags (not runtime mode).
    pub execution: QuantExecutionDeployConfig,
}

/// Quant background worker structural parameters.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QuantWorkersConfig {
    /// Report scheduler tick interval (seconds). Phase 4 consumes this knob.
    pub report_scheduler_tick_secs: u64,
}

impl Default for QuantWorkersConfig {
    fn default() -> Self {
        Self {
            report_scheduler_tick_secs: default_report_scheduler_tick_secs(),
        }
    }
}

/// Quant execution deploy flags (distinct from [`QuantRuntimeMode`]).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QuantExecutionDeployConfig {
    /// When false, private keys are not loaded in `ReportOnly` mode.
    pub load_credentials_in_report_only: bool,
}

impl Default for QuantExecutionDeployConfig {
    fn default() -> Self {
        Self {
            load_credentials_in_report_only: default_load_credentials_in_report_only(),
        }
    }
}

const fn default_report_scheduler_tick_secs() -> u64 {
    30
}

const fn default_load_credentials_in_report_only() -> bool {
    false
}
