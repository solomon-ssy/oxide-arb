//! Logging configuration (`[observability]`, deploy).
//!
//! Logging only: Prometheus metrics are always-on and scraped through the web
//! server's `GET /metrics` (no separate port), and operator alerting is owned
//! by the runtime `notification` section.

use serde::Deserialize;

/// Logging level and output format.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ObservabilityConfig {
    /// Default `tracing` filter directive (e.g. `info`,
    /// `info,quant_pivot_core=debug`). The `RUST_LOG` environment variable, when
    /// set, overrides this value entirely. Default: `info`.
    pub log_level: String,
    /// Emit JSON-structured log lines instead of human-readable text. Enable
    /// in production for log aggregation. Default: `false`.
    pub log_json: bool,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            log_level: default_log_level(),
            log_json: false,
        }
    }
}

fn default_log_level() -> String {
    "info".into()
}
