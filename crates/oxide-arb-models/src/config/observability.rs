//! Observability (logging, metrics, alerts) configuration.

use serde::Deserialize;
use validator::Validate;

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct ObservabilityConfig {
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default)]
    pub log_json: bool,
    #[serde(default)]
    pub metrics: MetricsConfig,
    #[serde(default)]
    pub alerts: AlertsConfig,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            log_level: default_log_level(),
            log_json: false,
            metrics: MetricsConfig::default(),
            alerts: AlertsConfig::default(),
        }
    }
}

fn default_log_level() -> String {
    "info".into()
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct MetricsConfig {
    #[serde(default = "default_metrics_enabled")]
    pub enabled: bool,
    #[serde(default = "default_metrics_port")]
    pub port: u16,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: default_metrics_enabled(),
            port: default_metrics_port(),
        }
    }
}

const fn default_metrics_enabled() -> bool {
    true
}
const fn default_metrics_port() -> u16 {
    9090
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct AlertsConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_alert_cooldown")]
    pub cooldown_secs: u64,
}

impl Default for AlertsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cooldown_secs: default_alert_cooldown(),
        }
    }
}

const fn default_alert_cooldown() -> u64 {
    300
}
