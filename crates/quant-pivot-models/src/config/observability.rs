//! Logging and operator-channel configuration (deploy, restart to apply).
//!
//! Prometheus metrics are always-on and scraped through the web server's
//! `GET /metrics` (no separate port). Channel bindings live here while
//! hot-reloadable event routing lives in `OperationsPolicy.notifications`.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::secret::SecretText;

/// Logging level and output format.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
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

/// External operator notification channels. Bindings and credential names are
/// deployment concerns and therefore require a restart to apply.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NotificationChannelsConfig {
    pub telegram: TelegramChannelConfig,
    pub webhook: WebhookChannelConfig,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TelegramChannelConfig {
    /// Zeroizing Telegram bot token used only by the operator-notification adapter.
    #[serde(serialize_with = "super::secret::serialize_empty")]
    pub bot_token: SecretText,
    /// Telegram chat identifier; this is a destination binding, not a secret.
    pub chat_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WebhookChannelConfig {
    /// HTTPS endpoint. Webhook paths commonly carry bearer material, so the
    /// complete endpoint is treated as a deploy secret even when a separate
    /// authorization header is configured.
    #[serde(serialize_with = "super::secret::serialize_empty")]
    pub url: SecretText,
    /// Optional complete HTTP Authorization value.
    #[serde(serialize_with = "super::secret::serialize_empty")]
    pub authorization: SecretText,
}
