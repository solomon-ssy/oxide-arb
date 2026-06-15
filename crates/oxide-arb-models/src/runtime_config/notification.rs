//! Notification channel runtime configuration (`notification` section).
//!
//! Fully hot-reloadable, including credentials, so an operator can rotate a
//! Telegram bot token or webhook URL without a restart. Credentials are stored
//! in the versioned runtime-config JSON: the read API masks `bot_token` and
//! `webhook.url`, and activation requires a governed role plus an audited
//! reason.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Operator alert channels and the global alert cooldown.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct NotificationConfig {
    /// Minimum interval (seconds) between alerts with the same severity+title
    /// (anti-flood; applies to all channels). Default: `60`.
    #[schemars(extend("x-format" = "integer"))]
    pub alert_cooldown_secs: u64,
    /// Telegram channel (Emergency/Critical alerts).
    pub telegram: TelegramConfig,
    /// Webhook channel (Warning and above).
    pub webhook: WebhookConfig,
}

impl Default for NotificationConfig {
    fn default() -> Self {
        Self {
            alert_cooldown_secs: default_alert_cooldown_secs(),
            telegram: TelegramConfig::default(),
            webhook: WebhookConfig::default(),
        }
    }
}

const fn default_alert_cooldown_secs() -> u64 {
    60
}

/// Telegram alert channel.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct TelegramConfig {
    /// Whether Telegram alerts are dispatched. Live-mode validation requires a
    /// non-empty `bot_token` and `chat_id` when enabled. Default: `false`.
    pub enabled: bool,
    /// Bot token (sensitive — masked in read APIs). Default: empty.
    #[schemars(extend("x-sensitive" = true))]
    pub bot_token: String,
    /// Destination chat ID (numeric, as a string). Default: empty.
    pub chat_id: String,
}

/// Webhook alert channel.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct WebhookConfig {
    /// Whether webhook alerts are dispatched. Live-mode validation requires a
    /// non-empty `url` when enabled. Default: `false`.
    pub enabled: bool,
    /// POST target URL (sensitive — masked in read APIs). Default: empty.
    #[schemars(extend("x-sensitive" = true))]
    pub url: String,
}
