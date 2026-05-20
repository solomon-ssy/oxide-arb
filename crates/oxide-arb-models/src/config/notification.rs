//! Notification channel configuration.

use serde::Deserialize;
use validator::Validate;

#[derive(Debug, Clone, Default, Deserialize, Validate)]
pub struct NotificationConfig {
    #[serde(default)]
    pub telegram: TelegramConfig,
    #[serde(default)]
    pub webhook: WebhookConfig,
}

#[derive(Debug, Clone, Default, Deserialize, Validate)]
pub struct TelegramConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub bot_token: String,
    #[serde(default)]
    pub chat_id: String,
}

#[derive(Debug, Clone, Default, Deserialize, Validate)]
pub struct WebhookConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub url: String,
}
