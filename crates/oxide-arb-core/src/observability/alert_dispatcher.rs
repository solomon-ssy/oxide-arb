//! Operator alert dispatch (Telegram + webhook) with per-title cooldown.
//!
//! Channels and the cooldown are built from the runtime `notification`
//! section and are fully hot-reloadable through [`AlertDispatcher::reload`]
//! (lock-free `ArcSwap` snapshot), so an operator can rotate a bot token or
//! webhook URL without a restart.

use arc_swap::ArcSwap;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use oxide_arb_models::{enums::common::AlertLevel, runtime_config::NotificationConfig};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use teloxide::{prelude::*, types::ChatId};

#[derive(Clone)]
pub struct Alert {
    pub severity: AlertLevel,
    pub title: String,
    pub body: String,
    pub timestamp: DateTime<Utc>,
}

struct TelegramChannel {
    bot: Bot,
    chat_id: ChatId,
}

struct WebhookChannel {
    client: reqwest::Client,
    url: String,
}

/// Hot-swappable channel set + cooldown derived from [`NotificationConfig`].
struct Channels {
    telegram: Option<TelegramChannel>,
    webhook: Option<WebhookChannel>,
    cooldown_duration: Duration,
}

impl Channels {
    fn from_config(config: &NotificationConfig) -> Self {
        let token = config.telegram.bot_token.trim();
        let chat = config.telegram.chat_id.trim();
        let telegram = if config.telegram.enabled && !token.is_empty() {
            chat.parse().ok().map(|chat_id: i64| TelegramChannel {
                bot: Bot::new(token),
                chat_id: ChatId(chat_id),
            })
        } else {
            None
        };

        let url = config.webhook.url.trim();
        let webhook = if config.webhook.enabled && !url.is_empty() {
            Some(WebhookChannel {
                client: reqwest::Client::new(),
                url: url.to_owned(),
            })
        } else {
            None
        };

        Self {
            telegram,
            webhook,
            cooldown_duration: Duration::from_secs(config.alert_cooldown_secs),
        }
    }
}

pub struct AlertDispatcher {
    channels: ArcSwap<Channels>,
    cooldown: DashMap<String, Instant>,
    recordings: Option<Arc<Mutex<Vec<Alert>>>>,
}

impl AlertDispatcher {
    /// Build the dispatcher from the runtime notification config.
    #[must_use]
    pub fn new(config: &NotificationConfig) -> Self {
        Self {
            channels: ArcSwap::from_pointee(Channels::from_config(config)),
            cooldown: DashMap::new(),
            recordings: None,
        }
    }

    /// Hot-reload channels + cooldown (runtime-config activation). Rebuilds
    /// the Telegram bot / webhook client; in-flight cooldown stamps persist.
    pub fn reload(&self, config: &NotificationConfig) {
        self.channels.store(Arc::new(Channels::from_config(config)));
    }

    #[must_use]
    pub fn with_recordings(recordings: Arc<Mutex<Vec<Alert>>>) -> Self {
        Self::with_recordings_and_cooldown(recordings, Duration::ZERO)
    }

    #[must_use]
    pub fn with_recordings_and_cooldown(
        recordings: Arc<Mutex<Vec<Alert>>>,
        cooldown_duration: Duration,
    ) -> Self {
        Self {
            channels: ArcSwap::from_pointee(Channels {
                telegram: None,
                webhook: None,
                cooldown_duration,
            }),
            cooldown: DashMap::new(),
            recordings: Some(recordings),
        }
    }

    pub async fn dispatch(&self, alert: Alert) {
        let channels = self.channels.load_full();
        let cooldown_key = format!("{}:{}", alert.severity, alert.title);
        if let Some(last) = self.cooldown.get(&cooldown_key) {
            if last.elapsed() < channels.cooldown_duration {
                tracing::debug!(title = %alert.title, "alert suppressed by cooldown");
                return;
            }
        }
        self.cooldown.insert(cooldown_key, Instant::now());

        if let Some(recordings) = &self.recordings {
            if let Ok(mut guard) = recordings.lock() {
                guard.push(alert.clone());
            }
        }

        let text = format!(
            "[{}] {}\n{}\n{}",
            alert.severity, alert.title, alert.body, alert.timestamp
        );

        match alert.severity {
            AlertLevel::Emergency | AlertLevel::Critical => {
                send_telegram(&channels, &text).await;
                send_webhook(&channels, &alert).await;
            }
            AlertLevel::Warning => {
                send_webhook(&channels, &alert).await;
            }
            AlertLevel::Info => {
                tracing::info!(severity = %alert.severity, title = %alert.title, "{}", alert.body);
            }
        }
    }

    pub fn dispatch_background(self: &Arc<Self>, alert: Alert) {
        let dispatcher = Arc::clone(self);
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    dispatcher.dispatch(alert).await;
                });
            }
            Err(error) => {
                tracing::error!(%error, title = %alert.title, "cannot dispatch alert outside Tokio runtime");
            }
        }
    }
}

async fn send_telegram(channels: &Channels, text: &str) {
    let Some(tg) = &channels.telegram else { return };
    if let Err(e) = tg.bot.send_message(tg.chat_id, text).await {
        tracing::error!(error = %e, "failed to send telegram alert");
    }
}

async fn send_webhook(channels: &Channels, alert: &Alert) {
    let Some(wh) = &channels.webhook else { return };
    let payload = serde_json::json!({
        "severity": format!("{}", alert.severity),
        "title": alert.title,
        "body": alert.body,
        "timestamp": alert.timestamp.to_rfc3339(),
    });
    let result = wh
        .client
        .post(&wh.url)
        .json(&payload)
        .timeout(Duration::from_secs(5))
        .send()
        .await;
    if let Err(e) = result {
        tracing::error!(error = %e, "failed to send webhook alert");
    }
}

#[cfg(test)]
mod tests {
    use super::{Alert, AlertDispatcher};
    use chrono::Utc;
    use oxide_arb_models::{enums::common::AlertLevel, runtime_config::NotificationConfig};
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    fn alert(title: &str) -> Alert {
        Alert {
            severity: AlertLevel::Critical,
            title: title.to_owned(),
            body: "body".to_owned(),
            timestamp: Utc::now(),
        }
    }

    #[tokio::test]
    async fn cooldown_suppresses_duplicate_alert_recordings() {
        let recordings = Arc::new(Mutex::new(Vec::new()));
        let dispatcher = AlertDispatcher::with_recordings_and_cooldown(
            Arc::clone(&recordings),
            Duration::from_secs(60),
        );

        dispatcher.dispatch(alert("same")).await;
        dispatcher.dispatch(alert("same")).await;
        dispatcher.dispatch(alert("different")).await;

        let recorded_titles = {
            let guard = recordings.lock().expect("recordings lock");
            assert_eq!(guard.len(), 2);
            guard
                .iter()
                .map(|alert| alert.title.clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(recorded_titles, ["same", "different"]);
    }

    /// `reload` must change suppression behaviour immediately: a runtime
    /// config activation that lengthens `alert_cooldown_secs` suppresses
    /// repeats that the previous (zero) cooldown allowed, while in-flight
    /// cooldown stamps persist across the reload.
    #[tokio::test]
    async fn reload_applies_new_cooldown_from_config() {
        let recordings = Arc::new(Mutex::new(Vec::new()));
        let dispatcher = AlertDispatcher::with_recordings(Arc::clone(&recordings));

        dispatcher.dispatch(alert("same")).await;
        dispatcher.dispatch(alert("same")).await;
        assert_eq!(recordings.lock().expect("lock").len(), 2);

        let config = NotificationConfig {
            alert_cooldown_secs: 3600,
            ..NotificationConfig::default()
        };
        dispatcher.reload(&config);

        dispatcher.dispatch(alert("same")).await;
        dispatcher.dispatch(alert("other")).await;

        let recorded_titles = {
            let guard = recordings.lock().expect("recordings lock");
            guard
                .iter()
                .map(|alert| alert.title.clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            recorded_titles,
            ["same", "same", "other"],
            "post-reload duplicate must be suppressed by the new cooldown"
        );
    }
}
