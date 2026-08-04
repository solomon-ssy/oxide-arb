//! Operator alert dispatch (Telegram + webhook) with per-title cooldown.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use arc_swap::{ArcSwap, ArcSwapOption};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use quant_pivot_error::{QuantResult, config::ConfigError};
use quant_pivot_models::{
    config::{NotificationChannelsConfig, secret::SecretText},
    domain::runtime::{CoreEvent, CoreEventPublisher, SystemAlertEvent},
    enums::common::{AlertCategory, AlertLevel, AlertSource},
};
use reqwest::{
    Client, Url,
    header::{AUTHORIZATION, HeaderValue},
};
use teloxide_core::{Bot, requests::Requester, types::ChatId};
use tokio::runtime::Handle;

#[derive(Clone)]
pub struct Alert {
    pub idempotency_key: String,
    pub severity: AlertLevel,
    pub category: AlertCategory,
    pub source: AlertSource,
    pub title: String,
    pub body: String,
    pub affects_trading: bool,
    pub visible_toast: bool,
    pub dedupe_secs: u64,
    pub timestamp: DateTime<Utc>,
}

impl Alert {
    #[must_use]
    pub fn new(
        idempotency_key: impl Into<String>,
        severity: AlertLevel,
        category: AlertCategory,
        source: AlertSource,
        title: impl Into<String>,
        body: impl Into<String>,
        timestamp: DateTime<Utc>,
    ) -> Self {
        Self {
            idempotency_key: idempotency_key.into(),
            severity,
            category,
            source,
            title: title.into(),
            body: body.into(),
            affects_trading: category.default_affects_trading(),
            visible_toast: category.default_visible_toast(severity),
            dedupe_secs: 0,
            timestamp,
        }
    }

    #[must_use]
    pub const fn with_dedupe_secs(mut self, secs: u64) -> Self {
        self.dedupe_secs = secs;
        self
    }

    #[must_use]
    pub const fn with_affects_trading(mut self, affects_trading: bool) -> Self {
        self.affects_trading = affects_trading;
        self
    }

    #[must_use]
    pub const fn with_visible_toast(mut self, visible_toast: bool) -> Self {
        self.visible_toast = visible_toast;
        self
    }

    #[must_use]
    pub fn event(&self, fallback_dedupe_secs: u64) -> SystemAlertEvent {
        SystemAlertEvent {
            idempotency_key: self.idempotency_key.clone(),
            level: self.severity,
            category: self.category,
            source: self.source,
            title: self.title.clone(),
            message: self.body.clone(),
            affects_trading: self.affects_trading,
            visible_toast: self.visible_toast,
            dedupe_secs: self.dedupe_secs.max(fallback_dedupe_secs),
        }
    }
}

struct TelegramChannel {
    bot: Bot,
    chat_id: ChatId,
}

struct WebhookChannel {
    client: Client,
    url: Url,
    authorization: Option<HeaderValue>,
}

/// Process-bound channel set resolved from deploy configuration.
struct Channels {
    telegram: Option<TelegramChannel>,
    webhook: Option<WebhookChannel>,
    cooldown_duration: Duration,
}

impl Channels {
    fn from_config(config: &NotificationChannelsConfig) -> QuantResult<Self> {
        let token = (!config.telegram.bot_token.is_empty()).then_some(&config.telegram.bot_token);
        let chat = config.telegram.chat_id.trim();
        let telegram = token
            .map(|token| {
                let chat_id = chat
                    .parse::<i64>()
                    .map_err(|error| ConfigError::InvalidValue {
                        field: "notifications.telegram.chat_id".to_owned(),
                        reason: error.to_string(),
                    })?;
                Ok::<_, ConfigError>(TelegramChannel {
                    bot: Bot::new(token.expose_secret()),
                    chat_id: ChatId(chat_id),
                })
            })
            .transpose()?;

        let url = config.webhook.url.expose_secret().trim();
        let authorization =
            (!config.webhook.authorization.is_empty()).then_some(&config.webhook.authorization);
        if url.is_empty() && authorization.is_some() {
            return Err(ConfigError::MissingField {
                section: "notifications.webhook".to_owned(),
                field: "url".to_owned(),
            }
            .into());
        }
        let webhook = if url.is_empty() {
            None
        } else {
            let parsed = Url::parse(url).map_err(|error| ConfigError::InvalidValue {
                field: "notifications.webhook.url".to_owned(),
                reason: error.to_string(),
            })?;
            if parsed.scheme() != "https" {
                return Err(ConfigError::InvalidValue {
                    field: "notifications.webhook.url".to_owned(),
                    reason: "operator webhooks require HTTPS".to_owned(),
                }
                .into());
            }
            Some(WebhookChannel {
                client: Client::new(),
                url: parsed,
                authorization: authorization.map(secret_header).transpose()?,
            })
        };

        Ok(Self {
            telegram,
            webhook,
            cooldown_duration: Duration::from_mins(1),
        })
    }
}

fn secret_header(secret: &SecretText) -> QuantResult<HeaderValue> {
    let mut value = HeaderValue::from_str(secret.expose_secret()).map_err(|error| {
        ConfigError::InvalidValue {
            field: "notifications.webhook.authorization".to_owned(),
            reason: error.to_string(),
        }
    })?;
    value.set_sensitive(true);
    Ok(value)
}

pub struct AlertDispatcher {
    channels: ArcSwap<Channels>,
    events: ArcSwapOption<CoreEventPublisher>,
    cooldown: DashMap<String, Instant>,
    recordings: Option<Arc<Mutex<Vec<Alert>>>>,
}

impl AlertDispatcher {
    /// Resolve process-bound notification channels from deploy configuration.
    pub fn new(config: &NotificationChannelsConfig) -> QuantResult<Self> {
        Ok(Self {
            channels: ArcSwap::from_pointee(Channels::from_config(config)?),
            events: ArcSwapOption::empty(),
            cooldown: DashMap::new(),
            recordings: None,
        })
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
            events: ArcSwapOption::empty(),
            cooldown: DashMap::new(),
            recordings: Some(recordings),
        }
    }

    /// Attach the real-time event bus so the dispatcher is the single alert
    /// router for external channels and WebSocket clients.
    pub fn attach_event_publisher(&self, events: CoreEventPublisher) {
        self.events.store(Some(Arc::new(events)));
    }

    pub async fn dispatch(&self, alert: Alert) {
        let channels = self.channels.load_full();
        let event = alert.event(channels.cooldown_duration.as_secs());
        let cooldown_key = event.idempotency_key.clone();
        let cooldown_duration = Duration::from_secs(event.dedupe_secs);
        if let Some(last) = self.cooldown.get(&cooldown_key)
            && last.elapsed() < cooldown_duration
        {
            tracing::debug!(alert_key = %event.idempotency_key, title = %event.title, "alert suppressed by cooldown");
            return;
        }
        self.cooldown.insert(cooldown_key, Instant::now());

        if let Some(events) = self.events.load_full() {
            events.publish(CoreEvent::Alert(event.clone()));
        }

        if let Some(recordings) = &self.recordings
            && let Ok(mut guard) = recordings.lock()
        {
            guard.push(alert.clone());
        }

        let text = format!(
            "[{}] {}\n{}\n{}",
            event.level, event.title, event.message, alert.timestamp
        );

        match event.level {
            AlertLevel::Emergency | AlertLevel::Critical => {
                send_telegram(&channels, &text).await;
                send_webhook(&channels, &alert).await;
            }
            AlertLevel::Warning => {
                send_webhook(&channels, &alert).await;
            }
            AlertLevel::Info => {
                tracing::info!(severity = %event.level, title = %event.title, "{}", event.message);
            }
        }
    }

    /// Dispatch an operator notification to all configured external channels,
    /// regardless of alert severity. This is for business lifecycle notices
    /// such as report publication, not trading-safety escalation.
    pub async fn dispatch_operator_notification(&self, alert: Alert) {
        let channels = self.channels.load_full();
        let event = alert.event(channels.cooldown_duration.as_secs());
        let cooldown_key = event.idempotency_key.clone();
        let cooldown_duration = Duration::from_secs(event.dedupe_secs);
        if let Some(last) = self.cooldown.get(&cooldown_key)
            && last.elapsed() < cooldown_duration
        {
            tracing::debug!(alert_key = %event.idempotency_key, title = %event.title, "operator notification suppressed by cooldown");
            return;
        }
        self.cooldown.insert(cooldown_key, Instant::now());

        if let Some(events) = self.events.load_full() {
            events.publish(CoreEvent::Alert(event.clone()));
        }

        if let Some(recordings) = &self.recordings
            && let Ok(mut guard) = recordings.lock()
        {
            guard.push(alert.clone());
        }

        let text = format!(
            "[{}] {}\n{}\n{}",
            event.level, event.title, event.message, alert.timestamp
        );
        send_telegram(&channels, &text).await;
        send_webhook(&channels, &alert).await;
    }

    pub fn dispatch_background(self: &Arc<Self>, alert: Alert) {
        let dispatcher = Arc::clone(self);
        match Handle::try_current() {
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
        "idempotency_key": &alert.idempotency_key,
        "severity": format!("{}", alert.severity),
        "category": alert.category,
        "source": alert.source,
        "title": &alert.title,
        "body": &alert.body,
        "affects_trading": alert.affects_trading,
        "timestamp": alert.timestamp.to_rfc3339(),
    });
    let mut request = wh.client.post(wh.url.clone());
    if let Some(authorization) = &wh.authorization {
        request = request.header(AUTHORIZATION, authorization.clone());
    }
    let result = request
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
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    use chrono::Utc;
    use quant_pivot_models::enums::common::{AlertCategory, AlertLevel, AlertSource};

    use super::{Alert, AlertDispatcher};

    fn alert(title: &str) -> Alert {
        Alert::new(
            format!("test.{title}"),
            AlertLevel::Critical,
            AlertCategory::Infrastructure,
            AlertSource::System,
            title,
            "body",
            Utc::now(),
        )
    }

    #[tokio::test]
    async fn cooldown_suppresses_duplicate_recordings() {
        let recordings = Arc::new(Mutex::new(Vec::new()));
        let dispatcher = AlertDispatcher::with_recordings_and_cooldown(
            Arc::clone(&recordings),
            Duration::from_mins(1),
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
}
