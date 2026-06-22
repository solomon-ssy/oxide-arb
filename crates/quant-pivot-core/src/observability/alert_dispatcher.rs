//! Operator alert dispatch (Telegram + webhook) with per-title cooldown.

use arc_swap::{ArcSwap, ArcSwapOption};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use quant_pivot_models::{
    domain::{CoreEvent, CoreEventPublisher, SystemAlertEvent},
    enums::common::{AlertCategory, AlertLevel, AlertSource},
    runtime_config::NotificationConfig,
};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use teloxide::{prelude::*, types::ChatId};

/// Legacy scheduler alert payload retained for notification wiring tests.
#[derive(Debug, Clone)]
pub struct ScheduleAlert {
    pub schedule_id: String,
    pub detail: String,
}

impl ScheduleAlert {
    #[must_use]
    pub fn operator_message(&self) -> (String, String) {
        (
            format!("Scheduler alert: {}", self.schedule_id),
            self.detail.clone(),
        )
    }

    #[must_use]
    pub fn idempotency_suffix(&self) -> &str {
        self.schedule_id.as_str()
    }
}

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
    events: ArcSwapOption<CoreEventPublisher>,
    cooldown: DashMap<String, Instant>,
    recordings: Option<Arc<Mutex<Vec<Alert>>>>,
}

impl AlertDispatcher {
    /// Build the dispatcher from the runtime notification config.
    #[must_use]
    pub fn new(config: &NotificationConfig) -> Self {
        Self {
            channels: ArcSwap::from_pointee(Channels::from_config(config)),
            events: ArcSwapOption::empty(),
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
        if let Some(last) = self.cooldown.get(&cooldown_key) {
            if last.elapsed() < cooldown_duration {
                tracing::debug!(alert_key = %event.idempotency_key, title = %event.title, "alert suppressed by cooldown");
                return;
            }
        }
        self.cooldown.insert(cooldown_key, Instant::now());

        if let Some(events) = self.events.load_full() {
            events.publish(CoreEvent::Alert(event.clone()));
        }

        if let Some(recordings) = &self.recordings {
            if let Ok(mut guard) = recordings.lock() {
                guard.push(alert.clone());
            }
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

    /// Dispatch a scheduler cadence alert (Phase 0 stub — materialization removed).
    pub async fn dispatch_schedule_alert(&self, alert: ScheduleAlert) {
        let (title, body) = alert.operator_message();
        self.dispatch(
            Alert::new(
                format!("materialization.scheduler.{}", alert.idempotency_suffix()),
                AlertLevel::Warning,
                AlertCategory::SchedulerHealth,
                AlertSource::Scheduler,
                title,
                body,
                Utc::now(),
            )
            .with_affects_trading(false)
            .with_visible_toast(true),
        )
        .await;
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
    use quant_pivot_models::{
        enums::common::{AlertCategory, AlertLevel, AlertSource},
        runtime_config::NotificationConfig,
    };
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

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
