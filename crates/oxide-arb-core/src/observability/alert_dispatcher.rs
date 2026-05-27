use chrono::{DateTime, Utc};
use dashmap::DashMap;
use std::{
    fmt::{self, Display, Formatter},
    time::{Duration, Instant},
};
use teloxide::{prelude::*, types::ChatId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
    Emergency,
}

impl Display for AlertSeverity {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Info => write!(f, "INFO"),
            Self::Warning => write!(f, "WARNING"),
            Self::Critical => write!(f, "CRITICAL"),
            Self::Emergency => write!(f, "EMERGENCY"),
        }
    }
}

pub struct Alert {
    pub severity: AlertSeverity,
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

pub struct AlertDispatcher {
    telegram: Option<TelegramChannel>,
    webhook: Option<WebhookChannel>,
    cooldown: DashMap<String, Instant>,
    cooldown_duration: Duration,
}

impl AlertDispatcher {
    pub fn new(
        telegram_bot_token: Option<&str>,
        telegram_chat_id: Option<i64>,
        webhook_url: Option<&str>,
        cooldown_secs: u64,
    ) -> Self {
        let telegram = match (telegram_bot_token, telegram_chat_id) {
            (Some(token), Some(chat_id)) => Some(TelegramChannel {
                bot: Bot::new(token),
                chat_id: ChatId(chat_id),
            }),
            _ => None,
        };

        let webhook = webhook_url.map(|url| WebhookChannel {
            client: reqwest::Client::new(),
            url: url.to_owned(),
        });

        Self {
            telegram,
            webhook,
            cooldown: DashMap::new(),
            cooldown_duration: Duration::from_secs(cooldown_secs),
        }
    }

    pub async fn dispatch(&self, alert: Alert) {
        let cooldown_key = format!("{}:{}", alert.severity, alert.title);
        if let Some(last) = self.cooldown.get(&cooldown_key) {
            if last.elapsed() < self.cooldown_duration {
                tracing::debug!(title = %alert.title, "alert suppressed by cooldown");
                return;
            }
        }
        self.cooldown.insert(cooldown_key, Instant::now());

        let text = format!(
            "[{}] {}\n{}\n{}",
            alert.severity, alert.title, alert.body, alert.timestamp
        );

        match alert.severity {
            AlertSeverity::Emergency | AlertSeverity::Critical => {
                self.send_telegram(&text).await;
                self.send_webhook(&alert).await;
            }
            AlertSeverity::Warning => {
                self.send_webhook(&alert).await;
            }
            AlertSeverity::Info => {
                tracing::info!(severity = %alert.severity, title = %alert.title, "{}", alert.body);
            }
        }
    }

    async fn send_telegram(&self, text: &str) {
        let Some(tg) = &self.telegram else { return };
        if let Err(e) = tg.bot.send_message(tg.chat_id, text).await {
            tracing::error!(error = %e, "failed to send telegram alert");
        }
    }

    async fn send_webhook(&self, alert: &Alert) {
        let Some(wh) = &self.webhook else { return };
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
}
