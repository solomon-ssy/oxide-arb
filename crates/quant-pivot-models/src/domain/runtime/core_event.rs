//! In-process runtime event bus for control-plane notifications.

use crate::{
    domain::{
        MarketBookView, OrderIntentInfo, RecommendationReportInfo, governance::system::SystemStatus,
    },
    enums::common::{AlertCategory, AlertLevel, AlertSource},
    enums::quant::{
        EmptyReason, OrderIntentStatus, QuantRuntimeMode, RecommendationReportStatus, ReportKind,
    },
    types::MarketId,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

/// Operator-facing alert payload published on the core event bus.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SystemAlertEvent {
    pub idempotency_key: String,
    pub level: AlertLevel,
    pub category: AlertCategory,
    pub source: AlertSource,
    pub title: String,
    pub message: String,
    pub affects_trading: bool,
    pub visible_toast: bool,
    pub dedupe_secs: u64,
}

/// Which lifecycle transition a [`ReportLifecycleEvent`] describes.
///
/// `Started` / `Failed` are **ephemeral** observability signals emitted around
/// the build pipeline: a build can fail before any row is persisted, so those
/// events carry no `recommendation_report_id`. `Published` / `Empty` / `Revoked`
/// / `Expired` are always backed by a committed report row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReportEventKind {
    /// A report build started (no row yet).
    Started,
    /// A report committed with at least one recommendation.
    Published,
    /// A report committed with zero recommendations.
    Empty,
    /// A report build failed before commit (no row).
    Failed,
    /// A committed report was revoked.
    Revoked,
    /// A committed report expired by TTL.
    Expired,
}

impl ReportEventKind {
    /// Dotted observability wire name (`quant.report.<event>`).
    #[must_use]
    pub const fn wire(self) -> &'static str {
        match self {
            Self::Started => "quant.report.started",
            Self::Published => "quant.report.published",
            Self::Empty => "quant.report.empty",
            Self::Failed => "quant.report.failed",
            Self::Revoked => "quant.report.revoked",
            Self::Expired => "quant.report.expired",
        }
    }
}

/// Report lifecycle event fanned out on the single `quant.report` channel.
///
/// The discriminant is [`Self::event`]; backing report fields are present only
/// for committed-row events (`recommendation_report_id` is `None` for the
/// ephemeral `started` / `failed` signals, which correlate by `trigger_key`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportLifecycleEvent {
    pub event: ReportEventKind,
    pub trigger_key: String,
    pub recommendation_report_id: Option<String>,
    pub report_kind: ReportKind,
    pub runtime_mode: QuantRuntimeMode,
    pub status: RecommendationReportStatus,
    pub as_of: DateTime<Utc>,
    pub published_at: Option<DateTime<Utc>>,
    pub recommendation_count: u32,
    pub empty_reason: Option<EmptyReason>,
    pub error_code: Option<String>,
    pub status_reason: Option<String>,
}

impl ReportLifecycleEvent {
    /// Ephemeral `started` signal for a build keyed by `trigger_key`.
    #[must_use]
    pub const fn started(
        trigger_key: String,
        report_kind: ReportKind,
        runtime_mode: QuantRuntimeMode,
        as_of: DateTime<Utc>,
    ) -> Self {
        Self {
            event: ReportEventKind::Started,
            trigger_key,
            recommendation_report_id: None,
            report_kind,
            runtime_mode,
            status: RecommendationReportStatus::Building,
            as_of,
            published_at: None,
            recommendation_count: 0,
            empty_reason: None,
            error_code: None,
            status_reason: None,
        }
    }

    /// Ephemeral `failed` signal for a build that errored before commit.
    #[must_use]
    pub const fn failed(
        trigger_key: String,
        report_kind: ReportKind,
        runtime_mode: QuantRuntimeMode,
        as_of: DateTime<Utc>,
        error_code: String,
        status_reason: String,
    ) -> Self {
        Self {
            event: ReportEventKind::Failed,
            trigger_key,
            recommendation_report_id: None,
            report_kind,
            runtime_mode,
            status: RecommendationReportStatus::Failed,
            as_of,
            published_at: None,
            recommendation_count: 0,
            empty_reason: None,
            error_code: Some(error_code),
            status_reason: Some(status_reason),
        }
    }

    /// Committed `published` / `empty` event, discriminated by the report status.
    #[must_use]
    pub fn committed(report: &RecommendationReportInfo) -> Self {
        let event = if report.status == RecommendationReportStatus::PublishedEmpty {
            ReportEventKind::Empty
        } else {
            ReportEventKind::Published
        };
        Self::from_report(report, event)
    }

    /// Committed `revoked` event.
    #[must_use]
    pub fn revoked(report: &RecommendationReportInfo) -> Self {
        Self::from_report(report, ReportEventKind::Revoked)
    }

    /// Committed `expired` event.
    #[must_use]
    pub fn expired(report: &RecommendationReportInfo) -> Self {
        Self::from_report(report, ReportEventKind::Expired)
    }

    /// Ephemeral `empty` signal when `publish_empty_reports=false` suppresses PG persistence.
    #[must_use]
    pub fn ephemeral_empty(
        trigger_key: String,
        report_kind: ReportKind,
        runtime_mode: QuantRuntimeMode,
        as_of: DateTime<Utc>,
        empty_reason: EmptyReason,
    ) -> Self {
        Self {
            event: ReportEventKind::Empty,
            trigger_key,
            recommendation_report_id: None,
            report_kind,
            runtime_mode,
            status: RecommendationReportStatus::PublishedEmpty,
            as_of,
            published_at: None,
            recommendation_count: 0,
            empty_reason: Some(empty_reason),
            error_code: None,
            status_reason: Some(empty_reason.as_str().to_owned()),
        }
    }

    fn from_report(report: &RecommendationReportInfo, event: ReportEventKind) -> Self {
        Self {
            event,
            trigger_key: report.trigger_key.clone(),
            recommendation_report_id: Some(report.recommendation_report_id.to_string()),
            report_kind: report.report_kind,
            runtime_mode: report.runtime_mode,
            status: report.status,
            as_of: report.as_of,
            published_at: report.published_at,
            recommendation_count: report.summary_json.published_recommendation_count,
            empty_reason: report.summary_json.empty_reason,
            error_code: None,
            status_reason: report.status_reason.clone(),
        }
    }
}

/// Which lifecycle transition an [`IntentLifecycleEvent`] describes.
///
/// Every variant is backed by a committed `quant_order_intent` row (intents are
/// only ever persisted, never ephemeral): `Created` for a freshly reserved
/// intent, `Approved` after operator approval, and the terminal
/// `Rejected` / `Cancelled` / `Expired` / `Invalidated` transitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntentEventKind {
    /// An intent was created and its capital reserved.
    Created,
    /// A pending intent was approved by an operator.
    Approved,
    /// A pending intent was rejected; capital released.
    Rejected,
    /// A not-yet-submitted intent was cancelled; capital released.
    Cancelled,
    /// A pending intent passed its `expires_at`; capital released.
    Expired,
    /// A governed fact changed and the intent was invalidated; capital released.
    Invalidated,
}

impl IntentEventKind {
    /// Dotted observability wire name (`quant.intent.<event>`).
    #[must_use]
    pub const fn wire(self) -> &'static str {
        match self {
            Self::Created => "quant.intent.created",
            Self::Approved => "quant.intent.approved",
            Self::Rejected => "quant.intent.rejected",
            Self::Cancelled => "quant.intent.cancelled",
            Self::Expired => "quant.intent.expired",
            Self::Invalidated => "quant.intent.invalidated",
        }
    }
}

/// Order-intent lifecycle event fanned out on the single `quant.intent` channel.
///
/// The discriminant is [`Self::event`]; the payload carries the correlation ids,
/// the post-transition `status`, and the `reason` (status reason or approval
/// reason) so dashboards can render the audit trail without a follow-up fetch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntentLifecycleEvent {
    pub event: IntentEventKind,
    pub order_intent_id: String,
    pub recommendation_id: String,
    pub runtime_mode: QuantRuntimeMode,
    pub status: OrderIntentStatus,
    pub reason: Option<String>,
    pub occurred_at: DateTime<Utc>,
}

impl IntentLifecycleEvent {
    /// Build a lifecycle event from a committed intent row.
    #[must_use]
    pub fn from_intent(
        info: &OrderIntentInfo,
        event: IntentEventKind,
        occurred_at: DateTime<Utc>,
    ) -> Self {
        Self {
            event,
            order_intent_id: info.order_intent_id.to_string(),
            recommendation_id: info.recommendation_id.to_string(),
            runtime_mode: info.runtime_mode,
            status: info.status,
            reason: info
                .status_reason
                .clone()
                .or_else(|| info.approval_reason.clone()),
            occurred_at,
        }
    }
}

/// Cross-subsystem runtime events consumed by web and observability layers.
#[derive(Debug, Clone)]
pub enum CoreEvent {
    SystemStatusChanged(SystemStatus),
    MarketBookUpdate {
        market_id: MarketId,
        view: Box<MarketBookView>,
    },
    MarketResolved {
        market_id: MarketId,
        outcome: bool,
    },
    ConfigActivated {
        version_id: String,
    },
    Report(ReportLifecycleEvent),
    Intent(IntentLifecycleEvent),
    Alert(SystemAlertEvent),
}

impl CoreEvent {
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::SystemStatusChanged(_) => "system.status",
            Self::MarketBookUpdate { .. } => "market.book_update",
            Self::MarketResolved { .. } => "market.resolved",
            Self::ConfigActivated { .. } => "config.activated",
            Self::Report(event) => event.event.wire(),
            Self::Intent(event) => event.event.wire(),
            Self::Alert(_) => "system.alert",
        }
    }
}

pub type DropObserver = Arc<dyn Fn(&'static str) + Send + Sync>;

/// Non-blocking publisher for [`CoreEvent`] with drop counting.
#[derive(Clone)]
pub struct CoreEventPublisher {
    tx: flume::Sender<CoreEvent>,
    on_drop: Option<DropObserver>,
    dropped: Arc<AtomicU64>,
}

impl CoreEventPublisher {
    #[must_use]
    pub fn bounded(capacity: usize) -> (Self, flume::Receiver<CoreEvent>) {
        let (tx, rx) = flume::bounded(capacity);
        (
            Self {
                tx,
                on_drop: None,
                dropped: Arc::new(AtomicU64::new(0)),
            },
            rx,
        )
    }

    #[must_use]
    pub fn with_drop_hook(mut self, observer: DropObserver) -> Self {
        self.on_drop = Some(observer);
        self
    }

    /// Publish an event without ever blocking. Drops and counts on a full or
    /// disconnected channel, invoking the per-kind drop observer.
    pub fn publish(&self, event: CoreEvent) {
        let kind = event.kind();
        if self.tx.try_send(event).is_err() {
            let dropped = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            if let Some(observer) = &self.on_drop {
                observer(kind);
            }
            tracing::warn!(dropped, kind, "core event channel full; dropping event");
        }
    }

    #[must_use]
    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}
