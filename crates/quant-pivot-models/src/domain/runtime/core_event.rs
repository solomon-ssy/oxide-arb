//! In-process runtime event bus for control-plane notifications.

use crate::{
    domain::{
        MarketBookView, OrderIntentInfo, RecommendationReportInfo, ReportRunInfo, SystemStatusView,
    },
    enums::{
        common::{AlertCategory, AlertLevel, AlertSource},
        execution::{ReconciliationResult, SettlementRedeemState},
        quant::{
            EmptyReportReason, EntryConditionState, OrderIntentStatus, QuantRuntimeMode,
            RecommendationReportStatus, ReportKind, ReportRunStatus, ReportRunTerminalReason,
            ResearchJobKind, ResearchJobStatus, TrainingDatasetStatus,
        },
    },
    types::{
        ConditionTruth, ContentHash, EntryConditionInstanceId, MarketId, RecommendationReportId,
        ReportRunId, ResearchProfileId,
    },
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

/// Durable lifecycle transition represented by a [`ReportLifecycleEvent`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReportEventKind {
    Prepared,
    Published,
    Superseded,
    Obsolete,
    Revoked,
    Expired,
    DeliveryRetrying,
    DeliveryFailed,
}

impl ReportEventKind {
    /// Dotted observability wire name (`quant.report.<event>`).
    #[must_use]
    pub const fn wire(self) -> &'static str {
        match self {
            Self::Prepared => "quant.report.prepared",
            Self::Published => "quant.report.published",
            Self::Superseded => "quant.report.superseded",
            Self::Obsolete => "quant.report.obsolete",
            Self::Revoked => "quant.report.revoked",
            Self::Expired => "quant.report.expired",
            Self::DeliveryRetrying => "quant.report.delivery_retrying",
            Self::DeliveryFailed => "quant.report.delivery_failed",
        }
    }
}

/// Report lifecycle revision hint backed by committed Postgres state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportLifecycleEvent {
    pub event: ReportEventKind,
    pub recommendation_report_id: String,
    pub profile_id: ResearchProfileId,
    pub report_kind: ReportKind,
    pub runtime_mode: QuantRuntimeMode,
    pub status: RecommendationReportStatus,
    pub decision_at: DateTime<Utc>,
    pub published_at: Option<DateTime<Utc>>,
    pub recommendation_count: u32,
    pub empty_reason: Option<EmptyReportReason>,
    pub error_code: Option<String>,
    pub status_reason: Option<String>,
}

impl ReportLifecycleEvent {
    /// Committed prepared artifact event.
    #[must_use]
    pub fn prepared(report: &RecommendationReportInfo) -> Self {
        Self::from_report(report, ReportEventKind::Prepared)
    }

    /// Committed publication event. Empty is carried in `empty_reason`, not as a state.
    #[must_use]
    pub fn committed(report: &RecommendationReportInfo) -> Self {
        Self::from_report(report, ReportEventKind::Published)
    }

    /// Committed supersession event.
    #[must_use]
    pub fn superseded(report: &RecommendationReportInfo) -> Self {
        Self::from_report(report, ReportEventKind::Superseded)
    }

    /// Committed obsolete event.
    #[must_use]
    pub fn obsolete(report: &RecommendationReportInfo) -> Self {
        Self::from_report(report, ReportEventKind::Obsolete)
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

    /// Durable fact-delivery retry hint.
    #[must_use]
    pub fn delivery_retrying(report: &RecommendationReportInfo) -> Self {
        Self::from_report(report, ReportEventKind::DeliveryRetrying)
    }

    /// Durable terminal fact-delivery failure hint.
    #[must_use]
    pub fn delivery_failed(report: &RecommendationReportInfo) -> Self {
        Self::from_report(report, ReportEventKind::DeliveryFailed)
    }

    fn from_report(report: &RecommendationReportInfo, event: ReportEventKind) -> Self {
        Self {
            event,
            recommendation_report_id: report.recommendation_report_id.to_string(),
            profile_id: report.profile_id.clone(),
            report_kind: report.report_kind,
            runtime_mode: report.runtime_mode,
            status: report.status,
            decision_at: report.decision_at,
            published_at: report.published_at,
            recommendation_count: report.summary_json.published_recommendation_count,
            empty_reason: report.summary_json.empty_reason,
            error_code: None,
            status_reason: report.status_reason.clone(),
        }
    }
}

/// Durable report-run revision hint; clients always re-fetch the REST view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportRunLifecycleEvent {
    pub report_run_id: ReportRunId,
    pub status: ReportRunStatus,
    pub terminal_reason: Option<ReportRunTerminalReason>,
    pub output_report_id: Option<RecommendationReportId>,
    pub occurred_at: DateTime<Utc>,
}

impl ReportRunLifecycleEvent {
    #[must_use]
    pub fn from_run(run: &ReportRunInfo, occurred_at: DateTime<Utc>) -> Self {
        Self {
            report_run_id: run.report_run_id.clone(),
            status: run.status,
            terminal_reason: run.terminal_reason,
            output_report_id: run.output_report_id.clone(),
            occurred_at,
        }
    }
}

/// Which lifecycle transition an [`IntentLifecycleEvent`] describes.
///
/// Every variant is backed by a committed `quant_order_intent` row (intents are
/// only ever persisted, never ephemeral). The pre-submission transitions
/// (`Created` / `Approved` / `Rejected` / `Cancelled` / `Expired` /
/// `Invalidated`) are published by the intent service; the post-submission
/// transitions (`Submitted` / `AdmissionRejected` / `PartiallyFilled` /
/// `Filled` / `Failed`) are published by the dispatcher and the reconciliation
/// service as the venue truth settles, so ledger consoles converge in real time.
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
    /// An approved intent was submitted to the venue (order write-ahead).
    Submitted,
    /// Admission denied the claimed intent; capital released.
    AdmissionRejected,
    /// A submitted intent partially filled at the venue.
    PartiallyFilled,
    /// A submitted intent fully filled at the venue.
    Filled,
    /// A submitted intent failed at the venue (unfilled); capital released.
    Failed,
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
            Self::Submitted => "quant.intent.submitted",
            Self::AdmissionRejected => "quant.intent.admission_rejected",
            Self::PartiallyFilled => "quant.intent.partially_filled",
            Self::Filled => "quant.intent.filled",
            Self::Failed => "quant.intent.failed",
        }
    }

    /// The post-submission lifecycle event for a committed status, if the status
    /// maps to an observable venue-settled transition.
    ///
    /// Pre-submission states (`Draft` / `PendingApproval` / `Approved` /
    /// `ApprovedByPolicy`) and the transient `AdmissionPending` claim have no
    /// post-submission event; the intent service publishes their transitions
    /// explicitly. Used by the dispatcher and reconciliation service to fan out
    /// the venue-settled outcome via [`IntentLifecyclePublisher`].
    ///
    /// [`IntentLifecyclePublisher`]: crate::domain::runtime::core_event
    #[must_use]
    pub const fn for_execution_status(status: OrderIntentStatus) -> Option<Self> {
        match status {
            OrderIntentStatus::Submitted => Some(Self::Submitted),
            OrderIntentStatus::AdmissionRejected => Some(Self::AdmissionRejected),
            OrderIntentStatus::PartiallyFilled => Some(Self::PartiallyFilled),
            OrderIntentStatus::Filled => Some(Self::Filled),
            OrderIntentStatus::Failed => Some(Self::Failed),
            OrderIntentStatus::Cancelled => Some(Self::Cancelled),
            _ => None,
        }
    }
}

#[cfg(test)]
mod intent_event_kind_tests {
    use super::{IntentEventKind, OrderIntentStatus};

    #[test]
    fn post_submission_statuses_map_to_events() {
        assert_eq!(
            IntentEventKind::for_execution_status(OrderIntentStatus::Submitted),
            Some(IntentEventKind::Submitted),
        );
        assert_eq!(
            IntentEventKind::for_execution_status(OrderIntentStatus::Filled),
            Some(IntentEventKind::Filled),
        );
        assert_eq!(
            IntentEventKind::for_execution_status(OrderIntentStatus::PartiallyFilled),
            Some(IntentEventKind::PartiallyFilled),
        );
        assert_eq!(
            IntentEventKind::for_execution_status(OrderIntentStatus::Failed),
            Some(IntentEventKind::Failed),
        );
        assert_eq!(
            IntentEventKind::for_execution_status(OrderIntentStatus::AdmissionRejected),
            Some(IntentEventKind::AdmissionRejected),
        );
        assert_eq!(
            IntentEventKind::for_execution_status(OrderIntentStatus::Cancelled),
            Some(IntentEventKind::Cancelled),
        );
    }

    #[test]
    fn pre_submission_and_transient_statuses_have_no_event() {
        for status in [
            OrderIntentStatus::Draft,
            OrderIntentStatus::PendingApproval,
            OrderIntentStatus::Approved,
            OrderIntentStatus::ApprovedByPolicy,
            OrderIntentStatus::AdmissionPending,
        ] {
            assert_eq!(IntentEventKind::for_execution_status(status), None);
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

/// Which materialization job a [`MaterializationRunEvent`] describes, so the
/// workbench can scope its re-fetch (dataset build vs model train vs backtest).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaterializationRunKind {
    /// Offline training-dataset plan → build.
    Dataset,
    /// Offline model training run.
    Training,
    /// Point-in-time backtest run.
    Backtest,
    /// Favorite-longshot bias-table fit (Phase 11.2.1).
    BiasTableFit,
    /// Model-score probability-calibrator fit (Phase 11.3).
    ModelCalibrationFit,
    /// Combinatorial Purged Cross-Validation + governed trial-grid run (Phase 11.5).
    CpcvBacktest,
    /// Deterministic training/serving parity replay (Phase 11.6).
    FeatureParity,
    /// Executable trade-policy fit (Phase 11.7).
    TradePolicyFit,
    /// Independent row-level trade-policy validation (Phase 11.7.2).
    TradePolicyValidation,
}

/// Terminal-or-progress status of a materialization run.
///
/// Normalized across the dataset builder (`TrainingDatasetStatus`) and model-run
/// (`ModelRunStatus`) state machines into the single wire vocabulary the
/// workbench renders.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaterializationRunStatus {
    /// Enqueued, not yet started (reserved for a future async job queue).
    Queued,
    /// In progress.
    Running,
    /// Finished successfully.
    Completed,
    /// Finished with an error.
    Failed,
    /// Cancelled before completion.
    Cancelled,
}

impl From<ResearchJobKind> for MaterializationRunKind {
    fn from(kind: ResearchJobKind) -> Self {
        match kind {
            ResearchJobKind::DatasetBuild => Self::Dataset,
            ResearchJobKind::ModelTrain => Self::Training,
            ResearchJobKind::Backtest => Self::Backtest,
            ResearchJobKind::BiasTableFit => Self::BiasTableFit,
            ResearchJobKind::ModelCalibrationFit => Self::ModelCalibrationFit,
            ResearchJobKind::CpcvBacktest => Self::CpcvBacktest,
            ResearchJobKind::FeatureParity => Self::FeatureParity,
            ResearchJobKind::TradePolicyFit => Self::TradePolicyFit,
            ResearchJobKind::TradePolicyValidation => Self::TradePolicyValidation,
        }
    }
}

impl From<ResearchJobStatus> for MaterializationRunStatus {
    fn from(status: ResearchJobStatus) -> Self {
        match status {
            ResearchJobStatus::Queued => Self::Queued,
            ResearchJobStatus::Running => Self::Running,
            ResearchJobStatus::Succeeded => Self::Completed,
            ResearchJobStatus::Failed => Self::Failed,
            ResearchJobStatus::Cancelled => Self::Cancelled,
        }
    }
}

impl From<TrainingDatasetStatus> for MaterializationRunStatus {
    fn from(status: TrainingDatasetStatus) -> Self {
        match status {
            TrainingDatasetStatus::Failed | TrainingDatasetStatus::InsufficientLabels => {
                Self::Failed
            }
            TrainingDatasetStatus::Planned | TrainingDatasetStatus::Building => Self::Running,
            TrainingDatasetStatus::Ready | TrainingDatasetStatus::Expired => Self::Completed,
        }
    }
}

/// Materialization run lifecycle event fanned out on `materialization.run_update`.
///
/// A revision hint only: the workbench re-fetches the dataset / model / report
/// by id (WS never carries catalog rows).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MaterializationRunEvent {
    /// The run subject id (`training_dataset_id` for datasets, `model_run_id`
    /// for training / backtest runs), or the durable job id when no result
    /// artifact exists yet.
    pub run_id: String,
    pub kind: MaterializationRunKind,
    pub status: MaterializationRunStatus,
    /// The durable research-job id driving this run (async job engine).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    /// Current execution phase (e.g. `prefetch`, `materialize`, `finalize`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    /// Completion fraction in `[0, 1]` when a positive total is known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pct: Option<f64>,
}

impl MaterializationRunEvent {
    /// A minimal revision hint (no job/progress detail) — open catalogs re-fetch.
    #[must_use]
    pub fn revision(
        run_id: impl Into<String>,
        kind: MaterializationRunKind,
        status: MaterializationRunStatus,
    ) -> Self {
        Self {
            run_id: run_id.into(),
            kind,
            status,
            job_id: None,
            phase: None,
            pct: None,
        }
    }

    /// A job-scoped progress/lifecycle event carrying phase + completion fraction.
    #[must_use]
    pub fn job(
        job_id: impl Into<String>,
        run_id: impl Into<String>,
        kind: MaterializationRunKind,
        status: MaterializationRunStatus,
        phase: Option<String>,
        pct: Option<f64>,
    ) -> Self {
        Self {
            run_id: run_id.into(),
            kind,
            status,
            job_id: Some(job_id.into()),
            phase,
            pct,
        }
    }
}

/// Reconciliation row lifecycle event fanned out on `quant.reconciliation`.
///
/// A revision hint only: the reconciliation queue + recovery panel re-fetch over
/// REST on any bump. Carries the correlation ids + terminal verdict for logging.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconciliationLifecycleEvent {
    pub execution_order_id: String,
    pub order_intent_id: String,
    pub result: ReconciliationResult,
    /// Whether an operator (vs the worker) drove this resolution.
    pub operator_resolved: bool,
}

/// Settlement-redeem state transition fanned out on `quant.settlement`.
///
/// A revision hint only: the settlement ledger re-fetches over REST on any bump.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettlementRedeemLifecycleEvent {
    pub settlement_redeem_id: String,
    pub market_id: MarketId,
    pub state: SettlementRedeemState,
}

/// Condition-instance revision hint fanned out on `quant.condition`.
/// Full evidence remains on the REST detail endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntryConditionLifecycleEvent {
    pub condition_instance_id: EntryConditionInstanceId,
    pub revision: i64,
    pub state: EntryConditionState,
    pub truth: Option<ConditionTruth>,
    pub evaluation_hash: Option<ContentHash>,
}

/// Cross-subsystem runtime events consumed by web and observability layers.
#[derive(Debug, Clone)]
pub enum CoreEvent {
    SystemStatusChanged(Box<SystemStatusView>),
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
    ReportRun(ReportRunLifecycleEvent),
    Intent(IntentLifecycleEvent),
    Condition(EntryConditionLifecycleEvent),
    Alert(SystemAlertEvent),
    MaterializationRun(MaterializationRunEvent),
    Reconciliation(ReconciliationLifecycleEvent),
    Settlement(SettlementRedeemLifecycleEvent),
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
            Self::ReportRun(_) => "quant.report_run",
            Self::Intent(event) => event.event.wire(),
            Self::Condition(_) => "quant.condition",
            Self::Alert(_) => "system.alert",
            Self::MaterializationRun(_) => "materialization.run_update",
            Self::Reconciliation(_) => "quant.reconciliation",
            Self::Settlement(_) => "quant.settlement",
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
