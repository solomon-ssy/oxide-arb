//! Report lifecycle service.

use std::sync::{Arc, OnceLock};

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult, report::ReportError};
use quant_pivot_models::{
    domain::{NewOperationLog, RecommendationReportInfo, ReportLifecycleEvent},
    enums::{
        execution::ApprovalInvalidation,
        operation_log::{OperationCategory, OperationOutcome},
        quant::{EmptyReportReason, QuantRuntimeMode, RecommendationReportStatus, ReportKind},
        rbac::ResourceType,
    },
    runtime_config::RuntimeConfig,
    types::{OperationLogId, RecommendationId, RecommendationReportId},
};
use quant_pivot_repository::traits::{
    RecommendationReportRepository, RecommendationRepository, RuntimeConfigVersionRepository,
};
use tokio::sync::Mutex;

use crate::{
    execution::IntentInvalidationHook, governance::RuntimeModeHandle,
    observability::metrics_hub::MetricsHub,
};

use super::{
    builder::{ReportBuilder, report_as_of},
    publisher::ReportPublisher,
    types::{BuildReportRequest, ComposedReport, ReportTrigger},
};

/// Scheduled report trigger request.
#[derive(Debug, Clone)]
pub struct ScheduledReportRequest {
    pub schedule_id: String,
    pub trigger_time: DateTime<Utc>,
}

/// Ad-hoc report trigger request.
#[derive(Debug, Clone)]
pub struct AdHocReportRequest {
    pub request_id: String,
    pub trigger_time: DateTime<Utc>,
    pub top_n: Option<u32>,
    pub source_delay_secs: Option<u64>,
}

/// Dependencies for [`ReportLifecycleService`].
pub struct ReportLifecycleDeps {
    pub report_repo: Arc<dyn RecommendationReportRepository>,
    pub recommendation_repo: Arc<dyn RecommendationRepository>,
    pub runtime_config_repo: Arc<dyn RuntimeConfigVersionRepository>,
    pub builder: Arc<dyn ReportBuilder>,
    pub publisher: Arc<ReportPublisher>,
    pub runtime_mode: RuntimeModeHandle,
    pub metrics: Arc<MetricsHub>,
}

/// End-to-end report lifecycle entry point.
pub struct ReportLifecycleService {
    report_repo: Arc<dyn RecommendationReportRepository>,
    recommendation_repo: Arc<dyn RecommendationRepository>,
    runtime_config_repo: Arc<dyn RuntimeConfigVersionRepository>,
    builder: Arc<dyn ReportBuilder>,
    publisher: Arc<ReportPublisher>,
    runtime_mode: RuntimeModeHandle,
    metrics: Arc<MetricsHub>,
    run_lock: Mutex<()>,
    /// Cascade hook: on revoke / expire, the active order intents derived from
    /// the report are invalidated and their capital released. Set once at boot
    /// (absent in report-only builds without the execution plane).
    intent_invalidation: OnceLock<Arc<dyn IntentInvalidationHook>>,
}

impl ReportLifecycleService {
    /// Build a lifecycle service.
    #[must_use]
    pub fn new(deps: ReportLifecycleDeps) -> Self {
        Self {
            report_repo: deps.report_repo,
            recommendation_repo: deps.recommendation_repo,
            runtime_config_repo: deps.runtime_config_repo,
            builder: deps.builder,
            publisher: deps.publisher,
            runtime_mode: deps.runtime_mode,
            metrics: deps.metrics,
            run_lock: Mutex::new(()),
            intent_invalidation: OnceLock::new(),
        }
    }

    /// Install the order-intent cascade hook (idempotent; first set wins).
    pub fn set_intent_invalidation_hook(&self, hook: Arc<dyn IntentInvalidationHook>) {
        let _ = self.intent_invalidation.set(hook);
    }

    /// Cascade-invalidate the active intents derived from a terminated report.
    ///
    /// Best-effort: a failure is logged, not propagated — the approval-time
    /// re-check and the expiry sweep are the fail-closed backstops.
    async fn cascade_intent_invalidation(
        &self,
        report_id: &RecommendationReportId,
        reason: ApprovalInvalidation,
        now: DateTime<Utc>,
    ) {
        let Some(hook) = self.intent_invalidation.get() else {
            return;
        };
        match hook.invalidate_for_report(report_id, reason, now).await {
            Ok(count) if count > 0 => {
                tracing::info!(%report_id, count, reason = reason.as_str(), "cascaded intent invalidation");
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(%report_id, %error, "intent cascade invalidation failed");
            }
        }
    }

    /// Cascade-invalidate the active intents derived from one expired
    /// recommendation. Best-effort (the intent's own `expires_at` sweep is the
    /// fail-closed backstop, since `intent.expires_at <= recommendation.valid_until`).
    async fn cascade_recommendation_invalidation(
        &self,
        recommendation_id: &RecommendationId,
        reason: ApprovalInvalidation,
        now: DateTime<Utc>,
    ) {
        let Some(hook) = self.intent_invalidation.get() else {
            return;
        };
        match hook
            .invalidate_for_recommendation(recommendation_id, reason, now)
            .await
        {
            Ok(count) if count > 0 => {
                tracing::info!(%recommendation_id, count, reason = reason.as_str(), "cascaded recommendation intent invalidation");
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(%recommendation_id, %error, "recommendation intent cascade failed");
            }
        }
    }

    /// Run a scheduled report with fixed idempotency key semantics.
    pub async fn run_scheduled(
        &self,
        request: ScheduledReportRequest,
    ) -> QuantResult<RecommendationReportInfo> {
        let trigger = ReportTrigger::Scheduled {
            schedule_id: request.schedule_id,
        };
        self.run(BuildReportRequest {
            trigger,
            trigger_time: request.trigger_time,
            top_n_override: None,
            source_delay_secs_override: None,
        })
        .await
    }

    /// Run an ad-hoc report with request-id idempotency.
    pub async fn run_ad_hoc(
        &self,
        request: AdHocReportRequest,
    ) -> QuantResult<RecommendationReportInfo> {
        let trigger = ReportTrigger::AdHoc {
            request_id: request.request_id,
        };
        self.run(BuildReportRequest {
            trigger,
            trigger_time: request.trigger_time,
            top_n_override: request.top_n,
            source_delay_secs_override: request.source_delay_secs,
        })
        .await
    }

    /// Revoke a committed report in one repository transaction, then publish the
    /// lifecycle event after commit.
    pub async fn revoke(
        &self,
        report_id: &RecommendationReportId,
        reason: &str,
        revoked_at: DateTime<Utc>,
    ) -> QuantResult<RecommendationReportInfo> {
        let report = self
            .report_repo
            .revoke(
                report_id,
                reason,
                revoked_at,
                lifecycle_operation_log("revoke", report_id, reason),
            )
            .await?;
        self.publisher.publish_revoked(&report);
        self.cascade_intent_invalidation(
            report_id,
            ApprovalInvalidation::ReportRevoked,
            revoked_at,
        )
        .await;
        Ok(report)
    }

    /// Expire every recommendation whose data-driven `valid_until` has elapsed
    /// (`Published` / `IntentCreated -> Expired`), oldest deadline first, up to
    /// `limit` per pass. Each expiry: its own committed transaction, then a
    /// best-effort cascade releasing that recommendation's reserved capital, then
    /// a best-effort report roll-up. A single conflict is logged and skipped.
    /// Returns the number of recommendations expired.
    pub async fn expire_due_recommendations(
        &self,
        now: DateTime<Utc>,
        limit: u64,
    ) -> QuantResult<u32> {
        let due = self.recommendation_repo.find_expirable(now, limit).await?;

        let mut expired = 0_u32;
        for recommendation_id in due {
            let log = recommendation_operation_log(&recommendation_id, "ttl_expired");
            match self
                .recommendation_repo
                .expire(&recommendation_id, log)
                .await
            {
                Ok(recommendation) => {
                    expired = expired.saturating_add(1);
                    self.cascade_recommendation_invalidation(
                        &recommendation_id,
                        ApprovalInvalidation::RecommendationExpired,
                        now,
                    )
                    .await;
                    self.try_roll_up_report(&recommendation.recommendation_report_id, now)
                        .await;
                }
                Err(error) => {
                    tracing::warn!(%recommendation_id, %error, "recommendation ttl expiry skipped");
                }
            }
        }
        Ok(expired)
    }

    /// Roll reports up to `Expired` once all their recommendations are terminal,
    /// oldest roll-up deadline first, up to `limit`. This is the durable backstop
    /// for the per-recommendation deadline scheduler (it also finalizes empty
    /// reports, which carry no recommendations to drive the roll-up). Returns the
    /// number of reports rolled up.
    pub async fn expire_due_reports(&self, now: DateTime<Utc>, limit: u64) -> QuantResult<u32> {
        let due = self.report_repo.find_expirable(now, limit).await?;

        let mut rolled = 0_u32;
        for report_id in due {
            if self.try_roll_up_report(&report_id, now).await {
                rolled = rolled.saturating_add(1);
            }
        }
        Ok(rolled)
    }

    /// Roll a report up to `Expired` iff every recommendation is terminal, then
    /// publish the lifecycle event. Best-effort; returns whether it rolled up.
    /// No intent cascade here — each recommendation already released its own
    /// intents on expiry, and empty reports carry no intents.
    async fn try_roll_up_report(
        &self,
        report_id: &RecommendationReportId,
        now: DateTime<Utc>,
    ) -> bool {
        match self
            .report_repo
            .roll_up_to_expired(
                report_id,
                now,
                lifecycle_operation_log("expire", report_id, "ttl_expired"),
            )
            .await
        {
            Ok(Some(report)) => {
                self.publisher.publish_expired(&report);
                true
            }
            Ok(None) => false,
            Err(error) => {
                tracing::warn!(%report_id, %error, "report roll-up to expired skipped");
                false
            }
        }
    }

    async fn run(&self, request: BuildReportRequest) -> QuantResult<RecommendationReportInfo> {
        let trigger_time = request.trigger_time;
        let trigger_key = request.trigger.key(request.trigger_time);
        if let Some(existing) = self.report_repo.find_by_trigger_key(&trigger_key).await? {
            return Ok(existing);
        }
        let _run_guard = self.run_lock.lock().await;
        if let Some(existing) = self.report_repo.find_by_trigger_key(&trigger_key).await? {
            return Ok(existing);
        }

        // Ephemeral start/fail signals correlate by `trigger_key` (no row yet).
        let runtime_mode = self.runtime_mode.current();
        let as_of = report_as_of(self.runtime_config_repo.as_ref(), &request).await?;

        self.publisher
            .publish_ephemeral(ReportLifecycleEvent::started(
                trigger_key.clone(),
                ReportKind::TopN,
                runtime_mode,
                as_of,
            ));

        let composed = match self.builder.build(request).await {
            Ok(composed) => composed,
            Err(error) => {
                self.publish_failed(&trigger_key, runtime_mode, as_of, &error);
                return Err(error);
            }
        };
        let config = active_runtime_config(self.runtime_config_repo.as_ref(), trigger_time).await?;
        if should_suppress_empty_report(&composed, &config) {
            let empty_reason = empty_reason_from_composed(&composed);
            self.publisher
                .publish_ephemeral(ReportLifecycleEvent::ephemeral_empty(
                    trigger_key.clone(),
                    ReportKind::TopN,
                    runtime_mode,
                    as_of,
                    empty_reason,
                ));
            self.metrics.report_skipped_empty_total.inc();
            return Err(ReportError::EmptyReportSuppressed {
                reason: empty_reason.as_str().to_owned(),
            }
            .into());
        }
        match self
            .report_repo
            .create_report(composed.transaction.clone())
            .await
        {
            Ok(report) => {
                self.publisher.publish_committed(&report, &composed).await;
                Ok(report)
            }
            Err(error) => {
                if let Some(existing) = self.report_repo.find_by_trigger_key(&trigger_key).await? {
                    return Ok(existing);
                }
                let error: QuantError = error.into();
                self.publish_failed(&trigger_key, runtime_mode, as_of, &error);
                Err(error)
            }
        }
    }

    fn publish_failed(
        &self,
        trigger_key: &str,
        runtime_mode: QuantRuntimeMode,
        as_of: DateTime<Utc>,
        error: &QuantError,
    ) {
        self.publisher
            .publish_ephemeral(ReportLifecycleEvent::failed(
                trigger_key.to_owned(),
                ReportKind::TopN,
                runtime_mode,
                as_of,
                error.code().to_owned(),
                error.to_string(),
            ));
    }
}

async fn active_runtime_config(
    runtime_config_repo: &dyn RuntimeConfigVersionRepository,
    trigger_time: DateTime<Utc>,
) -> QuantResult<RuntimeConfig> {
    let version = runtime_config_repo
        .load_active_at(trigger_time)
        .await?
        .ok_or_else(|| QuantError::config("no active runtime config version"))?;
    RuntimeConfig::from_json(&version.config_json).map_err(QuantError::from)
}

fn should_suppress_empty_report(composed: &ComposedReport, config: &RuntimeConfig) -> bool {
    !config.reports.publish_empty_reports
        && composed.transaction.recommendations.is_empty()
        && composed.transaction.report.status == RecommendationReportStatus::PublishedEmpty
}

fn empty_reason_from_composed(composed: &ComposedReport) -> EmptyReportReason {
    composed
        .notification
        .empty_reason
        .unwrap_or(EmptyReportReason::EmptySelection)
}

fn recommendation_operation_log(
    recommendation_id: &RecommendationId,
    reason: &str,
) -> NewOperationLog {
    NewOperationLog {
        id: OperationLogId::from_v7(),
        request_id: format!("quant-recommendation:expire:{recommendation_id}"),
        actor_user_id: None,
        actor_username: Some("system".to_owned()),
        acting_role: Some("report_lifecycle".to_owned()),
        category: OperationCategory::QuantReport,
        action: "recommendation.expire".to_owned(),
        resource_type: Some(ResourceType::QuantReport),
        resource_id: Some(recommendation_id.to_string()),
        http_method: "SYSTEM".to_owned(),
        http_path: format!("/system/quant/recommendation/{recommendation_id}/expire"),
        http_status: 200,
        outcome: OperationOutcome::Success,
        client_ip: None,
        user_agent: None,
        latency_ms: 0,
        detail: serde_json::json!({ "reason": reason }),
        before_hash: None,
        after_hash: None,
        governance_audit_event_id: None,
        governance_audit_sequence: None,
    }
}

fn lifecycle_operation_log(
    action: &str,
    report_id: &RecommendationReportId,
    reason: &str,
) -> NewOperationLog {
    NewOperationLog {
        id: OperationLogId::from_v7(),
        request_id: format!("quant-report:{action}:{report_id}"),
        actor_user_id: None,
        actor_username: Some("system".to_owned()),
        acting_role: Some("report_lifecycle".to_owned()),
        category: OperationCategory::QuantReport,
        action: action.to_owned(),
        resource_type: Some(ResourceType::QuantReport),
        resource_id: Some(report_id.to_string()),
        http_method: "SYSTEM".to_owned(),
        http_path: format!("/system/quant/report/{report_id}/{action}"),
        http_status: 200,
        outcome: OperationOutcome::Success,
        client_ip: None,
        user_agent: None,
        latency_ms: 0,
        detail: serde_json::json!({ "reason": reason }),
        before_hash: None,
        after_hash: None,
        governance_audit_event_id: None,
        governance_audit_sequence: None,
    }
}
