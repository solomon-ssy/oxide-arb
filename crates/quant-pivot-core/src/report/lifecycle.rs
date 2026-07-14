//! Report lifecycle service.

use std::sync::{Arc, OnceLock};

use chrono::{DateTime, Utc};
use quant_pivot_error::{
    QuantError, QuantResult,
    report::ReportError,
    storage::{StorageError, entity},
};
use quant_pivot_models::{
    domain::{NewOperationLog, OrderIntentInfo, RecommendationReportInfo, ReportLifecycleEvent},
    enums::{
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
    execution::IntentTerminalEventSink,
    governance::RuntimeModeHandle,
    observability::metrics_hub::MetricsHub,
    service::feature_integrity::{FeatureParityGatePort, FeatureParityRunCoordinator},
};

use super::{
    builder::{ReportBuilder, report_decision_at},
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
    pub knowledge_lag_secs: Option<u64>,
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
    pub feature_parity_gate: Arc<dyn FeatureParityGatePort>,
    pub feature_parity_runs: Arc<FeatureParityRunCoordinator>,
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
    feature_parity_gate: Arc<dyn FeatureParityGatePort>,
    feature_parity_runs: Arc<FeatureParityRunCoordinator>,
    run_lock: Mutex<()>,
    /// Post-commit event sink for intents atomically invalidated by report or
    /// recommendation terminal repository commands.
    intent_terminal_events: OnceLock<Arc<dyn IntentTerminalEventSink>>,
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
            feature_parity_gate: deps.feature_parity_gate,
            feature_parity_runs: deps.feature_parity_runs,
            run_lock: Mutex::new(()),
            intent_terminal_events: OnceLock::new(),
        }
    }

    /// Install the order-intent terminal event sink (idempotent; first set wins).
    pub fn set_intent_terminal_event_sink(&self, sink: Arc<dyn IntentTerminalEventSink>) {
        let _ = self.intent_terminal_events.set(sink);
    }

    fn publish_invalidated_intents(&self, intents: &[OrderIntentInfo], now: DateTime<Utc>) {
        if let Some(sink) = self.intent_terminal_events.get() {
            sink.publish_invalidated(intents, now);
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
            knowledge_lag_secs_override: None,
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
            knowledge_lag_secs_override: request.knowledge_lag_secs,
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
        let (report, invalidated_intents) = self
            .report_repo
            .revoke(
                report_id,
                reason,
                revoked_at,
                lifecycle_operation_log("revoke", report_id, reason),
            )
            .await?;
        self.publisher.publish_revoked(&report);
        self.publish_invalidated_intents(&invalidated_intents, revoked_at);
        Ok(report)
    }

    /// Idempotently contain one report affected by a deterministic parity
    /// incident.
    ///
    /// The repository revoke command atomically terminates recommendations,
    /// pre-submission intents, conditions, and capital before this method emits
    /// any lifecycle event. Only the exact terminal-state race is accepted;
    /// unrelated failures continue to fail containment closed.
    pub(crate) async fn contain_parity_incident(
        &self,
        report_id: &RecommendationReportId,
        reason: &str,
        occurred_at: DateTime<Utc>,
    ) -> QuantResult<()> {
        let current = self
            .report_repo
            .find_by_id(report_id)
            .await?
            .ok_or_else(|| {
                StorageError::not_found(entity::QUANT_RECOMMENDATION_REPORT, report_id)
            })?;

        if !is_parity_contained_report_status(current.status) {
            match self
                .report_repo
                .revoke(
                    report_id,
                    reason,
                    occurred_at,
                    lifecycle_operation_log("parity_containment", report_id, reason),
                )
                .await
            {
                Ok((revoked, invalidated_intents)) => {
                    self.publisher.publish_revoked(&revoked);
                    self.publish_invalidated_intents(&invalidated_intents, occurred_at);
                }
                Err(error) if is_report_revoke_transition_conflict(&error, report_id) => {
                    let latest =
                        self.report_repo
                            .find_by_id(report_id)
                            .await?
                            .ok_or_else(|| {
                                StorageError::not_found(
                                    entity::QUANT_RECOMMENDATION_REPORT,
                                    report_id,
                                )
                            })?;
                    if !is_parity_contained_report_status(latest.status) {
                        return Err(error.into());
                    }
                }
                Err(error) => return Err(error.into()),
            }
        }

        Ok(())
    }

    /// Expire every recommendation whose data-driven `valid_until` has elapsed
    /// (`Published` / `IntentCreated -> Expired`), oldest deadline first, up to
    /// `limit` per pass. Each expiry atomically releases every pre-submission
    /// reservation before commit, then performs a best-effort report roll-up. A
    /// single conflict is logged and skipped.
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
                .expire(&recommendation_id, now, log)
                .await
            {
                Ok((recommendation, invalidated_intents)) => {
                    expired =
                        expired
                            .checked_add(1)
                            .ok_or_else(|| ReportError::NumericOverflow {
                                field: "report.expired_recommendation_count",
                                detail: "expiry sweep count exceeds u32".to_owned(),
                            })?;
                    self.publish_invalidated_intents(&invalidated_intents, now);
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
                rolled = rolled
                    .checked_add(1)
                    .ok_or_else(|| ReportError::NumericOverflow {
                        field: "report.expired_report_count",
                        detail: "expiry sweep count exceeds u32".to_owned(),
                    })?;
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
            self.feature_parity_runs
                .ensure_report_sample_committed(&existing)
                .await?;
            return Ok(existing);
        }
        let _run_guard = self.run_lock.lock().await;
        if let Some(existing) = self.report_repo.find_by_trigger_key(&trigger_key).await? {
            self.feature_parity_runs
                .ensure_report_sample_committed(&existing)
                .await?;
            return Ok(existing);
        }
        self.feature_parity_gate
            .ensure_clear("new report generation")
            .await?;

        // Ephemeral start/fail signals correlate by `trigger_key` (no row yet).
        let runtime_mode = self.runtime_mode.current();
        let decision_at = report_decision_at(self.runtime_config_repo.as_ref(), &request).await?;

        self.publisher
            .publish_ephemeral(ReportLifecycleEvent::started(
                trigger_key.clone(),
                ReportKind::TopN,
                runtime_mode,
                decision_at,
            ));

        let mut composed = match self.builder.build(request).await {
            Ok(composed) => composed,
            Err(error) => {
                self.publish_failed(&trigger_key, runtime_mode, decision_at, &error);
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
                    decision_at,
                    empty_reason,
                ));
            self.metrics.report_skipped_empty_total.inc();
            return Err(ReportError::EmptyReportSuppressed {
                reason: empty_reason.as_str().to_owned(),
            }
            .into());
        }
        let sampled_feature_parity = self
            .feature_parity_runs
            .build_report_sample(&composed.transaction.report)
            .await?;
        composed.transaction.sampled_feature_parity = Some(sampled_feature_parity);
        composed.transaction.feature_parity_state_id = Some(
            self.feature_parity_gate
                .commit_state_id("report commit")
                .await?,
        );
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
                    self.feature_parity_runs
                        .ensure_report_sample_committed(&existing)
                        .await?;
                    return Ok(existing);
                }
                let error: QuantError = error.into();
                self.publish_failed(&trigger_key, runtime_mode, decision_at, &error);
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

const fn is_parity_contained_report_status(status: RecommendationReportStatus) -> bool {
    matches!(
        status,
        RecommendationReportStatus::Failed
            | RecommendationReportStatus::Revoked
            | RecommendationReportStatus::Expired
    )
}

fn is_report_revoke_transition_conflict(
    error: &StorageError,
    report_id: &RecommendationReportId,
) -> bool {
    let expected_id = report_id.to_string();
    matches!(
        error,
        StorageError::IllegalTransition {
            entity: error_entity,
            id: Some(error_id),
            to,
            ..
        } if *error_entity == entity::QUANT_RECOMMENDATION_REPORT
            && error_id == &expected_id
            && to == RecommendationReportStatus::Revoked.as_str()
    )
}

#[cfg(test)]
mod parity_containment_tests {
    use super::*;

    #[test]
    fn only_terminal_report_states_skip_parity_revoke() {
        for status in [
            RecommendationReportStatus::Failed,
            RecommendationReportStatus::Revoked,
            RecommendationReportStatus::Expired,
        ] {
            assert!(is_parity_contained_report_status(status));
        }
        for status in [
            RecommendationReportStatus::Building,
            RecommendationReportStatus::Published,
            RecommendationReportStatus::PublishedEmpty,
        ] {
            assert!(!is_parity_contained_report_status(status));
        }
    }

    #[test]
    fn only_exact_report_revoke_transition_conflict_is_retry_classified() {
        let report_id = RecommendationReportId::from_v7();
        let exact = StorageError::illegal_transition(
            entity::QUANT_RECOMMENDATION_REPORT,
            Some(&report_id),
            RecommendationReportStatus::Expired.as_str(),
            RecommendationReportStatus::Revoked.as_str(),
        );
        assert!(is_report_revoke_transition_conflict(&exact, &report_id));

        let unrelated_report = StorageError::illegal_transition(
            entity::QUANT_RECOMMENDATION_REPORT,
            Some(&RecommendationReportId::from_v7()),
            RecommendationReportStatus::Expired.as_str(),
            RecommendationReportStatus::Revoked.as_str(),
        );
        assert!(!is_report_revoke_transition_conflict(
            &unrelated_report,
            &report_id
        ));
        let unrelated_entity = StorageError::illegal_transition(
            entity::QUANT_RECOMMENDATION,
            Some(&report_id),
            "attributed",
            "revoked",
        );
        assert!(!is_report_revoke_transition_conflict(
            &unrelated_entity,
            &report_id
        ));
        assert!(!is_report_revoke_transition_conflict(
            &StorageError::Connection("database unavailable".to_owned()),
            &report_id
        ));
    }
}
