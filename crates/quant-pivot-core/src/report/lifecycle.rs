//! Report lifecycle service.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    domain::{NewOperationLog, RecommendationReportInfo, ReportLifecycleEvent},
    enums::{
        operation_log::{OperationCategory, OperationOutcome},
        quant::{QuantRuntimeMode, ReportKind},
        rbac::ResourceType,
    },
    types::{OperationLogId, RecommendationReportId},
};
use quant_pivot_repository::traits::RecommendationReportRepository;

use crate::governance::RuntimeModeHandle;

use super::{
    builder::ReportBuilder,
    publisher::ReportPublisher,
    types::{BuildReportRequest, ReportTrigger},
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
    pub builder: Arc<dyn ReportBuilder>,
    pub publisher: Arc<ReportPublisher>,
    pub runtime_mode: RuntimeModeHandle,
}

/// End-to-end report lifecycle entry point.
pub struct ReportLifecycleService {
    report_repo: Arc<dyn RecommendationReportRepository>,
    builder: Arc<dyn ReportBuilder>,
    publisher: Arc<ReportPublisher>,
    runtime_mode: RuntimeModeHandle,
}

impl ReportLifecycleService {
    /// Build a lifecycle service.
    #[must_use]
    pub fn new(deps: ReportLifecycleDeps) -> Self {
        Self {
            report_repo: deps.report_repo,
            builder: deps.builder,
            publisher: deps.publisher,
            runtime_mode: deps.runtime_mode,
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
        Ok(report)
    }

    /// Expire a committed report in one repository transaction, then publish the
    /// lifecycle event after commit.
    pub async fn expire(
        &self,
        report_id: &RecommendationReportId,
        reason: &str,
        expired_at: DateTime<Utc>,
    ) -> QuantResult<RecommendationReportInfo> {
        let report = self
            .report_repo
            .expire(
                report_id,
                reason,
                expired_at,
                lifecycle_operation_log("expire", report_id, reason),
            )
            .await?;
        self.publisher.publish_expired(&report);
        Ok(report)
    }

    /// Expire all reports whose TTL has elapsed (`published_at + ttl <= now`),
    /// oldest first, up to `limit` per pass. Each expiry is one committed
    /// transaction (status + operation log) followed by a lifecycle event, so a
    /// single conflict (e.g. a concurrent revoke) is logged and skipped without
    /// aborting the sweep. Returns the number of reports expired.
    pub async fn expire_due_reports(
        &self,
        now: DateTime<Utc>,
        ttl_secs: u64,
        limit: u64,
    ) -> QuantResult<u32> {
        let ttl = i64::try_from(ttl_secs)
            .map_err(|error| QuantError::config(format!("report_ttl_secs too large: {error}")))?;
        let published_before = now - Duration::seconds(ttl);
        let due = self
            .report_repo
            .find_expirable(published_before, limit)
            .await?;

        let mut expired = 0_u32;
        for report_id in due {
            match self.expire(&report_id, "ttl_expired", now).await {
                Ok(_) => expired = expired.saturating_add(1),
                Err(error) => {
                    tracing::warn!(%report_id, %error, "report ttl expiry skipped");
                }
            }
        }
        Ok(expired)
    }

    async fn run(&self, request: BuildReportRequest) -> QuantResult<RecommendationReportInfo> {
        let trigger_key = request.trigger.key(request.trigger_time);
        if let Some(existing) = self.report_repo.find_by_trigger_key(&trigger_key).await? {
            return Ok(existing);
        }

        // Ephemeral start/fail signals correlate by `trigger_key` (no row yet).
        let runtime_mode = self.runtime_mode.current();
        let as_of = request.trigger_time;
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
        governance_audit_event_id: None,
        governance_audit_sequence: None,
    }
}
