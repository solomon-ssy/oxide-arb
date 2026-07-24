//! Report lifecycle service.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use quant_pivot_error::{
    QuantError, QuantResult,
    infra::InfraError,
    report::ReportError,
    storage::{
        StorageError, entity,
        entity::{QUANT_RECOMMENDATION_REPORT, QUANT_REPORT_RUN},
    },
};
use quant_pivot_models::{
    domain::{
        governance::NewOperationLog,
        quant::{
            EnqueueReportRunOutcome, NewReportRun, RecommendationReportInfo,
            ReportFactDeliveryInfo, ReportRunClaim, ReportRunInfo,
        },
    },
    enums::{
        operation_log::{OperationCategory, OperationHttpMethod, OperationOutcome},
        quant::{RecommendationReportStatus, ReportRunStatus, ReportTriggerKind},
        rbac::ResourceType,
    },
    types::{
        OperationDetailDocument, OperationLogId, RecommendationId, RecommendationReportId,
        ReportRunId, ReportTriggerKey,
    },
};
use quant_pivot_repository::traits::{
    RecommendationReportRepository, RecommendationRepository, ReportRunRepository,
};
use quant_pivot_research::artifact::ArtifactStore;

use super::{
    builder::ReportBuilder,
    fact_bundle::prepare_report_fact_bundle,
    publisher::ReportPublisher,
    types::{BuildReportRequest, ComposedReport, ReportTrigger, durable_report_error_summary},
};
use crate::{
    execution::IntentTerminalEventSink,
    service::feature_integrity::{FeatureParityGatePort, FeatureParityRunCoordinator},
};

/// Ad-hoc report trigger request.
#[derive(Debug, Clone)]
pub struct AdHocReportRequest {
    pub request_id: String,
    pub trigger_time: DateTime<Utc>,
    pub top_n: Option<u32>,
    pub knowledge_lag_secs: Option<u64>,
}

/// Operator-requested retry of one terminal ad-hoc run.
#[derive(Debug, Clone)]
pub struct RetryAdHocReportRequest {
    pub source_run_id: ReportRunId,
    pub request_id: String,
    pub requested_at: DateTime<Utc>,
}

/// Dependencies for [`ReportLifecycleService`].
pub struct ReportLifecycleDeps {
    pub report_repo: Arc<dyn RecommendationReportRepository>,
    pub run_repo: Arc<dyn ReportRunRepository>,
    pub recommendation_repo: Arc<dyn RecommendationRepository>,
    pub builder: Arc<dyn ReportBuilder>,
    pub publisher: Arc<ReportPublisher>,
    pub feature_parity_gate: Arc<dyn FeatureParityGatePort>,
    pub feature_parity_runs: Arc<FeatureParityRunCoordinator>,
    pub artifact_store: Arc<dyn ArtifactStore>,
    pub ad_hoc_queue_capacity: u64,
    pub ad_hoc_queue_ttl_secs: u64,
}

/// End-to-end report lifecycle entry point.
pub struct ReportLifecycleService {
    report_repo: Arc<dyn RecommendationReportRepository>,
    run_repo: Arc<dyn ReportRunRepository>,
    recommendation_repo: Arc<dyn RecommendationRepository>,
    builder: Arc<dyn ReportBuilder>,
    publisher: Arc<ReportPublisher>,
    feature_parity_gate: Arc<dyn FeatureParityGatePort>,
    feature_parity_runs: Arc<FeatureParityRunCoordinator>,
    artifact_store: Arc<dyn ArtifactStore>,
    ad_hoc_queue_capacity: u64,
    ad_hoc_queue_ttl_secs: u64,
}

impl ReportLifecycleService {
    /// Build a lifecycle service.
    #[must_use]
    pub fn new(deps: ReportLifecycleDeps) -> Self {
        Self {
            report_repo: deps.report_repo,
            run_repo: deps.run_repo,
            recommendation_repo: deps.recommendation_repo,
            builder: deps.builder,
            publisher: deps.publisher,
            feature_parity_gate: deps.feature_parity_gate,
            feature_parity_runs: deps.feature_parity_runs,
            artifact_store: deps.artifact_store,
            ad_hoc_queue_capacity: deps.ad_hoc_queue_capacity,
            ad_hoc_queue_ttl_secs: deps.ad_hoc_queue_ttl_secs,
        }
    }

    /// Install the order-intent terminal event sink (idempotent; first set wins).
    pub fn set_intent_event_sink(&self, sink: Arc<dyn IntentTerminalEventSink>) {
        self.publisher.set_intent_event_sink(sink);
    }

    /// Idempotently enqueue an ad-hoc report and return its durable run row.
    pub async fn run_ad_hoc(
        &self,
        request: AdHocReportRequest,
    ) -> QuantResult<EnqueueReportRunOutcome> {
        let request_id = request.request_id;
        let top_n = request
            .top_n
            .map(i32::try_from)
            .transpose()
            .map_err(|error| QuantError::config(format!("top_n exceeds i32: {error}")))?;
        let knowledge_lag_secs = request
            .knowledge_lag_secs
            .map(i64::try_from)
            .transpose()
            .map_err(|error| {
                QuantError::config(format!("knowledge_lag_secs exceeds i64: {error}"))
            })?;
        let run = NewReportRun {
            report_run_id: ReportRunId::from_v7(),
            trigger_kind: ReportTriggerKind::AdHoc,
            trigger_key: ReportTriggerKey::parse(format!("ad_hoc:{request_id}"))
                .map_err(|error| QuantError::config(error.to_string()))?,
            schedule_id: None,
            request_id: Some(request_id.into()),
            retry_of_run_id: None,
            scheduled_for: None,
            requested_at: request.trigger_time,
            status: ReportRunStatus::Queued,
            top_n,
            knowledge_lag_secs,
        };
        let outcome = self
            .run_repo
            .enqueue_ad_hoc(run, self.ad_hoc_queue_capacity, self.ad_hoc_queue_ttl_secs)
            .await?;
        if outcome.created() {
            self.publisher.publish_run(outcome.run(), Utc::now());
        }
        Ok(outcome)
    }

    /// Create a new queued attempt from one terminal ad-hoc run. The source row
    /// remains immutable and supplies exact retry lineage plus frozen overrides.
    pub async fn retry_ad_hoc(
        &self,
        request: RetryAdHocReportRequest,
    ) -> QuantResult<EnqueueReportRunOutcome> {
        let source = self
            .run_repo
            .find_by_id(&request.source_run_id)
            .await?
            .ok_or_else(|| StorageError::not_found(QUANT_REPORT_RUN, request.source_run_id))?;
        if source.trigger_kind != ReportTriggerKind::AdHoc
            || !matches!(
                source.status,
                ReportRunStatus::Failed | ReportRunStatus::Skipped | ReportRunStatus::Abandoned
            )
        {
            return Err(StorageError::state_conflict(
                QUANT_REPORT_RUN,
                Some(&request.source_run_id),
                "only failed, skipped, or abandoned ad-hoc runs can be retried",
            )
            .into());
        }
        let run = NewReportRun {
            report_run_id: ReportRunId::from_v7(),
            trigger_kind: ReportTriggerKind::AdHoc,
            trigger_key: ReportTriggerKey::parse(format!(
                "ad_hoc_retry:{}:{}",
                request.source_run_id, request.request_id
            ))
            .map_err(|error| QuantError::config(error.to_string()))?,
            schedule_id: None,
            request_id: Some(request.request_id.into()),
            retry_of_run_id: Some(request.source_run_id),
            scheduled_for: None,
            requested_at: request.requested_at,
            status: ReportRunStatus::Queued,
            top_n: source.top_n,
            knowledge_lag_secs: source.knowledge_lag_secs,
        };
        let outcome = self
            .run_repo
            .enqueue_ad_hoc(run, self.ad_hoc_queue_capacity, self.ad_hoc_queue_ttl_secs)
            .await?;
        if outcome.created() {
            self.publisher.publish_run(outcome.run(), Utc::now());
        }
        Ok(outcome)
    }

    /// Requeue a failed immutable fact bundle for verification/publication.
    pub async fn retry_publication(
        &self,
        report_id: &RecommendationReportId,
        occurred_at: DateTime<Utc>,
    ) -> QuantResult<ReportFactDeliveryInfo> {
        let delivery = self
            .report_repo
            .retry_fact_delivery(report_id, occurred_at)
            .await?;
        let report = self
            .report_repo
            .find_by_id(report_id)
            .await?
            .ok_or_else(|| StorageError::not_found(QUANT_RECOMMENDATION_REPORT, report_id))?;
        self.publisher.publish_delivery_state(&report, false);
        Ok(delivery)
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
                lifecycle_operation_log("revoke", report_id, reason)?,
            )
            .await?;
        self.publisher.publish_revoked(&report);
        self.publisher
            .publish_invalidated_intents(&invalidated_intents, revoked_at);
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
            .ok_or_else(|| StorageError::not_found(QUANT_RECOMMENDATION_REPORT, report_id))?;

        if !parity_is_contained(current.status) {
            match self
                .report_repo
                .revoke(
                    report_id,
                    reason,
                    occurred_at,
                    lifecycle_operation_log("parity_containment", report_id, reason)?,
                )
                .await
            {
                Ok((revoked, invalidated_intents)) => {
                    self.publisher.publish_revoked(&revoked);
                    self.publisher
                        .publish_invalidated_intents(&invalidated_intents, occurred_at);
                }
                Err(error) if revoke_transition_conflicts(&error, report_id) => {
                    let latest =
                        self.report_repo
                            .find_by_id(report_id)
                            .await?
                            .ok_or_else(|| {
                                StorageError::not_found(QUANT_RECOMMENDATION_REPORT, report_id)
                            })?;
                    if !parity_is_contained(latest.status) {
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
            let log = recommendation_operation_log(&recommendation_id, "ttl_expired")?;
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
                    self.publisher
                        .publish_invalidated_intents(&invalidated_intents, now);
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
        let log = match lifecycle_operation_log("expire", report_id, "ttl_expired") {
            Ok(log) => log,
            Err(error) => {
                tracing::error!(%error, %report_id, "report expiry audit detail is invalid");
                return false;
            }
        };
        match self
            .report_repo
            .roll_up_to_expired(report_id, now, log)
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

    /// Execute a previously claimed run under its exact lease CAS identity.
    pub async fn execute_claimed(
        &self,
        run: ReportRunInfo,
    ) -> QuantResult<RecommendationReportInfo> {
        let composed = match self.prepare_claimed(&run).await {
            Ok(composed) => composed,
            Err(error) => {
                self.fail_claimed_run(&run, &error).await;
                return Err(error);
            }
        };
        match self.commit_claimed(&run, composed).await {
            Ok(report) => Ok(report),
            Err(error) => {
                self.fail_claimed_run(&run, &error).await;
                Err(error)
            }
        }
    }

    /// Build and freeze a claimed run without touching the report/run commit.
    /// The coordinator keeps the lease alive while this future is in flight.
    pub(crate) async fn prepare_claimed(&self, run: &ReportRunInfo) -> QuantResult<ComposedReport> {
        let (request, _) = build_claimed_request(run)?;
        self.feature_parity_gate
            .ensure_clear("new report generation")
            .await?;
        let mut composed = self.builder.build(request).await?;
        let sampled_feature_parity = self
            .feature_parity_runs
            .build_report_sample(&composed.transaction.report, run)
            .await?;
        composed.transaction.sampled_feature_parity = Some(sampled_feature_parity);
        let parity_state = self
            .feature_parity_gate
            .commit_state_id("report commit")
            .await?;
        composed.transaction.feature_parity_state_id = Some(parity_state);
        prepare_report_fact_bundle(&self.artifact_store, &mut composed).await?;
        Ok(composed)
    }

    /// Commit one fully prepared artifact under the latest exact lease identity.
    pub(crate) async fn commit_claimed(
        &self,
        run: &ReportRunInfo,
        composed: ComposedReport,
    ) -> QuantResult<RecommendationReportInfo> {
        let (_, claim) = build_claimed_request(run)?;
        let prepared = self
            .report_repo
            .create_prepared_report(claim, composed.transaction)
            .await?;
        self.publisher.publish_prepared(&prepared.report);
        self.publisher.publish_run(&prepared.run, Utc::now());
        Ok(prepared.report)
    }

    pub(crate) async fn fail_claimed_run(&self, run: &ReportRunInfo, error: &QuantError) {
        let Some(worker_id) = run.lease_owner else {
            return;
        };
        let summary = durable_report_error_summary(error);
        match self
            .run_repo
            .fail_run(&run.report_run_id, worker_id, error.code(), &summary)
            .await
        {
            Ok(failed) => self.publisher.publish_run(&failed, Utc::now()),
            Err(fail_error) => tracing::warn!(
                run_id = %run.report_run_id,
                %fail_error,
                "report run failure could not be committed; lease recovery will reconcile it"
            ),
        }
    }
}

fn build_claimed_request(run: &ReportRunInfo) -> QuantResult<(BuildReportRequest, ReportRunClaim)> {
    if run.status != ReportRunStatus::Running {
        return Err(ReportError::ContractViolation {
            detail: format!("report run {} is not Running", run.report_run_id),
        }
        .into());
    }
    let decision_at = run
        .decision_at
        .ok_or_else(|| ReportError::InvariantViolation {
            stage: "report_run_claim",
            detail: "Running report run has no decision_at".to_owned(),
        })?;
    let lease_owner = run
        .lease_owner
        .ok_or_else(|| ReportError::InvariantViolation {
            stage: "report_run_claim",
            detail: "Running report run has no lease_owner".to_owned(),
        })?;
    let lease_expires_at = run
        .lease_expires_at
        .ok_or_else(|| ReportError::InvariantViolation {
            stage: "report_run_claim",
            detail: "Running report run has no lease_expires_at".to_owned(),
        })?;
    let top_n = run.top_n.ok_or_else(|| ReportError::InvariantViolation {
        stage: "report_run_claim",
        detail: "Running report run has no frozen top_n".to_owned(),
    })?;
    let knowledge_lag_secs =
        run.knowledge_lag_secs
            .ok_or_else(|| ReportError::InvariantViolation {
                stage: "report_run_claim",
                detail: "Running report run has no frozen knowledge_lag_secs".to_owned(),
            })?;
    let trigger = match run.trigger_kind {
        ReportTriggerKind::Scheduled => ReportTrigger::Scheduled {
            schedule_id: run.schedule_id.clone().ok_or_else(|| {
                ReportError::InvariantViolation {
                    stage: "report_run_claim",
                    detail: "scheduled report run has no schedule_id".to_owned(),
                }
            })?,
        },
        ReportTriggerKind::AdHoc => ReportTrigger::AdHoc {
            request_id: run
                .request_id
                .clone()
                .ok_or_else(|| ReportError::InvariantViolation {
                    stage: "report_run_claim",
                    detail: "ad-hoc report run has no request_id".to_owned(),
                })?,
        },
    };
    let request = BuildReportRequest {
        trigger,
        trigger_time: decision_at,
        top_n_override: Some(u32::try_from(top_n).map_err(|error| {
            ReportError::NumericOverflow {
                field: "report_run.top_n",
                detail: error.to_string(),
            }
        })?),
        knowledge_lag_secs_override: Some(u64::try_from(knowledge_lag_secs).map_err(|error| {
            ReportError::NumericOverflow {
                field: "report_run.knowledge_lag_secs",
                detail: error.to_string(),
            }
        })?),
    };
    Ok((
        request,
        ReportRunClaim {
            report_run_id: run.report_run_id,
            lease_owner,
            lease_expires_at,
        },
    ))
}

fn recommendation_operation_log(
    recommendation_id: &RecommendationId,
    reason: &str,
) -> QuantResult<NewOperationLog> {
    Ok(NewOperationLog {
        id: OperationLogId::from_v7(),
        request_id: format!("quant-recommendation:expire:{recommendation_id}").into(),
        actor_user_id: None,
        actor_username: Some("system".to_owned()),
        acting_role: Some("report_lifecycle".into()),
        category: OperationCategory::QuantReport,
        action: "recommendation.expire".into(),
        resource_type: Some(ResourceType::QuantReport),
        resource_id: Some(recommendation_id.to_string()),
        http_method: OperationHttpMethod::System,
        http_path: format!("/system/quant/recommendation/{recommendation_id}/expire"),
        http_status: 200,
        outcome: OperationOutcome::Success,
        client_ip: None,
        user_agent: None,
        latency_ms: 0,
        detail: operation_detail(reason)?,
        before_hash: None,
        after_hash: None,
        governance_audit_event_id: None,
        governance_audit_sequence: None,
    })
}

fn lifecycle_operation_log(
    action: &str,
    report_id: &RecommendationReportId,
    reason: &str,
) -> QuantResult<NewOperationLog> {
    Ok(NewOperationLog {
        id: OperationLogId::from_v7(),
        request_id: format!("quant-report:{action}:{report_id}").into(),
        actor_user_id: None,
        actor_username: Some("system".to_owned()),
        acting_role: Some("report_lifecycle".into()),
        category: OperationCategory::QuantReport,
        action: action.into(),
        resource_type: Some(ResourceType::QuantReport),
        resource_id: Some(report_id.to_string()),
        http_method: OperationHttpMethod::System,
        http_path: format!("/system/quant/report/{report_id}/{action}"),
        http_status: 200,
        outcome: OperationOutcome::Success,
        client_ip: None,
        user_agent: None,
        latency_ms: 0,
        detail: operation_detail(reason)?,
        before_hash: None,
        after_hash: None,
        governance_audit_event_id: None,
        governance_audit_sequence: None,
    })
}

fn operation_detail(reason: &str) -> QuantResult<OperationDetailDocument> {
    OperationDetailDocument::from_serializable(&serde_json::json!({ "reason": reason })).map_err(
        |error| {
            InfraError::AuditDetailInvalid {
                detail: error.to_string(),
            }
            .into()
        },
    )
}

const fn parity_is_contained(status: RecommendationReportStatus) -> bool {
    matches!(
        status,
        RecommendationReportStatus::Superseded
            | RecommendationReportStatus::Obsolete
            | RecommendationReportStatus::Revoked
            | RecommendationReportStatus::Expired
    )
}

fn revoke_transition_conflicts(error: &StorageError, report_id: &RecommendationReportId) -> bool {
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
    use entity::{QUANT_RECOMMENDATION, QUANT_RECOMMENDATION_REPORT};

    use super::*;

    #[test]
    fn only_terminal_report_revoke() {
        for status in [
            RecommendationReportStatus::Superseded,
            RecommendationReportStatus::Obsolete,
            RecommendationReportStatus::Revoked,
            RecommendationReportStatus::Expired,
        ] {
            assert!(parity_is_contained(status));
        }
        for status in [
            RecommendationReportStatus::Prepared,
            RecommendationReportStatus::Published,
        ] {
            assert!(!parity_is_contained(status));
        }
    }

    #[test]
    fn only_exact_report_classified() {
        let report_id = RecommendationReportId::from_v7();
        let exact = StorageError::illegal_transition(
            QUANT_RECOMMENDATION_REPORT,
            Some(&report_id),
            RecommendationReportStatus::Expired.as_str(),
            RecommendationReportStatus::Revoked.as_str(),
        );
        assert!(revoke_transition_conflicts(&exact, &report_id));

        let unrelated_report = StorageError::illegal_transition(
            QUANT_RECOMMENDATION_REPORT,
            Some(&RecommendationReportId::from_v7()),
            RecommendationReportStatus::Expired.as_str(),
            RecommendationReportStatus::Revoked.as_str(),
        );
        assert!(!revoke_transition_conflicts(&unrelated_report, &report_id));
        let unrelated_entity = StorageError::illegal_transition(
            QUANT_RECOMMENDATION,
            Some(&report_id),
            "attributed",
            "revoked",
        );
        assert!(!revoke_transition_conflicts(&unrelated_entity, &report_id));
        assert!(!revoke_transition_conflicts(
            &StorageError::Connection("database unavailable".to_owned()),
            &report_id
        ));
    }
}
