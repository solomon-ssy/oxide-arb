//! Governed order-intent service.
//!
//! The create / approve / reject / cancel / expire / report-cascade closure that
//! turns a published recommendation into a capital-reserving `OrderIntent` and
//! drives its approval lifecycle.
//!
//! Money invariant: every state transition is atomic over the intent FSM and the
//! capital FSM in one repository transaction (see [`OrderIntentRepository`]).
//! HTTP-origin mutations are audited by the web middleware; background-origin
//! transitions (`expire`, `invalidate`) carry their own operation-log row inside
//! the transaction. Non-authoritative WebSocket lifecycle events are published
//! only after the transaction commits.

use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Duration, FixedOffset, Utc};
use quant_pivot_error::{
    QuantError, QuantResult, execution::ExecutionError, infra::InfraError, storage::StorageError,
};
use quant_pivot_models::{
    domain::{
        ApproveIntentCommand, ApproveOrderIntent, ApproveOrderIntentOutcome, CancelIntentCommand,
        CreateIntentCommand, IntentCreationLimits, IntentEventKind, NewCapitalAllocation,
        NewOperationLog, NewOrderIntent, OrderIntentInfo, OrderIntentListQuery, OrderIntentPort,
        Paginated, RecommendationInfo, RecommendationReportInfo, RejectIntentCommand,
    },
    enums::{
        common::{OrderType, Side},
        execution::{CapitalAllocationState, OrderIntentKind},
        operation_log::{OperationCategory, OperationHttpMethod, OperationOutcome},
        quant::{
            ApprovalStatus, FillRequirement, OrderIntentStatus, QuantRuntimeMode,
            RecommendationReportStatus, ReportFactDeliveryStatus,
        },
        rbac::ResourceType,
    },
    types::{
        CapitalAllocationId, ContentHash, EntryConditionInstanceId, EntryOrderPolicy,
        EntryOrderSpec, ExitPolicySpec, ModelVersionId, OperationDetailDocument, OperationLogId,
        OrderAmount, OrderIntentId, Price, RecommendationId, RecommendationReportId,
        RecommendationTradePlan, ResearchProfileRef, TradePolicyArtifactId, Usd,
    },
};
use quant_pivot_repository::traits::{
    EntryConditionRepository, ModelRegistryRepository, OrderIntentRepository,
    RecommendationReportRepository, RecommendationRepository, TradePolicyRepository,
};
use quant_pivot_research::{
    artifact::ArtifactStore,
    model::{CalibrationArtifactLoader, load_hash_verified_artifact},
};

use crate::{
    execution::{
        dispatch_wake::DispatchWake,
        intent_lifecycle::IntentLifecyclePublisher,
        mode_gate::{IntentPolicyDecision, RuntimeModeGate},
        trade_policy_guard::require_frozen_trade_policy,
    },
    governance::{KillSwitchHandle, RuntimeModeHandle, resolve_return_model_calibration},
    observability::metrics_hub::MetricsHub,
    runtime_config::DecisionPolicyStore,
    service::feature_integrity::FeatureParityGatePort,
};
/// Post-commit event sink for terminally invalidated intents.
///
/// Persistence and capital release have already committed; this dependency only
/// fans lifecycle notifications out to in-process readers.
pub trait IntentTerminalEventSink: Send + Sync {
    fn publish_invalidated(&self, intents: &[OrderIntentInfo], now: DateTime<Utc>);
}

/// Dependencies for [`CoreOrderIntentService`].
pub struct OrderIntentServiceDeps {
    pub mode_gate: Arc<dyn RuntimeModeGate>,
    pub runtime_mode: RuntimeModeHandle,
    pub runtime_config: Arc<DecisionPolicyStore>,
    pub kill_switch: KillSwitchHandle,
    pub recommendations: Arc<dyn RecommendationRepository>,
    pub reports: Arc<dyn RecommendationReportRepository>,
    pub intents: Arc<dyn OrderIntentRepository>,
    pub conditions: Arc<dyn EntryConditionRepository>,
    pub metrics: Arc<MetricsHub>,
    /// Shared `quant.intent` lifecycle fan-out (bootstrap singleton).
    pub intent_lifecycle: Arc<IntentLifecyclePublisher>,
    /// Wake the dispatcher when an `ApprovedByPolicy` intent is created (auto).
    pub dispatch_wake: DispatchWake,
    /// Model registry (calibration-state recheck at `SemiAuto`/`AutoExecution`
    /// intent-creation time — Phase 11.3 closed-loop hardening).
    pub model_registry: Arc<dyn ModelRegistryRepository>,
    /// Governance source used to re-verify the frozen policy at intent creation.
    pub trade_policies: Arc<dyn TradePolicyRepository>,
    /// Content-addressed model artifact store (calibration recheck).
    pub artifact_store: Arc<dyn ArtifactStore>,
    /// Deep calibrator resolution (calibration recheck), the same shared
    /// check publish / report / admission use.
    pub calibration_loader: Arc<dyn CalibrationArtifactLoader>,
    /// Global training-serving parity latch. Existing exits and settlement do
    /// not enter this service; only risk-increasing intent creation is blocked.
    pub feature_parity_gate: Arc<dyn FeatureParityGatePort>,
}

/// Governed order-intent service (implements the web [`OrderIntentPort`], the
/// report terminal-event [`IntentTerminalEventSink`], and the sweep `expire_due`).
pub struct CoreOrderIntentService {
    mode_gate: Arc<dyn RuntimeModeGate>,
    runtime_mode: RuntimeModeHandle,
    runtime_config: Arc<DecisionPolicyStore>,
    kill_switch: KillSwitchHandle,
    recommendations: Arc<dyn RecommendationRepository>,
    reports: Arc<dyn RecommendationReportRepository>,
    intents: Arc<dyn OrderIntentRepository>,
    conditions: Arc<dyn EntryConditionRepository>,
    metrics: Arc<MetricsHub>,
    lifecycle: Arc<IntentLifecyclePublisher>,
    dispatch_wake: DispatchWake,
    model_registry: Arc<dyn ModelRegistryRepository>,
    trade_policies: Arc<dyn TradePolicyRepository>,
    artifact_store: Arc<dyn ArtifactStore>,
    calibration_loader: Arc<dyn CalibrationArtifactLoader>,
    feature_parity_gate: Arc<dyn FeatureParityGatePort>,
}

struct SemiAutoCanaryAuthorization {
    policy_id: TradePolicyArtifactId,
    policy_hash: ContentHash,
    expires_at: DateTime<Utc>,
    limits: IntentCreationLimits,
}

fn malformed_canary(detail: impl Display) -> QuantError {
    QuantError::config(format!("active SemiAuto canary is malformed: {detail}"))
}

impl CoreOrderIntentService {
    /// Assemble the service from its dependencies.
    #[must_use]
    pub fn new(deps: OrderIntentServiceDeps) -> Self {
        Self {
            mode_gate: deps.mode_gate,
            runtime_mode: deps.runtime_mode,
            runtime_config: deps.runtime_config,
            kill_switch: deps.kill_switch,
            recommendations: deps.recommendations,
            reports: deps.reports,
            intents: deps.intents,
            conditions: deps.conditions,
            metrics: deps.metrics,
            lifecycle: deps.intent_lifecycle,
            dispatch_wake: deps.dispatch_wake,
            model_registry: deps.model_registry,
            trade_policies: deps.trade_policies,
            artifact_store: deps.artifact_store,
            calibration_loader: deps.calibration_loader,
            feature_parity_gate: deps.feature_parity_gate,
        }
    }

    /// Re-verify the bound model version's return-model calibration state at
    /// intent-creation time — not just at report-build time — closing the
    /// TOCTOU window between report generation and intent creation: a
    /// calibrator deactivated after a report was built must not let a stale,
    /// frozen `execution_eligibility` still create a capital-reserving
    /// `SemiAuto`/`AutoExecution` intent (Phase 11.3 closed-loop hardening;
    /// the same [`resolve_return_model_calibration`] deep check publish /
    /// report / admission share). A free function (not `&self`) so it is
    /// directly unit-testable against fakes for just its three dependencies.
    async fn ensure_return_model_still_calibrated(
        &self,
        model_version_id: &ModelVersionId,
    ) -> QuantResult<()> {
        recheck_return_model_calibrated(
            &self.model_registry,
            &self.artifact_store,
            &self.calibration_loader,
            model_version_id,
        )
        .await
    }

    async fn ensure_trade_policy_still_executable(
        &self,
        recommendation: &RecommendationInfo,
    ) -> QuantResult<ResearchProfileRef> {
        let version = self
            .model_registry
            .find_model_version_by_id(&recommendation.evidence_refs.model_version_id)
            .await?
            .ok_or_else(|| ExecutionError::IntentDenied {
                reason: "recommendation model version no longer exists".to_owned(),
            })?;
        require_frozen_trade_policy(self.trade_policies.as_ref(), &version, recommendation).await
    }

    fn require_semi_auto_canary(
        &self,
        recommendation: &RecommendationInfo,
        report: &RecommendationReportInfo,
        now: DateTime<Utc>,
    ) -> QuantResult<SemiAutoCanaryAuthorization> {
        let config = self.runtime_config.current();
        let canary = &config.execution_authorization.semi_auto.canary;
        if !canary.enabled {
            return Err(ExecutionError::IntentDenied {
                reason: "SemiAuto canary is not enabled".to_owned(),
            }
            .into());
        }
        let policy_id = canary
            .policy_artifact_id
            .as_deref()
            .ok_or_else(|| malformed_canary("policy_artifact_id is missing"))?
            .parse::<TradePolicyArtifactId>()
            .map_err(|error| malformed_canary(format!("policy_artifact_id is invalid: {error}")))?;
        let policy_hash = canary
            .policy_content_hash
            .as_deref()
            .ok_or_else(|| malformed_canary("policy_content_hash is missing"))?
            .parse::<ContentHash>()
            .map_err(|error| {
                malformed_canary(format!("policy_content_hash is invalid: {error}"))
            })?;
        let expires_at = canary
            .expires_at
            .as_deref()
            .ok_or_else(|| malformed_canary("expires_at is missing"))?
            .parse::<DateTime<FixedOffset>>()
            .map_err(|error| malformed_canary(format!("expires_at is invalid: {error}")))?
            .with_timezone(&Utc);
        if expires_at <= now {
            return Err(ExecutionError::IntentDenied {
                reason: format!("SemiAuto canary expired at {expires_at}"),
            }
            .into());
        }
        let RecommendationTradePlan::Frozen { policy, sizing, .. } = &recommendation.trade_plan
        else {
            return Err(ExecutionError::IntentDenied {
                reason: "recommendation has no frozen trade policy".to_owned(),
            }
            .into());
        };
        if policy.artifact_id != policy_id || policy.artifact_hash != policy_hash {
            return Err(ExecutionError::IntentDenied {
                reason:
                    "recommendation policy identity is not authorized by the active SemiAuto canary"
                        .to_owned(),
            }
            .into());
        }
        let allowed_tier = canary
            .allowed_cash_budget_tiers_usd
            .iter()
            .any(|tier| Usd::new(tier.value) == sizing.suggested_usd);
        if !allowed_tier {
            return Err(ExecutionError::IntentDenied {
                reason: format!(
                    "recommendation tier {} is not authorized by the active SemiAuto canary",
                    sizing.suggested_usd
                ),
            }
            .into());
        }
        let max_total_cash_per_report = Usd::new(canary.max_total_cash_per_report.value);
        if canary.max_open_intents == 0 || !max_total_cash_per_report.is_positive() {
            return Err(malformed_canary(
                "open-intent and report cash-budget limits must be positive",
            ));
        }
        Ok(SemiAutoCanaryAuthorization {
            policy_id,
            policy_hash,
            expires_at,
            limits: IntentCreationLimits {
                recommendation_report_id: report.recommendation_report_id.clone(),
                max_open_intents: canary.max_open_intents,
                max_total_cash_per_report,
            },
        })
    }

    /// Create an intent from a recommendation at `now` (mode-gated, atomic with
    /// the capital reservation).
    pub async fn create_at(
        &self,
        command: CreateIntentCommand,
        now: DateTime<Utc>,
    ) -> QuantResult<OrderIntentInfo> {
        self.feature_parity_gate
            .ensure_clear("new entry intent creation")
            .await?;
        let rec = self.load_recommendation(&command.recommendation_id).await?;
        let report = self.load_report(&rec.recommendation_report_id).await?;
        self.ensure_report_facts_verified(&rec.recommendation_report_id)
            .await?;
        self.ensure_create_allowed(&rec, &report, now).await?;

        let mode = self.runtime_mode.current();
        let profile_ref = if matches!(
            mode,
            QuantRuntimeMode::SemiAuto | QuantRuntimeMode::AutoExecution
        ) {
            let profile_ref = self.ensure_trade_policy_still_executable(&rec).await?;
            self.ensure_return_model_still_calibrated(&rec.evidence_refs.model_version_id)
                .await?;
            profile_ref
        } else {
            self.ensure_trade_policy_still_executable(&rec).await?
        };
        let canary = if mode == QuantRuntimeMode::SemiAuto {
            Some(self.require_semi_auto_canary(&rec, &report, now)?)
        } else {
            None
        };
        let policy = self.mode_gate.evaluate_intent_policy(mode, &rec).await?;
        let entry = project_entry_order_spec(&rec, now)?;
        let exit = project_exit_policy_spec(&rec, entry.limit_price)?;
        let mut resolved = resolve_policy(policy, now, rec.valid_until, entry.valid_until)?;
        if let Some(canary) = canary.as_ref() {
            resolved.expires_at = resolved.expires_at.min(canary.expires_at);
            resolved.policy_id = Some(canary.policy_id.to_string());
            resolved.policy_hash = Some(canary.policy_hash.clone());
        }
        let condition = self
            .conditions
            .find_by_recommendation(&rec.recommendation_id)
            .await?
            .ok_or_else(|| ExecutionError::IntentDenied {
                reason: "recommendation has no entry-condition instance".to_owned(),
            })?;
        let (new_intent, allocation) = compose_create_rows(ComposeCreateRowsInput {
            recommendation: &rec,
            report: &report,
            profile_ref,
            mode,
            resolved,
            entry,
            exit,
            condition_instance_id: condition.condition_instance_id,
        })?;

        let intent = self
            .intents
            .create_with_allocation(new_intent, allocation, canary.map(|value| value.limits))
            .await?;
        self.metrics
            .inc_order_intent_created(intent.runtime_mode.as_str(), intent.intent_kind.as_str());
        self.publish(&intent, IntentEventKind::Created, now);
        if intent.status == OrderIntentStatus::ApprovedByPolicy {
            self.metrics.inc_order_intent_approved(
                intent.runtime_mode.as_str(),
                intent.intent_kind.as_str(),
            );
            self.dispatch_wake.wake();
        }
        Ok(intent)
    }

    async fn ensure_create_allowed(
        &self,
        rec: &RecommendationInfo,
        report: &RecommendationReportInfo,
        now: DateTime<Utc>,
    ) -> QuantResult<()> {
        if rec.valid_until < now {
            return Err(ExecutionError::RecommendationExpired {
                reason: format!("recommendation {} expired", rec.recommendation_id),
            }
            .into());
        }
        if !rec.status.allows_new_intent() {
            return Err(ExecutionError::IntentDenied {
                reason: format!(
                    "recommendation {} is {} (not actionable for intent creation)",
                    rec.recommendation_id,
                    rec.status.as_str()
                ),
            }
            .into());
        }
        if report.status != RecommendationReportStatus::Published {
            return Err(ExecutionError::IntentDenied {
                reason: format!(
                    "source report {} is {}",
                    report.recommendation_report_id,
                    report.status.as_str()
                ),
            }
            .into());
        }
        if rec.profile_ref != report.profile_ref {
            return Err(ExecutionError::IntentDenied {
                reason: "recommendation research profile does not match its report".to_owned(),
            }
            .into());
        }
        if !self.kill_switch.allows_new_entry() {
            return Err(ExecutionError::KillSwitchBlocks {
                state: self.kill_switch.current().as_str().to_owned(),
                operation: "intent creation".to_owned(),
            }
            .into());
        }
        if self
            .intents
            .find_active_by_recommendation(&rec.recommendation_id)
            .await?
            .is_some()
        {
            return Err(ExecutionError::IntentDenied {
                reason: format!(
                    "blocking order intent already exists for recommendation {}",
                    rec.recommendation_id
                ),
            }
            .into());
        }
        Ok(())
    }

    /// Approve a pending intent at `now`: in-transaction invalidation re-check
    /// (fail closed), apply any operator downscale, transition to `Approved`.
    pub async fn approve_at(
        &self,
        command: ApproveIntentCommand,
        now: DateTime<Utc>,
    ) -> QuantResult<OrderIntentInfo> {
        let intent = self.load_intent(&command.order_intent_id).await?;
        if intent.status != OrderIntentStatus::PendingApproval {
            return Err(ExecutionError::IntentDenied {
                reason: format!(
                    "intent {} is {} (only pending_approval can be approved)",
                    intent.order_intent_id,
                    intent.status.as_str()
                ),
            }
            .into());
        }
        if intent.runtime_mode == QuantRuntimeMode::SemiAuto {
            let recommendation = self.load_recommendation(&intent.recommendation_id).await?;
            let report = self
                .load_report(&recommendation.recommendation_report_id)
                .await?;
            self.ensure_report_facts_verified(&recommendation.recommendation_report_id)
                .await?;
            self.ensure_trade_policy_still_executable(&recommendation)
                .await?;
            self.ensure_return_model_still_calibrated(
                &recommendation.evidence_refs.model_version_id,
            )
            .await?;
            let canary = self.require_semi_auto_canary(&recommendation, &report, now)?;
            let canary_policy_id = canary.policy_id.to_string();
            if intent.policy_id.as_deref() != Some(canary_policy_id.as_str())
                || intent.policy_hash.as_ref() != Some(&canary.policy_hash)
            {
                return Err(ExecutionError::IntentDenied {
                    reason: "pending intent is not bound to the active SemiAuto canary identity"
                        .to_owned(),
                }
                .into());
            }
        }

        let (entry_override, allocated_override) =
            resolve_downscale(&command, &intent.entry_order_json)?;
        let approval = ApproveOrderIntent {
            approved_by: command.operator_id.clone(),
            approval_reason: approval_reason(&command),
            approved_at: now,
        };
        match self
            .intents
            .approve(
                &intent.order_intent_id,
                approval,
                entry_override,
                allocated_override,
                now,
            )
            .await?
        {
            ApproveOrderIntentOutcome::Approved(approved) => {
                self.metrics.inc_order_intent_approved(
                    approved.runtime_mode.as_str(),
                    approved.intent_kind.as_str(),
                );
                self.publish(&approved, IntentEventKind::Approved, now);
                // Approval is the operator's final authorization: it arms the
                // durable trigger worker and may result in a later submission.
                self.dispatch_wake.wake();
                Ok(approved)
            }
            ApproveOrderIntentOutcome::Invalidated(invalidated, reason) => {
                self.publish(&invalidated, IntentEventKind::Invalidated, now);
                Err(ExecutionError::ApprovalInvalidated {
                    reason: reason.as_str().to_owned(),
                }
                .into())
            }
        }
    }

    /// Reject a pending intent at `now`, releasing its capital.
    pub async fn reject_at(
        &self,
        command: RejectIntentCommand,
        now: DateTime<Utc>,
    ) -> QuantResult<OrderIntentInfo> {
        let current = self.load_intent(&command.order_intent_id).await?;
        let log = intent_operation_log("quant.intent.reject", &current, &command.reason)?;
        let rejected = self
            .intents
            .reject(&command.order_intent_id, command.reason, now, log)
            .await?;
        self.metrics.inc_order_intent_rejected(
            rejected.runtime_mode.as_str(),
            rejected.intent_kind.as_str(),
        );
        self.publish(&rejected, IntentEventKind::Rejected, now);
        Ok(rejected)
    }

    /// Cancel a not-yet-submitted intent at `now`, releasing its capital.
    pub async fn cancel_at(
        &self,
        command: CancelIntentCommand,
        now: DateTime<Utc>,
    ) -> QuantResult<OrderIntentInfo> {
        let current = self.load_intent(&command.order_intent_id).await?;
        let log = intent_operation_log("quant.intent.cancel", &current, &command.reason)?;
        let cancelled = self
            .intents
            .cancel(&command.order_intent_id, command.reason, now, log)
            .await?;
        self.publish(&cancelled, IntentEventKind::Cancelled, now);
        Ok(cancelled)
    }

    /// Expire every intent past its `expires_at`, releasing capital. Each expiry
    /// is its own committed transaction; a conflict is logged and skipped.
    /// Returns the number expired.
    pub async fn expire_due(&self, now: DateTime<Utc>, limit: usize) -> QuantResult<u32> {
        let due = self.intents.find_expired(now).await?;
        let mut expired = 0_u32;
        for intent in due.into_iter().take(limit) {
            let log = intent_operation_log("quant.intent.expire", &intent, "ttl_expired")?;
            match self.intents.expire(&intent.order_intent_id, now, log).await {
                Ok(updated) => {
                    self.publish(&updated, IntentEventKind::Expired, now);
                    expired = expired.saturating_add(1);
                }
                Err(error) => {
                    tracing::warn!(intent_id = %intent.order_intent_id, %error, "intent expiry skipped");
                }
            }
        }
        Ok(expired)
    }

    async fn load_recommendation(
        &self,
        recommendation_id: &RecommendationId,
    ) -> QuantResult<RecommendationInfo> {
        self.recommendations
            .find_by_id(recommendation_id)
            .await?
            .ok_or_else(|| {
                StorageError::NotFound {
                    entity: "recommendation",
                    id: recommendation_id.to_string(),
                }
                .into()
            })
    }

    async fn load_report(
        &self,
        report_id: &RecommendationReportId,
    ) -> QuantResult<RecommendationReportInfo> {
        self.reports.find_by_id(report_id).await?.ok_or_else(|| {
            StorageError::NotFound {
                entity: "recommendation_report",
                id: report_id.to_string(),
            }
            .into()
        })
    }

    async fn ensure_report_facts_verified(
        &self,
        report_id: &RecommendationReportId,
    ) -> QuantResult<()> {
        let delivery = self.reports.find_fact_delivery(report_id).await?;
        if delivery.as_ref().map(|row| row.status) != Some(ReportFactDeliveryStatus::Verified) {
            return Err(ExecutionError::IntentDenied {
                reason: format!("report {report_id} facts are not verified"),
            }
            .into());
        }
        Ok(())
    }

    async fn load_intent(&self, intent_id: &OrderIntentId) -> QuantResult<OrderIntentInfo> {
        self.intents.find_by_id(intent_id).await?.ok_or_else(|| {
            StorageError::NotFound {
                entity: "order_intent",
                id: intent_id.to_string(),
            }
            .into()
        })
    }

    fn publish(&self, intent: &OrderIntentInfo, event: IntentEventKind, now: DateTime<Utc>) {
        self.lifecycle.publish(intent, event, now);
    }
}

#[async_trait]
impl OrderIntentPort for CoreOrderIntentService {
    async fn create(&self, command: CreateIntentCommand) -> QuantResult<OrderIntentInfo> {
        self.create_at(command, Utc::now()).await
    }

    async fn approve(&self, command: ApproveIntentCommand) -> QuantResult<OrderIntentInfo> {
        self.approve_at(command, Utc::now()).await
    }

    async fn reject(&self, command: RejectIntentCommand) -> QuantResult<OrderIntentInfo> {
        self.reject_at(command, Utc::now()).await
    }

    async fn cancel(&self, command: CancelIntentCommand) -> QuantResult<OrderIntentInfo> {
        self.cancel_at(command, Utc::now()).await
    }

    async fn list(&self, query: OrderIntentListQuery) -> QuantResult<Paginated<OrderIntentInfo>> {
        Ok(self.intents.page(query).await?)
    }

    async fn find(&self, id: &OrderIntentId) -> QuantResult<Option<OrderIntentInfo>> {
        Ok(self.intents.find_by_id(id).await?)
    }
}

impl IntentTerminalEventSink for CoreOrderIntentService {
    fn publish_invalidated(&self, intents: &[OrderIntentInfo], now: DateTime<Utc>) {
        for intent in intents {
            self.publish(intent, IntentEventKind::Invalidated, now);
        }
    }
}

/// The Phase 11.3 closed-loop calibration recheck (see
/// [`CoreOrderIntentService::ensure_return_model_still_calibrated`]), extracted
/// as a free function over its three dependencies so it is unit-testable
/// without constructing a full [`CoreOrderIntentService`].
///
/// # Errors
///
/// Returns [`ExecutionError::IntentDenied`] when the model version is missing
/// or its return model is `Heuristic` (uncalibrated); propagates
/// [`resolve_return_model_calibration`]'s fail-closed error when a
/// `Calibrated` return model's bound calibrator is missing / inactive /
/// hash-mismatched.
async fn recheck_return_model_calibrated(
    model_registry: &Arc<dyn ModelRegistryRepository>,
    artifact_store: &Arc<dyn ArtifactStore>,
    calibration_loader: &Arc<dyn CalibrationArtifactLoader>,
    model_version_id: &ModelVersionId,
) -> QuantResult<()> {
    let version = model_registry
        .find_model_version_by_id(model_version_id)
        .await?
        .ok_or_else(|| ExecutionError::IntentDenied {
            reason: format!("model version {model_version_id} not found for calibration recheck"),
        })?;
    let artifact = load_hash_verified_artifact(artifact_store, &version).await?;
    let resolved = resolve_return_model_calibration(calibration_loader.as_ref(), &artifact).await?;
    if resolved.is_none() {
        return Err(ExecutionError::IntentDenied {
            reason: "return model is uncalibrated (heuristic) — SemiAuto/AutoExecution intent \
                     creation is fail-closed"
                .to_owned(),
        }
        .into());
    }
    Ok(())
}

/// Resolved per-mode intent fields produced by the mode gate decision.
struct ResolvedPolicy {
    status: OrderIntentStatus,
    approval_status: ApprovalStatus,
    approval_reason: Option<String>,
    approved_at: Option<DateTime<Utc>>,
    policy_id: Option<String>,
    policy_hash: Option<ContentHash>,
    expires_at: DateTime<Utc>,
}

struct ComposeCreateRowsInput<'a> {
    recommendation: &'a RecommendationInfo,
    report: &'a RecommendationReportInfo,
    profile_ref: ResearchProfileRef,
    mode: QuantRuntimeMode,
    resolved: ResolvedPolicy,
    entry: EntryOrderSpec,
    exit: ExitPolicySpec,
    condition_instance_id: EntryConditionInstanceId,
}

fn compose_create_rows(
    input: ComposeCreateRowsInput<'_>,
) -> Result<(NewOrderIntent, NewCapitalAllocation), ExecutionError> {
    let ComposeCreateRowsInput {
        recommendation: rec,
        report,
        profile_ref,
        mode,
        resolved,
        entry,
        exit,
        condition_instance_id,
    } = input;
    let intent_id = OrderIntentId::from_v7();
    let (_, _, _, _, risk_envelope) =
        rec.trade_plan
            .frozen()
            .ok_or_else(|| ExecutionError::IntentDenied {
                reason: "recommendation trade plan is unavailable".to_owned(),
            })?;
    let planned_usd = entry.notional();
    let intent = NewOrderIntent {
        order_intent_id: intent_id.clone(),
        recommendation_id: rec.recommendation_id.clone(),
        runtime_mode: mode,
        decision_policy_snapshot_id: report.decision_policy_snapshot_id.clone(),
        model_version_id: report.model_version_id.clone(),
        research_profile_artifact_id: profile_ref.artifact_id(),
        intent_kind: OrderIntentKind::Buy,
        status: resolved.status,
        approval_status: resolved.approval_status,
        approved_by: None,
        approval_reason: resolved.approval_reason,
        approved_at: resolved.approved_at,
        policy_id: resolved.policy_id,
        policy_hash: resolved.policy_hash,
        status_reason: None,
        admission_trace_ref: None,
        condition_instance_id,
        entry_order_json: entry,
        exit_policy_json: exit,
        risk_envelope_hash: risk_envelope.envelope_hash.clone(),
        expires_at: resolved.expires_at,
    };
    let allocation = NewCapitalAllocation {
        capital_allocation_id: CapitalAllocationId::from_v7(),
        order_intent_id: intent_id,
        recommendation_id: rec.recommendation_id.clone(),
        state: CapitalAllocationState::Allocated,
        planned_usd,
        allocated_usd: planned_usd,
        locked_usd: Usd::ZERO,
        spent_usd: Usd::ZERO,
        released_usd: Usd::ZERO,
        reason: "intent created".to_owned(),
    };
    Ok((intent, allocation))
}

/// Map a mode-gate decision to the frozen intent fields, or the closing error.
///
/// `report_only` and `denied` never produce an intent. `semi_auto` expires at
/// `min(now + approval_ttl, recommendation.valid_until, entry.valid_until)` so
/// capital is not reserved past the recommendation or entry window. Auto policy
/// intents expire at `min(entry.valid_until, recommendation.valid_until)`.
fn resolve_policy(
    policy: IntentPolicyDecision,
    now: DateTime<Utc>,
    recommendation_valid_until: DateTime<Utc>,
    entry_valid_until: DateTime<Utc>,
) -> Result<ResolvedPolicy, ExecutionError> {
    let ensure_future_expiry =
        |expires_at: DateTime<Utc>| -> Result<DateTime<Utc>, ExecutionError> {
            if expires_at <= now {
                return Err(ExecutionError::RecommendationExpired {
                    reason: "recommendation or entry window already elapsed".to_owned(),
                });
            }
            Ok(expires_at)
        };

    match policy {
        IntentPolicyDecision::ReportOnly => Err(ExecutionError::ReportOnlyMode),
        IntentPolicyDecision::Denied { reason } => Err(ExecutionError::IntentDenied {
            reason: reason.as_str().to_owned(),
        }),
        IntentPolicyDecision::RequiresApproval { approval_ttl, .. } => {
            let ttl_secs = i64::try_from(approval_ttl.as_secs()).map_err(|error| {
                execution_time_conversion("approval_ttl_secs", &approval_ttl.as_secs(), &error)
            })?;
            let approval_deadline = now
                .checked_add_signed(Duration::seconds(ttl_secs))
                .ok_or_else(|| ExecutionError::TimeConversion {
                    field: "approval_deadline",
                    value: format!("{now}+{ttl_secs}s"),
                    detail: "deadline is outside the chrono range".to_owned(),
                })?;
            let expires_at = ensure_future_expiry(
                approval_deadline
                    .min(recommendation_valid_until)
                    .min(entry_valid_until),
            )?;
            Ok(ResolvedPolicy {
                status: OrderIntentStatus::PendingApproval,
                approval_status: ApprovalStatus::Pending,
                approval_reason: None,
                approved_at: None,
                policy_id: None,
                policy_hash: None,
                expires_at,
            })
        }
        IntentPolicyDecision::ApprovedByPolicy {
            policy_id,
            policy_hash,
            reason,
        } => {
            let expires_at =
                ensure_future_expiry(entry_valid_until.min(recommendation_valid_until))?;
            Ok(ResolvedPolicy {
                status: OrderIntentStatus::ApprovedByPolicy,
                approval_status: ApprovalStatus::NotRequired,
                approval_reason: Some(reason),
                approved_at: Some(now),
                policy_id: Some(policy_id),
                policy_hash,
                expires_at,
            })
        }
    }
}

/// Project a recommendation's `EntryPlan` + `SizingPlan` into the executable
/// [`EntryOrderSpec`] frozen on the intent.
fn project_entry_order_spec(
    rec: &RecommendationInfo,
    now: DateTime<Utc>,
) -> Result<EntryOrderSpec, ExecutionError> {
    const GTD_MIN_EFFECTIVE_LIFETIME_SECS: i64 = 120;
    const GTD_SECURITY_BUFFER_SECS: i64 = 60;
    let Some((_, entry_plan, sizing, _, _)) = rec.trade_plan.frozen() else {
        return Err(ExecutionError::IntentDenied {
            reason: "recommendation trade plan is unavailable".to_owned(),
        });
    };
    let (limit_price, order_type, amount, post_only) = match entry_plan.order_policy {
        EntryOrderPolicy::Aggressive {
            worst_price,
            fill_requirement,
        } => {
            let order_type = match fill_requirement {
                FillRequirement::AllOrNothing => OrderType::Fok,
                FillRequirement::AllowPartial => OrderType::Fak,
            };
            (
                worst_price,
                order_type,
                OrderAmount::CashBudget(sizing.suggested_usd),
                false,
            )
        }
        EntryOrderPolicy::Passive {
            limit_price,
            post_only,
        } => {
            if !post_only {
                return Err(ExecutionError::IntentDenied {
                    reason: "passive entry policy must be post-only".to_owned(),
                });
            }
            if entry_plan.valid_until - now < Duration::seconds(GTD_MIN_EFFECTIVE_LIFETIME_SECS) {
                return Err(ExecutionError::IntentDenied {
                    reason: "passive entry has less than 120 seconds of effective GTD lifetime"
                        .to_owned(),
                });
            }
            let wire_expiration = entry_plan
                .valid_until
                .checked_add_signed(Duration::seconds(GTD_SECURITY_BUFFER_SECS))
                .ok_or_else(|| ExecutionError::TimeConversion {
                    field: "entry_plan.valid_until",
                    value: entry_plan.valid_until.to_string(),
                    detail: "GTD security buffer overflows timestamp".to_owned(),
                })?;
            let expiration = u64::try_from(wire_expiration.timestamp()).map_err(|error| {
                execution_time_conversion(
                    "entry_plan.valid_until",
                    &wire_expiration.timestamp(),
                    &error,
                )
            })?;
            (
                limit_price,
                OrderType::Gtd { expiration },
                OrderAmount::Shares(sizing.suggested_shares),
                true,
            )
        }
    };
    Ok(EntryOrderSpec {
        token_id: rec.token_id.clone(),
        side: Side::Buy,
        order_type,
        post_only,
        limit_price,
        amount,
        max_slippage_bps: entry_plan.max_slippage_bps,
        valid_until: entry_plan.valid_until,
    })
}

/// Project a recommendation's `ExitPlan` into the [`ExitPolicySpec`] frozen on
/// the intent — a **faithful, complete** projection so the exit monitor never
/// re-reads the (possibly expired/revoked) recommendation for the price / time /
/// trailing / partial ladder. Only genuine scaled exits (`sell_pct < 1`) ride in
/// `partial_exit_nodes`; the canonical full exit stays in the scalar fields. The
/// entry reference price and composite score are frozen as the baselines for
/// percentage-based stops and signal-degradation re-inference.
fn project_exit_policy_spec(
    rec: &RecommendationInfo,
    entry_reference_price: Price,
) -> Result<ExitPolicySpec, ExecutionError> {
    let Some((_, _, _, exit, _)) = rec.trade_plan.frozen() else {
        return Err(ExecutionError::IntentDenied {
            reason: "recommendation trade plan is unavailable".to_owned(),
        });
    };
    Ok(ExitPolicySpec {
        take_profit_price: exit.take_profit_price,
        take_profit_pct: exit.take_profit_pct,
        stop_loss_price: exit.stop_loss_price,
        stop_loss_pct: exit.stop_loss_pct,
        time_exit_at: exit.time_exit_at,
        max_hold_secs: exit.max_hold_secs,
        trailing_stop: exit.trailing_stop.clone(),
        thesis_invalidation: exit.thesis_invalidation.clone(),
        opportunistic_exit: exit.opportunistic_exit.clone(),
        scale_out_targets: exit.scale_out_targets.clone(),
        settlement_mode: exit.settlement_mode,
        redeem_policy: exit.redeem_policy,
        manual_review_at: exit.manual_review_at,
        entry_reference_price,
        entry_composite_score: rec.composite_score,
    })
}

fn execution_time_conversion(
    field: &'static str,
    value: &impl Display,
    error: &impl Display,
) -> ExecutionError {
    ExecutionError::TimeConversion {
        field,
        value: value.to_string(),
        detail: error.to_string(),
    }
}

/// Validate an operator downscale against the frozen entry. Returns the narrowed
/// `(entry_override, allocated_override)`, or `(None, None)` for a pure approval.
/// Widening size, raising the limit, or downscaling when disabled are rejected.
fn resolve_downscale(
    command: &ApproveIntentCommand,
    frozen: &EntryOrderSpec,
) -> Result<(Option<EntryOrderSpec>, Option<Usd>), ExecutionError> {
    if command.override_amount.is_none() && command.override_price.is_none() {
        return Ok((None, None));
    }
    let new_amount = match (frozen.amount, command.override_amount) {
        (current, None) => current,
        (OrderAmount::CashBudget(current), Some(OrderAmount::CashBudget(value)))
            if value.is_positive() && value <= current =>
        {
            OrderAmount::CashBudget(value)
        }
        (OrderAmount::Shares(current), Some(OrderAmount::Shares(value)))
            if value.is_positive() && value <= current =>
        {
            OrderAmount::Shares(value)
        }
        (OrderAmount::CashBudget(_), Some(OrderAmount::Shares(_)))
        | (OrderAmount::Shares(_), Some(OrderAmount::CashBudget(_))) => {
            return Err(ExecutionError::IntentDenied {
                reason: "approval amount unit must match the frozen order amount".to_owned(),
            });
        }
        (_, Some(_)) => {
            return Err(ExecutionError::IntentDenied {
                reason: "approval amount must be positive and cannot exceed the frozen amount"
                    .to_owned(),
            });
        }
    };
    let new_limit = command.override_price.unwrap_or(frozen.limit_price);
    let price_widens_risk = match frozen.side {
        Side::Buy => new_limit > frozen.limit_price,
        Side::Sell => new_limit < frozen.limit_price,
    };
    if price_widens_risk {
        return Err(ExecutionError::IntentDenied {
            reason: "approval price override cannot widen the frozen side-aware price bound"
                .to_owned(),
        });
    }

    let mut entry = frozen.clone();
    entry.limit_price = new_limit;
    entry.amount = new_amount;
    let new_notional = entry.notional();
    Ok((Some(entry), Some(new_notional)))
}

fn approval_reason(command: &ApproveIntentCommand) -> String {
    command.reason.clone()
}

/// Build the WORM operation-log row written inside a background-origin intent
/// transition transaction (sweep expiry / report-cascade invalidation).
fn intent_operation_log(
    action: &str,
    intent: &OrderIntentInfo,
    reason: &str,
) -> QuantResult<NewOperationLog> {
    Ok(NewOperationLog {
        id: OperationLogId::from_v7(),
        request_id: format!("quant-intent:{action}:{}", intent.order_intent_id).into(),
        actor_user_id: None,
        actor_username: Some("system".to_owned()),
        acting_role: Some("intent_lifecycle".into()),
        category: OperationCategory::Governance,
        action: action.into(),
        resource_type: Some(ResourceType::OrderIntent),
        resource_id: Some(intent.order_intent_id.to_string()),
        http_method: OperationHttpMethod::System,
        http_path: format!("/system/quant/intent/{}/{action}", intent.order_intent_id),
        http_status: 200,
        outcome: OperationOutcome::Success,
        client_ip: None,
        user_agent: None,
        latency_ms: 0,
        detail: OperationDetailDocument::from_serializable(&serde_json::json!({
            "reason": reason,
        }))
        .map_err(|error| InfraError::AuditDetailInvalid {
            detail: error.to_string(),
        })?,
        before_hash: None,
        after_hash: None,
        governance_audit_event_id: None,
        governance_audit_sequence: None,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        project_entry_order_spec, project_exit_policy_spec, resolve_downscale, resolve_policy,
    };
    use crate::execution::mode_gate::IntentPolicyDecision;
    use chrono::{DateTime, Duration, TimeZone, Utc};
    use quant_pivot_error::execution::ExecutionError;
    use quant_pivot_models::{
        domain::{ApproveIntentCommand, RecommendationInfo, evaluate_intent_approval_invalidation},
        enums::{
            common::{OrderType, Side},
            execution::ApprovalInvalidation,
            quant::{
                ApprovalStatus, FillRequirement, OrderIntentStatus, OutcomeSide,
                RecommendationReportStatus, ReportKind,
            },
        },
        types::{
            Bps, ContentHash, DecisionPolicySnapshotId, EntryOrderPolicy, EntryOrderSpec,
            EntryPlan, ExitPlan, OrderAmount, OrderIntentId, Price, RecommendationId,
            RecommendationReportId, RecommendationTradePlan, RiskEnvelope, RoleCode, Shares,
            SizingPlan, TokenId, Usd, UserId,
        },
    };
    use quant_pivot_test_support::report_fixtures;
    use rust_decimal_macros::dec;
    use std::time::Duration as StdDuration;

    fn rec() -> RecommendationInfo {
        report_fixtures::recommendation(
            RecommendationReportId::from_v7(),
            RecommendationId::from_v7(),
            1,
            "0xmkt",
            OutcomeSide::Yes,
            Usd::new(dec!(250)),
        )
    }

    fn frozen_entry() -> EntryOrderSpec {
        EntryOrderSpec {
            token_id: TokenId::new("token-1"),
            side: Side::Buy,
            order_type: OrderType::Gtc,
            post_only: false,
            limit_price: Price::new(dec!(0.60)),
            amount: OrderAmount::Shares(Shares::new(dec!(100))),
            max_slippage_bps: Bps::new(dec!(50)),
            valid_until: Utc::now(),
        }
    }

    fn entry(rec: &RecommendationInfo) -> &EntryPlan {
        match &rec.trade_plan {
            RecommendationTradePlan::Frozen { entry, .. } => entry,
            RecommendationTradePlan::Unavailable { .. } => panic!("fixture must be frozen"),
        }
    }

    fn entry_mut(rec: &mut RecommendationInfo) -> &mut EntryPlan {
        match &mut rec.trade_plan {
            RecommendationTradePlan::Frozen { entry, .. } => entry,
            RecommendationTradePlan::Unavailable { .. } => panic!("fixture must be frozen"),
        }
    }

    fn sizing(rec: &RecommendationInfo) -> &SizingPlan {
        rec.trade_plan.sizing().expect("fixture frozen sizing")
    }

    fn exit(rec: &RecommendationInfo) -> &ExitPlan {
        match &rec.trade_plan {
            RecommendationTradePlan::Frozen { exit, .. } => exit,
            RecommendationTradePlan::Unavailable { .. } => panic!("fixture must be frozen"),
        }
    }

    fn risk(rec: &RecommendationInfo) -> &RiskEnvelope {
        match &rec.trade_plan {
            RecommendationTradePlan::Frozen { risk_envelope, .. } => risk_envelope,
            RecommendationTradePlan::Unavailable { .. } => panic!("fixture must be frozen"),
        }
    }

    fn approve_command() -> ApproveIntentCommand {
        ApproveIntentCommand {
            order_intent_id: OrderIntentId::from_v7(),
            operator_id: UserId::new(uuid::Uuid::nil()),
            acting_role: RoleCode::new("operator"),
            reason: "ok".to_owned(),
            override_amount: None,
            override_price: None,
        }
    }

    #[test]
    fn entry_projection_is_gtd_for_resting_limit() {
        let mut rec = rec();
        let now = Utc.timestamp_opt(1_700_000_000, 0).single().expect("time");
        entry_mut(&mut rec).valid_until = now + Duration::minutes(10);
        let spec = project_entry_order_spec(&rec, now).expect("projection");
        assert_eq!(spec.token_id, rec.token_id);
        assert_eq!(spec.side, Side::Buy);
        assert_eq!(spec.projected_shares(), sizing(&rec).suggested_shares);
        assert!(spec.post_only);
        assert_eq!(
            spec.order_type,
            OrderType::Gtd {
                expiration: u64::try_from(
                    (entry(&rec).valid_until + Duration::seconds(60)).timestamp()
                )
                .expect("timestamp"),
            }
        );
    }

    #[test]
    fn aggressive_all_or_nothing_entry_projects_to_fok_usd() {
        let mut rec = rec();
        entry_mut(&mut rec).order_policy = EntryOrderPolicy::Aggressive {
            worst_price: Price::new(dec!(0.60)),
            fill_requirement: FillRequirement::AllOrNothing,
        };
        let spec = project_entry_order_spec(&rec, Utc::now()).expect("projection");
        assert_eq!(spec.order_type, OrderType::Fok);
        assert!(!spec.post_only);
        assert_eq!(
            spec.amount,
            OrderAmount::CashBudget(sizing(&rec).suggested_usd)
        );
    }

    #[test]
    fn aggressive_partial_entry_projects_to_fak_usd() {
        let mut rec = rec();
        entry_mut(&mut rec).order_policy = EntryOrderPolicy::Aggressive {
            worst_price: Price::new(dec!(0.60)),
            fill_requirement: FillRequirement::AllowPartial,
        };
        let spec = project_entry_order_spec(&rec, Utc::now()).expect("projection");
        assert_eq!(spec.order_type, OrderType::Fak);
        assert!(!spec.post_only);
        assert_eq!(
            spec.amount,
            OrderAmount::CashBudget(sizing(&rec).suggested_usd)
        );
    }

    #[test]
    fn passive_entry_rejects_effective_lifetime_below_two_minutes() {
        let mut rec = rec();
        let now = Utc.timestamp_opt(1_700_000_000, 0).single().expect("time");
        entry_mut(&mut rec).valid_until = now + Duration::seconds(119);
        assert!(matches!(
            project_entry_order_spec(&rec, now),
            Err(ExecutionError::IntentDenied { .. })
        ));
    }

    #[test]
    fn passive_entry_rejects_non_post_only_policy() {
        let mut rec = rec();
        entry_mut(&mut rec).order_policy = EntryOrderPolicy::Passive {
            limit_price: Price::new(dec!(0.60)),
            post_only: false,
        };
        assert!(matches!(
            project_entry_order_spec(&rec, Utc::now()),
            Err(ExecutionError::IntentDenied { reason })
                if reason == "passive entry policy must be post-only"
        ));
    }

    #[test]
    fn exit_projection_drops_full_exit_nodes() {
        let rec = rec();
        let projected =
            project_exit_policy_spec(&rec, Price::new(dec!(0.60))).expect("exit projection");
        assert_eq!(projected.take_profit_price, exit(&rec).take_profit_price);
        assert!(
            projected
                .scale_out_targets
                .iter()
                .all(|target| target.target_cumulative_exit_pct < dec!(1))
        );
    }

    #[test]
    fn downscale_noop_without_overrides() {
        let (entry, alloc) = resolve_downscale(&approve_command(), &frozen_entry()).expect("noop");
        assert!(entry.is_none());
        assert!(alloc.is_none());
    }

    #[test]
    fn downscale_reduces_shares_and_notional() {
        let mut cmd = approve_command();
        cmd.override_amount = Some(OrderAmount::Shares(Shares::new(dec!(50))));
        let (entry, alloc) = resolve_downscale(&cmd, &frozen_entry()).expect("downscale");
        let entry = entry.expect("entry override");
        assert_eq!(entry.amount, OrderAmount::Shares(Shares::new(dec!(50))));
        assert_eq!(alloc, Some(Usd::new(dec!(30)))); // 50 * 0.60
    }

    #[test]
    fn downscale_rejects_widening_size() {
        let mut cmd = approve_command();
        cmd.override_amount = Some(OrderAmount::Shares(Shares::new(dec!(200))));
        assert!(resolve_downscale(&cmd, &frozen_entry()).is_err());
    }

    #[test]
    fn downscale_rejects_raising_limit() {
        let mut cmd = approve_command();
        cmd.override_price = Some(Price::new(dec!(0.70)));
        assert!(resolve_downscale(&cmd, &frozen_entry()).is_err());
    }

    #[test]
    fn usd_price_only_override_preserves_frozen_spend() {
        let mut cmd = approve_command();
        cmd.override_price = Some(Price::new(dec!(0.55)));
        let mut frozen = frozen_entry();
        frozen.amount = OrderAmount::CashBudget(Usd::new(dec!(60)));
        let (entry, allocation) = resolve_downscale(&cmd, &frozen).expect("tighten USD order");
        assert_eq!(
            entry.expect("override").amount,
            OrderAmount::CashBudget(Usd::new(dec!(60)))
        );
        assert_eq!(allocation, Some(Usd::new(dec!(60))));
    }

    #[test]
    fn sell_price_override_cannot_lower_frozen_bound() {
        let mut cmd = approve_command();
        cmd.override_price = Some(Price::new(dec!(0.50)));
        let mut frozen = frozen_entry();
        frozen.side = Side::Sell;
        assert!(resolve_downscale(&cmd, &frozen).is_err());
        cmd.override_price = Some(Price::new(dec!(0.70)));
        assert!(resolve_downscale(&cmd, &frozen).is_ok());
    }

    #[test]
    fn downscale_rejects_amount_unit_mismatch() {
        let mut cmd = approve_command();
        cmd.override_amount = Some(OrderAmount::CashBudget(Usd::new(dec!(10))));
        assert!(resolve_downscale(&cmd, &frozen_entry()).is_err());
    }

    #[test]
    fn invalidation_passes_for_fresh_published_intent() {
        let mut rec = rec();
        rec.valid_until = Utc::now() + Duration::hours(1);
        let report = report_fixtures::report(
            rec.recommendation_report_id.clone(),
            ReportKind::TopN,
            RecommendationReportStatus::Published,
        );
        let version = DecisionPolicySnapshotId::from_v7();
        let hash = risk(&rec).envelope_hash.clone();
        assert!(
            evaluate_intent_approval_invalidation(
                &rec,
                &report,
                true,
                &version,
                &version,
                &hash,
                Utc::now()
            )
            .is_none()
        );
    }

    #[test]
    fn semi_auto_expires_at_is_capped_by_recommendation_valid_until() {
        let now = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        let rec_valid_until = now + Duration::hours(1);
        let entry_valid_until = now + Duration::hours(2);
        let resolved = resolve_policy(
            IntentPolicyDecision::RequiresApproval {
                approval_ttl: StdDuration::from_hours(24),
            },
            now,
            rec_valid_until,
            entry_valid_until,
        )
        .expect("policy");
        assert_eq!(resolved.status, OrderIntentStatus::PendingApproval);
        assert_eq!(resolved.expires_at, rec_valid_until);
    }

    #[test]
    fn semi_auto_expires_at_uses_approval_ttl_when_shorter_than_recommendation() {
        let now = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        let rec_valid_until = now + Duration::hours(24);
        let entry_valid_until = now + Duration::hours(24);
        let resolved = resolve_policy(
            IntentPolicyDecision::RequiresApproval {
                approval_ttl: StdDuration::from_mins(15),
            },
            now,
            rec_valid_until,
            entry_valid_until,
        )
        .expect("policy");
        assert_eq!(resolved.expires_at, now + Duration::seconds(900));
    }

    #[test]
    fn semi_auto_rejects_unrepresentable_approval_ttl() {
        let now = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        assert!(matches!(
            resolve_policy(
                IntentPolicyDecision::RequiresApproval {
                    approval_ttl: StdDuration::from_secs(u64::MAX),
                },
                now,
                now + Duration::hours(1),
                now + Duration::hours(1),
            ),
            Err(ExecutionError::TimeConversion {
                field: "approval_ttl_secs",
                ..
            })
        ));
    }

    #[test]
    fn gtd_projection_rejects_security_buffer_overflow() {
        let mut rec = rec();
        entry_mut(&mut rec).valid_until = DateTime::<Utc>::MAX_UTC;
        assert!(matches!(
            project_entry_order_spec(&rec, DateTime::<Utc>::MAX_UTC - Duration::seconds(120)),
            Err(ExecutionError::TimeConversion {
                field: "entry_plan.valid_until",
                ..
            })
        ));
    }

    #[test]
    fn semi_auto_expires_at_is_capped_by_entry_valid_until() {
        let now = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        let rec_valid_until = now + Duration::hours(24);
        let entry_valid_until = now + Duration::minutes(30);
        let resolved = resolve_policy(
            IntentPolicyDecision::RequiresApproval {
                approval_ttl: StdDuration::from_hours(24),
            },
            now,
            rec_valid_until,
            entry_valid_until,
        )
        .expect("policy");
        assert_eq!(resolved.expires_at, entry_valid_until);
    }

    #[test]
    fn resolve_policy_rejects_when_all_deadlines_elapsed() {
        let now = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        assert!(matches!(
            resolve_policy(
                IntentPolicyDecision::RequiresApproval {
                    approval_ttl: StdDuration::from_mins(15),
                },
                now,
                now - Duration::minutes(1),
                now - Duration::minutes(2),
            ),
            Err(ExecutionError::RecommendationExpired { .. })
        ));
    }

    #[test]
    fn auto_policy_expires_at_is_capped_by_recommendation_valid_until() {
        let now = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        let rec_valid_until = now + Duration::hours(1);
        let entry_valid_until = now + Duration::hours(2);
        let resolved = resolve_policy(
            IntentPolicyDecision::ApprovedByPolicy {
                policy_id: "policy-1".to_owned(),
                policy_hash: None,
                reason: "ok".to_owned(),
            },
            now,
            rec_valid_until,
            entry_valid_until,
        )
        .expect("policy");
        assert_eq!(resolved.status, OrderIntentStatus::ApprovedByPolicy);
        assert_eq!(resolved.approval_status, ApprovalStatus::NotRequired);
        assert_eq!(resolved.expires_at, rec_valid_until);
    }

    #[test]
    fn invalidation_trips_each_condition() {
        let now = Utc::now();
        let mut rec = rec();
        rec.valid_until = now + Duration::hours(1);
        let report = report_fixtures::report(
            rec.recommendation_report_id.clone(),
            ReportKind::TopN,
            RecommendationReportStatus::Published,
        );
        let version = DecisionPolicySnapshotId::from_v7();
        let hash = risk(&rec).envelope_hash.clone();

        // recommendation expired
        let mut expired = rec.clone();
        expired.valid_until = now - Duration::hours(1);
        assert_eq!(
            evaluate_intent_approval_invalidation(
                &expired, &report, true, &version, &version, &hash, now
            ),
            Some(ApprovalInvalidation::RecommendationExpired)
        );

        // report no longer published
        let revoked = report_fixtures::report(
            rec.recommendation_report_id.clone(),
            ReportKind::TopN,
            RecommendationReportStatus::Revoked,
        );
        assert_eq!(
            evaluate_intent_approval_invalidation(
                &rec, &revoked, true, &version, &version, &hash, now
            ),
            Some(ApprovalInvalidation::ReportRevoked)
        );

        // kill switch opened
        assert_eq!(
            evaluate_intent_approval_invalidation(
                &rec, &report, false, &version, &version, &hash, now
            ),
            Some(ApprovalInvalidation::KillSwitchOpened)
        );

        // runtime config version changed
        let other_version = DecisionPolicySnapshotId::from_v7();
        assert_eq!(
            evaluate_intent_approval_invalidation(
                &rec,
                &report,
                true,
                &other_version,
                &version,
                &hash,
                now
            ),
            Some(ApprovalInvalidation::RuntimeConfigChanged)
        );

        // risk envelope hash mismatch: a stored hash differing from the rec's.
        let stored_hash = ContentHash::parse(format!("blake3:{}", "1".repeat(64))).expect("hash");
        assert_ne!(stored_hash, risk(&rec).envelope_hash);
        assert_eq!(
            evaluate_intent_approval_invalidation(
                &rec,
                &report,
                true,
                &version,
                &version,
                &stored_hash,
                now
            ),
            Some(ApprovalInvalidation::RiskEnvelopeMismatch)
        );
    }

    // ── Phase 11.3 closed-loop hardening: intent-creation-time calibration
    // recheck (closes the TOCTOU window between report generation and intent
    // creation — a calibrator deactivated after a report was built must not
    // let a stale `execution_eligibility` still create a capital-reserving
    // `SemiAuto`/`AutoExecution` intent) ────────────────────────────────────

    mod calibration_recheck {
        use std::{
            env, process,
            sync::{
                Arc,
                atomic::{AtomicU64, Ordering},
            },
        };

        use crate::execution::intent_service::recheck_return_model_calibrated;
        use async_trait::async_trait;
        use chrono::Utc;
        use quant_pivot_error::{
            QuantError, QuantResult, research::ResearchError, storage::StorageError,
        };
        use quant_pivot_models::{
            domain::{
                ModelPickerSide, ModelSpecInfo, ModelSpecListQuery, ModelVersionInfo,
                ModelVersionListQuery, NewModelSpec, NewModelVersion, Paginated,
                PublishedModelCatalogInfo,
            },
            enums::{
                common::MarketCategory,
                quant::{DownsideSource, PublicationStatus},
            },
            runtime_config::FactorCrossSectionConfig,
            types::{
                BacktestPathSetId, CalibrationArtifactId, ContentHash, ModelInputContract,
                ModelSpecId, ModelVersionId,
                calibration::{MonotoneMapping, ReliabilityReport},
                model_metrics::ModelVersionMetrics,
                model_quality::QualityGateReport,
                model_training::ModelTrainingObjective,
            },
        };
        use quant_pivot_repository::traits::{
            ModelRegistryRepository, PublishModelVersionCommit, PublishModelVersionResult,
        };
        use quant_pivot_research::{
            artifact::{ArtifactStore, LocalArtifactStore},
            factors::{FrozenReferenceQuantiles, names::LIQUIDITY_DEPTH},
            model::{
                CalibratedReturnModel, CalibrationArtifactLoader, FactorWeight, ModelArtifact,
                ModelArtifactHeader, ModelFamily, ResolvedCalibration, ReturnModelSpec,
                ScoreMultiplierSpec, SubstitutionConfidenceRules, WeightedFactorModelArtifact,
                model_input_contract_hash,
            },
        };
        use quant_pivot_test_support::{
            execution_pg_seed::fixture_profile_ref, model_spec_fixtures::model_spec_lineage_fixture,
        };

        struct FakeRegistry {
            version: Option<ModelVersionInfo>,
        }

        #[async_trait]
        impl ModelRegistryRepository for FakeRegistry {
            async fn create_model_spec(
                &self,
                _spec: NewModelSpec,
            ) -> Result<ModelSpecInfo, StorageError> {
                unimplemented!("not exercised by the calibration recheck tests")
            }
            async fn find_model_spec_by_id(
                &self,
                _model_spec_id: &ModelSpecId,
            ) -> Result<Option<ModelSpecInfo>, StorageError> {
                unimplemented!("not exercised by the calibration recheck tests")
            }
            async fn create_model_version(
                &self,
                _version: NewModelVersion,
            ) -> Result<ModelVersionInfo, StorageError> {
                unimplemented!("not exercised by the calibration recheck tests")
            }
            async fn next_version_for_spec(
                &self,
                _model_spec_id: &ModelSpecId,
            ) -> Result<i32, StorageError> {
                unimplemented!("not exercised by the calibration recheck tests")
            }
            async fn find_model_version_by_id(
                &self,
                model_version_id: &ModelVersionId,
            ) -> Result<Option<ModelVersionInfo>, StorageError> {
                Ok(self
                    .version
                    .as_ref()
                    .filter(|version| version.model_version_id == *model_version_id)
                    .cloned())
            }
            async fn page_specs(
                &self,
                _query: ModelSpecListQuery,
            ) -> Result<Paginated<ModelSpecInfo>, StorageError> {
                unimplemented!("not exercised by the calibration recheck tests")
            }
            async fn page_versions(
                &self,
                _query: ModelVersionListQuery,
            ) -> Result<Paginated<ModelVersionInfo>, StorageError> {
                unimplemented!("not exercised by the calibration recheck tests")
            }
            async fn list_published_catalog(
                &self,
                _side: ModelPickerSide,
                _category: Option<MarketCategory>,
            ) -> Result<Vec<PublishedModelCatalogInfo>, StorageError> {
                unimplemented!("not exercised by the calibration recheck tests")
            }
            async fn list_published_for_spec(
                &self,
                _model_spec_id: &ModelSpecId,
            ) -> Result<Vec<ModelVersionInfo>, StorageError> {
                unimplemented!("not exercised by the calibration recheck tests")
            }
            async fn retire_model_version(
                &self,
                _model_version_id: &ModelVersionId,
            ) -> Result<ModelVersionInfo, StorageError> {
                unimplemented!("not exercised by the calibration recheck tests")
            }
            async fn publish_model_version(
                &self,
                _commit: PublishModelVersionCommit<'_>,
            ) -> Result<PublishModelVersionResult, StorageError> {
                unimplemented!("not exercised by the calibration recheck tests")
            }
            async fn promote_model_to_shadow(
                &self,
                _model_version_id: &ModelVersionId,
            ) -> Result<ModelVersionInfo, StorageError> {
                unimplemented!("not exercised by the calibration recheck tests")
            }
            async fn set_quality_gate_report(
                &self,
                _model_version_id: &ModelVersionId,
                _quality_gate_report: QualityGateReport,
            ) -> Result<ModelVersionInfo, StorageError> {
                unimplemented!("not exercised by the calibration recheck tests")
            }
            async fn set_publish_path_set_id(
                &self,
                _model_version_id: &ModelVersionId,
                _publish_path_set_id: Option<BacktestPathSetId>,
            ) -> Result<ModelVersionInfo, StorageError> {
                unimplemented!("not exercised by the calibration recheck tests")
            }
        }

        /// Always resolves (or always fails), regardless of which artifact
        /// id is requested — sufficient for these tests, which only vary the
        /// artifact's `ReturnModelSpec` variant, not the calibrator lookup.
        struct FakeCalibrationLoader {
            succeeds: bool,
        }

        #[async_trait]
        impl CalibrationArtifactLoader for FakeCalibrationLoader {
            async fn load(
                &self,
                artifact_id: &CalibrationArtifactId,
            ) -> QuantResult<ResolvedCalibration> {
                if self.succeeds {
                    Ok(ResolvedCalibration {
                        artifact_id: artifact_id.clone(),
                        mapping: MonotoneMapping::Isotonic { knots: Vec::new() },
                        reliability: ReliabilityReport {
                            bins: Vec::new(),
                            brier_score: rust_decimal::Decimal::ZERO,
                            log_loss: rust_decimal::Decimal::ZERO,
                            ece: rust_decimal::Decimal::ZERO,
                            n_samples: 0,
                        },
                    })
                } else {
                    Err(QuantError::from(ResearchError::DatasetBuild {
                        detail: "calibrator deactivated after report build (test)".to_owned(),
                    }))
                }
            }
        }

        fn temp_store() -> Arc<dyn ArtifactStore> {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let root = env::temp_dir().join(format!(
                "qp_intent_calibration_recheck_test_{}_{}_{}",
                process::id(),
                Utc::now().timestamp_nanos_opt().unwrap_or_default(),
                COUNTER.fetch_add(1, Ordering::Relaxed)
            ));
            Arc::new(LocalArtifactStore::new(root))
        }

        fn header(model_version_id: ModelVersionId) -> ModelArtifactHeader {
            ModelArtifactHeader {
                model_version_id,
                model_spec_definition_hash: ContentHash::parse(format!(
                    "blake3:{}",
                    "0".repeat(64)
                ))
                .expect("hash"),
                profile_ref: fixture_profile_ref(),
                model_family: ModelFamily::WeightedFactor,
                feature_schema_hash: ContentHash::parse(format!("blake3:{}", "1".repeat(64)))
                    .expect("hash"),
                factor_schema_hash: ContentHash::parse(format!("blake3:{}", "2".repeat(64)))
                    .expect("hash"),
                trade_policy_artifact_id: None,
                trade_policy_hash: None,
            }
        }

        fn weighted_artifact(
            model_version_id: ModelVersionId,
            return_model: ReturnModelSpec,
        ) -> ModelArtifact {
            let input_contract = ModelInputContract::single_required("book.mid");
            let input_contract_hash =
                model_input_contract_hash(&input_contract).expect("input contract hash");
            ModelArtifact::WeightedFactor(Box::new(WeightedFactorModelArtifact {
                header: header(model_version_id),
                training_dataset_hash: ContentHash::parse(format!("blake3:{}", "3".repeat(64)))
                    .expect("hash"),
                training_input_hash: ContentHash::parse(format!("blake3:{}", "4".repeat(64)))
                    .expect("hash"),
                input_contract,
                input_contract_hash,
                weights: vec![FactorWeight {
                    factor: LIQUIDITY_DEPTH,
                    weight: rust_decimal::Decimal::ONE,
                }],
                prediction_horizon_secs: 86_400,
                multipliers: ScoreMultiplierSpec::conservative(),
                substitution_confidence_rules: SubstitutionConfidenceRules::conservative(),
                return_model,
                factor_cross_section: FactorCrossSectionConfig::default(),
                frozen_reference_quantiles: FrozenReferenceQuantiles::empty(),
                objective_report: None,
                category_scope: None,
            }))
        }

        /// Seeds a real `ModelVersionInfo` + a hash-verified artifact into a
        /// real (file-backed, no Docker) `LocalArtifactStore` — no fake
        /// needed for `ArtifactStore`/`load_hash_verified_artifact` itself.
        async fn seeded(
            return_model: ReturnModelSpec,
        ) -> (Arc<dyn ArtifactStore>, ModelVersionInfo) {
            let store = temp_store();
            let model_version_id = ModelVersionId::from_v7();
            let artifact = weighted_artifact(model_version_id.clone(), return_model);
            let digest = artifact.content_hash().expect("hash");
            let key = ModelArtifact::artifact_key(&digest).expect("key");
            store
                .put(key, &artifact.to_bytes().expect("bytes"))
                .await
                .expect("put");
            let (model_spec_thesis, model_spec_definition_hash) =
                model_spec_lineage_fixture("intent-service-test-spec");
            let version = ModelVersionInfo {
                model_version_id,
                model_spec_id: ModelSpecId::from_v7(),
                model_spec_name: "intent-service-test-spec".to_owned(),
                model_family: ModelFamily::WeightedFactor,
                model_spec_thesis,
                model_spec_definition_hash,
                version: 1,
                artifact_hash: digest,
                category_scope: None,
                profile_ref: fixture_profile_ref(),
                training_dataset_id: None,
                trade_policy_artifact_id: None,
                trade_policy_hash: None,
                publish_path_set_id: None,
                derivation_kind: ModelVersionInfo::training_derivation_kind(),
                parent_model_version_id: None,
                source_backtest_report_id: None,
                calibration_artifact_id: None,
                score_multiplier_calibration_report: None,
                derivation_evidence_hash: None,
                metrics: ModelVersionMetrics::not_measured("test fixture"),
                training_objective: ModelTrainingObjective::hand_authored("test fixture"),
                quality_gate_report: None,
                publication_status: PublicationStatus::Published,
                published_at: Some(Utc::now()),
                retired_at: None,
                created_at: Utc::now(),
            };
            (store, version)
        }

        #[tokio::test]
        async fn uncalibrated_model_blocks_semi_auto_intent_creation() {
            // Alias for the acceptance name in Phase 11.3 §8 — same TOCTOU
            // close as `heuristic_return_model_denies_semi_auto_intent_creation`.
            let (store, version) = seeded(ReturnModelSpec::heuristic_default()).await;
            let model_registry: Arc<dyn ModelRegistryRepository> = Arc::new(FakeRegistry {
                version: Some(version.clone()),
            });
            let calibration_loader: Arc<dyn CalibrationArtifactLoader> =
                Arc::new(FakeCalibrationLoader { succeeds: true });
            let result = recheck_return_model_calibrated(
                &model_registry,
                &store,
                &calibration_loader,
                &version.model_version_id,
            )
            .await;
            assert!(
                result.is_err(),
                "uncalibrated heuristic return model must block SemiAuto intent creation"
            );
        }

        #[tokio::test]
        async fn heuristic_return_model_denies_semi_auto_intent_creation() {
            let (store, version) = seeded(ReturnModelSpec::heuristic_default()).await;
            let model_registry: Arc<dyn ModelRegistryRepository> = Arc::new(FakeRegistry {
                version: Some(version.clone()),
            });
            let calibration_loader: Arc<dyn CalibrationArtifactLoader> =
                Arc::new(FakeCalibrationLoader { succeeds: true });
            let result = recheck_return_model_calibrated(
                &model_registry,
                &store,
                &calibration_loader,
                &version.model_version_id,
            )
            .await;
            assert!(
                result.is_err(),
                "an uncalibrated (heuristic) return model must deny SemiAuto/AutoExecution \
                 intent creation, not silently allow it: {result:?}"
            );
        }

        #[tokio::test]
        async fn calibrated_and_still_active_allows_intent_creation() {
            let calibrator_ref = CalibrationArtifactId::from_v7();
            let (store, version) = seeded(ReturnModelSpec::Calibrated(CalibratedReturnModel {
                calibrator_ref,
                downside_source: DownsideSource::MfeMae,
            }))
            .await;
            let model_registry: Arc<dyn ModelRegistryRepository> = Arc::new(FakeRegistry {
                version: Some(version.clone()),
            });
            let calibration_loader: Arc<dyn CalibrationArtifactLoader> =
                Arc::new(FakeCalibrationLoader { succeeds: true });
            let result = recheck_return_model_calibrated(
                &model_registry,
                &store,
                &calibration_loader,
                &version.model_version_id,
            )
            .await;
            assert!(
                result.is_ok(),
                "a Calibrated return model whose calibrator still resolves must allow intent \
                 creation: {result:?}"
            );
        }

        #[tokio::test]
        async fn calibrated_but_deactivated_after_report_build_denies_intent_creation() {
            // This is the TOCTOU scenario itself: the return model is still
            // tagged `Calibrated` (as it was when the report was built), but
            // the bound calibrator has since been deactivated/superseded —
            // `resolve_return_model_calibration` must fail closed.
            let calibrator_ref = CalibrationArtifactId::from_v7();
            let (store, version) = seeded(ReturnModelSpec::Calibrated(CalibratedReturnModel {
                calibrator_ref,
                downside_source: DownsideSource::MfeMae,
            }))
            .await;
            let model_registry: Arc<dyn ModelRegistryRepository> = Arc::new(FakeRegistry {
                version: Some(version.clone()),
            });
            let calibration_loader: Arc<dyn CalibrationArtifactLoader> =
                Arc::new(FakeCalibrationLoader { succeeds: false });
            let result = recheck_return_model_calibrated(
                &model_registry,
                &store,
                &calibration_loader,
                &version.model_version_id,
            )
            .await;
            assert!(
                result.is_err(),
                "a calibrator deactivated after report build must deny intent creation \
                 (TOCTOU close), not silently continue trusting the stale reference"
            );
        }

        #[tokio::test]
        async fn missing_model_version_denies_intent_creation() {
            let model_registry: Arc<dyn ModelRegistryRepository> =
                Arc::new(FakeRegistry { version: None });
            let calibration_loader: Arc<dyn CalibrationArtifactLoader> =
                Arc::new(FakeCalibrationLoader { succeeds: true });
            let store = temp_store();
            let result = recheck_return_model_calibrated(
                &model_registry,
                &store,
                &calibration_loader,
                &ModelVersionId::from_v7(),
            )
            .await;
            assert!(
                result.is_err(),
                "a missing model version must deny, not panic or pass"
            );
        }
    }
}
