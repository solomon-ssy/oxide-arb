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

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantResult, execution::ExecutionError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        ApproveIntentCommand, ApproveOrderIntent, ApproveOrderIntentOutcome, CancelIntentCommand,
        CreateIntentCommand, IntentEventKind, NewCapitalAllocation, NewOperationLog,
        NewOrderIntent, OrderIntentInfo, OrderIntentListQuery, OrderIntentPort, Paginated,
        RecommendationInfo, RecommendationReportInfo, RejectIntentCommand,
    },
    enums::{
        common::{OrderType, Side},
        execution::{ApprovalInvalidation, CapitalAllocationState, OrderIntentKind},
        operation_log::{OperationCategory, OperationOutcome},
        quant::{ApprovalStatus, OrderIntentStatus, QuantRuntimeMode, RecommendationReportStatus},
        rbac::ResourceType,
    },
    runtime_config::EntryOrderPolicy,
    types::{
        CapitalAllocationId, ContentHash, EntryOrderSpec, ExitPolicySpec, ModelVersionId,
        OperationLogId, OrderIntentId, Price, RecommendationId, RecommendationReportId, Usd,
    },
};
use quant_pivot_repository::traits::{
    ModelRegistryRepository, OrderIntentRepository, RecommendationReportRepository,
    RecommendationRepository,
};
use quant_pivot_research::{
    artifact::ArtifactStore,
    model::{CalibrationArtifactLoader, load_hash_verified_artifact},
};
use rust_decimal::Decimal;

use crate::{
    execution::{
        dispatch_wake::DispatchWake,
        intent_lifecycle::IntentLifecyclePublisher,
        mode_gate::{IntentPolicyDecision, RuntimeModeGate},
    },
    governance::{KillSwitchHandle, RuntimeModeHandle, resolve_return_model_calibration},
    observability::metrics_hub::MetricsHub,
    runtime_config::RuntimeConfigStore,
};

/// Notify the intent plane that a report's recommendations are no longer valid.
///
/// The active intents derived from the report are invalidated and their capital
/// released. Implemented by [`CoreOrderIntentService`]; consumed by the report
/// lifecycle on revoke / expire (dependency inversion — the report plane does
/// not depend on execution types).
#[async_trait]
pub trait IntentInvalidationHook: Send + Sync {
    /// Invalidate every active (pre-submission) intent derived from `report_id`,
    /// releasing reserved capital. Returns the number invalidated. Best-effort:
    /// the caller logs and continues on error (approval re-check + sweep backstop).
    async fn invalidate_for_report(
        &self,
        report_id: &RecommendationReportId,
        reason: ApprovalInvalidation,
        now: DateTime<Utc>,
    ) -> QuantResult<u32>;

    /// Invalidate every active (pre-submission) intent derived from a single
    /// `recommendation_id`, releasing reserved capital — the per-recommendation
    /// expiry cascade. Returns the number invalidated. Best-effort.
    async fn invalidate_for_recommendation(
        &self,
        recommendation_id: &RecommendationId,
        reason: ApprovalInvalidation,
        now: DateTime<Utc>,
    ) -> QuantResult<u32>;
}

/// Dependencies for [`CoreOrderIntentService`].
pub struct OrderIntentServiceDeps {
    pub mode_gate: Arc<dyn RuntimeModeGate>,
    pub runtime_mode: RuntimeModeHandle,
    pub kill_switch: KillSwitchHandle,
    pub recommendations: Arc<dyn RecommendationRepository>,
    pub reports: Arc<dyn RecommendationReportRepository>,
    pub intents: Arc<dyn OrderIntentRepository>,
    pub config: Arc<RuntimeConfigStore>,
    pub metrics: Arc<MetricsHub>,
    /// Shared `quant.intent` lifecycle fan-out (bootstrap singleton).
    pub intent_lifecycle: Arc<IntentLifecyclePublisher>,
    /// Wake the dispatcher when an `ApprovedByPolicy` intent is created (auto).
    pub dispatch_wake: DispatchWake,
    /// Model registry (calibration-state recheck at `SemiAuto`/`AutoExecution`
    /// intent-creation time — Phase 11.3 closed-loop hardening).
    pub model_registry: Arc<dyn ModelRegistryRepository>,
    /// Content-addressed model artifact store (calibration recheck).
    pub artifact_store: Arc<dyn ArtifactStore>,
    /// Deep calibrator resolution (calibration recheck), the same shared
    /// check publish / report / admission use.
    pub calibration_loader: Arc<dyn CalibrationArtifactLoader>,
}

/// Governed order-intent service (implements the web [`OrderIntentPort`], the
/// report-cascade [`IntentInvalidationHook`], and the sweep `expire_due`).
pub struct CoreOrderIntentService {
    mode_gate: Arc<dyn RuntimeModeGate>,
    runtime_mode: RuntimeModeHandle,
    kill_switch: KillSwitchHandle,
    recommendations: Arc<dyn RecommendationRepository>,
    reports: Arc<dyn RecommendationReportRepository>,
    intents: Arc<dyn OrderIntentRepository>,
    config: Arc<RuntimeConfigStore>,
    metrics: Arc<MetricsHub>,
    lifecycle: Arc<IntentLifecyclePublisher>,
    dispatch_wake: DispatchWake,
    model_registry: Arc<dyn ModelRegistryRepository>,
    artifact_store: Arc<dyn ArtifactStore>,
    calibration_loader: Arc<dyn CalibrationArtifactLoader>,
}

impl CoreOrderIntentService {
    /// Assemble the service from its dependencies.
    #[must_use]
    pub fn new(deps: OrderIntentServiceDeps) -> Self {
        Self {
            mode_gate: deps.mode_gate,
            runtime_mode: deps.runtime_mode,
            kill_switch: deps.kill_switch,
            recommendations: deps.recommendations,
            reports: deps.reports,
            intents: deps.intents,
            config: deps.config,
            metrics: deps.metrics,
            lifecycle: deps.intent_lifecycle,
            dispatch_wake: deps.dispatch_wake,
            model_registry: deps.model_registry,
            artifact_store: deps.artifact_store,
            calibration_loader: deps.calibration_loader,
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

    /// Create an intent from a recommendation at `now` (mode-gated, atomic with
    /// the capital reservation).
    pub async fn create_at(
        &self,
        command: CreateIntentCommand,
        now: DateTime<Utc>,
    ) -> QuantResult<OrderIntentInfo> {
        let rec = self.load_recommendation(&command.recommendation_id).await?;
        let report = self.load_report(&rec.recommendation_report_id).await?;
        self.ensure_create_allowed(&rec, &report, now).await?;

        let mode = self.runtime_mode.current();
        if matches!(
            mode,
            QuantRuntimeMode::SemiAuto | QuantRuntimeMode::AutoExecution
        ) {
            self.ensure_return_model_still_calibrated(&rec.evidence_refs.model_version_id)
                .await?;
        }
        let policy = self.mode_gate.evaluate_intent_policy(mode, &rec).await?;
        let config = self.config.current();
        let entry = project_entry_order_spec(&rec, &config.execution.entry_order_policy)?;
        let exit = project_exit_policy_spec(&rec);
        let resolved = resolve_policy(policy, now, rec.valid_until, entry.valid_until)?;
        let (new_intent, allocation) =
            compose_create_rows(&rec, &report, mode, resolved, entry, exit);

        let intent = self
            .intents
            .create_with_allocation(new_intent, allocation)
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
        if !rec.status.is_actionable_for_intent() {
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

        let allow_downscale = self
            .config
            .current()
            .execution
            .semi_auto
            .allow_size_reduction;
        let (entry_override, allocated_override) =
            resolve_downscale(&command, &intent.entry_order_json, allow_downscale)?;
        let approval = ApproveOrderIntent {
            approved_by: command.operator_id,
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
        let rejected = self
            .intents
            .reject(&command.order_intent_id, command.reason)
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
        let cancelled = self
            .intents
            .cancel(&command.order_intent_id, command.reason)
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
            let log = intent_operation_log("quant.intent.expire", &intent, "ttl_expired");
            match self.intents.expire(&intent.order_intent_id, log).await {
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

#[async_trait]
impl IntentInvalidationHook for CoreOrderIntentService {
    async fn invalidate_for_report(
        &self,
        report_id: &RecommendationReportId,
        reason: ApprovalInvalidation,
        now: DateTime<Utc>,
    ) -> QuantResult<u32> {
        let active = self.intents.find_active_by_report(report_id).await?;
        let mut count = 0_u32;
        for intent in active {
            let log = intent_operation_log("quant.intent.invalidate", &intent, reason.as_str());
            match self
                .intents
                .invalidate(&intent.order_intent_id, reason, log)
                .await
            {
                Ok(updated) => {
                    self.publish(&updated, IntentEventKind::Invalidated, now);
                    count = count.saturating_add(1);
                }
                Err(error) => {
                    tracing::warn!(intent_id = %intent.order_intent_id, %error, "intent invalidation skipped");
                }
            }
        }
        Ok(count)
    }

    async fn invalidate_for_recommendation(
        &self,
        recommendation_id: &RecommendationId,
        reason: ApprovalInvalidation,
        now: DateTime<Utc>,
    ) -> QuantResult<u32> {
        let active = self
            .intents
            .find_active_intents_by_recommendation(recommendation_id)
            .await?;
        let mut count = 0_u32;
        for intent in active {
            let log = intent_operation_log("quant.intent.invalidate", &intent, reason.as_str());
            match self
                .intents
                .invalidate(&intent.order_intent_id, reason, log)
                .await
            {
                Ok(updated) => {
                    self.publish(&updated, IntentEventKind::Invalidated, now);
                    count = count.saturating_add(1);
                }
                Err(error) => {
                    tracing::warn!(intent_id = %intent.order_intent_id, %error, "intent invalidation skipped");
                }
            }
        }
        Ok(count)
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

fn compose_create_rows(
    rec: &RecommendationInfo,
    report: &RecommendationReportInfo,
    mode: QuantRuntimeMode,
    resolved: ResolvedPolicy,
    entry: EntryOrderSpec,
    exit: ExitPolicySpec,
) -> (NewOrderIntent, NewCapitalAllocation) {
    let intent_id = OrderIntentId::from_v7();
    let planned_usd = rec.sizing_plan.suggested_usd;
    let intent = NewOrderIntent {
        order_intent_id: intent_id.clone(),
        recommendation_id: rec.recommendation_id.clone(),
        runtime_mode: mode,
        runtime_config_version_id: report.runtime_config_version_id.clone(),
        model_version_id: report.model_version_id.clone(),
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
        entry_order_json: entry,
        exit_policy_json: exit,
        risk_envelope_hash: rec.risk_envelope.envelope_hash.clone(),
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
    (intent, allocation)
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
            let approval_deadline =
                now + Duration::seconds(i64::try_from(approval_ttl.as_secs()).unwrap_or(0));
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
    policy: &EntryOrderPolicy,
) -> Result<EntryOrderSpec, ExecutionError> {
    let limit_price = rec
        .entry_plan
        .limit_price
        .or(rec.entry_plan.trigger_price)
        .ok_or_else(|| ExecutionError::IntentDenied {
            reason: format!(
                "recommendation {} has no entry limit price",
                rec.recommendation_id
            ),
        })?;
    let order_type = if policy.allow_market_orders {
        OrderType::Fok
    } else {
        OrderType::Gtd {
            expiration: u64::try_from(rec.entry_plan.valid_until.timestamp()).unwrap_or(0),
        }
    };
    Ok(EntryOrderSpec {
        token_id: rec.token_id.clone(),
        side: Side::Buy,
        order_type,
        limit_price,
        shares: rec.sizing_plan.suggested_shares,
        max_slippage_bps: rec.entry_plan.max_slippage_bps,
        valid_until: rec.entry_plan.valid_until,
    })
}

/// Project a recommendation's `ExitPlan` into the [`ExitPolicySpec`] frozen on
/// the intent — a **faithful, complete** projection so the exit monitor never
/// re-reads the (possibly expired/revoked) recommendation for the price / time /
/// trailing / partial ladder. Only genuine scaled exits (`sell_pct < 1`) ride in
/// `partial_exit_nodes`; the canonical full exit stays in the scalar fields. The
/// entry reference price and composite score are frozen as the baselines for
/// percentage-based stops and signal-degradation re-inference.
fn project_exit_policy_spec(rec: &RecommendationInfo) -> ExitPolicySpec {
    let exit = &rec.exit_plan;
    let entry_reference_price = rec
        .entry_plan
        .limit_price
        .or(rec.entry_plan.trigger_price)
        .or(rec.market_context.mid_price)
        .or(rec.market_context.best_ask)
        .unwrap_or(Price::ZERO);
    ExitPolicySpec {
        take_profit_price: exit.take_profit_price,
        take_profit_pct: exit.take_profit_pct,
        stop_loss_price: exit.stop_loss_price,
        stop_loss_pct: exit.stop_loss_pct,
        time_exit_at: exit.time_exit_at,
        max_hold_secs: exit.max_hold_secs,
        trailing_stop: exit.trailing_stop.clone(),
        signal_invalidation_rules: exit.signal_invalidation_rules.clone(),
        partial_exit_nodes: exit
            .partial_exit_nodes
            .iter()
            .filter(|node| node.sell_pct < Decimal::ONE)
            .cloned()
            .collect(),
        settlement_mode: exit.settlement_mode,
        redeem_policy: exit.redeem_policy,
        manual_review_at: exit.manual_review_at,
        entry_reference_price,
        entry_composite_score: rec.composite_score,
    }
}

/// Validate an operator downscale against the frozen entry. Returns the narrowed
/// `(entry_override, allocated_override)`, or `(None, None)` for a pure approval.
/// Widening size, raising the limit, or downscaling when disabled are rejected.
fn resolve_downscale(
    command: &ApproveIntentCommand,
    frozen: &EntryOrderSpec,
    allow_downscale: bool,
) -> Result<(Option<EntryOrderSpec>, Option<Usd>), ExecutionError> {
    if command.override_shares.is_none()
        && command.override_limit_price.is_none()
        && command.max_allowed_usd.is_none()
    {
        return Ok((None, None));
    }
    if !allow_downscale {
        return Err(ExecutionError::IntentDenied {
            reason: "size reduction is disabled by config".to_owned(),
        });
    }

    let new_shares = command.override_shares.unwrap_or(frozen.shares);
    let new_limit = command.override_limit_price.unwrap_or(frozen.limit_price);

    if new_shares > frozen.shares {
        return Err(ExecutionError::IntentDenied {
            reason: "approval cannot increase order size".to_owned(),
        });
    }
    if new_limit > frozen.limit_price {
        return Err(ExecutionError::IntentDenied {
            reason: "approval cannot raise the limit price above the recommendation".to_owned(),
        });
    }

    let new_notional = new_shares * new_limit;
    if let Some(cap) = command.max_allowed_usd
        && new_notional > cap
    {
        return Err(ExecutionError::IntentDenied {
            reason: format!("approved notional {new_notional} exceeds max_allowed_usd {cap}"),
        });
    }

    let mut entry = frozen.clone();
    entry.shares = new_shares;
    entry.limit_price = new_limit;
    Ok((Some(entry), Some(new_notional)))
}

/// Combine the approval reason with the optional operator override note.
fn approval_reason(command: &ApproveIntentCommand) -> String {
    command.override_note.as_ref().map_or_else(
        || command.reason.clone(),
        |note| format!("{}; note: {note}", command.reason),
    )
}

/// Build the WORM operation-log row written inside a background-origin intent
/// transition transaction (sweep expiry / report-cascade invalidation).
fn intent_operation_log(action: &str, intent: &OrderIntentInfo, reason: &str) -> NewOperationLog {
    NewOperationLog {
        id: OperationLogId::from_v7(),
        request_id: format!("quant-intent:{action}:{}", intent.order_intent_id),
        actor_user_id: None,
        actor_username: Some("system".to_owned()),
        acting_role: Some("intent_lifecycle".to_owned()),
        category: OperationCategory::Governance,
        action: action.to_owned(),
        resource_type: Some(ResourceType::OrderIntent),
        resource_id: Some(intent.order_intent_id.to_string()),
        http_method: "SYSTEM".to_owned(),
        http_path: format!("/system/quant/intent/{}/{action}", intent.order_intent_id),
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

#[cfg(test)]
mod tests {
    use super::{
        project_entry_order_spec, project_exit_policy_spec, resolve_downscale, resolve_policy,
    };
    use crate::execution::mode_gate::IntentPolicyDecision;
    use chrono::{Duration, TimeZone, Utc};
    use quant_pivot_error::execution::ExecutionError;
    use quant_pivot_models::{
        domain::{ApproveIntentCommand, RecommendationInfo, evaluate_intent_approval_invalidation},
        enums::{
            common::{OrderType, Side},
            execution::ApprovalInvalidation,
            quant::{
                ApprovalStatus, OrderIntentStatus, OutcomeSide, RecommendationReportStatus,
                ReportKind,
            },
        },
        runtime_config::EntryOrderPolicy,
        types::{
            Bps, ContentHash, EntryOrderSpec, OrderIntentId, Price, RecommendationId,
            RecommendationReportId, RuntimeConfigVersionId, Shares, TokenId, Usd,
        },
    };
    use quant_pivot_test_support::report_fixtures;
    use rust_decimal_macros::dec;
    use std::time::Duration as StdDuration;
    use uuid::Uuid;

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
            limit_price: Price::new(dec!(0.60)),
            shares: Shares::new(dec!(100)),
            max_slippage_bps: Bps::new(dec!(50)),
            valid_until: Utc::now(),
        }
    }

    fn approve_command() -> ApproveIntentCommand {
        ApproveIntentCommand {
            order_intent_id: OrderIntentId::from_v7(),
            operator_id: Uuid::nil(),
            acting_role: "operator".to_owned(),
            reason: "ok".to_owned(),
            override_shares: None,
            override_limit_price: None,
            max_allowed_usd: None,
            override_note: None,
        }
    }

    #[test]
    fn entry_projection_is_gtd_for_resting_limit() {
        let rec = rec();
        let policy = EntryOrderPolicy {
            allow_market_orders: false,
            ..EntryOrderPolicy::default()
        };
        let spec = project_entry_order_spec(&rec, &policy).expect("projection");
        assert_eq!(spec.token_id, rec.token_id);
        assert_eq!(spec.side, Side::Buy);
        assert_eq!(spec.shares, rec.sizing_plan.suggested_shares);
        assert!(matches!(spec.order_type, OrderType::Gtd { .. }));
    }

    #[test]
    fn entry_projection_is_fok_when_market_orders_allowed() {
        let policy = EntryOrderPolicy {
            allow_market_orders: true,
            ..EntryOrderPolicy::default()
        };
        let spec = project_entry_order_spec(&rec(), &policy).expect("projection");
        assert_eq!(spec.order_type, OrderType::Fok);
    }

    #[test]
    fn exit_projection_drops_full_exit_nodes() {
        let exit = project_exit_policy_spec(&rec());
        assert_eq!(exit.take_profit_price, rec().exit_plan.take_profit_price);
        // Fixture has no partial nodes; full-exit nodes (sell_pct == 1) are never carried.
        assert!(exit.partial_exit_nodes.iter().all(|n| n.sell_pct < dec!(1)));
    }

    #[test]
    fn downscale_noop_without_overrides() {
        let (entry, alloc) =
            resolve_downscale(&approve_command(), &frozen_entry(), true).expect("noop");
        assert!(entry.is_none());
        assert!(alloc.is_none());
    }

    #[test]
    fn downscale_reduces_shares_and_notional() {
        let mut cmd = approve_command();
        cmd.override_shares = Some(Shares::new(dec!(50)));
        let (entry, alloc) = resolve_downscale(&cmd, &frozen_entry(), true).expect("downscale");
        let entry = entry.expect("entry override");
        assert_eq!(entry.shares, Shares::new(dec!(50)));
        assert_eq!(alloc, Some(Usd::new(dec!(30)))); // 50 * 0.60
    }

    #[test]
    fn downscale_rejects_widening_size() {
        let mut cmd = approve_command();
        cmd.override_shares = Some(Shares::new(dec!(200)));
        assert!(resolve_downscale(&cmd, &frozen_entry(), true).is_err());
    }

    #[test]
    fn downscale_rejects_raising_limit() {
        let mut cmd = approve_command();
        cmd.override_limit_price = Some(Price::new(dec!(0.70)));
        assert!(resolve_downscale(&cmd, &frozen_entry(), true).is_err());
    }

    #[test]
    fn downscale_rejected_when_disabled() {
        let mut cmd = approve_command();
        cmd.override_shares = Some(Shares::new(dec!(50)));
        assert!(resolve_downscale(&cmd, &frozen_entry(), false).is_err());
    }

    #[test]
    fn downscale_rejects_exceeding_max_allowed() {
        let mut cmd = approve_command();
        cmd.override_shares = Some(Shares::new(dec!(50)));
        cmd.max_allowed_usd = Some(Usd::new(dec!(10))); // 50 * 0.60 = 30 > 10
        assert!(resolve_downscale(&cmd, &frozen_entry(), true).is_err());
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
        let version = RuntimeConfigVersionId::from_v7();
        let hash = rec.risk_envelope.envelope_hash.clone();
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
        let version = RuntimeConfigVersionId::from_v7();
        let hash = rec.risk_envelope.envelope_hash.clone();

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
        let other_version = RuntimeConfigVersionId::from_v7();
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
        assert_ne!(stored_hash, rec.risk_envelope.envelope_hash);
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
        use crate::execution::intent_service::recheck_return_model_calibrated;
        use async_trait::async_trait;
        use chrono::Utc;
        use quant_pivot_error::storage::StorageError;
        use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
        use quant_pivot_models::{
            domain::{
                ModelSpecInfo, ModelSpecListQuery, ModelVersionInfo, ModelVersionListQuery,
                NewModelSpec, NewModelVersion, Paginated,
            },
            enums::quant::{DownsideSource, PublicationStatus},
            types::{CalibrationArtifactId, ContentHash, ModelSpecId, ModelVersionId},
        };
        use quant_pivot_repository::traits::ModelRegistryRepository;
        use quant_pivot_research::{
            artifact::{ArtifactStore, LocalArtifactStore},
            factors::names::LIQUIDITY_DEPTH,
            model::{
                CalibratedReturnModel, CalibrationArtifactLoader, FactorWeight, ModelArtifact,
                ModelArtifactHeader, ModelFamily, MonotoneMapping, ReliabilityReport,
                ResolvedCalibration, ReturnModelSpec, ScoreMultiplierSpec,
                SubstitutionConfidenceRules, WeightedFactorModelArtifact,
            },
        };
        use std::sync::Arc;

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
            async fn list_published_for_spec(
                &self,
                _model_spec_id: &ModelSpecId,
            ) -> Result<Vec<ModelVersionInfo>, StorageError> {
                unimplemented!("not exercised by the calibration recheck tests")
            }
            async fn publish_model_version(
                &self,
                _model_version_id: &ModelVersionId,
            ) -> Result<ModelVersionInfo, StorageError> {
                unimplemented!("not exercised by the calibration recheck tests")
            }
            async fn retire_model_version(
                &self,
                _model_version_id: &ModelVersionId,
            ) -> Result<ModelVersionInfo, StorageError> {
                unimplemented!("not exercised by the calibration recheck tests")
            }
            async fn promote_model_to_shadow(
                &self,
                _model_version_id: &ModelVersionId,
            ) -> Result<ModelVersionInfo, StorageError> {
                unimplemented!("not exercised by the calibration recheck tests")
            }
            async fn restore_model_version(
                &self,
                _model_version_id: &ModelVersionId,
            ) -> Result<ModelVersionInfo, StorageError> {
                unimplemented!("not exercised by the calibration recheck tests")
            }
            async fn set_quality_gate_report(
                &self,
                _model_version_id: &ModelVersionId,
                _quality_gate_report: serde_json::Value,
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
            use std::sync::atomic::{AtomicU64, Ordering};
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let root = std::env::temp_dir().join(format!(
                "qp_intent_calibration_recheck_test_{}_{}_{}",
                std::process::id(),
                Utc::now().timestamp_nanos_opt().unwrap_or_default(),
                COUNTER.fetch_add(1, Ordering::Relaxed)
            ));
            Arc::new(LocalArtifactStore::new(root))
        }

        fn header(model_version_id: ModelVersionId) -> ModelArtifactHeader {
            ModelArtifactHeader {
                model_version_id,
                model_family: ModelFamily::WeightedFactor,
                feature_schema_hash: ContentHash::parse(format!("blake3:{}", "1".repeat(64)))
                    .expect("hash"),
                factor_schema_hash: ContentHash::parse(format!("blake3:{}", "2".repeat(64)))
                    .expect("hash"),
            }
        }

        fn weighted_artifact(
            model_version_id: ModelVersionId,
            return_model: ReturnModelSpec,
        ) -> ModelArtifact {
            ModelArtifact::WeightedFactor(Box::new(WeightedFactorModelArtifact {
                header: header(model_version_id),
                weights: vec![FactorWeight {
                    factor: LIQUIDITY_DEPTH,
                    weight: rust_decimal::Decimal::ONE,
                }],
                prediction_horizon_secs: 86_400,
                multipliers: ScoreMultiplierSpec::conservative(),
                substitution_confidence_rules: SubstitutionConfidenceRules::conservative(),
                return_model,
                required_features: Vec::new(),
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
            let version = ModelVersionInfo {
                model_version_id,
                model_spec_id: ModelSpecId::from_v7(),
                version: 1,
                artifact_hash: digest,
                training_dataset_id: None,
                metrics_json: serde_json::json!({}),
                quality_gate_report: serde_json::json!({}),
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
