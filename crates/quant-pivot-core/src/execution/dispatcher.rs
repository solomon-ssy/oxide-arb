//! Execution dispatcher — the single bridge from an admitted intent to a real
//! venue order (Phase 05.4).
//!
//! `submit_if_admitted` is the only path that signs and submits money. It is
//! **claim-first**: a short row-locked transaction moves the intent
//! `Approved`/`ApprovedByPolicy -> AdmissionPending` (the double-submit guard),
//! then admission is evaluated against the *pre-claim* approval snapshot, then —
//! on `Allow` — the order is write-ahead persisted (`Submitted`) with capital
//! locked, the venue is called **outside any DB lock**, and the result is
//! settled in one transaction. Unconfirmed venue responses become `Ambiguous`
//! (capital held, reconciled) — never silently failed.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{
    QuantError, QuantResult, execution::ExecutionError, storage::StorageError,
};
use quant_pivot_models::{
    domain::{
        CapitalSettlement, ExecutionOrderInfo, ExecutionSubmitPort, IntentEventKind,
        NewExecutionOrder, NewReconciliation, OrderIntentInfo, PositionFill, RecommendationInfo,
        SubmissionLedgerWrite,
    },
    enums::{
        clickhouse::ChQuantLedgerEventKind,
        common::OrderType,
        execution::{
            AdmissionOutcome, ExecutionOrderPhase, OrderTypeKind, ReconciliationEvidenceKind,
        },
        quant::{AccountSource, ExecutionOrderState, OrderIntentStatus},
    },
    types::{
        EntryOrderSpec, ExecutionOrderId, FeatureParityStateId, OrderIntentId, Price,
        ReconciliationEvidence, ReconciliationEvidenceChain, ReconciliationId,
    },
};
use quant_pivot_repository::traits::{ExecutionSubmissionRepository, OrderIntentRepository};

use crate::{
    execution::{
        admission::{AdmissionDecision, AdmissionInputBuilder, ExecutionAdmissionEngine},
        breaker::ExecutionBreaker,
        intent_lifecycle::IntentLifecyclePublisher,
        order_client::{PolymarketOrderClient, VenueOrder, VenueOutcome, VenueSubmitResult},
    },
    observability::metrics_hub::MetricsHub,
    observability::{
        execution_fact_writer::ExecutionEventWriter,
        ledger_fact_projection::project_execution_event,
    },
    service::feature_integrity::FeatureParityGatePort,
};

/// Collaborators for the core execution dispatcher.
pub struct ExecutionDispatcherDeps {
    pub intents: Arc<dyn OrderIntentRepository>,
    pub submission: Arc<dyn ExecutionSubmissionRepository>,
    pub admission_builder: Arc<AdmissionInputBuilder>,
    pub admission: Arc<dyn ExecutionAdmissionEngine>,
    pub order_client: Arc<dyn PolymarketOrderClient>,
    pub breaker: Arc<ExecutionBreaker>,
    pub metrics: Arc<MetricsHub>,
    pub execution_events: Arc<ExecutionEventWriter>,
    /// Fans out `quant.intent` lifecycle events as the venue truth settles.
    pub intent_lifecycle: Arc<IntentLifecyclePublisher>,
    /// Checked before claim, then captured as an exact clear generation and
    /// revalidated under the parity advisory lock inside the write-ahead
    /// transaction so a newly opened latch cannot race entry.
    pub feature_parity_gate: Arc<dyn FeatureParityGatePort>,
}

/// Production [`ExecutionSubmitPort`]: the single bridge from an admitted intent
/// to a signed venue order.
pub struct CoreExecutionDispatcher {
    deps: ExecutionDispatcherDeps,
}

impl CoreExecutionDispatcher {
    #[must_use]
    pub const fn new(deps: ExecutionDispatcherDeps) -> Self {
        Self { deps }
    }
}

#[async_trait]
impl ExecutionSubmitPort for CoreExecutionDispatcher {
    async fn submit_if_admitted(
        &self,
        intent_id: &OrderIntentId,
    ) -> QuantResult<ExecutionOrderInfo> {
        let now = Utc::now();
        self.deps
            .feature_parity_gate
            .ensure_clear("entry submission pre-claim")
            .await?;

        // 1. Pre-check: friendly, non-mutating submittability gate.
        let intent = self
            .deps
            .intents
            .find_by_id(intent_id)
            .await?
            .ok_or_else(|| ExecutionError::NotSubmittable {
                intent_id: intent_id.to_string(),
                state: "not_found".to_owned(),
            })?;
        ensure_submittable(&intent, now)?;
        // Pre-claim approval status; the post-settle lifecycle event is emitted
        // relative to this so a resting `Submitted` or an immediate `Filled`
        // fans out on `quant.intent`.
        let prior_status = intent.status;

        // 2. Claim (atomic double-submit guard): Approved -> AdmissionPending. A
        //    lost race / state change surfaces as a non-submittable conflict.
        if let Err(error) = self
            .deps
            .submission
            .claim_for_submission(intent_id, now)
            .await
        {
            return Err(submission_storage_failure(intent_id, error));
        }

        // 3–7. Admission gate, write-ahead, venue call, and ledger settle.
        let recommendation = Box::pin(self.evaluate_admission(intent_id, &intent, now)).await?;
        let feature_parity_state_id = match self
            .deps
            .feature_parity_gate
            .commit_state_id("entry submission commit")
            .await
        {
            Ok(state_id) => state_id,
            Err(error) => return Err(self.revert_and(intent_id, error).await),
        };
        self.venue_submit_and_settle(
            intent_id,
            &intent,
            &recommendation,
            prior_status,
            &feature_parity_state_id,
            now,
        )
        .await
    }
}

impl CoreExecutionDispatcher {
    /// Build admission input from the pre-claim snapshot, evaluate, and map
    /// `Deny`/`Defer` into terminal errors (reverting the claim on defer).
    async fn evaluate_admission(
        &self,
        intent_id: &OrderIntentId,
        intent: &OrderIntentInfo,
        now: DateTime<Utc>,
    ) -> QuantResult<RecommendationInfo> {
        let input = match self.deps.admission_builder.build(intent, now).await {
            Ok(input) => input,
            Err(error) => return Err(self.revert_and(intent_id, error).await),
        };
        let recommendation = input.recommendation.clone();
        let decision = match self.deps.admission.evaluate(input).await {
            Ok(decision) => decision,
            Err(error) => return Err(self.revert_and(intent_id, error).await),
        };

        match decision.outcome {
            AdmissionOutcome::Allow => Ok(recommendation),
            AdmissionOutcome::Deny => {
                let reason = decision
                    .denial_reason
                    .clone()
                    .unwrap_or_else(|| "admission denied".to_owned());
                let rejected = self
                    .deps
                    .submission
                    .reject_admission(intent_id, reason.clone(), denial_trace_ref(&decision))
                    .await?;
                self.deps.intent_lifecycle.publish(
                    &rejected,
                    IntentEventKind::AdmissionRejected,
                    now,
                );
                Err(ExecutionError::AdmissionDenied { reason }.into())
            }
            AdmissionOutcome::Defer => {
                self.deps.submission.revert_claim(intent_id).await?;
                Err(ExecutionError::AdmissionDeferred {
                    reason: defer_reason(&decision),
                }
                .into())
            }
        }
    }

    /// Write-ahead persist, submit to the venue (no DB lock), observe breaker
    /// health, settle the ledger, and fan out the post-settle intent lifecycle.
    async fn venue_submit_and_settle(
        &self,
        intent_id: &OrderIntentId,
        intent: &OrderIntentInfo,
        recommendation: &RecommendationInfo,
        prior_status: OrderIntentStatus,
        feature_parity_state_id: &FeatureParityStateId,
        now: DateTime<Utc>,
    ) -> QuantResult<ExecutionOrderInfo> {
        let spec = intent.entry_order_json.clone();
        let new_order = build_new_execution_order(intent, recommendation, &spec)?;
        let execution_order = match self
            .deps
            .submission
            .create_entry_order_and_lock_capital(new_order, feature_parity_state_id)
            .await
        {
            Ok(order) => order,
            Err(error) => return Err(self.revert_and(intent_id, error.into()).await),
        };
        self.deps.metrics.inc_execution_order_submitted();
        self.deps.execution_events.write(project_execution_event(
            &execution_order,
            recommendation.recommendation_id.clone(),
            ChQuantLedgerEventKind::Submitted,
            now,
        ));

        let result = self
            .deps
            .order_client
            .submit(build_venue_order(recommendation, &spec))
            .await
            .with_order_type_semantics(&spec.order_type);

        let venue_ok = result.outcome != VenueOutcome::Ambiguous;
        self.deps
            .breaker
            .observe_venue(venue_ok, result.detail.as_deref().unwrap_or("venue submit"))
            .await;

        if matches!(
            result.outcome,
            VenueOutcome::Filled | VenueOutcome::PartiallyFilled
        ) {
            self.deps.metrics.inc_execution_fill();
        }
        let write = build_ledger_write(&result, recommendation, &spec, &execution_order);
        let recorded = self
            .deps
            .submission
            .record_submission_result(&execution_order.execution_order_id, write)
            .await?;
        self.deps.execution_events.write(project_execution_event(
            &recorded,
            recommendation.recommendation_id.clone(),
            ChQuantLedgerEventKind::SubmissionResult,
            now,
        ));
        if let Some(settled) = self.deps.intents.find_by_id(intent_id).await? {
            self.deps
                .intent_lifecycle
                .publish_transition(prior_status, &settled, now);
        }
        Ok(recorded)
    }

    /// Best-effort release the claim (`AdmissionPending -> Approved`) before
    /// propagating a pre-submission error, so the intent stays retryable.
    async fn revert_and(&self, intent_id: &OrderIntentId, error: QuantError) -> QuantError {
        if let Err(revert_error) = self.deps.submission.revert_claim(intent_id).await {
            tracing::error!(%revert_error, %intent_id, "failed to revert admission claim");
        }
        error
    }
}

fn ensure_submittable(intent: &OrderIntentInfo, now: DateTime<Utc>) -> Result<(), ExecutionError> {
    if !matches!(
        intent.status,
        OrderIntentStatus::Approved | OrderIntentStatus::ApprovedByPolicy
    ) {
        return Err(ExecutionError::NotSubmittable {
            intent_id: intent.order_intent_id.to_string(),
            state: intent.status.as_str().to_owned(),
        });
    }
    if intent.expires_at <= now {
        return Err(ExecutionError::NotSubmittable {
            intent_id: intent.order_intent_id.to_string(),
            state: "expired".to_owned(),
        });
    }
    Ok(())
}

/// Compact, audit-friendly reference of the non-passing admission checks.
fn denial_trace_ref(decision: &AdmissionDecision) -> Option<String> {
    let parts: Vec<String> = decision
        .trace
        .iter()
        .filter(|trace| trace.outcome != AdmissionOutcome::Allow)
        .map(|trace| format!("{:?}:{}", trace.check, trace.detail))
        .collect();
    (!parts.is_empty()).then(|| parts.join("; "))
}

fn defer_reason(decision: &AdmissionDecision) -> String {
    decision
        .trace
        .iter()
        .rev()
        .find(|trace| trace.outcome == AdmissionOutcome::Defer)
        .map_or_else(
            || "admission deferred".to_owned(),
            |trace| trace.detail.clone(),
        )
}

pub(crate) const fn order_type_kind(order_type: &OrderType) -> OrderTypeKind {
    match order_type {
        OrderType::Fok => OrderTypeKind::Fok,
        OrderType::Fak => OrderTypeKind::Fak,
        OrderType::Gtc => OrderTypeKind::Gtc,
        OrderType::Gtd { .. } => OrderTypeKind::Gtd,
    }
}

pub(crate) fn gtd_expiration_at(
    order_type: &OrderType,
) -> Result<Option<DateTime<Utc>>, ExecutionError> {
    match order_type {
        OrderType::Gtd { expiration } => {
            let seconds =
                i64::try_from(*expiration).map_err(|error| ExecutionError::TimeConversion {
                    field: "order_type.gtd.expiration",
                    value: expiration.to_string(),
                    detail: error.to_string(),
                })?;
            DateTime::from_timestamp(seconds, 0)
                .map(Some)
                .ok_or_else(|| ExecutionError::TimeConversion {
                    field: "order_type.gtd.expiration",
                    value: expiration.to_string(),
                    detail: "timestamp is outside the chrono range".to_owned(),
                })
        }
        OrderType::Fok | OrderType::Fak | OrderType::Gtc => Ok(None),
    }
}

/// Build the write-ahead execution-order row (`state = Submitted`).
fn build_new_execution_order(
    intent: &OrderIntentInfo,
    recommendation: &RecommendationInfo,
    spec: &EntryOrderSpec,
) -> Result<NewExecutionOrder, ExecutionError> {
    Ok(NewExecutionOrder {
        execution_order_id: ExecutionOrderId::from_v7(),
        order_intent_id: intent.order_intent_id.clone(),
        order_phase: ExecutionOrderPhase::Entry,
        market_id: recommendation.market_id.clone(),
        token_id: spec.token_id.clone(),
        side: spec.side,
        order_type: order_type_kind(&spec.order_type),
        price: spec.limit_price,
        shares: spec.projected_shares(),
        cost_usd: spec.notional(),
        venue_order_id: None,
        venue_status: None,
        state: ExecutionOrderState::Submitted,
        submitted_at: None,
        filled_at: None,
        cancelled_at: None,
        gtd_expiration_at: gtd_expiration_at(&spec.order_type)?,
        error_message: None,
    })
}

fn build_venue_order(recommendation: &RecommendationInfo, spec: &EntryOrderSpec) -> VenueOrder {
    VenueOrder {
        market_id: recommendation.market_id.clone(),
        token_id: spec.token_id.clone(),
        side: spec.side,
        price: spec.limit_price,
        amount: spec.amount,
        order_type: spec.order_type,
        post_only: spec.post_only,
        category: recommendation.identity.category,
    }
}

fn fill_avg_price(result: &VenueSubmitResult, spec: &EntryOrderSpec) -> Price {
    result.avg_fill_price.unwrap_or(spec.limit_price)
}

fn position_fill(
    result: &VenueSubmitResult,
    recommendation: &RecommendationInfo,
    spec: &EntryOrderSpec,
    order_intent_id: &OrderIntentId,
) -> PositionFill {
    let price = fill_avg_price(result, spec);
    let fill_cost = result.filled_shares * price;
    PositionFill {
        order_intent_id: order_intent_id.clone(),
        token_id: spec.token_id.clone(),
        market_id: recommendation.market_id.clone(),
        event_id: Some(recommendation.event_id.clone()),
        category: recommendation.identity.category,
        side: recommendation.outcome_side,
        shares: result.filled_shares,
        price,
        cost_usd: fill_cost + result.fee_paid,
        filled_at: result.responded_at,
        source: AccountSource::Polymarket,
    }
}

fn reconciliation_row(
    result: &VenueSubmitResult,
    execution_order: &ExecutionOrderInfo,
    outcome: VenueOutcome,
) -> NewReconciliation {
    let detail = format!(
        "venue outcome {outcome:?}; order_id={:?}; filled={}",
        result.venue_order_id, result.filled_shares
    );
    let evidence = ReconciliationEvidenceChain(vec![ReconciliationEvidence {
        kind: ReconciliationEvidenceKind::ClobOrderStatus,
        observed_at: result.responded_at,
        detail,
        venue_ref: result.venue_order_id.as_ref().map(ToString::to_string),
        shares: Some(result.filled_shares),
        price: result.avg_fill_price,
    }]);
    NewReconciliation {
        reconciliation_id: ReconciliationId::from_v7(),
        execution_order_id: execution_order.execution_order_id.clone(),
        order_intent_id: execution_order.order_intent_id.clone(),
        result: outcome.reconciliation_result(),
        evidence_json: evidence,
        venue_filled_shares: Some(result.filled_shares),
        venue_avg_price: result.avg_fill_price,
        discrepancy_usd: None,
        resolved_by: None,
        resolved_at: None,
    }
}

/// Translate a venue outcome into the atomic ledger write applied in
/// `record_submission_result` (entry state + capital + position + intent + recon).
fn build_ledger_write(
    result: &VenueSubmitResult,
    recommendation: &RecommendationInfo,
    spec: &EntryOrderSpec,
    execution_order: &ExecutionOrderInfo,
) -> SubmissionLedgerWrite {
    let outcome = result.outcome;
    let fill_cost = result.filled_shares * fill_avg_price(result, spec);
    let total_spent = fill_cost + result.fee_paid;
    let common_venue_status = outcome.venue_order_status();

    match outcome {
        VenueOutcome::Filled => SubmissionLedgerWrite {
            state: ExecutionOrderState::Filled,
            intent_status: OrderIntentStatus::Filled,
            venue_order_id: result.venue_order_id.clone(),
            venue_status: common_venue_status,
            submitted_at: result.submitted_at,
            filled_at: Some(result.responded_at),
            cancelled_at: None,
            error_message: None,
            capital: CapitalSettlement::SettleFull {
                spent_usd: total_spent,
            },
            fill: Some(position_fill(
                result,
                recommendation,
                spec,
                &execution_order.order_intent_id,
            )),
            reconciliation: Some(reconciliation_row(result, execution_order, outcome)),
        },
        VenueOutcome::PartiallyFilled => SubmissionLedgerWrite {
            state: ExecutionOrderState::PartiallyFilled,
            intent_status: OrderIntentStatus::PartiallyFilled,
            venue_order_id: result.venue_order_id.clone(),
            venue_status: common_venue_status,
            submitted_at: result.submitted_at,
            filled_at: Some(result.responded_at),
            cancelled_at: None,
            error_message: None,
            capital: CapitalSettlement::SettlePartial {
                spent_usd: total_spent,
            },
            fill: Some(position_fill(
                result,
                recommendation,
                spec,
                &execution_order.order_intent_id,
            )),
            reconciliation: Some(reconciliation_row(result, execution_order, outcome)),
        },
        VenueOutcome::Open => SubmissionLedgerWrite {
            // Resting limit order: stays `Submitted`, capital stays locked, no
            // recon (05.5 polls open orders). Only venue metadata is recorded.
            state: ExecutionOrderState::Submitted,
            intent_status: OrderIntentStatus::Submitted,
            venue_order_id: result.venue_order_id.clone(),
            venue_status: common_venue_status,
            submitted_at: result.submitted_at,
            filled_at: None,
            cancelled_at: None,
            error_message: None,
            capital: CapitalSettlement::Hold,
            fill: None,
            reconciliation: None,
        },
        VenueOutcome::Rejected => SubmissionLedgerWrite {
            state: ExecutionOrderState::Failed,
            intent_status: OrderIntentStatus::Failed,
            venue_order_id: result.venue_order_id.clone(),
            venue_status: common_venue_status,
            submitted_at: result.submitted_at,
            filled_at: None,
            cancelled_at: None,
            error_message: result.detail.clone(),
            capital: CapitalSettlement::ReleaseAll,
            fill: None,
            reconciliation: Some(reconciliation_row(result, execution_order, outcome)),
        },
        VenueOutcome::Cancelled | VenueOutcome::Expired => SubmissionLedgerWrite {
            state: ExecutionOrderState::Cancelled,
            intent_status: OrderIntentStatus::Cancelled,
            venue_order_id: result.venue_order_id.clone(),
            venue_status: common_venue_status,
            submitted_at: result.submitted_at,
            filled_at: None,
            cancelled_at: Some(result.responded_at),
            error_message: result.detail.clone(),
            capital: CapitalSettlement::ReleaseAll,
            fill: None,
            reconciliation: Some(reconciliation_row(result, execution_order, outcome)),
        },
        VenueOutcome::Ambiguous => SubmissionLedgerWrite {
            // Most dangerous state: unconfirmed. Hold capital, do not fill, must
            // reconcile. Intent stays `Submitted` until venue truth is known.
            state: ExecutionOrderState::Ambiguous,
            intent_status: OrderIntentStatus::Submitted,
            venue_order_id: result.venue_order_id.clone(),
            venue_status: None,
            submitted_at: result.submitted_at,
            filled_at: None,
            cancelled_at: None,
            error_message: result.detail.clone(),
            capital: CapitalSettlement::Hold,
            fill: None,
            reconciliation: Some(reconciliation_row(result, execution_order, outcome)),
        },
    }
}

fn submission_storage_failure(intent_id: &OrderIntentId, error: StorageError) -> QuantError {
    let intent_id = intent_id.to_string();
    let not_submittable = |state: String| {
        ExecutionError::NotSubmittable {
            intent_id: intent_id.clone(),
            state,
        }
        .into()
    };

    match error {
        StorageError::NotFound { id, .. } => not_submittable(id),
        StorageError::IllegalTransition {
            entity, from, to, ..
        } => not_submittable(format!("illegal transition on {entity}: {from} -> {to}")),
        StorageError::StateConflict { entity, detail, .. } => {
            not_submittable(format!("{entity}: {detail}"))
        }
        StorageError::InvariantViolation { detail, .. } => not_submittable(detail),
        StorageError::Duplicate { entity, key } => {
            not_submittable(format!("{entity} already exists: {key}"))
        }
        other => other.into(),
    }
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::enums::common::OrderType;

    use super::gtd_expiration_at;

    #[test]
    fn gtd_expiration_rejects_unrepresentable_timestamp() {
        assert!(
            gtd_expiration_at(&OrderType::Gtd {
                expiration: u64::MAX,
            })
            .is_err()
        );
    }
}
