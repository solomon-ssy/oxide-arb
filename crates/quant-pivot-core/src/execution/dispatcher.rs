//! Execution dispatcher — the single bridge from an admitted intent to a real
//! venue order.
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
        ports::ExecutionSubmitPort,
        quant::{
            CapitalSettlement, EntryConditionClaim, EntryConditionInstanceInfo, ExecutionOrderInfo,
            NewExecutionOrder, NewReconciliation, OrderIntentInfo, PositionFill,
            RecommendationInfo, SubmissionLedgerWrite,
        },
        runtime::{CoreEvent, EntryConditionLifecycleEvent, IntentEventKind},
    },
    enums::{
        clickhouse::ChQuantLedgerEventKind,
        common::OrderType,
        execution::{
            AdmissionOutcome, ExecutionOrderPhase, ReconciliationEvidenceKind, ReconciliationResult,
        },
        quant::{AccountSource, ExecutionOrderState, OrderIntentStatus},
    },
    hashing::CanonicalDigest,
    types::{
        EntryOrderSpec, ExecutionAccountId, ExecutionOrderId, FeatureParityStateId, OrderIntentId,
        PreparedVenueOrder, ReconciliationEvidence, ReconciliationEvidenceChain, ReconciliationId,
        Usd,
    },
};
use quant_pivot_repository::traits::{
    EntryConditionRepository, ExecutionSubmissionRepository, OrderIntentRepository,
};

use crate::{
    execution::{
        admission::{AdmissionDecision, AdmissionInputBuilder, ExecutionAdmissionEngine},
        breaker::ExecutionBreaker,
        execution_order_lifecycle::ExecutionOrderLifecyclePublisher,
        intent_lifecycle::IntentLifecyclePublisher,
        order_client::{PolymarketOrderClient, VenueOrder, VenueOutcome, VenueSubmitResult},
    },
    observability::{
        execution_fact_writer::ExecutionEventWriter,
        ledger_fact_projection::project_execution_event, metrics_hub::MetricsHub,
    },
    service::feature_integrity::FeatureParityGatePort,
};

/// Collaborators for the core execution dispatcher.
pub struct ExecutionDispatcherDeps {
    pub intents: Arc<dyn OrderIntentRepository>,
    pub submission: Arc<dyn ExecutionSubmissionRepository>,
    pub conditions: Arc<dyn EntryConditionRepository>,
    pub admission_builder: Arc<AdmissionInputBuilder>,
    pub admission: Arc<dyn ExecutionAdmissionEngine>,
    pub order_client: Arc<dyn PolymarketOrderClient>,
    pub breaker: Arc<ExecutionBreaker>,
    pub metrics: Arc<MetricsHub>,
    pub execution_events: Arc<ExecutionEventWriter>,
    /// Fans out `quant.intent` lifecycle events as the venue truth settles.
    pub intent_lifecycle: Arc<IntentLifecyclePublisher>,
    /// Fans out committed execution-order creation and venue-result transitions.
    pub order_lifecycle: Arc<ExecutionOrderLifecyclePublisher>,
    /// Checked before claim, then captured as an exact clear generation and
    /// revalidated under the parity advisory lock inside the write-ahead
    /// transaction so a newly opened latch cannot race entry.
    pub feature_parity_gate: Arc<dyn FeatureParityGatePort>,
}

struct VenueSubmissionContext<'a> {
    intent_id: &'a OrderIntentId,
    intent: &'a OrderIntentInfo,
    recommendation: &'a RecommendationInfo,
    prepared_order: &'a PreparedVenueOrder,
    prior_status: OrderIntentStatus,
    feature_parity_state_id: &'a FeatureParityStateId,
    now: DateTime<Utc>,
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

        // 2. Evaluate against a read-only frozen snapshot. The exact condition
        // revision and admission fingerprint are revalidated by the atomic
        // claim below, so a concurrent correction cannot slip through.
        let (recommendation, condition_claim, prepared_order) =
            Box::pin(self.evaluate_admission(intent_id, &intent, now)).await?;
        // 3. Atomically claim intent + condition. There is no observable
        // `AdmissionPending` intent with an unconsumed condition.
        let (_, claimed_condition) = match self
            .deps
            .submission
            .claim_for_submission(condition_claim)
            .await
        {
            Ok(claimed) => claimed,
            Err(error) => return Err(submission_storage_failure(intent_id, error)),
        };
        self.publish_condition(&claimed_condition);
        let feature_parity_state_id = match self
            .deps
            .feature_parity_gate
            .commit_state_id("entry submission commit")
            .await
        {
            Ok(state_id) => state_id,
            Err(error) => return Err(self.revert_and(intent_id, error).await),
        };
        self.venue_submit_and_settle(VenueSubmissionContext {
            intent_id,
            intent: &intent,
            recommendation: &recommendation,
            prepared_order: &prepared_order,
            prior_status,
            feature_parity_state_id: &feature_parity_state_id,
            now,
        })
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
    ) -> QuantResult<(RecommendationInfo, EntryConditionClaim, PreparedVenueOrder)> {
        let input = match Box::pin(self.deps.admission_builder.build(intent, now)).await {
            Ok(input) => input,
            Err(error) => return Err(error),
        };
        let recommendation = input.recommendation.clone();
        let condition = input.condition.clone();
        let prepared_order = input.prepare_entry_order()?;
        let decision = match self.deps.admission.evaluate(input).await {
            Ok(decision) => decision,
            Err(error) => return Err(error),
        };

        match decision.outcome {
            AdmissionOutcome::Allow => {
                let admission_state_version =
                    CanonicalDigest::content_hash_json(&decision.state_version)?;
                Ok((
                    recommendation,
                    EntryConditionClaim {
                        condition_instance_id: condition.condition_instance_id,
                        order_intent_id: *intent_id,
                        artifact_id: condition.artifact_id,
                        artifact_hash: condition.artifact_hash,
                        expected_revision: condition.revision,
                        evaluation_hash: condition.evaluation_hash,
                        input_fingerprint: condition.input_fingerprint,
                        continuity_hash: condition.continuity_hash,
                        admission_state_version,
                        claimed_at: now,
                    },
                    prepared_order,
                ))
            }
            AdmissionOutcome::Deny => {
                let reason = decision
                    .denial_reason
                    .clone()
                    .unwrap_or_else(|| "admission denied".to_owned());
                let rejected = self
                    .deps
                    .submission
                    .reject_admission(intent_id, reason.clone(), decision.denial_trace_ref())
                    .await?;
                self.deps.intent_lifecycle.publish(
                    &rejected,
                    IntentEventKind::AdmissionRejected,
                    now,
                );
                Err(ExecutionError::AdmissionDenied { reason }.into())
            }
            AdmissionOutcome::Defer => Err(ExecutionError::AdmissionDeferred {
                reason: decision.defer_reason(),
            }
            .into()),
        }
    }

    /// Write-ahead persist, submit to the venue (no DB lock), observe breaker
    /// health, settle the ledger, and fan out the post-settle intent lifecycle.
    async fn venue_submit_and_settle(
        &self,
        context: VenueSubmissionContext<'_>,
    ) -> QuantResult<ExecutionOrderInfo> {
        let VenueSubmissionContext {
            intent_id,
            intent,
            recommendation,
            prepared_order,
            prior_status,
            feature_parity_state_id,
            now,
        } = context;
        let spec = intent.entry_order_json.clone();
        let new_order = build_new_execution_order(intent, recommendation, &spec, prepared_order)?;
        let execution_order = match self
            .deps
            .submission
            .create_entry_order(new_order, feature_parity_state_id)
            .await
        {
            Ok(order) => order,
            Err(error) => return Err(self.revert_and(intent_id, error.into()).await),
        };
        self.deps.order_lifecycle.created(&execution_order, now);
        self.deps.metrics.inc_execution_order_submitted();
        self.deps.execution_events.write(project_execution_event(
            &execution_order,
            recommendation.recommendation_id,
            ChQuantLedgerEventKind::Submitted,
            now,
        ));

        let result = self
            .deps
            .order_client
            .submit(build_venue_order(recommendation, prepared_order))
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
        let write = build_ledger_write(
            &result,
            recommendation,
            &spec,
            &execution_order,
            intent.execution_account_id,
        );
        let recorded = self
            .deps
            .submission
            .record_submission_result(&execution_order.execution_order_id, write)
            .await?;
        self.deps
            .order_lifecycle
            .transition(&execution_order, &recorded, now);
        self.deps.execution_events.write(project_execution_event(
            &recorded,
            recommendation.recommendation_id,
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
        match self.deps.submission.revert_claim(intent_id).await {
            Ok(intent) => {
                match self
                    .deps
                    .conditions
                    .find_instance(&intent.condition_instance_id)
                    .await
                {
                    Ok(Some(condition)) => self.publish_condition(&condition),
                    Ok(None) => {}
                    Err(revert_error) => {
                        tracing::error!(%revert_error, %intent_id, "failed to load reverted condition");
                    }
                }
            }
            Err(revert_error) => {
                tracing::error!(%revert_error, %intent_id, "failed to revert admission claim");
            }
        }
        error
    }

    fn publish_condition(&self, condition: &EntryConditionInstanceInfo) {
        self.deps
            .intent_lifecycle
            .publisher()
            .publish(CoreEvent::Condition(EntryConditionLifecycleEvent {
                condition_instance_id: condition.condition_instance_id,
                revision: condition.revision,
                state: condition.state,
                truth: condition.truth_json.clone(),
                evaluation_hash: condition.evaluation_hash,
            }));
    }
}

fn ensure_submittable(intent: &OrderIntentInfo, now: DateTime<Utc>) -> Result<(), ExecutionError> {
    if !matches!(
        intent.status,
        OrderIntentStatus::Approved | OrderIntentStatus::ApprovedByPolicy
    ) {
        return Err(ExecutionError::NotSubmittable {
            intent_id: intent.order_intent_id.to_string(),
            state: intent.status.to_string(),
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

impl AdmissionDecision {
    /// Compact, audit-friendly reference of the non-passing admission checks.
    fn denial_trace_ref(&self) -> Option<String> {
        let parts: Vec<String> = self
            .trace
            .iter()
            .filter(|trace| trace.outcome != AdmissionOutcome::Allow)
            .map(|trace| format!("{:?}:{}", trace.check, trace.detail))
            .collect();
        (!parts.is_empty()).then(|| parts.join("; "))
    }
}

impl AdmissionDecision {
    fn defer_reason(&self) -> String {
        self.trace
            .iter()
            .rev()
            .find(|trace| trace.outcome == AdmissionOutcome::Defer)
            .map_or_else(
                || "admission deferred".to_owned(),
                |trace| trace.detail.clone(),
            )
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
    prepared_order: &PreparedVenueOrder,
) -> Result<NewExecutionOrder, ExecutionError> {
    Ok(NewExecutionOrder {
        execution_order_id: ExecutionOrderId::from_v7(),
        order_intent_id: intent.order_intent_id,
        order_phase: ExecutionOrderPhase::Entry,
        market_id: recommendation.market_id.clone(),
        token_id: spec.token_id.clone(),
        side: spec.side,
        order_type: spec.order_type.into(),
        price: spec.limit_price,
        shares: prepared_order.expected_filled_shares,
        cost_usd: prepared_order
            .cash_budget
            .unwrap_or_else(|| spec.notional()),
        prepared_order_json: prepared_order.clone(),
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

fn build_venue_order(
    recommendation: &RecommendationInfo,
    prepared: &PreparedVenueOrder,
) -> VenueOrder {
    VenueOrder {
        market_id: recommendation.market_id.clone(),
        token_id: prepared.token_id.clone(),
        side: prepared.side,
        price: prepared.worst_price,
        amount: prepared.venue_amount,
        order_type: prepared.order_type,
        post_only: prepared.post_only,
        expected_fee: prepared.expected_fee,
        fee_schedule_hash: prepared.fee_schedule.schedule_hash,
    }
}

fn position_fill(
    result: &VenueSubmitResult,
    recommendation: &RecommendationInfo,
    spec: &EntryOrderSpec,
    order_intent_id: &OrderIntentId,
    execution_account_id: ExecutionAccountId,
) -> PositionFill {
    let price = result.avg_fill_price.unwrap_or(spec.limit_price);
    let fill_cost = result.filled_shares * price;
    PositionFill {
        order_intent_id: *order_intent_id,
        execution_account_id,
        token_id: spec.token_id.clone(),
        market_id: recommendation.market_id.clone(),
        event_id: Some(recommendation.event_id.clone()),
        category: recommendation.identity.category,
        side: recommendation.outcome_side,
        shares: result.filled_shares,
        price,
        cost_usd: fill_cost + result.expected_fee,
        filled_at: result.responded_at,
        source: AccountSource::Polymarket,
    }
}

fn reconciliation_row(
    result: &VenueSubmitResult,
    execution_order: &ExecutionOrderInfo,
    outcome: VenueOutcome,
) -> NewReconciliation {
    let reconciliation_result = outcome.reconciliation_result();
    let (resolved_by, resolved_at) =
        submit_response_resolution(reconciliation_result, result.responded_at);
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
        fee_evidence: Some(result.fee_evidence.clone()),
    }]);
    NewReconciliation {
        reconciliation_id: ReconciliationId::from_v7(),
        execution_order_id: execution_order.execution_order_id,
        order_intent_id: execution_order.order_intent_id,
        result: reconciliation_result,
        evidence_json: evidence,
        venue_filled_shares: Some(result.filled_shares),
        venue_avg_price: result.avg_fill_price,
        expected_cash_delta_usd: Some(Usd::new(
            execution_order.prepared_order_json.total_cash_delta,
        )),
        venue_cash_delta_usd: None,
        realized_pnl_usd: None,
        expected_fee_usd: Some(result.expected_fee),
        observed_fee_usd: result.observed_fee,
        fee_delta_usd: result.observed_fee.map(|fee| fee - result.expected_fee),
        resolved_by,
        resolved_at,
    }
}

/// Freeze terminal truth observed synchronously in the venue submit response.
///
/// `Pending` remains unresolved for the asynchronous reconciliation worker.
/// Every other result is already a terminal source fact and must not be
/// indistinguishable from an ambiguous submission.
pub(super) fn submit_response_resolution(
    result: ReconciliationResult,
    responded_at: DateTime<Utc>,
) -> (Option<String>, Option<DateTime<Utc>>) {
    if result == ReconciliationResult::Pending {
        (None, None)
    } else {
        (Some("venue_submit_response".to_owned()), Some(responded_at))
    }
}

/// Translate a venue outcome into the atomic ledger write applied in
/// `record_submission_result` (entry state + capital + position + intent + recon).
fn build_ledger_write(
    result: &VenueSubmitResult,
    recommendation: &RecommendationInfo,
    spec: &EntryOrderSpec,
    execution_order: &ExecutionOrderInfo,
    execution_account_id: ExecutionAccountId,
) -> SubmissionLedgerWrite {
    let outcome = result.outcome;
    let fill_cost = result.filled_shares * result.avg_fill_price.unwrap_or(spec.limit_price);
    let total_spent = fill_cost + result.expected_fee;
    let common_venue_status = outcome.venue_order_status();
    let identity_refs = result.identity_refs();

    match outcome {
        VenueOutcome::Filled => SubmissionLedgerWrite {
            identity_refs,
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
                execution_account_id,
            )),
            reconciliation: Some(reconciliation_row(result, execution_order, outcome)),
        },
        VenueOutcome::PartiallyFilled => SubmissionLedgerWrite {
            identity_refs,
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
                execution_account_id,
            )),
            reconciliation: Some(reconciliation_row(result, execution_order, outcome)),
        },
        VenueOutcome::Open => SubmissionLedgerWrite {
            identity_refs,
            // Resting limit order: stays `Submitted`, capital stays locked, no
            // Reconciliation polls open orders. Only venue metadata is recorded.
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
            identity_refs,
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
            identity_refs,
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
            identity_refs,
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
    fn gtd_expiration_rejects_timestamp() {
        assert!(
            gtd_expiration_at(&OrderType::Gtd {
                expiration: u64::MAX,
            })
            .is_err()
        );
    }
}
