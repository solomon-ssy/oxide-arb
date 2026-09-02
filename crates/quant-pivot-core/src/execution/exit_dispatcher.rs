//! Exit dispatcher — submits a triggered Sell exit order and settles it in one
//! transaction.
//!
//! Mirrors the entry dispatcher's write-ahead / venue-call / settle shape but
//! for the exit side: it does **not** run the 27-check admission engine (an exit
//! reduces existing exposure; its lightweight gates — kill-switch `allows_auto_exit`,
//! a readable mark — are enforced in [`decide_exit`](super::decide_exit)). The
//! per-intent lot's capital is already `Spent` from entry; a full exit completes
//! it to `Released`, a partial keeps it `Spent`. Realized `PnL` is exact (per-lot
//! average cost, net the venue exit fee). Unconfirmed venue responses become
//! `Ambiguous` (position untouched, recon enqueued) — never silently exited.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use quant_pivot_api::clob::{
    ClobClient, VenueFundingAsset, VenueFundingEvidence, VenueOrderMetadata,
};
use quant_pivot_error::{
    QuantResult,
    execution::ExecutionError,
    storage::{
        StorageError,
        entity::{QUANT_ORDER_INTENT, QUANT_RECOMMENDATION, QUANT_RECOMMENDATION_REPORT},
    },
};
use quant_pivot_models::{
    domain::{
        order::{CanonicalOrderAmounts, OrderRequest, PolymarketOrderRules, VenueOrderRuleError},
        quant::{
            ExecutionOrderInfo, ExitLedgerWrite, NewExecutionOrder, NewReconciliation,
            PositionExit, RecommendationReportInfo, StrategyPositionLot,
        },
    },
    enums::{
        clickhouse::ChQuantLedgerEventKind,
        common::{OrderType, Side},
        execution::{ExecutionOrderPhase, ExitReason, ExitState, ReconciliationEvidenceKind},
        quant::{ExecutionOrderState, FillRequirement},
    },
    hashing::CanonicalDigest,
    types::{
        ClobMarketInfoVersion, EntryMakerRebateTerms, ExecutionOrderId, MarketId, PendingScaleOut,
        PreparedFeeSchedule, PreparedVenueOrder, Price, ReconciliationEvidence,
        ReconciliationEvidenceChain, ReconciliationId, ResearchProfileRef, TokenId, Usd,
        VenueOrderAmount,
    },
};
use quant_pivot_repository::traits::{
    ClobMarketInfoRepository, ExecutionSubmissionRepository, OrderIntentRepository,
    RecommendationReportRepository, RecommendationRepository,
};
use quant_pivot_research::execution_semantics::{
    BookWalkOutcome, LiquidityRole, walk_sell_exact_shares,
};

use crate::{
    execution::{
        admission::pit_fee_schedule,
        breaker::ExecutionBreaker,
        dispatcher::{gtd_expiration_at, submit_response_resolution},
        execution_order_lifecycle::ExecutionOrderLifecyclePublisher,
        exit_monitor::ExitOrderSpec,
        order_client::{PolymarketOrderClient, VenueOrder, VenueOutcome, VenueSubmitResult},
    },
    ingest::book_store::BookStore,
    observability::{
        execution_fact_writer::ExecutionEventWriter,
        ledger_fact_projection::project_execution_event, metrics_hub::MetricsHub,
    },
};

/// A triggered exit to submit for one open position lot.
pub struct ExitSubmitRequest {
    /// The lot being (partially) exited.
    pub lot: StrategyPositionLot,
    /// Why the exit fired (recorded on the order + intent).
    pub reason: ExitReason,
    /// The concrete sell order (side / type / limit / shares).
    pub order: ExitOrderSpec,
    /// Stable deterministic scale-out target id, when applicable.
    pub pending_scale_out: Option<PendingScaleOut>,
}

impl ExitOrderSpec {
    fn canonical_order(
        &self,
        rules: PolymarketOrderRules,
    ) -> Result<CanonicalExitOrder, VenueOrderRuleError> {
        let limit_price = rules.sell_limit_at_least(self.limit_price)?;
        let amounts = rules.canonical_order(
            Side::Sell,
            VenueOrderAmount::Shares(self.shares),
            limit_price,
        )?;
        Ok(CanonicalExitOrder {
            limit_price,
            amounts,
        })
    }
}

#[derive(Debug)]
struct CanonicalExitOrder {
    limit_price: Price,
    amounts: CanonicalOrderAmounts,
}

/// Collaborators the exit dispatcher needs.
pub struct ExitDispatcherDeps {
    pub submission: Arc<dyn ExecutionSubmissionRepository>,
    pub order_client: Arc<dyn PolymarketOrderClient>,
    pub breaker: Arc<ExecutionBreaker>,
    pub metrics: Arc<MetricsHub>,
    pub execution_events: Arc<ExecutionEventWriter>,
    pub intents: Arc<dyn OrderIntentRepository>,
    pub recommendations: Arc<dyn RecommendationRepository>,
    pub reports: Arc<dyn RecommendationReportRepository>,
    pub clob_market_info: Arc<dyn ClobMarketInfoRepository>,
    pub clob: Arc<ClobClient>,
    pub book_store: Arc<BookStore>,
    pub order_lifecycle: Arc<ExecutionOrderLifecyclePublisher>,
}

/// Submits exit (Sell) orders and settles their venue outcome atomically.
pub struct CoreExitDispatcher {
    deps: ExitDispatcherDeps,
}

struct ExitVenueEvidence<'a> {
    frozen: &'a ClobMarketInfoVersion,
    current: &'a ClobMarketInfoVersion,
    live: &'a VenueOrderMetadata,
}

struct ValidatedExitVenue {
    current: ClobMarketInfoVersion,
    live: VenueOrderMetadata,
    rules: PolymarketOrderRules,
}

impl ExitVenueEvidence<'_> {
    fn validate(
        &self,
        market_id: &MarketId,
        token_id: &TokenId,
    ) -> Result<PolymarketOrderRules, ExecutionError> {
        self.frozen
            .validate()
            .map_err(|reason| ExecutionError::IntentDenied {
                reason: format!("report-time CLOB market info is invalid: {reason}"),
            })?;
        self.current
            .validate()
            .map_err(|reason| ExecutionError::IntentDenied {
                reason: format!("current CLOB market info is invalid: {reason}"),
            })?;
        let has_frozen_token = self
            .frozen
            .tokens
            .iter()
            .any(|token| &token.token_id == token_id);
        let has_current_token = self
            .current
            .tokens
            .iter()
            .any(|token| &token.token_id == token_id);
        if &self.frozen.market_id != market_id
            || &self.current.market_id != market_id
            || &self.live.market_id != market_id
            || !has_frozen_token
            || !has_current_token
            || &self.live.token_id != token_id
        {
            return Err(ExecutionError::IntentDenied {
                reason: "exit frozen/current/live market or token identity mismatch".to_owned(),
            });
        }
        if self.current.tick_size != self.live.tick_size
            || self.current.minimum_order_size != self.live.minimum_order_size
            || self.frozen.neg_risk != self.current.neg_risk
            || self.current.neg_risk != self.live.neg_risk
        {
            return Err(ExecutionError::IntentDenied {
                reason: "exit frozen/current/live venue rules changed".to_owned(),
            });
        }
        PolymarketOrderRules::new(self.current.tick_size, self.current.minimum_order_size).map_err(
            |error| ExecutionError::IntentDenied {
                reason: format!("exit venue rules are invalid: {error}"),
            },
        )
    }
}

impl CoreExitDispatcher {
    #[must_use]
    pub const fn new(deps: ExitDispatcherDeps) -> Self {
        Self { deps }
    }

    /// Submit one triggered exit: write-ahead the Exit order (lot `-> Closing`),
    /// call the venue (no DB lock), feed the breaker (venue + realized-loss), and
    /// settle the position/capital/exit-FSM in one transaction.
    pub async fn submit_exit(&self, request: ExitSubmitRequest) -> QuantResult<ExecutionOrderInfo> {
        let ExitSubmitRequest {
            lot,
            reason,
            order,
            pending_scale_out,
        } = request;

        let intent_id = lot
            .order_intent_id
            .ok_or_else(|| ExecutionError::IntentDenied {
                reason: "recovery-origin position lot is not authorized for strategy exit"
                    .to_owned(),
            })?;
        let intent = self
            .deps
            .intents
            .find_by_id(&intent_id)
            .await?
            .ok_or_else(|| StorageError::not_found(QUANT_ORDER_INTENT, intent_id))?;
        let recommendation = self
            .deps
            .recommendations
            .find_by_id(&intent.recommendation_id)
            .await?
            .ok_or_else(|| {
                StorageError::not_found(QUANT_RECOMMENDATION, intent.recommendation_id)
            })?;
        if recommendation.market_id != lot.market_id {
            return Err(ExecutionError::IntentDenied {
                reason: "exit lot and recommendation market identity mismatch".to_owned(),
            }
            .into());
        }
        let report = self
            .deps
            .reports
            .find_by_id(&recommendation.recommendation_report_id)
            .await?
            .ok_or_else(|| {
                StorageError::not_found(
                    QUANT_RECOMMENDATION_REPORT,
                    recommendation.recommendation_report_id,
                )
            })?;
        let prepared_order = self
            .prepare_exit_order(&lot, &order, &intent.profile_ref, &report)
            .await?;

        // 1. Write-ahead the Exit order (Submitted) + lot Open->Closing + exit FSM.
        let new_order = build_exit_order(&lot, &order, prepared_order.clone())?;
        let execution_order = self
            .deps
            .submission
            .create_exit_order(new_order, reason, pending_scale_out)
            .await?;
        self.deps
            .order_lifecycle
            .created(&execution_order, Utc::now());
        let recommendation_id = intent.recommendation_id;
        self.deps.execution_events.write(project_execution_event(
            &execution_order,
            recommendation_id,
            ChQuantLedgerEventKind::ExitSubmitted,
            Utc::now(),
        ));

        // 2. Venue submission — NO DB lock held across this network call.
        let venue_order = VenueOrder {
            market_id: prepared_order.market_id.clone(),
            token_id: prepared_order.token_id.clone(),
            tick_size: prepared_order.tick_size,
            minimum_order_size: prepared_order.minimum_order_size,
            neg_risk: prepared_order.neg_risk,
            clob_market_info_payload_hash: prepared_order.clob_market_info_payload_hash,
            side: prepared_order.side,
            limit_price: prepared_order.limit_price,
            amount: prepared_order.venue_amount,
            order_type: prepared_order.order_type,
            post_only: prepared_order.post_only,
            expected_fee: prepared_order.expected_fee,
            fee_schedule_hash: prepared_order.fee_schedule.schedule_hash,
        };
        let result = self
            .deps
            .order_client
            .submit(venue_order)
            .await
            .with_order_type_semantics(&order.order_type);

        // 3. Feed venue health (unconfirmed == failure for breaker purposes).
        let venue_ok = result.outcome != VenueOutcome::Ambiguous;
        self.deps
            .breaker
            .observe_venue(venue_ok, result.detail.as_deref().unwrap_or("venue exit"))
            .await;

        // 4. Build the atomic ledger write + the confirmed realized PnL (if any).
        let (write, realized_pnl) =
            build_exit_ledger_write(&result, &lot, reason, &execution_order);

        // 5. Feed the daily realized-loss dimension with the confirmed PnL.
        if let Some(pnl) = realized_pnl {
            self.deps
                .breaker
                .observe_realized_pnl(pnl, result.responded_at)
                .await;
        }

        // 6. Settle in one transaction.
        let recorded = self
            .deps
            .submission
            .record_exit_result(&execution_order.execution_order_id, write)
            .await?;
        self.deps
            .order_lifecycle
            .transition(&execution_order, &recorded, result.responded_at);
        self.deps.execution_events.write(project_execution_event(
            &recorded,
            recommendation_id,
            ChQuantLedgerEventKind::ExitSubmissionResult,
            Utc::now(),
        ));
        self.deps.metrics.inc_exit_trigger(reason.as_str());
        Ok(recorded)
    }

    async fn prepare_exit_order(
        &self,
        lot: &StrategyPositionLot,
        order: &ExitOrderSpec,
        profile_ref: &ResearchProfileRef,
        report: &RecommendationReportInfo,
    ) -> QuantResult<PreparedVenueOrder> {
        let now = Utc::now();
        let book = self
            .deps
            .book_store
            .load_fresh_by_id(&lot.token_id)
            .map_err(|unavailable| ExecutionError::IntentDenied {
                reason: format!("cannot prepare exit without a fresh L2 book: {unavailable:?}"),
            })?;
        let venue = self.exit_venue_evidence(lot, report, now).await?;
        let canonical = order
            .canonical_order(venue.rules)
            .map_err(|error| Self::classify_exit_rule(&error))?;
        let schedule = pit_fee_schedule(&venue.current, now)?;
        let requirement = match order.order_type {
            OrderType::Fok => FillRequirement::AllOrNothing,
            OrderType::Fak | OrderType::Gtc | OrderType::Gtd { .. } => {
                FillRequirement::AllowPartial
            }
        };
        let fill = walk_sell_exact_shares(
            &book.bids,
            canonical.amounts.requested_shares,
            canonical.limit_price,
            requirement,
            &schedule,
            LiquidityRole::Taker,
            now,
        )
        .map_err(|error| ExecutionError::IntentDenied {
            reason: format!("exit execution preparation failed: {error:?}"),
        })?;
        if order.order_type == OrderType::Fok && fill.outcome == BookWalkOutcome::Unfilled {
            return Err(ExecutionError::IntentDenied {
                reason: "FOK exit cannot fill from the current L2 book".to_owned(),
            }
            .into());
        }
        let book_hash = CanonicalDigest::content_hash_json(&(
            book.timestamp_ms,
            book.version,
            book.bids.as_ref(),
            book.asks.as_ref(),
        ))?;
        let clob_market_info_payload_hash = venue.current.payload_hash;
        let valid_until = match order.order_type {
            OrderType::Gtd { expiration } => DateTime::from_timestamp(
                i64::try_from(expiration).map_err(|error| ExecutionError::TimeConversion {
                    field: "exit.gtd.expiration",
                    value: expiration.to_string(),
                    detail: error.to_string(),
                })?,
                0,
            )
            .ok_or_else(|| ExecutionError::TimeConversion {
                field: "exit.gtd.expiration",
                value: expiration.to_string(),
                detail: "timestamp is outside chrono range".to_owned(),
            })?,
            OrderType::Fok | OrderType::Fak | OrderType::Gtc => now + Duration::minutes(1),
        };
        let prepared = PreparedVenueOrder {
            profile_ref: profile_ref.clone(),
            market_id: lot.market_id.clone(),
            token_id: lot.token_id.clone(),
            tick_size: venue.current.tick_size,
            minimum_order_size: venue.current.minimum_order_size,
            neg_risk: venue.current.neg_risk,
            side: Side::Sell,
            order_type: order.order_type,
            post_only: false,
            limit_price: canonical.limit_price,
            expected_worst_fill_price: fill.worst_price.unwrap_or(canonical.limit_price),
            cash_budget: None,
            venue_amount: canonical.amounts.venue_amount,
            requested_shares: canonical.amounts.requested_shares,
            expected_fee: fill.immediate_cost.total_fee_usd(),
            total_cash_delta: fill.account_cash_delta_usd,
            expected_filled_shares: fill.filled_shares,
            book_hash,
            clob_market_info_payload_hash,
            fee_schedule: PreparedFeeSchedule {
                schedule_hash: schedule.schedule_hash,
                effective_at: schedule.effective_at,
                available_at: schedule.available_at,
                platform_rate: schedule.platform_rate,
                exponent: schedule.exponent,
                taker_only: schedule.taker_only,
                builder_maker_fee_bps: schedule.builder_maker_fee_bps,
                builder_taker_fee_bps: schedule.builder_taker_fee_bps,
                builder_attribution: schedule.builder_attribution,
            },
            maker_rebate_terms: EntryMakerRebateTerms::AggressiveNotApplicable,
            prepared_at: now,
            valid_until,
        };
        let funding_request = OrderRequest {
            market_id: lot.market_id.clone(),
            token_id: lot.token_id.clone(),
            expected_tick_size: prepared.tick_size,
            expected_minimum_order_size: prepared.minimum_order_size,
            expected_neg_risk: prepared.neg_risk,
            expected_clob_market_info_payload_hash: prepared.clob_market_info_payload_hash,
            side: Side::Sell,
            amount: prepared.venue_amount,
            expected_fee: prepared.expected_fee,
            price: prepared.limit_price,
            order_type: prepared.order_type,
            post_only: prepared.post_only,
        };
        let funding = self
            .deps
            .clob
            .order_funding_evidence(&funding_request, &venue.live)
            .await?;
        Self::require_exit_funding(&lot.token_id, &funding)?;
        Ok(prepared)
    }

    async fn exit_venue_evidence(
        &self,
        lot: &StrategyPositionLot,
        report: &RecommendationReportInfo,
        now: DateTime<Utc>,
    ) -> QuantResult<ValidatedExitVenue> {
        let clob = Arc::clone(&self.deps.clob);
        let token_id = lot.token_id.clone();
        let (frozen_result, current_result, live_result) = tokio::join!(
            self.deps
                .clob_market_info
                .at(&lot.market_id, report.decision_at, report.created_at),
            self.deps.clob_market_info.at(&lot.market_id, now, now),
            async move { clob.order_metadata(&token_id).await },
        );
        let frozen_market_info = frozen_result?.ok_or_else(|| ExecutionError::IntentDenied {
            reason: "cannot prepare exit without report-time CLOB market info".to_owned(),
        })?;
        let market_info = current_result?.ok_or_else(|| ExecutionError::IntentDenied {
            reason: "cannot prepare exit without current CLOB market info".to_owned(),
        })?;
        let live_metadata = live_result?;
        let rules = ExitVenueEvidence {
            frozen: &frozen_market_info,
            current: &market_info,
            live: &live_metadata,
        }
        .validate(&lot.market_id, &lot.token_id)?;
        Ok(ValidatedExitVenue {
            current: market_info,
            live: live_metadata,
            rules,
        })
    }

    fn require_exit_funding(
        token_id: &TokenId,
        evidence: &VenueFundingEvidence,
    ) -> Result<(), ExecutionError> {
        let snapshot = evidence.snapshot();
        if snapshot.asset != VenueFundingAsset::Conditional
            || snapshot.token_id.as_ref() != Some(token_id)
        {
            return Err(ExecutionError::IntentDenied {
                reason: "exit funding evidence identity mismatch".to_owned(),
            });
        }
        let Some(deficit) = evidence.deficit() else {
            return Ok(());
        };
        Err(ExecutionError::ExitFundingDeferred {
            deficit,
            required: evidence.required().to_string(),
            balance: snapshot.balance.to_string(),
            allowance: snapshot
                .allowance
                .as_ref()
                .map_or_else(|| "missing".to_owned(), ToString::to_string),
        })
    }

    fn classify_exit_rule(error: &VenueOrderRuleError) -> ExecutionError {
        if matches!(error, VenueOrderRuleError::PriceOutsideBounds { .. }) {
            ExecutionError::ExitPriceDeferred {
                reason: error.to_string(),
            }
        } else {
            ExecutionError::IntentDenied {
                reason: format!("exit order cannot be canonicalized: {error}"),
            }
        }
    }
}

/// Build the write-ahead Exit execution-order row (`state = Submitted`).
fn build_exit_order(
    lot: &StrategyPositionLot,
    order: &ExitOrderSpec,
    prepared_order_json: PreparedVenueOrder,
) -> Result<NewExecutionOrder, ExecutionError> {
    if prepared_order_json.market_id != lot.market_id
        || prepared_order_json.token_id != lot.token_id
    {
        return Err(ExecutionError::IntentDenied {
            reason: "prepared exit venue identity differs from its owning position".to_owned(),
        });
    }
    Ok(NewExecutionOrder {
        execution_order_id: ExecutionOrderId::from_v7(),
        order_intent_id: lot
            .order_intent_id
            .ok_or_else(|| ExecutionError::IntentDenied {
                reason: "recovery-origin position lot cannot create a strategy exit order"
                    .to_owned(),
            })?,
        order_phase: ExecutionOrderPhase::Exit,
        market_id: prepared_order_json.market_id.clone(),
        token_id: prepared_order_json.token_id.clone(),
        side: Side::Sell,
        order_type: order.order_type.into(),
        price: prepared_order_json.limit_price,
        shares: prepared_order_json.requested_shares,
        cost_usd: prepared_order_json.requested_shares * prepared_order_json.limit_price,
        prepared_order_json,
        venue_order_id: None,
        venue_status: None,
        state: ExecutionOrderState::Submitted,
        submitted_at: None,
        filled_at: None,
        cancelled_at: None,
        gtd_expiration_at: gtd_expiration_at(&order.order_type)?,
        error_message: None,
    })
}

/// Translate an exit venue outcome into the atomic ledger write, returning the
/// confirmed realized `PnL` (for the breaker's daily-loss dimension) when filled.
fn build_exit_ledger_write(
    result: &VenueSubmitResult,
    lot: &StrategyPositionLot,
    reason: ExitReason,
    execution_order: &ExecutionOrderInfo,
) -> (ExitLedgerWrite, Option<Usd>) {
    let outcome = result.outcome;
    let venue_status = outcome.venue_order_status();
    let filled = result.filled_shares;
    let exit_avg = result.avg_fill_price.unwrap_or(execution_order.price);

    // Exact per-lot average-cost realized PnL, net the venue exit fee.
    let proceeds_usd = filled * exit_avg - result.expected_fee;
    let cost_basis = lot.avg_price * filled;
    let realized_pnl_usd = proceeds_usd - cost_basis;
    let fully_exited = filled >= lot.shares;

    let position_exit = |state: ExitState| ExitLedgerWrite {
        identity_refs: result.identity_refs(),
        order_state: (outcome).outcome_order_state(),
        venue_order_id: result.venue_order_id.clone(),
        venue_status,
        filled_at: Some(result.responded_at),
        cancelled_at: None,
        error_message: None,
        exit_state: state,
        exit_reason: reason,
        position_exit: Some(PositionExit {
            shares: filled,
            avg_price: exit_avg,
            proceeds_usd,
            realized_pnl_usd,
            exited_at: result.responded_at,
            reason,
        }),
        fully_exited,
        revert_to_open: false,
        reconciliation: Some(exit_reconciliation_row(result, execution_order, outcome)),
    };

    match outcome {
        VenueOutcome::Filled => {
            let state = if fully_exited {
                ExitState::Exited
            } else {
                ExitState::PartiallyExited
            };
            (position_exit(state), Some(realized_pnl_usd))
        }
        VenueOutcome::PartiallyFilled => (
            position_exit(ExitState::PartiallyExited),
            Some(realized_pnl_usd),
        ),
        // Resting sell limit: stays Submitted, no position change; recon sweep
        // polls it (no recon row, like a resting entry order).
        VenueOutcome::Open => (resting_open_exit_write(result, reason), None),
        // Clean rejection / cancellation: revert the lot to Open and re-monitor.
        VenueOutcome::Rejected => (
            failed_exit_write(
                result,
                execution_order,
                reason,
                ExecutionOrderState::Failed,
                outcome,
            ),
            None,
        ),
        VenueOutcome::Cancelled | VenueOutcome::Expired => (
            failed_exit_write(
                result,
                execution_order,
                reason,
                ExecutionOrderState::Cancelled,
                outcome,
            ),
            None,
        ),
        // Unconfirmed: hold the position, enqueue recon (fail-closed). No PnL.
        VenueOutcome::Ambiguous => (ambiguous_exit_write(result, execution_order, reason), None),
    }
}

/// Ledger write for a resting `Open` sell limit: stays `Submitted`, no position
/// change; the recon sweep polls it (no recon row, like a resting entry order).
fn resting_open_exit_write(result: &VenueSubmitResult, reason: ExitReason) -> ExitLedgerWrite {
    ExitLedgerWrite {
        identity_refs: result.identity_refs(),
        order_state: ExecutionOrderState::Submitted,
        venue_order_id: result.venue_order_id.clone(),
        venue_status: VenueOutcome::Open.venue_order_status(),
        filled_at: None,
        cancelled_at: None,
        error_message: None,
        exit_state: ExitState::OrderSubmitted,
        exit_reason: reason,
        position_exit: None,
        fully_exited: false,
        revert_to_open: false,
        reconciliation: None,
    }
}

/// Ledger write for an unconfirmed (`Ambiguous`) venue response: hold the
/// position untouched and enqueue reconciliation (fail-closed; no `PnL`).
fn ambiguous_exit_write(
    result: &VenueSubmitResult,
    execution_order: &ExecutionOrderInfo,
    reason: ExitReason,
) -> ExitLedgerWrite {
    ExitLedgerWrite {
        identity_refs: result.identity_refs(),
        order_state: ExecutionOrderState::Ambiguous,
        venue_order_id: result.venue_order_id.clone(),
        venue_status: None,
        filled_at: None,
        cancelled_at: None,
        error_message: result.detail.clone(),
        exit_state: ExitState::OrderSubmitted,
        exit_reason: reason,
        position_exit: None,
        fully_exited: false,
        revert_to_open: false,
        reconciliation: Some(exit_reconciliation_row(
            result,
            execution_order,
            VenueOutcome::Ambiguous,
        )),
    }
}

/// Ledger write for a failed/cancelled exit attempt: revert `Closing -> Open`
/// and route the lot back to `Monitoring` so the worker re-evaluates it.
fn failed_exit_write(
    result: &VenueSubmitResult,
    execution_order: &ExecutionOrderInfo,
    reason: ExitReason,
    order_state: ExecutionOrderState,
    outcome: VenueOutcome,
) -> ExitLedgerWrite {
    ExitLedgerWrite {
        identity_refs: result.identity_refs(),
        order_state,
        venue_order_id: result.venue_order_id.clone(),
        venue_status: outcome.venue_order_status(),
        filled_at: None,
        cancelled_at: Some(result.responded_at),
        error_message: result.detail.clone(),
        exit_state: ExitState::Monitoring,
        exit_reason: reason,
        position_exit: None,
        fully_exited: false,
        revert_to_open: true,
        reconciliation: Some(exit_reconciliation_row(result, execution_order, outcome)),
    }
}

impl VenueOutcome {
    /// The terminal exit-order state for a (partial) fill venue outcome.
    const fn outcome_order_state(self) -> ExecutionOrderState {
        match self {
            Self::PartiallyFilled => ExecutionOrderState::PartiallyFilled,
            _ => ExecutionOrderState::Filled,
        }
    }
}

/// Build the submit-time reconciliation row for an exit order.
fn exit_reconciliation_row(
    result: &VenueSubmitResult,
    execution_order: &ExecutionOrderInfo,
    outcome: VenueOutcome,
) -> NewReconciliation {
    let reconciliation_result = outcome.reconciliation_result();
    let (resolved_by, resolved_at) =
        submit_response_resolution(reconciliation_result, result.responded_at);
    let detail = format!(
        "exit venue outcome {outcome:?}; order_id={:?}; filled={}",
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
        resolved_by,
        resolved_at,
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use quant_pivot_api::clob::{
        VenueBalanceAllowanceSnapshot, VenueFundingAsset, VenueFundingBalance,
        VenueFundingEvidence, VenueOrderMetadata,
    };
    use quant_pivot_error::execution::ExecutionError;
    use quant_pivot_models::{
        domain::{
            order::{PolymarketOrderRules, VenueOrderRuleError},
            quant::StrategyPositionLot,
        },
        enums::{
            common::{MarketCategory, OrderType, Side, TickSize},
            execution::{PositionLedgerState, StrategyPositionOriginKind},
            quant::{AccountSource, OutcomeSide},
        },
        hashing::CanonicalDigest,
        types::{
            ClobFeeDetails, ClobMarketInfoVersion, ClobMarketInfoVersionId, ClobTokenDescriptor,
            EvmAddress, EvmUint256, ExecutionAccountId, MarketId, OrderIntentId, Price, Shares,
            StrategyPositionLotId, TokenId, Usd, VenueOrderAmount,
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{CoreExitDispatcher, ExitOrderSpec, ExitVenueEvidence, build_exit_order};
    use crate::test_fixtures::execution_pg_seed::PreparedOrderFixture;

    fn rules() -> PolymarketOrderRules {
        PolymarketOrderRules::new(TickSize::Hundredth, Shares::new(dec!(5)))
            .expect("valid exit rules")
    }

    fn order(shares: Decimal) -> ExitOrderSpec {
        ExitOrderSpec {
            token_id: TokenId::new("token-1"),
            side: Side::Sell,
            order_type: OrderType::Gtc,
            limit_price: Price::new(dec!(0.50)),
            shares: Shares::new(shares),
        }
    }

    fn lot() -> StrategyPositionLot {
        StrategyPositionLot {
            strategy_position_lot_id: StrategyPositionLotId::from_v7(),
            origin_kind: StrategyPositionOriginKind::SystemIntent,
            order_intent_id: Some(OrderIntentId::from_v7()),
            recovery_incident_id: None,
            execution_account_id: ExecutionAccountId::from_v7(),
            token_id: TokenId::new("token-1"),
            market_id: MarketId::new("0xmarket"),
            event_id: None,
            category: MarketCategory::Politics,
            side: OutcomeSide::Yes,
            state: PositionLedgerState::Open,
            shares: Shares::new(dec!(10.009)),
            avg_price: Price::new(dec!(0.40)),
            cost_usd: Usd::new(dec!(4.0036)),
            realized_pnl_usd: Usd::ZERO,
            source: AccountSource::Polymarket,
            opened_at: Utc::now(),
            updated_at: Utc::now(),
            closed_at: None,
        }
    }

    fn funding_snapshot() -> VenueBalanceAllowanceSnapshot {
        VenueBalanceAllowanceSnapshot {
            asset: VenueFundingAsset::Conditional,
            token_id: Some(TokenId::new("token-1")),
            spender: EvmAddress::parse(format!("0x{}", "a".repeat(40))).expect("canonical spender"),
            balance: EvmUint256::parse("10000000").expect("canonical balance"),
            human_balance: VenueFundingBalance::Conditional(Shares::new(dec!(10))),
            allowance: Some(EvmUint256::parse("10000000").expect("canonical allowance")),
        }
    }

    fn market_info(
        tick_size: TickSize,
        minimum_order_size: Shares,
        neg_risk: bool,
        fee_rate: Decimal,
        seed: &str,
    ) -> ClobMarketInfoVersion {
        let raw_payload = serde_json::json!({ "seed": seed });
        ClobMarketInfoVersion {
            version_id: ClobMarketInfoVersionId::from_v7(),
            market_id: MarketId::new("0xmarket"),
            tokens: vec![
                ClobTokenDescriptor {
                    token_id: TokenId::new("token-1"),
                    outcome: "Yes".to_owned(),
                },
                ClobTokenDescriptor {
                    token_id: TokenId::new("token-2"),
                    outcome: "No".to_owned(),
                },
            ],
            tick_size,
            minimum_order_size,
            neg_risk,
            taker_order_delay_enabled: false,
            minimum_order_age_secs: None,
            blockaid_check_enabled: false,
            fee_details: ClobFeeDetails {
                rate: fee_rate,
                exponent: 1,
                taker_only: true,
            },
            builder_maker_fee_rate_bps: 0,
            builder_taker_fee_rate_bps: 0,
            effective_at: Utc::now(),
            available_at: Utc::now(),
            payload_hash: CanonicalDigest::content_hash_json(&raw_payload)
                .expect("market-info payload hash"),
            raw_payload,
        }
    }

    fn live_metadata(tick_size: TickSize, minimum_order_size: Shares) -> VenueOrderMetadata {
        VenueOrderMetadata {
            market_id: MarketId::new("0xmarket"),
            token_id: TokenId::new("token-1"),
            tick_size,
            minimum_order_size,
            neg_risk: false,
        }
    }

    #[test]
    fn canonical_exit_floors_shares() {
        let canonical = order(dec!(10.009))
            .canonical_order(rules())
            .expect("canonical exit amount");
        assert_eq!(canonical.amounts.requested_shares, Shares::new(dec!(10.00)));
        assert_eq!(
            canonical.amounts.venue_amount,
            VenueOrderAmount::Shares(Shares::new(dec!(10.00)))
        );
    }

    #[test]
    fn rule_changes_allow_exit() {
        let frozen = market_info(
            TickSize::Hundredth,
            Shares::new(dec!(5)),
            false,
            dec!(0.02),
            "frozen",
        );
        let current = market_info(
            TickSize::Thousandth,
            Shares::new(dec!(3)),
            false,
            dec!(0.03),
            "current",
        );
        let live = live_metadata(TickSize::Thousandth, Shares::new(dec!(3)));

        let rules = ExitVenueEvidence {
            frozen: &frozen,
            current: &current,
            live: &live,
        }
        .validate(&MarketId::new("0xmarket"), &TokenId::new("token-1"))
        .expect("legitimate current rule changes must not lock an exit");

        assert_eq!(rules.tick_size, TickSize::Thousandth);
        assert_eq!(rules.minimum_order_size, Shares::new(dec!(3)));

        let mut exit = order(dec!(10.009));
        exit.limit_price = Price::new(dec!(0.5001));
        let canonical = exit
            .canonical_order(rules)
            .expect("current rules canonicalize the exit");
        let prepared = PreparedOrderFixture {
            market_id: lot().market_id,
            token_id: lot().token_id,
            side: Side::Sell,
            order_type: OrderType::Gtc,
            venue_amount: canonical.amounts.venue_amount,
            expected_fee: Usd::ZERO,
            expected_filled_shares: canonical.amounts.requested_shares,
            limit_price: canonical.limit_price,
        }
        .build();
        let wal = build_exit_order(&lot(), &exit, prepared).expect("current-rule WAL");
        assert_eq!(wal.price, Price::new(dec!(0.501)));
        assert_eq!(wal.shares, Shares::new(dec!(10.00)));
    }

    #[test]
    fn identity_drift_rejects_exit() {
        let frozen = market_info(
            TickSize::Hundredth,
            Shares::new(dec!(5)),
            false,
            dec!(0.02),
            "frozen",
        );
        let current = market_info(
            TickSize::Hundredth,
            Shares::new(dec!(5)),
            false,
            dec!(0.02),
            "current",
        );
        let mut live = live_metadata(TickSize::Hundredth, Shares::new(dec!(5)));
        live.token_id = TokenId::new("other-token");

        assert!(
            ExitVenueEvidence {
                frozen: &frozen,
                current: &current,
                live: &live,
            }
            .validate(&MarketId::new("0xmarket"), &TokenId::new("token-1"))
            .is_err()
        );
    }

    #[test]
    fn neg_risk_drift_rejects() {
        let frozen = market_info(
            TickSize::Hundredth,
            Shares::new(dec!(5)),
            false,
            dec!(0.02),
            "frozen",
        );
        let current = market_info(
            TickSize::Hundredth,
            Shares::new(dec!(5)),
            false,
            dec!(0.02),
            "current",
        );
        let mut live = live_metadata(TickSize::Hundredth, Shares::new(dec!(5)));
        live.neg_risk = true;

        assert!(
            ExitVenueEvidence {
                frozen: &frozen,
                current: &current,
                live: &live,
            }
            .validate(&MarketId::new("0xmarket"), &TokenId::new("token-1"))
            .is_err()
        );
    }

    #[test]
    fn minimum_blocks_wal() {
        let error = order(dec!(4.999))
            .canonical_order(rules())
            .expect_err("floored exit below minimum must fail before WAL creation");
        assert!(matches!(
            error,
            VenueOrderRuleError::OrderBelowMinimum {
                requested,
                minimum,
            } if requested == Shares::new(dec!(4.99)) && minimum == Shares::new(dec!(5))
        ));
    }

    #[test]
    fn exact_minimum_is_valid() {
        let canonical = order(dec!(5))
            .canonical_order(rules())
            .expect("exact venue minimum must remain valid");
        assert_eq!(canonical.amounts.requested_shares, Shares::new(dec!(5)));
    }

    #[test]
    fn ceiling_defers_exit() {
        let mut exit = order(dec!(5));
        exit.limit_price = Price::new(dec!(0.999));
        let error = exit
            .canonical_order(rules())
            .expect_err("unrepresentable SELL hard minimum must fail before WAL");
        assert!(matches!(
            CoreExitDispatcher::classify_exit_rule(&error),
            ExecutionError::ExitPriceDeferred { .. }
        ));
    }

    #[test]
    fn wal_uses_canonical_identity() {
        let mut exit = order(dec!(10.009));
        exit.limit_price = Price::new(dec!(0.501));
        let canonical = exit
            .canonical_order(rules())
            .expect("canonical exit amount");
        let mut prepared = PreparedOrderFixture {
            market_id: lot().market_id,
            token_id: lot().token_id,
            side: Side::Sell,
            order_type: OrderType::Gtc,
            venue_amount: canonical.amounts.venue_amount,
            expected_fee: Usd::ZERO,
            expected_filled_shares: canonical.amounts.requested_shares,
            limit_price: canonical.limit_price,
        }
        .build();
        prepared.expected_filled_shares = Shares::new(dec!(9.75));
        prepared.expected_worst_fill_price = Price::new(dec!(0.49));

        let wal = build_exit_order(&lot(), &exit, prepared)
            .expect("canonical exit must build its WAL row");
        assert_eq!(wal.price, canonical.limit_price);
        assert_eq!(wal.price, Price::new(dec!(0.51)));
        assert!(wal.price >= exit.limit_price);
        assert_eq!(wal.shares, canonical.amounts.requested_shares);
        assert_eq!(
            wal.prepared_order_json.expected_filled_shares,
            Shares::new(dec!(9.75))
        );
        assert_eq!(
            wal.prepared_order_json.expected_worst_fill_price,
            Price::new(dec!(0.49))
        );
    }

    #[test]
    fn exact_exit_funding_passes() {
        let evidence = VenueFundingEvidence::Ready {
            snapshot: funding_snapshot(),
            required: EvmUint256::parse("10000000").expect("canonical required amount"),
        };
        assert!(
            CoreExitDispatcher::require_exit_funding(&TokenId::new("token-1"), &evidence).is_ok()
        );
    }

    #[test]
    fn exit_funding_defers() {
        let required = EvmUint256::parse("10000000").expect("canonical required amount");
        for evidence in [
            VenueFundingEvidence::MissingAllowance {
                snapshot: funding_snapshot(),
                required: required.clone(),
            },
            VenueFundingEvidence::InsufficientBalance {
                snapshot: funding_snapshot(),
                required: required.clone(),
            },
            VenueFundingEvidence::InsufficientAllowance {
                snapshot: funding_snapshot(),
                required,
            },
        ] {
            assert!(matches!(
                CoreExitDispatcher::require_exit_funding(&TokenId::new("token-1"), &evidence),
                Err(ExecutionError::ExitFundingDeferred { .. })
            ));
        }
    }
}
