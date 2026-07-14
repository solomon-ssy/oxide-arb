//! Full-stack demo seed for Phase 10.4 execution-plane + 10.5 research-catalog UI validation.
//!
//! Populates Postgres ledger tables and matching `ClickHouse` quant facts using
//! repository code paths (not raw SQL). Trigger keys are prefixed with
//! `ui-demo:` so rows are easy to spot in the admin UI.

use std::sync::{Arc, OnceLock};

static DEMO_RUN_ID: OnceLock<String> = OnceLock::new();

fn demo_run_id() -> &'static str {
    DEMO_RUN_ID.get_or_init(|| Utc::now().timestamp_millis().to_string())
}

use chrono::Utc;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::{
        ChDecimal64, ChPrice, ChProbability, ChShares, ChUsd, QuantCapitalAllocationEventRow,
        QuantExecutionEventRow, QuantPositionEventRow, QuantRecommendationAttributionEventRow,
        QuantRecommendationEventRow,
    },
    domain::{
        CapitalReconcileSettlement, CapitalSettlement, ConfirmSettlementRedeem, InsertFinalOutcome,
        NewMarketSelection, NewRecommendationAttribution, NewReconciliation, NewSettlementRedeem,
        NewSettlementRedeemLot, PositionExit, ReconciliationLedgerWrite, SettlementRedeemLotWrite,
        SubmissionLedgerWrite,
    },
    entities::{market, quant_execution_order, quant_order_intent},
    enums::{
        clickhouse::{
            ChCapitalAllocationState, ChExecutionSide, ChOutcomeSide, ChPositionLedgerState,
            ChQuantLedgerEventKind, ChRecommendationAttributionOutcome, ChRecommendationStatus,
        },
        common::MarketCategory,
        execution::{
            ExecutionOrderPhase, ExitReason, ReconciliationEvidenceKind, ReconciliationResult,
            SettlementRedeemState, VenueOrderStatus,
        },
        quant::{
            ExecutionOrderState, ExecutionWalletKind, ExitSettlementMode, OrderIntentStatus,
            OutcomeSide, RecommendationAttributionOutcome, RedeemPolicy,
        },
    },
    types::{
        AccountSnapshotId, AttributionDetail, CapitalAllocationId, ContentHash,
        EntryConditionInstanceId, EntryOutcome, ExecutionOrderId, ExitOutcome, MarketId,
        MarketSelectionId, OrderId, OrderIntentId, PortfolioPlanId, PositionId, Price,
        RecommendationId, RecommendationReportId, ReconciliationEvidence,
        ReconciliationEvidenceChain, ReconciliationId, ReportDataQualitySnapshotId,
        RuntimeConfigVersionId, SelectionExclusionSummary, SettlementBalanceEvidence,
        SettlementPayoutVector, SettlementRedeemId, SettlementRedeemIndexSets,
        SettlementRedeemLotId, SettlementTokenBalance, Shares, TokenId, Usd,
    },
};
use quant_pivot_repository::{
    clickhouse::ChQuantFactRepository,
    postgres::{
        PgAttributionRepository, PgCapitalAllocationRepository, PgEventRepository,
        PgExecutionSubmissionRepository, PgMarketRepository, PgMarketSelectionRepository,
        PgOrderIntentRepository, PgPositionRepository, PgRecommendationReportRepository,
        PgRecommendationRepository, PgSettlementRedeemRepository,
    },
    traits::{
        AttributionRepository, CapitalAllocationRepository, EventRepository,
        ExecutionSubmissionRepository, MarketRepository, MarketSelectionRepository,
        OrderIntentRepository, PositionRepository, QuantFactRepository,
        RecommendationReportRepository, RecommendationRepository, SettlementRedeemRepository,
    },
};
use quant_pivot_storage::clickhouse::{ChWriteManager, ClickHousePool};
use rust_decimal_macros::dec;
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel,
    QueryFilter,
};

use crate::{
    catalog_fixtures::{make_event, make_market},
    execution_pg_seed::{
        self, EXECUTION_NOTIONAL, ExecutionTxnIds, ReportBuildOptions, ReportSeedConfig,
        SharedDemoInfra, build_custom_report_transaction, claim_entry_for_test,
        close_position_full, demo_recommendation, entry_execution_order, fill_entry_lot,
        report_operation_log, seed_approved_intent, seed_conditional_price_report_on_infra,
        seed_manual_approved_intent, seed_pending_intent, seed_report_on_infra,
        seed_shared_demo_infra,
    },
    research_ui_seed::{ResearchUiSeedSummary, seed_research_ui_demo_pg},
};

const DEMO_TAG: &str = "ui-demo";
const SETTLE_MARKET: &str = "ui-demo-settle-mkt";
const SETTLE_EVENT: &str = "ui-demo-settle-evt";
const SETTLE_YES: &str = "ui-demo-yes-token";
const SETTLE_NO: &str = "ui-demo-no-token";

/// One seeded recommendation + optional downstream ledger ids.
#[derive(Debug, Clone)]
pub struct DemoSeedRecord {
    pub slug: String,
    pub report_id: RecommendationReportId,
    pub recommendation_id: RecommendationId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub intent_id: Option<OrderIntentId>,
    pub execution_order_id: Option<ExecutionOrderId>,
    pub position_id: Option<PositionId>,
    pub capital_allocation_id: Option<CapitalAllocationId>,
}

/// Summary printed after seeding.
#[derive(Debug, Default)]
pub struct UiDemoSeedSummary {
    pub reports: usize,
    pub intents: usize,
    pub execution_orders: usize,
    pub positions: usize,
    pub reconciliations: usize,
    pub settlement_redeems: usize,
    pub attributions: usize,
    pub clickhouse_rows: usize,
    pub research: ResearchUiSeedSummary,
    pub records: Vec<DemoSeedRecord>,
    /// Diff tab: older baseline report (filter `ui-demo:report:diff-base`).
    pub diff_base_report_id: Option<RecommendationReportId>,
    /// Diff tab: newer compare report (filter `ui-demo:report:diff-current`).
    pub diff_current_report_id: Option<RecommendationReportId>,
    /// Create-intent entry: published recommendation without blocking intent.
    pub actionable_recommendation_id: Option<RecommendationId>,
}

/// Seed Postgres demo data for execution-plane UI pages.
pub async fn seed_ui_demo_pg(db: &DatabaseConnection, funder: &str) -> UiDemoSeedSummary {
    let infra = seed_shared_demo_infra(db).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    let mut summary = UiDemoSeedSummary::default();

    let scenarios = [
        seed_pending_a(db, &infra, &submission).await,
        seed_pending_b(db, &infra, &submission).await,
        seed_approved(db, &infra, &submission).await,
        seed_approved_policy(db, &infra, &submission).await,
        seed_submitted(db, &infra, &submission).await,
        seed_partial(db, &infra, &submission).await,
        seed_filled_open(db, &infra, &submission).await,
        seed_filled_closed(db, &infra, &submission).await,
        seed_rejected(db, &infra, &submission).await,
        seed_cancelled(db, &infra, &submission).await,
        seed_expired(db, &infra, &submission).await,
        seed_failed(db, &infra, &submission).await,
    ];

    for record in scenarios {
        summary.reports += 1;
        if record.intent_id.is_some() {
            summary.intents += 1;
        }
        if record.execution_order_id.is_some() {
            summary.execution_orders += 1;
        }
        if record.position_id.is_some() {
            summary.positions += 1;
        }
        summary.records.push(record);
    }

    seed_reconciliation_queue(db, &infra, &submission, &mut summary).await;
    seed_settlement_redeems(db, &infra, &submission, funder, &mut summary).await;
    seed_report_plane_scenarios(db, &infra, &mut summary).await;
    seed_filled_closed_attribution(db, &mut summary).await;
    summary.research = seed_research_ui_demo_pg(db, &infra).await;

    summary
}

/// Mirror seeded Postgres rows into `ClickHouse` quant fact tables.
pub async fn seed_ui_demo_ck(
    pool: Arc<ClickHousePool>,
    summary: &UiDemoSeedSummary,
) -> Result<usize, StorageError> {
    pool.ensure_schema().await?;
    let write_manager = Arc::new(ChWriteManager::new(4));
    let facts = ChQuantFactRepository::new(pool, write_manager);
    let now = Utc::now().timestamp();
    let mut batches = DemoCkFactBatches::default();

    for record in &summary.records {
        push_demo_ck_rows(record, now, &mut batches);
    }

    flush_demo_ck_batches(&facts, batches).await
}

#[derive(Default)]
struct DemoCkFactBatches {
    recommendation: Vec<QuantRecommendationEventRow>,
    execution: Vec<QuantExecutionEventRow>,
    position: Vec<QuantPositionEventRow>,
    capital: Vec<QuantCapitalAllocationEventRow>,
    attribution: Vec<QuantRecommendationAttributionEventRow>,
}

fn push_demo_ck_rows(record: &DemoSeedRecord, now: i64, batches: &mut DemoCkFactBatches) {
    batches.recommendation.push(QuantRecommendationEventRow {
        event_time: now,
        recommendation_report_id: record.report_id.clone(),
        recommendation_id: record.recommendation_id.clone(),
        rank: 1,
        market_id: record.market_id.clone(),
        token_id: record.token_id.clone(),
        side: ChOutcomeSide::Yes,
        score: ChProbability::from(dec!(0.72)),
        risk_adjusted_score: ChProbability::from(dec!(0.68)),
        trade_plan_available: true,
        suggested_usd: Some(ChUsd::from(Usd::new(EXECUTION_NOTIONAL))),
        valid_until: now + 86_400,
        status: ChRecommendationStatus::IntentCreated,
    });

    if let (Some(intent_id), Some(order_id)) = (&record.intent_id, &record.execution_order_id) {
        batches.execution.push(QuantExecutionEventRow {
            event_time: now,
            order_intent_id: intent_id.clone(),
            execution_order_id: order_id.clone(),
            recommendation_id: record.recommendation_id.clone(),
            event_kind: ChQuantLedgerEventKind::Submitted,
            market_id: record.market_id.clone(),
            token_id: record.token_id.clone(),
            side: ChExecutionSide::Buy,
            price: ChPrice::from(Price::new(dec!(0.6))),
            shares: ChShares::from(Shares::new(dec!(100))),
            cost_usd: ChUsd::from(Usd::new(EXECUTION_NOTIONAL)),
            venue_order_id: Some(OrderId::new("ui-demo-venue")),
            ingestion_time: now,
        });
    }

    if let (Some(intent_id), Some(position_id)) = (&record.intent_id, &record.position_id) {
        batches.position.push(QuantPositionEventRow {
            event_time: now,
            position_id: position_id.clone(),
            order_intent_id: intent_id.clone(),
            market_id: record.market_id.clone(),
            token_id: record.token_id.clone(),
            event_kind: ChQuantLedgerEventKind::Opened,
            state: ChPositionLedgerState::Open,
            side: ChOutcomeSide::Yes,
            shares: ChShares::from(Shares::new(dec!(100))),
            avg_price: ChPrice::from(Price::new(dec!(0.6))),
            cost_usd: ChUsd::from(Usd::new(EXECUTION_NOTIONAL)),
            realized_pnl_usd: ChUsd::from(Usd::ZERO),
            ingestion_time: now,
        });
    }

    if let (Some(intent_id), Some(allocation_id)) =
        (&record.intent_id, &record.capital_allocation_id)
    {
        batches.capital.push(QuantCapitalAllocationEventRow {
            event_time: now,
            capital_allocation_id: allocation_id.clone(),
            order_intent_id: intent_id.clone(),
            recommendation_id: record.recommendation_id.clone(),
            event_kind: ChQuantLedgerEventKind::Submitted,
            state: ChCapitalAllocationState::Allocated,
            allocated_usd: ChUsd::from(Usd::new(EXECUTION_NOTIONAL)),
            locked_usd: ChUsd::from(Usd::ZERO),
            spent_usd: ChUsd::from(Usd::ZERO),
            released_usd: ChUsd::from(Usd::ZERO),
            ingestion_time: now,
        });
    }

    if record.slug == "filled-closed" {
        batches
            .attribution
            .push(QuantRecommendationAttributionEventRow {
                event_time: now,
                recommendation_id: record.recommendation_id.clone(),
                outcome: ChRecommendationAttributionOutcome::FilledExited,
                realized_pnl_usd: ChUsd::from(Usd::new(dec!(-5))),
                max_adverse_excursion_bps: Some(ChDecimal64::from(dec!(120))),
                max_favorable_excursion_bps: ChDecimal64::from(dec!(80)),
                label_available_at: now,
                ingestion_time: now,
            });
    }
}

async fn flush_demo_ck_batches(
    facts: &ChQuantFactRepository,
    batches: DemoCkFactBatches,
) -> Result<usize, StorageError> {
    let mut rows = 0usize;
    if !batches.recommendation.is_empty() {
        rows += batches.recommendation.len();
        facts
            .insert_recommendation_events(batches.recommendation)
            .await?;
    }
    if !batches.execution.is_empty() {
        rows += batches.execution.len();
        facts.insert_execution_events(batches.execution).await?;
    }
    if !batches.position.is_empty() {
        rows += batches.position.len();
        facts.insert_position_events(batches.position).await?;
    }
    if !batches.capital.is_empty() {
        rows += batches.capital.len();
        facts
            .insert_capital_allocation_events(batches.capital)
            .await?;
    }
    if !batches.attribution.is_empty() {
        rows += batches.attribution.len();
        facts
            .insert_recommendation_attribution_events(batches.attribution)
            .await?;
    }
    Ok(rows)
}

fn demo_report(slug: &str) -> ReportSeedConfig {
    ReportSeedConfig {
        event_id: format!("{DEMO_TAG}-evt-{slug}"),
        market_id: format!("{DEMO_TAG}-mkt-{slug}"),
        market_question: format!("UI demo: will scenario `{slug}` resolve Yes?"),
        market_slug: format!("{DEMO_TAG}-{slug}"),
        token_id: format!("{DEMO_TAG}-token-{slug}"),
        trigger_key: format!("{DEMO_TAG}:report:{slug}:{}", demo_run_id()),
    }
}

async fn seed_report(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    slug: &str,
) -> ExecutionTxnIds {
    seed_report_on_infra(db, infra, demo_report(slug)).await
}

fn base_record(slug: &str, ids: &ExecutionTxnIds) -> DemoSeedRecord {
    DemoSeedRecord {
        slug: slug.to_owned(),
        report_id: ids.report.clone(),
        recommendation_id: ids.recommendation.clone(),
        market_id: MarketId::new(&ids.market),
        token_id: TokenId::new(&ids.token),
        intent_id: None,
        execution_order_id: None,
        position_id: None,
        capital_allocation_id: None,
    }
}

async fn attach_intent_meta(
    db: &DatabaseConnection,
    record: &mut DemoSeedRecord,
    intent_id: OrderIntentId,
) {
    record.intent_id = Some(intent_id.clone());
    if let Some(capital) = PgCapitalAllocationRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("capital lookup")
    {
        record.capital_allocation_id = Some(capital.capital_allocation_id);
    }
    if let Some(position) = PgPositionRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("position lookup")
    {
        record.position_id = Some(position.position_id);
    }
}

async fn patch_intent_status(
    db: &DatabaseConnection,
    intent_id: &OrderIntentId,
    status: OrderIntentStatus,
) {
    let row = quant_order_intent::Entity::find_by_id(intent_id.clone())
        .one(db)
        .await
        .expect("load intent")
        .expect("intent row");
    let mut active = row.into_active_model();
    active.status = ActiveValue::Set(status);
    active.update(db).await.expect("patch intent status");
}

async fn seed_pending_a(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    _: &PgExecutionSubmissionRepository,
) -> DemoSeedRecord {
    let ids = seed_report(db, infra, "pending-a").await;
    let mut record = base_record("pending-a", &ids);
    let intent_id = seed_pending_intent(db, &ids).await;
    attach_intent_meta(db, &mut record, intent_id).await;
    record
}

async fn seed_pending_b(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    _: &PgExecutionSubmissionRepository,
) -> DemoSeedRecord {
    let ids = seed_report(db, infra, "pending-b").await;
    let mut record = base_record("pending-b", &ids);
    let intent_id = seed_pending_intent(db, &ids).await;
    attach_intent_meta(db, &mut record, intent_id).await;
    record
}

async fn seed_approved(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    _: &PgExecutionSubmissionRepository,
) -> DemoSeedRecord {
    let ids = seed_conditional_price_report_on_infra(db, infra, demo_report("approved")).await;
    let mut record = base_record("approved", &ids);
    let intent_id = seed_manual_approved_intent(db, &ids).await;
    attach_intent_meta(db, &mut record, intent_id).await;
    record
}

async fn seed_approved_policy(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    _: &PgExecutionSubmissionRepository,
) -> DemoSeedRecord {
    let ids = seed_report(db, infra, "approved-policy").await;
    let mut record = base_record("approved-policy", &ids);
    let intent_id = seed_approved_intent(db, &ids).await;
    attach_intent_meta(db, &mut record, intent_id).await;
    record
}

async fn seed_submitted(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    submission: &PgExecutionSubmissionRepository,
) -> DemoSeedRecord {
    let ids = seed_report(db, infra, "submitted").await;
    let mut record = base_record("submitted", &ids);
    let intent_id = seed_approved_intent(db, &ids).await;
    claim_entry_for_test(db, submission, &intent_id).await;
    let order = submission
        .create_entry_order_and_lock_capital(
            entry_execution_order(&intent_id, &ids),
            &ids.feature_parity_state_id,
        )
        .await
        .expect("create entry order");
    record.execution_order_id = Some(order.execution_order_id);
    attach_intent_meta(db, &mut record, intent_id).await;
    record
}

async fn seed_partial(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    submission: &PgExecutionSubmissionRepository,
) -> DemoSeedRecord {
    let ids = seed_report(db, infra, "partial").await;
    let mut record = base_record("partial", &ids);
    let intent_id = seed_approved_intent(db, &ids).await;
    claim_entry_for_test(db, submission, &intent_id).await;
    let order = submission
        .create_entry_order_and_lock_capital(
            entry_execution_order(&intent_id, &ids),
            &ids.feature_parity_state_id,
        )
        .await
        .expect("create entry order");
    let partial_cost = Usd::new(dec!(30));
    submission
        .record_submission_result(
            &order.execution_order_id,
            SubmissionLedgerWrite {
                state: ExecutionOrderState::PartiallyFilled,
                intent_status: OrderIntentStatus::PartiallyFilled,
                venue_order_id: Some(OrderId::new("ui-demo-partial")),
                venue_status: Some(VenueOrderStatus::PartiallyFilled),
                submitted_at: Utc::now(),
                filled_at: Some(Utc::now()),
                cancelled_at: None,
                error_message: None,
                capital: CapitalSettlement::SettlePartial {
                    spent_usd: partial_cost,
                },
                fill: Some(execution_pg_seed::position_fill_public(
                    &ids,
                    &intent_id,
                    Shares::new(dec!(50)),
                    partial_cost,
                )),
                reconciliation: Some(unresolvable_reconciliation(
                    &order.execution_order_id,
                    &intent_id,
                )),
            },
        )
        .await
        .expect("partial fill");
    record.execution_order_id = Some(order.execution_order_id);
    attach_intent_meta(db, &mut record, intent_id).await;
    record
}

async fn seed_filled_open(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    submission: &PgExecutionSubmissionRepository,
) -> DemoSeedRecord {
    let ids = seed_report(db, infra, "filled-open").await;
    let mut record = base_record("filled-open", &ids);
    let intent_id = seed_approved_intent(db, &ids).await;
    fill_entry_lot(db, submission, &ids, &intent_id).await;
    if let Some(order) = first_entry_order(db, &intent_id).await {
        record.execution_order_id = Some(order);
    }
    attach_intent_meta(db, &mut record, intent_id).await;
    record
}

async fn seed_filled_closed(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    submission: &PgExecutionSubmissionRepository,
) -> DemoSeedRecord {
    let ids = seed_report(db, infra, "filled-closed").await;
    let mut record = base_record("filled-closed", &ids);
    let intent_id = seed_approved_intent(db, &ids).await;
    close_position_full(db, submission, &ids, &intent_id, None).await;
    if let Some(order) = first_entry_order(db, &intent_id).await {
        record.execution_order_id = Some(order);
    }
    attach_intent_meta(db, &mut record, intent_id).await;
    record
}

async fn seed_rejected(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    _: &PgExecutionSubmissionRepository,
) -> DemoSeedRecord {
    let ids = seed_report(db, infra, "rejected").await;
    let mut record = base_record("rejected", &ids);
    let intent_id = seed_pending_intent(db, &ids).await;
    patch_intent_status(db, &intent_id, OrderIntentStatus::Rejected).await;
    attach_intent_meta(db, &mut record, intent_id).await;
    record
}

async fn seed_cancelled(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    _: &PgExecutionSubmissionRepository,
) -> DemoSeedRecord {
    let ids = seed_report(db, infra, "cancelled").await;
    let mut record = base_record("cancelled", &ids);
    let intent_id = seed_manual_approved_intent(db, &ids).await;
    PgOrderIntentRepository::new(db.clone())
        .cancel(&intent_id, "ui-demo operator cancelled".to_owned())
        .await
        .expect("cancel intent");
    attach_intent_meta(db, &mut record, intent_id).await;
    record
}

async fn seed_expired(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    _: &PgExecutionSubmissionRepository,
) -> DemoSeedRecord {
    let ids = seed_report(db, infra, "expired").await;
    let mut record = base_record("expired", &ids);
    let intent_id = seed_pending_intent(db, &ids).await;
    patch_intent_status(db, &intent_id, OrderIntentStatus::Expired).await;
    attach_intent_meta(db, &mut record, intent_id).await;
    record
}

async fn seed_failed(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    submission: &PgExecutionSubmissionRepository,
) -> DemoSeedRecord {
    let ids = seed_report(db, infra, "failed").await;
    let mut record = base_record("failed", &ids);
    let intent_id = seed_approved_intent(db, &ids).await;
    claim_entry_for_test(db, submission, &intent_id).await;
    let order = submission
        .create_entry_order_and_lock_capital(
            entry_execution_order(&intent_id, &ids),
            &ids.feature_parity_state_id,
        )
        .await
        .expect("create entry order");
    submission
        .record_submission_result(
            &order.execution_order_id,
            SubmissionLedgerWrite {
                state: ExecutionOrderState::Failed,
                intent_status: OrderIntentStatus::Failed,
                venue_order_id: None,
                venue_status: Some(VenueOrderStatus::Rejected),
                submitted_at: Utc::now(),
                filled_at: None,
                cancelled_at: Some(Utc::now()),
                error_message: Some("venue rejected: insufficient balance".to_owned()),
                capital: CapitalSettlement::ReleaseAll,
                fill: None,
                reconciliation: None,
            },
        )
        .await
        .expect("failed submission");
    record.execution_order_id = Some(order.execution_order_id);
    attach_intent_meta(db, &mut record, intent_id).await;
    record
}

async fn seed_reconciliation_queue(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    submission: &PgExecutionSubmissionRepository,
    summary: &mut UiDemoSeedSummary,
) {
    for slug in ["recon-open-a", "recon-open-b", "recon-resolved"] {
        let ids = seed_report(db, infra, slug).await;
        let mut record = base_record(slug, &ids);
        let intent_id = seed_approved_intent(db, &ids).await;
        let (intent_id, order_id) = ambiguous_order(db, submission, &ids, &intent_id).await;
        record.execution_order_id = Some(order_id.clone());
        attach_intent_meta(db, &mut record, intent_id.clone()).await;

        if slug == "recon-resolved" {
            submission
                .apply_reconciliation(&order_id, filled_reconciliation_write(&intent_id, &ids))
                .await
                .expect("resolve reconciliation");
        } else {
            summary.reconciliations += 1;
        }

        summary.reports += 1;
        summary.intents += 1;
        summary.execution_orders += 1;
        summary.records.push(record);
    }
}

async fn seed_settlement_redeems(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    submission: &PgExecutionSubmissionRepository,
    funder: &str,
    summary: &mut UiDemoSeedSummary,
) {
    let hold_intents = seed_settlement_hold_lots(db, infra, submission, summary).await;
    seed_settlement_isolated_markets(db, infra, summary).await;

    let redeem_repo = PgSettlementRedeemRepository::new(db.clone());
    seed_settlement_pending_redeem(&redeem_repo, funder, summary).await;
    seed_settlement_submitted_redeem(&redeem_repo, funder, summary).await;
    seed_settlement_failed_redeem(&redeem_repo, funder, summary).await;
    seed_confirmed_settlement_batch(db, &redeem_repo, funder, &hold_intents, summary).await;
}

async fn seed_settlement_hold_lots(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    submission: &PgExecutionSubmissionRepository,
    summary: &mut UiDemoSeedSummary,
) -> Vec<(ExecutionTxnIds, OrderIntentId)> {
    let mut hold_intents = Vec::new();
    for slug in ["settle-lot-a", "settle-lot-b", "settle-lot-c"] {
        let config = ReportSeedConfig {
            event_id: SETTLE_EVENT.to_owned(),
            market_id: SETTLE_MARKET.to_owned(),
            market_question: "UI demo: settlement redeem batch".to_owned(),
            market_slug: "ui-demo-settle".to_owned(),
            token_id: SETTLE_YES.to_owned(),
            trigger_key: format!("{DEMO_TAG}:report:{slug}:{}", demo_run_id()),
        };
        let ids = seed_report_on_infra(db, infra, config).await;
        if slug == "settle-lot-a" {
            align_settlement_market(db).await;
        }
        let intent_id = seed_approved_intent(db, &ids).await;
        patch_hold_to_resolution(db, &intent_id).await;
        fill_entry_lot(db, submission, &ids, &intent_id).await;
        hold_intents.push((ids, intent_id));
        summary.reports += 1;
        summary.intents += 1;
        summary.execution_orders += 1;
        summary.positions += 1;
    }
    hold_intents
}

async fn seed_settlement_isolated_markets(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    summary: &mut UiDemoSeedSummary,
) {
    for (slug, market_suffix) in [
        ("settle-pending-mkt", "ui-demo-settle-pending"),
        ("settle-submitted-mkt", "ui-demo-settle-submitted"),
        ("settle-failed-mkt", "ui-demo-settle-failed"),
    ] {
        seed_report_on_infra(
            db,
            infra,
            ReportSeedConfig {
                event_id: format!("{DEMO_TAG}-evt-{slug}"),
                market_id: market_suffix.to_owned(),
                market_question: format!("UI demo settlement market ({slug})"),
                market_slug: slug.to_owned(),
                token_id: format!("{DEMO_TAG}-token-{slug}"),
                trigger_key: format!("{DEMO_TAG}:report:{slug}:{}", demo_run_id()),
            },
        )
        .await;
        summary.reports += 1;
    }
}

async fn seed_settlement_pending_redeem(
    redeem_repo: &PgSettlementRedeemRepository,
    funder: &str,
    summary: &mut UiDemoSeedSummary,
) {
    redeem_repo
        .upsert_pending(new_settlement_redeem(
            SettlementRedeemId::from_v7(),
            &MarketId::new("ui-demo-settle-pending"),
            funder,
            SettlementRedeemState::Pending,
        ))
        .await
        .expect("pending redeem");
    summary.settlement_redeems += 1;
}

async fn seed_settlement_submitted_redeem(
    redeem_repo: &PgSettlementRedeemRepository,
    funder: &str,
    summary: &mut UiDemoSeedSummary,
) {
    let submitted_id = SettlementRedeemId::from_v7();
    let submitted_market = MarketId::new("ui-demo-settle-submitted");
    redeem_repo
        .upsert_pending(new_settlement_redeem(
            submitted_id.clone(),
            &submitted_market,
            funder,
            SettlementRedeemState::Pending,
        ))
        .await
        .expect("upsert submitted redeem");
    let submitted_row = redeem_repo
        .find_by_market_funder(&submitted_market, funder)
        .await
        .expect("load submitted redeem")
        .expect("submitted redeem row");
    redeem_repo
        .mark_submitted(
            &submitted_row.settlement_redeem_id,
            "0xuidemo1111111111111111111111111111111111111111111111111111111111".to_owned(),
            Utc::now(),
        )
        .await
        .expect("mark submitted");
    summary.settlement_redeems += 1;
}

async fn seed_settlement_failed_redeem(
    redeem_repo: &PgSettlementRedeemRepository,
    funder: &str,
    summary: &mut UiDemoSeedSummary,
) {
    let failed_market = MarketId::new("ui-demo-settle-failed");
    redeem_repo
        .upsert_pending(new_settlement_redeem(
            SettlementRedeemId::from_v7(),
            &failed_market,
            funder,
            SettlementRedeemState::Pending,
        ))
        .await
        .expect("upsert failed redeem");
    let failed_row = redeem_repo
        .find_by_market_funder(&failed_market, funder)
        .await
        .expect("load failed redeem")
        .expect("failed redeem row");
    redeem_repo
        .mark_failed(
            &failed_row.settlement_redeem_id,
            "simulated on-chain revert: payout vector mismatch".to_owned(),
            Some(Utc::now() + chrono::Duration::hours(1)),
            Utc::now(),
            false,
        )
        .await
        .expect("mark failed");
    summary.settlement_redeems += 1;
}

async fn seed_confirmed_settlement_batch(
    db: &DatabaseConnection,
    redeem_repo: &PgSettlementRedeemRepository,
    funder: &str,
    hold_intents: &[(ExecutionTxnIds, OrderIntentId)],
    summary: &mut UiDemoSeedSummary,
) {
    let market_id = MarketId::new(SETTLE_MARKET);
    let confirmed_id = SettlementRedeemId::from_v7();
    redeem_repo
        .upsert_pending(new_settlement_redeem(
            confirmed_id.clone(),
            &market_id,
            funder,
            SettlementRedeemState::Pending,
        ))
        .await
        .expect("upsert confirmed redeem");
    let confirmed_row = redeem_repo
        .find_by_market_funder(&market_id, funder)
        .await
        .expect("load confirmed redeem")
        .expect("confirmed redeem row");
    let lot_writes = build_confirmed_settlement_lots(
        db,
        hold_intents,
        &confirmed_row.settlement_redeem_id,
        summary,
    )
    .await;
    redeem_repo
        .confirm(ConfirmSettlementRedeem {
            settlement_redeem_id: confirmed_row.settlement_redeem_id,
            balance_after_json: zero_balance_evidence(),
            payout_usd: Usd::new(dec!(300)),
            gas_fee_pol: Some(dec!(0.002)),
            confirmed_at: Utc::now(),
            lots: lot_writes,
        })
        .await
        .expect("confirm redeem");
    summary.settlement_redeems += 1;
}

async fn build_confirmed_settlement_lots(
    db: &DatabaseConnection,
    hold_intents: &[(ExecutionTxnIds, OrderIntentId)],
    settlement_redeem_id: &SettlementRedeemId,
    summary: &mut UiDemoSeedSummary,
) -> Vec<SettlementRedeemLotWrite> {
    let mut lot_writes = Vec::new();
    for (ids, intent_id) in hold_intents {
        let position = PgPositionRepository::new(db.clone())
            .find_by_intent(intent_id)
            .await
            .expect("position")
            .expect("open lot");
        lot_writes.push(SettlementRedeemLotWrite {
            lot: NewSettlementRedeemLot {
                settlement_redeem_lot_id: SettlementRedeemLotId::from_v7(),
                settlement_redeem_id: settlement_redeem_id.clone(),
                position_id: position.position_id.clone(),
                order_intent_id: intent_id.clone(),
                token_id: TokenId::new(SETTLE_YES),
                side: OutcomeSide::Yes,
                shares_redeemed: position.shares,
                cost_basis_usd: position.cost_usd,
                payout_usd: position.shares * Price::new(dec!(1.0)),
                realized_pnl_usd: position.shares * Price::new(dec!(0.4)),
            },
            position_exit: PositionExit {
                shares: position.shares,
                avg_price: Price::new(dec!(1.0)),
                proceeds_usd: position.shares * Price::new(dec!(1.0)),
                realized_pnl_usd: position.shares * Price::new(dec!(0.4)),
                exited_at: Utc::now(),
                reason: ExitReason::ResolutionRedeem,
            },
        });
        summary.records.push(DemoSeedRecord {
            slug: format!("settle-{intent_id}"),
            report_id: ids.report.clone(),
            recommendation_id: ids.recommendation.clone(),
            market_id: MarketId::new(SETTLE_MARKET),
            token_id: TokenId::new(SETTLE_YES),
            intent_id: Some(intent_id.clone()),
            execution_order_id: None,
            position_id: Some(position.position_id),
            capital_allocation_id: None,
        });
    }
    lot_writes
}

async fn align_settlement_market(db: &DatabaseConnection) {
    let row = market::Entity::find_by_id(MarketId::new(SETTLE_MARKET))
        .one(db)
        .await
        .expect("load market")
        .expect("settlement market row");
    let mut active = row.into_active_model();
    active.yes_token_id = ActiveValue::Set(TokenId::new(SETTLE_YES));
    active.no_token_id = ActiveValue::Set(TokenId::new(SETTLE_NO));
    active.update(db).await.expect("align settlement market");
}

async fn patch_hold_to_resolution(db: &DatabaseConnection, intent_id: &OrderIntentId) {
    let row = quant_order_intent::Entity::find_by_id(intent_id.clone())
        .one(db)
        .await
        .expect("load intent")
        .expect("intent row");
    let mut exit = row.exit_policy_json.clone();
    exit.settlement_mode = ExitSettlementMode::HoldToResolution;
    exit.redeem_policy = RedeemPolicy::Auto;
    let mut active = row.into_active_model();
    active.exit_policy_json = ActiveValue::Set(exit);
    active.update(db).await.expect("patch exit policy");
}

async fn first_entry_order(
    db: &DatabaseConnection,
    intent_id: &OrderIntentId,
) -> Option<ExecutionOrderId> {
    quant_execution_order::Entity::find()
        .filter(quant_execution_order::Column::OrderIntentId.eq(intent_id.clone()))
        .filter(quant_execution_order::Column::OrderPhase.eq(ExecutionOrderPhase::Entry))
        .one(db)
        .await
        .expect("load entry order")
        .map(|row| row.execution_order_id)
}

async fn ambiguous_order(
    db: &DatabaseConnection,
    submission: &PgExecutionSubmissionRepository,
    ids: &ExecutionTxnIds,
    intent_id: &OrderIntentId,
) -> (OrderIntentId, ExecutionOrderId) {
    claim_entry_for_test(db, submission, intent_id).await;
    let order = submission
        .create_entry_order_and_lock_capital(
            entry_execution_order(intent_id, ids),
            &ids.feature_parity_state_id,
        )
        .await
        .expect("create entry order");
    submission
        .record_submission_result(
            &order.execution_order_id,
            SubmissionLedgerWrite {
                state: ExecutionOrderState::Ambiguous,
                intent_status: OrderIntentStatus::Submitted,
                venue_order_id: Some(OrderId::new("ui-demo-amb")),
                venue_status: None,
                submitted_at: Utc::now(),
                filled_at: None,
                cancelled_at: None,
                error_message: Some("venue timeout".to_owned()),
                capital: CapitalSettlement::Hold,
                fill: None,
                reconciliation: Some(pending_reconciliation(&order.execution_order_id, intent_id)),
            },
        )
        .await
        .expect("record ambiguous");
    (intent_id.clone(), order.execution_order_id)
}

fn pending_reconciliation(
    execution_order_id: &ExecutionOrderId,
    intent_id: &OrderIntentId,
) -> NewReconciliation {
    NewReconciliation {
        reconciliation_id: ReconciliationId::from_v7(),
        execution_order_id: execution_order_id.clone(),
        order_intent_id: intent_id.clone(),
        result: ReconciliationResult::Pending,
        evidence_json: ReconciliationEvidenceChain(vec![recon_evidence(
            "submit: ambiguous venue response",
        )]),
        venue_filled_shares: None,
        venue_avg_price: None,
        discrepancy_usd: None,
        resolved_by: None,
        resolved_at: None,
    }
}

fn unresolvable_reconciliation(
    execution_order_id: &ExecutionOrderId,
    intent_id: &OrderIntentId,
) -> NewReconciliation {
    NewReconciliation {
        reconciliation_id: ReconciliationId::from_v7(),
        execution_order_id: execution_order_id.clone(),
        order_intent_id: intent_id.clone(),
        result: ReconciliationResult::Unresolvable,
        evidence_json: ReconciliationEvidenceChain(vec![recon_evidence(
            "partial fill: venue status drift",
        )]),
        venue_filled_shares: None,
        venue_avg_price: None,
        discrepancy_usd: None,
        resolved_by: None,
        resolved_at: None,
    }
}

fn recon_evidence(detail: &str) -> ReconciliationEvidence {
    ReconciliationEvidence {
        kind: ReconciliationEvidenceKind::ClobOrderStatus,
        observed_at: Utc::now(),
        detail: detail.to_owned(),
        venue_ref: Some("ui-demo-venue".to_owned()),
        shares: None,
        price: None,
    }
}

fn filled_reconciliation_write(
    intent_id: &OrderIntentId,
    ids: &ExecutionTxnIds,
) -> ReconciliationLedgerWrite {
    ReconciliationLedgerWrite {
        order_state: ExecutionOrderState::Filled,
        intent_status: OrderIntentStatus::Filled,
        venue_status: Some(VenueOrderStatus::Filled),
        venue_order_id: Some(OrderId::new("ui-demo-amb")),
        filled_at: Some(Utc::now()),
        cancelled_at: None,
        error_message: None,
        capital: CapitalReconcileSettlement::Settle {
            spent_usd: Usd::new(EXECUTION_NOTIONAL),
        },
        fill: Some(execution_pg_seed::position_fill_public(
            ids,
            intent_id,
            Shares::new(dec!(100)),
            Usd::new(EXECUTION_NOTIONAL),
        )),
        exit: None,
        exit_fully: false,
        exit_state: None,
        revert_lot: false,
        result: ReconciliationResult::Filled,
        evidence: ReconciliationEvidenceChain(vec![recon_evidence(
            "operator resolved: venue filled",
        )]),
        venue_filled_shares: Some(Shares::new(dec!(100))),
        venue_avg_price: Some(Price::new(dec!(0.6))),
        discrepancy_usd: Some(Usd::ZERO),
        resolved_by: Some("ui-demo-operator".to_owned()),
        resolved_at: Some(Utc::now()),
    }
}

fn new_settlement_redeem(
    settlement_redeem_id: SettlementRedeemId,
    market_id: &MarketId,
    funder: &str,
    state: SettlementRedeemState,
) -> NewSettlementRedeem {
    NewSettlementRedeem {
        settlement_redeem_id,
        market_id: market_id.clone(),
        funder_address: funder.to_owned(),
        wallet_kind: ExecutionWalletKind::Proxy,
        state,
        tx_hash: None,
        index_sets_json: SettlementRedeemIndexSets {
            index_sets: vec![1, 2],
        },
        payout_vector_json: SettlementPayoutVector {
            denominator: "1".to_owned(),
            yes: "1".to_owned(),
            no: "0".to_owned(),
        },
        balance_before_json: matched_yes_balance_evidence(),
        balance_after_json: None,
        payout_usd: Usd::ZERO,
        gas_fee_pol: None,
        attempt_count: 0,
        next_attempt_at: None,
        last_error: None,
        submitted_at: None,
        confirmed_at: None,
        failed_at: None,
    }
}

fn matched_yes_balance_evidence() -> SettlementBalanceEvidence {
    SettlementBalanceEvidence {
        yes: SettlementTokenBalance {
            token_id: SETTLE_YES.to_owned(),
            index_set: 1,
            raw_balance: "300000000".to_owned(),
            shares: "300".to_owned(),
        },
        no: SettlementTokenBalance {
            token_id: SETTLE_NO.to_owned(),
            index_set: 2,
            raw_balance: "0".to_owned(),
            shares: "0".to_owned(),
        },
    }
}

fn zero_balance_evidence() -> SettlementBalanceEvidence {
    SettlementBalanceEvidence {
        yes: SettlementTokenBalance {
            token_id: SETTLE_YES.to_owned(),
            index_set: 1,
            raw_balance: "0".to_owned(),
            shares: "0".to_owned(),
        },
        no: SettlementTokenBalance {
            token_id: SETTLE_NO.to_owned(),
            index_set: 2,
            raw_balance: "0".to_owned(),
            shares: "0".to_owned(),
        },
    }
}

/// Phase 10.3 report-plane fixtures: empty/revoked/expired reports, diff pair,
/// and a published recommendation with no blocking intent (create-intent entry).
async fn seed_report_plane_scenarios(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    summary: &mut UiDemoSeedSummary,
) {
    seed_actionable_report(db, infra, summary).await;
    seed_empty_report(db, infra, summary).await;
    seed_revoked_report(db, infra, summary).await;
    seed_expired_report(db, infra, summary).await;
    seed_diff_report_pair(db, infra, summary).await;
}

async fn seed_actionable_report(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    summary: &mut UiDemoSeedSummary,
) {
    let ids = seed_report(db, infra, "actionable").await;
    summary.actionable_recommendation_id = Some(ids.recommendation.clone());
    summary.reports += 1;
    summary.records.push(base_record("actionable", &ids));
}

async fn seed_empty_report(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    summary: &mut UiDemoSeedSummary,
) {
    let record =
        seed_custom_report(db, infra, "empty", ReportBuildOptions::published_empty()).await;
    summary.reports += 1;
    summary.records.push(record);
}

async fn seed_revoked_report(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    summary: &mut UiDemoSeedSummary,
) {
    let ids = seed_report(db, infra, "revoked").await;
    let report_repo = PgRecommendationReportRepository::new(db.clone());
    report_repo
        .revoke(
            &ids.report,
            "ui-demo operator revoked stale report",
            Utc::now(),
            report_operation_log(&ids),
        )
        .await
        .expect("revoke report");
    summary.reports += 1;
    summary.records.push(base_record("revoked", &ids));
}

async fn seed_expired_report(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    summary: &mut UiDemoSeedSummary,
) {
    let ids = seed_report(db, infra, "report-expired").await;
    let recommendation_repo = PgRecommendationRepository::new(db.clone());
    recommendation_repo
        .expire(&ids.recommendation, report_operation_log(&ids))
        .await
        .expect("expire recommendation");
    let report_repo = PgRecommendationReportRepository::new(db.clone());
    report_repo
        .roll_up_to_expired(&ids.report, Utc::now(), report_operation_log(&ids))
        .await
        .expect("roll up expired report")
        .expect("report expired");
    summary.reports += 1;
    summary.records.push(base_record("report-expired", &ids));
}

async fn seed_diff_report_pair(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    summary: &mut UiDemoSeedSummary,
) {
    let base_ids = seed_diff_report(
        db,
        infra,
        "diff-base",
        Utc::now() - chrono::Duration::hours(2),
        vec![(
            "ui-demo-mkt-diff-a",
            "ui-demo-evt-diff-a",
            "ui-demo-token-diff-a",
        )],
    )
    .await;
    let current_ids = seed_diff_report(
        db,
        infra,
        "diff-current",
        Utc::now() - chrono::Duration::hours(1),
        vec![
            (
                "ui-demo-mkt-diff-a",
                "ui-demo-evt-diff-a",
                "ui-demo-token-diff-a",
            ),
            (
                "ui-demo-mkt-diff-b",
                "ui-demo-evt-diff-b",
                "ui-demo-token-diff-b",
            ),
        ],
    )
    .await;
    summary.diff_base_report_id = Some(base_ids.report.clone());
    summary.diff_current_report_id = Some(current_ids.report.clone());
    summary.reports += 2;
    summary.records.push(base_record("diff-base", &base_ids));
    summary
        .records
        .push(base_record("diff-current", &current_ids));
}

async fn seed_diff_report(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    slug: &str,
    as_of: chrono::DateTime<Utc>,
    markets: Vec<(&str, &str, &str)>,
) -> ExecutionTxnIds {
    for (market_id, event_id, _token_id) in &markets {
        seed_market_catalog_for_diff(db, event_id, market_id, slug).await;
    }
    let config = demo_report(slug);
    let market_selection_id =
        seed_market_selection_for_diff(db, &infra.runtime_config_version_id).await;
    let report_id = RecommendationReportId::from_v7();
    let primary = markets.first().expect("at least one market");
    let ids = ExecutionTxnIds {
        feature_parity_state_id: infra.feature_parity_state_id.clone(),
        account_snapshot: AccountSnapshotId::from_v7(),
        data_quality_snapshot: ReportDataQualitySnapshotId::from_v7(),
        portfolio_plan: PortfolioPlanId::from_v7(),
        report: report_id.clone(),
        recommendation: RecommendationId::from_v7(),
        condition_instance: EntryConditionInstanceId::from_v7(),
        model_version: infra.model_version_id.clone(),
        model_run: infra.model_run_id.clone(),
        market_selection: market_selection_id,
        runtime_config_version: infra.runtime_config_version_id.clone(),
        trade_policy: infra.trade_policy.clone(),
        market: primary.0.to_owned(),
        event: primary.1.to_owned(),
        token: primary.2.to_owned(),
    };
    let recommendations = markets
        .iter()
        .enumerate()
        .map(|(idx, (market_id, event_id, token_id))| {
            demo_recommendation(
                RecommendationId::from_v7(),
                report_id.clone(),
                &ids,
                i32::try_from(idx + 1).expect("rank"),
                market_id,
                event_id,
                token_id,
            )
        })
        .collect::<Vec<_>>();
    let mut options = ReportBuildOptions::published_single(&ids);
    options.recommendations = recommendations;
    options.as_of = as_of;
    options.published_at = Some(as_of);
    PgRecommendationReportRepository::new(db.clone())
        .create_report(build_custom_report_transaction(
            &ids,
            &config.trigger_key,
            options,
        ))
        .await
        .expect("create diff report");
    ids
}

async fn seed_custom_report(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    slug: &str,
    options: ReportBuildOptions,
) -> DemoSeedRecord {
    let config = demo_report(slug);
    seed_market_catalog_for_diff(
        db,
        &format!("{DEMO_TAG}-evt-{slug}"),
        &format!("{DEMO_TAG}-mkt-{slug}"),
        slug,
    )
    .await;
    let market_selection_id =
        seed_market_selection_for_diff(db, &infra.runtime_config_version_id).await;
    let ids = ExecutionTxnIds {
        feature_parity_state_id: infra.feature_parity_state_id.clone(),
        account_snapshot: AccountSnapshotId::from_v7(),
        data_quality_snapshot: ReportDataQualitySnapshotId::from_v7(),
        portfolio_plan: PortfolioPlanId::from_v7(),
        report: RecommendationReportId::from_v7(),
        recommendation: RecommendationId::from_v7(),
        condition_instance: EntryConditionInstanceId::from_v7(),
        model_version: infra.model_version_id.clone(),
        model_run: infra.model_run_id.clone(),
        market_selection: market_selection_id,
        runtime_config_version: infra.runtime_config_version_id.clone(),
        trade_policy: infra.trade_policy.clone(),
        market: format!("{DEMO_TAG}-mkt-{slug}"),
        event: format!("{DEMO_TAG}-evt-{slug}"),
        token: format!("{DEMO_TAG}-token-{slug}"),
    };
    PgRecommendationReportRepository::new(db.clone())
        .create_report(build_custom_report_transaction(
            &ids,
            &config.trigger_key,
            options,
        ))
        .await
        .expect("create custom report");
    base_record(slug, &ids)
}

async fn seed_market_catalog_for_diff(
    db: &DatabaseConnection,
    event_id: &str,
    market_id: &str,
    slug: &str,
) {
    PgEventRepository::new(db.clone())
        .upsert(make_event(
            event_id,
            &format!("UI demo event ({slug})"),
            &format!("ui-demo-event-{slug}"),
            MarketCategory::Politics,
        ))
        .await
        .expect("seed diff event");
    PgMarketRepository::new(db.clone())
        .upsert(make_market(
            market_id,
            event_id,
            &format!("UI demo diff market ({slug})"),
            &format!("ui-demo-{slug}"),
            MarketCategory::Politics,
            None,
        ))
        .await
        .expect("seed diff market");
}

async fn seed_market_selection_for_diff(
    db: &DatabaseConnection,
    runtime_config_version_id: &RuntimeConfigVersionId,
) -> MarketSelectionId {
    let id = MarketSelectionId::from_v7();
    PgMarketSelectionRepository::new(db.clone())
        .create_snapshot(
            NewMarketSelection {
                market_selection_id: id.clone(),
                decision_at: Utc::now(),
                runtime_config_version_id: runtime_config_version_id.clone(),
                selector_hash: ContentHash::parse(format!("blake3:{}", "d".repeat(64)))
                    .expect("hash"),
                market_count: 1,
                exclusion_summary: SelectionExclusionSummary::default(),
            },
            Vec::new(),
        )
        .await
        .expect("diff market selection");
    id
}

async fn seed_filled_closed_attribution(db: &DatabaseConnection, summary: &mut UiDemoSeedSummary) {
    let Some(record) = summary
        .records
        .iter()
        .find(|row| row.slug == "filled-closed")
    else {
        return;
    };
    let attribution_repo = PgAttributionRepository::new(db.clone());
    let outcome = attribution_repo
        .insert_final_and_mark_attributed(NewRecommendationAttribution {
            recommendation_id: record.recommendation_id.clone(),
            outcome: RecommendationAttributionOutcome::FilledExited,
            entry_outcome_json: EntryOutcome {
                entry_filled: true,
                fill_price: Some(Price::new(dec!(0.6))),
                fill_shares: Some(Shares::new(dec!(100))),
                entry_slippage_bps: None,
                filled_at: Some(Utc::now()),
            },
            exit_outcome_json: ExitOutcome {
                exit_price: Some(Price::new(dec!(0.55))),
                exit_shares: Some(Shares::new(dec!(100))),
                exit_compliance: true,
                exited_at: Some(Utc::now()),
                ..ExitOutcome::default()
            },
            realized_pnl_usd: Some(Usd::new(dec!(-5))),
            max_adverse_excursion_bps: Some(dec!(120)),
            max_favorable_excursion_bps: Some(dec!(80)),
            label_available_at: Some(Utc::now()),
            attribution_json: AttributionDetail {
                hit_stop_loss: true,
                notes: vec!["ui-demo: filled then stop-loss exit".to_owned()],
                ..AttributionDetail::default()
            },
        })
        .await
        .expect("insert attribution");
    if matches!(outcome, InsertFinalOutcome::Written(_)) {
        summary.attributions += 1;
    }
}
