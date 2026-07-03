//! Phase 05.10 — settlement redeem **unit tier** (no Docker).
//!
//! Pure-logic coverage (`is_auto_redeem_candidate`, balance validation, lot writes)
//! lives in [`quant_pivot_core::execution::settlement_redeem::tests`].
//!
//! Postgres + mock CTF integration: [`phase_05_10_settlement_redeem`] (`#[ignore = "requires Docker"]`).

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use chrono::{Duration, TimeZone, Utc};
use quant_pivot_core::{
    execution::{
        SettlementCtfBalances, SettlementCtfClient, SettlementCtfPayoutVector,
        SettlementCtfSubmittedRedeemReceipt, SettlementRedeemService, SettlementRedeemServiceDeps,
        SettlementRedeemTx,
    },
    governance::{KillSwitchHandle, RuntimeModeHandle},
    observability::{
        capital_allocation_fact_writer::CapitalAllocationEventWriter,
        position_fact_writer::PositionEventWriter,
    },
    runtime_config::RuntimeConfigStore,
};
use quant_pivot_error::{
    rpc::RpcError,
    storage::{StorageError, entity},
};
use quant_pivot_models::{
    domain::{
        ApproveOrderIntent, ApproveOrderIntentOutcome, CapitalAllocationInfo,
        ConfirmSettlementRedeem, ExitTrainingLotRow, MarketInfo, NewCapitalAllocation,
        NewOperationLog, NewOrderIntent, NewSettlementRedeem, OrderIntentInfo,
        OrderIntentListQuery, Paginated, PositionExit, PositionFill, PositionInfo,
        PositionListQuery, RecommendationInfo, SettlementRedeemInfo, SettlementRedeemListQuery,
        SettlementRedeemLotInfo,
    },
    enums::{
        common::{MarketCategory, OrderType, Side},
        execution::{ApprovalInvalidation, ExitState, OrderIntentKind, PositionLedgerState},
        quant::{
            AccountSource, ApprovalStatus, ExecutionWalletKind, ExitSettlementMode,
            OrderIntentStatus, OutcomeSide, QuantRuntimeMode, RedeemPolicy,
        },
    },
    runtime_config::RuntimeConfig,
    types::{
        Bps, EntryOrderSpec, EventId, ExecutedPartialExitNodes, ExitPolicySpec, MarketId,
        ModelVersionId, OpportunisticExitState, OrderIntentId, PositionId, Price, RecommendationId,
        RecommendationReportId, RuntimeConfigVersionId, SettlementRedeemId, Shares, TokenId, Usd,
    },
};
use quant_pivot_repository::traits::{
    CapitalAllocationRepository, MarketRepository, OrderIntentRepository, PositionRepository,
    SettlementRedeemRepository,
};
use quant_pivot_storage::write::{AsyncWriter, AsyncWriterConfig, AsyncWriterObservability};
use quant_pivot_test_support::report_fixtures;
use rust_decimal_macros::dec;

const NOW_SECS: i64 = 1_700_020_000;
const MARKET_ID: &str = "0xsettle-mkt";
const YES_TOKEN: &str = "yes-token";

fn now() -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(NOW_SECS, 0).unwrap()
}

fn recommendation() -> RecommendationInfo {
    report_fixtures::recommendation(
        RecommendationReportId::from_v7(),
        RecommendationId::from_v7(),
        1,
        MARKET_ID,
        OutcomeSide::Yes,
        Usd::new(dec!(100)),
    )
}

fn intent(redeem_policy: RedeemPolicy) -> OrderIntentInfo {
    let rec = recommendation();
    OrderIntentInfo {
        order_intent_id: OrderIntentId::from_v7(),
        recommendation_id: rec.recommendation_id.clone(),
        runtime_mode: QuantRuntimeMode::AutoExecution,
        runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
        model_version_id: ModelVersionId::from_v7(),
        intent_kind: OrderIntentKind::Buy,
        status: OrderIntentStatus::Submitted,
        approval_status: ApprovalStatus::NotRequired,
        approved_by: None,
        approval_reason: None,
        approved_at: None,
        policy_id: Some("auto".to_owned()),
        policy_hash: None,
        status_reason: None,
        admission_trace_ref: None,
        entry_order_json: EntryOrderSpec {
            token_id: rec.token_id.clone(),
            side: Side::Buy,
            order_type: OrderType::Gtc,
            limit_price: Price::new(dec!(0.6)),
            shares: Shares::new(dec!(10)),
            max_slippage_bps: Bps::new(dec!(50)),
            valid_until: rec.entry_plan.valid_until,
        },
        exit_policy_json: ExitPolicySpec {
            take_profit_price: rec.exit_plan.take_profit_price,
            take_profit_pct: rec.exit_plan.take_profit_pct,
            stop_loss_price: rec.exit_plan.stop_loss_price,
            stop_loss_pct: rec.exit_plan.stop_loss_pct,
            time_exit_at: None,
            max_hold_secs: rec.exit_plan.max_hold_secs,
            trailing_stop: rec.exit_plan.trailing_stop.clone(),
            signal_invalidation_rules: rec.exit_plan.signal_invalidation_rules.clone(),
            partial_exit_nodes: Vec::new(),
            settlement_mode: ExitSettlementMode::HoldToResolution,
            redeem_policy,
            manual_review_at: rec.exit_plan.manual_review_at,
            entry_reference_price: rec.entry_plan.limit_price.unwrap_or(Price::ZERO),
            entry_composite_score: rec.composite_score,
        },
        risk_envelope_hash: rec.risk_envelope.canonical_hash().expect("hash"),
        expires_at: now() + Duration::hours(1),
        exit_state: ExitState::Monitoring,
        exit_reason: None,
        next_check_at: None,
        peak_mark_price: None,
        last_signal_recheck_at: None,
        executed_partial_exit_node_ids: ExecutedPartialExitNodes::default(),
        pending_partial_exit_node_id: None,
        opportunistic_exit_state: OpportunisticExitState::default(),
        created_at: now(),
        updated_at: now(),
    }
}

fn open_lot(intent_id: &OrderIntentId, position_id: PositionId) -> PositionInfo {
    PositionInfo {
        position_id,
        order_intent_id: intent_id.clone(),
        token_id: TokenId::new(YES_TOKEN),
        market_id: MarketId::new(MARKET_ID),
        event_id: Some(EventId::new("evt")),
        category: MarketCategory::Politics,
        side: OutcomeSide::Yes,
        state: PositionLedgerState::Open,
        shares: Shares::new(dec!(10)),
        avg_price: Price::new(dec!(0.5)),
        cost_usd: Usd::new(dec!(5)),
        realized_pnl_usd: Usd::ZERO,
        source: AccountSource::Polymarket,
        opened_at: now(),
        updated_at: now(),
        closed_at: None,
    }
}

struct StubPositions {
    lots: Vec<PositionInfo>,
}

#[async_trait]
impl PositionRepository for StubPositions {
    async fn apply_fill(&self, _: PositionFill) -> Result<PositionInfo, StorageError> {
        unimplemented!()
    }

    async fn apply_exit(
        &self,
        _: &OrderIntentId,
        _: PositionExit,
    ) -> Result<PositionInfo, StorageError> {
        unimplemented!()
    }

    async fn find_by_intent(
        &self,
        _: &OrderIntentId,
    ) -> Result<Option<PositionInfo>, StorageError> {
        Ok(None)
    }

    async fn find_by_id(&self, _: &PositionId) -> Result<Option<PositionInfo>, StorageError> {
        Ok(None)
    }

    async fn page(
        &self,
        query: PositionListQuery,
    ) -> Result<Paginated<PositionInfo>, StorageError> {
        Ok(Paginated::from_request(
            Vec::new(),
            0,
            &query.normalized().page,
        ))
    }

    async fn find_open_lots(&self) -> Result<Vec<PositionInfo>, StorageError> {
        Ok(self.lots.clone())
    }

    async fn find_lots_by_token(&self, _: &TokenId) -> Result<Vec<PositionInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn find_open_by_market(&self, _: &MarketId) -> Result<Vec<PositionInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn realized_pnl_cumulative_usd(&self) -> Result<Usd, StorageError> {
        Ok(Usd::ZERO)
    }

    async fn find_exit_training_lots(
        &self,
        _: chrono::DateTime<Utc>,
        _: chrono::DateTime<Utc>,
        _: u64,
    ) -> Result<Vec<ExitTrainingLotRow>, StorageError> {
        Ok(Vec::new())
    }
}

struct StubIntents {
    by_id: HashMap<OrderIntentId, OrderIntentInfo>,
}

#[async_trait]
impl OrderIntentRepository for StubIntents {
    async fn create_with_allocation(
        &self,
        _: NewOrderIntent,
        _: NewCapitalAllocation,
    ) -> Result<OrderIntentInfo, StorageError> {
        Err(StorageError::state_conflict(
            entity::QUANT_ORDER_INTENT,
            None::<&str>,
            "stub",
        ))
    }

    async fn approve(
        &self,
        _: &OrderIntentId,
        _: ApproveOrderIntent,
        _: Option<EntryOrderSpec>,
        _: Option<Usd>,
        _: chrono::DateTime<Utc>,
    ) -> Result<ApproveOrderIntentOutcome, StorageError> {
        unimplemented!()
    }

    async fn reject(&self, _: &OrderIntentId, _: String) -> Result<OrderIntentInfo, StorageError> {
        unimplemented!()
    }

    async fn cancel(&self, _: &OrderIntentId, _: String) -> Result<OrderIntentInfo, StorageError> {
        unimplemented!()
    }

    async fn expire(
        &self,
        _: &OrderIntentId,
        _: NewOperationLog,
    ) -> Result<OrderIntentInfo, StorageError> {
        unimplemented!()
    }

    async fn invalidate(
        &self,
        _: &OrderIntentId,
        _: ApprovalInvalidation,
        _: NewOperationLog,
    ) -> Result<OrderIntentInfo, StorageError> {
        unimplemented!()
    }

    async fn find_by_id(
        &self,
        intent_id: &OrderIntentId,
    ) -> Result<Option<OrderIntentInfo>, StorageError> {
        Ok(self.by_id.get(intent_id).cloned())
    }

    async fn page(
        &self,
        query: OrderIntentListQuery,
    ) -> Result<Paginated<OrderIntentInfo>, StorageError> {
        Ok(Paginated::from_request(
            Vec::new(),
            0,
            &query.normalized().page,
        ))
    }

    async fn find_expired(
        &self,
        _: chrono::DateTime<Utc>,
    ) -> Result<Vec<OrderIntentInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn upcoming_expirations(
        &self,
        _: chrono::DateTime<Utc>,
        _: u64,
    ) -> Result<Vec<(OrderIntentId, chrono::DateTime<Utc>)>, StorageError> {
        Ok(Vec::new())
    }

    async fn find_active_by_recommendation(
        &self,
        _: &RecommendationId,
    ) -> Result<Option<OrderIntentInfo>, StorageError> {
        Ok(None)
    }

    async fn find_active_intents_by_recommendation(
        &self,
        _: &RecommendationId,
    ) -> Result<Vec<OrderIntentInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn find_active_by_report(
        &self,
        _: &RecommendationReportId,
    ) -> Result<Vec<OrderIntentInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn find_blocking_by_report(
        &self,
        _: &RecommendationReportId,
    ) -> Result<Vec<OrderIntentInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn count_open(&self) -> Result<u64, StorageError> {
        Ok(0)
    }

    async fn find_attribution_candidates(
        &self,
        _: Vec<OrderIntentStatus>,
        _: u64,
    ) -> Result<Vec<OrderIntentInfo>, StorageError> {
        Ok(Vec::new())
    }
}

struct EmptyMarkets;

#[async_trait]
impl MarketRepository for EmptyMarkets {
    async fn find_by_id(&self, _: &MarketId) -> Result<Option<Arc<MarketInfo>>, StorageError> {
        Ok(None)
    }

    async fn find_by_ids(&self, _: &[MarketId]) -> Result<Vec<Arc<MarketInfo>>, StorageError> {
        Ok(Vec::new())
    }

    async fn page(
        &self,
        query: quant_pivot_models::domain::MarketPageQuery,
    ) -> Result<Paginated<MarketInfo>, StorageError> {
        Ok(Paginated::from_request(
            Vec::new(),
            0,
            &query.normalized().page,
        ))
    }

    async fn find_active(&self) -> Result<Arc<[MarketInfo]>, StorageError> {
        Ok(Vec::new().into())
    }

    async fn find_by_event(&self, _: &str) -> Result<Vec<Arc<MarketInfo>>, StorageError> {
        Ok(Vec::new())
    }

    async fn find_existing_ids(&self, _: &[MarketId]) -> Result<HashSet<String>, StorageError> {
        Ok(HashSet::new())
    }

    async fn upsert(
        &self,
        _: quant_pivot_models::domain::UpsertMarket,
    ) -> Result<Arc<MarketInfo>, StorageError> {
        Err(StorageError::state_conflict(
            entity::MARKET,
            None::<&str>,
            "stub",
        ))
    }

    async fn upsert_batch(
        &self,
        _: Vec<quant_pivot_models::domain::UpsertMarket>,
    ) -> Result<u64, StorageError> {
        Ok(0)
    }

    async fn update_status(
        &self,
        _: &MarketId,
        _: &str,
        _: Option<&str>,
    ) -> Result<(), StorageError> {
        Ok(())
    }
}

struct EmptySettlementRedeems;

#[async_trait]
impl SettlementRedeemRepository for EmptySettlementRedeems {
    async fn find_by_id(
        &self,
        _: &SettlementRedeemId,
    ) -> Result<Option<SettlementRedeemInfo>, StorageError> {
        Ok(None)
    }

    async fn find_by_market_funder(
        &self,
        _: &MarketId,
        _: &str,
    ) -> Result<Option<SettlementRedeemInfo>, StorageError> {
        Ok(None)
    }

    async fn page(
        &self,
        query: SettlementRedeemListQuery,
    ) -> Result<Paginated<SettlementRedeemInfo>, StorageError> {
        Ok(Paginated::from_request(
            Vec::new(),
            0,
            &query.normalized().page,
        ))
    }

    async fn list_lots_by_redeem(
        &self,
        _: &SettlementRedeemId,
    ) -> Result<Vec<SettlementRedeemLotInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn upsert_pending(
        &self,
        _: NewSettlementRedeem,
    ) -> Result<SettlementRedeemInfo, StorageError> {
        unimplemented!()
    }

    async fn mark_submitted(
        &self,
        _: &SettlementRedeemId,
        _: String,
        _: chrono::DateTime<Utc>,
    ) -> Result<SettlementRedeemInfo, StorageError> {
        unimplemented!()
    }

    async fn mark_failed(
        &self,
        _: &SettlementRedeemId,
        _: String,
        _: Option<chrono::DateTime<Utc>>,
        _: chrono::DateTime<Utc>,
        _: bool,
    ) -> Result<SettlementRedeemInfo, StorageError> {
        unimplemented!()
    }

    async fn confirm(
        &self,
        _: ConfirmSettlementRedeem,
    ) -> Result<SettlementRedeemInfo, StorageError> {
        unimplemented!()
    }
}

struct StubCapital;

#[async_trait]
impl CapitalAllocationRepository for StubCapital {
    async fn find_by_intent(
        &self,
        _: &OrderIntentId,
    ) -> Result<Option<CapitalAllocationInfo>, StorageError> {
        Ok(None)
    }

    async fn sum_reserved_usd(&self) -> Result<Usd, StorageError> {
        Ok(Usd::ZERO)
    }

    async fn has_impaired(&self) -> Result<bool, StorageError> {
        Ok(false)
    }
}

struct UnreachableCtf;

#[async_trait]
impl SettlementCtfClient for UnreachableCtf {
    async fn binary_payout_vector(
        &self,
        _: &MarketId,
    ) -> Result<SettlementCtfPayoutVector, RpcError> {
        unimplemented!("must not reach CTF when market catalog row is missing")
    }

    async fn binary_balances(
        &self,
        _: &str,
        _: &TokenId,
        _: &TokenId,
    ) -> Result<SettlementCtfBalances, RpcError> {
        unimplemented!()
    }

    async fn simulate_standard_binary_redeem(&self, _: &MarketId) -> Result<(), RpcError> {
        unimplemented!()
    }

    async fn submit_standard_binary_redeem(
        &self,
        _: &MarketId,
    ) -> Result<Box<dyn SettlementRedeemTx>, RpcError> {
        unimplemented!()
    }

    async fn submitted_redeem_receipt(
        &self,
        _: &str,
        _: u64,
    ) -> Result<SettlementCtfSubmittedRedeemReceipt, RpcError> {
        unimplemented!()
    }
}

fn noop_capital_writer() -> Arc<CapitalAllocationEventWriter> {
    let (writer, _worker) = AsyncWriter::new(
        AsyncWriterConfig::new("unit_capital_events"),
        |_| Box::pin(async move { Ok(()) }),
        prometheus::IntCounter::new("unit_capital_events_dropped", "test").unwrap(),
        AsyncWriterObservability::default(),
    );
    Arc::new(CapitalAllocationEventWriter::new(Arc::new(writer)))
}

fn noop_position_writer() -> Arc<PositionEventWriter> {
    let (writer, _worker) = AsyncWriter::new(
        AsyncWriterConfig::new("unit_position_events"),
        |_| Box::pin(async move { Ok(()) }),
        prometheus::IntCounter::new("unit_position_events_dropped", "test").unwrap(),
        AsyncWriterObservability::default(),
    );
    Arc::new(PositionEventWriter::new(Arc::new(writer)))
}

fn settlement_service(positions: StubPositions, intents: StubIntents) -> SettlementRedeemService {
    let mut config = RuntimeConfig::default();
    config.execution.settlement_redeem.enabled = true;
    config.execution.settlement_redeem.batch_size = 4;
    SettlementRedeemService::new(SettlementRedeemServiceDeps {
        positions: Arc::new(positions) as Arc<dyn PositionRepository>,
        intents: Arc::new(intents) as Arc<dyn OrderIntentRepository>,
        markets: Arc::new(EmptyMarkets) as Arc<dyn MarketRepository>,
        settlement_redeems: Arc::new(EmptySettlementRedeems) as Arc<dyn SettlementRedeemRepository>,
        capital: Arc::new(StubCapital) as Arc<dyn CapitalAllocationRepository>,
        ctf: Arc::new(UnreachableCtf) as Arc<dyn SettlementCtfClient>,
        runtime_mode: RuntimeModeHandle::new(QuantRuntimeMode::AutoExecution),
        kill_switch: KillSwitchHandle::default(),
        config: Arc::new(RuntimeConfigStore::new(config)),
        funder_address: "0xfunder".to_owned(),
        wallet_kind: ExecutionWalletKind::Eoa,
        capital_events: noop_capital_writer(),
        position_events: noop_position_writer(),
    })
}

#[tokio::test]
async fn run_pass_scans_auto_hold_lots_only() {
    let auto = intent(RedeemPolicy::Auto);
    let manual = intent(RedeemPolicy::Manual);
    let auto_lot = open_lot(&auto.order_intent_id, PositionId::from_v7());
    let manual_lot = open_lot(&manual.order_intent_id, PositionId::from_v7());
    let mut by_id = HashMap::new();
    by_id.insert(auto.order_intent_id.clone(), auto);
    by_id.insert(manual.order_intent_id.clone(), manual);

    let service = settlement_service(
        StubPositions {
            lots: vec![auto_lot, manual_lot],
        },
        StubIntents { by_id },
    );

    let summary = service.run_pass(now()).await.expect("run pass");
    assert_eq!(
        summary.candidates, 1,
        "only auto-redeem hold lots are grouped"
    );
    assert_eq!(
        summary.skipped, 1,
        "missing market catalog row skips the group"
    );
}
