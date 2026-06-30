//! Phase 05.10 — settlement redeem service integration tests (Postgres + mock CTF).
//!
//! Requires Docker. Exercises the hold-to-resolution auto-redeem sweep against real
//! ledger writes while stubbing on-chain CTF/relayer I/O.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use chrono::{Duration, Utc};
use quant_pivot_core::{
    execution::{
        SettlementCtfBalances, SettlementCtfClient, SettlementCtfPayoutVector,
        SettlementCtfRedeemReceipt, SettlementCtfSubmittedRedeemReceipt, SettlementRedeemService,
        SettlementRedeemServiceDeps, SettlementRedeemTx,
    },
    governance::{KillSwitchHandle, RuntimeModeHandle},
    runtime_config::RuntimeConfigStore,
};
use quant_pivot_error::rpc::RpcError;
use quant_pivot_models::{
    constants::COLLATERAL_SCALE,
    domain::NewSettlementRedeem,
    entities::{market, quant_order_intent, quant_position, quant_settlement_redeem},
    enums::{
        execution::{
            CapitalAllocationState, ExitReason, ExitState, PositionLedgerState,
            SettlementRedeemState,
        },
        quant::{ExecutionWalletKind, ExitSettlementMode, QuantRuntimeMode, RedeemPolicy},
    },
    runtime_config::RuntimeConfig,
    types::{
        MarketId, OrderIntentId, SettlementBalanceEvidence, SettlementPayoutVector,
        SettlementRedeemId, SettlementRedeemIndexSets, SettlementTokenBalance, TokenId,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgCapitalAllocationRepository, PgExecutionSubmissionRepository, PgMarketRepository,
        PgOrderIntentRepository, PgPositionRepository, PgSettlementRedeemRepository,
    },
    traits::{
        CapitalAllocationRepository, MarketRepository, OrderIntentRepository, PositionRepository,
        SettlementRedeemRepository,
    },
};
use quant_pivot_test_support::{
    execution_pg_seed::{
        ExecutionTxnIds, fill_entry_lot, seed_approved_intent, seed_report_fixture,
    },
    pg::setup_pg,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, EntityTrait, IntoActiveModel, QueryFilter,
};
use std::str::FromStr;

const FUNDER: &str = "0x0000000000000000000000000000000000000001";
const YES_TOKEN: &str = "token-1";
const NO_TOKEN: &str = "token-2";
const TX_HASH: &str = "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef";
const SHARES_RAW: &str = "100000000";

#[derive(Default)]
struct MockCtfFlags {
    simulate_fail: bool,
    submit_fail: bool,
}

struct MockCtfState {
    payout: SettlementCtfPayoutVector,
    balances: SettlementCtfBalances,
    balances_after: SettlementCtfBalances,
    flags: MockCtfFlags,
    submit_count: usize,
    redeemed: bool,
    receipt_by_tx: HashMap<String, SettlementCtfSubmittedRedeemReceipt>,
}

impl MockCtfState {
    fn yes_wins_resolved() -> Self {
        Self {
            payout: SettlementCtfPayoutVector {
                denominator: "1".to_owned(),
                yes: "1".to_owned(),
                no: "0".to_owned(),
            },
            balances: matched_yes_balances(),
            balances_after: zero_balances(),
            flags: MockCtfFlags::default(),
            submit_count: 0,
            redeemed: false,
            receipt_by_tx: HashMap::new(),
        }
    }
}

struct MockCtfClient {
    inner: Arc<Mutex<MockCtfState>>,
}

impl MockCtfClient {
    fn new(state: MockCtfState) -> Self {
        Self {
            inner: Arc::new(Mutex::new(state)),
        }
    }

    fn with_flags(self, flags: MockCtfFlags) -> Self {
        self.inner.lock().unwrap().flags = flags;
        self
    }

    fn submit_count(&self) -> usize {
        self.inner.lock().unwrap().submit_count
    }
}

struct MockRedeemTx {
    tx_hash: String,
    receipt: SettlementCtfRedeemReceipt,
}

#[async_trait]
impl SettlementRedeemTx for MockRedeemTx {
    fn tx_hash(&self) -> &str {
        &self.tx_hash
    }

    async fn wait(
        self: Box<Self>,
        _confirmations: u64,
    ) -> Result<SettlementCtfRedeemReceipt, RpcError> {
        Ok(self.receipt)
    }
}

#[async_trait]
impl SettlementCtfClient for MockCtfClient {
    async fn binary_payout_vector(
        &self,
        _market_id: &MarketId,
    ) -> Result<SettlementCtfPayoutVector, RpcError> {
        Ok(self.inner.lock().unwrap().payout.clone())
    }

    async fn binary_balances(
        &self,
        _funder_address: &str,
        _yes_token_id: &TokenId,
        _no_token_id: &TokenId,
    ) -> Result<SettlementCtfBalances, RpcError> {
        let state = self.inner.lock().unwrap();
        if state.redeemed {
            Ok(state.balances_after.clone())
        } else {
            Ok(state.balances.clone())
        }
    }

    async fn simulate_standard_binary_redeem(&self, _market_id: &MarketId) -> Result<(), RpcError> {
        if self.inner.lock().unwrap().flags.simulate_fail {
            return Err(RpcError::CallFailed {
                method: "redeemPositions.call".into(),
                reason: "simulated revert".to_owned(),
            });
        }
        Ok(())
    }

    async fn submit_standard_binary_redeem(
        &self,
        _market_id: &MarketId,
    ) -> Result<Box<dyn SettlementRedeemTx>, RpcError> {
        let mut state = self.inner.lock().unwrap();
        if state.flags.submit_fail {
            return Err(RpcError::CallFailed {
                method: "redeemPositions.send".into(),
                reason: "simulated submit failure".to_owned(),
            });
        }
        state.submit_count += 1;
        state.redeemed = true;
        drop(state);
        Ok(Box::new(MockRedeemTx {
            tx_hash: TX_HASH.to_owned(),
            receipt: SettlementCtfRedeemReceipt {
                tx_hash: TX_HASH.to_owned(),
                gas_used: 21_000,
                effective_gas_price_wei: 1_000_000_000,
            },
        }))
    }

    async fn submitted_redeem_receipt(
        &self,
        tx_hash: &str,
        _confirmations: u64,
    ) -> Result<SettlementCtfSubmittedRedeemReceipt, RpcError> {
        let mut state = self.inner.lock().unwrap();
        if let Some(status) = state.receipt_by_tx.get(tx_hash).cloned() {
            if matches!(status, SettlementCtfSubmittedRedeemReceipt::Confirmed(_)) {
                state.redeemed = true;
            }
            drop(state);
            return Ok(status);
        }
        state.redeemed = true;
        drop(state);
        Ok(SettlementCtfSubmittedRedeemReceipt::Confirmed(
            SettlementCtfRedeemReceipt {
                tx_hash: tx_hash.to_owned(),
                gas_used: 21_000,
                effective_gas_price_wei: 1_000_000_000,
            },
        ))
    }
}

fn matched_yes_balances() -> SettlementCtfBalances {
    SettlementCtfBalances {
        yes_raw: SHARES_RAW.to_owned(),
        no_raw: "0".to_owned(),
    }
}

fn zero_balances() -> SettlementCtfBalances {
    SettlementCtfBalances {
        yes_raw: "0".to_owned(),
        no_raw: "0".to_owned(),
    }
}

fn excess_chain_balances() -> SettlementCtfBalances {
    SettlementCtfBalances {
        yes_raw: "101000000".to_owned(),
        no_raw: "0".to_owned(),
    }
}

fn settlement_config() -> RuntimeConfig {
    let mut config = RuntimeConfig::default();
    config.execution.settlement_redeem.enabled = true;
    config.execution.settlement_redeem.batch_size = 8;
    config.execution.settlement_redeem.max_attempts = 5;
    config.execution.settlement_redeem.retry_backoff_secs = 300;
    config.execution.settlement_redeem.confirmation_blocks = 1;
    config
}

fn settlement_service(
    db: &sea_orm::DatabaseConnection,
    ctf: Arc<dyn SettlementCtfClient>,
    runtime_mode: QuantRuntimeMode,
    config: RuntimeConfig,
) -> SettlementRedeemService {
    let db = db.clone();
    SettlementRedeemService::new(SettlementRedeemServiceDeps {
        positions: Arc::new(PgPositionRepository::new(db.clone())) as Arc<dyn PositionRepository>,
        intents: Arc::new(PgOrderIntentRepository::new(db.clone()))
            as Arc<dyn OrderIntentRepository>,
        markets: Arc::new(PgMarketRepository::new(db.clone())) as Arc<dyn MarketRepository>,
        settlement_redeems: Arc::new(PgSettlementRedeemRepository::new(db))
            as Arc<dyn SettlementRedeemRepository>,
        ctf,
        runtime_mode: RuntimeModeHandle::new(runtime_mode),
        kill_switch: KillSwitchHandle::default(),
        config: Arc::new(RuntimeConfigStore::new(config)),
        funder_address: FUNDER.to_owned(),
        wallet_kind: ExecutionWalletKind::Eoa,
    })
}

async fn patch_exit_policy(
    db: &sea_orm::DatabaseConnection,
    intent_id: &OrderIntentId,
    settlement_mode: ExitSettlementMode,
    redeem_policy: RedeemPolicy,
) {
    let row = quant_order_intent::Entity::find_by_id(intent_id.clone())
        .one(db)
        .await
        .expect("load intent")
        .expect("intent row");
    let mut policy = row.exit_policy_json.clone();
    policy.settlement_mode = settlement_mode;
    policy.redeem_policy = redeem_policy;
    let mut active = row.into_active_model();
    active.exit_policy_json = ActiveValue::Set(policy);
    active.update(db).await.expect("patch exit policy");
}

async fn patch_position_state(
    db: &sea_orm::DatabaseConnection,
    intent_id: &OrderIntentId,
    state: PositionLedgerState,
) {
    let row = quant_position::Entity::find()
        .filter(quant_position::Column::OrderIntentId.eq(intent_id.clone()))
        .one(db)
        .await
        .expect("load position")
        .expect("position row");
    let mut active = row.into_active_model();
    active.state = ActiveValue::Set(state);
    active.update(db).await.expect("patch position state");
}

async fn align_market_tokens_with_position_lots(db: &sea_orm::DatabaseConnection, market_id: &str) {
    let row = market::Entity::find_by_id(MarketId::new(market_id))
        .one(db)
        .await
        .expect("load market")
        .expect("market row");
    let mut active = row.into_active_model();
    active.yes_token_id = ActiveValue::Set(TokenId::new(YES_TOKEN));
    active.no_token_id = ActiveValue::Set(TokenId::new(NO_TOKEN));
    active.update(db).await.expect("align market tokens");
}

async fn seed_hold_lot_on_fixture(
    db: &sea_orm::DatabaseConnection,
    ids: &ExecutionTxnIds,
    settlement_mode: ExitSettlementMode,
    redeem_policy: RedeemPolicy,
) -> OrderIntentId {
    let intent_id = seed_approved_intent(db, ids).await;
    patch_exit_policy(db, &intent_id, settlement_mode, redeem_policy).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    fill_entry_lot(&submission, ids, &intent_id).await;
    intent_id
}

async fn seed_auto_hold_lot(
    db: &sea_orm::DatabaseConnection,
    settlement_mode: ExitSettlementMode,
    redeem_policy: RedeemPolicy,
) -> (ExecutionTxnIds, OrderIntentId) {
    let ids = seed_report_fixture(db).await;
    align_market_tokens_with_position_lots(db, &ids.market).await;
    let intent_id = seed_hold_lot_on_fixture(db, &ids, settlement_mode, redeem_policy).await;
    (ids, intent_id)
}

fn balance_evidence(balances: &SettlementCtfBalances) -> SettlementBalanceEvidence {
    SettlementBalanceEvidence {
        yes: SettlementTokenBalance {
            token_id: YES_TOKEN.to_owned(),
            index_set: 1,
            raw_balance: balances.yes_raw.clone(),
            shares: raw_to_shares(&balances.yes_raw).to_string(),
        },
        no: SettlementTokenBalance {
            token_id: NO_TOKEN.to_owned(),
            index_set: 2,
            raw_balance: balances.no_raw.clone(),
            shares: raw_to_shares(&balances.no_raw).to_string(),
        },
    }
}

fn raw_to_shares(raw: &str) -> Decimal {
    Decimal::from_str(raw).expect("raw balance") / Decimal::from(COLLATERAL_SCALE)
}

async fn insert_submitted_redeem(
    db: &sea_orm::DatabaseConnection,
    market_id: &MarketId,
    tx_hash: &str,
) -> SettlementRedeemId {
    let redeem_id = SettlementRedeemId::from_v7();
    let balances = matched_yes_balances();
    let repo = PgSettlementRedeemRepository::new(db.clone());
    repo.upsert_pending(NewSettlementRedeem {
        settlement_redeem_id: redeem_id.clone(),
        market_id: market_id.clone(),
        funder_address: FUNDER.to_owned(),
        wallet_kind: ExecutionWalletKind::Eoa,
        state: SettlementRedeemState::Pending,
        tx_hash: None,
        index_sets_json: SettlementRedeemIndexSets {
            index_sets: vec![1, 2],
        },
        payout_vector_json: SettlementPayoutVector {
            denominator: "1".to_owned(),
            yes: "1".to_owned(),
            no: "0".to_owned(),
        },
        balance_before_json: balance_evidence(&balances),
        balance_after_json: None,
        payout_usd: quant_pivot_models::types::Usd::ZERO,
        gas_fee_pol: None,
        attempt_count: 0,
        next_attempt_at: None,
        last_error: None,
        submitted_at: None,
        confirmed_at: None,
        failed_at: None,
    })
    .await
    .expect("upsert pending redeem");
    repo.mark_submitted(&redeem_id, tx_hash.to_owned(), Utc::now())
        .await
        .expect("mark submitted");
    redeem_id
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn redeem_worker_scans_auto_hold_to_resolution_lots_only() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let (_ids, auto_intent) = seed_auto_hold_lot(
        &db,
        ExitSettlementMode::HoldToResolution,
        RedeemPolicy::Auto,
    )
    .await;

    let mut state = MockCtfState::yes_wins_resolved();
    state.balances_after = zero_balances();
    let ctf = Arc::new(MockCtfClient::new(state));
    let service = settlement_service(
        &db,
        ctf.clone(),
        QuantRuntimeMode::AutoExecution,
        settlement_config(),
    );

    let summary = service.run_pass(Utc::now()).await.expect("run pass");
    assert_eq!(summary.candidates, 1);
    assert_eq!(summary.confirmed, 1);
    assert_eq!(ctf.submit_count(), 1);

    let auto_position = PgPositionRepository::new(db.clone())
        .find_by_intent(&auto_intent)
        .await
        .expect("auto position")
        .expect("auto lot");
    assert_eq!(auto_position.state, PositionLedgerState::Closed);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn manual_hold_to_resolution_lot_is_not_auto_redeemed() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_auto_hold_lot(
        &db,
        ExitSettlementMode::HoldToResolution,
        RedeemPolicy::Manual,
    )
    .await;

    let ctf = Arc::new(MockCtfClient::new(MockCtfState::yes_wins_resolved()));
    let service = settlement_service(
        &db,
        ctf.clone(),
        QuantRuntimeMode::AutoExecution,
        settlement_config(),
    );

    let summary = service.run_pass(Utc::now()).await.expect("run pass");
    assert_eq!(summary.candidates, 0);
    assert_eq!(summary.confirmed, 0);
    assert_eq!(ctf.submit_count(), 0);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn successful_redeem_closes_lot_and_releases_capital() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let (_ids, intent_id) = seed_auto_hold_lot(
        &db,
        ExitSettlementMode::HoldToResolution,
        RedeemPolicy::Auto,
    )
    .await;

    let mut state = MockCtfState::yes_wins_resolved();
    state.balances_after = zero_balances();
    let service = settlement_service(
        &db,
        Arc::new(MockCtfClient::new(state)),
        QuantRuntimeMode::AutoExecution,
        settlement_config(),
    );
    let summary = service.run_pass(Utc::now()).await.expect("run pass");
    assert_eq!(summary.confirmed, 1);

    let position = PgPositionRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("position")
        .expect("lot");
    assert_eq!(position.state, PositionLedgerState::Closed);
    assert_eq!(
        position.realized_pnl_usd,
        quant_pivot_models::types::Usd::new(dec!(40))
    );

    let intent = PgOrderIntentRepository::new(db.clone())
        .find_by_id(&intent_id)
        .await
        .expect("intent")
        .expect("row");
    assert_eq!(intent.exit_state, ExitState::Exited);
    assert_eq!(intent.exit_reason, Some(ExitReason::ResolutionRedeem));

    let capital = PgCapitalAllocationRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("capital")
        .expect("row");
    assert_eq!(capital.state, CapitalAllocationState::Released);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn redeem_idempotent_after_success() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_auto_hold_lot(
        &db,
        ExitSettlementMode::HoldToResolution,
        RedeemPolicy::Auto,
    )
    .await;

    let mut state = MockCtfState::yes_wins_resolved();
    state.balances_after = zero_balances();
    let ctf = Arc::new(MockCtfClient::new(state));
    let service = settlement_service(
        &db,
        ctf.clone(),
        QuantRuntimeMode::AutoExecution,
        settlement_config(),
    );

    let first = service.run_pass(Utc::now()).await.expect("first pass");
    assert_eq!(first.confirmed, 1);
    assert_eq!(ctf.submit_count(), 1);

    let second = service.run_pass(Utc::now()).await.expect("second pass");
    assert_eq!(second.confirmed, 0);
    assert_eq!(second.candidates, 0);
    assert_eq!(ctf.submit_count(), 1);

    let redeem = quant_settlement_redeem::Entity::find()
        .filter(quant_settlement_redeem::Column::MarketId.eq(MarketId::new("0xmarket")))
        .one(&db)
        .await
        .expect("redeem row")
        .expect("row");
    assert_eq!(redeem.state, SettlementRedeemState::Confirmed);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn redeem_failure_preserves_position_and_retries() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let (_ids, intent_id) = seed_auto_hold_lot(
        &db,
        ExitSettlementMode::HoldToResolution,
        RedeemPolicy::Auto,
    )
    .await;

    let ctf = Arc::new(
        MockCtfClient::new(MockCtfState::yes_wins_resolved()).with_flags(MockCtfFlags {
            submit_fail: true,
            ..MockCtfFlags::default()
        }),
    );
    let service = settlement_service(
        &db,
        ctf.clone(),
        QuantRuntimeMode::AutoExecution,
        settlement_config(),
    );

    let failed = service.run_pass(Utc::now()).await.expect("failed pass");
    assert_eq!(failed.failed, 1);

    let position = PgPositionRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("position")
        .expect("lot");
    assert_eq!(position.state, PositionLedgerState::Open);

    let capital = PgCapitalAllocationRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("capital")
        .expect("row");
    assert_eq!(capital.state, CapitalAllocationState::Spent);

    let redeem = quant_settlement_redeem::Entity::find()
        .filter(quant_settlement_redeem::Column::MarketId.eq(MarketId::new("0xmarket")))
        .one(&db)
        .await
        .expect("redeem row")
        .expect("row");
    assert_eq!(redeem.state, SettlementRedeemState::Failed);
    assert!(redeem.next_attempt_at.is_some());

    let ctf = Arc::new(MockCtfClient::new(MockCtfState::yes_wins_resolved()));
    let service = settlement_service(
        &db,
        ctf.clone(),
        QuantRuntimeMode::AutoExecution,
        settlement_config(),
    );
    let retried = service
        .run_pass(Utc::now() + Duration::seconds(301))
        .await
        .expect("retry pass");
    assert_eq!(retried.confirmed, 1);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn manual_required_when_chain_balance_exceeds_strategy_lots() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let (_ids, intent_id) = seed_auto_hold_lot(
        &db,
        ExitSettlementMode::HoldToResolution,
        RedeemPolicy::Auto,
    )
    .await;

    let mut state = MockCtfState::yes_wins_resolved();
    state.balances = excess_chain_balances();
    let ctf = Arc::new(MockCtfClient::new(state));
    let service = settlement_service(
        &db,
        ctf.clone(),
        QuantRuntimeMode::AutoExecution,
        settlement_config(),
    );

    let summary = service.run_pass(Utc::now()).await.expect("run pass");
    assert_eq!(summary.manual_required, 1);
    assert_eq!(ctf.submit_count(), 0);

    let position = PgPositionRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("position")
        .expect("lot");
    assert_eq!(position.state, PositionLedgerState::Open);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn submitted_redeem_recovers_from_persisted_tx_hash() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let (ids, intent_id) = seed_auto_hold_lot(
        &db,
        ExitSettlementMode::HoldToResolution,
        RedeemPolicy::Auto,
    )
    .await;

    insert_submitted_redeem(&db, &MarketId::new(&ids.market), TX_HASH).await;

    let mut state = MockCtfState::yes_wins_resolved();
    state.balances_after = zero_balances();
    let ctf = Arc::new(MockCtfClient::new(state));
    let service = settlement_service(
        &db,
        ctf.clone(),
        QuantRuntimeMode::AutoExecution,
        settlement_config(),
    );

    let summary = service.run_pass(Utc::now()).await.expect("run pass");
    assert_eq!(summary.confirmed, 1);
    assert_eq!(ctf.submit_count(), 0);

    let intent = PgOrderIntentRepository::new(db.clone())
        .find_by_id(&intent_id)
        .await
        .expect("intent")
        .expect("row");
    assert_eq!(intent.exit_state, ExitState::Exited);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn settlement_redeem_runs_in_report_only_mode() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_auto_hold_lot(
        &db,
        ExitSettlementMode::HoldToResolution,
        RedeemPolicy::Auto,
    )
    .await;

    let mut state = MockCtfState::yes_wins_resolved();
    state.balances_after = zero_balances();
    let ctf = Arc::new(MockCtfClient::new(state));
    let service = settlement_service(
        &db,
        ctf.clone(),
        QuantRuntimeMode::ReportOnly,
        settlement_config(),
    );

    let summary = service.run_pass(Utc::now()).await.expect("run pass");
    assert_eq!(summary.confirmed, 1);
    assert_eq!(ctf.submit_count(), 1);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn closing_position_is_not_auto_redeemed() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let (_ids, closing_intent) = seed_auto_hold_lot(
        &db,
        ExitSettlementMode::HoldToResolution,
        RedeemPolicy::Auto,
    )
    .await;
    patch_position_state(&db, &closing_intent, PositionLedgerState::Closing).await;

    let mut state = MockCtfState::yes_wins_resolved();
    state.balances = matched_yes_balances();
    state.balances_after = zero_balances();
    let ctf = Arc::new(MockCtfClient::new(state));
    let service = settlement_service(
        &db,
        ctf,
        QuantRuntimeMode::AutoExecution,
        settlement_config(),
    );

    let summary = service.run_pass(Utc::now()).await.expect("run pass");
    assert_eq!(summary.candidates, 0);
    assert_eq!(summary.confirmed, 0);
}
