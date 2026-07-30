//! Canonical Phase 12.1 settlement persistence contracts.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};
use quant_pivot_api::settlement::confirmation::{
    SettlementConfirmationError, VerifiedSettlementConfirmation,
};
use quant_pivot_core::{
    execution::{
        settlement_confirmation::{
            build_settlement_confirmation, settlement_reconciliation_command,
        },
        settlement_discovery::SettlementDiscoveryService,
        settlement_governed_action_service::{
            SettlementGovernedActionExecutor, SettlementGovernedActionPassOutcome,
            SettlementGovernedActionService, SettlementGovernedActionServiceDeps,
            SettlementGovernedActionTrackingResult,
        },
        settlement_service::{
            SettlementDispatchResult, SettlementExecutorError, SettlementPassOutcome,
            SettlementService, SettlementServiceDeps, SettlementSubmissionExecutor,
            SettlementTrackingResult,
        },
    },
    governance::RuntimeControlsHandle,
    observability::metrics_hub::MetricsHub,
};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    config::SettlementDeployConfig,
    domain::{
        api::settlement_redeem::SettlementRedeemListQuery,
        quant::{
            ExecutionOutcomeReconciliationResult, NewExecutionAccount, PositionInfo,
            settlement::{
                ApproveSettlementAuthorization, BeginSettlementDispatch,
                NewSettlementChainSubmission, NewSettlementRedeem,
                PersistPreparedSettlementSubmission, PersistSettlementPreflight,
                RecordEoaSettlementBroadcast, SettlementChainSubmissionInfo, SettlementRedeemInfo,
                StageSettlementAuthorization,
            },
            settlement_governance::{
                AdvanceSettlementExternalCursor, NewSettlementExternalCursor,
                NewSettlementGovernedAction, PersistExternalSettlementScan,
                SettlementGovernedActionInfo,
            },
            settlement_inventory::NewSettlementInventoryLot,
        },
    },
    entities::{
        market::Entity as MarketEntity,
        quant_domain_event_outbox::Entity as QuantDomainEventOutboxEntity,
        quant_execution_order::{
            Column as QuantExecutionOrderColumn, Entity as QuantExecutionOrderEntity,
        },
        quant_order_intent::Entity as QuantOrderIntentEntity,
        quant_position::{Column as QuantPositionColumn, Entity as QuantPositionEntity},
        quant_settlement_authorization::{
            Column as QuantSettlementAuthorizationColumn,
            Entity as QuantSettlementAuthorizationEntity,
        },
        quant_settlement_chain_submission::Entity as QuantSettlementChainSubmissionEntity,
        quant_settlement_inventory_lot::Entity as QuantSettlementInventoryLotEntity,
        quant_settlement_redeem::Entity as QuantSettlementRedeemEntity,
    },
    enums::{
        common::MarketCategory,
        execution::{CapitalAllocationState, ExitState, KillSwitchState, PositionLedgerState},
        market::MarketStatus,
        quant::{
            ExecutionOrderState, ExecutionWalletKind, ExitSettlementMode, QuantRuntimeMode,
            RedeemPolicy,
        },
        settlement::{
            SettlementAuthorizationState, SettlementCaseState, SettlementEffectivePolicy,
            SettlementGovernedActionKind, SettlementGovernedActionState, SettlementReadinessStatus,
            SettlementReconciliationState, SettlementRoute, SettlementSubmissionKind,
            SettlementSubmissionPurpose, SettlementSubmissionState, SettlementWritePolicy,
        },
    },
    types::{
        ContentHash, EvmAddress, EvmBlockHash, EvmCalldataHash, EvmCodeHash, EvmTransactionHash,
        EvmUint256, ExecutionAccountId, MarketId, OrderIntentId, Price,
        SettlementActionIdempotencyKey, SettlementChainSubmissionId, SettlementEvidenceVersion,
        SettlementExternalCursorId, SettlementGovernedActionId, SettlementInventoryLotId,
        SettlementRedeemId, Shares, TokenId, Usd, UserId, WorkerId,
        settlement_payload::{
            SettlementBalanceEvidence, SettlementChainReceiptEvidence, SettlementFailureHistory,
            SettlementMinedCallEvidence, SettlementOperatorApprovalReceiptEvidence,
            SettlementPayoutVector, SettlementPusdMintEvidence, SettlementReadinessEvidence,
            SettlementReceiptEvidence, SettlementTokenBalance, SettlementWrappedPayoutEvidence,
        },
    },
};
use quant_pivot_repository::{
    postgres::{
        PgCapitalAllocationRepository, PgEventRepository, PgExecutionAccountRepository,
        PgExecutionSubmissionRepository, PgMarketRepository, PgOrderIntentRepository,
        PgPositionRepository, PgRecommendationExecutionOutcomeRepository, PgUserRepository,
        quant::{
            settlement_governance::PgSettlementGovernanceRepository,
            settlement_redeem::PgSettlementRedeemRepository,
        },
    },
    traits::{
        CapitalAllocationRepository, EventRepository, ExecutionAccountRepository, MarketRepository,
        OrderIntentRepository, PositionRepository, RecommendationExecutionOutcomeRepository,
        UserRepository,
        quant::{
            settlement_governance::{
                SettlementExternalCursorRepository, SettlementGovernanceRepository,
            },
            settlement_redeem::SettlementRedeemRepository,
        },
    },
};
use quant_pivot_system_tests::{
    postgres::{PostgresClock, setup_pg, with_postgres_suite},
    support::{
        catalog_fixtures::{make_event, make_market},
        execution_pg_seed::{
            ENTRY_FILLED_SHARES, enable_test_admission, fill_entry_lot, partial_exit_lot,
            seed_approved_intent, seed_settlement_report_fixture,
        },
    },
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel,
    QueryFilter,
};

const MARKET_ID: &str = "settlement-persistence-market";
const WINNING_TOKEN_RAW_BALANCE: &str = "40000000";
const MIXED_WINNING_TOKEN_RAW_BALANCE: &str = "30000000";
const SETTLEMENT_PAYOUT_USD: Decimal = ENTRY_FILLED_SHARES;
const MIXED_SETTLEMENT_PAYOUT_USD: Decimal = dec!(30);
const CURRENT_STANDARD: &str = "0xada100db00ca00073811820692005400218fce1f";
const CURRENT_STANDARD_CODE_HASH: &str =
    "0x93b965351d01c1a128821ac79fc98a18105daefb46bda0d1e5b52306d713aa4f";
const CURRENT_CTF: &str = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045";
const CURRENT_PUSD: &str = "0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb";
const CURRENT_USDCE: &str = "0x2791bca1f2de4661ed88a30c99a7a9449aa84174";

#[tokio::test]
async fn canonical_settlement_rejects_combinations() {
    Box::pin(with_postgres_suite(settlement_persistence_scenario()))
        .await
        .expect("start disposable PostgreSQL settlement suite");
}

#[tokio::test]
async fn settlement_orchestration_exclusive_first() {
    Box::pin(with_postgres_suite(settlement_orchestration_scenario()))
        .await
        .expect("start disposable PostgreSQL settlement orchestration suite");
}

#[tokio::test]
async fn settlement_confirmation_atomically_outbox() {
    Box::pin(with_postgres_suite(settlement_confirmation_scenario()))
        .await
        .expect("start disposable PostgreSQL settlement confirmation suite");
}

#[tokio::test]
async fn partial_exchange_exit_outcome() {
    Box::pin(with_postgres_suite(partial_exchange_resolution_scenario()))
        .await
        .expect("start mixed exchange/settlement outcome suite");
}

#[tokio::test]
async fn settlement_discovery_tracks_truth() {
    Box::pin(with_postgres_suite(settlement_discovery_scenario()))
        .await
        .expect("start disposable PostgreSQL settlement discovery suite");
}

#[tokio::test]
async fn governed_action_worker_first() {
    Box::pin(with_postgres_suite(governed_action_worker_scenario()))
        .await
        .expect("start disposable PostgreSQL governed-action worker suite");
}

#[tokio::test]
async fn governed_canary_consumed_submission() {
    Box::pin(with_postgres_suite(governed_canary_consumption_scenario()))
        .await
        .expect("start disposable PostgreSQL governed-canary consumption suite");
}

#[tokio::test]
async fn manual_only_cannot_recovers() {
    Box::pin(with_postgres_suite(manual_only_inventory_scenario()))
        .await
        .expect("start disposable PostgreSQL manual-only settlement suite");
}

async fn manual_only_inventory_scenario() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_market(&db).await;
    seed_execution_accounts(&db, &[0x91, 0x92]).await;
    let repository = PgSettlementRedeemRepository::new(db.clone());
    let now = Utc::now();

    let mut manual_case = ready_case(0x91);
    manual_case.effective_policy = SettlementEffectivePolicy::ManualOnly;
    let manual_case = insert_case_fixture(&db, &repository, manual_case)
        .await
        .expect("insert manual-only settlement case");
    assert!(
        repository
            .claim_next_new_submission(&WorkerId::from_v7(), now, now + TimeDelta::seconds(30),)
            .await
            .expect("query manual-only submission queue")
            .is_none(),
        "manual-only inventory must never enter the new-submission queue"
    );

    let owner = WorkerId::from_v7();
    let model = QuantSettlementRedeemEntity::find_by_id(manual_case.settlement_redeem_id)
        .one(&db)
        .await
        .expect("load manual-only persistence guard case")
        .expect("manual-only persistence guard case exists");
    let mut active = model.into_active_model();
    active.claim_owner = ActiveValue::Set(Some(owner));
    active.lease_expires_at = ActiveValue::Set(Some(now + TimeDelta::seconds(30)));
    active
        .update(&db)
        .await
        .expect("seed a corrupt direct repository claim");
    assert!(
        repository
            .stage_authorization(StageSettlementAuthorization {
                settlement_redeem_id: manual_case.settlement_redeem_id,
                owner,
                digest: ContentHash::from_bytes([0x93; 32]),
                expires_at: now + TimeDelta::minutes(5),
                expected_target_adapter: manual_case
                    .target_adapter
                    .clone()
                    .expect("manual-only ready target"),
                expected_deployment_digest: manual_case
                    .deployment_digest
                    .expect("manual-only ready deployment"),
                staged_at: now,
            })
            .await
            .is_err(),
        "manual-only inventory cannot stage a SemiAuto authorization"
    );
    assert!(
        repository
            .persist_prepared_submission(PersistPreparedSettlementSubmission {
                owner,
                expected_authorization_digest: None,
                expected_canary_action_id: None,
                submission: current_eoa_submission(manual_case.settlement_redeem_id, 0x91),
                persisted_at: now,
            })
            .await
            .is_err(),
        "manual-only inventory cannot bypass admission through repository persistence"
    );

    let mut recovery_case = ready_case(0x92);
    recovery_case.effective_policy = SettlementEffectivePolicy::ManualOnly;
    let recovery_case = insert_case_fixture(&db, &repository, recovery_case)
        .await
        .expect("insert manual-only recovery case");
    insert_submission_fixture(
        &db,
        current_eoa_submission(recovery_case.settlement_redeem_id, 0x92),
    )
    .await
    .expect("seed an already durable submission identity");
    let recovery = repository
        .claim_next_recovery(&WorkerId::from_v7(), now, now + TimeDelta::seconds(30))
        .await
        .expect("claim existing manual-only durable identity")
        .expect("existing manual-only durable identity remains recoverable");
    assert_eq!(
        recovery.redeem.settlement_redeem_id,
        recovery_case.settlement_redeem_id
    );
}

async fn settlement_discovery_scenario() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let PreparedDiscoveryScenario {
        repository,
        service,
        market_id,
        execution_account_id,
        intent_id,
        observed_at,
        discovered,
        original_digest,
    } = prepare_discovery_scenario(&db).await;
    assert_execution_scope_quiescence(
        &db,
        repository.as_ref(),
        &market_id,
        &execution_account_id,
        intent_id,
    )
    .await;

    let ready = persist_ready_preflight(
        repository.as_ref(),
        &discovered,
        0x91,
        observed_at + TimeDelta::seconds(2),
    )
    .await;
    set_intent_redeem_policy(
        &db,
        intent_id,
        RedeemPolicy::Manual,
        observed_at + TimeDelta::seconds(3),
    )
    .await;
    let position = QuantPositionEntity::find()
        .filter(QuantPositionColumn::OrderIntentId.eq(intent_id))
        .one(&db)
        .await
        .expect("load discovery position")
        .expect("discovery position exists");
    let mut changed = position.into_active_model();
    changed.shares = ActiveValue::Set(Shares::new(dec!(101)));
    changed.cost_usd = ActiveValue::Set(Usd::new(dec!(60.6)));
    changed.updated_at = ActiveValue::Set(observed_at + TimeDelta::seconds(3));
    changed
        .update(&db)
        .await
        .expect("simulate a late reconciled fill");
    let stale_owner = WorkerId::from_v7();
    let stale_claim = repository
        .claim_next_new_submission(
            &stale_owner,
            observed_at + TimeDelta::seconds(4),
            observed_at + TimeDelta::seconds(34),
        )
        .await
        .expect("claim stale automatic case")
        .expect("stale automatic case remains visible to the queue projection");
    assert_eq!(
        stale_claim.redeem.settlement_redeem_id,
        ready.settlement_redeem_id
    );
    assert!(
        repository
            .stage_authorization(StageSettlementAuthorization {
                settlement_redeem_id: ready.settlement_redeem_id,
                owner: stale_owner,
                digest: ContentHash::from_bytes([0x92; 32]),
                expires_at: observed_at + TimeDelta::minutes(5),
                expected_target_adapter: ready
                    .target_adapter
                    .clone()
                    .expect("ready late-fill target"),
                expected_deployment_digest: ready
                    .deployment_digest
                    .expect("ready late-fill deployment"),
                staged_at: observed_at + TimeDelta::seconds(4),
            })
            .await
            .is_err(),
        "a late Manual contributor must invalidate authorization before discovery catches up"
    );
    assert!(
        repository
            .release_claim(&ready.settlement_redeem_id, &stale_owner)
            .await
            .expect("release stale-inventory claim")
    );
    let refreshed = service
        .run_once(observed_at + TimeDelta::seconds(5), 32)
        .await
        .expect("refresh stale inventory digest");
    assert_eq!(refreshed.refreshed, 1);
    let refreshed_case = repository
        .find_by_id(&discovered.settlement_redeem_id)
        .await
        .expect("reload refreshed case")
        .expect("refreshed case exists");
    assert_ne!(refreshed_case.inventory_digest, original_digest);
    assert_eq!(
        refreshed_case.effective_policy,
        SettlementEffectivePolicy::ManualOnly
    );
    let refreshed_page = repository
        .page(SettlementRedeemListQuery::default())
        .await
        .expect("list refreshed cases");
    assert_eq!(
        refreshed_page.items[0].inventory_lot_count, 1,
        "list count must use the current inventory digest, not append-only history"
    );

    let position = QuantPositionEntity::find()
        .filter(QuantPositionColumn::OrderIntentId.eq(intent_id))
        .one(&db)
        .await
        .expect("reload discovery position")
        .expect("discovery position still exists");
    let mut closed = position.into_active_model();
    closed.state = ActiveValue::Set(PositionLedgerState::Closed);
    closed.shares = ActiveValue::Set(Shares::ZERO);
    closed.cost_usd = ActiveValue::Set(Usd::ZERO);
    closed.closed_at = ActiveValue::Set(Some(observed_at + TimeDelta::seconds(6)));
    closed.updated_at = ActiveValue::Set(observed_at + TimeDelta::seconds(6));
    closed.update(&db).await.expect("close durable position");
    let absent = service
        .run_once(observed_at + TimeDelta::seconds(7), 32)
        .await
        .expect("mark zero inventory not required");
    assert_eq!(absent.marked_not_required, 1);
    assert_eq!(
        repository
            .find_by_id(&discovered.settlement_redeem_id)
            .await
            .expect("reload not-required case")
            .expect("not-required case exists")
            .state,
        SettlementCaseState::NotRequired
    );
}

async fn assert_execution_scope_quiescence(
    db: &DatabaseConnection,
    repository: &PgSettlementRedeemRepository,
    market_id: &MarketId,
    execution_account_id: &ExecutionAccountId,
    intent_id: OrderIntentId,
) {
    let execution_order = QuantExecutionOrderEntity::find()
        .filter(QuantExecutionOrderColumn::OrderIntentId.eq(intent_id))
        .one(db)
        .await
        .expect("load contributor execution order")
        .expect("contributor execution order exists");
    let mut partial_order = execution_order.into_active_model();
    partial_order.state = ActiveValue::Set(ExecutionOrderState::PartiallyFilled);
    let partial_order = partial_order
        .update(db)
        .await
        .expect("simulate an order that can still receive a late fill");
    assert_eq!(
        repository
            .count_unsettled_execution_orders(market_id, execution_account_id)
            .await
            .expect("count unsettled market/account execution"),
        1
    );
    let mut ambiguous_order = partial_order.into_active_model();
    ambiguous_order.state = ActiveValue::Set(ExecutionOrderState::Ambiguous);
    let ambiguous_order = ambiguous_order
        .update(db)
        .await
        .expect("simulate an unsettled late-fill identity");
    assert_eq!(
        repository
            .count_unsettled_execution_orders(market_id, execution_account_id)
            .await
            .expect("count ambiguous market/account execution"),
        1
    );
    let mut settled_order = ambiguous_order.into_active_model();
    settled_order.state = ActiveValue::Set(ExecutionOrderState::Filled);
    settled_order
        .update(db)
        .await
        .expect("restore reconciled execution truth");
    assert_eq!(
        repository
            .count_unsettled_execution_orders(market_id, execution_account_id)
            .await
            .expect("recheck quiescent market/account execution"),
        0
    );
}

struct PreparedDiscoveryScenario {
    repository: Arc<PgSettlementRedeemRepository>,
    service: SettlementDiscoveryService,
    market_id: MarketId,
    execution_account_id: ExecutionAccountId,
    intent_id: OrderIntentId,
    observed_at: DateTime<Utc>,
    discovered: SettlementRedeemInfo,
    original_digest: ContentHash,
}

async fn prepare_discovery_scenario(db: &DatabaseConnection) -> PreparedDiscoveryScenario {
    let ids = seed_settlement_report_fixture(db).await;
    enable_test_admission(db, "settlement-discovery-test").await;
    let intent_id = seed_approved_intent(db, &ids).await;
    let execution = PgExecutionSubmissionRepository::new(db.clone());
    fill_entry_lot(db, &execution, &ids, &intent_id).await;
    let observed_at = Utc::now();
    set_intent_redeem_policy(db, intent_id, RedeemPolicy::Auto, observed_at).await;
    let market = MarketEntity::find_by_id(MarketId::new(&ids.market))
        .one(db)
        .await
        .expect("load discovery market")
        .expect("discovery market exists");
    let mut active_market = market.into_active_model();
    active_market.status = ActiveValue::Set(MarketStatus::Settled);
    active_market.outcome = ActiveValue::Set(Some("Yes".to_owned()));
    active_market.yes_token_id = ActiveValue::Set(TokenId::new(&ids.token));
    active_market.resolved_at = ActiveValue::Set(Some(observed_at));
    active_market.content_hash = ActiveValue::Set(ContentHash::from_bytes([0x91; 32]));
    active_market
        .update(db)
        .await
        .expect("persist durable market resolution");

    let repository = Arc::new(PgSettlementRedeemRepository::new(db.clone()));
    let service = SettlementDiscoveryService::new(
        Arc::clone(&repository) as Arc<dyn SettlementRedeemRepository>
    );
    let first = service
        .run_once(observed_at + TimeDelta::seconds(1), 32)
        .await
        .expect("discover exact resolved inventory");
    assert_eq!(first.discovered, 1);
    let discovered = repository
        .find_by_market_account(&MarketId::new(&ids.market), &ids.execution_account)
        .await
        .expect("load discovered case")
        .expect("account-scoped settlement case exists");
    let original_digest = discovered.inventory_digest;
    assert_eq!(
        repository
            .list_current_inventory(&discovered.settlement_redeem_id)
            .await
            .expect("load first inventory")
            .len(),
        1
    );
    let first_page = repository
        .page(SettlementRedeemListQuery::default())
        .await
        .expect("list discovered cases");
    assert_eq!(first_page.items[0].inventory_lot_count, 1);
    PreparedDiscoveryScenario {
        repository,
        service,
        market_id: MarketId::new(&ids.market),
        execution_account_id: ids.execution_account,
        intent_id,
        observed_at,
        discovered,
        original_digest,
    }
}

async fn settlement_orchestration_scenario() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let actor = PgUserRepository::new(db.clone())
        .find_by_username("admin")
        .await
        .expect("load seeded admin")
        .expect("seeded admin exists")
        .id;
    let repository = Arc::new(PgSettlementRedeemRepository::new(db.clone()));
    let governance = Arc::new(PgSettlementGovernanceRepository::new(db.clone()));
    let redeem = seed_ready_automatic_case(&db, repository.as_ref(), 0x71).await;
    let runtime_controls = RuntimeControlsHandle::default();
    let executor = Arc::new(ProbeSettlementExecutor::default());
    let service_worker = WorkerId::from_v7();
    let service = SettlementService::new(SettlementServiceDeps {
        repository: Arc::clone(&repository) as Arc<dyn SettlementRedeemRepository>,
        governance: governance as Arc<dyn SettlementGovernanceRepository>,
        positions: Arc::new(PgPositionRepository::new(db)),
        executor: Arc::clone(&executor) as Arc<dyn SettlementSubmissionExecutor>,
        runtime_controls: runtime_controls.clone(),
        config: SettlementDeployConfig::default(),
        worker_id: service_worker,
        metrics: Arc::new(MetricsHub::new()),
    });
    let now = Utc::now();

    let report_only = Box::pin(service.run_once(now))
        .await
        .expect("report-only pass is read-only");
    assert!(matches!(
        report_only,
        SettlementPassOutcome::NewSubmissionBlocked { .. }
    ));
    assert_eq!(executor.preparations.load(Ordering::SeqCst), 0);
    assert_eq!(executor.dispatches.load(Ordering::SeqCst), 0);

    let mut controls = runtime_controls.snapshot();
    controls.revision += 1;
    controls.quant_runtime_mode = QuantRuntimeMode::SemiAuto;
    controls.settlement_write_policy = SettlementWritePolicy::SemiAuto;
    runtime_controls.publish_local(controls);
    let authorization_started_at = now + TimeDelta::seconds(6);
    let pending = Box::pin(service.run_once(authorization_started_at))
        .await
        .expect("semi-auto stages exact authorization without signing");
    let SettlementPassOutcome::AuthorizationPending { digest, .. } = pending else {
        panic!("expected authorization challenge");
    };
    assert_eq!(executor.preparations.load(Ordering::SeqCst), 0);
    let durable = authorize_prepare_and_dispatch(
        repository.as_ref(),
        redeem.settlement_redeem_id,
        digest,
        actor,
        authorization_started_at,
    )
    .await;

    let mut controls = runtime_controls.snapshot();
    controls.revision += 1;
    controls.quant_runtime_mode = QuantRuntimeMode::ReportOnly;
    controls.kill_switch_state = KillSwitchState::EmergencyHalted;
    controls.kill_switch_requires_ack = true;
    runtime_controls.publish_local(controls);
    let recovered = Box::pin(service.run_once(authorization_started_at + TimeDelta::seconds(7)))
        .await
        .expect("existing submission recovery ignores new-submission gates");
    assert_eq!(
        recovered,
        SettlementPassOutcome::ExistingSubmissionTracked {
            settlement_chain_submission_id: durable.settlement_chain_submission_id,
        }
    );
    assert_eq!(executor.preparations.load(Ordering::SeqCst), 0);
    assert_eq!(executor.tracked.load(Ordering::SeqCst), 1);
}

async fn governed_action_worker_scenario() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_execution_accounts(&db, &[0x81]).await;
    let operator = PgUserRepository::new(db.clone())
        .find_by_username("admin")
        .await
        .expect("load governed-action operator")
        .expect("seeded operator exists");
    let execution_account_id = execution_account(0x81).execution_account_id;
    let now = Utc::now();
    let governance = Arc::new(PgSettlementGovernanceRepository::new(db));
    let action = governance
        .create_action(governed_approval_action(
            execution_account_id,
            operator.id,
            now,
            0xa1,
        ))
        .await
        .expect("create exact operator-approval action");

    let first_owner = WorkerId::from_v7();
    let claimed = governance
        .claim_next_action(
            &execution_account_id,
            &first_owner,
            now,
            now + TimeDelta::seconds(30),
        )
        .await
        .expect("claim governed action")
        .expect("authorized action is claimable");
    assert_eq!(
        claimed.action.settlement_governed_action_id,
        action.settlement_governed_action_id
    );
    assert!(
        governance
            .claim_next_action(
                &execution_account_id,
                &WorkerId::from_v7(),
                now,
                now + TimeDelta::seconds(30),
            )
            .await
            .expect("competing governed-action claim")
            .is_none(),
        "governed action has one lease owner"
    );
    assert!(
        governance
            .release_action_claim(&action.settlement_governed_action_id, &first_owner)
            .await
            .expect("release probe claim")
    );

    let controls = RuntimeControlsHandle::default();
    let mut enabled = controls.snapshot();
    enabled.revision += 1;
    enabled.quant_runtime_mode = QuantRuntimeMode::SemiAuto;
    enabled.settlement_write_policy = SettlementWritePolicy::GovernedCanary;
    controls.publish_local(enabled);
    let executor = Arc::new(ProbeGovernedActionExecutor::default());
    let service = SettlementGovernedActionService::new(SettlementGovernedActionServiceDeps {
        repository: Arc::clone(&governance) as Arc<dyn SettlementGovernanceRepository>,
        executor: Arc::clone(&executor) as Arc<dyn SettlementGovernedActionExecutor>,
        runtime_controls: controls.clone(),
        config: SettlementDeployConfig::default(),
        execution_account_id,
        worker_id: WorkerId::from_v7(),
        metrics: Arc::new(MetricsHub::new()),
    });

    let dispatched = service
        .run_once(Utc::now())
        .await
        .expect("prepare, journal, and dispatch governed action");
    assert!(matches!(
        dispatched,
        SettlementGovernedActionPassOutcome::DispatchAccepted { .. }
    ));
    assert_eq!(executor.preparations.load(Ordering::SeqCst), 1);
    assert_eq!(executor.dispatches.load(Ordering::SeqCst), 1);
    let durable = governance
        .find_submission_by_action(&action.settlement_governed_action_id)
        .await
        .expect("load governed durable submission")
        .expect("governed action has a durable submission");
    assert_eq!(durable.state, SettlementSubmissionState::AwaitingFinality);

    let mut halted = controls.snapshot();
    halted.revision += 1;
    halted.quant_runtime_mode = QuantRuntimeMode::ReportOnly;
    halted.settlement_write_policy = SettlementWritePolicy::Disabled;
    halted.kill_switch_state = KillSwitchState::EmergencyHalted;
    controls.publish_local(halted);
    let confirmed = service
        .run_once(Utc::now() + TimeDelta::seconds(10))
        .await
        .expect("existing governed identity recovers while writes are halted");
    assert!(matches!(
        confirmed,
        SettlementGovernedActionPassOutcome::Confirmed { .. }
    ));
    assert_eq!(executor.tracked.load(Ordering::SeqCst), 1);
    let completed = governance
        .find_action(&action.settlement_governed_action_id)
        .await
        .expect("reload governed action")
        .expect("governed action exists");
    assert_eq!(completed.state, SettlementGovernedActionState::Consumed);
    let completed_submission = governance
        .find_submission_by_action(&action.settlement_governed_action_id)
        .await
        .expect("reload confirmed governed submission")
        .expect("confirmed governed submission exists");
    assert_eq!(
        completed_submission.state,
        SettlementSubmissionState::Confirmed
    );
    assert!(matches!(
        completed_submission.receipt_evidence_json,
        Some(SettlementChainReceiptEvidence::OperatorApproval(_))
    ));
}

async fn governed_canary_consumption_scenario() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let actor = PgUserRepository::new(db.clone())
        .find_by_username("admin")
        .await
        .expect("load seeded canary operator")
        .expect("seeded canary operator exists")
        .id;
    let repository = Arc::new(PgSettlementRedeemRepository::new(db.clone()));
    let governance = Arc::new(PgSettlementGovernanceRepository::new(db.clone()));
    let redeem = seed_ready_automatic_case(&db, repository.as_ref(), 0x73).await;
    let now = Utc::now();
    let owner = WorkerId::from_v7();
    let claim = repository
        .claim_next_new_submission(&owner, now, now + TimeDelta::seconds(60))
        .await
        .expect("claim governed-canary case")
        .expect("governed-canary case is claimable");
    let authorization_digest = ContentHash::from_bytes([0x74; 32]);
    repository
        .stage_authorization(StageSettlementAuthorization {
            settlement_redeem_id: redeem.settlement_redeem_id,
            owner,
            digest: authorization_digest,
            expires_at: now + TimeDelta::minutes(5),
            expected_target_adapter: redeem
                .target_adapter
                .clone()
                .expect("governed-canary target"),
            expected_deployment_digest: redeem
                .deployment_digest
                .expect("governed-canary deployment digest"),
            staged_at: now,
        })
        .await
        .expect("stage governed-canary authorization");
    repository
        .approve_authorization(ApproveSettlementAuthorization {
            settlement_redeem_id: redeem.settlement_redeem_id,
            digest: authorization_digest,
            actor,
            approved_at: now + TimeDelta::seconds(1),
        })
        .await
        .expect("approve governed-canary authorization");
    let canary = governance
        .create_action(governed_canary_action(
            &redeem,
            authorization_digest,
            actor,
            now + TimeDelta::seconds(1),
        ))
        .await
        .expect("create exact governed-canary grant");

    let mut first_submission = current_eoa_submission(claim.redeem.settlement_redeem_id, 0x73);
    first_submission.canary_action_id = Some(canary.settlement_governed_action_id);
    let mut second_submission = first_submission.clone();
    second_submission.settlement_chain_submission_id = SettlementChainSubmissionId::from_v7();
    second_submission.transaction_hash = Some(transaction_hash(0x74));
    second_submission.calldata = vec![0xde, 0xad, 0xbe, 0x74];
    second_submission.calldata_hash = calldata_hash(0x74);
    let second_envelope = vec![0x02, 0x74];
    second_submission.signed_envelope_hash = Some(ContentHash::from_bytes(
        *blake3::hash(&second_envelope).as_bytes(),
    ));
    second_submission.signed_envelope = Some(second_envelope);
    second_submission.prepared_nonce = Some(EvmUint256::parse("18").expect("second nonce"));

    let first = repository.persist_prepared_submission(PersistPreparedSettlementSubmission {
        owner,
        expected_authorization_digest: Some(authorization_digest),
        expected_canary_action_id: Some(canary.settlement_governed_action_id),
        submission: first_submission,
        persisted_at: now + TimeDelta::seconds(2),
    });
    let second = repository.persist_prepared_submission(PersistPreparedSettlementSubmission {
        owner,
        expected_authorization_digest: Some(authorization_digest),
        expected_canary_action_id: Some(canary.settlement_governed_action_id),
        submission: second_submission,
        persisted_at: now + TimeDelta::seconds(2),
    });
    let (first_result, second_result) = tokio::join!(first, second);
    assert_eq!(
        usize::from(first_result.is_ok()) + usize::from(second_result.is_ok()),
        1,
        "one-shot canary and authorization must create exactly one durable envelope"
    );

    let consumed_redeem = repository
        .find_by_id(&redeem.settlement_redeem_id)
        .await
        .expect("reload governed-canary case")
        .expect("governed-canary case exists");
    assert_eq!(
        consumed_redeem.authorization_state,
        SettlementAuthorizationState::Consumed
    );
    let consumed_canary = governance
        .find_action(&canary.settlement_governed_action_id)
        .await
        .expect("reload governed-canary action")
        .expect("governed-canary action exists");
    assert_eq!(
        consumed_canary.state,
        SettlementGovernedActionState::Consumed
    );
    assert_eq!(
        repository
            .list_submissions_by_redeem(&redeem.settlement_redeem_id)
            .await
            .expect("load governed-canary submissions")
            .len(),
        1
    );
}

async fn settlement_confirmation_scenario() {
    let (pool, mismatch_database) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = prepare_confirmation_fixture(&db).await;
    let confirmed_at = db.statement_time().await;
    let settlement = PgSettlementRedeemRepository::new(db.clone());
    let confirmation_worker = WorkerId::from_v7();
    settlement
        .claim_next_recovery(
            &confirmation_worker,
            confirmed_at,
            confirmed_at + TimeDelta::seconds(30),
        )
        .await
        .expect("claim confirmation reconciliation")
        .expect("active submission is claimable");
    assert_mismatch_holds_accounting(
        &db,
        &settlement,
        &fixture.redeem,
        &fixture.submission,
        &fixture.intent_id,
        confirmation_worker,
        confirmed_at,
    )
    .await;
    let retry_at = confirmed_at + TimeDelta::seconds(1);
    assert!(
        settlement
            .claim_next_recovery(
                &confirmation_worker,
                retry_at,
                retry_at + TimeDelta::seconds(30),
            )
            .await
            .expect("query terminal reconciliation queue")
            .is_none(),
        "business-evidence mismatch must not auto-retry the failed submission"
    );
    assert_eq!(
        settlement
            .find_submission_by_id(&fixture.submission.settlement_chain_submission_id)
            .await
            .expect("reload reconciled submission")
            .expect("reconciled submission exists")
            .state,
        SettlementSubmissionState::Failed
    );
    drop(settlement);
    drop(db);
    drop(pool);
    drop(mismatch_database);

    let (pool, _confirmation_container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = prepare_confirmation_fixture(&db).await;
    let confirmed_at = db.statement_time().await;
    let position_repository = PgPositionRepository::new(db.clone());
    let settlement = PgSettlementRedeemRepository::new(db.clone());
    let confirmation_worker = WorkerId::from_v7();
    settlement
        .claim_next_recovery(
            &confirmation_worker,
            confirmed_at,
            confirmed_at + TimeDelta::seconds(30),
        )
        .await
        .expect("claim positive confirmation")
        .expect("positive confirmation submission is active");
    let write = build_settlement_confirmation(
        &fixture.redeem,
        &fixture.submission,
        fixture.positions,
        settlement
            .list_current_inventory(&fixture.redeem.settlement_redeem_id)
            .await
            .expect("load frozen settlement inventory"),
        fixture.confirmation,
        confirmed_at,
        confirmation_worker,
    )
    .expect("build exact settlement accounting command");
    let mut corrupt_write = write.clone();
    corrupt_write.lots[0].lot.order_intent_id = OrderIntentId::from_v7();
    assert!(
        settlement.confirm(corrupt_write).await.is_err(),
        "a downstream accounting failure rolls back case, submission and outbox"
    );
    assert_eq!(
        settlement
            .find_submission_by_id(&fixture.submission.settlement_chain_submission_id)
            .await
            .expect("reload rolled-back submission")
            .expect("submission still exists")
            .state,
        SettlementSubmissionState::AwaitingFinality
    );
    assert!(
        QuantDomainEventOutboxEntity::find()
            .all(&db)
            .await
            .expect("reload rolled-back outbox")
            .is_empty()
    );
    let confirmed = settlement
        .confirm(write)
        .await
        .expect("atomically confirm settlement accounting");
    assert_confirmed_accounting(
        &db,
        &settlement,
        &position_repository,
        &fixture.redeem,
        &fixture.submission,
        &fixture.intent_id,
        &confirmed,
    )
    .await;
}

async fn partial_exchange_resolution_scenario() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = prepare_partial_exchange_fixture(&db).await;
    let confirmed_at = db.statement_time().await;
    let settlement = PgSettlementRedeemRepository::new(db.clone());
    let confirmation_worker = WorkerId::from_v7();
    settlement
        .claim_next_recovery(
            &confirmation_worker,
            confirmed_at,
            confirmed_at + TimeDelta::seconds(30),
        )
        .await
        .expect("claim mixed confirmation")
        .expect("mixed confirmation submission is active");
    let write = build_settlement_confirmation(
        &fixture.redeem,
        &fixture.submission,
        fixture.positions,
        settlement
            .list_current_inventory(&fixture.redeem.settlement_redeem_id)
            .await
            .expect("load mixed frozen settlement inventory"),
        fixture.confirmation,
        confirmed_at,
        confirmation_worker,
    )
    .expect("build mixed settlement accounting command");
    settlement
        .confirm(write)
        .await
        .expect("confirm mixed exchange/settlement accounting");

    let outcome = PgRecommendationExecutionOutcomeRepository::new(db.clone())
        .reconcile_intent(&fixture.intent_id, db.statement_time().await)
        .await
        .expect("seal mixed execution outcome");
    let outcome = match outcome {
        ExecutionOutcomeReconciliationResult::Inserted(outcome) => outcome,
        other => panic!("expected inserted mixed execution outcome, got {other:?}"),
    };
    assert_eq!(outcome.exit_filled_shares, Some(Shares::new(dec!(10))));
    assert_eq!(outcome.exit_avg_price, Some(Price::new(dec!(0.5))));
    assert_eq!(outcome.settlement_payout_usd, Some(Usd::new(dec!(30))));
    assert_eq!(outcome.realized_pnl_usd, Some(Usd::new(dec!(10))));
}

struct SettlementConfirmationFixture {
    intent_id: OrderIntentId,
    positions: Vec<PositionInfo>,
    redeem: SettlementRedeemInfo,
    submission: SettlementChainSubmissionInfo,
    confirmation: VerifiedSettlementConfirmation,
}

async fn prepare_confirmation_fixture(db: &DatabaseConnection) -> SettlementConfirmationFixture {
    prepare_confirmation_fixture_shape(db, ConfirmationFixtureShape::ResolutionOnly).await
}

async fn prepare_partial_exchange_fixture(
    db: &DatabaseConnection,
) -> SettlementConfirmationFixture {
    prepare_confirmation_fixture_shape(db, ConfirmationFixtureShape::PartialExchangeThenResolution)
        .await
}

#[derive(Clone, Copy)]
enum ConfirmationFixtureShape {
    ResolutionOnly,
    PartialExchangeThenResolution,
}

async fn prepare_confirmation_fixture_shape(
    db: &DatabaseConnection,
    shape: ConfirmationFixtureShape,
) -> SettlementConfirmationFixture {
    seed_execution_accounts(db, &[0x75]).await;
    let ids = seed_settlement_report_fixture(db).await;
    enable_test_admission(db, "settlement-confirmation-test").await;
    let intent_id = seed_approved_intent(db, &ids).await;
    let execution = PgExecutionSubmissionRepository::new(db.clone());
    fill_entry_lot(db, &execution, &ids, &intent_id).await;
    if matches!(
        shape,
        ConfirmationFixtureShape::PartialExchangeThenResolution
    ) {
        partial_exit_lot(
            db,
            &execution,
            &ids,
            &intent_id,
            Shares::new(dec!(10)),
            Price::new(dec!(0.5)),
        )
        .await;
    }
    let observed_at = db.statement_time().await;
    let (raw_balance, redeem_shares, payout_usd) = match shape {
        ConfirmationFixtureShape::ResolutionOnly => (
            WINNING_TOKEN_RAW_BALANCE,
            Shares::new(ENTRY_FILLED_SHARES),
            Usd::new(SETTLEMENT_PAYOUT_USD),
        ),
        ConfirmationFixtureShape::PartialExchangeThenResolution => (
            MIXED_WINNING_TOKEN_RAW_BALANCE,
            Shares::new(MIXED_SETTLEMENT_PAYOUT_USD),
            Usd::new(MIXED_SETTLEMENT_PAYOUT_USD),
        ),
    };
    let position_repository = PgPositionRepository::new(db.clone());
    let market = MarketEntity::find_by_id(MarketId::new(&ids.market))
        .one(db)
        .await
        .expect("load settlement market")
        .expect("settlement market exists");
    let positions = position_repository
        .find_open_position(&MarketId::new(&ids.market), &ids.execution_account)
        .await
        .expect("load open settlement lots");
    assert_eq!(positions.len(), 1);
    let settlement = PgSettlementRedeemRepository::new(db.clone());
    let mut new_case = ready_case(0x75);
    new_case.market_id = MarketId::new(&ids.market);
    new_case.yes_token_id = TokenId::new(&ids.token);
    new_case.no_token_id = market.no_token_id.clone();
    new_case.execution_account_id = ids.execution_account;
    new_case.state = SettlementCaseState::Submitted;
    new_case.balance_before_json = Some(SettlementBalanceEvidence {
        yes: SettlementTokenBalance {
            token_id: TokenId::new(&ids.token),
            raw_balance: EvmUint256::parse(raw_balance).expect("YES raw balance"),
            shares: redeem_shares,
        },
        no: SettlementTokenBalance {
            token_id: market.no_token_id.clone(),
            raw_balance: EvmUint256::parse("0").expect("NO raw balance"),
            shares: Shares::ZERO,
        },
    });
    new_case.expected_payout_usd = Some(payout_usd);
    new_case.attempt_count = 1;
    new_case.submitted_at = Some(observed_at);
    let redeem = insert_case_fixture(db, &settlement, new_case)
        .await
        .expect("insert submitted settlement case");
    insert_confirmation_inventory(db, &redeem, &positions).await;
    let mut new_submission = current_eoa_submission(redeem.settlement_redeem_id, 0x75);
    new_submission.state = SettlementSubmissionState::AwaitingFinality;
    new_submission.dispatched_at = Some(observed_at);
    new_submission.chain_hash_observed_at = Some(observed_at);
    let submission = insert_submission_fixture(db, new_submission)
        .await
        .expect("insert finality-tracked submission");
    let confirmation = VerifiedSettlementConfirmation {
        receipt: receipt_evidence(&submission, observed_at, raw_balance, payout_usd),
        balances_after: SettlementBalanceEvidence {
            yes: SettlementTokenBalance {
                token_id: TokenId::new(&ids.token),
                raw_balance: EvmUint256::parse("0").expect("zero YES balance"),
                shares: Shares::ZERO,
            },
            no: SettlementTokenBalance {
                token_id: market.no_token_id,
                raw_balance: EvmUint256::parse("0").expect("zero NO balance"),
                shares: Shares::ZERO,
            },
        },
        actual_payout_usd: payout_usd,
        gas_fee_pol: dec!(0.003),
    };
    SettlementConfirmationFixture {
        intent_id,
        positions,
        redeem,
        submission,
        confirmation,
    }
}

async fn insert_confirmation_inventory(
    db: &DatabaseConnection,
    redeem: &SettlementRedeemInfo,
    positions: &[PositionInfo],
) {
    let position = positions.first().expect("one frozen settlement position");
    QuantSettlementInventoryLotEntity::insert(
        NewSettlementInventoryLot {
            settlement_inventory_lot_id: SettlementInventoryLotId::from_v7(),
            settlement_redeem_id: redeem.settlement_redeem_id,
            inventory_digest: redeem.inventory_digest,
            contributor_lots_digest: redeem.contributor_lots_digest,
            execution_account_id: redeem.execution_account_id,
            position_id: position.position_id,
            order_intent_id: position.order_intent_id,
            token_id: position.token_id.clone(),
            side: position.side,
            shares: position.shares,
            cost_basis_usd: position.cost_usd,
            settlement_mode: ExitSettlementMode::HoldToResolution,
            redeem_policy: RedeemPolicy::Auto,
            position_version_at: position.updated_at,
            intent_version_at: position.updated_at,
        }
        .into_active_model(),
    )
    .exec(db)
    .await
    .expect("insert frozen settlement inventory");
}

async fn assert_mismatch_holds_accounting(
    db: &DatabaseConnection,
    settlement: &PgSettlementRedeemRepository,
    redeem: &SettlementRedeemInfo,
    submission: &SettlementChainSubmissionInfo,
    intent_id: &OrderIntentId,
    worker: WorkerId,
    observed_at: DateTime<Utc>,
) {
    let command = settlement_reconciliation_command(
        redeem,
        submission,
        &SettlementConfirmationError::PayoutMismatch,
        observed_at,
        worker,
    );
    let mismatched = settlement
        .require_reconciliation(command)
        .await
        .expect("persist typed evidence mismatch without accounting release");
    assert_eq!(
        mismatched.state,
        SettlementCaseState::ReconciliationRequired
    );
    assert_eq!(
        PgPositionRepository::new(db.clone())
            .find_by_intent(intent_id)
            .await
            .expect("reload held position")
            .expect("held position exists")
            .state,
        PositionLedgerState::Open
    );
    assert_eq!(
        PgCapitalAllocationRepository::new(db.clone())
            .find_by_intent(intent_id)
            .await
            .expect("reload held capital")
            .expect("held capital exists")
            .state,
        CapitalAllocationState::Spent
    );
    assert!(
        QuantDomainEventOutboxEntity::find()
            .all(db)
            .await
            .expect("load empty settlement outbox")
            .is_empty()
    );
}

async fn assert_confirmed_accounting(
    db: &DatabaseConnection,
    settlement: &PgSettlementRedeemRepository,
    positions: &PgPositionRepository,
    redeem: &SettlementRedeemInfo,
    submission: &SettlementChainSubmissionInfo,
    intent_id: &OrderIntentId,
    confirmed: &SettlementRedeemInfo,
) {
    assert_eq!(confirmed.state, SettlementCaseState::Confirmed);
    assert_eq!(
        confirmed.actual_payout_usd,
        Some(Usd::new(SETTLEMENT_PAYOUT_USD))
    );
    let durable_submission = settlement
        .find_submission_by_id(&submission.settlement_chain_submission_id)
        .await
        .expect("reload confirmed submission")
        .expect("confirmed submission exists");
    assert_eq!(
        durable_submission.state,
        SettlementSubmissionState::Confirmed
    );
    assert!(durable_submission.receipt_evidence_json.is_some());
    assert_eq!(
        settlement
            .list_lots_by_redeem(&redeem.settlement_redeem_id)
            .await
            .expect("load immutable settlement lots")
            .len(),
        1
    );
    assert_eq!(
        positions
            .find_by_intent(intent_id)
            .await
            .expect("reload closed position")
            .expect("position exists")
            .state,
        PositionLedgerState::Settled
    );
    assert_eq!(
        PgCapitalAllocationRepository::new(db.clone())
            .find_by_intent(intent_id)
            .await
            .expect("reload released capital")
            .expect("capital exists")
            .state,
        CapitalAllocationState::Released
    );
    assert_eq!(
        PgOrderIntentRepository::new(db.clone())
            .find_by_id(intent_id)
            .await
            .expect("reload exited intent")
            .expect("intent exists")
            .exit_state,
        ExitState::Exited
    );
    let execution_outcome = PgRecommendationExecutionOutcomeRepository::new(db.clone())
        .reconcile_intent(intent_id, db.statement_time().await)
        .await
        .expect("seal settled execution outcome");
    let execution_outcome = match execution_outcome {
        ExecutionOutcomeReconciliationResult::Inserted(outcome) => outcome,
        other => panic!("expected inserted settled execution outcome, got {other:?}"),
    };
    assert_eq!(
        execution_outcome.position_terminal_state,
        Some(PositionLedgerState::Settled)
    );
    assert_eq!(
        execution_outcome.settlement_payout_usd,
        Some(Usd::new(SETTLEMENT_PAYOUT_USD))
    );
    assert_eq!(execution_outcome.realized_pnl_usd, Some(Usd::new(dec!(15))));
    assert_eq!(
        QuantDomainEventOutboxEntity::find()
            .all(db)
            .await
            .expect("load durable domain outbox")
            .len(),
        1
    );
}

async fn authorize_prepare_and_dispatch(
    repository: &PgSettlementRedeemRepository,
    redeem_id: SettlementRedeemId,
    digest: ContentHash,
    actor: UserId,
    now: DateTime<Utc>,
) -> SettlementChainSubmissionInfo {
    let wrong_approval = repository
        .approve_authorization(ApproveSettlementAuthorization {
            settlement_redeem_id: redeem_id,
            digest: ContentHash::from_bytes([0x72; 32]),
            actor,
            approved_at: now + TimeDelta::seconds(2),
        })
        .await;
    assert!(wrong_approval.is_err(), "approval digest is a strict CAS");
    let approved = repository
        .approve_authorization(ApproveSettlementAuthorization {
            settlement_redeem_id: redeem_id,
            digest,
            actor,
            approved_at: now + TimeDelta::seconds(2),
        })
        .await
        .expect("approve exact unexpired authorization");
    let idempotent = repository
        .approve_authorization(ApproveSettlementAuthorization {
            settlement_redeem_id: redeem_id,
            digest,
            actor,
            approved_at: now + TimeDelta::seconds(2),
        })
        .await
        .expect("same actor and digest retry is idempotent");
    assert_eq!(idempotent.authorized_at, approved.authorized_at);

    let writer = WorkerId::from_v7();
    let lease_until = now + TimeDelta::seconds(30);
    let claim = repository
        .claim_next_new_submission(&writer, now + TimeDelta::seconds(3), lease_until)
        .await
        .expect("claim ready authorized case")
        .expect("authorized case is claimable");
    let competing = repository
        .claim_next_new_submission(
            &WorkerId::from_v7(),
            now + TimeDelta::seconds(3),
            lease_until,
        )
        .await
        .expect("concurrent claim query");
    assert!(competing.is_none(), "SKIP LOCKED lease is single-owner");

    let durable = repository
        .persist_prepared_submission(PersistPreparedSettlementSubmission {
            owner: writer,
            expected_authorization_digest: Some(digest),
            expected_canary_action_id: None,
            submission: current_eoa_submission(claim.redeem.settlement_redeem_id, 0x71),
            persisted_at: now + TimeDelta::seconds(4),
        })
        .await
        .expect("authorization consumption and envelope insert are atomic");
    let consumed = repository
        .find_by_id(&redeem_id)
        .await
        .expect("reload consumed case")
        .expect("case exists");
    assert_eq!(
        consumed.authorization_state,
        SettlementAuthorizationState::Consumed
    );

    let envelope_hash = durable.signed_envelope_hash.expect("frozen envelope hash");
    let wrong_dispatch = repository
        .begin_dispatch(BeginSettlementDispatch {
            settlement_redeem_id: redeem_id,
            settlement_chain_submission_id: durable.settlement_chain_submission_id,
            owner: writer,
            expected_target_adapter: address(0x7f),
            expected_deployment_digest: durable.deployment_digest,
            expected_calldata_hash: durable.calldata_hash.clone(),
            expected_signed_envelope_hash: envelope_hash,
            dispatching_at: now + TimeDelta::seconds(5),
        })
        .await;
    assert!(
        wrong_dispatch.is_err(),
        "target replacement fails the dispatch CAS"
    );
    repository
        .begin_dispatch(BeginSettlementDispatch {
            settlement_redeem_id: redeem_id,
            settlement_chain_submission_id: durable.settlement_chain_submission_id,
            owner: writer,
            expected_target_adapter: durable.target_adapter.clone(),
            expected_deployment_digest: durable.deployment_digest,
            expected_calldata_hash: durable.calldata_hash.clone(),
            expected_signed_envelope_hash: envelope_hash,
            dispatching_at: now + TimeDelta::seconds(5),
        })
        .await
        .expect("exact frozen submission enters dispatching");
    repository
        .record_eoa_broadcast(RecordEoaSettlementBroadcast {
            settlement_redeem_id: redeem_id,
            settlement_chain_submission_id: durable.settlement_chain_submission_id,
            owner: writer,
            expected_signed_envelope_hash: envelope_hash,
            observed_at: now + TimeDelta::seconds(6),
        })
        .await
        .expect("EOA local hash advances separately to finality tracking");
    repository
        .release_claim(&redeem_id, &writer)
        .await
        .expect("release writer claim");
    durable
}

#[derive(Default)]
struct ProbeGovernedActionExecutor {
    preparations: AtomicUsize,
    dispatches: AtomicUsize,
    tracked: AtomicUsize,
}

#[async_trait]
impl SettlementGovernedActionExecutor for ProbeGovernedActionExecutor {
    async fn prepare_action(
        &self,
        action: &SettlementGovernedActionInfo,
    ) -> Result<NewSettlementChainSubmission, SettlementExecutorError> {
        self.preparations.fetch_add(1, Ordering::SeqCst);
        Ok(governed_approval_submission(action, 0xa2))
    }

    async fn dispatch_action(
        &self,
        _submission: &SettlementChainSubmissionInfo,
    ) -> Result<SettlementDispatchResult, SettlementExecutorError> {
        self.dispatches.fetch_add(1, Ordering::SeqCst);
        Ok(SettlementDispatchResult::EoaAccepted)
    }

    async fn track_action(
        &self,
        action: &SettlementGovernedActionInfo,
        submission: &SettlementChainSubmissionInfo,
    ) -> Result<SettlementGovernedActionTrackingResult, SettlementExecutorError> {
        self.tracked.fetch_add(1, Ordering::SeqCst);
        let desired_approval =
            action
                .desired_approval
                .ok_or_else(|| SettlementExecutorError::Invariant {
                    detail: "probe governed action has no desired approval state".to_owned(),
                })?;
        let transaction_hash = submission.transaction_hash.clone().ok_or_else(|| {
            SettlementExecutorError::Invariant {
                detail: "probe governed submission has no transaction hash".to_owned(),
            }
        })?;
        Ok(SettlementGovernedActionTrackingResult::Confirmed(Box::new(
            SettlementChainReceiptEvidence::OperatorApproval(Box::new(
                SettlementOperatorApprovalReceiptEvidence {
                    chain_id: 137,
                    transaction_hash,
                    block_number: 90_685_201,
                    block_hash: block_hash(0xa3),
                    finalized_block_number: 90_685_202,
                    finalized_block_hash: block_hash(0xa4),
                    call: SettlementMinedCallEvidence {
                        wallet_kind: ExecutionWalletKind::Eoa,
                        outer_sender: address(0x81),
                        outer_target: submission.call_target.clone(),
                        outer_calldata_hash: submission.calldata_hash.clone(),
                        inner_target: submission.call_target.clone(),
                        inner_calldata_hash: submission.calldata_hash.clone(),
                    },
                    receipt_success: true,
                    desired_approval,
                    operator_approved: desired_approval,
                    canonical_checked_at: Utc::now(),
                    observed_at: Utc::now(),
                },
            )),
        )))
    }
}

#[derive(Default)]
struct ProbeSettlementExecutor {
    preparations: AtomicUsize,
    dispatches: AtomicUsize,
    tracked: AtomicUsize,
}

#[async_trait]
impl SettlementSubmissionExecutor for ProbeSettlementExecutor {
    async fn prepare(
        &self,
        _redeem: &SettlementRedeemInfo,
    ) -> Result<NewSettlementChainSubmission, SettlementExecutorError> {
        self.preparations.fetch_add(1, Ordering::SeqCst);
        Err(SettlementExecutorError::Invariant {
            detail: "probe executor must never sign in this scenario".to_owned(),
        })
    }

    async fn dispatch(
        &self,
        _submission: &SettlementChainSubmissionInfo,
    ) -> Result<SettlementDispatchResult, SettlementExecutorError> {
        self.dispatches.fetch_add(1, Ordering::SeqCst);
        Ok(SettlementDispatchResult::Ambiguous)
    }

    async fn track(
        &self,
        _redeem: &SettlementRedeemInfo,
        _submission: &SettlementChainSubmissionInfo,
    ) -> Result<SettlementTrackingResult, SettlementExecutorError> {
        self.tracked.fetch_add(1, Ordering::SeqCst);
        Ok(SettlementTrackingResult::Pending)
    }
}

async fn settlement_persistence_scenario() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_market(&db).await;
    seed_execution_accounts(&db, &[0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19]).await;
    let repository = PgSettlementRedeemRepository::new(db.clone());

    let active_case = insert_case_fixture(&db, &repository, discovered_case(0x11))
        .await
        .expect("insert active submission case");
    let durable_eoa = insert_submission_fixture(
        &db,
        current_eoa_submission(active_case.settlement_redeem_id, 0x31),
    )
    .await
    .expect("current verified EOA submission is durable");
    let recovered_eoa = repository
        .find_submission_by_id(&durable_eoa.settlement_chain_submission_id)
        .await
        .expect("reload durable EOA submission")
        .expect("durable EOA submission exists after repository restart boundary");
    assert_eq!(recovered_eoa.signed_envelope, durable_eoa.signed_envelope);
    assert_eq!(recovered_eoa.prepared_nonce, durable_eoa.prepared_nonce);
    assert_eq!(recovered_eoa.gas_limit, durable_eoa.gas_limit);
    assert_eq!(
        recovered_eoa.signed_envelope_hash,
        durable_eoa.signed_envelope_hash
    );
    let duplicate_active = insert_submission_fixture(
        &db,
        current_eoa_submission(active_case.settlement_redeem_id, 0x32),
    )
    .await;
    assert!(
        duplicate_active.is_err(),
        "one case cannot have two active redeem submissions"
    );

    let external_case = insert_case_fixture(&db, &repository, discovered_case(0x12))
        .await
        .expect("insert external-observation case");
    let external_prepared = insert_submission_fixture(
        &db,
        external_submission(
            external_case.settlement_redeem_id,
            SettlementSubmissionState::Prepared,
            None,
        ),
    )
    .await;
    assert!(
        external_prepared.is_err(),
        "external observation cannot enter Prepared"
    );
    insert_submission_fixture(
        &db,
        external_submission(
            external_case.settlement_redeem_id,
            SettlementSubmissionState::AwaitingFinality,
            Some(transaction_hash(0x41)),
        ),
    )
    .await
    .expect("current target is available for external reconciliation");

    let relayer_case = insert_case_fixture(&db, &repository, discovered_case(0x13))
        .await
        .expect("insert relayer corruption case");
    let mut relayer_without_identity =
        current_eoa_submission(relayer_case.settlement_redeem_id, 0x33);
    relayer_without_identity.kind = SettlementSubmissionKind::Relayer;
    relayer_without_identity.state = SettlementSubmissionState::AwaitingChainHash;
    relayer_without_identity.transaction_hash = None;
    let relayer_without_identity = insert_submission_fixture(&db, relayer_without_identity).await;
    assert!(
        relayer_without_identity.is_err(),
        "AwaitingChainHash requires a durable relayer ID"
    );

    let relayer_restart_case = insert_case_fixture(&db, &repository, discovered_case(0x16))
        .await
        .expect("insert relayer restart case");
    let mut prepared_relayer =
        current_eoa_submission(relayer_restart_case.settlement_redeem_id, 0x35);
    prepared_relayer.kind = SettlementSubmissionKind::Relayer;
    prepared_relayer.transaction_hash = None;
    prepared_relayer.signed_envelope = Some(br#"{"type":"SAFE","nonce":"19"}"#.to_vec());
    prepared_relayer.signed_envelope_hash = Some(
        ContentHash::parse(
            "blake3:3bcbab3912c3b373510ca9907fe1bc8c46818b75a5d30eae3491b0b6e182af00",
        )
        .expect("relayer envelope hash"),
    );
    prepared_relayer.prepared_nonce = Some(EvmUint256::parse("19").expect("relayer nonce"));
    prepared_relayer.gas_limit = Some(EvmUint256::parse("0").expect("Safe gas limit"));
    let durable_relayer = insert_submission_fixture(&db, prepared_relayer)
        .await
        .expect("durably prepare relayer body before submit");
    let recovered_relayer = repository
        .find_submission_by_id(&durable_relayer.settlement_chain_submission_id)
        .await
        .expect("reload durable relayer submission")
        .expect("relayer submission survives restart boundary");
    assert_eq!(
        recovered_relayer.signed_envelope,
        durable_relayer.signed_envelope
    );
    assert_eq!(
        recovered_relayer.prepared_nonce,
        durable_relayer.prepared_nonce
    );
    assert!(recovered_relayer.transaction_hash.is_none());

    let corrupt_case = insert_case_fixture(&db, &repository, discovered_case(0x15))
        .await
        .expect("insert envelope corruption case");
    let mut envelope_without_hash = current_eoa_submission(corrupt_case.settlement_redeem_id, 0x34);
    envelope_without_hash.signed_envelope_hash = None;
    let envelope_without_hash = insert_submission_fixture(&db, envelope_without_hash).await;
    assert!(
        envelope_without_hash.is_err(),
        "signed envelope bytes require a durable envelope hash, nonce and gas limit"
    );

    let binding_case = insert_case_fixture(&db, &repository, discovered_case(0x19))
        .await
        .expect("insert binding corruption case");
    let mut wrong_binding = current_eoa_submission(binding_case.settlement_redeem_id, 0x36);
    wrong_binding.collateral_token =
        EvmAddress::parse("0x9999999999999999999999999999999999999999")
            .expect("syntactically valid wrong collateral");
    assert!(
        insert_submission_fixture(&db, wrong_binding).await.is_err(),
        "a submission cannot freeze bindings outside the canonical boot manifest"
    );

    let mut incomplete_ready = discovered_case(0x14);
    incomplete_ready.readiness_status = SettlementReadinessStatus::Ready;
    let incomplete_ready = insert_case_fixture(&db, &repository, incomplete_ready).await;
    assert!(
        incomplete_ready.is_err(),
        "Ready cannot be persisted without target, digest, evidence version and block hash"
    );

    Box::pin(assert_account_scope_renewal(&db, &repository)).await;
    let governed_parent = insert_case_fixture(&db, &repository, ready_case(0x18))
        .await
        .expect("insert governed-action parent constraint case");
    assert_governed_action_invariants(&db, &repository, governed_parent).await;
}

async fn assert_account_scope_renewal(
    db: &DatabaseConnection,
    repository: &PgSettlementRedeemRepository,
) {
    let redeem = seed_ready_automatic_case(db, repository, 0x17).await;
    let mut duplicate = discovered_case(0x17);
    duplicate.market_id = redeem.market_id.clone();
    duplicate.yes_token_id = redeem.yes_token_id.clone();
    duplicate.no_token_id = redeem.no_token_id.clone();
    duplicate.execution_account_id = redeem.execution_account_id;
    duplicate.route = redeem.route;
    duplicate.resolution_content_hash = redeem.resolution_content_hash;
    duplicate
        .resolution_outcome
        .clone_from(&redeem.resolution_outcome);
    duplicate.resolved_at = redeem.resolved_at;
    let duplicate = insert_case_fixture(db, repository, duplicate).await;
    assert!(
        duplicate.is_err(),
        "one market cannot contain duplicate cases for the same immutable execution account"
    );
    let exact = repository
        .find_by_market_account(&redeem.market_id, &redeem.execution_account_id)
        .await
        .expect("load exact market/account case")
        .expect("exact account-scoped case exists");
    assert_eq!(exact.settlement_redeem_id, redeem.settlement_redeem_id);
    assert_eq!(exact.execution_account_id, redeem.execution_account_id);

    let now = Utc::now();
    let owner = WorkerId::from_v7();
    let claim = repository
        .claim_next_new_submission(&owner, now, now + TimeDelta::seconds(60))
        .await
        .expect("claim authorization renewal case")
        .expect("ready account-scoped case is claimable");
    assert_eq!(
        claim.redeem.settlement_redeem_id,
        redeem.settlement_redeem_id
    );
    let first_digest = ContentHash::from_bytes([0x81; 32]);
    let first = repository
        .stage_authorization(StageSettlementAuthorization {
            settlement_redeem_id: redeem.settlement_redeem_id,
            owner,
            digest: first_digest,
            expires_at: now + TimeDelta::seconds(2),
            expected_target_adapter: redeem
                .target_adapter
                .clone()
                .expect("ready case target adapter"),
            expected_deployment_digest: redeem
                .deployment_digest
                .expect("ready case deployment digest"),
            staged_at: now,
        })
        .await
        .expect("stage first immutable authorization attempt");
    assert_eq!(
        first.authorization_state,
        SettlementAuthorizationState::Pending
    );
    let first_id = first
        .current_authorization_id
        .expect("first authorization identity");

    let second_digest = ContentHash::from_bytes([0x82; 32]);
    let renewed = repository
        .stage_authorization(StageSettlementAuthorization {
            settlement_redeem_id: redeem.settlement_redeem_id,
            owner,
            digest: second_digest,
            expires_at: now + TimeDelta::seconds(30),
            expected_target_adapter: redeem
                .target_adapter
                .clone()
                .expect("ready case target adapter"),
            expected_deployment_digest: redeem
                .deployment_digest
                .expect("ready case deployment digest"),
            staged_at: now + TimeDelta::seconds(3),
        })
        .await
        .expect("expired authorization is renewed with a new immutable attempt");
    assert_ne!(renewed.current_authorization_id, Some(first_id));
    assert_eq!(renewed.authorization_digest, Some(second_digest));
    let attempts = QuantSettlementAuthorizationEntity::find()
        .filter(
            QuantSettlementAuthorizationColumn::SettlementRedeemId.eq(redeem.settlement_redeem_id),
        )
        .all(db)
        .await
        .expect("load authorization attempt history");
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].state, SettlementAuthorizationState::Expired);
    assert!(attempts[0].expired_at.is_some());
    assert_eq!(attempts[1].attempt_ordinal, 2);
    assert_eq!(attempts[1].state, SettlementAuthorizationState::Pending);
}

async fn assert_governed_action_invariants(
    db: &DatabaseConnection,
    redeem_repository: &PgSettlementRedeemRepository,
    redeem: SettlementRedeemInfo,
) {
    let users = PgUserRepository::new(db.clone());
    let operator = users
        .find_by_username("admin")
        .await
        .expect("load governed operator")
        .expect("seeded operator exists");
    let governance = PgSettlementGovernanceRepository::new(db.clone());
    let now = Utc::now();
    let action_id = SettlementGovernedActionId::from_v7();
    let scope_digest = ContentHash::from_bytes([0x91; 32]);
    let new_action = NewSettlementGovernedAction {
        settlement_governed_action_id: action_id,
        execution_account_id: execution_account(0x18).execution_account_id,
        settlement_redeem_id: None,
        kind: SettlementGovernedActionKind::OutcomeTokenApproval,
        state: SettlementGovernedActionState::Authorized,
        route: Some(SettlementRoute::StandardV2),
        target_adapter: Some(EvmAddress::parse(CURRENT_STANDARD).expect("current governed target")),
        deployment_digest: Some(ContentHash::from_bytes([0x92; 32])),
        deployment_evidence_version: Some(evidence_version()),
        verified_block_number: Some(90_685_098),
        verified_block_hash: Some(block_hash(0x93)),
        desired_approval: Some(true),
        authorization_digest: None,
        payout_ceiling_usd: None,
        scope_digest,
        idempotency_key: SettlementActionIdempotencyKey::parse("approval-operator-1")
            .expect("governed action idempotency key"),
        authorization_reason: "operator authorized exact adapter scope".to_owned(),
        authorized_by: operator.id,
        revoked_by: None,
        revocation_reason: None,
        expires_at: now + TimeDelta::minutes(5),
        authorized_at: now,
        consumed_at: None,
        revoked_at: None,
        failure_code: None,
        retry_count: 0,
        claim_owner: None,
        lease_expires_at: None,
        next_attempt_at: Some(now),
        last_error: None,
    };
    let action = governance
        .create_action(new_action.clone())
        .await
        .expect("authorized operator creates exact governed action");
    let approved_action = governance
        .create_action(new_action)
        .await
        .expect("same operator idempotency replay succeeds");
    assert_eq!(
        approved_action.state,
        SettlementGovernedActionState::Authorized
    );
    assert_eq!(approved_action.authorized_by, operator.id);
    assert_eq!(
        approved_action.settlement_governed_action_id,
        action.settlement_governed_action_id
    );

    let mut corrupt_parent = current_eoa_submission(redeem.settlement_redeem_id, 0x94);
    corrupt_parent.settlement_governed_action_id = Some(action.settlement_governed_action_id);
    assert!(
        insert_submission_fixture(db, corrupt_parent).await.is_err(),
        "submission must belong to exactly one typed parent"
    );

    let cursor_id = SettlementExternalCursorId::from_v7();
    governance
        .ensure_cursor(NewSettlementExternalCursor {
            settlement_external_cursor_id: cursor_id,
            execution_account_id: execution_account(0x18).execution_account_id,
            chain_id: 137,
            route: SettlementRoute::StandardV2,
            target_adapter: EvmAddress::parse(CURRENT_STANDARD).expect("cursor target"),
            target_code_hash: code_hash(CURRENT_STANDARD_CODE_HASH),
            deployment_digest: ContentHash::from_bytes([0x18; 32]),
            deployment_evidence_version: evidence_version(),
            next_block_number: 100,
            last_observed_block_number: None,
            last_observed_block_hash: None,
        })
        .await
        .expect("create deployment-scoped external cursor");
    let advanced = governance
        .persist_scan(PersistExternalSettlementScan {
            cursor: AdvanceSettlementExternalCursor {
                settlement_external_cursor_id: cursor_id,
                expected_next_block_number: 100,
                next_block_number: 110,
                last_observed_block_number: 109,
                last_observed_block_hash: block_hash(0x96),
            },
            submissions: vec![external_scan_submission(&redeem)],
            observed_at: now + TimeDelta::seconds(3),
        })
        .await
        .expect("journal external observation and advance cursor atomically");
    assert_eq!(advanced.next_block_number, 110);
    assert_eq!(
        redeem_repository
            .list_submissions_by_redeem(&redeem.settlement_redeem_id)
            .await
            .expect("load journaled external submission")
            .len(),
        1
    );
    assert!(
        governance
            .advance_cursor(AdvanceSettlementExternalCursor {
                settlement_external_cursor_id: cursor_id,
                expected_next_block_number: 100,
                next_block_number: 120,
                last_observed_block_number: 119,
                last_observed_block_hash: block_hash(0x97),
            })
            .await
            .is_err(),
        "stale cursor compare-and-swap cannot skip persisted progress"
    );
}

fn external_scan_submission(redeem: &SettlementRedeemInfo) -> NewSettlementChainSubmission {
    let mut submission = external_submission(
        redeem.settlement_redeem_id,
        SettlementSubmissionState::AwaitingFinality,
        Some(transaction_hash(0x98)),
    );
    submission.deployment_digest = redeem
        .deployment_digest
        .expect("ready case deployment digest");
    submission.verified_block_hash = block_hash(0x99);
    submission
}

async fn seed_market(db: &DatabaseConnection) {
    PgEventRepository::new(db.clone())
        .upsert(make_event(
            "settlement-persistence-event",
            "Settlement persistence event",
            "settlement-persistence-event",
            MarketCategory::Crypto,
        ))
        .await
        .expect("seed settlement event");
    PgMarketRepository::new(db.clone())
        .upsert(make_market(
            MARKET_ID,
            "settlement-persistence-event",
            "Will the settlement schema remain fail closed?",
            MARKET_ID,
            MarketCategory::Crypto,
            None,
        ))
        .await
        .expect("seed settlement market");
}

async fn seed_execution_accounts(db: &DatabaseConnection, address_bytes: &[u8]) {
    let repository = PgExecutionAccountRepository::new(db.clone());
    for address_byte in address_bytes {
        repository
            .ensure(execution_account(*address_byte))
            .await
            .expect("seed immutable execution account");
    }
}

async fn seed_ready_automatic_case(
    db: &DatabaseConnection,
    repository: &PgSettlementRedeemRepository,
    identity_byte: u8,
) -> SettlementRedeemInfo {
    let ids = seed_settlement_report_fixture(db).await;
    enable_test_admission(db, "settlement-ready-case").await;
    let intent_id = seed_approved_intent(db, &ids).await;
    set_intent_redeem_policy(db, intent_id, RedeemPolicy::Auto, Utc::now()).await;
    fill_entry_lot(
        db,
        &PgExecutionSubmissionRepository::new(db.clone()),
        &ids,
        &intent_id,
    )
    .await;

    let resolved_at = Utc::now();
    let market = MarketEntity::find_by_id(MarketId::new(&ids.market))
        .one(db)
        .await
        .expect("load automatic settlement market")
        .expect("automatic settlement market exists");
    let mut active_market = market.into_active_model();
    active_market.status = ActiveValue::Set(MarketStatus::Settled);
    active_market.outcome = ActiveValue::Set(Some("Yes".to_owned()));
    active_market.yes_token_id = ActiveValue::Set(TokenId::new(&ids.token));
    active_market.resolved_at = ActiveValue::Set(Some(resolved_at));
    active_market.content_hash =
        ActiveValue::Set(ContentHash::from_bytes([identity_byte.wrapping_add(1); 32]));
    active_market
        .update(db)
        .await
        .expect("persist automatic settlement resolution");

    let discovery =
        SettlementDiscoveryService::new(Arc::new(PgSettlementRedeemRepository::new(db.clone())));
    let summary = discovery
        .run_once(resolved_at + TimeDelta::seconds(1), 32)
        .await
        .expect("discover automatic settlement inventory");
    assert_eq!(summary.discovered, 1);
    let discovered = repository
        .find_by_market_account(&MarketId::new(&ids.market), &ids.execution_account)
        .await
        .expect("load automatic settlement case")
        .expect("automatic settlement case exists");
    assert_eq!(
        discovered.effective_policy,
        SettlementEffectivePolicy::AutomaticEligible
    );
    persist_ready_preflight(
        repository,
        &discovered,
        identity_byte,
        resolved_at + TimeDelta::seconds(2),
    )
    .await
}

async fn persist_ready_preflight(
    repository: &PgSettlementRedeemRepository,
    redeem: &SettlementRedeemInfo,
    identity_byte: u8,
    observed_at: DateTime<Utc>,
) -> SettlementRedeemInfo {
    let owner = WorkerId::from_v7();
    let claim = repository
        .claim_next_preflight(&owner, observed_at, observed_at + TimeDelta::seconds(30))
        .await
        .expect("claim signer-free settlement preflight")
        .expect("unchecked settlement case is preflight-claimable");
    assert_eq!(
        claim.redeem.settlement_redeem_id,
        redeem.settlement_redeem_id
    );
    repository
        .persist_preflight(PersistSettlementPreflight {
            settlement_redeem_id: redeem.settlement_redeem_id,
            owner,
            expected_inventory_digest: redeem.inventory_digest,
            readiness_status: SettlementReadinessStatus::Ready,
            readiness_evidence: SettlementReadinessEvidence::default(),
            target_adapter: Some(
                EvmAddress::parse(CURRENT_STANDARD).expect("canonical current standard adapter"),
            ),
            target_code_hash: Some(code_hash(CURRENT_STANDARD_CODE_HASH)),
            deployment_digest: Some(ContentHash::from_bytes([identity_byte; 32])),
            deployment_evidence_version: Some(evidence_version()),
            verified_block_number: Some(90_685_098),
            verified_block_hash: Some(block_hash(identity_byte)),
            payout_vector: payout_vector(),
            balance_before: Some(SettlementBalanceEvidence {
                yes: SettlementTokenBalance {
                    token_id: redeem.yes_token_id.clone(),
                    raw_balance: EvmUint256::parse(WINNING_TOKEN_RAW_BALANCE)
                        .expect("automatic YES raw balance"),
                    shares: Shares::new(ENTRY_FILLED_SHARES),
                },
                no: SettlementTokenBalance {
                    token_id: redeem.no_token_id.clone(),
                    raw_balance: EvmUint256::parse("0").expect("automatic NO raw balance"),
                    shares: Shares::ZERO,
                },
            }),
            expected_payout_usd: Some(Usd::new(SETTLEMENT_PAYOUT_USD)),
            failure_code: None,
            next_attempt_at: None,
            observed_at,
        })
        .await
        .expect("persist current automatic settlement preflight")
}

async fn set_intent_redeem_policy(
    db: &DatabaseConnection,
    order_intent_id: OrderIntentId,
    redeem_policy: RedeemPolicy,
    updated_at: DateTime<Utc>,
) {
    let intent = QuantOrderIntentEntity::find_by_id(order_intent_id)
        .one(db)
        .await
        .expect("load settlement contributor intent")
        .expect("settlement contributor intent exists");
    let mut exit_policy = intent.exit_policy_json.clone();
    exit_policy.settlement_mode = ExitSettlementMode::HoldToResolution;
    exit_policy.redeem_policy = redeem_policy;
    let mut active = intent.into_active_model();
    active.exit_policy_json = ActiveValue::Set(exit_policy);
    active.updated_at = ActiveValue::Set(updated_at);
    active
        .update(db)
        .await
        .expect("set settlement contributor redeem policy");
}

fn execution_account(address_byte: u8) -> NewExecutionAccount {
    let funder = address(address_byte);
    NewExecutionAccount::build(
        137,
        funder.clone(),
        ExecutionWalletKind::Eoa,
        funder.clone(),
        funder,
        None,
        None,
    )
    .expect("canonical execution account")
}

async fn insert_case_fixture(
    db: &DatabaseConnection,
    repository: &PgSettlementRedeemRepository,
    redeem: NewSettlementRedeem,
) -> Result<SettlementRedeemInfo, StorageError> {
    let settlement_redeem_id = redeem.settlement_redeem_id;
    QuantSettlementRedeemEntity::insert(redeem.into_active_model())
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    repository
        .find_by_id(&settlement_redeem_id)
        .await?
        .ok_or_else(|| StorageError::not_found("quant_settlement_redeem", settlement_redeem_id))
}

async fn insert_submission_fixture(
    db: &DatabaseConnection,
    submission: NewSettlementChainSubmission,
) -> Result<SettlementChainSubmissionInfo, StorageError> {
    QuantSettlementChainSubmissionEntity::insert(submission.into_active_model())
        .exec_with_returning(db)
        .await
        .map_err(StorageError::from)
        .map(Into::into)
}

fn discovered_case(address_byte: u8) -> NewSettlementRedeem {
    let execution_account = execution_account(address_byte);
    let now = Utc::now();
    NewSettlementRedeem {
        settlement_redeem_id: SettlementRedeemId::from_v7(),
        market_id: MarketId::new(MARKET_ID),
        yes_token_id: TokenId::new("101"),
        no_token_id: TokenId::new("102"),
        execution_account_id: execution_account.execution_account_id,
        resolution_content_hash: ContentHash::from_bytes([address_byte.wrapping_add(2); 32]),
        resolution_outcome: "Yes".to_owned(),
        resolved_at: now,
        route: SettlementRoute::StandardV2,
        effective_policy: SettlementEffectivePolicy::AutomaticEligible,
        inventory_digest: ContentHash::from_bytes([address_byte; 32]),
        contributor_lots_digest: ContentHash::from_bytes([address_byte.wrapping_add(1); 32]),
        state: SettlementCaseState::Discovered,
        readiness_status: SettlementReadinessStatus::Unchecked,
        readiness_evidence_json: SettlementReadinessEvidence::default(),
        target_adapter: None,
        target_code_hash: None,
        deployment_digest: None,
        deployment_evidence_version: None,
        verified_block_number: None,
        verified_block_hash: None,
        current_authorization_id: None,
        reconciliation_state: SettlementReconciliationState::NotRequired,
        payout_vector_json: payout_vector(),
        balance_before_json: None,
        balance_after_json: None,
        expected_payout_usd: None,
        actual_payout_usd: None,
        gas_fee_pol: None,
        failure_code: None,
        attempt_count: 0,
        retry_count: 0,
        next_attempt_at: None,
        claim_owner: None,
        lease_expires_at: None,
        last_error: None,
        prepared_at: None,
        submitted_at: None,
        confirmed_at: None,
        failed_at: None,
        created_at: now,
        updated_at: now,
    }
}

fn ready_case(identity_byte: u8) -> NewSettlementRedeem {
    let mut redeem = discovered_case(identity_byte);
    let now = Utc::now();
    redeem.state = SettlementCaseState::Prepared;
    redeem.readiness_status = SettlementReadinessStatus::Ready;
    redeem.target_adapter =
        Some(EvmAddress::parse(CURRENT_STANDARD).expect("canonical current standard adapter"));
    redeem.target_code_hash = Some(code_hash(CURRENT_STANDARD_CODE_HASH));
    redeem.deployment_digest = Some(ContentHash::from_bytes([identity_byte; 32]));
    redeem.deployment_evidence_version = Some(evidence_version());
    redeem.verified_block_number = Some(90_685_098);
    redeem.verified_block_hash = Some(block_hash(0x51));
    redeem.balance_before_json = Some(SettlementBalanceEvidence {
        yes: SettlementTokenBalance {
            token_id: TokenId::new("101"),
            raw_balance: EvmUint256::parse("1000000").expect("YES raw balance"),
            shares: Shares::new(dec!(1)),
        },
        no: SettlementTokenBalance {
            token_id: TokenId::new("102"),
            raw_balance: EvmUint256::parse("0").expect("NO raw balance"),
            shares: Shares::new(dec!(0)),
        },
    });
    redeem.expected_payout_usd = Some(Usd::new(dec!(1)));
    redeem.prepared_at = Some(now);
    redeem.updated_at = now;
    redeem
}

fn governed_approval_action(
    execution_account_id: ExecutionAccountId,
    authorized_by: UserId,
    authorized_at: DateTime<Utc>,
    identity_byte: u8,
) -> NewSettlementGovernedAction {
    NewSettlementGovernedAction {
        settlement_governed_action_id: SettlementGovernedActionId::from_v7(),
        execution_account_id,
        settlement_redeem_id: None,
        kind: SettlementGovernedActionKind::OutcomeTokenApproval,
        state: SettlementGovernedActionState::Authorized,
        route: Some(SettlementRoute::StandardV2),
        target_adapter: Some(
            EvmAddress::parse(CURRENT_STANDARD).expect("canonical governed target"),
        ),
        deployment_digest: Some(ContentHash::from_bytes([identity_byte; 32])),
        deployment_evidence_version: Some(evidence_version()),
        verified_block_number: Some(90_685_098),
        verified_block_hash: Some(block_hash(identity_byte)),
        desired_approval: Some(true),
        authorization_digest: None,
        payout_ceiling_usd: None,
        scope_digest: ContentHash::from_bytes([identity_byte.wrapping_add(1); 32]),
        idempotency_key: SettlementActionIdempotencyKey::parse(format!(
            "governed-approval-{identity_byte}"
        ))
        .expect("governed idempotency key"),
        authorization_reason: "authorize exact current adapter".to_owned(),
        authorized_by,
        revoked_by: None,
        revocation_reason: None,
        expires_at: authorized_at + TimeDelta::minutes(5),
        authorized_at,
        consumed_at: None,
        revoked_at: None,
        failure_code: None,
        retry_count: 0,
        claim_owner: None,
        lease_expires_at: None,
        next_attempt_at: Some(authorized_at),
        last_error: None,
    }
}

fn governed_canary_action(
    redeem: &SettlementRedeemInfo,
    authorization_digest: ContentHash,
    authorized_by: UserId,
    authorized_at: DateTime<Utc>,
) -> NewSettlementGovernedAction {
    NewSettlementGovernedAction {
        settlement_governed_action_id: SettlementGovernedActionId::from_v7(),
        execution_account_id: redeem.execution_account_id,
        settlement_redeem_id: Some(redeem.settlement_redeem_id),
        kind: SettlementGovernedActionKind::CanaryGrant,
        state: SettlementGovernedActionState::Authorized,
        route: Some(redeem.route),
        target_adapter: redeem.target_adapter.clone(),
        deployment_digest: redeem.deployment_digest,
        deployment_evidence_version: redeem.deployment_evidence_version.clone(),
        verified_block_number: redeem.verified_block_number,
        verified_block_hash: redeem.verified_block_hash.clone(),
        desired_approval: None,
        authorization_digest: Some(authorization_digest),
        payout_ceiling_usd: redeem.expected_payout_usd,
        scope_digest: ContentHash::from_bytes([0x75; 32]),
        idempotency_key: SettlementActionIdempotencyKey::parse("governed-canary-73")
            .expect("governed-canary idempotency key"),
        authorization_reason: "authorize exact one-shot settlement canary".to_owned(),
        authorized_by,
        revoked_by: None,
        revocation_reason: None,
        expires_at: authorized_at + TimeDelta::minutes(5),
        authorized_at,
        consumed_at: None,
        revoked_at: None,
        failure_code: None,
        retry_count: 0,
        claim_owner: None,
        lease_expires_at: None,
        next_attempt_at: Some(authorized_at),
        last_error: None,
    }
}

fn governed_approval_submission(
    action: &SettlementGovernedActionInfo,
    identity_byte: u8,
) -> NewSettlementChainSubmission {
    let signed_envelope = vec![0x02, identity_byte];
    let signed_envelope_hash = ContentHash::from_bytes(*blake3::hash(&signed_envelope).as_bytes());
    NewSettlementChainSubmission {
        settlement_chain_submission_id: SettlementChainSubmissionId::from_v7(),
        settlement_redeem_id: None,
        settlement_governed_action_id: Some(action.settlement_governed_action_id),
        canary_action_id: None,
        purpose: SettlementSubmissionPurpose::OutcomeTokenApproval,
        kind: SettlementSubmissionKind::DirectEoa,
        state: SettlementSubmissionState::Prepared,
        route: action.route.expect("governed route"),
        target_adapter: action
            .target_adapter
            .clone()
            .expect("governed target adapter"),
        target_code_hash: code_hash(CURRENT_STANDARD_CODE_HASH),
        conditional_tokens: EvmAddress::parse(CURRENT_CTF).expect("canonical CTF"),
        collateral_token: EvmAddress::parse(CURRENT_PUSD).expect("canonical pUSD"),
        usdce: EvmAddress::parse(CURRENT_USDCE).expect("canonical USDC.e"),
        call_target: EvmAddress::parse(CURRENT_CTF).expect("canonical approval call target"),
        deployment_digest: action
            .deployment_digest
            .expect("governed deployment digest"),
        deployment_evidence_version: action
            .deployment_evidence_version
            .clone()
            .expect("governed evidence version"),
        verified_block_number: action
            .verified_block_number
            .expect("governed verified block")
            + 1,
        verified_block_hash: block_hash(identity_byte.wrapping_add(1)),
        prepared_block_number: Some(90_685_100),
        prepared_block_hash: Some(block_hash(identity_byte.wrapping_add(2))),
        calldata_hash: calldata_hash(identity_byte),
        calldata: vec![0xa2, identity_byte],
        signed_envelope: Some(signed_envelope),
        signed_envelope_hash: Some(signed_envelope_hash),
        prepared_nonce: Some(EvmUint256::parse("19").expect("governed nonce")),
        gas_limit: Some(EvmUint256::parse("100000").expect("governed gas limit")),
        relayer_transaction_id: None,
        transaction_hash: Some(transaction_hash(identity_byte)),
        failure_code: None,
        failure_history_json: SettlementFailureHistory::default(),
        receipt_evidence_json: None,
        attempt_ordinal: 1,
        last_error: None,
        dispatched_at: None,
        chain_hash_observed_at: None,
        confirmed_at: None,
    }
}

fn current_eoa_submission(
    settlement_redeem_id: SettlementRedeemId,
    identity_byte: u8,
) -> NewSettlementChainSubmission {
    let signed_envelope = vec![0x02, identity_byte];
    let signed_envelope_hash = ContentHash::from_bytes(*blake3::hash(&signed_envelope).as_bytes());
    NewSettlementChainSubmission {
        settlement_chain_submission_id: SettlementChainSubmissionId::from_v7(),
        settlement_redeem_id: Some(settlement_redeem_id),
        settlement_governed_action_id: None,
        canary_action_id: None,
        purpose: SettlementSubmissionPurpose::Redeem,
        kind: SettlementSubmissionKind::DirectEoa,
        state: SettlementSubmissionState::Prepared,
        route: SettlementRoute::StandardV2,
        target_adapter: EvmAddress::parse(CURRENT_STANDARD).expect("canonical current adapter"),
        target_code_hash: code_hash(CURRENT_STANDARD_CODE_HASH),
        conditional_tokens: EvmAddress::parse(CURRENT_CTF).expect("canonical CTF"),
        collateral_token: EvmAddress::parse(CURRENT_PUSD).expect("canonical pUSD"),
        usdce: EvmAddress::parse(CURRENT_USDCE).expect("canonical USDC.e"),
        call_target: EvmAddress::parse(CURRENT_STANDARD).expect("canonical redeem target"),
        deployment_digest: ContentHash::from_bytes([identity_byte; 32]),
        deployment_evidence_version: evidence_version(),
        verified_block_number: 90_685_098,
        verified_block_hash: block_hash(0x51),
        prepared_block_number: Some(90_685_100),
        prepared_block_hash: Some(block_hash(0x52)),
        calldata_hash: calldata_hash(identity_byte),
        calldata: vec![0xde, 0xad, 0xbe, identity_byte],
        signed_envelope: Some(signed_envelope),
        signed_envelope_hash: Some(signed_envelope_hash),
        prepared_nonce: Some(EvmUint256::parse("17").expect("nonce")),
        gas_limit: Some(EvmUint256::parse("120000").expect("gas limit")),
        relayer_transaction_id: None,
        transaction_hash: Some(transaction_hash(identity_byte)),
        failure_code: None,
        failure_history_json: SettlementFailureHistory::default(),
        receipt_evidence_json: None,
        attempt_ordinal: 1,
        last_error: None,
        dispatched_at: None,
        chain_hash_observed_at: None,
        confirmed_at: None,
    }
}

fn external_submission(
    settlement_redeem_id: SettlementRedeemId,
    state: SettlementSubmissionState,
    transaction_hash: Option<EvmTransactionHash>,
) -> NewSettlementChainSubmission {
    NewSettlementChainSubmission {
        settlement_chain_submission_id: SettlementChainSubmissionId::from_v7(),
        settlement_redeem_id: Some(settlement_redeem_id),
        settlement_governed_action_id: None,
        canary_action_id: None,
        purpose: SettlementSubmissionPurpose::Redeem,
        kind: SettlementSubmissionKind::ExternallyObserved,
        state,
        route: SettlementRoute::StandardV2,
        target_adapter: EvmAddress::parse(CURRENT_STANDARD).expect("canonical current adapter"),
        target_code_hash: code_hash(CURRENT_STANDARD_CODE_HASH),
        conditional_tokens: EvmAddress::parse(CURRENT_CTF).expect("canonical CTF"),
        collateral_token: EvmAddress::parse(CURRENT_PUSD).expect("canonical pUSD"),
        usdce: EvmAddress::parse(CURRENT_USDCE).expect("canonical USDC.e"),
        call_target: EvmAddress::parse(CURRENT_STANDARD).expect("canonical current redeem target"),
        deployment_digest: ContentHash::from_bytes([0x61; 32]),
        deployment_evidence_version: evidence_version(),
        verified_block_number: 90_685_098,
        verified_block_hash: block_hash(0x62),
        prepared_block_number: None,
        prepared_block_hash: None,
        calldata_hash: calldata_hash(0x63),
        calldata: vec![0xde, 0xad, 0x63],
        signed_envelope: None,
        signed_envelope_hash: None,
        prepared_nonce: None,
        gas_limit: None,
        relayer_transaction_id: None,
        transaction_hash,
        failure_code: None,
        failure_history_json: SettlementFailureHistory::default(),
        receipt_evidence_json: None,
        attempt_ordinal: 1,
        last_error: None,
        dispatched_at: None,
        chain_hash_observed_at: None,
        confirmed_at: None,
    }
}

fn payout_vector() -> SettlementPayoutVector {
    SettlementPayoutVector {
        denominator: EvmUint256::parse("1").expect("unit denominator"),
        yes: EvmUint256::parse("1").expect("winning numerator"),
        no: EvmUint256::parse("0").expect("losing numerator"),
    }
}

fn receipt_evidence(
    submission: &SettlementChainSubmissionInfo,
    observed_at: DateTime<Utc>,
    raw_payout: &str,
    payout_usd: Usd,
) -> SettlementReceiptEvidence {
    SettlementReceiptEvidence {
        chain_id: 137,
        transaction_hash: submission
            .transaction_hash
            .clone()
            .expect("submission transaction hash"),
        block_number: 90_685_200,
        block_hash: block_hash(0x76),
        finalized_block_number: 90_685_201,
        finalized_block_hash: block_hash(0x77),
        call: SettlementMinedCallEvidence {
            wallet_kind: ExecutionWalletKind::Eoa,
            outer_sender: address(0x75),
            outer_target: submission.call_target.clone(),
            outer_calldata_hash: submission.calldata_hash.clone(),
            inner_target: submission.call_target.clone(),
            inner_calldata_hash: submission.calldata_hash.clone(),
        },
        receipt_success: true,
        pusd_mint: SettlementPusdMintEvidence {
            token: EvmAddress::parse("0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb")
                .expect("pUSD address"),
            from: EvmAddress::parse("0x0000000000000000000000000000000000000000")
                .expect("zero address"),
            to: address(0x75),
            raw_amount: EvmUint256::parse(raw_payout).expect("payout raw amount"),
            amount_usd: payout_usd,
            log_index: 3,
        },
        wrapped_payout: SettlementWrappedPayoutEvidence {
            collateral_token: EvmAddress::parse("0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb")
                .expect("pUSD address"),
            caller: submission.target_adapter.clone(),
            asset: EvmAddress::parse("0x2791bca1f2de4661ed88a30c99a7a9449aa84174")
                .expect("USDC.e address"),
            to: address(0x75),
            raw_amount: EvmUint256::parse(raw_payout).expect("payout raw amount"),
            amount_usd: payout_usd,
            log_index: 4,
        },
        canonical_checked_at: observed_at,
        observed_at,
    }
}

fn evidence_version() -> SettlementEvidenceVersion {
    SettlementEvidenceVersion::parse("polymarket-v2-2026-07-22.1")
        .expect("canonical evidence version")
}

fn address(byte: u8) -> EvmAddress {
    EvmAddress::parse(format!("0x{}", hex::encode([byte; 20]))).expect("canonical address")
}

fn block_hash(byte: u8) -> EvmBlockHash {
    EvmBlockHash::parse(format!("0x{}", hex::encode([byte; 32]))).expect("canonical block hash")
}

fn calldata_hash(byte: u8) -> EvmCalldataHash {
    EvmCalldataHash::parse(format!("0x{}", hex::encode([byte; 32])))
        .expect("canonical calldata hash")
}

fn code_hash(value: &str) -> EvmCodeHash {
    EvmCodeHash::parse(value).expect("canonical runtime code hash")
}

fn transaction_hash(byte: u8) -> EvmTransactionHash {
    EvmTransactionHash::parse(format!("0x{}", hex::encode([byte; 32])))
        .expect("canonical transaction hash")
}
