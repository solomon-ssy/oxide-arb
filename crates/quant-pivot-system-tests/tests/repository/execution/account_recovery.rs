//! Account-chain association and incident idempotency on real `PostgreSQL`.

use chrono::{DateTime, Utc};
use quant_pivot_core::execution::account_recovery_reconciler::AccountRecoveryReconciler;
use quant_pivot_models::{
    domain::quant::{
        AccountPauseConfirmation, AccountPauseDispatch, AccountRecoveryAssessmentInput,
        AccountRecoveryExecutionDelta, AccountRecoveryIncidentInfo, AccountRecoveryManifestDraft,
        AccountRecoveryManifestInfo, AccountRecoveryTokenBalance, FinalizeAccountRecoveryIncident,
        NewAccountChainExecution, NewAccountPauseOperation, NewExecutionAccount,
        NewStrategyPositionLot, SealAccountRecoveryIncident,
    },
    entities::{
        quant_account_clean_funder_blocker::Entity as CleanFunderBlockerEntity,
        quant_execution_order::Entity as ExecutionOrderEntity,
    },
    enums::{
        common::{MarketCategory, Side},
        execution::{
            AccountChainExecutionRole, AccountExecutionAssociationKind, AccountPauseOperationKind,
            AccountPauseOperationState, AccountRecoveryIncidentStatus, PositionLedgerState,
            StrategyPositionOriginKind, VenueOrderStatus,
        },
        quant::{AccountSource, OutcomeSide},
        settlement::SettlementSubmissionKind,
    },
    types::{
        AccountChainExecutionId, AccountPauseOperationId, AccountRecoveryIncidentId, ContentHash,
        EventId, EvmAddress, EvmBlockHash, EvmCalldataHash, EvmTransactionHash, EvmUint256,
        OrderId, Price, Shares, StrategyPositionLotId, TokenId, Usd, UserId,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgAccountChainExecutionRepository, PgAccountPauseOperationRepository,
        PgAccountRecoveryRepository, PgExecutionAccountRepository, PgStrategyPositionLotRepository,
    },
    traits::{
        AccountChainExecutionRepository, AccountPauseOperationRepository,
        AccountRecoveryRepository, ExecutionAccountRepository, StrategyPositionLotRepository,
    },
};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::execution_pg_seed::{
        ExecutionTxnIds, fixture_execution_account, new_execution_order, seed_approved_intent,
        seed_report_fixture,
    },
};
use rust_decimal_macros::dec;
use sea_orm::{DatabaseConnection, EntityTrait, IntoActiveModel};

const fn hash(byte: u8) -> ContentHash {
    ContentHash::from_bytes([byte; 32])
}

pub async fn unknown_execution_is_idempotent() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let account = fixture_execution_account();
    PgExecutionAccountRepository::new(db.clone())
        .ensure(account.clone())
        .await
        .expect("execution account");
    let source_event_hash = hash(1);
    let execution_id = AccountChainExecutionId::from_content_hash(&source_event_hash);
    let observed_at = Utc::now();
    PgAccountChainExecutionRepository::new(db.clone())
        .append(vec![NewAccountChainExecution {
            account_chain_execution_id: execution_id,
            execution_account_id: account.execution_account_id,
            role: AccountChainExecutionRole::Taker,
            chain_id: 137,
            protocol_version: 2,
            exchange_address: EvmAddress::parse("0xe111180000d2663c0091e4f400237545b87b996b")
                .expect("exchange"),
            block_number: 100,
            block_hash: EvmBlockHash::parse(format!("0x{}", "2".repeat(64))).expect("block hash"),
            transaction_hash: EvmTransactionHash::parse(format!("0x{}", "3".repeat(64)))
                .expect("transaction hash"),
            transaction_index: 1,
            log_index: 2,
            order_id: OrderId::new(format!("0x{}", "4".repeat(64))),
            maker_address: account.funder_address.clone(),
            taker_address: EvmAddress::parse("0x2222222222222222222222222222222222222222")
                .expect("taker"),
            order_side: Side::Buy,
            order_token_id: TokenId::new("12345"),
            maker_amount_raw: "600000".to_owned(),
            taker_amount_raw: "1000000".to_owned(),
            account_side: Some(Side::Buy),
            account_token_id: Some(TokenId::new("12345")),
            shares: Some(Shares::new(dec!(1))),
            principal_usd: Some(Usd::new(dec!(0.6))),
            exact_fee_usd: Some(Usd::new(dec!(0.01))),
            builder_code: None,
            metadata: None,
            source_event_hash,
            availability_policy_hash: hash(5),
            observed_at,
            available_at: observed_at,
        }])
        .await
        .expect("append account execution");

    let recovery = PgAccountRecoveryRepository::new(db.clone());
    let first = recovery
        .associate_execution(&execution_id, observed_at)
        .await
        .expect("first association");
    let replay = recovery
        .associate_execution(&execution_id, observed_at)
        .await
        .expect("association replay");
    assert!(first.incident_created);
    assert!(!replay.incident_created);
    assert_eq!(first.association, replay.association);
    assert_eq!(first.incident, replay.incident);

    let ids = seed_report_fixture(&db).await;
    let incident_id = first
        .incident
        .as_ref()
        .expect("unknown execution incident")
        .account_recovery_incident_id;
    verify_recovery_lot(&db, &account, &ids, incident_id, observed_at).await;
    let pause_repository = confirm_pause(&db, incident_id, observed_at).await;

    let (manifest_draft, manifest, reconciling) = append_recovery_manifest(
        &db,
        &account,
        &ids,
        &recovery,
        execution_id,
        incident_id,
        observed_at,
    )
    .await;
    let actor = UserId::from_v7();
    let sealed = recovery
        .seal_incident(SealAccountRecoveryIncident {
            recovery_incident_id: incident_id,
            account_recovery_manifest_id: manifest.account_recovery_manifest_id,
            expected_revision: reconciling.revision,
            actor,
            sealed_at: Utc::now(),
        })
        .await
        .expect("freeze recovery seal");
    assert_eq!(sealed.status, AccountRecoveryIncidentStatus::Reconciling);
    assert_eq!(sealed.seal_hash, Some(manifest.evidence_hash));
    assert!(
        recovery
            .finalize_incident(FinalizeAccountRecoveryIncident {
                recovery_incident_id: incident_id,
                expected_revision: sealed.revision,
                finalized_at: Utc::now(),
            })
            .await
            .is_err(),
        "incident must not finalize before unpause evidence",
    );
    finalize_recovery(
        &recovery,
        &pause_repository,
        incident_id,
        &sealed,
        &manifest_draft,
    )
    .await;

    verify_system_association(&db, &account, &ids, &recovery, observed_at).await;
    verify_clean_funder(&db, &account, &ids, &recovery, observed_at).await;
}

async fn append_recovery_manifest(
    db: &DatabaseConnection,
    account: &NewExecutionAccount,
    ids: &ExecutionTxnIds,
    recovery: &PgAccountRecoveryRepository,
    execution_id: AccountChainExecutionId,
    incident_id: AccountRecoveryIncidentId,
    observed_at: DateTime<Utc>,
) -> (
    AccountRecoveryManifestDraft,
    AccountRecoveryManifestInfo,
    AccountRecoveryIncidentInfo,
) {
    let recovery_executions = recovery
        .incident_executions(&incident_id)
        .await
        .expect("incident executions");
    assert_eq!(recovery_executions.len(), 1);
    let balance = AccountRecoveryTokenBalance {
        token_id: TokenId::new("12345"),
        shares: Shares::new(dec!(1)),
    };
    let input = AccountRecoveryAssessmentInput {
        recovery_incident_id: incident_id,
        execution_account_id: account.execution_account_id,
        observed_at,
        finalized_block_number: 101,
        finalized_block_hash: EvmBlockHash::parse(format!("0x{}", "f".repeat(64)))
            .expect("manifest block"),
        clob_snapshot_hash: hash(21),
        data_api_snapshot_hash: hash(22),
        chain_snapshot_hash: hash(23),
        settlement_snapshot_hash: hash(24),
        pause_confirmed: true,
        venue_snapshot_stable: true,
        clob_collateral_usd: Usd::new(dec!(100)),
        chain_collateral_usd: Usd::new(dec!(100)),
        reserved_usd: Usd::ZERO,
        open_order_ids: Vec::new(),
        unmapped_token_ids: Vec::new(),
        invalid_execution_ids: Vec::new(),
        clean_funder_blocker: None,
        data_api_positions: vec![balance.clone()],
        chain_positions: vec![balance],
        open_lots: Vec::new(),
        incident_executions: vec![AccountRecoveryExecutionDelta {
            account_chain_execution_id: execution_id,
            token_id: TokenId::new("12345"),
            shares_delta: dec!(1),
            principal_usd: Usd::new(dec!(0.6)),
            exact_fee_usd: Usd::new(dec!(0.01)),
            available_at: observed_at,
        }],
        explicit_sell_allocations: Vec::new(),
        pending_settlement_count: 0,
    };
    let assessment = AccountRecoveryReconciler::assess(input.clone()).expect("assessment");
    assert!(assessment.converged());
    let created = assessment.created_lots[0].clone();
    let materialized_lot = NewStrategyPositionLot {
        strategy_position_lot_id: created.strategy_position_lot_id,
        origin_kind: StrategyPositionOriginKind::AccountRecoveryIncident,
        order_intent_id: None,
        recovery_incident_id: Some(incident_id),
        execution_account_id: account.execution_account_id,
        token_id: created.token_id.clone(),
        market_id: ids.market.as_str().into(),
        event_id: Some(EventId::new(&ids.event)),
        category: MarketCategory::Weather,
        side: OutcomeSide::Yes,
        state: PositionLedgerState::Open,
        shares: created.remaining_shares,
        avg_price: Price::new(created.acquired_cost_usd.inner() / created.acquired_shares.inner()),
        cost_usd: created.remaining_cost_usd,
        realized_pnl_usd: created.realized_pnl_delta_usd,
        source: AccountSource::Polymarket,
        opened_at: observed_at,
        closed_at: None,
    };
    let draft = AccountRecoveryManifestDraft {
        recovery_incident_id: incident_id,
        input,
        assessment,
        created_lots: vec![materialized_lot],
    };
    let manifest = recovery
        .append_manifest(draft.clone())
        .await
        .expect("append recovery manifest");
    let replay = recovery
        .append_manifest(draft.clone())
        .await
        .expect("replay recovery manifest");
    assert_eq!(manifest, replay);
    assert_eq!(manifest.attempt_no, 1);
    assert!(manifest.converged);
    let materialized = PgStrategyPositionLotRepository::new(db.clone())
        .find_by_id(&created.strategy_position_lot_id)
        .await
        .expect("materialized recovery lot")
        .expect("recovery lot exists");
    assert_eq!(materialized.position.shares, Shares::new(dec!(1)));
    assert_eq!(materialized.position.cost_usd, Usd::new(dec!(0.61)));
    assert_eq!(
        recovery
            .latest_manifest(&incident_id)
            .await
            .expect("latest manifest"),
        Some(manifest.clone()),
    );
    let reconciling = recovery
        .active_incident(&account.execution_account_id)
        .await
        .expect("active incident before seal")
        .expect("reconciling incident");
    assert_eq!(
        reconciling.status,
        AccountRecoveryIncidentStatus::Reconciling
    );
    (draft, manifest, reconciling)
}

async fn verify_recovery_lot(
    db: &DatabaseConnection,
    account: &NewExecutionAccount,
    ids: &ExecutionTxnIds,
    incident_id: AccountRecoveryIncidentId,
    observed_at: DateTime<Utc>,
) {
    let recovery_lot = PgStrategyPositionLotRepository::new(db.clone())
        .create_recovery_lot(NewStrategyPositionLot {
            strategy_position_lot_id: StrategyPositionLotId::from_v7(),
            origin_kind: StrategyPositionOriginKind::AccountRecoveryIncident,
            order_intent_id: None,
            recovery_incident_id: Some(incident_id),
            execution_account_id: account.execution_account_id,
            token_id: TokenId::new(&ids.token),
            market_id: ids.market.as_str().into(),
            event_id: Some(EventId::new(&ids.event)),
            category: MarketCategory::Weather,
            side: OutcomeSide::Yes,
            state: PositionLedgerState::Open,
            shares: Shares::new(dec!(1)),
            avg_price: Price::new(dec!(0.6)),
            cost_usd: Usd::new(dec!(0.6)),
            realized_pnl_usd: Usd::ZERO,
            source: AccountSource::Polymarket,
            opened_at: observed_at,
            closed_at: None,
        })
        .await
        .expect("recovery position lot");
    assert!(recovery_lot.order_intent_id.is_none());
    assert_eq!(recovery_lot.recovery_incident_id, Some(incident_id));
}

async fn confirm_pause(
    db: &DatabaseConnection,
    incident_id: AccountRecoveryIncidentId,
    observed_at: DateTime<Utc>,
) -> PgAccountPauseOperationRepository {
    let pause_hash = hash(11);
    let pause_id = AccountPauseOperationId::from_content_hash(&pause_hash);
    let repository = PgAccountPauseOperationRepository::new(db.clone());
    let pause = NewAccountPauseOperation {
        account_pause_operation_id: pause_id,
        recovery_incident_id: incident_id,
        exchange_address: EvmAddress::parse("0xe111180000d2663c0091e4f400237545b87b996b")
            .expect("pause exchange"),
        operation_kind: AccountPauseOperationKind::Pause,
        state: AccountPauseOperationState::Prepared,
        submission_kind: SettlementSubmissionKind::DirectEoa,
        requested_block: 100,
        interval_blocks: Some(237),
        effective_block: Some(337),
        prepared_block_number: 100,
        prepared_block_hash: EvmBlockHash::parse(format!("0x{}", "c".repeat(64)))
            .expect("prepared block"),
        prepared_nonce: EvmUint256::parse("1").expect("nonce"),
        gas_limit: Some(EvmUint256::parse("100000").expect("gas limit")),
        calldata_hash: EvmCalldataHash::parse(format!("0x{}", "d".repeat(64)))
            .expect("calldata hash"),
        deployment_digest: hash(12),
        signed_envelope: vec![1, 2, 3],
        signed_envelope_hash: pause_hash,
        transaction_hash: Some(
            EvmTransactionHash::parse(format!("0x{}", "e".repeat(64))).expect("pause transaction"),
        ),
    };
    let prepared = repository
        .insert_prepared(pause.clone())
        .await
        .expect("prepared pause");
    let replayed = repository
        .insert_prepared(pause)
        .await
        .expect("replayed pause");
    assert_eq!(prepared, replayed);
    let dispatched = repository
        .record_dispatch(&pause_id, AccountPauseDispatch::EoaAccepted, observed_at)
        .await
        .expect("dispatched pause");
    assert_eq!(dispatched.state, AccountPauseOperationState::Dispatched);
    let confirmed = repository
        .confirm(
            &pause_id,
            AccountPauseConfirmation {
                block_number: 101,
                block_hash: EvmBlockHash::parse(format!("0x{}", "f".repeat(64)))
                    .expect("confirmation block"),
                transaction_hash: EvmTransactionHash::parse(format!("0x{}", "a".repeat(64)))
                    .expect("confirmation transaction"),
                log_index: 1,
                confirmed_at: observed_at,
            },
        )
        .await
        .expect("confirmed pause");
    assert_eq!(confirmed.state, AccountPauseOperationState::Confirmed);
    repository
}

async fn finalize_recovery(
    recovery: &PgAccountRecoveryRepository,
    pause_repository: &PgAccountPauseOperationRepository,
    incident_id: AccountRecoveryIncidentId,
    sealed: &AccountRecoveryIncidentInfo,
    manifest_draft: &AccountRecoveryManifestDraft,
) {
    let unpause_hash = hash(31);
    let unpause_id = AccountPauseOperationId::from_content_hash(&unpause_hash);
    let unpause = pause_repository
        .insert_prepared(NewAccountPauseOperation {
            account_pause_operation_id: unpause_id,
            recovery_incident_id: incident_id,
            exchange_address: EvmAddress::parse("0xe111180000d2663c0091e4f400237545b87b996b")
                .expect("unpause exchange"),
            operation_kind: AccountPauseOperationKind::Unpause,
            state: AccountPauseOperationState::Prepared,
            submission_kind: SettlementSubmissionKind::DirectEoa,
            requested_block: 400,
            interval_blocks: None,
            effective_block: None,
            prepared_block_number: 400,
            prepared_block_hash: EvmBlockHash::parse(format!("0x{}", "1".repeat(64)))
                .expect("unpause prepared block"),
            prepared_nonce: EvmUint256::parse("2").expect("unpause nonce"),
            gas_limit: Some(EvmUint256::parse("100000").expect("unpause gas limit")),
            calldata_hash: EvmCalldataHash::parse(format!("0x{}", "2".repeat(64)))
                .expect("unpause calldata hash"),
            deployment_digest: hash(32),
            signed_envelope: vec![4, 5, 6],
            signed_envelope_hash: unpause_hash,
            transaction_hash: Some(
                EvmTransactionHash::parse(format!("0x{}", "3".repeat(64)))
                    .expect("unpause transaction"),
            ),
        })
        .await
        .expect("prepared unpause");
    assert_eq!(unpause.operation_kind, AccountPauseOperationKind::Unpause);
    pause_repository
        .record_dispatch(&unpause_id, AccountPauseDispatch::EoaAccepted, Utc::now())
        .await
        .expect("dispatched unpause");
    pause_repository
        .confirm(
            &unpause_id,
            AccountPauseConfirmation {
                block_number: 401,
                block_hash: EvmBlockHash::parse(format!("0x{}", "4".repeat(64)))
                    .expect("unpause confirmation block"),
                transaction_hash: EvmTransactionHash::parse(format!("0x{}", "5".repeat(64)))
                    .expect("unpause confirmation transaction"),
                log_index: 2,
                confirmed_at: Utc::now(),
            },
        )
        .await
        .expect("confirmed unpause");
    let terminal = recovery
        .finalize_incident(FinalizeAccountRecoveryIncident {
            recovery_incident_id: incident_id,
            expected_revision: sealed.revision,
            finalized_at: Utc::now(),
        })
        .await
        .expect("terminal recovery seal");
    assert_eq!(terminal.status, AccountRecoveryIncidentStatus::Sealed);
    assert!(
        recovery
            .append_manifest(manifest_draft.clone())
            .await
            .is_err(),
        "sealed incident must reject additional manifests",
    );
}

async fn verify_system_association(
    db: &DatabaseConnection,
    account: &NewExecutionAccount,
    ids: &ExecutionTxnIds,
    recovery: &PgAccountRecoveryRepository,
    observed_at: DateTime<Utc>,
) {
    let intent_id = seed_approved_intent(db, ids).await;
    let mut order = new_execution_order(&intent_id, ids);
    let system_order_id = OrderId::new(format!("0x{}", "6".repeat(64)));
    order.venue_order_id = Some(system_order_id.clone());
    order.venue_status = Some(VenueOrderStatus::Open);
    order.submitted_at = Some(observed_at);
    let execution_order_id = order.execution_order_id;
    ExecutionOrderEntity::insert(order.into_active_model())
        .exec(db)
        .await
        .expect("system execution order");

    let system_hash = hash(9);
    let system_execution_id = AccountChainExecutionId::from_content_hash(&system_hash);
    PgAccountChainExecutionRepository::new(db.clone())
        .append(vec![NewAccountChainExecution {
            account_chain_execution_id: system_execution_id,
            execution_account_id: account.execution_account_id,
            role: AccountChainExecutionRole::Taker,
            chain_id: 137,
            protocol_version: 2,
            exchange_address: EvmAddress::parse("0xe111180000d2663c0091e4f400237545b87b996b")
                .expect("exchange"),
            block_number: 101,
            block_hash: EvmBlockHash::parse(format!("0x{}", "7".repeat(64))).expect("block hash"),
            transaction_hash: EvmTransactionHash::parse(format!("0x{}", "8".repeat(64)))
                .expect("transaction hash"),
            transaction_index: 1,
            log_index: 3,
            order_id: system_order_id,
            maker_address: account.funder_address.clone(),
            taker_address: EvmAddress::parse("0x2222222222222222222222222222222222222222")
                .expect("taker"),
            order_side: Side::Buy,
            order_token_id: TokenId::new(&ids.token),
            maker_amount_raw: "600000".to_owned(),
            taker_amount_raw: "1000000".to_owned(),
            account_side: Some(Side::Buy),
            account_token_id: Some(TokenId::new(&ids.token)),
            shares: Some(Shares::new(dec!(1))),
            principal_usd: Some(Usd::new(dec!(0.6))),
            exact_fee_usd: Some(Usd::new(dec!(0.01))),
            builder_code: None,
            metadata: None,
            source_event_hash: system_hash,
            availability_policy_hash: hash(10),
            observed_at,
            available_at: observed_at,
        }])
        .await
        .expect("append system execution");
    let system = recovery
        .associate_execution(&system_execution_id, observed_at)
        .await
        .expect("system association");
    assert_eq!(
        system.association.kind,
        AccountExecutionAssociationKind::SystemOrder
    );
    assert_eq!(
        system.association.execution_order_id,
        Some(execution_order_id)
    );
    assert!(system.incident.is_none());
}

async fn verify_clean_funder(
    db: &DatabaseConnection,
    account: &NewExecutionAccount,
    ids: &ExecutionTxnIds,
    recovery: &PgAccountRecoveryRepository,
    observed_at: DateTime<Utc>,
) {
    let maker_hash = hash(41);
    let maker_execution_id = AccountChainExecutionId::from_content_hash(&maker_hash);
    PgAccountChainExecutionRepository::new(db.clone())
        .append(vec![NewAccountChainExecution {
            account_chain_execution_id: maker_execution_id,
            execution_account_id: account.execution_account_id,
            role: AccountChainExecutionRole::Maker,
            chain_id: 137,
            protocol_version: 2,
            exchange_address: EvmAddress::parse("0xe111180000d2663c0091e4f400237545b87b996b")
                .expect("maker exchange"),
            block_number: 102,
            block_hash: EvmBlockHash::parse(format!("0x{}", "9".repeat(64)))
                .expect("maker block hash"),
            transaction_hash: EvmTransactionHash::parse(format!("0x{}", "a".repeat(64)))
                .expect("maker transaction hash"),
            transaction_index: 2,
            log_index: 1,
            order_id: OrderId::new(format!("0x{}", "b".repeat(64))),
            maker_address: account.funder_address.clone(),
            taker_address: EvmAddress::parse("0x3333333333333333333333333333333333333333")
                .expect("maker taker"),
            order_side: Side::Sell,
            order_token_id: TokenId::new(&ids.token),
            maker_amount_raw: "1000000".to_owned(),
            taker_amount_raw: "600000".to_owned(),
            account_side: Some(Side::Sell),
            account_token_id: Some(TokenId::new(&ids.token)),
            shares: Some(Shares::new(dec!(1))),
            principal_usd: Some(Usd::new(dec!(0.6))),
            exact_fee_usd: Some(Usd::new(dec!(0.01))),
            builder_code: None,
            metadata: None,
            source_event_hash: maker_hash,
            availability_policy_hash: hash(42),
            observed_at,
            available_at: observed_at,
        }])
        .await
        .expect("append external maker execution");
    let maker = recovery
        .associate_execution(&maker_execution_id, observed_at)
        .await
        .expect("associate external maker execution");
    let maker_incident_id = maker
        .incident
        .expect("maker recovery incident")
        .account_recovery_incident_id;
    let blocker = recovery
        .clean_funder_blocker(&maker_incident_id)
        .await
        .expect("read clean-funder blocker")
        .expect("maker execution must latch clean-funder blocker");
    assert_eq!(blocker.role, AccountChainExecutionRole::Maker);
    assert!(
        CleanFunderBlockerEntity::delete_by_id(maker_incident_id)
            .exec(db)
            .await
            .is_err(),
        "clean-funder blocker must be append-only and cannot be acknowledged away",
    );
}
