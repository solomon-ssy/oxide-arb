//! Account-chain association and incident idempotency on real PostgreSQL.

use chrono::Utc;
use quant_pivot_models::{
    domain::quant::{
        AccountPauseConfirmation, AccountPauseDispatch, NewAccountChainExecution,
        NewAccountPauseSubmission, NewStrategyPositionLot,
    },
    entities::quant_execution_order::Entity as ExecutionOrderEntity,
    enums::{
        common::{MarketCategory, Side},
        execution::{
            AccountChainExecutionRole, AccountExecutionAssociationKind,
            AccountPauseSubmissionState, PositionLedgerState, StrategyPositionOriginKind,
            VenueOrderStatus,
        },
        quant::{AccountSource, OutcomeSide},
        settlement::SettlementSubmissionKind,
    },
    types::{
        AccountChainExecutionId, AccountPauseSubmissionId, ContentHash, EventId, EvmAddress,
        EvmBlockHash, EvmCalldataHash, EvmTransactionHash, EvmUint256, OrderId, Price, Shares,
        StrategyPositionLotId, TokenId, Usd,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgAccountChainExecutionRepository, PgAccountPauseRepository, PgAccountRecoveryRepository,
        PgExecutionAccountRepository, PgStrategyPositionLotRepository,
    },
    traits::{
        AccountChainExecutionRepository, AccountPauseRepository, AccountRecoveryRepository,
        ExecutionAccountRepository, StrategyPositionLotRepository,
    },
};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::execution_pg_seed::{
        fixture_execution_account, new_execution_order, seed_approved_intent, seed_report_fixture,
    },
};
use rust_decimal_macros::dec;
use sea_orm::{EntityTrait, IntoActiveModel};

fn hash(byte: u8) -> ContentHash {
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
            role: AccountChainExecutionRole::Maker,
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

    let pause_hash = hash(11);
    let pause_id = AccountPauseSubmissionId::from_content_hash(&pause_hash);
    let pause_repository = PgAccountPauseRepository::new(db.clone());
    let pause = NewAccountPauseSubmission {
        account_pause_submission_id: pause_id,
        recovery_incident_id: incident_id,
        exchange_address: EvmAddress::parse("0xe111180000d2663c0091e4f400237545b87b996b")
            .expect("pause exchange"),
        state: AccountPauseSubmissionState::Prepared,
        kind: SettlementSubmissionKind::DirectEoa,
        requested_block: 100,
        interval_blocks: 237,
        effective_block: 337,
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
    let prepared = pause_repository
        .insert_prepared(pause.clone())
        .await
        .expect("prepared pause");
    let replayed = pause_repository
        .insert_prepared(pause)
        .await
        .expect("replayed pause");
    assert_eq!(prepared, replayed);
    let dispatched = pause_repository
        .record_dispatch(&pause_id, AccountPauseDispatch::EoaAccepted, observed_at)
        .await
        .expect("dispatched pause");
    assert_eq!(dispatched.state, AccountPauseSubmissionState::Dispatched);
    let confirmed = pause_repository
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
    assert_eq!(confirmed.state, AccountPauseSubmissionState::Confirmed);

    let intent_id = seed_approved_intent(&db, &ids).await;
    let mut order = new_execution_order(&intent_id, &ids);
    let system_order_id = OrderId::new(format!("0x{}", "6".repeat(64)));
    order.venue_order_id = Some(system_order_id.clone());
    order.venue_status = Some(VenueOrderStatus::Open);
    order.submitted_at = Some(observed_at);
    let execution_order_id = order.execution_order_id;
    ExecutionOrderEntity::insert(order.into_active_model())
        .exec(&db)
        .await
        .expect("system execution order");

    let system_hash = hash(9);
    let system_execution_id = AccountChainExecutionId::from_content_hash(&system_hash);
    PgAccountChainExecutionRepository::new(db)
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
            maker_address: account.funder_address,
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
