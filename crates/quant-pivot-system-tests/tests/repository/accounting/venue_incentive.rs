//! Venue-incentive append-only ledger persistence contracts.

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::NewVenueIncentiveEvent,
    enums::fee::{VenueIncentiveKind, VenueIncentiveStage},
    types::{
        ContentHash, EvmTransactionHash, ExecutionAccountId, MarketId, Usd, VenueIncentiveEventId,
    },
};
use quant_pivot_repository::{
    postgres::PgVenueIncentiveRepository, traits::VenueIncentiveRepository,
};
use quant_pivot_system_tests::{
    postgres::setup_pg, support::execution_pg_seed::ensure_fixture_execution_account,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

pub async fn revisions_are_pit_cumulative() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let account_id = ensure_fixture_execution_account(&db).await;
    let repository = PgVenueIncentiveRepository::new(db);
    let observed_at = Utc::now() - Duration::hours(2);
    let first_available_at = observed_at + Duration::minutes(10);
    let revised_available_at = observed_at + Duration::minutes(20);

    let first = MakerAwardFixture {
        account_id,
        market_id: "market-a",
        partition: "maker:market-a:day",
        identity: "maker:market-a:day:v1",
        amount: dec!(1.25),
        observed_at,
        available_at: first_available_at,
        hash_seed: 'a',
    }
    .event();
    repository
        .record(vec![first.clone(), first])
        .await
        .expect("exact incentive retry is idempotent");
    repository
        .record(vec![
            MakerAwardFixture {
                account_id,
                market_id: "market-a",
                partition: "maker:market-a:day",
                identity: "maker:market-a:day:v2",
                amount: dec!(1.50),
                observed_at,
                available_at: revised_available_at,
                hash_seed: 'b',
            }
            .event(),
        ])
        .await
        .expect("append revised venue award");
    repository
        .record(vec![
            MakerAwardFixture {
                account_id,
                market_id: "market-b",
                partition: "maker:market-b:day",
                identity: "maker:market-b:day:v1",
                amount: dec!(0.75),
                observed_at,
                available_at: first_available_at,
                hash_seed: 'c',
            }
            .event(),
        ])
        .await
        .expect("append independent venue award");

    let before_revision = repository
        .reconciliation_cumulative(&account_id, first_available_at)
        .await
        .expect("read pre-revision reconciliation");
    assert_eq!(
        before_revision.venue_awarded_maker_usd,
        Usd::new(dec!(2.00))
    );

    let after_revision = repository
        .reconciliation_cumulative(&account_id, revised_available_at)
        .await
        .expect("read revised reconciliation");
    assert_eq!(after_revision.venue_awarded_maker_usd, Usd::new(dec!(2.25)));
    assert_eq!(after_revision.wallet_credit_total(), Usd::ZERO);
}

pub async fn wallet_credit_is_cash() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let account_id = ensure_fixture_execution_account(&db).await;
    let repository = PgVenueIncentiveRepository::new(db);
    let observed_at = Utc::now() - Duration::minutes(30);
    let available_at = observed_at + Duration::minutes(5);

    repository
        .record(vec![
            wallet_credit(
                account_id,
                VenueIncentiveKind::MakerRebate,
                "credit:maker:tx",
                dec!(1.10),
                observed_at,
                available_at,
                'd',
            ),
            wallet_credit(
                account_id,
                VenueIncentiveKind::TakerRebate,
                "credit:taker:tx",
                dec!(0.40),
                observed_at,
                available_at,
                'e',
            ),
        ])
        .await
        .expect("persist wallet credits");

    assert_eq!(
        repository
            .credited_cumulative(&account_id, available_at)
            .await
            .expect("read credited cash"),
        Usd::new(dec!(1.50))
    );
    let reconciliation = repository
        .reconciliation_cumulative(&account_id, available_at)
        .await
        .expect("read incentive stages");
    assert_eq!(
        reconciliation.wallet_credited_maker_usd,
        Usd::new(dec!(1.10))
    );
    assert_eq!(
        reconciliation.wallet_credited_taker_usd,
        Usd::new(dec!(0.40))
    );
    assert_eq!(reconciliation.venue_awarded_maker_usd, Usd::ZERO);
}

pub async fn conflicting_identity_rolls_back() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let account_id = ensure_fixture_execution_account(&db).await;
    let repository = PgVenueIncentiveRepository::new(db);
    let observed_at = Utc::now() - Duration::hours(1);
    let available_at = observed_at + Duration::minutes(5);
    let identity = "credit:maker:conflict";
    let original = wallet_credit(
        account_id,
        VenueIncentiveKind::MakerRebate,
        identity,
        dec!(1.00),
        observed_at,
        available_at,
        'f',
    );
    repository
        .record(vec![original.clone()])
        .await
        .expect("persist original incentive event");

    let mut conflicting = original;
    conflicting.venue_incentive_event_id = VenueIncentiveEventId::from_v7();
    conflicting.amount_usd = Usd::new(dec!(9.00));
    let result = repository.record(vec![conflicting]).await;
    assert!(matches!(result, Err(StorageError::StateConflict { .. })));

    assert_eq!(
        repository
            .credited_cumulative(&account_id, available_at)
            .await
            .expect("read unchanged credited cash"),
        Usd::new(dec!(1.00))
    );
}

struct MakerAwardFixture<'a> {
    account_id: ExecutionAccountId,
    market_id: &'a str,
    partition: &'a str,
    identity: &'a str,
    amount: Decimal,
    observed_at: DateTime<Utc>,
    available_at: DateTime<Utc>,
    hash_seed: char,
}

impl MakerAwardFixture<'_> {
    fn event(&self) -> NewVenueIncentiveEvent {
        NewVenueIncentiveEvent {
            venue_incentive_event_id: VenueIncentiveEventId::from_v7(),
            execution_account_id: self.account_id,
            execution_fill_id: None,
            market_id: Some(MarketId::new(self.market_id)),
            kind: VenueIncentiveKind::MakerRebate,
            stage: VenueIncentiveStage::VenueAwarded,
            program_date: self.observed_at.date_naive(),
            amount_usd: Usd::new(self.amount),
            source_schedule_hash: None,
            source_partition: self.partition.to_owned(),
            source_identity: self.identity.to_owned(),
            transaction_hash: None,
            observed_at: self.observed_at,
            available_at: self.available_at,
            evidence_hash: content_hash(self.hash_seed),
        }
    }
}

fn wallet_credit(
    execution_account_id: ExecutionAccountId,
    kind: VenueIncentiveKind,
    source_identity: &str,
    amount: Decimal,
    observed_at: DateTime<Utc>,
    available_at: DateTime<Utc>,
    hash_seed: char,
) -> NewVenueIncentiveEvent {
    NewVenueIncentiveEvent {
        venue_incentive_event_id: VenueIncentiveEventId::from_v7(),
        execution_account_id,
        execution_fill_id: None,
        market_id: None,
        kind,
        stage: VenueIncentiveStage::WalletCredited,
        program_date: observed_at.date_naive(),
        amount_usd: Usd::new(amount),
        source_schedule_hash: None,
        source_partition: source_identity.to_owned(),
        source_identity: source_identity.to_owned(),
        transaction_hash: Some(
            EvmTransactionHash::parse(format!("0x{}", hash_seed.to_string().repeat(64)))
                .expect("valid transaction hash"),
        ),
        observed_at,
        available_at,
        evidence_hash: content_hash(hash_seed),
    }
}

fn content_hash(seed: char) -> ContentHash {
    ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64)))
        .expect("valid content hash")
}
