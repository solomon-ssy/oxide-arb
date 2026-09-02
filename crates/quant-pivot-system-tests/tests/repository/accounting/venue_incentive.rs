//! Venue-incentive append-only ledger persistence contracts.

use chrono::{DateTime, Duration, NaiveDate, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::venue_incentive::{
        NewVenueIncentiveEvent, NewVenueIncentiveReconciliationScan,
        NewVenueIncentiveReportedAccrualSnapshot,
    },
    enums::fee::{VenueIncentiveKind, VenueIncentiveReconciliationScanStatus, VenueIncentiveStage},
    types::{
        ContentHash, EvmTransactionHash, ExecutionAccountId, MarketId, Usd, VenueIncentiveEventId,
        ids::VenueIncentiveReconciliationScanId,
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

pub async fn award_snapshots_revise_retract() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let account_id = ensure_fixture_execution_account(&db).await;
    let repository = PgVenueIncentiveRepository::new(db);
    let observed_at = Utc::now() - Duration::hours(2);
    let first_available_at = observed_at + Duration::minutes(10);
    let revised_available_at = observed_at + Duration::minutes(20);

    let market_a_v1 = MakerAwardFixture {
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
    let market_b = MakerAwardFixture {
        account_id,
        market_id: "market-b",
        partition: "maker:market-b:day",
        identity: "maker:market-b:day:v1",
        amount: dec!(0.75),
        observed_at,
        available_at: first_available_at,
        hash_seed: 'c',
    }
    .event();
    let initial = award_snapshot(
        account_id,
        observed_at.date_naive(),
        first_available_at,
        vec![market_a_v1, market_b.clone()],
        '1',
    );
    let repeated_at = first_available_at + Duration::minutes(5);
    assert_snapshot_idempotency(
        &repository,
        account_id,
        observed_at.date_naive(),
        initial,
        first_available_at,
        repeated_at,
    )
    .await;

    repository
        .apply_reported_accrual_snapshot(award_snapshot(
            account_id,
            observed_at.date_naive(),
            revised_available_at,
            vec![
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
                market_b,
            ],
            '2',
        ))
        .await
        .expect("apply revised complete award snapshot");

    let after_revision = repository
        .reconciliation_cumulative(&account_id, revised_available_at)
        .await
        .expect("read revised reconciliation");
    assert_eq!(
        after_revision.venue_reported_maker_accrual_usd,
        Usd::new(dec!(2.25))
    );
    assert_eq!(after_revision.wallet_credit_total(), Usd::ZERO);

    let deletion_at = revised_available_at + Duration::minutes(10);
    repository
        .apply_reported_accrual_snapshot(award_snapshot(
            account_id,
            observed_at.date_naive(),
            deletion_at,
            vec![
                MakerAwardFixture {
                    account_id,
                    market_id: "market-a",
                    partition: "maker:market-a:day",
                    identity: "maker:market-a:day:v3",
                    amount: dec!(1.50),
                    observed_at,
                    available_at: deletion_at,
                    hash_seed: 'd',
                }
                .event(),
            ],
            '3',
        ))
        .await
        .expect("missing partition creates a retraction");
    assert_eq!(
        repository
            .reconciliation_cumulative(&account_id, deletion_at)
            .await
            .expect("read after deleted award partition")
            .venue_reported_maker_accrual_usd,
        Usd::new(dec!(1.50))
    );

    let empty_at = deletion_at + Duration::minutes(10);
    repository
        .apply_reported_accrual_snapshot(award_snapshot(
            account_id,
            observed_at.date_naive(),
            empty_at,
            Vec::new(),
            '4',
        ))
        .await
        .expect("empty complete snapshot retracts remaining partitions");
    assert_eq!(
        repository
            .reconciliation_cumulative(&account_id, empty_at)
            .await
            .expect("read after empty award snapshot")
            .venue_reported_maker_accrual_usd,
        Usd::ZERO
    );
}

async fn assert_snapshot_idempotency(
    repository: &PgVenueIncentiveRepository,
    account_id: ExecutionAccountId,
    program_date: NaiveDate,
    initial: NewVenueIncentiveReportedAccrualSnapshot,
    first_available_at: DateTime<Utc>,
    repeated_at: DateTime<Utc>,
) {
    repository
        .apply_reported_accrual_snapshot(initial.clone())
        .await
        .expect("apply initial complete award snapshot");
    let mut conflicting_retry = initial.clone();
    conflicting_retry.awards[0].amount_usd = Usd::new(dec!(9.99));
    assert!(matches!(
        repository
            .apply_reported_accrual_snapshot(conflicting_retry)
            .await,
        Err(StorageError::StateConflict { .. })
    ));
    repository
        .apply_reported_accrual_snapshot(initial.clone())
        .await
        .expect("exact complete snapshot retry is idempotent");

    let mut repeated = initial;
    repeated.scan.venue_incentive_reconciliation_scan_id =
        VenueIncentiveReconciliationScanId::from_v7();
    repeated.scan.started_at = repeated_at - Duration::seconds(1);
    repeated.scan.completed_at = repeated_at;
    for award in &mut repeated.awards {
        award.observed_at = repeated_at;
        award.available_at = repeated_at;
        award.venue_incentive_event_id = VenueIncentiveEventId::from_v7();
    }
    repository
        .apply_reported_accrual_snapshot(repeated)
        .await
        .expect(
            "same response at a later cadence refreshes scan health without duplicating economics",
        );
    let scans = repository
        .scans(&account_id, program_date, program_date)
        .await
        .expect("read repeated award scans");
    assert_eq!(scans.len(), 2);
    assert_eq!(
        scans
            .iter()
            .map(|scan| scan.completed_at)
            .max()
            .expect("latest scan completion"),
        repeated_at
    );

    let before_revision = repository
        .reconciliation_cumulative(&account_id, first_available_at)
        .await
        .expect("read pre-revision reconciliation");
    assert_eq!(
        before_revision.venue_reported_maker_accrual_usd,
        Usd::new(dec!(2.00))
    );
}

pub async fn wallet_credit_is_cash() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let account_id = ensure_fixture_execution_account(&db).await;
    let repository = PgVenueIncentiveRepository::new(db);
    let observed_at = Utc::now() - Duration::minutes(30);
    let available_at = observed_at + Duration::minutes(5);

    let maker_credit = wallet_credit(
        account_id,
        VenueIncentiveKind::MakerRebate,
        "credit:maker:tx",
        dec!(1.10),
        observed_at,
        available_at,
        'd',
    );
    repository
        .record_scan(
            successful_scan(
                account_id,
                VenueIncentiveKind::MakerRebate,
                observed_at.date_naive(),
                available_at,
                1,
                'd',
            ),
            vec![maker_credit.clone()],
        )
        .await
        .expect("persist maker wallet credit scan");
    let repeated_at = available_at + Duration::minutes(5);
    let mut repeated_maker_credit = maker_credit;
    repeated_maker_credit.venue_incentive_event_id = VenueIncentiveEventId::from_v7();
    repeated_maker_credit.available_at = repeated_at;
    repository
        .record_scan(
            successful_scan(
                account_id,
                VenueIncentiveKind::MakerRebate,
                observed_at.date_naive(),
                repeated_at,
                1,
                'd',
            ),
            vec![repeated_maker_credit],
        )
        .await
        .expect("repeat maker wallet response at a later cadence");
    repository
        .record_scan(
            successful_scan(
                account_id,
                VenueIncentiveKind::TakerRebate,
                observed_at.date_naive(),
                available_at,
                1,
                'e',
            ),
            vec![wallet_credit(
                account_id,
                VenueIncentiveKind::TakerRebate,
                "credit:taker:tx",
                dec!(0.40),
                observed_at,
                available_at,
                'e',
            )],
        )
        .await
        .expect("persist taker wallet credit scan");
    repository
        .record_scan(
            failed_scan(
                account_id,
                VenueIncentiveKind::MakerRebate,
                observed_at.date_naive(),
                available_at + Duration::minutes(10),
            ),
            Vec::new(),
        )
        .await
        .expect("persist failed wallet scan without a response digest");

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
    assert_eq!(reconciliation.venue_reported_maker_accrual_usd, Usd::ZERO);
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
            clob_trade_observation_id: None,
            market_id: Some(MarketId::new(self.market_id)),
            kind: VenueIncentiveKind::MakerRebate,
            stage: VenueIncentiveStage::VenueReportedAccrual,
            program_date: self.observed_at.date_naive(),
            amount_usd: Usd::new(self.amount),
            source_terms_hash: None,
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
        clob_trade_observation_id: None,
        market_id: None,
        kind,
        stage: VenueIncentiveStage::WalletCredited,
        program_date: observed_at.date_naive(),
        amount_usd: Usd::new(amount),
        source_terms_hash: None,
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

fn award_snapshot(
    execution_account_id: ExecutionAccountId,
    program_date: NaiveDate,
    completed_at: DateTime<Utc>,
    awards: Vec<NewVenueIncentiveEvent>,
    hash_seed: char,
) -> NewVenueIncentiveReportedAccrualSnapshot {
    NewVenueIncentiveReportedAccrualSnapshot {
        scan: NewVenueIncentiveReconciliationScan {
            venue_incentive_reconciliation_scan_id: VenueIncentiveReconciliationScanId::from_v7(),
            execution_account_id,
            kind: VenueIncentiveKind::MakerRebate,
            stage: VenueIncentiveStage::VenueReportedAccrual,
            program_date,
            started_at: completed_at - Duration::seconds(1),
            completed_at,
            status: VenueIncentiveReconciliationScanStatus::Succeeded,
            response_digest: Some(content_hash(hash_seed)),
            response_count: i32::try_from(awards.len()).expect("fixture award count fits i32"),
            error_code: None,
        },
        awards,
    }
}

fn successful_scan(
    execution_account_id: ExecutionAccountId,
    kind: VenueIncentiveKind,
    program_date: NaiveDate,
    completed_at: DateTime<Utc>,
    response_count: i32,
    hash_seed: char,
) -> NewVenueIncentiveReconciliationScan {
    NewVenueIncentiveReconciliationScan {
        venue_incentive_reconciliation_scan_id: VenueIncentiveReconciliationScanId::from_v7(),
        execution_account_id,
        kind,
        stage: VenueIncentiveStage::WalletCredited,
        program_date,
        started_at: completed_at - Duration::seconds(1),
        completed_at,
        status: VenueIncentiveReconciliationScanStatus::Succeeded,
        response_digest: Some(content_hash(hash_seed)),
        response_count,
        error_code: None,
    }
}

fn failed_scan(
    execution_account_id: ExecutionAccountId,
    kind: VenueIncentiveKind,
    program_date: NaiveDate,
    completed_at: DateTime<Utc>,
) -> NewVenueIncentiveReconciliationScan {
    NewVenueIncentiveReconciliationScan {
        venue_incentive_reconciliation_scan_id: VenueIncentiveReconciliationScanId::from_v7(),
        execution_account_id,
        kind,
        stage: VenueIncentiveStage::WalletCredited,
        program_date,
        started_at: completed_at - Duration::seconds(1),
        completed_at,
        status: VenueIncentiveReconciliationScanStatus::Failed,
        response_digest: None,
        response_count: 0,
        error_code: Some("fixture_upstream_unavailable".to_owned()),
    }
}

fn content_hash(seed: char) -> ContentHash {
    ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64)))
        .expect("valid content hash")
}
